// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Storage that serves each location with its prefix's credential.

use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use iceberg::Result;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
};
use serde::{Deserialize, Serialize};

/// Per-prefix batch size of `delete_stream`, as in Java's `S3FileIO.deleteFiles`.
const DELETE_BATCH_SIZE: usize = 1000;

/// Serves each path with the credential of its longest matching prefix, as
/// Java's `S3FileIO.clientForStoragePath` does, else without credentials.
#[derive(Serialize, Deserialize)]
pub(crate) struct PrefixedStorage {
    default: Arc<dyn Storage>,
    /// Longest prefix first.
    prefixed: Vec<(String, Arc<dyn Storage>)>,
}

impl PrefixedStorage {
    /// Builds the storage for `config` with `build`, which is passed configs
    /// without storage credentials.
    pub(crate) fn build(
        config: &StorageConfig,
        build: impl Fn(&StorageConfig) -> Result<Arc<dyn Storage>>,
    ) -> Result<Arc<dyn Storage>> {
        if config.storage_credentials().is_empty() {
            return build(config);
        }
        Ok(Arc::new(Self::new(config, build)?))
    }

    fn new(
        config: &StorageConfig,
        build: impl Fn(&StorageConfig) -> Result<Arc<dyn Storage>>,
    ) -> Result<Self> {
        let base = StorageConfig::from_props(config.props().clone());
        // The last credential for a prefix wins, as in Java's `clientByPrefix`.
        let credentials: HashMap<&str, &HashMap<String, String>> = config
            .storage_credentials()
            .iter()
            .filter(|credential| !credential.prefix().is_empty())
            .map(|credential| (credential.prefix(), credential.config()))
            .collect();
        let mut prefixed = credentials
            .into_iter()
            .map(|(prefix, credential)| {
                let config = base.clone().with_props(credential.clone());
                Ok((prefix.to_string(), build(&config)?))
            })
            .collect::<Result<Vec<_>>>()?;
        prefixed.sort_by_key(|(prefix, _)| std::cmp::Reverse(prefix.len()));
        Ok(Self {
            default: build(&base)?,
            prefixed,
        })
    }

    /// Matches raw strings, as Java does: `s3://b/data` covers `s3://b/database`.
    fn prefix_index(&self, path: &str) -> Option<usize> {
        self.prefixed
            .iter()
            .position(|(prefix, _)| path.starts_with(prefix.as_str()))
    }

    fn storage_at(&self, index: Option<usize>) -> &dyn Storage {
        match index {
            Some(index) => self.prefixed[index].1.as_ref(),
            None => self.default.as_ref(),
        }
    }

    fn storage_for(&self, path: &str) -> &dyn Storage {
        self.storage_at(self.prefix_index(path))
    }
}

// A backend's Debug may print the credentials it was built with.
impl Debug for PrefixedStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let prefixes = fmt::from_fn(|f| {
            f.debug_list()
                .entries(self.prefixed.iter().map(|(prefix, _)| prefix))
                .finish()
        });
        f.debug_struct("PrefixedStorage")
            .field("default", &self.default)
            .field("prefixes", &prefixes)
            .finish_non_exhaustive()
    }
}

#[async_trait]
#[typetag::serde]
impl Storage for PrefixedStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        self.storage_for(path).exists(path).await
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        self.storage_for(path).metadata(path).await
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        self.storage_for(path).read(path).await
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        self.storage_for(path).reader(path).await
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        self.storage_for(path).write(path, bs).await
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        self.storage_for(path).writer(path).await
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.storage_for(path).delete(path).await
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        // Route as a directory, so an `s3://b/t/` prefix covers `s3://b/t`.
        let dir = format!("{}/", path.strip_suffix('/').unwrap_or(path));
        self.storage_for(&dir).delete_prefix(path).await
    }

    async fn delete_stream(&self, mut paths: BoxStream<'static, String>) -> Result<()> {
        let mut batches: HashMap<Option<usize>, Vec<String>> = HashMap::new();
        while let Some(path) = paths.next().await {
            let index = self.prefix_index(&path);
            let batch = batches.entry(index).or_default();
            batch.push(path);
            if batch.len() >= DELETE_BATCH_SIZE {
                let batch = std::mem::take(batch);
                self.storage_at(index)
                    .delete_stream(stream::iter(batch).boxed())
                    .await?;
            }
        }
        for (index, batch) in batches {
            if !batch.is_empty() {
                self.storage_at(index)
                    .delete_stream(stream::iter(batch).boxed())
                    .await?;
            }
        }
        Ok(())
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        self.storage_for(path).new_input(path)
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        self.storage_for(path).new_output(path)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use iceberg::io::StorageCredential;

    use super::*;

    type CallLog = Arc<Mutex<Vec<(String, String)>>>;

    /// Records each call's path, or `delete_stream`'s batch size, with its `scope` prop.
    #[derive(Debug, Serialize, Deserialize)]
    struct RecordingStorage {
        props: HashMap<String, String>,
        #[serde(skip)]
        calls: CallLog,
    }

    impl RecordingStorage {
        fn record(&self, arg: impl Into<String>) {
            self.calls
                .lock()
                .unwrap()
                .push((self.props["scope"].clone(), arg.into()));
        }
    }

    #[async_trait]
    #[typetag::serde]
    impl Storage for RecordingStorage {
        async fn exists(&self, path: &str) -> Result<bool> {
            self.record(path);
            Ok(true)
        }

        async fn metadata(&self, _path: &str) -> Result<FileMetadata> {
            unimplemented!()
        }

        async fn read(&self, _path: &str) -> Result<Bytes> {
            unimplemented!()
        }

        async fn reader(&self, _path: &str) -> Result<Box<dyn FileRead>> {
            unimplemented!()
        }

        async fn write(&self, _path: &str, _bs: Bytes) -> Result<()> {
            unimplemented!()
        }

        async fn writer(&self, _path: &str) -> Result<Box<dyn FileWrite>> {
            unimplemented!()
        }

        async fn delete(&self, _path: &str) -> Result<()> {
            unimplemented!()
        }

        async fn delete_prefix(&self, path: &str) -> Result<()> {
            self.record(path);
            Ok(())
        }

        async fn delete_stream(&self, paths: BoxStream<'static, String>) -> Result<()> {
            self.record(paths.count().await.to_string());
            Ok(())
        }

        fn new_input(&self, _path: &str) -> Result<InputFile> {
            unimplemented!()
        }

        fn new_output(&self, _path: &str) -> Result<OutputFile> {
            unimplemented!()
        }
    }

    /// A storage with `scope` "default" and a credential per (prefix, scope).
    fn recording_storage(credentials: &[(&str, &str)]) -> (PrefixedStorage, CallLog) {
        let scope = |scope: &str| HashMap::from([("scope".to_string(), scope.to_string())]);
        let config = StorageConfig::builder()
            .props(scope("default"))
            .storage_credentials(
                credentials
                    .iter()
                    .map(|(prefix, credential)| StorageCredential::new(*prefix, scope(credential)))
                    .collect(),
            )
            .build();
        let calls = CallLog::default();
        let storage = PrefixedStorage::new(&config, |config| {
            Ok(Arc::new(RecordingStorage {
                props: config.props().clone(),
                calls: calls.clone(),
            }))
        })
        .unwrap();
        (storage, calls)
    }

    fn recorded(calls: &[(&str, &str)]) -> Vec<(String, String)> {
        calls
            .iter()
            .map(|(scope, arg)| (scope.to_string(), arg.to_string()))
            .collect()
    }

    #[tokio::test]
    async fn test_routes_a_path_to_its_longest_matching_prefix() {
        let (storage, calls) = recording_storage(&[
            ("s3://bucket/data", "short"),
            ("s3://bucket/data/warehouse", "long"),
            ("", "empty"),
        ]);

        for path in [
            "s3://bucket/data/warehouse/f",
            "s3://bucket/data/other/f",
            "s3://bucket/database/f",
            "s3://bucket/elsewhere/f",
        ] {
            storage.exists(path).await.unwrap();
        }

        assert_eq!(
            *calls.lock().unwrap(),
            recorded(&[
                ("long", "s3://bucket/data/warehouse/f"),
                ("short", "s3://bucket/data/other/f"),
                ("short", "s3://bucket/database/f"),
                ("default", "s3://bucket/elsewhere/f"),
            ])
        );
    }

    #[tokio::test]
    async fn test_a_later_credential_replaces_an_earlier_one_for_its_prefix() {
        let (storage, calls) = recording_storage(&[
            ("s3://bucket/table/", "earlier"),
            ("s3://bucket/table/", "later"),
        ]);

        storage.exists("s3://bucket/table/f").await.unwrap();

        assert_eq!(
            *calls.lock().unwrap(),
            recorded(&[("later", "s3://bucket/table/f")])
        );
    }

    #[tokio::test]
    async fn test_delete_prefix_routes_a_directory_with_or_without_its_slash() {
        for path in ["s3://bucket/table", "s3://bucket/table/"] {
            let (storage, calls) = recording_storage(&[("s3://bucket/table/", "vended")]);

            storage.delete_prefix(path).await.unwrap();

            assert_eq!(*calls.lock().unwrap(), recorded(&[("vended", path)]));
        }
    }

    #[tokio::test]
    async fn test_delete_stream_batches_paths_per_prefix() {
        let (storage, calls) = recording_storage(&[("s3://bucket/table/", "vended")]);
        let paths = (0..DELETE_BATCH_SIZE + 1)
            .map(|i| format!("s3://bucket/table/{i}"))
            .chain(["s3://bucket/other/f".to_string()]);

        storage
            .delete_stream(stream::iter(paths).boxed())
            .await
            .unwrap();

        let mut calls = calls.lock().unwrap().clone();
        calls.sort();
        assert_eq!(
            calls,
            recorded(&[("default", "1"), ("vended", "1"), ("vended", "1000")])
        );
    }

    #[test]
    fn test_debug_omits_prefixed_storages() {
        let (storage, _) = recording_storage(&[("s3://bucket/table/", "vended-secret")]);

        let debug = format!("{storage:?}");

        assert!(!debug.contains("vended-secret"), "{debug}");
        assert!(debug.contains("s3://bucket/table/"), "{debug}");
    }
}
