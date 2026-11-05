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

use crate::OpenDalStorage;

/// Batch size of `delete_stream`, as in Java's `S3FileIO.deleteFiles`.
pub(crate) const DELETE_BATCH_SIZE: usize = 1000;

/// Serves each path with the credential of its longest matching prefix, as
/// Java's `S3FileIO.clientForStoragePath` does, else without credentials.
#[derive(Serialize, Deserialize)]
pub(crate) struct PrefixedStorage {
    default: OpenDalStorage,
    /// Longest prefix first.
    prefixed: Vec<(String, OpenDalStorage)>,
}

impl PrefixedStorage {
    /// Builds the storage for `config` with `build_fn`, which is passed configs
    /// without storage credentials.
    pub(crate) fn build(
        config: &StorageConfig,
        build_fn: impl Fn(&StorageConfig) -> Result<OpenDalStorage>,
    ) -> Result<Arc<dyn Storage>> {
        if config.storage_credentials().is_empty() {
            return Ok(Arc::new(build_fn(config)?));
        }
        Ok(Arc::new(Self::new(config, build_fn)?))
    }

    /// Builds one storage per credential of `config` and one for its props
    /// alone, with `build_fn` as in [`Self::build`].
    pub(crate) fn new(
        config: &StorageConfig,
        build_fn: impl Fn(&StorageConfig) -> Result<OpenDalStorage>,
    ) -> Result<Self> {
        let base = StorageConfig::from_props(config.props().clone());
        let mut prefixed = config
            .storage_credentials()
            .iter()
            .map(|credential| {
                let config = base.clone().with_props(credential.config().clone());
                Ok((credential.prefix().to_string(), build_fn(&config)?))
            })
            .collect::<Result<Vec<_>>>()?;
        prefixed.sort_by_key(|(prefix, _)| std::cmp::Reverse(prefix.len()));
        Ok(Self {
            default: build_fn(&base)?,
            prefixed,
        })
    }

    /// Matches raw strings, as Java does: `s3://b/data` covers `s3://b/database`.
    fn prefix_index(&self, path: &str) -> Option<usize> {
        self.prefixed
            .iter()
            .position(|(prefix, _)| path.starts_with(prefix.as_str()))
    }

    fn storage_at(&self, index: Option<usize>) -> &OpenDalStorage {
        match index {
            Some(index) => &self.prefixed[index].1,
            None => &self.default,
        }
    }

    pub(crate) fn storage_for(&self, path: &str) -> &OpenDalStorage {
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

#[cfg(all(test, feature = "opendal-memory"))]
mod tests {
    use std::time::Duration;

    use iceberg::io::StorageCredential;
    use rstest::rstest;

    use super::*;
    use crate::{OPENDAL_IO_TIMEOUT_MS, OpenDalStorageFactory};

    fn timeout_ms(ms: u64) -> HashMap<String, String> {
        HashMap::from([(OPENDAL_IO_TIMEOUT_MS.to_string(), ms.to_string())])
    }

    /// Memory storages with an IO timeout of 1s by default and of `ms` under each `prefix`.
    fn memory_storage(credentials: &[(&str, u64)]) -> PrefixedStorage {
        let config = StorageConfig::builder()
            .props(timeout_ms(1000))
            .storage_credentials(
                credentials
                    .iter()
                    .map(|(prefix, ms)| StorageCredential::new(*prefix, timeout_ms(*ms)).unwrap())
                    .collect(),
            )
            .build();
        PrefixedStorage::new(&config, |config| {
            OpenDalStorageFactory::Memory.build_storage(config)
        })
        .unwrap()
    }

    fn timeout_for(storage: &PrefixedStorage, path: &str) -> Duration {
        storage.storage_for(path).client_config().io_timeout()
    }

    #[rstest]
    #[case::longest_prefix("memory:/data/warehouse/f", 3000)]
    #[case::shorter_prefix("memory:/data/other/f", 2000)]
    #[case::prefix_of_a_longer_name("memory:/database/f", 2000)]
    #[case::no_prefix("memory:/elsewhere/f", 1000)]
    fn test_routes_a_path_to_its_longest_matching_prefix(
        #[case] path: &str,
        #[case] expected_ms: u64,
    ) {
        let storage = memory_storage(&[("memory:/data", 2000), ("memory:/data/warehouse", 3000)]);

        assert_eq!(
            timeout_for(&storage, path),
            Duration::from_millis(expected_ms)
        );
    }

    #[rstest]
    #[case::without_slash("memory:/table")]
    #[case::with_slash("memory:/table/")]
    #[tokio::test]
    async fn test_delete_prefix_routes_a_directory_with_or_without_its_slash(#[case] path: &str) {
        let storage = memory_storage(&[("memory:/table/", 2000)]);
        storage
            .write("memory:/table/f", Bytes::from("data"))
            .await
            .unwrap();

        storage.delete_prefix(path).await.unwrap();

        assert!(!storage.exists("memory:/table/f").await.unwrap());
    }

    #[tokio::test]
    async fn test_delete_stream_deletes_each_path_with_its_prefix_storage() {
        let storage = memory_storage(&[("memory:/table/", 2000)]);
        let paths: Vec<_> = (0..DELETE_BATCH_SIZE + 1)
            .map(|i| format!("memory:/table/{i}"))
            .chain(["memory:/other/f".to_string()])
            .collect();
        for path in &paths {
            storage.write(path, Bytes::from("data")).await.unwrap();
        }

        storage
            .delete_stream(stream::iter(paths.clone()).boxed())
            .await
            .unwrap();

        for path in &paths {
            assert!(!storage.exists(path).await.unwrap(), "{path}");
        }
    }

    #[test]
    fn test_debug_omits_prefixed_storages() {
        let storage = memory_storage(&[("memory:/table/", 2000)]);

        assert_eq!(
            format!("{storage:?}"),
            format!(
                r#"PrefixedStorage {{ default: {:?}, prefixes: ["memory:/table/"], .. }}"#,
                storage.default
            )
        );
    }
}
