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

//! `object_store`-based storage implementation for Apache Iceberg.
//!
//! This crate provides [`ObjectStoreStorage`] and [`ObjectStoreStorageFactory`],
//! which implement the [`Storage`] and
//! [`StorageFactory`] traits from the `iceberg` crate
//! using the [`object_store`](https://docs.rs/object_store) crate as the backend.
//!
//! Currently only S3 storage is supported (via the `object_store-s3` feature flag,
//! enabled by default).

#[cfg(feature = "object_store-s3")]
mod s3;

use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
#[cfg(feature = "object_store-s3")]
use iceberg::io::S3Config;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, WriteMultipart};
#[cfg(feature = "object_store-s3")]
use s3::{build_s3_store, parse_s3_url};
use serde::{Deserialize, Serialize};

/// Convert an `object_store::Error` into an `iceberg::Error`,
/// dispatching known variants to their corresponding `ErrorKind`.
fn from_object_store_error(e: object_store::Error) -> Error {
    let (kind, msg) = match &e {
        object_store::Error::NotFound { path, .. } => {
            (ErrorKind::DataInvalid, format!("Object not found: {path}"))
        }
        object_store::Error::AlreadyExists { path, .. } => (
            ErrorKind::DataInvalid,
            format!("Object already exists: {path}"),
        ),
        object_store::Error::PermissionDenied { path, .. } => {
            (ErrorKind::DataInvalid, format!("Permission denied: {path}"))
        }
        object_store::Error::Unauthenticated { path, .. } => {
            (ErrorKind::DataInvalid, format!("Unauthenticated: {path}"))
        }
        object_store::Error::NotSupported { .. } => (
            ErrorKind::FeatureUnsupported,
            "Operation not supported".to_string(),
        ),
        _ => (
            ErrorKind::Unexpected,
            "Failure in doing io operation".to_string(),
        ),
    };
    Error::new(kind, msg).with_source(e)
}

/// Convert `object_store::ObjectMeta` into `iceberg::io::FileMetadata`.
fn to_file_metadata(meta: object_store::ObjectMeta) -> FileMetadata {
    FileMetadata { size: meta.size }
}

/// `object_store`-based storage factory.
///
/// Use this factory with `FileIOBuilder::new(factory)` to create FileIO instances
/// backed by the `object_store` crate.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ObjectStoreStorageFactory {
    /// S3 storage factory.
    #[cfg(feature = "object_store-s3")]
    S3,
}

#[typetag::serde(name = "ObjectStoreStorageFactory")]
impl StorageFactory for ObjectStoreStorageFactory {
    #[allow(unused_variables)]
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        match self {
            #[cfg(feature = "object_store-s3")]
            ObjectStoreStorageFactory::S3 => {
                let s3_config = S3Config::try_from(config)?;
                Ok(Arc::new(ObjectStoreStorage::S3(S3Storage {
                    config: Arc::new(s3_config),
                    store_cache: Arc::new(DashMap::new()),
                })))
            }
        }
    }
}

type StoreCache = Arc<DashMap<String, Arc<dyn ObjectStore>>>;

/// `object_store` S3 storage state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct S3Storage {
    config: Arc<S3Config>,
    #[serde(skip, default)]
    store_cache: StoreCache,
}

/// `object_store`-based storage implementation.
///
/// Stores are cached per bucket to avoid rebuilding the client on every operation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ObjectStoreStorage {
    /// S3 storage variant.
    #[cfg(feature = "object_store-s3")]
    S3(S3Storage),
}

struct StoreAndPath {
    store: Arc<dyn ObjectStore>,
    path: ObjectStorePath,
}

impl ObjectStoreStorage {
    /// Get or create a cached store and extract the relative `ObjectStorePath`.
    fn get_store_and_path(&self, path: &str) -> Result<StoreAndPath> {
        match self {
            #[cfg(feature = "object_store-s3")]
            ObjectStoreStorage::S3(s3) => {
                let parsed = parse_s3_url(path)?;

                let store = s3
                    .store_cache
                    .entry(parsed.bucket.clone())
                    .or_try_insert_with(|| build_s3_store(&s3.config, &parsed.bucket))?
                    .value()
                    .clone();

                let object_path =
                    ObjectStorePath::from_url_path(&parsed.relative).map_err(|e| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!("Invalid URL path: {}", parsed.relative),
                        )
                        .with_source(e)
                    })?;

                Ok(StoreAndPath {
                    store,
                    path: object_path,
                })
            }
        }
    }
}

#[typetag::serde(name = "ObjectStoreStorage")]
#[async_trait]
impl Storage for ObjectStoreStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let target = self.get_store_and_path(path)?;
        match target.store.head(&target.path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(from_object_store_error(e)),
        }
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let target = self.get_store_and_path(path)?;
        let meta = target
            .store
            .head(&target.path)
            .await
            .map_err(from_object_store_error)?;
        Ok(to_file_metadata(meta))
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let target = self.get_store_and_path(path)?;
        let result = target
            .store
            .get(&target.path)
            .await
            .map_err(from_object_store_error)?;
        result.bytes().await.map_err(from_object_store_error)
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let target = self.get_store_and_path(path)?;
        Ok(Box::new(ObjectStoreReader {
            store: target.store,
            path: target.path,
        }))
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        let target = self.get_store_and_path(path)?;
        target
            .store
            .put(&target.path, PutPayload::from_bytes(bs))
            .await
            .map_err(from_object_store_error)?;
        Ok(())
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let target = self.get_store_and_path(path)?;
        let upload = target
            .store
            .put_multipart(&target.path)
            .await
            .map_err(from_object_store_error)?;
        let writer = WriteMultipart::new(upload);
        Ok(Box::new(ObjectStoreWriter {
            writer: Some(writer),
        }))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let target = self.get_store_and_path(path)?;
        target
            .store
            .delete(&target.path)
            .await
            .map_err(from_object_store_error)?;
        Ok(())
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let target = self.get_store_and_path(path)?;
        let locations = target
            .store
            .list(Some(&target.path))
            .map_ok(|m| m.location)
            .boxed();
        target
            .store
            .delete_stream(locations)
            .try_collect::<Vec<_>>()
            .await
            .map_err(from_object_store_error)?;
        Ok(())
    }

    async fn delete_stream(&self, paths: BoxStream<'static, String>) -> Result<()> {
        // Collect and group by bucket so each store gets a single bulk DeleteObjects call.
        let all_paths: Vec<String> = paths.collect().await;
        let mut grouped: std::collections::HashMap<String, Vec<ObjectStorePath>> =
            std::collections::HashMap::new();
        let mut stores: std::collections::HashMap<String, Arc<dyn ObjectStore>> =
            std::collections::HashMap::new();

        for path in all_paths {
            let target = self.get_store_and_path(&path)?;
            let bucket = match self {
                #[cfg(feature = "object_store-s3")]
                ObjectStoreStorage::S3(_) => {
                    let parsed = parse_s3_url(&path)?;
                    parsed.bucket
                }
            };
            stores.entry(bucket.clone()).or_insert(target.store);
            grouped.entry(bucket).or_default().push(target.path);
        }

        for (bucket, locations) in grouped {
            let store = stores.remove(&bucket).expect("store must exist");
            let location_stream = futures::stream::iter(locations.into_iter().map(Ok)).boxed();
            store
                .delete_stream(location_stream)
                .try_collect::<Vec<_>>()
                .await
                .map_err(from_object_store_error)?;
        }
        Ok(())
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

/// Reader that implements `FileRead` using `object_store`.
struct ObjectStoreReader {
    store: Arc<dyn ObjectStore>,
    path: ObjectStorePath,
}

#[async_trait]
impl FileRead for ObjectStoreReader {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        let opts = object_store::GetOptions {
            range: Some((range.start..range.end).into()),
            ..Default::default()
        };
        let result = self
            .store
            .get_opts(&self.path, opts)
            .await
            .map_err(from_object_store_error)?;
        result.bytes().await.map_err(from_object_store_error)
    }
}

/// Writer that implements `FileWrite` using `object_store` multipart upload.
struct ObjectStoreWriter {
    writer: Option<WriteMultipart>,
}

impl Drop for ObjectStoreWriter {
    fn drop(&mut self) {
        if let Some(writer) = self.writer.take() {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let _ = writer.abort().await;
                });
            } else {
                tracing::warn!(
                    "ObjectStoreWriter dropped outside a Tokio runtime; multipart upload abort skipped"
                );
            }
        }
    }
}

#[async_trait]
impl FileWrite for ObjectStoreWriter {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "Writer has already been closed"))?;
        writer.put(bs);
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        let writer = self
            .writer
            .take()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "Writer has already been closed"))?;
        writer.finish().await.map_err(from_object_store_error)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "object_store-s3")]
    fn make_s3_storage() -> ObjectStoreStorage {
        ObjectStoreStorage::S3(S3Storage {
            config: Arc::new(S3Config::default()),
            store_cache: Arc::new(DashMap::new()),
        })
    }

    #[cfg(feature = "object_store-s3")]
    #[test]
    fn test_store_cache_reuses_store() {
        let storage = make_s3_storage();
        let target1 = storage
            .get_store_and_path("s3://test-bucket/file1.parquet")
            .unwrap();
        let target2 = storage
            .get_store_and_path("s3://test-bucket/file2.parquet")
            .unwrap();
        assert!(Arc::ptr_eq(&target1.store, &target2.store));
    }

    #[cfg(feature = "object_store-s3")]
    #[test]
    fn test_store_cache_different_buckets() {
        let storage = make_s3_storage();
        let target1 = storage
            .get_store_and_path("s3://bucket-a/file.parquet")
            .unwrap();
        let target2 = storage
            .get_store_and_path("s3://bucket-b/file.parquet")
            .unwrap();
        assert!(!Arc::ptr_eq(&target1.store, &target2.store));
    }

    #[cfg(feature = "object_store-s3")]
    #[test]
    fn test_relative_path_extraction() {
        let storage = make_s3_storage();
        let target = storage
            .get_store_and_path("s3://my-bucket/data/file.parquet")
            .unwrap();
        assert_eq!(target.path.as_ref(), "data/file.parquet");

        let target_encoded = storage
            .get_store_and_path("s3://my-bucket/data%20dir/file.parquet")
            .unwrap();
        assert_eq!(target_encoded.path.as_ref(), "data dir/file.parquet");
    }

    #[cfg(feature = "object_store-s3")]
    #[test]
    fn test_storage_serialization_roundtrip() {
        let storage = make_s3_storage();
        let serialized = serde_json::to_string(&storage).unwrap();
        let deserialized: ObjectStoreStorage = serde_json::from_str(&serialized).unwrap();
        match deserialized {
            ObjectStoreStorage::S3(s3) => {
                assert_eq!(s3.config, Arc::new(S3Config::default()));
            }
        }
    }

    #[cfg(feature = "object_store-s3")]
    #[test]
    fn test_storage_factory_serialization_roundtrip() {
        let factory = ObjectStoreStorageFactory::S3;
        let serialized = serde_json::to_string(&factory).unwrap();
        let deserialized: ObjectStoreStorageFactory = serde_json::from_str(&serialized).unwrap();
        assert!(matches!(deserialized, ObjectStoreStorageFactory::S3));
    }

    #[cfg(feature = "object_store-s3")]
    #[test]
    fn test_file_io_serialization_roundtrip() {
        use iceberg::io::FileIOBuilder;
        let factory = Arc::new(ObjectStoreStorageFactory::S3);
        let file_io = FileIOBuilder::new(factory).build();
        let bytes = file_io.serialize_all().unwrap();
        let deserialized = iceberg::io::FileIO::deserialize_all(&bytes).unwrap();
        assert_eq!(file_io.config(), deserialized.config());
    }

    #[tokio::test]
    async fn test_writer_already_closed_errors() {
        let mut writer = ObjectStoreWriter { writer: None };
        let write_err = writer.write(Bytes::from_static(b"data")).await.unwrap_err();
        assert_eq!(write_err.kind(), ErrorKind::Unexpected);
        assert_eq!(write_err.message(), "Writer has already been closed");

        let close_err = writer.close().await.unwrap_err();
        assert_eq!(close_err.kind(), ErrorKind::Unexpected);
        assert_eq!(close_err.message(), "Writer has already been closed");
    }

    #[test]
    fn test_writer_drop_outside_tokio_warns_and_does_not_panic() {
        // A plain synchronous #[test] runs on an OS thread outside a Tokio runtime context
        let writer = ObjectStoreWriter { writer: None };
        drop(writer);
    }
}
