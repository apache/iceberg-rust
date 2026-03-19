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
//! which implement the [`Storage`](iceberg::io::Storage) and
//! [`StorageFactory`](iceberg::io::StorageFactory) traits from the `iceberg` crate
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
use futures::StreamExt;
use futures::stream::BoxStream;
#[cfg(feature = "object_store-s3")]
use iceberg::io::S3Config;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectStore, PutPayload, WriteMultipart};
#[cfg(feature = "object_store-s3")]
use s3::{build_s3_store, parse_s3_url};
use serde::{Deserialize, Serialize};

/// Convert an `object_store::Error` into an `iceberg::Error`.
fn from_object_store_error(e: object_store::Error) -> Error {
    Error::new(ErrorKind::Unexpected, "Failure in doing io operation").with_source(e)
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
                Ok(Arc::new(ObjectStoreStorage::S3 {
                    config: Arc::new(s3_config),
                    store_cache: Arc::new(DashMap::new()),
                }))
            }
        }
    }
}

/// `object_store`-based storage implementation.
///
/// Stores are cached per bucket to avoid rebuilding the client on every operation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ObjectStoreStorage {
    /// S3 storage variant.
    #[cfg(feature = "object_store-s3")]
    S3 {
        /// Parsed S3 configuration from iceberg core.
        config: Arc<S3Config>,
        /// Per-bucket store cache.
        #[serde(skip, default)]
        store_cache: Arc<DashMap<String, Arc<dyn ObjectStore>>>,
    },
}

impl ObjectStoreStorage {
    /// Get or create a cached store and extract the relative `ObjectStorePath`.
    fn get_store_and_path(&self, path: &str) -> Result<(Arc<dyn ObjectStore>, ObjectStorePath)> {
        match self {
            #[cfg(feature = "object_store-s3")]
            ObjectStoreStorage::S3 {
                config,
                store_cache,
            } => {
                let (_scheme, bucket, relative) = parse_s3_url(path)?;

                let store = store_cache
                    .entry(bucket.to_string())
                    .or_try_insert_with(|| build_s3_store(config, bucket))?
                    .value()
                    .clone();

                Ok((store, ObjectStorePath::from(relative)))
            }
        }
    }
}

#[typetag::serde(name = "ObjectStoreStorage")]
#[async_trait]
impl Storage for ObjectStoreStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let (store, object_path) = self.get_store_and_path(path)?;
        match store.head(&object_path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(from_object_store_error(e)),
        }
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let (store, object_path) = self.get_store_and_path(path)?;
        let meta = store
            .head(&object_path)
            .await
            .map_err(from_object_store_error)?;
        Ok(FileMetadata {
            size: meta.size as u64,
        })
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let (store, object_path) = self.get_store_and_path(path)?;
        let result = store
            .get(&object_path)
            .await
            .map_err(from_object_store_error)?;
        result.bytes().await.map_err(from_object_store_error)
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let (store, object_path) = self.get_store_and_path(path)?;
        Ok(Box::new(ObjectStoreReader {
            store,
            path: object_path,
        }))
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        let (store, object_path) = self.get_store_and_path(path)?;
        store
            .put(&object_path, PutPayload::from_bytes(bs))
            .await
            .map_err(from_object_store_error)?;
        Ok(())
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let (store, object_path) = self.get_store_and_path(path)?;
        let upload = store
            .put_multipart(&object_path)
            .await
            .map_err(from_object_store_error)?;
        let writer = WriteMultipart::new(upload);
        Ok(Box::new(ObjectStoreWriter {
            writer: Some(writer),
        }))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let (store, object_path) = self.get_store_and_path(path)?;
        store
            .delete(&object_path)
            .await
            .map_err(from_object_store_error)?;
        Ok(())
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let (store, object_path) = self.get_store_and_path(path)?;
        let prefix = if object_path.as_ref().ends_with('/') {
            object_path
        } else {
            ObjectStorePath::from(format!("{}/", object_path.as_ref()))
        };

        let mut list_stream = store.list(Some(&prefix));
        while let Some(entry) = list_stream.next().await {
            let entry = entry.map_err(from_object_store_error)?;
            store
                .delete(&entry.location)
                .await
                .map_err(from_object_store_error)?;
        }
        Ok(())
    }

    async fn delete_stream(&self, mut paths: BoxStream<'static, String>) -> Result<()> {
        while let Some(path) = paths.next().await {
            let (store, object_path) = self.get_store_and_path(&path)?;
            store
                .delete(&object_path)
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

#[async_trait]
impl FileWrite for ObjectStoreWriter {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "Writer has already been closed"))?;
        writer.write(&bs);
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
        ObjectStoreStorage::S3 {
            config: Arc::new(S3Config::default()),
            store_cache: Arc::new(DashMap::new()),
        }
    }

    #[cfg(feature = "object_store-s3")]
    #[test]
    fn test_store_cache_reuses_store() {
        let storage = make_s3_storage();
        let (store1, _) = storage
            .get_store_and_path("s3://test-bucket/file1.parquet")
            .unwrap();
        let (store2, _) = storage
            .get_store_and_path("s3://test-bucket/file2.parquet")
            .unwrap();
        assert!(Arc::ptr_eq(&store1, &store2));
    }

    #[cfg(feature = "object_store-s3")]
    #[test]
    fn test_store_cache_different_buckets() {
        let storage = make_s3_storage();
        let (store1, _) = storage
            .get_store_and_path("s3://bucket-a/file.parquet")
            .unwrap();
        let (store2, _) = storage
            .get_store_and_path("s3://bucket-b/file.parquet")
            .unwrap();
        assert!(!Arc::ptr_eq(&store1, &store2));
    }

    #[cfg(feature = "object_store-s3")]
    #[test]
    fn test_relative_path_extraction() {
        let storage = make_s3_storage();
        let (_, path) = storage
            .get_store_and_path("s3://my-bucket/data/file.parquet")
            .unwrap();
        assert_eq!(path.as_ref(), "data/file.parquet");
    }
}
