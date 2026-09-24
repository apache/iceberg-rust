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
use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use futures::stream::{BoxStream, FuturesUnordered};
use futures::{StreamExt, TryStreamExt};
#[cfg(feature = "object_store-s3")]
use iceberg::io::S3Config;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};
use object_store::path::Path as ObjectStorePath;
use object_store::{MultipartUpload, ObjectStore, ObjectStoreExt, PutPayload, UploadPart};
#[cfg(feature = "object_store-s3")]
use s3::{build_s3_store, parse_s3_url};
use serde::{Deserialize, Serialize};

/// Convert an `object_store::Error` into an `iceberg::Error`,
/// dispatching known variants to their corresponding `ErrorKind`.
fn from_object_store_error(e: object_store::Error) -> Error {
    let (kind, msg) = match &e {
        object_store::Error::NotFound { path, .. } => {
            (ErrorKind::Unexpected, format!("Object not found: {path}"))
        }
        object_store::Error::AlreadyExists { path, .. } => (
            ErrorKind::Unexpected,
            format!("Object already exists: {path}"),
        ),
        object_store::Error::PermissionDenied { path, .. } => {
            (ErrorKind::Unexpected, format!("Permission denied: {path}"))
        }
        object_store::Error::Unauthenticated { path, .. } => {
            (ErrorKind::Unexpected, format!("Unauthenticated: {path}"))
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

/// Property key for configuring S3 bulk delete batch size.
pub const S3_DELETE_BATCH_SIZE: &str = "s3.delete-batch-size";
/// Default batch size for S3 bulk deletions (matches AWS S3 DeleteObjects max).
pub const DEFAULT_DELETE_BATCH_SIZE: usize = 1000;
/// Maximum batch size allowed by the AWS S3 DeleteObjects API specification.
pub const S3_MAX_DELETE_BATCH_SIZE: usize = 1000;

fn parse_delete_batch_size(config: &StorageConfig) -> usize {
    if let Some(val) = config.get(S3_DELETE_BATCH_SIZE) {
        match val.parse::<usize>() {
            Ok(parsed) if parsed > 0 => {
                if parsed > S3_MAX_DELETE_BATCH_SIZE {
                    tracing::warn!(
                        configured = parsed,
                        limit = S3_MAX_DELETE_BATCH_SIZE,
                        "Configured s3.delete-batch-size exceeds AWS S3 hard limit of 1000; requests may fail with MalformedXML"
                    );
                }
                parsed
            }
            _ => {
                tracing::warn!(
                    val = %val,
                    "Invalid s3.delete-batch-size; falling back to default 1000"
                );
                DEFAULT_DELETE_BATCH_SIZE
            }
        }
    } else {
        DEFAULT_DELETE_BATCH_SIZE
    }
}

fn default_delete_batch_size() -> usize {
    DEFAULT_DELETE_BATCH_SIZE
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
                let delete_batch_size = parse_delete_batch_size(config);
                tracing::info!(
                    batch_size = delete_batch_size,
                    "Initialized S3 storage with delete batch size {} (configure via '{}' to adjust)",
                    delete_batch_size,
                    S3_DELETE_BATCH_SIZE
                );
                Ok(Arc::new(ObjectStoreStorage::S3(S3Storage {
                    config: Arc::new(s3_config),
                    delete_batch_size,
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
    #[serde(default = "default_delete_batch_size")]
    pub delete_batch_size: usize,
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
    bucket: String,
    store: Arc<dyn ObjectStore>,
    path: ObjectStorePath,
}

/// Helper for batching deletions per bucket.
struct BucketBatch {
    store: Arc<dyn ObjectStore>,
    locations: Vec<ObjectStorePath>,
}

impl ObjectStoreStorage {
    fn delete_batch_size(&self) -> usize {
        match self {
            #[cfg(feature = "object_store-s3")]
            ObjectStoreStorage::S3(s3) => s3.delete_batch_size,
        }
    }

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
                    bucket: parsed.bucket,
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
        Ok(Box::new(ObjectStoreWriter {
            upload: Some(upload),
            buffer: BytesMut::new(),
            tasks: FuturesUnordered::new(),
            parts_submitted: 0,
            bytes_written: 0,
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
            .try_for_each(|_| async { Ok(()) })
            .await
            .map_err(from_object_store_error)?;
        Ok(())
    }

    async fn delete_stream(&self, paths: BoxStream<'static, String>) -> Result<()> {
        let batch_size = self.delete_batch_size();
        let mut chunk_stream = paths.chunks(batch_size);

        while let Some(chunk) = chunk_stream.next().await {
            let mut batches: std::collections::HashMap<String, BucketBatch> =
                std::collections::HashMap::new();

            for path in chunk {
                let target = self.get_store_and_path(&path)?;
                batches
                    .entry(target.bucket)
                    .or_insert_with(|| BucketBatch {
                        store: target.store,
                        locations: Vec::new(),
                    })
                    .locations
                    .push(target.path);
            }

            for (_bucket, batch) in batches {
                let location_stream =
                    futures::stream::iter(batch.locations.into_iter().map(Ok)).boxed();
                batch
                    .store
                    .delete_stream(location_stream)
                    .try_for_each(|_| async { Ok(()) })
                    .await
                    .map_err(from_object_store_error)?;
            }
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
        if range.is_empty() {
            return Ok(Bytes::new());
        }
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

/// Minimum part size for S3 multipart upload (5 MiB).
const MIN_PART_SIZE: usize = 5 * 1024 * 1024;

/// Default maximum concurrent in-flight part uploads.
const MAX_CONCURRENT_PART_UPLOADS: usize = 8;

/// Writer that implements `FileWrite` using `object_store` multipart upload.
struct ObjectStoreWriter {
    upload: Option<Box<dyn MultipartUpload>>,
    buffer: BytesMut,
    tasks: FuturesUnordered<UploadPart>,
    parts_submitted: usize,
    bytes_written: u64,
}

impl ObjectStoreWriter {
    /// Flushes any buffered bytes as an in-flight part upload to S3.
    fn flush_buffer(
        buffer: &mut BytesMut,
        tasks: &mut FuturesUnordered<UploadPart>,
        parts_submitted: &mut usize,
        upload: &mut Box<dyn MultipartUpload>,
    ) {
        if !buffer.is_empty() {
            let part_data = std::mem::take(buffer).freeze();
            let part_fut = upload.put_part(PutPayload::from_bytes(part_data));
            tasks.push(part_fut);
            *parts_submitted += 1;
        }
    }

    /// Accumulates bytes into `buffer`, flushing 5 MiB parts when full.
    fn append_bytes(
        buffer: &mut BytesMut,
        tasks: &mut FuturesUnordered<UploadPart>,
        parts_submitted: &mut usize,
        mut bs: Bytes,
        upload: &mut Box<dyn MultipartUpload>,
    ) {
        while !bs.is_empty() {
            let remaining = MIN_PART_SIZE.saturating_sub(buffer.len());
            if remaining == 0 {
                Self::flush_buffer(buffer, tasks, parts_submitted, upload);
                continue;
            }

            if bs.len() < remaining {
                buffer.extend_from_slice(&bs);
                return;
            }
            let chunk = bs.split_to(remaining);
            buffer.extend_from_slice(&chunk);
            Self::flush_buffer(buffer, tasks, parts_submitted, upload);
        }
    }
}

impl Drop for ObjectStoreWriter {
    fn drop(&mut self) {
        if let Some(mut upload) = self.upload.take() {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    if let Err(e) = upload.abort().await {
                        tracing::warn!(
                            error = %e,
                            "Failed to abort multipart upload on drop"
                        );
                    }
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
        let upload = self.upload.as_mut().ok_or_else(|| {
            Error::new(
                ErrorKind::PreconditionFailed,
                "Writer has already been closed",
            )
        })?;
        self.bytes_written += bs.len() as u64;
        Self::append_bytes(
            &mut self.buffer,
            &mut self.tasks,
            &mut self.parts_submitted,
            bs,
            upload,
        );

        // Throttle in-flight uploads: fail fast if any part upload fails
        while self.tasks.len() >= MAX_CONCURRENT_PART_UPLOADS {
            if let Some(res) = self.tasks.next().await {
                if let Err(e) = res {
                    if let Some(mut upload) = self.upload.take() {
                        if let Err(abort_err) = upload.abort().await {
                            tracing::warn!(
                                error = %abort_err,
                                "Failed to abort multipart upload after part upload failure"
                            );
                        }
                    }
                    return Err(from_object_store_error(e));
                }
            }
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<FileMetadata> {
        let mut upload = self.upload.take().ok_or_else(|| {
            Error::new(
                ErrorKind::PreconditionFailed,
                "Writer has already been closed",
            )
        })?;

        // Flush any remaining buffered data, or emit an empty part if 0 parts
        // have been sent (S3 requires at least 1 part to complete a multipart upload).
        if !self.buffer.is_empty() || self.parts_submitted == 0 {
            let part_data = std::mem::take(&mut self.buffer).freeze();
            let part_fut = upload.put_part(PutPayload::from_bytes(part_data));
            self.tasks.push(part_fut);
            self.parts_submitted += 1;
        }

        // Await all in-flight part uploads; abort if any part fails.
        while let Some(res) = self.tasks.next().await {
            if let Err(e) = res {
                if let Err(abort_err) = upload.abort().await {
                    tracing::warn!(
                        error = %abort_err,
                        "Failed to abort multipart upload after part upload failure"
                    );
                }
                return Err(from_object_store_error(e));
            }
        }

        // Complete multipart upload; abort if S3 rejects completion.
        if let Err(e) = upload.complete().await {
            if let Err(abort_err) = upload.abort().await {
                tracing::warn!(
                    error = %abort_err,
                    "Failed to abort multipart upload after complete failure"
                );
            }
            return Err(from_object_store_error(e));
        }

        Ok(FileMetadata {
            size: self.bytes_written,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "object_store-s3")]
    fn make_s3_storage() -> ObjectStoreStorage {
        ObjectStoreStorage::S3(S3Storage {
            config: Arc::new(S3Config::default()),
            delete_batch_size: DEFAULT_DELETE_BATCH_SIZE,
            store_cache: Arc::new(DashMap::new()),
        })
    }

    #[test]
    fn test_parse_delete_batch_size_default() {
        let config = StorageConfig::new();
        assert_eq!(parse_delete_batch_size(&config), DEFAULT_DELETE_BATCH_SIZE);
    }

    #[test]
    fn test_parse_delete_batch_size_custom() {
        let mut props = std::collections::HashMap::new();
        props.insert(S3_DELETE_BATCH_SIZE.to_string(), "250".to_string());
        let config = StorageConfig::from_props(props);
        assert_eq!(parse_delete_batch_size(&config), 250);
    }

    #[test]
    fn test_parse_delete_batch_size_exceeds_max() {
        let mut props = std::collections::HashMap::new();
        props.insert(S3_DELETE_BATCH_SIZE.to_string(), "5000".to_string());
        let config = StorageConfig::from_props(props);
        assert_eq!(parse_delete_batch_size(&config), 5000);
    }

    #[test]
    fn test_parse_delete_batch_size_invalid_fallback() {
        let mut props = std::collections::HashMap::new();
        props.insert(S3_DELETE_BATCH_SIZE.to_string(), "invalid_number".to_string());
        let config = StorageConfig::from_props(props);
        assert_eq!(parse_delete_batch_size(&config), DEFAULT_DELETE_BATCH_SIZE);

        let mut props_zero = std::collections::HashMap::new();
        props_zero.insert(S3_DELETE_BATCH_SIZE.to_string(), "0".to_string());
        let config_zero = StorageConfig::from_props(props_zero);
        assert_eq!(parse_delete_batch_size(&config_zero), DEFAULT_DELETE_BATCH_SIZE);
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
    async fn test_reader_empty_range() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let path = ObjectStorePath::from("data/test.parquet");
        let reader = ObjectStoreReader { store, path };

        let result = reader.read(0..0).await.unwrap();
        assert!(result.is_empty());

        let result = reader.read(10..10).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_writer_already_closed_errors() {
        let mut writer = ObjectStoreWriter {
            upload: None,
            buffer: BytesMut::new(),
            tasks: FuturesUnordered::new(),
            parts_submitted: 0,
            bytes_written: 0,
        };
        let write_err = writer.write(Bytes::from_static(b"data")).await.unwrap_err();
        assert_eq!(write_err.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(write_err.message(), "Writer has already been closed");

        let close_res = writer.close().await;
        assert!(close_res.is_err());
        let close_err = close_res.err().unwrap();
        assert_eq!(close_err.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(close_err.message(), "Writer has already been closed");
    }

    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use object_store::PutResult;

    #[derive(Debug)]
    struct MockMultipartUpload {
        parts_count: Arc<AtomicUsize>,
        completed: Arc<AtomicBool>,
        aborted: Arc<AtomicBool>,
        fail_part: bool,
        fail_complete: bool,
    }

    #[async_trait]
    impl MultipartUpload for MockMultipartUpload {
        fn put_part(&mut self, _data: PutPayload) -> UploadPart {
            let count = self.parts_count.clone();
            let fail = self.fail_part;
            Box::pin(async move {
                if fail {
                    Err(object_store::Error::Generic {
                        store: "mock",
                        source: "part failed".into(),
                    })
                } else {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            })
        }

        async fn complete(&mut self) -> object_store::Result<PutResult> {
            if self.fail_complete {
                Err(object_store::Error::Generic {
                    store: "mock",
                    source: "complete failed".into(),
                })
            } else {
                self.completed.store(true, Ordering::SeqCst);
                Ok(PutResult {
                    e_tag: None,
                    version: None,
                })
            }
        }

        async fn abort(&mut self) -> object_store::Result<()> {
            self.aborted.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    fn make_mock_writer(
        fail_part: bool,
        fail_complete: bool,
    ) -> (
        ObjectStoreWriter,
        Arc<AtomicUsize>,
        Arc<AtomicBool>,
        Arc<AtomicBool>,
    ) {
        let parts_count = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicBool::new(false));
        let aborted = Arc::new(AtomicBool::new(false));

        let upload = Box::new(MockMultipartUpload {
            parts_count: parts_count.clone(),
            completed: completed.clone(),
            aborted: aborted.clone(),
            fail_part,
            fail_complete,
        });

        let writer = ObjectStoreWriter {
            upload: Some(upload),
            buffer: BytesMut::new(),
            tasks: FuturesUnordered::new(),
            parts_submitted: 0,
            bytes_written: 0,
        };

        (writer, parts_count, completed, aborted)
    }

    #[test]
    fn test_writer_drop_outside_tokio_warns_and_does_not_panic() {
        // A plain synchronous #[test] runs on an OS thread outside a Tokio runtime context.
        // Passing an active upload verifies that Handle::try_current() failing logs a warning without panicking.
        let (writer, _parts, completed, aborted) = make_mock_writer(false, false);
        drop(writer);

        assert!(!completed.load(Ordering::SeqCst));
        assert!(!aborted.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_writer_close_success() {
        let (mut writer, parts, completed, aborted) = make_mock_writer(false, false);
        writer
            .write(Bytes::from(vec![0u8; 6 * 1024 * 1024]))
            .await
            .unwrap();
        writer.close().await.unwrap();

        assert_eq!(parts.load(Ordering::SeqCst), 2); // 5 MiB + 1 MiB leftover part
        assert!(completed.load(Ordering::SeqCst));
        assert!(!aborted.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_writer_close_empty_file_emits_one_part() {
        let (mut writer, parts, completed, aborted) = make_mock_writer(false, false);
        // Closing a fresh writer with 0 bytes must emit exactly 1 part for S3 validity
        writer.close().await.unwrap();

        assert_eq!(parts.load(Ordering::SeqCst), 1);
        assert!(completed.load(Ordering::SeqCst));
        assert!(!aborted.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_writer_part_failure_aborts() {
        let (mut writer, _parts, completed, aborted) = make_mock_writer(true, false);
        writer
            .write(Bytes::from(vec![0u8; 6 * 1024 * 1024]))
            .await
            .unwrap();
        let close_res = writer.close().await;
        assert!(close_res.is_err());
        let err = close_res.err().unwrap();

        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(!completed.load(Ordering::SeqCst));
        assert!(
            aborted.load(Ordering::SeqCst),
            "abort() MUST be called when part upload fails"
        );
    }

    #[tokio::test]
    async fn test_writer_complete_failure_aborts() {
        let (mut writer, _parts, completed, aborted) = make_mock_writer(false, true);
        writer.write(Bytes::from_static(b"hello")).await.unwrap();
        let close_res = writer.close().await;
        assert!(close_res.is_err());
        let err = close_res.err().unwrap();

        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(!completed.load(Ordering::SeqCst));
        assert!(
            aborted.load(Ordering::SeqCst),
            "abort() MUST be called when complete() fails"
        );
    }

    #[tokio::test]
    async fn test_writer_bounded_concurrency_past_limit() {
        let (mut writer, parts, completed, aborted) = make_mock_writer(false, false);
        // Write 10 parts (50 MiB) which exceeds MAX_CONCURRENT_PART_UPLOADS (8)
        writer
            .write(Bytes::from(vec![0u8; 10 * 5 * 1024 * 1024]))
            .await
            .unwrap();
        writer.close().await.unwrap();

        assert_eq!(parts.load(Ordering::SeqCst), 10);
        assert!(completed.load(Ordering::SeqCst));
        assert!(!aborted.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_writer_fail_fast_during_write() {
        let (mut writer, _parts, completed, aborted) = make_mock_writer(true, false);
        // Writing past MAX_CONCURRENT_PART_UPLOADS triggers in-flight task awaiting inside write()
        let err = writer
            .write(Bytes::from(vec![0u8; 10 * 5 * 1024 * 1024]))
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(!completed.load(Ordering::SeqCst));
        assert!(
            aborted.load(Ordering::SeqCst),
            "abort() MUST be called immediately on write() failure"
        );
    }

    #[tokio::test]
    async fn test_writer_drop_aborts() {
        let (writer, _parts, completed, aborted) = make_mock_writer(false, false);
        drop(writer);

        // Allow background tokio task to run
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!completed.load(Ordering::SeqCst));
        assert!(
            aborted.load(Ordering::SeqCst),
            "abort() MUST be called on drop"
        );
    }
}
