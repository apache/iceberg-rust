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
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory,
};
#[cfg(feature = "object_store-s3")]
use iceberg::io::{S3_DELETE_BATCH_SIZE, S3_MULTIPART_PART_SIZE_BYTES, S3Config};
use iceberg::{Error, ErrorKind, Result};
use object_store::path::Path as ObjectStorePath;
use object_store::{MultipartUpload, ObjectStore, ObjectStoreExt, PutPayload};
#[cfg(feature = "object_store-s3")]
use s3::{S3RetrySettings, build_s3_store, parse_s3_url};
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;

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

/// Default batch size for S3 bulk deletions (matches Iceberg Java specification).
pub(crate) const DEFAULT_DELETE_BATCH_SIZE: usize = 250;
/// Maximum batch size allowed by the AWS S3 DeleteObjects API specification.
pub(crate) const S3_MAX_DELETE_BATCH_SIZE: usize = 1000;
/// Default part size for S3 multipart uploads (32 MiB, matches Iceberg Java specification).
pub(crate) const DEFAULT_PART_SIZE_BYTES: usize = 32 * 1024 * 1024;
/// Minimum part size allowed by the AWS S3 MultipartUpload API specification (5 MiB).
pub(crate) const MIN_PART_SIZE_BYTES: usize = 5 * 1024 * 1024;

fn parse_delete_batch_size(config: &StorageConfig) -> Result<usize> {
    let Some(val) = config.get(S3_DELETE_BATCH_SIZE) else {
        return Ok(DEFAULT_DELETE_BATCH_SIZE);
    };
    let batch_size = val.parse::<usize>().map_err(|e| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid {S3_DELETE_BATCH_SIZE}: {val}: {e}"),
        )
    })?;
    if batch_size == 0 {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid {S3_DELETE_BATCH_SIZE}: batch size must be greater than 0"),
        ));
    }
    Ok(batch_size.clamp(1, S3_MAX_DELETE_BATCH_SIZE))
}

fn default_delete_batch_size() -> usize {
    DEFAULT_DELETE_BATCH_SIZE
}

fn parse_part_size_bytes(config: &StorageConfig) -> Result<usize> {
    let Some(val) = config.get(S3_MULTIPART_PART_SIZE_BYTES) else {
        return Ok(DEFAULT_PART_SIZE_BYTES);
    };
    let part_size = val.parse::<usize>().map_err(|e| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid {S3_MULTIPART_PART_SIZE_BYTES}: {val}: {e}"),
        )
    })?;
    if part_size < MIN_PART_SIZE_BYTES {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Invalid {S3_MULTIPART_PART_SIZE_BYTES}: {part_size} bytes is below the S3 minimum part size of 5 MiB"
            ),
        ));
    }
    if part_size > i32::MAX as usize {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Invalid {S3_MULTIPART_PART_SIZE_BYTES}: {part_size} bytes is above the maximum allowed by Java spec"
            ),
        ));
    }
    Ok(part_size)
}

fn default_part_size_bytes() -> usize {
    DEFAULT_PART_SIZE_BYTES
}

/// Convert `object_store::ObjectMeta` into `iceberg::io::FileMetadata`.
fn to_file_metadata(meta: object_store::ObjectMeta) -> FileMetadata {
    FileMetadata { size: meta.size }
}

/// `object_store`-based storage factory.
///
/// Use this factory with `FileIOBuilder::new(factory)` to create FileIO instances
/// backed by the `object_store` crate.
#[non_exhaustive]
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
                s3::validate_s3_config(&s3_config)?;
                let delete_batch_size = parse_delete_batch_size(config)?;
                let part_size_bytes = parse_part_size_bytes(config)?;
                let retry_settings = S3RetrySettings::parse(config)?;
                Ok(Arc::new(ObjectStoreStorage::S3(S3Storage {
                    config: Arc::new(s3_config),
                    delete_batch_size,
                    part_size_bytes,
                    retry_settings,
                    store_cache: Arc::new(DashMap::new()),
                })))
            }
        }
    }
}

type StoreCache = Arc<DashMap<String, Arc<dyn ObjectStore>>>;

/// `object_store` S3 storage state.
#[derive(Clone, Serialize, Deserialize)]
pub struct S3Storage {
    config: Arc<S3Config>,
    #[serde(default = "default_delete_batch_size")]
    delete_batch_size: usize,
    #[serde(default = "default_part_size_bytes")]
    part_size_bytes: usize,
    #[serde(default)]
    retry_settings: S3RetrySettings,
    #[serde(skip, default)]
    store_cache: StoreCache,
}

impl std::fmt::Debug for S3Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Storage")
            .field("endpoint", &self.config.endpoint)
            .field("region", &self.config.region)
            .field("access_key_id", &self.config.access_key_id)
            .field(
                "secret_access_key",
                &self.config.secret_access_key.as_ref().map(|_| "[REDACTED]"),
            )
            .field(
                "session_token",
                &self.config.session_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field(
                "server_side_encryption_customer_key",
                &self
                    .config
                    .server_side_encryption_customer_key
                    .as_ref()
                    .map(|_| "[REDACTED]"),
            )
            .field("delete_batch_size", &self.delete_batch_size)
            .field("part_size_bytes", &self.part_size_bytes)
            .finish_non_exhaustive()
    }
}

/// `object_store`-based storage implementation.
///
/// Stores are cached per bucket to avoid rebuilding the client on every operation.
#[non_exhaustive]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ObjectStoreStorage {
    /// S3 storage variant.
    #[cfg(feature = "object_store-s3")]
    S3(S3Storage),
}

#[derive(Debug)]
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
            ObjectStoreStorage::S3(s3) => s3.delete_batch_size.clamp(1, S3_MAX_DELETE_BATCH_SIZE),
        }
    }

    fn part_size_bytes(&self) -> usize {
        match self {
            #[cfg(feature = "object_store-s3")]
            ObjectStoreStorage::S3(s3) => s3.part_size_bytes.max(MIN_PART_SIZE_BYTES),
        }
    }

    /// Get or create a cached store and extract the relative `ObjectStorePath`.
    fn get_store_and_path(&self, path: &str) -> Result<StoreAndPath> {
        match self {
            #[cfg(feature = "object_store-s3")]
            ObjectStoreStorage::S3(s3) => {
                let parsed = parse_s3_url(path)?;

                // Reject paths that cannot be represented losslessly by object_store::path::Path
                // (e.g. trailing slashes alias non-slash keys, and '//' creates empty segments).
                if parsed.relative.starts_with('/')
                    || parsed.relative.ends_with('/')
                    || parsed.relative.contains("//")
                {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Path cannot be represented losslessly by object_store: '{path}'. Empty path segments ('//') and trailing slashes are not supported.",
                        ),
                    ));
                }

                let store = s3
                    .store_cache
                    .entry(parsed.bucket.clone())
                    .or_try_insert_with(|| {
                        build_s3_store(&s3.config, &s3.retry_settings, &parsed.bucket)
                    })?
                    .value()
                    .clone();

                let object_path =
                    ObjectStorePath::parse(parsed.relative.as_str()).map_err(|e| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!("Invalid relative path: {}", parsed.relative),
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
        Ok(Box::new(ObjectStoreWriter::new(
            target.store,
            target.path,
            self.part_size_bytes(),
        )))
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

/// Default maximum concurrent in-flight part uploads.
const MAX_CONCURRENT_PART_UPLOADS: usize = 8;

/// Writer that implements `FileWrite` using `object_store` multipart upload.
struct ObjectStoreWriter {
    store: Arc<dyn ObjectStore>,
    path: ObjectStorePath,
    upload: Option<Box<dyn MultipartUpload>>,
    buffer: BytesMut,
    tasks: JoinSet<object_store::Result<()>>,
    parts_submitted: usize,
    bytes_written: u64,
    part_size_bytes: usize,
    closed: bool,
    failed: bool,
}

impl ObjectStoreWriter {
    fn new(store: Arc<dyn ObjectStore>, path: ObjectStorePath, part_size_bytes: usize) -> Self {
        Self {
            store,
            path,
            upload: None,
            buffer: BytesMut::new(),
            tasks: JoinSet::new(),
            parts_submitted: 0,
            bytes_written: 0,
            part_size_bytes,
            closed: false,
            failed: false,
        }
    }

    #[cfg(test)]
    fn with_mock_upload(upload: Box<dyn MultipartUpload>, part_size_bytes: usize) -> Self {
        Self {
            store: Arc::new(object_store::memory::InMemory::new()),
            path: ObjectStorePath::from("mock_test"),
            upload: Some(upload),
            buffer: BytesMut::new(),
            tasks: JoinSet::new(),
            parts_submitted: 0,
            bytes_written: 0,
            part_size_bytes,
            closed: false,
            failed: false,
        }
    }

    /// Aborts all in-flight task uploads, drains the join set to ensure no
    /// requests race the abort call, and then aborts the multipart upload session.
    async fn drain_and_abort(
        tasks: &mut JoinSet<object_store::Result<()>>,
        upload: &mut Box<dyn MultipartUpload>,
        context: &'static str,
    ) {
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}

        if let Err(abort_err) = upload.abort().await {
            tracing::warn!(
                error = %abort_err,
                "Failed to abort multipart upload after {context}"
            );
        }
    }

    /// Best-effort abort on upload error, converting the underlying
    /// object_store error to an iceberg Error while logging abort failures.
    async fn abort_and_wrap(
        tasks: &mut JoinSet<object_store::Result<()>>,
        upload: &mut Box<dyn MultipartUpload>,
        e: object_store::Error,
        context: &'static str,
    ) -> Error {
        Self::drain_and_abort(tasks, upload, context).await;
        from_object_store_error(e)
    }

    /// Drains in-flight tasks down below `MAX_CONCURRENT_PART_UPLOADS`.
    async fn drain_capacity(
        tasks: &mut JoinSet<object_store::Result<()>>,
        upload: &mut Box<dyn MultipartUpload>,
    ) -> Result<()> {
        while tasks.len() >= MAX_CONCURRENT_PART_UPLOADS {
            if let Some(res) = tasks.join_next().await {
                match res {
                    Ok(Err(e)) => {
                        return Err(
                            Self::abort_and_wrap(tasks, upload, e, "part upload failure").await,
                        );
                    }
                    Err(join_err) => {
                        Self::drain_and_abort(tasks, upload, "task panic").await;
                        return Err(
                            Error::new(ErrorKind::Unexpected, "Part upload task panicked")
                                .with_source(join_err),
                        );
                    }
                    Ok(Ok(())) => {}
                }
            }
        }
        Ok(())
    }

    fn check_tokio_runtime() -> Result<()> {
        tokio::runtime::Handle::try_current()
            .map(|_| ())
            .map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Tokio runtime not found for async task execution",
                )
                .with_source(e)
            })
    }

    /// Flushes any buffered bytes as an in-flight part upload to S3,
    /// throttling concurrency so that at most `MAX_CONCURRENT_PART_UPLOADS`
    /// uploads are in flight at any given time.
    async fn flush_buffer(
        buffer: &mut BytesMut,
        tasks: &mut JoinSet<object_store::Result<()>>,
        parts_submitted: &mut usize,
        upload: &mut Box<dyn MultipartUpload>,
    ) -> Result<()> {
        if !buffer.is_empty() {
            Self::drain_capacity(tasks, upload).await?;

            Self::check_tokio_runtime()?;
            let part_data = std::mem::take(buffer).freeze();
            let part_fut = upload.put_part(PutPayload::from_bytes(part_data));
            tasks.spawn(part_fut);
            *parts_submitted += 1;
        }
        Ok(())
    }

    /// Accumulates bytes into `buffer`, flushing parts when the configured part size is reached.
    async fn append_bytes(
        buffer: &mut BytesMut,
        tasks: &mut JoinSet<object_store::Result<()>>,
        parts_submitted: &mut usize,
        part_size_bytes: usize,
        mut bs: Bytes,
        upload: &mut Box<dyn MultipartUpload>,
    ) -> Result<()> {
        while !bs.is_empty() {
            let remaining = part_size_bytes.saturating_sub(buffer.len());
            if remaining == 0 {
                Self::flush_buffer(buffer, tasks, parts_submitted, upload).await?;
                continue;
            }

            if bs.len() < remaining {
                buffer.extend_from_slice(&bs);
                return Ok(());
            }
            let chunk = bs.split_to(remaining);
            buffer.extend_from_slice(&chunk);
            Self::flush_buffer(buffer, tasks, parts_submitted, upload).await?;
        }
        Ok(())
    }
}

impl Drop for ObjectStoreWriter {
    fn drop(&mut self) {
        if let Some(mut upload) = self.upload.take() {
            let mut tasks = std::mem::take(&mut self.tasks);
            if Self::check_tokio_runtime().is_ok() {
                tokio::runtime::Handle::current().spawn(async move {
                    Self::drain_and_abort(&mut tasks, &mut upload, "drop cleanup").await;
                });
            } else {
                tasks.abort_all();
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
        if self.closed {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "Writer has already been closed",
            ));
        }
        if self.failed {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "Writer has encountered an unrecoverable failure",
            ));
        }
        self.bytes_written += bs.len() as u64;

        if self.upload.is_none() {
            if self.buffer.len() + bs.len() < self.part_size_bytes {
                self.buffer.extend_from_slice(&bs);
                return Ok(());
            }

            // Exceeded threshold: initialize multipart upload session
            let upload = match self.store.put_multipart(&self.path).await {
                Ok(u) => u,
                Err(e) => {
                    self.failed = true;
                    return Err(from_object_store_error(e));
                }
            };
            self.upload = Some(upload);
        }

        let upload = self.upload.as_mut().unwrap();
        if let Err(e) = Self::append_bytes(
            &mut self.buffer,
            &mut self.tasks,
            &mut self.parts_submitted,
            self.part_size_bytes,
            bs,
            upload,
        )
        .await
        {
            self.failed = true;
            let _ = self.upload.take();
            return Err(e);
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<FileMetadata> {
        if self.closed {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "Writer has already been closed",
            ));
        }
        if self.failed {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "Writer has encountered an unrecoverable failure",
            ));
        }
        self.closed = true;

        if self.upload.is_none() {
            let payload = PutPayload::from_bytes(std::mem::take(&mut self.buffer).freeze());
            self.store
                .put(&self.path, payload)
                .await
                .map_err(from_object_store_error)?;
            return Ok(FileMetadata {
                size: self.bytes_written,
            });
        }

        let upload = self.upload.as_mut().unwrap();

        // Flush any remaining buffered data, or emit an empty part if 0 parts
        // have been sent (S3 requires at least 1 part to complete a multipart upload).
        if !self.buffer.is_empty() || self.parts_submitted == 0 {
            if let Err(e) = Self::drain_capacity(&mut self.tasks, upload).await {
                self.failed = true;
                let _ = self.upload.take();
                return Err(e);
            }
            if let Err(e) = Self::check_tokio_runtime() {
                self.failed = true;
                let _ = self.upload.take();
                return Err(e);
            }
            let part_data = std::mem::take(&mut self.buffer).freeze();
            let part_fut = upload.put_part(PutPayload::from_bytes(part_data));
            self.tasks.spawn(part_fut);
            self.parts_submitted += 1;
        }

        // Await all in-flight part uploads; abort if any part fails.
        while let Some(res) = self.tasks.join_next().await {
            match res {
                Ok(Err(e)) => {
                    self.failed = true;
                    let err =
                        Self::abort_and_wrap(&mut self.tasks, upload, e, "part upload failure")
                            .await;
                    let _ = self.upload.take();
                    return Err(err);
                }
                Err(join_err) => {
                    self.failed = true;
                    Self::drain_and_abort(&mut self.tasks, upload, "task panic").await;
                    let _ = self.upload.take();
                    return Err(
                        Error::new(ErrorKind::Unexpected, "Part upload task panicked")
                            .with_source(join_err),
                    );
                }
                Ok(Ok(())) => {}
            }
        }

        // Complete multipart upload; abort if S3 rejects completion.
        if let Err(e) = upload.complete().await {
            self.failed = true;
            let err = Self::abort_and_wrap(&mut self.tasks, upload, e, "complete failure").await;
            let _ = self.upload.take();
            return Err(err);
        }

        // Successfully completed upload; clear so Drop won't attempt abort.
        let _ = self.upload.take();

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
            part_size_bytes: DEFAULT_PART_SIZE_BYTES,
            retry_settings: S3RetrySettings::default(),
            store_cache: Arc::new(DashMap::new()),
        })
    }

    #[cfg(feature = "object_store-s3")]
    #[test]
    fn test_storage_factory_eagerly_rejects_unsupported_config() {
        let factory = ObjectStoreStorageFactory::S3;

        let mut role_props = std::collections::HashMap::new();
        role_props.insert(
            "client.assume-role.arn".to_string(),
            "arn:aws:iam::123456789012:role/test".to_string(),
        );
        let err = factory
            .build(&StorageConfig::from_props(role_props))
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::FeatureUnsupported);

        let mut ec2_props = std::collections::HashMap::new();
        ec2_props.insert("s3.disable-ec2-metadata".to_string(), "true".to_string());
        let err = factory
            .build(&StorageConfig::from_props(ec2_props))
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::FeatureUnsupported);
    }

    #[cfg(feature = "object_store-s3")]
    #[test]
    fn test_s3_storage_debug_redacts_credentials() {
        let storage = ObjectStoreStorage::S3(S3Storage {
            config: Arc::new(
                S3Config::builder()
                    .endpoint("http://localhost:9000")
                    .region("us-east-1")
                    .access_key_id("my_access_key")
                    .secret_access_key("super_secret_key_123")
                    .session_token("session_token_xyz")
                    .server_side_encryption_customer_key("ssec_key_456")
                    .build(),
            ),
            delete_batch_size: DEFAULT_DELETE_BATCH_SIZE,
            part_size_bytes: DEFAULT_PART_SIZE_BYTES,
            retry_settings: S3RetrySettings::default(),
            store_cache: Arc::new(DashMap::new()),
        });

        let debug_str = format!("{storage:?}");
        assert!(!debug_str.contains("super_secret_key_123"));
        assert!(!debug_str.contains("session_token_xyz"));
        assert!(!debug_str.contains("ssec_key_456"));
        assert!(debug_str.contains("[REDACTED]"));
        assert!(debug_str.contains("my_access_key"));
        assert!(debug_str.contains("us-east-1"));
        assert!(debug_str.contains("http://localhost:9000"));
    }

    #[test]
    fn test_parse_part_size_bytes_default() {
        let config = StorageConfig::new();
        assert_eq!(
            parse_part_size_bytes(&config).unwrap(),
            DEFAULT_PART_SIZE_BYTES
        );
    }

    #[test]
    fn test_parse_part_size_bytes_custom() {
        let mut props = std::collections::HashMap::new();
        props.insert(
            S3_MULTIPART_PART_SIZE_BYTES.to_string(),
            (64 * 1024 * 1024).to_string(),
        );
        let config = StorageConfig::from_props(props);
        assert_eq!(parse_part_size_bytes(&config).unwrap(), 64 * 1024 * 1024);
    }

    #[test]
    fn test_parse_part_size_bytes_below_minimum_rejected() {
        let mut props = std::collections::HashMap::new();
        props.insert(
            S3_MULTIPART_PART_SIZE_BYTES.to_string(),
            "1048576".to_string(), // 1 MiB (< 5 MiB)
        );
        let config = StorageConfig::from_props(props);
        let err = parse_part_size_bytes(&config).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    #[test]
    fn test_parse_delete_batch_size_default() {
        let config = StorageConfig::new();
        assert_eq!(
            parse_delete_batch_size(&config).unwrap(),
            DEFAULT_DELETE_BATCH_SIZE
        );
    }

    #[test]
    fn test_parse_delete_batch_size_custom() {
        let mut props = std::collections::HashMap::new();
        props.insert(S3_DELETE_BATCH_SIZE.to_string(), "500".to_string());
        let config = StorageConfig::from_props(props);
        assert_eq!(parse_delete_batch_size(&config).unwrap(), 500);
    }

    #[test]
    fn test_parse_delete_batch_size_exceeds_max() {
        let mut props = std::collections::HashMap::new();
        props.insert(S3_DELETE_BATCH_SIZE.to_string(), "5000".to_string());
        let config = StorageConfig::from_props(props);
        assert_eq!(
            parse_delete_batch_size(&config).unwrap(),
            S3_MAX_DELETE_BATCH_SIZE
        );
    }

    #[test]
    fn test_parse_delete_batch_size_invalid_rejected() {
        let mut props = std::collections::HashMap::new();
        props.insert(
            S3_DELETE_BATCH_SIZE.to_string(),
            "invalid_number".to_string(),
        );
        let config = StorageConfig::from_props(props);
        let err = parse_delete_batch_size(&config).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);

        let mut props_zero = std::collections::HashMap::new();
        props_zero.insert(S3_DELETE_BATCH_SIZE.to_string(), "0".to_string());
        let config_zero = StorageConfig::from_props(props_zero);
        let err_zero = parse_delete_batch_size(&config_zero).unwrap_err();
        assert_eq!(err_zero.kind(), ErrorKind::DataInvalid);
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

        let target_spaces = storage
            .get_store_and_path("s3://my-bucket/data%20dir/file.parquet")
            .unwrap();
        assert_eq!(target_spaces.path.as_ref(), "data%20dir/file.parquet");

        let target_partition = storage
            .get_store_and_path("s3://my-bucket/dt=a%2Fb/file.parquet")
            .unwrap();
        assert_eq!(target_partition.path.as_ref(), "dt=a%2Fb/file.parquet");
    }

    #[cfg(feature = "object_store-s3")]
    #[tokio::test]
    async fn test_storage_layer_rejects_unrepresentable_paths() {
        let storage = make_s3_storage();

        // Trailing slash aliasing: in S3 'dir/' != 'dir', but Path::parse strips trailing slashes
        let err = storage.exists("s3://my-bucket/dir/").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("losslessly"));

        let err = storage.delete("s3://my-bucket/dir/").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);

        // Consecutive slashes ('//')
        let err = storage
            .read("s3://my-bucket/data//file.parquet")
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);

        // Leading double slash after bucket
        let res = storage.metadata("s3://my-bucket//file.parquet").await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().kind(), ErrorKind::DataInvalid);
    }

    #[cfg(feature = "object_store-s3")]
    #[test]
    fn test_storage_deserialization_clamps_delete_batch_size() {
        let storage = make_s3_storage();
        let mut val = serde_json::to_value(&storage).unwrap();
        val["S3"]["delete_batch_size"] = serde_json::json!(0);
        let storage_zero: ObjectStoreStorage = serde_json::from_value(val.clone()).unwrap();
        assert_eq!(storage_zero.delete_batch_size(), 1);

        val["S3"]["delete_batch_size"] = serde_json::json!(5000);
        let storage_large: ObjectStoreStorage = serde_json::from_value(val).unwrap();
        assert_eq!(storage_large.delete_batch_size(), S3_MAX_DELETE_BATCH_SIZE);
    }

    #[cfg(feature = "object_store-s3")]
    #[test]
    fn test_storage_deserialization_clamps_part_size() {
        let storage = make_s3_storage();
        let mut val = serde_json::to_value(&storage).unwrap();
        val["S3"]["part_size_bytes"] = serde_json::json!(1024);
        let storage_small: ObjectStoreStorage = serde_json::from_value(val).unwrap();
        assert_eq!(storage_small.part_size_bytes(), MIN_PART_SIZE_BYTES);
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
        let mut writer = ObjectStoreWriter::with_mock_upload(
            Box::new(MockMultipartUpload {
                parts_count: Arc::new(AtomicUsize::new(0)),
                completed: Arc::new(AtomicBool::new(false)),
                abort_count: Arc::new(AtomicUsize::new(0)),
                fail_part: false,
                fail_complete: false,
            }),
            MIN_PART_SIZE_BYTES,
        );
        writer.closed = true;
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

    use object_store::{PutResult, UploadPart};

    #[derive(Debug)]
    struct MockMultipartUpload {
        parts_count: Arc<AtomicUsize>,
        completed: Arc<AtomicBool>,
        abort_count: Arc<AtomicUsize>,
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
                    extensions: Default::default(),
                })
            }
        }

        async fn abort(&mut self) -> object_store::Result<()> {
            self.abort_count.fetch_add(1, Ordering::SeqCst);
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
        Arc<AtomicUsize>,
    ) {
        let parts_count = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicBool::new(false));
        let abort_count = Arc::new(AtomicUsize::new(0));

        let upload = Box::new(MockMultipartUpload {
            parts_count: parts_count.clone(),
            completed: completed.clone(),
            abort_count: abort_count.clone(),
            fail_part,
            fail_complete,
        });

        let writer = ObjectStoreWriter::with_mock_upload(upload, MIN_PART_SIZE_BYTES);

        (writer, parts_count, completed, abort_count)
    }

    #[test]
    fn test_writer_drop_outside_tokio_warns_and_does_not_panic() {
        // A plain synchronous #[test] runs on an OS thread outside a Tokio runtime context.
        // Passing an active upload verifies that Handle::try_current() failing logs a warning without panicking.
        let (writer, _parts, completed, abort_count) = make_mock_writer(false, false);
        drop(writer);

        assert!(!completed.load(Ordering::SeqCst));
        assert_eq!(abort_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_writer_close_success() {
        let (mut writer, parts, completed, abort_count) = make_mock_writer(false, false);
        writer
            .write(Bytes::from(vec![0u8; 6 * 1024 * 1024]))
            .await
            .unwrap();
        writer.close().await.unwrap();

        assert_eq!(parts.load(Ordering::SeqCst), 2); // 5 MiB + 1 MiB leftover part
        assert!(completed.load(Ordering::SeqCst));
        assert_eq!(abort_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_writer_close_empty_file_emits_one_part() {
        let (mut writer, parts, completed, abort_count) = make_mock_writer(false, false);
        // Closing a fresh writer with 0 bytes must emit exactly 1 part for S3 validity
        writer.close().await.unwrap();

        assert_eq!(parts.load(Ordering::SeqCst), 1);
        assert!(completed.load(Ordering::SeqCst));
        assert_eq!(abort_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_writer_empty_file_uses_single_put() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let path = ObjectStorePath::from("empty.parquet");
        let mut writer = ObjectStoreWriter::new(store.clone(), path.clone(), MIN_PART_SIZE_BYTES);

        let meta = writer.close().await.unwrap();
        assert_eq!(meta.size, 0);

        let head = store.head(&path).await.unwrap();
        assert_eq!(head.size, 0);
    }

    #[tokio::test]
    async fn test_writer_small_file_uses_single_put() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let path = ObjectStorePath::from("small.parquet");
        let mut writer = ObjectStoreWriter::new(store.clone(), path.clone(), MIN_PART_SIZE_BYTES);

        writer
            .write(Bytes::from_static(b"hello world"))
            .await
            .unwrap();
        assert!(writer.upload.is_none());

        let meta = writer.close().await.unwrap();
        assert_eq!(meta.size, 11);

        let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"hello world"));
    }

    #[tokio::test]
    async fn test_writer_part_failure_aborts() {
        let (mut writer, _parts, completed, abort_count) = make_mock_writer(true, false);
        writer
            .write(Bytes::from(vec![0u8; 6 * 1024 * 1024]))
            .await
            .unwrap();
        let close_res = writer.close().await;
        assert!(close_res.is_err());
        let err = close_res.err().unwrap();

        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(!completed.load(Ordering::SeqCst));
        drop(writer);
        tokio::task::yield_now().await;
        assert_eq!(
            abort_count.load(Ordering::SeqCst),
            1,
            "abort() MUST be called exactly once when part upload fails"
        );
    }

    #[tokio::test]
    async fn test_writer_complete_failure_aborts() {
        let (mut writer, _parts, completed, abort_count) = make_mock_writer(false, true);
        writer.write(Bytes::from_static(b"hello")).await.unwrap();
        let close_res = writer.close().await;
        assert!(close_res.is_err());
        let err = close_res.err().unwrap();

        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(!completed.load(Ordering::SeqCst));

        // Subsequent close() must fail fast with PreconditionFailed
        let second_close_res = writer.close().await;
        assert!(second_close_res.is_err());
        assert_eq!(
            second_close_res.err().unwrap().kind(),
            ErrorKind::PreconditionFailed
        );

        drop(writer);
        tokio::task::yield_now().await;
        assert_eq!(
            abort_count.load(Ordering::SeqCst),
            1,
            "abort() MUST be called exactly once when complete() fails"
        );
    }

    #[tokio::test]
    async fn test_writer_write_failure_rejects_subsequent_close() {
        let (mut writer, _parts, _completed, abort_count) = make_mock_writer(true, false);

        // Writing past MAX_CONCURRENT_PART_UPLOADS triggers in-flight task awaiting inside write()
        let write_err = writer
            .write(Bytes::from(vec![0u8; 10 * 5 * 1024 * 1024]))
            .await
            .unwrap_err();
        assert_eq!(write_err.kind(), ErrorKind::Unexpected);
        assert_eq!(abort_count.load(Ordering::SeqCst), 1);

        // Subsequent close() must reject with PreconditionFailed rather than fall back to single-PUT
        let close_res = writer.close().await;
        assert!(close_res.is_err());
        let close_err = close_res.err().unwrap();
        assert_eq!(close_err.kind(), ErrorKind::PreconditionFailed);
        assert!(close_err.message().contains("unrecoverable failure"));

        // Subsequent write() must also reject with PreconditionFailed
        let post_write_err = writer
            .write(Bytes::from_static(b"extra"))
            .await
            .unwrap_err();
        assert_eq!(post_write_err.kind(), ErrorKind::PreconditionFailed);
    }

    #[tokio::test]
    async fn test_writer_bounded_concurrency_past_limit() {
        let (mut writer, parts, completed, abort_count) = make_mock_writer(false, false);
        // Write 10 parts (50 MiB) which exceeds MAX_CONCURRENT_PART_UPLOADS (8)
        writer
            .write(Bytes::from(vec![0u8; 10 * 5 * 1024 * 1024]))
            .await
            .unwrap();
        writer.close().await.unwrap();

        assert_eq!(parts.load(Ordering::SeqCst), 10);
        assert!(completed.load(Ordering::SeqCst));
        assert_eq!(abort_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_writer_fail_fast_during_write() {
        let (mut writer, _parts, completed, abort_count) = make_mock_writer(true, false);
        // Writing past MAX_CONCURRENT_PART_UPLOADS triggers in-flight task awaiting inside write()
        let err = writer
            .write(Bytes::from(vec![0u8; 10 * 5 * 1024 * 1024]))
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(!completed.load(Ordering::SeqCst));
        drop(writer);
        tokio::task::yield_now().await;
        assert_eq!(
            abort_count.load(Ordering::SeqCst),
            1,
            "abort() MUST be called exactly once immediately on write() failure"
        );
    }

    #[tokio::test]
    async fn test_writer_drop_aborts() {
        let (writer, _parts, completed, abort_count) = make_mock_writer(false, false);
        drop(writer);

        // Allow background tokio task to run
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!completed.load(Ordering::SeqCst));
        assert_eq!(
            abort_count.load(Ordering::SeqCst),
            1,
            "abort() MUST be called on drop"
        );
    }

    #[tokio::test]
    async fn test_writer_bounded_concurrency_in_flight_cap() {
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));

        #[derive(Debug)]
        struct TrackingMockUpload {
            in_flight: Arc<AtomicUsize>,
            max_in_flight: Arc<AtomicUsize>,
        }

        #[async_trait]
        impl MultipartUpload for TrackingMockUpload {
            fn put_part(&mut self, _data: PutPayload) -> UploadPart {
                let in_flight = self.in_flight.clone();
                let max_in_flight = self.max_in_flight.clone();
                Box::pin(async move {
                    let current = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    max_in_flight.fetch_max(current, Ordering::SeqCst);
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                    Ok(())
                })
            }

            async fn complete(&mut self) -> object_store::Result<PutResult> {
                Ok(PutResult {
                    e_tag: None,
                    version: None,
                    extensions: Default::default(),
                })
            }

            async fn abort(&mut self) -> object_store::Result<()> {
                Ok(())
            }
        }

        let upload = Box::new(TrackingMockUpload {
            in_flight: in_flight.clone(),
            max_in_flight: max_in_flight.clone(),
        });

        let mut writer = ObjectStoreWriter::with_mock_upload(upload, MIN_PART_SIZE_BYTES);

        // Write 20 parts (100 MiB) in a single write call
        writer
            .write(Bytes::from(vec![0u8; 20 * 5 * 1024 * 1024]))
            .await
            .unwrap();
        writer.close().await.unwrap();

        assert!(
            max_in_flight.load(Ordering::SeqCst) <= MAX_CONCURRENT_PART_UPLOADS,
            "max concurrent in-flight uploads exceeded {} (was {})",
            MAX_CONCURRENT_PART_UPLOADS,
            max_in_flight.load(Ordering::SeqCst)
        );
    }

    #[derive(Debug)]
    struct PanickingMockUpload {
        abort_count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl MultipartUpload for PanickingMockUpload {
        fn put_part(&mut self, _data: PutPayload) -> UploadPart {
            Box::pin(async move {
                panic!("simulated part upload panic");
            })
        }

        async fn complete(&mut self) -> object_store::Result<PutResult> {
            Ok(PutResult {
                e_tag: None,
                version: None,
                extensions: Default::default(),
            })
        }

        async fn abort(&mut self) -> object_store::Result<()> {
            self.abort_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_writer_task_panic_in_close_aborts() {
        let abort_count = Arc::new(AtomicUsize::new(0));
        let upload = Box::new(PanickingMockUpload {
            abort_count: abort_count.clone(),
        });
        let mut writer = ObjectStoreWriter::with_mock_upload(upload, MIN_PART_SIZE_BYTES);
        writer
            .write(Bytes::from(vec![0u8; 6 * 1024 * 1024]))
            .await
            .unwrap();
        let close_res = writer.close().await;
        assert!(close_res.is_err());
        let err = close_res.err().unwrap();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert_eq!(err.message(), "Part upload task panicked");
        drop(writer);
        tokio::task::yield_now().await;
        assert_eq!(
            abort_count.load(Ordering::SeqCst),
            1,
            "abort() MUST be called exactly once when part upload task panics during close"
        );
    }

    #[tokio::test]
    async fn test_writer_task_panic_in_write_drain_aborts() {
        let abort_count = Arc::new(AtomicUsize::new(0));
        let upload = Box::new(PanickingMockUpload {
            abort_count: abort_count.clone(),
        });
        let mut writer = ObjectStoreWriter::with_mock_upload(upload, MIN_PART_SIZE_BYTES);
        // Writing 10 parts (> MAX_CONCURRENT_PART_UPLOADS of 8) forces drain_capacity inside write()
        let write_res = writer
            .write(Bytes::from(vec![0u8; 10 * 5 * 1024 * 1024]))
            .await;
        assert!(write_res.is_err());
        let err = write_res.err().unwrap();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert_eq!(err.message(), "Part upload task panicked");
        drop(writer);
        tokio::task::yield_now().await;
        assert_eq!(
            abort_count.load(Ordering::SeqCst),
            1,
            "abort() MUST be called exactly once when part upload task panics during write drain"
        );
    }

    #[test]
    fn test_runtime_guard_missing() {
        let err = ObjectStoreWriter::check_tokio_runtime().unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
    }

    #[tokio::test]
    async fn test_runtime_guard_active() {
        assert!(ObjectStoreWriter::check_tokio_runtime().is_ok());
    }

    #[tokio::test]
    async fn test_writer_part_failure_drains_siblings_before_abort() {
        #[derive(Debug)]
        struct OrderingMockUpload {
            active_tasks: Arc<AtomicUsize>,
            aborted_while_tasks_active: Arc<AtomicBool>,
            part_index: Arc<AtomicUsize>,
        }

        #[async_trait]
        impl MultipartUpload for OrderingMockUpload {
            fn put_part(&mut self, _data: PutPayload) -> UploadPart {
                let idx = self.part_index.fetch_add(1, Ordering::SeqCst);
                let active = self.active_tasks.clone();
                Box::pin(async move {
                    active.fetch_add(1, Ordering::SeqCst);
                    struct TaskGuard(Arc<AtomicUsize>);
                    impl Drop for TaskGuard {
                        fn drop(&mut self) {
                            self.0.fetch_sub(1, Ordering::SeqCst);
                        }
                    }
                    let _guard = TaskGuard(active);
                    if idx == 0 {
                        Err(object_store::Error::Generic {
                            store: "mock",
                            source: "part 0 failed".into(),
                        })
                    } else {
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                        Ok(())
                    }
                })
            }

            async fn complete(&mut self) -> object_store::Result<PutResult> {
                Ok(PutResult {
                    e_tag: None,
                    version: None,
                    extensions: Default::default(),
                })
            }

            async fn abort(&mut self) -> object_store::Result<()> {
                if self.active_tasks.load(Ordering::SeqCst) > 0 {
                    self.aborted_while_tasks_active
                        .store(true, Ordering::SeqCst);
                }
                Ok(())
            }
        }

        let active_tasks = Arc::new(AtomicUsize::new(0));
        let aborted_while_tasks_active = Arc::new(AtomicBool::new(false));
        let upload = Box::new(OrderingMockUpload {
            active_tasks: active_tasks.clone(),
            aborted_while_tasks_active: aborted_while_tasks_active.clone(),
            part_index: Arc::new(AtomicUsize::new(0)),
        });

        let mut writer = ObjectStoreWriter::with_mock_upload(upload, MIN_PART_SIZE_BYTES);

        // Write 2 parts: part 0 fails, sibling part 1 is live in-flight
        let _ = writer.write(Bytes::from(vec![0u8; 11 * 1024 * 1024])).await;
        let _ = writer.close().await;

        assert!(
            !aborted_while_tasks_active.load(Ordering::SeqCst),
            "upload.abort() MUST NOT be called while sibling tasks are still active"
        );
        assert_eq!(active_tasks.load(Ordering::SeqCst), 0);
    }
}
