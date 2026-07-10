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

use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use futures::{Stream, StreamExt, stream};

use super::storage::{
    LocalFsStorageFactory, MemoryStorageFactory, Storage, StorageConfig, StorageFactory,
};
use crate::Result;

/// FileIO implementation, used to manipulate files in underlying storage.
///
/// FileIO wraps a `dyn Storage` with lazy initialization via `StorageFactory`.
/// The storage is created on first use and cached for subsequent operations.
///
/// # Note
///
/// All paths passed to `FileIO` must be absolute paths starting with the scheme string
/// appropriate for the storage backend being used.
///
/// This crate provides native support for local filesystem (`file://`) and
/// memory (`memory://`) storage. For extensive storage backend support (S3, GCS,
/// OSS, Azure, etc.), use the
/// [`iceberg-storage-opendal`](https://crates.io/crates/iceberg-storage-opendal) crate.
///
/// # Example
///
/// ```rust,ignore
/// use iceberg::io::{FileIO, FileIOBuilder};
/// use iceberg::io::{LocalFsStorageFactory, MemoryStorageFactory};
/// use std::sync::Arc;
///
/// // Create FileIO with memory storage for testing
/// let file_io = FileIO::new_with_memory();
///
/// // Create FileIO with local filesystem storage
/// let file_io = FileIO::new_with_fs();
///
/// // Create FileIO with custom factory
/// let file_io = FileIOBuilder::new(Arc::new(LocalFsStorageFactory))
///     .with_prop("key", "value")
///     .build();
/// ```
#[derive(Clone, Debug)]
pub struct FileIO {
    /// Storage configuration containing properties
    config: StorageConfig,
    /// Factory for creating storage instances
    factory: Arc<dyn StorageFactory>,
    /// Cached storage instance (lazily initialized)
    storage: Arc<OnceLock<Arc<dyn Storage>>>,
    /// Per-prefix storages (longest prefix first) for tables that vend distinct
    /// credentials per location prefix. Paths matching none use `storage` above.
    prefixed: Arc<Vec<PrefixedStorage>>,
}

/// A storage scoped to a location `prefix`, lazily built from its own config.
#[derive(Debug)]
struct PrefixedStorage {
    prefix: String,
    config: StorageConfig,
    storage: OnceLock<Arc<dyn Storage>>,
}

impl FileIO {
    /// Create a new FileIO backed by in-memory storage.
    ///
    /// This is useful for testing scenarios where persistent storage is not needed.
    pub fn new_with_memory() -> Self {
        Self {
            config: StorageConfig::new(),
            factory: Arc::new(MemoryStorageFactory),
            storage: Arc::new(OnceLock::new()),
            prefixed: Arc::new(Vec::new()),
        }
    }

    /// Create a new FileIO backed by local filesystem storage.
    ///
    /// This is useful for local development and testing with real files.
    pub fn new_with_fs() -> Self {
        Self {
            config: StorageConfig::new(),
            factory: Arc::new(LocalFsStorageFactory),
            storage: Arc::new(OnceLock::new()),
            prefixed: Arc::new(Vec::new()),
        }
    }

    /// Get the storage configuration.
    pub fn config(&self) -> &StorageConfig {
        &self.config
    }

    /// Get or create the storage for `path`, routing to the longest-matching
    /// prefix storage if any, else the default. Built once, then cached.
    fn get_storage(&self, path: &str) -> Result<Arc<dyn Storage>> {
        // `prefixed` is sorted longest-first, so the first match is most specific.
        // Selection is by longest matching string prefix, per the Iceberg REST
        // spec's storage-credentials semantics (and Java's `S3FileIO`).
        for ps in self.prefixed.iter() {
            if path.starts_with(&ps.prefix) {
                return Self::get_or_build(&ps.storage, &self.factory, &ps.config);
            }
        }
        Self::get_or_build(&self.storage, &self.factory, &self.config)
    }

    /// Get a cached storage from `cell`, building it from `config` on first use.
    fn get_or_build(
        cell: &OnceLock<Arc<dyn Storage>>,
        factory: &Arc<dyn StorageFactory>,
        config: &StorageConfig,
    ) -> Result<Arc<dyn Storage>> {
        if let Some(storage) = cell.get() {
            return Ok(storage.clone());
        }
        let storage = factory.build(config)?;
        // Another thread might have set it first; keep whatever ends up in the cell.
        let _ = cell.set(storage);
        Ok(cell.get().unwrap().clone())
    }

    /// Deletes file.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub async fn delete(&self, path: impl AsRef<str>) -> Result<()> {
        self.get_storage(path.as_ref())?.delete(path.as_ref()).await
    }

    /// Remove the path and all nested dirs and files recursively.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    ///
    /// # Behavior
    ///
    /// - If the path is a file or not exist, this function will be no-op.
    /// - If the path is a empty directory, this function will remove the directory itself.
    /// - If the path is a non-empty directory, this function will remove the directory and all nested files and directories.
    pub async fn delete_prefix(&self, path: impl AsRef<str>) -> Result<()> {
        self.get_storage(path.as_ref())?
            .delete_prefix(path.as_ref())
            .await
    }

    /// Delete multiple files from a stream of paths.
    ///
    /// # Arguments
    ///
    /// * paths: A stream of absolute paths starting with the scheme string used to construct [`FileIO`].
    pub async fn delete_stream(
        &self,
        paths: impl Stream<Item = String> + Send + 'static,
    ) -> Result<()> {
        // No per-prefix storages: delete the whole batch on the default storage.
        if self.prefixed.is_empty() {
            return self.get_storage("")?.delete_stream(paths.boxed()).await;
        }

        // Route by prefix, flushing bounded batches as we iterate so memory stays
        // bounded on large streams (like Java's `S3FileIO.deleteFiles`).
        const DELETE_BATCH_SIZE: usize = 1000;
        let mut groups: HashMap<String, Vec<String>> = HashMap::new();
        let mut paths = paths.boxed();
        while let Some(path) = paths.next().await {
            let key = self
                .prefixed
                .iter()
                .find(|ps| path.starts_with(&ps.prefix))
                .map(|ps| ps.prefix.clone())
                .unwrap_or_default();
            let buf = groups.entry(key).or_default();
            buf.push(path);
            if buf.len() >= DELETE_BATCH_SIZE {
                let full = std::mem::take(buf);
                self.get_storage(&full[0])?
                    .delete_stream(stream::iter(full).boxed())
                    .await?;
            }
        }

        // Flush remainders.
        for batch in groups.into_values() {
            if batch.is_empty() {
                continue;
            }
            self.get_storage(&batch[0])?
                .delete_stream(stream::iter(batch).boxed())
                .await?;
        }
        Ok(())
    }

    /// Check file exists.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub async fn exists(&self, path: impl AsRef<str>) -> Result<bool> {
        self.get_storage(path.as_ref())?.exists(path.as_ref()).await
    }

    /// Creates input file.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub fn new_input(&self, path: impl AsRef<str>) -> Result<InputFile> {
        self.get_storage(path.as_ref())?.new_input(path.as_ref())
    }

    /// Creates output file.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub fn new_output(&self, path: impl AsRef<str>) -> Result<OutputFile> {
        self.get_storage(path.as_ref())?.new_output(path.as_ref())
    }
}

/// Builder for [`FileIO`].
///
/// The builder accepts an explicit `StorageFactory` and configuration properties.
/// Storage is lazily initialized on first use.
#[derive(Clone, Debug)]
pub struct FileIOBuilder {
    /// Factory for creating storage instances
    factory: Arc<dyn StorageFactory>,
    /// Storage configuration
    config: StorageConfig,
    /// Per-location-prefix configs (prefix, config).
    prefixed: Vec<(String, StorageConfig)>,
}

impl FileIOBuilder {
    /// Creates a new builder with the given storage factory.
    pub fn new(factory: Arc<dyn StorageFactory>) -> Self {
        Self {
            factory,
            config: StorageConfig::new(),
            prefixed: Vec::new(),
        }
    }

    /// Add a configuration property.
    pub fn with_prop(mut self, key: impl ToString, value: impl ToString) -> Self {
        self.config = self.config.with_prop(key.to_string(), value.to_string());
        self
    }

    /// Add multiple configuration properties.
    pub fn with_props(
        mut self,
        args: impl IntoIterator<Item = (impl ToString, impl ToString)>,
    ) -> Self {
        self.config = self
            .config
            .with_props(args.into_iter().map(|e| (e.0.to_string(), e.1.to_string())));
        self
    }

    /// Add a per-prefix storage config. Paths starting with `prefix` (longest
    /// match wins) use these props instead of the default config.
    pub fn with_prefixed_props(
        mut self,
        prefix: impl Into<String>,
        props: impl IntoIterator<Item = (impl ToString, impl ToString)>,
    ) -> Self {
        let config = StorageConfig::from_props(
            props
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        );
        self.prefixed.push((prefix.into(), config));
        self
    }

    /// Get the storage configuration.
    pub fn config(&self) -> &StorageConfig {
        &self.config
    }

    /// Builds [`FileIO`].
    pub fn build(self) -> FileIO {
        let mut prefixed: Vec<PrefixedStorage> = self
            .prefixed
            .into_iter()
            .map(|(prefix, config)| PrefixedStorage {
                prefix,
                config,
                storage: OnceLock::new(),
            })
            .collect();
        // Longest prefix first so routing picks the most specific match.
        prefixed.sort_by_key(|item| std::cmp::Reverse(item.prefix.len()));
        FileIO {
            config: self.config,
            factory: self.factory,
            storage: Arc::new(OnceLock::new()),
            prefixed: Arc::new(prefixed),
        }
    }
}

/// The struct the represents the metadata of a file.
///
/// TODO: we can add last modified time, content type, etc. in the future.
pub struct FileMetadata {
    /// The size of the file.
    pub size: u64,
}

/// Trait for reading file.
///
/// # TODO
/// It's possible for us to remove the async_trait, but we need to figure
/// out how to handle the object safety.
#[async_trait::async_trait]
pub trait FileRead: Send + Sync + Unpin + 'static {
    /// Read file content with given range.
    ///
    /// TODO: we can support reading non-contiguous bytes in the future.
    async fn read(&self, range: Range<u64>) -> crate::Result<Bytes>;
}

#[async_trait::async_trait]
impl<T: AsRef<dyn FileRead> + Send + Sync + Unpin + 'static> FileRead for T {
    async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
        self.as_ref().read(range).await
    }
}

/// Input file is used for reading from files.
#[derive(Debug)]
pub struct InputFile {
    storage: Arc<dyn Storage>,
    // Absolute path of file.
    path: String,
}

impl InputFile {
    /// Creates a new input file.
    pub fn new(storage: Arc<dyn Storage>, path: String) -> Self {
        Self { storage, path }
    }

    /// Absolute path to root uri.
    pub fn location(&self) -> &str {
        &self.path
    }

    /// Check if file exists.
    pub async fn exists(&self) -> crate::Result<bool> {
        self.storage.exists(&self.path).await
    }

    /// Fetch and returns metadata of file.
    pub async fn metadata(&self) -> crate::Result<FileMetadata> {
        self.storage.metadata(&self.path).await
    }

    /// Read and returns whole content of file.
    ///
    /// For continuous reading, use [`Self::reader`] instead.
    pub async fn read(&self) -> crate::Result<Bytes> {
        self.storage.read(&self.path).await
    }

    /// Creates [`FileRead`] for continuous reading.
    ///
    /// For one-time reading, use [`Self::read`] instead.
    pub async fn reader(&self) -> crate::Result<Box<dyn FileRead>> {
        self.storage.reader(&self.path).await
    }
}

/// Trait for writing file.
///
/// # TODO
///
/// It's possible for us to remove the async_trait, but we need to figure
/// out how to handle the object safety.
#[async_trait::async_trait]
pub trait FileWrite: Send + Unpin + 'static {
    /// Write bytes to file.
    ///
    /// TODO: we can support writing non-contiguous bytes in the future.
    async fn write(&mut self, bs: Bytes) -> crate::Result<()>;

    /// Close file.
    ///
    /// Calling close on closed file will generate an error.
    async fn close(&mut self) -> crate::Result<()>;
}

/// Output file is used for writing to files..
#[derive(Debug)]
pub struct OutputFile {
    storage: Arc<dyn Storage>,
    // Absolute path of file.
    path: String,
}

impl OutputFile {
    /// Creates a new output file.
    pub fn new(storage: Arc<dyn Storage>, path: String) -> Self {
        Self { storage, path }
    }

    /// Relative path to root uri.
    pub fn location(&self) -> &str {
        &self.path
    }

    /// Checks if file exists.
    pub async fn exists(&self) -> Result<bool> {
        self.storage.exists(&self.path).await
    }

    /// Deletes file.
    ///
    /// If the file does not exist, it will not return error.
    pub async fn delete(&self) -> Result<()> {
        self.storage.delete(&self.path).await
    }

    /// Converts into [`InputFile`].
    pub fn to_input_file(self) -> InputFile {
        InputFile {
            storage: self.storage,
            path: self.path,
        }
    }

    /// Create a new output file with given bytes.
    ///
    /// # Notes
    ///
    /// Calling `write` will overwrite the file if it exists.
    /// For continuous writing, use [`Self::writer`].
    pub async fn write(&self, bs: Bytes) -> crate::Result<()> {
        self.storage.write(&self.path, bs).await
    }

    /// Creates output file for continuous writing.
    ///
    /// # Notes
    ///
    /// For one-time writing, use [`Self::write`] instead.
    pub async fn writer(&self) -> crate::Result<Box<dyn FileWrite>> {
        self.storage.writer(&self.path).await
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{File, create_dir_all};
    use std::io::Write;
    use std::path::Path;
    use std::sync::Arc;

    use bytes::Bytes;
    use futures::AsyncReadExt;
    use futures::io::AllowStdIo;
    use tempfile::TempDir;

    use super::{FileIO, FileIOBuilder};
    use crate::io::{LocalFsStorageFactory, MemoryStorageFactory};

    fn create_local_file_io() -> FileIO {
        FileIO::new_with_fs()
    }

    fn write_to_file<P: AsRef<Path>>(s: &str, path: P) {
        create_dir_all(path.as_ref().parent().unwrap()).unwrap();
        let mut f = File::create(path).unwrap();
        write!(f, "{s}").unwrap();
    }

    async fn read_from_file<P: AsRef<Path>>(path: P) -> String {
        let mut f = AllowStdIo::new(File::open(path).unwrap());
        let mut s = String::new();
        f.read_to_string(&mut s).await.unwrap();
        s
    }

    #[tokio::test]
    async fn test_local_input_file() {
        let tmp_dir = TempDir::new().unwrap();

        let file_name = "a.txt";
        let content = "Iceberg loves rust.";

        let full_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), file_name);
        write_to_file(content, &full_path);

        let file_io = create_local_file_io();
        let input_file = file_io.new_input(&full_path).unwrap();

        assert!(input_file.exists().await.unwrap());
        assert_eq!(&full_path, input_file.location());
        let read_content = read_from_file(full_path).await;

        assert_eq!(content, &read_content);
    }

    #[tokio::test]
    async fn test_delete_local_file() {
        let tmp_dir = TempDir::new().unwrap();

        let a_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), "a.txt");
        let sub_dir_path = format!("{}/sub", tmp_dir.path().to_str().unwrap());
        let b_path = format!("{}/{}", sub_dir_path, "b.txt");
        let c_path = format!("{}/{}", sub_dir_path, "c.txt");
        write_to_file("Iceberg loves rust.", &a_path);
        write_to_file("Iceberg loves rust.", &b_path);
        write_to_file("Iceberg loves rust.", &c_path);

        let file_io = create_local_file_io();
        assert!(file_io.exists(&a_path).await.unwrap());

        // Remove a file should be no-op.
        file_io.delete_prefix(&a_path).await.unwrap();
        assert!(file_io.exists(&a_path).await.unwrap());

        // Remove a not exist dir should be no-op.
        file_io.delete_prefix("not_exists/").await.unwrap();

        // Remove a dir should remove all files in it.
        file_io.delete_prefix(&sub_dir_path).await.unwrap();
        assert!(!file_io.exists(&b_path).await.unwrap());
        assert!(!file_io.exists(&c_path).await.unwrap());
        assert!(file_io.exists(&a_path).await.unwrap());

        file_io.delete(&a_path).await.unwrap();
        assert!(!file_io.exists(&a_path).await.unwrap());
    }

    #[tokio::test]
    async fn test_delete_non_exist_file() {
        let tmp_dir = TempDir::new().unwrap();

        let file_name = "a.txt";
        let full_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), file_name);

        let file_io = create_local_file_io();
        assert!(!file_io.exists(&full_path).await.unwrap());
        assert!(file_io.delete(&full_path).await.is_ok());
        assert!(file_io.delete_prefix(&full_path).await.is_ok());
    }

    #[tokio::test]
    async fn test_local_output_file() {
        let tmp_dir = TempDir::new().unwrap();

        let file_name = "a.txt";
        let content = "Iceberg loves rust.";

        let full_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), file_name);

        let file_io = create_local_file_io();
        let output_file = file_io.new_output(&full_path).unwrap();

        assert!(!output_file.exists().await.unwrap());
        {
            output_file.write(content.into()).await.unwrap();
        }

        assert_eq!(&full_path, output_file.location());

        let read_content = read_from_file(full_path).await;

        assert_eq!(content, &read_content);
    }

    #[tokio::test]
    async fn test_memory_io() {
        let io = FileIO::new_with_memory();

        let path = format!("{}/1.txt", TempDir::new().unwrap().path().to_str().unwrap());

        let output_file = io.new_output(&path).unwrap();
        output_file.write("test".into()).await.unwrap();

        assert!(io.exists(&path.clone()).await.unwrap());
        let input_file = io.new_input(&path).unwrap();
        let content = input_file.read().await.unwrap();
        assert_eq!(content, Bytes::from("test"));

        io.delete(&path).await.unwrap();
        assert!(!io.exists(&path).await.unwrap());
    }

    #[tokio::test]
    async fn test_file_io_builder_with_props() {
        let factory = Arc::new(MemoryStorageFactory);
        let file_io = FileIOBuilder::new(factory)
            .with_prop("key1", "value1")
            .with_prop("key2", "value2")
            .build();

        assert_eq!(file_io.config().get("key1"), Some(&"value1".to_string()));
        assert_eq!(file_io.config().get("key2"), Some(&"value2".to_string()));
    }

    #[tokio::test]
    async fn test_file_io_builder_with_multiple_props() {
        let factory = Arc::new(LocalFsStorageFactory);
        let props = vec![("key1", "value1"), ("key2", "value2")];
        let file_io = FileIOBuilder::new(factory).with_props(props).build();

        assert_eq!(file_io.config().get("key1"), Some(&"value1".to_string()));
        assert_eq!(file_io.config().get("key2"), Some(&"value2".to_string()));
    }

    #[tokio::test]
    async fn test_prefixed_props_sorted_by_descending_prefix_length() {
        let factory = Arc::new(MemoryStorageFactory);
        let file_io = FileIOBuilder::new(factory)
            .with_prefixed_props("memory://a/", [("k", "short")])
            .with_prefixed_props("memory://a/longer/", [("k", "long")])
            .build();

        // Longest prefix first so the most specific match wins at routing time.
        let prefixes: Vec<&str> = file_io.prefixed.iter().map(|p| p.prefix.as_str()).collect();
        assert_eq!(prefixes, vec!["memory://a/longer/", "memory://a/"]);
    }

    #[tokio::test]
    async fn test_prefixed_config_carries_credential_values() {
        // Prefix config gets the vended credentials; default config keeps only base props.
        let factory = Arc::new(MemoryStorageFactory);
        let file_io = FileIOBuilder::new(factory)
            .with_prop("s3.region", "us-east-1")
            .with_prefixed_props("s3://bucket/table", [
                ("s3.region", "us-east-1"),
                ("s3.access-key-id", "vended-key"),
                ("s3.secret-access-key", "vended-secret"),
            ])
            .build();

        // Default: base props, no credentials.
        assert_eq!(
            file_io.config().get("s3.region"),
            Some(&"us-east-1".to_string())
        );
        assert_eq!(file_io.config().get("s3.access-key-id"), None);

        // Prefix: base props + vended credentials.
        let prefixed = &file_io.prefixed[0].config;
        assert_eq!(prefixed.get("s3.region"), Some(&"us-east-1".to_string()));
        assert_eq!(
            prefixed.get("s3.access-key-id"),
            Some(&"vended-key".to_string())
        );
        assert_eq!(
            prefixed.get("s3.secret-access-key"),
            Some(&"vended-secret".to_string())
        );
    }

    #[tokio::test]
    async fn test_get_storage_routes_by_prefix() {
        let factory = Arc::new(MemoryStorageFactory);
        let file_io = FileIOBuilder::new(factory)
            .with_prop("scope", "default")
            .with_prefixed_props("memory://creds/", [("scope", "prefixed")])
            .build();

        let default_a = file_io.get_storage("memory://other/x").unwrap();
        let default_b = file_io.get_storage("memory://other/y").unwrap();
        let prefixed_a = file_io.get_storage("memory://creds/x").unwrap();
        let prefixed_b = file_io.get_storage("memory://creds/y").unwrap();

        // Repeated routing to the same bucket returns the memoized storage...
        assert!(Arc::ptr_eq(&default_a, &default_b));
        assert!(Arc::ptr_eq(&prefixed_a, &prefixed_b));
        // ...and a prefix-matching path resolves to a distinct storage from the default.
        assert!(!Arc::ptr_eq(&default_a, &prefixed_a));
    }

    #[tokio::test]
    async fn test_delete_stream_routes_by_prefix() {
        let factory = Arc::new(MemoryStorageFactory);
        let file_io = FileIOBuilder::new(factory)
            .with_prefixed_props("memory:/creds/", [("k", "v")])
            .build();

        // One file under each routing bucket (default vs prefixed storage).
        let default_path = "memory:/other/a.txt";
        let prefixed_path = "memory:/creds/b.txt";
        for path in [default_path, prefixed_path] {
            file_io
                .new_output(path)
                .unwrap()
                .write("x".into())
                .await
                .unwrap();
            assert!(file_io.exists(path).await.unwrap());
        }

        // delete_stream must route each path to the storage that holds it.
        file_io
            .delete_stream(futures::stream::iter(vec![
                default_path.to_string(),
                prefixed_path.to_string(),
            ]))
            .await
            .unwrap();

        assert!(!file_io.exists(default_path).await.unwrap());
        assert!(!file_io.exists(prefixed_path).await.unwrap());
    }

    #[tokio::test]
    async fn test_delete_stream_flushes_across_batches() {
        // More than the flush threshold (1000): exercises mid-stream flush + remainder.
        let factory = Arc::new(MemoryStorageFactory);
        let file_io = FileIOBuilder::new(factory)
            .with_prefixed_props("memory:/creds/", [("k", "v")])
            .build();

        let n = 1050;
        let mut paths = Vec::with_capacity(n);
        for i in 0..n {
            let p = format!("memory:/creds/f{i}.txt");
            file_io
                .new_output(&p)
                .unwrap()
                .write("x".into())
                .await
                .unwrap();
            paths.push(p);
        }

        file_io
            .delete_stream(futures::stream::iter(paths.clone()))
            .await
            .unwrap();

        for p in &paths {
            assert!(!file_io.exists(p).await.unwrap());
        }
    }
}
