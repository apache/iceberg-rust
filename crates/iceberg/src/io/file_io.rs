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

use std::ops::Range;
use std::sync::{Arc, OnceLock};

use bytes::Bytes;

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
/// Supported storages:
///
/// | Storage            | Feature Flag      | Expected Path Format             | Schemes                       |
/// |--------------------|-------------------|----------------------------------| ------------------------------|
/// | Local file system  | `storage-fs`      | `file`                           | `file://path/to/file`         |
/// | Memory             | `storage-memory`  | `memory`                         | `memory://path/to/file`       |
/// | S3                 | `storage-s3`      | `s3`, `s3a`                      | `s3://<bucket>/path/to/file`  |
/// | GCS                | `storage-gcs`     | `gs`, `gcs`                      | `gs://<bucket>/path/to/file`  |
/// | OSS                | `storage-oss`     | `oss`                            | `oss://<bucket>/path/to/file` |
/// | Azure Datalake     | `storage-azdls`   | `abfs`, `abfss`, `wasb`, `wasbs` | `abfs://<filesystem>@<account>.dfs.core.windows.net/path/to/file` or `wasb://<container>@<account>.blob.core.windows.net/path/to/file` |
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
        }
    }

    /// Get the storage configuration.
    pub fn config(&self) -> &StorageConfig {
        &self.config
    }

    /// Get or create the storage instance.
    ///
    /// The factory is invoked on first access and the result is cached
    /// for all subsequent operations.
    fn get_storage(&self) -> Result<Arc<dyn Storage>> {
        // Check if already initialized
        if let Some(storage) = self.storage.get() {
            return Ok(storage.clone());
        }

        // Build the storage
        let storage = self.factory.build(&self.config)?;

        // Try to set it (another thread might have set it first)
        let _ = self.storage.set(storage.clone());

        // Return whatever is in the cell (either ours or another thread's)
        Ok(self.storage.get().unwrap().clone())
    }

    /// Deletes file.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub async fn delete(&self, path: impl AsRef<str>) -> Result<()> {
        self.get_storage()?.delete(path.as_ref()).await
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
        self.get_storage()?.delete_prefix(path.as_ref()).await
    }

    /// Check file exists.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub async fn exists(&self, path: impl AsRef<str>) -> Result<bool> {
        self.get_storage()?.exists(path.as_ref()).await
    }

    /// Creates input file.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub fn new_input(&self, path: impl AsRef<str>) -> Result<InputFile> {
        self.get_storage()?.new_input(path.as_ref())
    }

    /// Creates output file.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub fn new_output(&self, path: impl AsRef<str>) -> Result<OutputFile> {
        self.get_storage()?.new_output(path.as_ref())
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
}

impl FileIOBuilder {
    /// Creates a new builder with the given storage factory.
    pub fn new(factory: Arc<dyn StorageFactory>) -> Self {
        Self {
            factory,
            config: StorageConfig::new(),
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

    /// Get the storage configuration.
    pub fn config(&self) -> &StorageConfig {
        &self.config
    }

    /// Builds [`FileIO`].
    pub fn build(self) -> FileIO {
        FileIO {
            config: self.config,
            factory: self.factory,
            storage: Arc::new(OnceLock::new()),
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
}
