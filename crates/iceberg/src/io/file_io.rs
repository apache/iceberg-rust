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

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use url::Url;

// Re-export traits from storage module
pub use super::storage::{Storage, StorageFactory, StorageRegistry};
use crate::io::STORAGE_LOCATION_SCHEME;
use crate::{Error, ErrorKind, Result};

/// FileIO implementation, used to manipulate files in underlying storage.
///
/// # Note
///
/// All path passed to `FileIO` must be absolute path starting with scheme string used to construct `FileIO`.
/// For example, if you construct `FileIO` with `s3a` scheme, then all path passed to `FileIO` must start with `s3a://`.
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
#[derive(Clone, Debug)]
pub struct FileIO {
    builder: FileIOBuilder,

    inner: Arc<dyn Storage>,
}

impl FileIO {
    /// Convert FileIO into [`FileIOBuilder`] which used to build this FileIO.
    ///
    /// This function is useful when you want serialize and deserialize FileIO across
    /// distributed systems.
    pub fn into_builder(self) -> FileIOBuilder {
        self.builder
    }

    /// Try to infer file io scheme from path. See [`FileIO`] for supported schemes.
    ///
    /// - If it's a valid url, for example `s3://bucket/a`, url scheme will be used, and the rest of the url will be ignored.
    /// - If it's not a valid url, will try to detect if it's a file path.
    ///
    /// Otherwise will return parsing error.
    pub fn from_path(path: impl AsRef<str>) -> crate::Result<FileIOBuilder> {
        let url = Url::parse(path.as_ref())
            .map_err(Error::from)
            .or_else(|e| {
                Url::from_file_path(path.as_ref()).map_err(|_| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Input is neither a valid url nor path",
                    )
                    .with_context("input", path.as_ref().to_string())
                    .with_source(e)
                })
            })?;

        Ok(FileIOBuilder::new(url.scheme()))
    }

    /// Deletes file.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub async fn delete(&self, path: impl AsRef<str>) -> Result<()> {
        self.inner.delete(path.as_ref()).await
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
        self.inner.delete_prefix(path.as_ref()).await
    }

    /// Check file exists.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub async fn exists(&self, path: impl AsRef<str>) -> Result<bool> {
        self.inner.exists(path.as_ref()).await
    }

    /// Creates input file.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub fn new_input(&self, path: impl AsRef<str>) -> Result<InputFile> {
        self.inner.new_input(path.as_ref())
    }

    /// Creates output file.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub fn new_output(&self, path: impl AsRef<str>) -> Result<OutputFile> {
        self.inner.new_output(path.as_ref())
    }
}

/// Container for storing type-safe extensions used to configure underlying FileIO behavior.
#[derive(Clone, Debug, Default)]
pub struct Extensions(HashMap<TypeId, Arc<dyn Any + Send + Sync>>);

impl Extensions {
    /// Add an extension.
    pub fn add<T: Any + Send + Sync>(&mut self, ext: T) {
        self.0.insert(TypeId::of::<T>(), Arc::new(ext));
    }

    /// Extends the current set of extensions with another set of extensions.
    pub fn extend(&mut self, extensions: Extensions) {
        self.0.extend(extensions.0);
    }

    /// Fetch an extension.
    pub fn get<T>(&self) -> Option<Arc<T>>
    where T: 'static + Send + Sync + Clone {
        let type_id = TypeId::of::<T>();
        self.0
            .get(&type_id)
            .and_then(|arc_any| Arc::clone(arc_any).downcast::<T>().ok())
    }
}

/// Builder for [`FileIO`].
///
/// # Custom Storage Implementations
///
/// You can use custom storage implementations by creating a custom
/// [`StorageRegistry`] and registering your storage factory:
///
/// ```rust,ignore
/// use iceberg::io::{StorageRegistry, StorageFactory, FileIOBuilder};
/// use std::sync::Arc;
///
/// // Create your custom storage factory
/// let my_factory = Arc::new(MyCustomStorageFactory);
///
/// // Register it with a custom scheme
/// let mut registry = StorageRegistry::new();
/// registry.register("mycustom", my_factory);
///
/// // Use it to build FileIO
/// let file_io = FileIOBuilder::new("mycustom")
///     .with_prop("key", "value")
///     .with_registry(registry)
///     .build()?;
/// ```
#[derive(Clone, Debug)]
pub struct FileIOBuilder {
    /// This is used to infer scheme of operator.
    ///
    /// If this is `None`, then [`FileIOBuilder::build`](FileIOBuilder::build) will build a local file io.
    scheme_str: Option<String>,
    /// Arguments for operator.
    props: HashMap<String, String>,
    /// Optional extensions to configure the underlying FileIO behavior.
    extensions: Extensions,
    /// Optional custom registry. If None, a default registry will be created.
    registry: Option<StorageRegistry>,
}

impl FileIOBuilder {
    /// Creates a new builder with scheme.
    /// See [`FileIO`] for supported schemes.
    pub fn new(scheme_str: impl ToString) -> Self {
        Self {
            scheme_str: Some(scheme_str.to_string()),
            props: HashMap::default(),
            extensions: Extensions::default(),
            registry: None,
        }
    }

    /// Creates a new builder for local file io.
    pub fn new_fs_io() -> Self {
        Self {
            scheme_str: None,
            props: HashMap::default(),
            extensions: Extensions::default(),
            registry: None,
        }
    }

    /// Fetch the scheme string.
    ///
    /// The scheme_str will be empty if it's None.
    pub fn into_parts(
        self,
    ) -> (
        String,
        HashMap<String, String>,
        Extensions,
        Option<StorageRegistry>,
    ) {
        (
            self.scheme_str.unwrap_or_default(),
            self.props,
            self.extensions,
            self.registry,
        )
    }

    /// Add argument for operator.
    pub fn with_prop(mut self, key: impl ToString, value: impl ToString) -> Self {
        self.props.insert(key.to_string(), value.to_string());
        self
    }

    /// Add argument for operator.
    pub fn with_props(
        mut self,
        args: impl IntoIterator<Item = (impl ToString, impl ToString)>,
    ) -> Self {
        self.props
            .extend(args.into_iter().map(|e| (e.0.to_string(), e.1.to_string())));
        self
    }

    /// Add an extension to the file IO builder.
    pub fn with_extension<T: Any + Send + Sync>(mut self, ext: T) -> Self {
        self.extensions.add(ext);
        self
    }

    /// Adds multiple extensions to the file IO builder.
    pub fn with_extensions(mut self, extensions: Extensions) -> Self {
        self.extensions.extend(extensions);
        self
    }

    /// Fetch an extension from the file IO builder.
    pub fn extension<T>(&self) -> Option<Arc<T>>
    where T: 'static + Send + Sync + Clone {
        self.extensions.get::<T>()
    }

    /// Sets a custom storage registry.
    ///
    /// This allows you to register custom storage implementations that can be used
    /// when building the FileIO. If not set, a default registry with built-in
    /// storage types will be used.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use iceberg::io::{StorageRegistry, FileIOBuilder};
    /// use std::sync::Arc;
    ///
    /// let mut registry = StorageRegistry::new();
    /// registry.register("mycustom", Arc::new(MyCustomStorageFactory));
    ///
    /// let file_io = FileIOBuilder::new("mycustom")
    ///     .with_registry(registry)
    ///     .build()?;
    /// ```
    pub fn with_registry(mut self, registry: StorageRegistry) -> Self {
        self.registry = Some(registry);
        self
    }

    /// Builds [`FileIO`].
    pub fn build(self) -> Result<FileIO> {
        // Use the scheme to determine the storage type
        let scheme = self.scheme_str.clone().unwrap_or_default();

        // Use custom registry if provided, otherwise create default
        let registry = self.registry.clone().unwrap_or_default();

        let factory = registry.get_factory(scheme.as_str())?;

        let mut props_with_scheme = self.props.clone();
        props_with_scheme.insert(STORAGE_LOCATION_SCHEME.to_string(), scheme);

        // Build storage with props and extensions
        let storage = factory.build(props_with_scheme, self.extensions.clone())?;

        Ok(FileIO {
            builder: self,
            inner: storage,
        })
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
impl FileRead for opendal::Reader {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        Ok(opendal::Reader::read(self, range).await?.to_bytes())
    }
}

/// Input file is used for reading from files.
#[derive(Debug)]
pub struct InputFile {
    storage: Arc<dyn Storage>,
    path: String,
}

impl InputFile {
    /// Creates a new input file.
    ///
    /// # Arguments
    ///
    /// * `storage` - The storage backend to use
    /// * `path` - Absolute path to the file
    pub fn new(storage: Arc<dyn Storage>, path: String) -> Self {
        Self { storage, path }
    }

    /// Returns the storage backend for this input file.
    pub fn storage(&self) -> &Arc<dyn Storage> {
        &self.storage
    }

    /// Absolute path to root uri.
    pub fn location(&self) -> &str {
        &self.path
    }

    /// Check if file exists.
    pub async fn exists(&self) -> Result<bool> {
        self.storage.exists(&self.path).await
    }

    /// Fetch and returns metadata of file.
    pub async fn metadata(&self) -> Result<FileMetadata> {
        self.storage.metadata(&self.path).await
    }

    /// Read and returns whole content of file.
    ///
    /// For continuous reading, use [`Self::reader`] instead.
    pub async fn read(&self) -> Result<Bytes> {
        self.storage.read(&self.path).await
    }

    /// Creates [`FileRead`] for continuous reading.
    ///
    /// For one-time reading, use [`Self::read`] instead.
    pub async fn reader(&self) -> Result<Box<dyn FileRead>> {
        self.storage.reader(&self.path).await
    }
}

/// Trait for writing file.
///
/// # TODO
///
/// It's possible for us to remove the async_trait, but we need to figure
/// out how to handle the object safety.
#[async_trait]
pub trait FileWrite: Send + Sync + Unpin + 'static {
    /// Write bytes to file.
    ///
    /// TODO: we can support writing non-contiguous bytes in the future.
    async fn write(&mut self, bs: Bytes) -> Result<()>;

    /// Close file.
    ///
    /// Calling close on closed file will generate an error.
    async fn close(&mut self) -> Result<()>;
}

#[async_trait::async_trait]
impl FileWrite for opendal::Writer {
    async fn write(&mut self, bs: Bytes) -> crate::Result<()> {
        Ok(opendal::Writer::write(self, bs).await?)
    }

    async fn close(&mut self) -> crate::Result<()> {
        let _ = opendal::Writer::close(self).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl FileWrite for Box<dyn FileWrite> {
    async fn write(&mut self, bs: Bytes) -> crate::Result<()> {
        self.as_mut().write(bs).await
    }

    async fn close(&mut self) -> crate::Result<()> {
        self.as_mut().close().await
    }
}

/// Output file is used for writing to files.
#[derive(Debug)]
pub struct OutputFile {
    storage: Arc<dyn Storage>,
    path: String,
}

impl OutputFile {
    /// Creates a new output file.
    ///
    /// # Arguments
    ///
    /// * `storage` - The storage backend to use
    /// * `path` - Absolute path to the file
    pub fn new(storage: Arc<dyn Storage>, path: String) -> Self {
        Self { storage, path }
    }

    /// Returns the storage backend for this output file.
    pub fn storage(&self) -> &Arc<dyn Storage> {
        &self.storage
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
        InputFile::new(self.storage, self.path)
    }

    /// Create a new output file with given bytes.
    ///
    /// # Notes
    ///
    /// Calling `write` will overwrite the file if it exists.
    /// For continuous writing, use [`Self::writer`].
    pub async fn write(&self, bs: Bytes) -> crate::Result<()> {
        self.storage.write(self.path.as_str(), bs).await
    }

    /// Creates output file for continuous writing.
    ///
    /// # Notes
    ///
    /// For one-time writing, use [`Self::write`] instead.
    pub async fn writer(&self) -> crate::Result<Box<dyn FileWrite>> {
        Ok(Box::new(self.storage.writer(&self.path).await?))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::{File, create_dir_all};
    use std::io::Write;
    use std::path::Path;
    use std::sync::{Arc, Mutex, MutexGuard};

    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::AsyncReadExt;
    use futures::io::AllowStdIo;
    use serde::{Deserialize, Serialize};
    use tempfile::TempDir;

    use super::{FileIO, FileIOBuilder};
    use crate::io::{
        Extensions, FileMetadata, FileRead, FileWrite, InputFile, OutputFile,
        STORAGE_LOCATION_SCHEME, Storage, StorageFactory, StorageRegistry,
    };
    use crate::{Error, ErrorKind, Result};

    // Test storage implementation that tracks write operations
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestStorage {
        #[serde(skip, default = "default_written")]
        written: Arc<Mutex<Vec<String>>>,
        received_props: HashMap<String, String>,
    }

    fn default_written() -> Arc<Mutex<Vec<String>>> {
        Arc::new(Mutex::new(Vec::new()))
    }

    #[allow(dead_code)]
    impl TestStorage {
        pub fn written(&self) -> MutexGuard<'_, Vec<String>> {
            self.written.lock().unwrap()
        }

        pub fn received_props(&self) -> &HashMap<String, String> {
            &self.received_props
        }
    }

    #[async_trait]
    #[typetag::serde]
    impl Storage for TestStorage {
        async fn exists(&self, _path: &str) -> Result<bool> {
            Ok(true)
        }

        async fn metadata(&self, _path: &str) -> Result<FileMetadata> {
            Ok(FileMetadata { size: 42 })
        }

        async fn read(&self, _path: &str) -> Result<Bytes> {
            Ok(Bytes::from("test data"))
        }

        async fn reader(&self, _path: &str) -> Result<Box<dyn FileRead>> {
            Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "TestStorage does not support reader",
            ))
        }

        async fn write(&self, path: &str, _bs: Bytes) -> Result<()> {
            self.written.lock().unwrap().push(path.to_string());
            Ok(())
        }

        async fn writer(&self, _path: &str) -> Result<Box<dyn FileWrite>> {
            Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "TestStorage does not support writer",
            ))
        }

        async fn delete(&self, _path: &str) -> Result<()> {
            Ok(())
        }

        async fn delete_prefix(&self, _path: &str) -> Result<()> {
            Ok(())
        }

        fn new_input(&self, path: &str) -> Result<InputFile> {
            Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
        }

        fn new_output(&self, path: &str) -> Result<OutputFile> {
            Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
        }
    }

    fn default_received_props() -> Arc<Mutex<HashMap<String, String>>> {
        Arc::new(Mutex::new(HashMap::new()))
    }

    // Test storage factory
    #[derive(Debug, Serialize, Deserialize)]
    struct TestStorageFactory {
        #[serde(skip, default = "default_written")]
        written: Arc<Mutex<Vec<String>>>,
        #[serde(skip, default = "default_received_props")]
        received_props: Arc<Mutex<HashMap<String, String>>>,
    }

    impl TestStorageFactory {
        pub fn written(&self) -> MutexGuard<'_, Vec<String>> {
            self.written.lock().unwrap()
        }

        pub fn received_props(&self) -> MutexGuard<'_, HashMap<String, String>> {
            self.received_props.lock().unwrap()
        }
    }

    #[typetag::serde]
    impl StorageFactory for TestStorageFactory {
        fn build(
            &self,
            props: HashMap<String, String>,
            _extensions: Extensions,
        ) -> Result<Arc<dyn Storage>> {
            *self.received_props.lock().unwrap() = props.clone();
            Ok(Arc::new(TestStorage {
                written: self.written.clone(),
                received_props: props,
            }))
        }
    }

    fn create_local_file_io() -> FileIO {
        FileIOBuilder::new_fs_io().build().unwrap()
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
        // Remove heading slash
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

    #[test]
    fn test_create_file_from_path() {
        let io = FileIO::from_path("/tmp/a").unwrap();
        assert_eq!("file", io.scheme_str.unwrap().as_str());

        let io = FileIO::from_path("file:/tmp/b").unwrap();
        assert_eq!("file", io.scheme_str.unwrap().as_str());

        let io = FileIO::from_path("file:///tmp/c").unwrap();
        assert_eq!("file", io.scheme_str.unwrap().as_str());

        let io = FileIO::from_path("s3://bucket/a").unwrap();
        assert_eq!("s3", io.scheme_str.unwrap().as_str());

        let io = FileIO::from_path("tmp/||c");
        assert!(io.is_err());
    }

    #[tokio::test]
    async fn test_memory_io() {
        let io = FileIOBuilder::new("memory").build().unwrap();

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

    #[test]
    fn test_custom_registry() {
        // Create a custom registry and register test storage
        let factory = Arc::new(TestStorageFactory {
            written: Arc::new(Mutex::new(Vec::new())),
            received_props: Arc::new(Mutex::new(HashMap::new())),
        });

        let mut registry = StorageRegistry::new();
        registry.register("test", factory.clone());

        // Build FileIO with custom storage
        let file_io = FileIOBuilder::new("test")
            .with_registry(registry)
            .build()
            .unwrap();

        // Verify we can create files with the custom storage
        assert!(file_io.new_output("test://test.txt").is_ok());
        assert!(file_io.new_input("test://test.txt").is_ok());
    }

    #[tokio::test]
    async fn test_custom_registry_operations() {
        // Create test storage with write tracking
        let factory = Arc::new(TestStorageFactory {
            written: Arc::new(Mutex::new(Vec::new())),
            received_props: Arc::new(Mutex::new(HashMap::new())),
        });

        let mut registry = StorageRegistry::new();
        registry.register("test", factory.clone());

        // Build FileIO with test storage
        let file_io = FileIOBuilder::new("test")
            .with_registry(registry)
            .build()
            .unwrap();

        // Perform operations
        let output = file_io.new_output("test://bucket/file.txt").unwrap();
        output.write(Bytes::from("test")).await.unwrap();

        let input = file_io.new_input("test://bucket/file.txt").unwrap();
        let data = input.read().await.unwrap();
        assert_eq!(data, Bytes::from("test data"));

        let metadata = input.metadata().await.unwrap();
        assert_eq!(metadata.size, 42);

        // Verify write was tracked
        let tracked = factory.written();
        assert_eq!(tracked.len(), 1);
        assert_eq!(tracked[0], "test://bucket/file.txt");
    }

    #[test]
    fn test_scheme_and_props_propagation() {
        // Create test storage that captures props
        let factory = Arc::new(TestStorageFactory {
            written: Arc::new(Mutex::new(Vec::new())),
            received_props: Arc::new(Mutex::new(HashMap::new())),
        });

        let mut registry = StorageRegistry::new();
        registry.register("myscheme", factory.clone());

        // Build FileIO with custom scheme and additional props
        let file_io = FileIOBuilder::new("myscheme")
            .with_prop("custom.prop", "custom_value")
            .with_registry(registry)
            .build()
            .unwrap();

        // Verify the storage was created
        assert!(file_io.new_output("myscheme://test.txt").is_ok());

        // Verify the scheme was propagated to the factory
        let props = factory.received_props();
        assert_eq!(
            props.get(STORAGE_LOCATION_SCHEME),
            Some(&"myscheme".to_string())
        );
        // Verify custom props were also passed
        assert_eq!(props.get("custom.prop"), Some(&"custom_value".to_string()));
    }

    #[test]
    fn test_into_parts_includes_registry() {
        let registry = StorageRegistry::new();

        let builder = FileIOBuilder::new("memory")
            .with_prop("key", "value")
            .with_registry(registry.clone());

        let (scheme, props, _extensions, returned_registry) = builder.into_parts();

        assert_eq!(scheme, "memory");
        assert_eq!(props.get("key"), Some(&"value".to_string()));
        assert!(returned_registry.is_some());
    }

    #[test]
    fn test_into_parts_without_registry() {
        let builder = FileIOBuilder::new("memory").with_prop("key", "value");

        let (scheme, props, _extensions, returned_registry) = builder.into_parts();

        assert_eq!(scheme, "memory");
        assert_eq!(props.get("key"), Some(&"value".to_string()));
        assert!(returned_registry.is_none());
    }
}
