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
use std::sync::{Arc, Once, OnceLock};

use bytes::Bytes;
use futures::{Stream, StreamExt};

use super::storage::{
    LocalFsStorageFactory, MemoryStorageFactory, Storage, StorageConfig, StorageCredentialProvider,
    StorageCredentialProviderFactory, StorageFactory,
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
    /// Optional provider of refreshable, backend-specific credentials
    credential_provider: Option<Arc<dyn StorageCredentialProvider>>,
    /// Cached storage instance (lazily initialized)
    storage: Arc<OnceLock<Arc<dyn Storage>>>,
}

mod _serde {
    use std::sync::Arc;

    use serde::{Deserialize, Serialize};

    use super::{StorageConfig, StorageFactory};

    #[derive(Serialize)]
    pub(super) struct SerializableFileIO<'a> {
        pub(super) config: &'a StorageConfig,
        pub(super) factory: &'a Arc<dyn StorageFactory>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(super) credential_provider: Option<serde_json::Value>,
    }

    #[derive(Deserialize)]
    pub(super) struct DeserializedFileIO {
        pub(super) config: StorageConfig,
        pub(super) factory: Arc<dyn StorageFactory>,
        /// Kept as JSON, so that a provider factory the receiving binary does
        /// not link can be dropped instead of failing the whole `FileIO`.
        #[serde(default)]
        pub(super) credential_provider: Option<serde_json::Value>,
    }
}

/// Warns that a `FileIO` crossing a process boundary loses its credential
/// provider. Engines may serialize a `FileIO` per task, so each cause, tracked
/// by its own `warned`, is reported once.
fn warn_once_without_provider(warned: &'static Once, reason: impl FnOnce() -> String) {
    warned.call_once(|| {
        tracing::warn!(
            "{}; its credentials will not be refreshed after deserialization",
            reason()
        )
    });
}

impl FileIO {
    /// Create a new FileIO backed by in-memory storage.
    ///
    /// This is useful for testing scenarios where persistent storage is not needed.
    pub fn new_with_memory() -> Self {
        Self {
            config: StorageConfig::new(),
            factory: Arc::new(MemoryStorageFactory),
            credential_provider: None,
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
            credential_provider: None,
            storage: Arc::new(OnceLock::new()),
        }
    }

    /// Serializes all portable state of this `FileIO` into a byte vector.
    ///
    /// This includes the storage configuration and factory, but not the cached storage instance.
    /// The storage cache is rebuilt lazily on first use after calling [`FileIO::deserialize_all`].
    ///
    /// The serialized representation is not a stable format and may change between crate versions.
    /// Applications should not rely on it for long-term storage or exchange it between incompatible
    /// versions of this crate.
    ///
    /// All storage configuration properties are included in the serialized representation. These
    /// properties may contain credentials or other sensitive values, so the returned bytes must be
    /// protected in transit and at rest by the application embedding this crate. A serialized
    /// credential provider may likewise carry catalog authentication and vended credentials.
    ///
    /// Storage factories are serialized through [`typetag`](https://docs.rs/typetag). Third-party
    /// factories must use `#[typetag::serde]` on their [`StorageFactory`] implementation.
    ///
    /// A credential provider is serialized as the
    /// [`StorageCredentialProviderFactory`] returned by
    /// [`StorageCredentialProvider::factory`], and rebuilt on deserialization. A provider that
    /// cannot be rebuilt in another process is left out with a warning, as with
    /// [`FileIO::without_credential_provider`]: the deserialized `FileIO` uses the credentials in
    /// its configuration, which are not refreshed. Storage factories that hold their own
    /// credential sources, such as a custom AWS credential loader, still fail to serialize, as
    /// leaving them out would leave no credentials at all.
    pub fn serialize_all(&self) -> Result<Vec<u8>> {
        let credential_provider = self.credential_provider.as_ref().and_then(|provider| {
            provider
                .factory()
                .and_then(|factory| Ok(serde_json::to_value(factory)?))
                .inspect_err(|error| {
                    static WARNED: Once = Once::new();
                    warn_once_without_provider(&WARNED, || {
                        format!("serializing FileIO without its credential provider: {error}")
                    })
                })
                .ok()
        });

        Ok(serde_json::to_vec(&_serde::SerializableFileIO {
            config: &self.config,
            factory: &self.factory,
            credential_provider,
        })?)
    }

    /// Deserializes a `FileIO` previously produced by [`FileIO::serialize_all`].
    ///
    /// The receiving binary must use a compatible crate version and link the concrete factory
    /// implementation so it is registered with `typetag`. Backend-specific requirements are
    /// documented by each storage factory implementation.
    ///
    /// A credential provider is rebuilt when the binary links its factory implementation, such as
    /// a catalog's. Otherwise, or when rebuilding fails, the `FileIO` is deserialized without
    /// it and logs a warning, as [`FileIO::serialize_all`] does.
    pub fn deserialize_all(bytes: &[u8]) -> Result<Self> {
        let _serde::DeserializedFileIO {
            config,
            factory,
            credential_provider,
        } = serde_json::from_slice(bytes)?;
        let credential_provider = credential_provider.and_then(|provider_factory| {
            // The factory may hold credentials, and serde errors quote the
            // offending value, so only its type is reported.
            let factory_type = provider_factory
                .get("type")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown")
                .to_owned();
            match serde_json::from_value::<Arc<dyn StorageCredentialProviderFactory>>(
                provider_factory,
            ) {
                Ok(provider_factory) => provider_factory
                    .build(&config)
                    .inspect_err(|error| {
                        static WARNED: Once = Once::new();
                        warn_once_without_provider(&WARNED, || {
                            format!(
                                "deserializing FileIO without its credential provider, which \
                                 could not be rebuilt: {error}"
                            )
                        })
                    })
                    .ok(),
                Err(_) => {
                    static WARNED: Once = Once::new();
                    warn_once_without_provider(&WARNED, || {
                        format!(
                            "deserializing FileIO without its credential provider, whose \
                             factory {factory_type} is not linked or does not match this version"
                        )
                    });
                    None
                }
            }
        });
        Ok(Self {
            config,
            factory,
            credential_provider,
            storage: Arc::new(OnceLock::new()),
        })
    }

    /// Returns a copy of this `FileIO` without its credential provider.
    ///
    /// The copy uses only the credentials in its storage configuration, which are not refreshed.
    /// Use this to serialize a `FileIO` without its credential provider, even when the provider
    /// could be rebuilt in another process.
    pub fn without_credential_provider(&self) -> Self {
        Self {
            config: self.config.clone(),
            factory: Arc::clone(&self.factory),
            credential_provider: None,
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

        // Build the storage, passing any credential provider so backends that
        // support refreshable credentials can wire it into their operators.
        let storage = self
            .factory
            .build_with_credential_provider(&self.config, self.credential_provider.clone())?;

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

    /// Delete multiple files from a stream of paths.
    ///
    /// # Arguments
    ///
    /// * paths: A stream of absolute paths starting with the scheme string used to construct [`FileIO`].
    pub async fn delete_stream(
        &self,
        paths: impl Stream<Item = String> + Send + 'static,
    ) -> Result<()> {
        self.get_storage()?.delete_stream(paths.boxed()).await
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
    /// Optional provider of refreshable, backend-specific credentials
    credential_provider: Option<Arc<dyn StorageCredentialProvider>>,
}

impl FileIOBuilder {
    /// Creates a new builder with the given storage factory.
    pub fn new(factory: Arc<dyn StorageFactory>) -> Self {
        Self {
            factory,
            config: StorageConfig::new(),
            credential_provider: None,
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

    /// Attach a provider of refreshable, backend-specific credentials.
    ///
    /// Storage factories that cannot use the provider ignore it and use the
    /// credentials in the configuration.
    pub fn with_credential_provider(
        mut self,
        provider: Arc<dyn StorageCredentialProvider>,
    ) -> Self {
        self.credential_provider = Some(provider);
        self
    }

    /// Builds [`FileIO`].
    pub fn build(self) -> FileIO {
        FileIO {
            config: self.config,
            factory: self.factory,
            credential_provider: self.credential_provider,
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
    async fn read(&self, range: Range<u64>) -> Result<Bytes>;
}

#[async_trait::async_trait]
impl<T: AsRef<dyn FileRead> + Send + Sync + Unpin + 'static> FileRead for T {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
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
#[async_trait::async_trait]
pub trait FileWrite: Send + Unpin + 'static {
    /// Write bytes to file.
    ///
    /// TODO: we can support writing non-contiguous bytes in the future.
    async fn write(&mut self, bs: Bytes) -> Result<()>;

    /// Close the file and return its stored size, including encryption overhead for encrypted files.
    ///
    /// Calling close on closed file will generate an error.
    async fn close(&mut self) -> Result<FileMetadata>;
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
    pub async fn write(&self, bs: Bytes) -> Result<()> {
        self.storage.write(&self.path, bs).await
    }

    /// Creates output file for continuous writing.
    ///
    /// # Notes
    ///
    /// For one-time writing, use [`Self::write`] instead.
    pub async fn writer(&self) -> Result<Box<dyn FileWrite>> {
        self.storage.writer(&self.path).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::{File, create_dir_all};
    use std::io::Write;
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::AsyncReadExt;
    use futures::io::AllowStdIo;
    use serde::{Deserialize, Serialize};
    use tempfile::TempDir;

    use super::{FileIO, FileIOBuilder};
    use crate::Result;
    use crate::io::{
        GCS_TOKEN, LocalFsStorageFactory, MemoryStorageFactory, Storage, StorageConfig,
        StorageCredential, StorageCredentialProvider, StorageCredentialProviderFactory,
        StorageFactory,
    };

    #[derive(Debug)]
    struct TestCredentialProvider;

    #[async_trait]
    impl StorageCredentialProvider for TestCredentialProvider {
        fn supports_path(&self, _path: &str) -> bool {
            true
        }

        async fn load_credential(&self, _path: &str) -> Result<StorageCredential> {
            unreachable!("unsupported factories must ignore the provider")
        }
    }

    /// A provider rebuilt from the `FileIO` configuration, like a catalog provider.
    #[derive(Debug)]
    struct PortableCredentialProvider {
        endpoint: Option<String>,
    }

    #[async_trait]
    impl StorageCredentialProvider for PortableCredentialProvider {
        fn supports_path(&self, _path: &str) -> bool {
            true
        }

        async fn load_credential(&self, _path: &str) -> Result<StorageCredential> {
            Ok(StorageCredential::new(
                "gs",
                HashMap::from([(
                    GCS_TOKEN.to_string(),
                    self.endpoint.clone().unwrap_or_default(),
                )]),
            ))
        }

        fn factory(&self) -> Result<Arc<dyn StorageCredentialProviderFactory>> {
            Ok(Arc::new(PortableCredentialProviderFactory))
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct PortableCredentialProviderFactory;

    #[typetag::serde]
    impl StorageCredentialProviderFactory for PortableCredentialProviderFactory {
        fn build(&self, config: &StorageConfig) -> Result<Arc<dyn StorageCredentialProvider>> {
            Ok(Arc::new(PortableCredentialProvider {
                endpoint: config.get("endpoint").cloned(),
            }))
        }
    }

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

    /// Memory storage that records whether it was given a credential provider.
    #[derive(Debug, Default, Serialize, Deserialize)]
    struct ProviderRecordingFactory {
        #[serde(skip)]
        received_provider: Arc<AtomicBool>,
    }

    #[typetag::serde]
    impl StorageFactory for ProviderRecordingFactory {
        fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
            self.build_with_credential_provider(config, None)
        }

        fn build_with_credential_provider(
            &self,
            config: &StorageConfig,
            credential_provider: Option<Arc<dyn StorageCredentialProvider>>,
        ) -> Result<Arc<dyn Storage>> {
            self.received_provider
                .store(credential_provider.is_some(), Ordering::SeqCst);
            MemoryStorageFactory.build(config)
        }
    }

    #[tokio::test]
    async fn test_file_io_passes_credential_provider_to_factory() {
        let received_provider = Arc::new(AtomicBool::new(false));
        let file_io = FileIOBuilder::new(Arc::new(ProviderRecordingFactory {
            received_provider: Arc::clone(&received_provider),
        }))
        .with_credential_provider(Arc::new(TestCredentialProvider))
        .build();

        file_io.exists("memory://file").await.unwrap();
        assert!(received_provider.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_file_io_ignores_credentials_for_unsupported_factory() {
        let file_io = FileIOBuilder::new(Arc::new(MemoryStorageFactory))
            .with_credential_provider(Arc::new(TestCredentialProvider))
            .build();

        file_io
            .new_output("memory://file")
            .unwrap()
            .write("data".into())
            .await
            .unwrap();
        assert!(file_io.exists("memory://file").await.unwrap());
    }

    #[test]
    fn test_file_io_drops_unserializable_credential_provider() {
        let file_io = FileIOBuilder::new(Arc::new(MemoryStorageFactory))
            .with_prop("s3.access-key-id", "static-key")
            .with_credential_provider(Arc::new(TestCredentialProvider))
            .build();

        let deserialized = FileIO::deserialize_all(&file_io.serialize_all().unwrap()).unwrap();
        assert!(deserialized.credential_provider.is_none());
        assert_eq!(
            deserialized.config().get("s3.access-key-id"),
            Some(&"static-key".to_string())
        );
        assert_eq!(
            file_io.serialize_all().unwrap(),
            file_io
                .without_credential_provider()
                .serialize_all()
                .unwrap()
        );
    }

    /// A provider whose factory cannot be serialized.
    #[derive(Debug)]
    struct UnserializableFactoryProvider;

    #[async_trait]
    impl StorageCredentialProvider for UnserializableFactoryProvider {
        fn supports_path(&self, _path: &str) -> bool {
            true
        }

        async fn load_credential(&self, _path: &str) -> Result<StorageCredential> {
            unreachable!("serialization tests never load credentials")
        }

        fn factory(&self) -> Result<Arc<dyn StorageCredentialProviderFactory>> {
            Ok(Arc::new(UnserializableFactory))
        }
    }

    #[derive(Debug, Deserialize)]
    struct UnserializableFactory;

    impl Serialize for UnserializableFactory {
        fn serialize<S: serde::Serializer>(
            &self,
            _serializer: S,
        ) -> std::result::Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("cannot serialize"))
        }
    }

    #[typetag::serde]
    impl StorageCredentialProviderFactory for UnserializableFactory {
        fn build(&self, _config: &StorageConfig) -> Result<Arc<dyn StorageCredentialProvider>> {
            unreachable!("never serialized")
        }
    }

    #[test]
    fn test_file_io_drops_provider_whose_factory_fails_to_serialize() {
        let file_io = FileIOBuilder::new(Arc::new(MemoryStorageFactory))
            .with_credential_provider(Arc::new(UnserializableFactoryProvider))
            .build();

        let deserialized = FileIO::deserialize_all(&file_io.serialize_all().unwrap()).unwrap();
        assert!(deserialized.credential_provider.is_none());
    }

    #[test]
    fn test_file_io_deserializes_without_unknown_credential_provider() {
        let file_io = FileIOBuilder::new(Arc::new(MemoryStorageFactory))
            .with_prop("s3.access-key-id", "static-key")
            .with_credential_provider(Arc::new(PortableCredentialProvider { endpoint: None }))
            .build();
        // A receiving binary that does not link the provider's factory.
        let mut serialized: serde_json::Value =
            serde_json::from_slice(&file_io.serialize_all().unwrap()).unwrap();
        serialized["credential_provider"]["type"] = "UnlinkedProviderFactory".into();

        let deserialized =
            FileIO::deserialize_all(&serde_json::to_vec(&serialized).unwrap()).unwrap();
        assert!(deserialized.credential_provider.is_none());
        assert_eq!(
            deserialized.config().get("s3.access-key-id"),
            Some(&"static-key".to_string())
        );
    }

    #[tokio::test]
    async fn test_file_io_rebuilds_credential_provider_after_serialization() {
        let file_io = FileIOBuilder::new(Arc::new(MemoryStorageFactory))
            .with_prop("endpoint", "https://catalog/credentials")
            .with_credential_provider(Arc::new(PortableCredentialProvider { endpoint: None }))
            .build();

        let deserialized = FileIO::deserialize_all(&file_io.serialize_all().unwrap()).unwrap();
        let credential = deserialized
            .credential_provider
            .unwrap()
            .load_credential("gs://bucket/file")
            .await
            .unwrap();
        // The provider is rebuilt from the deserialized configuration.
        assert_eq!(
            credential.config().get(GCS_TOKEN).map(String::as_str),
            Some("https://catalog/credentials")
        );
    }

    #[tokio::test]
    async fn test_memory_file_io_serialization_roundtrip() {
        let file_io = FileIOBuilder::new(Arc::new(MemoryStorageFactory))
            .with_prop("test-property", "test-value")
            .with_prop("s3.session-token", "test-token")
            .build();

        file_io
            .new_output("memory://test/file.txt")
            .unwrap()
            .write("test".into())
            .await
            .unwrap();
        assert!(file_io.storage.get().is_some());

        let serialized = file_io.serialize_all().unwrap();
        let deserialized = FileIO::deserialize_all(&serialized).unwrap();
        assert!(deserialized.storage.get().is_none());
        assert_eq!(
            deserialized.config().get("test-property"),
            Some(&"test-value".to_string())
        );
        assert_eq!(
            deserialized.config().get("s3.session-token"),
            Some(&"test-token".to_string())
        );

        deserialized
            .new_output("memory://test/roundtrip.txt")
            .unwrap()
            .write("roundtrip".into())
            .await
            .unwrap();
        assert_eq!(
            deserialized
                .new_input("memory://test/roundtrip.txt")
                .unwrap()
                .read()
                .await
                .unwrap(),
            Bytes::from("roundtrip")
        );
        assert!(deserialized.storage.get().is_some());
    }

    #[tokio::test]
    async fn test_local_fs_file_io_serialization_roundtrip() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("roundtrip.txt");
        let path = path.to_str().unwrap();
        let file_io = FileIOBuilder::new(Arc::new(LocalFsStorageFactory))
            .with_prop("test-property", "test-value")
            .build();

        file_io
            .new_output(path)
            .unwrap()
            .write("roundtrip".into())
            .await
            .unwrap();
        assert!(file_io.storage.get().is_some());

        let serialized = file_io.serialize_all().unwrap();
        let deserialized = FileIO::deserialize_all(&serialized).unwrap();
        assert!(deserialized.storage.get().is_none());
        assert_eq!(
            deserialized.config().get("test-property"),
            Some(&"test-value".to_string())
        );
        assert!(deserialized.exists(path).await.unwrap());
        assert_eq!(
            deserialized.new_input(path).unwrap().read().await.unwrap(),
            Bytes::from("roundtrip")
        );
        assert!(deserialized.storage.get().is_some());
    }
}
