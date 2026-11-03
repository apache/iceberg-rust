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

//! Resolving storage that auto-detects the scheme from a path and delegates
//! to the appropriate [`OpenDalStorage`] variant.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageCredential, StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::prefixed::{DELETE_BATCH_SIZE, PrefixedStorage};
#[cfg(feature = "opendal-s3")]
use crate::s3::CustomAwsCredentialLoader;
use crate::{OpenDalClientConfig, OpenDalStorage};

/// Schemes supported by OpenDalResolvingStorage
pub const SCHEME_MEMORY: &str = "memory";
pub const SCHEME_FILE: &str = "file";
pub const SCHEME_S3: &str = "s3";
pub const SCHEME_S3A: &str = "s3a";
pub const SCHEME_S3N: &str = "s3n";
pub const SCHEME_GS: &str = "gs";
pub const SCHEME_GCS: &str = "gcs";
pub const SCHEME_OSS: &str = "oss";
pub const SCHEME_ABFSS: &str = "abfss";
pub const SCHEME_ABFS: &str = "abfs";
pub const SCHEME_WASBS: &str = "wasbs";
pub const SCHEME_WASB: &str = "wasb";
pub const SCHEME_HF: &str = "hf";
pub const SCHEME_HDFS: &str = "hdfs";

/// Parse a URL scheme string.
fn parse_scheme(scheme: &str) -> Result<&'static str> {
    match scheme {
        SCHEME_MEMORY => Ok("memory"),
        SCHEME_FILE | "" => Ok("file"),
        SCHEME_S3 | SCHEME_S3A | SCHEME_S3N => Ok("s3"),
        SCHEME_GS | SCHEME_GCS => Ok("gcs"),
        SCHEME_OSS => Ok("oss"),
        SCHEME_ABFSS | SCHEME_ABFS | SCHEME_WASBS | SCHEME_WASB => Ok("azdls"),
        SCHEME_HF => Ok("hf"),
        SCHEME_HDFS => Ok("hdfs"),
        s => Err(Error::new(
            ErrorKind::FeatureUnsupported,
            format!("Unsupported storage scheme: {s}"),
        )),
    }
}

/// Extract the scheme from a path URL.
fn extract_scheme(path: &str) -> Result<&'static str> {
    let url = Url::parse(path).map_err(|e| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid path: {path}, failed to parse URL: {e}"),
        )
    })?;
    parse_scheme(url.scheme())
}

/// Build an [`OpenDalStorage`] variant for the given scheme and config properties.
fn build_storage_for_scheme(
    scheme: &'static str,
    props: &HashMap<String, String>,
    #[cfg(feature = "opendal-s3")] customized_credential_load: &Option<CustomAwsCredentialLoader>,
) -> Result<OpenDalStorage> {
    let client_config = OpenDalClientConfig::from_properties(props)?;
    match scheme {
        #[cfg(feature = "opendal-s3")]
        "s3" => {
            let config = crate::s3::s3_config_parse(props.clone())?;
            Ok(OpenDalStorage::S3 {
                config: Arc::new(config),
                customized_credential_load: customized_credential_load.clone(),
                client_config,
            })
        }
        #[cfg(feature = "opendal-gcs")]
        "gcs" => {
            let config = crate::gcs::gcs_config_parse(props.clone())?;
            Ok(OpenDalStorage::Gcs {
                config: Arc::new(config),
                client_config,
            })
        }
        #[cfg(feature = "opendal-oss")]
        "oss" => {
            let config = crate::oss::oss_config_parse(props.clone())?;
            Ok(OpenDalStorage::Oss {
                config: Arc::new(config),
                client_config,
            })
        }
        #[cfg(feature = "opendal-azdls")]
        "azdls" => {
            let config = crate::azdls::azdls_config_parse(props.clone())?;
            Ok(OpenDalStorage::Azdls {
                config: Arc::new(config),
                client_config,
            })
        }
        #[cfg(feature = "opendal-fs")]
        "file" => Ok(OpenDalStorage::LocalFs { client_config }),
        #[cfg(feature = "opendal-memory")]
        "memory" => Ok(OpenDalStorage::Memory {
            operator: crate::memory::memory_config_build()?,
            client_config,
        }),
        #[cfg(feature = "opendal-hf")]
        "hf" => {
            let config = crate::hf::hf_config_parse(props.clone())?;
            Ok(OpenDalStorage::Hf {
                config: Arc::new(config),
                client_config,
            })
        }
        #[cfg(feature = "opendal-hdfs-native")]
        "hdfs" => {
            let config = crate::hdfs_native::hdfs_native_config_parse(props.clone())?;
            Ok(OpenDalStorage::HdfsNative(crate::HdfsNativeStorage::new(
                config,
                client_config,
            )))
        }
        unsupported => Err(Error::new(
            ErrorKind::FeatureUnsupported,
            format!("Unsupported storage scheme: {unsupported}"),
        )),
    }
}

/// A resolving storage factory that creates [`OpenDalResolvingStorage`] instances.
///
/// This factory accepts paths from any supported storage system and dynamically
/// delegates operations to the appropriate [`OpenDalStorage`] variant based on
/// the path scheme.
///
/// Supports [storage credentials](iceberg::io::StorageConfig::storage_credentials).
///
/// # Serialization
///
/// Serialization fails when a custom S3 credential loader is configured because the loader holds
/// process-local state that cannot be reconstructed in another process. Construct the factory
/// without a custom loader before serializing it.
///
/// # Example
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use iceberg::io::FileIOBuilder;
/// use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
///
/// let factory = OpenDalResolvingStorageFactory::new();
/// let file_io = FileIOBuilder::new(Arc::new(factory))
///     .with_prop("s3.region", "us-east-1")
///     .build();
/// ```
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OpenDalResolvingStorageFactory {
    /// Custom AWS credential loader for S3 storage.
    #[cfg(feature = "opendal-s3")]
    #[serde(
        skip_deserializing,
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::serialize_custom_credential_loader"
    )]
    customized_credential_load: Option<CustomAwsCredentialLoader>,
}

impl Default for OpenDalResolvingStorageFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenDalResolvingStorageFactory {
    /// Create a new resolving storage factory.
    pub fn new() -> Self {
        Self {
            #[cfg(feature = "opendal-s3")]
            customized_credential_load: None,
        }
    }

    /// Set a custom AWS credential loader for S3 storage.
    #[cfg(feature = "opendal-s3")]
    pub fn with_s3_credential_loader(mut self, loader: CustomAwsCredentialLoader) -> Self {
        self.customized_credential_load = Some(loader);
        self
    }
}

#[typetag::serde]
impl StorageFactory for OpenDalResolvingStorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(OpenDalResolvingStorage {
            props: config.props().clone(),
            storage_credentials: config.storage_credentials().to_vec(),
            storages: RwLock::new(HashMap::new()),
            #[cfg(feature = "opendal-s3")]
            customized_credential_load: self.customized_credential_load.clone(),
        }))
    }
}

/// A resolving storage that auto-detects the scheme from a path and delegates
/// to the appropriate [`OpenDalStorage`] variant.
///
/// Sub-storages are lazily created on first use for each scheme and cached
/// for subsequent operations. Scheme aliases like `s3`/`s3a`/`s3n` map to
/// the same canonical scheme, so they share a storage instance.
#[derive(Debug, Serialize, Deserialize)]
pub struct OpenDalResolvingStorage {
    /// Configuration properties shared across all backends.
    props: HashMap<String, String>,
    /// Credentials that apply to locations under their prefixes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    storage_credentials: Vec<StorageCredential>,
    /// Cache of canonical scheme to storage mappings.
    #[serde(skip, default)]
    storages: RwLock<HashMap<&'static str, Arc<PrefixedStorage>>>,
    /// Custom AWS credential loader for S3 storage.
    #[cfg(feature = "opendal-s3")]
    #[serde(skip)]
    customized_credential_load: Option<CustomAwsCredentialLoader>,
}

impl OpenDalResolvingStorage {
    /// Resolve the storage for the given path by extracting the canonical scheme and
    /// returning the cached or newly-created storage for it.
    fn resolve(&self, path: &str) -> Result<Arc<PrefixedStorage>> {
        let scheme = extract_scheme(path)?;

        // Fast path: check read lock first.
        {
            let cache = self
                .storages
                .read()
                .map_err(|_| Error::new(ErrorKind::Unexpected, "Storage cache lock poisoned"))?;
            if let Some(storage) = cache.get(&scheme) {
                return Ok(storage.clone());
            }
        }

        // Slow path: build and insert under write lock.
        let mut cache = self
            .storages
            .write()
            .map_err(|_| Error::new(ErrorKind::Unexpected, "Storage cache lock poisoned"))?;

        // Double-check after acquiring write lock.
        if let Some(storage) = cache.get(&scheme) {
            return Ok(storage.clone());
        }

        // A prefix such as `s3://bucket/` or `s3` names its scheme, and never
        // matches the paths of another.
        let storage_credentials = self
            .storage_credentials
            .iter()
            .filter(|credential| {
                let prefix = credential.prefix();
                let name = prefix.split_once(':').map_or(prefix, |(name, _)| name);
                parse_scheme(&name.to_ascii_lowercase()).map_or(true, |s| s == scheme)
            })
            .cloned()
            .collect();
        let config = StorageConfig::builder()
            .props(self.props.clone())
            .storage_credentials(storage_credentials)
            .build();
        let storage = PrefixedStorage::new(&config, |config| {
            build_storage_for_scheme(
                scheme,
                config.props(),
                #[cfg(feature = "opendal-s3")]
                &self.customized_credential_load,
            )
        })?;
        let storage = Arc::new(storage);
        cache.insert(scheme, storage.clone());
        Ok(storage)
    }
}

#[async_trait]
#[typetag::serde]
impl Storage for OpenDalResolvingStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        self.resolve(path)?.exists(path).await
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        self.resolve(path)?.metadata(path).await
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        self.resolve(path)?.read(path).await
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        self.resolve(path)?.reader(path).await
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        self.resolve(path)?.write(path, bs).await
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        self.resolve(path)?.writer(path).await
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.resolve(path)?.delete(path).await
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        self.resolve(path)?.delete_prefix(path).await
    }

    async fn delete_stream(&self, mut paths: BoxStream<'static, String>) -> Result<()> {
        // Group paths by canonical scheme so each resolved storage receives a batch,
        // avoiding repeated operator creation per path.
        let mut grouped: HashMap<&'static str, Vec<String>> = HashMap::new();
        while let Some(path) = paths.next().await {
            let scheme = extract_scheme(&path)?;
            let batch = grouped.entry(scheme).or_default();
            batch.push(path);
            if batch.len() >= DELETE_BATCH_SIZE {
                let batch = std::mem::take(batch);
                let storage = self.resolve(&batch[0])?;
                storage
                    .delete_stream(futures::stream::iter(batch).boxed())
                    .await?;
            }
        }

        for (_, paths) in grouped {
            if paths.is_empty() {
                continue;
            }
            let storage = self.resolve(&paths[0])?;
            storage
                .delete_stream(futures::stream::iter(paths).boxed())
                .await?;
        }
        Ok(())
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        self.resolve(path)?.new_input(path)
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        self.resolve(path)?.new_output(path)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::OPENDAL_IO_TIMEOUT_MS;

    #[cfg(feature = "opendal-s3")]
    #[derive(Debug)]
    struct EmptyCredentialLoader;

    #[cfg(feature = "opendal-s3")]
    impl crate::s3::ProvideCredential for EmptyCredentialLoader {
        type Credential = crate::s3::AwsCredential;

        async fn provide_credential(
            &self,
            _ctx: &reqsign_core::Context,
        ) -> reqsign_core::Result<Option<Self::Credential>> {
            Ok(None)
        }
    }

    #[cfg(feature = "opendal-s3")]
    #[test]
    fn test_custom_credential_loader_serialization_fails() {
        let factory = OpenDalResolvingStorageFactory::new()
            .with_s3_credential_loader(CustomAwsCredentialLoader::new(EmptyCredentialLoader));
        let file_io = iceberg::io::FileIOBuilder::new(Arc::new(factory)).build();

        let err = file_io.serialize_all().unwrap_err();
        assert!(
            err.to_string()
                .contains("custom AWS credential loaders cannot be serialized")
        );
    }

    /// Builds a resolving storage with empty props, suitable for `resolve()`
    /// calls that don't actually hit any backend.
    fn empty_resolving_storage() -> OpenDalResolvingStorage {
        OpenDalResolvingStorage {
            props: HashMap::new(),
            storage_credentials: Vec::new(),
            storages: RwLock::new(HashMap::new()),
            #[cfg(feature = "opendal-s3")]
            customized_credential_load: None,
        }
    }

    #[test]
    fn test_resolve_propagates_io_timeout() {
        let mut storage = empty_resolving_storage();
        storage
            .props
            .insert(OPENDAL_IO_TIMEOUT_MS.to_string(), "45000".to_string());

        let paths: &[&str] = &[
            #[cfg(feature = "opendal-memory")]
            "memory:/key",
            #[cfg(feature = "opendal-fs")]
            "file:/key",
            #[cfg(feature = "opendal-s3")]
            "s3://bucket/key",
            #[cfg(feature = "opendal-gcs")]
            "gs://bucket/key",
            #[cfg(feature = "opendal-oss")]
            "oss://bucket/key",
            #[cfg(feature = "opendal-azdls")]
            "abfss://myfs@myaccount.dfs.core.windows.net/key",
            #[cfg(feature = "opendal-hf")]
            "hf://datasets/user/repo/key",
        ];
        for path in paths {
            let resolved = storage.resolve(path).unwrap();
            assert_eq!(
                resolved.storage_for(path).client_config().io_timeout(),
                Duration::from_secs(45),
                "{path}"
            );
        }
    }

    #[cfg(feature = "opendal-memory")]
    #[test]
    fn test_resolve_ignores_credentials_for_other_schemes() {
        let mut storage = empty_resolving_storage();
        storage.storage_credentials = ["s3://bucket/", "s3"]
            .map(|prefix| {
                StorageCredential::new(
                    prefix,
                    HashMap::from([(OPENDAL_IO_TIMEOUT_MS.to_string(), "invalid".to_string())]),
                )
                .unwrap()
            })
            .to_vec();

        assert!(storage.resolve("memory:/key").is_ok());
    }

    #[cfg(feature = "opendal-s3")]
    #[test]
    fn test_resolve_applies_a_scheme_name_prefix_to_its_aliases() {
        let mut storage = empty_resolving_storage();
        storage.storage_credentials = vec![
            StorageCredential::new(
                "s3",
                HashMap::from([(OPENDAL_IO_TIMEOUT_MS.to_string(), "2000".to_string())]),
            )
            .unwrap(),
        ];

        let timeouts = ["s3://bucket/key", "s3a://bucket/key"].map(|path| {
            storage
                .resolve(path)
                .unwrap()
                .storage_for(path)
                .client_config()
                .io_timeout()
        });

        assert_eq!(timeouts, [Duration::from_secs(2); 2]);
    }

    /// A resolving storage holding one delete batch of memory files, and their paths.
    #[cfg(feature = "opendal-memory")]
    async fn storage_with_a_full_batch() -> (OpenDalResolvingStorage, Vec<String>) {
        let storage = empty_resolving_storage();
        let paths: Vec<_> = (0..DELETE_BATCH_SIZE)
            .map(|i| format!("memory:/table/{i}"))
            .collect();
        for path in &paths {
            storage.write(path, Bytes::from("data")).await.unwrap();
        }
        (storage, paths)
    }

    #[cfg(feature = "opendal-memory")]
    #[tokio::test(start_paused = true)]
    async fn test_delete_stream_deletes_a_full_batch_before_the_stream_ends() {
        let (storage, paths) = storage_with_a_full_batch().await;
        let stream = futures::stream::iter(paths.clone())
            .chain(futures::stream::pending())
            .boxed();

        tokio::time::timeout(Duration::from_secs(1), storage.delete_stream(stream))
            .await
            .unwrap_err();

        for path in &paths {
            assert!(!storage.exists(path).await.unwrap(), "{path}");
        }
    }

    #[cfg(feature = "opendal-memory")]
    #[tokio::test]
    async fn test_delete_stream_deletes_a_stream_of_exactly_one_batch() {
        let (storage, paths) = storage_with_a_full_batch().await;

        storage
            .delete_stream(futures::stream::iter(paths.clone()).boxed())
            .await
            .unwrap();

        for path in &paths {
            assert!(!storage.exists(path).await.unwrap(), "{path}");
        }
    }

    #[cfg(feature = "opendal-s3")]
    #[test]
    fn test_resolve_s3_aliases_share_instance() {
        let storage = empty_resolving_storage();

        // All three S3-family schemes must collapse to a single cached
        // storage so that catalogs handing the resolver a mix
        // of `s3://`, `s3a://`, `s3n://` paths don't rebuild operators.
        let a = storage.resolve("s3://bucket/key").unwrap();
        let b = storage.resolve("s3a://bucket/key").unwrap();
        let c = storage.resolve("s3n://bucket/key").unwrap();

        assert!(Arc::ptr_eq(&a, &b), "s3 and s3a should share one instance");
        assert!(Arc::ptr_eq(&a, &c), "s3 and s3n should share one instance");
    }

    #[cfg(feature = "opendal-hdfs-native")]
    #[test]
    fn test_resolve_hdfs_native_returns_hdfs_variant() {
        let storage = empty_resolving_storage();

        let path = "hdfs://nameservice1/a/b";
        let resolved = storage.resolve(path).unwrap();

        assert!(
            matches!(resolved.storage_for(path), OpenDalStorage::HdfsNative(_)),
            "expected HdfsNative variant, got {resolved:?}"
        );
    }

    #[cfg(feature = "opendal-hdfs-native")]
    #[test]
    fn test_resolve_hdfs_native_distinct_authorities_share_instance() {
        let storage = empty_resolving_storage();

        let a = storage.resolve("hdfs://ns1/a").unwrap();
        let b = storage.resolve("hdfs://ns2/b").unwrap();

        assert!(
            Arc::ptr_eq(&a, &b),
            "different authorities should share the OpenDalStorage::HdfsNative instance (operator cache is internal)"
        );
    }

    #[cfg(feature = "opendal-azdls")]
    #[test]
    fn test_resolve_azdls_aliases_share_instance() {
        let storage = empty_resolving_storage();

        let path_for = |scheme: &str| {
            format!("{scheme}://myfs@myaccount.dfs.core.windows.net/path/to/file.parquet")
        };

        // All Azure schemes collapse onto one cached instance.
        let abfss = storage.resolve(&path_for("abfss")).unwrap();
        let abfs = storage.resolve(&path_for("abfs")).unwrap();

        assert!(
            Arc::ptr_eq(&abfss, &abfs),
            "abfss and abfs should share one instance"
        );
    }
}
