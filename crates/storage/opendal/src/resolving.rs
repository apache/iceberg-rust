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
    StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};
use opendal::Scheme;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::OpenDalStorage;
#[cfg(feature = "opendal-s3")]
use crate::s3::CustomAwsCredentialLoader;

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

/// Parse a URL scheme string into an [`opendal::Scheme`].
fn parse_scheme(scheme: &str) -> Result<Scheme> {
    match scheme {
        SCHEME_MEMORY => Ok(Scheme::Memory),
        SCHEME_FILE | "" => Ok(Scheme::Fs),
        SCHEME_S3 | SCHEME_S3A | SCHEME_S3N => Ok(Scheme::S3),
        SCHEME_GS | SCHEME_GCS => Ok(Scheme::Gcs),
        SCHEME_OSS => Ok(Scheme::Oss),
        SCHEME_ABFSS | SCHEME_ABFS | SCHEME_WASBS | SCHEME_WASB => Ok(Scheme::Azdls),
        s => s.parse::<Scheme>().map_err(|e| {
            Error::new(
                ErrorKind::FeatureUnsupported,
                format!("Unsupported storage scheme: {s}: {e}"),
            )
        }),
    }
}

/// Extract the scheme string from a path URL.
fn extract_scheme(path: &str) -> Result<String> {
    let url = Url::parse(path).map_err(|e| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid path: {path}, failed to parse URL: {e}"),
        )
    })?;
    Ok(url.scheme().to_string())
}

/// Build an [`OpenDalStorage`] variant for the given scheme and config properties.
fn build_storage_for_scheme(
    scheme: &str,
    props: &HashMap<String, String>,
    #[cfg(feature = "opendal-s3")] customized_credential_load: &Option<CustomAwsCredentialLoader>,
) -> Result<OpenDalStorage> {
    match parse_scheme(scheme)? {
        #[cfg(feature = "opendal-s3")]
        Scheme::S3 => {
            let config = crate::s3::s3_config_parse(props.clone())?;
            Ok(OpenDalStorage::S3 {
                configured_scheme: scheme.to_string(),
                config: Arc::new(config),
                customized_credential_load: customized_credential_load.clone(),
            })
        }
        #[cfg(feature = "opendal-gcs")]
        Scheme::Gcs => {
            let config = crate::gcs::gcs_config_parse(props.clone())?;
            Ok(OpenDalStorage::Gcs {
                config: Arc::new(config),
            })
        }
        #[cfg(feature = "opendal-oss")]
        Scheme::Oss => {
            let config = crate::oss::oss_config_parse(props.clone())?;
            Ok(OpenDalStorage::Oss {
                config: Arc::new(config),
            })
        }
        #[cfg(feature = "opendal-azdls")]
        Scheme::Azdls => {
            let configured_scheme: crate::azdls::AzureStorageScheme = scheme.parse()?;
            let config = crate::azdls::azdls_config_parse(props.clone())?;
            Ok(OpenDalStorage::Azdls {
                configured_scheme,
                config: Arc::new(config),
            })
        }
        #[cfg(feature = "opendal-fs")]
        Scheme::Fs => Ok(OpenDalStorage::LocalFs),
        #[cfg(feature = "opendal-memory")]
        Scheme::Memory => Ok(OpenDalStorage::Memory(crate::memory::memory_config_build()?)),
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
    #[serde(skip)]
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
/// for subsequent operations.
#[derive(Debug, Serialize, Deserialize)]
pub struct OpenDalResolvingStorage {
    /// Configuration properties shared across all backends.
    props: HashMap<String, String>,
    /// Cache of scheme → storage mappings.
    #[serde(skip, default)]
    storages: RwLock<HashMap<String, Arc<OpenDalStorage>>>,
    /// Custom AWS credential loader for S3 storage.
    #[cfg(feature = "opendal-s3")]
    #[serde(skip)]
    customized_credential_load: Option<CustomAwsCredentialLoader>,
}

impl OpenDalResolvingStorage {
    /// Resolve the storage for the given path by extracting the scheme and
    /// returning the cached or newly-created [`OpenDalStorage`].
    fn resolve(&self, path: &str) -> Result<Arc<OpenDalStorage>> {
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

        let storage = build_storage_for_scheme(
            &scheme,
            &self.props,
            #[cfg(feature = "opendal-s3")]
            &self.customized_credential_load,
        )?;
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
        // Group paths by scheme so each resolved storage receives a batch,
        // avoiding repeated operator creation per path.
        let mut grouped: HashMap<String, Vec<String>> = HashMap::new();
        while let Some(path) = paths.next().await {
            let scheme = extract_scheme(&path)?;
            grouped.entry(scheme).or_default().push(path);
        }

        for (_, paths) in grouped {
            let storage = self.resolve(&paths[0])?;
            storage
                .delete_stream(futures::stream::iter(paths).boxed())
                .await?;
        }
        Ok(())
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(
            Arc::new(self.resolve(path)?.as_ref().clone()),
            path.to_string(),
        ))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(
            Arc::new(self.resolve(path)?.as_ref().clone()),
            path.to_string(),
        ))
    }
}
