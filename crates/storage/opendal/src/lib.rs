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

//! OpenDAL-based storage implementation for Apache Iceberg.
//!
//! This crate provides [`OpenDalStorage`] and [`OpenDalStorageFactory`],
//! which implement the [`Storage`] and
//! [`StorageFactory`] traits from the `iceberg` crate
//! using [OpenDAL](https://opendal.apache.org/) as the backend.

mod utils;

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use cfg_if::cfg_if;
use futures::StreamExt;
use futures::stream::BoxStream;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageCredentialProvider, StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};
use opendal::Operator;
use opendal::layers::{RetryLayer, TimeoutLayer};
use serde::{Deserialize, Serialize};
use utils::from_opendal_error;

cfg_if! {
    if #[cfg(feature = "opendal-azdls")] {
        mod azdls;
        use azdls::*;
        use opendal::services::AzdlsConfig;
    }
}

cfg_if! {
    if #[cfg(feature = "opendal-hf")] {
        mod hf;
        use hf::*;
        use opendal::services::HfConfig;
    }
}

cfg_if! {
    if #[cfg(feature = "opendal-fs")] {
        mod fs;
        use fs::*;
    }
}

cfg_if! {
    if #[cfg(feature = "opendal-gcs")] {
        mod gcs;
        use gcs::*;
        use opendal::services::GcsConfig;
    }
}

cfg_if! {
    if #[cfg(feature = "opendal-memory")] {
        mod memory;
        use memory::*;
    }
}

cfg_if! {
    if #[cfg(feature = "opendal-oss")] {
        mod oss;
        use opendal::services::OssConfig;
        use oss::*;
    }
}

cfg_if! {
    if #[cfg(feature = "opendal-s3")] {
        mod s3;
        use opendal::services::S3Config;
        pub use s3::*;
    }
}

mod resolving;
pub use resolving::{OpenDalResolvingStorage, OpenDalResolvingStorageFactory};

/// OpenDAL-based storage factory.
///
/// Maps scheme to the corresponding OpenDalStorage storage variant.
/// Use this factory with `FileIOBuilder::new(factory)` to create FileIO instances.
///
/// # Serialization
///
/// The receiving binary must enable the feature corresponding to the serialized backend variant.
/// For example, deserializing `OpenDalStorageFactory::S3` requires the `opendal-s3` feature.
///
/// Serialization fails when the `OpenDalStorageFactory::S3` variant contains a custom AWS
/// credential loader because the loader holds process-local state that cannot be reconstructed in
/// another process. Construct the factory without a custom loader before serializing it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum OpenDalStorageFactory {
    /// Memory storage factory.
    #[cfg(feature = "opendal-memory")]
    Memory,
    /// Local filesystem storage factory.
    #[cfg(feature = "opendal-fs")]
    Fs,
    /// S3 storage factory.
    #[cfg(feature = "opendal-s3")]
    S3 {
        /// Custom AWS credential loader.
        #[serde(
            skip_deserializing,
            skip_serializing_if = "Option::is_none",
            serialize_with = "serialize_custom_credential_loader"
        )]
        customized_credential_load: Option<CustomAwsCredentialLoader>,
    },
    /// GCS storage factory.
    #[cfg(feature = "opendal-gcs")]
    Gcs,
    /// OSS storage factory.
    #[cfg(feature = "opendal-oss")]
    Oss,
    /// Azure Data Lake Storage factory.
    #[cfg(feature = "opendal-azdls")]
    Azdls,
    /// HuggingFace Hub storage factory.
    #[cfg(feature = "opendal-hf")]
    Hf,
}

#[cfg(feature = "opendal-s3")]
pub(crate) fn serialize_custom_credential_loader<S>(
    _loader: &Option<CustomAwsCredentialLoader>,
    _serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    Err(serde::ser::Error::custom(
        "custom AWS credential loaders cannot be serialized",
    ))
}

#[typetag::serde(name = "OpenDalStorageFactory")]
impl StorageFactory for OpenDalStorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        self.build_with_credentials(config, None)
    }

    #[allow(unused_variables)]
    fn build_with_credentials(
        &self,
        config: &StorageConfig,
        credential_provider: Option<Arc<dyn StorageCredentialProvider>>,
    ) -> Result<Arc<dyn Storage>> {
        #[allow(unreachable_patterns)]
        let supports_credential_provider = match self {
            #[cfg(feature = "opendal-s3")]
            OpenDalStorageFactory::S3 { .. } => true,
            #[cfg(feature = "opendal-gcs")]
            OpenDalStorageFactory::Gcs => true,
            _ => false,
        };
        if credential_provider.is_some() && !supports_credential_provider {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "OpenDAL storage factory does not support refreshable credentials for this backend",
            ));
        }

        match self {
            #[cfg(feature = "opendal-memory")]
            OpenDalStorageFactory::Memory => {
                Ok(Arc::new(OpenDalStorage::Memory(memory_config_build()?)))
            }
            #[cfg(feature = "opendal-fs")]
            OpenDalStorageFactory::Fs => Ok(Arc::new(OpenDalStorage::LocalFs)),
            #[cfg(feature = "opendal-s3")]
            OpenDalStorageFactory::S3 {
                customized_credential_load,
            } => Ok(Arc::new(OpenDalStorage::S3 {
                config: s3_config_parse(config.props().clone())?.into(),
                customized_credential_load: customized_credential_load.clone(),
                credential_provider,
            })),
            #[cfg(feature = "opendal-gcs")]
            OpenDalStorageFactory::Gcs => Ok(Arc::new(OpenDalStorage::Gcs {
                config: gcs_config_parse(config.props().clone())?.into(),
                credential_provider,
            })),
            #[cfg(feature = "opendal-oss")]
            OpenDalStorageFactory::Oss => Ok(Arc::new(OpenDalStorage::Oss {
                config: oss_config_parse(config.props().clone())?.into(),
            })),
            #[cfg(feature = "opendal-azdls")]
            OpenDalStorageFactory::Azdls => Ok(Arc::new(OpenDalStorage::Azdls {
                config: azdls_config_parse(config.props().clone())?.into(),
            })),
            #[cfg(feature = "opendal-hf")]
            OpenDalStorageFactory::Hf => Ok(Arc::new(OpenDalStorage::Hf {
                config: hf_config_parse(config.props().clone())?.into(),
            })),
            #[cfg(all(
                not(feature = "opendal-memory"),
                not(feature = "opendal-fs"),
                not(feature = "opendal-s3"),
                not(feature = "opendal-gcs"),
                not(feature = "opendal-oss"),
                not(feature = "opendal-azdls"),
                not(feature = "opendal-hf"),
            ))]
            _ => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "No storage service has been enabled",
            )),
        }
    }
}

/// Default memory operator for serde deserialization.
#[cfg(feature = "opendal-memory")]
fn default_memory_operator() -> Operator {
    memory_config_build().expect("Failed to create default memory operator")
}

/// OpenDAL-based storage implementation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum OpenDalStorage {
    /// Memory storage variant.
    #[cfg(feature = "opendal-memory")]
    Memory(#[serde(skip, default = "self::default_memory_operator")] Operator),
    /// Local filesystem storage variant.
    #[cfg(feature = "opendal-fs")]
    LocalFs,
    /// S3 storage variant.
    ///
    /// Accepts any S3-family URL (`s3://`, `s3a://`, `s3n://`); the scheme is
    /// derived from the path at call time.
    #[cfg(feature = "opendal-s3")]
    S3 {
        /// S3 configuration.
        config: Arc<S3Config>,
        /// Custom AWS credential loader.
        #[serde(skip)]
        customized_credential_load: Option<CustomAwsCredentialLoader>,
        /// Provider of refreshable vended credentials, supplied by the catalog.
        #[serde(skip)]
        credential_provider: Option<Arc<dyn StorageCredentialProvider>>,
    },
    /// GCS storage variant.
    #[cfg(feature = "opendal-gcs")]
    Gcs {
        /// GCS configuration.
        config: Arc<GcsConfig>,
        /// Provider of refreshable vended credentials, supplied by the catalog.
        #[serde(skip)]
        credential_provider: Option<Arc<dyn StorageCredentialProvider>>,
    },
    /// OSS storage variant.
    #[cfg(feature = "opendal-oss")]
    Oss {
        /// OSS configuration.
        config: Arc<OssConfig>,
    },
    /// Azure Data Lake Storage variant.
    ///
    /// Accepts paths of the form
    /// `abfs[s]://<filesystem>@<account>.dfs.<endpoint-suffix>/<path>` or
    /// `wasb[s]://<container>@<account>.blob.<endpoint-suffix>/<path>`.
    /// The scheme is derived from the path at call time.
    #[cfg(feature = "opendal-azdls")]
    Azdls {
        /// Azure DLS configuration.
        config: Arc<AzdlsConfig>,
    },
    /// HuggingFace Hub storage variant.
    ///
    /// Accepts paths of the form
    /// `hf://<repo_type>/<owner>/<repo>[@<revision>]/<path_in_repo>`,
    /// where `<repo_type>` must be one of `models`, `datasets`, `spaces`, or `buckets`.
    #[cfg(feature = "opendal-hf")]
    Hf {
        /// HuggingFace Hub configuration (token + endpoint).
        config: Arc<HfConfig>,
    },
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum DeleteCredentialScope {
    Static,
    Dynamic(DynamicCredentialScope),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum DynamicCredentialScope {
    Unscoped,
    Prefix(String),
}

impl DynamicCredentialScope {
    fn from_prefix(prefix: Option<&str>) -> Self {
        match prefix {
            Some(prefix) => Self::Prefix(prefix.to_string()),
            None => Self::Unscoped,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct DeleteBatchKey {
    storage: String,
    credential_scope: DeleteCredentialScope,
}

impl OpenDalStorage {
    /// Creates operator from path.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`](iceberg::io::FileIO).
    ///
    /// # Returns
    ///
    /// The return value consists of two parts:
    ///
    /// * An [`opendal::Operator`] instance used to operate on file.
    /// * Relative path to the root uri of [`opendal::Operator`].
    #[allow(unreachable_code, unused_variables)]
    pub(crate) fn create_operator<'a>(
        &self,
        path: &'a impl AsRef<str>,
    ) -> Result<(Operator, &'a str)> {
        self.create_operator_with_scope(path, None)
    }

    /// Creates an operator, optionally binding dynamic credentials to the exact
    /// scope used to group a bulk-delete batch.
    #[allow(unreachable_code, unused_variables)]
    fn create_operator_with_scope<'a>(
        &self,
        path: &'a impl AsRef<str>,
        credential_scope: Option<&DynamicCredentialScope>,
    ) -> Result<(Operator, &'a str)> {
        let path = path.as_ref();
        let (operator, relative_path): (Operator, &str) = match self {
            #[cfg(feature = "opendal-memory")]
            OpenDalStorage::Memory(op) => {
                if let Some(stripped) = path.strip_prefix("memory:/") {
                    (op.clone(), stripped)
                } else {
                    (op.clone(), &path[1..])
                }
            }
            #[cfg(feature = "opendal-fs")]
            OpenDalStorage::LocalFs => {
                let op = fs_config_build()?;
                if let Some(stripped) = path.strip_prefix("file:/") {
                    (op, stripped)
                } else {
                    (op, &path[1..])
                }
            }
            #[cfg(feature = "opendal-s3")]
            OpenDalStorage::S3 {
                config,
                customized_credential_load,
                credential_provider,
            } => {
                let op = s3_config_build(
                    config,
                    customized_credential_load,
                    credential_provider,
                    path,
                    credential_scope,
                )?;
                let op_info = op.info();

                // Use the URL scheme in the path for prefix matching. This enables
                // use of S3-compatible storage backends using custom schemes (e.g., `minio://`, `r2://`).
                let url = url::Url::parse(path).map_err(|e| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid s3 url: {path}: {e}"),
                    )
                })?;
                let prefix = format!("{}://{}/", url.scheme(), op_info.name());
                if path.starts_with(&prefix) {
                    (op, &path[prefix.len()..])
                } else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid s3 url: {path}, should start with {prefix}"),
                    ));
                }
            }
            #[cfg(feature = "opendal-gcs")]
            OpenDalStorage::Gcs {
                config,
                credential_provider,
            } => {
                let operator =
                    gcs_config_build(config, credential_provider, path, credential_scope)?;
                let url = url::Url::parse(path).map_err(|e| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid gcs url: {path}: {e}"),
                    )
                })?;
                let prefix = format!("{}://{}/", url.scheme(), operator.info().name());
                if path.starts_with(&prefix) {
                    (operator, &path[prefix.len()..])
                } else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid gcs url: {path}, should start with {prefix}"),
                    ));
                }
            }
            #[cfg(feature = "opendal-oss")]
            OpenDalStorage::Oss { config } => {
                let op = oss_config_build(config, path)?;
                let prefix = format!("oss://{}/", op.info().name());
                if path.starts_with(&prefix) {
                    (op, &path[prefix.len()..])
                } else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid oss url: {path}, should start with {prefix}"),
                    ));
                }
            }
            #[cfg(feature = "opendal-azdls")]
            OpenDalStorage::Azdls { config } => azdls_create_operator(path, config)?,
            #[cfg(feature = "opendal-hf")]
            OpenDalStorage::Hf { config } => hf_config_build(config, path)?,
            #[cfg(all(
                not(feature = "opendal-s3"),
                not(feature = "opendal-fs"),
                not(feature = "opendal-gcs"),
                not(feature = "opendal-oss"),
                not(feature = "opendal-azdls"),
                not(feature = "opendal-hf"),
            ))]
            _ => {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    "No storage service has been enabled",
                ));
            }
        };

        // Apply observability/resilience layers. TimeoutLayer must be
        // inside RetryLayer so each retry attempt is independently
        // bounded — without a per-attempt timeout, a future parked on a
        // silently dropped TCP connection never produces an `Err` and
        // RetryLayer cannot retry, leaving the caller hung indefinitely.
        // See: https://opendal.apache.org/docs/rust/opendal/layers/struct.TimeoutLayer.html
        //
        // Transient errors are common for object stores; we retry temporary
        // failures with exponential backoff. The retry behavior also
        // benefits non-object-store backends.
        let operator = operator.layer(TimeoutLayer::new()).layer(RetryLayer::new());
        Ok((operator, relative_path))
    }

    /// Returns a cache key used by `delete_stream` to group paths by storage operator.
    ///
    /// For most backends the URL host (bucket name) is sufficient. For HF the host
    /// encodes the repo type, not the repo identity, so a more specific key is used.
    fn batch_key_for_path(&self, path: &str) -> String {
        match self {
            #[cfg(feature = "opendal-hf")]
            OpenDalStorage::Hf { .. } => hf_batch_key(path),
            _ => url::Url::parse(path)
                .ok()
                .and_then(|u| u.host_str().map(|s| s.to_string()))
                .unwrap_or_default(),
        }
    }

    /// Return the dynamic credential provider that serves `path`, if any.
    fn credential_provider_for_path(
        &self,
        path: &str,
    ) -> Option<&Arc<dyn StorageCredentialProvider>> {
        let provider: &Arc<dyn StorageCredentialProvider> = (match self {
            #[cfg(feature = "opendal-s3")]
            OpenDalStorage::S3 {
                customized_credential_load: None,
                credential_provider: Some(provider),
                ..
            } => Some(provider),
            #[cfg(feature = "opendal-gcs")]
            OpenDalStorage::Gcs {
                credential_provider: Some(provider),
                ..
            } => Some(provider),
            _ => None,
        })?;
        provider.supports_path(path).then_some(provider)
    }

    /// Returns a key that keeps bulk deletes within one operator and credential
    /// scope. Loading the credential is normally a cache hit and avoids rebuilding
    /// an operator for every path while preventing a batch from crossing prefixes.
    async fn delete_batch_key_for_path(&self, path: &str) -> Result<DeleteBatchKey> {
        let credential_scope = match self.credential_provider_for_path(path) {
            Some(provider) => {
                let credential = provider.load_credential(path).await?;
                if credential
                    .prefix()
                    .is_some_and(|prefix| prefix.is_empty() || !path.starts_with(prefix))
                {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "vended credential prefix {:?} does not cover storage location {path:?}",
                            credential.prefix()
                        ),
                    ));
                }
                DeleteCredentialScope::Dynamic(DynamicCredentialScope::from_prefix(
                    credential.prefix(),
                ))
            }
            None => DeleteCredentialScope::Static,
        };

        Ok(DeleteBatchKey {
            storage: self.batch_key_for_path(path),
            credential_scope,
        })
    }

    /// Extracts the relative path from an absolute path without building an operator.
    ///
    /// This is a lightweight alternative to [`create_operator`](Self::create_operator) for cases
    /// where only the relative path is needed (e.g. bulk deletes where the operator is already
    /// available).
    #[allow(unreachable_code, unused_variables)]
    pub(crate) fn relativize_path<'a>(&self, path: &'a str) -> Result<&'a str> {
        match self {
            #[cfg(feature = "opendal-memory")]
            OpenDalStorage::Memory(_) => Ok(path.strip_prefix("memory:/").unwrap_or(&path[1..])),
            #[cfg(feature = "opendal-fs")]
            OpenDalStorage::LocalFs => Ok(path.strip_prefix("file:/").unwrap_or(&path[1..])),
            #[cfg(feature = "opendal-s3")]
            OpenDalStorage::S3 { .. } => {
                let url = url::Url::parse(path)?;
                let bucket = url.host_str().ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid s3 url: {path}, missing bucket"),
                    )
                })?;
                let prefix = format!("{}://{}/", url.scheme(), bucket);
                if path.starts_with(&prefix) {
                    Ok(&path[prefix.len()..])
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid s3 url: {path}, should start with {prefix}"),
                    ))
                }
            }
            #[cfg(feature = "opendal-gcs")]
            OpenDalStorage::Gcs { .. } => {
                let url = url::Url::parse(path)?;
                if !matches!(url.scheme(), "gs" | "gcs") {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid gcs url: {path}, expected gs:// or gcs://"),
                    ));
                }
                let bucket = url.host_str().ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid gcs url: {path}, missing bucket"),
                    )
                })?;
                let prefix = format!("{}://{}/", url.scheme(), bucket);
                if path.starts_with(&prefix) {
                    Ok(&path[prefix.len()..])
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid gcs url: {path}, should start with {prefix}"),
                    ))
                }
            }
            #[cfg(feature = "opendal-oss")]
            OpenDalStorage::Oss { .. } => {
                let url = url::Url::parse(path)?;
                let bucket = url.host_str().ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid oss url: {path}, missing bucket"),
                    )
                })?;
                let prefix = format!("oss://{}/", bucket);
                if path.starts_with(&prefix) {
                    Ok(&path[prefix.len()..])
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid oss url: {path}, should start with {prefix}"),
                    ))
                }
            }
            #[cfg(feature = "opendal-azdls")]
            OpenDalStorage::Azdls { config } => {
                let azure_path = path.parse::<AzureStoragePath>()?;
                match_path_with_config(&azure_path, config)?;
                let relative_path_len = azure_path.path.len();
                Ok(&path[path.len() - relative_path_len..])
            }
            #[cfg(feature = "opendal-hf")]
            OpenDalStorage::Hf { .. } => {
                let parsed = HfUri::parse(path).ok_or_else(|| {
                    Error::new(ErrorKind::DataInvalid, format!("Invalid hf url: {path}"))
                })?;
                Ok(&path[path.len() - parsed.path.len()..])
            }
            #[cfg(all(
                not(feature = "opendal-s3"),
                not(feature = "opendal-fs"),
                not(feature = "opendal-gcs"),
                not(feature = "opendal-oss"),
                not(feature = "opendal-azdls"),
                not(feature = "opendal-hf"),
            ))]
            _ => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "No storage service has been enabled",
            )),
        }
    }
}

#[typetag::serde(name = "OpenDalStorage")]
#[async_trait]
impl Storage for OpenDalStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let (op, relative_path) = self.create_operator(&path)?;
        Ok(op.exists(relative_path).await.map_err(from_opendal_error)?)
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let (op, relative_path) = self.create_operator(&path)?;
        let meta = op.stat(relative_path).await.map_err(from_opendal_error)?;
        Ok(FileMetadata {
            size: meta.content_length(),
        })
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let (op, relative_path) = self.create_operator(&path)?;
        Ok(op
            .read(relative_path)
            .await
            .map_err(from_opendal_error)?
            .to_bytes())
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let (op, relative_path) = self.create_operator(&path)?;
        Ok(Box::new(OpenDalReader(
            op.reader(relative_path).await.map_err(from_opendal_error)?,
        )))
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        let (op, relative_path) = self.create_operator(&path)?;
        op.write(relative_path, bs)
            .await
            .map_err(from_opendal_error)?;
        Ok(())
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let (op, relative_path) = self.create_operator(&path)?;
        Ok(Box::new(OpenDalWriter(
            op.writer(relative_path).await.map_err(from_opendal_error)?,
        )))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let (op, relative_path) = self.create_operator(&path)?;
        Ok(op.delete(relative_path).await.map_err(from_opendal_error)?)
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let (op, relative_path) = self.create_operator(&path)?;
        let path = if relative_path.ends_with('/') {
            relative_path.to_string()
        } else {
            format!("{relative_path}/")
        };
        Ok(op
            .delete_with(&path)
            .recursive(true)
            .await
            .map_err(from_opendal_error)?)
    }

    async fn delete_stream(&self, mut paths: BoxStream<'static, String>) -> Result<()> {
        let mut deleters: HashMap<DeleteBatchKey, opendal::Deleter> = HashMap::new();

        while let Some(path) = paths.next().await {
            let batch_key = self.delete_batch_key_for_path(&path).await?;

            let (relative_path, deleter) = match deleters.entry(batch_key) {
                Entry::Occupied(entry) => {
                    (self.relativize_path(&path)?.to_string(), entry.into_mut())
                }
                Entry::Vacant(entry) => {
                    let credential_scope = match &entry.key().credential_scope {
                        DeleteCredentialScope::Static => None,
                        DeleteCredentialScope::Dynamic(scope) => Some(scope),
                    };
                    let (op, rel) = self.create_operator_with_scope(&path, credential_scope)?;
                    let rel = rel.to_string();
                    let deleter = op.deleter().await.map_err(from_opendal_error)?;
                    (rel, entry.insert(deleter))
                }
            };

            deleter
                .delete(relative_path)
                .await
                .map_err(from_opendal_error)?;
        }

        for (_, mut deleter) in deleters {
            deleter.close().await.map_err(from_opendal_error)?;
        }

        Ok(())
    }

    #[allow(unreachable_code, unused_variables)]
    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    #[allow(unreachable_code, unused_variables)]
    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

// Newtype wrappers for opendal types to satisfy orphan rules.
// We can't implement iceberg's FileRead/FileWrite traits directly on opendal's
// Reader/Writer since neither trait nor type is defined in this crate.

/// Wrapper around `opendal::Reader` that implements `FileRead`.
pub(crate) struct OpenDalReader(pub(crate) opendal::Reader);

#[async_trait]
impl FileRead for OpenDalReader {
    async fn read(&self, range: std::ops::Range<u64>) -> Result<Bytes> {
        Ok(opendal::Reader::read(&self.0, range)
            .await
            .map_err(from_opendal_error)?
            .to_bytes())
    }
}

/// Wrapper around `opendal::Writer` that implements `FileWrite`.
pub(crate) struct OpenDalWriter(pub(crate) opendal::Writer);

#[async_trait]
impl FileWrite for OpenDalWriter {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        Ok(opendal::Writer::write(&mut self.0, bs)
            .await
            .map_err(from_opendal_error)?)
    }

    async fn close(&mut self) -> Result<()> {
        let _ = opendal::Writer::close(&mut self.0)
            .await
            .map_err(from_opendal_error)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(any(feature = "opendal-s3", feature = "opendal-gcs"))]
    #[derive(Debug)]
    struct AlwaysSupportedCredentialProvider;

    #[cfg(any(feature = "opendal-s3", feature = "opendal-gcs"))]
    #[async_trait]
    impl StorageCredentialProvider for AlwaysSupportedCredentialProvider {
        async fn load_credential(&self, path: &str) -> Result<iceberg::io::StorageCredential> {
            let prefix = if path.contains("/table-a/") {
                "s3://bucket/table-a"
            } else {
                "s3://bucket/table-b"
            };
            Ok(
                iceberg::io::StorageCredential::new(iceberg::io::StorageCredentialKind::S3(
                    iceberg::io::S3Credential::new("access-key", "secret-key", None),
                ))
                .with_prefix(prefix),
            )
        }
    }

    #[cfg(feature = "opendal-s3")]
    #[derive(Debug)]
    struct EmptyCredentialLoader;

    #[cfg(feature = "opendal-s3")]
    impl ProvideCredential for EmptyCredentialLoader {
        type Credential = AwsCredential;

        async fn provide_credential(
            &self,
            _ctx: &reqsign_core::Context,
        ) -> reqsign_core::Result<Option<AwsCredential>> {
            Ok(None)
        }
    }

    #[cfg(feature = "opendal-s3")]
    #[test]
    fn test_s3_factory_custom_credential_loader_serialization_fails() {
        let file_io = iceberg::io::FileIOBuilder::new(Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: Some(CustomAwsCredentialLoader::new(EmptyCredentialLoader)),
        }))
        .build();

        let err = file_io.serialize_all().unwrap_err();
        assert!(
            err.to_string()
                .contains("custom AWS credential loaders cannot be serialized")
        );
    }

    #[cfg(feature = "opendal-memory")]
    #[test]
    fn test_default_memory_operator() {
        let op = default_memory_operator();
        assert_eq!(op.info().scheme().to_string(), "memory");
    }

    #[cfg(all(
        feature = "opendal-memory",
        any(feature = "opendal-s3", feature = "opendal-gcs")
    ))]
    #[test]
    fn test_factory_rejects_credentials_for_unsupported_backend() {
        let error = OpenDalStorageFactory::Memory
            .build_with_credentials(
                &StorageConfig::new(),
                Some(Arc::new(AlwaysSupportedCredentialProvider)),
            )
            .expect_err("memory must reject a credential provider");

        assert_eq!(error.kind(), ErrorKind::FeatureUnsupported);
    }

    #[cfg(feature = "opendal-memory")]
    #[test]
    fn test_relativize_path_memory() {
        let storage = OpenDalStorage::Memory(default_memory_operator());

        assert_eq!(
            storage.relativize_path("memory:/path/to/file").unwrap(),
            "path/to/file"
        );
        // Without the scheme prefix, falls back to stripping the leading slash
        assert_eq!(
            storage.relativize_path("/path/to/file").unwrap(),
            "path/to/file"
        );
    }

    #[cfg(feature = "opendal-fs")]
    #[test]
    fn test_relativize_path_fs() {
        let storage = OpenDalStorage::LocalFs;

        assert_eq!(
            storage
                .relativize_path("file:/tmp/data/file.parquet")
                .unwrap(),
            "tmp/data/file.parquet"
        );
        assert_eq!(
            storage.relativize_path("/tmp/data/file.parquet").unwrap(),
            "tmp/data/file.parquet"
        );
    }

    #[cfg(feature = "opendal-s3")]
    #[test]
    fn test_relativize_path_s3() {
        let storage = OpenDalStorage::S3 {
            config: Arc::new(S3Config::default()),
            customized_credential_load: None,
            credential_provider: None,
        };

        // All S3-family schemes are accepted by the same storage instance.
        // Custom schemes for S3-compatible stores (e.g., `minio://`) are also
        // accepted because the path's scheme is used as-is for prefix matching.
        for scheme in ["s3", "s3a", "s3n", "minio"] {
            assert_eq!(
                storage
                    .relativize_path(&format!("{scheme}://my-bucket/path/to/file.parquet"))
                    .unwrap(),
                "path/to/file.parquet"
            );
        }
    }

    #[cfg(feature = "opendal-s3")]
    #[tokio::test]
    async fn test_dynamic_credentials_batch_by_prefix() {
        let storage = OpenDalStorage::S3 {
            config: Arc::new(S3Config::default()),
            customized_credential_load: None,
            credential_provider: Some(Arc::new(AlwaysSupportedCredentialProvider)),
        };
        let first = "s3://bucket/table-a/file.parquet";
        let same_scope = "s3://bucket/table-a/other.parquet";
        let other_scope = "s3://bucket/table-b/file.parquet";

        let first_key = storage.delete_batch_key_for_path(first).await.unwrap();
        assert_eq!(
            first_key.credential_scope,
            DeleteCredentialScope::Dynamic(DynamicCredentialScope::Prefix(
                "s3://bucket/table-a".to_string()
            ))
        );
        assert_eq!(
            first_key,
            storage.delete_batch_key_for_path(same_scope).await.unwrap()
        );
        assert_ne!(
            first_key,
            storage
                .delete_batch_key_for_path(other_scope)
                .await
                .unwrap()
        );
    }

    #[cfg(feature = "opendal-s3")]
    #[tokio::test]
    async fn test_custom_s3_credential_loader_ignores_dynamic_provider_for_batching() {
        let storage = OpenDalStorage::S3 {
            config: Arc::new(S3Config::default()),
            customized_credential_load: Some(CustomAwsCredentialLoader::new(
                reqsign_aws_v4::StaticCredentialProvider::new("access-key", "secret-key"),
            )),
            credential_provider: Some(Arc::new(AlwaysSupportedCredentialProvider)),
        };

        let key = storage
            .delete_batch_key_for_path("s3://bucket/table-a/file.parquet")
            .await
            .unwrap();
        assert_eq!(key.credential_scope, DeleteCredentialScope::Static);
    }

    #[cfg(feature = "opendal-s3")]
    #[test]
    fn test_s3_rejects_anonymous_dynamic_credentials() {
        let mut config = S3Config::default();
        config.skip_signature = true;
        let storage = OpenDalStorage::S3 {
            config: Arc::new(config),
            customized_credential_load: None,
            credential_provider: Some(Arc::new(AlwaysSupportedCredentialProvider)),
        };

        let error = storage
            .create_operator(&"s3://bucket/file.parquet")
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
    }

    #[cfg(feature = "opendal-gcs")]
    #[test]
    fn test_gcs_rejects_anonymous_dynamic_credentials() {
        let mut config = GcsConfig::default();
        config.skip_signature = true;
        let storage = OpenDalStorage::Gcs {
            config: Arc::new(config),
            credential_provider: Some(Arc::new(AlwaysSupportedCredentialProvider)),
        };

        let error = storage
            .create_operator(&"gs://bucket/file.parquet")
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
    }

    #[cfg(feature = "opendal-gcs")]
    #[test]
    fn test_relativize_path_gcs() {
        let storage = OpenDalStorage::Gcs {
            config: Arc::new(GcsConfig::default()),
            credential_provider: None,
        };

        for scheme in ["gs", "gcs"] {
            let path = format!("{scheme}://my-bucket/path/to/file.parquet");
            assert_eq!(
                storage.relativize_path(&path).unwrap(),
                "path/to/file.parquet"
            );
            let (_, relative) = storage.create_operator(&path).unwrap();
            assert_eq!(relative, "path/to/file.parquet");
        }
    }

    #[cfg(feature = "opendal-gcs")]
    #[test]
    fn test_relativize_path_gcs_invalid_scheme() {
        let storage = OpenDalStorage::Gcs {
            config: Arc::new(GcsConfig::default()),
            credential_provider: None,
        };

        assert!(
            storage
                .relativize_path("s3://my-bucket/path/to/file.parquet")
                .is_err()
        );
        assert!(
            storage
                .create_operator(&"s3://my-bucket/path/to/file.parquet")
                .is_err()
        );
    }

    #[cfg(feature = "opendal-oss")]
    #[test]
    fn test_relativize_path_oss() {
        let storage = OpenDalStorage::Oss {
            config: Arc::new(OssConfig::default()),
        };

        assert_eq!(
            storage
                .relativize_path("oss://my-bucket/path/to/file.parquet")
                .unwrap(),
            "path/to/file.parquet"
        );
    }

    #[cfg(feature = "opendal-oss")]
    #[test]
    fn test_relativize_path_oss_invalid_scheme() {
        let storage = OpenDalStorage::Oss {
            config: Arc::new(OssConfig::default()),
        };

        assert!(
            storage
                .relativize_path("s3://my-bucket/path/to/file.parquet")
                .is_err()
        );
    }

    #[cfg(feature = "opendal-azdls")]
    #[test]
    fn test_relativize_path_azdls() {
        let storage = OpenDalStorage::Azdls {
            config: Arc::new(AzdlsConfig {
                account_name: Some("myaccount".to_string()),
                endpoint: Some("https://myaccount.dfs.core.windows.net".to_string()),
                ..Default::default()
            }),
        };

        assert_eq!(
            storage
                .relativize_path("abfss://myfs@myaccount.dfs.core.windows.net/path/to/file.parquet")
                .unwrap(),
            "/path/to/file.parquet"
        );
    }
}
