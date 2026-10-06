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
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

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
use iceberg_property_macro::Properties;
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

/// Timeout in milliseconds for each IO call on a reader, writer, lister or deleter, applied per
/// retry attempt via OpenDAL's `TimeoutLayer::with_io_timeout`. Defaults to 10 seconds.
///
/// OpenDAL's separate `timeout`, which bounds whole control operations such as `stat`, is not
/// affected and stays at its 60-second default.
pub const OPENDAL_IO_TIMEOUT_MS: &str = "opendal.io-timeout-ms";

/// Matches OpenDAL's `TimeoutLayer` default.
const DEFAULT_IO_TIMEOUT_MS: NonZeroU64 = NonZeroU64::new(10_000).unwrap();

/// Backend-independent client settings shared by every [`OpenDalStorage`] variant.
#[derive(Clone, Debug, Properties, Serialize, Deserialize)]
#[serde(default)]
pub struct OpenDalClientConfig {
    /// IO timeout in milliseconds.
    #[property(
        key = OPENDAL_IO_TIMEOUT_MS,
        default = DEFAULT_IO_TIMEOUT_MS,
        parse_with = parse_io_timeout_ms
    )]
    io_timeout_ms: NonZeroU64,
}

impl Default for OpenDalClientConfig {
    fn default() -> Self {
        Self {
            io_timeout_ms: DEFAULT_IO_TIMEOUT_MS,
        }
    }
}

impl OpenDalClientConfig {
    pub(crate) fn io_timeout(&self) -> Duration {
        Duration::from_millis(self.io_timeout_ms.get())
    }
}

fn parse_io_timeout_ms(value: &str) -> Result<NonZeroU64> {
    value.parse().map_err(|error| {
        Error::new(
            ErrorKind::DataInvalid,
            "Expected a positive integer number of milliseconds",
        )
        .with_context("value", format!("{value:?}"))
        .with_source(error)
    })
}

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

/// Fails serialization of a storage holding a credential provider, which would
/// otherwise silently fall back to the configured or ambient credentials.
/// [`FileIO`](iceberg::io::FileIO) serializes its provider separately.
#[cfg(any(feature = "opendal-s3", feature = "opendal-gcs"))]
pub(crate) fn serialize_credential_provider<S>(
    _provider: &Option<Arc<dyn StorageCredentialProvider>>,
    _serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    Err(serde::ser::Error::custom(
        "storage with a credential provider cannot be serialized; serialize its FileIO instead",
    ))
}

#[typetag::serde(name = "OpenDalStorageFactory")]
impl StorageFactory for OpenDalStorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        self.build_with_credential_provider(config, None)
    }

    #[allow(unused_variables)]
    fn build_with_credential_provider(
        &self,
        config: &StorageConfig,
        credential_provider: Option<Arc<dyn StorageCredentialProvider>>,
    ) -> Result<Arc<dyn Storage>> {
        // Only S3 and GCS consume the provider; other backends use the
        // credentials in `config`.
        let client_config = OpenDalClientConfig::from_properties(config.props())?;
        match self {
            #[cfg(feature = "opendal-memory")]
            OpenDalStorageFactory::Memory => Ok(Arc::new(OpenDalStorage::Memory {
                operator: memory_config_build()?,
                client_config,
            })),
            #[cfg(feature = "opendal-fs")]
            OpenDalStorageFactory::Fs => Ok(Arc::new(OpenDalStorage::LocalFs { client_config })),
            #[cfg(feature = "opendal-s3")]
            OpenDalStorageFactory::S3 {
                customized_credential_load,
            } => Ok(Arc::new(OpenDalStorage::S3 {
                config: s3_config_parse(config.props().clone())?.into(),
                customized_credential_load: customized_credential_load.clone(),
                credential_provider,
                client_config,
            })),
            #[cfg(feature = "opendal-gcs")]
            OpenDalStorageFactory::Gcs => Ok(Arc::new(OpenDalStorage::Gcs {
                config: gcs_config_parse(config.props().clone())?.into(),
                credential_provider,
                client_config,
            })),
            #[cfg(feature = "opendal-oss")]
            OpenDalStorageFactory::Oss => Ok(Arc::new(OpenDalStorage::Oss {
                config: oss_config_parse(config.props().clone())?.into(),
                client_config,
            })),
            #[cfg(feature = "opendal-azdls")]
            OpenDalStorageFactory::Azdls => Ok(Arc::new(OpenDalStorage::Azdls {
                config: azdls_config_parse(config.props().clone())?.into(),
                client_config,
            })),
            #[cfg(feature = "opendal-hf")]
            OpenDalStorageFactory::Hf => Ok(Arc::new(OpenDalStorage::Hf {
                config: hf_config_parse(config.props().clone())?.into(),
                client_config,
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
///
/// The serialized representation is not a stable format and may change between crate versions.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub enum OpenDalStorage {
    /// Memory storage variant.
    #[cfg(feature = "opendal-memory")]
    Memory {
        /// Pre-built memory operator.
        #[serde(skip, default = "self::default_memory_operator")]
        operator: Operator,
        /// Backend-independent client settings.
        #[serde(default)]
        client_config: OpenDalClientConfig,
    },
    /// Local filesystem storage variant.
    #[cfg(feature = "opendal-fs")]
    LocalFs {
        /// Backend-independent client settings.
        #[serde(default)]
        client_config: OpenDalClientConfig,
    },
    /// S3 storage variant.
    ///
    /// Accepts any S3-family URL (`s3://`, `s3a://`, `s3n://`); the scheme is
    /// derived from the path at call time.
    #[non_exhaustive]
    #[cfg(feature = "opendal-s3")]
    S3 {
        /// S3 configuration.
        config: Arc<S3Config>,
        /// Custom AWS credential loader.
        #[serde(skip)]
        customized_credential_load: Option<CustomAwsCredentialLoader>,
        /// Provider of refreshable vended credentials, supplied by the catalog.
        #[serde(
            skip_deserializing,
            skip_serializing_if = "Option::is_none",
            serialize_with = "serialize_credential_provider"
        )]
        credential_provider: Option<Arc<dyn StorageCredentialProvider>>,
        /// Backend-independent client settings.
        #[serde(default)]
        client_config: OpenDalClientConfig,
    },
    /// GCS storage variant.
    #[non_exhaustive]
    #[cfg(feature = "opendal-gcs")]
    Gcs {
        /// GCS configuration.
        config: Arc<GcsConfig>,
        /// Provider of refreshable vended credentials, supplied by the catalog.
        #[serde(
            skip_deserializing,
            skip_serializing_if = "Option::is_none",
            serialize_with = "serialize_credential_provider"
        )]
        credential_provider: Option<Arc<dyn StorageCredentialProvider>>,
        /// Backend-independent client settings.
        #[serde(default)]
        client_config: OpenDalClientConfig,
    },
    /// OSS storage variant.
    #[non_exhaustive]
    #[cfg(feature = "opendal-oss")]
    Oss {
        /// OSS configuration.
        config: Arc<OssConfig>,
        /// Backend-independent client settings.
        #[serde(default)]
        client_config: OpenDalClientConfig,
    },
    /// Azure Data Lake Storage variant.
    ///
    /// Accepts paths of the form
    /// `abfs[s]://<filesystem>@<account>.dfs.<endpoint-suffix>/<path>` or
    /// `wasb[s]://<container>@<account>.blob.<endpoint-suffix>/<path>`.
    /// The scheme is derived from the path at call time.
    #[non_exhaustive]
    #[cfg(feature = "opendal-azdls")]
    Azdls {
        /// Azure DLS configuration.
        config: Arc<AzdlsConfig>,
        /// Backend-independent client settings.
        #[serde(default)]
        client_config: OpenDalClientConfig,
    },
    /// HuggingFace Hub storage variant.
    ///
    /// Accepts paths of the form
    /// `hf://<repo_type>/<owner>/<repo>[@<revision>]/<path_in_repo>`,
    /// where `<repo_type>` must be one of `models`, `datasets`, `spaces`, or `buckets`.
    #[non_exhaustive]
    #[cfg(feature = "opendal-hf")]
    Hf {
        /// HuggingFace Hub configuration (token + endpoint).
        config: Arc<HfConfig>,
        /// Backend-independent client settings.
        #[serde(default)]
        client_config: OpenDalClientConfig,
    },
}

/// Groups bulk deletes by operator and credential scope.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct DeleteBatchKey {
    storage: String,
    credential: BatchCredential,
}

impl DeleteBatchKey {
    /// Open batches to flush before adding a path with this key.
    ///
    /// A scoped credential inside the scope of an open batch means the catalog
    /// is replacing that batch's broader credential, such as a scheme-wide or
    /// account-wide seed. Such batches are flushed while their credential is
    /// still valid: once it expires, nothing may cover their whole scope.
    fn superseded_batches<'a>(
        &self,
        open: impl Iterator<Item = &'a DeleteBatchKey>,
    ) -> Vec<DeleteBatchKey> {
        let BatchCredential::Scoped { prefix } = &self.credential else {
            return Vec::new();
        };
        open.filter(|key| {
            key.storage == self.storage
                && key
                    .credential
                    .location()
                    .is_some_and(|location| location != prefix && prefix.starts_with(location))
        })
        .cloned()
        .collect()
    }
}

/// The credential shared by the paths of a bulk-delete batch.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum BatchCredential {
    /// Served by the backend's configured credentials.
    Static,
    /// A dynamic credential for a whole scheme, such as `s3`, looked up for
    /// the storage root.
    SchemeWide { root: String },
    /// A dynamic credential scoped to `prefix`, looked up for the prefix.
    Scoped { prefix: String },
}

impl BatchCredential {
    /// Location the batch operator looks dynamic credentials up for.
    fn location(&self) -> Option<&str> {
        match self {
            BatchCredential::Static => None,
            BatchCredential::SchemeWide { root } => Some(root),
            BatchCredential::Scoped { prefix } => Some(prefix),
        }
    }
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
    pub(crate) fn create_operator<'a>(
        &self,
        path: &'a impl AsRef<str>,
    ) -> Result<(Operator, &'a str)> {
        self.create_operator_with_credential_location(path, None)
    }

    /// Creates an operator whose dynamic credentials are looked up for
    /// `credential_location` instead of `path`.
    ///
    /// A bulk-delete batch passes its scope location, so every credential the
    /// operator loads covers all paths in the batch.
    #[allow(unreachable_code, unreachable_patterns, unused_variables)]
    fn create_operator_with_credential_location<'a>(
        &self,
        path: &'a impl AsRef<str>,
        credential_location: Option<&str>,
    ) -> Result<(Operator, &'a str)> {
        let path = path.as_ref();
        let (operator, relative_path): (Operator, &str) = match self {
            #[cfg(feature = "opendal-memory")]
            OpenDalStorage::Memory { operator: op, .. } => {
                if let Some(stripped) = path.strip_prefix("memory:/") {
                    (op.clone(), stripped)
                } else {
                    (op.clone(), &path[1..])
                }
            }
            #[cfg(feature = "opendal-fs")]
            OpenDalStorage::LocalFs { .. } => {
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
                ..
            } => {
                let op = s3_config_build(
                    config,
                    customized_credential_load,
                    credential_provider,
                    path,
                    credential_location,
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
                ..
            } => {
                let operator =
                    gcs_config_build(config, credential_provider, path, credential_location)?;
                let prefix = format!("gs://{}/", operator.info().name());
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
            OpenDalStorage::Oss { config, .. } => {
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
            OpenDalStorage::Azdls { config, .. } => azdls_create_operator(path, config)?,
            #[cfg(feature = "opendal-hf")]
            OpenDalStorage::Hf { config, .. } => hf_config_build(config, path)?,
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
        let operator = operator
            .layer(TimeoutLayer::new().with_io_timeout(self.client_config().io_timeout()))
            .layer(RetryLayer::new());
        Ok((operator, relative_path))
    }

    pub(crate) fn client_config(&self) -> &OpenDalClientConfig {
        match self {
            #[cfg(feature = "opendal-memory")]
            OpenDalStorage::Memory { client_config, .. } => client_config,
            #[cfg(feature = "opendal-fs")]
            OpenDalStorage::LocalFs { client_config } => client_config,
            #[cfg(feature = "opendal-s3")]
            OpenDalStorage::S3 { client_config, .. } => client_config,
            #[cfg(feature = "opendal-gcs")]
            OpenDalStorage::Gcs { client_config, .. } => client_config,
            #[cfg(feature = "opendal-oss")]
            OpenDalStorage::Oss { client_config, .. } => client_config,
            #[cfg(feature = "opendal-azdls")]
            OpenDalStorage::Azdls { client_config, .. } => client_config,
            #[cfg(feature = "opendal-hf")]
            OpenDalStorage::Hf { client_config, .. } => client_config,
            #[cfg(all(
                not(feature = "opendal-memory"),
                not(feature = "opendal-s3"),
                not(feature = "opendal-fs"),
                not(feature = "opendal-gcs"),
                not(feature = "opendal-oss"),
                not(feature = "opendal-azdls"),
                not(feature = "opendal-hf"),
            ))]
            _ => unreachable!(),
        }
    }

    /// Returns a cache key used by `delete_stream` to group paths by storage operator.
    ///
    /// For most backends the URL host (bucket name) is sufficient. For HF the host
    /// encodes the repo type, not the repo identity, so a more specific key is used.
    #[allow(unreachable_patterns)]
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
        let credential = match self.credential_provider_for_path(path) {
            Some(provider) => {
                let credential = provider.load_credential(path).await?;
                if !credential.covers(path) {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        utils::uncovered_location_message(path, &credential),
                    ));
                }
                let prefix = credential.prefix();
                if prefix.contains("://") {
                    BatchCredential::Scoped {
                        prefix: prefix.to_string(),
                    }
                } else {
                    // A scheme-wide prefix such as `s3` is no location to look
                    // up, so the batch looks its credential up for the root.
                    BatchCredential::SchemeWide {
                        root: utils::storage_root(path)?,
                    }
                }
            }
            None => BatchCredential::Static,
        };

        Ok(DeleteBatchKey {
            storage: self.batch_key_for_path(path),
            credential,
        })
    }

    /// Extracts the relative path from an absolute path without building an operator.
    ///
    /// This is a lightweight alternative to [`create_operator`](Self::create_operator) for cases
    /// where only the relative path is needed (e.g. bulk deletes where the operator is already
    /// available).
    #[allow(unreachable_code, unreachable_patterns, unused_variables)]
    pub(crate) fn relativize_path<'a>(&self, path: &'a str) -> Result<&'a str> {
        match self {
            #[cfg(feature = "opendal-memory")]
            OpenDalStorage::Memory { .. } => {
                Ok(path.strip_prefix("memory:/").unwrap_or(&path[1..]))
            }
            #[cfg(feature = "opendal-fs")]
            OpenDalStorage::LocalFs { .. } => Ok(path.strip_prefix("file:/").unwrap_or(&path[1..])),
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
                let bucket = url.host_str().ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid gcs url: {path}, missing bucket"),
                    )
                })?;
                let prefix = format!("gs://{}/", bucket);
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
            OpenDalStorage::Azdls { config, .. } => {
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
        Ok(Box::new(OpenDalWriter::new(
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

            for key in batch_key.superseded_batches(deleters.keys()) {
                if let Some(mut deleter) = deleters.remove(&key) {
                    deleter.close().await.map_err(from_opendal_error)?;
                }
            }

            let (relative_path, deleter) = match deleters.entry(batch_key) {
                Entry::Occupied(entry) => {
                    (self.relativize_path(&path)?.to_string(), entry.into_mut())
                }
                Entry::Vacant(entry) => {
                    let (op, rel) = self.create_operator_with_credential_location(
                        &path,
                        entry.key().credential.location(),
                    )?;
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
pub(crate) struct OpenDalWriter {
    inner: opendal::Writer,
    bytes_written: u64,
}

impl OpenDalWriter {
    pub(crate) fn new(inner: opendal::Writer) -> Self {
        Self {
            inner,
            bytes_written: 0,
        }
    }
}

#[async_trait]
impl FileWrite for OpenDalWriter {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        let len = bs.len() as u64;
        opendal::Writer::write(&mut self.inner, bs)
            .await
            .map_err(from_opendal_error)?;
        self.bytes_written += len;
        Ok(())
    }

    async fn close(&mut self) -> Result<FileMetadata> {
        let metadata = opendal::Writer::close(&mut self.inner)
            .await
            .map_err(from_opendal_error)?;

        // Object stores may omit the size (reported as 0); validate only a reported nonzero size.
        let reported_size = metadata.content_length();
        if reported_size != 0 && reported_size != self.bytes_written {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!(
                    "Wrote {} bytes but storage reports {reported_size}",
                    self.bytes_written
                ),
            ));
        }

        Ok(FileMetadata {
            size: self.bytes_written,
        })
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "opendal-s3")]
    use std::sync::atomic::{AtomicBool, Ordering};

    #[cfg(feature = "opendal-gcs")]
    use iceberg::io::{GCS_SERVICE_HOST, GCS_TOKEN};
    #[cfg(any(feature = "opendal-s3", feature = "opendal-gcs"))]
    use iceberg::io::{S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, StorageCredential};

    #[allow(unused_imports)]
    use super::*;

    #[cfg(any(feature = "opendal-s3", feature = "opendal-gcs"))]
    #[derive(Debug)]
    struct AlwaysSupportedCredentialProvider;

    #[cfg(any(feature = "opendal-s3", feature = "opendal-gcs"))]
    #[async_trait]
    impl StorageCredentialProvider for AlwaysSupportedCredentialProvider {
        fn supports_path(&self, _path: &str) -> bool {
            true
        }

        async fn load_credential(&self, path: &str) -> Result<StorageCredential> {
            let prefix = if path.contains("/table-a/") {
                "s3://bucket/table-a"
            } else {
                "s3://bucket/table-b"
            };
            Ok(s3_credential(prefix, "access-key"))
        }
    }

    /// An S3 credential for locations under `prefix`.
    #[cfg(any(feature = "opendal-s3", feature = "opendal-gcs"))]
    fn s3_credential(prefix: impl Into<String>, access_key: &str) -> StorageCredential {
        StorageCredential::new(
            prefix,
            HashMap::from([
                (S3_ACCESS_KEY_ID.to_string(), access_key.to_string()),
                (S3_SECRET_ACCESS_KEY.to_string(), "secret-key".to_string()),
            ]),
        )
    }

    fn client_config(value: &str) -> Result<OpenDalClientConfig> {
        OpenDalClientConfig::from_properties(&HashMap::from([(
            OPENDAL_IO_TIMEOUT_MS.to_string(),
            value.to_string(),
        )]))
    }

    #[test]
    fn test_io_timeout_parsing() {
        let unset = OpenDalClientConfig::from_properties(&HashMap::new()).unwrap();
        assert_eq!(
            unset.io_timeout(),
            Duration::from_millis(DEFAULT_IO_TIMEOUT_MS.get())
        );

        let max = u64::MAX.to_string();
        for (valid, ms) in [("45000", 45_000), ("1", 1), (max.as_str(), u64::MAX)] {
            assert_eq!(
                client_config(valid).unwrap().io_timeout(),
                Duration::from_millis(ms),
                "{valid}"
            );
        }

        for invalid in ["0", "-1", "12.5", "abc", "", "18446744073709551616"] {
            let err = client_config(invalid).unwrap_err().to_string();
            assert!(err.contains(OPENDAL_IO_TIMEOUT_MS), "{invalid}");
            assert!(err.contains(&format!("value: {invalid:?}")), "{err}");
            let reason = invalid.parse::<NonZeroU64>().unwrap_err().to_string();
            assert!(err.contains(&reason), "{err}");
        }
    }

    #[test]
    fn test_default_timeouts_match_opendal() {
        // `TimeoutLayer` has no getters, so compare through `Debug`.
        let opendal_default = format!("{:?}", TimeoutLayer::new());
        let layer = |timeout, io_timeout| {
            format!(
                "{:?}",
                TimeoutLayer::new()
                    .with_timeout(timeout)
                    .with_io_timeout(io_timeout)
            )
        };
        let control = Duration::from_secs(60);
        let io = Duration::from_millis(DEFAULT_IO_TIMEOUT_MS.get());
        assert_eq!(opendal_default, layer(control, io));
        assert_ne!(opendal_default, layer(control + Duration::from_secs(1), io));
        assert_ne!(
            opendal_default,
            layer(control, io + Duration::from_millis(1))
        );
    }

    #[cfg(feature = "opendal-s3")]
    #[test]
    fn test_client_config_serde_round_trip() {
        let storage = OpenDalStorage::S3 {
            config: Arc::new(S3Config::default()),
            customized_credential_load: None,
            credential_provider: None,
            client_config: client_config("45000").unwrap(),
        };

        let mut value = serde_json::to_value(&storage).unwrap();
        let restored: OpenDalStorage = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(
            restored.client_config().io_timeout(),
            Duration::from_secs(45)
        );

        let mut zero = value.clone();
        zero["S3"]["client_config"]["io_timeout_ms"] = 0.into();
        assert!(serde_json::from_value::<OpenDalStorage>(zero).is_err());

        value["S3"].as_object_mut().unwrap().remove("client_config");
        let restored: OpenDalStorage = serde_json::from_value(value).unwrap();
        assert_eq!(
            restored.client_config().io_timeout(),
            Duration::from_millis(DEFAULT_IO_TIMEOUT_MS.get())
        );
    }

    #[cfg(all(feature = "opendal-fs", feature = "opendal-memory"))]
    #[test]
    fn test_old_unit_variant_forms_are_rejected() {
        for old in [r#""LocalFs""#, r#""Memory""#, r#"{"Memory":null}"#] {
            assert!(
                serde_json::from_str::<OpenDalStorage>(old).is_err(),
                "{old}"
            );
        }
    }

    #[cfg(feature = "opendal-memory")]
    #[test]
    fn test_factory_rejects_invalid_io_timeout() {
        let config = StorageConfig::new().with_prop(OPENDAL_IO_TIMEOUT_MS, "nope");

        let err = OpenDalStorageFactory::Memory.build(&config).unwrap_err();
        assert!(err.to_string().contains(OPENDAL_IO_TIMEOUT_MS));
    }

    #[test]
    fn test_factory_propagates_io_timeout() {
        let config = StorageConfig::new().with_prop(OPENDAL_IO_TIMEOUT_MS, "45000");
        let factories = [
            #[cfg(feature = "opendal-memory")]
            OpenDalStorageFactory::Memory,
            #[cfg(feature = "opendal-fs")]
            OpenDalStorageFactory::Fs,
            #[cfg(feature = "opendal-s3")]
            OpenDalStorageFactory::S3 {
                customized_credential_load: None,
            },
            #[cfg(feature = "opendal-gcs")]
            OpenDalStorageFactory::Gcs,
            #[cfg(feature = "opendal-oss")]
            OpenDalStorageFactory::Oss,
            #[cfg(feature = "opendal-azdls")]
            OpenDalStorageFactory::Azdls,
            #[cfg(feature = "opendal-hf")]
            OpenDalStorageFactory::Hf,
        ];
        for factory in factories {
            // `build` returns `dyn Storage`, so read the config back from its serialized form.
            let storage = factory.build(&config).unwrap();
            let json = serde_json::to_value(&*storage).unwrap();
            let client_config = json
                .as_object()
                .unwrap()
                .values()
                .find_map(|variant| variant.get("client_config"))
                .unwrap_or_else(|| panic!("{json}"));
            assert_eq!(client_config["io_timeout_ms"], 45_000, "{factory:?}");
        }
    }

    #[cfg(feature = "opendal-memory")]
    #[tokio::test(start_paused = true)]
    async fn test_io_timeout_reaches_timeout_layer() {
        use opendal::layers::ConcurrentLimitLayer;

        // A zero concurrency limit stalls every IO call; paused time skips the waits.
        let storage = OpenDalStorage::Memory {
            operator: default_memory_operator().layer(ConcurrentLimitLayer::new(0)),
            client_config: client_config("45000").unwrap(),
        };

        let err = storage
            .read("memory:/stalled")
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("io operation timeout reached"), "{err}");
        assert!(err.contains("{ timeout: 45 }"), "{err}");
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
    fn test_factory_ignores_credentials_for_unsupported_backend() {
        let storage = OpenDalStorageFactory::Memory
            .build_with_credential_provider(
                &StorageConfig::new(),
                Some(Arc::new(AlwaysSupportedCredentialProvider)),
            )
            .expect("memory must ignore a credential provider");

        assert!(storage.new_input("memory:/key").is_ok());
    }

    #[cfg(feature = "opendal-memory")]
    #[tokio::test]
    async fn test_writer_close_returns_stored_size() {
        use iceberg::encryption::{EncryptedOutputFile, StandardKeyMetadata};

        // Note: the memory service does report a content length, so this only pins the happy
        // path. The counter in `OpenDalWriter` is what covers services that don't, such as S3.
        let storage = Arc::new(OpenDalStorage::Memory {
            operator: default_memory_operator(),
            client_config: OpenDalClientConfig::default(),
        });
        let path = "memory:///stored-size";
        for plaintext in [
            Bytes::new(),
            Bytes::from_static(b"test data"),
            Bytes::from(vec![7; 3 * 1024]),
        ] {
            let mut writer = storage.writer(path).await.unwrap();
            for chunk in plaintext.chunks(1024) {
                writer.write(Bytes::copy_from_slice(chunk)).await.unwrap();
            }
            let metadata = writer.close().await.unwrap();
            assert_eq!(metadata.size, plaintext.len() as u64);
            assert_eq!(metadata.size, storage.metadata(path).await.unwrap().size);

            let output = EncryptedOutputFile::new(
                OutputFile::new(storage.clone(), path.to_string()),
                StandardKeyMetadata::try_new(b"0123456789abcdef").unwrap(),
            );
            let metadata = output.write(plaintext.clone()).await.unwrap();
            assert!(metadata.size > plaintext.len() as u64);
            assert_eq!(metadata.size, storage.metadata(path).await.unwrap().size);
        }
    }

    #[cfg(feature = "opendal-memory")]
    #[test]
    fn test_relativize_path_memory() {
        let storage = OpenDalStorage::Memory {
            operator: default_memory_operator(),
            client_config: OpenDalClientConfig::default(),
        };

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
        let storage = OpenDalStorage::LocalFs {
            client_config: OpenDalClientConfig::default(),
        };

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
            client_config: OpenDalClientConfig::default(),
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

    /// Vends a credential for the whole `s3` scheme until it has served
    /// `table-c`, then credentials scoped to each table. The scheme-wide
    /// credential expires once the `table-c` batch is signed.
    #[cfg(feature = "opendal-s3")]
    #[derive(Debug, Default)]
    struct ChangingScopeProvider {
        scoped: AtomicBool,
        scheme_wide_expired: AtomicBool,
    }

    #[cfg(feature = "opendal-s3")]
    #[async_trait]
    impl StorageCredentialProvider for ChangingScopeProvider {
        fn supports_path(&self, _path: &str) -> bool {
            true
        }

        async fn load_credential(&self, path: &str) -> Result<StorageCredential> {
            if path.starts_with("s3://bucket/table-c") {
                self.scoped.store(true, Ordering::SeqCst);
            }
            // Signing the scoped batch means the scheme-wide credential is gone.
            if path == "s3://bucket/table-c" {
                self.scheme_wide_expired.store(true, Ordering::SeqCst);
            }
            match path.strip_prefix("s3://bucket/") {
                Some(rest) if self.scoped.load(Ordering::SeqCst) && !rest.is_empty() => {
                    let table = rest.split('/').next().unwrap_or_default();
                    Ok(s3_credential(format!("s3://bucket/{table}"), "SCOPED_AK"))
                }
                _ if self.scheme_wide_expired.load(Ordering::SeqCst) => Err(Error::new(
                    ErrorKind::Unexpected,
                    "the scheme-wide credential expired",
                )),
                _ => Ok(s3_credential("s3", "SCHEME_AK")),
            }
        }
    }

    #[cfg(feature = "opendal-s3")]
    #[tokio::test]
    async fn test_delete_stream_signs_each_batch_with_its_vended_credential() {
        let mut server = mockito::Server::new_async().await;
        let mut delete = |path: &str, access_key: &str| {
            server
                .mock("DELETE", path)
                .match_header(
                    "authorization",
                    mockito::Matcher::Regex(format!("Credential={access_key}/")),
                )
                .expect(1)
                .with_status(204)
        };
        // The scheme-wide batch is flushed, with its own credential, before
        // the scoped credential that replaces it is used.
        let scheme_wide = delete("/bucket/table-b/f.parquet", "SCHEME_AK")
            .create_async()
            .await;
        let scoped = delete("/bucket/table-c/f.parquet", "SCOPED_AK")
            .create_async()
            .await;

        let mut config = S3Config::default();
        config.endpoint = Some(server.url());
        config.region = Some("us-east-1".to_string());
        config.disable_config_load = true;
        config.disable_ec2_metadata = true;
        let storage = OpenDalStorage::S3 {
            config: Arc::new(config),
            customized_credential_load: None,
            credential_provider: Some(Arc::new(ChangingScopeProvider::default())),
            client_config: OpenDalClientConfig::default(),
        };
        storage
            .delete_stream(
                futures::stream::iter([
                    "s3://bucket/table-b/f.parquet".to_string(),
                    "s3://bucket/table-c/f.parquet".to_string(),
                ])
                .boxed(),
            )
            .await
            .unwrap();

        scheme_wide.assert_async().await;
        scoped.assert_async().await;
    }

    /// Vends `token` as a GCS credential, or fails when it is `None`.
    #[cfg(feature = "opendal-gcs")]
    #[derive(Debug)]
    struct GcsTokenProvider(Option<&'static str>);

    #[cfg(feature = "opendal-gcs")]
    #[async_trait]
    impl StorageCredentialProvider for GcsTokenProvider {
        fn supports_path(&self, _path: &str) -> bool {
            true
        }

        async fn load_credential(&self, _path: &str) -> Result<StorageCredential> {
            let token = self
                .0
                .ok_or_else(|| Error::new(ErrorKind::Unexpected, "refresh failed"))?;
            Ok(StorageCredential::new(
                "gs",
                HashMap::from([(GCS_TOKEN.to_string(), token.to_string())]),
            ))
        }
    }

    #[cfg(feature = "opendal-gcs")]
    fn gcs_storage(server: &mockito::Server, provider: GcsTokenProvider) -> OpenDalStorage {
        let config = gcs_config_parse(HashMap::from([
            (GCS_SERVICE_HOST.to_string(), server.url()),
            (GCS_TOKEN.to_string(), "static-token".to_string()),
        ]))
        .unwrap();
        OpenDalStorage::Gcs {
            config: Arc::new(config),
            credential_provider: Some(Arc::new(provider)),
            client_config: OpenDalClientConfig::default(),
        }
    }

    #[cfg(feature = "opendal-gcs")]
    #[tokio::test]
    async fn test_gcs_signs_with_the_vended_token() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", mockito::Matcher::Any)
            .match_header("authorization", "Bearer vended-token")
            .expect_at_least(1)
            .with_status(404)
            .create_async()
            .await;

        let storage = gcs_storage(&server, GcsTokenProvider(Some("vended-token")));
        assert!(!storage.exists("gs://bucket/file.parquet").await.unwrap());
        mock.assert_async().await;
    }

    #[cfg(feature = "opendal-gcs")]
    #[tokio::test]
    async fn test_gcs_does_not_fall_back_when_the_provider_fails() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", mockito::Matcher::Any)
            .expect(0)
            .create_async()
            .await;

        let storage = gcs_storage(&server, GcsTokenProvider(None));
        assert!(storage.exists("gs://bucket/file.parquet").await.is_err());
        mock.assert_async().await;
    }

    #[cfg(feature = "opendal-s3")]
    #[test]
    fn test_storage_with_credential_provider_is_not_serializable() {
        let storage =
            |credential_provider: Option<Arc<dyn StorageCredentialProvider>>| OpenDalStorage::S3 {
                config: Arc::new(S3Config::default()),
                customized_credential_load: None,
                credential_provider,
                client_config: OpenDalClientConfig::default(),
            };

        assert!(serde_json::to_string(&storage(None)).is_ok());
        let error =
            serde_json::to_string(&storage(Some(Arc::new(AlwaysSupportedCredentialProvider))))
                .unwrap_err();
        assert!(error.to_string().contains("credential provider"), "{error}");
    }

    #[cfg(feature = "opendal-s3")]
    #[tokio::test]
    async fn test_dynamic_credentials_batch_by_prefix() {
        let storage = OpenDalStorage::S3 {
            config: Arc::new(S3Config::default()),
            customized_credential_load: None,
            credential_provider: Some(Arc::new(AlwaysSupportedCredentialProvider)),
            client_config: OpenDalClientConfig::default(),
        };
        let first = "s3://bucket/table-a/file.parquet";
        let same_scope = "s3://bucket/table-a/other.parquet";
        let other_scope = "s3://bucket/table-b/file.parquet";

        let first_key = storage.delete_batch_key_for_path(first).await.unwrap();
        assert_eq!(first_key.credential.location(), Some("s3://bucket/table-a"));
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
    async fn test_scheme_wide_dynamic_credentials_batch_by_storage_root() {
        #[derive(Debug)]
        struct SchemeWideProvider;

        #[async_trait]
        impl StorageCredentialProvider for SchemeWideProvider {
            fn supports_path(&self, _path: &str) -> bool {
                true
            }

            async fn load_credential(&self, _path: &str) -> Result<StorageCredential> {
                Ok(s3_credential("s3", "access-key"))
            }
        }

        let storage = OpenDalStorage::S3 {
            config: Arc::new(S3Config::default()),
            customized_credential_load: None,
            credential_provider: Some(Arc::new(SchemeWideProvider)),
            client_config: OpenDalClientConfig::default(),
        };

        let key = storage
            .delete_batch_key_for_path("s3://bucket/table-a/file.parquet")
            .await
            .unwrap();
        assert_eq!(key.credential, BatchCredential::SchemeWide {
            root: "s3://bucket/".to_string()
        });
        assert_eq!(
            key,
            storage
                .delete_batch_key_for_path("s3://bucket/table-b/file.parquet")
                .await
                .unwrap()
        );
    }

    #[test]
    fn test_scoped_batch_flushes_broader_batches_of_same_storage() {
        let key = |storage: &str, credential: BatchCredential| DeleteBatchKey {
            storage: storage.to_string(),
            credential,
        };
        let unscoped = key("bucket", BatchCredential::SchemeWide {
            root: "s3://bucket/".to_string(),
        });
        let other_storage = key("other", BatchCredential::SchemeWide {
            root: "s3://other/".to_string(),
        });
        let scoped = key("bucket", BatchCredential::Scoped {
            prefix: "s3://bucket/table-a".to_string(),
        });
        let open = [unscoped.clone(), other_storage, scoped.clone()];

        let new_scope = key("bucket", BatchCredential::Scoped {
            prefix: "s3://bucket/table-b".to_string(),
        });
        assert_eq!(new_scope.superseded_batches(open.iter()), vec![
            unscoped.clone()
        ]);

        // A narrower scope also supersedes a broader scoped batch.
        let nested = key("bucket", BatchCredential::Scoped {
            prefix: "s3://bucket/table-a/data".to_string(),
        });
        let superseded = nested.superseded_batches(open.iter());
        assert_eq!(superseded.len(), 2);
        assert!(superseded.contains(&unscoped) && superseded.contains(&scoped));
        // A batch never supersedes itself.
        assert_eq!(scoped.superseded_batches(open.iter()), vec![
            unscoped.clone()
        ]);
        // Scheme-wide and static paths leave open batches alone.
        assert!(unscoped.superseded_batches(open.iter()).is_empty());
        assert!(
            key("bucket", BatchCredential::Static)
                .superseded_batches(open.iter())
                .is_empty()
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
            client_config: OpenDalClientConfig::default(),
        };

        let key = storage
            .delete_batch_key_for_path("s3://bucket/table-a/file.parquet")
            .await
            .unwrap();
        assert_eq!(key.credential, BatchCredential::Static);
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
            client_config: OpenDalClientConfig::default(),
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
            client_config: OpenDalClientConfig::default(),
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
            client_config: OpenDalClientConfig::default(),
        };

        assert_eq!(
            storage
                .relativize_path("gs://my-bucket/path/to/file.parquet")
                .unwrap(),
            "path/to/file.parquet"
        );
    }

    #[cfg(feature = "opendal-gcs")]
    #[test]
    fn test_relativize_path_gcs_invalid_scheme() {
        let storage = OpenDalStorage::Gcs {
            config: Arc::new(GcsConfig::default()),
            credential_provider: None,
            client_config: OpenDalClientConfig::default(),
        };

        assert!(
            storage
                .relativize_path("s3://my-bucket/path/to/file.parquet")
                .is_err()
        );
    }

    #[cfg(feature = "opendal-oss")]
    #[test]
    fn test_relativize_path_oss() {
        let storage = OpenDalStorage::Oss {
            config: Arc::new(OssConfig::default()),
            client_config: OpenDalClientConfig::default(),
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
            client_config: OpenDalClientConfig::default(),
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
            client_config: OpenDalClientConfig::default(),
        };

        assert_eq!(
            storage
                .relativize_path("abfss://myfs@myaccount.dfs.core.windows.net/path/to/file.parquet")
                .unwrap(),
            "/path/to/file.parquet"
        );
    }
}
