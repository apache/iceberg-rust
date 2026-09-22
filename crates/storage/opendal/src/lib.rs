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
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use cfg_if::cfg_if;
use futures::StreamExt;
use futures::stream::BoxStream;
use iceberg::io::{
    CLIENT_IO_TIMEOUT_MS, FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage,
    StorageConfig, StorageFactory,
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

/// Matches the `opendal::layers::TimeoutLayer` default.
const DEFAULT_IO_TIMEOUT_MS: u64 = 10_000;

/// Backend-independent client settings, shared by every [`OpenDalStorage`] variant.
///
/// Fields are private, so later settings are additive rather than breaking. The
/// container-level serde default lets an older payload deserialize as new fields appear.
#[derive(Clone, Debug, Properties, Serialize, Deserialize)]
#[serde(default)]
pub struct OpenDalClientConfig {
    /// Per-attempt deadline for one IO operation, in milliseconds.
    #[property(
        key = CLIENT_IO_TIMEOUT_MS,
        default = DEFAULT_IO_TIMEOUT_MS,
        parse_with = parse_io_timeout_ms,
        getter
    )]
    io_timeout_ms: u64,
}

impl OpenDalClientConfig {
    const DEFAULT: Self = Self {
        io_timeout_ms: DEFAULT_IO_TIMEOUT_MS,
    };
}

impl Default for OpenDalClientConfig {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Parses one timeout value; the `Properties` derive adds the property-key context.
/// Zero is rejected: it would time every operation out before it starts.
fn parse_io_timeout_ms(value: &str) -> Result<u64> {
    match value.parse::<u64>() {
        Ok(ms) if ms > 0 => Ok(ms),
        _ => Err(Error::new(
            ErrorKind::DataInvalid,
            "Expected a positive integer number of milliseconds",
        )
        .with_context("value", value)),
    }
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

#[typetag::serde(name = "OpenDalStorageFactory")]
impl StorageFactory for OpenDalStorageFactory {
    #[allow(unused_variables)]
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        let client = OpenDalClientConfig::from_properties(config.props())?;
        match self {
            #[cfg(feature = "opendal-memory")]
            OpenDalStorageFactory::Memory => Ok(Arc::new(OpenDalStorage::Memory {
                operator: memory_config_build()?,
                client,
            })),
            #[cfg(feature = "opendal-fs")]
            OpenDalStorageFactory::Fs => Ok(Arc::new(OpenDalStorage::LocalFs { client })),
            #[cfg(feature = "opendal-s3")]
            OpenDalStorageFactory::S3 {
                customized_credential_load,
            } => Ok(Arc::new(OpenDalStorage::S3 {
                config: s3_config_parse(config.props().clone())?.into(),
                customized_credential_load: customized_credential_load.clone(),
                client,
            })),
            #[cfg(feature = "opendal-gcs")]
            OpenDalStorageFactory::Gcs => Ok(Arc::new(OpenDalStorage::Gcs {
                config: gcs_config_parse(config.props().clone())?.into(),
                client,
            })),
            #[cfg(feature = "opendal-oss")]
            OpenDalStorageFactory::Oss => Ok(Arc::new(OpenDalStorage::Oss {
                config: oss_config_parse(config.props().clone())?.into(),
                client,
            })),
            #[cfg(feature = "opendal-azdls")]
            OpenDalStorageFactory::Azdls => Ok(Arc::new(OpenDalStorage::Azdls {
                config: azdls_config_parse(config.props().clone())?.into(),
                client,
            })),
            #[cfg(feature = "opendal-hf")]
            OpenDalStorageFactory::Hf => Ok(Arc::new(OpenDalStorage::Hf {
                config: hf_config_parse(config.props().clone())?.into(),
                client,
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
    Memory {
        /// Pre-built memory operator.
        #[serde(skip, default = "self::default_memory_operator")]
        operator: Operator,
        /// Backend-independent client settings.
        #[serde(default)]
        client: OpenDalClientConfig,
    },
    /// Local filesystem storage variant.
    #[cfg(feature = "opendal-fs")]
    LocalFs {
        /// Backend-independent client settings.
        #[serde(default)]
        client: OpenDalClientConfig,
    },
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
        /// Backend-independent client settings.
        #[serde(default)]
        client: OpenDalClientConfig,
    },
    /// GCS storage variant.
    #[cfg(feature = "opendal-gcs")]
    Gcs {
        /// GCS configuration.
        config: Arc<GcsConfig>,
        /// Backend-independent client settings.
        #[serde(default)]
        client: OpenDalClientConfig,
    },
    /// OSS storage variant.
    #[cfg(feature = "opendal-oss")]
    Oss {
        /// OSS configuration.
        config: Arc<OssConfig>,
        /// Backend-independent client settings.
        #[serde(default)]
        client: OpenDalClientConfig,
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
        /// Backend-independent client settings.
        #[serde(default)]
        client: OpenDalClientConfig,
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
        /// Backend-independent client settings.
        #[serde(default)]
        client: OpenDalClientConfig,
    },
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
        let path = path.as_ref();
        let (operator, relative_path): (Operator, &str) = match self {
            #[cfg(feature = "opendal-memory")]
            OpenDalStorage::Memory { operator, .. } => {
                if let Some(stripped) = path.strip_prefix("memory:/") {
                    (operator.clone(), stripped)
                } else {
                    (operator.clone(), &path[1..])
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
                ..
            } => {
                let op = s3_config_build(config, customized_credential_load, path)?;
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
            OpenDalStorage::Gcs { config, .. } => {
                let operator = gcs_config_build(config, path)?;
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
            .layer(
                TimeoutLayer::new()
                    .with_io_timeout(Duration::from_millis(self.client().io_timeout_ms())),
            )
            .layer(RetryLayer::new());
        Ok((operator, relative_path))
    }

    /// Client settings for this backend. The fallback arm is gated on every backend feature
    /// being off, when the enum has no variants, so a new variant is a compile error here.
    pub fn client(&self) -> &OpenDalClientConfig {
        match self {
            #[cfg(feature = "opendal-memory")]
            OpenDalStorage::Memory { client, .. } => client,
            #[cfg(feature = "opendal-fs")]
            OpenDalStorage::LocalFs { client } => client,
            #[cfg(feature = "opendal-s3")]
            OpenDalStorage::S3 { client, .. } => client,
            #[cfg(feature = "opendal-gcs")]
            OpenDalStorage::Gcs { client, .. } => client,
            #[cfg(feature = "opendal-oss")]
            OpenDalStorage::Oss { client, .. } => client,
            #[cfg(feature = "opendal-azdls")]
            OpenDalStorage::Azdls { client, .. } => client,
            #[cfg(feature = "opendal-hf")]
            OpenDalStorage::Hf { client, .. } => client,
            #[cfg(all(
                not(feature = "opendal-memory"),
                not(feature = "opendal-s3"),
                not(feature = "opendal-fs"),
                not(feature = "opendal-gcs"),
                not(feature = "opendal-oss"),
                not(feature = "opendal-azdls"),
                not(feature = "opendal-hf"),
            ))]
            _ => &OpenDalClientConfig::DEFAULT,
        }
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

    /// Extracts the relative path from an absolute path without building an operator.
    ///
    /// This is a lightweight alternative to [`create_operator`](Self::create_operator) for cases
    /// where only the relative path is needed (e.g. bulk deletes where the operator is already
    /// available).
    #[allow(unreachable_code, unused_variables)]
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
        let mut deleters: HashMap<String, opendal::Deleter> = HashMap::new();

        while let Some(path) = paths.next().await {
            let bucket = self.batch_key_for_path(&path);

            let (relative_path, deleter) = match deleters.entry(bucket) {
                Entry::Occupied(entry) => {
                    (self.relativize_path(&path)?.to_string(), entry.into_mut())
                }
                Entry::Vacant(entry) => {
                    let (op, rel) = self.create_operator(&path)?;
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
    use super::*;

    fn client_config(value: &str) -> Result<OpenDalClientConfig> {
        OpenDalClientConfig::from_properties(&HashMap::from([(
            CLIENT_IO_TIMEOUT_MS.to_string(),
            value.to_string(),
        )]))
    }

    #[test]
    fn test_io_timeout_parsing() {
        let unset = OpenDalClientConfig::from_properties(&HashMap::new()).unwrap();
        assert_eq!(unset.io_timeout_ms(), 10_000);
        assert_eq!(client_config("45000").unwrap().io_timeout_ms(), 45_000);

        for invalid in ["0", "-1", "12.5", "abc", ""] {
            let err = client_config(invalid).unwrap_err();
            assert!(err.to_string().contains(CLIENT_IO_TIMEOUT_MS), "{invalid}");
        }
    }

    #[cfg(feature = "opendal-memory")]
    #[test]
    fn test_factory_rejects_invalid_io_timeout() {
        let config = StorageConfig::from_props(HashMap::from([(
            CLIENT_IO_TIMEOUT_MS.to_string(),
            "nope".to_string(),
        )]));

        let err = OpenDalStorageFactory::Memory.build(&config).unwrap_err();
        assert!(err.to_string().contains(CLIENT_IO_TIMEOUT_MS));
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

    #[cfg(feature = "opendal-memory")]
    #[tokio::test]
    async fn test_writer_close_returns_stored_size() {
        use iceberg::encryption::{EncryptedOutputFile, StandardKeyMetadata};

        // Note: the memory service does report a content length, so this only pins the happy
        // path. The counter in `OpenDalWriter` is what covers services that don't, such as S3.
        let storage = Arc::new(OpenDalStorage::Memory(default_memory_operator()));
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
            client: OpenDalClientConfig::default(),
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
            client: OpenDalClientConfig::default(),
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
            client: OpenDalClientConfig::default(),
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

    #[cfg(feature = "opendal-gcs")]
    #[test]
    fn test_relativize_path_gcs() {
        let storage = OpenDalStorage::Gcs {
            config: Arc::new(GcsConfig::default()),
            client: OpenDalClientConfig::default(),
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
            client: OpenDalClientConfig::default(),
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
            client: OpenDalClientConfig::default(),
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
            client: OpenDalClientConfig::default(),
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
            client: OpenDalClientConfig::default(),
        };

        assert_eq!(
            storage
                .relativize_path("abfss://myfs@myaccount.dfs.core.windows.net/path/to/file.parquet")
                .unwrap(),
            "/path/to/file.parquet"
        );
    }
}
