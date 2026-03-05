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
//! which implement the [`Storage`](iceberg::io::Storage) and
//! [`StorageFactory`](iceberg::io::StorageFactory) traits from the `iceberg` crate
//! using [OpenDAL](https://opendal.apache.org/) as the backend.

mod utils;

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use cfg_if::cfg_if;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};
use opendal::Operator;
use opendal::layers::RetryLayer;
use serde::{Deserialize, Serialize};
use utils::from_opendal_error;

cfg_if! {
    if #[cfg(feature = "opendal-azdls")] {
        mod azdls;
        use azdls::AzureStorageScheme;
        use azdls::*;
        use opendal::services::AzdlsConfig;
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

/// OpenDAL-based storage factory.
///
/// Maps scheme to the corresponding OpenDalStorage storage variant.
/// Use this factory with `FileIOBuilder::new(factory)` to create FileIO instances.
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
        /// s3 storage could have `s3://` and `s3a://`.
        /// Storing the scheme string here to return the correct path.
        configured_scheme: String,
        /// Custom AWS credential loader.
        #[serde(skip)]
        customized_credential_load: Option<s3::CustomAwsCredentialLoader>,
    },
    /// GCS storage factory.
    #[cfg(feature = "opendal-gcs")]
    Gcs,
    /// OSS storage factory.
    #[cfg(feature = "opendal-oss")]
    Oss,
    /// Azure Data Lake Storage factory.
    #[cfg(feature = "opendal-azdls")]
    Azdls {
        /// The configured Azure storage scheme.
        configured_scheme: AzureStorageScheme,
    },
}

#[typetag::serde(name = "OpenDalStorageFactory")]
impl StorageFactory for OpenDalStorageFactory {
    #[allow(unused_variables)]
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        match self {
            #[cfg(feature = "opendal-memory")]
            OpenDalStorageFactory::Memory => {
                Ok(Arc::new(OpenDalStorage::Memory(memory_config_build()?)))
            }
            #[cfg(feature = "opendal-fs")]
            OpenDalStorageFactory::Fs => Ok(Arc::new(OpenDalStorage::LocalFs)),
            #[cfg(feature = "opendal-s3")]
            OpenDalStorageFactory::S3 {
                configured_scheme,
                customized_credential_load,
            } => Ok(Arc::new(OpenDalStorage::S3 {
                configured_scheme: configured_scheme.clone(),
                config: s3_config_parse(config.props().clone())?.into(),
                customized_credential_load: customized_credential_load.clone(),
            })),
            #[cfg(feature = "opendal-gcs")]
            OpenDalStorageFactory::Gcs => Ok(Arc::new(OpenDalStorage::Gcs {
                config: gcs_config_parse(config.props().clone())?.into(),
            })),
            #[cfg(feature = "opendal-oss")]
            OpenDalStorageFactory::Oss => Ok(Arc::new(OpenDalStorage::Oss {
                config: oss_config_parse(config.props().clone())?.into(),
            })),
            #[cfg(feature = "opendal-azdls")]
            OpenDalStorageFactory::Azdls { configured_scheme } => {
                Ok(Arc::new(OpenDalStorage::Azdls {
                    configured_scheme: configured_scheme.clone(),
                    config: azdls_config_parse(config.props().clone())?.into(),
                }))
            }
            #[cfg(all(
                not(feature = "opendal-memory"),
                not(feature = "opendal-fs"),
                not(feature = "opendal-s3"),
                not(feature = "opendal-gcs"),
                not(feature = "opendal-oss"),
                not(feature = "opendal-azdls"),
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
    #[cfg(feature = "opendal-s3")]
    S3 {
        /// s3 storage could have `s3://` and `s3a://`.
        /// Storing the scheme string here to return the correct path.
        configured_scheme: String,
        /// S3 configuration.
        config: Arc<S3Config>,
        /// Custom AWS credential loader.
        #[serde(skip)]
        customized_credential_load: Option<s3::CustomAwsCredentialLoader>,
    },
    /// GCS storage variant.
    #[cfg(feature = "opendal-gcs")]
    Gcs {
        /// GCS configuration.
        config: Arc<GcsConfig>,
    },
    /// OSS storage variant.
    #[cfg(feature = "opendal-oss")]
    Oss {
        /// OSS configuration.
        config: Arc<OssConfig>,
    },
    /// Azure Data Lake Storage variant.
    /// Expects paths of the form
    /// `abfs[s]://<filesystem>@<account>.dfs.<endpoint-suffix>/<path>` or
    /// `wasb[s]://<container>@<account>.blob.<endpoint-suffix>/<path>`.
    #[cfg(feature = "opendal-azdls")]
    #[allow(private_interfaces)]
    Azdls {
        /// The configured Azure storage scheme.
        /// Because Azdls accepts multiple possible schemes, we store the full
        /// passed scheme here to later validate schemes passed via paths.
        configured_scheme: AzureStorageScheme,
        /// Azure DLS configuration.
        config: Arc<AzdlsConfig>,
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
                configured_scheme,
                config,
                customized_credential_load,
            } => {
                let op = s3_config_build(config, customized_credential_load, path)?;
                let op_info = op.info();

                // Check prefix of s3 path.
                let prefix = format!("{}://{}/", configured_scheme, op_info.name());
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
            OpenDalStorage::Gcs { config } => {
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
            OpenDalStorage::Azdls {
                configured_scheme,
                config,
            } => azdls_create_operator(path, config, configured_scheme)?,
            #[cfg(all(
                not(feature = "opendal-s3"),
                not(feature = "opendal-fs"),
                not(feature = "opendal-gcs"),
                not(feature = "opendal-oss"),
                not(feature = "opendal-azdls"),
            ))]
            _ => {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    "No storage service has been enabled",
                ));
            }
        };

        // Transient errors are common for object stores; however there's no
        // harm in retrying temporary failures for other storage backends as well.
        let operator = operator.layer(RetryLayer::new());
        Ok((operator, relative_path))
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
        Ok(op.remove_all(&path).await.map_err(from_opendal_error)?)
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

    #[cfg(feature = "opendal-memory")]
    #[test]
    fn test_default_memory_operator() {
        let op = default_memory_operator();
        assert_eq!(op.info().scheme().to_string(), "memory");
    }
}
