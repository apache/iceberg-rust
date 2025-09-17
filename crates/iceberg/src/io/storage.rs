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

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use opendal::layers::RetryLayer;
#[cfg(feature = "storage-azdls")]
use opendal::services::AzdlsConfig;
#[cfg(feature = "storage-gcs")]
use opendal::services::GcsConfig;
#[cfg(feature = "storage-oss")]
use opendal::services::OssConfig;
#[cfg(feature = "storage-s3")]
use opendal::services::S3Config;
use opendal::{Operator, Scheme};

#[cfg(feature = "storage-azdls")]
use super::AzureStorageScheme;
use super::{FileIOBuilder, FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage};
#[cfg(feature = "storage-s3")]
use crate::io::CustomAwsCredentialLoader;
use crate::{Error, ErrorKind, Result};

/// The storage carries all supported storage services in iceberg
#[derive(Debug, Clone)]
pub(crate) enum OpenDALStorage {
    #[cfg(feature = "storage-memory")]
    Memory(Operator),
    #[cfg(feature = "storage-fs")]
    LocalFs,
    /// Expects paths of the form `s3[a]://<bucket>/<path>`.
    #[cfg(feature = "storage-s3")]
    S3 {
        /// s3 storage could have `s3://` and `s3a://`.
        /// Storing the scheme string here to return the correct path.
        configured_scheme: String,
        config: Arc<S3Config>,
        customized_credential_load: Option<CustomAwsCredentialLoader>,
    },
    #[cfg(feature = "storage-gcs")]
    Gcs { config: Arc<GcsConfig> },
    #[cfg(feature = "storage-oss")]
    Oss { config: Arc<OssConfig> },
    /// Expects paths of the form
    /// `abfs[s]://<filesystem>@<account>.dfs.<endpoint-suffix>/<path>` or
    /// `wasb[s]://<container>@<account>.blob.<endpoint-suffix>/<path>`.
    #[cfg(feature = "storage-azdls")]
    Azdls {
        /// Because Azdls accepts multiple possible schemes, we store the full
        /// passed scheme here to later validate schemes passed via paths.
        configured_scheme: AzureStorageScheme,
        config: Arc<AzdlsConfig>,
    },
}

#[async_trait]
impl Storage for OpenDALStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let (op, relative_path) = self.create_operator(&path)?;
        Ok(op.exists(relative_path).await?)
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let (op, relative_path) = self.create_operator(&path)?;
        let meta = op.stat(relative_path).await?;

        Ok(FileMetadata {
            size: meta.content_length(),
        })
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let (op, relative_path) = self.create_operator(&path)?;
        Ok(op.read(relative_path).await?.to_bytes())
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let (op, relative_path) = self.create_operator(&path)?;
        Ok(Box::new(op.reader(relative_path).await?))
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        let mut writer = self.writer(path).await?;
        writer.write(bs).await?;
        writer.close().await
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let (op, relative_path) = self.create_operator(&path)?;
        Ok(Box::new(op.writer(relative_path).await?))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let (op, relative_path) = self.create_operator(&path)?;
        Ok(op.delete(relative_path).await?)
    }

    async fn remove_dir_all(&self, path: &str) -> Result<()> {
        let (op, relative_path) = self.create_operator(&path)?;
        let path = if relative_path.ends_with('/') {
            relative_path.to_string()
        } else {
            format!("{relative_path}/")
        };
        Ok(op.remove_all(&path).await?)
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        let storage = Arc::new(self.clone());
        let path = path.to_string();
        Ok(InputFile { storage, path })
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        let storage = Arc::new(self.clone());
        let path = path.to_string();
        Ok(OutputFile { storage, path })
    }
}

impl OpenDALStorage {
    /// Convert iceberg config to opendal config.
    pub(crate) fn build(file_io_builder: FileIOBuilder) -> Result<Self> {
        let (scheme_str, props, extensions) = file_io_builder.into_parts();
        let scheme = Self::parse_scheme(&scheme_str)?;

        match scheme {
            #[cfg(feature = "storage-memory")]
            Scheme::Memory => Ok(Self::Memory(super::memory_config_build()?)),
            #[cfg(feature = "storage-fs")]
            Scheme::Fs => Ok(Self::LocalFs),
            #[cfg(feature = "storage-s3")]
            Scheme::S3 => Ok(Self::S3 {
                configured_scheme: scheme_str,
                config: super::s3_config_parse(props)?.into(),
                customized_credential_load: extensions
                    .get::<CustomAwsCredentialLoader>()
                    .map(Arc::unwrap_or_clone),
            }),
            #[cfg(feature = "storage-gcs")]
            Scheme::Gcs => Ok(Self::Gcs {
                config: super::gcs_config_parse(props)?.into(),
            }),
            #[cfg(feature = "storage-oss")]
            Scheme::Oss => Ok(Self::Oss {
                config: super::oss_config_parse(props)?.into(),
            }),
            #[cfg(feature = "storage-azdls")]
            Scheme::Azdls => {
                let scheme = scheme_str.parse::<AzureStorageScheme>()?;
                Ok(Self::Azdls {
                    config: super::azdls_config_parse(props)?.into(),
                    configured_scheme: scheme,
                })
            }
            // Update doc on [`FileIO`] when adding new schemes.
            _ => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!("Constructing file io from scheme: {scheme} not supported now",),
            )),
        }
    }

    /// Creates operator from path.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
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
    ) -> crate::Result<(Operator, &'a str)> {
        let path = path.as_ref();
        let (operator, relative_path): (Operator, &str) = match self {
            #[cfg(feature = "storage-memory")]
            OpenDALStorage::Memory(op) => {
                if let Some(stripped) = path.strip_prefix("memory:/") {
                    Ok::<_, crate::Error>((op.clone(), stripped))
                } else {
                    Ok::<_, crate::Error>((op.clone(), &path[1..]))
                }
            }
            #[cfg(feature = "storage-fs")]
            OpenDALStorage::LocalFs => {
                let op = super::fs_config_build()?;

                if let Some(stripped) = path.strip_prefix("file:/") {
                    Ok::<_, crate::Error>((op, stripped))
                } else {
                    Ok::<_, crate::Error>((op, &path[1..]))
                }
            }
            #[cfg(feature = "storage-s3")]
            OpenDALStorage::S3 {
                configured_scheme,
                config,
                customized_credential_load,
            } => {
                let op = super::s3_config_build(config, customized_credential_load, path)?;
                let op_info = op.info();

                // Check prefix of s3 path.
                let prefix = format!("{}://{}/", configured_scheme, op_info.name());
                if path.starts_with(&prefix) {
                    Ok((op, &path[prefix.len()..]))
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid s3 url: {}, should start with {}", path, prefix),
                    ))
                }
            }
            #[cfg(feature = "storage-gcs")]
            OpenDALStorage::Gcs { config } => {
                let operator = super::gcs_config_build(config, path)?;
                let prefix = format!("gs://{}/", operator.info().name());
                if path.starts_with(&prefix) {
                    Ok((operator, &path[prefix.len()..]))
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid gcs url: {}, should start with {}", path, prefix),
                    ))
                }
            }
            #[cfg(feature = "storage-oss")]
            OpenDALStorage::Oss { config } => {
                let op = super::oss_config_build(config, path)?;

                // Check prefix of oss path.
                let prefix = format!("oss://{}/", op.info().name());
                if path.starts_with(&prefix) {
                    Ok((op, &path[prefix.len()..]))
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid oss url: {}, should start with {}", path, prefix),
                    ))
                }
            }
            #[cfg(feature = "storage-azdls")]
            OpenDALStorage::Azdls {
                configured_scheme,
                config,
            } => super::azdls_create_operator(path, config, configured_scheme),
            #[cfg(all(
                not(feature = "storage-s3"),
                not(feature = "storage-fs"),
                not(feature = "storage-gcs"),
                not(feature = "storage-oss"),
                not(feature = "storage-azdls"),
            ))]
            _ => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "No storage service has been enabled",
            )),
        }?;

        // Transient errors are common for object stores; however there's no
        // harm in retrying temporary failures for other storage backends as well.
        let operator = operator.layer(RetryLayer::new());

        Ok((operator, relative_path))
    }

    /// Parse scheme.
    fn parse_scheme(scheme: &str) -> crate::Result<Scheme> {
        match scheme {
            "memory" => Ok(Scheme::Memory),
            "file" | "" => Ok(Scheme::Fs),
            "s3" | "s3a" => Ok(Scheme::S3),
            "gs" | "gcs" => Ok(Scheme::Gcs),
            "oss" => Ok(Scheme::Oss),
            "abfss" | "abfs" | "wasbs" | "wasb" => Ok(Scheme::Azdls),
            s => Ok(s.parse::<Scheme>()?),
        }
    }
}
