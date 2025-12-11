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

//! Storage implementation for Iceberg using OpenDAL.
//!
//! This module provides a unified storage abstraction that handles all supported
//! storage backends (S3, GCS, Azure, local filesystem, memory, etc.) through OpenDAL.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use opendal::layers::RetryLayer;
use opendal::Operator;
use serde::{Deserialize, Serialize};

use super::{FileIOBuilder, FileMetadata, FileRead, FileWrite, InputFile, OutputFile};
use crate::{Error, ErrorKind, Result};

/// Trait for storage operations in Iceberg.
///
/// This trait defines the interface for all storage backends. The default implementation
/// uses OpenDAL to support various storage systems like S3, GCS, Azure, local filesystem, etc.
///
/// The trait supports serialization via `typetag`, allowing storage instances to be
/// serialized and deserialized across process boundaries.
///
/// Third-party implementations can implement this trait to provide custom storage backends.
#[async_trait]
#[typetag::serde(tag = "type")]
pub trait Storage: Debug + Send + Sync {
    /// Check if a file exists at the given path
    async fn exists(&self, path: &str) -> Result<bool>;

    /// Get metadata from an input path
    async fn metadata(&self, path: &str) -> Result<FileMetadata>;

    /// Read bytes from a path
    async fn read(&self, path: &str) -> Result<Bytes>;

    /// Get FileRead from a path
    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>>;

    /// Write bytes to an output path
    async fn write(&self, path: &str, bs: Bytes) -> Result<()>;

    /// Get FileWrite from a path
    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>>;

    /// Delete a file at the given path
    async fn delete(&self, path: &str) -> Result<()>;

    /// Delete all files with the given prefix
    async fn delete_prefix(&self, path: &str) -> Result<()>;

    /// Create a new input file for reading
    fn new_input(&self, path: &str) -> Result<InputFile>;

    /// Create a new output file for writing
    fn new_output(&self, path: &str) -> Result<OutputFile>;
}

/// Unified OpenDAL-based storage implementation.
///
/// This storage handles all supported schemes (S3, GCS, Azure, filesystem, memory)
/// through OpenDAL, creating operators on-demand based on the path scheme.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum OpenDALStorage {
    /// In-memory storage, useful for testing
    #[cfg(feature = "storage-memory")]
    Memory {
        /// Cached operator (lazily initialized)
        #[serde(skip, default = "default_memory_op")]
        op: Arc<std::sync::Mutex<Option<Operator>>>,
    },
    /// Local filesystem storage
    #[cfg(feature = "storage-fs")]
    LocalFs,
    /// Amazon S3 storage
    #[cfg(feature = "storage-s3")]
    S3 {
        /// The configured scheme (s3 or s3a)
        configured_scheme: String,
        /// S3 configuration
        config: opendal::services::S3Config,
        /// Optional custom credential loader
        #[serde(skip)]
        customized_credential_load: Option<super::CustomAwsCredentialLoader>,
    },
    /// Google Cloud Storage
    #[cfg(feature = "storage-gcs")]
    Gcs {
        /// GCS configuration
        config: opendal::services::GcsConfig,
    },
    /// Alibaba Cloud OSS
    #[cfg(feature = "storage-oss")]
    Oss {
        /// OSS configuration
        config: opendal::services::OssConfig,
    },
    /// Azure Data Lake Storage
    #[cfg(feature = "storage-azdls")]
    Azdls {
        /// The configured scheme (abfs, abfss, wasb, wasbs)
        configured_scheme: super::AzureStorageScheme,
        /// Azure DLS configuration
        config: opendal::services::AzdlsConfig,
    },
}

#[cfg(feature = "storage-memory")]
fn default_memory_op() -> Arc<std::sync::Mutex<Option<Operator>>> {
    Arc::new(std::sync::Mutex::new(None))
}

impl OpenDALStorage {
    /// Build storage from FileIOBuilder
    pub fn build(file_io_builder: FileIOBuilder) -> Result<Self> {
        let (scheme_str, props, extensions) = file_io_builder.into_parts();
        let _ = (&props, &extensions);

        match scheme_str.to_lowercase().as_str() {
            #[cfg(feature = "storage-memory")]
            "memory" => Ok(OpenDALStorage::Memory {
                op: Arc::new(std::sync::Mutex::new(None)),
            }),

            #[cfg(feature = "storage-fs")]
            "file" | "" => Ok(OpenDALStorage::LocalFs),

            #[cfg(feature = "storage-s3")]
            "s3" | "s3a" => {
                let config = super::s3_config_parse(props)?;
                let customized_credential_load = extensions
                    .get::<super::CustomAwsCredentialLoader>()
                    .map(Arc::unwrap_or_clone);
                Ok(OpenDALStorage::S3 {
                    configured_scheme: scheme_str,
                    config,
                    customized_credential_load,
                })
            }

            #[cfg(feature = "storage-gcs")]
            "gs" | "gcs" => {
                let config = super::gcs_config_parse(props)?;
                Ok(OpenDALStorage::Gcs { config })
            }

            #[cfg(feature = "storage-oss")]
            "oss" => {
                let config = super::oss_config_parse(props)?;
                Ok(OpenDALStorage::Oss { config })
            }

            #[cfg(feature = "storage-azdls")]
            "abfs" | "abfss" | "wasb" | "wasbs" => {
                let configured_scheme = scheme_str.parse::<super::AzureStorageScheme>()?;
                let config = super::azdls_config_parse(props)?;
                Ok(OpenDALStorage::Azdls {
                    configured_scheme,
                    config,
                })
            }

            _ => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!(
                    "Constructing file io from scheme: {} not supported now",
                    scheme_str
                ),
            )),
        }
    }

    /// Creates operator from path.
    fn create_operator<'a>(&self, path: &'a str) -> Result<(Operator, &'a str)> {
        let (operator, relative_path): (Operator, &str) = match self {
            #[cfg(feature = "storage-memory")]
            OpenDALStorage::Memory { op } => {
                let mut guard = op.lock().unwrap();
                if guard.is_none() {
                    *guard = Some(super::memory_config_build()?);
                }
                let op = guard.as_ref().unwrap().clone();

                if let Some(stripped) = path.strip_prefix("memory:/") {
                    Ok((op, stripped))
                } else {
                    Ok((op, &path[1..]))
                }
            }

            #[cfg(feature = "storage-fs")]
            OpenDALStorage::LocalFs => {
                let op = super::fs_config_build()?;

                if let Some(stripped) = path.strip_prefix("file:/") {
                    Ok((op, stripped))
                } else {
                    Ok((op, &path[1..]))
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

                let prefix = format!("{}://{}/", configured_scheme, op_info.name());
                if path.starts_with(&prefix) {
                    Ok((op, &path[prefix.len()..]))
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid s3 url: {path}, should start with {prefix}"),
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
                        format!("Invalid gcs url: {path}, should start with {prefix}"),
                    ))
                }
            }

            #[cfg(feature = "storage-oss")]
            OpenDALStorage::Oss { config } => {
                let op = super::oss_config_build(config, path)?;
                let prefix = format!("oss://{}/", op.info().name());
                if path.starts_with(&prefix) {
                    Ok((op, &path[prefix.len()..]))
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid oss url: {path}, should start with {prefix}"),
                    ))
                }
            }

            #[cfg(feature = "storage-azdls")]
            OpenDALStorage::Azdls {
                configured_scheme,
                config,
            } => super::azdls_create_operator(path, config, configured_scheme),
        }?;

        // Transient errors are common for object stores
        let operator = operator.layer(RetryLayer::new());

        Ok((operator, relative_path))
    }
}

#[async_trait]
#[typetag::serde]
impl Storage for OpenDALStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let (op, relative_path) = self.create_operator(path)?;
        Ok(op.exists(relative_path).await?)
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let (op, relative_path) = self.create_operator(path)?;
        let meta = op.stat(relative_path).await?;
        Ok(FileMetadata {
            size: meta.content_length(),
        })
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let (op, relative_path) = self.create_operator(path)?;
        Ok(op.read(relative_path).await?.to_bytes())
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let (op, relative_path) = self.create_operator(path)?;
        Ok(Box::new(op.reader(relative_path).await?))
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        let mut writer = self.writer(path).await?;
        writer.write(bs).await?;
        writer.close().await
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let (op, relative_path) = self.create_operator(path)?;
        Ok(Box::new(op.writer(relative_path).await?))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let (op, relative_path) = self.create_operator(path)?;
        Ok(op.delete(relative_path).await?)
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let (op, relative_path) = self.create_operator(path)?;
        let path = if relative_path.ends_with('/') {
            relative_path.to_string()
        } else {
            format!("{relative_path}/")
        };
        Ok(op.remove_all(&path).await?)
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIOBuilder;

    #[test]
    #[cfg(feature = "storage-memory")]
    fn test_opendal_storage_memory() {
        let builder = FileIOBuilder::new("memory");
        let storage = OpenDALStorage::build(builder).unwrap();
        assert!(matches!(storage, OpenDALStorage::Memory { .. }));
    }

    #[test]
    #[cfg(feature = "storage-fs")]
    fn test_opendal_storage_fs() {
        let builder = FileIOBuilder::new("file");
        let storage = OpenDALStorage::build(builder).unwrap();
        assert!(matches!(storage, OpenDALStorage::LocalFs));
    }

    #[test]
    #[cfg(feature = "storage-s3")]
    fn test_opendal_storage_s3() {
        let builder = FileIOBuilder::new("s3");
        let storage = OpenDALStorage::build(builder).unwrap();
        assert!(matches!(storage, OpenDALStorage::S3 { .. }));
    }

    #[test]
    #[cfg(feature = "storage-memory")]
    fn test_storage_serialization() {
        let builder = FileIOBuilder::new("memory");
        let storage = OpenDALStorage::build(builder).unwrap();

        // Serialize
        let serialized = serde_json::to_string(&storage).unwrap();

        // Deserialize
        let deserialized: OpenDALStorage = serde_json::from_str(&serialized).unwrap();

        assert!(matches!(deserialized, OpenDALStorage::Memory { .. }));
    }
}
