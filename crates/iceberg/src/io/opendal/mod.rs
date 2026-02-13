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

//! OpenDAL-based storage implementation.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
#[cfg(feature = "storage-azdls")]
use azdls::AzureStorageScheme;
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
#[cfg(feature = "storage-s3")]
pub use s3::CustomAwsCredentialLoader;
use serde::{Deserialize, Serialize};

use super::file_io::Extensions;
use super::refreshable_storage::RefreshableOpenDalStorageBuilder;
use super::{
    FileIOBuilder, FileMetadata, FileRead, FileWrite, InputFile, MetadataLocation, OutputFile,
    Storage, StorageConfig, StorageCredential, StorageCredentialsLoader, StorageFactory,
};
use crate::{Error, ErrorKind, Result};

#[cfg(feature = "storage-azdls")]
mod azdls;
#[cfg(feature = "storage-fs")]
mod fs;
#[cfg(feature = "storage-gcs")]
mod gcs;
#[cfg(feature = "storage-memory")]
mod memory;
#[cfg(feature = "storage-oss")]
mod oss;
#[cfg(feature = "storage-s3")]
mod s3;

#[cfg(feature = "storage-azdls")]
use azdls::*;
#[cfg(feature = "storage-fs")]
use fs::*;
#[cfg(feature = "storage-gcs")]
use gcs::*;
#[cfg(feature = "storage-memory")]
use memory::*;
#[cfg(feature = "storage-oss")]
use oss::*;
#[cfg(feature = "storage-s3")]
pub use s3::*;

/// OpenDAL-based storage factory.
///
/// Maps scheme to the corresponding OpenDalStorage storage variant.
///
/// TODO this is currently not used, we still use OpenDalStorage::build() for now
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum OpenDalStorageFactory {
    /// Memory storage factory.
    #[cfg(feature = "storage-memory")]
    Memory,
    /// Local filesystem storage factory.
    #[cfg(feature = "storage-fs")]
    Fs,
    /// S3 storage factory.
    #[cfg(feature = "storage-s3")]
    S3 {
        /// Custom AWS credential loader.
        #[serde(skip)]
        customized_credential_load: Option<CustomAwsCredentialLoader>,
    },
    /// GCS storage factory.
    #[cfg(feature = "storage-gcs")]
    Gcs,
    /// OSS storage factory.
    #[cfg(feature = "storage-oss")]
    Oss,
    /// Azure Data Lake Storage factory.
    #[cfg(feature = "storage-azdls")]
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
            #[cfg(feature = "storage-memory")]
            OpenDalStorageFactory::Memory => {
                Ok(Arc::new(OpenDalStorage::Memory(memory_config_build()?)))
            }
            #[cfg(feature = "storage-fs")]
            OpenDalStorageFactory::Fs => Ok(Arc::new(OpenDalStorage::LocalFs)),
            #[cfg(feature = "storage-s3")]
            OpenDalStorageFactory::S3 {
                customized_credential_load,
            } => Ok(Arc::new(OpenDalStorage::S3 {
                configured_scheme: "s3".to_string(),
                config: s3_config_parse(config.props().clone())?.into(),
                customized_credential_load: customized_credential_load.clone(),
            })),
            #[cfg(feature = "storage-gcs")]
            OpenDalStorageFactory::Gcs => Ok(Arc::new(OpenDalStorage::Gcs {
                config: gcs_config_parse(config.props().clone())?.into(),
            })),
            #[cfg(feature = "storage-oss")]
            OpenDalStorageFactory::Oss => Ok(Arc::new(OpenDalStorage::Oss {
                config: oss_config_parse(config.props().clone())?.into(),
            })),
            #[cfg(feature = "storage-azdls")]
            OpenDalStorageFactory::Azdls { configured_scheme } => {
                Ok(Arc::new(OpenDalStorage::Azdls {
                    configured_scheme: configured_scheme.clone(),
                    config: azdls_config_parse(config.props().clone())?.into(),
                }))
            }
            #[cfg(all(
                not(feature = "storage-memory"),
                not(feature = "storage-fs"),
                not(feature = "storage-s3"),
                not(feature = "storage-gcs"),
                not(feature = "storage-oss"),
                not(feature = "storage-azdls"),
            ))]
            _ => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "No storage service has been enabled",
            )),
        }
    }
}

/// Default memory operator for serde deserialization.
#[cfg(feature = "storage-memory")]
fn default_memory_operator() -> Operator {
    memory_config_build().expect("Failed to create default memory operator")
}

/// OpenDAL-based storage implementation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum OpenDalStorage {
    /// Memory storage variant.
    #[cfg(feature = "storage-memory")]
    Memory(#[serde(skip, default = "self::default_memory_operator")] Operator),
    /// Local filesystem storage variant.
    #[cfg(feature = "storage-fs")]
    LocalFs,
    /// S3 storage variant.
    #[cfg(feature = "storage-s3")]
    S3 {
        /// s3 storage could have `s3://` and `s3a://`.
        /// Storing the scheme string here to return the correct path.
        configured_scheme: String,
        /// S3 configuration.
        config: Arc<S3Config>,
        /// Custom AWS credential loader.
        #[serde(skip)]
        customized_credential_load: Option<CustomAwsCredentialLoader>,
    },
    /// GCS storage variant.
    #[cfg(feature = "storage-gcs")]
    Gcs {
        /// GCS configuration.
        config: Arc<GcsConfig>,
    },
    /// OSS storage variant.
    #[cfg(feature = "storage-oss")]
    Oss {
        /// OSS configuration.
        config: Arc<OssConfig>,
    },
    /// Azure Data Lake Storage variant.
    /// Expects paths of the form
    /// `abfs[s]://<filesystem>@<account>.dfs.<endpoint-suffix>/<path>` or
    /// `wasb[s]://<container>@<account>.blob.<endpoint-suffix>/<path>`.
    #[cfg(feature = "storage-azdls")]
    #[allow(private_interfaces)]
    Azdls {
        /// The configured Azure storage scheme.
        /// Because Azdls accepts multiple possible schemes, we store the full
        /// passed scheme here to later validate schemes passed via paths.
        configured_scheme: AzureStorageScheme,
        /// Azure DLS configuration.
        config: Arc<AzdlsConfig>,
    },
    /// Wraps any storage with credential refresh capability.
    Refreshable {
        /// The refreshable storage backend.
        /// `None` only after deserialization (cannot be reconstructed from serialized form).
        #[serde(skip)]
        backend: Option<Arc<super::refreshable_storage::RefreshableOpenDalStorage>>,
    },
}

impl OpenDalStorage {
    /// Convert iceberg config to opendal config.
    ///
    /// TODO Switch to use OpenDalStorageFactory::build()
    pub(crate) fn build(file_io_builder: FileIOBuilder) -> Result<Self> {
        let (scheme_str, props, extensions) = file_io_builder.into_parts();

        // Check if credential refresh is requested
        if let Some(loader) = extensions.get::<Arc<dyn StorageCredentialsLoader>>() {
            let initial_creds = extensions.get::<StorageCredential>().map(|c| (*c).clone());
            let location = extensions
                .get::<MetadataLocation>()
                .map(|l| l.0.clone())
                .unwrap_or_default();
            let backend = RefreshableOpenDalStorageBuilder::new()
                .scheme(scheme_str)
                .base_props(props)
                .credentials_loader(Arc::clone(&loader))
                .initial_credentials(initial_creds)
                .location(location)
                .extensions(extensions)
                .build()?;
            return Ok(Self::Refreshable {
                backend: Some(backend),
            });
        }

        // Otherwise, build storage normally
        Self::build_from_props(&scheme_str, props, &extensions)
    }

    /// Build storage from scheme, properties, and extensions.
    ///
    /// This is the core builder used by both `build()` and `RefreshableOpenDalStorage`.
    pub(crate) fn build_from_props(
        scheme_str: &str,
        props: HashMap<String, String>,
        extensions: &Extensions,
    ) -> Result<Self> {
        let _ = (&props, &extensions);
        let scheme = Self::parse_scheme(scheme_str)?;

        match scheme {
            #[cfg(feature = "storage-memory")]
            Scheme::Memory => Ok(Self::Memory(memory_config_build()?)),
            #[cfg(feature = "storage-fs")]
            Scheme::Fs => Ok(Self::LocalFs),
            #[cfg(feature = "storage-s3")]
            Scheme::S3 => Ok(Self::S3 {
                configured_scheme: scheme_str.to_string(),
                config: s3_config_parse(props)?.into(),
                customized_credential_load: extensions
                    .get::<CustomAwsCredentialLoader>()
                    .map(Arc::unwrap_or_clone),
            }),
            #[cfg(feature = "storage-gcs")]
            Scheme::Gcs => Ok(Self::Gcs {
                config: gcs_config_parse(props)?.into(),
            }),
            #[cfg(feature = "storage-oss")]
            Scheme::Oss => Ok(Self::Oss {
                config: oss_config_parse(props)?.into(),
            }),
            #[cfg(feature = "storage-azdls")]
            Scheme::Azdls => {
                let scheme = scheme_str.parse::<AzureStorageScheme>()?;
                Ok(Self::Azdls {
                    config: azdls_config_parse(props)?.into(),
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
    #[allow(unreachable_code, unused_variables)]
    pub(crate) fn create_operator<'a>(
        &self,
        path: &'a impl AsRef<str>,
    ) -> Result<(Operator, &'a str)> {
        let path = path.as_ref();
        let (operator, relative_path): (Operator, &str) = match self {
            #[cfg(feature = "storage-memory")]
            OpenDalStorage::Memory(op) => {
                if let Some(stripped) = path.strip_prefix("memory:/") {
                    (op.clone(), stripped)
                } else {
                    (op.clone(), &path[1..])
                }
            }
            #[cfg(feature = "storage-fs")]
            OpenDalStorage::LocalFs => {
                let op = fs_config_build()?;
                if let Some(stripped) = path.strip_prefix("file:/") {
                    (op, stripped)
                } else {
                    (op, &path[1..])
                }
            }
            #[cfg(feature = "storage-s3")]
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
            #[cfg(feature = "storage-gcs")]
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
            #[cfg(feature = "storage-oss")]
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
            #[cfg(feature = "storage-azdls")]
            OpenDalStorage::Azdls {
                configured_scheme,
                config,
            } => azdls_create_operator(path, config, configured_scheme)?,
            OpenDalStorage::Refreshable { backend } => {
                let backend = backend.as_ref().ok_or_else(|| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "Deserialized RefreshableOpenDalStorage cannot create operators",
                    )
                })?;
                let (operator, relative_path) = backend.refreshable_create_operator(path)?;
                // relative_path is always a suffix of `path`, so slice the
                // input directly to avoid an allocation.
                let relative_path_ref = &path[path.len() - relative_path.len()..];
                (operator, relative_path_ref)
            }
            #[cfg(all(
                not(feature = "storage-s3"),
                not(feature = "storage-fs"),
                not(feature = "storage-gcs"),
                not(feature = "storage-oss"),
                not(feature = "storage-azdls"),
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

    /// Parse scheme.
    fn parse_scheme(scheme: &str) -> Result<Scheme> {
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

#[typetag::serde(name = "OpenDalStorage")]
#[async_trait]
impl Storage for OpenDalStorage {
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
        let (op, relative_path) = self.create_operator(&path)?;
        op.write(relative_path, bs).await?;
        Ok(())
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let (op, relative_path) = self.create_operator(&path)?;
        Ok(Box::new(op.writer(relative_path).await?))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let (op, relative_path) = self.create_operator(&path)?;
        Ok(op.delete(relative_path).await?)
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let (op, relative_path) = self.create_operator(&path)?;
        let path = if relative_path.ends_with('/') {
            relative_path.to_string()
        } else {
            format!("{relative_path}/")
        };
        Ok(op.remove_all(&path).await?)
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

// OpenDAL implementations for FileRead and FileWrite traits

#[async_trait]
impl FileRead for opendal::Reader {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        Ok(opendal::Reader::read(self, range).await?.to_bytes())
    }
}

#[async_trait]
impl FileWrite for opendal::Writer {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        Ok(opendal::Writer::write(self, bs).await?)
    }

    async fn close(&mut self) -> Result<()> {
        let _ = opendal::Writer::close(self).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{FileIOBuilder, StorageCredential, StorageCredentialsLoader};

    #[cfg(feature = "storage-memory")]
    #[test]
    fn test_default_memory_operator() {
        let op = default_memory_operator();
        assert_eq!(op.info().scheme().to_string(), "memory");
    }

    #[derive(Debug)]
    struct TestCredentialLoader;

    #[async_trait::async_trait]
    impl StorageCredentialsLoader for TestCredentialLoader {
        async fn load_credentials(&self, _location: &str) -> crate::Result<StorageCredential> {
            Ok(StorageCredential {
                prefix: "s3://test/".to_string(),
                config: HashMap::new(),
            })
        }
    }

    #[test]
    fn test_storage_build_with_credentials_loader_creates_refreshable() {
        let loader: Arc<dyn StorageCredentialsLoader> = Arc::new(TestCredentialLoader);

        let file_io_builder = FileIOBuilder::new("s3")
            .with_prop("bucket", "test-bucket")
            .with_extension(loader);

        let storage = OpenDalStorage::build(file_io_builder).unwrap();

        // Verify it created a Refreshable variant
        match storage {
            OpenDalStorage::Refreshable { backend: Some(_) } => {} // Success
            _ => panic!("Expected Refreshable variant"),
        }
    }

    #[cfg(feature = "storage-memory")]
    #[test]
    fn test_storage_build_without_loader_creates_normal_storage() {
        let file_io_builder = FileIOBuilder::new("memory");
        let storage = OpenDalStorage::build(file_io_builder).unwrap();

        // Verify it created a normal Memory variant
        match storage {
            OpenDalStorage::Memory(_) => {} // Success
            _ => panic!("Expected Memory variant"),
        }
    }

    #[test]
    fn test_storage_build_with_both_loader_and_initial_credentials() {
        let loader: Arc<dyn StorageCredentialsLoader> = Arc::new(TestCredentialLoader);
        let initial_cred = StorageCredential {
            prefix: "s3://initial/".to_string(),
            config: HashMap::new(),
        };

        let file_io_builder = FileIOBuilder::new("s3")
            .with_prop("bucket", "test-bucket")
            .with_extension(loader)
            .with_extension(initial_cred);

        let storage = OpenDalStorage::build(file_io_builder).unwrap();

        // Verify it created a Refreshable variant
        match storage {
            OpenDalStorage::Refreshable { backend: Some(_) } => {} // Success
            _ => panic!("Expected Refreshable variant"),
        }
    }

    #[cfg(feature = "storage-s3")]
    #[test]
    fn test_storage_build_from_props_never_creates_refreshable() {
        let mut props = HashMap::new();
        props.insert("bucket".to_string(), "test-bucket".to_string());

        let storage = OpenDalStorage::build_from_props("s3", props, &Default::default()).unwrap();

        // Verify it created a normal S3 variant, not Refreshable
        match storage {
            OpenDalStorage::S3 { .. } => {} // Success
            OpenDalStorage::Refreshable { .. } => {
                panic!("build_from_props should not create Refreshable")
            }
            _ => panic!("Expected S3 variant"),
        }
    }
}
