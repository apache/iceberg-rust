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

use std::future::Future;
use std::pin::Pin;
#[cfg(any(
    feature = "storage-s3",
    feature = "storage-gcs",
    feature = "storage-oss",
    feature = "storage-azdls",
))]
use std::sync::Arc;

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
use super::{FileIOBuilder, RuntimeHandle};
#[cfg(feature = "storage-s3")]
use crate::io::CustomAwsCredentialLoader;
use crate::{Error, ErrorKind};

/// Custom OpenDAL executor that spawns tasks on a specific Tokio runtime.
///
/// This executor implements the OpenDAL Execute trait and routes all spawned
/// tasks to a configured Tokio runtime handle, enabling runtime segregation.
#[derive(Clone)]
struct CustomTokioExecutor {
    handle: tokio::runtime::Handle,
}

impl opendal::Execute for CustomTokioExecutor {
    fn execute(&self, f: Pin<Box<dyn Future<Output = ()> + Send>>) {
        self.handle.spawn(f);
    }
}

/// The storage carries all supported storage services in iceberg
pub(crate) struct Storage {
    backend: StorageBackend,
    executor: Option<opendal::Executor>,
}

#[derive(Debug)]
enum StorageBackend {
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

impl std::fmt::Debug for Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Storage")
            .field("backend", &self.backend)
            .field(
                "executor",
                &self.executor.as_ref().map(|_| "Some(Executor)"),
            )
            .finish()
    }
}

impl Storage {
    /// Convert iceberg config to opendal config.
    pub(crate) fn build(file_io_builder: FileIOBuilder) -> crate::Result<Self> {
        let (scheme_str, props, extensions) = file_io_builder.into_parts();
        let _ = (&props, &extensions);
        let scheme = Self::parse_scheme(&scheme_str)?;

        // Extract runtime handle and create executor if provided
        let executor = extensions.get::<RuntimeHandle>().map(|runtime_handle| {
            let handle = Arc::unwrap_or_clone(runtime_handle).0;
            opendal::Executor::with(CustomTokioExecutor { handle })
        });

        let backend = match scheme {
            #[cfg(feature = "storage-memory")]
            Scheme::Memory => StorageBackend::Memory(super::memory_config_build()?),
            #[cfg(feature = "storage-fs")]
            Scheme::Fs => StorageBackend::LocalFs,
            #[cfg(feature = "storage-s3")]
            Scheme::S3 => StorageBackend::S3 {
                configured_scheme: scheme_str,
                config: super::s3_config_parse(props)?.into(),
                customized_credential_load: extensions
                    .get::<CustomAwsCredentialLoader>()
                    .map(Arc::unwrap_or_clone),
            },
            #[cfg(feature = "storage-gcs")]
            Scheme::Gcs => StorageBackend::Gcs {
                config: super::gcs_config_parse(props)?.into(),
            },
            #[cfg(feature = "storage-oss")]
            Scheme::Oss => StorageBackend::Oss {
                config: super::oss_config_parse(props)?.into(),
            },
            #[cfg(feature = "storage-azdls")]
            Scheme::Azdls => {
                let scheme = scheme_str.parse::<AzureStorageScheme>()?;
                StorageBackend::Azdls {
                    config: super::azdls_config_parse(props)?.into(),
                    configured_scheme: scheme,
                }
            }
            // Update doc on [`FileIO`] when adding new schemes.
            _ => {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    format!("Constructing file io from scheme: {scheme} not supported now",),
                ));
            }
        };

        Ok(Self { backend, executor })
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
        let _ = path;
        let (operator, relative_path): (Operator, &str) = match &self.backend {
            #[cfg(feature = "storage-memory")]
            StorageBackend::Memory(op) => {
                if let Some(stripped) = path.strip_prefix("memory:/") {
                    Ok::<_, crate::Error>((op.clone(), stripped))
                } else {
                    Ok::<_, crate::Error>((op.clone(), &path[1..]))
                }
            }
            #[cfg(feature = "storage-fs")]
            StorageBackend::LocalFs => {
                let op = super::fs_config_build()?;

                if let Some(stripped) = path.strip_prefix("file:/") {
                    Ok::<_, crate::Error>((op, stripped))
                } else {
                    Ok::<_, crate::Error>((op, &path[1..]))
                }
            }
            #[cfg(feature = "storage-s3")]
            StorageBackend::S3 {
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
                        format!("Invalid s3 url: {path}, should start with {prefix}"),
                    ))
                }
            }
            #[cfg(feature = "storage-gcs")]
            StorageBackend::Gcs { config } => {
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
            StorageBackend::Oss { config } => {
                let op = super::oss_config_build(config, path)?;

                // Check prefix of oss path.
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
            StorageBackend::Azdls {
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

        // Apply custom executor if configured for runtime segregation
        if let Some(ref executor) = self.executor {
            let executor_clone = executor.clone();
            operator.update_executor(|_| executor_clone);
        }

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
