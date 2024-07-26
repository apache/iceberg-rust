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

use opendal::{Operator, Scheme};

use super::FileIOBuilder;
#[cfg(feature = "storage-fs")]
use super::FsConfig;
#[cfg(feature = "storage-memory")]
use super::MemoryConfig;
#[cfg(feature = "storage-s3")]
use super::S3Config;
use crate::{Error, ErrorKind};

/// The storage carries all supported storage services in iceberg
#[derive(Debug)]
pub(crate) enum Storage {
    #[cfg(feature = "storage-memory")]
    Memory { config: MemoryConfig },
    #[cfg(feature = "storage-fs")]
    LocalFs { config: FsConfig },
    #[cfg(feature = "storage-s3")]
    S3 {
        /// s3 storage could have `s3://` and `s3a://`.
        /// Storing the scheme string here to return the correct path.
        scheme_str: String,
        config: S3Config,
    },
}

impl Storage {
    /// Convert iceberg config to opendal config.
    pub(crate) fn build(file_io_builder: FileIOBuilder) -> crate::Result<Self> {
        let (scheme_str, props) = file_io_builder.into_parts();
        let scheme = Self::parse_scheme(&scheme_str)?;

        match scheme {
            #[cfg(feature = "storage-memory")]
            Scheme::Memory => Ok(Self::Memory {
                config: MemoryConfig::new(props),
            }),
            #[cfg(feature = "storage-fs")]
            Scheme::Fs => Ok(Self::LocalFs {
                config: FsConfig::new(props),
            }),
            #[cfg(feature = "storage-s3")]
            Scheme::S3 => Ok(Self::S3 {
                scheme_str,
                config: S3Config::new(props),
            }),
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
        match self {
            #[cfg(feature = "storage-memory")]
            Storage::Memory { config } => {
                let op = config.build(path)?;

                if let Some(stripped) = path.strip_prefix("memory:/") {
                    Ok((op, stripped))
                } else {
                    Ok((op, &path[1..]))
                }
            }
            #[cfg(feature = "storage-fs")]
            Storage::LocalFs { config } => {
                let op = config.build(path)?;

                if let Some(stripped) = path.strip_prefix("file:/") {
                    Ok((op, stripped))
                } else {
                    Ok((op, &path[1..]))
                }
            }
            #[cfg(feature = "storage-s3")]
            Storage::S3 { scheme_str, config } => {
                let op = config.build(path)?;
                let op_info = op.info();

                // Check prefix of s3 path.
                let prefix = format!("{}://{}/", scheme_str, op_info.name());
                if path.starts_with(&prefix) {
                    Ok((op, &path[prefix.len()..]))
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid s3 url: {}, should start with {}", path, prefix),
                    ))
                }
            }
            #[cfg(all(not(feature = "storage-s3"), not(feature = "storage-fs")))]
            _ => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "No storage service has been enabled",
            )),
        }
    }

    /// Parse scheme.
    fn parse_scheme(scheme: &str) -> crate::Result<Scheme> {
        match scheme {
            "memory" => Ok(Scheme::Memory),
            "file" | "" => Ok(Scheme::Fs),
            "s3" | "s3a" => Ok(Scheme::S3),
            s => Ok(s.parse::<Scheme>()?),
        }
    }
}

/// redact_secret will redact the secret part of the string.
#[inline]
pub(crate) fn redact_secret(s: &str) -> String {
    let len = s.len();
    if len <= 6 {
        return "***".to_string();
    }

    format!("{}***{}", &s[0..3], &s[len - 3..len])
}
