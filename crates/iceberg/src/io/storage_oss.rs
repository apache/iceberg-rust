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

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use opendal::services::OssConfig;
use opendal::{Configurator, Operator};
use url::Url;

use crate::io::{
    Extensions, InputFileRef, OpenDALInputFile, OpenDALOutputFile, OutputFileRef, Storage,
    StorageBuilder,
};
use crate::{Error, ErrorKind, Result};

/// Required configuration arguments for creating an Aliyun OSS Operator with OpenDAL:
/// - `oss.endpoint`: The OSS service endpoint URL
/// - `oss.access-key-id`: The access key ID for authentication
/// - `oss.access-key-secret`: The access key secret for authentication
///   Aliyun oss endpoint.
pub const OSS_ENDPOINT: &str = "oss.endpoint";
/// Aliyun oss access key id.
pub const OSS_ACCESS_KEY_ID: &str = "oss.access-key-id";
/// Aliyun oss access key secret.
pub const OSS_ACCESS_KEY_SECRET: &str = "oss.access-key-secret";

/// Parse iceberg props to oss config.
pub(crate) fn oss_config_parse(mut m: HashMap<String, String>) -> Result<OssConfig> {
    let mut cfg: OssConfig = OssConfig::default();
    if let Some(endpoint) = m.remove(OSS_ENDPOINT) {
        cfg.endpoint = Some(endpoint);
    };
    if let Some(access_key_id) = m.remove(OSS_ACCESS_KEY_ID) {
        cfg.access_key_id = Some(access_key_id);
    };
    if let Some(access_key_secret) = m.remove(OSS_ACCESS_KEY_SECRET) {
        cfg.access_key_secret = Some(access_key_secret);
    };

    Ok(cfg)
}

/// Build new opendal operator from give path.
pub(crate) fn oss_config_build(cfg: &OssConfig, path: &str) -> Result<Operator> {
    let url = Url::parse(path)?;
    let bucket = url.host_str().ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid oss url: {}, missing bucket", path),
        )
    })?;

    let builder = cfg.clone().into_builder().bucket(bucket);

    Ok(Operator::new(builder)?.finish())
}

/// OSS storage implementation using OpenDAL
#[derive(Debug)]
pub struct OpenDALOssStorage {
    config: Arc<OssConfig>,
}

impl OpenDALOssStorage {
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
        path: &'a (impl AsRef<str> + ?Sized),
    ) -> Result<(Operator, &'a str)> {
        let path = path.as_ref();
        let op = oss_config_build(&self.config, path)?;

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
}

#[async_trait]
impl Storage for OpenDALOssStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let (op, relative_path) = self.create_operator(path)?;
        Ok(op.exists(relative_path).await?)
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let (op, relative_path) = self.create_operator(path)?;
        Ok(op.delete(relative_path).await?)
    }

    async fn remove_dir_all(&self, path: &str) -> Result<()> {
        let (op, relative_path) = self.create_operator(path)?;
        let path = if relative_path.ends_with('/') {
            relative_path.to_string()
        } else {
            format!("{relative_path}/")
        };
        Ok(op.remove_all(&path).await?)
    }

    fn new_input(&self, path: &str) -> Result<InputFileRef> {
        let (op, relative_path) = self.create_operator(path)?;
        let path = path.to_string();
        let relative_path_pos = path.len() - relative_path.len();
        Ok(Arc::new(OpenDALInputFile {
            op,
            path,
            relative_path_pos,
        }))
    }

    fn new_output(&self, path: &str) -> Result<OutputFileRef> {
        let (op, relative_path) = self.create_operator(path)?;
        let path = path.to_string();
        let relative_path_pos = path.len() - relative_path.len();
        Ok(Arc::new(OpenDALOutputFile {
            op,
            path,
            relative_path_pos,
        }))
    }
}

/// Builder for OpenDAL OSS storage
#[derive(Debug, Default)]
pub struct OpenDALOssStorageBuilder;

impl StorageBuilder for OpenDALOssStorageBuilder {
    type S = OpenDALOssStorage;

    fn build(self, props: HashMap<String, String>, _extensions: Extensions) -> Result<Self::S> {
        let cfg = oss_config_parse(props)?;
        Ok(OpenDALOssStorage {
            config: Arc::new(cfg),
        })
    }
}
