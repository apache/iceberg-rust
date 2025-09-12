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
use bytes::Bytes;
use opendal::services::OssConfig;
use opendal::{Configurator, Operator};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::io::{
    Extensions, FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageFactory,
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
            format!("Invalid oss url: {path}, missing bucket"),
        )
    })?;

    let builder = cfg.clone().into_builder().bucket(bucket);

    Ok(Operator::new(builder)?.finish())
}

/// OSS storage implementation using OpenDAL
///
/// Stores configuration and creates operators on-demand.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenDALOssStorage {
    config: Arc<OssConfig>,
}

impl OpenDALOssStorage {
    /// Creates operator from path.
    fn create_operator<'a>(&self, path: &'a str) -> Result<(Operator, &'a str)> {
        let op = oss_config_build(&self.config, path)?;
        let prefix = format!("oss://{}/", op.info().name());

        if path.starts_with(&prefix) {
            let op = op.layer(opendal::layers::RetryLayer::new());
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
#[typetag::serde]
impl Storage for OpenDALOssStorage {
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

/// Factory for OpenDAL OSS storage
#[derive(Debug, Serialize, Deserialize)]
pub struct OpenDALOssStorageFactory;

#[typetag::serde]
impl StorageFactory for OpenDALOssStorageFactory {
    fn build(
        &self,
        props: HashMap<String, String>,
        _extensions: Extensions,
    ) -> Result<Arc<dyn Storage>> {
        let cfg = oss_config_parse(props)?;
        Ok(Arc::new(OpenDALOssStorage {
            config: Arc::new(cfg),
        }))
    }
}
