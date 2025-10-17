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
use opendal::Operator;
use opendal::services::MemoryConfig;

use crate::Result;
use crate::io::{
    Extensions, FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageBuilder,
};

/// Memory storage implementation using OpenDAL
#[derive(Debug, Clone)]
pub struct OpenDALMemoryStorage {
    op: Operator,
}

impl OpenDALMemoryStorage {
    /// Extract relative path from memory:// URLs
    fn extract_relative_path<'a>(&self, path: &'a str) -> &'a str {
        if let Some(stripped) = path.strip_prefix("memory:/") {
            stripped
        } else {
            &path[1..]
        }
    }
}

#[async_trait]
impl Storage for OpenDALMemoryStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let relative_path = self.extract_relative_path(path);
        Ok(self.op.exists(relative_path).await?)
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let relative_path = self.extract_relative_path(path);
        let meta = self.op.stat(relative_path).await?;
        Ok(FileMetadata {
            size: meta.content_length(),
        })
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let relative_path = self.extract_relative_path(path);
        Ok(self.op.read(relative_path).await?.to_bytes())
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let relative_path = self.extract_relative_path(path);
        Ok(Box::new(self.op.reader(relative_path).await?))
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        let mut writer = self.writer(path).await?;
        writer.write(bs).await?;
        writer.close().await
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let relative_path = self.extract_relative_path(path);
        Ok(Box::new(self.op.writer(relative_path).await?))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let relative_path = self.extract_relative_path(path);
        Ok(self.op.delete(relative_path).await?)
    }

    async fn remove_dir_all(&self, path: &str) -> Result<()> {
        let relative_path = self.extract_relative_path(path);
        let path = if relative_path.ends_with('/') {
            relative_path.to_string()
        } else {
            format!("{relative_path}/")
        };
        Ok(self.op.remove_all(&path).await?)
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

/// Builder for OpenDAL Memory storage
#[derive(Debug)]
pub struct OpenDALMemoryStorageBuilder;

impl StorageBuilder for OpenDALMemoryStorageBuilder {
    fn build(
        &self,
        _props: HashMap<String, String>,
        _extensions: Extensions,
    ) -> Result<Arc<dyn Storage>> {
        let op = Operator::from_config(MemoryConfig::default())?.finish();
        Ok(Arc::new(OpenDALMemoryStorage { op }))
    }
}
