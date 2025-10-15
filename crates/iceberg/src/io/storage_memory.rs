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
use opendal::Operator;
use opendal::services::MemoryConfig;

use crate::Result;
use crate::io::{
    Extensions, InputFileRef, OpenDALInputFile, OpenDALOutputFile, OutputFileRef, Storage,
    StorageBuilder,
};

/// Memory storage implementation using OpenDAL
#[derive(Debug)]
pub struct OpenDALMemoryStorage {
    op: Operator,
}

impl OpenDALMemoryStorage {
    /// Extract relative path from memory:// URLs
    fn extract_relative_path<'a>(&self, path: &'a str) -> Result<&'a str> {
        if let Some(stripped) = path.strip_prefix("memory:/") {
            Ok(stripped)
        } else {
            Ok(&path[1..])
        }
    }
}

#[async_trait]
impl Storage for OpenDALMemoryStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let relative_path = self.extract_relative_path(path)?;
        Ok(self.op.exists(relative_path).await?)
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let relative_path = self.extract_relative_path(path)?;
        Ok(self.op.delete(relative_path).await?)
    }

    async fn remove_dir_all(&self, path: &str) -> Result<()> {
        let relative_path = self.extract_relative_path(path)?;
        let path = if relative_path.ends_with('/') {
            relative_path.to_string()
        } else {
            format!("{relative_path}/")
        };
        Ok(self.op.remove_all(&path).await?)
    }

    fn new_input(&self, path: &str) -> Result<InputFileRef> {
        let relative_path = self.extract_relative_path(path)?;
        let path = path.to_string();
        let relative_path_pos = path.len() - relative_path.len();
        Ok(Arc::new(OpenDALInputFile {
            op: self.op.clone(),
            path,
            relative_path_pos,
        }))
    }

    fn new_output(&self, path: &str) -> Result<OutputFileRef> {
        let relative_path = self.extract_relative_path(path)?;
        let path = path.to_string();
        let relative_path_pos = path.len() - relative_path.len();
        Ok(Arc::new(OpenDALOutputFile {
            op: self.op.clone(),
            path,
            relative_path_pos,
        }))
    }
}

/// Builder for OpenDAL Memory storage
#[derive(Debug, Default)]
pub struct OpenDALMemoryStorageBuilder;

impl StorageBuilder for OpenDALMemoryStorageBuilder {
    type S = OpenDALMemoryStorage;

    fn build(self, _props: HashMap<String, String>, _extensions: Extensions) -> Result<Self::S> {
        let op = Operator::from_config(MemoryConfig::default())?.finish();
        Ok(OpenDALMemoryStorage { op })
    }
}
