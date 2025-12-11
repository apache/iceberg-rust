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
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use opendal::Operator;
use opendal::services::MemoryConfig;
use serde::{Deserialize, Serialize};

use crate::Result;
use crate::io::{
    Extensions, FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageFactory,
};

/// Memory storage implementation using OpenDAL
///
/// Uses lazy initialization - the operator is created on first use and then cached.
/// This allows the storage to be serialized/deserialized while maintaining state.
/// The operator field is skipped during serialization and recreated on first use.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenDALMemoryStorage {
    #[serde(skip, default = "default_op")]
    op: Arc<Mutex<Option<Operator>>>,
}

fn default_op() -> Arc<Mutex<Option<Operator>>> {
    Arc::new(Mutex::new(None))
}

impl Default for OpenDALMemoryStorage {
    fn default() -> Self {
        Self {
            op: Arc::new(Mutex::new(None)),
        }
    }
}

impl OpenDALMemoryStorage {
    /// Get or create the memory operator (lazy initialization)
    fn get_operator(&self) -> Result<Operator> {
        let mut guard = self.op.lock().unwrap();
        if guard.is_none() {
            *guard = Some(Operator::from_config(MemoryConfig::default())?.finish());
        }
        Ok(guard.as_ref().unwrap().clone())
    }

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
#[typetag::serde]
impl Storage for OpenDALMemoryStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let op = self.get_operator()?;
        let relative_path = self.extract_relative_path(path);
        Ok(op.exists(relative_path).await?)
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let op = self.get_operator()?;
        let relative_path = self.extract_relative_path(path);
        let meta = op.stat(relative_path).await?;
        Ok(FileMetadata {
            size: meta.content_length(),
        })
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let op = self.get_operator()?;
        let relative_path = self.extract_relative_path(path);
        Ok(op.read(relative_path).await?.to_bytes())
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let op = self.get_operator()?;
        let relative_path = self.extract_relative_path(path);
        Ok(Box::new(op.reader(relative_path).await?))
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        let mut writer = self.writer(path).await?;
        writer.write(bs).await?;
        writer.close().await
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let op = self.get_operator()?;
        let relative_path = self.extract_relative_path(path);
        Ok(Box::new(op.writer(relative_path).await?))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let op = self.get_operator()?;
        let relative_path = self.extract_relative_path(path);
        Ok(op.delete(relative_path).await?)
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let op = self.get_operator()?;
        let relative_path = self.extract_relative_path(path);
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

/// Factory for OpenDAL Memory storage
#[derive(Debug, Serialize, Deserialize)]
pub struct OpenDALMemoryStorageFactory;

#[typetag::serde]
impl StorageFactory for OpenDALMemoryStorageFactory {
    fn build(
        &self,
        _props: HashMap<String, String>,
        _extensions: Extensions,
    ) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(OpenDALMemoryStorage::default()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::Storage;

    #[test]
    fn test_memory_storage_serialization() {
        // Create a memory storage instance using the factory
        let factory = OpenDALMemoryStorageFactory;
        let storage = factory
            .build(HashMap::new(), Extensions::default())
            .unwrap();

        // Serialize the storage
        let serialized = serde_json::to_string(&storage).unwrap();

        // Deserialize the storage
        let deserialized: Box<dyn Storage> = serde_json::from_str(&serialized).unwrap();

        // Verify the type is correct
        assert!(format!("{deserialized:?}").contains("OpenDALMemoryStorage"));
    }

    #[test]
    fn test_memory_factory_serialization() {
        use crate::io::StorageFactory;

        // Create a factory instance
        let factory: Box<dyn StorageFactory> = Box::new(OpenDALMemoryStorageFactory);

        // Serialize the factory
        let serialized = serde_json::to_string(&factory).unwrap();

        // Deserialize the factory
        let deserialized: Box<dyn StorageFactory> = serde_json::from_str(&serialized).unwrap();

        // Verify the type is correct
        assert!(format!("{deserialized:?}").contains("OpenDALMemoryStorageFactory"));
    }

    #[test]
    fn test_memory_factory_to_storage_serialization() {
        use crate::io::{Extensions, StorageFactory};

        // Create a factory and build storage
        let factory = OpenDALMemoryStorageFactory;
        let storage = factory
            .build(HashMap::new(), Extensions::default())
            .unwrap();

        // Serialize the storage
        let storage_json = serde_json::to_string(&storage).unwrap();

        // Deserialize the storage
        let deserialized_storage: Box<dyn Storage> = serde_json::from_str(&storage_json).unwrap();

        // Verify storage type
        assert!(format!("{deserialized_storage:?}").contains("OpenDALMemoryStorage"));

        // Serialize the factory
        let factory_boxed: Box<dyn StorageFactory> = Box::new(factory);
        let factory_json = serde_json::to_string(&factory_boxed).unwrap();

        // Deserialize the factory
        let deserialized_factory: Box<dyn StorageFactory> =
            serde_json::from_str(&factory_json).unwrap();

        // Verify factory type
        assert!(format!("{deserialized_factory:?}").contains("OpenDALMemoryStorageFactory"));
    }
}
