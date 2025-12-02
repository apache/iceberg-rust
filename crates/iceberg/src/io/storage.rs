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

//! Storage traits and implementations for Iceberg.
//!
//! This module provides the core storage abstraction used throughout Iceberg Rust.
//! Storage implementations handle reading and writing files across different backends
//! (S3, GCS, Azure, local filesystem, etc.).

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use super::{Extensions, FileMetadata, FileRead, FileWrite, InputFile, OutputFile};
use crate::Result;

/// Trait for storage operations in Iceberg.
///
/// This trait defines the interface for all storage backends. Implementations
/// provide access to different storage systems like S3, GCS, Azure, local filesystem, etc.
///
/// The trait supports serialization via `typetag`, allowing storage instances to be
/// serialized and deserialized across process boundaries.
///
/// # Example
///
/// ```rust,ignore
/// use iceberg::io::Storage;
///
/// async fn example(storage: Arc<dyn Storage>) -> Result<()> {
///     // Check if file exists
///     if storage.exists("s3://bucket/path/file.parquet").await? {
///         // Read file
///         let data = storage.read("s3://bucket/path/file.parquet").await?;
///     }
///     Ok(())
/// }
/// ```
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

/// Common interface for all storage factories.
///
/// Storage factories are responsible for creating storage instances from configuration
/// properties and extensions. Each storage backend (S3, GCS, etc.) provides its own
/// factory implementation.
///
/// The trait supports serialization via `typetag`, allowing factory instances to be
/// serialized and deserialized across process boundaries.
///
/// # Example
///
/// ```rust,ignore
/// use iceberg::io::{StorageFactory, Extensions};
/// use std::collections::HashMap;
///
/// #[derive(Debug, Serialize, Deserialize)]
/// struct MyStorageFactory;
///
/// #[typetag::serde]
/// impl StorageFactory for MyStorageFactory {
///     fn build(
///         &self,
///         props: HashMap<String, String>,
///         extensions: Extensions,
///     ) -> Result<Arc<dyn Storage>> {
///         // Parse configuration and create storage
///         Ok(Arc::new(MyStorage::new(props)?))
///     }
/// }
/// ```
#[typetag::serde(tag = "type")]
pub trait StorageFactory: Debug + Send + Sync {
    /// Create a new storage instance with the given properties and extensions.
    ///
    /// # Arguments
    ///
    /// * `props` - Configuration properties for the storage backend
    /// * `extensions` - Additional extensions (e.g., custom credential loaders)
    ///
    /// # Returns
    ///
    /// An `Arc<dyn Storage>` that can be used for file operations.
    fn build(
        &self,
        props: HashMap<String, String>,
        extensions: Extensions,
    ) -> Result<Arc<dyn Storage>>;
}

/// A registry of storage factories.
///
/// The registry allows you to register custom storage factories for different URI schemes.
/// By default, it includes factories for all enabled storage features.
///
/// # Example
///
/// ```rust
/// use iceberg::io::StorageRegistry;
///
/// // Create a new registry with default factories
/// let registry = StorageRegistry::new();
///
/// // Get supported storage types
/// let types = registry.supported_types();
/// println!("Supported storage types: {:?}", types);
///
/// // Get a factory for a specific scheme
/// # #[cfg(feature = "storage-memory")]
/// # {
/// let factory = registry.get_factory("memory").unwrap();
/// # }
/// ```
///
/// You can also register custom storage factories:
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use iceberg::io::{StorageRegistry, StorageFactory};
///
/// let mut registry = StorageRegistry::new();
///
/// // Register a custom storage factory
/// registry.register("custom", Arc::new(MyCustomStorageFactory));
/// ```
#[derive(Debug, Clone)]
pub struct StorageRegistry {
    factories: HashMap<String, Arc<dyn StorageFactory>>,
}

impl StorageRegistry {
    /// Create a new storage registry with default factories based on enabled features.
    pub fn new() -> Self {
        let mut factories: HashMap<String, Arc<dyn StorageFactory>> = HashMap::new();

        #[cfg(feature = "storage-memory")]
        {
            use crate::io::storage_memory::OpenDALMemoryStorageFactory;
            let factory = Arc::new(OpenDALMemoryStorageFactory) as Arc<dyn StorageFactory>;
            factories.insert("memory".to_string(), factory);
        }

        #[cfg(feature = "storage-fs")]
        {
            use crate::io::storage_fs::OpenDALFsStorageFactory;
            let factory = Arc::new(OpenDALFsStorageFactory) as Arc<dyn StorageFactory>;
            factories.insert("file".to_string(), factory.clone());
            factories.insert("".to_string(), factory);
        }

        #[cfg(feature = "storage-s3")]
        {
            use crate::io::storage_s3::OpenDALS3StorageFactory;
            let factory = Arc::new(OpenDALS3StorageFactory) as Arc<dyn StorageFactory>;
            factories.insert("s3".to_string(), factory.clone());
            factories.insert("s3a".to_string(), factory);
        }

        #[cfg(feature = "storage-gcs")]
        {
            use crate::io::storage_gcs::OpenDALGcsStorageFactory;
            let factory = Arc::new(OpenDALGcsStorageFactory) as Arc<dyn StorageFactory>;
            factories.insert("gs".to_string(), factory.clone());
            factories.insert("gcs".to_string(), factory);
        }

        #[cfg(feature = "storage-oss")]
        {
            use crate::io::storage_oss::OpenDALOssStorageFactory;
            let factory = Arc::new(OpenDALOssStorageFactory) as Arc<dyn StorageFactory>;
            factories.insert("oss".to_string(), factory);
        }

        #[cfg(feature = "storage-azdls")]
        {
            use crate::io::storage_azdls::OpenDALAzdlsStorageFactory;
            let factory = Arc::new(OpenDALAzdlsStorageFactory) as Arc<dyn StorageFactory>;
            factories.insert("abfs".to_string(), factory.clone());
            factories.insert("abfss".to_string(), factory.clone());
            factories.insert("wasb".to_string(), factory.clone());
            factories.insert("wasbs".to_string(), factory);
        }

        Self { factories }
    }

    /// Register a custom storage factory for a given scheme.
    pub fn register(&mut self, scheme: impl Into<String>, factory: Arc<dyn StorageFactory>) {
        self.factories.insert(scheme.into(), factory);
    }

    /// Get a storage factory by scheme.
    pub fn get_factory(&self, scheme: &str) -> Result<Arc<dyn StorageFactory>> {
        let key = scheme.trim();
        self.factories
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(key))
            .map(|(_, factory)| factory.clone())
            .ok_or_else(|| {
                use crate::{Error, ErrorKind};
                Error::new(
                    ErrorKind::FeatureUnsupported,
                    format!(
                        "Unsupported storage type: {}. Supported types: {}",
                        scheme,
                        self.supported_types().join(", ")
                    ),
                )
            })
    }

    /// Return the list of supported storage types.
    pub fn supported_types(&self) -> Vec<String> {
        self.factories.keys().cloned().collect()
    }
}

impl Default for StorageRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_registry_new() {
        let registry = StorageRegistry::new();
        let types = registry.supported_types();

        // At least one storage type should be available
        assert!(!types.is_empty());
    }

    #[test]
    #[cfg(feature = "storage-memory")]
    fn test_storage_registry_get_factory() {
        let registry = StorageRegistry::new();

        // Should be able to get memory storage factory
        let factory = registry.get_factory("memory");
        assert!(factory.is_ok());

        // Should be case-insensitive
        let factory = registry.get_factory("MEMORY");
        assert!(factory.is_ok());
    }

    #[test]
    fn test_storage_registry_unsupported_type() {
        let registry = StorageRegistry::new();

        // Should return error for unsupported type
        let result = registry.get_factory("unsupported");
        assert!(result.is_err());
    }

    #[test]
    #[cfg(feature = "storage-memory")]
    fn test_storage_registry_clone() {
        let registry = StorageRegistry::new();
        let cloned = registry.clone();

        // Both should have the same factories
        assert_eq!(
            registry.supported_types().len(),
            cloned.supported_types().len()
        );
    }
}
