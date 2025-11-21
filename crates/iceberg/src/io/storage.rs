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

    /// Remove a directory and all its contents recursively
    async fn remove_dir_all(&self, path: &str) -> Result<()>;

    /// Create a new input file for reading
    fn new_input(&self, path: &str) -> Result<InputFile>;

    /// Create a new output file for writing
    fn new_output(&self, path: &str) -> Result<OutputFile>;
}

/// Common interface for all storage builders.
///
/// Storage builders are responsible for creating storage instances from configuration
/// properties and extensions. Each storage backend (S3, GCS, etc.) provides its own
/// builder implementation.
///
/// # Example
///
/// ```rust,ignore
/// use iceberg::io::{StorageBuilder, Extensions};
/// use std::collections::HashMap;
///
/// struct MyStorageBuilder;
///
/// impl StorageBuilder for MyStorageBuilder {
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
pub trait StorageBuilder: Debug + Send + Sync {
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

/// A registry of storage builders.
///
/// The registry allows you to register custom storage builders for different URI schemes.
/// By default, it includes builders for all enabled storage features.
///
/// # Example
///
/// ```rust
/// use iceberg::io::StorageBuilderRegistry;
///
/// // Create a new registry with default builders
/// let registry = StorageBuilderRegistry::new();
///
/// // Get supported storage types
/// let types = registry.supported_types();
/// println!("Supported storage types: {:?}", types);
///
/// // Get a builder for a specific scheme
/// # #[cfg(feature = "storage-memory")]
/// # {
/// let builder = registry.get_builder("memory").unwrap();
/// # }
/// ```
///
/// You can also register custom storage builders:
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use iceberg::io::{StorageBuilderRegistry, StorageBuilder};
///
/// let mut registry = StorageBuilderRegistry::new();
///
/// // Register a custom storage builder
/// registry.register("custom", Arc::new(MyCustomStorageBuilder));
/// ```
#[derive(Debug, Clone)]
pub struct StorageBuilderRegistry {
    builders: HashMap<String, Arc<dyn StorageBuilder>>,
}

impl StorageBuilderRegistry {
    /// Create a new storage registry with default builders based on enabled features.
    pub fn new() -> Self {
        let mut builders: HashMap<String, Arc<dyn StorageBuilder>> = HashMap::new();

        #[cfg(feature = "storage-memory")]
        {
            use crate::io::storage_memory::OpenDALMemoryStorageBuilder;
            let builder = Arc::new(OpenDALMemoryStorageBuilder) as Arc<dyn StorageBuilder>;
            builders.insert("memory".to_string(), builder);
        }

        #[cfg(feature = "storage-fs")]
        {
            use crate::io::storage_fs::OpenDALFsStorageBuilder;
            let builder = Arc::new(OpenDALFsStorageBuilder) as Arc<dyn StorageBuilder>;
            builders.insert("file".to_string(), builder.clone());
            builders.insert("".to_string(), builder);
        }

        #[cfg(feature = "storage-s3")]
        {
            use crate::io::storage_s3::OpenDALS3StorageBuilder;
            let builder = Arc::new(OpenDALS3StorageBuilder) as Arc<dyn StorageBuilder>;
            builders.insert("s3".to_string(), builder.clone());
            builders.insert("s3a".to_string(), builder);
        }

        #[cfg(feature = "storage-gcs")]
        {
            use crate::io::storage_gcs::OpenDALGcsStorageBuilder;
            let builder = Arc::new(OpenDALGcsStorageBuilder) as Arc<dyn StorageBuilder>;
            builders.insert("gs".to_string(), builder.clone());
            builders.insert("gcs".to_string(), builder);
        }

        #[cfg(feature = "storage-oss")]
        {
            use crate::io::storage_oss::OpenDALOssStorageBuilder;
            let builder = Arc::new(OpenDALOssStorageBuilder) as Arc<dyn StorageBuilder>;
            builders.insert("oss".to_string(), builder);
        }

        #[cfg(feature = "storage-azdls")]
        {
            use crate::io::storage_azdls::OpenDALAzdlsStorageBuilder;
            let builder = Arc::new(OpenDALAzdlsStorageBuilder) as Arc<dyn StorageBuilder>;
            builders.insert("abfs".to_string(), builder.clone());
            builders.insert("abfss".to_string(), builder.clone());
            builders.insert("wasb".to_string(), builder.clone());
            builders.insert("wasbs".to_string(), builder);
        }

        Self { builders }
    }

    /// Register a custom storage builder for a given scheme.
    pub fn register(&mut self, scheme: impl Into<String>, builder: Arc<dyn StorageBuilder>) {
        self.builders.insert(scheme.into(), builder);
    }

    /// Get a storage builder by scheme.
    pub fn get_builder(&self, scheme: &str) -> Result<Arc<dyn StorageBuilder>> {
        let key = scheme.trim();
        self.builders
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(key))
            .map(|(_, builder)| builder.clone())
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
        self.builders.keys().cloned().collect()
    }
}

impl Default for StorageBuilderRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_builder_registry_new() {
        let registry = StorageBuilderRegistry::new();
        let types = registry.supported_types();

        // At least one storage type should be available
        assert!(!types.is_empty());
    }

    #[test]
    #[cfg(feature = "storage-memory")]
    fn test_storage_builder_registry_get_builder() {
        let registry = StorageBuilderRegistry::new();

        // Should be able to get memory storage builder
        let builder = registry.get_builder("memory");
        assert!(builder.is_ok());

        // Should be case-insensitive
        let builder = registry.get_builder("MEMORY");
        assert!(builder.is_ok());
    }

    #[test]
    fn test_storage_builder_registry_unsupported_type() {
        let registry = StorageBuilderRegistry::new();

        // Should return error for unsupported type
        let result = registry.get_builder("unsupported");
        assert!(result.is_err());
    }

    #[test]
    #[cfg(feature = "storage-memory")]
    fn test_storage_builder_registry_clone() {
        let registry = StorageBuilderRegistry::new();
        let cloned = registry.clone();

        // Both should have the same builders
        assert_eq!(
            registry.supported_types().len(),
            cloned.supported_types().len()
        );
    }
}
