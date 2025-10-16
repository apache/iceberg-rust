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

// todo move this to a new crate
/// todo doc: mimic storage builder
use std::collections::HashMap;
use std::sync::Arc;

use crate::io::StorageBuilder;
#[cfg(feature = "storage-azdls")]
use crate::io::storage_azdls::OpenDALAzdlsStorageBuilder;
#[cfg(feature = "storage-fs")]
use crate::io::storage_fs::OpenDALFsStorageBuilder;
#[cfg(feature = "storage-gcs")]
use crate::io::storage_gcs::OpenDALGcsStorageBuilder;
#[cfg(feature = "storage-memory")]
use crate::io::storage_memory::OpenDALMemoryStorageBuilder;
#[cfg(feature = "storage-oss")]
use crate::io::storage_oss::OpenDALOssStorageBuilder;
#[cfg(feature = "storage-s3")]
use crate::io::storage_s3::OpenDALS3StorageBuilder;
use crate::{Error, ErrorKind, Result};

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
            let builder = Arc::new(OpenDALMemoryStorageBuilder) as Arc<dyn StorageBuilder>;
            builders.insert("memory".to_string(), builder);
        }

        #[cfg(feature = "storage-fs")]
        {
            let builder = Arc::new(OpenDALFsStorageBuilder) as Arc<dyn StorageBuilder>;
            builders.insert("file".to_string(), builder.clone());
            builders.insert("".to_string(), builder);
        }

        #[cfg(feature = "storage-s3")]
        {
            let builder = Arc::new(OpenDALS3StorageBuilder) as Arc<dyn StorageBuilder>;
            builders.insert("s3".to_string(), builder.clone());
            builders.insert("s3a".to_string(), builder);
        }

        #[cfg(feature = "storage-gcs")]
        {
            let builder = Arc::new(OpenDALGcsStorageBuilder) as Arc<dyn StorageBuilder>;
            builders.insert("gs".to_string(), builder.clone());
            builders.insert("gcs".to_string(), builder);
        }

        #[cfg(feature = "storage-oss")]
        {
            let builder = Arc::new(OpenDALOssStorageBuilder) as Arc<dyn StorageBuilder>;
            builders.insert("oss".to_string(), builder);
        }

        #[cfg(feature = "storage-azdls")]
        {
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

// Global default registry for backward compatibility
static DEFAULT_REGISTRY: std::sync::OnceLock<StorageBuilderRegistry> = std::sync::OnceLock::new();

fn get_default_registry() -> &'static StorageBuilderRegistry {
    DEFAULT_REGISTRY.get_or_init(StorageBuilderRegistry::new)
}

/// Return the list of supported storage types from the default registry.
#[allow(dead_code)]
pub fn supported_types() -> Vec<String> {
    get_default_registry().supported_types()
}

/// Create a storage builder by storage type.
pub fn create_storage_builder(r#type: &str) -> Result<Arc<dyn StorageBuilder>> {
    get_default_registry().get_builder(r#type)
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
