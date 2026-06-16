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

// TODO Add specific configs
//! Storage configuration for storage backends.
//!
//! This module provides configuration types for various storage backends.
//! The configuration types are designed to be used with the `StorageFactory`
//! trait to create storage instances.
//!
//! # Available Configurations
//!
//! - [`StorageConfig`]: Base configuration containing properties for storage backends
//! - [`S3Config`]: Amazon S3 specific configuration
//! - [`GcsConfig`]: Google Cloud Storage specific configuration
//! - [`OssConfig`]: Alibaba Cloud OSS specific configuration
//! - [`AzdlsConfig`]: Azure Data Lake Storage specific configuration

mod azdls;
mod gcs;
mod hf;
mod oss;
mod s3;

use std::collections::HashMap;

pub use azdls::*;
pub use gcs::*;
pub use hf::*;
pub use oss::*;
pub use s3::*;
use serde::{Deserialize, Serialize};

/// A storage credential scoped to a location prefix.
///
/// This mirrors the `storage-credentials` entries returned by an Iceberg REST
/// catalog (see the REST spec). Each credential applies to objects whose path
/// starts with `prefix`; when several credentials match a path, the one with
/// the longest prefix should be preferred.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct StorageCredential {
    /// Storage location prefix this credential applies to (e.g. `s3://bucket/db/table`).
    pub prefix: String,
    /// Backend configuration carrying the credential (e.g. `s3.access-key-id`, ...).
    pub config: HashMap<String, String>,
}

impl StorageCredential {
    /// Create a new storage credential for the given location prefix.
    pub fn new(prefix: impl Into<String>, config: HashMap<String, String>) -> Self {
        Self {
            prefix: prefix.into(),
            config,
        }
    }
}

/// Configuration properties for storage backends.
///
/// This struct contains only configuration properties without specifying
/// which storage backend to use. The storage type is determined by the
/// explicit factory selection.
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct StorageConfig {
    /// Configuration properties for the storage backend
    props: HashMap<String, String>,
    /// Per-prefix storage credentials, typically vended by a catalog.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    credentials: Vec<StorageCredential>,
}

impl StorageConfig {
    /// Create a new empty StorageConfig.
    pub fn new() -> Self {
        Self {
            props: HashMap::new(),
            credentials: Vec::new(),
        }
    }

    /// Create a StorageConfig from existing properties.
    ///
    /// # Arguments
    ///
    /// * `props` - Configuration properties for the storage backend
    pub fn from_props(props: HashMap<String, String>) -> Self {
        Self {
            props,
            credentials: Vec::new(),
        }
    }

    /// Get all configuration properties.
    pub fn props(&self) -> &HashMap<String, String> {
        &self.props
    }

    /// Get the per-prefix storage credentials.
    pub fn credentials(&self) -> &[StorageCredential] {
        &self.credentials
    }

    /// Attach per-prefix storage credentials (e.g. vended by a REST catalog).
    ///
    /// This is a builder-style method that returns `self` for chaining.
    pub fn with_credentials(mut self, credentials: Vec<StorageCredential>) -> Self {
        self.credentials = credentials;
        self
    }

    /// Resolve the most specific credential for `location`.
    ///
    /// Returns the credential whose `prefix` is the longest match for the given
    /// location, mirroring the selection rule from the Iceberg REST spec. Returns
    /// `None` when no credential prefix matches.
    pub fn resolve_credential(&self, location: &str) -> Option<&StorageCredential> {
        self.credentials
            .iter()
            .filter(|c| location.starts_with(&c.prefix))
            .max_by_key(|c| c.prefix.len())
    }

    /// Build the effective property map for `location`: base props overlaid with
    /// the most specific matching credential's config (credential wins, per spec).
    pub fn resolved_props(&self, location: &str) -> HashMap<String, String> {
        let mut props = self.props.clone();
        if let Some(cred) = self.resolve_credential(location) {
            props.extend(cred.config.clone());
        }
        props
    }

    /// Get a specific configuration property by key.
    ///
    /// # Arguments
    ///
    /// * `key` - The property key to look up
    ///
    /// # Returns
    ///
    /// An `Option` containing a reference to the property value if it exists.
    pub fn get(&self, key: &str) -> Option<&String> {
        self.props.get(key)
    }

    /// Add a configuration property.
    ///
    /// This is a builder-style method that returns `self` for chaining.
    ///
    /// # Arguments
    ///
    /// * `key` - The property key
    /// * `value` - The property value
    pub fn with_prop(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.props.insert(key.into(), value.into());
        self
    }

    /// Add multiple configuration properties.
    ///
    /// This is a builder-style method that returns `self` for chaining.
    ///
    /// # Arguments
    ///
    /// * `props` - An iterator of key-value pairs to add
    pub fn with_props(
        mut self,
        props: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        self.props
            .extend(props.into_iter().map(|(k, v)| (k.into(), v.into())));
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_config_new() {
        let config = StorageConfig::new();

        assert!(config.props().is_empty());
    }

    #[test]
    fn test_storage_config_from_props() {
        let props = HashMap::from([
            ("region".to_string(), "us-east-1".to_string()),
            ("bucket".to_string(), "my-bucket".to_string()),
        ]);
        let config = StorageConfig::from_props(props.clone());

        assert_eq!(config.props(), &props);
    }

    #[test]
    fn test_storage_config_default() {
        let config = StorageConfig::default();

        assert!(config.props().is_empty());
    }

    #[test]
    fn test_storage_config_get() {
        let config = StorageConfig::new().with_prop("region", "us-east-1");

        assert_eq!(config.get("region"), Some(&"us-east-1".to_string()));
        assert_eq!(config.get("nonexistent"), None);
    }

    #[test]
    fn test_storage_config_with_prop() {
        let config = StorageConfig::new()
            .with_prop("region", "us-east-1")
            .with_prop("bucket", "my-bucket");

        assert_eq!(config.get("region"), Some(&"us-east-1".to_string()));
        assert_eq!(config.get("bucket"), Some(&"my-bucket".to_string()));
    }

    #[test]
    fn test_storage_config_with_props() {
        let additional_props = vec![("key1", "value1"), ("key2", "value2")];
        let config = StorageConfig::new().with_props(additional_props);

        assert_eq!(config.get("key1"), Some(&"value1".to_string()));
        assert_eq!(config.get("key2"), Some(&"value2".to_string()));
    }

    #[test]
    fn test_storage_config_clone() {
        let config = StorageConfig::new().with_prop("region", "us-east-1");
        let cloned = config.clone();

        assert_eq!(config, cloned);
        assert_eq!(cloned.get("region"), Some(&"us-east-1".to_string()));
    }

    #[test]
    fn test_storage_config_serialization_roundtrip() {
        let config = StorageConfig::new()
            .with_prop("region", "us-east-1")
            .with_prop("bucket", "my-bucket");

        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: StorageConfig = serde_json::from_str(&serialized).unwrap();

        assert_eq!(config, deserialized);
    }

    #[test]
    fn test_storage_config_clone_independence() {
        let original = StorageConfig::new().with_prop("region", "us-east-1");
        let mut cloned = original.clone();

        // Modify the clone
        cloned = cloned.with_prop("region", "eu-west-1");
        cloned = cloned.with_prop("new_key", "new_value");

        // Original should be unchanged
        assert_eq!(original.get("region"), Some(&"us-east-1".to_string()));
        assert_eq!(original.get("new_key"), None);

        // Clone should have the new values
        assert_eq!(cloned.get("region"), Some(&"eu-west-1".to_string()));
        assert_eq!(cloned.get("new_key"), Some(&"new_value".to_string()));
    }

    #[test]
    fn test_storage_config_from_props_empty() {
        let config = StorageConfig::from_props(HashMap::new());

        assert!(config.props().is_empty());
    }

    #[test]
    fn test_resolve_credential_longest_prefix_wins() {
        let config = StorageConfig::new().with_credentials(vec![
            StorageCredential::new(
                "s3://bucket",
                HashMap::from([("s3.access-key-id".to_string(), "broad".to_string())]),
            ),
            StorageCredential::new(
                "s3://bucket/db/table",
                HashMap::from([("s3.access-key-id".to_string(), "specific".to_string())]),
            ),
        ]);

        let cred = config
            .resolve_credential("s3://bucket/db/table/data/0.parquet")
            .expect("a credential should match");
        assert_eq!(cred.prefix, "s3://bucket/db/table");
        assert_eq!(cred.config.get("s3.access-key-id").unwrap(), "specific");

        // A path only under the broad prefix falls back to it.
        let cred = config
            .resolve_credential("s3://bucket/other/file.parquet")
            .unwrap();
        assert_eq!(cred.prefix, "s3://bucket");
    }

    #[test]
    fn test_resolve_credential_no_match() {
        let config = StorageConfig::new().with_credentials(vec![StorageCredential::new(
            "s3://bucket-a",
            HashMap::from([("s3.access-key-id".to_string(), "a".to_string())]),
        )]);

        assert!(config.resolve_credential("s3://bucket-b/x").is_none());
    }

    #[test]
    fn test_resolved_props_credential_overrides_base() {
        let config = StorageConfig::new()
            .with_prop("s3.region", "us-east-1")
            .with_prop("s3.access-key-id", "base")
            .with_credentials(vec![StorageCredential::new(
                "s3://bucket",
                HashMap::from([("s3.access-key-id".to_string(), "vended".to_string())]),
            )]);

        let props = config.resolved_props("s3://bucket/db/table/data/0.parquet");
        // Credential wins over base for the same key, base-only keys are preserved.
        assert_eq!(props.get("s3.access-key-id").unwrap(), "vended");
        assert_eq!(props.get("s3.region").unwrap(), "us-east-1");

        // No matching credential: base props returned unchanged.
        let props = config.resolved_props("gs://other/file");
        assert_eq!(props.get("s3.access-key-id").unwrap(), "base");
    }

    #[test]
    fn test_storage_config_serialization_empty() {
        let config = StorageConfig::new();

        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: StorageConfig = serde_json::from_str(&serialized).unwrap();

        assert_eq!(config, deserialized);
        assert!(deserialized.props().is_empty());
    }
}
