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

//! Storage interfaces for Iceberg.

mod config;
mod local_fs;
mod memory;

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Once};

use async_trait::async_trait;
use bytes::Bytes;
pub use config::*;
use futures::stream::BoxStream;
pub use local_fs::{LocalFsStorage, LocalFsStorageFactory};
pub use memory::{MemoryStorage, MemoryStorageFactory};

use super::{FileMetadata, FileRead, FileWrite, InputFile, OutputFile};
use crate::{Error, ErrorKind, Result};

/// Trait for storage operations in Iceberg.
///
/// The trait supports serialization via `typetag`, allowing storage instances to be
/// serialized and deserialized across process boundaries.
///
/// Third-party implementations can implement this trait to provide custom storage backends.
///
/// # Implementing Custom Storage
///
/// To implement a custom storage backend:
///
/// 1. Create a struct that implements this trait
/// 2. Add `#[typetag::serde]` attribute for serialization support
/// 3. Implement all required methods
///
/// # Example
///
/// ```rust,ignore
/// #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
/// struct MyStorage {
///     // custom fields
/// }
///
/// #[async_trait]
/// #[typetag::serde]
/// impl Storage for MyStorage {
///     async fn exists(&self, path: &str) -> Result<bool> {
///         // implementation
///         todo!()
///     }
///     // ... implement other methods
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

    /// Delete multiple files from a stream of paths.
    async fn delete_stream(&self, paths: BoxStream<'static, String>) -> Result<()>;

    /// Create a new input file for reading
    fn new_input(&self, path: &str) -> Result<InputFile>;

    /// Create a new output file for writing
    fn new_output(&self, path: &str) -> Result<OutputFile>;
}

/// Factory for creating Storage instances from configuration.
///
/// Implement this trait to provide custom storage backends. The factory pattern
/// allows for lazy initialization of storage instances and enables users to
/// inject custom storage implementations into catalogs.
///
/// # Example
///
/// ```rust,ignore
/// #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
/// struct MyCustomStorageFactory {
///     // custom configuration
/// }
///
/// #[typetag::serde]
/// impl StorageFactory for MyCustomStorageFactory {
///     fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
///         // Create and return custom storage implementation
///         todo!()
///     }
/// }
/// ```
#[typetag::serde(tag = "type")]
pub trait StorageFactory: Debug + Send + Sync {
    /// Build a new Storage instance from the given configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - The storage configuration containing scheme and properties
    ///
    /// # Returns
    ///
    /// A `Result` containing an `Arc<dyn Storage>` on success, or an error
    /// if the storage could not be created.
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>>;

    /// Build a new Storage instance, optionally supplying a credential provider
    /// that the backend can call to obtain and refresh short-lived credentials.
    ///
    /// Backends that cannot use the provider ignore it and use the credentials
    /// in `config`, as they would without one. The default does so and logs a
    /// warning once, since the storage's credentials are then not refreshed.
    /// Factories that wrap another factory should forward the provider to it,
    /// and factories whose storage needs no credentials can ignore it silently.
    fn build_with_credential_provider(
        &self,
        config: &StorageConfig,
        credential_provider: Option<Arc<dyn StorageCredentialProvider>>,
    ) -> Result<Arc<dyn Storage>> {
        if credential_provider.is_some() {
            static WARNED: Once = Once::new();
            WARNED.call_once(|| {
                tracing::warn!(
                    "{} ignores storage credential providers; its storage uses the credentials \
                     in its configuration, which are not refreshed",
                    std::any::type_name::<Self>()
                )
            });
        }
        self.build(config)
    }
}

/// Supplies fresh, backend-specific storage credentials on demand.
///
/// A catalog that vends temporary credentials implements this trait so that
/// storage backends can re-fetch credentials as they approach expiry instead
/// of failing once the initial token's TTL runs out.
///
/// # Debug output
///
/// [`FileIO`](crate::io::FileIO) and storage implementations print the
/// provider in their `Debug` output, so the provider's `Debug` implementation
/// must not expose credentials or other secrets.
///
/// # Caching
///
/// [`load_credential`](Self::load_credential) may be called very frequently.
/// Implementations must cache internally and only re-fetch when the current
/// credential is at or near expiry; otherwise every object-store request could
/// trigger a call back to the catalog.
///
/// Backends may treat a credential as stale before it expires, and load
/// another on every request until they get a fresher one. The OpenDAL S3 and
/// GCS backends do so within two minutes of expiry, and reject a newly loaded
/// credential that expires within ten seconds. Implementations should
/// therefore refresh early enough that the credential they return has more
/// than two minutes left.
#[async_trait]
pub trait StorageCredentialProvider: Debug + Send + Sync {
    /// Return whether this provider supplies credentials for `path`.
    ///
    /// Backends replace their own credential chain with the provider only for
    /// paths it supports, so a provider must return `false` for the storage it
    /// does not serve, such as other schemes behind a resolving storage.
    fn supports_path(&self, path: &str) -> bool;

    /// Load a fresh credential for the storage location identified by `path`.
    ///
    /// `path` is an absolute location: usually the file being accessed, such
    /// as `s3://bucket/warehouse/db/table/data/file.parquet`, but for a bulk
    /// delete the location shared by a batch, which may be a storage root
    /// (`s3://bucket/`) or the prefix of a previously returned credential
    /// (`s3://bucket/warehouse/db/table`). Providers that vend distinct
    /// credentials per location prefix use it to select the most specific
    /// match, which must [cover](StorageCredential::covers) `path`.
    ///
    /// Backends read the credential's expiry from its config, such as
    /// `s3.session-token-expires-at-ms`, and load a new one before it expires.
    /// A credential without an expiry is used for as long as the backend lives.
    async fn load_credential(&self, path: &str) -> Result<StorageCredential>;

    /// Return a factory that rebuilds an equivalent provider in another process.
    ///
    /// [`FileIO::serialize_all`](crate::io::FileIO::serialize_all) serializes this
    /// factory in place of the provider. On an error, which the default always
    /// returns, it serializes the `FileIO` without the provider and logs a
    /// warning.
    fn factory(&self) -> Result<Arc<dyn StorageCredentialProviderFactory>> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "storage credential provider cannot be serialized",
        ))
    }
}

/// Serializable recipe that rebuilds a [`StorageCredentialProvider`] after
/// [`FileIO`](crate::io::FileIO) deserialization.
///
/// Factories are serialized through [`typetag`](https://docs.rs/typetag), so
/// implementations must use `#[typetag::serde]`, and the receiving binary must
/// link the concrete implementation.
#[typetag::serde(tag = "type")]
pub trait StorageCredentialProviderFactory: Debug + Send + Sync {
    /// Build a provider for a `FileIO` with the given storage configuration.
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn StorageCredentialProvider>>;
}

/// A vended storage credential: the storage properties that apply to
/// locations under a prefix, like Java's `StorageCredential`.
///
/// `config` holds backend storage properties, such as `s3.access-key-id`,
/// `s3.secret-access-key`, `s3.session-token` and
/// `s3.session-token-expires-at-ms`. Backends read their credential and its
/// expiry from it.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageCredential {
    /// Storage-location prefix this credential applies to, such as
    /// `s3://bucket/table` or, for a whole scheme, `s3`.
    prefix: String,
    /// Backend storage properties holding the credential.
    config: HashMap<String, String>,
}

impl StorageCredential {
    /// Create a credential for locations under `prefix`.
    ///
    /// An empty prefix covers no location, as Java rejects it.
    pub fn new(prefix: impl Into<String>, config: HashMap<String, String>) -> Self {
        Self {
            prefix: prefix.into(),
            config,
        }
    }

    /// Return the storage-location prefix this credential applies to.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Return the storage properties holding the credential.
    pub fn config(&self) -> &HashMap<String, String> {
        &self.config
    }

    /// Return whether this credential applies to `location`.
    ///
    /// As in Java, `location` must start with the non-empty prefix, compared
    /// as plain strings: `s3` covers every `s3://`, `s3a://` and `s3n://`
    /// location, and `s3://bucket/tab` covers `s3://bucket/table`.
    pub fn covers(&self, location: &str) -> bool {
        !self.prefix.is_empty() && location.starts_with(&self.prefix)
    }
}

impl Debug for StorageCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Config values are secrets.
        f.debug_struct("StorageCredential")
            .field("prefix", &self.prefix)
            .field("config_keys", &self.config.keys().collect::<Vec<_>>())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scoped(prefix: &str) -> StorageCredential {
        StorageCredential::new(
            prefix,
            HashMap::from([("k".to_string(), "secret".to_string())]),
        )
    }

    #[test]
    fn test_credential_prefix_matches_like_java() {
        let credential = scoped("s3://bucket/table");
        assert!(credential.covers("s3://bucket/table"));
        assert!(credential.covers("s3://bucket/table/data/file.parquet"));
        assert!(credential.covers("s3://bucket/table2/file.parquet"));
        assert!(!credential.covers("s3://bucket/tab"));
        assert!(!credential.covers("s3a://bucket/table/file.parquet"));

        // A scheme prefix covers every location with that scheme, including
        // `s3a` and `s3n`.
        assert!(scoped("s3").covers("s3a://bucket/file.parquet"));
        assert!(!scoped("s3").covers("gs://bucket/file.parquet"));
        assert!(!scoped("").covers("s3://bucket/file.parquet"));
    }

    #[test]
    fn test_credential_debug_omits_config_values() {
        let debug = format!("{:?}", scoped("s3://bucket"));
        assert!(debug.contains("s3://bucket"), "{debug}");
        assert!(!debug.contains("secret"), "{debug}");
    }
}
