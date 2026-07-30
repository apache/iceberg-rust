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

use std::fmt::Debug;
use std::sync::Arc;
use std::time::SystemTime;

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
    fn build_with_credentials(
        &self,
        config: &StorageConfig,
        credential_provider: Option<Arc<dyn StorageCredentialProvider>>,
    ) -> Result<Arc<dyn Storage>> {
        if credential_provider.is_some() {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Storage factory does not support refreshable credential providers",
            ));
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
/// # Caching
///
/// [`load_credential`](Self::load_credential) may be called very frequently —
/// the S3 backend, for example, rebuilds its operator (and therefore its
/// signer) on every file operation. Implementations must cache internally and
/// only re-fetch when the current credential is at or near expiry; otherwise
/// every object-store request would trigger a call back to the catalog.
#[async_trait]
pub trait StorageCredentialProvider: Debug + Send + Sync {
    /// Return whether this provider has refresh configuration for `path`.
    ///
    /// Backends use this before replacing their normal credential chain. The
    /// default is `true` for single-backend providers; multi-backend providers
    /// should return `false` for schemes they do not configure.
    fn supports_path(&self, _path: &str) -> bool {
        true
    }

    /// Load a fresh credential for the storage location identified by `path`.
    ///
    /// `path` is the absolute location being accessed (e.g.
    /// `s3://bucket/warehouse/db/table/...`). Providers that vend distinct
    /// credentials per location prefix use it to select the most specific
    /// match.
    async fn load_credential(&self, path: &str) -> Result<StorageCredential>;
}

/// A vended storage credential together with when it expires.
#[derive(Clone, Debug)]
pub struct StorageCredential {
    /// The backend-specific credential material.
    pub kind: StorageCredentialKind,
    /// When the credential expires, if known. `None` means non-expiring and
    /// backends treat such a credential as always valid and never refresh it.
    pub expires_at: Option<SystemTime>,
}

/// Backend-specific credential material.
#[derive(Clone, Debug)]
pub enum StorageCredentialKind {
    /// Amazon S3 credentials.
    S3(S3Credential),
    /// Google Cloud Storage credentials.
    Gcs(GcsCredential),
}

/// Temporary Amazon S3 credentials.
#[derive(Clone)]
pub struct S3Credential {
    /// AWS access key ID.
    pub access_key_id: String,
    /// AWS secret access key.
    pub secret_access_key: String,
    /// AWS session token, set for temporary (STS/vended) credentials.
    pub session_token: Option<String>,
}

impl Debug for S3Credential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Credential").finish_non_exhaustive()
    }
}

/// Temporary Google Cloud Storage credentials (an OAuth2 access token).
#[derive(Clone)]
pub struct GcsCredential {
    /// OAuth2 bearer token used to access GCS.
    pub token: String,
}

impl Debug for GcsCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcsCredential").finish_non_exhaustive()
    }
}
