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
    ///
    /// Backends that cannot use the provider ignore it and use the credentials
    /// in `config`, as they would without one. The default does exactly that.
    #[allow(unused_variables)]
    fn build_with_credentials(
        &self,
        config: &StorageConfig,
        credential_provider: Option<Arc<dyn StorageCredentialProvider>>,
    ) -> Result<Arc<dyn Storage>> {
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
/// [`load_credential`](Self::load_credential) may be called very frequently.
/// Implementations must cache internally and only re-fetch when the current
/// credential is at or near expiry; otherwise every object-store request could
/// trigger a call back to the catalog.
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
    /// match. When the selected credential has a declared
    /// [`StorageCredential::prefix`], it must [cover](StorageCredential::covers) `path`.
    async fn load_credential(&self, path: &str) -> Result<StorageCredential>;

    /// Return a factory that rebuilds an equivalent provider in another process.
    ///
    /// [`FileIO::serialize_all`](crate::io::FileIO::serialize_all) serializes this
    /// factory in place of the provider. The default reports that the provider
    /// cannot be serialized.
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

/// A vended storage credential together with its scope and expiry.
#[derive(Clone, Debug)]
pub struct StorageCredential {
    /// Storage-location prefix this credential is scoped to. `None` represents a
    /// credential without a declared scope, sourced from flat storage properties.
    prefix: Option<String>,
    /// The backend-specific credential material.
    kind: StorageCredentialKind,
    /// When the credential expires, if known. `None` means non-expiring and
    /// backends treat such a credential as always valid and never refresh it.
    expires_at: Option<SystemTime>,
}

impl StorageCredential {
    /// Create a storage credential with no declared scope or expiration.
    pub fn new(kind: StorageCredentialKind) -> Self {
        Self {
            prefix: None,
            kind,
            expires_at: None,
        }
    }

    /// Set the storage-location prefix this credential is scoped to.
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    /// Set when this credential expires.
    pub fn with_expiration(mut self, expires_at: SystemTime) -> Self {
        self.expires_at = Some(expires_at);
        self
    }

    /// Return the storage-location prefix this credential is scoped to.
    pub fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }

    /// Return whether this credential applies to `location`.
    ///
    /// A credential without a prefix covers every location. Otherwise the
    /// prefix must match whole path segments of `location`, and scheme
    /// aliases (`s3a`/`s3n` for `s3`, `gcs` for `gs`, and the plain-text
    /// Azure schemes for their TLS variants) are treated as equal. A prefix
    /// that is only a scheme, such as `s3`, covers every location with that
    /// scheme.
    pub fn covers(&self, location: &str) -> bool {
        self.prefix
            .as_deref()
            .is_none_or(|prefix| storage_prefix_covers(prefix, location))
    }

    /// Return the backend-specific credential material.
    pub fn kind(&self) -> &StorageCredentialKind {
        &self.kind
    }

    /// Consume this credential and return its backend-specific material.
    pub fn into_kind(self) -> StorageCredentialKind {
        self.kind
    }

    /// Return when this credential expires.
    pub fn expires_at(&self) -> Option<SystemTime> {
        self.expires_at
    }
}

/// Return whether the storage-location `prefix` covers `location`, with the
/// matching rules of [`StorageCredential::covers`].
pub fn storage_prefix_covers(prefix: &str, location: &str) -> bool {
    let Some((location_scheme, location_rest)) = location.split_once("://") else {
        return false;
    };
    let Some((prefix_scheme, prefix_rest)) = prefix.split_once("://") else {
        return !prefix.is_empty() && canonical_scheme(prefix) == canonical_scheme(location_scheme);
    };

    canonical_scheme(prefix_scheme) == canonical_scheme(location_scheme)
        && location_rest
            .strip_prefix(prefix_rest)
            .is_some_and(|remainder| {
                prefix_rest.is_empty()
                    || prefix_rest.ends_with('/')
                    || remainder.is_empty()
                    || remainder.starts_with('/')
            })
}

fn canonical_scheme(scheme: &str) -> String {
    let scheme = scheme.to_ascii_lowercase();
    match scheme.as_str() {
        "s3a" | "s3n" => "s3".to_string(),
        "gcs" => "gs".to_string(),
        "abfs" => "abfss".to_string(),
        "wasb" => "wasbs".to_string(),
        _ => scheme,
    }
}

/// Backend-specific credential material.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum StorageCredentialKind {
    /// Amazon S3 credentials.
    S3(S3Credential),
    /// Google Cloud Storage credentials.
    Gcs(GcsCredential),
    /// Azure Data Lake Storage credentials.
    Azdls(AzdlsCredential),
}

/// Temporary Azure Data Lake Storage credentials (a shared access signature).
#[derive(Clone)]
pub struct AzdlsCredential {
    /// Shared access signature used to access Azure storage.
    sas_token: String,
}

impl AzdlsCredential {
    /// Create an Azure Data Lake Storage credential.
    pub fn new(sas_token: impl Into<String>) -> Self {
        Self {
            sas_token: sas_token.into(),
        }
    }

    /// Return the Azure shared access signature.
    pub fn sas_token(&self) -> &str {
        &self.sas_token
    }

    /// Consume this credential and return its shared access signature.
    pub fn into_sas_token(self) -> String {
        self.sas_token
    }
}

impl Debug for AzdlsCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzdlsCredential").finish_non_exhaustive()
    }
}

/// Temporary Amazon S3 credentials.
#[derive(Clone)]
pub struct S3Credential {
    /// AWS access key ID.
    access_key_id: String,
    /// AWS secret access key.
    secret_access_key: String,
    /// AWS session token, set for temporary (STS/vended) credentials.
    session_token: Option<String>,
}

impl S3Credential {
    /// Create temporary Amazon S3 credentials.
    pub fn new(
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        session_token: Option<String>,
    ) -> Self {
        Self {
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
            session_token,
        }
    }

    /// Return the AWS access key ID.
    pub fn access_key_id(&self) -> &str {
        &self.access_key_id
    }

    /// Return the AWS secret access key.
    pub fn secret_access_key(&self) -> &str {
        &self.secret_access_key
    }

    /// Return the AWS session token, if present.
    pub fn session_token(&self) -> Option<&str> {
        self.session_token.as_deref()
    }

    /// Consume these credentials and return their component values.
    pub fn into_parts(self) -> (String, String, Option<String>) {
        let Self {
            access_key_id,
            secret_access_key,
            session_token,
        } = self;
        (access_key_id, secret_access_key, session_token)
    }
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
    token: String,
}

impl GcsCredential {
    /// Create a Google Cloud Storage credential.
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
        }
    }

    /// Return the OAuth2 bearer token used to access GCS.
    pub fn token(&self) -> &str {
        &self.token
    }

    /// Consume this credential and return its OAuth2 bearer token.
    pub fn into_token(self) -> String {
        self.token
    }
}

impl Debug for GcsCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcsCredential").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scoped(prefix: &str) -> StorageCredential {
        StorageCredential::new(StorageCredentialKind::Gcs(GcsCredential::new("token")))
            .with_prefix(prefix)
    }

    #[test]
    fn credential_prefix_matches_whole_segments() {
        let credential = scoped("s3://bucket/table");
        assert!(credential.covers("s3://bucket/table"));
        assert!(credential.covers("s3://bucket/table/data/file.parquet"));
        assert!(!credential.covers("s3://bucket/table2/data/file.parquet"));
        assert!(!credential.covers("s3://bucket/tab"));
        assert!(!credential.covers("s3://other/table/file.parquet"));

        assert!(scoped("s3://bucket/table/").covers("s3://bucket/table/file.parquet"));
        assert!(scoped("s3://").covers("s3://any/file.parquet"));
    }

    #[test]
    fn credential_prefix_treats_scheme_aliases_as_equal() {
        let credential = scoped("s3://bucket/table");
        assert!(credential.covers("s3a://bucket/table/file.parquet"));
        assert!(credential.covers("S3N://bucket/table/file.parquet"));
        assert!(scoped("gcs://bucket").covers("gs://bucket/file.parquet"));
        assert!(
            scoped("abfss://fs@account.dfs.core.windows.net/table")
                .covers("abfs://fs@account.dfs.core.windows.net/table/file.parquet")
        );
        assert!(!credential.covers("gs://bucket/table/file.parquet"));
    }

    #[test]
    fn credential_scheme_prefix_covers_the_whole_scheme() {
        assert!(scoped("s3").covers("s3a://bucket/file.parquet"));
        assert!(!scoped("s3").covers("gs://bucket/file.parquet"));
        assert!(!scoped("").covers("s3://bucket/file.parquet"));
        assert!(!scoped("s3").covers("not-a-url"));
        assert!(
            StorageCredential::new(StorageCredentialKind::Gcs(GcsCredential::new("token")))
                .covers("gs://bucket/file.parquet")
        );
    }
}
