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

//! Encryption manager for file-level encryption and two-layer envelope key management.
//!
//! [`EncryptionManager`] provides file-level `decrypt` / `encrypt`
//! operations matching Java's `org.apache.iceberg.encryption.EncryptionManager`,
//! using envelope encryption:
//! - A master key (in KMS) wraps a Key Encryption Key (KEK)
//! - The KEK wraps Data Encryption Keys (DEKs) locally

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use aes_gcm::aead::OsRng;
use aes_gcm::aead::rand_core::RngCore;
use chrono::Utc;
use moka::future::Cache;
use uuid::Uuid;

const MILLIS_IN_DAY: i64 = 24 * 60 * 60 * 1000;

use super::crypto::{AesGcmCipher, AesKeySize, SecureKey, SensitiveBytes};
use super::io::EncryptedOutputFile;
use super::key_metadata::StandardKeyMetadata;
use super::kms::KeyManagementClient;
use crate::io::OutputFile;
use crate::spec::EncryptedKey;
use crate::{Error, ErrorKind, Result};

/// Property key for the KEK creation timestamp (milliseconds since epoch).
/// Matches Java's `StandardEncryptionManager.KEY_TIMESTAMP`.
pub const KEK_CREATED_AT_PROPERTY: &str = "KEY_TIMESTAMP";

/// Default KEK lifespan in days, per NIST SP 800-57.
const DEFAULT_KEK_LIFESPAN_DAYS: i64 = 730;

/// Default cache TTL for unwrapped KEKs.
const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(3600);

/// Default AAD prefix length in bytes.
/// Matches Java's `TableProperties.ENCRYPTION_AAD_LENGTH_DEFAULT`.
const AAD_PREFIX_LENGTH: usize = 16;

/// File-level encryption manager using two-layer envelope encryption.
///
/// Uses an async cache for unwrapped KEK bytes to avoid repeated KMS calls.
#[derive(typed_builder::TypedBuilder)]
#[builder(mutators(
    /// Add an encryption key (KEK or wrapped key metadata entry).
    pub fn add_encryption_key(&mut self, key: EncryptedKey) {
        self.encryption_keys.insert(key.key_id().to_string(), key);
    }
    /// Set all encryption keys from table metadata.
    pub fn encryption_keys(&mut self, keys: HashMap<String, EncryptedKey>) {
        self.encryption_keys = keys;
    }
))]
pub struct EncryptionManager {
    kms_client: Arc<dyn KeyManagementClient>,
    #[builder(
        default = Cache::builder().time_to_live(DEFAULT_CACHE_TTL).build(),
        setter(skip)
    )]
    kek_cache: Cache<String, SensitiveBytes>,
    /// AES key size for DEK generation. Defaults to 128-bit.
    #[builder(default = AesKeySize::default())]
    key_size: AesKeySize,
    /// Master key ID from table property `encryption.key-id`.
    #[builder(setter(into))]
    table_key_id: String,
    /// All encryption keys from table metadata (KEKs and wrapped key metadata entries).
    #[builder(default, via_mutators)]
    encryption_keys: HashMap<String, EncryptedKey>,
}

impl fmt::Debug for EncryptionManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EncryptionManager")
            .field("key_size", &self.key_size)
            .field("table_key_id", &self.table_key_id)
            .finish_non_exhaustive()
    }
}

impl EncryptionManager {
    /// Encrypt a file with AGS1 stream encryption.
    ///
    /// Returns an [`EncryptedOutputFile`] that transparently encrypts on
    /// write, along with key metadata for later decryption.
    pub fn encrypt(&self, raw_output: OutputFile) -> EncryptedOutputFile {
        let dek = SecureKey::generate(self.key_size);
        let aad_prefix = Self::generate_aad_prefix();
        let metadata = StandardKeyMetadata::new(dek.as_bytes()).with_aad_prefix(&aad_prefix);
        EncryptedOutputFile::new(raw_output, metadata)
    }

    /// Wrap key metadata bytes with a KEK for storage in table metadata.
    ///
    /// Returns `(wrapped_entry, optional_new_kek)`. The wrapped entry
    /// contains the key metadata encrypted by the KEK, and should be stored
    /// in `TableMetadata.encryption_keys`. The optional second element is a
    /// newly created KEK — present only when no active KEK existed (first
    /// write) or the existing KEK expired (rotation). When `Some`, the
    /// caller must also persist this KEK in table metadata so that future
    /// `unwrap_key_metadata` calls can find it.
    pub async fn wrap_key_metadata(
        &self,
        key_metadata: &[u8],
    ) -> Result<(EncryptedKey, Option<EncryptedKey>)> {
        let (kek, new_kek) = match self.find_active_kek(&self.encryption_keys) {
            Some(existing) => (existing.clone(), None),
            None => {
                let created = self.create_kek().await?;
                let cloned = created.clone();
                (created, Some(cloned))
            }
        };

        let kek_bytes = self.unwrap_kek(&kek).await?;

        // Use the KEK timestamp as AAD to prevent timestamp tampering attacks.
        let aad = Self::kek_timestamp_aad(&kek)?;
        let wrapped_metadata = self.wrap_dek_with_kek(key_metadata, &kek_bytes, Some(aad))?;

        let wrapped_key = EncryptedKey::builder()
            .key_id(Uuid::new_v4().to_string())
            .encrypted_key_metadata(wrapped_metadata)
            .encrypted_by_id(kek.key_id())
            .build();

        Ok((wrapped_key, new_kek))
    }

    /// Unwrap key metadata that was KEK-wrapped and stored in table metadata.
    ///
    /// Given an `EncryptedKey` entry (from a manifest list or snapshot) and
    /// the full map of encryption keys from `TableMetadata`, returns the
    /// unwrapped key metadata bytes (e.g. serialized `StandardKeyMetadata`).
    pub async fn unwrap_key_metadata(
        &self,
        encrypted_key: &EncryptedKey,
        encryption_keys: &HashMap<String, EncryptedKey>,
    ) -> Result<Vec<u8>> {
        let kek_key_id = encrypted_key.encrypted_by_id().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "EncryptedKey '{}' has no encrypted_by_id",
                    encrypted_key.key_id()
                ),
            )
        })?;

        self.decrypt_dek(
            kek_key_id,
            encrypted_key.encrypted_key_metadata(),
            encryption_keys,
        )
        .await
    }

    /// Create a new KEK, wrapped by the table's master key.
    async fn create_kek(&self) -> Result<EncryptedKey> {
        let (plaintext_kek, wrapped_kek) = if self.kms_client.supports_key_generation() {
            let result = self.kms_client.generate_key(&self.table_key_id).await?;
            (result.key().clone(), result.wrapped_key().to_vec())
        } else {
            let plaintext_key = SecureKey::generate(self.key_size);
            let wrapped = self
                .kms_client
                .wrap_key(plaintext_key.as_bytes(), &self.table_key_id)
                .await?;

            (SensitiveBytes::new(plaintext_key.as_bytes()), wrapped)
        };

        let key_id = Uuid::new_v4().to_string();
        let now_ms = Utc::now().timestamp_millis();

        let mut properties = HashMap::new();
        properties.insert(KEK_CREATED_AT_PROPERTY.to_string(), now_ms.to_string());

        self.kek_cache.insert(key_id.clone(), plaintext_kek).await;

        Ok(EncryptedKey::builder()
            .key_id(key_id)
            .encrypted_key_metadata(wrapped_kek)
            .encrypted_by_id(&self.table_key_id)
            .properties(properties)
            .build())
    }

    /// Check whether a KEK has exceeded its configured lifespan (730 days per NIST SP 800-57).
    fn is_kek_expired(&self, kek: &EncryptedKey) -> bool {
        let created_at_ms = match kek
            .properties()
            .get(KEK_CREATED_AT_PROPERTY)
            .and_then(|ts| ts.parse::<i64>().ok())
        {
            Some(ts) => ts,
            None => return true, // No timestamp -> treat as expired
        };

        let now_ms = Utc::now().timestamp_millis();
        let lifespan_ms = DEFAULT_KEK_LIFESPAN_DAYS * MILLIS_IN_DAY;
        (now_ms - created_at_ms) >= lifespan_ms
    }

    /// Find the latest non-expired KEK for the table's master key.
    fn find_active_kek<'a>(
        &self,
        encryption_keys: &'a HashMap<String, EncryptedKey>,
    ) -> Option<&'a EncryptedKey> {
        encryption_keys
            .values()
            .filter(|kek| {
                kek.encrypted_by_id()
                    .map(|id| id == self.table_key_id)
                    .unwrap_or(false)
                    && !self.is_kek_expired(kek)
            })
            .max_by_key(|kek| {
                kek.properties()
                    .get(KEK_CREATED_AT_PROPERTY)
                    .and_then(|ts| ts.parse::<i64>().ok())
                    .unwrap_or(0)
            })
    }

    /// Unwrap a KEK using the KMS, with caching to avoid repeated calls.
    async fn unwrap_kek(&self, kek: &EncryptedKey) -> Result<SensitiveBytes> {
        let cache_key = kek.key_id().to_string();

        if let Some(cached) = self.kek_cache.get(&cache_key).await {
            return Ok(cached);
        }

        let master_key_id = kek.encrypted_by_id().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("KEK '{}' has no encrypted_by_id", kek.key_id()),
            )
        })?;

        let plaintext = self
            .kms_client
            .unwrap_key(kek.encrypted_key_metadata(), master_key_id)
            .await?;

        self.kek_cache.insert(cache_key, plaintext.clone()).await;

        Ok(plaintext)
    }

    /// Decrypt a wrapped DEK using the KEK identified by `kek_key_id`.
    async fn decrypt_dek(
        &self,
        kek_key_id: &str,
        wrapped_dek: &[u8],
        encryption_keys: &HashMap<String, EncryptedKey>,
    ) -> Result<Vec<u8>> {
        let kek = encryption_keys.get(kek_key_id).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("KEK not found in encryption keys: {kek_key_id}"),
            )
        })?;

        // KEK timestamp as AAD prevents timestamp tampering.
        let aad = Self::kek_timestamp_aad(kek)?;

        let kek_bytes = self.unwrap_kek(kek).await?;
        self.unwrap_dek_with_kek(wrapped_dek, &kek_bytes, Some(aad))
            .map_err(|e| {
                Error::new(
                    e.kind(),
                    format!("Failed to unwrap key metadata with KEK '{kek_key_id}'"),
                )
                .with_source(e)
            })
    }

    /// Extract the KEK timestamp for use as AAD. Returns an error if missing.
    fn kek_timestamp_aad(kek: &EncryptedKey) -> Result<&[u8]> {
        kek.properties()
            .get(KEK_CREATED_AT_PROPERTY)
            .map(|ts| ts.as_bytes())
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "KEK '{}' is missing required '{}' property",
                        kek.key_id(),
                        KEK_CREATED_AT_PROPERTY
                    ),
                )
            })
    }

    /// Generate a random AAD prefix for file encryption.
    fn generate_aad_prefix() -> Box<[u8]> {
        let mut prefix = vec![0u8; AAD_PREFIX_LENGTH];
        OsRng.fill_bytes(&mut prefix);
        prefix.into_boxed_slice()
    }

    /// Wrap a DEK with a KEK using local AES-GCM.
    fn wrap_dek_with_kek(
        &self,
        dek: &[u8],
        kek: &SensitiveBytes,
        aad: Option<&[u8]>,
    ) -> Result<Vec<u8>> {
        let key = SecureKey::try_from(kek.clone())?;
        let cipher = AesGcmCipher::new(key);
        cipher.encrypt(dek, aad)
    }

    /// Unwrap a DEK with a KEK using local AES-GCM.
    fn unwrap_dek_with_kek(
        &self,
        wrapped_dek: &[u8],
        kek: &SensitiveBytes,
        aad: Option<&[u8]>,
    ) -> Result<Vec<u8>> {
        let key = SecureKey::try_from(kek.clone())?;
        let cipher = AesGcmCipher::new(key);
        cipher.decrypt(wrapped_dek, aad)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encryption::EncryptedInputFile;
    use crate::encryption::kms::MemoryKeyManagementClient;

    fn create_test_kms() -> Arc<dyn KeyManagementClient> {
        let kms = MemoryKeyManagementClient::new();
        kms.add_master_key("master-1").unwrap();
        Arc::new(kms)
    }

    fn create_test_manager() -> EncryptionManager {
        EncryptionManager::builder()
            .kms_client(create_test_kms())
            .table_key_id("master-1")
            .build()
    }

    #[tokio::test]
    async fn test_create_kek() {
        let mgr = create_test_manager();
        let kek = mgr.create_kek().await.unwrap();

        assert!(!kek.key_id().is_empty());
        assert!(!kek.encrypted_key_metadata().is_empty());
        assert_eq!(kek.encrypted_by_id(), Some("master-1"));
        assert!(kek.properties().contains_key(KEK_CREATED_AT_PROPERTY));
    }

    #[tokio::test]
    async fn test_wrap_unwrap_key_metadata_roundtrip() {
        let kms = create_test_kms();
        let mgr = EncryptionManager::builder()
            .kms_client(Arc::clone(&kms))
            .table_key_id("master-1")
            .build();

        let plaintext = b"some-key-metadata";
        let (entry, new_kek) = mgr.wrap_key_metadata(plaintext).await.unwrap();

        // First wrap should create a new KEK
        assert!(new_kek.is_some());
        let kek = new_kek.unwrap();

        // Build a manager with the KEK so we can unwrap
        let mgr = EncryptionManager::builder()
            .kms_client(kms)
            .table_key_id("master-1")
            .add_encryption_key(kek.clone())
            .build();

        let mut encryption_keys = HashMap::new();
        encryption_keys.insert(kek.key_id().to_string(), kek);
        let decrypted = mgr
            .unwrap_key_metadata(&entry, &encryption_keys)
            .await
            .unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[tokio::test]
    async fn test_kek_reuse_when_not_expired() {
        let kms = create_test_kms();
        let mgr = EncryptionManager::builder()
            .kms_client(Arc::clone(&kms))
            .table_key_id("master-1")
            .build();

        // First wrap creates a new KEK
        let (_, new_kek) = mgr.wrap_key_metadata(b"data-1").await.unwrap();
        let kek = new_kek.unwrap();

        // Build manager with the active KEK (same KMS to unwrap)
        let mgr = EncryptionManager::builder()
            .kms_client(kms)
            .table_key_id("master-1")
            .add_encryption_key(kek.clone())
            .build();

        // Second wrap should reuse the existing KEK (no new KEK)
        let (entry, new_kek2) = mgr.wrap_key_metadata(b"data-2").await.unwrap();
        assert!(new_kek2.is_none());
        assert_eq!(entry.encrypted_by_id(), Some(kek.key_id()));
    }

    #[tokio::test]
    async fn test_kek_rotation_when_expired() {
        let kms = create_test_kms();

        // Create a KEK with a timestamp 3 years in the past (exceeds 730-day lifespan)
        let three_years_ago_ms = Utc::now().timestamp_millis() - (3 * 365 * MILLIS_IN_DAY);
        let mut properties = HashMap::new();
        properties.insert(
            KEK_CREATED_AT_PROPERTY.to_string(),
            three_years_ago_ms.to_string(),
        );

        // Wrap a real KEK so unwrap works if needed
        let kek_key = SecureKey::generate(AesKeySize::Bits128);
        let wrapped = kms.wrap_key(kek_key.as_bytes(), "master-1").await.unwrap();

        let old_kek = EncryptedKey::builder()
            .key_id("expired-kek")
            .encrypted_key_metadata(wrapped)
            .encrypted_by_id("master-1")
            .properties(properties)
            .build();

        // Build manager with the expired KEK
        let mgr = EncryptionManager::builder()
            .kms_client(kms)
            .table_key_id("master-1")
            .add_encryption_key(old_kek.clone())
            .build();

        // Wrap should rotate to a new KEK since the existing one is expired
        let (_, new_kek) = mgr.wrap_key_metadata(b"data").await.unwrap();
        assert!(new_kek.is_some());
        assert_ne!(new_kek.unwrap().key_id(), old_kek.key_id());
    }

    #[tokio::test]
    async fn test_is_kek_expired_no_timestamp() {
        let mgr = create_test_manager();

        // KEK without a created-at timestamp -> treated as expired
        let kek = EncryptedKey::builder()
            .key_id("no-ts")
            .encrypted_key_metadata(vec![0u8; 32])
            .build();

        assert!(mgr.is_kek_expired(&kek));
    }

    #[tokio::test]
    async fn test_decrypt_dek_with_unknown_kek() {
        let mgr = create_test_manager();

        let encryption_keys: HashMap<String, EncryptedKey> = HashMap::new();
        let result = mgr
            .decrypt_dek("nonexistent-kek", &[1, 2, 3], &encryption_keys)
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_kek_cache_hit() {
        let kms = create_test_kms();
        let mgr = EncryptionManager::builder()
            .kms_client(Arc::clone(&kms))
            .table_key_id("master-1")
            .build();

        // Create KEK (caches the plaintext KEK)
        let kek = mgr.create_kek().await.unwrap();
        let mgr = EncryptionManager::builder()
            .kms_client(kms)
            .table_key_id("master-1")
            .add_encryption_key(kek.clone())
            .build();

        let mut encryption_keys = HashMap::new();
        encryption_keys.insert(kek.key_id().to_string(), kek);

        // Wrap key metadata (unwraps KEK -- should hit cache from create_kek)
        let (entry, _) = mgr.wrap_key_metadata(b"test-data").await.unwrap();

        // Unwrap key metadata (unwraps KEK again -- should hit cache)
        let decrypted = mgr
            .unwrap_key_metadata(&entry, &encryption_keys)
            .await
            .unwrap();
        assert_eq!(decrypted, b"test-data");
    }

    #[tokio::test]
    async fn test_unwrap_fails_when_kek_missing_timestamp() {
        let kms = create_test_kms();
        let mgr = EncryptionManager::builder()
            .kms_client(Arc::clone(&kms))
            .table_key_id("master-1")
            .build();

        // Wrap some metadata to get a valid encrypted entry
        let (entry, new_kek) = mgr.wrap_key_metadata(b"secret").await.unwrap();
        let kek = new_kek.unwrap();

        // Re-create the KEK without its KEY_TIMESTAMP property
        let kek_no_ts = EncryptedKey::builder()
            .key_id(kek.key_id())
            .encrypted_key_metadata(kek.encrypted_key_metadata())
            .encrypted_by_id(kek.encrypted_by_id().unwrap())
            .build();

        let mut encryption_keys = HashMap::new();
        encryption_keys.insert(kek_no_ts.key_id().to_string(), kek_no_ts);

        let mgr = EncryptionManager::builder()
            .kms_client(kms)
            .table_key_id("master-1")
            .build();

        let result = mgr.unwrap_key_metadata(&entry, &encryption_keys).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.to_string().contains(KEK_CREATED_AT_PROPERTY),
            "error should mention the missing property: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_unwrap_fails_when_kek_timestamp_tampered() {
        let kms = create_test_kms();
        let mgr = EncryptionManager::builder()
            .kms_client(Arc::clone(&kms))
            .table_key_id("master-1")
            .build();

        // Wrap metadata normally
        let (entry, new_kek) = mgr.wrap_key_metadata(b"secret").await.unwrap();
        let kek = new_kek.unwrap();

        // Tamper with the KEK timestamp (change the AAD)
        let mut tampered_properties = kek.properties().clone();
        tampered_properties.insert(KEK_CREATED_AT_PROPERTY.to_string(), "9999999".to_string());

        let tampered_kek = EncryptedKey::builder()
            .key_id(kek.key_id())
            .encrypted_key_metadata(kek.encrypted_key_metadata())
            .encrypted_by_id(kek.encrypted_by_id().unwrap())
            .properties(tampered_properties)
            .build();

        let mut encryption_keys = HashMap::new();
        encryption_keys.insert(tampered_kek.key_id().to_string(), tampered_kek);

        let mgr = EncryptionManager::builder()
            .kms_client(kms)
            .table_key_id("master-1")
            .build();

        // Unwrap should fail because the AAD (timestamp) doesn't match what was used to wrap
        let result = mgr.unwrap_key_metadata(&entry, &encryption_keys).await;
        assert!(
            result.is_err(),
            "tampered timestamp should cause decryption failure"
        );
    }

    #[tokio::test]
    async fn test_encrypt_decrypt_roundtrip() {
        use crate::io::FileIO;

        let io = FileIO::new_with_memory();
        let path = "memory:///test/encrypt_roundtrip.bin";

        let kms = MemoryKeyManagementClient::new();
        kms.add_master_key("master-1").unwrap();
        let mgr = EncryptionManager::builder()
            .kms_client(Arc::new(kms) as Arc<dyn KeyManagementClient>)
            .table_key_id("master-1")
            .build();

        let output = io.new_output(path).unwrap();
        let encrypted_output = mgr.encrypt(output);

        let plaintext = b"Hello, encrypted Iceberg round-trip!";
        let serialized_metadata = encrypted_output.key_metadata().encode().unwrap();
        encrypted_output
            .write(bytes::Bytes::from(plaintext.to_vec()))
            .await
            .unwrap();

        let input = io.new_input(path).unwrap();
        let parsed_metadata = StandardKeyMetadata::decode(&serialized_metadata).unwrap();
        let decrypted_file = EncryptedInputFile::new(input, parsed_metadata);

        let content = decrypted_file.read().await.unwrap();
        assert_eq!(&content[..], plaintext);
    }
}
