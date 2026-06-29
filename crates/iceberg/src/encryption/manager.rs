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
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use moka::future::Cache;
use uuid::Uuid;

const MILLIS_IN_DAY: i64 = 24 * 60 * 60 * 1000;

use super::crypto::{AesGcmCipher, AesKeySize, SecureKey, SensitiveBytes};
use super::handler::{FileEncryptionHandler, generate_standard_key_metadata};
use super::io::EncryptedOutputFile;
use super::key_metadata::StandardKeyMetadata;
use super::kms::KeyManagementClient;
use crate::io::OutputFile;
use crate::spec::{EncryptedKey, FormatVersion, TableMetadataRef};
use crate::{Error, ErrorKind, Result};

/// Property key for the KEK creation timestamp (milliseconds since epoch).
/// Matches Java's `StandardEncryptionManager.KEY_TIMESTAMP`.
pub const KEK_CREATED_AT_PROPERTY: &str = "KEY_TIMESTAMP";

/// Default KEK lifespan in days, per NIST SP 800-57.
const DEFAULT_KEK_LIFESPAN_DAYS: i64 = 730;

/// Default cache TTL for unwrapped KEKs.
const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(3600);

/// File-level encryption manager using two-layer envelope encryption.
///
/// Uses an async cache for unwrapped KEK bytes to avoid repeated KMS calls.
#[derive(typed_builder::TypedBuilder)]
#[builder(mutators(
    /// Add an encryption key (KEK or wrapped key metadata entry).
    pub fn add_encryption_key(&mut self, key: EncryptedKey) {
        self.encryption_keys
            .write()
            .expect("encryption_keys lock poisoned")
            .insert(key.key_id().to_string(), key);
    }
    /// Set all encryption keys from table metadata.
    pub fn encryption_keys(&mut self, keys: HashMap<String, EncryptedKey>) {
        self.encryption_keys = RwLock::new(keys);
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
    /// Newly created KEKs and wrapped manifest-list entries are inserted here so
    /// callers can snapshot the full set at commit time via [`EncryptionManager::encryption_keys`].
    #[builder(default = RwLock::new(HashMap::new()), via_mutators)]
    encryption_keys: RwLock<HashMap<String, EncryptedKey>>,
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
    /// Attempt to construct an [`EncryptionManager`] from table metadata.
    ///
    /// Returns `Ok(None)` if the format version is below v3 or the
    /// `encryption.key-id` property is not set. Returns an error if the
    /// property is set but no [`KeyManagementClient`] was provided.
    pub(crate) fn from_table_metadata(
        kms_client: Option<&Arc<dyn KeyManagementClient>>,
        metadata: &TableMetadataRef,
    ) -> Result<Option<Arc<Self>>> {
        if metadata.format_version() < FormatVersion::V3 {
            return Ok(None);
        }

        let table_properties = metadata.table_properties()?;
        let Some(table_key_id) = table_properties.encryption_key_id else {
            if kms_client.is_some() {
                tracing::warn!(
                    "KeyManagementClient provided but table does not have encryption.key-id set"
                );
            }
            return Ok(None);
        };

        let kms_client = kms_client.ok_or_else(|| {
            Error::new(
                ErrorKind::PreconditionFailed,
                "Table has encryption.key-id set but no KeyManagementClient was provided to TableBuilder",
            )
        })?;

        let em = EncryptionManager::builder()
            .kms_client(Arc::clone(kms_client))
            .table_key_id(table_key_id)
            .encryption_keys(metadata.encryption_keys.clone())
            .key_size(AesKeySize::from_key_length(
                table_properties.encryption_data_key_length,
            )?)
            .build();
        Ok(Some(Arc::new(em)))
    }

    /// Encrypt a file with AGS1 stream encryption.
    ///
    /// Returns an [`EncryptedOutputFile`] that transparently encrypts on
    /// write, along with key metadata for later decryption.
    pub fn encrypt(&self, raw_output: OutputFile) -> EncryptedOutputFile {
        EncryptedOutputFile::new(raw_output, generate_standard_key_metadata(self.key_size))
    }

    /// Wrap a manifest list key metadata with a KEK for storage in table metadata.
    ///
    /// Stores the resulting wrapped entry (and any newly created KEK) in the
    /// manager's internal `encryption_keys` map. Callers persist the full set
    /// at commit time via [`Self::encryption_keys`].
    ///
    /// Returns the `key_id` of the wrapped entry, which should be recorded on
    /// the snapshot as `encryption_key_id` so readers can locate it later.
    pub async fn encrypt_manifest_list_key_metadata(
        &self,
        key_metadata: &StandardKeyMetadata,
    ) -> Result<String> {
        let kek = match self.find_active_kek()? {
            Some(existing) => existing,
            None => self.create_kek().await?,
        };

        let kek_bytes = self.unwrap_key_encryption_key(&kek).await?;

        // Use the KEK timestamp as AAD to prevent timestamp tampering attacks.
        let aad = Self::kek_timestamp_aad(&kek)?;
        let serialized = key_metadata.encode()?;
        let wrapped_metadata = self.wrap_dek_with_kek(&serialized, &kek_bytes, Some(aad))?;

        let wrapped_key = EncryptedKey::builder()
            .key_id(Uuid::new_v4().to_string())
            .encrypted_key_metadata(wrapped_metadata)
            .encrypted_by_id(kek.key_id())
            .build();

        let wrapped_key_id = wrapped_key.key_id().to_string();
        self.insert_encryption_key(wrapped_key);
        Ok(wrapped_key_id)
    }

    /// Decrypt a manifest list key metadata previously wrapped via
    /// [`Self::encrypt_manifest_list_key_metadata`].
    ///
    /// Looks up the entry by `encryption_key_id` (typically read from the
    /// snapshot) in the manager's `encryption_keys` map.
    pub async fn decrypt_manifest_list_key_metadata(
        &self,
        encryption_key_id: &str,
    ) -> Result<StandardKeyMetadata> {
        let encrypted_key = self
            .encryption_keys
            .read()
            .expect("encryption_keys lock poisoned")
            .get(encryption_key_id)
            .cloned()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Encryption key '{encryption_key_id}' not found"),
                )
            })?;

        let kek_key_id = encrypted_key.encrypted_by_id().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "EncryptedKey '{}' has no encrypted_by_id",
                    encrypted_key.key_id()
                ),
            )
        })?;

        let bytes = self
            .decrypt_dek(kek_key_id, encrypted_key.encrypted_key_metadata())
            .await?;

        StandardKeyMetadata::decode(bytes.as_bytes())
    }

    /// Borrow the encryption keys held by this manager.
    ///
    /// Use at commit time to persist newly created KEKs and wrapped
    /// manifest-list entries into `TableMetadata.encryption_keys`.
    pub fn with_encryption_keys<F, R>(&self, f: F) -> R
    where F: FnOnce(&HashMap<String, EncryptedKey>) -> R {
        let keys = self
            .encryption_keys
            .read()
            .expect("encryption_keys lock poisoned");
        f(&keys)
    }

    fn insert_encryption_key(&self, key: EncryptedKey) {
        self.encryption_keys
            .write()
            .expect("encryption_keys lock poisoned")
            .insert(key.key_id().to_string(), key);
    }

    /// Create a new KEK, wrapped by the table's master key, and store it in
    /// the manager's `encryption_keys` map.
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

        let kek = EncryptedKey::builder()
            .key_id(key_id)
            .encrypted_key_metadata(wrapped_kek)
            .encrypted_by_id(&self.table_key_id)
            .properties(properties)
            .build();

        self.insert_encryption_key(kek.clone());
        Ok(kek)
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
    fn find_active_kek(&self) -> Result<Option<EncryptedKey>> {
        let keys = self
            .encryption_keys
            .read()
            .expect("encryption_keys lock poisoned");
        Ok(keys
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
            .cloned())
    }

    /// Unwrap a KEK using the KMS, with caching to avoid repeated calls.
    async fn unwrap_key_encryption_key(&self, kek: &EncryptedKey) -> Result<SensitiveBytes> {
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

    /// Decrypt a wrapped DEK using the KEK identified by `kek_key_id`,
    /// looked up in the manager's own `encryption_keys` map.
    async fn decrypt_dek(&self, kek_key_id: &str, wrapped_dek: &[u8]) -> Result<SensitiveBytes> {
        let kek = self
            .encryption_keys
            .read()
            .expect("encryption_keys lock poisoned")
            .get(kek_key_id)
            .cloned()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("KEK not found in encryption keys: {kek_key_id}"),
                )
            })?;

        // KEK timestamp as AAD prevents timestamp tampering.
        let aad = Self::kek_timestamp_aad(&kek)?;

        let kek_bytes = self.unwrap_key_encryption_key(&kek).await?;
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
    ) -> Result<SensitiveBytes> {
        let key = SecureKey::try_from(kek.clone())?;
        let cipher = AesGcmCipher::new(key);
        cipher.decrypt(wrapped_dek, aad).map(SensitiveBytes::new)
    }
}

#[async_trait]
impl FileEncryptionHandler for EncryptionManager {
    /// Generate per-file key metadata for the standard encryption scheme.
    ///
    /// Returns a fresh plaintext DEK + AAD prefix sized to the manager's
    /// configured [`AesKeySize`]. No KMS round-trip — the KMS/KEK envelope
    /// work happens one tier up when the manifest-list key metadata is wrapped.
    async fn next_key_metadata(&self) -> Result<StandardKeyMetadata> {
        Ok(generate_standard_key_metadata(self.key_size))
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

    fn sample_key_metadata() -> StandardKeyMetadata {
        StandardKeyMetadata::new(b"0123456789abcdef").with_aad_prefix(b"test-aad-prefix!")
    }

    #[tokio::test]
    async fn test_wrap_unwrap_key_metadata_roundtrip() {
        let mgr = create_test_manager();
        let plaintext = sample_key_metadata();

        let key_id = mgr
            .encrypt_manifest_list_key_metadata(&plaintext)
            .await
            .unwrap();

        // First wrap should create a new KEK and the wrapped entry — both stored on the manager
        assert_eq!(mgr.with_encryption_keys(|k| k.len()), 2);

        let decrypted = mgr
            .decrypt_manifest_list_key_metadata(&key_id)
            .await
            .unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[tokio::test]
    async fn test_kek_reuse_when_not_expired() {
        let mgr = create_test_manager();

        // First wrap creates a new KEK + wrapped entry (2 keys)
        let _id1 = mgr
            .encrypt_manifest_list_key_metadata(&sample_key_metadata())
            .await
            .unwrap();
        let kek_id = mgr.with_encryption_keys(|keys| {
            assert_eq!(keys.len(), 2);
            keys.values()
                .find(|k| k.encrypted_by_id() == Some("master-1"))
                .unwrap()
                .key_id()
                .to_string()
        });

        // Second wrap should reuse the existing KEK (only adds 1 new wrapped entry)
        let id2 = mgr
            .encrypt_manifest_list_key_metadata(&sample_key_metadata())
            .await
            .unwrap();
        let entry2 = mgr.with_encryption_keys(|keys| {
            assert_eq!(keys.len(), 3);
            keys.get(&id2).cloned().unwrap()
        });
        assert_eq!(entry2.encrypted_by_id(), Some(kek_id.as_str()));
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
        let new_entry_id = mgr
            .encrypt_manifest_list_key_metadata(&sample_key_metadata())
            .await
            .unwrap();
        let entry = mgr
            .with_encryption_keys(|keys| keys.get(&new_entry_id).cloned())
            .unwrap();
        let used_kek_id = entry.encrypted_by_id().unwrap();
        assert_ne!(used_kek_id, old_kek.key_id());
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
    async fn test_decrypt_with_unknown_key_id() {
        let mgr = create_test_manager();
        let result = mgr.decrypt_manifest_list_key_metadata("nonexistent").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_kek_cache_hit() {
        let mgr = create_test_manager();

        // First wrap caches the plaintext KEK during create_kek().
        let key_id = mgr
            .encrypt_manifest_list_key_metadata(&sample_key_metadata())
            .await
            .unwrap();

        // Decrypt unwraps the KEK; with the cache populated this should not hit KMS again.
        let _ = mgr
            .decrypt_manifest_list_key_metadata(&key_id)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_unwrap_fails_when_kek_missing_timestamp() {
        let mgr = create_test_manager();

        // Wrap some metadata to get a valid encrypted entry stored on the manager
        let entry_id = mgr
            .encrypt_manifest_list_key_metadata(&sample_key_metadata())
            .await
            .unwrap();

        // Find the KEK that wrapped the entry and replace it with a copy that
        // is missing the KEY_TIMESTAMP property, simulating a malformed table.
        let mut keys = mgr.with_encryption_keys(|k| k.clone());
        let kek_id = keys
            .get(&entry_id)
            .unwrap()
            .encrypted_by_id()
            .unwrap()
            .to_string();
        let kek = keys.remove(&kek_id).unwrap();
        let kek_no_ts = EncryptedKey::builder()
            .key_id(kek.key_id())
            .encrypted_key_metadata(kek.encrypted_key_metadata())
            .encrypted_by_id(kek.encrypted_by_id().unwrap())
            .build();
        keys.insert(kek_no_ts.key_id().to_string(), kek_no_ts);

        let mgr = EncryptionManager::builder()
            .kms_client(create_test_kms())
            .table_key_id("master-1")
            .encryption_keys(keys)
            .build();

        let result = mgr.decrypt_manifest_list_key_metadata(&entry_id).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.to_string().contains(KEK_CREATED_AT_PROPERTY),
            "error should mention the missing property: {err}"
        );
    }

    #[tokio::test]
    async fn test_unwrap_fails_when_kek_timestamp_tampered() {
        let mgr = create_test_manager();

        // Wrap metadata normally
        let entry_id = mgr
            .encrypt_manifest_list_key_metadata(&sample_key_metadata())
            .await
            .unwrap();

        // Tamper with the KEK timestamp (change the AAD)
        let mut keys = mgr.with_encryption_keys(|k| k.clone());
        let kek_id = keys
            .get(&entry_id)
            .unwrap()
            .encrypted_by_id()
            .unwrap()
            .to_string();
        let kek = keys.remove(&kek_id).unwrap();
        let mut tampered_properties = kek.properties().clone();
        tampered_properties.insert(KEK_CREATED_AT_PROPERTY.to_string(), "9999999".to_string());
        let tampered_kek = EncryptedKey::builder()
            .key_id(kek.key_id())
            .encrypted_key_metadata(kek.encrypted_key_metadata())
            .encrypted_by_id(kek.encrypted_by_id().unwrap())
            .properties(tampered_properties)
            .build();
        keys.insert(tampered_kek.key_id().to_string(), tampered_kek);

        let mgr = EncryptionManager::builder()
            .kms_client(create_test_kms())
            .table_key_id("master-1")
            .encryption_keys(keys)
            .build();

        // Unwrap should fail because the AAD (timestamp) doesn't match what was used to wrap
        let result = mgr.decrypt_manifest_list_key_metadata(&entry_id).await;
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
