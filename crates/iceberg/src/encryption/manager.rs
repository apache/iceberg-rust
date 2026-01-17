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

//! Central encryption manager for coordinating encryption operations.

use std::sync::Arc;
use std::time::Duration;

use crate::encryption::{
    AesGcmEncryptor, EncryptionAlgorithm, EncryptionKeyMetadata, KeyCache, KeyManagementClient,
    SecureKey, StandardKeyMetadata,
};
use crate::{Error, ErrorKind, Result};

/// Central manager for encryption operations.
///
/// The EncryptionManager coordinates:
/// - Key Management Service (KMS) operations for wrapping/unwrapping keys
/// - Caching of unwrapped keys to reduce KMS calls
/// - Creation of encryptors for file encryption/decryption
///
/// It is designed to be shared across multiple operations using `Arc`.
#[derive(Clone)]
pub struct EncryptionManager {
    /// KMS client for key wrapping/unwrapping
    kms_client: Arc<dyn KeyManagementClient>,
    /// Encryption algorithm to use
    algorithm: EncryptionAlgorithm,
    /// Cache for unwrapped keys
    key_cache: Arc<KeyCache>,
}

impl EncryptionManager {
    /// Creates a new encryption manager.
    ///
    /// # Arguments
    /// * `kms_client` - The KMS client to use for key operations
    /// * `algorithm` - The encryption algorithm to use
    /// * `cache_ttl` - Time-to-live for cached keys (typically 1 hour)
    pub fn new(
        kms_client: Arc<dyn KeyManagementClient>,
        algorithm: EncryptionAlgorithm,
        cache_ttl: Duration,
    ) -> Self {
        Self {
            kms_client,
            algorithm,
            key_cache: Arc::new(KeyCache::new(cache_ttl)),
        }
    }

    /// Creates an encryption manager with default settings.
    ///
    /// Uses AES-128-GCM algorithm and 1-hour cache TTL.
    pub fn with_defaults(kms_client: Arc<dyn KeyManagementClient>) -> Self {
        Self::new(
            kms_client,
            EncryptionAlgorithm::Aes128Gcm,
            Duration::from_secs(3600), // 1 hour
        )
    }

    /// Prepares an encryptor for decrypting an input file.
    ///
    /// This method:
    /// 1. Checks the key cache for an existing encryptor
    /// 2. If not cached, deserializes the key metadata
    /// 3. Unwraps the DEK from the KMS
    /// 4. Creates an encryptor with the unwrapped key
    /// 5. Caches the encryptor for future use
    ///
    /// # Arguments
    /// * `key_metadata` - The serialized StandardKeyMetadata from the file metadata
    ///
    /// # Returns
    /// An encryptor ready to decrypt the file
    pub async fn prepare_decryption(&self, key_metadata: &[u8]) -> Result<Arc<AesGcmEncryptor>> {
        // Check cache first
        if let Some(cached) = self.key_cache.get(key_metadata).await {
            return Ok(cached);
        }

        // Deserialize metadata
        let metadata = StandardKeyMetadata::deserialize(key_metadata)?;

        // Unwrap DEK from KMS
        let dek_bytes = self
            .kms_client
            .unwrap_key(metadata.encryption_key())
            .await?;

        // Validate DEK length matches algorithm
        if dek_bytes.len() != self.algorithm.key_length() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Unwrapped DEK length {} doesn't match algorithm requirement {}",
                    dek_bytes.len(),
                    self.algorithm.key_length()
                ),
            ));
        }

        // Create secure key
        let dek = SecureKey::new(dek_bytes, self.algorithm)?;

        // Create encryptor
        let encryptor = Arc::new(AesGcmEncryptor::new(dek));

        // Cache it
        self.key_cache
            .insert(key_metadata.to_vec(), encryptor.clone())
            .await;

        Ok(encryptor)
    }

    /// Extracts the AAD prefix from encrypted key metadata.
    ///
    /// This is needed for native Parquet encryption where the AAD prefix
    /// must be passed separately to the Parquet reader.
    ///
    /// # Arguments
    /// * `key_metadata` - The serialized StandardKeyMetadata
    ///
    /// # Returns
    /// The AAD prefix bytes
    pub fn extract_aad_prefix(&self, key_metadata: &[u8]) -> Result<Vec<u8>> {
        let metadata = StandardKeyMetadata::deserialize(key_metadata)?;
        Ok(metadata.aad_prefix().to_vec())
    }

    /// Bulk preparation of decryption for multiple files.
    ///
    /// This method processes multiple key metadata entries in parallel,
    /// making concurrent KMS calls for improved performance.
    ///
    /// # Arguments
    /// * `key_metadatas` - Vector of key metadata bytes
    ///
    /// # Returns
    /// Vector of encryptors in the same order as input
    pub async fn bulk_prepare_decryption(
        &self,
        key_metadatas: Vec<Vec<u8>>,
    ) -> Result<Vec<Arc<AesGcmEncryptor>>> {
        use futures::stream::{self, StreamExt};

        let results: Vec<Result<Arc<AesGcmEncryptor>>> = stream::iter(key_metadatas)
            .map(|metadata| async move { self.prepare_decryption(&metadata).await })
            .buffer_unordered(10) // Process 10 in parallel
            .collect()
            .await;

        results.into_iter().collect()
    }

    /// Evicts expired entries from the key cache.
    ///
    /// This should be called periodically to clean up the cache.
    pub async fn evict_expired(&self) {
        self.key_cache.evict_expired().await;
    }

    /// Clears all entries from the key cache.
    pub async fn clear_cache(&self) {
        self.key_cache.clear().await;
    }

    /// Returns the encryption algorithm used by this manager.
    pub fn algorithm(&self) -> EncryptionAlgorithm {
        self.algorithm
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encryption::InMemoryKms;

    async fn create_test_manager() -> EncryptionManager {
        let master_key = vec![0u8; 16]; // 128-bit master key
        let kms = Arc::new(InMemoryKms::new_with_master_key(
            "test-key".to_string(),
            master_key,
        ));

        EncryptionManager::with_defaults(kms)
    }

    async fn create_test_metadata(_manager: &EncryptionManager) -> Vec<u8> {
        let dek = SecureKey::generate(EncryptionAlgorithm::Aes128Gcm);
        let aad_prefix = b"test_aad_prefix".to_vec();

        // Wrap the DEK
        let kms = Arc::new(InMemoryKms::new_with_master_key(
            "test-key".to_string(),
            vec![0u8; 16],
        ));
        let wrapped_key = kms.wrap_key(dek.as_bytes(), "test-key").await.unwrap();

        // Create metadata
        let metadata = StandardKeyMetadata::new(wrapped_key, aad_prefix, Some(1024));
        metadata.serialize().unwrap()
    }

    #[tokio::test]
    async fn test_prepare_decryption() {
        let manager = create_test_manager().await;
        let key_metadata = create_test_metadata(&manager).await;

        let encryptor = manager.prepare_decryption(&key_metadata).await.unwrap();
        assert!(Arc::strong_count(&encryptor) >= 1);
    }

    #[tokio::test]
    async fn test_prepare_decryption_caching() {
        let manager = create_test_manager().await;
        let key_metadata = create_test_metadata(&manager).await;

        // First call - should unwrap from KMS
        let encryptor1 = manager.prepare_decryption(&key_metadata).await.unwrap();

        // Second call - should come from cache
        let encryptor2 = manager.prepare_decryption(&key_metadata).await.unwrap();

        // Both should be the same Arc
        assert!(Arc::ptr_eq(&encryptor1, &encryptor2));
    }

    #[tokio::test]
    async fn test_extract_aad_prefix() {
        let manager = create_test_manager().await;
        let key_metadata = create_test_metadata(&manager).await;

        let aad_prefix = manager.extract_aad_prefix(&key_metadata).unwrap();
        assert_eq!(&aad_prefix, b"test_aad_prefix");
    }

    #[tokio::test]
    async fn test_bulk_prepare_decryption() {
        let manager = create_test_manager().await;

        let metadata1 = create_test_metadata(&manager).await;
        let metadata2 = create_test_metadata(&manager).await;
        let metadata3 = create_test_metadata(&manager).await;

        let encryptors = manager
            .bulk_prepare_decryption(vec![metadata1, metadata2, metadata3])
            .await
            .unwrap();

        assert_eq!(encryptors.len(), 3);
    }

    #[tokio::test]
    async fn test_clear_cache() {
        let manager = create_test_manager().await;
        let key_metadata = create_test_metadata(&manager).await;

        // Prepare decryption to populate cache
        manager.prepare_decryption(&key_metadata).await.unwrap();

        // Clear the cache
        manager.clear_cache().await;

        // Next call should hit KMS again (we can't directly verify,
        // but this tests that clear doesn't panic)
        let encryptor = manager.prepare_decryption(&key_metadata).await.unwrap();
        assert!(Arc::strong_count(&encryptor) >= 1);
    }

    #[tokio::test]
    async fn test_invalid_metadata() {
        let manager = create_test_manager().await;
        let invalid_metadata = b"not valid avro data";

        let result = manager.prepare_decryption(invalid_metadata).await;
        assert!(result.is_err());
    }
}
