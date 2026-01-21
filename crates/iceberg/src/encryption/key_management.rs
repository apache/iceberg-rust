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

//! Key management client trait and implementations.
//!
//! This module provides a pluggable interface for key wrapping and unwrapping
//! operations with Key Management Services (KMS).

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::{Error, ErrorKind, Result};

/// Trait for key management clients that can wrap and unwrap encryption keys.
///
/// Implementations of this trait provide integration with various KMS services
/// (AWS KMS, Azure Key Vault, GCP KMS, etc.) for envelope encryption.
#[async_trait::async_trait]
pub trait KeyManagementClient: Send + Sync {
    /// Wraps a Data Encryption Key (DEK) using a Key Encryption Key (KEK).
    ///
    /// # Arguments
    /// * `dek` - The plaintext data encryption key to wrap
    /// * `master_key_id` - The identifier of the master key to use for wrapping
    ///
    /// # Returns
    /// The wrapped (encrypted) DEK as bytes
    async fn wrap_key(&self, dek: &[u8], master_key_id: &str) -> Result<Vec<u8>>;

    /// Unwraps a wrapped Data Encryption Key (DEK).
    ///
    /// # Arguments
    /// * `wrapped_dek` - The wrapped (encrypted) data encryption key
    ///
    /// # Returns
    /// The unwrapped (plaintext) DEK as bytes
    async fn unwrap_key(&self, wrapped_dek: &[u8]) -> Result<Vec<u8>>;
}

/// In-memory KMS implementation for testing and development.
///
/// This implementation stores master keys in memory and uses AES-GCM for
/// wrapping/unwrapping operations. It should NOT be used in production.
///
/// # Security Warning
/// This implementation is for testing only. Master keys are stored in memory
/// without secure storage or access controls.
pub struct InMemoryKms {
    /// Master keys indexed by key ID
    keys: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl InMemoryKms {
    /// Creates a new in-memory KMS with no keys.
    pub fn new() -> Self {
        Self {
            keys: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Creates a new in-memory KMS with a single master key.
    pub fn new_with_master_key(key_id: String, master_key: Vec<u8>) -> Self {
        let mut keys = HashMap::new();
        keys.insert(key_id, master_key);
        Self {
            keys: Arc::new(RwLock::new(keys)),
        }
    }

    /// Adds a master key to this KMS.
    pub async fn add_master_key(&self, key_id: String, master_key: Vec<u8>) {
        let mut keys = self.keys.write().await;
        keys.insert(key_id, master_key);
    }

    /// Retrieves a master key by ID.
    async fn get_master_key(&self, key_id: &str) -> Result<Vec<u8>> {
        let keys = self.keys.read().await;
        keys.get(key_id).cloned().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Master key not found: {}", key_id),
            )
        })
    }
}

impl Default for InMemoryKms {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl KeyManagementClient for InMemoryKms {
    async fn wrap_key(&self, dek: &[u8], master_key_id: &str) -> Result<Vec<u8>> {
        use crate::encryption::{AesGcmEncryptor, EncryptionAlgorithm, SecureKey};

        // Get the master key
        let master_key_bytes = self.get_master_key(master_key_id).await?;

        // Determine algorithm based on master key length
        let algorithm = match master_key_bytes.len() {
            16 => EncryptionAlgorithm::Aes128Gcm,
            _ => {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Unsupported master key length: {} (expected 16 for AES-128)",
                        master_key_bytes.len()
                    ),
                ));
            }
        };

        // Create secure key and encryptor
        let master_key = SecureKey::new(master_key_bytes, algorithm)?;
        let encryptor = AesGcmEncryptor::new(master_key);

        // Wrap the DEK using the master key
        // AAD includes the master key ID for additional authentication
        let aad = master_key_id.as_bytes();
        encryptor.encrypt(dek, Some(aad))
    }

    async fn unwrap_key(&self, wrapped_dek: &[u8]) -> Result<Vec<u8>> {
        use crate::encryption::{AesGcmEncryptor, EncryptionAlgorithm, SecureKey};

        // For unwrapping, we need to extract the master key ID from context
        // In a real implementation, this would be part of the wrapped key metadata
        // For this in-memory version, we'll try all available keys

        let keys = self.keys.read().await;

        // Try each master key until one works
        for (key_id, master_key_bytes) in keys.iter() {
            let algorithm = match master_key_bytes.len() {
                16 => EncryptionAlgorithm::Aes128Gcm,
                _ => continue,
            };

            let master_key = SecureKey::new(master_key_bytes.clone(), algorithm)?;
            let encryptor = AesGcmEncryptor::new(master_key);

            let aad = key_id.as_bytes();
            if let Ok(unwrapped) = encryptor.decrypt(wrapped_dek, Some(aad)) {
                return Ok(unwrapped);
            }
        }

        Err(Error::new(
            ErrorKind::DataInvalid,
            "Failed to unwrap key: no master key succeeded",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_in_memory_kms_wrap_unwrap() {
        let master_key = vec![0u8; 16]; // 128-bit key
        let kms = InMemoryKms::new_with_master_key("test-key".to_string(), master_key);

        let dek = b"data_encryption_key";

        let wrapped = kms.wrap_key(dek, "test-key").await.unwrap();
        assert_ne!(&wrapped[..], dek); // Should be encrypted

        let unwrapped = kms.unwrap_key(&wrapped).await.unwrap();
        assert_eq!(&unwrapped[..], dek);
    }

    #[tokio::test]
    async fn test_in_memory_kms_missing_key() {
        let kms = InMemoryKms::new();
        let dek = b"data_encryption_key";

        let result = kms.wrap_key(dek, "nonexistent").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_in_memory_kms_add_key() {
        let kms = InMemoryKms::new();
        kms.add_master_key("key1".to_string(), vec![0u8; 16]).await;

        let dek = b"test_dek";
        let wrapped = kms.wrap_key(dek, "key1").await.unwrap();
        let unwrapped = kms.unwrap_key(&wrapped).await.unwrap();
        assert_eq!(&unwrapped[..], dek);
    }

    #[tokio::test]
    async fn test_in_memory_kms_multiple_keys() {
        let kms = InMemoryKms::new();
        kms.add_master_key("key1".to_string(), vec![1u8; 16]).await;
        kms.add_master_key("key2".to_string(), vec![2u8; 16]).await;

        let dek = b"test_dek";

        // Wrap with key1
        let wrapped1 = kms.wrap_key(dek, "key1").await.unwrap();
        let unwrapped1 = kms.unwrap_key(&wrapped1).await.unwrap();
        assert_eq!(&unwrapped1[..], dek);

        // Wrap with key2
        let wrapped2 = kms.wrap_key(dek, "key2").await.unwrap();
        let unwrapped2 = kms.unwrap_key(&wrapped2).await.unwrap();
        assert_eq!(&unwrapped2[..], dek);

        // Different keys should produce different ciphertexts
        assert_ne!(wrapped1, wrapped2);
    }

    #[tokio::test]
    async fn test_in_memory_kms_invalid_wrapped_key() {
        let master_key = vec![0u8; 16];
        let kms = InMemoryKms::new_with_master_key("test-key".to_string(), master_key);

        let invalid_wrapped = b"not_a_valid_wrapped_key";
        let result = kms.unwrap_key(invalid_wrapped).await;
        assert!(result.is_err());
    }
}
