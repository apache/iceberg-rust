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

//! In-memory KMS implementation for testing and development.
//!
//! **WARNING**: This implementation is NOT suitable for production use.
//! Keys are stored in memory only and will be lost when the process exits.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;

use super::KeyManagementClient;
use crate::encryption::{AesGcmCipher, AesKeySize, SecureKey, SensitiveBytes};
use crate::error::lock_error;
use crate::{Error, ErrorKind, Result};

/// In-memory KMS for testing. Not suitable for production use.
///
/// ```
/// use iceberg::encryption::KeyManagementClient;
/// use iceberg::encryption::kms::MemoryKeyManagementClient;
///
/// # async fn example() -> iceberg::Result<()> {
/// let kms = MemoryKeyManagementClient::new();
/// kms.add_master_key("my-master-key")?;
///
/// let dek = vec![0u8; 16];
/// let wrapped = kms.wrap_key(&dek, "my-master-key").await?;
/// let unwrapped = kms.unwrap_key(&wrapped, "my-master-key").await?;
/// assert_eq!(dek.as_slice(), unwrapped.as_bytes());
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Default)]
pub struct MemoryKeyManagementClient {
    master_keys: Arc<RwLock<HashMap<String, SensitiveBytes>>>,
    master_key_size: AesKeySize,
}

impl fmt::Debug for MemoryKeyManagementClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryKeyManagementClient")
            .field("master_key_size", &self.master_key_size)
            .field("key_count", &self.key_count())
            .finish()
    }
}

impl MemoryKeyManagementClient {
    /// Creates a new in-memory KMS with 128-bit AES keys.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new in-memory KMS with the specified master key size.
    pub fn with_master_key_size(master_key_size: AesKeySize) -> Self {
        Self {
            master_keys: Arc::new(RwLock::new(HashMap::new())),
            master_key_size,
        }
    }

    /// Adds a randomly generated master key with the given ID.
    pub fn add_master_key(&self, key_id: impl Into<String>) -> Result<()> {
        let key = SecureKey::generate(self.master_key_size);
        self.insert_key(key_id.into(), SensitiveBytes::new(key.as_bytes()))
    }

    /// Adds a master key with explicit key bytes.
    ///
    /// Use this to seed the KMS with known key material, e.g. for
    /// cross-language integration tests where both Java and Rust must
    /// share the same master key bytes.
    pub fn add_master_key_bytes(
        &self,
        key_id: impl Into<String>,
        key_bytes: SensitiveBytes,
    ) -> Result<()> {
        Self::check_key_length(&key_bytes)?;
        self.insert_key(key_id.into(), key_bytes)
    }

    /// Check the key length is valid by constructing a SecureKey.
    fn check_key_length(key_bytes: &SensitiveBytes) -> Result<()> {
        SecureKey::new(key_bytes.as_bytes())?;
        Ok(())
    }

    fn insert_key(&self, key_id: String, key: SensitiveBytes) -> Result<()> {
        let mut keys = self.master_keys.write().map_err(lock_error)?;

        if keys.contains_key(&key_id) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Master key already exists: {key_id}"),
            ));
        }

        keys.insert(key_id, key);
        Ok(())
    }

    fn get_master_key(&self, key_id: &str) -> Result<SensitiveBytes> {
        let keys = self.master_keys.read().map_err(lock_error)?;

        keys.get(key_id).cloned().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Master key not found: {key_id}"),
            )
        })
    }

    /// Number of master keys stored.
    pub fn key_count(&self) -> usize {
        self.master_keys.read().map(|keys| keys.len()).unwrap_or(0)
    }

    /// Whether a master key with the given ID exists.
    pub fn has_key(&self, key_id: &str) -> bool {
        self.master_keys
            .read()
            .map(|keys| keys.contains_key(key_id))
            .unwrap_or(false)
    }
}

#[async_trait]
impl KeyManagementClient for MemoryKeyManagementClient {
    async fn wrap_key(&self, key: &[u8], wrapping_key_id: &str) -> Result<Vec<u8>> {
        let master_key_bytes = self.get_master_key(wrapping_key_id)?;
        let master_key = SecureKey::new(master_key_bytes.as_bytes())?;
        let cipher = AesGcmCipher::new(master_key);

        cipher.encrypt(key, None)
    }

    async fn unwrap_key(
        &self,
        wrapped_key: &[u8],
        wrapping_key_id: &str,
    ) -> Result<SensitiveBytes> {
        let master_key_bytes = self.get_master_key(wrapping_key_id)?;
        let master_key = SecureKey::new(master_key_bytes.as_bytes())?;
        let cipher = AesGcmCipher::new(master_key);

        Ok(SensitiveBytes::new(cipher.decrypt(wrapped_key, None)?))
    }

    fn supports_key_generation(&self) -> bool {
        false
    }

    async fn generate_key(&self, _wrapping_key_id: &str) -> Result<super::GeneratedKey> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "MemoryKeyManagementClient does not support server-side key generation",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_wrap_unwrap_roundtrip() {
        let kms = MemoryKeyManagementClient::new();
        kms.add_master_key("master-1").unwrap();
        let dek = vec![0u8; 16];

        let wrapped = kms.wrap_key(&dek, "master-1").await.unwrap();
        let unwrapped = kms.unwrap_key(&wrapped, "master-1").await.unwrap();
        assert_eq!(unwrapped.as_bytes(), dek.as_slice());
    }

    #[tokio::test]
    async fn test_wrap_unknown_key_fails() {
        let kms = MemoryKeyManagementClient::new();
        let dek = vec![0u8; 16];

        let result = kms.wrap_key(&dek, "nonexistent").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_wrong_master_key_fails_unwrap() {
        let kms = MemoryKeyManagementClient::new();
        kms.add_master_key("master-1").unwrap();
        kms.add_master_key("master-2").unwrap();
        let dek = vec![0u8; 16];

        let wrapped = kms.wrap_key(&dek, "master-1").await.unwrap();

        let result = kms.unwrap_key(&wrapped, "master-2").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_does_not_support_key_generation() {
        let kms = MemoryKeyManagementClient::new();
        assert!(!kms.supports_key_generation());

        let result = kms.generate_key("master-1").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_multiple_master_keys() {
        let kms = MemoryKeyManagementClient::new();
        kms.add_master_key("master-1").unwrap();
        kms.add_master_key("master-2").unwrap();
        let dek1 = vec![1u8; 16];
        let dek2 = vec![2u8; 16];

        let wrapped1 = kms.wrap_key(&dek1, "master-1").await.unwrap();
        let wrapped2 = kms.wrap_key(&dek2, "master-2").await.unwrap();

        let unwrapped1 = kms.unwrap_key(&wrapped1, "master-1").await.unwrap();
        let unwrapped2 = kms.unwrap_key(&wrapped2, "master-2").await.unwrap();

        assert_eq!(unwrapped1.as_bytes(), dek1.as_slice());
        assert_eq!(unwrapped2.as_bytes(), dek2.as_slice());
    }

    #[tokio::test]
    async fn test_add_master_key() {
        let kms = MemoryKeyManagementClient::new();

        kms.add_master_key("my-key").unwrap();
        assert!(kms.has_key("my-key"));
        assert_eq!(kms.key_count(), 1);

        let result = kms.add_master_key("my-key");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_add_master_key_bytes() {
        let kms = MemoryKeyManagementClient::new();
        let key_bytes = SensitiveBytes::new([42u8; 16]);

        kms.add_master_key_bytes("my-key", key_bytes).unwrap();
        assert!(kms.has_key("my-key"));

        let dek = vec![7u8; 16];
        let wrapped = kms.wrap_key(&dek, "my-key").await.unwrap();
        let unwrapped = kms.unwrap_key(&wrapped, "my-key").await.unwrap();
        assert_eq!(unwrapped.as_bytes(), dek.as_slice());
    }

    #[tokio::test]
    async fn test_add_master_key_bytes_invalid_length() {
        let kms = MemoryKeyManagementClient::new();

        let result = kms.add_master_key_bytes("my-key", SensitiveBytes::new([0u8; 7]));
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_with_master_key_size() {
        let kms = MemoryKeyManagementClient::with_master_key_size(AesKeySize::Bits256);
        kms.add_master_key("master-256").unwrap();

        let dek = vec![0u8; 16];
        let wrapped = kms.wrap_key(&dek, "master-256").await.unwrap();
        let unwrapped = kms.unwrap_key(&wrapped, "master-256").await.unwrap();
        assert_eq!(unwrapped.as_bytes(), dek.as_slice());
    }

    #[tokio::test]
    async fn test_clone_shares_state() {
        let kms1 = MemoryKeyManagementClient::new();
        let kms2 = kms1.clone();

        kms1.add_master_key("shared-key").unwrap();
        assert!(kms2.has_key("shared-key"));
    }
}
