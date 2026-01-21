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

//! Key caching for encryption operations.
//!
//! This module provides an LRU cache with TTL for caching unwrapped encryption keys
//! to reduce the number of KMS calls.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use lru::LruCache;
use tokio::sync::RwLock;

use crate::encryption::AesGcmEncryptor;

/// A cached encryption key with expiration time.
struct CachedKey {
    /// The encryptor for this key
    encryptor: Arc<AesGcmEncryptor>,
    /// When this cache entry expires
    expires_at: Instant,
}

/// LRU cache for encryption keys with TTL support.
///
/// This cache stores unwrapped encryption keys to avoid repeated KMS calls.
/// Entries expire after a configurable TTL (typically 1 hour).
///
/// # Thread Safety
/// This cache is thread-safe and can be shared across multiple tasks using `Arc`.
pub struct KeyCache {
    /// The underlying LRU cache
    cache: Arc<RwLock<LruCache<Vec<u8>, CachedKey>>>,
    /// Time-to-live for cached keys
    ttl: Duration,
}

impl KeyCache {
    /// Creates a new key cache with the specified TTL.
    ///
    /// # Arguments
    /// * `ttl` - Time-to-live for cached keys
    ///
    /// # Default Capacity
    /// The cache can hold up to 1000 keys by default.
    pub fn new(ttl: Duration) -> Self {
        Self::with_capacity(ttl, 1000)
    }

    /// Creates a new key cache with the specified TTL and capacity.
    ///
    /// # Arguments
    /// * `ttl` - Time-to-live for cached keys
    /// * `capacity` - Maximum number of keys to cache
    pub fn with_capacity(ttl: Duration, capacity: usize) -> Self {
        Self {
            cache: Arc::new(RwLock::new(LruCache::new(
                NonZeroUsize::new(capacity).expect("Capacity must be non-zero"),
            ))),
            ttl,
        }
    }

    /// Retrieves an encryptor from the cache if it exists and hasn't expired.
    ///
    /// # Arguments
    /// * `key_metadata` - The key metadata bytes to use as cache key
    ///
    /// # Returns
    /// `Some(encryptor)` if found and not expired, `None` otherwise
    pub async fn get(&self, key_metadata: &[u8]) -> Option<Arc<AesGcmEncryptor>> {
        let mut cache = self.cache.write().await;

        if let Some(cached) = cache.get(key_metadata) {
            if cached.expires_at > Instant::now() {
                return Some(cached.encryptor.clone());
            } else {
                // Expired - remove it
                cache.pop(key_metadata);
            }
        }

        None
    }

    /// Inserts an encryptor into the cache.
    ///
    /// # Arguments
    /// * `key_metadata` - The key metadata bytes to use as cache key
    /// * `encryptor` - The encryptor to cache
    pub async fn insert(&self, key_metadata: Vec<u8>, encryptor: Arc<AesGcmEncryptor>) {
        let mut cache = self.cache.write().await;

        cache.put(key_metadata, CachedKey {
            encryptor,
            expires_at: Instant::now() + self.ttl,
        });
    }

    /// Removes all expired entries from the cache.
    ///
    /// This method should be called periodically to clean up expired entries.
    pub async fn evict_expired(&self) {
        let mut cache = self.cache.write().await;
        let now = Instant::now();

        // Collect keys to remove (LruCache doesn't support filtering in place)
        let expired_keys: Vec<Vec<u8>> = cache
            .iter()
            .filter(|(_, v)| v.expires_at <= now)
            .map(|(k, _)| k.clone())
            .collect();

        for key in expired_keys {
            cache.pop(&key);
        }
    }

    /// Clears all entries from the cache.
    pub async fn clear(&self) {
        let mut cache = self.cache.write().await;
        cache.clear();
    }

    /// Returns the number of entries in the cache (including expired ones).
    pub async fn len(&self) -> usize {
        let cache = self.cache.read().await;
        cache.len()
    }

    /// Returns whether the cache is empty.
    pub async fn is_empty(&self) -> bool {
        let cache = self.cache.read().await;
        cache.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encryption::{EncryptionAlgorithm, SecureKey};

    #[tokio::test]
    async fn test_key_cache_basic() {
        let cache = KeyCache::new(Duration::from_secs(60));
        let key_metadata = b"metadata1".to_vec();

        let secure_key = SecureKey::generate(EncryptionAlgorithm::Aes128Gcm);
        let encryptor = Arc::new(AesGcmEncryptor::new(secure_key));

        cache.insert(key_metadata.clone(), encryptor.clone()).await;

        let retrieved = cache.get(&key_metadata).await;
        assert!(retrieved.is_some());
    }

    #[tokio::test]
    async fn test_key_cache_expiration() {
        let cache = KeyCache::new(Duration::from_millis(100));
        let key_metadata = b"metadata1".to_vec();

        let secure_key = SecureKey::generate(EncryptionAlgorithm::Aes128Gcm);
        let encryptor = Arc::new(AesGcmEncryptor::new(secure_key));

        cache.insert(key_metadata.clone(), encryptor).await;

        // Should be available immediately
        assert!(cache.get(&key_metadata).await.is_some());

        // Wait for expiration
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Should be expired
        assert!(cache.get(&key_metadata).await.is_none());
    }

    #[tokio::test]
    async fn test_key_cache_evict_expired() {
        let cache = KeyCache::new(Duration::from_millis(100));

        let key1 = b"key1".to_vec();
        let key2 = b"key2".to_vec();

        let encryptor1 = Arc::new(AesGcmEncryptor::new(SecureKey::generate(
            EncryptionAlgorithm::Aes128Gcm,
        )));
        let encryptor2 = Arc::new(AesGcmEncryptor::new(SecureKey::generate(
            EncryptionAlgorithm::Aes128Gcm,
        )));

        cache.insert(key1.clone(), encryptor1).await;

        // Wait a bit before inserting key2
        tokio::time::sleep(Duration::from_millis(60)).await;
        cache.insert(key2.clone(), encryptor2).await;

        // Wait for key1 to expire
        tokio::time::sleep(Duration::from_millis(60)).await;

        assert_eq!(cache.len().await, 2);

        // Evict expired entries
        cache.evict_expired().await;

        // key1 should be gone, key2 should remain
        assert_eq!(cache.len().await, 1);
        assert!(cache.get(&key1).await.is_none());
        assert!(cache.get(&key2).await.is_some());
    }

    #[tokio::test]
    async fn test_key_cache_clear() {
        let cache = KeyCache::new(Duration::from_secs(60));

        let key1 = b"key1".to_vec();
        let key2 = b"key2".to_vec();

        let encryptor1 = Arc::new(AesGcmEncryptor::new(SecureKey::generate(
            EncryptionAlgorithm::Aes128Gcm,
        )));
        let encryptor2 = Arc::new(AesGcmEncryptor::new(SecureKey::generate(
            EncryptionAlgorithm::Aes128Gcm,
        )));

        cache.insert(key1.clone(), encryptor1).await;
        cache.insert(key2.clone(), encryptor2).await;

        assert_eq!(cache.len().await, 2);

        cache.clear().await;

        assert_eq!(cache.len().await, 0);
        assert!(cache.is_empty().await);
    }

    #[tokio::test]
    async fn test_key_cache_capacity() {
        let cache = KeyCache::with_capacity(Duration::from_secs(60), 2);

        let key1 = b"key1".to_vec();
        let key2 = b"key2".to_vec();
        let key3 = b"key3".to_vec();

        let encryptor = Arc::new(AesGcmEncryptor::new(SecureKey::generate(
            EncryptionAlgorithm::Aes128Gcm,
        )));

        cache.insert(key1.clone(), encryptor.clone()).await;
        cache.insert(key2.clone(), encryptor.clone()).await;
        cache.insert(key3.clone(), encryptor.clone()).await;

        // With capacity 2, key1 should have been evicted
        assert_eq!(cache.len().await, 2);
        assert!(cache.get(&key1).await.is_none());
        assert!(cache.get(&key2).await.is_some());
        assert!(cache.get(&key3).await.is_some());
    }

    #[tokio::test]
    async fn test_key_cache_miss() {
        let cache = KeyCache::new(Duration::from_secs(60));
        let key_metadata = b"nonexistent".to_vec();

        assert!(cache.get(&key_metadata).await.is_none());
    }
}
