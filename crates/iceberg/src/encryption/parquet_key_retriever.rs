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

//! Key retriever implementation for Parquet file decryption.
//!
//! This module provides integration between Iceberg's encryption manager
//! and Parquet's key retrieval API.

use std::sync::Arc;

use parquet::encryption::decrypt::KeyRetriever;
use parquet::errors::{ParquetError, Result as ParquetResult};

use crate::encryption::EncryptionManager;

/// Key retriever for Parquet files that integrates with Iceberg's EncryptionManager.
///
/// This retriever unwraps DEKs from the KMS using the EncryptionManager when
/// Parquet requests decryption keys during file reading.
pub struct IcebergKeyRetriever {
    encryption_manager: Arc<EncryptionManager>,
    /// Runtime handle for executing async operations in sync context
    runtime: tokio::runtime::Handle,
}

impl IcebergKeyRetriever {
    /// Creates a new Iceberg key retriever.
    ///
    /// # Arguments
    /// * `encryption_manager` - The encryption manager to use for unwrapping keys
    /// * `runtime` - Tokio runtime handle for async operations
    pub fn new(encryption_manager: Arc<EncryptionManager>, runtime: tokio::runtime::Handle) -> Self {
        Self {
            encryption_manager,
            runtime,
        }
    }
}

impl KeyRetriever for IcebergKeyRetriever {
    fn retrieve_key(&self, key_metadata: &[u8]) -> ParquetResult<Vec<u8>> {
        // The key_metadata contains Iceberg's StandardKeyMetadata in serialized form
        // We need to unwrap the DEK from the KMS using our EncryptionManager

        // Clone what we need for the async block
        let encryption_manager = self.encryption_manager.clone();
        let key_metadata = key_metadata.to_vec();
        let handle = self.runtime.clone();

        // Use spawn_blocking to avoid "cannot block within async" panics
        // This is necessary because Parquet calls this sync method from various contexts
        let result = std::thread::scope(|s| {
            s.spawn(|| {
                handle.block_on(async move {
                    encryption_manager
                        .prepare_decryption(&key_metadata)
                        .await
                })
            })
            .join()
            .unwrap()
        });

        let encryptor = result.map_err(|e| {
            ParquetError::General(format!("Failed to prepare decryption for Parquet file: {}", e))
        })?;

        // Return the raw DEK bytes that Parquet will use for decryption
        Ok(encryptor.key().as_bytes().to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encryption::{EncryptionAlgorithm, InMemoryKms, KeyManagementClient, SecureKey, StandardKeyMetadata};

    #[tokio::test]
    async fn test_key_retriever() {
        // Setup encryption manager
        let master_key = vec![0u8; 16];
        let kms = Arc::new(InMemoryKms::new_with_master_key(
            "test-key".to_string(),
            master_key,
        ));
        let encryption_manager = Arc::new(EncryptionManager::with_defaults(kms.clone()));

        // Create test key metadata
        let dek = SecureKey::generate(EncryptionAlgorithm::Aes128Gcm);
        let dek_bytes = dek.as_bytes().to_vec();
        let wrapped_key = kms.wrap_key(&dek_bytes, "test-key").await.unwrap();
        let metadata = StandardKeyMetadata::new(wrapped_key, b"aad".to_vec(), None);
        let key_metadata_bytes = metadata.serialize().unwrap();

        // Get current runtime handle (simulates how ArrowReader gets it)
        let handle = tokio::runtime::Handle::current();

        // Create key retriever
        let retriever = IcebergKeyRetriever::new(encryption_manager, handle);

        // Simulate Parquet calling retrieve_key from a blocking context
        // (Parquet's KeyRetriever trait is sync, so it would call this from a blocking thread)
        let retrieved_key = tokio::task::spawn_blocking(move || {
            retriever.retrieve_key(&key_metadata_bytes)
        })
        .await
        .unwrap()
        .unwrap();

        // Verify the retrieved key matches the original DEK
        assert_eq!(retrieved_key, dek_bytes);
    }
}
