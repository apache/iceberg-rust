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

//! Key management client trait for encryption key operations.
//!
//! Mirrors the Java `KeyManagementClient` interface from the Apache Iceberg spec.

use async_trait::async_trait;

use crate::Result;
use crate::encryption::SensitiveBytes;

/// Result of a server-side key generation operation.
///
/// Returned by [`KeyManagementClient::generate_key`] when the KMS supports
/// atomic key generation and wrapping.
pub struct GeneratedKey {
    key: SensitiveBytes,
    wrapped_key: Vec<u8>,
}

impl GeneratedKey {
    /// Creates a new `GeneratedKey` from plaintext key bytes and wrapped key bytes.
    pub fn new(key: SensitiveBytes, wrapped_key: Vec<u8>) -> Self {
        Self { key, wrapped_key }
    }

    /// Returns the plaintext key bytes. Zeroized on drop, redacted in Debug.
    pub fn key(&self) -> &SensitiveBytes {
        &self.key
    }

    /// Returns the wrapped (encrypted) key bytes.
    pub fn wrapped_key(&self) -> &[u8] {
        &self.wrapped_key
    }
}

/// Pluggable interface for key management systems (AWS KMS, Azure Key Vault, etc.).
#[async_trait]
pub trait KeyManagementClient: Send + Sync + std::fmt::Debug {
    /// Wrap (encrypt) a key using a wrapping key managed by the KMS.
    async fn wrap_key(&self, key: &[u8], wrapping_key_id: &str) -> Result<Vec<u8>>;

    /// Unwrap (decrypt) a previously wrapped key.
    async fn unwrap_key(&self, wrapped_key: &[u8], wrapping_key_id: &str)
    -> Result<SensitiveBytes>;

    /// Whether this KMS supports server-side key generation.
    ///
    /// If `true`, callers can use [`generate_key`](Self::generate_key) for atomic
    /// key generation and wrapping, which is more secure than generating a key
    /// locally and then wrapping it.
    fn supports_key_generation(&self) -> bool;

    /// Generate a new key and wrap it atomically on the server side.
    ///
    /// This is only supported when [`supports_key_generation`](Self::supports_key_generation)
    /// returns `true`.
    async fn generate_key(&self, wrapping_key_id: &str) -> Result<GeneratedKey>;
}

#[async_trait]
impl<T: AsRef<dyn KeyManagementClient> + Send + Sync + std::fmt::Debug> KeyManagementClient for T {
    async fn wrap_key(&self, key: &[u8], wrapping_key_id: &str) -> Result<Vec<u8>> {
        self.as_ref().wrap_key(key, wrapping_key_id).await
    }

    async fn unwrap_key(
        &self,
        wrapped_key: &[u8],
        wrapping_key_id: &str,
    ) -> Result<SensitiveBytes> {
        self.as_ref().unwrap_key(wrapped_key, wrapping_key_id).await
    }

    fn supports_key_generation(&self) -> bool {
        self.as_ref().supports_key_generation()
    }

    async fn generate_key(&self, wrapping_key_id: &str) -> Result<GeneratedKey> {
        self.as_ref().generate_key(wrapping_key_id).await
    }
}
