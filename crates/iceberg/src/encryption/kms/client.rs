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

use std::sync::Arc;

use async_trait::async_trait;

use crate::encryption::SensitiveBytes;
use crate::{Error, ErrorKind, Result};

/// Result of a server-side key generation operation.
///
/// Returned by [`KeyManagementClient::generate_key`] when the KMS supports
/// atomic key generation and wrapping.
pub struct GeneratedKey {
    /// The plaintext key bytes. Zeroized on drop, redacted in Debug.
    pub key: SensitiveBytes,
    /// The wrapped (encrypted) key bytes.
    pub wrapped_key: Vec<u8>,
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
    fn supports_key_generation(&self) -> bool {
        false
    }

    /// Generate a new key and wrap it atomically on the server side.
    ///
    /// This is only supported when [`supports_key_generation`](Self::supports_key_generation)
    /// returns `true`. The default implementation returns `FeatureUnsupported`.
    async fn generate_key(&self, _wrapping_key_id: &str) -> Result<GeneratedKey> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "This KMS client does not support server-side key generation",
        ))
    }
}

#[async_trait]
impl KeyManagementClient for Arc<dyn KeyManagementClient> {
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
