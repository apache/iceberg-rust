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

//! Pluggable generation of per-file `key_metadata` on the write path.

use std::fmt::Debug;

use aes_gcm::aead::OsRng;
use aes_gcm::aead::rand_core::RngCore;
use async_trait::async_trait;

use super::crypto::{AesKeySize, SecureKey};
use super::key_metadata::StandardKeyMetadata;
use crate::Result;

/// AAD prefix length in bytes.
/// Matches Java's `TableProperties.ENCRYPTION_AAD_LENGTH_DEFAULT`.
const AAD_PREFIX_LENGTH: usize = 16;

/// Produces the per-file `key_metadata` that the writer attaches to each
/// emitted [`DataFile`] and uses to encrypt the file.
///
/// The spec defines `key_metadata` (field 131) as implementation-specific. The
/// reference *standard* encryption scheme stores a [`StandardKeyMetadata`]
/// containing a fresh plaintext DEK + AAD prefix per file, generated locally
/// without a KMS round-trip (see [`StandardFileEncryptionHandler`]). Other
/// schemes may need to call out to a KMS to wrap a freshly minted DEK, hence
/// the `async` signature.
///
/// This is the write-side counterpart of `FileKeyResolver` on the read path:
/// readers resolve `key_metadata` bytes back into a [`StandardKeyMetadata`];
/// writers produce one to embed.
///
/// [`DataFile`]: crate::spec::DataFile
#[async_trait]
pub trait FileEncryptionHandler: Debug + Send + Sync {
    /// Produce key material for the next file to be written.
    async fn next_key_metadata(&self) -> Result<StandardKeyMetadata>;
}

/// Default [`FileEncryptionHandler`] for the standard encryption scheme.
///
/// Generates a fresh random DEK and AAD prefix per file with no KMS
/// round-trip; satisfies the async signature trivially.
#[derive(Debug, Default, Clone)]
pub struct StandardFileEncryptionHandler {
    key_size: AesKeySize,
}

impl StandardFileEncryptionHandler {
    /// Creates a new handler with the given DEK size.
    pub fn new(key_size: AesKeySize) -> Self {
        Self { key_size }
    }
}

#[async_trait]
impl FileEncryptionHandler for StandardFileEncryptionHandler {
    async fn next_key_metadata(&self) -> Result<StandardKeyMetadata> {
        Ok(generate_standard_key_metadata(self.key_size))
    }
}

/// Generate a [`StandardKeyMetadata`] with a fresh random DEK and AAD prefix.
pub(crate) fn generate_standard_key_metadata(key_size: AesKeySize) -> StandardKeyMetadata {
    let dek = SecureKey::generate(key_size);
    let aad_prefix = generate_aad_prefix();
    StandardKeyMetadata::new(dek.as_bytes()).with_aad_prefix(&aad_prefix)
}

fn generate_aad_prefix() -> Box<[u8]> {
    let mut prefix = vec![0u8; AAD_PREFIX_LENGTH];
    OsRng.fill_bytes(&mut prefix);
    prefix.into_boxed_slice()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_standard_handler_emits_distinct_keys() {
        let handler = StandardFileEncryptionHandler::default();
        let a = handler.next_key_metadata().await.unwrap();
        let b = handler.next_key_metadata().await.unwrap();
        assert_ne!(
            a.encryption_key().as_bytes(),
            b.encryption_key().as_bytes(),
            "each file must get a fresh DEK"
        );
        assert_ne!(
            a.aad_prefix(),
            b.aad_prefix(),
            "each file must get a fresh AAD prefix"
        );
    }
}
