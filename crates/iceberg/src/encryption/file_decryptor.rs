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

//! File-level decryption helper for AGS1 stream-encrypted files.

use std::fmt;
use std::sync::Arc;

use super::crypto::{AesGcmCipher, SecureKey};
use super::stream::AesGcmFileRead;
use crate::Result;
use crate::io::FileRead;

/// Holds the decryption material for a single encrypted file.
///
/// Created from a plaintext DEK and AAD prefix, then used to wrap
/// an encrypted file reader for transparent decryption on read.
pub struct AesGcmFileDecryptor {
    cipher: Arc<AesGcmCipher>,
    aad_prefix: Box<[u8]>,
}

impl AesGcmFileDecryptor {
    /// Creates a new `AesGcmFileDecryptor` from a plaintext DEK and AAD prefix.
    pub fn new(dek: &[u8], aad_prefix: impl Into<Box<[u8]>>) -> Result<Self> {
        let key = SecureKey::new(dek)?;
        let cipher = Arc::new(AesGcmCipher::new(key));
        Ok(Self {
            cipher,
            aad_prefix: aad_prefix.into(),
        })
    }

    /// Wraps a raw encrypted-file reader in a decrypting [`AesGcmFileRead`].
    pub fn wrap_reader(
        &self,
        reader: Box<dyn FileRead>,
        encrypted_file_length: u64,
    ) -> Result<Box<dyn FileRead>> {
        let decrypting = AesGcmFileRead::new(
            reader,
            Arc::clone(&self.cipher),
            self.aad_prefix.clone(),
            encrypted_file_length,
        )?;
        Ok(Box::new(decrypting))
    }

    /// Calculates the plaintext length from an encrypted file's total length.
    pub fn plaintext_length(&self, encrypted_file_length: u64) -> Result<u64> {
        AesGcmFileRead::calculate_plaintext_length(encrypted_file_length)
    }
}

impl fmt::Debug for AesGcmFileDecryptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AesGcmFileDecryptor")
            .field("aad_prefix_len", &self.aad_prefix.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use bytes::Bytes;

    use super::*;
    use crate::encryption::AesGcmFileEncryptor;
    use crate::io::FileWrite;

    struct MemoryFileRead(Bytes);

    #[async_trait::async_trait]
    impl FileRead for MemoryFileRead {
        async fn read(&self, range: Range<u64>) -> Result<Bytes> {
            Ok(self.0.slice(range.start as usize..range.end as usize))
        }
    }

    struct MemoryFileWrite {
        buffer: std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
    }

    #[async_trait::async_trait]
    impl FileWrite for MemoryFileWrite {
        async fn write(&mut self, bs: Bytes) -> Result<()> {
            self.buffer.lock().unwrap().extend_from_slice(&bs);
            Ok(())
        }

        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_wrap_reader_roundtrip() {
        let key = b"0123456789abcdef";
        let aad_prefix = b"test-aad-prefix!";
        let plaintext = b"Hello from file decryptor!";

        // Encrypt via the encryptor wrapper
        let encryptor = AesGcmFileEncryptor::new(key.as_slice(), aad_prefix.as_slice()).unwrap();
        let buffer = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut writer = encryptor.wrap_writer(Box::new(MemoryFileWrite {
            buffer: buffer.clone(),
        }));
        writer.write(Bytes::from(plaintext.to_vec())).await.unwrap();
        writer.close().await.unwrap();
        let encrypted = buffer.lock().unwrap().clone();
        let encrypted_len = encrypted.len() as u64;

        // Decrypt via the decryptor wrapper
        let decryptor = AesGcmFileDecryptor::new(key.as_slice(), aad_prefix.as_slice()).unwrap();
        let reader = decryptor
            .wrap_reader(
                Box::new(MemoryFileRead(Bytes::from(encrypted))),
                encrypted_len,
            )
            .unwrap();

        let result = reader.read(0..plaintext.len() as u64).await.unwrap();
        assert_eq!(&result[..], plaintext);
    }

    #[tokio::test]
    async fn test_invalid_key_length() {
        let result = AesGcmFileDecryptor::new(b"too-short", b"aad".as_slice());
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_plaintext_length() {
        let decryptor = AesGcmFileDecryptor::new(b"0123456789abcdef", b"aad".as_slice()).unwrap();
        // header(8) + nonce(12) + 10 bytes ciphertext + tag(16) = 46
        let encrypted_len = 8 + 12 + 10 + 16;
        let plain_len = decryptor.plaintext_length(encrypted_len).unwrap();
        assert_eq!(plain_len, 10);
    }
}
