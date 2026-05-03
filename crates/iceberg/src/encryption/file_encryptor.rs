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

//! File-level encryption helper for AGS1 stream-encrypted files.

use std::fmt;
use std::sync::Arc;

use super::crypto::{AesGcmCipher, SecureKey};
use super::stream::AesGcmFileWrite;
use crate::Result;
use crate::io::FileWrite;

/// Holds the encryption material for a single encrypted file.
///
/// This is the write-side counterpart to
/// [`AesGcmFileDecryptor`](super::AesGcmFileDecryptor). Created from
/// a plaintext DEK and AAD prefix, then used to wrap an output writer
/// for transparent encryption on write.
pub struct AesGcmFileEncryptor {
    cipher: Arc<AesGcmCipher>,
    aad_prefix: Box<[u8]>,
}

impl AesGcmFileEncryptor {
    /// Creates a new `AesGcmFileEncryptor` from a plaintext DEK and AAD prefix.
    pub fn new(dek: &[u8], aad_prefix: impl Into<Box<[u8]>>) -> Result<Self> {
        let key = SecureKey::new(dek)?;
        let cipher = Arc::new(AesGcmCipher::new(key));
        Ok(Self {
            cipher,
            aad_prefix: aad_prefix.into(),
        })
    }

    /// Wraps a raw output writer in an encrypting [`AesGcmFileWrite`].
    pub fn wrap_writer(&self, writer: Box<dyn FileWrite>) -> Box<dyn FileWrite> {
        Box::new(AesGcmFileWrite::new(
            writer,
            Arc::clone(&self.cipher),
            self.aad_prefix.clone(),
        ))
    }
}

impl fmt::Debug for AesGcmFileEncryptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AesGcmFileEncryptor")
            .field("aad_prefix_len", &self.aad_prefix.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use bytes::Bytes;

    use super::*;
    use crate::encryption::AesGcmFileDecryptor;
    use crate::io::FileRead;

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
    async fn test_wrap_writer_roundtrip() {
        let key = b"0123456789abcdef";
        let aad_prefix = b"test-aad-prefix!";
        let plaintext = b"Hello from file encryptor!";

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
        let result = AesGcmFileEncryptor::new(b"bad-key", b"aad".as_slice());
        assert!(result.is_err());
    }
}
