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

//! Stream encryption and decryption for metadata files.
//!
//! This module implements the AGS1 (AES-GCM Stream 1) format used for encrypting
//! manifest and manifest list files in Iceberg.

use std::num::NonZeroUsize;
use std::ops::Range;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use lru::LruCache;
use tokio::sync::RwLock;

use crate::encryption::{AesGcmEncryptor, EncryptionKeyMetadata, StandardKeyMetadata};
use crate::io::FileRead;
use crate::{Error, ErrorKind, Result};

/// Magic header for AGS1 format
const MAGIC_HEADER: &[u8] = b"AGS1";
/// Length of magic header in bytes
const MAGIC_HEADER_LEN: usize = 4;
/// Length of block size field in bytes
const BLOCK_SIZE_LEN: usize = 4;
/// Total header length
const HEADER_LEN: usize = MAGIC_HEADER_LEN + BLOCK_SIZE_LEN;
/// Nonce length for AES-GCM
const NONCE_LENGTH: usize = 12;
/// Authentication tag length for AES-GCM
const TAG_LENGTH: usize = 16;

/// Header information parsed from AGS1 stream
struct StreamHeader {
    /// Plaintext block size
    plain_block_size: usize,
    /// Ciphertext block size (includes nonce and tag)
    cipher_block_size: usize,
    /// Offset where blocks start (after header)
    blocks_start_offset: u64,
    /// Total plaintext length
    plaintext_length: u64,
    /// Total encrypted file length
    file_length: u64,
}

/// A reader that transparently decrypts AGS1-format encrypted streams.
///
/// This reader wraps an underlying `FileRead` implementation and provides
/// transparent decryption of files encrypted in AGS1 format. It supports
/// random access reads and caches decrypted blocks for performance.
///
/// # Format
///
/// The AGS1 format is:
/// ```text
/// [Header: "AGS1" + block_size (4 bytes LE)]
/// [Block 0: nonce (12B) + ciphertext + tag (16B)]
/// [Block 1: nonce (12B) + ciphertext + tag (16B)]
/// ...
/// ```
pub struct AesGcmFileRead {
    /// Underlying encrypted reader
    inner: Box<dyn FileRead>,
    /// Encryptor for decryption
    encryptor: Arc<AesGcmEncryptor>,
    /// AAD prefix from key metadata
    aad_prefix: Vec<u8>,
    /// Parsed header information
    header: StreamHeader,
    /// LRU cache of decrypted blocks
    block_cache: Arc<RwLock<LruCache<u32, Bytes>>>,
}

impl AesGcmFileRead {
    /// Calculates plaintext length from encrypted file size without needing a full reader.
    ///
    /// This is a helper for cases where you need to know the plaintext size before
    /// creating a full AesGcmFileRead. It reads just the block size from header.
    ///
    /// # Arguments
    /// * `file_read` - The encrypted file to read header from
    /// * `file_length` - The total encrypted file length
    pub async fn calculate_plaintext_length_from_file(
        file_read: &dyn FileRead,
        file_length: u64,
    ) -> Result<u64> {
        // Read header to get block size
        let header_bytes = file_read.read(0..HEADER_LEN as u64).await?;

        if header_bytes.len() < HEADER_LEN {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "File too short for AGS1 header",
            ));
        }

        // Validate magic header
        if &header_bytes[..MAGIC_HEADER_LEN] != MAGIC_HEADER {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Invalid AGS1 magic header",
            ));
        }

        // Parse block size
        let plain_block_size = u32::from_le_bytes(
            header_bytes[MAGIC_HEADER_LEN..HEADER_LEN]
                .try_into()
                .unwrap(),
        ) as usize;

        if plain_block_size == 0 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Invalid AGS1 block size: cannot be zero",
            ));
        }

        let cipher_block_size = plain_block_size + NONCE_LENGTH + TAG_LENGTH;

        Self::calculate_plaintext_length(file_length, plain_block_size, cipher_block_size)
    }

    /// Creates a new AGS1 file reader.
    ///
    /// This constructor:
    /// 1. Reads and validates the stream header
    /// 2. Calculates the total plaintext length
    /// 3. Initializes the block cache
    ///
    /// # Arguments
    /// * `inner` - The underlying reader for encrypted data
    /// * `encryptor` - The encryptor to use for decryption
    /// * `key_metadata` - The key metadata containing AAD prefix
    /// * `file_length` - The total length of the encrypted file
    pub async fn new(
        inner: Box<dyn FileRead>,
        encryptor: Arc<AesGcmEncryptor>,
        key_metadata: &StandardKeyMetadata,
        file_length: u64,
    ) -> Result<Self> {
        // Read and parse header
        let header_bytes = inner.read(0..HEADER_LEN as u64).await?;

        if header_bytes.len() < HEADER_LEN {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "File too short for AGS1 header: expected at least {} bytes, got {}",
                    HEADER_LEN,
                    header_bytes.len()
                ),
            ));
        }

        // Validate magic header
        if &header_bytes[..MAGIC_HEADER_LEN] != MAGIC_HEADER {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Invalid AGS1 magic header: expected {:?}, got {:?}",
                    MAGIC_HEADER,
                    &header_bytes[..MAGIC_HEADER_LEN]
                ),
            ));
        }

        // Parse block size
        let plain_block_size = u32::from_le_bytes(
            header_bytes[MAGIC_HEADER_LEN..HEADER_LEN]
                .try_into()
                .unwrap(),
        ) as usize;

        if plain_block_size == 0 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Invalid AGS1 block size: cannot be zero",
            ));
        }

        let cipher_block_size = plain_block_size + NONCE_LENGTH + TAG_LENGTH;

        // Calculate total plaintext length from encrypted file length
        let plaintext_length =
            Self::calculate_plaintext_length(file_length, plain_block_size, cipher_block_size)?;

        let header = StreamHeader {
            plain_block_size,
            cipher_block_size,
            blocks_start_offset: HEADER_LEN as u64,
            plaintext_length,
            file_length,
        };

        Ok(Self {
            inner,
            encryptor,
            aad_prefix: key_metadata.aad_prefix().to_vec(),
            header,
            block_cache: Arc::new(RwLock::new(LruCache::new(
                NonZeroUsize::new(16).unwrap(), // Cache 16 blocks (16MB for 1MB blocks)
            ))),
        })
    }

    /// Calculates the total plaintext length from the encrypted file size.
    ///
    /// The AGS1 format is: [header][block0][block1]...
    /// - Header: 8 bytes
    /// - Each full block: cipher_block_size bytes (plain_block_size + nonce + tag)
    /// - Last block may be partial
    fn calculate_plaintext_length(
        file_length: u64,
        plain_block_size: usize,
        cipher_block_size: usize,
    ) -> Result<u64> {
        if file_length < HEADER_LEN as u64 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "File too short: expected at least {} bytes, got {}",
                    HEADER_LEN, file_length
                ),
            ));
        }

        let encrypted_data_length = file_length - HEADER_LEN as u64;

        // Calculate number of full blocks
        let full_blocks = encrypted_data_length / cipher_block_size as u64;
        let remainder = encrypted_data_length % cipher_block_size as u64;

        // Plaintext length = full blocks * plain_block_size + last block plaintext
        let mut plaintext_length = full_blocks * plain_block_size as u64;

        // If there's a remainder, it's a partial last block
        if remainder > 0 {
            // Last block has: nonce (12) + ciphertext + tag (16)
            // So plaintext = remainder - 12 - 16
            if remainder < (NONCE_LENGTH + TAG_LENGTH) as u64 {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Invalid last block size: {} bytes (need at least {} for nonce+tag)",
                        remainder,
                        NONCE_LENGTH + TAG_LENGTH
                    ),
                ));
            }
            plaintext_length += remainder - NONCE_LENGTH as u64 - TAG_LENGTH as u64;
        }

        Ok(plaintext_length)
    }

    /// Returns the plaintext length of the file.
    pub fn plaintext_length(&self) -> u64 {
        self.header.plaintext_length
    }

    /// Maps a plaintext byte range to the block indices that contain it.
    fn range_to_blocks(&self, range: &Range<u64>) -> Range<u32> {
        let start_block = (range.start / self.header.plain_block_size as u64) as u32;
        let end_block = if range.end == u64::MAX {
            // Special case for reading to end - use a very large block number
            u32::MAX
        } else {
            ((range.end + self.header.plain_block_size as u64 - 1)
                / self.header.plain_block_size as u64) as u32
        };
        start_block..end_block
    }

    /// Reads and decrypts a single block.
    ///
    /// This method:
    /// 1. Checks the cache for the block
    /// 2. If not cached, reads the encrypted block from the file
    /// 3. Decrypts the block using the encryptor
    /// 4. Caches the decrypted block
    async fn read_block(&self, block_index: u32) -> Result<Bytes> {
        // Check cache first
        {
            let cache = self.block_cache.read().await;
            if let Some(cached) = cache.peek(&block_index) {
                return Ok(cached.clone());
            }
        }

        // Calculate offset in encrypted file
        let cipher_offset = self.header.blocks_start_offset
            + (block_index as u64 * self.header.cipher_block_size as u64);

        // Read from this offset to end of file (or at least try to get one block)
        // This handles both full blocks and partial last blocks gracefully
        let read_end = self.header.file_length;
        let ciphertext = self.inner.read(cipher_offset..read_end).await?;

        // Check if we got any data
        if ciphertext.is_empty() {
            // No more data - we're past the end of file
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Block {} is beyond end of file", block_index),
            ));
        }

        // Validate we have at least nonce + tag
        if ciphertext.len() < NONCE_LENGTH + TAG_LENGTH {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Encrypted block too short: expected at least {} bytes, got {}",
                    NONCE_LENGTH + TAG_LENGTH,
                    ciphertext.len()
                ),
            ));
        }

        // If we read more than one block worth of data, truncate to block size
        // This happens when reading from middle of file with open-ended range
        let ciphertext = if ciphertext.len() > self.header.cipher_block_size {
            ciphertext.slice(0..self.header.cipher_block_size)
        } else {
            ciphertext
        };

        // Construct AAD: aad_prefix || block_index (little-endian)
        let mut aad = BytesMut::with_capacity(self.aad_prefix.len() + 4);
        aad.extend_from_slice(&self.aad_prefix);
        aad.extend_from_slice(&block_index.to_le_bytes());

        // Decrypt the block
        let plaintext = self
            .encryptor
            .decrypt(&ciphertext, Some(&aad))
            .map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("Failed to decrypt block {}", block_index),
                )
                .with_source(e)
            })?;

        let plaintext_bytes = Bytes::from(plaintext);

        // Cache the decrypted block
        {
            let mut cache = self.block_cache.write().await;
            cache.put(block_index, plaintext_bytes.clone());
        }

        Ok(plaintext_bytes)
    }
}

#[async_trait::async_trait]
impl FileRead for AesGcmFileRead {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        // Determine which blocks we need
        let block_range = self.range_to_blocks(&range);

        // Read and decrypt all needed blocks
        let mut blocks = Vec::new();
        let mut first_error = None;
        for block_idx in block_range.clone() {
            match self.read_block(block_idx).await {
                Ok(block) => blocks.push(block),
                Err(e) => {
                    // Save first error and stop reading
                    if first_error.is_none() && blocks.is_empty() {
                        // If we haven't read any blocks yet, this is a real error
                        first_error = Some(e);
                    }
                    // Might be trying to read past end of file
                    break;
                }
            }
        }

        // If we got no blocks and had an error, return the error
        if blocks.is_empty() {
            if let Some(err) = first_error {
                return Err(err);
            }
            return Ok(Bytes::new());
        }

        // Concatenate blocks
        let mut combined = BytesMut::new();
        for block in blocks {
            combined.extend_from_slice(&block);
        }

        // Extract the requested range from the combined plaintext
        let start_in_first_block = (range.start % self.header.plain_block_size as u64) as usize;
        let total_combined = combined.len();

        // Calculate end offset, handling u64::MAX case
        let end_offset = if range.end == u64::MAX {
            // Read to end of available data
            total_combined
        } else {
            start_in_first_block + (range.end - range.start) as usize
        };

        // Clamp to available data
        let actual_end = end_offset.min(total_combined);

        if start_in_first_block >= total_combined {
            return Ok(Bytes::new());
        }

        Ok(combined.freeze().slice(start_in_first_block..actual_end))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encryption::{EncryptionAlgorithm, SecureKey};

    /// Mock FileRead implementation for testing
    struct MockEncryptedFile {
        data: Bytes,
    }

    #[async_trait::async_trait]
    impl FileRead for MockEncryptedFile {
        async fn read(&self, range: Range<u64>) -> Result<Bytes> {
            let start = range.start as usize;
            let end = range.end.min(self.data.len() as u64) as usize;
            if start >= self.data.len() {
                return Ok(Bytes::new());
            }
            Ok(self.data.slice(start..end))
        }
    }

    fn create_ags1_test_file(plaintext: &[u8], encryptor: &AesGcmEncryptor, aad_prefix: &[u8]) -> Bytes {
        let plain_block_size = 256; // Small blocks for testing
        let mut result = BytesMut::new();

        // Write header
        result.extend_from_slice(MAGIC_HEADER);
        result.extend_from_slice(&(plain_block_size as u32).to_le_bytes());

        // Write blocks
        let mut offset = 0;
        let mut block_index = 0u32;

        while offset < plaintext.len() {
            let block_end = (offset + plain_block_size).min(plaintext.len());
            let block_data = &plaintext[offset..block_end];

            // Construct AAD
            let mut aad = BytesMut::with_capacity(aad_prefix.len() + 4);
            aad.extend_from_slice(aad_prefix);
            aad.extend_from_slice(&block_index.to_le_bytes());

            // Encrypt block
            let ciphertext = encryptor.encrypt(block_data, Some(&aad)).unwrap();
            result.extend_from_slice(&ciphertext);

            offset = block_end;
            block_index += 1;
        }

        result.freeze()
    }

    #[tokio::test]
    async fn test_ags1_file_read_basic() {
        let plaintext = b"Hello, Iceberg encryption! This is a test of the AGS1 format.";
        let key = SecureKey::generate(EncryptionAlgorithm::Aes128Gcm);
        let encryptor = Arc::new(AesGcmEncryptor::new(key));
        let aad_prefix = b"test_aad";

        let encrypted_data = create_ags1_test_file(plaintext, &encryptor, aad_prefix);
        let file_length = encrypted_data.len() as u64;
        let mock_file = Box::new(MockEncryptedFile {
            data: encrypted_data,
        });

        let metadata = StandardKeyMetadata::new(vec![], aad_prefix.to_vec(), None);
        let reader = AesGcmFileRead::new(mock_file, encryptor, &metadata, file_length)
            .await
            .unwrap();

        // Read the entire file
        let decrypted = reader.read(0..plaintext.len() as u64).await.unwrap();
        assert_eq!(&decrypted[..], plaintext);
    }

    #[tokio::test]
    async fn test_ags1_file_read_partial() {
        let plaintext = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        let key = SecureKey::generate(EncryptionAlgorithm::Aes128Gcm);
        let encryptor = Arc::new(AesGcmEncryptor::new(key));
        let aad_prefix = b"test_aad";

        let encrypted_data = create_ags1_test_file(plaintext, &encryptor, aad_prefix);
        let file_length = encrypted_data.len() as u64;
        let mock_file = Box::new(MockEncryptedFile {
            data: encrypted_data,
        });

        let metadata = StandardKeyMetadata::new(vec![], aad_prefix.to_vec(), None);
        let reader = AesGcmFileRead::new(mock_file, encryptor, &metadata, file_length)
            .await
            .unwrap();

        // Read a portion of the file
        let decrypted = reader.read(5..15).await.unwrap();
        assert_eq!(&decrypted[..], &plaintext[5..15]);
    }

    #[tokio::test]
    async fn test_ags1_invalid_header() {
        let invalid_data = Bytes::from("WRONG_HEADER");
        let file_length = invalid_data.len() as u64;
        let mock_file = Box::new(MockEncryptedFile {
            data: invalid_data,
        });

        let key = SecureKey::generate(EncryptionAlgorithm::Aes128Gcm);
        let encryptor = Arc::new(AesGcmEncryptor::new(key));
        let aad_prefix = b"test_aad";
        let metadata = StandardKeyMetadata::new(vec![], aad_prefix.to_vec(), None);

        let result = AesGcmFileRead::new(mock_file, encryptor, &metadata, file_length).await;
        assert!(result.is_err());
    }
}
