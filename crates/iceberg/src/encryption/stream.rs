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

//! AGS1 stream encryption/decryption for Iceberg.
//!
//! Implements the block-based AES-GCM stream format used by Iceberg for
//! encrypting manifest lists and manifest files. The format is
//! byte-compatible with Java's `AesGcmInputStream` / `AesGcmOutputStream`.
//!
//! # AGS1 File Format
//!
//! ```text
//! ┌─────────────────────────────────────────────┐
//! │ Header (8 bytes)                            │
//! │   Magic: "AGS1" (4 bytes, ASCII)            │
//! │   Plain block size: u32 LE (4 bytes)        │
//! │     Default: 1,048,576 (1 MiB)              │
//! ├─────────────────────────────────────────────┤
//! │ Block 0                                     │
//! │   Nonce (12 bytes)                          │
//! │   Ciphertext (up to plain_block_size bytes) │
//! │   GCM Tag (16 bytes)                        │
//! ├─────────────────────────────────────────────┤
//! │ Block 1..N (same structure)                 │
//! ├─────────────────────────────────────────────┤
//! │ Final block (may be shorter)                │
//! └─────────────────────────────────────────────┘
//! ```
//!
//! Each block's AAD is: `aad_prefix || block_index (4 bytes, LE)`.

use std::ops::Range;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use super::AesGcmCipher;
use crate::io::{FileRead, FileWrite};
use crate::{Error, ErrorKind, Result};

/// Default plaintext block size (1 MiB), matching Java's `Ciphers.PLAIN_BLOCK_SIZE`.
pub const PLAIN_BLOCK_SIZE: u32 = 1024 * 1024;

/// AES-GCM nonce length in bytes.
pub const NONCE_LENGTH: u32 = 12;

/// AES-GCM authentication tag length in bytes.
pub const GCM_TAG_LENGTH: u32 = 16;

/// Cipher block size = plaintext block size + nonce + GCM tag.
pub const CIPHER_BLOCK_SIZE: u32 = PLAIN_BLOCK_SIZE + NONCE_LENGTH + GCM_TAG_LENGTH;

/// AGS1 stream magic bytes.
pub const GCM_STREAM_MAGIC: [u8; 4] = *b"AGS1";

/// AGS1 stream header length (4-byte magic + 4-byte block size).
pub const GCM_STREAM_HEADER_LENGTH: u32 = 8;

/// Minimum valid AGS1 stream length (header + one empty block).
#[cfg(test)]
pub const MIN_STREAM_LENGTH: u32 = GCM_STREAM_HEADER_LENGTH + NONCE_LENGTH + GCM_TAG_LENGTH;

/// Constructs the per-block AAD for AGS1 stream encryption.
///
/// Format: `aad_prefix || block_index (4 bytes, little-endian)`
///
/// This matches Java's `Ciphers.streamBlockAAD()`.
pub(crate) fn stream_block_aad(aad_prefix: &[u8], block_index: u32) -> Vec<u8> {
    let index_bytes = block_index.to_le_bytes();
    if aad_prefix.is_empty() {
        index_bytes.to_vec()
    } else {
        let mut aad = Vec::with_capacity(aad_prefix.len() + 4);
        aad.extend_from_slice(aad_prefix);
        aad.extend_from_slice(&index_bytes);
        aad
    }
}

/// Transparent decryption of AGS1 stream-encrypted files.
///
/// Implements the [`FileRead`] trait, providing random-access reads over
/// encrypted data. Each `read()` call determines which encrypted blocks
/// overlap the requested plaintext range, reads and decrypts them, then
/// returns the requested plaintext bytes.
///
/// # Usage
///
/// ```ignore
/// // (ignored: requires async runtime and concrete FileRead/FileWrite impls)
/// let reader = AesGcmFileRead::new(
///     inner_reader,       // Box<dyn FileRead> for the encrypted file
///     cipher,             // Arc<AesGcmCipher> with the DEK
///     aad_prefix.to_vec(),
///     encrypted_file_length,
/// )?;
///
/// // Read plaintext bytes transparently
/// let plaintext = reader.read(0..1024).await?;
/// ```
pub struct AesGcmFileRead {
    /// The underlying encrypted file reader.
    inner: Box<dyn FileRead>,
    /// The AES-GCM cipher holding the DEK.
    cipher: Arc<AesGcmCipher>,
    /// AAD prefix from the key metadata.
    aad_prefix: Box<[u8]>,
    /// Total plaintext stream size in bytes.
    plain_stream_size: u64,
    /// Total number of encrypted blocks.
    num_blocks: u64,
    /// Size of the last cipher block (may be smaller than `CIPHER_BLOCK_SIZE`).
    last_cipher_block_size: u32,
}

impl AesGcmFileRead {
    /// Creates a new `AesGcmFileRead` for decrypting an AGS1 stream.
    ///
    /// Computes the plaintext size and block layout from the encrypted file
    /// length. No I/O is performed; header validation happens implicitly
    /// when blocks are decrypted (GCM authentication will fail on corrupt data).
    ///
    /// # Arguments
    ///
    /// * `inner` - Reader for the underlying encrypted file
    /// * `cipher` - AES-GCM cipher initialized with the file's DEK
    /// * `aad_prefix` - AAD prefix from the file's `StandardKeyMetadata`
    /// * `encrypted_file_length` - Total byte length of the encrypted file
    pub fn new(
        inner: Box<dyn FileRead>,
        cipher: Arc<AesGcmCipher>,
        aad_prefix: Box<[u8]>,
        encrypted_file_length: u64,
    ) -> Result<Self> {
        let plain_stream_size = Self::calculate_plaintext_length(encrypted_file_length)?;
        let stream_length = encrypted_file_length - GCM_STREAM_HEADER_LENGTH as u64;

        if stream_length == 0 {
            return Ok(Self {
                inner,
                cipher,
                aad_prefix,
                plain_stream_size: 0,
                num_blocks: 0,
                last_cipher_block_size: 0,
            });
        }

        let num_full_blocks = stream_length / CIPHER_BLOCK_SIZE as u64;
        let cipher_bytes_in_last_block = (stream_length % CIPHER_BLOCK_SIZE as u64) as u32;
        let full_blocks_only = cipher_bytes_in_last_block == 0;

        let num_blocks = if full_blocks_only {
            num_full_blocks
        } else {
            num_full_blocks + 1
        };

        if num_blocks > u32::MAX as u64 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "AGS1 format supports at most {} blocks (~4 TiB per file), but file requires {num_blocks} blocks",
                    u32::MAX
                ),
            ));
        }

        let last_cipher_block_size = if full_blocks_only {
            CIPHER_BLOCK_SIZE
        } else {
            cipher_bytes_in_last_block
        };

        Ok(Self {
            inner,
            cipher,
            aad_prefix,
            plain_stream_size,
            num_blocks,
            last_cipher_block_size,
        })
    }

    /// Returns the plaintext stream size in bytes.
    pub fn plaintext_length(&self) -> u64 {
        self.plain_stream_size
    }

    /// Calculates the plaintext length from an encrypted file's total length.
    ///
    /// This is a static calculation matching Java's
    /// `AesGcmInputStream.calculatePlaintextLength()`.
    pub fn calculate_plaintext_length(encrypted_file_length: u64) -> Result<u64> {
        if encrypted_file_length < GCM_STREAM_HEADER_LENGTH as u64 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Encrypted file too short: {encrypted_file_length} bytes (minimum {GCM_STREAM_HEADER_LENGTH})"
                ),
            ));
        }

        let stream_length = encrypted_file_length - GCM_STREAM_HEADER_LENGTH as u64;

        if stream_length == 0 {
            return Ok(0);
        }

        let num_full_blocks = stream_length / CIPHER_BLOCK_SIZE as u64;
        let cipher_bytes_in_last_block = stream_length % CIPHER_BLOCK_SIZE as u64;
        let full_blocks_only = cipher_bytes_in_last_block == 0;

        let plain_bytes_in_last_block = if full_blocks_only {
            0
        } else {
            if cipher_bytes_in_last_block < (NONCE_LENGTH + GCM_TAG_LENGTH) as u64 {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Truncated encrypted file: last block is {} bytes (minimum {})",
                        cipher_bytes_in_last_block,
                        NONCE_LENGTH + GCM_TAG_LENGTH
                    ),
                ));
            }
            cipher_bytes_in_last_block - NONCE_LENGTH as u64 - GCM_TAG_LENGTH as u64
        };

        Ok(num_full_blocks * PLAIN_BLOCK_SIZE as u64 + plain_bytes_in_last_block)
    }

    /// Returns the encrypted byte offset for a given block index.
    fn encrypted_block_offset(block_index: u64) -> u64 {
        block_index * CIPHER_BLOCK_SIZE as u64 + GCM_STREAM_HEADER_LENGTH as u64
    }

    /// Returns the cipher block size for a given block index.
    fn cipher_block_size(&self, block_index: u64) -> u32 {
        if block_index == self.num_blocks - 1 {
            self.last_cipher_block_size
        } else {
            CIPHER_BLOCK_SIZE
        }
    }
}

#[async_trait::async_trait]
impl FileRead for AesGcmFileRead {
    /// Reads and decrypts a plaintext byte range from the encrypted AGS1 stream.
    ///
    /// The caller specifies a range in **plaintext** coordinates (e.g. "bytes 0..1024
    /// of the original file"). This method translates that into the encrypted file
    /// layout and performs the following steps:
    ///
    /// 1. **Map to blocks** — divides the plaintext range by `PLAIN_BLOCK_SIZE` to
    ///    find which encrypted blocks (`first_block..=last_block`) contain the
    ///    requested data.
    ///
    /// 2. **Single I/O read** — calculates the contiguous byte range in the
    ///    encrypted file that covers all needed blocks (including the 8-byte AGS1
    ///    header offset, 12-byte nonces, and 16-byte GCM tags) and fetches them in
    ///    one call to the inner `FileRead`.
    ///
    /// 3. **Decrypt per block** — iterates over each cipher block in the response,
    ///    decrypts it with AES-GCM using the per-block AAD (`aad_prefix || block_index`),
    ///    and slices out only the plaintext bytes that overlap the requested range.
    ///
    /// 4. **Assemble result** — concatenates the slices into a single `Bytes` buffer
    ///    matching exactly `range.end - range.start` bytes.
    ///
    /// Because each block is independently encrypted with its own nonce and AAD,
    /// arbitrary random-access reads are supported without decrypting the entire
    /// file. GCM authentication is verified per-block, so any tampering is detected
    /// at the granularity of individual blocks.
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        if range.start == range.end {
            return Ok(Bytes::new());
        }

        if range.start > range.end {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Invalid read range: start ({}) is greater than end ({})",
                    range.start, range.end
                ),
            ));
        }

        if range.end > self.plain_stream_size {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Read range {}..{} exceeds plaintext size {}",
                    range.start, range.end, self.plain_stream_size
                ),
            ));
        }

        if self.num_blocks == 0 {
            return Ok(Bytes::new());
        }

        let first_block = range.start / PLAIN_BLOCK_SIZE as u64;
        let last_block = (range.end - 1) / PLAIN_BLOCK_SIZE as u64;

        // Read all needed encrypted blocks in a single I/O call
        let encrypted_start = Self::encrypted_block_offset(first_block);
        let encrypted_end =
            Self::encrypted_block_offset(last_block) + self.cipher_block_size(last_block) as u64;

        let all_encrypted = self.inner.read(encrypted_start..encrypted_end).await?;

        // Decrypt each block and extract the requested plaintext range
        let result_len = (range.end - range.start) as usize;
        let mut result = BytesMut::with_capacity(result_len);
        let mut encrypted_offset = 0usize;

        for block_idx in first_block..=last_block {
            let block_size = self.cipher_block_size(block_idx) as usize;
            let cipher_block = &all_encrypted[encrypted_offset..encrypted_offset + block_size];
            encrypted_offset += block_size;

            let aad = stream_block_aad(&self.aad_prefix, block_idx as u32);
            let decrypted = self.cipher.decrypt(cipher_block, Some(&aad))?;

            // Calculate which slice of this decrypted block we need
            let block_plain_start = block_idx * PLAIN_BLOCK_SIZE as u64;
            let slice_start = if block_idx == first_block {
                (range.start - block_plain_start) as usize
            } else {
                0
            };
            let slice_end = if block_idx == last_block {
                (range.end - block_plain_start) as usize
            } else {
                decrypted.len()
            };

            result.extend_from_slice(&decrypted[slice_start..slice_end]);
        }

        Ok(result.freeze())
    }
}

/// Transparent encryption of AGS1 stream-encrypted files.
///
/// Implements the [`FileWrite`] trait, buffering plaintext and emitting
/// encrypted AGS1 blocks. This is the streaming write counterpart to
/// [`AesGcmFileRead`].
///
/// # Usage
///
/// ```ignore
/// // (ignored: requires async runtime and concrete FileRead/FileWrite impls)
/// let writer = AesGcmFileWrite::new(
///     inner_writer,       // Box<dyn FileWrite> for the output file
///     cipher,             // Arc<AesGcmCipher> with the DEK
///     aad_prefix.to_vec(),
/// );
///
/// writer.write(plaintext_chunk).await?;
/// writer.close().await?;
/// ```
pub struct AesGcmFileWrite {
    /// The underlying output writer.
    inner: Box<dyn FileWrite>,
    /// The AES-GCM cipher holding the DEK.
    cipher: Arc<AesGcmCipher>,
    /// AAD prefix from the key metadata.
    aad_prefix: Box<[u8]>,
    /// Plaintext buffer accumulating data before block encryption.
    buffer: Vec<u8>,
    /// Current block index for AAD construction.
    block_index: u32,
    /// Whether the AGS1 header has been written.
    header_written: bool,
    /// Whether close() has been called.
    closed: bool,
    /// Whether the writer is in a poisoned state due to a failed inner write.
    /// Once poisoned, all subsequent operations are rejected because the inner
    /// writer may have received partial data.
    poisoned: bool,
}

impl AesGcmFileWrite {
    /// Creates a new `AesGcmFileWrite` for encrypting to AGS1 format.
    ///
    /// No I/O is performed until `write()` or `close()` is called.
    pub fn new(
        inner: Box<dyn FileWrite>,
        cipher: Arc<AesGcmCipher>,
        aad_prefix: impl Into<Box<[u8]>>,
    ) -> Self {
        Self {
            inner,
            cipher,
            aad_prefix: aad_prefix.into(),
            buffer: Vec::new(),
            block_index: 0,
            header_written: false,
            closed: false,
            poisoned: false,
        }
    }

    /// Writes the AGS1 header (magic + plain block size) to the inner writer.
    async fn write_header(&mut self) -> Result<()> {
        let mut header = Vec::with_capacity(GCM_STREAM_HEADER_LENGTH as usize);
        header.extend_from_slice(&GCM_STREAM_MAGIC);
        header.extend_from_slice(&PLAIN_BLOCK_SIZE.to_le_bytes());
        if let Err(e) = self.inner.write(Bytes::from(header)).await {
            self.poisoned = true;
            return Err(e);
        }
        self.header_written = true;
        Ok(())
    }

    /// Encrypts a plaintext block and writes it to the inner writer.
    async fn encrypt_and_write_block(&mut self, block_data: &[u8]) -> Result<()> {
        let aad = stream_block_aad(&self.aad_prefix, self.block_index);
        let encrypted = self.cipher.encrypt(block_data, Some(&aad))?;
        if let Err(e) = self.inner.write(Bytes::from(encrypted)).await {
            self.poisoned = true;
            return Err(e);
        }
        self.block_index = self.block_index.checked_add(1).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "AGS1 block index overflow: file exceeds the maximum supported size (~4 TiB)",
            )
        })?;
        Ok(())
    }

    /// Encrypts the first `PLAIN_BLOCK_SIZE` bytes of the buffer in-place
    /// and drains them, avoiding a 1 MiB temporary copy.
    async fn encrypt_and_drain_block(&mut self) -> Result<()> {
        let aad = stream_block_aad(&self.aad_prefix, self.block_index);
        let encrypted = self
            .cipher
            .encrypt(&self.buffer[..PLAIN_BLOCK_SIZE as usize], Some(&aad))?;
        if let Err(e) = self.inner.write(Bytes::from(encrypted)).await {
            self.poisoned = true;
            return Err(e);
        }
        self.block_index = self.block_index.checked_add(1).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "AGS1 block index overflow: file exceeds the maximum supported size (~4 TiB)",
            )
        })?;
        self.buffer.drain(..PLAIN_BLOCK_SIZE as usize);
        Ok(())
    }
}

#[async_trait::async_trait]
impl FileWrite for AesGcmFileWrite {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        if self.closed {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Cannot write to a closed AesGcmFileWrite",
            ));
        }
        if self.poisoned {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "AesGcmFileWrite is in a poisoned state due to a previous write failure",
            ));
        }

        if !self.header_written {
            self.write_header().await?;
        }

        self.buffer.extend_from_slice(&bs);

        // Flush full blocks
        while self.buffer.len() >= PLAIN_BLOCK_SIZE as usize {
            self.encrypt_and_drain_block().await?;
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if self.closed {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "AesGcmFileWrite already closed",
            ));
        }
        if self.poisoned {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "AesGcmFileWrite is in a poisoned state due to a previous write failure",
            ));
        }

        if !self.header_written {
            self.write_header().await?;
        }

        // Write the final block if there's remaining data, or if this is an empty file
        // (block_index == 0). Skip writing a spurious empty block when the plaintext was
        // exactly block-aligned (buffer empty, blocks already written).
        if !self.buffer.is_empty() || self.block_index == 0 {
            let final_block = std::mem::take(&mut self.buffer);
            self.encrypt_and_write_block(&final_block).await?;
        }
        self.closed = true;

        self.inner.close().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Encrypts plaintext into AGS1 format for testing.
    ///
    /// Mirrors Java's `AesGcmOutputStream` behavior:
    /// - Always writes header + at least one block (even for empty input)
    /// - Full blocks are `PLAIN_BLOCK_SIZE` bytes; last block may be shorter
    fn encrypt_ags1(plaintext: &[u8], cipher: &AesGcmCipher, aad_prefix: &[u8]) -> Vec<u8> {
        let mut result = Vec::new();

        // Write header: "AGS1" + PLAIN_BLOCK_SIZE (LE)
        result.extend_from_slice(&GCM_STREAM_MAGIC);
        result.extend_from_slice(&PLAIN_BLOCK_SIZE.to_le_bytes());

        // Write blocks
        let mut offset = 0;
        let mut block_index = 0u32;

        loop {
            let remaining = plaintext.len() - offset;
            let block_size = std::cmp::min(remaining, PLAIN_BLOCK_SIZE as usize);

            // Block 0 is always written (even if empty); subsequent empty blocks are skipped
            if block_size == 0 && block_index > 0 {
                break;
            }

            let block_data = &plaintext[offset..offset + block_size];
            let aad = stream_block_aad(aad_prefix, block_index);
            let encrypted = cipher.encrypt(block_data, Some(&aad)).unwrap();
            result.extend_from_slice(&encrypted);

            offset += block_size;
            block_index += 1;

            // A partial block is always the last
            if block_size < PLAIN_BLOCK_SIZE as usize {
                break;
            }
        }

        result
    }

    /// Helper to create an AesGcmCipher from raw key bytes.
    fn make_cipher(key: &[u8]) -> AesGcmCipher {
        use super::super::SecureKey;
        let secure_key = SecureKey::new(key).unwrap();
        AesGcmCipher::new(secure_key)
    }

    /// Helper to create an in-memory FileRead from bytes.
    fn memory_reader(data: Vec<u8>) -> Box<dyn FileRead> {
        Box::new(MemoryFileRead(Bytes::from(data)))
    }

    /// Simple in-memory FileRead for tests.
    struct MemoryFileRead(Bytes);

    #[async_trait::async_trait]
    impl FileRead for MemoryFileRead {
        async fn read(&self, range: Range<u64>) -> Result<Bytes> {
            let start = range.start as usize;
            let end = range.end as usize;
            if end > self.0.len() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Range {}..{} out of bounds for {} bytes",
                        start,
                        end,
                        self.0.len()
                    ),
                ));
            }
            Ok(self.0.slice(start..end))
        }
    }

    #[tokio::test]
    async fn test_empty_file_roundtrip() {
        let key = b"0123456789abcdef";
        let aad_prefix = b"test-aad-prefix!";
        let cipher = make_cipher(key);

        let encrypted = encrypt_ags1(b"", &cipher, aad_prefix);

        // Verify minimum length: header(8) + nonce(12) + tag(16) = 36
        assert_eq!(encrypted.len(), MIN_STREAM_LENGTH as usize);

        let reader = AesGcmFileRead::new(
            memory_reader(encrypted.clone()),
            Arc::new(make_cipher(key)),
            aad_prefix.as_slice().into(),
            encrypted.len() as u64,
        )
        .unwrap();

        assert_eq!(reader.plaintext_length(), 0);

        // Reading empty range should return empty bytes
        let result = reader.read(0..0).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_small_file_roundtrip() {
        let key = b"0123456789abcdef";
        let aad_prefix = b"test-aad-prefix!";
        let plaintext = b"Hello, Iceberg encryption!";
        let cipher = make_cipher(key);

        let encrypted = encrypt_ags1(plaintext, &cipher, aad_prefix);

        let reader = AesGcmFileRead::new(
            memory_reader(encrypted.clone()),
            Arc::new(make_cipher(key)),
            aad_prefix.as_slice().into(),
            encrypted.len() as u64,
        )
        .unwrap();

        assert_eq!(reader.plaintext_length(), plaintext.len() as u64);

        // Read entire file
        let result = reader.read(0..plaintext.len() as u64).await.unwrap();
        assert_eq!(&result[..], plaintext);
    }

    #[tokio::test]
    async fn test_partial_read() {
        let key = b"0123456789abcdef";
        let aad_prefix = b"aad-prefix-here!";
        let plaintext = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        let cipher = make_cipher(key);

        let encrypted = encrypt_ags1(plaintext, &cipher, aad_prefix);

        let reader = AesGcmFileRead::new(
            memory_reader(encrypted.clone()),
            Arc::new(make_cipher(key)),
            aad_prefix.as_slice().into(),
            encrypted.len() as u64,
        )
        .unwrap();

        // Read a slice from the middle
        let result = reader.read(10..20).await.unwrap();
        assert_eq!(&result[..], &plaintext[10..20]);

        // Read first byte
        let result = reader.read(0..1).await.unwrap();
        assert_eq!(&result[..], &plaintext[0..1]);

        // Read last byte
        let last = plaintext.len() as u64;
        let result = reader.read(last - 1..last).await.unwrap();
        assert_eq!(&result[..], &plaintext[plaintext.len() - 1..]);
    }

    #[tokio::test]
    async fn test_multi_block_roundtrip() {
        let key = b"0123456789abcdef";
        let aad_prefix = b"multi-block-aad!";

        // 1.5 blocks of data
        let size = PLAIN_BLOCK_SIZE as usize + PLAIN_BLOCK_SIZE as usize / 2;
        let plaintext: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
        let cipher = make_cipher(key);

        let encrypted = encrypt_ags1(&plaintext, &cipher, aad_prefix);

        let reader = AesGcmFileRead::new(
            memory_reader(encrypted.clone()),
            Arc::new(make_cipher(key)),
            aad_prefix.as_slice().into(),
            encrypted.len() as u64,
        )
        .unwrap();

        assert_eq!(reader.plaintext_length(), plaintext.len() as u64);

        // Read entire file
        let result = reader.read(0..plaintext.len() as u64).await.unwrap();
        assert_eq!(&result[..], &plaintext[..]);
    }

    #[tokio::test]
    async fn test_cross_block_read() {
        let key = b"0123456789abcdef";
        let aad_prefix = b"cross-block-aad!";

        // 2.5 blocks of data
        let size = PLAIN_BLOCK_SIZE as usize * 2 + PLAIN_BLOCK_SIZE as usize / 2;
        let plaintext: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
        let cipher = make_cipher(key);

        let encrypted = encrypt_ags1(&plaintext, &cipher, aad_prefix);

        let reader = AesGcmFileRead::new(
            memory_reader(encrypted.clone()),
            Arc::new(make_cipher(key)),
            aad_prefix.as_slice().into(),
            encrypted.len() as u64,
        )
        .unwrap();

        // Read across block boundary (last 100 bytes of block 0 + first 100 bytes of block 1)
        let boundary = PLAIN_BLOCK_SIZE as u64;
        let result = reader.read(boundary - 100..boundary + 100).await.unwrap();
        assert_eq!(
            &result[..],
            &plaintext[(boundary - 100) as usize..(boundary + 100) as usize]
        );

        // Read across two block boundaries (spans blocks 0, 1, and 2)
        let result = reader.read(boundary - 50..boundary * 2 + 50).await.unwrap();
        assert_eq!(
            &result[..],
            &plaintext[(boundary - 50) as usize..(boundary * 2 + 50) as usize]
        );
    }

    #[tokio::test]
    async fn test_exact_block_size() {
        let key = b"0123456789abcdef";
        let aad_prefix = b"exact-block-aad!";

        // Exactly 1 block
        let plaintext: Vec<u8> = (0..PLAIN_BLOCK_SIZE as usize)
            .map(|i| (i % 256) as u8)
            .collect();
        let cipher = make_cipher(key);

        let encrypted = encrypt_ags1(&plaintext, &cipher, aad_prefix);

        let reader = AesGcmFileRead::new(
            memory_reader(encrypted.clone()),
            Arc::new(make_cipher(key)),
            aad_prefix.as_slice().into(),
            encrypted.len() as u64,
        )
        .unwrap();

        assert_eq!(reader.plaintext_length(), PLAIN_BLOCK_SIZE as u64);

        let result = reader.read(0..PLAIN_BLOCK_SIZE as u64).await.unwrap();
        assert_eq!(&result[..], &plaintext[..]);
    }

    #[tokio::test]
    async fn test_block_size_plus_one() {
        let key = b"0123456789abcdef";
        let aad_prefix = b"block-plus-one!!";

        // 1 block + 1 byte
        let size = PLAIN_BLOCK_SIZE as usize + 1;
        let plaintext: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
        let cipher = make_cipher(key);

        let encrypted = encrypt_ags1(&plaintext, &cipher, aad_prefix);

        let reader = AesGcmFileRead::new(
            memory_reader(encrypted.clone()),
            Arc::new(make_cipher(key)),
            aad_prefix.as_slice().into(),
            encrypted.len() as u64,
        )
        .unwrap();

        assert_eq!(reader.plaintext_length(), size as u64);

        // Read the last byte (in block 1)
        let result = reader.read(size as u64 - 1..size as u64).await.unwrap();
        assert_eq!(result[0], plaintext[size - 1]);

        // Read all
        let result = reader.read(0..size as u64).await.unwrap();
        assert_eq!(&result[..], &plaintext[..]);
    }

    #[tokio::test]
    async fn test_block_size_minus_one() {
        let key = b"0123456789abcdef";
        let aad_prefix = b"block-minus-one!";

        // 1 block - 1 byte
        let size = PLAIN_BLOCK_SIZE as usize - 1;
        let plaintext: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
        let cipher = make_cipher(key);

        let encrypted = encrypt_ags1(&plaintext, &cipher, aad_prefix);

        let reader = AesGcmFileRead::new(
            memory_reader(encrypted.clone()),
            Arc::new(make_cipher(key)),
            aad_prefix.as_slice().into(),
            encrypted.len() as u64,
        )
        .unwrap();

        assert_eq!(reader.plaintext_length(), size as u64);

        let result = reader.read(0..size as u64).await.unwrap();
        assert_eq!(&result[..], &plaintext[..]);
    }

    #[tokio::test]
    async fn test_wrong_aad_fails() {
        let key = b"0123456789abcdef";
        let aad_prefix = b"correct-aad-here";
        let plaintext = b"sensitive data here";
        let cipher = make_cipher(key);

        let encrypted = encrypt_ags1(plaintext, &cipher, aad_prefix);

        // Try to decrypt with wrong AAD
        let mut bad_aad = aad_prefix.to_vec();
        bad_aad[0] ^= 0xFF;

        let reader = AesGcmFileRead::new(
            memory_reader(encrypted.clone()),
            Arc::new(make_cipher(key)),
            bad_aad.as_slice().into(),
            encrypted.len() as u64,
        )
        .unwrap();

        let result = reader.read(0..plaintext.len() as u64).await;
        assert!(result.is_err(), "Decryption with wrong AAD should fail");
    }

    #[tokio::test]
    async fn test_wrong_key_fails() {
        let key = b"0123456789abcdef";
        let wrong_key = b"fedcba9876543210";
        let aad_prefix = b"test-aad-prefix!";
        let plaintext = b"sensitive data";
        let cipher = make_cipher(key);

        let encrypted = encrypt_ags1(plaintext, &cipher, aad_prefix);

        let reader = AesGcmFileRead::new(
            memory_reader(encrypted.clone()),
            Arc::new(make_cipher(wrong_key)),
            aad_prefix.as_slice().into(),
            encrypted.len() as u64,
        )
        .unwrap();

        let result = reader.read(0..plaintext.len() as u64).await;
        assert!(result.is_err(), "Decryption with wrong key should fail");
    }

    #[tokio::test]
    async fn test_out_of_bounds_read() {
        let key = b"0123456789abcdef";
        let aad_prefix = b"test-aad-prefix!";
        let plaintext = b"short data";
        let cipher = make_cipher(key);

        let encrypted = encrypt_ags1(plaintext, &cipher, aad_prefix);

        let reader = AesGcmFileRead::new(
            memory_reader(encrypted.clone()),
            Arc::new(make_cipher(key)),
            aad_prefix.as_slice().into(),
            encrypted.len() as u64,
        )
        .unwrap();

        let result = reader.read(0..plaintext.len() as u64 + 1).await;
        assert!(result.is_err(), "Reading past end should fail");
    }

    #[tokio::test]
    async fn test_calculate_plaintext_length() {
        // Empty file: header only (not valid per Java, but handled)
        assert_eq!(
            AesGcmFileRead::calculate_plaintext_length(GCM_STREAM_HEADER_LENGTH as u64).unwrap(),
            0
        );

        // Empty file with one empty block: header(8) + nonce(12) + tag(16) = 36
        assert_eq!(
            AesGcmFileRead::calculate_plaintext_length(MIN_STREAM_LENGTH as u64).unwrap(),
            0
        );

        // One full block: header(8) + cipher_block(1048604) = 1048612
        let one_full = GCM_STREAM_HEADER_LENGTH as u64 + CIPHER_BLOCK_SIZE as u64;
        assert_eq!(
            AesGcmFileRead::calculate_plaintext_length(one_full).unwrap(),
            PLAIN_BLOCK_SIZE as u64
        );

        // One full block + 1 byte: need partial second block
        // Second block = nonce(12) + 1 byte ciphertext + tag(16) = 29
        let one_full_plus_one = one_full + NONCE_LENGTH as u64 + 1 + GCM_TAG_LENGTH as u64;
        assert_eq!(
            AesGcmFileRead::calculate_plaintext_length(one_full_plus_one).unwrap(),
            PLAIN_BLOCK_SIZE as u64 + 1
        );
    }

    #[tokio::test]
    async fn test_stream_block_aad() {
        // With prefix
        let aad = stream_block_aad(b"prefix", 0);
        assert_eq!(&aad[..6], b"prefix");
        assert_eq!(&aad[6..], &0u32.to_le_bytes());

        let aad = stream_block_aad(b"prefix", 1);
        assert_eq!(&aad[..6], b"prefix");
        assert_eq!(&aad[6..], &1u32.to_le_bytes());

        // Without prefix
        let aad = stream_block_aad(b"", 42);
        assert_eq!(&aad[..], &42u32.to_le_bytes());
    }

    #[tokio::test]
    async fn test_encrypted_file_too_short() {
        let result = AesGcmFileRead::new(
            memory_reader(vec![0; 4]),
            Arc::new(make_cipher(b"0123456789abcdef")),
            [].into(),
            4,
        );
        assert!(result.is_err());
    }

    // --- AesGcmFileWrite tests ---

    /// Shared-buffer FileWrite for testing AesGcmFileWrite output.
    struct SharedMemoryWrite {
        buffer: std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
    }

    /// FileWrite that fails after a configured number of successful writes.
    struct FailingFileWrite {
        writes_before_failure: usize,
        write_count: usize,
    }

    #[async_trait::async_trait]
    impl FileWrite for FailingFileWrite {
        async fn write(&mut self, _bs: Bytes) -> Result<()> {
            if self.write_count >= self.writes_before_failure {
                return Err(Error::new(ErrorKind::Unexpected, "simulated write failure"));
            }
            self.write_count += 1;
            Ok(())
        }

        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl FileWrite for SharedMemoryWrite {
        async fn write(&mut self, bs: Bytes) -> Result<()> {
            self.buffer.lock().unwrap().extend_from_slice(&bs);
            Ok(())
        }

        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    /// Helper: one-shot encrypt through AesGcmFileWrite, return encrypted bytes.
    async fn write_through_ags1(plaintext: &[u8], key: &[u8], aad_prefix: &[u8]) -> Vec<u8> {
        let buffer = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let inner: Box<dyn FileWrite> = Box::new(SharedMemoryWrite {
            buffer: buffer.clone(),
        });
        let cipher = Arc::new(make_cipher(key));
        let mut writer = AesGcmFileWrite::new(inner, cipher, aad_prefix.to_vec());

        writer.write(Bytes::from(plaintext.to_vec())).await.unwrap();
        writer.close().await.unwrap();

        buffer.lock().unwrap().clone()
    }

    #[tokio::test]
    async fn test_write_empty_roundtrip() {
        let key = b"0123456789abcdef";
        let aad_prefix = b"test-aad-prefix!";

        let encrypted = write_through_ags1(b"", key, aad_prefix).await;

        // Should produce header + one empty encrypted block
        assert_eq!(encrypted.len(), MIN_STREAM_LENGTH as usize);

        let reader = AesGcmFileRead::new(
            memory_reader(encrypted.clone()),
            Arc::new(make_cipher(key)),
            aad_prefix.as_slice().into(),
            encrypted.len() as u64,
        )
        .unwrap();

        assert_eq!(reader.plaintext_length(), 0);
    }

    #[tokio::test]
    async fn test_write_small_roundtrip() {
        let key = b"0123456789abcdef";
        let aad_prefix = b"test-aad-prefix!";
        let plaintext = b"Hello, Iceberg encryption!";

        let encrypted = write_through_ags1(plaintext, key, aad_prefix).await;

        let reader = AesGcmFileRead::new(
            memory_reader(encrypted.clone()),
            Arc::new(make_cipher(key)),
            aad_prefix.as_slice().into(),
            encrypted.len() as u64,
        )
        .unwrap();

        assert_eq!(reader.plaintext_length(), plaintext.len() as u64);
        let result = reader.read(0..plaintext.len() as u64).await.unwrap();
        assert_eq!(&result[..], plaintext);
    }

    #[tokio::test]
    async fn test_write_multi_block_roundtrip() {
        let key = b"0123456789abcdef";
        let aad_prefix = b"multi-block-aad!";

        // 1.5 blocks of data
        let size = PLAIN_BLOCK_SIZE as usize + PLAIN_BLOCK_SIZE as usize / 2;
        let plaintext: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();

        let encrypted = write_through_ags1(&plaintext, key, aad_prefix).await;

        let reader = AesGcmFileRead::new(
            memory_reader(encrypted.clone()),
            Arc::new(make_cipher(key)),
            aad_prefix.as_slice().into(),
            encrypted.len() as u64,
        )
        .unwrap();

        assert_eq!(reader.plaintext_length(), plaintext.len() as u64);
        let result = reader.read(0..plaintext.len() as u64).await.unwrap();
        assert_eq!(&result[..], &plaintext[..]);
    }

    #[tokio::test]
    async fn test_write_cross_block_accumulation() {
        let key = b"0123456789abcdef";
        let aad_prefix = b"cross-block-aad!";

        let buffer = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let inner: Box<dyn FileWrite> = Box::new(SharedMemoryWrite {
            buffer: buffer.clone(),
        });
        let cipher = Arc::new(make_cipher(key));
        let mut writer = AesGcmFileWrite::new(inner, cipher, aad_prefix.to_vec());

        // Write 1.5 blocks in 1000-byte chunks
        let total_size = PLAIN_BLOCK_SIZE as usize + PLAIN_BLOCK_SIZE as usize / 2;
        let plaintext: Vec<u8> = (0..total_size).map(|i| (i % 256) as u8).collect();
        let chunk_size = 1000;
        for chunk in plaintext.chunks(chunk_size) {
            writer.write(Bytes::from(chunk.to_vec())).await.unwrap();
        }
        writer.close().await.unwrap();

        let encrypted = buffer.lock().unwrap().clone();

        let reader = AesGcmFileRead::new(
            memory_reader(encrypted.clone()),
            Arc::new(make_cipher(key)),
            aad_prefix.as_slice().into(),
            encrypted.len() as u64,
        )
        .unwrap();

        assert_eq!(reader.plaintext_length(), plaintext.len() as u64);
        let result = reader.read(0..plaintext.len() as u64).await.unwrap();
        assert_eq!(&result[..], &plaintext[..]);
    }

    #[tokio::test]
    async fn test_write_exact_block_size() {
        let key = b"0123456789abcdef";
        let aad_prefix = b"exact-block-aad!";

        // Exactly 1 block
        let plaintext: Vec<u8> = (0..PLAIN_BLOCK_SIZE as usize)
            .map(|i| (i % 256) as u8)
            .collect();

        let encrypted = write_through_ags1(&plaintext, key, aad_prefix).await;

        let reader = AesGcmFileRead::new(
            memory_reader(encrypted.clone()),
            Arc::new(make_cipher(key)),
            aad_prefix.as_slice().into(),
            encrypted.len() as u64,
        )
        .unwrap();

        assert_eq!(reader.plaintext_length(), PLAIN_BLOCK_SIZE as u64);
        let result = reader.read(0..PLAIN_BLOCK_SIZE as u64).await.unwrap();
        assert_eq!(&result[..], &plaintext[..]);
    }

    #[tokio::test]
    async fn test_write_block_aligned_no_spurious_empty_block() {
        let key = b"0123456789abcdef";
        let aad_prefix = b"block-align-aad!";

        // Write exactly one block of plaintext — close() should NOT add
        // a trailing empty encrypted block (28 bytes: 12-byte nonce + 16-byte tag).
        let plaintext: Vec<u8> = (0..PLAIN_BLOCK_SIZE as usize)
            .map(|i| (i % 256) as u8)
            .collect();

        let encrypted_via_writer = write_through_ags1(&plaintext, key, aad_prefix).await;
        let encrypted_via_reference = encrypt_ags1(&plaintext, &make_cipher(key), aad_prefix);

        // Both should be the same length — no extra 28-byte empty block
        assert_eq!(
            encrypted_via_writer.len(),
            encrypted_via_reference.len(),
            "Writer output should match reference encryption length (no spurious trailing block)"
        );

        // Verify roundtrip
        let reader = AesGcmFileRead::new(
            memory_reader(encrypted_via_writer.clone()),
            Arc::new(make_cipher(key)),
            aad_prefix.as_slice().into(),
            encrypted_via_writer.len() as u64,
        )
        .unwrap();

        assert_eq!(reader.plaintext_length(), PLAIN_BLOCK_SIZE as u64);
        let result = reader.read(0..PLAIN_BLOCK_SIZE as u64).await.unwrap();
        assert_eq!(&result[..], &plaintext[..]);
    }

    #[tokio::test]
    async fn test_write_two_blocks_aligned_no_spurious_empty_block() {
        let key = b"0123456789abcdef";
        let aad_prefix = b"2blk-align-aad!!";

        // Exactly 2 blocks
        let size = PLAIN_BLOCK_SIZE as usize * 2;
        let plaintext: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();

        let encrypted_via_writer = write_through_ags1(&plaintext, key, aad_prefix).await;
        let encrypted_via_reference = encrypt_ags1(&plaintext, &make_cipher(key), aad_prefix);

        assert_eq!(
            encrypted_via_writer.len(),
            encrypted_via_reference.len(),
            "Writer output should match reference encryption length (no spurious trailing block)"
        );

        let reader = AesGcmFileRead::new(
            memory_reader(encrypted_via_writer.clone()),
            Arc::new(make_cipher(key)),
            aad_prefix.as_slice().into(),
            encrypted_via_writer.len() as u64,
        )
        .unwrap();

        assert_eq!(reader.plaintext_length(), size as u64);
        let result = reader.read(0..size as u64).await.unwrap();
        assert_eq!(&result[..], &plaintext[..]);
    }

    #[tokio::test]
    async fn test_write_poisoned_after_inner_write_failure() {
        let cipher = Arc::new(make_cipher(b"0123456789abcdef"));
        // Fail on the second write (first write is the header, second is block data)
        let inner: Box<dyn FileWrite> = Box::new(FailingFileWrite {
            writes_before_failure: 1,
            write_count: 0,
        });
        let mut writer = AesGcmFileWrite::new(inner, cipher, b"aad-prefix-here!".to_vec());

        // First write triggers header (succeeds) + block encrypt+write (fails)
        let data = vec![0u8; PLAIN_BLOCK_SIZE as usize];
        let result = writer.write(Bytes::from(data)).await;
        assert!(result.is_err());

        // Subsequent write should be rejected as poisoned
        let result = writer.write(Bytes::from(b"more data".to_vec())).await;
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("poisoned"),
            "expected poisoned error"
        );

        // Close should also be rejected
        let result = writer.close().await;
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("poisoned"),
            "expected poisoned error on close"
        );
    }
}
