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

//! Encrypted file wrappers for InputFile / OutputFile.

use std::sync::Arc;

use bytes::Bytes;

use super::crypto::{AesGcmCipher, SecureKey};
use super::key_metadata::StandardKeyMetadata;
use super::stream::{AesGcmFileRead, AesGcmFileWrite};
use crate::Result;
use crate::io::{FileMetadata, FileRead, FileWrite, InputFile, OutputFile};

/// An AGS1 stream-encrypted input file wrapping a plain [`InputFile`].
///
/// Transparently decrypts on read.
pub struct EncryptedInputFile {
    inner: InputFile,
    key_metadata: StandardKeyMetadata,
}

impl EncryptedInputFile {
    /// Creates a new encrypted input file.
    pub fn new(inner: InputFile, key_metadata: StandardKeyMetadata) -> Self {
        Self {
            inner,
            key_metadata,
        }
    }

    /// Absolute path of the file.
    pub fn location(&self) -> &str {
        self.inner.location()
    }

    /// Check if file exists.
    pub async fn exists(&self) -> Result<bool> {
        self.inner.exists().await
    }

    /// Fetch and returns metadata of file.
    ///
    /// The returned size is the **plaintext** size.
    pub async fn metadata(&self) -> Result<FileMetadata> {
        let raw_meta = self.inner.metadata().await?;
        let plaintext_size = AesGcmFileRead::calculate_plaintext_length(raw_meta.size)?;
        Ok(FileMetadata {
            size: plaintext_size,
        })
    }

    /// Read and returns whole content of file (decrypted plaintext).
    pub async fn read(&self) -> Result<Bytes> {
        let meta = self.metadata().await?;
        let reader = self.reader().await?;
        reader.read(0..meta.size).await
    }

    /// Creates a reader that transparently decrypts on each read.
    pub async fn reader(&self) -> Result<Box<dyn FileRead>> {
        let raw_meta = self.inner.metadata().await?;
        let raw_reader = self.inner.reader().await?;
        let cipher = build_cipher(&self.key_metadata)?;
        let aad_prefix: Box<[u8]> = self.key_metadata.aad_prefix().unwrap_or_default().into();
        let decrypting = AesGcmFileRead::new(raw_reader, cipher, aad_prefix, raw_meta.size)?;
        Ok(Box::new(decrypting))
    }

    /// Returns a reference to the file's key metadata.
    pub fn key_metadata(&self) -> &StandardKeyMetadata {
        &self.key_metadata
    }

    /// Consumes self and returns the underlying plain input file.
    pub fn into_inner(self) -> InputFile {
        self.inner
    }
}

impl std::fmt::Debug for EncryptedInputFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptedInputFile")
            .field("path", &self.inner.location())
            .finish_non_exhaustive()
    }
}

/// An AGS1 stream-encrypted output file wrapping a plain [`OutputFile`].
///
/// Transparently encrypts on write.
pub struct EncryptedOutputFile {
    inner: OutputFile,
    key_metadata: StandardKeyMetadata,
}

impl EncryptedOutputFile {
    /// Creates a new encrypted output file.
    pub fn new(inner: OutputFile, key_metadata: StandardKeyMetadata) -> Self {
        Self {
            inner,
            key_metadata,
        }
    }

    /// Returns a reference to the file's key metadata.
    pub fn key_metadata(&self) -> &StandardKeyMetadata {
        &self.key_metadata
    }

    /// Absolute path of the file.
    pub fn location(&self) -> &str {
        self.inner.location()
    }

    /// Creates a writer that transparently encrypts on each write.
    pub async fn writer(&self) -> Result<Box<dyn FileWrite>> {
        let raw_writer = self.inner.writer().await?;
        let cipher = build_cipher(&self.key_metadata)?;
        let aad_prefix: Box<[u8]> = self.key_metadata.aad_prefix().unwrap_or_default().into();
        Ok(Box::new(AesGcmFileWrite::new(
            raw_writer, cipher, aad_prefix,
        )))
    }

    /// Write bytes to file (transparently encrypted).
    pub async fn write(&self, bs: Bytes) -> Result<()> {
        let mut writer = self.writer().await?;
        writer.write(bs).await?;
        writer.close().await
    }

    /// Deletes the underlying file.
    pub async fn delete(&self) -> Result<()> {
        self.inner.delete().await
    }

    /// Consumes self and returns the underlying plain output file.
    pub fn into_inner(self) -> OutputFile {
        self.inner
    }
}

impl std::fmt::Debug for EncryptedOutputFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptedOutputFile")
            .field("path", &self.inner.location())
            .finish_non_exhaustive()
    }
}

fn build_cipher(metadata: &StandardKeyMetadata) -> Result<Arc<AesGcmCipher>> {
    let key = SecureKey::new(metadata.encryption_key().as_bytes())?;
    Ok(Arc::new(AesGcmCipher::new(key)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIO;

    fn key_metadata() -> StandardKeyMetadata {
        StandardKeyMetadata::new(b"0123456789abcdef").with_aad_prefix(b"test-aad-prefix!")
    }

    #[tokio::test]
    async fn test_write_read_roundtrip() {
        let fileio = FileIO::new_with_memory();
        let path = "memory:///test/io_roundtrip.bin";
        let plaintext = b"Hello from EncryptedInputFile/EncryptedOutputFile!";

        let output = EncryptedOutputFile::new(fileio.new_output(path).unwrap(), key_metadata());
        output.write(Bytes::from(plaintext.to_vec())).await.unwrap();

        let input = EncryptedInputFile::new(fileio.new_input(path).unwrap(), key_metadata());
        let content = input.read().await.unwrap();
        assert_eq!(&content[..], plaintext);
    }

    #[tokio::test]
    async fn test_metadata_returns_plaintext_size() {
        let fileio = FileIO::new_with_memory();
        let path = "memory:///test/io_metadata.bin";
        let plaintext = b"some bytes to measure";

        let output = EncryptedOutputFile::new(fileio.new_output(path).unwrap(), key_metadata());
        output.write(Bytes::from(plaintext.to_vec())).await.unwrap();

        let raw_size = fileio.new_input(path).unwrap().metadata().await.unwrap().size;
        assert!(
            raw_size > plaintext.len() as u64,
            "encrypted file should be larger than plaintext (header + nonce + tag)"
        );

        let input = EncryptedInputFile::new(fileio.new_input(path).unwrap(), key_metadata());
        let meta = input.metadata().await.unwrap();
        assert_eq!(meta.size, plaintext.len() as u64);
    }
}
