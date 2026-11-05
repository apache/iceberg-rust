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

use super::crypto::AesGcmCipher;
use super::key_metadata::StandardKeyMetadata;
use super::stream::{AesGcmFileRead, AesGcmFileWrite, MIN_STREAM_LENGTH};
use crate::Result;
use crate::error::invalid_data;
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

    /// Returns file metadata from the declared encrypted length without performing I/O.
    ///
    /// The returned size is the **plaintext** size.
    pub fn metadata(&self) -> Result<FileMetadata> {
        let plaintext_size = AesGcmFileRead::calculate_plaintext_length(self.encrypted_length()?)?;
        Ok(FileMetadata {
            size: plaintext_size,
        })
    }

    /// Read and returns whole content of file (decrypted plaintext).
    pub async fn read(&self) -> Result<Bytes> {
        let meta = self.metadata()?;
        let reader = self.reader().await?;
        reader.read(0..meta.size).await
    }

    /// Creates a reader that transparently decrypts on each read.
    pub async fn reader(&self) -> Result<Box<dyn FileRead>> {
        let encrypted_length = self.encrypted_length()?;
        let raw_reader = self.inner.reader().await?;
        let cipher = build_cipher(&self.key_metadata)?;
        let aad_prefix: Box<[u8]> = self.key_metadata.aad_prefix().unwrap_or_default().into();
        let decrypting = AesGcmFileRead::new(raw_reader, cipher, aad_prefix, encrypted_length)?;
        Ok(Box::new(decrypting))
    }

    // A storage stat would hide truncation; require the original length from key metadata.
    fn encrypted_length(&self) -> Result<u64> {
        let length = self.key_metadata.file_length().ok_or_else(|| {
            invalid_data!("AGS1 key metadata is missing the encrypted file length")
        })?;
        if length < u64::from(MIN_STREAM_LENGTH) {
            return Err(invalid_data!(
                "Invalid encrypted file length: {length} is less than {MIN_STREAM_LENGTH}"
            ));
        }
        Ok(length)
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

    /// Returns key metadata using the encrypted size returned by [`FileWrite::close`] or [`Self::write`].
    pub fn key_metadata_with_saved_file_metadata(
        &self,
        file_metadata: &FileMetadata,
    ) -> StandardKeyMetadata {
        self.key_metadata
            .clone()
            .with_file_length(file_metadata.size)
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

    /// Write bytes to the file and return its encrypted size.
    pub async fn write(&self, bs: Bytes) -> Result<FileMetadata> {
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
    let key = metadata.encryption_key().clone();
    Ok(Arc::new(AesGcmCipher::new(key)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorKind;
    use crate::encryption::stream::{
        CIPHER_BLOCK_SIZE, GCM_STREAM_HEADER_LENGTH, PLAIN_BLOCK_SIZE,
    };
    use crate::io::FileIO;

    fn key_metadata() -> StandardKeyMetadata {
        StandardKeyMetadata::try_new(b"0123456789abcdef")
            .unwrap()
            .with_aad_prefix(b"test-aad-prefix!")
    }

    #[tokio::test]
    async fn test_write_read_roundtrip() {
        let fileio = FileIO::new_with_memory();
        let path = "memory:///test/io_roundtrip.bin";
        let plaintext = b"Hello from EncryptedInputFile/EncryptedOutputFile!";

        let output = EncryptedOutputFile::new(fileio.new_output(path).unwrap(), key_metadata());
        let file_metadata = output.write(Bytes::from(plaintext.to_vec())).await.unwrap();

        let input = EncryptedInputFile::new(
            fileio.new_input(path).unwrap(),
            output.key_metadata_with_saved_file_metadata(&file_metadata),
        );
        let content = input.read().await.unwrap();
        assert_eq!(&content[..], plaintext);
    }

    #[tokio::test]
    async fn test_metadata_returns_plaintext_size() {
        let fileio = FileIO::new_with_memory();
        let path = "memory:///test/io_metadata.bin";
        let plaintext = b"some bytes to measure";

        let output = EncryptedOutputFile::new(fileio.new_output(path).unwrap(), key_metadata());
        let file_metadata = output.write(Bytes::from(plaintext.to_vec())).await.unwrap();

        let raw_size = fileio
            .new_input(path)
            .unwrap()
            .metadata()
            .await
            .unwrap()
            .size;
        assert!(
            raw_size > plaintext.len() as u64,
            "encrypted file should be larger than plaintext (header + nonce + tag)"
        );

        // A missing path proves the size comes from the key metadata rather than a stat call.
        let input = EncryptedInputFile::new(
            fileio.new_input("memory:///does-not-exist").unwrap(),
            output.key_metadata_with_saved_file_metadata(&file_metadata),
        );
        let meta = input.metadata().unwrap();
        assert_eq!(meta.size, plaintext.len() as u64);
    }

    #[tokio::test]
    async fn test_missing_file_length_is_rejected() {
        let fileio = FileIO::new_with_memory();
        let path = "memory:///test/missing_length.bin";
        let output = EncryptedOutputFile::new(fileio.new_output(path).unwrap(), key_metadata());
        output.write(Bytes::from_static(b"data")).await.unwrap();
        let input = EncryptedInputFile::new(fileio.new_input(path).unwrap(), key_metadata());

        for err in [
            input.metadata().err().unwrap(),
            input.reader().await.err().unwrap(),
            input.read().await.unwrap_err(),
        ] {
            assert_eq!(err.kind(), ErrorKind::DataInvalid);
            assert!(
                err.to_string()
                    .contains("missing the encrypted file length")
            );
        }
    }

    #[tokio::test]
    async fn test_invalid_file_length_is_rejected() {
        let fileio = FileIO::new_with_memory();
        for length in [
            0,
            u64::from(GCM_STREAM_HEADER_LENGTH),
            u64::from(MIN_STREAM_LENGTH - 1),
        ] {
            let input = EncryptedInputFile::new(
                fileio
                    .new_input("memory:///test/invalid_length.bin")
                    .unwrap(),
                key_metadata().with_file_length(length),
            );
            assert_eq!(
                input.metadata().err().unwrap().kind(),
                ErrorKind::DataInvalid
            );
            assert_eq!(
                input.reader().await.err().unwrap().kind(),
                ErrorKind::DataInvalid
            );
        }
    }

    #[tokio::test]
    async fn test_oversized_file_length_is_rejected() {
        let fileio = FileIO::new_with_memory();
        let path = "memory:///test/oversized_length.bin";
        let plaintext = Bytes::from_static(b"some bytes to measure");
        let output = EncryptedOutputFile::new(fileio.new_output(path).unwrap(), key_metadata());
        let file_metadata = output.write(plaintext.clone()).await.unwrap();

        // A declared length is trusted without a stat, so an inflated one is only caught once a
        // read runs off the end of the real file. Both a minimal overstatement and one spanning a
        // whole extra block must fail rather than silently return short plaintext.
        for excess in [1, u64::from(CIPHER_BLOCK_SIZE)] {
            let input = EncryptedInputFile::new(
                fileio.new_input(path).unwrap(),
                key_metadata().with_file_length(file_metadata.size + excess),
            );

            let inflated_size = input.metadata().unwrap().size;
            assert!(inflated_size > plaintext.len() as u64);

            assert_eq!(
                input.read().await.unwrap_err().kind(),
                ErrorKind::DataInvalid
            );

            // Not even the bytes that genuinely are on disk can be read back.
            let reader = input.reader().await.unwrap();
            assert_eq!(
                reader
                    .read(0..plaintext.len() as u64)
                    .await
                    .unwrap_err()
                    .kind(),
                ErrorKind::DataInvalid
            );
        }
    }

    #[tokio::test]
    async fn test_truncated_file_is_rejected() {
        let fileio = FileIO::new_with_memory();
        let path = "memory:///test/truncated.bin";
        let plaintext = Bytes::from(vec![42; 2 * PLAIN_BLOCK_SIZE as usize + 17]);
        let output = EncryptedOutputFile::new(fileio.new_output(path).unwrap(), key_metadata());
        let file_metadata = output.write(plaintext.clone()).await.unwrap();
        let metadata = key_metadata().with_file_length(file_metadata.size);
        let ciphertext = fileio.new_input(path).unwrap().read().await.unwrap();
        let truncated_length = (GCM_STREAM_HEADER_LENGTH + CIPHER_BLOCK_SIZE) as usize;
        fileio
            .new_output(path)
            .unwrap()
            .write(ciphertext.slice(..truncated_length))
            .await
            .unwrap();

        let input = EncryptedInputFile::new(fileio.new_input(path).unwrap(), metadata);
        assert_eq!(input.metadata().unwrap().size, plaintext.len() as u64);
        let reader = input.reader().await.unwrap();
        assert_eq!(
            reader.read(0..u64::from(PLAIN_BLOCK_SIZE)).await.unwrap(),
            plaintext.slice(..PLAIN_BLOCK_SIZE as usize)
        );
        assert_eq!(
            input.read().await.unwrap_err().kind(),
            ErrorKind::DataInvalid
        );
    }

    #[tokio::test]
    async fn test_truncated_empty_file_is_rejected() {
        let fileio = FileIO::new_with_memory();
        let path = "memory:///test/truncated_empty.bin";
        let output = EncryptedOutputFile::new(fileio.new_output(path).unwrap(), key_metadata());
        let file_metadata = output.write(Bytes::new()).await.unwrap();
        let metadata = key_metadata().with_file_length(file_metadata.size);
        let ciphertext = fileio.new_input(path).unwrap().read().await.unwrap();
        fileio
            .new_output(path)
            .unwrap()
            .write(ciphertext.slice(..GCM_STREAM_HEADER_LENGTH as usize))
            .await
            .unwrap();
        let input = EncryptedInputFile::new(fileio.new_input(path).unwrap(), metadata);
        assert_eq!(
            input.read().await.unwrap_err().kind(),
            ErrorKind::DataInvalid
        );
    }

    #[tokio::test]
    async fn test_close_returns_encrypted_size() {
        let fileio = FileIO::new_with_memory();
        let path = "memory:///test/streaming.bin";
        let output = EncryptedOutputFile::new(fileio.new_output(path).unwrap(), key_metadata());
        for plaintext in [
            Bytes::from(vec![42; PLAIN_BLOCK_SIZE as usize + 17]),
            Bytes::new(),
        ] {
            let mut writer = output.writer().await.unwrap();
            for chunk in plaintext.chunks(1024) {
                writer.write(Bytes::copy_from_slice(chunk)).await.unwrap();
            }
            let metadata = writer.close().await.unwrap();
            let size = fileio
                .new_input(path)
                .unwrap()
                .metadata()
                .await
                .unwrap()
                .size;
            assert_eq!(metadata.size, size);
        }
    }
}
