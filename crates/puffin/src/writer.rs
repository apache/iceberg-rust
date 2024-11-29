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

use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use bytes::Bytes;
use iceberg::io::{FileWrite, OutputFile};
use iceberg::writer::file_writer::TrackWriter;
use iceberg::{Error, ErrorKind, Result};

use crate::blob::Blob;
use crate::compression::CompressionCodec;
use crate::metadata::{BlobMetadata, ByteNumber, FileMetadata, Flag};

/// Puffin writer
pub struct PuffinWriter {
    writer: TrackWriter,
    written_blobs_metadata: Vec<BlobMetadata>,
    properties: HashMap<String, String>,
    footer_compression_codec: CompressionCodec,
    flags: HashSet<Flag>,
    header_written: bool,
    is_closed: bool,
}

impl PuffinWriter {
    /// Returns a new Puffin writer
    pub async fn new(
        output_file: &OutputFile,
        properties: HashMap<String, String>,
        compress_footer: bool,
    ) -> Result<Self> {
        let mut flags = HashSet::<Flag>::new();
        let footer_compression_codec = if compress_footer {
            flags.insert(Flag::FooterPayloadCompressed);
            CompressionCodec::Lz4
        } else {
            CompressionCodec::None
        };

        let written_size = Arc::new(AtomicU64::new(0));
        let track_writer = TrackWriter::new(output_file.writer().await?, written_size);

        Ok(Self {
            writer: track_writer,
            written_blobs_metadata: Vec::new(),
            properties,
            footer_compression_codec,
            flags,
            header_written: false,
            is_closed: false,
        })
    }

    fn already_closed_err<T>() -> Result<T> {
        Err(Error::new(
            ErrorKind::Unexpected,
            "PuffinWriter is already closed",
        ))
    }

    async fn maybe_write_header(&mut self) -> Result<()> {
        if !self.header_written {
            self.writer
                .write(Bytes::copy_from_slice(&FileMetadata::MAGIC))
                .await?;
            self.header_written = true;
        }
        Ok(())
    }

    /// Adds blob to Puffin file
    pub async fn add(&mut self, blob: Blob, compression_codec: CompressionCodec) -> Result<()> {
        if self.is_closed {
            PuffinWriter::already_closed_err()
        } else {
            self.maybe_write_header().await?;

            let offset = self.writer.bytes_written();

            let compressed_data = compression_codec.compress(blob.data)?;

            self.writer
                .write(Bytes::copy_from_slice(&compressed_data))
                .await?;

            self.written_blobs_metadata.push(BlobMetadata {
                r#type: blob.r#type,
                input_fields: blob.input_fields,
                snapshot_id: blob.snapshot_id,
                sequence_number: blob.sequence_number,
                offset,
                length: compressed_data.len(),
                compression_codec,
                properties: blob.properties,
            });

            Ok(())
        }
    }

    fn footer_payload_bytes(&self) -> Result<Vec<u8>> {
        let file_metadata = FileMetadata {
            blobs: self.written_blobs_metadata.clone(),
            properties: self.properties.clone(),
        };
        let json = serde_json::to_string::<FileMetadata>(&file_metadata)?;
        let bytes = json.as_bytes();
        self.footer_compression_codec.compress(bytes.to_vec())
    }

    fn flags_bytes(&mut self) -> Vec<u8> {
        let mut flags_by_byte_number: HashMap<ByteNumber, Vec<&Flag>> = HashMap::new();
        for flag in &self.flags {
            let byte_number = flag.byte_number();
            match flags_by_byte_number.get_mut(&byte_number) {
                Some(vec) => vec.push(flag),
                None => {
                    let _ = flags_by_byte_number.insert(byte_number, vec![flag]);
                }
            };
        }

        let mut flags_bytes = Vec::new();
        for byte_number in 0..FileMetadata::FOOTER_STRUCT_FLAGS_LENGTH {
            let mut byte_flag: u8 = 0;
            for flag in flags_by_byte_number
                .get(&ByteNumber(byte_number))
                .unwrap_or(&vec![])
            {
                byte_flag |= 0x1 << flag.bit_number().0;
            }

            flags_bytes.push(byte_flag);
        }
        flags_bytes
    }

    async fn write_footer(&mut self) -> Result<()> {
        let mut footer_payload_bytes = self.footer_payload_bytes()?;
        let footer_payload_bytes_length = u32::to_le_bytes(footer_payload_bytes.len().try_into()?);

        let mut footer_bytes = Vec::new();
        footer_bytes.extend(&FileMetadata::MAGIC);
        footer_bytes.append(&mut footer_payload_bytes);
        footer_bytes.extend(footer_payload_bytes_length);
        footer_bytes.append(&mut self.flags_bytes());
        footer_bytes.extend(&FileMetadata::MAGIC);

        self.writer.write(footer_bytes.into()).await?;

        Ok(())
    }

    /// Finalizes the Puffin file
    pub async fn close(&mut self) -> Result<()> {
        if self.is_closed {
            PuffinWriter::already_closed_err()
        } else {
            self.maybe_write_header().await?;
            self.write_footer().await?;
            self.writer.close().await?;
            self.is_closed = true;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use iceberg::io::{FileIOBuilder, InputFile, OutputFile};
    use iceberg::Result;
    use tempfile::TempDir;

    use crate::blob::Blob;
    use crate::compression::CompressionCodec;
    use crate::metadata::FileMetadata;
    use crate::test_utils::{
        blob_0, blob_1, empty_footer_payload, empty_footer_payload_bytes, file_properties,
        java_empty_uncompressed_input_file, java_uncompressed_metric_input_file,
        java_zstd_compressed_metric_input_file, uncompressed_metric_file_metadata,
        zstd_compressed_metric_file_metadata,
    };
    use crate::writer::PuffinWriter;
    use crate::PuffinReader;

    #[tokio::test]
    async fn test_throws_error_if_attempt_to_add_blob_after_closing() {
        let temp_dir = TempDir::new().unwrap();

        let file_name = "temp_puffin.bin";
        let full_path = format!("{}/{}", temp_dir.path().to_str().unwrap(), file_name);

        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let output_file = file_io.new_output(full_path).unwrap();
        let mut writer = PuffinWriter::new(&output_file, HashMap::new(), false)
            .await
            .unwrap();
        writer.close().await.unwrap();

        assert_eq!(
            writer
                .add(blob_0(), CompressionCodec::None)
                .await
                .unwrap_err()
                .to_string(),
            "Unexpected => PuffinWriter is already closed",
        )
    }

    #[tokio::test]
    async fn test_throws_error_if_attempt_to_close_multiple_times() {
        let temp_dir = TempDir::new().unwrap();

        let file_name = "temp_puffin.bin";
        let full_path = format!("{}/{}", temp_dir.path().to_str().unwrap(), file_name);

        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let output_file = file_io.new_output(full_path).unwrap();
        let mut writer = PuffinWriter::new(&output_file, HashMap::new(), false)
            .await
            .unwrap();
        writer.close().await.unwrap();

        assert_eq!(
            writer.close().await.unwrap_err().to_string(),
            "Unexpected => PuffinWriter is already closed",
        )
    }

    async fn write_puffin_file(
        temp_dir: &TempDir,
        blobs: Vec<(Blob, CompressionCodec)>,
        properties: HashMap<String, String>,
    ) -> Result<OutputFile> {
        let file_io = FileIOBuilder::new_fs_io().build()?;

        let path_buf = temp_dir.path().join("temp_puffin.bin");
        let temp_path = path_buf.to_str().unwrap();
        let output_file = file_io.new_output(temp_path)?;

        let mut writer = PuffinWriter::new(&output_file, properties, false).await?;
        for (blob, compression_codec) in blobs {
            writer.add(blob, compression_codec).await?;
        }
        writer.close().await?;

        Ok(output_file)
    }

    async fn read_all_blobs_from_puffin_file(input_file: InputFile) -> Vec<Blob> {
        let mut puffin_reader = PuffinReader::new(input_file);
        let mut blobs = Vec::new();
        let blobs_metadata = puffin_reader.file_metadata().await.unwrap().clone().blobs;
        for blob_metadata in blobs_metadata {
            blobs.push(puffin_reader.blob(blob_metadata).await.unwrap());
        }
        blobs
    }

    #[tokio::test]
    async fn test_write_uncompressed_empty_file() {
        let temp_dir = TempDir::new().unwrap();

        let input_file = write_puffin_file(&temp_dir, Vec::new(), HashMap::new())
            .await
            .unwrap()
            .to_input_file();

        assert_eq!(
            FileMetadata::read(&input_file).await.unwrap(),
            empty_footer_payload()
        );

        assert_eq!(
            input_file.read().await.unwrap().len(),
            FileMetadata::MAGIC_LENGTH as usize
                // no blobs since puffin file is empty
                + FileMetadata::MAGIC_LENGTH as usize
                + empty_footer_payload_bytes().len()
                + FileMetadata::FOOTER_STRUCT_LENGTH as usize
        )
    }

    fn blobs_with_compression(
        blobs: Vec<Blob>,
        compression_codec: CompressionCodec,
    ) -> Vec<(Blob, CompressionCodec)> {
        blobs
            .into_iter()
            .map(|blob| (blob, compression_codec))
            .collect()
    }

    #[tokio::test]
    async fn test_write_uncompressed_metric_data() {
        let temp_dir = TempDir::new().unwrap();
        let blobs = vec![blob_0(), blob_1()];
        let blobs_with_compression = blobs_with_compression(blobs.clone(), CompressionCodec::None);

        let input_file = write_puffin_file(&temp_dir, blobs_with_compression, file_properties())
            .await
            .unwrap()
            .to_input_file();

        assert_eq!(
            FileMetadata::read(&input_file).await.unwrap(),
            uncompressed_metric_file_metadata()
        );

        assert_eq!(read_all_blobs_from_puffin_file(input_file).await, blobs)
    }

    #[tokio::test]
    async fn test_write_zstd_compressed_metric_data() {
        let temp_dir = TempDir::new().unwrap();
        let blobs = vec![blob_0(), blob_1()];
        let blobs_with_compression = blobs_with_compression(blobs.clone(), CompressionCodec::Zstd);

        let input_file = write_puffin_file(&temp_dir, blobs_with_compression, file_properties())
            .await
            .unwrap()
            .to_input_file();

        assert_eq!(
            FileMetadata::read(&input_file).await.unwrap(),
            zstd_compressed_metric_file_metadata()
        );

        assert_eq!(read_all_blobs_from_puffin_file(input_file).await, blobs)
    }

    #[tokio::test]
    async fn test_write_lz4_compressed_metric_data() {
        let temp_dir = TempDir::new().unwrap();
        let blobs = vec![blob_0(), blob_1()];
        let blobs_with_compression = blobs_with_compression(blobs.clone(), CompressionCodec::Lz4);

        assert_eq!(
            write_puffin_file(&temp_dir, blobs_with_compression, file_properties())
                .await
                .unwrap_err()
                .to_string(),
            "FeatureUnsupported => LZ4 compression is not supported currently"
        );
    }

    async fn get_file_as_byte_vec(input_file: InputFile) -> Vec<u8> {
        input_file.read().await.unwrap().to_vec()
    }

    async fn assert_files_are_bit_identical(actual: OutputFile, expected: InputFile) {
        let actual_bytes = get_file_as_byte_vec(actual.to_input_file()).await;
        let expected_bytes = get_file_as_byte_vec(expected).await;
        assert_eq!(actual_bytes, expected_bytes);
    }

    #[tokio::test]
    async fn test_uncompressed_empty_puffin_file_is_bit_identical_to_java_generated_file() {
        let temp_dir = TempDir::new().unwrap();

        assert_files_are_bit_identical(
            write_puffin_file(&temp_dir, Vec::new(), HashMap::new())
                .await
                .unwrap(),
            java_empty_uncompressed_input_file(),
        )
        .await
    }

    #[tokio::test]
    async fn test_uncompressed_metric_data_is_bit_identical_to_java_generated_file() {
        let temp_dir = TempDir::new().unwrap();
        let blobs = vec![blob_0(), blob_1()];
        let blobs_with_compression = blobs_with_compression(blobs, CompressionCodec::None);

        assert_files_are_bit_identical(
            write_puffin_file(&temp_dir, blobs_with_compression, file_properties())
                .await
                .unwrap(),
            java_uncompressed_metric_input_file(),
        )
        .await
    }

    #[tokio::test]
    async fn test_zstd_compressed_metric_data_is_bit_identical_to_java_generated_file() {
        let temp_dir = TempDir::new().unwrap();
        let blobs = vec![blob_0(), blob_1()];
        let blobs_with_compression = blobs_with_compression(blobs, CompressionCodec::Zstd);

        assert_files_are_bit_identical(
            write_puffin_file(&temp_dir, blobs_with_compression, file_properties())
                .await
                .unwrap(),
            java_zstd_compressed_metric_input_file(),
        )
        .await
    }
}
