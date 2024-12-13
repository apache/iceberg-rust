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

use bytes::Bytes;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

use crate::io::{FileRead, InputFile};
use crate::puffin::compression::CompressionCodec;
use crate::{Error, ErrorKind, Result};

/// Human-readable identification of the application writing the file, along with its version.
/// Example: "Trino version 381"
pub const CREATED_BY_PROPERTY: &str = "created-by";

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
/// Metadata about a blob.
/// For more information, see: https://iceberg.apache.org/puffin-spec/#blobmetadata
pub struct BlobMetadata {
    /// See blob types: https://iceberg.apache.org/puffin-spec/#blob-types
    pub(crate) r#type: String,
    /// List of field IDs the blob was computed for; the order of items is used to compute sketches stored in the blob.
    #[serde(rename = "fields")]
    pub(crate) input_fields: Vec<i32>,
    /// ID of the Iceberg table's snapshot the blob was computed from
    pub(crate) snapshot_id: i64,
    /// Sequence number of the Iceberg table's snapshot the blob was computed from
    pub(crate) sequence_number: i64,
    /// The offset in the file where the blob contents start
    pub(crate) offset: u64,
    /// The length of the blob stored in the file (after compression, if compressed)
    pub(crate) length: usize,
    /// The compression codec used to compress the data
    #[serde(skip_serializing_if = "CompressionCodec::is_none")]
    #[serde(default)]
    pub(crate) compression_codec: CompressionCodec,
    /// Arbitrary meta-information about the blob
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(default)]
    pub(crate) properties: HashMap<String, String>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) enum Flag {
    FooterPayloadCompressed,
}

#[derive(PartialEq, Eq, Hash)]
pub(crate) struct ByteNumber(pub u8);

#[derive(PartialEq, Eq, Hash)]
pub(crate) struct BitNumber(pub u8);

static FLAGS_BY_BYTE_AND_BIT: Lazy<HashMap<(ByteNumber, BitNumber), Flag>> = Lazy::new(|| {
    let mut m = HashMap::new();
    m.insert(
        (
            Flag::FooterPayloadCompressed.byte_number(),
            Flag::FooterPayloadCompressed.bit_number(),
        ),
        Flag::FooterPayloadCompressed,
    );
    m
});

impl Flag {
    pub(crate) fn byte_number(&self) -> ByteNumber {
        match self {
            Flag::FooterPayloadCompressed => ByteNumber(0),
        }
    }

    pub(crate) fn bit_number(&self) -> BitNumber {
        match self {
            Flag::FooterPayloadCompressed => BitNumber(0),
        }
    }

    fn from(byte_and_bit: &(ByteNumber, BitNumber)) -> Option<Flag> {
        FLAGS_BY_BYTE_AND_BIT.get(byte_and_bit).cloned()
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
/// Metadata about a puffin file.
/// For more information, see: https://iceberg.apache.org/puffin-spec/#filemetadata
pub struct FileMetadata {
    /// Metadata about blobs in file
    pub blobs: Vec<BlobMetadata>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(default)]
    /// Arbitrary meta-information, like writer identification/version.
    pub properties: HashMap<String, String>,
}

impl FileMetadata {
    pub(crate) const MAGIC_LENGTH: u8 = 4;
    pub(crate) const MAGIC: [u8; FileMetadata::MAGIC_LENGTH as usize] = [0x50, 0x46, 0x41, 0x31];

    // We use the term FOOTER_STRUCT to refer to the fixed-length portion of the Footer, as illustrated below.
    //
    //                        Footer
    //                          |
    //  -------------------------------------------------
    // |                                                 |
    // Magic FooterPayload FooterPayloadLength Flags Magic
    //                     |                             |
    //                      -----------------------------
    //                                    |
    //                              FOOTER_STRUCT

    const FOOTER_STRUCT_PAYLOAD_LENGTH_OFFSET: u8 = 0;
    const FOOTER_STRUCT_PAYLOAD_LENGTH_LENGTH: u8 = 4;
    const FOOTER_STRUCT_FLAGS_OFFSET: u8 = FileMetadata::FOOTER_STRUCT_PAYLOAD_LENGTH_OFFSET
        + FileMetadata::FOOTER_STRUCT_PAYLOAD_LENGTH_LENGTH;
    pub(crate) const FOOTER_STRUCT_FLAGS_LENGTH: u8 = 4;
    const FOOTER_STRUCT_MAGIC_OFFSET: u8 =
        FileMetadata::FOOTER_STRUCT_FLAGS_OFFSET + FileMetadata::FOOTER_STRUCT_FLAGS_LENGTH;
    pub(crate) const FOOTER_STRUCT_LENGTH: u8 =
        FileMetadata::FOOTER_STRUCT_MAGIC_OFFSET + FileMetadata::MAGIC_LENGTH;

    fn check_magic(bytes: &[u8]) -> Result<()> {
        if bytes != FileMetadata::MAGIC {
            Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Bad magic value: {:?} should be {:?}",
                    bytes,
                    FileMetadata::MAGIC
                ),
            ))
        } else {
            Ok(())
        }
    }

    async fn read_footer_payload_length(
        file_read: &dyn FileRead,
        input_file_length: u64,
    ) -> Result<u32> {
        let start = input_file_length - u64::from(FileMetadata::FOOTER_STRUCT_LENGTH);
        let end = start + u64::from(FileMetadata::FOOTER_STRUCT_PAYLOAD_LENGTH_LENGTH);
        let footer_payload_length_bytes = file_read.read(start..end).await?;
        let mut buf = [0; 4];
        buf.copy_from_slice(&footer_payload_length_bytes);
        let footer_payload_length = u32::from_le_bytes(buf);
        Ok(footer_payload_length)
    }

    async fn read_footer_bytes(
        file_read: &dyn FileRead,
        input_file_length: u64,
        footer_payload_length: u32,
    ) -> Result<Bytes> {
        let footer_length = u64::from(footer_payload_length)
            + u64::from(FileMetadata::FOOTER_STRUCT_LENGTH)
            + u64::from(FileMetadata::MAGIC_LENGTH);
        let start = input_file_length - footer_length;
        let end = input_file_length;
        file_read.read(start..end).await
    }

    fn err_out_of_bounds<T>() -> Result<T> {
        Err(Error::new(
            ErrorKind::DataInvalid,
            "Index range is out of bounds.",
        ))
    }

    fn decode_flags(footer_bytes: &[u8]) -> Result<HashSet<Flag>> {
        let mut flags = HashSet::new();
        for byte_number in 0..FileMetadata::FOOTER_STRUCT_FLAGS_LENGTH {
            let byte_offset = footer_bytes.len()
                - usize::from(FileMetadata::MAGIC_LENGTH)
                - usize::from(FileMetadata::FOOTER_STRUCT_FLAGS_LENGTH)
                + usize::from(byte_number);

            let mut flag_byte = match footer_bytes.get(byte_offset) {
                None => FileMetadata::err_out_of_bounds(),
                Some(byte) => Ok(*byte),
            }?;
            let mut bit_number = 0;
            while flag_byte != 0 {
                if flag_byte & 0x1 != 0 {
                    match Flag::from(&(ByteNumber(byte_number), BitNumber(bit_number))) {
                        Some(flag) => flags.insert(flag),
                        None => {
                            return Err(Error::new(
                                ErrorKind::DataInvalid,
                                format!(
                                    "Unknown flag byte {} and bit {} combination",
                                    byte_number, bit_number
                                ),
                            ))
                        }
                    };
                }
                flag_byte >>= 1;
                bit_number += 1;
            }
        }
        Ok(flags)
    }

    fn extract_footer_payload_as_str(
        footer_bytes: &[u8],
        footer_payload_length: u32,
    ) -> Result<String> {
        let flags = FileMetadata::decode_flags(footer_bytes)?;
        let footer_compression_codec = if flags.contains(&Flag::FooterPayloadCompressed) {
            CompressionCodec::Lz4
        } else {
            CompressionCodec::None
        };

        let start_offset = usize::from(FileMetadata::MAGIC_LENGTH);
        let end_offset =
            usize::from(FileMetadata::MAGIC_LENGTH) + usize::try_from(footer_payload_length)?;
        let footer_payload_bytes = match footer_bytes.get(start_offset..end_offset) {
            None => FileMetadata::err_out_of_bounds(),
            Some(data) => Ok(data),
        }?;
        let decompressed_footer_payload_bytes =
            footer_compression_codec.decompress(footer_payload_bytes.into())?;

        match String::from_utf8(decompressed_footer_payload_bytes) {
            Err(src) => Err(Error::new(
                ErrorKind::DataInvalid,
                "Footer is not a valid UTF-8 string",
            )
            .with_source(src)),
            Ok(str) => Ok(str),
        }
    }

    fn from_json_str(string: &str) -> Result<FileMetadata> {
        match serde_json::from_str::<FileMetadata>(string) {
            Ok(file_metadata) => Ok(file_metadata),
            Err(src) => Err(
                Error::new(ErrorKind::DataInvalid, "Given string is not valid JSON")
                    .with_source(src),
            ),
        }
    }

    #[rustfmt::skip]
    /// Returns the file metadata about a Puffin file
    pub(crate) async fn read(input_file: &InputFile) -> Result<FileMetadata> {
        let file_read = input_file.reader().await?;

        let first_four_bytes = file_read.read(0..FileMetadata::MAGIC_LENGTH.into()).await?;
        FileMetadata::check_magic(&first_four_bytes)?;

        let input_file_length = input_file.metadata().await?.size;
        let footer_payload_length = FileMetadata::read_footer_payload_length(&file_read, input_file_length).await?;
        let footer_bytes = FileMetadata::read_footer_bytes(&file_read, input_file_length, footer_payload_length).await?;

        let magic_length = usize::from(FileMetadata::MAGIC_LENGTH);
        FileMetadata::check_magic(&footer_bytes[..magic_length])?;                      // first four bytes of footer
        FileMetadata::check_magic(&footer_bytes[footer_bytes.len() - magic_length..])?; // last four bytes of footer

        let footer_payload_str = FileMetadata::extract_footer_payload_as_str(&footer_bytes, footer_payload_length)?;
        FileMetadata::from_json_str(&footer_payload_str)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytes::Bytes;
    use tempfile::TempDir;

    use crate::io::{FileIOBuilder, InputFile};
    use crate::puffin::metadata::{BlobMetadata, CompressionCodec, FileMetadata};
    use crate::puffin::test_utils::{
        empty_footer_payload, empty_footer_payload_bytes, empty_footer_payload_bytes_length_bytes,
        rust_empty_uncompressed_input_file, rust_uncompressed_metric_input_file,
        rust_zstd_compressed_metric_input_file, uncompressed_metric_file_metadata,
        zstd_compressed_metric_file_metadata,
    };

    const INVALID_MAGIC_VALUE: [u8; 4] = [80, 70, 65, 0];

    async fn input_file_with_bytes(temp_dir: &TempDir, slice: &[u8]) -> InputFile {
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();

        let path_buf = temp_dir.path().join("abc.puffin");
        let temp_path = path_buf.to_str().unwrap();
        let output_file = file_io.new_output(temp_path).unwrap();

        output_file
            .write(Bytes::copy_from_slice(slice))
            .await
            .unwrap();

        output_file.to_input_file()
    }

    async fn input_file_with_payload(temp_dir: &TempDir, payload_str: &str) -> InputFile {
        let payload_bytes = payload_str.as_bytes();

        let mut bytes = vec![];
        bytes.extend(FileMetadata::MAGIC.to_vec());
        bytes.extend(FileMetadata::MAGIC.to_vec());
        bytes.extend(payload_bytes);
        bytes.extend(u32::to_le_bytes(payload_bytes.len() as u32));
        bytes.extend(vec![0, 0, 0, 0]);
        bytes.extend(FileMetadata::MAGIC);

        input_file_with_bytes(temp_dir, &bytes).await
    }

    #[tokio::test]
    async fn test_file_starting_with_invalid_magic_returns_error() {
        let temp_dir = TempDir::new().unwrap();

        let mut bytes = vec![];
        bytes.extend(INVALID_MAGIC_VALUE.to_vec());
        bytes.extend(FileMetadata::MAGIC.to_vec());
        bytes.extend(empty_footer_payload_bytes());
        bytes.extend(empty_footer_payload_bytes_length_bytes());
        bytes.extend(vec![0, 0, 0, 0]);
        bytes.extend(FileMetadata::MAGIC);

        let input_file = input_file_with_bytes(&temp_dir, &bytes).await;

        assert_eq!(
            FileMetadata::read(&input_file)
                .await
                .unwrap_err()
                .to_string(),
            "DataInvalid => Bad magic value: [80, 70, 65, 0] should be [80, 70, 65, 49]",
        )
    }

    #[tokio::test]
    async fn test_file_with_invalid_magic_at_start_of_footer_returns_error() {
        let temp_dir = TempDir::new().unwrap();

        let mut bytes = vec![];
        bytes.extend(FileMetadata::MAGIC.to_vec());
        bytes.extend(INVALID_MAGIC_VALUE.to_vec());
        bytes.extend(empty_footer_payload_bytes());
        bytes.extend(empty_footer_payload_bytes_length_bytes());
        bytes.extend(vec![0, 0, 0, 0]);
        bytes.extend(FileMetadata::MAGIC);

        let input_file = input_file_with_bytes(&temp_dir, &bytes).await;

        assert_eq!(
            FileMetadata::read(&input_file)
                .await
                .unwrap_err()
                .to_string(),
            "DataInvalid => Bad magic value: [80, 70, 65, 0] should be [80, 70, 65, 49]",
        )
    }

    #[tokio::test]
    async fn test_file_ending_with_invalid_magic_returns_error() {
        let temp_dir = TempDir::new().unwrap();

        let mut bytes = vec![];
        bytes.extend(FileMetadata::MAGIC.to_vec());
        bytes.extend(FileMetadata::MAGIC.to_vec());
        bytes.extend(empty_footer_payload_bytes());
        bytes.extend(empty_footer_payload_bytes_length_bytes());
        bytes.extend(vec![0, 0, 0, 0]);
        bytes.extend(INVALID_MAGIC_VALUE);

        let input_file = input_file_with_bytes(&temp_dir, &bytes).await;

        assert_eq!(
            FileMetadata::read(&input_file)
                .await
                .unwrap_err()
                .to_string(),
            "DataInvalid => Bad magic value: [80, 70, 65, 0] should be [80, 70, 65, 49]",
        )
    }

    #[tokio::test]
    async fn test_encoded_payload_length_larger_than_actual_payload_length_returns_error() {
        let temp_dir = TempDir::new().unwrap();

        let mut bytes = vec![];
        bytes.extend(FileMetadata::MAGIC.to_vec());
        bytes.extend(FileMetadata::MAGIC.to_vec());
        bytes.extend(empty_footer_payload_bytes());
        bytes.extend(u32::to_le_bytes(
            empty_footer_payload_bytes().len() as u32 + 1,
        ));
        bytes.extend(vec![0, 0, 0, 0]);
        bytes.extend(FileMetadata::MAGIC.to_vec());

        let input_file = input_file_with_bytes(&temp_dir, &bytes).await;

        assert_eq!(
            FileMetadata::read(&input_file)
                .await
                .unwrap_err()
                .to_string(),
            "DataInvalid => Bad magic value: [49, 80, 70, 65] should be [80, 70, 65, 49]",
        )
    }

    #[tokio::test]
    async fn test_encoded_payload_length_smaller_than_actual_payload_length_returns_error() {
        let temp_dir = TempDir::new().unwrap();

        let mut bytes = vec![];
        bytes.extend(FileMetadata::MAGIC.to_vec());
        bytes.extend(FileMetadata::MAGIC.to_vec());
        bytes.extend(empty_footer_payload_bytes());
        bytes.extend(u32::to_le_bytes(
            empty_footer_payload_bytes().len() as u32 - 1,
        ));
        bytes.extend(vec![0, 0, 0, 0]);
        bytes.extend(FileMetadata::MAGIC.to_vec());

        let input_file = input_file_with_bytes(&temp_dir, &bytes).await;

        assert_eq!(
            FileMetadata::read(&input_file)
                .await
                .unwrap_err()
                .to_string(),
            "DataInvalid => Bad magic value: [70, 65, 49, 123] should be [80, 70, 65, 49]",
        )
    }

    #[tokio::test]
    async fn test_lz4_compressed_footer_returns_error() {
        let temp_dir = TempDir::new().unwrap();

        let mut bytes = vec![];
        bytes.extend(FileMetadata::MAGIC.to_vec());
        bytes.extend(FileMetadata::MAGIC.to_vec());
        bytes.extend(empty_footer_payload_bytes());
        bytes.extend(empty_footer_payload_bytes_length_bytes());
        bytes.extend(vec![0b00000001, 0, 0, 0]);
        bytes.extend(FileMetadata::MAGIC.to_vec());

        let input_file = input_file_with_bytes(&temp_dir, &bytes).await;

        assert_eq!(
            FileMetadata::read(&input_file)
                .await
                .unwrap_err()
                .to_string(),
            "FeatureUnsupported => LZ4 decompression is not supported currently",
        )
    }

    #[tokio::test]
    async fn test_unknown_byte_bit_combination_returns_error() {
        let temp_dir = TempDir::new().unwrap();

        let mut bytes = vec![];
        bytes.extend(FileMetadata::MAGIC.to_vec());
        bytes.extend(FileMetadata::MAGIC.to_vec());
        bytes.extend(empty_footer_payload_bytes());
        bytes.extend(empty_footer_payload_bytes_length_bytes());
        bytes.extend(vec![0b00000010, 0, 0, 0]);
        bytes.extend(FileMetadata::MAGIC.to_vec());

        let input_file = input_file_with_bytes(&temp_dir, &bytes).await;

        assert_eq!(
            FileMetadata::read(&input_file)
                .await
                .unwrap_err()
                .to_string(),
            "DataInvalid => Unknown flag byte 0 and bit 1 combination",
        )
    }

    #[tokio::test]
    async fn test_non_utf8_string_payload_returns_error() {
        let temp_dir = TempDir::new().unwrap();

        let payload_bytes: [u8; 4] = [0, 159, 146, 150];
        let payload_bytes_length_bytes: [u8; 4] = u32::to_le_bytes(payload_bytes.len() as u32);

        let mut bytes = vec![];
        bytes.extend(FileMetadata::MAGIC.to_vec());
        bytes.extend(FileMetadata::MAGIC.to_vec());
        bytes.extend(payload_bytes);
        bytes.extend(payload_bytes_length_bytes);
        bytes.extend(vec![0, 0, 0, 0]);
        bytes.extend(FileMetadata::MAGIC.to_vec());

        let input_file = input_file_with_bytes(&temp_dir, &bytes).await;

        assert_eq!(
            FileMetadata::read(&input_file).await.unwrap_err().to_string(),
            "DataInvalid => Footer is not a valid UTF-8 string, source: invalid utf-8 sequence of 1 bytes from index 1",
        )
    }

    #[tokio::test]
    async fn test_minimal_valid_file_returns_file_metadata() {
        let temp_dir = TempDir::new().unwrap();

        let mut bytes = vec![];
        bytes.extend(FileMetadata::MAGIC.to_vec());
        bytes.extend(FileMetadata::MAGIC.to_vec());
        bytes.extend(empty_footer_payload_bytes());
        bytes.extend(empty_footer_payload_bytes_length_bytes());
        bytes.extend(vec![0, 0, 0, 0]);
        bytes.extend(FileMetadata::MAGIC);

        let input_file = input_file_with_bytes(&temp_dir, &bytes).await;

        assert_eq!(
            FileMetadata::read(&input_file).await.unwrap(),
            FileMetadata {
                blobs: vec![],
                properties: HashMap::new(),
            }
        )
    }

    #[tokio::test]
    async fn test_returns_file_metadata_property() {
        let temp_dir = TempDir::new().unwrap();

        let input_file = input_file_with_payload(
            &temp_dir,
            r#"{
                "blobs" : [ ],
                "properties" : {
                    "a property" : "a property value"
                }
            }"#,
        )
        .await;

        assert_eq!(
            FileMetadata::read(&input_file).await.unwrap(),
            FileMetadata {
                blobs: vec![],
                properties: {
                    let mut map = HashMap::new();
                    map.insert("a property".to_string(), "a property value".to_string());
                    map
                },
            }
        )
    }

    #[tokio::test]
    async fn test_returns_file_metadata_properties() {
        let temp_dir = TempDir::new().unwrap();

        let input_file = input_file_with_payload(
            &temp_dir,
            r#"{
                "blobs" : [ ],
                "properties" : {
                    "a property" : "a property value",
                    "another one": "also with value"
                }
            }"#,
        )
        .await;

        assert_eq!(
            FileMetadata::read(&input_file).await.unwrap(),
            FileMetadata {
                blobs: vec![],
                properties: {
                    let mut map = HashMap::new();
                    map.insert("a property".to_string(), "a property value".to_string());
                    map.insert("another one".to_string(), "also with value".to_string());
                    map
                },
            }
        )
    }

    #[tokio::test]
    async fn test_returns_error_if_blobs_field_is_missing() {
        let temp_dir = TempDir::new().unwrap();

        let input_file = input_file_with_payload(
            &temp_dir,
            r#"{
                "properties" : {}
            }"#,
        )
        .await;

        assert_eq!(
            FileMetadata::read(&input_file).await.unwrap_err().to_string(),
            format!(
                "DataInvalid => Given string is not valid JSON, source: missing field `blobs` at line 3 column 13"
            ),
        )
    }

    #[tokio::test]
    async fn test_returns_error_if_blobs_field_is_bad() {
        let temp_dir = TempDir::new().unwrap();

        let input_file = input_file_with_payload(
            &temp_dir,
            r#"{
                "blobs" : {}
            }"#,
        )
        .await;

        assert_eq!(
            FileMetadata::read(&input_file).await.unwrap_err().to_string(),
            format!("DataInvalid => Given string is not valid JSON, source: invalid type: map, expected a sequence at line 2 column 26"),
        )
    }

    #[tokio::test]
    async fn test_returns_blobs_metadatas() {
        let temp_dir = TempDir::new().unwrap();

        let input_file = input_file_with_payload(
            &temp_dir,
            r#"{
                "blobs" : [
                    {
                        "type" : "type-a",
                        "fields" : [ 1 ],
                        "snapshot-id" : 14,
                        "sequence-number" : 3,
                        "offset" : 4,
                        "length" : 16
                    },
                    {
                        "type" : "type-bbb",
                        "fields" : [ 2, 3, 4 ],
                        "snapshot-id" : 77,
                        "sequence-number" : 4,
                        "offset" : 21474836470000,
                        "length" : 79834
                    }
                ]
            }"#,
        )
        .await;

        assert_eq!(
            FileMetadata::read(&input_file).await.unwrap(),
            FileMetadata {
                blobs: vec![
                    BlobMetadata {
                        r#type: "type-a".to_string(),
                        input_fields: vec![1],
                        snapshot_id: 14,
                        sequence_number: 3,
                        offset: 4,
                        length: 16,
                        compression_codec: CompressionCodec::None,
                        properties: HashMap::new(),
                    },
                    BlobMetadata {
                        r#type: "type-bbb".to_string(),
                        input_fields: vec![2, 3, 4],
                        snapshot_id: 77,
                        sequence_number: 4,
                        offset: 21474836470000,
                        length: 79834,
                        compression_codec: CompressionCodec::None,
                        properties: HashMap::new(),
                    },
                ],
                properties: HashMap::new(),
            }
        )
    }

    #[tokio::test]
    async fn test_returns_properties_in_blob_metadata() {
        let temp_dir = TempDir::new().unwrap();

        let input_file = input_file_with_payload(
            &temp_dir,
            r#"{
                "blobs" : [
                    {
                        "type" : "type-a",
                        "fields" : [ 1 ],
                        "snapshot-id" : 14,
                        "sequence-number" : 3,
                        "offset" : 4,
                        "length" : 16,
                        "properties" : {
                            "some key" : "some value"
                        }
                    }
                ]
            }"#,
        )
        .await;

        assert_eq!(
            FileMetadata::read(&input_file).await.unwrap(),
            FileMetadata {
                blobs: vec![BlobMetadata {
                    r#type: "type-a".to_string(),
                    input_fields: vec![1],
                    snapshot_id: 14,
                    sequence_number: 3,
                    offset: 4,
                    length: 16,
                    compression_codec: CompressionCodec::None,
                    properties: {
                        let mut map = HashMap::new();
                        map.insert("some key".to_string(), "some value".to_string());
                        map
                    },
                }],
                properties: HashMap::new(),
            }
        )
    }

    #[tokio::test]
    async fn test_returns_error_if_blobs_fields_value_is_outside_i32_range() {
        let temp_dir = TempDir::new().unwrap();

        let out_of_i32_range_number: i64 = i32::MAX as i64 + 1;

        let input_file = input_file_with_payload(
            &temp_dir,
            &format!(
                r#"{{
                    "blobs" : [
                        {{
                            "type" : "type-a",
                            "fields" : [ {} ],
                            "snapshot-id" : 14,
                            "sequence-number" : 3,
                            "offset" : 4,
                            "length" : 16
                        }}
                    ]
                }}"#,
                out_of_i32_range_number
            ),
        )
        .await;

        assert_eq!(
            FileMetadata::read(&input_file).await.unwrap_err().to_string(),
            format!(
                "DataInvalid => Given string is not valid JSON, source: invalid value: integer `{}`, expected i32 at line 5 column 51",
                out_of_i32_range_number
            ),
        )
    }

    #[tokio::test]
    async fn test_returns_errors_if_footer_payload_is_not_encoded_in_json_format() {
        let temp_dir = TempDir::new().unwrap();

        let input_file = input_file_with_payload(&temp_dir, r#""blobs" = []"#).await;
        assert_eq!(
            FileMetadata::read(&input_file).await.unwrap_err().to_string(),
            "DataInvalid => Given string is not valid JSON, source: invalid type: string \"blobs\", expected struct FileMetadata at line 1 column 7",
        )
    }

    #[tokio::test]
    async fn test_read_file_metadata_of_uncompressed_empty_file() {
        let input_file = rust_empty_uncompressed_input_file();
        let file_metadata = FileMetadata::read(&input_file).await.unwrap();
        assert_eq!(file_metadata, empty_footer_payload())
    }

    #[tokio::test]
    async fn test_read_file_metadata_of_uncompressed_metric_data() {
        let input_file = rust_uncompressed_metric_input_file();
        let file_metadata = FileMetadata::read(&input_file).await.unwrap();
        assert_eq!(file_metadata, uncompressed_metric_file_metadata())
    }

    #[tokio::test]
    async fn test_read_file_metadata_of_zstd_compressed_metric_data() {
        let input_file = rust_zstd_compressed_metric_input_file();
        let file_metadata = FileMetadata::read(&input_file).await.unwrap();
        assert_eq!(file_metadata, zstd_compressed_metric_file_metadata())
    }
}
