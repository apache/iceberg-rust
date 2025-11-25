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

//! Deletion vector encoding, decoding, and writing for Puffin files
//!
//! This module implements the deletion-vector-v1 blob type format as specified in the
//! Apache Iceberg Puffin specification, along with utilities for creating Puffin files
//! containing deletion vectors.
//!
//! ## Binary Format
//!
//! The serialized blob contains:
//! - 4 bytes (big-endian): combined length of the vector and magic bytes
//! - 4 bytes: magic sequence `D1 D3 39 64`
//! - Variable length: the vector, serialized as Roaring64
//! - 4 bytes (big-endian): CRC-32 checksum of magic bytes and serialized vector
//!
//! ## Roaring64 Serialization
//!
//! The position vector supports positive 64-bit positions but is optimized for 32-bit values.
//! 64-bit positions are divided into:
//! - Upper 32 bits: "key"
//! - Lower 32 bits: "sub-position"
//!
//! Serialization format:
//! - 8 bytes (little-endian): number of 32-bit Roaring bitmaps
//! - For each 32-bit Roaring bitmap (ordered by key):
//!   - 4 bytes (little-endian): the key
//!   - Variable: 32-bit Roaring bitmap in portable format

use std::collections::HashMap;
use std::io::{Cursor, Write};

use byteorder::{BigEndian, LittleEndian, ReadBytesExt, WriteBytesExt};
use roaring::{RoaringBitmap, RoaringTreemap};

use crate::delete_vector::DeleteVector;
use crate::io::FileIO;
use crate::puffin::{Blob, CompressionCodec, DELETION_VECTOR_V1, PuffinWriter};
use crate::{Error, ErrorKind, Result};

/// Magic bytes for deletion vector blob
const MAGIC: &[u8; 4] = &[0xD1, 0xD3, 0x39, 0x64];

/// Serialize a RoaringTreemap to the deletion-vector-v1 blob format
pub fn serialize_deletion_vector(treemap: &RoaringTreemap) -> Result<Vec<u8>> {
    // Serialize the Roaring64 bitmap
    let vector_bytes = serialize_roaring64(treemap)?;

    // Calculate total length: magic (4) + vector
    let total_length = 4 + vector_bytes.len();

    // Build the final blob: length + magic + vector + crc
    let mut blob = Vec::with_capacity(4 + total_length + 4);

    // Write length (big-endian)
    blob.write_u32::<BigEndian>(total_length as u32)
        .map_err(|e| Error::new(ErrorKind::Unexpected, "Failed to write length").with_source(e))?;

    // Write magic bytes
    blob.write_all(MAGIC)
        .map_err(|e| Error::new(ErrorKind::Unexpected, "Failed to write magic").with_source(e))?;

    // Write vector
    blob.write_all(&vector_bytes)
        .map_err(|e| Error::new(ErrorKind::Unexpected, "Failed to write vector").with_source(e))?;

    // Calculate CRC-32 over magic + vector
    let crc_data = &blob[4..]; // Skip the length field
    let crc = crc32fast::hash(crc_data);

    // Write CRC (big-endian)
    blob.write_u32::<BigEndian>(crc)
        .map_err(|e| Error::new(ErrorKind::Unexpected, "Failed to write CRC").with_source(e))?;

    Ok(blob)
}

/// Deserialize a deletion-vector-v1 blob to a RoaringTreemap
pub fn deserialize_deletion_vector(blob: &[u8]) -> Result<RoaringTreemap> {
    let mut cursor = Cursor::new(blob);

    // Read length (big-endian)
    let length = cursor
        .read_u32::<BigEndian>()
        .map_err(|e| Error::new(ErrorKind::DataInvalid, "Failed to read length").with_source(e))?
        as usize;

    // Read magic bytes
    let mut magic = [0u8; 4];
    std::io::Read::read_exact(&mut cursor, &mut magic).map_err(|e| {
        Error::new(ErrorKind::DataInvalid, "Failed to read magic bytes").with_source(e)
    })?;

    if &magic != MAGIC {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid magic bytes: expected {:?}, got {:?}", MAGIC, magic),
        ));
    }

    // Calculate expected vector length
    let vector_length = length - 4; // Subtract magic bytes

    // Read vector bytes
    let mut vector_bytes = vec![0u8; vector_length];
    std::io::Read::read_exact(&mut cursor, &mut vector_bytes).map_err(|e| {
        Error::new(ErrorKind::DataInvalid, "Failed to read vector bytes").with_source(e)
    })?;

    // Read CRC (big-endian)
    let stored_crc = cursor
        .read_u32::<BigEndian>()
        .map_err(|e| Error::new(ErrorKind::DataInvalid, "Failed to read CRC").with_source(e))?;

    // Verify CRC over magic + vector
    let crc_data = &blob[4..4 + length];
    let calculated_crc = crc32fast::hash(crc_data);

    if stored_crc != calculated_crc {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "CRC mismatch: expected {}, got {}",
                calculated_crc, stored_crc
            ),
        ));
    }

    // Deserialize the Roaring64 bitmap
    deserialize_roaring64(&vector_bytes)
}

/// Serialize a RoaringTreemap to Roaring64 format
fn serialize_roaring64(treemap: &RoaringTreemap) -> Result<Vec<u8>> {
    let mut buffer = Vec::new();

    // Get all the bitmaps from the treemap
    let bitmaps: Vec<(u32, RoaringBitmap)> =
        treemap.bitmaps().map(|(k, v)| (k, v.clone())).collect();

    // Write the number of bitmaps (8 bytes, little-endian)
    buffer
        .write_u64::<LittleEndian>(bitmaps.len() as u64)
        .map_err(|e| {
            Error::new(ErrorKind::Unexpected, "Failed to write bitmap count").with_source(e)
        })?;

    // Write each bitmap
    for (key, bitmap) in bitmaps {
        // Write the key (4 bytes, little-endian)
        buffer.write_u32::<LittleEndian>(key).map_err(|e| {
            Error::new(ErrorKind::Unexpected, "Failed to write bitmap key").with_source(e)
        })?;

        // Serialize the bitmap in portable format
        bitmap.serialize_into(&mut buffer).map_err(|e| {
            Error::new(ErrorKind::Unexpected, "Failed to serialize bitmap").with_source(e)
        })?;
    }

    Ok(buffer)
}

/// Deserialize a Roaring64 format to a RoaringTreemap
fn deserialize_roaring64(data: &[u8]) -> Result<RoaringTreemap> {
    let mut cursor = Cursor::new(data);

    // Read the number of bitmaps (8 bytes, little-endian)
    let bitmap_count = cursor.read_u64::<LittleEndian>().map_err(|e| {
        Error::new(ErrorKind::DataInvalid, "Failed to read bitmap count").with_source(e)
    })? as usize;

    let mut treemap = RoaringTreemap::new();

    // Read each bitmap
    for _ in 0..bitmap_count {
        // Read the key (4 bytes, little-endian)
        let key = cursor.read_u32::<LittleEndian>().map_err(|e| {
            Error::new(ErrorKind::DataInvalid, "Failed to read bitmap key").with_source(e)
        })?;

        // Deserialize the bitmap from portable format
        let bitmap = RoaringBitmap::deserialize_from(&mut cursor).map_err(|e| {
            Error::new(ErrorKind::DataInvalid, "Failed to deserialize bitmap").with_source(e)
        })?;

        // Insert all values from this bitmap into the treemap
        // Each value in the bitmap is a lower 32-bit value that needs to be combined with the key
        for sub_pos in bitmap.iter() {
            let full_position = ((key as u64) << 32) | (sub_pos as u64);
            treemap.insert(full_position);
        }
    }

    Ok(treemap)
}

/// Builder for creating Puffin files containing deletion vectors
pub struct DeletionVectorWriter {
    file_io: FileIO,
    snapshot_id: i64,
    sequence_number: i64,
    compress_footer: bool,
}

impl DeletionVectorWriter {
    /// Create a new deletion vector writer
    pub fn new(file_io: FileIO, snapshot_id: i64, sequence_number: i64) -> Self {
        Self {
            file_io,
            snapshot_id,
            sequence_number,
            compress_footer: false,
        }
    }

    /// Enable footer compression
    pub fn with_footer_compression(mut self, compress: bool) -> Self {
        self.compress_footer = compress;
        self
    }

    /// Write deletion vectors for multiple data files to a Puffin file
    ///
    /// Returns a map of data file paths to their deletion vector metadata
    /// (offset, length) within the Puffin file.
    pub async fn write_deletion_vectors(
        &self,
        puffin_path: &str,
        deletion_vectors: HashMap<String, DeleteVector>,
    ) -> Result<HashMap<String, DeletionVectorMetadata>> {
        let output_file = self.file_io.new_output(puffin_path)?;
        let mut puffin_writer =
            PuffinWriter::new(&output_file, HashMap::new(), self.compress_footer).await?;

        let mut metadata_map = HashMap::new();
        let mut current_offset = 4i64; // Puffin header is "PFA1" (4 bytes)

        for (data_file_path, delete_vector) in deletion_vectors {
            let dv_bytes = serialize_deletion_vector(delete_vector.inner())?;

            let mut properties = HashMap::new();
            properties.insert("referenced-data-file".to_string(), data_file_path.clone());
            properties.insert("cardinality".to_string(), delete_vector.len().to_string());

            let blob = Blob::builder()
                .r#type(DELETION_VECTOR_V1.to_string())
                .fields(vec![])
                .snapshot_id(self.snapshot_id)
                .sequence_number(self.sequence_number)
                .data(dv_bytes.clone())
                .properties(properties)
                .build();

            let content_size = dv_bytes.len() as i64;

            puffin_writer.add(blob, CompressionCodec::None).await?;

            metadata_map.insert(data_file_path, DeletionVectorMetadata {
                offset: current_offset,
                length: content_size,
            });

            current_offset += content_size;
        }

        puffin_writer.close().await?;

        Ok(metadata_map)
    }

    /// Write a single deletion vector to a Puffin file
    pub async fn write_single_deletion_vector(
        &self,
        puffin_path: &str,
        data_file_path: &str,
        delete_vector: DeleteVector,
    ) -> Result<DeletionVectorMetadata> {
        let mut map = HashMap::new();
        map.insert(data_file_path.to_string(), delete_vector);

        let result = self.write_deletion_vectors(puffin_path, map).await?;

        result.get(data_file_path).cloned().ok_or_else(|| {
            crate::Error::new(
                crate::ErrorKind::Unexpected,
                "Failed to retrieve deletion vector metadata",
            )
        })
    }

    /// Create a deletion vector from a collection of row positions
    pub fn create_deletion_vector<I: IntoIterator<Item = u64>>(
        positions: I,
    ) -> Result<DeleteVector> {
        let mut treemap = RoaringTreemap::new();
        for pos in positions {
            treemap.insert(pos);
        }
        Ok(DeleteVector::new(treemap))
    }
}

/// Metadata about a deletion vector stored in a Puffin file
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletionVectorMetadata {
    /// Offset within the Puffin file where the deletion vector blob starts
    pub offset: i64,
    /// Length of the deletion vector blob in bytes
    pub length: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_deserialize_empty() {
        let treemap = RoaringTreemap::new();
        let serialized = serialize_deletion_vector(&treemap).unwrap();
        let deserialized = deserialize_deletion_vector(&serialized).unwrap();

        assert_eq!(treemap.len(), deserialized.len());
        assert_eq!(treemap.len(), 0);
    }

    #[test]
    fn test_serialize_deserialize_single_value() {
        let mut treemap = RoaringTreemap::new();
        treemap.insert(42);

        let serialized = serialize_deletion_vector(&treemap).unwrap();
        let deserialized = deserialize_deletion_vector(&serialized).unwrap();

        assert_eq!(treemap.len(), deserialized.len());
        assert!(deserialized.contains(42));
    }

    #[test]
    fn test_serialize_deserialize_32bit_values() {
        let mut treemap = RoaringTreemap::new();
        let values = vec![0, 1, 100, 1000, 10000, u32::MAX as u64];

        for v in &values {
            treemap.insert(*v);
        }

        let serialized = serialize_deletion_vector(&treemap).unwrap();
        let deserialized = deserialize_deletion_vector(&serialized).unwrap();

        assert_eq!(treemap.len(), deserialized.len());
        for v in &values {
            assert!(deserialized.contains(*v));
        }
    }

    #[test]
    fn test_serialize_deserialize_64bit_values() {
        let mut treemap = RoaringTreemap::new();
        let values = vec![
            0u64,
            1u64 << 32,          // Different high key
            (1u64 << 32) + 1,    // Same high key, different low
            (2u64 << 32) + 100,  // Another high key
            (1u64 << 33) + 1000, // Large value
        ];

        for v in &values {
            treemap.insert(*v);
        }

        let serialized = serialize_deletion_vector(&treemap).unwrap();
        let deserialized = deserialize_deletion_vector(&serialized).unwrap();

        assert_eq!(treemap.len(), deserialized.len());
        for v in &values {
            assert!(deserialized.contains(*v), "Missing value: {}", v);
        }
    }

    #[test]
    fn test_serialize_deserialize_many_values() {
        let mut treemap = RoaringTreemap::new();

        // Insert many values across different keys
        for i in 0..1000 {
            treemap.insert(i);
            treemap.insert((1u64 << 32) + i);
            treemap.insert((2u64 << 32) + i);
        }

        let serialized = serialize_deletion_vector(&treemap).unwrap();
        let deserialized = deserialize_deletion_vector(&serialized).unwrap();

        assert_eq!(treemap.len(), deserialized.len());
        assert_eq!(deserialized.len(), 3000);

        // Verify all values are present
        for i in 0..1000 {
            assert!(deserialized.contains(i));
            assert!(deserialized.contains((1u64 << 32) + i));
            assert!(deserialized.contains((2u64 << 32) + i));
        }
    }

    #[test]
    fn test_invalid_magic_bytes() {
        let mut blob = vec![0u8; 16];
        blob[0..4].copy_from_slice(&[0, 0, 0, 12]); // length
        blob[4..8].copy_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF]); // bad magic

        let result = deserialize_deletion_vector(&blob);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid magic bytes")
        );
    }

    #[test]
    fn test_invalid_crc() {
        let treemap = RoaringTreemap::new();
        let mut serialized = serialize_deletion_vector(&treemap).unwrap();

        // Corrupt the CRC
        let len = serialized.len();
        serialized[len - 1] ^= 0xFF;

        let result = deserialize_deletion_vector(&serialized);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("CRC mismatch"));
    }

    #[test]
    fn test_blob_format_structure() {
        let mut treemap = RoaringTreemap::new();
        treemap.insert(42);

        let serialized = serialize_deletion_vector(&treemap).unwrap();

        // Verify structure: length (4) + magic (4) + vector + crc (4)
        assert!(serialized.len() >= 12); // At minimum: 4 + 4 + 0 + 4

        // Verify magic bytes at correct position
        assert_eq!(&serialized[4..8], MAGIC);
    }

    // Tests for DeletionVectorWriter
    use tempfile::TempDir;

    use crate::puffin::PuffinReader;

    #[tokio::test]
    async fn test_write_single_deletion_vector() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path();
        let file_io = FileIO::from_path(table_location.as_os_str().to_str().unwrap())
            .unwrap()
            .build()
            .unwrap();

        let puffin_path = format!("{}/test.puffin", table_location.to_str().unwrap());
        let data_file_path = format!("{}/data.parquet", table_location.to_str().unwrap());

        // Create deletion vector
        let positions = vec![0u64, 5, 10, 100, 1000];
        let delete_vector =
            DeletionVectorWriter::create_deletion_vector(positions.clone()).unwrap();

        // Write to Puffin file
        let writer = DeletionVectorWriter::new(file_io.clone(), 1, 1);
        let metadata = writer
            .write_single_deletion_vector(&puffin_path, &data_file_path, delete_vector)
            .await
            .unwrap();

        // Verify metadata
        assert_eq!(metadata.offset, 4); // After "PFA1" header
        assert!(metadata.length > 0);

        // Read back and verify
        let input_file = file_io.new_input(&puffin_path).unwrap();
        let puffin_reader = PuffinReader::new(input_file);
        let file_metadata = puffin_reader.file_metadata().await.unwrap();

        assert_eq!(file_metadata.blobs.len(), 1);
        let blob_metadata = &file_metadata.blobs[0];
        assert_eq!(blob_metadata.r#type, DELETION_VECTOR_V1);
        assert_eq!(blob_metadata.offset, metadata.offset as u64);
        assert_eq!(blob_metadata.length, metadata.length as u64);

        // Verify the blob content
        let blob = puffin_reader.blob(blob_metadata).await.unwrap();
        assert_eq!(blob.blob_type(), DELETION_VECTOR_V1);

        // Deserialize and verify positions
        let loaded_treemap = deserialize_deletion_vector(&blob.data).unwrap();
        let loaded_dv = DeleteVector::new(loaded_treemap);

        assert_eq!(loaded_dv.len(), positions.len() as u64);
        for pos in positions {
            assert!(loaded_dv.iter().any(|p| p == pos));
        }
    }

    #[tokio::test]
    async fn test_write_multiple_deletion_vectors() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path();
        let file_io = FileIO::from_path(table_location.as_os_str().to_str().unwrap())
            .unwrap()
            .build()
            .unwrap();

        let puffin_path = format!("{}/multi.puffin", table_location.to_str().unwrap());

        // Create multiple deletion vectors
        let mut deletion_vectors = HashMap::new();

        let data_file_1 = format!("{}/data1.parquet", table_location.to_str().unwrap());
        let dv1 = DeletionVectorWriter::create_deletion_vector(vec![0, 1, 2]).unwrap();
        deletion_vectors.insert(data_file_1.clone(), dv1);

        let data_file_2 = format!("{}/data2.parquet", table_location.to_str().unwrap());
        let dv2 = DeletionVectorWriter::create_deletion_vector(vec![10, 20, 30, 40]).unwrap();
        deletion_vectors.insert(data_file_2.clone(), dv2);

        let data_file_3 = format!("{}/data3.parquet", table_location.to_str().unwrap());
        let dv3 = DeletionVectorWriter::create_deletion_vector(vec![100, 200]).unwrap();
        deletion_vectors.insert(data_file_3.clone(), dv3);

        // Write to Puffin file
        let writer = DeletionVectorWriter::new(file_io.clone(), 1, 1);
        let metadata_map = writer
            .write_deletion_vectors(&puffin_path, deletion_vectors)
            .await
            .unwrap();

        // Verify all metadata entries
        assert_eq!(metadata_map.len(), 3);
        assert!(metadata_map.contains_key(&data_file_1));
        assert!(metadata_map.contains_key(&data_file_2));
        assert!(metadata_map.contains_key(&data_file_3));

        // Read back and verify
        let input_file = file_io.new_input(&puffin_path).unwrap();
        let puffin_reader = PuffinReader::new(input_file);
        let file_metadata = puffin_reader.file_metadata().await.unwrap();

        assert_eq!(file_metadata.blobs.len(), 3);

        // Verify each blob
        for blob_metadata in &file_metadata.blobs {
            assert_eq!(blob_metadata.r#type, DELETION_VECTOR_V1);
            let blob = puffin_reader.blob(blob_metadata).await.unwrap();

            let referenced_file = blob.properties().get("referenced-data-file").unwrap();

            // Verify cardinality
            let cardinality: u64 = blob
                .properties()
                .get("cardinality")
                .unwrap()
                .parse()
                .unwrap();

            let expected_cardinality = if referenced_file.contains("data1") {
                3
            } else if referenced_file.contains("data2") {
                4
            } else {
                2
            };

            assert_eq!(cardinality, expected_cardinality);
        }
    }

    #[tokio::test]
    async fn test_write_with_64bit_positions() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path();
        let file_io = FileIO::from_path(table_location.as_os_str().to_str().unwrap())
            .unwrap()
            .build()
            .unwrap();

        let puffin_path = format!("{}/64bit.puffin", table_location.to_str().unwrap());
        let data_file_path = format!("{}/data.parquet", table_location.to_str().unwrap());

        // Create deletion vector with 64-bit positions
        let positions = vec![
            0u64,
            u32::MAX as u64,
            (1u64 << 32) + 42,
            (2u64 << 32) + 1000,
        ];
        let delete_vector =
            DeletionVectorWriter::create_deletion_vector(positions.clone()).unwrap();

        // Write to Puffin file
        let writer = DeletionVectorWriter::new(file_io.clone(), 1, 1);
        writer
            .write_single_deletion_vector(&puffin_path, &data_file_path, delete_vector)
            .await
            .unwrap();

        // Read back and verify
        let input_file = file_io.new_input(&puffin_path).unwrap();
        let puffin_reader = PuffinReader::new(input_file);
        let file_metadata = puffin_reader.file_metadata().await.unwrap();

        let blob = puffin_reader.blob(&file_metadata.blobs[0]).await.unwrap();

        let loaded_treemap = deserialize_deletion_vector(&blob.data).unwrap();
        let loaded_dv = DeleteVector::new(loaded_treemap);

        assert_eq!(loaded_dv.len(), positions.len() as u64);
        for pos in positions {
            assert!(
                loaded_dv.iter().any(|p| p == pos),
                "Position {} not found",
                pos
            );
        }
    }
}
