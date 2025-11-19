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

//! Deletion vector encoding and decoding for Puffin files
//!
//! This module implements the deletion-vector-v1 blob type format as specified in the
//! Apache Iceberg Puffin specification.
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

use std::io::{Cursor, Write};

use byteorder::{BigEndian, LittleEndian, ReadBytesExt, WriteBytesExt};
use roaring::RoaringBitmap;
use roaring::RoaringTreemap;

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
            format!(
                "Invalid magic bytes: expected {:?}, got {:?}",
                MAGIC, magic
            ),
        ));
    }

    // Calculate expected vector length
    let vector_length = length - 4; // Subtract magic bytes

    // Read vector bytes
    let mut vector_bytes = vec![0u8; vector_length];
    std::io::Read::read_exact(&mut cursor, &mut vector_bytes).map_err(|e| {
        Error::new(
            ErrorKind::DataInvalid,
            "Failed to read vector bytes",
        )
        .with_source(e)
    })?;

    // Read CRC (big-endian)
    let stored_crc = cursor.read_u32::<BigEndian>().map_err(|e| {
        Error::new(ErrorKind::DataInvalid, "Failed to read CRC").with_source(e)
    })?;

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
    let bitmaps: Vec<(u32, RoaringBitmap)> = treemap
        .bitmaps()
        .map(|(k, v)| (k, v.clone()))
        .collect();

    // Write the number of bitmaps (8 bytes, little-endian)
    buffer
        .write_u64::<LittleEndian>(bitmaps.len() as u64)
        .map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                "Failed to write bitmap count",
            )
            .with_source(e)
        })?;

    // Write each bitmap
    for (key, bitmap) in bitmaps {
        // Write the key (4 bytes, little-endian)
        buffer.write_u32::<LittleEndian>(key).map_err(|e| {
            Error::new(ErrorKind::Unexpected, "Failed to write bitmap key").with_source(e)
        })?;

        // Serialize the bitmap in portable format
        bitmap.serialize_into(&mut buffer).map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                "Failed to serialize bitmap",
            )
            .with_source(e)
        })?;
    }

    Ok(buffer)
}

/// Deserialize a Roaring64 format to a RoaringTreemap
fn deserialize_roaring64(data: &[u8]) -> Result<RoaringTreemap> {
    let mut cursor = Cursor::new(data);

    // Read the number of bitmaps (8 bytes, little-endian)
    let bitmap_count = cursor.read_u64::<LittleEndian>().map_err(|e| {
        Error::new(
            ErrorKind::DataInvalid,
            "Failed to read bitmap count",
        )
        .with_source(e)
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
            Error::new(
                ErrorKind::DataInvalid,
                "Failed to deserialize bitmap",
            )
            .with_source(e)
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
            1u64 << 32,             // Different high key
            (1u64 << 32) + 1,       // Same high key, different low
            (2u64 << 32) + 100,     // Another high key
            (1u64 << 33) + 1000,    // Large value
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
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid magic bytes"));
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
}
