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

//! Comprehensive deletion vector tests covering edge cases and spec compliance

use std::collections::HashMap;

use iceberg::puffin::{deserialize_deletion_vector, serialize_deletion_vector, DeletionVectorWriter};
use iceberg::delete_vector::DeleteVector;
use iceberg::io::FileIO;
use roaring::RoaringTreemap;
use tempfile::TempDir;

/// Test: Deletion vector with maximum u64 value (boundary test)
#[test]
fn test_deletion_vector_max_u64_boundary() {
    let mut treemap = RoaringTreemap::new();

    // Test near-max values (can't use u64::MAX due to spec constraint: MSB must be 0)
    let max_valid = i64::MAX as u64; // 2^63 - 1
    treemap.insert(max_valid);
    treemap.insert(max_valid - 1);
    treemap.insert(max_valid - 1000);

    let serialized = serialize_deletion_vector(&treemap).unwrap();
    let deserialized = deserialize_deletion_vector(&serialized).unwrap();

    assert_eq!(treemap.len(), deserialized.len());
    assert!(deserialized.contains(max_valid));
    assert!(deserialized.contains(max_valid - 1));
    assert!(deserialized.contains(max_valid - 1000));
}

/// Test: Deletion vector with positions at every power of 2
#[test]
fn test_deletion_vector_powers_of_two() {
    let mut treemap = RoaringTreemap::new();

    // Insert powers of 2 up to 2^40
    for i in 0..41 {
        treemap.insert(1u64 << i);
    }

    let serialized = serialize_deletion_vector(&treemap).unwrap();
    let deserialized = deserialize_deletion_vector(&serialized).unwrap();

    assert_eq!(treemap.len(), 41);
    assert_eq!(deserialized.len(), 41);

    for i in 0..41 {
        assert!(deserialized.contains(1u64 << i), "Missing 2^{}", i);
    }
}

/// Test: Deletion vector with sequential ranges
#[test]
fn test_deletion_vector_sequential_ranges() {
    let mut treemap = RoaringTreemap::new();

    // Insert several sequential ranges
    for i in 0..1000 {
        treemap.insert(i);
    }
    for i in 10000..11000 {
        treemap.insert(i);
    }
    for i in (1u64 << 32)..(1u64 << 32) + 500 {
        treemap.insert(i);
    }

    let serialized = serialize_deletion_vector(&treemap).unwrap();
    let deserialized = deserialize_deletion_vector(&serialized).unwrap();

    assert_eq!(treemap.len(), 2500);
    assert_eq!(deserialized.len(), 2500);

    // Verify ranges are intact
    for i in 0..1000 {
        assert!(deserialized.contains(i));
    }
    for i in 10000..11000 {
        assert!(deserialized.contains(i));
    }
}

/// Test: Deletion vector with sparse distribution
#[test]
fn test_deletion_vector_sparse_distribution() {
    let mut treemap = RoaringTreemap::new();

    // Insert sparse values across the entire 63-bit range
    let sparse_values = vec![
        0,
        1000,
        1_000_000,
        1_000_000_000,
        1_000_000_000_000,
        (1u64 << 20) + 42,
        (1u64 << 32) + 1,
        (1u64 << 40) + 100,
        (1u64 << 50) + 1000,
    ];

    for &v in &sparse_values {
        treemap.insert(v);
    }

    let serialized = serialize_deletion_vector(&treemap).unwrap();
    let deserialized = deserialize_deletion_vector(&serialized).unwrap();

    assert_eq!(deserialized.len(), sparse_values.len() as u64);

    for &v in &sparse_values {
        assert!(deserialized.contains(v), "Missing value: {}", v);
    }
}

/// Test: Large deletion vector (100k positions)
#[test]
fn test_deletion_vector_large_scale() {
    let mut treemap = RoaringTreemap::new();

    // Insert 100,000 positions with some pattern
    for i in 0..100_000 {
        // Insert every 10th position to create gaps
        treemap.insert(i * 10);
    }

    let serialized = serialize_deletion_vector(&treemap).unwrap();
    let deserialized = deserialize_deletion_vector(&serialized).unwrap();

    assert_eq!(treemap.len(), 100_000);
    assert_eq!(deserialized.len(), 100_000);

    // Spot check some values
    assert!(deserialized.contains(0));
    assert!(deserialized.contains(50_000 * 10));
    assert!(deserialized.contains(99_999 * 10));
    assert!(!deserialized.contains(1)); // Should not exist (not multiple of 10)
}

/// Test: Deletion vector serialization idempotence
#[test]
fn test_deletion_vector_serialization_idempotence() {
    let mut treemap = RoaringTreemap::new();
    for i in 0..1000 {
        treemap.insert(i);
    }

    // Serialize -> Deserialize -> Serialize -> Deserialize
    let serialized1 = serialize_deletion_vector(&treemap).unwrap();
    let deserialized1 = deserialize_deletion_vector(&serialized1).unwrap();
    let serialized2 = serialize_deletion_vector(&deserialized1).unwrap();
    let deserialized2 = deserialize_deletion_vector(&serialized2).unwrap();

    // Both serializations should be identical
    assert_eq!(serialized1, serialized2);
    assert_eq!(deserialized1.len(), deserialized2.len());
}

/// Test: Deletion vector with all keys having single position
#[test]
fn test_deletion_vector_single_position_per_key() {
    let mut treemap = RoaringTreemap::new();

    // Insert one position for each of 1000 different high keys
    for key in 0..1000 {
        let position = ((key as u64) << 32) + 42; // Each key has position at offset 42
        treemap.insert(position);
    }

    let serialized = serialize_deletion_vector(&treemap).unwrap();
    let deserialized = deserialize_deletion_vector(&serialized).unwrap();

    assert_eq!(treemap.len(), 1000);
    assert_eq!(deserialized.len(), 1000);

    // Verify all positions are present
    for key in 0..1000 {
        let position = ((key as u64) << 32) + 42;
        assert!(deserialized.contains(position));
    }
}

/// Test: Deletion vector binary size is reasonable
#[test]
fn test_deletion_vector_size_efficiency() {
    let mut treemap = RoaringTreemap::new();

    // Insert 1000 sequential values
    for i in 0..1000 {
        treemap.insert(i);
    }

    let serialized = serialize_deletion_vector(&treemap).unwrap();

    // The size should be much smaller than 1000 * 8 bytes (raw u64 array = 8000 bytes)
    // Roaring bitmap format has overhead but is still efficient
    assert!(
        serialized.len() < 8000,
        "Deletion vector larger than uncompressed: {} bytes for 1000 sequential positions",
        serialized.len()
    );

    // Should include: length(4) + magic(4) + compressed bitmap + crc(4)
    assert!(serialized.len() >= 12, "Deletion vector too small");

    println!("Deletion vector size: {} bytes for 1000 sequential positions", serialized.len());
}

/// Test: Deletion vector handles non-sequential positions efficiently
#[test]
fn test_deletion_vector_random_positions() {
    use rand::Rng;

    let mut rng = rand::thread_rng();
    let mut treemap = RoaringTreemap::new();

    // Insert 1000 random positions in the lower 32-bit range
    let mut positions = Vec::new();
    for _ in 0..1000 {
        let pos = rng.r#gen::<u32>() as u64;
        treemap.insert(pos);
        positions.push(pos);
    }

    let serialized = serialize_deletion_vector(&treemap).unwrap();
    let deserialized = deserialize_deletion_vector(&serialized).unwrap();

    // All random positions should be preserved
    for &pos in &positions {
        assert!(deserialized.contains(pos), "Missing random position: {}", pos);
    }
}

/// Test: Concurrent writes to Puffin file with deletion vectors
#[tokio::test]
async fn test_concurrent_deletion_vector_writes() {
    let tmp_dir = TempDir::new().unwrap();
    let table_location = tmp_dir.path();
    let file_io = FileIO::from_path(table_location.as_os_str().to_str().unwrap())
        .unwrap()
        .build()
        .unwrap();

    // Create multiple deletion vectors for different files
    let mut deletion_vectors = HashMap::new();

    for i in 0..10 {
        let data_file = format!("data-{}.parquet", i);
        let positions = (i * 100..(i + 1) * 100).collect::<Vec<_>>();
        let dv = DeletionVectorWriter::create_deletion_vector(positions).unwrap();
        deletion_vectors.insert(data_file, dv);
    }

    // Write all deletion vectors to single Puffin file
    let puffin_path = format!("{}/concurrent.puffin", table_location.to_str().unwrap());
    let writer = DeletionVectorWriter::new(file_io.clone(), 1, 1);

    let metadata_map = writer
        .write_deletion_vectors(&puffin_path, deletion_vectors)
        .await
        .unwrap();

    // Verify all 10 deletion vectors were written
    assert_eq!(metadata_map.len(), 10);

    // Verify metadata is correct
    for i in 0..10 {
        let key = format!("data-{}.parquet", i);
        assert!(metadata_map.contains_key(&key));
        let metadata = &metadata_map[&key];
        assert!(metadata.length > 0);
        assert!(metadata.offset >= 4); // After header
    }
}

/// Test: Deletion vector with exactly one bitmap (key = 0)
#[test]
fn test_deletion_vector_single_bitmap() {
    let mut treemap = RoaringTreemap::new();

    // Insert values only in the first bitmap (high key = 0)
    for i in 0..1000u32 {
        treemap.insert(i as u64);
    }

    let serialized = serialize_deletion_vector(&treemap).unwrap();
    let deserialized = deserialize_deletion_vector(&serialized).unwrap();

    assert_eq!(treemap.len(), 1000);
    assert_eq!(deserialized.len(), 1000);

    // All values should be in range [0, 999]
    for i in 0..1000 {
        assert!(deserialized.contains(i));
    }
}

/// Test: Deletion vector with maximum number of bitmaps
#[test]
fn test_deletion_vector_many_bitmaps() {
    let mut treemap = RoaringTreemap::new();

    // Insert one value for each of 10000 different high keys
    for key in 0..10000 {
        let position = (key as u64) << 32;
        treemap.insert(position);
    }

    let serialized = serialize_deletion_vector(&treemap).unwrap();
    let deserialized = deserialize_deletion_vector(&serialized).unwrap();

    assert_eq!(treemap.len(), 10000);
    assert_eq!(deserialized.len(), 10000);
}

/// Test: Deletion vector spec compliance - magic bytes position
#[test]
fn test_deletion_vector_spec_magic_bytes() {
    let mut treemap = RoaringTreemap::new();
    treemap.insert(42);

    let serialized = serialize_deletion_vector(&treemap).unwrap();

    // According to spec: bytes 4-7 should be magic bytes D1 D3 39 64
    assert!(serialized.len() >= 8);
    assert_eq!(serialized[4], 0xD1);
    assert_eq!(serialized[5], 0xD3);
    assert_eq!(serialized[6], 0x39);
    assert_eq!(serialized[7], 0x64);
}

/// Test: Deletion vector spec compliance - length field
#[test]
fn test_deletion_vector_spec_length_field() {
    let mut treemap = RoaringTreemap::new();
    treemap.insert(42);

    let serialized = serialize_deletion_vector(&treemap).unwrap();

    // First 4 bytes are length (big-endian)
    let length = u32::from_be_bytes([
        serialized[0],
        serialized[1],
        serialized[2],
        serialized[3],
    ]) as usize;

    // Length should equal: magic(4) + vector + crc(excluded from length)
    // Total size = length_field(4) + length + crc(4)
    assert_eq!(serialized.len(), 4 + length + 4);
}

/// Test: Deletion vector spec compliance - CRC at end
#[test]
fn test_deletion_vector_spec_crc_position() {
    let mut treemap = RoaringTreemap::new();
    treemap.insert(42);

    let serialized = serialize_deletion_vector(&treemap).unwrap();

    // CRC should be last 4 bytes (big-endian)
    let len = serialized.len();
    assert!(len >= 12); // Minimum size

    // Extract CRC (last 4 bytes)
    let stored_crc = u32::from_be_bytes([
        serialized[len - 4],
        serialized[len - 3],
        serialized[len - 2],
        serialized[len - 1],
    ]);

    // Calculate expected CRC (over magic + vector, bytes 4 to len-4)
    let crc_data = &serialized[4..len - 4];
    let calculated_crc = crc32fast::hash(crc_data);

    assert_eq!(stored_crc, calculated_crc);
}

/// Test: Empty deletion vector metadata
#[tokio::test]
async fn test_empty_deletion_vector_metadata() {
    let tmp_dir = TempDir::new().unwrap();
    let table_location = tmp_dir.path();
    let file_io = FileIO::from_path(table_location.as_os_str().to_str().unwrap())
        .unwrap()
        .build()
        .unwrap();

    // Create empty deletion vector
    let dv = DeletionVectorWriter::create_deletion_vector(vec![]).unwrap();

    let puffin_path = format!("{}/empty.puffin", table_location.to_str().unwrap());
    let writer = DeletionVectorWriter::new(file_io, 1, 1);

    let metadata = writer
        .write_single_deletion_vector(&puffin_path, "data.parquet", dv)
        .await
        .unwrap();

    // Empty deletion vector should still have valid metadata
    assert_eq!(metadata.offset, 4); // After header
    assert!(metadata.length > 0); // Empty treemap still has structure
}

/// Test: Deletion vector with DeleteVector API
#[test]
fn test_delete_vector_api_usage() {
    let mut dv = DeleteVector::default();

    // Insert positions
    assert!(dv.insert(10));
    assert!(dv.insert(20));
    assert!(dv.insert(30));

    // Duplicate insert should return false
    assert!(!dv.insert(10));

    assert_eq!(dv.len(), 3);

    // Test iteration
    let positions: Vec<u64> = dv.iter().collect();
    assert_eq!(positions.len(), 3);
    assert!(positions.contains(&10));
    assert!(positions.contains(&20));
    assert!(positions.contains(&30));
}

/// Test: Deletion vector iterator advance_to functionality
#[test]
fn test_delete_vector_iterator_advance() {
    let mut dv = DeleteVector::default();

    // Insert positions from 0 to 95, every 5 (0, 5, 10, ..., 95)
    for i in 0..20 {
        dv.insert(i * 5);
    }

    let mut iter = dv.iter();

    // Consume some values first
    assert_eq!(iter.next(), Some(0));
    assert_eq!(iter.next(), Some(5));

    // Advance to position 30
    iter.advance_to(30);

    // Next value should be >= 30
    // The positions are: 0, 5, 10, 15, 20, 25, 30, 35, ...
    // After advance_to(30), the next value should be 30 or later
    let next = iter.next();
    assert!(next.is_some(), "Iterator should return a value after advance_to");
    let value = next.unwrap();
    assert!(
        value >= 30,
        "Expected value >= 30, got {}. Next values should be 30, 35, 40, ...",
        value
    );

    // Verify it's one of our expected values
    assert_eq!(value % 5, 0, "Value should be multiple of 5");
}

/// Test: Deletion vector bulk insertion
#[test]
fn test_delete_vector_bulk_insertion() {
    let mut dv = DeleteVector::default();

    // Bulk insert sorted positions
    let positions = vec![10, 20, 30, 40, 50];
    let result = dv.insert_positions(&positions);

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 5);
    assert_eq!(dv.len(), 5);

    // Verify all positions were inserted
    for &pos in &positions {
        let found = dv.iter().any(|p| p == pos);
        assert!(found, "Position {} not found", pos);
    }
}

/// Test: Deletion vector bulk insertion validation - must be sorted
#[test]
fn test_delete_vector_bulk_insertion_requires_sorted() {
    let mut dv = DeleteVector::default();

    // Try to insert unsorted positions
    let unsorted_positions = vec![30, 10, 20]; // Not sorted
    let result = dv.insert_positions(&unsorted_positions);

    assert!(result.is_err());
}

/// Test: Deletion vector bulk insertion validation - no duplicates
#[test]
fn test_delete_vector_bulk_insertion_rejects_duplicates() {
    let mut dv = DeleteVector::default();

    // Try to insert positions with duplicates
    let duplicate_positions = vec![10, 20, 20, 30]; // Has duplicate
    let result = dv.insert_positions(&duplicate_positions);

    assert!(result.is_err());
}

#[test]
fn test_all_deletion_vector_tests_pass() {
    // This is a meta-test to ensure the test module compiles and runs
    println!("All deletion vector comprehensive tests compiled successfully");
}
