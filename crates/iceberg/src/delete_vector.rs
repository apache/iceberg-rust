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

use std::io::Cursor;
use std::ops::BitOrAssign;

use roaring::RoaringTreemap;
use roaring::bitmap::Iter;
use roaring::treemap::BitmapIter;

use crate::{Error, ErrorKind, Result};

#[allow(dead_code)]
const DV_BLOB_MAGIC: [u8; 4] = [0xD1, 0xD3, 0x39, 0x64];
#[allow(dead_code)]
const DV_LENGTH_FIELD_SIZE: usize = 4;
#[allow(dead_code)]
const DV_MAGIC_SIZE: usize = 4;
#[allow(dead_code)]
const DV_CRC_SIZE: usize = 4;
#[allow(dead_code)]
const DV_MIN_BLOB_SIZE: usize = DV_LENGTH_FIELD_SIZE + DV_MAGIC_SIZE + DV_CRC_SIZE;

#[derive(Debug, Default)]
pub struct DeleteVector {
    inner: RoaringTreemap,
}

impl DeleteVector {
    #[allow(unused)]
    pub fn new(roaring_treemap: RoaringTreemap) -> DeleteVector {
        DeleteVector {
            inner: roaring_treemap,
        }
    }

    pub fn iter(&self) -> DeleteVectorIterator<'_> {
        let outer = self.inner.bitmaps();
        DeleteVectorIterator { outer, inner: None }
    }

    pub fn insert(&mut self, pos: u64) -> bool {
        self.inner.insert(pos)
    }

    /// Marks the given `positions` as deleted and returns the number of elements appended.
    ///
    /// The input slice must be strictly ordered in ascending order, and every value must be greater than all existing values already in the set.
    ///
    /// # Errors
    ///
    /// Returns an error if the precondition is not met.
    #[allow(dead_code)]
    pub fn insert_positions(&mut self, positions: &[u64]) -> Result<usize> {
        if let Err(err) = self.inner.append(positions.iter().copied()) {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "failed to marks rows as deleted".to_string(),
            )
            .with_source(err));
        }
        Ok(positions.len())
    }

    #[allow(unused)]
    pub fn len(&self) -> u64 {
        self.inner.len()
    }

    #[allow(dead_code)]
    pub fn deserialize_blob(blob_bytes: &[u8], expected_cardinality: Option<u64>) -> Result<Self> {
        if blob_bytes.len() < DV_MIN_BLOB_SIZE {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "deletion-vector-v1 blob too short: {} bytes (minimum {DV_MIN_BLOB_SIZE})",
                    blob_bytes.len()
                ),
            ));
        }

        // 2GB cap matches Java's signed-i32 length field.
        if blob_bytes.len() > i32::MAX as usize {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "deletion-vector-v1 blob exceeds 2GB: {} bytes",
                    blob_bytes.len()
                ),
            ));
        }

        let declared_len =
            u32::from_be_bytes(blob_bytes[0..DV_LENGTH_FIELD_SIZE].try_into().unwrap()) as usize;
        let expected_len = blob_bytes.len() - DV_LENGTH_FIELD_SIZE - DV_CRC_SIZE;
        if declared_len != expected_len {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "deletion-vector-v1 length mismatch: declared {declared_len}, actual {expected_len}"
                ),
            ));
        }

        let magic_end = DV_LENGTH_FIELD_SIZE + DV_MAGIC_SIZE;
        let magic = &blob_bytes[DV_LENGTH_FIELD_SIZE..magic_end];
        if magic != DV_BLOB_MAGIC {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "deletion-vector-v1 bad magic: {magic:02X?}, expected {DV_BLOB_MAGIC:02X?}"
                ),
            ));
        }

        let roaring_end = blob_bytes.len() - DV_CRC_SIZE;
        let mut cursor = Cursor::new(&blob_bytes[magic_end..roaring_end]);
        let inner = RoaringTreemap::deserialize_from(&mut cursor).map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                "failed to deserialize deletion-vector-v1 roaring payload",
            )
            .with_source(e)
        })?;

        let expected_crc = u32::from_be_bytes(blob_bytes[roaring_end..].try_into().unwrap());
        let computed_crc = crc32fast::hash(&blob_bytes[DV_LENGTH_FIELD_SIZE..roaring_end]);
        if computed_crc != expected_crc {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "deletion-vector-v1 CRC mismatch: computed 0x{computed_crc:08x}, expected 0x{expected_crc:08x}"
                ),
            ));
        }

        if let Some(expected) = expected_cardinality {
            let actual = inner.len();
            if actual != expected {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "deletion-vector-v1 cardinality mismatch: got {actual}, expected {expected}"
                    ),
                ));
            }
        }

        Ok(DeleteVector { inner })
    }
}

// Ideally, we'd just wrap `roaring::RoaringTreemap`'s iterator, `roaring::treemap::Iter` here.
// But right now, it does not have a corresponding implementation of `roaring::bitmap::Iter::advance_to`,
// which is very handy in ArrowReader::build_deletes_row_selection.
// There is a PR open on roaring to add this (https://github.com/RoaringBitmap/roaring-rs/pull/314)
// and if that gets merged then we can simplify `DeleteVectorIterator` here, refactoring `advance_to`
// to just a wrapper around the underlying iterator's method.
pub struct DeleteVectorIterator<'a> {
    // NB: `BitMapIter` was only exposed publicly in https://github.com/RoaringBitmap/roaring-rs/pull/316
    // which is not yet released. As a consequence our Cargo.toml temporarily uses a git reference for
    // the roaring dependency.
    outer: BitmapIter<'a>,
    inner: Option<DeleteVectorIteratorInner<'a>>,
}

struct DeleteVectorIteratorInner<'a> {
    high_bits: u32,
    bitmap_iter: Iter<'a>,
}

impl Iterator for DeleteVectorIterator<'_> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(inner) = &mut self.inner
            && let Some(inner_next) = inner.bitmap_iter.next()
        {
            return Some(u64::from(inner.high_bits) << 32 | u64::from(inner_next));
        }

        if let Some((high_bits, next_bitmap)) = self.outer.next() {
            self.inner = Some(DeleteVectorIteratorInner {
                high_bits,
                bitmap_iter: next_bitmap.iter(),
            })
        } else {
            return None;
        }

        self.next()
    }
}

impl DeleteVectorIterator<'_> {
    pub fn advance_to(&mut self, pos: u64) {
        let hi = (pos >> 32) as u32;
        let lo = pos as u32;

        let Some(ref mut inner) = self.inner else {
            return;
        };

        while inner.high_bits < hi {
            let Some((next_hi, next_bitmap)) = self.outer.next() else {
                return;
            };

            *inner = DeleteVectorIteratorInner {
                high_bits: next_hi,
                bitmap_iter: next_bitmap.iter(),
            }
        }

        inner.bitmap_iter.advance_to(lo);
    }
}

impl BitOrAssign for DeleteVector {
    fn bitor_assign(&mut self, other: Self) {
        self.inner.bitor_assign(&other.inner);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insertion_and_iteration() {
        let mut dv = DeleteVector::default();
        assert!(dv.insert(42));
        assert!(dv.insert(100));
        assert!(!dv.insert(42));

        let mut items: Vec<u64> = dv.iter().collect();
        items.sort();
        assert_eq!(items, vec![42, 100]);
        assert_eq!(dv.len(), 2);
    }

    #[test]
    fn test_successful_insert_positions() {
        let mut dv = DeleteVector::default();
        let positions = vec![1, 2, 3, 1000, 1 << 33];
        assert_eq!(dv.insert_positions(&positions).unwrap(), 5);

        let mut collected: Vec<u64> = dv.iter().collect();
        collected.sort();
        assert_eq!(collected, positions);
    }

    /// Testing scenario: bulk insertion fails because input positions are not strictly increasing.
    #[test]
    fn test_failed_insertion_unsorted_elements() {
        let mut dv = DeleteVector::default();
        let positions = vec![1, 3, 5, 4];
        let res = dv.insert_positions(&positions);
        assert!(res.is_err());
    }

    /// Testing scenario: bulk insertion fails because input positions have intersection with existing ones.
    #[test]
    fn test_failed_insertion_with_intersection() {
        let mut dv = DeleteVector::default();
        let positions = vec![1, 3, 5];
        assert_eq!(dv.insert_positions(&positions).unwrap(), 3);

        let res = dv.insert_positions(&[2, 4]);
        assert!(res.is_err());
    }

    /// Testing scenario: bulk insertion fails because input positions have duplicates.
    #[test]
    fn test_failed_insertion_duplicate_elements() {
        let mut dv = DeleteVector::default();
        let positions = vec![1, 3, 5, 5];
        let res = dv.insert_positions(&positions);
        assert!(res.is_err());
    }

    // ----- deletion-vector-v1 blob deserialization -----

    fn serialize_blob_for_test(dv: &DeleteVector) -> Vec<u8> {
        let mut roaring = Vec::with_capacity(dv.inner.serialized_size());
        dv.inner.serialize_into(&mut roaring).unwrap();

        let bitmap_data_len = DV_MAGIC_SIZE + roaring.len();
        let mut blob = Vec::with_capacity(DV_LENGTH_FIELD_SIZE + bitmap_data_len + DV_CRC_SIZE);
        blob.extend_from_slice(&(bitmap_data_len as u32).to_be_bytes());
        blob.extend_from_slice(&DV_BLOB_MAGIC);
        blob.extend_from_slice(&roaring);
        let crc = crc32fast::hash(&blob[DV_LENGTH_FIELD_SIZE..]);
        blob.extend_from_slice(&crc.to_be_bytes());
        blob
    }

    fn dv_from(positions: impl IntoIterator<Item = u64>) -> DeleteVector {
        let mut dv = DeleteVector::default();
        for p in positions {
            dv.insert(p);
        }
        dv
    }

    fn assert_positions(dv: &DeleteVector, expected: &[u64]) {
        let mut got: Vec<u64> = dv.iter().collect();
        got.sort_unstable();
        let mut want = expected.to_vec();
        want.sort_unstable();
        assert_eq!(got, want);
    }

    #[test]
    fn dv_blob_roundtrip_empty() {
        let dv = DeleteVector::default();
        let bytes = serialize_blob_for_test(&dv);
        let parsed = DeleteVector::deserialize_blob(&bytes, Some(0)).unwrap();
        assert_eq!(parsed.len(), 0);
        assert_positions(&parsed, &[]);
    }

    #[test]
    fn dv_blob_roundtrip_small() {
        let dv = dv_from([0, 5, 100]);
        let bytes = serialize_blob_for_test(&dv);
        let parsed = DeleteVector::deserialize_blob(&bytes, Some(3)).unwrap();
        assert_positions(&parsed, &[0, 5, 100]);
    }

    #[test]
    fn dv_blob_roundtrip_64bit_keys() {
        // Positions span multiple inner 32-bit bitmaps.
        let positions = [1u64, 1u64 << 33, (1u64 << 33) + 5];
        let dv = dv_from(positions);
        let bytes = serialize_blob_for_test(&dv);
        let parsed = DeleteVector::deserialize_blob(&bytes, Some(3)).unwrap();
        assert_positions(&parsed, &positions);
    }

    #[test]
    fn dv_blob_roundtrip_dense_range() {
        let dv = dv_from(0..10_000);
        let bytes = serialize_blob_for_test(&dv);
        let parsed = DeleteVector::deserialize_blob(&bytes, Some(10_000)).unwrap();
        assert_eq!(parsed.len(), 10_000);
    }

    #[test]
    fn dv_blob_cardinality_mismatch_errors() {
        let bytes = serialize_blob_for_test(&dv_from([0, 5, 100]));
        let err = DeleteVector::deserialize_blob(&bytes, Some(99)).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message().contains("cardinality mismatch"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn dv_blob_cardinality_skip_when_none() {
        let bytes = serialize_blob_for_test(&dv_from([0, 5, 100]));
        let parsed = DeleteVector::deserialize_blob(&bytes, None).unwrap();
        assert_positions(&parsed, &[0, 5, 100]);
    }

    #[test]
    fn dv_blob_rejects_short_buffer() {
        let err = DeleteVector::deserialize_blob(&[0u8; 11], None).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message().contains("too short"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn dv_blob_rejects_wrong_magic() {
        let mut bytes = serialize_blob_for_test(&dv_from([0]));
        bytes[DV_LENGTH_FIELD_SIZE] ^= 0xFF;
        let err = DeleteVector::deserialize_blob(&bytes, None).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message().contains("bad magic"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn dv_blob_rejects_bad_crc() {
        let mut bytes = serialize_blob_for_test(&dv_from([0, 1, 2]));
        // Corrupt a roaring payload byte; roaring may also reject before CRC check.
        let mid = DV_LENGTH_FIELD_SIZE + DV_MAGIC_SIZE + 1;
        bytes[mid] ^= 0xFF;
        let err = DeleteVector::deserialize_blob(&bytes, None).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        let msg = err.message();
        assert!(
            msg.contains("CRC mismatch") || msg.contains("roaring"),
            "expected CRC or roaring failure, got: {msg}"
        );
    }

    #[test]
    fn dv_blob_rejects_length_mismatch() {
        let mut bytes = serialize_blob_for_test(&dv_from([0]));
        let original_len = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
        bytes[0..4].copy_from_slice(&(original_len - 1).to_be_bytes());
        let err = DeleteVector::deserialize_blob(&bytes, None).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message().contains("length mismatch"),
            "unexpected error: {err}"
        );
    }

    // ----- golden fixtures (see testdata/deletes/README.md) -----

    fn fixture_path(name: &str) -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("testdata")
            .join("deletes")
            .join(name)
    }

    fn read_fixture(name: &str) -> Vec<u8> {
        let path = fixture_path(name);
        std::fs::read(&path).unwrap_or_else(|e| panic!("missing fixture {}: {e}", path.display()))
    }

    #[test]
    fn dv_blob_golden_empty() {
        let bytes = read_fixture("empty.bin");
        let dv = DeleteVector::deserialize_blob(&bytes, Some(0)).unwrap();
        assert_eq!(dv.len(), 0);
    }

    #[test]
    fn dv_blob_golden_single_position() {
        let bytes = read_fixture("single-position.bin");
        let dv = DeleteVector::deserialize_blob(&bytes, Some(1)).unwrap();
        assert_positions(&dv, &[0]);
    }

    #[test]
    fn dv_blob_golden_small_bitmap() {
        let bytes = read_fixture("small-bitmap.bin");
        let dv = DeleteVector::deserialize_blob(&bytes, Some(4)).unwrap();
        assert_positions(&dv, &[0, 1, 100, 1000]);
    }

    #[test]
    fn dv_blob_golden_spanning_keys() {
        let bytes = read_fixture("spanning-keys.bin");
        let dv = DeleteVector::deserialize_blob(&bytes, Some(4)).unwrap();
        assert_positions(&dv, &[0, 1u64 << 33, (1u64 << 33) + 5, 1u64 << 34]);
    }

    #[test]
    fn dv_blob_golden_dense_range() {
        let bytes = read_fixture("dense-range.bin");
        let dv = DeleteVector::deserialize_blob(&bytes, Some(10_000)).unwrap();
        assert_eq!(dv.len(), 10_000);
        assert!(dv.iter().any(|p| p == 0));
        assert!(dv.iter().any(|p| p == 9_999));
    }

    /// Regenerate the committed golden fixtures (see testdata README).
    #[test]
    #[ignore]
    fn dv_blob_regenerate_golden_fixtures() {
        let dir = fixture_path("");
        std::fs::create_dir_all(&dir).unwrap();
        let cases: Vec<(&str, DeleteVector)> = vec![
            ("empty.bin", DeleteVector::default()),
            ("single-position.bin", dv_from([0])),
            ("small-bitmap.bin", dv_from([0, 1, 100, 1000])),
            (
                "spanning-keys.bin",
                dv_from([0, 1u64 << 33, (1u64 << 33) + 5, 1u64 << 34]),
            ),
            ("dense-range.bin", dv_from(0..10_000)),
        ];
        for (name, dv) in cases {
            let bytes = serialize_blob_for_test(&dv);
            std::fs::write(dir.join(name), bytes).unwrap();
        }
    }
}
