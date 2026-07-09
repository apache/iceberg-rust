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

use std::ops::BitOrAssign;

use roaring::RoaringTreemap;
use roaring::bitmap::Iter;
use roaring::treemap::BitmapIter;

use crate::{Error, ErrorKind, Result};

/// Magic bytes prefixing a serialized `deletion-vector-v1` bitmap, per the Iceberg Puffin spec.
/// Iceberg-Java stores these as the little-endian int 1681511377 (0x6439D3D1).
const DV_MAGIC: [u8; 4] = [0xD1, 0xD3, 0x39, 0x64];
const DV_LENGTH_PREFIX_BYTES: usize = 4;
const DV_MAGIC_BYTES: usize = 4;
const DV_CRC_BYTES: usize = 4;
const DV_MIN_BLOB_BYTES: usize = DV_LENGTH_PREFIX_BYTES + DV_MAGIC_BYTES + DV_CRC_BYTES;

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

    /// Parses a `deletion-vector-v1` Puffin blob into a `DeleteVector`.
    ///
    /// The layout, defined by the Iceberg Puffin spec and matching Iceberg-Java's
    /// `BitmapPositionDeleteIndex`, is:
    ///
    /// ```text
    /// [length: u32 big-endian][magic: D1 D3 39 64][vector][crc: u32 big-endian]
    /// ```
    ///
    /// `length` counts the magic and vector bytes (not itself or the CRC). The CRC-32 is
    /// computed over the magic and vector. `vector` is a roaring bitmap in the portable
    /// 64-bit format read by [`RoaringTreemap::deserialize_from`].
    ///
    /// Cardinality is not checked here. The caller validates the decoded length against the
    /// delete file's `record_count`, where the manifest metadata is available.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::DataInvalid`] if the blob is shorter than the minimum, the length
    /// prefix or CRC does not match, the magic is wrong, or the roaring payload fails to decode.
    // Consumed by the scan delete loader once the deletion-vector read path is wired up.
    #[allow(dead_code)]
    pub fn deserialize(blob: &[u8]) -> Result<Self> {
        if blob.len() < DV_MIN_BLOB_BYTES {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "deletion-vector-v1 blob is {} bytes, shorter than the {DV_MIN_BLOB_BYTES}-byte minimum",
                    blob.len()
                ),
            ));
        }

        // The magic and vector, i.e. the bytes covered by both the length prefix and the CRC.
        let body = &blob[DV_LENGTH_PREFIX_BYTES..blob.len() - DV_CRC_BYTES];

        let declared_len =
            u32::from_be_bytes(blob[..DV_LENGTH_PREFIX_BYTES].try_into().unwrap()) as usize;
        if declared_len != body.len() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "deletion-vector-v1 length prefix is {declared_len}, expected {}",
                    body.len()
                ),
            ));
        }

        // Verify the CRC before interpreting any bytes so a corrupt blob yields a single clear
        // error rather than an opaque roaring decode failure.
        let stored_crc =
            u32::from_be_bytes(blob[blob.len() - DV_CRC_BYTES..].try_into().unwrap());
        let computed_crc = crc32fast::hash(body);
        if computed_crc != stored_crc {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "deletion-vector-v1 CRC mismatch: computed {computed_crc:#010x}, stored {stored_crc:#010x}"
                ),
            ));
        }

        let (magic, vector) = body.split_at(DV_MAGIC_BYTES);
        if magic != DV_MAGIC {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("deletion-vector-v1 magic mismatch: {magic:02x?}, expected {DV_MAGIC:02x?}"),
            ));
        }

        let inner = RoaringTreemap::deserialize_from(vector).map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                "failed to decode deletion-vector-v1 roaring payload",
            )
            .with_source(e)
        })?;

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

    // Reproduces Iceberg-Java's `deletion-vector-v1` framing so tests can round-trip through
    // `deserialize` without a Java writer. Cross-implementation golden fixtures produced by
    // Iceberg-Java are tracked separately; this only checks that our decode matches our encode.
    fn encode_dv_blob(dv: &DeleteVector) -> Vec<u8> {
        let mut vector = Vec::with_capacity(dv.inner.serialized_size());
        dv.inner.serialize_into(&mut vector).unwrap();

        let body_len = DV_MAGIC_BYTES + vector.len();
        let mut blob = Vec::with_capacity(DV_LENGTH_PREFIX_BYTES + body_len + DV_CRC_BYTES);
        blob.extend_from_slice(&(body_len as u32).to_be_bytes());
        blob.extend_from_slice(&DV_MAGIC);
        blob.extend_from_slice(&vector);
        let crc = crc32fast::hash(&blob[DV_LENGTH_PREFIX_BYTES..]);
        blob.extend_from_slice(&crc.to_be_bytes());
        blob
    }

    fn dv_of(positions: impl IntoIterator<Item = u64>) -> DeleteVector {
        let mut dv = DeleteVector::default();
        for pos in positions {
            dv.insert(pos);
        }
        dv
    }

    fn sorted(dv: &DeleteVector) -> Vec<u64> {
        let mut positions: Vec<u64> = dv.iter().collect();
        positions.sort_unstable();
        positions
    }

    #[test]
    fn deserialize_roundtrip_empty() {
        let blob = encode_dv_blob(&DeleteVector::default());
        assert_eq!(DeleteVector::deserialize(&blob).unwrap().len(), 0);
    }

    #[test]
    fn deserialize_roundtrip_small() {
        let positions = [0u64, 5, 100, 1000];
        let dv = DeleteVector::deserialize(&encode_dv_blob(&dv_of(positions))).unwrap();
        assert_eq!(sorted(&dv), positions);
    }

    #[test]
    fn deserialize_roundtrip_spanning_64bit_keys() {
        let positions = [1u64, 1 << 33, (1 << 33) + 5, 1 << 34];
        let dv = DeleteVector::deserialize(&encode_dv_blob(&dv_of(positions))).unwrap();
        assert_eq!(sorted(&dv), positions);
    }

    // Java run-optimizes every deletion vector before writing, so real blobs carry RUN
    // containers, which use the SERIAL_COOKIE roaring layout. Force that layout so decode
    // exercises the run-container path rather than only array and bitmap containers.
    #[test]
    fn deserialize_roundtrip_run_optimized() {
        let mut dv = dv_of(0..10_000);
        assert!(
            dv.inner.optimize(),
            "expected a dense range to run-length encode"
        );
        let decoded = DeleteVector::deserialize(&encode_dv_blob(&dv)).unwrap();
        assert_eq!(decoded.len(), 10_000);
        assert_eq!(sorted(&decoded).first(), Some(&0));
        assert_eq!(sorted(&decoded).last(), Some(&9_999));
    }

    #[test]
    fn deserialize_rejects_short_blob() {
        let err = DeleteVector::deserialize(&[0u8; DV_MIN_BLOB_BYTES - 1]).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    #[test]
    fn deserialize_rejects_bad_magic() {
        let mut blob = encode_dv_blob(&dv_of([1]));
        blob[DV_LENGTH_PREFIX_BYTES] ^= 0xFF;
        // Recompute the CRC so the magic check, not the CRC check, is what fails.
        let end = blob.len() - DV_CRC_BYTES;
        let crc = crc32fast::hash(&blob[DV_LENGTH_PREFIX_BYTES..end]);
        blob[end..].copy_from_slice(&crc.to_be_bytes());
        let err = DeleteVector::deserialize(&blob).unwrap_err();
        assert!(err.message().contains("magic mismatch"), "got: {err}");
    }

    #[test]
    fn deserialize_rejects_bad_crc() {
        let mut blob = encode_dv_blob(&dv_of([1, 2, 3]));
        let end = blob.len() - DV_CRC_BYTES;
        blob[end] ^= 0xFF;
        let err = DeleteVector::deserialize(&blob).unwrap_err();
        assert!(err.message().contains("CRC mismatch"), "got: {err}");
    }

    #[test]
    fn deserialize_rejects_length_prefix_mismatch() {
        let mut blob = encode_dv_blob(&dv_of([1]));
        let declared = u32::from_be_bytes(blob[..DV_LENGTH_PREFIX_BYTES].try_into().unwrap());
        blob[..DV_LENGTH_PREFIX_BYTES].copy_from_slice(&(declared + 1).to_be_bytes());
        let err = DeleteVector::deserialize(&blob).unwrap_err();
        assert!(err.message().contains("length prefix"), "got: {err}");
    }
}
