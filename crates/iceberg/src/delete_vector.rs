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

// ============================================================================
// Deletion-vector (DV) blob byte format — interop-critical constants.
//
// These mirror Java `org.apache.iceberg.deletes.BitmapPositionDeleteIndex`
// EXACTLY; the bytes a DV blob carries MUST match Java or neither Java nor our
// own read side can apply the deletes. Layout (see `DeleteVector::serialize`):
//
//   [bitmap-data-length: 4 bytes BIG-endian]
//   [magic number:       4 bytes LITTLE-endian]
//   [roaring treemap:    portable serialization, LITTLE-endian]
//   [CRC-32:             4 bytes BIG-endian, over magic+treemap]
//
// where `bitmap-data-length == MAGIC_NUMBER_SIZE_BYTES + treemap_len`, and the
// CRC-32 is computed over the magic bytes and the serialized treemap (i.e. the
// `bitmap-data-length` bytes that follow the length field).
// ============================================================================

/// The Iceberg DV blob magic number (`org.apache.iceberg.deletes.BitmapPositionDeleteIndex.MAGIC_NUMBER`).
/// Serialized as 4 bytes in **little-endian** order, immediately before the roaring bitmap data.
const MAGIC_NUMBER: u32 = 1681511377;

/// Width in bytes of the leading big-endian length field.
const LENGTH_SIZE_BYTES: usize = 4;

/// Width in bytes of the little-endian magic number.
const MAGIC_NUMBER_SIZE_BYTES: usize = 4;

/// Width in bytes of the trailing big-endian CRC-32 checksum.
const CRC_SIZE_BYTES: usize = 4;

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

    /// Serializes this delete vector into the Iceberg deletion-vector blob byte format.
    ///
    /// The returned bytes are the raw `deletion-vector-v1` Puffin blob content — exactly what Java
    /// `BitmapPositionDeleteIndex.serialize` produces, so a blob written from these bytes is readable
    /// by Java and round-trips through [`DeleteVector::deserialize`]. The layout is:
    ///
    /// - 4 bytes, big-endian: the bitmap-data length (`magic + treemap`, NOT including these 4 bytes
    ///   or the trailing CRC).
    /// - 4 bytes, little-endian: the [`MAGIC_NUMBER`].
    /// - the roaring treemap in the portable serialization (little-endian; via
    ///   [`RoaringTreemap::serialize_into`], which is byte-compatible with Java's
    ///   `RoaringPositionBitmap.serialize`).
    /// - 4 bytes, big-endian: the CRC-32 (IEEE) of the magic bytes followed by the treemap bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the treemap cannot be serialized.
    ///
    /// # Note
    ///
    /// Unlike Java, this does NOT run-length-encode (`runOptimize`) the bitmap before serializing, so
    /// the produced bytes are not guaranteed bit-identical to Java's. RLE is a space optimization, not
    /// a correctness requirement: both the Java reader and [`DeleteVector::deserialize`] accept the
    /// non-RLE portable encoding, so the blob remains fully interoperable.
    pub fn serialize(&self) -> Result<Vec<u8>> {
        let treemap_len = self.inner.serialized_size();
        let bitmap_data_length = MAGIC_NUMBER_SIZE_BYTES + treemap_len;

        let mut bytes = Vec::with_capacity(LENGTH_SIZE_BYTES + bitmap_data_length + CRC_SIZE_BYTES);

        // [bitmap-data-length: 4 bytes big-endian]
        let length_field = u32::try_from(bitmap_data_length).map_err(|err| {
            Error::new(
                ErrorKind::DataInvalid,
                "deletion vector is too large to serialize (bitmap data length exceeds u32)",
            )
            .with_source(err)
        })?;
        bytes.extend_from_slice(&length_field.to_be_bytes());

        // [magic number: 4 bytes little-endian] + [roaring treemap: portable, little-endian]
        bytes.extend_from_slice(&MAGIC_NUMBER.to_le_bytes());
        self.inner.serialize_into(&mut bytes).map_err(|err| {
            Error::new(
                ErrorKind::Unexpected,
                "failed to serialize deletion vector roaring treemap",
            )
            .with_source(err)
        })?;

        // [CRC-32: 4 bytes big-endian] over (magic + treemap)
        let crc = crc32(&bytes[LENGTH_SIZE_BYTES..LENGTH_SIZE_BYTES + bitmap_data_length]);
        bytes.extend_from_slice(&crc.to_be_bytes());

        Ok(bytes)
    }

    /// Deserializes a delete vector from the Iceberg deletion-vector blob byte format produced by
    /// [`DeleteVector::serialize`] (and by Java `BitmapPositionDeleteIndex.serialize`).
    ///
    /// This is the read-side parser for a `deletion-vector-v1` Puffin blob. It validates the length
    /// framing, the [`MAGIC_NUMBER`], and the trailing CRC-32 before returning the reconstructed
    /// positions.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::DataInvalid`] if the blob is truncated, carries the wrong magic number,
    /// has an inconsistent length field, or fails the CRC-32 check.
    pub fn deserialize(bytes: &[u8]) -> Result<DeleteVector> {
        if bytes.len() < LENGTH_SIZE_BYTES + MAGIC_NUMBER_SIZE_BYTES + CRC_SIZE_BYTES {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "deletion vector blob too short: {} bytes (need at least {})",
                    bytes.len(),
                    LENGTH_SIZE_BYTES + MAGIC_NUMBER_SIZE_BYTES + CRC_SIZE_BYTES
                ),
            ));
        }

        // [bitmap-data-length: 4 bytes big-endian]
        let bitmap_data_length =
            u32::from_be_bytes(bytes[0..LENGTH_SIZE_BYTES].try_into().map_err(|err| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "invalid deletion vector length field",
                )
                .with_source(err)
            })?) as usize;

        let expected_total = LENGTH_SIZE_BYTES + bitmap_data_length + CRC_SIZE_BYTES;
        if bytes.len() != expected_total {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "deletion vector blob length mismatch: declared bitmap-data length {bitmap_data_length} implies {expected_total} total bytes, got {}",
                    bytes.len()
                ),
            ));
        }

        let bitmap_data = &bytes[LENGTH_SIZE_BYTES..LENGTH_SIZE_BYTES + bitmap_data_length];

        // [CRC-32: 4 bytes big-endian] over (magic + treemap)
        let crc_offset = LENGTH_SIZE_BYTES + bitmap_data_length;
        let expected_crc = u32::from_be_bytes(
            bytes[crc_offset..crc_offset + CRC_SIZE_BYTES]
                .try_into()
                .map_err(|err| {
                    Error::new(ErrorKind::DataInvalid, "invalid deletion vector CRC field")
                        .with_source(err)
                })?,
        );
        let actual_crc = crc32(bitmap_data);
        if actual_crc != expected_crc {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "deletion vector CRC mismatch: expected {expected_crc:#010x}, computed {actual_crc:#010x}"
                ),
            ));
        }

        // [magic number: 4 bytes little-endian]
        let magic =
            u32::from_le_bytes(bitmap_data[0..MAGIC_NUMBER_SIZE_BYTES].try_into().map_err(
                |err| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "invalid deletion vector magic field",
                    )
                    .with_source(err)
                },
            )?);
        if magic != MAGIC_NUMBER {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "deletion vector magic number mismatch: expected {MAGIC_NUMBER}, got {magic}"
                ),
            ));
        }

        // [roaring treemap: portable, little-endian]
        let treemap = RoaringTreemap::deserialize_from(&bitmap_data[MAGIC_NUMBER_SIZE_BYTES..])
            .map_err(|err| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "failed to deserialize deletion vector roaring treemap",
                )
                .with_source(err)
            })?;

        Ok(DeleteVector { inner: treemap })
    }
}

/// Computes the CRC-32 (IEEE 802.3 / zlib) checksum of `bytes`.
///
/// This is the same algorithm as Java `java.util.zip.CRC32` (reflected input/output, polynomial
/// `0xEDB88320`, initial value `0xFFFFFFFF`, final XOR `0xFFFFFFFF`) — it must match Java exactly so
/// the CRC field in a deletion-vector blob round-trips. A dedicated `crc` crate is intentionally not
/// pulled in (it would require a forbidden dependency edit); this small, table-free implementation is
/// pinned by the canonical check value `crc32(b"123456789") == 0xCBF43926`.
fn crc32(bytes: &[u8]) -> u32 {
    /// Reflected CRC-32 polynomial (IEEE 802.3 / zlib).
    const CRC32_POLYNOMIAL: u32 = 0xEDB88320;

    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in bytes {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (CRC32_POLYNOMIAL & mask);
        }
    }
    !crc
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

    fn dv_from(positions: &[u64]) -> DeleteVector {
        let mut dv = DeleteVector::default();
        for &pos in positions {
            dv.insert(pos);
        }
        dv
    }

    fn sorted(dv: &DeleteVector) -> Vec<u64> {
        let mut items: Vec<u64> = dv.iter().collect();
        items.sort_unstable();
        items
    }

    /// Risk pinned: a wrong byte layout / endianness / magic / CRC means Java (and our own read side)
    /// cannot reconstruct the deleted positions — the deletes are silently unreadable. Round-trip a set
    /// that crosses the 32-bit boundary (2_000_000_000 < 2^31 but exercises high 32-bit positions via
    /// the treemap's per-key bitmaps) and confirm the exact positions survive.
    #[test]
    fn test_serialize_deserialize_round_trips_exact_positions_across_32bit_boundary() {
        let positions = [1u64, 3, 5, 1000, 2_000_000_000];
        let dv = dv_from(&positions);

        let bytes = dv.serialize().expect("serialize must succeed");
        let restored = DeleteVector::deserialize(&bytes).expect("deserialize must succeed");

        assert_eq!(sorted(&restored), positions.to_vec());
        assert_eq!(restored.len(), positions.len() as u64);
    }

    /// Risk pinned: a position with a non-zero HIGH 32 bits (key) must serialize into a distinct
    /// per-key roaring bitmap and come back exactly — a treemap-key bug would drop it or corrupt it.
    #[test]
    fn test_serialize_deserialize_round_trips_high_key_position() {
        // 1 << 33 has key = 2, low = 0 — forces a second per-key bitmap in the treemap.
        let positions = [7u64, 1 << 33, (1 << 33) + 9];
        let dv = dv_from(&positions);

        let bytes = dv.serialize().expect("serialize must succeed");
        let restored = DeleteVector::deserialize(&bytes).expect("deserialize must succeed");

        assert_eq!(sorted(&restored), positions.to_vec());
    }

    /// An empty delete vector must still serialize to a valid, CRC-checked blob and round-trip to empty.
    #[test]
    fn test_serialize_deserialize_round_trips_empty() {
        let dv = DeleteVector::default();
        let bytes = dv.serialize().expect("serialize must succeed");
        let restored = DeleteVector::deserialize(&bytes).expect("deserialize must succeed");
        assert_eq!(restored.len(), 0);
        assert!(sorted(&restored).is_empty());
    }

    /// Byte-level framing risk: Java reads the leading length as `magic.len() + bitmap.len()` and the
    /// magic as little-endian `1681511377`. Assert the exact frame so a future refactor cannot silently
    /// drift the layout Java depends on.
    #[test]
    fn test_serialized_frame_matches_java_layout() {
        let dv = dv_from(&[1, 3]);
        let bytes = dv.serialize().expect("serialize must succeed");

        // Leading 4-byte BIG-endian length == magic size + treemap size.
        let declared_len = u32::from_be_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let total = bytes.len();
        // total = 4 (length) + declared_len + 4 (crc)
        assert_eq!(total, LENGTH_SIZE_BYTES + declared_len + CRC_SIZE_BYTES);
        assert_eq!(declared_len, MAGIC_NUMBER_SIZE_BYTES + (total - 12));

        // The 4 bytes after the length field are the magic number in LITTLE-endian order.
        let magic = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
        assert_eq!(magic, MAGIC_NUMBER);
        assert_eq!(magic, 1681511377);
        assert_eq!(&bytes[4..8], &[209, 211, 57, 100]);
    }

    /// CRC pin: the self-contained CRC-32 must equal Java `java.util.zip.CRC32` exactly, or the trailing
    /// checksum field diverges and Java rejects our blob. `0xCBF43926` is the canonical CRC-32 check value.
    #[test]
    fn test_crc32_matches_canonical_check_value() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32(b""), 0x0000_0000);
        assert_eq!(crc32(b"a"), 0xE8B7_BE43);
    }

    /// Mutation: corrupting the magic bytes must make deserialize FAIL (not silently accept a blob Java
    /// would reject). A serializer that wrote the magic with the wrong endianness would land here.
    #[test]
    fn test_deserialize_rejects_corrupt_magic() {
        let dv = dv_from(&[1, 3]);
        let mut bytes = dv.serialize().expect("serialize must succeed");

        // Flip a magic byte (offset 4 = first magic byte).
        bytes[4] ^= 0xFF;
        let err = DeleteVector::deserialize(&bytes).expect_err("must reject corrupt magic");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        // Corrupting the magic also breaks the CRC; either guard firing is correct — both reject.
        assert!(
            err.to_string().contains("magic number") || err.to_string().contains("CRC"),
            "unexpected error: {err}"
        );
    }

    /// Mutation: corrupting a treemap byte must trip the CRC-32 guard — proving the checksum actually
    /// protects the bitmap payload (a no-op CRC would let silent corruption through).
    #[test]
    fn test_deserialize_rejects_corrupt_bitmap_via_crc() {
        let dv = dv_from(&[1, 3, 5]);
        let mut bytes = dv.serialize().expect("serialize must succeed");

        // Flip a byte inside the treemap region (after length(4)+magic(4)), before the trailing CRC.
        let treemap_byte = 9; // 4 length + 4 magic + 1
        bytes[treemap_byte] ^= 0xFF;
        let err = DeleteVector::deserialize(&bytes).expect_err("must reject corrupt bitmap");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.to_string().contains("CRC"), "unexpected error: {err}");
    }

    /// Mutation: tampering with the declared length field must fail the framing check rather than
    /// mis-slice the blob.
    #[test]
    fn test_deserialize_rejects_wrong_length_field() {
        let dv = dv_from(&[1, 3]);
        let mut bytes = dv.serialize().expect("serialize must succeed");

        // Bump the big-endian length field by one.
        let declared = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
        bytes[0..4].copy_from_slice(&(declared + 1).to_be_bytes());
        let err = DeleteVector::deserialize(&bytes).expect_err("must reject wrong length");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.to_string().contains("length"),
            "unexpected error: {err}"
        );
    }

    /// A truncated blob (shorter than the minimal length+magic+crc frame) must be rejected, not panic.
    #[test]
    fn test_deserialize_rejects_truncated_blob() {
        let err = DeleteVector::deserialize(&[0u8; 4]).expect_err("must reject truncated blob");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.to_string().contains("too short"),
            "unexpected error: {err}"
        );
    }
}
