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

use std::io::Read;
use std::ops::BitOrAssign;

use roaring::bitmap::Iter;
use roaring::treemap::BitmapIter;
use roaring::{RoaringBitmap, RoaringTreemap};

use crate::{Error, ErrorKind, Result};

#[derive(Debug, Default)]
pub struct DeleteVector {
    inner: RoaringTreemap,
}

/// Size in bytes of the big-endian `u32` length prefix of a `deletion-vector-v1` blob
/// (Java `BitmapPositionDeleteIndex.LENGTH_SIZE_BYTES`).
const DV_LENGTH_PREFIX_SIZE: usize = 4;
/// Size in bytes of the magic sequence (Java `BitmapPositionDeleteIndex.MAGIC_NUMBER_SIZE_BYTES`).
const DV_MAGIC_SIZE: usize = 4;
/// Size in bytes of the big-endian CRC-32 trailer (Java `BitmapPositionDeleteIndex.CRC_SIZE_BYTES`).
const DV_CRC_SIZE: usize = 4;
/// The `deletion-vector-v1` magic sequence as it appears on disk: the little-endian encoding of
/// Java `BitmapPositionDeleteIndex.MAGIC_NUMBER` (1681511377 = 0x6439D3D1), i.e. `D1 D3 39 64`
/// per the Puffin spec ("A 4-byte magic sequence, `D1 D3 39 64`").
const DV_MAGIC_BYTES: [u8; DV_MAGIC_SIZE] = [0xD1, 0xD3, 0x39, 0x64];
/// Size in bytes of the little-endian `u64` bitmap count that starts the portable 64-bit roaring
/// serialization (Java `RoaringPositionBitmap.BITMAP_COUNT_SIZE_BYTES`).
const DV_BITMAP_COUNT_SIZE: usize = 8;
/// Size in bytes of one little-endian `u32` bitmap key (Java
/// `RoaringPositionBitmap.BITMAP_KEY_SIZE_BYTES`).
const DV_BITMAP_KEY_SIZE: usize = 4;
/// The minimum serialized size of one (key, 32-bit bitmap) pair: a 4-byte key plus an EMPTY
/// standard-format roaring bitmap (4-byte cookie + 4-byte container count). Used to reject a
/// hostile bitmap count that could not possibly fit in the payload before looping over it.
const DV_MIN_BITMAP_ENTRY_SIZE: u64 = (DV_BITMAP_KEY_SIZE + 8) as u64;
/// The largest key Java accepts (`RoaringPositionBitmap.readKey`: `key <= Integer.MAX_VALUE - 1`;
/// a key with the sign bit set reads as negative in Java and is rejected by `key >= 0`).
const DV_MAX_BITMAP_KEY: u32 = i32::MAX as u32 - 1;

fn dv_blob_error(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::DataInvalid, message)
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

    /// Deserializes a Puffin `deletion-vector-v1` blob payload into a [`DeleteVector`].
    ///
    /// `blob` must be exactly the blob bytes the Puffin footer (and the `DeleteFile`'s
    /// `content_offset` / `content_size_in_bytes`) describe. The on-disk layout — Puffin spec
    /// "deletion-vector-v1 blob type" + Java `BitmapPositionDeleteIndex.serialize` — is:
    ///
    /// 1. a big-endian `u32` length of (magic + bitmap),
    /// 2. the 4-byte magic sequence `D1 D3 39 64`,
    /// 3. the bitmap in the portable 64-bit roaring format (little-endian): a `u64` count of
    ///    32-bit bitmaps, then per bitmap a `u32` key (ascending) + a standard-format 32-bit
    ///    roaring bitmap; a 64-bit position is `(key << 32) | low32`,
    /// 4. a big-endian `u32` CRC-32 (zlib polynomial) of (magic + bitmap).
    ///
    /// This parses UNTRUSTED storage bytes: every framing violation (truncation, length-prefix
    /// mismatch, bad magic, CRC mismatch, bitmap-count overflow, out-of-range or non-ascending
    /// keys, garbage bitmap bytes, trailing bytes) returns a clean [`ErrorKind::DataInvalid`]
    /// error naming what failed — never a panic. Validations mirror Java
    /// `BitmapPositionDeleteIndex.deserialize` + `RoaringPositionBitmap.deserialize`
    /// (`readBitmapCount` / `readKey`), with one deliberate ordering difference: the CRC is
    /// verified BEFORE the bitmap is parsed (Java parses first, checks the CRC last) so corrupt
    /// bytes are rejected at the door; the accepted-input set is identical because both sides
    /// require every check to pass.
    pub fn deserialize_deletion_vector_v1(blob: &[u8]) -> Result<DeleteVector> {
        // 1. The big-endian length prefix covering magic + bitmap.
        let length_prefix: [u8; DV_LENGTH_PREFIX_SIZE] = blob
            .get(..DV_LENGTH_PREFIX_SIZE)
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or_else(|| {
                dv_blob_error(format!(
                    "Invalid deletion vector blob: {} bytes is too short to hold the 4-byte length prefix",
                    blob.len()
                ))
            })?;
        let bitmap_data_length = u64::from(u32::from_be_bytes(length_prefix));

        // Java `readBitmapDataLength` rejects `length != contentSizeInBytes - LENGTH - CRC`; the
        // slice we are given IS content_size_in_bytes bytes, so the equivalent check is that the
        // declared length plus prefix and CRC equals the slice length exactly.
        let expected_total = (DV_LENGTH_PREFIX_SIZE + DV_CRC_SIZE) as u64 + bitmap_data_length;
        if blob.len() as u64 != expected_total {
            return Err(dv_blob_error(format!(
                "Invalid deletion vector blob: length prefix declares {bitmap_data_length} bytes \
                 of magic + bitmap (total {expected_total}), but the blob is {} bytes",
                blob.len()
            )));
        }
        if bitmap_data_length < (DV_MAGIC_SIZE + DV_BITMAP_COUNT_SIZE) as u64 {
            return Err(dv_blob_error(format!(
                "Invalid deletion vector blob: declared magic + bitmap length \
                 {bitmap_data_length} is shorter than the minimum {} (magic + bitmap count)",
                DV_MAGIC_SIZE + DV_BITMAP_COUNT_SIZE
            )));
        }
        // The equality check above proved `bitmap_data_length == blob.len() - 8`, so this
        // conversion cannot lose range (blob.len() is a usize).
        let bitmap_data_end = blob.len() - DV_CRC_SIZE;
        let bitmap_data = &blob[DV_LENGTH_PREFIX_SIZE..bitmap_data_end];

        // 2. The magic sequence (Java `deserializeBitmap`: "Invalid magic number").
        let magic = &bitmap_data[..DV_MAGIC_SIZE];
        if magic != DV_MAGIC_BYTES {
            return Err(dv_blob_error(format!(
                "Invalid deletion vector magic: {magic:02X?}, expected {DV_MAGIC_BYTES:02X?}"
            )));
        }

        // 3. The CRC-32 trailer over magic + bitmap (Java `deserialize`: "Invalid CRC"). Checked
        //    BEFORE parsing the bitmap so corrupt bytes never reach the bitmap parser.
        let mut crc = flate2::Crc::new();
        crc.update(bitmap_data);
        let actual_crc = crc.sum();
        let expected_crc_bytes: [u8; DV_CRC_SIZE] = blob[bitmap_data_end..]
            .try_into()
            .expect("length validated above: exactly DV_CRC_SIZE bytes remain");
        let expected_crc = u32::from_be_bytes(expected_crc_bytes);
        if actual_crc != expected_crc {
            return Err(dv_blob_error(format!(
                "Invalid deletion vector CRC: computed {actual_crc:#010X}, stored {expected_crc:#010X}"
            )));
        }

        // 4. The portable 64-bit roaring bitmap (after the magic).
        Self::deserialize_portable_bitmap(&bitmap_data[DV_MAGIC_SIZE..])
    }

    /// Parses the portable 64-bit roaring serialization (little-endian `u64` bitmap count, then
    /// per bitmap an ascending little-endian `u32` key + a standard-format 32-bit roaring
    /// bitmap), mirroring Java `RoaringPositionBitmap.deserialize`. The whole region must be
    /// consumed exactly — trailing bytes are rejected as corruption.
    fn deserialize_portable_bitmap(region: &[u8]) -> Result<DeleteVector> {
        let mut cursor = std::io::Cursor::new(region);

        let mut count_bytes = [0u8; DV_BITMAP_COUNT_SIZE];
        cursor.read_exact(&mut count_bytes).map_err(|source| {
            dv_blob_error("Invalid deletion vector bitmap: truncated before the bitmap count")
                .with_source(source)
        })?;
        let bitmap_count = u64::from_le_bytes(count_bytes);

        // Java `readBitmapCount` rejects counts above Integer.MAX_VALUE; additionally reject any
        // count that cannot fit in the remaining bytes (each entry needs at least a key plus an
        // empty bitmap) so a hostile count fails fast with a named error.
        if bitmap_count > i32::MAX as u64 {
            return Err(dv_blob_error(format!(
                "Invalid deletion vector bitmap count: {bitmap_count} exceeds the maximum {}",
                i32::MAX
            )));
        }
        let remaining = (region.len() - DV_BITMAP_COUNT_SIZE) as u64;
        if bitmap_count > remaining / DV_MIN_BITMAP_ENTRY_SIZE {
            return Err(dv_blob_error(format!(
                "Invalid deletion vector bitmap count: {bitmap_count} bitmaps cannot fit in the \
                 {remaining} remaining payload bytes"
            )));
        }

        let mut treemap = RoaringTreemap::new();
        let mut last_key: Option<u32> = None;
        for _ in 0..bitmap_count {
            let mut key_bytes = [0u8; DV_BITMAP_KEY_SIZE];
            cursor.read_exact(&mut key_bytes).map_err(|source| {
                dv_blob_error("Invalid deletion vector bitmap: truncated before a bitmap key")
                    .with_source(source)
            })?;
            let key = u32::from_le_bytes(key_bytes);

            // Java `readKey`: keys are non-negative signed ints (so the sign bit must be clear),
            // at most Integer.MAX_VALUE - 1, and strictly ascending.
            if key > DV_MAX_BITMAP_KEY {
                return Err(dv_blob_error(format!(
                    "Invalid deletion vector bitmap key: {key} exceeds the maximum {DV_MAX_BITMAP_KEY}"
                )));
            }
            if let Some(last) = last_key
                && key <= last
            {
                return Err(dv_blob_error(format!(
                    "Invalid deletion vector bitmap key order: key {key} follows {last}, keys \
                     must be strictly ascending"
                )));
            }

            // The checked deserializer validates the 32-bit bitmap's internal structure and
            // consumes exactly its serialized bytes from the cursor.
            let bitmap = RoaringBitmap::deserialize_from(&mut cursor).map_err(|source| {
                dv_blob_error(format!(
                    "Invalid deletion vector: malformed 32-bit roaring bitmap for key {key}"
                ))
                .with_source(source)
            })?;

            // Reassemble 64-bit positions: the key holds the high 32 bits, the bitmap the low 32
            // (Java `RoaringPositionBitmap.toPosition`). Keys ascend strictly and positions
            // within a bitmap iterate ascending, so the appended sequence is strictly ascending.
            let high_bits = u64::from(key) << 32;
            treemap
                .append(bitmap.iter().map(|low| high_bits | u64::from(low)))
                .map_err(|source| {
                    dv_blob_error(format!(
                        "Invalid deletion vector: positions for key {key} are not strictly ascending"
                    ))
                    .with_source(source)
                })?;

            last_key = Some(key);
        }

        // Every byte of the declared (and CRC-covered) bitmap region must belong to the bitmap;
        // leftovers mean the length prefix and the bitmap disagree (corruption).
        let consumed = cursor.position();
        if consumed != region.len() as u64 {
            return Err(dv_blob_error(format!(
                "Invalid deletion vector bitmap: {} trailing bytes after the declared {bitmap_count} bitmaps",
                region.len() as u64 - consumed
            )));
        }

        Ok(DeleteVector { inner: treemap })
    }

    /// Serializes this vector as a Puffin `deletion-vector-v1` blob payload, byte-identical to
    /// what Java writes for the same position set.
    ///
    /// The layout mirrors Java `BitmapPositionDeleteIndex.serialize` (L124-137) framing around
    /// `RoaringPositionBitmap.serialize` (L245-252):
    ///
    /// 1. a big-endian `u32` length of (magic + bitmap),
    /// 2. the 4-byte magic sequence `D1 D3 39 64` (the little-endian `MAGIC_NUMBER`),
    /// 3. the bitmap in the portable 64-bit roaring format: a little-endian `u64` count of
    ///    32-bit bitmaps that is **DENSE** — Java writes `bitmaps.length` (= highest key + 1)
    ///    entries INCLUDING empty gap bitmaps — then per key `0..=max_key` a little-endian `u32`
    ///    key + the standard-format 32-bit roaring bitmap,
    /// 4. a big-endian `u32` CRC-32 (zlib polynomial) of (magic + bitmap).
    ///
    /// Each 32-bit sub-bitmap is run-length encoded first wherever that is smaller, exactly like
    /// Java's `bitmap.runLengthEncode()` call at serialize time (`RoaringPositionBitmap`
    /// L176-182, `runOptimize()` per sub-bitmap): for ARRAY and BITMAP stores — the only stores
    /// an `insert()`-built treemap holds — `roaring-rs`'s [`RoaringBitmap::optimize`] applies the
    /// identical run-iff-strictly-smaller criterion (Java RoaringBitmap 1.3.0
    /// `ArrayContainer.runOptimize`: run iff `2 + 4·runs < 2·cardinality`, ties keep array;
    /// `BitmapContainer.runOptimize`: run iff `2 + 4·runs < 8192`), so the container choice —
    /// and therefore the serialized bytes — match Java's, exact ties included. CAVEAT (relevant
    /// once DESERIALIZED vectors are re-serialized, e.g. the D3 previous-deletes merge): for a
    /// store that is ALREADY a run container, `roaring-rs` compares against the array size
    /// WITHOUT Java's 2-byte cardinality overhead (`Container::optimize` Run branch vs Java
    /// `RunContainer.toEfficientContainer`), so at exactly `cardinality == 2·runs` Java keeps
    /// the run container while we would emit the (equally readable) array form — a byte-parity,
    /// not correctness, divergence. The optimization runs on per-sub-bitmap CLONES so `&self`
    /// stays unmutated (Java mutates its bitmap in place; the output bytes are the same either
    /// way).
    ///
    /// NOTE: `roaring-rs`'s own `RoaringTreemap::serialize_into` writes a SPARSE count (present
    /// keys only) — readers (including Java's) accept both, but byte parity with Java requires
    /// the dense layout, so the outer framing is hand-rolled here.
    ///
    /// # Errors
    ///
    /// - the vector is EMPTY: `BaseDVFileWriter` never serializes an empty index (a per-path
    ///   `Deletes` entry only exists once `delete()` recorded a position, BaseDVFileWriter.java
    ///   L74-79), and a cardinality-0 DV `DeleteFile` is meaningless — fail loud instead of
    ///   writing one;
    /// - a position's high 32 bits exceed `i32::MAX - 1`: unrepresentable in Java's dense bitmap
    ///   array (`RoaringPositionBitmap` doc L45-49 + `validatePosition`/`MAX_POSITION` L342-348);
    ///   our `RoaringTreemap` can hold such keys, so the door check lives here;
    /// - the serialized blob would exceed 2 GB: Java `computeBitmapDataLength` L158-163
    ///   ("Can't serialize index > 2GB"), checked BEFORE allocating the buffer.
    pub fn serialize_deletion_vector_v1(&self) -> Result<Vec<u8>> {
        if self.inner.is_empty() {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "Cannot serialize an empty deletion vector: a deletion-vector-v1 blob must \
                 delete at least one position (BaseDVFileWriter never writes an empty DV)",
            ));
        }

        // Run-length encode each present sub-bitmap (on a clone) and index them by key.
        let mut optimized_by_key: std::collections::BTreeMap<u32, RoaringBitmap> =
            std::collections::BTreeMap::new();
        for (key, bitmap) in self.inner.bitmaps() {
            if key > DV_MAX_BITMAP_KEY {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot serialize deletion vector: bitmap key {key} exceeds the maximum \
                         {DV_MAX_BITMAP_KEY} Java's dense bitmap array can represent"
                    ),
                ));
            }
            let mut optimized = bitmap.clone();
            optimized.optimize();
            optimized_by_key.insert(key, optimized);
        }
        let max_key = *optimized_by_key
            .keys()
            .next_back()
            .expect("non-empty vector has at least one sub-bitmap");
        let dense_bitmap_count = u64::from(max_key) + 1;

        // Pre-compute the exact sizes (Java `serializedSizeInBytes` L220-226 +
        // `computeBitmapDataLength` L158-163) and enforce the 2 GB bound BEFORE allocating.
        // Every dense slot carries a key; an absent key additionally contributes an EMPTY
        // standard-format bitmap. Computed in O(present keys) so a hostile sparse high key is
        // rejected without looping the dense range.
        let empty_bitmap = RoaringBitmap::new();
        let empty_bitmap_size = empty_bitmap.serialized_size() as u64;
        let present_bitmaps_size: u64 = optimized_by_key
            .values()
            .map(|bitmap| bitmap.serialized_size() as u64)
            .sum();
        let absent_bitmap_count = dense_bitmap_count - optimized_by_key.len() as u64;
        let portable_bitmap_size = DV_BITMAP_COUNT_SIZE as u64
            + dense_bitmap_count * DV_BITMAP_KEY_SIZE as u64
            + present_bitmaps_size
            + absent_bitmap_count * empty_bitmap_size;
        let bitmap_data_length = DV_MAGIC_SIZE as u64 + portable_bitmap_size;
        let total_blob_size = (DV_LENGTH_PREFIX_SIZE + DV_CRC_SIZE) as u64 + bitmap_data_length;
        if total_blob_size > i32::MAX as u64 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot serialize deletion vector: blob would be {total_blob_size} bytes, \
                     which exceeds the 2GB limit (Java BitmapPositionDeleteIndex rejects \
                     indexes > Integer.MAX_VALUE bytes)"
                ),
            ));
        }
        // Range-checked above; `as` would silently truncate, `try_from` keeps the proof local.
        let bitmap_data_length_u32 = u32::try_from(bitmap_data_length)
            .expect("bitmap data length bounded by the 2GB check above");

        // 1. + 2. The big-endian length prefix and the magic.
        let mut blob = Vec::with_capacity(total_blob_size as usize);
        blob.extend_from_slice(&bitmap_data_length_u32.to_be_bytes());
        blob.extend_from_slice(&DV_MAGIC_BYTES);

        // 3. The DENSE portable 64-bit roaring bitmap.
        blob.extend_from_slice(&dense_bitmap_count.to_le_bytes());
        for key in 0..=max_key {
            blob.extend_from_slice(&key.to_le_bytes());
            let bitmap = optimized_by_key.get(&key).unwrap_or(&empty_bitmap);
            bitmap.serialize_into(&mut blob).map_err(|source| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("Failed to serialize the 32-bit roaring bitmap for key {key}"),
                )
                .with_source(source)
            })?;
        }

        // 4. The big-endian CRC-32 (zlib) of magic + bitmap (everything after the length prefix).
        let mut crc = flate2::Crc::new();
        crc.update(&blob[DV_LENGTH_PREFIX_SIZE..]);
        blob.extend_from_slice(&crc.sum().to_be_bytes());

        debug_assert_eq!(blob.len() as u64, total_blob_size);
        Ok(blob)
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
pub(crate) mod tests {
    use super::*;

    /// Frames `bitmap_bytes` (the portable 64-bit roaring serialization) as a
    /// `deletion-vector-v1` blob: BE u32 length of (magic + bitmap), the magic, the bitmap, and
    /// a BE u32 CRC-32 of (magic + bitmap). The trivial inverse of
    /// [`DeleteVector::deserialize_deletion_vector_v1`]'s framing, used to synthesize fixtures.
    pub(crate) fn frame_deletion_vector_v1(bitmap_bytes: &[u8]) -> Vec<u8> {
        let mut bitmap_data = Vec::with_capacity(DV_MAGIC_SIZE + bitmap_bytes.len());
        bitmap_data.extend_from_slice(&DV_MAGIC_BYTES);
        bitmap_data.extend_from_slice(bitmap_bytes);

        let mut crc = flate2::Crc::new();
        crc.update(&bitmap_data);

        let mut blob = Vec::with_capacity(DV_LENGTH_PREFIX_SIZE + bitmap_data.len() + DV_CRC_SIZE);
        blob.extend_from_slice(
            &u32::try_from(bitmap_data.len())
                .expect("test blob < 4GB")
                .to_be_bytes(),
        );
        blob.extend_from_slice(&bitmap_data);
        blob.extend_from_slice(&crc.sum().to_be_bytes());
        blob
    }

    /// Encodes a NON-EMPTY set of positions as a `deletion-vector-v1` blob via the PRODUCTION
    /// serializer ([`DeleteVector::serialize_deletion_vector_v1`]) — the D1 test encoder this
    /// helper replaced used the sparse `RoaringTreemap::serialize_into` layout; the production
    /// serializer writes Java's dense layout, which the decoder equally accepts.
    pub(crate) fn encode_deletion_vector_v1(positions: &[u64]) -> Vec<u8> {
        let treemap: RoaringTreemap = positions.iter().copied().collect();
        DeleteVector::new(treemap)
            .serialize_deletion_vector_v1()
            .expect("serialize test positions")
    }

    /// Encodes explicit (key, 32-bit bitmap) pairs — full control over the serialized layout for
    /// run-container and malformed-ordering fixtures.
    fn encode_deletion_vector_v1_from_pairs(pairs: &[(u32, RoaringBitmap)]) -> Vec<u8> {
        let mut bitmap_bytes = Vec::new();
        bitmap_bytes.extend_from_slice(&(pairs.len() as u64).to_le_bytes());
        for (key, bitmap) in pairs {
            bitmap_bytes.extend_from_slice(&key.to_le_bytes());
            bitmap
                .serialize_into(&mut bitmap_bytes)
                .expect("serialize test bitmap");
        }
        frame_deletion_vector_v1(&bitmap_bytes)
    }

    /// Recomputes and rewrites the CRC trailer after a deliberate payload mutation, so a test can
    /// reach the validations BEHIND the CRC check (magic, count, keys, bitmap structure).
    fn rewrite_valid_crc(blob: &mut [u8]) {
        let end = blob.len() - DV_CRC_SIZE;
        let mut crc = flate2::Crc::new();
        crc.update(&blob[DV_LENGTH_PREFIX_SIZE..end]);
        blob[end..].copy_from_slice(&crc.sum().to_be_bytes());
    }

    /// Asserts decode rejects `blob` with a `DataInvalid` error whose message contains
    /// `expected_fragment`. Any panic inside fails the test, pinning the no-panic contract for
    /// malformed untrusted input.
    fn assert_rejects(blob: &[u8], expected_fragment: &str) {
        let error = DeleteVector::deserialize_deletion_vector_v1(blob)
            .expect_err("malformed deletion vector blob must be rejected");
        assert_eq!(error.kind(), crate::ErrorKind::DataInvalid);
        assert!(
            error.to_string().contains(expected_fragment),
            "error {error} does not name the failure (expected fragment {expected_fragment:?})"
        );
    }

    /// Risk pinned: a framing/decode regression silently changing the position set — the exact
    /// set (including values straddling the 32-bit key boundary) must survive a round-trip.
    #[test]
    fn test_dv_blob_round_trip_preserves_positions_across_key_boundary() {
        let positions = [0u64, 5, 1022, (1u64 << 32) + 5, (1u64 << 33) + 1];
        let blob = encode_deletion_vector_v1(&positions);

        let decoded =
            DeleteVector::deserialize_deletion_vector_v1(&blob).expect("valid blob must decode");

        let decoded_positions: Vec<u64> = decoded.iter().collect();
        assert_eq!(decoded_positions, positions);
        assert_eq!(decoded.len(), positions.len() as u64);
    }

    /// Risk pinned: the high-bits reassembly (`(key << 32) | low`) — without the shift, the
    /// positions above 2^32 collapse onto their low words and the set is wrong. This is the
    /// mutation-(c) sentinel.
    #[test]
    fn test_dv_blob_positions_above_u32_range_keep_high_bits() {
        let positions = [7u64, (1u64 << 32) + 7, (5u64 << 32) + 7];
        let blob = encode_deletion_vector_v1(&positions);

        let decoded =
            DeleteVector::deserialize_deletion_vector_v1(&blob).expect("valid blob must decode");

        let decoded_positions: Vec<u64> = decoded.iter().collect();
        assert_eq!(
            decoded_positions, positions,
            "positions above 2^32 must keep their high 32 bits"
        );
    }

    /// Risk pinned: an empty DV (0 bitmaps) must decode to an empty vector, not error — Java's
    /// portable format legally encodes zero bitmaps (`RoaringPositionBitmap.serialize` over an
    /// empty array writes count 0). The frame is synthesized raw because the PRODUCTION
    /// serializer deliberately refuses empty vectors (see the sibling test below).
    #[test]
    fn test_dv_blob_empty_vector_decodes_to_zero_positions() {
        let blob = frame_deletion_vector_v1(&0u64.to_le_bytes());
        let decoded =
            DeleteVector::deserialize_deletion_vector_v1(&blob).expect("empty blob must decode");
        assert_eq!(decoded.len(), 0);
    }

    /// Risk pinned (D2 golden, exact bytes): a serializer regression silently changing ANY byte
    /// of the on-disk encoding — framing, magic, dense count, key order, container layout, or
    /// CRC — corrupts the table for every OTHER reader. The expected bytes for positions
    /// {0, 5, 2^32+1} were HAND-COMPUTED from the Puffin spec + the Roaring format spec
    /// (independent of this code, via python struct/zlib):
    ///   length BE u32 = 58 (magic 4 + count 8 + 2×(key 4 + bitmap)), magic D1 D3 39 64,
    ///   count 2 LE u64, key 0 + array container {0,5} (cookie 12346, 1 container, desc key 0
    ///   card-1 1, offset 16, values 0/5 → 20 bytes), key 1 + array container {1} (18 bytes),
    ///   CRC-32(zlib, magic+bitmap) = 0x9ACC8CA4 BE.
    #[test]
    fn test_dv_serialize_golden_bytes_hand_computed() {
        let treemap: RoaringTreemap = [0u64, 5, (1u64 << 32) + 1].into_iter().collect();
        let blob = DeleteVector::new(treemap)
            .serialize_deletion_vector_v1()
            .expect("serialize golden fixture");

        #[rustfmt::skip]
        let expected: [u8; 66] = [
            // BE u32 length of magic + bitmap = 58
            0x00, 0x00, 0x00, 0x3A,
            // LE magic
            0xD1, 0xD3, 0x39, 0x64,
            // LE u64 dense bitmap count = 2
            0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // key 0 (LE u32) + standard bitmap {0, 5}
            0x00, 0x00, 0x00, 0x00,
            0x3A, 0x30, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
            0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00,
            // key 1 (LE u32) + standard bitmap {1}
            0x01, 0x00, 0x00, 0x00,
            0x3A, 0x30, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x10, 0x00, 0x00, 0x00, 0x01, 0x00,
            // BE u32 CRC-32 of magic + bitmap
            0x9A, 0xCC, 0x8C, 0xA4,
        ];
        assert_eq!(
            blob, expected,
            "serialized DV blob must match the hand-computed bytes"
        );
    }

    /// Risk pinned (D2, the DENSE-layout contract — the mutation-(a) sentinel): Java writes
    /// `bitmaps.length` = max key + 1 entries INCLUDING empty gap bitmaps
    /// (`RoaringPositionBitmap.serialize` L245-252); a sparse encoding (present keys only) is
    /// readable but NOT byte-identical to Java. Positions {0, 2^33} occupy keys 0 and 2, so the
    /// blob must declare count 3 and carry a literal empty key-1 entry. Bytes hand-computed as
    /// in the golden test; CRC = 0xBC98851A.
    #[test]
    fn test_dv_serialize_dense_gap_writes_empty_middle_bitmap_like_java() {
        let treemap: RoaringTreemap = [0u64, 1u64 << 33].into_iter().collect();
        let blob = DeleteVector::new(treemap)
            .serialize_deletion_vector_v1()
            .expect("serialize dense-gap fixture");

        #[rustfmt::skip]
        let expected: [u8; 76] = [
            // BE u32 length of magic + bitmap = 68
            0x00, 0x00, 0x00, 0x44,
            // LE magic
            0xD1, 0xD3, 0x39, 0x64,
            // LE u64 dense bitmap count = 3 (NOT 2: the gap key 1 is included)
            0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // key 0 + standard bitmap {0}
            0x00, 0x00, 0x00, 0x00,
            0x3A, 0x30, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x10, 0x00, 0x00, 0x00, 0x00, 0x00,
            // key 1 + EMPTY standard bitmap (cookie 12346, 0 containers) — the dense gap entry
            0x01, 0x00, 0x00, 0x00,
            0x3A, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // key 2 + standard bitmap {0}
            0x02, 0x00, 0x00, 0x00,
            0x3A, 0x30, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x10, 0x00, 0x00, 0x00, 0x00, 0x00,
            // BE u32 CRC-32 of magic + bitmap
            0xBC, 0x98, 0x85, 0x1A,
        ];
        assert_eq!(
            blob, expected,
            "dense layout must include the empty key-1 gap bitmap, like Java writes it"
        );
    }

    /// Risk pinned (D2): the serializer must round-trip through D1's decoder for every shape the
    /// writer can produce — position 0, gaps between keys, >2^32 positions, and a run-shaped
    /// range. The decoder was proven against real Java bytes in D1, so round-tripping through it
    /// is the in-house byte-compatibility floor.
    #[test]
    fn test_dv_serialize_round_trips_through_decoder() {
        let shapes: Vec<Vec<u64>> = vec![
            vec![0],
            vec![0, 5, (1u64 << 32) + 1],
            vec![7, (1u64 << 32) + 7, (5u64 << 32) + 7], // gap keys 2..=4
            (1000..6000).collect(),                      // run-shaped
        ];
        // NOTE: a position with key near DV_MAX_BITMAP_KEY cannot round-trip — the DENSE layout
        // makes the blob > 2GB (the guard below fires), exactly as Java's serialize would
        // (`computeBitmapDataLength` checkState). The decode-side accept boundary is pinned
        // separately in `test_dv_blob_max_valid_key_boundary_accepted`.
        for positions in shapes {
            let treemap: RoaringTreemap = positions.iter().copied().collect();
            let blob = DeleteVector::new(treemap)
                .serialize_deletion_vector_v1()
                .expect("serialize round-trip fixture");
            let decoded = DeleteVector::deserialize_deletion_vector_v1(&blob)
                .expect("production-serialized blob must decode");
            let decoded_positions: Vec<u64> = decoded.iter().collect();
            assert_eq!(decoded_positions, positions);
        }
    }

    /// Risk pinned (D2, runLengthEncode parity): Java calls `runLengthEncode()` before
    /// serializing (`BitmapPositionDeleteIndex.serialize` L126), so a 5000-long contiguous run
    /// serializes as a RUN container (cookie 12347). If our serializer skipped the equivalent
    /// `optimize()`, the bytes would be a (much larger) array/bitmap container — readable, but
    /// not byte-identical to Java.
    #[test]
    fn test_dv_serialize_run_shaped_input_emits_run_container_like_java() {
        let treemap: RoaringTreemap = (1000u64..6000).collect();
        let blob = DeleteVector::new(treemap)
            .serialize_deletion_vector_v1()
            .expect("serialize run fixture");

        // The first sub-bitmap starts after prefix(4) + magic(4) + count(8) + key(4); its first
        // two bytes are the standard-format cookie: 12347 == "contains run containers".
        let cookie_at =
            DV_LENGTH_PREFIX_SIZE + DV_MAGIC_SIZE + DV_BITMAP_COUNT_SIZE + DV_BITMAP_KEY_SIZE;
        let cookie = u16::from_le_bytes([blob[cookie_at], blob[cookie_at + 1]]);
        assert_eq!(
            cookie, 12347,
            "a 5000-run must serialize as a run container"
        );

        let decoded = DeleteVector::deserialize_deletion_vector_v1(&blob)
            .expect("run-container blob must decode");
        assert_eq!(decoded.len(), 5000);
    }

    /// Risk pinned (D2 review, the run-vs-array TIE): positions {0,1,2} sit EXACTLY on the
    /// array/run size tie — array = 2·3 = 6 bytes, run = 2 + 4·1 = 6 bytes. Java's
    /// `ArrayContainer.runOptimize` converts only when the array is STRICTLY larger
    /// (`getArraySizeInBytes() > sizeAsRunContainer`), so the tie keeps the ARRAY container;
    /// `roaring-rs`'s `optimize` (`size_as_array <= size_as_run` ⇒ keep) agrees. A criterion
    /// drift to `>=`/`<` on either side would flip the container cookie and break byte parity.
    /// The same {0,1,2} set rides the interop fixture (`tests/interop_dv_write.rs`), where the
    /// Java byte-compare settles the tie empirically.
    #[test]
    fn test_dv_serialize_array_run_size_tie_keeps_array_container_like_java() {
        let treemap: RoaringTreemap = [0u64, 1, 2].into_iter().collect();
        let blob = DeleteVector::new(treemap)
            .serialize_deletion_vector_v1()
            .expect("serialize tie fixture");

        let cookie_at =
            DV_LENGTH_PREFIX_SIZE + DV_MAGIC_SIZE + DV_BITMAP_COUNT_SIZE + DV_BITMAP_KEY_SIZE;
        let cookie = u16::from_le_bytes([blob[cookie_at], blob[cookie_at + 1]]);
        assert_eq!(
            cookie, 12346,
            "the exact array/run size tie must KEEP the array container (cookie 12346), like \
             Java's strictly-smaller criterion"
        );

        let decoded =
            DeleteVector::deserialize_deletion_vector_v1(&blob).expect("tie-case blob must decode");
        assert_eq!(decoded.iter().collect::<Vec<_>>(), vec![0, 1, 2]);
    }

    /// Risk pinned (D2): serializing an EMPTY vector must fail loud — `BaseDVFileWriter` never
    /// writes one (a `Deletes` entry only exists once a position was recorded), and a
    /// cardinality-0 DV `DeleteFile` would be a meaningless table entry.
    #[test]
    fn test_dv_serialize_empty_vector_rejected() {
        let error = DeleteVector::default()
            .serialize_deletion_vector_v1()
            .expect_err("empty vector must not serialize");
        assert!(
            error.to_string().contains("empty deletion vector"),
            "error must name the empty-vector rejection, got: {error}"
        );
    }

    /// Risk pinned (D2): a key above `i32::MAX - 1` is unrepresentable in Java's dense bitmap
    /// array (`RoaringPositionBitmap` MAX_POSITION, L45-53) — serializing it would write a blob
    /// Java's `readKey` rejects. Our treemap CAN hold such positions, so the serializer is the
    /// door.
    #[test]
    fn test_dv_serialize_key_above_java_max_rejected() {
        let mut treemap = RoaringTreemap::new();
        treemap.insert(u64::from(i32::MAX as u32) << 32); // key == i32::MAX > MAX-1
        let error = DeleteVector::new(treemap)
            .serialize_deletion_vector_v1()
            .expect_err("key i32::MAX must not serialize");
        assert!(
            error.to_string().contains("exceeds the maximum"),
            "error must name the key bound, got: {error}"
        );
    }

    /// Risk pinned (D2): the 2GB bound (Java `computeBitmapDataLength` L158-163) must fire from
    /// the size PRE-computation — before any allocation. A single position with a huge key forces
    /// a dense count of ~179M entries × 12 bytes ≈ 2.15 GB of (mostly empty-gap) bitmaps.
    #[test]
    fn test_dv_serialize_over_2gb_rejected_before_allocating() {
        let mut treemap = RoaringTreemap::new();
        treemap.insert(179_000_000u64 << 32);
        let error = DeleteVector::new(treemap)
            .serialize_deletion_vector_v1()
            .expect_err("a >2GB blob must not serialize");
        assert!(
            error.to_string().contains("2GB"),
            "error must name the 2GB bound, got: {error}"
        );
    }

    /// Risk pinned: Java run-length-encodes before serializing (`runLengthEncode`), so real DV
    /// blobs carry RUN containers — the decoder must take that container path, not just arrays.
    #[test]
    fn test_dv_blob_run_length_container_decodes() {
        let mut dense = RoaringBitmap::new();
        dense.insert_range(1000..200_000);
        dense.optimize(); // run-length encode wherever smaller, like Java's runLengthEncode()

        // Prove the fixture really serializes with run containers: the standard-format cookie
        // for a bitmap CONTAINING run containers is 12347 (SERIAL_COOKIE); 12346 means none.
        let mut serialized = Vec::new();
        dense
            .serialize_into(&mut serialized)
            .expect("serialize dense fixture bitmap");
        let cookie = u16::from_le_bytes([serialized[0], serialized[1]]);
        assert_eq!(
            cookie, 12347,
            "fixture must actually contain run containers"
        );

        let blob = encode_deletion_vector_v1_from_pairs(&[(0, dense)]);
        let decoded = DeleteVector::deserialize_deletion_vector_v1(&blob)
            .expect("run-container blob must decode");

        assert_eq!(decoded.len(), 199_000);
        let decoded_positions: Vec<u64> = decoded.iter().collect();
        assert_eq!(decoded_positions[0], 1000);
        assert_eq!(decoded_positions[198_999], 199_999);
    }

    /// Risk pinned: Java pads the serialized bitmap array densely from key 0 to the max key, so
    /// a blob with positions only in high keys carries EMPTY gap bitmaps
    /// (`RoaringPositionBitmap.serialize` writes `bitmaps.length` entries) — they must decode as
    /// "no positions", not error.
    #[test]
    fn test_dv_blob_with_empty_gap_bitmap_decodes_like_java_writes_it() {
        let mut key0 = RoaringBitmap::new();
        key0.insert(3);
        let key1_empty = RoaringBitmap::new();
        let mut key2 = RoaringBitmap::new();
        key2.insert(9);

        let blob = encode_deletion_vector_v1_from_pairs(&[(0, key0), (1, key1_empty), (2, key2)]);
        let decoded = DeleteVector::deserialize_deletion_vector_v1(&blob)
            .expect("blob with an empty gap bitmap must decode");

        let decoded_positions: Vec<u64> = decoded.iter().collect();
        assert_eq!(decoded_positions, vec![3, (2u64 << 32) + 9]);
    }

    /// Risk pinned: truncated untrusted input at every framing boundary must produce a clean
    /// error (never a panic). Truncating a valid blob always trips the total-length equality
    /// first; the inner truncation paths are exercised by the bitmap-garbage tests below.
    #[test]
    fn test_dv_blob_truncation_at_each_boundary_rejects_cleanly() {
        let blob = encode_deletion_vector_v1(&[1, 2, 3]);

        // Shorter than the 4-byte length prefix.
        assert_rejects(&[], "too short to hold the 4-byte length prefix");
        assert_rejects(&blob[..3], "too short to hold the 4-byte length prefix");
        // Cut inside the magic, the bitmap, and the CRC trailer: the length prefix no longer
        // matches the byte count.
        assert_rejects(&blob[..6], "length prefix declares");
        assert_rejects(&blob[..blob.len() / 2], "length prefix declares");
        assert_rejects(&blob[..blob.len() - 2], "length prefix declares");
    }

    /// Risk pinned: a wrong magic sequence must be rejected by NAME even when the CRC is valid
    /// for the corrupted bytes (the magic check is independent of the checksum).
    #[test]
    fn test_dv_blob_wrong_magic_rejects() {
        let mut blob = encode_deletion_vector_v1(&[1, 2, 3]);
        blob[DV_LENGTH_PREFIX_SIZE] ^= 0xFF;
        rewrite_valid_crc(&mut blob);
        assert_rejects(&blob, "Invalid deletion vector magic");
    }

    /// Risk pinned: the CRC check actually fires — a single corrupted bitmap byte (stored CRC
    /// untouched) must be rejected as a CRC mismatch BEFORE the bitmap parser sees it. This is
    /// the mutation-(a) sentinel.
    #[test]
    fn test_dv_blob_crc_mismatch_rejects() {
        let mut blob = encode_deletion_vector_v1(&[1, 2, 3]);
        let corrupt_at = blob.len() - DV_CRC_SIZE - 1;
        blob[corrupt_at] ^= 0x01;
        assert_rejects(&blob, "Invalid deletion vector CRC");

        // The stored CRC itself corrupted must also reject.
        let mut blob = encode_deletion_vector_v1(&[1, 2, 3]);
        let crc_at = blob.len() - 1;
        blob[crc_at] ^= 0x01;
        assert_rejects(&blob, "Invalid deletion vector CRC");
    }

    /// Risk pinned: a length prefix disagreeing with the actual payload (both directions, and a
    /// hostile u32::MAX that would overflow naive arithmetic) must reject cleanly.
    #[test]
    fn test_dv_blob_length_prefix_mismatch_rejects() {
        let blob = encode_deletion_vector_v1(&[1, 2, 3]);

        let mut longer = blob.clone();
        let declared = u32::from_be_bytes(longer[..4].try_into().expect("4 bytes")) + 1;
        longer[..4].copy_from_slice(&declared.to_be_bytes());
        assert_rejects(&longer, "length prefix declares");

        let mut shorter = blob.clone();
        let declared = u32::from_be_bytes(shorter[..4].try_into().expect("4 bytes")) - 1;
        shorter[..4].copy_from_slice(&declared.to_be_bytes());
        assert_rejects(&shorter, "length prefix declares");

        let mut hostile = blob.clone();
        hostile[..4].copy_from_slice(&u32::MAX.to_be_bytes());
        assert_rejects(&hostile, "length prefix declares");

        // A declared length too small to even hold magic + bitmap count: framing an EMPTY
        // bitmap region declares length 4 (magic only) < the 12-byte minimum.
        let tiny = frame_deletion_vector_v1(&[]);
        assert_rejects(&tiny, "shorter than the minimum");
    }

    /// Risk pinned: garbage where the 32-bit bitmap should be (CRC recomputed so it passes the
    /// checksum) must be rejected by the checked bitmap parser, not panic or wrongly decode.
    #[test]
    fn test_dv_blob_garbage_bitmap_bytes_reject() {
        // count = 1, key = 0, then garbage instead of a serialized bitmap.
        let mut bitmap_bytes = Vec::new();
        bitmap_bytes.extend_from_slice(&1u64.to_le_bytes());
        bitmap_bytes.extend_from_slice(&0u32.to_le_bytes());
        bitmap_bytes.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF, 0xDE, 0xAD, 0xBE, 0xEF]);
        let blob = frame_deletion_vector_v1(&bitmap_bytes);
        assert_rejects(&blob, "malformed 32-bit roaring bitmap");
    }

    /// Risk pinned: a bitmap count larger than the payload could hold (including u64::MAX, which
    /// would loop ~forever or overflow naive size math) must fail fast with a named error.
    #[test]
    fn test_dv_blob_bitmap_count_overflow_rejects() {
        // count = u64::MAX over an empty remainder.
        let blob = frame_deletion_vector_v1(&u64::MAX.to_le_bytes());
        assert_rejects(&blob, "exceeds the maximum");

        // A count far larger than the remaining bytes could hold fails the fit bound by name.
        let single = encode_deletion_vector_v1(&[1, 2, 3]);
        let mut bitmap_bytes =
            single[DV_LENGTH_PREFIX_SIZE + DV_MAGIC_SIZE..single.len() - DV_CRC_SIZE].to_vec();
        bitmap_bytes[..8].copy_from_slice(&1000u64.to_le_bytes());
        let blob = frame_deletion_vector_v1(&bitmap_bytes);
        assert_rejects(&blob, "cannot fit");

        // count = 2 with one entry present sneaks under the minimum-size bound but must still
        // reject cleanly when the second bitmap's bytes run out.
        let mut bitmap_bytes =
            single[DV_LENGTH_PREFIX_SIZE + DV_MAGIC_SIZE..single.len() - DV_CRC_SIZE].to_vec();
        bitmap_bytes[..8].copy_from_slice(&2u64.to_le_bytes());
        let blob = frame_deletion_vector_v1(&bitmap_bytes);
        assert_rejects(&blob, "truncated before a bitmap key");
    }

    /// Risk pinned: Java rejects out-of-range and non-ascending keys (`readKey`) — accepting
    /// them would silently reorder or alias position ranges.
    #[test]
    fn test_dv_blob_invalid_keys_reject() {
        let mut one = RoaringBitmap::new();
        one.insert(1);

        // Key with the sign bit set (reads negative in Java).
        let blob = encode_deletion_vector_v1_from_pairs(&[(u32::MAX, one.clone())]);
        assert_rejects(&blob, "exceeds the maximum");

        // Key == Integer.MAX_VALUE (Java allows at most MAX_VALUE - 1).
        let blob = encode_deletion_vector_v1_from_pairs(&[(i32::MAX as u32, one.clone())]);
        assert_rejects(&blob, "exceeds the maximum");

        // Non-ascending keys.
        let blob = encode_deletion_vector_v1_from_pairs(&[(5, one.clone()), (3, one.clone())]);
        assert_rejects(&blob, "strictly ascending");

        // Duplicate keys.
        let blob = encode_deletion_vector_v1_from_pairs(&[(5, one.clone()), (5, one)]);
        assert_rejects(&blob, "strictly ascending");
    }

    /// Risk pinned (reviewer, 2026-06-10): DoS-by-allocation via the INNER 32-bit roaring
    /// container count — the outer bitmap_count bound cannot constrain what the per-key payload
    /// claims, so a hostile cookie/size must be rejected by `RoaringBitmap::deserialize_from`
    /// fast and allocation-bounded (roaring 0.11.3 caps the container count at 65536 BEFORE its
    /// description allocation), never looped over or panicked on.
    #[test]
    fn test_dv_blob_hostile_inner_container_count_rejects_fast() {
        // count = 1, key = 0, then a no-run cookie (12346) claiming u32::MAX containers.
        let mut bitmap_bytes = Vec::new();
        bitmap_bytes.extend_from_slice(&1u64.to_le_bytes());
        bitmap_bytes.extend_from_slice(&0u32.to_le_bytes());
        bitmap_bytes.extend_from_slice(&12346u32.to_le_bytes()); // SERIAL_COOKIE_NO_RUNCONTAINER
        bitmap_bytes.extend_from_slice(&u32::MAX.to_le_bytes()); // hostile container count
        let blob = frame_deletion_vector_v1(&bitmap_bytes);
        let start = std::time::Instant::now();
        assert_rejects(&blob, "malformed 32-bit roaring bitmap");
        assert!(
            start.elapsed().as_secs() < 2,
            "must fail fast, no huge alloc/loop"
        );

        // Run cookie (12347) with max upper-16 size (65536 containers) over an empty payload.
        let mut bitmap_bytes = Vec::new();
        bitmap_bytes.extend_from_slice(&1u64.to_le_bytes());
        bitmap_bytes.extend_from_slice(&0u32.to_le_bytes());
        let run_cookie: u32 = 12347 | (0xFFFFu32 << 16);
        bitmap_bytes.extend_from_slice(&run_cookie.to_le_bytes());
        let blob = frame_deletion_vector_v1(&bitmap_bytes);
        // Tripped EARLIER by the outer fit bound (the entry is smaller than the 12-byte minimum).
        assert_rejects(&blob, "cannot fit");

        // Same run cookie but padded past the outer fit bound so the inner parser sees it.
        let mut bitmap_bytes = Vec::new();
        bitmap_bytes.extend_from_slice(&1u64.to_le_bytes());
        bitmap_bytes.extend_from_slice(&0u32.to_le_bytes());
        bitmap_bytes.extend_from_slice(&run_cookie.to_le_bytes());
        bitmap_bytes.extend_from_slice(&[0u8; 16]);
        let blob = frame_deletion_vector_v1(&bitmap_bytes);
        let start = std::time::Instant::now();
        assert_rejects(&blob, "malformed 32-bit roaring bitmap");
        assert!(start.elapsed().as_secs() < 2, "must fail fast");

        // No-run cookie with a count just under the inner cap (65536) but no payload.
        let mut bitmap_bytes = Vec::new();
        bitmap_bytes.extend_from_slice(&1u64.to_le_bytes());
        bitmap_bytes.extend_from_slice(&0u32.to_le_bytes());
        bitmap_bytes.extend_from_slice(&12346u32.to_le_bytes());
        bitmap_bytes.extend_from_slice(&65536u32.to_le_bytes());
        let blob = frame_deletion_vector_v1(&bitmap_bytes);
        assert_rejects(&blob, "malformed 32-bit roaring bitmap");
    }

    /// Risk pinned (reviewer, 2026-06-10): the ACCEPT side of the key boundary — Java `readKey`
    /// accepts keys up to exactly `Integer.MAX_VALUE - 1`; the builder pinned the reject side
    /// (i32::MAX, u32::MAX). Over-tightening the bound would reject valid Java-written DVs with
    /// high keys, shrinking the accepted-input set below Java's.
    #[test]
    fn test_dv_blob_max_valid_key_boundary_accepted() {
        let mut one = RoaringBitmap::new();
        one.insert(1);
        let key = i32::MAX as u32 - 1;
        let blob = encode_deletion_vector_v1_from_pairs(&[(key, one)]);
        let decoded = DeleteVector::deserialize_deletion_vector_v1(&blob)
            .expect("key i32::MAX-1 is the largest key Java accepts");
        let positions: Vec<u64> = decoded.iter().collect();
        assert_eq!(positions, vec![(u64::from(key) << 32) | 1]);
    }

    /// Risk pinned: trailing bytes inside the declared (CRC-covered) bitmap region mean the
    /// length prefix and the bitmap disagree — silent acceptance would mask corruption.
    #[test]
    fn test_dv_blob_trailing_bytes_after_bitmaps_reject() {
        let valid = encode_deletion_vector_v1(&[1, 2, 3]);
        let mut bitmap_bytes =
            valid[DV_LENGTH_PREFIX_SIZE + DV_MAGIC_SIZE..valid.len() - DV_CRC_SIZE].to_vec();
        bitmap_bytes.extend_from_slice(&[0u8; 3]);
        let blob = frame_deletion_vector_v1(&bitmap_bytes);
        assert_rejects(&blob, "trailing bytes");
    }

    /// EMPIRICAL Java byte-compatibility pin (env-gated; run by dev/java-interop/run-interop-dv.sh).
    /// The Java oracle serializes a `BitmapPositionDeleteIndex` with positions spanning the
    /// 32-bit key boundary AND a run-length-encoded range into `<dir>/dv_blob.bin` (+ the
    /// expected positions in `dv_blob_expected.json`); this test decodes the REAL Java bytes and
    /// asserts the exact position set. This is the test that settles whether `roaring-rs`'s
    /// portable format is byte-compatible with Java's `RoaringPositionBitmap`.
    #[test]
    fn test_dv_blob_decodes_java_written_blob_when_env_set() {
        let Some(dir) = std::env::var_os("ICEBERG_INTEROP_DV_DIR")
            .filter(|value| !value.is_empty())
            .map(std::path::PathBuf::from)
        else {
            println!(
                "skipping java DV blob decode pin — set ICEBERG_INTEROP_DV_DIR \
                 (run dev/java-interop/run-interop-dv.sh)"
            );
            return;
        };

        let blob = std::fs::read(dir.join("dv_blob.bin")).expect("read dv_blob.bin");
        let expected_json =
            std::fs::read_to_string(dir.join("dv_blob_expected.json")).expect("read expected json");
        let expected: Vec<u64> =
            serde_json::from_str(&expected_json).expect("parse expected positions");

        let decoded = DeleteVector::deserialize_deletion_vector_v1(&blob)
            .expect("Java-written deletion-vector-v1 blob must decode");

        let decoded_positions: Vec<u64> = decoded.iter().collect();
        assert_eq!(
            decoded_positions, expected,
            "Rust-decoded positions must equal the positions Java serialized"
        );
    }

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
}
