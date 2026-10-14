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

//! Deletion vectors: the deleted row positions of one data file, as a Puffin
//! `deletion-vector-v1` blob decodes to. Public so an engine that reads
//! Parquet with a reader of its own can still decode the vectors Iceberg
//! writes.

use std::ops::BitOrAssign;

use bytes::Buf;
use roaring::bitmap::Iter;
use roaring::treemap::BitmapIter;
use roaring::{RoaringBitmap, RoaringTreemap};

use crate::error::invalid_data;
use crate::{Error, ErrorKind, Result};

/// Magic bytes prefixing a serialized `deletion-vector-v1` bitmap, per the Iceberg Puffin spec.
/// Iceberg-Java stores these as the little-endian int 1681511377 (0x6439D3D1).
const DV_MAGIC: [u8; 4] = [0xD1, 0xD3, 0x39, 0x64];
const DV_LENGTH_PREFIX_BYTES: usize = 4;
const DV_MAGIC_BYTES: usize = 4;
const DV_CRC_BYTES: usize = 4;
const DV_MIN_BLOB_BYTES: usize = DV_LENGTH_PREFIX_BYTES + DV_MAGIC_BYTES + DV_CRC_BYTES;

/// The deleted row positions of one data file: a set of 64-bit positions,
/// kept as a [`RoaringTreemap`].
#[derive(Debug, Default)]
pub struct DeleteVector {
    inner: RoaringTreemap,
}

impl DeleteVector {
    /// Wraps `roaring_treemap`, whose members are the deleted positions.
    pub fn new(roaring_treemap: RoaringTreemap) -> DeleteVector {
        DeleteVector {
            inner: roaring_treemap,
        }
    }

    /// Iterates the deleted positions in ascending order.
    pub fn iter(&self) -> DeleteVectorIterator<'_> {
        let outer = self.inner.bitmaps();
        DeleteVectorIterator { outer, inner: None }
    }

    /// Marks `pos` as deleted, returning whether it was not already.
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

    /// How many positions are deleted.
    pub fn len(&self) -> u64 {
        self.inner.len()
    }

    /// Whether no position is deleted.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns the deleted positions as ascending, non-overlapping half-open ranges.
    ///
    /// Adjacent ranges are not always merged: a run that crosses a 2^32 boundary comes out as
    /// two ranges.
    pub(crate) fn ranges(&self) -> DeleteVectorRanges<'_> {
        DeleteVectorRanges {
            bitmaps: self.inner.bitmaps(),
            high_bits: 0,
            bitmap_iter: None,
            pending: None,
            pending_range: None,
        }
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
    /// computed over the magic and vector. `vector` is a roaring bitmap in the portable 64-bit
    /// format: a directory of 32-bit key / 32-bit roaring bitmap pairs, ordered by unsigned
    /// comparison of the keys, one bitmap per key.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::DataInvalid`] if the blob is shorter than the minimum, the length
    /// prefix or CRC does not match, the magic is wrong, the roaring bitmap count exceeds the
    /// portable format's maximum, the roaring directory's keys are not ordered by unsigned
    /// comparison, or the roaring payload fails to decode.
    pub fn deserialize(blob: &[u8]) -> Result<Self> {
        if blob.len() < DV_MIN_BLOB_BYTES {
            return Err(invalid_data!(
                "deletion-vector-v1 blob is {} bytes, shorter than the {DV_MIN_BLOB_BYTES}-byte minimum",
                blob.len()
            ));
        }

        // The magic and vector, i.e. the bytes covered by both the length prefix and the CRC.
        let body = &blob[DV_LENGTH_PREFIX_BYTES..blob.len() - DV_CRC_BYTES];
        verify_length_prefix(&blob[..DV_LENGTH_PREFIX_BYTES], body)?;

        // Verify the CRC before interpreting any bytes so a corrupt blob yields a single clear
        // error rather than an opaque roaring decode failure.
        verify_crc(body, &blob[blob.len() - DV_CRC_BYTES..])?;

        let (magic, vector) = body.split_at(DV_MAGIC_BYTES);
        verify_magic(magic)?;

        let inner = decode_roaring_directory(vector)?;

        Ok(DeleteVector { inner })
    }
}

fn verify_length_prefix(mut prefix: &[u8], body: &[u8]) -> Result<()> {
    let declared_len = prefix.try_get_u32().map_err(|e| {
        Error::new(
            ErrorKind::Unexpected,
            "failed to read the deletion-vector-v1 length prefix",
        )
        .with_source(e)
    })? as usize;
    if declared_len != body.len() {
        return Err(invalid_data!(
            "deletion-vector-v1 length prefix is {declared_len}, expected {}",
            body.len()
        ));
    }
    Ok(())
}

fn verify_crc(body: &[u8], mut crc_bytes: &[u8]) -> Result<()> {
    let stored_crc = crc_bytes.try_get_u32().map_err(|e| {
        Error::new(
            ErrorKind::Unexpected,
            "failed to read the deletion-vector-v1 CRC",
        )
        .with_source(e)
    })?;
    let computed_crc = crc32fast::hash(body);
    if computed_crc != stored_crc {
        return Err(invalid_data!(
            "deletion-vector-v1 CRC mismatch: computed {computed_crc:#010x}, stored {stored_crc:#010x}"
        ));
    }
    Ok(())
}

fn verify_magic(magic: &[u8]) -> Result<()> {
    if magic != DV_MAGIC {
        return Err(invalid_data!(
            "deletion-vector-v1 magic mismatch: {magic:02x?}, expected {DV_MAGIC:02x?}"
        ));
    }
    Ok(())
}

// The Puffin spec defines the roaring directory as the bitmaps "ordered by unsigned comparison
// of the 32-bit keys", with one bitmap per key. Walk it ourselves (rather than
// `RoaringTreemap::deserialize_from`, which stores keys in a `BTreeMap` via a plain insert and
// would silently accept a stream with duplicate or out-of-order keys, discarding the earlier
// bitmap on a duplicate) so a non-conformant blob is rejected instead of decoded into a value
// that doesn't match what was actually written.
fn decode_roaring_directory(mut reader: &[u8]) -> Result<RoaringTreemap> {
    let bitmap_count = reader.try_get_u64_le().map_err(|e| {
        invalid_data!("failed to decode deletion-vector-v1 roaring payload").with_source(e)
    })?;
    // The roaring portable format restricts the bitmap count to [0, 2^32 - 1] (it is stored as a
    // u64 with the upper 32 bits reserved as zero padding).
    if bitmap_count > u32::MAX as u64 {
        return Err(invalid_data!(
            "deletion-vector-v1 roaring bitmap count {bitmap_count} exceeds the {}-key maximum",
            u32::MAX
        ));
    }

    let mut bitmaps = Vec::new();
    let mut last_key: Option<u32> = None;
    for _ in 0..bitmap_count {
        let key = reader.try_get_u32_le().map_err(|e| {
            invalid_data!("failed to decode deletion-vector-v1 roaring payload").with_source(e)
        })?;
        if let Some(last) = last_key
            && key <= last
        {
            return Err(invalid_data!(
                "deletion-vector-v1 roaring keys must be ordered by unsigned comparison, got key {key} after {last}"
            ));
        }
        last_key = Some(key);

        let bitmap = RoaringBitmap::deserialize_from(&mut reader).map_err(|e| {
            invalid_data!("failed to decode deletion-vector-v1 roaring payload").with_source(e)
        })?;
        bitmaps.push((key, bitmap));
    }

    // `bitmaps` is already sorted by key, but `roaring` has no constructor that accepts
    // pre-sorted pairs without re-sorting them; revisit if that changes upstream.
    Ok(RoaringTreemap::from_bitmaps(bitmaps))
}

// Ideally, we'd just wrap `roaring::RoaringTreemap`'s iterator, `roaring::treemap::Iter` here.
// But right now, it does not have a corresponding implementation of `roaring::bitmap::Iter::advance_to`,
// which `advance_to` below needs.
// There is a PR open on roaring to add this (https://github.com/RoaringBitmap/roaring-rs/pull/314)
// and if that gets merged then we can simplify `DeleteVectorIterator` here, refactoring `advance_to`
// to just a wrapper around the underlying iterator's method.
/// Iterates the deleted positions of a [`DeleteVector`] in ascending order.
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
    /// Skips the positions below `pos`, so the next one yielded is the first
    /// at or after it. Does nothing before the first position is yielded.
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

/// Number of consecutive positions [`DeleteVectorRanges`] walks one by one before it asks the
/// bitmap for the rest of the run.
///
/// `roaring`'s `next_range` binary-searches the rest of an array container on every call, which
/// costs more than `next` when most runs are a single position, so it only pays off for long
/// runs.
const RUN_PROBE_LEN: u32 = 4;

/// Iterator over a [`DeleteVector`]'s deleted positions as half-open ranges, see
/// [`DeleteVector::ranges`].
pub(crate) struct DeleteVectorRanges<'a> {
    bitmaps: BitmapIter<'a>,
    /// High 32 bits of the positions in `bitmap_iter`.
    high_bits: u32,
    bitmap_iter: Option<Iter<'a>>,
    /// A position already taken from `bitmap_iter` that starts the next range.
    pending: Option<u32>,
    /// A complete range already taken from `bitmap_iter`, returned next.
    pending_range: Option<(u32, u32)>,
}

impl DeleteVectorRanges<'_> {
    fn full_range(&self, start: u32, end_inclusive: u32) -> (u64, u64) {
        let base = u64::from(self.high_bits) << 32;
        (
            base | u64::from(start),
            (base | u64::from(end_inclusive)) + 1,
        )
    }

    /// Skips every deleted position below `pos`. A range that starts below `pos` and ends after
    /// it is cut to start at `pos`.
    pub(crate) fn skip_to(&mut self, pos: u64) {
        let hi = (pos >> 32) as u32;
        let lo = pos as u32;

        if self.bitmap_iter.is_some() && self.high_bits > hi {
            return;
        }
        if self.bitmap_iter.is_some() && self.high_bits == hi {
            if let Some((start, end)) = self.pending_range {
                if end < lo {
                    self.pending_range = None;
                } else if start < lo {
                    self.pending_range = Some((lo, end));
                }
            }
            if self.pending.is_some_and(|p| p < lo) {
                self.pending = None;
            }
            if let Some(iter) = self.bitmap_iter.as_mut() {
                iter.advance_to(lo);
            }
            return;
        }

        // `pos` is in a later bitmap: drop everything buffered from the current one.
        self.bitmap_iter = None;
        self.pending = None;
        self.pending_range = None;
        for (high_bits, bitmap) in self.bitmaps.by_ref() {
            if high_bits < hi {
                continue;
            }
            let mut iter = bitmap.iter();
            if high_bits == hi {
                iter.advance_to(lo);
            }
            self.high_bits = high_bits;
            self.bitmap_iter = Some(iter);
            return;
        }
    }
}

impl Iterator for DeleteVectorRanges<'_> {
    type Item = (u64, u64);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some((start, end)) = self.pending_range.take() {
                return Some(self.full_range(start, end));
            }

            if let Some(iter) = self.bitmap_iter.as_mut()
                && let Some(start) = self.pending.take().or_else(|| iter.next())
            {
                // Walk short runs with `next`, and ask for the rest of a long run at once.
                let mut end = start;
                let mut walked = 0;
                loop {
                    if walked == RUN_PROBE_LEN {
                        match iter.next_range() {
                            Some(run) if Some(*run.start()) == end.checked_add(1) => {
                                end = *run.end();
                            }
                            Some(run) => self.pending_range = Some((*run.start(), *run.end())),
                            None => {}
                        }
                        break;
                    }
                    match iter.next() {
                        Some(next) if Some(next) == end.checked_add(1) => {
                            end = next;
                            walked += 1;
                        }
                        next => {
                            self.pending = next;
                            break;
                        }
                    }
                }
                return Some(self.full_range(start, end));
            }

            let (high_bits, bitmap) = self.bitmaps.next()?;
            self.high_bits = high_bits;
            self.bitmap_iter = Some(bitmap.iter());
        }
    }
}

impl BitOrAssign for DeleteVector {
    fn bitor_assign(&mut self, other: Self) {
        self.inner.bitor_assign(&other.inner);
    }
}

// Reproduces Iceberg-Java's `deletion-vector-v1` framing so tests can round-trip through
// `deserialize` without a Java writer, and so other test modules can build blob fixtures.
// Cross-implementation golden fixtures produced by Iceberg-Java are tracked separately; this
// only checks that our decode matches our encode.
#[cfg(test)]
pub(crate) fn frame_dv_blob(vector: &[u8]) -> Vec<u8> {
    let body_len = DV_MAGIC_BYTES + vector.len();
    let mut blob = Vec::with_capacity(DV_LENGTH_PREFIX_BYTES + body_len + DV_CRC_BYTES);
    blob.extend_from_slice(&(body_len as u32).to_be_bytes());
    blob.extend_from_slice(&DV_MAGIC);
    blob.extend_from_slice(vector);
    let crc = crc32fast::hash(&blob[DV_LENGTH_PREFIX_BYTES..]);
    blob.extend_from_slice(&crc.to_be_bytes());
    blob
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic pseudo-random sets mixing single positions, short and long runs, and runs
    /// across 2^16 and 2^32 boundaries.
    fn mixed_delete_vectors() -> Vec<DeleteVector> {
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let mut rand = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };

        let mut vectors = vec![DeleteVector::default()];
        for case in 0..40u64 {
            let mut dv = DeleteVector::default();
            let base = if case % 4 == 0 { (1u64 << 32) - 40 } else { 0 };
            let mut pos = base + rand() % 50;
            for _ in 0..(rand() % 60) {
                let run = match rand() % 5 {
                    0 => 1,
                    1 => 2,
                    // one past the probe length: the rest comes from `next_range`
                    2 => u64::from(RUN_PROBE_LEN) + 1,
                    3 => 1 + rand() % 40,
                    _ => 1 + rand() % 70_000,
                };
                dv.inner.insert_range(pos..pos + run);
                pos += run + 1 + rand() % 30;
            }
            vectors.push(dv);
        }
        let mut boundaries = DeleteVector::default();
        boundaries.inner.insert_range((1 << 16) - 3..(1 << 16) + 3);
        boundaries
            .inner
            .insert_range((1 << 32) - 20..(1 << 32) + 20);
        boundaries.inner.insert((2 << 32) + 7);
        vectors.push(boundaries);
        vectors
    }

    #[test]
    fn test_ranges_cover_exactly_the_deleted_positions() {
        for dv in mixed_delete_vectors() {
            let positions: Vec<u64> = dv.iter().collect();
            let mut expanded = Vec::new();
            let mut last_end = None;
            for (start, end) in dv.ranges() {
                assert!(start < end, "empty range {start}..{end}");
                assert!(
                    last_end.is_none_or(|last| last <= start),
                    "ranges out of order"
                );
                last_end = Some(end);
                expanded.extend(start..end);
            }
            assert_eq!(expanded, positions);
        }
    }

    #[test]
    fn test_ranges_skip_to_matches_model() {
        let mut state = 7u64;
        let mut rand = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };

        for dv in mixed_delete_vectors() {
            let mut remaining: std::collections::BTreeSet<u64> = dv.iter().collect();
            let max = remaining.last().copied().unwrap_or(0) + 10;
            let mut ranges = dv.ranges();
            let mut target = 0u64;
            loop {
                if rand() % 3 == 0 {
                    target = target.max(remaining.first().copied().unwrap_or(max));
                    target += rand() % 200;
                    ranges.skip_to(target);
                    remaining.retain(|&p| p >= target);
                    continue;
                }
                match ranges.next() {
                    Some((start, end)) => {
                        assert_eq!(Some(start), remaining.first().copied());
                        for p in start..end {
                            assert!(remaining.remove(&p), "{p} returned but not deleted");
                        }
                        target = target.max(end);
                    }
                    None => {
                        assert!(remaining.is_empty(), "missed {remaining:?}");
                        break;
                    }
                }
            }
        }
    }

    #[test]
    fn test_ranges_skip_to_inside_buffered_run() {
        // After a run of exactly RUN_PROBE_LEN + 1, the following run is already buffered.
        let probe = u64::from(RUN_PROBE_LEN);
        let mut dv = DeleteVector::default();
        dv.inner.insert_range(0..probe + 1);
        dv.inner.insert_range(probe + 3..probe + 13);
        let mut ranges = dv.ranges();
        assert_eq!(ranges.next(), Some((0, probe + 1)));
        ranges.skip_to(probe + 8);
        assert_eq!(ranges.next(), Some((probe + 8, probe + 13)));
        assert_eq!(ranges.next(), None);
    }

    // The two cases from https://github.com/apache/iceberg-rust/issues/3400.
    #[test]
    fn test_ranges_skip_to_across_2_pow_32() {
        // Target in a later bitmap that doesn't exist: nothing is left.
        let dv = DeleteVector::new(RoaringTreemap::from_iter([1u64, 5, 9]));
        let mut ranges = dv.ranges();
        assert_eq!(ranges.next(), Some((1, 2)));
        ranges.skip_to((1 << 32) + 5);
        assert_eq!(ranges.next(), None);

        // Target below the current bitmap: no-op.
        let high = 1u64 << 32;
        let dv = DeleteVector::new(RoaringTreemap::from_iter([high + 1, high + 2, high + 9]));
        let mut ranges = dv.ranges();
        assert_eq!(ranges.next(), Some((high + 1, high + 3)));
        ranges.skip_to(10);
        assert_eq!(ranges.next(), Some((high + 9, high + 10)));
        assert_eq!(ranges.next(), None);
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

    fn encode_dv_blob(dv: &DeleteVector) -> Vec<u8> {
        let mut vector = Vec::with_capacity(dv.inner.serialized_size());
        dv.inner.serialize_into(&mut vector).unwrap();
        frame_dv_blob(&vector)
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
    fn test_deserialize_roundtrip_empty() {
        let blob = encode_dv_blob(&DeleteVector::default());
        assert_eq!(DeleteVector::deserialize(&blob).unwrap().len(), 0);
    }

    #[test]
    fn test_deserialize_roundtrip_small() {
        let positions = [0u64, 5, 100, 1000];
        let dv = DeleteVector::deserialize(&encode_dv_blob(&dv_of(positions))).unwrap();
        assert_eq!(sorted(&dv), positions);
    }

    #[test]
    fn test_deserialize_roundtrip_spanning_64bit_keys() {
        let positions = [1u64, 1 << 33, (1 << 33) + 5, 1 << 34];
        let dv = DeleteVector::deserialize(&encode_dv_blob(&dv_of(positions))).unwrap();
        assert_eq!(sorted(&dv), positions);
    }

    // Java run-optimizes every deletion vector before writing, so real blobs carry RUN
    // containers, which use the SERIAL_COOKIE roaring layout. Force that layout so decode
    // exercises the run-container path rather than only array and bitmap containers.
    #[test]
    fn test_deserialize_roundtrip_run_optimized() {
        let mut dv = dv_of(0..10_000);
        assert!(
            dv.inner.optimize(),
            "expected a dense range to run-length encode"
        );
        let decoded = DeleteVector::deserialize(&encode_dv_blob(&dv)).unwrap();
        assert_eq!(decoded.len(), 10_000);
        let positions = sorted(&decoded);
        assert_eq!(positions.first(), Some(&0));
        assert_eq!(positions.last(), Some(&9_999));
    }

    #[test]
    fn test_deserialize_rejects_short_blob() {
        let err = DeleteVector::deserialize(&[0u8; DV_MIN_BLOB_BYTES - 1]).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    #[test]
    fn test_deserialize_rejects_bad_magic() {
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
    fn test_deserialize_rejects_bad_crc() {
        let mut blob = encode_dv_blob(&dv_of([1, 2, 3]));
        let end = blob.len() - DV_CRC_BYTES;
        blob[end] ^= 0xFF;
        let err = DeleteVector::deserialize(&blob).unwrap_err();
        assert!(err.message().contains("CRC mismatch"), "got: {err}");
    }

    #[test]
    fn test_deserialize_rejects_length_prefix_mismatch() {
        let mut blob = encode_dv_blob(&dv_of([1]));
        let declared = u32::from_be_bytes(blob[..DV_LENGTH_PREFIX_BYTES].try_into().unwrap());
        blob[..DV_LENGTH_PREFIX_BYTES].copy_from_slice(&(declared + 1).to_be_bytes());
        let err = DeleteVector::deserialize(&blob).unwrap_err();
        assert!(err.message().contains("length prefix"), "got: {err}");
    }

    // Crafts a raw roaring treemap directory (bitmap count header + key/bitmap entries) so tests
    // can exercise key-ordering violations that `dv_of`/`encode_dv_blob` can never produce, since
    // `DeleteVector::insert` always keeps keys unique and ascending.
    fn raw_roaring_vector(entries: &[(u32, &[u32])]) -> Vec<u8> {
        let mut vector = Vec::new();
        vector.extend_from_slice(&(entries.len() as u64).to_le_bytes());
        for (key, positions) in entries {
            let mut bitmap = RoaringBitmap::new();
            for &pos in *positions {
                bitmap.insert(pos);
            }
            vector.extend_from_slice(&key.to_le_bytes());
            bitmap.serialize_into(&mut vector).unwrap();
        }
        vector
    }

    // `RoaringTreemap::deserialize_from` stores keys in a `BTreeMap` via a plain insert, so
    // without our own ordering check, decoding this would silently keep only the second bitmap
    // for key 5 (position 2) and drop the first (position 1).
    #[test]
    fn test_deserialize_rejects_duplicate_keys() {
        let vector = raw_roaring_vector(&[(5, &[1]), (5, &[2])]);
        let err = DeleteVector::deserialize(&frame_dv_blob(&vector)).unwrap_err();
        assert!(err.message().contains("unsigned comparison"), "got: {err}");
    }

    // The Puffin spec requires the roaring directory's keys to be "ordered by unsigned
    // comparison"; a stream with unique but out-of-order keys is not a conformant blob even
    // though `BTreeMap` would happily reorder it into a correct-looking result.
    #[test]
    fn test_deserialize_rejects_out_of_order_keys() {
        let vector = raw_roaring_vector(&[(5, &[1]), (3, &[2])]);
        let err = DeleteVector::deserialize(&frame_dv_blob(&vector)).unwrap_err();
        assert!(err.message().contains("unsigned comparison"), "got: {err}");
    }

    // The roaring portable format stores the bitmap count as a u64 with the upper 32 bits
    // reserved as zero padding, restricting it to [0, 2^32 - 1]; a value above that is not a
    // conformant blob, regardless of whether any key/bitmap entries follow.
    #[test]
    fn test_deserialize_rejects_bitmap_count_overflow() {
        let vector = (u32::MAX as u64 + 1).to_le_bytes().to_vec();
        let err = DeleteVector::deserialize(&frame_dv_blob(&vector)).unwrap_err();
        assert!(err.message().contains("exceeds the"), "got: {err}");
    }
}
