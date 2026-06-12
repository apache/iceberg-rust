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

//! Theta sketch (update form) + CompactSketch v3 serialization, byte-compatible with Apache
//! DataSketches `org.apache.datasketches.theta`.
//!
//! The byte format is the payload of Iceberg's `apache-datasketches-theta-v1` Puffin blob. The
//! update path ([`ThetaSketch`]) is a one-to-one port of `HeapQuickSelectSketch` (hash table +
//! quickselect theta-lowering) so that, given the same inputs, the retained hash set and `theta`
//! match Java to the bit. The compact serialization ([`ThetaSketch::serialize_compact`]) is the
//! `HeapCompactSketch.toByteArray` layout: a 1/2/3-long preamble followed by the ascending-sorted
//! retained hashes.
//!
//! Format facts (1.10.0 jar `theta/PreambleUtil`, `Family`):
//! - SerVer = 3, FamilyID = 3 (COMPACT, preLongs 1..=3).
//! - Preamble bytes: 0 preLongs|lgResizeFactor, 1 serVer, 2 family, 3 lgNomLongs, 4 lgArrLongs,
//!   5 flags, 6-7 seedHash(LE u16), 8-11 retainedEntries(LE i32), 12-15 P(LE f32),
//!   16-23 thetaLong(LE i64).
//! - Flags: EMPTY=4, COMPACT=8, ORDERED=16, SINGLEITEM=32 (READ_ONLY=2 is always set on compact).
//! - Compact sketches zero bytes 3 and 4 (lgNomLongs/lgArrLongs are update-only state).

use crate::error::{SketchError, SketchResult};
use crate::hash::{DEFAULT_UPDATE_SEED, compute_seed_hash, hash_bytes, hash_long};

/// `SerVer` byte for theta serialization version 3.
const SERIAL_VERSION_3: u8 = 3;
/// `FamilyID` byte for the theta `COMPACT` family.
const FAMILY_COMPACT: u8 = 3;
/// Maximum theta as a `long` — `Long.MAX_VALUE`. Exact-mode sketches keep this value.
pub const MAX_THETA: i64 = i64::MAX;
/// The default DataSketches sampling probability `p`, serialized as a float at preamble bytes 12-15.
const DEFAULT_P: f32 = 1.0;
/// The DataSketches `9.223372036854776E18` double literal (`2^63` rounded to the nearest `f64`). Both
/// the standard estimate and the Alpha construction math multiply by this exact constant; it equals
/// [`MAX_THETA`] `as f64` to the bit (`0x43e0_0000_0000_0000`).
pub(crate) const TWO_POW_63_AS_F64: f64 = 9.223372036854776e18;
/// Read-only flag bit — always set on a compact (serialized) sketch.
const FLAG_READ_ONLY: u8 = 2;
/// Empty flag bit.
const FLAG_EMPTY: u8 = 4;
/// Compact flag bit.
const FLAG_COMPACT: u8 = 8;
/// Ordered flag bit.
const FLAG_ORDERED: u8 = 16;
/// Single-item flag bit.
const FLAG_SINGLEITEM: u8 = 32;
/// Low-5-flag-bits pattern (`READ_ONLY | COMPACT | ORDERED` = 2|8|16 = 26) that marks a *legacy*
/// single-item compact sketch with `preLongs == 1` but WITHOUT the explicit [`FLAG_SINGLEITEM`] bit.
/// DataSketches' `SingleItemSketch.otherCheckForSingleItem(preLongs, serVer, famId, flags)` treats a
/// v3 compact 1-long preamble whose `flags & 31 == 26` as a single item even when bit 32 is clear.
const FLAG_LEGACY_SINGLE_LOW5: u8 = 26;
/// DataSketches default lg resize factor (`X8`).
const DEFAULT_LG_RESIZE_FACTOR: u8 = 3;
/// Minimum lg array size for the update hash table.
const MIN_LG_ARR_LONGS: u32 = 5;
/// The default lg nominal entries used by Iceberg's `theta_sketch_agg` (`UpdateSketch.builder()`
/// default = 4096 nominal = lgK 12).
pub const DEFAULT_LG_NOMINAL_LONGS: u32 = 12;
/// The DataSketches default lg resize factor as a plain integer (`ResizeFactor.X8.lg() == 3`). Shared
/// by the QuickSelect resize path and the Alpha clean-grow path.
pub(crate) const DEFAULT_LG_RESIZE_FACTOR_LG: u32 = DEFAULT_LG_RESIZE_FACTOR as u32;
/// The minimum lg array size for an update hash table, shared by both update families.
pub(crate) const MIN_LG_ARR_LONGS_SHARED: u32 = MIN_LG_ARR_LONGS;

/// A theta sketch in mutable (update) form — accepts values and retains a bounded hash set.
///
/// Mirrors Java `HeapQuickSelectSketch`: an open-addressing hash table sized `2^lg_arr_longs`,
/// growing up to `2^(lg_nominal_longs + 1)` and then lowering `theta` via quickselect to keep at
/// most `2^lg_nominal_longs` "kept" entries on rebuild.
#[derive(Debug, Clone)]
pub struct ThetaSketch {
    lg_nominal_longs: u32,
    lg_arr_longs: u32,
    seed: u64,
    table: Vec<u64>,
    current_count: usize,
    theta: i64,
    is_empty: bool,
    hash_table_threshold: usize,
}

impl ThetaSketch {
    /// Builds an empty sketch with the Iceberg-default nominal entries (lgK 12) and seed 9001.
    pub fn new() -> Self {
        Self::with_lg_nominal_longs(DEFAULT_LG_NOMINAL_LONGS, DEFAULT_UPDATE_SEED)
    }

    /// Builds an empty sketch with the given `lg_nominal_longs` (lgK) and seed.
    ///
    /// `lg_nominal_longs` is clamped to the legal DataSketches range `4..=26`.
    pub fn with_lg_nominal_longs(lg_nominal_longs: u32, seed: u64) -> Self {
        let lg_nominal_longs = lg_nominal_longs.clamp(4, 26);
        let lg_arr_longs = starting_sub_multiple(
            lg_nominal_longs + 1,
            DEFAULT_LG_RESIZE_FACTOR as u32,
            MIN_LG_ARR_LONGS,
        );
        let hash_table_threshold = set_hash_table_threshold(lg_nominal_longs, lg_arr_longs);
        ThetaSketch {
            lg_nominal_longs,
            lg_arr_longs,
            seed,
            table: vec![0u64; 1usize << lg_arr_longs],
            current_count: 0,
            theta: MAX_THETA,
            is_empty: true,
            hash_table_threshold,
        }
    }

    /// Updates the sketch with the 8-byte little-endian form of a `u64` value (e.g. an Iceberg
    /// long/int column value). Hashes via the long-array path (identical to the LE-8-byte path).
    pub fn update_u64(&mut self, value: u64) {
        let full = hash_long(value, self.seed);
        self.update_hash(full[0] >> 1);
    }

    /// Updates the sketch with a `u64` value via the byte path (LE 8 bytes). Equivalent to
    /// [`Self::update_u64`]; provided for callers that hash a value already serialized to bytes.
    pub fn update_long_bytes(&mut self, value: u64) {
        // Identical hash to update_u64 (Java: hash(long[]{v}) == hash(LE8(v))); kept distinct for
        // call-site clarity. Routes through the long-array path for a single source of truth.
        self.update_u64(value);
    }

    /// Updates the sketch with an arbitrary non-empty byte string (e.g. an Iceberg string/binary
    /// value's UTF-8 / raw bytes). Empty input is silently ignored, mirroring Java's
    /// `UpdateSketch.update(byte[])` which rejects null/empty without retaining anything.
    pub fn update_bytes(&mut self, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        // hash_bytes only errors on empty input, which we already excluded.
        let full = match hash_bytes(bytes, self.seed) {
            Ok(value) => value,
            Err(_) => return,
        };
        self.update_hash(full[0] >> 1);
    }

    /// Updates the sketch from a 63-bit hash key. Mirrors `HeapQuickSelectSketch.hashUpdate`.
    fn update_hash(&mut self, hash: u64) {
        self.is_empty = false;
        // continueCondition: reject if hash >= theta or hash == 0.
        if hash == 0 || hash >= self.theta as u64 {
            return;
        }
        if self.search_or_insert(hash).is_some() {
            return; // duplicate
        }
        self.current_count += 1;
        if self.current_count > self.hash_table_threshold {
            if self.lg_arr_longs <= self.lg_nominal_longs {
                self.resize_table();
            } else {
                self.quick_select_and_rebuild();
            }
        }
    }

    /// Open-addressing search-or-insert. Returns `Some(index)` on duplicate, `None` on insert.
    /// Probe stride mirrors `HashOperations.getStride` (odd double-hashing stride).
    fn search_or_insert(&mut self, hash: u64) -> Option<usize> {
        Self::search_or_insert_into(&mut self.table, self.lg_arr_longs, hash)
    }

    /// Standalone search-or-insert so it can be reused during resize/rebuild without borrow clash.
    fn search_or_insert_into(table: &mut [u64], lg_arr_longs: u32, hash: u64) -> Option<usize> {
        let mask = (1u64 << lg_arr_longs) - 1;
        let stride = get_stride(hash, lg_arr_longs);
        let mut probe = (hash & mask) as usize;
        let loop_index = probe;
        loop {
            let value = table[probe];
            if value == 0 {
                table[probe] = hash;
                return None;
            }
            if value == hash {
                return Some(probe);
            }
            probe = ((probe as u64 + stride) & mask) as usize;
            if probe == loop_index {
                // Should be unreachable: the table is never full past threshold.
                return None;
            }
        }
    }

    /// Grows the hash table (`resizeCache`) when still below nominal capacity.
    fn resize_table(&mut self) {
        let target = self.lg_nominal_longs + 1;
        let delta_lg = (DEFAULT_LG_RESIZE_FACTOR as u32)
            .min(target - self.lg_arr_longs)
            .max(1);
        self.lg_arr_longs += delta_lg;
        let mut new_table = vec![0u64; 1usize << self.lg_arr_longs];
        let count = Self::rebuild_into(&self.table, &mut new_table, self.lg_arr_longs, self.theta);
        self.current_count = count;
        self.table = new_table;
        self.hash_table_threshold =
            set_hash_table_threshold(self.lg_nominal_longs, self.lg_arr_longs);
    }

    /// Lowers `theta` to the (nominal+1)-th smallest retained hash and rebuilds the table to keep
    /// only hashes strictly below the new theta (`quickSelectAndRebuild`).
    fn quick_select_and_rebuild(&mut self) {
        let nominal = 1usize << self.lg_nominal_longs;
        let new_theta = select_excluding_zeros(&self.table, nominal);
        self.theta = new_theta;
        let mut new_table = vec![0u64; 1usize << self.lg_arr_longs];
        let count = Self::rebuild_into(&self.table, &mut new_table, self.lg_arr_longs, self.theta);
        self.current_count = count;
        self.table = new_table;
    }

    /// Re-inserts all non-zero hashes strictly below `theta` from `source` into `dest`.
    fn rebuild_into(source: &[u64], dest: &mut [u64], lg_arr_longs: u32, theta: i64) -> usize {
        let mut count = 0;
        for &hash in source {
            if hash != 0 && (hash as i64) < theta {
                Self::search_or_insert_into(dest, lg_arr_longs, hash);
                count += 1;
            }
        }
        count
    }

    /// The current theta as a `long`.
    pub fn theta_long(&self) -> i64 {
        self.theta
    }

    /// The number of retained hashes.
    pub fn retained_entries(&self) -> usize {
        self.current_count
    }

    /// Whether the sketch is empty (no value was ever offered).
    pub fn is_empty(&self) -> bool {
        self.is_empty
    }

    /// The estimated number of distinct values. Mirrors Java `Sketch.getEstimate`:
    /// `retained / (theta / MAX_THETA)` in exact mode collapses to the retained count.
    pub fn estimate(&self) -> f64 {
        estimate(self.theta, self.current_count, self.is_empty)
    }

    /// The ascending-sorted retained hashes (the compact-ordered set).
    fn sorted_hashes(&self) -> Vec<u64> {
        let mut hashes: Vec<u64> = self.table.iter().copied().filter(|&h| h != 0).collect();
        hashes.sort_unstable();
        hashes
    }

    /// Serializes to the compact (immutable, ordered) byte form — the Iceberg theta blob payload.
    ///
    /// Chooses the preamble shape by mode:
    /// - empty → 8-byte single-preamble, EMPTY flag, no seed hash, no hashes;
    /// - exactly one retained hash with theta == MAX → 16-byte SINGLEITEM form;
    /// - otherwise → 2-long preamble when theta == MAX, 3-long preamble when theta < MAX, followed
    ///   by the ascending hashes.
    ///
    /// # Errors
    /// Returns [`SketchError::ZeroSeedHash`] only if the seed hashes to zero (9001 never does).
    pub fn serialize_compact(&self) -> SketchResult<Vec<u8>> {
        let hashes = self.sorted_hashes();
        serialize_compact_from_parts(self.is_empty, self.theta, &hashes, self.seed)
    }

    /// Parses a compact theta sketch and verifies its seed hash against this sketch's seed,
    /// returning a [`CompactThetaSketch`] view (retained hashes + theta + estimate).
    ///
    /// See [`CompactThetaSketch::deserialize`] — provided here so callers with a configured seed
    /// can validate seed-hash compatibility in one call.
    pub fn deserialize_compact(&self, bytes: &[u8]) -> SketchResult<CompactThetaSketch> {
        CompactThetaSketch::deserialize_with_seed(bytes, self.seed)
    }
}

impl Default for ThetaSketch {
    fn default() -> Self {
        Self::new()
    }
}

/// A read-only view of a deserialized compact theta sketch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactThetaSketch {
    /// The retained hashes in the order they appeared on the wire (ascending for an ordered blob,
    /// which is what every current engine writes; an unordered blob's hashes are kept verbatim).
    /// Empty for an empty sketch.
    pub hashes: Vec<u64>,
    /// The theta value as a `long`.
    pub theta: i64,
    /// Whether the sketch is empty.
    pub is_empty: bool,
    /// Whether the sketch was serialized in ordered form (the `ORDERED` flag). DataSketches can emit
    /// unordered compact sketches (`compact(false, ...)`); the reader accepts them verbatim, matching
    /// Java's heapify, which records the flag but does not re-validate hash ordering.
    pub is_ordered: bool,
}

impl CompactThetaSketch {
    /// The number of retained hashes.
    pub fn retained_entries(&self) -> usize {
        self.hashes.len()
    }

    /// The estimated number of distinct values.
    pub fn estimate(&self) -> f64 {
        estimate(self.theta, self.hashes.len(), self.is_empty)
    }

    /// Deserializes a compact theta sketch WITHOUT checking the seed hash.
    ///
    /// # Errors
    /// Returns a [`SketchError`] on a malformed preamble, unsupported version/family, or truncated
    /// payload. Never panics on hostile input.
    pub fn deserialize(bytes: &[u8]) -> SketchResult<Self> {
        Self::parse(bytes, None)
    }

    /// Deserializes a compact theta sketch and verifies its seed hash equals
    /// `compute_seed_hash(seed)`.
    ///
    /// # Errors
    /// As [`Self::deserialize`], plus [`SketchError::SeedHashMismatch`] if the blob's stored seed
    /// hash differs from the expected one (the blob was written with a different seed).
    pub fn deserialize_with_seed(bytes: &[u8], seed: u64) -> SketchResult<Self> {
        Self::parse(bytes, Some(seed))
    }

    /// Core parser. Validates the preamble at every step before reading the body.
    ///
    /// SERIAL-VERSION POSTURE (Y1-reviewer, bytecode-derived): DataSketches' `CompactSketch.heapify`
    /// ACCEPTS serial-versions 1, 2, and 3 (it routes v1/v2 through
    /// `ForwardCompatibility.heapify{1,2}to3`). This reader is deliberately **v3-only**: every Iceberg
    /// `apache-datasketches-theta-v1` blob written by a current engine is v3 (v1/v2 are pre-0.10
    /// DataSketches layouts, ~2015, never emitted by any Iceberg engine). A v1/v2 blob is therefore
    /// unreachable in practice; rejecting it LOUDLY (rather than wrongly decoding a layout we lack)
    /// is the safe posture. If a v1/v2 reader is ever needed, port `ForwardCompatibility` here.
    fn parse(bytes: &[u8], seed: Option<u64>) -> SketchResult<Self> {
        // Need at least the 1-long preamble.
        if bytes.len() < 8 {
            return Err(SketchError::TruncatedInput {
                expected: 8,
                actual: bytes.len(),
            });
        }
        let pre_longs = bytes[0] & 0x3f;
        let serial_version = bytes[1];
        let family = bytes[2];
        let flags = bytes[5];

        if serial_version != SERIAL_VERSION_3 {
            return Err(SketchError::UnsupportedSerialVersion {
                found: serial_version,
            });
        }
        if family != FAMILY_COMPACT {
            return Err(SketchError::UnexpectedFamily { found: family });
        }
        if !matches!(pre_longs, 1..=3) {
            return Err(SketchError::InvalidPreambleLongs { found: pre_longs });
        }
        if flags & FLAG_COMPACT == 0 {
            return Err(SketchError::NotCompact);
        }

        let is_empty = flags & FLAG_EMPTY != 0;
        // Single-item detection mirrors Java `CompactOperations.memoryToCompact` /
        // `SingleItemSketch.otherCheckForSingleItem`: either the explicit SINGLEITEM bit is set, OR
        // it is a legacy v3 compact 1-long preamble whose low 5 flag bits are exactly 26
        // (READ_ONLY|COMPACT|ORDERED, no EMPTY, no SINGLEITEM). serial_version (==3) and family
        // (==COMPACT) were already validated above.
        let is_legacy_single = pre_longs == 1 && (flags & 0x1f) == FLAG_LEGACY_SINGLE_LOW5;
        let is_single = flags & FLAG_SINGLEITEM != 0 || is_legacy_single;
        let is_ordered = flags & FLAG_ORDERED != 0;

        // Empty sketch: 1-long preamble, no seed hash stored, no body.
        if is_empty {
            return Ok(CompactThetaSketch {
                hashes: Vec::new(),
                theta: MAX_THETA,
                is_empty: true,
                is_ordered: true,
            });
        }

        // Verify the stored seed hash (bytes 6-7) when a seed is provided. Not present on empty.
        if let Some(seed) = seed {
            let expected = compute_seed_hash(seed)?;
            let found = u16::from_le_bytes([bytes[6], bytes[7]]);
            if found != expected {
                return Err(SketchError::SeedHashMismatch { expected, found });
            }
        }

        // Single-item: 1-long preamble + one 8-byte hash, theta == MAX.
        if is_single {
            let required = 16;
            if bytes.len() < required {
                return Err(SketchError::TruncatedInput {
                    expected: required,
                    actual: bytes.len(),
                });
            }
            let hash = u64::from_le_bytes(bytes[8..16].try_into().expect("8 bytes"));
            return Ok(CompactThetaSketch {
                hashes: vec![hash],
                theta: MAX_THETA,
                is_empty: false,
                is_ordered: true,
            });
        }

        // Multi-entry: retained count at 8-11, theta at 16-23 when pre_longs == 3 (else MAX).
        let required_preamble = (pre_longs as usize) * 8;
        if bytes.len() < required_preamble {
            return Err(SketchError::TruncatedInput {
                expected: required_preamble,
                actual: bytes.len(),
            });
        }
        // Java `memoryToCompact`: curCount is read from bytes 8-11 ONLY for pre_longs > 1; a 1-long
        // preamble that is neither empty nor single carries zero retained entries (and no count
        // field — reading bytes 8-11 here would over-read the 8-byte buffer and panic).
        let retained = if pre_longs > 1 {
            u32::from_le_bytes(bytes[8..12].try_into().expect("4 bytes")) as usize
        } else {
            0
        };
        let theta = if pre_longs == 3 {
            i64::from_le_bytes(bytes[16..24].try_into().expect("8 bytes"))
        } else {
            MAX_THETA
        };

        let body_offset = required_preamble;
        let required_total = body_offset + retained * 8;
        if bytes.len() < required_total {
            return Err(SketchError::TruncatedInput {
                expected: required_total,
                actual: bytes.len(),
            });
        }

        let mut hashes = Vec::with_capacity(retained);
        for index in 0..retained {
            let start = body_offset + index * 8;
            let hash = u64::from_le_bytes(bytes[start..start + 8].try_into().expect("8 bytes"));
            hashes.push(hash);
        }

        Ok(CompactThetaSketch {
            hashes,
            theta,
            is_empty: false,
            is_ordered,
        })
    }
}

/// Serializes the compact byte form from explicit parts. Shared by [`ThetaSketch`], the Alpha update
/// form ([`crate::alpha::AlphaSketch`]), and tests — the single serialization path (no per-family fork).
///
/// `hashes` must be ascending-sorted. `is_empty` true forces the empty form regardless of hashes.
pub(crate) fn serialize_compact_from_parts(
    is_empty: bool,
    theta: i64,
    hashes: &[u64],
    seed: u64,
) -> SketchResult<Vec<u8>> {
    // Empty form: 8 bytes, preLongs=1, EMPTY|COMPACT|ORDERED|READ_ONLY, no seed hash.
    if is_empty || hashes.is_empty() {
        let mut buffer = vec![0u8; 8];
        buffer[0] = 1; // preLongs
        buffer[1] = SERIAL_VERSION_3;
        buffer[2] = FAMILY_COMPACT;
        // bytes 3,4 = 0 (lgNomLongs/lgArrLongs)
        buffer[5] = FLAG_EMPTY | FLAG_COMPACT | FLAG_ORDERED | FLAG_READ_ONLY;
        // bytes 6,7 = 0 (no seed hash on empty)
        return Ok(buffer);
    }

    let seed_hash = compute_seed_hash(seed)?;

    // Single-item form: preLongs=1, SINGLEITEM|COMPACT|ORDERED|READ_ONLY, one hash, theta == MAX.
    if hashes.len() == 1 && theta == MAX_THETA {
        let mut buffer = vec![0u8; 16];
        buffer[0] = 1;
        buffer[1] = SERIAL_VERSION_3;
        buffer[2] = FAMILY_COMPACT;
        buffer[5] = FLAG_SINGLEITEM | FLAG_COMPACT | FLAG_ORDERED | FLAG_READ_ONLY;
        buffer[6..8].copy_from_slice(&seed_hash.to_le_bytes());
        buffer[8..16].copy_from_slice(&hashes[0].to_le_bytes());
        return Ok(buffer);
    }

    // Multi-entry: preLongs=2 (theta == MAX) or 3 (theta < MAX).
    let estimation_mode = theta < MAX_THETA;
    let pre_longs: u8 = if estimation_mode { 3 } else { 2 };
    let preamble_bytes = (pre_longs as usize) * 8;
    let mut buffer = vec![0u8; preamble_bytes + hashes.len() * 8];
    buffer[0] = pre_longs;
    buffer[1] = SERIAL_VERSION_3;
    buffer[2] = FAMILY_COMPACT;
    buffer[5] = FLAG_COMPACT | FLAG_ORDERED | FLAG_READ_ONLY;
    buffer[6..8].copy_from_slice(&seed_hash.to_le_bytes());
    buffer[8..12].copy_from_slice(&(hashes.len() as u32).to_le_bytes());
    buffer[12..16].copy_from_slice(&DEFAULT_P.to_le_bytes());
    if estimation_mode {
        buffer[16..24].copy_from_slice(&theta.to_le_bytes());
    }
    for (index, &hash) in hashes.iter().enumerate() {
        let start = preamble_bytes + index * 8;
        buffer[start..start + 8].copy_from_slice(&hash.to_le_bytes());
    }
    Ok(buffer)
}

/// The DataSketches estimate, byte-for-byte as Java `Sketch.estimate(long thetaLong, int curCount)`:
/// `curCount * (2^63_as_f64 / thetaLong)`.
///
/// Java's static helper is `((double) curCount) * (9.223372036854776E18 / (double) thetaLong)` with
/// NO special case for empty or exact mode — those fall out of the same expression (an empty sketch
/// has `curCount == 0`, and `theta == MAX_THETA` makes the ratio `2^63 / (2^63 - 1)`, which still
/// collapses to the exact count after the multiply). The constant `2^63` equals `MAX_THETA as f64`
/// to the bit (both round to `0x43e0000000000000`). We reproduce the exact order of operations —
/// `count * (MAX / theta)`, NOT `count / (theta / MAX)` — because the two differ by up to one ULP
/// (e.g. the lgK=4 1000-value fixture: Java `829.7403132548839`, the algebraically-equal but
/// reordered form `829.740313254884`). The estimate is part of the cross-engine NDV contract, so it
/// must match Java bit-exactly, not just within a tolerance.
///
/// A corrupt `theta == 0` yields `+inf` here — exactly as Java's helper does (division by `0.0`).
///
/// Shared by [`ThetaSketch`], [`CompactThetaSketch`], and the Alpha update form's COMPACT estimate
/// ([`crate::alpha::AlphaSketch::compact`]). This is the family-COMPACT estimator Java's
/// `CompactSketch.getEstimate` uses — it is NOT the Alpha update sketch's sampling-mode estimator
/// (see [`crate::alpha::AlphaSketch::estimate`]).
pub(crate) fn estimate(theta: i64, retained: usize, _is_empty: bool) -> f64 {
    let max_theta_as_f64 = MAX_THETA as f64;
    (retained as f64) * (max_theta_as_f64 / (theta as f64))
}

/// `Util.startingSubMultiple(lgTarget, lgRF, minLg)`. Shared by both update families.
pub(crate) fn starting_sub_multiple(lg_target: u32, lg_resize_factor: u32, min_lg: u32) -> u32 {
    if lg_target <= min_lg {
        min_lg
    } else if lg_resize_factor == 0 {
        lg_target
    } else {
        (lg_target - min_lg) % lg_resize_factor + min_lg
    }
}

/// `setHashTableThreshold`: `floor(fraction * 2^lgArr)`. Identical for the QuickSelect and Alpha
/// update families (both call the same `setHashTableThreshold(lgNom, lgArr)`).
pub(crate) fn set_hash_table_threshold(lg_nominal_longs: u32, lg_arr_longs: u32) -> usize {
    let fraction = if lg_arr_longs <= lg_nominal_longs {
        0.5
    } else {
        0.9375
    };
    (fraction * f64::from(1u32 << lg_arr_longs)).floor() as usize
}

/// `HashOperations.getStride`: an odd double-hashing stride `2 * ((hash >>> lgArr) & 127) + 1`.
/// Shared by both update families' open-addressing probes.
pub(crate) fn get_stride(hash: u64, lg_arr_longs: u32) -> u64 {
    2 * ((hash >> lg_arr_longs) & 127) + 1
}

/// `HashOperations.hashSearchOrInsert(table, lgArr, hash)`: open-addressing search-or-insert into a
/// clean table. Returns `Some(index)` on a found duplicate (no insert), `None` after inserting.
///
/// Shared by [`ThetaSketch`] and [`crate::alpha::AlphaSketch`]'s clean path — both use the same
/// `HashOperations` primitive. The probe stride is [`get_stride`].
pub(crate) fn hash_search_or_insert(
    table: &mut [u64],
    lg_arr_longs: u32,
    hash: u64,
) -> Option<usize> {
    let mask = (1u64 << lg_arr_longs) - 1;
    let stride = get_stride(hash, lg_arr_longs);
    let mut probe = (hash & mask) as usize;
    let loop_index = probe;
    loop {
        let value = table[probe];
        if value == 0 {
            table[probe] = hash;
            return None;
        }
        if value == hash {
            return Some(probe);
        }
        probe = ((probe as u64 + stride) & mask) as usize;
        if probe == loop_index {
            // Unreachable past threshold: the table is never full when this is called.
            return None;
        }
    }
}

/// `HashOperations.hashArrayInsert(srcArr, destArr, lgArr, theta)`: re-insert every non-zero source
/// entry strictly below `theta` into `dest`, returning the inserted count. The rebuild/resize
/// primitive shared by both update families.
pub(crate) fn hash_array_insert(
    source: &[u64],
    dest: &mut [u64],
    lg_arr_longs: u32,
    theta: i64,
) -> usize {
    let mut count = 0;
    for &hash in source {
        if hash != 0 && (hash as i64) < theta {
            hash_search_or_insert(dest, lg_arr_longs, hash);
            count += 1;
        }
    }
    count
}

/// `QuickSelect.selectExcludingZeros(cache, curCount, nominal+1)`: returns the (nominal+1)-th
/// smallest non-zero hash, i.e. the element at 0-based index `nominal` of the sorted non-zero set.
///
/// We sort the non-zero entries (the retained set is small — bounded by `2^(lgK+1)`), which is
/// simpler and equivalent in result to Java's in-place quickselect for our purposes.
fn select_excluding_zeros(table: &[u64], nominal: usize) -> i64 {
    let mut non_zero: Vec<u64> = table.iter().copied().filter(|&h| h != 0).collect();
    non_zero.sort_unstable();
    // The (nominal+1)-th smallest is index `nominal` (0-based). curCount > threshold guarantees
    // enough entries are present.
    non_zero[nominal] as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fixture provenance: every `*_HEX` below was emitted by dev/theta fixture `Gen.java` /
    /// `Gen2.java` run against `datasketches-java-3.3.0` (~/.m2) with seed 9001. See
    /// `crates/sketches/testdata/README.md`.
    fn hex_to_bytes(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("valid hex"))
            .collect()
    }

    // ---- Java-generated fixtures (exact bytes) ----
    const EMPTY_HEX: &str = "01030300001e0000";
    const SINGLE_HEX: &str = "01030300003acc9315f97dcbbd86a105";
    const EXACT10_HEX: &str = "02030300001acc930a0000000000803f15f97dcbbd86a10540de2ee1c9db3d08698bb991b8685708fe162113fb98bc10bd3273724691cc14c397fc1281709d1ee56b61eec88044201ad1300b998c2f22ba40b3c1da06695de0f48bea9983c37c";
    const EST1000_LGK4_HEX: &str = "03030300001acc93180000000000803f393eeb1613ceb303d589edf2ba742a00deac6be1e1d96500fa74b34c16b16e00c7c48185137f7300ff3edb4931fa96005167013eefe2dd004bbf7eee7e40f500e4c758129ac40601af5f15a649924e010a22dbb71c875f0188655737d10a74017b7fec66ed787f01fb38798913248f01b9bf9feafa9eaa01783d46e37d7cd301efafaecf5bddf40194ded6c8a0752a02d1eeb5b119b493026df3fb88558d96027347a26e394d9902ae9ad119a4702703a5907be534f96d03b833b51ac36e72030f5d1190dc88b103";

    // ---- Y1-reviewer estimation-breadth fixture (Java-generated, datasketches-java-3.3.0) ----
    // lgK=8 (nominal 256), 5000 distinct longs `1_000_000 + i*7` (a seed-independent value set that
    // forces deep resize + repeated quickselect). Byte-exact: preLongs=3, retained=400, theta<MAX.
    // Java: retained=400, theta=722749449306535422, getEstimate()=5104.602733743722.
    const EST5000_LGK8_HEX: &str = "03030300001acc93900100000000803ffe75867bcbb8070a2b42893aa02d030069b5cb1306df2a00499ecc10ee492f00a8848975f5c12f00721265fc2b8f3700e8641c8093ce4400a2eee66320af4a000e25ff75fde956002cc1ada99e846700d0133ddcd36e6800ccadfbbe81bb6b00a981813ba6066d0010d17b44f1127700ce2b984ae03f7800c29ff2e3266e7a00d200bd89cd467d00e799af0ba2c982009c11b6d852fc8200a4eb636e44d78400d00faa1f6fd6880014d40e0826d78d00b7f89b8f7f088e00da6c7f75d8378e00f3aec03a993a9500f61171636cc297008e8cfd35f1439e0030b9edb9932d9f00644ada3db468a500226d545f6672a500d33573531c8abc00355d249badb2c00092adab70199bc3003396f5002a24c400fbe7a70d8f5bc400371841d45849cb005144b061455fcb00a815d2aaf7a2d600ebb2e42e6d37dc007d938e389598dd0021ad54628da3e300ddda246b8e01eb009ce96f7c18beed00dbd3149d00e0f10065396e4f7f54f400a90fdf11a255f4006b95ce6ce6a0f800e6bbd6421c32f90086287b3c38b7ff00b67c463d532c0b01a40a6958b9a60e0168f58c4c6c7e15016db934ac15b31501f9b2f0632a741c0132b58353cea0260144522b18d0d92a01fdb699c266fd3801d2d24cffb5fa3c0133ef7ff73f423f01e2202a478afc44015b4f871efd236201be77257713206701f873cbcbc6b06a01e7caba8d021f6d0144be9852ed316d0150eafaa5dc827501f38d3c56893183010c52d8e0bcc28d014619f8fa97a38f010e0033e9039291014dd8ca3bcc12a4019e4f69f60a05a6010dd02f70dc71b001be17bc9b6bfcb1011391b88ac4abb501b9c34cc01c6cb60107a38b125470c8011cbd27f471a1c801d9b150bd87e0c901ebbd9604b936d80162a79aba6556f10167b19d7a1656f301ff61ae603b04f501ac58b7a02a96fe01b23fa0b089a9ff015fc0824f3efa01020cea349e3aac0e029e65b600cd6d17022ec586c92e56250217c190f304243802cfe9d57dcb5c3b02de8d965567703f02b629b2809e264a02b70df62ca4e24b0236b0c4b5ffff620278b5193b55988102671fa94a76928202e9db0e9bc1b38e021d766b665be19c02943d4aaa0d749f02d280af09f02fac02d52c650d75d5bc0237f0f1e18a7cd002f8c19bb8fad2d002ccdd8e745c24da025e3f34db76dade0284776d3ba40ee2023d91a6096430eb029ab1cfa76555ed0287506fa36c180d03fd97d7be73470e034fec5435924d0e03bc99d4630303170335a4624d0a9018039b68c4d68f3e1c0323c840ca938f23031cfeab5643ec30032bcfeef33b403303a84d9e8cc5d4330373a423d6a5583403d43aa4ef06f2360305778d0fbc0b3c03f0991a1fe6b83f0335a823ad199f4003ee6fd6267146530356a6a786cd087a030e5cbaabb1aa7c03f4246709a9608303c9c80dbd413f85032453c8761ff09103b97ded8ae0579403ef0a939fadce9d038e7b61848acda003ab1abb47f9b2a3037b59f863fc5fa503e6c2a2ea21ecaf035186d94d593ab20328683017e50fb403a4b474c2d863ba034a7fae1760a0ba030e240e4273edc003aad295c12f74cc03bb1521b559cace039c63b21d0adacf03545c6e486decde03a6a68ffbaf03e2032f8d783598c6e50327dad619ab7ae9037f643a6fedaeec03e84bbd508f6df20302539c7763abf303480d9ed1f229f5034e3a093030e6f5032567e5afe503f603c9e71530f5440004fac52f3161e104041a340aafc2e906041fe3b9509c121304a73f9b477aa313040d3bf083e28d15044a009a2ff17d2704f7f322fd70912c042f7bb1d6309c3704f7f9785ddbf6670453ade0f15c097304770bbfb41bd97504e4e0d66e57a67704a6334d321f6c7b04c33b4851f21d7c04abcab43790f87d04c4a90e3d8b5f8704bd221660b081970452175ebfb39ea3048af489e2734bac048501546cb572b204187e89e7821dba04270a444fc835bf04b5d6b57c33abbf046fa057456d2ac3043fcc1f6e7849c9041b32df37c6fecd0436444769106cd604c0c5b55b9d90dd04fc9737c64a62ea04f449d2bd6d7dea04fdeb04630c28f90405346a7754e20405dc39cf3548180505a0437aea288c06053291a93323f611052db60edea8d814053526f239e8361705dd840926da7a1c056f9ebad2d0fb2a05939566229b902d0555c280a833a72e0548e7bb7a35773f05cb61bf9b8d814b0541625527a7444f05299bf7cddb5b5f05827af505a9af680514b1b43c77966d05428d075b93e871052efd155aad7a7205c522e5c059657b05217c07ac47767e0539a186131add7f05ad24058625898d057ae553263fc092055068ea701671a805c94bbd4b020cac059fa95e21b753ba05f018e8effba8c7059396c9343bf2d50549755a609260d80507c7199cbed7d90569a11268248cdd0528c3de953c0ee305dce72e7efacfe6053f42e6df72d9e905e4c2aaf4aeeaec05dad2360da3f3f505195fb408df920306d052560a39e70306fed2317e5ace0c067a53c0aed2e810066746f7ee0cb81106ac5d260a405112069b9f8b9047381606f2c40cc510821a0634a72896115c1e063785a5f752b52206dc445f3ab32d3006f57b41a800b0320628ce7f3ea6e53a065c10df5848f93c065c7e67b94a503d062de2ad104dbb40069c60f5c6fffc5706516900634f25600692d1cfa52d366a06f212e534735f6e063e975598e43d7406f6aface546218506aefba0d80f2888060aee7dab16bb8e06b8571999608d9006ac2a968c6e749106ed25d95ad03f97066f1fe671c6ef9706bea8ed0d5a1a9b0603e9f9cf9d1a9e06b9239f25f973a206087692570774ad06e0a1df68e042bc0696539b4f308dbe06d3077b211603c006da242466f32ec006aa2117b7239fc006d13844536b98c606a80727d6d702d00633676dd8cccbe0061ff3cbd1fc4fe106d71e38821461e40671e6d60c5cb3e906838c85840839ec06ddd2b09bec28ed06d59f4afd0323f10683682ce3ea4df306a5a66e3012e7fe061f7c63a754420107158e4ba27b75050798af9d6494620a07e28d19cdb9df0b07d9cd9a3f3fc80d07673a5ae8bbb10e07dd39de298d9f1c071aa8987b29541d076e9a763fe00b2007f53715753ba523079a31d267fa282907b04fd622bf09360715d51112998c4707ca5a8f255b946907f7e404787e127007fd6e4e37d8a97707e62014d3c01e87079d80785f43888a07f83c4975eb4c9007f9680909ed059c07faf84033515aa8079b078abfbbaead0752671b1344a8ae0792426018b0efba07f49b555addc8c007feda125c4f5fc10773c4721c8a7cc7070e2ffc649fe3ce07860af73104d9d8078ea1bf2e4351de078d27f077691ce7074a4041b9deffe807fbba629da8d7f3072692b41f2df1fd077c041862cb2000087a4921eb923d0108db41c9964b5c020819c133e3fc881e086f49aa9eaee32708d66b1fafbea828088541b4cef2273c086e5bc5f493864008681c65c0e76a4408387b479afb7348086f39f226fed44f08246e224956ca52086f5260b7ad3f55087d9a4d262eb357089877fddfa5016408dd71f6900aa66608c2ece7e109a368080fe89c9906f768087f678648a3cd6e084edc9f3a4f1e6f08a66122787d816f082b4f626e7f886f083de01d403cb56f08ed1560d4b32271081b68e988e6967608b9b20343c32378087208b73dc4117e08aec831df8e658b08b4606a34b68e8b08bfa85ef3f65b9008616a6fa91f1396081633ae6b0908a308ab88e932cb0eb00893b066e76dfdb0084f39c36ade81b608c6cedb51d905b90869f681fe3682c5081910fcbfd6bfc608de7be655898bc90875016cd6ae86cb0872be1a39a763db0899da8957969fdf0882dc9d3a8785e1080891aca8fa24e208b3b7dc18447ae508c7118d222d52e708acdba64dfaf0e70820ba7f55a226e80898c31e5ffc56ea08daf5100aef3ef008be7fedb007adf3082176fa525ddff9088a8735902416120940ed2dc99c5e140933add9f39a101809e389a3ccb1452309fe3c0d5b1a4928091ba2bd87d35928094c8c7da3668e2909b7f62ec55b932c09c1fe9efc38144009d3188942bcc442096cdc8695eaba4709ce7c4cd127b55b09b8feb910ac435e09d92eb7d2399f6309eeb189552f5e64090387ffb1c2286b09f541bb7a7c546d093f9c7f768d6e740912e43912704a9409562a5ea511c7a20968f452a195c0a3091138f567de01aa095c7754c46a17b3095e27758d0b82b809dd174e22eff5b9094de7d2de7694c3097800fbc0a5dec309bbddcb83113fc80914b249c65b9bc9094877fc4857c2cc0936d6466f66a0d009de04e3ddf588d709f4a603ca2f1ad90922105c16a96adf09f8c5e34ec14ee009df676cff6d53e309790201f95b2be80954642fbb5753eb091358ae6b92a7ee09b6873f5251d7f009fd3f94da7b2cf109429d5f7b8a95f4097e52c156ede0f40916c5bd372faefd09c2c970eb9813ff09";

    #[test]
    fn test_empty_serialization_matches_java_bytes() {
        // Risk: an empty NDV blob other engines can't parse. Java empty == 8 bytes, EMPTY flag.
        let sketch = ThetaSketch::new();
        assert!(sketch.is_empty());
        let bytes = sketch.serialize_compact().unwrap();
        assert_eq!(
            bytes,
            hex_to_bytes(EMPTY_HEX),
            "empty bytes diverge from Java"
        );
        assert_eq!(bytes.len(), 8);
    }

    #[test]
    fn test_single_item_serialization_matches_java_bytes() {
        // Risk: the SINGLEITEM special preamble (flag 0x20, no count/theta) must match exactly.
        let mut sketch = ThetaSketch::new();
        sketch.update_u64(1);
        assert_eq!(sketch.retained_entries(), 1);
        let bytes = sketch.serialize_compact().unwrap();
        assert_eq!(
            bytes,
            hex_to_bytes(SINGLE_HEX),
            "single-item bytes diverge from Java"
        );
        assert_eq!(bytes.len(), 16);
    }

    #[test]
    fn test_exact_mode_serialization_matches_java_bytes() {
        // Risk: exact mode (preLongs=2, theta NOT stored, retained count + P float) byte layout.
        let mut sketch = ThetaSketch::new();
        for value in 0..10u64 {
            sketch.update_u64(value);
        }
        assert_eq!(sketch.retained_entries(), 10);
        assert_eq!(sketch.theta_long(), MAX_THETA);
        let bytes = sketch.serialize_compact().unwrap();
        assert_eq!(
            bytes,
            hex_to_bytes(EXACT10_HEX),
            "exact-mode bytes diverge from Java"
        );
        assert_eq!(bytes.len(), 96);
    }

    #[test]
    fn test_estimation_mode_serialization_matches_java_bytes() {
        // Risk: estimation mode (preLongs=3, theta stored at b16, quickselect-bounded set) is the
        // hardest path — the retained set AND theta must reproduce Java's quickselect to the bit.
        let mut sketch = ThetaSketch::with_lg_nominal_longs(4, DEFAULT_UPDATE_SEED);
        for value in 0..1000u64 {
            sketch.update_u64(value);
        }
        assert_eq!(sketch.retained_entries(), 24);
        assert_eq!(sketch.theta_long(), 266783384329207353);
        let bytes = sketch.serialize_compact().unwrap();
        assert_eq!(
            bytes,
            hex_to_bytes(EST1000_LGK4_HEX),
            "estimation-mode bytes diverge from Java"
        );
        assert_eq!(bytes.len(), 216);
    }

    #[test]
    fn test_round_trip_build_serialize_deserialize_estimate() {
        // Risk: a serialize→deserialize cycle that loses hashes/theta corrupts the NDV.
        let mut sketch = ThetaSketch::new();
        for value in 0..50u64 {
            sketch.update_u64(value);
        }
        let bytes = sketch.serialize_compact().unwrap();
        let parsed =
            CompactThetaSketch::deserialize_with_seed(&bytes, DEFAULT_UPDATE_SEED).unwrap();
        assert_eq!(parsed.retained_entries(), sketch.retained_entries());
        assert_eq!(parsed.theta, sketch.theta_long());
        assert_eq!(parsed.estimate(), sketch.estimate());
        assert_eq!(parsed.estimate(), 50.0);
    }

    #[test]
    fn test_deserialize_java_empty_single_exact_estimation() {
        // Risk: we must READ Java-written blobs of every mode, not just write our own.
        let empty = CompactThetaSketch::deserialize(&hex_to_bytes(EMPTY_HEX)).unwrap();
        assert!(empty.is_empty);
        assert_eq!(empty.retained_entries(), 0);
        assert_eq!(empty.estimate(), 0.0);

        let single = CompactThetaSketch::deserialize_with_seed(
            &hex_to_bytes(SINGLE_HEX),
            DEFAULT_UPDATE_SEED,
        )
        .unwrap();
        assert_eq!(single.retained_entries(), 1);
        assert_eq!(single.estimate(), 1.0);

        let exact = CompactThetaSketch::deserialize_with_seed(
            &hex_to_bytes(EXACT10_HEX),
            DEFAULT_UPDATE_SEED,
        )
        .unwrap();
        assert_eq!(exact.retained_entries(), 10);
        assert_eq!(exact.theta, MAX_THETA);
        assert_eq!(exact.estimate(), 10.0);

        let est = CompactThetaSketch::deserialize_with_seed(
            &hex_to_bytes(EST1000_LGK4_HEX),
            DEFAULT_UPDATE_SEED,
        )
        .unwrap();
        assert_eq!(est.retained_entries(), 24);
        assert_eq!(est.theta, 266783384329207353);
        // Java getEstimate() for this sketch == 829.7403132548839.
        assert!((est.estimate() - 829.7403132548839).abs() < 1e-6);
    }

    #[test]
    fn test_estimation_mode_estimate_within_theoretical_bounds() {
        // Risk: a wrong estimator math (theta fraction) gives a wildly off NDV. For 1000 distinct
        // inputs, the estimate must be within the 3-sigma error bound for lgK=4.
        let mut sketch = ThetaSketch::with_lg_nominal_longs(4, DEFAULT_UPDATE_SEED);
        for value in 0..1000u64 {
            sketch.update_u64(value);
        }
        let estimate = sketch.estimate();
        // Relative standard error for theta with k=16 is ~1/sqrt(16) = 0.25; allow 3 sigma.
        let lower = 1000.0 * (1.0 - 3.0 * 0.25);
        let upper = 1000.0 * (1.0 + 3.0 * 0.25);
        assert!(
            estimate >= lower && estimate <= upper,
            "estimate {estimate} outside [{lower}, {upper}]"
        );
    }

    #[test]
    fn test_exact_mode_estimate_is_exact_count() {
        // Risk: exact mode (theta == MAX) must return the exact distinct count, not an estimate.
        let mut sketch = ThetaSketch::new();
        for value in 0..100u64 {
            sketch.update_u64(value);
        }
        assert_eq!(sketch.theta_long(), MAX_THETA);
        assert_eq!(sketch.estimate(), 100.0);
    }

    #[test]
    fn test_duplicates_do_not_increase_count() {
        // Risk: NDV must count DISTINCT values; a duplicate must not bump the retained set.
        let mut sketch = ThetaSketch::new();
        for _ in 0..100 {
            sketch.update_u64(42);
        }
        assert_eq!(sketch.retained_entries(), 1);
        assert_eq!(sketch.estimate(), 1.0);
    }

    #[test]
    fn test_update_bytes_and_update_u64_le_agree() {
        // Risk: the two update entry points must hash identically for the same logical value.
        let mut by_long = ThetaSketch::new();
        by_long.update_u64(7);
        let mut by_bytes = ThetaSketch::new();
        by_bytes.update_bytes(&7u64.to_le_bytes());
        assert_eq!(by_long.sorted_hashes(), by_bytes.sorted_hashes());
    }

    #[test]
    fn test_empty_bytes_update_is_ignored() {
        // Risk: an empty string value must not panic and must not retain anything.
        let mut sketch = ThetaSketch::new();
        sketch.update_bytes(&[]);
        assert!(sketch.is_empty());
        assert_eq!(sketch.retained_entries(), 0);
    }

    // ---- malformed-input rejection (loud errors, no panics) ----

    #[test]
    fn test_truncated_preamble_rejected() {
        // Risk: a 3-byte buffer must error, never index-panic.
        let result = CompactThetaSketch::deserialize(&[1, 3, 3]);
        assert!(matches!(result, Err(SketchError::TruncatedInput { .. })));
    }

    #[test]
    fn test_bad_serial_version_rejected() {
        let mut bytes = hex_to_bytes(SINGLE_HEX);
        bytes[1] = 99; // serial version
        assert!(matches!(
            CompactThetaSketch::deserialize(&bytes),
            Err(SketchError::UnsupportedSerialVersion { found: 99 })
        ));
    }

    #[test]
    fn test_bad_family_rejected() {
        let mut bytes = hex_to_bytes(SINGLE_HEX);
        bytes[2] = 1; // family ALPHA, not COMPACT
        assert!(matches!(
            CompactThetaSketch::deserialize(&bytes),
            Err(SketchError::UnexpectedFamily { found: 1 })
        ));
    }

    #[test]
    fn test_invalid_preamble_longs_rejected() {
        let mut bytes = hex_to_bytes(EXACT10_HEX);
        bytes[0] = (bytes[0] & 0xc0) | 5; // preLongs = 5, illegal
        assert!(matches!(
            CompactThetaSketch::deserialize(&bytes),
            Err(SketchError::InvalidPreambleLongs { found: 5 })
        ));
    }

    #[test]
    fn test_truncated_body_rejected() {
        // Risk: a preamble that claims 10 hashes but only carries 2 must error, not panic.
        let mut bytes = hex_to_bytes(EXACT10_HEX);
        bytes.truncate(16 + 8); // preamble + one hash only
        assert!(matches!(
            CompactThetaSketch::deserialize(&bytes),
            Err(SketchError::TruncatedInput { .. })
        ));
    }

    #[test]
    fn test_seed_hash_mismatch_rejected() {
        // Risk: a blob written with a non-9001 seed must be rejected (its hashes are incomparable).
        let bytes = hex_to_bytes(SINGLE_HEX);
        // Deserialize with a different seed → its seed hash differs → mismatch.
        let result = CompactThetaSketch::deserialize_with_seed(&bytes, 12345);
        assert!(matches!(result, Err(SketchError::SeedHashMismatch { .. })));
    }

    #[test]
    fn test_not_compact_rejected() {
        // Risk: an update-form (non-compact) sketch must be rejected by the compact parser.
        let mut bytes = hex_to_bytes(EXACT10_HEX);
        bytes[5] &= !FLAG_COMPACT; // clear COMPACT flag
        assert!(matches!(
            CompactThetaSketch::deserialize(&bytes),
            Err(SketchError::NotCompact)
        ));
    }

    #[test]
    fn test_string_values_round_trip() {
        // Risk: string/binary columns hash via the byte path; the set + serialization must survive.
        let mut sketch = ThetaSketch::new();
        for index in 0..5 {
            sketch.update_bytes(format!("item-{index}").as_bytes());
        }
        assert_eq!(sketch.retained_entries(), 5);
        let bytes = sketch.serialize_compact().unwrap();
        let parsed =
            CompactThetaSketch::deserialize_with_seed(&bytes, DEFAULT_UPDATE_SEED).unwrap();
        assert_eq!(parsed.retained_entries(), 5);
        assert_eq!(parsed.estimate(), 5.0);
    }

    #[test]
    fn test_starting_sub_multiple_and_threshold_match_java() {
        // Risk: a wrong initial table size silently changes which path (resize vs quickselect)
        // a build takes, diverging the retained set. Pin the helpers against the bytecode.
        // lgNom=4: startingSubMultiple(5, 3, 5) == 5; threshold floor(0.9375 * 32) == 30.
        assert_eq!(starting_sub_multiple(5, 3, 5), 5);
        assert_eq!(set_hash_table_threshold(4, 5), 30);
        // lgNom=12 (Iceberg default): startingSubMultiple(13, 3, 5) == (13-5)%3+5 == 7.
        assert_eq!(starting_sub_multiple(13, 3, 5), 7);
        // get_stride is always odd.
        assert_eq!(get_stride(0, 5) & 1, 1);
        assert_eq!(get_stride(12345, 5) & 1, 1);
    }

    // ---- Y1-reviewer: estimation breadth (#2), serial-version / ordered posture (#3),
    //      hostile-bytes battery (#4), and the bit-exact estimator (#5). ----

    #[test]
    fn test_estimation_mode_lgk8_5000_matches_java_bytes_and_estimate() {
        // Risk: one lgK=4 fixture is thin. A second, seed-independent value set at a different lgK
        // (deep resize + repeated quickselect) catches a quickselect/resize-path bug the lgK=4 set
        // could miss. Byte-exact + bit-exact estimate vs datasketches-java-3.3.0.
        let mut sketch = ThetaSketch::with_lg_nominal_longs(8, DEFAULT_UPDATE_SEED);
        for index in 0..5000u64 {
            sketch.update_u64(1_000_000 + index * 7);
        }
        assert_eq!(sketch.retained_entries(), 400);
        assert_eq!(sketch.theta_long(), 722749449306535422);
        let bytes = sketch.serialize_compact().unwrap();
        assert_eq!(bytes, hex_to_bytes(EST5000_LGK8_HEX), "lgK=8 bytes diverge");
        assert_eq!(bytes.len(), 3224);
        // Java getEstimate() == 5104.602733743722 (bit-exact).
        assert_eq!(sketch.estimate().to_bits(), 5104.602733743722f64.to_bits());
    }

    #[test]
    fn test_estimation_default_lgk12_100k_contract_matches_java() {
        // Risk: the DEFAULT lgK (12, 4096 nominal — what Iceberg's theta_sketch_agg actually uses)
        // entering estimation with 100k distinct values. The retained set + theta + estimate are the
        // cross-engine contract; pinned against datasketches-java-3.3.0 at scale.
        let mut sketch = ThetaSketch::with_lg_nominal_longs(12, DEFAULT_UPDATE_SEED);
        for index in 0..100_000u64 {
            sketch.update_u64(index);
        }
        assert_eq!(sketch.retained_entries(), 4285);
        assert_eq!(sketch.theta_long(), 403733047849016500);
        // Java getEstimate() == 97891.78614058554 (bit-exact).
        assert_eq!(sketch.estimate().to_bits(), 97891.78614058554f64.to_bits());
    }

    #[test]
    fn test_estimate_is_bit_exact_with_java_not_just_close() {
        // Risk (#5): Java computes `count * (2^63_as_f64 / theta)`; the algebraically-equal
        // `count / (theta / MAX)` rounds one ULP differently on the lgK=4 fixture. The estimate is
        // part of the cross-engine NDV contract, so it must match Java BIT-for-bit, not within a
        // tolerance. This test pins the exact f64 bits (the < 1e-6 assertion elsewhere would not).
        let est = CompactThetaSketch::deserialize_with_seed(
            &hex_to_bytes(EST1000_LGK4_HEX),
            DEFAULT_UPDATE_SEED,
        )
        .unwrap();
        assert_eq!(est.theta, 266783384329207353);
        assert_eq!(est.retained_entries(), 24);
        // Java getEstimate() == 829.7403132548839 == bits 0x4089edec295b142c.
        assert_eq!(est.estimate().to_bits(), 0x4089_edec_295b_142cu64);
        assert_eq!(est.estimate(), 829.7403132548839);
    }

    #[test]
    fn test_unordered_compact_blob_is_accepted_like_java() {
        // Risk (#3): DataSketches CAN emit an UNORDERED compact sketch (`compact(false, ...)`) with
        // the ORDERED flag CLEAR (flags 0x0a). The reader must accept it verbatim (count + theta are
        // what matter), matching Java's heapify which only reads the ORDERED bit into `dstOrdered`
        // and never re-validates hash ordering. This is the same 10-element set as EXACT10, in the
        // hash-table iteration order rather than ascending.
        let unordered = "02030300000acc930a0000000000803f15f97dcbbd86a1051ad1300b998c2f22ba40b3c1da06695dbd3273724691cc1440de2ee1c9db3d08c397fc1281709d1ee0f48bea9983c37ce56b61eec8804420698bb991b8685708fe162113fb98bc10";
        let parsed = CompactThetaSketch::deserialize_with_seed(
            &hex_to_bytes(unordered),
            DEFAULT_UPDATE_SEED,
        )
        .unwrap();
        assert_eq!(parsed.retained_entries(), 10);
        assert!(!parsed.is_ordered, "unordered flag must be reported");
        assert_eq!(parsed.estimate(), 10.0);
        // The stored hashes are NOT ascending (proves we did not silently sort/reject).
        assert!(parsed.hashes.windows(2).any(|w| w[0] > w[1]));
    }

    #[test]
    fn test_serial_version_1_and_2_rejected_loudly() {
        // Posture (#3): Java accepts serial-versions 1 and 2 via ForwardCompatibility.heapify{1,2}to3.
        // Rust deliberately reads v3 ONLY — every Iceberg `apache-datasketches-theta-v1` blob written
        // by a current engine is v3, so v1/v2 are unreachable in practice. The reader must reject them
        // LOUDLY (never silently misparse a legacy layout), not panic.
        for version in [1u8, 2u8] {
            let mut bytes = hex_to_bytes(SINGLE_HEX);
            bytes[1] = version;
            assert!(matches!(
                CompactThetaSketch::deserialize(&bytes),
                Err(SketchError::UnsupportedSerialVersion { found }) if found == version
            ));
        }
    }

    #[test]
    fn test_prelongs1_nonsingle_nonempty_reads_as_zero_entries_like_java() {
        // Risk (#1/#4 — REGRESSION for the reviewer's panic fix): a compact blob with preLongs=1 that
        // is neither EMPTY nor SINGLEITEM (flags 0x0a) is exactly 8 bytes. The previous reader fell
        // through to the multi-entry path and indexed `bytes[8..12]`, PANICKING. Java's
        // `memoryToCompact` reads curCount=0 for preLongs<=1 (not single). The reader must now return
        // a 0-entry sketch — no panic. Pinned against Java heapify (retained=0, theta=MAX, est=0.0).
        let bytes = hex_to_bytes("01030300000acc93");
        assert_eq!(bytes.len(), 8);
        let parsed =
            CompactThetaSketch::deserialize_with_seed(&bytes, DEFAULT_UPDATE_SEED).unwrap();
        assert_eq!(parsed.retained_entries(), 0);
        assert_eq!(parsed.theta, MAX_THETA);
        assert!(!parsed.is_empty);
        assert_eq!(parsed.estimate(), 0.0);
    }

    #[test]
    fn test_prelongs1_legacy_single_flag_reads_one_hash_like_java() {
        // Risk (#1): DataSketches' `otherCheckForSingleItem` treats a v3 compact 1-long preamble whose
        // low-5 flag bits == 26 (READ_ONLY|COMPACT|ORDERED, NO SINGLEITEM bit) as a single item and
        // reads the hash at byte 8 — the legacy single-item encoding. Java heapify returns retained=1.
        // Without the fix the reader would fall through to multi-entry and panic/misread.
        let bytes = hex_to_bytes("01030300001acc9315f97dcbbd86a105");
        let parsed =
            CompactThetaSketch::deserialize_with_seed(&bytes, DEFAULT_UPDATE_SEED).unwrap();
        assert_eq!(parsed.retained_entries(), 1);
        assert_eq!(parsed.theta, MAX_THETA);
        assert_eq!(parsed.estimate(), 1.0);
        assert_eq!(parsed.hashes[0], 0x05a1_86bd_cb7d_f915);
    }

    #[test]
    fn test_retained_count_allocation_bomb_errors_before_alloc() {
        // Risk (#4): a hostile blob claiming retained = i32::MAX in a 16/24-byte buffer must fail
        // CLOSED with a TruncatedInput error BEFORE allocating ~16 GiB — never panic, never OOM.
        // (Java's heapify trusts curCount and throws OutOfMemoryError; Rust fails the length check
        // first, which is strictly safer and still loud.)
        // preLongs=2, count=0x7fffffff, only 16 bytes present.
        let bomb2 = hex_to_bytes("02030300001acc93ffffff7f0000803f");
        assert_eq!(bomb2.len(), 16);
        assert!(matches!(
            CompactThetaSketch::deserialize_with_seed(&bomb2, DEFAULT_UPDATE_SEED),
            Err(SketchError::TruncatedInput { .. })
        ));
        // preLongs=3, count=0x7fffffff, only 24 bytes present.
        let bomb3 = hex_to_bytes("03030300001acc93ffffff7f0000803f0102030405060708");
        assert_eq!(bomb3.len(), 24);
        assert!(matches!(
            CompactThetaSketch::deserialize_with_seed(&bomb3, DEFAULT_UPDATE_SEED),
            Err(SketchError::TruncatedInput { .. })
        ));
    }

    #[test]
    fn test_empty_flag_with_nonzero_count_short_circuits_like_java() {
        // Risk (#4): the EMPTY flag (0x04) takes precedence over the retained-count field. A blob
        // with EMPTY set but a non-zero count field must read as an empty sketch — matching Java's
        // heapify (which returns empty=true, retained=0) — not trust the bogus count.
        let bytes = hex_to_bytes("03030300001ecc93050000000000803f0102030405060708");
        let parsed = CompactThetaSketch::deserialize(&bytes).unwrap();
        assert!(parsed.is_empty);
        assert_eq!(parsed.retained_entries(), 0);
        assert_eq!(parsed.estimate(), 0.0);
    }

    #[test]
    fn test_corrupt_theta_zero_yields_infinity_like_java() {
        // Risk (#4/#5): theta == 0 is corrupt (a valid theta is >= 1). The estimator divides by
        // theta; Java's `count * (2^63/0.0)` yields +inf, and Rust must match (no panic, no NaN, no
        // divide-by-zero trap). Parity with Java's getEstimate on a corrupt blob.
        let bytes =
            hex_to_bytes("03030300001acc93010000000000803f00000000000000000102030405060708");
        let parsed =
            CompactThetaSketch::deserialize_with_seed(&bytes, DEFAULT_UPDATE_SEED).unwrap();
        assert_eq!(parsed.theta, 0);
        assert!(parsed.estimate().is_infinite());
        assert!(parsed.estimate().is_sign_positive());
    }

    #[test]
    fn test_descending_ordered_hashes_accepted_verbatim_like_java() {
        // Risk (#4): an ORDERED-flagged blob whose hashes are actually DESCENDING (non-monotonic).
        // Java does not re-validate ordering on read; Rust accepts the bytes verbatim too. Pinned so
        // a future "validate ascending" change is a conscious divergence, not an accident.
        let bytes =
            hex_to_bytes("02030300001acc93020000000000803fff000000000000000100000000000000");
        let parsed =
            CompactThetaSketch::deserialize_with_seed(&bytes, DEFAULT_UPDATE_SEED).unwrap();
        assert_eq!(parsed.hashes, vec![255, 1]);
        assert!(parsed.is_ordered);
    }
}
