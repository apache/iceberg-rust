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

//! DataSketches MurmurHash3 (128-bit, x64) — a byte-exact port of Apache DataSketches'
//! `org.apache.datasketches.hash.MurmurHash3`.
//!
//! This is NOT the canonical byte-stream MurmurHash3: DataSketches processes the input as a
//! sequence of 16-byte blocks (two little-endian `u64`s per block) and takes a 64-bit seed,
//! whereas the common `murmur3` crate streams bytes with a 32-bit seed. The block/finalization
//! structure is ported one-to-one from the 1.10.0 jar bytecode
//! (`MurmurHash3$HashState::blockMix128` / `finalMix128` / `finalMix64`) so the hashes match Java
//! to the bit. Theta NDV correctness depends on this exactness — a single divergent bit makes the
//! sketch incompatible with every other DataSketches engine.
//!
//! The Iceberg theta blob uses the DataSketches default seed [`DEFAULT_UPDATE_SEED`] = 9001.

use crate::error::{SketchError, SketchResult};

/// The DataSketches default update seed (`Util.DEFAULT_UPDATE_SEED`). Apache Iceberg's
/// `apache-datasketches-theta-v1` blob is always built with this seed.
pub const DEFAULT_UPDATE_SEED: u64 = 9001;

/// First mixing constant `C1` (`0x87c37b911142_53d5`).
const C1: u64 = 0x87c3_7b91_1142_53d5;
/// Second mixing constant `C2` (`0x4cf5ad4327_45937f`).
const C2: u64 = 0x4cf5_ad43_2745_937f;
/// Block-merge additive constant for `h1` (`0x52dce729`).
const BLOCK_CONST_H1: u64 = 0x52dc_e729;
/// Block-merge additive constant for `h2` (`0x38495ab5`).
const BLOCK_CONST_H2: u64 = 0x3849_5ab5;

/// The running 128-bit hash state — mirrors Java's `MurmurHash3$HashState`.
struct HashState {
    h1: u64,
    h2: u64,
}

impl HashState {
    #[inline]
    fn new(seed: u64) -> Self {
        // Java seeds both halves with the same seed value.
        HashState { h1: seed, h2: seed }
    }

    /// `mixK1`: `k *= C1; k = rotl(k, 31); k *= C2`.
    #[inline]
    fn mix_k1(k: u64) -> u64 {
        k.wrapping_mul(C1).rotate_left(31).wrapping_mul(C2)
    }

    /// `mixK2`: `k *= C2; k = rotl(k, 33); k *= C1`.
    #[inline]
    fn mix_k2(k: u64) -> u64 {
        k.wrapping_mul(C2).rotate_left(33).wrapping_mul(C1)
    }

    /// `finalMix64`: the standard MurmurHash3 64-bit finalizer.
    #[inline]
    fn final_mix64(mut k: u64) -> u64 {
        k ^= k >> 33;
        k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
        k ^= k >> 33;
        k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
        k ^= k >> 33;
        k
    }

    /// `blockMix128`: fold one complete 16-byte block (two `u64`s) into the state.
    #[inline]
    fn block_mix128(&mut self, k1: u64, k2: u64) {
        self.h1 ^= Self::mix_k1(k1);
        self.h1 = self.h1.rotate_left(27);
        self.h1 = self.h1.wrapping_add(self.h2);
        self.h1 = self.h1.wrapping_mul(5).wrapping_add(BLOCK_CONST_H1);

        self.h2 ^= Self::mix_k2(k2);
        self.h2 = self.h2.rotate_left(31);
        self.h2 = self.h2.wrapping_add(self.h1);
        self.h2 = self.h2.wrapping_mul(5).wrapping_add(BLOCK_CONST_H2);
    }

    /// `finalMix128`: fold the (possibly zero) tail and the byte length, then finalize.
    #[inline]
    fn final_mix128(&mut self, k1: u64, k2: u64, length_bytes: u64) -> [u64; 2] {
        self.h1 ^= Self::mix_k1(k1);
        self.h2 ^= Self::mix_k2(k2);
        self.h1 ^= length_bytes;
        self.h2 ^= length_bytes;
        self.h1 = self.h1.wrapping_add(self.h2);
        self.h2 = self.h2.wrapping_add(self.h1);
        self.h1 = Self::final_mix64(self.h1);
        self.h2 = Self::final_mix64(self.h2);
        self.h1 = self.h1.wrapping_add(self.h2);
        self.h2 = self.h2.wrapping_add(self.h1);
        [self.h1, self.h2]
    }
}

/// Hashes a single `u64` value (the `hash(long, long)` overload).
///
/// DataSketches treats a lone long as a degenerate "block": `finalMix128(value, 0, 8)` with no
/// `blockMix128` call. This is the path used to compute the seed hash and to hash long-valued
/// Iceberg columns for NDV.
#[inline]
pub fn hash_long(value: u64, seed: u64) -> [u64; 2] {
    let mut state = HashState::new(seed);
    state.final_mix128(value, 0, 8)
}

/// Hashes a slice of `u64`s (the `hash(long[], long)` overload). `length_bytes` is `values.len() * 8`.
///
/// # Errors
/// Returns [`SketchError::EmptyHashInput`] when `values` is empty — DataSketches' `checkPositive`
/// rejects a zero-length array rather than returning a fixed hash.
pub fn hash_longs(values: &[u64], seed: u64) -> SketchResult<[u64; 2]> {
    if values.is_empty() {
        return Err(SketchError::EmptyHashInput);
    }
    let mut state = HashState::new(seed);
    let block_count = values.len() / 2;
    for block in 0..block_count {
        state.block_mix128(values[block * 2], values[block * 2 + 1]);
    }
    // The trailing long, if the count is odd (one of the two finalizer lanes is zero).
    let tail = if values.len() & 1 == 1 {
        values[block_count * 2]
    } else {
        0
    };
    let length_bytes = (values.len() as u64) * 8;
    Ok(state.final_mix128(tail, 0, length_bytes))
}

/// Hashes a byte slice (the `hash(byte[], long)` overload).
///
/// Processes 16-byte blocks (two little-endian `u64`s each), then assembles the 0..16-byte tail
/// into `k1`/`k2` exactly as Java's switch-on-remainder does before [`HashState::final_mix128`].
///
/// # Errors
/// Returns [`SketchError::EmptyHashInput`] when `bytes` is empty — DataSketches' `checkPositive`
/// rejects a zero-length array.
pub fn hash_bytes(bytes: &[u8], seed: u64) -> SketchResult<[u64; 2]> {
    if bytes.is_empty() {
        return Err(SketchError::EmptyHashInput);
    }
    let mut state = HashState::new(seed);
    let block_count = bytes.len() / 16;
    for block in 0..block_count {
        let offset = block * 16;
        let k1 = read_u64_le(&bytes[offset..offset + 8]);
        let k2 = read_u64_le(&bytes[offset + 8..offset + 16]);
        state.block_mix128(k1, k2);
    }

    let tail = &bytes[block_count * 16..];
    let (k1, k2) = assemble_tail(tail);
    Ok(state.final_mix128(k1, k2, bytes.len() as u64))
}

/// Reads exactly 8 bytes little-endian into a `u64`.
#[inline]
fn read_u64_le(eight: &[u8]) -> u64 {
    let mut array = [0u8; 8];
    array.copy_from_slice(eight);
    u64::from_le_bytes(array)
}

/// Assembles a 0..16-byte tail into the `(k1, k2)` lanes, mirroring Java's per-byte `shl` cascade.
#[inline]
fn assemble_tail(tail: &[u8]) -> (u64, u64) {
    let mut k1: u64 = 0;
    let mut k2: u64 = 0;
    let len = tail.len();
    // Bytes 8..15 fill k2 (low to high); bytes 0..7 fill k1.
    for (index, &byte) in tail.iter().enumerate() {
        if index < 8 {
            k1 |= (byte as u64) << (8 * index);
        } else {
            k2 |= (byte as u64) << (8 * (index - 8));
        }
    }
    let _ = len;
    (k1, k2)
}

/// Computes the 16-bit DataSketches seed hash for a given seed (`Util.computeSeedHash`).
///
/// Defined as `hash([seed], 0)[0] & 0xFFFF`. For the Iceberg default seed 9001 this is 37836.
///
/// # Errors
/// Returns [`SketchError::ZeroSeedHash`] if the seed produces a zero seed hash (Java throws here);
/// 9001 never does.
pub fn compute_seed_hash(seed: u64) -> SketchResult<u16> {
    let full = hash_longs(&[seed], 0)?;
    let seed_hash = (full[0] & 0xFFFF) as u16;
    if seed_hash == 0 {
        return Err(SketchError::ZeroSeedHash { seed });
    }
    Ok(seed_hash)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Java oracle: org.apache.datasketches.Util.computeSeedHash(9001) == 37836.
    // Generated by dev fixture Gen.java against datasketches-java-3.3.0.
    #[test]
    fn test_compute_seed_hash_9001_matches_java() {
        // Risk: a wrong seed hash silently makes every blob unreadable by Java (seed-hash mismatch).
        assert_eq!(compute_seed_hash(DEFAULT_UPDATE_SEED).unwrap(), 37836);
    }

    #[test]
    fn test_hash_long_value_1_matches_java() {
        // Risk: the long-hashing path diverging means every long-valued NDV diverges.
        // Java: hash(long[]{1L}, 9001) == [811507182322053675, 16783237272830240613]
        let result = hash_longs(&[1], DEFAULT_UPDATE_SEED).unwrap();
        assert_eq!(result[0], 811507182322053675u64);
        assert_eq!(result[1], 16783237272830240613u64);
    }

    #[test]
    fn test_hash_long_single_overload_matches_array_overload() {
        // Java: hash(1L, 9001) == hash(long[]{1L}, 9001). The single-long fast path must agree.
        let single = hash_long(1, DEFAULT_UPDATE_SEED);
        let array = hash_longs(&[1], DEFAULT_UPDATE_SEED).unwrap();
        assert_eq!(single, array);
    }

    #[test]
    fn test_hash_long_known_vectors_match_java() {
        // Risk: block vs tail handling for the long path. Each pinned against Gen2.java output.
        let cases: &[(u64, u64, u64)] = &[
            (0, 4650249816222390219, 11131435645388517298),
            (1, 811507182322053675, 16783237272830240613),
            (2, 4412086184306093958, 3957119527107230474),
            (u64::MAX, 2087312376421901529, 7243929014912122502), // -1L
            (123456789, 10889116934698988946, 7084972492449668709),
            (
                0x8000_0000_0000_0000,
                17523321407797336437,
                2415194165527083631,
            ), // Long.MIN
            (
                0x7fff_ffff_ffff_ffff,
                4002618241258404607,
                4409248992697414981,
            ), // Long.MAX
        ];
        for &(value, expected_h1, expected_h2) in cases {
            let result = hash_long(value, DEFAULT_UPDATE_SEED);
            assert_eq!(result[0], expected_h1, "h1 mismatch for {value}");
            assert_eq!(result[1], expected_h2, "h2 mismatch for {value}");
        }
    }

    #[test]
    fn test_hash_bytes_all_tail_lengths_match_java() {
        // Risk: the byte-tail switch (0..15 leftover bytes) is the easiest place to drift.
        // "0123456789abcdefXYZ"[..len] hashed with seed 9001, pinned against Gen2.java for len 1..=18.
        let base = b"0123456789abcdefXYZ";
        let expected: &[(usize, u64, u64)] = &[
            (1, 17042770340677059783, 1024190034996811051),
            (2, 3624182529596621378, 10995392388412396048),
            (3, 16814881781816398211, 2614988551244636500),
            (4, 5405430862944215782, 6238000445414628299),
            (5, 18215665259540650138, 8221071799270095505),
            (6, 14897616347385554639, 8783068347568985934),
            (7, 3886705525017619838, 14083411504994225039),
            (8, 187136084882017701, 11586484421923796158),
            (9, 13924264334057898021, 11554039545103497790),
            (10, 17579566394514981921, 6653283052961074134),
            (11, 10622088179445861509, 863985400059898186),
            (12, 3984435718574281470, 2275363259253117820),
            (13, 12914523260373086523, 3041660387479922359),
            (14, 4712152595523311661, 838719515658091014),
            (15, 6333319067145729191, 6959091633497066201),
            (16, 2700858395109921824, 8157911888552768403), // exactly one block
            (17, 5723306595654597929, 4182451143326858093), // one block + 1 tail byte
            (18, 17606649832191597951, 115412951967574093),
        ];
        for &(len, expected_h1, expected_h2) in expected {
            let result = hash_bytes(&base[..len], DEFAULT_UPDATE_SEED).unwrap();
            assert_eq!(result[0], expected_h1, "h1 mismatch for len {len}");
            assert_eq!(result[1], expected_h2, "h2 mismatch for len {len}");
        }
    }

    #[test]
    fn test_hash_bytes_le_long_equals_long_hash() {
        // Java: hashing 1L via the 8-byte little-endian form equals hashing long[]{1L}.
        // This underpins building a theta sketch from either byte or long Iceberg columns.
        let le = 1u64.to_le_bytes();
        let via_bytes = hash_bytes(&le, DEFAULT_UPDATE_SEED).unwrap();
        let via_long = hash_long(1, DEFAULT_UPDATE_SEED);
        assert_eq!(via_bytes, via_long);
    }

    #[test]
    fn test_update_overload_equivalence_long_path_vs_le8_for_many_values() {
        // Headline contract (Y1-reviewer): Java's `UpdateSketch.update(long v)` hashes via the
        // long-ARRAY path (`MurmurHash3.hash(long[]{v})`), NOT via `v.to_le_bytes()`. Y2 will feed
        // long-valued columns — if a caller instead hashed the LE-8 byte form, the sketch would only
        // match Java IF `hash(long[]{v}) == hash(LE8(v))`. It does, for every value: the single-long
        // path is `finalMix128(v, 0, 8)` and the 8-LE-byte path reduces to the same `(k1=v, k2=0,
        // lengthBytes=8)`. Pinned against datasketches-java-3.3.0 vectors for edge values.
        // Risk: a single divergent value would silently corrupt Y2's value-feeding for long columns.
        let values: [u64; 17] = [
            0,
            1,
            2,
            3,
            u64::MAX,
            255,
            256,
            65535,
            65536,
            123456789,
            0x8000_0000_0000_0000, // Long.MIN
            0x7fff_ffff_ffff_ffff, // Long.MAX
            0x0102_0304_0506_0708,
            0xffff_ffff_ffff_ffff,
            0x8000_0000_0000_0001,
            42,
            0xdead_beef_cafe_babe,
        ];
        for value in values {
            let via_long_array = hash_longs(&[value], DEFAULT_UPDATE_SEED).unwrap();
            let via_single = hash_long(value, DEFAULT_UPDATE_SEED);
            let via_le8_bytes = hash_bytes(&value.to_le_bytes(), DEFAULT_UPDATE_SEED).unwrap();
            assert_eq!(via_single, via_long_array, "single != array for {value}");
            assert_eq!(via_le8_bytes, via_long_array, "LE8 != array for {value}");
        }
    }

    #[test]
    fn test_hash_bytes_multiblock_and_extreme_bytes_match_java() {
        // Risk: the 16-byte block loop (multi-block inputs) and the all-zero / all-0xFF tails are
        // where a block-index or tail-shift error hides — the per-tail-length test only covers 1..=18.
        // All vectors pinned against datasketches-java-3.3.0 `MurmurHash3.hash(byte[], 9001)`.

        // Deterministic multi-block pattern: byte[i] = (i*31+7) & 0xff, lengths spanning many blocks.
        let multiblock: &[(usize, u64, u64)] = &[
            (32, 18067827991026121723, 3170733979262934608),
            (64, 3261044431120185660, 7688580558209580324),
            (100, 18203138622845278841, 7265759745346112457),
            (1000, 6290371237009507117, 2449189515327642072),
        ];
        for &(len, expected_h1, expected_h2) in multiblock {
            let mut buffer = vec![0u8; len];
            for (index, slot) in buffer.iter_mut().enumerate() {
                *slot = ((index * 31 + 7) & 0xff) as u8;
            }
            let result = hash_bytes(&buffer, DEFAULT_UPDATE_SEED).unwrap();
            assert_eq!(result[0], expected_h1, "h1 mismatch for pattern{len}");
            assert_eq!(result[1], expected_h2, "h2 mismatch for pattern{len}");
        }

        // All-zero bytes across block/tail boundaries.
        let zeros: &[(usize, u64, u64)] = &[
            (1, 577496657207264936, 227261091609271464),
            (8, 4650249816222390219, 11131435645388517298),
            (16, 6510113626874825040, 17325734760312219798),
            (17, 3770696296370359454, 11846900099303288626),
            (32, 18272850091436446269, 13423080626484549674),
            (33, 12240544946171321492, 1358470335954300120),
        ];
        for &(len, expected_h1, expected_h2) in zeros {
            let result = hash_bytes(&vec![0u8; len], DEFAULT_UPDATE_SEED).unwrap();
            assert_eq!(result[0], expected_h1, "h1 mismatch for zeros{len}");
            assert_eq!(result[1], expected_h2, "h2 mismatch for zeros{len}");
        }

        // All-0xFF runs across block/tail boundaries.
        let ones: &[(usize, u64, u64)] = &[
            (1, 17759876224205324590, 11832774449320164160),
            (7, 13271399597427191306, 3760464041025908925),
            (8, 2087312376421901529, 7243929014912122502),
            (15, 8079591283864495576, 2123698991461168532),
            (16, 14246821909310903660, 3335402941727292517),
            (17, 5128525722441745885, 1397071597227926016),
            (31, 12396090143832533608, 8904580233204750465),
            (32, 8601845271419794905, 3244551582229296622),
        ];
        for &(len, expected_h1, expected_h2) in ones {
            let result = hash_bytes(&vec![0xffu8; len], DEFAULT_UPDATE_SEED).unwrap();
            assert_eq!(result[0], expected_h1, "h1 mismatch for ff{len}");
            assert_eq!(result[1], expected_h2, "h2 mismatch for ff{len}");
        }
    }

    #[test]
    fn test_hash_longs_multi_element_arrays_match_java() {
        // Risk: the long-array block loop (2+ elements) + odd-tail handling. Pinned against
        // datasketches-java-3.3.0 `MurmurHash3.hash(long[], 9001)`.
        let cases: &[(&[u64], u64, u64)] = &[
            (&[1, 2], 6699810392823224663, 8925721110872640926),
            (&[1, 2, 3], 2648175425730656304, 10281930421578918121),
            (&[1, 2, 3, 4], 7369743747634429344, 11243504068131660622),
            (&[0, 0, 0], 17743926045731775657, 10552146633376958706),
            (
                &[u64::MAX, u64::MAX],
                14246821909310903660,
                3335402941727292517,
            ),
        ];
        for &(values, expected_h1, expected_h2) in cases {
            let result = hash_longs(values, DEFAULT_UPDATE_SEED).unwrap();
            assert_eq!(result[0], expected_h1, "h1 mismatch for {values:?}");
            assert_eq!(result[1], expected_h2, "h2 mismatch for {values:?}");
        }
    }

    #[test]
    fn test_empty_input_rejected_not_panicked() {
        // Risk: DataSketches throws on zero-length input; mirror that loudly, never panic.
        assert!(matches!(
            hash_bytes(&[], DEFAULT_UPDATE_SEED),
            Err(SketchError::EmptyHashInput)
        ));
        assert!(matches!(
            hash_longs(&[], DEFAULT_UPDATE_SEED),
            Err(SketchError::EmptyHashInput)
        ));
    }
}
