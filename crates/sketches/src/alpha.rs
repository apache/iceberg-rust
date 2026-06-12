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

//! Alpha theta sketch (update form) — a one-to-one port of Apache DataSketches'
//! `org.apache.datasketches.theta.HeapAlphaSketch`, the family Apache Iceberg's NDV pipeline actually
//! builds (`ThetaSketchAgg.createAggregationBuffer` → `UpdateSketch.builder.setFamily(Family.ALPHA)
//! .build()`, default lgK 12 / seed 9001).
//!
//! # Why a second update family
//!
//! [`crate::theta::ThetaSketch`] ports `HeapQuickSelectSketch`. In **exact mode** (`theta == MAX`)
//! Alpha and QuickSelect retain the same set and serialize byte-identically. In **estimation mode**
//! they diverge: Alpha lowers `theta` by a fixed per-insert multiplier `alpha = nominal/(nominal+1)`
//! once the table fills past nominal, whereas QuickSelect lowers `theta` to the (nominal+1)-th smallest
//! retained hash via quickselect. On a high-cardinality column the two emit different on-disk bytes and
//! a different NDV. Iceberg writes Alpha; this type closes that cross-engine gap.
//!
//! # The COMPACT form is family-COMPACT, not family-ALPHA
//!
//! [`AlphaSketch::compact`] produces the SAME [`CompactThetaSketch`] that [`crate::theta::ThetaSketch`]
//! serializes — there is one serialization path. Java's `UpdateSketch.compact()` calls
//! `componentsToCompact(thetaLong, getRetainedEntries(true), seedHash, isEmpty, ..., cache)`, which
//! filters the cache to entries `0 < h < theta`, sorts them ascending, and writes the family-COMPACT
//! (id 3) preamble — exactly [`crate::theta`]'s `serialize_compact_from_parts`. The Alpha-specific
//! behavior is entirely on the UPDATE side (which hashes the cache retains and what `theta` becomes).
//!
//! # The two estimators (and which one Iceberg's `ndv` reads)
//!
//! - [`AlphaSketch::estimate`] is the Alpha UPDATE sketch's estimator (`HeapAlphaSketch.getEstimate`):
//!   `theta > split1` → standard `count*(2^63/theta)`; `theta <= split1` → SAMPLING
//!   `nominal*(2^63/theta)`. This is what you would read off the live update sketch.
//! - **Iceberg's `ndv` property reads the COMPACT sketch's estimate**, not this one. Java
//!   `NDVSketchUtil.toBlob` does `Sketch sketch = CompactSketch.wrap(bytes)` then
//!   `String.valueOf((long) sketch.getEstimate())` — the family-COMPACT STANDARD estimator
//!   `compactRetained * (2^63/theta)`. So callers computing the Iceberg `ndv` must use
//!   `alpha.compact().estimate()` ([`CompactThetaSketch::estimate`]), NOT [`AlphaSketch::estimate`].
//!   (Probe, lgK 12: n=1M → update sampling estimate 1002319 vs compact estimate 1004032; Iceberg's
//!   `ndv` is 1004032.)
//!
//! All field/method semantics below are cited to the `datasketches-java-3.3.0` jar bytecode
//! (`javap -p -c -constants org.apache.datasketches.theta.HeapAlphaSketch`).

use crate::error::SketchResult;
use crate::hash::{DEFAULT_UPDATE_SEED, hash_bytes, hash_long};
use crate::theta::{
    CompactThetaSketch, DEFAULT_LG_NOMINAL_LONGS, DEFAULT_LG_RESIZE_FACTOR_LG,
    MIN_LG_ARR_LONGS_SHARED, TWO_POW_63_AS_F64, estimate as compact_estimate, get_stride,
    hash_array_insert, hash_search_or_insert, serialize_compact_from_parts,
    set_hash_table_threshold, starting_sub_multiple,
};

/// Alpha's minimum legal `lg_nominal_longs` (`HeapAlphaSketch.ALPHA_MIN_LG_NOM_LONGS == 9`, i.e. 512
/// nominal). `newHeapInstance` throws below this; the Iceberg-default lgK 12 satisfies it.
pub const ALPHA_MIN_LG_NOM_LONGS: u32 = 9;

/// An Alpha-family theta sketch in mutable (update) form.
///
/// Mirrors Java `HeapAlphaSketch`: an open-addressing hash table sized `2^lg_arr_longs`. Below nominal
/// capacity it grows the table (`resizeClean`); once `theta <= split1` it enters a dirty sampling phase
/// where each insert multiplies `theta` by `alpha` and stale (above-theta) entries accumulate until a
/// rebuild purges them. The serialized COMPACT form keeps only the entries strictly below `theta`.
#[derive(Debug, Clone)]
pub struct AlphaSketch {
    lg_nominal_longs: u32,
    lg_arr_longs: u32,
    seed: u64,
    /// `alpha_` = `nominal / (nominal + 1)` — the per-insert theta-decay multiplier.
    alpha: f64,
    /// `split1_` = `(long)((p * (alpha + 1) / 2) * 2^63)` — the theta boundary where the dirty
    /// sampling phase begins (`p == 1.0` for Iceberg).
    split1: i64,
    cache: Vec<u64>,
    current_count: usize,
    theta: i64,
    is_empty: bool,
    is_dirty: bool,
    hash_table_threshold: usize,
}

impl AlphaSketch {
    /// Builds an empty Alpha sketch with the Iceberg-default nominal entries (lgK 12) and seed 9001 —
    /// exactly `UpdateSketch.builder().setFamily(ALPHA).build()`.
    pub fn new() -> Self {
        Self::with_lg_nominal_longs(DEFAULT_LG_NOMINAL_LONGS, DEFAULT_UPDATE_SEED)
    }

    /// Builds an empty Alpha sketch with the given `lg_nominal_longs` (lgK) and seed.
    ///
    /// `lg_nominal_longs` is clamped to the legal Alpha range `9..=26` ([`ALPHA_MIN_LG_NOM_LONGS`] is
    /// Alpha's hard floor; `HeapAlphaSketch.newHeapInstance` throws below it).
    pub fn with_lg_nominal_longs(lg_nominal_longs: u32, seed: u64) -> Self {
        let lg_nominal_longs = lg_nominal_longs.clamp(ALPHA_MIN_LG_NOM_LONGS, 26);
        // newHeapInstance: nominal = 2^lgK; alpha = nominal/(nominal+1);
        // split1 = (long)((p*(alpha+1)/2)*2^63); theta0 = (long)(p*2^63); p == 1.0 for Iceberg.
        let nominal = (1u64 << lg_nominal_longs) as f64;
        let alpha = nominal / (nominal + 1.0);
        let probability = 1.0_f64;
        let split1 = ((probability * (alpha + 1.0) / 2.0) * TWO_POW_63_AS_F64) as i64;
        let theta = (probability * TWO_POW_63_AS_F64) as i64; // == MAX_THETA (saturating i64 cast)
        let lg_arr_longs = starting_sub_multiple(
            lg_nominal_longs + 1,
            DEFAULT_LG_RESIZE_FACTOR_LG,
            MIN_LG_ARR_LONGS_SHARED,
        );
        let hash_table_threshold = set_hash_table_threshold(lg_nominal_longs, lg_arr_longs);
        AlphaSketch {
            lg_nominal_longs,
            lg_arr_longs,
            seed,
            alpha,
            split1,
            cache: vec![0u64; 1usize << lg_arr_longs],
            current_count: 0,
            theta,
            is_empty: true,
            is_dirty: false,
            hash_table_threshold,
        }
    }

    /// Updates the sketch with a `u64` value via the long-array hash path (identical to the LE-8-byte
    /// path; see [`crate::theta::ThetaSketch::update_u64`]).
    pub fn update_u64(&mut self, value: u64) {
        let full = hash_long(value, self.seed);
        self.hash_update(full[0] >> 1);
    }

    /// Updates the sketch with an arbitrary non-empty byte string (an Iceberg string/binary/decimal
    /// single-value serialization). Empty input is silently ignored, matching Java's
    /// `UpdateSketch.update(byte[])`.
    pub fn update_bytes(&mut self, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        let full = match hash_bytes(bytes, self.seed) {
            Ok(value) => value,
            Err(_) => return,
        };
        self.hash_update(full[0] >> 1);
    }

    /// Mirrors `HeapAlphaSketch.hashUpdate(long)`. Routes to the clean or dirty insert path and applies
    /// the Alpha theta-decay + resize/rebuild rules.
    fn hash_update(&mut self, hash: u64) {
        self.is_empty = false;
        // continueCondition: reject when hash == 0 or hash >= theta.
        if hash == 0 || hash >= self.theta as u64 {
            return;
        }
        if self.is_dirty {
            self.enhanced_hash_insert(hash);
            return;
        }
        // ---- CLEAN path ----
        if hash_search_or_insert(&mut self.cache, self.lg_arr_longs, hash).is_some() {
            return; // RejectedDuplicate
        }
        self.current_count += 1;
        if self.theta > self.split1 {
            // Sampling not yet started.
            let nominal = 1usize << self.lg_nominal_longs;
            if self.current_count > nominal {
                self.decay_theta();
                self.is_dirty = true;
            } else if self.is_out_of_space(self.current_count) {
                self.resize_clean();
            }
        } else {
            // theta <= split1: sampling started (Java asserts lgArr > lgNom here).
            self.decay_theta();
            self.is_dirty = true;
            if self.is_out_of_space(self.current_count) {
                self.rebuild_dirty();
            }
        }
    }

    /// `theta = (long)(theta * alpha)` — the fixed Alpha per-insert decay. The `f64` multiply then a
    /// truncating `as i64` cast mirrors Java's `(long)(((double) thetaLong_) * alpha_)`.
    fn decay_theta(&mut self) {
        self.theta = ((self.theta as f64) * self.alpha) as i64;
    }

    /// `isOutOfSpace(count)` = `count > hashTableThreshold_`.
    fn is_out_of_space(&self, count: usize) -> bool {
        count > self.hash_table_threshold
    }

    /// Mirrors `HeapAlphaSketch.enhancedHashInsert(cache, hash)` — the DIRTY-path open-addressing
    /// insert. Probes for the hash; on the way it remembers the FIRST stale (>= theta) slot and will
    /// reuse it (an in-place "delete + insert") rather than always landing in an empty slot.
    ///
    /// Every successful insert (empty or reused-stale) decays `theta` and sets `dirty`. Reusing a stale
    /// slot does NOT increment `current_count` (`InsertedCountNotIncremented`); using a truly empty slot
    /// does (`InsertedCountIncremented`) and may trigger [`Self::rebuild_dirty`] when the table fills.
    fn enhanced_hash_insert(&mut self, hash: u64) {
        let mask = (1u64 << self.lg_arr_longs) - 1;
        let stride = get_stride(hash, self.lg_arr_longs);
        let mut probe = (hash & mask) as usize;
        let loop_index = probe;
        let mut current = self.cache[probe];

        // Outer loop: skip live (below-theta) entries until we hit the hash, an empty slot, or a stale
        // (>= theta) entry.
        while current != hash && current != 0 {
            if (current as i64) >= self.theta {
                // ---- Stale slot found: this becomes the reuse target. ----
                let target = probe;
                // Inner loop: keep advancing to confirm the hash is not already present further along.
                probe = ((probe as u64 + stride) & mask) as usize;
                current = self.cache[probe];
                while current != hash && current != 0 {
                    probe = ((probe as u64 + stride) & mask) as usize;
                    current = self.cache[probe];
                }
                if current == hash {
                    return; // RejectedDuplicate (already present beyond the stale slot)
                }
                // Reuse the stale slot in place; no count increment.
                self.cache[target] = hash;
                self.decay_theta();
                self.is_dirty = true;
                return; // InsertedCountNotIncremented
            }
            // current < theta: a live entry; keep probing.
            probe = ((probe as u64 + stride) & mask) as usize;
            current = self.cache[probe];
            if probe == loop_index {
                // Java throws "No empty slot in table!"; unreachable past the threshold invariant.
                return;
            }
        }

        if current == hash {
            return; // RejectedDuplicate
        }
        // current == 0: a truly empty slot.
        self.cache[probe] = hash;
        self.decay_theta();
        self.is_dirty = true;
        self.current_count += 1;
        if self.current_count > self.hash_table_threshold {
            self.rebuild_dirty();
        }
    }

    /// Mirrors `resizeClean()`: grow the table while still below nominal capacity. The growth delta is
    /// `max(1, min(rf.lg(), (lgNom+1) - lgArr))`; if already at/above `lgNom+1`, grow by 1.
    fn resize_clean(&mut self) {
        let lg_target = self.lg_nominal_longs + 1;
        let delta = if lg_target > self.lg_arr_longs {
            DEFAULT_LG_RESIZE_FACTOR_LG
                .min(lg_target - self.lg_arr_longs)
                .max(1)
        } else {
            1
        };
        self.force_resize_clean_cache(delta);
    }

    /// Mirrors `forceResizeCleanCache(lgDeltaLongs)`: grow `lg_arr_longs` by the delta, re-insert all
    /// below-theta entries into the larger table, and recompute the threshold. Used only on the CLEAN
    /// (non-dirty) path, so `current_count` is preserved exactly by the re-insert.
    fn force_resize_clean_cache(&mut self, lg_delta_longs: u32) {
        self.lg_arr_longs += lg_delta_longs;
        let mut new_cache = vec![0u64; 1usize << self.lg_arr_longs];
        let count = hash_array_insert(&self.cache, &mut new_cache, self.lg_arr_longs, self.theta);
        self.current_count = count;
        self.cache = new_cache;
        self.hash_table_threshold =
            set_hash_table_threshold(self.lg_nominal_longs, self.lg_arr_longs);
    }

    /// Mirrors `rebuildDirty()`: purge stale (>= theta) entries into a fresh SAME-size table; if nothing
    /// was purged (the table was full of live entries), grow by 1 via [`Self::force_resize_clean_cache`].
    fn rebuild_dirty(&mut self) {
        let previous_count = self.current_count;
        self.force_rebuild_dirty_cache();
        if previous_count == self.current_count {
            self.force_resize_clean_cache(1);
        }
    }

    /// Mirrors `forceRebuildDirtyCache()`: allocate a new SAME-size table, re-insert every entry strictly
    /// below the (now-lowered) `theta`, set the new `current_count`, and clear `dirty`.
    fn force_rebuild_dirty_cache(&mut self) {
        let mut new_cache = vec![0u64; 1usize << self.lg_arr_longs];
        let count = hash_array_insert(&self.cache, &mut new_cache, self.lg_arr_longs, self.theta);
        self.current_count = count;
        self.cache = new_cache;
        self.is_dirty = false;
    }

    /// The current theta as a `long`.
    pub fn theta_long(&self) -> i64 {
        self.theta
    }

    /// Whether the sketch is empty (no value was ever offered).
    pub fn is_empty(&self) -> bool {
        self.is_empty
    }

    /// The number of retained hashes in the COMPACT (below-theta) sense — `getRetainedEntries(true)`:
    /// when dirty, the count of cache entries strictly below `theta` (Java `countPart`); otherwise the
    /// raw `current_count` (which the clean path keeps equal to that count).
    pub fn retained_entries(&self) -> usize {
        if self.is_dirty {
            self.below_theta_hashes().len()
        } else {
            self.current_count
        }
    }

    /// The Alpha UPDATE sketch's estimate (`HeapAlphaSketch.getEstimate`): `theta > split1` → the
    /// standard `count*(2^63/theta)`; `theta <= split1` → the SAMPLING `nominal*(2^63/theta)`.
    ///
    /// NOTE: this is the live-update-sketch estimate. Iceberg's `ndv` property reads the COMPACT
    /// sketch's estimate instead (see the module doc + [`Self::compact`]).
    pub fn estimate(&self) -> f64 {
        if self.theta > self.split1 {
            // Standard estimate over the dirty-aware retained count.
            compact_estimate(self.theta, self.retained_entries(), self.is_empty)
        } else {
            // Sampling-mode estimate: nominal * (2^63 / theta).
            let nominal = (1u64 << self.lg_nominal_longs) as f64;
            nominal * (TWO_POW_63_AS_F64 / (self.theta as f64))
        }
    }

    /// The cache entries strictly below `theta`, ascending-sorted — the compact retained set.
    fn below_theta_hashes(&self) -> Vec<u64> {
        let mut hashes: Vec<u64> = self
            .cache
            .iter()
            .copied()
            .filter(|&h| h != 0 && (h as i64) < self.theta)
            .collect();
        hashes.sort_unstable();
        hashes
    }

    /// Compacts to the immutable family-COMPACT [`CompactThetaSketch`] — the same type and bytes
    /// [`crate::theta::ThetaSketch`] serializes. Mirrors Java `UpdateSketch.compact()` over an Alpha
    /// sketch: keep cache entries `0 < h < theta`, sort ascending, write the family-COMPACT preamble.
    ///
    /// # Errors
    /// Propagates [`crate::error::SketchError::ZeroSeedHash`] only if the seed hashes to zero (9001
    /// never does).
    pub fn compact(&self) -> SketchResult<CompactThetaSketch> {
        let bytes = self.serialize_compact()?;
        // Re-parse through the canonical reader so the COMPACT view (and its standard estimate) is
        // produced by exactly the same code path that reads a Java-written blob — one source of truth.
        CompactThetaSketch::deserialize_with_seed(&bytes, self.seed)
    }

    /// Serializes to the compact byte form — the `apache-datasketches-theta-v1` Puffin blob payload.
    /// Identical layout to [`crate::theta::ThetaSketch::serialize_compact`] (one serialization path).
    ///
    /// # Errors
    /// Returns [`crate::error::SketchError::ZeroSeedHash`] only if the seed hashes to zero (9001 never
    /// does).
    pub fn serialize_compact(&self) -> SketchResult<Vec<u8>> {
        let hashes = self.below_theta_hashes();
        serialize_compact_from_parts(self.is_empty, self.theta, &hashes, self.seed)
    }
}

impl Default for AlphaSketch {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::theta::{MAX_THETA, ThetaSketch};

    /// Fixture provenance: every `ALPHA_*` value/byte below was emitted by the Alpha cases in
    /// `crates/sketches/testdata/theta_fixture_generator.java` run against `datasketches-java-3.3.0`
    /// (~/.m2) with seed 9001 and `UpdateSketch.builder().setFamily(Family.ALPHA)`. See the README.
    fn hex_to_bytes(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("valid hex"))
            .collect()
    }

    // Exact-mode Alpha bytes (lgK 12, 10 distinct longs) — MUST equal the QuickSelect EXACT10 bytes.
    const ALPHA_EXACT10_HEX: &str = "02030300001acc930a0000000000803f15f97dcbbd86a10540de2ee1c9db3d08698bb991b8685708fe162113fb98bc10bd3273724691cc14c397fc1281709d1ee56b61eec88044201ad1300b998c2f22ba40b3c1da06695de0f48bea9983c37c";
    const ALPHA_SINGLE_HEX: &str = "01030300003acc9315f97dcbbd86a105";
    const ALPHA_EMPTY_HEX: &str = "01030300001e0000";

    // Estimation-mode Alpha fixture (lgK 9, 520 distinct longs `0..520`): preLongs=3, retained=514,
    // theta=9080515283922012160, compact getEstimate()=522.0863661049558 (bits 0x408050b0e0b66225).
    const ALPHA_EST_LGK9_520_HEX: &str = "03030300001acc93020200000000803f0048ad438a78047edeac6be1e1d96500fa74b34c16b16e00c7c48185137f7300e4c758129ac406010a22dbb71c875f0188655737d10a7401fb38798913248f01b9bf9feafa9eaa0194ded6c8a0752a02b833b51ac36e72030f5d1190dc88b103393eeb1613ceb303e662386e3d7fd303736268de4b6bfc034962bb9062b6fc03e8a7bd68fe0b8604e2dacf9fcdeabe0445a54733acacdd04ed0b4d6d52ba060552ec19491d2d680515f97dcbbd86a1052596796c863eb205f7ce79d2837fc9056aa9cade240aed05fb8ab7e53efb7b065dbd9f79e3553207aecbd00df30056073c2f7b180a0fbb07f9a53149b2f7e10763319a572b2de6078e958d10615df0071f728df16d810e0840de2ee1c9db3d08da867b095f474508698bb991b8685708dc3f9f540c995a08fa69f9593ca28308a0fd066176e58e081cf5d5c57106c408f385d11765f6c508be111e938546fe08436cf0e814e03709a8c1da2d5ffa87099b263ecacc5b9709b4659a020a47f40928208425807c510a0503c0251079c20ab21e8b1c4525480b614ecd35427f6f0bb26f45601710360ccddd31b4198a800cac287907ccd6cf0cfdc6c548069ed10c83b932ccf95b7f0dca21eb031d99bd0d8b823d3e056dce0db821e450ee6b660eb2620ba2f404d50e6a918b0b0be72c0fad67b1b395585f0f94e5e7319d8b600f8d78d98f60a6820f435b5536eef09b0fa1b6e3d18204dc0fc4135026dc1517109089ab3971c01810fe162113fb98bc10d721e1056e19d610bc7aba16f93bf9105abf7438df2611116f2bbce0f96b5d115e4407176a13a211919df55cf66fba116c451ad95640c011a941ed97e13fd5110a6d14091666d5110464c625b83509126bc6ce371d821112a0fe7025d41237123aaf3425407c8112cf55507ad206931219648b8c27d89412233c99e753e3bf122e9f0c12d016e5125f671bd84e440f13f328775e8c381f134d45a93d422ef613cb310b8765060a14e77024e4a6db1714b7ffa9e61c171b14bd3273724691cc145e1029e108e6df14e802fa33b628f81492978236e5b4fa14e9ec774fa8807e15ce2798b85acf7f15ca0361aae7339f1566ad6c580f96d015717c6b9ed8d96716e1b8e7c83740a316df667f308282a91653523f6fd889ab16d15ea0428d9ceb160b1710f9327600175084584e7a2cd017ad04c485b271d117759936b55732e8170eecf476273a0e18503d42a326b5ab18e258d346278fb51959545c7d9564d619cc24210aa95cdd192adbd05d816ff91958a07ef020a5431ab3c141b1764d7a1a6efb6d179538ac1abdb72d1c8377551b0d9154ca86df801ba60cd3805392931b7042a38f13a7c61b9ba3841106ddc91b8ea6d38d5134e51b559fb1735fae741c4ff24de0bc0cae1c050a27be7582061de7a4b9cc1f766b1d6fb169e9fd86721d793c2e4ea1d58b1d0902bd41b058a41d10860424e5edc01dea21378621d93e1e6da916bc4a66611e692e2450ccfb941ec397fc1281709d1e4ba441d03eb1ea1ee50176616597591fb8627a5e6dc0671f23a55b381afd741f06135d9f6929891f85d620bbecdfc11fba47b55464e0fe1f9afb298d666a38206546afaa13a44120e56b61eec880442055710d229117a8203ebadca36f6fb6201f4824da70f817219a9871fee43f4121746994069c5f6521b6f28e894f5f72213c6fd824cce47621c624d61245fe7e217057568a5cfa1322315bbb135742142250e7250ca5092d221ad1300b998c2f228ad464fd3b354c22c0a2ad94080e5a22dd7ab5af1f1f612235a09d646fb2e422d388280e933efc22c6c84cde883d2b23ea4b8a9161ce602350fac0135b25692396f51e8d78ee3e247e0aeffa617d5524f0f5753327d41b252251bc89db261d25d1ce907b99582625b8dba51a00cc8925f796fbd50949aa25ce11097c32f909264d69f5d6d58b2a268cd0c9864cc93a26caaca917d32d9b2670ff80bfcb98c1260321255484cad826395e67a280d61a27b7bf427995364a27786573adfb894b275f7a7c1582608627178d896d8fdc9227d3f2311a62c3a727f0ecbf20b4c4ad27c9439b4807d2d22790540846b8532a28cb5d302df6c43c28e84221a0e921c628bd65fb53cb26e12823ebd9a25e181d29d5b943d7862936299936de3747b23f29f0f6a4e1159d7d29b2895be3cf63b929f0982c22f993bf29efed8cb1763bd2299729bb21cb96ee29e989b3c427290f2a72972a2b04257d2af9fe1c2bc47fe82a9e7cb9eca01a742b2bcc3f7dbe11842b98a12f29f52c972b63c5f250575f182cef567c303f57362c37eb67321b8a8e2cb6ffb29ab891a62c5a009e423836b22c5e1b65951a3eba2cab022299f7b4be2c76f49ec84a12cb2ce041704ce2863b2d6aafdf995c68682d9ad4182833a6a12d67d0f76013084c2e685eb9069f54f12eac7704ec1e69252f57f760a5a6503a2f793c4bb4da264b305e0f82f1bb42593052463e230fcf82306d076e37996cb4309912b1088513b730d65b666cfd8c2631df8cc0e645474b31cae110ba40869231bac0c7156aedb131424b32502f6fff31db8ce73be68c0e32d771a1e06b725d32631437a39c3ad4323ea1b8c18cfc023348d7055521fe93336a8f7ee03a7aa63356aaa6db396b1434dde48048b5c22634547d6e772638a634a18b2e6c674b2035a6b3ef4418c44b3546e2c432ab245535220d71f225839635f950fd6ac1c5a435870905b6f441a735453134877ef80d36d7a26026496fd0363de1164c15e5d13642227e3f7d601f37a10da502e6306a37d1a2fd2c6b157537019940a8a3317c3729315d349eb981370c0523d313cb8d37c42e59acf29b2139a0e358515941563973f7831f13db8739cb8928ab341ad73903ed1f15bb60f8399d34578e49662d3a5cf6b706ec3a633a8c182ffbe5f3843a571ec49caf41973ac6b546b62aba133bc234e6e259841d3bb6a1c5c7a841393bc05719a286d0633b90d40c17eab4a13b003851d5bf89f93bc0dfa5390d80fb3b9c459445500b093ca0eb6b2a82b01e3c97d145bfe25b2e3c16a302d7ee663c3c3ec0b1aa3e25713c6b3c1a37cc05763c1a175f49605f873c8d819f94f905fe3c5cab5440133d793df442b693af09c73ddb9704f17adb223eca5027d44eee3f3e587a993dc857b13edf6a4c38a7fec43ec9659f5288c9033f67f8adf578d98b3fb765a5dbd68d8d3f503df232d443a63fc87b09a1c88af93f66d7d2aee6d76040adc13a6550bcee404dc49bfff07ab541cfc5b5394300da41e25bb782f288244254ac99488c1b3942db05bbfe39b95b42d1794e9fb761c7425067f9cca831e24261581c0cd285fd425e8e8a3924510043ff191bf65cde6043a7a65f00eec57d4458896dc18486b944e1ee28e20ad8cd447f10b97d0b8b1e459f66104219cf2645fde9e5bbdbfc7d45548d927993b6a845d4941d67fc71cc45ba84992e9ada6c46d609f2304ed38546a2b1e773bd7a0b47122fd3f489168c47b9f016b90efc98479a81a125fe3bf947a9b61742298e01480d5ee8e6d7194048b7621a298d094b48793e0e392b5ccc48aabda3c1fb260b49643d126e8bd73b491e14f84d728cd449693ec65a4fb30d4a81d359e79bf58f4ab97c04311b3a914a04e2b8c793ebe44affa96f0c7bce7e4b7452c75136d3ab4ba67e65032cbcb84bac5b9c8d299e324c1b21786f70cc3d4c4976d15c67a4744cbcef325e8a87ac4cd460622ad38eb04cfce616362299424dd6e3f974672e674d10ee4f39f314234eb90603bd49018d4e6ead0067be4ce84ee30c3516c90aee4e53e6aeb387f7c24fc76158f740c44b509972f213b34e735098a2ac7dc780905035667fe89b374051589fceadaa5e8b517bfe8f23818e1b520719ba2ffe9b2352bd68f3b02aab6c5232cca3fc9b432a53c0e0f9a2c7c5ba530401b440e4dfca5318b6818ff75ccc532f4273ec7615ff539267e7c43f0203544071f816718a2654a0ed3a90506a9e54f9aced09bd180055d49dda3f67af2155b31f41af5b7a765552879e359e74bd55437b00cf38d2305693c04d62bd355d56f32a37890d746356a9753ac8e1307b565cb07ce45623d7563b01ffa1b643db566f07d8551f690157eef5b4b6ac130e575db4593b41b3135759d84fe1426e3c5731c387c0ea9c6757cb5f1436252b685740cfc2b1c6ac8c57ea45460466afcd57efa916ee8509e75710b371654002f65751217877778066581725108c6e2fde5854a77620ce1a2159e1da9222beae885a78dd956b801a8d5a7ecd98d0defdbe5a4a1c791e8cb6d55a775e98dc15041d5b72484f86c5ca275ba476357d6ac68c5b46d47b3c3d6ac55bbc23b4847ef8eb5b34b6fb5c4ee0fe5bfdb4610979da425c683d832b29066b5c6de2a9d65fc6765c5a771ea8fbde925cd94a78f239d99f5c4b58e07a5cbfbc5cf0523a3f5938cd5cba40b3c1da06695d72284447c4dc4e5ed2b7551754fdd35e967f7818d512e15ec2316c8f3703f65e1bd6f3713ec1f75e6d2e899ff1ed435fd6b5dd74ebb06c5f23c8dfa6e8909c5f5aa6d62d066ea55f959c783b6dc6aa5ff4e30b835059d95fe725df62884a1160a794af02a3f9f46083bef70a8fe343612fbd5dbe06808561b9b4f87d77dbea61cc85644e3cc00d62ea1d858111d66c62d0c148324be14c6381bcd9d5355451638060af755a05fa6318d8a7d303ae2364b864bb570ca0b564dc007ee7cd94fe640e453c650a933065c1f2887877be3565317109a073524e65230c6f45c1828065c29b1e9f5e96b165ef0bd2ab19d9ff653063c70f8fadda6686af8228a1b3596752bcd9c978d3d167d11ec3f934fe9868862fbdbcc5e7ce68da88415b93b0336917f20579764c576926d05ad3042da5697c57cfbfa5f0fb697e48dbb6121f0a6a1d833c15fe82696a31555359516cbd6a6a64b8b46b34f66aec0d2550a54f0d6c0f3cc1132d5e7f6c8a269cb6201f856c47127fb19b9eab6c2a192e4ed2fbbc6cf8e4dbf40a7c596d1414c24506815a6d21a2ecd7d7d6676d1b15593c3f4c7b6d0c59d2ae0be5ab6dc5379d03e3ebec6df3e7ca478cc7576e5a2348680ebbb16ed6c3171dd452e26e19c7cb1ef8f31e6fdc9ffcb1d447286fd5c442db7a6f4f6fcc7b3f3b3d65ad6f4e05b6074a25ca6f9f6c2fa5eec5cc6f409cdc93e71e9670277cf1027bcd97708a63c4a3a405a470e7ed98e8932eca709a904f5da74ae37001115ad2bf354271a174f9a667296c7149aa98fc658c9571cf16b50168520e72ee4fbd3743c2ef729faf497954d22c73260b6d5b6ab6b67300652ce6441bc7737cfc78b4303f017479dbd3db296db3746cc94d305b7c40757cd8c5dc2eba4775a91ec062efb2b775c37bded8f57cf375bb77e85df4c70f76ebe7785b0dfe1d76063cf13e11801e7659a4550d7680247672b0f83fcbe75a7602c4a9d783fb76765156bffe99e38776fa9eef334e0dc676fef43e5903fae3766b020ca2986433783dc5d4a4810982780415b908a2cd84786cc2bd19a8e31b79ec5aa8ff3b6d32794abf506aefa7b4793302ae1d035aea79086f5e288186ea79226183f1a734ed799e964647cdd4837a4717162d214df17aac3fead8311a0a7b9dde5e609a90177b72153d5fe039617b148f71652fb2717b42c5d575dae8997b9ded1b30aea8b27b137ccbe37836327c54fa2146aaae8b7c975e4355df22977cb0ccd3ef8a94c17ce0f48bea9983c37c692966c7e63f237de73d418715b1917da8df80aa86d6b97d65d4a05bb03dc07dfd8437c6ab16e77d";

    // ---- Exact-mode equivalence pins: Alpha == QuickSelect on the same low-cardinality input ----

    #[test]
    fn test_alpha_exact_mode_bytes_equal_quickselect_bytes() {
        // Risk (FAMILY DISCRIMINATION baseline): in exact mode the two families MUST be byte-identical.
        // Pin Alpha against the Java-generated EXACT10 bytes AND against the live QuickSelect sketch.
        let mut alpha = AlphaSketch::new();
        let mut quickselect = ThetaSketch::new();
        for value in 0..10u64 {
            alpha.update_u64(value);
            quickselect.update_u64(value);
        }
        let alpha_bytes = alpha.serialize_compact().unwrap();
        assert_eq!(
            alpha_bytes,
            hex_to_bytes(ALPHA_EXACT10_HEX),
            "Alpha exact-mode bytes diverge from Java"
        );
        assert_eq!(
            alpha_bytes,
            quickselect.serialize_compact().unwrap(),
            "exact-mode Alpha and QuickSelect must serialize identically"
        );
        assert_eq!(alpha.theta_long(), MAX_THETA);
        assert_eq!(alpha.retained_entries(), 10);
    }

    #[test]
    fn test_alpha_single_and_empty_bytes_equal_quickselect_bytes() {
        // Risk: the SINGLEITEM + EMPTY special preambles must be byte-identical across families.
        let empty = AlphaSketch::new();
        assert!(empty.is_empty());
        assert_eq!(
            empty.serialize_compact().unwrap(),
            hex_to_bytes(ALPHA_EMPTY_HEX),
            "Alpha empty bytes diverge from Java"
        );

        let mut single = AlphaSketch::new();
        single.update_u64(1);
        assert_eq!(single.retained_entries(), 1);
        let single_bytes = single.serialize_compact().unwrap();
        assert_eq!(
            single_bytes,
            hex_to_bytes(ALPHA_SINGLE_HEX),
            "Alpha single-item bytes diverge from Java"
        );
        let mut qs_single = ThetaSketch::new();
        qs_single.update_u64(1);
        assert_eq!(single_bytes, qs_single.serialize_compact().unwrap());
    }

    // ---- Estimation-mode Alpha fixtures (Java-generated) ----

    #[test]
    fn test_alpha_estimation_mode_lgk9_520_matches_java_bytes_and_estimate() {
        // Risk (the HARDEST path): estimation mode reproduces the Alpha cache + theta decay to the bit.
        // Byte-exact vs datasketches-java-3.3.0 (lgK 9, 520 distinct). The compact estimate (the value
        // Iceberg's ndv reads) is pinned bit-exact.
        let mut sketch = AlphaSketch::with_lg_nominal_longs(9, DEFAULT_UPDATE_SEED);
        for value in 0..520u64 {
            sketch.update_u64(value);
        }
        assert_eq!(sketch.retained_entries(), 514);
        assert_eq!(sketch.theta_long(), 9080515283922012160);
        let bytes = sketch.serialize_compact().unwrap();
        assert_eq!(
            bytes,
            hex_to_bytes(ALPHA_EST_LGK9_520_HEX),
            "Alpha lgK=9/520 estimation bytes diverge from Java"
        );
        assert_eq!(bytes.len(), 4136);
        // The COMPACT estimate (Iceberg's ndv source) == Java CompactSketch.getEstimate, bit-exact.
        let compact = sketch.compact().unwrap();
        assert_eq!(compact.estimate().to_bits(), 0x4080_50b0_e0b6_6225u64);
        assert_eq!(compact.estimate(), 522.0863661049558);
        assert_eq!(compact.estimate() as i64, 522);
    }

    #[test]
    fn test_alpha_estimation_mode_n7000_ndv_is_6963_via_compact() {
        // HEADLINE PIN (the prompt's 6963): lgK 12 default, 7000 distinct. The Iceberg ndv reads the
        // COMPACT sketch's estimate. Java probe: compact retained=4090, theta=5417200111458890752,
        // compact getEstimate=6963.669581070137 → ndv 6963. (The UPDATE sampling estimate is 6973 — a
        // DIFFERENT object; the action must NOT use it.)
        let mut sketch = AlphaSketch::new();
        for value in 0..7000u64 {
            sketch.update_u64(value);
        }
        assert_eq!(sketch.retained_entries(), 4090);
        assert_eq!(sketch.theta_long(), 5417200111458890752);
        let compact = sketch.compact().unwrap();
        assert_eq!(compact.retained_entries(), 4090);
        assert_eq!(compact.estimate(), 6963.669581070137);
        assert_eq!(
            compact.estimate() as i64,
            6963,
            "Iceberg ndv (compact estimate)"
        );
        // The update-form sampling estimate is the DIFFERENT value 6973 — pinned to make the
        // object-selection load-bearing (the action reads the compact one).
        assert_eq!(
            sketch.estimate() as i64,
            6973,
            "Alpha UPDATE sampling estimate (NOT the ndv)"
        );
    }

    #[test]
    fn test_alpha_estimation_mode_n1m_ndv_is_1004032_via_compact() {
        // HEADLINE PIN (the prompt's 1004032): lgK 12 default, 1_000_000 distinct. Java probe: compact
        // retained=4103, theta=37691512080307240, compact getEstimate=1004032.2974197501 → ndv 1004032.
        let mut sketch = AlphaSketch::new();
        for value in 0..1_000_000u64 {
            sketch.update_u64(value);
        }
        assert_eq!(sketch.retained_entries(), 4103);
        assert_eq!(sketch.theta_long(), 37691512080307240);
        let compact = sketch.compact().unwrap();
        assert_eq!(compact.retained_entries(), 4103);
        assert_eq!(compact.estimate(), 1004032.2974197501);
        assert_eq!(
            compact.estimate() as i64,
            1004032,
            "Iceberg ndv (compact estimate) for n=1M"
        );
        // Update-form sampling estimate (a DIFFERENT object) is 1002319 — not the ndv.
        assert_eq!(
            sketch.estimate() as i64,
            1002319,
            "Alpha UPDATE sampling estimate (NOT the ndv)"
        );
    }

    #[test]
    fn test_alpha_estimation_mode_diverges_from_quickselect_n1m() {
        // FAMILY DISCRIMINATION: at n=1M the Alpha and QuickSelect ndv DIFFER (Alpha 1004032 vs
        // QuickSelect 1002714). This is the gap Y3 closes. Pin both compact estimates.
        let mut alpha = AlphaSketch::new();
        let mut quickselect = ThetaSketch::new();
        for value in 0..1_000_000u64 {
            alpha.update_u64(value);
            quickselect.update_u64(value);
        }
        let alpha_ndv = alpha.compact().unwrap().estimate() as i64;
        let quickselect_ndv = quickselect.estimate() as i64;
        assert_eq!(alpha_ndv, 1004032, "Alpha ndv");
        assert_eq!(quickselect_ndv, 1002714, "QuickSelect ndv");
        assert_ne!(
            alpha_ndv, quickselect_ndv,
            "the two families MUST diverge in estimation mode (this is the gap Y3 closes)"
        );
        // And the on-disk bytes differ too (different retained set + theta).
        assert_ne!(
            alpha.serialize_compact().unwrap(),
            quickselect.serialize_compact().unwrap(),
            "estimation-mode Alpha and QuickSelect bytes must differ"
        );
    }

    #[test]
    fn test_alpha_deep_resize_lgk9_50k_matches_java() {
        // Risk: a deep estimation run (lgK 9, 50_000 distinct of a seed-independent value set) exercises
        // repeated dirty rebuilds. Java probe: retained=536, theta=99944646323968464,
        // compact getEstimate=49464.654622211296 → ndv 49464.
        let mut sketch = AlphaSketch::with_lg_nominal_longs(9, DEFAULT_UPDATE_SEED);
        for index in 0..50_000u64 {
            sketch.update_u64(1_000_000 + index * 7);
        }
        assert_eq!(sketch.retained_entries(), 536);
        assert_eq!(sketch.theta_long(), 99944646323968464);
        let compact = sketch.compact().unwrap();
        assert_eq!(compact.estimate(), 49464.654622211296);
        assert_eq!(compact.estimate() as i64, 49464);
    }

    // ---- Update-path unit pins (bytecode-cited constants) ----

    #[test]
    fn test_alpha_construction_constants_match_bytecode() {
        // Risk: a wrong alpha / split1 / initial theta silently diverges every estimation-mode sketch.
        // Pinned against `HeapAlphaSketch.newHeapInstance` + the Java probe.
        // lgK 12 (Iceberg default): alpha=0.9997559189650964, split1=9222246411758747648, theta0=MAX.
        let lgk12 = AlphaSketch::with_lg_nominal_longs(12, DEFAULT_UPDATE_SEED);
        assert_eq!(lgk12.alpha.to_bits(), 0.9997559189650964f64.to_bits());
        assert_eq!(lgk12.split1, 9222246411758747648);
        assert_eq!(
            lgk12.theta, MAX_THETA,
            "theta0 = (long)(1.0 * 2^63) saturates to Long.MAX"
        );
        // lgArr = startingSubMultiple(13, 3, 5) == 7; threshold = floor(0.5 * 2^7) == 64
        // (lgArr 7 <= lgNom 12 ⇒ the 0.5 fraction, NOT 0.9375 — setHashTableThreshold).
        assert_eq!(lgk12.lg_arr_longs, 7);
        assert_eq!(lgk12.hash_table_threshold, 64);
        // lgK 9 (Alpha minimum): alpha=0.9980506822612085, split1=9214382395493318656.
        let lgk9 = AlphaSketch::with_lg_nominal_longs(9, DEFAULT_UPDATE_SEED);
        assert_eq!(lgk9.alpha.to_bits(), 0.9980506822612085f64.to_bits());
        assert_eq!(lgk9.split1, 9214382395493318656);
        // lgArr = startingSubMultiple(10, 3, 5) == (10-5)%3+5 == 7; threshold = floor(0.5 * 2^7) == 64
        // (lgArr 7 <= lgNom 9 ⇒ the 0.5 fraction).
        assert_eq!(lgk9.lg_arr_longs, 7);
        assert_eq!(lgk9.hash_table_threshold, 64);
    }

    #[test]
    fn test_alpha_lgk_clamped_to_minimum_9() {
        // Risk: Alpha throws below lgK 9 ("minimum nominal entries of 512"); we clamp rather than panic.
        let clamped = AlphaSketch::with_lg_nominal_longs(4, DEFAULT_UPDATE_SEED);
        assert_eq!(clamped.lg_nominal_longs, 9);
    }

    #[test]
    fn test_alpha_theta_stays_max_until_nominal_exceeded() {
        // Risk: theta must NOT decay while the count is at or below nominal (2^lgK). Pin the boundary:
        // at lgK 9, nominal = 512. After 512 distinct inserts theta is still MAX; the 513th drops it.
        let mut sketch = AlphaSketch::with_lg_nominal_longs(9, DEFAULT_UPDATE_SEED);
        for value in 0..512u64 {
            sketch.update_u64(value);
        }
        assert_eq!(
            sketch.theta_long(),
            MAX_THETA,
            "theta must stay MAX through nominal inserts"
        );
        assert!(!sketch.is_dirty, "no decay yet ⇒ still clean");
        sketch.update_u64(512);
        assert!(
            sketch.theta_long() < MAX_THETA,
            "the (nominal+1)-th insert decays theta"
        );
        assert!(sketch.is_dirty, "theta decay sets dirty");
    }

    #[test]
    fn test_alpha_first_decay_crosses_split1_at_lgk9() {
        // Risk: the split1 boundary governs which estimator the UPDATE sketch uses. At lgK 9 the FIRST
        // decay (theta0 * alpha) already lands at-or-below split1 (Java probe: afterInsert=513,
        // theta=9205392754131861504 <= split1=9214382395493318656). Pin that the very first decay
        // crosses split1, so subsequent inserts take the sampling/rebuild branch.
        let mut sketch = AlphaSketch::with_lg_nominal_longs(9, DEFAULT_UPDATE_SEED);
        for value in 0..513u64 {
            sketch.update_u64(value);
        }
        assert_eq!(sketch.theta_long(), 9205392754131861504);
        assert!(
            sketch.theta_long() <= sketch.split1,
            "the first decay lands at/below split1 at lgK 9"
        );
    }

    #[test]
    fn test_alpha_duplicates_do_not_increase_retained() {
        // Risk: NDV counts DISTINCT values; a duplicate must not change the retained set or theta.
        let mut sketch = AlphaSketch::new();
        for _ in 0..1000 {
            sketch.update_u64(42);
        }
        assert_eq!(sketch.retained_entries(), 1);
        assert_eq!(sketch.theta_long(), MAX_THETA);
        assert_eq!(sketch.compact().unwrap().estimate(), 1.0);
    }

    #[test]
    fn test_alpha_update_bytes_and_update_u64_agree() {
        // Risk: the byte and long update entry points must hash identically for the same logical value.
        let mut by_long = AlphaSketch::new();
        by_long.update_u64(7);
        let mut by_bytes = AlphaSketch::new();
        by_bytes.update_bytes(&7u64.to_le_bytes());
        assert_eq!(
            by_long.serialize_compact().unwrap(),
            by_bytes.serialize_compact().unwrap()
        );
    }

    #[test]
    fn test_alpha_empty_bytes_update_is_ignored() {
        // Risk: an empty string value must not panic and must not retain anything.
        let mut sketch = AlphaSketch::new();
        sketch.update_bytes(&[]);
        assert!(sketch.is_empty());
        assert_eq!(sketch.retained_entries(), 0);
    }

    #[test]
    fn test_alpha_compact_round_trips_through_canonical_reader() {
        // Risk: compact() must produce a CompactThetaSketch readable by the SAME parser that reads a
        // Java blob — and the round-trip preserves retained/theta/estimate.
        let mut sketch = AlphaSketch::new();
        for value in 0..50u64 {
            sketch.update_u64(value);
        }
        let compact = sketch.compact().unwrap();
        let bytes = sketch.serialize_compact().unwrap();
        let reparsed =
            CompactThetaSketch::deserialize_with_seed(&bytes, DEFAULT_UPDATE_SEED).unwrap();
        assert_eq!(compact, reparsed);
        assert_eq!(compact.retained_entries(), 50);
        assert_eq!(compact.estimate(), 50.0);
    }
}
