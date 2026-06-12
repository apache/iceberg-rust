<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# map.md — crates/sketches/src/

## Purpose

`iceberg-sketches` — a dependency-free, byte-exact port of the Apache DataSketches **theta**
`CompactSketch` serialized format, the payload of Iceberg's `apache-datasketches-theta-v1` Puffin
blob (per-column NDV statistics). The contract is cross-engine byte compatibility: blobs this crate
writes are readable by Java DataSketches (Spark/Trino/Flink), and theirs are readable here. The hash
and the build/serialize/deserialize paths are ported one-to-one from the DataSketches 3.3.0 jar
bytecode and pinned against Java-generated fixtures.

**Two update families.** `theta.rs` ports `HeapQuickSelectSketch` (the builder default); `alpha.rs`
ports `HeapAlphaSketch` — **the family Iceberg's NDV pipeline actually builds** (`ThetaSketchAgg` →
`setFamily(Family.ALPHA)`). Exact mode: byte-identical. Estimation mode: they diverge (different
retained set / theta / bytes / NDV). Both serialize to the SAME family-COMPACT form (one path).
`ComputeTableStats` (in the `iceberg` crate) builds an `AlphaSketch` and reads the `ndv` off the
COMPACT sketch's estimate (the standard estimator, NOT the Alpha update sampling estimator).

## Contents

| File | What it does |
|---|---|
| `lib.rs` | crate doc + re-exports (`ThetaSketch`, `CompactThetaSketch`, `hash::*`, `SketchError`) |
| `hash.rs` | DataSketches MurmurHash3 (128-bit x64, long-block + byte-tail variants) ported from `MurmurHash3$HashState` bytecode; `DEFAULT_UPDATE_SEED=9001`; `compute_seed_hash` |
| `theta.rs` | `ThetaSketch` (update form = `HeapQuickSelectSketch`: hash table + quickselect theta-lowering) + `CompactThetaSketch` (deserialize) + the compact byte serializer (empty/single/exact/estimation) + the `pub(crate)` shared helpers (`serialize_compact_from_parts`, `estimate`, `hash_search_or_insert`, `hash_array_insert`, `get_stride`, `set_hash_table_threshold`, `starting_sub_multiple`) |
| `alpha.rs` | `AlphaSketch` (update form = `HeapAlphaSketch`: hash table + fixed `alpha = nominal/(nominal+1)` theta-decay + dirty/clean insert + resize/rebuild). Reuses theta.rs's serializer + helpers (no fork). The family Iceberg writes |
| `error.rs` | `SketchError` (no `thiserror` dep — the crate is dependency-free): hash/seed/truncation/version/family/seed-hash/not-compact errors with operator-actionable messages |
| `testdata/README.md` | fixture provenance (Java-generated, datasketches-java-3.3.0) + reproduce steps |
| `testdata/theta_fixture_generator.java` | the standalone Java oracle that emits the pinned bytes + hash vectors |

## I want to...

| I want to... | go to |
|---|---|
| Build the NDV sketch Iceberg writes (Alpha) | `AlphaSketch::new()` + `update_u64` / `update_bytes` in [alpha.rs](alpha.rs) |
| Build a QuickSelect NDV sketch | `ThetaSketch::new()` + `update_u64` / `update_bytes` in [theta.rs](theta.rs) |
| Serialize to the `apache-datasketches-theta-v1` blob payload | `AlphaSketch::serialize_compact()` / `ThetaSketch::serialize_compact()` |
| Read a Java-written theta blob | `CompactThetaSketch::deserialize` / `deserialize_with_seed` in [theta.rs](theta.rs) |
| Compute the Iceberg `ndv` | `AlphaSketch::compact()`.`estimate()` (the COMPACT estimate — what `NDVSketchUtil` reads), NOT `AlphaSketch::estimate()` (the update sampling form) |
| Estimate NDV (QuickSelect) | `ThetaSketch::estimate()` / `CompactThetaSketch::estimate()` in [theta.rs](theta.rs) |
| Hash a value the DataSketches way | `hash::hash_long` / `hash_longs` / `hash_bytes` (seed 9001) in [hash.rs](hash.rs) |
| Regenerate the fixtures | follow [testdata/README.md](../testdata/README.md) |

## Pointers

- **Up:** [crates/](../../) — a workspace member (`crates/sketches` in the root `Cargo.toml`).
- **Related:** `crates/iceberg/src/puffin/blob.rs` (the `APACHE_DATASKETCHES_THETA_V1` blob-type
  constant), `crates/iceberg/src/transaction/update_statistics.rs` (the existing
  `UpdateStatisticsAction` Y2 will register the blob through);
  `docs/parity/GAP_MATRIX.md` "ComputeTableStats" row.

## Debug

### Known failure modes

| Symptom | Likely cause |
|---|---|
| A serialized blob diverges from Java bytes | Re-derive against the bytecode: preamble bytes (0 preLongs\|lgResizeFactor, 1 serial-version=3, 2 family=3, 3/4 zeroed for compact, 5 flags, 6-7 seedHash LE, 8-11 count, 12-15 P=1.0f, 16-23 theta); flags EMPTY=4/COMPACT=8/ORDERED=16/SINGLEITEM=32/READ_ONLY=2; empty stores NO seed hash; single uses preLongs=1+SINGLEITEM; exact preLongs=2 (theta NOT stored); estimation preLongs=3 |
| Hash differs from Java | This is NOT canonical byte-stream MurmurHash3: DataSketches processes 16-byte blocks as two LE u64s with a 64-bit seed, and the long path passes `len*8` (not `len`) as lengthBytes to finalMix128; the byte path uses the actual byte length. Re-check `MurmurHash3$HashState.blockMix128`/`finalMix128` |
| Estimation-mode retained set / theta differs (QuickSelect) | The quickselect must use the SAME inputs and lgK; theta = the (2^lgK)-th smallest non-zero hash (0-based index 2^lgK); the table starts at `startingSubMultiple(lgK+1, 3, 5)` and grows then quickselects per `hashUpdate`. A wrong initial table size flips resize-vs-quickselect |
| Alpha estimation-mode retained set / theta differs | Alpha (`alpha.rs`) decays `theta *= alpha` (`alpha = nominal/(nominal+1)`) on each insert past nominal, NOT quickselect. Check the clean/dirty split (`is_dirty`), the `enhanced_hash_insert` stale-slot reuse (no count bump), and `rebuild_dirty`/`resize_clean`. lgK must be >= 9 (Alpha min). The COMPACT retained set is the cache entries strictly below the decayed `theta` |
| Alpha `ndv` differs from Java | The `ndv` reads the COMPACT sketch's `estimate()` (standard `retained*2^63/theta`), NOT `AlphaSketch::estimate()` (the update sampling form `nominal*2^63/theta`). These DIFFER in estimation mode (n=1M: compact 1004032 vs update 1002319). `NDVSketchUtil` uses `CompactSketch.wrap(bytes).getEstimate()` |
| A foreign blob is rejected as a seed mismatch | The blob was written with a non-9001 seed; its hashes are incomparable. Iceberg always uses 9001 — a mismatch is a genuine incompatibility, not a bug |
| Empty-input hash panics | It does not — `hash_bytes`/`hash_longs` return `SketchError::EmptyHashInput` (DataSketches' `checkPositive` throws); `update_bytes(&[])` silently no-ops like Java's `update(byte[])` |
| A serial-version 1/2 blob is rejected | DELIBERATE v3-only posture. Java's `heapify` accepts v1/v2 via `ForwardCompatibility.heapify{1,2}to3`; we don't (every Iceberg theta blob is v3). To support legacy blobs, port `ForwardCompatibility` (see the `parse` doc) |
| `estimate()` differs from Java by ~1 ULP | The estimator MUST be `count * (2^63_as_f64 / theta)` (Java `Sketch.estimate(long,int)`), NOT the algebraically-equal `count / (theta / MAX)` — the two round differently. Pinned bit-exact by the `EST*` fixtures |
| A `preLongs==1` non-empty non-single blob panics / decodes wrong | `preLongs==1` carries curCount=0 (Java `memoryToCompact`), UNLESS `flags & 31 == 26` (the legacy single-item encoding without the SINGLEITEM bit, `otherCheckForSingleItem`) → 1 hash at byte 8. Do not read bytes 8-11 for `preLongs==1` |

### First checks

1. `cargo test -p iceberg-sketches` — the per-mode fixture tests localize which layout/mode drifted.
2. Regenerate the Java oracle bytes (testdata/README.md) and diff the hex against the `*_HEX`
   constants in [theta.rs](theta.rs) / the vectors in [hash.rs](hash.rs). The jar is the pin.
3. For the hash specifically, decompile `org.apache.datasketches.hash.MurmurHash3` +
   `MurmurHash3$HashState` from `~/.m2/.../datasketches-java-3.3.0.jar` — bytecode is truth.

### Escalate to

- [dev/java-interop/map.md](../../../dev/java-interop/map.md#debug) for oracle-driven byte comparisons.
- [docs/parity/GAP_MATRIX.md](../../../docs/parity/GAP_MATRIX.md) "ComputeTableStats" row for scope.
