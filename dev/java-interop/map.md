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

# map.md — dev/java-interop/

## Purpose

**The interop oracle — the project's objective verifier.** A TEST-ONLY Maven module driving Java
`iceberg-core` 1.10.0 to prove bidirectional compatibility of Rust capabilities (Direction 1: Rust
reproduces Java's result offline from committed fixtures; Direction 2: Java reads what Rust writes).
A GAP_MATRIX row flips ✅ only through this harness. Not a Cargo crate; `cargo test` never invokes
Java. **Read [README.md](README.md) — it is the authoritative doc for scenarios, modes, and the
generate/verify flow; this map only routes.**

## Contents

| File | What it does |
|---|---|
| `README.md` | the full contract: directions, scenarios, comparison semantics (structural, not byte-for-byte) |
| `pom.xml` | Maven module, `package org.apache.iceberg` (reaches package-private ctors like `@VisibleForTesting SchemaUpdate(Schema,int)`) |
| `src/.../InteropOracle.java` | all oracles in one program: schema / partition / manage-snapshots `generate` + `verify`, scan-exec data writers, inspection expectations |
| `run.sh` | metadata-evolution oracle pass (generate fixtures + verify Rust output) |
| `run-interop-scan-exec.sh` / `-d2.sh` | data-level scan-execution interop (Java writes parquet + position delete; D2: Java reads Rust) |
| `run-interop-eq-delete.sh` / `-d2.sh` | equality-delete scan-exec interop, both directions |
| `run-interop-part.sh` / `-d2.sh` | partitioned merge-on-read scan-exec interop, both directions |
| `run-interop-dv.sh` | deletion-vector interop, BOTH directions + table + metadata + REPLACEMENT level (16 steps). D1 (scan): Java writes a V3 table + a real `BaseDVFileWriter` DV → Rust scans it; also a synthetic high-bits/run-container blob for the byte-level decode pin. D2 (write, blob-level): Rust `DVFileWriter` writes a Puffin DV file → Java's production reader (`Puffin.read` + `PositionDeleteIndex.deserialize`) verifies it (`verify-interop-dv-write`, sentinel-grepped) AND emits its own blobs for the Rust byte-parity pin (`tests/interop_dv_write.rs`). D4 (table level, the headline): Rust COMMITS a complete V3 table (2 partitions, one puffin with two DVs via `fast_append`+`row_delta`) → Java's PRODUCTION scan reads it (`verify-interop-dv-table`, sentinel-grepped) + manifest-API DeleteFile cross-check. D4 (metadata level): both sides run the same DV chain (`generate-interop-dv-table` mirrors the Rust GEN) and the canonical snapshot-metadata views are compared 3 ways (`emit-snapshot-meta` + byte-diff + `tests/interop_dv_table.rs`). **REPLACEMENT (Arc-E Inc 2, steps 12-16):** Rust commits the writer-merged DV replacement chain (`fast_append` + DV1 + add merged DV2 + remove DV1, via `DVFileWriter::with_previous_deletes`) → Java reads it (`verify-interop-dv-replace`: merged DV applied, DV1 ABSENT from manifests) + the merged-blob byte-compare (the Run-store tie pin) + the metadata 3-way (`generate-interop-dv-replace`/`DvReplaceOracle` — the FIRST LIVE `removed-dvs` comparison); `tests/interop_dv_replace.rs` |
| `run-interop-rowdelta-meta.sh` | E1 metadata-level row-delta interop: canonical snapshot-metadata view over the 3 scan-exec fixtures, 3 comparison directions each |
| `run-interop-write-actions.sh` | E2 + Increment-4 metadata-level write-actions interop: ONE eight-step chain (`WriteActionsOracle`: delete/overwrite/replace-partitions/rewrite/rewrite-manifests/merge-append) + a delete-bearing seq-preserving `rewrite_files` fixture B (`RewriteSeqOracle`), each judged 3 ways via `SnapshotMetaOracle` |
| `run-interop-expire.sh` | A3 ExpireSnapshots interop (B1 retention + B2 `ReachableFileCleanup` file GC) vs Java `expireSnapshots().cleanExpiredFiles(true).deleteWith(collector)`, 5 fixtures (`ExpireOracle`: linear/tag_protected/stats/deletes/rewrite), BOTH directions: D1 byte-equal canonical views (`SnapshotMetaOracle`) + Java-`verify`'d surviving snapshots/refs + deleted set; D2 Rust view == Java + Rust cleanup deleted set == Java `deleteWith` set. Java is FORCED down `ReachableFileCleanup` (the only ported strategy) via a surviving tag (`hasNonMainSnapshots(current)`). Deleted sets compared as path-independent `<funnel>@ord<N>` descriptors (Java/Rust write at different paths). Consumer: `tests/interop_expire.rs` |
| `run-interop-write-data.sh` | S1+W1+W2+W3 data-level write-actions interop: SEVEN FIXTURES, 22 steps, REAL parquet, both directions + 2nd-pass repeat + sabotage battery + S3-class mutation pin. Fixture A (merge_append, S1): V2 partitioned table (`identity(category)`), fast_append A+B → set min-count=2 → merge_append G; live rows `{10,20,30,40,60}`. Fixture B (rewrite_files, S1, seq-preservation): unpartitioned 2-field table, fast_append A → equality-delete(ids 20+40, seq 2) → rewrite {A}→{A'} with `data_sequence_number(1)` — proves A'.data_seq=1 < eq_del.seq=2; live rows `{10,30,50}`. Fixture C (overwrite_files, W1): V2 partitioned table, fast_append A+B → overwrite_files DELETE B ADD B'(cat=b,id=41,d'); live rows `{10,20,30,41}` (id=40 absent). Fixture D (delete_files, W1): V2 partitioned table, fast_append A+B+C_file → delete_files {B}; live rows `{10,20,30,50}` (id=40 absent). Fixture E (replace_partitions, W2): V2 partitioned table, fast_append A(cat=a,10/20/30)+B(cat=b,40) → replace_partitions E_new(cat=a,id=11,data="a'") — ALL of partition a replaced; B byte-untouched (EXISTING manifest status); live rows `{(11,a'),(40,d)}`; extra assertion: B's file PATH in EXISTING status in live manifests. Fixture F (partitioned rewrite_files, W2): V2 partitioned table, fast_append A(cat=a,10/20/30)+B(cat=b,40) → row_delta eq-delete(cat=a, id=20, seq 2) → rewrite {A}→{A'} `data_sequence_number(1)` — proves partitioned seq-preservation (A'.data_seq=1 < eq_del.seq=2); B untouched; live rows `{(10,a),(30,c),(40,d)}`. **Fixture G (multi-bin merge_append, W3):** V2 partitioned table, 4 fast_appends (A+B+C1+C2) → dynamic `target-size-bytes = max_manifest_len*2+1` + `min-count-to-merge=2` → merge_append G(cat=a,id=60); `pack_end` bins [new],[e1,e2],[e3,e4]; both size-2 bins merge → ≥2 merged manifests (bin-count assert); live 7 rows a-rows={10,20,30,50,60}, b-rows={40,55}. Per fixture: Java writes+emits ground truth (step 2), Rust writes+Java verifies (steps 3-10), Rust reads Java table (step 11). Steps 12-14: 2nd pass (determinism). Step 15: sabotage battery (metadata corruption → verify must fail; covers all 5 W-fixtures C+D+E+F+G, truncate+bogus-path each). Step 16: S3-class mutation — feeds fixture E's verify a genuinely WRONG table and asserts FAILS closed with partition-column pin (3e). Partition column pinned in ALL fixtures (S3 partition-projection lesson). Separate from `run-interop-write-actions.sh` (metadata-level only). Consumer: `tests/interop_write_data.rs`. |
| `run-interop-cherrypick.sh` | S2 cherrypick metadata-level interop: THREE FIXTURES (ff / replay / dedup), 6 steps, BOTH DIRECTIONS. D1 (Rust acts, Java judges): Java generates each fixture (stage + cherrypick), Rust performs the same chain, Java byte-diffs its view of the Rust-produced table against its own view (`emit-snapshot-meta` + `diff`) and `verify-interop-cherrypick` confirms per-fixture facts (FF: 2 snapshots; replay/dedup: 4 + `source-snapshot-id`; dedup: second cherrypick raises `CherrypickAncestorCommitException`). D2 (Java acts, Rust verifies): Rust's `snapshot_meta_view` of each Java-produced table equals Java's view. Consumer: `tests/interop_cherrypick.rs`. |
| `run-interop-staged-wap.sh` | Z1 staged-WAP interop: THREE FIXTURES (S-ff / S-replay / S-dedup), 7 steps, BOTH DIRECTIONS, TWO VIEWS PER FIXTURE. BOTH sides use REAL staging APIs (`stageOnly()` / `stage_only()`). Staged-state view = new coverage (canonical view of a table with a ref-less staged snapshot; `current-snapshot-id` unchanged, staged snapshot in `metadata.snapshots()`). D1 (Rust acts, Java judges): Rust performs the same chain via real `stage_only()` + `cherry_pick`, landing `rust_staged_table` + `rust_final_table`; Java `SnapshotMetaOracle` byte-diffs BOTH views and `verify-interop-staged-wap` asserts per-fixture facts (S-ff: FF, no new snapshot; S-replay: `published-wap-id=w2`; S-dedup: second cherry-pick rejected as `DuplicateWAPCommitException` / `DataInvalid`, table unchanged). D2 (Java acts, Rust verifies): Rust view == Java view for both staged + final. Sabotage battery: 4 corruptions all closed (7a/7b/7c/7d — including staged-snapshot-removal pin). Consumer: `tests/interop_staged_wap.rs`. |
| `run-interop-multi-spec.sh` | Z2 multi-spec interop: SINGLE FIXTURE, 5 steps + 4-sabotage battery, BOTH DIRECTIONS. Chain: ms1 fast_append F1(spec0) → ms2 spec-evolve add identity(b) → ms3 fast_append F2(spec1) → ms4 ONE multi-spec fast_append (F0 spec0 + F3 spec1) — TWO manifests with DIFFERENT `partition_spec_id` values (0 and 1) in ONE snapshot. TIE-SHAPING: F0 and F3 both have `record_count=10`, so the two ms4 manifests tie on ALL 9 prior sort-tuple keys; `partition_spec_id` is the SOLE disambiguator (W3 tiebreaker). D1 (Rust writes, Java judges): Java `emit-snapshot-meta` byte-diffs Rust-chain view vs `java_meta.json`. D2: Rust's canonical view of BOTH chains equals `java_meta.json`. Sabotage: SB1 truncated manifest fails closed; SB2 dropped-ms4 snapshot leaves 2 ordinals vs 3; SB3 control clean-chain passes; SB4 wrong-spec rendering (swap the spec-0/spec-1 field DEFINITIONS in the SOURCE metadata, then RE-EMIT) re-derives a divergent view because each ms4 manifest's partition tuple now renders under the wrong-arity spec (proves partition tuples are rendered under each manifest's OWN spec — the file's-own-spec rule — and that rendering is load-bearing, not just the `partition_spec_id` integer). Consumer: `tests/interop_multi_spec.rs`. |
| `run-interop-partition-stats.sh` | Z3 partition-stats file interop: SINGLE FIXTURE, 8 steps + 4-sabotage battery, BOTH DIRECTIONS + cross-version projection. Fixture: V2 table `identity(category)` {id long, category string, data string optional}, S1 fast-append (cat=a 3rec 300B + cat=b 2rec 200B), S2 row-delta pos-delete (cat=a 1rec 50B). D1 (Rust writes, Java judges): Rust `compute_and_write_stats_file` + `register_partition_stats_file` → Java `PartitionStatsHandler.readPartitionStatsFile` decodes the stats parquet → compares against `expected_stats.json` (hand-declared: cat=a data_records=3 pos_del_records=1 last_updated=S2, cat=b data_records=2 last_updated=S1). D2 (Java writes, Rust reads): Java `computeAndWriteStatsFile` writes the stats parquet → Rust `read_partition_stats_file` decodes it → compares against `java_stats.json`. Cross-version: Java-written V2 stats parquet (12 cols, no `dv_count`) read against V3 schema → `project_struct_type_to_batch` null-fills `dv_count` to 0. Sabotage: 7a truncate Rust parquet → D1 fails; 7b corrupt counter cell via SOURCE byte edit + re-read (Z2 lesson) → D1 fails; 7c truncate Java parquet → D2 fails; 7d remove partition-statistics from Rust metadata → D1 fails. Consumer: `tests/interop_partition_stats.rs`. |
| `run-interop-view.sh` | I2 view metadata interop: BOTH directions + 5-sabotage battery. D1 (Rust `MemoryCatalog`+`ReplaceViewVersionAction` writes, Java `ViewMetadataParser.fromJson` judges); D2 (Java `InMemoryCatalog.buildView` writes, Rust `ViewMetadata::read_from` reads). Tolerance control: Java-ordered JSON (view-uuid first, no properties) + Rust always-emit-properties both tolerated. Consumer: `tests/interop_view.rs`. |
| `run-interop-wap-data.sh` | I3 data-level WAP interop: BOTH directions, REAL parquet, 8-row fixture (base 4 + bump 1 + staged 3), REPLAY shape. D1 (Rust `stage_only()` + DataFileWriter writes, Java `manageSnapshots().cherrypick()` verifies): Rust stages cat=a(50,60)+cat=b(70) via S-replay order (stage before bump), Java cherry-picks and asserts 8 rows + `source-snapshot-id` + `published-wap-id`. D2 (Java `stageOnly()` writes, Rust `CherryPickAction` + `to_arrow()` reads): Rust loads `java_cherrypick_table`, scans 8 rows, pins WAP semantics + partition routing a/b. Sabotage battery: 7a STRUCTURAL truncate metadata; 7b STRUCTURAL bogus manifest-list path; 7c SEMANTIC corrupt `wap.id` w1→w1-CORRUPTED (cherry-pick emits wrong `published-wap-id` → WAP-ID pin fires); 7d STRUCTURAL remove staged snapshot entirely. Consumer: `tests/interop_wap_data.rs`. |
| `run-inspection-manifests.sh` | inspection-table expectation generation |

Durable artifacts are the committed fixtures under `crates/iceberg/testdata/interop/` and the Rust
tests in [crates/iceberg/tests/](../../crates/iceberg/tests/map.md) that read them.

## I want to...

| I want to... | go to |
|---|---|
| Flip a 🟡 row to ✅ | add scenarios on BOTH sides (Rust `apply_scenario_ops` + `InteropOracle` scenarios, identical names), `generate` fixtures, write the Rust test, run the `-d2` verify |
| Regenerate fixtures after a contract change | the matching `run*.sh` (needs `mvn` + JDK; fixtures are committed so plain `cargo test` stays offline) |
| Write Rust output for Java to verify | run the Rust interop test with `ICEBERG_INTEROP_GEN=1`, then the `-d2` script |
| Understand why comparison is structural | README "Comparison" — Jackson vs serde_json key order makes raw bytes meaningless; logical identity incl. field ids is the contract |

## Pointers

- **Up:** `dev/` (sibling: `dev/spark/`, the same oracle pattern for Spark).
- **Related:** [crates/iceberg/tests/map.md](../../crates/iceberg/tests/map.md) (the consuming
  tests); [docs/testing.md](../../docs/testing.md) (the interop requirement);
  [docs/parity/GAP_MATRIX.md](../../docs/parity/GAP_MATRIX.md) (what's proven).

## Debug

### Known failure modes

| Symptom | Likely cause |
|---|---|
| Direction-1 test fails after a Java upgrade | Fixtures encode a specific `iceberg-core` version's behavior — regenerate and diff before assuming a Rust bug |
| Java rejects Rust metadata in `verify` | Real parity divergence — this is the harness working; read the Java parse error, find the Java validation, mirror it |
| Inspection expectations don't match Rust output | Materialize Java expectations from a **re-parsed** base (Java's on-disk round-trip), not Java's in-memory objects — e.g. `operation` is split out of the summary map on disk |
| A sabotage/mutation step passes on an uncorrupted artifact | False-green door: the corruption must land in the SOURCE artifact and be RE-DERIVED through the production reader (never post-edit an emitted view), and the pass test must distinguish the EXPECTED failure from ANY exception — a crash/collision BEFORE the read satisfies an absence-of-sentinel check vacuously. Run a clean-verify CONTROL per failure site. _Promoted 2026-06-12 from lessons (Z2/Z3)._ |
| A data-level fixture passes while a partition/column divergence exists | Projection-completeness gap: the row compare must cover EVERY schema column (partition columns included) on BOTH language sides — a dumper reused from a narrower template inherits its projection, and the Java verify needs its own column assertion (an id-keyed map is blind to uncompared columns). _Promoted 2026-06-12 from lessons (S3/W1)._ |
| `mvn` step fails offline | The oracle needs network for first dependency resolution; committed fixtures keep `cargo test` independent of it |
| Scenario passes one direction only | Scenario op-sequences must be **identical and identically named** on both sides — diff `apply_scenario_ops` vs `InteropOracle.scenarios()` |
| A Java code path silently doesn't run in the oracle | Drive REAL Java objects (`new BaseTable(ops, name).updateSpec()…commit()` over an in-memory `TableOperations`), NOT the `@VisibleForTesting` ctors — those set `base = null` and skip base-dependent paths (e.g. field-id recycling) |
| Interop test passes but proves nothing | Mutation-prove BOTH directions by corrupting fixtures: edit a Java-written field (Dir-1 assertion must fail) and shrink a Rust-written value (Dir-2 `mvn verify` must exit 1). A harness comparing a file to itself passes tautologically |
| A cross-language comparison fails on ordering | CHECK ORDER-INSENSITIVELY FIRST (sort both sides by an explicit total key) before hunting a semantic bug — manifest-list ties within one commit fall back to writer-dependent order; fix canonicalization in BOTH emitters symmetrically. _Promoted 2026-06-11._ |
| A version-ancestry claim sourced from `/tmp/iceberg-java-ref` | The ref is a depth-1 TAGLESS shallow clone — `git log -S` / `merge-base --is-ancestor` answers about version ancestry are ARTIFACTS. Verify version-sensitive behavior against the pinned 1.10.0 jar bytecode (`javap` from `~/.m2`), or run a live oracle probe. _Promoted 2026-06-11._ |
| Noisy fixture diffs after a regen | Java's `newTableMetadata` regenerates `table-uuid`/`last-updated-ms` every run — confirm STRUCTURAL identity, and `git checkout --` fixtures outside your increment's scope |
| `SnapshotMetaOracle.emit` NPE / `snapshot_meta_view` panic on an EXPIRED table | A surviving snapshot's parent was EXPIRED out of the table, so `ordinals.get(parentId)` is null (NPE on `writeNumberField`) / `ordinals[&parent_id]` panics. Emit `null` `parent_ordinal` when the parent has no in-table ordinal. The shared `snapshot_meta_view.rs` lacks this (no prior fixture expires); `interop_expire.rs` carries a local `expire_meta_view` with the fix. _Promoted 2026-06-11 (A3)._ |
| Cross-language DELETED-FILE set can't compare raw paths | Java + Rust write tables at DIFFERENT absolute paths (random manifest/list UUIDs, different temp roots), so a `deleteWith`/`CleanupReport` path set is never directly comparable. Normalize each deleted file to a path-independent `<funnel>@ord<N>` descriptor multiset (funnel = content/manifest/manifest_list/statistics; ordinal = owning snapshot's sequence-number position in the pre-expire metadata) on BOTH sides. _Promoted 2026-06-11 (A3)._ |
| Java picks `IncrementalFileCleanup`, diverging from the Rust port | The Rust side ports ONLY `ReachableFileCleanup`. Java's `RemoveSnapshots.cleanExpiredSnapshots` picks INCREMENTAL when `!specifiedSnapshotId && !hasRemovedNonMainAncestors && !hasNonMainSnapshots(current)` (1.10.0 bytecode). FORCE Java to Reachable by leaving a surviving TAG/branch (`hasNonMainSnapshots` true) — which also doubles as a ref-protection fixture element. _Promoted 2026-06-11 (A3)._ |

### First checks

- Is the failure in fixture generation (Java side) or comparison (Rust side)? Run the Rust test
  alone first — it's offline and fast.
- Check the README's scenario table for what each scenario pins before editing it.

### Escalate to

- Which Rust module owns the divergence → the relevant source map
  ([transaction](../../crates/iceberg/src/transaction/map.md) /
  [inspect](../../crates/iceberg/src/inspect/map.md) / [scan](../../crates/iceberg/src/scan/map.md) /
  [writer](../../crates/iceberg/src/writer/map.md)).
