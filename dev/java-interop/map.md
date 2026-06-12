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
| `run-interop-write-data.sh` | S1 data-level write-actions interop (increment S1): TWO FIXTURES, 6 steps, REAL parquet, both directions. Fixture A (merge_append): V2 partitioned table (`identity(category)`), Java+Rust write fast_append A+B → set min-count=2 → merge_append G; Java `IcebergGenerics` reads the Rust table (step 4), Rust reads the Java table (step 6). Fixture B (rewrite_files, seq-preservation): unpartitioned 2-field table, Java+Rust write fast_append A → equality-delete(ids 20+40, seq 2) → rewrite {A}→{A'} with `data_sequence_number(1)` — proves A'.data_seq=1 < eq_del.seq=2 so ids 20/40 remain deleted after the rewrite. Separate from `run-interop-write-actions.sh` (metadata-level only). Consumer: `tests/interop_write_data.rs`. |
| `run-interop-cherrypick.sh` | S2 cherrypick metadata-level interop: THREE FIXTURES (ff / replay / dedup), 6 steps, BOTH DIRECTIONS. D1 (Rust acts, Java judges): Java generates each fixture (stage + cherrypick), Rust performs the same chain, Java byte-diffs its view of the Rust-produced table against its own view (`emit-snapshot-meta` + `diff`) and `verify-interop-cherrypick` confirms per-fixture facts (FF: 2 snapshots; replay/dedup: 4 + `source-snapshot-id`; dedup: second cherrypick raises `CherrypickAncestorCommitException`). D2 (Java acts, Rust verifies): Rust's `snapshot_meta_view` of each Java-produced table equals Java's view. Consumer: `tests/interop_cherrypick.rs`. |
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
