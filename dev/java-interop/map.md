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
| `run-interop-rowdelta-meta.sh` | E1 metadata-level row-delta interop: canonical snapshot-metadata view over the 3 scan-exec fixtures, 3 comparison directions each |
| `run-interop-write-actions.sh` | E2 + Increment-4 metadata-level write-actions interop: ONE eight-step chain (`WriteActionsOracle`: delete/overwrite/replace-partitions/rewrite/rewrite-manifests/merge-append) + a delete-bearing seq-preserving `rewrite_files` fixture B (`RewriteSeqOracle`), each judged 3 ways via `SnapshotMetaOracle` |
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
| Noisy fixture diffs after a regen | Java's `newTableMetadata` regenerates `table-uuid`/`last-updated-ms` every run — confirm STRUCTURAL identity, and `git checkout --` fixtures outside your increment's scope |

### First checks

- Is the failure in fixture generation (Java side) or comparison (Rust side)? Run the Rust test
  alone first — it's offline and fast.
- Check the README's scenario table for what each scenario pins before editing it.

### Escalate to

- Which Rust module owns the divergence → the relevant source map
  ([transaction](../../crates/iceberg/src/transaction/map.md) /
  [inspect](../../crates/iceberg/src/inspect/map.md) / [scan](../../crates/iceberg/src/scan/map.md) /
  [writer](../../crates/iceberg/src/writer/map.md)).
