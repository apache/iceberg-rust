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

# Parity Roadmap — Rust Iceberg ↔ Java `iceberg-core`/`iceberg-api`

> **Superseded by [`../../Roadmap.md`](../../Roadmap.md)** — the aggregate project-manager roadmap, which
> consolidates this phase plan with current status and a new-session on-ramp. This file is kept for origin
> detail; the live capability checklist is [GAP_MATRIX.md](GAP_MATRIX.md). **Follow `../../Roadmap.md`.**

> **North star.** A **Rust-native** implementation of Apache Iceberg with **1-to-1 capability
> parity with the Java `iceberg-core` / `iceberg-api` library** — the engine-agnostic table-format
> library, *not* the Spark engine integration. We **fork and own** the `iceberg-rust` crates and
> maintain them indefinitely. **Glue + S3 Tables** are the first-priority catalogs. Python/PySpark is
> **deferred** (no PyIceberg, no PySpark layer for now).
>
> Companion doc: [GAP_MATRIX.md](GAP_MATRIX.md) (the living capability audit).

## Locked decisions (2026-06-06)

| Decision | Choice |
|---|---|
| Parity scope | Java **core library** (`iceberg-core`/`iceberg-api`), not the Spark surface |
| Core ownership | **Fork & own** the crates; drop the upstream-mergeability constraint |
| Deliverable | **Rust-native library** only; Python/PySpark deferred |
| Catalog priority | **Glue + S3 Tables first**, then REST, then Hive/JDBC/Nessie |
| Base | **Sync to upstream 0.9.x first**, then own from there |
| Python layers | **Delete** `iceberg-spark-python/`, `iceberg-spark-pyspark/`, **and** `bindings/python/` |

These supersede the obsolete Spark-surface framing in the legacy `PROJECT.md`
(which described an unbuilt project as if complete). `PROJECT.md` and `CLAUDE.md` are to be rewritten
to this north star in Phase 0.

## Working principles

- **Tests land with the code** (same change). Plus **interop tests**: read tables Java wrote, and
  prove Java can read what we write — the only true 1:1 evidence.
- **The Java repo is the spec-by-example.** Keep a reference checkout; re-crawl on each Java release.
- **Re-audit after every upstream sync and every phase** — keep [GAP_MATRIX.md](GAP_MATRIX.md) live.
- Ordered by **dependency then value**: metadata correctness underpins writes; writes underpin
  maintenance actions.

## Phases

### Phase 0 — Repo reset & base sync
- Sync to upstream `iceberg` 0.9.x; bump datafusion/arrow to the matching family; `cargo test` green.
- **Re-run the gap audit** against the new base; strike rows already solved by 0.8/0.9.
- Delete `iceberg-spark-python/`, `iceberg-spark-pyspark/`, `bindings/python/`.
- Rewrite `PROJECT.md` + `CLAUDE.md` to this north star; remove dead references (`AGENTS.md`,
  `spark-sql-iceberg-parity.md`, fake `[patch]`/version pins). Prune stale memory/docs.
- Declare ownership; drop the mergeability invariant (but keep cherry-picking upstream wins).

### Phase 1 — Spec & metadata completeness
- `UpdateSchema` (add/drop/rename/reorder/promote, make-optional/required).
- `UpdatePartitionSpec` (partition evolution).
- `ManageSnapshots` (branch/tag create+remove, rollback, cherrypick, set-current, fast-forward).
- Full snapshot-ref handling; V3 groundwork (row lineage fields, column default values).

### Phase 2 — Write engine (largest functional gap)
- Merge append, `OverwriteFiles`, `ReplacePartitions`, `DeleteFiles`, `RowDelta`, `RewriteFiles`,
  `RewriteManifests`.
- Finalize position-delete + deletion-vector writers.
- Multi-op transactions + optimistic-concurrency retry, **validated against Glue + S3 Tables**.

### Phase 3 — Scan parity
- Inclusive/strict metrics evaluators; complete residual evaluation.
- `IncrementalAppendScan`, `IncrementalChangelogScan`, `BatchScan`; split planning.
- `ScanReport` / `MetricsReporter`; full metadata-inspection table set (files, entries, history,
  refs, partitions, all_* …).

### Phase 4 — Format & type breadth
- ORC + Avro **data** file read/write (parity with Java `data/`).
- V3 types end-to-end: variant (incl. shredding), geometry/geography + geospatial predicates,
  timestamp_ns, unknown, column default values.

### Phase 5 — Catalog & views
- `ViewCatalog` + view operations (create/replace/drop/list, view versions/representations) on
  **Glue + S3 Tables**, then REST.
- `SessionCatalog`, `LockManager` completeness; Glue + S3 Tables hardening (the priority targets).

### Phase 6 — Maintenance actions & encryption
- Engine-agnostic action layer: `ExpireSnapshots`, `DeleteOrphanFiles`, `RewriteDataFiles`
  (compaction), `RewritePositionDeleteFiles`, `RemoveDanglingDeleteFiles`,
  `ComputeTableStats`/`ComputePartitionStats`, `SnapshotTable`/`MigrateTable`/`RewriteTablePath`.
- Encryption: `EncryptionManager`, KMS client, encrypted FileIO + encrypted manifests/data (V3).
- Metrics reporting + events/listeners.

### Phase 7 — Continuous parity
- Automation tracking Java release tags → re-crawl new features into [GAP_MATRIX.md](GAP_MATRIX.md).
- Differential conformance suite vs Java-produced tables, run in CI.
- Selective adoption of upstream `iceberg-rust` improvements.

## Definition of done (per capability)

A row in [GAP_MATRIX.md](GAP_MATRIX.md) flips to ✅ only when: (1) the Rust API matches the Java
contract's behavior, (2) unit tests ship with it, and (3) an interop test proves byte-level table
compatibility with Java in both directions where applicable.
</content>
