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

# Java ↔ Rust Capability Gap Matrix

> **Goal:** 1-to-1 capability parity between this Rust implementation and the Apache Iceberg
> **Java `iceberg-core` / `iceberg-api`** library (the engine-agnostic table-format library — *not*
> the Spark engine integration). This is a **living document**: re-run the audit after every upstream
> sync and after each parity phase lands.

## Status legend

- ✅ **present** — implemented to a usable degree
- 🟡 **partial** — exists but incomplete vs Java
- ❌ **missing** — not implemented

## Audit provenance

- **Rust base audited:** owned fork on upstream **`iceberg` 0.9.1** (datafusion 52.2, arrow 57.1,
  parquet 57.1, MSRV 1.92), **re-audited 2026-06-07** after the Phase 0 sync. Source of truth:
  `crates/iceberg/src/{spec,expr,scan,transaction,writer,arrow,io,inspect,puffin,catalog}` plus the
  new `crates/storage/opendal` FileIO crate.
- **Java reference:** `apache/iceberg` `main`, modules `api/` + `core/` + `data/` + `orc/` +
  `parquet/` + `arrow/`.
- **Re-audit policy:** re-run after every upstream sync and after each parity phase; date-stamp this
  block and strike the rows the sync/phase solved.
- **What the 0.7→0.9.1 sync changed (flipped rows below):** `timestamp_ns` type, column default
  values (`initial_default`/`write_default`), merge-on-read **read** application of position-deletes +
  deletion-vectors during scan, the `upgrade_format_version` transaction action, and a real
  `TransactionAction`/`ApplyTransactionAction` extension seam. The **headline gaps are unchanged**:
  write-engine actions, schema/partition/snapshot evolution, incremental scans, ORC/Avro data files,
  variant/geo/unknown types, catalog view ops, maintenance actions, encryption.

## Matrix

| Area | Status | Java reference | Rust location / note |
|---|---|---|---|
| Primitive + nested types | ✅ | `api/.../types/Types.java` | `spec/datatypes.rs` |
| V3 types: variant | ❌ | `api/.../variants/` | none |
| V3 types: geometry / geography | ❌ | `api/.../geospatial/` | none |
| V3 types: timestamp_ns | ✅ | `types/Types.java` | `spec/datatypes.rs` (`PrimitiveType::TimestampNs`) |
| V3 types: unknown | ❌ | `types/Types.java` | none |
| Column default values (initial/write) | ✅ | `Schema`/`Types` | `spec/datatypes.rs` `NestedField` carries `initial_default`/`write_default` |
| Partition transforms (identity/bucket/truncate/year/month/day/hour/void) | ✅ | `api/.../transforms/` | `spec/transform.rs` |
| Schema evolution (`UpdateSchema`) | ❌ | `api/UpdateSchema.java` | no transaction action |
| Partition evolution (`UpdatePartitionSpec`) | ❌ | `api/UpdatePartitionSpec.java` | none |
| Sort order (`ReplaceSortOrder`) | ✅ | `api/ReplaceSortOrder.java` | `transaction/sort_order.rs` |
| Snapshot model + refs (branches/tags) | 🟡 | `api/Snapshot.java`, `SnapshotRef.java` | spec types + ref ops (`transaction/manage_snapshots.rs`) |
| Snapshot management (`ManageSnapshots`: branch/tag CRUD, rollback, cherrypick, set-current, fast-forward) | 🟡 | `api/ManageSnapshots.java` | `transaction/manage_snapshots.rs`: create/replace/remove branch+tag, rename-branch, set-current, rollback (ancestry-checked), fast-forward, retention — with optimistic-concurrency `RefSnapshotIdMatch` guards + unit tests. **Deferred:** cherrypick, rollbackToTime; Java interop test pending before ✅. |
| Manifest + manifest-list read/write | ✅ | `core/.../ManifestReader/Writer` | `spec/manifest`, `spec/manifest_list.rs` |
| `RewriteManifests` | ❌ | `api/RewriteManifests.java` | none |
| Write: fast append | ✅ | `api/AppendFiles.java` | `transaction/append.rs` |
| Write: merge append | ❌ | `AppendFiles` (merge mode) | none |
| Write: `OverwriteFiles` | ❌ | `api/OverwriteFiles.java` | none |
| Write: `ReplacePartitions` (dynamic/static overwrite) | ❌ | `api/ReplacePartitions.java` | none |
| Write: `DeleteFiles` | ❌ | `api/DeleteFiles.java` | none |
| Write: `RowDelta` (merge-on-read) | ❌ | `api/RowDelta.java` | none |
| Write: `RewriteFiles` (compaction commit) | ❌ | `api/RewriteFiles.java` | none |
| Transaction action extension seam | 🟡 | `core/.../BaseTransaction` | `transaction/action.rs` — `TransactionAction`/`ApplyTransactionAction` + `ActionCommit` exist (trait is `pub(crate)`; we own it → make `pub` in Phase 2) |
| Write: `upgrade_format_version` action | ✅ | format-version upgrade | `transaction/upgrade_format_version.rs` (new in 0.9) |
| Multi-op transactions + optimistic-concurrency retry | 🟡 | `api/Transaction.java` | `catalog.update_table`; needs validation against Glue/S3 Tables |
| Writer: data file | ✅ | `data/` | `writer/base_writer/data_file_writer.rs` |
| Writer: equality-delete | ✅ | `data/` | `writer/base_writer/equality_delete_writer.rs` |
| Writer: position-delete | 🟡 | `data/` | no dedicated `PositionDeleteWriter` in `writer/base_writer/`; read-side apply is done (see below) |
| Writer: deletion-vector (V3 puffin DV) | 🟡 | `core/.../deletes` | `delete_vector.rs` + `puffin/` (read solid; write side partial) |
| Writer: partitioning (fanout/clustered/unpartitioned) | ✅ | — | `writer/partitioning/` |
| Read: Parquet → Arrow | ✅ | `parquet/` | `arrow/reader.rs` |
| Read: merge-on-read apply (position-deletes + DVs during scan) | ✅ | `data/.../DeleteFilter` | `arrow/delete_filter.rs`, `arrow/caching_delete_file_loader.rs`, `delete_file_index.rs` (new in 0.8/0.9) |
| Read: ORC data files | ❌ | `orc/` | none |
| Read/write: Avro data files | ❌ | `core/.../avro` (data) | Avro is manifest-only here |
| Scan planning + partition pruning | ✅ | `api/TableScan.java` | `scan/` |
| Metrics evaluators (inclusive/strict) + residual evaluation | 🟡 | `expressions/` | `expr/visitors` (partial) |
| `IncrementalAppendScan` | ❌ | `api/IncrementalAppendScan.java` | none |
| `IncrementalChangelogScan` | ❌ | `api/IncrementalChangelogScan.java` | none |
| `BatchScan` | ❌ | `api/BatchScan.java` | none |
| Scan/commit metrics reporting (`ScanReport`, `MetricsReporter`) | ❌ | `metrics/` | none |
| Catalogs: REST, Hive, Glue, S3 Tables, SQL/JDBC, in-memory | ✅ | `core/.../{rest,jdbc,inmemory}`, `aws`, `hive-metastore` | `crates/catalog/*`, `catalog/memory` |
| `ViewCatalog` + view operations (create/replace/drop/list, versions) | 🟡 | `api/catalog/ViewCatalog.java`, `api/view/` | view metadata spec + builder (`spec/view_metadata*`, `view_version.rs`) and `ViewCreation`/`ViewUpdate` types in `catalog/mod.rs`; **no `ViewCatalog` trait / no catalog view ops** (REST/Glue/etc.) |
| `SessionCatalog` | ❌ | `api/catalog/SessionCatalog.java` | none |
| `LockManager` | 🟡 | `api/LockManager.java` | partial |
| Encryption (`EncryptionManager`, KMS, encrypted FileIO/manifests) | ❌ | `api/encryption/`, `core/.../encryption` | V3 `spec/encrypted_key.rs` stub only |
| FileIO (S3/GCS/Azure/OSS/fs/memory) | ✅ | `core/.../io`, cloud modules | `io/` + extracted `crates/storage/opendal` (OpenDAL) |
| Puffin read/write + blob types (theta NDV, DV) | 🟡 | `core/.../puffin`, `api/.../puffin` | `puffin/` (blob coverage partial) |
| Maintenance: `ExpireSnapshots` | ❌ | `api/actions/ExpireSnapshots.java` | none |
| Maintenance: `DeleteOrphanFiles` | ❌ | `api/actions/DeleteOrphanFiles.java` | none |
| Maintenance: `RewriteDataFiles` (compaction) | ❌ | `api/actions/RewriteDataFiles.java` | none |
| Maintenance: `RewritePositionDeleteFiles` | ❌ | `api/actions/RewritePositionDeleteFiles.java` | none |
| Maintenance: `RemoveDanglingDeleteFiles` | ❌ | `api/actions/RemoveDanglingDeleteFiles.java` | none |
| Maintenance: `ComputeTableStats` / `ComputePartitionStats` | ❌ | `api/actions/Compute*.java` | none |
| Maintenance: `SnapshotTable` / `MigrateTable` / `RewriteTablePath` | ❌ | `api/actions/` | none |
| Partition statistics (`UpdatePartitionStatistics`, `PartitionStatisticsScan`) | ❌ | `api/Partition*Statistics*.java` | table-level stats partial |
| Table-level statistics (`UpdateStatistics`) | ✅ | `api/UpdateStatistics.java` | `transaction/update_statistics.rs` |
| Metadata inspection tables | 🟡 | `core/.../*Table` (~15 variants) | `inspect/` has snapshots + manifests only |
| Name mapping (schema-less Parquet) | ✅ | `mapping/` | `spec/name_mapping/` |
| Events / listeners | ❌ | `api/events/`, `core/.../events` | none |
| Type utilities (prune/assign-ids/reassign/check-compat) | 🟡 | `types/TypeUtil.java` etc. | partial |

## Headline gaps (ranked by effort × value)

1. **Write engine** — everything beyond fast-append (`OverwriteFiles`, `ReplacePartitions`,
   `DeleteFiles`, `RowDelta`, `RewriteFiles`, `RewriteManifests`, merge append).
2. **Schema/partition evolution + snapshot management** (`UpdateSchema`, `UpdatePartitionSpec`,
   `ManageSnapshots`).
3. **Format & type breadth** — ORC + Avro data files; remaining V3 types (variant, geo, unknown).
   (`timestamp_ns` and column default values already landed in the 0.8/0.9 base — see the matrix.)
4. **Views in catalogs** (`ViewCatalog` + view operations).
5. **Maintenance actions** (expire/orphan/compaction/rewrite-deletes/compute-stats/migrate).
6. **Encryption** (`EncryptionManager`, KMS, encrypted FileIO/manifests).
</content>
</invoke>
