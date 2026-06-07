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

- **Rust base audited:** fork at `iceberg` **0.7.0** (datafusion 50, arrow 56.2). Source of truth:
  `crates/iceberg/src/{spec,expr,scan,transaction,writer,arrow,io,inspect,puffin,catalog}`.
- **Java reference:** `apache/iceberg` `main` (shallow clone), modules `api/` + `core/` +
  `data/` + `orc/` + `parquet/` + `arrow/`.
- **⚠️ This matrix predates the upstream 0.9.x sync (Phase 0).** Upstream 0.8.0 (Jan 2026), 0.9.0
  (Mar 2026), and 0.9.1 (May 2026) likely close several rows below — **re-run this audit against the
  0.9.x base before starting Phase 1** and strike the rows already solved.

## Matrix

| Area | Status | Java reference | Rust location / note |
|---|---|---|---|
| Primitive + nested types | ✅ | `api/.../types/Types.java` | `spec/datatypes.rs` |
| V3 types: variant | ❌ | `api/.../variants/` | none |
| V3 types: geometry / geography | ❌ | `api/.../geospatial/` | none |
| V3 types: timestamp_ns, unknown | ❌ | `types/Types.java` | none |
| Column default values (initial/write) | ❌ | `Schema`/`Types` | none |
| Partition transforms (identity/bucket/truncate/year/month/day/hour/void) | ✅ | `api/.../transforms/` | `spec/transform.rs` |
| Schema evolution (`UpdateSchema`) | ❌ | `api/UpdateSchema.java` | no transaction action |
| Partition evolution (`UpdatePartitionSpec`) | ❌ | `api/UpdatePartitionSpec.java` | none |
| Sort order (`ReplaceSortOrder`) | ✅ | `api/ReplaceSortOrder.java` | `transaction/sort_order.rs` |
| Snapshot model + refs (branches/tags) | 🟡 | `api/Snapshot.java`, `SnapshotRef.java` | spec types only; no ops |
| Snapshot management (`ManageSnapshots`: branch/tag CRUD, rollback, cherrypick, set-current, fast-forward) | ❌ | `api/ManageSnapshots.java` | none |
| Manifest + manifest-list read/write | ✅ | `core/.../ManifestReader/Writer` | `spec/manifest`, `spec/manifest_list.rs` |
| `RewriteManifests` | ❌ | `api/RewriteManifests.java` | none |
| Write: fast append | ✅ | `api/AppendFiles.java` | `transaction/append.rs` |
| Write: merge append | ❌ | `AppendFiles` (merge mode) | none |
| Write: `OverwriteFiles` | ❌ | `api/OverwriteFiles.java` | none |
| Write: `ReplacePartitions` (dynamic/static overwrite) | ❌ | `api/ReplacePartitions.java` | none |
| Write: `DeleteFiles` | ❌ | `api/DeleteFiles.java` | none |
| Write: `RowDelta` (MOR) | ❌ | `api/RowDelta.java` | none |
| Write: `RewriteFiles` (compaction commit) | ❌ | `api/RewriteFiles.java` | none |
| Multi-op transactions + optimistic-concurrency retry | 🟡 | `api/Transaction.java` | `catalog.update_table`; needs validation |
| Writer: data file | ✅ | `data/` | `writer/base_writer/data_file_writer.rs` |
| Writer: equality-delete | ✅ | `data/` | `writer/base_writer/equality_delete_writer.rs` |
| Writer: position-delete | 🟡 | `data/` | partial |
| Writer: deletion-vector (V3 puffin DV) | 🟡 | `core/.../deletes` | `delete_vector.rs` + `puffin/` |
| Writer: partitioning (fanout/clustered/unpartitioned) | ✅ | — | `writer/partitioning/` |
| Read: Parquet → Arrow | ✅ | `parquet/` | `arrow/reader.rs` |
| Read: ORC data files | ❌ | `orc/` | none |
| Read/write: Avro data files | ❌ | `core/.../avro` (data) | Avro is manifest-only here |
| Scan planning + partition pruning | ✅ | `api/TableScan.java` | `scan/` |
| Metrics evaluators (inclusive/strict) + residual evaluation | 🟡 | `expressions/` | `expr/visitors` (partial) |
| `IncrementalAppendScan` | ❌ | `api/IncrementalAppendScan.java` | none |
| `IncrementalChangelogScan` | ❌ | `api/IncrementalChangelogScan.java` | none |
| `BatchScan` | ❌ | `api/BatchScan.java` | none |
| Scan/commit metrics reporting (`ScanReport`, `MetricsReporter`) | ❌ | `metrics/` | none |
| Catalogs: REST, Hive, Glue, S3 Tables, SQL/JDBC, in-memory | ✅ | `core/.../{rest,jdbc,inmemory}`, `aws`, `hive-metastore` | `crates/catalog/*`, `catalog/memory` |
| `ViewCatalog` + view operations (create/replace/drop/list, versions) | 🟡 | `api/catalog/ViewCatalog.java`, `api/view/` | view *metadata* spec only (`spec/view_*`) |
| `SessionCatalog` | ❌ | `api/catalog/SessionCatalog.java` | none |
| `LockManager` | 🟡 | `api/LockManager.java` | partial |
| Encryption (`EncryptionManager`, KMS, encrypted FileIO/manifests) | ❌ | `api/encryption/`, `core/.../encryption` | V3 `spec/encrypted_key.rs` stub only |
| FileIO (S3/GCS/Azure/OSS/fs/memory) | ✅ | `core/.../io`, cloud modules | `io/storage_*.rs` (OpenDAL) |
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
3. **Format & type breadth** — ORC + Avro data files; V3 types (variant, geo, timestamp_ns, defaults).
4. **Views in catalogs** (`ViewCatalog` + view operations).
5. **Maintenance actions** (expire/orphan/compaction/rewrite-deletes/compute-stats/migrate).
6. **Encryption** (`EncryptionManager`, KMS, encrypted FileIO/manifests).
</content>
</invoke>
