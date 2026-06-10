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
- **Interop oracle (test-only):** `dev/java-interop/` drives Java `iceberg-core` **1.10.0** as a
  read/write oracle for bidirectional fixtures (not shipped, not a Cargo dependency). One
  `generate`/`verify` pass now covers all THREE Phase-1 evolution capabilities — it backs the
  ✅-by-interop rows `UpdateSchema`, `UpdatePartitionSpec`, and the `ManageSnapshots` ref-operation surface
  (incl. "Snapshot model + refs"), all dated 2026-06-07.
- **What the 0.7→0.9.1 sync changed (flipped rows below):** `timestamp_ns` type, column default
  values (`initial_default`/`write_default`), merge-on-read **read** application of position-deletes +
  deletion-vectors during scan, the `upgrade_format_version` transaction action, and a real
  `TransactionAction`/`ApplyTransactionAction` extension seam. The **headline gaps are unchanged**:
  write-engine actions, schema/partition/snapshot evolution, incremental scans, ORC/Avro data files,
  variant/geo/unknown types, catalog view ops, maintenance actions, encryption.
- **Cell style (de-triplication pass, 2026-06-10):** this matrix is the ONLY status record (the
  one-home-per-fact rule in [CLAUDE.md](../../CLAUDE.md)). Cells stay TERSE — Rust location, a 1–2
  sentence summary, flip dates, links. The full per-increment narratives the cells used to carry
  moved verbatim to [archive/2026-06_matrix-cell-narratives.md](archive/2026-06_matrix-cell-narratives.md)
  (grep by row Area). When a status flips: update the cell here, link evidence, write the narrative
  in the todo/lessons flow — do NOT grow the cell back.

## Matrix

| Area | Status | Java reference | Rust location / note |
|---|---|---|---|
| Primitive + nested types | ✅ | `api/.../types/Types.java` | `spec/datatypes.rs` |
| V3 types: variant | ❌ | `api/.../variants/` | none |
| V3 types: geometry / geography | ❌ | `api/.../geospatial/` | none |
| V3 types: timestamp_ns | ✅ | `types/Types.java` | `spec/datatypes.rs` (`PrimitiveType::TimestampNs`/`TimestamptzNs`); **V3-only format-version gate enforced** — `Schema::check_compatibility` rejects a `timestamp_ns`/`timestamptz_ns` field (incl. nested) on a V1/V2 table (Java `Schema.MIN_FORMAT_VERSIONS`, "Invalid type for {col}: timestamp_ns is not supported until v3") |
| V3 types: unknown | ❌ | `types/Types.java` | none |
| Column default values (initial/write) | ✅ | `Schema`/`Types` | `spec/datatypes.rs` `NestedField` carries `initial_default`/`write_default` |
| Partition transforms (identity/bucket/truncate/year/month/day/hour/void) | ✅ | `api/.../transforms/` | `spec/transform.rs` |
| Schema evolution (`UpdateSchema`) | ✅ | `api/UpdateSchema.java`, `core/SchemaUpdate.java`, `schema/UnionByNameVisitor.java` | `transaction/update_schema.rs` — full Java `SchemaUpdate` parity: add/rename/update-type/doc/make-optional/require/delete/move/set-identifier-fields, `union_by_name` (full `UnionByNameVisitor`), LEVEL-ORDER fresh field-id assignment, column initial/write defaults, and the V3-only initial-default + type gates (`Schema::check_compatibility` wired into `TableMetadataBuilder::add_schema`). **Interop ✅ 2026-06-07** — bidirectional via `crates/iceberg/tests/interop_update_schema.rs` (7 scenarios). Full narrative + edge cases: [archive](archive/2026-06_matrix-cell-narratives.md). |
| Partition evolution (`UpdatePartitionSpec`) | ✅ | `api/UpdatePartitionSpec.java`, `core/BaseUpdatePartitionSpec.java` | `transaction/update_partition_spec.rs` — full `BaseUpdatePartitionSpec` parity: auto-naming, field-id+name recycling, V1 void replacement, guard rails; the partition-name↔schema collision guard was relaxed to identity-OR-void (a real divergence interop surfaced and fixed). **Interop ✅ 2026-06-07** — bidirectional via `crates/iceberg/tests/interop_update_partition_spec.rs` (7 scenarios). Narrative: [archive](archive/2026-06_matrix-cell-narratives.md). |
| Sort order (`ReplaceSortOrder`) | ✅ | `api/ReplaceSortOrder.java` | `transaction/sort_order.rs` |
| Snapshot model + refs (branches/tags) | ✅ | `api/Snapshot.java`, `SnapshotRef.java`, `core/SnapshotParser.java` | Spec types + ref operations (`transaction/manage_snapshots.rs`). **Interop ✅ 2026-06-07** — the `SnapshotReference`/`SnapshotRetention` model round-trips with Java via `crates/iceberg/tests/interop_manage_snapshots.rs`. Narrative: [archive](archive/2026-06_matrix-cell-narratives.md). |
| Snapshot management (`ManageSnapshots`: branch/tag CRUD, rollback, rollback-to-time, set-current, fast-forward) | ✅ (ref-op surface; **`cherrypick` Phase-2-gated**) | `api/ManageSnapshots.java`, `core/SetSnapshotOperation.java`, `api/SnapshotRef.java` | `transaction/manage_snapshots.rs` — branch/tag CRUD + rename, set-current, rollback (ancestry-checked), rollback-to-time (strict `<`), fast-forward, retention (non-positive rejected), no-op suppression. **Interop ✅ 2026-06-07** — 7 scenarios both directions via `crates/iceberg/tests/interop_manage_snapshots.rs`. `cherrypick` extends `MergingSnapshotProducer` (replays data files) → Phase-2-gated. Narrative: [archive](archive/2026-06_matrix-cell-narratives.md). |
| Manifest + manifest-list read/write | ✅ | `core/.../ManifestReader/Writer` | `spec/manifest`, `spec/manifest_list.rs`. Spec-mandated default-to-0 reads for V1→V2 fields VERIFIED + mutation-pinned 2026-06-07 (snapshot `sequence-number` lenient read fixed; the Avro siblings were already correct). Residual: table-metadata `last-sequence-number` strict read — a non-Java robustness gap only (Java always writes it). Narrative: [archive](archive/2026-06_matrix-cell-narratives.md). |
| `RewriteManifests` | ❌ | `api/RewriteManifests.java` | none |
| Write: fast append | ✅ | `api/AppendFiles.java` | `transaction/append.rs` |
| Write: merge append | ❌ | `AppendFiles` (merge mode) | none |
| Write: `OverwriteFiles` | 🟡 | `api/OverwriteFiles.java`, `core/BaseOverwriteFiles.java`, `core/MergingSnapshotProducer.java` | `transaction/overwrite_files.rs` — explicit add + delete in one snapshot, dynamic `operation()`, `overwrite_by_row_filter` (residual-based KEEP/DELETE/PARTIAL-error per file), conflict validations: `validate_no_conflicting_data` (filter-based), `validate_added_files_match_overwrite_filter`, `validateNewDeletes` branches A (row-filter, landed 2026-06-10) + B. Known SAFE divergence: equality-delete applicability over-approximates (over-rejects, never under).  **Metadata-level interop ✅ 2026-06-10 (explicit-API paths):** the canonical snapshot-metadata view of the Rust action matches Java's byte-for-byte in the five-commit write-actions chain, judged by Java itself (`dev/java-interop/run-interop-write-actions.sh` + `crates/iceberg/tests/interop_write_actions_meta.rs`); row-filter/conflict-validation/multi-spec paths NOT covered by the chain. Deferred (→ ✅ beyond the chain): `validateDataFilesExist` wiring. Narrative: [archive](archive/2026-06_matrix-cell-narratives.md). || StrictMetricsEvaluator::eval(rowFilter,file))` (the `InclusiveProjection`/`StrictProjection` of the rowFilter on the partition + strict metrics on the FULL rowFilter — correct here, unlike the delete path) → else "Cannot append file with rows that do not match filter". **The conflict-filter default now follows Java `dataConflictDetectionFilter()` (L181-188):** with `overwrite_by_row_filter` set + no explicit deleted data files, `validate_no_conflicting_data`'s default conflict filter is the ROW FILTER (not `AlwaysTrue`); the non-row-filter path is byte-for-byte behavior-preserving (still `None`/explicit). 11 new `MemoryCatalog` tests (end-to-end strict-partition delete + scan; full replace; the PARTIAL-match error; non-match kept; added-files accept/reject both directions; the row-filter-as-default-conflict-filter discriminating pair + explicit-precedence + disabled-when-explicit-deletes), mutation-verified (strict→inclusive on the DELETE decision → the partial-match test fails; drop the row-filter default → the conflict-default test fails). **Conservative postures (documented, delete-manifest-specific, irrelevant to the data-file row-filter case):** Java's `failAnyDelete`, the duplicate-path warning, and the `isDelete`/`isDanglingDV`/`minSequenceNumber` branches are not ported. **`validateNoConflictingDeletes` rowFilter branch LANDED (2026-06-09):** the row-filter sub-branch of Java `BaseOverwriteFiles.validate`'s `validateNewDeletes` (L168-172) is now wired — when `validate_no_conflicting_deletes()` is on AND an `overwrite_by_row_filter` is set (`rowFilter() != alwaysFalse()`), `validate()` runs `validate_no_conflicting_added_delete_files` (Java `validateNoNewDeleteFiles`, `MergingSnapshotProducer` L562-570; pre-existing helper) AND the NEW filter-based `validate_deleted_data_files` (Java `validateDeletedDataFiles`, L636-654: walk concurrently-DELETED data-file tombstones via `deleted_data_files_after(skip_deletes=false)` = op-set `{Overwrite,Delete}`, test each via `InclusiveMetricsEvaluator`, reject "Found conflicting deleted files that can contain records matching {filter}: {path}") with `filter = conflict_detection_filter ?? rowFilter()`. 7 `MemoryCatalog` tests (both positives + the no-override tx-captured-start pin + flag-off control + the two row-filter-gate cases [neither-filter / conflict-filter-only]); reviewer mutation-pinned each non-vacuous (disable each check → exactly its test fails; refreshed-head start → exactly the no-override pin fails; wrong gate → the conflict-filter-only gate test fails). NO new deps. **Deferred to ✅:** data-level Java interop round-trip. Row stays 🟡 until data-level interop. |
| Write: `ReplacePartitions` (dynamic/static overwrite) | 🟡 | `api/ReplacePartitions.java`, `core/BaseReplacePartitions.java`, `core/MergingSnapshotProducer.java`, `core/ManifestFilterManager.java` | `transaction/replace_partitions.rs` — DYNAMIC partition overwrite: replaces every partition an added file belongs to via `SnapshotProducer::resolve_partition_deletes` feeding the shared rewrite path; full replace on an unpartitioned table; `replace-partitions` marker; opt-in `validate_no_conflicting_data`.  **Metadata-level interop ✅ 2026-06-10 (explicit-API paths):** the canonical snapshot-metadata view of the Rust action matches Java's byte-for-byte in the five-commit write-actions chain, judged by Java itself (`dev/java-interop/run-interop-write-actions.sh` + `crates/iceberg/tests/interop_write_actions_meta.rs`); row-filter/conflict-validation/multi-spec paths NOT covered by the chain. Deferred (→ ✅ beyond the chain): static `replaceByRowFilter`/explicit-partition APIs. Narrative: [archive](archive/2026-06_matrix-cell-narratives.md). |
| Write: `DeleteFiles` | 🟡 | `api/DeleteFiles.java`, `core/StreamingDelete.java`, `core/ManifestFilterManager.java` | `transaction/delete_files.rs` — delete by path / `DataFile` reference, plus the foundational manifest-filter/rewrite machinery (`SnapshotProducer::process_deletes`: rewrite/keep/drop + provenance preservation) every delete-bearing action reuses; opt-in `validate_files_exist` (the status-axis walk, 2026-06-09).  **Metadata-level interop ✅ 2026-06-10 (explicit-API paths):** the canonical snapshot-metadata view of the Rust action matches Java's byte-for-byte in the five-commit write-actions chain, judged by Java itself (`dev/java-interop/run-interop-write-actions.sh` + `crates/iceberg/tests/interop_write_actions_meta.rs`); row-filter/conflict-validation/multi-spec paths NOT covered by the chain. Deferred (→ ✅ beyond the chain): delete-by-row-filter/partition-predicate. Narrative: [archive](archive/2026-06_matrix-cell-narratives.md). |
| Write: `RowDelta` (merge-on-read) | 🟡 | `api/RowDelta.java`, `core/BaseRowDelta.java`, `core/MergingSnapshotProducer.java` | `transaction/row_delta.rs` — the merge-on-read commit: data files + position/equality DELETE files in ONE snapshot; producer delete-manifest support; added delete entries inherit the new snapshot's seq (the load-bearing invariant); dynamic `operation()`; conflict validations `validate_no_conflicting_data_files`/`_delete_files`, `validate_data_files_exist` (+ skip-deletes op-set), `validate_no_new_deletes_for_data_files` (validation-only). The full write→read chain is proven, and the DATA level is **interop ✅ both directions 2026-06-09** (see the merge-on-read read row). **Metadata-level interop ✅ 2026-06-10** — the canonical snapshot-metadata view (operation classification, count summaries, data/delete manifest split, post-inheritance sequence numbers) of Rust-written `fast_append`+`row_delta` chains matches Java's BYTE-FOR-BYTE across all three scan-exec fixtures, judged by Java itself (`dev/java-interop/run-interop-rowdelta-meta.sh` + `crates/iceberg/tests/interop_rowdelta_meta.rs`); the harness surfaced + fixed the `changed-partition-count` summary parity bug (`spec/snapshot_summary.rs`, 2026-06-10). Deferred (→ ✅): `removeRows`/`removeDeletes`, `validateAddedDVs`. Narrative: [archive](archive/2026-06_matrix-cell-narratives.md). |
| Write: `RewriteFiles` (compaction commit) | 🟡 | `api/RewriteFiles.java`, `core/BaseRewriteFiles.java`, `core/MergingSnapshotProducer.java`, `core/ManifestFilterManager.java` | `transaction/rewrite_files.rs` — atomically replace DATA files in one `Operation::Replace` snapshot (Java `BaseRewriteFiles`); non-empty-delete precondition; reuses the shared by-path rewrite machinery. A HARD guard rejects tables with ANY outstanding delete manifests because `dataSequenceNumber` preservation is not yet ported (prevents the row-resurrection corruption).  **Metadata-level interop ✅ 2026-06-10 (explicit-API paths):** the canonical snapshot-metadata view of the Rust action matches Java's byte-for-byte in the five-commit write-actions chain, judged by Java itself (`dev/java-interop/run-interop-write-actions.sh` + `crates/iceberg/tests/interop_write_actions_meta.rs`); row-filter/conflict-validation/multi-spec paths NOT covered by the chain. Deferred (→ ✅ beyond the chain): DELETE-file rewrite, seq preservation (lifts the guard), `validateNoNewDeletes` (shadowed by the guard). Narrative: [archive](archive/2026-06_matrix-cell-narratives.md). |
| Transaction action extension seam | 🟡 | `core/.../BaseTransaction` | `transaction/action.rs` — `TransactionAction`/`ApplyTransactionAction` + `ActionCommit` exist (trait is `pub(crate)`; we own it → make `pub` in Phase 2) |
| Write: `upgrade_format_version` action | ✅ | format-version upgrade | `transaction/upgrade_format_version.rs` (new in 0.9) |
| Multi-op transactions + optimistic-concurrency retry | 🟡 | `api/Transaction.java`, `core/SnapshotProducer.java`, `core/MergingSnapshotProducer.java` | `transaction/mod.rs` + `catalog.update_table` retry (`backon`, retryable-only) + the conflict-validation FOUNDATION (2026-06-08): tx-captured `starting_snapshot_id` surviving the re-base, the `validate` hook against the refreshed base, non-retryable `DataInvalid` conflicts, the shared `files_after` walk family. Deferred (→ ✅): remaining per-action validation blocks; validation against the REAL catalogs (Glue + S3 Tables). Narrative: [archive](archive/2026-06_matrix-cell-narratives.md). |
| Writer: data file | ✅ | `data/` | `writer/base_writer/data_file_writer.rs` |
| Writer: equality-delete | ✅ | `data/` | `writer/base_writer/equality_delete_writer.rs` |
| Writer: position-delete | 🟡 | `data/`, `core/.../deletes/PositionDeleteWriter.java` | `writer/base_writer/position_delete_writer.rs` — reserved field ids (2147483546/2147483545), write-as-given (sorting is the caller's job, matching Java), schema built FROM the reserved metadata-column fields. Proven Java-readable via the scan-exec Direction-2 interop (2026-06-09). Deferred: the sorting writer variant; the optional `row` column. Narrative: [archive](archive/2026-06_matrix-cell-narratives.md). |
| Writer: deletion-vector (V3 puffin DV) | 🟡 | `core/.../deletes` | `delete_vector.rs` + `puffin/` (read solid; write side partial) |
| Writer: partitioning (fanout/clustered/unpartitioned) | ✅ | — | `writer/partitioning/` |
| Read: Parquet → Arrow | ✅ | `parquet/` | `arrow/reader.rs` |
| Read: merge-on-read apply (position-deletes + DVs during scan) | ✅ | `data/.../DeleteFilter` | `arrow/delete_filter.rs`, `arrow/caching_delete_file_loader.rs`, `delete_file_index.rs`. **Data-level interop ✅ 2026-06-09 — the full cross-product** {position, equality} deletes × {Java-writes-Rust-reads, Rust-writes-Java-reads} × {unpartitioned, partitioned}, real parquet + real avro, via `crates/iceberg/tests/interop_scan_exec.rs` + the `dev/java-interop` run scripts (mutation-pinned by corrupting the written artifacts). Deferred: multi-file-per-partition, non-identity transforms, more column types. Narrative: [archive](archive/2026-06_matrix-cell-narratives.md). |
| Read: ORC data files | ❌ | `orc/` | none |
| Read/write: Avro data files | ❌ | `core/.../avro` (data) | Avro is manifest-only here |
| Scan planning + partition pruning | ✅ | `api/TableScan.java` | `scan/` — manifest-list → manifest → file pruning (manifest evaluator, partition filter, metrics evaluator), `use_ref` branch/tag scanning (2026-06-08), per-task PARTITION-REDUCED residuals (2026-06-08). **Scan-PLANNING interop ✅ 2026-06-09 (A4):** planned file set + per-file delete association + residual-always-true bit match Java; full residual-expression string comparison deferred (cross-language syntax). Narrative: [archive](archive/2026-06_matrix-cell-narratives.md). |
| Metrics evaluators (inclusive/strict) + residual evaluation | 🟡 | `expressions/`, `api/.../expressions/ResidualEvaluator.java` | `expr/visitors/` — inclusive/strict metrics evaluators + `ResidualEvaluator` (core 2026-06-08 + scan wiring: each `FileScanTask` carries the partition-reduced residual). Deferred: identity-partition constant materialization (`record_batch_transformer::constants_map` has latent type bugs — activation REVERTED 2026-06-08, needs its own increment gated on the datafusion + integration read tests). Narrative: [archive](archive/2026-06_matrix-cell-narratives.md). |
| `IncrementalAppendScan` | 🟡 | `api/IncrementalAppendScan.java`, `core/BaseIncrementalAppendScan.java`, `core/BaseIncrementalScan.java` | `scan/incremental.rs` (2026-06-08) — exclusive/inclusive `from` ranges, append-only snapshot walk, own-added-manifest + `Added`-entry filters, reuses the normal scan's plan machinery via the `_from_files` seam. Deferred (→ ✅): Java interop. Narrative: [archive](archive/2026-06_matrix-cell-narratives.md). |
| `IncrementalChangelogScan` | 🟡 | `api/IncrementalChangelogScan.java`, `core/BaseIncrementalChangelogScan.java`, `core/BaseIncrementalScan.java` | `scan/incremental.rs` (2026-06-08) — Insert/Delete changelog tasks with OLDEST→0 ordinals, deletes sourced from the deleting snapshot's own rewritten manifests; delegates range resolution to the append-scan builder. Deferred: `BatchScan`, CDC-merge (`UPDATE_BEFORE`/`UPDATE_AFTER`), interop. Narrative: [archive](archive/2026-06_matrix-cell-narratives.md). |
| `BatchScan` | ❌ | `api/BatchScan.java` | none |
| Scan/commit metrics reporting (`ScanReport`, `MetricsReporter`) | 🟡 | `metrics/` | `metrics/mod.rs` (data model, 2026-06-08: Java-shape serde incl. counter/timer wire format) + OPT-IN scan emission wiring (2026-06-09: `with_metrics_reporter`, report ONCE on full stream consumption; no-reporter path byte-unchanged). Documented divergence: the `filter` field uses Rust `Predicate` serde, not Java `ExpressionParser` JSON (large separate port). Deferred: `LoggingMetricsReporter` (needs a logging-facade dep approval); the 6 not-yet-collectable counters stay `None`. Narrative: [archive](archive/2026-06_matrix-cell-narratives.md). |
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
| Metadata inspection tables | 🟡 | `core/.../*Table` (~15 variants) | `inspect/` — the COMPLETE Java table set: `snapshots`, `manifests`, the `files` family, `entries`, `history`/`refs`/`metadata_log_entries`, `partitions`, the five `all_*` tables, and the `readable_metrics` virtual column; all SQL-queryable via `iceberg-datafusion`. **Inspection interop COMPLETE (2026-06-09→10, Direction-1 — read-only tables):** pure-metadata (offline JSON) + manifest-reading A1–A3 + scan-planning A4 + `readable_metrics` (run.sh-driven). Documented residual divergences keep the row 🟡: empty-partition column kept on unpartitioned tables (Java drops it), cross-spec partition-type unification deferred, interior readable_metrics field-id order (Java HashMap-order is non-portable; compare by name). Narrative: [archive](archive/2026-06_matrix-cell-narratives.md). |
| Name mapping (schema-less Parquet) | ✅ | `mapping/` | `spec/name_mapping/` |
| Events / listeners | ❌ | `api/events/`, `core/.../events` | none |
| Type utilities (prune/assign-ids/reassign/check-compat) | 🟡 | `types/TypeUtil.java` etc. | partial |

## Headline gaps (ranked by effort × value)

1. **Write engine** — everything beyond fast-append (`RowDelta`, `RewriteManifests`, merge append).
   **Started 2026-06-07: `DeleteFiles` 🟡 + `OverwriteFiles` 🟡 + `ReplacePartitions` 🟡 + `RewriteFiles` 🟡.**
   `DeleteFiles` delete-by-path/reference + the foundational manifest-filter / rewrite machinery in
   `SnapshotProducer::process_deletes`; `OverwriteFiles` composes that delete path with the fast-append
   add path in one `Overwrite` snapshot, reusing the now-shared `SnapshotProducer::{resolve_delete_paths,
   current_data_manifests}` (factored out of `DeleteFiles`); `ReplacePartitions` (dynamic partition
   overwrite) adds a by-PARTITION sibling resolver `SnapshotProducer::resolve_partition_deletes` that feeds
   the SAME `process_deletes` rewrite path (every partition an added file belongs to is replaced; an
   unpartitioned table is a full replace; `replace-partitions=true` summary marker); `RewriteFiles` (the
   compaction-commit primitive) replaces a set of DATA files with a new set in one `Replace` snapshot,
   reusing the by-path `resolve_delete_paths` + `process_deletes` machinery unchanged (Java
   `BaseRewriteFiles`: `operation()` = `REPLACE`, delete set must be non-empty, `failMissingDeletePaths`).
   All four `MemoryCatalog`-tested (post-commit SCAN live set + cross-partition isolation + provenance +
   compaction-to-fewer-files). Deferred: `overwriteByRowFilter` / delete-by-row-filter / static
   `replaceByRowFilter` (metrics evaluators), DELETE-file rewrite + `dataSequenceNumber` preservation
   (delete-file write path), concurrent-commit conflict validation (serializable isolation), and
   data-level Java interop.
2. **Schema/partition evolution + snapshot management** — `UpdateSchema`, `UpdatePartitionSpec`, **and the
   `ManageSnapshots` ref-operation surface** are now **✅** (bidirectional Java interop round-trips landed
   2026-06-07 via the `dev/java-interop/` oracle + the `crates/iceberg/tests/interop_update_schema.rs`,
   `interop_update_partition_spec.rs`, and `interop_manage_snapshots.rs` Direction-1 tests; one
   `generate`/`verify` pass covers all THREE capabilities — 7 schema + 7 partition + 7 manage-snapshots
   scenarios, both directions, all 21 PASS). The `UpdatePartitionSpec` interop surfaced (and fixed,
   in-scope) a real Rust↔Java divergence: the partition-name↔schema collision check was identity-only and
   rejected the legitimate V1 void replacement; relaxed to identity-OR-void (source-id-gated) to mirror
   Java's bind path. `UpdateSchema` column initial/write **defaults** are plumbed and
   `Schema::check_compatibility` (in `TableMetadataBuilder::add_schema`) now mirrors Java
   `Schema.checkCompatibility` in FULL — both the **V3-only initial-default guard** (rejects a non-null
   `initial_default` below v3) AND the **V3-only type gate** (rejects a `timestamp_ns`/`timestamptz_ns`
   field, incl. nested, below v3). For `ManageSnapshots`, both the **"Snapshot model + refs"** row and the
   **ref-operation surface** of the **"Snapshot management"** row are now ✅; **`cherrypick` remains
   Phase-2-gated and is NOT interop-proven** — it extends `MergingSnapshotProducer` / replays data files,
   so it is tracked under the write engine, not this metadata row. The manage-snapshots interop surfaced a
   read divergence the Rust V2/V3 snapshot reader required `sequence-number`, while the spec
   (`format/spec.md` lines 1979 & 2002) and Java's `SnapshotParser` both default an absent
   `sequence-number` to 0 on read; **FIXED 2026-06-07** with `#[serde(default)]` on
   `_serde::SnapshotV2`/`SnapshotV3.sequence_number`, so Rust now reads the seq-0 V1→V2-upgrade-carryover
   table class Java emits. The sibling spec-mandated default-to-0 read fields on the **Avro manifest /
   manifest-list path** — manifest-list `content`/`sequence-number`/`min-sequence-number`; manifest-entry
   `sequence_number`/`file_sequence_number`; data-file `content` — were **VERIFIED already-correct and pinned
   2026-06-07** (per-field empirical proof + mutation-verified regression tests; see the "Manifest +
   manifest-list read/write" row). The only residual sibling is table-metadata **`last-sequence-number`**,
   which Java ALWAYS writes for V2+ (`TableMetadataParser.toJson`), so Rust's required field never bites a
   Java-written table — a non-Java/hand-written robustness gap only, tracked in `task/todo.md`.
3. **Format & type breadth** — ORC + Avro data files; remaining V3 types (variant, geo, unknown). The
   V3-only **type** gate (Java `Schema.MIN_FORMAT_VERSIONS`: `timestamp_ns`/`variant`/`unknown`/`geometry`/
   `geography` require v3) is now **enforced for the representable types** (`timestamp_ns`/`timestamptz_ns`)
   in `Schema::check_compatibility`; `variant`/`unknown`/`geometry`/`geography` get a one-line
   `min_format_version` arm each when those types land (tracked in `task/todo.md`). (`timestamp_ns` and
   column default values already landed in the 0.8/0.9 base — see the matrix.)
4. **Views in catalogs** (`ViewCatalog` + view operations).
5. **Maintenance actions** (expire/orphan/compaction/rewrite-deletes/compute-stats/migrate).
6. **Encryption** (`EncryptionManager`, KMS, encrypted FileIO/manifests).
</content>
</invoke>
