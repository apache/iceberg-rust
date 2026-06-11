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

# Todo Archive — Phase 3 — Scan parity

Archived completed-increment narratives for Phase 3 (residual evaluation, inspection tables, scan metrics, and inspection / scan interop). Verbatim; not read by default. See [../todo.md](../todo.md) for live work and [map.md](map.md) for the index.

---

## Active: Scan metrics emission wiring (TableScan → MetricsReporter) — BUILDER Opus, 2026-06-09

Completes the DEFERRED scan-emission wiring from the metrics data-model increment (GAP_MATRIX row 97).
OPT-IN: when no reporter is set, the `plan_files` path is BYTE-UNCHANGED. Java authority:
`SnapshotScan.planFiles` (timer start → `doPlanFiles()` → `whenComplete` report on close) + `ManifestGroup`
/`ScanMetricsUtil.fileTask` (per-task counters) + `DataTableScan` (manifest-list totals).

- [x] Add `scan/metrics_collector.rs`: `ScanMetricsCollector` (Arc'd `AtomicI64` counters) + `snapshot()`
      → `ScanMetricsResult`. Only the cleanly-collectable counters + the planning timer are populated;
      indexed/equality/positional/dvs/skipped-files left `None` (documented not-yet-populated).
- [x] `TableScanBuilder::with_metrics_reporter(Arc<dyn MetricsReporter>)` (Option, default None) →
      `TableScan.metrics_reporter`.
- [x] Thread `Option<Arc<ScanMetricsCollector>>` through `PlanContext` → `ManifestFileContext` /
      `ManifestEntryContext` (None when no reporter). Manifest-list totals counted in `plan_files`;
      scanned/skipped at the `context.rs` prune point; per-task result_*/size in the stream wrapper.
- [x] Emission: wrap the `FileScanTaskStream` so that on FULL consumption (stream returns `None`) the
      collector is snapshotted, the timer stopped, a `ScanReport` is built (table name / snapshot id /
      schema id / projected field ids+names / filter), and `reporter.report(Scan(report))` is called ONCE.
      When `metrics_reporter` is None: no collector, no timer, no wrapper — byte-unchanged.
- [x] Tests (5) + mutation checks (a–d). Docs: GAP_MATRIX row 97 / Roadmap / lessons / todo.

Outcome: see final report. Stayed on `phase2-3-remnants`; no commit/push; no Cargo edits.

## Active: Phase 3 — Scan parity (inspection-table set)

Parity target: Java `iceberg-core` metadata-inspection tables (`core/.../*Table`, ~15 variants).
Authoritative plan: [Roadmap.md](../Roadmap.md) Phase 3; status: [GAP_MATRIX.md](../docs/parity/GAP_MATRIX.md)
`Metadata inspection tables` row. The `inspect/` framework already ships `snapshots` + `manifests`
(`MetadataTable<'a>` + `MetadataTableType` enum + per-table `XTable<'a>` with `schema()` + async `scan()`
→ `ArrowRecordBatchStream`).

**Inspection-table sub-sequence (dependency, then value):**
1. **`files` family** (`files` / `data_files` / `delete_files`) — THIS increment. Reads the current
   snapshot's manifest list → manifests → live entries → `DataFile`; the three differ ONLY by the
   manifest content filter (Java `BaseFilesTable`).
2. **`entries`** (+ `all_entries`) — the raw manifest-entry view (status/snapshot-id/seq + the `data_file`
   struct); supersedes much of the files-family read.
3. **`history` + `refs` + `metadata_log_entries`** — pure-metadata tables (no manifest IO).
4. **`partitions`** — per-partition aggregation over entries.
5. **`all_*`** (`all_data_files` / `all_manifests` / `all_entries` …) — scan across ALL snapshots, not
   just current.

### Phase 3 Increment 1 — `files` / `data_files` / `delete_files` (BUILDER Opus, 2026-06-08)
New file `crates/iceberg/src/inspect/files.rs`; wired via `inspect/mod.rs` + `metadata_table.rs`
(enum variants + accessors). Mirrors Java `BaseFilesTable` (one schema + one read + one projection,
the three tables differ ONLY by the manifest content filter — Rule of Three, factored as a shared base).

**Java contract verified against source** (`core/.../BaseFilesTable.java`, `FilesTable.java`,
`DataFilesTable.java`, `DeleteFilesTable.java`, `api/.../DataFile.java`):
- **Content filter is at the MANIFEST level** (Java `FilesTableScan.manifests()`): `files` →
  `snapshot().allManifests()`; `data_files` → `snapshot().dataManifests()` (content == DATA);
  `delete_files` → `snapshot().deleteManifests()` (content == DELETES). Within a manifest, only LIVE
  entries (Added/Existing, Rust `is_alive()`) are rows. A bug that wrongly filters content = wrong table.
  Rust mirror: filter `ManifestFile.content` (`ManifestContentType::{Data,Deletes}`) per table, then
  `entry.is_alive()`.
- **Schema = `DataFile.getType(partitionType).fields()`** (field-ids from `api/DataFile.java`), order:
  `content`(134), `file_path`(100), `file_format`(101), `spec_id`(141), `partition`(102),
  `record_count`(103), `file_size_in_bytes`(104), `column_sizes`(108) map<int,long>,
  `value_counts`(109), `null_value_counts`(110), `nan_value_counts`(137), `lower_bounds`(125)
  map<int,binary>, `upper_bounds`(128), `key_metadata`(131) binary, `split_offsets`(132) list<long>,
  `equality_ids`(135) list<int>, `sort_order_id`(140), `first_row_id`(142), `referenced_data_file`(143),
  `content_offset`(144), `content_size_in_bytes`(145). (`record_count`/`file_size` are LONG in the
  metadata-table schema even though the Rust `DataFile` holds them as `u64`.)

Plan:
- [x] `FilesTable<'a>` base struct (holds `&Table` + a `FilesTableKind` filter); `schema()` builds the
      Iceberg schema above from the table's DEFAULT partition type (partition struct field). `scan()`:
      current snapshot → `load_manifest_list` → for each `ManifestFile` whose content passes the table's
      manifest-content filter, `load_manifest` → for each LIVE entry, append a row from its `DataFile`.
- [x] One struct + a `FilesTableKind` { All, Data, Deletes } enum (the only thing that differs) +
      ctors `FilesTable::{all,data,deletes}`; `MetadataTable::{files, data_files, delete_files}`
      accessors + `MetadataTableType::{Files, DataFiles, DeleteFiles}` (+ `as_str`/`TryFrom`).
- [x] Partition column: `StructBuilder::from_fields(partition_arrow_fields)`; per partition field,
      dispatch on its `PrimitiveType` and append each per-row `Option<Literal>` (extracting the
      `PrimitiveLiteral`) — covers bool/int/long/float/double/date/time/timestamp/timestamp_ns/string/
      binary/decimal. Metrics maps via `MapBuilder<Int32Builder, Int64Builder>` (counts) /
      `<Int32Builder, LargeBinaryBuilder>` (bounds, RAW `Datum::to_bytes` per Java map<int,binary>);
      keys sorted for determinism. **DEVIATION:** `get_arrow_datum`/`Datum::new` was NOT used — direct
      `PrimitiveLiteral` extraction into typed Arrow builders is simpler and avoids scalar-array concat.
      **Timezone-tagged partition types (`timestamptz`/`timestamptz_ns`) return `FeatureUnsupported`**
      (the tz-tagged Arrow child can't be produced from a plain micro/nano builder without a panic at
      `StructBuilder::finish`; flagged as a deferred edge — partition-on-timestamptz is rare).
- [x] **DEFERRED `readable_metrics`** (Java `MetricsUtil.readableMetricsStruct`). All raw columns incl.
      the metrics maps + V3 DV fields land. Flagged.
- [x] 8 tests (`TableTestFixture` + a self-contained DATA + DELETE manifest writer — public crate APIs
      only, no scan-fixture private helper, no real parquet since the table reads manifest metadata):
      `files`=live data+delete set, `data_files`=DATA only, `delete_files`=deletes only, content 0-vs-1,
      record_count/file_size vs committed metadata, partition struct + column_sizes spot-check, Arrow
      schema column/type set, empty table. Content-filter + `is_alive()` mutation-verified load-bearing.
- [x] Docs: GAP_MATRIX `Metadata inspection tables` row (files/data_files/delete_files landed;
      readable_metrics deferred); Roadmap Phase 3 (inspection sub-sequence + this increment); this todo;
      lessons. Verify gate from repo root.

**Outcome (2026-06-08, Phase 3 Increment 1, BUILDER Opus):** `files` / `data_files` / `delete_files`
land at Java `BaseFilesTable` schema parity (🟡). One `FilesTable<'a>` + a `FilesTableKind` filter
(the Rule-of-Three shared base); the three accessors + enum variants wired. Reads the CURRENT snapshot's
manifest list, selects manifests by content (`All`/`Data`/`Deletes`), emits one row per LIVE entry's
`DataFile`. Arrow schema mirrors `DataFile.getType(partitionType).fields()` with the canonical field ids;
ALL raw columns present (metrics maps map<int,long>/map<int,binary>, list columns, V3 DV fields).
**Deferred:** `readable_metrics`; the other inspection variants (entries/history/refs/partitions/all_*);
timezone-tagged partition columns (FeatureUnsupported); Java/Spark interop comparison (→ ✅).
**Verify (repo root):** build clean; lib ×2 = 1417/0 both runs (was 1409 baseline → +8); 3 interop suites
4/4 each (manage_snapshots/update_schema/update_partition_spec); clippy -D warnings clean; fmt --check
clean. Content-filter (`data_files` wrongly included the delete file) AND `is_alive()` (Deleted tombstone
leaked in) mutations each fail the matching test. Files touched exactly the allowed set:
`inspect/files.rs` (new), `inspect/mod.rs`, `inspect/metadata_table.rs`, GAP_MATRIX, Roadmap, todo,
lessons. No Cargo/lockfile/`arrow/` edits, no commit. An Opus REVIEWER verifies next.

#### Phase 3 Increment 1 — REVIEW (2026-06-08, Opus REVIEWER, DELEGATED)
Adversarially verified points 1–5 against the Java source (`/tmp/iceberg-java-ref`) + throwaway probes
(deleted after). Plan:
- [x] **Pt 1 (content filter): CONFIRMED + mutation-verified.** Manifest-level filter is correct
      (`ManifestContentType` is whole-manifest DATA(0)|DELETES(1) — Iceberg never mixes content in one
      manifest, so manifest-level == file-level, matching Java `dataManifests`/`deleteManifests`).
      MUTATION A (`Data => true`, include delete manifests) → `test_data_files_table_excludes_delete_files`
      FAILS. MUTATION B (`Deletes => true`, include data manifests) → `test_delete_files_table_lists_only_
      delete_files` FAILS. Both restored.
- [x] **Pt 2 (live-entry filter): CONFIRMED + mutation-verified.** `entry.is_alive()` = Added|Existing;
      Deleted tombstone dropped. MUTATION C (`if true` instead of `is_alive()`, leak the Deleted
      2.parquet) → 4 tests FAIL (live-set, data-files-exclude, record-count count=4, partition includes
      200). Restored.
- [x] **Pt 3 (column mapping + field ids): CONFIRMED.** A throwaway probe dumped all 21 Arrow field ids;
      EVERY id + order matches Java `DataFile.getType(partitionType)` exactly (content/134, file_path/100,
      file_format/101, spec_id/141, partition/102, record_count/103, file_size/104, the 4 count maps
      108/117-118…137/138-139, the 2 bound maps 125/126-127, 128/129-130, key_metadata/131,
      split_offsets/132-133, equality_ids/135-136, sort_order_id/140, first_row_id/142,
      referenced_data_file/143, content_offset/144, content_size/145). Map value fields carry
      `nullable:false` (Java `MapType.ofRequired`). Values: `content` 0/1 vs committed; `lower_bounds`
      for `Datum::long(1)` = raw LE `[1,0,0,0,0,0,0,0]` (Iceberg single-value serialization, Java
      `map<int,binary>`). `content_type() as i32` and `file_format().to_string()` (lowercase) correct.
- [x] **Pt 4 (partition extraction + tz deferral): CONFIRMED.** A probe pinned partition value PER
      file_path: `{1.parquet:100, 3.parquet:300, delete-1.parquet:100}` — each row's partition struct
      matches THAT file (no row misalignment). The `timestamptz`/`timestamptz_ns` → `FeatureUnsupported`
      deferral is honest (explicit error arm in `append_partition_field`, not a silent wrong value).
- [x] **Pt 4b (deferrals honestly tracked): CONFIRMED.** `readable_metrics` + tz-partition
      `FeatureUnsupported` both in GAP_MATRIX/todo; row is 🟡; nothing silently wrong.
- [x] **Pt 5 (no regression + scope): CONFIRMED.** Build clean; lib ×2 = 1417/0 (→1418 with the new
      divergence test); 3 interop suites 4/4 each; clippy -D warnings clean; fmt clean. Only the named
      files. No bare `.unwrap()` in production (one justified `.expect("…statically valid")`). Empty
      table → 0 rows, no panic.
- [x] **DIVERGENCE FOUND (untracked) → pinned + tracked, NOT fixed:** Java `BaseFilesTable.schema()`
      drops the `partition` field for an UNPARTITIONED table (empty partition type); Rust keeps an
      empty-struct `partition` column. Verified non-corrupting (a probe scanned an unpartitioned table
      with one file: 1 row, `partition=Struct([])`, NO panic). Decision: the fix (conditionally omit the
      column through the 21-column builder) is more invasive than the BUILDER's scoped partitioned path
      and is non-corrupting + narrow, so — matching the Increment-5 V2-default precedent — added a
      divergence-PINNING test (`test_files_table_unpartitioned_keeps_empty_partition_struct_known_
      divergence`, asserts the current empty-struct column; flips to assert-absent when fixed) and tracked
      it as a GAP_MATRIX deferral rather than a silent gap. Row stays 🟡.

**Review outcome (2026-06-08, Opus REVIEWER):** all 5 brief points VERIFIED; content-filter (×2) +
`is_alive` (×1) mutations each fail the matching test; field-id + partition-value + bound-byte checks pass
vs Java. One untracked schema-shape divergence (unpartitioned empty-partition column) found → pinned with
a test + tracked in GAP_MATRIX (not fixed — non-corrupting, narrow, invasive-to-fix; left 🟡). Files
touched: `inspect/files.rs` (+1 test), GAP_MATRIX, todo, lessons. No production-logic change, no Cargo
edits, no commit, no branch switch.

### Phase 3 Increment 2 — `entries` inspection table (BUILDER Opus, 2026-06-08)
New file `crates/iceberg/src/inspect/entries.rs`; a shared `data_file` projection factored out of
`inspect/files.rs` into a new `inspect/data_file.rs` submodule (Rule of Three, 2nd use — `files` flattens
the projection to top-level columns, `entries` nests it under a single `data_file` struct column). Wired
via `inspect/mod.rs` + `metadata_table.rs` (`MetadataTableType::Entries` + `MetadataTable::entries`).
Mirrors Java `BaseEntriesTable` / `ManifestEntriesTable`.

**Java contract verified against source** (`core/.../BaseEntriesTable.java`,
`core/.../ManifestEntriesTable.java`, `core/.../ManifestEntry.java`):
- **Reads `snapshot().allManifests(io)` = ALL manifests (data AND delete)** of the CURRENT snapshot, and
  does NOT filter `isLive()` — so it SHOWS the `Deleted` tombstone (`status==2`). This is THE difference
  from `files` (which excludes Deleted). The `ManifestEntriesTable` javadoc: "exposes internal details,
  like files that have been deleted."
- **Schema = `ManifestEntry.getSchema(partitionType)` = `wrapFileSchema(DataFile.getType(partitionType))`:**
  `status`(0, required int), `snapshot_id`(1, optional long), `sequence_number`(**3**, optional long),
  `file_sequence_number`(**4**, optional long), `data_file`(**2**, required struct =
  `DataFile.getType(partitionType)` — the SAME field set the files table projects). **NOTE — the brief said
  ids 0/1/2/3/4; the authoritative Java source (`ManifestEntry.java:51-55`) uses 0/1/3/4/2** (seq=3,
  file_seq=4, data_file=2). Per CLAUDE.md (Java source is the spec-by-example, not the paraphrase), I
  implement the REAL Java ids 0/1/3/4/2 — and these match the Rust `ManifestEntry` doc comments exactly.
- **Status repr** = Java `Status` enum `EXISTING(0)/ADDED(1)/DELETED(2)` == Rust `ManifestStatus`
  `Existing=0/Added=1/Deleted=2` (`status() as i32`).

Plan:
- [x] Extract shared `inspect/data_file.rs`: `data_file_fields(partition_type) -> Vec<NestedFieldRef>`
      (the 21 `DataFile` columns) + a `DataFileStructBuilder` (a `StructBuilder` of those fields + the
      partition type) with `append(&DataFile)` / `finish() -> StructArray`. Refactored `files.rs` to wrap
      it (flatten = build the one `data_file` struct, emit `StructArray::columns()` as the 21 top-level
      columns). All the per-type partition/metric append helpers + extract helpers moved to the shared
      module. **KEY GOTCHA (solved):** `StructBuilder::from_fields` builds Map/List children as the BOXED
      shapes `MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>` / `ListBuilder<Box<dyn ArrayBuilder>>`
      (via `make_builder`) — NOT the typed `MapBuilder<Int32Builder, Int64Builder>` the old flat
      `FilesRowBuilder` constructed by hand. So the append helpers now take the `Dyn{Map,List}Builder`
      aliases and downcast the inner key/value/element builders. The boxed builders PRESERVE the keys/values/
      element field metadata (so field ids survive), so the explicit `metrics_map_fields`/`map_field_names`
      helpers were deleted.
- [x] `EntriesTable<'a>` (holds `&Table`); `schema()` = the 5-field entry schema (data_file =
      `Type::Struct(StructType::new(data_file_fields))`); `scan()`: current snapshot → `load_manifest_list`
      → for EVERY manifest (no content filter) → for EVERY entry (no `is_alive` filter) → append a row
      (status int, nullable snapshot_id/seq/file_seq via `append_option`, data_file struct via the shared
      builder). `file_sequence_number` is a public FIELD on `ManifestEntry`, not a method.
- [x] Wired `MetadataTableType::Entries` (+ `as_str` "entries" + `TryFrom`) + `MetadataTable::entries()`.
- [x] 6 tests (`TableTestFixture` + a DATA manifest Added/Deleted/Existing + a DELETE manifest Added —
      public crate APIs): (a) ALL 4 entries incl. the Deleted tombstone present (status 2); (b) status per
      entry (Added=1/Existing=0/Deleted=2); (c) snapshot_id/seq/file_seq vs committed — Existing preserves
      parent's id+seq, Deleted is snapshot-id-STAMPED to current by `add_delete_entry` but seq PRESERVED,
      Added inherits current's id+seq at read time; (d) data_file struct carries the right file
      (record_count/content + the Added file's column_sizes {1:42}) per entry; (e) Arrow schema = 5 cols,
      ids 0/1/3/4/2, data_file Struct (file_path keeps nested id 100), status non-null + snapshot_id
      nullable; (f) empty table → 0 rows.
- [x] Deferred `readable_metrics` (as files did). Docs: GAP_MATRIX inspection row, Roadmap Phase 3, todo,
      lessons. Verify gate from repo root.

**Outcome (2026-06-08, Phase 3 Increment 2, BUILDER Opus):** the `entries` inspection table lands at Java
`BaseEntriesTable`/`ManifestEntriesTable` schema parity (🟡). It reads the CURRENT snapshot's
`allManifests` (data AND delete) and emits EVERY entry — INCLUDING the `Deleted` tombstones (`status==2`)
that `files` excludes (the diagnostic view). The **shared data_file projection** (`data_file_fields` +
`DataFileStructBuilder`) was factored out of `files.rs` into `inspect/data_file.rs` (Rule of Three, 2nd
use): `files` FLATTENS the struct's `.columns()` to top-level columns, `entries` NESTS it under `data_file`
— one source of truth, the two cannot drift. **DEVIATION FROM BRIEF (flagged): field ids.** The brief said
data_file ids 0/1/2/3/4; the authoritative Java source `ManifestEntry.java:51-55` uses **0/1/3/4/2**
(status 0, snapshot_id 1, sequence_number 3, file_sequence_number 4, data_file 2). Per CLAUDE.md (Java
source is the spec-by-example, not the paraphrase), I implemented the REAL Java ids — which also match the
Rust `ManifestEntry` doc comments exactly. **Verify (repo root):** build clean; lib ×2 = 1424/0 both runs
(was 1418 baseline → +6 new entries tests; the files refactor is behavior-preserving — its 9 tests stay
green); 3 interop suites 4/4 each; clippy -D warnings clean; fmt --check clean. Files touched exactly the
allowed set: `inspect/data_file.rs` (new shared), `inspect/entries.rs` (new), `inspect/files.rs` (refactor
to the shared projection ONLY), `inspect/mod.rs`, `inspect/metadata_table.rs`, GAP_MATRIX, Roadmap, todo,
lessons. No Cargo/lockfile/`arrow/` edits, no commit, no branch switch. **Deferred:** `readable_metrics`;
`all_entries`/`history`/`refs`/`metadata_log`/`partitions`/`all_*`; Java/Spark interop (→ ✅). An Opus
REVIEWER verifies next.

### Phase 3 Increment 3 — `history` / `refs` / `metadata_log_entries` (BUILDER Opus, 2026-06-08)
Three PURE-METADATA inspection tables (no manifest IO) — each reads only `TableMetadata` fields and
projects to a `RecordBatch`, mirroring `inspect/snapshots.rs` (NOT the manifest-reading `files`/`entries`).
New files `inspect/{history.rs, refs.rs, metadata_log_entries.rs}`; wired via `inspect/mod.rs` +
`metadata_table.rs` (`MetadataTableType::{History, Refs, MetadataLogEntries}` + accessors + `as_str` +
`TryFrom` + `all_types`).

**Java schema authority — CONFIRMED from source (field ids verbatim):**
- `HistoryTable.java:37-42` — `made_current_at` ts(zone) #1 req, `snapshot_id` long #2 req, `parent_id`
  long #3 opt, `is_current_ancestor` bool #4 req. Row fn (`convertHistoryEntryFunc`): `madeCurrentAt =
  historyEntry.timestampMillis()*1000` (millis→micros); `parent_id = snapshots.get(snapshotId).parentId()`
  (the SNAPSHOT's parent, nullable); `is_current_ancestor = SnapshotUtil.currentAncestorIds(table)
  .contains(snapshotId)` (the parent-chain of the CURRENT snapshot).
- `RefsTable.java:33-40` — `name` str #1 req, `type` str #2 req ("BRANCH"/"TAG" via `SnapshotRefType.name()`),
  `snapshot_id` long #3 req, `max_reference_age_in_ms` long #4 opt, `min_snapshots_to_keep` int #5 opt
  (branches only), `max_snapshot_age_in_ms` long #6 opt (branches only). Tag carries ONLY max_ref_age.
- `MetadataLogEntriesTable.java:29-35` — `timestamp` ts(zone) #1 req, `file` str #2 req, `latest_snapshot_id`
  long #3 opt, `latest_schema_id` int #4 opt, `latest_sequence_number` long #5 opt. The log = `previousFiles`
  (metadata_log) PLUS a synthetic final entry `(lastUpdatedMillis, metadataFileLocation)` for the CURRENT
  file. `latest_*` derived from `SnapshotUtil.snapshotIdAsOfTime(ts)` = the LAST snapshot-log entry whose
  `made_current_at <= ts`; its `schema_id` + `sequence_number`; NULL when no snapshot ≤ ts (creation-time
  entry). All tractable from Rust metadata — NO deferral needed.

**`is_current_ancestor`**: build a `HashSet<i64>` by walking `current_snapshot_id` via `parent_snapshot_id`
(mirrors `manage_snapshots::is_ancestor_of` / Java `currentAncestorIds`); a history row is a current-ancestor
iff its snapshot id ∈ the set. A forked/rolled-back snapshot not on the current parent chain ⇒ false.

**Data sources (all PUBLIC or `pub(crate)`, reachable from in-crate `inspect/`)**: `metadata.history()`
(`&[SnapshotLog{snapshot_id,timestamp_ms}]`), `metadata.metadata_log()` (`&[MetadataLog{metadata_file,
timestamp_ms}]`), `metadata.refs` (`pub(crate) HashMap<String,SnapshotReference>` — reachable in-crate),
`metadata.current_snapshot_id()`, `metadata.last_updated_ms()`, `table.metadata_location()`,
`snapshot_by_id(id)` → `Snapshot::{parent_snapshot_id, schema_id, sequence_number, timestamp_ms}`.
Timestamps: spec `Timestamptz` → Arrow `Timestamp(µs, "+00:00")`; multiply `timestamp_ms * 1000` (like
snapshots.rs). NO new public spec accessor needed.

Plan:
- [x] `inspect/history.rs`: `HistoryTable` with `schema()` (4 fields) + async `scan()`. Build the
      current-ancestor id set; one row per `metadata.history()` entry; `parent_id` from the SNAPSHOT's parent.
- [x] `inspect/refs.rs`: `RefsTable` `schema()` (6 fields) + `scan()`; one row per `metadata.refs` entry;
      `type` = "BRANCH"/"TAG"; retention fields from `SnapshotRetention::{Branch,Tag}` (tag → only
      max_reference_age; branch unset → NULL). Sort by name for deterministic output.
- [x] `inspect/metadata_log_entries.rs`: `MetadataLogEntriesTable` `schema()` (5 fields) + `scan()`; rows =
      `metadata_log()` + synthetic CURRENT (`last_updated_ms`, `metadata_location`); `latest_*` via a local
      `snapshot_id_as_of_time` mirroring Java (last history entry ≤ ts), then its schema_id/sequence_number.
- [x] Wire enum + accessors in `metadata_table.rs`; `mod`+`pub use` in `mod.rs`.
- [x] Tests (in-crate `#[cfg(test)]`, build tables via `make_v2_table()` + `into_builder`/`add_snapshot`/
      `set_ref` + `with_metadata` — the `manage_snapshots.rs` forked-table pattern): history 2-snapshot +
      forked non-ancestor (is_current_ancestor false); refs main+branch+tag w/ retention set/unset; metadata
      log entries multi-commit; each table's Arrow schema (cols + field ids + types). Name each test for its risk.
- [x] Docs: GAP_MATRIX inspection row (history/refs/metadata_log_entries landed); Roadmap Phase 3; this todo; lessons.
- [x] Verify from repo root: build; lib ×2 (counts); 3 interop suites; clippy -D warnings; fmt --check.

**Outcome (2026-06-08, Phase 3 Increment 3, BUILDER Opus):** the three PURE-METADATA inspection tables
land at Java schema parity (🟡). Each reads only `TableMetadata` fields (no manifest IO), mirroring
`inspect/snapshots.rs`. **Field ids confirmed verbatim from the Java source** (not the brief paraphrase):
`HistoryTable` 1/2/3/4 (made_current_at/snapshot_id/parent_id/is_current_ancestor),
`RefsTable` 1..6 (name/type/snapshot_id/max_reference_age_in_ms/min_snapshots_to_keep/max_snapshot_age_in_ms),
`MetadataLogEntriesTable` 1..5 (timestamp/file/latest_snapshot_id/latest_schema_id/latest_sequence_number).
**`is_current_ancestor`** = membership in the set walked from `current_snapshot_id` via `parent_snapshot_id`
(local `current_ancestor_ids`, mirrors Java `SnapshotUtil.currentAncestorIds`; cycle-guarded). **refs**
branch/tag handled by matching `SnapshotRetention::{Branch,Tag}` — branch emits all three retention fields
(NULL where unset), tag emits only `max_reference_age_in_ms` and NULLs the two branch-only fields. **`latest_*`
FULLY IMPLEMENTED, nothing deferred:** a local `snapshot_id_as_of_time` walks `metadata.history()` for the last
entry with `timestamp_ms <= ts` (Java `SnapshotUtil.nullableSnapshotIdAsOfTime`), then reads that snapshot's
`schema_id`/`sequence_number`; NULL when no snapshot is at/older than the timestamp (creation-time file). The log
rows = `metadata_log()` + a synthetic final entry `(last_updated_ms, metadata_location)` for the CURRENT file,
exactly as Java appends. Timestamps: `Timestamptz` → Arrow `Timestamp(µs,"+00:00")`, value = log millis × 1000.
**Tests: 10 (4 history / 3 refs / 3 metadata_log_entries),** each named for its risk; tables built from the
committed `TableMetadataV2Valid.json` fixture (a local `make_v2_table` per module — `transaction::tests` is
private to `transaction/`) plus `into_builder`/`set_ref` for refs + branch/tag, a TWO-commit forked rollback for
the non-ancestor history case, and a directly-set `metadata_log` for the as-of-time resolution. **KEY FINDING
(forked history):** a single transaction that sets `main`→SIBLING then rolls back to CURRENT drops SIBLING from
the snapshot log as an *intermediate* snapshot (`TableMetadataBuilder::update_snapshot_log`, mirroring Java) — so
the forked fixture commits in TWO steps (commit 1 makes SIBLING current; commit 2 rolls main back to CURRENT),
leaving SIBLING a persisted log row that is correctly `is_current_ancestor == false`. The `is_current_ancestor`
column is mutation-verified (forcing it true fails the forked test). **Verify (repo root):** build clean; lib ×2
= 1434/0 both runs (was 1424 baseline → +10); 3 interop suites 4/4 each; clippy -D warnings clean; fmt --check
clean. Files touched exactly the allowed set: `inspect/{history,refs,metadata_log_entries}.rs` (new),
`inspect/mod.rs`, `inspect/metadata_table.rs`, GAP_MATRIX, Roadmap, todo, lessons. No Cargo/lockfile/`spec/`/`arrow/`
edits; NO new public spec accessor needed (`metadata.refs` is `pub(crate)`, reachable in-crate; everything else is
already public). No commit, no branch switch. **Deferred:** `partitions`/`all_*` variants; Java/Spark interop
(→ ✅). An Opus REVIEWER verifies next.

#### Phase 3 Increment 3 — REVIEW (2026-06-08, Opus REVIEWER, DELEGATED)
Adversarially verified points 1–5 against the Java source (`HistoryTable`/`RefsTable`/`MetadataLogEntriesTable`
+ `SnapshotUtil.{currentAncestorIds,ancestorsOf,nullableSnapshotIdAsOfTime}`) and by mutation-testing the
non-trivial computations.
- [x] **Pt 1 (field ids + Arrow types): CONFIRMED all three.** Java `HISTORY_SCHEMA` (1 made_current_at
      timestamptz req / 2 snapshot_id long req / 3 parent_id long opt / 4 is_current_ancestor bool req),
      `SNAPSHOT_REF_SCHEMA` (1 name / 2 type str req / 3 snapshot_id long req / 4 max_reference_age_in_ms long
      opt / 5 min_snapshots_to_keep int opt / 6 max_snapshot_age_in_ms long opt), `METADATA_LOG_ENTRIES_SCHEMA`
      (1 timestamp tstz req / 2 file str req / 3 latest_snapshot_id long opt / 4 latest_schema_id int opt / 5
      latest_sequence_number long opt) match the Rust `schema()` ids + nullability byte-for-byte. The
      `arrow_schema_*` expect-tests probe the PRODUCED Arrow field ids + the `Timestamp(µs, "+00:00")` UTC-micros
      type — all correct. `"BRANCH"`/`"TAG"` == Java `SnapshotRefType.name()`.
- [x] **Pt 2 (is_current_ancestor): CONFIRMED + mutation-verified.** Rust `current_ancestor_ids` walks
      `current_snapshot_id` → `parent_snapshot_id` (inclusive), == Java `ancestorsOf(currentSnapshot)`; the
      forked two-commit fixture makes SIBLING a non-ancestor log row → false; ROOT+CURRENT → true. Mutation
      (always-true) FAILS the forked test (left Some(true) vs right Some(false)). `parent_id` =
      `snapshot_by_id(entry.snapshot_id).parent_snapshot_id()` (the SNAPSHOT's parent, nullable) == Java
      `snap.parentId()`, NOT the previous log row. (Rust adds a benign cycle-guard Java lacks — cannot change
      correct-input results.)
- [x] **Pt 3 (refs retention): CONFIRMED.** Branch → all three fields (NULL where the `Option` is unset, e.g.
      `main`); tag → only `max_reference_age_in_ms`, branch-only fields explicitly NULL. Field-id→accessor map
      (4←maxRefAge, 5←minKeep, 6←maxSnapAge) matches Java `referencesToRows`.
- [x] **Pt 4 (latest_* as-of-time): CONFIRMED + TEST-GAP FIXED.** `snapshot_id_as_of_time` (last snapshot-log
      entry with `timestamp_ms <= ts`, else None) == Java `nullableSnapshotIdAsOfTime`; NULL-before-first / ROOT
      / CURRENT cases + the synthetic current-file row all correct. **GAP:** the builder's test offset every
      metadata-log entry OFF the snapshot timestamps, so the `<=`-vs-`<` boundary was UNPINNED — I mutation-tested
      `<=`→`<` and the test SURVIVED. Added `test_…_inclusive_of_exact_snapshot_timestamp` (entries at exactly
      `ROOT_TS`/`CURRENT_TS` → ROOT/CURRENT, not NULL/ROOT); mutation-verified it now FAILS under `<`. A wrong
      as-of-time = wrong latest_* per row, so this is load-bearing.
- [x] **Pt 5 (no regression + scope): CONFIRMED.** All prior tests green (lib ×2 = 1435/0); only the named
      files; no `spec/`/`arrow/`/`Cargo` edits; no public spec accessor added (`metadata.refs` is `pub(crate)`,
      in-crate). **FIXED:** the three `schema()` methods used bare `.unwrap()` (CLAUDE.md NN#3 violation) — copied
      from the OLDER `snapshots.rs`, but the in-scope Increment-1/2 siblings (`files.rs`/`entries.rs`) already use
      `.expect("… statically valid")`; replaced all three to match. Empty cases (no refs / no snapshot-log / empty
      metadata-log) verified panic-free by inspection (the schema-test already exercises the 1-synthetic-row
      empty-metadata-log path).

**Review outcome (2026-06-08, Opus REVIEWER):** all 5 points adjudicated; row stays 🟡 (interop deferred → ✅).
One test-strength gap fixed (the as-of-time `<=` boundary, mutation-verified) + one engineering-floor fix (bare
`.unwrap()` → `.expect()` ×3). Files touched: `inspect/{history,refs,metadata_log_entries}.rs` (the 3 in-scope
new files only) + todo + lessons. +1 test (1434 → 1435 lib). build/clippy/fmt clean; 3 interop suites 4/4 each.
No commit, no branch switch, no Cargo/spec/arrow edits.
---

### Phase 3 Increment 4 — `partitions` metadata table (SCOPED by orchestrator, 2026-06-08)
The first AGGREGATING inspection table: per-partition rollup over the current snapshot's LIVE manifest
entries (data AND delete). New file `inspect/partitions.rs`; wired via `inspect/mod.rs` +
`metadata_table.rs` (`MetadataTableType::Partitions` + accessor + `as_str` + `TryFrom`).

**Java authority — `core/src/main/java/org/apache/iceberg/PartitionsTable.java` (CONFIRMED from source):**
- Schema, in COLUMN order (= Java field-id declaration order; ids are NON-sequential):
  `partition`/1 (struct = `Partitioning.partitionType(table)`, REQUIRED) — DROPPED when unpartitioned
  (`TypeUtil.select` excludes it); `spec_id`/4 int req; `record_count`/2 long req (DATA records);
  `file_count`/3 int req (DATA files); `total_data_file_size_in_bytes`/11 long req;
  `position_delete_record_count`/5 long req; `position_delete_file_count`/6 int req;
  `equality_delete_record_count`/7 long req; `equality_delete_file_count`/8 int req;
  `last_updated_at`/9 ts(zone) OPT; `last_updated_snapshot_id`/10 long OPT.
- Rows = `partitions(table, scan)`: read `scan.snapshot().allManifests()` (DATA + DELETE), `liveEntries()`
  (Added/Existing == Rust `is_alive()`), group by the partition key (`StructLikeMap`). For each entry
  `Partition.update(file, snapshot)`:
  - `snapshot = table.snapshot(entry.snapshotId())`; if non-null and `snapshot.timestampMillis()*1000 >
    lastUpdatedAt` → set `specId = file.specId()`, `lastUpdatedAt = commitTimeMicros`,
    `lastUpdatedSnapshotId = snapshot.snapshotId()` (most-recent-commit file wins — `>` not `>=`).
  - content DATA → dataRecordCount += recordCount; dataFileCount += 1; dataFileSizeInBytes += fileSize.
  - content POSITION_DELETES → posDeleteRecordCount += recordCount; posDeleteFileCount += 1.
  - content EQUALITY_DELETES → eqDeleteRecordCount += recordCount; eqDeleteFileCount += 1.
- Unpartitioned table → single "root" partition row, partition column dropped (see decision below).

**Rust mapping (confirmed reachable):** `metadata.default_partition_type()` for the schema partition
struct (matches `files.rs`); the partition `Struct` derives `Hash`+`Eq` → use it directly as the
`HashMap`/`IndexMap` key. Per entry: `entry.is_alive()`, `entry.snapshot_id()` → Option<i64> →
`metadata.snapshot_by_id(id)` → `.timestamp_ms()`/`.snapshot_id()`; `data_file.content_type()`
(`DataContentType::{Data,PositionDeletes,EqualityDeletes}`), `.record_count()`, `.file_size_in_bytes()`,
`.partition()`, `.partition_spec_id`. Reuse the partition-struct Arrow builder primitive
`inspect::data_file::append_partition` (promote to `pub(super)` if reused — shared in-module helper,
in scope). Timestamps: micros = `timestamp_ms * 1000`, Arrow `Timestamp(µs, "+00:00")` (like snapshots.rs).
Sort output rows deterministically by partition value.

**Two scoping DECISIONS the builder must make against the Java source + state rationale:**
1. **Unpartitioned partition column:** Java DROPS it; `files.rs` KEPT an empty-struct column as a
   *documented* divergence (`test_files_table_unpartitioned_keeps_empty_partition_struct_known_divergence`).
   RECOMMENDED: match the `files.rs` precedent for one consistent module-wide divergence (keep empty
   struct, document + pin with a test), OR match Java exactly (drop). Either way: test it + document it.
2. **Multi-spec partition evolution:** Java unifies via `Partitioning.partitionType` + `coercePartition`.
   Rust has only `default_partition_type()` (no cross-spec unifier found). RECOMMENDED: implement the
   single-spec-correct path (key by the file's partition `Struct`, schema = default partition type) and
   DOCUMENT cross-spec unification as a deferral (known divergence) in GAP_MATRIX/todo — do NOT silently
   aggregate wrongly. If a unifier helper does exist, use it.

Plan:
- [x] `inspect/partitions.rs`: `PartitionsTable<'a>` + `schema()` (11 fields; kept empty-struct partition
      column when unpartitioned per decision 1) + async `scan()` doing the aggregation above into one
      `RecordBatch`.
- [x] Wire `MetadataTableType::Partitions` + `partitions()` accessor + `as_str`/`TryFrom`/`all_types` in
      `metadata_table.rs`; `mod partitions;` + `pub use` in `mod.rs`. Promoted
      `inspect::data_file::append_partition` to `pub(super)` for reuse (Rule-of-Three 3rd use).
- [x] Tests (in-crate, `TableTestFixture` + the `files.rs` manifest-builder pattern): two files same
      partition → summed record/file/size; multiple partitions → row-per + sorted; delete files counted
      in pos/eq columns (DATA columns unchanged); Deleted-tombstone excluded (`is_alive()`);
      `last_updated_*` + `spec_id` = most-recent-snapshot file (mutation-pinned the `>` comparison);
      empty table → 0 rows; unpartitioned → 1 root row + empty-struct column (decision 1); Arrow schema
      cols + field ids + types; file spec_id reported. 10 tests, each risk-named.
- [x] Docs: GAP_MATRIX inspection row, Roadmap Phase 3 list + Current state, this todo, lessons.
- [x] Verify from repo root: build; lib ×2 (1444 ea); 3 interop suites (4/4 ea); clippy -D warnings; fmt
      --check. All clean.

**Outcome (2026-06-08, BUILDER Opus).** Built `inspect/partitions.rs` (`PartitionsTable<'a>` + `schema()`
+ async `scan()`) — the first AGGREGATING inspection table — plus wiring in `metadata_table.rs`/`mod.rs`
and the `pub(super)` promotion of `append_partition` in `data_file.rs`. Reads the current snapshot's
manifest list → ALL manifests (data + delete) → live entries (`is_alive()`), groups by the file's
partition `Struct` (derives `Hash`+`Eq`), and rolls up per Java `PartitionsTable.Partition.update`. Schema
is 11 fields in Java column order with the EXACT non-sequential ids verified against
`PartitionsTable.java`: `partition`/1 (struct=`default_partition_type`, REQUIRED, lines 107-108),
`spec_id`/4 (44-45), `record_count`/2 (46-48), `file_count`/3 (49-50), `total_data_file_size_in_bytes`/11
(51-56), `position_delete_record_count`/5 (57-62), `position_delete_file_count`/6 (63-68),
`equality_delete_record_count`/7 (69-74), `equality_delete_file_count`/8 (75-80), `last_updated_at`/9
OPT (81-86), `last_updated_snapshot_id`/10 OPT (87-92). The update logic was confirmed against
`PartitionsTable.java:331-360` (commit time = `snapshot.timestampMillis()*1000`, strict `>`, content
switch). **Decision 1 (unpartitioned column): KEEP the empty-struct column** to match the `files`-family
precedent → one consistent module-wide divergence (Java drops it); pinned + documented. **Decision 2
(multi-spec evolution): DEFER cross-spec unification** — no `Partitioning.partitionType`/`coercePartition`
analogue exists in Rust (confirmed by grep — only `default_partition_type`), so I key by the file's own
partition `Struct` (single-spec-correct), report the per-file `spec_id`, and document the multi-spec
unification gap as a known divergence. 10 risk-named tests. Mutations run + caught: (a) content-type
switch — counting a position-delete as DATA fails `..._delete_files_counted_separately_from_data`
(record_count 6≠4); (b) the `>` comparison flipped to `<` (min-wins) makes the OLDEST snapshot win and
fails `..._last_updated_reflects_most_recent_commit_file` (1515…≠1555…) — note the FIRST cut of this test
was un-pinning because `write_data_manifest` used `add_entry` (which RESTAMPS the snapshot id to the
manifest's), so I rewrote it to write the older file via `add_existing_entry` (which PRESERVES the parent
id); (c) `is_alive()` bypass counts the Deleted tombstone and fails `..._excludes_deleted_tombstones`
(record_count 3≠1). Gate: build clean; lib 1444 passed ×2; interop_manage_snapshots/update_schema/
update_partition_spec 4/4 each; clippy -D warnings clean; fmt clean. No commit/branch/Cargo/spec/arrow
edits.

**REVIEW outcome (2026-06-08, REVIEWER Opus, DELEGATED).** Verdict: CHANGES-MADE (test-strength only; no
production-code change — the aggregation/tie-break/field-id logic is byte-for-byte correct vs
`PartitionsTable.java`, re-confirmed all 11 ids/types/nullability against lines 42-118 and the
`Partition.update` fold against 331-360). Independently ran SEVEN mutations by injection; FIVE were already
caught by the builder's suite (content-type misroute, `>`→`<`, `is_alive()` bypass, swapped field ids,
flipped null ordering). TWO SURVIVED — exactly the builder's self-flagged weak spots: (1) `>`→`>=`
(exact-commit-time tie: `>=` lets a later equal-time file overwrite the first-seen one, diverging from
Java's strict `>`), and (2) delete-file size leaking into `total_data_file_size_in_bytes` (no test asserted
the data-size total in the presence of delete files). Both are now PINNED:
- Added `test_partitions_table_last_updated_exact_commit_time_tie_keeps_first_seen_file` — rebuilds the
  parent snapshot with a commit time EQUAL to the current snapshot's (splicing it back into the `pub(crate)`
  `snapshots` map in-test, no new accessor), adds the parent's file FIRST and the current's SECOND, and
  asserts the FIRST-seen (parent) wins. Verified: passes under `>`, FAILS under `>=`.
- Strengthened `test_partitions_table_delete_files_counted_separately_from_data` to assert
  `total_data_file_size_in_bytes == FILE_SIZE` (DATA-only). Verified: FAILS when a delete size is summed in.
- Added `test_partitions_table_partition_with_only_delete_files_present_with_zero_data` (edge case: a
  delete-only partition still emits a row, data columns 0, delete columns populated — also asserts the size
  total stays 0).
- Added a direct `test_compare_partition_values_multi_field_and_null_first_ordering` unit test for the
  hand-rolled comparator (builder flag #2): multi-field tiebreak, null-first, and a non-long (String) field
  type. Verified: FAILS when the null ordering is flipped.
Gate after changes: build clean; lib **1447** passed ×2 (was 1444; +3 net new tests); interop
manage_snapshots/update_schema/update_partition_spec 4/4 each; clippy -D warnings clean; fmt clean. Scope:
touched ONLY `inspect/partitions.rs` (tests) + `docs/parity/GAP_MATRIX.md` + this file. No production-logic,
Cargo, spec, or arrow edits; no commit/branch. Residual risk: cross-spec partition-evolution unification
stays a documented DEFERRAL (decision 2); the unpartitioned empty-struct partition column stays a documented
module-wide DIVERGENCE (decision 1) — both honestly stated, neither overstated as parity.

---

### Phase 3 Increment 5a — `all_data_files` / `all_delete_files` / `all_files` / `all_entries` (SCOPED, 2026-06-08)
The cross-snapshot file/entry inspection tables: the SAME projection/rows as their current-snapshot
counterparts (`data_files`/`delete_files`/`files`/`entries`), but the manifest source becomes the
**deduplicated union of manifests reachable from ALL snapshots** (Java `BaseAllMetadataTableScan
.reachableManifests`). `all_manifests` is a SEPARATE later increment (5b) — distinct machinery.

**Java authority (CONFIRMED from source):**
- `BaseAllMetadataTableScan.reachableManifests(toManifests)` (L74-86): union over `table().snapshots()` of
  `toManifests(snapshot)`, **deduplicated via a HashSet of ManifestFile** (== by manifest path). NOT
  per-snapshot-tagged (no reference_snapshot_id — that's only `all_manifests`).
- `AllDataFilesTable` (extends `BaseFilesTable`): `manifests() = reachableManifests(snapshot.dataManifests)`.
  `AllFilesTable` → `allManifests`; `AllDeleteFilesTable` → `deleteManifests`. Same `BaseFilesTable` schema
  + LIVE-entry filter (`is_alive`) as the current-snapshot files family. Javadoc: "valid file = readable
  from ANY snapshot currently tracked"; "may return duplicate rows" (files NOT deduped — only manifests).
- `AllEntriesTable` (extends `BaseEntriesTable`): `reachableManifests(snapshot.allManifests)`, then the SAME
  entries rows (ALL entries incl. Deleted tombstones — NO is_alive filter), same schema as `entries`.

**Rust mapping (confirmed):** `metadata.snapshots()` → `impl ExactSizeIterator<&SnapshotRef>` (all snapshots);
per snapshot `snapshot.load_manifest_list(file_io, metadata)` → entries; dedup the `ManifestFile`s by
`manifest_path: String` (HashSet/seen-set, preserve first-seen order for determinism); then the EXISTING
per-manifest read in `files.rs`/`entries.rs` is reused verbatim. The content filter (`FilesTableKind`) and
`is_alive()` (files) / no-filter (entries) stay exactly as today.

**Design:** add a snapshot-SCOPE axis to `FilesTable` and `EntriesTable` (enum `{CurrentSnapshot, AllSnapshots}`
or similar), orthogonal to `FilesTableKind`. Factor a shared manifest-source helper (e.g. `pub(super) async fn
collect_manifests(table, scope, content_filter) -> Result<Vec<ManifestFile>>`, deduped for AllSnapshots) so
files.rs + entries.rs cannot drift. Wire `MetadataTableType::{AllDataFiles,AllDeleteFiles,AllFiles,AllEntries}`
+ `MetadataTable::{all_data_files,all_delete_files,all_files,all_entries}` accessors + as_str/TryFrom.

Plan:
- [x] Shared manifest-source helper (deduped reachable-manifest union for AllSnapshots; current-snapshot list
      for CurrentSnapshot), used by both tables. Keep the existing current-snapshot behavior byte-identical.
- [x] `FilesTable`: add the scope axis; `FilesTable::all_*` constructors; `scan()` uses the helper.
- [x] `EntriesTable`: add the scope axis; `EntriesTable::all()` constructor; `scan()` uses the helper.
- [x] Wire enum variants + accessors + as_str + TryFrom in `metadata_table.rs`.
- [x] Tests (in-crate): a MULTI-snapshot fixture where an OLD snapshot's manifest holds a file that the
      current snapshot's manifests do NOT → `all_*` includes it, the current-snapshot table excludes it;
      manifest DEDUP (a manifest shared by 2 snapshots is not double-read by all_entries); content filters
      still hold for all_data_files/all_delete_files; all_entries shows Deleted tombstones across snapshots;
      Arrow schema parity with the non-all counterparts. Mutation-pin the dedup + the all-vs-current scope.
- [x] Docs: GAP_MATRIX inspection row, Roadmap Phase 3, todo, lessons.
- [x] Verify from repo root: build; lib ×2; 3 interop suites; clippy -D warnings; fmt --check.

**Outcome (2026-06-08, BUILDER Opus):** Landed the four cross-snapshot file/entry inspection tables
`all_data_files` / `all_delete_files` / `all_files` / `all_entries` 🟡. **Files modified:**
`inspect/files.rs` (scope axis + `all_files`/`all_data_files`/`all_delete_files` ctors + 6 tests),
`inspect/entries.rs` (scope axis + `EntriesTable::all()` + 5 tests), `inspect/metadata_table.rs` (4 enum
variants + accessors + `as_str` + `TryFrom`), `inspect/mod.rs` (register the new module). **File created:**
`inspect/manifest_source.rs` — the SHARED single-source-of-truth helper. **Shared-helper design:** a new
`MetadataScope {CurrentSnapshot, AllSnapshots}` enum (ORTHOGONAL to `FilesTableKind`) drives
`pub(super) collect_manifest_files(table, scope) -> Result<Vec<ManifestFile>>`: `CurrentSnapshot` returns
the current snapshot's manifest-list entries (empty when none — old current-snapshot behavior byte-identical);
`AllSnapshots` walks `metadata.snapshots()`, loads each `load_manifest_list`, and deduplicates `ManifestFile`s
by `manifest_path` (a `HashSet` seen-set, FIRST-SEEN order preserved for determinism). Both `files.rs` and
`entries.rs` call it; the content filter (`FilesTableKind::includes_manifest`) + `is_alive()` (files) /
no-filter (entries) stay UNCHANGED in each table's scan loop, so the row machinery + schema are byte-identical
across the scope axis. **Java lines verified:** `BaseAllMetadataTableScan.reachableManifests` (L74-86, union
over `table().snapshots()` deduped via `Sets.newHashSet` = by-path manifest dedup); `AllDataFilesTable` /
`AllFilesTable` / `AllDeleteFilesTable` / `AllEntriesTable` (each swaps the manifest source to
`dataManifests` / `allManifests` / `deleteManifests` / `allManifests`, reusing `BaseFilesTable` /
`BaseEntriesTable` schema + row machinery; javadocs "valid file = readable from ANY snapshot currently
tracked", "may return duplicate rows" = files NOT deduped). **Tests (11):**
files.rs — `test_all_files_includes_file_only_in_old_snapshot` (cross-snapshot inclusion: old-only file in
`all_files` not current `files`), `test_all_data_files_excludes_delete_files_across_snapshots` (content
filter under AllSnapshots), `test_all_delete_files_excludes_data_files_across_snapshots` (content filter
under AllSnapshots), `test_all_files_deduplicates_shared_manifest` (shared manifest read once),
`test_all_files_schema_equals_files_schema` (Arrow schema parity all_files/all_data_files/all_delete_files
== their non-all counterparts), `test_all_files_empty_table_yields_empty_batch` (no snapshots → 0 rows);
entries.rs — `test_all_entries_includes_entries_only_in_old_snapshot` (cross-snapshot inclusion),
`test_all_entries_shows_deleted_tombstone_from_old_snapshot` (tombstones across snapshots, the is_alive
distinction vs all_files), `test_all_entries_deduplicates_shared_manifest` (shared manifest's entry once),
`test_all_entries_schema_equals_entries_schema` (Arrow schema parity), `test_all_entries_empty_table_yields_
empty_batch`. Multi-snapshot fixture built on `TableTestFixture::new()` (which has a parent + current
snapshot): writes BOTH snapshots' manifest lists (the current-snapshot fixtures leave the parent list
unwritten); a SHARED manifest is referenced by both lists by the same path (its seq number stamped to the
parent's explicitly, because a manifest list only ASSIGNS a seq to a manifest it ADDED — a carried-forward
manifest must already carry one, else "Found unassigned sequence number"). **Mutations run + caught (both):**
(a) dropped the dedup seen-set (push every manifest) → both `*_deduplicates_shared_manifest` tests FAIL
(count 2 ≠ 1); (b) `AllSnapshots` reads only the current snapshot → all 3 cross-snapshot-inclusion tests +
the tombstone test FAIL (old-only file/entry/tombstone absent). Both restored after. **Gate (repo root,
pinned nightly):** `cargo build -p iceberg` clean; `cargo test -p iceberg --lib` ×2 = **1458 passed / 0
failed** both runs (was 1447; +11); `cargo test -p iceberg --test interop_manage_snapshots
--test interop_update_schema --test interop_update_partition_spec` = 4/4 each; `cargo clippy -p iceberg
--all-targets -- -D warnings` clean; `cargo fmt --all -- --check` clean. **`all_manifests` (5b) NOT built**

**REVIEW (2026-06-08, REVIEWER Opus):** PASS 🟡 with ONE test-strength fix landed in scope. Verified Java
authority myself: `BaseAllMetadataTableScan.reachableManifests` L74-86 (union over `table().snapshots()`,
`Sets.newHashSet`); the four per-table sources (`AllDataFilesTable`→`dataManifests`,
`AllFilesTable`/`AllEntriesTable`→`allManifests`, `AllDeleteFilesTable`→`deleteManifests`); and
`GenericManifestFile.equals`/`hashCode` L405-419 = `manifestPath` ONLY, so the Rust `seen_paths` HashSet is a
1:1 dedup key. CurrentSnapshot arm confirmed byte-identical (mutation: dropping a manifest from it fails the
Increment-1/2 tests). Schema-parity confirmed (`schema()` unchanged; the `all_*` schemas equal their non-all
counterparts via the schema-parity tests). Mutation sweep (all run by injection, restored after): (a) drop
dedup → both `*_deduplicates_shared_manifest` fail; (b) all→current → 3 inclusion + tombstone fail;
(d) content-filter swap under AllSnapshots → all- AND current-snapshot content tests fail; CurrentSnapshot
manifest-drop → Increment-1/2 fail. **(c) all→ANCESTRY-walk SURVIVED the builder's 11 tests** — the base
`TableTestFixture` metadata has exactly 2 snapshots (current + its parent), so "all `metadata.snapshots()`"
and "current snapshot's ancestry" are indistinguishable. FIX: added
`test_all_files_includes_file_from_non_ancestor_snapshot` (files.rs) — grafts a THIRD tracked snapshot that
is a FORK off the parent (sibling of current, NOT in its ancestry; `main` stays at current) with its own
manifest list on disk holding `fork-1.parquet`, and asserts `all_files` includes it while current `files`
does not. Verified it FAILS under the ancestry-walk mutation and PASSES on correct code. No production code
changed (manifest_source.rs byte-identical to the builder's). Re-ran gate: `cargo test -p iceberg --lib` ×2 =
**1459 passed / 0 failed** both runs (+1); 3 interop suites 4/4 each; clippy `-D warnings` clean; fmt --check
clean. Residual risk: low. The fixture now distinguishes all-vs-ancestry; the only remaining residual is the
absence of a true Java/Spark interop round-trip (deferred → ✅ as planned). The pre-existing
`iceberg-datafusion` non-exhaustive-match break the builder flagged is the orchestrator's, NOT 5a's, and was
not touched. **`all_manifests` (5b) NOT built**
(out of scope: different machinery + a `reference_snapshot_id` column). **Pre-existing issue FLAGGED (not
mine, not in scope):** `iceberg-datafusion` does NOT compile at the Increment-4 HEAD baseline — its
`table/metadata_table.rs` match on `MetadataTableType` only handles `Snapshots`/`Manifests` and is already
non-exhaustive over the `Files`/`DataFiles`/.../`Partitions` variants prior increments added; my 4 new
variants extend that already-broken match. Verified by `git stash`-ing my inspect changes and building
`-p iceberg-datafusion` (same E0004 non-exhaustive errors at HEAD). The `-p iceberg` gate never builds the
datafusion crate, so this slipped through Increments 1-4 — a human should add the missing match arms (or a
catch-all) in `crates/integrations/datafusion/src/table/metadata_table.rs` as a standalone fix.

---

### Phase 3 Increment 5b — `all_manifests` metadata table (SCOPED, 2026-06-08)
The last inspection table. Distinct machinery from the file/entry `all_*` (5a): it iterates EACH snapshot's
manifest list (NO dedup — "may return duplicate rows"), one row per (manifest × referencing snapshot), and
adds a `reference_snapshot_id` column. New file `inspect/all_manifests.rs`; wired via `inspect/mod.rs` +
`metadata_table.rs` (`MetadataTableType::AllManifests` + accessor + as_str + TryFrom) + the datafusion
provider match (both `schema()`/`scan()` arms) — datafusion now builds workspace-wide, so 5b MUST add its arm.

**Java authority — `core/.../AllManifestsTable.java` (CONFIRMED from source):**
- `MANIFEST_FILE_SCHEMA` (L57-81), in COLUMN order with ids + nullability (use VERBATIM — do NOT copy the
  regular `manifests.rs` schema, which has different nullability for some fields):
  `content`/14 int REQ, `path`/1 str REQ, `length`/2 long REQ, `partition_spec_id`/3 int OPT,
  `added_snapshot_id`/4 long OPT, `added_data_files_count`/5 int OPT, `existing_data_files_count`/6 int OPT,
  `deleted_data_files_count`/7 int OPT, `added_delete_files_count`/15 int REQ, `existing_delete_files_count`/16
  int REQ, `deleted_delete_files_count`/17 int REQ, `partition_summaries`/8 list OPT (element/9 struct:
  `contains_null`/10 bool REQ, `contains_nan`/11 bool REQ, `lower_bound`/12 str OPT, `upper_bound`/13 str OPT),
  `reference_snapshot_id`/18 long REQ, `key_metadata`/19 binary OPT.
- Row (`manifestFileToRow`, L286-303) — counts are CONTENT-GATED: `added_data_files_count =
  content==DATA ? addedFilesCount : 0`, ..., `added_delete_files_count = content==DELETES ? addedFilesCount : 0`,
  etc. `added_snapshot_id = manifest.snapshotId()`. `reference_snapshot_id = the snapshot whose manifest list
  this row came from`. `partition_summaries` via the same FieldSummary→(contains_null/nan, lower/upper bound as
  STRING) conversion `manifests.rs` already does. `key_metadata = manifest.keyMetadata()` (raw bytes).
- Iteration (`doPlanFiles`, L122-158): for EACH snapshot in `table().snapshots()` (no filter ⇒ ALL), read that
  snapshot's manifest list, emit one row per manifest tagged with that snapshot's id. NOT deduplicated.

**Rust mapping:** iterate `metadata.snapshots()` directly (the 5a `manifest_source::collect_manifest_files`
helper is NOT reusable here — it dedups AND drops the snapshot identity; all_manifests needs per-snapshot
(manifest, ref_snapshot_id) pairs WITHOUT dedup). Per snapshot: `snapshot.load_manifest_list(file_io,
metadata)` → `ManifestFile{ content, manifest_path, manifest_length, partition_spec_id, added_snapshot_id,
added_files_count/existing_files_count/deleted_files_count (Option<u32>), partitions (Option<Vec<FieldSummary>>),
key_metadata (Option<Vec<u8>>) }`; `snapshot.snapshot_id()` = reference_snapshot_id. Reuse the
partition-summary builder/conversion from `manifests.rs` (factor a `pub(super)` shared helper if convenient —
in scope; else replicate carefully). NEW code follows the engineering floor (no bare `.unwrap()` in production;
`.expect("…statically valid")` only for the schema build) even though the OLD `manifests.rs` has pre-existing
`.unwrap()`s.

**FLAG / decide (verify against `ManifestsTable.java` AND `AllManifestsTable.java`):** the existing
`manifests.rs` scan (L173-183) appends the manifest's added/existing/deleted counts to BOTH the data-count
columns AND the delete-count columns UNCONDITIONALLY (no content gating). Java content-gates. Determine whether
the regular `manifests` table is genuinely buggy (Java `ManifestsTable` content-gates too). Implement
`all_manifests` CORRECTLY (content-gated) regardless. For the regular table: if the fix is a small, confirmed,
shared-helper change, fixing it (and updating its `expect_test`) is acceptable and preferred for consistency;
otherwise FLAG it for the orchestrator with the Java evidence — do NOT leave all_manifests matching the buggy
behavior.

Plan:
- [x] `inspect/all_manifests.rs`: `AllManifestsTable` + `schema()` (14 cols per Java AllManifestsTable) + async
      `scan()` (per-snapshot iteration, content-gated counts, reference_snapshot_id, key_metadata).
- [x] Wire `MetadataTableType::AllManifests` + `all_manifests()` accessor + as_str/TryFrom in
      `metadata_table.rs`; `mod` + `pub use` in `mod.rs`; the datafusion provider arm in BOTH matches.
- [x] Tests (in-crate, multi-snapshot fixture): per-snapshot duplication (a manifest in 2 snapshots → 2 rows
      with different reference_snapshot_id, NOT deduped); content-gated counts (a DELETE manifest has 0 in the
      data-count columns and vice-versa — mutation-pin the gating); reference_snapshot_id correctness;
      key_metadata present/null; partition_summaries (lower/upper bound strings); Arrow schema cols+ids+
      nullability incl. reference_snapshot_id/18 + key_metadata/19; empty table → 0 rows.
- [x] Docs: GAP_MATRIX inspection row (inspection-table set COMPLETE), Roadmap Phase 3, todo, lessons; note
      the manifests content-gating finding.
- [x] Verify from repo root: build `-p iceberg` AND `--workspace --exclude iceberg-sqllogictest`; lib ×2; 3
      interop suites; datafusion tests; clippy (workspace); fmt --check.

**Outcome (2026-06-08, Phase 3 Increment 5b, BUILDER Opus):** `all_manifests` — the LAST inspection table —
lands 🟡; the inspection-table SET IS NOW COMPLETE. New file `inspect/all_manifests.rs` (`AllManifestsTable`
+ `schema()` + per-snapshot `scan()`): iterates `metadata.snapshots()` directly (NO dedup — Java javadoc
"may return duplicate rows"), one row per (manifest × referencing snapshot) tagged with
`reference_snapshot_id`, CONTENT-GATED counts (a DATA manifest → *_data_files_count columns, delete columns
0; a DELETE manifest the reverse), `key_metadata` raw bytes, partition summaries via the reused converter.
Schema is the 14-field Java `AllManifestsTable.MANIFEST_FILE_SCHEMA` verbatim (ids + nullability — note the
three `*_delete_files_count` columns + `contains_nan` are REQUIRED here, unlike the regular `manifests`
table). Wired `MetadataTableType::AllManifests` (+ as_str/TryFrom/EnumIter `all_types`) +
`MetadataTable::all_manifests()` + `mod`/`pub use` in `inspect/mod.rs` + the datafusion provider arm in BOTH
`schema()`/`scan()` matches.

**CONTENT-GATING DECISION — FIXED the regular `manifests` table (surgical, shared-helper).** Verified against
BOTH Java files: `AllManifestsTable.manifestFileToRow` (L286-303) AND `ManifestsTable.manifestFileToRow`
(L108-113) content-gate IDENTICALLY (`manifest.content() == ManifestContent.DATA ? count : 0` for data
columns, `== DELETES ? count : 0` for delete columns). The Rust `inspect/manifests.rs` scan (old L173-183)
appended each manifest's added/existing/deleted counts to BOTH the data AND delete column families
UNCONDITIONALLY — a genuine bug vs Java. The fix is a small, confirmed `is_data` gate (the SAME logic
`all_manifests` uses), so per the brief's "prefer fixing if surgical" I fixed it and regenerated its
`expect_test` block via `UPDATE_EXPECT=1` (the fixture's DATA manifest now reports 0/0/0 in the delete
columns; diff inspected — only those three columns changed). Both tables now content-gate consistently.

**Java lines verified:** schema = `AllManifestsTable.java` L57-81 (`MANIFEST_FILE_SCHEMA` + `REF_SNAPSHOT_ID`
L53-54); content-gated row logic = `manifestFileToRow` L286-303 (+ `ManifestsTable.java` L108-113 for the
regular table); per-snapshot iteration = `doPlanFiles` L122-158 (iterate `table().snapshots()`, one row per
manifest per snapshot, no dedup). Shared partition-summary builder/conversion factored into new
`inspect/partition_summary.rs` (`pub(super)` fns, Rule of Three) used by both `manifests.rs` and
`all_manifests.rs`; the new code carries no bare `.unwrap()` (errors propagated; `.expect(...)` only for the
statically-valid schema build), and I upgraded the touched `manifests.rs` schema `.unwrap()` → `.expect(...)`
in passing.

**Tests (7, each named for its risk):**
- `test_all_manifests_does_not_dedup_shared_manifest_across_snapshots` — non-dedup: a manifest in 2 snapshots
  → 2 rows with different `reference_snapshot_id` (dedup OR current-snapshot-only read collapses it).
- `test_all_manifests_content_gates_counts_per_manifest_content` — DATA manifest's counts only in data
  columns (delete 0), DELETE manifest's only in delete columns (data 0).
- `test_all_manifests_reference_snapshot_id_distinct_from_added_snapshot_id` — the shared manifest's
  `added_snapshot_id` is PARENT while its current-snapshot row's `reference_snapshot_id` is CURRENT.
- `test_all_manifests_key_metadata_present_when_set_null_when_absent` — DELETE manifest's bytes present;
  DATA manifest's null.
- `test_all_manifests_renders_partition_summary_bounds_as_strings` — lower/upper bounds render to "100"/"300".
- `test_all_manifests_arrow_schema_columns_ids_and_nullability` — 14 cols in order, exact ids + nullability
  incl. reference_snapshot_id/18 (req) + key_metadata/19 (opt, LargeBinary).
- `test_all_manifests_empty_table_yields_no_rows` — 0 rows, 14-col shape, no panic.

**Mutations run (all caught):** (a) un-gate the counts (write into both data+delete families) → content-gating
test FAILS; (b) read only the current snapshot → non-dedup test FAILS; (b') dedup by `manifest_path`
(5a-style) → non-dedup test FAILS; (c) emit `added_snapshot_id` for `reference_snapshot_id` → ref-id test
FAILS; plus un-gating the regular `manifests.rs` scan → its `expect_test` block FAILS. Each restored after.

**Verification gate (all clean, from repo root):** `cargo build -p iceberg` ✅; `cargo build --workspace
--exclude iceberg-sqllogictest --all-targets` ✅; `cargo test -p iceberg --lib` ×2 = **1466 passed / 0
failed** both runs (was 1458 baseline → +7 all_manifests tests + 1 net from the shared-helper refactor;
stable); 3 interop suites (`interop_manage_snapshots` / `interop_update_schema` /
`interop_update_partition_spec`) = 4/4 each ✅; `cargo test -p iceberg-datafusion` lib+integration ✅ (after
regenerating the `test_provider_list_table_names` expect block — one additive `all_manifests` line; the only
datafusion doctest failure is the PRE-EXISTING `table_provider_factory` tokio-flavor one, unrelated);
`cargo test --doc --workspace --exclude iceberg-sqllogictest` green ✅; `cargo clippy --workspace --exclude
iceberg-sqllogictest --all-targets -- -D warnings` clean ✅; `cargo fmt --all -- --check` clean ✅. No commit,
no branch switch, no Cargo/spec/arrow/scan-production edits.

#### Phase 3 Increment 5b — REVIEW (2026-06-08, Opus REVIEWER, DELEGATED) — CHANGES-MADE
Verdict: **CHANGES-MADE** (one production-panic bug fixed in scope; everything else PASS). Independently
re-verified all 14 all_manifests field ids+nullability against `AllManifestsTable.MANIFEST_FILE_SCHEMA`
(L57-81) — all match Java. Confirmed BOTH Java tables content-gate identically (`AllManifestsTable.java`
L294-299 + `ManifestsTable.java` L108-113, both `content()==DATA ? n : 0` / `==DELETES ? n : 0`), so the
builder's `manifests.rs` content-gating fix is CORRECT + MINIMAL (schema unchanged, only row values; its
`expect_test` regen reflects the fix — DATA manifest's delete columns now 0/0/0 — and is mutation-pinned).
Per-snapshot/non-dedup/ref-id verification all hold; ran mutations (a) un-gate, (b) current-only, (d)
ref-id=added_snapshot_id, (e) un-gate manifests.rs — every one fails the matching test. Scope clean (git
modified set == declared scope; no Cargo/spec/arrow/scan-production touched). The pre-existing
`iceberg-datafusion` `table_provider_factory` doctest failure is environmental (`rt-multi-thread` disabled),
unrelated, file untouched — confirmed.

**BUG FOUND + FIXED — `contains_nan` production panic.** The builder copied Java's `contains_nan`/11 nullability
VERBATIM as REQUIRED. But Java's `PartitionFieldSummary.containsNaN()` returns a NULLABLE `Boolean` that is
`null` when the manifest has no NaN info (V1 manifests, or V2 without it — `contains_nan`/518 is OPTIONAL in
the on-disk manifest-list spec, `manifest_list.rs:572`), and Java's loosely-typed `StaticDataTask.Row` emits
that null into the nominally-`required` cell WITHOUT enforcement. Arrow ENFORCES non-nullability at
array-build time: the shared `append_partition_summaries` does `append_option(summary.contains_nan)`, so when
`contains_nan == None`, `partition_summaries.finish()` PANICS — `InvalidArgumentError("Found unmasked nulls
for non-nullable StructArray field \"contains_nan\"")` — crashing `AllManifestsTable::scan()`. The builder's
7 tests all missed this because the test fixture's `ManifestWriterBuilder` always defaults `contains_nan` to
`Some(false)`. FIX (in scope, `inspect/all_manifests.rs`): made `contains_nan`/11 OPTIONAL — faithfully
carries Java's emitted null when unset, and is consistent with the regular `manifests` table (whose Java
schema `ManifestsTable.java` L51 ALSO marks `contains_nan`/11 optional). Added
`test_all_manifests_renders_null_contains_nan_without_panicking` (writes a manifest whose summary has
`contains_nan == None`, scans, asserts a NULL cell; mutation-pinned — reverting to `required` re-triggers the
exact panic). Updated the schema doc-comment + GAP_MATRIX to document the divergence. This is a deliberate,
documented deviation from Java's NOMINAL `required` flag (which Java never enforces); correctness > nominal
metadata parity. lib total **1467** (was 1466; +1 net regression test, probe removed). Full gate (all 7)
re-run clean after the fix.

---

## SEQUENCE: ResidualEvaluator + filter-based conflict validation (Phase 3, started 2026-06-08)
Branch `phase3-residual-eval` (off merged `main` `a31657dd`). The inspection-table set is COMPLETE; this
sequence builds the residual-evaluation gap + the deferred Phase-2 filter-based conflict validation it unblocks.

**Why:** Java `api/.../expressions/ResidualEvaluator.java` is ABSENT in Rust. It partially-evaluates a row
filter against a partition's values → the residual predicate. Needed for (a) correct per-task scan residuals
(`FileScanTask` today carries the FULL bound predicate, not the partition-reduced residual) and (b) the
DEFERRED `OverwriteFiles`/`RowDelta` `validateNoConflictingData` (filter-based concurrent-commit safety).

Infrastructure that already exists (the build leans on it): `BoundPredicateVisitor` trait
(`expr/visitors/bound_predicate_visitor.rs`); `Transform::project` (inclusive) + `Transform::strict_project`
(strict) in `spec/transform.rs`; the `ExpressionEvaluatorVisitor` pattern (`expr/visitors/expression_evaluator.rs`)
that evaluates a bound predicate against a partition `Struct` → bool; `Predicate::{AlwaysTrue,AlwaysFalse}` +
`BoundPredicate::{AlwaysTrue,AlwaysFalse}`; `PartitionField.source_id` for the by-source lookup; the scan
already carries a `partition_bound_predicate` (`scan/mod.rs`).

### Increment 1 — ResidualEvaluator core (THIS increment)
New `crates/iceberg/src/expr/visitors/residual_evaluator.rs`. Mirror Java `ResidualEvaluator`:
- `ResidualEvaluator` holding `(partition_spec, filter, case_sensitive)`; constructors `unpartitioned(filter)`
  (residual is ALWAYS the filter unchanged) and `of(spec, filter, case_sensitive)` (→ unpartitioned when the
  spec has no fields).
- `residual_for(&self, partition: &Struct) -> Result<Predicate>` — the residual visitor:
  - and/or/not → recurse + combine; alwaysTrue/alwaysFalse pass through.
  - leaf unary/binary/set predicate on a partition-SOURCE column: get the partition fields with
    `source_id == ref.field_id()`; for each, compute `strict_project` (if its evaluation against the partition
    struct is TRUE → residual `AlwaysTrue`) and `project` (inclusive; if FALSE → residual `AlwaysFalse`); else
    keep the original predicate. (Java `predicate()` L227-288.) Evaluate the projected (partition-bound)
    predicate against the partition struct via the `ExpressionEvaluatorVisitor` pattern.
  - leaf on a non-partition column → keep the predicate (can't reduce).
- Tests: the Javadoc `day(ts)` example's 4 residual cases (`d>day(a)&&d<day(b)`→true; `d==day(a)`→`ts>=a`;
  `d==day(b)`→`ts<=b`; `d==day(a)==day(b)`→both); identity partition (eq reduces to alwaysTrue/false);
  bucket partition (non-reducible → keeps predicate); unpartitioned (returns filter verbatim); alwaysTrue/
  alwaysFalse filters; a predicate on a non-partition column (kept). Mutation-pin strict-vs-inclusive.

Plan / checkboxes (Increment 1):
- [x] New `expr/visitors/residual_evaluator.rs`: `ResidualEvaluator { spec, filter (BoundPredicate),
      partition_schema, case_sensitive }` + `unpartitioned(filter)` + `of(spec, schema, filter, case_sensitive)`.
- [x] `residual_for(&self, partition: &Struct) -> Result<Predicate>` via a `ResidualVisitor`
      (`BoundPredicateVisitor<T = Predicate>`): and/or/not via the simplifying combinators; leaf →
      strict-true ⇒ `AlwaysTrue`, inclusive-false ⇒ `AlwaysFalse`, else keep the original (bound→unbound).
- [x] Register `pub(crate) mod residual_evaluator;` in `expr/visitors/mod.rs`.
- [x] Tests (in-crate): 4 day-example cases + identity + bucket + non-partition + unpartitioned + alwaysTrue/
      alwaysFalse + and/or/not mixes; mutation-pin strict↔inclusive and partition-ignore.
- [x] Docs: GAP_MATRIX residual row, Roadmap Phase 3, todo Outcome, lessons.
- [x] Gate: build -p iceberg / workspace build / lib ×2 / 3 interop / clippy / fmt.

### Increment 2 — wire residuals into scan FileScanTask
Compute the per-task residual during planning so each `FileScanTask` carries the partition-reduced residual
(Java parity), connecting to the existing `partition_bound_predicate` machinery in `scan/mod.rs`. Guard
behavior-equivalence (residual ⊆ original filter; row-level filtering must not change results).

### Increment 3 — OverwriteFiles (+ RowDelta) filter-based validateNoConflictingData
Consume the residual/metrics evaluators to implement the deferred filter-based conflict validation, building
on the Phase-2 foundation (`Transaction.starting_snapshot_id` + `TransactionAction::validate` +
`added_data_files_after`; `ReplacePartitions.validate_no_conflicting_data` already wired). Scope precisely
against Java `MergingSnapshotProducer.validateAddedDataFiles` / `OverwriteFiles` when reached.

**Outcome (2026-06-08, Phase 3 Increment 1 — ResidualEvaluator core, BUILDER Opus):** `ResidualEvaluator`
landed 🟡 in the new `crates/iceberg/src/expr/visitors/residual_evaluator.rs`. It partially evaluates a bound
row filter against a partition's `Struct` and returns the residual — the part of the filter NOT already
decided by the partition.

**Type decisions (deliberate divergences from Java, documented in the module + GAP_MATRIX):**
- Java's `ResidualEvaluator` holds one `Expression expr` (the bound/unbound union) and `residualFor` returns
  an `Expression`. The Rust port holds the filter as a `BoundPredicate` (the scan binds before planning) and
  returns the residual as an UNBOUND `Predicate`. Signature:
  `ResidualEvaluator::of(spec: PartitionSpecRef, schema: &Schema, filter: BoundPredicate, case_sensitive: bool)
  -> Result<Self>`, `ResidualEvaluator::unpartitioned(filter: BoundPredicate) -> Self`,
  `residual_for(&self, partition: &Struct) -> Result<Predicate>`.
- `of` takes the table `schema` (Java's `PartitionSpec` carries its schema; the Rust one does not), used once
  in the ctor to compute `spec.partition_type(schema)` → a partition `Schema` the projected predicates bind to.
- For the leaf "keep the original predicate" case Java returns the original BOUND `pred`; the Rust port
  reconstructs the unbound `Predicate` from the bound leaf by the source field's NAME
  (`bound_to_unbound` + `unbound_reference`) — partition-source columns are top-level schema fields, so the
  field name is the unbound reference name.
- Visibility `pub(crate)` (Increments 2/3 are in-crate consumers); `#![allow(dead_code)]` until they land.

**Residual algorithm (mirrors Java `ResidualVisitor`):** a `BoundPredicateVisitor<T = Predicate>`. and/or →
the simplifying `Predicate::{and,or}` combinators (so AlwaysTrue/AlwaysFalse short-circuit like
`Expressions.and/or`); not → a Java-`Expressions.not`-faithful `simplifying_not` (`not(true)=false`,
`not(false)=true`, `not(not x)=x`, else wrap) because the `Predicate` `!` operator does NOT simplify
constants. Every leaf method routes to `reduce_leaf(reference, predicate)`: find the partition fields with
`source_id == reference.field().id`; if none, keep the predicate; otherwise for each part compute
`Transform::strict_project` (bind to the partition schema, evaluate against the partition via
`ExpressionEvaluatorVisitor` → if TRUE, return `AlwaysTrue`) then `Transform::project` (inclusive; if FALSE,
return `AlwaysFalse`); if neither is conclusive, keep the original predicate. An `AlwaysTrue`/`AlwaysFalse`
*projection* short-circuits to its constant (Java's "if the result is not a predicate it must be a constant").

**Reuse:** made `ExpressionEvaluatorVisitor` + its `new` `pub(crate)` in `expression_evaluator.rs` (the ONLY
edit to that file — the brief's sanctioned small helper) so the residual evaluator reuses the exact
per-operator bound-predicate-against-partition-`Struct` evaluation rather than duplicating it.

**Java lines verified:** `ResidualEvaluator.unpartitioned` L73-75 + `UnpartitionedResidualEvaluator` L53-65
(residual is always the whole expr); `of` L84-90 (empty spec → unpartitioned); the leaf methods L146-223
(each evaluates the bound ref against the partition struct → alwaysTrue/alwaysFalse); the load-bearing
`predicate(BoundPredicate)` L227-288 (strict-projection-true ⇒ alwaysTrue; inclusive-projection-false ⇒
alwaysFalse; else keep `pred`); and `Expressions.not` L63-73 (the constant-folding negation).

**Tests (16, each named for its risk):** the 4 Javadoc `day(utc_timestamp)` cases
(`*_strictly_between_bounds_reduces_to_always_true`, `*_equals_lower_bound_keeps_lower_predicate_only`,
`*_equals_upper_bound_keeps_upper_predicate_only`, `*_equals_both_bounds_keeps_both_predicates`) — the
headline reductions; `*_identity_partition_eq_matching_value_reduces_to_always_true` /
`*_non_matching_value_reduces_to_always_false` (identity eq collapses by the partition value);
`*_bucket_partition_keeps_predicate_unchanged` (non-invertible transform → predicate survives; the partition
value is computed as the real `bucket(5,16)` via `transform_literal` so the inclusive projection is true and
strict is absent); `*_predicate_on_non_partition_column_is_kept`;
`*_unpartitioned_spec_returns_filter_verbatim` (empty spec via `of`) + `*_unpartitioned_constructor_*`;
`*_always_true_filter_passes_through` / `*_always_false_*`;
`*_and_of_reducible_and_non_reducible_keeps_only_the_non_reducible`,
`*_and_short_circuits_to_false_when_partition_excludes_the_partition_leaf`,
`*_or_of_reducible_true_and_non_reducible_short_circuits_to_true`,
`*_not_over_partition_leaf_negates_the_reduced_constant` (and/or/not mixes of a reducible + a non-reducible
leaf).

**Mutations run (each caught):** (a) swap `strict_project`↔`project` in `reduce_leaf` → 4 reduction tests
FAIL (the three "equals one bound" day cases + the bucket-keep). (b) make `residual_for` ignore the partition
(always `bound_to_unbound(filter)`) → 9 reduction tests FAIL (all day reductions + identity + the and/or/not
combinations); the 7 "kept verbatim" tests legitimately still pass. Both restored after.

**Gate (all clean, from repo root, pinned nightly):** `cargo build -p iceberg` ✅; `cargo build --workspace
--exclude iceberg-sqllogictest --all-targets` ✅; `cargo test -p iceberg --lib` ×2 = **1483 passed / 0 failed**
both runs (was 1467 → +16 residual tests); 3 interop suites (`interop_manage_snapshots` /
`interop_update_schema` / `interop_update_partition_spec`) = 4/4 each ✅; `cargo clippy --workspace --exclude
iceberg-sqllogictest --all-targets -- -D warnings` clean ✅ (collapsed two `if let { if }` into let-chains);
`cargo fmt --all -- --check` clean ✅. No commit, no branch switch, no Cargo/spec/scan/transaction edits.
Files: NEW `expr/visitors/residual_evaluator.rs`; `expr/visitors/mod.rs` (`pub(crate) mod residual_evaluator;`);
`expr/visitors/expression_evaluator.rs` (`ExpressionEvaluatorVisitor` + `new` → `pub(crate)`); docs
(GAP_MATRIX/Roadmap/todo/lessons).

#### Phase 3 Increment 1 — REVIEW (2026-06-08, Opus REVIEWER, DELEGATED)
Adversarially verified the ResidualEvaluator port against the Java source (`/tmp/iceberg-java-ref`) and by
running 7 code mutations. Findings:
- **Strict/inclusive direction: CONFIRMED CORRECT (not swapped).** Java `predicate(BoundPredicate)`
  L248-262: `projectStrict` result `op()==TRUE` ⇒ `alwaysTrue`; L265-283: `project` (inclusive) result
  `op()==FALSE` ⇒ `alwaysFalse`. Rust `reduce_leaf` uses `strict_project`→AlwaysTrue then `project`
  (inclusive)→AlwaysFalse — same direction. Swap mutation fails 4-6 tests (the 3 day "equals one bound" +
  bucket-keep, now also truncate + year).
- **The 4 day-residual cases: asserted EXACTLY** (strictly-between→`AlwaysTrue`; `==day(a)`→`ts>=a`;
  `==day(b)`→`ts<=b`; `==both`→`ts>=a AND ts<=b`), not weakened to "not AlwaysTrue".
- **Untested transform classes: VERIFIED + PINNED.** Exercised truncate / year / void through `residual_for`
  (throwaway probes): all reduce correctly (truncate all-three-ways; year keeps-both-inside + AlwaysFalse-
  outside; void KEEPS — Java `VoidTransform` returns null for both projections, no panic). Added permanent
  pinning tests for all three.
- **Leaf reconstruction round-trip: VERIFIED + PINNED** for set (`in`/`not_in`) and unary (`is_null`) on a
  non-partition column (builder only covered binary). `not(in)` correctly round-trips as `Not(In{..})` (the
  binder keeps the `Not` wrapper, not a `NotIn` operator) — faithful, not a bug.
- **Combinator simplification: CONFIRMED.** `Predicate::and`/`or` constant-fold (predicate.rs L563-599);
  `simplifying_not` mirrors Java `Expressions.not` L63-73 EXACTLY (not(true)=false/not(false)=true/
  not(not x)=x, else wrap). The `!` operator does NOT fold, so `simplifying_not` is load-bearing.
- **Multi-field-per-source loop: PINNED.** A spec with bucket(category)+identity(category) forces the loop
  to continue past the inconclusive bucket field; break-after-first mutation now fails.
- **Mutations run (all caught):** (a) strict↔inclusive swap; (b) ignore-partition; (c) drop inclusive-false;
  (d) drop strict-true; (e) break `not` folding; (f) drop negation in `bound_to_unbound` `Not` arm
  [SURVIVED the builder's 16 tests — a real gap, now closed]; (g) break-after-first in the parts loop
  [closed].
- **Scope + floor: CLEAN.** Production code byte-identical to the builder's (only test additions);
  `expression_evaluator.rs` is ONLY the visibility bump; `#![allow(dead_code)]` is module-scoped; no bare
  `.unwrap()` in production paths; no Cargo/spec/scan/transaction edits; no commit/branch switch.

**Review outcome:** CHANGES-MADE — added 8 tests (16→24); closed two genuine test-strength gaps (the
unpartitioned-`Not` reconstruction and the multi-field-per-source loop) the builder's suite did not catch.
**Verify:** build -p iceberg clean; workspace build clean; lib ×2 = 1491/0 (was 1483 → +8); 3 interop
suites 4/4 each; clippy -D warnings clean; fmt clean. Row stays 🟡 (scan-wiring Inc 2 + conflict-validation
Inc 3 + Java interop still pending). Files touched: `residual_evaluator.rs` (tests only), todo, lessons.

### Increment 2 — wire the ResidualEvaluator into scan FileScanTask (DETAIL, 2026-06-08)
Java `BaseFileScanTask.residual()` = `residuals.residualFor(file.partition())` — each task carries the
PARTITION-REDUCED residual, and the reader filters rows with it. Today Rust sets `FileScanTask.predicate` to
the FULL `snapshot_bound_predicate` (`scan/context.rs::into_file_scan_task`), and `partition_spec` is `None`
(a TODO). The ONLY runtime consumer of `task.predicate` is `arrow/reader.rs` (row filtering); datafusion does
not read it.

Plan:
- [x] Thread the file's `Arc<PartitionSpec>` into `ManifestEntryContext` / `into_file_scan_task` (resolve the
      `partition_spec: None` TODO) — resolved in `PlanContext::create_manifest_file_context` via
      `table_metadata.partition_spec_by_id(manifest_file.partition_spec_id)`. Set `partition_spec: Some(spec)`
      on the task too.
- [x] In `into_file_scan_task`, when there is a filter: compute the residual via the per-manifest
      `ResidualEvaluator`, BIND the resulting unbound `Predicate` to `snapshot_schema`, and store it as
      `predicate`. No filter ⇒ `predicate: None`; unpartitioned spec ⇒ residual == the full filter (unchanged
      behavior). The evaluator is constructed ONCE per manifest file (the spec + snapshot filter are constant
      within a manifest) and shared across its entries — the per-spec cache pattern, but per-manifest, so no
      shared mutable cache was needed.
- [x] Reader UNCHANGED (it already uses `task.predicate` for row filtering; now it's the residual — and
      `task.partition_spec` for identity-partition constants, which the threading newly activates).
- [x] Tests (CRITICAL — prove RESULT-EQUIVALENCE + genuine reduction): 11 scan-wiring tests on identity AND
      truncate transform partitions; reduced-residual (not full filter); fully-implied → `AlwaysTrue`; spec
      populated; result-equivalence reads; unpartitioned/no-filter unchanged; partition-mismatch pruning.
      Mutations caught: (a) leave `predicate`=full filter → 3 reduction tests fail; (b) residual against the
      WRONG (empty) partition → all 5 residual tests fail.
- [x] Docs: GAP_MATRIX residual/scan row (scan wiring done), Roadmap, todo, lessons.

**Outcome (2026-06-08, Phase 3 Increment 2 — residual scan-wiring, BUILDER Opus):** each `FileScanTask` now
carries the PARTITION-REDUCED residual of the scan filter, not the full snapshot filter — Java
`BaseFileScanTask.residual()` parity (`residuals.residualFor(file.partition())`). **What I threaded:** the
file's `Arc<PartitionSpec>` (`table_metadata.partition_spec_by_id(manifest_file.partition_spec_id)`) and an
`Arc<ResidualEvaluator>`, both added to `ManifestFileContext` (built in
`PlanContext::create_manifest_file_context`, which has `table_metadata`) and propagated to each
`ManifestEntryContext`. **Where the residual is computed:** `ManifestEntryContext::into_file_scan_task` calls a
new `residual_predicate()` helper — it runs the per-manifest evaluator's `residual_for(file.partition())`,
binds the unbound residual `Predicate` back to the snapshot schema, and stores the `BoundPredicate` as
`task.predicate`. The evaluator is built ONCE per manifest file (spec + filter constant within a manifest) and
shared across entries — the cleaner choice over a shared mutable cache, since each manifest already maps to one
spec id. `partition_spec: None` → `Some(spec)` resolves the TODO. **Result-equivalence argument:** every row in
a data file belongs to that file's single partition tuple, so the partition-implied conditions the residual
drops are TRUE for all rows in the file; filtering with the residual therefore selects exactly the rows the
full filter would — result-preserving, only cheaper. Stated in an `into_file_scan_task` comment and proven by
the equivalence read tests (identity + truncate). **Java lines verified:** `BaseFileScanTask.residual()` =
`residuals.residualFor(file.partition())` (the per-task residual is the partition-reduced one); the
`ResidualEvaluator` semantics were verified in Increment 1. **The one unforeseen interaction (flagged):**
threading `task.partition_spec` ALSO activates the arrow reader's identity-partition constant materialization
(`reader.rs:451-455` → `record_batch_transformer::constants_map`, Java `PartitionUtil.constantsMap`), which was
dormant while the field was `None`. This is CORRECT Iceberg behavior but exposed a pre-existing inconsistency in
the shared `TableTestFixture` (partition `x`=100/200/300 vs the parquet `x` column `[1; 1024]`). I made the
fixture consistent (partition `x`=1, matching the data) — a test-only change in `scan/mod.rs` — which is the
truthful resolution; no read test prunes on `x`, so nothing else changes. This rippled one expect-snapshot in
`inspect/manifests.rs` (partition-summary bounds `"100"`/`"300"` → `"1"`/`"1"`, mechanical) — OUTSIDE the
allowed-edit set, flagged here and in the final report. **Test list (11, each pins a risk):**
`test_task_predicate_is_partition_reduced_residual_not_full_filter` (residual reduced, not full filter —
mutation (a) target); `test_task_predicate_residual_is_always_true_when_filter_fully_implied` (filter fully
implied by partition → `AlwaysTrue`); `test_task_partition_spec_is_populated_from_table_metadata` (TODO
resolved); `test_residual_scan_result_equivalent_to_full_filter_identity_partition` (RESULT-EQUIVALENCE read,
identity); `test_unpartitioned_table_keeps_full_filter_as_task_predicate` (no reduction on unpartitioned);
`test_no_filter_scan_leaves_task_predicate_none` (no-filter unchanged + spec still threaded);
`test_filter_excluding_the_partition_prunes_the_file_entirely` (partition mismatch → file pruned);
`test_task_predicate_residual_reduced_on_transform_truncate_partition` (NON-identity transform residual
reduced); `test_residual_scan_result_equivalent_to_full_filter_truncate_partition` (RESULT-EQUIVALENCE read,
truncate transform — x read from data, not constant). Plus the 8 pre-existing scan read tests updated to decode
the now-run-encoded identity-partition `x` constant (new `decode_int64_column` helper), and the
`test_select_with_repeated_column_names` type assertions (`x` is `RunEndEncoded`, like `_file`).
**Mutations run (each caught):** (a) `residual_predicate` returns the full `snapshot_bound_predicate` →
`test_task_predicate_is_partition_reduced_*` + `*_residual_is_always_true_*` +
`*_residual_reduced_on_transform_truncate_*` all FAIL; (b) `residual_for(&Struct::empty())` (wrong partition) →
all 5 residual tests FAIL (3 panic in the accessor, 2 wrong-result). Both restored; production byte-identical
after. **Removed** the module-scoped `#![allow(dead_code)]` from `residual_evaluator.rs` (now consumed in
production — clippy `-D warnings` confirms every item is reachable). **Gate (all from repo root, pinned
nightly):** `cargo build -p iceberg` ✅; `cargo build --workspace --exclude iceberg-sqllogictest --all-targets`
✅; `cargo test -p iceberg --lib` ×2 = **1500 passed / 0 failed** both runs (was 1491 → +9 residual-wiring
tests, net; the 8 updated read tests + 1 inspect snapshot are edits not adds); 3 interop suites
(`interop_manage_snapshots`/`interop_update_schema`/`interop_update_partition_spec`) = 4/4 each ✅; `cargo
clippy --workspace --exclude iceberg-sqllogictest --all-targets -- -D warnings` clean ✅; `cargo fmt --all --
--check` clean ✅. Existing scan/reader filtered-read tests all still pass. No commit, no branch switch, no
Cargo/spec/transaction edits; `arrow/reader.rs` behavior unchanged (only the now-active partition-constant path
it already contained). **Files:** `scan/context.rs` (spec + residual threading, residual computation),
`scan/mod.rs` (fixture consistency + truncate fixture + 9 new tests + 8 read-test decode updates),
`expr/visitors/residual_evaluator.rs` (removed `allow(dead_code)` + doc), `inspect/manifests.rs` (forced
expect-snapshot ripple), docs (GAP_MATRIX/Roadmap/todo/lessons).
- [x] Verify from repo root: build -p iceberg + workspace; lib ×2; 3 interop; clippy (workspace); fmt.

#### Phase 3 Increment 2 — REVIEW (2026-06-08, Opus REVIEWER, DELEGATED)
Adversarially verified residual result-equivalence and scope against the Java source + 3 code mutations.
**VERDICT: CHANGES-MADE.**
- **Residual result-equivalence: CONFIRMED.** The residual is computed from the FILE's own spec
  (`partition_spec_by_id(manifest_file.partition_spec_id)`, resolved per manifest), evaluated against the
  FILE's own `data_file().partition()`, then BOUND BACK to the snapshot schema — exactly Java
  `BaseFileScanTask.residual()` = `residuals.residualFor(file.partition())`. The data-matches-partition
  invariant (the original fixture VIOLATED it: data `x`=1, partition `x`=100/200/300) makes the
  fixture-consistency fix genuinely NECESSARY, not optional — confirmed independently.
- **Fixture fix DECISION: KEPT the collapse (3→1 partition).** Investigated the multi-partition alternative
  and REJECTED it: the shared fixture writes BYTE-IDENTICAL parquet data to all three files via one
  `for n in 1..=3` loop (only 2 are live — 2.parquet is the Deleted entry), so keeping 100/200/300 would
  require rewriting `write_parquet_data_files` to emit per-file distinct `x` data and rippling ~8 read tests —
  disproportionate. Crucially, NO coverage is lost: the base `scan/mod.rs` had ZERO tests filtering on the
  partition column `x` with 100/200/300, and the dedicated multi-partition inspect tests
  (`all_manifests`/`entries`/`files`/`partitions`) build their OWN inline 3-partition manifests (untouched,
  60/60 green). The collapse is adequate and the better fix is disproportionately heavy.
- **Constants-map activation (`partition_spec=Some`) DECISION: KEPT, justified.** Verified by REAL reads:
  `record_batch_transformer::constants_map` materializes ONLY identity-transform fields (line 60-61); the
  identity fixture reads `x` as a RunEndEncoded constant from partition metadata (value 1) and the truncate
  fixture reads `x` from the data file (non-identity) — both confirmed by equivalence read tests. `arrow/reader.rs`
  is byte-unchanged (not in the diff). Correct Iceberg behavior (Java `PartitionUtil.constantsMap`), resolves a
  real TODO, fully pinned — KEEP over DEFER.
- **inspect/manifests.rs ripple: BENIGN.** Mechanical consequence of the (necessary) partition-value change on
  the SAME manifest the scan reads; the partition-summary bound is genuinely now 1. Not a masked bug.
- **Mutations run (3):** (a) store full filter instead of residual → CAUGHT (4 tests, incl. the identity
  equivalence read via its pre-read reduced-residual assertion); (b) residual vs EMPTY partition → CAUGHT
  (5 tests, incl. BOTH equivalence reads); (c) use the table DEFAULT spec instead of the file's own
  `partition_spec_id` → **SURVIVED** the builder's suite (fixture had only one spec) — a real test-strength
  gap (the exact silent partition-evolution data bug). **CLOSED** by a new pinning test.
- **Test added:** `test_residual_uses_files_own_spec_not_the_table_default_spec` — a 2-spec fixture
  (`new_with_evolved_default_spec` + `setup_manifest_files_under_spec_zero`) where the live file is written
  under non-default `identity(x)` spec 0 while the table default is unpartitioned spec 1; pins that the
  residual reduces to `AlwaysTrue` (file's spec) and `task.partition_spec.spec_id()==0`. Re-running mutation
  (c) now FAILS it.
- **Scope + floor: CLEAN.** Production code byte-identical to the builder's after every mutation reverted; my
  additions are test-only (fixture builder + 1 test) in `scan/mod.rs`; no bare `.unwrap()` added in production;
  no Cargo/spec/reader-logic/transaction edits; no commit/branch switch.
- **Gate (all clean, repo root, pinned nightly):** build -p iceberg ✅; workspace build (--exclude
  sqllogictest --all-targets) ✅; lib ×2 = **1501/0** both (was 1500 → +1 spec-evolution pin); 3 interop
  suites 4/4 each ✅; clippy -D warnings clean ✅; fmt clean ✅. Row stays 🟡 (Increment 3 conflict-validation
  + Java interop still pending).

### Increment 3 — OverwriteFiles filter-based validateNoConflictingData (DETAIL, 2026-06-08)
The headline write-safety unblock. Java `BaseOverwriteFiles.validate` → `validateNewDataFiles` →
`MergingSnapshotProducer.validateAddedDataFiles(base, startingSnapshotId, conflictDetectionFilter, parent)`:
enumerate DATA files ADDED by concurrent commits since the starting snapshot, and reject the commit if ANY
could contain records matching the conflict-detection filter. SCOPE: OverwriteFiles `validateNoConflictingData`
ONLY (the filter-based added-data-file conflict check). DEFER: `validateAddedFilesMatchOverwriteFilter`
(block 1, the writer's own files), `validateNoConflictingDeletes` (block 3, delete conflicts), and RowDelta —
each its own follow-up.

Model (already landed): `transaction/replace_partitions.rs::validate` — flag → `effective_start =
validate_from_snapshot.or(starting_snapshot_id)` → `added_data_files_after(current, effective_start)` →
per-file conflict test → non-retryable `ErrorKind::DataInvalid` on the first conflict. OverwriteFiles differs
ONLY in the conflict test: instead of `(spec_id, partition) ∈ drop-set`, it's "could this added file match the
conflict filter?" via the EXISTING `InclusiveMetricsEvaluator::eval(&bound_filter, file) -> Result<bool>`
(file metrics/bounds; `pub(crate)` in `expr/visitors/inclusive_metrics_evaluator.rs`).

Building blocks (all exist): `added_data_files_after(table, Option<i64>) -> Result<Vec<DataFile>>`
(`transaction/snapshot.rs`); `InclusiveMetricsEvaluator::eval`; `Predicate::bind(schema, case_sensitive) ->
Result<BoundPredicate>`. The `validate` hook is `async fn validate(self: Arc<Self>, starting_snapshot_id:
Option<i64>, current: &Table) -> Result<()>` (default no-op; override it, mirroring ReplacePartitions).

Plan:
- [x] Add fields to `OverwriteFilesAction`: `validate_no_conflicting_data: bool`, `conflict_detection_filter:
      Option<Predicate>`, `validate_from_snapshot: Option<i64>` (+ builder methods named like ReplacePartitions:
      `validate_no_conflicting_data()`, `conflict_detection_filter(Predicate)`, `validate_from_snapshot(i64)`).
- [x] Override `validate`: no-op unless the flag is set; `effective_start`; `added_data_files_after`; bind the
      conflict filter (if `None`, treat as `AlwaysTrue` = ANY concurrent added data file conflicts — the most
      conservative serializable check); for each added file `InclusiveMetricsEvaluator::eval(&bound, file, true)?`
      → first match → non-retryable `DataInvalid` naming the file + filter (Java "Found conflicting files that
      can contain records matching %s: %s"). Default case-sensitivity true (Java `isCaseSensitive()`; the
      action has no such field — noted in the `validate` doc-comment).
- [x] Module doc: drop `validateNoConflictingData` from the deferred list (note block1/block3/RowDelta remain).
- [x] Tests (MemoryCatalog, the ReplacePartitions conflict-test pattern — commit a CONCURRENT snapshot after
      the txn's starting point, then validate): (1) no concurrent commit → commit OK; (2) concurrent commit
      adds a file whose metrics MATCH the filter → validate fails `DataInvalid` (non-retryable, doesn't loop);
      (3) concurrent commit adds a file whose metrics EXCLUDE the filter (bounds outside) → commit OK; (4) flag
      OFF → no check (snapshot isolation) even with a conflict; (5) `conflict_detection_filter` None → any
      concurrent added file conflicts; (6) `validate_from_snapshot` override (+ a negative half + a
      tx-captured-start survives-rebase pin). Build files WITH bounds so the inclusive evaluator can
      include/exclude. Mutation-pin: the include-vs-exclude metrics decision + the non-retryable error kind.
- [x] Docs: GAP_MATRIX (OverwriteFiles validateNoConflictingData landed, row stays 🟡), Roadmap, todo, lessons.
- [x] Verify from repo root: build -p iceberg + workspace; lib ×2; 3 interop; clippy (workspace); fmt.

**Outcome (2026-06-08, Phase 3 Increment 3 — OverwriteFiles filter-based `validateNoConflictingData`, BUILDER
Opus):** Added the serializable-isolation write-safety layer to `OverwriteFilesAction` in
`transaction/overwrite_files.rs` (the ONLY production file touched besides docs). It is the SAME shape as the
already-landed `ReplacePartitions.validate`; only the per-file conflict TEST differs.
- **Builder API:** three opt-in fields + builders, named exactly like ReplacePartitions —
  `validate_no_conflicting_data(self) -> Self` (enable), `validate_from_snapshot(self, i64) -> Self` (override
  the starting snapshot), and the NEW `conflict_detection_filter(self, Predicate) -> Self` (the data filter
  OverwriteFiles has but ReplacePartitions doesn't). All default OFF/None in `new()`.
- **`validate` algorithm** (overrides the `TransactionAction` default no-op): `if
  !validate_no_conflicting_data { return Ok(()) }` → `effective_start = validate_from_snapshot.or(starting_
  snapshot_id)` → `added = added_data_files_after(current, effective_start).await?` (empty ⇒ Ok) → bind the
  conflict filter against `current.metadata().current_schema()` case-sensitive=`true` (the filter is the
  caller's `conflict_detection_filter` when set, else `Predicate::AlwaysTrue`) → for each added file, if
  `InclusiveMetricsEvaluator::eval(&bound_filter, file, true)?` is true, return a non-retryable
  `Error::new(ErrorKind::DataInvalid, "Found conflicting files that can contain records matching {filter}:
  {path}")` on the FIRST conflict.
- **Java lines verified against** (`/tmp/iceberg-java-ref`): `BaseOverwriteFiles.validate` L136-179 — the
  `validateNewDataFiles` branch L163-165 calls `validateAddedDataFiles(base, startingSnapshotId,
  dataConflictDetectionFilter(), parent)`; `MergingSnapshotProducer.validateAddedDataFiles` L391-412 (build a
  `ManifestGroup` over the concurrent-added DATA manifests filtered by the conflict filter; throw "Found
  conflicting files that can contain records matching %s: %s" if any entry survives); `dataConflictDetection
  Filter()` L181-187 (filter when set, else rowFilter when not alwaysFalse + no deleted files, else
  `alwaysTrue()`). VERIFIED our `overwriteByRowFilter` is deferred so `rowFilter()` is effectively
  `alwaysFalse()` and the row-filter branch can never apply → None ⇒ `AlwaysTrue`, matching Java.
- **None-filter default decision:** a `None` `conflict_detection_filter` binds `Predicate::AlwaysTrue` ⇒ ANY
  concurrently-added DATA file is a conflict (the most conservative serializable check). This mirrors Java
  `dataConflictDetectionFilter()` returning `alwaysTrue()` and is pinned by
  `test_overwrite_none_filter_treats_any_concurrent_add_as_conflict` (a no-bounds concurrent file is still a
  conflict).
- **Case-sensitivity default decision:** Java binds with `isCaseSensitive()`. `OverwriteFilesAction` has no
  case-sensitivity field, so the filter is bound case-sensitive (`true` = the Iceberg/Java default); noted in
  the `validate` doc-comment. (Adding a `case_sensitive` builder is a trivial future follow-up if needed.)
- **Test list (9 new conflict tests; each risk pinned):**
  1. `test_overwrite_validation_no_concurrent_commit_succeeds` — validation enabled but nothing concurrent ⇒
     commit OK (a race-free commit must not be blocked).
  2. `test_overwrite_rejects_concurrent_added_file_matching_filter` — HEADLINE: concurrent file with y bounds
     [60,70] vs filter `y>=50` ⇒ REJECTED, non-retryable, error names the file (silent lost-write prevention).
  3. `test_overwrite_allows_concurrent_added_file_excluded_by_filter` — concurrent file y bounds [10,20]
     entirely below `y>=50` ⇒ inclusive evaluator EXCLUDES ⇒ commit OK (no false conflict).
  4. `test_overwrite_without_validation_allows_conflicting_concurrent_append` — flag OFF (filter present but
     inert) ⇒ a would-be-conflict concurrent append does NOT block (snapshot isolation unchanged / opt-in).
  5. `test_overwrite_none_filter_treats_any_concurrent_add_as_conflict` — None filter ⇒ AlwaysTrue ⇒ a
     no-bounds concurrent add is a conflict (the conservative serializable default; the OPPOSITE of "no check").
  6. `test_overwrite_validate_from_snapshot_override_changes_concurrent_window` — `validate_from_snapshot(S0)`
     widens the window to include S1's add ⇒ REJECTED (the override genuinely shifts the boundary).
  7. `test_overwrite_validate_from_snapshot_at_head_finds_no_conflict` — `validate_from_snapshot(S1=head)` ⇒
     nothing concurrent ⇒ commit OK (the negative half proving the boundary shift is real).
  8. `test_overwrite_rejects_concurrent_using_tx_captured_starting_snapshot` — NO `validate_from_snapshot`;
     relies on the tx-captured start surviving `do_commit`'s re-base ⇒ REJECTED (the only test exercising the
     capture; if the start were re-read from the refreshed head the check would silently always pass).
  All 9 use the ReplacePartitions concurrent-commit pattern: a SEPARATE `fast_append` lands between building
  the txn and committing it, advancing the catalog head; `do_commit` refreshes to that head and runs `validate`
  against it. Data files carry y-column bounds via the new `data_file_with_y_bounds(path, part, lo, hi)` helper
  (sets `lower_bounds`/`upper_bounds` on schema field id 2).
- **Mutations run + each caught:** (a) invert the metrics decision (`if !eval(...)`) → the EXCLUDE test
  (`..excluded_by_filter`) AND the MATCH/None tests fail; (b) make `validate` early-`return Ok(())` (skip the
  check) → exactly the 4 rejection tests (#2/#5/#6/#8) fail, the OK tests stay green; (c) change the error kind
  to `CatalogCommitConflicts` → the 4 rejection tests' `kind()==DataInvalid` assertions fail. Bonus
  genuinely-non-retryable check: `.with_retryable(true)` on the conflict error → the `!err.retryable()`
  assertion fails AND the test runtime jumps 0.12s→1.57s (the retry loop demonstrably loops) — confirming the
  property is verified, not merely asserted.
- **Gate (all from repo root, pinned nightly):** `cargo build -p iceberg` ✅ (Finished); `cargo build
  --workspace --exclude iceberg-sqllogictest --all-targets` ✅ (Finished); `cargo test -p iceberg --lib` ✅ TWICE
  (run1 1509 passed / 0 failed; run2 1509 passed / 0 failed — **new lib total 1509**, was 1500, +9 tests);
  3 interop binaries ✅ (`interop_manage_snapshots` 4/4, `interop_update_partition_spec` 4/4,
  `interop_update_schema` 4/4); `cargo clippy --workspace --exclude iceberg-sqllogictest --all-targets -- -D
  warnings` ✅ (Finished, no warnings); `cargo fmt --all -- --check` ✅ (clean after one `cargo fmt`);
  `cargo test -p iceberg --lib transaction::` ✅ (225 passed). `transaction::overwrite_files` alone: 18/18.
- **Scope:** touched ONLY `transaction/overwrite_files.rs` + docs (GAP_MATRIX, Roadmap, todo, lessons). No
  edits to `snapshot.rs` / `inclusive_metrics_evaluator.rs` / `action.rs` / `predicate.rs` / `replace_partitions.rs`
  (read-only). No `Cargo.toml`/lockfile/spec/scan/arrow changes. No commit, no branch switch, no push.
- **UNSURE / flagged:** (1) The Java `addedDataFiles` builds a `ManifestGroup` that pre-filters at the MANIFEST
  level (manifest partition/metrics summary) BEFORE the per-entry inclusive-metrics test; the Rust port
  approximates this at the FILE level only (`added_data_files_after` returns every added DATA file, then we run
  `InclusiveMetricsEvaluator::eval` per file). This is a CONSERVATIVE over-scan — it can only over-reject
  (inspect a file Java's manifest pre-filter would skip), never under-reject, so it cannot let a real conflict
  through; it is a performance/precision gap, not a correctness one. (2) `include_empty_files = true` is passed
  to `eval` so a zero-record concurrent file is evaluated by its bounds rather than auto-excluded on emptiness
  — conservative (a 0-record file is effectively never a real conflict, but never excluding it is safe). (3)
  The concurrent commit is simulated EXACTLY as the landed ReplacePartitions tests do it (a real second
  `fast_append` through the same `MemoryCatalog`, between txn-build and txn-commit), so `do_commit`'s genuine
  refresh/re-base path runs — not a hand-mocked base. The non-retryable property is verified by BOTH the
  `!err.retryable()` assertion AND the runtime-doesn't-loop mutation evidence above.

#### Increment 3 — REVIEW (2026-06-08, Opus REVIEWER, DELEGATED). VERDICT: CHANGES-MADE (docs only).
Adversarially verified against the Java source (`/tmp/iceberg-java-ref`) + 7 code mutations. Production
`validate` + 9 conflict tests are CORRECT and well-pinned; NO production change made.
- **`validate` invoked by the real commit path:** `do_commit` (mod.rs:308-312) runs each action's
  `.validate(self.starting_snapshot_id, &current_table)` AFTER the refresh/re-base (line 295) and BEFORE
  re-apply (314), against the refreshed base; `starting_snapshot_id` captured once in `Transaction::new`
  (117), survives the re-base. Not dead code.
- **THE KEY FINDING — enumeration MATCHES Java, no under-reject:** read `validationHistory` (L913-963) +
  `addedDataFiles` (L424-462) + `SnapshotUtil.ancestorsBetween`/`ancestorsOf`. Same INCLUSIVE-parent /
  EXCLUSIVE-start boundary, same op set {APPEND,OVERWRITE}, same `manifest.snapshotId()==snapshot.snapshotId()`
  manifest filter, same Added-only entry filter. Java's `newSnapshots.contains(entry.snapshotId())` is
  jointly satisfied by exactly those Added entries. The only divergence (Inc-6, already pinned): non-ancestor
  start → Java throws, Rust over-scans = Rust-STRICTER. NO false-negative vector.
- **None-filter=AlwaysTrue:** faithful mirror of `dataConflictDetectionFilter()` (rowFilter branch unreachable
  while `overwriteByRowFilter` deferred). **Non-retryable:** `Error::new` defaults `retryable:false`
  (error.rs:235), independent of kind; loop stops (`.when(|e| e.retryable())`).
- **Mutations run (all CAUGHT):** invert metrics (exclude + match tests fail); skip check (4 rejection fail);
  kind→CatalogCommitConflicts (4 kind asserts fail); drop validate_from_snapshot (override test fails);
  re-read refreshed head instead of tx-captured start (tx-captured test fails — the silent-always-pass
  guard); `.with_retryable(true)` (retryable asserts fail + 0.11→1.59s loop); EXCLUSIVE→INCLUSIVE boundary in
  shared `added_data_files_after` (5 tests fail across both actions). No surviving mutation.
- **CHANGES-MADE (docs only):** reconciled two stale Roadmap narrative mentions (lines ~159, ~302 said
  "conflict validation … deferred" for OverwriteFiles — under-claim) + the `mod.rs::overwrite_files()` ctor
  doc ("not yet supported"). GAP_MATRIX row 75 was already accurate + honest (over-scan, None-default,
  case-sensitivity, 🟡 rationale). Production `overwrite_files.rs`/`snapshot.rs` byte-identical to pre-review.
- **Scope + floor: CLEAN.** No bare `.unwrap()` in the production `validate` path. **Gate (repo root, pinned
  nightly):** build -p iceberg ✅; workspace --exclude sqllogictest --all-targets ✅; lib ×2 = 1509/0 both;
  transaction:: 225; 3 interop 4/4 each; clippy -D warnings ✅; fmt ✅. Row stays 🟡 (data-level Java interop
  + `overwriteByRowFilter` + `validateNoConflictingDeletes` still pending).

### Increment 4 — RowDelta validateNoConflictingDataFiles + shared conflict-check helper (DETAIL, 2026-06-08)
Java `BaseRowDelta.validate` → `validateNewDataFiles` → `validateAddedDataFiles(base, startingSnapshotId,
conflictDetectionFilter, parent)` — the IDENTICAL filter-based added-data-file conflict check just built for
OverwriteFiles (Increment 3). SCOPE: RowDelta `validateNoConflictingDataFiles` ONLY. DEFER (need a new
concurrent-DELETE-file enumeration helper — bigger, separate): `validateNoConflictingDeleteFiles`
(`validateNoNewDeleteFiles`), `validateDeletedFiles`/`validateDataFilesExist`, `validateAddedDVs`.

Because this is the 2nd use of the exact same load-bearing safety check, FACTOR a shared `pub(crate)` helper
(next to `added_data_files_after` in `transaction/snapshot.rs`) so OverwriteFiles + RowDelta cannot drift:
`async fn validate_no_conflicting_added_data_files(current: &Table, effective_start: Option<i64>,
conflict_filter: Option<&Predicate>, case_sensitive: bool) -> Result<()>` doing the
`added_data_files_after` walk + bind (None ⇒ AlwaysTrue) + per-file `InclusiveMetricsEvaluator::eval` +
first-conflict non-retryable `DataInvalid`. Refactor OverwriteFiles.validate to call it (BEHAVIOR-PRESERVING —
Increment 3's 9 tests stay green, the proof).

Plan:
- [x] `transaction/snapshot.rs`: add the shared `pub(crate)` helper next to `added_data_files_after`. No
      behavior change to existing functions.
- [x] `transaction/overwrite_files.rs`: refactor `validate` to call the helper (keep the flag-guard +
      `effective_start`); Increment-3 tests unchanged + green = behavior-preserving proof.
- [x] `transaction/row_delta.rs`: add fields `validate_no_conflicting_data_files: bool`,
      `conflict_detection_filter: Option<Predicate>`, `validate_from_snapshot: Option<i64>` + builder methods;
      override `validate` (flag-guard → `effective_start = validate_from_snapshot.or(starting_snapshot_id)` →
      the shared helper). Update the module doc (drop `validateNoConflictingDataFiles` from deferred; note the
      delete-file blocks remain).
- [x] Tests (RowDelta, MemoryCatalog + real concurrent fast_append, mirroring Increment 3): no-concurrent → OK;
      metrics-match → rejected non-retryable (names file); metrics-exclude → OK; flag OFF → snapshot isolation;
      None filter → any concurrent add conflicts; `validate_from_snapshot` override. Mutation-pin the
      include/exclude decision + the non-retryable kind. (OverwriteFiles tests double as the helper's pins.)
- [x] Docs: GAP_MATRIX (RowDelta validateNoConflictingDataFiles 🟡), Roadmap, todo, lessons.
- [x] Verify from repo root: build -p iceberg + workspace; lib ×2; transaction::; 3 interop; clippy; fmt.

**Outcome (2026-06-08, Phase 3 Increment 4 — RowDelta `validateNoConflictingDataFiles` + shared conflict-check
helper, BUILDER Opus):** `RowDelta` gained the SAME filter-based concurrent-commit conflict validation
`OverwriteFiles` has, and because this was the 2nd use of the load-bearing safety logic it was FACTORED into a
shared helper so the two checks cannot drift.
- **Shared helper** (`transaction/snapshot.rs`, right next to `added_data_files_after`, nothing else in that
  file touched — `added_data_files_after` is BYTE-IDENTICAL): `pub(crate) async fn
  validate_no_conflicting_added_data_files(current: &Table, effective_start: Option<i64>, conflict_filter:
  Option<&Predicate>, case_sensitive: bool) -> Result<()>`. It runs `added_data_files_after` → empty ⇒ `Ok`
  → binds the filter to the table's current schema (`None` ⇒ `Predicate::AlwaysTrue`) → per added file
  `InclusiveMetricsEvaluator::eval(&bound, file, true)` → first match ⇒ non-retryable
  `Error::new(ErrorKind::DataInvalid, "Found conflicting files that can contain records matching {filter}:
  {path}")` (filter rendered as `"true"` for `None`, else `Display`, exactly the Increment-3 wording).
- **OverwriteFiles refactor (BEHAVIOR-PRESERVING):** `OverwriteFilesAction::validate` keeps its flag-guard +
  `effective_start` computation and now delegates the walk+bind+eval+error to the shared helper (the inline
  ~35 lines deleted; `Bind`/`BoundPredicate`/`InclusiveMetricsEvaluator`/`added_data_files_after`/`Error`/
  `ErrorKind` imports removed from `overwrite_files.rs`). **Proof of behavior-preservation:** Increment 3's
  9 OverwriteFiles conflict tests (+ the 9 core overwrite tests = 18 total) stayed GREEN unchanged.
- **RowDelta feature:** added fields `validate_no_conflicting_data_files: bool` (default false),
  `conflict_detection_filter: Option<Predicate>` (None), `validate_from_snapshot: Option<i64>` (None), all
  init in `new()`; builder methods `validate_no_conflicting_data_files(self)`, `conflict_detection_filter(self,
  Predicate)`, `validate_from_snapshot(self, i64)` (named exactly like OverwriteFiles). Overrode `validate` in
  the `TransactionAction for RowDeltaAction` impl: `if !self.validate_no_conflicting_data_files { return Ok(()) }`
  → `effective_start = self.validate_from_snapshot.or(starting_snapshot_id)` → the shared helper with
  `self.conflict_detection_filter.as_ref()` and case-sensitive `true` (Java `isCaseSensitive()`; the action has
  no such field — defaults to `true`, noted in the doc). Updated the module doc: dropped
  `validateNoConflictingDataFiles` from the deferred list; the delete-file blocks
  (`validateNoConflictingDeleteFiles`/`validateDataFilesExist`/`validateAddedDVs`) remain deferred (they need a
  concurrent-DELETE-file enumeration helper that does not exist yet).
- **Java lines verified** (`/tmp/iceberg-java-ref`): `BaseRowDelta.validate` L132-174 — the `validateNewDataFiles`
  branch L155-157 calls `validateAddedDataFiles(base, startingSnapshotId, conflictDetectionFilter, parent)`, the
  IDENTICAL call OverwriteFiles makes; `MergingSnapshotProducer.validateAddedDataFiles` L391-412 (build the
  conflict iterable, throw "Found conflicting files that can contain records matching %s: %s" if any). The other
  blocks I DEFERRED, confirmed against the source: `validateDataFilesExist` (L141-149, referenced-files),
  `validateNewDeleteFiles`/`validateNoNewDeleteFiles` (L159-168), `validateNoConflictingFileAndPositionDeletes`
  (L170/L181-193), `validateAddedDVs` (L172) — all need delete-file enumeration that does not exist yet.
- **RowDelta test list (8 new, each pins a risk):**
  1. `test_row_delta_validation_no_concurrent_commit_succeeds` — validation enabled, nothing concurrent ⇒ OK
     (a race-free commit must not be blocked).
  2. `test_row_delta_rejects_concurrent_added_file_matching_filter` — HEADLINE: concurrent file y bounds [60,70]
     vs filter `y>=50` ⇒ REJECTED non-retryable, error names the file (lost/incorrect merge-on-read prevention).
  3. `test_row_delta_allows_concurrent_added_file_excluded_by_filter` — concurrent file y bounds [10,20] below
     `y>=50` ⇒ inclusive evaluator EXCLUDES ⇒ OK (no false conflict). The mutation-(a) target.
  4. `test_row_delta_without_validation_allows_conflicting_concurrent_append` — flag OFF ⇒ snapshot isolation
     unchanged / opt-in.
  5. `test_row_delta_none_filter_treats_any_concurrent_add_as_conflict` — None filter ⇒ AlwaysTrue ⇒ a no-bounds
     concurrent add conflicts (the conservative serializable default, opposite of "no check").
  6. `test_row_delta_validate_from_snapshot_override_changes_concurrent_window` — `validate_from_snapshot(S0)`
     widens the window to include S1's add ⇒ REJECTED (override genuinely shifts the boundary).
  7. `test_row_delta_validate_from_snapshot_at_head_finds_no_conflict` — `validate_from_snapshot(S1=head)` ⇒
     nothing concurrent ⇒ OK (the negative half).
  8. `test_row_delta_rejects_concurrent_using_tx_captured_starting_snapshot` — NO override; the tx-captured
     start surviving `do_commit`'s re-base is the only thing that detects the conflict (if the start were
     re-read from the refreshed head the check would silently always pass).
  All 8 simulate a REAL concurrent commit (a separate `fast_append` lands between txn-build and txn-commit, so
  `do_commit` refreshes to the new head and runs `validate` against it). Data files carry y-column bounds via a
  new `data_file_with_y_bounds(path, part, lo, hi)` helper (schema field id 2).
- **Mutations run (each caught), incl. the cross-action one:** (a) invert the metrics decision in the SHARED
  helper (`if !eval(...)`) → the EXCLUDE test fails for BOTH `transaction::overwrite_files` AND
  `transaction::row_delta` (the cross-action proof the shared helper is load-bearing for both). (b) force
  RowDelta's `validate` always-`Ok` → exactly its 4 rejection tests (#2/#5/#6/#8) fail, OK tests green. (c) make
  the helper's conflict error retryable (`.with_retryable(true)`) → the `!err.retryable()` assertion fails AND
  the retry loop visibly spins (0.06s → 1.57s, a kind/timing failure). All restored byte-identical; verified the
  OverwriteFiles 9 conflict tests stay green after the refactor (behavior-preservation).
- **Gate (all from repo root, pinned nightly):** `cargo build -p iceberg` ✅; `cargo build --workspace
  --exclude iceberg-sqllogictest --all-targets` ✅; `cargo test -p iceberg --lib` ✅ TWICE (run1 1517 passed /
  0 failed; run2 1517 passed / 0 failed — **new lib total 1517**, was 1509, +8 RowDelta conflict tests);
  `cargo test -p iceberg --lib transaction::` ✅ (233 passed; `transaction::overwrite_files` 18/18 unchanged,
  `transaction::row_delta` 19/19); 3 interop binaries ✅ (`interop_manage_snapshots` 4/4,
  `interop_update_partition_spec` 4/4, `interop_update_schema` 4/4); `cargo clippy --workspace --exclude
  iceberg-sqllogictest --all-targets -- -D warnings` ✅ (no warnings); `cargo fmt --all -- --check` ✅ (clean
  after one `cargo fmt`).
- **Scope:** touched ONLY `transaction/snapshot.rs` (ADDED the shared helper + 2 imports — `added_data_files_after`
  unchanged), `transaction/overwrite_files.rs` (refactor `validate` to delegate), `transaction/row_delta.rs`
  (feature + 8 tests), and docs (GAP_MATRIX, Roadmap, todo, lessons). No edits to
  `inclusive_metrics_evaluator.rs` / `action.rs` / `replace_partitions.rs` / `predicate.rs` / `spec/` / `scan/`
  / `arrow/` / Cargo / lockfiles. No on-disk format change. No commit, no branch switch, no push — work left on
  `phase3-residual-eval`.
- **UNSURE / flagged:** (1) The RowDelta conflict tests assert `add_deletes`-only commits with SYNTHETIC delete
  files (no real parquet) — same as the existing RowDelta manifest/summary tests; the conflict check is on the
  concurrently-added DATA files, independent of the delete-file payload, so synthetic delete files are
  sufficient and faithful. (2) Java `BaseRowDelta`'s `conflictDetectionFilter` DEFAULTS to `Expressions.alwaysTrue()`
  (not null) and is shared across ALL the validate blocks; our `None ⇒ AlwaysTrue` binding mirrors the default
  for the data-file block, the only block we implement. (3) The non-ancestor `validate_from_snapshot` over-scan
  divergence (Rust over-scans to root where Java throws) is inherited UNCHANGED from `added_data_files_after`
  (Increment-6-flagged, Rust-STRICTER, over-reject-only) — not re-introduced here.

---

## OVERNIGHT AUTONOMOUS RUN (2026-06-08/09) — branch `phase3-conflict-and-scan` (off `phase3-rowdelta-delete-conflicts` d327e80d)
User asleep; run via actor-critic, COMMIT LOCALLY ONLY (no push). User approved "all 5". RECON FOUND 2 of the
planned items are COUPLED (not clean reuses), so ADAPTED to clean additive items (documented for the morning):
- OverwriteFiles `validateNoConflictingDeletes` block 3 is GATED on `rowFilter() != alwaysFalse()` → needs
  `overwriteByRowFilter` (not built). DROPPED.
- `StreamingDelete` (DeleteFiles) only has `validateFilesExist` → needs a *deleted*-data-file enumeration +
  resolved removed files, NOT the existing helpers. DEFERRED.
The clean conflict-validation reuses are ALREADY done (RowDelta data+delete, OverwriteFiles data,
ReplacePartitions). So the overnight run pivots to clean, additive, locally-verifiable scan/inspection items.

ADAPTED SEQUENCE (each: builder → adversarial reviewer → independent widened gate → local commit):
1. **`readable_metrics`** inspection column (Java `MetricsUtil.readableMetricsStruct`) — read-only/additive.
2. **`IncrementalAppendScan`** (Java `BaseIncrementalAppendScan`/`appendsBetween`) — new scan entry, additive.
3. **`ScanReport` / `MetricsReporter`** (Java `metrics/ScanReport.java`) — self-contained observability.
4. **DeleteFiles `validateFilesExist` + `deleted_data_files_after`** (verify clean when reached; the deleted-
   data-file enumeration is parallel to `added_data_files_after`, DeleteFiles has `delete_paths`). Substitute
   the `position_deletes` metadata table if coupled.
5. **`position_deletes` metadata table** (Java `PositionDeletesTable`) OR `IncrementalChangelogScan` — pick when
   reached. (Not yet built; a real inspection/scan gap.)
Widened gate for ALL: workspace build + iceberg lib ×2 + transaction:: + iceberg-datafusion tests + 3 interop +
workspace clippy + fmt + typos.

### Increment 1 — readable_metrics (DETAIL)
Java `MetricsUtil` (`core/.../MetricsUtil.java`): `READABLE_METRIC_COLS` (L140-193) = the 6 per-column metrics
(`column_size`/`value_count`/`null_value_count`/`nan_value_count` = long, from the file's metric maps by field
id; `lower_bound`/`upper_bound` = the COLUMN's type, decoded via `Conversions.fromByteBuffer` — NOT raw bytes);
`readableMetricsSchema(dataTableSchema, metadataTableSchema)` (L356) builds a `readable_metrics` STRUCT with one
sub-field PER leaf column of the data table (named by column path), each = a struct of the 6; `READABLE_METRICS
= "readable_metrics"` (L195). `BaseFilesTable`/`BaseEntriesTable` APPEND it (`TypeUtil.join(schema,
readableMetricsSchema(...))`). Rust: add to the `files` family + `entries` (the shared `inspect/data_file.rs`
projection, or appended as a top-level/nested column matching Java); the per-column bound decode reuses
`Datum::try_from_bytes`/typed conversion. Was DEFERRED across the inspection set.

#### Increment 1 — BUILD PLAN (Opus builder, 2026-06-08)
- [x] New module `inspect/readable_metrics.rs` — ONE source of truth used by `files` + `entries`:
      builds the `readable_metrics` STRUCT field (one sub-field per LEAF/primitive column of the data
      table's CURRENT schema, named by dotted path; type = a 6-field struct: column_size/value_count/
      null_value_count/nan_value_count long opt; lower_bound/upper_bound = the COLUMN's own type, opt).
      Ports Java `readableMetricsSchema` id scheme (nextId = metadata-table highest id; pre-increment per
      col-struct then its 6 sub-fields, then the top-level readable_metrics field). ONE documented
      divergence: Java assigns ids in `idToName.keySet()` = Java-HashMap (arbitrary) order; we use ASCENDING
      data-table-field-id order (deterministic) then sort emitted sub-fields by name (Java sorts after id
      assignment). Row builder fills 4 counts from the file's maps by field id (null when absent) and decodes
      lower/upper bound to the COLUMN's typed value via `Datum::try_from_bytes(stored.to_bytes(), col_type)`
      (inverse of the raw map's `to_bytes`; mirrors Java `Conversions.fromByteBuffer(field.type(), bytes)`).
- [x] Append `readable_metrics` to `files` family schema+scan (flat last column) AND `entries` schema+scan
      (last top-level column after `data_file`), matching Java APPEND order; `all_*` inherit it.
- [x] Tests: schema present (files/data_files/delete_files/entries + an all_*), one sub-field per leaf each a
      6-field struct, bound sub-fields carry the COLUMN type, field ids match the ported scheme; values
      (counts + DECODED typed bounds + nulls); backward-compat (raw maps still present, unchanged).
- [x] Mutations: (a) swap lower<->upper decode -> value test fails; (b) wrong field id for a count -> value
      test fails; (c) raw bytes instead of typed bound -> type/value test fails.
- [x] Docs (GAP_MATRIX, Roadmap, todo Outcome, lessons) + gate (all 8) + new lib total x2.

Outcome (2026-06-08, BUILDER Opus): `readable_metrics` landed on the `files` family + `entries` (+ `all_*`
inherit it through the shared schema/projection). NEW module `inspect/readable_metrics.rs` is the single
source of truth: `readable_metrics_field(data_schema, host_highest_field_id)` builds the virtual STRUCT (one
sub-field per LEAF/primitive data column, dotted-named via `Schema::field_id_to_name_map`, each a 6-field
struct: `column_size`/`value_count`/`null_value_count`/`nan_value_count` long opt + `lower_bound`/
`upper_bound` typed as the COLUMN's own type, opt); `ReadableMetricsBuilder` fills the row per `DataFile`.
**Field-id scheme** (Java `MetricsUtil.readableMetricsSchema`, `MetricsUtil.java:356-393`): pre-increment
counter seeded at the HOST metadata-table schema's `highest_field_id()` (= the data_file projection's 145 for
both tables), per leaf column assigns col-struct id then its 6 sub-field ids, then the top-level
`readable_metrics` id (8-column fixture → readable_metrics = 202). **ONE documented divergence:** Java's
`idToName()` HashMap iteration order (arbitrary, non-portable — `IndexByName.byId()` over a `HashMap`) is
replaced by deterministic ASCENDING data-table-field-id assignment; sub-fields still sorted by name (so the
column ORDER matches Java; exact interior ids match only when Java's HashMap order happens to coincide). Full
byte-level id parity with a JVM is the residual Java/Spark interop concern. **Typed bound decode:** the stored
`Datum` is re-serialized (`to_bytes`) and re-decoded against the COLUMN's primitive type
(`Datum::try_from_bytes`) — the inverse of the raw `lower_bounds`/`upper_bounds` map columns, mirroring Java
`Conversions.fromByteBuffer(field.type(), buffer)`; widens an evolved field's bound to the column's current
type. Raw metrics-map columns UNCHANGED (additive). **Java lines verified:** `READABLE_METRIC_COLS` L140-193
(the 6 metrics; counts by field id nullable; bounds via `Conversions.fromByteBuffer(field.type(), …)`),
`READABLE_METRICS` L195, `readableMetricsSchema` L356-393 (id scheme + sort-by-name + per-column doc),
`readableMetricsStruct` L403-429, `IndexByName.byId()` L78-80 (the HashMap-order source). Provider untouched
(it delegates to `.schema()`/`.scan()` — `test_metadata_table` green). **Files modified:** new
`crates/iceberg/src/inspect/readable_metrics.rs`; `inspect/mod.rs` (+`mod readable_metrics`); `inspect/files.rs`
(schema+scan append + 4 tests + 2 existing schema assertions updated); `inspect/entries.rs` (schema+scan
append + 2 tests + 1 existing schema assertion updated); `inspect/data_file.rs` (module doc — readable_metrics
no longer "deferred"). **Tests (12 new):** readable_metrics.rs ×6 — one-struct-per-leaf sorted-by-name;
6-field metric struct; bounds carry the COLUMN type (long/string/double, counts always long); the exact
pre-increment id scheme (101/108/115 column structs, 102-107 + 116-121 sub-ids, 122 top-level); primitives
nested in struct/list included by dotted path; complex-only (map) columns excluded. files.rs ×4 — schema
present with per-column bound type (x→Int64, a→Utf8); counts + DECODED typed bounds + nulls (x.column_size=42,
x.lower_bound=1 long, absent metrics null, y all null, i32 bound Int32-typed null); distinct lower=10/upper=99
both typed longs; raw maps unchanged regression guard. entries.rs ×2 — readable_metrics present with decoded
bound (entries x.column_size=42, x.lower_bound=1); `all_entries` schema inherits readable_metrics.
**Mutations (all caught, re-run by injection):** (a) swap lower↔upper source maps → distinct-bounds test
fails (reads 99 where 10 expected); (b) read column_size from `field_id+1` → counts test fails (x.column_size
null); (c) declare bounds as raw `Binary` → bound-type schema test + the column-type unit test + 4 builder
dispatch tests fail. **Gate (all 8, pinned nightly):** (1) `cargo build -p iceberg` OK; (2)
`cargo build --workspace --exclude iceberg-sqllogictest --all-targets` OK; (3) `cargo test -p iceberg --lib`
×2 = **1528 passed / 0 failed** both runs (was 1516 → +12); (4) `cargo test -p iceberg-datafusion --lib
--tests` 9/9 integration green incl. `test_metadata_table` (the all-targets DOCTEST run fails ONLY the
pre-existing `table_provider_factory.rs` `tokio::main`/rt-multi-thread environmental artifact documented in
lessons 2026-06-07 Increment-7 REVIEWER — a file this change never touched); (5) interop_manage_snapshots /
interop_update_schema / interop_update_partition_spec = 4/4 each; (6) `cargo clippy --workspace --exclude
iceberg-sqllogictest --all-targets -- -D warnings` clean; (7) `cargo fmt --all -- --check` clean; (8) `typos`
repo-wide exit 0. No commit, no branch switch, no edits outside `inspect/` + docs.

### OVERNIGHT branch correction + Increment 2
NOTE: a stray git op during Increment 1's builder left the tree on `main`; since the overnight scan/inspection
items are INDEPENDENT of the rowdelta-delete helper, the overnight work is on branch **`phase3-overnight` off
`main` (063b7153)** — NOT stacked on rowdelta-delete (simpler; no rebase needed). Increment 1 (readable_metrics)
committed `31595dd4`. After EACH agent: re-verify `git branch --show-current == phase3-overnight`.

### Increment 2 — IncrementalAppendScan (DETAIL)
Java `BaseIncrementalAppendScan` (`core/.../BaseIncrementalAppendScan.java`): `doPlanFiles(fromExclusive,
toInclusive)` → `appendsBetween(table, fromExclusive, toInclusive)` = the APPEND-operation snapshots in
`(from, to]` (via `SnapshotUtil.ancestorsBetween` filtered to `operation == DataOperations.APPEND` — L111),
then `appendFilesFromSnapshots(snapshots)` plans FileScanTasks for the data files those append snapshots ADDED
(only Added entries from the manifests each snapshot itself added). Append-only: OVERWRITE/DELETE snapshots in
the range are EXCLUDED. The API (`IncrementalAppendScan` interface): `fromSnapshotInclusive`/
`fromSnapshotExclusive` + `toSnapshot` (default to = current).

Rust approach (LOWEST-RISK — additive, do NOT refactor the existing single-snapshot `plan_files`): a SEPARATE
planner. `Table::incremental_append_scan()` → a builder with `from_snapshot_id` (exclusive) + optional
`to_snapshot_id` (default current) + `with_filter`/`select` (mirror `TableScanBuilder`). Planning: compute the
append snapshots in `(from, to]` (mirror the `added_data_files_after` ancestor walk in `transaction/snapshot.rs`,
but filtered to `Operation::Append` and bounded by both ends); for each, read the DATA manifests it ADDED
(`added_snapshot_id == snapshot_id`), and for each `Added` entry build a `ManifestEntryContext` (reuse the
existing `scan/context.rs` struct + `into_file_scan_task`, with an EMPTY delete index — an append scan applies
no deletes) and stream `FileScanTask`s. Reuse the residual evaluator + the bound-filter machinery exactly as
the normal scan does. Must NOT change existing `TableScan`/`plan_files` behavior.

Tests (MemoryCatalog/TableTestFixture): multi-snapshot fixture with ≥2 appends; `incremental_append_scan`
from=S0(excl) to=S2(incl) returns ONLY the files appended in S1+S2 (not S0's); from==to → empty; an OVERWRITE
snapshot in the range is EXCLUDED; a filter prunes appended files by partition; default to=current. Mutation-
pin: the exclusive-from boundary, the append-only op filter, the added-manifest filter. Confirm the normal
`table.scan()` is unchanged. Widened gate (incl. datafusion tests).

#### BUILDER plan (Opus, 2026-06-08)
- [x] Add `scan/incremental.rs`: `IncrementalAppendScanBuilder` (`from_snapshot_id_exclusive`,
      `from_snapshot_id_inclusive`, `to_snapshot_id`, `with_filter`, `select`/`select_all`/`select_empty`,
      concurrency limits) → `build()` → `IncrementalAppendScan`. Validate snapshots exist + `to` descends `from`.
- [x] `IncrementalAppendScan::plan_files()`: bounded ancestor walk of `(from, to]` filtered to
      `Operation::Append`; for each, keep DATA manifests it ADDED (`added_snapshot_id == snapshot_id`); stream
      `Added`-entry `FileScanTask`s reusing `PlanContext`/`ManifestEntryContext`/`into_file_scan_task` + EMPTY
      delete index + residual evaluator.
- [x] Seam in `scan/context.rs`: factor `build_manifest_file_contexts` to also accept an explicit
      `Vec<ManifestFile>` (behavior-preserving for the normal scan) so the incremental planner reuses the
      partition-pruning + residual machinery; add an `Added`-only entry filter for the incremental path.
- [x] `Table::incremental_append_scan()` entry point in `table.rs`.
- [x] Tests in `incremental.rs` (multi-append fixture): core multi-append set; from==to (rejected, Java-faithful);
      OVERWRITE-in-range excluded; with_filter partition prune; default to=current; exclusive-from boundary.
      Mutation-pin from-exclusive, append-only op filter, added-manifest filter.
- [x] Regression: normal `table.scan().plan_files()` unchanged. Docs: GAP_MATRIX row 94 / Roadmap / lessons.

#### Outcome (BUILDER, Opus, 2026-06-08)
**API.** `Table::incremental_append_scan() -> IncrementalAppendScanBuilder` (mirrors `Table::scan()`). Builder:
`from_snapshot_id_exclusive(i64)` (Java `fromSnapshotExclusive`), `from_snapshot_id_inclusive(i64)`
(Java `fromSnapshotInclusive` — resolved to the parent as the exclusive bound at `build()`), `to_snapshot_id(i64)`
(Java `toSnapshot`, defaults to the current snapshot), `with_filter`/`select`/`select_all`/`select_empty`/
`with_case_sensitive`/`with_batch_size`/`with_concurrency_limit`. `build()` validates the `to`/`from` snapshots
exist and the Java preconditions (inclusive `from` is an ancestor of `to`; exclusive `from` is a *parent
ancestor* of `to`), then resolves schema/field-ids/bound-filter exactly as `TableScanBuilder::build`.
**Planner design.** `IncrementalAppendScan::plan_files()` is a SEPARATE planner — the single-snapshot
`TableScan::plan_files` is byte-unchanged. (1) `appends_between` does a bounded parent-chain walk from `to` back,
stopping BEFORE the exclusive `from`, keeping only `Operation::Append` snapshots (Java `appendsBetween` /
`SnapshotUtil.ancestorsBetween` filtered to APPEND; `from==to` short-circuits to empty inside the walk, but the
exclusive `isParentAncestorOf` precondition gates it first). (2) For each append snapshot it loads the manifest
list and keeps the DATA manifests THAT snapshot itself added (`added_snapshot_id == snapshot_id`, Java
`manifest.snapshotId() == snapshot.snapshotId()`). (3) Those manifests are driven through the REUSED
`PlanContext::build_manifest_file_contexts_from_files` (the additive seam) → the SAME partition-filter pruning +
residual-evaluator construction as the normal scan, with an EMPTY `DeleteFileIndex` (drop the sender so it
resolves to no-deletes). (4) `process_append_manifest_entry` keeps only `Added`-status entries (Java
`filterManifestEntries(status==ADDED)`), runs the partition-expression + inclusive-metrics filters, and
`into_file_scan_task` builds each `FileScanTask` (residual predicate, EMPTY deletes).
**Range/append-only semantics.** `(from exclusive, to inclusive]`, APPEND-only — overwrites/deletes in the range
are excluded entirely (snapshot-level op filter); no delete files applied (empty delete index).
**Java lines verified.** `BaseIncrementalAppendScan.doPlanFiles` L46-57 → `appendsBetween` L105-117 (the
`operation == DataOperations.APPEND` filter) → `appendFilesFromSnapshots` L68-99 (the
`snapshotIds.contains(manifest.snapshotId())` manifest filter + `manifestEntry.status() == ADDED` entry filter).
`BaseIncrementalScan` L159-187 (`fromSnapshotIdExclusive`: inclusive ⇒ `isAncestorOf` + parent bound;
exclusive ⇒ `isParentAncestorOf`) + L133-157 (`toSnapshotIdInclusive` defaults to current). `SnapshotUtil`
L211-229 (`ancestorsBetween` equal-from/to ⇒ empty) + L46-86 (`isAncestorOf`/`isParentAncestorOf`).
**Tests (12, `scan/incremental.rs`).** core range S1+S2-not-S0 (the planner includes both later appends, excludes
`from`'s files); exclusive-from boundary (S1's own files excluded); inclusive-from includes `from`'s files;
`from==to` exclusive REJECTED (Java precondition; a snapshot is not its own parent ancestor); range with only an
OVERWRITE → zero tasks (append-only filter); `with_filter(x==10)` prunes the x=20 appended file; default
to=current; no-from whole-lineage; empty table → zero; non-ancestor `from` rejected; own-added-manifest-only
count (carried-forward manifests don't re-surface files).
**Mutations caught (edit+revert).** (a) inclusive from boundary (process `from` before breaking) → exclusive-from
+ core + 5 others fail; (b) drop the `Operation::Append` filter (`|| true`) → overwrite-excluded + no-append-range
fail; (c) read all manifests not just the snapshot's own added (`|| true`) → own-added-manifest count + core +
others fail. Each reverted after; full lib suite re-run green.
**Gate (all 8, pinned nightly).** (1) `cargo build -p iceberg` OK; (2) `cargo build --workspace --exclude
iceberg-sqllogictest --all-targets` OK; (3) `cargo test -p iceberg --lib` ×2 = **1542 passed / 0 failed** both
runs (+12 new, was 1530); (4) `cargo test -p iceberg --lib scan::` = 55 passed (existing scan tests unchanged +
12 new); (5) `cargo test -p iceberg-datafusion --lib --tests` = 80 + 9 passed; (6) interop_manage_snapshots /
interop_update_schema / interop_update_partition_spec = 4/4 each; (7) `cargo clippy --workspace --exclude
iceberg-sqllogictest --all-targets -- -D warnings` clean; (8) `cargo fmt --all -- --check` clean · `typos`
exit 0. The normal `TableScan`/`plan_files` is UNCHANGED (the only `scan/mod.rs` edit is `mod incremental;` +
`pub use`; the `context.rs` edit is the additive `_from_files` delegation). Stayed on `phase3-overnight`; no
commit, no push; edits only in `scan/{mod,context,incremental}.rs` + `table.rs` + docs.

#### REVIEW (2026-06-08, Opus REVIEWER, DELEGATED) — VERDICT: CHANGES-MADE (1 test added), PASS 🟡
Verified independently: (1) the `context.rs` seam is behavior-preserving — read the delegation line-by-line:
the loop body is byte-identical, only `manifest_list.entries().iter().collect()`→`.to_vec()` (owned clones,
no semantic change) and `for x in manifest_files`→`for x in &manifest_files`; inside the loop `x` is
`&ManifestFile` in BOTH versions, so `get_partition_filter`/`create_manifest_file_context` see identical
types. (2) Range/append-only/own-manifest semantics match Java line-for-line (`appendsBetween` L105-117,
`ancestorsBetween`/`isAncestorOf`/`isParentAncestorOf` L46-86/L211-229, the inclusive→parent + exclusive→
parent-ancestor preconditions L159-187). (3) Java's entry-level `snapshotIds.contains(entry.snapshotId())`
filter is REDUNDANT in Rust given manifest-selection + `status==Added` (not a missing check) — documented in
lessons. **MUTATIONS run (edit+revert):** (a) inclusive-from CAUGHT, (b) drop append-only filter CAUGHT,
(c) read-all-manifests CAUGHT, (d) exclude-`to` (start at to's parent) CAUGHT — all by existing tests; **(e)
drop the `status==Added` entry filter SURVIVED** (the builder's noted gap — fast-append fixtures hold only
Added entries). **Closed (e)** by adding `test_incremental_append_keeps_only_added_entries_of_own_manifest`,
reusing `scan::tests::TableTestFixture::setup_manifest_files` (an APPEND snapshot whose own manifest carries
Added+Existing+Deleted) — incremental scan returns ONLY the Added file; mutation-verified it fails iff the
filter is dropped and nothing else. lib 1542→**1543**, scan:: 55→**56**. All 8 gates green; clippy/fmt/typos
clean; no bare unwrap/expect in production; scope intact (no Cargo/spec/transaction/arrow/datafusion edits);
docs reconciled to 13 tests / 4 mutations. NOTE: `git checkout -- context.rs` to revert a mutation wiped the
uncommitted seam (tracked file → reverts to HEAD); restored byte-exact from the read diff and re-verified.
Stayed on `phase3-overnight`; no commit, no push.

### Increment 3 — ScanReport / MetricsReporter data model + reporter API (DETAIL)
Increment 2 (IncrementalAppendScan) committed `bdb02365`. The Java `metrics/` package is LARGE (~30 files);
the SCAN-EMISSION wiring needs instrumenting the concurrent/lazy `plan_files` stream (fiddly — DEFER to a
supervised increment). This increment = the SELF-CONTAINED data model + reporter trait (also the REST catalog's
metrics-report JSON contract), low-risk + high-value.

Java authority: `api/.../metrics/{MetricsReport,ScanReport,ScanMetricsResult,CounterResult,TimerResult,
MetricsReporter}.java` + `core/.../metrics/{LoggingMetricsReporter,InMemoryMetricsReporter}.java` +
`ScanReportParser.java` (the REST JSON). Build:
- `MetricsReport` (marker trait/enum), `ScanReport` (table_name, snapshot_id, schema_id, projected_field_ids,
  projected_field_names, filter [the bound/unbound expression as a string or Predicate], metrics:
  ScanMetricsResult).
- `ScanMetricsResult` — the ~14 counters/timers (`total_planning_duration` Timer; `result_data_files`,
  `result_delete_files`, `total_data_manifests`, `total_delete_manifests`, `scanned_data_manifests`,
  `skipped_data_manifests`, `total_file_size_in_bytes`, `total_delete_file_size_in_bytes`, `skipped_data_files`,
  `skipped_delete_files`, `indexed_delete_files`, `equality_delete_files`, `positional_delete_files` Counters).
- `CounterResult { unit, value }` + `TimerResult { time_unit, total_duration, count }` (or Option-typed
  fields). `MetricsReporter` trait (`report(&self, report: …)`). `LoggingMetricsReporter` (tracing) +
  `InMemoryMetricsReporter` (stores last report, test-friendly).
- serde JSON matching `ScanReportParser` (the REST `report-metrics` payload) — include if clean.
DEFER (documented): wiring into `TableScan`/`plan_files` to actually collect + emit (concurrent/lazy-stream
instrumentation) — its own supervised increment. Self-contained module `metrics/` (new). Tests: construct +
report to both reporters + JSON round-trip. Widened gate.

#### Increment 3 — BUILD PLAN (BUILDER Opus, 2026-06-08)
- [x] New self-contained module `crates/iceberg/src/metrics/mod.rs` (mirrors Java `metrics/` package);
      `pub mod metrics;` in `lib.rs`.
- [x] `MetricUnit` enum (`Undefined`/`Bytes`/`Count` = Java `MetricsContext.Unit`, serde by displayName
      `undefined`/`bytes`/`count`); `TimeUnit` enum (Java `java.util.concurrent.TimeUnit`, serde lowercase).
- [x] `CounterResult { unit: MetricUnit, value: i64 }`, `TimerResult { time_unit, total_duration: Duration,
      count: i64 }`. `ScanMetricsResult` — every Java metric as `Option<CounterResult>` / `Option<TimerResult>`
      (Java optionality: a never-incremented counter is absent). Full counter set incl. the `dvs`,
      `scanned_delete_manifests`, `skipped_delete_manifests` ones the brief's short list omitted.
- [x] `ScanReport { table_name, snapshot_id, schema_id, projected_field_ids, projected_field_names, filter:
      Predicate, scan_metrics, metadata: HashMap }`. `MetricsReport` ENUM (`Scan(ScanReport)`) — dispatch
      choice = enum over `dyn` (avoids downcasting, illegal states unrepresentable, idiomatic Rust).
- [x] `MetricsReporter` trait `fn report(&self, report: MetricsReport)`. `LoggingMetricsReporter` (tracing
      structured event). `InMemoryMetricsReporter` (Mutex-held last report + `last_report()` / `last_scan_report()`).
- [x] serde: counter/timer shapes + dashed metric names (skip-if-none) match Java `ScanReportParser`/
      `ScanMetricsResultParser`/`CounterResultParser`/`TimerResultParser`. Timer JSON = `{count, time-unit
      (lowercase), total-duration (in unit)}`; counter = `{unit (displayName), value}`. Top-level field names
      `table-name`/`snapshot-id`/`schema-id`/`projected-field-ids`/`projected-field-names`/`filter`/`metrics`.
      DECISION: `filter` serialized via Rust `Predicate`'s OWN serde (NOT Java `ExpressionParser` JSON) — that
      port is large + separate; documented as a deferred divergence. Everything else matches Java.
- [x] Tests: construct+accessor round-trip (Java names); absent counter = None; both reporters; JSON
      round-trip + pinned field/metric names vs hand-written expected JSON. Mutations: drop a counter; rename a
      JSON field; InMemory keep-first.
- DEFER: scan-emission wiring into `plan_files`/`TableScan` (separate supervised increment).

**Outcome (BUILDER Opus, 2026-06-08).** Landed the self-contained scan metrics-report data model + reporter
API in ONE new cohesive module `crates/iceberg/src/metrics/mod.rs` (`pub mod metrics;` in `lib.rs`) — no edits
to `scan/`, `transaction/`, `spec/`, `arrow/`, or datafusion. **Module layout:** metric-name constants (private
`metric_names` submodule, Java `ScanMetrics` strings) → `MetricUnit` + `TimeUnit` enums → `CounterResult` /
`TimerResult` (with a hand-written `Serialize`/`Deserialize` for `TimerResult` so `total-duration` is expressed
in the timer's own unit via a truncating convert, exactly Java `TimerResultParser.fromDuration`/`toDuration`) →
`ScanMetricsResult` (all 17 Java metrics as `Option`, `skip_serializing_if`+`default`, dashed `rename`s) → a
compile-time `const` block asserting the name constants == the serde renames → `ScanReport` → `MetricsReport`
enum → `MetricsReporter` trait + the two reporters. **Dispatch choice:** `MetricsReport` is a closed
`#[non_exhaustive] enum` with a single `Scan(ScanReport)` variant (not a `dyn MetricsReport` trait object) —
mirrors Java's marker interface while avoiding downcasting and making an unknown report kind unrepresentable;
`last_scan_report()` matches exhaustively (no wildcard) so a future variant forces an update. **serde decision:**
the metrics object + counter/timer shapes + all dashed top-level field names match Java's parsers faithfully;
the ONE divergence is the `filter` field — serialized via the Rust `Predicate`'s own serde derive, NOT Java
`ExpressionParser` JSON (a large separate port, documented in the module docs + GAP_MATRIX + Roadmap as a
tracked follow-up). **Java field names verified** against `/tmp/iceberg-java-ref` `metrics/`: `MetricsReport`
(empty marker iface), `MetricsReporter.report(MetricsReport)`, `ScanReport` (table_name/snapshot_id/filter/
schema_id/projectedFieldIds/projectedFieldNames/scanMetrics/metadata), `ScanMetricsResult` (the 17 `@Nullable`
accessors — the brief's short list omitted `scanned_delete_manifests`/`skipped_delete_manifests`/`dvs`, all
included here), `CounterResult` (unit+value), `TimerResult` (timeUnit/totalDuration/count), `ScanMetrics` (the
17 string constants), `MetricsContext.Unit` (undefined/bytes/count), and the four `*Parser`s for the JSON shape.
**Tests (9, each risk-named):** `test_scan_report_fields_round_trip_through_accessors` (Java-named fields),
`test_absent_counter_is_none` (Java optionality), `test_logging_reporter_does_not_panic` +
`test_logging_reporter_emits_a_tracing_event` (captured custom subscriber counts exactly 1 event),
`test_in_memory_reporter_stores_the_report` + `test_in_memory_reporter_keeps_the_last_report`,
`test_scan_report_json_round_trips_and_matches_java_shape` (round-trip + pinned NAMES + counter/timer shapes vs
hand-written expected JSON + absent-omitted), `test_timer_total_duration_is_expressed_in_its_time_unit`,
`test_non_empty_metadata_round_trips`. **Mutations caught (Edit + revert, NOT git checkout):** (a) `#[serde(skip)]`
on `result_data_files` (drop a counter) → JSON round-trip test fails; (b) `rename = "tableName"` (rename a JSON
field) → JSON-shape test fails at the `table-name present` assertion; (c) `InMemoryMetricsReporter::report`
keep-first (`if guard.is_none()`) → keeps-last test fails while stores test stays green. **Gate (all 7, on
`phase3-overnight`):** (1) `cargo build -p iceberg` clean; (2) workspace build (excl. sqllogictest) clean; (3)
`cargo test -p iceberg --lib` TWICE = **1552 passed / 0 failed** both runs (was 1543; +9 metrics tests); (4)
`iceberg-datafusion --lib --tests` 80 + 9 passed; (5) interop manage_snapshots/update_schema/update_partition_spec
4/4 each; (6) clippy `-D warnings` clean (fixed one `manual_map` by matching on `last_report()?`); (7) `fmt
--check` clean + `typos` exit 0. **Scan-wiring is DEFERRED** (no `plan_files`/`TableScan` instrumentation).
**DEVIATION flagged:** added `tracing = { workspace = true }` to `crates/iceberg/Cargo.toml` (the crate had no
logging facade and the brief mandated `tracing`-based logging) — a one-line `Cargo.lock` add (tracing was already
a resolved workspace dep). This touches a dependency file, which the scope rules flag for sign-off; surfaced here
+ in the final report.

**Orchestrator surgery (2026-06-08):** the unapproved dep edit was REJECTED. `LoggingMetricsReporter` + its 2
tests (`test_logging_reporter_does_not_panic`, `test_logging_reporter_emits_a_tracing_event`) were REMOVED
(replaced by a NOTE comment deferring it pending dep approval), and `crates/iceberg/Cargo.toml`/`Cargo.lock`
were reverted to NO `tracing` dep. The increment is now the dependency-free model + `MetricsReporter` trait +
`InMemoryMetricsReporter` + serde. Test count: 7 metrics tests (was 9); `iceberg --lib` = **1550** (was 1552).

**REVIEW (2026-06-08, Opus REVIEWER) — VERDICT 🟡 PASS (docs corrected).** Verified vs `/tmp/iceberg-java-ref`:
all 17 `ScanMetricsResult` metrics present with the right names + counter-vs-timer kind (cross-checked
`ScanMetrics.java` constants + `ScanMetricsResult.java` `@Nullable` accessors); serde JSON shape matches all 4
parsers — top-level field names (`ScanReportParser`), counter `{unit (displayName), value}`
(`CounterResultParser`), timer `{count, time-unit (lowercase), total-duration (in-unit truncating convert)}`
(`TimerResultParser`), dashed metric keys + absent-omitted (`ScanMetricsResultParser`); field types
(`snapshot-id` i64, `schema-id` i32) match Java `long`/`int`. The `filter`-via-`Predicate`-serde divergence is
honestly documented (module docs + GAP_MATRIX + Roadmap) and acceptable for a model-only increment. Dep-removal
is CLEAN: no `tracing::` code (only doc/comment mentions), `git diff HEAD -- Cargo.toml Cargo.lock` EMPTY,
`tracing` not a dep of the `iceberg` crate. `InMemoryMetricsReporter` Mutex-poison handled WITHOUT bare
`.unwrap()` (`unwrap_or_else(|p| p.into_inner())` on both lock sites); `MetricsReport` `#[non_exhaustive]` enum
matched without a wildcard. Ran all 4 mutations (Edit+revert): (a) drop a counter → 2 JSON tests fail; (b)
rename `table-name`→`tableName` → JSON-shape test fails; (c) InMemory keep-first → keeps-last test fails; (d)
drop a `None`-counter's `skip_serializing_if` → absent-omitted assertion fails — ALL 4 caught, no test added.
Gate (on `phase3-overnight`): build `-p iceberg` + workspace (excl. sqllogictest) clean; `--lib` ×2 = 1550/0;
`iceberg-datafusion --lib --tests` 80 + 9; interop manage_snapshots/update_schema/update_partition_spec 4/4 each;
clippy `-D warnings` clean; fmt clean; typos 0; `Cargo.toml`/`Cargo.lock` diff EMPTY. **CHANGES MADE:** corrected
stale post-surgery docs in `GAP_MATRIX.md` + `Roadmap.md` (they still claimed `LoggingMetricsReporter`/`tracing`
landed, 9 tests, and a flagged dep add) to reflect the dependency-free reality (7 tests, 4 mutations, no dep
change, Logging deferred). No source/test change needed. Residual risk: the deferred `filter` JSON (won't
byte-match Java REST until `ExpressionParser` is ported) + the deferred scan-emission (no metrics are COLLECTED
yet — the model is inert until `plan_files` is instrumented).

### Increment 4 — IncrementalChangelogScan (DETAIL)
Increment 3 (ScanReport model) committed (commit hash `0xba590e64`). Java `BaseIncrementalChangelogScan`
(`core/.../BaseIncrementalChangelogScan.java`, 183 lines) — builds on IncrementalAppendScan (Inc 2):
- `orderedChangelogSnapshots(from excl, to incl)` (L103-118): the `ancestorsBetween` range snapshots EXCLUDING
  `Operation::Replace`, oldest-first; **throws UnsupportedOperationException if any has DELETE manifests**
  (L108-111 — current Java scope is data-file changes only; port as `ErrorKind::FeatureUnsupported`).
- change ordinals: oldest snapshot = 0, incrementing (L124-134).
- For each changelog snapshot's DATA manifests IT added (`manifest.snapshotId() ∈ changelogSnapshotIds`),
  `ignoreExisting()`, filter entries to those snapshots, apply the row filter: ADDED entry →
  `AddedRowsScanTask` (INSERT), DELETED entry → `DeletedDataFileScanTask` (DELETE), each carrying
  (changeOrdinal, commitSnapshotId, dataFile, schema, spec, residual) (L136-182).

Rust: new types — `ChangelogOperation { Insert, Delete }` + `ChangelogScanTask { change_ordinal: i32,
commit_snapshot_id: i64, operation: ChangelogOperation, <the data file + schema + project_field_ids +
predicate, reusing FileScanTask fields or embedding a FileScanTask> }`. `Table::incremental_changelog_scan()`
→ builder mirroring `IncrementalAppendScanBuilder` (from excl/incl + to + with_filter + select). Planner mirrors
`scan/incremental.rs` (the range walk + own-added-data-manifest reading) but: includes BOTH Added (→Insert) and
Deleted (→Delete) entries (NOT just Added), excludes Replace snapshots, computes ordinals, guards delete
manifests. Put it in `scan/incremental.rs` (alongside the append scan) — reuse its helpers.

Tests (multi-snapshot fixture): a 2-append range → Insert tasks with correct ordinals (oldest=0); a snapshot
that DELETED a data file (an overwrite/delete that removes a file, no delete-MANIFEST) → a Delete task; Replace
snapshot excluded; a range containing a DELETE-manifest snapshot → FeatureUnsupported error; filter prunes;
from==to empty. Mutation-pin: the ordinal order (oldest=0), the Added→Insert/Deleted→Delete mapping, the
Replace exclusion, the delete-manifest guard. Confirm normal scan + IncrementalAppendScan unchanged. Widened gate.


#### Working plan (BUILDER Opus, 2026-06-08)
- [x] Verify the load-bearing question against Rust writers: a snapshot's OWN added DATA manifest carries the
      `Deleted` tombstones for the files it removed. CONFIRMED via `transaction/snapshot.rs`:
      `rewrite_manifest_with_deletes` writes the rewritten manifest through `new_filtering_manifest_writer`
      with `Some(self.snapshot_id)` (the NEW snapshot id) -> `ManifestFile.added_snapshot_id == new snapshot id`
      (writer.rs:477), and `add_delete_entry` stamps `entry.snapshot_id = self.snapshot_id` (writer.rs:305).
      So `manifest.added_snapshot_id == snapshot.snapshot_id()` selects the deleting snapshot's rewritten
      manifest, and a `Deleted` entry's `snapshot_id()` == the commit snapshot id. Mirrors Java exactly.
- [x] New types in `scan/task.rs`: `ChangelogOperation { Insert, Delete }`, `ChangelogScanTask` (embed a
      `FileScanTask` + change_ordinal/commit_snapshot_id/operation), `ChangelogScanTaskStream` alias.
- [x] `IncrementalChangelogScanBuilder` + `IncrementalChangelogScan` in `scan/incremental.rs`;
      `Table::incremental_changelog_scan()` entry point in `table.rs`.
- [x] Planner: `ordered_changelog_snapshots` (async; Replace-exclusion + delete-manifest guard); ordinals
      oldest->0; per snapshot, build manifest contexts for its OWN added DATA manifests, tag each task with that
      snapshot's ordinal + commit_snapshot_id = entry.snapshot_id(); Added->Insert, Deleted->Delete.
- [x] Tests + 4 mutation checks; confirm normal scan + IncrementalAppendScan unchanged; run the widened gate.

**Outcome (BUILDER Opus, 2026-06-08):** `IncrementalChangelogScan` LANDED 🟡. **Types** (`scan/task.rs`, pub):
`ChangelogOperation { Insert, Delete }` (ports Java `ChangelogOperation`; the CDC-merge `UPDATE_BEFORE`/
`UPDATE_AFTER` variants are intentionally OMITTED + documented), `ChangelogScanTask` (embeds a `FileScanTask`
+ `change_ordinal: i32` + `commit_snapshot_id: i64` + `operation`, with accessors), `ChangelogScanTaskStream`.
**Builder/planner** (`scan/incremental.rs`): `IncrementalChangelogScanBuilder` DELEGATES range resolution +
`PlanContext` construction to `IncrementalAppendScanBuilder` (so the two scans share, and cannot drift, the
non-trivial range/projection logic; a small `pub(crate) plan_context()` accessor was opened on
`IncrementalAppendScan`). `IncrementalChangelogScan::plan_files`: (1) `ordered_changelog_snapshots` walks the
range parent-chain newest-first, EXCLUDES `Operation::Replace`, REJECTS the range with
`FeatureUnsupported("Delete files are currently not supported in changelog scans")` if any kept snapshot's
manifest list carries a `ManifestContentType::Deletes` manifest, then reverses to oldest-first; (2) ordinals
oldest→0; (3) per snapshot, reads the DATA manifests it itself added (`added_snapshot_id == snapshot_id`) via
the REUSED `build_manifest_file_contexts_from_files` (same partition-filter pruning + residual, empty delete
index), and converts `Added`→Insert / `Deleted`→Delete (skip `Existing`), tagging each task with the
snapshot's ordinal + `commit_snapshot_id = entry.snapshot_id()` (fallback the snapshot id). `Table::
incremental_changelog_scan()` is the entry point. **Java lines verified** against
`/tmp/iceberg-java-ref/.../BaseIncrementalChangelogScan.java`: `doPlanFiles` L56-93, `orderedChangelogSnapshots`
L103-118 (ancestorsBetween range, Replace-exclusion, delete-manifest throw L108-111), `computeSnapshotOrdinals`
L124-134, `CreateDataFileChangeTasks` L136-182 (Added→AddedRowsScanTask=INSERT, Deleted→DeletedDataFileScanTask
=DELETE, `commitSnapshotId = entry.snapshotId()`). **Load-bearing question RESOLVED:** a snapshot's OWN added
DATA manifest DOES carry the `Deleted` tombstones for files it removed — `rewrite_manifest_with_deletes` writes
the rewritten manifest through a writer stamped with the new snapshot id (`added_snapshot_id == snapshot_id`,
`writer.rs:477`) and `add_delete_entry` stamps the `Deleted` entry's `snapshot_id` = that snapshot
(`writer.rs:305`); confirmed by `test_changelog_overwrite_emits_delete_for_removed_file` passing. **Tests (8,
risk-named):** ordinal scheme oldest=0 + commit-id; overwrite emits DELETE(removed)+INSERT(added); Replace
excluded; delete-manifest range → FeatureUnsupported; with_filter partition prune; inclusive from==to = only that
change; exclusive from==to rejected; carried-forward only-own-added. **Mutations (Edit+revert, all caught):**
(a) newest=0 ordinal → ordinal test fails; (b) Deleted→Insert → delete test fails; (c) include Replace →
Replace-excluded test fails; (d) drop delete-manifest guard → FeatureUnsupported test fails. **Normal scan +
IncrementalAppendScan UNCHANGED** (the 13 append-scan + the regular scan tests stay green; only an additive
`pub(crate)` accessor was added). **Gate (on `phase3-overnight`):** `build -p iceberg` clean; workspace
all-targets (excl. sqllogictest) clean; `--lib` ×2 = 1558/0 (1550 prior + 8); `scan::` 64/0; datafusion
`--lib --tests` 80 + 9; interop manage_snapshots/update_schema/update_partition_spec 4/4/4; clippy `-D warnings`
clean; fmt clean; typos 0. **NO Cargo edits.**

### Increment 5 — TableScan use_ref (scan a branch/tag by name) (DETAIL)
Increment 4 (IncrementalChangelogScan) committed `4a772640`. Java `TableScan.useRef(String ref)`
(`api/.../TableScan.java:48`, impl in `BaseTableScan`/`SnapshotScan.useRef`): select the snapshot a
branch/tag reference points to. Rust `TableScanBuilder` has `snapshot_id(i64)` but NO ref-based selector;
`TableMetadata::snapshot_for_ref(name) -> Option<&SnapshotRef>` already exists. Bounded, self-contained,
low-risk, valuable (branch/tag reads).

Build: add `snapshot_ref: Option<String>` to `TableScanBuilder` + `use_ref(impl Into<String>)`; in `build()`
resolve the ref via `metadata.snapshot_for_ref(&name)` → its snapshot (error `DataInvalid`/`Unexpected` if the
ref doesn't exist). Java rejects combining `useRef` with `useSnapshot`/`asOfTime` — reject `use_ref` + 
`snapshot_id` both set (a clear error). Otherwise scan the resolved snapshot exactly like `snapshot_id`.
Tests: `use_ref("main")` scans the current snapshot; a tag/branch ref scans its snapshot; unknown ref → error;
`use_ref` + `snapshot_id` both set → error; default (neither) unchanged. Mutation-pin the ref resolution + the
both-set rejection. Confirm existing scans unchanged. Widened gate. ONLY `scan/mod.rs` + docs.

Plan (BUILDER Opus, 2026-06-08):
- [x] Read Java `SnapshotScan.useRef` (L116-128): MAIN_BRANCH ⇒ table default; else reject if snapshot id
      already set ("Cannot override ref…"); resolve `table().snapshot(name)`; reject null ("Cannot find ref %s").
- [x] Add `snapshot_ref: Option<String>` field (init `None`) + `pub fn use_ref(impl Into<String>)`.
- [x] In `build()`: both-set ⇒ `DataInvalid`; ref-set ⇒ `snapshot_for_ref` (unknown ⇒ `DataInvalid` with name);
      else unchanged `snapshot_id`-or-current logic.
- [x] Tests against the existing `example_table_metadata_v2.json` fixture: `test` tag → older snapshot
      `3051729675574597004`; `main` → current; unknown ref → err; both-set → err; default unchanged; plus a
      result-equivalence read through `use_ref("main")`.
- [x] Mutations (Edit+revert): ignore `snapshot_ref`; drop both-set rejection; drop unknown-ref error.
- [x] Docs: GAP_MATRIX, Roadmap, todo Outcome, lessons (if non-obvious).

**Outcome (2026-06-08, Increment 5 — TableScan use_ref, BUILDER Opus):** `TableScanBuilder::use_ref(impl
Into<String>)` lands in `crates/iceberg/src/scan/mod.rs`, mirroring Java `SnapshotScan.useRef`
(`core/SnapshotScan.java` L116-128). A new `snapshot_ref: Option<String>` field (init `None`) records the
intent; `build()` resolves BOTH selectors up front via a `match (self.snapshot_id, self.snapshot_ref)`:
ref+snapshot-id ⇒ `DataInvalid` ("Cannot scan using both a ref … and a snapshot id", Java "Cannot override
ref"); ref-only ⇒ `TableMetadata::snapshot_for_ref(name)` (the EXISTING accessor — no `spec/` change), unknown
⇒ `DataInvalid` ("snapshot ref '…' not found", Java "Cannot find ref %s"); else the ORIGINAL `snapshot_id`-or-
current logic flows unchanged (including the no-current-snapshot empty-scan early return). `"main"` resolves to
the current snapshot because the parse path auto-injects a `main` ref at the current snapshot id
(`table_metadata.rs` `TryFrom`), matching Java `useRef(MAIN_BRANCH)` returning the table default — so no special
`MAIN_BRANCH` arm is needed. 6 unit tests over the shared `example_table_metadata_v2.json` fixture (which
already carries two snapshots + a `test` TAG pointing at the OLDER one): `main`→current, `test`→older snapshot
(asserts ≠ current — the core branch/tag-read behavior), unknown→err+names-ref, ref+id→err (order-independent),
default-unchanged, and a planning result-equivalence read (`use_ref("main")` plans the same 2 files as a plain
`.build()`). Mutations (Edit+revert): (a) ignore `snapshot_ref` (always current) → tag test fails 3055…≠3051…;
(b) drop the both-set rejection (id wins) → both-set test fails; (c) drop the unknown-ref error (fall back to
current) → unknown-ref test fails — all caught. Existing scans UNCHANGED: the full `scan::` suite stayed green
(70 tests, 64 prior + 6 new). Gate green on `phase3-overnight`. ONLY `scan/mod.rs` + docs touched; NO Cargo
edits, NO `spec/` change.

#### Increment 5 — REVIEW (2026-06-08, Opus REVIEWER, DELEGATED) — CHANGES-MADE (PASS 🟡)
Verdict: PASS 🟡 with one test added. Resolution logic confirmed faithful to Java `SnapshotScan.useRef`
L116-128 (both-set→reject, unknown→reject, `"main"`/ref→correct snapshot via the auto-injected main ref); the
default no-`use_ref` path is byte-unchanged (the new pre-resolution `match` only computes a `snapshot_id`, then
the ORIGINAL `match snapshot_id` block — incl. the empty-table early return — is untouched bar `self.snapshot_id`
→ `snapshot_id`). Test-strength: the core different-ref test pins the resolved snapshot id (the load-bearing
logic), and full planning is separately proven via `use_ref("main")` — judged acceptable, and STRENGTHENED with
`test_plan_files_use_non_main_ref_plans_referenced_snapshot` (adds a non-main tag at the current snapshot via
`set_ref`, then plans through it = the default file set), guarding against any future special-casing of `"main"`
in planning. Re-ran all 3 builder mutations + a 4th (swap both-set to silently resolve the ref instead of
erroring) — all 4 caught. The `use_ref("main")`-on-an-empty-table divergence (Java returns an empty-default;
Rust errors `DataInvalid` because no `main` ref is injected without a current snapshot) is narrow, documented,
and a LOUD error (no silent corruption) — judged acceptable. Engineering floor: no bare `.unwrap()` in
production (resolution uses `.ok_or_else`/`?`); test `.unwrap()` matches the local module convention; error
kinds/messages name the ref. Scope clean: `scan/mod.rs` + the 4 docs only; no `spec/`/Cargo edits. Lib total now
1565/0 (×2 stable), `scan::` 71/0, interop 4+4+4, clippy/fmt/typos clean. Stayed on `phase3-overnight`; no
commit/push.

---

## SEQUENCE: RowDelta delete-file conflict validation (Phase 3, started 2026-06-08)
Branch `phase3-rowdelta-delete-conflicts` (off merged `main` `063b7153`). Completes the RowDelta serializable-
isolation surface beyond the data-file conflict check (which landed in the residual sequence Inc 4).

### Increment — RowDelta validateNoConflictingDeleteFiles (THIS increment)
Java `BaseRowDelta.validate` → `validateNewDeleteFiles` → `validateNoNewDeleteFiles(base, startingSnapshotId,
conflictDetectionFilter, parent)` (`MergingSnapshotProducer.java:562-570`): enumerate DELETE files ADDED by
concurrent commits since the starting snapshot, and reject if ANY could apply to records matching the conflict
filter. The error: "Found new conflicting delete files that can apply to records matching %s: %s".

**Building blocks + Java authority (CONFIRMED):**
- `addedDeleteFiles` (`MergingSnapshotProducer.java:601-625`): V2-ONLY (`base.formatVersion() < 2` ⇒ empty —
  delete files don't exist in V1); `validationHistory(..., VALIDATE_ADDED_DELETE_FILES_OPERATIONS,
  ManifestContent.DELETES, ...)` then a `DeleteFileIndex` filtered by the dataFilter + startingSequenceNumber.
  `VALIDATE_ADDED_DELETE_FILES_OPERATIONS = {OVERWRITE, DELETE}` (vs `{APPEND, OVERWRITE}` for data).
- Rust model: `added_data_files_after` (`transaction/snapshot.rs:980`) is the data-file analogue — same walk,
  EXCLUSIVE of start, only manifests the snapshot itself added, only `Added` entries. The delete version
  differs ONLY in the manifest content filter (`Deletes`) + the operation set (`Overwrite | Delete`) + the V2
  guard. **Generalize the walk** into a private `added_files_after(table, start, content, op_predicate)`;
  `added_data_files_after` + new `added_delete_files_after` both call it (Rule-of-Three on the walk).
- Conflict TEST = the SAME `InclusiveMetricsEvaluator` per file used by the data check; extract a shared
  `first_conflicting_file(files, current, conflict_filter, case_sensitive) -> Result<Option<DataFile>>` and
  have the existing `validate_no_conflicting_added_data_files` (refactor, behavior-preserving — Inc-4
  OverwriteFiles + RowDelta data tests stay green) AND a new `validate_no_conflicting_added_delete_files` (with
  the DELETE error message) use it.

Plan:
- [x] `transaction/snapshot.rs`: generalize the walk → `added_files_after`; add `added_delete_files_after`
      (content=Deletes, ops=Overwrite|Delete, V2 guard `format_version() < FormatVersion::V2 ⇒ empty`); extract
      `first_conflicting_file` (the inclusive-metrics test); add `validate_no_conflicting_added_delete_files`
      using it + the delete message; refactor `validate_no_conflicting_added_data_files` onto the shared test
      (no behavior change). `added_data_files_after` enumeration semantics unchanged.
- [x] `transaction/row_delta.rs`: add `validate_no_conflicting_delete_files: bool` field + builder; extend
      `validate` — after the data check, if enabled run the delete check (same `effective_start`,
      `conflict_detection_filter`, case-sensitive true). Module doc: drop `validateNoConflictingDeleteFiles`
      from deferred (note `validateDataFilesExist` + `validateNoNewDeletesForDataFiles` + V3 `validateAddedDVs`
      still deferred — they need `referenced_data_files`/`removed_data_files` on the action, which it lacks).
- [x] Tests (MemoryCatalog + a real concurrent commit that ADDS a delete file via a RowDelta/overwrite path):
      no-concurrent-delete → OK; concurrent commit adds a DELETE file whose metrics MATCH the filter → rejected
      non-retryable `DataInvalid` (names the file, delete-specific message); metrics-EXCLUDE → OK; flag OFF →
      no delete check; None filter → any concurrent delete conflicts; V1 table → no delete check (guard);
      data-check and delete-check independent (enabling one doesn't enable the other). Mutation-pin: the
      content filter (Data vs Deletes in the walk), the metrics decision, the non-retryable kind. Keep Inc-4
      data tests green (shared-test refactor proof).
- [x] Docs: GAP_MATRIX (RowDelta validateNoConflictingDeleteFiles 🟡 + the seq-number/DeleteFileIndex precision
      deferral), Roadmap, todo, lessons.
- [x] Verify from repo root: build -p iceberg + workspace; lib ×2; transaction::; datafusion tests; 3 interop;
      clippy (workspace); fmt.

**Outcome (2026-06-08, BUILDER Opus):** RowDelta `validateNoConflictingDeleteFiles` landed 🟡.
- **Generalized walk.** Factored the data-file walk into a private `async fn added_files_after(table,
  starting_snapshot_id, content: ManifestContentType, operation_adds: fn(&Operation)->bool) ->
  Result<Vec<DataFile>>` (Java `validationHistory`). `added_data_files_after` is now a thin call
  `(Data, operation_adds_data_files)`; its observable behavior is unchanged — the existing OverwriteFiles /
  ReplacePartitions / RowDelta data conflict tests (241 transaction:: tests) stayed green unchanged. Added
  `pub(crate) added_delete_files_after` = `(Deletes, operation_adds_delete_files)` with the V2 guard
  (`format_version() < FormatVersion::V2 ⇒ Ok(vec![])`, Java `addedDeleteFiles` L608). New
  `operation_adds_delete_files = matches!(op, Overwrite | Delete)` (Java `VALIDATE_ADDED_DELETE_FILES_OPERATIONS`,
  distinct from data's `{Append, Overwrite}`).
- **Shared per-file test extraction.** Extracted `fn first_conflicting_file(files: &[DataFile], current:
  &Table, conflict_filter: Option<&Predicate>, case_sensitive) -> Result<Option<DataFile>>` (bind None⇒AlwaysTrue
  once, per-file `InclusiveMetricsEvaluator::eval`, return first match). Refactored
  `validate_no_conflicting_added_data_files` onto it (DATA message unchanged) and added
  `validate_no_conflicting_added_delete_files` onto it (DELETE message "Found new conflicting delete files that
  can apply to records matching {filter}: {path}"). Behavior-preservation proof: the data path's tests pass
  unchanged.
- **RowDelta wiring.** Added `validate_no_conflicting_delete_files: bool` + builder
  `validate_no_conflicting_delete_files()`. The `validate` override now runs the data check (if its flag) THEN
  the delete check (if ITS flag), sharing `effective_start = validate_from_snapshot.or(starting)` +
  `conflict_detection_filter`; either failing rejects. The two flags are independent.
- **V2 guard finding (honest):** the V1 end-to-end test STILL passes with the guard dropped — the concurrent
  commit on V1 is an `Append` (excluded by `{Overwrite,Delete}`) and no DELETE manifests can exist on V1, so the
  walk naturally yields nothing. The guard is a Java-faithful short-circuit (avoids a needless walk), NOT
  behavior-changing for this scenario. Documented as such; the V1 test also asserts `added_delete_files_after`
  directly returns empty.
- **Over-scan (documented):** the port omits Java's `DeleteFileIndex` `startingSequenceNumber` refinement —
  conservative (over-reject only), same class as Inc-3's manifest-summary pre-filter deferral.
- **Java lines verified:** `BaseRowDelta.validate` L131-174 (the `validateNewDeleteFiles` branch L159-167; the
  `!removedDataFiles.isEmpty()` sub-branch L161-164 is unreachable here — the action has no `removeRows` — so it
  is correctly out of scope); `MergingSnapshotProducer.validateNoNewDeleteFiles` L562-570 (the error message);
  `addedDeleteFiles` L601-625 (V2 guard L608, `VALIDATE_ADDED_DELETE_FILES_OPERATIONS` + `ManifestContent.DELETES`
  L614-620); the op sets L73-85 (`{OVERWRITE, DELETE}` for deletes vs `{APPEND, OVERWRITE}` for data).
- **Tests (8 new delete-conflict, each risk-named):** `..._delete_validation_no_concurrent_commit_succeeds`
  (race-free → OK); `..._rejects_concurrent_added_delete_file_matching_filter` (HEADLINE — non-retryable +
  names the file + DELETE-specific message asserted distinct from the data message); `..._allows_concurrent_added_delete_file_excluded_by_filter`
  (EXCLUDE → OK); `..._without_delete_validation_allows_conflicting_concurrent_delete` (flag OFF → snapshot
  isolation); `..._delete_none_filter_treats_any_concurrent_delete_as_conflict` (None⇒AlwaysTrue);
  `..._delete_check_is_noop_on_v1_table` (V2 guard, + direct `added_delete_files_after` assertion);
  `..._delete_check_does_not_run_data_check` + `..._data_check_does_not_run_delete_check` (independence both
  ways). 27 row_delta tests total (was 19).
- **Mutations run + caught:** (a) `added_delete_files_after` walks Data not Deletes → the headline delete test
  fails (content filter load-bearing); (b) invert `first_conflicting_file` decision → the EXCLUDE test fails for
  BOTH overwrite_files (data) AND row_delta (delete) — shared test load-bearing for both; (c) delete error made
  retryable → `!retryable()` assertion fails + the retry loop spins (0.07s→1.55s); (d) drop the V2 guard → the
  V1 test STILL passes (walk yields nothing — guard is a faithful short-circuit, documented).
- **Gate (all 8 green):** (1) build -p iceberg OK; (2) build --workspace --exclude iceberg-sqllogictest
  --all-targets OK; (3) `cargo test -p iceberg --lib` ×2 = **1524 passed, 0 failed** both runs (new lib total
  1524; was 1516); (4) `transaction::` = 241 passed (Inc-3 OverwriteFiles + Inc-4 RowDelta data tests
  UNCHANGED-green); (5) iceberg-datafusion lib 80 + integration 9 pass; the ONE failing doctest
  (`table_provider_factory.rs` line 41, `rt-multi-thread` disabled under `#[tokio::main]`) is PRE-EXISTING +
  environmental — fails identically with my changes git-stashed; (6) interop_manage_snapshots / interop_update_schema
  / interop_update_partition_spec = 4/4 each; (7) clippy --workspace --exclude iceberg-sqllogictest --all-targets
  -D warnings clean; (8) fmt --check clean. No commit, no branch switch, edits only in snapshot.rs / row_delta.rs +
  docs (GAP_MATRIX, Roadmap, todo, lessons).

**Note (over-scan, document):** Java's `addedDeleteFiles` additionally filters the DeleteFileIndex by
`startingSequenceNumber`; this port enumerates concurrent-added delete files by the snapshot walk + inclusive
metrics only (no seq-number refinement) — a conservative over-scan (can only over-reject, never under-reject),
same class as Inc-3's manifest-summary pre-filter deferral.

---

## Active (2026-06-09): Inspection-table interop — `snapshots` + `refs` (Phase 3 interop Increment 1)

Increment: the FIRST byte/field-level "read a table Java wrote" evidence for the inspection tables — flips the
`snapshots` + `refs` inspection rows to interop-✅ (the rest of the inspection set stays 🟡). Builder→reviewer
actor-critic via Workflow; orchestrator independently re-ran the full gate + committed. **Purely additive — NO
production-code change.**

- [x] **Java oracle** (`dev/java-interop/.../InteropOracle.java`) — new `generate-inspection` exec mode +
  nested `InspectionOracle` + dedicated `InMemoryInspectionOperations` (its `io()` returns a real
  `InMemoryFileIO` with the metadata file pre-`addFile`d). Builds a purpose-rich V2 base (3 snapshots
  ROOT/CURRENT/SIBLING — CURRENT carries a MULTI-KEY summary, ROOT operation-only → empty map; refs main
  branch-no-retention / dev branch-full-retention / stable tag-only-max-ref-age), writes `base.metadata.json`,
  RE-PARSES it (`TableMetadataParser.fromJson`), then materializes Java's REAL `SnapshotsTable`/`RefsTable`
  rows via `MetadataTableUtils.createMetadataTableInstance` + `mt.newScan().planFiles()` +
  `task.asDataTask().rows()`, serialized by `JsonUtil`/`JsonGenerator` to `java_{snapshots,refs}.json`. No
  Maven deps added; pom untouched.
- [x] **Rust test** (`crates/iceberg/tests/interop_inspection.rs`, 2 tests) — loads the same
  `base.metadata.json`, runs `inspect().snapshots()/.refs().scan()`, extracts every Arrow column, asserts
  field-for-field equality vs the Java rows ORDER-INDEPENDENTLY (snapshots by id, refs by name; summary as a
  `HashMap`) + focused named assertions (committed_at micros, empty-vs-multi-key summary, operation-not-in-map,
  retention NULL-per-kind).
- [x] **Gate (orchestrator-rerun, all green):** iceberg lib 1595, interop_inspection 2/2, the other 3 interop
  suites 4/4/4, datafusion 80+9, clippy/fmt/typos clean; `git status` = only InteropOracle.java +
  testdata/interop/inspection/ + the new test (no existing fixtures changed).
- [x] **Docs** — GAP_MATRIX (interop note on the inspection row), lessons, todo.

**Key finding (memory `reference_java_snapshot_summary_operation.md`):** Java `SnapshotParser.fromJson` splits
`operation` OUT of the summary map on the on-disk round-trip ⇒ Rust's `additional_properties`-only summary
column already matches Java's RE-PARSED `summary()` — NO Rust change. Verified against `/tmp/iceberg-java-ref`
1.10.0 before any edit (averted a wrong "fix"). **Gotcha:** Java `StaticDataTask.rows()` is a lazy transform
over ONE mutable projection — serialize each row eagerly, never stash `StructLike`s.

**Deferred (next interop increments):** `history` + `metadata_log_entries` (need a multi-entry snapshot-log +
metadata-log fixture so `is_current_ancestor=false` and the `latest_*` as-of-time columns are exercised); then
the manifest-reading tables `files`/`entries`/`manifests`/`partitions`/`all_*` + scan interop (need real
on-disk manifests + parquet, a bigger harness step).

---

## Active (2026-06-09): Inspection-table interop — `history` + `metadata_log_entries` (Phase 3 interop Increment 2)

Increment: interop evidence for the two DERIVED-column pure-metadata inspection tables — completes interop
for ALL FOUR pure-metadata inspection tables. Builder→reviewer (validation-first, per user "validation is
key"); orchestrator independently re-ran the full gate + committed. **Purely additive — NO production change.**

- [x] **Java oracle** — new `generate-inspection-log` exec mode + `InspectionLogOracle` (reuses the prior
  increment's `InMemoryInspectionOperations` / `RowWriter` / `rowsToJson`). Builds a FORKED snapshot-log via
  the 3-commit RE-PARSE recipe (B0 ROOT→main, B1 SIBLING→main, B2 CURRENT→main, re-parse between each to
  dodge intermediate-snapshot pruning) → log `[ROOT@2018-01, SIBLING@2018-08, CURRENT@2019-04]`,
  CURRENT.parent=ROOT ⇒ SIBLING off-ancestry. INJECTS a deterministic `metadata-log` (3 straddling entries)
  via `JsonUtil.mapper()`, re-parses with a stable logical URI, materializes Java's REAL
  `HistoryTable`/`MetadataLogEntriesTable` rows → `java_history.json` / `java_metadata_log_entries.json`
  under `testdata/interop/inspection_history/`.
- [x] **Rust tests (2, added to `interop_inspection.rs`)** — `inspect().history()/.metadata_log_entries()
  .scan()` asserted field-for-field equal vs the Java rows (all 4 / all 5 columns, order-independent —
  history by composite `(made_current_at,snapshot_id)`, log by timestamp) + focused pins:
  `is_current_ancestor` true/FALSE/true (ROOT/SIBLING/CURRENT); `latest_*` NULL/ROOT/SIBLING/CURRENT (with
  schema_id 0 + seq 1/2/3); synthetic-entry `file` == the stable metadata location.
- [x] **Gate (orchestrator-rerun, all green):** iceberg lib 1595, interop_inspection 4/4, other 3 interop
  4/4/4, datafusion 80+9, clippy/fmt/typos clean; `git status` = only `InteropOracle.java` +
  `interop_inspection.rs` + `inspection_history/` (existing `inspection/` fixtures untouched).
- [x] **Docs** — GAP_MATRIX, Roadmap (6c), lessons, todo.

**Key recipe (lessons):** forked snapshot-log needs SEPARATE commits + re-parse between (Java
`intermediateSnapshotIdSet` pruning); deterministic `latest_*` needs an INJECTED metadata-log (real commits
stamp ≈now); Java `addSnapshot` sets `lastUpdatedMillis = snapshot.ts`, so the synthetic entry lands on
`CURRENT_TS`, bonus-pinning the `<=` boundary; pin `metadata_location` to a stable URI on both sides.
Reviewer mutation-probed the fixture (flip SIBLING ancestor; flip creation-row NULL) — each fails the test.

**Deferred (next):** manifest-reading inspection tables (`files`/`entries`/`manifests`/`partitions`/`all_*`)
+ scan interop — need real on-disk manifests + parquet (a bigger harness step: the oracle must write actual
manifest/data files, or the Rust side must read Java-written ones).

---

## Active (2026-06-09): Manifest-reading interop A1 — `files`/`data_files`/`delete_files` (Phase 3)

Increment: FOUNDATION of the manifest-reading inspection interop. User chose **item (a)** (take on the
manifest-reading tables) and **run.sh-driven** wiring. Builder→reviewer; orchestrator independently re-ran
the offline gate AND the run.sh round-trip + committed. **Purely additive — NO production change.**

- [x] **Java oracle** — `generate-inspection-manifests` mode + `InspectionManifestsOracle` + minimal
  `LocalFileIO` (`Files.localOutput/localInput`) + `LocalTableOperations` (commit writes metadata to disk).
  Writes a REAL partitioned V2 table (no parquet/hadoop deps): `newAppend` 2 data files (2 partitions, with
  metrics + id/value bounds) then `newRowDelta` 1 position-delete; materializes Java's REAL `FilesTable`
  rows (`MetadataTableUtils` + `planFiles` + `ManifestReadTask.asDataTask().rows()`) → java_{files,data_files,
  delete_files}.json + the table under a gitignored `target/` temp dir.
- [x] **Rust test** (`interop_inspection_manifests.rs`) — ENV-GATED (`ICEBERG_INTEROP_MANIFEST_DIR`),
  runtime early-return no-op when unset (offline gate stays green; NOT `#[ignore]`). When set: load
  `final.metadata.json`, build a Table with `FileIO::new_with_fs()`, scan files/data_files/delete_files,
  field-match all 21 non-derived columns order-independently (bounds as raw bytes; content filter pinned).
- [x] **run script** `dev/java-interop/run-inspection-manifests.sh` (mvn generate → env-gated cargo test).
- [x] **Gate (orchestrator-rerun, all green):** offline (manifests test no-op 1 passed; interop_inspection
  4/4; workspace build/clippy/fmt/typos clean) + run.sh round-trip (files=3/data_files=2/delete_files=1
  matched). `git status` = only InteropOracle.java + interop_inspection_manifests.rs + run-inspection-manifests.sh.
- [x] **Docs** — GAP_MATRIX, Roadmap (6d), lessons, todo.

**Findings (lessons):** Java writes real manifests with NO parquet/hadoop (LocalFileIO + real commits);
run.sh-driven env-gated test keeps the offline gate green; VERIFY on-disk bytes before calling a render a
divergence. **Two documented presentation divergences (on-disk MATCHES; not bugs):** `file_format` case
(Java row UPPERCASE via the FileFormat enum vs Rust lowercase Display — a small inspection-table-only parity
gap) + absent metric-map `{}` vs `null` (Rust non-optional maps). readable_metrics deferred from comparison.

### FOLLOW-UP DONE (2026-06-09): `file_format` UPPERCASE in inspection tables
`inspect/data_file.rs` now emits `file_format` upper-cased (`.to_string().to_uppercase()`) for the shared
files/entries/all_* projection, matching Java's `FilesTable` enum-NAME rendering. On-disk write UNCHANGED
(`DataFileFormat::Display`/serde, still lowercase; `git diff crates/iceberg/src/spec` empty). The A1 interop
test was tightened to EXACT file_format equality (canonicalization dropped). No unit-snapshot churn (the
inspect unit tests assert only the Arrow schema, never the rendered value); datafusion/sqllogictest had no
file_format value expectation. Gate + run.sh round-trip green. Own commit (fix(inspect)).

**Deferred next (manifest interop):** A2 `entries`/`manifests`/`partitions` (same harness); A3 `all_*`
(multi-snapshot table); A4 scan PLANNING (file set + residuals); A5 scan EXECUTION (reads parquet → Arrow —
needs parquet Maven deps + a separate approval).

---

## Active (2026-06-09): Manifest-reading interop A2 — `entries`/`manifests`/`partitions` (Phase 3)

Increment: 3 more manifest-reading tables over a RICHER multi-snapshot table (`InspectionManifestsA2Oracle`
→ `<dir>/table_a2`; A1's `<dir>/table` + test untouched). Builder→reviewer; orchestrator independently
re-ran the offline gate + the run.sh round-trip + verified the production fix + committed.

- [x] **Java oracle** — A2 table: `newAppend` A=a/B=b/C=a/D=b → `newRowDelta` +pos-delete (cat=a) →
  `newDelete` B → DELETED tombstone + content-gated DATA/DELETE manifests + 2 live partitions. Materializes
  Java's REAL `EntriesTable`/`ManifestsTable`/`PartitionsTable` rows → java_{entries,manifests,partitions}.json.
- [x] **Rust tests (3, added to `interop_inspection_manifests.rs`)** — entries/manifests/partitions field-match
  Java order-independently; entries reuses A1's `FileRow` for the nested data_file via a new `ColumnSource`
  trait; focused pins: status==2 tombstone, content-gating (data manifest 0 delete-counts / delete manifest 0
  data-counts), partition delete-counts + DATA-only `total_data_file_size_in_bytes`.
- [x] **Production parity fix** (`inspect/partition_summary.rs`) — `partition_summaries` STRING bounds were
  JSON-quoted (`Datum::to_string`) vs Java's bare `Transform.toHumanString`; fixed to `to_human_string` (only
  string bounds change; int/long unit tests unaffected). Verified both the Rust method semantics (datum.rs:1195)
  AND the Java call-site (ManifestsTable L134-144).
- [x] **Gate (orchestrator-rerun, all green):** offline (A2 tests no-op; A1 still green; lib 1595; datafusion
  80+9; clippy/fmt/typos) + run.sh round-trip (A1 + 3 A2). `git status` = partition_summary.rs +
  interop_inspection_manifests.rs + InteropOracle.java + run-inspection-manifests.sh.
- [x] **Docs** — GAP_MATRIX, Roadmap (6e), lessons, todo.

**Deferred next:** A3 `all_*` (all_data_files/all_delete_files/all_files/all_entries/all_manifests —
multi-snapshot reachability + non-dedup for all_manifests); A4 scan PLANNING (planFiles file set + residuals);
A5 scan EXECUTION (parquet → Arrow, needs parquet Maven deps + separate approval). Then squash + PR.

---

## Active (2026-06-09): Manifest-reading interop A3 — the five cross-snapshot `all_*` tables (Phase 3)

Increment: `all_data_files`/`all_delete_files`/`all_files`/`all_entries`/`all_manifests` interop, REUSING the
A2 table read-only. **COMPLETES manifest-reading interop for every inspection table.** Purely additive — NO
production change. Builder→reviewer; orchestrator re-ran gate + run.sh + fixed the stale run.sh echo + committed.

- [x] **Java oracle** — extended the A2 generate step to ALSO materialize Java's REAL `All*` rows over the
  same `table_a2` → java_all_{data_files,delete_files,files,entries,manifests}.json (generic `rowsToJson`,
  `all_manifests`'s `reference_snapshot_id` is just another scalar).
- [x] **Rust tests (5, added to `interop_inspection_manifests.rs`)** — reuse FileRow/EntryRow/ManifestRow
  extraction (+ an AllManifestRow wrapper). Order-independent MULTISET comparison (sort full rows by Debug,
  element-wise, NO dedup — these tables may return duplicates). Focused pins: cross-snapshot reach (B present
  in all_*, absent in current data_files); duplicates preserved (8 rows / 5 distinct paths); all_manifests
  non-dedup (shared manifest → 2 rows, reference_snapshot_id distinct + ≠ added_snapshot_id).
- [x] **Gate (orchestrator-rerun, all green):** offline (A3 no-op; A1+A2 green; lib 1595; datafusion 80+9;
  clippy/fmt/typos) + run.sh round-trip (9 tests = A1 + 3 A2 + 5 A3). Fixed the stale run.sh header/echo (A3).
- [x] **Docs** — GAP_MATRIX, Roadmap (6f), lessons, todo.

**Manifest-reading interop COMPLETE for all inspection tables.** Remaining inspection interop: the
`readable_metrics` virtual column (interior field-id JVM-order residual); SCAN interop — A4 PLANNING (planFiles
file set + residuals, no parquet needed) + A5 EXECUTION (parquet → Arrow, needs parquet Maven deps + approval).
Then: squash the `phase2-3-remnants` set (now ~9 commits, LOCAL-only) + open the PR.

---

## Active (2026-06-09): Scan-PLANNING interop A4 (Phase 3) — DONE

Increment: scan-planning interop. Java plans 4 filter scenarios via REAL `table.newScan().filter().planFiles()`
over a dedicated `table_a4` (F1/F2/F3 with distinct `id` bounds + a position-delete on F1, identity(category));
Rust plans the same via `table.scan().with_filter().plan_files()`; {planned file SET, per-file delete paths,
residual-always-true} match EXACTLY. Purely additive — NO production change.

- [x] **Java oracle** — `InspectionScanA4Oracle` writes `table_a4` + plans s0 no_filter / s1 partition_a /
  s2 metric_id_gt_15 / s3 combined → java_scan_*.json.
- [x] **Rust test** — `test_scan_planning_matches_java_plans` (added to interop_inspection_manifests.rs).
  Pins partition pruning (s1 drops F2), COLUMN-METRIC pruning (s2 drops F1 via upper bound 10), combined
  (s3 = F3), residual-covered split, delete association (cat=a delete on BOTH F1+F3). Residual-EXPRESSION
  string equality DEFERRED (cross-language normalization; Rust residuals unit-tested).
- [x] **Gate (orchestrator-rerun, all green):** offline (A4 no-op; A1-A3 green; lib 1595; datafusion 80+9;
  clippy/fmt/typos) + run.sh round-trip (10 tests). spec/ untouched (0 lines).
- [x] **Docs** — GAP_MATRIX (scan-planning row), Roadmap (6g), lessons, todo.

### NEXT: squash `phase2-3-remnants` + open PR (awaiting user go-ahead on squash-vs-keep + confirm base = the origin fork, NEVER apache upstream)
Branch = ~10 commits off main `ccfcb062` (3 Phase-2/3 remnants + 4 pure-metadata/manifest-A1 interop + file_format fix + A2 + A3 + A4). All LOCAL-only, not pushed. Deferred beyond this PR: `readable_metrics` interop; A5 scan EXECUTION (parquet deps + approval); other Phase 2/3 remnants (overwriteByRowFilter, RowDelta validateNoNewDeletesForDataFiles, BatchScan, CDC-merge).

**MERGED: `5f32b10d` "Phase2 3 remnants (#8)" → `5ed1582b` "Phase2 write validation (#9)" → `6c07ea18` "ci: free disk space (#10)". `main` now at `6c07ea18`. The write-validation cluster (#9) and the CI free-disk-space fix (#10) are MERGED; the data-level interop capstone (below) is pushed (branch `phase3-scan-exec-interop`, 4 commits) + PR-ready.**

---

## Active (2026-06-09): DATA-LEVEL write/scan interop capstone (branch `phase3-scan-exec-interop` off main `5f32b10d`)

User chose this as the highest-value next sequence (flips write actions + Phase-3 scan-execution toward ✅ via
real-row interop). Run.sh-driven, env-gated. The Java oracle now writes REAL parquet data + delete files
(approved deps in the oracle pom: `iceberg-data` + `iceberg-parquet` + `hadoop-client-runtime`; Rust Cargo
FROZEN, 0-diff).

- [x] **Increment 1: scan-execution merge-on-read interop (position deletes)** — `ScanExecOracle` /
  `generate-interop-scan-exec` writes a real table (5-row parquet data file + a real position-delete deleting
  {1,3}=ids 20/40), commits append→rowDelta, emits Java's OWN `IcebergGenerics` read ({10,30,50}). Rust
  `interop_scan_exec.rs` loads it, `scan().to_arrow()`, asserts rows == Java's read (20/40 absent). NO Rust
  production change. Reviewer mutation-pinned 2 ways (poison expected; pre-delete snapshot). Gate green (offline
  no-op + run.sh round-trip). **This is the A5 scan-execution proof + the read-side write-action interop.**
- [ ] **Increment 2: equality deletes** + multi-data-file + schema/type variety.
- [x] **Increment 2: Direction-2 — Rust WRITES a real table → Java reads it** (the write-action ✅ flip) —
  env-gated GEN path (`ICEBERG_INTEROP_SCAN_GEN_DIR`) writes a real table via the PUBLIC write path
  (MemoryCatalog+LocalFsStorageFactory, real parquet data + a real PositionDeleteFileWriter delete, fast_append
  → row_delta, `TableMetadata::write_to` → final.metadata.json); the oracle's `verify-interop-scan-exec` reads
  it via `IcebergGenerics` + verifies {10,30,50}. **Java reads Rust's output byte-for-byte, NO massaging, NO
  Rust production change, no new deps (Cargo 0-diff).** Reviewer mutated the WRITTEN artifacts (rm delete
  parquet → Java NotFoundException; truncate avro manifest → Java RuntimeIOException). Gate green (offline no-op
  + D2 round-trip). Fixed an abbreviation typos slip in the Increment-1 lesson (+ permanently re-internalized
  the chain-the-gate-to-the-commit rule).
- [x] **Increment 3: equality deletes, BOTH directions** — D1 (Java writes eq-delete → Rust reads) + D2 (Rust
  writes eq-delete via `EqualityDeleteFileWriter` → Java reads), `equality_ids=[1]`, delete id ∈ {20,40},
  sequence-ordered (data seq 1 < eq-delete seq 2). Both = {10,30,50}. NO Rust production change, Cargo 0-diff.
  Reviewer decoded the avro manifests with the production reader + mutated both ways. **Run-script hardening:**
  the D2 scripts now grep the Java verify for `: 0 failures` (mvn exec:java swallows System.exit) — applied to
  BOTH the eq-delete-d2 and the Increment-2 scan-exec-d2 scripts.
- [x] **Increment 4: partitioned tables, BOTH directions** — `identity(category)` table (spec id 0), one real
  parquet data file PER PARTITION (cat=a: 10/20/30, cat=b: 40/50) + a PARTITION-SCOPED position-delete in
  partition a (position 1 = id=20), committed at seq 2 (data first at seq 1). Live set {10,30,40,50}: only id=20
  deleted, cat=b untouched (the partition-scoping correctness point — the cat=a delete must NOT reach cat=b).
  **D1:** Java writes (`GenericAppenderFactory` + `PartitionData`) → Rust `scan().to_arrow()` applies it
  partition-aware (`delete_file_index` keys by partition + spec id). **D2:** Rust WRITES via the production path
  (`DataFileWriter` built with a `PartitionKey` that auto-stamps the partition Struct + spec id AND routes the
  parquet under the partition path; `PositionDeleteFileWriter` with the cat=a `PartitionKey` row_delta'd at seq
  2) → Java reads. Both = {10,30,40,50}. NO Rust production change, Cargo 0-diff. Reviewer mutation-pinned both
  directions (poison expected; rm/truncate the Rust-written partition artifacts → Java NotFound/RuntimeIO;
  confirm cat=a delete does NOT drop cat=b). Gate green (offline no-op `interop_scan_exec` 6 passed + both
  round-trips exit 0). **Capstone COMPLETE** (position + equality deletes, both directions, unpartitioned +
  partitioned). PR next. Deferred within partitioning: multi-file-per-partition + non-identity transforms
  (bucket/truncate); more column types.

Deferred elsewhere: `readable_metrics` interop; ORC/Avro data; V3 types (Phase 4); BatchScan; CDC-merge.

---

<!-- Archived by todo-archival pass 2, 2026-06-11 (verbatim moves; see skills/compaction.md §Todo Archival). -->

## DONE (2026-06-10 overnight): readable_metrics inspection interop — Increment 3 of OVERNIGHT_BRIEF

The LAST inspection-table surface without a Java interop round-trip. Extended the existing manifest-reading
harness (A1/A2 pattern, run.sh-driven, Direction-1, env-gated) with a dedicated `table_rm`. Builder→reviewer
actor-critic; orchestrator independently re-ran BOTH the offline gate AND the round-trip + committed.

- [x] `dev/java-interop/.../InteropOracle.java` — new `InspectionReadableMetricsRmOracle`: unpartitioned V2
      `{id long, name string, score double}` table at `<dir>/table_rm` with rich per-column metrics (distinct
      column_sizes; null counts non-zero on name; nan counts non-zero only on the double; distinct typed
      lower/upper bounds via `Conversions.toByteBuffer(<column type>, value)`); materializes Java's REAL
      FilesTable readable_metrics struct → `java_rm_files.json` keyed by leaf-column NAME → 6 metric names.
      A1/A2/A4 emitters untouched (byte-identical).
- [x] `crates/iceberg/tests/interop_inspection_manifests.rs` — new env-gated test comparing Rust
      `inspect().files()` readable_metrics to Java BY NAME (order-independent; counts distinguish absent/None
      from 0; long+string+double typed bounds, double via `f64::to_bits`; leaf-column SET pinned).
- [x] `run-inspection-manifests.sh` — header note; same single round-trip (table_rm generated alongside).

**Outcome:** Cargo/pom FROZEN (no new deps). Independent gate green — offline interop suite **11 passed**
(new test no-ops when `ICEBERG_INTEROP_MANIFEST_DIR` unset), `cargo test -p iceberg --lib` 1642 passed,
clippy/fmt clean. **Round-trip re-run by orchestrator: 11 passed — readable_metrics matched Java field-for-field
(3 leaf columns, counts + typed bounds).** Reviewer poison-fixture-pinned every axis (bound/count/drop-column/
absent↔zero/string-bound/double-bits → matching assertion fails); no production bug. By-NAME comparison
side-steps the documented interior field-id JVM-HashMap-order divergence. GAP_MATRIX inspection row updated;
inspection interop now COMPLETE (set + columns + scan A4/A5). DEFERRED: promoted-type bound (needs schema
evolution); byte-level interior field-id parity (the HashMap-order residual). Row stays 🟡.

