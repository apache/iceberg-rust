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

# Lessons archive — 2026-06 Phase-2 completion (pass 2, archived 2026-06-11)

Entries archived by the second compaction pass (size trigger; agentic-pace amendment applied).
Covers the Phase-2 write-engine completion arc (PR #20), the E-sprint metadata interop, the
2026-06-08/09 scan + metrics + conflict-validation increments, and the matrix repair. Verbatim
moves; promoted entries carry a `_promoted_` stub naming the rule's new home. Not session-start
reading — grep on demand (see [../../skills/compaction.md](../../skills/compaction.md)).

### 2026-06-07 (Phase 2 Increment 4 — RewriteFiles, REVIEWER Opus)
- **DECISION (dataSequenceNumber deferral): ADD A GUARD, don't just document.** A data-file rewrite stamps
  the added files with a FRESH (higher) data sequence number. Merge-on-read deletes apply only to data with
  `data_seq <= delete_seq`, so compacting a deleted-from data file into a higher-seq file makes the old
  delete stop applying → deleted rows RESURRECT (silent data corruption). Java carries the replaced files'
  max data-seq onto the added files (`setNewDataFilesDataSequenceNumber`); this action defers that.
  Adjudication: (a) **today** this library cannot itself WRITE delete files — no `RowDelta`, no
  position/equality-delete commit path, and every add path runs `validate_added_data_files` which rejects
  non-`Data` content ("Only data content type is allowed for fast append") — so a table THIS library wrote
  has no outstanding deletes. BUT it can READ + operate on a Java-written table that DOES (a Java `RowDelta`
  snapshot has `Deletes`-content manifests), and `rewrite_files` on such a table would corrupt it. (b) The
  original deferral note framed the fresh seq as merely "correct for a pure data rewrite with no outstanding
  deletes" — true but not a *guard*; a note is not a regression barrier. (c) FIXED: added a HARD precondition
  in `commit()` — `has_outstanding_delete_files(table)` loads the current snapshot's manifest list and rejects
  (`ErrorKind::FeatureUnsupported`) if ANY entry is `ManifestContentType::Deletes`. This makes the unsafe case
  impossible (fail-loud) instead of documented. Test
  `test_rewrite_rejected_when_table_has_outstanding_delete_files` builds a table with a real position-delete
  manifest (via the production manifest/list writers + catalog `update_table`, since no public action writes
  deletes) and asserts rejection + table-unchanged. **Mutation-verified the guard AND the corruption: with the
  guard disabled the rewrite COMMITS (`expect_err` panics on an `Ok`) — proving the resurrection path is real,
  not theoretical.** Guard is small + in scope (the action's own file) and lifts cleanly when
  `dataSequenceNumber` preservation lands (docs say so in three places: module, action struct, `Transaction`
  ctor).
- **DO detect "outstanding merge-on-read deletes" by scanning the current snapshot's manifest list for a
  `ManifestContentType::Deletes` entry — not by reading the `total-delete-files` summary property.** *Why:*
  the manifest-list content type is the on-disk ground truth (a Java-written delete manifest always carries
  `content == Deletes`); a summary property can be absent, stale, or omitted by a non-Java writer. Loading the
  manifest list is one `load_manifest_list` call and is exactly what the producer already does.
- **DO confirm a producer-level guard does NOT catch the add-only case before trusting the action's
  precondition.** *Why:* the producer's `manifest_file()` rejects only a TRULY-empty commit (`added.is_empty()
  && deletes.is_empty() && props.is_empty()`). An add-only rewrite has added files, so the producer passes it —
  ONLY the action's `deleted_data_files.is_empty()` precondition rejects it. Mutation-verified: disabling the
  action precondition makes `test_rewrite_add_without_delete_rejected` COMMIT successfully (the `expect_err`
  panics on a returned `Table`), proving the action precondition — not the producer — is the load-bearing guard
  for add-only. The brief's worry (producer only rejects all-empty) is exactly right.
- **FLAG (tracked 🟡, not fixed): Java's REPLACE record-count invariant is unmirrored.** Java
  `SnapshotProducer` (the `BaseSnapshot`-construction path, lines 347-359) rejects a REPLACE whose summary has
  `added-records > deleted-records` ("Invalid REPLACE operation: %s added records > %s replaced records") — a
  compaction must not increase the live row count. Rust's producer commit path has NO such check. NOT added
  here: the check belongs in the shared `snapshot.rs` producer (Java puts it in `SnapshotProducer`, shared
  across ops), which is outside this increment's named file set, and it is a logical-consistency guard, not the
  data-loss trap. The point-2 claim "`SnapshotProducer.summary` is operation-agnostic" is correct for the
  TOTALS method (`summary(previous)`, all `updateTotal`, no per-op branch) — but the SIBLING REPLACE invariant
  lives in the snapshot-construction path, not `summary()`, so admitting `Replace` to the
  `update_snapshot_summaries` allowlist is right AND this separate guard is a distinct missing item.

### 2026-06-08 (Phase 2 Increment 5b — RowDelta action + producer delete-manifest support, BUILDER Opus)
- **DO add producer delete-manifest support as an ADDITIVE second add-path, parallel to
  `write_added_manifest`, not by overloading the data path.** *Why:* `SnapshotProducer::manifest_file()`
  already wrote a DATA manifest from `added_data_files`; merge-on-read needs a SECOND manifest with
  `ManifestContentType::Deletes` from a NEW `added_delete_files: Vec<DataFile>` field. The clean shape is
  `with_added_delete_files()` (a builder setter, so existing `SnapshotProducer::new` callers are
  untouched) + `write_added_delete_manifest()` (a byte-for-byte mirror of `write_added_manifest` that calls
  `new_manifest_writer(ManifestContentType::Deletes)` — the producer ALREADY had the `Deletes` arm wired to
  `build_v2_deletes`/`build_v3_deletes`) + a `manifest_file()` push when non-empty. The producer was built
  generic enough that data-only, delete-only, or both fall out of the same `commit()` with no operation
  branching. Keep the V1 arm (`builder.snapshot_id(id).build()`) for symmetry even though V1 has no delete
  manifests — it never fires (validation rejects delete content before a V1 table reaches it).
- **DO relax the empty-commit precondition to count `added_delete_files`, and verify the seq-inheritance
  path is the SAME as added data files.** *Why:* the producer's `manifest_file()` guard rejected a commit
  with no added DATA files + no removed files + no props — which would wrongly reject an add-deletes-only
  `RowDelta` (the crown-jewel case). Add `&& self.added_delete_files.is_empty()` to the guard. The added
  delete entries are `Added` with NO sequence number for V2/V3 (`builder.build()`, not
  `.snapshot_id(id).build()`), so the manifest-list reader inherits the new snapshot's seq at read time —
  IDENTICAL to added data files. This is load-bearing: a delete file's seq MUST exceed the target data's
  seq for the read side (`delete_file_index.rs`: pos-delete applies iff `delete_seq >= data_seq`) to apply
  it. Mutation-verified BOTH ways: a seq-0 stamp fails the dedicated seq test AND the crown jewel (the
  seq-0 delete no longer applies to seq-1 data → the deleted rows RESURRECT in the scan).
- **DO mirror Java `BaseRowDelta.operation()` DYNAMICALLY (Append / Delete / Overwrite), classified on
  the REQUESTED add sets, not statically `Overwrite`.** *Why:* the brief said "operation is OVERWRITE" but
  `core/BaseRowDelta.operation()` returns APPEND (adds data, no delete files, no data deletes), DELETE
  (adds delete files, no data files), else OVERWRITE — so the crown-jewel add-deletes-only case is DELETE,
  not Overwrite. The OverwriteFiles reviewer already established "align to the Java source, not the brief's
  hint" for the dynamic-operation question. `update_snapshot_summaries` already admits Append/Delete/
  Overwrite, so NO summary-allowlist edit was needed (unlike RewriteFiles, which had to add `Replace`).
- **DO route added delete files through `SnapshotSummaryCollector::add_file` and DON'T touch
  `snapshot_summary.rs` — its `add_file` already branches on content type.** *Why:* the brief flagged
  `snapshot_summary.rs` as edit-only-if-needed. The collector's `add_file`/`remove_file` ALREADY handle
  `PositionDeletes`/`EqualityDeletes` (incrementing `added_delete_files` + `added_pos_delete_files` +
  `added_pos_deletes` / the equality siblings) — so wiring a `for delete_file in &self.added_delete_files
  { summary_collector.add_file(...) }` loop in the producer's `summary()` is the whole change; the summary
  emits `added-delete-files`/`added-position-delete-files`/`added-position-deletes` for free. Verified by a
  test asserting those exact properties. Flagged: `snapshot_summary.rs` NOT touched.
- **DO build the crown-jewel end-to-end test on a REAL-FS `MemoryCatalog`
  (`MemoryCatalogBuilder::default().with_storage_factory(Arc::new(LocalFsStorageFactory))` over a tempdir
  warehouse) so the scan's FileIO reads the data + delete parquet files you wrote.** *Why:* the default
  `MemoryCatalog` storage is in-memory; a position-delete file written to the local FS would be invisible
  to the scan. Write the data file via the bare `ParquetWriterBuilder` + the table's `FileIO` under
  `{location}/data/`, finish its `DataFileBuilder` with `content(Data)` + partition; write the delete file
  via the 5a `PositionDeleteFileWriter` (with a `PartitionKey` so the delete file's partition MATCHES the
  data file's — the `delete_file_index` keys pos-deletes by `(partition, spec_id)` AND requires
  `delete_seq >= data_seq`); the delete parquet's `file_path` column rows MUST be the exact data-file path
  (the loader keys the delete vector by that path). Then `scan().select(["y"]).to_arrow()` and assert the
  surviving y-values. The crown jewel is the ONLY test that proves the write path produces delete files the
  read side actually honors — a manifest-shape test alone cannot (it never reads a row).
- **DO bring `FileWriterBuilder` (not just `FileWriter`) into scope to call `ParquetWriterBuilder::build`.**
  *Why:* `build()` is on the `FileWriterBuilder` trait; `write()`/`close()` are on `FileWriter`. Importing
  only `FileWriter` gives `no method named build` + three `type annotations needed` errors (the build call's
  type can't be inferred without the trait). Import both.
- **DO assert a row-delta's added-manifest shape by COLLECTING live file paths keyed by manifest content
  type, not by `manifest_file.has_added_files()`.** *Why:* `existing_manifest` carries EVERY prior manifest
  forward (a row delta only adds), and the fast-appended data manifest also has `has_added_files() == true`
  — so counting `Data` manifests with added files gives 2, not 1. Assert instead that the new data file's
  path appears in a DATA manifest and the delete file's path appears in the (exactly one) DELETE manifest;
  that is the real signal and is robust to carried-forward manifests.

### 2026-06-08 — RowDelta (increment 5b) REVIEW: seq-inheritance + forward-application verification
- **The position-delete forward-application negative is protected by TWO independent mechanisms, not
  just the seq guard — know this before claiming a test "isolates" the sequence number.** A position
  delete added at seq N must NOT apply to data added LATER (seq > N; spec line 1071: applies only when
  `data_seq <= delete_seq`). Two things enforce it: (1) the `delete_file_index` seq guard
  (`delete.sequence_number() >= Some(seq_num)` for pos-deletes, line ~204) decides which delete files
  are *candidates* for a data file; (2) `arrow/delete_filter.rs` keys the loaded delete VECTOR by the
  data-file path read from the delete file's `file_path` column (`upsert_delete_vector(data_file_path,
  ...)`), so a delete naming D1's path produces a vector under D1's path and the scan of D2 looks up
  D2's path → empty. Mutation-verified: forcibly removing the seq guard (mechanism 1) did NOT fail the
  end-to-end forward test, because mechanism 2 still spares D2. The e2e test
  (`test_row_delta_position_delete_does_not_apply_to_later_data`) is a valid behavioral pin (D2 stays
  intact through the real scan even when it shares D1's partition AND has rows at the deleted positions)
  but is NOT a clean isolation of the seq guard — the index-level seq semantics are unit-pinned
  separately in `delete_file_index.rs` (`test_delete_file_index_partitioned`/`unpartitioned`).
- **The seq-0 inheritance mutation is the decisive corruption probe — it fails BOTH the crown jewel and
  the forward test.** Forcing `builder.sequence_number(0)` on the added delete entry (instead of leaving
  it unassigned for V2/V3 inheritance) makes `data_seq(1) <= delete_seq(0)` FALSE → the position delete
  never applies → the deleted rows RESURRECT (scan returns all 5). This proves the inheritance is
  genuinely load-bearing: the delete entry MUST be written `Added` with no explicit seq so the
  manifest-list writer stamps `next_seq_num` (= the new snapshot's seq) via
  `assign_sequence_numbers` → `inherit_data` at read time — identical to added DATA files.
- **Java `BaseRowDelta.operation()` APPEND branch has `&& !deletesDataFiles()` that the Rust two-branch
  form omits — this is SOUND only because removeRows/removeDeletes are deferred.** Java:
  `addsDataFiles() && !addsDeleteFiles() && !deletesDataFiles() → APPEND`. The Rust RowDelta never
  removes files (its `delete_files`/`delete_entries` return empty), so `deletesDataFiles()` is always
  false and the simplified `adds_data && !adds_deletes → Append` is equivalent for this increment's
  surface. When removeRows/removeDeletes land, the third condition MUST be added or a data-add +
  data-remove row delta would wrongly record Append instead of Overwrite. Added
  `test_row_delta_add_data_only_records_append` (the previously-uncovered op branch) — mutation-verified.
- **The added-delete `validate_added_delete_files` requires the DEFAULT partition spec id, stricter than
  Java `add(DeleteFile)` (which only checks `spec(file.specId()) != null`, i.e. the spec EXISTS).** This
  matches the existing `validate_added_data_files` convention and the producer's single-default-spec
  manifest writer — a consistent, pre-existing producer limitation (non-default-spec writes are a broader
  producer change), NOT a RowDelta regression. Acceptable for this increment; flag if Java-faithful
  multi-spec delete commits are needed later.

### 2026-06-08 (Phase 3 Increment 3 — ScanReport / MetricsReporter data model, BUILDER Opus)
- **DO model Java's `MetricsReport` marker interface as a closed `#[non_exhaustive] enum`, not a `dyn` trait
  object.** *Why:* Java's `MetricsReport` is an empty marker and reporters downcast (`instanceof ScanReport`);
  `InMemoryMetricsReporter.scanReport()` even THROWS on a kind mismatch. A Rust `enum MetricsReport {
  Scan(ScanReport) }` makes the kind part of the type — `last_scan_report()` matches exhaustively (no wildcard)
  so a future `Commit(CommitReport)` variant forces every consumer to update, and there is no downcast to get
  wrong. `#[non_exhaustive]` keeps adding a variant non-breaking. Reporters take `MetricsReport` by value (the
  trait is `Send + Sync` so reporters can be shared).
- **DO carry Java's `@Nullable` metric optionality as `Option<_>` per field with `skip_serializing_if =
  "Option::is_none"` + `default`, NOT a zero default.** *Why:* Java's `ScanMetricsResult` accessors are all
  `@Nullable` and `ScanMetricsResultParser` OMITS a null counter/timer from the JSON (and `CounterResult.
  fromCounter` returns null for a no-op counter). A never-incremented counter must be ABSENT, not `{"value":0}`
  — both for byte-parity with the REST `report-metrics` payload and to distinguish "not measured" from
  "measured zero". `..Default::default()` on the struct + `Default` on the field gives the absent state for free.
- **DO hand-write `Serialize`/`Deserialize` for the timer so `total-duration` is expressed in the timer's OWN
  unit, not nanoseconds.** *Why:* Java `TimerResultParser` writes `total-duration` as `unit.convert(duration.
  toNanos(), NANOSECONDS)` (a TRUNCATING integer convert into the reported `time-unit`) and reads it back with
  `Duration.of(val, chronoUnit(unit))`. A naive `#[derive(Serialize)]` over a `Duration` would emit a
  `{secs,nanos}` struct (wrong shape) — and emitting raw nanos under a `milliseconds` time-unit is wrong by 10^6.
  The `Duration` is kept exact in memory; convert ONLY at the serde boundary via a `nanos_per_unit()` table.
  Pin it with a non-nanosecond unit (250ms → `total-duration: 250`, not 250_000_000).
- **DO pin the JSON shape against a HAND-WRITTEN expected `serde_json::json!` for the top-level field names AND
  a couple of metric/counter/timer names, not just a serialize→deserialize round-trip.** *Why:* a round-trip
  (`to_value` → `from_value` → `==`) is TAUTOLOGICAL on field names — rename `table-name`→`tableName` on the
  struct and the round-trip still passes (both directions use the same rename). Only an explicit
  `json.get("table-name").is_some()` / `json["metrics"]["result-data-files"] == {"unit":"count","value":5}`
  assertion catches a drifted wire name. Mutation-verified: the rename mutation fails ONLY the shape assertion,
  not the round-trip.
- **DO scope the `filter` serde to the Rust `Predicate`'s own derive and DOCUMENT the divergence from Java's
  `ExpressionParser` JSON — do not silently imply parity.** *Why:* Java's `ScanReportParser` emits `filter` via
  `ExpressionParser.toJson` (a structured expression-tree JSON); the Rust `Predicate` serde is a different
  shape. Porting `ExpressionParser` is a large separate effort; the metric data is the high-value part of the
  contract. State the gap in the module docs + GAP_MATRIX so a future REST-interop increment knows the `filter`
  sub-document is the one unfaithful field.
- **CONSTRAINT HIT: the `iceberg` crate had NO logging facade (`tracing`/`log` are not direct deps), but the
  brief mandated `tracing`-based logging.** Added `tracing = { workspace = true }` to `crates/iceberg/Cargo.toml`
  (already a resolved workspace dep, so the `Cargo.lock` delta is a single `tracing` line under the iceberg
  crate's dep list). This edits a dependency file — an Absolute Prohibition / scope-flag — so it is surfaced for
  orchestrator sign-off rather than done silently. *Lesson for the next agent:* the core crate logs NOWHERE
  today; any first `tracing` use forces this same one-line dep add. If sign-off is withheld, the fallback is a
  caller-injected log sink (over-engineered for one reporter) — the workspace-dep add is the clean path.

### 2026-06-09 (DeleteFiles validateFilesExist — status axis on the validation walk, BUILDER Opus)
- **DO add a STATUS AXIS to the shared `files_after` walk (was `added_files_after`) — parameterize
  `status_to_keep: ManifestStatus` — rather than fork a second near-identical walk.** *Why:* Java's
  `validationHistory` is one walk; the per-check `ManifestGroup` entry filter differs only in the status it
  keeps (`Added` for the conflict checks via `ignoreDeleted().ignoreExisting()`; `Deleted` for
  `validateDataFilesExist`/`deletedDataFiles` via `entry.status() == DELETED` + `ignoreExisting()`). The two
  existing callers (`added_data_files_after`, `added_delete_files_after`) pass `ManifestStatus::Added` and are
  BEHAVIOR-PRESERVING — proven by the ReplacePartitions/RowDelta/OverwriteFiles conflict tests staying green.
  The new `deleted_data_files_after` passes `ManifestStatus::Deleted`. One walk, one axis, no drift.
- **DO confirm the Deleted-tombstone SOURCING before trusting the `added_snapshot_id == snapshot_id` manifest
  filter for a concurrent deletion.** *Why:* the load-bearing question is "does a concurrently-deleted file's
  `Deleted` tombstone live in a manifest the concurrent snapshot ITSELF wrote?" It does:
  `rewrite_manifest_with_deletes` (snapshot.rs) writes the removed entry via `add_delete_entry` into a NEW
  manifest whose `added_snapshot_id` is the committing snapshot id. So the SAME `added_snapshot_id ==
  snapshot.snapshot_id()` filter that finds a snapshot's `Added` entries also finds its `Deleted` tombstones —
  exactly Java's `manifest.snapshotId() == currentSnapshot.snapshotId()`. Mutation-verified end-to-end (the
  headline test fails the instant the status axis keeps `Added`), so the sourcing claim is pinned, not assumed.
- **DO model `VALIDATE_DATA_FILES_EXIST_OPERATIONS = {OVERWRITE, REPLACE, DELETE}` as `{Overwrite, Delete}` in
  Rust and SAY WHY.** *Why:* the Rust `Operation` enum has no `Replace` variant (a `ReplacePartitions` commit
  records `Operation::Overwrite`; a rewrite is not yet a distinct op). Dropping the unrepresentable `REPLACE` is
  faithful — Rust never records a `REPLACE` snapshot to miss — NOT a gap. Document it on the predicate so a
  future agent adding a `Replace` op extends the set.
- **DO recognize the flag-OFF control yields a DIFFERENT error than the validation path, and assert on the
  DISTINCTION.** *Why:* with `validate_files_exist()` OFF, the re-based delete still cannot resolve the
  concurrently-vanished file, so `resolve_delete_paths` fails with the GENERIC "Missing required files to delete"
  — NOT the validateDataFilesExist "Cannot commit, missing data files". Asserting the OFF test gets the generic
  message AND does NOT get the validation message proves the validation path is the genuinely OPT-IN one (a
  weaker `is_err()` assertion would pass even if validation always ran). The two messages must stay textually
  distinct for this to hold.
- **DO note the `StreamingDelete.validate()` wiring nuance honestly.** *Why:* Java `StreamingDelete.validate()`
  calls `failMissingDeletePaths()` (the filter-manager required-deletes mechanism), NOT `validateDataFilesExist`
  directly — `validateDataFilesExist` is the method the brief targets for the Rust delete path
  (requiredDataFiles = the files being deleted), modeled on RowDelta/ReplacePartitions' `validate` seam. The
  Rust port is faithful to `validateDataFilesExist`'s CONTRACT; flag the wiring difference rather than imply
  `StreamingDelete` calls it.

### 2026-06-09 (Phase 2 — DeleteFiles `validateFilesExist`, REVIEWER Opus)
- **DO add the no-override tx-captured-start test for EVERY new conflict-validation action — it is the SAME
  gap the Increment-6 reviewer already caught for OverwriteFiles/RowDelta/ReplacePartitions, and it recurs.**
  *Why:* the DeleteFiles builder's 5 files-exist tests ALL passed `.validate_from_snapshot(...)`, which
  short-circuits `effective_start = validate_from_snapshot.or(starting_snapshot_id)` and NEVER reads the
  tx-captured `starting_snapshot_id` field. Mutation-verified the gap was REAL: rewriting the fallback to read
  the REFRESHED head (`current.metadata().current_snapshot_id()`) instead of the tx-captured start passed ALL
  16 delete_files tests — the brief's #1 danger (start re-read at validation time ⇒ start == current head ⇒
  empty concurrent window ⇒ the files-exist check silently ALWAYS PASSES) was completely unpinned. Added
  `test_delete_files_exist_rejects_concurrent_using_tx_captured_starting_snapshot` (validate enabled, NO
  override, concurrent same-file delete): the refreshed-head mutation now fails EXACTLY this one test (16
  passed / 1 failed), proving it uniquely pins the `Transaction::new` capture surviving `do_commit`'s re-base.
  When porting a validation that has BOTH an explicit `validate_from_snapshot` override AND an implicit
  tx-captured default, a test that always sets the override cannot pin the default source — write one that omits it.
- **DO mutation-test the content-type axis AND the intersection direction separately, not just the status/op
  axes.** *Why:* beyond the four mutations the builder ran, two more axes are independently load-bearing and
  were each pinned by a DISTINCT test: (1) `deleted_data_files_after` using `ManifestContentType::Deletes`
  instead of `Data` misses the data-file tombstone (tombstones live in DATA manifests) → headline fails;
  (2) the `validate` intersection ignoring `delete_paths` (matching ANY concurrently-deleted file) rejects a
  disjoint concurrent delete → the different-file negative control fails. The negative control is what makes
  `requiredDataFiles = delete_paths` load-bearing rather than "any concurrent deletion rejects." A mutation
  that survives every existing test = an unpinned axis; run the content-type and intersection mutations, not
  only the status/op/retryable ones.
- **CONFIRMED behavior-preserving (status-axis generalization): inverting the shared `added_snapshot_id ==
  snapshot_id` manifest filter fails 17 transaction tests across BOTH axes** — the DeleteFiles headline/override
  (Deleted-tombstone sourcing) AND the OverwriteFiles/RowDelta/ReplacePartitions conflict tests (Added-entry
  sourcing). One mutation failing both families is the proof the `files_after` generalization kept the
  manifest-sourcing semantics identical for the two `ManifestStatus::Added` callers while extending it to the
  `Deleted` caller — the status axis is the ONLY behavioral change, and it is parameterized, not hard-coded.

### 2026-06-09 (RowDelta validateDataFilesExist + the skip-deletes op-set variant, BUILDER Opus)
- **DO add the `skipDeletes` op-set axis as a `skip_deletes: bool` PARAM on `deleted_data_files_after`, not a
  sibling fn, and make the EXISTING DeleteFiles caller pass `false` explicitly.** *Why:* Java's two op sets
  differ by exactly one member — `VALIDATE_DATA_FILES_EXIST_OPERATIONS = {OVERWRITE, REPLACE, DELETE}` vs
  `VALIDATE_DATA_FILES_EXIST_SKIP_DELETE_OPERATIONS = {OVERWRITE, REPLACE}` (drops DELETE). A bool param selecting
  between `operation_removes_data_files` (`{Overwrite, Delete}`) and a new
  `operation_removes_data_files_skip_deletes` (`{Overwrite}`) keeps ONE walk and one call site per caller.
  Behavior-preservation of the DeleteFiles path is PROVEN by a mutation: forcing the DeleteFiles caller to
  `skip_deletes = true` fails 3 DeleteFiles files-exist tests (their concurrent deletion is a `Delete`-op
  `delete_files` snapshot, excluded by `{Overwrite}`) — so `false` is both load-bearing AND the value that keeps
  the existing tests green. `REPLACE` is unrepresentable in the Rust `Operation` enum in BOTH sets, so it is
  absent either way — faithful, not a gap.
- **DO get the skip-deletes DEFAULT right: RowDelta passes `skip_deletes = !validate_deleted_files`, and
  `validate_deleted_files` is `false` by default ⇒ `skipDeletes = true` ⇒ `{OVERWRITE}` BY DEFAULT.** *Why:* Java
  `BaseRowDelta.validate` (L146) calls `validateDataFilesExist(..., !validateDeletes, ...)` and `validateDeletes`
  starts `false` (set only by `validateDeletedFiles()`). The intuitive-but-WRONG default is to include DELETE-op
  snapshots (`skip_deletes = false`); that would reject a legitimate concurrent merge-on-read DELETE the default
  is meant to tolerate. The ONLY test that distinguishes the two op sets is a two-half test: a concurrent
  DELETE-op (`delete_files`) deletion of the referenced file COMMITS by default (excluded) and is REJECTED after
  `validate_deleted_files()` (included). A concurrent OVERWRITE-op deletion (`overwrite_files().add+delete`) is in
  BOTH sets, so use it for the headline (rejects WITHOUT needing `validate_deleted_files()`).
- **DO keep `referenced_data_files` CALLER-PROVIDED, mirroring Java's `CharSequenceSet referencedDataFiles`
  populated by `validateDataFilesExist(referencedFiles)` — do NOT derive it from the added delete files.** *Why:*
  Java `BaseRowDelta.referencedDataFiles` is a field the engine fills by passing the position deletes' referenced
  data-file paths into `validateDataFilesExist(Iterable<CharSequence>)`; the action never inspects the delete
  files to compute it. The Rust `validate_data_files_exist(impl IntoIterator<Item = impl Into<String>>)` takes the
  caller's set the same way; non-empty ENABLES the check (Java's `if (!referencedDataFiles.isEmpty())` guard).
  Deriving it would be a different (and unfaithful) contract — the position-delete `DataFile` in this Rust model
  does not even carry the referenced data-file path as a first-class field. The different-file negative control
  (concurrent deletion of a NON-referenced file → OK) is what makes the referenced-set intersection load-bearing
  rather than "any concurrent deletion rejects."
- **DO simulate the concurrent deletion that the skip-deletes-DEFAULT check must still see with an OVERWRITE,
  and the one it must IGNORE with a `delete_files` DELETE.** *Why:* `overwrite_files().add_file(g).delete_file(f)`
  records `Operation::Overwrite` (Java `BaseOverwriteFiles.operation()` when it both adds and deletes) and writes
  `f`'s `Deleted` tombstone on a DATA manifest the new snapshot owns — in BOTH op sets. `delete_files().delete_
  file(f)` records `Operation::Delete` — only in the non-skip set. Pairing them across the 6 tests exercises both
  op-set branches with REAL concurrent commits through the catalog (no hand-built tombstones).

### 2026-06-09 (Scan metrics EMISSION wiring — TableScan → MetricsReporter, BUILDER Opus)
- **DO make a lazy/concurrent stream the EMISSION point by counting per-task in `poll_next` and reporting
  ONCE on the `Ready(None)` exhaustion transition — the faithful analogue of Java
  `CloseableIterable.whenComplete(doPlanFiles(), closeHook)`.** *Why:* Java `SnapshotScan.planFiles` starts the
  timer, builds the plan, and on the iterable's CLOSE (after full consumption) builds the `ScanReport` and calls
  `metricsReporter().report(...)`. A custom `Stream` adapter (`MetricsReportingFileScanTaskStream`) that (a)
  calls `record_file_task` on each `Ready(Some(Ok(task)))` (mirroring the lazy `createFileScanTasks` transform /
  `ScanMetricsUtil.fileTask`) and (b) emits the report on the FIRST `Ready(None)` behind a `reported: bool` guard
  gives "per-task accounting + exactly-once on completion" deterministically. Do NOT emit in `Drop` (fires on
  early drop with partial counts) and do NOT emit per-task. Pin "exactly once" with a task-by-task drain
  asserting report count stays 0 mid-stream, ==1 after exhaustion, ==1 after re-polling the exhausted stream —
  mutation-verified by moving the emit into the `Some(Ok(task))` arm (the mid-stream==0 assertion fails after
  the 1st task).
- **DO enforce OPT-IN at the TYPE level: thread `Option<Arc<ScanMetricsCollector>>` and gate every increment on
  `Some` — when `None` there is no collector, no `Instant`, no stream wrapper, so the un-instrumented
  `plan_files` is byte-for-byte unchanged.** *Why:* the brief's paramount property. A task-set regression test
  (no reporter ⇒ same tasks) CANNOT catch a broken opt-in (counting does not change which tasks are planned), so
  add a STRUCTURAL test that asserts `scan.plan_context.metrics_collector.is_none()` with no reporter and
  `.is_some()` with one. Mutation-verified: forcing `metrics_collector: Some(...)` unconditionally in `build()`
  fails EXACTLY the structural test and nothing else. (`scan::tests` is a child module, so it reads the private
  `TableScan.plan_context` + `pub(crate)` `PlanContext.metrics_collector` directly — no production-visibility
  widening.)
- **DO read the Java COUNTER semantics per-metric from the source, not by name-intuition — `result-delete-files`
  is per-TASK delete REFERENCES, not distinct delete files/manifests.** *Why:* `ScanMetricsUtil.fileTask` runs
  once per produced `FileScanTask` and does `resultDeleteFiles().increment(deleteFiles.length)` — a delete file
  applying to N data files counts N times. `total-data-manifests` = the manifest-LIST entries by content
  (`DataTableScan.doPlanFiles` `dataManifests.size()`), NOT the scanned subset; `scanned`+`skipped` == `total`
  only WITH a filter (no filter ⇒ all scanned, skipped 0). The delete-manifest fixture test asserts
  `result_delete_files == Σ task.deletes.len()` (10 here: one position-delete in the shared partition attaches to
  all 10 data files) and `result_data_files == 10` (excludes the delete file) — mutation-verified by folding the
  delete count into `result_data_files`.
- **DO count `scanned`/`skipped` manifests at the partition-filter prune point in `context.rs`
  (`build_manifest_file_contexts_from_files`), per the manifest's `content`, and the manifest-LIST totals in
  `plan_files` right after `get_manifest_list` — two different Java sites (`ManifestGroup`'s
  `CloseableIterable.filter/count` vs `DataTableScan.doPlanFiles`).** *Why:* a pruned manifest
  (`manifest_evaluator.eval(...) == false` → `continue`) is SKIPPED; a survivor is SCANNED. The prune point is
  the only place the data-vs-delete content + the prune decision are both in hand. Mutation-verified: swapping
  the skipped increment to scanned fails the prune test (`skipped >= 1` + `scanned+skipped==total`).
- **DO capture the `ScanReport` identity (table name / snapshot id / schema id / projected ids+names / filter)
  ONCE at `build()` time, before the scan's `filter`/`field_ids`/`schema` are moved into `PlanContext`.** *Why:*
  Java reads exactly these to build `ImmutableScanReport`. In Rust the filter is consumed by
  `predicate: self.filter.map(Arc::new)` and `field_ids` by `Arc::new(field_ids)`, so the report inputs must be
  cloned into a `ScanMetricsContext` BEFORE those moves. Projected field NAMES come from
  `Schema::name_by_field_id(id)` per projected id (Java `schema().findColumnName(id)`), NOT the raw
  `column_names` (a metadata column has no schema name). The report `filter` defaults to `Predicate::AlwaysTrue`
  when the scan has no filter (Java `BaseScan.filter()`).
- **DO leave a metric `None` (not `Some(0)`) when the planner cannot collect it cleanly, and DOCUMENT which +
  why.** *Why:* Java's `@Nullable` "never incremented ⇒ absent" shape. The Rust planner cleanly collects 8
  manifest/file counters + 2 byte sizes + the timer; `skipped_data_files`/`skipped_delete_files`/
  `indexed_delete_files`/`equality_delete_files`/`positional_delete_files`/`dvs` count delete-index internals +
  per-file metrics pruning the planner doesn't expose at a single accumulation point — left `None`, documented
  in the collector module. A fabricated 0 would diverge from Java's optionality AND imply fidelity the planner
  lacks.
- **DO use `Ordering::Relaxed` for the `AtomicI64` scan counters shared across the spawned manifest/entry
  tasks.** *Why:* the increments are commutative + order-independent and no counter value gates another thread's
  control flow; the happens-before barrier that matters is the stream draining to completion (the wrapper's
  `Ready(None)`) before `snapshot()` reads them, which the await chain provides. `SeqCst` would be unjustified
  overhead. (`dyn MetricsReporter` is not `Debug`, so `ScanMetricsContext` needs a MANUAL `Debug` eliding the
  reporter — `TableScan` derives `Debug`; do NOT add `Debug` to the `MetricsReporter` trait, that would touch
  the out-of-scope `metrics/mod.rs` and change a public trait.)

### 2026-06-09 (Scan metrics EMISSION wiring — REVIEWER Opus)
- **DO add a test that pins "no report on PARTIAL-consume-then-drop," separately from the "exactly once on
  full consumption" test — the two are NOT redundant.** *Why:* the exactly-once test fully drains the stream,
  so a `Drop`-based emit fires AFTER the `Ready(None)` emit and the `reported` guard makes it a silent no-op —
  the exactly-once test stays GREEN under a `Drop`-emit mutation. The builder correctly *chose* emit-on-
  `Ready(None)` (not on `Drop`) to avoid partial-count reports, and documented it, but left the property
  UNPINNED. Added `test_partial_consume_then_drop_emits_no_report` (pull 3 of 10 tasks, `drop(stream)`, assert
  `last_report().is_none()`); mutation-verified by adding a `Drop` impl that emits — the new test FAILS, the
  exactly-once test PASSES, proving the new test is the only guard for the early-drop contract. The 60s test
  timeout also confirms `drop`-before-exhaust does NOT deadlock the spawned producers (the dropped mpsc
  receiver lets the senders error out, same as the un-instrumented path).
- **DO verify the default (no-reporter) `plan_files` return is the LITERAL pre-change expression, not just
  "tests pass."** *Why:* the byte-unchanged guarantee is the #1 regression risk. Confirmed `git show
  HEAD:scan/mod.rs` ended `plan_files` with `Ok(file_scan_task_rx.boxed())`, and the None-metrics match arm
  (`_ => Ok(file_scan_task_rx.boxed())`) returns exactly that — same boxed stream, same producers, same
  channel, same order; `planning_started_at`/the manifest-list fold/the wrapper are all gated on
  `self.metrics.is_some()`. The `field_ids` hoist (`Arc::new` moved one statement earlier) is behavior-
  identical (same value, just named before two consumers use it). Mutation-verified the opt-in with a
  structural test (collector `Some` only with a reporter).
- **DO confirm the report build never `.unwrap()`s a poisoned lock.** *Why:* brief concern #4. The collector
  is atomics-only (no `Mutex`); `emit_report` clones captured fields + reads atomics; the only lock is inside
  `InMemoryMetricsReporter::report`, which uses `unwrap_or_else(|p| p.into_inner())` (poison-safe) and lives in
  the out-of-scope `metrics/mod.rs` (unchanged). No bare `.unwrap()`/`.expect()`/`println!` in any production
  region of the three scan files.

### 2026-06-09 (Inspection-table interop — `snapshots` + `refs` — ORCHESTRATOR + REVIEWER Opus)
- **DO verify a suspected parity divergence against the LIVE Java source BEFORE instructing a "fix".** *Why:*
  Java `SnapshotsTable.snapshotToRow` passes `snap.summary()` (the WHOLE map) into the summary column, which
  *looked* like Rust diverges (Rust emits only `additional_properties`, dropping `operation`). Reading
  `/tmp/iceberg-java-ref` `SnapshotParser.fromJson` (1.10.0, ~L153–156) showed the on-disk round-trip SPLITS
  `operation` OUT of the summary map — so a re-parsed snapshot's `summary()` is `additional_properties`-only,
  exactly matching Rust. The "divergence" was an artifact of the in-memory-CONSTRUCTED snapshot (our oracle's
  `snapshot()` helper puts `operation` in the map), NOT the canonical path. Had I trusted the first read and
  "fixed" Rust to inject `operation`, I'd have BROKEN parity. Captured in memory `reference_java_snapshot_summary_operation.md`.
- **DO materialize an inspection-table interop oracle from a `TableMetadataParser.fromJson`-RE-PARSED base —
  the same bytes the Rust reader consumes — not the freshly-built in-memory `TableMetadata`.** *Why:* the two
  diverge on `summary` (operation-in-map vs split-out) AND the re-parse is what gives a non-null
  `metadataFileLocation()` that `SnapshotsTable.task`/`RefsTable.task` hand to `io().newInputFile(...)`. Build
  the base, write it, re-parse from disk, THEN scan.
- **GOTCHA: Java `StaticDataTask.rows()` is a LAZY `Iterables.transform` over a SINGLE mutable
  `StructProjection` that re-`wrap`s each row.** Accumulating the `StructLike` references into a `List` and
  reading them AFTER the loop yields the LAST row N times (the builder's first run emitted three identical
  snapshot rows). *Fix:* consume each row EAGERLY inside the iteration (serialize to JSON per-row while the
  projection still points at it). Any metadata-table row reader must not stash `StructLike`s for later.
- **DO drive inspection-table interop as Direction-1-ONLY.** Metadata tables are READ-ONLY virtual projections
  of `TableMetadata`; there is nothing of Rust's for Java to read back (no Direction 2). The equality of the
  projected rows IS the round-trip proof. Compare ALL columns ORDER-INDEPENDENTLY (sort both sides; compare a
  map column as a `HashMap`, not by key order) so JVM/serde map-ordering never makes the test flaky.
- **Cheap fixture richness pays off:** one snapshot with a MULTI-KEY summary + one with an operation-only
  (→ empty-map) summary, and refs covering branch-full-retention / tag-only-max-ref-age / branch-no-retention,
  exercise every non-trivial projection (map column, retention NULL-per-kind) in a 3-row/3-ref fixture.

### 2026-06-09 (Inspection-table interop — `history` + `metadata_log_entries` — ORCHESTRATOR + REVIEWER Opus)
- **A FORKED snapshot-log (needed for `is_current_ancestor=false`) must be built across SEPARATE commits,
  re-parsing between each.** *Why:* Java `TableMetadata.Builder.intermediateSnapshotIdSet` (and the Rust
  `update_snapshot_log` mirror) prunes from the snapshot-LOG any snapshot that, WITHIN ONE build's
  `changes`, is both AddSnapshot'd AND set-as-main AND no-longer-current. Doing
  `addSnapshot(A)+setMain(A)+addSnapshot(B)+setMain(B)` in one build drops A as "intermediate" → log=[B]
  only. Re-parsing (`fromJson(toJson(..))`) clears `changes`, so a prior snapshot counts as
  already-persisted and survives the next commit's pruning. Recipe: B0 addSnapshot(ROOT)+setMain(ROOT) →
  reparse → B1 add(SIBLING)+setMain(SIBLING) → reparse → B2 add(CURRENT)+setMain(CURRENT) → log
  [ROOT,SIBLING,CURRENT]; with CURRENT.parent=ROOT, SIBLING is off the current ancestry.
- **Because each snapshot is added in the SAME build it becomes main, the snapshot-LOG entry timestamp is
  the snapshot's OWN `timestampMillis` (Java `isAddedSnapshot ? snapshot.timestampMillis() :
  lastUpdatedMillis`).** So a no-rollback forked log is FULLY deterministic — and Java `addSnapshot` also
  sets `lastUpdatedMillis = snapshot.timestampMillis()`, so the `metadata_log_entries` SYNTHETIC current
  entry (at `lastUpdatedMillis`) lands on EXACTLY the last snapshot's ts — which BONUS-pins the `<=`
  inclusive boundary of `snapshotIdAsOfTime` (an entry exactly on a snapshot-log ts must resolve TO that
  snapshot, not the previous one). Keep the three snapshot timestamps ASCENDING (ROOT<SIBLING<CURRENT) or
  the "before last snapshot-log entry" guard trips and `snapshotIdAsOfTime` (which assumes ascending) misreads.
- **The `metadata-log` must be INJECTED (not driven by real commits) for deterministic `latest_*`
  resolution.** *Why:* real commits stamp metadata-log timestamps with `base.lastUpdatedMillis()` (≈ now),
  which sit far AFTER the 2018–2020 snapshot timestamps, so every entry collapses to the latest snapshot —
  the NULL/middle cases are never exercised. Inject via `JsonUtil.mapper().readTree(json)` → set a
  `metadata-log` ArrayNode (keys EXACTLY `timestamp-ms`/`metadata-file`) → re-serialize. This is the Java
  analog of the Rust unit test's `meta.metadata_log = vec![...]`; Java's REAL `MetadataLogEntriesTable`
  still computes `latest_*` over it, so it stays a genuine oracle. Straddle the snapshot-log timestamps to
  hit NULL (before first) / a-middle-snapshot / current.
- **Pin `metadata_location` to a STABLE LOGICAL URI on BOTH sides.** The `metadata_log_entries` synthetic
  current entry's `file` column = `metadataFileLocation()` (Java) / `metadata_location()` (Rust). Re-parse
  the Java base with a fixed logical URI (`fromJson(STABLE_URI, json)`, NOT the on-disk path) and set the
  Rust test's `.metadata_location(STABLE_URI)` to the same literal, or that one row mismatches
  non-portably. (Snapshots/refs don't surface the location, so the prior increment didn't need this.)
- **A log can carry DUPLICATE snapshot ids (rollbacks re-stamp), so sort history rows by the COMPOSITE
  `(made_current_at, snapshot_id)`, not by snapshot_id alone**, for order-independent comparison.
- **The validation-first reviewer ran its OWN mutations** (flip SIBLING `is_current_ancestor`; flip the
  creation row's NULL `latest_snapshot_id`), each failing the matching interop test, then restored
  byte-clean — confirming the derived columns are load-bearing in the comparison, not just decorative
  asserts. Good pattern when "validation is key": the critic mutation-probes the FIXTURE/test, not just
  the prose.

### 2026-06-09 (Manifest-reading interop A1 — `files`/`data_files`/`delete_files` — ORCHESTRATOR + REVIEWER Opus)
- **Java can write a REAL on-disk table (metadata + avro manifest-list + manifests) with NO parquet/hadoop
  deps.** Replicate Java's own test infra: a `LocalFileIO` (`org.apache.iceberg.Files.localOutput/localInput`
  for newOutputFile/newInputFile; `java.io` delete) + a minimal `LocalTableOperations` (commit writes
  `vN.metadata.json` to disk via the FileIO; `metadataFileLocation`=`<dir>/metadata/<name>`;
  `locationProvider`=`LocationProviders.locationsFor(location,props)`; `newSnapshotId`=counter). Then
  `new BaseTable(ops,name).newAppend().appendFile(df).commit()` / `.newRowDelta().addDeletes(del).commit()`
  write genuine avro manifests + manifest-list. The `DataFile`/`DeleteFile` are pure metadata
  (`DataFiles.builder(spec).withPath/withRecordCount/withFileSizeInBytes/withMetrics/withPartitionPath`,
  `FileMetadata.deleteFileBuilder(spec).ofPositionDeletes()...`) — their .parquet paths NEED NOT EXIST
  because the `files`/`entries`/`manifests`/`partitions` tables read only the manifest. The metadata-table
  rows materialize the SAME way as the pure-metadata tables (`MetadataTableUtils` + `planFiles()` +
  `asDataTask().rows()`) because `BaseFilesTable.ManifestReadTask implements DataTask`.
- **Manifest interop is run.sh-driven (regenerate-and-compare), NOT offline-committed.** Avro manifests +
  manifest-list bake in ABSOLUTE paths, so committed binary fixtures aren't portable. So: a new run script
  regenerates the table into a gitignored `dev/java-interop/target/` temp dir each run; the Rust test is
  ENV-GATED (`ICEBERG_INTEROP_MANIFEST_DIR`) and does a runtime EARLY-RETURN (a clean no-op, NOT `#[ignore]`)
  when the var is unset, so the offline `cargo test` gate stays green. The orchestrator verifies by running
  BOTH the offline gate AND the run script. (Pure-metadata interop stays offline-committed JSON.)
- **VERIFY an on-disk representation against the actual bytes before calling it a divergence.** The `files`
  table's `file_format` looked like a write divergence (Java row `PARQUET`, Rust `parquet`). Reading the
  Java-written avro manifest DATA BLOCK showed the on-disk value is LOWERCASE `parquet` on BOTH sides — Java
  reads it via `FileFormat.fromString` (`toUpperCase`→`valueOf`) and its `FilesTable` row re-emits the enum's
  uppercase NAME, while Rust surfaces the on-disk string via `DataFileFormat`'s lowercase `Display`. So it is
  a COSMETIC inspection-table-only difference (NO on-disk / Direction-2 divergence). A small follow-up can
  upper-case the `files`/`entries`/`all_*` `file_format` column to match Java. (Don't "fix" the on-disk
  `Display`/serde — that's correct.)
- **Rust `spec::DataFile` models the metric maps (column_sizes/value_counts/null/nan/bounds) as
  NON-optional `HashMap`**, so an absent map projects to an EMPTY `{}` whereas Java emits `null`. A
  model-level (not rendering) divergence — to match exactly would need optional maps. Documented; canonicalize
  `None`≡`Some(empty)` in the interop comparison.
- **A foundation increment should SURFACE divergences, not hide them.** A1 canonicalized the two presentation
  divergences for the BULK equality but RAW-pinned both in focused asserts so neither can silently drift, and
  documented each — the right move (mirrors the existing GAP_MATRIX "known divergence" pattern for the
  unpartitioned-partition column). The reviewer mutation-probed the comparison (corrupt a `record_count` and a
  single lower-bound hex byte → both FAIL) to prove it's byte/value-level, not a false-pass.

### 2026-06-09 (Manifest-reading interop A2 — `entries`/`manifests`/`partitions` — ORCHESTRATOR + REVIEWER Opus)
- **Interop surfaced a REAL production parity bug — and the FIX was a verified one-liner.** Rust
  `inspect/partition_summary.rs::bound_to_string` rendered a STRING partition bound JSON-QUOTED (`"a"`) via
  `Datum::to_string`, whereas Java `ManifestsTable.partitionSummariesToRows` (core L117–144) renders each
  bound via `Transform.toHumanString(type, value)` → bare `a` for a string. Fix: `Datum::to_string` →
  `Datum::to_human_string` (datum.rs:1195 — raw for `PrimitiveLiteral::String`, delegates to `to_string` for
  EVERY other primitive). Because non-string bounds are byte-identical, no int/long-partition unit test broke;
  the new A2 `manifests` test pins the bare-string case. **Verify BOTH halves before accepting a parity fix:**
  (a) the Rust method's exact semantics (here: only strings change), (b) the Java call-site
  (`grep ManifestsTable` → `toHumanString`). This is the payoff of interop testing on partitioned tables (the
  pure-metadata + int-partitioned unit fixtures never exercised a string partition bound).
- **Don't predict the exact metadata-table rows — let Java materialize them and assert Rust==Java.** The A2
  prompt guessed surviving data files would be status 1 (ADDED); Java's `newDelete` REWRITES the DATA manifest,
  so survivors are carried as status 0 (EXISTING) and only the position-delete is status 1. The builder
  asserted against Java's REAL rows (oracle wins) and still hit the required headline (a status==2 tombstone).
  Build a table RICH enough to hit the cases (tombstone, content-gated manifests, multi-partition + deletes);
  the values are whatever Java produces.
- **A `newDelete(file)` removes that partition's only live data → the partition VANISHES from `partitions`.**
  Keep a SURVIVING data file in a partition you want to remain a row, while still deleting another file to
  create the `entries` DELETED tombstone (A2 added D=cat=b so cat=b stays a live partition after deleting B).
- **Reuse the nested-struct extraction across tables via a small trait, not a copy.** `entries.data_file` is
  the SAME 21-field DataFile projection as `files`; the builder added a `ColumnSource` trait so A1's `FileRow`
  extraction runs over a `StructArray` (nested) as well as a `RecordBatch` (flat) — no duplicate extractor.
- **Keep increments isolated by writing a SEPARATE table per increment** (`<dir>/table` for A1,
  `<dir>/table_a2` for A2) under the same run script + test file, so a richer A2 table (with a tombstone that
  changes the live-file set) does NOT churn A1's committed-behavior assertions. Both are regenerated; the
  reviewer confirmed A1 stayed green throughout.

### 2026-06-09 (Manifest-reading interop A3 — the cross-snapshot `all_*` tables — ORCHESTRATOR + REVIEWER Opus)
- **The `all_*` tables "may return duplicate rows" (Java javadoc) — compare as an order-independent MULTISET,
  never a set.** A `HashSet`/`dedup` comparison would hide a missing OR extra duplicate. Pattern: sort BOTH
  sides by a TOTAL key (the row's `Debug` repr works for derive-`PartialEq`+`Debug` rows, since two rows are
  `==` iff their `Debug` strings match) and `assert_eq!` the equal-length vectors element-by-element — no
  dedup. (A3's `all_files` = 8 rows / 5 distinct paths: A/C/D carried in both the original and the rewritten
  manifest appear twice; the comparison must KEEP them.)
- **Pin the cross-snapshot reach with a present-in-all / absent-in-current pair.** The whole point of `all_*`
  is the dedup-by-path union of manifests reachable from ALL snapshots (Java `reachableManifests`), so assert a
  file deleted at the current snapshot (live in an OLDER reachable manifest) IS in `all_data_files` AND is NOT
  in a fresh `inspect().data_files()` scan over the same table — that assertion FAILS if the impl read only the
  current snapshot, which a pass-by-coincidence test would miss.
- **One richer table can serve multiple increments read-only.** A3 reused A2's `table_a2` (3 snapshots, a
  carried/shared manifest, a rewrite) verbatim — it already exercised cross-snapshot reach + a shared manifest
  (for `all_manifests`' per-(manifest×snapshot) non-dedup) + content-gating. A3 only ADDED the `all_*`
  materialization + tests; A2's table/JSONs/tests stayed byte-identical. **With A3, manifest-reading interop is
  COMPLETE for every inspection table** (pure-metadata done earlier); only the `readable_metrics` virtual
  column + scan interop (A4/A5) remain.

### 2026-06-09 (Scan-PLANNING interop A4 — ORCHESTRATOR + REVIEWER Opus)
- **Scan PLANNING interop needs NO parquet** — `table.newScan().filter(expr).planFiles()` (Java) /
  `table.scan().with_filter(pred).plan_files()` (Rust) read MANIFESTS + apply partition/metric pruning +
  associate deletes; they never open the data files. So A4 rides the same run.sh-driven no-parquet harness.
  (Only scan EXECUTION = reading rows → Arrow needs parquet — that's A5.)
- **Compare what's robustly comparable cross-language; DEFER what isn't, explicitly.** A4 compares the
  {planned data-file SET (by path), per-file sorted delete-file paths, a `residual_always_true` boolean}.
  The full residual EXPRESSION string differs by language syntax (Java `Expression.toString()` vs Rust
  `Predicate` Display), so string-comparing it would be fragile or vacuous — DEFERRED with a doc note (Rust
  residuals are unit-tested in `scan/mod.rs`). The boolean `residual fully covered by partitioning` (Java
  `residual().op()==Expression.Operation.TRUE` ↔ Rust predicate `None`/`AlwaysTrue`) IS a robust, non-vacuous
  parity signal — it varies across scenarios (true for pure-partition filters, false where a data-column
  predicate remains) and proves the partition-filter-removal split matches.
- **Make a fixture that exercises the SUBTLE pruning path: COLUMN-METRIC pruning.** Give the data files
  DISJOINT `id` bounds ([1,10]/[11,20]/[21,30]) so a filter like `id>15` MUST drop the [1,10] file via its
  upper bound — partition pruning alone wouldn't catch a regressed metric evaluator. (Partition pruning is the
  easy case; metric pruning is where planners diverge.)
- **Java attaches a partition-scoped position-delete to EVERY live data file in that partition** with a
  covering sequence number — not just the file it "came from". The cat=a delete associated with BOTH F1 and
  F3; Rust matched. Let the bulk per-file delete-set comparison (not a single hand-picked file) prove this, so
  you assert Java's REAL association rather than an assumed one.

### 2026-06-09 (DATA-LEVEL scan-execution interop — real parquet + merge-on-read — ORCHESTRATOR + REVIEWER Opus)
- **The A1-A4 manifest harness extends to REAL parquet data with `iceberg-data` + `iceberg-parquet`.**
  `GenericAppenderFactory(schema, spec).newDataWriter(localOutput, PARQUET)` writes a real parquet data file +
  builds the `DataFile` from its real metrics (the `data/.../FileHelpers.java` template); `newPosDeleteWriter`
  writes a real position-delete (`PositionDelete.set(path, pos, null)`). It writes to an iceberg `OutputFile`
  (`Files.localOutput`), so NO Hadoop FileSystem — but the parquet-hadoop classes need a runtime jar, so the
  oracle pom also needs `org.apache.hadoop:hadoop-client-runtime`. The first `mvn` run must be ONLINE to fetch
  the new deps; `-o` works after.
- **Java emitting its OWN read is the ground truth, not a hand-coded expected set.** `IcebergGenerics.read(table)`
  applies the deletes; emit the rows it returns (`java_scan_rows.json`). Then "Rust scan == Java read" is a true
  1:1 (Rust's `to_arrow()` merge-on-read vs Java's reader), not "Rust matches my guess".
- **DATA-LEVEL interop deps go in the TEST-ORACLE pom ONLY; the Rust `Cargo.toml`/`Cargo.lock` stay 0-diff.**
  The reviewer's #1 check is `git diff Cargo.toml Cargo.lock crates/*/Cargo.toml` == EMPTY. The shipped library's
  dependency surface is frozen; only the dev oracle (a tool like `dev/spark/`) gains parquet/hadoop.
- **Mutation-prove a merge-on-read read TWO ways:** (1) poison the expected-rows JSON (resurrect a deleted row)
  → the value comparison fails; (2) point the env dir at the PRE-delete snapshot (the data-only commit) → Rust
  scans all rows → fails. Both confirm the delete is genuinely applied + the comparison is non-vacuous, on the
  gitignored temp dir (restore by re-running the run script).
- **Env-gate edge case:** `std::env::var_os` treats set-but-EMPTY as present, so `VAR=""` would proceed +
  panic instead of skip. Harmless (the offline gate runs the var UNSET; run.sh passes an ABSOLUTE path), but
  treating empty-as-unset is the more robust gate. Also: `cargo test` sets CWD to the crate dir, so the env
  path must be ABSOLUTE (run.sh derives it from `SCRIPT_DIR`).
- **Direction-2 ("Java reads what RUST writes") is the write-action ✅ flip — and the PUBLIC API suffices.** An
  integration test can't see the `pub(crate)` row_delta crown-jewel helpers, so it RE-builds the write path from
  the public surface only: `iceberg::memory::MemoryCatalogBuilder` + `iceberg::io::LocalFsStorageFactory` (real
  on-disk warehouse), `iceberg::writer::*` (`ParquetWriterBuilder`/`FileWriter` for a real parquet data file,
  `PositionDeleteFileWriter` for a real position-delete), `tx.fast_append()`/`tx.row_delta()`, and
  `TableMetadata::write_to(file_io, "<dir>/.../final.metadata.json")` for a deterministic load path. Java's
  `IcebergGenerics.read` then reads it. **No Rust production change, no new deps** (the write path already
  exists). That this public-only path produces a Java-readable merge-on-read table is itself a parity result.
- **Make the cross-impl read NON-VACUOUS by mutating the WRITTEN ARTIFACTS, not just the expected rows.** The
  reviewer deleted Rust's delete-parquet (→ Java `NotFoundException` on the exact path Rust's manifest
  references) and truncated Rust's avro manifest (→ Java `RuntimeIOException`) — proving Java genuinely opens
  Rust's on-disk files, not a re-derivation. A green Direction-2 with these mutations failing = real byte-level
  write parity.

### 2026-06-09 (Equality-delete interop + run-script exit-code hardening — ORCHESTRATOR + REVIEWER Opus)
- **An equality delete only applies to data with a STRICTLY LOWER data-sequence-number.** The interop fixture
  must commit the DATA first (seq 1) and the equality-delete SECOND (seq 2, a later `rowDelta`/`row_delta`); if
  they shared one commit the delete would NOT apply and the "deletes work" test would be vacuously green. Pin
  the ordering. (The delete carries `equality_ids` = the keyed field ids + the delete-value rows; Java
  `GenericAppenderFactory.newEqDeleteWriter`, Rust `EqualityDeleteFileWriter`.)
- **`mvn -q exec:java` does NOT propagate the program's `System.exit(1)` to the shell exit code.** A
  run.sh-driven Java VERIFY step (Java reads what Rust wrote) therefore needs an explicit OUTPUT-SENTINEL
  check, not reliance on `$?`: capture the mvn output and assert `: 0 failures` is present with no `^FAIL `
  line, else `exit 1`. Without it, a future write-incompatibility would print FAIL but the script would
  falsely report DONE (a vacuous gate). Applied to every Direction-2 (Rust-writes→Java-reads) run script.
- **The reviewer decoded the Rust-written avro MANIFESTS with the production Rust reader to confirm the
  delete's content-type + `equality_ids` + sequence** — not just "the rows came out right". For a delete-type
  interop, verify the on-disk delete file is the RIGHT KIND (EqualityDeletes vs PositionDeletes) carrying the
  right metadata, so a position-delete masquerading as the fixture can't pass.
- **Workflow scripts are PLAIN JS, not TS:** an inline template-literal prompt containing angle-bracket tokens
  (Java generics like `List<Record>`, or `<->`) can trip the script parser ("Unexpected token … TypeScript
  syntax"). Build long prompts as `[ '...', '...' ].join('\n')` arrays of plain strings to avoid stray `<`/`>`
  and backtick-escaping hazards.

### 2026-06-09 (Partitioned merge-on-read interop, both directions — capstone Increment 4 — ORCHESTRATOR + REVIEWER Opus)
- **A partition-scoped delete is only correct if it does NOT cross partitions — pin BOTH the deleted row's
  absence AND a sibling partition's survival.** The fixture partitions by `identity(category)` with a data file
  PER partition (cat=a: 10/20/30, cat=b: 40/50) and a position-delete in cat=a only (position 1 = id=20). The
  load-bearing assertions are (1) id=20 ABSENT and (2) cat=b's 40/50 ALL present — a bug that applied the cat=a
  delete to cat=b, or dropped a whole partition, fails (2), not (1). Rust's `delete_file_index` keys deletes by
  partition + spec id, so the cat=a delete reaches only the cat=a data file; that's the behavior under test.
- **Direction-2 partitioned write needs a `PartitionKey`, and it does double duty.** Building the production
  `DataFileWriter`/`PositionDeleteFileWriter` with a `PartitionKey::new(spec, schema, Struct::from_iter([Some(
  Literal::string("a"))]))` both (a) auto-stamps the partition `Struct` + spec id onto the written `DataFile`
  (so the manifest entry's partition matches) AND (b) routes the parquet under the partition path via the
  location generator. One data file per partition fast_appended at seq 1, then the cat=a partition-scoped
  position-delete row_delta'd at seq 2 — Java's `IcebergGenerics` reads it back to {10,30,40,50}. Still NO Rust
  production change, Cargo still 0-diff (the partitioned write + partition-aware read already exist).
- **The capstone is the cross-product, not a single axis.** Done = {position, equality} deletes × {Java-writes-
  Rust-reads, Rust-writes-Java-reads} × {unpartitioned, partitioned} — each cell a real-row round-trip with the
  reviewer mutating the written artifacts. Partitioning was the last axis; with it the data-level interop suite
  is complete (4 commits). Deferred within partitioning (its own future increment): multi-file-per-partition +
  non-identity transforms (bucket/truncate) + more column types.

### 2026-06-09 (OverwriteFiles.validateNoConflictingDeletes + shared validate_no_new_deletes_for_data_files — ORCHESTRATOR + REVIEWER Opus)
- **The delete-applies-to-data-file sequence boundary is INCLUSIVE `>=`, not `>`.** Java
  `DeleteFileIndex.forDataFile` → `*.filter` → `findStartIndex` keeps deletes with `data_seq >=
  startingSequenceNumber`. My builder PROMPT loosely paraphrased it as `>`; the builder correctly followed the
  REAL Java source (`delete_seq < starting_sequence_number => not applicable`, i.e. `>=`) — a good instance of
  the builder trusting the cited source over the orchestrator's loose wording. LESSON (orchestrator): cite the
  exact Java method + boundary; don't paraphrase comparisons. LESSON (reviewer): re-derive the boundary from
  the Java source, not the prompt.
- **A conservative OVER-approximation is a legitimate, safe divergence for a REJECT check.** The equality-delete
  applicability omits Java `EqualityDeletes.filter`'s per-file `canContainEqDeletesForFile` bounds check, so it
  may flag MORE conflicts than Java, never fewer. For a serializable-isolation validation that REJECTS on
  conflict, over-rejecting is safe (never lets a real conflict through); under-rejecting would be the bug.
  Document it in code as the contract (same shape as the `InclusiveMetricsEvaluator` "file-level only, never
  under-rejects" note on `validate_no_conflicting_data`).
- **Add a focused seq-PRESERVING sibling walk rather than refactoring the shared one.** The sequence number
  lives on `ManifestEntry`; the heavily-documented shared `added_delete_files_after` returns bare
  `Vec<DataFile>` (drops it). `forDataFile` needs the entry seq, so the builder added
  `added_delete_files_with_seq_after` (→ `Vec<(DataFile, Option<i64>)>`) alongside it — least churn to the
  proven shared walk, and the seq is exactly the extra datum this check needs.
- **The tx-captured-start pin remains the recurring must-have** on EVERY concurrent-commit validation: a test
  that OMITS `validate_from_snapshot` and still rejects (relying solely on the `Transaction::new` capture),
  mutation-checked by making the code read the refreshed head and confirming that one test fails.

### 2026-06-09 (OverwriteFiles.overwriteByRowFilter — ORCHESTRATOR + REVIEWER Opus)
- **Delete-by-row-filter evaluates metrics on the per-file RESIDUAL, NOT the full predicate.** Java
  `ManifestFilterManager.PartitionAndMetricsEvaluator` does `residual = residualEvaluator.residualFor(partition)`
  THEN strict/inclusive metrics on the residual. My builder prompt WRONGLY suggested an inclusive-partition
  pre-filter + metrics on the full predicate — which spuriously partial-errors on a partition-column predicate
  (`x==0` on a file in partition `x=0` that has no `x` metrics column: strict=false/inclusive=true → false
  PARTIAL). The residual folds the partition tuple (`x=0` ⇒ residual `alwaysTrue` ⇒ strict-match ⇒ DELETE). The
  builder caught + corrected this by reading the REAL Java source. LESSON (orchestrator): for predicate-vs-file
  logic, cite the exact Java evaluator (here `PartitionAndMetricsEvaluator` + `residualFor`) and don't invent a
  composition; Rust already has `ResidualEvaluator` (it internally does the strict/project + ExpressionEvaluator).
- **The delete-by-filter decision tree is KEEP / DELETE / PARTIAL-ERROR.** `!Inclusive` ⇒ keep (no rows match);
  `Strict` ⇒ delete (all rows match); else (might-but-not-all) ⇒ non-retryable error "Cannot delete file where
  some, but not all, rows match filter". The partial-error is the SUBTLE correctness point — without it a
  row-filter overwrite silently drops non-matching rows. Pin it with a straddling-bounds test; mutation-check by
  flipping the DELETE decision strict→inclusive (the partial file then deletes silently → the test must fail).
- **Implementing a write MODE shifts a downstream DEFAULT.** Adding `overwrite_by_row_filter` made the obsolete
  GAP_MATRIX note "the row-filter branch never applies" WRONG: Java `dataConflictDetectionFilter()` now routes
  the row filter as `validate_no_conflicting_data`'s default conflict filter (when set + no explicit deletes).
  When you land a deferred mode, GREP the docs/code for "deferred"/"never applies" notes that referenced it and
  fix them — a new capability can silently change a default elsewhere.
- **Document the non-ported nuances as conservative postures.** `ManifestFilterManager` has delete-manifest-
  specific branches (`failAnyDelete`, duplicate-path warning, `isDelete`/`isDanglingDV`/`minSequenceNumber`)
  irrelevant to the data-file row-filter case; not porting them is fine IF named explicitly in the report +
  module doc so a future reader knows the boundary.

### 2026-06-10 (E2 — rewrite-family metadata interop — ORCHESTRATOR Fable + REVIEWER Opus)
- **DO canonicalize manifest-LIST order with the FULL count tuple — (content, seq, min_seq) TIES
  within a single commit.** A rewritten (tombstone-carrying) manifest and the same commit's added
  manifest share all three; the tie falls back to each writer's manifest-list file order, which is
  writer-dependent and NOT a spec contract. The first E2 run failed on exactly this; an
  order-insensitive re-comparison proved every hunk a pure swap (Rust's four write actions were
  already Java-identical). When a cross-language comparison fails, CHECK ORDER-INSENSITIVELY before
  hunting a semantic bug. (The extended 9-tuple is still not provably total — a future
  fanout/multi-spec fixture could tie with differing entries; flagged in both emitters.)
- **DO mirror Java's FAST producer (`newFastAppend`), never `newAppend`, when the Rust side uses
  `fast_append`** — `newAppend` is the MERGING producer whose manifest-merge machinery Rust does
  not have; under merge thresholds it would produce a different manifest count/shape and the
  comparison would fail on machinery Rust never claims.
- **Java's delete-resolution is PATH equality end-to-end** (`DataFileSet` equality/hashCode are
  purely `file.location()`), so Java `deleteFile(DataFile)` and Rust `delete_file(path)` are the
  SAME resolved semantics — the API shapes differ, the contract doesn't (reviewer-cited).
- **DO scope an interop claim to the PATHS the fixture exercises.** The five-commit chain proves
  the explicit-API paths only — not row-filter commits, not conflict validation (linear chain, no
  concurrency), not DELETE-content manifests in the rewrite family, not multi-spec. The GAP_MATRIX
  notes say "metadata-level interop ✅ (explicit-API paths)", rows stay 🟡. Also
  reviewer-corrected: Java does NOT enforce rewrite record-count conservation
  (`validateReplacedAndAddedFiles` checks non-emptiness only) — conservation in the fixture is a
  test-data property, not a Java invariant.

_promoted 2026-06-11 → dev/java-interop/map.md#debug (order-insensitive-first comparison rule)_

### 2026-06-10 (Phase-2 completion arc Increment 1 — RewriteManifests, BUILDER + REVIEWER Opus)
- **DO pick the RIGHT corrupting mutation for a provenance suite: `add_entry` (re-stamp) KEEPS an
  explicit non-negative sequence number, so it only fails metadata assertions — the resurrection
  mutation is SEQ-STRIPPING (`sequence_number = None` ⇒ V2/V3 re-inheritance of the NEW, higher
  snapshot seq ⇒ older position deletes stop applying).** *Why:* the builder's re-stamp mutation
  passed the merge-on-read scan test and looked like a scan-level blind spot; the reviewer's
  seq-strip mutation made the SAME scan test fail with resurrected rows — the suite was sound, the
  first mutation was just too weak. Run BOTH mutations on any manifest-rewriting change; pin the
  on-disk seqs too (read the rewritten manifest's RAW avro via `Manifest::try_from_avro_bytes`,
  pre-inheritance, and assert explicit original seqs — never null).
- **DO NOT trust a builder's "Java would not emit key X" divergence claim without re-deriving from
  `SnapshotSummary.Builder.build()`.** *Why:* the builder flagged `changed-partition-count=0` as a
  Rust-only key for rewrite snapshots; the source (build() L191-213) shows `trustPartitionMetrics`
  stays true with no files added and Java emits `changed-partition-count=0` TOO — parity, not
  divergence. The interop s6 comparison must expect it on both sides.
- **DO key cluster/fanout manifest writers by `(key, partition_spec_id)` and PIN the multi-spec
  axis with a partition-evolution fixture** (append spec 0 → evolve → append spec 1 → cluster by a
  CONSTANT key → one output manifest per spec id). *Why:* a spec-id-less key cross-merges
  partition tuples from different specs into one manifest (`zip_eq` panic at best, corrupt
  partition metadata at worst); single-spec fixtures can never catch it.

_promoted 2026-06-11 → docs/testing.md (re-stamp + seq-strip double mutation; raw-avro on-disk seq pins)_

### 2026-06-10 (Phase-2 completion arc Increment 2 — RewriteFiles seq preservation, BUILDER + REVIEWER Opus)
- **WHEN LIFTING A COARSE GUARD, AUDIT THE WHOLE PATH IT GUARDED — the guard may be masking a
  SECOND latent bug.** *Why:* lifting `has_outstanding_delete_files` exposed that
  `RewriteFilesOperation::existing_manifest` returned `current_data_manifests()` only — a rewrite
  on a delete-bearing table silently DROPPED every DELETE manifest from the new snapshot (table-
  wide delete loss, resurrection regardless of seq preservation). The guard had made the broken
  path unreachable, so no test could ever catch it. Fixed: carry ALL current manifests; a DELETE
  manifest can never match a data `delete_path` in `process_deletes`, so it flows through the
  carry branch. Java parity: `MergingSnapshotProducer.apply` (L973-1011) composes BOTH
  `filterManager.filterManifests(dataManifests)` AND `deleteFilterManager.filterManifests(
  deleteManifests)`; Rust's carry-unchanged is conservative-safe (Java also drops fully-dangling
  delete manifests — not ported, harmless retention). Pin BOTH levels: the read-side crown jewel
  (scan) AND a manifest-LIST structural pin (delete-manifest count survives the commit) — the two
  fail under DIFFERENT mutations (carry-revert vs seq-strip), so neither subsumes the other.
- **THE SAME `current_data_manifests()`-only bug exists in `delete_files.rs` (L256), `overwrite_
  files.rs` (L695), `replace_partitions.rs` (L451) — UNGUARDED.** Any of those actions committed
  on a merge-on-read table (Java- OR Rust-written) drops all outstanding delete manifests. The E2
  metadata interop never saw it (its chain had no delete manifests — exactly the scoped-out path).
  Tracked as the arc's Increment 2b (fix + per-action crown jewels).
- **DO wire `ignore_equality_deletes = data_sequence_number.is_some()` (Java MergingSnapshot-
  Producer L475-479) and pin BOTH directions with a concurrent-eq-delete pair.** *Why:* with the
  seq preserved, a concurrent equality delete still applies to the rewritten data (eq applies iff
  `data_seq < delete_seq` STRICTLY) — not a conflict; without preservation it IS fatal. A
  position delete is path-scoped — it dies with the replaced file, so a NEW one is ALWAYS fatal.
  Corollary: the resurrection crown jewel MUST use an EQUALITY delete — a position delete cannot
  resurrect rows via sequence numbers (the delete vector is keyed by the data file's path).
- **DO reject a NEGATIVE explicit data sequence number at the action boundary.** *Why:* the Rust
  `ManifestWriter::add_entry` silently STRIPS a negative explicit seq into `None` ⇒ V2/V3
  re-inheritance of the new (higher) seq ⇒ exactly the resurrection the parameter exists to
  prevent. Java never receives one (compactions pass real seqs); Rust fails loudly.

_promoted 2026-06-11 → crates/iceberg/src/transaction/map.md#debug (a lifted guard exposes latent bugs in the path it guarded)_

### 2026-06-10 (Phase-2 completion arc Increment 3 — MergeAppend, BUILDER + REVIEWER Opus)
- **The uncommitted-new-manifest read-back chain is load-bearing for any merging producer:**
  `load_manifest` on a manifest whose list entry has `sequence_number == -1` (UNASSIGNED) makes
  `inherit_data` stamp `Some(-1)` into its Added entries; re-routing them through `add_entry`
  strips the negative back to `None` on disk ⇒ they correctly re-inherit the REAL snapshot seq at
  commit. Carried committed entries go through `add_existing_entry` (touches ONLY status; both seq
  fields + snapshot id preserved verbatim; `add_entry_inner` hard-errors on a missing seq). Pin
  BOTH halves with a raw-avro on-disk test (new entries None, carried entries Some(original) !=
  the merged list seq).
- **A suppression/filter test is VACUOUS if an earlier filter already removes its fixture — route
  the case through the path under test and ASSERT the routing.** *Why:* the first tombstone-
  suppression test used a manifest whose only entry was the tombstone; `existing_manifest`'s
  has-live-files filter dropped it before the merge ever saw it, and the broaden-mutation passed.
  Fix: co-locate the tombstone with a LIVE entry in one manifest and assert pre-merge that the
  manifest reaches the merge input.
- **`manifests-created`/`-kept`/`-replaced` summary keys: Java MAIN's merging producer emits them
  (`SnapshotProducer.buildManifestCountSummary` L716-733); Rust emits them ONLY from
  RewriteManifests.** The interop canonical view's `SUMMARY_COUNT_KEYS` allowlist EXCLUDES them,
  so the s7 merge-append comparison is insensitive either way — no allowlist or production change
  needed for Increment 4. Proving manifests-* parity later requires a properly-tagged Java
  checkout (the /tmp ref is a depth-1 TAGLESS shallow clone — `git log -S` / `merge-base
  --is-ancestor` answers from it about version ancestry are ARTIFACTS, not facts).
- **Bin-packing port: Java `canAdd` is `<=` on weight, `<` on max-items; `packEnd` = reverse input
  → pack → reverse each bin → reverse bin list; min-count gate is STRICT `<` (== merges).**
  Hand-trace ≥3 cases through BOTH algorithms before trusting unit tests — a test asserting the
  port's own behavior pins nothing about Java.

_promoted 2026-06-11 → docs/testing.md (vacuous-suppression-test routing rule) + dev/java-interop/map.md#debug (tagless shallow clone / verify from jar bytecode)_

### 2026-06-10 (Phase-2 completion arc Increment 5 — stale-deferral audit + matrix repair, ORCHESTRATOR Fable)
- **DO verify a deferral note against the LIVE Java source before building it.** *Why:* the
  Increment-5 brief and the GAP_MATRIX OverwriteFiles cell both said "deferred:
  `validateDataFilesExist` wiring" — but Java's `BaseOverwriteFiles.validate` (L135-175) has
  exactly three blocks, ALL ported; `validateDataFilesExist` is RowDelta-ONLY (grep of core/
  confirms the single caller) and already landed in Rust 2026-06-09. OverwriteFiles' concurrent-
  removal protection is `failMissingDeletePaths` ≡ Rust `resolve_delete_paths`. Building the
  "missing" block would have been anti-parity. Same family as the 2026-06-09 "verify a suspected
  parity divergence against the LIVE Java source BEFORE instructing a fix" lesson — it applies to
  deferral notes too.
- **DO pipe-count-audit the GAP_MATRIX after any matrix-wide edit: every `^|` row must carry
  exactly 5 `|` characters.** *Why:* the de-triplication cell mover split the OverwriteFiles
  narrative MID-EXPRESSION on the logical-OR inside `(strict.eval(part) || metrics.eval(file))` —
  half the narrative was stranded in the matrix as a phantom column and the archive section ended
  mid-sentence. Raw pipes inside code spans break naive pipe-delimited cell handling. Repaired
  2026-06-10 by rejoining the strand verbatim in the archive (conservation preserved).

_promoted 2026-06-11 → CLAUDE.md Working conventions (GAP_MATRIX pipe-count audit)_

