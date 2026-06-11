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

# Todo Archive — Phase 2 — Write engine

Archived completed-increment narratives for Phase 2 (write actions + concurrent-commit conflict validation). Verbatim; not read by default. See [../todo.md](../todo.md) for live work and [map.md](map.md) for the index.

---

## Active: Phase 2 — Write engine (FIRST write increment)

Parity target: Java `iceberg-core` write actions (`MergingSnapshotProducer`, `ManifestFilterManager`,
`StreamingDelete`/`DeleteFiles`). Authoritative plan: [Roadmap.md](../Roadmap.md) Phase 2; status:
[docs/parity/GAP_MATRIX.md](../docs/parity/GAP_MATRIX.md).

### Phase-2 increment SEQUENCE (dependency, then value) — recorded 2026-06-07
1. **DeleteFiles** (this increment) — delete data files by path/reference; builds the foundational
   manifest-filter / rewrite machinery in `SnapshotProducer` that the rest reuse.
2. **OverwriteFiles** — delete-by-filter/files + add data files in one snapshot (reuses the filter machinery).
3. **ReplacePartitions** (dynamic partition overwrite) — replace whole partitions (reuses the machinery).
4. **RewriteFiles** — atomic replace of a set of files with another set (compaction primitive).
5. **RewriteManifests** — re-cluster/merge manifests without changing data.
6. **merge-append** — `MergeAppend` (vs the existing fast-append): merge small new manifests.
7. **RowDelta + position-delete / deletion-vector writers** — merge-on-read deletes.
8. **multi-op transaction hardening** — multiple write actions + optimistic-concurrency retry, Glue/S3 Tables.

### Increment 1 — DeleteFiles + manifest-filter machinery (IN PROGRESS, 2026-06-07, BUILDER Opus)
New file `crates/iceberg/src/transaction/delete_files.rs`; manifest-filter machinery added to
`crates/iceberg/src/transaction/snapshot.rs`; wired into `transaction/mod.rs`.

**Java rules to mirror (verified against `/tmp/iceberg-java-ref`):**
- `ManifestFilterManager.filterManifest` (line 368): a manifest that CANNOT contain a to-be-deleted file is
  carried forward UNCHANGED (`filteredManifests.put(manifest, manifest); return manifest`) — efficiency +
  fewer files. `canContainDeletedFiles` returns false when the manifest has no live files (here: when it
  contains none of the target paths).
- `filterManifestWithDeletedFiles` (line 497): rewrite — for each LIVE entry, if it matches a target,
  `writer.delete(entry)` (status→Deleted, carries the existing data_file, preserves data/file seq, snapshot_id
  set to the NEW snapshot); else `writer.existing(entry)` (status→Existing, preserves snapshot_id + both seq
  numbers — V2/V3 inheritance). Mirror via the Rust `ManifestWriter::add_delete_entry` / `add_existing_entry`
  (both already preserve exactly these fields; `add_existing_entry` keeps the original snapshot_id, matching
  Java `writer.existing`).
- `MergingSnapshotProducer.apply` (lines 1002-1009): after filtering, KEEP a manifest iff
  `hasAddedFiles() || hasExistingFiles() || snapshotId() == snapshotId()` (the new commit's id). A rewritten
  manifest where ALL live entries became Deleted has no added/existing files but its `added_snapshot_id` == the
  new snapshot id → it is KEPT (still written, with the Deleted entries). **DECISION: keep the rewritten manifest
  even when all-deleted (matches Java's `snapshotId()==snapshotId()` keep), and DROP an originally-empty manifest
  with no live files.** A manifest with no matching target is carried forward unchanged.
- `StreamingDelete` + `DeleteFiles.validateFilesExist` (line 91) + `failMissingDeletePaths`
  (`ManifestFilterManager.validateRequiredDeletes`, line 279): deleting a path not present in the table is an
  error when validation is on. Java defaults `validateFilesToDeleteExist=false`, but for Increment-1 correctness
  (and because path-based deletes have no partition pre-filter) we VALIDATE BY DEFAULT that every requested path
  matched a live entry, erroring otherwise (mirrors `failMissingDeletePaths`'s "Missing required files to
  delete" with `ErrorKind::DataInvalid`). **delete-by-row-filter / partition-predicate is OUT OF SCOPE** (Java
  `deleteFromRowFilter`/`dropPartition` — defer to OverwriteFiles/ReplacePartitions increments).
- Precondition relaxation: `SnapshotProducer::manifest_file` currently rejects a commit with no added files +
  no snapshot properties. A delete-only commit has deletes but no adds → relax to allow it; reject a
  truly-empty commit (no adds, no deletes, no properties).

**Seam design:** extend `SnapshotProduceOperation` with `delete_files(&producer) -> Vec<DataFile>` returning the
data files to delete (resolved from paths against the current snapshot at commit time). `manifest_file()` builds
a `HashSet<&str>` of target paths and, for each existing manifest, rewrites iff it contains ≥1 matching live
entry. The operation also supplies `existing_manifest()` (all current manifests, unchanged — the rewrite happens
in the producer, generic, reused by future ops). `Operation::Delete` recorded in the summary.

Plan:
- [x] A. `SnapshotProducer`: added `process_deletes` + `rewrite_manifest_with_deletes` +
      `new_filtering_manifest_writer` into `manifest_file()` — reads each existing manifest, rewrites the ones
      containing target paths (Deleted/Existing per entry, rewriting with the SOURCE manifest's own partition
      spec so spec-id/partition-type is preserved), carries the rest forward unchanged, drops no-live-file
      manifests (all-deleted rewritten manifests are kept — their `added_snapshot_id` is the new snapshot id,
      Java's `snapshotId()==snapshotId()` keep rule). Relaxed the empty-commit precondition (delete-only OK;
      truly-empty rejected). Extended `SnapshotProduceOperation` with `delete_files(&producer) -> Vec<DataFile>`
      (FastAppend returns empty).
- [x] B. `delete_files.rs`: `DeleteFilesAction` with `delete_file(path)` / `delete_files(paths)` /
      `delete_data_files(DataFiles)`; `DeleteFilesOperation` (`Operation::Delete`) resolves paths→DataFiles
      against the current snapshot AND validates that every requested path matched a live entry (the missing-path
      "failMissingDeletePaths" check must live in the operation, since the producer only sees the resolved
      `DataFile`s). `Transaction::delete_files()` + `mod` + `use` wiring.
- [x] C. 8 in-crate unit tests via `MemoryCatalog` + `make_v3_minimal_table_in_catalog`, all asserting the
      post-commit SCAN live set (the real correctness signal): removes-only-targeted-file → {A,C};
      marks-entry-deleted-and-counts-correct (Existing/Deleted + manifest counts); carries-untouched-manifest-
      forward-unchanged (same manifest_path); across-multiple-manifests; delete-all-in-a-manifest → empty;
      delete-only-commit allowed; absent-file errors; mixed-present-and-absent errors. Mutation-verified:
      breaking carry-forward fails the carry-forward test; swapping Deleted→Existing fails 6 tests.
- [x] D. Docs: GAP_MATRIX `Write: DeleteFiles` row 🟡 + headline-gap #1; Roadmap Phase 2 → 🟡 + sequence +
      current-state + next-move; this todo; lessons.
- [x] E. Verify gate from repo root: build clean; lib ×2 = 1352/0 both runs (was 1344 → +8); interop
      manage_snapshots/update_schema/update_partition_spec all 4/4; clippy -D warnings clean; fmt --check clean
      (one reflow applied via `cargo fmt`).

**Outcome (2026-06-07, Phase 2 Increment 1, BUILDER Opus):** `DeleteFiles` + the foundational manifest-filter /
rewrite machinery land 🟡 — the FIRST write-engine increment. **Filter machinery** lives in
`SnapshotProducer::process_deletes` (`transaction/snapshot.rs`), reused by every future write op via the new
`SnapshotProduceOperation::delete_files` seam (returns the `DataFile`s to remove). Java semantics mirrored
(each cited): unchanged-manifest carry-forward = `ManifestFilterManager.filterManifest` (a manifest with no
matching target is returned as-is); per-entry Deleted/Existing = `filterManifestWithDeletedFiles`
(`writer.delete(entry)` → `add_delete_entry` stamps the new snapshot id + preserves data/file seq;
`writer.existing(entry)` → `add_existing_entry` preserves snapshot-id + both seq numbers, the V2/V3
inheritance contract); all-deleted-manifest KEPT + no-live-file dropped = `MergingSnapshotProducer.apply`
lines 1002-1009 (`hasAddedFiles() || hasExistingFiles() || snapshotId()==snapshotId()`); precondition
relaxation lets a delete-only commit through (rejects a truly-empty one); absent-path error =
`failMissingDeletePaths`/`validateRequiredDeletes` ("Missing required files to delete", `DataInvalid`).
**API:** `Transaction::delete_files()` → `DeleteFilesAction::{delete_file, delete_files, delete_data_files,
set_commit_uuid, set_key_metadata, set_snapshot_properties}`. **OUT OF SCOPE (deferred, flagged):**
delete-by-row-filter / partition-predicate (Java `deleteFromRowFilter`/`dropPartition` — needs metrics
evaluators, lands with OverwriteFiles/ReplacePartitions); data-level Java interop round-trip (Spark/Docker =
CI-only). Files touched exactly the allowed set: `transaction/snapshot.rs`, new `transaction/delete_files.rs`,
`transaction/mod.rs` (wiring) + `transaction/append.rs` (the trait gained `delete_files`, so `FastAppendOperation`
needed the empty impl — flagged below as a necessary touch of an in-scope sibling), docs
`GAP_MATRIX.md`/`Roadmap.md`/`task/{todo.md,lessons.md}`. No Cargo/lockfile edits; no `#[ignore]`; no bare
`.unwrap()` in production paths. An Opus REVIEWER verifies next.

**Note on the one extra file (`transaction/append.rs`):** the brief's allowed-set listed `snapshot.rs`,
`delete_files.rs`, `mod.rs`, and the docs. Extending `SnapshotProduceOperation` with the new `delete_files`
method forced an empty impl on the EXISTING `FastAppendOperation` in `append.rs` (a 5-line method returning
`Ok(vec![])`) for the crate to compile — this is the trait's own sibling impl, not unrelated code. Flagged
per §6 rather than silently expanded; no behavior change to fast-append.

#### Increment 1 — REVIEW (2026-06-07, Opus REVIEWER, DELEGATED)
Adversarially verified points 1–6 against the Java source (`/tmp/iceberg-java-ref`) + independent
mutation tests. **No corruption bug in the production code — the rewrite/keep/drop logic is correct.** One
real TEST-COVERAGE gap found + fixed (the most dangerous bug class was unpinned).
- **Pt 1 (provenance — the #1 risk): CONFIRMED CORRECT + GAP FIXED.** Rust `add_existing_entry` preserves
  the entry's original `snapshot_id`/`sequence_number`/`file_sequence_number` (touches only `status`);
  `add_delete_entry` stamps the NEW snapshot id but keeps both seqs — exactly Java `GenericManifestEntry.
  wrapExisting`/`wrapDelete`. Entries arrive with populated seqs because `load_manifest` runs `inherit_data`.
  Proved end-to-end with a new test (append A@S1, append B+C@S2 one-manifest, delete B → C kept as Existing
  with S2+seq2, A carried fwd keeps S1, B tombstone = S3 + B's original seqs). **GAP: the builder's 8 tests
  ALL passed under a `snapshot_id` re-stamp mutation** — none pinned surviving-entry provenance. Added
  `test_delete_preserves_surviving_entry_provenance_across_snapshots` (mutation-verified: the ONLY test that
  catches the re-stamp).
- **Pt 2 (deleted entries + keep/drop): CONFIRMED.** Rewritten all-deleted manifest KEPT (its
  `added_snapshot_id` == new snapshot, Java `snapshotId()==snapshotId()`); unrewritten no-live-file manifest
  DROPPED (`has_added_files()||has_existing_files()`, mirroring Java `shouldKeep`). Added
  `test_all_deleted_manifest_kept_by_creating_commit_then_dropped_by_next` pinning the two-commit lifecycle
  (kept by creating commit, dropped by next) — the builder only covered the single-commit case.
- **Pt 3 (live-only + source spec): CONFIRMED.** Only `is_alive()` entries are eligible (already-Deleted
  skipped); rewrite uses `partition_spec_by_id(source_manifest.partition_spec_id)` (Java `reader.spec()`),
  NOT the table default — correct for partition-evolved tables. A full spec-evolution+data fixture is hard
  via the public API (`validate_added_data_files` rejects non-default-spec appends); the code path is
  correct by inspection. (Minor inspected-not-fixed nit: the filtering writer pairs the source spec with the
  table's CURRENT schema rather than the spec's bound schema — harmless because partition source-column
  types are stable; matches Java in practice. Tracked, not a bug.)
- **Pt 4 (mutation tests REAL): CONFIRMED all three.** (a) `add_existing_entry` instead of `add_delete_entry`
  for the target → 7 delete_files tests FAIL. (b) force every manifest to rewrite (never carry forward) →
  `test_..._carries_untouched_manifest_forward_unchanged` FAILs (path changes). (c) re-stamp existing
  snapshot id → only the new provenance test FAILs (see Pt 1). The scan assertions check the real live set,
  not just emitted updates.
- **Pt 5 (absent-file + precondition): CONFIRMED + GAP FIXED.** Absent path → `DataInvalid` "Missing
  required files to delete" (in the operation's resolution, where the requested set is known); mixed
  present+absent → same error (no silent partial delete); delete-only commit allowed; truly-empty rejected.
  The truly-empty rejection had NO test — added `test_empty_delete_commit_is_rejected`. (Benign redundancy:
  the missing-path check exists in BOTH `DeleteFilesOperation::delete_files` and `process_deletes`; the
  latter can't fire since it sees already-resolved files — defense-in-depth, not a defect.)
- **Pt 6 (no fast-append regression + scope): CONFIRMED.** `FastAppendOperation::delete_files` returns empty
  → `process_deletes` early-returns → fast-append unchanged (append.rs tests pass). The trait extension is
  benign. No Cargo edits; no bare `.unwrap()` added to production.

**Review outcome (2026-06-07, Opus REVIEWER):** all 6 points adjudicated; NO production correctness bug
(provenance + keep/drop are right). Strengthened tests against the most dangerous unpinned bug class: +3
tests (provenance across snapshots; all-deleted keep-then-drop lifecycle; empty-commit rejection), each
mutation-verified. Files touched: `transaction/delete_files.rs` (+3 tests + 2 helpers), todo, lessons. NO
production `.rs` change, NO Cargo/lockfile edits. **Verify (repo root):** build clean; lib ×2 = 1355/0 both
runs (was 1352 → +3); interop manage_snapshots/update_schema/update_partition_spec all 4/4; clippy -D
warnings clean; fmt --check clean. Row stays **🟡** (data-level Java interop deferred, per the increment's
scope).

### Phase 2 Increment 2 — OverwriteFiles (explicit add + delete) (IN PROGRESS, 2026-06-07, BUILDER Opus)
New file `crates/iceberg/src/transaction/overwrite_files.rs`: `OverwriteFilesAction` composing the
fast-append add path with the DeleteFiles manifest-filter path in ONE `Operation::Overwrite` snapshot.
Data-integrity-critical. Reuses the producer machinery wholesale.

**Java rules verified against source** (`BaseOverwriteFiles.java` / `OverwriteFiles.java` /
`MergingSnapshotProducer.add`/`delete`):
- `addFile(DataFile)` → `add(file)` (same path fast-append uses); `deleteFile(DataFile)` →
  `deletedDataFiles.add(file); delete(file)`; `deleteFiles(DataFileSet, DeleteFileSet)` bulk variant.
- Added files validated like fast-append; deleted paths resolved against the current snapshot
  (`failMissingDeletePaths` semantics inherited from `ManifestFilterManager`).
- **Java `operation()` is DYNAMIC** (delete-only→DELETE, add-only→APPEND, both→OVERWRITE).
  **DELIBERATE DEVIATION (per brief):** this Rust action always records `Operation::Overwrite` (the brief's
  KEY test asserts Overwrite for delete+add AND the add-only / delete-only cases). Flagged as a tracked gap.
- Overwrite summary reflects BOTH added AND deleted file/record counts (`SnapshotSummary` overwrite).
- OUT OF SCOPE (deferred + noted): `overwriteByRowFilter(Expression)` (inclusive/strict metrics
  evaluators), concurrent-commit conflict validation (`validateNoConflictingData`/`...Deletes`/
  `validateFromSnapshot` — serializable isolation).

Plan:
- [x] **Shared-helper extraction (Rule of Three: two identical non-trivial uses → extract).** Factored the
      delete-path resolution + missing-path validation and the data-manifest listing into
      `SnapshotProducer::{resolve_delete_paths, current_data_manifests}` in snapshot.rs; `DeleteFilesOperation`
      and `OverwriteFilesOperation` both call them (DeleteFiles' two methods shrank to one-liners).
- [x] **Summary reflects deletes.** Added a `removed_data_files: Vec<DataFile>` field to `SnapshotProducer`,
      resolved once in `commit()` (via the operation's `delete_files` seam) BEFORE `summary()` and stored;
      `summary()` now also calls `SnapshotSummaryCollector::remove_file` for each (so `deleted-data-files` /
      `deleted-records` land); `manifest_file()` `std::mem::take`s the stored set instead of re-resolving.
      **SAME-CHANGE BUG FIX (in scope):** corrected the producer's `previous_snapshot` resolution from
      `snapshot_by_id(self.snapshot_id)` (the NOT-yet-committed new snapshot → always None → totals seeded
      from 0) to `current_snapshot()` (the real parent / branch head, Java `previousBranchHead`). Without it,
      `update_totals` underflowed (`0 - removed`) on any net-removal commit. **Also flipped the producer's
      `update_snapshot_summaries` `truncate_full_table` arg from `(op == Overwrite)` → `false`:** Java
      `SnapshotProducer.summary(previous)` calls `updateTotal` unconditionally with NO full-table-truncate
      branch — a partial `OverwriteFiles` must NOT reset totals to 0; that Rust path is for a future full
      replace/truncate action, and nothing else produces an Overwrite snapshot, so zero blast radius.
- [x] `OverwriteFilesAction`: `add_file`/`add_files`, `delete_file`/`delete_files`/`delete_data_files`,
      `set_commit_uuid`/`set_snapshot_properties`/`set_key_metadata`. `commit()` builds the producer with
      added files, calls `validate_added_data_files`, then `producer.commit(OverwriteFilesOperation,
      DefaultManifestProcess)`. `operation()` = `Operation::Overwrite`; `delete_files` → shared resolver;
      `existing_manifest` → shared data-manifest list.
- [x] Wired `Transaction::overwrite_files()` + `mod overwrite_files;` + `use ...OverwriteFilesAction` in
      `transaction/mod.rs`.
- [x] 9 tests (MemoryCatalog, V3 minimal identity(x) table; SCAN live set asserted): KEY delete-B+add-D →
      {A,C,D} op==Overwrite + B Deleted; add-only (Overwrite op); delete-only (Overwrite op);
      replace-in-same-partition; delete-absent errors (+ table unchanged); mixed present+absent errors;
      empty overwrite rejected; survivor provenance across snapshots (C keeps S2/seq2, A carried-fwd keeps
      S1, D gets S3 + new seq, B tombstone S3 + B's seqs); summary reflects added + deleted counts.
- [x] Docs: GAP_MATRIX `Write: OverwriteFiles` ❌ → 🟡 (+ DeleteFiles "will reuse" → "now reuses" + headline);
      Roadmap Phase 2 status/sequence/snapshot lines; this todo; lessons.
- [x] Verify gate (repo root, pinned nightly): build clean; lib ×2 = 1364/0 both runs (was 1355 → +9);
      interop manage_snapshots/update_schema/update_partition_spec 4/4 each; clippy -D warnings clean;
      fmt --check clean (one `cargo fmt` reflow applied to the new file + delete_files).

**Outcome (2026-06-07, Phase 2 Increment 2, BUILDER Opus):** `OverwriteFilesAction` lands 🟡 —
explicit add + delete in ONE `Overwrite` snapshot. The producer seam composes cleanly: added files go
through the fast-append path (`SnapshotProducer::new` + `validate_added_data_files` + `write_added_manifest`)
and deleted files through the DeleteFiles filter path (`process_deletes`) in a single `commit()`. Shared
`SnapshotProducer::{resolve_delete_paths, current_data_manifests}` factored out of DeleteFiles (Rule of
Three). The overwrite summary now reflects BOTH added and deleted file/record counts. Two same-change
producer fixes were required and are correct (and Java-faithful): the previous-snapshot resolution and the
truncate-arg — both pre-existing latent bugs that only surfaced once a producer operation actually removed
files. **DEVIATION (tracked):** always records `Operation::Overwrite` (Java's `operation()` is dynamic) —
per the brief; the summary carries the precise counts. **Deferred:** `overwriteByRowFilter` (metrics
evaluators), concurrent-commit conflict validation (serializable isolation), data-level Java interop. Files
touched exactly the allowed set: `transaction/overwrite_files.rs` (new), `transaction/snapshot.rs` (shared
helpers + summary + producer fixes), `transaction/delete_files.rs` (call the shared helpers + drop now-unused
imports), `transaction/mod.rs` (wiring), GAP_MATRIX, Roadmap, todo, lessons. No `snapshot_summary.rs` /
Cargo / lockfile edits; no `#[ignore]`; no bare `.unwrap()` in production paths. An Opus REVIEWER verifies next.

#### Phase 2 Increment 2 — REVIEW (2026-06-07, Opus REVIEWER, DELEGATED)
Adversarially verified the 5 brief points against the Java source (`/tmp/iceberg-java-ref`). Plan:
- [x] **Pt 1 (Operation parity) — GROUND TRUTH ESTABLISHED: Java IS dynamic; FIXED the always-Overwrite bug.**
      `BaseOverwriteFiles.operation()` (lines 50-60): `deletesDataFiles() && !addsDataFiles()` → DELETE;
      `addsDataFiles() && !deletesDataFiles()` → APPEND; both → OVERWRITE. `addsDataFiles()` =
      `!newDataFilesBySpec.isEmpty()` (requested adds); `deletesDataFiles()` = `filterManager.containsDeletes()`
      = `!deletePaths.isEmpty()` (requested delete-PATHS, before resolution). The builder's always-Overwrite
      was a parity BUG → made `OverwriteFilesOperation::operation()` dynamic via a new `adds_data_files: bool`
      field + `match (adds_data_files, !delete_paths.is_empty())`. Renamed the two tests
      (`..._records_append_operation`, `..._records_delete_operation`) to assert APPEND/DELETE. Mutation-verified
      (always-Overwrite → both fail; both-case tests stay green, so they can't pin the rule alone).
- [x] **Pt 2a (previous_snapshot fix): CONFIRMED Java-faithful + cumulative-totals test ADDED.** Java
      `SnapshotProducer.summary(previous)` (L392-419) seeds totals from
      `previous.snapshot(previousBranchHead.snapshotId()).summary()` (the parent / branch head), defaulting to 0
      only when there is no previous ref. Rust `current_snapshot()` = the main branch head = exactly that. Old
      code looked up `self.snapshot_id` (the not-yet-committed new snapshot) → always None → seed 0 →
      `0 - removed` underflow on net removal. Added `test_running_totals_accumulate_across_snapshots` (append
      A,B → 2; append C,D → 4 cumulative; overwrite-delete A → 3). Mutation-verified: reverting to the seed-0
      logic fails at the snapshot-2 accumulation (left=2, right=4).
- [x] **Pt 2b (truncate_full_table=false): CONFIRMED.** Java has NO full-table-truncate branch in
      `summary(previous)`; even `BaseReplacePartitions` only sets `replace-partitions=true` and accumulates
      totals via the same `updateTotal` path. So `false` is correct for OverwriteFiles (the Rust truncate path
      has no Java analogue for any standard op — noted as a residual). Residual: Java `updateTotal` is signed
      `long` + stops on negative; Rust `update_totals` is `u64` + panics on underflow (tracked, out of scope).
- [x] **Pts 3-5 (correctness / extraction / regression): CONFIRMED.** Live-set {A,C,D} + provenance +
      summary verified; mutation-tested provenance (swap `add_existing_entry`→`add_entry` re-stamps survivor →
      the provenance test fails on "surviving C must keep S2"). Shared helpers byte-identical to Increment-1
      inline code (DeleteFiles' 11 tests green). No fast-append regression (append tests green); scope clean.

**Review outcome (2026-06-07, Phase 2 Increment 2, REVIEWER Opus):** ONE parity bug fixed (always-Overwrite →
dynamic operation, matching Java `BaseOverwriteFiles.operation()`). Both producer summary fixes (a + b)
CONFIRMED Java-faithful; added the cumulative-running-totals regression test the change needed (would underflow
under the old seed-0 logic). 11 overwrite tests (9 → 11: +1 cumulative-totals, +0 net from the 2 renames).
Verify gate from repo root: build clean; lib ×2 = 1365/0 both runs (was 1364 → +1); interop
manage_snapshots/update_schema/update_partition_spec 4/4 each; clippy -D warnings clean; fmt --check clean (one
reflow applied). Files touched: `transaction/overwrite_files.rs` (dynamic op + 2 renamed tests + 1 new test),
GAP_MATRIX (dynamic-operation note replaces the stale deviation), todo, lessons. NO snapshot.rs production
change needed (both summary fixes were already correct); no Cargo edits; no `#[ignore]`; no bare `.unwrap()`
added. 🟡 stays.

### Phase 2 Increment 3 — ReplacePartitions (dynamic partition overwrite) (IN PROGRESS, 2026-06-07, BUILDER Opus)
New file `crates/iceberg/src/transaction/replace_partitions.rs`: `ReplacePartitionsAction` (dynamic
partition overwrite). Mirrors Java `BaseReplacePartitions` — when committed, for every partition an ADDED
file belongs to, DELETE every existing live data file in that same `(spec_id, partition)`, then add the new
files, in ONE `Operation::Overwrite` snapshot. Reuses the Increment-1 manifest-filter/rewrite machinery.

**Java rules verified against `/tmp/iceberg-java-ref` source (BaseReplacePartitions.java):**
- `operation()` returns `DataOperations.OVERWRITE` → `Operation::Overwrite` (always; line ~45). [VERIFIED]
- Ctor sets `SnapshotSummary.REPLACE_PARTITIONS_PROP = "replace-partitions" = "true"` (line ~36). [VERIFIED]
- `addFile(file)` → `dropPartition(file.specId(), file.partition())` + `replacedPartitions.add(...)` +
  `add(file)` (lines 49-55): the partition of each added file is dropped (every existing live file in that
  `(specId, partition)` is removed), and the file is added.
- `ManifestFilterManager.manifestHasDeletedFiles`/`filterManifestWithDeletedFiles` mark a live entry for
  delete iff `dropPartitions.contains(file.specId(), file.partition())` (lines 463, 518) — the by-PARTITION
  match (no path/row-filter/metrics needed). [VERIFIED]
- **Unpartitioned = full replace:** `apply()` (line ~108) — `if dataSpec().isUnpartitioned()
  deleteByRowFilter(Expressions.alwaysTrue())` → every file removed. (Falls out naturally: every file is in
  the single empty partition, so dropping that partition drops all.) [VERIFIED]
- **No `failMissingDeletePaths` for partition drops:** `validateRequiredDeletes` only validates path/file
  deletes (lines 280-300); `dropPartitions` has NO missing-validation → replacing a partition with no
  existing files is a pure add, no spurious delete, no error. [VERIFIED]
- **No-added-files:** Java does not special-case it; `super.apply()` (`SnapshotProducer.apply`) requires the
  commit to produce content. Rust mirrors via the existing precondition (no adds + no deletes + no props →
  rejected). A ReplacePartitions with no added files + no resolved deletes is effectively empty → rejected.
- **OUT OF SCOPE (defer + flag):** static `replaceByRowFilter`/explicit-partition APIs (need expression
  evaluators); concurrent-commit conflict validation (`validateNoConflictingData`/`...Deletes`/
  `validateFromSnapshot`) — serializable isolation, ancestor-chain replay.

**CRITICAL summary finding (verified against Java + Increment-2 review):** Java `SnapshotProducer.summary()`
has NO truncate/full-table branch — it computes `total = previous + added - removed` UNCONDITIONALLY via
`updateTotal` (SnapshotProducer.java:926). `replace-partitions=true` is JUST a summary prop. So the
Java-faithful Rust call is `truncate_full_table = FALSE`: the by-partition resolution already reports EVERY
removed file (so `deleted-data-files`/`deleted-records` are correct), and `update_totals` computes the right
post-replace totals (e.g. unpartitioned full replace: prev=N, added=M, removed=N → N+M-N = M, no underflow).
Setting `truncate_full_table=true` would DOUBLE-COUNT deletes vs. the resolved-removed set and diverge from
Java. (This corrects the brief's hint — flag in final report. The `truncate_full_table` Rust path has no
Java analogue for any standard op, per the Increment-2 reviewer.) So NO producer truncate-flag change; the
summary correctness comes from the resolved removed-file set + existing `update_totals`.

**By-partition delete resolution design (reuses Increment-1, no duplication):** The producer's manifest
rewrite (`process_deletes`) matches removed files by PATH. ReplacePartitions resolves its drop-partition set
to the matching `Vec<DataFile>` in the `delete_files` seam — scan the current data manifests, collect every
live `DataFile` whose `(partition_spec_id, partition)` is in the drop set — and returns them. The producer
then drives the EXACT SAME rewrite/keep/drop + provenance machinery unchanged. New shared helper
`SnapshotProducer::resolve_partition_deletes(&HashSet<(i32, Struct)>) -> Result<Vec<DataFile>>` in
snapshot.rs (sibling of `resolve_delete_paths`); `replace_partitions.rs`'s operation calls it. No change to
`process_deletes`/`rewrite_manifest_with_deletes`/`current_data_manifests`.

Plan:
- [x] A. `snapshot.rs`: added `resolve_partition_deletes(&self, &HashSet<(i32, Struct)>) -> Result<Vec<DataFile>>`
      (scans current data manifests, collects live DataFiles whose `(partition_spec_id, partition)` ∈ set; no
      missing-validation — partition drops are not path deletes). Shared, sibling of `resolve_delete_paths`.
- [x] B. `replace_partitions.rs`: `ReplacePartitionsAction` with `add_file`/`add_files` +
      `set_commit_uuid`/`set_snapshot_properties`/`set_key_metadata`. `commit()`: validates added files via
      `validate_added_data_files`; collects the drop set `{(spec_id, partition)}` from the added files; sets
      the `replace-partitions=true` snapshot property (layered ON TOP of caller props); drives
      `producer.commit(ReplacePartitionsOperation, DefaultManifestProcess)`. Operation = `Operation::Overwrite`;
      `delete_files` seam → `resolve_partition_deletes(drop_set)`; `existing_manifest` → `current_data_manifests`.
- [x] C. Wired `Transaction::replace_partitions()` + `mod replace_partitions;` + `use ...ReplacePartitionsAction`.
- [x] D. 8 tests (in `replace_partitions.rs`, `MemoryCatalog`; identity(x) fixture + an in-test unpartitioned
      V3 table helper since no unpartitioned JSON fixture exists): (1) cross-partition isolation A@x=0,B@x=1 →
      replace A2@x=0 → {A2,B}; (2) replace multiple partitions; (3) replace partition with multiple new files;
      (4) surviving-entry provenance (untouched B keeps S1/seqs); (5) unpartitioned FULL replace → {C}, marker
      set, totals 2-2+1=1, `deleted-data-files=2`; (6) replace empty partition = pure add; (7) Overwrite op
      recorded (in test 1); (8) marker + Deleted tombstone on partitioned replace. Mutation-verified: resolve
      nothing (under-delete) fails 6 tests; resolve everything (over-delete/cross-partition loss) fails 5
      tests incl. cross-partition isolation; re-stamp surviving entries fails the provenance test.
- [x] E. Docs: GAP_MATRIX `Write: ReplacePartitions` ❌ → 🟡 + headline-gap #1 + DeleteFiles reuse note;
      Roadmap Phase 2 status/sequence/snapshot/current-state lines; this todo; lessons.
- [x] F. Verify gate from repo root: build clean; lib ×2 = 1373/0 both runs (was 1365 → +8); interop
      manage_snapshots/update_schema/update_partition_spec all 4/4; clippy -D warnings clean; fmt --check clean
      (one reflow applied via `cargo fmt`).

**Outcome (2026-06-07, Phase 2 Increment 3, BUILDER Opus):** `ReplacePartitionsAction` (dynamic partition
overwrite) lands 🟡. **By-partition delete-resolution design:** the producer's `process_deletes` rewrite
matches removed files by PATH; ReplacePartitions's `delete_files` seam resolves its drop-partition set
`{(spec_id, partition)}` (collected from the added files) to the matching live `DataFile`s via the new
shared `SnapshotProducer::resolve_partition_deletes` (sibling of `resolve_delete_paths`), which then feed the
EXACT SAME Increment-1 rewrite/keep/drop + provenance-preservation machinery UNCHANGED — zero edits to
`process_deletes`/`rewrite_manifest_with_deletes`/`current_data_manifests`. **Java semantics mirrored
(cited):** `operation()` = `Overwrite` (`BaseReplacePartitions.operation()` = `DataOperations.OVERWRITE`);
`addFile` drops `file.partition()` then adds (`dropPartition` + `add`); the by-partition match is
`ManifestFilterManager`'s `dropPartitions.contains(file.specId(), file.partition())`;
`replace-partitions=true` summary marker (`SnapshotSummary.REPLACE_PARTITIONS_PROP`); unpartitioned = full
replace (every file in the single empty partition → all replaced, Java's `deleteByRowFilter(alwaysTrue)`);
a replaced partition with no existing files = pure add (Java's `failMissingDeletePaths` guards only path
deletes). **SUMMARY-FLAG CORRECTION (flagged in final report):** the brief hinted at setting the producer's
`truncate_full_table` flag for the unpartitioned full replace. The Java-faithful answer is
`truncate_full_table = FALSE` (the producer already passes `false`, unchanged): Java
`SnapshotProducer.summary()` has NO truncate branch — it computes `total = previous + added - removed`
unconditionally; the by-partition resolution already reports EVERY removed file, so `deleted-data-files`/
`deleted-records` are correct and `update_totals` yields the right post-replace totals (full replace of N
adding M → N+M-N = M, no underflow — verified by the unpartitioned test asserting total=1 + deleted=2).
Setting `truncate_full_table=true` would DOUBLE-COUNT vs. the resolved-removed set and diverge from Java
(matches the Increment-2 reviewer's "Java has no full-table-truncate branch even for ReplacePartitions").
**No-added-files behavior:** Java does not special-case it; a no-added-files replace resolves no deletes →
nothing added/removed (pinned by test 8 — the existing file is untouched). The action always sets the
`replace-partitions` marker (a snapshot property), so the producer's empty-commit precondition does not trip
for the no-added case; this is the standard Java shape (the marker is a property). **OUT OF SCOPE (deferred,
flagged):** static `replaceByRowFilter`/explicit-partition APIs (need metrics evaluators); concurrent-commit
conflict validation (`validateNoConflictingData`/`...Deletes`/`validateFromSnapshot` — serializable
isolation, ancestor-chain replay); data-level Java interop round-trip. **Files touched exactly the allowed
set:** new `transaction/replace_partitions.rs`, `transaction/snapshot.rs` (the by-partition resolver only —
no rewrite-machinery change), `transaction/mod.rs` (wiring), docs `GAP_MATRIX.md`/`Roadmap.md`/
`task/{todo.md,lessons.md}`. **Nothing outside the allowed set** — no `delete_files.rs`/`overwrite_files.rs`
touch was needed (the shared factor already existed from Increment 2); no Cargo/lockfile edits; no
`#[ignore]`; no bare `.unwrap()` in production paths. An Opus REVIEWER verifies next.

### Phase 2 Increment 4 — RewriteFiles (compaction-commit primitive) (IN PROGRESS, 2026-06-07, BUILDER Opus)
New file `crates/iceberg/src/transaction/rewrite_files.rs`: `RewriteFilesAction` — atomically replace a set
of DATA files with a new set in ONE `Operation::Replace` snapshot (Java `BaseRewriteFiles`). Data-file
rewrite ONLY (DELETE-file rewrite deferred). Mirrors the OverwriteFiles shape: added files → producer
(added manifest), to-delete `DataFile`s → resolved BY PATH via the shared `resolve_delete_paths` and removed
through the Increment-1 `process_deletes` rewrite machinery, in one snapshot.

**Java contract verified against `/tmp/iceberg-java-ref/core/.../BaseRewriteFiles.java` (read the source):**
- `operation()` returns `DataOperations.REPLACE` → `Operation::Replace` (NOT Overwrite/Delete).
- ctor calls `failMissingDeletePaths()` → every to-delete file MUST be present in the current snapshot
  (error if absent). Reuses `resolve_delete_paths` which already enforces this (Java `failMissingDeletePaths`).
- `validate()` → `validateReplacedAndAddedFiles()`: THREE preconditions:
  (1) `deletesDataFiles() || deletesDeleteFiles()` → **"Files to delete cannot be empty"** — the delete set
      MUST be non-empty (data-file rewrite: data-files-to-delete non-empty).
  (2) `deletesDataFiles() || !addsDataFiles()` → **"Data files to add must be empty because there's no data
      file to be rewritten"** — adds allowed only if data-files are being deleted (subsumed by (1) for the
      data-only case, but mirror it for the exact message).
  (3) delete-file precondition — OUT OF SCOPE (DELETE-file rewrite deferred).
  So: delete-only rewrite (delete N, add 0) is LEGAL; add-only rewrite (delete 0, add M) is REJECTED.
- `rewriteFiles(Set<DataFile> filesToDelete, Set<DataFile> filesToAdd)` is the primary entry; each `deleteFile`
  adds to `replacedDataFiles` + `delete(file)`, each `addFile` → `add(file)`.

**Out of scope (deferred + noted precisely):**
- (a) DELETE-file (position-delete / DV) rewrite (`deleteFile(DeleteFile)`/`addFile(DeleteFile)`) — needs the
  delete-file write path (later increment). Data-file rewrite only.
- (b) `dataSequenceNumber` preservation (`setNewDataFilesDataSequenceNumber` / carrying the replaced files'
  max seq onto added files to keep merge-on-read deletes applicable) — added files get a FRESH seq via the
  standard add path, correct for a pure data rewrite with no outstanding deletes. Tracked compaction follow-up.
- (c) `validateFromSnapshot` / `validateNoNewDeletesForDataFiles` / concurrent-commit conflict validation.

Plan:
- [x] A. `RewriteFilesAction`: `rewrite_files(files_to_delete, files_to_add)` primary entry +
      `delete_file(DataFile)`/`delete_files`/`add_file`/`add_files` builders; `set_commit_uuid`/
      `set_snapshot_properties`/`set_key_metadata`. To-delete files held as `Vec<DataFile>` (callers hold
      them); paths extracted into a `HashSet<String>` at commit via `delete_paths()` for `resolve_delete_paths`.
- [x] B. `commit()`: Java `validateReplacedAndAddedFiles` precondition (1) FIRST — reject empty-delete ("Files
      to delete cannot be empty"). Precondition (2) ("Data files to add must be empty...") is SUBSUMED by (1)
      for the data-only case (DELETE-file rewrite out of scope → deletesDataFiles() iff delete set non-empty),
      documented in the source rather than coded as an unreachable branch. Then `validate_added_data_files`;
      then `producer.commit(RewriteFilesOperation{ delete_paths }, DefaultManifestProcess)`.
      `RewriteFilesOperation::operation()` = `Operation::Replace`; `delete_files` → `resolve_delete_paths`;
      `existing_manifest` → `current_data_manifests`.
- [x] C. Wired `Transaction::rewrite_files(files_to_delete, files_to_add)` + `mod rewrite_files;` +
      `use ...RewriteFilesAction` in mod.rs.
- [x] D. **REQUIRED shared change (flagged):** added `Operation::Replace` to the
      `spec/snapshot_summary.rs::update_snapshot_summaries` op allowlist (was {Append, Overwrite, Delete}) —
      WITHOUT it the Replace snapshot fails to commit ("Operation is not supported."). One-line addition
      mirroring the existing entries; Java's `SnapshotProducer.summary` is op-agnostic. Mutation-verified
      load-bearing (removing it fails the KEY test with that exact error). Outside the named allowed set;
      minimal + required.
- [x] E. 10 tests (`MemoryCatalog`; assert post-commit SCAN live set): (1) KEY delete[A,B]+add[D] → live
      {C,D}, op==Replace, A&B Deleted tombstones, C keeps provenance; (2) rewrite across multiple manifests;
      (3) compaction-to-fewer-files (3→1); (4) delete-absent errors (table unchanged); (5) empty-delete
      rejected; (6) add-without-delete rejected (table unchanged); (7) delete-only rewrite legal; (8) summary
      added+deleted counts; (9) cross-snapshot provenance preservation (C keeps S2, A keeps S1, D gets S3, B
      tombstone S3 keeps orig seqs); (10) incremental-builder equivalence. Mutation-verified: forcing
      `operation()` → Overwrite fails the 3 op-asserting tests; disabling the empty-delete precondition fails
      the 3 precondition tests; removing `Operation::Replace` from the summary allowlist fails the KEY test.
- [x] F. Docs: GAP_MATRIX `Write: RewriteFiles` ❌ → 🟡 (+ headline-gap #1); Roadmap Phase 2 status/sequence/
      snapshot/current-state/next-increments lines; this todo; lessons.
- [x] G. Verify gate from repo root: build clean; lib ×2 = 1383/0 both runs (was 1373 → +10); interop
      manage_snapshots/update_schema/update_partition_spec all 4/4; clippy -D warnings clean; fmt --check clean
      (one reflow applied via `cargo fmt`).

**Outcome (2026-06-07, Phase 2 Increment 4, BUILDER Opus):** `RewriteFilesAction` (the compaction-commit
primitive) lands 🟡. **Design:** mirrors `OverwriteFilesAction` exactly — added files → producer (added
manifest), to-delete `DataFile`s → resolved BY PATH (paths extracted from the provided `DataFile`s, since
callers hold them) via the shared `SnapshotProducer::resolve_delete_paths` → the SAME Increment-1
`process_deletes` rewrite/keep/drop + provenance-preservation machinery, in one snapshot. ZERO edits to the
rewrite machinery (`process_deletes`/`rewrite_manifest_with_deletes`/`resolve_delete_paths`/
`current_data_manifests`). **Java semantics mirrored (cited against `core/.../BaseRewriteFiles.java`):**
`operation()` = `DataOperations.REPLACE` → always `Operation::Replace` (NOT dynamic like OverwriteFiles);
ctor `failMissingDeletePaths()` → every to-delete file must be present (reused via `resolve_delete_paths`'s
"Missing required files to delete" error); `validateReplacedAndAddedFiles()` precondition (1)
`deletesDataFiles() || deletesDeleteFiles()` → "Files to delete cannot be empty" (delete-only legal, add-only
/ empty rejected); precondition (2) subsumed (DELETE-file rewrite out of scope). **REQUIRED shared change
(flagged):** `Operation::Replace` added to the `update_snapshot_summaries` op allowlist in
`spec/snapshot_summary.rs` (the ONE edit outside the named allowed set) — without it `Replace` can't commit;
it's the direct analogue of the existing Overwrite/Delete entries and Java's summary is op-agnostic.
**OUT OF SCOPE (deferred, flagged):** (a) DELETE-file (position-delete / DV) rewrite — needs the delete-file
write path (later increment); DATA-file rewrite only. (b) `dataSequenceNumber` preservation
(`setNewDataFilesDataSequenceNumber`) — added files get a FRESH seq via the standard add path (correct for a
pure data rewrite with no outstanding deletes); tracked compaction-correctness follow-up paired with (a).
(c) `validateFromSnapshot` / `validateNoNewDeletesForDataFiles` / concurrent-commit conflict validation.
(d) data-level Java interop round-trip. **Files touched:** new `transaction/rewrite_files.rs`,
`transaction/mod.rs` (wiring), `spec/snapshot_summary.rs` (the one-line allowlist addition — flagged),
docs `GAP_MATRIX.md`/`Roadmap.md`/`task/{todo.md,lessons.md}`. No `snapshot.rs` change needed (the by-path
resolver + rewrite machinery already existed). No `overwrite_files.rs`/`delete_files.rs` touch needed. No
Cargo/lockfile edits; no `#[ignore]`; no bare `.unwrap()` in production paths. An Opus REVIEWER verifies next.

#### Phase 2 Increment 4 — REVIEW (2026-06-07, Opus REVIEWER, DELEGATED)
Adversarially verified points 1-5 against the Java source (`/tmp/iceberg-java-ref/core/.../BaseRewriteFiles.java`,
`SnapshotProducer.java`, `MergingSnapshotProducer.java`), with mutation tests for every load-bearing claim.
- [x] **Pt 1 (operation + precondition): CONFIRMED + mutation-verified.** `BaseRewriteFiles.operation()` →
      `DataOperations.REPLACE` always; Rust `RewriteFilesOperation::operation()` → `Operation::Replace`
      (mutation to `Overwrite` fails 4 op-asserting tests). `validateReplacedAndAddedFiles` precondition (1)
      `deletesDataFiles() || deletesDeleteFiles()` → "Files to delete cannot be empty": delete-only LEGAL,
      add-only/empty REJECTED. Rust enforces it in the action's `commit()` (the producer's own guard only
      rejects all-empty — confirmed: disabling the action precondition makes the add-only test COMMIT, so the
      action precondition is the sole load-bearing guard for add-only).
- [x] **Pt 2 (snapshot_summary allowlist): CONFIRMED + minimal + mutation-verified.** `update_snapshot_summaries`
      now admits `Operation::Replace`; mutation (remove it) fails 7 tests with "Operation is not supported."
      Java's `SnapshotProducer.summary(previous)` totals method is op-agnostic (all `updateTotal`, no per-op
      branch, no truncate), so admitting Replace is Java-faithful — directly analogous to the Overwrite/Delete
      entries. The added clause only short-circuits when op IS Replace, so existing ops are unaffected.
- [x] **Pt 3 (standard correctness): CONFIRMED, assertions real not tautological.** KEY scan test
      (A,B,C → delete[A,B]+add[D] → {C,D}, Replace, tombstones), compaction-to-fewer (3→1), surviving
      provenance, absent-file errors, cross-snapshot provenance all assert post-commit SCAN live sets +
      entry provenance. Mutation-verified the provenance: re-stamping surviving entries in
      `rewrite_manifest_with_deletes` fails exactly the KEY + cross-snapshot provenance tests.
- [x] **Pt 4 (dataSequenceNumber data-loss trap): GUARD ADDED + mutation-verified the corruption.** Today this
      library cannot WRITE delete files (no `RowDelta`; every add path rejects non-`Data` content) — so a table
      it wrote has no outstanding deletes. BUT it can READ + rewrite a Java-written table that has them →
      resurrection. The deferral was documented but NOT as a hard precondition. FIXED: `commit()` now rejects a
      rewrite when the current snapshot has any `Deletes`-content manifest (`has_outstanding_delete_files`,
      `ErrorKind::FeatureUnsupported`). Test builds a real position-delete manifest + asserts rejection;
      mutation (disable guard) → the rewrite COMMITS, proving the corruption is real. Docs updated in 3 places.
- [x] **Pt 5 (no regression + scope): CONFIRMED.** lib ×2 = 1384/0 (was 1383, +1 guard test); interop
      manage_snapshots/update_schema/update_partition_spec 4/4 each; clippy -D warnings clean; fmt clean. Only
      `rewrite_files.rs` (guard + test) + `transaction/mod.rs` (doc) touched in this review; no Cargo edits; no
      bare `.unwrap()` in production (the guard helper uses `?`).
- [x] **TRACKED 🟡 (not fixed, flagged):** Java's REPLACE record-count invariant (`SnapshotProducer` lines
      347-359: `added-records <= deleted-records`) is unmirrored — belongs in the shared `snapshot.rs` producer
      (outside this increment's file set) and is a logical-consistency guard, not a data-loss one. Follow-up.

**Review outcome (2026-06-07, Phase 2 Increment 4, REVIEWER Opus):** ONE data-loss guard ADDED + mutation-
verified (reject rewrite on a table with outstanding merge-on-read deletes — the dataSequenceNumber-resurrection
trap), shipping with a test that builds a real delete manifest. All 5 points adjudicated; row stays 🟡 (interop +
dataSequenceNumber preservation + conflict validation still deferred). Files touched: `transaction/rewrite_files.rs`
(guard + helper + test + doc), `transaction/mod.rs` (ctor doc), `task/{todo.md,lessons.md}`. One tracked 🟡
follow-up (REPLACE record-count invariant). No Cargo/lockfile edits.

### Phase 2 Increment 5a — PositionDeleteWriter (RowDelta write-path piece 1, BUILDER Opus, 2026-06-08)
First piece of the merge-on-read RowDelta write path: a base writer that writes Iceberg position-delete
files. Correctness-critical — a malformed delete file silently fails to delete rows or deletes the wrong
rows. Mirrors `EqualityDeleteFileWriter` structurally. New file
`crates/iceberg/src/writer/base_writer/position_delete_writer.rs`.

**RowDelta subsystem sub-sequence (this increment is 5a):**
- **5a PositionDeleteWriter** [THIS] — writes a position-delete file (`file_path`,`pos`) with
  `content(PositionDeletes)`; write-as-given (Java-faithful, caller sorts).
- **5b RowDelta action** — the `RowDelta` transaction action + producer delete-manifest handling (add the
  written delete files to a new snapshot via `MergingSnapshotProducer`-equivalent, delete-manifest write).
- **5c deletion-vector writer** — V3 Puffin DV writer (`delete_vector.rs` + `puffin/`); writers must not add
  new position-delete files to v3 tables (spec line 1933) — DVs replace them.

**Java rule verified against source** (`core/.../deletes/PositionDeleteWriter.java`, `MetadataColumns.java`,
`format/spec.md`):
- Position-delete schema (spec lines 449-451 / 1393-1394): `file_path: string` REQUIRED, field id
  **2147483546** (= `Integer.MAX_VALUE - 101`, Java `DELETE_FILE_PATH`); `pos: long` REQUIRED, field id
  **2147483545** (= `Integer.MAX_VALUE - 102`, Java `DELETE_FILE_POS`). Optional `row: struct` field id
  **2147483544** — OUT OF SCOPE (position-delete-with-row-data, noted).
- Content type `position deletes` (manifest `content` = 1).
- **Sorting:** Java basic `PositionDeleteWriter` writes records AS GIVEN (no reorder); the sorted
  (file_path,pos) ordering is the caller's / `SortingPositionOnlyDeleteWriter`'s job. Spec lines 1403-1405
  RECOMMEND sorting by file_path then pos (readers binary-search). Mirror Java: write as-given, DOC the
  recommendation + that sorting is the caller's responsibility. Do NOT silently reorder.
- Sort-order-id must be null for position deletes (spec line 745) — left null (DataFile default).

**metadata_columns.rs ALREADY has the constants — NOT touched.** `RESERVED_FIELD_ID_DELETE_FILE_PATH =
i32::MAX - 101 = 2147483546` and `RESERVED_FIELD_ID_DELETE_FILE_POS = i32::MAX - 102 = 2147483545` already
exist with the exact Java ids, plus `delete_file_path_field()` / `delete_file_pos_field()` helpers
(required string / required long). The read side (`arrow/delete_filter.rs`) already consumes exactly these
ids. So the writer just reuses them — no metadata_columns.rs edit needed (flagged: nothing added there).

Plan:
- [x] `PositionDeleteWriterConfig` — holds the position-delete Iceberg `SchemaRef` (built from
      `delete_file_path_field()` + `delete_file_pos_field()`) + its projected Arrow schema ref (for input
      validation). `pos_delete_schema()` free fn builds the canonical 2-field schema.
- [x] `PositionDeleteFileWriterBuilder<B,L,F>` { inner: RollingFileWriterBuilder, config } +
      `PositionDeleteFileWriter` mirroring the equality-delete writer; `IcebergWriterBuilder::build` clones
      the inner rolling builder + config + partition_key.
- [x] `IcebergWriter::write(RecordBatch)` — VALIDATE the batch's arrow schema equals the position-delete
      schema (field ids 2147483546/2147483545, types Utf8/Int64, required) before writing; reject otherwise
      (`ErrorKind::DataInvalid`). Write as-given (no reorder). `close()` stamps
      `content(DataContentType::PositionDeletes)`, partition/spec from partition_key, returns DataFile(s).
- [x] Tests (named for the risk): round-trip exact (file_path,pos) pairs (dropped/mangled positions = data
      not deleted); field ids in written parquet == 2147483546/2147483545 (wrong ids = Java can't read);
      content==PositionDeletes + record_count==N; multiple data files' positions in one file; reject a
      batch with the wrong schema; empty-input convention (matches equality-delete: no rows → still produces
      a closeable writer; assert behavior).
- [x] Wire `pub mod position_delete_writer;` into `writer/base_writer/mod.rs`.
- [x] Docs: GAP_MATRIX `Writer: position-delete` row note (writer landed, stays 🟡 pending RowDelta +
      interop); Roadmap Phase 2 (PositionDeleteWriter landed); this todo; lessons.
- [x] Verify gate from repo root.

**Deferred (flagged):** the optional `row` column (position-delete-with-row-data, field id 2147483544);
caller-side sorting by (file_path,pos) — 5b/`SortingPositionOnlyDeleteWriter`; `referenced_data_file`
single-file optimization (the read side already handles null); the RowDelta action (5b) + DV writer (5c);
Java interop round-trip (→ ✅). Row stays 🟡.

**Outcome (2026-06-08, Phase 2 Increment 5a, BUILDER Opus):** `PositionDeleteFileWriter` lands as the first
piece of the `RowDelta` merge-on-read write path (🟡). New file
`crates/iceberg/src/writer/base_writer/position_delete_writer.rs` mirroring `EqualityDeleteFileWriter`:
`PositionDeleteWriterConfig` (holds the position-delete `Schema` + Arrow schema; `pos_delete_schema()` free
fn) + `PositionDeleteFileWriterBuilder<B,L,F>` + `PositionDeleteFileWriter`. Writes a parquet position-delete
file whose schema is exactly `file_path: string` (field id **2147483546**) + `pos: long` (field id
**2147483545**) — reserved ids reused from `metadata_columns.rs` (`delete_file_path_field()` /
`delete_file_pos_field()`); **metadata_columns.rs NOT touched** (the constants already exist and already match
Java `MetadataColumns.DELETE_FILE_PATH`/`DELETE_FILE_POS`). `close()` stamps
`content(DataContentType::PositionDeletes)` + partition/spec + correct `record_count`. **Java-faithful
sorting: writes records AS GIVEN (no reorder); sorting by (file_path,pos) is the caller's / a later
`SortingPositionOnlyDeleteWriter`'s job — DOC'd in the module header + the spec recommendation.**
`write()` validates the input batch's Arrow schema against the position-delete schema and rejects a mismatch
(`ErrorKind::DataInvalid`). **6 tests** (all named for the risk): `pos_delete_schema_has_reserved_field_ids`
(schema/id contract), `round_trips_exact_positions` (dropped/mangled positions = data not deleted — reads
parquet back, asserts exact (path,pos) pairs in order), `written_field_ids_match_reserved` (parquet field ids
== 2147483546/2147483545 — interop), `multiple_data_files_one_delete_file`, `rejects_mismatched_schema`,
`empty_input_closes`. Mutation-verified: dropping the last row of each batch fails the round-trip + multi-file
+ field-id tests. **Verify (repo root, pinned nightly):** `cargo build -p iceberg` clean; lib ×2 = **1390/0**
both runs (was 1384 baseline → +6); interop `interop_manage_snapshots` 4/4, `interop_update_schema` 4/4,
`interop_update_partition_spec` 4/4 (all stay green); clippy -D warnings clean; fmt --check clean (one reflow
applied via `cargo fmt`). Files touched exactly the allowed set:
`writer/base_writer/position_delete_writer.rs` (new), `writer/base_writer/mod.rs` (one `pub mod` line),
`docs/parity/GAP_MATRIX.md`, `Roadmap.md`, `task/todo.md`, `task/lessons.md`. **NOT touched:**
`metadata_columns.rs` (constants already present — flagged), Cargo.toml/lockfiles. No commit. Row stays 🟡.
**Deferred:** optional `row` column (id 2147483544); caller-side sorting (5b); the `RowDelta` action (5b) +
DV writer (5c); Java interop round-trip (→ ✅). An Opus REVIEWER verifies next.

### Phase 2 Increment 5b — RowDelta action + producer delete-manifest support (IN PROGRESS, 2026-06-08, BUILDER Opus)
The merge-on-read write-commit core: add data files + add DELETE files (position/equality) in ONE snapshot,
writing the producer's first DELETE manifest alongside the DATA manifest. New file
`crates/iceberg/src/transaction/row_delta.rs`; producer delete-manifest support in
`crates/iceberg/src/transaction/snapshot.rs`; wired into `transaction/mod.rs`.

**Java rules verified against `/tmp/iceberg-java-ref` source:**
- `BaseRowDelta.operation()` is DYNAMIC: `APPEND` (adds data, no delete files, no data deletes), `DELETE`
  (adds delete files, no data files), else `OVERWRITE`. The add-deletes-only crown-jewel case → `DELETE`.
  I mirror Java's dynamic op (the OverwriteFiles reviewer established "align to Java, not the brief's hint";
  the brief's "it is OVERWRITE" is the both-add-data+add-deletes case). `update_snapshot_summaries` already
  admits Append/Delete/Overwrite — no summary-allowlist edit needed.
- `MergingSnapshotProducer.add(DeleteFile)` routes delete files into a DELETE manifest written via the
  producer's delete-manifest writer; added delete entries inherit the new snapshot's seq at read time
  (Added entries with no seq → inherited from the manifest-list entry's `added_snapshot_id`/seq), exactly
  like added data files. So a delete file added now applies to EARLIER data (data_seq <= delete_seq).

Plan:
- [x] A. Producer: add `added_delete_files: Vec<DataFile>` to `SnapshotProducer`; new ctor param (or a
      `with_added_delete_files`); `write_added_delete_manifest()` mirroring `write_added_manifest` but using
      `new_manifest_writer(ManifestContentType::Deletes)`; push it in `manifest_file()` when non-empty.
      Relax the empty-commit precondition to also count `added_delete_files`. Route added delete files
      through `summary()`'s `add_file` (delete-content branch already in `SnapshotSummaryCollector`).
- [x] B. `RowDeltaAction`: `add_data_files`/`add_deletes` + `set_commit_uuid`/`set_snapshot_properties`/
      `set_key_metadata`. Validate: data files = `Data` content (reuse `validate_added_data_files`); delete
      files = PositionDeletes/EqualityDeletes content (reject Data) + partition-spec match. Dynamic op via
      `RowDeltaOperation`. Wire `Transaction::row_delta()` + mod/use.
- [x] C. Summary: added data + added delete files + pos/eq delete counts via the collector (no
      snapshot_summary.rs edit — `add_file` already branches on content type). Flagged: NOT touched.
- [x] CROWN-JEWEL e2e: MemoryCatalog (LocalFsStorageFactory over tempdir) → create table → fast-append a
      real parquet data file with known rows → `PositionDeleteFileWriter` produces a pos-delete file pointing
      at specific rows → `row_delta().add_deletes([that file]).commit()` → SCAN → assert deleted rows ABSENT.
- [x] Plus tests: delete manifest written with content==Deletes + referenced in manifest list; add data +
      deletes in one RowDelta; add-deletes-only allowed; summary counts; added delete entry seq == new
      snapshot seq (applies to earlier data); reject Data content in add_deletes; partition-spec mismatch.
- [x] Docs: GAP_MATRIX RowDelta ❌→🟡 + position-delete writer note (end-to-end); Roadmap; this todo; lessons.
- [x] Verify gate from repo root.

**OUT OF SCOPE (deferred):** equality-delete WRITER end-to-end (focus crown-jewel on POSITION deletes);
concurrent-commit conflict validation (`validateFromSnapshot`/`validateNoConflictingDataFiles`/
`validateDeletedFiles`/`validateDataFilesExist`); the deletion-vector path (5c). RowDelta the COMMIT
primitive is the deliverable.

**Outcome (2026-06-08, Phase 2 Increment 5b, BUILDER Opus):** `RowDeltaAction` lands 🟡 — the merge-on-read
write commit. New file `transaction/row_delta.rs`; `SnapshotProducer` (snapshot.rs) gained delete-manifest
support (`added_delete_files` field + `with_added_delete_files` + `validate_added_delete_files` +
`write_added_delete_manifest` via the existing `Deletes` writer arm; empty-commit precondition relaxed to
count added delete files; added delete files routed through `summary()`'s `add_file`); wired
`Transaction::row_delta()` + mod/use. **Operation dynamic** (Java `BaseRowDelta.operation()`: Append/Delete/
Overwrite). **Added delete entries inherit the new snapshot's seq** (V2/V3 no-seq Added entries) so they
apply to earlier data. **THE CROWN-JEWEL** `test_row_delta_position_deletes_drop_deleted_rows_from_scan`:
real-FS MemoryCatalog → fast-append a real 5-row parquet data file → real position-delete file (5a writer) at
positions {1,3} → `row_delta().add_deletes(...).commit()` → SCAN returns {10,30,50}, the deleted {20,40}
ABSENT — the full write→read chain. **9 tests**; mutation-verified the crown jewel three ways (mangled
positions, skipped delete manifest, seq-0 inheritance → resurrection). **`snapshot_summary.rs` NOT touched**
(its `add_file` already branches on content type — flagged). **Verify (repo root, pinned nightly):** build
clean; lib ×2 = **1399/0** both runs (was 1390 → +9); interop manage_snapshots/update_schema/
update_partition_spec 4/4 each; clippy -D warnings clean; fmt --check clean (one reflow applied). Files
touched exactly the allowed set: `transaction/row_delta.rs` (new), `transaction/snapshot.rs`,
`transaction/mod.rs`, GAP_MATRIX, Roadmap, todo, lessons. **NOT touched:** `snapshot_summary.rs`,
Cargo.toml/lockfiles. No commit. **Deferred:** equality-delete WRITER e2e (crown jewel = POSITION deletes);
`removeRows`/`removeDeletes`; conflict validation (`validateFromSnapshot`/`validateNoConflictingDataFiles`/
`validateNoConflictingDeleteFiles`/`validateDeletedFiles`/`validateDataFilesExist`); DV path (5c); data-level
Java interop. An Opus REVIEWER verifies next.

### Phase 2 Increment 6 — Concurrent-commit conflict-validation FOUNDATION + ReplacePartitions `validateNoConflictingData` (BUILDER Opus, 2026-06-08)
The serializable-isolation safety layer: a `TransactionAction::validate` hook run in `do_commit` AFTER the
refresh/re-base and BEFORE re-apply, plus its first concrete check (`ReplacePartitions` rejecting a commit
when a CONCURRENT snapshot added data to a replaced partition). Architecturally significant + correctness-
critical. EFFORT=MEDIUM, DELEGATED.

**Java rules verified against source** (`/tmp/iceberg-java-ref`):
- `BaseReplacePartitions.validate(currentMetadata, parent)` (lines 88-110): IF `validateConflictingData`,
  call `validateAddedDataFiles(currentMetadata, startingSnapshotId, replacedPartitions, parent)` (partitioned)
  or with `Expressions.alwaysTrue()` (unpartitioned). Opt-in via `validateNoConflictingData()` (sets the flag)
  + `validateFromSnapshot(long)` (overrides the starting snapshot).
- `MergingSnapshotProducer.validateAddedDataFiles(base, startingSnapshotId, partitionSet, parent)`
  (lines 363-381): enumerate `addedDataFiles(...)`; if ANY conflict entry exists, throw `ValidationException`
  "Found conflicting files that can contain records matching partitions %s: %s".
- `addedDataFiles` (424-463) → `validationHistory` (913-953): `SnapshotUtil.ancestorsBetween(parent.snapshotId
  (), startingSnapshotId, base::snapshot)` walks the parent chain from `parent` (INCLUSIVE) back to
  `startingSnapshotId` (EXCLUSIVE). For each snapshot whose `operation ∈ {APPEND, OVERWRITE}`
  (`VALIDATE_ADDED_FILES_OPERATIONS`, line 73-74), collect its DATA manifests where `manifest.snapshotId() ==
  snapshot.snapshotId()` (manifests added BY that snapshot) + add the snapshot id to `newSnapshots`. Then
  filter entries: `is_added` (`ignoreDeleted().ignoreExisting()`) AND `entry.snapshotId() ∈ newSnapshots` AND
  (partitionSet.contains(specId, partition)). ⇒ Rust port: walk `current.metadata()` from current-snapshot
  back via `parent_snapshot_id` until `starting_snapshot_id` (exclusive); for each APPEND/OVERWRITE snapshot,
  load manifest list → DATA manifests added by that snapshot → entries with `status == Added` → collect
  `DataFile`s.
- `ValidationException` is non-retryable → Rust `Error::new(ErrorKind::DataInvalid, ...)` (retryable defaults
  to FALSE; the `backon` `.when(|e| e.retryable())` loop STOPS on a non-retryable error and propagates it,
  unlike a `CatalogCommitConflicts` (retryable=true) which retries).

Plan:
- [x] **A1 — starting-snapshot capture.** `Transaction` gains `starting_snapshot_id: Option<i64>`, set ONCE
      at `Transaction::new` = `table.metadata().current_snapshot_id()`, NEVER overwritten by the `do_commit`
      re-base (its own field, so the re-base that overwrites `self.table` does not lose the original head).
- [x] **A2 — validate hook.** Add a default-no-op `async fn validate(self: Arc<Self>, starting_snapshot_id:
      Option<i64>, current: &Table) -> Result<()> { Ok(()) }` to `TransactionAction` (existing actions need
      no change — they inherit the no-op).
- [x] **A3 — run the hook in do_commit.** AFTER the refresh/re-base, BEFORE the re-apply loop: for each
      action, `Arc::clone(action).validate(self.starting_snapshot_id, &current_table).await?`. `current_table`
      is the refreshed base, so `validate` can enumerate the concurrent snapshots (those `current_table` has
      that are newer than `starting_snapshot_id`). A validation failure returns non-retryable DataInvalid →
      the retry loop stops + the error propagates (Java's non-retryable `ValidationException`).
- [x] **A4 — added-files-since helper.** `SnapshotProducer::added_data_files_after(starting_snapshot_id)`
      (or a free fn in snapshot.rs) walks `table.metadata()` from current-snapshot back via
      `parent_snapshot_id` to `starting_snapshot_id` (exclusive); for each APPEND/OVERWRITE snapshot, load its
      manifest list → DATA manifests added BY it → `Added` entries → collect `DataFile`s. Reusable by future
      validations (OverwriteFiles, etc.). Mirror Java `addedDataFiles`/`validationHistory`/`ancestorsBetween`.
- [x] **B — ReplacePartitions opt-in.** `ReplacePartitionsAction::{validate_from_snapshot(i64),
      validate_no_conflicting_data()}` builder methods (store `validate_from_snapshot: Option<i64>` +
      `validate_no_conflicting_data: bool`). Impl `TransactionAction::validate` for it: IF
      `validate_no_conflicting_data`, effective-start = `self.validate_from_snapshot` else the tx-provided
      `starting_snapshot_id`; enumerate added files since it (A4 helper, scoped to the action's helper-need);
      if ANY added file's `(spec_id, partition)` ∈ the action's replaced-partition set → non-retryable
      `DataInvalid` ValidationException naming the conflicting partition. Default (no opt-in) = NO validation.
- [x] **Tests (replace_partitions.rs, MemoryCatalog, REAL concurrent commit):**
      - KEY: append S0 (x=0,x=1) → build `replace_partitions(x=0).validate_from_snapshot(S0)
        .validate_no_conflicting_data()` → CONCURRENT `fast_append` adding data to x=0 (S1) → commit the
        replace → FAILS with the validation error, NON-RETRYABLE (assert `!err.retryable()` + the validation
        message, not a retry-exhaustion).
      - NEGATIVE control: concurrent append targets x=1 → replace(x=0) validation PASSES + commits.
      - OFF control: no opt-in → concurrent x=0 append → commit SUCCEEDS (default behavior unchanged).
      Each named for the risk (silently clobbering concurrent data = serializable-isolation violation).
- [x] Docs: GAP_MATRIX (conflict-validation foundation + ReplacePartitions `validateNoConflictingData`
      landed; the `Multi-op transactions + optimistic-concurrency` row + the `ReplacePartitions` row);
      Roadmap (Phase 2 increment 6); record the conflict-validation SUB-SEQUENCE in this todo (6 foundation+
      ReplacePartitions [this]; then OverwriteFiles `validateNoConflictingData`/`...Deletes`; RowDelta/
      DeleteFiles `validateDataFilesExist`; RewriteFiles `validateNoNewDeletes`); lessons.
- [x] Verify (repo root): `cargo build -p iceberg`; `cargo test -p iceberg --lib` ×2 (counts); the 3 interop
      tests; `cargo clippy -p iceberg --all-targets -- -D warnings`; `cargo fmt --all -- --check`.

**Conflict-validation sub-sequence (recorded):** (6) foundation + `ReplacePartitions.validateNoConflictingData`
[THIS increment]; then `OverwriteFiles` `validateNoConflictingData` / `validateNoConflictingDeletes`;
`RowDelta` / `DeleteFiles` `validateDataFilesExist`; `RewriteFiles` `validateNoNewDeletes`. Each reuses the
A4 added-files-since helper (+ siblings: deleted-files-since, added-delete-files-since) and the A2/A3 hook.

**Outcome (2026-06-08, Phase 2 Increment 6, BUILDER Opus):** the concurrent-commit conflict-validation
FOUNDATION + its first concrete check landed 🟡. **Foundation (3 production files):** (A1) `Transaction`
gained `starting_snapshot_id: Option<i64>`, captured ONCE in `new()` = `table.metadata().current_snapshot_id()`
as its OWN field so it SURVIVES the `do_commit` staleness re-base (which overwrites `self.table`). (A2)
`TransactionAction` gained a default-no-op `async fn validate(self: Arc<Self>, starting_snapshot_id:
Option<i64>, current: &Table) -> Result<()> { Ok(()) }` — every existing action inherits the no-op, zero
change. (A3) `do_commit` runs `Arc::clone(action).validate(self.starting_snapshot_id, &current_table)` for
each action AFTER the refresh/re-base and BEFORE the re-apply loop (so `current_table` is the refreshed base
= Java `parent`); a validation failure is a non-retryable `DataInvalid` (the `backon` `.when(|e|
e.retryable())` loop STOPS → it propagates, matching Java's non-retryable `ValidationException`). (A4) shared
free fn `added_data_files_after(table, starting_snapshot_id)` in `transaction/snapshot.rs` walks the current
snapshot's parent chain back to `starting_snapshot_id` (EXCLUSIVE), and for each APPEND/OVERWRITE snapshot
(Java `VALIDATE_ADDED_FILES_OPERATIONS`) collects every `Added`-status entry from the DATA manifests that
snapshot ADDED (`added_snapshot_id == snapshot_id`) — Java `MergingSnapshotProducer.addedDataFiles` /
`validationHistory` / `SnapshotUtil.ancestorsBetween`. **ReplacePartitions opt-in (B):**
`validate_no_conflicting_data()` (enable) + `validate_from_snapshot(id)` (override start) builder methods;
`impl TransactionAction::validate` for `ReplacePartitionsAction` — when enabled, effective-start =
`validate_from_snapshot` else the tx-provided id; enumerate added files via the A4 helper; reject (non-retryable
`DataInvalid` naming the conflicting partition + file) if any added file's `(spec_id, partition)` ∈ the
replaced-partition set (Java `BaseReplacePartitions.validate` → `validateAddedDataFiles`). Default (no opt-in)
= snapshot isolation, unchanged. **Tests (3, real concurrent commit via a separate `fast_append` between
build and commit):** KEY — conflict in the replaced partition x=0 → REJECTED (asserts `!retryable()` + the
"conflicting files" message + the catalog head is unchanged); NEGATIVE control — concurrent append to the
untouched x=1 → PASSES; OFF control — no opt-in → concurrent x=0 append → commit SUCCEEDS (default unchanged,
the concurrent file clobbered per snapshot isolation). Each names the serializable-isolation risk.
Mutation-verified BOTH directions: `validate`→always-`Ok` fails the KEY test (replace wrongly commits over
the concurrent file); conflict-predicate→always-`true` fails the NEGATIVE control (disjoint concurrent write
wrongly rejected). **Verify (repo root, pinned nightly):** `cargo build -p iceberg` clean; `cargo test -p
iceberg --lib` ×2 = 1404/0 both runs (+3 new tests); `interop_manage_snapshots`/`interop_update_schema`/
`interop_update_partition_spec` all 4/4; `cargo clippy -p iceberg --all-targets -- -D warnings` clean (fixed
one `map_clone` → `.cloned()`); `cargo fmt --all -- --check` clean (one reflow applied). Files touched exactly
the allowed set: `transaction/mod.rs` (field + capture + hook call), `transaction/action.rs` (default validate
hook), `transaction/replace_partitions.rs` (opt-in API + validate impl + 3 tests), `transaction/snapshot.rs`
(`added_data_files_after` + `operation_adds_data_files` helpers), GAP_MATRIX, Roadmap, todo, lessons. NO
Cargo/lockfile edits; no other action's file touched (the inherited no-op `validate` needs none); no
`#[ignore]`; no bare `.unwrap()` in production paths; no commit. Conflict-validation sub-sequence recorded
above. An Opus REVIEWER verifies next.

#### Phase 2 Increment 6 — REVIEW (2026-06-08, Opus REVIEWER, DELEGATED)
Adversarially verified points 1–6 against the Java source (`/tmp/iceberg-java-ref`) + mutation tests.
- [x] **Pt 1 (NON-retryable, infinite-loop risk): CONFIRMED.** The conflict error is
      `Error::new(DataInvalid, ...)` (retryable defaults to `false`; only `CatalogCommitConflicts` is
      `with_retryable(true)`), so `commit()`'s `backon` `.when(|e| e.retryable())` STOPS and propagates it.
      Mutation: adding `.with_retryable(true)` to the validation error made the KEY test fail at
      `assert!(!err.retryable())` AND took 1.56s (the loop re-refreshed to the same S1 + re-failed +
      exhausted 4 retries) — exactly the loop the brief warns about. The `!retryable()` + message asserts
      catch it cleanly.
- [x] **Pt 2 (starting_snapshot_id SURVIVES re-base): CONFIRMED + TEST GAP FOUND & FIXED.** Captured once in
      `Transaction::new` as its own field; `do_commit`'s `self.table = refreshed` does NOT touch it.
      Mutation (`effective_start = current.metadata().current_snapshot_id()`, the re-based head) makes the
      check always-pass. **GAP:** the builder's 3 tests all pinned `.validate_from_snapshot(S0)`, which
      short-circuits the tx field — so capturing `None` / re-reading the head failed NOTHING. Added
      `test_..._using_tx_captured_starting_snapshot` (no override): the head-mutation now fails EXACTLY it
      (1 pass override KEY / 1 fail new). This is the #2 highest risk, now pinned.
- [x] **Pt 3 (added-files walk + carried-forward exclusion): CONFIRMED + isolated test added.** The
      `added_snapshot_id == snapshot_id` manifest filter + `Added`-status entry filter + APPEND/OVERWRITE
      operation filter match Java `addedDataFiles`/`validationHistory`/`ancestorsBetween`. Probe (S0 has
      x=0 a + x=1 b; concurrent S1 adds x=0 a_new): `added_data_files_after(table, S0)` returns EXACTLY
      `{a_new}` — carried-forward a/b excluded. Kept as
      `test_added_data_files_after_excludes_carried_forward_manifests`.
- [x] **Pt 4 (real race + conflict predicate): CONFIRMED.** Tests use a genuine concurrent `fast_append` on
      the same `MemoryCatalog` between build and commit (refresh→S1→validate), not a faked mismatch.
      Mutation BOTH directions: predicate→always-`true` fails the NEGATIVE control (disjoint x=1 append
      wrongly rejected); predicate→always-`false` fails the KEY test (conflict undetected). Default (no
      opt-in) commits over the concurrent file (OFF control) = snapshot isolation, unchanged.
- [x] **Pt 5 (edge cases): SANE, one divergence flagged.** `starting_snapshot_id == None` ⇒ walk to root
      (validate from history start). No parent / missing parent id ⇒ `break` (no panic). Empty table (no
      current snapshot) ⇒ empty (no panic) — pinned by `test_added_data_files_after_empty_table_yields_empty`.
      **DIVERGENCE:** a non-ancestor `validate_from_snapshot` OVER-SCANS to root where Java fails loud
      (`validationHistory`'s `lastSnapshot.parentId() == start` check). Rust-STRICTER (can only over-reject,
      never miss a conflict) → SAFE; documented + pinned
      (`test_added_data_files_after_nonancestor_start_overscans_does_not_panic`), tracked as a parity
      follow-up (add the post-walk ancestor guard), NOT fixed (narrow misuse, errs strict).
- [x] **Pt 6 (default unchanged + scope): CONFIRMED.** Only `ReplacePartitionsAction` overrides `validate`;
      every other action inherits the no-op (grep-verified). No bare `.unwrap()` in the new code (all `?`).
      No Cargo/lockfile edits. Files touched = exactly the named set + this todo/lessons. Full lib suite
      1408/0 ×2 (was 1404 builder baseline; +4 reviewer tests); 3 interop suites 4/4; clippy `-D warnings`
      clean; fmt clean.

**Review outcome (2026-06-08, Opus REVIEWER):** all 6 points adjudicated; the two highest risks
(non-retryable, survives-rebase) hold — survives-rebase had a real TEST GAP (now fixed with a tx-field
test, mutation-verified as the unique guard). No production bug. +4 tests (tx-captured-start,
carried-forward isolation, non-ancestor over-scan, empty-table), all mutation-/probe-verified. ONE
flagged divergence (non-ancestor over-scan vs Java fail-loud, Rust-stricter, tracked). Files touched:
`transaction/replace_partitions.rs` (+4 tests), todo, lessons. No production `.rs` logic changed, no Cargo
edits, no commit. Rows stay 🟡.

**Follow-up (tracked):** add Java `validationHistory`'s post-walk `lastSnapshot.parentId() == start` guard
to `added_data_files_after` so a non-ancestor `validate_from_snapshot` fails loud (parity) rather than
over-scanning — fold into the conflict-validation sub-sequence hardening.

---

## Active (2026-06-09): DeleteFiles validateFilesExist (validateDataFilesExist for the delete path)

Increment: Java `MergingSnapshotProducer.validateDataFilesExist` semantics for the DeleteFiles action —
reject the commit if any data file this op is deleting was DELETED by a concurrent commit since the start.

- [x] **snapshot.rs status axis** — generalize `added_files_after`'s hard `status() == Added` filter into a
  `status_to_keep: ManifestStatus` parameter. The two existing callers (`added_data_files_after`,
  `added_delete_files_after`) pass `ManifestStatus::Added` (BEHAVIOR-PRESERVING — their tests stay green).
- [x] **snapshot.rs `operation_removes_data_files`** — `{Overwrite, Replace, Delete}` (Java
  `VALIDATE_DATA_FILES_EXIST_OPERATIONS`). Note: Rust `Operation` has no `Replace` variant → only
  `{Overwrite, Delete}` are representable; document.
- [x] **snapshot.rs `deleted_data_files_after(table, start) -> Vec<DataFile>`** — DATA content,
  `operation_removes_data_files`, `ManifestStatus::Deleted`. The concurrent delete's rewritten manifest
  carries the Deleted tombstone with `added_snapshot_id == that snapshot` (verified in
  `rewrite_manifest_with_deletes`), so the existing manifest filter finds it.
- [x] **delete_files.rs** — add `validate_files_exist: bool` + `validate_from_snapshot: Option<i64>` fields +
  `validate_files_exist()` / `validate_from_snapshot(i64)` builders + a `validate` override (mirror
  ReplacePartitions): if OFF ⇒ Ok; effective_start = override.or(start); enumerate
  `deleted_data_files_after`; if any deleted file's path ∈ self.delete_paths ⇒ non-retryable
  `Error::new(DataInvalid, "Cannot commit, missing data files: <path>")`.
- [x] **Tests (5)** + mutation checks (a–d) + behavior-preservation of the two Added callers.
- [x] **Docs** — GAP_MATRIX, Roadmap, lessons; note skip-deletes op-set variant + RowDelta
  validateDataFilesExist deferred.

**Java divergence flagged up front:** `StreamingDelete.validate()` actually calls `failMissingDeletePaths()`
(the filter-manager required-deletes check), NOT `validateDataFilesExist`. The brief directs the
`validateDataFilesExist`-semantics port for the delete path (requiredDataFiles = the files being deleted),
modeled on RowDelta/ReplacePartitions. Faithful to `validateDataFilesExist`'s contract; report the
StreamingDelete wiring nuance.

**Outcome (2026-06-09):** Landed. `transaction/snapshot.rs` (status axis `files_after` +
`operation_removes_data_files` + `deleted_data_files_after`) + `transaction/delete_files.rs`
(`validate_files_exist()` / `validate_from_snapshot()` + `validate` override + 5 tests). Lib total
1573 → 1578. transaction:: 246 green (the two Added callers behavior-preserving). 4 mutations caught.
Docs updated (GAP_MATRIX, Roadmap, lessons). Deferred: skip-deletes op-set variant + RowDelta
validateDataFilesExist.

**REVIEW (2026-06-09, Opus):** Verified behavior-preservation (inverting the shared manifest filter fails 17
tests across BOTH the Deleted and Added axes), the deleted-file enumeration vs Java (content DATA, op
`{Overwrite, Delete}`, status Deleted — op set PINNED by a real `Delete`-op concurrent deletion), end-to-end
through `tx.commit` + non-retryable. Ran 8 mutations (the builder's 4 + content-type/manifest-filter/intersection/
tx-captured-fallback); ALL caught after a fix. **Found + fixed 1 SURVIVING mutation:** the tx-captured
`starting_snapshot_id` fallback (no `validate_from_snapshot`) was unpinned (the recurring Increment-6 gap — all
5 builder tests set the override). Added `test_delete_files_exist_rejects_concurrent_using_tx_captured_starting_snapshot`
(reviewer); the refreshed-head mutation now fails exactly it. Lib total 1578 → **1579**. Docs reconciled (test
count 5→6, mutation list 4→8).

---

## Active (2026-06-09): RowDelta validateDataFilesExist + the skip-deletes op-set variant

Increment: Java `BaseRowDelta.validateDataFilesExist(referencedFiles)` — RowDelta gains a builder providing
the data files its added position-deletes REFERENCE, and at commit rejects if any referenced data file was
DELETED by a concurrent commit since the start. Reuses `deleted_data_files_after`; ADDS the skip-deletes
op-set variant (Java `VALIDATE_DATA_FILES_EXIST_SKIP_DELETE_OPERATIONS = {OVERWRITE, REPLACE}`).

- [x] **snapshot.rs `skip_deletes` op-set variant** — `deleted_data_files_after(table, start, skip_deletes:
  bool)`: `skip_deletes=true` ⇒ `{Overwrite}` (new `operation_removes_data_files_skip_deletes`; Java drops
  DELETE; REPLACE unrepresentable); `skip_deletes=false` ⇒ `{Overwrite, Delete}`. The existing DeleteFiles
  caller passes `false` (BEHAVIOR-PRESERVING — proven: forcing it to `true` fails 3 DeleteFiles tests).
- [x] **row_delta.rs** — added `referenced_data_files: HashSet<String>` + `validate_deleted_files: bool`
  fields + `validate_data_files_exist(...)` + `validate_deleted_files()` builders + the `validate`-override
  branch (`skip_deletes = !validate_deleted_files`; intersection of `deleted_data_files_after` paths ∩
  `referenced_data_files` ⇒ non-retryable `DataInvalid` "Cannot commit, missing data files: <path>").
- [x] **Tests (6)** + mutation checks (a–d, all caught) + the DeleteFiles behavior-preservation mutation.
- [x] **Docs** — GAP_MATRIX, Roadmap, lessons updated.

**Faithfulness note:** Java `referencedDataFiles` is CALLER-PROVIDED (`CharSequenceSet`, populated by
`validateDataFilesExist(referencedFiles)`), NOT derived from the added delete files. The Rust port mirrors
this — `validate_data_files_exist([paths])` takes the caller's set.

**Outcome (2026-06-09):** Landed. `transaction/snapshot.rs` (skip-deletes op-set axis on
`deleted_data_files_after` + `operation_removes_data_files_skip_deletes`) +
`transaction/row_delta.rs` (`validate_data_files_exist` / `validate_deleted_files` + the `validate` branch +
6 tests) + `transaction/delete_files.rs` (caller passes `skip_deletes = false`). transaction:: 247 → 253
green (the DeleteFiles increment-1 + data/delete conflict tests behavior-preserving). 4 mutations (a–d)
caught + the DeleteFiles `skip_deletes=true` mutation caught. Deferred: `validateNoNewDeletesForDataFiles`,
`validateAddedDVs` (need `removed_data_files`), `OverwriteFiles.validateDataFilesExist`.

---

## DONE (2026-06-09): Phase 2 WRITE-VALIDATION remnants cluster — MERGED as `5ed1582b` (#9) (branch `phase2-write-validation` off main `5f32b10d`)

User chose "write-validation remnants" as next. Sequence (each = builder→reviewer Workflow; orchestrator
independently re-runs the gate + commits; production, unit-tested via MemoryCatalog concurrent-commit tests,
NO interop needed):

- [x] **Increment 1: `OverwriteFiles.validateNoConflictingDeletes` + shared `validate_no_new_deletes_for_data_files`**
  (snapshot.rs) — the genuinely-missing core (per-removed-data-file delete-applicability, Java
  `MergingSnapshotProducer.validateNoNewDeletesForDataFiles`). NEW seq-preserving walk
  `added_delete_files_with_seq_after` + `starting_sequence_number` + `delete_applies_to_data_file` (ports Java
  `DeleteFileIndex.forDataFile`; INCLUSIVE `>=` boundary; one documented conservative equality-delete
  over-approximation). 8 MemoryCatalog tests incl. the tx-captured-start pin; mutation-verified. Gate green
  (lib 1603, transaction:: 261, datafusion 80+9, clippy/fmt/typos). `validate_no_conflicting_data`
  behavior-preserving + independent flag.
- [x] **Increment 2: `RowDelta.validateNoNewDeletesForDataFiles`** — added `remove_data_files`/`remove_rows`
  (VALIDATION-ONLY removal — apply-side drop deferred + documented; callers → `overwrite_files().delete_data_files`)
  + wired the shared helper under the existing `validate_no_conflicting_delete_files()` flag, 2a before 2b
  (Java order). 9 MemoryCatalog tests incl. the tx-captured-start pin; helper reused UNCHANGED; gate green
  (lib 1612, transaction:: 270). `validateAddedDVs` SPLIT OUT → Increment 2b below.
- [ ] **Increment 2b: `RowDelta.validateAddedDVs`** (V3 deletion-vector conflict) — identify added DVs
  (position-delete files with `referenced_data_file` + content_offset = puffin DV), index by referenced file,
  reject a concurrently-added DV for the same referenced data file. Java `MergingSnapshotProducer.validateAddedDVs`
  (L825-895).
- [ ] **Increment 3: `OverwriteFiles.overwriteByRowFilter` + `validateAddedFilesMatchOverwriteFilter`** — the
  row-filter overwrite mode (delete-by-row-filter) + strict/inclusive/metrics validation of added files
  (StrictMetricsEvaluator + inclusive/strict partition Projections).
- [x] **Increment 3: `ReplacePartitions.validateNoConflictingDeletes`** (completes the validateNoConflictingDeletes
  pair) — PARTITION-SET-based (NOT the per-data-file helper): `validateNoNewDeleteFiles(replacedPartitions)` +
  `validateDeletedDataFiles(replacedPartitions)` reusing `added_delete_files_after` + `deleted_data_files_after`
  + the SINGLE existing replaced-partition predicate (`file_in_replaced_partition`). op-sets read from the Java
  constants; skip_deletes=false. 8 MemoryCatalog tests incl. tx-captured pin + non-replaced-partition controls;
  mutation-verified. Gate green (lib 1620, transaction:: 278). NO snapshot.rs churn.

Remaining in the cluster:
- [ ] **Increment 4: `OverwriteFiles.overwriteByRowFilter` + `validateAddedFilesMatchOverwriteFilter`** — the
  row-filter overwrite mode (delete-by-row-filter at commit) + strict/inclusive/metrics validation of added
  files (StrictMetricsEvaluator + inclusive/strict partition Projections). The meatiest one (a new mode).
- [x] **Increment 4: `RowDelta.validateAddedDVs`** (V3 deletion-vector conflict) — ALWAYS-ON (self-skips when
  no DVs added); DV = Puffin-format delete file (Java `ContentFileUtil.isDV`), indexed by `referenced_data_file`;
  rejects a concurrently-added DV for the same referenced file. Reuses `added_delete_files_after` filtered to
  Puffin DVs. Behavior-preserving (48 row_delta tests green; non-DV no-op pinned). 6 tests + tx-captured pin;
  mutation-verified. Gate green (lib 1626, transaction:: 284). Cosmetic: dvDesc Option Debug-format (noted).

- [x] **Increment 5: `OverwriteFiles.overwriteByRowFilter` + `validateAddedFilesMatchOverwriteFilter`** (the
  meatiest) — NEW delete-by-EXPRESSION overwrite mode via `resolve_filter_deletes` (snapshot.rs) feeding
  `process_deletes`; ports Java `ManifestFilterManager`/`PartitionAndMetricsEvaluator` by composing the REAL
  `ResidualEvaluator` + `Strict`/`InclusiveMetricsEvaluator` on the per-file RESIDUAL (the builder CORRECTED my
  prompt's full-predicate suggestion — residual is what Java does + avoids spurious partial-errors). KEEP /
  DELETE / PARTIAL-error decision tree. `validate_added_files_match_overwrite_filter()` (block 1) +
  conflict-filter default now follows the row filter (Java `dataConflictDetectionFilter`). 11 tests incl. the
  partial-match error + the row-filter-conflict-default pair; mutation-verified. Gate green (lib 1636,
  transaction:: 294). Conservative postures documented (failAnyDelete / duplicate-path / DV-manifest branches
  not ported — delete-manifest-specific). Deferred: the rowFilter branch of validateNoConflictingDeletes.

**CLUSTER COMPLETE (5 increments). MERGED as `5ed1582b` "Phase2 write validation (#9)".**

<!-- Archived by todo-archival pass 2, 2026-06-11 (verbatim moves; see skills/compaction.md §Todo Archival). -->

## ACTIVE (2026-06-10/11 overnight): autonomous 8-hour plan (user asleep, auto mode)

User instruction: complete the DV sequence, then plan + execute ~8 hours autonomously. **Tier
decision (documented):** the Fable-subagent authorization was scoped to the DV sequence (now
complete); overnight arcs revert to the STANDING default — Opus builder → Opus reviewer (the
parity-orchestration feedback memory + CLAUDE.md subagent policy; budget-prudent while the user
cannot approve frontier spend). Orchestrator (Fable) preps, briefs, gates, commits, pushes;
merges NOTHING; compaction passes NOT run (interactive-only — triggers have fired, re-flagged
for morning).

- [ ] **Arc E — DV previous-deletes merge + superseded-delete removal** (branch `phase2/dv-merge`
      STACKED on `phase2/dv-writer`; if the user squash-merges dv-writer in the morning, rebase
      `--onto origin/main` per the standing instruction). The deferred half of Java's DV surface:
      the DELETE-manifest filter machinery (the `deleteFilterManager` sibling of `process_deletes`
      — rewrite DELETE manifests tombstoning superseded delete files), RowDelta `removeDeletes`
      apply-side, `DVFileWriter` `loadPreviousDeletes` merge hook, then LIFT the fresh-DV door.
      Completes the DV story; `removed-dvs` becomes reachable end-to-end. 1-2 increments.
  - [ ] **Arc-E Increment 1 — apply-side DELETE-FILE removal (BUILDER Opus, 2026-06-10).** The
        `deleteFilterManager` sibling of the data-manifest filtering + `RowDelta::remove_deletes`
        + the fresh-DV door relaxation. (The DVFileWriter merge hook + interop = NEXT increment.)
        Plan:
        1. **Producer (`snapshot.rs`):** add `removed_delete_files: Vec<DataFile>` + builder setter
           `with_removed_delete_files` (mirroring `with_added_delete_files`). In `commit()`, resolve
           them against the CURRENT snapshot's DELETE manifests BY PATH (new
           `resolve_delete_file_paths`, missing path → "Missing required files to delete: %s"). Feed
           them to the SAME `process_deletes` rewrite path (matched by path) AND to the summary's
           `remove_file` (DV → removed-dvs, parquet pos → removed-pos-delete-files, eq → removed-eq).
           Extend `new_filtering_manifest_writer` to build a DELETE-content writer when the SOURCE
           manifest is a DELETE manifest (`build_v2_deletes`/`build_v3_deletes`) — keyed off
           `source_manifest.content`. The DATA-side behavior must be byte-identical (its tests prove it).
        2. **RowDelta surface (`row_delta.rs`):** `remove_deletes(DataFile)` + `remove_deletes_many`,
           rejecting DATA-content files (Java `delete(DeleteFile)` is delete-content-only). Wire to
           the producer via `with_removed_delete_files` + `RowDeltaOperation::delete_files` returning
           removed delete files (so they flow through `process_deletes`).
        3. **Door relaxation (`validate_fresh_dvs_only`):** a DV add for a referenced file with an
           EXISTING live DV (or shadowed legacy-parquet delete) is legal IFF that existing delete's
           path is in this commit's removed set. The D3 door tests stay green (add-DV-without-removal
           still rejected); the removed-set escape hatch is purely additive.
        4. **OPERATION CLASSIFICATION FIX (the 2026-06-08 lesson's third condition — TODAY).**
           BYTECODE FINDING (1.10.0 jar): `BaseRowDelta.operation()` is TWO-branch — `addsDeleteFiles
           && !addsDataFiles → DELETE; else → OVERWRITE`. NO APPEND branch (MAIN's 3-branch
           `addsDataFiles && !addsDeleteFiles && !deletesDataFiles → APPEND` is POST-1.10.0). The
           interop oracle pins 1.10.0, so the faithful classification is the 1.10.0 two-branch form.
           This SUPERSEDES the current Rust 3-branch form + flips `test_row_delta_add_data_only_
           records_append` (add-data-only RowDelta → Overwrite per 1.10.0, not Append). `removeRows`
           feeds `deletesDataFiles`, `removeDeletes` feeds `deletesDeleteFiles` — NEITHER is used by
           1.10.0 `operation()`, so the two-branch form handles all removal cases correctly.
        5. **CROWN JEWEL:** V3 → parquet → DV1 {1} committed → scan → DV2 {1,3} (hand-merged) →
           `row_delta().add_deletes(dv2).remove_deletes(dv1)` → scan = survivors of {1,3}; old DV
           tombstoned (raw-avro provenance: tombstone carries new snapshot id, survivors keep
           original); summary `removed-dvs: 1` + `added-dvs: 1`; manifest list holds exactly ONE live
           DV. Mutations: (a) skip removal + door off → scan rejects at load door (two DVs); (b)
           survivors re-stamped (`add_existing_entry`→`add_entry`) → provenance pin fails.
        6. **Other tests:** remove parquet pos delete (V2) e2e; remove eq delete; missing-removal-path
           error; remove-only commit (operation classification per 1.10.0); door-relaxation pair;
           provenance pin on rewritten delete manifest; cumulative totals append→row_delta(DV)→
           row_delta(replace DV). NO new concurrent-window validation added (removal reuses the
           existing `resolve` + `process_deletes` path; the tx-captured-start pin is N/A — say so).
        7. **Docs:** GAP_MATRIX (DV-writer + RowDelta rows), transaction/map.md, this outcome.
        BUILDER OUTCOME (2026-06-10, Arc-E Inc 1 — Opus; awaiting reviewer): **CROWN JEWEL GREEN ON
        FIRST RUN — all-Rust DV-replaces-DV closed the merge-and-replace loop.** Producer gained
        `removed_delete_files` + `with_removed_delete_files` + `resolve_delete_file_paths` (the
        by-path DELETE-manifest sibling of `resolve_delete_paths`, missing-path → Java
        `failMissingDeletePaths` shape); removed delete files flow through the SAME `process_deletes`
        rewrite (matched by path across the full manifest list) + the summary's `remove_file` (DV →
        `removed-dvs`, parquet pos → `removed-position-delete-files`, eq → `removed-equality-delete-
        files`). `new_filtering_manifest_writer` now CONTENT-KEYED off the source manifest
        (`build_v2/v3_deletes` for a DELETE source) — the LOUD change; data-side stays byte-identical
        (`build_v2/v3_data`, its existing rewrite tests + a new explicit pin prove it). RowDelta:
        `remove_deletes(DeleteFile)` + `remove_deletes_many` (reject Data content), wired via
        `with_removed_delete_files`; the fresh-DV door gained the `remove_deletes` escape hatch (a DV
        may shadow a live delete IFF it is removed in the same commit — Java's merge-and-replace).
        **OPERATION CLASSIFICATION FIX (the 2026-06-08 lesson's third condition, TODAY):** read the
        1.10.0 JAR BYTECODE of `BaseRowDelta.operation()` — it is the TWO-branch `addsDeleteFiles &&
        !addsDataFiles ⇒ DELETE; else ⇒ OVERWRITE` with NO APPEND branch (MAIN's append arm + its
        `!deletesDataFiles()` guard are POST-1.10.0). The interop oracle pins 1.10.0, so the faithful
        fix DROPS to the two-branch form (not "add MAIN's third condition") — re-classifying
        add-data-only RowDelta as Overwrite (was Append); flipped `test_row_delta_add_data_only_
        records_append` → `..._records_overwrite_per_1_10_0`. 11 new tests (lib 1756→1767): crown
        jewel (DV-replaces-DV: one live DV, old tombstoned, removed-dvs:1+added-dvs:1, raw-avro
        provenance) + door pair (with-removal commits / without-removal still rejected) + remove
        parquet pos delete e2e + remove eq delete + missing-removal-path message + remove-only op
        classification + remove-Data-content rejection + survivor-provenance pin + cumulative totals +
        data-side-unchanged regression guard. 5 mutations (snapshot to /tmp, restored byte-clean, full
        suite re-run): (1) escape-hatch disabled ⇒ exactly the 3 removal-commit tests; (2) survivor
        `add_existing_entry`→`add_entry` ⇒ my provenance pin + 6 data-side provenance tests (shared
        helper covered from EVERY consumer); (3) content-keyed writer→always-data ⇒ exactly the 7
        delete-removal tests (data-side test stays green ⇒ byte-identical); (4) re-add APPEND branch ⇒
        exactly the operation pin; (5) missing-path validation off ⇒ exactly the missing-removal test.
        NO new concurrent-window validation added (removal reuses resolve + process_deletes); the
        tx-captured-start pin is N/A — stated. `snapshot_summary.rs` NOT touched (D3 already wired the
        `remove_file` DV branch — no counter gap). Gate: typos/fmt/clippy(workspace excl. sqllogictest)
        clean; lib 1767 ×2; datafusion 80+9 (the documented pre-existing rt-multi-thread doctest
        artifact — unrelated, fails on clean tree); `run-interop-dv.sh` GREEN end-to-end (D1-D4 surface
        intact). Files: only transaction/{snapshot,row_delta}.rs + transaction/map.md + GAP_MATRIX +
        todo. DEFERRED LOUDLY (next increment): the WRITER-side `loadPreviousDeletes` auto-merge (the
        test HAND-merges the super-set DV); apply-side `removeRows` data removal still validation-only;
        interop for the removal path.
  - [x] **Arc-E Increment 2 — DVFileWriter previous-deletes MERGE hook + DV-replacement interop
        (BUILDER Opus, 2026-06-10).** DONE — see BUILDER OUTCOME below (DV-writer row flipped ✅).
        Completes Java's DV write surface — the WRITER-side
        `BaseDVFileWriter.loadPreviousDeletes` half E1 deferred. After this the GAP_MATRIX DV-writer
        row is judged for ✅. Plan:
        1. **The merge hook (`deletion_vector_writer.rs`):** `DVFileWriter::with_previous_deletes(...)`
           — a Rust-pragmatic mirror of Java's ctor `loadPreviousDeletes: Function<String,
           PositionDeleteIndex>`. Per referenced data-file path it carries the previous positions
           (`DeleteVector`) + the SOURCE delete `DataFile`(s) they came from (Java
           `PositionDeleteIndex.deleteFiles()`). On `close()`: union previous positions into the new
           DV for that path (record_count/cardinality = MERGED set); previous source files that are
           FILE-SCOPED (`is_file_scoped` — Java `ContentFileUtil.isFileScoped` = `referencedDataFile
           != null`, BYTECODE-verified: DV OR path-scoped position delete, NOT equality, NOT
           partition-scoped) are returned as `rewritten_delete_files` (a `DeleteWriteResult`-shaped
           return — `DVWriteResult { delete_files, rewritten_delete_files }`, mirroring Java's
           `DeleteWriteResult(dvs, referencedDataFiles, rewrittenDeleteFiles)`). Files NOT file-scoped
           are NOT rewritten (Java L121-124). No-previous case BYTE-IDENTICAL to today (D2/D4 pins are
           the floor). `is_file_scoped`: REUSE `is_deletion_vector` (it lives in `delete_file_index.rs`
           — NOT in scope; implement a small `is_file_scoped` predicate IN `deletion_vector_writer.rs`
           via the public `referenced_data_file()` accessor, NOT forking `is_deletion_vector`).
        2. **Merge support (`delete_vector.rs` if needed):** a positions-out accessor / union helper.
           `BitOrAssign` already exists; a `clone`/`from_iter` from positions may be needed. The
           serializer itself does NOT change.
        3. **Crown jewel (`row_delta.rs` tests):** V3 → data file → DV1 {1} committed → load DV1's
           positions back via the PRODUCTION read path (`CachingDeleteFileLoader`/decoder, NOT a
           hand-built vector) → feed as previous-deletes to a new DVFileWriter writing position {3} →
           writer outputs merged DV {1,3} + rewritten=[dv1] → `row_delta().add_deletes(dv2)
           .remove_deletes(rewritten...)` commits (E1's escape hatch unlocks) → scan = survivors of
           {1,3} = {10,30,50}. Mirrors the REAL engine flow (Spark `SparkPositionDeltaWrite`
           L251+L255-256: `addDeletes(dv)` + `for rewritten: removeDeletes(file)`).
        4. **The Run-store re-serialization question (the D2 caveat, now LIVE):** the previous DV
           deserializes into Run containers; after merge we re-serialize. Determine EMPIRICALLY whether
           the merged blob byte-matches Java's merged blob (extend Direction-2 byte-compare — the
           oracle does the SAME merge in Java via `BaseDVFileWriter` + a `loadPreviousDeletes` fn). If
           the tie diverges, document precisely + scope the byte claim (positions identical, bytes may
           differ at the documented tie); Java reads our blob either way (the oracle proves it). Do NOT
           contort production code to chase the tie.
        5. **Interop (`run-interop-dv.sh` + oracle):** (a) table-level Dir-2: the Rust REPLACEMENT
           chain's final table read by Java's production scan (rows reflect merged DV; old DV absent
           from manifests — Java manifest cross-check); (b) metadata-level: both sides run {append,
           row_delta(DV1), row_delta(add DV2 + remove DV1)} → canonical views 3-way (`removed-dvs`
           allowlist key, first LIVE comparison); fail-closed sentinels per the D4 rule. Mutations: (i)
           skip the remove in the Rust chain → metadata diff fails (extra live DV + missing
           removed-dvs); (ii) skip the merge (DV2={3} only) → Java table-read shows position-1 rows
           RESURRECTED. Restore + green.
        6. **GAP_MATRIX reckoning:** with merge + removal + replacement interop both directions, judge
           the DV-writer row against DoD (API matches Java + unit tests + interop both directions). If
           ✅, flip with dated evidence chain + name residue moving elsewhere (read row keeps its own
           residue). RowDelta + read rows: terse note updates. transaction/ should need NOTHING — STOP
           and report if otherwise. Pipe audit.
        7. **Docs:** writer/map.md, this outcome, lessons entry if a correction lands.
        BUILDER OUTCOME (2026-06-10, Arc-E Inc 2 — Opus; awaiting reviewer): **DV-WRITER ROW FLIPPED
        🟡→✅ — the writer surface is now complete vs Java's BaseDVFileWriter.** Hook:
        `DVFileWriter::with_previous_deletes(HashMap<path, PreviousDeletes>)` (mirrors Java's
        `loadPreviousDeletes` ctor arg) + `close_with_result() -> DVWriteResult { delete_files,
        rewritten_delete_files }` (mirrors Java `DeleteWriteResult`; `close()` kept returning just the
        DVs — ZERO blast radius for ~10 existing callers). Merge unions previous positions
        (`DeleteVector::merge`) into the new DV; file-scoped source files returned for removal.
        **isFileScoped finding (1.10.0 BYTECODE-verified):** `ContentFileUtil.isFileScoped(df) ==
        (referencedDataFile(df) != null)` — NOT just `isDV`; it is eq-delete→false, then non-null
        `referenced_data_file`, then the `_file_path`-bounds-equal fallback (DV OR path-scoped pos
        delete). `is_file_scoped` lives in the writer module (NOT a fork of `is_deletion_vector`, which
        is `format==Puffin`). Engine-caller contract mirrored: Spark `SparkPositionDeltaWrite`
        L251+L255-256 `addDeletes(dv)` + `for rewritten: removeDeletes(file)` → Rust
        `add_deletes(result.delete_files).remove_deletes_many(result.rewritten_delete_files)`.
        **Run-store byte question: ANSWERED.** The merged blob (prev {1} ∪ new {3} = {1,3}, array
        container) is BYTE-IDENTICAL to Java's same merge (interop `test_dv_replace_merged_blob_bytes`,
        44 B). The documented universal caveat stands (a previous DV whose store is ALREADY a Run
        container ties at `card == 2·runs` to array on roaring-rs vs run on Java — positions identical,
        bytes may differ; Java reads our blob either way — not contorted around). **Tests:** 7 unit
        (merge cardinality, file-scoped selectivity, eq-delete exclusion, unwritten-path ignore,
        byte-identical floor, `is_file_scoped` predicate, the crown jewel: DV1{1} → loaded back via the
        production DECODER → writer merges {1,3} → add+remove → scan {10,30,50}) + 3 interop (table
        Dir-2 read incl. DV1-absent manifest check, metadata 3-way incl. first LIVE `removed-dvs`, the
        merged-blob byte-compare). Mutations: merge→no-op (crown jewel + unit fail), `is_file_scoped`→
        always-true (3 scope tests fail); interop (i) skip remove → Rust commit REJECTED at the fresh-DV
        door (stronger than the briefed metadata-diff — fail-loud at commit); (ii) skip merge → Rust
        scan sanity shows id 20 RESURRECTED (stronger than the briefed Java-read — caught at GEN). Both
        restored byte-clean. **lib 1775 ×2; datafusion 80 lib + 9 integration (the documented
        rt-multi-thread doctest artifact still fails on the clean tree — unrelated); full
        `run-interop-dv.sh` GREEN end-to-end (16 steps, both mutations shown failing then restored).**
        **OUT-OF-SCOPE EDIT FLAGGED:** `lib.rs` `mod delete_vector` → `pub mod delete_vector` (the
        public `PreviousDeletes::new(DeleteVector, …)` API requires `DeleteVector` to be NAMEABLE
        downstream — it was a private-module pub type, callable but not constructible externally). Added
        7 doc comments + `is_empty()` to satisfy `#![deny(missing_docs)]` + clippy on the now-public
        type. transaction/ took ZERO production change (only the crown-jewel TEST, as expected). Files:
        ONLY the allowed set + the flagged `lib.rs`.
- [ ] **Arc F — `cherrypick`** (branch `phase2/cherrypick` off MAIN — no DV dependency): Java
      `SnapshotManager.cherrypick` / cherry-pick operation (WAP semantics: `wap.id`,
      `published-wap-id`, `source-snapshot-id`; fast-forward when the source is directly ahead;
      conflict validation between source base and current head). The last Phase-2-gated
      ManageSnapshots item, now unblocked by the write machinery. 1-2 increments.
- [ ] **Arc G (as time remains) — carried-forward small items off MAIN:** (1) table-metadata
      `last-sequence-number` lenient read (`#[serde(default)]`, the Phase-1 carried item) with
      spec citation + tests; (2) the retention-positivity question — settle WHERE (if anywhere)
      Java enforces non-positive rejection from 1.10.0 bytecode, then port or close the item with
      evidence. Then, if hours remain: data-level write-actions interop starter.
- [ ] **Morning report** in this file + the final session message: per-arc outcomes, branches +
      compare URLs, anything skipped, the compaction flags.

## DONE (2026-06-10): Deletion-vector arc (branch `phase2/dv-writer`, 4 commits 88f852b4→67aa056f, pushed — ONE PR ready for morning review)

Actor-critic per increment with **FABLE builder + FABLE reviewer** (user-authorized for this
sequence, naming the tier explicitly — supersedes the Opus default for this arc only).
Orchestrator re-runs the gate + commits; one commit per increment, pushed; Cargo FROZEN —
**exception pre-authorized: NONE; if a dep is truly needed, STOP.**

**Scope correction found during orchestrator prep:** the GAP_MATRIX read row claims
"position-deletes + DVs during scan ✅" but `caching_delete_file_loader.rs` routes ALL
`PositionDeletes` content to the PARQUET reader (`parquet_to_batch_stream`); the Puffin DV loader
is a literal TODO (L52). The ✅ came from the 0.7→0.9.1 sync notes and was never scan-verified —
the scan-exec interop cross-product covered parquet position/equality deletes only, never DVs.
The read path is therefore Increment D1, before any writer work.

- [x] **D1 — DV scan READ path** (DONE 2026-06-10 — Fable builder + Fable reviewer APPROVED +
      orchestrator gate/commit): dispatch Puffin-format position deletes in
      `CachingDeleteFileLoader` to a DV loader (direct ranged read → `deletion-vector-v1` blob →
      magic/length/CRC framing → `RoaringTreemap` → `DeleteVector` keyed by
      `referenced_data_file`); DV-vs-position-delete precedence per Java `DeleteFileIndex`;
      corrected the over-claiming GAP_MATRIX read row. Crown jewel GREEN: Rust scans a JAVA
      1.10.0-written V3+DV table (incl. >2^32 positions + run containers). Reviewer added 3 pins
      (hostile-container-count DoS, max-valid-key boundary, two-DVs-in-one-puffin) + proved
      fail-loud-on-corruption against the real fixture + settled the serde-default blast radius
      (old serializations fail loudly in the parquet reader — correct posture). Gate: lib 1722 ×2,
      datafusion 80+9 (known doctest artifact), interop script green ×2 runs.
      BUILDER PLAN (2026-06-10, D1 builder — FABLE):
      - [x] `delete_vector.rs`: `DeleteVector::deserialize_deletion_vector_v1(&[u8])` — framing
            per puffin-spec.md L146-164 + Java `BitmapPositionDeleteIndex.deserialize` (BE u32
            length prefix over magic+bitmap; LE magic 0x6439D3D1 = bytes D1 D3 39 64; BE CRC-32
            of magic+bitmap via the existing CRC-32 dependency, checked BEFORE bitmap parse); portable 64-bit
            roaring decoded MANUALLY mirroring Java `RoaringPositionBitmap.deserialize` L260-307
            (u64 LE count ≤ i32::MAX + payload bound; u32 LE keys ≥0, ≤ i32::MAX-1, strictly
            ascending; checked `RoaringBitmap::deserialize_from` per key; exact consumption — no
            trailing bytes); positions appended as `(key << 32) | low`. Unit tests: round-trip
            (incl. >2^32), empty, dense run-container, EVERY malformed class (truncations at each
            boundary, wrong magic, CRC mismatch, length-prefix mismatch both ways, bitmap
            garbage, count overflow, non-ascending keys, trailing bytes) — all clean `Err`, no
            panics; env-gated Java-blob byte test (`ICEBERG_INTEROP_DV_DIR`).
      - [x] **SCOPE ADDITION (flagged):** `scan/task.rs` — extend `FileScanTaskDeleteFile` with
            `file_format`, `referenced_data_file`, `content_offset`, `content_size_in_bytes`,
            `record_count` (serde-defaulted) + fill in the `From<&DeleteFileContext>` impl. The
            DV discriminator Java uses (`ContentFileUtil.isDV` = format == PUFFIN, L142-144) and
            the direct-ranged-read inputs (`BaseDeleteLoader.readDV` L171-183) can only travel on
            the task's delete entry; no in-scope alternative exists. Test literals in
            `arrow/reader.rs` (3 sites, test-only) gain the new fields.
      - [x] `delete_file_index.rs`: `dv_by_path: HashMap<String, Vec<Arc<DeleteFileContext>>>`
            keyed by `referenced_data_file` (Java `DeleteFileIndex.build` L505-506, `add` L528-535);
            `get_deletes_for_data_file` mirrors `forDataFile` L151-168: a data file with a DV gets
            {global eq, partition eq, DV} and NO parquet position deletes. DEFERRED (documented):
            the two ValidationException paths (duplicate DV → moved to the loader's door;
            `dv.dataSequenceNumber() >= seq` L209-213 → infallible signature, residue).
      - [x] `arrow/caching_delete_file_loader.rs` + `arrow/delete_file_loader.rs`: dispatch
            PositionDeletes+Puffin to a DV load (direct ranged read via new
            `BasicDeleteFileLoader::read_bytes_range`, per Java `BaseDeleteLoader.readDV` L171-183
            — deliberately NOT PuffinReader, Java cites ≥3 requests, L144-147); validations per
            `validateDV` L266-283 (offset/length present, ≤2GB, referenced path present) +
            cardinality == record_count (Java `deserializeBitmap` L203-209); duplicate-DV reject
            before load; cache key `{path}@{offset}` (one puffin file holds MANY blobs — the bare
            path key would mark blob 2 "already loaded"); decoded vector upserted under
            `referenced_data_file`. Loader tests incl. the sibling-file negative control + an
            ArrowReader-level scan test (Rust-synthesized DV; deleted positions absent, sibling
            file intact).
      - [x] `dev/java-interop/`: `DvScanOracle` (generate-interop-dv) — Java 1.10.0 writes a V3
            table (2 real parquet data files + a real DV via `BaseDVFileWriter` deleting positions
            {1,3} of file 1) + `java_dv_scan_rows.json`; ALSO emits a synthetic
            high-bits/run-container DV blob (`dv_blob.bin` + expected positions JSON) for the
            byte-level decode pin. `run-interop-dv.sh` drives generate → both env-gated Rust tests.
            New `crates/iceberg/tests/interop_dv_scan.rs` (env-gated, empty-string-safe, offline
            no-op).
      - [x] Docs: GAP_MATRIX merge-on-read read row corrected (pre-change reality: DVs were NOT
            loadable — routed to the parquet reader; D1 adds the DV scan path + Direction-1
            interop); todo outcome; scan/map.md only if stale.
      - [x] Mutations (with /tmp backups + full-suite re-run after restore): (a) skip CRC check;
            (b) key vector by the DV file's own path; (c) drop the `<<32` high-bits shift.
      - [x] Gate: typos; fmt; clippy workspace (excl. sqllogictest); `cargo test -p iceberg --lib`
            ×2; `cargo test -p iceberg-datafusion` (read-path rule); offline no-op of the new
            interop test; the REAL `run-interop-dv.sh` end-to-end.
      BUILDER OUTCOME (2026-06-10, D1 builder — FABLE; awaiting reviewer): **CROWN JEWEL GREEN ON
      THE FIRST RUN** — Rust scanned the Java-1.10.0-written V3 table with a real `BaseDVFileWriter`
      DV to exactly Java's own read ({10,30,50,60,70,80}; 20/40 deleted, sibling file intact), AND
      the raw Java-serialized blob (5005 positions incl. 2^32+7 / 2^33+1 + a 5000-position run →
      RUN containers) decoded to the exact position set — `roaring-rs`'s portable treemap layout is
      EMPIRICALLY byte-compatible with Java `RoaringPositionBitmap.serialize` (count is non-padded
      vs Java's dense-with-empty-gap-bitmaps array, but the decoder accepts both; pinned by the
      empty-gap-bitmap unit test). Implementation: manual outer decode (Java `readBitmapCount`/
      `readKey` validations + exact-consumption check) + checked `RoaringBitmap::deserialize_from`
      per key; CRC via the crate's existing gzip dependency (no Cargo change); loader does ONE ranged read per
      Java `BaseDeleteLoader.readDV` (not PuffinReader), cache-keyed `{path}@{offset}` (one Puffin
      file holds many blobs — the bare-path key would mark blob 2 already-loaded = silent
      under-delete); index mirrors `forDataFile` precedence (DV supersedes parquet pos-deletes for
      its file; eq deletes still apply). 25 new lib tests *(reviewer-corrected count: 14
      delete_vector incl. 8 malformed-input tests + 4 index + 6 loader incl. ArrowReader-level
      scan + 1 serde-default; one of the 14 env-gated)* + 1 env-gated integration test
      (`interop_dv_scan.rs`). Mutations all caught + restored byte-clean + full suite re-run: (a) CRC skip
      → exactly the CRC test; (b) key-by-DV-path → path-keying + cache + scan tests AND the crown
      jewel (ids 20/40 resurrect); (c) `<<32` drop → exactly the 3 high-bits tests. Gate: typos/
      fmt/clippy(workspace excl. sqllogictest) clean; lib 1719 ×2; datafusion 80+9 (doctest
      failure = the documented pre-existing rt-multi-thread artifact); all interop tests no-op
      offline; `run-interop-dv.sh` end-to-end green twice. Cargo/pom 0-diff. SCOPE ADDITIONS
      (flagged): `scan/task.rs` (5 serde-defaulted fields on `FileScanTaskDeleteFile` + the From
      impl — the only carrier for Java's isDV discriminator + readDV inputs) and `arrow/reader.rs`
      (3 test literals gained the new fields, test-only). DEFERRED (documented in code + matrix):
      Java's two index-level ValidationExceptions — duplicate-DV moved to the load door
      (fail-loud `Err`), `dv_seq >= data_seq` deferred (infallible index signature; invalid-table
      state only); DV caching beyond one loader instance (Java doesn't cache DVs either);
      Direction-2 DV interop (needs the D2 writer).
      REVIEWER OUTCOME (2026-06-10, D1 reviewer — FABLE): APPROVED with 3 added pins (lib 1722 ×2).
      All 8 review points verified; no correctness bug found. Pins added (each mutation-verified):
      `test_two_dvs_in_one_puffin_file_both_load_under_own_data_file` (the exact case the
      `{path}@{offset}` cache key exists for — fails under a bare-path key),
      `test_dv_blob_hostile_inner_container_count_rejects_fast` (DoS-by-allocation via the INNER
      roaring container count; roaring 0.11.3 caps containers at 65536 pre-allocation — verified in
      its source), `test_dv_blob_max_valid_key_boundary_accepted` (the ACCEPT side of Java
      `readKey`'s `i32::MAX-1` bound — builder pinned only the reject side). Adversarial probes all
      clean (no panic): hostile inner cookie/count, `read_bytes_range` overflow/past-EOF, declared
      length 4/8, old-serialized DV task (defaults to Parquet → LOUD "Corrupt footer" error, the
      pre-D1-equivalent; default confirmed right — no in-repo serializer of `FileScanTask` exists).
      Fail-loud on REAL corruption proven against the Java fixture: CRC byte-flip → loud
      `Invalid deletion vector CRC`, truncation → loud error; crown jewel re-verified to catch
      mutation (b) (ids 20/40 resurrect). Builder mutations (a)/(c) + reviewer mutations (pos
      deletes alongside DV → supersede pin fails; duplicate-DV door disabled → duplicate test
      fails) all caught. Known pre-existing (NOT D1): a delete-file load error leaves a stale
      `Loading` notify entry in the shared `DeleteFilter` (same class as the parquet pos-del path);
      scan still errors loudly. Test-count breakdown in the builder block corrected above.
- [x] **D2 — DV serialization + `DVFileWriter`** (DONE 2026-06-10 — Fable builder + Fable
      reviewer APPROVED + orchestrator gate/commit. Byte parity with Java proven UNCONDITIONALLY
      incl. run containers — roaring-rs `optimize()` matches Java's criteria incl. ties; 3 interop
      fixtures byte-identical 69/76/46 B; builder corrected the BRIEF's wrong reserved id
      (ROW_POSITION = 2147483645, not 2147483545); reviewer confirmed the dense-gap size door
      (25 GB-by-gaps rejected in 303 µs, gap-term mutation caught), MAX_POSITION bit-exact
      (0x7FFFFFFE_80000000), added the tie pin + duplicate-position pin, softened the Run-store
      re-serialization caveat for D3. Gate: lib 1737 ×2, Direction-2 oracle green ×2.):
      bitmap serialization byte-exact vs Java
      `BitmapPositionDeleteIndex.serialize` (portable 64-bit roaring + index framing), puffin blob
      w/ `referenced-data-file`+`cardinality` properties (BaseDVFileWriter.java L52-53, L173-186),
      DeleteFile metadata (content_offset/content_size_in_bytes/referenced_data_file/record_count
      L145-159); exact-byte fixtures + round-trip through D1's reader.
      BUILDER PLAN (2026-06-10, D2 builder — FABLE):
      - [x] `delete_vector.rs`: production `DeleteVector::serialize_deletion_vector_v1(&self) ->
            Result<Vec<u8>>` — Java-faithful DENSE layout (`RoaringPositionBitmap.serialize`
            L245-252 writes `bitmaps.length` = max key + 1 entries INCLUDING empty gap bitmaps;
            `roaring-rs` treemap serialize is SPARSE so the outer layout is hand-rolled per
            sub-bitmap), per-sub-bitmap run-length encode via `RoaringBitmap::optimize()` on a
            clone (Java `runLengthEncode` L176-182 → `runOptimize()`; roaring 0.11.3 HAS
            `optimize()` with the identical run-iff-strictly-smaller criterion — verified in
            registry source `bitmap/container.rs:243`), framing per
            `BitmapPositionDeleteIndex.serialize` L124-137 (BE u32 length of magic+bitmap, LE
            magic D1 D3 39 64, BE zlib CRC-32 of magic+bitmap). Errors: empty vector (never
            serialized per BaseDVFileWriter flow), key > i32::MAX-1 (unrepresentable in Java's
            dense array — `validatePosition`/`MAX_POSITION` L342-348), total > 2GB
            (`computeBitmapDataLength` L158-163). Test encoder `encode_deletion_vector_v1`
            delegates to the production fn; empty-decode test switches to a raw count=0 frame.
      - [x] `delete_vector.rs` tests: HAND-COMPUTED golden bytes for {0,5,2^32+1} (66 bytes incl.
            CRC 0x9ACC8CA4, derived via python struct+zlib independent of the production code) and
            the DENSE-GAP pin {0,2^33} → count 3 with a literal empty key-1 entry (76 bytes, CRC
            0xBC98851A); round-trip through D1's decoder (gaps, 0, >2^32, run shape); empty/key-
            bound/2GB-guard error tests.
      - [x] `puffin/writer.rs` (minimal write-side extension, FLAGGED): `add()` returns
            `Result<BlobMetadata>` (Java `PuffinWriter.write(Blob)` returns BlobMetadata —
            BaseDVFileWriter L164 consumes it for content_offset/content_size); `close()` returns
            `Result<u64>` file size (Java `fileSize()`, consumed at L134). All existing callers
            are tests; call sites compile unchanged (`?;` discards the value).
      - [x] NEW `writer/base_writer/deletion_vector_writer.rs` + `mod.rs` wiring: `DVFileWriter`
            mirroring `BaseDVFileWriter` — `new(OutputFile)`, `delete(path, pos,
            Option<&PartitionKey>)` accumulating per path (partition captured at FIRST delete per
            path, Java `computeIfAbsent` L74-79; pos validated vs MAX_POSITION), async
            `close() -> Result<Vec<DataFile>>`: no deletes ⇒ NO file (L106-109); else ONE puffin,
            one uncompressed `deletion-vector-v1` blob per path in SORTED path order (determinism
            is OUR contract; Java iterates a HashMap — order is not Java's contract), blob fields
            = [ROW_POSITION id 2147483645 = i32::MAX-2] (BRIEF CORRECTION: the brief said
            2147483545 which is DELETE_FILE_POS; MetadataColumns.java L39-44 says MAX-2),
            snapshot_id/sequence_number −1 (L177-178), properties referenced-data-file +
            cardinality (L181-185); per-path DeleteFile per createDV L145-159. DEFERRED LOUDLY to
            D3: `loadPreviousDeletes` merge + `rewrittenDeleteFiles` (L117-126, the commit-path
            concern) — this writer takes only fresh positions.
      - [x] Writer tests: multi-file blob offsets distinct + full DeleteFile metadata; determinism
            across two runs (byte-identical puffin); no-deletes ⇒ no file; file_size ==
            on-disk size; round-trip write → D1 `CachingDeleteFileLoader` → positions match
            (crate-internal unit test).
      - [x] Direction-2 oracle: new `crates/iceberg/tests/interop_dv_write.rs` (env
            `ICEBERG_INTEROP_DV_WRITE_DIR`, offline no-op): GEN writes a real puffin via
            DVFileWriter (2 referenced files; positions incl. 0, a 5000-run, >2^32, a dense-gap
            key) + expected JSON {path → positions + blob offset/size}. InteropOracle new mode
            `verify-interop-dv-write`: Java reads the RUST puffin via Puffin.read footer + ranged
            blob read + `PositionDeleteIndex.deserialize` (the production scan path,
            BaseDeleteLoader.readDV L171-183), asserts positions; ALSO emits Java's own
            serialization of the SAME position sets via BaseDVFileWriter → `java_dv_blob_*.bin`.
            Rust byte-parity test asserts rust-puffin blob bytes == java blob bytes (incl. the
            run-shaped set — roaring-rs CAN emit runs, so byte-exactness is pinned for runs too).
            `run-interop-dv.sh` extended to drive D1 AND D2 phases with the output-sentinel grep.
      - [x] Mutations (snapshot to /tmp, restore, full-suite re-run): (a) sparse-not-dense ⇒
            dense-gap golden fails; (b) CRC over bitmap only ⇒ golden + round-trip fail; (c)
            count = max_key ⇒ golden + decoder fail; (d) blob offset off by footer magic ⇒ writer
            round-trip fails.
      - [x] Docs: GAP_MATRIX DV-writer row ❌→🟡; `writer/map.md` row; `dev/java-interop/map.md`
            run-interop-dv.sh row; this todo outcome.
      BUILDER OUTCOME (2026-06-10, D2 builder — FABLE; awaiting reviewer): **CROWN JEWEL GREEN ON
      THE FIRST RUN, BYTE-EXACT INCLUDING RUN CONTAINERS** — Java's production reader
      (`Puffin.read` footer + the `readDV`-style ranged read + `PositionDeleteIndex.deserialize`)
      decoded the Rust-written Puffin DVs exactly (5003 positions incl. the 5000-run + 2^32+7;
      dense-gap set), AND every Rust blob is BYTE-IDENTICAL to Java's own `BaseDVFileWriter`
      serialization of the same positions — the run-container question is SETTLED: roaring 0.11.3
      `optimize()` (verified in registry source) makes Java-identical run-iff-strictly-smaller
      container choices, so byte parity holds for run-shaped inputs too (69-byte run blob
      matched). BRIEF CORRECTION: blob `fields` = [2147483645] (ROW_POSITION = i32::MAX−2,
      MetadataColumns.java L39-44), NOT the brief's 2147483545 (that is DELETE_FILE_POS).
      Implementation: production `serialize_deletion_vector_v1` (dense layout hand-rolled —
      roaring's treemap serialize is sparse; per-sub-bitmap optimize on clones; errors: empty /
      key>i32::MAX−1 / >2GB pre-alloc) absorbing the D1 test encoder; `PuffinWriter.add` →
      `Result<BlobMetadata>` + `close` → `Result<u64>` (file size) mirroring Java's returns (all
      callers were tests, call sites unchanged); new `DVFileWriter` (sorted-path blob order = our
      determinism contract, partition captured at first delete per path, MAX_POSITION door incl.
      the Java low-word quirk 0x80000000). 13 new lib tests (1735 ×2): hand-computed exact-byte
      goldens ({0,5,2^32+1} 66B CRC 0x9ACC8CA4; dense-gap {0,2^33} 76B with the literal empty
      key-1 entry), round-trips, run-container cookie pin, 3 error doors, 6 writer tests incl.
      the D1-loader round-trip. Mutations all caught + restored byte-clean (cmp): (a) sparse ⇒
      dense-gap golden fails; (b) CRC-sans-magic ⇒ goldens + all round-trips; (c) count=max_key ⇒
      12 tests incl. decoder trailing-bytes; (d) offset-before-header-magic ⇒ loader round-trip +
      coordinates + puffin Java-bit-identical tests. KNOWN RESIDUE (flagged): the Puffin FOOTER
      JSON is not byte-deterministic (HashMap property order, pre-existing `puffin/metadata.rs`,
      outside the file set) — blob region + structural footer pinned instead; Java reads footers
      as JSON so interop is unaffected. DEFERRED LOUDLY: previous-deletes merge +
      `rewrittenDeleteFiles` (BaseDVFileWriter L117-126) → D3 with the commit path.
      REVIEWER OUTCOME (2026-06-10, D2 reviewer — FABLE): **APPROVED, 2 pins added, 1 doc
      correction, no production-code bugs.** All seven attack points held: (1) the dense-gap size
      door INCLUDES gap bytes (probed: one position at key 10_000 ⇒ exactly 120_042-byte blob =
      12 B/gap entry; key 250M ⇒ 3.0 GB-by-gaps REJECTED in 433 µs; key i32::MAX−2 ⇒ 25.77 GB
      rejected in 303 µs — O(present keys), pre-allocation; matches Java `serializedSizeInBytes`
      over the dense array + `computeBitmapDataLength` ≤ Integer.MAX_VALUE, re-derived from
      1.10.0 BYTECODE); reviewer mutation (drop the absent×empty term) caught by 4 tests incl.
      the 2GB door test. Write loop for legal gappy blobs is O(dense) like Java's (~418 ns/gap
      entry debug) — flagged, not fixed. (2) MAX_POSITION re-derived from bytecode:
      `toPosition(2147483646, Integer.MIN_VALUE)` = 0x7FFFFFFE_80000000 = 9223372030412324864;
      Rust constant + boundary tests sit exactly on it; positions in (MAX, key-ceiling] rejected
      by the delete door like Java's `validatePosition` (Java's DESERIALIZER would accept them —
      the serializer layer matches Java's serialize, the delete door matches set(); layering
      identical). (3) run-criterion parity verified at SOURCE level both sides (roaring-rs
      0.11.3 container.rs vs RoaringBitmap 1.3.0 bytecode): Array/Bitmap branches IDENTICAL
      incl. ties; CAVEAT found+documented — for an already-RUN store (deserialized DVs, D3
      merge) roaring-rs omits Java's 2-byte array overhead, so at cardinality == 2·runs Java
      keeps run / Rust would emit array (readable, byte-parity-only; doc softened). PIN ADDED:
      the exact array/run size tie {0,1,2} (6 == 6 bytes) as lib test + THIRD interop fixture
      file — Java byte-compare settled it (46-byte blob byte-identical). (4) puffin diff is
      signature+return only; reviewer mutation (offset captured AFTER blob bytes) caught by 7
      tests incl. the pre-existing Java-bit-identical pins; all non-DV callers are tests. (5)
      createDV metadata verified against bytecode (toBlob fields=[2147483645=ROW_POSITION,
      bytecode-confirmed], −1/−1, two properties; shared fileSize after close like Java's
      Optional). PIN ADDED: duplicate-position ⇒ record_count 1. NOTE (D3): `delete(path, pos,
      None)` loses the spec id (DataFileBuilder default) — Java always receives the spec;
      revisit when the commit path wires real specs. (6)(7) no-deletes ⇒ no file (filesystem
      probed); interop re-run END-TO-END ×2 (incl. extended fixture) green; oracle
      CAN-fail proven (tampered expected JSON ⇒ FAIL line ⇒ script grep trips; NOTE the verify
      step is not re-runnable in place — emit table collides — harmless, script resets dirs).
      Suite 1737 ×2 (1735 + 2 reviewer pins); gate green; Cargo/pom 0-diff.
- [x] **D3 — commit path** (DONE 2026-06-10 — Fable builder + Fable reviewer + orchestrator
      gate/commit. Builder: gate + fresh-DV door + validateAddedDVs op-set fix [stale "REPLACE
      unrepresentable" claim — 1.10.0 set is {overwrite, delete, replace}] + the missing
      validateNoConflictingFileAndPositionDeletes + summary DV counters + 56-test V2-fixture
      migration. REVIEWER FOUND + FIXED 2 DOOR BUGS with fail-before proof: under-fire (door keyed
      on the DV's own spec/partition — cross-spec legacy delete shadowed = resurrection class) and
      over-fire (no seq filter — predating legacy delete froze DV writes); fix resolves the
      referenced file's LIVE entry and mirrors read-path applicability incl. delete_seq >=
      data_seq; +5 reviewer pins incl. the concurrent-format-upgrade refreshed-base race. Gate:
      lib 1756 ×3, datafusion 80+9, interop script green both directions, Cargo/pom frozen.):
      RowDelta DV adds; V2-forbids/V3-requires gating
      (`validateDeleteFileForVersion`, MergingSnapshotProducer L295-316); `validateAddedDVs`
      (L824-870, "Found concurrently added DV for %s: %s") + the no-override tx-captured-start
      pin; write→scan crown jewel on V3.
      BUILDER PLAN (2026-06-10, D3 builder — FABLE). Pre-flight findings: `validateAddedDVs` ALREADY
      landed pre-D1 (commit c1c58f7b) incl. the tx-captured pin + disjoint negative + self-skip +
      malformed-DV tests — D3 task 3 is verify/fix, not build. Found one REAL bug in it: its walk
      reuses `added_delete_files_after` (`{Overwrite, Delete}`) but Java 1.10.0
      `VALIDATE_ADDED_DVS_OPERATIONS` = `{overwrite, delete, replace}` (bytecode-verified) and
      `Operation::Replace` IS representable in Rust since the rewrite actions landed — the stale
      "REPLACE unrepresentable" doc claim hid a missed concurrent-REPLACE-adds-DV window. 1.10.0
      bytecode also shows: NO apply-time re-validation of buffered deletes (that is MAIN-only);
      the gate fires in `add(DeleteFile)` → `validateNewDeleteFile`; `BaseRowDelta.validate` ALSO
      calls `validateNoConflictingFileAndPositionDeletes()` (present in 1.10.0 bytecode, missing in
      Rust). Summary bytecode: a DV increments `added-dvs` INSTEAD of `added-position-delete-files`,
      but STILL increments `added-delete-files` + `added-position-deletes`; sizes use
      `ScanTaskUtil.contentSizeInBytes` (DV ⇒ `content_size_in_bytes`, not file size).
      - [x] `snapshot.rs`: per-file format-version gate in `validate_added_delete_files`
            (V1 throws / V2 rejects DVs / V3 requires DVs for position deletes, eq exempt; exact
            Java messages incl. `dvDesc`); `dv_desc` helper; `operation_adds_dvs` op filter
            (`{Overwrite, Delete, Replace}`) + `added_dv_candidate_delete_files_after` wrapper.
      - [x] `row_delta.rs`: switch `validate_added_dvs` to the DV op-set walk + Java-exact `dv_desc`
            message; add always-on `validateNoConflictingFileAndPositionDeletes`; add the
            fresh-DV-only door (Rust-conservative: reject a DV add when the CURRENT snapshot already
            has a live DV — or a shadowed parquet position delete — for the referenced file; Java
            instead merges previous deletes, BaseDVFileWriter L117-126 — deferred).
      - [x] `spec/snapshot_summary.rs`: `added_dvs`/`removed_dvs` counters + DV content-size
            accounting, offline unit pins on exact keys.
      - [x] Tests: gating × versions/content; door 3-way (+ the V2→V3-upgrade parquet-shadow pin);
            DV-op-set walk (hand-built REPLACE snapshot); manifest round-trip pin (committed DV's
            referenced_data_file/content_offset/content_size_in_bytes survive Rust manifest
            write→read — CLEAN, no spec/manifest fix needed); crown jewel (DVFileWriter →
            row_delta → scan survivors) + resurrection mutation.
      - [x] TRAP-1 migrations: V3 fixtures that row_delta PARQUET position deletes move to a new
            `make_v2_minimal_table_in_catalog` (same schema as V3 minimal — verified identical);
            DV/equality tests stay V3. Affected files (test-only, flagged): row_delta.rs,
            delete_files.rs, overwrite_files.rs, replace_partitions.rs, rewrite_files.rs,
            merge_append.rs, rewrite_manifests.rs, scan/incremental.rs (synthetic delete became
            DV-shaped, stays V3), transaction/mod.rs (the new fixture).
      - [x] Docs: GAP_MATRIX (DV writer/RowDelta rows), transaction/map.md, this outcome.
      BUILDER OUTCOME (2026-06-10, D3 builder — FABLE; awaiting reviewer): **CROWN JEWEL GREEN —
      the all-Rust chain closed** (real parquet → D2 `DVFileWriter` DV {1,3} → D3
      `row_delta().add_deletes` commit → D1 scan returns exactly {10,30,50}; stripping the delete
      manifest from the commit resurrects {10,20,30,40,50} — mutation-verified). Format gate
      Java-byte-exact at all three versions (1.10.0-bytecode-verified; 1.10.0 has NO apply-time
      re-validation — that is MAIN-only; Rust's commit-time placement vs the refreshed base
      subsumes both). validateAddedDVs was pre-existing (c1c58f7b) but its walk MISSED Java's
      REPLACE op (`VALIDATE_ADDED_DVS_OPERATIONS` has 3 members; `Operation::Replace` became
      representable with the rewrite actions) — fixed + pinned with a hand-built REPLACE-op DV
      commit whose message assert isolates the walk from the door. Missing 1.10.0 check
      `validateNoConflictingFileAndPositionDeletes` added (exact message). Fresh-DV-only door
      (documented Rust-conservative divergence): rejects a DV add when the file already has a live
      DV (two-DVs = fail-late scan rejection) OR a shadowed legacy parquet position delete
      (V2→V3-upgrade fixture; DV-supersedes precedence would silently resurrect) — 3-way + upgrade
      tests, both mutation directions caught. Summary: added-dvs/removed-dvs landed exactly per
      bytecode (DV counts INSTEAD of added-position-delete-files, still in added-delete-files +
      added-position-deletes; size = blob content_size_in_bytes per ScanTaskUtil) — collector pins
      + the commit-level pin inside the crown jewel; removed-dvs reachable only collector-level
      (no delete-file removal path yet, documented). Manifest round-trip FINDING: CLEAN — the Rust
      V3 manifest writer already carries fields 143/144/145; raw `Manifest::try_from_avro_bytes`
      pin added, no spec/manifest change. TRAP-1: 56 tests broke under the gate; 53 migrated to a
      new V2 in-catalog fixture (subject = parquet position deletes, now the V2-only reality),
      1 stayed V3 with an equality delete (the DV-check self-skip pin), 1 became a DV-shaped
      fixture (incremental changelog), + the new fixture itself. 14 new tests (lib 1737 → 1751).
      Mutations (8, all caught, restored byte-clean, full suite re-run): delete-manifest strip ⇒
      resurrection; gate off ⇒ exactly the 3 gate pins; V3-eq-exemption drop ⇒ eq tests; V2 arm
      invert ⇒ 33 (the migrated suite IS the regression pin); door off ⇒ exactly the 2 door pins;
      door key-blind ⇒ exactly the 2 negative controls; walk op-set revert ⇒ exactly the REPLACE
      pin; summary DV-branch kill / blob-size kill ⇒ the summary pins + crown jewel.
      REVIEWER OUTCOME (2026-06-10, D3 reviewer — FABLE): all bytecode claims RE-VERIFIED against
      the 1.10.0 jars (`VALIDATE_ADDED_DVS_OPERATIONS` = {overwrite, delete, replace};
      `validateNewDeleteFile` switch incl. exact messages + V4 arm; `validateAddedDVs` walk +
      message; `validateNoConflictingFileAndPositionDeletes` semantics — intersects
      `removedDataFiles` locations with the `validateDataFilesExist`-fed `referencedDataFiles`,
      Java `List` rendering matched; `ContentFileUtil.dvDesc`/`isDV`; `SnapshotSummary
      .UpdateMetrics.addedFile/removedFile` branch ordering + `ScanTaskUtil.contentSizeInBytes`;
      `ADDED_DVS_PROP`/`REMOVED_DVS_PROP` keys). Re-ran mutations: walk op-set revert ⇒ exactly
      the REPLACE pin; shadow-arm disable ⇒ exactly the upgrade-fixture pin; V2-arm invert ⇒ 60
      fail (the migrated suite is the regression pin); rewrite_files seq-strip ⇒ exactly the eq
      crown jewel (migration did not weaken it). TWO DOOR BUGS FOUND + FIXED (fail-before/
      pass-after probes kept as pins, mutation-verified): (1) UNDER-fire — the parquet-shadow arm
      keyed applicability on the added DV's own (spec id, partition); after a partition evolution
      the legacy spec-0 delete never matched the spec-1 DV, so the DV COMMITTED and silently
      superseded a still-applying delete (resurrection class); (2) OVER-fire — no sequence test,
      so a partition-matched legacy delete PREDATING the referenced data file (delete_seq <
      data_seq, applies to nothing) froze all DV writes into that partition. Fix: the door now
      resolves each referenced file's LIVE data-manifest entry and mirrors the read-path test —
      path/(spec id, partition) scope vs THAT entry + delete_seq >= data_seq; referenced file
      with no live entry (same-commit add) ⇒ nothing applies. +5 pins (eq-delete door control,
      path-scoped-other-file door control, cross-spec under-fire, seq over-fire, and the
      refreshed-base gate race — a parquet delete built on V2 is rejected after a CONCURRENT
      V2→V3 upgrade, probing the do_commit re-base claim empirically); lib 1751 → 1756 ×2
      deterministic; fmt/typos/clippy clean; run-interop-dv.sh green both directions; Cargo/pom
      0-diff. Noted (not fixed): Java skips ALL of `BaseRowDelta.validate` when parent == null —
      Rust runs the removed∩referenced check on an empty table too (conservative-only divergence);
      Rust's summary `content_size_in_bytes` keys on PositionDeletes+Puffin where Java keys on
      non-DATA+Puffin (differs only for a pathological Puffin EQUALITY delete, on which Java NPEs
      for a null size — unreachable from real writers); V2/V3 fixture schemas differ by V3 x's
      initial/write-defaults (doc claim softened in mod.rs).
- [x] **D4 — interop:** bidirectional DV round-trips (Java writes V3+DV → Rust scans; Rust writes
      → Java reads) on the scan-exec harness; metadata-level chain notes; GAP_MATRIX flips with
      evidence.
      BUILDER PLAN (2026-06-10, D4 builder — FABLE):
      - [x] NEW `crates/iceberg/tests/interop_dv_table.rs` (one env var
            `ICEBERG_INTEROP_DV_TABLE_DIR`, empty-string-safe, offline no-op; phases invoked by
            test name from the script, like `interop_dv_write.rs`):
            (1) `test_dv_table_gen_rust_writes_java_readable_v3_dv_table` — the HEADLINE
            Direction-2 table: V3 identity(category) table at `<dir>/rust_table` on a real-FS
            MemoryCatalog; TWO real parquet data files (cat=a: (10,a,x)(20,a,y)(30,a,z); cat=b:
            (40,b,p)(50,b,q)(60,b,r)) `fast_append`ed at seq 1; D2's `DVFileWriter` writes ONE
            puffin holding TWO DVs (A: pos {1} = id 20; B: pos {0,2} = ids 40/60 — distinct
            record counts so the canonical entry sort never ties); D3's `row_delta` commits both
            at seq 2; Rust scan sanity = {10,30,50}; `final.metadata.json` via
            `TableMetadata::write_to`; emit `expected_rows.json`.
            (2) `test_dv_meta_views_match_java` — Rust's `snapshot_meta_view` of the JAVA mirror
            table AND of the Rust table both == `java_meta.json` (the E1 3-way, directions 1+2;
            direction 3 = the script's byte-diff).
      - [x] `InteropOracle.java`: new `DvTableOracle` — mode `verify-interop-dv-table` (load the
            RUST V3 metadata → `IcebergGenerics` PRODUCTION read → rows == expected_rows.json;
            PLUS the manifest-API cross-check: every delete entry content==POSITION_DELETES,
            format==PUFFIN, referencedDataFile/contentOffset/contentSizeInBytes set, recordCount
            == cardinality; sentinel "verify-interop-dv-table: N failures") and mode
            `generate-interop-dv-table` (the JAVA mirror chain for the metadata fixture:
            same schema/spec/V3, real parquet via the PartScanExec machinery, `newFastAppend`,
            `BaseDVFileWriter` two DVs in one puffin, `newRowDelta().addDeletes`,
            final.metadata.json under `<dir>/table`).
      - [x] `run-interop-dv.sh`: TMP3 reset in step 1 (idempotency — the D2 reviewer's collision
            note); new steps: Rust GEN → Java verify (sentinel grep) → Java mirror-table GEN →
            `emit-snapshot-meta` ×2 + `diff -u` (Java judging Rust's metadata byte-for-byte) →
            Rust meta test. D1/D2 steps preserved.
      - [x] Mutations: (a) GEN drops B's DV from the commit → Java table-read step FAILS with
            resurrected ids 40/60; (b) GEN skips the row_delta → the metadata byte-diff FAILS
            (missing snapshot). Restore byte-clean + full script green.
      - [x] Docs: GAP_MATRIX (read row: DV both directions data-level proven; DV-writer row STAYS
            🟡 — previous-deletes merge + old-delete removal deferred, BaseDVFileWriter L117-126;
            RowDelta row DV note) + pipe-count audit; tests/map.md + dev/java-interop/map.md rows;
            this todo outcome.
      - [x] Gate: typos; fmt; clippy (workspace excl. sqllogictest); `cargo test -p iceberg
            --lib` ×2; offline no-ops of ALL interop tests (env unset); `run-interop-dv.sh`
            end-to-end green.
      BUILDER OUTCOME (2026-06-10, D4 builder — FABLE; awaiting reviewer): **EVERYTHING GREEN ON
      THE FIRST RUN — both new proofs, zero canonicalization surprises, zero production changes.**
      (1) TABLE-level Direction-2 (the headline): Java's PRODUCTION scan (`IcebergGenerics` →
      `BaseDeleteLoader.readDV`) read the Rust-COMMITTED V3 table — 2 identity(category)
      partitions of real parquet `fast_append`ed at seq 1, ONE puffin holding TWO DVs (cat=a pos
      {1}, cat=b pos {0,2} — distinct cardinalities 1/2) `row_delta`'d at seq 2 — to exactly
      {(10,x),(30,z),(50,q)}, AND the manifest-API cross-check matched every committed DeleteFile
      field (content/format/referencedDataFile/contentOffset/contentSizeInBytes/recordCount +
      both DVs sharing ONE puffin location). Java 1.10.0 parsed the Rust V3 metadata
      (`next-row-id`), manifest list, and V3 delete manifests with no issue. (2) METADATA-level
      (E1-family): Java's canonical snapshot-meta view of the Rust DV chain byte-diffed IDENTICAL
      to Java's view of its own mirror chain (`newFastAppend` + `BaseDVFileWriter` +
      `newRowDelta`) on the FIRST diff — `added-dvs: 2` (with NO `added-position-delete-files`,
      the instead-of branch D3 wired), operation `delete`, `changed-partition-count: 2`, delete
      manifest split + post-inheritance seq 2 — and Rust's views of BOTH tables equal it
      (3-way complete). No `snapshot_meta_view.rs` change needed (the E2 lesson's
      order-insensitive fallback never even fired). SCOPE NOTE stated in the test module: the
      canonical entry tuple omits referenced_data_file/content_offset — covered instead by the
      table-level manifest cross-check. New: `interop_dv_table.rs` (2 env-gated tests,
      `ICEBERG_INTEROP_DV_TABLE_DIR`, offline + empty-string no-op), `DvTableOracle`
      (verify-interop-dv-table + generate-interop-dv-table), `run-interop-dv.sh` steps 7-11
      (D1/D2 steps preserved; TMP3 reset in step 1). HARNESS FIX found while mutation-testing:
      on this machine `mvn exec:java` DOES surface the oracle's `System.exit(1)` (contra the
      D2-era note), so under `set -e` the `VERIFY_OUT="$(...)"` captures aborted BEFORE echoing
      Java's diagnostics — both sentinel captures (steps 5 + 8) now `|| true` with the verdict
      taken ONLY from the output sentinel (success line present, no `^FAIL`), which is robust to
      either mvn behavior. Mutations (test-only, /tmp backup, restored byte-clean via cmp):
      (a) drop cat=b's DV from the commit ⇒ step 8 FAILS loudly — Java reads RESURRECTED ids
      40/60 ({10,30,40,50,60}) + manifest count 1≠2 ⇒ script exit 1; (b) skip the row_delta ⇒
      the metadata byte-diff FAILS showing the entire missing DV snapshot (added-dvs block) ⇒
      diff exit 1. Full script re-run END-TO-END GREEN after restore. GAP_MATRIX: DV-writer row
      gains the D4 table+metadata proofs but STAYS 🟡 (previous-deletes merge + superseded-delete
      removal, `BaseDVFileWriter` L117-126 — deferred; `removed-dvs` collector-level only);
      merge-on-read READ row now claims DVs data-level interop ✅ BOTH directions (stays 🟡 for
      its own residue); RowDelta row gains the D4 DV note; pipe-count audit clean. Gate:
      typos/fmt/clippy(workspace excl. sqllogictest) clean; lib 1756 ×2 (no lib change in D4);
      ALL 11 interop test binaries no-op offline; script green ×2 (initial + post-restore).
      Cargo/pom 0-diff. NO production parity bug found — the D1-D3 surface held under both new
      proofs.
      REVIEWER OUTCOME (2026-06-10, D4 reviewer — FABLE): **APPROVED, zero fixes needed — the
      harness is non-vacuous and fails closed.** Adversarial probes (all different from the
      builder's two mutations, each restored byte-clean via cmp): (a) EMPTY-OUTPUT probe — step
      8's capture pointed at /bin/true (mvn dying early with NO output) ⇒ script exit 1 via the
      success-sentinel-ABSENT branch (the verdict is not merely ^FAIL-present); (b) POISONED
      GROUND TRUTH — expected_rows.json edited between steps 7 and 8 to claim DV-deleted id 20
      survives ⇒ step 8 FAILS loudly (java-read {10,30,50} ≠ expected {10,20,30,50}, "1
      failures", exit 1) WITH Java's diagnostics echoed before the verdict — proving the `||
      true` change does what it claims; (c) DIRECT mvn exit-code probe — `verify-interop-dv-table`
      against an empty dir returns MVN-EXIT=1, empirically confirming the builder's claim that
      `mvn exec:java` DOES surface `System.exit(1)` here (so without `|| true`, `set -e` would
      abort the capture assignment pre-echo). No capture FILES exist (verdicts from shell vars;
      TMP/TMP2/TMP3 rm-rf'd in step 1) ⇒ no stale-sentinel leak possible. Mirror-chain
      equivalence read side-by-side: same schema/spec/V3, same rows, same DV positions/
      cardinalities (1 vs 2), same commit shape (append seq 1, row_delta seq 2); step 10 emits
      the two DIFFERENT tables (`table/` vs `rust_table/`) — diff non-vacuous (java_meta.json
      inspected: operation `delete`, `added-dvs: 2`, no `added-position-delete-files`, delete
      manifest entries at seq 2). Manifest cross-check is VALUE-level (offsets 4/46, sizes
      42/44, cardinalities 1/2 compared against expected_dvs.json), not presence-only. Note
      (accepted, E1-convention precedent): "byte-identical 3 ways" is strictly byte-level only
      for the script's diff direction; the two Rust-side directions are serde_json::Value
      structural equality. Independent gate re-run: typos/fmt/clippy clean; lib 1756 ×2; all 11
      interop binaries no-op with env unset AND the new one with empty-string env; zero src/
      diff + snapshot_meta_view.rs untouched confirmed; full script green (baseline + final
      post-probe runs, exit 0).

## DONE (2026-06-10 overnight): Phase-2 write-engine completion arc (branch `phase2/write-engine-completion`, squash-merged as PR #20)
## ACTIVE (2026-06-10): Arc F — `cherrypick` (branch `phase2/cherrypick`, BUILDER Opus)

The last Phase-2-gated `ManageSnapshots` item: Java `CherryPickOperation` (288 lines) — write-audit-publish
(WAP) semantics. Correctness-critical: a wrong replay or a missed dedup publishes staged data twice.

Plan (recorded before writing code):
- [x] Read all required Rust (`manage_snapshots`/`snapshot`/`append`/`replace_partitions`/`mod`/`spec/snapshot`)
      + Java `CherryPickOperation`/`WapUtil`/`SnapshotSummary`/`SnapshotChanges` + the two exceptions FULLY.
      Bytecode-verified all version-sensitive strings (4 summary keys + 2 exception formats + 6 cherrypick
      messages) against the 1.10.0 jars in `~/.m2` — all match the `/tmp/iceberg-java-ref` source.
- [x] **API + surface decision:** standalone `CherryPickAction` in new `transaction/cherry_pick.rs` +
      `Transaction::cherry_pick(snapshot_id)` ctor; a doc pointer on `ManageSnapshotsAction` (NOT a delegating
      method — the ref-op action only EMITS ref updates and has no snapshot-producing path; cherrypick needs
      the full `SnapshotProducer`. They do not compose cleanly; standalone is the honest shape, like
      `replace_partitions`/`overwrite_files`).
- [x] `cherry_pick.rs`: store `snapshot_id`; resolve everything at commit/validate against the REFRESHED table.
      Three cases mirroring `cherrypick(long)` L69-141 with the FF-precedence of `apply()` L193-204:
      APPEND replay / OVERWRITE+replace-partitions replay / else FF-required — but FF (parent==head) takes
      precedence over replay for BOTH append and overwrite (`requireFastForward || isFastForward(base)`).
- [x] The `validate` hook (non-FF only, L161-171): `validateNonAncestor` (both variants),
      `validateReplacedPartitions` (ancestors-between walk since `picked.parent`), WAP re-check.
- [x] Tests (MemoryCatalog, grafted STAGED snapshots) + mutations. Docs: GAP_MATRIX cell, map.md row,
      this todo, lessons.
- Window pin: `validateReplacedPartitions` walks `ancestorsBetween(currentSnapshot, picked.parentId)` —
      starting id = `picked.parentId`, NOT the tx-captured start. The tx-captured `starting_snapshot_id` is
      **N/A for cherrypick's shape** (its concurrent window is defined by the picked snapshot's parent, not the
      transaction's read point) — stated explicitly in the doc comment + report.
- **Outcome (2026-06-11):** DONE. `transaction/cherry_pick.rs` (new) + mod.rs ctor + manage_snapshots doc
      pointer. 13 MemoryCatalog tests (builder), all green; 5 mutations run, every one caught (validateNonAncestor
      disable → ancestor+dedup fail; source-snapshot-id drop → dedup+happy fail; published-wap-id→wap.id swap →
      WAP-prop test fails; FF-precedence break → both FF snapshot-count pins fail; changed-partition
      over-broaden → negative control + happy replace fail). All version-sensitive strings bytecode-verified
      against 1.10.0 jars. Deferred: Java↔Rust interop (🟡), the stage-only WAP write path.
- **Reviewer (2026-06-11):** VERIFIED + 3 pins added → 16 tests. Precedence matrix confirmed cell-by-cell
      against Java `cherrypick(long)` L69-141 + `apply` L193-204 + `isFastForward` L173-182 (FF predicate
      provably equivalent; concurrent-head-moved cells already covered by the replay fixtures — append replays,
      delete fails, exactly as Java). Found + fixed THREE coverage gaps (each fail-before/pass-after):
      (1) the double-publish dedup SCOPE was unpinned — a mutation scanning ALL snapshots instead of the
      current ancestry passed every existing test (a dangling prior-publish would FALSELY block a re-publish);
      added `test_cherrypick_dangling_prior_publish_does_not_block_republish`. (2) the non-eligible-op
      fast-forward cell (delete with parent==head FF's verbatim, Java's else-branch accepts any FF-able op) was
      untested; added `test_cherrypick_delete_with_parent_equal_head_fast_forwards`. (3) the MULTI-SPEC replay
      divergence (Java preserves per-file specId + per-spec manifests and SUCCEEDS; Rust's
      `validate_added_data_files` requires the default spec ⇒ FAILS-LOUD non-retryably) was undocumented +
      untested; documented in the module header + added `test_cherrypick_multispec_replay_fails_loud`. Replay
      removed-side `copyWithoutStats` divergence is immaterial (the tombstone is rewritten from the live source
      manifest entry, not the picked copy). Gate clean: typos, fmt, clippy (workspace excl. sqllogictest,
      -D warnings), lib 1710 ×2 (baseline 1694 + 16). Tree restored, no commit.

## ACTIVE (2026-06-10 overnight): Phase-2 write-engine completion arc (branch `phase2/write-engine-completion`)

Session brief: `../FABLE_SESSION_BRIEF_2026-06-10_phase2-completion.md`. Actor-critic per increment
(Opus builder → Opus reviewer, orchestrator re-runs the gate + commits). One commit per increment,
pushed; merge nothing. Gate chained in ONE `&&` chain; Cargo files FROZEN.

**POST-ARC AUDIT (2026-06-10, orchestrator Fable — user-requested logic + security audit of
everything built):** full manual read of every production region the arc touched. TWO parity bugs
found, empirically pinned (fail-before), fixed: (1) merge_append's `first` was gated on
`added_snapshot_id == this snapshot` — Java's `first` is the unconditional stream HEAD
(ManifestMergeManager L85), so a properties-only merging append dropped the min-count protection
and over-merged; (2) duplicate `delete_manifest` args double-counted the replaced side of
`validateFilesCounts` (Java's field is a path-equality Set — now deduped at insertion). THREE
saturating-arithmetic hardenings on accumulators fed by untrusted `manifest_length` (bin weights
×2, rolling estimate ×1 — debug-build panic / release wrap on hostile values, now saturate).
Verified clean: u64 count accumulation (no overflow, stronger than Java's int), division-by-zero
guards, negative-length clamps, add_manifest None-count rejection == Java's null semantics,
kept-manifest integrity, no unsafe/no logging surface, eager-vs-commit error placement, retry
statelessness, release-mode debug_assert acceptable (unreachable via the only constructor).
Interop round-trip re-run GREEN post-fix. Lib 1694 ×2.

**ARC OUTCOME (2026-06-10, all six increments DONE — 6 commits on
`phase2/write-engine-completion`, each pushed, nothing merged):** RewriteManifests (8f2fc3a3) →
RewriteFiles seq-preservation + guard lift + validateNoNewDeletes (e96719e3) → the sibling
delete-manifest-carry corruption fix (fcf8da9d) → MergeAppend + bin-packing port (601eef30) → the
8-step Java-judged interop extension + delete-bearing rewrite fixture, ALL SIX comparisons green
with ZERO production changes (b140319a) → the stale-deferral correction + matrix cell-split repair
(2173feb3) + this Roadmap refresh. Lib suite 1643 → 1692 (+49); every increment
builder→reviewer→independent-gate; 20+ mutations run, every one caught (after two test fixes the
reviewers forced). Headline save: the arc surfaced and fixed a FOUR-action silent-corruption class
(delete manifests dropped from every delete-bearing commit on MoR tables — masked in rewrite_files
by the old guard, UNGUARDED in the three siblings). **Compaction triggers FIRED, not run (per the
brief — interactive-approval-only):** lessons.md at 985 lines / 93 KB (trigger ~800 / 50 KB),
todo.md at ~580 lines (guideline < ~500). Both need a compaction/archival pass next interactive
session.

- [x] **Increment 1 — `RewriteManifests`** (DONE 2026-06-10 — builder + reviewer + gate): new
      `transaction/rewrite_manifests.rs`, cluster/keep partition of current manifests, provenance-
      preserving re-group via the existing-entry writer path, `validateDeletedManifests` +
      `validateFilesCounts`, `Operation::Replace`, live set unchanged. Provenance re-stamp mutation
      pin mandatory. Done-bar 🟡 (interop in Increment 4).
      Outcome: 17 MemoryCatalog tests (14 builder + 3 reviewer pins: on-disk explicit seqs via raw
      avro, multi-spec cluster keying, user-set vs computed-count precedence); REVIEWER verdict NO
      BUG, zero production changes — the seq-strip RESURRECTION mutation fails the MoR scan test
      (the builder's weaker re-stamp mutation had only failed the metadata pin), and the builder's
      changed-partition-count divergence claim was CORRECTED (Java emits =0 too — parity; interop
      s6 must expect it both sides). Gate: lib 1660 ×2, clippy (workspace, excl. sqllogictest)
      clean, fmt+typos clean, Cargo FROZEN.
      BUILDER PLAN (2026-06-10):
      - [x] Read all required sources + Java `BaseRewriteManifests` (386 lines) fully.
      - [x] `table_properties.rs`: add 3 consts (target-size 8388608, min-count-to-merge 100,
            merge-enabled true). Only target-size consumed now.
      - [x] `snapshot.rs` (additive only): widen `new_filtering_manifest_writer` to `pub(crate)`,
            add `pub(crate) extend_snapshot_properties`, expose `snapshot_id()`.
      - [x] `rewrite_manifests.rs`: `RewriteManifestsAction` (cluster_by/rewrite_if/add_manifest/
            delete_manifest/set/set_commit_uuid/set_key_metadata). Commit: no-current-snapshot →
            DataInvalid; build producer; load ALL manifests (data+deletes); validateDeletedManifests;
            performRewrite (cluster) OR keepActiveManifests; validateFilesCounts; stamp added
            snapshot ids; compose new-first list; feed through SnapshotProduceOperation
            (Operation::Replace) + DefaultManifestProcess. Estimated-length size rolling.
      - [x] 13 tests + provenance re-stamp mutation pin (add_entry vs add_existing_entry).
      - [x] mod.rs wiring + map.md row + GAP_MATRIX cell flip.
      - Orchestrator design decisions (pre-briefed): the action pre-computes the full new manifest
        list and feeds it through `SnapshotProduceOperation::existing_manifest` with
        `DefaultManifestProcess` (no producer-trait change); Java's `writer.length()` size-rolling
        becomes a documented estimated-length proxy (Rust's `ManifestWriter` buffers entries — no
        incremental length); `add_manifest` is V2+ only (the V1 `copyManifest` legacy path is
        deferred, rejected `FeatureUnsupported`); new `TableProperties` consts for
        `commit.manifest.target-size-bytes` (+ merge siblings for Increment 3).
- [x] **Increment 2 — `RewriteFiles` dataSequenceNumber preservation + guard lift +
      `validateNoNewDeletes`** (DONE 2026-06-10 — builder + reviewer + gate): crown-jewel
      resurrection test (MoR table, EQUALITY-delete on X at seq 2, rewrite [X]→[X'] preserving
      seq 1 ⇒ scan still drops rows; mutation strips preservation ⇒ fails). Plus the
      tx-captured-start pin for the new validation.
      Outcome: REVIEWER verdict SHIP IT, zero production fixes needed beyond the builder's; +1
      reviewer structural pin (delete-manifest count survives the rewrite — fails under the
      carry-revert mutation, insensitive to seq-strip; disambiguates the two fixes). 6 mutations
      run, all caught incl. both ignore_equality_deletes directions + the shared-helper
      cross-consumer mutation (fails rewrite_files+overwrite_files+row_delta together). All-DELETED
      delete-manifest edge: Rust drop == Java drop (shouldKeep rule) — verified. Gate: lib 1670 ×2,
      21 rewrite_files tests, clippy/fmt/typos clean, Cargo FROZEN.
- [x] **Increment 2b — fix the SAME delete-manifest-dropping bug in the three sibling actions**
      (DONE 2026-06-10 — builder + reviewer + gate. Outcome: shared
      `SnapshotProducer::current_manifests()` helper, 4 consumers switched, orphaned
      `current_data_manifests` removed; 3 crown jewels + 3 structural pins; per-action
      carry-revert mutations prove isolation (each fails ONLY its own action's tests); REVIEWER
      ACCEPT — add-only overwrite path proven behavior-identical, dangling-delete retention
      documented conservative-safe. Gate: lib 1673 ×2, clippy/fmt/typos clean, Cargo FROZEN.
      Reviewer-flagged pre-existing: the OverwriteFiles GAP_MATRIX cell has an old mid-cell `||`
      breaking the table row — future docs pass.)
      (`delete_files.rs` L262 / `overwrite_files.rs` L701 / `replace_partitions.rs` L457 all
      return `current_data_manifests()` only — UNGUARDED silent delete loss on any MoR table;
      discovered while reviewing Increment 2; see lessons 2026-06-10). Carry ALL current manifests
      (the Increment-2 fix shape); per-action crown jewel (delete still applies post-commit) + the
      structural delete-manifest-count pin; carry-revert mutation per action.
      BUILDER PLAN (2026-06-10, Increment-2b builder):
      - [x] `snapshot.rs`: added `pub(crate) async fn current_manifests(&self) -> Result<Vec<ManifestFile>>`
            — loads the current snapshot's manifest list, returns ALL entries (data + deletes), empty when
            no current snapshot. Doc cites `MergingSnapshotProducer.apply` L973-1011 (composes BOTH
            `filterManager.filterManifests(dataManifests)` AND
            `deleteFilterManager.filterManifests(deleteManifests)`) + the resurrection corruption it
            prevents + the conservative dangling-delete posture (Java L982-993 `dropDeleteFilesOlderThan`
            / `removeDanglingDeletesFor` NOT ported — keeping a stale delete is harmless, dropping a live
            one resurrects rows).
      - [x] Switched `rewrite_files.rs`'s inline `existing_manifest` to the helper (behavior-preserving —
            its structural pin + crown jewel stayed green; merged its inline doc content into the helper;
            updated the structural-pin test's mutation note since `current_data_manifests` is gone).
      - [x] Switched `delete_files.rs` / `overwrite_files.rs` / `replace_partitions.rs` `existing_manifest`
            from `current_data_manifests()` → `current_manifests()` (each keeps a short action-specific
            comment: delete manifests carry unchanged + conservative dangling-delete posture).
      - [x] `current_data_manifests` was ORPHANED by the switch (its ONLY three code callers were the
            three broken actions) → REMOVED it (renamed-by-removal into `current_manifests`; dead-code
            rule). The two remaining textual refs were doc/comment prose, updated. NOTE: `rewrite_manifests
            .rs` (out of scope) has its own inline copy of the same "load full manifest list" logic with a
            LOCAL var named `current_manifests` — a 5th candidate consumer; FLAGGED, not touched.
      - [x] Tests per action (row_delta crown-jewel fixture: real parquet data + a REAL position-delete
            via the production writer + a production scan): X (partition a, position-delete masking y=20) +
            Y (partition b); the action; scan shows X's masked y=20 STILL ABSENT + the action's effect;
            structural pin (delete-manifest count == 1). All three named
            `test_*_preserves_outstanding_delete_manifests_no_resurrection`.
      - [x] Mutations: filtered each action's `existing_manifest` to DATA-only (the old data-only
            `current_data_manifests` behavior) — three separate, surgical (one block each, restored
            in-place) ⇒ THAT action's crown jewel fails (y=20 resurrected) + others stay green. Verified
            each: delete_files {10,20} vs {10}; overwrite_files {10,20,80} vs {10,80}; replace_partitions
            {10,20,80} vs {10,80}.
      - [x] Docs: GAP_MATRIX three action cells + the Phase-2 narrative line + map.md `snapshot.rs` row.
      Outcome: shared `current_manifests` helper carries DATA + DELETE forward; all four delete-bearing
      actions (rewrite_files + the three fixed) use it; `current_data_manifests` removed (orphaned). 3 new
      crown-jewel tests (1 per action) + structural pins, all green; three per-action mutations confirm
      per-action isolation (no accidental coupling). LESSON LEARNED: back up files AFTER tests land, then
      mutate the ONE production line surgically — restoring a whole-file pre-fix backup wiped the new test
      (recovered + re-applied). Done-bar 🟡 (unit-proven; interop with a delete-bearing fixture deferred).
      BUILDER PLAN (2026-06-10):
      - [x] `snapshot.rs` (producer, additive only): add field
            `new_data_files_data_sequence_number: Option<i64>` + builder setter
            `with_new_data_files_data_sequence_number(seq)` (mirror `with_added_delete_files`);
            consume in `write_added_manifest`: when `Some(seq)` and V2/V3, build each added entry with
            `.sequence_number(seq)` (writer keeps explicit data seq; file seq still inherits). V1
            ignored (no seqs). None default ⇒ every existing caller unaffected. NOT the shared helper.
      - [x] `rewrite_files.rs`: `data_sequence_number(seq: i64)` builder + thread to producer; REJECT
            `seq < 0` (`DataInvalid`) at commit (Rust-only fail-loud — writer silently strips negatives
            into re-inheritance). `validate_from_snapshot(snapshot_id)`. Implement `TransactionAction::
            validate`: when `deleted_data_files` non-empty, call shared
            `validate_no_new_deletes_for_data_files(current, effective_start, None,
            &self.deleted_data_files, self.data_sequence_number.is_some())`, `effective_start =
            validate_from_snapshot.or(tx_captured)`, UNCONDITIONAL. REMOVED the SAFETY GUARD
            (`has_outstanding_delete_files` + its commit rejection + the guard test). Rewrote the
            three doc sites (module doc, action-struct doc, mod.rs ctor doc) to the new contract.
            Ctor stays as-is (no 3-arg overload — builder suffices).
      - [x] **BUG FOUND + FIXED (latent, exposed by the guard lift):** `RewriteFilesOperation::
            existing_manifest` returned only DATA manifests, so a rewrite DROPPED every DELETE manifest
            and lost all outstanding deletes → resurrection regardless of seq. Fixed to carry ALL current
            manifests forward (data + deletes); `process_deletes` leaves delete manifests untouched
            (their entries are delete-file paths, never in the data `delete_paths`). The old guard had
            hidden this — no rewrite ever ran on a delete-bearing table. The crown jewel only goes green
            with this fix.
      - [x] Tests (10): crown-jewel eq-delete resurrection (real parquet + eq-delete writer + scan +
            raw-avro on-disk seq=1 pin + seq-strip mutation); no-preservation Java-faithful hazard;
            conflict-pair (eq-delete ignored WITH seq / rejected WITHOUT — exact msg + !retryable());
            new position delete always fatal; no-override tx-captured-start (+ refreshed-head mutation);
            disjoint negative control; pre-existing deletes not conflicts; no-concurrent-commit clean walk;
            negative-seq rejected. 20 rewrite_files tests total.
      - [x] Docs: map.md rewrite_files row, GAP_MATRIX RewriteFiles cell, this bullet outcome.
      Outcome: 20 rewrite_files lib tests green (8 pre-existing + 12 new/revised; the removed guard test
      replaced by the crown jewel + hazard pins). BOTH mandatory mutations run + restored: (1) seq-strip
      in `write_added_manifest` ⇒ crown jewel fails with y=20 resurrected; (2) refreshed-head
      `effective_start` ⇒ no-override test fails (commit wrongly succeeds). Done-bar 🟡 (unit-proven;
      interop in Increment 4 with a delete-bearing rewrite fixture).
- [x] **Increment 3 — merge append** (`MergeAppend` / `ManifestMergeManager` merge machinery) — DONE
      2026-06-10 (builder + reviewer ACCEPT + gate). REVIEWER: bin-packing port hand-traced against
      Java on 3 adversarial cases (packEnd double-reversal, `<=` weight boundary, lookback-1
      no-lookahead) — all match; read-back seq chain independently verified; 3 mutations re-run all
      caught; 1.10.0/manifests-* question RESOLVED for Increment 4 (the canonical view's
      SUMMARY_COUNT_KEYS allowlist excludes manifests-created/-kept/-replaced ⇒ s7 insensitive,
      no production/allowlist change needed; the /tmp Java ref is a tagless shallow clone — version-
      ancestry answers from it are artifacts). Gate: lib 1692 ×2. `merge_append()` action honoring
      `commit.manifest-merge.enabled` / `commit.manifest.min-count-to-merge` /
      `commit.manifest.target-size-bytes`; provenance preserved in merged manifests; passthrough
      below threshold / property-disabled. Done-bar 🟡 (interop in Increment 4).
      BUILDER PLAN (2026-06-10, Increment-3 builder):
      - [x] `snapshot.rs` seam: changed `ManifestProcess::process_manifests` → async + `Result` +
            `&mut SnapshotProducer` (same `impl Future + Send` style as `SnapshotProduceOperation`).
            `DefaultManifestProcess` stays a passthrough (fast_append byte-identical); single
            `manifest_file()` call site now `.await?`. NOTHING else changed in snapshot.rs.
      - [x] New `transaction/merge_append.rs`: `MergeAppendAction` mirroring `FastAppendAction`
            surface (add_data_files / set_commit_uuid / set_key_metadata / set_snapshot_properties /
            with_check_duplicate + validate_added_data_files + validate_duplicate_files). Op =
            `Operation::Append`; `existing_manifest` mirrors append.rs. `MergeManifestProcess`:
            split DATA vs DELETE; DELETES carried UNCHANGED (deferred); reorder DATA [new added FIRST
            via added_snapshot_id == producer.snapshot_id, then existing]; group by spec id
            REVERSE-sorted; packEnd by manifest_length; three bin rules via pure `bin_disposition`;
            three-way routing (this-snapshot DELETED → add_delete_entry [unreachable]; this-snapshot
            ADDED → add_entry; else → add_existing_entry); output merged-data then delete manifests.
      - [x] private `bin_packing` module (ported BinPacking.PackingIterator + ListPacker.packEnd,
            general lookback/largest-bin-first/max-items) + 7 unit tests.
      - [x] `mod.rs`: `merge_append()` ctor + docs (3 properties + fast_append/newAppend contrast).
      - [x] Tests (19, MemoryCatalog, mirror rewrite_manifests fixtures): below-min-count passthrough,
            at-threshold merge w/ provenance (raw-avro on-disk seqs + summary-shape), property-disabled
            passthrough, old-tombstone suppression (live+tombstone in one manifest so it reaches the
            merge), multi-spec separation (higher-spec-first), tiny-target size-1-keep, MoR
            delete-carry crown jewel, cumulative totals, empty-reject, 3 `bin_disposition` units, 7
            `pack_end`/`pack` units. Mutations run+restored: provenance re-stamp (→ provenance test
            fails), tombstone-suppression broaden (→ suppression test fails), bin-gate broaden (→
            bin_disposition unit fails).
      - [x] Docs: map.md merge_append row + snapshot.rs ManifestProcess seam note; GAP_MATRIX ❌→🟡.
      Outcome: 19 merge_append tests green (5x stable — replaced a FLAKY length-arithmetic bin test
      with a deterministic 1-byte-target size-1 pin + pure `bin_disposition` unit tests; manifest avro
      length varies a few bytes/commit so `target = 2*one_len` was non-deterministic). Seam change kept
      fast_append byte-identical (337 transaction tests green). PHYSICS verified: new added manifest
      reads back with seq=-1, Added entries inherit Some(-1), add_entry strips to None ⇒ re-inherits.
      SUMMARY-SHAPE finding: Java's MergingSnapshotProducer adds manifests-created/-kept/-replaced;
      Rust's SnapshotProducer.summary + MergeManifestProcess do NOT ⇒ merge_append == fast_append shape.
      PHYSICS VERIFIED: the new added manifest read back has ManifestFile.sequence_number == -1;
      its Added entries inherit seq=Some(-1) via `inherit_data` (status Added branch). `add_entry`
      then STRIPS Some(-1) (since -1 >= 0 is false) → writes seq=None on disk ⇒ re-inherits the new
      snapshot's real seq at commit. Carried (committed) entries have real Some(seq) ⇒
      `add_existing_entry` preserves them explicitly on disk. Merged manifest has
      added_snapshot_id == new_snapshot_id ⇒ `assign_sequence_numbers` stamps the real new seq.
      SUMMARY SHAPE: Java's MergingSnapshotProducer.apply adds manifests-created/-kept/-replaced via
      buildManifestCountSummary; the Rust SnapshotProducer.summary does NOT, and MergeManifestProcess
      will NOT inject them either ⇒ merge_append summary == fast_append summary shape (documented
      divergence from Java).
- [x] **Increment 4 — interop extension** (DONE 2026-06-10 — builder + reviewer VERIFIED + gate.
      REVIEWER: independent script re-run green; 2 NEW sensitivity mutations both caught (constant
      cluster key → per-partition vs single-manifest diff; dropped row_delta delete → the view
      visibly loses the delete manifest — the survives-claim is load-bearing); merging-producer +
      property-arming verified on both sides (post-s8 view = ONE merged manifest); A' seq==1 pin
      confirmed in the per-entry view field; the dangling-delete probe wording CORRECTED — the
      empirical KEEP on 1.10.0 is real but the mechanism is that 1.10.0 prunes only dangling DVs
      (PUFFIN-gated isDanglingDV) — parquet position-deletes are structurally exempt; BaseRewriteFiles
      overrides nothing dangling-related. Gate: lib 1692 ×2, all offline interop tests no-op green.):
      extended the
      E2 chain (`WriteActionsOracle` + `interop_write_actions_meta.rs`) with s6 rewrite_manifests
      (cluster by partition), s7 property-set (min-count-to-merge=2, NO snapshot) + s8 merge-append
      (Java `newAppend`), and a delete-bearing seq-preserving rewrite fixture B. ROUND-TRIP GREEN on
      the FIRST run (3 directions × 2 fixtures, ZERO production/canonicalization changes); 2 mutations
      verified the harness non-vacuous. GAP_MATRIX notes scoped, rows stay 🟡. Dangling-delete probe:
      1.10.0 keeps the dangling delete on a RewriteFiles = PARITY with Rust (documented).
      BUILDER PLAN (2026-06-10, Increment-4 builder):
      - [x] **A. Extend the E2 write-actions chain.** Java `WriteActionsOracle.generate` += s6
            `rewriteManifests().clusterBy(f -> String.valueOf(f.partition()))`, s7
            `updateProperties().set("commit.manifest.min-count-to-merge","2").commit()`, s8
            `newAppend().appendFile(G cat=a,60).commit()` (the MERGING producer). Rust GEN test mirrors:
            `rewrite_manifests().cluster_by(|f| format!("{:?}", f.partition()))`,
            `update_table_properties().set(min-count-to-merge=2)`, `merge_append().add_data_files([G])`.
            Document the chosen cluster-key fns on both sides (key string never appears in metadata —
            only the GROUPING must match). s7 produces NO snapshot — confirm the view is unaffected.
      - [x] **B. New delete-bearing rewrite fixture (fixture B, E1-family, metadata-only).** Java
            `RewriteSeqOracle`: fast-append A(a,10)+B(b,20) seq1 → row-delta adding a metadata-only
            POSITION-delete referencing B (seq2) → `newRewrite().validateFromSnapshot(rowDeltaSnap)
            .rewriteFiles(Set.of(A), Set.of(A'), 1L)` (dataSequenceNumber=1). Rust mirror: build the
            rewrite tx AFTER the row-delta commit (tx-captured start ⇒ empty concurrent window =
            semantic twin of Java's explicit validateFromSnapshot — DOCUMENT in both) with
            `.data_sequence_number(1)`. Two load-bearing assertions: A' carries data_seq 1 (not the
            rewrite snap's seq) post-inheritance; the delete manifest survives the rewrite intact.
            Delete references B (SURVIVOR) ⇒ Java dangling-delete machinery dormant both sides.
      - [x] **OPTIONAL probe:** a 2nd step rewriting B too (now-dangling delete) — EMPIRICALLY discover
            1.10.0 behavior vs Rust carry-unchanged. If divergent: do NOT force green; document
            (GAP_MATRIX + fixture comment) + leave it OUT of the byte-diffed chain. Report either way.
      - [x] **C. Wire-up:** extend `run-interop-write-actions.sh` to cover BOTH the extended chain AND
            fixture B in one run; extend the Rust env-gated tests (offline no-op early-return when the
            env var is unset). New Rust test goes in `interop_write_actions_meta.rs` (shares the view
            helper) gated on a fixture-B env var.
      - [x] Offline gate (typos/fmt/clippy/lib ×2/both interop binaries no-op). Round-trip green.
            Mandatory mutation (poison one Rust GEN value ⇒ comparison fails ⇒ restore ⇒ green).
            GAP_MATRIX three cells gain scoped "metadata-level interop ✅ 2026-06-10 (chain paths)"
            notes; rows STAY 🟡. map.md row updates (tests/ + java-interop/).
- [x] **Increment 5 (stretch) — RESCOPED: the deferral was STALE; matrix reconciliation instead**
      (DONE 2026-06-10, orchestrator, docs-only). The premise (`OverwriteFiles.validateDataFilesExist`
      wiring) does not exist in Java: `BaseOverwriteFiles.validate` (L135-175) has exactly three
      blocks, all already ported; `validateDataFilesExist` is RowDelta-only (single caller in core/,
      already landed 2026-06-09); concurrent-removal protection is `failMissingDeletePaths` ≡
      `resolve_delete_paths`. Building it would have been anti-parity — decision per the brief's
      "decide, document, move on".
      Outcome: (1) the stale deferral corrected in the OverwriteFiles GAP_MATRIX cell; (2) the
      2b-reviewer-flagged broken cell ROOT-CAUSED and repaired — the de-triplication mover had split
      the OverwriteFiles narrative MID-EXPRESSION on the `||` inside
      `(strict.eval(part) || metrics.eval(file))`, stranding 2.7 KB of narrative in the matrix as a
      phantom column while the archive section ended mid-sentence; strand rejoined VERBATIM in
      `archive/2026-06_matrix-cell-narratives.md` (conservation preserved), cell now terse + closed;
      (3) every matrix row pipe-count-audited (all exactly 5 `|`); (4) the three "five-commit chain"
      citations updated for the 8-step extension; (5) two lessons appended.

## DONE (2026-06-10): Sprint increment E2 — rewrite-family METADATA-level interop (branch `interop/write-actions-meta`)

All four rewrite-family actions proven in ONE five-commit chain (fast-append → DeleteFiles →
OverwriteFiles → ReplacePartitions → RewriteFiles) on a partitioned V2 table, through the E1
canonical-view oracle (no parquet — pure manifest metadata). **GREEN with ZERO Rust production
changes** — the Phase-2 ports already emit Java-identical metadata semantics.

- [x] Shared-module refactor: the E1 view builder → `tests/common/snapshot_meta_view.rs` (E1
      round-trip re-run green); allowlist += `replace-partitions` (both sides).
- [x] Manifest comparator extended with the count fields on BOTH sides — the first run surfaced a
      TIE: within one commit a rewritten (tombstone) manifest and an added manifest share
      (content, seq, min_seq) and the tie fell back to writer-dependent manifest-LIST order
      (order-insensitive re-comparison proved every hunk a pure swap — canonicalization, not
      semantics).
- [x] Java `WriteActionsOracle` (newFastAppend — NOT the merging newAppend — newDelete,
      newOverwrite, newReplacePartitions, newRewrite) + Rust GEN chain via the production actions
      + `run-interop-write-actions.sh` (Java byte-diff judge).
- [x] REVIEWER (Opus): APPROVE — chain faithfulness line-cited (DataFileSet path-equality =
      Rust's by-path resolution; FastAppend mirror correct); tie-extension exercised AND
      load-bearing in-fixture (non-total in general — flagged for future fanout fixtures);
      corrected an over-claim (Java does NOT enforce rewrite record-count conservation —
      `validateReplacedAndAddedFiles` checks non-emptiness only); 2 reviewer mutations caught
      (one-sided allowlist removal → both tests fail; delete-C-instead-of-B → legible cascade).

**Outcome:** 3 comparison directions green; s2 provenance (A tombstoned seq 1, B/C Existing
seq 1), s4 `replace-partitions=true` + C tombstoned, s5 `replace` with E tombstoned at seq 4 all
Java-identical. Poison mutation fails exactly the 2 comparison tests. Gate: lib 1643,
clippy/fmt/typos clean, both interop binaries no-op offline; Cargo FROZEN. GAP_MATRIX: the four
cells gain a SCOPED "metadata-level interop ✅ (explicit-API paths)" note — rows stay 🟡
(row-filter/conflict/multi-spec paths uncovered by the chain).

## DONE (2026-06-10): Sprint increment E1 — RowDelta METADATA-level interop (branch `interop/rowdelta-metadata`)

The snapshot/manifest SEMANTICS proof on top of the data-level scan-exec interop. Both sides emit a
CANONICAL "snapshot metadata view" (ordinal snapshots, COUNT-only summaries, manifest-list → entry
structure with POST-INHERITANCE sequence numbers, single-value-JSON partitions) over the three
EXISTING scan-exec fixtures; compared 3 ways per fixture.

- [x] Java `SnapshotMetaOracle` + `emit-snapshot-meta` mode (InteropOracle.java); explicit
      cross-language entry sort tuple.
- [x] Rust mirror `crates/iceberg/tests/interop_rowdelta_meta.rs` (env-gated, offline no-op).
- [x] `run-interop-rowdelta-meta.sh`: Java writes 3 tables + emits views; Rust writes 3 equivalents
      (existing GEN paths reused); Java emits + byte-DIFFS its view of each Rust table vs its own
      (Java judging Rust); Rust asserts its views of BOTH tables equal Java's.
- [x] **REAL parity bug found + fixed** (`spec/snapshot_summary.rs`): Rust omitted
      `changed-partition-count` from every summary (unpartitioned files never tracked; count
      emitted only-if-positive; `trust_partition_metrics` defaulted FALSE vs Java's trusted
      default). Fixed Java-faithfully (trust-gated count incl. 0, empty-partition tracked,
      `partition-summaries-included`, empty-key skip); 2 inconsistent test fixtures reconciled.
- [x] REVIEWER (Opus, actor-critic): canonicalization verified sound (null-vs-empty equality_ids
      symmetric; sort-tuple ties render identical; *-files-size exclusion correct); production fix
      line-cited exact vs Java `SnapshotSummary.java`; found 2 UNPINNED offline mutation axes
      (count only-if-positive; marker unconditional) and added the closing test; flagged the
      V1 sequence-number-tie ordinal limit (documented in both emitters).

**Outcome:** round-trip GREEN — 3 fixtures × {Java-reads-Rust byte-diff, Rust-reads-Java,
Rust-self} (9 comparisons). Mutations: pre-fix run failure = the disable-mutation; poisoned count
fails both Rust tests; reviewer's 2 mutations caught by the added test. Offline gate: lib **1643**,
datafusion 80+9 (doctest = documented pre-existing artifact), clippy/fmt/typos clean; Cargo FROZEN.
Offline write-path pin added (`fast_append` summary carries `changed-partition-count`).
DEFERRED: `file_sequence_number` in the view (no public accessor); V1-table ordinal tiebreaker.

## DONE (2026-06-10 overnight): OverwriteFiles validateNewDeletes branch A — Increment 2 of OVERNIGHT_BRIEF

Added the MISSING row-filter sub-branch of Java `BaseOverwriteFiles.validate`'s `validateNewDeletes`
(L168-172) — Rust previously had only branch B (`!deletedDataFiles.isEmpty()`). Builder→reviewer
actor-critic; orchestrator independently re-ran the gate + committed.

- [x] `snapshot.rs` — new `validate_deleted_data_files` (filter-based port of Java
      `MergingSnapshotProducer.validateDeletedDataFiles` L636-654; reuses `deleted_data_files_after(.., false)`
      + private `first_conflicting_file`; exact Java "Found conflicting deleted files that can contain records
      matching {filter}: {path}").
- [x] `overwrite_files.rs` — restructured the `validate_no_conflicting_deletes` block: branch A
      (`row_filter != AlwaysFalse` ⇒ `filter = conflict_detection_filter ?? row_filter` ⇒
      `validate_no_conflicting_added_delete_files` + `validate_deleted_data_files`) + branch B unchanged.
- [x] 7 `MemoryCatalog` tests (2 positives, the no-override tx-captured-start pin, flag-off control, 2
      row-filter-gate cases incl. the reviewer-added conflict-filter-only gap). Reviewer mutation-pinned each.

**Outcome:** Cargo FROZEN (0 dep changes). Independent gate green — typos/fmt/workspace-clippy clean,
`cargo test -p iceberg --lib` **1642 passed**, transaction:: 300 ×2. GAP_MATRIX `OverwriteFiles` row note
updated (stays 🟡 — data-level interop deferred). **2a (`validateAddedFilesMatchOverwriteFilter`) was already
done in PR #9; 2c (`RewriteFiles.validateNoNewDeletes`) is shadowed by the coarse `has_outstanding_delete_files`
guard (both `validate` + `commit` run on the refreshed base) so it cannot fire/be tested while that guard
stands — SKIPPED, see morning report.**

## NEXT (plan sketch, NOT built): `RewriteManifests` (Phase 2 write engine — the next Roadmap increment)

Increment 4 of OVERNIGHT_BRIEF was STRETCH; SKETCHED not built — it is new machinery (correctness-critical
manifest re-cluster with per-entry provenance preservation) that warrants its own focused builder→reviewer
cycle, not a rushed end-of-session pass. Java `BaseRewriteManifests` (`/tmp/iceberg-java-ref/core/.../BaseRewriteManifests.java`,
386 lines) extends `SnapshotProducer<RewriteManifests>` (NOT `MergingSnapshotProducer`) and produces an
`Operation::Replace` snapshot whose LIVE FILE SET IS UNCHANGED — only manifest grouping changes.

**Scope (one increment):**
- New action `transaction/rewrite_manifests.rs` (mirror `sort_order.rs` action shape + wire `mod.rs` + a `pub fn`
  ctor). Builder surface: `cluster_by(fn: DataFile -> key)` (Java `clusterBy`), `rewrite_if(pred: ManifestFile -> bool)`
  (Java `rewriteIf`, default all), `add_manifest`/`delete_manifest` (Java `addManifest`/`deleteManifest` — the
  explicit-replacement mode), `set(property,value)`.
- `apply` (Java L170-195): partition the current snapshot's data manifests into KEPT (predicate false, or no
  cluster fn) vs REWRITTEN (predicate true); `performRewrite` (Java L239-276) reads each rewritten manifest's
  ENTRIES and re-groups them by `cluster_by_func(entry.file())` into new manifests sized to
  `manifest.target-size-bytes`. **THE LOAD-BEARING INVARIANT: each re-written entry MUST keep its ORIGINAL
  `snapshot_id` + `data_sequence_number` + `file_sequence_number` + status** (Java copies the entry verbatim via
  the manifest writer's existing-entry path) — re-stamping is the silent-corruption class (resurrects/loses rows
  on the next merge-on-read scan). The live set (paths) is identical before/after.
- Validations: `validateDeletedManifests` (Java L284-302 — every `delete_manifest` must be a current manifest,
  not concurrently gone) + `validateFilesCounts` (Java L304-322 — total entry count across new manifests ==
  count across replaced manifests; the conservation guard).
- Producer support: the existing `snapshot.rs` `SnapshotProducer` writes manifests from `added_data_files`
  (fresh `Added` entries) — it does NOT currently re-emit EXISTING entries with preserved provenance for a
  cluster. Likely needs a new producer path "write these pre-built `ManifestEntry`s verbatim into N new
  manifests" (analogous to `rewrite_manifest_with_deletes` but clustering, not filtering). Scope this carefully;
  it may be the bulk of the increment.

**Tests (MemoryCatalog, no interop): the provenance pin is MANDATORY** (docs/testing.md write-action pin #2):
after a rewrite, assert each entry's `snapshot_id`/`data_sequence_number`/`file_sequence_number` == its
pre-rewrite value (mutation: re-stamp with the new snapshot id → the pin fails); the post-rewrite SCAN live set
== pre-rewrite (paths unchanged); `validateFilesCounts` fires on a count mismatch; `clusterBy` actually groups
(N input manifests → M output by key); `rewriteIf` keeps the predicate-false manifests untouched (byte-identical
ManifestFile). Add the cumulative-totals + provenance mutation pins. Done-bar 🟡 (unit; data-level interop later).

**Why deferred, not attempted:** the Rust `SnapshotProducer` is shaped around add/delete-file PATHS, not
entry-level manifest re-clustering with preserved provenance; getting that producer path right is the increment's
real risk and deserves a full actor-critic cycle rather than a tail-of-night rush. Pick this up first next session.

