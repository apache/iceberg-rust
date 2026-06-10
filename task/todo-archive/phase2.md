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
