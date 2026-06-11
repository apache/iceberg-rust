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

# Plan / Todo

The current plan for in-flight work. The operating manuals ([skills/](../skills/)) require this file
to be written **before** any non-trivial change and kept current as work proceeds.

How to use it (see the manuals' §1):

- Write a 3–7 bullet plan here before writing code.
- Flip `[ ]` → `[x]` as items complete; add a one-sentence "what changed and why" per step.
- Add indented sub-bullets when a step reveals unexpected complexity.
- Leave an `Outcome:` / `Done:` note when the work lands.

---


> **Archival log.** Last pass: 2026-06-11 (size trigger — 1,381 lines; pass 2) → [todo-archive/](todo-archive/) (phase1/phase2/phase3 + 2026-06_ops-hardening). 16 `##` sections: 2 kept live, 14 archived (9 → phase2, 2 → phase1, 1 → phase3, 2 → ops-hardening); the platform-cut-line paragraph lifted into Carried-forward. Prior pass: 2026-06-09 (size trigger — 4,344 lines) → [todo-archive/](todo-archive/) (phase1/phase2/phase3). Completed-increment narratives moved verbatim; this file keeps the active sprint + open items + archive pointers. Procedure: [skills/compaction.md](../skills/compaction.md) §Todo Archival. Archives are not read by default.

## DONE (2026-06-11): Lessons compaction pass 2 (branch `docs/lessons-compaction-pass-2`, user-approved)

- Trigger: SIZE — 1,369 lines / 128 KB on the settled post-#24 main (trigger ~800 / 50 KB).
- Tally: **42 entries → 17 KEEP / 25 ARCHIVE / 6 rules PROMOTED** (the 25-entry archive set and
  all six promotion diffs are byte-identical to the version presented for approval pre-merges;
  the KEEP set absorbed every 2026-06-10/11 arc entry — current work feeding the platform plan).
- Archive: `task/lessons-archive/2026-06_phase2-completion.md` (+ archive map row). Promotions:
  2 → docs/testing.md, 2 → dev/java-interop/map.md#debug, 1 → transaction/map.md#debug,
  1 → CLAUDE.md. Conservation: 42 == 17 + 25, no duplicates. Active file 583 lines.

## DONE (2026-06-11): Todo archival pass 2 (branch `docs/todo-archival-pass-2`, stacked on the lessons pass)

- Trigger: SIZE — 1,381 lines on the settled post-#24 main (target < ~500).
- 16 `##` sections: 2 KEEP live (the lessons-pass record + the pointer section), 14 ARCHIVE
  verbatim — 9 → phase2.md (the write-engine arc ×2 incl. the union-merge bare-header artifact,
  the DV arc, the overnight plan + morning report, Arc F, E1, E2, the OverwriteFiles branch-A
  increment, the superseded RewriteManifests sketch), 2 → phase1.md (Arc G + the closed
  carried-forward items), 1 → phase3.md (readable_metrics interop), 2 → the NEW
  `2026-06_ops-hardening.md` (increment D + the hardening meta-sprint — meta work, deliberately
  not phase-filed; deviation documented in that file's header).
- LIFTED (the sanctioned carve-out): the platform-cut-line open decision → Carried-forward below.
- Conservation: every pre-pass `##` heading in exactly one place; no checkbox flipped; no
  paraphrase. typos clean.
- Stale-box audit (the rule's verify-before-deciding step): 8 unticked `[ ]` boxes ride the
  archived sections, ALL stale-done, verified — Arc E (merged PR #22), Arc-E Inc 1 (#22), Arc F
  (#23), Arc G (#24), the morning report (delivered via the wrapper-root file), increment C
  (archival pass 1, 2026-06-09, recorded in the archival log), E / E3 (inspection interop
  COMPLETE per the GAP_MATRIX row). None surfaced as live work; preserved unflipped in context.

## ACTIVE (2026-06-11): Near-full-parity direction — next arcs (planning record)

Directive (user, 2026-06-11): table DataFusion/RePark; run this fork's Roadmap to **almost the
full 1:1 Java replacement**. Sequencing in Roadmap.md "Headline gap AREAS" (handoff-aware:
judgment-heavy → frontier window before 2026-06-22; templated breadth → Opus).

- [ ] **Next arc proposal (awaiting user green-light): Phase-2/3 closeout** — multi-spec writes
      (producer per-spec manifests; unlocks the documented default-spec-only divergences),
      the constants-map increment (reverted 2026-06-08; known latent type bugs; gated on
      datafusion + integration read tests), `removeRows` apply-side, the `dv_seq >= data_seq`
      index-validation residue.
- [ ] **Then: maintenance actions** (`ExpireSnapshots` first — the GC-safety judgment increment).
- [ ] **Scheduled with the user:** real-catalog (Glue + S3 Tables) hardening — needs credentials.
- [ ] **Opus-queue (post-handoff or parallel):** data-level write-action interop paydown,
      cherrypick interop + `stageOnly`, ORC/Avro breadth, view ops, incremental-scan interop.
## DONE (2026-06-11): Multi-spec writes — producer per-spec grouping (BUILDER, Group A, wt-closeout)

Goal: lift the Rust `SnapshotProducer` from DEFAULT-SPEC-ONLY to Java-parity PER-SPEC manifest groups.

- [x] **Producer grouping (snapshot.rs):** `write_added_manifests`/`write_added_delete_manifests` group
  `added_data_files` / `added_delete_files` by `partition_spec_id` (helper `group_files_by_spec`, spec-id
  DESCENDING) and write one manifest per (content × spec) via `new_cluster_manifest_writer(spec, content)`
  (generalized to take content; the two existing callers pass `Data`). The explicit-data-seq (RewriteFiles)
  + V1 snapshot-id stamping paths preserved. Removed the now-dead default-spec `new_manifest_writer` (which
  also carried a bare `.unwrap()`).
- [x] **Validation lift (snapshot.rs):** `validate_added_data_files`/`validate_added_delete_files` now check
  spec EXISTENCE via `partition_type_for_added_file` with Java's exact "Cannot find partition spec %s for
  {data,delete} file: %s"; partition-value compat against the FILE's own spec.
- [x] **Summary ripple:** `summary()` passes each file's own spec via `file_partition_spec(file)` (Java
  `addedFile(spec(file.specId()), file)`), not the default — the changed-partition-summaries fix.
- [x] **Cherrypick conversion:** `test_cherrypick_multispec_replay_fails_loud` →
  `test_cherrypick_multispec_replay_produces_per_spec_manifest` (replay SUCCEEDS, manifest stamped spec 0,
  scan correct); module-doc note rewritten to the per-spec parity contract.
- [x] **Tests:** 6 producer tests in `snapshot::multispec_tests` (two-spec data + delete manifests,
  unknown-spec data + delete rejection, wrong-spec-type, cumulative totals) + the cherrypick conversion.
  Renamed `row_delta::test_row_delta_rejects_partition_spec_mismatch` →
  `test_row_delta_rejects_unknown_partition_spec` (stale default-spec assertion fixed).
- [x] **Mutations:** grouping-revert (default-spec-only) ⇒ all 4 grouping tests fail (`zip_eq` tuple-arity
  panic = partition corruption); validation-revert (default fallback) ⇒ all 3 unknown-spec tests fail
  (door message gone). Both restored from /tmp/wtA_snapshot_pre_mutation.rs.
- [x] **Docs:** GAP_MATRIX (multi-op row + cherrypick cell), transaction/map.md, lessons.

**Outcome:** Producer is Java-parity per-spec. Verification: typos clean, fmt clean, clippy `-D warnings`
clean (workspace ex-sqllogictest), `cargo test -p iceberg --lib` 1804 passed ×2 (was 1798 baseline +6 new
−1 renamed... net 1798→1804 = +6 producer +6 unchanged... 1798+6=1804), `iceberg-datafusion` lib+integration
9/9 + write-path insert tests green. PRE-EXISTING unrelated failure flagged: an `iceberg-datafusion` DOCTEST
(`table_provider_factory.rs:41`) fails to compile (`#[tokio::main]` multi_thread w/o `rt-multi-thread`) — not
touched by this increment. Deferred (flagged): WRITER-LAYER spec threading; `OverwriteFiles::validate_added_files`
default-spec (Java's `dataSpec()` rejects multi-spec there anyway); multi-spec Java↔Rust interop. No commit.

**REVIEWER PASS (Group A, 2026-06-11, wt-closeout).** Verdict: APPROVE with two added pins.
- **THE MISSING SUMMARY PIN (point 1) — confirmed gap, fixed.** The builder shipped NO test that fails
  CLEANLY under a summary-collector revert. The summary-revert mutation only crashed the 3 arity-differing
  manifest tests via a `partition_to_path` index-out-of-bounds PANIC (the "lucky" version) — a same-arity
  different-NAME multi-spec commit would silently render the WRONG `partitions.{path}` key with NO panic.
  Added `test_fast_append_multispec_partition_summary_keys_use_file_spec` (spec0=`identity(x)`, spec1=
  `identity(y)` via a same-arity rename; both files partition value 5): asserts `partitions.x=5` present
  (NOT `partitions.y=5`-only) AND `changed-partition-count=2` (the default-spec bug collapses both onto
  `y=5` ⇒ 1). Fails CLEANLY under the summary-revert (asserted, not panic); passes on fixed. Verified Java
  `SnapshotSummary.Builder.addedFile(spec(file.specId()), file)` → `updatePartitions` → `partitionToPath`
  uses the FILE's spec (1.10.0 bytecode).
- **V1 multi-spec (point 4) — probed, WORKS.** Added `test_v1_fast_append_two_specs_produces_per_spec_data_manifests`:
  a V1 two-spec DATA append produces one V1 manifest per spec (not fail-loud) — Java parity.
- **Mutations re-run (point 6):** grouping-revert ⇒ 3 manifest tests fail (zip_eq); validation-revert ⇒ 3
  unknown-spec tests fail (door message gone, deeper failure confirms defense-in-depth); cherrypick
  default-spec-stamp ⇒ conversion test fails (zip_eq) — pins per-spec, not just success. NEW reviewer
  mutation: `group_files_by_spec` file-LOSS (truncate to 1 group) ⇒ caught by the two-spec manifest tests
  (count + per-file presence). NOTE: `test_fast_append_multispec_cumulative_totals` does NOT catch file-loss
  — `added-data-files`/`total-data-files` come from `added_data_files` BEFORE grouping, so its docstring
  ("a dropped spec group would under-count") slightly overclaims; the manifest tests are the real loss guard.
- **Ordering (point 2) — FLAG for future interop (view NOT changed).** `snapshot_meta_view.rs` manifest sort
  tuple (L113) is `(content_rank, seq, min_seq, 6×counts)` and does NOT include `partition_spec_id`; the
  emitted manifest JSON also omits it. Two same-content/same-seq/same-counts manifests of DIFFERENT specs
  TIE on the whole tuple ⇒ array order falls back to manifest-LIST position (Rust spec-descending vs Java
  HashMap order). NO current interop fixture is multi-spec single-commit, so nothing is broken today; a
  FUTURE multi-spec interop fixture must either add spec id to the comparator tuple or assert the manifest
  SET (order-insensitively).
- **Pre-existing, untouched:** `validate_partition_value` has two near-duplicate messages (L843 "...not
  compatible WITH partition type" arity branch vs L859 "...not compatible partition type" per-field branch);
  both present in HEAD, the increment's test asserts the variant it triggers. Cosmetic; out of scope.
- Stale cherry_pick.rs banner comment (L1442 "fail-loud divergence") corrected to the converted contract.
  GAP_MATRIX/todo test count 6→8. Gate clean: typos, fmt, clippy -D warnings (workspace ex-sqllogictest),
  `cargo test -p iceberg --lib` 1806 ×2, `iceberg-datafusion` lib 80 + integration 9. Tree clean, no commit.

## IN PROGRESS (2026-06-11): Identity-partition constants-map ACTIVATION (BUILDER, Group A, increment 2, wt-closeout)

Goal: re-thread `task.partition_spec` and ACTIVATE the arrow reader's identity-partition constant
materialization (Java `PartitionUtil.constantsMap`), fixing the two transformer bugs that caused the
2026-06-08 revert. The decisive gate is `cargo test -p iceberg-datafusion` (lib + integration).

- [x] **Bug (a) — REE leak.** Constant identity-partition columns were materialized as `RunEndEncoded`
  (via `datum_to_arrow_type_with_ree`), so the output batch schema declared REE where the projected scan
  schema says plain `Utf8`/`Int64` ("expected Utf8 but found RunEndEncoded", `test_insert_into_partitioned`).
  FIX: materialize identity-partition constants as PLAIN arrays whose Arrow type equals the field's declared
  scan-schema type — the output batch schema now equals the declared scan schema EXACTLY. (Java's
  `constantsMap` is type-agnostic about Arrow encoding; REE was a Rust-only storage optimization that broke
  the schema contract. _file metadata + initial_default still use the existing REE path — unchanged.)
- [x] **Bug (b) — int->long widening.** A partition literal stored as `Int(i32)` could not materialize into
  an `Int64`/`Long` column ("Unsupported constant type combination: Int64 with Some(Int(19))",
  `test_evolved_schema`). FIX: derive the constant's value from the FIELD's iceberg type via
  `Datum::to(&field.field_type)` — the canonical Iceberg coercion (mirrors Java
  `IdentityPartitionConverters.convertConstant(partitionType.field(pos).type(), value)`): it widens
  `Int->Long`, `Int->Date`, `Long->Timestamp/Timestamptz`, passes through equal types, audited matrix.
- [x] **Threading.** `create_manifest_file_context` already resolves the manifest's spec (Arc) for the
  residual; thread that Arc onto `ManifestFileContext` -> `ManifestEntryContext` -> `FileScanTask.partition_spec`
  (once per manifest). Reader activation site (reader.rs:451) already consumes it — was dormant only because
  the field was `None`.
- [x] **Multi-spec interaction (sits on increment 1).** Each task's spec comes from ITS manifest's
  `partition_spec_id`; a multi-spec scan materializes each file's constants under its OWN spec. Tested.
- [x] **Tests:** transformer unit pins for both bug classes (REE-leak schema-equality + int->long); a
  metadata-vs-file-value scan test (file value DIFFERS from partition value -> scan returns PARTITION value);
  multi-spec scan test; bucket/truncate negative control (NOT materialized); null-partition-value case.
- [x] **Mutations:** disable activation -> metadata-vs-file-value test fails (reads file value); break the
  widening coercion -> int->long pin fails.
- [x] **Gate:** `cargo test -p iceberg-datafusion` (lib + integration incl. `test_insert_into_partitioned`,
  `test_evolved_schema`) run EARLY and often.
- [x] **Docs:** GAP_MATRIX residual/constants-map row, scan/map.md, lessons, this file.

## DONE (2026-06-11): Multi-spec closeout 3 — `removeRows` apply-side + dv_seq validation (BUILDER, Group A, increment 3, wt-closeout)

Goal: land the two residue items left by the merge-on-read arc — `RowDelta::removeRows` apply-side data
removal (was validation-only) and the `dv_seq >= data_seq` validation (was deferred for the infallible
index signature).

- [x] **Item 1 — `removeRows` apply-side (row_delta.rs):** `RowDeltaOperation` gained
  `removed_data_file_paths`; `delete_files()` resolves them via the shared `SnapshotProducer::resolve_delete_paths`
  EXACTLY as `OverwriteFilesOperation::delete_files` does, so the producer's existing `commit()` routing
  (`removed_data_files` → `process_deletes` rewrite + summary `remove_file`) drops the file from the scan in
  the SAME row-delta snapshot. NO snapshot.rs change needed — the producer machinery already routes
  `delete_files()` through the rewrite + summary; only the operation's seam was empty. `operation()`
  CONFIRMED unaffected: the 1.10.0 two-branch `addsDeleteFiles && !addsDataFiles ⇒ Delete; else Overwrite`
  consults neither `deletesDataFiles()` nor the removal set — a remove+add-delete and a remove-only row
  delta are both Overwrite. The removed∩referenced rejection fires FIRST (in `validate()`, which `do_commit`
  runs for ALL actions before any `commit()`).
- [x] **Item 1 docs flipped:** EVERY "validation-only / deferred" surface in row_delta.rs (module doc,
  "Out of scope", the `removed_data_files` field doc, the `remove_data_files`/`remove_rows` method docs, the
  `removed_delete_files` contrast, the `remove_deletes` contrast, 2 test comments). NO rename — Java's
  `removeRows` is already mirrored by `remove_rows`; `remove_data_files` is the bulk primitive (kept).
- [x] **Item 1 tests (5):** drops-from-scan e2e (remove A + add delete for B ⇒ scan {B}, A tombstoned,
  DELETE manifest present); remove-only ⇒ Overwrite + drops A; missing-path fail-loud + no partial add;
  ordering pin (removed∩referenced rejects before apply-side removal, table untouched); summary counters
  (deleted-data-files/deleted-records appear, cumulative total-data-files/total-records pin). MUTATION:
  sever `delete_files` → `Ok(vec![])` ⇒ 4 tests fail (scan shows A, remove-only empty-commit, missing-path
  silent), the ordering test correctly STILL passes (validate-time rejection independent of routing).
- [x] **Item 2 — dv_seq validation (delete_file_index.rs):** made the index FALLIBLE (`get_deletes_for_data_file`
  → `Result<Vec<…>>`). PLACEMENT JUSTIFIED: the index is the ONLY place both sequence numbers are in hand
  (`seq_num` = the data file's, the DV's via its manifest entry); the caching-loader door (the duplicate-DV
  door's home) receives NEITHER — `FileScanTaskDeleteFile` drops the sequence number in its
  `From<&DeleteFileContext>` conversion, so candidate (a) would need to thread two new seqs through a public
  serialized struct + the loader. Ripple of (b) was SMALL: one production caller (`scan/context.rs:144`,
  already `Result`-returning, just added `?`). The check fires `dv_seq < data_seq` ⇒ the EXACT 1.10.0 message
  (bytecode-verified against `iceberg-core-1.10.0.jar`): "DV data sequence number (%s) must be greater than
  or equal to data file sequence number (%s)".
- [x] **Item 2 tests:** invalid-table (hand-built DV at seq 5 vs data file seq 9 ⇒ loud DataInvalid naming
  both seqs); the prior `test_dv_is_not_sequence_filtered` SPLIT — the valid boundary half kept
  (`dv_seq==data_seq` / `dv_seq>data_seq` apply the DV) + the invalid half is the new test. MUTATION: disable
  the check (`&& false`) ⇒ the invalid-table test sees silent `Ok(vec![dv])` instead of the error.
- [x] **Docs:** GAP_MATRIX (RowDelta row residue flip + Read row dv_seq residue flip), transaction/map.md,
  this file, lessons.

**Outcome:** both residue items landed. Verification: typos clean (reworded a parenthesized prefix to dodge a
false positive), fmt clean, clippy `-D warnings` clean (workspace ex-sqllogictest), `cargo test -p iceberg --lib`
**1818 passed ×2** (baseline 1812 + 5 row_delta + 1 net delete_file_index split), `iceberg-datafusion` lib
80 + 9 green. PRE-EXISTING unrelated failure flagged: the `iceberg-datafusion` DOCTEST
(`table_provider_factory.rs:41`, `#[tokio::main]` multi_thread w/o `rt-multi-thread`) — not touched (no
datafusion files changed). Files changed: row_delta.rs, delete_file_index.rs, scan/context.rs (the `?` —
flagged as the item-2 placement consequence), transaction/map.md, GAP_MATRIX.md, todo.md, lessons.md.
Deferred (flagged): multi-spec delete commits + full conflict-validation interop (RowDelta stays 🟡); the
manifest-comparator multi-spec tie (from increment 1). No commit.

**REVIEWER PASS (Group A, increment 3, 2026-06-11, wt-closeout). Verdict: APPROVE with one added doc + one added pin.**
- **Point 1 (fallibility ripple) — VERIFIED.** Grepped every caller: the async `DeleteFileIndex::get_deletes_for_data_file`
  threads the inner `Result` at BOTH populated call sites (L115/L125); the sole production caller `scan/context.rs:146`
  adds `?`. Traced the error END-TO-END: `into_file_scan_task`(`?`) → `process_data_manifest_entry`(`?`, mod.rs:706)
  → `try_for_each_concurrent` short-circuits → `Err` sent into `file_scan_task_rx` (mod.rs:610) ⇒ the scan stream
  yields the `DataInvalid` as a LOUD item, NOT swallowed into an empty delete set nor a dropped task. Message + arg
  order BYTECODE-verified (`javap -c DeleteFileIndex.findDV`, 1.10.0): slot-0 `%s` = `dv.dataSequenceNumber()` (DV
  FIRST), slot-1 = `seq` (data file); comparison `lcmp; iflt` ⇒ check is `dv_seq >= seq` (boundary `==` VALID) —
  Rust's `dv_seq < data_seq ⇒ Err` is the exact complement.
- **Point 2 (ordering) — VERIFIED, wording accurate.** Read `transaction/mod.rs::do_commit`: structure is
  ALL-validates-then-ALL-commits (loop 1 L374-378 runs every action's `validate`; loop 2 L380-389 runs every
  `commit`). The doc/test/lessons wording ("`do_commit` runs `validate()` for ALL actions before any `commit()`")
  matches this exactly — no overstatement. The ordering test correctly STILL PASSES under mutation A (validate-time
  gate, apply-path-independent). Re-ran: green.
- **Point 3a (failMissingDeletePaths posture) — REAL DOC GAP, FIXED.** Bytecode (`javap -c BaseRowDelta`):
  `removeRows` = `removedDataFiles.add(file)` + `delete(file)` (no LIVE check beyond delete, matches builder); the
  ctor does NOT set `failMissingDeletePaths`, and the only `failMissingDeletePaths()` call sits in `validate()` behind
  `if (validateDeletes)` (gates the UNRELATED `validateDataFilesExist` walk). `StreamingDelete`(1)/`BaseOverwriteFiles`(2)
  DO call it. ⇒ Rust's `resolve_delete_paths` unconditional fail-loud is Java-faithful for DeleteFiles/OverwriteFiles
  but STRICTER than Java's `RowDelta` default for the NEW `removeRows` caller — and the docs cited `failMissingDeletePaths`
  as if it were parity. ADDED the divergence note (module-doc apply-side block in row_delta.rs + the shared
  `resolve_delete_paths` doc in snapshot.rs, mirroring the Arc-E `removeDeletes` posture note on `resolve_delete_file_paths`).
- **Point 3b (replace-in-place) — PROBED, sensible, PINNED.** Added `test_row_delta_remove_and_add_same_path_replaces_in_place`:
  remove X + add a fresh file at the SAME path X ⇒ old entry tombstoned (Deleted), new Added, X stays live, summary
  counts both (deleted=1, added=1, cumulative total=1). Matches Java's `removeRows(X)`-tombstones + `addRows(X)`-adds.
  No silent weirdness. Kept as a permanent pin (+1 test; matrix count 5→6).
- **Point 4 (mutations) — all 3 confirmed.** (A) sever `RowDeltaOperation::delete_files`→`Ok(vec![])` ⇒ exactly 4
  removeRows tests fail (drops-from-scan, remove-only, missing-path, summary), ordering test correctly STILL passes.
  (B) disable dv_seq check (`&& false`) ⇒ `test_dv_lower_seq_than_data_file_is_invalid_table` fails (silent Ok),
  valid-boundary still passes. (C, mine) change `>=`→`>` (`<`→`<=`) ⇒ `test_dv_is_not_sequence_filtered_at_valid_boundary`
  fails at `dv_seq==data_seq` (boundary pinned from BOTH sides), invalid test still passes. All restored from /tmp/wtA3_rev_*.bak.
- **Gate:** typos clean, fmt clean, clippy `-D warnings` clean (workspace ex-sqllogictest), `cargo test -p iceberg --lib`
  **1819 ×2** (1818 + my replace-in-place pin), `iceberg-datafusion` lib 80 + integration 9 green; the
  `table_provider_factory.rs:41` DOCTEST failure CONFIRMED pre-existing + unrelated (no datafusion files changed).
  Pipe audit CLEAN. Files added to the changed set by the reviewer: `transaction/snapshot.rs` (the `resolve_delete_paths`
  posture note). Tree clean, no commit.
## ACTIVE (2026-06-11): ExpireSnapshots Increment B1 — METADATA retention semantics (worktree wt-expire, BUILDER Fable, Group B)

Java `RemoveSnapshots` retention computation, metadata-only (`cleanExpiredFiles(false)` semantics).
**B1 is METADATA-ONLY: no file deletion of any kind — file cleanup is Increment B2, not this one.**

- [x] `transaction/expire_snapshots.rs` (new): `ExpireSnapshotsAction` — `expire_older_than(ts)`,
      `retain_last(n)` (deferred-to-commit validation, Java's exact message), `expire_snapshot_id(id)`
      repeatable; commit-time retention computation against the refreshed table (1.10.0
      `internalApply`, bytecode-verified): retained refs (main never ref-expired; `now − ts <=
      maxRefAgeMs` retains; missing-snapshot refs dropped) → explicit-id-vs-retained-ref precondition
      ("Cannot expire %s. Still referenced by refs: %s") → per-branch contiguous-prefix retention
      (`kept < minToKeep || ts >= cutoff`, EARLY STOP) → unreferenced retention (`ts >=
      defaultExpireOlderThan`) → emit `RemoveSnapshotRef` (expired refs, sorted) +
      `RemoveSnapshots` (sorted ids) + `RefSnapshotIdMatch` guards for every ref consulted.
      GC gate (`gc.enabled`, Java ctor message verbatim) at commit. No-op emits nothing.
- [x] `spec/table_properties.rs`: `history.expire.max-snapshot-age-ms` (default 432000000),
      `history.expire.min-snapshots-to-keep` (default 1), `history.expire.max-ref-age-ms`
      (default i64::MAX) — all three bytecode-verified vs 1.10.0; plus `gc.enabled` (default true,
      needed by the GC gate — flagged as the 4th const).
- [x] `spec/table_metadata_builder.rs` — complete `remove_snapshots` vs Java 1.10.0
      `rewriteSnapshotsInternal`: prune statistics + partition statistics per removed id (with
      changes), and remove dangling refs via `remove_ref` semantics (pushes `RemoveSnapshotRef`,
      resets `current_snapshot_id` for main) instead of the silent `refs.retain`. (catalog/mod.rs
      apply routing already correct — no edit needed there.)
- [x] Wire `Transaction::expire_snapshots()` in `transaction/mod.rs` + `transaction/map.md` row.
- [x] Tests both directions of every boundary (age boundary ON the cutoff, retain_last n/1/n>len,
      per-ref overrides both ways, tag expiry removed/kept, main never expired, explicit-id error +
      seed-set semantics, ancestors survive, unreferenced boundary, no-op, idempotent re-run,
      snapshot-log prune, stats prune, dangling-main, GC gate, ref guards + stale-guard conflict,
      end-to-end memory-catalog commit). Then the 4 mutations (age-boundary flip, min-floor drop,
      ancestor-retention drop, tag-expiry invert) + restore + full suite ×2.
- [x] Docs: GAP_MATRIX `ExpireSnapshots` ❌→🟡 (metadata semantics landed; file cleanup = B2;
      interop deferred), map.md row, lessons.

Deferred loudly: B2 file cleanup (`cleanExpiredFiles`, delete callbacks, incremental-cleanup
strategy selection), `cleanExpiredMetadata` (spec/schema GC — needs manifest IO), interop.

Outcome (2026-06-11): landed in one increment. 32 action tests + 2 builder tests (1832 lib total,
×2 green). 5 builder mutations all caught (branch-walk `>=`→`>`; unreferenced `>=`→`>`; min-floor
drop; ancestor-retention drop; ref-age-expiry invert + the M5 builder revert proving the apply-side
tests fail pre-fix). `catalog/mod.rs` needed NO edit (apply routing was already correct — the
completeness gap was in the builder). One extra const beyond the planned three: `gc.enabled`
(needed by the GC gate; same file, flagged). `tracing` is NOT an iceberg-crate dep, so the
invalid-ref drop is silent with a comment (Java WARN-logs) — no Cargo edit made.

Review (2026-06-11, REVIEWER Fable): re-derived `internalApply` / `computeRetainedRefs` /
`computeBranchSnapshotsToRetain` / `unreferencedSnapshotsToRetain` / `rewriteSnapshotsInternal` /
`removeRef` / `removeStatistics` from 1.10.0 bytecode — semantics, messages, defaults, and change
recording all confirmed; M5 revert re-run (exactly the 2 builder tests catch it);
requirements-drop mutation caught by the stale-guard + requirements-shape tests. THREE survivor
mutations found and pinned (+5 tests, 1 extension): the branch-walk EARLY STOP under clock-skewed
timestamps (a newer-than-cutoff ancestor behind the stop point IS expired — previously removable
with the whole suite green), the ref-age `<=` boundary (age == maxRefAgeMs retained), and the
refs-first update emission order (Java's change order; the apply-side dangling sweep self-heals,
so only a shape pin catches a reorder). Also pinned: explicit id == main's head errors with
`[main]`; a mid-ancestry explicit-id hole clears the snapshot log at the hole on apply; a branch's
own `max_snapshot_age_ms` beats even the EXPLICIT `expire_older_than` (Java: `expireOlderThan`
only overwrites the default). Known acceptable gap (Java's REST posture shares it): a ref CREATED
concurrently at a to-be-expired snapshot is not guardable via `RefSnapshotIdMatch`; the apply-side
sweep then drops it — full-CAS catalogs (Java's primary path) reject it; revisit at B2.

## ACTIVE (2026-06-11): ExpireSnapshots Increment B2 — FILE CLEANUP (worktree wt-expire, BUILDER Fable, Group B)

Port Java 1.10.0 `ReachableFileCleanup` (the general-correct strategy) as a post-commit cleanup
seam. **THE most dangerous increment: it deletes files. Every choice biases under-deletion; every
test pins the deletion set BOTH directions.**

- [x] `transaction/expire_cleanup.rs` (new): `ExpireSnapshotsCleanup` (FileIO + injectable
      delete fn) with `clean_expired_files(before, after) -> CleanupReport` (the two-state core,
      1.10.0 `ReachableFileCleanup.cleanFiles` bytecode-rederived) and
      `commit_and_clean(tx, catalog)` (the commit-THEN-clean wrapper; deletion structurally
      unreachable on a failed commit — Java `RemoveSnapshots.commit()` ordering). GC gate
      re-honored at the cleanup door (Java's is in the ctor, which also covers cleanup).
- [x] Set algebra (bytecode-cited): expired = before.snapshots − after.snapshots (by id);
      manifest-lists of expired snapshots (RUST SAFETY DIVERGENCE: minus retained lists — Java
      deletes unconditionally, unreachable case for Java-written tables); candidate manifests =
      ∪ expired lists' entries; retained = ∪ after-snapshots' lists' entries (path equality —
      `GenericManifestFile.equals` is manifestPath-only); manifests-to-delete = candidates −
      retained; content files = ∪ LIVE entries (status != DELETED — `isLiveEntry`) of
      manifests-to-delete, minus ∪ LIVE entries of retained manifests (BOTH data + delete
      manifests — the 1.10.0 cleanup projection omits `content` and the avro ctor defaults DATA,
      so Java walks both identically; DV puffin path dedup via path-set semantics); stats =
      before-locations − after-locations.
- [x] Failure posture (divergence from Java's log-and-continue, no-logging-dep constraint):
      manifest-LIST read errors → hard Err BEFORE any deletion (Java throwFailureWhenFinished);
      candidate-manifest read error → collect + skip its files; retained-manifest read error →
      collect + CLEAR the whole content-file set (Java catch-Throwable→empty, fail-safe);
      per-file delete errors → collect + continue. `CleanupReport {deleted_* per funnel,
      failures: Vec<CleanupFailure {path, kind, error}>}`.
- [x] Tests (15, each class both directions): grafted shared manifest-list survives (the pinned
      Rust divergence); carried-forward shared manifest SURVIVES + expired list dies (the #1
      pin); rewritten-but-live data file survives (rewrite_manifests); expired-only data file
      dies + retained tombstone does NOT protect (delete_files chain); shared puffin survives
      via cross-manifest carried-EXISTING DV + replaced puffin dies (NOTE: the planned
      two-DVs-one-puffin-remove-one shape is unbuildable — delete-file removal is BY PATH in
      Java too, 1.10.0 `ManifestFilterManager.delete` adds `file.location()` to `deletePaths`,
      so removing one DV tombstones every same-path entry; fixture reshaped, finding recorded
      in lessons); expired-only DV puffin dies; stats file dies / retained stats survives;
      failed-commit ⇒ zero deletes (MockCatalog + recorder); injected failing delete → failure
      listed, sweep continues; dry-run by injection (storage untouched); unreadable retained
      manifest → ALL content files spared; unreadable candidate manifest → its files skipped,
      manifest still dies; unreadable manifest list → Err before any deletion; empty-expiry
      no-op; GC gate refused with Java's message.
- [x] Mutations (`wtB2_*`): M1 drop the `!` in the manifest subtraction → 9 tests fail,
      headlined by the carried-forward pin ("the SHARED manifest must survive: [...m0.avro]" —
      the data-loss class); M2 `if false` the retained-files subtraction in (c) → 3 fail
      (rewritten-but-live "the still-live data file must NOT die", shared-puffin,
      unreadable-retained); M3 cleanup-not-gated-on-commit-success (fabricated post-state on
      Err) → failed-commit pin fails ("the failed commit must propagate" + recorder
      non-empty). Snapshot-copied before each, restored surgically, full suite green after.
- [x] Docs: GAP_MATRIX ExpireSnapshots row (B2 landed; Incremental deferred with the
      optimization-with-stricter-eligibility rationale; interop deferred), transaction/map.md
      `expire_cleanup.rs` row, expire_snapshots.rs + mod.rs module-doc pointers, lessons.
- [x] Gate: typos clean; fmt clean; clippy workspace -D warnings (excl. sqllogictest) clean;
      `cargo test -p iceberg --lib` ×2 — 1847 passed (baseline 1832 + 15 new).

Outcome (2026-06-11): B2 landed in one increment — `ReachableFileCleanup` semantics ported as
the explicit post-commit `ExpireSnapshotsCleanup` seam (Java's `cleanExpiredFiles(true)` default
deliberately NOT mirrored: deletion is opt-in via `commit_and_clean`/`clean_expired_files`,
documented in-module + GAP_MATRIX). Java-side findings that reshaped the port, all
bytecode-derived: (1) 1.10.0's cleanup walks DELETE manifests through the DATA reader because
`MANIFEST_PROJECTION` omits `content` and the avro ctor defaults it to DATA — so delete files /
DV puffins ARE cleaned, despite `readPaths`' delete-manifest precondition reading as if they
could not be; (2) `GenericManifestFile` equality is path-only; (3) `findFilesToDelete` returns
the EMPTY set on any retained-side enumeration failure (catch-Throwable) — ported as
clear-and-report; (4) delete-file removal is by-path (shared-puffin fixture reshaped). The B1
concurrently-created-ref gap is unchanged by B2: cleanup computes reachability from the
COMMITTED post-state (Java refreshes; we use the returned table — equivalence argued in-module),
so the window is inherited from the metadata commit, not widened. Files touched:
`transaction/expire_cleanup.rs` (new), `transaction/expire_snapshots.rs` (doc pointer +
`parse_property` → `pub(super)`), `transaction/mod.rs` (mod + re-export + doc), map.md,
GAP_MATRIX, todo, lessons.

Review (2026-06-11, REVIEWER Fable): re-derived `cleanFiles` / `readManifests` /
`pruneReferencedManifests` / `findFilesToDelete` (incl. the lambda exception tables) /
`FileCleanupStrategy.{MANIFEST_PROJECTION,deleteFiles,statsFileLocations}` /
`RemoveSnapshots.{commit,cleanExpiredSnapshots}` / `GenericManifestFile.{equals,avro-ctor}` /
`ManifestReader.{liveEntries,isLiveEntry}` / `ManifestFiles.readPaths` from the 1.10.0
bytecode — set algebra, three failure tiers (Err / skip / clear-all scopes), sweep order, gate,
and the MANIFEST_PROJECTION finding all confirmed exactly. Timing verdict: no new concurrency
window beyond B1's recorded ref gap (post-commit append files can never be candidates;
double-expire races are double-delete-or-planning-abort, never over-deletion). M1/M2 re-run
(9 and 3 tests respectively, matching the build record). TWO survivor mutations found and
pinned (+2 tests, 1 extension; 1849 total ×2 green): the cross-funnel SWEEP ORDER
(lists-before-content survived everything — pinned structurally via the recorder's invocation
sequence; the order is the crash-RESUME property: leaves before indexes keeps the expired
lists plannable until last) and the GC gate SIDE (`after` instead of `before` survived —
pinned with before=disabled/after=enabled must refuse, Java's ctor reads `base`). Also added
the re-run pin (second sweep of the same (before, after) aborts at planning with zero delete
calls — Java's `readManifests` throws identically). Doc corrections: the "staler `before` only
shrinks" claim was unsound (a concurrent expire GROWS the set — still safe, argued via
unreachability-from-`after`; module doc + lessons fixed), `BaseSnapshot.equals` citation
corrected (5 fields, id-diff equivalent by immutability), the inherited B1 window now stated
in the module docs, and the Rust-stricter retained-list read scope noted (Java's prune
early-exits; Rust always reads both sides — more pre-deletion `Err` cases only).

## Carried-forward open items (full context in todo-archive/)

**Explicitly NOT decided:** the "platform cut line" through the GAP_MATRIX (which rows block the
user's trading platform vs continuous-parity backlog, incl. re-ordering maintenance actions ahead of
Phase-4 format exotica) was proposed but is an **open user decision — do not assume it.**
  _RESOLVED-AS-TABLED 2026-06-11: the user tabled the DataFusion/RePark direction and redirected
  the fork to near-full 1:1 Java parity — recorded in Roadmap.md (decision record item 5 + the
  re-sequenced headline areas). Originating narrative:
  [todo-archive/2026-06_ops-hardening.md](todo-archive/2026-06_ops-hardening.md)._

## Archived increment narratives

Completed-increment narratives moved verbatim out of this file (see [skills/compaction.md](../skills/compaction.md)
§Todo Archival). Not session-start reading — grep/open on demand.

- [todo-archive/phase1.md](todo-archive/phase1.md) — Phase 1 spec & metadata completeness (schema /
  partition / snapshot evolution + spec-read robustness).
- [todo-archive/phase2.md](todo-archive/phase2.md) — Phase 2 write engine (write actions + the
  concurrent-commit conflict-validation cluster, incl. the merged write-validation PR #9).
- [todo-archive/phase3.md](todo-archive/phase3.md) — Phase 3 scan parity (residual evaluation,
  inspection tables, scan-metrics emission, and inspection / scan-execution interop).
- [todo-archive/2026-06_ops-hardening.md](todo-archive/2026-06_ops-hardening.md) — the doc-infrastructure / hardening meta-sprints (not phase work).
- Index: [todo-archive/map.md](todo-archive/map.md).
