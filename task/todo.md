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


> **Archival log.** Last todo-archival pass: 2026-06-09 (size trigger — 4,344 lines) → [todo-archive/](todo-archive/) (phase1/phase2/phase3). Completed-increment narratives moved verbatim; this file keeps the active sprint + open items + archive pointers. Procedure: [skills/compaction.md](../skills/compaction.md) §Todo Archival. Archives are not read by default.

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

## DONE (2026-06-10): Sprint increment D — status de-triplication (branch `docs/de-triplication`)

One home per fact. Trigger: the sprint plan (A ✅ B ✅ C ✅ E3 ✅ → D), freshly motivated by the
overnight stale-narrative incident (Roadmap said `validateAddedFilesMatchOverwriteFilter` was
deferred; it had landed in PR #9).

- [x] **GAP_MATRIX cells go terse.** The 19 cells over 400 chars (worst: 36 KB) move VERBATIM to
      `docs/parity/archive/2026-06_matrix-cell-narratives.md` (keyed by row Area); each cell is
      rewritten as: Rust location · 1–2 sentence capability summary · flip dates · links (interop
      test, archive anchor). Status icons unchanged — this pass moves prose, never status.
- [x] **Roadmap shrinks to plan + pointers.** The per-increment narrative blobs ("For a new
      session" item 2, "Current state", per-phase Progress sections) move VERBATIM to
      `task/todo-archive/roadmap-narratives-2026-06.md`; "Current state" is rewritten ≤ ~30 lines
      pointing at the matrix; phase sections keep Goal/Gates/Deliverables/Exit + one-line status.
- [x] **CLAUDE.md gains the one-home-per-fact rule** (status lives in the matrix; narrative lives
      in archives/git; never write the same status twice — link instead).
- [x] **Gates:** conservation (every moved block verbatim, anchors resolve), all links resolve,
      `typos` clean, session-start read order (CLAUDE.md+Roadmap+GAP_MATRIX+lessons+todo) measured
      and recorded — target ≤ ~40k tokens.
      **Outcome:** GAP_MATRIX 136 KB → 27 KB (19 cells → archive verbatim, conservation 19/19);
      Roadmap 823 → 298 lines (whole pre-rewrite file archived verbatim, byte-identical diff);
      read order measured **~41.6k tokens** (was ~210k pre-sprint; lessons.md is the remaining
      ~19k and shrinks at the next compaction pass); links resolve; typos clean.
- [x] Update `task/todo-archive/map.md` for the new archive file; flip the sprint-D checkbox.

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

## Active: Operational hardening & Opus handoff — the meta-sprint (2026-06-09)

**Decided 2026-06-09 (user-approved).** Context: frontier-tier (Fable) sessions are available only
until **2026-06-22**; after that **Opus is the default maintainer tier**. The planning files have
outgrown the session-start read contract (todo.md 380 KB + lessons.md 256 KB + Roadmap.md 72 KB +
GAP_MATRIX.md 132 KB ≈ 840 KB of mandated reading — several context windows), so this sprint hardens
the documentation infrastructure FIRST, then spends the remaining frontier budget on the
highest-judgment interop debt. Decision record + rationale: Roadmap.md §"Operational hardening
sprint (2026-06-09)".

**Ordering constraints (load-bearing):**
- [skills/compaction.md](../skills/compaction.md) routes promoted lessons into directory
  `map.md#debug` sections → **A (maps) must land before B (lessons compaction)**.
- The terse GAP_MATRIX cells produced by D link to archived increment narratives → **C (todo
  archival) must land before D (de-triplication)**.
- B and C follow compaction.md discipline: **own PR, nothing else in the diff, conservation check,
  interactive approval of the verdict tally before commit.**

- [x] **Step 0 — stabilize the base.** phase3-scan-exec-interop landed on main as PR #11 (tree
      verified identical; local main fast-forwarded to 7e56fbf7; 1636 lib tests green on this tree).
- [x] **A — `map.md` scaffolding.** Created map + `## Debug` for the seven hot directories:
      `crates/iceberg/src/{transaction,inspect,scan,expr/visitors,writer}/`, `dev/java-interop/`,
      `crates/iceberg/tests/`. Seeded from code (module lists verified against `mod.rs`); Debug
      sections seeded with already-recorded failure modes and left thin for B's promotions.
      Relative links verified resolving; `typos` clean; no code touched. Rides in the same PR as
      the sprint-plan record (separate commits) since `gh` is unavailable locally.
- [x] **B — lessons.md compaction pass (own PR, interactive).** DONE 2026-06-09 — tally + promotion
      diffs + the agentic-pace recency deviation user-approved; committed on
      `docs/lessons-compaction-pass-1` (skills bundle as its own prior commit on the branch).
      - Trigger: size (2,650 lines / 256 KB vs the ~800-line / 50 KB trigger).
      - **Tally: 74 entries → 31 PROMOTE, 22 KEEP, 21 ARCHIVE.** Conservation check reconciles
        (74 = 22 active + 52 archived; heading diff empty). Active file now 797 lines.
      - Archive: `task/lessons-archive/2026-06_phase1-phase3.md` (+ archive map.md).
      - Promotion targets: docs/testing.md (new "Mutation-testing & review discipline" section +
        gate-widening rules), CLAUDE.md (gate-chained-commit convention), and the Debug sections of
        transaction/inspect/scan/writer + dev/java-interop + tests map.md files.
      - **Recency-rule deviation (needs the user's sign-off):** ALL 74 entries are within the
        7-day window (the project is 3 days old) — a strict reading makes every pass a no-op.
        Applied the intent (protect in-flight context): all 22 same-day (2026-06-09) entries +
        older entries feeding open work KEPT; landed-and-merged increment narratives archived.
        Codified as the "agentic-pace amendment" in skills/compaction.md.
      - **Prerequisite discovered:** `skills/Fable.md`, `skills/compaction.md`, and the updated
        `skills/map.md` from the prior Fable session were never committed — added verbatim from
        the pasted bundle (separate commit on the same branch so the compaction diff stays pure);
        CLAUDE.md read order now names Fable.md.
- [ ] **C — todo.md archival (own PR).** First WRITE the procedure (a todo-archival section in
      skills/compaction.md or a sibling doc: completed increments archive by phase into
      `task/todo-archive/` with its own map.md), then execute: completed-increment narratives move
      verbatim; live todo keeps open items + current context, < ~500 lines; same conservation
      discipline.
- [x] **D — de-triplicate status (own PR, review carefully).** DONE 2026-06-10, see below. One home per fact:
      GAP_MATRIX becomes the ONLY status record with terse cells (icon, date, one sentence, links to
      the interop test + archived narrative); Roadmap "current state" shrinks to ≤ ~30 lines pointing
      at the matrix; per-increment narrative paragraphs move to the todo-archive; CLAUDE.md gains the
      one-home-per-fact rule. Gate: every link resolves; session-start read order measured ≤ ~40k
      tokens (record the number).
- [ ] **E — interop debt paydown (rest of the frontier budget; one PR per increment).** Risk order:
      - [x] **E1 — `RowDelta` metadata-level interop:** DONE 2026-06-10 (see the E1 section
            above) — canonical-view equality across 3 fixtures × 3 directions; surfaced + fixed
            the `changed-partition-count` summary parity bug.
      - [x] **E2 — the rewrite-family four:** DONE 2026-06-10 (see the E2 section above) — one
            five-commit chain through the E1 oracle; zero production changes needed.
      - [ ] **E3 — inspection-table interop:** mechanical, well-templated — the explicit leave-to-
            Opus candidate if the budget runs out.

**Explicitly NOT decided:** the "platform cut line" through the GAP_MATRIX (which rows block the
user's trading platform vs continuous-parity backlog, incl. re-ordering maintenance actions ahead of
Phase-4 format exotica) was proposed but is an **open user decision — do not assume it.**


## Carried-forward open items (full context in `todo-archive/`)

Genuinely-open follow-ups lifted out of otherwise-shipped phase narratives so they stay visible in
the live plan. Full originating context is archived verbatim (pointers below). The active sprint's
own open items (C/D/E) live in the hardening section above, not here.

- [ ] **Retention positivity validation (Phase 1, unverified).** Java may reject `≤ 0` for
      `min_snapshots_to_keep` / `max_snapshot_age_ms` / `max_ref_age_ms`. A grep of `SnapshotRef.java`
      found no such `checkArgument` — confirm where (if anywhere) Java enforces it before adding it in
      Rust. _Originating narrative: [todo-archive/phase1.md](todo-archive/phase1.md) §"Review remediation"._
- [ ] **Table-metadata `last-sequence-number` lenient read (Phase 1, low priority).** The spec mandates
      default-to-0 on read, but Java always writes it for V2+, so this bites only non-Java / hand-written
      metadata (not the Java-interop path). The Avro-path siblings were CLOSED by Increment 11; only this
      JSON-serde field remains. Fix shape: `#[serde(default)]` on `last_sequence_number`. _Originating
      narrative: [todo-archive/phase1.md](todo-archive/phase1.md) §"Increment 10 — tracked follow-up" + §"Increment 11"._


## Archived increment narratives

Completed-increment narratives moved verbatim out of this file (see [skills/compaction.md](../skills/compaction.md)
§Todo Archival). Not session-start reading — grep/open on demand.

- [todo-archive/phase1.md](todo-archive/phase1.md) — Phase 1 spec & metadata completeness (schema /
  partition / snapshot evolution + spec-read robustness).
- [todo-archive/phase2.md](todo-archive/phase2.md) — Phase 2 write engine (write actions + the
  concurrent-commit conflict-validation cluster, incl. the merged write-validation PR #9).
- [todo-archive/phase3.md](todo-archive/phase3.md) — Phase 3 scan parity (residual evaluation,
  inspection tables, scan-metrics emission, and inspection / scan-execution interop).
- Index: [todo-archive/map.md](todo-archive/map.md).
