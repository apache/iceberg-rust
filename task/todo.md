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
      - [ ] **E2 — the rewrite-family four:** `DeleteFiles` / `OverwriteFiles` / `ReplacePartitions`
            / `RewriteFiles` — one oracle `generate`/`verify` pass, per-action scenarios (the Phase-1
            three-capability consolidation pattern).
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
