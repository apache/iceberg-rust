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
- [ ] **D — de-triplicate status (own PR, review carefully).** One home per fact:
      GAP_MATRIX becomes the ONLY status record with terse cells (icon, date, one sentence, links to
      the interop test + archived narrative); Roadmap "current state" shrinks to ≤ ~30 lines pointing
      at the matrix; per-increment narrative paragraphs move to the todo-archive; CLAUDE.md gains the
      one-home-per-fact rule. Gate: every link resolves; session-start read order measured ≤ ~40k
      tokens (record the number).
- [ ] **E — interop debt paydown (rest of the frontier budget; one PR per increment).** Risk order:
      - [ ] **E1 — `RowDelta` metadata-level interop:** sequence-number inheritance, delete
            manifests, summary fields vs Java `BaseRowDelta` (the scan-exec interop already proves
            the data level both directions — this targets the snapshot/manifest metadata).
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
