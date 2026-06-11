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

# Todo archive — operational hardening & doc-infrastructure sprints (2026-06)

Meta-work narratives (doc infrastructure, compaction/archival, de-triplication, the Opus-handoff
sprint) — deliberately NOT in a per-phase file: they are not capability work, and filing them
under a phase would mislead phase greps. Deviation from the per-phase layout documented in the
archival-pass plan. Verbatim moves.

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


