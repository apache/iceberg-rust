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

## Carried-forward open items (full context in todo-archive/)

**Explicitly NOT decided:** the "platform cut line" through the GAP_MATRIX (which rows block the
user's trading platform vs continuous-parity backlog, incl. re-ordering maintenance actions ahead of
Phase-4 format exotica) was proposed but is an **open user decision — do not assume it.**
  _Status 2026-06-11: about to be decided — the user's DataFusion/SQL platform discussion is the
  cut line materializing; record the decision in Roadmap.md when made. Originating narrative:
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
