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

# `task/todo-archive/` — map

## Purpose

Verbatim archive of **completed-increment narratives** moved out of [`../todo.md`](../todo.md) by the
todo-archival pass (procedure: [`../../skills/compaction.md`](../../skills/compaction.md) §Todo
Archival). One file per Roadmap phase. **Not session-start reading** — the live `todo.md` is the plan;
these are the shipped history, read on demand (a Carried-forward pointer routes you here, or you are
reconstructing why a shipped increment did what it did).

## Contents

| File | Covers |
|---|---|
| [2026-06_ops-hardening.md](2026-06_ops-hardening.md) | **Meta-sprints (not phase work).** Increment D status de-triplication + the operational-hardening & Opus-handoff sprint plan (2026-06-09/10). Deliberately not phase-filed (deviation documented in the file header). Added by archival pass 2 (2026-06-11), which also appended to the three phase files: phase2 +9 sections (the write-engine completion arc, the DV arc, the overnight plan + morning report, Arc F cherrypick, E1, E2, the OverwriteFiles branch-A increment, the superseded RewriteManifests sketch), phase1 +2 (Arc G + the closed carried-forward items), phase3 +1 (readable_metrics interop). |
| [phase1.md](phase1.md) | **Phase 1 — Spec & metadata completeness.** Schema evolution (`UpdateSchema`), partition evolution (`UpdatePartitionSpec`), snapshot management (`ManageSnapshots`), column defaults, and the spec-mandated default-to-0 read-robustness increments (10/11). |
| [phase2.md](phase2.md) | **Phase 2 — Write engine.** The first write increment + `DeleteFiles` / `OverwriteFiles` / `ReplacePartitions` / `RewriteFiles` / `RowDelta` actions, the `PositionDeleteFileWriter`, and the concurrent-commit conflict-validation cluster (incl. `validateFilesExist` / `validateDataFilesExist` and the write-validation PR #9 remnants). |
| [phase3.md](phase3.md) | **Phase 3 — Scan parity.** Inspection-table set (`files`/`entries`/`history`/`partitions`/`all_*` + `readable_metrics`), `ResidualEvaluator` + scan-wiring, the conflict-validation sequence done in the Phase-3 window, scan-metrics emission (`ScanReport`/`MetricsReporter`), and inspection / scan-planning / scan-execution interop. |
| [roadmap-narratives-2026-06.md](roadmap-narratives-2026-06.md) | **The complete pre-de-triplication `Roadmap.md`** (2026-06-10, sprint increment D), archived verbatim — every per-increment narrative paragraph the Roadmap used to carry. The live Roadmap is now plan-only; statuses live in the GAP_MATRIX. |

## I want to...

| ...find | go to |
|---|---|
| the current plan / open work | [`../todo.md`](../todo.md) — the live file (not here) |
| why a shipped write action behaves as it does | [phase2.md](phase2.md) (grep the action name) |
| the residual / inspection / interop increment notes | [phase3.md](phase3.md) |
| the schema/partition/snapshot evolution increment notes | [phase1.md](phase1.md) |
| the archival procedure / how to run the next pass | [`../../skills/compaction.md`](../../skills/compaction.md) §Todo Archival |

## Pointers

- **Up:** [`../todo.md`](../todo.md) (live plan), [`../lessons.md`](../lessons.md) (lessons; its own
  archive is [`../lessons-archive/`](../lessons-archive/)).
- **Related:** [`../../docs/parity/GAP_MATRIX.md`](../../docs/parity/GAP_MATRIX.md) (capability status —
  the source of truth for what is done), [`../../Roadmap.md`](../../Roadmap.md) (phase plan).

## Archival log

- **2026-06-09** (size trigger — live `todo.md` was 4,344 lines): first todo-archival pass. 18 `## ` /
  42 `### ` sections — 1 `## ` kept live (the hardening sprint), 17 archived (phase1 1, phase2 4,
  phase3 12); 2 genuinely-open Phase-1 follow-ups lifted into the live "Carried-forward open items"
  section. Live `todo.md` → 137 lines.

## Debug

### Known failure modes

| Symptom | Likely cause | First check |
|---|---|---|
| A heading seems missing after a pass | a section assigned to no destination, or hand-copied | re-run the conservation check (`grep -c '^## '` / `'^### '` across the live file + all archives vs the pre-pass file); they must reconcile with no duplicates |
| A `[ ]` item looks "lost" | it was stale (superseded) and archived with its section, not surfaced as live | grep the phase archive for the item; if its work shipped under a renumbered `[x]` increment, it is correctly archived, not lost |
| The live file is growing again | new completed narratives accumulating since the last pass | run the next pass when `todo.md` passes the size trigger (~500 lines) — see [`../../skills/compaction.md`](../../skills/compaction.md) §Todo Archival |

### First checks

- The conservation invariant: every pre-pass `## `/`### ` heading exists in exactly one of {live file, one archive}. The split is scripted (Python over heading boundaries) precisely so this holds.
- Archives are append-closed: a new pass creates a NEW per-phase grouping or appends a dated block; it never silently rewrites an existing archived narrative.

### Escalate to

[`../../skills/compaction.md`](../../skills/compaction.md) §Todo Archival (the procedure + done gate),
then [`../../CLAUDE.md`](../../CLAUDE.md) (precedence).
