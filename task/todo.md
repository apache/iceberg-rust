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


> **Archival log.** Last pass: 2026-06-12 (size trigger — 2,358 lines; pass 3) →
> [todo-archive/2026-06_wave3-wave4-overnight.md](todo-archive/2026-06_wave3-wave4-overnight.md)
> (24 sections: 2 kept live, 22 archived to the one pass-scoped file incl. the superseded wave
> planning section; open items lifted into the fresh ACTIVE section). Prior passes: 2026-06-11
> (pass 2 — 1,381 lines → phase files + ops-hardening) and 2026-06-09 (pass 1 — 4,344 lines →
> phase files). Procedure: [skills/compaction.md](../skills/compaction.md) §Todo Archival.
> Archives are not read by default.

## ACTIVE (2026-06-12): Near-full-parity direction — open queue (planning record)

Directive (user, 2026-06-11): run this fork's Roadmap to **almost the full 1:1 Java replacement**;
DataFusion/RePark tabled. Waves 3–4 + the overnight session landed PRs #28–#37 (multi-spec writes,
constants-map, ExpireSnapshots + interop, DeleteOrphanFiles, RewriteDataFiles,
RemoveDanglingDeleteFiles, the variant arc end-to-end, stage_only + WAP dedup, ComputePartitionStats,
data-level interop fixtures A–G, cherrypick interop). Statuses live ONLY in the GAP_MATRIX.

- [ ] **Named next-wave interop items:** the staged-WAP fixture (V1's machinery + the harness; the
      Java oracle must enumerate ALL metadata snapshots, not ancestry, when it lands — exit-audit
      caveat); the multi-spec fixture (comparator groundwork RESOLVED in W3 — spec_id is the final
      tiebreaker on all three view copies); Java-reads-our-partition-stats-file.
- [ ] **Partition-stats residue:** the INCREMENTAL compute path; time/uuid/fixed/binary partition
      values in stats files (loud errors today).
- [ ] **`ComputeTableStats` (NDV/theta sketches) — DEPENDENCY-GATED, user decision:** needs a
      DataSketches-equivalent crate; Cargo frozen.
- [ ] **Reported divergences awaiting their increments:** manifest-list carried-vs-new entry ORDER
      differs from Java (cosmetic — readers + the canonical oracle reconcile by seq; O1 reviewer);
      F1 pre-existing global flags (Java lowercases ALL type names on parse, Rust exact-lowercase;
      Rust sort orders unbound/unvalidated on metadata parse; struct map-value Avro record naming
      shares the duplicate-name hazard variant's fix closed).
- [ ] **Scheduled with the user:** real-catalog (Glue + S3 Tables) hardening — needs credentials.
- [ ] **Opus-queue (post-handoff or parallel):** ORC/Avro breadth, view ops + SessionCatalog +
      LockManager (Sonnet-builder + Opus-critic per the calibrated split), incremental-scan interop,
      scan completion (BatchScan / CDC / split planning).

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
- [todo-archive/2026-06_wave3-wave4-overnight.md](todo-archive/2026-06_wave3-wave4-overnight.md) — Waves 3–4 + the overnight session (PRs #25–#37; pass-scoped).
- Index: [todo-archive/map.md](todo-archive/map.md).
