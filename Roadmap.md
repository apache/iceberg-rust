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

# Roadmap — Rust Iceberg (Java `iceberg-core` parity)

> **What this is.** The aggregate, project-manager plan for this repository: a **Rust-native**
> implementation of Apache Iceberg targeting **1:1 capability parity with the Java
> `iceberg-core` / `iceberg-api`** library — the engine-agnostic *table-format* core, **not** the
> Spark engine integration. It sequences all work from the current base through full parity, names the
> gate and exit criteria per phase, and is the entry point for any session (human or agent) picking up
> work here.
>
> **Authority.** This file is the **plan** (altitude + sequencing). The **living capability checklist**
> is [docs/parity/GAP_MATRIX.md](docs/parity/GAP_MATRIX.md). Repo conventions + read order live in
> [CLAUDE.md](CLAUDE.md); the testing contract is [docs/testing.md](docs/testing.md). When this file and
> the GAP_MATRIX disagree on a capability's status, the **GAP_MATRIX** (re-audited against the live base)
> wins and this file is corrected.

---

## North star

A **Rust-native** Apache Iceberg implementation with **1:1 capability parity** with the Java
`iceberg-core` / `iceberg-api` library. We **fork and own** these crates and maintain them indefinitely;
mergeability with upstream `apache/iceberg-rust` is **not** a constraint (we sync up from it and
cherry-pick wins, but diverge freely in service of parity). **Glue + S3 Tables** are the first-priority
catalogs. Python / PySpark is **deferred** (no PyIceberg, no PySpark layer) and the existing Python
layers are removed in Phase 0.

### Locked decisions

| Decision | Choice |
|---|---|
| Parity scope | Java **core library** (`iceberg-core` / `iceberg-api`), not the Spark surface |
| Core ownership | **Fork & own** the crates; drop the upstream-mergeability constraint |
| Deliverable | **Rust-native library** only; Python / PySpark deferred |
| Catalog priority | **Glue + S3 Tables first**, then REST, then Hive / JDBC / Nessie |
| Base | **Sync to upstream `iceberg` 0.9.x first**, then own from there |
| Python layers | **Delete** `iceberg-spark-python/`, `iceberg-spark-pyspark/`, `bindings/python/` |

---


## For a new session — start here

1. Read [CLAUDE.md](CLAUDE.md) (intent, prohibitions, conventions, read order) → this `Roadmap.md` →
   [docs/parity/GAP_MATRIX.md](docs/parity/GAP_MATRIX.md) (per-capability status — the ONLY status
   record) → [docs/testing.md](docs/testing.md) → [task/lessons.md](task/lessons.md) in full →
   [task/todo.md](task/todo.md) (the live plan).
2. **`main` is the owned 0.9.1 base.** Start from `main` or a short-lived feature branch off it.
   Where each phase stands: the one-line Status on each phase below; where each CAPABILITY stands:
   the GAP_MATRIX. Increment-level narrative lives in [task/todo-archive/](task/todo-archive/) and
   [task/lessons-archive/](task/lessons-archive/) — grep on demand, never required reading.
3. Verify the build before and after each change: `make build` / `make check` / `make test`, or the
   cargo commands in [docs/testing.md](docs/testing.md) (which also owns the gate-widening rules —
   workspace build for cross-crate changes; run `iceberg-datafusion` tests for read-path changes).
4. **Tests land with the code, same change.** A capability is ✅ only with unit tests AND an interop
   round-trip (see Definition of done).

### Sub-agent note
Per [CLAUDE.md](CLAUDE.md) `<subagent_policy>` the default is single-agent. The two heavy phases —
**Phase 2 (write engine)** and **Phase 4 (formats & types)** — are the natural fan-out candidates if the
policy is lifted; everything else is comfortably single-agent.

---

## Operational hardening sprint (2026-06-09) — decision record

A frontier-tier (Fable) review of the repo on 2026-06-09 concluded the **engineering discipline is
sound but the planning documents have outgrown their own read contract**: `task/todo.md` (380 KB),
`task/lessons.md` (256 KB), this file (72 KB), and the GAP_MATRIX (132 KB) totalled ~840 KB of
mandated session-start reading — several context windows — and the same increment status was being
written in triplicate (Roadmap current-state, Roadmap phase sections, GAP_MATRIX cells). The user
approved a hardening sprint **before further parity work**. Live plan + checkboxes:
[task/todo.md](task/todo.md) §"Operational hardening & Opus handoff".

**The decisions, for future sessions:**

1. **Model-tier handoff context.** Frontier-tier sessions are available only until **2026-06-22**;
   thereafter **Opus is the default maintainer tier**. Judgment-heavy work (write-engine interop
   semantics: sequence-number inheritance, delete manifests, conflict validation) is prioritized
   while the frontier tier remains; mechanical breadth (inspection interop, scenario fill-out, ORC,
   V3 exotica) is deliberately left for Opus — it is well-templated by the existing harness.
2. **The interop oracle is the project's objective verifier and its model-tier equalizer.** A
   weaker model's reasoning error cannot survive a bidirectional Java round-trip. Protect
   `dev/java-interop/` above other assets; every 🟡→✅ flip goes through it.
3. **Doc-mass discipline (sprint increments A–D):** `map.md` coverage for the hot source
   directories (A); the `skills/compaction.md` pass on lessons.md (B — it is 3–5× over its own
   trigger); a todo-archival convention + pass (C); then **one home per fact** (D): the GAP_MATRIX
   becomes the only status record with terse cells, this file's current-state section shrinks to
   ~30 lines, and increment narratives live in `task/todo-archive/`. After D, never write the same
   status in two places — link instead.
4. **Interop-debt budget (sprint increment E).** The 🟡-with-deferred-interop pattern is accepted,
   but the debt is paid down in risk order (RowDelta metadata semantics → the rewrite-family four →
   inspection tables) rather than accumulating indefinitely.
5. **Open decision — the platform cut line.** Which GAP_MATRIX rows block the downstream
   algorithmic-trading platform (and whether maintenance actions move ahead of Phase-4 format
   breadth) was proposed but **not decided**. Do not assume it; ask the user when it becomes
   load-bearing.

> **Sprint status (2026-06-10):** A (maps), B (lessons compaction), C (todo archival), D (this
> de-triplication), and E3 (inspection interop) are DONE; E1/E2 (write-action metadata interop)
> remain. The size warnings above are resolved; the one-home-per-fact rule below is now in force.

---
---

## Current state (one screen — details live in the GAP_MATRIX)

**Base:** upstream `iceberg` 0.9.1 (datafusion 52.2 / arrow 57.1 / parquet 57.1, MSRV 1.92), owned
fork on `main` since 2026-06-07. No Python layers. Offline lib suite green (~1,640 tests); Docker
suites via `make test`; `sqllogictest` needs `protoc`.

**Interop-proven ✅:** the Phase-1 evolution surface (`UpdateSchema`, `UpdatePartitionSpec`,
`ManageSnapshots` ref-ops + snapshot refs — bidirectional metadata round-trips, 2026-06-07);
merge-on-read DATA-level scan execution (the full {position, equality} × {Java-writes, Rust-writes}
× {unpartitioned, partitioned} cross-product, 2026-06-09); scan PLANNING (A4); the COMPLETE
inspection-table set incl. `readable_metrics` (Direction-1, 2026-06-09→10).

**Built but interop-deferred 🟡:** the Phase-2 write actions (`DeleteFiles`, `OverwriteFiles`,
`ReplacePartitions`, `RewriteFiles`, `RowDelta` + position-delete writer) with their
serializable-isolation conflict validations; incremental append/changelog scans; residual
evaluation; scan-metrics model + emission.

**Missing ❌:** `RewriteManifests` (sketched in [task/todo.md](task/todo.md)), merge append,
deletion-vector writer, ORC/Avro data files, variant/geo/unknown types, views, maintenance
actions, encryption.

**Row-by-row truth:** [docs/parity/GAP_MATRIX.md](docs/parity/GAP_MATRIX.md).

---

## Working principles

- **Tests land with the code** (same change), plus **interop tests**: read tables Java wrote, and prove
  Java can read what we write — the only true 1:1 evidence.
- **The Java repo is the spec-by-example.** Keep a reference checkout of `apache/iceberg`; re-crawl on
  each Java release.
- **Re-audit after every upstream sync and every phase** — keep the GAP_MATRIX live.
- **Order by dependency, then value:** metadata correctness underpins writes; writes underpin
  maintenance actions.
- Engineering floor (no bare `.unwrap()` in prod paths, `thiserror`/`anyhow`, `tracing`, house style,
  `map.md` navigation): [CLAUDE.md](CLAUDE.md).

---


## Phase plan

Each phase: **Goal · Gates on · Key deliverables · Exit criteria · Status.** Granular per-capability
detail and live status live in [docs/parity/GAP_MATRIX.md](docs/parity/GAP_MATRIX.md).


### Phase 0 — Repo reset & base sync  ·  **Status: ✅ complete (2026-06-07)**
- **Goal:** a clean, owned, Rust-native base on upstream 0.9.x before any parity feature work.
- **Gates on:** —
- **Key deliverables:**
  - **Sync** to upstream `iceberg` 0.9.x; bump datafusion / arrow / parquet / object_store / opendal /
    AWS SDK / MSRV / toolchain to the family 0.9.x targets (≈ datafusion 52 / arrow 57); regenerate
    `Cargo.lock`; `cargo build` + `cargo test` green.
  - **Re-audit** the GAP_MATRIX against the 0.9.x base; strike rows already solved by 0.8 / 0.9.
  - **Delete** `iceberg-spark-python/`, `iceberg-spark-pyspark/`, `bindings/python/` and their CI/workspace
    references; workspace still builds.
  - **Rewrite** `PROJECT.md` + `CLAUDE.md` to this north star (remove the legacy Spark-drop-in framing,
    dead references, and any fake version pins); the CLAUDE.md ownership banner flags this rewrite as owed.
- **Exit criteria:** workspace builds + tests on 0.9.x; GAP_MATRIX reflects 0.9.x reality; Python layers
  gone; contract docs match reality; one clean commit per workstream (sync / re-audit / wipe / docs).
- **Sequencing within the phase:** sync → re-audit → wipe Python → rewrite contracts (rewrite last so it
  documents the *real* synced versions). **Recommended human checkpoints:** after the sync (the riskiest,
  most conflict-prone step) and before the irreversible Python deletion.

### Phase 1 — Spec & metadata completeness  ·  **Status: ✅ effectively complete (2026-06-07; `cherrypick` reclassified Phase-2)**
- **Goal:** the metadata-evolution surface that writes depend on.
- **Gates on:** Phase 0.
- **Key deliverables:** `UpdateSchema`, `UpdatePartitionSpec`, `ManageSnapshots` (branch/tag CRUD,
  rollback, rollback-to-time, set-current, fast-forward), full snapshot-ref handling, V3 groundwork.
- **Exit criteria:** each action matches the Java contract with unit + interop tests; GAP_MATRIX
  rows ✅. **Met for the entire surface** (all three capabilities bidirectionally interop-proven);
  residual V3 groundwork (row-lineage fields, remaining `MIN_FORMAT_VERSIONS` types) tracks in the
  GAP_MATRIX. Increment narratives: [task/todo-archive/phase1.md](task/todo-archive/phase1.md).

### Phase 2 — Write engine  ·  **Status: 🟡 in progress (all core actions built; interop + remaining machinery outstanding)**
- **Goal:** the full commit/write surface beyond fast-append.
- **Gates on:** Phase 1.
- **Key deliverables:** `DeleteFiles`, `OverwriteFiles`, `ReplacePartitions`, `RewriteFiles`,
  `RowDelta` + position-delete/DV writers, `RewriteManifests`, merge append, multi-op transactions +
  optimistic-concurrency retry validated against Glue + S3 Tables.
- **Where it stands:** the five core actions + the conflict-validation layer are built and
  unit-proven (per-action status: GAP_MATRIX); `RewriteManifests` is sketched in
  [task/todo.md](task/todo.md); merge append + DV writer + real-catalog hardening remain.
- **Exit criteria:** each write action commits correctly through the real catalogs with conflict
  detection, with interop round-trips vs Java. Narratives:
  [task/todo-archive/phase2.md](task/todo-archive/phase2.md).

### Phase 3 — Scan parity  ·  **Status: 🟡 far along (inspection COMPLETE + interop'd; incremental scans built; reporting wired)**
- **Goal:** full read/scan capability + reporting + inspection.
- **Gates on:** Phase 1; benefits from Phase 2 (delete files to scan).
- **Key deliverables:** metrics/residual evaluators; incremental append/changelog scans +
  `BatchScan`; split planning; `ScanReport`/`MetricsReporter`; the full inspection-table set.
- **Where it stands:** inspection tables COMPLETE and interop-proven; data-level scan execution
  interop-proven both directions; residual evaluation wired into planning; incremental scans built
  (interop deferred); metrics model + opt-in emission landed. Remaining: `BatchScan`, CDC-merge,
  split planning, strict-evaluator completion, the constants-map increment (per-row status:
  GAP_MATRIX).
- **Exit criteria:** scans match Java result-for-result with reporting parity. Narratives:
  [task/todo-archive/phase3.md](task/todo-archive/phase3.md).

### Phase 4 — Format & type breadth  ·  **Status: ❌ (heavy)**
- **Goal:** data-file format and V3 type coverage on par with Java `data/`.
- **Gates on:** Phase 1 (types in spec).
- **Key deliverables:** ORC + Avro **data** file read/write; remaining V3 types end-to-end — variant
  (incl. shredding), geometry/geography + geospatial predicates, `unknown`. (`timestamp_ns` and column
  default values already landed in the 0.9.1 base — see GAP_MATRIX.)
- **Exit criteria:** read/write parity for ORC + Avro data; V3 types round-trip and interop with Java.

### Phase 5 — Catalog & views  ·  **Status: 🟡**
- **Goal:** view support + catalog completeness, Glue + S3 Tables first.
- **Gates on:** Phase 1.
- **Key deliverables:** `ViewCatalog` + view operations (create/replace/drop/list, view
  versions/representations) on Glue + S3 Tables, then REST; `SessionCatalog`; `LockManager` completeness;
  Glue + S3 Tables hardening.
- **Exit criteria:** view lifecycle works on the priority catalogs with interop tests; session/lock gaps
  closed.

### Phase 6 — Maintenance actions & encryption  ·  **Status: ❌**
- **Goal:** the engine-agnostic action layer + encryption.
- **Gates on:** Phase 2 (writes) and Phase 3 (scans).
- **Key deliverables:** `ExpireSnapshots`, `DeleteOrphanFiles`, `RewriteDataFiles` (compaction),
  `RewritePositionDeleteFiles`, `RemoveDanglingDeleteFiles`, `ComputeTableStats`/`ComputePartitionStats`,
  `SnapshotTable`/`MigrateTable`/`RewriteTablePath`; encryption (`EncryptionManager`, KMS client, encrypted
  FileIO + encrypted manifests/data, V3); metrics reporting + events/listeners.
- **Exit criteria:** maintenance actions match Java behavior with tests; encryption round-trips.

### Phase 7 — Continuous parity  ·  **Status: ❌ (ongoing)**
- **Goal:** keep parity from drifting as Java evolves.
- **Gates on:** Phases 1–6 maturing.
- **Key deliverables:** automation tracking Java release tags → re-crawl new features into the GAP_MATRIX;
  a differential conformance suite vs Java-produced tables run in CI; selective adoption of upstream
  `iceberg-rust` improvements.
- **Exit criteria:** CI fails on a parity regression vs Java; new Java features land as GAP_MATRIX rows
  automatically.

---


## Headline gap AREAS (ranked by effort × value — statuses live in the GAP_MATRIX)

1. **Write engine completion** — `RewriteManifests`, merge append, DV writer, real-catalog
   (Glue + S3 Tables) hardening, metadata-level interop for the landed actions.
2. **Format & type breadth** — ORC + Avro data files; remaining V3 types (variant, geo, `unknown`).
3. **Scan completion** — `BatchScan`, CDC-merge, split planning, incremental-scan interop.
4. **Views in catalogs** (`ViewCatalog` + view operations).
5. **Maintenance actions** (expire / orphan / compaction / rewrite-deletes / compute-stats / migrate).
6. **Encryption** (`EncryptionManager`, KMS, encrypted FileIO / manifests).

---

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| GAP_MATRIX drifts from reality as work lands or Java evolves | One home per fact (statuses ONLY in the matrix); re-audit after every sync and every phase; Phase 7 automates Java-release tracking. |
| Parity claimed without true 1:1 evidence | Definition of Done requires an interop test (Java↔Rust round-trip) before a row flips to ✅. |
| Status narratives regrow in this file | The de-triplication rule in [CLAUDE.md](CLAUDE.md): never write a capability's status outside the matrix — link instead. Archived narrative: [task/todo-archive/](task/todo-archive/). |

---

## Definition of done (per capability)

A GAP_MATRIX row flips to ✅ only when **(1)** the Rust API matches the Java contract's behavior,
**(2)** unit tests ship with it (same change), and **(3)** an interop test proves byte-level table
compatibility with Java in both directions where applicable.

---

## Cross-references

- [docs/parity/GAP_MATRIX.md](docs/parity/GAP_MATRIX.md) — the living capability audit (the checklist this
  roadmap drives).
- [CLAUDE.md](CLAUDE.md) — repository intent, prohibitions, conventions, read order, sub-agent policy.
- [docs/testing.md](docs/testing.md) — the testing contract (tests-with-code + interop tests).
- [README.md](README.md) — project front door.
