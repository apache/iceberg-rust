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
   [docs/parity/GAP_MATRIX.md](docs/parity/GAP_MATRIX.md) → [docs/testing.md](docs/testing.md).
2. **The first move is Phase 0** (base sync + repo reset) — everything else is gated on it. Do not start
   parity feature work (Phase 1+) until Phase 0 is green and the GAP_MATRIX is re-audited.
3. Verify the build before and after each change:
   ```bash
   cargo build --workspace
   cargo test --workspace            # NOT --all-features if the cdylib test binary breaks (see CLAUDE.md)
   cargo clippy --all-targets --workspace -- -D warnings
   cargo fmt --all -- --check
   ```
   (Or the `Makefile` targets: `make build` / `make check` / `make test`.)
4. **Tests land with the code, same change.** A capability is not "done" until it has unit tests *and*
   an interop test proving byte-level compatibility with Java where applicable (see Definition of Done).

### Sub-agent note
Per [CLAUDE.md](CLAUDE.md) `<subagent_policy>` the default is single-agent. The two heavy phases —
**Phase 2 (write engine)** and **Phase 4 (formats & types)** — are the natural fan-out candidates if the
policy is lifted; everything else is comfortably single-agent.

---

## Current state

Audited base: **`iceberg` 0.7.0** (datafusion 50, arrow 56.2, Rust nightly toolchain, MSRV 1.87). The
repo still carries the Python layers slated for deletion. Roughly: spec types, partition transforms,
manifest read/write, fast-append, data/equality-delete writers, Parquet→Arrow read, scan planning, the
catalog set (REST/Hive/Glue/S3 Tables/SQL/memory), and FileIO are **present**; the **write engine beyond
fast-append, schema/partition/snapshot evolution, incremental scans, ORC/Avro data files, V3 types,
view operations, and all maintenance actions are missing**. Full row-by-row status:
[docs/parity/GAP_MATRIX.md](docs/parity/GAP_MATRIX.md).

> ⚠️ The current GAP_MATRIX was audited against the **0.7.0** base. Upstream 0.8.0 / 0.9.0 / 0.9.1 likely
> close several rows — **re-audit against the 0.9.x base in Phase 0** before opening Phase 1.

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

### Phase 0 — Repo reset & base sync  ·  **Status: not started (the unblocking move)**
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

### Phase 1 — Spec & metadata completeness  ·  **Status: ❌**
- **Goal:** the metadata-evolution surface that writes depend on.
- **Gates on:** Phase 0.
- **Key deliverables:** `UpdateSchema` (add/drop/rename/reorder/promote, make-optional/required);
  `UpdatePartitionSpec` (partition evolution); `ManageSnapshots` (branch/tag CRUD, rollback, cherry-pick,
  set-current, fast-forward); full snapshot-ref handling; V3 groundwork (row-lineage fields, column
  default values).
- **Exit criteria:** each action matches the Java contract behavior, with unit + interop tests; GAP_MATRIX
  rows flipped.

### Phase 2 — Write engine  ·  **Status: ❌ (largest functional gap)**
- **Goal:** the full commit/write surface beyond fast-append.
- **Gates on:** Phase 1.
- **Key deliverables:** merge append, `OverwriteFiles`, `ReplacePartitions`, `DeleteFiles`, `RowDelta`,
  `RewriteFiles`, `RewriteManifests`; finalize position-delete + deletion-vector writers; multi-op
  transactions + optimistic-concurrency retry, **validated against Glue + S3 Tables**.
- **Exit criteria:** each write action commits correctly through the real catalogs with conflict
  detection, `MemoryCatalog`-testable AWS-free, with interop round-trips vs Java.

### Phase 3 — Scan parity  ·  **Status: 🟡**
- **Goal:** full read/scan capability + reporting + inspection.
- **Gates on:** Phase 1 (metadata); benefits from Phase 2 (delete files to scan).
- **Key deliverables:** inclusive/strict metrics evaluators + complete residual evaluation;
  `IncrementalAppendScan`, `IncrementalChangelogScan`, `BatchScan`; split planning;
  `ScanReport` / `MetricsReporter`; the full metadata-inspection table set (files, entries, history, refs,
  partitions, all_* …).
- **Exit criteria:** scans match Java planning/results incl. residuals; inspection tables present; reports
  emitted.

### Phase 4 — Format & type breadth  ·  **Status: ❌ (heavy)**
- **Goal:** data-file format and V3 type coverage on par with Java `data/`.
- **Gates on:** Phase 1 (types in spec).
- **Key deliverables:** ORC + Avro **data** file read/write; V3 types end-to-end — variant (incl.
  shredding), geometry/geography + geospatial predicates, `timestamp_ns`, `unknown`, column default values.
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

## Dependency graph & critical path

```
0 (reset + sync)
 └─ 1 (schema/partition/snapshot evolution)
     ├─ 2 (write engine) ───────────────┐
     ├─ 3 (scan parity)                  ├─ 6 (maintenance + encryption) ─ 7 (continuous parity)
     ├─ 4 (formats + V3 types)           │
     └─ 5 (views + catalogs) ────────────┘
```

- **Phase 0 unblocks everything.** It is the single most important move and is independent of the rest.
- **Metadata (1) underpins writes (2); writes + scans (2,3) underpin maintenance (6).** Phases 3/4/5 can
  run in parallel once Phase 1 lands.
- The **write engine (2)** is the largest and highest-value functional gap.

---

## Headline gaps (ranked by effort × value)

1. **Write engine** — everything beyond fast-append (`OverwriteFiles`, `ReplacePartitions`, `DeleteFiles`,
   `RowDelta`, `RewriteFiles`, `RewriteManifests`, merge append) + finalized position-delete / DV writers.
2. **Schema/partition evolution + snapshot management** (`UpdateSchema`, `UpdatePartitionSpec`,
   `ManageSnapshots`).
3. **Format & type breadth** — ORC + Avro data files; V3 types (variant, geo, `timestamp_ns`, defaults).
4. **Views in catalogs** (`ViewCatalog` + view operations).
5. **Maintenance actions** (expire / orphan / compaction / rewrite-deletes / compute-stats / migrate).
6. **Encryption** (`EncryptionManager`, KMS, encrypted FileIO / manifests).

---

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| 0.7→0.9.x is two major versions + a datafusion/arrow family bump → API breakage cascades | Sync as a merge/rebase of upstream's already-working 0.9.x integration (lean on upstream's pins + working DataFusion integration) rather than hand-porting each change; green-up incrementally; human checkpoint after the sync. |
| The datafusion-integration crate breaks on the family bump | Adopt upstream's exact 0.9.x pins as a set; fix the physical-plan integration against upstream's working version. |
| Irreversible Python-layer deletion removes something still wanted | Safety tag/branch before deleting; one revertable commit; checkpoint before the deletion. |
| GAP_MATRIX drifts from reality as work lands or Java evolves | Re-audit after every sync and every phase; Phase 7 automates Java-release tracking. |
| Parity claimed without true 1:1 evidence | Definition of Done requires an interop test (Java↔Rust byte-level round-trip) before a row flips to ✅. |

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
