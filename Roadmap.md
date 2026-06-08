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
   [docs/parity/GAP_MATRIX.md](docs/parity/GAP_MATRIX.md) → [docs/testing.md](docs/testing.md).
2. **Phase 0 is complete (2026-06-07); Phase 1 is in progress.** **`main` is the owned 0.9.1 base** (the
   Phase 0 sync landed on it 2026-06-07) — start from `main`, or a short-lived feature branch off it.
   Within Phase 1, increments 1–4 have landed (all 🟡): increments 1–3 (`ManageSnapshots`,
   `UpdatePartitionSpec`, `UpdateSchema`), and increment 4 (the `ManageSnapshots` tail — `rollbackToTime` +
   non-positive-retention rejection — plus `UpdateSchema` column initial/write defaults). **`cherrypick` is
   reclassified as Phase-2-gated** (it extends `MergingSnapshotProducer` / replays data files, so it belongs
   to the write engine, not this metadata surface). **`UpdateSchema` is now a clean ✅** — the increment-5
   interop pilot (2026-06-07) landed a bidirectional Java round-trip (`dev/java-interop/` oracle +
   `crates/iceberg/tests/interop_update_schema.rs`, 7 scenarios, both directions green), and increments 6–7
   (2026-06-07) closed the last holes by enforcing the V3-only initial-default guard AND the V3-only
   **type** gate (`Schema::check_compatibility` in `TableMetadataBuilder::add_schema`, mirroring Java
   `Schema.checkCompatibility` in full — both `DEFAULT_VALUES_MIN_FORMAT_VERSION` and `MIN_FORMAT_VERSIONS`).
   **`UpdatePartitionSpec` is now ✅ too** (bidirectional interop landed 2026-06-07 via the same
   `dev/java-interop/` oracle — one `generate`/`verify` pass now covers both schema + partition; the
   interop surfaced and fixed a real Rust↔Java divergence in the partition-name↔schema collision check —
   identity-only → identity-OR-void, mirroring Java's bind path). **`ManageSnapshots` (the ref-operation
   surface) is now ✅ too** (bidirectional interop landed 2026-06-07 via the same `dev/java-interop/`
   oracle — one `generate`/`verify` pass now covers all THREE capabilities (schema + partition +
   manage-snapshots), 7 manage-snapshots scenarios both directions green via
   `crates/iceberg/tests/interop_manage_snapshots.rs`; both the "Snapshot model + refs" and the
   ref-operation surface of "Snapshot management" flipped to ✅). **`cherrypick` is NOT interop-proven and
   stays Phase-2-gated** (it extends `MergingSnapshotProducer` / replays data files). The interop surfaced
   a Rust read divergence (the V2/V3 snapshot reader required `sequence-number` while the spec and Java
   default an absent value to 0 on read) — **FIXED 2026-06-07** with `#[serde(default)]` on
   `_serde::SnapshotV2`/`SnapshotV3.sequence_number`, so Rust now reads the seq-0 V1→V2-upgrade-carryover
   tables Java emits. The sibling spec default-to-0 read fields on the **Avro manifest / manifest-list path**
   (manifest-list `content`/`sequence-number`/`min-sequence-number`; manifest-entry `sequence_number`/
   `file_sequence_number`; data-file `content`) were **VERIFIED already-correct and pinned 2026-06-07**
   (per-field empirical proof + mutation-verified regression tests — no production change needed; the readers
   were already lenient, the gap was test coverage). The only residual is table-metadata
   `last-sequence-number`, which Java ALWAYS writes for V2+ — a non-Java/hand-written robustness gap only.
   **With all three Phase-1 evolution capabilities interop-proven, Phase 2 (write engine) has begun: increment 1
   — `DeleteFiles` + the foundational manifest-filter / rewrite machinery in `SnapshotProducer` — landed 🟡
   on 2026-06-07 (delete data files by path/reference; `MemoryCatalog` unit tests; data-level Java interop
   deferred); increment 2 — `OverwriteFiles` (explicit add + delete in one `Overwrite` snapshot, composing
   the add + delete paths via the now-shared `SnapshotProducer::{resolve_delete_paths,
   current_data_manifests}`; summary reflects added + deleted counts) — landed 🟡 the same day; increment 3
   — `ReplacePartitions` (dynamic partition overwrite; a by-PARTITION sibling resolver
   `SnapshotProducer::resolve_partition_deletes` feeds the same rewrite path; replaces every partition an
   added file belongs to, full replace on an unpartitioned table, `replace-partitions` marker) — landed 🟡
   the same day; increment 4 — `RewriteFiles` (the compaction-commit primitive: atomically replace a set of
   DATA files with a new set in one `Operation::Replace` snapshot, Java `BaseRewriteFiles`; reuses the
   by-path `resolve_delete_paths` + `process_deletes` machinery unchanged; delete set must be non-empty) —
   landed 🟡 the same day. Increment 5a — the `PositionDeleteFileWriter`
   (`writer/base_writer/position_delete_writer.rs`, first piece of the `RowDelta` merge-on-read write path:
   writes the Iceberg position-delete file `file_path`/`pos` with `content(PositionDeletes)`, Java-faithful
   write-as-given) — landed 🟡 on 2026-06-08. Next Phase-2 increments: `RowDelta` action (5b) + producer
   delete-manifest handling, deletion-vector writer (5c), `RewriteManifests`, merge append. (V3
   groundwork — row-lineage fields + the remaining `MIN_FORMAT_VERSIONS` types
   `variant`/`unknown`/`geometry`/`geography` — also remains.)** The live,
   increment-level plan and checkbox
   state are in
   [task/todo.md](task/todo.md) — read it (and [task/lessons.md](task/lessons.md) in full) before starting.
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

**Base: upstream `iceberg` 0.9.1** (datafusion 52.2, arrow 57.1, parquet 57.1, MSRV 1.92), adopted as the
owned fork in Phase 0 (2026-06-07) and merged to **`main`** (the canonical base). No Python layers remain. The workspace builds
green and the offline lib/unit suite passes (0 failures); service-bound integration suites need Docker
(`make test`) and the `sqllogictest` crate needs `protoc`. Roughly: spec types, partition transforms,
manifest read/write, fast-append, data/equality-delete writers, Parquet→Arrow read **plus merge-on-read
delete application**, scan planning, the catalog set (REST/Hive/Glue/S3 Tables/SQL/memory), FileIO,
`timestamp_ns` + column default values are **present**; **schema evolution (`UpdateSchema`) is ✅**
(Phase 1 increments 1–7: full `SchemaUpdate` incl. `UnionByNameVisitor`, level-order field-id assignment,
column initial/write defaults, the V3-only initial-default AND type gates (`Schema::check_compatibility` in
`TableMetadataBuilder::add_schema`, fully mirroring Java `Schema.checkCompatibility`), AND a bidirectional
Java interop round-trip via `dev/java-interop/` + `crates/iceberg/tests/interop_update_schema.rs`);
**partition evolution (`UpdatePartitionSpec`) is ✅ too** (full `BaseUpdatePartitionSpec` parity + a
bidirectional Java interop round-trip via the same oracle + `crates/iceberg/tests/interop_update_partition_spec.rs`,
which surfaced and fixed a real partition-name↔schema collision divergence); **snapshot management
(`ManageSnapshots`) — the ref-operation surface — is ✅ now too** (branch/tag lifecycle + rollback +
rollback-to-time + set-current + fast-forward + retention (non-positive rejected), bidirectionally
interop-proven via the same oracle + `crates/iceberg/tests/interop_manage_snapshots.rs`, 7 scenarios both
directions; both "Snapshot model + refs" and the ref-op surface of "Snapshot management" flipped to ✅).
**`cherrypick` is NOT interop-proven and stays Phase-2-gated** (it extends `MergingSnapshotProducer` /
replays data files). **Phase 2 has started: `DeleteFiles` + the manifest-filter / rewrite machinery is 🟡**
(`transaction/delete_files.rs` + `SnapshotProducer::process_deletes`, `MemoryCatalog`-tested; data-level
Java interop deferred); **`OverwriteFiles` is 🟡** (`transaction/overwrite_files.rs` — explicit add + delete
in one `Overwrite` snapshot, reusing the shared resolve/list helpers; summary reflects added + deleted
counts; filter-based `validateNoConflictingData` landed reusing the `InclusiveMetricsEvaluator`;
`overwriteByRowFilter` + `validateNoConflictingDeletes` + interop deferred); **`ReplacePartitions` is 🟡**
(`transaction/replace_partitions.rs` — dynamic partition overwrite via a by-PARTITION sibling resolver
`SnapshotProducer::resolve_partition_deletes` feeding the same rewrite path; full replace on an
unpartitioned table; `replace-partitions` marker; static `replaceByRowFilter` + conflict validation +
interop deferred); **`RewriteFiles` is 🟡** (`transaction/rewrite_files.rs` — the compaction-commit
primitive: atomically replace a set of DATA files with a new set in one `Operation::Replace` snapshot,
reusing the by-path `resolve_delete_paths` + `process_deletes` machinery unchanged; delete set must be
non-empty (`failMissingDeletePaths`); DELETE-file rewrite + `dataSequenceNumber` preservation + conflict
validation + interop deferred). **Phase 3 (scan parity) has started on the inspection-table set: `files` /
`data_files` / `delete_files` (Increment 1, `inspect/files.rs`), `entries` (Increment 2, `inspect/entries.rs`),
the three pure-metadata tables `history` / `refs` / `metadata_log_entries` (Increment 3,
`inspect/{history,refs,metadata_log_entries}.rs`), and the first AGGREGATING table `partitions` (Increment 4,
`inspect/partitions.rs`) are all 🟡** (alongside the pre-existing `snapshots` +
`manifests`); the four cross-snapshot `all_*` file/entry tables (`all_data_files` / `all_delete_files` /
`all_files` / `all_entries`, Increment 5a, `inspect/{files,entries}.rs` + shared `inspect/manifest_source.rs`)
and the final `all_manifests` table (Increment 5b, `inspect/all_manifests.rs`) are now 🟡 too — the
inspection-table set is COMPLETE; only inspection interop + `readable_metrics` remain. The rest of the **write engine
(merge append, `RowDelta`, `RewriteManifests`), incremental scans, ORC/Avro data files,
variant/geo/unknown types, catalog view ops, and all maintenance actions are missing**. Full row-by-row
status (re-audited on 0.9.1): [docs/parity/GAP_MATRIX.md](docs/parity/GAP_MATRIX.md).

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

### Phase 1 — Spec & metadata completeness  ·  **Status: 🟡 in progress**
- **Goal:** the metadata-evolution surface that writes depend on.
- **Gates on:** Phase 0.
- **Key deliverables:** `UpdateSchema` (add/drop/rename/reorder/promote, make-optional/required, column
  defaults); `UpdatePartitionSpec` (partition evolution); `ManageSnapshots` (branch/tag CRUD, rollback,
  rollback-to-time, set-current, fast-forward); full snapshot-ref handling; V3 groundwork (row-lineage
  fields). (`cherrypick` is **Phase-2-gated** — it extends `MergingSnapshotProducer` / replays data files.
  Column default *values* already present in the 0.9.1 base as spec types; the *builder API* to set them
  landed in increment 4 — see GAP_MATRIX.)
- **Progress:** increment 1 — `ManageSnapshots` (branch/tag lifecycle, rollback, fast-forward, retention)
  landed with unit tests (🟡 — `cherrypick` + `rollbackToTime` deferred; Java interop test pending).
  Increment 2 — `UpdatePartitionSpec` landed at full `BaseUpdatePartitionSpec` parity (🟡), reviewed against
  `TestUpdatePartitionSpec.java` (two parity bugs fixed in review: `recycleOrCreatePartitionField` name
  reuse, and the `addNonDefaultSpec` requirement set; Java interop test pending).
  Increment 3 — `UpdateSchema` landed at `SchemaUpdate` parity (🟡): add/rename/update-type/doc/
  make-optional/require/delete/move/set-identifier-fields + `union_by_name_with`. Reviewed by three
  perspective-diverse critics; the remediation fixed one **blocker** (nested fresh field-id assignment was
  depth-first; Java `AssignFreshIds`/`CustomOrderSchemaVisitor` is level-order — struct assigns all
  immediate ids before descending, map assigns key-id then value-id first; pinned by `testAddNestedMapOf
  Structs`/`testAddNestedListOfStructs` exact-id tests) and several **major** gaps: `union_by_name` now
  mirrors the full `UnionByNameVisitor` (adds nested under list/map structs, relaxes required→optional,
  rejects incompatible primitive + complex↔primitive type changes with the Java "Cannot change column
  type" message, recurses list/map), and `Schema::build` now rejects case-insensitive lowercase-name
  collisions (`Cannot build lower case index: a and b collide`, Java `TypeUtil.indexByLowerCaseName`).
  63 update-schema unit tests + 2 schema-build collision tests; key fixes mutation-verified.
  Increment 4 — the `ManageSnapshots` tail + `UpdateSchema` column defaults (🟡). `ManageSnapshots`:
  `rollbackToTime` (Java `SetSnapshotOperation.findLatestAncestorOlderThan` — newest ancestor with
  `timestamp_ms` strictly `<` the arg; errors if none) and non-positive-retention rejection
  (`SnapshotRef.Builder` `> 0` messages). `UpdateSchema`: column initial/write **defaults** through the
  builder API (Java `addColumn(..,Literal)` / `addRequiredColumn(..,default)` / `updateColumnDefault`
  overloads) — add sets both defaults, a required add WITH a default needs no `allow_incompatible_changes`,
  `update_column_default` sets only the write default; defaults type-validated. `cherrypick` reclassified
  as **Phase-2-gated** (extends `MergingSnapshotProducer` / replays data files). +22 unit tests
  (11 manage_snapshots, 11 update_schema — the review added 2 covering the defaulted-add → require
  interaction, Java `testAddColumnWith[UpdateColumn]DefaultToRequiredColumn`). **Deferred to ✅ (all 🟡
  rows):** Java interop round-trip.
  Increment 5 — **`UpdateSchema` interop pilot → row ✅ (2026-06-07).** Built the bidirectional Java
  interop harness under `dev/java-interop/` (a TEST-ONLY oracle, like `dev/spark/`; not a crate, not in
  the Cargo graph): a `package org.apache.iceberg` program reaching the package-private
  `@VisibleForTesting SchemaUpdate(Schema,int)` ctor to (a) `generate` base + Java-evolved metadata JSON
  and (b) `verify` that Java reads the Rust-evolved metadata. The Rust side is
  `crates/iceberg/tests/interop_update_schema.rs` over 7 committed scenarios (`add_top_level_columns`,
  `add_nested_struct_and_map` [the level-order nested-id blocker case], `rename_and_move`,
  `update_type_promotion`, `make_optional_and_delete`, `set_identifier_fields`,
  `add_required_with_default_and_update_default`). Direction 1 (Rust reproduces Java's evolution — recursive
  field-id/name/type/required/doc/default + identifier ids + current-schema-id + last-column-id) runs
  offline through a real `MemoryCatalog` commit; Direction 2 (`mvn ... verify`) confirms Java reads the
  Rust output. **7/7 PASS both directions.** The harness is the template for the remaining 🟡 rows.
  Increment 6 — **V3 initial-default guard → `UpdateSchema` row a clean ✅ (2026-06-07).** Closed the one
  parity hole the pilot surfaced: Rust had no V3-only guard on column initial defaults, so a defaulted add
  on a V1/V2 table emitted metadata Java rejects. Added `Schema::check_compatibility(format_version)`
  (`spec/schema/mod.rs`) mirroring Java `Schema.checkCompatibility` — iterates ALL fields incl. nested (the
  recursive id→field index = Java's `lazyIdToField()`) and rejects a non-null `initial_default` when
  `format_version < 3` ("...not supported until v3" / "Invalid schema for v{N}", `ErrorKind::DataInvalid`);
  gates `initial_default` only, NOT `write_default` (matching Java). Wired into the single choke point
  `TableMetadataBuilder::add_schema` (Java's `addSchemaInternal`), so it fires for the UpdateSchema action's
  emitted `AddSchema`, CTAS, and every catalog commit. Reconciled the two tests that pinned the old behavior
  (V2-default unit test → moved to a V3 base + V2-rejects sibling; the interop divergence test → flipped to
  assert rejection); +5 `add_schema` guard tests (V2 top-level/nested reject, V3 allow, write-default-only
  allow, no-default unaffected) + 2 `Schema::check_compatibility` tests, all mutation-verified.
  Increment 7 — **V3-only TYPE gate → `Schema::check_compatibility` fully mirrors Java (2026-06-07).**
  Extended the same method with the V3-only type rule (Java `MIN_FORMAT_VERSIONS`): a `min_format_version`
  helper returns `V3` for `PrimitiveType::{TimestampNs, TimestamptzNs}` (Java `TIMESTAMP_NANO`), and the
  single field-iteration pass now records a type problem ("Invalid type for {col}: timestamp_ns is not
  supported until v3") alongside the initial-default problem, accumulated into the one combined "Invalid
  schema for v{N}:" error ordered by field id (Java's TreeMap). Closes the live `add_column(timestamp_ns)`-
  on-V2 hole. +5 `check_compatibility` tests (V2 reject `timestamp_ns`/`timestamptz_ns`, V3 allow, nested
  reject with dotted path, both-problems accumulation), mutation-verified by disabling the type branch.
  `variant`/`unknown`/`geometry`/`geography` are a one-line `min_format_version` arm each when those types
  land (tracked in `task/todo.md`).
- **Exit criteria:** each action matches the Java contract behavior, with unit + interop tests; GAP_MATRIX
  rows flipped to ✅ (interop proven). **`UpdateSchema` ✅ (V3 initial-default + type gates enforced) and
  `UpdatePartitionSpec` ✅ (bidirectional interop landed 2026-06-07, surfacing+fixing the identity-only
  partition-name collision divergence); `ManageSnapshots` awaits the same interop treatment.**

### Phase 2 — Write engine  ·  **Status: 🟡 (in progress — `DeleteFiles` + manifest-filter machinery + `OverwriteFiles` + `ReplacePartitions` + `RewriteFiles` landed 2026-06-07; `PositionDeleteFileWriter` (RowDelta increment 5a) + `RowDelta` action (5b) landed 2026-06-08 — the merge-on-read write→read chain is now end-to-end; the concurrent-commit conflict-validation FOUNDATION + `ReplacePartitions.validateNoConflictingData` (increment 6) landed 2026-06-08 — the serializable-isolation safety layer; filter-based `OverwriteFiles.validateNoConflictingData` landed 2026-06-08 (Phase 3 Increment 3) reusing the `InclusiveMetricsEvaluator`; `RowDelta.validateNoConflictingDataFiles` landed 2026-06-08 (Phase 3 Increment 4) via the shared `validate_no_conflicting_added_data_files` helper now used by BOTH OverwriteFiles + RowDelta — the 2nd-use → shared-helper extraction, behavior-preserving for OverwriteFiles; `RowDelta.validateNoConflictingDeleteFiles` landed 2026-06-08 via a generalized walk `added_files_after` + new `added_delete_files_after` (V2-guarded) + a shared per-file test `first_conflicting_file` — the data + delete checks now share the per-file metrics test, behavior-preserving for the data path)**
- **Goal:** the full commit/write surface beyond fast-append.
- **Gates on:** Phase 1.
- **Increment sequence (dependency, then value):** **1. `DeleteFiles`** (done 🟡 — delete data files by
  path/reference; built the foundational manifest-filter / rewrite machinery in `SnapshotProducer` that the
  rest reuse), **2. `OverwriteFiles`** (done 🟡 — explicit add + delete in one `Overwrite` snapshot,
  composing the fast-append add path with the `DeleteFiles` filter path via the now-shared
  `SnapshotProducer::{resolve_delete_paths, current_data_manifests}`; summary reflects added + deleted
  counts; filter-based `validateNoConflictingData` landed (Phase 3 Increment 3, reusing the
  `InclusiveMetricsEvaluator`); `overwriteByRowFilter` + `validateNoConflictingDeletes` + interop deferred),
  **3. `ReplacePartitions`**
  (done 🟡 — dynamic partition overwrite; a by-PARTITION sibling resolver
  `SnapshotProducer::resolve_partition_deletes` feeds the same `process_deletes` rewrite path; replaces
  every partition an added file belongs to, full replace on an unpartitioned table, `replace-partitions`
  marker; static `replaceByRowFilter` + conflict validation + interop deferred), **4. `RewriteFiles`**
  (done 🟡 — the compaction-commit primitive: atomically replace a set of DATA files with a new set in one
  `Operation::Replace` snapshot, Java `BaseRewriteFiles`; reuses the by-path `resolve_delete_paths` +
  `process_deletes` rewrite machinery unchanged; delete set must be non-empty (`failMissingDeletePaths`);
  DELETE-file rewrite + `dataSequenceNumber` preservation + conflict validation + interop deferred),
  5. `RewriteManifests`, 6. merge append, 7. `RowDelta` + position-delete / deletion-vector
  writers (**increment 5a done 🟡 2026-06-08 — `PositionDeleteFileWriter` in
  `writer/base_writer/position_delete_writer.rs`: writes the Iceberg position-delete file
  (`file_path` id 2147483546, `pos` id 2147483545) with `content(PositionDeletes)`, Java-faithful
  write-as-given; round-trip + field-id tests prove read/Java consumability. Sub-sequence: 5a writer
  [done], 5b `RowDelta` action + producer delete-manifest handling [**done 🟡 2026-06-08** —
  `transaction/row_delta.rs`: `RowDeltaAction` adds data files + position/equality DELETE files in one
  snapshot; `SnapshotProducer` gained delete-manifest support (`with_added_delete_files` +
  `write_added_delete_manifest`, the `Deletes` writer; empty-commit precondition relaxed); added delete
  entries inherit the new snapshot's seq so they apply to earlier data; dynamic op (Java
  `BaseRowDelta.operation()`). THE CROWN-JEWEL test proves a scan drops the deleted rows after the row
  delta — the full merge-on-read write→read chain], 5c deletion-vector writer**),
  **6. concurrent-commit conflict-validation FOUNDATION + `ReplacePartitions.validateNoConflictingData`**
  (**done 🟡 2026-06-08** — the serializable-isolation safety layer: `Transaction` captures
  `starting_snapshot_id` (surviving the `do_commit` re-base); a default-no-op `TransactionAction::validate`
  hook runs in `do_commit` against the refreshed base BEFORE re-apply; a conflict is a non-retryable
  `DataInvalid` (Java's non-retryable `ValidationException`) so the retry loop stops; shared
  `added_data_files_after` helper enumerates the concurrent commits' added DATA files (Java
  `MergingSnapshotProducer.addedDataFiles`/`ancestorsBetween`); `ReplacePartitions` gained the opt-in
  `validate_no_conflicting_data()`/`validate_from_snapshot(id)` rejecting a concurrent append into a
  replaced partition. **`OverwriteFiles.validateNoConflictingData` (filter-based) landed 🟡 2026-06-08**
  (Phase 3 Increment 3, `transaction/overwrite_files.rs`) — the same opt-in shape plus a
  `conflict_detection_filter(Predicate)`, tested per-file with the existing `InclusiveMetricsEvaluator`
  (None-filter ⇒ `AlwaysTrue`); see Phase 3. **Conflict-validation sub-sequence:** (6) foundation +
  `ReplacePartitions` [done 🟡]; `OverwriteFiles.validateNoConflictingData` [done 🟡]; then `OverwriteFiles`
  `validateAddedFilesMatchOverwriteFilter`/`...Deletes`; `RowDelta`/`DeleteFiles` `validateDataFilesExist`;
  `RewriteFiles` `validateNoNewDeletes`),
  7. `RewriteManifests`, merge append,
  8. multi-op transaction hardening + optimistic-concurrency retry on the real catalogs.
- **Key deliverables:** merge append, `OverwriteFiles`, `ReplacePartitions`, `DeleteFiles` (🟡), `RowDelta`,
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
- **Residual evaluation (started 2026-06-08):** the `ResidualEvaluator` CORE landed 🟡 (Increment 1,
  `expr/visitors/residual_evaluator.rs`, Java `ResidualEvaluator`) — partially evaluates a bound row filter
  against a partition's values → the residual `Predicate` (strict-projection-true ⇒ `AlwaysTrue`,
  inclusive-projection-false ⇒ `AlwaysFalse`, else keep), reusing `Transform::{project,strict_project}` +
  the `ExpressionEvaluatorVisitor`; 16 unit tests incl. the Javadoc `day(ts)` 4-case example, both mutations
  (strict↔inclusive, partition-ignore) caught. **Scan-wiring landed 🟡 (Increment 2, 2026-06-08,
  `scan/context.rs`):** each `FileScanTask` now carries the PARTITION-REDUCED residual (Java
  `BaseFileScanTask.residual()` = `residuals.residualFor(file.partition())`), not the full snapshot filter —
  the evaluator is built once per manifest file in `PlanContext::create_manifest_file_context` and
  `into_file_scan_task` evaluates it against the file's partition, binds the residual back to the snapshot
  schema, and stores it as `task.predicate`; the file's spec is used only to build the residual evaluator and
  the task's `partition_spec` stays `None` (the identity-partition constant-materialization path,
  `PartitionUtil.constantsMap`, is DEFERRED — its `record_batch_transformer` has latent type bugs that broke
  integration tests; the residual does not need it). The arrow reader is unchanged. Result-equivalence is
  proven by 9 scan tests on identity AND truncate partitions; both mutations (leave-full-filter,
  wrong-partition) caught. **Filter-based
  concurrent-commit conflict validation landed 🟡 (Increment 3, 2026-06-08, `transaction/overwrite_files.rs`):**
  `OverwriteFiles.validateNoConflictingData` — the serializable-isolation write-safety layer. Opt-in
  `validate_no_conflicting_data()` + `conflict_detection_filter(Predicate)` + `validate_from_snapshot(id)`
  mirror Java `BaseOverwriteFiles.validate` (L163-165) → `MergingSnapshotProducer.validateAddedDataFiles`
  (L391-412): when enabled, reject the commit (non-retryable `DataInvalid`) if any DATA file ADDED by a
  concurrent commit since the starting snapshot COULD contain records matching the conflict filter, tested
  per-file with the EXISTING `InclusiveMetricsEvaluator` over the shared `added_data_files_after` walk. The
  None-filter default is `AlwaysTrue` (any concurrent add conflicts); case-sensitivity defaults to `true`.
  9 conflict tests; the metrics include/exclude decision, the skip-the-check path, and the non-retryable
  error kind are each mutation-pinned. **`RowDelta.validateNoConflictingDataFiles` (data-file) landed 🟡
  (Increment 4, 2026-06-08, `transaction/row_delta.rs`):** the SAME filter-based added-data-file conflict
  check (Java `BaseRowDelta.validate` L155-157 → the identical `validateAddedDataFiles`). Because this is the
  2nd use of the load-bearing safety logic, the walk + bind + per-file inclusive-metrics eval + first-conflict
  non-retryable `DataInvalid` was FACTORED into a shared `pub(crate)`
  `validate_no_conflicting_added_data_files` helper (next to `added_data_files_after` in
  `transaction/snapshot.rs`); `OverwriteFiles` was refactored to delegate to it (behavior-preserving — its 9
  conflict tests stayed green unchanged), and `RowDelta` gained `validate_no_conflicting_data_files()` +
  `conflict_detection_filter(Predicate)` + `validate_from_snapshot(id)` calling the same helper. 8 RowDelta
  conflict tests (real concurrent `fast_append`); 3 mutations caught incl. the CROSS-ACTION one (invert the
  helper's metrics decision → the EXCLUDE test fails for BOTH actions). **`RowDelta.validateNoConflictingDeleteFiles`
  (DELETE-file) landed 🟡 (2026-06-08, `transaction/row_delta.rs` + `transaction/snapshot.rs`):** the
  delete-file counterpart of the data-file check (Java `BaseRowDelta.validate` L159-167 → `validateNewDeleteFiles`
  → `MergingSnapshotProducer.validateNoNewDeleteFiles` L562-570 → `addedDeleteFiles` L601-625). The data-file
  walk was GENERALIZED into a private `added_files_after(table, start, content, op_adds)`, so `added_data_files_after`
  (unchanged behavior) and the new `pub(crate) added_delete_files_after` (V2-guarded — Java `base.formatVersion() < 2`
  ⇒ empty — over `ManifestContent::Deletes` + the `{OVERWRITE,DELETE}` op set) are both thin calls; the per-file
  inclusive-metrics test was EXTRACTED into a shared `first_conflicting_file`, used by both
  `validate_no_conflicting_added_data_files` (refactored, behavior-preserving) and the new
  `validate_no_conflicting_added_delete_files` (DELETE-specific message). `RowDelta` gained
  `validate_no_conflicting_delete_files()`, INDEPENDENT of the data flag. 8 delete-conflict tests (real concurrent
  `row_delta().add_deletes`); 4 mutations caught (content filter Data-vs-Deletes; the shared metrics decision —
  fails BOTH data + delete EXCLUDE tests; retryable kind; the V2 guard documented as a Java-faithful short-circuit
  the walk renders redundant on V1). **Over-scan (documented, conservative):** Java's `addedDeleteFiles` also
  filters its `DeleteFileIndex` by `startingSequenceNumber`; this port uses the snapshot walk + inclusive-metrics
  filter only (can only over-reject). **Still deferred (separate follow-ups):**
  `OverwriteFiles.validateAddedFilesMatchOverwriteFilter` (block 1) + `validateNoConflictingDeletes` (block 3); the
  remaining RowDelta DELETE-file blocks (`validateNoNewDeletesForDataFiles`, `validateDataFilesExist`,
  `validateAddedDVs` — need `referenced_data_files`/`removed_data_files` on the action); `RewriteFiles`
  `validateNoNewDeletes`.
- **Inspection-table sub-sequence (dependency, then value):**
  1. **`files` family** (`files` / `data_files` / `delete_files`) — **DONE 🟡 (2026-06-08, Increment 1,
     `inspect/files.rs`).** Reads the current snapshot's manifest list → manifests → live entries →
     `DataFile`; the three differ ONLY by the manifest-content filter (Java `BaseFilesTable`). Arrow schema
     mirrors `DataFile.getType(partitionType)` (all raw columns incl. the metrics maps + V3 DV fields);
     `readable_metrics` deferred. 8 unit tests (content-filter + `is_alive()` mutation-verified).
  2. **`entries`** — the raw manifest-entry diagnostic view — **DONE 🟡 (2026-06-08, Increment 2,
     `inspect/entries.rs`).** Reads the current snapshot's `allManifests` (data AND delete) and emits EVERY
     entry — INCLUDING the `Deleted` tombstones (`status==2`) that `files` excludes (Java
     `ManifestEntriesTable`). Schema = Java `ManifestEntry.getSchema` (5 cols: `status`/0, `snapshot_id`/1,
     `sequence_number`/3, `file_sequence_number`/4, `data_file`/2 = the SAME data_file projection NESTED).
     The shared data_file projection (`data_file_fields` + `DataFileStructBuilder`) was factored into
     `inspect/data_file.rs` (Rule of Three — `files` flattens it, `entries` nests it); `readable_metrics`
     deferred. 6 unit tests (Deleted-tombstone-present + status + seq/snapshot-id vs committed + nested
     data_file + schema field ids). (`all_entries` — across ALL snapshots — folds into the `all_*` item.)
  3. **`history` + `refs` + `metadata_log_entries`** — pure-metadata tables (no manifest IO) — **DONE 🟡
     (2026-06-08, Increment 3, `inspect/{history,refs,metadata_log_entries}.rs`).** Each reads only
     `TableMetadata` fields and projects to a `RecordBatch`, mirroring `inspect/snapshots.rs`. `history` =
     one row per snapshot-log entry (`made_current_at`/`snapshot_id`/`parent_id`/`is_current_ancestor`,
     the ancestor flag = membership in the current-snapshot parent chain, Java `currentAncestorIds`);
     `refs` = one row per branch/tag (`name`/`type`/`snapshot_id` + retention, tags carry only
     `max_reference_age_in_ms`); `metadata_log_entries` = one row per metadata-log entry + a synthetic
     current entry, with `latest_*` resolved to the snapshot current at each entry's timestamp (Java
     `snapshotIdAsOfTime`). Field ids verbatim from Java `HistoryTable`/`RefsTable`/`MetadataLogEntriesTable`.
     10 unit tests (incl. a forked/rolled-back `is_current_ancestor==false` case). Interop deferred (→ ✅).
  4. **`partitions`** — per-partition aggregation over entries — **DONE 🟡 (2026-06-08, Increment 4,
     `inspect/partitions.rs`).** The first AGGREGATING table (Java `PartitionsTable`): groups the current
     snapshot's LIVE manifest entries (data + delete) by partition `Struct` and rolls up record/file/size +
     pos/eq-delete counts + `last_updated_*`/`spec_id` from the most-recent-commit file (strict `>`
     tie-break). 11 fields in Java column order with the exact non-sequential field ids
     (`partition`/1 … `last_updated_snapshot_id`/10). 10 unit tests; content-type / `>` / `is_alive`
     mutations caught. TWO scoping decisions: keeps the empty-struct partition column for unpartitioned
     tables (matches the `files` family; documented divergence), and DEFERS cross-spec partition-type
     unification (`Partitioning.partitionType`) as a known divergence (single-spec-correct path). Interop
     deferred (→ ✅).
  5. **`all_*`** (across ALL snapshots) — the four file/entry tables **`all_data_files` / `all_delete_files`
     / `all_files` / `all_entries` DONE 🟡 (2026-06-08, Increment 5a, `inspect/{files,entries}.rs` + the new
     shared `inspect/manifest_source.rs`)**: SAME projection/rows/schema as their current-snapshot
     counterparts, but the manifest source becomes the **deduplicated union of manifests reachable from ALL
     snapshots** (Java `BaseAllMetadataTableScan.reachableManifests`, dedup by `manifest_path`; files NOT
     deduped — "may return duplicate rows"). Modeled as a `MetadataScope {CurrentSnapshot, AllSnapshots}`
     axis orthogonal to `FilesTableKind`, with the manifest source factored into one shared
     `collect_manifest_files(table, scope)` helper so the two tables can't drift; 12 unit tests
     (cross-snapshot inclusion + manifest dedup + content filters + tombstones + schema parity + empty +
     a NON-ANCESTOR forked-sibling file in `all_files`), dedup + all-vs-current + all-vs-ancestry scope +
     content-filter mutation-pinned (the all-vs-ancestry pin added in REVIEW); interop deferred (→ ✅).
     **`all_manifests` DONE 🟡 (2026-06-08, Increment 5b, `inspect/all_manifests.rs`)** — the LAST
     inspection table: distinct machinery from the file/entry `all_*` (it iterates EACH snapshot's
     manifest list with NO dedup, one row per (manifest × referencing snapshot), Java javadoc "may return
     duplicate rows"), adds a `reference_snapshot_id`/18 column + `key_metadata`/19, and CONTENT-GATES the
     count columns (Java `AllManifestsTable.manifestFileToRow`). Surfaced + fixed a real content-gating
     bug in the regular `manifests` table (it appended counts to BOTH the data AND delete column families
     unconditionally — Java `ManifestsTable.manifestFileToRow` content-gates there too); the shared
     partition-summary builder/conversion was factored into `inspect/partition_summary.rs`. The
     inspection-table SET IS NOW COMPLETE; remaining inspection work is `readable_metrics` + interop.
- **Exit criteria:** scans match Java planning/results incl. residuals; inspection tables present; reports
  emitted.

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
   `ManageSnapshots` are all 🟡 — branch/tag lifecycle, rollback, rollback-to-time, fast-forward, retention,
   and column defaults landed; only the Java interop round-trip remains before ✅. `cherrypick` is
   Phase-2-gated — it extends `MergingSnapshotProducer` / replays data files).
3. **Format & type breadth** — ORC + Avro data files; remaining V3 types (variant, geo, `unknown`).
   (`timestamp_ns` and column default values already present — see GAP_MATRIX.)
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
