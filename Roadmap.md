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
   ref-operation surface of "Snapshot management" flipped to ✅). **`cherrypick` landed 🟡 in Phase-2
   increment 7 (2026-06-08)** — `transaction/cherry_pick.rs` replays a snapshot onto `main` via the
   `SnapshotProducer` write engine (it does extend the Java `MergingSnapshotProducer` analogue, which now
   exists); data-level Java interop is the only remaining gate to ✅. The interop surfaced
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
   write-as-given) — landed 🟡 on 2026-06-08. Increment 5b — the `RowDelta` action + producer
   delete-manifest handling — landed 🟡 on 2026-06-08 (the crown-jewel write→read merge-on-read chain).
   Increment 5c — the V3 deletion-vector writer (`delete_vector.rs` byte-level DV serialization +
   `writer/base_writer/deletion_vector_writer.rs`) — landed 🟡 on 2026-06-08, completing the merge-on-read
   WRITE story. Increment 9 — merge append (`MergeAppend` / `AppendFiles` merge mode: the `async`
   `ManifestProcess` seam + `MergeAppendAction` / `Transaction::merge_append()` bin-packing + merging small
   manifests per spec, Java `ManifestMergeManager`) — landed 🟡 on 2026-06-08. Next Phase-2 increments: wire
   the DV writer into RowDelta-for-V3 end-to-end, `RewriteManifests`. (V3 groundwork — row-lineage fields + the remaining `MIN_FORMAT_VERSIONS` types
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
**`cherrypick` landed 🟡 as Phase-2 increment 7 (2026-06-08)** — `transaction/cherry_pick.rs` replays a
snapshot onto `main` (fast-forward / append-replay / dynamic-overwrite) via the `SnapshotProducer` write
engine, mirroring Java `CherryPickOperation`; 8 `MemoryCatalog` unit tests; data-level Java interop is the
only remaining gate to ✅. **Phase 2 has started: `DeleteFiles` + the manifest-filter / rewrite machinery is 🟡**
(`transaction/delete_files.rs` + `SnapshotProducer::process_deletes`, `MemoryCatalog`-tested; data-level
Java interop deferred); **`OverwriteFiles` is 🟡** (`transaction/overwrite_files.rs` — explicit add + delete
in one `Overwrite` snapshot, reusing the shared resolve/list helpers; summary reflects added + deleted
counts; `overwriteByRowFilter` + conflict validation + interop deferred); **`ReplacePartitions` is 🟡**
(`transaction/replace_partitions.rs` — dynamic partition overwrite via a by-PARTITION sibling resolver
`SnapshotProducer::resolve_partition_deletes` feeding the same rewrite path; full replace on an
unpartitioned table; `replace-partitions` marker; static `replaceByRowFilter` + conflict validation +
interop deferred); **`RewriteFiles` is 🟡** (`transaction/rewrite_files.rs` — the compaction-commit
primitive: atomically replace a set of DATA files with a new set in one `Operation::Replace` snapshot,
reusing the by-path `resolve_delete_paths` + `process_deletes` machinery unchanged; delete set must be
non-empty (`failMissingDeletePaths`); DELETE-file rewrite + `dataSequenceNumber` preservation + conflict
validation + interop deferred); **merge append is 🟡** (`transaction/merge_append.rs` — `MergeAppend` /
`AppendFiles` merge mode: the `async` `ManifestProcess` producer seam + `MergeAppendAction` /
`Transaction::merge_append()` bin-pack + merge small manifests per partition spec, Java
`ManifestMergeManager`, with provenance-preserving entry copy; interop + delete-manifest merge deferred);
the rest of the **write engine (`RewriteManifests`), incremental scans, ORC/Avro data files,
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

### Phase 2 — Write engine  ·  **Status: 🟡 (in progress — `DeleteFiles` + manifest-filter machinery + `OverwriteFiles` + `ReplacePartitions` + `RewriteFiles` landed 2026-06-07; `PositionDeleteFileWriter` (RowDelta increment 5a) + `RowDelta` action (5b) landed 2026-06-08 — the merge-on-read write→read chain is now end-to-end; the concurrent-commit conflict-validation FOUNDATION + `ReplacePartitions.validateNoConflictingData` (increment 6) landed 2026-06-08 — the serializable-isolation safety layer; `cherrypick` (snapshot replay onto `main`: fast-forward / append-replay / dynamic-overwrite) landed 2026-06-08 as increment 7 — no longer Phase-2-gated; the V3 deletion-vector (DV) writer (increment 5c) landed 2026-06-08 — `DeleteVector::serialize`/`deserialize` at byte-level Java parity + `DeletionVectorFileWriter`, completing the merge-on-read WRITE story for V3; **merge append (`MergeAppend` / `AppendFiles` merge mode) landed 2026-06-08 as increment 9** — the `ManifestProcess` producer seam is now `async`, and `MergeAppendAction` / `Transaction::merge_append()` bin-packs + merges small manifests per partition spec (Java `ManifestMergeManager`) so the manifest count stays bounded, with the entry-copy preserving every entry's provenance)**
- **Goal:** the full commit/write surface beyond fast-append.
- **Gates on:** Phase 1.
- **Increment sequence (dependency, then value):** **1. `DeleteFiles`** (done 🟡 — delete data files by
  path/reference; built the foundational manifest-filter / rewrite machinery in `SnapshotProducer` that the
  rest reuse), **2. `OverwriteFiles`** (done 🟡 — explicit add + delete in one `Overwrite` snapshot,
  composing the fast-append add path with the `DeleteFiles` filter path via the now-shared
  `SnapshotProducer::{resolve_delete_paths, current_data_manifests}`; summary reflects added + deleted
  counts; `overwriteByRowFilter` + conflict validation + interop deferred), **3. `ReplacePartitions`**
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
  delta — the full merge-on-read write→read chain], 5c deletion-vector writer [**done 🟡 2026-06-08** —
  `delete_vector.rs`: `DeleteVector::serialize`/`deserialize` at byte-level Java parity with
  `BitmapPositionDeleteIndex.serialize` (`[len: 4 BE][magic 1681511377: 4 LE][portable roaring treemap:
  LE][CRC-32: 4 BE]`; treemap via `roaring::serialize_into` = Java `RoaringPositionBitmap`; inline CRC-32,
  no dep added); `writer/base_writer/deletion_vector_writer.rs`: `DeletionVectorFileWriter` writes ONE DV
  blob per referenced data file into a Puffin file and returns a `DataFile{content=PositionDeletes,
  format=Puffin, referenced_data_file, content_offset/content_size = the Puffin-footer blob offset/length,
  record_count=cardinality}` (mirrors `BaseDVFileWriter.createDV`); Puffin `add` now returns `BlobMetadata`
  + `close` returns the file size. Round-trip + byte-level + CRC + mutation tests prove the offset/size index
  the blob and the bytes round-trip. **Deferred:** RowDelta-for-V3 e2e (a V3 DV is a DeleteFile → composes),
  `BaseDVFileWriter` multi-data-file batching, Java interop round-trip]**),
  **6. concurrent-commit conflict-validation FOUNDATION + `ReplacePartitions.validateNoConflictingData`**
  (**done 🟡 2026-06-08** — the serializable-isolation safety layer: `Transaction` captures
  `starting_snapshot_id` (surviving the `do_commit` re-base); a default-no-op `TransactionAction::validate`
  hook runs in `do_commit` against the refreshed base BEFORE re-apply; a conflict is a non-retryable
  `DataInvalid` (Java's non-retryable `ValidationException`) so the retry loop stops; shared
  `added_data_files_after` helper enumerates the concurrent commits' added DATA files (Java
  `MergingSnapshotProducer.addedDataFiles`/`ancestorsBetween`); `ReplacePartitions` gained the opt-in
  `validate_no_conflicting_data()`/`validate_from_snapshot(id)` rejecting a concurrent append into a
  replaced partition. **Conflict-validation sub-sequence:** (6) this; then `OverwriteFiles`
  `validateNoConflictingData`/`...Deletes`; `RowDelta`/`DeleteFiles` `validateDataFilesExist`; `RewriteFiles`
  `validateNoNewDeletes`),
  **7. `cherrypick`** (**done 🟡 2026-06-08** — snapshot replay onto `main`, the freshly-unblocked commit
  that needed the `MergingSnapshotProducer`-equivalent (the `SnapshotProducer` add+delete seam):
  `transaction/cherry_pick.rs` `CherryPickAction` / `Transaction::cherry_pick(snapshot_id)` mirrors Java
  `CherryPickOperation.cherrypick` across all three modes — **fast-forward** (source's parent IS the current
  head → move `main`, NO new snapshot), **APPEND replay** (re-add the source's added data files in a new
  `Append` snapshot, set `source-snapshot-id` + `published-wap-id`), **dynamic-OVERWRITE replay**
  (`replace-partitions` source → re-add added + re-delete removed files in a new `Overwrite` snapshot);
  `validateNonAncestor` + `WapUtil.validateWapPublish` enforced; new `snapshot.rs` helpers
  `added_data_files_by_snapshot`/`removed_data_files_by_snapshot`. 8 `MemoryCatalog` unit tests assert the
  post-commit scan shows the picked data. `validateReplacedPartitions` (the dynamic-overwrite concurrent-change
  partition scan) + data-level Java interop deferred),
  8. `RewriteManifests`, merge append,
  9. multi-op transaction hardening + optimistic-concurrency retry on the real catalogs.
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
   and column defaults landed; only the Java interop round-trip remains before ✅. `cherrypick` landed 🟡 as
   Phase-2 increment 7 — `transaction/cherry_pick.rs` replays a snapshot onto `main` via the write engine).
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
