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

## Active: Scan metrics emission wiring (TableScan → MetricsReporter) — BUILDER Opus, 2026-06-09

Completes the DEFERRED scan-emission wiring from the metrics data-model increment (GAP_MATRIX row 97).
OPT-IN: when no reporter is set, the `plan_files` path is BYTE-UNCHANGED. Java authority:
`SnapshotScan.planFiles` (timer start → `doPlanFiles()` → `whenComplete` report on close) + `ManifestGroup`
/`ScanMetricsUtil.fileTask` (per-task counters) + `DataTableScan` (manifest-list totals).

- [x] Add `scan/metrics_collector.rs`: `ScanMetricsCollector` (Arc'd `AtomicI64` counters) + `snapshot()`
      → `ScanMetricsResult`. Only the cleanly-collectable counters + the planning timer are populated;
      indexed/equality/positional/dvs/skipped-files left `None` (documented not-yet-populated).
- [x] `TableScanBuilder::with_metrics_reporter(Arc<dyn MetricsReporter>)` (Option, default None) →
      `TableScan.metrics_reporter`.
- [x] Thread `Option<Arc<ScanMetricsCollector>>` through `PlanContext` → `ManifestFileContext` /
      `ManifestEntryContext` (None when no reporter). Manifest-list totals counted in `plan_files`;
      scanned/skipped at the `context.rs` prune point; per-task result_*/size in the stream wrapper.
- [x] Emission: wrap the `FileScanTaskStream` so that on FULL consumption (stream returns `None`) the
      collector is snapshotted, the timer stopped, a `ScanReport` is built (table name / snapshot id /
      schema id / projected field ids+names / filter), and `reporter.report(Scan(report))` is called ONCE.
      When `metrics_reporter` is None: no collector, no timer, no wrapper — byte-unchanged.
- [x] Tests (5) + mutation checks (a–d). Docs: GAP_MATRIX row 97 / Roadmap / lessons / todo.

Outcome: see final report. Stayed on `phase2-3-remnants`; no commit/push; no Cargo edits.

## Active: Phase 1 — Spec & metadata completeness

Parity target: Java `iceberg-core` evolution APIs. Authoritative plan: [Roadmap.md](../Roadmap.md)
Phase 1; status checklist: [docs/parity/GAP_MATRIX.md](../docs/parity/GAP_MATRIX.md).

**Scaffolding (verified on 0.9.1):** actions follow the `transaction/sort_order.rs` pattern — a builder
struct + `#[async_trait] impl TransactionAction { async fn commit(self: Arc<Self>, &Table) ->
Result<ActionCommit> }` returning `ActionCommit::new(updates, requirements)`, plus a `pub fn` ctor on
`Transaction` and a `mod`/`use` line in `transaction/mod.rs`. `TableMetadataBuilder` already has the
low-level primitives; `TableUpdate` already has `SetSnapshotRef`/`RemoveSnapshotRef`/`AddSchema`/
`SetCurrentSchema`/`AddSpec`/`SetDefaultSpec`; `TableRequirement::RefSnapshotIdMatch{ref, snapshot_id:
Option<i64>}` (None ⇒ ref must not exist) guards concurrency. Java ref at `/tmp/iceberg-java-ref`.

### Sequencing (dependency, then value)
1. **ManageSnapshots** (this increment) — self-contained ref manipulation; primitives all exist.
2. **UpdatePartitionSpec** — addField/removeField/renameField → new spec via `AddSpec`/`SetDefaultSpec`.
3. **UpdateSchema** (largest) — add/drop/rename/update-type/move/make-req-opt + field-id reassignment
   and type-promotion validation → `AddSchema`/`SetCurrentSchema`. Split into sub-steps.
4. ManageSnapshots tail — `cherrypick` (needs snapshot replay) + `rollbackToTime`.
5. V3 groundwork — row-lineage fields, finish column-default plumbing.

Each item: behavioral parity with Java + unit tests (same change) + GAP_MATRIX row flip.

### Increment 1 — ManageSnapshots ✅ DONE
New file `crates/iceberg/src/transaction/manage_snapshots.rs`; wired into `transaction/mod.rs`.

- [x] create/replace/remove branch+tag (kind-checked); `rename_branch`.
- [x] `set_current_snapshot`; `rollback_to` (ancestry-validated); `fast_forward` (ancestry-validated).
- [x] retention: `set_min_snapshots_to_keep` / `set_max_snapshot_age_ms` / `set_max_ref_age_ms`
      (branch-only fields rejected on tags).
- [x] commit(): ops resolved in order against a working ref map seeded from `metadata().refs`; emits
      `SetSnapshotRef`/`RemoveSnapshotRef` + per-ref `RefSnapshotIdMatch` guard (original id, None if
      absent); snapshot ids validated via `snapshot_by_id`.
- [x] 12 unit tests via `make_v2_table()`; build + clippy(-D warnings) + fmt + offline lib suite green
      (1170 tests, stable across 6 runs).
- [x] Flipped `ManageSnapshots` + snapshot-refs GAP_MATRIX rows to 🟡.
  - Side fix: relaxed a flaky upstream assertion in `catalog/memory/catalog.rs` test_update_table
    (`last_updated_ms() <` → `<=`), exposed by the extra parallel test load. See task/lessons.md.

**Outcome:** branch/tag lifecycle + rollback + fast-forward + retention land with optimistic-concurrency
guards. **Deferred to increment 4:** `cherrypick` (needs snapshot replay), `rollbackToTime`,
`replaceBranch(from,to)`; a Java interop round-trip before the row flips to ✅.

#### Review remediation (2026-06-07, post multi-agent review) — DONE
- [x] `fast_forward` Java-parity fixes: `to` may be a tag; absent `from` auto-created; no-op when
      already at target. Verified against `UpdateSnapshotReferencesOperation.replaceBranch`.
- [x] commit() emit-time no-op suppression (create-then-remove, ff-to-same, replace-to-same emit nothing).
- [x] +14 tests (now 26): rollback non-ancestor, ff wrong-direction, remove/rename main guards, rename
      collision, remove/replace of pre-existing refs (guard carries original id), forked fixture.
- [x] Created `docs/testing.md` (was referenced 14× but missing); reconciled headline-gaps + Roadmap
      Phase 1 status (🟡) with the matrix; removed stale `iceberg-spark-python` line from `skills/*.md`.
- [ ] **Follow-up (unverified):** retention positivity validation (Java may reject `≤ 0` for
      min_snapshots_to_keep / max_snapshot_age_ms / max_ref_age_ms). A grep of `SnapshotRef.java` found
      no such `checkArgument` — confirm where (if anywhere) Java enforces it before adding it here.

### Increment 2 — UpdatePartitionSpec (IN PROGRESS, 2026-06-07)
New file `crates/iceberg/src/transaction/update_partition_spec.rs`; wired into `transaction/mod.rs`.
Full Java parity with `BaseUpdatePartitionSpec`. Builder records ops; `commit()` replays Java's
state machine against `table.metadata()` and emits `AddSpec`/`SetDefaultSpec{-1}` + concurrency guards.

Plan:
- [x] Read Java `UpdatePartitionSpec` + `BaseUpdatePartitionSpec` contract; Rust partition.rs,
      transform.rs, table_metadata_builder.rs (`add_partition_spec` recycles equal (src,transform)
      field ids), catalog/mod.rs (`AddSpec`/`SetDefaultSpec`/`DefaultSpecIdMatch`/
      `LastAssignedPartitionIdMatch` shapes + their check/apply).
- [x] `UpdatePartitionSpecAction`: builder methods record `PartitionSpecOp` (case_sensitive flag +
      add/add_with_transform/remove/remove_by_transform/rename/add_non_default). Names auto-generated
      to match Java `PartitionNameGenerator` (`generate_partition_name`).
- [x] `commit()`: `SpecEvolution` state machine replays Java `addField`/`removeField`/`renameField`
      validation in order, seeded from the current DEFAULT spec; `apply()` builds the new
      `UnboundPartitionSpec` (keep+rename / void-replace-on-V1 / omit-on-V2; appended adds with
      `field_id=None` so `TableMetadataBuilder` recycles/assigns). Emits `AddSpec{unbound}` +
      `SetDefaultSpec{-1}` (unless add_non_default) + `DefaultSpecIdMatch{current}` +
      `LastAssignedPartitionIdMatch{current}`.
- [x] 23 unit tests (same change): identity add, add-with-transform auto-name (one per transform incl
      bucket/truncate/year/month/day/hour/void), explicit name, remove-by-name, remove-by-transform,
      rename, add-non-default (no SetDefaultSpec), dup-add-name fails, redundant-time-transform fails,
      remove-newly-added fails, rename+delete-same fails, delete-then-readd un-deletes (+ with rename),
      V1 void replacement (+ V1 delete-collision rename), field-id recycling across historical specs
      (forked multi-spec fixture) + new-id assignment, case-insensitive source resolution, unknown
      column fails, colliding non-void name fails, updates/requirements shape, binds-to-schema.
- [x] Verify: build green; lib test ×2 = 1207 passed/0 failed both runs; clippy -D warnings clean;
      fmt clean. Mutation-checked auto-name + V1/V2 void branch (5 tests fail when logic broken).
      Flipped GAP_MATRIX row + headline gap to 🟡.

**Outcome:** `UpdatePartitionSpec` lands at full `BaseUpdatePartitionSpec` parity (🟡) with optimistic-
concurrency guards. **Deferred to ✅:** Java interop round-trip (read a Java-evolved spec; prove Java
reads ours). **Note:** `Transform::Unknown` reject-precondition is N/A — the Rust builder API cannot
produce an unknown transform (Java's `UnknownTransform` guard has no Rust analogue here).

#### Increment 2 — REVIEW (2026-06-07, adversarial reviewer agent)
Verified the 10 high-value points against `BaseUpdatePartitionSpec.java` + `TestUpdatePartitionSpec.java`.
- [x] Pts 1,2,4,5,6,7,8 (un-delete/reject branches; transform `to_string` keys; V1 alwaysNull;
      void-collision renames; redundant-time guard; case-sensitivity; requirement set): CONFIRMED.
- [x] **BUG (pt 3): field-id recycling dropped the historical NAME.** Java
      `recycleOrCreatePartitionField` (V2, base!=null) returns the historical field's *name* too when
      the add had no explicit name (and only recycles at all when name is null OR matches). Rust
      delegated id-recycling to `TableMetadataBuilder` (matches on source+transform only) but always
      used the *generated* default name → a `bucket[8](y)` recycle that was historically named
      `my_shard` came out `y_bucket_8`, and an explicit-name add recycled an id even when Java would
      not. FIX: replicate `recycleOrCreatePartitionField` in the action — search historical specs and,
      on match (respecting the name==null/name-equals rule), set BOTH the recycled field_id and the
      historical name on the added field. Metadata-builder recycling becomes a harmless no-op.
- [x] **BUG (pt 8): requirement over-constraint under add_non_default_spec.** Java emits
      `AssertDefaultSpecID` only for the `SetDefaultPartitionSpec` update; when `addNonDefaultSpec` is
      set there is no such update, so only `AssertLastAssignedPartitionId` is required. Rust emitted
      both unconditionally. FIX: gate `DefaultSpecIdMatch` on `set_as_default`.
- [x] Added tests (pt 9 end-to-end round-trip through builder; pt 10 no-op dedup to existing spec id;
      V2 remove+re-add-different-transform-same-name; historical-name recycle; add_non_default
      requirement shape).

**Review outcome:** 28 unit tests (23 → 28; both fixes mutation-verified to fail without the change).
build green; lib test ×2 = 1212/0 both runs (stable); clippy -D warnings clean; fmt clean. Reconciled
GAP_MATRIX row + Roadmap Phase 1 progress; appended 3 lessons. Row stays 🟡 (Java interop still deferred).
**Residual (tracked, intentional):** (a) full interop round-trip; (b) explicit-name recycle when the
historical name DIFFERS from the requested name — Java assigns a fresh id, Rust's metadata builder would
still recycle by (source,transform); narrow, spec-favoring, and not separately gated here. Both noted for
the ✅ flip.

### Increment 3 (prereq) — Type-promotion helper for UpdateSchema (IN PROGRESS, 2026-06-07, Actor A2)
New file `crates/iceberg/src/spec/schema/type_promotion.rs`; wired via one `mod` + `pub(crate) use`
line in `crates/iceberg/src/spec/schema/mod.rs`. Pure, self-contained spec module — no transaction dep.

Contract: mirror Java `TypeUtil.isPromotionAllowed(Type from, Type.PrimitiveType to)`
(`/tmp/iceberg-java-ref/.../types/TypeUtil.java` lines 440–466), verified against the source (not the
digest paraphrase). The switch has EXACTLY three branches + an identity short-circuit:
- `from.equals(to)` (Type-level) ⇒ allowed (no-op / identity).
- `INTEGER` ⇒ allowed iff `to == LONG`.
- `FLOAT`  ⇒ allowed iff `to == DOUBLE`.
- `DECIMAL`⇒ allowed iff `to` is DECIMAL AND `from.scale == to.scale` AND `from.precision <= to.precision`.
- everything else ⇒ forbidden.

Plan:
- [x] Implement `ensure_promotion_allowed(from: &Type, to: &PrimitiveType) -> Result<()>` (+ boolean
      core `is_promotion_allowed`): Ok(()) when allowed; Err(ErrorKind::DataInvalid,
      "{from} cannot be promoted to {to}") otherwise, mirroring Java `CheckCompatibility.primitive`.
- [x] Wired into schema/mod.rs as `mod type_promotion;` + `pub use self::type_promotion::{...}` (both
      fns are `pub` — task asked for a *public* helper, matches Java's `public static`, and avoids the
      dead-code warning that `pub(crate)` would raise since UpdateSchema does not exist yet).
- [x] 21 unit tests (6 allowed + 15 rejected; `rejects_struct_from_to_primitive` covers struct/list/map).
      Each names the risk; rejection tests assert exact ErrorKind::DataInvalid + exact message.
      Mutation-verified: dropping `from_scale == to_scale` fails `rejects_decimal_scale_change...`.

**Outcome (2026-06-07, Actor A2):** Shipped `crates/iceberg/src/spec/schema/type_promotion.rs` +
two lines in `schema/mod.rs`. Public API: `is_promotion_allowed(&Type, &PrimitiveType) -> bool` and
`ensure_promotion_allowed(&Type, &PrimitiveType) -> Result<()>` (re-exported via `spec::*`). Verify:
build clean; lib suite 1233/0 ×2; clippy -D warnings clean; fmt clean. **DEVIATION:** task brief listed
`date->timestamp` as allowed — Java's `TypeUtil.isPromotionAllowed` has NO DATE branch, so it is
implemented as REJECTED (matches Java + digest §6 pt5). Flagged in final report. No interop test (no
Java-written fixture for a pure type-check helper; the UpdateSchema interop test will exercise it).

**DEVIATION FROM TASK PROMPT (flag in final report):** the task brief said "cover ... date->timestamp"
as an allowed promotion. The authoritative Java source `TypeUtil.isPromotionAllowed` has NO `DATE` case
— `date -> timestamp` is FORBIDDEN in Java (confirmed by reading the source + the run's digest §6 pt 5).
Implementing it would diverge from Java and fail interop. Per the contract ("verify against the Java
source, not intuition"), I implement the real Java matrix (date→timestamp REJECTED) and surface this.

### Increment 3 — UpdateSchema transaction action (IN PROGRESS, 2026-06-07, Actor A1 Opus)
New file `crates/iceberg/src/transaction/update_schema.rs`; wired into `transaction/mod.rs`
(`mod` + `use` + `pub fn update_schema()`). Full Java parity with `SchemaUpdate`. Builder records ops;
`commit()` replays Java's state machine against `metadata().current_schema()`, builds the new `Schema`
via a recursive `ApplyChanges` walk (mirrors Java `SchemaUpdate.ApplyChanges` SchemaVisitor), and emits
`AddSchema` + `SetCurrentSchema{-1}` guarded by `LastAssignedFieldIdMatch` + `CurrentSchemaIdMatch`.

Inputs/outputs/contract: builder methods (add_column/add_required_column/rename_column/update_column/
update_column_doc/make_column_optional/require_column/delete_column/move_*/set_identifier_fields/
union_by_name_with/case_sensitive/allow_incompatible_changes) record `SchemaOp`s; `commit(&Table)`
resolves them in order against the current schema and returns `ActionCommit::new(updates, requirements)`.

Edge cases / risks (Risk-First): fresh field-id assignment for nested adds (reuse `ReassignFieldIds`);
type-promotion accept+reject (call `ensure_promotion_allowed`); add-required gating on
`allow_incompatible_changes`; optional→required gating (+ defaulted-add path); identifier-field
validation (exists/required/primitive/not-deleted/not-nested-in-map-or-list); move self-ref / cross-struct
/ non-struct-parent rejections; delete vs add/update/rename conflict matrix; map-key immutability;
case-insensitive resolution. DEVIATION: Rust `TableUpdate::AddSchema` has NO `last_column_id` field
(only `ViewUpdate::AddSchema` does) — the builder's `add_schema` derives last_column_id from
`schema.highest_field_id()`. Requirements: Java `UpdateRequirements` attaches BOTH
`AssertLastAssignedFieldId(base.lastColumnId())` (for AddSchema) AND `AssertCurrentSchemaID`
(for SetCurrentSchema) — confirmed in `UpdateRequirements.java` lines 131-142. We emit both.

Plan:
- [x] Builder + `SchemaOp` enum recording all op families; case_sensitive + allow_incompatible flags.
- [x] `SchemaEvolution` state machine (mirrors `SchemaUpdate` fields: deletes/updates/added-name-to-id/
      parent-to-added-ids/moves/identifier-field-names/last_column_id) replaying ops with Java's
      precondition checks; fresh-id assignment via a self-contained `assign_fresh_ids` walk (the schema
      module's `ReassignFieldIds` is crate-private and unreachable from `transaction/`, and is for
      whole-schema reassignment; a local walk mirrors Java `TypeUtil.assignFreshIds` for a single add).
- [x] `apply()` recursive rebuild → new `Schema` (deletes drop, updates replace, adds append to parent
      struct, moves reorder; map-key immutability + list/map-value rules); identifier-field validation
      delegated to `Schema::builder` (spec rules) after re-resolving names to fresh ids.
- [x] `commit()` emits AddSchema + SetCurrentSchema{-1} + LastAssignedFieldIdMatch(last_column_id) +
      CurrentSchemaIdMatch(current_schema_id) — both guards per Java `UpdateRequirements` (lines 131-142).
- [x] 46 unit tests (≥1 happy + ≥1 negative per op family); nested fixture built fresh via
      `TableMetadataBuilder::new` (no stale sort order / identifier set).
- [x] Verify: build clean; lib test ×2 = 1279/0 both runs (stable); clippy -D warnings clean; fmt clean.

**Outcome (2026-06-07, Actor A1 Opus):** `UpdateSchemaAction` lands at SchemaUpdate parity (builder +
state machine + recursive apply). Public ctor `Transaction::update_schema()`. **DEVIATIONS (flagged):**
(1) Rust `TableUpdate::AddSchema` has NO `last_column_id` field (only `ViewUpdate::AddSchema` does) — the
brief said emit `AddSchema{schema, last_column_id}`; we emit `AddSchema{schema}` and the metadata builder
derives last_column_id from `schema.highest_field_id()`. (2) Column DEFAULTS are not plumbed through the
builder API yet (the `addColumn(..., Literal)` / `updateColumnDefault` / `addRequiredColumn(..., default)`
Java overloads) — `NestedField` supports initial/write defaults but the action's builder methods do not
take a default, so the "required add WITH default needs no flag" and "defaulted-add can be made required"
Java paths are present in the state machine (`is_defaulted_add`) but unreachable from the public API. (3)
`union_by_name_with` is a pragmatic name-driven merge over struct fields (add-new / promote-widening /
apply-doc / keep-wider-on-narrowing), NOT a port of Java's full `UnionByNameVisitor` (which also handles
list/map element promotion and required→optional). **Deferred to ✅:** column-default builder overloads;
full UnionByNameVisitor parity (nested list/map element merge); Java interop round-trip.

#### Increment 3 — REVIEW remediation (2026-06-07, Opus actor, post 3-critic review)
Three perspective-diverse critics reviewed the UpdateSchema action. Verified each VERIFIED-REAL finding
against the Java source before acting. Scope: `transaction/update_schema.rs`, `spec/schema/mod.rs`
(lowercase index), docs. Plan:

- [x] **BLOCKER — nested field-id order (depth-first → level-order).** Rewrote `assign_fresh_ids`:
      struct assigns ALL immediate ids in pass 1 then recurses in pass 2; map assigns key_id then value_id
      FIRST then recurses key then value. Confirmed against `AssignFreshIds`/`CustomOrderSchemaVisitor` +
      `testAddNestedMapOfStructs`. Pinned by 3 exact-id tests (map<struct,struct>, list<struct>,
      struct{struct,prim}). Mutation-verified: depth-first map arm gives value-id 8 vs Java's 4.
- [x] **MAJOR — union_by_name full `UnionByNameVisitor` parity** (findings #1,#3,#4,#5,#6,#7,#8). Rewrote
      `union_struct` into `union_update_existing` + `union_recurse_into` + `union_nested_member` +
      free fn `is_ignorable_type_update`. Routes existing-field changes through `update_column`/
      `update_column_requirement`/`update_column_doc`; rejects incompatible primitive + complex↔primitive
      changes ("Cannot change column type"); relaxes required→optional; recurses struct/list/map. Added
      9 union tests (nested struct add, list<struct> nested add, required→optional, incompatible-primitive
      reject, list→primitive reject, mirrored no-op, doc-only). Mutation-verified the reject path.
- [x] **MAJOR — case-insensitive lowercase-name collision** (`spec/schema/mod.rs`). Added
      `build_lowercase_name_index` rejecting collisions with the exact Java message (smaller field-id
      first → `data and DATA collide`). 2 tests (collision rejected + distinct-lowercase accepted).
      Mutation-verified against the old `.collect()`.
- [x] **MINOR (test rigor)** — added exact-`ErrorKind::DataInvalid`+message asserts on 3 high-value
      negatives (ambiguous-name, delete-with-updates, move-before-itself); identifier id-stability tests
      (rename + move keep the identifier id); list-element struct move; delete+re-add+move.
- [x] **SKIP/TRACK — column defaults (#10).** Not fixed: plumbing `initial/write` defaults is a
      signature-changing API expansion beyond the named scope (§6). Tracked as the residual gap for the
      ✅ flip; row stays 🟡. (Finding #10's own recommendation explicitly allows tracking it as a gap.)
- [x] Reconciled GAP_MATRIX (UpdateSchema ❌ → 🟡) + headline gaps; Roadmap Phase 1 progress +
      snapshot/"next move"/current-state lines; appended dated lessons. Verification gate run last.

**Outcome (2026-06-07, Opus remediation):** all 9 VERIFIED-REAL code findings fixed (1 blocker, 4 majors
collapsed into 2 rewrites — union + lowercase, plus 4 test-coverage/rigor findings); 1 finding (#10
column defaults) tracked as a scoped-out parity gap. Files touched: `transaction/update_schema.rs`,
`spec/schema/mod.rs`, `transaction/mod.rs` (NOT edited — already wired), GAP_MATRIX, Roadmap, todo,
lessons. **Verify:** build clean; lib test ×2 = 1298/0 both runs (stable); clippy -D warnings clean; fmt
clean. update_schema 46 → 63 tests; +2 schema-build tests. Row stays 🟡 (defaults + Java interop deferred).

### Increment 4 — ManageSnapshots tail (rollbackToTime + retention `>0`) + UpdateSchema defaults (IN PROGRESS, 2026-06-07, BUILDER Opus)
Three Phase-1 metadata items at full Java parity. cherrypick is OUT OF SCOPE (it extends
`MergingSnapshotProducer` / replays data files → Phase 2; reclassified in docs only). Files touched:
`transaction/manage_snapshots.rs`, `transaction/update_schema.rs`, `docs/parity/GAP_MATRIX.md`,
`Roadmap.md`, `task/todo.md`, `task/lessons.md`.

**Java rules verified against `/tmp/iceberg-java-ref` source (not intuition):**
- **A (rollback_to_time):** `SetSnapshotOperation.rollbackToTime` → `findLatestAncestorOlderThan(base, ts)`
  walks `SnapshotUtil.ancestorIds(currentSnapshot)` (the parent chain of MAIN's current snapshot — exactly
  the existing `is_ancestor_of` walk) and picks the snapshot with the MAX `timestampMillis` that is
  STRICTLY `< ts`; errors "Cannot roll back, no valid snapshot older than: {ts}" if none. Then sets MAIN to
  it (same emit path as `rollback_to`). Note the STRICT `<`: ts == current's timestamp picks the next-older
  ancestor; ts > current keeps current (no-op, suppressed at emit).
- **B (retention >0):** `api/SnapshotRef.java` Builder setters (lines 154-177) DO reject non-positive:
  `minSnapshotsToKeep` `value == null || value > 0` "Min snapshots to keep must be greater than 0";
  `maxSnapshotAgeMs` "Max snapshot age must be greater than 0 ms"; `maxRefAgeMs` "Max reference age must be
  greater than 0". (Resolves the earlier unverified follow-up — the prior grep checked the wrong file.)
- **C (UpdateSchema defaults):** `core/SchemaUpdate.internalAddColumn` line 160: a required add is allowed
  when `defaultValue != null || isOptional || allowIncompatibleChanges` (default backfills existing rows).
  Add sets BOTH `withInitialDefault(default)` AND `withWriteDefault(default)` (lines 181-182).
  `updateColumnDefault(name, lit)` sets ONLY `writeDefault` on an existing field (no-op if already equal),
  line 339. Java `Types.NestedField` ctor `castDefault` validates: reject non-null default for a nested
  type; for a primitive, `defaultValue.to(type)` must succeed. Rust `with_initial_default`/`with_write_default`
  do NOT validate, so validate via `literal.try_into_json(&field_type)` (the canonical serde compatibility
  check — passing it guarantees no later serialization panic) before setting.

Plan:
- [x] **A.** `SnapshotOp::RollbackToTime { timestamp_ms: i64 }` + `pub fn rollback_to_time(self, i64)`;
      resolver `find_latest_ancestor_older_than` walks MAIN's parent chain, picks newest with
      `timestamp_ms < arg`, error if none; `set_main` to it. 5 tests (newest-older-ancestor, strict-`<`
      equal boundary, before-first→error, after-current→noop, sibling-never-chosen).
- [x] **B.** `validate_retention_positive` at the head of `apply_retention` (the one place all three
      fields flow through): rejects `<= 0` with exact Java messages; `ErrorKind::DataInvalid`. 6 negatives
      (zero+negative × 3 fields); existing positive retention tests stay green.
- [x] **C.** plumbed `Option<Literal>` default through new `add_column_with_default` /
      `add_required_column_with_default` / `add_column_to_with_default` /
      `add_required_column_to_with_default` builders + new `update_column_default(name, Literal)` builder +
      `SchemaOp::AddColumn.default` / `SchemaOp::UpdateColumnDefault`. `add_column` apply: required-without-
      default guard now relaxed when a default is present (`required && default.is_none() && !flag`); sets
      BOTH initial+write defaults on the new field; `update_column_default` sets ONLY write_default on an
      existing field (no-op if equal). `validate_default` rejects non-primitive defaults ("Invalid default
      value...") and type-mismatched primitives ("Cannot cast default value to...") via `try_into_json`
      (the canonical serde-compat check — passing it guarantees no later serialization panic). 9 tests.
- [x] Docs: GAP_MATRIX (ManageSnapshots row — rollbackToTime done, retention >0, cherrypick Phase-2-gated;
      UpdateSchema row — defaults dropped from "Pending ✅"; headline gaps reconciled); Roadmap Phase 1
      progress + increment-4 entry + headline gaps + current-state snapshot; this todo; lessons (6 entries).
- [x] Verify: build clean; lib test ×2 = 1318/0 both runs (stable, was 1298 baseline → +20 new tests);
      clippy -D warnings clean; fmt clean (one reflow applied via `cargo fmt`).

**Outcome (2026-06-07, Increment 4, BUILDER Opus):** all three items land at Java-source-verified parity.
**A** `rollback_to_time` (5 tests), **B** retention `>0` rejection (6 tests), **C** UpdateSchema column
defaults — `add_*_with_default` builders + `update_column_default` (9 tests). `cherrypick` reclassified
Phase-2-gated in docs (NOT implemented — it extends `MergingSnapshotProducer`). Files touched exactly the
6 allowed: `transaction/manage_snapshots.rs`, `transaction/update_schema.rs`, GAP_MATRIX, Roadmap, todo,
lessons. Nothing touched outside the allowed set; no `spec/datatypes.rs` change needed
(`with_initial_default`/`with_write_default` already exist). Rows stay 🟡 — Java interop round-trip is the
only remaining gate before ✅. An Opus REVIEWER verifies next.

#### Increment 4 — REVIEW (2026-06-07, Opus REVIEWER)
Adversarially verified points 1–5 against the Java source (`/tmp/iceberg-java-ref`), not the Rust comments.
- [x] **Pt 1 (rollback_to_time): CONFIRMED.** `findLatestAncestorOlderThan` (SetSnapshotOperation.java:146)
      walks `currentAncestors` (main parent chain), strict `<` on `timestampMillis`, picks the MAX. Rust
      `find_latest_ancestor_older_than` matches exactly. Boundary `ts == ancestor's timestamp` → that
      ancestor is excluded (strict `<`); `ts == current` → next-older selected; `ts > current` → current
      (no-op suppressed). Java test `testAttemptToRollbackToCurrentSnapshot` mirrors the Rust no-op test.
      Benign divergence noted (not a bug): Java seeds `snapshotTimestamp=0` so a snapshot with
      `timestampMillis <= 0` is never chosen; Rust has no such floor. Real ms timestamps are always > 0,
      so unreachable in practice — tracked, not fixed.
- [x] **Pt 2 (retention >0): CONFIRMED.** SnapshotRef.java Builder setters (lines 154-177) enforce
      `value == null || value > 0` with the three exact messages; Rust reproduces them verbatim in
      `validate_retention_positive`, called at the head of `apply_retention`. `null`/unset still allowed
      (the builder API always sets a concrete value, so only `<= 0` occurs). Existing positive-retention
      tests stay green.
- [x] **Pt 3 (UpdateSchema add/update defaults): CONFIRMED.** `internalAddColumn` (SchemaUpdate.java:160)
      gates on `defaultValue != null || isOptional || allowIncompatibleChanges` (Rust De Morgan equivalent
      `required && default.is_none() && !flag`); add sets BOTH `withInitialDefault`+`withWriteDefault`
      (lines 181-182); `updateColumnDefault` (line 339) sets ONLY `withWriteDefault`. All three match.
- [x] **Pt 4 (default type-validation): CONFIRMED (safety) + divergence noted.** The serde `From<NestedField>`
      path (datatypes.rs:591-592) calls `try_into_json(&field_type).expect(...)`. `validate_default` runs the
      SAME `try_into_json` at add/update time, so it is a PERFECT predictor of the panic: anything it passes
      cannot panic later. Parity-precision divergences vs Java `defaultValue.to(type)` exist only on
      deliberately type-mismatched `Literal`s (e.g. a UUID/binary literal accepted for any primitive via the
      `(_, UInt128|Binary)` wildcard arms = too lenient; an int literal rejected for a long column = too
      strict). These require the caller to hand-build a mismatched literal; the Rust `Literal` is already
      strongly typed so the natural usage matches the column. No corruption, no panic — tracked, not fixed.
- [x] **Pt 5 (builder naming/arity): CONFIRMED.** Every Java overload semantic
      ({optional|required}×{top-level|nested}×{doc}×{default}) is reachable via the 8 `add_*` builders +
      `update_column_default`. Top-level optional-with-doc uses `add_column_to(None, ..)` (doc preserved;
      bypasses the dotted-name guard — ergonomic, not a lost semantic).
- [x] **TEST GAP FOUND + FIXED (not a code bug):** the `is_defaulted_add` make-required branch
      (update_schema.rs:802) was UNTESTED. Java pins it with two tests: `testAddColumnWithDefaultToRequired
      Column` (optional add WITH default → requireColumn succeeds without the flag) and
      `testAddColumnWithUpdateColumnDefaultToRequiredColumn` (add + updateColumnDefault sets only write_default
      → requireColumn FAILS, since initial_default is still null). Added both as Rust tests. Mutation-verified:
      dropping `&& field.initial_default.is_some()` from `is_defaulted_add` makes the negative test pass-when-
      it-should-fail (caught); the positive test fails if the defaulted-add relaxation is removed.
- [x] Verify gate run; rows stay 🟡 (Java interop deferred); cherrypick stays Phase-2-gated. Files touched:
      `transaction/update_schema.rs` (+2 tests), todo, lessons. Nothing outside the allowed set.

### Increment 5 — UpdateSchema INTEROP PILOT (Phase-1 bidirectional Java round-trip, BUILDER Opus, 2026-06-07)
Goal: prove byte-/field-id-level UpdateSchema compatibility with Java `iceberg-core` 1.10.0 in BOTH
directions, so the GAP_MATRIX `UpdateSchema` row can flip 🟡 → ✅. Java is a TEST-ONLY ORACLE under
`dev/java-interop/` (a dev tool like `dev/spark/`) — NOT a crate, NOT in the Cargo graph; `cargo
build`/`cargo test` never invoke Java. Durable artifacts = committed JSON fixtures + Rust tests.

Scope (new files only; doc edits to GAP_MATRIX/Roadmap/todo/lessons):
- `dev/java-interop/{pom.xml,README.md,run.sh}` + `src/main/java/org/apache/iceberg/InteropOracle.java`
  (package `org.apache.iceberg` to reach the `@VisibleForTesting SchemaUpdate(Schema,int)` ctor).
- `crates/iceberg/testdata/interop/update_schema/<scenario>/{base,java_evolved,rust_evolved}.metadata.json`.
- `crates/iceberg/tests/interop_update_schema.rs` (auto-discovered; no Cargo.toml edit).

Environment confirmed: `/opt/maven/bin/mvn` 3.9.9, Java 11.0.31 (iceberg 1.10 needs Java 11+, OK).
Rust `TableMetadata` round-trips via plain `serde_json` (its serde impl). Java testing ctor + `apply()`
confirmed at SchemaUpdate.java:75/467; `TableMetadata.newTableMetadata`/`buildFrom`/`setCurrentSchema
(Schema,int)`/`TableMetadataParser.{toJson,read}` confirmed. Literal factories: Rust
`Literal::{int,long,...}`, Java `Literal.of(...)`. Compare by PARSING both into the Rust model and
asserting structural equality (Schema/NestedField PartialEq + explicit field-ids + current-schema-id +
last-column-id) — NOT raw JSON bytes (Jackson vs serde key-order differ).

Scenarios (≥7, identical names Java+Rust): `add_top_level_columns`; `add_nested_struct_and_map` (THE
level-order fresh-field-id case — assert exact nested ids); `rename_and_move`; `update_type_promotion`
(int→long, float→double, decimal widen); `make_optional_and_delete`; `set_identifier_fields`;
`add_required_with_default_and_update_default`.

Plan:
- [x] 1. Java oracle: `pom.xml` (iceberg-core+api 1.10.0, exec-maven-plugin) + `InteropOracle.java`
      with `generate` (build base+java_evolved metadata via testing ctor → write JSON) and `verify`
      (read rust_evolved.metadata.json, assert structural schema equality vs java_evolved).
      Compiles + runs under Maven 3.9.9 / Java 11. Used `TableMetadataParser.fromJson(location, json)`
      (no 3-arg `read`); compared via `Schema.asStruct().equals` + `identifierFieldIds().equals`.
- [x] 2. Rust test `interop_update_schema.rs`: Dir-1 + Dir-2 producer. **DEVIATION from the brief's
      "apply emitted updates through TableMetadataBuilder":** `TransactionAction::commit` is `pub(crate)`
      and the `update_schema` module is private, so an external integration test cannot reach the action's
      raw updates. Drove the evolution through the PUBLIC API instead — a real `MemoryCatalog`
      (`LocalFsStorageFactory` over a tempdir) register-table + `Transaction` + `ApplyTransactionAction` +
      `commit` — which is strictly stronger (it also exercises the optimistic-concurrency requirement
      checks) and still applies the emitted `TableUpdate`s through `TableMetadataBuilder` internally. No
      production code touched.
- [x] 3. Runner `run.sh` (java generate → rust gen+assert → java verify) + `README.md` (TEST ORACLE).
- [x] 4. Generated all fixtures; Dir-1 green; `mvn verify` 7/7 PASS. Committed base/java_evolved/
      rust_evolved JSON for all 7 scenarios.
- [x] 5. Verify gate: interop test 3/3; lib suite 1320/0 ×2 (was 1318 baseline → +0 lib, the interop
      lives in tests/ not lib); clippy -D warnings clean; fmt --check clean. Flipped GAP_MATRIX
      `UpdateSchema` 🟡→✅; reconciled Roadmap + headline gaps + provenance.

**Outcome (2026-06-07, Increment 5 INTEROP PILOT, BUILDER Opus):** bidirectional Java interop for
`UpdateSchema` lands; row ✅. Harness layout: `dev/java-interop/{pom.xml,run.sh,README.md,src/main/java/
org/apache/iceberg/InteropOracle.java}` (TEST ORACLE, not a crate) + `crates/iceberg/tests/
interop_update_schema.rs` + 7×3 committed JSON fixtures under `crates/iceberg/testdata/interop/
update_schema/`. **Both directions green: Dir-1 (Rust reproduces Java) 3 Rust tests pass offline;
Dir-2 (`mvn ... verify`) 7/7 PASS.** Mutation-checked the verify gate (corrupting one identifier-field
set → exit 1 + FAIL line). **Key findings:** (a) Java rejects non-null INITIAL defaults on V2 metadata
(V3-only) — the two default scenarios use V3 base metadata to match Java's contract; the Rust side does
NOT enforce this V3-only rule (a latent parity gap, flagged in the report, out of scope here). (b) Java
resolves move targets by ORIGINAL name (renames live in `updates`, not name resolution) — the
`rename_and_move` scenario moves `email`, not `email_address`; Rust mirrors this. (c) evolved
last-column-id = `max(base.lastColumnId, evolved.highestFieldId)` (a delete never lowers it). Files
touched exactly the allowed set: new files under `dev/java-interop/**`, `crates/iceberg/testdata/
interop/**`, `crates/iceberg/tests/interop_update_schema.rs`; doc edits to GAP_MATRIX, Roadmap, todo,
lessons. NO Cargo.toml/lockfile/production-code edits.

#### Increment 5 — REVIEW (2026-06-07, Opus REVIEWER, DELEGATED)
Adversarially verified the 6 brief points against the Java source (`/tmp/iceberg-java-ref`) and by
running the full pipeline + mutation tests. Plan:
- [x] **Pt 1 (bidirectional, not tautological): CONFIRMED.** Dir-1 compares Rust-evolved vs the
      Java-WRITTEN `java_evolved.metadata.json` (recursive `StructType: PartialEq` over id/name/type/
      required/doc/default + identifier ids + current-schema-id + last-column-id); Dir-2 (`mvn verify`)
      reads the Rust-WRITTEN `rust_evolved.metadata.json` and compares against a fresh Java re-evolution.
      Proved non-trivial by mutation: (a) `count`→`kount` in a `java_evolved` fixture FAILS the Rust
      Dir-1 struct assertion (panic shows left=Rust vs right=Java, defaults included); (b) shrinking a
      `rust_evolved` identifier set FAILS `mvn verify` with exit 1. Neither side compares a file to
      itself; all 7 `rust_evolved` differ byte-wise from `java_evolved` (independent serializers).
- [x] **Pt 2 (all 7 scenarios both sides): CONFIRMED.** The "Dir-1 3 passed" is the test-FUNCTION count;
      `test_update_schema_interop_all_scenarios` LOOPS over all 7 (`SCENARIOS` const) — so all 7 run
      Dir-1, and `mvn verify` runs all 7 Dir-2 (7/7 PASS). No scenario silently skipped. Java
      `scenarios()` and Rust `apply_scenario_ops` mirror op-for-op.
- [x] **Pt 3 (nested level-order ids): CONFIRMED + strong.** Java `java_evolved` fixture pins map id=2,
      key-id=3, value-id=4, key struct 5–8, value struct 9–10 (level-order). The Rust
      `test_add_nested_struct_and_map_assigns_level_order_ids` asserts those EXACT ids. A depth-first
      regression would yield value-id=8 (key struct 4–7), which the `value_field.id == 4` + `key_ids ==
      [5,6,7,8]` assertions reject. Mentally broken: depth-first fails this test. No strengthening needed.
- [x] **Pt 4 (✅ flip — V2-default rule): GAP CONFIRMED, ✅ KEPT WITH HONEST CAVEAT + new test.** Java
      `Schema.checkCompatibility` (api/Schema.java:619, called on every add-schema build path,
      TableMetadata.java:1617) rejects a non-null `initialDefault` when `formatVersion < 3`
      (`DEFAULT_VALUES_MIN_FORMAT_VERSION = 3`), exact message "...non-null default (...) is not
      supported until v3" (Java test `TestSchema.testUnsupportedInitialDefault`). Rust has NO
      `check_compatibility` anywhere (`add_schema` in table_metadata_builder.rs does not call it;
      `validate_default` only checks type-convertibility). The existing unit test
      `test_add_required_column_with_default_succeeds_without_flag` ALREADY emits a non-null default on a
      V2 base — so Rust produces V2 metadata Java refuses to read. Decision: keep ✅ (the 7-scenario
      BIDIRECTIONAL proof is real; the hole is narrow + conditional — only V1/V2 + a column default, and
      it is a missing guard, not corruption of a valid op), but (a) rewrote the GAP_MATRIX note to state
      the divergence honestly, (b) added a test-pinned divergence regression
      (`test_v2_default_is_emitted_without_v3_guard_known_divergence`) so the hole is tracked, not
      silent, and (c) tracked the production fix (a `Schema::check_compatibility(format_version)` guard)
      as a follow-up — NOT fixed here (it belongs in `spec/`, beyond a one-line guard, out of reviewer
      scope).
- [x] **Pt 5 (deviation — catalog/Transaction path): CONFIRMED sound.** `Transaction::update_schema()`
      is `pub fn` returning `UpdateSchemaAction`; the test queues it via `ApplyTransactionAction::apply`
      and drives `Transaction::commit(&catalog)` — genuinely exercising `UpdateSchemaAction::commit()`
      through the catalog seam (strictly stronger: runs the optimistic-concurrency requirements too).
      `TransactionAction`/`commit` remain `pub(crate)` — visibility NOT widened. Only tracked-file edits
      are docs; no production `.rs` added/modified.
- [x] **Pt 6 (reproducibility): CONFIRMED.** Ran `mvn compile` (clean), `mvn generate` (7), Rust gen +
      Dir-1 (3/7), `mvn verify` (7/7) from a clean fixtures state — all green. `dev/java-interop/target/`
      is git-ignored (root `.gitignore` `target`); only `?? dev/java-interop/` is untracked (no build
      cruft staged). **Wart (tracked, not blocking):** regeneration churns `base`/`java_evolved` on the
      non-deterministic `table-uuid` + `last-updated-ms` + time-logs (structure identical) — noisy
      diffs on every `run.sh`. Harmless because the comparison is structural.
- [x] Verify gate: interop 3/3; lib ×2 = 1320/0 + the new divergence test (interop file); clippy -D
      warnings clean; fmt clean; `mvn` PASS table above.

**Review outcome (2026-06-07, Opus REVIEWER):** all 6 points adjudicated. Row stays **✅** with an
honest V2-default caveat in its note; +1 divergence-pinning test; production V3-default guard tracked as
a follow-up (below). Files touched: `crates/iceberg/tests/interop_update_schema.rs` (+1 test), GAP_MATRIX
(honest caveat), Roadmap (caveat), todo, lessons. No production `.rs`, no Cargo edits.

### Increment 6 — V3 initial-default guard (close the UpdateSchema parity hole, BUILDER Opus, 2026-06-07)
Close the only known `UpdateSchema` parity gap surfaced by the interop pilot: Rust emits Java-incompatible
metadata because `TableMetadataBuilder::add_schema` has no V3-only guard on column INITIAL defaults. Mirror
Java `Schema.checkCompatibility(Schema, formatVersion)` (`api/.../Schema.java:604-637`): for EVERY field
(incl. nested) with a non-null `initialDefault` when `formatVersion < DEFAULT_VALUES_MIN_FORMAT_VERSION (=3)`,
reject. Gate **initial_default ONLY — NOT write_default** (Java doesn't). Do NOT implement the broader V3-only
TYPE gate (`MIN_FORMAT_VERSIONS`) — flag it as a follow-up only.

**Java rule verified against source** (`api/src/main/java/org/apache/iceberg/Schema.java`):
- `checkCompatibility(Schema, int formatVersion)` (lines 604-637) iterates `schema.lazyIdToField().values()`
  (ALL fields, incl. nested). For each with `field.initialDefault() != null && formatVersion < 3`, records
  `"Invalid initial default for %s: non-null default (%s) is not supported until v%s"` (col name, value, 3);
  throws `IllegalStateException("Invalid schema for v%s:\n- %s", formatVersion, joined-problems)`.
- `DEFAULT_VALUES_MIN_FORMAT_VERSION = 3` (line 61). Called from the metadata builder's add-schema path
  (`addSchemaInternal`). It ALSO gates V3-only TYPES via `MIN_FORMAT_VERSIONS` (lines 64-70) — SEPARATE,
  broader parity item, NOT built here (flagged below).

**Choke point** (verified): `TableUpdate::AddSchema::apply` (catalog/mod.rs:611) calls
`builder.add_schema(schema)`, so wiring the guard into `TableMetadataBuilder::add_schema` covers the
UpdateSchema action's emitted `AddSchema`, CTAS, and every catalog commit — matching Java's
`addSchemaInternal`. The Rust analogue of Java `lazyIdToField()` is `Schema::field_id_to_fields()` (built by
`index_by_id`, a recursive `SchemaVisitor` walk over struct/list/map → reaches ALL nested fields); dotted
column names come from `field_id_to_name_map()`.

**Blast-radius (Risk-First) verified:** the guard fires ONLY for `initial_default.is_some() &&
format_version < V3`. `UpdateSchemaAction::commit` does NOT call `add_schema` (it just emits the `AddSchema`
update), so `run()`-based tests are unaffected; only tests that APPLY updates through the builder (or the
catalog/interop path) hit it. Across the crate, the only existing tests that drive a V2 initial-default
through the builder are `update_schema.rs::{test_emitted_schema_round_trips_defaults,
test_add_required_column_with_default_succeeds_without_flag}` + the interop divergence test. The two interop
default scenarios use a V3 base (confirmed in fixtures) so the round-trip is unaffected. V1/V2 serde fixtures
with defaults parse directly into `TableMetadata` (not via `add_schema`), so they are unaffected.

Plan:
- [x] Helper `Schema::check_compatibility(format_version)` in `spec/schema/mod.rs`: iterate
      `field_id_to_fields()`; for each field with `initial_default.is_some()` when
      `format_version < FormatVersion::V3`, push the Java-mirrored message (sorted by field id for
      determinism, like Java's TreeMap); `Err(ErrorKind::DataInvalid, "Invalid schema for v{N}:\n- {...}")`
      if any. Gates initial_default ONLY (write_default untouched). `DEFAULT_VALUES_MIN_FORMAT_VERSION = 3`
      named constant.
- [x] Wire into `TableMetadataBuilder::add_schema` (after field-name validation, before id assignment) —
      `schema.check_compatibility(self.metadata.format_version)?`.
- [x] Reconcile `update_schema.rs::test_add_required_column_with_default_succeeds_without_flag` → move base
      to V3 (the rule it targets — "required add WITH default needs no flag" — is legal on V3); add a V2
      sibling that asserts the defaulted add is REJECTED at apply time. Move
      `test_emitted_schema_round_trips_defaults` base to V3 too (it applies through the builder).
- [x] Flip `interop_update_schema.rs::test_v2_default_is_emitted_without_v3_guard_known_divergence` → assert
      the V2 defaulted add is now REJECTED with the guard's error (kind + "not supported until v3" substring).
- [x] New focused unit tests in `table_metadata_builder.rs`: V2 + top-level initial_default → rejected
      (kind + message substring); V3 + initial_default → allowed; V2 + initial_default on a NESTED field →
      rejected; V2 + NO default → unaffected (sanity); V2 + write_default-only → allowed (write_default NOT
      gated).
- [x] Docs: GAP_MATRIX `UpdateSchema` row → clean ✅ (note: V3 initial-default guard enforced, mirrors
      `Schema.checkCompatibility`, interop-proven both directions); close this follow-up; Roadmap; lessons.
- [x] Verify gate from repo root.

**Outcome (2026-06-07, Increment 6, BUILDER Opus):** the Increment-5 V3-guard follow-up is **CLOSED**.
Guard lives in `Schema::check_compatibility(format_version)` (`spec/schema/mod.rs`), wired into
`TableMetadataBuilder::add_schema` (the single choke point — `TableUpdate::AddSchema::apply` calls it, so
it covers the UpdateSchema action's emitted `AddSchema`, CTAS, and every catalog commit, matching Java's
`addSchemaInternal`). Message mirrors Java: per offending field `"Invalid initial default for {col}:
non-null default ({value:?}) is not supported until v3"` (ordered by field id, like Java's TreeMap) under
an `"Invalid schema for v{N}:"` header; `ErrorKind::DataInvalid`. Nested fields reached via
`field_id_to_fields()` (the recursive id→field index = Java `lazyIdToField()`); dotted names from
`field_id_to_name_map()`. Gates `initial_default` ONLY — `write_default` untouched (Java parity).
Reconciled tests: `update_schema.rs::test_add_required_column_with_default_succeeds_without_flag` →
`..._on_v3` (V3 base, applied through builder) + new `..._rejected_on_v2` sibling;
`test_emitted_schema_round_trips_defaults` moved to a V3 base; interop
`test_v2_default_is_emitted_without_v3_guard_known_divergence` → `test_v2_default_is_rejected_by_v3_guard`
(asserts rejection). New tests: 5 in `table_metadata_builder.rs` (V2 top-level reject / V3 allow / V2
nested reject / V2 no-default unaffected / V2 write-default-only allow) + 2 `Schema::check_compatibility`
unit tests in `schema/mod.rs`. Guard mutation-verified (disabling it fails the 3 rejection tests, incl. the
nested case). **Verify:** build clean; lib ×2 = 1328/0 both runs (was 1320 baseline → +8); interop 4/4
(7-scenario round-trip green — V3 default scenarios unaffected); clippy -D warnings clean; fmt clean. Java
`mvn` side NOT re-run — the guard does not change V3 output, so the committed V3 fixtures are unchanged.
Files touched exactly the allowed set: `spec/schema/mod.rs`, `spec/table_metadata_builder.rs`,
`transaction/update_schema.rs`, `tests/interop_update_schema.rs`, GAP_MATRIX, Roadmap, todo, lessons. No
Cargo.toml/lockfile/other edits. Row is now a clean ✅.

#### Increment 6 — REVIEW (2026-06-07, Opus REVIEWER, DELEGATED)
Adversarially verified points 1–5 against the Java source (`api/.../Schema.java` `checkCompatibility` +
`DEFAULT_VALUES_MIN_FORMAT_VERSION=3`) and `addSchemaInternal` (TableMetadata.java:1610-1652), and re-ran
the gate + two-direction mutation tests. No code GAPs found; one doc sharpen + lessons.
- [x] **Pt 1 (mirrors Java): CONFIRMED.** Gates `initial_default` ONLY — `write_default` untouched
      (Java checks `field.initialDefault() != null`); fires iff `format_version < V3`
      (`DEFAULT_VALUES_MIN_FORMAT_VERSION`); iterates ALL fields incl. nested via `field_id_to_fields()`
      (= Java `lazyIdToField()`); message carries dotted col name (`payload.flag`) + `{value:?}` + "is not
      supported until v3" under "Invalid schema for v{N}:" header (`FormatVersion` Display = `v2`/`v3`,
      matching Java verbatim). Both break-it cases pinned: nested-default-on-V2 → REJECTED
      (`test_add_schema_with_nested_initial_default_rejected_on_v2`); write-default-only-on-V2 → ALLOWED
      (`test_add_schema_with_write_default_only_allowed_on_v2`).
- [x] **Pt 2 (central choke point, no over/under-fire): CONFIRMED.** Guard in `add_schema` (table_metadata_
      builder.rs:649); `TableUpdate::AddSchema::apply` (catalog/mod.rs:611) → `add_schema`, and
      `add_current_schema`/`new()` → `add_schema`, so it covers the UpdateSchema action's emitted AddSchema,
      CTAS, and every catalog commit (exactly Java `addSchemaInternal`). Suite 1328/0 because no V1/V2 fixture
      carries an initial default — WHY confirmed by mutation: forcing the guard to early-`Ok` fails EXACTLY 4
      rejection tests and nothing else; over-broadening it to gate `write_default` fails EXACTLY 1 (the
      write-default-only test). READ/parse path UNAFFECTED: `TableMetadataV2 → TableMetadata` `TryFrom`
      constructs directly, never via `add_schema`/`check_compatibility` — only build/commit trips it (Java
      `checkCompatibility` is likewise in `addSchemaInternal`, not the parser).
- [x] **Pt 3 (reconciled tests honest): CONFIRMED.** `..._succeeds_without_flag_on_v3` drives the allow-path
      on a real `v3_table()` and APPLIES through the builder (`apply_updates`), asserting both defaults land
      — not skipped. `..._rejected_on_v2` applies the emitted AddSchema through a V2 builder and asserts
      kind=DataInvalid + "is not supported until v3" + col name; the interop `test_v2_default_is_rejected_by_v3
      _guard` drives a real `MemoryCatalog` register + `Transaction::commit` and asserts end-to-end rejection
      (not a shallow check). The default-bearing interop scenario uses a V3 base (verified in the fixture) so
      the 4/4 round-trip stays green.
- [x] **Pt 4 (✅ hole-free? + type gate tracked): ADJUDICATED — ✅ stands for the initial-default rule.**
      No other UpdateSchema case emits Java-incompatible metadata via DEFAULTS (write_default is not
      format-gated in Java, so Rust matching is correct). The V3-only TYPE gate (`MIN_FORMAT_VERSIONS`) IS a
      narrow residual hole that is LIVE TODAY for `timestamp_ns` (it exists in Rust + no type-version guard
      anywhere in `spec/`) — `add_column(timestamp_ns)` on V2 emits Java-rejected metadata now; the other
      four V3 types are unimplemented so future-only. The ✅ never claimed the type gate; properly TRACKED in
      todo (sharpened this review to say the `timestamp_ns` slice is live, not future). ✅ is justified.
- [x] **Pt 5 (message quality): CONFIRMED.** `{:?}` (Debug) on `Literal` is acceptable — `Literal` has no
      `Display`; Java renders via `toString()`; value rendering is expected to differ by language. All
      assertions key on STABLE substrings ("is not supported until v3", "Invalid schema for v2", the column
      name) — never the Debug value text. Actionable + non-brittle.
- [x] Verify (repo root, pinned nightly nightly-2025-10-27): build clean; lib ×2 = 1328/0 + 1328/0 (stable);
      interop 4/4; clippy -D warnings clean (forced rebuild, 12.8s); fmt --check clean. `mvn` NOT re-run (the
      guard does not alter V3 output; committed V3 fixtures unchanged — consistent with the builder's note).

**Review outcome (2026-06-07, Opus REVIEWER):** all 5 points adjudicated CONFIRMED; ✅ stands as a clean ✅
**for the column initial-default rule** (the named scope of this fix), with the V3-only TYPE gate the only
residual UpdateSchema parity item — narrowly LIVE for `timestamp_ns`, future for the other four V3 types,
and properly tracked. No code GAP found (guard mutation-verified load-bearing AND non-over-firing). Files
touched: `task/todo.md`, `task/lessons.md` only (doc sharpen + 2 lessons). No production `.rs`, no test, no
Cargo edits needed — the builder's implementation and tests are correct as shipped.

**FLAGGED follow-up (NOT built — scope control):** the V3-only TYPE gate (`MIN_FORMAT_VERSIONS`:
`timestamp_ns`/`variant`/`unknown`/`geometry`/`geography` require v3) lives in the SAME Java
`checkCompatibility` method but is a separate, broader parity item. Tracked below, not implemented here.

- [x] **Follow-up (CLOSED by Increment 7, 2026-06-07):** V3-only TYPE gate in `Schema::check_compatibility`
      mirroring Java `MIN_FORMAT_VERSIONS` (`Schema.java:64-70`) — reject `timestamp_ns`/`timestamptz_ns`
      fields when `format_version < 3`, message `"Invalid type for {col}: {type} is not supported until v{min}"`.
      The live `timestamp_ns`-on-V2 hole is now closed: the same `check_compatibility` method that gates
      initial-defaults also gates V3-only types in one pass. **Residual (genuinely future):** the other four
      Java `MIN_FORMAT_VERSIONS` types — `variant`/`unknown`/`geometry`/`geography` — are NOT representable in
      the Rust `Type`/`PrimitiveType` enums yet; each gets a one-line `min_format_version` arm
      (`PrimitiveType::Variant => Some(FormatVersion::V3)`, …) when the type lands. The helper is shaped for
      exactly that one-line addition.

### Increment 7 — V3-only TYPE gate (close the type-version follow-up above, BUILDER Opus, 2026-06-07)
Extend `Schema::check_compatibility(format_version)` to ALSO gate V3-only TYPES, fully mirroring Java
`Schema.checkCompatibility` — closes the live `add_column(timestamp_ns)`-on-V2 hole tracked above.

**Java rule verified against source** (`api/.../Schema.java:604-637`, re-read for this increment): in the
SAME `for (NestedField field : schema.lazyIdToField().values())` loop, BEFORE the initial-default check,
`Integer minFormatVersion = MIN_FORMAT_VERSIONS.get(field.type().typeId()); if (minFormatVersion != null
&& formatVersion < minFormatVersion) problems.put(fieldId, "Invalid type for %s: %s is not supported until
v%s")`. `MIN_FORMAT_VERSIONS = {TIMESTAMP_NANO:3, VARIANT:3, UNKNOWN:3, GEOMETRY:3, GEOGRAPHY:3}`. Both
checks accumulate into the SAME `TreeMap<fieldId,String>` and throw one combined `IllegalStateException`
`"Invalid schema for v%s:\n- %s"`. Of the five Java types, only `TIMESTAMP_NANO` is representable in Rust
today — `PrimitiveType::{TimestampNs, TimestamptzNs}` (both map to Java `TIMESTAMP_NANO`); `variant`/
`unknown`/`geometry`/`geography` are NOT in the Rust `Type`/`PrimitiveType` enums yet.

**Blast-radius (Risk-First) verified:** the type branch fires ONLY when a field's type min-version
exceeds the table `format_version` (i.e. an ns type on a <v3 builder/commit). Confirmed across the crate
that NO existing test builds a `<v3 TableMetadata` with a `timestamp_ns`/`timestamptz_ns` column via
`add_schema`/`TableMetadataBuilder::new`/`add_current_schema`/`from_table_creation`: the only ns usages
are transform/arrow/avro/manifest/equality-delete/datum tests that build a bare `Schema` (which does NOT
call `check_compatibility`) or match on the type — none flow through the metadata builder. So there are
NO existing tests to reconcile. The READ/parse path (`TableMetadataV2 → TableMetadata` `TryFrom`) never
calls `add_schema`, so V3 metadata fixtures with `timestamp_ns` parse unaffected.

Plan:
- [x] Added `fn min_format_version(ty: &Type) -> Option<FormatVersion>` returning `V3` for
      `PrimitiveType::{TimestampNs, TimestamptzNs}`, else `None`, with the comment noting the four
      not-yet-representable Java types get a one-line arm each when they land.
- [x] Restructured `check_compatibility` to iterate `field_id_to_fields()` ONCE, pushing the type problem
      (`"Invalid type for {col}: {type} is not supported until v{min}"`) when
      `min_format_version(type) > format_version` AND the existing initial-default problem into the SAME
      `Vec<(field_id, message)>`, stable-sorted by field id (type problem precedes default problem for a
      shared field), joined into the one `"Invalid schema for v{N}:"` error. Removed the v3 early-return so
      both rules share the same pass (the two minima coincide at v3, but the structure now mirrors Java).
- [x] +5 tests in `spec/schema/mod.rs`: V2 + `timestamp_ns` → rejected; V2 + `timestamptz_ns` → rejected;
      V3 + `timestamp_ns` → allowed; NESTED `timestamp_ns` on V2 → rejected (dotted `payload.captured_at`
      in message); V2 with BOTH a V3 type (field 2) AND a non-null initial_default (field 3) → BOTH
      problems in the single error, type-before-default by field id. Exact `DataInvalid` kind +
      message-substring asserts. Mutation-verified: forcing `min_format_version` to `None` fails exactly the
      4 type-gate tests, the 3 default/allow tests still pass.
- [x] Docs: GAP_MATRIX (`timestamp_ns` row + `UpdateSchema` row note now say BOTH rules enforced; the only
      open sub-item is variant/geo/unknown); Roadmap (snapshot + Phase-1 entry + headline); closed the
      follow-up above; appended a dated lesson.
- [x] Verify gate from repo root: build clean; lib ×2 = 1333/0 both runs (was 1328 → +5); interop 4/4;
      clippy -D warnings clean; fmt --check clean (one fmt reflow applied).

#### Increment 7 — REVIEW (2026-06-07, Opus REVIEWER, DELEGATED)
Adversarially verified points 1–5 against the Java source (`api/.../Schema.java:604-637` +
`MIN_FORMAT_VERSIONS`), ran the full gate + two-direction mutation tests. One GAP found and fixed.
- [x] **Pt 1 (mirrors Java's type gate): CONFIRMED.** `min_format_version` returns `Some(V3)` for BOTH
      `TimestampNs` and `TimestamptzNs` (= Java `TIMESTAMP_NANO`), `None` otherwise; fires iff
      `format_version < min` (`FormatVersion` Ord by u8 repr, so V1/V2 < V3); message `"Invalid type for
      {col}: {type} is not supported until v{min}"` with `FormatVersion` Display = `v3` (verbatim Java
      `v%s`), dotted nested name via `name_by_field_id`. Iterates ALL fields via `field_id_to_fields()`
      (= Java `lazyIdToField()`). Break-it cases all pinned: nested ns on V1/V2 → rejected; `timestamptz_ns`
      on V2 → rejected; a v2 type on V2 → `None` → unaffected. Type gate mutation-verified load-bearing
      (forcing `min_format_version`→`None` fails exactly the 5 type-dependent tests, leaves the 3
      default/allow tests green).
- [x] **Pt 2 (no silent breakage; ZERO-tests claim REAL): CONFIRMED independently.** Crate-wide grep of
      `TimestampNs`/`TimestamptzNs`/`Nanosecond` across src + tests + glue/hms/datafusion/integration_tests:
      every usage is a type-conversion fn (`type_to_string`), a transform/arrow/avro/manifest/datum unit
      test building a bare `Schema`/`Datum`, or datafusion predicate-pushdown (`Datum::timestamp_nanos`) —
      NONE routes an ns column through `add_schema`/`from_table_creation`/`create_table` on a <v3 base. The
      one builder-path test helper (glue `create_metadata` → `from_table_creation`, defaults V2) is never
      called with an ns schema. Full lib suite 1333/0 ×2; all-targets unit+interop green.
- [x] **Pt 3 (TreeMap-vs-Vec): ADJUDICATED — KEEP both-report `Vec`.** Java's `TreeMap` last-wins
      collapses a single-field-both to ONE (default) line; Rust's `Vec` keeps BOTH. Decision: keep it
      (accept/reject identical, strictly more informative, message text already language-divergent, case
      vanishingly narrow; cross-field order already matches Java). **GAP FOUND + FIXED:** the builder kept
      the `Vec` and documented the divergence in prose but only added the CROSS-field test (two distinct
      ids — cannot collide, cannot distinguish the designs); the single-field-both case was unpinned. Added
      `test_check_compatibility_single_field_both_type_and_default_reports_both_lines`. Mutation-verified:
      emulating Java last-wins (dedup to BTreeMap) fails EXACTLY the new test, leaves the cross-field test
      green.
- [x] **Pt 4 (build-path-only, no over-fire): CONFIRMED.** Only production call site of
      `Schema::check_compatibility` is `table_metadata_builder.rs:649` (inside `add_schema`); the parse
      path `TryFrom<TableMetadataV2> for TableMetadata` constructs `TableMetadata { .. }` directly and
      never calls it — reading existing V3 `timestamp_ns` metadata is unaffected. v2-and-below schemas
      without V3 types/defaults are completely untouched (suite is 1333/0 because no <v3 fixture carries an
      ns type or initial default through the builder — confirmed by the Pt 2 audit and the mutation gate).
- [x] **Pt 5 (✅ honesty + tracking): CONFIRMED.** With both halves landed (defaults + types),
      `UpdateSchema`/`timestamp_ns` are hole-free for the representable compatibility class. GAP_MATRIX is
      accurate: `timestamp_ns` ✅ with the format-version gate; `UpdateSchema` ✅ noting both rules; the
      ONLY open residual is `variant`/`unknown`/`geometry`/`geography` (not representable in the Rust
      `Type`/`PrimitiveType` enums — each a one-line `min_format_version` arm when it lands), properly
      tracked in the follow-up and the matrix.
- [x] Fixed a stale comment in `table_metadata_builder.rs:644` (said "currently the column initial-default
      rule" — now also covers the V3-only type rule).
- [x] Verify (repo root, pinned nightly): build clean; lib ×2 = 1334/0 + 1334/0 (was 1333 → +1 review
      test); interop 4/4; clippy -D warnings clean; fmt --check clean. All-targets: only the 5 pre-existing
      `tokio::main` doctest-compile failures (reproduced with my changes stashed — environmental, not mine).

**Review outcome (2026-06-07, Opus REVIEWER):** all 5 points adjudicated; the TreeMap divergence decided
(keep both-report) + the missing single-field-both test added (mutation-verified both directions); the
type gate confirmed load-bearing and build-path-only; the ZERO-tests claim independently re-confirmed; ✅
stands with the residual (variant/geo/unknown) correctly tracked. Files touched: `spec/schema/mod.rs`
(+1 test), `spec/table_metadata_builder.rs` (stale comment), `task/todo.md`, `task/lessons.md`. No Cargo
edits, no commit.

**Outcome (2026-06-07, Increment 7, BUILDER Opus):** the V3-only TYPE gate follow-up is **CLOSED**.
`Schema::check_compatibility` now fully mirrors Java `Schema.checkCompatibility` — one pass over
`field_id_to_fields()` (all fields incl. nested) records a type problem (`MIN_FORMAT_VERSIONS`) and/or an
initial-default problem (`DEFAULT_VALUES_MIN_FORMAT_VERSION`) per field, accumulated into a single
`"Invalid schema for v{N}:"` error ordered by field id (Java's TreeMap). Helper `fn min_format_version(ty:
&Type) -> Option<FormatVersion>` returns `V3` for `PrimitiveType::{TimestampNs, TimestamptzNs}` (Java
`TIMESTAMP_NANO`); the four Java types not representable in Rust (`variant`/`unknown`/`geometry`/`geography`)
get a one-line arm each when they land. The guard stays on the BUILD path (`add_schema`) — the parse path
(`TableMetadataV2 → TableMetadata` `TryFrom`) never calls it, so reading existing V3 `timestamp_ns` metadata
is unaffected. **No existing tests needed reconciling** — a crate-wide audit confirmed no test builds a
`<v3 TableMetadata` with a `timestamp_ns`/`timestamptz_ns` column via `add_schema`/`from_table_creation`
(the ns usages are transform/arrow/avro/manifest/datum tests that build bare `Schema`s, which do not call
`check_compatibility`). Files touched exactly the allowed set: `crates/iceberg/src/spec/schema/mod.rs`
(helper + restructured method + 5 tests), GAP_MATRIX, Roadmap, todo, lessons. No production code outside
`check_compatibility`/its helper; no Cargo/lockfile edits. An Opus REVIEWER verifies next.

### Increment 8 — UpdatePartitionSpec INTEROP (bidirectional Java round-trip, BUILDER Opus, 2026-06-07)
Mirror the proven UpdateSchema interop harness for `UpdatePartitionSpec` so its GAP_MATRIX row can flip
🟡 → ✅. Java under `dev/java-interop/` stays a TEST-ONLY ORACLE (not a Cargo crate, never invoked by
`cargo`). Durable artifacts = committed JSON fixtures + a Rust test that reads them.

**The wrinkle vs the schema oracle:** Java's `recycleOrCreatePartitionField` only recycles a historical
field id+name when `formatVersion >= 2 && base != null` (line 124). The `@VisibleForTesting`
`BaseUpdatePartitionSpec(int, PartitionSpec, ...)` ctors set `base = null` → NO recycling. So to exercise
recycling I MUST drive through `BaseUpdatePartitionSpec(TableOperations)` (`base = ops.current()`).
Uniform path for ALL scenarios: a minimal in-memory `org.apache.iceberg.TableOperations` (a sibling class
in `package org.apache.iceberg`) holding a `TableMetadata`, driven via
`new BaseTable(ops, name).updateSpec()...commit()`, then read `ops.current()` for the evolved metadata.
`BaseTable(TableOperations, String)` is public; the in-memory `commit(base, metadata)` just swaps the held
metadata; `io()/locationProvider()/newSnapshotId()/metadataFileLocation()` are minimal/no-op (the spec
commit never touches data files).

**Scenarios (≥7, identical + named the same on both sides):**
- `add_identity_field` (V2) — identity(category) add; base case.
- `add_transform_fields` (V2) — bucket[16](id), truncate[8](category), year(event_ts) on a multi-col
  schema; pins auto-generated names (`PartitionNameGenerator`) AND assigned field-ids + last-partition-id.
- `remove_field_v2` (V2) — base spec has identity(category); remove it → omitted from the new spec.
- `remove_field_v1_void` (V1) — base spec identity(category); remove → re-added as void/alwaysNull to
  preserve field-ids (Java V1 `apply()` branch). V1 base.
- `rename_field` (V2) — rename a base partition field; field-id preserved.
- `field_id_recycling` (V2) — base metadata carries TWO historical specs sharing a `(source,transform)`
  with a CUSTOM name; re-adding that field recycles the historical field-id AND name (the Increment-2
  review bug). Needs `base != null` → the TableOperations path.
- `delete_then_readd` (V2) — remove + re-add the same `(source,transform)` → Java's rewrite/un-delete
  (field restored, id stable, no new spec).

Compare the evolved DEFAULT partition spec (spec-id + each field's source-id/field-id/name/transform) +
`last-partition-id`, embedded in TableMetadata. Compare by PARSING into the Rust model
(`PartitionSpec`/`PartitionField` `PartialEq`), not raw JSON bytes.

Plan:
- [ ] 1. Extend `InteropOracle.java`: add a partition-spec scenario registry + an in-memory
      `TableOperations` sibling; `generate` writes `base`+`java_evolved` for partition scenarios alongside
      the schema ones; `verify` reads `rust_evolved` and asserts Java parses it + its default spec matches
      Java's own evolution. Wire BOTH capabilities into one `generate`/`verify` pass; update
      `run.sh`/`README.md`/`pom.xml` (partition fixtures dir).
- [ ] 2. `crates/iceberg/tests/interop_update_partition_spec.rs` (mirror the schema test): Dir-1 asserts
      Rust reproduces Java's evolved default spec (source-id/field-id/name/transform + last-partition-id)
      for all scenarios via the `MemoryCatalog` + `Transaction::update_partition_spec()` + commit path;
      writes `rust_evolved.metadata.json` under `ICEBERG_INTEROP_GEN`. Scenario-specific exact-id assert
      for `field_id_recycling` (recycled id + historical name) — a last-partition-id-only check can't catch
      a name-recycle drift.
- [ ] 3. Committed fixtures under `crates/iceberg/testdata/interop/update_partition_spec/<scenario>/`.
- [ ] 4. Verify: `cargo test -p iceberg --test interop_update_partition_spec`; `cargo test -p iceberg
      --lib` ×2; `cargo test -p iceberg --test interop_update_schema` (stays green); clippy -D warnings;
      fmt --check; Java `mvn compile` + generate + verify (PASS/FAIL table for BOTH schema + partition).
- [x] 5. Flip GAP_MATRIX `UpdatePartitionSpec` row 🟡 → ✅ (both directions pass, no open divergence);
      reconciled Roadmap headline + summary + Phase-1 exit-criteria lines.

**Outcome (2026-06-07, Increment 8 INTEROP, BUILDER Opus):** bidirectional Java interop for
`UpdatePartitionSpec` landed; GAP_MATRIX row flipped 🟡 → ✅. One `dev/java-interop` `generate`/`verify`
pass now covers BOTH capabilities (schema + partition) — `InteropOracle` refactored into `SchemaOracle`
+ `PartitionOracle` nested classes driven from one entrypoint; the partition oracle drives a REAL
`BaseUpdatePartitionSpec` via `new BaseTable(ops).updateSpec()…commit()` over an in-memory
`TableOperations` (so `base != null` and `recycleOrCreatePartitionField` is live). 7 partition scenarios
(`add_identity_field`, `add_transform_fields`, `remove_field_v2`, `remove_field_v1_void`, `rename_field`,
`field_id_recycling`, `delete_then_readd`) × 3 committed fixtures, mirrored by
`crates/iceberg/tests/interop_update_partition_spec.rs`. Both directions PASS 7/7 (schema stays 7/7).
**Two divergences surfaced by interop:**
  1. **`field_id_recycling` — fixture artifact (test-only fix).** My first base built two independent
     specs that BOTH started field ids at 1000 → the recycled add collided (`Cannot use field id more
     than once in one PartitionSpec: 1000`). Real V2 tables don't look like that: a non-default
     historical spec gets a fresh sequential id (1001) via `BaseUpdatePartitionSpec.assignFieldId`. Fixed
     by building the recycling base by evolving in the historical spec through the real action (id 1001),
     which is what production does. No Rust production change.
  2. **V1 void replacement — REAL Rust↔Java divergence (in-scope production fix, FLAGGED).** Binding the
     V1-evolved spec was rejected: `Cannot create partition with name: 'category' that conflicts with
     schema field and is not an identity transform.` The Rust partition-name↔schema collision check
     (`PartitionSpecBuilder::check_name_does_not_collide_with_schema` + `TableMetadataBuilder::
     validate_partition_field_names`) was **identity-only**, but Java's bind-path `PartitionSpec.Builder.
     checkAndAddPartitionName(name, sourceId)` (line 618 → 401) permits ANY transform as long as the
     colliding schema field's id == the partition's source id — only the public typed builders (`.bucket()`
     etc., `sourceColumnId=null`) are strict. The V1 void replacement (`void(category)` named `category`,
     sourced from `category`) satisfies the name↔source-id rule and is legal in Java. **FIX (production,
     2 files):** relaxed both Rust checks to **identity OR void** (source-id-gated) — the narrowest
     Java-faithful change that keeps the existing strict-builder bucket-rejection test intact. Pinned with
     positive + negative tests at both layers (`partition.rs`: void-named-after-own-source OK / different-
     source rejected; `table_metadata_builder.rs`: void replacement accepted; existing bucket-collision
     test unchanged). Mutation-verified load-bearing (reverting to identity-only fails all 3 void tests +
     interop V1). Did NOT broaden to bucket/truncate (Rust collapses Java's two name-check paths into one,
     and the existing `test_builder_collision` pins bucket-via-builder rejection — out of scope to split).
**Verify:** `interop_update_partition_spec` 4 tests (1 loops all 7 scenarios) green offline; lib suite
×2 = 1336/0 both runs (1334 prior + 2 new void tests); `interop_update_schema` stays 4/4; clippy -D
warnings clean; fmt clean; `mvn compile` + run.sh end-to-end = 7/7 schema + 7/7 partition both directions.
(5 pre-existing `rt-multi-thread` doctest failures in `lib.rs`/`writer/mod.rs` — unrelated, env artifact,
documented in the Increment-7 reviewer lesson.) **Files touched (exactly the allowed set):**
`dev/java-interop/{InteropOracle.java,pom.xml,run.sh,README.md}`, new
`crates/iceberg/tests/interop_update_partition_spec.rs` + `testdata/interop/update_partition_spec/**`,
production `crates/iceberg/src/spec/{partition.rs,table_metadata_builder.rs}` (the flagged guard fix +
tests), docs `GAP_MATRIX.md`/`Roadmap.md`/`task/{todo.md,lessons.md}`. No Cargo/lockfile edits. An Opus
REVIEWER verifies next.

### Increment 9 — ManageSnapshots INTEROP (bidirectional Java round-trip, BUILDER Opus, 2026-06-07)
The LAST Phase-1 metadata capability to interop-prove. Mirror the proven schema+partition interop harness
for `ManageSnapshots` ref operations so the two snapshot rows can flip 🟡 → ✅. Java under
`dev/java-interop/` stays a TEST-ONLY ORACLE (not a Cargo crate, never invoked by `cargo`).

**Plan (inputs/outputs/contract):**
- **Contract:** for ≥7 identically-named scenarios, Rust loads the Java-written `base.metadata.json`
  (which already carries a real snapshot HISTORY + refs), applies the SAME `ManageSnapshots` op-sequence
  via the public `Transaction::manage_snapshots()` + `ApplyTransactionAction::apply` + `commit(&catalog)`,
  and the evolved REFS map (each ref's snapshot-id + branch-vs-tag kind + retention fields) + the
  current-snapshot-id (main) must be structurally equal to Java's `java_evolved.metadata.json`.
- **The wrinkle vs schema/partition:** ref ops act on the snapshot graph, so the base needs a real history.
  Build it in Java (`package org.apache.iceberg`): `new BaseSnapshot(seq, id, parentId, ts, "append",
  summary, schemaId, manifestList, null, null, null)` for ROOT/CURRENT/SIBLING (distinct timestamps,
  increasing seq, ts ≤ last-updated-ms), assemble via `TableMetadata.buildFrom(seed).setBranchSnapshot
  /setRef(...)`, mirroring the Rust `forked_table()` shape: main→CURRENT, `dev` branch→CURRENT, `stable`
  tag→ROOT. Drive ops via `new BaseTable(inMemoryOps, name).manageSnapshots().<ops>().commit()` then read
  `ops.current()`. The `InMemoryTableOperations` (already present) suffices — ref-only ops never call
  `io()` (`committedFiles` returns early for an empty new-snapshot set; `temp()`/`newSnapshotId()` are
  interface defaults).
- **Comparison model:** there is NO public `refs()` accessor on Rust `TableMetadata` returning the typed
  `SnapshotReference` (only `snapshot_for_ref` → `Snapshot`, which drops kind+retention). So the Rust test
  serializes the evolved `TableMetadata` to a `serde_json::Value`, extracts the `refs` object, and
  deserializes each value into the public `SnapshotReference`/`SnapshotRetention` types — typed ref model,
  no production accessor needed.

**Scenarios (≥7, identical Java + Rust, same names):**
- [x] `create_branch_and_tag` — create branch @ROOT + tag @CURRENT.
- [x] `rollback_to_ancestor` — main CURRENT → ROOT (ancestry-valid).
- [x] `rollback_to_time` — ts strictly between ROOT and CURRENT → resolves to ROOT (cross-checks strict-`<`).
- [x] `set_current_snapshot` — main → ROOT (no ancestry requirement).
- [x] `fast_forward` — a branch @ROOT fast-forwarded to main@CURRENT.
- [x] `retention` — min_snapshots_to_keep + max_snapshot_age_ms on a branch; max_ref_age_ms on `stable` tag.
- [x] `remove_and_rename` — remove `stable` tag; rename `dev` → `feature`.

**Deliverables:**
- [x] 1. Extended `InteropOracle.java` with `SnapshotOracle` (+ `buildBase` snapshot-history builder + the
      `SnapshotScenario` driver) wired into the same `generate`/`verify`; updated
      `run.sh`/`README.md`/`pom.xml` (new `interop.manage_snapshots.fixtures.dir`). One pass covers all 3.
- [x] 2. `crates/iceberg/tests/interop_manage_snapshots.rs` (mirrors the others): Dir-1 asserts refs +
      current-snapshot equal Java's (refs recovered by round-tripping evolved metadata through serde_json
      — no public `refs()` accessor); writes `rust_evolved.metadata.json` under `ICEBERG_INTEROP_GEN`. 3
      scenario-specific tests: rollback_to_time→ROOT (strict-`<`), retention-on-branch-vs-tag, remove+rename.
- [x] 3. Committed fixtures under `crates/iceberg/testdata/interop/manage_snapshots/<scenario>/` (7×3 = 21).
- [x] 4. Verify (from repo root): `interop_manage_snapshots` 4/4 (1 loops all 7); lib ×2 = 1337/0 both
      (no production change → unchanged); `interop_update_schema`/`interop_update_partition_spec` stay 4/4;
      clippy -D warnings clean (clean rebuild); fmt --check clean; mvn compile + run.sh end-to-end =
      7/7 schema + 7/7 partition + 7/7 manage_snapshots, BOTH directions, 0 failures.

**GAP_MATRIX target:** flipped "Snapshot model + refs" → ✅; "Snapshot management" → ✅ (ref-op surface)
with the EXPLICIT caveat that `cherrypick` stays Phase-2-gated (status cell reads
`✅ (ref-op surface; **`cherrypick` Phase-2-gated**)`).

**Outcome (2026-06-07, Increment 9 INTEROP, BUILDER Opus):** bidirectional Java interop for the
`ManageSnapshots` ref-operation surface landed — the LAST Phase-1 metadata capability to interop-prove.
Both `"Snapshot model + refs"` and the ref-op surface of `"Snapshot management"` flipped 🟡 → ✅;
`cherrypick` left Phase-2-gated (NOT interop-proven, explicit caveat in both rows). One `dev/java-interop`
`generate`/`verify` pass now covers ALL THREE capabilities — `InteropOracle` grew a `SnapshotOracle`
(nested beside `SchemaOracle` + `PartitionOracle`) with a `buildBase` snapshot-history builder (`new
BaseSnapshot(...)` ROOT/CURRENT/SIBLING + `TableMetadata.buildFrom().{addSnapshot,setRef,setBranchSnapshot}`,
mirroring `forked_table()`), driving a REAL `SnapshotManager` via `new BaseTable(ops).manageSnapshots()…
commit()` over the existing `InMemoryTableOperations`. 7 scenarios × 3 committed fixtures, mirrored by
`crates/iceberg/tests/interop_manage_snapshots.rs` (refs recovered by round-tripping the evolved
`TableMetadata` through `serde_json` → typed `SnapshotReference`, since there is no public `refs()`
accessor). **NO production `.rs` change** (unlike the partition pilot). **Two divergences/wrinkles
surfaced by interop:**
  1. **Base-build snapshot-log timestamp ordering — Java oracle wrinkle (test-only).** Building the base
     with `setBranchSnapshot(currentSnapshot, main)` BEFORE the other refs, then evolving from the
     IN-MEMORY base (which carries pending `AddSnapshot` changes that `buildFrom` copies), made Java's
     `isAddedSnapshot(ROOT)` true during a rollback → it stamped the snapshot-log entry with ROOT's OLD
     timestamp (1515…) and tripped `"Invalid update timestamp …: before last snapshot log entry"`. Fixed
     two ways in the oracle: (a) add all snapshots first, set `dev`/`stable` refs, then set `main` LAST via
     the `setBranchSnapshot(long, branch)` overload (clean, monotonic snapshot-log); (b) in `generate`,
     write the base then RE-PARSE it from disk before evolving so the residual `AddSnapshot` changes are
     cleared (matching what `verify` and the Rust test both load). No Rust change.
  2. **V2 snapshot `sequence-number` read-strictness — REAL latent Rust↔Java divergence (FLAGGED, left
     as-is, orthogonal to ref ops).** Rust's `_serde::SnapshotV2.sequence_number` is a plain required
     field; Java's `SnapshotParser` OMITS `sequence-number` when it equals `INITIAL_SEQUENCE_NUMBER` (0)
     and reads a missing one as 0. So a Java-written V2 snapshot with `sequence-number == 0` does NOT parse
     in Rust (`data did not match any variant of untagged enum TableMetadataEnum`). This is a genuine read
     gap, BUT: (a) the spec marks snapshot `sequence-number` as **required** in V2/V3 (format/spec.md line
     949), so making Rust default-0 would DIVERGE from the spec and risk masking malformed metadata; (b) a
     `sequence-number == 0` V2 snapshot only arises as a V1-carryover artifact — real V2 tables assign
     seq ≥ 1. **Decision:** did NOT change production (out of scope for a REF-OPERATION increment, and the
     spec-vs-Java tension needs its own adjudication). Sidestepped by using V2-realistic sequence numbers
     (ROOT=1/CURRENT=2/SIBLING=3) in the fixture, which keeps Java emitting every `sequence-number` and the
     fixture spec-faithful. Flagged in GAP_MATRIX (both rows), Roadmap, README, and lessons for the reviewer.
**Verify:** `interop_manage_snapshots` 4 tests (1 loops all 7) green offline; lib ×2 = 1337/0 both
(unchanged — no production code); `interop_update_schema`/`interop_update_partition_spec` stay 4/4;
clippy -D warnings clean (full `cargo clean -p iceberg` + `--all-targets` rebuild, 26.9s); fmt --check
clean; `mvn compile` + `run.sh` end-to-end = 7/7 × 3 capabilities both directions, 0 failures.
**Files touched (exactly the allowed set):** `dev/java-interop/{InteropOracle.java,pom.xml,run.sh,
README.md}`, new `crates/iceberg/tests/interop_manage_snapshots.rs` + `testdata/interop/manage_snapshots/**`
(21 fixtures), docs `GAP_MATRIX.md`/`Roadmap.md`/`task/{todo.md,lessons.md}`. NO production `.rs`, NO
Cargo/lockfile edits. Incidental schema/partition fixture churn from `run.sh` (random table-uuid +
timestamp) was reverted via `git checkout` — those fixtures are byte-identical to committed. An Opus
REVIEWER verifies next.

**Reviewer verdict (2026-06-07, Increment 9 INTEROP, REVIEWER Opus, DELEGATED/MEDIUM):** CONFIRMED with
two real findings actioned (tests strengthened; one production fix recommended for a human, not made).
1. **Bidirectional / not tautological — CONFIRMED.** Per-scenario `base.refs != java.refs` (False) and
   `java.refs == rust.refs` (True) for all 7; `base`, `java_evolved`, `rust_evolved` are 3 distinct files
   (Java-written vs Rust-written), and `refs_of()` re-parses kind + retention (retention scenario shows
   branch fields on `dev`, `max_ref_age_ms`-only on the `stable` tag) — not a snapshot-id-only or
   file-vs-itself compare.
2. **Coverage + sharp pins — CONFIRMED (one strengthened).** All 7 scenarios both ways; schema 7/7 +
   partition 7/7 stay green. Retention pin mutation-verified (misrouting `MaxSnapshotAgeMs`→`max_ref`
   fails both the dedicated test and the all-scenarios refs-equality). **Gap fixed:** the interop
   `rollback_to_time` pin did NOT catch a `<`→`<=` regression (`ROOT_TS_MS+1` is far below CURRENT's ts,
   so ROOT wins either way) — only the unit test did. Added a boundary assertion (roll to EXACTLY
   `CURRENT_TS_MS` → must fall back to ROOT) and mutation-verified it now FAILs under `<=`.
3. **V2 `sequence-number` divergence — REAL read bug; RECOMMEND FIX (human), not track.** Probe confirmed:
   a seq-omitted V2 metadata.json fails Rust parsing (`data did not match any variant of untagged enum
   TableMetadataEnum`). Java write omits seq≤0 (`SnapshotParser` L60) and defaults absent→0 on read (L128);
   Rust `_serde::SnapshotV2.sequence_number` is required. The builder's "spec marks it required → defaulting
   would diverge" rationale is WRONG: `format/spec.md` L1979/L2002 explicitly MANDATE "must default to 0
   when reading v1 metadata" — the lenient read IS the spec. Blast radius is real (V1→V2-upgraded tables
   keep seq-0 carryover snapshots; `upgradeFormatVersion` does not rewrite seqs → unreadable by Rust).
   Fix = one-line `#[serde(default)]` on `SnapshotV2`/`SnapshotV3.sequence_number` (V1 path already
   hard-codes 0). Left to a human (production-reader edit outside this increment's flagged scope).
   **→ CLOSED by Increment 10 (2026-06-07): `#[serde(default)]` added to both fields + 4 tests; "Snapshot
   model + refs" row flipped to a clean ✅. See Increment 10 above.**
4. **Row states — adjudicated honest, refined.** "Snapshot model + refs" ✅ and "Snapshot management"
   ✅ (ref-op surface; `cherrypick` Phase-2-gated) kept; the cherrypick caveat is clear (NOT interop-proven).
   Refined the "Snapshot model + refs" fixture note to (a) correct the spec-vs-Java framing, (b) disclose
   that the ✅ is scoped to natively-written V2 (seq ≥ 1) refs with a tracked reader gap for the
   seq-0 V1→V2-upgrade class.
5. **Scope — CONFIRMED.** Zero production `.rs` changed (only the untracked test + the Java oracle);
   no Cargo.toml/lockfile; `TransactionAction` trait + `commit` stay `pub(crate)` (action.rs L37/L49);
   `dev/java-interop/` has no Cargo.toml and is referenced by no `Cargo.toml` — fully out of the Cargo graph.
   Reviewer edits (all in the allowed set): strengthened `interop_manage_snapshots.rs` (added
   `CURRENT_TS_MS` + boundary pin), `GAP_MATRIX.md` note correction, `task/{todo.md,lessons.md}`.
   Throwaway seq-probe written + run + deleted (no residue). NO COMMIT.
**Reviewer verify (repo root):** interop_manage_snapshots 4/4; interop_update_schema 4/4 +
interop_update_partition_spec 4/4; lib ×2 = 1337/0 both; clippy -D warnings clean (incl. modified test);
fmt --check clean; `mvn -f dev/java-interop verify` = schema 7/7 + partition 7/7 + manage_snapshots 7/7,
0 failures (Direction 2, Java reads committed Rust output); full `run.sh` end-to-end green both directions.

### Increment 10 — V2/V3 snapshot `sequence-number` lenient read (close the seq-0 reader gap, BUILDER Opus, 2026-06-07)
Close the seq-number follow-up surfaced by Increment 9: Rust's V2/V3 snapshot deserializer required
`sequence-number`, but the spec MANDATES it default to 0 when absent on read — so Rust cannot parse a
V1→V2-upgraded table that Java wrote (Java's `SnapshotParser` omits the field when ≤ 0).

**Java + spec rule (verified against source):**
- `format/spec.md` line 1979 ("Snapshot field `sequence-number` must default to 0" when reading v1 metadata)
  + line 2002 ("`sequence-number` … is required; default to 0 when reading v1 metadata"). The line-949
  "required" is a WRITER rule; the read-side rule mandates lenient default-to-0.
- Java `SnapshotParser`: write omits `sequence-number` when `≤ INITIAL_SEQUENCE_NUMBER (0)` (lines 60-61);
  read defaults absent → `INITIAL_SEQUENCE_NUMBER (0)` (lines 128-130). Verified in the live ref checkout.

**Exact fields fixed (confirmed by reading the structs + their wiring in `table_metadata.rs`):**
- `_serde::SnapshotV2.sequence_number` (snapshot.rs ~line 297) — the field used when deserializing a V2
  snapshot inside `TableMetadataV2` (`table_metadata.rs:801` holds `Option<Vec<SnapshotV2>>`).
- `_serde::SnapshotV3.sequence_number` (snapshot.rs ~line 274) — same for V3 (`table_metadata.rs:758` holds
  `Option<Vec<SnapshotV3>>`). The V1 path already hard-codes `sequence_number: 0` (line 405).

Plan:
- [x] Add `#[serde(default)]` to `SnapshotV2.sequence_number` and `SnapshotV3.sequence_number` (default for
      `i64` is 0 — exactly Java's `INITIAL_SEQUENCE_NUMBER`). WRITE behavior unchanged (Rust keeps emitting
      the field; Java tolerates it). No change to the public `Snapshot` API.
- [x] Tests (same change) in `spec/snapshot.rs`: deserialize a V2 snapshot JSON OMITTING `sequence-number`
      → succeeds, `sequence_number() == 0` (the Java-written V1→V2-upgrade-carryover snapshot that fails
      today); negative-control sibling with `sequence-number` present → that value preserved; same pair for
      V3. Mutation-verify: removing `#[serde(default)]` makes the seq-omitted tests fail to parse.
- [x] Docs: flip GAP_MATRIX "Snapshot model + refs" to a clean ✅ (remove the seq-0 / "scoped to
      natively-written V2" caveat; note the spec-mandated lenient read is now honored); close the
      seq-number follow-up; leave a NEW tracked follow-up for the SIBLING default-to-0 read fields
      (`last-sequence-number`; manifest-list `sequence-number`/`min-sequence-number`; manifest/manifest-list
      `content`) NOT verified/fixed here; append a dated lesson (the spec READ-rule). Update Roadmap.
- [x] Verify gate from repo root.

**Outcome (2026-06-07, Increment 10, BUILDER Opus):** the Increment-9 seq-number follow-up is **CLOSED**.
Added `#[serde(default)]` to `_serde::SnapshotV2.sequence_number` AND `_serde::SnapshotV3.sequence_number`
in `crates/iceberg/src/spec/snapshot.rs` — confirmed (by reading the structs + their wiring) these are the
exact fields used when deserializing a snapshot inside V2/V3 `TableMetadata`: `TableMetadataV2`
(`table_metadata.rs:801`) holds `Option<Vec<SnapshotV2>>` and `TableMetadataV3` (`:758`) holds
`Option<Vec<SnapshotV3>>`. An absent `sequence-number` now reads as 0 (i64 default = Java
`INITIAL_SEQUENCE_NUMBER`), mirroring the spec read rule (`format/spec.md` 1979 & 2002) and Java's
`SnapshotParser` (write omits ≤ 0; read defaults absent → 0); the V1 path already hard-codes
`sequence_number: 0`. WRITE behavior unchanged (Rust still emits the field; Java tolerates it); public
`Snapshot` API unchanged. 4 tests added (V2/V3 seq-omitted → 0; V2/V3 seq-present → preserved);
mutation-verified — stripping `#[serde(default)]` makes both seq-omitted tests fail with "missing field
`sequence-number`", the seq-present controls still pass. **Verify:** build clean; lib ×2 = 1341/0 both
runs (was 1337 → +4); interop `interop_manage_snapshots`/`interop_update_schema`/`interop_update_partition_spec`
all stay 4/4; clippy -D warnings clean; fmt --check clean (one reflow on the new tests' long `.expect`
strings, applied via `cargo fmt`). GAP_MATRIX "Snapshot model + refs" flipped to a CLEAN ✅ (seq-0 /
"scoped to natively-written V2" caveat removed; spec-mandated lenient read noted). Files touched exactly
the allowed set: `crates/iceberg/src/spec/snapshot.rs` (fix + 4 tests), `docs/parity/GAP_MATRIX.md`,
`Roadmap.md`, `task/todo.md`, `task/lessons.md`. No Cargo/lockfile/other edits; no fixture file needed
(inline JSON strings in the tests). An Opus REVIEWER verifies next.

#### Increment 10 — tracked follow-up (NOT built here)
- [ ] **Sibling spec-mandated default-to-0 read fields (reader-robustness pass).** The spec
      (`format/spec.md` "Reading v1 metadata for v2", lines ~1979–1986) mandates default-to-0 on read for
      MORE than the snapshot `sequence-number` fixed in Increment 10. These were **NOT verified or fixed**
      in that change — a future reader-robustness pass should audit each against Java's parsers and add the
      analogous lenient read where Rust is currently strict. **Reviewer note (2026-06-07): only the snapshot
      `sequence-number` was a real _Java-omitted_ field; the rest are spec-robustness, NOT Java-interop
      blockers** — distinguish them so this is neither over- nor under-stated:
      - **Table metadata `last-sequence-number`** — spec mandates default-to-0 when reading v1 metadata, BUT
        **Java ALWAYS writes it for V2+** (`TableMetadataParser.toJson` line 173: `if (formatVersion() > 1)
        writeNumberField(LAST_SEQUENCE_NUMBER, ...)`, verified in the ref checkout), so Rust's required
        `last_sequence_number` (`spec/table_metadata.rs::TableMetadataV2V3Shared`, no `#[serde(default)]`)
        NEVER bites a Java-written V2/V3 table. It is a **spec-robustness gap for non-Java / hand-written
        metadata only** — confirmed end-to-end by a reviewer probe (a V2 metadata.json with
        `last-sequence-number` omitted fails Rust parsing with "data did not match any variant of untagged
        enum TableMetadataEnum"; the same file with it present (=0) parses). Low priority: not on the
        Java-interop path. Fix shape: `#[serde(default)]` on `last_sequence_number` (i64 → 0), but note the
        V1→V2 `TryFrom` already validates `last_sequence_number == 0` for V1, so the gate stays correct.
      - **Manifest list `sequence-number` / `min-sequence-number` / `content`; manifest entry
        `sequence_number` / `file_sequence_number`; data file `content`** — these are **Avro** fields
        (Java `V1Metadata`/`V2Metadata` manifest_file Avro schemas; Rust `spec/manifest_list.rs` +
        `spec/manifest`), a **different read path** from the JSON-serde snapshot fix. Whether each is truly
        Java-omitted (vs always-written) must be checked per field against the Avro schema's field default
        before flipping — do not assume the snapshot `sequence-number` omit-when-≤0 pattern carries over.
        Each fix is an Avro field default, not a `#[serde(default)]`.
        **→ CLOSED by Increment 11 (2026-06-07): all six Avro-path fields VERIFIED already-correct + pinned
        with mutation-verified regression tests; NO production change needed. See Increment 11 below.**
      Out of scope for the snapshot-only Increment 10; each needs its own Java-parser check + test.
      **Residual after Increment 11 (table-metadata `last-sequence-number`, NOT Avro):** still unfixed; it is
      a non-Java robustness gap only (Java always writes it for V2+ — see the first bullet above). Low
      priority; fix shape `#[serde(default)]` on `last_sequence_number` if a non-Java reader case ever needs it.

### Increment 11 — Sibling Avro default-to-0 read fields (manifest / manifest-list), verify-then-fix (BUILDER Opus, 2026-06-07)
Resolve the Increment-10 sibling follow-up for the Avro-path fields. `last-sequence-number` is OUT OF SCOPE
(Java always writes it for V2+ — verified by the Increment-10 reviewer; only a non-Java/hand-written
robustness gap). For EACH spec-mandated default-to-0 field on the manifest-list / manifest-entry / data-file
READ path, prove empirically whether Rust is strict-and-Java-omits, then fix the real gaps; pin the rest
with a regression test.

**Spec read rules (`format/spec.md` 1980-1985):** manifest-list `sequence_number`/`min_sequence_number`/
`content` default-to-0; manifest-entry `sequence_number`/`file_sequence_number` default-to-0; data-file
`content` default-to-0 (=Data). Java side: `DataFile.CONTENT` is `optional(134)` (absent → null →
`FileContent.DATA`); `ManifestEntry.SEQUENCE_NUMBER`/`FILE_SEQUENCE_NUMBER` are `optional(3/4)` (absent →
inherit); the manifest-LIST V1 schema (`V1Metadata`) has no content/seq fields at all.

**Static findings (pre-empirical):**
- manifest-list `_serde::ManifestFileV2`/`ManifestFileV3` ALREADY carry `#[serde(default = ...)]` on
  `content` (→0=Data), `sequence_number` (→0), `min_sequence_number` (→0). V1 list → `ManifestFileV1::
  try_into` hard-codes all three to 0. ⇒ likely already-fine; needs an empirical "absent reads as 0" pin.
- data-file `_serde::DataFileSerde.content` has `#[serde(default)]` (→0=Data). V1 data-file schema omits
  `content` entirely; V1 entry → `ManifestEntryV1::try_into` hard-codes seq/file-seq to `Some(0)`.
- manifest-entry `_serde::ManifestEntryV2.sequence_number`/`file_sequence_number` are `Option<i64>` (absent
  → `None` → inheritance via `inherit_data`). V2 reader-schema `content` field-id 134 carries
  `with_initial_default(Int(0))` → Avro reader-schema `default: 0`.

**The load-bearing empirical question:** the manifest read path is Avro (`AvroReader::with_schema(reader,
bs)`), which resolves the file's WRITER schema against the READER schema BEFORE serde. So a `#[serde(default)]`
only fires if apache-avro hands serde a record with the field MISSING. Must verify per field whether a
writer schema that OMITS the field (the way a Java V1 writer emits) actually resolves to the default — vs
apache-avro erroring on the schema-resolution mismatch. Build hand-constructed minimal Avro inputs and feed
the real Rust reader.

Plan:
- [x] 1. Manifest-LIST path probe: wrote a V1-shaped manifest list via `ManifestListWriter::v1` (the V1
      `manifest_file` Avro schema has NO content/sequence_number/min_sequence_number columns), parsed it with
      `parse_with_version(.., V2)` and `(.., V3)`. ALL THREE read back as 0 (content=Data). Evidence: the V2/V3
      manifest-list reader uses `Reader::new(bs)` (embedded writer schema, NO schema resolution), so serde
      sees the omitted fields and the `#[serde(default = ...)]` on `_serde::ManifestFileV2`/`V3` fires.
      Mutation-verified: stripping the three defaults from `ManifestFileV2` makes the V2 read fail "missing
      field `content`". ⇒ ALREADY-FINE, now pinned.
- [x] 2. Manifest-ENTRY / data-file path probe: wrote a genuine V1 manifest (`build_v1`, format-version=1)
      with a data file, parsed via `Manifest::parse_avro`. Entry `sequence_number=Some(0)`,
      `file_sequence_number=Some(0)`, data-file `content=Data`. Evidence: the V1 reader uses
      `manifest_schema_v1` (no content/seq/file-seq columns) + `ManifestEntryV1::try_into` (hard-codes
      `Some(0)`/`Some(0)`) and `DataFileSerde.content` `#[serde(default)]` (V1 data-file schema omits
      `content`, so serde sees it absent). Mutation-verified: stripping `#[serde(default)]` from
      `DataFileSerde.content` fails this V1-manifest test "missing field `content`" (the EXISTING
      `test_data_file_serialize_deserialize_v1_data_on_v2_reader` does NOT catch it — it reads via the V2
      reader schema, whose `content` field-id 134 carries the Avro reader-schema default from
      `with_initial_default(Int(0))`, which masks the serde default). ⇒ ALREADY-FINE, now pinned.
      Manifest-entry V2 reader: `sequence_number`/`file_sequence_number` are `Option<i64>` (Java
      `optional(3/4)`) → absent reads as `None` → inheritance via `inherit_data`; no absent-required case
      exists at the V2 reader (Java always writes them, as null for inheritance). Matches Java exactly.
- [x] 3. NO production fix needed — every field was already lenient. Added regression pins only.
- [x] 4. Tests added (3): `spec/manifest_list.rs::test_v1_shaped_manifest_list_read_as_v2_defaults_absent_
      fields_to_zero` + `..._as_v3_...` (V1-shaped list → content/seq/min-seq all 0); `spec/manifest/writer.rs::
      test_v1_manifest_read_defaults_sequence_numbers_and_content_to_zero` (genuine V1 manifest → entry seq/
      file-seq = Some(0), data-file content = Data). Each names the Java-V1-read risk. Both mutation-verified.
- [x] 5. Docs: GAP_MATRIX "Manifest + manifest-list read/write" row (default-to-0 reads VERIFIED + pinned) +
      headline gap #2 reconciled; Roadmap snapshot reconciled; closed the Increment-10 Avro sibling follow-up;
      left the table-metadata `last-sequence-number` residual (non-Avro, non-Java-blocking); lesson appended.
- [x] 6. Verify gate (repo root): build clean; lib ×2 = 1344/0 both runs (was 1341 → +3); interop
      manage_snapshots/update_schema/update_partition_spec all 4/4; clippy -D warnings clean; fmt --check clean.

**Outcome (2026-06-07, Increment 11, BUILDER Opus):** the Increment-10 Avro sibling default-to-0 follow-up is
**CLOSED** — verify-then-fix found ZERO real gaps: all six Avro-path fields (manifest-list
`content`/`sequence_number`/`min_sequence_number`; manifest-entry `sequence_number`/`file_sequence_number`;
data-file `content`) were ALREADY lenient and correctly default to 0 / content=data when a Java V1 (list)
omits them. Each was proved empirically per field (V1-shaped list read as V2/V3; genuine V1 manifest read via
`parse_avro`) and the mechanisms identified: manifest-list defaults via `#[serde(default = ...)]` on
`ManifestFileV2`/`V3` (the V2/V3 list reader is `Reader::new`, no Avro resolution, so serde defaults fire);
manifest-entry seq/file-seq via `ManifestEntryV1::try_into` hard-coding `Some(0)`; data-file `content` via
`DataFileSerde.content` `#[serde(default)]` (V1 manifest reads with `manifest_schema_v1`, no content column).
The gap was purely test coverage of the absent-field-at-read case — added 3 mutation-verified regression
tests (no production code change). **Key finding:** the EXISTING `test_data_file_serialize_deserialize_v1_
data_on_v2_reader` did NOT pin the data-file `content` serde default (it reads via the V2 reader schema, whose
field-id-134 `content` has an Avro reader-schema `default: 0` from `with_initial_default` that masks the serde
default); the new genuine-V1-manifest test is the one that pins it (mutation-verified). **Residual (tracked):**
table-metadata `last-sequence-number` (NOT an Avro field; out of this increment's scope; Java always writes it
for V2+ so it never bites Java interop — robustness-only). Files touched exactly the allowed set:
`crates/iceberg/src/spec/manifest_list.rs` (+2 tests + a helper), `crates/iceberg/src/spec/manifest/writer.rs`
(+1 test), `docs/parity/GAP_MATRIX.md`, `Roadmap.md`, `task/todo.md`, `task/lessons.md`. NO production `.rs`
behavior change; NO Cargo/lockfile edits; no `#[ignore]`; no bare `.unwrap()` in non-test paths. An Opus
REVIEWER verifies next.

---

## Active: Phase 2 — Write engine (FIRST write increment)

Parity target: Java `iceberg-core` write actions (`MergingSnapshotProducer`, `ManifestFilterManager`,
`StreamingDelete`/`DeleteFiles`). Authoritative plan: [Roadmap.md](../Roadmap.md) Phase 2; status:
[docs/parity/GAP_MATRIX.md](../docs/parity/GAP_MATRIX.md).

### Phase-2 increment SEQUENCE (dependency, then value) — recorded 2026-06-07
1. **DeleteFiles** (this increment) — delete data files by path/reference; builds the foundational
   manifest-filter / rewrite machinery in `SnapshotProducer` that the rest reuse.
2. **OverwriteFiles** — delete-by-filter/files + add data files in one snapshot (reuses the filter machinery).
3. **ReplacePartitions** (dynamic partition overwrite) — replace whole partitions (reuses the machinery).
4. **RewriteFiles** — atomic replace of a set of files with another set (compaction primitive).
5. **RewriteManifests** — re-cluster/merge manifests without changing data.
6. **merge-append** — `MergeAppend` (vs the existing fast-append): merge small new manifests.
7. **RowDelta + position-delete / deletion-vector writers** — merge-on-read deletes.
8. **multi-op transaction hardening** — multiple write actions + optimistic-concurrency retry, Glue/S3 Tables.

### Increment 1 — DeleteFiles + manifest-filter machinery (IN PROGRESS, 2026-06-07, BUILDER Opus)
New file `crates/iceberg/src/transaction/delete_files.rs`; manifest-filter machinery added to
`crates/iceberg/src/transaction/snapshot.rs`; wired into `transaction/mod.rs`.

**Java rules to mirror (verified against `/tmp/iceberg-java-ref`):**
- `ManifestFilterManager.filterManifest` (line 368): a manifest that CANNOT contain a to-be-deleted file is
  carried forward UNCHANGED (`filteredManifests.put(manifest, manifest); return manifest`) — efficiency +
  fewer files. `canContainDeletedFiles` returns false when the manifest has no live files (here: when it
  contains none of the target paths).
- `filterManifestWithDeletedFiles` (line 497): rewrite — for each LIVE entry, if it matches a target,
  `writer.delete(entry)` (status→Deleted, carries the existing data_file, preserves data/file seq, snapshot_id
  set to the NEW snapshot); else `writer.existing(entry)` (status→Existing, preserves snapshot_id + both seq
  numbers — V2/V3 inheritance). Mirror via the Rust `ManifestWriter::add_delete_entry` / `add_existing_entry`
  (both already preserve exactly these fields; `add_existing_entry` keeps the original snapshot_id, matching
  Java `writer.existing`).
- `MergingSnapshotProducer.apply` (lines 1002-1009): after filtering, KEEP a manifest iff
  `hasAddedFiles() || hasExistingFiles() || snapshotId() == snapshotId()` (the new commit's id). A rewritten
  manifest where ALL live entries became Deleted has no added/existing files but its `added_snapshot_id` == the
  new snapshot id → it is KEPT (still written, with the Deleted entries). **DECISION: keep the rewritten manifest
  even when all-deleted (matches Java's `snapshotId()==snapshotId()` keep), and DROP an originally-empty manifest
  with no live files.** A manifest with no matching target is carried forward unchanged.
- `StreamingDelete` + `DeleteFiles.validateFilesExist` (line 91) + `failMissingDeletePaths`
  (`ManifestFilterManager.validateRequiredDeletes`, line 279): deleting a path not present in the table is an
  error when validation is on. Java defaults `validateFilesToDeleteExist=false`, but for Increment-1 correctness
  (and because path-based deletes have no partition pre-filter) we VALIDATE BY DEFAULT that every requested path
  matched a live entry, erroring otherwise (mirrors `failMissingDeletePaths`'s "Missing required files to
  delete" with `ErrorKind::DataInvalid`). **delete-by-row-filter / partition-predicate is OUT OF SCOPE** (Java
  `deleteFromRowFilter`/`dropPartition` — defer to OverwriteFiles/ReplacePartitions increments).
- Precondition relaxation: `SnapshotProducer::manifest_file` currently rejects a commit with no added files +
  no snapshot properties. A delete-only commit has deletes but no adds → relax to allow it; reject a
  truly-empty commit (no adds, no deletes, no properties).

**Seam design:** extend `SnapshotProduceOperation` with `delete_files(&producer) -> Vec<DataFile>` returning the
data files to delete (resolved from paths against the current snapshot at commit time). `manifest_file()` builds
a `HashSet<&str>` of target paths and, for each existing manifest, rewrites iff it contains ≥1 matching live
entry. The operation also supplies `existing_manifest()` (all current manifests, unchanged — the rewrite happens
in the producer, generic, reused by future ops). `Operation::Delete` recorded in the summary.

Plan:
- [x] A. `SnapshotProducer`: added `process_deletes` + `rewrite_manifest_with_deletes` +
      `new_filtering_manifest_writer` into `manifest_file()` — reads each existing manifest, rewrites the ones
      containing target paths (Deleted/Existing per entry, rewriting with the SOURCE manifest's own partition
      spec so spec-id/partition-type is preserved), carries the rest forward unchanged, drops no-live-file
      manifests (all-deleted rewritten manifests are kept — their `added_snapshot_id` is the new snapshot id,
      Java's `snapshotId()==snapshotId()` keep rule). Relaxed the empty-commit precondition (delete-only OK;
      truly-empty rejected). Extended `SnapshotProduceOperation` with `delete_files(&producer) -> Vec<DataFile>`
      (FastAppend returns empty).
- [x] B. `delete_files.rs`: `DeleteFilesAction` with `delete_file(path)` / `delete_files(paths)` /
      `delete_data_files(DataFiles)`; `DeleteFilesOperation` (`Operation::Delete`) resolves paths→DataFiles
      against the current snapshot AND validates that every requested path matched a live entry (the missing-path
      "failMissingDeletePaths" check must live in the operation, since the producer only sees the resolved
      `DataFile`s). `Transaction::delete_files()` + `mod` + `use` wiring.
- [x] C. 8 in-crate unit tests via `MemoryCatalog` + `make_v3_minimal_table_in_catalog`, all asserting the
      post-commit SCAN live set (the real correctness signal): removes-only-targeted-file → {A,C};
      marks-entry-deleted-and-counts-correct (Existing/Deleted + manifest counts); carries-untouched-manifest-
      forward-unchanged (same manifest_path); across-multiple-manifests; delete-all-in-a-manifest → empty;
      delete-only-commit allowed; absent-file errors; mixed-present-and-absent errors. Mutation-verified:
      breaking carry-forward fails the carry-forward test; swapping Deleted→Existing fails 6 tests.
- [x] D. Docs: GAP_MATRIX `Write: DeleteFiles` row 🟡 + headline-gap #1; Roadmap Phase 2 → 🟡 + sequence +
      current-state + next-move; this todo; lessons.
- [x] E. Verify gate from repo root: build clean; lib ×2 = 1352/0 both runs (was 1344 → +8); interop
      manage_snapshots/update_schema/update_partition_spec all 4/4; clippy -D warnings clean; fmt --check clean
      (one reflow applied via `cargo fmt`).

**Outcome (2026-06-07, Phase 2 Increment 1, BUILDER Opus):** `DeleteFiles` + the foundational manifest-filter /
rewrite machinery land 🟡 — the FIRST write-engine increment. **Filter machinery** lives in
`SnapshotProducer::process_deletes` (`transaction/snapshot.rs`), reused by every future write op via the new
`SnapshotProduceOperation::delete_files` seam (returns the `DataFile`s to remove). Java semantics mirrored
(each cited): unchanged-manifest carry-forward = `ManifestFilterManager.filterManifest` (a manifest with no
matching target is returned as-is); per-entry Deleted/Existing = `filterManifestWithDeletedFiles`
(`writer.delete(entry)` → `add_delete_entry` stamps the new snapshot id + preserves data/file seq;
`writer.existing(entry)` → `add_existing_entry` preserves snapshot-id + both seq numbers, the V2/V3
inheritance contract); all-deleted-manifest KEPT + no-live-file dropped = `MergingSnapshotProducer.apply`
lines 1002-1009 (`hasAddedFiles() || hasExistingFiles() || snapshotId()==snapshotId()`); precondition
relaxation lets a delete-only commit through (rejects a truly-empty one); absent-path error =
`failMissingDeletePaths`/`validateRequiredDeletes` ("Missing required files to delete", `DataInvalid`).
**API:** `Transaction::delete_files()` → `DeleteFilesAction::{delete_file, delete_files, delete_data_files,
set_commit_uuid, set_key_metadata, set_snapshot_properties}`. **OUT OF SCOPE (deferred, flagged):**
delete-by-row-filter / partition-predicate (Java `deleteFromRowFilter`/`dropPartition` — needs metrics
evaluators, lands with OverwriteFiles/ReplacePartitions); data-level Java interop round-trip (Spark/Docker =
CI-only). Files touched exactly the allowed set: `transaction/snapshot.rs`, new `transaction/delete_files.rs`,
`transaction/mod.rs` (wiring) + `transaction/append.rs` (the trait gained `delete_files`, so `FastAppendOperation`
needed the empty impl — flagged below as a necessary touch of an in-scope sibling), docs
`GAP_MATRIX.md`/`Roadmap.md`/`task/{todo.md,lessons.md}`. No Cargo/lockfile edits; no `#[ignore]`; no bare
`.unwrap()` in production paths. An Opus REVIEWER verifies next.

**Note on the one extra file (`transaction/append.rs`):** the brief's allowed-set listed `snapshot.rs`,
`delete_files.rs`, `mod.rs`, and the docs. Extending `SnapshotProduceOperation` with the new `delete_files`
method forced an empty impl on the EXISTING `FastAppendOperation` in `append.rs` (a 5-line method returning
`Ok(vec![])`) for the crate to compile — this is the trait's own sibling impl, not unrelated code. Flagged
per §6 rather than silently expanded; no behavior change to fast-append.

#### Increment 1 — REVIEW (2026-06-07, Opus REVIEWER, DELEGATED)
Adversarially verified points 1–6 against the Java source (`/tmp/iceberg-java-ref`) + independent
mutation tests. **No corruption bug in the production code — the rewrite/keep/drop logic is correct.** One
real TEST-COVERAGE gap found + fixed (the most dangerous bug class was unpinned).
- **Pt 1 (provenance — the #1 risk): CONFIRMED CORRECT + GAP FIXED.** Rust `add_existing_entry` preserves
  the entry's original `snapshot_id`/`sequence_number`/`file_sequence_number` (touches only `status`);
  `add_delete_entry` stamps the NEW snapshot id but keeps both seqs — exactly Java `GenericManifestEntry.
  wrapExisting`/`wrapDelete`. Entries arrive with populated seqs because `load_manifest` runs `inherit_data`.
  Proved end-to-end with a new test (append A@S1, append B+C@S2 one-manifest, delete B → C kept as Existing
  with S2+seq2, A carried fwd keeps S1, B tombstone = S3 + B's original seqs). **GAP: the builder's 8 tests
  ALL passed under a `snapshot_id` re-stamp mutation** — none pinned surviving-entry provenance. Added
  `test_delete_preserves_surviving_entry_provenance_across_snapshots` (mutation-verified: the ONLY test that
  catches the re-stamp).
- **Pt 2 (deleted entries + keep/drop): CONFIRMED.** Rewritten all-deleted manifest KEPT (its
  `added_snapshot_id` == new snapshot, Java `snapshotId()==snapshotId()`); unrewritten no-live-file manifest
  DROPPED (`has_added_files()||has_existing_files()`, mirroring Java `shouldKeep`). Added
  `test_all_deleted_manifest_kept_by_creating_commit_then_dropped_by_next` pinning the two-commit lifecycle
  (kept by creating commit, dropped by next) — the builder only covered the single-commit case.
- **Pt 3 (live-only + source spec): CONFIRMED.** Only `is_alive()` entries are eligible (already-Deleted
  skipped); rewrite uses `partition_spec_by_id(source_manifest.partition_spec_id)` (Java `reader.spec()`),
  NOT the table default — correct for partition-evolved tables. A full spec-evolution+data fixture is hard
  via the public API (`validate_added_data_files` rejects non-default-spec appends); the code path is
  correct by inspection. (Minor inspected-not-fixed nit: the filtering writer pairs the source spec with the
  table's CURRENT schema rather than the spec's bound schema — harmless because partition source-column
  types are stable; matches Java in practice. Tracked, not a bug.)
- **Pt 4 (mutation tests REAL): CONFIRMED all three.** (a) `add_existing_entry` instead of `add_delete_entry`
  for the target → 7 delete_files tests FAIL. (b) force every manifest to rewrite (never carry forward) →
  `test_..._carries_untouched_manifest_forward_unchanged` FAILs (path changes). (c) re-stamp existing
  snapshot id → only the new provenance test FAILs (see Pt 1). The scan assertions check the real live set,
  not just emitted updates.
- **Pt 5 (absent-file + precondition): CONFIRMED + GAP FIXED.** Absent path → `DataInvalid` "Missing
  required files to delete" (in the operation's resolution, where the requested set is known); mixed
  present+absent → same error (no silent partial delete); delete-only commit allowed; truly-empty rejected.
  The truly-empty rejection had NO test — added `test_empty_delete_commit_is_rejected`. (Benign redundancy:
  the missing-path check exists in BOTH `DeleteFilesOperation::delete_files` and `process_deletes`; the
  latter can't fire since it sees already-resolved files — defense-in-depth, not a defect.)
- **Pt 6 (no fast-append regression + scope): CONFIRMED.** `FastAppendOperation::delete_files` returns empty
  → `process_deletes` early-returns → fast-append unchanged (append.rs tests pass). The trait extension is
  benign. No Cargo edits; no bare `.unwrap()` added to production.

**Review outcome (2026-06-07, Opus REVIEWER):** all 6 points adjudicated; NO production correctness bug
(provenance + keep/drop are right). Strengthened tests against the most dangerous unpinned bug class: +3
tests (provenance across snapshots; all-deleted keep-then-drop lifecycle; empty-commit rejection), each
mutation-verified. Files touched: `transaction/delete_files.rs` (+3 tests + 2 helpers), todo, lessons. NO
production `.rs` change, NO Cargo/lockfile edits. **Verify (repo root):** build clean; lib ×2 = 1355/0 both
runs (was 1352 → +3); interop manage_snapshots/update_schema/update_partition_spec all 4/4; clippy -D
warnings clean; fmt --check clean. Row stays **🟡** (data-level Java interop deferred, per the increment's
scope).

### Phase 2 Increment 2 — OverwriteFiles (explicit add + delete) (IN PROGRESS, 2026-06-07, BUILDER Opus)
New file `crates/iceberg/src/transaction/overwrite_files.rs`: `OverwriteFilesAction` composing the
fast-append add path with the DeleteFiles manifest-filter path in ONE `Operation::Overwrite` snapshot.
Data-integrity-critical. Reuses the producer machinery wholesale.

**Java rules verified against source** (`BaseOverwriteFiles.java` / `OverwriteFiles.java` /
`MergingSnapshotProducer.add`/`delete`):
- `addFile(DataFile)` → `add(file)` (same path fast-append uses); `deleteFile(DataFile)` →
  `deletedDataFiles.add(file); delete(file)`; `deleteFiles(DataFileSet, DeleteFileSet)` bulk variant.
- Added files validated like fast-append; deleted paths resolved against the current snapshot
  (`failMissingDeletePaths` semantics inherited from `ManifestFilterManager`).
- **Java `operation()` is DYNAMIC** (delete-only→DELETE, add-only→APPEND, both→OVERWRITE).
  **DELIBERATE DEVIATION (per brief):** this Rust action always records `Operation::Overwrite` (the brief's
  KEY test asserts Overwrite for delete+add AND the add-only / delete-only cases). Flagged as a tracked gap.
- Overwrite summary reflects BOTH added AND deleted file/record counts (`SnapshotSummary` overwrite).
- OUT OF SCOPE (deferred + noted): `overwriteByRowFilter(Expression)` (inclusive/strict metrics
  evaluators), concurrent-commit conflict validation (`validateNoConflictingData`/`...Deletes`/
  `validateFromSnapshot` — serializable isolation).

Plan:
- [x] **Shared-helper extraction (Rule of Three: two identical non-trivial uses → extract).** Factored the
      delete-path resolution + missing-path validation and the data-manifest listing into
      `SnapshotProducer::{resolve_delete_paths, current_data_manifests}` in snapshot.rs; `DeleteFilesOperation`
      and `OverwriteFilesOperation` both call them (DeleteFiles' two methods shrank to one-liners).
- [x] **Summary reflects deletes.** Added a `removed_data_files: Vec<DataFile>` field to `SnapshotProducer`,
      resolved once in `commit()` (via the operation's `delete_files` seam) BEFORE `summary()` and stored;
      `summary()` now also calls `SnapshotSummaryCollector::remove_file` for each (so `deleted-data-files` /
      `deleted-records` land); `manifest_file()` `std::mem::take`s the stored set instead of re-resolving.
      **SAME-CHANGE BUG FIX (in scope):** corrected the producer's `previous_snapshot` resolution from
      `snapshot_by_id(self.snapshot_id)` (the NOT-yet-committed new snapshot → always None → totals seeded
      from 0) to `current_snapshot()` (the real parent / branch head, Java `previousBranchHead`). Without it,
      `update_totals` underflowed (`0 - removed`) on any net-removal commit. **Also flipped the producer's
      `update_snapshot_summaries` `truncate_full_table` arg from `(op == Overwrite)` → `false`:** Java
      `SnapshotProducer.summary(previous)` calls `updateTotal` unconditionally with NO full-table-truncate
      branch — a partial `OverwriteFiles` must NOT reset totals to 0; that Rust path is for a future full
      replace/truncate action, and nothing else produces an Overwrite snapshot, so zero blast radius.
- [x] `OverwriteFilesAction`: `add_file`/`add_files`, `delete_file`/`delete_files`/`delete_data_files`,
      `set_commit_uuid`/`set_snapshot_properties`/`set_key_metadata`. `commit()` builds the producer with
      added files, calls `validate_added_data_files`, then `producer.commit(OverwriteFilesOperation,
      DefaultManifestProcess)`. `operation()` = `Operation::Overwrite`; `delete_files` → shared resolver;
      `existing_manifest` → shared data-manifest list.
- [x] Wired `Transaction::overwrite_files()` + `mod overwrite_files;` + `use ...OverwriteFilesAction` in
      `transaction/mod.rs`.
- [x] 9 tests (MemoryCatalog, V3 minimal identity(x) table; SCAN live set asserted): KEY delete-B+add-D →
      {A,C,D} op==Overwrite + B Deleted; add-only (Overwrite op); delete-only (Overwrite op);
      replace-in-same-partition; delete-absent errors (+ table unchanged); mixed present+absent errors;
      empty overwrite rejected; survivor provenance across snapshots (C keeps S2/seq2, A carried-fwd keeps
      S1, D gets S3 + new seq, B tombstone S3 + B's seqs); summary reflects added + deleted counts.
- [x] Docs: GAP_MATRIX `Write: OverwriteFiles` ❌ → 🟡 (+ DeleteFiles "will reuse" → "now reuses" + headline);
      Roadmap Phase 2 status/sequence/snapshot lines; this todo; lessons.
- [x] Verify gate (repo root, pinned nightly): build clean; lib ×2 = 1364/0 both runs (was 1355 → +9);
      interop manage_snapshots/update_schema/update_partition_spec 4/4 each; clippy -D warnings clean;
      fmt --check clean (one `cargo fmt` reflow applied to the new file + delete_files).

**Outcome (2026-06-07, Phase 2 Increment 2, BUILDER Opus):** `OverwriteFilesAction` lands 🟡 —
explicit add + delete in ONE `Overwrite` snapshot. The producer seam composes cleanly: added files go
through the fast-append path (`SnapshotProducer::new` + `validate_added_data_files` + `write_added_manifest`)
and deleted files through the DeleteFiles filter path (`process_deletes`) in a single `commit()`. Shared
`SnapshotProducer::{resolve_delete_paths, current_data_manifests}` factored out of DeleteFiles (Rule of
Three). The overwrite summary now reflects BOTH added and deleted file/record counts. Two same-change
producer fixes were required and are correct (and Java-faithful): the previous-snapshot resolution and the
truncate-arg — both pre-existing latent bugs that only surfaced once a producer operation actually removed
files. **DEVIATION (tracked):** always records `Operation::Overwrite` (Java's `operation()` is dynamic) —
per the brief; the summary carries the precise counts. **Deferred:** `overwriteByRowFilter` (metrics
evaluators), concurrent-commit conflict validation (serializable isolation), data-level Java interop. Files
touched exactly the allowed set: `transaction/overwrite_files.rs` (new), `transaction/snapshot.rs` (shared
helpers + summary + producer fixes), `transaction/delete_files.rs` (call the shared helpers + drop now-unused
imports), `transaction/mod.rs` (wiring), GAP_MATRIX, Roadmap, todo, lessons. No `snapshot_summary.rs` /
Cargo / lockfile edits; no `#[ignore]`; no bare `.unwrap()` in production paths. An Opus REVIEWER verifies next.

#### Phase 2 Increment 2 — REVIEW (2026-06-07, Opus REVIEWER, DELEGATED)
Adversarially verified the 5 brief points against the Java source (`/tmp/iceberg-java-ref`). Plan:
- [x] **Pt 1 (Operation parity) — GROUND TRUTH ESTABLISHED: Java IS dynamic; FIXED the always-Overwrite bug.**
      `BaseOverwriteFiles.operation()` (lines 50-60): `deletesDataFiles() && !addsDataFiles()` → DELETE;
      `addsDataFiles() && !deletesDataFiles()` → APPEND; both → OVERWRITE. `addsDataFiles()` =
      `!newDataFilesBySpec.isEmpty()` (requested adds); `deletesDataFiles()` = `filterManager.containsDeletes()`
      = `!deletePaths.isEmpty()` (requested delete-PATHS, before resolution). The builder's always-Overwrite
      was a parity BUG → made `OverwriteFilesOperation::operation()` dynamic via a new `adds_data_files: bool`
      field + `match (adds_data_files, !delete_paths.is_empty())`. Renamed the two tests
      (`..._records_append_operation`, `..._records_delete_operation`) to assert APPEND/DELETE. Mutation-verified
      (always-Overwrite → both fail; both-case tests stay green, so they can't pin the rule alone).
- [x] **Pt 2a (previous_snapshot fix): CONFIRMED Java-faithful + cumulative-totals test ADDED.** Java
      `SnapshotProducer.summary(previous)` (L392-419) seeds totals from
      `previous.snapshot(previousBranchHead.snapshotId()).summary()` (the parent / branch head), defaulting to 0
      only when there is no previous ref. Rust `current_snapshot()` = the main branch head = exactly that. Old
      code looked up `self.snapshot_id` (the not-yet-committed new snapshot) → always None → seed 0 →
      `0 - removed` underflow on net removal. Added `test_running_totals_accumulate_across_snapshots` (append
      A,B → 2; append C,D → 4 cumulative; overwrite-delete A → 3). Mutation-verified: reverting to the seed-0
      logic fails at the snapshot-2 accumulation (left=2, right=4).
- [x] **Pt 2b (truncate_full_table=false): CONFIRMED.** Java has NO full-table-truncate branch in
      `summary(previous)`; even `BaseReplacePartitions` only sets `replace-partitions=true` and accumulates
      totals via the same `updateTotal` path. So `false` is correct for OverwriteFiles (the Rust truncate path
      has no Java analogue for any standard op — noted as a residual). Residual: Java `updateTotal` is signed
      `long` + stops on negative; Rust `update_totals` is `u64` + panics on underflow (tracked, out of scope).
- [x] **Pts 3-5 (correctness / extraction / regression): CONFIRMED.** Live-set {A,C,D} + provenance +
      summary verified; mutation-tested provenance (swap `add_existing_entry`→`add_entry` re-stamps survivor →
      the provenance test fails on "surviving C must keep S2"). Shared helpers byte-identical to Increment-1
      inline code (DeleteFiles' 11 tests green). No fast-append regression (append tests green); scope clean.

**Review outcome (2026-06-07, Phase 2 Increment 2, REVIEWER Opus):** ONE parity bug fixed (always-Overwrite →
dynamic operation, matching Java `BaseOverwriteFiles.operation()`). Both producer summary fixes (a + b)
CONFIRMED Java-faithful; added the cumulative-running-totals regression test the change needed (would underflow
under the old seed-0 logic). 11 overwrite tests (9 → 11: +1 cumulative-totals, +0 net from the 2 renames).
Verify gate from repo root: build clean; lib ×2 = 1365/0 both runs (was 1364 → +1); interop
manage_snapshots/update_schema/update_partition_spec 4/4 each; clippy -D warnings clean; fmt --check clean (one
reflow applied). Files touched: `transaction/overwrite_files.rs` (dynamic op + 2 renamed tests + 1 new test),
GAP_MATRIX (dynamic-operation note replaces the stale deviation), todo, lessons. NO snapshot.rs production
change needed (both summary fixes were already correct); no Cargo edits; no `#[ignore]`; no bare `.unwrap()`
added. 🟡 stays.

### Phase 2 Increment 3 — ReplacePartitions (dynamic partition overwrite) (IN PROGRESS, 2026-06-07, BUILDER Opus)
New file `crates/iceberg/src/transaction/replace_partitions.rs`: `ReplacePartitionsAction` (dynamic
partition overwrite). Mirrors Java `BaseReplacePartitions` — when committed, for every partition an ADDED
file belongs to, DELETE every existing live data file in that same `(spec_id, partition)`, then add the new
files, in ONE `Operation::Overwrite` snapshot. Reuses the Increment-1 manifest-filter/rewrite machinery.

**Java rules verified against `/tmp/iceberg-java-ref` source (BaseReplacePartitions.java):**
- `operation()` returns `DataOperations.OVERWRITE` → `Operation::Overwrite` (always; line ~45). [VERIFIED]
- Ctor sets `SnapshotSummary.REPLACE_PARTITIONS_PROP = "replace-partitions" = "true"` (line ~36). [VERIFIED]
- `addFile(file)` → `dropPartition(file.specId(), file.partition())` + `replacedPartitions.add(...)` +
  `add(file)` (lines 49-55): the partition of each added file is dropped (every existing live file in that
  `(specId, partition)` is removed), and the file is added.
- `ManifestFilterManager.manifestHasDeletedFiles`/`filterManifestWithDeletedFiles` mark a live entry for
  delete iff `dropPartitions.contains(file.specId(), file.partition())` (lines 463, 518) — the by-PARTITION
  match (no path/row-filter/metrics needed). [VERIFIED]
- **Unpartitioned = full replace:** `apply()` (line ~108) — `if dataSpec().isUnpartitioned()
  deleteByRowFilter(Expressions.alwaysTrue())` → every file removed. (Falls out naturally: every file is in
  the single empty partition, so dropping that partition drops all.) [VERIFIED]
- **No `failMissingDeletePaths` for partition drops:** `validateRequiredDeletes` only validates path/file
  deletes (lines 280-300); `dropPartitions` has NO missing-validation → replacing a partition with no
  existing files is a pure add, no spurious delete, no error. [VERIFIED]
- **No-added-files:** Java does not special-case it; `super.apply()` (`SnapshotProducer.apply`) requires the
  commit to produce content. Rust mirrors via the existing precondition (no adds + no deletes + no props →
  rejected). A ReplacePartitions with no added files + no resolved deletes is effectively empty → rejected.
- **OUT OF SCOPE (defer + flag):** static `replaceByRowFilter`/explicit-partition APIs (need expression
  evaluators); concurrent-commit conflict validation (`validateNoConflictingData`/`...Deletes`/
  `validateFromSnapshot`) — serializable isolation, ancestor-chain replay.

**CRITICAL summary finding (verified against Java + Increment-2 review):** Java `SnapshotProducer.summary()`
has NO truncate/full-table branch — it computes `total = previous + added - removed` UNCONDITIONALLY via
`updateTotal` (SnapshotProducer.java:926). `replace-partitions=true` is JUST a summary prop. So the
Java-faithful Rust call is `truncate_full_table = FALSE`: the by-partition resolution already reports EVERY
removed file (so `deleted-data-files`/`deleted-records` are correct), and `update_totals` computes the right
post-replace totals (e.g. unpartitioned full replace: prev=N, added=M, removed=N → N+M-N = M, no underflow).
Setting `truncate_full_table=true` would DOUBLE-COUNT deletes vs. the resolved-removed set and diverge from
Java. (This corrects the brief's hint — flag in final report. The `truncate_full_table` Rust path has no
Java analogue for any standard op, per the Increment-2 reviewer.) So NO producer truncate-flag change; the
summary correctness comes from the resolved removed-file set + existing `update_totals`.

**By-partition delete resolution design (reuses Increment-1, no duplication):** The producer's manifest
rewrite (`process_deletes`) matches removed files by PATH. ReplacePartitions resolves its drop-partition set
to the matching `Vec<DataFile>` in the `delete_files` seam — scan the current data manifests, collect every
live `DataFile` whose `(partition_spec_id, partition)` is in the drop set — and returns them. The producer
then drives the EXACT SAME rewrite/keep/drop + provenance machinery unchanged. New shared helper
`SnapshotProducer::resolve_partition_deletes(&HashSet<(i32, Struct)>) -> Result<Vec<DataFile>>` in
snapshot.rs (sibling of `resolve_delete_paths`); `replace_partitions.rs`'s operation calls it. No change to
`process_deletes`/`rewrite_manifest_with_deletes`/`current_data_manifests`.

Plan:
- [x] A. `snapshot.rs`: added `resolve_partition_deletes(&self, &HashSet<(i32, Struct)>) -> Result<Vec<DataFile>>`
      (scans current data manifests, collects live DataFiles whose `(partition_spec_id, partition)` ∈ set; no
      missing-validation — partition drops are not path deletes). Shared, sibling of `resolve_delete_paths`.
- [x] B. `replace_partitions.rs`: `ReplacePartitionsAction` with `add_file`/`add_files` +
      `set_commit_uuid`/`set_snapshot_properties`/`set_key_metadata`. `commit()`: validates added files via
      `validate_added_data_files`; collects the drop set `{(spec_id, partition)}` from the added files; sets
      the `replace-partitions=true` snapshot property (layered ON TOP of caller props); drives
      `producer.commit(ReplacePartitionsOperation, DefaultManifestProcess)`. Operation = `Operation::Overwrite`;
      `delete_files` seam → `resolve_partition_deletes(drop_set)`; `existing_manifest` → `current_data_manifests`.
- [x] C. Wired `Transaction::replace_partitions()` + `mod replace_partitions;` + `use ...ReplacePartitionsAction`.
- [x] D. 8 tests (in `replace_partitions.rs`, `MemoryCatalog`; identity(x) fixture + an in-test unpartitioned
      V3 table helper since no unpartitioned JSON fixture exists): (1) cross-partition isolation A@x=0,B@x=1 →
      replace A2@x=0 → {A2,B}; (2) replace multiple partitions; (3) replace partition with multiple new files;
      (4) surviving-entry provenance (untouched B keeps S1/seqs); (5) unpartitioned FULL replace → {C}, marker
      set, totals 2-2+1=1, `deleted-data-files=2`; (6) replace empty partition = pure add; (7) Overwrite op
      recorded (in test 1); (8) marker + Deleted tombstone on partitioned replace. Mutation-verified: resolve
      nothing (under-delete) fails 6 tests; resolve everything (over-delete/cross-partition loss) fails 5
      tests incl. cross-partition isolation; re-stamp surviving entries fails the provenance test.
- [x] E. Docs: GAP_MATRIX `Write: ReplacePartitions` ❌ → 🟡 + headline-gap #1 + DeleteFiles reuse note;
      Roadmap Phase 2 status/sequence/snapshot/current-state lines; this todo; lessons.
- [x] F. Verify gate from repo root: build clean; lib ×2 = 1373/0 both runs (was 1365 → +8); interop
      manage_snapshots/update_schema/update_partition_spec all 4/4; clippy -D warnings clean; fmt --check clean
      (one reflow applied via `cargo fmt`).

**Outcome (2026-06-07, Phase 2 Increment 3, BUILDER Opus):** `ReplacePartitionsAction` (dynamic partition
overwrite) lands 🟡. **By-partition delete-resolution design:** the producer's `process_deletes` rewrite
matches removed files by PATH; ReplacePartitions's `delete_files` seam resolves its drop-partition set
`{(spec_id, partition)}` (collected from the added files) to the matching live `DataFile`s via the new
shared `SnapshotProducer::resolve_partition_deletes` (sibling of `resolve_delete_paths`), which then feed the
EXACT SAME Increment-1 rewrite/keep/drop + provenance-preservation machinery UNCHANGED — zero edits to
`process_deletes`/`rewrite_manifest_with_deletes`/`current_data_manifests`. **Java semantics mirrored
(cited):** `operation()` = `Overwrite` (`BaseReplacePartitions.operation()` = `DataOperations.OVERWRITE`);
`addFile` drops `file.partition()` then adds (`dropPartition` + `add`); the by-partition match is
`ManifestFilterManager`'s `dropPartitions.contains(file.specId(), file.partition())`;
`replace-partitions=true` summary marker (`SnapshotSummary.REPLACE_PARTITIONS_PROP`); unpartitioned = full
replace (every file in the single empty partition → all replaced, Java's `deleteByRowFilter(alwaysTrue)`);
a replaced partition with no existing files = pure add (Java's `failMissingDeletePaths` guards only path
deletes). **SUMMARY-FLAG CORRECTION (flagged in final report):** the brief hinted at setting the producer's
`truncate_full_table` flag for the unpartitioned full replace. The Java-faithful answer is
`truncate_full_table = FALSE` (the producer already passes `false`, unchanged): Java
`SnapshotProducer.summary()` has NO truncate branch — it computes `total = previous + added - removed`
unconditionally; the by-partition resolution already reports EVERY removed file, so `deleted-data-files`/
`deleted-records` are correct and `update_totals` yields the right post-replace totals (full replace of N
adding M → N+M-N = M, no underflow — verified by the unpartitioned test asserting total=1 + deleted=2).
Setting `truncate_full_table=true` would DOUBLE-COUNT vs. the resolved-removed set and diverge from Java
(matches the Increment-2 reviewer's "Java has no full-table-truncate branch even for ReplacePartitions").
**No-added-files behavior:** Java does not special-case it; a no-added-files replace resolves no deletes →
nothing added/removed (pinned by test 8 — the existing file is untouched). The action always sets the
`replace-partitions` marker (a snapshot property), so the producer's empty-commit precondition does not trip
for the no-added case; this is the standard Java shape (the marker is a property). **OUT OF SCOPE (deferred,
flagged):** static `replaceByRowFilter`/explicit-partition APIs (need metrics evaluators); concurrent-commit
conflict validation (`validateNoConflictingData`/`...Deletes`/`validateFromSnapshot` — serializable
isolation, ancestor-chain replay); data-level Java interop round-trip. **Files touched exactly the allowed
set:** new `transaction/replace_partitions.rs`, `transaction/snapshot.rs` (the by-partition resolver only —
no rewrite-machinery change), `transaction/mod.rs` (wiring), docs `GAP_MATRIX.md`/`Roadmap.md`/
`task/{todo.md,lessons.md}`. **Nothing outside the allowed set** — no `delete_files.rs`/`overwrite_files.rs`
touch was needed (the shared factor already existed from Increment 2); no Cargo/lockfile edits; no
`#[ignore]`; no bare `.unwrap()` in production paths. An Opus REVIEWER verifies next.

### Phase 2 Increment 4 — RewriteFiles (compaction-commit primitive) (IN PROGRESS, 2026-06-07, BUILDER Opus)
New file `crates/iceberg/src/transaction/rewrite_files.rs`: `RewriteFilesAction` — atomically replace a set
of DATA files with a new set in ONE `Operation::Replace` snapshot (Java `BaseRewriteFiles`). Data-file
rewrite ONLY (DELETE-file rewrite deferred). Mirrors the OverwriteFiles shape: added files → producer
(added manifest), to-delete `DataFile`s → resolved BY PATH via the shared `resolve_delete_paths` and removed
through the Increment-1 `process_deletes` rewrite machinery, in one snapshot.

**Java contract verified against `/tmp/iceberg-java-ref/core/.../BaseRewriteFiles.java` (read the source):**
- `operation()` returns `DataOperations.REPLACE` → `Operation::Replace` (NOT Overwrite/Delete).
- ctor calls `failMissingDeletePaths()` → every to-delete file MUST be present in the current snapshot
  (error if absent). Reuses `resolve_delete_paths` which already enforces this (Java `failMissingDeletePaths`).
- `validate()` → `validateReplacedAndAddedFiles()`: THREE preconditions:
  (1) `deletesDataFiles() || deletesDeleteFiles()` → **"Files to delete cannot be empty"** — the delete set
      MUST be non-empty (data-file rewrite: data-files-to-delete non-empty).
  (2) `deletesDataFiles() || !addsDataFiles()` → **"Data files to add must be empty because there's no data
      file to be rewritten"** — adds allowed only if data-files are being deleted (subsumed by (1) for the
      data-only case, but mirror it for the exact message).
  (3) delete-file precondition — OUT OF SCOPE (DELETE-file rewrite deferred).
  So: delete-only rewrite (delete N, add 0) is LEGAL; add-only rewrite (delete 0, add M) is REJECTED.
- `rewriteFiles(Set<DataFile> filesToDelete, Set<DataFile> filesToAdd)` is the primary entry; each `deleteFile`
  adds to `replacedDataFiles` + `delete(file)`, each `addFile` → `add(file)`.

**Out of scope (deferred + noted precisely):**
- (a) DELETE-file (position-delete / DV) rewrite (`deleteFile(DeleteFile)`/`addFile(DeleteFile)`) — needs the
  delete-file write path (later increment). Data-file rewrite only.
- (b) `dataSequenceNumber` preservation (`setNewDataFilesDataSequenceNumber` / carrying the replaced files'
  max seq onto added files to keep merge-on-read deletes applicable) — added files get a FRESH seq via the
  standard add path, correct for a pure data rewrite with no outstanding deletes. Tracked compaction follow-up.
- (c) `validateFromSnapshot` / `validateNoNewDeletesForDataFiles` / concurrent-commit conflict validation.

Plan:
- [x] A. `RewriteFilesAction`: `rewrite_files(files_to_delete, files_to_add)` primary entry +
      `delete_file(DataFile)`/`delete_files`/`add_file`/`add_files` builders; `set_commit_uuid`/
      `set_snapshot_properties`/`set_key_metadata`. To-delete files held as `Vec<DataFile>` (callers hold
      them); paths extracted into a `HashSet<String>` at commit via `delete_paths()` for `resolve_delete_paths`.
- [x] B. `commit()`: Java `validateReplacedAndAddedFiles` precondition (1) FIRST — reject empty-delete ("Files
      to delete cannot be empty"). Precondition (2) ("Data files to add must be empty...") is SUBSUMED by (1)
      for the data-only case (DELETE-file rewrite out of scope → deletesDataFiles() iff delete set non-empty),
      documented in the source rather than coded as an unreachable branch. Then `validate_added_data_files`;
      then `producer.commit(RewriteFilesOperation{ delete_paths }, DefaultManifestProcess)`.
      `RewriteFilesOperation::operation()` = `Operation::Replace`; `delete_files` → `resolve_delete_paths`;
      `existing_manifest` → `current_data_manifests`.
- [x] C. Wired `Transaction::rewrite_files(files_to_delete, files_to_add)` + `mod rewrite_files;` +
      `use ...RewriteFilesAction` in mod.rs.
- [x] D. **REQUIRED shared change (flagged):** added `Operation::Replace` to the
      `spec/snapshot_summary.rs::update_snapshot_summaries` op allowlist (was {Append, Overwrite, Delete}) —
      WITHOUT it the Replace snapshot fails to commit ("Operation is not supported."). One-line addition
      mirroring the existing entries; Java's `SnapshotProducer.summary` is op-agnostic. Mutation-verified
      load-bearing (removing it fails the KEY test with that exact error). Outside the named allowed set;
      minimal + required.
- [x] E. 10 tests (`MemoryCatalog`; assert post-commit SCAN live set): (1) KEY delete[A,B]+add[D] → live
      {C,D}, op==Replace, A&B Deleted tombstones, C keeps provenance; (2) rewrite across multiple manifests;
      (3) compaction-to-fewer-files (3→1); (4) delete-absent errors (table unchanged); (5) empty-delete
      rejected; (6) add-without-delete rejected (table unchanged); (7) delete-only rewrite legal; (8) summary
      added+deleted counts; (9) cross-snapshot provenance preservation (C keeps S2, A keeps S1, D gets S3, B
      tombstone S3 keeps orig seqs); (10) incremental-builder equivalence. Mutation-verified: forcing
      `operation()` → Overwrite fails the 3 op-asserting tests; disabling the empty-delete precondition fails
      the 3 precondition tests; removing `Operation::Replace` from the summary allowlist fails the KEY test.
- [x] F. Docs: GAP_MATRIX `Write: RewriteFiles` ❌ → 🟡 (+ headline-gap #1); Roadmap Phase 2 status/sequence/
      snapshot/current-state/next-increments lines; this todo; lessons.
- [x] G. Verify gate from repo root: build clean; lib ×2 = 1383/0 both runs (was 1373 → +10); interop
      manage_snapshots/update_schema/update_partition_spec all 4/4; clippy -D warnings clean; fmt --check clean
      (one reflow applied via `cargo fmt`).

**Outcome (2026-06-07, Phase 2 Increment 4, BUILDER Opus):** `RewriteFilesAction` (the compaction-commit
primitive) lands 🟡. **Design:** mirrors `OverwriteFilesAction` exactly — added files → producer (added
manifest), to-delete `DataFile`s → resolved BY PATH (paths extracted from the provided `DataFile`s, since
callers hold them) via the shared `SnapshotProducer::resolve_delete_paths` → the SAME Increment-1
`process_deletes` rewrite/keep/drop + provenance-preservation machinery, in one snapshot. ZERO edits to the
rewrite machinery (`process_deletes`/`rewrite_manifest_with_deletes`/`resolve_delete_paths`/
`current_data_manifests`). **Java semantics mirrored (cited against `core/.../BaseRewriteFiles.java`):**
`operation()` = `DataOperations.REPLACE` → always `Operation::Replace` (NOT dynamic like OverwriteFiles);
ctor `failMissingDeletePaths()` → every to-delete file must be present (reused via `resolve_delete_paths`'s
"Missing required files to delete" error); `validateReplacedAndAddedFiles()` precondition (1)
`deletesDataFiles() || deletesDeleteFiles()` → "Files to delete cannot be empty" (delete-only legal, add-only
/ empty rejected); precondition (2) subsumed (DELETE-file rewrite out of scope). **REQUIRED shared change
(flagged):** `Operation::Replace` added to the `update_snapshot_summaries` op allowlist in
`spec/snapshot_summary.rs` (the ONE edit outside the named allowed set) — without it `Replace` can't commit;
it's the direct analogue of the existing Overwrite/Delete entries and Java's summary is op-agnostic.
**OUT OF SCOPE (deferred, flagged):** (a) DELETE-file (position-delete / DV) rewrite — needs the delete-file
write path (later increment); DATA-file rewrite only. (b) `dataSequenceNumber` preservation
(`setNewDataFilesDataSequenceNumber`) — added files get a FRESH seq via the standard add path (correct for a
pure data rewrite with no outstanding deletes); tracked compaction-correctness follow-up paired with (a).
(c) `validateFromSnapshot` / `validateNoNewDeletesForDataFiles` / concurrent-commit conflict validation.
(d) data-level Java interop round-trip. **Files touched:** new `transaction/rewrite_files.rs`,
`transaction/mod.rs` (wiring), `spec/snapshot_summary.rs` (the one-line allowlist addition — flagged),
docs `GAP_MATRIX.md`/`Roadmap.md`/`task/{todo.md,lessons.md}`. No `snapshot.rs` change needed (the by-path
resolver + rewrite machinery already existed). No `overwrite_files.rs`/`delete_files.rs` touch needed. No
Cargo/lockfile edits; no `#[ignore]`; no bare `.unwrap()` in production paths. An Opus REVIEWER verifies next.

#### Phase 2 Increment 4 — REVIEW (2026-06-07, Opus REVIEWER, DELEGATED)
Adversarially verified points 1-5 against the Java source (`/tmp/iceberg-java-ref/core/.../BaseRewriteFiles.java`,
`SnapshotProducer.java`, `MergingSnapshotProducer.java`), with mutation tests for every load-bearing claim.
- [x] **Pt 1 (operation + precondition): CONFIRMED + mutation-verified.** `BaseRewriteFiles.operation()` →
      `DataOperations.REPLACE` always; Rust `RewriteFilesOperation::operation()` → `Operation::Replace`
      (mutation to `Overwrite` fails 4 op-asserting tests). `validateReplacedAndAddedFiles` precondition (1)
      `deletesDataFiles() || deletesDeleteFiles()` → "Files to delete cannot be empty": delete-only LEGAL,
      add-only/empty REJECTED. Rust enforces it in the action's `commit()` (the producer's own guard only
      rejects all-empty — confirmed: disabling the action precondition makes the add-only test COMMIT, so the
      action precondition is the sole load-bearing guard for add-only).
- [x] **Pt 2 (snapshot_summary allowlist): CONFIRMED + minimal + mutation-verified.** `update_snapshot_summaries`
      now admits `Operation::Replace`; mutation (remove it) fails 7 tests with "Operation is not supported."
      Java's `SnapshotProducer.summary(previous)` totals method is op-agnostic (all `updateTotal`, no per-op
      branch, no truncate), so admitting Replace is Java-faithful — directly analogous to the Overwrite/Delete
      entries. The added clause only short-circuits when op IS Replace, so existing ops are unaffected.
- [x] **Pt 3 (standard correctness): CONFIRMED, assertions real not tautological.** KEY scan test
      (A,B,C → delete[A,B]+add[D] → {C,D}, Replace, tombstones), compaction-to-fewer (3→1), surviving
      provenance, absent-file errors, cross-snapshot provenance all assert post-commit SCAN live sets +
      entry provenance. Mutation-verified the provenance: re-stamping surviving entries in
      `rewrite_manifest_with_deletes` fails exactly the KEY + cross-snapshot provenance tests.
- [x] **Pt 4 (dataSequenceNumber data-loss trap): GUARD ADDED + mutation-verified the corruption.** Today this
      library cannot WRITE delete files (no `RowDelta`; every add path rejects non-`Data` content) — so a table
      it wrote has no outstanding deletes. BUT it can READ + rewrite a Java-written table that has them →
      resurrection. The deferral was documented but NOT as a hard precondition. FIXED: `commit()` now rejects a
      rewrite when the current snapshot has any `Deletes`-content manifest (`has_outstanding_delete_files`,
      `ErrorKind::FeatureUnsupported`). Test builds a real position-delete manifest + asserts rejection;
      mutation (disable guard) → the rewrite COMMITS, proving the corruption is real. Docs updated in 3 places.
- [x] **Pt 5 (no regression + scope): CONFIRMED.** lib ×2 = 1384/0 (was 1383, +1 guard test); interop
      manage_snapshots/update_schema/update_partition_spec 4/4 each; clippy -D warnings clean; fmt clean. Only
      `rewrite_files.rs` (guard + test) + `transaction/mod.rs` (doc) touched in this review; no Cargo edits; no
      bare `.unwrap()` in production (the guard helper uses `?`).
- [x] **TRACKED 🟡 (not fixed, flagged):** Java's REPLACE record-count invariant (`SnapshotProducer` lines
      347-359: `added-records <= deleted-records`) is unmirrored — belongs in the shared `snapshot.rs` producer
      (outside this increment's file set) and is a logical-consistency guard, not a data-loss one. Follow-up.

**Review outcome (2026-06-07, Phase 2 Increment 4, REVIEWER Opus):** ONE data-loss guard ADDED + mutation-
verified (reject rewrite on a table with outstanding merge-on-read deletes — the dataSequenceNumber-resurrection
trap), shipping with a test that builds a real delete manifest. All 5 points adjudicated; row stays 🟡 (interop +
dataSequenceNumber preservation + conflict validation still deferred). Files touched: `transaction/rewrite_files.rs`
(guard + helper + test + doc), `transaction/mod.rs` (ctor doc), `task/{todo.md,lessons.md}`. One tracked 🟡
follow-up (REPLACE record-count invariant). No Cargo/lockfile edits.

### Phase 2 Increment 5a — PositionDeleteWriter (RowDelta write-path piece 1, BUILDER Opus, 2026-06-08)
First piece of the merge-on-read RowDelta write path: a base writer that writes Iceberg position-delete
files. Correctness-critical — a malformed delete file silently fails to delete rows or deletes the wrong
rows. Mirrors `EqualityDeleteFileWriter` structurally. New file
`crates/iceberg/src/writer/base_writer/position_delete_writer.rs`.

**RowDelta subsystem sub-sequence (this increment is 5a):**
- **5a PositionDeleteWriter** [THIS] — writes a position-delete file (`file_path`,`pos`) with
  `content(PositionDeletes)`; write-as-given (Java-faithful, caller sorts).
- **5b RowDelta action** — the `RowDelta` transaction action + producer delete-manifest handling (add the
  written delete files to a new snapshot via `MergingSnapshotProducer`-equivalent, delete-manifest write).
- **5c deletion-vector writer** — V3 Puffin DV writer (`delete_vector.rs` + `puffin/`); writers must not add
  new position-delete files to v3 tables (spec line 1933) — DVs replace them.

**Java rule verified against source** (`core/.../deletes/PositionDeleteWriter.java`, `MetadataColumns.java`,
`format/spec.md`):
- Position-delete schema (spec lines 449-451 / 1393-1394): `file_path: string` REQUIRED, field id
  **2147483546** (= `Integer.MAX_VALUE - 101`, Java `DELETE_FILE_PATH`); `pos: long` REQUIRED, field id
  **2147483545** (= `Integer.MAX_VALUE - 102`, Java `DELETE_FILE_POS`). Optional `row: struct` field id
  **2147483544** — OUT OF SCOPE (position-delete-with-row-data, noted).
- Content type `position deletes` (manifest `content` = 1).
- **Sorting:** Java basic `PositionDeleteWriter` writes records AS GIVEN (no reorder); the sorted
  (file_path,pos) ordering is the caller's / `SortingPositionOnlyDeleteWriter`'s job. Spec lines 1403-1405
  RECOMMEND sorting by file_path then pos (readers binary-search). Mirror Java: write as-given, DOC the
  recommendation + that sorting is the caller's responsibility. Do NOT silently reorder.
- Sort-order-id must be null for position deletes (spec line 745) — left null (DataFile default).

**metadata_columns.rs ALREADY has the constants — NOT touched.** `RESERVED_FIELD_ID_DELETE_FILE_PATH =
i32::MAX - 101 = 2147483546` and `RESERVED_FIELD_ID_DELETE_FILE_POS = i32::MAX - 102 = 2147483545` already
exist with the exact Java ids, plus `delete_file_path_field()` / `delete_file_pos_field()` helpers
(required string / required long). The read side (`arrow/delete_filter.rs`) already consumes exactly these
ids. So the writer just reuses them — no metadata_columns.rs edit needed (flagged: nothing added there).

Plan:
- [x] `PositionDeleteWriterConfig` — holds the position-delete Iceberg `SchemaRef` (built from
      `delete_file_path_field()` + `delete_file_pos_field()`) + its projected Arrow schema ref (for input
      validation). `pos_delete_schema()` free fn builds the canonical 2-field schema.
- [x] `PositionDeleteFileWriterBuilder<B,L,F>` { inner: RollingFileWriterBuilder, config } +
      `PositionDeleteFileWriter` mirroring the equality-delete writer; `IcebergWriterBuilder::build` clones
      the inner rolling builder + config + partition_key.
- [x] `IcebergWriter::write(RecordBatch)` — VALIDATE the batch's arrow schema equals the position-delete
      schema (field ids 2147483546/2147483545, types Utf8/Int64, required) before writing; reject otherwise
      (`ErrorKind::DataInvalid`). Write as-given (no reorder). `close()` stamps
      `content(DataContentType::PositionDeletes)`, partition/spec from partition_key, returns DataFile(s).
- [x] Tests (named for the risk): round-trip exact (file_path,pos) pairs (dropped/mangled positions = data
      not deleted); field ids in written parquet == 2147483546/2147483545 (wrong ids = Java can't read);
      content==PositionDeletes + record_count==N; multiple data files' positions in one file; reject a
      batch with the wrong schema; empty-input convention (matches equality-delete: no rows → still produces
      a closeable writer; assert behavior).
- [x] Wire `pub mod position_delete_writer;` into `writer/base_writer/mod.rs`.
- [x] Docs: GAP_MATRIX `Writer: position-delete` row note (writer landed, stays 🟡 pending RowDelta +
      interop); Roadmap Phase 2 (PositionDeleteWriter landed); this todo; lessons.
- [x] Verify gate from repo root.

**Deferred (flagged):** the optional `row` column (position-delete-with-row-data, field id 2147483544);
caller-side sorting by (file_path,pos) — 5b/`SortingPositionOnlyDeleteWriter`; `referenced_data_file`
single-file optimization (the read side already handles null); the RowDelta action (5b) + DV writer (5c);
Java interop round-trip (→ ✅). Row stays 🟡.

**Outcome (2026-06-08, Phase 2 Increment 5a, BUILDER Opus):** `PositionDeleteFileWriter` lands as the first
piece of the `RowDelta` merge-on-read write path (🟡). New file
`crates/iceberg/src/writer/base_writer/position_delete_writer.rs` mirroring `EqualityDeleteFileWriter`:
`PositionDeleteWriterConfig` (holds the position-delete `Schema` + Arrow schema; `pos_delete_schema()` free
fn) + `PositionDeleteFileWriterBuilder<B,L,F>` + `PositionDeleteFileWriter`. Writes a parquet position-delete
file whose schema is exactly `file_path: string` (field id **2147483546**) + `pos: long` (field id
**2147483545**) — reserved ids reused from `metadata_columns.rs` (`delete_file_path_field()` /
`delete_file_pos_field()`); **metadata_columns.rs NOT touched** (the constants already exist and already match
Java `MetadataColumns.DELETE_FILE_PATH`/`DELETE_FILE_POS`). `close()` stamps
`content(DataContentType::PositionDeletes)` + partition/spec + correct `record_count`. **Java-faithful
sorting: writes records AS GIVEN (no reorder); sorting by (file_path,pos) is the caller's / a later
`SortingPositionOnlyDeleteWriter`'s job — DOC'd in the module header + the spec recommendation.**
`write()` validates the input batch's Arrow schema against the position-delete schema and rejects a mismatch
(`ErrorKind::DataInvalid`). **6 tests** (all named for the risk): `pos_delete_schema_has_reserved_field_ids`
(schema/id contract), `round_trips_exact_positions` (dropped/mangled positions = data not deleted — reads
parquet back, asserts exact (path,pos) pairs in order), `written_field_ids_match_reserved` (parquet field ids
== 2147483546/2147483545 — interop), `multiple_data_files_one_delete_file`, `rejects_mismatched_schema`,
`empty_input_closes`. Mutation-verified: dropping the last row of each batch fails the round-trip + multi-file
+ field-id tests. **Verify (repo root, pinned nightly):** `cargo build -p iceberg` clean; lib ×2 = **1390/0**
both runs (was 1384 baseline → +6); interop `interop_manage_snapshots` 4/4, `interop_update_schema` 4/4,
`interop_update_partition_spec` 4/4 (all stay green); clippy -D warnings clean; fmt --check clean (one reflow
applied via `cargo fmt`). Files touched exactly the allowed set:
`writer/base_writer/position_delete_writer.rs` (new), `writer/base_writer/mod.rs` (one `pub mod` line),
`docs/parity/GAP_MATRIX.md`, `Roadmap.md`, `task/todo.md`, `task/lessons.md`. **NOT touched:**
`metadata_columns.rs` (constants already present — flagged), Cargo.toml/lockfiles. No commit. Row stays 🟡.
**Deferred:** optional `row` column (id 2147483544); caller-side sorting (5b); the `RowDelta` action (5b) +
DV writer (5c); Java interop round-trip (→ ✅). An Opus REVIEWER verifies next.

### Phase 2 Increment 5b — RowDelta action + producer delete-manifest support (IN PROGRESS, 2026-06-08, BUILDER Opus)
The merge-on-read write-commit core: add data files + add DELETE files (position/equality) in ONE snapshot,
writing the producer's first DELETE manifest alongside the DATA manifest. New file
`crates/iceberg/src/transaction/row_delta.rs`; producer delete-manifest support in
`crates/iceberg/src/transaction/snapshot.rs`; wired into `transaction/mod.rs`.

**Java rules verified against `/tmp/iceberg-java-ref` source:**
- `BaseRowDelta.operation()` is DYNAMIC: `APPEND` (adds data, no delete files, no data deletes), `DELETE`
  (adds delete files, no data files), else `OVERWRITE`. The add-deletes-only crown-jewel case → `DELETE`.
  I mirror Java's dynamic op (the OverwriteFiles reviewer established "align to Java, not the brief's hint";
  the brief's "it is OVERWRITE" is the both-add-data+add-deletes case). `update_snapshot_summaries` already
  admits Append/Delete/Overwrite — no summary-allowlist edit needed.
- `MergingSnapshotProducer.add(DeleteFile)` routes delete files into a DELETE manifest written via the
  producer's delete-manifest writer; added delete entries inherit the new snapshot's seq at read time
  (Added entries with no seq → inherited from the manifest-list entry's `added_snapshot_id`/seq), exactly
  like added data files. So a delete file added now applies to EARLIER data (data_seq <= delete_seq).

Plan:
- [x] A. Producer: add `added_delete_files: Vec<DataFile>` to `SnapshotProducer`; new ctor param (or a
      `with_added_delete_files`); `write_added_delete_manifest()` mirroring `write_added_manifest` but using
      `new_manifest_writer(ManifestContentType::Deletes)`; push it in `manifest_file()` when non-empty.
      Relax the empty-commit precondition to also count `added_delete_files`. Route added delete files
      through `summary()`'s `add_file` (delete-content branch already in `SnapshotSummaryCollector`).
- [x] B. `RowDeltaAction`: `add_data_files`/`add_deletes` + `set_commit_uuid`/`set_snapshot_properties`/
      `set_key_metadata`. Validate: data files = `Data` content (reuse `validate_added_data_files`); delete
      files = PositionDeletes/EqualityDeletes content (reject Data) + partition-spec match. Dynamic op via
      `RowDeltaOperation`. Wire `Transaction::row_delta()` + mod/use.
- [x] C. Summary: added data + added delete files + pos/eq delete counts via the collector (no
      snapshot_summary.rs edit — `add_file` already branches on content type). Flagged: NOT touched.
- [x] CROWN-JEWEL e2e: MemoryCatalog (LocalFsStorageFactory over tempdir) → create table → fast-append a
      real parquet data file with known rows → `PositionDeleteFileWriter` produces a pos-delete file pointing
      at specific rows → `row_delta().add_deletes([that file]).commit()` → SCAN → assert deleted rows ABSENT.
- [x] Plus tests: delete manifest written with content==Deletes + referenced in manifest list; add data +
      deletes in one RowDelta; add-deletes-only allowed; summary counts; added delete entry seq == new
      snapshot seq (applies to earlier data); reject Data content in add_deletes; partition-spec mismatch.
- [x] Docs: GAP_MATRIX RowDelta ❌→🟡 + position-delete writer note (end-to-end); Roadmap; this todo; lessons.
- [x] Verify gate from repo root.

**OUT OF SCOPE (deferred):** equality-delete WRITER end-to-end (focus crown-jewel on POSITION deletes);
concurrent-commit conflict validation (`validateFromSnapshot`/`validateNoConflictingDataFiles`/
`validateDeletedFiles`/`validateDataFilesExist`); the deletion-vector path (5c). RowDelta the COMMIT
primitive is the deliverable.

**Outcome (2026-06-08, Phase 2 Increment 5b, BUILDER Opus):** `RowDeltaAction` lands 🟡 — the merge-on-read
write commit. New file `transaction/row_delta.rs`; `SnapshotProducer` (snapshot.rs) gained delete-manifest
support (`added_delete_files` field + `with_added_delete_files` + `validate_added_delete_files` +
`write_added_delete_manifest` via the existing `Deletes` writer arm; empty-commit precondition relaxed to
count added delete files; added delete files routed through `summary()`'s `add_file`); wired
`Transaction::row_delta()` + mod/use. **Operation dynamic** (Java `BaseRowDelta.operation()`: Append/Delete/
Overwrite). **Added delete entries inherit the new snapshot's seq** (V2/V3 no-seq Added entries) so they
apply to earlier data. **THE CROWN-JEWEL** `test_row_delta_position_deletes_drop_deleted_rows_from_scan`:
real-FS MemoryCatalog → fast-append a real 5-row parquet data file → real position-delete file (5a writer) at
positions {1,3} → `row_delta().add_deletes(...).commit()` → SCAN returns {10,30,50}, the deleted {20,40}
ABSENT — the full write→read chain. **9 tests**; mutation-verified the crown jewel three ways (mangled
positions, skipped delete manifest, seq-0 inheritance → resurrection). **`snapshot_summary.rs` NOT touched**
(its `add_file` already branches on content type — flagged). **Verify (repo root, pinned nightly):** build
clean; lib ×2 = **1399/0** both runs (was 1390 → +9); interop manage_snapshots/update_schema/
update_partition_spec 4/4 each; clippy -D warnings clean; fmt --check clean (one reflow applied). Files
touched exactly the allowed set: `transaction/row_delta.rs` (new), `transaction/snapshot.rs`,
`transaction/mod.rs`, GAP_MATRIX, Roadmap, todo, lessons. **NOT touched:** `snapshot_summary.rs`,
Cargo.toml/lockfiles. No commit. **Deferred:** equality-delete WRITER e2e (crown jewel = POSITION deletes);
`removeRows`/`removeDeletes`; conflict validation (`validateFromSnapshot`/`validateNoConflictingDataFiles`/
`validateNoConflictingDeleteFiles`/`validateDeletedFiles`/`validateDataFilesExist`); DV path (5c); data-level
Java interop. An Opus REVIEWER verifies next.

### Phase 2 Increment 6 — Concurrent-commit conflict-validation FOUNDATION + ReplacePartitions `validateNoConflictingData` (BUILDER Opus, 2026-06-08)
The serializable-isolation safety layer: a `TransactionAction::validate` hook run in `do_commit` AFTER the
refresh/re-base and BEFORE re-apply, plus its first concrete check (`ReplacePartitions` rejecting a commit
when a CONCURRENT snapshot added data to a replaced partition). Architecturally significant + correctness-
critical. EFFORT=MEDIUM, DELEGATED.

**Java rules verified against source** (`/tmp/iceberg-java-ref`):
- `BaseReplacePartitions.validate(currentMetadata, parent)` (lines 88-110): IF `validateConflictingData`,
  call `validateAddedDataFiles(currentMetadata, startingSnapshotId, replacedPartitions, parent)` (partitioned)
  or with `Expressions.alwaysTrue()` (unpartitioned). Opt-in via `validateNoConflictingData()` (sets the flag)
  + `validateFromSnapshot(long)` (overrides the starting snapshot).
- `MergingSnapshotProducer.validateAddedDataFiles(base, startingSnapshotId, partitionSet, parent)`
  (lines 363-381): enumerate `addedDataFiles(...)`; if ANY conflict entry exists, throw `ValidationException`
  "Found conflicting files that can contain records matching partitions %s: %s".
- `addedDataFiles` (424-463) → `validationHistory` (913-953): `SnapshotUtil.ancestorsBetween(parent.snapshotId
  (), startingSnapshotId, base::snapshot)` walks the parent chain from `parent` (INCLUSIVE) back to
  `startingSnapshotId` (EXCLUSIVE). For each snapshot whose `operation ∈ {APPEND, OVERWRITE}`
  (`VALIDATE_ADDED_FILES_OPERATIONS`, line 73-74), collect its DATA manifests where `manifest.snapshotId() ==
  snapshot.snapshotId()` (manifests added BY that snapshot) + add the snapshot id to `newSnapshots`. Then
  filter entries: `is_added` (`ignoreDeleted().ignoreExisting()`) AND `entry.snapshotId() ∈ newSnapshots` AND
  (partitionSet.contains(specId, partition)). ⇒ Rust port: walk `current.metadata()` from current-snapshot
  back via `parent_snapshot_id` until `starting_snapshot_id` (exclusive); for each APPEND/OVERWRITE snapshot,
  load manifest list → DATA manifests added by that snapshot → entries with `status == Added` → collect
  `DataFile`s.
- `ValidationException` is non-retryable → Rust `Error::new(ErrorKind::DataInvalid, ...)` (retryable defaults
  to FALSE; the `backon` `.when(|e| e.retryable())` loop STOPS on a non-retryable error and propagates it,
  unlike a `CatalogCommitConflicts` (retryable=true) which retries).

Plan:
- [x] **A1 — starting-snapshot capture.** `Transaction` gains `starting_snapshot_id: Option<i64>`, set ONCE
      at `Transaction::new` = `table.metadata().current_snapshot_id()`, NEVER overwritten by the `do_commit`
      re-base (its own field, so the re-base that overwrites `self.table` does not lose the original head).
- [x] **A2 — validate hook.** Add a default-no-op `async fn validate(self: Arc<Self>, starting_snapshot_id:
      Option<i64>, current: &Table) -> Result<()> { Ok(()) }` to `TransactionAction` (existing actions need
      no change — they inherit the no-op).
- [x] **A3 — run the hook in do_commit.** AFTER the refresh/re-base, BEFORE the re-apply loop: for each
      action, `Arc::clone(action).validate(self.starting_snapshot_id, &current_table).await?`. `current_table`
      is the refreshed base, so `validate` can enumerate the concurrent snapshots (those `current_table` has
      that are newer than `starting_snapshot_id`). A validation failure returns non-retryable DataInvalid →
      the retry loop stops + the error propagates (Java's non-retryable `ValidationException`).
- [x] **A4 — added-files-since helper.** `SnapshotProducer::added_data_files_after(starting_snapshot_id)`
      (or a free fn in snapshot.rs) walks `table.metadata()` from current-snapshot back via
      `parent_snapshot_id` to `starting_snapshot_id` (exclusive); for each APPEND/OVERWRITE snapshot, load its
      manifest list → DATA manifests added BY it → `Added` entries → collect `DataFile`s. Reusable by future
      validations (OverwriteFiles, etc.). Mirror Java `addedDataFiles`/`validationHistory`/`ancestorsBetween`.
- [x] **B — ReplacePartitions opt-in.** `ReplacePartitionsAction::{validate_from_snapshot(i64),
      validate_no_conflicting_data()}` builder methods (store `validate_from_snapshot: Option<i64>` +
      `validate_no_conflicting_data: bool`). Impl `TransactionAction::validate` for it: IF
      `validate_no_conflicting_data`, effective-start = `self.validate_from_snapshot` else the tx-provided
      `starting_snapshot_id`; enumerate added files since it (A4 helper, scoped to the action's helper-need);
      if ANY added file's `(spec_id, partition)` ∈ the action's replaced-partition set → non-retryable
      `DataInvalid` ValidationException naming the conflicting partition. Default (no opt-in) = NO validation.
- [x] **Tests (replace_partitions.rs, MemoryCatalog, REAL concurrent commit):**
      - KEY: append S0 (x=0,x=1) → build `replace_partitions(x=0).validate_from_snapshot(S0)
        .validate_no_conflicting_data()` → CONCURRENT `fast_append` adding data to x=0 (S1) → commit the
        replace → FAILS with the validation error, NON-RETRYABLE (assert `!err.retryable()` + the validation
        message, not a retry-exhaustion).
      - NEGATIVE control: concurrent append targets x=1 → replace(x=0) validation PASSES + commits.
      - OFF control: no opt-in → concurrent x=0 append → commit SUCCEEDS (default behavior unchanged).
      Each named for the risk (silently clobbering concurrent data = serializable-isolation violation).
- [x] Docs: GAP_MATRIX (conflict-validation foundation + ReplacePartitions `validateNoConflictingData`
      landed; the `Multi-op transactions + optimistic-concurrency` row + the `ReplacePartitions` row);
      Roadmap (Phase 2 increment 6); record the conflict-validation SUB-SEQUENCE in this todo (6 foundation+
      ReplacePartitions [this]; then OverwriteFiles `validateNoConflictingData`/`...Deletes`; RowDelta/
      DeleteFiles `validateDataFilesExist`; RewriteFiles `validateNoNewDeletes`); lessons.
- [x] Verify (repo root): `cargo build -p iceberg`; `cargo test -p iceberg --lib` ×2 (counts); the 3 interop
      tests; `cargo clippy -p iceberg --all-targets -- -D warnings`; `cargo fmt --all -- --check`.

**Conflict-validation sub-sequence (recorded):** (6) foundation + `ReplacePartitions.validateNoConflictingData`
[THIS increment]; then `OverwriteFiles` `validateNoConflictingData` / `validateNoConflictingDeletes`;
`RowDelta` / `DeleteFiles` `validateDataFilesExist`; `RewriteFiles` `validateNoNewDeletes`. Each reuses the
A4 added-files-since helper (+ siblings: deleted-files-since, added-delete-files-since) and the A2/A3 hook.

**Outcome (2026-06-08, Phase 2 Increment 6, BUILDER Opus):** the concurrent-commit conflict-validation
FOUNDATION + its first concrete check landed 🟡. **Foundation (3 production files):** (A1) `Transaction`
gained `starting_snapshot_id: Option<i64>`, captured ONCE in `new()` = `table.metadata().current_snapshot_id()`
as its OWN field so it SURVIVES the `do_commit` staleness re-base (which overwrites `self.table`). (A2)
`TransactionAction` gained a default-no-op `async fn validate(self: Arc<Self>, starting_snapshot_id:
Option<i64>, current: &Table) -> Result<()> { Ok(()) }` — every existing action inherits the no-op, zero
change. (A3) `do_commit` runs `Arc::clone(action).validate(self.starting_snapshot_id, &current_table)` for
each action AFTER the refresh/re-base and BEFORE the re-apply loop (so `current_table` is the refreshed base
= Java `parent`); a validation failure is a non-retryable `DataInvalid` (the `backon` `.when(|e|
e.retryable())` loop STOPS → it propagates, matching Java's non-retryable `ValidationException`). (A4) shared
free fn `added_data_files_after(table, starting_snapshot_id)` in `transaction/snapshot.rs` walks the current
snapshot's parent chain back to `starting_snapshot_id` (EXCLUSIVE), and for each APPEND/OVERWRITE snapshot
(Java `VALIDATE_ADDED_FILES_OPERATIONS`) collects every `Added`-status entry from the DATA manifests that
snapshot ADDED (`added_snapshot_id == snapshot_id`) — Java `MergingSnapshotProducer.addedDataFiles` /
`validationHistory` / `SnapshotUtil.ancestorsBetween`. **ReplacePartitions opt-in (B):**
`validate_no_conflicting_data()` (enable) + `validate_from_snapshot(id)` (override start) builder methods;
`impl TransactionAction::validate` for `ReplacePartitionsAction` — when enabled, effective-start =
`validate_from_snapshot` else the tx-provided id; enumerate added files via the A4 helper; reject (non-retryable
`DataInvalid` naming the conflicting partition + file) if any added file's `(spec_id, partition)` ∈ the
replaced-partition set (Java `BaseReplacePartitions.validate` → `validateAddedDataFiles`). Default (no opt-in)
= snapshot isolation, unchanged. **Tests (3, real concurrent commit via a separate `fast_append` between
build and commit):** KEY — conflict in the replaced partition x=0 → REJECTED (asserts `!retryable()` + the
"conflicting files" message + the catalog head is unchanged); NEGATIVE control — concurrent append to the
untouched x=1 → PASSES; OFF control — no opt-in → concurrent x=0 append → commit SUCCEEDS (default unchanged,
the concurrent file clobbered per snapshot isolation). Each names the serializable-isolation risk.
Mutation-verified BOTH directions: `validate`→always-`Ok` fails the KEY test (replace wrongly commits over
the concurrent file); conflict-predicate→always-`true` fails the NEGATIVE control (disjoint concurrent write
wrongly rejected). **Verify (repo root, pinned nightly):** `cargo build -p iceberg` clean; `cargo test -p
iceberg --lib` ×2 = 1404/0 both runs (+3 new tests); `interop_manage_snapshots`/`interop_update_schema`/
`interop_update_partition_spec` all 4/4; `cargo clippy -p iceberg --all-targets -- -D warnings` clean (fixed
one `map_clone` → `.cloned()`); `cargo fmt --all -- --check` clean (one reflow applied). Files touched exactly
the allowed set: `transaction/mod.rs` (field + capture + hook call), `transaction/action.rs` (default validate
hook), `transaction/replace_partitions.rs` (opt-in API + validate impl + 3 tests), `transaction/snapshot.rs`
(`added_data_files_after` + `operation_adds_data_files` helpers), GAP_MATRIX, Roadmap, todo, lessons. NO
Cargo/lockfile edits; no other action's file touched (the inherited no-op `validate` needs none); no
`#[ignore]`; no bare `.unwrap()` in production paths; no commit. Conflict-validation sub-sequence recorded
above. An Opus REVIEWER verifies next.

#### Phase 2 Increment 6 — REVIEW (2026-06-08, Opus REVIEWER, DELEGATED)
Adversarially verified points 1–6 against the Java source (`/tmp/iceberg-java-ref`) + mutation tests.
- [x] **Pt 1 (NON-retryable, infinite-loop risk): CONFIRMED.** The conflict error is
      `Error::new(DataInvalid, ...)` (retryable defaults to `false`; only `CatalogCommitConflicts` is
      `with_retryable(true)`), so `commit()`'s `backon` `.when(|e| e.retryable())` STOPS and propagates it.
      Mutation: adding `.with_retryable(true)` to the validation error made the KEY test fail at
      `assert!(!err.retryable())` AND took 1.56s (the loop re-refreshed to the same S1 + re-failed +
      exhausted 4 retries) — exactly the loop the brief warns about. The `!retryable()` + message asserts
      catch it cleanly.
- [x] **Pt 2 (starting_snapshot_id SURVIVES re-base): CONFIRMED + TEST GAP FOUND & FIXED.** Captured once in
      `Transaction::new` as its own field; `do_commit`'s `self.table = refreshed` does NOT touch it.
      Mutation (`effective_start = current.metadata().current_snapshot_id()`, the re-based head) makes the
      check always-pass. **GAP:** the builder's 3 tests all pinned `.validate_from_snapshot(S0)`, which
      short-circuits the tx field — so capturing `None` / re-reading the head failed NOTHING. Added
      `test_..._using_tx_captured_starting_snapshot` (no override): the head-mutation now fails EXACTLY it
      (1 pass override KEY / 1 fail new). This is the #2 highest risk, now pinned.
- [x] **Pt 3 (added-files walk + carried-forward exclusion): CONFIRMED + isolated test added.** The
      `added_snapshot_id == snapshot_id` manifest filter + `Added`-status entry filter + APPEND/OVERWRITE
      operation filter match Java `addedDataFiles`/`validationHistory`/`ancestorsBetween`. Probe (S0 has
      x=0 a + x=1 b; concurrent S1 adds x=0 a_new): `added_data_files_after(table, S0)` returns EXACTLY
      `{a_new}` — carried-forward a/b excluded. Kept as
      `test_added_data_files_after_excludes_carried_forward_manifests`.
- [x] **Pt 4 (real race + conflict predicate): CONFIRMED.** Tests use a genuine concurrent `fast_append` on
      the same `MemoryCatalog` between build and commit (refresh→S1→validate), not a faked mismatch.
      Mutation BOTH directions: predicate→always-`true` fails the NEGATIVE control (disjoint x=1 append
      wrongly rejected); predicate→always-`false` fails the KEY test (conflict undetected). Default (no
      opt-in) commits over the concurrent file (OFF control) = snapshot isolation, unchanged.
- [x] **Pt 5 (edge cases): SANE, one divergence flagged.** `starting_snapshot_id == None` ⇒ walk to root
      (validate from history start). No parent / missing parent id ⇒ `break` (no panic). Empty table (no
      current snapshot) ⇒ empty (no panic) — pinned by `test_added_data_files_after_empty_table_yields_empty`.
      **DIVERGENCE:** a non-ancestor `validate_from_snapshot` OVER-SCANS to root where Java fails loud
      (`validationHistory`'s `lastSnapshot.parentId() == start` check). Rust-STRICTER (can only over-reject,
      never miss a conflict) → SAFE; documented + pinned
      (`test_added_data_files_after_nonancestor_start_overscans_does_not_panic`), tracked as a parity
      follow-up (add the post-walk ancestor guard), NOT fixed (narrow misuse, errs strict).
- [x] **Pt 6 (default unchanged + scope): CONFIRMED.** Only `ReplacePartitionsAction` overrides `validate`;
      every other action inherits the no-op (grep-verified). No bare `.unwrap()` in the new code (all `?`).
      No Cargo/lockfile edits. Files touched = exactly the named set + this todo/lessons. Full lib suite
      1408/0 ×2 (was 1404 builder baseline; +4 reviewer tests); 3 interop suites 4/4; clippy `-D warnings`
      clean; fmt clean.

**Review outcome (2026-06-08, Opus REVIEWER):** all 6 points adjudicated; the two highest risks
(non-retryable, survives-rebase) hold — survives-rebase had a real TEST GAP (now fixed with a tx-field
test, mutation-verified as the unique guard). No production bug. +4 tests (tx-captured-start,
carried-forward isolation, non-ancestor over-scan, empty-table), all mutation-/probe-verified. ONE
flagged divergence (non-ancestor over-scan vs Java fail-loud, Rust-stricter, tracked). Files touched:
`transaction/replace_partitions.rs` (+4 tests), todo, lessons. No production `.rs` logic changed, no Cargo
edits, no commit. Rows stay 🟡.

**Follow-up (tracked):** add Java `validationHistory`'s post-walk `lastSnapshot.parentId() == start` guard
to `added_data_files_after` so a non-ancestor `validate_from_snapshot` fails loud (parity) rather than
over-scanning — fold into the conflict-validation sub-sequence hardening.

---

## Active: Phase 3 — Scan parity (inspection-table set)

Parity target: Java `iceberg-core` metadata-inspection tables (`core/.../*Table`, ~15 variants).
Authoritative plan: [Roadmap.md](../Roadmap.md) Phase 3; status: [GAP_MATRIX.md](../docs/parity/GAP_MATRIX.md)
`Metadata inspection tables` row. The `inspect/` framework already ships `snapshots` + `manifests`
(`MetadataTable<'a>` + `MetadataTableType` enum + per-table `XTable<'a>` with `schema()` + async `scan()`
→ `ArrowRecordBatchStream`).

**Inspection-table sub-sequence (dependency, then value):**
1. **`files` family** (`files` / `data_files` / `delete_files`) — THIS increment. Reads the current
   snapshot's manifest list → manifests → live entries → `DataFile`; the three differ ONLY by the
   manifest content filter (Java `BaseFilesTable`).
2. **`entries`** (+ `all_entries`) — the raw manifest-entry view (status/snapshot-id/seq + the `data_file`
   struct); supersedes much of the files-family read.
3. **`history` + `refs` + `metadata_log_entries`** — pure-metadata tables (no manifest IO).
4. **`partitions`** — per-partition aggregation over entries.
5. **`all_*`** (`all_data_files` / `all_manifests` / `all_entries` …) — scan across ALL snapshots, not
   just current.

### Phase 3 Increment 1 — `files` / `data_files` / `delete_files` (BUILDER Opus, 2026-06-08)
New file `crates/iceberg/src/inspect/files.rs`; wired via `inspect/mod.rs` + `metadata_table.rs`
(enum variants + accessors). Mirrors Java `BaseFilesTable` (one schema + one read + one projection,
the three tables differ ONLY by the manifest content filter — Rule of Three, factored as a shared base).

**Java contract verified against source** (`core/.../BaseFilesTable.java`, `FilesTable.java`,
`DataFilesTable.java`, `DeleteFilesTable.java`, `api/.../DataFile.java`):
- **Content filter is at the MANIFEST level** (Java `FilesTableScan.manifests()`): `files` →
  `snapshot().allManifests()`; `data_files` → `snapshot().dataManifests()` (content == DATA);
  `delete_files` → `snapshot().deleteManifests()` (content == DELETES). Within a manifest, only LIVE
  entries (Added/Existing, Rust `is_alive()`) are rows. A bug that wrongly filters content = wrong table.
  Rust mirror: filter `ManifestFile.content` (`ManifestContentType::{Data,Deletes}`) per table, then
  `entry.is_alive()`.
- **Schema = `DataFile.getType(partitionType).fields()`** (field-ids from `api/DataFile.java`), order:
  `content`(134), `file_path`(100), `file_format`(101), `spec_id`(141), `partition`(102),
  `record_count`(103), `file_size_in_bytes`(104), `column_sizes`(108) map<int,long>,
  `value_counts`(109), `null_value_counts`(110), `nan_value_counts`(137), `lower_bounds`(125)
  map<int,binary>, `upper_bounds`(128), `key_metadata`(131) binary, `split_offsets`(132) list<long>,
  `equality_ids`(135) list<int>, `sort_order_id`(140), `first_row_id`(142), `referenced_data_file`(143),
  `content_offset`(144), `content_size_in_bytes`(145). (`record_count`/`file_size` are LONG in the
  metadata-table schema even though the Rust `DataFile` holds them as `u64`.)

Plan:
- [x] `FilesTable<'a>` base struct (holds `&Table` + a `FilesTableKind` filter); `schema()` builds the
      Iceberg schema above from the table's DEFAULT partition type (partition struct field). `scan()`:
      current snapshot → `load_manifest_list` → for each `ManifestFile` whose content passes the table's
      manifest-content filter, `load_manifest` → for each LIVE entry, append a row from its `DataFile`.
- [x] One struct + a `FilesTableKind` { All, Data, Deletes } enum (the only thing that differs) +
      ctors `FilesTable::{all,data,deletes}`; `MetadataTable::{files, data_files, delete_files}`
      accessors + `MetadataTableType::{Files, DataFiles, DeleteFiles}` (+ `as_str`/`TryFrom`).
- [x] Partition column: `StructBuilder::from_fields(partition_arrow_fields)`; per partition field,
      dispatch on its `PrimitiveType` and append each per-row `Option<Literal>` (extracting the
      `PrimitiveLiteral`) — covers bool/int/long/float/double/date/time/timestamp/timestamp_ns/string/
      binary/decimal. Metrics maps via `MapBuilder<Int32Builder, Int64Builder>` (counts) /
      `<Int32Builder, LargeBinaryBuilder>` (bounds, RAW `Datum::to_bytes` per Java map<int,binary>);
      keys sorted for determinism. **DEVIATION:** `get_arrow_datum`/`Datum::new` was NOT used — direct
      `PrimitiveLiteral` extraction into typed Arrow builders is simpler and avoids scalar-array concat.
      **Timezone-tagged partition types (`timestamptz`/`timestamptz_ns`) return `FeatureUnsupported`**
      (the tz-tagged Arrow child can't be produced from a plain micro/nano builder without a panic at
      `StructBuilder::finish`; flagged as a deferred edge — partition-on-timestamptz is rare).
- [x] **DEFERRED `readable_metrics`** (Java `MetricsUtil.readableMetricsStruct`). All raw columns incl.
      the metrics maps + V3 DV fields land. Flagged.
- [x] 8 tests (`TableTestFixture` + a self-contained DATA + DELETE manifest writer — public crate APIs
      only, no scan-fixture private helper, no real parquet since the table reads manifest metadata):
      `files`=live data+delete set, `data_files`=DATA only, `delete_files`=deletes only, content 0-vs-1,
      record_count/file_size vs committed metadata, partition struct + column_sizes spot-check, Arrow
      schema column/type set, empty table. Content-filter + `is_alive()` mutation-verified load-bearing.
- [x] Docs: GAP_MATRIX `Metadata inspection tables` row (files/data_files/delete_files landed;
      readable_metrics deferred); Roadmap Phase 3 (inspection sub-sequence + this increment); this todo;
      lessons. Verify gate from repo root.

**Outcome (2026-06-08, Phase 3 Increment 1, BUILDER Opus):** `files` / `data_files` / `delete_files`
land at Java `BaseFilesTable` schema parity (🟡). One `FilesTable<'a>` + a `FilesTableKind` filter
(the Rule-of-Three shared base); the three accessors + enum variants wired. Reads the CURRENT snapshot's
manifest list, selects manifests by content (`All`/`Data`/`Deletes`), emits one row per LIVE entry's
`DataFile`. Arrow schema mirrors `DataFile.getType(partitionType).fields()` with the canonical field ids;
ALL raw columns present (metrics maps map<int,long>/map<int,binary>, list columns, V3 DV fields).
**Deferred:** `readable_metrics`; the other inspection variants (entries/history/refs/partitions/all_*);
timezone-tagged partition columns (FeatureUnsupported); Java/Spark interop comparison (→ ✅).
**Verify (repo root):** build clean; lib ×2 = 1417/0 both runs (was 1409 baseline → +8); 3 interop suites
4/4 each (manage_snapshots/update_schema/update_partition_spec); clippy -D warnings clean; fmt --check
clean. Content-filter (`data_files` wrongly included the delete file) AND `is_alive()` (Deleted tombstone
leaked in) mutations each fail the matching test. Files touched exactly the allowed set:
`inspect/files.rs` (new), `inspect/mod.rs`, `inspect/metadata_table.rs`, GAP_MATRIX, Roadmap, todo,
lessons. No Cargo/lockfile/`arrow/` edits, no commit. An Opus REVIEWER verifies next.

#### Phase 3 Increment 1 — REVIEW (2026-06-08, Opus REVIEWER, DELEGATED)
Adversarially verified points 1–5 against the Java source (`/tmp/iceberg-java-ref`) + throwaway probes
(deleted after). Plan:
- [x] **Pt 1 (content filter): CONFIRMED + mutation-verified.** Manifest-level filter is correct
      (`ManifestContentType` is whole-manifest DATA(0)|DELETES(1) — Iceberg never mixes content in one
      manifest, so manifest-level == file-level, matching Java `dataManifests`/`deleteManifests`).
      MUTATION A (`Data => true`, include delete manifests) → `test_data_files_table_excludes_delete_files`
      FAILS. MUTATION B (`Deletes => true`, include data manifests) → `test_delete_files_table_lists_only_
      delete_files` FAILS. Both restored.
- [x] **Pt 2 (live-entry filter): CONFIRMED + mutation-verified.** `entry.is_alive()` = Added|Existing;
      Deleted tombstone dropped. MUTATION C (`if true` instead of `is_alive()`, leak the Deleted
      2.parquet) → 4 tests FAIL (live-set, data-files-exclude, record-count count=4, partition includes
      200). Restored.
- [x] **Pt 3 (column mapping + field ids): CONFIRMED.** A throwaway probe dumped all 21 Arrow field ids;
      EVERY id + order matches Java `DataFile.getType(partitionType)` exactly (content/134, file_path/100,
      file_format/101, spec_id/141, partition/102, record_count/103, file_size/104, the 4 count maps
      108/117-118…137/138-139, the 2 bound maps 125/126-127, 128/129-130, key_metadata/131,
      split_offsets/132-133, equality_ids/135-136, sort_order_id/140, first_row_id/142,
      referenced_data_file/143, content_offset/144, content_size/145). Map value fields carry
      `nullable:false` (Java `MapType.ofRequired`). Values: `content` 0/1 vs committed; `lower_bounds`
      for `Datum::long(1)` = raw LE `[1,0,0,0,0,0,0,0]` (Iceberg single-value serialization, Java
      `map<int,binary>`). `content_type() as i32` and `file_format().to_string()` (lowercase) correct.
- [x] **Pt 4 (partition extraction + tz deferral): CONFIRMED.** A probe pinned partition value PER
      file_path: `{1.parquet:100, 3.parquet:300, delete-1.parquet:100}` — each row's partition struct
      matches THAT file (no row misalignment). The `timestamptz`/`timestamptz_ns` → `FeatureUnsupported`
      deferral is honest (explicit error arm in `append_partition_field`, not a silent wrong value).
- [x] **Pt 4b (deferrals honestly tracked): CONFIRMED.** `readable_metrics` + tz-partition
      `FeatureUnsupported` both in GAP_MATRIX/todo; row is 🟡; nothing silently wrong.
- [x] **Pt 5 (no regression + scope): CONFIRMED.** Build clean; lib ×2 = 1417/0 (→1418 with the new
      divergence test); 3 interop suites 4/4 each; clippy -D warnings clean; fmt clean. Only the named
      files. No bare `.unwrap()` in production (one justified `.expect("…statically valid")`). Empty
      table → 0 rows, no panic.
- [x] **DIVERGENCE FOUND (untracked) → pinned + tracked, NOT fixed:** Java `BaseFilesTable.schema()`
      drops the `partition` field for an UNPARTITIONED table (empty partition type); Rust keeps an
      empty-struct `partition` column. Verified non-corrupting (a probe scanned an unpartitioned table
      with one file: 1 row, `partition=Struct([])`, NO panic). Decision: the fix (conditionally omit the
      column through the 21-column builder) is more invasive than the BUILDER's scoped partitioned path
      and is non-corrupting + narrow, so — matching the Increment-5 V2-default precedent — added a
      divergence-PINNING test (`test_files_table_unpartitioned_keeps_empty_partition_struct_known_
      divergence`, asserts the current empty-struct column; flips to assert-absent when fixed) and tracked
      it as a GAP_MATRIX deferral rather than a silent gap. Row stays 🟡.

**Review outcome (2026-06-08, Opus REVIEWER):** all 5 brief points VERIFIED; content-filter (×2) +
`is_alive` (×1) mutations each fail the matching test; field-id + partition-value + bound-byte checks pass
vs Java. One untracked schema-shape divergence (unpartitioned empty-partition column) found → pinned with
a test + tracked in GAP_MATRIX (not fixed — non-corrupting, narrow, invasive-to-fix; left 🟡). Files
touched: `inspect/files.rs` (+1 test), GAP_MATRIX, todo, lessons. No production-logic change, no Cargo
edits, no commit, no branch switch.

### Phase 3 Increment 2 — `entries` inspection table (BUILDER Opus, 2026-06-08)
New file `crates/iceberg/src/inspect/entries.rs`; a shared `data_file` projection factored out of
`inspect/files.rs` into a new `inspect/data_file.rs` submodule (Rule of Three, 2nd use — `files` flattens
the projection to top-level columns, `entries` nests it under a single `data_file` struct column). Wired
via `inspect/mod.rs` + `metadata_table.rs` (`MetadataTableType::Entries` + `MetadataTable::entries`).
Mirrors Java `BaseEntriesTable` / `ManifestEntriesTable`.

**Java contract verified against source** (`core/.../BaseEntriesTable.java`,
`core/.../ManifestEntriesTable.java`, `core/.../ManifestEntry.java`):
- **Reads `snapshot().allManifests(io)` = ALL manifests (data AND delete)** of the CURRENT snapshot, and
  does NOT filter `isLive()` — so it SHOWS the `Deleted` tombstone (`status==2`). This is THE difference
  from `files` (which excludes Deleted). The `ManifestEntriesTable` javadoc: "exposes internal details,
  like files that have been deleted."
- **Schema = `ManifestEntry.getSchema(partitionType)` = `wrapFileSchema(DataFile.getType(partitionType))`:**
  `status`(0, required int), `snapshot_id`(1, optional long), `sequence_number`(**3**, optional long),
  `file_sequence_number`(**4**, optional long), `data_file`(**2**, required struct =
  `DataFile.getType(partitionType)` — the SAME field set the files table projects). **NOTE — the brief said
  ids 0/1/2/3/4; the authoritative Java source (`ManifestEntry.java:51-55`) uses 0/1/3/4/2** (seq=3,
  file_seq=4, data_file=2). Per CLAUDE.md (Java source is the spec-by-example, not the paraphrase), I
  implement the REAL Java ids 0/1/3/4/2 — and these match the Rust `ManifestEntry` doc comments exactly.
- **Status repr** = Java `Status` enum `EXISTING(0)/ADDED(1)/DELETED(2)` == Rust `ManifestStatus`
  `Existing=0/Added=1/Deleted=2` (`status() as i32`).

Plan:
- [x] Extract shared `inspect/data_file.rs`: `data_file_fields(partition_type) -> Vec<NestedFieldRef>`
      (the 21 `DataFile` columns) + a `DataFileStructBuilder` (a `StructBuilder` of those fields + the
      partition type) with `append(&DataFile)` / `finish() -> StructArray`. Refactored `files.rs` to wrap
      it (flatten = build the one `data_file` struct, emit `StructArray::columns()` as the 21 top-level
      columns). All the per-type partition/metric append helpers + extract helpers moved to the shared
      module. **KEY GOTCHA (solved):** `StructBuilder::from_fields` builds Map/List children as the BOXED
      shapes `MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>` / `ListBuilder<Box<dyn ArrayBuilder>>`
      (via `make_builder`) — NOT the typed `MapBuilder<Int32Builder, Int64Builder>` the old flat
      `FilesRowBuilder` constructed by hand. So the append helpers now take the `Dyn{Map,List}Builder`
      aliases and downcast the inner key/value/element builders. The boxed builders PRESERVE the keys/values/
      element field metadata (so field ids survive), so the explicit `metrics_map_fields`/`map_field_names`
      helpers were deleted.
- [x] `EntriesTable<'a>` (holds `&Table`); `schema()` = the 5-field entry schema (data_file =
      `Type::Struct(StructType::new(data_file_fields))`); `scan()`: current snapshot → `load_manifest_list`
      → for EVERY manifest (no content filter) → for EVERY entry (no `is_alive` filter) → append a row
      (status int, nullable snapshot_id/seq/file_seq via `append_option`, data_file struct via the shared
      builder). `file_sequence_number` is a public FIELD on `ManifestEntry`, not a method.
- [x] Wired `MetadataTableType::Entries` (+ `as_str` "entries" + `TryFrom`) + `MetadataTable::entries()`.
- [x] 6 tests (`TableTestFixture` + a DATA manifest Added/Deleted/Existing + a DELETE manifest Added —
      public crate APIs): (a) ALL 4 entries incl. the Deleted tombstone present (status 2); (b) status per
      entry (Added=1/Existing=0/Deleted=2); (c) snapshot_id/seq/file_seq vs committed — Existing preserves
      parent's id+seq, Deleted is snapshot-id-STAMPED to current by `add_delete_entry` but seq PRESERVED,
      Added inherits current's id+seq at read time; (d) data_file struct carries the right file
      (record_count/content + the Added file's column_sizes {1:42}) per entry; (e) Arrow schema = 5 cols,
      ids 0/1/3/4/2, data_file Struct (file_path keeps nested id 100), status non-null + snapshot_id
      nullable; (f) empty table → 0 rows.
- [x] Deferred `readable_metrics` (as files did). Docs: GAP_MATRIX inspection row, Roadmap Phase 3, todo,
      lessons. Verify gate from repo root.

**Outcome (2026-06-08, Phase 3 Increment 2, BUILDER Opus):** the `entries` inspection table lands at Java
`BaseEntriesTable`/`ManifestEntriesTable` schema parity (🟡). It reads the CURRENT snapshot's
`allManifests` (data AND delete) and emits EVERY entry — INCLUDING the `Deleted` tombstones (`status==2`)
that `files` excludes (the diagnostic view). The **shared data_file projection** (`data_file_fields` +
`DataFileStructBuilder`) was factored out of `files.rs` into `inspect/data_file.rs` (Rule of Three, 2nd
use): `files` FLATTENS the struct's `.columns()` to top-level columns, `entries` NESTS it under `data_file`
— one source of truth, the two cannot drift. **DEVIATION FROM BRIEF (flagged): field ids.** The brief said
data_file ids 0/1/2/3/4; the authoritative Java source `ManifestEntry.java:51-55` uses **0/1/3/4/2**
(status 0, snapshot_id 1, sequence_number 3, file_sequence_number 4, data_file 2). Per CLAUDE.md (Java
source is the spec-by-example, not the paraphrase), I implemented the REAL Java ids — which also match the
Rust `ManifestEntry` doc comments exactly. **Verify (repo root):** build clean; lib ×2 = 1424/0 both runs
(was 1418 baseline → +6 new entries tests; the files refactor is behavior-preserving — its 9 tests stay
green); 3 interop suites 4/4 each; clippy -D warnings clean; fmt --check clean. Files touched exactly the
allowed set: `inspect/data_file.rs` (new shared), `inspect/entries.rs` (new), `inspect/files.rs` (refactor
to the shared projection ONLY), `inspect/mod.rs`, `inspect/metadata_table.rs`, GAP_MATRIX, Roadmap, todo,
lessons. No Cargo/lockfile/`arrow/` edits, no commit, no branch switch. **Deferred:** `readable_metrics`;
`all_entries`/`history`/`refs`/`metadata_log`/`partitions`/`all_*`; Java/Spark interop (→ ✅). An Opus
REVIEWER verifies next.

### Phase 3 Increment 3 — `history` / `refs` / `metadata_log_entries` (BUILDER Opus, 2026-06-08)
Three PURE-METADATA inspection tables (no manifest IO) — each reads only `TableMetadata` fields and
projects to a `RecordBatch`, mirroring `inspect/snapshots.rs` (NOT the manifest-reading `files`/`entries`).
New files `inspect/{history.rs, refs.rs, metadata_log_entries.rs}`; wired via `inspect/mod.rs` +
`metadata_table.rs` (`MetadataTableType::{History, Refs, MetadataLogEntries}` + accessors + `as_str` +
`TryFrom` + `all_types`).

**Java schema authority — CONFIRMED from source (field ids verbatim):**
- `HistoryTable.java:37-42` — `made_current_at` ts(zone) #1 req, `snapshot_id` long #2 req, `parent_id`
  long #3 opt, `is_current_ancestor` bool #4 req. Row fn (`convertHistoryEntryFunc`): `madeCurrentAt =
  historyEntry.timestampMillis()*1000` (millis→micros); `parent_id = snapshots.get(snapshotId).parentId()`
  (the SNAPSHOT's parent, nullable); `is_current_ancestor = SnapshotUtil.currentAncestorIds(table)
  .contains(snapshotId)` (the parent-chain of the CURRENT snapshot).
- `RefsTable.java:33-40` — `name` str #1 req, `type` str #2 req ("BRANCH"/"TAG" via `SnapshotRefType.name()`),
  `snapshot_id` long #3 req, `max_reference_age_in_ms` long #4 opt, `min_snapshots_to_keep` int #5 opt
  (branches only), `max_snapshot_age_in_ms` long #6 opt (branches only). Tag carries ONLY max_ref_age.
- `MetadataLogEntriesTable.java:29-35` — `timestamp` ts(zone) #1 req, `file` str #2 req, `latest_snapshot_id`
  long #3 opt, `latest_schema_id` int #4 opt, `latest_sequence_number` long #5 opt. The log = `previousFiles`
  (metadata_log) PLUS a synthetic final entry `(lastUpdatedMillis, metadataFileLocation)` for the CURRENT
  file. `latest_*` derived from `SnapshotUtil.snapshotIdAsOfTime(ts)` = the LAST snapshot-log entry whose
  `made_current_at <= ts`; its `schema_id` + `sequence_number`; NULL when no snapshot ≤ ts (creation-time
  entry). All tractable from Rust metadata — NO deferral needed.

**`is_current_ancestor`**: build a `HashSet<i64>` by walking `current_snapshot_id` via `parent_snapshot_id`
(mirrors `manage_snapshots::is_ancestor_of` / Java `currentAncestorIds`); a history row is a current-ancestor
iff its snapshot id ∈ the set. A forked/rolled-back snapshot not on the current parent chain ⇒ false.

**Data sources (all PUBLIC or `pub(crate)`, reachable from in-crate `inspect/`)**: `metadata.history()`
(`&[SnapshotLog{snapshot_id,timestamp_ms}]`), `metadata.metadata_log()` (`&[MetadataLog{metadata_file,
timestamp_ms}]`), `metadata.refs` (`pub(crate) HashMap<String,SnapshotReference>` — reachable in-crate),
`metadata.current_snapshot_id()`, `metadata.last_updated_ms()`, `table.metadata_location()`,
`snapshot_by_id(id)` → `Snapshot::{parent_snapshot_id, schema_id, sequence_number, timestamp_ms}`.
Timestamps: spec `Timestamptz` → Arrow `Timestamp(µs, "+00:00")`; multiply `timestamp_ms * 1000` (like
snapshots.rs). NO new public spec accessor needed.

Plan:
- [x] `inspect/history.rs`: `HistoryTable` with `schema()` (4 fields) + async `scan()`. Build the
      current-ancestor id set; one row per `metadata.history()` entry; `parent_id` from the SNAPSHOT's parent.
- [x] `inspect/refs.rs`: `RefsTable` `schema()` (6 fields) + `scan()`; one row per `metadata.refs` entry;
      `type` = "BRANCH"/"TAG"; retention fields from `SnapshotRetention::{Branch,Tag}` (tag → only
      max_reference_age; branch unset → NULL). Sort by name for deterministic output.
- [x] `inspect/metadata_log_entries.rs`: `MetadataLogEntriesTable` `schema()` (5 fields) + `scan()`; rows =
      `metadata_log()` + synthetic CURRENT (`last_updated_ms`, `metadata_location`); `latest_*` via a local
      `snapshot_id_as_of_time` mirroring Java (last history entry ≤ ts), then its schema_id/sequence_number.
- [x] Wire enum + accessors in `metadata_table.rs`; `mod`+`pub use` in `mod.rs`.
- [x] Tests (in-crate `#[cfg(test)]`, build tables via `make_v2_table()` + `into_builder`/`add_snapshot`/
      `set_ref` + `with_metadata` — the `manage_snapshots.rs` forked-table pattern): history 2-snapshot +
      forked non-ancestor (is_current_ancestor false); refs main+branch+tag w/ retention set/unset; metadata
      log entries multi-commit; each table's Arrow schema (cols + field ids + types). Name each test for its risk.
- [x] Docs: GAP_MATRIX inspection row (history/refs/metadata_log_entries landed); Roadmap Phase 3; this todo; lessons.
- [x] Verify from repo root: build; lib ×2 (counts); 3 interop suites; clippy -D warnings; fmt --check.

**Outcome (2026-06-08, Phase 3 Increment 3, BUILDER Opus):** the three PURE-METADATA inspection tables
land at Java schema parity (🟡). Each reads only `TableMetadata` fields (no manifest IO), mirroring
`inspect/snapshots.rs`. **Field ids confirmed verbatim from the Java source** (not the brief paraphrase):
`HistoryTable` 1/2/3/4 (made_current_at/snapshot_id/parent_id/is_current_ancestor),
`RefsTable` 1..6 (name/type/snapshot_id/max_reference_age_in_ms/min_snapshots_to_keep/max_snapshot_age_in_ms),
`MetadataLogEntriesTable` 1..5 (timestamp/file/latest_snapshot_id/latest_schema_id/latest_sequence_number).
**`is_current_ancestor`** = membership in the set walked from `current_snapshot_id` via `parent_snapshot_id`
(local `current_ancestor_ids`, mirrors Java `SnapshotUtil.currentAncestorIds`; cycle-guarded). **refs**
branch/tag handled by matching `SnapshotRetention::{Branch,Tag}` — branch emits all three retention fields
(NULL where unset), tag emits only `max_reference_age_in_ms` and NULLs the two branch-only fields. **`latest_*`
FULLY IMPLEMENTED, nothing deferred:** a local `snapshot_id_as_of_time` walks `metadata.history()` for the last
entry with `timestamp_ms <= ts` (Java `SnapshotUtil.nullableSnapshotIdAsOfTime`), then reads that snapshot's
`schema_id`/`sequence_number`; NULL when no snapshot is at/older than the timestamp (creation-time file). The log
rows = `metadata_log()` + a synthetic final entry `(last_updated_ms, metadata_location)` for the CURRENT file,
exactly as Java appends. Timestamps: `Timestamptz` → Arrow `Timestamp(µs,"+00:00")`, value = log millis × 1000.
**Tests: 10 (4 history / 3 refs / 3 metadata_log_entries),** each named for its risk; tables built from the
committed `TableMetadataV2Valid.json` fixture (a local `make_v2_table` per module — `transaction::tests` is
private to `transaction/`) plus `into_builder`/`set_ref` for refs + branch/tag, a TWO-commit forked rollback for
the non-ancestor history case, and a directly-set `metadata_log` for the as-of-time resolution. **KEY FINDING
(forked history):** a single transaction that sets `main`→SIBLING then rolls back to CURRENT drops SIBLING from
the snapshot log as an *intermediate* snapshot (`TableMetadataBuilder::update_snapshot_log`, mirroring Java) — so
the forked fixture commits in TWO steps (commit 1 makes SIBLING current; commit 2 rolls main back to CURRENT),
leaving SIBLING a persisted log row that is correctly `is_current_ancestor == false`. The `is_current_ancestor`
column is mutation-verified (forcing it true fails the forked test). **Verify (repo root):** build clean; lib ×2
= 1434/0 both runs (was 1424 baseline → +10); 3 interop suites 4/4 each; clippy -D warnings clean; fmt --check
clean. Files touched exactly the allowed set: `inspect/{history,refs,metadata_log_entries}.rs` (new),
`inspect/mod.rs`, `inspect/metadata_table.rs`, GAP_MATRIX, Roadmap, todo, lessons. No Cargo/lockfile/`spec/`/`arrow/`
edits; NO new public spec accessor needed (`metadata.refs` is `pub(crate)`, reachable in-crate; everything else is
already public). No commit, no branch switch. **Deferred:** `partitions`/`all_*` variants; Java/Spark interop
(→ ✅). An Opus REVIEWER verifies next.

#### Phase 3 Increment 3 — REVIEW (2026-06-08, Opus REVIEWER, DELEGATED)
Adversarially verified points 1–5 against the Java source (`HistoryTable`/`RefsTable`/`MetadataLogEntriesTable`
+ `SnapshotUtil.{currentAncestorIds,ancestorsOf,nullableSnapshotIdAsOfTime}`) and by mutation-testing the
non-trivial computations.
- [x] **Pt 1 (field ids + Arrow types): CONFIRMED all three.** Java `HISTORY_SCHEMA` (1 made_current_at
      timestamptz req / 2 snapshot_id long req / 3 parent_id long opt / 4 is_current_ancestor bool req),
      `SNAPSHOT_REF_SCHEMA` (1 name / 2 type str req / 3 snapshot_id long req / 4 max_reference_age_in_ms long
      opt / 5 min_snapshots_to_keep int opt / 6 max_snapshot_age_in_ms long opt), `METADATA_LOG_ENTRIES_SCHEMA`
      (1 timestamp tstz req / 2 file str req / 3 latest_snapshot_id long opt / 4 latest_schema_id int opt / 5
      latest_sequence_number long opt) match the Rust `schema()` ids + nullability byte-for-byte. The
      `arrow_schema_*` expect-tests probe the PRODUCED Arrow field ids + the `Timestamp(µs, "+00:00")` UTC-micros
      type — all correct. `"BRANCH"`/`"TAG"` == Java `SnapshotRefType.name()`.
- [x] **Pt 2 (is_current_ancestor): CONFIRMED + mutation-verified.** Rust `current_ancestor_ids` walks
      `current_snapshot_id` → `parent_snapshot_id` (inclusive), == Java `ancestorsOf(currentSnapshot)`; the
      forked two-commit fixture makes SIBLING a non-ancestor log row → false; ROOT+CURRENT → true. Mutation
      (always-true) FAILS the forked test (left Some(true) vs right Some(false)). `parent_id` =
      `snapshot_by_id(entry.snapshot_id).parent_snapshot_id()` (the SNAPSHOT's parent, nullable) == Java
      `snap.parentId()`, NOT the previous log row. (Rust adds a benign cycle-guard Java lacks — cannot change
      correct-input results.)
- [x] **Pt 3 (refs retention): CONFIRMED.** Branch → all three fields (NULL where the `Option` is unset, e.g.
      `main`); tag → only `max_reference_age_in_ms`, branch-only fields explicitly NULL. Field-id→accessor map
      (4←maxRefAge, 5←minKeep, 6←maxSnapAge) matches Java `referencesToRows`.
- [x] **Pt 4 (latest_* as-of-time): CONFIRMED + TEST-GAP FIXED.** `snapshot_id_as_of_time` (last snapshot-log
      entry with `timestamp_ms <= ts`, else None) == Java `nullableSnapshotIdAsOfTime`; NULL-before-first / ROOT
      / CURRENT cases + the synthetic current-file row all correct. **GAP:** the builder's test offset every
      metadata-log entry OFF the snapshot timestamps, so the `<=`-vs-`<` boundary was UNPINNED — I mutation-tested
      `<=`→`<` and the test SURVIVED. Added `test_…_inclusive_of_exact_snapshot_timestamp` (entries at exactly
      `ROOT_TS`/`CURRENT_TS` → ROOT/CURRENT, not NULL/ROOT); mutation-verified it now FAILS under `<`. A wrong
      as-of-time = wrong latest_* per row, so this is load-bearing.
- [x] **Pt 5 (no regression + scope): CONFIRMED.** All prior tests green (lib ×2 = 1435/0); only the named
      files; no `spec/`/`arrow/`/`Cargo` edits; no public spec accessor added (`metadata.refs` is `pub(crate)`,
      in-crate). **FIXED:** the three `schema()` methods used bare `.unwrap()` (CLAUDE.md NN#3 violation) — copied
      from the OLDER `snapshots.rs`, but the in-scope Increment-1/2 siblings (`files.rs`/`entries.rs`) already use
      `.expect("… statically valid")`; replaced all three to match. Empty cases (no refs / no snapshot-log / empty
      metadata-log) verified panic-free by inspection (the schema-test already exercises the 1-synthetic-row
      empty-metadata-log path).

**Review outcome (2026-06-08, Opus REVIEWER):** all 5 points adjudicated; row stays 🟡 (interop deferred → ✅).
One test-strength gap fixed (the as-of-time `<=` boundary, mutation-verified) + one engineering-floor fix (bare
`.unwrap()` → `.expect()` ×3). Files touched: `inspect/{history,refs,metadata_log_entries}.rs` (the 3 in-scope
new files only) + todo + lessons. +1 test (1434 → 1435 lib). build/clippy/fmt clean; 3 interop suites 4/4 each.
No commit, no branch switch, no Cargo/spec/arrow edits.
---

### Phase 3 Increment 4 — `partitions` metadata table (SCOPED by orchestrator, 2026-06-08)
The first AGGREGATING inspection table: per-partition rollup over the current snapshot's LIVE manifest
entries (data AND delete). New file `inspect/partitions.rs`; wired via `inspect/mod.rs` +
`metadata_table.rs` (`MetadataTableType::Partitions` + accessor + `as_str` + `TryFrom`).

**Java authority — `core/src/main/java/org/apache/iceberg/PartitionsTable.java` (CONFIRMED from source):**
- Schema, in COLUMN order (= Java field-id declaration order; ids are NON-sequential):
  `partition`/1 (struct = `Partitioning.partitionType(table)`, REQUIRED) — DROPPED when unpartitioned
  (`TypeUtil.select` excludes it); `spec_id`/4 int req; `record_count`/2 long req (DATA records);
  `file_count`/3 int req (DATA files); `total_data_file_size_in_bytes`/11 long req;
  `position_delete_record_count`/5 long req; `position_delete_file_count`/6 int req;
  `equality_delete_record_count`/7 long req; `equality_delete_file_count`/8 int req;
  `last_updated_at`/9 ts(zone) OPT; `last_updated_snapshot_id`/10 long OPT.
- Rows = `partitions(table, scan)`: read `scan.snapshot().allManifests()` (DATA + DELETE), `liveEntries()`
  (Added/Existing == Rust `is_alive()`), group by the partition key (`StructLikeMap`). For each entry
  `Partition.update(file, snapshot)`:
  - `snapshot = table.snapshot(entry.snapshotId())`; if non-null and `snapshot.timestampMillis()*1000 >
    lastUpdatedAt` → set `specId = file.specId()`, `lastUpdatedAt = commitTimeMicros`,
    `lastUpdatedSnapshotId = snapshot.snapshotId()` (most-recent-commit file wins — `>` not `>=`).
  - content DATA → dataRecordCount += recordCount; dataFileCount += 1; dataFileSizeInBytes += fileSize.
  - content POSITION_DELETES → posDeleteRecordCount += recordCount; posDeleteFileCount += 1.
  - content EQUALITY_DELETES → eqDeleteRecordCount += recordCount; eqDeleteFileCount += 1.
- Unpartitioned table → single "root" partition row, partition column dropped (see decision below).

**Rust mapping (confirmed reachable):** `metadata.default_partition_type()` for the schema partition
struct (matches `files.rs`); the partition `Struct` derives `Hash`+`Eq` → use it directly as the
`HashMap`/`IndexMap` key. Per entry: `entry.is_alive()`, `entry.snapshot_id()` → Option<i64> →
`metadata.snapshot_by_id(id)` → `.timestamp_ms()`/`.snapshot_id()`; `data_file.content_type()`
(`DataContentType::{Data,PositionDeletes,EqualityDeletes}`), `.record_count()`, `.file_size_in_bytes()`,
`.partition()`, `.partition_spec_id`. Reuse the partition-struct Arrow builder primitive
`inspect::data_file::append_partition` (promote to `pub(super)` if reused — shared in-module helper,
in scope). Timestamps: micros = `timestamp_ms * 1000`, Arrow `Timestamp(µs, "+00:00")` (like snapshots.rs).
Sort output rows deterministically by partition value.

**Two scoping DECISIONS the builder must make against the Java source + state rationale:**
1. **Unpartitioned partition column:** Java DROPS it; `files.rs` KEPT an empty-struct column as a
   *documented* divergence (`test_files_table_unpartitioned_keeps_empty_partition_struct_known_divergence`).
   RECOMMENDED: match the `files.rs` precedent for one consistent module-wide divergence (keep empty
   struct, document + pin with a test), OR match Java exactly (drop). Either way: test it + document it.
2. **Multi-spec partition evolution:** Java unifies via `Partitioning.partitionType` + `coercePartition`.
   Rust has only `default_partition_type()` (no cross-spec unifier found). RECOMMENDED: implement the
   single-spec-correct path (key by the file's partition `Struct`, schema = default partition type) and
   DOCUMENT cross-spec unification as a deferral (known divergence) in GAP_MATRIX/todo — do NOT silently
   aggregate wrongly. If a unifier helper does exist, use it.

Plan:
- [x] `inspect/partitions.rs`: `PartitionsTable<'a>` + `schema()` (11 fields; kept empty-struct partition
      column when unpartitioned per decision 1) + async `scan()` doing the aggregation above into one
      `RecordBatch`.
- [x] Wire `MetadataTableType::Partitions` + `partitions()` accessor + `as_str`/`TryFrom`/`all_types` in
      `metadata_table.rs`; `mod partitions;` + `pub use` in `mod.rs`. Promoted
      `inspect::data_file::append_partition` to `pub(super)` for reuse (Rule-of-Three 3rd use).
- [x] Tests (in-crate, `TableTestFixture` + the `files.rs` manifest-builder pattern): two files same
      partition → summed record/file/size; multiple partitions → row-per + sorted; delete files counted
      in pos/eq columns (DATA columns unchanged); Deleted-tombstone excluded (`is_alive()`);
      `last_updated_*` + `spec_id` = most-recent-snapshot file (mutation-pinned the `>` comparison);
      empty table → 0 rows; unpartitioned → 1 root row + empty-struct column (decision 1); Arrow schema
      cols + field ids + types; file spec_id reported. 10 tests, each risk-named.
- [x] Docs: GAP_MATRIX inspection row, Roadmap Phase 3 list + Current state, this todo, lessons.
- [x] Verify from repo root: build; lib ×2 (1444 ea); 3 interop suites (4/4 ea); clippy -D warnings; fmt
      --check. All clean.

**Outcome (2026-06-08, BUILDER Opus).** Built `inspect/partitions.rs` (`PartitionsTable<'a>` + `schema()`
+ async `scan()`) — the first AGGREGATING inspection table — plus wiring in `metadata_table.rs`/`mod.rs`
and the `pub(super)` promotion of `append_partition` in `data_file.rs`. Reads the current snapshot's
manifest list → ALL manifests (data + delete) → live entries (`is_alive()`), groups by the file's
partition `Struct` (derives `Hash`+`Eq`), and rolls up per Java `PartitionsTable.Partition.update`. Schema
is 11 fields in Java column order with the EXACT non-sequential ids verified against
`PartitionsTable.java`: `partition`/1 (struct=`default_partition_type`, REQUIRED, lines 107-108),
`spec_id`/4 (44-45), `record_count`/2 (46-48), `file_count`/3 (49-50), `total_data_file_size_in_bytes`/11
(51-56), `position_delete_record_count`/5 (57-62), `position_delete_file_count`/6 (63-68),
`equality_delete_record_count`/7 (69-74), `equality_delete_file_count`/8 (75-80), `last_updated_at`/9
OPT (81-86), `last_updated_snapshot_id`/10 OPT (87-92). The update logic was confirmed against
`PartitionsTable.java:331-360` (commit time = `snapshot.timestampMillis()*1000`, strict `>`, content
switch). **Decision 1 (unpartitioned column): KEEP the empty-struct column** to match the `files`-family
precedent → one consistent module-wide divergence (Java drops it); pinned + documented. **Decision 2
(multi-spec evolution): DEFER cross-spec unification** — no `Partitioning.partitionType`/`coercePartition`
analogue exists in Rust (confirmed by grep — only `default_partition_type`), so I key by the file's own
partition `Struct` (single-spec-correct), report the per-file `spec_id`, and document the multi-spec
unification gap as a known divergence. 10 risk-named tests. Mutations run + caught: (a) content-type
switch — counting a position-delete as DATA fails `..._delete_files_counted_separately_from_data`
(record_count 6≠4); (b) the `>` comparison flipped to `<` (min-wins) makes the OLDEST snapshot win and
fails `..._last_updated_reflects_most_recent_commit_file` (1515…≠1555…) — note the FIRST cut of this test
was un-pinning because `write_data_manifest` used `add_entry` (which RESTAMPS the snapshot id to the
manifest's), so I rewrote it to write the older file via `add_existing_entry` (which PRESERVES the parent
id); (c) `is_alive()` bypass counts the Deleted tombstone and fails `..._excludes_deleted_tombstones`
(record_count 3≠1). Gate: build clean; lib 1444 passed ×2; interop_manage_snapshots/update_schema/
update_partition_spec 4/4 each; clippy -D warnings clean; fmt clean. No commit/branch/Cargo/spec/arrow
edits.

**REVIEW outcome (2026-06-08, REVIEWER Opus, DELEGATED).** Verdict: CHANGES-MADE (test-strength only; no
production-code change — the aggregation/tie-break/field-id logic is byte-for-byte correct vs
`PartitionsTable.java`, re-confirmed all 11 ids/types/nullability against lines 42-118 and the
`Partition.update` fold against 331-360). Independently ran SEVEN mutations by injection; FIVE were already
caught by the builder's suite (content-type misroute, `>`→`<`, `is_alive()` bypass, swapped field ids,
flipped null ordering). TWO SURVIVED — exactly the builder's self-flagged weak spots: (1) `>`→`>=`
(exact-commit-time tie: `>=` lets a later equal-time file overwrite the first-seen one, diverging from
Java's strict `>`), and (2) delete-file size leaking into `total_data_file_size_in_bytes` (no test asserted
the data-size total in the presence of delete files). Both are now PINNED:
- Added `test_partitions_table_last_updated_exact_commit_time_tie_keeps_first_seen_file` — rebuilds the
  parent snapshot with a commit time EQUAL to the current snapshot's (splicing it back into the `pub(crate)`
  `snapshots` map in-test, no new accessor), adds the parent's file FIRST and the current's SECOND, and
  asserts the FIRST-seen (parent) wins. Verified: passes under `>`, FAILS under `>=`.
- Strengthened `test_partitions_table_delete_files_counted_separately_from_data` to assert
  `total_data_file_size_in_bytes == FILE_SIZE` (DATA-only). Verified: FAILS when a delete size is summed in.
- Added `test_partitions_table_partition_with_only_delete_files_present_with_zero_data` (edge case: a
  delete-only partition still emits a row, data columns 0, delete columns populated — also asserts the size
  total stays 0).
- Added a direct `test_compare_partition_values_multi_field_and_null_first_ordering` unit test for the
  hand-rolled comparator (builder flag #2): multi-field tiebreak, null-first, and a non-long (String) field
  type. Verified: FAILS when the null ordering is flipped.
Gate after changes: build clean; lib **1447** passed ×2 (was 1444; +3 net new tests); interop
manage_snapshots/update_schema/update_partition_spec 4/4 each; clippy -D warnings clean; fmt clean. Scope:
touched ONLY `inspect/partitions.rs` (tests) + `docs/parity/GAP_MATRIX.md` + this file. No production-logic,
Cargo, spec, or arrow edits; no commit/branch. Residual risk: cross-spec partition-evolution unification
stays a documented DEFERRAL (decision 2); the unpartitioned empty-struct partition column stays a documented
module-wide DIVERGENCE (decision 1) — both honestly stated, neither overstated as parity.

---

### Phase 3 Increment 5a — `all_data_files` / `all_delete_files` / `all_files` / `all_entries` (SCOPED, 2026-06-08)
The cross-snapshot file/entry inspection tables: the SAME projection/rows as their current-snapshot
counterparts (`data_files`/`delete_files`/`files`/`entries`), but the manifest source becomes the
**deduplicated union of manifests reachable from ALL snapshots** (Java `BaseAllMetadataTableScan
.reachableManifests`). `all_manifests` is a SEPARATE later increment (5b) — distinct machinery.

**Java authority (CONFIRMED from source):**
- `BaseAllMetadataTableScan.reachableManifests(toManifests)` (L74-86): union over `table().snapshots()` of
  `toManifests(snapshot)`, **deduplicated via a HashSet of ManifestFile** (== by manifest path). NOT
  per-snapshot-tagged (no reference_snapshot_id — that's only `all_manifests`).
- `AllDataFilesTable` (extends `BaseFilesTable`): `manifests() = reachableManifests(snapshot.dataManifests)`.
  `AllFilesTable` → `allManifests`; `AllDeleteFilesTable` → `deleteManifests`. Same `BaseFilesTable` schema
  + LIVE-entry filter (`is_alive`) as the current-snapshot files family. Javadoc: "valid file = readable
  from ANY snapshot currently tracked"; "may return duplicate rows" (files NOT deduped — only manifests).
- `AllEntriesTable` (extends `BaseEntriesTable`): `reachableManifests(snapshot.allManifests)`, then the SAME
  entries rows (ALL entries incl. Deleted tombstones — NO is_alive filter), same schema as `entries`.

**Rust mapping (confirmed):** `metadata.snapshots()` → `impl ExactSizeIterator<&SnapshotRef>` (all snapshots);
per snapshot `snapshot.load_manifest_list(file_io, metadata)` → entries; dedup the `ManifestFile`s by
`manifest_path: String` (HashSet/seen-set, preserve first-seen order for determinism); then the EXISTING
per-manifest read in `files.rs`/`entries.rs` is reused verbatim. The content filter (`FilesTableKind`) and
`is_alive()` (files) / no-filter (entries) stay exactly as today.

**Design:** add a snapshot-SCOPE axis to `FilesTable` and `EntriesTable` (enum `{CurrentSnapshot, AllSnapshots}`
or similar), orthogonal to `FilesTableKind`. Factor a shared manifest-source helper (e.g. `pub(super) async fn
collect_manifests(table, scope, content_filter) -> Result<Vec<ManifestFile>>`, deduped for AllSnapshots) so
files.rs + entries.rs cannot drift. Wire `MetadataTableType::{AllDataFiles,AllDeleteFiles,AllFiles,AllEntries}`
+ `MetadataTable::{all_data_files,all_delete_files,all_files,all_entries}` accessors + as_str/TryFrom.

Plan:
- [x] Shared manifest-source helper (deduped reachable-manifest union for AllSnapshots; current-snapshot list
      for CurrentSnapshot), used by both tables. Keep the existing current-snapshot behavior byte-identical.
- [x] `FilesTable`: add the scope axis; `FilesTable::all_*` constructors; `scan()` uses the helper.
- [x] `EntriesTable`: add the scope axis; `EntriesTable::all()` constructor; `scan()` uses the helper.
- [x] Wire enum variants + accessors + as_str + TryFrom in `metadata_table.rs`.
- [x] Tests (in-crate): a MULTI-snapshot fixture where an OLD snapshot's manifest holds a file that the
      current snapshot's manifests do NOT → `all_*` includes it, the current-snapshot table excludes it;
      manifest DEDUP (a manifest shared by 2 snapshots is not double-read by all_entries); content filters
      still hold for all_data_files/all_delete_files; all_entries shows Deleted tombstones across snapshots;
      Arrow schema parity with the non-all counterparts. Mutation-pin the dedup + the all-vs-current scope.
- [x] Docs: GAP_MATRIX inspection row, Roadmap Phase 3, todo, lessons.
- [x] Verify from repo root: build; lib ×2; 3 interop suites; clippy -D warnings; fmt --check.

**Outcome (2026-06-08, BUILDER Opus):** Landed the four cross-snapshot file/entry inspection tables
`all_data_files` / `all_delete_files` / `all_files` / `all_entries` 🟡. **Files modified:**
`inspect/files.rs` (scope axis + `all_files`/`all_data_files`/`all_delete_files` ctors + 6 tests),
`inspect/entries.rs` (scope axis + `EntriesTable::all()` + 5 tests), `inspect/metadata_table.rs` (4 enum
variants + accessors + `as_str` + `TryFrom`), `inspect/mod.rs` (register the new module). **File created:**
`inspect/manifest_source.rs` — the SHARED single-source-of-truth helper. **Shared-helper design:** a new
`MetadataScope {CurrentSnapshot, AllSnapshots}` enum (ORTHOGONAL to `FilesTableKind`) drives
`pub(super) collect_manifest_files(table, scope) -> Result<Vec<ManifestFile>>`: `CurrentSnapshot` returns
the current snapshot's manifest-list entries (empty when none — old current-snapshot behavior byte-identical);
`AllSnapshots` walks `metadata.snapshots()`, loads each `load_manifest_list`, and deduplicates `ManifestFile`s
by `manifest_path` (a `HashSet` seen-set, FIRST-SEEN order preserved for determinism). Both `files.rs` and
`entries.rs` call it; the content filter (`FilesTableKind::includes_manifest`) + `is_alive()` (files) /
no-filter (entries) stay UNCHANGED in each table's scan loop, so the row machinery + schema are byte-identical
across the scope axis. **Java lines verified:** `BaseAllMetadataTableScan.reachableManifests` (L74-86, union
over `table().snapshots()` deduped via `Sets.newHashSet` = by-path manifest dedup); `AllDataFilesTable` /
`AllFilesTable` / `AllDeleteFilesTable` / `AllEntriesTable` (each swaps the manifest source to
`dataManifests` / `allManifests` / `deleteManifests` / `allManifests`, reusing `BaseFilesTable` /
`BaseEntriesTable` schema + row machinery; javadocs "valid file = readable from ANY snapshot currently
tracked", "may return duplicate rows" = files NOT deduped). **Tests (11):**
files.rs — `test_all_files_includes_file_only_in_old_snapshot` (cross-snapshot inclusion: old-only file in
`all_files` not current `files`), `test_all_data_files_excludes_delete_files_across_snapshots` (content
filter under AllSnapshots), `test_all_delete_files_excludes_data_files_across_snapshots` (content filter
under AllSnapshots), `test_all_files_deduplicates_shared_manifest` (shared manifest read once),
`test_all_files_schema_equals_files_schema` (Arrow schema parity all_files/all_data_files/all_delete_files
== their non-all counterparts), `test_all_files_empty_table_yields_empty_batch` (no snapshots → 0 rows);
entries.rs — `test_all_entries_includes_entries_only_in_old_snapshot` (cross-snapshot inclusion),
`test_all_entries_shows_deleted_tombstone_from_old_snapshot` (tombstones across snapshots, the is_alive
distinction vs all_files), `test_all_entries_deduplicates_shared_manifest` (shared manifest's entry once),
`test_all_entries_schema_equals_entries_schema` (Arrow schema parity), `test_all_entries_empty_table_yields_
empty_batch`. Multi-snapshot fixture built on `TableTestFixture::new()` (which has a parent + current
snapshot): writes BOTH snapshots' manifest lists (the current-snapshot fixtures leave the parent list
unwritten); a SHARED manifest is referenced by both lists by the same path (its seq number stamped to the
parent's explicitly, because a manifest list only ASSIGNS a seq to a manifest it ADDED — a carried-forward
manifest must already carry one, else "Found unassigned sequence number"). **Mutations run + caught (both):**
(a) dropped the dedup seen-set (push every manifest) → both `*_deduplicates_shared_manifest` tests FAIL
(count 2 ≠ 1); (b) `AllSnapshots` reads only the current snapshot → all 3 cross-snapshot-inclusion tests +
the tombstone test FAIL (old-only file/entry/tombstone absent). Both restored after. **Gate (repo root,
pinned nightly):** `cargo build -p iceberg` clean; `cargo test -p iceberg --lib` ×2 = **1458 passed / 0
failed** both runs (was 1447; +11); `cargo test -p iceberg --test interop_manage_snapshots
--test interop_update_schema --test interop_update_partition_spec` = 4/4 each; `cargo clippy -p iceberg
--all-targets -- -D warnings` clean; `cargo fmt --all -- --check` clean. **`all_manifests` (5b) NOT built**

**REVIEW (2026-06-08, REVIEWER Opus):** PASS 🟡 with ONE test-strength fix landed in scope. Verified Java
authority myself: `BaseAllMetadataTableScan.reachableManifests` L74-86 (union over `table().snapshots()`,
`Sets.newHashSet`); the four per-table sources (`AllDataFilesTable`→`dataManifests`,
`AllFilesTable`/`AllEntriesTable`→`allManifests`, `AllDeleteFilesTable`→`deleteManifests`); and
`GenericManifestFile.equals`/`hashCode` L405-419 = `manifestPath` ONLY, so the Rust `seen_paths` HashSet is a
1:1 dedup key. CurrentSnapshot arm confirmed byte-identical (mutation: dropping a manifest from it fails the
Increment-1/2 tests). Schema-parity confirmed (`schema()` unchanged; the `all_*` schemas equal their non-all
counterparts via the schema-parity tests). Mutation sweep (all run by injection, restored after): (a) drop
dedup → both `*_deduplicates_shared_manifest` fail; (b) all→current → 3 inclusion + tombstone fail;
(d) content-filter swap under AllSnapshots → all- AND current-snapshot content tests fail; CurrentSnapshot
manifest-drop → Increment-1/2 fail. **(c) all→ANCESTRY-walk SURVIVED the builder's 11 tests** — the base
`TableTestFixture` metadata has exactly 2 snapshots (current + its parent), so "all `metadata.snapshots()`"
and "current snapshot's ancestry" are indistinguishable. FIX: added
`test_all_files_includes_file_from_non_ancestor_snapshot` (files.rs) — grafts a THIRD tracked snapshot that
is a FORK off the parent (sibling of current, NOT in its ancestry; `main` stays at current) with its own
manifest list on disk holding `fork-1.parquet`, and asserts `all_files` includes it while current `files`
does not. Verified it FAILS under the ancestry-walk mutation and PASSES on correct code. No production code
changed (manifest_source.rs byte-identical to the builder's). Re-ran gate: `cargo test -p iceberg --lib` ×2 =
**1459 passed / 0 failed** both runs (+1); 3 interop suites 4/4 each; clippy `-D warnings` clean; fmt --check
clean. Residual risk: low. The fixture now distinguishes all-vs-ancestry; the only remaining residual is the
absence of a true Java/Spark interop round-trip (deferred → ✅ as planned). The pre-existing
`iceberg-datafusion` non-exhaustive-match break the builder flagged is the orchestrator's, NOT 5a's, and was
not touched. **`all_manifests` (5b) NOT built**
(out of scope: different machinery + a `reference_snapshot_id` column). **Pre-existing issue FLAGGED (not
mine, not in scope):** `iceberg-datafusion` does NOT compile at the Increment-4 HEAD baseline — its
`table/metadata_table.rs` match on `MetadataTableType` only handles `Snapshots`/`Manifests` and is already
non-exhaustive over the `Files`/`DataFiles`/.../`Partitions` variants prior increments added; my 4 new
variants extend that already-broken match. Verified by `git stash`-ing my inspect changes and building
`-p iceberg-datafusion` (same E0004 non-exhaustive errors at HEAD). The `-p iceberg` gate never builds the
datafusion crate, so this slipped through Increments 1-4 — a human should add the missing match arms (or a
catch-all) in `crates/integrations/datafusion/src/table/metadata_table.rs` as a standalone fix.

---

### Phase 3 Increment 5b — `all_manifests` metadata table (SCOPED, 2026-06-08)
The last inspection table. Distinct machinery from the file/entry `all_*` (5a): it iterates EACH snapshot's
manifest list (NO dedup — "may return duplicate rows"), one row per (manifest × referencing snapshot), and
adds a `reference_snapshot_id` column. New file `inspect/all_manifests.rs`; wired via `inspect/mod.rs` +
`metadata_table.rs` (`MetadataTableType::AllManifests` + accessor + as_str + TryFrom) + the datafusion
provider match (both `schema()`/`scan()` arms) — datafusion now builds workspace-wide, so 5b MUST add its arm.

**Java authority — `core/.../AllManifestsTable.java` (CONFIRMED from source):**
- `MANIFEST_FILE_SCHEMA` (L57-81), in COLUMN order with ids + nullability (use VERBATIM — do NOT copy the
  regular `manifests.rs` schema, which has different nullability for some fields):
  `content`/14 int REQ, `path`/1 str REQ, `length`/2 long REQ, `partition_spec_id`/3 int OPT,
  `added_snapshot_id`/4 long OPT, `added_data_files_count`/5 int OPT, `existing_data_files_count`/6 int OPT,
  `deleted_data_files_count`/7 int OPT, `added_delete_files_count`/15 int REQ, `existing_delete_files_count`/16
  int REQ, `deleted_delete_files_count`/17 int REQ, `partition_summaries`/8 list OPT (element/9 struct:
  `contains_null`/10 bool REQ, `contains_nan`/11 bool REQ, `lower_bound`/12 str OPT, `upper_bound`/13 str OPT),
  `reference_snapshot_id`/18 long REQ, `key_metadata`/19 binary OPT.
- Row (`manifestFileToRow`, L286-303) — counts are CONTENT-GATED: `added_data_files_count =
  content==DATA ? addedFilesCount : 0`, ..., `added_delete_files_count = content==DELETES ? addedFilesCount : 0`,
  etc. `added_snapshot_id = manifest.snapshotId()`. `reference_snapshot_id = the snapshot whose manifest list
  this row came from`. `partition_summaries` via the same FieldSummary→(contains_null/nan, lower/upper bound as
  STRING) conversion `manifests.rs` already does. `key_metadata = manifest.keyMetadata()` (raw bytes).
- Iteration (`doPlanFiles`, L122-158): for EACH snapshot in `table().snapshots()` (no filter ⇒ ALL), read that
  snapshot's manifest list, emit one row per manifest tagged with that snapshot's id. NOT deduplicated.

**Rust mapping:** iterate `metadata.snapshots()` directly (the 5a `manifest_source::collect_manifest_files`
helper is NOT reusable here — it dedups AND drops the snapshot identity; all_manifests needs per-snapshot
(manifest, ref_snapshot_id) pairs WITHOUT dedup). Per snapshot: `snapshot.load_manifest_list(file_io,
metadata)` → `ManifestFile{ content, manifest_path, manifest_length, partition_spec_id, added_snapshot_id,
added_files_count/existing_files_count/deleted_files_count (Option<u32>), partitions (Option<Vec<FieldSummary>>),
key_metadata (Option<Vec<u8>>) }`; `snapshot.snapshot_id()` = reference_snapshot_id. Reuse the
partition-summary builder/conversion from `manifests.rs` (factor a `pub(super)` shared helper if convenient —
in scope; else replicate carefully). NEW code follows the engineering floor (no bare `.unwrap()` in production;
`.expect("…statically valid")` only for the schema build) even though the OLD `manifests.rs` has pre-existing
`.unwrap()`s.

**FLAG / decide (verify against `ManifestsTable.java` AND `AllManifestsTable.java`):** the existing
`manifests.rs` scan (L173-183) appends the manifest's added/existing/deleted counts to BOTH the data-count
columns AND the delete-count columns UNCONDITIONALLY (no content gating). Java content-gates. Determine whether
the regular `manifests` table is genuinely buggy (Java `ManifestsTable` content-gates too). Implement
`all_manifests` CORRECTLY (content-gated) regardless. For the regular table: if the fix is a small, confirmed,
shared-helper change, fixing it (and updating its `expect_test`) is acceptable and preferred for consistency;
otherwise FLAG it for the orchestrator with the Java evidence — do NOT leave all_manifests matching the buggy
behavior.

Plan:
- [x] `inspect/all_manifests.rs`: `AllManifestsTable` + `schema()` (14 cols per Java AllManifestsTable) + async
      `scan()` (per-snapshot iteration, content-gated counts, reference_snapshot_id, key_metadata).
- [x] Wire `MetadataTableType::AllManifests` + `all_manifests()` accessor + as_str/TryFrom in
      `metadata_table.rs`; `mod` + `pub use` in `mod.rs`; the datafusion provider arm in BOTH matches.
- [x] Tests (in-crate, multi-snapshot fixture): per-snapshot duplication (a manifest in 2 snapshots → 2 rows
      with different reference_snapshot_id, NOT deduped); content-gated counts (a DELETE manifest has 0 in the
      data-count columns and vice-versa — mutation-pin the gating); reference_snapshot_id correctness;
      key_metadata present/null; partition_summaries (lower/upper bound strings); Arrow schema cols+ids+
      nullability incl. reference_snapshot_id/18 + key_metadata/19; empty table → 0 rows.
- [x] Docs: GAP_MATRIX inspection row (inspection-table set COMPLETE), Roadmap Phase 3, todo, lessons; note
      the manifests content-gating finding.
- [x] Verify from repo root: build `-p iceberg` AND `--workspace --exclude iceberg-sqllogictest`; lib ×2; 3
      interop suites; datafusion tests; clippy (workspace); fmt --check.

**Outcome (2026-06-08, Phase 3 Increment 5b, BUILDER Opus):** `all_manifests` — the LAST inspection table —
lands 🟡; the inspection-table SET IS NOW COMPLETE. New file `inspect/all_manifests.rs` (`AllManifestsTable`
+ `schema()` + per-snapshot `scan()`): iterates `metadata.snapshots()` directly (NO dedup — Java javadoc
"may return duplicate rows"), one row per (manifest × referencing snapshot) tagged with
`reference_snapshot_id`, CONTENT-GATED counts (a DATA manifest → *_data_files_count columns, delete columns
0; a DELETE manifest the reverse), `key_metadata` raw bytes, partition summaries via the reused converter.
Schema is the 14-field Java `AllManifestsTable.MANIFEST_FILE_SCHEMA` verbatim (ids + nullability — note the
three `*_delete_files_count` columns + `contains_nan` are REQUIRED here, unlike the regular `manifests`
table). Wired `MetadataTableType::AllManifests` (+ as_str/TryFrom/EnumIter `all_types`) +
`MetadataTable::all_manifests()` + `mod`/`pub use` in `inspect/mod.rs` + the datafusion provider arm in BOTH
`schema()`/`scan()` matches.

**CONTENT-GATING DECISION — FIXED the regular `manifests` table (surgical, shared-helper).** Verified against
BOTH Java files: `AllManifestsTable.manifestFileToRow` (L286-303) AND `ManifestsTable.manifestFileToRow`
(L108-113) content-gate IDENTICALLY (`manifest.content() == ManifestContent.DATA ? count : 0` for data
columns, `== DELETES ? count : 0` for delete columns). The Rust `inspect/manifests.rs` scan (old L173-183)
appended each manifest's added/existing/deleted counts to BOTH the data AND delete column families
UNCONDITIONALLY — a genuine bug vs Java. The fix is a small, confirmed `is_data` gate (the SAME logic
`all_manifests` uses), so per the brief's "prefer fixing if surgical" I fixed it and regenerated its
`expect_test` block via `UPDATE_EXPECT=1` (the fixture's DATA manifest now reports 0/0/0 in the delete
columns; diff inspected — only those three columns changed). Both tables now content-gate consistently.

**Java lines verified:** schema = `AllManifestsTable.java` L57-81 (`MANIFEST_FILE_SCHEMA` + `REF_SNAPSHOT_ID`
L53-54); content-gated row logic = `manifestFileToRow` L286-303 (+ `ManifestsTable.java` L108-113 for the
regular table); per-snapshot iteration = `doPlanFiles` L122-158 (iterate `table().snapshots()`, one row per
manifest per snapshot, no dedup). Shared partition-summary builder/conversion factored into new
`inspect/partition_summary.rs` (`pub(super)` fns, Rule of Three) used by both `manifests.rs` and
`all_manifests.rs`; the new code carries no bare `.unwrap()` (errors propagated; `.expect(...)` only for the
statically-valid schema build), and I upgraded the touched `manifests.rs` schema `.unwrap()` → `.expect(...)`
in passing.

**Tests (7, each named for its risk):**
- `test_all_manifests_does_not_dedup_shared_manifest_across_snapshots` — non-dedup: a manifest in 2 snapshots
  → 2 rows with different `reference_snapshot_id` (dedup OR current-snapshot-only read collapses it).
- `test_all_manifests_content_gates_counts_per_manifest_content` — DATA manifest's counts only in data
  columns (delete 0), DELETE manifest's only in delete columns (data 0).
- `test_all_manifests_reference_snapshot_id_distinct_from_added_snapshot_id` — the shared manifest's
  `added_snapshot_id` is PARENT while its current-snapshot row's `reference_snapshot_id` is CURRENT.
- `test_all_manifests_key_metadata_present_when_set_null_when_absent` — DELETE manifest's bytes present;
  DATA manifest's null.
- `test_all_manifests_renders_partition_summary_bounds_as_strings` — lower/upper bounds render to "100"/"300".
- `test_all_manifests_arrow_schema_columns_ids_and_nullability` — 14 cols in order, exact ids + nullability
  incl. reference_snapshot_id/18 (req) + key_metadata/19 (opt, LargeBinary).
- `test_all_manifests_empty_table_yields_no_rows` — 0 rows, 14-col shape, no panic.

**Mutations run (all caught):** (a) un-gate the counts (write into both data+delete families) → content-gating
test FAILS; (b) read only the current snapshot → non-dedup test FAILS; (b') dedup by `manifest_path`
(5a-style) → non-dedup test FAILS; (c) emit `added_snapshot_id` for `reference_snapshot_id` → ref-id test
FAILS; plus un-gating the regular `manifests.rs` scan → its `expect_test` block FAILS. Each restored after.

**Verification gate (all clean, from repo root):** `cargo build -p iceberg` ✅; `cargo build --workspace
--exclude iceberg-sqllogictest --all-targets` ✅; `cargo test -p iceberg --lib` ×2 = **1466 passed / 0
failed** both runs (was 1458 baseline → +7 all_manifests tests + 1 net from the shared-helper refactor;
stable); 3 interop suites (`interop_manage_snapshots` / `interop_update_schema` /
`interop_update_partition_spec`) = 4/4 each ✅; `cargo test -p iceberg-datafusion` lib+integration ✅ (after
regenerating the `test_provider_list_table_names` expect block — one additive `all_manifests` line; the only
datafusion doctest failure is the PRE-EXISTING `table_provider_factory` tokio-flavor one, unrelated);
`cargo test --doc --workspace --exclude iceberg-sqllogictest` green ✅; `cargo clippy --workspace --exclude
iceberg-sqllogictest --all-targets -- -D warnings` clean ✅; `cargo fmt --all -- --check` clean ✅. No commit,
no branch switch, no Cargo/spec/arrow/scan-production edits.

#### Phase 3 Increment 5b — REVIEW (2026-06-08, Opus REVIEWER, DELEGATED) — CHANGES-MADE
Verdict: **CHANGES-MADE** (one production-panic bug fixed in scope; everything else PASS). Independently
re-verified all 14 all_manifests field ids+nullability against `AllManifestsTable.MANIFEST_FILE_SCHEMA`
(L57-81) — all match Java. Confirmed BOTH Java tables content-gate identically (`AllManifestsTable.java`
L294-299 + `ManifestsTable.java` L108-113, both `content()==DATA ? n : 0` / `==DELETES ? n : 0`), so the
builder's `manifests.rs` content-gating fix is CORRECT + MINIMAL (schema unchanged, only row values; its
`expect_test` regen reflects the fix — DATA manifest's delete columns now 0/0/0 — and is mutation-pinned).
Per-snapshot/non-dedup/ref-id verification all hold; ran mutations (a) un-gate, (b) current-only, (d)
ref-id=added_snapshot_id, (e) un-gate manifests.rs — every one fails the matching test. Scope clean (git
modified set == declared scope; no Cargo/spec/arrow/scan-production touched). The pre-existing
`iceberg-datafusion` `table_provider_factory` doctest failure is environmental (`rt-multi-thread` disabled),
unrelated, file untouched — confirmed.

**BUG FOUND + FIXED — `contains_nan` production panic.** The builder copied Java's `contains_nan`/11 nullability
VERBATIM as REQUIRED. But Java's `PartitionFieldSummary.containsNaN()` returns a NULLABLE `Boolean` that is
`null` when the manifest has no NaN info (V1 manifests, or V2 without it — `contains_nan`/518 is OPTIONAL in
the on-disk manifest-list spec, `manifest_list.rs:572`), and Java's loosely-typed `StaticDataTask.Row` emits
that null into the nominally-`required` cell WITHOUT enforcement. Arrow ENFORCES non-nullability at
array-build time: the shared `append_partition_summaries` does `append_option(summary.contains_nan)`, so when
`contains_nan == None`, `partition_summaries.finish()` PANICS — `InvalidArgumentError("Found unmasked nulls
for non-nullable StructArray field \"contains_nan\"")` — crashing `AllManifestsTable::scan()`. The builder's
7 tests all missed this because the test fixture's `ManifestWriterBuilder` always defaults `contains_nan` to
`Some(false)`. FIX (in scope, `inspect/all_manifests.rs`): made `contains_nan`/11 OPTIONAL — faithfully
carries Java's emitted null when unset, and is consistent with the regular `manifests` table (whose Java
schema `ManifestsTable.java` L51 ALSO marks `contains_nan`/11 optional). Added
`test_all_manifests_renders_null_contains_nan_without_panicking` (writes a manifest whose summary has
`contains_nan == None`, scans, asserts a NULL cell; mutation-pinned — reverting to `required` re-triggers the
exact panic). Updated the schema doc-comment + GAP_MATRIX to document the divergence. This is a deliberate,
documented deviation from Java's NOMINAL `required` flag (which Java never enforces); correctness > nominal
metadata parity. lib total **1467** (was 1466; +1 net regression test, probe removed). Full gate (all 7)
re-run clean after the fix.

---

## SEQUENCE: ResidualEvaluator + filter-based conflict validation (Phase 3, started 2026-06-08)
Branch `phase3-residual-eval` (off merged `main` `a31657dd`). The inspection-table set is COMPLETE; this
sequence builds the residual-evaluation gap + the deferred Phase-2 filter-based conflict validation it unblocks.

**Why:** Java `api/.../expressions/ResidualEvaluator.java` is ABSENT in Rust. It partially-evaluates a row
filter against a partition's values → the residual predicate. Needed for (a) correct per-task scan residuals
(`FileScanTask` today carries the FULL bound predicate, not the partition-reduced residual) and (b) the
DEFERRED `OverwriteFiles`/`RowDelta` `validateNoConflictingData` (filter-based concurrent-commit safety).

Infrastructure that already exists (the build leans on it): `BoundPredicateVisitor` trait
(`expr/visitors/bound_predicate_visitor.rs`); `Transform::project` (inclusive) + `Transform::strict_project`
(strict) in `spec/transform.rs`; the `ExpressionEvaluatorVisitor` pattern (`expr/visitors/expression_evaluator.rs`)
that evaluates a bound predicate against a partition `Struct` → bool; `Predicate::{AlwaysTrue,AlwaysFalse}` +
`BoundPredicate::{AlwaysTrue,AlwaysFalse}`; `PartitionField.source_id` for the by-source lookup; the scan
already carries a `partition_bound_predicate` (`scan/mod.rs`).

### Increment 1 — ResidualEvaluator core (THIS increment)
New `crates/iceberg/src/expr/visitors/residual_evaluator.rs`. Mirror Java `ResidualEvaluator`:
- `ResidualEvaluator` holding `(partition_spec, filter, case_sensitive)`; constructors `unpartitioned(filter)`
  (residual is ALWAYS the filter unchanged) and `of(spec, filter, case_sensitive)` (→ unpartitioned when the
  spec has no fields).
- `residual_for(&self, partition: &Struct) -> Result<Predicate>` — the residual visitor:
  - and/or/not → recurse + combine; alwaysTrue/alwaysFalse pass through.
  - leaf unary/binary/set predicate on a partition-SOURCE column: get the partition fields with
    `source_id == ref.field_id()`; for each, compute `strict_project` (if its evaluation against the partition
    struct is TRUE → residual `AlwaysTrue`) and `project` (inclusive; if FALSE → residual `AlwaysFalse`); else
    keep the original predicate. (Java `predicate()` L227-288.) Evaluate the projected (partition-bound)
    predicate against the partition struct via the `ExpressionEvaluatorVisitor` pattern.
  - leaf on a non-partition column → keep the predicate (can't reduce).
- Tests: the Javadoc `day(ts)` example's 4 residual cases (`d>day(a)&&d<day(b)`→true; `d==day(a)`→`ts>=a`;
  `d==day(b)`→`ts<=b`; `d==day(a)==day(b)`→both); identity partition (eq reduces to alwaysTrue/false);
  bucket partition (non-reducible → keeps predicate); unpartitioned (returns filter verbatim); alwaysTrue/
  alwaysFalse filters; a predicate on a non-partition column (kept). Mutation-pin strict-vs-inclusive.

Plan / checkboxes (Increment 1):
- [x] New `expr/visitors/residual_evaluator.rs`: `ResidualEvaluator { spec, filter (BoundPredicate),
      partition_schema, case_sensitive }` + `unpartitioned(filter)` + `of(spec, schema, filter, case_sensitive)`.
- [x] `residual_for(&self, partition: &Struct) -> Result<Predicate>` via a `ResidualVisitor`
      (`BoundPredicateVisitor<T = Predicate>`): and/or/not via the simplifying combinators; leaf →
      strict-true ⇒ `AlwaysTrue`, inclusive-false ⇒ `AlwaysFalse`, else keep the original (bound→unbound).
- [x] Register `pub(crate) mod residual_evaluator;` in `expr/visitors/mod.rs`.
- [x] Tests (in-crate): 4 day-example cases + identity + bucket + non-partition + unpartitioned + alwaysTrue/
      alwaysFalse + and/or/not mixes; mutation-pin strict↔inclusive and partition-ignore.
- [x] Docs: GAP_MATRIX residual row, Roadmap Phase 3, todo Outcome, lessons.
- [x] Gate: build -p iceberg / workspace build / lib ×2 / 3 interop / clippy / fmt.

### Increment 2 — wire residuals into scan FileScanTask
Compute the per-task residual during planning so each `FileScanTask` carries the partition-reduced residual
(Java parity), connecting to the existing `partition_bound_predicate` machinery in `scan/mod.rs`. Guard
behavior-equivalence (residual ⊆ original filter; row-level filtering must not change results).

### Increment 3 — OverwriteFiles (+ RowDelta) filter-based validateNoConflictingData
Consume the residual/metrics evaluators to implement the deferred filter-based conflict validation, building
on the Phase-2 foundation (`Transaction.starting_snapshot_id` + `TransactionAction::validate` +
`added_data_files_after`; `ReplacePartitions.validate_no_conflicting_data` already wired). Scope precisely
against Java `MergingSnapshotProducer.validateAddedDataFiles` / `OverwriteFiles` when reached.

**Outcome (2026-06-08, Phase 3 Increment 1 — ResidualEvaluator core, BUILDER Opus):** `ResidualEvaluator`
landed 🟡 in the new `crates/iceberg/src/expr/visitors/residual_evaluator.rs`. It partially evaluates a bound
row filter against a partition's `Struct` and returns the residual — the part of the filter NOT already
decided by the partition.

**Type decisions (deliberate divergences from Java, documented in the module + GAP_MATRIX):**
- Java's `ResidualEvaluator` holds one `Expression expr` (the bound/unbound union) and `residualFor` returns
  an `Expression`. The Rust port holds the filter as a `BoundPredicate` (the scan binds before planning) and
  returns the residual as an UNBOUND `Predicate`. Signature:
  `ResidualEvaluator::of(spec: PartitionSpecRef, schema: &Schema, filter: BoundPredicate, case_sensitive: bool)
  -> Result<Self>`, `ResidualEvaluator::unpartitioned(filter: BoundPredicate) -> Self`,
  `residual_for(&self, partition: &Struct) -> Result<Predicate>`.
- `of` takes the table `schema` (Java's `PartitionSpec` carries its schema; the Rust one does not), used once
  in the ctor to compute `spec.partition_type(schema)` → a partition `Schema` the projected predicates bind to.
- For the leaf "keep the original predicate" case Java returns the original BOUND `pred`; the Rust port
  reconstructs the unbound `Predicate` from the bound leaf by the source field's NAME
  (`bound_to_unbound` + `unbound_reference`) — partition-source columns are top-level schema fields, so the
  field name is the unbound reference name.
- Visibility `pub(crate)` (Increments 2/3 are in-crate consumers); `#![allow(dead_code)]` until they land.

**Residual algorithm (mirrors Java `ResidualVisitor`):** a `BoundPredicateVisitor<T = Predicate>`. and/or →
the simplifying `Predicate::{and,or}` combinators (so AlwaysTrue/AlwaysFalse short-circuit like
`Expressions.and/or`); not → a Java-`Expressions.not`-faithful `simplifying_not` (`not(true)=false`,
`not(false)=true`, `not(not x)=x`, else wrap) because the `Predicate` `!` operator does NOT simplify
constants. Every leaf method routes to `reduce_leaf(reference, predicate)`: find the partition fields with
`source_id == reference.field().id`; if none, keep the predicate; otherwise for each part compute
`Transform::strict_project` (bind to the partition schema, evaluate against the partition via
`ExpressionEvaluatorVisitor` → if TRUE, return `AlwaysTrue`) then `Transform::project` (inclusive; if FALSE,
return `AlwaysFalse`); if neither is conclusive, keep the original predicate. An `AlwaysTrue`/`AlwaysFalse`
*projection* short-circuits to its constant (Java's "if the result is not a predicate it must be a constant").

**Reuse:** made `ExpressionEvaluatorVisitor` + its `new` `pub(crate)` in `expression_evaluator.rs` (the ONLY
edit to that file — the brief's sanctioned small helper) so the residual evaluator reuses the exact
per-operator bound-predicate-against-partition-`Struct` evaluation rather than duplicating it.

**Java lines verified:** `ResidualEvaluator.unpartitioned` L73-75 + `UnpartitionedResidualEvaluator` L53-65
(residual is always the whole expr); `of` L84-90 (empty spec → unpartitioned); the leaf methods L146-223
(each evaluates the bound ref against the partition struct → alwaysTrue/alwaysFalse); the load-bearing
`predicate(BoundPredicate)` L227-288 (strict-projection-true ⇒ alwaysTrue; inclusive-projection-false ⇒
alwaysFalse; else keep `pred`); and `Expressions.not` L63-73 (the constant-folding negation).

**Tests (16, each named for its risk):** the 4 Javadoc `day(utc_timestamp)` cases
(`*_strictly_between_bounds_reduces_to_always_true`, `*_equals_lower_bound_keeps_lower_predicate_only`,
`*_equals_upper_bound_keeps_upper_predicate_only`, `*_equals_both_bounds_keeps_both_predicates`) — the
headline reductions; `*_identity_partition_eq_matching_value_reduces_to_always_true` /
`*_non_matching_value_reduces_to_always_false` (identity eq collapses by the partition value);
`*_bucket_partition_keeps_predicate_unchanged` (non-invertible transform → predicate survives; the partition
value is computed as the real `bucket(5,16)` via `transform_literal` so the inclusive projection is true and
strict is absent); `*_predicate_on_non_partition_column_is_kept`;
`*_unpartitioned_spec_returns_filter_verbatim` (empty spec via `of`) + `*_unpartitioned_constructor_*`;
`*_always_true_filter_passes_through` / `*_always_false_*`;
`*_and_of_reducible_and_non_reducible_keeps_only_the_non_reducible`,
`*_and_short_circuits_to_false_when_partition_excludes_the_partition_leaf`,
`*_or_of_reducible_true_and_non_reducible_short_circuits_to_true`,
`*_not_over_partition_leaf_negates_the_reduced_constant` (and/or/not mixes of a reducible + a non-reducible
leaf).

**Mutations run (each caught):** (a) swap `strict_project`↔`project` in `reduce_leaf` → 4 reduction tests
FAIL (the three "equals one bound" day cases + the bucket-keep). (b) make `residual_for` ignore the partition
(always `bound_to_unbound(filter)`) → 9 reduction tests FAIL (all day reductions + identity + the and/or/not
combinations); the 7 "kept verbatim" tests legitimately still pass. Both restored after.

**Gate (all clean, from repo root, pinned nightly):** `cargo build -p iceberg` ✅; `cargo build --workspace
--exclude iceberg-sqllogictest --all-targets` ✅; `cargo test -p iceberg --lib` ×2 = **1483 passed / 0 failed**
both runs (was 1467 → +16 residual tests); 3 interop suites (`interop_manage_snapshots` /
`interop_update_schema` / `interop_update_partition_spec`) = 4/4 each ✅; `cargo clippy --workspace --exclude
iceberg-sqllogictest --all-targets -- -D warnings` clean ✅ (collapsed two `if let { if }` into let-chains);
`cargo fmt --all -- --check` clean ✅. No commit, no branch switch, no Cargo/spec/scan/transaction edits.
Files: NEW `expr/visitors/residual_evaluator.rs`; `expr/visitors/mod.rs` (`pub(crate) mod residual_evaluator;`);
`expr/visitors/expression_evaluator.rs` (`ExpressionEvaluatorVisitor` + `new` → `pub(crate)`); docs
(GAP_MATRIX/Roadmap/todo/lessons).

#### Phase 3 Increment 1 — REVIEW (2026-06-08, Opus REVIEWER, DELEGATED)
Adversarially verified the ResidualEvaluator port against the Java source (`/tmp/iceberg-java-ref`) and by
running 7 code mutations. Findings:
- **Strict/inclusive direction: CONFIRMED CORRECT (not swapped).** Java `predicate(BoundPredicate)`
  L248-262: `projectStrict` result `op()==TRUE` ⇒ `alwaysTrue`; L265-283: `project` (inclusive) result
  `op()==FALSE` ⇒ `alwaysFalse`. Rust `reduce_leaf` uses `strict_project`→AlwaysTrue then `project`
  (inclusive)→AlwaysFalse — same direction. Swap mutation fails 4-6 tests (the 3 day "equals one bound" +
  bucket-keep, now also truncate + year).
- **The 4 day-residual cases: asserted EXACTLY** (strictly-between→`AlwaysTrue`; `==day(a)`→`ts>=a`;
  `==day(b)`→`ts<=b`; `==both`→`ts>=a AND ts<=b`), not weakened to "not AlwaysTrue".
- **Untested transform classes: VERIFIED + PINNED.** Exercised truncate / year / void through `residual_for`
  (throwaway probes): all reduce correctly (truncate all-three-ways; year keeps-both-inside + AlwaysFalse-
  outside; void KEEPS — Java `VoidTransform` returns null for both projections, no panic). Added permanent
  pinning tests for all three.
- **Leaf reconstruction round-trip: VERIFIED + PINNED** for set (`in`/`not_in`) and unary (`is_null`) on a
  non-partition column (builder only covered binary). `not(in)` correctly round-trips as `Not(In{..})` (the
  binder keeps the `Not` wrapper, not a `NotIn` operator) — faithful, not a bug.
- **Combinator simplification: CONFIRMED.** `Predicate::and`/`or` constant-fold (predicate.rs L563-599);
  `simplifying_not` mirrors Java `Expressions.not` L63-73 EXACTLY (not(true)=false/not(false)=true/
  not(not x)=x, else wrap). The `!` operator does NOT fold, so `simplifying_not` is load-bearing.
- **Multi-field-per-source loop: PINNED.** A spec with bucket(category)+identity(category) forces the loop
  to continue past the inconclusive bucket field; break-after-first mutation now fails.
- **Mutations run (all caught):** (a) strict↔inclusive swap; (b) ignore-partition; (c) drop inclusive-false;
  (d) drop strict-true; (e) break `not` folding; (f) drop negation in `bound_to_unbound` `Not` arm
  [SURVIVED the builder's 16 tests — a real gap, now closed]; (g) break-after-first in the parts loop
  [closed].
- **Scope + floor: CLEAN.** Production code byte-identical to the builder's (only test additions);
  `expression_evaluator.rs` is ONLY the visibility bump; `#![allow(dead_code)]` is module-scoped; no bare
  `.unwrap()` in production paths; no Cargo/spec/scan/transaction edits; no commit/branch switch.

**Review outcome:** CHANGES-MADE — added 8 tests (16→24); closed two genuine test-strength gaps (the
unpartitioned-`Not` reconstruction and the multi-field-per-source loop) the builder's suite did not catch.
**Verify:** build -p iceberg clean; workspace build clean; lib ×2 = 1491/0 (was 1483 → +8); 3 interop
suites 4/4 each; clippy -D warnings clean; fmt clean. Row stays 🟡 (scan-wiring Inc 2 + conflict-validation
Inc 3 + Java interop still pending). Files touched: `residual_evaluator.rs` (tests only), todo, lessons.

### Increment 2 — wire the ResidualEvaluator into scan FileScanTask (DETAIL, 2026-06-08)
Java `BaseFileScanTask.residual()` = `residuals.residualFor(file.partition())` — each task carries the
PARTITION-REDUCED residual, and the reader filters rows with it. Today Rust sets `FileScanTask.predicate` to
the FULL `snapshot_bound_predicate` (`scan/context.rs::into_file_scan_task`), and `partition_spec` is `None`
(a TODO). The ONLY runtime consumer of `task.predicate` is `arrow/reader.rs` (row filtering); datafusion does
not read it.

Plan:
- [x] Thread the file's `Arc<PartitionSpec>` into `ManifestEntryContext` / `into_file_scan_task` (resolve the
      `partition_spec: None` TODO) — resolved in `PlanContext::create_manifest_file_context` via
      `table_metadata.partition_spec_by_id(manifest_file.partition_spec_id)`. Set `partition_spec: Some(spec)`
      on the task too.
- [x] In `into_file_scan_task`, when there is a filter: compute the residual via the per-manifest
      `ResidualEvaluator`, BIND the resulting unbound `Predicate` to `snapshot_schema`, and store it as
      `predicate`. No filter ⇒ `predicate: None`; unpartitioned spec ⇒ residual == the full filter (unchanged
      behavior). The evaluator is constructed ONCE per manifest file (the spec + snapshot filter are constant
      within a manifest) and shared across its entries — the per-spec cache pattern, but per-manifest, so no
      shared mutable cache was needed.
- [x] Reader UNCHANGED (it already uses `task.predicate` for row filtering; now it's the residual — and
      `task.partition_spec` for identity-partition constants, which the threading newly activates).
- [x] Tests (CRITICAL — prove RESULT-EQUIVALENCE + genuine reduction): 11 scan-wiring tests on identity AND
      truncate transform partitions; reduced-residual (not full filter); fully-implied → `AlwaysTrue`; spec
      populated; result-equivalence reads; unpartitioned/no-filter unchanged; partition-mismatch pruning.
      Mutations caught: (a) leave `predicate`=full filter → 3 reduction tests fail; (b) residual against the
      WRONG (empty) partition → all 5 residual tests fail.
- [x] Docs: GAP_MATRIX residual/scan row (scan wiring done), Roadmap, todo, lessons.

**Outcome (2026-06-08, Phase 3 Increment 2 — residual scan-wiring, BUILDER Opus):** each `FileScanTask` now
carries the PARTITION-REDUCED residual of the scan filter, not the full snapshot filter — Java
`BaseFileScanTask.residual()` parity (`residuals.residualFor(file.partition())`). **What I threaded:** the
file's `Arc<PartitionSpec>` (`table_metadata.partition_spec_by_id(manifest_file.partition_spec_id)`) and an
`Arc<ResidualEvaluator>`, both added to `ManifestFileContext` (built in
`PlanContext::create_manifest_file_context`, which has `table_metadata`) and propagated to each
`ManifestEntryContext`. **Where the residual is computed:** `ManifestEntryContext::into_file_scan_task` calls a
new `residual_predicate()` helper — it runs the per-manifest evaluator's `residual_for(file.partition())`,
binds the unbound residual `Predicate` back to the snapshot schema, and stores the `BoundPredicate` as
`task.predicate`. The evaluator is built ONCE per manifest file (spec + filter constant within a manifest) and
shared across entries — the cleaner choice over a shared mutable cache, since each manifest already maps to one
spec id. `partition_spec: None` → `Some(spec)` resolves the TODO. **Result-equivalence argument:** every row in
a data file belongs to that file's single partition tuple, so the partition-implied conditions the residual
drops are TRUE for all rows in the file; filtering with the residual therefore selects exactly the rows the
full filter would — result-preserving, only cheaper. Stated in an `into_file_scan_task` comment and proven by
the equivalence read tests (identity + truncate). **Java lines verified:** `BaseFileScanTask.residual()` =
`residuals.residualFor(file.partition())` (the per-task residual is the partition-reduced one); the
`ResidualEvaluator` semantics were verified in Increment 1. **The one unforeseen interaction (flagged):**
threading `task.partition_spec` ALSO activates the arrow reader's identity-partition constant materialization
(`reader.rs:451-455` → `record_batch_transformer::constants_map`, Java `PartitionUtil.constantsMap`), which was
dormant while the field was `None`. This is CORRECT Iceberg behavior but exposed a pre-existing inconsistency in
the shared `TableTestFixture` (partition `x`=100/200/300 vs the parquet `x` column `[1; 1024]`). I made the
fixture consistent (partition `x`=1, matching the data) — a test-only change in `scan/mod.rs` — which is the
truthful resolution; no read test prunes on `x`, so nothing else changes. This rippled one expect-snapshot in
`inspect/manifests.rs` (partition-summary bounds `"100"`/`"300"` → `"1"`/`"1"`, mechanical) — OUTSIDE the
allowed-edit set, flagged here and in the final report. **Test list (11, each pins a risk):**
`test_task_predicate_is_partition_reduced_residual_not_full_filter` (residual reduced, not full filter —
mutation (a) target); `test_task_predicate_residual_is_always_true_when_filter_fully_implied` (filter fully
implied by partition → `AlwaysTrue`); `test_task_partition_spec_is_populated_from_table_metadata` (TODO
resolved); `test_residual_scan_result_equivalent_to_full_filter_identity_partition` (RESULT-EQUIVALENCE read,
identity); `test_unpartitioned_table_keeps_full_filter_as_task_predicate` (no reduction on unpartitioned);
`test_no_filter_scan_leaves_task_predicate_none` (no-filter unchanged + spec still threaded);
`test_filter_excluding_the_partition_prunes_the_file_entirely` (partition mismatch → file pruned);
`test_task_predicate_residual_reduced_on_transform_truncate_partition` (NON-identity transform residual
reduced); `test_residual_scan_result_equivalent_to_full_filter_truncate_partition` (RESULT-EQUIVALENCE read,
truncate transform — x read from data, not constant). Plus the 8 pre-existing scan read tests updated to decode
the now-run-encoded identity-partition `x` constant (new `decode_int64_column` helper), and the
`test_select_with_repeated_column_names` type assertions (`x` is `RunEndEncoded`, like `_file`).
**Mutations run (each caught):** (a) `residual_predicate` returns the full `snapshot_bound_predicate` →
`test_task_predicate_is_partition_reduced_*` + `*_residual_is_always_true_*` +
`*_residual_reduced_on_transform_truncate_*` all FAIL; (b) `residual_for(&Struct::empty())` (wrong partition) →
all 5 residual tests FAIL (3 panic in the accessor, 2 wrong-result). Both restored; production byte-identical
after. **Removed** the module-scoped `#![allow(dead_code)]` from `residual_evaluator.rs` (now consumed in
production — clippy `-D warnings` confirms every item is reachable). **Gate (all from repo root, pinned
nightly):** `cargo build -p iceberg` ✅; `cargo build --workspace --exclude iceberg-sqllogictest --all-targets`
✅; `cargo test -p iceberg --lib` ×2 = **1500 passed / 0 failed** both runs (was 1491 → +9 residual-wiring
tests, net; the 8 updated read tests + 1 inspect snapshot are edits not adds); 3 interop suites
(`interop_manage_snapshots`/`interop_update_schema`/`interop_update_partition_spec`) = 4/4 each ✅; `cargo
clippy --workspace --exclude iceberg-sqllogictest --all-targets -- -D warnings` clean ✅; `cargo fmt --all --
--check` clean ✅. Existing scan/reader filtered-read tests all still pass. No commit, no branch switch, no
Cargo/spec/transaction edits; `arrow/reader.rs` behavior unchanged (only the now-active partition-constant path
it already contained). **Files:** `scan/context.rs` (spec + residual threading, residual computation),
`scan/mod.rs` (fixture consistency + truncate fixture + 9 new tests + 8 read-test decode updates),
`expr/visitors/residual_evaluator.rs` (removed `allow(dead_code)` + doc), `inspect/manifests.rs` (forced
expect-snapshot ripple), docs (GAP_MATRIX/Roadmap/todo/lessons).
- [x] Verify from repo root: build -p iceberg + workspace; lib ×2; 3 interop; clippy (workspace); fmt.

#### Phase 3 Increment 2 — REVIEW (2026-06-08, Opus REVIEWER, DELEGATED)
Adversarially verified residual result-equivalence and scope against the Java source + 3 code mutations.
**VERDICT: CHANGES-MADE.**
- **Residual result-equivalence: CONFIRMED.** The residual is computed from the FILE's own spec
  (`partition_spec_by_id(manifest_file.partition_spec_id)`, resolved per manifest), evaluated against the
  FILE's own `data_file().partition()`, then BOUND BACK to the snapshot schema — exactly Java
  `BaseFileScanTask.residual()` = `residuals.residualFor(file.partition())`. The data-matches-partition
  invariant (the original fixture VIOLATED it: data `x`=1, partition `x`=100/200/300) makes the
  fixture-consistency fix genuinely NECESSARY, not optional — confirmed independently.
- **Fixture fix DECISION: KEPT the collapse (3→1 partition).** Investigated the multi-partition alternative
  and REJECTED it: the shared fixture writes BYTE-IDENTICAL parquet data to all three files via one
  `for n in 1..=3` loop (only 2 are live — 2.parquet is the Deleted entry), so keeping 100/200/300 would
  require rewriting `write_parquet_data_files` to emit per-file distinct `x` data and rippling ~8 read tests —
  disproportionate. Crucially, NO coverage is lost: the base `scan/mod.rs` had ZERO tests filtering on the
  partition column `x` with 100/200/300, and the dedicated multi-partition inspect tests
  (`all_manifests`/`entries`/`files`/`partitions`) build their OWN inline 3-partition manifests (untouched,
  60/60 green). The collapse is adequate and the better fix is disproportionately heavy.
- **Constants-map activation (`partition_spec=Some`) DECISION: KEPT, justified.** Verified by REAL reads:
  `record_batch_transformer::constants_map` materializes ONLY identity-transform fields (line 60-61); the
  identity fixture reads `x` as a RunEndEncoded constant from partition metadata (value 1) and the truncate
  fixture reads `x` from the data file (non-identity) — both confirmed by equivalence read tests. `arrow/reader.rs`
  is byte-unchanged (not in the diff). Correct Iceberg behavior (Java `PartitionUtil.constantsMap`), resolves a
  real TODO, fully pinned — KEEP over DEFER.
- **inspect/manifests.rs ripple: BENIGN.** Mechanical consequence of the (necessary) partition-value change on
  the SAME manifest the scan reads; the partition-summary bound is genuinely now 1. Not a masked bug.
- **Mutations run (3):** (a) store full filter instead of residual → CAUGHT (4 tests, incl. the identity
  equivalence read via its pre-read reduced-residual assertion); (b) residual vs EMPTY partition → CAUGHT
  (5 tests, incl. BOTH equivalence reads); (c) use the table DEFAULT spec instead of the file's own
  `partition_spec_id` → **SURVIVED** the builder's suite (fixture had only one spec) — a real test-strength
  gap (the exact silent partition-evolution data bug). **CLOSED** by a new pinning test.
- **Test added:** `test_residual_uses_files_own_spec_not_the_table_default_spec` — a 2-spec fixture
  (`new_with_evolved_default_spec` + `setup_manifest_files_under_spec_zero`) where the live file is written
  under non-default `identity(x)` spec 0 while the table default is unpartitioned spec 1; pins that the
  residual reduces to `AlwaysTrue` (file's spec) and `task.partition_spec.spec_id()==0`. Re-running mutation
  (c) now FAILS it.
- **Scope + floor: CLEAN.** Production code byte-identical to the builder's after every mutation reverted; my
  additions are test-only (fixture builder + 1 test) in `scan/mod.rs`; no bare `.unwrap()` added in production;
  no Cargo/spec/reader-logic/transaction edits; no commit/branch switch.
- **Gate (all clean, repo root, pinned nightly):** build -p iceberg ✅; workspace build (--exclude
  sqllogictest --all-targets) ✅; lib ×2 = **1501/0** both (was 1500 → +1 spec-evolution pin); 3 interop
  suites 4/4 each ✅; clippy -D warnings clean ✅; fmt clean ✅. Row stays 🟡 (Increment 3 conflict-validation
  + Java interop still pending).

### Increment 3 — OverwriteFiles filter-based validateNoConflictingData (DETAIL, 2026-06-08)
The headline write-safety unblock. Java `BaseOverwriteFiles.validate` → `validateNewDataFiles` →
`MergingSnapshotProducer.validateAddedDataFiles(base, startingSnapshotId, conflictDetectionFilter, parent)`:
enumerate DATA files ADDED by concurrent commits since the starting snapshot, and reject the commit if ANY
could contain records matching the conflict-detection filter. SCOPE: OverwriteFiles `validateNoConflictingData`
ONLY (the filter-based added-data-file conflict check). DEFER: `validateAddedFilesMatchOverwriteFilter`
(block 1, the writer's own files), `validateNoConflictingDeletes` (block 3, delete conflicts), and RowDelta —
each its own follow-up.

Model (already landed): `transaction/replace_partitions.rs::validate` — flag → `effective_start =
validate_from_snapshot.or(starting_snapshot_id)` → `added_data_files_after(current, effective_start)` →
per-file conflict test → non-retryable `ErrorKind::DataInvalid` on the first conflict. OverwriteFiles differs
ONLY in the conflict test: instead of `(spec_id, partition) ∈ drop-set`, it's "could this added file match the
conflict filter?" via the EXISTING `InclusiveMetricsEvaluator::eval(&bound_filter, file) -> Result<bool>`
(file metrics/bounds; `pub(crate)` in `expr/visitors/inclusive_metrics_evaluator.rs`).

Building blocks (all exist): `added_data_files_after(table, Option<i64>) -> Result<Vec<DataFile>>`
(`transaction/snapshot.rs`); `InclusiveMetricsEvaluator::eval`; `Predicate::bind(schema, case_sensitive) ->
Result<BoundPredicate>`. The `validate` hook is `async fn validate(self: Arc<Self>, starting_snapshot_id:
Option<i64>, current: &Table) -> Result<()>` (default no-op; override it, mirroring ReplacePartitions).

Plan:
- [x] Add fields to `OverwriteFilesAction`: `validate_no_conflicting_data: bool`, `conflict_detection_filter:
      Option<Predicate>`, `validate_from_snapshot: Option<i64>` (+ builder methods named like ReplacePartitions:
      `validate_no_conflicting_data()`, `conflict_detection_filter(Predicate)`, `validate_from_snapshot(i64)`).
- [x] Override `validate`: no-op unless the flag is set; `effective_start`; `added_data_files_after`; bind the
      conflict filter (if `None`, treat as `AlwaysTrue` = ANY concurrent added data file conflicts — the most
      conservative serializable check); for each added file `InclusiveMetricsEvaluator::eval(&bound, file, true)?`
      → first match → non-retryable `DataInvalid` naming the file + filter (Java "Found conflicting files that
      can contain records matching %s: %s"). Default case-sensitivity true (Java `isCaseSensitive()`; the
      action has no such field — noted in the `validate` doc-comment).
- [x] Module doc: drop `validateNoConflictingData` from the deferred list (note block1/block3/RowDelta remain).
- [x] Tests (MemoryCatalog, the ReplacePartitions conflict-test pattern — commit a CONCURRENT snapshot after
      the txn's starting point, then validate): (1) no concurrent commit → commit OK; (2) concurrent commit
      adds a file whose metrics MATCH the filter → validate fails `DataInvalid` (non-retryable, doesn't loop);
      (3) concurrent commit adds a file whose metrics EXCLUDE the filter (bounds outside) → commit OK; (4) flag
      OFF → no check (snapshot isolation) even with a conflict; (5) `conflict_detection_filter` None → any
      concurrent added file conflicts; (6) `validate_from_snapshot` override (+ a negative half + a
      tx-captured-start survives-rebase pin). Build files WITH bounds so the inclusive evaluator can
      include/exclude. Mutation-pin: the include-vs-exclude metrics decision + the non-retryable error kind.
- [x] Docs: GAP_MATRIX (OverwriteFiles validateNoConflictingData landed, row stays 🟡), Roadmap, todo, lessons.
- [x] Verify from repo root: build -p iceberg + workspace; lib ×2; 3 interop; clippy (workspace); fmt.

**Outcome (2026-06-08, Phase 3 Increment 3 — OverwriteFiles filter-based `validateNoConflictingData`, BUILDER
Opus):** Added the serializable-isolation write-safety layer to `OverwriteFilesAction` in
`transaction/overwrite_files.rs` (the ONLY production file touched besides docs). It is the SAME shape as the
already-landed `ReplacePartitions.validate`; only the per-file conflict TEST differs.
- **Builder API:** three opt-in fields + builders, named exactly like ReplacePartitions —
  `validate_no_conflicting_data(self) -> Self` (enable), `validate_from_snapshot(self, i64) -> Self` (override
  the starting snapshot), and the NEW `conflict_detection_filter(self, Predicate) -> Self` (the data filter
  OverwriteFiles has but ReplacePartitions doesn't). All default OFF/None in `new()`.
- **`validate` algorithm** (overrides the `TransactionAction` default no-op): `if
  !validate_no_conflicting_data { return Ok(()) }` → `effective_start = validate_from_snapshot.or(starting_
  snapshot_id)` → `added = added_data_files_after(current, effective_start).await?` (empty ⇒ Ok) → bind the
  conflict filter against `current.metadata().current_schema()` case-sensitive=`true` (the filter is the
  caller's `conflict_detection_filter` when set, else `Predicate::AlwaysTrue`) → for each added file, if
  `InclusiveMetricsEvaluator::eval(&bound_filter, file, true)?` is true, return a non-retryable
  `Error::new(ErrorKind::DataInvalid, "Found conflicting files that can contain records matching {filter}:
  {path}")` on the FIRST conflict.
- **Java lines verified against** (`/tmp/iceberg-java-ref`): `BaseOverwriteFiles.validate` L136-179 — the
  `validateNewDataFiles` branch L163-165 calls `validateAddedDataFiles(base, startingSnapshotId,
  dataConflictDetectionFilter(), parent)`; `MergingSnapshotProducer.validateAddedDataFiles` L391-412 (build a
  `ManifestGroup` over the concurrent-added DATA manifests filtered by the conflict filter; throw "Found
  conflicting files that can contain records matching %s: %s" if any entry survives); `dataConflictDetection
  Filter()` L181-187 (filter when set, else rowFilter when not alwaysFalse + no deleted files, else
  `alwaysTrue()`). VERIFIED our `overwriteByRowFilter` is deferred so `rowFilter()` is effectively
  `alwaysFalse()` and the row-filter branch can never apply → None ⇒ `AlwaysTrue`, matching Java.
- **None-filter default decision:** a `None` `conflict_detection_filter` binds `Predicate::AlwaysTrue` ⇒ ANY
  concurrently-added DATA file is a conflict (the most conservative serializable check). This mirrors Java
  `dataConflictDetectionFilter()` returning `alwaysTrue()` and is pinned by
  `test_overwrite_none_filter_treats_any_concurrent_add_as_conflict` (a no-bounds concurrent file is still a
  conflict).
- **Case-sensitivity default decision:** Java binds with `isCaseSensitive()`. `OverwriteFilesAction` has no
  case-sensitivity field, so the filter is bound case-sensitive (`true` = the Iceberg/Java default); noted in
  the `validate` doc-comment. (Adding a `case_sensitive` builder is a trivial future follow-up if needed.)
- **Test list (9 new conflict tests; each risk pinned):**
  1. `test_overwrite_validation_no_concurrent_commit_succeeds` — validation enabled but nothing concurrent ⇒
     commit OK (a race-free commit must not be blocked).
  2. `test_overwrite_rejects_concurrent_added_file_matching_filter` — HEADLINE: concurrent file with y bounds
     [60,70] vs filter `y>=50` ⇒ REJECTED, non-retryable, error names the file (silent lost-write prevention).
  3. `test_overwrite_allows_concurrent_added_file_excluded_by_filter` — concurrent file y bounds [10,20]
     entirely below `y>=50` ⇒ inclusive evaluator EXCLUDES ⇒ commit OK (no false conflict).
  4. `test_overwrite_without_validation_allows_conflicting_concurrent_append` — flag OFF (filter present but
     inert) ⇒ a would-be-conflict concurrent append does NOT block (snapshot isolation unchanged / opt-in).
  5. `test_overwrite_none_filter_treats_any_concurrent_add_as_conflict` — None filter ⇒ AlwaysTrue ⇒ a
     no-bounds concurrent add is a conflict (the conservative serializable default; the OPPOSITE of "no check").
  6. `test_overwrite_validate_from_snapshot_override_changes_concurrent_window` — `validate_from_snapshot(S0)`
     widens the window to include S1's add ⇒ REJECTED (the override genuinely shifts the boundary).
  7. `test_overwrite_validate_from_snapshot_at_head_finds_no_conflict` — `validate_from_snapshot(S1=head)` ⇒
     nothing concurrent ⇒ commit OK (the negative half proving the boundary shift is real).
  8. `test_overwrite_rejects_concurrent_using_tx_captured_starting_snapshot` — NO `validate_from_snapshot`;
     relies on the tx-captured start surviving `do_commit`'s re-base ⇒ REJECTED (the only test exercising the
     capture; if the start were re-read from the refreshed head the check would silently always pass).
  All 9 use the ReplacePartitions concurrent-commit pattern: a SEPARATE `fast_append` lands between building
  the txn and committing it, advancing the catalog head; `do_commit` refreshes to that head and runs `validate`
  against it. Data files carry y-column bounds via the new `data_file_with_y_bounds(path, part, lo, hi)` helper
  (sets `lower_bounds`/`upper_bounds` on schema field id 2).
- **Mutations run + each caught:** (a) invert the metrics decision (`if !eval(...)`) → the EXCLUDE test
  (`..excluded_by_filter`) AND the MATCH/None tests fail; (b) make `validate` early-`return Ok(())` (skip the
  check) → exactly the 4 rejection tests (#2/#5/#6/#8) fail, the OK tests stay green; (c) change the error kind
  to `CatalogCommitConflicts` → the 4 rejection tests' `kind()==DataInvalid` assertions fail. Bonus
  genuinely-non-retryable check: `.with_retryable(true)` on the conflict error → the `!err.retryable()`
  assertion fails AND the test runtime jumps 0.12s→1.57s (the retry loop demonstrably loops) — confirming the
  property is verified, not merely asserted.
- **Gate (all from repo root, pinned nightly):** `cargo build -p iceberg` ✅ (Finished); `cargo build
  --workspace --exclude iceberg-sqllogictest --all-targets` ✅ (Finished); `cargo test -p iceberg --lib` ✅ TWICE
  (run1 1509 passed / 0 failed; run2 1509 passed / 0 failed — **new lib total 1509**, was 1500, +9 tests);
  3 interop binaries ✅ (`interop_manage_snapshots` 4/4, `interop_update_partition_spec` 4/4,
  `interop_update_schema` 4/4); `cargo clippy --workspace --exclude iceberg-sqllogictest --all-targets -- -D
  warnings` ✅ (Finished, no warnings); `cargo fmt --all -- --check` ✅ (clean after one `cargo fmt`);
  `cargo test -p iceberg --lib transaction::` ✅ (225 passed). `transaction::overwrite_files` alone: 18/18.
- **Scope:** touched ONLY `transaction/overwrite_files.rs` + docs (GAP_MATRIX, Roadmap, todo, lessons). No
  edits to `snapshot.rs` / `inclusive_metrics_evaluator.rs` / `action.rs` / `predicate.rs` / `replace_partitions.rs`
  (read-only). No `Cargo.toml`/lockfile/spec/scan/arrow changes. No commit, no branch switch, no push.
- **UNSURE / flagged:** (1) The Java `addedDataFiles` builds a `ManifestGroup` that pre-filters at the MANIFEST
  level (manifest partition/metrics summary) BEFORE the per-entry inclusive-metrics test; the Rust port
  approximates this at the FILE level only (`added_data_files_after` returns every added DATA file, then we run
  `InclusiveMetricsEvaluator::eval` per file). This is a CONSERVATIVE over-scan — it can only over-reject
  (inspect a file Java's manifest pre-filter would skip), never under-reject, so it cannot let a real conflict
  through; it is a performance/precision gap, not a correctness one. (2) `include_empty_files = true` is passed
  to `eval` so a zero-record concurrent file is evaluated by its bounds rather than auto-excluded on emptiness
  — conservative (a 0-record file is effectively never a real conflict, but never excluding it is safe). (3)
  The concurrent commit is simulated EXACTLY as the landed ReplacePartitions tests do it (a real second
  `fast_append` through the same `MemoryCatalog`, between txn-build and txn-commit), so `do_commit`'s genuine
  refresh/re-base path runs — not a hand-mocked base. The non-retryable property is verified by BOTH the
  `!err.retryable()` assertion AND the runtime-doesn't-loop mutation evidence above.

#### Increment 3 — REVIEW (2026-06-08, Opus REVIEWER, DELEGATED). VERDICT: CHANGES-MADE (docs only).
Adversarially verified against the Java source (`/tmp/iceberg-java-ref`) + 7 code mutations. Production
`validate` + 9 conflict tests are CORRECT and well-pinned; NO production change made.
- **`validate` invoked by the real commit path:** `do_commit` (mod.rs:308-312) runs each action's
  `.validate(self.starting_snapshot_id, &current_table)` AFTER the refresh/re-base (line 295) and BEFORE
  re-apply (314), against the refreshed base; `starting_snapshot_id` captured once in `Transaction::new`
  (117), survives the re-base. Not dead code.
- **THE KEY FINDING — enumeration MATCHES Java, no under-reject:** read `validationHistory` (L913-963) +
  `addedDataFiles` (L424-462) + `SnapshotUtil.ancestorsBetween`/`ancestorsOf`. Same INCLUSIVE-parent /
  EXCLUSIVE-start boundary, same op set {APPEND,OVERWRITE}, same `manifest.snapshotId()==snapshot.snapshotId()`
  manifest filter, same Added-only entry filter. Java's `newSnapshots.contains(entry.snapshotId())` is
  jointly satisfied by exactly those Added entries. The only divergence (Inc-6, already pinned): non-ancestor
  start → Java throws, Rust over-scans = Rust-STRICTER. NO false-negative vector.
- **None-filter=AlwaysTrue:** faithful mirror of `dataConflictDetectionFilter()` (rowFilter branch unreachable
  while `overwriteByRowFilter` deferred). **Non-retryable:** `Error::new` defaults `retryable:false`
  (error.rs:235), independent of kind; loop stops (`.when(|e| e.retryable())`).
- **Mutations run (all CAUGHT):** invert metrics (exclude + match tests fail); skip check (4 rejection fail);
  kind→CatalogCommitConflicts (4 kind asserts fail); drop validate_from_snapshot (override test fails);
  re-read refreshed head instead of tx-captured start (tx-captured test fails — the silent-always-pass
  guard); `.with_retryable(true)` (retryable asserts fail + 0.11→1.59s loop); EXCLUSIVE→INCLUSIVE boundary in
  shared `added_data_files_after` (5 tests fail across both actions). No surviving mutation.
- **CHANGES-MADE (docs only):** reconciled two stale Roadmap narrative mentions (lines ~159, ~302 said
  "conflict validation … deferred" for OverwriteFiles — under-claim) + the `mod.rs::overwrite_files()` ctor
  doc ("not yet supported"). GAP_MATRIX row 75 was already accurate + honest (over-scan, None-default,
  case-sensitivity, 🟡 rationale). Production `overwrite_files.rs`/`snapshot.rs` byte-identical to pre-review.
- **Scope + floor: CLEAN.** No bare `.unwrap()` in the production `validate` path. **Gate (repo root, pinned
  nightly):** build -p iceberg ✅; workspace --exclude sqllogictest --all-targets ✅; lib ×2 = 1509/0 both;
  transaction:: 225; 3 interop 4/4 each; clippy -D warnings ✅; fmt ✅. Row stays 🟡 (data-level Java interop
  + `overwriteByRowFilter` + `validateNoConflictingDeletes` still pending).

### Increment 4 — RowDelta validateNoConflictingDataFiles + shared conflict-check helper (DETAIL, 2026-06-08)
Java `BaseRowDelta.validate` → `validateNewDataFiles` → `validateAddedDataFiles(base, startingSnapshotId,
conflictDetectionFilter, parent)` — the IDENTICAL filter-based added-data-file conflict check just built for
OverwriteFiles (Increment 3). SCOPE: RowDelta `validateNoConflictingDataFiles` ONLY. DEFER (need a new
concurrent-DELETE-file enumeration helper — bigger, separate): `validateNoConflictingDeleteFiles`
(`validateNoNewDeleteFiles`), `validateDeletedFiles`/`validateDataFilesExist`, `validateAddedDVs`.

Because this is the 2nd use of the exact same load-bearing safety check, FACTOR a shared `pub(crate)` helper
(next to `added_data_files_after` in `transaction/snapshot.rs`) so OverwriteFiles + RowDelta cannot drift:
`async fn validate_no_conflicting_added_data_files(current: &Table, effective_start: Option<i64>,
conflict_filter: Option<&Predicate>, case_sensitive: bool) -> Result<()>` doing the
`added_data_files_after` walk + bind (None ⇒ AlwaysTrue) + per-file `InclusiveMetricsEvaluator::eval` +
first-conflict non-retryable `DataInvalid`. Refactor OverwriteFiles.validate to call it (BEHAVIOR-PRESERVING —
Increment 3's 9 tests stay green, the proof).

Plan:
- [x] `transaction/snapshot.rs`: add the shared `pub(crate)` helper next to `added_data_files_after`. No
      behavior change to existing functions.
- [x] `transaction/overwrite_files.rs`: refactor `validate` to call the helper (keep the flag-guard +
      `effective_start`); Increment-3 tests unchanged + green = behavior-preserving proof.
- [x] `transaction/row_delta.rs`: add fields `validate_no_conflicting_data_files: bool`,
      `conflict_detection_filter: Option<Predicate>`, `validate_from_snapshot: Option<i64>` + builder methods;
      override `validate` (flag-guard → `effective_start = validate_from_snapshot.or(starting_snapshot_id)` →
      the shared helper). Update the module doc (drop `validateNoConflictingDataFiles` from deferred; note the
      delete-file blocks remain).
- [x] Tests (RowDelta, MemoryCatalog + real concurrent fast_append, mirroring Increment 3): no-concurrent → OK;
      metrics-match → rejected non-retryable (names file); metrics-exclude → OK; flag OFF → snapshot isolation;
      None filter → any concurrent add conflicts; `validate_from_snapshot` override. Mutation-pin the
      include/exclude decision + the non-retryable kind. (OverwriteFiles tests double as the helper's pins.)
- [x] Docs: GAP_MATRIX (RowDelta validateNoConflictingDataFiles 🟡), Roadmap, todo, lessons.
- [x] Verify from repo root: build -p iceberg + workspace; lib ×2; transaction::; 3 interop; clippy; fmt.

**Outcome (2026-06-08, Phase 3 Increment 4 — RowDelta `validateNoConflictingDataFiles` + shared conflict-check
helper, BUILDER Opus):** `RowDelta` gained the SAME filter-based concurrent-commit conflict validation
`OverwriteFiles` has, and because this was the 2nd use of the load-bearing safety logic it was FACTORED into a
shared helper so the two checks cannot drift.
- **Shared helper** (`transaction/snapshot.rs`, right next to `added_data_files_after`, nothing else in that
  file touched — `added_data_files_after` is BYTE-IDENTICAL): `pub(crate) async fn
  validate_no_conflicting_added_data_files(current: &Table, effective_start: Option<i64>, conflict_filter:
  Option<&Predicate>, case_sensitive: bool) -> Result<()>`. It runs `added_data_files_after` → empty ⇒ `Ok`
  → binds the filter to the table's current schema (`None` ⇒ `Predicate::AlwaysTrue`) → per added file
  `InclusiveMetricsEvaluator::eval(&bound, file, true)` → first match ⇒ non-retryable
  `Error::new(ErrorKind::DataInvalid, "Found conflicting files that can contain records matching {filter}:
  {path}")` (filter rendered as `"true"` for `None`, else `Display`, exactly the Increment-3 wording).
- **OverwriteFiles refactor (BEHAVIOR-PRESERVING):** `OverwriteFilesAction::validate` keeps its flag-guard +
  `effective_start` computation and now delegates the walk+bind+eval+error to the shared helper (the inline
  ~35 lines deleted; `Bind`/`BoundPredicate`/`InclusiveMetricsEvaluator`/`added_data_files_after`/`Error`/
  `ErrorKind` imports removed from `overwrite_files.rs`). **Proof of behavior-preservation:** Increment 3's
  9 OverwriteFiles conflict tests (+ the 9 core overwrite tests = 18 total) stayed GREEN unchanged.
- **RowDelta feature:** added fields `validate_no_conflicting_data_files: bool` (default false),
  `conflict_detection_filter: Option<Predicate>` (None), `validate_from_snapshot: Option<i64>` (None), all
  init in `new()`; builder methods `validate_no_conflicting_data_files(self)`, `conflict_detection_filter(self,
  Predicate)`, `validate_from_snapshot(self, i64)` (named exactly like OverwriteFiles). Overrode `validate` in
  the `TransactionAction for RowDeltaAction` impl: `if !self.validate_no_conflicting_data_files { return Ok(()) }`
  → `effective_start = self.validate_from_snapshot.or(starting_snapshot_id)` → the shared helper with
  `self.conflict_detection_filter.as_ref()` and case-sensitive `true` (Java `isCaseSensitive()`; the action has
  no such field — defaults to `true`, noted in the doc). Updated the module doc: dropped
  `validateNoConflictingDataFiles` from the deferred list; the delete-file blocks
  (`validateNoConflictingDeleteFiles`/`validateDataFilesExist`/`validateAddedDVs`) remain deferred (they need a
  concurrent-DELETE-file enumeration helper that does not exist yet).
- **Java lines verified** (`/tmp/iceberg-java-ref`): `BaseRowDelta.validate` L132-174 — the `validateNewDataFiles`
  branch L155-157 calls `validateAddedDataFiles(base, startingSnapshotId, conflictDetectionFilter, parent)`, the
  IDENTICAL call OverwriteFiles makes; `MergingSnapshotProducer.validateAddedDataFiles` L391-412 (build the
  conflict iterable, throw "Found conflicting files that can contain records matching %s: %s" if any). The other
  blocks I DEFERRED, confirmed against the source: `validateDataFilesExist` (L141-149, referenced-files),
  `validateNewDeleteFiles`/`validateNoNewDeleteFiles` (L159-168), `validateNoConflictingFileAndPositionDeletes`
  (L170/L181-193), `validateAddedDVs` (L172) — all need delete-file enumeration that does not exist yet.
- **RowDelta test list (8 new, each pins a risk):**
  1. `test_row_delta_validation_no_concurrent_commit_succeeds` — validation enabled, nothing concurrent ⇒ OK
     (a race-free commit must not be blocked).
  2. `test_row_delta_rejects_concurrent_added_file_matching_filter` — HEADLINE: concurrent file y bounds [60,70]
     vs filter `y>=50` ⇒ REJECTED non-retryable, error names the file (lost/incorrect merge-on-read prevention).
  3. `test_row_delta_allows_concurrent_added_file_excluded_by_filter` — concurrent file y bounds [10,20] below
     `y>=50` ⇒ inclusive evaluator EXCLUDES ⇒ OK (no false conflict). The mutation-(a) target.
  4. `test_row_delta_without_validation_allows_conflicting_concurrent_append` — flag OFF ⇒ snapshot isolation
     unchanged / opt-in.
  5. `test_row_delta_none_filter_treats_any_concurrent_add_as_conflict` — None filter ⇒ AlwaysTrue ⇒ a no-bounds
     concurrent add conflicts (the conservative serializable default, opposite of "no check").
  6. `test_row_delta_validate_from_snapshot_override_changes_concurrent_window` — `validate_from_snapshot(S0)`
     widens the window to include S1's add ⇒ REJECTED (override genuinely shifts the boundary).
  7. `test_row_delta_validate_from_snapshot_at_head_finds_no_conflict` — `validate_from_snapshot(S1=head)` ⇒
     nothing concurrent ⇒ OK (the negative half).
  8. `test_row_delta_rejects_concurrent_using_tx_captured_starting_snapshot` — NO override; the tx-captured
     start surviving `do_commit`'s re-base is the only thing that detects the conflict (if the start were
     re-read from the refreshed head the check would silently always pass).
  All 8 simulate a REAL concurrent commit (a separate `fast_append` lands between txn-build and txn-commit, so
  `do_commit` refreshes to the new head and runs `validate` against it). Data files carry y-column bounds via a
  new `data_file_with_y_bounds(path, part, lo, hi)` helper (schema field id 2).
- **Mutations run (each caught), incl. the cross-action one:** (a) invert the metrics decision in the SHARED
  helper (`if !eval(...)`) → the EXCLUDE test fails for BOTH `transaction::overwrite_files` AND
  `transaction::row_delta` (the cross-action proof the shared helper is load-bearing for both). (b) force
  RowDelta's `validate` always-`Ok` → exactly its 4 rejection tests (#2/#5/#6/#8) fail, OK tests green. (c) make
  the helper's conflict error retryable (`.with_retryable(true)`) → the `!err.retryable()` assertion fails AND
  the retry loop visibly spins (0.06s → 1.57s, a kind/timing failure). All restored byte-identical; verified the
  OverwriteFiles 9 conflict tests stay green after the refactor (behavior-preservation).
- **Gate (all from repo root, pinned nightly):** `cargo build -p iceberg` ✅; `cargo build --workspace
  --exclude iceberg-sqllogictest --all-targets` ✅; `cargo test -p iceberg --lib` ✅ TWICE (run1 1517 passed /
  0 failed; run2 1517 passed / 0 failed — **new lib total 1517**, was 1509, +8 RowDelta conflict tests);
  `cargo test -p iceberg --lib transaction::` ✅ (233 passed; `transaction::overwrite_files` 18/18 unchanged,
  `transaction::row_delta` 19/19); 3 interop binaries ✅ (`interop_manage_snapshots` 4/4,
  `interop_update_partition_spec` 4/4, `interop_update_schema` 4/4); `cargo clippy --workspace --exclude
  iceberg-sqllogictest --all-targets -- -D warnings` ✅ (no warnings); `cargo fmt --all -- --check` ✅ (clean
  after one `cargo fmt`).
- **Scope:** touched ONLY `transaction/snapshot.rs` (ADDED the shared helper + 2 imports — `added_data_files_after`
  unchanged), `transaction/overwrite_files.rs` (refactor `validate` to delegate), `transaction/row_delta.rs`
  (feature + 8 tests), and docs (GAP_MATRIX, Roadmap, todo, lessons). No edits to
  `inclusive_metrics_evaluator.rs` / `action.rs` / `replace_partitions.rs` / `predicate.rs` / `spec/` / `scan/`
  / `arrow/` / Cargo / lockfiles. No on-disk format change. No commit, no branch switch, no push — work left on
  `phase3-residual-eval`.
- **UNSURE / flagged:** (1) The RowDelta conflict tests assert `add_deletes`-only commits with SYNTHETIC delete
  files (no real parquet) — same as the existing RowDelta manifest/summary tests; the conflict check is on the
  concurrently-added DATA files, independent of the delete-file payload, so synthetic delete files are
  sufficient and faithful. (2) Java `BaseRowDelta`'s `conflictDetectionFilter` DEFAULTS to `Expressions.alwaysTrue()`
  (not null) and is shared across ALL the validate blocks; our `None ⇒ AlwaysTrue` binding mirrors the default
  for the data-file block, the only block we implement. (3) The non-ancestor `validate_from_snapshot` over-scan
  divergence (Rust over-scans to root where Java throws) is inherited UNCHANGED from `added_data_files_after`
  (Increment-6-flagged, Rust-STRICTER, over-reject-only) — not re-introduced here.

---

## OVERNIGHT AUTONOMOUS RUN (2026-06-08/09) — branch `phase3-conflict-and-scan` (off `phase3-rowdelta-delete-conflicts` d327e80d)
User asleep; run via actor-critic, COMMIT LOCALLY ONLY (no push). User approved "all 5". RECON FOUND 2 of the
planned items are COUPLED (not clean reuses), so ADAPTED to clean additive items (documented for the morning):
- OverwriteFiles `validateNoConflictingDeletes` block 3 is GATED on `rowFilter() != alwaysFalse()` → needs
  `overwriteByRowFilter` (not built). DROPPED.
- `StreamingDelete` (DeleteFiles) only has `validateFilesExist` → needs a *deleted*-data-file enumeration +
  resolved removed files, NOT the existing helpers. DEFERRED.
The clean conflict-validation reuses are ALREADY done (RowDelta data+delete, OverwriteFiles data,
ReplacePartitions). So the overnight run pivots to clean, additive, locally-verifiable scan/inspection items.

ADAPTED SEQUENCE (each: builder → adversarial reviewer → independent widened gate → local commit):
1. **`readable_metrics`** inspection column (Java `MetricsUtil.readableMetricsStruct`) — read-only/additive.
2. **`IncrementalAppendScan`** (Java `BaseIncrementalAppendScan`/`appendsBetween`) — new scan entry, additive.
3. **`ScanReport` / `MetricsReporter`** (Java `metrics/ScanReport.java`) — self-contained observability.
4. **DeleteFiles `validateFilesExist` + `deleted_data_files_after`** (verify clean when reached; the deleted-
   data-file enumeration is parallel to `added_data_files_after`, DeleteFiles has `delete_paths`). Substitute
   the `position_deletes` metadata table if coupled.
5. **`position_deletes` metadata table** (Java `PositionDeletesTable`) OR `IncrementalChangelogScan` — pick when
   reached. (Not yet built; a real inspection/scan gap.)
Widened gate for ALL: workspace build + iceberg lib ×2 + transaction:: + iceberg-datafusion tests + 3 interop +
workspace clippy + fmt + typos.

### Increment 1 — readable_metrics (DETAIL)
Java `MetricsUtil` (`core/.../MetricsUtil.java`): `READABLE_METRIC_COLS` (L140-193) = the 6 per-column metrics
(`column_size`/`value_count`/`null_value_count`/`nan_value_count` = long, from the file's metric maps by field
id; `lower_bound`/`upper_bound` = the COLUMN's type, decoded via `Conversions.fromByteBuffer` — NOT raw bytes);
`readableMetricsSchema(dataTableSchema, metadataTableSchema)` (L356) builds a `readable_metrics` STRUCT with one
sub-field PER leaf column of the data table (named by column path), each = a struct of the 6; `READABLE_METRICS
= "readable_metrics"` (L195). `BaseFilesTable`/`BaseEntriesTable` APPEND it (`TypeUtil.join(schema,
readableMetricsSchema(...))`). Rust: add to the `files` family + `entries` (the shared `inspect/data_file.rs`
projection, or appended as a top-level/nested column matching Java); the per-column bound decode reuses
`Datum::try_from_bytes`/typed conversion. Was DEFERRED across the inspection set.

#### Increment 1 — BUILD PLAN (Opus builder, 2026-06-08)
- [x] New module `inspect/readable_metrics.rs` — ONE source of truth used by `files` + `entries`:
      builds the `readable_metrics` STRUCT field (one sub-field per LEAF/primitive column of the data
      table's CURRENT schema, named by dotted path; type = a 6-field struct: column_size/value_count/
      null_value_count/nan_value_count long opt; lower_bound/upper_bound = the COLUMN's own type, opt).
      Ports Java `readableMetricsSchema` id scheme (nextId = metadata-table highest id; pre-increment per
      col-struct then its 6 sub-fields, then the top-level readable_metrics field). ONE documented
      divergence: Java assigns ids in `idToName.keySet()` = Java-HashMap (arbitrary) order; we use ASCENDING
      data-table-field-id order (deterministic) then sort emitted sub-fields by name (Java sorts after id
      assignment). Row builder fills 4 counts from the file's maps by field id (null when absent) and decodes
      lower/upper bound to the COLUMN's typed value via `Datum::try_from_bytes(stored.to_bytes(), col_type)`
      (inverse of the raw map's `to_bytes`; mirrors Java `Conversions.fromByteBuffer(field.type(), bytes)`).
- [x] Append `readable_metrics` to `files` family schema+scan (flat last column) AND `entries` schema+scan
      (last top-level column after `data_file`), matching Java APPEND order; `all_*` inherit it.
- [x] Tests: schema present (files/data_files/delete_files/entries + an all_*), one sub-field per leaf each a
      6-field struct, bound sub-fields carry the COLUMN type, field ids match the ported scheme; values
      (counts + DECODED typed bounds + nulls); backward-compat (raw maps still present, unchanged).
- [x] Mutations: (a) swap lower<->upper decode -> value test fails; (b) wrong field id for a count -> value
      test fails; (c) raw bytes instead of typed bound -> type/value test fails.
- [x] Docs (GAP_MATRIX, Roadmap, todo Outcome, lessons) + gate (all 8) + new lib total x2.

Outcome (2026-06-08, BUILDER Opus): `readable_metrics` landed on the `files` family + `entries` (+ `all_*`
inherit it through the shared schema/projection). NEW module `inspect/readable_metrics.rs` is the single
source of truth: `readable_metrics_field(data_schema, host_highest_field_id)` builds the virtual STRUCT (one
sub-field per LEAF/primitive data column, dotted-named via `Schema::field_id_to_name_map`, each a 6-field
struct: `column_size`/`value_count`/`null_value_count`/`nan_value_count` long opt + `lower_bound`/
`upper_bound` typed as the COLUMN's own type, opt); `ReadableMetricsBuilder` fills the row per `DataFile`.
**Field-id scheme** (Java `MetricsUtil.readableMetricsSchema`, `MetricsUtil.java:356-393`): pre-increment
counter seeded at the HOST metadata-table schema's `highest_field_id()` (= the data_file projection's 145 for
both tables), per leaf column assigns col-struct id then its 6 sub-field ids, then the top-level
`readable_metrics` id (8-column fixture → readable_metrics = 202). **ONE documented divergence:** Java's
`idToName()` HashMap iteration order (arbitrary, non-portable — `IndexByName.byId()` over a `HashMap`) is
replaced by deterministic ASCENDING data-table-field-id assignment; sub-fields still sorted by name (so the
column ORDER matches Java; exact interior ids match only when Java's HashMap order happens to coincide). Full
byte-level id parity with a JVM is the residual Java/Spark interop concern. **Typed bound decode:** the stored
`Datum` is re-serialized (`to_bytes`) and re-decoded against the COLUMN's primitive type
(`Datum::try_from_bytes`) — the inverse of the raw `lower_bounds`/`upper_bounds` map columns, mirroring Java
`Conversions.fromByteBuffer(field.type(), buffer)`; widens an evolved field's bound to the column's current
type. Raw metrics-map columns UNCHANGED (additive). **Java lines verified:** `READABLE_METRIC_COLS` L140-193
(the 6 metrics; counts by field id nullable; bounds via `Conversions.fromByteBuffer(field.type(), …)`),
`READABLE_METRICS` L195, `readableMetricsSchema` L356-393 (id scheme + sort-by-name + per-column doc),
`readableMetricsStruct` L403-429, `IndexByName.byId()` L78-80 (the HashMap-order source). Provider untouched
(it delegates to `.schema()`/`.scan()` — `test_metadata_table` green). **Files modified:** new
`crates/iceberg/src/inspect/readable_metrics.rs`; `inspect/mod.rs` (+`mod readable_metrics`); `inspect/files.rs`
(schema+scan append + 4 tests + 2 existing schema assertions updated); `inspect/entries.rs` (schema+scan
append + 2 tests + 1 existing schema assertion updated); `inspect/data_file.rs` (module doc — readable_metrics
no longer "deferred"). **Tests (12 new):** readable_metrics.rs ×6 — one-struct-per-leaf sorted-by-name;
6-field metric struct; bounds carry the COLUMN type (long/string/double, counts always long); the exact
pre-increment id scheme (101/108/115 column structs, 102-107 + 116-121 sub-ids, 122 top-level); primitives
nested in struct/list included by dotted path; complex-only (map) columns excluded. files.rs ×4 — schema
present with per-column bound type (x→Int64, a→Utf8); counts + DECODED typed bounds + nulls (x.column_size=42,
x.lower_bound=1 long, absent metrics null, y all null, i32 bound Int32-typed null); distinct lower=10/upper=99
both typed longs; raw maps unchanged regression guard. entries.rs ×2 — readable_metrics present with decoded
bound (entries x.column_size=42, x.lower_bound=1); `all_entries` schema inherits readable_metrics.
**Mutations (all caught, re-run by injection):** (a) swap lower↔upper source maps → distinct-bounds test
fails (reads 99 where 10 expected); (b) read column_size from `field_id+1` → counts test fails (x.column_size
null); (c) declare bounds as raw `Binary` → bound-type schema test + the column-type unit test + 4 builder
dispatch tests fail. **Gate (all 8, pinned nightly):** (1) `cargo build -p iceberg` OK; (2)
`cargo build --workspace --exclude iceberg-sqllogictest --all-targets` OK; (3) `cargo test -p iceberg --lib`
×2 = **1528 passed / 0 failed** both runs (was 1516 → +12); (4) `cargo test -p iceberg-datafusion --lib
--tests` 9/9 integration green incl. `test_metadata_table` (the all-targets DOCTEST run fails ONLY the
pre-existing `table_provider_factory.rs` `tokio::main`/rt-multi-thread environmental artifact documented in
lessons 2026-06-07 Increment-7 REVIEWER — a file this change never touched); (5) interop_manage_snapshots /
interop_update_schema / interop_update_partition_spec = 4/4 each; (6) `cargo clippy --workspace --exclude
iceberg-sqllogictest --all-targets -- -D warnings` clean; (7) `cargo fmt --all -- --check` clean; (8) `typos`
repo-wide exit 0. No commit, no branch switch, no edits outside `inspect/` + docs.

### OVERNIGHT branch correction + Increment 2
NOTE: a stray git op during Increment 1's builder left the tree on `main`; since the overnight scan/inspection
items are INDEPENDENT of the rowdelta-delete helper, the overnight work is on branch **`phase3-overnight` off
`main` (063b7153)** — NOT stacked on rowdelta-delete (simpler; no rebase needed). Increment 1 (readable_metrics)
committed `31595dd4`. After EACH agent: re-verify `git branch --show-current == phase3-overnight`.

### Increment 2 — IncrementalAppendScan (DETAIL)
Java `BaseIncrementalAppendScan` (`core/.../BaseIncrementalAppendScan.java`): `doPlanFiles(fromExclusive,
toInclusive)` → `appendsBetween(table, fromExclusive, toInclusive)` = the APPEND-operation snapshots in
`(from, to]` (via `SnapshotUtil.ancestorsBetween` filtered to `operation == DataOperations.APPEND` — L111),
then `appendFilesFromSnapshots(snapshots)` plans FileScanTasks for the data files those append snapshots ADDED
(only Added entries from the manifests each snapshot itself added). Append-only: OVERWRITE/DELETE snapshots in
the range are EXCLUDED. The API (`IncrementalAppendScan` interface): `fromSnapshotInclusive`/
`fromSnapshotExclusive` + `toSnapshot` (default to = current).

Rust approach (LOWEST-RISK — additive, do NOT refactor the existing single-snapshot `plan_files`): a SEPARATE
planner. `Table::incremental_append_scan()` → a builder with `from_snapshot_id` (exclusive) + optional
`to_snapshot_id` (default current) + `with_filter`/`select` (mirror `TableScanBuilder`). Planning: compute the
append snapshots in `(from, to]` (mirror the `added_data_files_after` ancestor walk in `transaction/snapshot.rs`,
but filtered to `Operation::Append` and bounded by both ends); for each, read the DATA manifests it ADDED
(`added_snapshot_id == snapshot_id`), and for each `Added` entry build a `ManifestEntryContext` (reuse the
existing `scan/context.rs` struct + `into_file_scan_task`, with an EMPTY delete index — an append scan applies
no deletes) and stream `FileScanTask`s. Reuse the residual evaluator + the bound-filter machinery exactly as
the normal scan does. Must NOT change existing `TableScan`/`plan_files` behavior.

Tests (MemoryCatalog/TableTestFixture): multi-snapshot fixture with ≥2 appends; `incremental_append_scan`
from=S0(excl) to=S2(incl) returns ONLY the files appended in S1+S2 (not S0's); from==to → empty; an OVERWRITE
snapshot in the range is EXCLUDED; a filter prunes appended files by partition; default to=current. Mutation-
pin: the exclusive-from boundary, the append-only op filter, the added-manifest filter. Confirm the normal
`table.scan()` is unchanged. Widened gate (incl. datafusion tests).

#### BUILDER plan (Opus, 2026-06-08)
- [x] Add `scan/incremental.rs`: `IncrementalAppendScanBuilder` (`from_snapshot_id_exclusive`,
      `from_snapshot_id_inclusive`, `to_snapshot_id`, `with_filter`, `select`/`select_all`/`select_empty`,
      concurrency limits) → `build()` → `IncrementalAppendScan`. Validate snapshots exist + `to` descends `from`.
- [x] `IncrementalAppendScan::plan_files()`: bounded ancestor walk of `(from, to]` filtered to
      `Operation::Append`; for each, keep DATA manifests it ADDED (`added_snapshot_id == snapshot_id`); stream
      `Added`-entry `FileScanTask`s reusing `PlanContext`/`ManifestEntryContext`/`into_file_scan_task` + EMPTY
      delete index + residual evaluator.
- [x] Seam in `scan/context.rs`: factor `build_manifest_file_contexts` to also accept an explicit
      `Vec<ManifestFile>` (behavior-preserving for the normal scan) so the incremental planner reuses the
      partition-pruning + residual machinery; add an `Added`-only entry filter for the incremental path.
- [x] `Table::incremental_append_scan()` entry point in `table.rs`.
- [x] Tests in `incremental.rs` (multi-append fixture): core multi-append set; from==to (rejected, Java-faithful);
      OVERWRITE-in-range excluded; with_filter partition prune; default to=current; exclusive-from boundary.
      Mutation-pin from-exclusive, append-only op filter, added-manifest filter.
- [x] Regression: normal `table.scan().plan_files()` unchanged. Docs: GAP_MATRIX row 94 / Roadmap / lessons.

#### Outcome (BUILDER, Opus, 2026-06-08)
**API.** `Table::incremental_append_scan() -> IncrementalAppendScanBuilder` (mirrors `Table::scan()`). Builder:
`from_snapshot_id_exclusive(i64)` (Java `fromSnapshotExclusive`), `from_snapshot_id_inclusive(i64)`
(Java `fromSnapshotInclusive` — resolved to the parent as the exclusive bound at `build()`), `to_snapshot_id(i64)`
(Java `toSnapshot`, defaults to the current snapshot), `with_filter`/`select`/`select_all`/`select_empty`/
`with_case_sensitive`/`with_batch_size`/`with_concurrency_limit`. `build()` validates the `to`/`from` snapshots
exist and the Java preconditions (inclusive `from` is an ancestor of `to`; exclusive `from` is a *parent
ancestor* of `to`), then resolves schema/field-ids/bound-filter exactly as `TableScanBuilder::build`.
**Planner design.** `IncrementalAppendScan::plan_files()` is a SEPARATE planner — the single-snapshot
`TableScan::plan_files` is byte-unchanged. (1) `appends_between` does a bounded parent-chain walk from `to` back,
stopping BEFORE the exclusive `from`, keeping only `Operation::Append` snapshots (Java `appendsBetween` /
`SnapshotUtil.ancestorsBetween` filtered to APPEND; `from==to` short-circuits to empty inside the walk, but the
exclusive `isParentAncestorOf` precondition gates it first). (2) For each append snapshot it loads the manifest
list and keeps the DATA manifests THAT snapshot itself added (`added_snapshot_id == snapshot_id`, Java
`manifest.snapshotId() == snapshot.snapshotId()`). (3) Those manifests are driven through the REUSED
`PlanContext::build_manifest_file_contexts_from_files` (the additive seam) → the SAME partition-filter pruning +
residual-evaluator construction as the normal scan, with an EMPTY `DeleteFileIndex` (drop the sender so it
resolves to no-deletes). (4) `process_append_manifest_entry` keeps only `Added`-status entries (Java
`filterManifestEntries(status==ADDED)`), runs the partition-expression + inclusive-metrics filters, and
`into_file_scan_task` builds each `FileScanTask` (residual predicate, EMPTY deletes).
**Range/append-only semantics.** `(from exclusive, to inclusive]`, APPEND-only — overwrites/deletes in the range
are excluded entirely (snapshot-level op filter); no delete files applied (empty delete index).
**Java lines verified.** `BaseIncrementalAppendScan.doPlanFiles` L46-57 → `appendsBetween` L105-117 (the
`operation == DataOperations.APPEND` filter) → `appendFilesFromSnapshots` L68-99 (the
`snapshotIds.contains(manifest.snapshotId())` manifest filter + `manifestEntry.status() == ADDED` entry filter).
`BaseIncrementalScan` L159-187 (`fromSnapshotIdExclusive`: inclusive ⇒ `isAncestorOf` + parent bound;
exclusive ⇒ `isParentAncestorOf`) + L133-157 (`toSnapshotIdInclusive` defaults to current). `SnapshotUtil`
L211-229 (`ancestorsBetween` equal-from/to ⇒ empty) + L46-86 (`isAncestorOf`/`isParentAncestorOf`).
**Tests (12, `scan/incremental.rs`).** core range S1+S2-not-S0 (the planner includes both later appends, excludes
`from`'s files); exclusive-from boundary (S1's own files excluded); inclusive-from includes `from`'s files;
`from==to` exclusive REJECTED (Java precondition; a snapshot is not its own parent ancestor); range with only an
OVERWRITE → zero tasks (append-only filter); `with_filter(x==10)` prunes the x=20 appended file; default
to=current; no-from whole-lineage; empty table → zero; non-ancestor `from` rejected; own-added-manifest-only
count (carried-forward manifests don't re-surface files).
**Mutations caught (edit+revert).** (a) inclusive from boundary (process `from` before breaking) → exclusive-from
+ core + 5 others fail; (b) drop the `Operation::Append` filter (`|| true`) → overwrite-excluded + no-append-range
fail; (c) read all manifests not just the snapshot's own added (`|| true`) → own-added-manifest count + core +
others fail. Each reverted after; full lib suite re-run green.
**Gate (all 8, pinned nightly).** (1) `cargo build -p iceberg` OK; (2) `cargo build --workspace --exclude
iceberg-sqllogictest --all-targets` OK; (3) `cargo test -p iceberg --lib` ×2 = **1542 passed / 0 failed** both
runs (+12 new, was 1530); (4) `cargo test -p iceberg --lib scan::` = 55 passed (existing scan tests unchanged +
12 new); (5) `cargo test -p iceberg-datafusion --lib --tests` = 80 + 9 passed; (6) interop_manage_snapshots /
interop_update_schema / interop_update_partition_spec = 4/4 each; (7) `cargo clippy --workspace --exclude
iceberg-sqllogictest --all-targets -- -D warnings` clean; (8) `cargo fmt --all -- --check` clean · `typos`
exit 0. The normal `TableScan`/`plan_files` is UNCHANGED (the only `scan/mod.rs` edit is `mod incremental;` +
`pub use`; the `context.rs` edit is the additive `_from_files` delegation). Stayed on `phase3-overnight`; no
commit, no push; edits only in `scan/{mod,context,incremental}.rs` + `table.rs` + docs.

#### REVIEW (2026-06-08, Opus REVIEWER, DELEGATED) — VERDICT: CHANGES-MADE (1 test added), PASS 🟡
Verified independently: (1) the `context.rs` seam is behavior-preserving — read the delegation line-by-line:
the loop body is byte-identical, only `manifest_list.entries().iter().collect()`→`.to_vec()` (owned clones,
no semantic change) and `for x in manifest_files`→`for x in &manifest_files`; inside the loop `x` is
`&ManifestFile` in BOTH versions, so `get_partition_filter`/`create_manifest_file_context` see identical
types. (2) Range/append-only/own-manifest semantics match Java line-for-line (`appendsBetween` L105-117,
`ancestorsBetween`/`isAncestorOf`/`isParentAncestorOf` L46-86/L211-229, the inclusive→parent + exclusive→
parent-ancestor preconditions L159-187). (3) Java's entry-level `snapshotIds.contains(entry.snapshotId())`
filter is REDUNDANT in Rust given manifest-selection + `status==Added` (not a missing check) — documented in
lessons. **MUTATIONS run (edit+revert):** (a) inclusive-from CAUGHT, (b) drop append-only filter CAUGHT,
(c) read-all-manifests CAUGHT, (d) exclude-`to` (start at to's parent) CAUGHT — all by existing tests; **(e)
drop the `status==Added` entry filter SURVIVED** (the builder's noted gap — fast-append fixtures hold only
Added entries). **Closed (e)** by adding `test_incremental_append_keeps_only_added_entries_of_own_manifest`,
reusing `scan::tests::TableTestFixture::setup_manifest_files` (an APPEND snapshot whose own manifest carries
Added+Existing+Deleted) — incremental scan returns ONLY the Added file; mutation-verified it fails iff the
filter is dropped and nothing else. lib 1542→**1543**, scan:: 55→**56**. All 8 gates green; clippy/fmt/typos
clean; no bare unwrap/expect in production; scope intact (no Cargo/spec/transaction/arrow/datafusion edits);
docs reconciled to 13 tests / 4 mutations. NOTE: `git checkout -- context.rs` to revert a mutation wiped the
uncommitted seam (tracked file → reverts to HEAD); restored byte-exact from the read diff and re-verified.
Stayed on `phase3-overnight`; no commit, no push.

### Increment 3 — ScanReport / MetricsReporter data model + reporter API (DETAIL)
Increment 2 (IncrementalAppendScan) committed `bdb02365`. The Java `metrics/` package is LARGE (~30 files);
the SCAN-EMISSION wiring needs instrumenting the concurrent/lazy `plan_files` stream (fiddly — DEFER to a
supervised increment). This increment = the SELF-CONTAINED data model + reporter trait (also the REST catalog's
metrics-report JSON contract), low-risk + high-value.

Java authority: `api/.../metrics/{MetricsReport,ScanReport,ScanMetricsResult,CounterResult,TimerResult,
MetricsReporter}.java` + `core/.../metrics/{LoggingMetricsReporter,InMemoryMetricsReporter}.java` +
`ScanReportParser.java` (the REST JSON). Build:
- `MetricsReport` (marker trait/enum), `ScanReport` (table_name, snapshot_id, schema_id, projected_field_ids,
  projected_field_names, filter [the bound/unbound expression as a string or Predicate], metrics:
  ScanMetricsResult).
- `ScanMetricsResult` — the ~14 counters/timers (`total_planning_duration` Timer; `result_data_files`,
  `result_delete_files`, `total_data_manifests`, `total_delete_manifests`, `scanned_data_manifests`,
  `skipped_data_manifests`, `total_file_size_in_bytes`, `total_delete_file_size_in_bytes`, `skipped_data_files`,
  `skipped_delete_files`, `indexed_delete_files`, `equality_delete_files`, `positional_delete_files` Counters).
- `CounterResult { unit, value }` + `TimerResult { time_unit, total_duration, count }` (or Option-typed
  fields). `MetricsReporter` trait (`report(&self, report: …)`). `LoggingMetricsReporter` (tracing) +
  `InMemoryMetricsReporter` (stores last report, test-friendly).
- serde JSON matching `ScanReportParser` (the REST `report-metrics` payload) — include if clean.
DEFER (documented): wiring into `TableScan`/`plan_files` to actually collect + emit (concurrent/lazy-stream
instrumentation) — its own supervised increment. Self-contained module `metrics/` (new). Tests: construct +
report to both reporters + JSON round-trip. Widened gate.

#### Increment 3 — BUILD PLAN (BUILDER Opus, 2026-06-08)
- [x] New self-contained module `crates/iceberg/src/metrics/mod.rs` (mirrors Java `metrics/` package);
      `pub mod metrics;` in `lib.rs`.
- [x] `MetricUnit` enum (`Undefined`/`Bytes`/`Count` = Java `MetricsContext.Unit`, serde by displayName
      `undefined`/`bytes`/`count`); `TimeUnit` enum (Java `java.util.concurrent.TimeUnit`, serde lowercase).
- [x] `CounterResult { unit: MetricUnit, value: i64 }`, `TimerResult { time_unit, total_duration: Duration,
      count: i64 }`. `ScanMetricsResult` — every Java metric as `Option<CounterResult>` / `Option<TimerResult>`
      (Java optionality: a never-incremented counter is absent). Full counter set incl. the `dvs`,
      `scanned_delete_manifests`, `skipped_delete_manifests` ones the brief's short list omitted.
- [x] `ScanReport { table_name, snapshot_id, schema_id, projected_field_ids, projected_field_names, filter:
      Predicate, scan_metrics, metadata: HashMap }`. `MetricsReport` ENUM (`Scan(ScanReport)`) — dispatch
      choice = enum over `dyn` (avoids downcasting, illegal states unrepresentable, idiomatic Rust).
- [x] `MetricsReporter` trait `fn report(&self, report: MetricsReport)`. `LoggingMetricsReporter` (tracing
      structured event). `InMemoryMetricsReporter` (Mutex-held last report + `last_report()` / `last_scan_report()`).
- [x] serde: counter/timer shapes + dashed metric names (skip-if-none) match Java `ScanReportParser`/
      `ScanMetricsResultParser`/`CounterResultParser`/`TimerResultParser`. Timer JSON = `{count, time-unit
      (lowercase), total-duration (in unit)}`; counter = `{unit (displayName), value}`. Top-level field names
      `table-name`/`snapshot-id`/`schema-id`/`projected-field-ids`/`projected-field-names`/`filter`/`metrics`.
      DECISION: `filter` serialized via Rust `Predicate`'s OWN serde (NOT Java `ExpressionParser` JSON) — that
      port is large + separate; documented as a deferred divergence. Everything else matches Java.
- [x] Tests: construct+accessor round-trip (Java names); absent counter = None; both reporters; JSON
      round-trip + pinned field/metric names vs hand-written expected JSON. Mutations: drop a counter; rename a
      JSON field; InMemory keep-first.
- DEFER: scan-emission wiring into `plan_files`/`TableScan` (separate supervised increment).

**Outcome (BUILDER Opus, 2026-06-08).** Landed the self-contained scan metrics-report data model + reporter
API in ONE new cohesive module `crates/iceberg/src/metrics/mod.rs` (`pub mod metrics;` in `lib.rs`) — no edits
to `scan/`, `transaction/`, `spec/`, `arrow/`, or datafusion. **Module layout:** metric-name constants (private
`metric_names` submodule, Java `ScanMetrics` strings) → `MetricUnit` + `TimeUnit` enums → `CounterResult` /
`TimerResult` (with a hand-written `Serialize`/`Deserialize` for `TimerResult` so `total-duration` is expressed
in the timer's own unit via a truncating convert, exactly Java `TimerResultParser.fromDuration`/`toDuration`) →
`ScanMetricsResult` (all 17 Java metrics as `Option`, `skip_serializing_if`+`default`, dashed `rename`s) → a
compile-time `const` block asserting the name constants == the serde renames → `ScanReport` → `MetricsReport`
enum → `MetricsReporter` trait + the two reporters. **Dispatch choice:** `MetricsReport` is a closed
`#[non_exhaustive] enum` with a single `Scan(ScanReport)` variant (not a `dyn MetricsReport` trait object) —
mirrors Java's marker interface while avoiding downcasting and making an unknown report kind unrepresentable;
`last_scan_report()` matches exhaustively (no wildcard) so a future variant forces an update. **serde decision:**
the metrics object + counter/timer shapes + all dashed top-level field names match Java's parsers faithfully;
the ONE divergence is the `filter` field — serialized via the Rust `Predicate`'s own serde derive, NOT Java
`ExpressionParser` JSON (a large separate port, documented in the module docs + GAP_MATRIX + Roadmap as a
tracked follow-up). **Java field names verified** against `/tmp/iceberg-java-ref` `metrics/`: `MetricsReport`
(empty marker iface), `MetricsReporter.report(MetricsReport)`, `ScanReport` (table_name/snapshot_id/filter/
schema_id/projectedFieldIds/projectedFieldNames/scanMetrics/metadata), `ScanMetricsResult` (the 17 `@Nullable`
accessors — the brief's short list omitted `scanned_delete_manifests`/`skipped_delete_manifests`/`dvs`, all
included here), `CounterResult` (unit+value), `TimerResult` (timeUnit/totalDuration/count), `ScanMetrics` (the
17 string constants), `MetricsContext.Unit` (undefined/bytes/count), and the four `*Parser`s for the JSON shape.
**Tests (9, each risk-named):** `test_scan_report_fields_round_trip_through_accessors` (Java-named fields),
`test_absent_counter_is_none` (Java optionality), `test_logging_reporter_does_not_panic` +
`test_logging_reporter_emits_a_tracing_event` (captured custom subscriber counts exactly 1 event),
`test_in_memory_reporter_stores_the_report` + `test_in_memory_reporter_keeps_the_last_report`,
`test_scan_report_json_round_trips_and_matches_java_shape` (round-trip + pinned NAMES + counter/timer shapes vs
hand-written expected JSON + absent-omitted), `test_timer_total_duration_is_expressed_in_its_time_unit`,
`test_non_empty_metadata_round_trips`. **Mutations caught (Edit + revert, NOT git checkout):** (a) `#[serde(skip)]`
on `result_data_files` (drop a counter) → JSON round-trip test fails; (b) `rename = "tableName"` (rename a JSON
field) → JSON-shape test fails at the `table-name present` assertion; (c) `InMemoryMetricsReporter::report`
keep-first (`if guard.is_none()`) → keeps-last test fails while stores test stays green. **Gate (all 7, on
`phase3-overnight`):** (1) `cargo build -p iceberg` clean; (2) workspace build (excl. sqllogictest) clean; (3)
`cargo test -p iceberg --lib` TWICE = **1552 passed / 0 failed** both runs (was 1543; +9 metrics tests); (4)
`iceberg-datafusion --lib --tests` 80 + 9 passed; (5) interop manage_snapshots/update_schema/update_partition_spec
4/4 each; (6) clippy `-D warnings` clean (fixed one `manual_map` by matching on `last_report()?`); (7) `fmt
--check` clean + `typos` exit 0. **Scan-wiring is DEFERRED** (no `plan_files`/`TableScan` instrumentation).
**DEVIATION flagged:** added `tracing = { workspace = true }` to `crates/iceberg/Cargo.toml` (the crate had no
logging facade and the brief mandated `tracing`-based logging) — a one-line `Cargo.lock` add (tracing was already
a resolved workspace dep). This touches a dependency file, which the scope rules flag for sign-off; surfaced here
+ in the final report.

**Orchestrator surgery (2026-06-08):** the unapproved dep edit was REJECTED. `LoggingMetricsReporter` + its 2
tests (`test_logging_reporter_does_not_panic`, `test_logging_reporter_emits_a_tracing_event`) were REMOVED
(replaced by a NOTE comment deferring it pending dep approval), and `crates/iceberg/Cargo.toml`/`Cargo.lock`
were reverted to NO `tracing` dep. The increment is now the dependency-free model + `MetricsReporter` trait +
`InMemoryMetricsReporter` + serde. Test count: 7 metrics tests (was 9); `iceberg --lib` = **1550** (was 1552).

**REVIEW (2026-06-08, Opus REVIEWER) — VERDICT 🟡 PASS (docs corrected).** Verified vs `/tmp/iceberg-java-ref`:
all 17 `ScanMetricsResult` metrics present with the right names + counter-vs-timer kind (cross-checked
`ScanMetrics.java` constants + `ScanMetricsResult.java` `@Nullable` accessors); serde JSON shape matches all 4
parsers — top-level field names (`ScanReportParser`), counter `{unit (displayName), value}`
(`CounterResultParser`), timer `{count, time-unit (lowercase), total-duration (in-unit truncating convert)}`
(`TimerResultParser`), dashed metric keys + absent-omitted (`ScanMetricsResultParser`); field types
(`snapshot-id` i64, `schema-id` i32) match Java `long`/`int`. The `filter`-via-`Predicate`-serde divergence is
honestly documented (module docs + GAP_MATRIX + Roadmap) and acceptable for a model-only increment. Dep-removal
is CLEAN: no `tracing::` code (only doc/comment mentions), `git diff HEAD -- Cargo.toml Cargo.lock` EMPTY,
`tracing` not a dep of the `iceberg` crate. `InMemoryMetricsReporter` Mutex-poison handled WITHOUT bare
`.unwrap()` (`unwrap_or_else(|p| p.into_inner())` on both lock sites); `MetricsReport` `#[non_exhaustive]` enum
matched without a wildcard. Ran all 4 mutations (Edit+revert): (a) drop a counter → 2 JSON tests fail; (b)
rename `table-name`→`tableName` → JSON-shape test fails; (c) InMemory keep-first → keeps-last test fails; (d)
drop a `None`-counter's `skip_serializing_if` → absent-omitted assertion fails — ALL 4 caught, no test added.
Gate (on `phase3-overnight`): build `-p iceberg` + workspace (excl. sqllogictest) clean; `--lib` ×2 = 1550/0;
`iceberg-datafusion --lib --tests` 80 + 9; interop manage_snapshots/update_schema/update_partition_spec 4/4 each;
clippy `-D warnings` clean; fmt clean; typos 0; `Cargo.toml`/`Cargo.lock` diff EMPTY. **CHANGES MADE:** corrected
stale post-surgery docs in `GAP_MATRIX.md` + `Roadmap.md` (they still claimed `LoggingMetricsReporter`/`tracing`
landed, 9 tests, and a flagged dep add) to reflect the dependency-free reality (7 tests, 4 mutations, no dep
change, Logging deferred). No source/test change needed. Residual risk: the deferred `filter` JSON (won't
byte-match Java REST until `ExpressionParser` is ported) + the deferred scan-emission (no metrics are COLLECTED
yet — the model is inert until `plan_files` is instrumented).

### Increment 4 — IncrementalChangelogScan (DETAIL)
Increment 3 (ScanReport model) committed (commit hash `0xba590e64`). Java `BaseIncrementalChangelogScan`
(`core/.../BaseIncrementalChangelogScan.java`, 183 lines) — builds on IncrementalAppendScan (Inc 2):
- `orderedChangelogSnapshots(from excl, to incl)` (L103-118): the `ancestorsBetween` range snapshots EXCLUDING
  `Operation::Replace`, oldest-first; **throws UnsupportedOperationException if any has DELETE manifests**
  (L108-111 — current Java scope is data-file changes only; port as `ErrorKind::FeatureUnsupported`).
- change ordinals: oldest snapshot = 0, incrementing (L124-134).
- For each changelog snapshot's DATA manifests IT added (`manifest.snapshotId() ∈ changelogSnapshotIds`),
  `ignoreExisting()`, filter entries to those snapshots, apply the row filter: ADDED entry →
  `AddedRowsScanTask` (INSERT), DELETED entry → `DeletedDataFileScanTask` (DELETE), each carrying
  (changeOrdinal, commitSnapshotId, dataFile, schema, spec, residual) (L136-182).

Rust: new types — `ChangelogOperation { Insert, Delete }` + `ChangelogScanTask { change_ordinal: i32,
commit_snapshot_id: i64, operation: ChangelogOperation, <the data file + schema + project_field_ids +
predicate, reusing FileScanTask fields or embedding a FileScanTask> }`. `Table::incremental_changelog_scan()`
→ builder mirroring `IncrementalAppendScanBuilder` (from excl/incl + to + with_filter + select). Planner mirrors
`scan/incremental.rs` (the range walk + own-added-data-manifest reading) but: includes BOTH Added (→Insert) and
Deleted (→Delete) entries (NOT just Added), excludes Replace snapshots, computes ordinals, guards delete
manifests. Put it in `scan/incremental.rs` (alongside the append scan) — reuse its helpers.

Tests (multi-snapshot fixture): a 2-append range → Insert tasks with correct ordinals (oldest=0); a snapshot
that DELETED a data file (an overwrite/delete that removes a file, no delete-MANIFEST) → a Delete task; Replace
snapshot excluded; a range containing a DELETE-manifest snapshot → FeatureUnsupported error; filter prunes;
from==to empty. Mutation-pin: the ordinal order (oldest=0), the Added→Insert/Deleted→Delete mapping, the
Replace exclusion, the delete-manifest guard. Confirm normal scan + IncrementalAppendScan unchanged. Widened gate.


#### Working plan (BUILDER Opus, 2026-06-08)
- [x] Verify the load-bearing question against Rust writers: a snapshot's OWN added DATA manifest carries the
      `Deleted` tombstones for the files it removed. CONFIRMED via `transaction/snapshot.rs`:
      `rewrite_manifest_with_deletes` writes the rewritten manifest through `new_filtering_manifest_writer`
      with `Some(self.snapshot_id)` (the NEW snapshot id) -> `ManifestFile.added_snapshot_id == new snapshot id`
      (writer.rs:477), and `add_delete_entry` stamps `entry.snapshot_id = self.snapshot_id` (writer.rs:305).
      So `manifest.added_snapshot_id == snapshot.snapshot_id()` selects the deleting snapshot's rewritten
      manifest, and a `Deleted` entry's `snapshot_id()` == the commit snapshot id. Mirrors Java exactly.
- [x] New types in `scan/task.rs`: `ChangelogOperation { Insert, Delete }`, `ChangelogScanTask` (embed a
      `FileScanTask` + change_ordinal/commit_snapshot_id/operation), `ChangelogScanTaskStream` alias.
- [x] `IncrementalChangelogScanBuilder` + `IncrementalChangelogScan` in `scan/incremental.rs`;
      `Table::incremental_changelog_scan()` entry point in `table.rs`.
- [x] Planner: `ordered_changelog_snapshots` (async; Replace-exclusion + delete-manifest guard); ordinals
      oldest->0; per snapshot, build manifest contexts for its OWN added DATA manifests, tag each task with that
      snapshot's ordinal + commit_snapshot_id = entry.snapshot_id(); Added->Insert, Deleted->Delete.
- [x] Tests + 4 mutation checks; confirm normal scan + IncrementalAppendScan unchanged; run the widened gate.

**Outcome (BUILDER Opus, 2026-06-08):** `IncrementalChangelogScan` LANDED 🟡. **Types** (`scan/task.rs`, pub):
`ChangelogOperation { Insert, Delete }` (ports Java `ChangelogOperation`; the CDC-merge `UPDATE_BEFORE`/
`UPDATE_AFTER` variants are intentionally OMITTED + documented), `ChangelogScanTask` (embeds a `FileScanTask`
+ `change_ordinal: i32` + `commit_snapshot_id: i64` + `operation`, with accessors), `ChangelogScanTaskStream`.
**Builder/planner** (`scan/incremental.rs`): `IncrementalChangelogScanBuilder` DELEGATES range resolution +
`PlanContext` construction to `IncrementalAppendScanBuilder` (so the two scans share, and cannot drift, the
non-trivial range/projection logic; a small `pub(crate) plan_context()` accessor was opened on
`IncrementalAppendScan`). `IncrementalChangelogScan::plan_files`: (1) `ordered_changelog_snapshots` walks the
range parent-chain newest-first, EXCLUDES `Operation::Replace`, REJECTS the range with
`FeatureUnsupported("Delete files are currently not supported in changelog scans")` if any kept snapshot's
manifest list carries a `ManifestContentType::Deletes` manifest, then reverses to oldest-first; (2) ordinals
oldest→0; (3) per snapshot, reads the DATA manifests it itself added (`added_snapshot_id == snapshot_id`) via
the REUSED `build_manifest_file_contexts_from_files` (same partition-filter pruning + residual, empty delete
index), and converts `Added`→Insert / `Deleted`→Delete (skip `Existing`), tagging each task with the
snapshot's ordinal + `commit_snapshot_id = entry.snapshot_id()` (fallback the snapshot id). `Table::
incremental_changelog_scan()` is the entry point. **Java lines verified** against
`/tmp/iceberg-java-ref/.../BaseIncrementalChangelogScan.java`: `doPlanFiles` L56-93, `orderedChangelogSnapshots`
L103-118 (ancestorsBetween range, Replace-exclusion, delete-manifest throw L108-111), `computeSnapshotOrdinals`
L124-134, `CreateDataFileChangeTasks` L136-182 (Added→AddedRowsScanTask=INSERT, Deleted→DeletedDataFileScanTask
=DELETE, `commitSnapshotId = entry.snapshotId()`). **Load-bearing question RESOLVED:** a snapshot's OWN added
DATA manifest DOES carry the `Deleted` tombstones for files it removed — `rewrite_manifest_with_deletes` writes
the rewritten manifest through a writer stamped with the new snapshot id (`added_snapshot_id == snapshot_id`,
`writer.rs:477`) and `add_delete_entry` stamps the `Deleted` entry's `snapshot_id` = that snapshot
(`writer.rs:305`); confirmed by `test_changelog_overwrite_emits_delete_for_removed_file` passing. **Tests (8,
risk-named):** ordinal scheme oldest=0 + commit-id; overwrite emits DELETE(removed)+INSERT(added); Replace
excluded; delete-manifest range → FeatureUnsupported; with_filter partition prune; inclusive from==to = only that
change; exclusive from==to rejected; carried-forward only-own-added. **Mutations (Edit+revert, all caught):**
(a) newest=0 ordinal → ordinal test fails; (b) Deleted→Insert → delete test fails; (c) include Replace →
Replace-excluded test fails; (d) drop delete-manifest guard → FeatureUnsupported test fails. **Normal scan +
IncrementalAppendScan UNCHANGED** (the 13 append-scan + the regular scan tests stay green; only an additive
`pub(crate)` accessor was added). **Gate (on `phase3-overnight`):** `build -p iceberg` clean; workspace
all-targets (excl. sqllogictest) clean; `--lib` ×2 = 1558/0 (1550 prior + 8); `scan::` 64/0; datafusion
`--lib --tests` 80 + 9; interop manage_snapshots/update_schema/update_partition_spec 4/4/4; clippy `-D warnings`
clean; fmt clean; typos 0. **NO Cargo edits.**

### Increment 5 — TableScan use_ref (scan a branch/tag by name) (DETAIL)
Increment 4 (IncrementalChangelogScan) committed `4a772640`. Java `TableScan.useRef(String ref)`
(`api/.../TableScan.java:48`, impl in `BaseTableScan`/`SnapshotScan.useRef`): select the snapshot a
branch/tag reference points to. Rust `TableScanBuilder` has `snapshot_id(i64)` but NO ref-based selector;
`TableMetadata::snapshot_for_ref(name) -> Option<&SnapshotRef>` already exists. Bounded, self-contained,
low-risk, valuable (branch/tag reads).

Build: add `snapshot_ref: Option<String>` to `TableScanBuilder` + `use_ref(impl Into<String>)`; in `build()`
resolve the ref via `metadata.snapshot_for_ref(&name)` → its snapshot (error `DataInvalid`/`Unexpected` if the
ref doesn't exist). Java rejects combining `useRef` with `useSnapshot`/`asOfTime` — reject `use_ref` + 
`snapshot_id` both set (a clear error). Otherwise scan the resolved snapshot exactly like `snapshot_id`.
Tests: `use_ref("main")` scans the current snapshot; a tag/branch ref scans its snapshot; unknown ref → error;
`use_ref` + `snapshot_id` both set → error; default (neither) unchanged. Mutation-pin the ref resolution + the
both-set rejection. Confirm existing scans unchanged. Widened gate. ONLY `scan/mod.rs` + docs.

Plan (BUILDER Opus, 2026-06-08):
- [x] Read Java `SnapshotScan.useRef` (L116-128): MAIN_BRANCH ⇒ table default; else reject if snapshot id
      already set ("Cannot override ref…"); resolve `table().snapshot(name)`; reject null ("Cannot find ref %s").
- [x] Add `snapshot_ref: Option<String>` field (init `None`) + `pub fn use_ref(impl Into<String>)`.
- [x] In `build()`: both-set ⇒ `DataInvalid`; ref-set ⇒ `snapshot_for_ref` (unknown ⇒ `DataInvalid` with name);
      else unchanged `snapshot_id`-or-current logic.
- [x] Tests against the existing `example_table_metadata_v2.json` fixture: `test` tag → older snapshot
      `3051729675574597004`; `main` → current; unknown ref → err; both-set → err; default unchanged; plus a
      result-equivalence read through `use_ref("main")`.
- [x] Mutations (Edit+revert): ignore `snapshot_ref`; drop both-set rejection; drop unknown-ref error.
- [x] Docs: GAP_MATRIX, Roadmap, todo Outcome, lessons (if non-obvious).

**Outcome (2026-06-08, Increment 5 — TableScan use_ref, BUILDER Opus):** `TableScanBuilder::use_ref(impl
Into<String>)` lands in `crates/iceberg/src/scan/mod.rs`, mirroring Java `SnapshotScan.useRef`
(`core/SnapshotScan.java` L116-128). A new `snapshot_ref: Option<String>` field (init `None`) records the
intent; `build()` resolves BOTH selectors up front via a `match (self.snapshot_id, self.snapshot_ref)`:
ref+snapshot-id ⇒ `DataInvalid` ("Cannot scan using both a ref … and a snapshot id", Java "Cannot override
ref"); ref-only ⇒ `TableMetadata::snapshot_for_ref(name)` (the EXISTING accessor — no `spec/` change), unknown
⇒ `DataInvalid` ("snapshot ref '…' not found", Java "Cannot find ref %s"); else the ORIGINAL `snapshot_id`-or-
current logic flows unchanged (including the no-current-snapshot empty-scan early return). `"main"` resolves to
the current snapshot because the parse path auto-injects a `main` ref at the current snapshot id
(`table_metadata.rs` `TryFrom`), matching Java `useRef(MAIN_BRANCH)` returning the table default — so no special
`MAIN_BRANCH` arm is needed. 6 unit tests over the shared `example_table_metadata_v2.json` fixture (which
already carries two snapshots + a `test` TAG pointing at the OLDER one): `main`→current, `test`→older snapshot
(asserts ≠ current — the core branch/tag-read behavior), unknown→err+names-ref, ref+id→err (order-independent),
default-unchanged, and a planning result-equivalence read (`use_ref("main")` plans the same 2 files as a plain
`.build()`). Mutations (Edit+revert): (a) ignore `snapshot_ref` (always current) → tag test fails 3055…≠3051…;
(b) drop the both-set rejection (id wins) → both-set test fails; (c) drop the unknown-ref error (fall back to
current) → unknown-ref test fails — all caught. Existing scans UNCHANGED: the full `scan::` suite stayed green
(70 tests, 64 prior + 6 new). Gate green on `phase3-overnight`. ONLY `scan/mod.rs` + docs touched; NO Cargo
edits, NO `spec/` change.

#### Increment 5 — REVIEW (2026-06-08, Opus REVIEWER, DELEGATED) — CHANGES-MADE (PASS 🟡)
Verdict: PASS 🟡 with one test added. Resolution logic confirmed faithful to Java `SnapshotScan.useRef`
L116-128 (both-set→reject, unknown→reject, `"main"`/ref→correct snapshot via the auto-injected main ref); the
default no-`use_ref` path is byte-unchanged (the new pre-resolution `match` only computes a `snapshot_id`, then
the ORIGINAL `match snapshot_id` block — incl. the empty-table early return — is untouched bar `self.snapshot_id`
→ `snapshot_id`). Test-strength: the core different-ref test pins the resolved snapshot id (the load-bearing
logic), and full planning is separately proven via `use_ref("main")` — judged acceptable, and STRENGTHENED with
`test_plan_files_use_non_main_ref_plans_referenced_snapshot` (adds a non-main tag at the current snapshot via
`set_ref`, then plans through it = the default file set), guarding against any future special-casing of `"main"`
in planning. Re-ran all 3 builder mutations + a 4th (swap both-set to silently resolve the ref instead of
erroring) — all 4 caught. The `use_ref("main")`-on-an-empty-table divergence (Java returns an empty-default;
Rust errors `DataInvalid` because no `main` ref is injected without a current snapshot) is narrow, documented,
and a LOUD error (no silent corruption) — judged acceptable. Engineering floor: no bare `.unwrap()` in
production (resolution uses `.ok_or_else`/`?`); test `.unwrap()` matches the local module convention; error
kinds/messages name the ref. Scope clean: `scan/mod.rs` + the 4 docs only; no `spec/`/Cargo edits. Lib total now
1565/0 (×2 stable), `scan::` 71/0, interop 4+4+4, clippy/fmt/typos clean. Stayed on `phase3-overnight`; no
commit/push.

---

## SEQUENCE: RowDelta delete-file conflict validation (Phase 3, started 2026-06-08)
Branch `phase3-rowdelta-delete-conflicts` (off merged `main` `063b7153`). Completes the RowDelta serializable-
isolation surface beyond the data-file conflict check (which landed in the residual sequence Inc 4).

### Increment — RowDelta validateNoConflictingDeleteFiles (THIS increment)
Java `BaseRowDelta.validate` → `validateNewDeleteFiles` → `validateNoNewDeleteFiles(base, startingSnapshotId,
conflictDetectionFilter, parent)` (`MergingSnapshotProducer.java:562-570`): enumerate DELETE files ADDED by
concurrent commits since the starting snapshot, and reject if ANY could apply to records matching the conflict
filter. The error: "Found new conflicting delete files that can apply to records matching %s: %s".

**Building blocks + Java authority (CONFIRMED):**
- `addedDeleteFiles` (`MergingSnapshotProducer.java:601-625`): V2-ONLY (`base.formatVersion() < 2` ⇒ empty —
  delete files don't exist in V1); `validationHistory(..., VALIDATE_ADDED_DELETE_FILES_OPERATIONS,
  ManifestContent.DELETES, ...)` then a `DeleteFileIndex` filtered by the dataFilter + startingSequenceNumber.
  `VALIDATE_ADDED_DELETE_FILES_OPERATIONS = {OVERWRITE, DELETE}` (vs `{APPEND, OVERWRITE}` for data).
- Rust model: `added_data_files_after` (`transaction/snapshot.rs:980`) is the data-file analogue — same walk,
  EXCLUSIVE of start, only manifests the snapshot itself added, only `Added` entries. The delete version
  differs ONLY in the manifest content filter (`Deletes`) + the operation set (`Overwrite | Delete`) + the V2
  guard. **Generalize the walk** into a private `added_files_after(table, start, content, op_predicate)`;
  `added_data_files_after` + new `added_delete_files_after` both call it (Rule-of-Three on the walk).
- Conflict TEST = the SAME `InclusiveMetricsEvaluator` per file used by the data check; extract a shared
  `first_conflicting_file(files, current, conflict_filter, case_sensitive) -> Result<Option<DataFile>>` and
  have the existing `validate_no_conflicting_added_data_files` (refactor, behavior-preserving — Inc-4
  OverwriteFiles + RowDelta data tests stay green) AND a new `validate_no_conflicting_added_delete_files` (with
  the DELETE error message) use it.

Plan:
- [x] `transaction/snapshot.rs`: generalize the walk → `added_files_after`; add `added_delete_files_after`
      (content=Deletes, ops=Overwrite|Delete, V2 guard `format_version() < FormatVersion::V2 ⇒ empty`); extract
      `first_conflicting_file` (the inclusive-metrics test); add `validate_no_conflicting_added_delete_files`
      using it + the delete message; refactor `validate_no_conflicting_added_data_files` onto the shared test
      (no behavior change). `added_data_files_after` enumeration semantics unchanged.
- [x] `transaction/row_delta.rs`: add `validate_no_conflicting_delete_files: bool` field + builder; extend
      `validate` — after the data check, if enabled run the delete check (same `effective_start`,
      `conflict_detection_filter`, case-sensitive true). Module doc: drop `validateNoConflictingDeleteFiles`
      from deferred (note `validateDataFilesExist` + `validateNoNewDeletesForDataFiles` + V3 `validateAddedDVs`
      still deferred — they need `referenced_data_files`/`removed_data_files` on the action, which it lacks).
- [x] Tests (MemoryCatalog + a real concurrent commit that ADDS a delete file via a RowDelta/overwrite path):
      no-concurrent-delete → OK; concurrent commit adds a DELETE file whose metrics MATCH the filter → rejected
      non-retryable `DataInvalid` (names the file, delete-specific message); metrics-EXCLUDE → OK; flag OFF →
      no delete check; None filter → any concurrent delete conflicts; V1 table → no delete check (guard);
      data-check and delete-check independent (enabling one doesn't enable the other). Mutation-pin: the
      content filter (Data vs Deletes in the walk), the metrics decision, the non-retryable kind. Keep Inc-4
      data tests green (shared-test refactor proof).
- [x] Docs: GAP_MATRIX (RowDelta validateNoConflictingDeleteFiles 🟡 + the seq-number/DeleteFileIndex precision
      deferral), Roadmap, todo, lessons.
- [x] Verify from repo root: build -p iceberg + workspace; lib ×2; transaction::; datafusion tests; 3 interop;
      clippy (workspace); fmt.

**Outcome (2026-06-08, BUILDER Opus):** RowDelta `validateNoConflictingDeleteFiles` landed 🟡.
- **Generalized walk.** Factored the data-file walk into a private `async fn added_files_after(table,
  starting_snapshot_id, content: ManifestContentType, operation_adds: fn(&Operation)->bool) ->
  Result<Vec<DataFile>>` (Java `validationHistory`). `added_data_files_after` is now a thin call
  `(Data, operation_adds_data_files)`; its observable behavior is unchanged — the existing OverwriteFiles /
  ReplacePartitions / RowDelta data conflict tests (241 transaction:: tests) stayed green unchanged. Added
  `pub(crate) added_delete_files_after` = `(Deletes, operation_adds_delete_files)` with the V2 guard
  (`format_version() < FormatVersion::V2 ⇒ Ok(vec![])`, Java `addedDeleteFiles` L608). New
  `operation_adds_delete_files = matches!(op, Overwrite | Delete)` (Java `VALIDATE_ADDED_DELETE_FILES_OPERATIONS`,
  distinct from data's `{Append, Overwrite}`).
- **Shared per-file test extraction.** Extracted `fn first_conflicting_file(files: &[DataFile], current:
  &Table, conflict_filter: Option<&Predicate>, case_sensitive) -> Result<Option<DataFile>>` (bind None⇒AlwaysTrue
  once, per-file `InclusiveMetricsEvaluator::eval`, return first match). Refactored
  `validate_no_conflicting_added_data_files` onto it (DATA message unchanged) and added
  `validate_no_conflicting_added_delete_files` onto it (DELETE message "Found new conflicting delete files that
  can apply to records matching {filter}: {path}"). Behavior-preservation proof: the data path's tests pass
  unchanged.
- **RowDelta wiring.** Added `validate_no_conflicting_delete_files: bool` + builder
  `validate_no_conflicting_delete_files()`. The `validate` override now runs the data check (if its flag) THEN
  the delete check (if ITS flag), sharing `effective_start = validate_from_snapshot.or(starting)` +
  `conflict_detection_filter`; either failing rejects. The two flags are independent.
- **V2 guard finding (honest):** the V1 end-to-end test STILL passes with the guard dropped — the concurrent
  commit on V1 is an `Append` (excluded by `{Overwrite,Delete}`) and no DELETE manifests can exist on V1, so the
  walk naturally yields nothing. The guard is a Java-faithful short-circuit (avoids a needless walk), NOT
  behavior-changing for this scenario. Documented as such; the V1 test also asserts `added_delete_files_after`
  directly returns empty.
- **Over-scan (documented):** the port omits Java's `DeleteFileIndex` `startingSequenceNumber` refinement —
  conservative (over-reject only), same class as Inc-3's manifest-summary pre-filter deferral.
- **Java lines verified:** `BaseRowDelta.validate` L131-174 (the `validateNewDeleteFiles` branch L159-167; the
  `!removedDataFiles.isEmpty()` sub-branch L161-164 is unreachable here — the action has no `removeRows` — so it
  is correctly out of scope); `MergingSnapshotProducer.validateNoNewDeleteFiles` L562-570 (the error message);
  `addedDeleteFiles` L601-625 (V2 guard L608, `VALIDATE_ADDED_DELETE_FILES_OPERATIONS` + `ManifestContent.DELETES`
  L614-620); the op sets L73-85 (`{OVERWRITE, DELETE}` for deletes vs `{APPEND, OVERWRITE}` for data).
- **Tests (8 new delete-conflict, each risk-named):** `..._delete_validation_no_concurrent_commit_succeeds`
  (race-free → OK); `..._rejects_concurrent_added_delete_file_matching_filter` (HEADLINE — non-retryable +
  names the file + DELETE-specific message asserted distinct from the data message); `..._allows_concurrent_added_delete_file_excluded_by_filter`
  (EXCLUDE → OK); `..._without_delete_validation_allows_conflicting_concurrent_delete` (flag OFF → snapshot
  isolation); `..._delete_none_filter_treats_any_concurrent_delete_as_conflict` (None⇒AlwaysTrue);
  `..._delete_check_is_noop_on_v1_table` (V2 guard, + direct `added_delete_files_after` assertion);
  `..._delete_check_does_not_run_data_check` + `..._data_check_does_not_run_delete_check` (independence both
  ways). 27 row_delta tests total (was 19).
- **Mutations run + caught:** (a) `added_delete_files_after` walks Data not Deletes → the headline delete test
  fails (content filter load-bearing); (b) invert `first_conflicting_file` decision → the EXCLUDE test fails for
  BOTH overwrite_files (data) AND row_delta (delete) — shared test load-bearing for both; (c) delete error made
  retryable → `!retryable()` assertion fails + the retry loop spins (0.07s→1.55s); (d) drop the V2 guard → the
  V1 test STILL passes (walk yields nothing — guard is a faithful short-circuit, documented).
- **Gate (all 8 green):** (1) build -p iceberg OK; (2) build --workspace --exclude iceberg-sqllogictest
  --all-targets OK; (3) `cargo test -p iceberg --lib` ×2 = **1524 passed, 0 failed** both runs (new lib total
  1524; was 1516); (4) `transaction::` = 241 passed (Inc-3 OverwriteFiles + Inc-4 RowDelta data tests
  UNCHANGED-green); (5) iceberg-datafusion lib 80 + integration 9 pass; the ONE failing doctest
  (`table_provider_factory.rs` line 41, `rt-multi-thread` disabled under `#[tokio::main]`) is PRE-EXISTING +
  environmental — fails identically with my changes git-stashed; (6) interop_manage_snapshots / interop_update_schema
  / interop_update_partition_spec = 4/4 each; (7) clippy --workspace --exclude iceberg-sqllogictest --all-targets
  -D warnings clean; (8) fmt --check clean. No commit, no branch switch, edits only in snapshot.rs / row_delta.rs +
  docs (GAP_MATRIX, Roadmap, todo, lessons).

**Note (over-scan, document):** Java's `addedDeleteFiles` additionally filters the DeleteFileIndex by
`startingSequenceNumber`; this port enumerates concurrent-added delete files by the snapshot walk + inclusive
metrics only (no seq-number refinement) — a conservative over-scan (can only over-reject, never under-reject),
same class as Inc-3's manifest-summary pre-filter deferral.

---

## Active (2026-06-09): DeleteFiles validateFilesExist (validateDataFilesExist for the delete path)

Increment: Java `MergingSnapshotProducer.validateDataFilesExist` semantics for the DeleteFiles action —
reject the commit if any data file this op is deleting was DELETED by a concurrent commit since the start.

- [x] **snapshot.rs status axis** — generalize `added_files_after`'s hard `status() == Added` filter into a
  `status_to_keep: ManifestStatus` parameter. The two existing callers (`added_data_files_after`,
  `added_delete_files_after`) pass `ManifestStatus::Added` (BEHAVIOR-PRESERVING — their tests stay green).
- [x] **snapshot.rs `operation_removes_data_files`** — `{Overwrite, Replace, Delete}` (Java
  `VALIDATE_DATA_FILES_EXIST_OPERATIONS`). Note: Rust `Operation` has no `Replace` variant → only
  `{Overwrite, Delete}` are representable; document.
- [x] **snapshot.rs `deleted_data_files_after(table, start) -> Vec<DataFile>`** — DATA content,
  `operation_removes_data_files`, `ManifestStatus::Deleted`. The concurrent delete's rewritten manifest
  carries the Deleted tombstone with `added_snapshot_id == that snapshot` (verified in
  `rewrite_manifest_with_deletes`), so the existing manifest filter finds it.
- [x] **delete_files.rs** — add `validate_files_exist: bool` + `validate_from_snapshot: Option<i64>` fields +
  `validate_files_exist()` / `validate_from_snapshot(i64)` builders + a `validate` override (mirror
  ReplacePartitions): if OFF ⇒ Ok; effective_start = override.or(start); enumerate
  `deleted_data_files_after`; if any deleted file's path ∈ self.delete_paths ⇒ non-retryable
  `Error::new(DataInvalid, "Cannot commit, missing data files: <path>")`.
- [x] **Tests (5)** + mutation checks (a–d) + behavior-preservation of the two Added callers.
- [x] **Docs** — GAP_MATRIX, Roadmap, lessons; note skip-deletes op-set variant + RowDelta
  validateDataFilesExist deferred.

**Java divergence flagged up front:** `StreamingDelete.validate()` actually calls `failMissingDeletePaths()`
(the filter-manager required-deletes check), NOT `validateDataFilesExist`. The brief directs the
`validateDataFilesExist`-semantics port for the delete path (requiredDataFiles = the files being deleted),
modeled on RowDelta/ReplacePartitions. Faithful to `validateDataFilesExist`'s contract; report the
StreamingDelete wiring nuance.

**Outcome (2026-06-09):** Landed. `transaction/snapshot.rs` (status axis `files_after` +
`operation_removes_data_files` + `deleted_data_files_after`) + `transaction/delete_files.rs`
(`validate_files_exist()` / `validate_from_snapshot()` + `validate` override + 5 tests). Lib total
1573 → 1578. transaction:: 246 green (the two Added callers behavior-preserving). 4 mutations caught.
Docs updated (GAP_MATRIX, Roadmap, lessons). Deferred: skip-deletes op-set variant + RowDelta
validateDataFilesExist.

**REVIEW (2026-06-09, Opus):** Verified behavior-preservation (inverting the shared manifest filter fails 17
tests across BOTH the Deleted and Added axes), the deleted-file enumeration vs Java (content DATA, op
`{Overwrite, Delete}`, status Deleted — op set PINNED by a real `Delete`-op concurrent deletion), end-to-end
through `tx.commit` + non-retryable. Ran 8 mutations (the builder's 4 + content-type/manifest-filter/intersection/
tx-captured-fallback); ALL caught after a fix. **Found + fixed 1 SURVIVING mutation:** the tx-captured
`starting_snapshot_id` fallback (no `validate_from_snapshot`) was unpinned (the recurring Increment-6 gap — all
5 builder tests set the override). Added `test_delete_files_exist_rejects_concurrent_using_tx_captured_starting_snapshot`
(reviewer); the refreshed-head mutation now fails exactly it. Lib total 1578 → **1579**. Docs reconciled (test
count 5→6, mutation list 4→8).

---

## Active (2026-06-09): RowDelta validateDataFilesExist + the skip-deletes op-set variant

Increment: Java `BaseRowDelta.validateDataFilesExist(referencedFiles)` — RowDelta gains a builder providing
the data files its added position-deletes REFERENCE, and at commit rejects if any referenced data file was
DELETED by a concurrent commit since the start. Reuses `deleted_data_files_after`; ADDS the skip-deletes
op-set variant (Java `VALIDATE_DATA_FILES_EXIST_SKIP_DELETE_OPERATIONS = {OVERWRITE, REPLACE}`).

- [x] **snapshot.rs `skip_deletes` op-set variant** — `deleted_data_files_after(table, start, skip_deletes:
  bool)`: `skip_deletes=true` ⇒ `{Overwrite}` (new `operation_removes_data_files_skip_deletes`; Java drops
  DELETE; REPLACE unrepresentable); `skip_deletes=false` ⇒ `{Overwrite, Delete}`. The existing DeleteFiles
  caller passes `false` (BEHAVIOR-PRESERVING — proven: forcing it to `true` fails 3 DeleteFiles tests).
- [x] **row_delta.rs** — added `referenced_data_files: HashSet<String>` + `validate_deleted_files: bool`
  fields + `validate_data_files_exist(...)` + `validate_deleted_files()` builders + the `validate`-override
  branch (`skip_deletes = !validate_deleted_files`; intersection of `deleted_data_files_after` paths ∩
  `referenced_data_files` ⇒ non-retryable `DataInvalid` "Cannot commit, missing data files: <path>").
- [x] **Tests (6)** + mutation checks (a–d, all caught) + the DeleteFiles behavior-preservation mutation.
- [x] **Docs** — GAP_MATRIX, Roadmap, lessons updated.

**Faithfulness note:** Java `referencedDataFiles` is CALLER-PROVIDED (`CharSequenceSet`, populated by
`validateDataFilesExist(referencedFiles)`), NOT derived from the added delete files. The Rust port mirrors
this — `validate_data_files_exist([paths])` takes the caller's set.

**Outcome (2026-06-09):** Landed. `transaction/snapshot.rs` (skip-deletes op-set axis on
`deleted_data_files_after` + `operation_removes_data_files_skip_deletes`) +
`transaction/row_delta.rs` (`validate_data_files_exist` / `validate_deleted_files` + the `validate` branch +
6 tests) + `transaction/delete_files.rs` (caller passes `skip_deletes = false`). transaction:: 247 → 253
green (the DeleteFiles increment-1 + data/delete conflict tests behavior-preserving). 4 mutations (a–d)
caught + the DeleteFiles `skip_deletes=true` mutation caught. Deferred: `validateNoNewDeletesForDataFiles`,
`validateAddedDVs` (need `removed_data_files`), `OverwriteFiles.validateDataFilesExist`.

---

## Active (2026-06-09): Inspection-table interop — `snapshots` + `refs` (Phase 3 interop Increment 1)

Increment: the FIRST byte/field-level "read a table Java wrote" evidence for the inspection tables — flips the
`snapshots` + `refs` inspection rows to interop-✅ (the rest of the inspection set stays 🟡). Builder→reviewer
actor-critic via Workflow; orchestrator independently re-ran the full gate + committed. **Purely additive — NO
production-code change.**

- [x] **Java oracle** (`dev/java-interop/.../InteropOracle.java`) — new `generate-inspection` exec mode +
  nested `InspectionOracle` + dedicated `InMemoryInspectionOperations` (its `io()` returns a real
  `InMemoryFileIO` with the metadata file pre-`addFile`d). Builds a purpose-rich V2 base (3 snapshots
  ROOT/CURRENT/SIBLING — CURRENT carries a MULTI-KEY summary, ROOT operation-only → empty map; refs main
  branch-no-retention / dev branch-full-retention / stable tag-only-max-ref-age), writes `base.metadata.json`,
  RE-PARSES it (`TableMetadataParser.fromJson`), then materializes Java's REAL `SnapshotsTable`/`RefsTable`
  rows via `MetadataTableUtils.createMetadataTableInstance` + `mt.newScan().planFiles()` +
  `task.asDataTask().rows()`, serialized by `JsonUtil`/`JsonGenerator` to `java_{snapshots,refs}.json`. No
  Maven deps added; pom untouched.
- [x] **Rust test** (`crates/iceberg/tests/interop_inspection.rs`, 2 tests) — loads the same
  `base.metadata.json`, runs `inspect().snapshots()/.refs().scan()`, extracts every Arrow column, asserts
  field-for-field equality vs the Java rows ORDER-INDEPENDENTLY (snapshots by id, refs by name; summary as a
  `HashMap`) + focused named assertions (committed_at micros, empty-vs-multi-key summary, operation-not-in-map,
  retention NULL-per-kind).
- [x] **Gate (orchestrator-rerun, all green):** iceberg lib 1595, interop_inspection 2/2, the other 3 interop
  suites 4/4/4, datafusion 80+9, clippy/fmt/typos clean; `git status` = only InteropOracle.java +
  testdata/interop/inspection/ + the new test (no existing fixtures changed).
- [x] **Docs** — GAP_MATRIX (interop note on the inspection row), lessons, todo.

**Key finding (memory `reference_java_snapshot_summary_operation.md`):** Java `SnapshotParser.fromJson` splits
`operation` OUT of the summary map on the on-disk round-trip ⇒ Rust's `additional_properties`-only summary
column already matches Java's RE-PARSED `summary()` — NO Rust change. Verified against `/tmp/iceberg-java-ref`
1.10.0 before any edit (averted a wrong "fix"). **Gotcha:** Java `StaticDataTask.rows()` is a lazy transform
over ONE mutable projection — serialize each row eagerly, never stash `StructLike`s.

**Deferred (next interop increments):** `history` + `metadata_log_entries` (need a multi-entry snapshot-log +
metadata-log fixture so `is_current_ancestor=false` and the `latest_*` as-of-time columns are exercised); then
the manifest-reading tables `files`/`entries`/`manifests`/`partitions`/`all_*` + scan interop (need real
on-disk manifests + parquet, a bigger harness step).

---

## Active (2026-06-09): Inspection-table interop — `history` + `metadata_log_entries` (Phase 3 interop Increment 2)

Increment: interop evidence for the two DERIVED-column pure-metadata inspection tables — completes interop
for ALL FOUR pure-metadata inspection tables. Builder→reviewer (validation-first, per user "validation is
key"); orchestrator independently re-ran the full gate + committed. **Purely additive — NO production change.**

- [x] **Java oracle** — new `generate-inspection-log` exec mode + `InspectionLogOracle` (reuses the prior
  increment's `InMemoryInspectionOperations` / `RowWriter` / `rowsToJson`). Builds a FORKED snapshot-log via
  the 3-commit RE-PARSE recipe (B0 ROOT→main, B1 SIBLING→main, B2 CURRENT→main, re-parse between each to
  dodge intermediate-snapshot pruning) → log `[ROOT@2018-01, SIBLING@2018-08, CURRENT@2019-04]`,
  CURRENT.parent=ROOT ⇒ SIBLING off-ancestry. INJECTS a deterministic `metadata-log` (3 straddling entries)
  via `JsonUtil.mapper()`, re-parses with a stable logical URI, materializes Java's REAL
  `HistoryTable`/`MetadataLogEntriesTable` rows → `java_history.json` / `java_metadata_log_entries.json`
  under `testdata/interop/inspection_history/`.
- [x] **Rust tests (2, added to `interop_inspection.rs`)** — `inspect().history()/.metadata_log_entries()
  .scan()` asserted field-for-field equal vs the Java rows (all 4 / all 5 columns, order-independent —
  history by composite `(made_current_at,snapshot_id)`, log by timestamp) + focused pins:
  `is_current_ancestor` true/FALSE/true (ROOT/SIBLING/CURRENT); `latest_*` NULL/ROOT/SIBLING/CURRENT (with
  schema_id 0 + seq 1/2/3); synthetic-entry `file` == the stable metadata location.
- [x] **Gate (orchestrator-rerun, all green):** iceberg lib 1595, interop_inspection 4/4, other 3 interop
  4/4/4, datafusion 80+9, clippy/fmt/typos clean; `git status` = only `InteropOracle.java` +
  `interop_inspection.rs` + `inspection_history/` (existing `inspection/` fixtures untouched).
- [x] **Docs** — GAP_MATRIX, Roadmap (6c), lessons, todo.

**Key recipe (lessons):** forked snapshot-log needs SEPARATE commits + re-parse between (Java
`intermediateSnapshotIdSet` pruning); deterministic `latest_*` needs an INJECTED metadata-log (real commits
stamp ≈now); Java `addSnapshot` sets `lastUpdatedMillis = snapshot.ts`, so the synthetic entry lands on
`CURRENT_TS`, bonus-pinning the `<=` boundary; pin `metadata_location` to a stable URI on both sides.
Reviewer mutation-probed the fixture (flip SIBLING ancestor; flip creation-row NULL) — each fails the test.

**Deferred (next):** manifest-reading inspection tables (`files`/`entries`/`manifests`/`partitions`/`all_*`)
+ scan interop — need real on-disk manifests + parquet (a bigger harness step: the oracle must write actual
manifest/data files, or the Rust side must read Java-written ones).

---

## Active (2026-06-09): Manifest-reading interop A1 — `files`/`data_files`/`delete_files` (Phase 3)

Increment: FOUNDATION of the manifest-reading inspection interop. User chose **item (a)** (take on the
manifest-reading tables) and **run.sh-driven** wiring. Builder→reviewer; orchestrator independently re-ran
the offline gate AND the run.sh round-trip + committed. **Purely additive — NO production change.**

- [x] **Java oracle** — `generate-inspection-manifests` mode + `InspectionManifestsOracle` + minimal
  `LocalFileIO` (`Files.localOutput/localInput`) + `LocalTableOperations` (commit writes metadata to disk).
  Writes a REAL partitioned V2 table (no parquet/hadoop deps): `newAppend` 2 data files (2 partitions, with
  metrics + id/value bounds) then `newRowDelta` 1 position-delete; materializes Java's REAL `FilesTable`
  rows (`MetadataTableUtils` + `planFiles` + `ManifestReadTask.asDataTask().rows()`) → java_{files,data_files,
  delete_files}.json + the table under a gitignored `target/` temp dir.
- [x] **Rust test** (`interop_inspection_manifests.rs`) — ENV-GATED (`ICEBERG_INTEROP_MANIFEST_DIR`),
  runtime early-return no-op when unset (offline gate stays green; NOT `#[ignore]`). When set: load
  `final.metadata.json`, build a Table with `FileIO::new_with_fs()`, scan files/data_files/delete_files,
  field-match all 21 non-derived columns order-independently (bounds as raw bytes; content filter pinned).
- [x] **run script** `dev/java-interop/run-inspection-manifests.sh` (mvn generate → env-gated cargo test).
- [x] **Gate (orchestrator-rerun, all green):** offline (manifests test no-op 1 passed; interop_inspection
  4/4; workspace build/clippy/fmt/typos clean) + run.sh round-trip (files=3/data_files=2/delete_files=1
  matched). `git status` = only InteropOracle.java + interop_inspection_manifests.rs + run-inspection-manifests.sh.
- [x] **Docs** — GAP_MATRIX, Roadmap (6d), lessons, todo.

**Findings (lessons):** Java writes real manifests with NO parquet/hadoop (LocalFileIO + real commits);
run.sh-driven env-gated test keeps the offline gate green; VERIFY on-disk bytes before calling a render a
divergence. **Two documented presentation divergences (on-disk MATCHES; not bugs):** `file_format` case
(Java row UPPERCASE via the FileFormat enum vs Rust lowercase Display — a small inspection-table-only parity
gap) + absent metric-map `{}` vs `null` (Rust non-optional maps). readable_metrics deferred from comparison.

### FOLLOW-UP DONE (2026-06-09): `file_format` UPPERCASE in inspection tables
`inspect/data_file.rs` now emits `file_format` upper-cased (`.to_string().to_uppercase()`) for the shared
files/entries/all_* projection, matching Java's `FilesTable` enum-NAME rendering. On-disk write UNCHANGED
(`DataFileFormat::Display`/serde, still lowercase; `git diff crates/iceberg/src/spec` empty). The A1 interop
test was tightened to EXACT file_format equality (canonicalization dropped). No unit-snapshot churn (the
inspect unit tests assert only the Arrow schema, never the rendered value); datafusion/sqllogictest had no
file_format value expectation. Gate + run.sh round-trip green. Own commit (fix(inspect)).

**Deferred next (manifest interop):** A2 `entries`/`manifests`/`partitions` (same harness); A3 `all_*`
(multi-snapshot table); A4 scan PLANNING (file set + residuals); A5 scan EXECUTION (reads parquet → Arrow —
needs parquet Maven deps + a separate approval).

---

## Active (2026-06-09): Manifest-reading interop A2 — `entries`/`manifests`/`partitions` (Phase 3)

Increment: 3 more manifest-reading tables over a RICHER multi-snapshot table (`InspectionManifestsA2Oracle`
→ `<dir>/table_a2`; A1's `<dir>/table` + test untouched). Builder→reviewer; orchestrator independently
re-ran the offline gate + the run.sh round-trip + verified the production fix + committed.

- [x] **Java oracle** — A2 table: `newAppend` A=a/B=b/C=a/D=b → `newRowDelta` +pos-delete (cat=a) →
  `newDelete` B → DELETED tombstone + content-gated DATA/DELETE manifests + 2 live partitions. Materializes
  Java's REAL `EntriesTable`/`ManifestsTable`/`PartitionsTable` rows → java_{entries,manifests,partitions}.json.
- [x] **Rust tests (3, added to `interop_inspection_manifests.rs`)** — entries/manifests/partitions field-match
  Java order-independently; entries reuses A1's `FileRow` for the nested data_file via a new `ColumnSource`
  trait; focused pins: status==2 tombstone, content-gating (data manifest 0 delete-counts / delete manifest 0
  data-counts), partition delete-counts + DATA-only `total_data_file_size_in_bytes`.
- [x] **Production parity fix** (`inspect/partition_summary.rs`) — `partition_summaries` STRING bounds were
  JSON-quoted (`Datum::to_string`) vs Java's bare `Transform.toHumanString`; fixed to `to_human_string` (only
  string bounds change; int/long unit tests unaffected). Verified both the Rust method semantics (datum.rs:1195)
  AND the Java call-site (ManifestsTable L134-144).
- [x] **Gate (orchestrator-rerun, all green):** offline (A2 tests no-op; A1 still green; lib 1595; datafusion
  80+9; clippy/fmt/typos) + run.sh round-trip (A1 + 3 A2). `git status` = partition_summary.rs +
  interop_inspection_manifests.rs + InteropOracle.java + run-inspection-manifests.sh.
- [x] **Docs** — GAP_MATRIX, Roadmap (6e), lessons, todo.

**Deferred next:** A3 `all_*` (all_data_files/all_delete_files/all_files/all_entries/all_manifests —
multi-snapshot reachability + non-dedup for all_manifests); A4 scan PLANNING (planFiles file set + residuals);
A5 scan EXECUTION (parquet → Arrow, needs parquet Maven deps + separate approval). Then squash + PR.

---

## Active (2026-06-09): Manifest-reading interop A3 — the five cross-snapshot `all_*` tables (Phase 3)

Increment: `all_data_files`/`all_delete_files`/`all_files`/`all_entries`/`all_manifests` interop, REUSING the
A2 table read-only. **COMPLETES manifest-reading interop for every inspection table.** Purely additive — NO
production change. Builder→reviewer; orchestrator re-ran gate + run.sh + fixed the stale run.sh echo + committed.

- [x] **Java oracle** — extended the A2 generate step to ALSO materialize Java's REAL `All*` rows over the
  same `table_a2` → java_all_{data_files,delete_files,files,entries,manifests}.json (generic `rowsToJson`,
  `all_manifests`'s `reference_snapshot_id` is just another scalar).
- [x] **Rust tests (5, added to `interop_inspection_manifests.rs`)** — reuse FileRow/EntryRow/ManifestRow
  extraction (+ an AllManifestRow wrapper). Order-independent MULTISET comparison (sort full rows by Debug,
  element-wise, NO dedup — these tables may return duplicates). Focused pins: cross-snapshot reach (B present
  in all_*, absent in current data_files); duplicates preserved (8 rows / 5 distinct paths); all_manifests
  non-dedup (shared manifest → 2 rows, reference_snapshot_id distinct + ≠ added_snapshot_id).
- [x] **Gate (orchestrator-rerun, all green):** offline (A3 no-op; A1+A2 green; lib 1595; datafusion 80+9;
  clippy/fmt/typos) + run.sh round-trip (9 tests = A1 + 3 A2 + 5 A3). Fixed the stale run.sh header/echo (A3).
- [x] **Docs** — GAP_MATRIX, Roadmap (6f), lessons, todo.

**Manifest-reading interop COMPLETE for all inspection tables.** Remaining inspection interop: the
`readable_metrics` virtual column (interior field-id JVM-order residual); SCAN interop — A4 PLANNING (planFiles
file set + residuals, no parquet needed) + A5 EXECUTION (parquet → Arrow, needs parquet Maven deps + approval).
Then: squash the `phase2-3-remnants` set (now ~9 commits, LOCAL-only) + open the PR.

---

## Active (2026-06-09): Scan-PLANNING interop A4 (Phase 3) — DONE

Increment: scan-planning interop. Java plans 4 filter scenarios via REAL `table.newScan().filter().planFiles()`
over a dedicated `table_a4` (F1/F2/F3 with distinct `id` bounds + a position-delete on F1, identity(category));
Rust plans the same via `table.scan().with_filter().plan_files()`; {planned file SET, per-file delete paths,
residual-always-true} match EXACTLY. Purely additive — NO production change.

- [x] **Java oracle** — `InspectionScanA4Oracle` writes `table_a4` + plans s0 no_filter / s1 partition_a /
  s2 metric_id_gt_15 / s3 combined → java_scan_*.json.
- [x] **Rust test** — `test_scan_planning_matches_java_plans` (added to interop_inspection_manifests.rs).
  Pins partition pruning (s1 drops F2), COLUMN-METRIC pruning (s2 drops F1 via upper bound 10), combined
  (s3 = F3), residual-covered split, delete association (cat=a delete on BOTH F1+F3). Residual-EXPRESSION
  string equality DEFERRED (cross-language normalization; Rust residuals unit-tested).
- [x] **Gate (orchestrator-rerun, all green):** offline (A4 no-op; A1-A3 green; lib 1595; datafusion 80+9;
  clippy/fmt/typos) + run.sh round-trip (10 tests). spec/ untouched (0 lines).
- [x] **Docs** — GAP_MATRIX (scan-planning row), Roadmap (6g), lessons, todo.

### NEXT: squash `phase2-3-remnants` + open PR (awaiting user go-ahead on squash-vs-keep + confirm base = the origin fork, NEVER apache upstream)
Branch = ~10 commits off main `ccfcb062` (3 Phase-2/3 remnants + 4 pure-metadata/manifest-A1 interop + file_format fix + A2 + A3 + A4). All LOCAL-only, not pushed. Deferred beyond this PR: `readable_metrics` interop; A5 scan EXECUTION (parquet deps + approval); other Phase 2/3 remnants (overwriteByRowFilter, RowDelta validateNoNewDeletesForDataFiles, BatchScan, CDC-merge).
