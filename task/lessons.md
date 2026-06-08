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

# Lessons

Accumulated DO / DO NOT lessons. The operating manuals ([skills/](../skills/)) require reading this
file **in full at the start of every session**, and appending to it after **any** correction from
the user.

How to use it (see the manuals' §2):

- After any correction, append a **date-stamped** entry immediately.
- Write each as a concrete **DO** or **DO NOT** statement with the *why* and how to apply it.
- Supersede an outdated rule with a dated note (`_superseded YYYY-MM-DD: see ..._`) rather than
  editing the original in place.

---

<!-- Newest entries at the bottom. Example shape:

### YYYY-MM-DD
- **DO** carry context on every fallible Rust call (`.with_context(...)` / `.expect("msg")`).
  *Why:* a bare `.unwrap()` panic gives the operator no cause from logs alone.
- **DO NOT** edit upstream crate files to land a fork feature when an additive module would do.
  *Why:* it makes the next upstream merge conflict-prone. Prefer additive changes.
-->

### 2026-06-07
- **DO** run a new test module against the *full* parallel lib suite (`cargo test -p iceberg --lib`),
  not just its own filter, before declaring green. *Why:* adding ManageSnapshots' 12 tests increased
  parallel load and surfaced a latent flaky assertion in an unrelated test
  (`catalog/memory/catalog.rs::test_update_table`). The new code was correct; a pre-existing race only
  became visible under load. A filtered run (`... transaction::manage_snapshots`) hid it.
- **DO** treat strict `<` comparisons on millisecond wall-clock timestamps as flaky, and assert `<=`
  (plus a structural check that the change happened, e.g. metadata-log growth). *Why:* two metadata
  versions can legitimately share a `last_updated_ms`. Fixed `test_update_table` accordingly — a
  legitimate fix since we own the fork, and upstreamable.
- **Pattern: adding a transaction action.** Mirror `transaction/sort_order.rs` — builder struct that
  records intent, `#[async_trait] impl TransactionAction { commit(self: Arc<Self>, &Table) ->
  Result<ActionCommit> }` resolving against `table.metadata()` and returning
  `ActionCommit::new(updates, requirements)`; add `mod x;` + `use ...XAction;` + a `pub fn x()` ctor in
  `transaction/mod.rs`. The `TransactionAction` trait must be `use`d in tests to call `.commit()`.

### 2026-06-07 (multi-agent review remediation)
- **DO** verify a parity contract against the Java *source*, not intuition, before implementing the
  Rust side. *Why:* the first `fast_forward` cut wrongly required `to` to be a branch and rejected an
  absent `from`; Java `UpdateSnapshotReferencesOperation.replaceBranch` only requires `from` to be a
  branch and **auto-creates** an absent `from`. Read `/tmp/iceberg-java-ref` for the exact precondition
  checks (`Preconditions.checkArgument(...)`) and the early-return no-op cases.
- **DO** emit only refs whose final state differs from their original (no-op suppression) in
  metadata-mutating commit actions. *Why:* create-then-remove, fast-forward-to-same, replace-to-same
  should produce zero `TableUpdate`s — matches Java's "only changed refs" and keeps the commit
  idempotent at the catalog layer. Compare `metadata.refs.get(name) == working.get(name)` (both
  `SnapshotReference: PartialEq`).
- **DO** flag an agent finding you could NOT confirm rather than acting on it. *Why:* a reviewer
  claimed Java rejects non-positive retention (`> 0`); a grep of `SnapshotRef.java` showed no such
  `checkArgument`. Left unimplemented and tracked in `task/todo.md` as a follow-up — don't add
  validation that may diverge from Java on an unverified claim.
- **DO** test pre-existing-ref paths with a fixture that actually contains those refs. *Why:* the
  straight-line `make_v2_table()` has only `main`, so remove/replace/rollback-non-ancestor were
  untestable; build a forked fixture (`add_snapshot` a sibling + `set_ref` a branch/tag). `add_snapshot`
  validates the timestamp against the metadata's `last-updated-ms` (not just snapshot timestamps) — set
  the grafted snapshot's `timestamp_ms` after it.
- **DO** keep summary/headline sections in sync with the detail table when flipping a status. *Why:*
  the GAP_MATRIX + Roadmap "Headline gaps" kept listing `timestamp_ns`/column-defaults as missing after
  the matrix body flipped them to ✅. When you change a row's status, grep for the capability name and
  reconcile every mention.

### 2026-06-07 (UpdatePartitionSpec review remediation)
- **DO** replicate Java's `recycleOrCreatePartitionField` in the partition-spec *action*, not just lean
  on `TableMetadataBuilder.reuse_partition_field_ids`. *Why:* the builder recycles a historical field's
  *id* (matching on `(source_id, transform)`) but NOT its *name* — so an add with no explicit name came
  out with the generated default name (`y_bucket_8`) instead of the historical name (`y_bucket`) Java
  reuses. Java returns the whole historical field when `name == null || field.name().equals(name)`. Fix:
  the action searches `metadata.partition_specs_iter()` and, on match, sets BOTH the recycled `field_id`
  and the historical `name` on the `UnboundPartitionField`; the builder's id-recycling then no-ops on the
  pre-set id. Pin it with a multi-spec fixture whose historical field has a *custom* name (id-only checks
  pass even when the name is wrong).
- **DO** derive a transaction action's `TableRequirement`s from the updates it actually emits, the way
  Java's `UpdateRequirements` visitor does. *Why:* `UpdatePartitionSpec` emitted both
  `LastAssignedPartitionIdMatch` AND `DefaultSpecIdMatch` unconditionally, but under `add_non_default_spec`
  there is no `SetDefaultSpec` update, so Java attaches only `AssertLastAssignedPartitionId`. Emitting the
  default-spec guard anyway over-constrains the commit. Gate each guard on the update that induces it
  (`AddSpec` ⇒ last-assigned-partition-id; `SetDefaultSpec` ⇒ default-spec-id).
- **DO** prove a metadata-mutating action end-to-end by driving its emitted `TableUpdate`s through
  `TableMetadataBuilder` (`update.apply(builder)`), not just by inspecting the unbound `apply()` shape.
  *Why:* the unbound spec test never exercises the metadata layer's spec dedup, `LAST_ADDED` resolution,
  or default-spec switch — a no-op evolution (remove-then-re-add the same field) must dedup back to the
  existing spec id and NOT advance `last_partition_id`; only a round-trip catches a regression there.

### 2026-06-07 (UpdateSchema review remediation, Opus)
- **DO** assign fresh nested field ids LEVEL-ORDER (pre-order) when mirroring Java `TypeUtil.assignFreshIds`
  — it runs as a `CustomOrderSchemaVisitor` whose `struct` assigns ALL immediate field ids before the
  child futures evaluate, and whose `map` assigns key-id then value-id before either future. A natural
  depth-first Rust walk (assign this field's id, then immediately recurse its type) diverges the instant a
  nested field has a following sibling: for `map<struct,struct>` it yields value-id = key-id + (size of key
  subtree) instead of key-id + 1. *Why:* the ids are observable and Java pins them
  (`TestSchemaUpdate.testAddNestedMapOfStructs`: key=3, value=4, then 5..8, 9..10); divergent ids break
  interop and round-trip parity. A `last_column_id`-only assertion CANNOT catch this — assert per-field ids.
- **DO NOT** gate a union-by-name (or any Java-visitor port) type change on "apply only if it's a legal
  promotion." *Why:* Java `UnionByNameVisitor.updateColumn` computes `needsTypeUpdate = !isIgnorableType
  Update` and calls `api.updateColumn` UNCONDITIONALLY for a non-ignorable change, so an *incompatible*
  change reaches `updateColumn`'s guard and throws "Cannot change column type". Skipping the call (the old
  Rust behavior) silently drops a change Java rejects. `isIgnorableTypeUpdate` is: existing-primitive →
  ignorable iff incoming is a primitive that is a *narrowing* (`isPromotionAllowed(incoming, existing)`,
  reversed); existing-complex → ignorable iff incoming is also complex (the recursion handles inner edits).
- **DO** recurse a union/partner visitor THROUGH list elements and map key+values, not just structs.
  *Why:* Java reaches `<path>.element` / `<path>.key` / `<path>.value` via `PartnerIdByNameAccessors`, so a
  new field nested inside an existing `list<struct>`/`map<.,struct>` is added and an element/value type
  change is validated. A struct-only recursion silently no-ops those.
- **DO** reject case-insensitive lowercase-name collisions when building a Rust schema index, mirroring
  Java `TypeUtil.indexByLowerCaseName` ("Cannot build lower case index: a and b collide"). *Why:* Rust's
  `lowercase_name_to_id = name_to_id.iter().map(to_lowercase).collect()` silently overwrites colliding keys,
  so a case-insensitive add of `DATA` after `data` builds a corrupt schema; Java throws. Report the smaller
  field-id name first so the message is deterministic despite `HashMap` order (matches Java's first-visited
  ordering for sequential adds).
- **DO NOT** write a test that calls `.expect_err()` directly on a `commit()` future whose `Ok` type is
  `ActionCommit` — `ActionCommit` is not `Debug`. Insert `.map(drop)` before `.expect_err("…")` (or assert
  on `result.is_err()` + re-extract the error). *Why:* `Result::expect_err` requires `T: Debug`.

### 2026-06-07 (Increment 4 — ManageSnapshots tail + UpdateSchema defaults, BUILDER Opus)
- **DO** locate `rollbackToTime` in `core/SetSnapshotOperation.java` (`SnapshotManager.rollbackToTime`
  delegates to `transaction.setBranchSnapshot().rollbackToTime`). The rule is
  `findLatestAncestorOlderThan(base, ts)`: walk `SnapshotUtil.ancestorIds(currentSnapshot)` (the MAIN
  parent chain — the existing `is_ancestor_of` walk) and pick the ancestor with the MAX `timestampMillis`
  that is **strictly `<` ts**; error "Cannot roll back, no valid snapshot older than: {ts}" if none. *Why
  the strict `<` matters:* ts == current's own timestamp does NOT select current (it picks the next-older
  ancestor); ts > current selects current (a no-op, suppressed at emit). A `<=` would wrongly keep current
  on the exact-equal boundary. Non-ancestor siblings are never visited by the parent-chain walk, so they
  can never be chosen — no extra guard needed.
- **DO** find the retention positivity checks in `api/SnapshotRef.java` **Builder setters** (lines
  ~154–177), NOT in the `SnapshotRef` ctor — that is why an earlier grep "of `SnapshotRef.java`" missed
  them (it likely scanned the ctor/equals region). Each setter has `Preconditions.checkArgument(value ==
  null || value > 0, "…")` with these EXACT messages: "Min snapshots to keep must be greater than 0",
  "Max snapshot age must be greater than 0 ms" (note the trailing " ms"), "Max reference age must be
  greater than 0" (no unit). `null` is allowed (clears the field); our `set_*` API always sets a concrete
  value, so only the `<= 0` case occurs. *Supersedes the 2026-06-07 "unverified retention `>0`" follow-up
  — it IS in Java, just in the Builder.*
- **DO** allow a **required column add WITH a default WITHOUT `allow_incompatible_changes`** —
  `core/SchemaUpdate.internalAddColumn` gates on `defaultValue != null || isOptional ||
  allowIncompatibleChanges` (line ~160). The default backfills existing rows, so it is a compatible change.
  A required add WITHOUT a default to a non-empty schema stays incompatible. *Why it's easy to get wrong:*
  the obvious guard `required && !flag` rejects the legal defaulted case; the correct guard is `required &&
  default.is_none() && !flag`.
- **DO** set BOTH `initial_default` AND `write_default` on an *added* column (Java `addColumn` calls
  `.withInitialDefault(default).withWriteDefault(default)`), but on `updateColumnDefault` set ONLY
  `write_default` (Java comment: "write default is always set and initial default is only set if the field
  requires one"). *Why:* the initial default is the existing-row backfill (fixed at add time); a later
  default change only affects future writes.
- **DO** validate a Rust `Literal` default against the column type via
  `literal.clone().try_into_json(&field_type)` — the Rust `NestedField::with_initial_default` /
  `with_write_default` setters do NOT validate, but the serde `From<NestedField>` path calls
  `try_into_json` with a panicking `.expect`. Running `try_into_json` at add/update time is the canonical
  compatibility check (mirrors Java `castDefault`'s `defaultValue.to(type)`): a non-primitive type rejects
  with "Invalid default value… (must be null)" and an incompatible primitive rejects with "Cannot cast
  default value to…". Passing it guarantees no later serialization panic. *Note:* `Type` has `is_primitive`
  / `is_struct` / `is_nested` but NOT `is_list` / `is_map`; use `!is_primitive()` for "is nested".
- **DO** reclassify `cherrypick` as **Phase-2-gated**, not a metadata op: Java `SnapshotManager.cherrypick`
  → `transaction.cherryPick()` whose operation **extends `MergingSnapshotProducer`** and replays data
  files. It belongs to the write engine (Phase 2), so it is out of scope for the metadata-only
  `ManageSnapshots` surface even though Java co-locates it on the same API.

### 2026-06-07 (Increment 4 — REVIEWER Opus)
- **DO** add a test for EVERY conditional relaxation branch, not just its happy/sad headline. *Why:* the
  Increment-4 builder shipped `add_*_with_default` (defaulted required add) and `update_column_default`
  (write-default only) but left the *interaction* branch `is_defaulted_add` (an added field's
  `initial_default.is_some()` lets `require_column` skip the incompatible-change gate) UNTESTED. Java pins it
  with `testAddColumnWithDefaultToRequiredColumn` (defaulted add → require succeeds) AND
  `testAddColumnWithUpdateColumnDefaultToRequiredColumn` (add + updateColumnDefault → require FAILS, because
  updateColumnDefault sets only write_default, not initial_default). The two together are the only thing that
  distinguishes the two default-setting paths at the require boundary; mutation-verify by swapping
  `initial_default` → `write_default` (negative test catches it) and by forcing `is_defaulted_add = false`
  (positive test catches it). When porting a Java API, grep its test file for the *combinations* of the new
  methods, not just each method alone.
- **DO** recognize when a same-call validation makes a downstream `.expect` panic-proof, and say so instead
  of hunting for a stronger check. *Why:* `validate_default` runs `Literal::try_into_json(&field_type)` —
  the EXACT call the serde `From<NestedField>` path (datatypes.rs:591-592) later runs under a panicking
  `.expect`. Identical input + identical type ⇒ if validation passes, the serde call returns `Ok`, so the
  panic is unreachable. That is the load-bearing safety property; the residual Rust-vs-Java parity gaps
  (the `(_, UInt128|Binary)` wildcard arms accept a UUID/binary literal for any primitive = too lenient; the
  strict primitive-pair match rejects an int literal on a long column = too strict) only bite a caller who
  hand-builds a type-mismatched `Literal`, which the strongly-typed Rust `Literal` makes unnatural. Track as
  a parity note, do not "fix" by diverging from the serde contract.

### 2026-06-07 (Increment 5 — UpdateSchema interop pilot, BUILDER Opus)
- **DO** drive an interop test's evolution through the PUBLIC API (`MemoryCatalog` register-table +
  `Transaction` + `ApplyTransactionAction::apply` + `Transaction::commit`), not the action's `commit()`
  directly. *Why:* `TransactionAction::commit` is `pub(crate)` and the `transaction::update_schema` module
  is private, so an external integration test in `crates/iceberg/tests/` CANNOT name `UpdateSchemaAction`
  or call `.commit()` (only in-crate unit tests can). The catalog path is the only public way to apply an
  action offline and read the evolved metadata — and it is strictly stronger (it also runs the
  optimistic-concurrency `TableRequirement` checks). Do NOT widen production visibility to make the test
  compile; route through the catalog instead.
- **DO** use a `MemoryCatalog` built with `with_storage_factory(Arc::new(LocalFsStorageFactory))` over a
  `tempfile::tempdir()` warehouse when an interop test must register a table from an EXACT pre-written
  metadata file. *Why:* the default `MemoryCatalog` storage is in-memory (`MemoryStorageFactory`) and
  `register_table` reads the metadata via the catalog's OWN FileIO — it never sees a file you wrote with a
  separate FileIO. Two gotchas the catalog enforces: the metadata file must live under a `metadata/`
  subdirectory of the table location, AND be named `<version>-<uuid>.metadata.json` (e.g.
  `00000-00000000-0000-0000-0000-000000000000.metadata.json`) — a bare `base.metadata.json` fails with
  "Invalid metadata file name format". (`crates/iceberg/src/catalog/metadata_location.rs`.)
- **DO** generate Java interop fixtures at format version 3 for any scenario with a column INITIAL default.
  *Why:* Java `iceberg-core` 1.10.0 rejects a non-null initial default on V2 metadata ("non-null default
  (...) is not supported until v3") at `TableMetadata` build time. The Rust side does NOT enforce this
  V3-only rule (`validate_default` only checks type-convertibility), so it would happily emit a default on
  V2 metadata that Java then refuses to read — a real latent parity gap the interop test surfaces. Match
  Java's contract (V3 base for defaults) and flag the missing Rust V3-gate as a tracked parity note.
- **DO** reference renamed columns by their ORIGINAL name in a move within the SAME UpdateSchema sequence.
  *Why:* Java `SchemaUpdate.findForMove` resolves names against `addedNameToId` then the ORIGINAL schema —
  a rename is recorded in `updates` (keyed by field id) and is NOT visible to name resolution until the
  schema is rebuilt. So `renameColumn("email","email_address").moveFirst("email")` is correct;
  `moveFirst("email_address")` throws "Cannot move missing column". The Rust `find_for_move` mirrors this
  exactly — a scenario that moved by the new name was the test's bug, not the code's.
- **DO** compute an interop fixture's evolved last-column-id as `max(base.lastColumnId,
  evolved.highestFieldId)`, never the evolved schema's `highestFieldId()` alone. *Why:* a delete lowers
  `highestFieldId()` but Iceberg NEVER reuses or lowers `lastColumnId` (ids are permanently retired);
  `TableMetadata.buildFrom(base).setCurrentSchema(evolved, evolved.highestFieldId())` throws "Invalid last
  column ID: 2 < 3 (previous last column ID)" on a delete scenario. Java's `addSchema` does the same
  `Math.max` internally.
- **DO** compare interop metadata by PARSING both files into the Rust model and asserting
  `Schema::as_struct() == other.as_struct()` (+ explicit identifier ids / current-schema-id /
  last-column-id), NOT raw JSON bytes. *Why:* `StructType: PartialEq` recurses field id + name + type +
  required + doc + default in one shot, which IS the field-id-level identity contract; Jackson and
  `serde_json` differ in key order and whitespace, so byte-equality would be both too strict (false
  failures) and beside the point. Add a scenario-specific exact-id assertion for the nested level-order
  case — a `last_column_id`-only check cannot catch an interior-id divergence that lands on the same max.

### 2026-06-07 (Increment 5 — UpdateSchema interop pilot, REVIEWER Opus)
- **DO** mutation-test BOTH directions of an interop harness before trusting a ✅, by corrupting the
  fixture each direction reads and confirming the assertion fires. *Why:* an interop test that compares a
  file against itself, or whose "verify" recomputes the same value it checks, passes tautologically.
  Confirmed Dir-1 by editing a Java-written `java_evolved` field name (`count`→`kount`) → the Rust struct
  assertion fails (panic shows left=Rust vs right=Java); Dir-2 by shrinking a Rust-written identifier set
  → `mvn verify` exits 1. Both `rust_evolved` and `java_evolved` differ byte-wise (independent
  serializers), so neither side compares a file to itself.
- **DO** distinguish a test-FUNCTION count from a SCENARIO count when a reviewer flags "3 passed but 7
  scenarios." *Why:* the UpdateSchema Dir-1 reports "3 tests" but one of them
  (`test_update_schema_interop_all_scenarios`) LOOPS over all 7 in a `SCENARIOS` const — every scenario
  IS exercised. Read the loop, don't infer coverage from the headline number.
- **DO** treat "Rust emits metadata Java would REJECT" as a real parity hole even when the interop test is
  green — green only proves the cases the fixtures EXERCISE. *Why:* the UpdateSchema pilot generates the
  two default-bearing scenarios at V3 because Java `Schema.checkCompatibility` (`api/Schema.java:619`,
  called via `TableMetadata$Builder.addSchemaInternal`) rejects a non-null `initial_default` on
  `format_version < 3`. Rust has NO such guard (`TableMetadataBuilder::add_schema` never calls
  `check_compatibility`; `validate_default` only checks type-convertibility), so a defaulted add on a V2
  table SUCCEEDS and emits Java-unreadable metadata — and the interop test sidesteps it by using V3. Pin
  the hole with a divergence test (`test_v2_default_is_emitted_without_v3_guard_known_divergence`) and an
  HONEST GAP_MATRIX caveat; do NOT let a "V3 base for defaults" fixture note quietly imply the Rust side
  enforces the rule. ✅ is defensible only because the hole is narrow + conditional (V1/V2 + a column
  default) and every other surface is bidirectionally interop-proven; a wider or unconditional hole would
  force 🟡.
- **DO** verify a TEST-ONLY oracle never enters the Cargo graph and its build dir is git-ignored. *Why:*
  `dev/java-interop/` is Java driven by Maven; `cargo build`/`cargo test` must never invoke it, and its
  `target/` must not be committed. Confirmed: `git check-ignore dev/java-interop/target/classes` →
  ignored (root `.gitignore` `target`); `git status` shows only `?? dev/java-interop/` (no staged
  cruft); `TransactionAction`/`commit` stayed `pub(crate)` (visibility not widened to make the test
  compile — the public `Transaction::update_schema()` + catalog-commit path is used instead).
- **DO** expect non-deterministic interop-fixture churn from Java's `newTableMetadata` (`table-uuid`,
  `last-updated-ms`, time-logs regenerate every run) and confirm a regen is STRUCTURALLY identical, not
  byte-identical. *Why:* `run.sh` re-running mutates the committed `base`/`java_evolved` on those fields
  only; the structural comparison the tests assert is unaffected, but it produces noisy diffs. Track it;
  don't mistake it for a logic regression.

### 2026-06-07 (Increment 6 — V3 initial-default guard, BUILDER Opus)
- **DO** wire a Java-parity schema guard into `TableMetadataBuilder::add_schema`, not into the transaction
  action's `commit()`. *Why:* `UpdateSchemaAction::commit` only EMITS a `TableUpdate::AddSchema` (it does
  not call `add_schema`); the guard belongs at the single choke point every add-schema path flows through —
  `TableUpdate::AddSchema::apply` (catalog/mod.rs) calls `builder.add_schema(schema)`, so a guard there
  covers the UpdateSchema action, CTAS, and every catalog commit at once, exactly mirroring Java's
  `TableMetadata$Builder.addSchemaInternal` calling `Schema.checkCompatibility`. A guard in `commit()` would
  miss CTAS/raw-builder paths.
- **CONSEQUENCE: a guard in `add_schema` only fires on the APPLY path, not on `run()`-style action tests.**
  *Why:* tests that call `action.commit()` and merely inspect the emitted `AddSchema` updates (the `run()`
  helper in `update_schema.rs`) never reach the metadata builder, so they're unaffected by the guard; ONLY
  tests that drive updates through `TableMetadataBuilder` (the `apply_updates()` helper, or a full catalog
  commit) hit it. Before adding such a guard, grep the test file for which tests APPLY vs. merely INSPECT —
  the blast radius is the apply-path subset, not every test that uses the relevant feature. Here only
  `test_emitted_schema_round_trips_defaults` (and the catalog-driven interop tests) needed reconciling
  beyond the one the brief named.
- **DO** reach ALL fields (incl. nested struct/list/map descendants) via `Schema::field_id_to_fields()` —
  the recursive id→field index built by `index_by_id` (a `SchemaVisitor` walk) — when mirroring a Java rule
  that iterates `schema.lazyIdToField().values()`. *Why:* a hand-rolled top-level-only loop silently lets a
  default (or any per-field violation) buried inside a nested struct through. `field_id_to_name_map()` gives
  the dotted column name (`payload.flag`) for the error, matching Java's `findColumnName`. Pin the nested
  reach with a test asserting the dotted path appears in the rejection message — a top-level-only guard can
  never produce it.
- **DO** gate `initial_default` ONLY, never `write_default`, when mirroring Java
  `Schema.checkCompatibility`'s `DEFAULT_VALUES_MIN_FORMAT_VERSION` check (`api/Schema.java:619`). *Why:* Java
  checks `field.initialDefault() != null` only — a write default affects future writes, not how existing
  rows are read, so it is legal on v1/v2. A test that adds a v2 schema with only a `write_default` must SUCCEED
  (`test_add_schema_with_write_default_only_allowed_on_v2`); gating it would wrongly reject legal metadata.
- **DO** order accumulated per-field problem messages by field id before joining them, mirroring Java's
  `Maps.newTreeMap()`. *Why:* iterating a Rust `HashMap` yields nondeterministic order, so a multi-field
  violation would produce a flaky error message; Java accumulates into a `TreeMap` keyed by field id for a
  stable order. Sort by id, then join with the Java separator (`"\n- "`).
- **DO** use `{:?}` (Debug) to render a Rust `Literal` in an error message — it has no `Display` impl. *Why:*
  Java renders the offending default via `Literal.toString()`; the Rust `Literal` only derives `Debug`, so
  `{:?}` (e.g. `Primitive(Long(7))`) is the faithful analogue. The message STRUCTURE (col name, value,
  "not supported until v3") mirrors Java; the value rendering differs by language and that is expected — assert
  on the substring `"is not supported until v3"` + the column name, not the exact value text.
- **DO** flag (not build) a co-located-but-broader parity item. *Why:* Java's `checkCompatibility` gates BOTH
  V3-only initial-defaults AND V3-only TYPES (`MIN_FORMAT_VERSIONS`: `timestamp_ns`/`variant`/`unknown`/
  `geometry`/`geography`) in one method. The type gate is tied to landing those V3 types and is a separate
  capability; implementing it here would balloon the scope. Left `Schema::check_compatibility` structured so
  the type gate slots into the same method later, and tracked it as a follow-up in `task/todo.md` — don't
  build adjacent scope just because the Java method co-locates it.

### 2026-06-07 (Increment 6 — V3 initial-default guard, REVIEWER Opus)
- **DO** check whether a "tracked future" gate is ALREADY live for a partially-landed feature before
  characterizing it as future-only. *Why:* the Increment-6 follow-up called the V3-only TYPE gate
  (`MIN_FORMAT_VERSIONS`) "tied to landing those V3 types," but `PrimitiveType::TimestampNs` is ALREADY in
  Rust (GAP_MATRIX "V3 types: timestamp_ns" ✅) and no type-version guard exists anywhere in `spec/` — so
  `UpdateSchema.add_column("ts", timestamp_ns)` on a V2 table emits Java-rejected metadata TODAY ("Invalid
  type for ts: ... is not supported until v3"). The other four V3 types are unimplemented, so for them the
  gate really is future. Sharpened the follow-up to say the `timestamp_ns` slice is LIVE, not future, so the
  next agent doesn't deprioritize it. The `UpdateSchema` ✅ stands for the **initial-default** rule only (it
  never claimed the type gate); the type gate is a distinct, narrowly-live residual.
- **DO** mutation-verify a guard from BOTH failure directions when reviewing: disable it (the rejection tests
  must fail → it is load-bearing) AND over-broaden it (a legal case must fail → it does not over-fire).
  *Why:* forcing `check_compatibility` to early-`Ok` fails exactly the 4 rejection tests (top-level/nested V2
  + the unit test + the action-path V2 test) and NOTHING else; widening it to also gate `write_default` fails
  exactly `test_add_schema_with_write_default_only_allowed_on_v2`. The two mutations together prove the guard
  fires iff `initial_default.is_some() && format < v3` — the precise Java contract — not merely "fires
  sometimes." A one-direction mutation (disable only) would miss an over-firing guard.

### 2026-06-07 (Increment 7 — V3-only TYPE gate in check_compatibility, BUILDER Opus)
- **DO** model Java's `Schema.MIN_FORMAT_VERSIONS` (a `TypeID → minVersion` map) as a small
  `fn min_format_version(ty: &Type) -> Option<FormatVersion>` returning `Some(V3)` for the V3-only types and
  `None` otherwise, then gate on `format_version < min` per field. *Why:* it isolates the "which types are
  V3-only" knowledge in one place, keeps the per-field check a one-liner, and makes adding a future V3 type
  (`variant`/`unknown`/`geometry`/`geography`) a single `match` arm. Only `PrimitiveType::{TimestampNs,
  TimestamptzNs}` are representable in Rust today (both = Java `TIMESTAMP_NANO`); the other four are not in the
  Rust `Type`/`PrimitiveType` enums, so leave a comment, not a stub.
- **DO** fold a co-located Java check into the SAME single field-iteration pass and the SAME problem
  accumulator, in Java's order. *Why:* Java's `checkCompatibility` loops `lazyIdToField().values()` ONCE,
  checking the type rule (`MIN_FORMAT_VERSIONS`) BEFORE the initial-default rule, putting both into one
  `TreeMap<fieldId,String>` and throwing one combined `IllegalStateException`. The Rust mirror iterates
  `field_id_to_fields()` once, pushes the type problem then the default problem into one `Vec<(field_id,
  String)>`, stable-sorts by field id, and joins into the one `"Invalid schema for v{N}:"` error. A field that
  violates both rules surfaces both lines (type first); two different fields surface in id order. Pin it with a
  test that builds a V2 schema with a V3-typed field AND a separately-defaulted field and asserts BOTH
  substrings appear, type-before-default — proves the shared accumulator, mirroring Java's TreeMap. (NOTE: in
  Java, because both `problems.put(fieldId, …)` use the SAME field id when one field has BOTH a V3 type and a
  V3 default, the second put OVERWRITES the first — only the default message survives for that one field. The
  Rust `Vec` keeps both for a single field; this is a benign over-reporting divergence that only differs when
  ONE field is simultaneously a V3 type and carries a default, which the strongly-typed builder makes
  unusual. The cross-FIELD accumulation — the case the brief asked to prove — matches Java exactly.)
- **DO** confirm a build-path guard does NOT trip the parse path before declaring the blast radius safe.
  *Why:* `check_compatibility` is called only from `TableMetadataBuilder::add_schema` (the build/commit
  path); the `TableMetadataV2 → TableMetadata` `TryFrom` (parse path) constructs directly and never calls it,
  so reading an existing V3 `timestamp_ns` metadata fixture is unaffected — exactly like Java, where
  `checkCompatibility` lives in `addSchemaInternal`, not the parser. A guard that fired on parse would reject
  metadata that is already legally on disk.
- **DO** audit for tests that build a `<v3 TableMetadata` with the newly-gated type via
  `add_schema`/`from_table_creation`/`add_current_schema` BEFORE assuming reconciliation is needed. *Why:* the
  Increment-7 brief expected breakage, but a crate-wide grep showed every `timestamp_ns`/`timestamptz_ns`
  usage was in transform/arrow/avro/manifest/datum tests that build a bare `Schema` (via
  `Schema::builder().build()`, which does NOT call `check_compatibility`) or match on the type — none flowed
  through the metadata builder below v3. So ZERO existing tests needed changing. The build path vs. the
  bare-`Schema`-build path is the distinction that bounds the blast radius.

### 2026-06-07 (Increment 7 — V3-only TYPE gate in check_compatibility, REVIEWER Opus)
- **DECISION (TreeMap-vs-Vec divergence): KEEP Rust's both-report `Vec`, do NOT match Java's TreeMap
  last-wins.** Java keys `problems` by field id in a `TreeMap`, so a SINGLE field that is BOTH a V3-only
  type AND carries a non-null initial default collapses to ONE message (the second `put` overwrites the
  first — only the default line survives). Rust's `Vec<(field_id, String)>` reports BOTH lines for that
  one field. *Justification:* accept/reject is identical (both reject); the extra line is strictly more
  informative (Java arbitrarily hides the type problem behind the default); message text already differs
  by language (Rust renders `Literal` via `Debug`, Java via `toString`) so byte-identical messages were
  never the contract; the case is vanishingly narrow (one field simultaneously a V3 type AND defaulted on
  a <v3 table, unnatural for the strongly-typed builder). The CROSS-field case (two different fields →
  field-id order) — the parity-load-bearing one — already matches Java exactly. *Why it must be pinned:*
  a kept divergence that is UNtested is indistinguishable from an accidental one; if a later refactor
  silently drops one line, nobody notices. Added
  `test_check_compatibility_single_field_both_type_and_default_reports_both_lines` (one `timestamp_ns`
  field with an `initial_default` on V2 → BOTH lines, type before default). Mutation-verified: emulating
  Java's last-wins (dedup to a `BTreeMap` keyed by field id) fails EXACTLY this test and leaves the
  cross-field test green — proving it pins the both-report, not just "rejects."
- **DO add the single-field-both test even when the BUILDER documented the divergence in prose.** *Why:*
  the Increment-7 builder correctly chose the `Vec` and wrote the divergence into lessons/todo, but only
  added the CROSS-field accumulation test (two distinct field ids — which CANNOT collide in either Java or
  Rust and so cannot distinguish the two designs). The one test that actually exercises the divergence
  (a single field hitting BOTH rules) was missing — a documented-but-unpinned divergence. A prose note is
  not a regression guard; the test is.
- **DO `git stash` the production source and re-run a failing target on the clean tree before blaming
  your change.** *Why:* `cargo test -p iceberg` (all-targets) shows 5 doctest COMPILE failures
  (`lib.rs:24`, `writer/mod.rs:{42,117,257,321}`) — but all are `tokio::main` "default runtime flavor is
  `multi_thread`, but `rt-multi-thread` is disabled" errors, reproduced IDENTICALLY with the
  `check_compatibility` change stashed (and under `--all-features`). They are a pre-existing
  environment/feature artifact, NOT introduced by the guard. A compile error in an unrelated writer
  doctest is a strong tell that the failure is environmental, since a runtime guard cannot cause a
  doctest not to compile.

### 2026-06-07 (Increment 8 — UpdatePartitionSpec INTEROP, BUILDER Opus)
- **DO drive the partition-spec interop oracle through a REAL `BaseUpdatePartitionSpec`
  (`new BaseTable(ops, name).updateSpec()…commit()` over an in-memory `TableOperations`), NOT the
  `@VisibleForTesting BaseUpdatePartitionSpec(int, PartitionSpec, …)` ctors.** *Why:* those test ctors set
  `base = null` (line 97), and `recycleOrCreatePartitionField` only recycles when `formatVersion >= 2 &&
  base != null` (line 124) — so the recycling scenario would silently NOT recycle and the fixture would
  prove nothing. The in-memory `TableOperations` only needs `current()`/`refresh()`/`commit()` (swap the
  held metadata) + a no-op `metadataFileLocation`; `io()`/`locationProvider()` can throw (a partition-spec
  commit never touches data files). `BaseTable(TableOperations, String)` is public.
- **DO build a multi-spec recycling fixture by EVOLVING a non-default spec through the real action, not by
  `buildFrom(base).addPartitionSpec(twoIndependentSpecs)`.** *Why:* `PartitionSpec.builderFor(schema)`
  starts each spec's field ids at 1000 and `freshSpec` PRESERVES the passed `field.fieldId()` — so two
  independently-built specs BOTH carry field id 1000, and a recycle of the second collides
  (`Cannot use field id more than once in one PartitionSpec: 1000`) in the Rust metadata builder. A real
  V2 table never looks like that: a historical non-default spec's field gets the next sequential id (1001)
  via `BaseUpdatePartitionSpec.assignFieldId`. Drive `updateSpec().addNonDefaultSpec().addField(...)` so
  the field gets 1001; then the recycle reuses 1001 (distinct from the default spec's 1000) and the
  realistic-id assertion (1001, not a fresh 1002) is the discriminating check.
- **DO recognize that Rust's partition-name↔schema collision check is identity-ONLY but Java's BIND path
  allows any transform sourced from its own column.** *Why:* Java has TWO name-check strictnesses keyed on
  `PartitionSpec.Builder.checkAndAddPartitionName(name, sourceColumnId)` — the public typed builders
  (`.bucket()`/`.truncate()`/`.year()`/…) pass `sourceColumnId=null` (STRICT: reject any schema-name
  collision), while the bind path (`UnboundPartitionSpec.bind` → `add(sourceId, fieldId, name, transform)`,
  line 618) passes the source id (LENIENT: allow if the colliding schema field's id == the partition's
  source id, regardless of transform). The Rust `PartitionSpecBuilder` collapses both into one identity-only
  rule, which wrongly rejects the V1 **void replacement** (`void(category)` named `category`, sourced from
  `category`) when its emitted spec is bound. Fix: relax `check_name_does_not_collide_with_schema` +
  `validate_partition_field_names` to **identity OR void** (source-id-gated) — the narrowest Java-faithful
  change; do NOT broaden to bucket/truncate (Rust can't split the two paths without a `checkConflicts`-style
  flag, and the existing `test_builder_collision` pins bucket-via-builder rejection — keep it green).
- **DO let the interop test surface bind-path bugs the unit tests can't.** *Why:* the `update_partition_spec.rs`
  unit tests inspect the unbound `apply()` shape (`test_remove_v1_replaces_with_void`) — they NEVER bind the
  emitted spec, so the V1-void name-collision divergence was invisible to them. The interop test goes through
  the FULL catalog commit (`register_table` → `Transaction::commit` → `add_partition_spec` → `bind`), which is
  the only path that exercises the bind-time name check. An action whose unbound output is correct can still
  emit metadata the metadata builder rejects; only an end-to-end commit proves the round-trip.
- **DO compare evolved partition specs structurally via `PartitionField: PartialEq` (source-id/field-id/
  name/transform) + `last_partition_id`, on the DEFAULT spec, parsed from the metadata — not raw JSON.**
  *Why:* same rationale as the schema interop (`StructType: PartialEq`): Jackson and `serde_json` differ in
  key order/whitespace; field-id-level identity is the contract. Java's `PartitionField.equals` covers the
  same four fields, so a `List<PartitionField>.equals` on both sides is the exact mirror. Add a
  scenario-specific exact-id+name assertion for recycling — a field-id-only check can't catch a recycled-id-
  but-generated-name regression (the Increment-2 review bug).

### 2026-06-07 (Increment 8 — UpdatePartitionSpec INTEROP, REVIEWER Opus)
- **DECISION (the guard relaxation is HONEST ✅, the residual divergence is Rust-STRICTER): the guard fix
  is safe in the only direction that matters.** Java's bind-path rule is
  `PartitionSpec.Builder.checkAndAddPartitionName(name, sourceColumnId)` (api/PartitionSpec.java:401-427,
  `checkConflicts=true` by default, reached for EVERY field via `UnboundPartitionSpec.bind →
  copyToBuilder → builder.add(sourceId, [fieldId,] name, transform) → checkAndAddPartitionName(name,
  sourceId)`): when `sourceColumnId != null` (identity/alwaysNull/`add`), look up `schemaField` by name; if
  the SOURCE column still exists in the schema, require `schemaField == null || schemaField.fieldId() ==
  sourceColumnId` — and if the source column was DROPPED, SKIP the name check entirely. The rule is
  **transform-AGNOSTIC** (any transform passes as long as name↔source-id holds) and has a **source-dropped
  bypass**. The Rust relaxed guard is narrower: `(identity OR void) AND collision.id == source_id`. Two
  residual divergences, BOTH Rust-STRICTER (Rust rejects valid-in-Java; never accepts invalid-in-Java):
  (a) a bucket/truncate/year field explicitly NAMED after its own source column — Java accepts (source-id
  matches), Rust rejects (not identity/void); (b) a collision where the source column was dropped — Java
  skips/accepts, Rust still compares ids and rejects. Neither can emit Java-unreadable metadata, so ✅ is
  honest WITHOUT a caveat: the dangerous direction (Rust accepts a spec Java rejects) does NOT exist here.
  Verified empirically with a throwaway probe (bucket-named-after-own-source → Rust REJECTS). The
  `add_partition_spec` path can only get stricter than Java, never looser.
- **DO confirm a relaxed collision guard still REJECTS the unrelated-source case before trusting ✅.** *Why:*
  the dangerous regression would be a `void`/identity field named after an UNRELATED schema column slipping
  through. Mutation-verified the source-id gate is load-bearing: forcing the source-id check past in BOTH
  layers (`partition.rs` `check_name_does_not_collide_with_schema` AND `table_metadata_builder.rs`
  `validate_partition_field_names`) makes a void-named-after-a-different-column spec ACCEPTED — caught by the
  partition.rs negative tests. Tightening the relaxation back to identity-only fails the V1-void unit tests
  (both layers) AND the `remove_field_v1_void` interop scenario. The guard fires iff
  `(identity|void) && collision.id == source_id` — the precise narrowed-Java rule.
- **DO add a public-entry-point negative test even when a downstream layer already catches the bad case.**
  *Why:* `add_partition_spec` runs `validate_partition_field_names` (builder layer) THEN
  `PartitionSpecBuilder::build` (partition.rs layer); the builder-layer `has_matching_source_id` rejection
  branch had NO test — a single-layer mutation (`has_matching_source_id = true`) failed nothing because the
  partition.rs guard caught it. Added `test_partition_spec_evolution_rejects_void_named_after_a_different_
  source_column` driving the rejection end-to-end through `add_partition_spec`; it pins the source-id gate
  as a whole (the both-layers mutation fails it). Defense-in-depth still needs a test at the public door.

### 2026-06-07 (Increment 9 — ManageSnapshots INTEROP, BUILDER Opus)
- **DO build a snapshot-history base by adding ALL snapshots first, then refs, with `main` set LAST via
  `setBranchSnapshot(long snapshotId, branch)` — and RE-PARSE the written base from disk before evolving.**
  *Why:* two coupled Java traps when assembling a `TableMetadata` with a history for ManageSnapshots. (a)
  `setBranchSnapshot(Snapshot, main)` stamps a snapshot-log entry; doing it mid-build (before the other
  refs) plus the `setRef`-for-main ordering can land `lastUpdatedMillis` on an OLD snapshot's timestamp,
  tripping `"Invalid update timestamp …: before last snapshot log entry"` at `TableMetadata.<init>`. Adding
  all snapshots, then `dev`/`stable`, then `main` LAST keeps the single snapshot-log entry at CURRENT's ts
  and `lastUpdatedMillis` monotone. (b) `TableMetadata.buildFrom(base)` COPIES `base.changes`
  (`AddSnapshot` entries), and `internalApply`/`setRef` use `isAddedSnapshot(id)` to decide a rollback's
  snapshot-log `timeOfChange` (`snapshot.timestampMillis()` for added, else `lastUpdatedMillis`). Evolving
  from the freshly-built in-memory base makes ROOT look "just added" → rollback stamps ROOT's old ts →
  same guard trips. A re-parsed (`TableMetadataParser.fromJson`) base has EMPTY changes — exactly what the
  Rust test and the Java `verify` step both load. Generate must round-trip the base through disk too.
- **DO recover the typed ref model by serializing the evolved `TableMetadata` to a `serde_json::Value` and
  re-parsing its `refs` object into `HashMap<String, SnapshotReference>` — there is NO public `refs()`
  accessor.** *Why:* `TableMetadata.refs` is `pub(crate)`, and the only public ref accessor
  (`snapshot_for_ref`) returns a `Snapshot`, which DROPS the branch-vs-tag kind and the three retention
  fields — the exact things the ManageSnapshots interop must compare. `SnapshotReference`/`SnapshotRetention`
  are public and serde-round-trippable, so `serde_json::to_value(&meta)["refs"]` → typed map gives the full
  ref model with zero production change. Compare the refs map (`SnapshotReference: PartialEq` covers
  snapshot-id + kind + all retention) + `current_snapshot_id()`; do NOT compare the snapshot list (ref ops
  leave it unchanged) or raw JSON (Jackson vs serde_json key-order/whitespace).
- **FLAG, DON'T BURY: Rust's `_serde::SnapshotV2.sequence_number` is required; Java's `SnapshotParser`
  omits `sequence-number` when it equals `INITIAL_SEQUENCE_NUMBER` (0).** *Why it bites:* a Java-written V2
  metadata whose snapshot has `sequence-number == 0` does NOT parse in Rust (`data did not match any variant
  of untagged enum TableMetadataEnum`). This is a genuine latent READ divergence — but the spec
  (`format/spec.md` line 949) marks snapshot `sequence-number` **required** in V2/V3, so adding
  `#[serde(default)]` would diverge from the spec and could mask malformed metadata; and a seq-0 V2 snapshot
  only arises as a V1-carryover artifact (real V2 assigns seq ≥ 1). *Decision for a ref-operation
  increment:* did NOT touch production — out of scope, and the spec-vs-Java tension needs its own
  adjudication. Sidestepped by giving the fixture V2-realistic sequence numbers (1/2/3, not 0/1/2), which
  keeps Java emitting every `sequence-number` and the fixture spec-faithful. Pinned the observation in
  GAP_MATRIX/Roadmap/README so the reviewer (or a future Phase-1-tail increment) can decide whether to relax
  Rust's reader.
- **DO revert incidental fixture churn from `run.sh` that lands outside your increment's scope.** *Why:*
  `run.sh` regenerates ALL THREE capabilities' fixtures, and `base.metadata.json` carries a random
  `table-uuid` + a wall-clock `last-updated-ms`, so a full run shows the schema/partition fixtures as
  "modified" even though only the noise changed. They are NOT in this increment's allowed-edit set —
  `git checkout -- testdata/interop/{update_schema,update_partition_spec}` restored them byte-for-byte. Only
  the new `manage_snapshots/**` fixtures are mine to add.

### 2026-06-07 (Increment 9 — ManageSnapshots INTEROP, REVIEWER Opus)
- **DO read the spec's version-compat "Reading vN metadata for vN+1" section, not just the field
  required/optional table, before calling a reader-leniency a spec divergence.** *Why:* the builder
  flagged the V2 `sequence-number` read gap (Java omits it when 0; Rust's `_serde::SnapshotV2.sequence_number`
  is a required `i64` with no `#[serde(default)]`, so a seq-omitted V2 metadata fails with `data did not
  match any variant of untagged enum TableMetadataEnum`) — VERIFIED by probe, real bug. But the builder's
  *rationale* for deferring ("adding `#[serde(default)]` would DIVERGE from the spec, which marks
  `sequence-number` required at `format/spec.md` line 949") is WRONG. The same spec, lines 1979 & 2002,
  says: "Snapshot field `sequence-number` **must default to 0** when reading v1 metadata [as v2]." The
  "required" on line 949 is a WRITER rule (write it when non-zero); the READER rule explicitly mandates
  defaulting absent→0 — exactly what Java's `SnapshotParser` does (write omits ≤0 at line 60; read defaults
  to `INITIAL_SEQUENCE_NUMBER` at line 128). So Rust's strict reader is a spec-VIOLATION on the read side,
  not stricter-than-spec. The Rust V1 path already hard-codes `sequence_number: 0` (`snapshot.rs` line 405),
  so the fix is the directly-analogous one-liner `#[serde(default)]` on `SnapshotV2.sequence_number` (and
  `SnapshotV3`). RECOMMENDATION: FIX (small spec-mandated reader-leniency, ships with a seq-omitted-V2
  parse test), not track — the blast radius is real (a V1→V2-upgraded table keeps pre-upgrade snapshots at
  seq 0, since `upgradeFormatVersion` only bumps the version and does not rewrite snapshot seqs, so its
  emitted V2 metadata is unreadable by Rust today). Deferred here only because it is a production-reader
  edit outside this REF-OP increment's flagged scope — a human should action it as a tiny standalone change.
- **DO mutation-verify that an interop pin catches the regression its doc comment CLAIMS to catch — a
  near-boundary timestamp does not pin a strict-`<`.** *Why:* the `rollback_to_time` interop scenario uses
  `ROOT_TS_MS + 1`, whose comment claimed it "cross-checks the strict-`<` semantics." It does NOT: under a
  `<`→`<=` mutation of `find_latest_ancestor_older_than` the interop pin still PASSED (CURRENT's timestamp
  is ~40e9 ms above `ROOT_TS_MS+1`, so ROOT is selected either way). Only the pre-existing unit test
  `test_rollback_to_time_strict_less_than_skips_equal_timestamp` (which sits EXACTLY on CURRENT's timestamp)
  caught it. Strengthened the interop pin to also roll to a timestamp EQUAL to `CURRENT_TS_MS` and assert it
  falls back to ROOT (not CURRENT) — mutation-verified it now FAILs under `<=`. The boundary, not a point
  near it, is what pins a strict inequality.

### 2026-06-07 (Increment 10 — V2/V3 snapshot `sequence-number` lenient read, BUILDER Opus)
- **DO read the spec's WRITER rule AND its READER rule separately — a field marked "required" on write may
  be explicitly mandated to DEFAULT on read.** *Why:* the Iceberg spec field table (`format/spec.md` line
  949) marks snapshot `sequence-number` _required_ in v2/v3, which an earlier increment read as "the reader
  must reject an absent value." But the same spec's "Reading v1 metadata for v2" section (lines 1979 & 2002)
  says `sequence-number` **must default to 0** when absent on read — that is the authoritative read rule, and
  Java's `SnapshotParser` implements exactly it (write omits the field when `<= INITIAL_SEQUENCE_NUMBER` (0),
  L60-61; read defaults an absent field to `INITIAL_SEQUENCE_NUMBER` (0), L128-130). The "required" is a
  WRITER obligation (emit it when non-zero), NOT a reader strictness. A strict Rust reader (`_serde::Snapshot
  V2/V3.sequence_number` with no `#[serde(default)]`) therefore VIOLATED the spec's read side, not exceeded
  it — it failed to parse a Java-written V1→V2-upgrade table (`upgradeFormatVersion` bumps the version without
  rewriting pre-upgrade snapshot seqs, so those snapshots stay seq-0 → Java omits the field → Rust errored
  "missing field `sequence-number`" / "data did not match any variant of untagged enum TableMetadataEnum").
  Fix = `#[serde(default)]` on both `SnapshotV2.sequence_number` and `SnapshotV3.sequence_number` (i64 default
  is 0 = `INITIAL_SEQUENCE_NUMBER`), mirroring the V1 `try_from` path that already hard-codes `sequence_number:
  0`. _Supersedes the Increment-9 builder's "spec marks it required → defaulting would diverge" rationale
  (lessons entry above + the GAP_MATRIX note), which conflated the writer rule with the reader rule._
- **DO confirm WHICH serde struct backs the on-disk read before annotating it.** *Why:* a snapshot inside
  `TableMetadata` deserializes through the version-specific `_serde::SnapshotV{1,2,3}` structs, not the public
  `Snapshot` — `TableMetadataV2` (`table_metadata.rs:801`) holds `Option<Vec<SnapshotV2>>` and
  `TableMetadataV3` (`:758`) holds `Option<Vec<SnapshotV3>>`. The `#[serde(default)]` must go on the
  `sequence_number` field of those `_serde` structs (the ones with the actual `i64`), not on the public
  `Snapshot` (whose field is populated by the `From`/`TryFrom` conversion, never by serde directly). Read the
  struct + grep its use in `table_metadata.rs` before guessing.
- **DO keep WRITE behavior unchanged when relaxing a READ.** *Why:* `#[serde(default)]` only affects
  deserialization; Rust keeps emitting `sequence-number` on write (Java tolerates a written seq-0 field — its
  parser reads any present value, and only the WRITER omits ≤ 0). The lenient read is purely additive: every
  existing seq-present snapshot parses identically; only the previously-erroring seq-absent case now succeeds
  as 0. Mutation-verify the new tests by STRIPPING the attribute (seq-omitted tests must fail "missing field")
  while the seq-present negative-control tests still pass — that proves the attribute is load-bearing and not
  clobbering supplied values.
- **DO inline the JSON fixture in the test rather than adding a testdata file when the input is a single
  small record.** *Why:* a kebab-case snapshot JSON literal (`snapshot-id`/`manifest-list`/`summary`/...) in
  the test body is self-documenting, keeps the risk + the data co-located, and avoids a fixture file that the
  reader has to go find. Reserve `crates/iceberg/testdata/` for multi-record / cross-test fixtures.

### 2026-06-07 (Increment 10 — V2/V3 snapshot `sequence-number` lenient read, REVIEWER Opus)
- **DO probe a "this unblocks table-class X" claim END-TO-END through the actual top-level reader, not just
  the leaf struct, AND mutation-confirm the leaf fix is what unblocks it.** *Why:* the snapshot
  `#[serde(default)]` fix is on `_serde::SnapshotV{2,3}.sequence_number`, but the claim is that a whole
  V1→V2-upgrade `TableMetadata` JSON now parses. Verified by feeding a realistic Java-shaped V2 metadata
  (format-version 2, `last-sequence-number: 0` PRESENT, carryover snapshot with `sequence-number` OMITTED)
  to `serde_json::from_str::<TableMetadata>` (the production path via `#[serde(try_from =
  "TableMetadataEnum")]`, untagged enum): it parses (`last_sequence_number=0`, snapshot `sequence_number=0`).
  Reverting the leaf `#[serde(default)]` makes that SAME top-level parse FAIL ("data did not match any
  variant of untagged enum TableMetadataEnum") — proving the leaf fix is the load-bearing cause, not an
  incidental pass. A leaf-only test (`SnapshotV2` in isolation) does NOT prove the end-to-end class is read.
- **DO distinguish a real _Java-omitted_ field from a _spec-robustness-only_ one when tracking a
  reader-leniency follow-up — verify the Java WRITER, not just the spec's read table.** *Why:* the spec's
  "Reading v1 metadata for v2" list (`format/spec.md` 1979–1986) mandates default-to-0 for several fields,
  which reads like a uniform set of interop gaps. But `TableMetadataParser.toJson` (line 173) ALWAYS writes
  `last-sequence-number` for `formatVersion > 1`, so Java NEVER emits a V2/V3 file omitting it — Rust's
  required `last_sequence_number` is therefore a spec-robustness gap for hand-written / non-Java metadata,
  NOT a Java-interop blocker (probe-confirmed: a `last-sequence-number`-omitted V2 JSON fails Rust parsing,
  but Java never produces that JSON). Only the snapshot `sequence-number` (Java omits when ≤ 0) was a true
  Java-omitted field. The manifest-list/manifest/data-file `content`/`sequence-number` siblings are Avro
  fields on a different read path whose omit-vs-always-write status needs per-field Avro-schema checking —
  do not assume the snapshot omit-when-≤0 pattern carries over. Sharpened the Increment-10 follow-up to say
  this so the next agent neither over-prioritizes `last-sequence-number` as interop-critical nor assumes the
  Avro fields are trivially the same shape.

### 2026-06-07 (Increment 11 — Avro manifest default-to-0 reads, verify-then-fix, BUILDER Opus)
- **DO distinguish the TWO manifest read mechanisms before deciding what a default-to-0 read depends on:**
  (a) the manifest-LIST V2/V3 reader uses `apache_avro::Reader::new(bs)` — the EMBEDDED writer schema, NO
  reader-schema resolution — so a field a V1 writer omitted reaches serde MISSING and a `#[serde(default)]`
  on the `_serde` struct fires; (b) the manifest-ENTRY reader uses `AvroReader::with_schema(reader, bs)` —
  the writer schema IS resolved against the reader schema, so a field with an Avro reader-schema `default`
  (produced from `NestedField::with_initial_default`, e.g. data-file `content` field-id 134) is filled by
  apache-avro BEFORE serde ever sees the record, and the `#[serde(default)]` on that same `_serde` field is
  never exercised on that path. Knowing which mechanism a given reader uses tells you which test actually
  pins the leniency. *Why it bites:* the EXISTING `test_data_file_serialize_deserialize_v1_data_on_v2_reader`
  appears to pin the data-file `content` serde default but does NOT — it reads via the V2 reader schema
  (Avro default masks the serde default). Stripping `#[serde(default)]` from `DataFileSerde.content` leaves
  that test GREEN; only a GENUINE V1-manifest read (`build_v1` → `parse_avro`, which uses `manifest_schema_v1`
  that has no `content` column at all) makes serde see content absent and turns the serde default load-bearing.
- **DO build the omitted-field input with the REAL V1 writer, not a hand-edited struct, when verifying an
  absent-field-at-read default.** *Why:* the existing `test_manifest_file_v1_to_v2_projection` /
  `test_data_file_serde_v1_field_defaults` only exercise the `ManifestFileV1::try_into` / `DataFileSerde::
  try_into` CONVERSION on an in-memory struct — they never go through the Avro reader, so they cannot catch
  a reader-side regression (a wrong reader schema, a dropped `#[serde(default)]` reachable on the V1 path,
  an apache-avro resolution change). Writing a V1-shaped Avro file via `ManifestListWriter::v1` /
  `ManifestWriterBuilder::build_v1` and reading it back through the production reader
  (`parse_with_version`/`parse_avro`) is the only test that pins the END-TO-END "read what Java's V1 writer
  emitted" behavior the spec mandates (`format/spec.md` 1980-1985).
- **DO honor verify-then-fix: an increment can legitimately land ZERO production changes.** *Why:* the brief
  said "do NOT assume every field is broken." All six Avro default-to-0 fields were already correct; the real
  deliverable was per-field EMPIRICAL proof + mutation-verified regression tests, plus an honest "verified
  already-fine, no production change" doc trail. Resist the pull to "fix" a working reader to feel productive
  — adding a redundant `.unwrap_or(0)` over an already-defaulted field is dead code that muddies the next
  audit. Pin the behavior, document the proof, close the follow-up.
- **DO use a throwaway `println!`-probe test to settle an empirical reader question, then convert it to a
  named regression test (never leave the probe).** *Why:* the load-bearing unknown was "does apache-avro
  error on a writer-schema-omits-field, or default it?" A 30-line probe answered it definitively
  (`Ok(... content: Data, sequence_number: 0 ...)`); I then rewrote it as
  `test_v1_shaped_manifest_list_read_as_v2_defaults_absent_fields_to_zero` with real asserts + a risk-named
  doc comment, and mutation-verified it. A probe that ships is an untested assertion; a probe deleted without
  a replacement loses the evidence.

### 2026-06-07 (Phase 2 Increment 1 — DeleteFiles + manifest-filter machinery, BUILDER Opus)
- **DO put the "missing required delete path" validation (Java `failMissingDeletePaths`) in the OPERATION's
  path-resolution, NOT in the producer's manifest-rewrite step.** *Why:* the producer's `process_deletes`
  receives only the RESOLVED `Vec<DataFile>` (the files the operation actually found in live entries) — it
  has no knowledge of the originally-REQUESTED path set, so it cannot tell that a requested path matched
  nothing. The first cut validated in the producer and two tests failed: deleting a single absent path
  resolved to an EMPTY set → `process_deletes` returned early → the precondition fired with
  `PreconditionFailed` instead of the intended `DataInvalid`; and a mixed present+absent delete SUCCEEDED
  (the present file was removed, the absent one silently ignored). Fix: the `DeleteFilesOperation::delete_files`
  resolver builds `found_paths` while scanning the current snapshot's live entries and errors
  `DataInvalid "Missing required files to delete: {missing}"` if any requested path is absent — because the
  requested set only lives in the operation. The producer keeps a redundant resolved-set guard as
  defense-in-depth.
- **DO rewrite a filtered manifest with the SOURCE manifest's own partition spec, not the table's default
  spec.** *Why:* Java `ManifestFilterManager.filterManifestWithDeletedFiles` writes with `reader.spec()`
  (the manifest's own spec). The existing `SnapshotProducer::new_manifest_writer` helper uses
  `default_partition_spec()` — fine for newly-added data, WRONG for a rewrite, since a manifest written under
  an older spec must keep its spec-id/partition-type when its `Existing` entries are copied forward. Added a
  separate `new_filtering_manifest_writer(source_manifest)` that resolves
  `metadata.partition_spec_by_id(source_manifest.partition_spec_id)`. (For Increment 1 every manifest shares
  the default spec, so the two coincide — but the seam is correct for the partition-evolution case the later
  write increments hit.)
- **DO carry an unchanged manifest forward as-is (same `manifest_path`) when it contains none of the target
  files, and KEEP an all-deleted rewritten manifest, DROP only a no-live-file unrewritten one.** *Why:* the
  three Java rules, separately cited: (a) `filterManifest` returns the manifest object unmodified when it
  cannot contain a deleted file (efficiency + fewer files written); (b) `MergingSnapshotProducer.apply`
  (lines 1002-1009) keeps a manifest iff `hasAddedFiles() || hasExistingFiles() || snapshotId()==snapshotId()`
  — a rewritten manifest where every live entry became `Deleted` has no added/existing files but its
  `added_snapshot_id` IS the new snapshot id (the filter manager writes with the producer's snapshot id), so
  it is KEPT (the Deleted entries are informational); (c) an UNREWRITTEN carried-forward manifest with no live
  files is dropped. Pin the carry-forward with a same-`manifest_path` assertion (mutation-verified: forcing
  every manifest to rewrite changes the path and fails the test) — a live-set-only check can't catch a
  needless rewrite (which is correct data but wasted IO + a corruption risk surface).
- **DO assert the post-commit SCAN live set (Added+Existing entry paths across the current snapshot's
  manifests), not just the emitted `TableUpdate`s, for a write action.** *Why:* the real correctness signal
  for `DeleteFiles` is "what would a scan read" — {A,C} after deleting B. A test that only inspects the
  `AddSnapshot` update or the manifest counts can pass while the live set is wrong (e.g. a deleted entry left
  as `Existing`). Mutation-verified the live-set tests are load-bearing: swapping `add_delete_entry` →
  `add_existing_entry` (so a "deleted" file stays live) fails 6 of the 8 tests. Build the helper that walks
  `current_snapshot().load_manifest_list().entries()[*].load_manifest().entries()` filtering `is_alive()`.
- **DO extend `SnapshotProduceOperation` with `delete_files(&producer) -> Vec<DataFile>` as the generic seam
  for ALL delete-bearing write ops, and add the empty impl to the existing `FastAppendOperation`.** *Why:*
  the manifest-rewrite machinery in the producer is operation-agnostic — the operation only supplies WHICH
  files to remove (resolved against the current snapshot at commit) and WHICH manifests to consider
  (`existing_manifest`). `OverwriteFiles`/`ReplacePartitions`/`RewriteFiles` will each implement `delete_files`
  differently (by-filter / by-partition / explicit set) and reuse the same `process_deletes`. Adding a method
  to the trait forces an impl on every existing implementor — `FastAppendOperation` needs a 5-line
  `Ok(vec![])`; that touch of `append.rs` is the trait's own sibling, not unrelated scope creep, but flag it.

### 2026-06-07 (Phase 2 Increment 1 — DeleteFiles, REVIEWER Opus)
- **DO add an explicit cross-snapshot PROVENANCE test for any manifest-rewrite action — a live-PATH-set
  test does NOT pin it.** *Why:* the #1 corruption risk in a delete/rewrite is re-stamping a SURVIVING
  entry with the new commit's snapshot id / sequence number instead of preserving its original (Java
  `writer.existing(entry)` keeps `entry.snapshotId()` + both seqs; `writer.delete(entry)` takes the NEW
  snapshot id but keeps the removed file's original seqs). The Increment-1 builder's 8 tests asserted only
  the live PATH set + statuses + manifest counts — ALL 8 PASSED under a mutation that re-stamps
  `add_existing_entry`'s `snapshot_id = self.snapshot_id` (silent data-sequence corruption that breaks
  merge-on-read delete application + incremental scans). Added
  `test_delete_preserves_surviving_entry_provenance_across_snapshots`: append A (snapshot S1), append B+C
  in one commit (snapshot S2, one manifest), delete B → assert the surviving C keeps S2 + its original
  data/file seq (NOT S3/new seq), carried-forward A keeps S1, and B's Deleted tombstone gets S3 but keeps
  B's original seqs. Mutation-verified it is the ONLY test that fails under the re-stamp. The Rust path is
  correct (provenance preserved end-to-end) — the gap was purely missing test coverage of the most
  dangerous bug class.
- **DO verify `inherit_data` runs before a rewrite so Existing/Deleted entries have populated seq numbers.**
  *Why:* `add_entry_inner` REJECTS an Existing/Deleted entry whose `sequence_number`/`file_sequence_number`
  is `None`. Entries read via `ManifestFile::load_manifest` are passed through `inherit_data` (inherits the
  manifest-list entry's `added_snapshot_id` + seq for null fields), so a rewrite of a manifest whose
  Added entries had null-on-disk seqs/snapshot-id still writes valid Existing/Deleted entries with the
  inherited (original) provenance. The rewrite path depends on this — a rewrite of un-inherited entries
  would error.
- **DO pin the all-deleted-manifest KEEP-then-DROP lifecycle across TWO commits, and the truly-empty-commit
  rejection.** *Why:* Java `MergingSnapshotProducer.apply` keeps a rewritten all-deleted manifest only in
  the commit that wrote it (`snapshotId()==snapshotId()`) and the NEXT commit drops it
  (`hasAddedFiles||hasExistingFiles` false). The builder tested the single-commit all-deleted case but not
  the two-commit lifecycle; added `test_all_deleted_manifest_kept_by_creating_commit_then_dropped_by_next`.
  Separately, the precondition relaxation that lets a delete-only commit through must still reject a no-op
  empty commit — the builder had `test_delete_only_commit_is_allowed` but no rejection test; added
  `test_empty_delete_commit_is_rejected`.

### 2026-06-07 (Phase 2 Increment 2 — OverwriteFiles, BUILDER Opus)
- **DO compose a new add+delete write action by reusing the producer's TWO existing paths, not
  re-implementing either.** *Why:* `SnapshotProducer::manifest_file` already (a) resolves an operation's
  `delete_files` and runs `process_deletes` (the DeleteFiles filter/rewrite path) AND (b) writes an added
  manifest from `added_data_files` (the fast-append path), in ONE snapshot. So `OverwriteFiles` is just a
  `SnapshotProduceOperation` returning `Operation::Overwrite` whose `delete_files`/`existing_manifest` mirror
  DeleteFiles, with the added files passed to `SnapshotProducer::new` exactly as fast-append does. No new
  manifest-writing or rewrite logic is needed — the seam was built for this. Validate added files with the
  existing `validate_added_data_files` (data content type + partition-spec match + partition-value), never a
  duplicate.
- **DO extract the delete-path resolution + data-manifest listing into shared `SnapshotProducer` methods on
  the SECOND identical use, not copy-paste.** *Why:* `DeleteFilesOperation::delete_files`/`existing_manifest`
  and `OverwriteFilesOperation`'s versions were byte-identical non-trivial logic (load manifest list → filter
  data manifests → resolve live entries by path → `failMissingDeletePaths`). Two identical uses of non-trivial
  logic is the Rule-of-Three "extract now" case. Factored `SnapshotProducer::{resolve_delete_paths(&HashSet
  <String>) -> Vec<DataFile>, current_data_manifests() -> Vec<ManifestFile>}` in snapshot.rs (the existing
  home of `process_deletes`); both operations' methods became one-liners. Keep `found_paths` as
  `HashSet<String>` (owned), NOT `HashSet<&str>` — the borrowed `&str` points into a per-iteration `manifest`
  that drops before the later missing-path filter reads it (E0597).
- **DO resolve an operation's deleted files BEFORE `summary()` so the snapshot summary can reflect them, and
  reuse the resolution in `manifest_file()` (don't resolve twice).** *Why:* Java `MergingSnapshotProducer.apply`
  merges the added-files summary AND the filter-manager's deleted-files summary, so the overwrite/delete
  summary carries `deleted-data-files`/`deleted-records`. The Rust `summary()` only wired
  `SnapshotSummaryCollector::add_file`; the collector already has `remove_file` (tracks `removed_data_files` →
  `deleted-data-files`, `deleted_records` → `deleted-records`). But `summary()` runs BEFORE `manifest_file()`
  in `commit()`, where deletes used to be resolved — so resolve once at the top of `commit()` (via the
  operation's `delete_files` seam), store on a `removed_data_files` producer field, call `remove_file` in
  `summary()`, and `std::mem::take` the field in `manifest_file()`. No double resolution.
- **DO fix the producer's previous-snapshot resolution to the CURRENT branch head, not a lookup of the
  not-yet-committed new snapshot id — wiring `remove_file` into the summary EXPOSES this latent bug as an
  arithmetic underflow.** *Why:* `summary()` computed `previous_snapshot =
  snapshot_by_id(self.snapshot_id).parent()`, but `self.snapshot_id` is the NEW snapshot, which is not in
  `table.metadata()` yet → always `None` → `update_totals` seeded all totals from 0. Harmless while every
  producer op only ADDED files (totals only grew), but the moment an op REMOVES files, `update_totals` does
  `0 - removed` → `attempt to subtract with overflow` panic. The fix mirrors Java
  `SnapshotProducer.summary(previous)` (`previous.snapshot(previousBranchHead.snapshotId())`): use
  `table.metadata().current_snapshot()` (the parent / branch head, which IS in metadata at summary time).
  Now totals are correct (cumulative) for append, delete, AND overwrite.
- **DO pass `truncate_full_table = false` from the producer's `update_snapshot_summaries` call — Java's
  `SnapshotProducer.summary` has NO full-table-truncate branch.** *Why:* the Rust producer keyed the
  truncate flag on `operation == Overwrite`, and `truncate_table_summary` zeroes all totals and reports the
  ENTIRE previous `total-data-files` as `deleted-data-files`. That is full-table-replace/INSERT-OVERWRITE
  semantics, WRONG for a PARTIAL `OverwriteFiles` (delete some, add some) — it would both underflow and
  over-report deletes. Verified against Java: `SnapshotProducer.summary(previous)` calls `updateTotal`
  unconditionally (prev + added − removed) for every operation, no truncate. Since nothing else emits an
  Overwrite snapshot, flipping the arg to `false` has zero blast radius and matches Java. (A future full
  replace/truncate action can opt into the truncate path explicitly.)
- **DO honor an explicit increment brief over Java when the brief deliberately diverges — and flag it.**
  *Why:* Java `BaseOverwriteFiles.operation()` is DYNAMIC (delete-only→`DELETE`, add-only→`APPEND`,
  both→`OVERWRITE`), but the increment brief's KEY tests assert `Operation::Overwrite` for ALL three cases
  (delete+add, add-only, delete-only). The brief is the authoritative spec here, so the action always records
  `Operation::Overwrite`; the divergence is documented in the module doc, the GAP_MATRIX row, and the report
  as a tracked gap (the summary carries the precise added/deleted counts regardless, so no information is
  lost). Don't silently "correct" to Java when the brief is explicit — surface the choice.
  _superseded 2026-06-07 (REVIEWER): the reviewer brief explicitly instructed to establish ground truth from
  the Java source and ALIGN — Java IS dynamic (`BaseOverwriteFiles.operation()` L50-60, on
  `addsDataFiles()`/`deletesDataFiles()`), so the always-Overwrite was a PARITY BUG, now fixed. See the
  REVIEW lesson block below._

### 2026-06-07 (Phase 2 Increment 2 — OverwriteFiles, REVIEWER Opus)
- **DO make `OverwriteFilesOperation::operation()` DYNAMIC, mirroring Java `BaseOverwriteFiles.operation()`
  (the always-`Overwrite` was a parity bug).** *Why:* Java (`core/BaseOverwriteFiles.java` L50-60) returns
  `DELETE` when `deletesDataFiles() && !addsDataFiles()`, `APPEND` when `addsDataFiles() && !deletesDataFiles()`,
  else `OVERWRITE`. `addsDataFiles()` = `!newDataFilesBySpec.isEmpty()` (`MergingSnapshotProducer` L230) and
  `deletesDataFiles()` = `filterManager.containsDeletes()` = `!deletePaths.isEmpty()` (`ManifestFilterManager`
  L198-203) — both keyed on the REQUESTED sets, evaluated at commit time BEFORE the deletes resolve against the
  table. So in Rust the classification is `match (!added_data_files.is_empty(), !delete_paths.is_empty())`. The
  recorded operation is read in `SnapshotProducer::summary` (`operation: op.operation()`), so a dynamic
  `operation()` is all that is needed — `update_snapshot_summaries` already accepts Append/Overwrite/Delete.
  Pin with three tests (add-only → Append, delete-only → Delete, both → Overwrite); mutation-verify by forcing
  `operation()` back to always-`Overwrite` (the add-only + delete-only tests fail; the both-tests stay green
  because they produce Overwrite either way — so they CANNOT pin the dynamic rule alone).
- **DO add a cumulative-running-totals test (append twice, then delete) for ANY producer-summary change — it
  is the only test that catches the `previous_snapshot` seed bug.** *Why:* the per-commit added/deleted-count
  tests pass under BOTH the correct (seed = current branch head) and the broken (seed = 0) logic, because they
  only assert THIS commit's `added-*`/`deleted-*`. Only a multi-snapshot total assertion (snapshot 1 → 2
  files, snapshot 2 → 4 files cumulative, snapshot 3 delete → 3) distinguishes them: under the seed-0 bug,
  snapshot 2's `total-data-files` is 2 (this commit only), not 4, AND snapshot 3 underflows `0 - 1` (u64
  panic). Mutation-verified: reverting `previous_snapshot` to `snapshot_by_id(self.snapshot_id).parent()`
  fails this test at the snapshot-2 accumulation assertion. This bug affects EVERY snapshot action
  (append/delete/overwrite), so the test belongs with the producer-summary contract, not just overwrite.
- **CONFIRMED Java-faithful (no change): `previous_snapshot = current_snapshot()` and
  `truncate_full_table = false`.** *Why:* Java `SnapshotProducer.summary(TableMetadata previous)` (L392-419)
  seeds `previousSummary` from `previous.snapshot(previousBranchHead.snapshotId()).summary()` (the parent /
  branch head = Rust `current_snapshot()`), defaulting totals to 0 ONLY when there is no previous branch ref —
  it never zeroes totals for an overwrite. There is NO full-table-truncate branch anywhere in `summary(previous)`;
  even `BaseReplacePartitions` only `set(REPLACE_PARTITIONS_PROP, "true")` and accumulates via the same
  `updateTotal` path. So the Rust `truncate_full_table` path (zero totals + report all prior files deleted) has
  NO Java analogue for any standard op — `false` for OverwriteFiles is unambiguously correct. (Residual note:
  Java `updateTotal` uses signed `long` and stops accumulating + omits the total once it goes negative;
  Rust `update_totals` uses `u64` and panics on underflow. With the corrected seed a well-formed sequence
  never underflows, but the `u64` is strictly less robust than Java against a malformed previous summary —
  tracked, not in this increment's scope.)
- **DO confirm a shared-helper extraction is byte-identical to the inline original before trusting "behavior
  preserving".** *Why:* `DeleteFilesOperation::{delete_files, existing_manifest}` shrank to one-liners calling
  `SnapshotProducer::{resolve_delete_paths, current_data_manifests}`. Diffed the extracted bodies against the
  Increment-1 inline code: same `metadata_ref()`, same `ManifestContentType::Data` filter, same
  `entry.is_alive() && delete_paths.contains(...)` match, same "Missing required files to delete" +
  `ErrorKind::DataInvalid`, same empty-set early return. All 11 DeleteFiles tests stay green — the extraction
  changed call sites, not semantics.

### 2026-06-07 (Phase 2 Increment 3 — ReplacePartitions, BUILDER Opus)
- **DO resolve a by-PARTITION dynamic overwrite into a `Vec<DataFile>` and feed it to the EXISTING by-path
  `process_deletes` rewrite, rather than adding a second rewrite path.** *Why:* Java's `ManifestFilterManager`
  marks a live entry for delete when `dropPartitions.contains(file.specId(), file.partition())` — a
  by-partition predicate. But the Rust producer's rewrite (`process_deletes`/`rewrite_manifest_with_deletes`)
  already matches removed files by PATH and already gets the rewrite/keep/drop + provenance-preservation right
  (Increment 1). So the minimal, no-duplication design is a new RESOLVER
  `SnapshotProducer::resolve_partition_deletes(&HashSet<(i32, Struct)>)` (sibling of `resolve_delete_paths`)
  that scans the current data manifests, collects every live `DataFile` whose `(partition_spec_id, partition)`
  is in the drop set, and returns it — the producer's `delete_files` seam then drives the SAME path unchanged.
  `Struct` derives `PartialEq + Eq + Hash`, so `(i32, Struct)` is a valid `HashSet` key. Zero edits to the
  rewrite machinery.
- **DO use `truncate_full_table = FALSE` for ReplacePartitions' unpartitioned full replace — the brief's
  truncate hint is WRONG against the Java source.** *Why:* Java `SnapshotProducer.summary()`
  (`SnapshotProducer.java:926` `updateTotal`) has NO full-table-truncate branch — it computes
  `total = previous + added - removed` UNCONDITIONALLY; `BaseReplacePartitions` only sets
  `replace-partitions=true` (a summary property) and reaches a full unpartitioned replace via
  `deleteByRowFilter(alwaysTrue)`, which makes the filter manager report EVERY file as deleted. The Rust
  by-partition resolver ALREADY produces the full removed-file set (an unpartitioned table's single empty
  partition matches all files), so `update_totals` computes the correct post-replace totals (N prev − N
  removed + M added = M, no underflow) and `remove_file` reports the right `deleted-data-files`/`-records`.
  Setting `truncate_full_table=true` (the Rust-only `truncate_table_summary` path) would ALSO set
  `deleted-data-files = previous total`, DOUBLE-COUNTING vs. the resolved-removed set, and has no Java
  analogue for any standard op (confirmed by the Increment-2 reviewer). Verify against the Java source, not
  the brief's intuition — pin the totals + deleted count in the unpartitioned test.
- **DO model the unpartitioned full replace as "drop the single empty partition," not as a special case.**
  *Why:* on an unpartitioned table every file's partition is `Struct::empty()`, so the drop set collected from
  any added file is `{(spec_id, empty)}` and the resolver matches ALL existing files — a full replace falls
  out of the SAME by-partition code with no `is_unpartitioned()` branch (Java reaches the same end via a
  different mechanism — `deleteByRowFilter(alwaysTrue)` — but the observable result is identical). One code
  path covers both partitioned and unpartitioned.
- **DO NOT add a `failMissingDeletePaths`-style validation to a partition-drop resolver.** *Why:* Java's
  `validateRequiredDeletes` (`ManifestFilterManager.java:280`) fires ONLY for path/file deletes
  (`deletePaths`/`deleteFiles`), never for `dropPartitions`. So replacing a partition that currently has no
  files is a legal PURE ADD (no spurious delete, no error) — `resolve_partition_deletes` must return an empty
  removed-set for an empty partition, unlike `resolve_delete_paths` which errors on a missing path. Pin it
  with a "replace an empty partition = pure add" test.
- **DO build an unpartitioned table fixture in-test via `TableCreation` with NO `partition_spec` when no
  unpartitioned JSON fixture exists and `testdata/` is out of the allowed-edit set.** *Why:*
  `TableCreation.partition_spec` is `Option<UnboundPartitionSpec>` defaulting to `None` = unpartitioned, so a
  catalog `create_table` with the minimal schema but no spec yields a real unpartitioned V3 table — no new
  committed fixture needed (mirrors `make_v3_minimal_table_in_catalog` minus the `.partition_spec(...)` call).
  Build the data files with `partition_spec_id(0)` + `Struct::empty()`.

### 2026-06-07 (Phase 2 Increment 4 — RewriteFiles, BUILDER Opus)
- **DO read the Java `validateReplacedAndAddedFiles` preconditions verbatim — RewriteFiles is STRICTER than
  OverwriteFiles: the DELETE set must be non-empty.** *Why:* Java `BaseRewriteFiles.validateReplacedAndAddedFiles`
  has THREE `Preconditions.checkArgument`s: (1) `deletesDataFiles() || deletesDeleteFiles()` ("Files to delete
  cannot be empty"), (2) `deletesDataFiles() || !addsDataFiles()` ("Data files to add must be empty because
  there's no data file to be rewritten"), (3) the delete-file analogue of (2). So a delete-only rewrite
  (delete N, add 0) is LEGAL but an add-only rewrite (delete 0, add M) is REJECTED — the OPPOSITE of
  OverwriteFiles, whose add-only path records an Append. The producer's own empty-commit precondition would
  WRONGLY allow an add-only rewrite (it only rejects all-empty), so the non-empty-delete check must be added in
  the action's `commit()` BEFORE the producer runs, with the exact Java message. For a DATA-only rewrite,
  precondition (2) is SUBSUMED by (1) (with DELETE-file rewrite out of scope, `deletesDataFiles()` is true iff
  the delete set is non-empty, i.e. exactly when (1) passes), so document it in prose rather than coding an
  unreachable `if added.is_empty() && deleted.is_empty()` branch that clippy/readers would (rightly) question.
- **DO add `Operation::Replace` to the `update_snapshot_summaries` op allowlist when landing the first Replace
  action — the producer's summary path hard-rejects any op outside {Append, Overwrite, Delete}.** *Why:*
  `spec/snapshot_summary.rs::update_snapshot_summaries` starts with a guard that returns
  `DataInvalid "Operation is not supported."` for any operation not in that set; `SnapshotProducer::summary`
  calls it unconditionally, so a `RewriteFilesOperation` recording `Operation::Replace` fails to commit AT ALL
  until `Replace` is admitted. Java's `SnapshotProducer.summary` is operation-agnostic (totals = prev + added -
  removed for every op), so admitting `Replace` is the Java-faithful one-liner — directly analogous to the
  existing Overwrite/Delete entries, no truncate branch. This edit is in `spec/`, technically outside a
  transaction-action increment's named file set, but it is the minimal + required enabler (flag it). Mutation-
  verify it is load-bearing: removing the `Replace` entry fails the KEY rewrite test with the exact "Operation
  is not supported." error.
- **DO reuse the by-PATH `resolve_delete_paths` for RewriteFiles even though callers pass `DataFile`s.** *Why:*
  Java's `deleteFile(DataFile)` adds the file to `replacedDataFiles` AND calls `delete(dataFile)`, but the
  filter manager ultimately matches removed files by PATH (`ManifestFilterManager`). Rust callers hold the
  to-delete `DataFile`s (after a scan), so the action stores `Vec<DataFile>` and extracts `file_path` into a
  `HashSet<String>` at commit, then drives the SAME `resolve_delete_paths` + `process_deletes` machinery that
  DeleteFiles/OverwriteFiles use — which already gets `failMissingDeletePaths`, rewrite/keep/drop, and surviving-
  entry provenance preservation right (Increments 1-2). Zero new resolution or rewrite logic; RewriteFiles is
  OverwriteFiles with a fixed `Operation::Replace` and the non-empty-delete precondition. The `delete_files`
  seam (`SnapshotProduceOperation::delete_files`) was built exactly for this — no machinery change needed.
- **DO NOT take a mutation-test backup AFTER the mutation — `cp file backup` following an in-place `sed`
  captures the MUTATION, not the original, and "restoring" re-corrupts the file.** *Why:* a botched mutation
  loop (`sed -i 's/Replace/Overwrite/'` that matched BOTH the production `operation()` AND a test assertion's
  expected value, then `cp file /tmp/backup`) baked a wrong assertion into the "backup," which then restored
  the corruption — the FILTERED test run passed (the mutation matched the test's own expectation too) but the
  FULL lib suite caught it (`test_rewrite_delete_only_is_allowed` asserted `Operation::Overwrite` while its
  message said "records Replace"). Always snapshot the file (`cp file /tmp/backup`) BEFORE any in-place edit;
  mutate; run; restore FROM the pre-mutation copy; and re-run the FULL suite (not just the filtered module)
  before declaring green — a filtered run can hide a mutation that corrupted a test's own expectation.

### 2026-06-07 (Phase 2 Increment 4 — RewriteFiles, REVIEWER Opus)
- **DECISION (dataSequenceNumber deferral): ADD A GUARD, don't just document.** A data-file rewrite stamps
  the added files with a FRESH (higher) data sequence number. Merge-on-read deletes apply only to data with
  `data_seq <= delete_seq`, so compacting a deleted-from data file into a higher-seq file makes the old
  delete stop applying → deleted rows RESURRECT (silent data corruption). Java carries the replaced files'
  max data-seq onto the added files (`setNewDataFilesDataSequenceNumber`); this action defers that.
  Adjudication: (a) **today** this library cannot itself WRITE delete files — no `RowDelta`, no
  position/equality-delete commit path, and every add path runs `validate_added_data_files` which rejects
  non-`Data` content ("Only data content type is allowed for fast append") — so a table THIS library wrote
  has no outstanding deletes. BUT it can READ + operate on a Java-written table that DOES (a Java `RowDelta`
  snapshot has `Deletes`-content manifests), and `rewrite_files` on such a table would corrupt it. (b) The
  original deferral note framed the fresh seq as merely "correct for a pure data rewrite with no outstanding
  deletes" — true but not a *guard*; a note is not a regression barrier. (c) FIXED: added a HARD precondition
  in `commit()` — `has_outstanding_delete_files(table)` loads the current snapshot's manifest list and rejects
  (`ErrorKind::FeatureUnsupported`) if ANY entry is `ManifestContentType::Deletes`. This makes the unsafe case
  impossible (fail-loud) instead of documented. Test
  `test_rewrite_rejected_when_table_has_outstanding_delete_files` builds a table with a real position-delete
  manifest (via the production manifest/list writers + catalog `update_table`, since no public action writes
  deletes) and asserts rejection + table-unchanged. **Mutation-verified the guard AND the corruption: with the
  guard disabled the rewrite COMMITS (`expect_err` panics on an `Ok`) — proving the resurrection path is real,
  not theoretical.** Guard is small + in scope (the action's own file) and lifts cleanly when
  `dataSequenceNumber` preservation lands (docs say so in three places: module, action struct, `Transaction`
  ctor).
- **DO detect "outstanding merge-on-read deletes" by scanning the current snapshot's manifest list for a
  `ManifestContentType::Deletes` entry — not by reading the `total-delete-files` summary property.** *Why:*
  the manifest-list content type is the on-disk ground truth (a Java-written delete manifest always carries
  `content == Deletes`); a summary property can be absent, stale, or omitted by a non-Java writer. Loading the
  manifest list is one `load_manifest_list` call and is exactly what the producer already does.
- **DO confirm a producer-level guard does NOT catch the add-only case before trusting the action's
  precondition.** *Why:* the producer's `manifest_file()` rejects only a TRULY-empty commit (`added.is_empty()
  && deletes.is_empty() && props.is_empty()`). An add-only rewrite has added files, so the producer passes it —
  ONLY the action's `deleted_data_files.is_empty()` precondition rejects it. Mutation-verified: disabling the
  action precondition makes `test_rewrite_add_without_delete_rejected` COMMIT successfully (the `expect_err`
  panics on a returned `Table`), proving the action precondition — not the producer — is the load-bearing guard
  for add-only. The brief's worry (producer only rejects all-empty) is exactly right.
- **FLAG (tracked 🟡, not fixed): Java's REPLACE record-count invariant is unmirrored.** Java
  `SnapshotProducer` (the `BaseSnapshot`-construction path, lines 347-359) rejects a REPLACE whose summary has
  `added-records > deleted-records` ("Invalid REPLACE operation: %s added records > %s replaced records") — a
  compaction must not increase the live row count. Rust's producer commit path has NO such check. NOT added
  here: the check belongs in the shared `snapshot.rs` producer (Java puts it in `SnapshotProducer`, shared
  across ops), which is outside this increment's named file set, and it is a logical-consistency guard, not the
  data-loss trap. The point-2 claim "`SnapshotProducer.summary` is operation-agnostic" is correct for the
  TOTALS method (`summary(previous)`, all `updateTotal`, no per-op branch) — but the SIBLING REPLACE invariant
  lives in the snapshot-construction path, not `summary()`, so admitting `Replace` to the
  `update_snapshot_summaries` allowlist is right AND this separate guard is a distinct missing item.

### 2026-06-08 (Phase 2 Increment 5a — PositionDeleteWriter, BUILDER Opus)
- **DO check `metadata_columns.rs` for the reserved position-delete field-id constants BEFORE adding
  them — they already exist and already match Java.** *Why:* the brief said "ADD them there if missing
  (use the exact reserved ids)," but `RESERVED_FIELD_ID_DELETE_FILE_PATH = i32::MAX - 101 = 2147483546`
  and `RESERVED_FIELD_ID_DELETE_FILE_POS = i32::MAX - 102 = 2147483545` (= Java
  `MetadataColumns.DELETE_FILE_PATH`/`DELETE_FILE_POS`, `Integer.MAX_VALUE - 101/-102`) were already
  defined, with `delete_file_path_field()` (required string) / `delete_file_pos_field()` (required long)
  helpers — and the read side (`arrow/delete_filter.rs`) already hard-codes those exact ids in its test.
  The position-delete writer just reuses the existing helpers via a `pos_delete_schema()` free fn. Adding
  duplicate constants would have been churn; verify the arithmetic (`i32::MAX - 101` == the spec's
  `2147483546`) and reuse.
- **DO build a delete-writer's parquet schema FROM the reserved metadata-column fields, not from a
  hand-rolled Arrow schema, so the field ids are guaranteed correct.** *Why:* a position-delete file with
  the wrong field ids is silently unreadable by Java (interop break) and by our own `delete_filter.rs`.
  Building `Schema::builder().with_fields([delete_file_path_field().clone(),
  delete_file_pos_field().clone()])` → `schema_to_arrow_schema` → `ParquetWriterBuilder` makes the
  parquet `PARQUET_FIELD_ID_META_KEY` come out 2147483546/2147483545 by construction. Pin it with a test
  that reads the written parquet's arrow schema and asserts the exact id strings — a round-trip-of-values
  test alone does NOT catch a wrong id (the values still round-trip).
- **DO mirror Java `PositionDeleteWriter`'s write-AS-GIVEN contract and DOC that sorting by
  (file_path, pos) is the caller's responsibility — do NOT silently reorder.** *Why:* Java's basic
  `PositionDeleteWriter` "does not keep track of seen deletes and assumes all incoming records are
  ordered by file and position"; the SORTED ordering is `SortingPositionOnlyDeleteWriter`'s job. The spec
  (lines 1403-1405) only RECOMMENDS the sort (readers binary-search); an unsorted file is still valid +
  readable, just sub-optimal. Reordering inside the writer would (a) diverge from Java and (b) require
  buffering all rows. Mutation-verified the round-trip test asserts ORDER (dropping the last row of each
  batch fails the round-trip + multi-file + field-id tests) — so a writer that mangles positions is
  caught.
- **DO NOT `use crate::Result;` in a writer test module whose tests return `Result<(), anyhow::Error>`.**
  *Why:* `crate::Result<T>` is a 1-arg alias (`std::result::Result<T, crate::Error>`), so it collides
  with the 2-arg `Result<(), anyhow::Error>` test signatures → `E0107 type alias takes 1 generic argument
  but 2 were supplied`. The equality-delete writer test deliberately imports only `ErrorKind` (not
  `Result`) and relies on the prelude `std::result::Result` for the 2-arg form. Match that.
- **DO validate a delete-writer's input `RecordBatch` schema against the expected (file_path, pos) Arrow
  schema in `write()` and reject a mismatch with `ErrorKind::DataInvalid`.** *Why:* unlike the
  equality-delete writer (which PROJECTS an arbitrary input down to the delete columns via
  `RecordBatchProjector`), a position-delete writer's input IS already the 2-column schema — so a
  mismatched batch (wrong column order, missing field ids, wrong types) is a caller bug that would
  otherwise produce a corrupt/unreadable delete file. Compare `batch.schema().as_ref() ==
  arrow_schema.as_ref()` up front. Pin it with a negative test (a plain pos-then-path batch with no
  field-id metadata → rejected).

### 2026-06-08 (Phase 2 Increment 5a — PositionDeleteWriter, REVIEWER Opus)
- **DO prove a delete-WRITER's read-side consumability END-TO-END with a throwaway in-crate probe that
  drives the real `CachingDeleteFileLoader`, not just by schema/field-id matching.** *Why:* the brief's
  item-3 fallback ("at minimum confirm the schema/field-ids match what the reader expects") is weaker than
  what is actually reachable. `CachingDeleteFileLoader` is `pub(crate)`, so an in-crate test in the writer
  module CAN: write a pos-delete file via `PositionDeleteFileWriter` pointing at a data-file path, build a
  `FileScanTaskDeleteFile { file_type: PositionDeletes, .. }`, call
  `loader.load_deletes(&[entry], schema).await??` (it returns a `futures` oneshot `Receiver<Result<DeleteFilter>>`
  — `await` then `?` the recv then `?` the inner), and assert `delete_filter.get_delete_vector_for_path(path)`
  yields a `DeleteVector` whose `.iter().collect::<HashSet<u64>>()` EQUALS the written positions. This is the
  exact path 5b (RowDelta) will use. Mutation-verified load-bearing: mangling the writer's positions (+1) makes
  the probe's read-side vector `{1,3,6,1001,1024}` ≠ expected `{0,2,5,1000,1023}` — catching silent
  wrong-deletion end-to-end. Probe deleted after (rules); the committed value-round-trip + field-id tests are
  the persistent regression guard, and this probe was the one-time proof the two halves connect.
- **DO confirm the read side parses pos-delete files BY COLUMN POSITION (col 0 = StringArray file_path, col 1 =
  Int64Array pos) — not by field-id lookup — so column ORDER is the load-bearing interop contract on the read
  path, while field-IDS are the interop contract for Java.** *Why:* `caching_delete_file_loader.rs:326-350`
  downcasts `columns[0]`→`StringArray`, `columns[1]`→`Int64Array` and zips them; it never reads
  `PARQUET_FIELD_ID_META_KEY`. So the Rust reader would accept a file with wrong field ids as long as the
  column order/types are right — but Java matches by field id (`2147483546`/`2147483545`), so BOTH must be
  correct for full interop. The writer gets both right by building its schema from `delete_file_path_field()`
  / `delete_file_pos_field()` (correct ids) in the canonical order (path, pos). The `delete_filter.rs` test
  hard-codes the same two ids as `2147483546`/`2147483545` — independent confirmation the read side expects
  exactly the writer's ids. Verify the reader's matching mechanism (position vs. id) before claiming "wrong id
  = unreadable by Rust": here a wrong id is unreadable by JAVA but (currently) tolerated by Rust; a wrong
  column ORDER breaks Rust.
- **CONFIRMED Java-faithful (no change): write-AS-GIVEN + sort-order-id null.** Java `MetadataColumns.DELETE_FILE_PATH`
  (`core/.../MetadataColumns.java:64-75`) = `required(MAX-101, "file_path", string)` / `required(MAX-102, "pos",
  long)` — byte-for-byte the Rust reserved fields. Java's basic `PositionDeleteWriter` writes records as-given
  (does not reorder; sorting is `SortingPositionOnlyDeleteWriter`'s job), and the spec (`format/spec.md:745,
  1403`) says position deletes "are required to be sorted by file and position" but "should set sort order id
  to null" + "Readers must ignore sort order id for position delete files." The Rust writer correctly defers
  the sort to the caller AND never claims a sort order (it sets no `sort_order_id` on the `DataFile`), so it
  does not advertise an ordering it doesn't enforce — the dangerous case. The optional `row` column
  (field-id `MAX-103` / `2147483544`) and `referenced_data_file` are correctly out of scope (documented, not
  silently wrong).

### 2026-06-08 (Phase 2 Increment 5b — RowDelta action + producer delete-manifest support, BUILDER Opus)
- **DO add producer delete-manifest support as an ADDITIVE second add-path, parallel to
  `write_added_manifest`, not by overloading the data path.** *Why:* `SnapshotProducer::manifest_file()`
  already wrote a DATA manifest from `added_data_files`; merge-on-read needs a SECOND manifest with
  `ManifestContentType::Deletes` from a NEW `added_delete_files: Vec<DataFile>` field. The clean shape is
  `with_added_delete_files()` (a builder setter, so existing `SnapshotProducer::new` callers are
  untouched) + `write_added_delete_manifest()` (a byte-for-byte mirror of `write_added_manifest` that calls
  `new_manifest_writer(ManifestContentType::Deletes)` — the producer ALREADY had the `Deletes` arm wired to
  `build_v2_deletes`/`build_v3_deletes`) + a `manifest_file()` push when non-empty. The producer was built
  generic enough that data-only, delete-only, or both fall out of the same `commit()` with no operation
  branching. Keep the V1 arm (`builder.snapshot_id(id).build()`) for symmetry even though V1 has no delete
  manifests — it never fires (validation rejects delete content before a V1 table reaches it).
- **DO relax the empty-commit precondition to count `added_delete_files`, and verify the seq-inheritance
  path is the SAME as added data files.** *Why:* the producer's `manifest_file()` guard rejected a commit
  with no added DATA files + no removed files + no props — which would wrongly reject an add-deletes-only
  `RowDelta` (the crown-jewel case). Add `&& self.added_delete_files.is_empty()` to the guard. The added
  delete entries are `Added` with NO sequence number for V2/V3 (`builder.build()`, not
  `.snapshot_id(id).build()`), so the manifest-list reader inherits the new snapshot's seq at read time —
  IDENTICAL to added data files. This is load-bearing: a delete file's seq MUST exceed the target data's
  seq for the read side (`delete_file_index.rs`: pos-delete applies iff `delete_seq >= data_seq`) to apply
  it. Mutation-verified BOTH ways: a seq-0 stamp fails the dedicated seq test AND the crown jewel (the
  seq-0 delete no longer applies to seq-1 data → the deleted rows RESURRECT in the scan).
- **DO mirror Java `BaseRowDelta.operation()` DYNAMICALLY (Append / Delete / Overwrite), classified on
  the REQUESTED add sets, not statically `Overwrite`.** *Why:* the brief said "operation is OVERWRITE" but
  `core/BaseRowDelta.operation()` returns APPEND (adds data, no delete files, no data deletes), DELETE
  (adds delete files, no data files), else OVERWRITE — so the crown-jewel add-deletes-only case is DELETE,
  not Overwrite. The OverwriteFiles reviewer already established "align to the Java source, not the brief's
  hint" for the dynamic-operation question. `update_snapshot_summaries` already admits Append/Delete/
  Overwrite, so NO summary-allowlist edit was needed (unlike RewriteFiles, which had to add `Replace`).
- **DO route added delete files through `SnapshotSummaryCollector::add_file` and DON'T touch
  `snapshot_summary.rs` — its `add_file` already branches on content type.** *Why:* the brief flagged
  `snapshot_summary.rs` as edit-only-if-needed. The collector's `add_file`/`remove_file` ALREADY handle
  `PositionDeletes`/`EqualityDeletes` (incrementing `added_delete_files` + `added_pos_delete_files` +
  `added_pos_deletes` / the equality siblings) — so wiring a `for delete_file in &self.added_delete_files
  { summary_collector.add_file(...) }` loop in the producer's `summary()` is the whole change; the summary
  emits `added-delete-files`/`added-position-delete-files`/`added-position-deletes` for free. Verified by a
  test asserting those exact properties. Flagged: `snapshot_summary.rs` NOT touched.
- **DO build the crown-jewel end-to-end test on a REAL-FS `MemoryCatalog`
  (`MemoryCatalogBuilder::default().with_storage_factory(Arc::new(LocalFsStorageFactory))` over a tempdir
  warehouse) so the scan's FileIO reads the data + delete parquet files you wrote.** *Why:* the default
  `MemoryCatalog` storage is in-memory; a position-delete file written to the local FS would be invisible
  to the scan. Write the data file via the bare `ParquetWriterBuilder` + the table's `FileIO` under
  `{location}/data/`, finish its `DataFileBuilder` with `content(Data)` + partition; write the delete file
  via the 5a `PositionDeleteFileWriter` (with a `PartitionKey` so the delete file's partition MATCHES the
  data file's — the `delete_file_index` keys pos-deletes by `(partition, spec_id)` AND requires
  `delete_seq >= data_seq`); the delete parquet's `file_path` column rows MUST be the exact data-file path
  (the loader keys the delete vector by that path). Then `scan().select(["y"]).to_arrow()` and assert the
  surviving y-values. The crown jewel is the ONLY test that proves the write path produces delete files the
  read side actually honors — a manifest-shape test alone cannot (it never reads a row).
- **DO bring `FileWriterBuilder` (not just `FileWriter`) into scope to call `ParquetWriterBuilder::build`.**
  *Why:* `build()` is on the `FileWriterBuilder` trait; `write()`/`close()` are on `FileWriter`. Importing
  only `FileWriter` gives `no method named build` + three `type annotations needed` errors (the build call's
  type can't be inferred without the trait). Import both.
- **DO assert a row-delta's added-manifest shape by COLLECTING live file paths keyed by manifest content
  type, not by `manifest_file.has_added_files()`.** *Why:* `existing_manifest` carries EVERY prior manifest
  forward (a row delta only adds), and the fast-appended data manifest also has `has_added_files() == true`
  — so counting `Data` manifests with added files gives 2, not 1. Assert instead that the new data file's
  path appears in a DATA manifest and the delete file's path appears in the (exactly one) DELETE manifest;
  that is the real signal and is robust to carried-forward manifests.

### 2026-06-08 — RowDelta (increment 5b) REVIEW: seq-inheritance + forward-application verification
- **The position-delete forward-application negative is protected by TWO independent mechanisms, not
  just the seq guard — know this before claiming a test "isolates" the sequence number.** A position
  delete added at seq N must NOT apply to data added LATER (seq > N; spec line 1071: applies only when
  `data_seq <= delete_seq`). Two things enforce it: (1) the `delete_file_index` seq guard
  (`delete.sequence_number() >= Some(seq_num)` for pos-deletes, line ~204) decides which delete files
  are *candidates* for a data file; (2) `arrow/delete_filter.rs` keys the loaded delete VECTOR by the
  data-file path read from the delete file's `file_path` column (`upsert_delete_vector(data_file_path,
  ...)`), so a delete naming D1's path produces a vector under D1's path and the scan of D2 looks up
  D2's path → empty. Mutation-verified: forcibly removing the seq guard (mechanism 1) did NOT fail the
  end-to-end forward test, because mechanism 2 still spares D2. The e2e test
  (`test_row_delta_position_delete_does_not_apply_to_later_data`) is a valid behavioral pin (D2 stays
  intact through the real scan even when it shares D1's partition AND has rows at the deleted positions)
  but is NOT a clean isolation of the seq guard — the index-level seq semantics are unit-pinned
  separately in `delete_file_index.rs` (`test_delete_file_index_partitioned`/`unpartitioned`).
- **The seq-0 inheritance mutation is the decisive corruption probe — it fails BOTH the crown jewel and
  the forward test.** Forcing `builder.sequence_number(0)` on the added delete entry (instead of leaving
  it unassigned for V2/V3 inheritance) makes `data_seq(1) <= delete_seq(0)` FALSE → the position delete
  never applies → the deleted rows RESURRECT (scan returns all 5). This proves the inheritance is
  genuinely load-bearing: the delete entry MUST be written `Added` with no explicit seq so the
  manifest-list writer stamps `next_seq_num` (= the new snapshot's seq) via
  `assign_sequence_numbers` → `inherit_data` at read time — identical to added DATA files.
- **Java `BaseRowDelta.operation()` APPEND branch has `&& !deletesDataFiles()` that the Rust two-branch
  form omits — this is SOUND only because removeRows/removeDeletes are deferred.** Java:
  `addsDataFiles() && !addsDeleteFiles() && !deletesDataFiles() → APPEND`. The Rust RowDelta never
  removes files (its `delete_files`/`delete_entries` return empty), so `deletesDataFiles()` is always
  false and the simplified `adds_data && !adds_deletes → Append` is equivalent for this increment's
  surface. When removeRows/removeDeletes land, the third condition MUST be added or a data-add +
  data-remove row delta would wrongly record Append instead of Overwrite. Added
  `test_row_delta_add_data_only_records_append` (the previously-uncovered op branch) — mutation-verified.
- **The added-delete `validate_added_delete_files` requires the DEFAULT partition spec id, stricter than
  Java `add(DeleteFile)` (which only checks `spec(file.specId()) != null`, i.e. the spec EXISTS).** This
  matches the existing `validate_added_data_files` convention and the producer's single-default-spec
  manifest writer — a consistent, pre-existing producer limitation (non-default-spec writes are a broader
  producer change), NOT a RowDelta regression. Acceptable for this increment; flag if Java-faithful
  multi-spec delete commits are needed later.

### 2026-06-08 (Phase 2 Increment 6 — concurrent-commit conflict-validation foundation + ReplacePartitions, BUILDER Opus)
- **DO capture the conflict-validation starting snapshot as its OWN `Transaction` field set once in `new()`,
  NOT by reading `self.table` at validation time.** *Why:* `do_commit` RE-BASES on staleness — when the
  catalog head moved, it overwrites `self.table` with the refreshed base (line ~286), so the ORIGINAL head
  (the snapshot the transaction was built against = Java `SnapshotProducer.base.currentSnapshot()`) is LOST
  the instant the re-base runs. Storing `starting_snapshot_id: Option<i64>` separately, seeded from
  `table.metadata().current_snapshot_id()` at `Transaction::new`, is the only way it survives. A validation
  that read the (re-based) `current_table`'s head as its start would compute an EMPTY concurrent-commit set
  (start == current head) and never detect a conflict — the check would silently always pass.
- **DO run the `validate` hook AFTER the refresh/re-base and BEFORE the re-apply loop in `do_commit`, with
  the REFRESHED base as `current`.** *Why:* Java `SnapshotProducer.validate(currentMetadata, parent)` is
  called against the refreshed metadata so it can enumerate the snapshots that landed concurrently (those
  `current` has that are newer than `starting_snapshot_id`). Running it before the refresh would validate
  against the stale base (no concurrent commits visible); running it after re-apply would be too late (the
  updates are already built). The seam is: refresh → re-base → `for action { validate(...) }` → `for action
  { commit(...) }`.
- **DO make a conflict a NON-retryable `DataInvalid` (retryable defaults to false), and a commit-conflict a
  retryable `CatalogCommitConflicts`.** *Why:* the `backon` retry loop is `.when(|e| e.retryable())` — a
  retryable error re-runs `do_commit` (re-refresh + re-validate), a non-retryable one STOPS and propagates.
  A `ValidationException` (Java) is non-retryable: retrying would re-refresh to the SAME concurrent state and
  re-fail forever (infinite loop / retry exhaustion). `Error::new(ErrorKind::DataInvalid, msg)` is
  non-retryable by default (no `.with_retryable(true)`), which is exactly right. Pin it with
  `assert!(!err.retryable())` in the conflict test so a future `.with_retryable(true)` slip is caught — and
  assert the validation MESSAGE (not just `is_err()`) so a retry-exhausted `CatalogCommitConflicts` can't
  masquerade as a pass.
- **DO put the "added data files since a starting snapshot" walk as a SHARED free fn (`added_data_files_after`)
  reusable by every future conflict check, not inline in one action.** *Why:* Java centralizes it in
  `MergingSnapshotProducer.addedDataFiles`/`validationHistory`; `OverwriteFiles`/`RowDelta`/`DeleteFiles` all
  reuse it (plus siblings: deleted-files-since, added-delete-files-since). The walk is: from the current
  snapshot back via `parent_snapshot_id` to `starting_snapshot_id` (EXCLUSIVE — Java `ancestorsBetween` stops
  before the starting id), for each APPEND/OVERWRITE snapshot (Java `VALIDATE_ADDED_FILES_OPERATIONS`, NOT
  Delete/Replace), load its manifest list, keep DATA manifests it ADDED (`manifest.added_snapshot_id ==
  snapshot.snapshot_id()` = Java `manifest.snapshotId() == currentSnapshot.snapshotId()` — carried-forward
  manifests belong to older snapshots), collect `Added`-status entries (Java `ignoreDeleted().ignoreExisting()`).
  The three subtle filters (operation set, manifest-added-by-this-snapshot, Added-only) each prevent a false
  conflict: without them an OLD file copied forward, or a delete tombstone, would wrongly read as "added since."
- **DO simulate a REAL concurrent commit in the test (a separate `fast_append` on the SAME catalog between
  building the action and committing it), not a hand-mutated metadata.** *Why:* the only faithful proof is
  the actual race: `Transaction::new(&table)` captures S0; build `replace_partitions(...).validate_*()`;
  then `append_files(&catalog, &table, ...)` lands S1 (advancing the catalog head while the tx still
  references S0); then `tx.commit(&catalog)` triggers `do_commit`'s refresh→S1→validate. This exercises the
  capture-survives-rebase property AND the refreshed-base enumeration in one go. Mutation-verified BOTH
  directions: forcing `validate` to always-`Ok` makes the KEY test (conflict in replaced partition) FAIL
  (the replace wrongly commits over the concurrent file); forcing the conflict predicate to always-`true`
  makes the NEGATIVE control (concurrent append to an UNTOUCHED partition) FAIL (a legitimate disjoint
  concurrent write is wrongly rejected). The OFF control (no opt-in → concurrent append clobbered, commit
  succeeds) pins that the foundation is OPT-IN and default behavior is unchanged.
- **DO keep the conflict check OPT-IN (default no-op `validate`).** *Why:* the brief + Java both make it
  opt-in (`validateNoConflictingData()` must be called). The default `TransactionAction::validate` is a no-op
  `Ok(())`, so EVERY existing action inherits it with zero change and the default commit is still snapshot
  isolation. Turning it on by default would change behavior for every `ReplacePartitions` caller and break
  those relying on snapshot isolation (last-writer-wins). The OFF control test documents the clobber so the
  semantics are explicit, not accidental.

### 2026-06-08 (Phase 2 Increment 6 — REVIEWER Opus)
- **DO add a test that exercises the TX-CAPTURED `starting_snapshot_id` (NO `validate_from_snapshot`
  override) — the override path does NOT pin the survives-rebase property.** *Why:* the builder's 3 conflict
  tests ALL passed `.validate_from_snapshot(S0)`, so `effective_start = validate_from_snapshot.or(...)` short-
  circuited on the override and NEVER read the tx field. Mutation-verified the gap was REAL: breaking
  `Transaction::new`'s capture to `None`, OR rewriting `validate` to fall back to the REFRESHED head
  (`current.metadata().current_snapshot_id()`) instead of the tx field, failed NOTHING — all 3 stayed green.
  The brief's #2 highest risk (start re-read at validation time ⇒ empty concurrent set ⇒ silent always-pass)
  was thus unpinned. Added `test_..._using_tx_captured_starting_snapshot` (validation enabled, NO override):
  the `effective_start=current-head` mutation now fails EXACTLY this test (1 passed/1 failed: the override
  KEY test still green, the new one red), proving it is the unique guard for the capture surviving the re-base.
  LESSON: when a feature has an explicit override AND an implicit default source, a test that always sets the
  override cannot pin the default source — write one that omits the override.
- **DO isolate `added_data_files_after`'s carried-forward exclusion in its OWN test, not only end-to-end.**
  *Why:* the KEY/NEGATIVE conflict tests prove the right END result but cannot distinguish "the walk returned
  the 1 new file" from "the walk returned 1 new + 2 carried-forward but the predicate happened to filter the
  carried-forward ones out." Probed the helper directly (S0 has x=0 `a` + x=1 `b`; concurrent S1 adds x=0
  `a_new`): `added_data_files_after(table, S0)` returns EXACTLY `{a_new}` — the `added_snapshot_id ==
  snapshot_id` manifest filter + `Added`-status entry filter correctly drop the carried-forward `a`/`b`
  (re-referenced via their S0 manifest, `Existing` status). Kept as `test_added_data_files_after_excludes_
  carried_forward_manifests`. A bug counting carried-forward manifests = false-positive rejection of valid
  commits; this is the only test that would catch it.
- **DIVERGENCE (Rust-STRICTER, flagged not fixed): a non-ancestor `validate_from_snapshot` over-scans to root
  instead of Java's fail-loud.** Java `MergingSnapshotProducer.validationHistory` ends with
  `ValidationException.check(lastSnapshot == null || lastSnapshot.parentId() == startingSnapshotId, "Cannot
  determine history between starting snapshot %s and the last known ancestor %s")` — if the starting id is
  NOT an ancestor of the parent, the walk runs off the chain and Java THROWS. Rust `added_data_files_after`
  has no such post-walk check: it silently walks to the history root and returns ALL added files (verified by
  probe: a bogus start returns every added file, no panic). Direction is SAFE — over-scanning can only
  OVER-reject (more files considered ⇒ more likely to flag a conflict), never let a real conflict through —
  so no data-loss risk; but it diverges from Java's explicit error and could surprise a caller who passes a
  stale/wrong id. Narrow (only a misused `validate_from_snapshot`), so documented + pinned
  (`test_added_data_files_after_nonancestor_start_overscans_does_not_panic`) rather than fixed. Tracked as a
  parity follow-up (add the `lastSnapshot.parentId() == start` guard when the conflict-validation sub-sequence
  is hardened).

### 2026-06-08 (Phase 3 Increment 1 — `files`/`data_files`/`delete_files` inspection tables, BUILDER Opus)
- **DO put the files-table content filter at the MANIFEST level, then `entry.is_alive()` — NOT on the
  entry's `DataFile.content`.** *Why:* Java `BaseFilesTable` selects which MANIFESTS to read per concrete
  table (`FilesTable` → `allManifests`, `DataFilesTable` → `dataManifests`/content==DATA, `DeleteFilesTable`
  → `deleteManifests`/content==DELETES); within a manifest it then takes every LIVE entry. So the Rust mirror
  filters `ManifestFile.content` (`ManifestContentType::{Data,Deletes}`) per table and within each manifest
  keeps `entry.is_alive()` (Added/Existing) — it does NOT inspect `DataFile.content_type()` to decide
  membership. A DATA manifest holds only DATA files and a DELETE manifest only delete files, so the two
  filters agree on the membership set, but filtering at the manifest level is the Java-faithful structure
  (and is what `dataManifests()`/`deleteManifests()` do). Both filters are load-bearing and must be
  mutation-pinned independently: mutating the manifest-content filter makes `data_files` swallow the delete
  file; mutating `is_alive()` makes `files` surface the Deleted tombstone.
- **DO build the inspection-table partition column with a `StructBuilder::from_fields(partition_arrow_fields)`
  and per-field `PrimitiveType` dispatch, NOT `get_arrow_datum`/`Datum::new`.** *Why:* there is NO
  Iceberg-`Struct`-value → Arrow-array helper in the crate (`arrow/value.rs` only goes Arrow→Iceberg);
  `get_arrow_datum` returns a single-element `Scalar`, awkward to accumulate into a column. Iterating the
  default partition type's fields and appending each per-row `Option<&Literal>` (extracting the inner
  `PrimitiveLiteral` into a typed Arrow builder reached via `StructBuilder::field_builder::<T>(index)`) is
  the direct, robust path. Align the per-row value by index with `partition_type.fields()` —
  `Struct::fields()[i]` is the i-th partition value as `Option<Literal>`.
- **DO return `FeatureUnsupported` for timezone-tagged partition types (`timestamptz`/`timestamptz_ns`) in a
  `StructBuilder`-based column rather than silently using a plain micro/nano builder.** *Why:*
  `schema_to_arrow_schema` produces `Timestamp(unit, Some("+00:00"))` for tz-tagged types, but
  `StructBuilder::from_fields` creates a plain (no-tz) `TimestampMicrosecondBuilder`/…Nanosecond for that
  child; `StructBuilder::finish` reconciles children against the declared tz-tagged `Fields` and the type
  mismatch would surface late. A plain partition-on-timestamptz is rare; an explicit `FeatureUnsupported`
  with the type in the message beats a confusing downstream Arrow error. Flag it as a deferred edge.
- **DO make the metric-map VALUE field non-nullable (`Field::new("value", ty, false)`) when building a
  `MapBuilder` to match `schema_to_arrow_schema`.** *Why:* Java `DataFile`'s metric maps use
  `MapType.ofRequired`, so `schema_to_arrow_schema` emits `value: non-null`. A `MapBuilder` value field built
  with `nullable=true` makes `RecordBatch::try_new` fail with "column types must match schema types, expected
  ... non-null Int64 but found ... Int64". Carry the canonical Iceberg key/value field ids
  (`PARQUET:field_id`) on the map's key/value `Field`s too, or the produced Arrow schema won't match.
- **DO write a self-contained inspection-table test fixture from the scan `TableTestFixture`'s PUBLIC fields
  (`table`, `table_location`) + public crate writer APIs, NOT its private helpers.** *Why:* `setup_manifest_
  files`/`next_manifest_file`/`write_parquet_data_files` are private to `scan::tests` and `scan/mod.rs` was
  out of this increment's edit scope. The metadata table reads ONLY manifest metadata (never the parquet
  data), so a test can skip real parquet entirely: use a fixed fake `file_size_in_bytes`, write a DATA
  manifest (`build_v2_data`) + a DELETE manifest (`build_v2_deletes`) via `ManifestWriterBuilder` over
  `fixture.table.file_io().new_output(...)`, and stitch them into the current snapshot's manifest list with
  `ManifestListWriter::v2`. `add_delete_entry`/`add_existing_entry` are `pub(crate)`, reachable from an
  in-crate `#[cfg(test)]` module.

### 2026-06-08 (Phase 3 Increment 1 — `files`/`data_files`/`delete_files`, REVIEWER Opus)
- **DO mirror Java `BaseFilesTable.schema()`'s empty-partition special-case when porting a files-table:
  drop the `partition` column entirely for an UNPARTITIONED table.** *Why:* Java (lines 50-54)
  `if (partitionType.fields().isEmpty()) schema = TypeUtil.selectNot(schema, PARTITION_ID)` — "avoid
  returning an empty struct, which is not always supported. instead, drop the partition field." A naive
  port that always emits the `partition` NestedField produces a `Struct([])` column for unpartitioned
  tables — non-corrupting (the rows are right, no panic) but a schema-shape divergence that breaks
  column-set parity for interop. Increment 1 left this unhandled; the REVIEWER pinned it with
  `test_files_table_unpartitioned_keeps_empty_partition_struct_known_divergence` (asserts the current
  empty-struct column; flips to assert-absent when fixed) and tracked it as a GAP_MATRIX deferral rather
  than do the invasive conditional-column fix through the fixed 21-column row builder. *Apply:* when a
  metadata-table schema embeds the partition struct, branch on `default_partition_type().fields().is_empty()`.
- **DO mutation-pin BOTH the manifest-content filter AND `is_alive()` independently when reviewing a
  files-table — they are separately load-bearing.** *Why:* the manifest-content filter (`data_files`=DATA
  manifests, `delete_files`=DELETE manifests) and the `entry.is_alive()` live-entry filter guard different
  bugs. Verified by three throwaway mutations (deleted after): `Data => true` leaks the delete file into
  `data_files` (fails `test_data_files_table_excludes_delete_files`); `Deletes => true` leaks data files
  into `delete_files`; `if true` instead of `is_alive()` resurrects the Deleted tombstone (fails 4 tests).
  A green suite after a single mutation = an un-pinned filter.
- **DO verify inspection-table Arrow field ids by DUMPING them from the produced
  `schema_to_arrow_schema` output and diffing against the Java `*.getType()` ids one-by-one — don't eyeball
  the source `NestedField` ids.** *Why:* field ids are the interop contract; a transposed id is invisible in
  a passing value test. A 6-line probe (`for field in arrow.fields() { field.metadata()["PARQUET:field_id"] }`)
  confirmed all 21 `files`-table ids equal Java `DataFile.getType` in order, including the nested map
  key/value (117/118…) and list element (133, 136) ids. Also pin the partition value PER row-key (file_path),
  not just the multiset — a multiset assertion passes even if partitions are shuffled across rows.

### 2026-06-08 (Phase 3 Increment 2 — `entries` inspection table, BUILDER Opus)
- **DO trust the Java SOURCE field ids over the brief's paraphrase, and flag the divergence.** *Why:* the
  brief said `entries` data_file projection ids 0/1/2/3/4; `ManifestEntry.java:51-55` actually assigns
  `status`=0, `snapshot_id`=1, `sequence_number`=**3**, `file_sequence_number`=**4**, `data_file`=**2**
  (`DATA_FILE_ID = 2`). CLAUDE.md makes the Java source the spec-by-example; implementing 0/1/2/3/4 would
  break interop. The Rust `ManifestEntry` doc comments already carry the real ids (1/3/4/2), so they were the
  cross-check. Read `getSchema`/`wrapFileSchema` for the actual `Schema(...)` field order + ids, never infer.
- **DO read that Java `entries` reads `snapshot().allManifests` with NO `isLive()` filter — the Deleted
  tombstones ARE rows.** *Why:* this is the ONE behavioral difference from the `files` family (which filters
  `is_alive()`). `BaseEntriesTable.planFiles` filters manifests by the row-filter/content evaluator only, and
  `ManifestReadTask` yields every entry; `ManifestEntriesTable`'s javadoc: "exposes internal details, like
  files that have been deleted." A port that reuses the files-table `is_alive()` filter silently drops the
  status-2 tombstone and the entries table is wrong. Pin it with a test that asserts the Deleted file IS a
  present row with `status == 2` — the headline risk.
- **GOTCHA: `StructBuilder::from_fields` builds Map/List children as BOXED builders
  (`MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>` / `ListBuilder<Box<dyn ArrayBuilder>>`), NOT the
  typed `MapBuilder<Int32Builder, Int64Builder>`.** *Why it bites:* the old flat `FilesRowBuilder`
  constructed each `MapBuilder<Int32Builder, Int64Builder>` by hand with `.with_keys_field`/`.with_values_field`
  to carry field ids. When the `data_file` projection became a single `StructBuilder` (so `entries` can nest
  it AND `files` can flatten its `.columns()`), arrow's `make_builder` produces the BOXED map/list shapes for
  the `DataType::Map`/`DataType::List` children — so `field_builder::<MapBuilder<Int32Builder, Int64Builder>>(i)`
  returns `None` → "child builder at index N has an unexpected type" at runtime. Fix: fetch the children as
  `MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>` / `ListBuilder<Box<dyn ArrayBuilder>>` and
  `downcast_mut` the inner key/value/element builders. `make_builder` PRESERVES the keys/values/element field
  metadata (`.with_keys_field`/`.with_field`), so field ids survive automatically — the hand-rolled
  `metrics_map_fields` helper is then dead and was deleted. The build COMPILES with the wrong typed builder
  (it's just a `field_builder::<T>` generic) and only fails at the first append — a `RecordBatch::try_new`
  schema-validation or a `field_builder` `None` is the tell, so run the tests, don't trust a green build.
- **DO factor a shared Arrow row-builder as a single `StructBuilder` when one consumer needs the nested
  struct and the other the flat columns.** *Why:* `entries` needs `data_file` as ONE struct column; `files`
  needs the SAME 21 fields as top-level columns. A single `DataFileStructBuilder` (a `StructBuilder` over the
  `data_file_fields`) serves BOTH: `entries` appends it as the `data_file` column; `files` calls
  `.finish()` → `StructArray` and emits `.columns().to_vec()` as the top-level columns (the flattened
  Arrow schema IS the struct's child fields, so `RecordBatch::try_new(flat_schema, struct.columns())`
  type-checks). One projection, two shapes, no drift — the Rule-of-Three extraction. The `files` tests
  (unchanged) staying green after the refactor is the behavior-preservation proof.
- **DO assert inspection rows against the GENUINELY committed values, including writer-side overrides — not
  an idealized model.** *Why:* `ManifestWriter::add_delete_entry` STAMPS `entry.snapshot_id = self.snapshot_id`
  (the manifest's snapshot id) but PRESERVES the data/file sequence numbers; `add_existing_entry` preserves
  everything; an Added entry with no id/seq INHERITS the manifest-list entry's `added_snapshot_id`/seq at read
  time. So a Deleted entry written with an explicit `.snapshot_id(parent)` comes back carrying CURRENT's id,
  seq=parent's. The first cut of the test asserted "Deleted = parent id" and FAILED — the code was right, the
  expectation was wrong. Read the writer's `add_*_entry` contract before asserting; the test that compares to
  the real committed shape (incl. the stamp + the inheritance) is the one that pins read correctness.

### 2026-06-08 (Phase 3 Increment 2 — `entries` + shared `DataFileStructBuilder`, REVIEWER Opus)
- **DO confirm a builder-claimed field-id "correction" against the Java SOURCE line, not the brief.** *Why:*
  the brief said the entries top-level ids were `0/1/2/3/4`; the BUILDER overrode to `0/1/3/4/2`. Verified
  against `core/.../ManifestEntry.java:51-56` — `STATUS=0`, `SNAPSHOT_ID=1`, `SEQUENCE_NUMBER=3`,
  `FILE_SEQUENCE_NUMBER=4`, `DATA_FILE_ID=2` (data_file is **2**, the seq fields are **3/4**). The correction
  is RIGHT; the brief was wrong. Per CLAUDE.md (Java source is the spec-by-example), the source line wins over
  the prose brief every time — and the Arrow-schema test pins these exact ids so a regression can't slip.
- **DO mutation-test a SHARED projection from BOTH consumer shapes, not just one.** *Why:* `DataFileStructBuilder`
  feeds `files` (flattened `.columns()`) AND `entries` (nested `data_file` struct). Corrupting `record_count`
  (index 5) failed a test in BOTH (`test_files_table_record_count_...` + `test_entries_table_data_file_struct_...`);
  corrupting the boxed map VALUE failed BOTH (`test_files_table_partition_struct_and_metrics_map_present` +
  the entries struct test). A mutation that only broke one shape would mean the other shape's coverage was a
  gap. Both-shapes mutation is the discipline a Rule-of-Three extraction demands.
- **DO probe the boxed BINARY map path specifically — a typed-vs-boxed downcast bug hides until the bytes are
  read.** *Why:* `StructBuilder::from_fields` boxes the `lower_bounds`/`upper_bounds` map's `LargeBinary` value
  builder; the append path downcasts to `LargeBinaryBuilder`. The existing tests assert the int-value maps
  (`column_sizes`) and the map TYPE but never read the bound-map BYTES. A throwaway probe confirmed
  `lower_bounds {1: long(1)}` round-trips to `[1,0,0,0,0,0,0,0]` (LE) and the nested map key/value field ids
  survive the boxing (column_sizes k117/v118, lower_bounds k126/v127, upper_bounds k129/v130 — exactly Java
  `DataFile.java`). `PrimitiveType::Binary` maps to Arrow `LargeBinary` (`arrow/schema.rs:691`), so the
  `LargeBinaryBuilder` downcast for both `key_metadata` and the bound-map values is correct. (Verdict: no bug
  — the boxed metrics maps are correctly populated in both shapes.)

### 2026-06-08 (Phase 3 Increment 3 — `history` / `refs` / `metadata_log_entries`, BUILDER Opus)
- **DO commit a non-current-ancestor history fixture in TWO transactions, not one.** *Why:* a single
  `into_builder().add_snapshot(SIBLING).set_ref("main", SIBLING).set_ref("main", CURRENT).build()` makes
  SIBLING an *intermediate* snapshot (made current then superseded WITHIN one changeset) — and
  `TableMetadataBuilder::update_snapshot_log` (mirroring Java `TableMetadata.Builder`) DROPS intermediate
  snapshots from the snapshot log, so SIBLING never appears as a `history` row and the only non-trivial
  column (`is_current_ancestor`) can't be exercised. Split it: commit 1 grafts SIBLING and makes it current
  (it persists in the log), commit 2 rolls `main` back to CURRENT. SIBLING is now a historical log entry
  (not intermediate) whose `is_current_ancestor` is correctly false. The final snapshot log is
  [ROOT, CURRENT, SIBLING, CURRENT] (the rollback re-stamps CURRENT) — assert ≥1 row per id, not a fixed
  count, and that duplicate rows for the same id agree.
- **DO read the Java row-builder for `parent_id` semantics: it is the SNAPSHOT's parent, not the previous
  log entry.** *Why:* `HistoryTable.convertHistoryEntryFunc` looks up `snapshots.get(snapshotId).parentId()`
  per history entry — so `history.parent_id` = `snapshot_by_id(entry.snapshot_id).parent_snapshot_id()`,
  nullable, NOT the snapshot id of the preceding snapshot-log row. Easy to get subtly wrong because in a
  straight-line history they coincide; a forked/rolled-back history is where they diverge.
- **DO implement `metadata_log_entries.latest_*` fully — it is tractable from Rust metadata, no deferral.**
  *Why:* Java derives them via `SnapshotUtil.snapshotIdAsOfTime(ts)` = the LAST `table.history()` (snapshot
  log) entry whose `timestampMillis <= ts`, then that snapshot's `schemaId`/`sequenceNumber`; NULL when no
  snapshot is at/older than the timestamp (a creation-time metadata file). Rust's `metadata.history()` +
  `snapshot_by_id().{schema_id,sequence_number}` give exactly this — a 6-line `snapshot_id_as_of_time` walk.
  Don't reach for the brief's "or NULL with a precise note if unresolvable" escape hatch when the data is
  right there. The synthetic final log row is `(last_updated_ms, metadata_location)` — Java appends the
  CURRENT metadata file as the last `MetadataLogEntry`; an empty `metadata_log()` therefore still yields ONE
  row (the current file), and `metadata_location()` being `Option` → use `unwrap_or("")` defensively.
- **DO cast an Arrow timestamp column to its `TimestampMicrosecondType`, never `Int64Type`, in tests.** *Why:*
  `as_primitive::<Int64Type>()` on a `Timestamp(µs, "+00:00")` array PANICS at runtime ("primitive array"
  type mismatch) even though the physical storage is i64 — `as_primitive` checks the Arrow DataType, not the
  physical width. Cast to `TimestampMicrosecondType`; `.value(i)` then returns the i64 micros directly
  (= log millis × 1000), so the millis→micros conversion is still assertable.
- **DO copy a private cross-module test fixture locally rather than widen its visibility.** *Why:*
  `transaction::tests::make_v2_table` is `mod tests` (private to `transaction/`), unreachable from
  `inspect/`'s `#[cfg(test)]`. Re-reading the same committed `TableMetadataV2Valid.json` into a 15-line local
  `make_v2_table` in each inspect test module is the in-scope path; making `transaction::tests` public to
  share one helper would touch an out-of-scope file for a test-only convenience. (The `scan::tests` module IS
  `pub` and is what `snapshots.rs`/`entries.rs` reuse — but its `TableTestFixture` has a different history
  shape; the JSON-fixture path is cleaner for the pure-metadata tables.)
- **DO reach `metadata.refs` directly in-crate (`pub(crate)`), no new accessor needed.** *Why:* the brief
  flagged "if you need to ADD a public accessor, STOP and report." None needed: `TableMetadata.refs` is
  `pub(crate)` and `inspect/` is in the same crate, and every other field (`history()`, `metadata_log()`,
  `current_snapshot_id()`, `last_updated_ms()`, `snapshot_by_id()`) plus `Snapshot::{parent_snapshot_id,
  schema_id, sequence_number, timestamp_ms}` is already public. Confirm visibility before reporting a blocker.

### 2026-06-08 (Phase 3 Increment 3 — `history` / `refs` / `metadata_log_entries`, REVIEWER Opus)
- **DO pin the `<=` (inclusive) boundary in `snapshot_id_as_of_time` with a metadata-log entry whose
  timestamp EXACTLY EQUALS a snapshot-log timestamp — offsetting every test entry off the snapshot
  timestamps leaves the boundary unpinned and a `<`-vs-`<=` mutation SURVIVES.** *Why:* Java
  `nullableSnapshotIdAsOfTime` keeps the LAST snapshot-log entry with `timestampMillis <= ts`; the only
  case that distinguishes `<=` from a strict `<` is `ts == a snapshot-log timestamp` (the metadata file
  written by the very commit that created the snapshot, whose `lastUpdatedMillis == snapshot.timestampMillis`).
  The builder's `latest_*` test used `ROOT_TS-1000` / `ROOT_TS+1000` / `last_updated_ms` (= `1602…`, well
  past `CURRENT_TS`) — none landing ON a snapshot timestamp — so I mutation-tested `<=`→`<` and the test
  still PASSED. Added `test_…_inclusive_of_exact_snapshot_timestamp` (entries at exactly `ROOT_TS` and
  `CURRENT_TS` → resolve to ROOT and CURRENT, not NULL/ROOT); mutation-verified it now FAILS under `<` (entry
  at `ROOT_TS` resolved to 0 instead of ROOT). A wrong as-of-time = wrong `latest_snapshot_id`/`schema_id`/
  `sequence_number` per row, so the boundary is load-bearing, not cosmetic.
- **DO replace a bare `.unwrap()` on a statically-valid `Schema::builder().build()` with
  `.expect("<table> metadata table schema is statically valid")`, matching the in-scope sibling
  precedent.** *Why:* CLAUDE.md Non-Negotiable #3 / Opus §Rust forbid bare `.unwrap()` in production paths.
  The Increment-3 `schema()` methods copied the bare `.unwrap()` from the OLDER `snapshots.rs` template, but
  the immediately-preceding Increment-1/2 siblings (`files.rs`/`entries.rs`) had already adopted
  `.expect("… statically valid")` — so the bare `.unwrap()` was both a non-negotiable miss and inconsistent
  with the freshest precedent. When two precedents disagree, follow the one that satisfies the engineering
  floor. (The `is_current_ancestor` always-true mutation and the field-id Arrow probes were already
  correctly pinned by the builder — those needed no change.)

### 2026-06-08 (Phase 3 Increment 4 — `partitions` aggregating table, BUILDER Opus)
- **DO write a "committed by the OLDER snapshot" test entry via `ManifestWriter::add_existing_entry`, NOT
  `add_entry` — `add_entry` RESTAMPS `entry.snapshot_id = self.snapshot_id` (the manifest's snapshot) and
  forces `status = Added`, silently discarding a `.snapshot_id(parent)` you set on the builder.** *Why it
  un-pins a tie-break test:* the `partitions` `last_updated_*` logic picks the file whose COMMITTING
  snapshot has the max commit time (Java `Partition.update`, strict `>`). To pin the `>` (vs `<`/min-wins)
  I needed two files committed by DIFFERENT snapshots in the same partition. My first cut built both via a
  `write_data_manifest` helper that used `add_entry` for every entry — so BOTH files got stamped with the
  CURRENT snapshot id, the parent never participated, and mutating `>`→`<` left the test GREEN (only one
  distinct commit time existed). Fix: write the older file with `add_existing_entry` (status Existing,
  PRESERVES the parent snapshot id + seqs) and the newer with `add_entry` (Added, inherits current). Then
  `>`→`<` flips the answer to the parent and the test FAILS. `add_existing_entry`/`add_delete_entry` are
  `pub(crate)`, reachable from an in-crate `#[cfg(test)]`. *General rule:* before asserting a
  most-recent/oldest tie-break, verify the writer actually PRESERVES the per-entry snapshot id you depend on
  — a restamping add path collapses your distinct-commit-time setup to one value and the mutation survives.
- **DO confirm a strict `>`/`<` comparison test produces a DIFFERENT, asserted result under the mutation —
  a min-vs-max comparison needs two entries with DISTINCT commit times that genuinely both reach the
  comparator.** *Why:* same root cause as the snapshot-id restamp above. The `is_none_or(|cur| t > cur)`
  fold is "max-time-wins, first-writer wins a tie"; `<` makes it "min-time-wins." Only a test where the
  oldest and newest snapshots BOTH contribute a file to the same partition distinguishes them. Assert both
  the winning time AND that it is `assert_ne!` the losing snapshot's id, so a min-wins regression can't pass.
- **DO key an aggregating inspection table by the file's OWN partition `Struct` and use
  `default_partition_type` for the column schema — there is NO cross-spec partition-type unifier in Rust.**
  *Why:* Java `PartitionsTable` unifies ALL of a table's specs into one partition type via
  `Partitioning.partitionType(table)` and coerces each file's partition into it (`PartitionUtil.coerce
  Partition`). A grep of `spec/`/`transform/` for any analogue found only `PartitionSpec::partition_type
  (schema)` (SINGLE spec) and `TableMetadata::default_partition_type` — no union/coerce. So the
  single-spec-correct path (`Struct` derives `Hash`+`Eq` → direct `HashMap` key; schema = default partition
  type) is the right scope; cross-spec UNIFICATION (partition evolution → differently shaped tuples) is a
  documented DEFERRAL, not silently misaggregated. Still report the per-file `spec_id` so nothing is
  misattributed within a single spec. Do NOT invent a unifier in an inspection-table increment — flag it.
- **DO reuse the `files`-family unpartitioned-column decision for module consistency: keep the empty-struct
  `partition` column rather than dropping it (Java drops it).** *Why:* `inspect/files.rs` already KEPT an
  empty-struct `partition` column for unpartitioned tables as a documented divergence (Java
  `BaseFilesTable.schema()` drops it). Matching that in `partitions` gives the `inspect` module ONE
  consistent unpartitioned-column behavior (and one future fix site) instead of two. Pin it with the same
  `..._unpartitioned_keeps_empty_partition_struct_known_divergence` shape that flips to assert-absent when
  the module-wide drop-empty rule lands. A new divergence direction in a sibling table is worse than
  mirroring an existing, tested one.
- **DO collapse `if let A && let B` with let-chains when clippy's `collapsible_if` fires on the pinned
  nightly — nested `if let Some(x) { if let Some(y) { … } }` is now a `-D warnings` error.** *Why:* the
  most-recent-commit lookup naturally reads as `if let Some(id) = entry.snapshot_id() { if let
  Some(snap) = metadata.snapshot_by_id(id) { … } }`; clippy on `nightly-2025-10-27` rejects it. Rewrite as
  `if let Some(id) = entry.snapshot_id() && let Some(snap) = metadata.snapshot_by_id(id) { … }` (let-chains
  are stable on this toolchain). Run clippy `--all-targets -- -D warnings` from the repo root (pinned
  nightly) before declaring done — a plain `cargo build` does NOT surface it.

### 2026-06-08 (Phase 3 Increment 5a — `all_*` cross-snapshot file/entry tables, BUILDER Opus)
- **DO stamp a SHARED manifest's `sequence_number`/`min_sequence_number` explicitly before referencing it in
  a SECOND snapshot's manifest list in a multi-snapshot fixture.** *Why:* `ManifestListWriter::assign_sequence
  _numbers` (`spec/manifest_list.rs:274`) only ASSIGNS a seq to a manifest the list ADDED — i.e. one whose
  `added_snapshot_id == list.snapshot_id`. A manifest written under the PARENT snapshot but also referenced
  in the CURRENT snapshot's list (the dedup-test setup: one `ManifestFile` in both lists by the same path)
  reaches the current list with `added_snapshot_id != current_id` AND an UNASSIGNED seq (-1), so the writer
  errors `DataInvalid "Found unassigned sequence number for a manifest from snapshot N"`. Fix: after
  `write_manifest_file()`, set `manifest.sequence_number = parent.sequence_number()` and `.min_sequence_number`
  likewise (both public fields) — exactly the seq a real commit would have stamped when the parent first
  wrote it. A manifest carried forward into a later snapshot's list MUST already carry an assigned seq; only
  the list that ADDED it assigns one. (This is the manifest-list analogue of the entry-level "Existing entry
  must have a populated seq" rule.)
- **DO use `TableTestFixture::new()`'s EXISTING parent+current snapshot pair for a multi-snapshot inspection
  fixture — but WRITE BOTH manifest lists.** *Why:* `example_table_metadata_v2.json` already has two snapshots
  (parent `3051…`/seq 0 → `manifest_list_1`, current `3055…`/seq 1 → `manifest_list_2`), reachable via
  `current_snapshot.parent_snapshot(&metadata)` (`pub(crate)`). The current-snapshot tables (`files`/`entries`)
  only read `current_snapshot.manifest_list()`, so the Increment-1/2 fixtures leave the PARENT list
  (`manifest_list_1`) UNWRITTEN — but the `all_*` tables read it via `metadata.snapshots()`. The fixture must
  write BOTH lists (a `write_manifest_list(fixture, snapshot, manifests)` helper at `snapshot.manifest_list()`),
  or `load_manifest_list` on the parent fails (file absent). No new public accessor needed; everything
  (`snapshots()`, `parent_snapshot`, `manifest_list()`, the `ManifestFile` fields) is `pub`/`pub(crate)`.
- **DO preserve FIRST-SEEN order when porting Java's `Sets.newHashSet(reachableManifests)` dedup, even though
  Java's `HashSet` is unordered.** *Why:* Java `BaseAllMetadataTableScan.reachableManifests` (L80-82) returns a
  `HashSet<ManifestFile>` (equality by path) — UNORDERED. A faithful port could use any order, but a `HashSet`
  +`Vec` seen-set that pushes on first sight gives DETERMINISTIC output (a strict superset of Java's contract)
  and makes the row-order tests reproducible without sorting in production. Dedup the MANIFESTS only; the FILES
  inside are NOT deduplicated (javadoc "may return duplicate rows") — a shared manifest's file appears once
  because the manifest is read once, NOT because files are deduped. Return ALL-content manifests from the
  shared helper and let each table content-filter (`FilesTableKind`): dedup-by-path then filter == filter then
  dedup (content is intrinsic to a manifest), so one all-content source == Java's per-content
  `reachableManifests(dataManifests|deleteManifests|allManifests)`.
- **FIXED (2026-06-08, orchestrator, post-5a): `iceberg-datafusion` non-exhaustive `MetadataTableType` match
  → workspace build restored + all inspection tables now SQL-queryable.** *Why:* the builder correctly flagged
  that `crates/integrations/datafusion/src/table/metadata_table.rs` matched `MetadataTableType` for
  `schema()`/`scan()` but only handled `Snapshots`/`Manifests` — already non-exhaustive (E0004) over the
  `Files`/`DataFiles`/`DeleteFiles`/`Entries`/`History`/`Refs`/`MetadataLogEntries`/`Partitions` variants
  Increments 1-4 added; 5a's 4 `all_*` variants extended the already-broken match. *Fix:* wired ALL 14
  variants into BOTH match blocks (`.schema()` + `.scan().await`) mapping each to its `MetadataTable` accessor
  — so the crate compiles AND every inspection table is queryable as `tbl$<name>` via DataFusion SQL (real
  parity value, mirroring Spark's `tbl.metadata_table` surface). Also bumped the `test_provider_list_table_names`
  `expect_test` block (2 → 14 names, regenerated via `UPDATE_EXPECT=1`, diff inspected — enum order). Verified:
  `cargo build -p iceberg-datafusion` clean, lib 80/0 + integration 9/0, clippy + fmt clean.
- **PROCESS: the per-increment gate MUST include the consumers of any enum/trait you extend, not just
  `-p iceberg`.** *Why:* the `-p iceberg` fast gate never builds `iceberg-datafusion`, so a non-exhaustive-match
  break sat latent across FOUR committed increments (1-4) before 5a's builder caught it by chance. Adding a
  public enum variant is a CROSS-CRATE change. The gate now adds `cargo build --workspace --exclude
  iceberg-sqllogictest --all-targets` (sqllogictest needs `protoc`) — the same build CI runs — to catch
  downstream non-exhaustiveness before commit. (One residual quirk: `cargo test -p iceberg-datafusion --doc`
  in ISOLATION fails the pre-existing `table_provider_factory` `#[tokio::main]` doctest because tokio's
  `rt-multi-thread` feature isn't unified in; at `--workspace` doc scope — how CI runs — it passes. Not a
  regression; run doctests workspace-wide.)

### 2026-06-08 (Phase 3 Increment 5b — `all_manifests`, BUILDER Opus)
- **DO content-gate the manifest count columns in BOTH `all_manifests` AND the regular `manifests` table —
  Java content-gates in BOTH.** *Why:* `AllManifestsTable.manifestFileToRow` (core L286-303) AND
  `ManifestsTable.manifestFileToRow` (core L108-113) use the IDENTICAL gate
  (`manifest.content() == DATA ? count : 0` for the *_data_files_count columns, `== DELETES ? count : 0` for
  the *_delete_files_count columns). The Rust `inspect/manifests.rs` scan appended each manifest's
  added/existing/deleted counts to BOTH families unconditionally — a real bug (a DATA manifest reported its
  data counts in the delete columns too, and vice-versa). The fix is a one-line `is_data` gate shared with
  `all_manifests`; it ships with the regenerated `manifests` `expect_test` block (the fixture's DATA
  manifest's delete columns flip 1/1/1 → 0/0/0) and is mutation-pinned. Do NOT let `all_manifests` match the
  buggy non-gated behavior "for consistency" — fix the buggy one instead.
- **DO build `all_manifests` by iterating `metadata.snapshots()` DIRECTLY, NOT via the 5a
  `manifest_source::collect_manifest_files` helper.** *Why:* `all_manifests` is the ONE inspection table that
  must NOT dedup — Java `AllManifestsTable` javadoc "may return duplicate rows", one row per (manifest ×
  referencing snapshot). The 5a helper dedups manifests by `manifest_path` AND drops the snapshot identity,
  so it is fundamentally wrong here (it would collapse a shared manifest to one row and lose the
  `reference_snapshot_id`). Per-snapshot iteration + `snapshot.snapshot_id()` as `reference_snapshot_id` is
  the distinct machinery. This is the key distinction from the 5a file/entry `all_*` tables, which DO dedup
  manifests (but not the files inside them).
- **DO make the `reference_snapshot_id`-vs-`added_snapshot_id` distinction TESTABLE by giving the shared
  manifest an `added_snapshot_id` that differs from the referencing snapshot's id.** *Why:* a manifest's
  `added_snapshot_id` (the snapshot that committed it) and `reference_snapshot_id` (the snapshot whose list
  this row came from) are EQUAL for the snapshot that added it but DIFFER for any later snapshot that carries
  it forward. Write the shared manifest with `Some(PARENT)` (→ `added_snapshot_id = PARENT`) and list it in
  BOTH the parent's and the current's manifest lists; the current-snapshot row then has
  `reference_snapshot_id = CURRENT != added_snapshot_id = PARENT`, so a mutation that emits
  `added_snapshot_id` where `reference_snapshot_id` belongs is caught. If both ids were the same the swap
  mutation would survive.
- **DO note the schema NULLABILITY differs between `manifests` and `all_manifests` — copy from the Java
  source, not the sibling Rust table.** *Why:* Java `AllManifestsTable.MANIFEST_FILE_SCHEMA` (L57-81) makes
  the three `*_delete_files_count` columns (15/16/17) REQUIRED, whereas `ManifestsTable.SNAPSHOT_SCHEMA`
  (in the Rust port) makes the delete counts nullable. Blindly copying `manifests.rs`'s
  `NestedField::new(.., false/true)` flags would produce the wrong nullability. The Arrow-schema test pins
  all 14 columns' ids AND nullability so this can't drift. _(Re: `contains_nan`/11 — superseded 2026-06-08,
  see next entry: Java marks it `required` but that flag is NOMINAL; the Rust port must make it OPTIONAL.)_
- **DO NOT copy a Java metadata-table field's `required` flag verbatim into an Arrow builder when the Java
  ROW VALUE for that field can be null — Arrow enforces non-nullability, Java's `StaticDataTask.Row` does
  not.** *Why (2026-06-08, `all_manifests` review):* Java `AllManifestsTable.MANIFEST_FILE_SCHEMA` declares
  `contains_nan`/11 `required`, but `PartitionFieldSummary.containsNaN()` returns a nullable `Boolean` that
  is `null` for manifests with no NaN info (V1, or V2 without it — `contains_nan`/518 is OPTIONAL in the
  on-disk manifest-list spec). Java's loosely-typed `Object[]` row emits that null into the nominally-required
  cell with NO check, so Java "works." Arrow's `StructArray::try_new` REJECTS a null in a non-nullable child
  → the Rust scan PANICS (`"Found unmasked nulls for non-nullable StructArray field \"contains_nan\""`) on the
  common `contains_nan == None` case. Fix: mark the field OPTIONAL in the Rust Arrow schema (carries Java's
  emitted null faithfully). General rule: when porting a Java metadata table, check whether the field's
  PRODUCER can return null (boxed `Boolean`/`Integer`, `@Nullable`, a default-null getter) independently of
  the schema's `required` flag; if so, the Arrow field MUST be nullable regardless of the Java schema. Pin it
  with a test that drives a real null value through `scan()` (not just `Some(false)` from the writer default,
  which dodges the path entirely).
- **DO factor a shared Arrow builder into a `pub(super)` module on the SECOND real use (Rule of Three is a
  ceiling, not a floor, when the duplication is verbatim and the divergence is implausible).** *Why:* the
  `partition_summaries` list builder + `FieldSummary`→string-bound conversion is byte-identical between
  `manifests` and `all_manifests` (same element id 9, same struct, same `Datum::try_from_bytes` render). I
  pulled it into `inspect/partition_summary.rs` (`pub(super)` free fns) rather than duplicate it; this also
  let me drop the old `manifests.rs` bare `.unwrap()`s (propagate errors / `.expect` only the statically-valid
  schema build) in one place. The shared module means the two tables physically cannot drift.

### 2026-06-08 (Phase 3 Increment 1 — ResidualEvaluator core, BUILDER Opus)
- **DO mirror Java `ResidualVisitor.not` with a constant-folding `simplifying_not`, NOT the `Predicate` `!`
  operator.** *Why:* Java `Expressions.not` (`api/.../expressions/Expressions.java` L63-73) folds
  `not(true)=false`, `not(false)=true`, `not(not x)=x`, else wraps; but Rust's `impl Not for Predicate`
  (`predicate.rs`) deliberately does NOT simplify — it just wraps in `Predicate::Not`. So a residual
  `NOT(category==5)` whose inner leaf reduced to `AlwaysTrue` came out as `Not(AlwaysTrue)` instead of
  `AlwaysFalse`. The bound→unbound reconstruction's `Not` arm has the same trap. Pin it with a test that
  asserts a `not` over a partition leaf collapses to a bare constant both ways
  (`test_not_over_partition_leaf_negates_the_reduced_constant`). (Contrast: `Predicate::{and,or}` DO simplify
  constants, so `and`/`or` can use the operators directly — it's only `not` that needs the helper.)
- **DO compute a bucket-partition test's partition value with `create_transform_function(&Transform::Bucket(n))
  .transform_literal(&Datum::int(v))`, not a hand-guessed integer.** *Why:* the bucket-keep test must place the
  partition value where an `x = v` row actually lands — `bucket(x) == bucket(v)` — so the INCLUSIVE projection
  evaluates TRUE (does not reduce to false) and, bucket having no strict projection for `eq`, the predicate
  survives (the residual KEEPs it). A guessed value (e.g. `3`) almost never equals `bucket(5,16)`, so the
  inclusive projection is false and the residual wrongly collapses to `AlwaysFalse` — the test then fails for
  the wrong reason. `transform_literal` returns `Option<Datum>` whose `.literal()` is `PrimitiveLiteral::Int`.
- **DO pass the table `schema` into the residual evaluator's `of` ctor and precompute the partition `Schema`
  once.** *Why:* Java's `PartitionSpec` carries its schema so `spec.partitionType()` is free; Rust's
  `PartitionSpec::partition_type(schema)` REQUIRES the schema. The projected (partition-column) predicates from
  `Transform::{project,strict_project}` are UNBOUND and must be bound to the partition type before evaluating
  against the partition `Struct` — build a partition `Schema` from `partition_type.fields()` in the ctor (the
  same shape `expression_evaluator.rs`'s test helper `create_partition_filter` uses) and reuse it for every
  `residual_for` call. Binding an `AlwaysTrue`/`AlwaysFalse` projection short-circuits to its constant before
  the `ExpressionEvaluatorVisitor` runs (Java: "if the result is not a predicate it must be a constant").
- **DO reconstruct the leaf "keep the original predicate" residual from the bound leaf by the source field's
  NAME, and lean on the existing `BoundReference::field().name`.** *Why:* Java returns the original BOUND
  `pred`, but the Rust residual is an unbound `Predicate`; there is NO public bound→unbound conversion and no
  `column_name` accessor on `BoundReference`. Partition-source columns are always TOP-LEVEL schema fields, so
  `Reference::new(reference.field().name.clone())` is the faithful unbound reference (the nested-path
  `column_name` distinction never applies to a partition source). A generic `bound_to_unbound` that recurses
  And/Or/Not and rebuilds Unary/Binary/Set from the field name covers both the leaf-keep and the unpartitioned
  cases.
- **DO mutation-check the strict-vs-inclusive DIRECTION, not just "it reduces."** *Why:* swapping
  `strict_project`↔`project` in `reduce_leaf` still reduces SOME cases, so a single happy-path "reduces to
  AlwaysTrue" test survives the swap. The discriminating tests are the day-example "equals ONE bound" cases
  (strict decides the satisfied half ⇒ that half drops; inclusive only gates the false short-circuit) — the
  swap makes those keep/drop the wrong half. Confirmed: the swap fails exactly the 3 "equals bound" day cases
  + the bucket-keep (4 total), and the partition-ignore mutation fails the 9 reduction tests while the 7
  "kept verbatim" tests legitimately survive.

### 2026-06-08 (Phase 3 Increment 1 — ResidualEvaluator core, REVIEWER Opus)
- **DO test the `bound_to_unbound` logical (And/Or/Not) arms through the UNPARTITIONED path — they are
  unreachable from the partitioned path.** *Why:* in `residual_for`, the partitioned branch runs the
  `visit`/`BoundPredicateVisitor`, which decomposes And/Or/Not BEFORE the leaf methods, so `reduce_leaf`
  only ever hands `bound_to_unbound` a single Unary/Binary/Set leaf. The And/Or/Not arms of
  `bound_to_unbound` are reached ONLY by the unpartitioned `residual_for` (`bound_to_unbound(&self.filter)`).
  The Increment-1 builder's unpartitioned tests used only `And` + a single binary, leaving the `Not` arm
  untested — a mutation that drops the negation there (`simplifying_not(inner)` → `inner`) SURVIVED all 16
  tests. Added `test_unpartitioned_spec_round_trips_a_not_filter_keeping_the_negation` (a `NOT(id>100)`
  filter, unpartitioned) which the mutation now fails. Lesson: when a helper has arms reachable by two
  different callers, enumerate which caller reaches each arm and test from that caller.
- **DO pin a residual test for transform classes beyond day/identity/bucket — truncate, a temporal
  (year/month/hour), and void all feed scan correctness.** *Why:* the residual is correct for ALL partition
  types or it silently drops/over-scans rows. Verified empirically (throwaway probes) that truncate reduces
  all three ways (straddling→kept, all-above→AlwaysTrue, all-below→AlwaysFalse), year keeps both bounds
  inside the year + AlwaysFalse outside, and void KEEPS the predicate (Java `VoidTransform.project`/
  `projectStrict` both return null → no projection → no reduction, no panic on the null partition value).
  Added pinning tests for all three; the truncate/year tests also fail the strict↔inclusive swap, proving
  they are not theater.
- **DO pin the multi-partition-field-per-source loop (`getFieldsBySourceId`) — the single-field tests can't
  exercise it.** *Why:* Java loops over EVERY partition field whose source id matches the predicate column;
  a `break`-after-first mutation survives any spec with one field per source. A spec with BOTH
  bucket(category) (inconclusive for eq) AND identity(category) (conclusive) forces the loop to continue
  past bucket to let identity decide. Added a test (AlwaysTrue on match / AlwaysFalse on miss); the
  break-after-first mutation fails it.

### 2026-06-08 (Phase 3 Increment 2 — residual scan-wiring, BUILDER Opus)
- **DO state + test the residual-validity invariant as the load-bearing correctness claim.** *Why:* the whole
  reduction is only result-equivalent because every row in a data file belongs to that file's SINGLE partition
  tuple, so the partition-implied leaves the residual drops are TRUE for all rows in the file. Wrote it as a
  comment in `into_file_scan_task` and proved it with read tests that assert the residual-path rows EXACTLY
  equal the full-filter row set (identity AND truncate partitions). A residual regression silently returns
  wrong scan results, so this is the one invariant the tests must pin, not just "the predicate is reduced."
- **DO resolve the `partition_spec: None` TODO via `table_metadata.partition_spec_by_id(manifest_file.
  partition_spec_id)` in `PlanContext::create_manifest_file_context` (it has `table_metadata`), thread it onto
  `ManifestFileContext` → `ManifestEntryContext` → the task.** *Why:* all files in one manifest share the
  manifest's `partition_spec_id`, so the spec (and the `ResidualEvaluator` built from spec+filter) is resolved
  ONCE per manifest file and shared across its entries — the per-spec cache pattern, but per-manifest, so no
  shared mutable cache is needed. Per-file rebuild would re-run `spec.partition_type(schema)` + a `Schema`
  build for every file.
- **FLAG, DON'T MISS: setting `task.partition_spec = Some(spec)` ACTIVATES the arrow reader's
  identity-partition constant materialization (`reader.rs:451-455` → `record_batch_transformer::constants_map`,
  Java `PartitionUtil.constantsMap`), which is dormant while the field is `None`.** *Why it bites:* threading
  the spec (a required TODO) silently changes how identity-partition columns are READ — they become run-end-
  encoded constants from the partition METADATA, not values read from the data file. This is correct Iceberg
  behavior, but it exposed a pre-existing inconsistency in `TableTestFixture` (partition `x`=100/200/300 vs the
  parquet `x` column `[1; 1024]`) and broke 9 tests asserting `x`==1 as a plain `Int64Array`. A change that
  only "sets a field on a struct" can still alter downstream read behavior — grep the field's consumers
  (`task.partition_spec`) before assuming "reader unchanged."
  **REVERTED (2026-06-08, post-push): the constants-map activation was BACKED OUT — `task.partition_spec` is
  left `None`.** *Why:* CI's INTEGRATION tests (which my `-p iceberg` + workspace-BUILD gate never RAN) exposed
  two latent bugs in `record_batch_transformer::constants_map`: it emits a `RunEndEncoded` column where the
  declared scan schema is plain `Utf8` (`test_insert_into_partitioned` — "expected Utf8 but found
  RunEndEncoded"), and it cannot widen an `Int(i32)` partition literal to an `Int64` column
  (`test_evolved_schema` — "Unsupported constant type combination: Int64 with Some(Int(19))"). The constants-map
  is a SEPARATE parity feature with real bugs; activating it was a BUNDLED extra in the residual increment, not
  the residual goal — the residual works with `partition_spec = None` (the reader reads identity-partition
  columns from the data file). Reverting kept the residual + the (still-necessary) fixture-consistency fix and
  removed the blast radius; the constants-map + its fixes are deferred to a dedicated increment gated by the
  datafusion + integration read tests. Reverting also required removing the now-dead `partition_spec` field
  threading through `ManifestFileContext`/`ManifestEntryContext` (the local spec that builds the residual
  evaluator stays) and undoing the RunEndEncoded read assertions (`x` is plain `Int64` again).
- **PROCESS (2nd gate-widening): the per-increment gate must RUN the downstream crates' TESTS, not just BUILD
  them.** *Why:* the residual scan-wiring built the workspace clean and passed `-p iceberg`, but
  `iceberg-datafusion`'s INTEGRATION tests (MemoryCatalog-backed, runnable locally — NO Docker) failed on the
  constants-map activation, and the Docker-gated `iceberg-integration-tests` failed too. A workspace BUILD
  compiles datafusion's tests but does not RUN them. Add `cargo test -p iceberg-datafusion` (lib + the
  MemoryCatalog `integration_datafusion_test`) to the gate for any change that touches the SCAN/READ path
  (first gate-widening was the non-exhaustive-match → workspace BUILD; this one is read-path → datafusion
  TESTS). The Docker-only suites (`iceberg-integration-tests` REST/MinIO) still can't run locally, so a
  read-path change that can't be proven locally is a flag to scope conservatively.
- **DO make a contrived test fixture INTERNALLY CONSISTENT rather than asserting on inconsistent data.** *Why:*
  the old fixture's partition values were arbitrary distinct constants (100/200/300) for manifest realism,
  never reconciled with the parquet data because `partition_spec` was always `None` (the constant path never
  ran). Once the spec is threaded, the partition value IS authoritative for an identity column — so the
  truthful fix is partition `x`=1 (= the data), not "assert `x`==100 (the now-materialized constant)". A
  consistent fixture is the higher-integrity resolution and keeps the read tests meaningful. The change rippled
  one `inspect/manifests.rs` expect-snapshot (partition-summary bounds `"100"`/`"300"` → `"1"`); a shared
  fixture's partition values can reach any test that snapshots partition summaries — grep for the old values.

### 2026-06-08 — Residual scan-wiring REVIEW (spec-evolution mutation gap)
- **DO mutation-test "use the FILE's own spec vs the table DEFAULT spec" when a scan reads a per-file spec by
  id.** *Why it bites silently:* `scan/context.rs` resolves the residual's spec via
  `partition_spec_by_id(manifest_file.partition_spec_id)`. A fixture with only ONE spec CANNOT distinguish this
  from `default_partition_spec()` — the swap mutation passed all 1500 tests. After partition evolution an older
  manifest's `partition_spec_id` differs from the current default, so the default-spec bug would compute the
  residual (and the identity-constant materialization) against the WRONG spec → a silent wrong-rows scan. *Fix:*
  a 2-spec fixture (`new_with_evolved_default_spec`) writes the live file under the NON-default spec; pin that
  the residual reduces by the file's spec (`AlwaysTrue`) and `task.partition_spec.spec_id()` is the file's, not
  the default. A single-spec fixture is structurally blind to this class of bug — add the spec-evolution case.
- **DO reject a "better" multi-partition fixture fix when the data is shared and no coverage is at stake.** The
  shared `TableTestFixture` writes byte-identical parquet to all files in one loop; keeping distinct partition
  values valid would mean per-file distinct data + rippling every read assertion, for ZERO coverage gain
  (no test filtered on the partition column, and the multi-partition inspect tests build their own inline
  manifests). Collapse-to-consistent is the right call; spend the effort on the spec-evolution pin instead.

### 2026-06-08 (Increment 3 — OverwriteFiles filter-based validateNoConflictingData, BUILDER Opus)
- **DO reuse `InclusiveMetricsEvaluator::eval(&bound_filter, file, true)` as the per-file conflict TEST when
  porting Java `MergingSnapshotProducer.validateAddedDataFiles` / `ManifestGroup.filterData`.** *Why:* Java's
  "could this added file contain records matching the conflict filter?" IS the inclusive-metrics question
  (bounds/null/nan stats → may-match). The Rust evaluator already answers it (`pub(crate)` in
  `expr/visitors/inclusive_metrics_evaluator.rs`); the conflict validation differs from ReplacePartitions'
  `(spec_id, partition) ∈ drop-set` test ONLY in this leaf. NOTE the signature is THREE args
  (`filter, data_file, include_empty_files: bool`), not the two the brief implied — pass
  `include_empty_files=true` (conservative: never auto-exclude a 0-record file on emptiness). Build the bound
  filter once via `Predicate::bind(schema, case_sensitive)` (the `Bind` trait), not per-file.
- **DO default a `None` conflict-detection filter to `Predicate::AlwaysTrue` (any concurrent add conflicts),
  NOT to "no conflict".** *Why:* Java `BaseOverwriteFiles.dataConflictDetectionFilter()` returns `alwaysTrue()`
  when neither a `conflictDetectionFilter` nor a (non-alwaysFalse) `rowFilter` is set — the MOST conservative
  serializable check. Treating `None` as "skip" would invert the safety property and let every concurrent
  append through (a serializable-isolation hole). Pin it with a no-bounds concurrent file that MUST still be a
  conflict under the None default. (Our `overwriteByRowFilter` is deferred, so `rowFilter()` is effectively
  `alwaysFalse()` and Java's middle branch can never apply → `None ⇒ AlwaysTrue` is the exact mirror.)
- **DO report a serializable-isolation conflict as a NON-retryable `ErrorKind::DataInvalid` (Java's
  non-retryable `ValidationException`) and VERIFY the property two ways.** *Why:* a retryable error makes the
  `do_commit` retry loop spin and re-fail the same check forever, then return a misleading retry-exhausted
  `CatalogCommitConflicts`. Assert `kind()==DataInvalid` AND `!err.retryable()`; the genuinely-verified proof
  is a `.with_retryable(true)` mutation that makes the `!retryable()` assertion fail AND visibly inflates the
  test runtime (the loop actually loops, 0.12s→1.57s here) — kind-equality alone does not prove the loop stops.
- **DO simulate the concurrent commit with a REAL second `fast_append` through the same `MemoryCatalog`
  between txn-build and txn-commit (the landed ReplacePartitions pattern), so `do_commit`'s genuine
  refresh/re-base runs `validate` against the refreshed head.** *Why:* a hand-mocked base would not exercise
  the `Transaction::new`-captured `starting_snapshot_id` surviving the re-base — the one subtle correctness
  property. Pin it with a test that calls ONLY `validate_no_conflicting_data()` (no `validate_from_snapshot`):
  if the start were re-read from the refreshed head, start==head ⇒ empty concurrent set ⇒ the check silently
  always passes. Build the discriminating data files WITH column bounds (`DataFileBuilder.lower_bounds/
  upper_bounds`, which survive the manifest round-trip and reach `added_data_files_after`) so the inclusive
  evaluator can include (overlap) vs exclude (bounds outside the predicate).

### 2026-06-08 (Increment 3 — OverwriteFiles filter-based validateNoConflictingData, REVIEWER Opus)
- **VERDICT: CHANGES-MADE (docs only) — the production `validate` + 9 tests are correct and well-pinned;
  no production change needed.** All 6 adversarial-checklist mutations are CAUGHT by the existing suite
  (verified by running each, then restoring byte-identical): (a) invert the metrics decision → the EXCLUDE
  no-false-conflict test AND the 4 match/None tests fail (BOTH the false-positive and false-negative guards
  are load-bearing); (b) `validate` early-`Ok` → exactly the 4 rejection tests fail, OK tests green; (c) kind
  → `CatalogCommitConflicts` → the 4 `kind()==DataInvalid` assertions fail; (d) drop the
  `validate_from_snapshot` override → exactly `..override_changes_concurrent_window` fails; (e) re-read the
  refreshed head instead of the tx-captured start (the silent-always-pass false-negative) → exactly
  `..tx_captured_starting_snapshot` fails; (f) `.with_retryable(true)` → the `!retryable()` assertions fail
  AND runtime jumps 0.11s→1.59s (the retry loop demonstrably loops only when retryable). I ALSO mutated the
  SHARED `added_data_files_after` boundary EXCLUSIVE→INCLUSIVE (snapshot.rs) — 5 tests fail across
  OverwriteFiles + ReplacePartitions (incl. the dedicated `..excludes_carried_forward_manifests`), proving
  the exclusive-of-start boundary is pinned. No surviving mutation → no new pinning test required.
- **The FALSE-NEGATIVE enumeration check (the most important finding): the Rust added-file set MATCHES Java's,
  no under-reject.** Read `MergingSnapshotProducer.validationHistory` (L913-963) + `addedDataFiles` (L424-462)
  + `SnapshotUtil.ancestorsBetween`/`ancestorsOf` directly. Java: walk `ancestorsBetween(parent, startingId)`
  = INCLUSIVE of parent, EXCLUSIVE of starting (the lookup returns null at `oldestSnapshotId`, and
  `latest==oldest ⇒ empty`); for each snapshot whose op ∈ {APPEND, OVERWRITE} collect `dataManifests` where
  `manifest.snapshotId() == snapshot.snapshotId()`, then keep entries with `ignoreDeleted().ignoreExisting()`
  (= Added status). The Rust `added_data_files_after` walk is byte-for-byte the same semantics: same
  inclusive/exclusive boundary (break at top when `current.snapshot_id()==starting`), same op set
  (`operation_adds_data_files` = {Append,Overwrite}), same manifest filter (`added_snapshot_id ==
  snapshot_id`), same Added-only entry filter. Java's extra `newSnapshots.contains(entry.snapshotId())`
  filter is jointly satisfied by exactly the Added entries in a self-added manifest (an Added entry inherits
  the manifest's `added_snapshot_id`), so it adds nothing Rust drops. The ONE divergence (Increment-6 reviewer
  already flagged + pinned): Java throws if `startingId` is not an ancestor of parent; Rust silently
  over-scans to root — Rust-STRICTER (over-reject only), never an under-reject. **No false-negative vector in
  the enumeration.**
- **None-filter = `AlwaysTrue` is the faithful Java mirror.** `BaseOverwriteFiles.dataConflictDetectionFilter()`
  (L180-188): filter if set; else `rowFilter()` when `!= alwaysFalse && deletedDataFiles.isEmpty()`; else
  `alwaysTrue()`. `overwriteByRowFilter` is deferred ⇒ `rowFilter()` defaults to `alwaysFalse()` ⇒ the middle
  branch is unreachable ⇒ `None ⇒ alwaysTrue()` exactly. Pinned by the no-bounds-concurrent-file test.
- **Non-retryable is REAL and independent of the error KIND.** `Error::new` sets `retryable: false` by
  default (error.rs:235); `retryable()` returns the field, NOT a kind→bool map. So the conflict
  `Error::new(DataInvalid, …)` (no `.with_retryable(true)`) is non-retryable, and the `commit` retry loop
  (`.when(|e| e.retryable())`) stops + propagates. The 0.11→1.59s runtime jump under the `.with_retryable(true)`
  mutation is the load-bearing proof the loop does NOT spin on the real error.
- **`validate` IS invoked by the real commit path with the correct starting snapshot.** `do_commit`
  (mod.rs:308-312) loops every action's `.validate(self.starting_snapshot_id, &current_table)` AFTER the
  refresh/re-base (line 295) and BEFORE the re-apply (line 314), with `current_table` = the refreshed base.
  `starting_snapshot_id` is captured once in `Transaction::new` (line 117) as its OWN field and survives the
  re-base. Not dead code.
- **DO reconcile the Roadmap NARRATIVE mentions when a status flips, not just the detail/header.** The builder
  updated the GAP_MATRIX row (accurate + honest: states the over-scan, None-default, case-sensitivity, 🟡
  rationale) and the Phase 2 status header, but left two Roadmap narrative lines saying "conflict validation …
  deferred" for OverwriteFiles (an under-claim) and the `mod.rs::overwrite_files()` ctor doc saying conflict
  validation is "not yet supported." Fixed all three (the only changes this review made). Recurring lesson:
  grep the capability name across Roadmap + the public-API ctor doc, not only the matrix.

### 2026-06-08 (Increment 4 — RowDelta validateNoConflictingDataFiles + shared conflict-check helper, BUILDER Opus)
- **DO extract a shared `pub(crate)` helper on the second use of a load-bearing safety check, and PROVE the
  refactor behavior-preserving with the EXISTING tests (don't write new pins for the refactor).** *Why:*
  `RowDelta.validateNoConflictingDataFiles` is the IDENTICAL filter-based added-data-file conflict check as
  `OverwriteFiles.validateNoConflictingData` (both = Java `MergingSnapshotProducer.validateAddedDataFiles`).
  Per Rule-of-Three this is exactly the "extract on the 2nd use when the duplication is unmistakably the same
  concept" case (no plausible divergence — the two leaf checks differ only in the action's flag/filter
  plumbing, which STAYS in each action). Factored the walk+bind(None⇒AlwaysTrue)+per-file
  `InclusiveMetricsEvaluator::eval`+first-conflict-non-retryable-`DataInvalid` into
  `validate_no_conflicting_added_data_files(current, effective_start, conflict_filter, case_sensitive)` next to
  `added_data_files_after`. The behavior-preservation PROOF is that Increment-3's 9 OverwriteFiles conflict
  tests stayed GREEN unchanged after `OverwriteFiles::validate` was rewritten to delegate — a refactor that
  needs new tests to validate it isn't behavior-preserving by definition. Keep the per-action flag-guard +
  `effective_start = validate_from_snapshot.or(starting_snapshot_id)` in each action; only the shared mechanics
  move.
- **DO render the conflict-filter the same way in the shared helper as the inline code did, so the error
  message + tests are unchanged across the refactor.** *Why:* the Increment-3 message rendered a `None` filter
  as the literal `"true"` (`conflict_filter.map_or_else(|| "true".to_string(), |f| format!("{f}"))`,
  `Predicate: Display`). The helper takes `Option<&Predicate>` and renders it identically, so the
  `err.message().contains("conflicting files")` + file-path assertions in BOTH actions' tests pass verbatim —
  no test churn, the strongest signal the extraction changed nothing observable.
- **DO mutation-test a SHARED helper from BOTH consumers (the cross-action mutation).** *Why:* a shared safety
  helper's load-bearing-ness must be proven for EVERY caller, not just one. Inverting the helper's
  `InclusiveMetricsEvaluator::eval` decision (`if !eval(...)`) failed the EXCLUDE/no-false-conflict test in
  BOTH `transaction::overwrite_files` AND `transaction::row_delta` simultaneously — the single mutation, two
  failures, is the proof neither action's coverage is a gap and the helper can't silently rot for one caller.
  (The action-local mutations — force RowDelta `validate` always-`Ok` → its 4 rejection tests fail; retryable
  error kind → `!retryable()` fails + the retry loop spins 0.06s→1.57s — pin the per-action plumbing.)
- **DO build the discriminating conflict-test data files WITH column bounds (`lower_bounds`/`upper_bounds` on
  the filtered field id), reusing the OverwriteFiles `data_file_with_y_bounds` shape.** *Why:* the
  include-vs-exclude decision is the `InclusiveMetricsEvaluator` reading the file's bounds against the
  predicate; a file with NO bounds is always a (conservative) match, so it can only pin the None/AlwaysTrue
  default, not the metrics path. A file with `y∈[60,70]` overlaps `y>=50` (match) and `y∈[10,20]` is below it
  (exclude) — the two halves of the metrics decision. The bounds must be on the schema field id (here `y`=id 2),
  and they survive the manifest round-trip into `added_data_files_after`.

### 2026-06-08 (Phase 3 — RowDelta validateNoConflictingDeleteFiles, BUILDER Opus)
- **DO parameterize the "added files since a starting snapshot" walk by `(content, op_predicate)` once a SECOND
  content type needs it — `added_files_after(table, start, content: ManifestContentType, op_adds: fn(&Operation)->
  bool)`.** *Why:* the data-file walk and the delete-file walk differ ONLY in the manifest-content filter
  (`Data` vs `Deletes`) and the operation set (`{Append,Overwrite}` vs `{Overwrite,Delete}` — Java's
  `VALIDATE_ADDED_FILES_OPERATIONS` vs `VALIDATE_ADDED_DELETE_FILES_OPERATIONS`). Everything else (the
  parent-chain walk, exclusive-of-start, manifest-added-by-this-snapshot, `Added`-only) is identical. Both
  `added_data_files_after` (`(Data, operation_adds_data_files)`) and the new `added_delete_files_after`
  (`(Deletes, operation_adds_delete_files)`) become thin calls; the data enumerator's observable behavior is
  unchanged (the 241 `transaction::` tests stay green = the proof). Note the op sets are NOT the same: an
  `Append` snapshot adds data but NEVER delete files; a `Delete` snapshot adds delete files but never data — so
  the delete enumerator MUST use `{Overwrite, Delete}`, not reuse the data predicate (a real, mutation-pinnable
  divergence).
- **DO put the V2 guard in `added_delete_files_after` itself (`format_version() < FormatVersion::V2 ⇒ Ok(vec![])`),
  mirroring Java `addedDeleteFiles`'s `base.formatVersion() < 2` early return.** *Why:* delete files don't exist
  before format version 2, so the guard belongs at the enumeration door, not in each caller. `FormatVersion`
  derives `Ord` (`V1=1 < V2=2 < V3=3`), so the `<` comparison is direct. CAVEAT — the guard is a Java-FAITHFUL
  SHORT-CIRCUIT, not behavior-changing: a V1 table cannot have DELETE manifests AND a concurrent V1 commit is an
  `Append` (excluded by the `{Overwrite,Delete}` op set), so the walk yields nothing even with the guard removed.
  The mutation "drop the guard" leaves the V1 test GREEN — which is the honest finding, NOT a missing guard.
  Document it as such; pin the guard's contribution by asserting `added_delete_files_after(v1_table, None)` returns
  empty directly (the only assertion that names the guard's effect, since no end-to-end V1 scenario can
  distinguish guard-present from guard-absent).
- **DO extract the per-file conflict test (`first_conflicting_file`) on the SECOND filter-based check and prove
  it load-bearing for BOTH consumers with a SINGLE cross-consumer mutation.** *Why:* the data-file and delete-file
  conflict checks share the exact "bind None⇒AlwaysTrue once → per-file `InclusiveMetricsEvaluator::eval` → first
  match" logic; only the enumeration (`added_data_files_after` vs `added_delete_files_after`) and the error message
  differ. Extracting `first_conflicting_file(files, current, filter, case_sensitive) -> Result<Option<DataFile>>`
  and having both `validate_no_conflicting_added_data_files` and `validate_no_conflicting_added_delete_files` call
  it means inverting its metrics decision (`if !eval`) fails the EXCLUDE test in BOTH `transaction::overwrite_files`
  (data) AND `transaction::row_delta` (delete) at once — one mutation, two failures, is the proof the shared test
  protects every caller. The DELETE message ("Found new conflicting delete files that can apply to records
  matching %s: %s", Java `validateNoNewDeleteFiles` L562-570) must DIFFER from the data message ("...can contain
  records...") so a test can assert the delete branch (not the data branch) fired — assert the DELETE substring is
  present AND the data substring is absent.
- **DO simulate the concurrent DELETE commit with a real `row_delta().add_deletes(...)` through the catalog**
  (the merge-on-read counterpart of `append_files`), committed between txn-build and txn-commit. *Why:* it produces
  a real `Operation::Delete` snapshot with a real DELETE manifest — exactly what `added_delete_files_after` must
  enumerate (and `Delete` is in `{Overwrite,Delete}`). A position-delete `DataFile` carrying `y` bounds (field id 2)
  drives the include/exclude metrics decision (the evaluator is content-agnostic — it reads the file's
  `lower_bounds`/`upper_bounds` regardless of content type), reusing the `data_file_with_y_bounds` shape.
- **DO keep the data + delete conflict checks INDEPENDENT (two flags, two `if` branches in `validate`), mirroring
  Java's two `validateNew*` booleans, and pin the independence BOTH ways.** *Why:* Java exposes
  `validateNoConflictingDataFiles()` and `validateNoConflictingDeleteFiles()` as separate methods setting separate
  flags; enabling one must NOT run the other. Two tests pin it: delete-flag-only must allow a matching concurrent
  DATA append (the data check did not run), and data-flag-only must allow a matching concurrent DELETE (the delete
  check did not run). A single "both off / both on" pair cannot catch an accidental coupling where one flag enables
  both checks.
