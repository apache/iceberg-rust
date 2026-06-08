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

### 2026-06-08 (Phase 2 Increment 7 — cherrypick / snapshot replay, BUILDER Opus)
- **DO decide fast-forward FIRST, then skip validateNonAncestor for it — mirror Java's `validate`
  control-flow, not just its predicates.** *Why:* Java `CherryPickOperation.validate` is
  `if (!isFastForward(base)) { validateNonAncestor(...); validateReplacedPartitions(...);
  WapUtil.validateWapPublish(...); }` (L165-170) — the non-ancestor check is GATED on not-fast-forward. A
  fast-forward source's parent IS the current head, so it is trivially NOT an ancestor of current and would
  pass anyway, but the ordering matters for the OTHER branches: classify FF (source.parent == current OR both
  null, Java `isFastForward` L173-182) BEFORE running validateNonAncestor / the op classification, because the
  fast-forward path produces NO snapshot and must not run the producer at all (it just emits
  `SetSnapshotRef(main → source)`).
- **DO write a per-SINGLE-snapshot "files added/removed by snapshot X" helper, distinct from the
  per-CHAIN `added_data_files_after`.** *Why:* the conflict-validation `added_data_files_after` WALKS the parent
  chain (inclusive of current, exclusive of a starting id) — that is the wrong shape for cherry-pick, which
  needs exactly ONE snapshot's changes (Java `SnapshotChanges.builderFor(cherrypickSnapshot)`). The two share
  the manifest-reading idea (`added_snapshot_id == snapshot_id` filter + status filter on the manifests THAT
  snapshot created) but differ in traversal. Added `added_data_files_by_snapshot`/`removed_data_files_by_snapshot`
  (shared body `snapshot_changed_data_files(table, id, wanted_status)`): load the SOURCE snapshot's manifest
  list, keep DATA manifests where `added_snapshot_id == source_id`, collect entries with `status ==
  Added`/`Deleted`. Carried-forward manifests (older `added_snapshot_id`) and `Existing` entries are excluded —
  they are not changes THIS snapshot introduced.
- **DO mutation-verify the replay helper is load-bearing by flipping the status filter Added→Existing.** *Why:*
  a cherry-pick test could pass tautologically if the picked data happened to already be reachable. Flipping
  `added_data_files_by_snapshot` to collect `Existing` instead of `Added` (sed on the one line) makes it
  return nothing for a fresh-append source → the crown-jewel append-replay test, the WAP test, AND the
  dynamic-overwrite test all FAIL (left `{d}` vs right `{a2, d}` etc.), and ONLY those three (the 5 validation/
  fast-forward tests stay green). That precisely localizes which tests pin the data replay vs. the control flow.
- **DO build the crown-jewel "data not on main" fixture with append→append→append + a rollback, and pick the
  snapshot whose PARENT is the intermediate (not the rollback target) so it is genuinely non-fast-forward.**
  *Why:* the obvious "append A→S1, append B→S2, roll main to S1, cherry_pick(S2)" is a FAST-FORWARD (S2.parent
  == S1 == current), so it tests the wrong mode. For a NON-fast-forward append replay you need source.parent ≠
  current: append A→S1, X→S2, C→S3, roll main to S1, cherry_pick(S3) — S3.parent == S2 ≠ S1 → append replay,
  and the scan asserts {A, C} (C picked, X NOT — S3 only added C). The same trap bites the WAP and
  dynamic-overwrite tests: stage the WAP/overwrite source behind an intermediate snapshot so the pick is
  non-FF (FF never runs `validate_wap_publish` / the producer).
- **DO build the dynamic-overwrite cherry-pick fixture so source.parent is an ancestor of current but ≠
  current.** *Why:* the overwrite branch requires `parentId == null || isCurrentAncestor(parentId)` (Java
  L101-105) AND not-fast-forward. Append A@x0→S1; `replace_partitions` A2@x0→S2 (Overwrite, parent S1,
  removes A); roll main back to S1; append D@x2→Q (child of S1). Now cherry_pick(S2): S2.parent == S1, current
  == Q ≠ S2 (not FF), S1 is an ancestor of Q (parent precondition holds) → dynamic replay re-adds A2 + re-deletes
  A → scan {A2, D}. Rolling main back to S2 itself would make it fast-forward; rolling to a NON-descendant of
  S1 would fail the parent-ancestor precondition — the sibling-via-rollback-then-append is the shape that lands
  in the replay branch.
- **DO route a non-fast-forward replay through `SnapshotProducer` with the source's op as the DYNAMIC
  `operation()` and the source's removed files via the `delete_files` seam.** *Why:* the producer already
  composes "write an added manifest from `added_data_files`" + "rewrite current manifests to delete
  `delete_files`" in one snapshot and stamps `source-snapshot-id`/`published-wap-id` via `snapshot_properties`
  — exactly Java `CherryPickOperation`'s `add(addedFile)` + `delete(deletedFile)` + `set(...)`. The
  `CherryPickOperation` impl records `operation()` = the SOURCE's operation (Java `operation()` returns
  `cherrypickSnapshot.operation()`) so an append-replay is an `Append` snapshot and an overwrite-replay an
  `Overwrite` snapshot. No new producer machinery — the increment-1/2 add+delete seam IS the
  MergingSnapshotProducer-equivalent the cherry-pick commit was gated on.
- **DO use `let Err(err) = result else { panic!(...) }` for a `commit()` whose `Ok` is `Table` (not just
  `ActionCommit`).** *Why:* clippy `err_expect` forbids `result.err().expect(...)`, but its suggested
  `expect_err` requires `T: Debug` and neither `Table` nor `ActionCommit` is `Debug`, so `expect_err` won't
  compile. The `let Err(...) else { panic! }` form extracts the error without a `Debug` bound and satisfies
  clippy. (Generalizes the earlier `ActionCommit`-not-`Debug` lesson to any non-`Debug` `Ok` type.)

### 2026-06-08 (Phase 2 Increment 7 — cherrypick REVIEW, Opus REVIEWER, DELEGATED)
Adversarially verified points 1–6 of the cherry-pick brief against the Java source
(`/tmp/iceberg-java-ref/core/.../CherryPickOperation.java`, `util/WapUtil.java`, `SnapshotSummary.java`) +
mutation tests. **No production bug found** — the builder's impl is Java-faithful across all three modes,
both validateNonAncestor legs, the WAP publish-once check, and the duplicate-file interaction. Added 3
isolation tests (replay-exactness helper probe, source-snapshot-id ancestry leg, overwrite removed-helper
probe), each mutation-verified as load-bearing. Lessons:
- **DO write a DIRECT helper-level probe of `added_data_files_by_snapshot` / `removed_data_files_by_snapshot`
  (replay exactness), not only the end-to-end crown jewel.** *Why:* the crown jewel proves the right final
  live set but cannot distinguish "the helper returned exactly the source's added files" from "it returned
  more, but the producer's dedup/carry-forward happened to mask it." A fast-append carries prior snapshots'
  manifests forward with their entries STILL `Added`-status (a fast append does NOT rewrite them to
  `Existing`), so the ONLY thing excluding A/B from S3's "added files" is the `added_snapshot_id ==
  source_id` manifest filter. Probed directly: `added_data_files_by_snapshot(table, S3) == {C}` (not
  {A,B,C}). Mutation-verified BOTH filters: dropping the `added_snapshot_id` filter returns {A,B,C} (fails
  the probe + crown jewel + WAP); flipping the status filter `Added`→`Existing` returns {} (fails all data-
  replay tests). The probe localizes the carried-forward-exclusion guarantee that the crown jewel only
  proves indirectly.
- **DO add an isolation test for the SECOND `validateNonAncestor` leg (`lookupAncestorBySourceSnapshot`),
  which the direct-ancestor test does NOT exercise.** *Why:* a non-fast-forward pick produces a NEW snapshot
  P (P != source S), so the source itself is NEVER an ancestor of main — the `is_ancestor_of(source,
  current)` leg stays false on a re-pick. ONLY the `source-snapshot-id == source` ancestry walk catches "an
  ancestor already published this source"; without it the SAME off-branch source replays twice (double-apply
  of data). Built: pick S3 → P (P.source-snapshot-id == S3); re-pick S3 → rejected. Mutation-verified:
  neutralizing the source-snapshot-id walk fails EXACTLY the new test while the direct-ancestor test stays
  green — proving they pin different legs. LESSON (generalizes increment 6's "override-vs-default-source"):
  when a guard has TWO independent rejection paths, a test hitting only the first cannot pin the second;
  write one whose ONLY reachable rejection is the second leg.
- **Fast-forward branch (point 4): the Rust `is_fast_forward` matches Java `isFastForward` incl. the
  both-null and parent-null-but-current-present edges.** Java: current!=null ⇒ `parentId != null &&
  current == parentId`; current==null ⇒ `parentId == null`. Rust: `Some(c) => source_parent == Some(c)`
  (a `None` source-parent → `None == Some(c)` = false, matching Java's `parentId != null` requirement);
  `None => source_parent.is_none()`. FF correctly SKIPS validateNonAncestor (`if !is_fast_forward {
  validate_non_ancestor() }`, gating on `!isFastForward` like Java L165) and produces NO snapshot (a single
  `SetSnapshotRef(main → source)`), pinned by `test_cherry_pick_fast_forward_moves_main_without_new_snapshot`
  (main == source, no `source-snapshot-id`). The FF path preserves main's EXISTING retention (reads
  `metadata.refs[main].retention`) rather than the producer's `branch(None,None,None)` reset — closer to
  Java, not a bug.
- **Duplicate-file interaction (point 5): cherry-pick does NOT call `validate_duplicate_files` (only
  `FastAppendAction` does, grep-confirmed), so re-adding a physically-existing off-branch file is NOT
  wrongly duplicate-rejected — correct.** A file already LIVE on main would be double-referenced if its
  source snapshot were picked, but that is prevented at SNAPSHOT granularity by validateNonAncestor (re-
  picking an applied/published snapshot errors), exactly as Java relies on it — Java's
  `MergingSnapshotProducer.add()` likewise has no per-file live-dup guard on the cherry-pick add path. The
  dynamic-overwrite re-delete routes through the producer's `process_deletes`, which fails loud if a
  removed path is no longer live (Java `failMissingDeletePaths`, L117) — Java-faithful.
- **WAP + property names (point 6): all four summary strings match Java `SnapshotSummary` exactly**
  (`wap.id` / `published-wap-id` / `source-snapshot-id` / `replace-partitions`). `validate_wap_publish`
  mirrors `WapUtil.validateWapPublish` + `isWapIdPublished` (walks current ancestry, rejects if any ancestor
  carries the id as STAGED or PUBLISHED). Both APPEND and dynamic-OVERWRITE replays set `published-wap-id`
  (one shared call site), matching Java setting it in both branches. Publish-once is pinned by
  `test_cherry_pick_rejects_already_published_wap_id`.
- **DEFERRED, correctly scoped (flagged not fixed):** (a) `validateReplacedPartitions` — the dynamic-
  overwrite concurrent-change partition scan (Java L218-252) — needs the conflict-validation-history replay;
  the increment ships the parent-is-ancestor precondition (L101-105) but not the between-snapshots scan.
  (b) The second commit-time `WapUtil.validateWapPublish(base)` re-check (Java validate() L169) against a
  concurrently-refreshed base is folded into the single commit-time call (no concurrent re-validation hook
  wired for cherry-pick yet — same sub-sequence as `validateReplacedPartitions`). (c) Data-level Java
  interop. All three are the documented gate to ✅; row stays 🟡. None is a correctness bug in the
  single-writer surface the increment claims.

### 2026-06-08 (Increment 8 / 5c — V3 deletion-vector writer, BUILDER Opus)
- **DO read BOTH `BitmapPositionDeleteIndex.java` AND `RoaringPositionBitmap.java` before serializing a DV
  blob — the byte layout spans two classes and mixes endianness.** *Why:* the DV blob content (Java
  `BitmapPositionDeleteIndex.serialize`) is `[bitmap-data-length: 4 BIG-endian]` + `[MAGIC_NUMBER 1681511377:
  4 LITTLE-endian]` + `[roaring treemap]` + `[CRC-32: 4 BIG-endian]`. The mixed endianness is a TRAP: the
  outer `ByteBuffer` is left big-endian (so length + CRC are BE via `putInt`), but the `bitmapData` SLICE is
  explicitly `.order(LITTLE_ENDIAN)` (so the magic + treemap are LE). `bitmap-data-length == 4 (magic) +
  treemap_len` and the CRC-32 covers `magic + treemap` (offset 4, length `bitmap-data-length`), NOT the
  length field and NOT the CRC itself. The treemap portable format lives in the SECOND class
  (`RoaringPositionBitmap.serialize`): 8-byte LE count + per-bitmap `[4-byte LE key + standard 32-bit
  RoaringBitmap]`. A digest paraphrase that says only "portable roaring, little-endian" hides the
  length/CRC endianness; read the source.
- **DO trust `roaring::RoaringTreemap::serialize_into` (roaring 0.11) as a byte-exact match for Java
  `RoaringPositionBitmap.serialize` — but VERIFY by reading the crate's `treemap/serialization.rs`, don't
  assume.** *Why:* confirmed `serialize_into` writes `write_u64::<LittleEndian>(map.len())` then per `(key,
  bitmap)` `write_u32::<LittleEndian>(key)` + `bitmap.serialize_into` — identical to Java's `putLong(len)` +
  per-key `putInt(key)` + `bitmaps[key].serialize(buffer)`. The crate doc explicitly claims C/C++/Java/Go
  compatibility. The `BTreeMap` key order (ascending unsigned) matches Java's ascending-key requirement.
  **Divergence (benign, flagged):** Java calls `bitmap.runOptimize()` (RLE) before serializing; the Rust
  crate does not. Both are valid portable encodings BOTH readers accept (RLE is a space optimization, not a
  correctness/format requirement), so the bytes are non-identical but mutually readable. A Java-interop
  fixture round-trip (deferred) would confirm.
- **DO implement CRC-32 inline rather than add a `crc`/`crc32fast` dep — the STOP condition is "add a
  dependency," not "compute a CRC."** *Why:* `crc32fast` is only a TRANSITIVE dep (via `flate2`), so `use
  crc32fast` would require adding it to `crates/iceberg/Cargo.toml` (forbidden Cargo edit). Java's
  `java.util.zip.CRC32` is the standard IEEE 802.3 / zlib CRC-32 (reflected, poly `0xEDB88320`, init/final
  `0xFFFFFFFF`). A 12-line bit-by-bit reflected implementation is boring, auditable, and pinned by the
  canonical check value `crc32(b"123456789") == 0xCBF43926` (plus `crc32(b"") == 0` and `crc32(b"a") ==
  0xE8B7BE43`). Don't reach for a table-driven version — the input is a few hundred bytes; clarity wins.
- **DO surface the Puffin blob's `(offset, length)` from `PuffinWriter::add` — they ARE the DV `DataFile`'s
  `content_offset`/`content_size_in_bytes`, and the writer was discarding them.** *Why:* the DV spec
  REQUIRES `content_offset`/`content_size_in_bytes` to exactly equal the blob's offset/length in the Puffin
  footer (readers seek the blob by them); Java `BaseDVFileWriter.createDV` reads `blobMetadata.offset()` /
  `.length()` from the `BlobMetadata` that `PuffinWriter.write(blob)` RETURNS. The Rust `PuffinWriter::add`
  computed both into the `BlobMetadata` it pushed but returned `()` — there was NO way to recover them. The
  genuinely-missing helper is to return the `BlobMetadata` (and have `close` return the total file size for
  `file_size_in_bytes`, since `close` consumes the writer). Both are additive: existing `add(...).await?` /
  `close().await?` call sites that ignore the value still compile.
- **DO pin "the offset/size INDEX the blob" by slicing the raw Puffin file at exactly `content_offset
  ..content_offset+content_size` and deserializing — not merely by checking a blob exists.** *Why:* the
  interop-critical failure is an offset/size that is off by even one byte (or points at the footer): a blob
  "exists" but a reader seeking by the recorded bytes gets garbage. The crown-jewel test reads the whole
  Puffin file, slices `[content_offset, content_offset+content_size)`, and `DeleteVector::deserialize`s it
  back to the exact positions — AND cross-checks that slice against the Puffin footer's `BlobMetadata.offset/
  length` and against the high-level `PuffinReader::blob` bytes. Three independent paths agreeing is the
  proof; a "blob count == 1" assertion is not.
- **DEFERRED, correctly scoped (flagged not fixed):** (a) wiring the DV writer into RowDelta-for-V3
  end-to-end — a V3 DV is a `DeleteFile` (content=PositionDeletes), so it composes through the existing
  `RowDelta` action; the V3 RowDelta scan-application e2e is a follow-up. (b) `BaseDVFileWriter` multi-data-
  file batching (one Puffin file carrying DVs for many data files) — this writer is one-DV-per-file. (c) A
  Java interop round-trip (the byte-level + RLE-divergence confirmation that flips the row → ✅). (d) Wiring
  `DeleteVector::deserialize` into `caching_delete_file_loader`'s `// TODO: Delete Vector loader from Puffin
  files` (the read path) — the parser now exists for it.

### 2026-06-08 (Increment 8 / 5c — V3 deletion-vector writer, REVIEWER Opus)
- **DO verify a "portable roaring == Java" CRATE-BEHAVIOR claim with an out-of-crate byte probe, not by
  trusting the crate's doc string.** *Why:* the whole DV interop rests on `roaring 0.11.3`'s
  `RoaringTreemap::serialize_into` matching Java `RoaringPositionBitmap.serialize` byte-for-byte. The crate
  DOES self-document "compatible with the official C/C++, Java and Go implementations," and reading
  `treemap/serialization.rs` shows `write_u64::<LittleEndian>(map.len())` then per-key
  `write_u32::<LittleEndian>(key)` + inner `bitmap.serialize_into` over a `BTreeMap<u32,_>` (so keys ascend).
  But I confirmed it EMPIRICALLY with a throwaway crate depending only on `roaring`: positions `{1, 2, 2^32+5}`
  serialize to count=2 (8-byte LE), key[0]=0 (4-byte LE), inner cookie `0x303A`=12346
  (`SERIAL_COOKIE_NO_RUNCONTAINER`, the standard format); keys emit ascending even when inserted `3,0,1`.
  Exactly Java's layout. A doc string is a claim; the bytes are the proof.
- **DO confirm BOTH directions of the runOptimize/RLE divergence with a probe before calling it "benign."**
  *Why:* Java runs `runLengthEncode()` before serializing; Rust does not. The dangerous direction is
  Rust-writes→Java-reads: Rust emits the `NO_RUNCONTAINER` cookie (12346), the baseline standard format every
  compliant RoaringBitmap reader (incl. Java) must handle — so Java reads our non-RLE DV. The reverse
  (Java-writes-RLE→Rust-reads) needs Rust's reader to accept run containers (cookie 12347 `SERIAL_COOKIE`):
  PROVED by serializing an RLE-optimized bitmap (lo16 cookie = 12347) and round-tripping it through both
  `RoaringBitmap::deserialize_from` AND a hand-framed `RoaringTreemap::deserialize_from` (len preserved). RLE
  is a per-container reader-agnostic flag; mutual readability holds both ways. Benign — verified, not asserted.
- **DO cross-check a hand-rolled CRC-32 against a SECOND independent implementation, not just the KATs.** *Why:*
  the DV CRC is hand-implemented (couldn't add `crc32fast` — it is only a TRANSITIVE dep in `Cargo.lock`, and a
  direct dep is a forbidden Cargo edit). The three canonical vectors (`123456789`→0xCBF43926, `""`→0, `a`→
  0xE8B7BE43) pass, AND a 256-entry table-driven CRC-32 (the classic zlib approach) agrees with the
  under-review bit-by-bit version on a non-trivial binary payload (0x1BAECBB1). Two independent algorithms
  agreeing on arbitrary bytes is far stronger evidence than three fixed strings.
- **DO map every DV `DataFile`/blob field to Java `BaseDVFileWriter.createDV`+`toBlob` by line.** All match:
  `createDV` → content=PositionDeletes, format=PUFFIN, path=puffin path, size=`writer.fileSize()`,
  referencedDataFile, contentOffset=`blobMetadata.offset()`, contentSize=`blobMetadata.length()`,
  recordCount=`cardinality`; `toBlob` → type=`DV_V1` (="deletion-vector-v1"), fields=`[ROW_POSITION.fieldId()]`
  (=Integer.MAX_VALUE-2 = Rust `RESERVED_FIELD_ID_POS` = i32::MAX-2), snapshot/seq = -1 (inherited),
  uncompressed, properties {referenced-data-file, cardinality}. The Puffin `add()` captures
  `offset=num_bytes_written` BEFORE writing + `length=compressed.len()` and pushes the SAME `BlobMetadata` into
  the footer, so the returned offset/length EQUAL the footer's — the crown-jewel test slices `[offset,
  offset+size)` from the raw Puffin file and deserializes back to the input positions. `close()` returns the
  footer-inclusive `num_bytes_written` = full file size.
- **DO confirm the Puffin `add`/`close` signature change broke no caller by running the bit-identical-to-Java
  Puffin tests.** *Why:* `add` now returns `BlobMetadata` (was `()`) and `close` returns `u64` (was `()`); the
  only callers are the new DV writer (consumes them) and the test helper (uses `?`, discards). The 36 puffin
  module tests — including `test_uncompressed_metric_data_is_bit_identical_to_java_generated_file` — stay green,
  proving the wire output is unperturbed; a signature change that touched the byte path would fail those.
- **NOTE (pre-existing, not in this diff): the `delete_vector.rs` doc comment "our Cargo.toml temporarily uses
  a git reference for the roaring dependency" is STALE** — `roaring = "0.11"` resolves to `0.11.3` from the
  crates.io REGISTRY (no git ref in Cargo.toml/lock). The comment predates this increment (not introduced by
  the diff) and is about `BitmapIter`/`advance_to`, not the DV format. Left untouched (out of this change's
  scope); flagged for a future cleanup pass.

### 2026-06-08 (Phase 2 Increment 9 — merge append, BUILDER Opus)
- **DO make a producer post-processing trait async via RPITIT (`-> impl Future<Output = Result<...>> + Send`),
  matching the SIBLING trait already in the file (`SnapshotProduceOperation`), NOT `#[async_trait]`.** *Why:*
  the existing `SnapshotProduceOperation` methods are `impl Future + Send` (a manual RPITIT, not the macro), so
  the `ManifestProcess` async change must follow the same convention for consistency and to keep the producer
  generic (`commit<OP, MP>`) monomorphized without boxing. An `async fn` in the trait works for the IMPLs
  (Rust 2024 desugars it), but the TRAIT METHOD must spell `impl Future + Send + 'a` with an explicit
  lifetime tying the borrow of `&'a mut SnapshotProducer` to the returned future — otherwise the `MP:
  ManifestProcess` bound on `commit` won't prove `Send` and the future captures get tangled. The default impl
  (`DefaultManifestProcess`) can still be a plain `async fn` body.
- **DO take `&mut SnapshotProducer` (not `&`) in the manifest post-processor when a future impl writes
  manifests.** *Why:* the original sync `process_manifests` took `&SnapshotProducer` because the pass-through
  needs nothing; a MERGING impl must call `new_filtering_manifest_writer` (advances the `manifest_counter` +
  builds a writer) and `load_manifest`, which need `&mut self`. The `manifest_file()` call site already holds
  `&mut self`, so widening the trait's receiver to `&mut` is free there. The whole merge body lives as METHODS
  on `SnapshotProducer` (`merge_manifests`/`merge_group`/`create_merged_manifest`) — the process struct just
  forwards — so they reuse `new_filtering_manifest_writer` (the same source-spec writer the delete-rewrite uses)
  unchanged.
- **DO identify "the new manifest" in the merge by `added_snapshot_id == self.snapshot_id`, NOT by list
  position.** *Why:* Java `ManifestMergeManager.mergeManifests` takes `first = manifestIter.next()` and gates
  the threshold on `bin.contains(first)`, where Java's `unmergedManifests = concat(prepareNewDataManifests(),
  filtered)` puts the NEW manifest FIRST. The Rust seam assembles the candidate list in the OPPOSITE order —
  existing manifests first (from `process_deletes`), the new added-data manifest PUSHED LAST. So "is this the
  new manifest?" must be a property test (`m.added_snapshot_id == self.snapshot_id`), not `bin[0]`/`first()`.
  A position-based port would gate the threshold on the wrong manifest and merge (or refuse to merge) the wrong
  bin.
- **DO copy a merged entry by status with `entry.snapshot_id() == Some(self.snapshot_id)` deciding Added-vs-
  Existing, mirroring Java `createManifest` line-for-line.** *Why:* Java's three arms are: `DELETED` → carry
  forward ONLY if `entry.snapshotId() == snapshotId()` (suppress prior-snapshot tombstones — informational,
  already counted); `ADDED && entry.snapshotId() == snapshotId()` → `writer.add` (stays Added — this snapshot's
  new files); else → `writer.existing` (every prior entry becomes Existing, preserving its snapshot id + BOTH
  sequence numbers). The Rust `add_existing_entry`/`add_delete_entry`/`add_entry` map exactly. The provenance
  preservation is the data-integrity contract: re-stamping an Existing entry with the merge snapshot's id/seq
  silently breaks merge-on-read delete application + incremental scans. Mutation-verified: swapping the Existing
  arm to `add_entry` (re-stamp) fails the provenance test; the path-set/count tests all PASS under the re-stamp,
  so ONLY a per-entry `(snapshot_id, sequence_number, file_sequence_number)` assertion catches it.
- **DO reproduce Java `BinPacking.ListPacker(target, lookback=1, largestBinFirst=false).packEnd` as a simple
  end-anchored greedy pack, not the full lookback machinery.** *Why:* a lookback of 1 collapses the
  `PackingIterable` to a sequential greedy pack (open one bin, keep adding while `binWeight + weight <= target`,
  else close it and open the next); `packEnd` just packs the REVERSED list and reverses the result so the
  under-filled bin lands FIRST (Java does this so the under-filled bin is merged next time, keeping file order
  stable as data ages off). The faithful Rust port is: iterate `manifests.rev()`, greedy-pack, then reverse
  each bin AND the bin list. An empty bin ALWAYS accepts the next item (a manifest larger than the target lands
  alone) — mirror Java's `Bin.canAdd` only gating a NON-empty bin. Use `saturating_add` on the `u64` weight to
  be safe against a pathological `manifest_length`.
- **DO group merge candidates by `partition_spec_id` and NEVER merge across specs — the merged manifest is
  written with ONE spec, so mixing specs mis-types the copied partition tuples.** *Why:* Java `groupBySpec`
  partitions the manifests by `partitionSpecId` and merges each group with `spec(specId)`. In Rust the merged
  writer comes from `new_filtering_manifest_writer(representative)`, which uses the source manifest's spec; if a
  bin held two specs, the writer's `check_data_file` (partition-type match) would reject the wrong-spec entries.
  Mutation-verified: grouping by a constant 0 (ignoring `partition_spec_id`) makes the cross-spec test PANIC at
  write time (`(x)` vs `(x,y)` partition shape mismatch) — the test evolves the spec to id 1 and asserts the
  spec-0 manifest survives as its own manifest + every manifest is single-spec.
- **DO duplicate the tiny `Append` `SnapshotProduceOperation` into `merge_append.rs` rather than widen
  `FastAppendOperation`'s visibility.** *Why:* the merge action's operation is byte-identical to fast-append's
  (Append, no deletes, carry forward every live manifest) but the merging behavior lives ENTIRELY in the
  `MergeManifestProcess` post-processor, not the operation. Making `append.rs::FastAppendOperation` `pub(crate)`
  to share it would be a visibility change NOT forced by the async-seam edit (the increment's scope rule allows
  touching `append.rs` ONLY if the seam forces a trivial signature change — it doesn't, since `FastAppendAction`
  already passes `DefaultManifestProcess`). A local `MergeAppendOperation` keeps `append.rs` untouched (Rule of
  Three: this is the 2nd use of the shape, extract on the 3rd).
- **DO add a `live_entry_count` (non-deduplicated) assertion alongside `live_file_paths` (a set) to pin the
  DUPLICATION risk a merge carries.** *Why:* `live_file_paths` is a `HashSet<String>`, so an entry copied TWICE
  into the merged manifest (e.g. a bin overlap bug) would be INVISIBLE to it — the set still equals the appended
  paths. Counting live entries (not de-duplicated) and asserting it equals the appended file count catches the
  double-copy. Data loss is caught by the set; duplication needs the count. Both are required for the
  data-integrity-critical merge.

### 2026-06-08 (Increment 9 — merge append, REVIEWER Opus)
- **VERDICT: merge append is CORRECT — no data loss / duplication / provenance / threshold / cross-spec bug
  found. No production change.** Independently re-ran the FULL mutation matrix against the live tests (each
  mutation caught by exactly the right test, minimal cross-fire): (1) drop one source manifest's entries → the
  KEY no-loss test + the exact-union probe FAIL; (2) copy a manifest twice → ONLY the non-dedup
  `live_entry_count` assertion FAILs (5 vs 4), the `live_file_paths` HashSet stays green — confirming the
  count detector is the load-bearing duplication guard, exactly as the builder designed; (3) re-stamp the
  `Existing` arm to `add_entry` → ONLY the provenance test FAILs (path/count tests all pass under a re-stamp);
  (4) group by a constant spec id → the cross-spec test PANICs at write time (partition-shape mismatch); (5)
  ignore the `merge_enabled` flag → the merge-disabled test FAILs. The `add_existing_entry` writer preserves
  `snapshot_id`/`sequence_number`/`file_sequence_number` (only sets status=Existing); `load_manifest` runs
  `inherit_data` so a carried-forward entry arrives fully provenance-populated. Verified against Java
  `ManifestMergeManager.createManifest` lines 198-216 + `BinPacking.ListPacker.packEnd` (lookback=1 = greedy)
  + `TableProperties` (8MB/100/true) line-for-line.
- **DO close the EXACT-threshold-boundary coverage gap (`bin.size() == min_count`) — the builder's tests only
  covered `4 ≥ 2` (merges) and `2 < 5` (doesn't), never the `==` boundary where the off-by-one lives.** *Why:*
  Java's keep-separate condition is `bin.contains(first) && bin.size() < minCountToMerge` (strict `<`), so
  `size == min_count` MERGES. Added `test_merge_append_at_exact_min_count_boundary_merges` (min-count 3, bin
  size 3 → merges) + `test_merge_append_one_below_min_count_does_not_merge` (min-count 4, bin size 3 → split).
  Both confirm the new-manifest-LAST assembly (vs Java new-FIRST) does NOT shift the decision: the
  `contains_new_manifest` check is `bin.iter().any(added_snapshot_id == self.snapshot_id)` — an
  order-INDEPENDENT membership test — and `bin.len()` is a count, so candidate ordering cannot move the
  threshold. (Ordering only affects which manifests share a bin when total size exceeds the 8MB target and
  bins split — a benign efficiency divergence, never loss/dup.)
- **BENIGN DIVERGENCE (noted, not fixed): group emission order differs from Java.** Java `groupBySpec` uses a
  `TreeMap` with `Comparator.reverseOrder()` (higher spec id first); the Rust `merge_manifests` sorts
  `spec_ids` ASCENDING. This only reorders the manifest-list entries across specs — it changes no entry's
  content, spec, or liveness, and DELETE manifests are appended after. No correctness impact; both are
  deterministic. Not worth diverging the Rust toward Java's reverse order.
