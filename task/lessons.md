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

> **Compaction log.** Last pass: 2026-06-12 (size trigger — 2,292 lines / ~210 KB; pass 3,
> agentic-pace amendment, tally 14 KEEP / 47 ARCHIVE / 3 promoted) →
> [lessons-archive/2026-06_wave3-wave4-overnight.md](lessons-archive/2026-06_wave3-wave4-overnight.md).
> Promoted that pass: 3 rules (2 → [docs/testing.md](../docs/testing.md), 1 →
> `dev/java-interop/map.md#debug`). Prior pass: 2026-06-11 (size trigger; pass 2, 17 KEEP /
> 25 ARCHIVE / 6 promoted) →
> [lessons-archive/2026-06_phase2-completion.md](lessons-archive/2026-06_phase2-completion.md).
> First pass: 2026-06-09 (size trigger; 31 entries' rules promoted) →
> [lessons-archive/2026-06_phase1-phase3.md](lessons-archive/2026-06_phase1-phase3.md).
> Archives are not read by default — see [skills/compaction.md](../skills/compaction.md).

---

<!-- Newest entries at the bottom. Example shape:

### YYYY-MM-DD
- **DO** carry context on every fallible Rust call (`.with_context(...)` / `.expect("msg")`).
  *Why:* a bare `.unwrap()` panic gives the operator no cause from logs alone.
- **DO NOT** edit upstream crate files to land a fork feature when an additive module would do.
  *Why:* it makes the next upstream merge conflict-prone. Prefer additive changes.
-->


### 2026-06-11 (Wave-4 Group O / O1 — REVIEWER Opus, wt-rewrite)
- **The merge_append NO-FIX verdict is CONFIRMED from 1.10.0 bytecode, both sub-questions.** `javap -c
  MergingSnapshotProducer` → `lambda$apply$16` (the `shouldKeep` Predicate) is exactly `hasAddedFiles()
  (offset 1, ifne 35) || hasExistingFiles() (offset 10, ifne 35) || manifest.snapshotId().longValue()
  == this.snapshotId() (offsets 18-32)`. (a) The third clause is UNREACHABLE for a carried manifest in a
  pure merge_append: `filterManifest` returns the manifest VERBATIM (old snapshot id) when there are no
  matching deletes (`!canContainDeletedFiles` → `return manifest`, offsets 23-45) — only a REWRITTEN
  manifest gets the new snapshot id, and merge_append rewrites nothing. So an all-tombstone carried
  manifest fails all three clauses ⇒ dropped, matching Rust's `has_added || has_existing`. (b) `shouldKeep`
  (apply local 8) is applied to BOTH the data-manifest iterable (apply offset 175) AND the delete-manifest
  iterable (offset 191) — so Java drops all-tombstone DELETE manifests in the merge path identically;
  Rust's content-agnostic filter matches. MAIN `MergingSnapshotProducer.java` L1003-1011 confirms the
  bytecode (the builder's "L1007-1011" citation is accurate; the third clause is L1007).
- **Entry-copy fidelity had a TEST GAP — closed by the reviewer.** `to_vec()` clones `ManifestFile`
  verbatim (Java `allManifests` carries the cached objects with no `copyOf`/`withSnapshotId` — that
  restamp is only `lambda$apply$1`, the unported `appendManifest()` path). But the original reproduction
  test only checked the carried entry's PATH + tombstone counts, NOT `added_snapshot_id`. Mutation —
  restamping `added_snapshot_id` to the new snapshot id — left the O1 tests GREEN and only tripped
  cross-cutting concurrent-conflict tests in overwrite/row_delta/replace_partitions/cherry_pick
  (`added_*_files_after` reads `added_snapshot_id`). Reviewer strengthened the reproduction pin to a full
  `ManifestFile` `==` against the pre-carry entry (covers added_snapshot_id, both seq numbers, all counts,
  partition summaries); mutation-verified it now fails directly on the `added_snapshot_id` diff. Lesson:
  a "carried verbatim" pin must field-compare against the SOURCE entry, not just re-assert the entry's own
  derived predicates — re-inheritance corruption hides in fields the predicate doesn't read.
- **Manifest-LIST entry ORDER diverges from Java but is NOT a spec/interop contract (REPORT, not fix).**
  Java `FastAppend.apply` writes NEW manifests FIRST then carried `allManifests` LAST (bytecode offsets
  4-23 then 74-99; MAIN `FastAppend.java` L153 then L166). Rust `SnapshotProducer::manifest_file`
  (snapshot.rs:1030-1039) writes CARRIED first (`process_deletes`) then NEW (`write_added_manifests`
  extend) — the OPPOSITE order. This is PRE-EXISTING (O1 only changed which carried entries survive, not
  the carried-vs-new order) and the canonical interop oracle (`snapshot_meta_view.rs:107-127`) SORTS
  manifests by a deterministic tuple precisely because manifest-list file order is "writer-dependent, not
  a spec contract." Both readers reconcile by sequence number, order-agnostic. So the three interop chains
  pass and the divergence is cosmetic. Fixing it would touch the shared `manifest_file` path (blast radius:
  every action) — out of O1 scope.
- **Comment drift: merge_append.rs:282 now LIES after O1.** It says its `existing_manifest` "Mirrors
  `FastAppendOperation::existing_manifest` exactly so the carried set is byte-identical" — but O1 made
  fast_append carry UNFILTERED while merge_append still filters. The two are now DELIBERATELY different.
  Reported to the orchestrator (merge_append.rs production is outside the reviewer's allowed file set; a
  one-line comment correction is the only follow-up).

### 2026-06-11 (Wave-4 Group O / O2 — `RewriteDataFiles`, REVIEWER Opus)
- **A COMPACTION/REWRITE READ MUST STRIP THE SCAN RESIDUAL (`FileScanTask.predicate`) BEFORE READING, or
  `.filter(Predicate)` SILENTLY DROPS LIVE ROWS.** *Found + fixed in O2.* `scan().with_filter(p)` computes a
  per-file partition-reduced RESIDUAL and stores it on each `FileScanTask.predicate`; `arrow/reader.rs` turns
  that residual into a row-level `RowFilter` (`final_predicate` → `with_row_filter`, reader.rs ~471/521). A
  maintenance action that re-feeds the planned tasks into `ArrowReaderBuilder::read` (to read the group's live
  rows) therefore ALSO applies the filter PER ROW — so a file whose rows only PARTIALLY match the filter has its
  non-matching LIVE rows DISCARDED from the rewritten output ⇒ permanent data loss (row count fell 10→5 in the
  repro). Java does NOT do this: `BinPackRewriteFilePlanner.planFileGroups` builds the plan scan with
  **`.ignoreResiduals()`** (core MAIN ~L291) so tasks carry NO residual, and `SparkBinPackFileRewriteRunner.doRewrite`
  reads the group by SCAN_TASK_SET_ID with NO row filter (reads ALL rows). FIX (the Rust analogue of
  `ignoreResiduals`): set `task.predicate = None` on the group tasks before `read`; keep the delete files (deletes
  still apply) and keep `.with_filter` on the PLANNING scan (file-selection only). RULE: when an action reads a
  pre-planned `FileScanTask` for a purpose OTHER than answering the filter (rewrite/compact/copy), STRIP
  `task.predicate` — the residual belongs to query reads, not data-movement reads. Pin: an e2e with rows
  straddling the filter boundary, asserting the post-rewrite scan still has BOTH sides.
- **The on-disk-seq mechanism pin is NECESSARY but not SUFFICIENT — add the behavioral concurrent-eq-delete e2e.**
  The builder pinned `use_starting_sequence_number` only via raw-avro on-disk seq. The REAL behavior is
  constructible by driving the action's internals: capture starting snapshot S; `write_compacted_files`; commit a
  CONCURRENT equality delete at seq S+1 (deleting a row in the to-be-rewritten files); then commit the rewrite
  stamping S — `ignore_equality_deletes = data_sequence_number.is_some()` makes the commit SUCCEED, and the
  post-rewrite scan shows the row GONE (data at seq S < delete seq S+1). Dropping the seq stamp either resurrects
  the row OR (more often) flips `ignore_equality_deletes` off ⇒ the delete now CONFLICTS ⇒ commit FAILS — either
  way the mutation is caught.
- **`validate_from_snapshot(start)` is REDUNDANT for a single-group first commit but LOAD-BEARING across groups.**
  `RewriteFilesAction.validate` uses `effective_start = validate_from_snapshot.or(starting_snapshot_id)` where the
  fallback is the TRANSACTION-captured base. In `rewrite_group`, `Transaction::new(table)` for the FIRST group has
  base == starting snapshot, so dropping `validate_from_snapshot` is behaviorally identical there (a single-group
  concurrent-delete e2e through `.execute()` does NOT catch the drop). It diverges only on the 2nd+ group (base has
  advanced past the original start). Keep the call (Java-faithful + multi-group-correct) but know a one-group e2e
  can't pin it; the staged conflict tests + the `.execute()` conflict pin cover the single-group path, and the
  bytecode + the `.or()` trace cover the rest.

### 2026-06-11 (Wave-4 F1 — REVIEWER Fable, wt-vschema)
- **A bytecode-derived Avro claim is incomplete without the REGISTRATION story: Java only attaches
  `VariantLogicalType` to PARSED schemas after `org.apache.iceberg.avro.Avro`'s static init runs
  `LogicalTypes.register("variant", ...)`, and Avro's `fromSchemaIgnoreInvalid` then silently DROPS
  the logical type when `validate` (= `isVariantSchema`) fails.** *Consequence:* Java's in-code
  "Invalid variant record: %s" precondition is UNREACHABLE for parsed schemas — live Java reads a
  malformed claimed-variant record as a plain struct (with a logged warning). The Rust loud
  rejection is a deliberate fail-loud divergence (B1 house precedent), now documented at `visit()`.
  A live probe that forgets `Class.forName("org.apache.iceberg.avro.Avro")` measures the
  UNREGISTERED behavior — probe both.
- **`isVariantSchema` is by-NAME (`getField`) ⇒ order-insensitive: live Java (registered) accepts
  `value`-before-`metadata`; Rust's `lookup`-based check matches.** Pinned with a value-first test
  after the swap-to-positional mutation SURVIVED the builder's suite — shape-check order
  insensitivity needs its own pin.
- **The map-value fallback name was a real cross-engine failure, not just a cosmetic divergence:
  two variant-valued maps in one schema emitted two records both named "variant", and Java's
  `Schema.Parser` rejects duplicate definitions ("Can't redefine: variant").** Fixed by renaming
  variant records in `map()` to Java's `r<fieldId>` (live-probed: `r8` for value-id 8); Java now
  reads the fixed output byte-identically to its own shape. PRE-EXISTING sibling bug left in place
  and flagged: a STRUCT map value keeps the `"null"` placeholder name (Java emits `r<id>`), so two
  struct-valued maps still collide — converter-wide naming fix is out of F1 scope.
- **Java's `Types.fromTypeName` lowercases (`toLowerCase(Locale.ROOT)`) — `SchemaParser` accepts
  "Variant"/"VARIANT"/"vArIaNt" (and "STRING") where Rust's serde is lowercase-exact for EVERY
  type name.** Pre-existing whole-parser divergence (read-tolerance of foreign-cased JSON only;
  both writers emit lowercase). Pinned the uniform case-sensitive posture so a future fix flips
  all names at once, never just variant.
- **Read-tolerance verdict (live-probed): Java `TableMetadataParser.fromJson` runs NO
  `checkCompatibility` — V1/V2 metadata with an existing variant column parses fine on both sides,
  and `buildFrom(v2-with-variant)` + unrelated edits or `upgradeFormatVersion(3)` commit without
  re-checking old schemas. The gate is add-schema-only in BOTH languages (creation +
  `TableUpdate::AddSchema` + evolution all funnel there; views ungated in both).** But the
  IDENTITY-TRANSFORM door DOES fire on Java's parse: metadata with `identity(variant)` in a
  partition spec or sort order fails to read ("Unsupported type for identity: variant" —
  `Identity.UNSUPPORTED_TYPES` is NOT redundant; it is the reachable door on bind paths, firing
  BEFORE the non-primitive `checkCompatibility` messages, which are unreachable for variant).
  Rust matches for partition specs (bound on parse → rejected); Rust sort orders are stored
  unbound and read-tolerated — pre-existing posture difference, flagged not fixed.
- **The untagged `SerdeType` arm order is NOT load-bearing — the swap-after-Primitive mutation
  survives because untagged serde tries arms until success and `PrimitiveType` independently
  rejects "variant".** The real guard is the PrimitiveType-rejection pin; the comment now says so
  instead of claiming the position matters.
- **A compile-forced arm unreachable through the public surface still needs a DIRECT unit test:**
  the `include_leaf_field_id` variant arm survived a full-suite mutation (scans die earlier at the
  arrow door), so its leaf semantics are pinned by calling the private fn directly — otherwise the
  arm's behavior is unspecified the day the arrow door opens.

### 2026-06-11 (Wave-4 F2 — variant shredding overlay + VariantVisitor, BUILDER Fable, wt-vschema)
- **1.10.0 `ShreddedObject` DIVERGES from MAIN twice — both found in bytecode and confirmed by live
  probe (`/tmp/variant-probe/ShredProbe.java`), and both are BUGS the port must not mirror:** (1)
  `remove()` lacks MAIN's `this.serializationState = null`, so `sizeInBytes()` → `remove(x)` →
  `writeTo` serializes the STALE cached state with x still present; (2) the `SerializationState`
  ctor's non-`SerializedObject` branch compiles to `aload_3` (the PARAMETER map = the overlay's
  LIVE `shreddedFields`) for the materializing merge while `this.shreddedFields` keeps the
  pre-merge COPY that `writeTo` iterates — the FIRST serialization over a constructed backing
  writes count/dataSize for the merged set but only the copy's fields ⇒ CORRUPT bytes (probe:
  re-read throws IndexOutOfBoundsException); a later state rebuild self-heals because the live map
  was polluted. MAIN fixed both (the param was renamed). The Rust port is STATELESS (plan computed
  fresh per call) and MAIN-consistent; the constructed-backing oracle is Java's SELF-HEALED second
  serialization (puts → sizeInBytes → re-put to reset the cache → serialize). LESSON: a
  source-vs-bytecode diff can hide in PARAMETER NAMING — `javap` shows `aload_<n>` vs `getfield`,
  which no source read reveals; for any stateful Java class, probe the mutate-after-size sequence.
- **The verbatim-slice contract is what makes the overlay safe over third-party data:**
  `SerializedObject.sliceValue(index)` spans are exactly B1's sorted-distinct-offsets field
  ranges, so `parse_object` grew an optional range recorder (one parser, no duplication) and the
  overlay copies untouched fields' ORIGINAL bytes — only ids/offsets/header recompute (ids
  re-resolve BY NAME at write time even for verbatim fields, Java `metadata.id` + "Invalid
  metadata, missing: %s"). The designated mutation (verbatim → canonical re-encode) is killed
  ONLY by the non-canonical fixtures (long-form string that fits short form; oversized offset
  width) — canonical-input fixtures pass under the mutation, so a fixture set without a
  non-canonical backing would NOT pin the contract at all.
- **Java's remove-then-put contract is deliberately inconsistent — mirror BOTH sides:** `put`
  does not clear `removedFields`, so after `remove(x)` + `put(x, v)` the views (`get` → null,
  `numFields`/`fieldNames` exclude x — removedFields filters them) DISAGREE with serialization
  (which includes x=v: the SerializationState's shredded map still carries it). Probe-pinned
  (`probe_remove_then_put_view numFields=2 get_b=null` + bytes containing b=9). Fixing either
  side to be "consistent" diverges from Java.
- **Duplicate backing-field names: the rejection is REPLACEMENT-SENSITIVE, so door at
  serialization time, not construction.** Java's ImmutableMap throws only when the duplicate
  SURVIVES the replaced/removed filter; putting or removing the duplicated name skips both
  occurrences and serializes fine (fixture-pinned both ways). An eager constructor-time door
  would over-reject vs Java. Constructed-backing duplicates collapse SILENTLY instead (HashMap
  put semantics) — two different dup behaviors in one class.
- **`VariantVisitor` facts (1.10.0 == MAIN, bytecode-verified):** drivers are `visit(Variant,
  visitor)` + `visit(VariantValue, visitor)` — the brief's `visit(VariantMetadata, VariantValue,
  VariantVisitor)` signature DOES NOT EXIST in 1.10.0 (brief ≠ spec, again); object traversal
  iterates `fieldNames()` (stored order) but recurses into `object.get(name)` — the NAME LOOKUP —
  so a non-name-sorted object NPEs in Java (Rust: named Err, after-hook still fired); after-hooks
  run in `finally` (pinned by asserting the hook fired on the error path); all defaults return
  null (Rust `Option::None`) and the result lists carry nulls. A Java-generated event log (the
  generator's LoggingVisitor) is a cheap, exact traversal-order oracle — pin the SEQUENCE, not
  properties of it.

### 2026-06-11 (Overnight Group V V1 — `stage_only()` WAP staging path, BUILDER Opus, wt-wap)
- **A staged (WAP) snapshot CONSUMES a sequence number exactly like a normal commit — `apply()` is
  stageOnly-INDEPENDENT (1.10.0 bytecode).** *Why it's load-bearing:* the brief flagged the seq-number
  behavior as a verify-from-bytecode item because the later cherrypick seq semantics depend on it.
  `SnapshotProducer.apply()` (the `Snapshot apply()`) computes `long seq = base.nextSequenceNumber()` at
  offset 18-24 UNCONDITIONALLY and builds `new BaseSnapshot(seq, ...)` — there is NO `stageOnly` branch
  in `apply()`. `stageOnly` ONLY gates the metadata-builder update set in `lambda$commit$2`
  (`stageOnly ? builder.addSnapshot(snapshot) : builder.setBranchSnapshot(snapshot, branch)`). So a staged
  snapshot's seq == `base.next_sequence_number()` (pinned on-disk). The seq is assigned at snapshot BUILD,
  the ref decision at metadata-UPDATE — two separate phases.
- **Java does NOT advance the snapshot-log for a staged snapshot, and the Rust builder ALREADY matches —
  no spec/ change.** *Why:* `TableMetadata.Builder.addSnapshot` (bytecode) touches `snapshots`,
  `snapshotsById`, `lastSequenceNumber`, `lastUpdatedMillis`, the `AddSnapshot` change, and (V3)
  `nextRowId` — but NEVER `snapshotLog`, `currentSnapshotId`, or `refs` (those live in
  `setBranchSnapshotInternal`→`setRef`, only reached by the NON-staged path). Rust's
  `TableMetadataBuilder::add_snapshot` is identical, and `update_snapshot_log()` early-returns when no
  `SetSnapshotRef(main)` is in the change set (`get_intermediate_snapshots` only collects added ids that
  ALSO have a `SetSnapshotRef(main)`). So an `AddSnapshot`-ALONE update leaves snapshot_log /
  current_snapshot_id / refs untouched on disk by construction. The brief's "spec/ only if genuinely
  needed" condition was NOT met — verify the existing builder's add-without-ref path before assuming a
  spec/ edit is required for a staging feature.
- **`stageOnly()` is declared on the `SnapshotUpdate<ThisT>` API interface, so EVERY snapshot-producing
  action exposes it — mirror that by putting the flag on the shared `SnapshotProducer` + a one-line
  `stage_only()` setter per action.** *Why:* `api/SnapshotUpdate.class` has `public abstract ThisT
  stageOnly()` (alongside `set(String,String)`, `deleteWith`, `scanManifestsWith`), and `SnapshotProducer`
  implements it as `iconst_1; putfield stageOnly:Z; return self()`. The Rust analogue: `stage_only: bool`
  on `SnapshotProducer` + `with_stage_only(bool)`, with each action carrying a `stage_only` field and a
  `stage_only()` builder setter threaded in at `commit()`. Wired FastAppend (crown jewel) + DeleteFiles
  (delete-bearing) this increment; the rest are one-line additions.
- **`wap.id` needs NO new `set()` — `set_snapshot_properties(HashMap)` already IS the engine-side
  summary-extension surface.** *Why:* Java's engine sets `wap.id` via `SnapshotUpdate.set(prop, value)`
  → the per-producer `summaryBuilder.set(...)` (`FastAppend` has its own `summaryBuilder` field). The Rust
  `set_snapshot_properties` on every action feeds the producer's `snapshot_properties`, which
  `summary()` merges into the snapshot summary — exactly the same channel. The cherry_pick tests already
  staged `wap.id` this way. A minimal parity `set()` would be redundant with the existing per-action
  surface; documented-not-added.
- **A staged snapshot is EXPIRABLE by the existing ExpireSnapshots with zero changes — it's just an
  unreferenced snapshot.** `unreferenced_snapshots_to_retain` iterates `metadata.snapshots()` (staged
  snapshots ARE in `snapshots`), drops those referenced by a retained ref (a staged snapshot is referenced
  by NONE), and keeps only `timestamp_ms >= cutoff`. So an aged staged snapshot lands in `ids_to_remove`
  — Java-faithful. Pin-only (no code change). The mutation-bait (neuter `if !self.stage_only` → `if true`)
  makes the staging publish to main, so the retention test then finds the snapshot referenced/current and
  the expire pin FAILS — proving the pin is behavioral, not vacuous.

### 2026-06-11 (Overnight Group V V1 — `stage_only()` WAP staging path, REVIEWER Opus, wt-wap)
- **The right oracle for a Rust action's commit REQUIREMENT-set is Java `UpdateRequirements.forUpdateTable(base,
  updates)`, NOT a guess about "what guard is meaningful."** *Why it's load-bearing:* the headline question
  ("what does Java require for an AddSnapshot-only staged commit?") is settled by ONE bytecode fact:
  `UpdateRequirements$Builder.update(MetadataUpdate)` (1.10.0) dispatches on 8 `instanceof` arms —
  `SetSnapshotRef, AddSchema, SetCurrentSchema, AddPartitionSpec, SetDefaultPartitionSpec, SetDefaultSortOrder,
  RemovePartitionSpecs, RemoveSchemas` — and has **NO `AddSnapshot` arm**. `forUpdateTable` seeds
  `AssertTableUUID` then forEach-applies `update`. So `[AddSnapshot]` ⇒ `[AssertTableUUID]` ALONE: no ref
  requirement, no last-sequence-number requirement. The Rust staged set (`UuidMatch` alone) is Java-EXACT. DO
  derive the expected requirement-set from `forUpdateTable` against the literal update list; DO NOT reason from
  "the ref guard is meaningless" (right answer, wrong method — it happens to coincide here but won't always).
- **The Rust transaction retry/rebase machinery MASKS an over-strict `RefSnapshotIdMatch` requirement
  end-to-end — pin the requirement-set at the ActionCommit SOURCE, not via a concurrent-commit behavior test.**
  *Why:* `Transaction::do_commit` refreshes + re-bases + RE-CALLS the action's `commit` against the refreshed
  base on every attempt, so a `RefSnapshotIdMatch{main, current}` is recomputed against the refreshed
  `current_snapshot_id` each time — under any concurrent publish it rebases to the NEW head and passes. So the
  over-strict-requirement mutation (`if !self.stage_only` → `if true` on the REQUIREMENTS block) is INVISIBLE to
  every end-to-end concurrency test (both publish orders still succeed). It only diverges on the REST wire
  protocol (an extra `assert-ref-snapshot-id` Java's `forUpdateTable` never emits). The catching pin asserts the
  ActionCommit's `take_requirements()` == `[UuidMatch]` EXACTLY (mirror the existing `test_fast_append`
  update/requirement-set assertion). This was a real survivor of the builder's on-disk-only tests.
- **`apply()` is stageOnly-independent for BOTH seq AND parent (1.10.0 bytecode), and the Rust action recomputes
  both against the refreshed base on retry — so a staged retry over a moved head is corruption-free.** `apply()`
  off 5-16 reads `latestSnapshot(base, targetBranch)` (the parent) and off 17-24 reads
  `base.nextSequenceNumber()` (the seq) — neither references the `stageOnly` field. The Rust producer reads
  `self.table.metadata().next_sequence_number()` + `.current_snapshot_id()` where `self.table` is the refreshed
  `current_table` passed by `do_commit`. Pin it with a concurrent-publish-then-staged-commit test asserting the
  staged snapshot's seq == refreshed-base next-seq (> the concurrent publish's seq) AND parent == the concurrent
  publish id. The builder's tests only covered the no-concurrency seq case.
- **Reader-invisibility breadth: a staged snapshot is in the `snapshots` metadata-table source but NOT the
  `history` one, and is readable by EXPLICIT id (time-travel) though hidden from the default scan — all
  Java-faithful, all already true in Rust.** `inspect/snapshots.rs` iterates `metadata.snapshots()` (includes
  staged); `inspect/history.rs` iterates `metadata.history()` (the snapshot-log, which a staged commit never
  touches). `TableScanBuilder::build` resolves an explicit `snapshot_id` via `snapshot_by_id` (any snapshot,
  staged included), so `scan().snapshot_id(staged_id)` builds and reads the staged data — matching Java's "a
  staged snapshot is a valid time-travel target, just not the table default."
- **wap.id rides `set_snapshot_properties` and is merged into the summary via `additional_properties.extend(...)`
  — user props OVERRIDE producer keys on collision (Rust), but Java's `ImmutableMap.Builder` would THROW on a
  true metric-key collision.** For `wap.id` (a non-reserved key) there is no collision, so both sides coexist —
  pinned. Note (report-only, OUT OF SCOPE for stage_only): Rust `Summary` has `operation: Operation` +
  `#[serde(flatten)] additional_properties` — a user property literally named `operation` would collide with the
  flattened key on serialize; Java's `SnapshotParser.toJson` explicitly SKIPS a summary-map `operation` entry
  (writes the field once). This is a pre-existing property-channel edge, not a stage_only regression.

### 2026-06-11 (Overnight Group V V2 — WAP-path publish dedup, REVIEWER Opus, wt-wap)
- **An ancestry-scoped dedup/guard has an "escape hatch" by construction — probe the SCOPE, not just the
  positive/negative cases, and PIN it as Java-faithful (or as a divergence) with a fail-before mutation that
  WIDENS the scope.** *Why:* `WapUtil.isWapIdPublished` walks `SnapshotUtil.ancestorIds(meta.currentSnapshot())`
  — the LIVE `main` ancestry only. So a WAP publish that is rolled BACK past (or whose snapshot is orphaned off
  `main` — cherry-pick only ever targets `main`) drops out of the walk and its `wap.id` REOPENS: a second same-id
  publish then succeeds. Java has the identical hole (same `currentSnapshot()` root) — NOT a Rust divergence. The
  discriminating test is fail-before/pass-after against a *scope-widening* mutation (walk ALL snapshots instead
  of the current ancestry → the legitimate rollback-and-redo is spuriously rejected), which is a sharper pin than
  the happy-path success assertion alone. Lesson: when a guard keys off "the current ancestry / live ref," the
  reviewer's job is to construct the off-ancestry case and document whether the resulting hole matches Java.
- **To settle a "is this constant enforced anywhere in core?" question, grep the constant-pool of EVERY core
  class for the inlined STRING VALUE, not the field symbol.** *Why:* a `static final String` is a compile-time
  constant — every consumer inlines its VALUE into ITS OWN constant pool, so the field reference vanishes but the
  literal `write.wap.enabled` would appear in any reader's class. Across all 1212 core 1.10.0 classes the literal
  appears in EXACTLY ONE (`TableProperties`, the definition) ⇒ zero runtime readers ⇒ V3 OUT, conclusively. The
  loop is `for c in $(find . -name '*.class'); do javap -v "$c" | grep -q 'write.wap.enabled' && echo "$c"; done`.
- **Reviewing a "verify-and-fill-coverage, zero-production-change" increment: confirm the byte-identical claim
  FIRST (diff the touched production region against HEAD), then re-derive the cited bytecode from the m2 jars
  independently — do NOT trust the builder's /tmp artifacts.** *Why:* the whole increment's value is the claim
  "Rust already matches Java"; if the re-derivation is borrowed, the review is circular. Re-extracting from
  `~/.m2/.../iceberg-{core,api}-1.10.0.jar` is cheap (`unzip` + `javap -c -p`) and catches an off-by-a-few offset
  citation (builder said `isWapIdPublished` offsets 53-74 / `validate` 8-55; the arms/order were exact, the
  offset ranges loose — semantics matched, citations slightly off, no functional impact).

### 2026-06-12 (W2 — ReplacePartitions + partitioned-RewriteFiles data-level interop, Opus REVIEWER)
- **A "mutation" step that runs the CLEAN artifact and greps for a hard-coded PASS string is a NO-OP —
  it proves nothing and passes on every clean run.** *Why:* the W2 builder's step-15 "S3-class
  mutation" claimed to "reroute fixture E's E_new to the wrong partition," but the code ran the
  ordinary `verify-interop-replace-partitions-data` on the UNMODIFIED Rust table and asserted only that
  the verify printed `partition column pinned — E_new→a, B→b` (a string the oracle emits on a clean
  pass). It never rerouted anything; the comments even said "we cannot re-run Rust with a code patch
  inline … instead we confirm … by verifying that the verify on the existing (CORRECT) rust_table
  passes." This is the SAME class as the W1 no-op sabotage — a verification step whose "failure mode"
  can never fire. FIX: a mutation must change the ARTIFACT UNDER TEST and assert the verify FAILS
  CLOSED. Replaced it with an in-chain mutation that feeds E's verify a genuinely different table
  (fixture F's `rust_table`, behind E's expected ground truth), runs a clean-verify CONTROL first, then
  asserts the verify fails AND that the partition-column pin (3e) specifically fires. RULE: for any
  "mutation / sabotage" step, ask "what artifact does this corrupt, and would the step turn RED if the
  pin were deleted?" — if the answer is "none / no," it is theater.
- **The PURE S3 case (wrong partition, identical {id,data} set) is UNCATCHABLE by editing metadata or
  the data column for an IDENTITY partition — and that is correct Iceberg behavior, not a gap.** *Why:*
  an identity-partition column is materialized from the manifest's PARTITION METADATA on read (the
  constants-map path), not from the parquet data column. The reviewer verified this two ways: (a)
  writing E_new into partition `pk_a` but stamping the data column `category="b"` left BOTH the Rust
  self-scan AND the Java `categoryById` reading `category="a"` (from partition metadata) — verify still
  passed; (b) the only way to actually misroute is a genuine wrong-partition-KEY write (`pk_b`), which
  changes `replace_partitions` semantics so the LIVE ROW SET changes ({10,11,20,30} not {11,40}) and
  BOTH sides fail loud (Rust GEN live-ids assertion; Java 7 failures incl. the explicit
  `partition-column (category) mismatch` line). So the partition pin's real protective surface is a
  wrong-partition-KEY write, and it IS non-vacuous there — pinned by the reviewer's out-of-chain
  mutation and the fixed in-chain step 15.
- **Decode the metadata yourself — the untouched-partition file-path pin and the seq-preservation
  sandwich are both confirmable by reading the manifest entries directly.** *Why:* a throwaway
  package-private Java probe (compiled into the oracle classpath, run via `mvn exec:exec
  -Dexec.args="-cp %classpath …"` since the pom hard-codes `mainClass` and `-Dexec.mainClass` does not
  override it, then DELETED) decoded the Rust-written E + F manifests. E snap-2: A (cat=a) status
  DELETED, B (cat=b) status EXISTING with the IDENTICAL path string across snap-1/snap-2 (the
  untouched-partition pin holds at the byte level — stronger than the oracle's "≥1 EXISTING entry"
  check), E_new ADDED. F snap-3: A DELETED, B (cat=b) EXISTING identical path, A' ADDED carrying
  **dataSeq=1** (preserved, NOT the rewrite snapshot's seq 3), and the equality-delete carrying
  dataSeq=2 with `part=PartitionData{category=a}` (genuinely PARTITION-SCOPED, eqIds=[1]). The seq
  sandwich A'.dataSeq=1 < eqDel.dataSeq=2 is what keeps id=20 deleted — confirmed from the raw entries,
  not just the behavioral scan.
- **`typos .` runs over docs too — a lesson that PASTES the flagged spelling re-introduces the failure.**
  *Why:* the W2 builder's own lesson about a `typos` false-positive pasted the bare offending camelCase
  token four times into `task/lessons.md`, so `typos .` (the FIRST verbatim-gate step) failed on the
  lessons file even though the oracle code was clean — meaning the builder's "verbatim gate ×2 / typos
  clean" claim was FALSE as committed. Reworded the entry to describe the token instead of pasting it;
  `typos .` then clean. RULE: after any typos-related edit, run `typos .` over the WHOLE tree (docs
  included), and never paste the raw flagged spelling into prose.

### 2026-06-11 (W3 — multi-bin merge_append data fixture + multi-spec comparator groundwork, BUILDER Sonnet)
- **DO measure actual manifest sizes at runtime when setting `target-size-bytes` for a multi-bin
  merge test — never guess.** *Why:* manifest avro sizes depend on schema complexity and record counts
  in unpredictable ways. After 4 fast_appends, load the manifest list, find `max_manifest_len =
  entries().iter().map(|m| m.manifest_length).max()`, then set `target_size_bytes = max_manifest_len
  * 2 + 1`. This guarantees `pack_end` sees two manifests fit per bin but not three — regardless of
  machine or avro encoder version.
- **DO place `partition_spec_id` in the emitted manifest JSON (sort-tuple position left open = ESCALATE).** *Why:*
  the field must go into the emitted view BEFORE the sort-tuple position is decided, so the JSON
  output evolves in one atomic change when the Opus reviewer makes the sort decision. Adding to the
  JSON without adding to the sort key is safe — constant 0 for all existing single-spec fixtures
  means the new field is byte-invisible. The sort-position is an Opus-level semantic judgment
  (Option A: position 2 after content_rank; Option B: position 10 as final tiebreaker — see W3
  escalation report). NEVER unilaterally resolve a named escalation question.
- **When a Java helper method needs to be reused by a sibling class in the same package, change
  `private static` to `static` (package-visible) rather than duplicating the code.** *Why:*
  the `MergeAppendDataOracle.writePartitionedDataFile` method was `private static` and needed by
  `MultiBinMergeAppendDataOracle`. Removing `private` (Java package-visibility default) is the
  minimal, safe change; the method stays inside the package and does not become part of any public
  API. Verify `mvn -o -q compile` is clean after the change.
- **The `cargo fmt` auto-fix sometimes reformats multi-line closure filter predicates to a single
  line.** *Why:* a two-line `m.content == ManifestContentType::Data\n    && m.existing_files_count...`
  filter was reformatted to one line by `cargo fmt`. Always run `cargo fmt --all` before the
  `-- --check` gate step to let the formatter fix style issues, then re-run `-- --check` to confirm
  clean. The gate itself (`typos && fmt -- --check && clippy && test`) will fail on any unfixed
  formatting.

### 2026-06-12 (W3 — multi-bin merge_append + comparator groundwork, Opus REVIEWER 2-of-2)
- **TIER-LEDGER / DISCIPLINE IMPROVEMENT: the W3 Sonnet builder correctly ESCALATED the one
  semantic question (the `partition_spec_id` sort-tuple position) instead of guessing it — a clean
  application of the addendum's lowered-escalation rule ("NEVER decide a semantic parity question
  yourself … an escalation is a success of the process, not a failure of yours").** *Why it
  matters:* this is the FIRST W-series increment where the builder split a change cleanly along the
  "I can verify this locally" line — it landed the emitted-field half (byte-invisible, fully
  verifiable: spec_id=0 everywhere, all chains green) and STOPPED at the sort-position half (a
  cross-language-determinism judgment with no local oracle). Contrast W1 (shipped behind a false
  "JVM blocker", chain never run) and W2 (a sabotage step that could never fire). The escalation
  was actionable: it named both options (A: position 2 after content_rank; B: position 10 final
  tiebreaker) with the tradeoff, so the reviewer's job was a RULING + verification, not a redesign.
  Record this as the discipline win it is; the builder's claims-vs-reality below were all TRUE.
- **THE RULING (Option B, implemented): `partition_spec_id` is the FINAL sort tiebreaker (position
  10), symmetrically on all three sides** (Rust `snapshot_meta_view.rs` + `interop_expire.rs`
  10-tuples, Java `SnapshotMetaOracle` `.thenComparingInt(ManifestFile::partitionSpecId)`).
  *Rationale:* the canonical view's sort exists to ERASE writer-dependent manifest-list ordering;
  its only contract is cross-language determinism, which the final-tiebreaker position provides for
  future multi-spec fixtures with zero risk to existing single-spec ordering (spec_id is constant 0,
  so the key is byte-invisible). Verified byte-invisible: all FIVE metadata chains (write-actions,
  rowdelta-meta, expire, dv, cherrypick) stayed green after the change — no pre-existing ordering
  instability surfaced (any diff would have meant the old 9-tuple left an ordering unstable).
- **`commit.manifest.min-count-to-merge` protects ONLY the bin containing the NEW (first) manifest,
  not the carried-existing bins** (Rust `bin_disposition`: `bin_contains_first && bin_len <
  min_count ⇒ Keep`; Java `MergingSnapshotProducer` parity). *Why it bit the review:* a natural
  mutation ("set min-count high so the merge never fires") did NOT fire the fixture-G bin-count
  assert — the carried bins of 2 merged anyway (correct Iceberg behavior). The genuine levers that
  make the multi-bin merge collapse to a no-merge/one-bin shape (and DO fire the `>= 2` assert
  loudly) are: (a) a too-large `target-size-bytes` (all manifests in one bin ⇒ 1 merged manifest)
  and (b) replacing `merge_append` with a plain `fast_append` for G (no merge ⇒ 0 Existing-carry
  manifests). Both proved the assert panics, not skips. When designing a "the guard never fires"
  mutation, confirm WHICH bin the guard actually scopes to first.
- **The dynamic `target-size-bytes = max_manifest_len * 2 + 1` is robust across runs/paths because
  it is DERIVED FROM the measured max, not hardcoded.** A longer tmp path inflates every manifest
  roughly uniformly (they embed the same parquet dir), so `max` grows and the target grows
  proportionally — the "2 fit, 3 don't" invariant (and "4 never fit in one bin") is preserved
  regardless of absolute path length. Verified: max=3863 → target=7727 → 2 bins of 2; the bin-count
  assert is a real `assert!` (fail-loud), proven by forcing target=max*1000 (collapses to 1 merged
  manifest, assert panics "got 1"). No fragility found — the measurement is the right design.

### 2026-06-12 (Group X / X1 — ComputePartitionStats COMPUTE core, BUILDER Opus, wt-pstats)
- **The 1.10.0 JAR's `PartitionStatsHandler` DIVERGES from the /tmp MAIN-source checkout — bytecode
  wins, and the divergence is load-bearing.** *Why:* the jar holds the stats schema as field-id
  constants ON the handler (`PARTITION_FIELD_ID=1 … DV_COUNT=13`) + a concrete `PartitionStats`
  class; the MAIN checkout (crawled 2026-06-06) is POST-1.10.0 — refactored to a `PartitionStatistics`
  INTERFACE + `BasePartitionStatistics`, with an `appendStats` that GUARDS `dv_count` behind a
  V2-backward-compat `targetStats.size() > DV_COUNT_POSITION` check. The 1.10.0 jar's `appendStats`
  adds `dv_count` UNCONDITIONALLY (bytecode: `getfield dvCount; iadd; putfield` with no size guard).
  Porting the MAIN guard would diverge from the pinned oracle. RULE (re-confirmed): `javap -p -c
  -constants` the JAR named by the brief BEFORE reading the MAIN .java — for an actively-refactored
  class the two can differ in class shape AND merge logic.
- **`total_record_count` is NEVER computed in the compute path — it stays NULL (boxed `Long`, Java
  comment "needs scanning the data").** The in-memory `PartitionStats` keeps the count members as
  PRIMITIVES (default 0) and only `totalRecordCount`/`lastUpdatedAt`/`lastUpdatedSnapshotId` as boxed
  nullable `Long`. The Rust row type must mirror that exactly (`i64`/`i32` counters, `Option<i64>` for
  the three nullables) — a "compute total_record_count from data_record_count" shortcut would diverge
  (Java leaves it null even when data records are counted).
- **FULL-compute reads `snapshot.allManifests` over ALL entries (LIVE + DELETED) — a DELETED tombstone
  bumps ONLY last-updated and KEEPS a zero-count row.** Bytecode `computeAndWriteStatsFile(Table,long)`
  full branch: `snapshot.allManifests(io)` (the snapshot's whole manifest list, data + delete) →
  `computeStats(.., incremental=false)`; `collectStatsForManifest` iterates `reader.entries()` (NOT
  `liveEntries`) and routes `isLive()` → `liveEntry`, else → `deletedEntry` (last-updated only). This
  is the OPPOSITE of `inspect::partitions` (which `continue`s on non-alive) — reusing that loop
  verbatim would DROP fully-deleted partition rows (Java `testCopyOnWriteDelete` keeps them at
  dataRecordCount==0). The mutation (add `if !is_alive { continue }`) is caught ONLY by a
  fully-deleted-partition fixture (append → delete-the-only-file → the tombstone's row must survive).
- **A same-order coercion fixture MASKS the "drop the spec-coercion" mutation — the field-id REMAP
  needs a REVERSED-spec-order pin.** *Why:* `coercePartition` must map each unified field BY FIELD ID
  to the file's per-spec tuple POSITION. When the spec's partition fields are already in ascending-id
  order (the common case — `identity(x)` then `identity(y)`), the spec position == the unified index,
  so a mutation that indexes by the UNIFIED position instead of remapping passes every same-order test
  AND the null-fill guard (`index < file_values.len()`) catches the missing trailing field. The ONLY
  distinguishing fixture is a spec whose partition tuple is in a DIFFERENT positional order than the
  unified ascending-by-id order (`[y@1001, x@1000]` with unified `{x@1000, y@1001}` → coerced must be
  `(x,y)` not `(y,x)`). Same family as the multi-spec "same-arity different-name" masking lesson: when
  a fix routes a per-element index/id, pin it with elements whose POSITION differs from their ID order.
- **The Rust core has NO cross-spec partition-type unifier — `inspect::partitions` documents this and
  keys by the file's OWN struct against `default_partition_type()`.** So `Partitioning.partitionType`
  (one struct field per unique partition field id across all specs, deduped, newest-spec name wins,
  sorted ascending) + `PartitionUtil.coercePartition` (`StructProjection.createAllowMissing`) had to be
  PORTED, not reused (both `inspect/` + `spec/` are READ-ONLY this increment). Built entirely from the
  public `PartitionSpec`/`StructType`/`Struct` accessors — zero visibility change needed. Reused only
  the manifest-iteration loop SHAPE + the partition-tuple COMPARATOR pattern from `inspect::partitions`
  (re-authored locally since the module is READ-ONLY).

### 2026-06-12 (Group X / X1 — ComputePartitionStats, REVIEWER Opus, wt-pstats)
- **`PartitionStats.liveEntry`'s PUFFIN test is the DV-routing oracle — a deletion vector is a PUFFIN
  position delete, and it routes to `dv_count` NOT `position_delete_file_count`, while STILL adding its
  records to `position_delete_record_count`.** *Why (1.10.0 jar bytecode, `liveEntry` POSITION_DELETES
  case, offsets 107-157):* `positionDeleteRecordCount += recordCount` ALWAYS, then
  `if format == PUFFIN → dvCount++ else positionDeleteFileCount++`. So a DV moves TWO cells
  (`dv_count` + `position_delete_record_count`), leaves `position_delete_file_count` at 0. DO pin this
  with a REAL V3 Puffin-DV fixture (V2 commits reject DVs via `validate_delete_file_for_version`; a DV
  `DataFile` needs `file_format=Puffin` + `referenced_data_file` + `content_offset`/`content_size`).
  A unit `live_entry` test alone is necessary but not sufficient — the routing only matters once a real
  DV survives the row-delta commit gate.
- **Last-updated for a CARRIED-FORWARD (EXISTING) manifest entry attributes to the ORIGINAL committer,
  not the compute target.** *Why:* Java `collectStatsForManifest` keys off
  `table.snapshot(entry.snapshotId())` (the entry's OWN id, bytecode offsets 145-161), and a carried
  EXISTING entry keeps its original `snapshot_id` (Rust `ManifestEntry::inherit_data` only fills it from
  the manifest's `added_snapshot_id` when it is `None`). DO probe this directly: append partition A (S1),
  then touch a DIFFERENT partition B (S2) so A's manifest is re-listed as EXISTING — A's `last_updated`
  must stay S1. The crown jewel pins it implicitly (its spec-0 rows stay at S1); an explicit probe
  catches the "key off current_snapshot" headline mutation independently.
- **A row's `spec_id` is the file's OWN spec = the MANIFEST's `partition_spec_id`, never the newest-seen
  spec for that partition.** Java's per-manifest supplier is `new PartitionStats(coercedPartition,
  manifestFile.partitionSpecId())` and `liveEntry` asserts `file.specId() == row.specId`. Across spec
  evolution, the same logical partition value can produce two rows (one per spec) with different
  `spec_id`s. DO pin explicitly (write under spec 0, evolve, write under spec 1 → two rows, spec_id 0
  and 1) — the crown jewel pins it only implicitly.
- **Java keys the partition map by `(specId, RAW file partition)` but stores the COERCED partition in the
  row; the Rust keys by `(spec_id, COERCED partition)` throughout — behaviorally equivalent** because
  coercion under a fixed spec is injective (id-remap + null-fill), so the `specId` prefix + the per-spec
  bijection make the two keyings produce identical groupings/merges. Worth confirming, not a bug.
- **Mutation-sweep a `coercePartition`-style remap with TWO disorder fixtures of different shapes** (a
  2-field reversed `[y,x]` AND a 3-field scramble `[z,x,y]`). The index-by-position mutation must fail
  on BOTH — a single fixture is the "fragile one-pin" the builder flagged. (Same family as the
  earlier "same-arity different-name masking" lesson.)

### 2026-06-12 (Group X increment X2 — partition-stats FILE write + registration + read-back, BUILDER Opus)
- **The field-id stamping that lands on disk is the WRITER's arrow schema, NOT the RecordBatch's column
  metadata.** *Why:* `ArrowWriter::try_new(buf, arrow_schema, ...)` writes columns under the SCHEMA you
  hand it; the batch's own field metadata is irrelevant to the file footer. When I mutation-tested "drop
  the field-id stamping" by stripping metadata in `partition_stats_to_record_batch`, the raw field-id
  test still PASSED — the file was stamped from `write_partition_stats_parquet`'s independent
  `schema_to_arrow_schema(stats_schema)`. Only stripping the WRITER's schema killed the raw pins (4
  tests). DO target the writer's schema for any "is the field id on disk" mutation, and DO derive the
  writer schema from the iceberg `Schema` (not from the batch) so the on-disk field-id contract is
  single-sourced.
- **The full `SetPartitionStatistics` metadata path already existed; the ONLY gap was a transaction
  action — so register at the CATALOG level, not by opening transaction/.** *Why:* `TableUpdate::
  SetPartitionStatistics` + `RemovePartitionStatistics` variants, their `apply` arms, and
  `TableMetadataBuilder::set_partition_statistics` all shipped (catalog/mod.rs + table_metadata_builder.rs).
  The existing `UpdateStatisticsAction` (transaction/) handles ONLY `StatisticsFile`/`SetStatistics` —
  partition statistics have no action, and transaction/ was READ-ONLY this increment. The in-scope path
  is a `TableCommit{updates: [SetPartitionStatistics], requirements: [UuidMatch]}` through
  `Catalog::update_table` — a faithful mirror of Java `updatePartitionStatistics().setPartitionStatistics()
  .commit()` (`UpdateRequirements.forUpdateTable` emits ONLY `AssertTableUUID` for a non-snapshot update,
  bytecode-verified). This was NOT a STOP — check whether the METADATA surface is already public before
  concluding a registration needs a new action.
- **`arrow_struct_to_literal(StructArray::from(batch), struct_type)` is the read-back oracle for any
  positional record decode.** *Why:* Java `recordToPartitionStats` reads `record.get(idx, Class)`
  positionally; the Rust mirror turns the whole RecordBatch into one `StructArray`, decodes it to one
  `Literal::Struct` per row via the existing arrow accessor (maps Arrow columns to the iceberg struct by
  field id), then reads the struct's fields positionally (0 = partition, 1 = spec_id, 2..N = counters).
  Reused the arrow read accessor verbatim — no new decoder. A v2 file (12 cols) and v3 file (13 cols)
  decode against their OWN `stats_schema` struct type, so the positional optional `dv_count` is only read
  when `fields.len() >= 13`.
- **Java's partition-stats LOCATION is `TableMetadata.location()` (the table base), NOT the metadata-file
  directory derived from the current metadata-json path.** *Why:* `BaseMetastoreTableOperations
  .metadataFileLocation(name)` = `write.metadata.path` if set, ELSE `<location()>/metadata/<name>`
  (bytecode const `%s/%s/%s` with `location, "metadata", name`). Deriving the dir by stripping the
  metadata-json filename would diverge whenever `write.metadata.path` is set or the metadata lives
  outside `<location>/metadata`. Build the path from `metadata.location()` + the `write.metadata.path`
  property directly, matching the bytecode.
- **`typos` flags the `m-i-s` hyphen-prefix as a typo (wants `miss`/`mist`)** — avoid hyphenated
  `m-i-s` prefixes (the "map/key/assign" verbs) in comments AND in lessons that quote them; write
  "resolve the wrong column" / "keys wrongly" / "wrongly assigned" instead. Cost two gate re-runs (the
  comment, then the lesson that quoted the offending forms).

## 2026-06-12 — X2 reviewer (partition-stats file write/read-back)

- **DO build logical-Arrow partition columns (Date32 / Timestamp ±tz / Decimal128) by calling the
  shared `pub(crate)` `crate::arrow::create_primitive_array_single_element(arrow_data_type, &prim_lit)`
  per row + `arrow_select::concat::concat`, NOT a hand-rolled native-array match.** *Why:* X2 originally
  refused any partition value beyond boolean/int/long/string (date/timestamp/decimal — the MOST common
  production partition shapes — errored `FeatureUnsupported`). The helper already maps the literal to the
  right Arrow array AND applies the field's exact timezone/precision/scale from the passed `arrow_data_type`
  — so feed it the field's actual Arrow `DataType` (from `schema_to_arrow_schema`) and the built array's
  type matches the on-disk schema field exactly (no manual `+00:00` tz logic, no `StructArray::from` type
  mismatch). The helper is reachable from `maintenance/` via the `pub use value::*` glob without touching
  arrow/ (read-only). RESIDUE after the fix: time/uuid/fixed/binary (Time64 / FixedSizeBinary / LargeBinary
  have no helper arm) — keep erroring loudly, never a corrupt column.
- **DO project the read schema down to the file's ACTUAL columns before `arrow_struct_to_literal` when
  reading a stats file that may be a different format version than the schema.** *Why:* `arrow_struct_to_literal`
  REQUIRES every iceberg schema field be present in the Arrow array (`Field id N not found in struct array`);
  it does NOT null-fill. Java `InternalData.read().project(schema)` null-fills a missing optional, and
  `PartitionStats.set(dvCountIdx, null)` coalesces dv_count→0 (1.10.0 bytecode: `ifnonnull` else `iconst_0`).
  So a V2 file (12 cols) read against the V3 schema (13 fields) ERRORED in Rust until the decode projected
  the struct type to the field ids present in the batch (`PARQUET_FIELD_ID_META_KEY` on each Arrow field),
  preserving schema order — then the existing `partition_stats_from_record` `fields.len() >= 13` tolerance
  leaves dv_count at its `PartitionStats::new` default (0). The reverse (V3 file / V2 schema) already worked
  (extra column ignored). This was a real divergence the builder's `>= 13` check could not reach because the
  arrow decode failed first.
- **DO drive a date/timestamp partition test through a REAL table fixture (identity over a Date column),
  then RAW-reopen the stats file and assert the partition child is a logical `Date32` (not Int32) carrying
  the spec field id.** *Why:* a write-side type mismatch (building an Int32 array for a Date32 schema field)
  would either panic at `StructArray::from` or silently produce the wrong on-disk physical type; asserting
  the raw Arrow child DataType == `Date32` pins the on-disk contract other engines read.

### 2026-06-12 (Wave-5 Group U / U1 — view metadata + view ops + catalog CRUD, BUILDER Opus, wt-views)
- **The view SPEC builder was ALREADY a 1:1 Java port — the V2 "read before assuming unbuilt" lesson
  paid off; the actual gap was the entire CATALOG-facing surface.** `spec/view_metadata_builder.rs`
  (58 KB) is a faithful port of Java `ViewMetadata.Builder`: `reuse_or_create_new_view_version_id` ==
  `reuseOrCreateNewViewVersionId`/`sameViewVersion`, schema interning, version-log append + expiry
  (`version.history.num-entries`, default 10), the dialect-drop rules. What was MISSING and had to be
  built: `ViewMetadata::read_from`/`write_to`, a `View` type, `ViewCommit`/`ViewRequirement`, the
  `replace_version`/`update_properties` ops, and the `Catalog` trait view methods + a catalog impl.
  DO survey the spec layer in full before scoping a "views" increment — most of the hard metadata
  logic may already be ported from upstream.
- **The view REPLACE requirement set is `[AssertViewUUID]` ALONE — bytecode-pinned from
  `UpdateRequirements.forReplaceView`, the V1-lesson view sibling.** 1.10.0 bytecode: `forReplaceView`
  seeds ONE `AssertViewUUID(metadata.uuid())` then forEach-applies `Builder.update`. The `Builder` is
  constructed with `base = null` (offset 34 `aconst_null`), and the `update(AddSchema)` arm only adds
  `AssertLastAssignedFieldId` when `base != null` (offset 11 `ifnull 33`) — so it NO-OPS for views.
  There is NO `AddViewVersion`/`SetCurrentViewVersion` arm at all. So a view replace commit carries
  `[AssertViewUUID]` regardless of the update list. Pin it at the ActionCommit source (`take_requirements`
  == `[UuidMatch]`), not via concurrency — the retry machinery masks over-strict requirements (V1 lesson).
- **`ViewVersionReplace.internalApply` assigns `versionId = max(versionId)+1` then defers REUSE to the
  builder — the op's `+1` is a hint, not the final id.** 1.10.0 bytecode matches the /tmp source exactly:
  `Preconditions.checkState` on representations/schema/defaultNamespace, build version with
  `versionId(maxVersionId+1)` + `timestampMillis(now)` + `putAllSummary(EnvironmentContext.get())`, then
  `ViewMetadata.buildFrom(base).setCurrentVersion(newVersion, schema).build()` — and `setCurrentVersion`
  runs `reuseOrCreateNewViewVersionId`, so committing identical representations REUSES the existing id
  (version count unchanged). The Rust `ReplaceViewVersionAction::to_commit` mirrors this: feed the
  `max+1` candidate through `into_builder().set_current_version(...)` and let the builder dedup.
- **The view JSON wire format DIVERGES from Rust serde in FIELD ORDER but not field SET — round-trip is
  field-set-exact, NOT byte-exact.** Java `ViewMetadataParser.toJson` writes `view-uuid, format-version,
  location, [properties only if non-empty], schemas, current-version-id, versions, version-log`; Java
  `ViewVersionParser` writes `version-id, timestamp-ms, schema-id, summary, [default-catalog only if
  non-null], default-namespace, representations`. Rust's `_serde::ViewMetadataV1` orders
  `format-version` FIRST and `schemas` LAST, and always emits `properties` (Java omits when empty). Both
  parse each other's field set (serde is order-insensitive). DO pin the field SET (parse a Java-ordered
  doc + round-trip), and flag the order/empty-properties divergence — byte-level view interop is a
  next-wave item, not a U1 claim.
- **Mirror the table CRUD idiom EXACTLY for views in MemoryCatalog: a SEPARATE `view_metadata_locations`
  map in `NamespaceState`, so a view and a table of the same name in one namespace do not collide.** Java
  keeps views and tables in distinct catalog spaces; the Rust MemoryCatalog already keyed tables by name
  in `table_metadata_locations`, so views need their own parallel map (+ `insert_new_view`/
  `remove_existing_view`/`commit_view_update`/`get_existing_view_location` + `ViewNotFound`/
  `ViewAlreadyExists` ErrorKinds for Java `NoSuchViewException`/`AlreadyExistsException` parity).

#### U1 REVIEWER corrections (2026-06-12, wt-views) — adversarial pass against 1.10.0 bytecode
- **CORRECTS the "separate map ⇒ do not collide" lesson above: separate maps are right, but Java
  ENFORCES a shared name space across them — a view CANNOT be created/renamed onto a TABLE's name,
  and vice versa.** The builder shipped views+tables coexisting silently (probe: `create_view` over a
  table AND `create_table` over a view both SUCCEEDED). Java 1.10.0 `InMemoryViewOperations.doCommit`
  (offset 85-117) throws `AlreadyExistsException("Table with same name already exists")` when
  `tables.containsKey(ident)`; the view-catalog's table builder throws `"View with same name already
  exists"` (`viewExists` check); `renameView`/`renameTable` both cross-check the OTHER map at the
  destination. FIX (this pass): added a `tables.contains_key`/`views.contains_key` cross-guard to
  `insert_new_table` + `insert_new_view` in `namespace_state.rs` (symmetric — both helpers live in the
  modified file; the table-direction guard only rejects a previously-silent corruption, no legit test
  hits it). DO add the cross-type collision guard to BOTH `insert_new_*` helpers whenever a new
  catalog object class shares the table name space.
- **The in-tree MemoryCatalog `update_view` (and the pre-existing `update_table` it mirrors) has NO
  base-location CAS — a stale concurrent commit silently lands (last-write-win), diverging from Java
  `InMemoryViewOperations.doCommit`'s `views.compute` location-equality check.** Probe: two
  `ReplaceViewVersionAction` commits built from the SAME base, applied sequentially — the second
  (stale) one SUCCEEDED (versions 1→2→3, location 00001→00002) instead of failing. Java's `doCommit`
  lambda throws `CommitFailedException("...because it has been concurrently modified to %s")` when the
  stored location ≠ the expected base location. ROOT CAUSE: `ViewCommit::apply` re-loads the CURRENT
  view inside the lock and the only requirement is `AssertViewUUID` — but the UUID is INVARIANT across
  replaces, so it never detects staleness. This is consistent with the pre-existing `update_table`
  path (same gap), so it is NOT a view-specific regression and was NOT fixed here (fixing only the view
  side would diverge from the table side; belongs in a dedicated optimistic-concurrency-parity
  increment touching both). DO NOT claim "catalog view CRUD at parity" without the concurrency
  dimension — `[AssertViewUUID]` alone is correct for the REST commit protocol (server does the CAS)
  but insufficient for an in-process catalog with no location CAS.
- **VERIFIED interop-readiness (escalation NOT needed): Java `ViewMetadataParser.fromJson` reads
  Rust's emitted wire format TODAY.** 1.10.0 bytecode: all reads are by-key (`JsonUtil.getString`/
  `getInt`/`get` → order-insensitive Jackson), and `properties` is read under `if (node.has(...))`
  (offset 46-67) — so Rust always-emitting `"properties":{}` is tolerated, and field ORDER is
  irrelevant. The reverse holds too: Rust `_serde::ViewMetadataV1.properties` is `Option<..>` +
  `unwrap_or_default()`, so Rust tolerates Java's omit-when-empty. The wire divergence is COSMETIC
  (field order + always-emit-empty-properties), not blocking — byte-level view interop stays a
  next-wave item but bidirectional field-set reads work now. Mutation pinned: dropping the required
  `default-namespace` field makes the parse fail (`ViewVersionV1.default_namespace` is non-Option).

### 2026-06-12 (Wave-5 Group U / U2 — SQL catalog views + REST view shapes, BUILDER Opus, wt-views)
- **The JDBC view storage scheme was ALREADY half-built in the Rust SQL catalog — views reuse the
  EXISTING `iceberg_type` discriminator column with value `'VIEW'`, no schema migration needed.**
  1.10.0 `JdbcUtil` bytecode: `CATALOG_TABLE_VIEW_NAME = "iceberg_tables"`, `RECORD_TYPE =
  "iceberg_type"`, `TABLE_RECORD_TYPE = "TABLE"`, `VIEW_RECORD_TYPE = "VIEW"`. Views live in the SAME
  `iceberg_tables` table; the view SQL constants (`GET_VIEW_SQL`, `LIST_VIEW_SQL`, `RENAME_VIEW_SQL`,
  `DROP_VIEW_SQL`, `V1_DO_COMMIT_VIEW_SQL`, `V1_DO_COMMIT_CREATE_SQL`) are the table-CRUD SQL with
  `AND iceberg_type = 'VIEW'`. KEY POSTURE DIFFERENCE: tables match `iceberg_type = 'TABLE' OR
  iceberg_type IS NULL` (V0-schema backward compat), but views match `iceberg_type = 'VIEW'` EXACTLY —
  `JdbcViewOperations.doCommit` always uses `SchemaVersion.V1` (views are a V1-only feature, no
  NULL-tolerance). The Rust catalog already creates the V1 schema (the `iceberg_type` column exists in
  its `CREATE TABLE`), so it is V1-native — mirror the exact-match posture, no version-flag gating
  needed. DO `javap -p -constants JdbcUtil` BEFORE designing view storage — the whole scheme is
  inlined string constants.
- **Java's JDBC location-CAS is a one-line WHERE-clause addition the Rust `update_table` ALREADY had
  — mirror it verbatim for views, it's genuinely per-catalog (not the shared seam the U1 reviewer
  deferred).** `V1_DO_COMMIT_VIEW_SQL` = `UPDATE iceberg_tables SET metadata_location = ?,
  previous_metadata_location = ? WHERE ... AND iceberg_type = 'VIEW' AND metadata_location = ?` →
  `rows_affected() == 0` is the conflict (Java raises `CommitFailedException "Cannot commit %s:
  metadata location %s has changed from %s"`). The Rust SQL `update_table` already does this for tables;
  `update_view` mirrors it ⇒ a per-catalog CAS that diverges DELIBERATELY from the in-tree MemoryCatalog
  (which has NO CAS — `[AssertViewUUID]` alone, UUID-invariant). This is the right split: the SQL
  catalog's store-level CAS is cheap and in-scope; the MemoryCatalog/shared-seam CAS is the separate
  deferred increment.
- **A reload-then-apply-then-CAS catalog method CANNOT be pinned by a SEQUENTIAL stale-commit test —
  the internal reload auto-rebases over the staleness (the V1 retry-masking lesson, again). Use a
  BARRIER-SYNCHRONIZED concurrent two-task race instead.** My first CAS test (two commits from the same
  in-memory base, applied sequentially) FAILED to conflict: `update_view` reloads the current view
  inside the call, so the "stale" second commit re-applied against the already-advanced store and
  succeeded at version 3. The genuine CAS race is: both tasks `load_view` (observe the SAME base
  metadata_location), a `tokio::sync::Barrier(2)` releases them together, both `apply` + `write` + CAS —
  the store serializes each update statement, the first wins, the second's `WHERE metadata_location =
  <stale>` matches 0 rows ⇒ `CatalogCommitConflicts`. The barrier makes it DETERMINISTIC (ran ×5,
  always exactly one winner). DO reach for a barrier'd concurrent test the moment a CAS sits behind a
  reload.
- **`ViewRepresentations` had a `pub(crate)` tuple constructor but is a `pub` field of the public
  `ViewCreation` — so NO out-of-crate catalog could construct a `ViewCreation` to call `create_view`.**
  This is a real public-API accessibility gap (the whole external-catalog view surface depends on it).
  Fixed with `ViewRepresentations::new(Vec<ViewRepresentation>)` added in `catalog/mod.rs` (the in-scope
  shared-type escape — `catalog/mod.rs` is in the iceberg crate so it can see the `pub(crate)` ctor; it
  also owns `ViewCreation`). DO check that every `pub` field of a public creation/builder struct is
  itself CONSTRUCTABLE from out-of-crate before declaring a trait method usable by external impls.
- **REST view commit reuses Java's `UpdateTableRequest` SHAPE (`identifier`/`requirements`/`updates`)
  but carries VIEW requirements/updates — and the Rust `ViewRequirement`/`ViewUpdate` types already
  serialize with the correct REST tags (`assert-view-uuid`, `set-properties`, etc.), so the wire types
  drop in for free.** 1.10.0 `RESTViewOperations` bytecode: commit calls
  `UpdateRequirements.forReplaceView(metadata, updates)` then `UpdateTableRequest.create(identifier,
  requirements, updates)` — the SAME request class as tables, just fed view-shaped lists. So
  `CommitViewRequest` is a structural twin of `CommitTableRequest` with `Vec<ViewRequirement>` /
  `Vec<ViewUpdate>`. The view request/response field names (`view-version`, `metadata-location`) are
  in `CreateViewRequestParser`/`LoadViewResponseParser` constant pools — `javap -c -constants` them.
- **`typos` flags the plural-`s` form of the SQL `UPDATE` verb (the inlined `U-P-D-A-T` substring) in
  a comment — write "each update statement", never the bare plural.** Same family as the earlier
  `m-i-s`-prefix and camelCase-token typos false-positives, AND the W2/X2 lesson "never paste the
  flagged spelling into prose" (this very entry tripped the gate twice by doing exactly that):
  after any comment mentioning SQL verbs in plural, run `typos .` over the whole tree
  before claiming the gate is clean.

### 2026-06-12 (Wave-5 Group U / U2 — SQL catalog views + REST view shapes, REVIEWER Opus, wt-views)
- **JDBC verbatim-ness verdict: CONFIRMED from `JdbcUtil` bytecode (`javap -p -c -constants`). The Rust
  SQL queries are SEMANTICALLY verbatim — the conjunct SET is identical; only AND-clause/SET-column
  ORDER differs, which is commutative-safe.** Re-derived all six view constants and diffed: the CAS
  `V1_DO_COMMIT_VIEW_SQL` carries `AND metadata_location = ?` (present in Rust); `iceberg_type = 'VIEW'`
  is EXACT-match for views vs `(iceberg_type = 'TABLE' OR iceberg_type IS NULL)` for tables (V0
  backcompat) — Rust mirrors both postures. The Rust create-view INSERT omits `previous_metadata_location`
  (Java inserts it `=null`), but that matches the EXISTING Rust `create_table` INSERT (the verified
  table port) — the Rust catalog relies on the column's NULL default, not a divergence. Collision
  messages are byte-verbatim against the inlined Java strings: `"Table with same name already exists:
  %s"` (JdbcViewOperations.createView off 93, fires on `tableExists`), `"View with same name already
  exists: %s"` (ViewAwareTableBuilder off 31), `"Cannot commit %s: metadata location %s has changed
  from %s"` (JdbcViewOperations off 41 — Rust truncates the `from %s` tail, keeps the load-bearing
  prefix). DO `javap -c -constants org/apache/iceberg/jdbc/JdbcUtil.class` and diff conjunct-SETS, not
  string-equality — the clause order is a red herring.
- **CROSS-CATALOG error-kind consistency is the real interop contract for collisions, and it HOLDS:
  MemoryCatalog (U1) and SqlCatalog (U2) both reject view-over-table with `TableAlreadyExists` and
  table-over-view with `ViewAlreadyExists`.** A consumer matching on `ErrorKind` must not care which
  catalog raised it. DO grep both catalogs' collision-error helpers and assert the kind pairs line up
  — message wording may differ (it does: Memory says "Cannot create view ... Table with same name
  already exists"; SQL says "Table with same name already exists: <ident>") but the KIND is the
  contract.
- **The CAS race test is NON-VACUOUS — mutation-proven.** Dropping `AND metadata_location = ?` from
  `update_view` makes the barrier'd race test fail ×5 with "both concurrent commits succeeded — the
  location-CAS did not fire" (both win = the loser silently overwrites the winner's metadata pointer =
  corruption). The loser's error is `CatalogCommitConflicts` + `.with_retryable(true)` — EXACTLY the
  table-side `update_table` convention (same kind, same retryable flag). Sequential-staleness
  auto-rebases correctly: `update_view` applies the commit to the FRESHLY-reloaded `load_view` base
  (not a caller-held stale view), and `ViewCommit::apply` recomputes `new_metadata_location` via
  `with_next_version()` off the fresh `current_metadata_location` — same structure as `update_table`.
  There is NO internal retry loop (single CAS attempt), also matching the table side.
- **REST mockito tests were BODY-BLIND — tightened the commit test with a hand-pinned body matcher.**
  None of the 7 view route-tests used `.match_body`, so a regression sending TABLE requirements/updates
  through the view commit (or a wrong wire key) would pass route+status checks. Added a
  `mockito::Matcher::AllOf` pinning `identifier` / `assert-view-uuid` / the uuid / `set-properties` /
  `comment:daily` on `test_update_view_maps_conflict_to_retryable`; mutation-verified (swap
  `assert-view-uuid`→`assert-table-uuid` ⇒ body misses the mock ⇒ the 409 conflict assertion fails).
  REST commit body shape re-derived from `RESTViewOperations` bytecode: `UpdateRequirements.forReplaceView`
  → `UpdateTableRequest.create(identifier, requirements, updates)` → `RESTClient.post(this.path,...)`
  where `path` = `V1_VIEW` (`.../views/{view}`). Routes confirmed verbatim from `ResourcePaths`
  constant pool. The `types.rs` serde round-trips ARE exact-JSON-pinned (`assert_eq!(reserialized,
  json)`), so the wire SHAPE was already body-pinned at the serde layer; only the catalog mockito layer
  was route-only.
- **REST view-commit 5xx → `CommitStateUnknown` was MISSING — added (Java `ViewCommitErrorHandler`
  lookupswitch: 404→NoSuchView, 409→CommitFailed, 500/502/503/504→CommitStateUnknownException).** The
  view `update_view` folded all 5xx into the generic default arm, diverging from BOTH Java AND the
  table-side `update_table` (which already maps 500/502/504→"commit state is unknown", `Unexpected`,
  NON-retryable). Added the four 5xx arms to `update_view` + a fail-before/pass-after test (503 ⇒
  `Unexpected` + "commit state is unknown" + NOT retryable). DO bytecode the matching error handler's
  lookupswitch when reviewing a REST commit method — a missing 5xx arm is invisible to a happy-path +
  conflict test.
- **REPORTED, not fixed (benign, unreachable): the create-view collision-check ORDER is reversed vs
  Java.** Java `createView` checks `tableExists` (→TableAlreadyExists) BEFORE `viewExists`
  (→ViewAlreadyExists); Rust checks `view_exists` first. This only changes the outcome when BOTH a
  table AND a view of the same name exist — a state the shared-namespace guards make unconstructible —
  so when at most one exists the order is irrelevant. Cosmetic divergence; not worth a behavior-changing
  fix.
- **Added two permanent pins the builder's suite lacked: (1) `rename_view` rejecting a TABLE source
  (the `iceberg_type='VIEW'` discriminator on rename was unpinned — mutation: drop the `view_exists(src)`
  guard + the WHERE filter ⇒ a table gets silently relocated ⇒ the new test fails); (2) a compile-level
  accessibility probe (`test_public_view_api_is_fully_constructible_out_of_crate`) in the SQL crate that
  builds the WHOLE public view surface — `ViewRepresentations::new`, `SqlViewRepresentation`,
  `ViewCreation::builder`, `ViewVersion::builder`, the `View` ops, and references `ViewUpdate`/
  `ViewRequirement` — purely out-of-crate, so any regression to crate-private STOPS THE BUILD forever
  (mutation: remove `ViewRepresentations::new` ⇒ the SQL test target fails E0599).** The probe couldn't
  name `uuid::Uuid` (not a SQL-crate dep) — used `ViewRequirement::NotExist` (unit variant) + pattern
  matches instead. Result: SQL 62→64, REST 51→52, iceberg 2188 unchanged; gate ×2 green.
### 2026-06-12 (Z1 — staged-WAP interop fixture, BUILDER Sonnet, wt-interop3)

- **The WAP-dedup `testDuplicateCherrypick` pattern REQUIRES both staged snapshots to share the SAME
  parent (S0) — if the second staged snapshot is committed AFTER the first cherry-pick, the second
  snapshot's parent becomes the current head (= w3-first after FF), the second cherry-pick FAST-FORWARDS
  (parent == head), and `validate_wap_publish` is NEVER reached.** *Why:* `CherryPickAction.validate`
  early-returns `Ok(())` for the fast-forward plan (Java `if (!isFastForward(base))` gates the whole
  validate block). The ONLY way to force the REPLAY path (which runs `validate_wap_publish`) is to ensure
  the second staged snapshot's parent IS NOT the current head — i.e., the first cherry-pick must have
  advanced `main` past S0 BEFORE the second staged snapshot inherits S0 as parent. Concretely: stage w3-first
  off S0, stage w3-second ALSO off S0 (still head, no cherry-pick yet), THEN cherry-pick w3-first (FF →
  main = w3-first), THEN cherry-pick w3-second (parent=S0 ≠ w3-first=head → REPLAY → dedup fires). Any
  design that commits the second staged snapshot AFTER the first cherry-pick stages it off the NEW head and
  silently skips the dedup check. Verified against 1.10.0 bytecode of `CherryPickOperation.apply` and
  `TestWapWorkflow.testDuplicateCherrypick` in the m2 jar — this is the exact pattern Java's own test uses.

- **The canonical `SnapshotMetaOracle.emit()` view does NOT include `current-snapshot-id` or `refs` — it
  covers ONLY `metadata.snapshots()` ordinals, sequence numbers, operations, summary counts, and manifest
  tuples.** *Why it matters:* sabotage 7d was originally designed to inject a `main` ref pointing at the
  staged snapshot AND advance `current-snapshot-id` to the staged id. The view produced by the injected
  metadata was IDENTICAL to `java_staged_meta.json` because neither refs nor current-snapshot-id appear in
  the output. The correct staged-state-specific sabotage is to REMOVE the staged snapshot from
  `metadata.snapshots()` entirely — the canonical view then has 1 ordinal instead of 2, diverges from
  `java_staged_meta.json`, and proves the view IS testing `metadata.snapshots()` coverage. Rule: when
  designing a sabotage for a metadata-view-based interop test, first read what the view ACTUALLY emits (the
  oracle's JSON keys); then design the corruption around a field that IS in the view.

- **`cargo fmt` reformats method chains that exceed line-width limits — always run `cargo fmt --all` BEFORE
  `cargo fmt --all -- --check` at the gate, not instead of it.** *Why:* after writing multi-line assertion
  bodies, the formatter may split a one-line `.expect("msg")` call into a two-line chain. `cargo fmt` fixes
  this automatically; running `-- --check` first without running `cargo fmt` causes a spurious gate failure
  that requires a separate fix commit. Pattern: always `cargo fmt --all && cargo fmt --all -- --check`
  (or just `cargo fmt --all` and rely on `-- --check` in the gate chain).

### 2026-06-12 (Z1 — staged-WAP interop, OPUS REVIEWER, wt-interop3)

- **A field the canonical view EXCLUDES must be VALUE-pinned (not just presence-pinned) in the per-fixture
  verify, on BOTH directions — and the pin must be HAND-DECLARED (anti-circular).** *Found in review:* the
  `SnapshotMetaOracle`/`snapshot_meta_view` summary allowlist excludes `wap.id`, so a corrupted staged
  `wap.id` value rides PAST the byte-equal view diff. The Java `verify()` (D1, Java-judges-Rust) only required
  SOME non-empty `wap.id` on the staged snapshot — corrupting `"w1" → "CORRUPTED"` in the Rust artifact
  passed silently (0 failures). The D2 (Rust-verifies-Java) side already hand-declared `wap.id == "w1"`.
  Mutation proof: corrupt the staged `wap.id` value → D1 must `FAIL`. *Fix:* added an `expectedStagedWapId`
  (S-ff→w1 / S-replay→w2 / S-dedup→w3) value pin to the Java staged-state verify, plus a FF `current` wap.id
  pin (S-ff final must carry wap.id=w1 — the verbatim-publish proxy for `current == staged-id`). Rule: for any
  fact the shared view omits (refs, current-snapshot-id, wap.id, source/published-wap-id values), run the
  corruption mutation on BOTH the producer-side artifact (D1) AND the oracle-side artifact (D2); a pin that
  only exists on one side is half-closed.

- **Ref-state (current-snapshot-id) is invisible to the canonical view, so a "published when it should be
  staged" corruption is caught ONLY by the per-fixture `staged != current` check — and that check is loose
  when >2 snapshots exist.** *Verified by mutation:* moving `current-snapshot-id` onto the staged snapshot in
  an S-ff artifact left the canonical view BYTE-IDENTICAL (view omits refs) yet the per-fixture verify caught
  it (the `staged != current` filter found no qualifying staged snapshot). But for S-replay (3 snapshots: S0,
  staged-w2, S2-advance) `staged != current` allowed `current ∈ {S0, S2}` — it did NOT hand-declare
  `current == S2`. *Fix:* added a hand-declared ref-state pin to the D2 S-replay staged check (current is the
  S2 advance: no `wap.id` AND non-root). Rule (W1 lesson, re-confirmed): when the view can't see ref state,
  the per-fixture verify must HAND-DECLARE the expected current-snapshot-id identity, not just "≠ staged".

### 2026-06-12 (Z2 — multi-spec metadata-level interop fixture, BUILDER Sonnet, wt-interop3)

- **Spec-id alignment across languages requires BOTH sides to start from the SAME initial spec.**
  *Why:* Java's `TableMetadata.newTableMetadata` with `PartitionSpec.unpartitioned()` as the seed
  assigns spec_id=0 to unpartitioned, so `updateSpec().addField("a")` creates spec_id=1 for
  identity(a) and a further `addField("b")` creates spec_id=2. Rust's seed-from-spec-0 approach
  (`with_spec_id(0)` + identity(a) directly) assigns spec_id=0 to identity(a) and spec_id=1 to
  identity(a)+identity(b). FIX: build Java's seed spec DIRECTLY (not from unpartitioned default):
  `PartitionSpec.builderFor(schema).identity("a").build()` then ONLY use `updateSpec().addField("b")`
  for evolution. This makes both sides produce spec_id=0=identity(a), spec_id=1=identity(a)+identity(b).
  Rule: when a cross-language fixture involves spec evolution, BUILD the initial spec explicitly on
  BOTH sides so the first `updateSpec()` call creates spec_id=1, not spec_id=0.

- **SB2 snapshot-stripping must also update `refs["main"]["snapshot-id"]` — Java's metadata parser
  rejects a ref pointing at a non-existent snapshot ID.** *Why:* the Python script stripped ms4 from
  `metadata.snapshots` but left `refs["main"]["snapshot-id"]` = ms4_id, causing Java to error:
  "Snapshot for reference SnapshotRef{snapshotId=…} does not exist in the existing snapshots list."
  FIX: after stripping, also set `refs["main"]["snapshot-id"] = ms3_id` AND
  `current-snapshot-id = ms3_id`. Rule: when stripping a snapshot from metadata JSON for a
  sabotage test, update ALL three ref points (snapshots array, current-snapshot-id, AND any branch
  refs) atomically or the Java parser rejects the file before the interop check can run.

- **SB4 subprocess `cwd` must point to the Maven project directory — not to the fixture sub-path.**
  *Why:* the Python subprocess in SB4 called `mvn` with `cwd = fixture_dir + "/../.."` but
  `fixture_dir` = `.../dev/java-interop/target/interop-multi-spec/fixture`, making `"../.."` =
  `.../dev/java-interop/target/interop-multi-spec/` (no pom.xml). The correct depth is `"../../.."` =
  `.../dev/java-interop/`. Rule: when a shell helper script embeds a Python subprocess that calls
  `mvn`, compute the Maven cwd by counting directory levels from the shell variable — use `realpath`
  or count levels manually against the known fixture path structure.

- **The `clippy::doc_overindented_list_items` lint fires on multi-line `//!` list continuations
  indented beyond 2 spaces.** *Why:* a `//!` bullet continuation of the form `//!        text`
  (8-space indent) was flagged by clippy as over-indented (wants 2 spaces). Fix: rewrite the
  multi-line bullet as a single line, or use exactly `//!   ` (2-space continuation indent).
  Rule: after writing `//!` doc blocks with multi-line bullets, run `cargo clippy --all-targets
  --workspace -- -D warnings` to catch this lint before the gate.

- **Tie-shaping proof belongs in the GEN test, not only in the shell script comment.** *Why:* the
  spec-id tiebreaker's effectiveness (the ONLY disambiguator for the two ms4 manifests) is only
  meaningful if the fixture data actually satisfies the tie condition (identical record counts).
  Asserting `file_f0.record_count() == file_f3.record_count()` AND different partition-tuple arities
  (1 vs 2 fields as the proxy for different spec ids, since `partition_spec_id()` is `pub(crate)`)
  IN THE TEST makes the tie-shaping property machine-checked, not just documented. Rule: whenever
  a fixture depends on a tie-shaping invariant (X == Y across two artifacts), assert it in the code,
  not only in prose.

### 2026-06-12 (Z2 — multi-spec interop fixture, REVIEWER Opus 2-of-2, wt-interop3)

- **A sabotage step that POST-EDITS the emitted view JSON does NOT prove the property it claims to
  re-derive — mutate the ARTIFACT and RE-EMIT.** *Why:* the builder's original SB4 emitted the Rust
  table's canonical view normally, then in Python swapped the `partition_spec_id` INTEGER on the two
  ms4 manifests IN THE OUTPUT JSON and diffed. Its comment claimed this was the "wrong-spec-rendering
  corruption — partition tuples rendered under the WRONG spec," but the partition TUPLES were never
  touched (they were carried through from the clean emit); only the integer moved. That proved the
  spec_id field is in the comparison KEY (a valid injectivity check) but NOT that partition tuples are
  rendered under each manifest's OWN spec (the file's-own-spec rule — the multi-spec arc's whole
  point). FIX: SB4 now swaps the spec-0/spec-1 FIELD DEFINITIONS in the SOURCE `final.metadata.json`
  (ids unchanged) and RE-EMITS via Java — the ms4 spec-0 manifest's 1-field tuple is then projected
  under the 2-field spec (and vice versa: spec-1's `{1000:r,1001:s}` renders as `{1000:r}`, the
  `1001:s` field DROPS), so the re-derived view genuinely diverges. Rule (reviewer mutation mandate):
  a fail-closed sabotage must mutate the on-disk artifact and re-run the production view builder, never
  post-edit the builder's output — the latter can pass while the property under test is broken.

- **The per-own-spec partition rendering IS exercised by the multi-spec fixture's POSITIVE
  comparisons, independent of SB4.** *Why:* `snapshot_meta_view.rs` (Rust) renders each entry's
  partition under `manifest_meta.partition_spec.partition_type(&schema)` and Java's `entryView` under
  `metadata.specsById().get(file.specId()).partitionType()` — both the file's OWN spec. The clean ms4
  manifests render spec-0 → `{1000:q}` (1-field) and spec-1 → `{1000:r,1001:s}` (2-field), and those
  exact tuples must byte-match Java in D1 + D2. So the own-spec rule is load-bearing in the positive
  path; SB4 (re-derived) is the explicit fail-closed pin. Verified by an independent reviewer probe
  (swap spec field-defs in metadata, re-emit → view diverges with `1001:s` dropped).

- **A genuine D1 step exists for multi-spec (Java judges the Rust-written chain) — the 'both
  directions' claim holds.** Script step [4/5] runs `emit-snapshot-meta` on the Rust-produced
  `rust_table/.../final.metadata.json` → `java_view_rust_meta.json` and `diff`s it against
  `java_meta.json` (Java-on-Java). This matches the cherrypick/staged-WAP house D1 pattern. Note: the
  Java `MultiSpecOracle.verify()` method + its `verify-interop-multi-spec` dispatch case are written
  but NEVER invoked by the run script (D1 is done inline via emit+diff instead) — harmless redundancy,
  flagged not removed (builder-owned code, no correctness impact).

### 2026-06-12 (Z3 — partition-stats file interop, BUILDER Sonnet, wt-interop3)

- **`DataFiles.Builder` has NO `withContent(FileContent)` method — it only builds DATA files.**
  *Why it matters:* the Java oracle code to build a position-delete file for a row-delta commit
  must use `FileMetadata.deleteFileBuilder(spec).ofPositionDeletes()`, NOT
  `DataFiles.builder(spec).withContent(FileContent.POSITION_DELETES)`. The `DataFiles.Builder`
  API has no `withContent` method; using it produces a compile error (`cannot find symbol: method
  withContent(org.apache.iceberg.FileContent)`). Rule: when building a delete file in Java oracle
  code, always use `FileMetadata.deleteFileBuilder(spec)` — confirmed via `javap` of the 1.10.0
  api jar.

- **`PartitionStatisticsFile` in the 1.10.0 jar has `path()`, NOT `statisticsPath()`.**
  *Why it matters:* calling `.statisticsPath()` produces a compile error
  (`cannot find symbol: method statisticsPath()`). The 1.10.0 api jar's
  `PartitionStatisticsFile.class` exposes `path()`, `snapshotId()`, and `fileSizeInBytes()` —
  confirmed via `javap -p`. Rule: for any 1.10.0 API class whose method signature is uncertain,
  run `javap -p ~/.m2/…/iceberg-api-1.10.0.jar!/…` BEFORE writing the call, not after the
  compile fails.

- **`PartitionStats.partition()` returns `StructLike`, NOT `PartitionData`, when decoded via
  `readPartitionStatsFile`.** *Why:* the `generate()` path builds `PartitionData` instances
  directly (via `PartitionData.put()`), but `readPartitionStatsFile` deserializes the partition
  struct from the parquet file as a `GenericRecord` / `StructProjection` — the runtime type is
  NOT `PartitionData`. Casting `row.partition()` to `PartitionData` produces a
  `ClassCastException: GenericRecord cannot be cast to PartitionData`. Rule: always use
  `StructLike partition = row.partition()` (the declared return type) and access fields with
  `partition.get(0, Object.class)` without a downcast — the concrete type is decoder-dependent.

### 2026-06-12 (Z3 — partition-stats file interop, REVIEWER Opus 2-of-2, wt-interop3)

- **A re-invoked Java verify that rebuilds a `Table` via `LocalTableOperations.commit(null, meta)` was
  CRASHING on `v0.metadata.json` "File already exists" — which the sabotage check misread as a
  fail-closed.** *Why (STOP-grade, caught by cold-start):* the Z3 sabotage steps 7a/7b/7d call
  `verify-interop-partition-stats` a SECOND/THIRD/FOURTH time against the SAME dir. The verify path
  builds a `Table` from the Rust metadata (to get `Partitioning.partitionType`) via
  `buildTableFromMetadata` → `ops.commit(null, meta)`, which writes `v0.metadata.json` with
  `LocalOutputFile.create()` (REFUSES to overwrite). The clean D1 step (step 4) writes `v0.metadata.json`
  first; every later verify call then throws "File already exists" BEFORE reading the stats parquet. The
  script's pass test is `! grep 'verify…: 0 failures'` — ANY exception (including the unrelated v0
  collision) satisfies it, so 7a/7b/7d "passed" on a file that was never even read. PROVEN by running 3
  consecutive CLEAN verifies: pre-fix #2/#3 collided (no "0 failures"); a wholly-UNcorrupted file thus
  "passed" the sabotage check. FIX (harness, scoped to `PartitionStatsOracle.buildTableFromMetadata`):
  delete any leftover `v\d+\.metadata\.json` (this helper's exclusive artifact — the Rust table uses
  `00000-*.metadata.json` + `final.metadata.json`) before each commit, so every verify reaches the real
  decode. Post-fix: 3 clean verifies all report "0 failures"; the sabotage check now means something.
  Rule (reviewer mandate): for a sabotage that asserts "the run FAILED," confirm the run failed for the
  RIGHT reason — re-run the SAME step on an UNcorrupted artifact and require it to PASS; a fail-closed
  that also "fails" clean is testing the wrong exception (the W2 false-green pattern).
- **The exact-field decode-depth check needs a parquet-AWARE rewrite, not a byte-search.** *Why:* 7b's
  byte-search for the int64-LE `0x0300000000000000` finds 4 occurrences (data page + footer min/max
  stats) and mutates the FIRST (offset 105), which structurally CORRUPTS the parquet — Java fails via a
  decode EXCEPTION, not a clean field mismatch (proves "garbage rejected," not "wrong counter caught at
  the right column"). To prove the field-level comparison is load-bearing, read the stats parquet, mutate
  ONE Int64 column's row-0 value preserving the field-id schema, rewrite, and re-verify: Java then emits
  `FAIL row 0 (partition cat=a): position_delete_record_count expected=1 actual=7` — confirming the exact
  column is compared. Reviewer-verified for `data_record_count`, `position_delete_record_count`,
  `last_updated_snapshot_id` (each fails on exactly its own line). The only stats field NEVER compared is
  `last_updated_at` (wall-clock millis, run-variant — `last_updated_snapshot_id` is its stable proxy);
  defensible omission, not a gap.
### 2026-06-12 (Wave-5 Group Y / Y1 — theta-sketch foundation, BUILDER Opus, wt-tstats)
- **DataSketches' MurmurHash3 is NOT the canonical byte-stream MurmurHash3 — DO port from the jar
  bytecode, never reuse the `murmur3` crate, even though it ships `murmur3_x64_128`.** *Why:* the
  crate streams bytes with a `u32` seed and XORs the running byte count; DataSketches'
  `MurmurHash3$HashState` (1.10.0 jar) processes the input as 16-byte BLOCKS of two LE `u64`s with a
  64-bit seed, the long-array path passes `len*8` (not `len`) as lengthBytes to `finalMix128`, and it
  REJECTS zero-length input (`checkPositive` throws). Constants: C1=0x87c37b911142_53d5,
  C2=0x4cf5ad4327_45937f, block adds 0x52dce729/0x38495ab5, fmix64 standard. The seed-hash for the
  Iceberg default seed 9001 is `computeSeedHash(9001) = hash([9001L],0)[0] & 0xFFFF = 37836` (0x93cc).
  A single divergent bit makes every NDV blob incompatible with all other engines — so the hash is
  pinned against Java vectors for byte-tail lengths 1..=18 AND representative longs.
- **The estimation-mode CompactSketch is byte-reproducible WITHOUT the C++ library if you port
  `HeapQuickSelectSketch.hashUpdate` + `quickSelectAndRebuild` faithfully — and the retained SET is
  probe-order-INDEPENDENT, so a simple stride-1 reproduction confirms the algorithm before porting the
  real `getStride`.** *Why:* I reproduced the lib's exact retained set (24 hashes) + theta
  (266783384329207353) for "1000 longs at lgK=4" in pure Java first. Keys are `hash[0] >>> 1` (63-bit);
  `theta = selectExcludingZeros(cache, curCount, 2^lgK + 1)` = the (2^lgK)-th smallest non-zero hash
  (0-based index 2^lgK); initial `lgArrLongs = startingSubMultiple(lgK+1, 3, 5)`; threshold =
  `floor((lgArr<=lgNom?0.5:0.9375) * 2^lgArr)`; on overflow grow if `lgArr<=lgNom` else quickselect.
  The real probe is `getStride = 2*((hash>>>lgArr)&127)+1` (odd) — ported for fidelity, but the SET is
  the same regardless of probe order, so a quick stride-1 Java repro is a cheap pre-port sanity oracle.
- **The COMPACT serialized form ZEROES bytes 3-4 (lgNomLongs/lgArrLongs) — those are update-only
  state, NOT part of the cross-engine contract.** *Why:* a decoded compact sketch reported lgNom=0
  lgArr=0 even at lgK=4. The compact contract is only: preamble (preLongs 1/2/3) + theta (stored only
  when preLongs=3 / theta<MAX) + the ascending hashes; count at b8-11, P=1.0f at b12-15. Empty stores
  NO seed hash (b6-7=0); single-item uses preLongs=1 + SINGLEITEM flag (0x20) + one hash, theta=MAX.
- **A new in-workspace crate can be ZERO-dependency (std-only) and is `cargo machete`-clean by
  construction — flag that explicitly when machete/taplo aren't installed locally.** *Why:* the
  theta hash + serialization are pure arithmetic + byte layout; no external crate is needed (the
  `murmur3` crate would be WRONG anyway). The crate's `[dependencies]` table is empty, so there is
  nothing for machete to flag; the Cargo.lock entry has no dependency list. Stated in the report
  since neither tool was runnable in this environment.

### 2026-06-12 (Wave-5 Group Y / Y1 — theta-sketch foundation, REVIEWER Opus, wt-tstats)
- **A "byte-exact vs Java" estimate pin with a `< 1e-6` tolerance is NOT a regression pin — it hides a
  real 1-ULP formula divergence. For any cross-engine numeric, assert `f64::to_bits()`, never a
  tolerance.** *Why:* the builder computed NDV `estimate` as `count / (theta / MAX)`; Java
  `Sketch.estimate(long thetaLong, int curCount)` (bytecode) is `curCount * (9.223372036854776E18 /
  thetaLong)` — i.e. `count * (2^63_f64 / theta)`. The two are algebraically equal but round ONE ULP
  apart on the lgK=4 fixture (Java `829.7403132548839` bits `…142c`; the builder's form `…142d`). The
  builder's own test asserted `(est - 829.7403132548839).abs() < 1e-6` → it PASSED on the wrong value.
  Fixed to Java's exact order-of-operations + pinned `to_bits()`. RULE: derive the estimator's exact
  expression from `getEstimate`'s bytecode and reproduce the OPERATION ORDER, not just the algebra; the
  constant `2^63` (Java's literal) equals `i64::MAX as f64` to the bit, so `MAX_THETA as f64` is right.
- **A compact-sketch parser that branches on `preLongs` MUST special-case `preLongs==1`, or a hostile
  8-byte blob panics on `bytes[8..12]`.** *Why (Java `CompactOperations.memoryToCompact` bytecode):*
  curCount is read from bytes 8-11 ONLY for `preLongs > 1`; a 1-long preamble that is neither EMPTY nor
  single carries curCount=0 (no count field — the buffer is only 8 bytes). The builder's reader fell
  through to the multi-entry path for `preLongs==1` and index-panicked (range end 12 > len 8). Java
  reads it as a 0-entry sketch (heapify-probed). Two sub-cases: ALSO single-item when `flags & 31 == 26`
  (READ_ONLY|COMPACT|ORDERED, no bit-32 — the LEGACY single encoding `otherCheckForSingleItem`), reading
  the hash at byte 8. Fixed both; pinned no-panic + Java-parity. RULE: a deserializer's "required-bytes"
  guard must be keyed off the SAME field shape the body-reader assumes — a length check that passes
  `8 < 8` does not protect a read of bytes 8-11.
- **Update-overload equivalence (`hash(long[]{v}) == hash(LE8(v))`) is a provable IDENTITY, not a
  coincidence — but verify the per-overload Java path from bytecode before trusting it.** *Why:* the
  headline cross-engine question for Y2 is whether feeding a long column via `update_u64` (long-array
  path) matches Java's `UpdateSketch.update(long)`. Bytecode: `update(long v)` builds `long[]{v}` and
  hashes via `MurmurHash3.hash([J,J)` then `>>> 1`; `update(byte[])` hashes the raw bytes via
  `hash([B,J)`. For a single long both reduce to `finalMix128(k1=v, k2=0, lengthBytes=8)` — identical.
  Confirmed EQ=true for all 17 edge values against datasketches-java-3.3.0. So `update_u64` is
  Java-faithful for long columns; a Y2 caller may feed either form for longs. (String/binary columns go
  through the byte path with their own bytes — no equivalence claim needed there.)
- **The DataSketches serial-version reader posture: Java accepts v1/v2/v3 (`CompactSketch.heapify` →
  `ForwardCompatibility.heapify{1,2}to3`); a v3-ONLY Rust reader is defensible because every Iceberg
  theta blob is v3, but it IS a documented reader gap — say so in the code + map, don't leave it
  implicit.** Likewise Java CAN emit UNORDERED compact (`compact(false,…)`, ORDERED flag clear, flags
  0x0a); the reader must accept it verbatim (Java only records the flag, never re-validates ordering).
  Both pinned. Positive divergence found + kept: a `count=i32::MAX` allocation-bomb blob errors
  (TruncatedInput) in Rust BEFORE allocating, where Java's heapify trusts curCount and throws
  OutOfMemoryError — Rust's fail-closed-before-alloc is strictly safer and still loud.
- **Reviewer probe hygiene: drive the crate's PUBLIC API from a throwaway `crates/<c>/tests/*.rs`
  integration file to compare against Java, then DELETE the dir before the gate.** Each probe test
  output was cross-checked line-by-line against a fresh Java oracle (`AdversarialOracle.java` +
  `HeapifyProbe.java`, compiled against the m2 jar) — `cargo test --nocapture` interleaves stdout when
  multiple `println!` race, so run each probe test with `--exact` individually for clean capture, or it
  silently drops lines and looks like a divergence.

### 2026-06-12 (Wave-5 Group Y / Y2 — ComputeTableStats action, BUILDER Opus, wt-tstats)
- **The theta-sketch VALUE→bytes contract IS `Datum::to_bytes()` — verify it byte-for-byte against
  `Conversions.toByteBuffer` bytecode before trusting it, then feed via `update_bytes`.** *Why:* the
  puffin spec says "values converted to bytes using Iceberg's single-value serialization"; `javap -c`
  on `iceberg-api-1.10.0` `Conversions.toByteBuffer` confirms per type — int/date=`allocate(4).order(LE)
  .putInt`, long/time/timestamp(tz)(Ns)=`allocate(8).order(LE).putLong`, float/double=LE, string=
  `CharBuffer.wrap → UTF-8 encoder` (unprefixed), uuid=16B BE, fixed/binary=raw, decimal=
  `unscaledValue().toByteArray()` (BE two's-complement minimal). `Datum::to_bytes()` matches all of
  these. A per-type byte-form unit pin (hand-declared bytes) is mandatory: the crown-jewel NDV-count
  test alone does NOT catch a wrong byte form (the display/JSON form yields the SAME distinct count) —
  only the byte pins fail when `to_bytes()` is swapped for `to_string()`. Proven by mutation.
- **The `ndv` blob property is `String.valueOf((long) sketch.getEstimate())` — `f64 as i64` TRUNCATION
  toward zero, NOT `round()`.** *Why:* `NDVSketchUtil` (spark, MAIN-only) + the puffin spec ("non-negative
  integer ... decimal digits, no leading/trailing spaces"). Rust `estimate() as i64` is the identical
  truncation. Don't reach for `.round()`.
- **Java `ComputeTableStats` defaults: columns = all TOP-LEVEL PRIMITIVE columns (skip nested), snapshot
  = current.** *Why:* `ComputeTableStatsSparkAction` filters `schema.columns().filter(type.isPrimitiveType())`
  and `validateColumns` rejects non-existent ("Can't find column %s") + non-primitive ("Can't compute stats
  on non-primitive type column"); the `ComputeTableStats` javadoc says "by default all columns are chosen".
- **The spark `theta_sketch_agg` / `NDVSketchUtil` feeding behavior is MAIN-only (lives in `iceberg-spark`,
  NOT in the m2 `iceberg-{api,core,data}` jars) — pin the action against the puffin SPEC + `Conversions`
  bytecode, and flag the agg as MAIN-only.** *Why:* the cross-engine contract is the BYTES + the blob/
  property shape, all derivable from core + the spec; the spark UDAF only orchestrates `update(byte[])`.
- **`PuffinWriter::close()` returns total size only — Java's `StatisticsFile` also needs `footerSize()`.**
  *Why:* `ComputeTableStatsSparkAction` builds `GenericStatisticsFile(..., writer.fileSize(),
  writer.footerSize(), ...)`. Add a non-breaking `footer_size()` accessor (MAGIC + footer_payload +
  FOOTER_STRUCT) rather than changing `close()`'s signature (3 in-crate callers). The existing
  `UpdateStatisticsAction` already covers `SetStatistics` — reuse it (do NOT rebuild a registration path
  like X2 had to for partition stats, which has no transaction action).

### 2026-06-12 (Wave-5 Group Y / Y2 — ComputeTableStats action, REVIEWER Opus, wt-tstats)
- **STOP-GRADE: Java's NDV pipeline builds an ALPHA-family sketch; the Y1 port is QUICKSELECT — do NOT
  dismiss the puffin spec's "Alpha" as a doc nit.** *Why:* three independent sources agree —
  `format/puffin-spec.md` ("constructing **Alpha family sketch**"), Spark `ThetaSketchAgg
  .createAggregationBuffer` MAIN source (`UpdateSketch.builder.setFamily(Family.ALPHA).build()`, all of
  spark v3.5/4.0/4.1 + the class doc), and `datasketches-java-3.3.0` bytecode (`UpdateSketchBuilder`'s
  DEFAULT family is QuickSelect, so the explicit `.setFamily(ALPHA)` is load-bearing — it overrides the
  default the Y1 port matches). Consequence (Java probe, lgK12/seed9001): **exact mode (≲ a few thousand
  distinct, theta==MAX) Alpha and QuickSelect are byte-identical + same ndv; estimation mode (≳7k) they
  DIVERGE** (n=1M → Alpha 1004032 vs QS 1002714, different retained set + bytes — Alpha switches to a
  sampling estimate `nominal*MAX/theta`). A test suite that feeds only ≤6 distinct values per column
  CANNOT see this — the divergence is SILENT. Lesson: when the headline is a sketch-FAMILY question, the
  crown-jewel hand-count test is necessary but NOT sufficient — add an explicit estimation-mode value pin
  (large distinct input) that documents the QuickSelect↔Alpha gap, so the next agent porting Alpha has a
  visible, citable pin (flip it to assert-equal-with-Alpha when the family lands). DON'T fix Y1's crate
  from Y2 (committed byte surface) — STOP-report the family verdict in the module doc + matrix cell.
- **The `footer_size <= file_size` invariant is too weak — a `footer_size == total` mutation SURVIVES it.**
  *Why:* a real Puffin stats file always has the leading MAGIC + the blob payloads BEFORE the footer, so
  `footer < total` STRICTLY; Java readers locate the blob region as `fileSize - footerSize`, so a footer
  that equals/exceeds the data is corrupt. Pin `<` not `<=`. (Verified the seam IS correct otherwise:
  Rust `footer_size()` == Java `PuffinWriter.footerSize()` byte-for-byte = MAGIC(4) + payload +
  FOOTER_STRUCT(12) = payload+16, the exact bytes `write_footer` appends.)
- **The 4 byte-form pins (long/date/string/decimal) skip the dangerous edges — add them.** *Why:*
  negative/zero decimals (Java `BigInteger.toByteArray` minimal two's-complement: -1→`ff`, 0→`00`,
  -300→`fe d4`), uuid (16B BE), float/double NaN + -0.0 (Java `putFloat`/`putDouble` write RAW bits, no
  NaN canonicalization — Rust `to_le_bytes` matches), and boolean (1 byte) all have distinct failure modes
  a long/string pin can't catch. `Datum::to_bytes` matches Java for ALL of them (verified vs a Java probe).
- **Spark agg/UDAF source IS available at `/tmp/iceberg-java-ref/spark/v{3.5,4.0,4.1}` even though no
  iceberg-spark JAR is in `~/.m2`.** *Why:* the orchestrator believed datasketches came in via a spark jar
  in m2 — it did NOT (only `datasketches-java`/`-memory` jars are there). For spark-action provenance read
  the MAIN source tree, not m2 bytecode; for the FAMILY question that source (`setFamily(ALPHA)`) is the
  authoritative oracle, cross-checked against the datasketches jar's builder-default bytecode.

### 2026-06-12 (Wave-5 Group Y / Y3 — Alpha-family update sketch, BUILDER Opus, wt-tstats)
- **THE ndv-source ruling: Iceberg's `ndv` reads the COMPACT sketch's `getEstimate`, NOT the Alpha
  update sketch's. The two genuinely DIFFER in estimation mode — derive which object from
  `NDVSketchUtil.toBlob`, never assume "the sketch's estimate".** *Why (decisive — spark v3.5/4.0/4.1
  `NDVSketchUtil.java` identical):* `Sketch sketch = CompactSketch.wrap(Memory.wrap(bytes)); ... ndv =
  String.valueOf((long) sketch.getEstimate())`. `sketch` is the COMPACT sketch reparsed from the
  serialized bytes — its `getEstimate` is the family-COMPACT STANDARD estimator `retained*(2^63/theta)`.
  The live Alpha UPDATE sketch's `getEstimate` (`HeapAlphaSketch.getEstimate` bytecode) is family-aware:
  `theta>split1` → standard, `theta<=split1` → SAMPLING `nominal*(2^63/theta)`. Java probe (lgK12/seed9001,
  n=1M): UPDATE sampling estimate = 1002319 but COMPACT estimate = 1004032 — and **1004032 is the prompt's
  pinned ndv**. So the action must do `CompactThetaSketch::deserialize(&payload).estimate()`, exactly
  Java's `CompactSketch.wrap(bytes).getEstimate()`. Pinning BOTH values (the compact one as the ndv, the
  update one as "NOT the ndv") makes the object-selection load-bearing. The prompt framed `nominal*MAX/theta`
  as "the Alpha estimator the Y2 probe saw" — true of the UPDATE sketch, but the ndv uses the COMPACT one;
  a builder who wired `alpha.estimate()` would emit 1002319 and silently diverge from every engine.
- **`HeapAlphaSketch.compact()` is family-COMPACT, not family-ALPHA — the on-disk form REUSES the
  QuickSelect serializer verbatim; only the UPDATE-side retention/theta differs.** *Why:* `toByteArray()`
  on the live Alpha sketch writes a family-ALPHA preamble, but Iceberg serializes via `UpdateSketch.compact()`
  → `componentsToCompact(thetaLong, getRetainedEntries(true), seedHash, isEmpty, ..., cache)` → `compactCache`
  keeps cache entries `0<h<theta`, `Arrays.sort` ascending, `loadCompactMemory` writes the family-COMPACT (id 3)
  preamble. So `AlphaSketch::serialize_compact()` = the Y1 `serialize_compact_from_parts(is_empty, theta,
  sorted_below_theta_hashes, seed)` UNCHANGED — one path, no fork. The reused retained count is
  `getRetainedEntries(true)` (the DIRTY-aware `countPart` = the below-theta cache count), NOT the raw
  `curCount_` (which over-counts dirty stale slots). Make the shared helpers `pub(crate)` rather than
  duplicating the serializer.
- **The Alpha dirty-phase insert (`enhancedHashInsert`) reuses stale (≥theta) slots WITHOUT a count
  bump and does NOT rebuild every insert — port it faithfully or the retained set drifts by an element.**
  *Why:* once `theta<=split1` the table accumulates above-theta entries; a new insert probes, and on the
  FIRST stale slot it reuses it in place (`InsertedCountNotIncremented`), decays theta, sets dirty; only a
  truly-empty slot bumps `curCount` and may trigger `rebuildDirty` (a same-size purge; if nothing purged,
  grow by 1). A naive "rebuild on every dirty insert" or "always land in an empty slot" port produced a
  retained SET that differed from Java by ONE borderline hash near theta — invisible to retained-count and
  theta pins (both matched) but caught by the BYTE-EXACT fixture. Lesson: for a stateful Java sketch, the
  byte-exact estimation fixture is the only pin that catches a single-element set drift; pin retained+theta
  AND the full bytes.
- **A byte-exact `*_HEX` const transcribed by hand into Rust is error-prone — generate it, then REPLACE
  the const programmatically from the verified-equal Rust/Java output, never retype a 8000-char hex.**
  *Why:* my first paste of the lgK9/520 fixture had 2 extra hex chars at byte 1880; the retained SET and
  theta were byte-identical (proven by dumping both sets — zero diff), so the bug was purely a transcription
  typo in the const, not the algorithm. A `python3 re.sub` replacing the const with the Java-generated hex
  (after confirming the live Rust `serialize_compact()` == the Java hex) fixed it in one shot. DO diff the
  retained SETS first when an estimation fixture fails — if the sets match, the bug is in the const, not the
  sketch.
- **`setHashTableThreshold(lgNom, lgArr)` uses the 0.5 fraction when `lgArr <= lgNom` (NOT 0.9375).** *Why:*
  the initial Alpha table at lgK12 has lgArr=7 (`startingSubMultiple(13,3,5)=7`), and 7<=12 ⇒ threshold =
  `floor(0.5*2^7)=64`, not 120. I twice wrongly asserted the 0.9375 branch; it only applies once the table
  has grown PAST nominal. Read the `if_icmpgt` direction in the bytecode, don't assume the resize-phase fraction.

### 2026-06-12 (Wave-5 Group Y / Y3 — Alpha-family update sketch, REVIEWER Opus, wt-tstats)
- **HEADLINE — the real bug was at the ACTION level, not in the dirty-path sketch: NO test pinned that
  PRODUCTION `write_stats_file` reads the COMPACT estimate (1004032), not the Alpha UPDATE sampling
  estimate (1002319). The headline `ndv` re-pin RECONSTRUCTED the path inline (`AlphaSketch` →
  `serialize_compact` → `CompactThetaSketch::deserialize().estimate()`) and never called
  `write_stats_file`; the crown jewel calls `execute()` but only on ≤6-distinct EXACT-mode data where
  the two estimators COINCIDE.** *Proof:* mutating `write_stats_file` to `let ndv = sketch.estimate()`
  (the update form) SURVIVED all 21 compute_table_stats tests. FIX: added
  `test_write_stats_file_ndv_property_reads_compact_estimate_in_estimation_mode` — mints a real snapshot,
  feeds a 1M-distinct `AlphaSketch`, drives the PRODUCTION `write_stats_file`, reopens the puffin, and
  asserts the on-disk `ndv` PROPERTY == 1004032 (≠ 1002319). Fail-before (1002319) / pass-after (1004032)
  verified. RULE: a pin that reconstructs the code path inline does NOT pin production — drive the real
  fn, and put the discriminating input (estimation-mode, where the two objects diverge) through it, or the
  object-selection decision is unguarded where it actually lives.
- **The dirty-path stale-slot machinery (`enhancedHashInsert` reuse, `rebuildDirty` grow-if-nothing-purged,
  count-on-empty-vs-reuse) is byte-output-EQUIVALENT to a naive clean-path port — so the byte-exact
  fixtures CANNOT catch most dirty-path mutations.** *Why:* theta decays exactly once per successful
  insert in BOTH paths (same decay count ⇒ same theta trajectory); the retained set is `{0<h<theta}`,
  invariant under table size and probe layout; rebuilds purge stale entries + re-sync count but never
  change theta or the below-theta set. Verified: `if false` (never take the dirty path), `current_count
  += 1` on stale reuse, and flipping `rebuildDirty`'s grow condition ALL produced byte-identical output on
  a 200k-element adversarial pseudo-random sequence (Rust == Java FNV-of-bytes identical). The ONE
  dirty-path mutation that bites is "reuse the EMPTY slot instead of the stale slot" — it leaves stale
  entries forever, the table fills, and the inner stale-search loop (no `loop_index` guard, faithful to
  Java) SPINS FOREVER (test hangs at 100% CPU). So the stale-slot reuse is load-bearing for LIVENESS, not
  for the bytes. LESSON: for a stateful sketch where the serialized form is a pure function of (theta,
  below-theta set), byte fixtures pin the OUTPUT contract but NOT the internal state machine; to pin the
  machine you need a differential oracle (run both candidate algorithms, diff) or a liveness/timeout pin —
  the prompt's worry "a subtle miss hides DESPITE byte-exact fixtures" is real, but it lives in the ACTION
  object-selection, not the cache mechanics.
- **The three trickiest bytecode facts RE-DERIVED independently and CONFIRMED against alpha.rs:**
  (a) `enhancedHashInsert` tracks the FIRST stale (`>= theta`) slot (`deleteIndex`, local 10) and reuses
  it in place WITHOUT a count bump (`InsertedCountNotIncremented`, offsets 154-179); a truly-empty slot
  bumps count + may `rebuildDirty` (offsets 272-318). The probe order is first-stale-wins (the inner loop
  at 89-119 only breaks on hash-found or empty, never on a later stale). (b) `rebuildDirty` (offsets 0-22):
  `prev=curCount; forceRebuildDirtyCache(); if (prev == curCount) forceResizeCleanCache(1)` — grow ONLY
  when NOTHING was purged (`prev == curCount`). (c) `getRetainedEntries(true)` = `(curCount>0 && arg &&
  isDirty()) ? HashOperations.countPart(cache,lgArr,theta) : curCount`; `countPart` counts `!continueCondition`
  = `{0<h<theta}`; `UpdateSketch.compact(b,mem)` feeds `getRetainedEntries(true)` (NOT raw curCount) to
  `componentsToCompact`, and `compactCache` THROWS "curCount parameter is incorrect" if the below-theta
  count `< curCount` — so a port that fed raw curCount when dirty would error in Java. Rust's
  `retained_entries()` (dirty ⇒ `below_theta_hashes().len()`) + `serialize_compact` (direct below-theta
  filter) match all three. The "raw curCount" mutation IS caught by the `retained_entries()` assertions.
- **Seam fixtures at the dirty-state-machine transitions (n=nominal/nominal+1/nominal+2 at lgK9; stale-reuse
  checkpoints n=600/1000/5000 with dup-interleave; mid-size lgK12/n=10000) are all byte-EXACT vs fresh
  datasketches-java-3.3.0 — but they catch nothing the existing endpoint fixtures miss** (per the
  equivalence finding above). Generated + verified them as a probe, then DELETED rather than bloat the
  suite with multi-KB redundant hex (the lesson's transcription-hazard warning). The benign `>` vs `>=`
  split1-comparison mutations also survive (boundary `theta == split1` is measure-zero) — not gaps; the
  estimator BRANCH-DIRECTION flip IS caught by the n=7000/1M sampling-estimate pins (6973/1002319).
