<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# RFC: Stateful Transaction

**Status:** Draft
**Target:** Apache Iceberg Rust (`iceberg-rust`)

**Scope:** Transaction/action lifecycle, retry-persistent state, retry and rebase semantics, conflict validation, incremental metadata caching, snapshot-producing execution, artifact lifecycle, and merging transaction actions.

---

# 1. Motivation

## 1.1 Missing retry infrastructure

On current `main`, transaction actions are stored as `Arc<dyn TransactionAction>` and committed by replaying the actions against a refreshed table.

This is sufficient for metadata-only actions and simple snapshot operations, but merging operations — rewrite-files, overwrite, delete, row-delta, and replace-partitions — require a stronger retry model.

Three pieces are missing.

### 1. No place to retain state across commit attempts

`TransactionAction::commit` currently cannot carry mutable state from one attempt to the next.

A snapshot-producing action therefore cannot retain:

* a stable `snapshot_id`;
* a stable `commit_uuid`;
* already-read immutable metadata;
* already-validated history;
* already-filtered manifests;
* generated manifests that may be reusable;
* a record of metadata artifacts written by previous attempts.

Every retry effectively starts from scratch.

### 2. No explicit validation and rebase boundary

Merging actions must validate concurrent changes between a starting snapshot and the refreshed transaction-local parent.

After a catalog conflict, the transaction must distinguish between:

* work that remains valid because its semantic dependencies are unchanged; and
* work that depended on the old base and must be recomputed.

For example, a snapshot that was already inspected is immutable and does not need to be re-read. A validation result for that snapshot may also remain valid if the validation context is unchanged.

By contrast, the final validation decision through the table head cannot be reused after the head changes.

### 3. No model for reusable work or artifact ownership

Snapshot-producing actions write metadata before `catalog.update_table` is called.

During retries, this may include:

* added-file manifests;
* rewritten manifests;
* delete manifests;
* manifest lists;
* later, other table-format-specific metadata.

Some of this work is reusable after a rebase. Some is tied to one exact attempt.

Regardless of reuse, the transaction must know which physical metadata artifacts it created so that uncommitted artifacts can eventually be cleaned up safely.

The retry architecture therefore needs to answer two independent questions:

1. **Reuse:** what completed work remains semantically valid?
2. **Ownership:** what physical artifacts did this action write?

This RFC introduces explicit retry-persistent state to answer both.

---

## 1.2 Current retry behavior already has a cost

Even FastAppend currently reconstructs a fresh `SnapshotProducer` on replay.

A fresh producer may generate a new `snapshot_id` and `commit_uuid`, which are embedded into generated metadata paths:

```text
{commit_uuid}-m0.avro
snap-{snapshot_id}-{attempt}-{commit_uuid}.avro
```

A catalog conflict may therefore look like:

```text
attempt 1:
    snapshot_id = S1
    commit_uuid = U1
    write U1-m0.avro
    write snap-S1-0-U1.avro
    catalog conflict

attempt 2:
    snapshot_id = S2
    commit_uuid = U2
    write U2-m0.avro
    write snap-S2-0-U2.avro
    success
```

The first attempt's metadata is neither reused nor naturally associated with the second attempt.

The immediate correctness requirement is stable action state. Once state exists, it also becomes possible to avoid repeating immutable metadata reads and deterministic metadata transformations.

---

# 2. Transaction Action and Retry State

## 2.1 Retry versus rebase

This RFC distinguishes two cases.

### Retry

A new catalog submission is made against the same table base:

```text
build from S10
    ↓
catalog submission fails
    ↓
refresh
    ↓
still S10
```

All semantic inputs derived from the table base remain unchanged.

A future implementation may therefore reuse an entire already-built attempt.

Whole-attempt reuse is not required by the initial implementation.

### Rebase

Refresh observes a newer table base:

```text
our base:      S10
concurrent:    S10 → S11
refresh:       S11
```

The action must execute again against `S11`.

However, a rebase does **not** imply that all previously computed state becomes invalid.

The central rule is:

> **A retry may reuse completed state whose semantic dependencies remain unchanged. A rebase invalidates results that depend on the previous base, while results derived solely from immutable action inputs or immutable committed metadata may remain reusable.**

For example:

```text
preserve:
    action intent
    snapshot identity
    already-loaded immutable snapshot metadata
    already-loaded immutable manifests
    deterministic filter results for unchanged manifests

recompute:
    current parent
    sequence number
    row-ID allocation
    complete current manifest set
    final validation through the new parent
    manifest list
    TableCommit
```

---

## 2.2 Transaction action interface

Each action declares its own retry-persistent state.

```rust
/// Marker for state that may survive transaction commit attempts.
pub(crate) trait TransactionActionState: Send + 'static {
    /// Stable snapshot identity, if this state carries one.
    /// Used when resolving an unknown commit outcome (section 6.6).
    fn snapshot_id(&self) -> Option<i64> {
        None
    }
}

impl TransactionActionState for () {}

#[async_trait]
pub(crate) trait TransactionAction: Sync + Send + 'static {
    type State: TransactionActionState;

    /// Called once, at the action's *first commit attempt*, against the
    /// refreshed transaction-local table (see "Lazy initialization" below).
    fn new_state(&self, table: &Table) -> Result<Self::State>;

    /// Called once per transaction attempt against the current
    /// transaction-local table.
    async fn commit(
        &self,
        state: &mut Self::State,
        table: &Table,
    ) -> Result<ActionCommit>;
}
```

The action and its state are paired before type erasure:

```rust
struct TransactionActionEntry<A: TransactionAction> {
    action: Arc<A>,
    /// `None` until the first commit attempt initializes it.
    /// Never reset afterwards for the life of this entry.
    state: Option<A::State>,
}

#[async_trait]
pub(crate) trait DynTransactionActionEntry: Send {
    /// Initializes state on first use, then commits one attempt.
    async fn commit(&mut self, table: &Table) -> Result<ActionCommit>;

    /// Stable snapshot identity, if initialized (section 6.6).
    fn snapshot_id(&self) -> Option<i64>;

    /// Clones the logical action with uninitialized state.
    fn fork(&self) -> Box<dyn DynTransactionActionEntry>;

    /// Terminal-outcome hook, driven by the transaction (section 6.7).
    /// Never invoked while the outcome is unknown.
    async fn cleanup(&mut self, outcome: TerminalOutcome<'_>) -> Result<()>;
}

/// Terminal commit outcomes as established in section 6.
pub(crate) enum TerminalOutcome<'a> {
    /// The transaction committed; the committed metadata drives
    /// reachability-based cleanup (section 6.4).
    Success(&'a TableMetadata),
    /// The transaction definitively did not commit and will not retry.
    Failure,
    // `Unknown` is deliberately absent: unknown is not terminal, and
    // cleanup must never run for it (section 6.6).
}

#[async_trait]
impl<A: TransactionAction> DynTransactionActionEntry for TransactionActionEntry<A> {
    async fn commit(&mut self, table: &Table) -> Result<ActionCommit> {
        let state = match &mut self.state {
            Some(state) => state,
            state @ None => state.insert(self.action.new_state(table)?),
        };
        self.action.commit(state, table).await
    }

    fn snapshot_id(&self) -> Option<i64> {
        self.state.as_ref().and_then(|s| s.snapshot_id())
    }

    fn fork(&self) -> Box<dyn DynTransactionActionEntry> {
        Box::new(TransactionActionEntry::<A> {
            action: Arc::clone(&self.action),
            state: None,
        })
    }

    // cleanup: iterate the state's artifact tracker (section 6.7).
}

pub struct Transaction {
    table: Table,
    actions: Vec<Box<dyn DynTransactionActionEntry>>,
}

impl Clone for Transaction {
    /// A cloned transaction is a new logical execution of the same plan:
    /// it shares the immutable actions and starts with uninitialized state,
    /// so it acquires fresh identity on its first commit attempt.
    fn clone(&self) -> Self {
        Self {
            table: self.table.clone(),
            actions: self.actions.iter().map(|entry| entry.fork()).collect(),
        }
    }
}
```

This preserves the `Action → State` relationship statically.

There is no independent `dyn Action` / `dyn State` pairing and no runtime downcast.

`ApplyTransactionAction::apply` only builds the entry; it performs no table
access and no I/O:

```rust
impl<T: TransactionAction + 'static> ApplyTransactionAction for T {
    fn apply(self, mut tx: Transaction) -> Result<Transaction>
    where Self: Sized {
        tx.actions.push(Box::new(TransactionActionEntry {
            action: Arc::new(self),
            state: None,
        }));
        Ok(tx)
    }
}
```

### Lazy initialization

State is initialized at the first commit attempt, not at apply time, for
three reasons:

1. **`Clone` must stay infallible.** `Transaction` derives `Clone` today and
   it is public API. `new_state` is fallible and table-dependent, so an
   eager-initialization design would force `Clone` to either share state
   (unacceptable: shared identity and shared artifact ownership) or panic.
   With lazy initialization, `fork` is trivial and total.
2. **A fresher collision check.** The `snapshot_id` collision check runs
   against the refreshed transaction-local table inside the commit loop,
   rather than against the possibly stale table captured at apply time.
3. **Java parity.** Java's producer generates `snapshotId()` lazily on first
   use; the entry reproduces that behavior without a long-lived producer
   object.

### Auto-traits

`TransactionActionState` requires `Send` but deliberately not `Sync`:
`commit` and `cleanup` take exclusive access, so nothing is shared across
threads by reference. `Transaction` therefore remains `Send` but is not
promised `Sync`. This is observable in `public-api.txt` and must be
regenerated as a deliberate change, not an accident.

---

## 2.3 Snapshot action state

Snapshot-producing actions require a stable logical identity.

```rust
pub(crate) struct SnapshotActionState {
    snapshot_id: i64,
    commit_uuid: Uuid,

    /// Correctness-bearing (not bookkeeping): part of every artifact path
    /// this action writes. Incremented at the start of every `commit()`
    /// invocation, before any I/O — including attempts that abort early.
    attempt: u32,
}

impl TransactionActionState for SnapshotActionState {
    fn snapshot_id(&self) -> Option<i64> {
        Some(self.snapshot_id)
    }
}
```

### `snapshot_id`

`snapshot_id` identifies the logical Iceberg snapshot being produced.

It is generated once when the action's state is initialized and remains stable across retries and rebases.

Reparenting the action from one current snapshot to another does not make it a different logical snapshot action.

Stability also gives an ambiguous commit outcome a durable identifier to
resolve against (section 6.6), and duplicate application is structurally
rejected: `TableMetadataBuilder::add_snapshot` fails on an already-present
snapshot ID.

### `commit_uuid`

`commit_uuid` namespaces physical metadata artifacts generated by the action.

Keeping it stable gives all retry attempts one artifact namespace and allows artifacts generated by earlier attempts to be reused.

### `attempt` and artifact naming

Stabilizing `commit_uuid` creates a naming hazard that did not exist before.
On current `main`, manifest paths are:

```text
{commit_uuid}-m{n}.avro          n = per-producer counter, restarts at 0
```

This is collision-free today only because `commit_uuid` rotates every
attempt. With a stable `commit_uuid` and an attempt-scoped counter, attempt
2's first new write would land on attempt 1's path — overwriting an artifact
that may be reused by the current attempt, or referenced by a previous
submission whose outcome is unknown. Object-store overwrites are also not
atomic, so even a transient overwrite can expose partial bytes to readers.

The fix is structural: every artifact path embeds the attempt that wrote it.

```text
manifest:       {commit_uuid}-{attempt}-m{n}.avro
manifest list:  snap-{snapshot_id}-{attempt}-{commit_uuid}.avro
```

* Cross-attempt uniqueness holds by construction — no counter needs to be
  threaded through retry state, and forgetting to persist a counter cannot
  reintroduce a collision.
* The per-attempt counter `n` stays attempt-local on the producer, exactly as
  today.
* Manifest lists were already attempt-qualified; this extends the same rule
  to every artifact.
* Reused artifacts keep their original attempt-qualified names: reuse
  references old paths, it never rewrites them.

The write-once rule this establishes is an invariant (section 8.4):

> **No artifact path is written more than once for the lifetime of an action,
> across all attempts.**

### Snapshot ID collision policy

The initial `snapshot_id` is collision-checked against the refreshed
transaction-local table available at the first commit attempt.

It is then preserved.

Regenerating the snapshot ID after metadata has already been written would invalidate ID-dependent metadata and break artifact reuse.

The normal table metadata checks remain the final protection against the vanishingly unlikely case where another writer independently commits the same random ID after initialization: `add_snapshot` rejects duplicate IDs outright.

---

## 2.4 Merging retry state

Merging actions require richer state:

```rust
pub(crate) struct MergeRetryState {
    snapshot: SnapshotActionState,
    source: SourceCache,
    derived: DerivedCache,
    artifacts: ArtifactTracker,
}

impl TransactionActionState for MergeRetryState {
    fn snapshot_id(&self) -> Option<i64> {
        self.snapshot.snapshot_id()
    }
}
```

These fields represent four different responsibilities.

---

### 2.4.1 `SnapshotActionState`: logical identity

Answers:

> Which logical snapshot-producing action is this?

It contains stable snapshot identity and the attempt counter.

---

### 2.4.2 `SourceCache`: immutable metadata already materialized

`SourceCache` records immutable committed metadata that this action has already read.

Conceptually:

```rust
struct SourceCache {
    processed_snapshots: HashMap<i64, ProcessedSnapshot>,
    manifests: HashMap<String, Manifest>,
}

struct ProcessedSnapshot {
    /// Integrity guard: identity of the committed snapshot this entry
    /// was built from (see below).
    fingerprint: SnapshotFingerprint,
    // ancestry information, manifest-list entries, ...
}

struct SnapshotFingerprint {
    parent_snapshot_id: Option<i64>,
    sequence_number: i64,
    manifest_list: String,
}
```

It answers:

> What immutable committed inputs have I already materialized?

Examples include:

* snapshot ancestry information;
* manifest lists for historical snapshots;
* manifests already read and decoded.

Because committed snapshots and manifests are immutable, these values can normally survive rebases.

The primary benefits are:

* incremental history discovery;
* avoiding repeated object-store reads;
* avoiding repeated manifest decoding.

**Fingerprint integrity guard.** The cache is keyed by snapshot ID, but
snapshot IDs are only unique within one metadata lineage. Each cached entry
therefore carries a `SnapshotFingerprint`; every history re-walk verifies the
fingerprint of any already-cached snapshot it encounters, and a mismatch is a
hard error ("snapshot changed while retrying a commit") rather than silent
reuse of stale contents. The comparison is three fields — cheap
defense-in-depth that converts a near-impossible corruption (ID reuse after
expiry, or cache poisoning by a bug) into a clean failure.

Note that the fingerprint guards *cached committed history*; it is not the
mechanism that resolves ambiguous commit outcomes (section 6.6).

**Memory.** Caching decoded manifests is a memory commitment beyond Java
parity — Java caches filter *results*, never decoded source manifests, and
wide tables have manifests with tens of thousands of large entries. The
implementation must either bound this cache (by entry count or estimated
size) or explicitly document acceptance of unbounded growth for the retry
window. Eviction policy details are deferred (section 10, Phase 6), the
bound is not.

---

### 2.4.3 `DerivedCache`: reusable deterministic work

`DerivedCache` records completed deterministic work whose semantic dependencies remain stable.

Conceptually:

```rust
struct DerivedCache {
    validation: ValidationCache,
    filtering: ManifestFilterCache,
    added_manifests: AddedManifestCache,
}
```

It answers:

> What work have I already completed that is still valid?

Examples:

```text
snapshot S11 + validation context
    -> no conflict

manifest M1 + removal intent
    -> rewritten manifest M1'

manifest M2 + removal intent
    -> unchanged

added files [F1, F2]
    -> manifest M3
```

Not all derived results correspond to physical files.

For example:

```rust
enum FilterResult {
    Unchanged,

    Rewritten {
        manifest: ManifestFile,
        removed_files: Vec<String>,
    },
}
```

`Unchanged` is useful retry state even though no artifact was produced.

`DerivedCache` therefore represents computation reuse, not artifact ownership.

---

### 2.4.4 `ArtifactTracker`: physical metadata ownership

`ArtifactTracker` records every physical metadata artifact successfully written by the action.

Conceptually:

```rust
struct ArtifactTracker {
    written: HashSet<String>,
}
```

It answers:

> What physical metadata files did this logical action create?

Examples:

```text
rewritten manifest M1'
added-file manifest M3
manifest list L1
manifest list L2
```

The tracker is deliberately independent of `DerivedCache`.

A physical artifact may be reusable or attempt-only:

```text
M1'   reusable + owned
M3    reusable + owned
L1    attempt-only + owned
```

Conversely, a derived cache result may have no artifact at all:

```text
M2 -> Unchanged
S11 validation -> NoConflict
```

Every successfully completed metadata write owned by the action must be recorded exactly once.

Failed writes are not recorded.

---

# 3. State Validity and Cache Semantics

## 3.1 Dependency-based reuse

Retry state is reusable based on semantic dependencies, not simply on whether an operation is "base-dependent" in the abstract.

The canonical rule is:

> **A cached result may be reused only while every semantic input that produced it remains unchanged.**

| State                                   | Semantic dependency                                     | Rebase behavior                    |
| --------------------------------------- | ------------------------------------------------------- | ---------------------------------- |
| Action configuration                    | logical action                                          | preserve                           |
| `snapshot_id` / `commit_uuid`           | logical action identity                                 | preserve                           |
| `attempt` counter                       | count of `commit()` invocations                         | monotonically increases; never reused |
| Processed snapshot                      | immutable committed snapshot (fingerprint-checked)      | preserve                           |
| Loaded manifest                         | immutable manifest                                      | preserve                           |
| Historical validation result            | snapshot + validation context                           | preserve if context unchanged      |
| Manifest filter result                  | source manifest + immutable filter/removal intent       | preserve if dependencies unchanged |
| Added-file manifest                     | added files + stable identity + inheritance-based attempt fields | preserve                  |
| Complete resulting manifest set         | current base                                            | rebuild                            |
| Parent snapshot                         | current base                                            | recompute                          |
| Sequence number                         | current base                                            | recompute                          |
| Row-ID allocation                       | current base                                            | recompute                          |
| Final validation through current parent | refreshed history head                                  | recompute / extend                 |
| Manifest list                           | complete attempt result                                 | rebuild                            |
| `TableCommit`                           | exact catalog base                                      | rebuild                            |

This replaces a coarse binary distinction between all persistent state and all attempt-local state.

**The added-manifest row has an unstated-elsewhere precondition worth naming.**
Added-file manifests are reusable across attempts only because every
attempt-scoped value they would otherwise contain is inheritance-based: the
snapshot ID (V2+), the data sequence number, and the first-row-id (V3) are
written as null in the manifest and resolved through the manifest-list entry
at read time. The stable `snapshot_id` covers the one value V1 bakes in. A
future table format that embeds attempt-scoped values directly in manifests
changes this row's dependency set and disables the reuse (section 4.8).

---

## 3.2 Source cache versus derived cache

The distinction is:

> **Source cache avoids repeating reads. Derived cache avoids repeating computation and writes.**

Example:

```text
M1.avro
   |
   | object-store read + decode
   v
SourceCache[M1] = parsed Manifest
   |
   | filter(remove A)
   v
DerivedCache[M1] = Rewritten(M1')
```

`M1` existed before this action and is immutable committed metadata.

`M1'` was produced by this action and may also be tracked by `ArtifactTracker`.

---

## 3.3 Incremental cache use does not weaken correctness

Cache reuse is permitted only as an optimization of already-established semantic work.

It must not prevent an attempt from covering newly introduced table history or newly introduced manifests.

For validation in particular:

> **Every attempt must establish validation coverage through the refreshed parent. Previously validated immutable history may be reused when its validation context is unchanged; newly introduced history must be validated before snapshot metadata production continues.**

This does **not** require re-validating every historical snapshot.

Example:

```text
starting snapshot = S0

attempt 1:
S0 -> S1 -> S2 -> S3

validation:
S1 PASS
S2 PASS
S3 PASS
```

The retry state may remember:

```text
(S1, validation-context C) -> PASS
(S2, validation-context C) -> PASS
(S3, validation-context C) -> PASS
```

A concurrent writer then commits:

```text
S3 -> S4 -> S5
```

After refresh:

```text
required history:
S1 S2 S3 S4 S5
```

If validation context `C` is unchanged:

```text
S1 cached PASS
S2 cached PASS
S3 cached PASS
S4 validate now
S5 validate now
```

The result is complete validation coverage through `S5` without repeating immutable historical work.

If the validation context changes, the cached historical results are no longer valid.

---

## 3.4 Validation context

A validation result is not conceptually keyed only by snapshot ID.

Its semantic dependencies may include:

* validation kind;
* referenced data files;
* conflict detection filter;
* isolation level;
* bound expression;
* schema context;
* partition-spec projection context;
* other operation-specific validation parameters.

Conceptually:

```text
(snapshot, validation_context)
    -> validation result / evidence
```

Most of these inputs are immutable action intent, so a reader may assume
"context unchanged" is trivially true. It is not, and the concrete
invalidation trigger deserves naming:

> **A rebase that changes the current schema or the relevant partition specs
> re-binds the conflict-detection filter. Validation results cached under the
> old binding are invalid.**

A bound expression and its partition projections depend on the schema and
specs they were bound against. The minimal safe implementation guards the
validation cache on `(current_schema_id, relevant partition-spec ids)`: if a
refresh changes either, cached historical validation is dropped and coverage
is re-established. The implementation may use a simpler key when action-scoped
state makes some dependencies implicit, but correctness must follow the full
semantic dependency model.

---

# 4. Snapshot-Producing Action Execution

## 4.1 Responsibility split

`MergingSnapshotProducer` is attempt-scoped.

It does not **own** retry-persistent state. It borrows retry state from the transaction action entry.

```rust
struct MergingSnapshotProducer<'a> {
    table: &'a Table,

    // Immutable action intent.
    added_files: &'a [DataFile],
    deleted_files: &'a [DataFile],
    starting_snapshot_id: Option<i64>,
    conflict_detection_filter: Option<&'a Expression>,

    // Retry-persistent state owned by ActionEntry.
    retry: &'a mut MergeRetryState,

    // Generic finalization layer.
    producer: SnapshotProducer<'a>,
}
```

The execution object exists only for one refreshed base.

The retry state may outlive many such execution objects.

---

## 4.2 Attempt context

Each attempt derives context from the current transaction-local table:

```rust
struct SnapshotAttemptContext {
    parent_snapshot_id: Option<i64>,
    sequence_number: i64,
    first_row_id: i64,
}
```

These values are never persisted across a rebase.

A new base implies a new attempt context.

---

## 4.3 Conflict validation

Validation is part of snapshot-producing execution.

The pipeline is:

```text
starting snapshot
      |
      v
discover required history through current parent
      |
      +-- SourceCache:
      |      reuse already-materialized snapshots/manifests
      |      (fingerprint-checked on every re-walk)
      |      load only unseen history
      |
      +-- DerivedCache:
      |      reuse historical validation results when
      |      validation context is unchanged (§3.4 guard)
      |
      v
validate newly introduced history
      |
      +-- conflict -> fail
      |
      +-- pass -> continue snapshot production
```

Validation must complete before the attempt writes new metadata artifacts.

A retry may therefore reuse validation work, but it cannot skip validation coverage through the refreshed parent.

---

## 4.4 Manifest transformation

After validation passes, the merging producer transforms the current snapshot's manifests.

For each source manifest:

```text
current source manifest M
        |
        v
DerivedCache contains FilterResult(M)?
       / \
     yes  no
      |    |
      |    v
      |  obtain source manifest from SourceCache
      |    |
      |  evaluate removal/filter logic
      |    |
      |   / \
      | unchanged rewritten
      |    |       |
      |    |       v
      |    |     write M' (attempt-qualified path, §2.3)
      |    |       |
      |    |     ArtifactTracker.record(M')
      |    |
      |  cache FilterResult
      |
      v
reuse result
```

A cache entry for a source manifest that disappears from the current base does not need eager invalidation.

The current attempt starts from the current base's manifest set. An old cache entry simply becomes unreachable.

---

## 4.5 Added manifests

Added data and delete files are also deterministic inputs.

For example:

```text
added files
    |
    v
DerivedCache hit?
   / \
 yes  no
  |    |
reuse  write manifest (attempt-qualified path)
       |
       v
ArtifactTracker.record(...)
       |
       v
cache result
```

A manifest may therefore be written once and reused across several catalog attempts.

Stable `snapshot_id` and `commit_uuid` make this possible, together with the
inheritance precondition of section 3.1: sequence numbers, first-row-ids, and
(V2+) snapshot IDs are resolved through the manifest-list entry, so nothing
attempt-scoped is baked into the reused file.

---

## 4.6 Manifest organization

The complete transformed manifest set is derived from the current base and must be reconstructed after a rebase.

For example:

```text
attempt 1:
[M1', M2, M3]

rebase adds M4:

attempt 2:
[M1', M2, M3, M4]
```

The grouping or bin-packing decision may therefore change.

The initial implementation treats organization as attempt-local.

Individual deterministic merge outputs may later be cached if their complete dependency set can be expressed safely.

---

## 4.7 Summary and finalization

After validation and manifest transformation:

```text
validated/transformed manifests
        |
        v
organize
        |
        v
summarize actual result
        |
        v
SnapshotProducer::finalize
```

`SnapshotProducer` remains a thin generic layer.

```rust
struct ProducedSnapshot {
    operation: Operation,
    manifests: Vec<ManifestFile>,
    summary: SnapshotSummary,
}

impl<'a> SnapshotProducer<'a> {
    fn attempt_context(&self) -> Result<SnapshotAttemptContext>;

    async fn finalize(
        self,
        produced: ProducedSnapshot,
    ) -> Result<ActionCommit>;
}
```

`SnapshotProducer` is responsible for:

* deriving attempt context;
* writing the manifest list / metadata root;
* building the `Snapshot`;
* producing `TableUpdate`s;
* producing `TableRequirement`s.

It receives identity — `snapshot_id`, `commit_uuid`, `attempt` — from the
action state and never generates it. Its per-attempt manifest counter stays
attempt-local; attempt-qualified paths make that safe (section 2.3).

It does not understand rewrite, overwrite, row-delta, or replace-partitions semantics.

Those belong to the merging layer.

---

## 4.8 Table-format seams

Retry state ownership is independent of a particular metadata format.

A future table-format version may change:

1. metadata generation for added/removed content;
2. metadata loading/filtering;
3. organization and snapshot finalization.

It should not require redesigning:

* `Transaction`;
* `TransactionActionState`;
* retry-state ownership;
* stable snapshot identity;
* retry/rebase semantics;
* dependency-based cache validity;
* artifact ownership.

`SourceCache` and `DerivedCache` remain private implementation details of the action state so their physical representations may evolve with the table format.

One format-sensitive dependency deserves restating here: added-manifest reuse
(section 3.1) holds only while attempt-scoped values are inheritance-based. A
format that bakes sequence numbers, row IDs, or snapshot IDs into manifests
moves that reuse behind seam 1 and disables the corresponding cache.

---

# 5. Transaction Retry and Replay

The transaction owns catalog refresh and action ordering.

Individual actions do not independently refresh the catalog.

The execution loop is:

```text
refresh catalog table
       |
       v
transaction-local current_table (rebased if the base moved)
       |
       v
for entry in order:
    entry.commit(&current_table)
       |    (lazy state init on first attempt; attempt += 1 before any I/O)
       v
    apply ActionCommit locally
       |
       v
    next entry observes previous entry's result
       |
       v
build final TableCommit
       |
       v
catalog.update_table(...)
```

On a retryable catalog conflict:

```text
catalog conflict
      |
      v
refresh
      |
      v
replay entries in order
      |
      v
each entry keeps its retry state
but receives a refreshed transaction-local table
```

On an operation-level semantic conflict:

```text
validation failure
      |
      v
terminal transaction failure (6.5, 6.7)
```

Retrying a catalog conflict therefore means:

> refresh, replay, extend/re-establish validation coverage, rebuild base-dependent state, and try catalog submission again.

It does **not** mean blindly resubmitting stale metadata.

Ambiguous catalog responses — the request was sent but the outcome is
unknown, so the commit may have landed — are a property of the artifact
lifecycle, not of replay: section 6.6 defines the minimum safe behavior, and
richer resolution is deliberately deferred (section 10, Phase 6).

---

# 6. Artifact Lifecycle

## 6.1 Ownership

Automatic cleanup is limited to metadata artifacts whose ownership belongs to the snapshot-producing layer.

Examples:

* generated manifests;
* rewritten manifests;
* generated delete manifests;
* manifest lists;
* future snapshot metadata artifacts written by this layer.

A `DataFile` or `DeleteFile` supplied by a caller is not automatically owned by the transaction merely because it is staged for addition.

The layer that created the artifact must establish ownership before automatic cleanup is allowed.

---

## 6.2 Recording artifacts

An artifact enters `ArtifactTracker` only after the physical write completes successfully.

```text
begin write
   |
   +-- failure -> do not record
   |
   +-- success -> ArtifactTracker.record(path)
```

This ensures retry state never claims ownership of an artifact whose write may be incomplete.

---

## 6.3 Retryable catalog conflict

A catalog conflict establishes that the current submission did not commit.

However, artifacts should not be eagerly deleted.

Some may be reusable:

```text
rewritten manifest
added-file manifest
```

Some may be attempt-only and already stale:

```text
manifest list for the previous parent
```

Both may remain tracked until a terminal outcome.

This keeps retry logic simple and prevents cleanup from racing reuse.

---

## 6.4 Confirmed success

After commit success, cleanup is based on reachability from committed metadata.

Conceptually:

```text
all metadata artifacts written by this action
        -
metadata artifacts reachable from committed snapshots
        =
safe-to-delete artifacts
```

Example:

```text
written:
    M1'
    M3
    L1
    L2

committed snapshot references:
    L2
    M1'
    M3

delete:
    L1
```

Cleanup should be based on what the committed metadata actually references, not merely on which client-side attempt is believed to have succeeded.

For a multi-action transaction, reachability must account for all snapshots committed by that transaction, not only the final current snapshot.

---

## 6.5 Confirmed terminal failure

If the transaction definitively did not commit and will not retry, no action-owned generated metadata can be reachable from a newly committed snapshot.

All action-owned artifacts may therefore be cleaned up.

```text
reachable = {}
delete all tracked action-owned metadata artifacts
```

---

## 6.6 Unknown outcome

An ambiguous catalog response is different.

For example:

```text
send commit
    |
server commits
    |
network timeout
    |
client receives error
```

The generated metadata may already be live.

Therefore:

> **Unknown outcome deletes nothing.**

The stable `snapshot_id` provides a durable identifier to resolve against: if
a later refresh shows it in table metadata, the commit landed and the success
path (6.4) applies. Absence alone proves nothing — the request may still be
applied server-side — so an unresolved unknown surfaces to the caller with
all artifacts intact, and the transaction drops its state without invoking
cleanup. The tracked artifacts become ordinary orphan candidates
(section 6.8).

Because identity is action state, this resolution exists only within one
`Transaction` value. Richer ambiguity handling — automatic resolution,
transport-level idempotency — is future work (section 10, Phase 6).

---

## 6.7 Cleanup driver

Cleanup crosses the type-erasure boundary through the entry API
(`DynTransactionActionEntry::cleanup`, section 2.2), and the **transaction**
drives it — actions never decide unilaterally when a terminal outcome has
been reached.

```text
terminal outcome established (6.4 - 6.6)
        |
        v
for EVERY entry (not only the failed one):
    entry.cleanup(outcome)
        |
        +-- Success(committed metadata) -> reachability cleanup (6.4)
        |
        +-- Failure -> delete all tracked artifacts (6.5)

unknown outcome -> cleanup is never invoked (6.6)
```

Iterating every entry matters: when entry B fails a semantic validation on a
later replay, entry A may hold artifacts from earlier successful replays.
Terminal failure of the transaction cleans A's artifacts too.

---

## 6.8 Orphan files

Process death or an abandoned transaction may leave action-owned metadata that never reaches a terminal cleanup path.

Those files are ordinary orphan metadata and must be handled by orphan-file cleanup tooling.

This RFC does not attempt to make metadata leakage impossible.

It guarantees instead:

* no eager deletion of possibly committed metadata;
* explicit ownership of generated metadata;
* best-effort terminal cleanup;
* safe retry reuse.

---

# 7. Worked Retry Trace

Consider a rewrite action:

```text
remove A
add D
```

Initial table:

```text
S10

M1 = [A, B]
M2 = [C]
```

The action is applied. Apply stores the entry with uninitialized state — no
table access, no I/O.

---

## Attempt 1 — base S10

State is initialized lazily at this first commit:

```text
snapshot_id = 91827364    collision-checked against refreshed S10
commit_uuid = 8ac3f9...   generated once
attempt     = 1           bumped before any I/O
```

These identity values remain stable for the life of the action.

Attempt context:

```text
parent = S10
sequence_number = 11
```

### Source cache

```text
processed snapshots:
    S10 (fingerprint recorded)

loaded manifests:
    M1 -> [A, B]
    M2 -> [C]
```

### Validation

Required history is validated through `S10`.

No conflict is found.

A validation result/evidence may be retained in `DerivedCache`.

### Manifest filtering

```text
M1 + remove A
    -> Rewritten(M1')

M2 + remove A
    -> Unchanged
```

`M1'` is written to `8ac3f9...-1-m0.avro`.

After the write completes:

```text
ArtifactTracker:
    8ac3f9...-1-m0.avro   (M1')
```

Derived state:

```text
filter[M1] -> Rewritten(M1')
filter[M2] -> Unchanged
```

### Added files

`D` is written into new manifest `M3` at `8ac3f9...-1-m1.avro`.

```text
DerivedCache:
    added-data -> M3

ArtifactTracker:
    M1' (…-1-m0)
    M3  (…-1-m1)
```

### Finalization

The attempt builds:

```text
resulting manifests:
    M1'
    M2
    M3

manifest list:
    L1 = snap-91827364-1-8ac3f9....avro
```

After `L1` is written:

```text
ArtifactTracker:
    M1'
    M3
    L1
```

Catalog submission fails cleanly because another writer committed:

```text
S10 -> S11
```

The concurrent commit appends:

```text
M4 = [E]
```

---

## Attempt 2 — base S11

Refresh observes `S11` — a rebase.

Stable action state:

```text
snapshot_id = 91827364    UNCHANGED
commit_uuid = 8ac3f9...   UNCHANGED
attempt     = 2           bumped before any I/O
```

New attempt context:

```text
parent = S11
sequence_number = 12
```

### Incremental source discovery

`S10`, `M1`, and `M2` are already materialized; `S10`'s fingerprint is
re-verified during the history walk.

Only newly introduced history is processed:

```text
S11
M4
```

No repeated read of `M1` or `M2` is required.

### Incremental validation

Previously validated history remains valid because the validation context is
unchanged (`S11` changed neither the current schema nor the relevant specs).

```text
old history -> cached validation
S11         -> validate now
```

If `S11` conflicts, the attempt fails before writing new metadata.

Assume it does not conflict.

### Manifest filtering

Current manifests are:

```text
M1
M2
M4
```

Cache lookup:

```text
M1 -> cached Rewritten(M1')
M2 -> cached Unchanged
M4 -> no cache
```

Only `M4` is newly evaluated:

```text
M4 -> Unchanged
```

No rewrite of `M1'` is required.

Had `M4` required a rewrite, it would land at `8ac3f9...-2-m0.avro` — the
per-attempt counter restarts at zero, yet nothing collides with attempt 1's
`...-1-m0.avro`, because every path embeds its attempt. This is the
write-once rule of section 2.3 working by construction.

### Added manifests

`M3` is reused from `DerivedCache` — referenced by its original
attempt-1 path, never rewritten.

### Finalization

The new complete result is:

```text
M1'
M2
M4
M3
```

A new attempt-local manifest list is written:

```text
L2 = snap-91827364-2-8ac3f9....avro
```

Tracker now contains:

```text
M1'
M3
L1
L2
```

Catalog submission succeeds.

---

## Terminal cleanup

Committed metadata references:

```text
L2
M1'
M3
```

`L1` is not reachable.

Therefore:

```text
keep:
    M1'
    M3
    L2

delete:
    L1
```

The example demonstrates:

* lazy, collision-checked identity initialization;
* stable action identity across attempts;
* attempt-qualified, write-once artifact paths;
* incremental history discovery with fingerprint verification;
* cached historical validation;
* manifest source reuse;
* manifest filtering reuse;
* generated manifest reuse;
* attempt-local finalization;
* reachability-based cleanup.

---

# 8. Invariants

The invariants are grouped by responsibility.

## 8.1 Retry-state invariants

### Stable intent

Action configuration does not change across attempts.

### Stable identity

A logical snapshot-producing action preserves its `snapshot_id` and `commit_uuid` across retries and rebases.

### Lazy, checked initialization

Action state is initialized exactly once, at the first commit attempt,
against the refreshed transaction-local table. A cloned transaction starts
with uninitialized state and acquires fresh identity.

### Dependency-safe reuse

A cached value is reused only while all semantic inputs that produced it remain unchanged.

### Cache integrity is verified

A cached processed snapshot whose fingerprint no longer matches the metadata
it is keyed by is a hard error, never silent reuse.

---

## 8.2 Validation invariants

### Complete validation coverage

Every attempt establishes the required conflict-validation coverage from its starting boundary through the refreshed parent.

Previously validated immutable history may be reused when validation context is unchanged.

### New history is validated

Snapshots newly introduced by a refresh are validated before snapshot metadata production continues.

### Context changes drop cached validation

A rebase that changes the current schema or the relevant partition specs
invalidates cached historical validation results (section 3.4).

### Validation precedes new metadata I/O

An attempt that discovers a semantic conflict does not write new snapshot metadata for that attempt.

---

## 8.3 Attempt invariants

### No stale base-derived state

A parent snapshot, sequence number, row-ID allocation, complete manifest set, manifest list, or `TableCommit` built against base `N` is not reused against different base `M`.

### Attempt monotonicity

The `attempt` counter increments at the start of every `commit()` invocation,
before any I/O, including attempts that abort early. No two attempts of one
action share a number.

### Ordering

Action B observes the transaction-local result of action A on every replay.

Individual actions do not refresh the catalog independently.

---

## 8.4 Artifact invariants

### Record after completion

A metadata artifact enters `ArtifactTracker` only after its write completes successfully.

### Complete ownership tracking

Every action-owned metadata artifact successfully written during the retry lifecycle is recorded exactly once.

### Write-once paths

Every generated artifact path embeds the attempt that wrote it, and no path
is written more than once for the lifetime of an action — a prior attempt's
artifact may be reused by the current attempt or referenced by a commit whose
outcome is unknown.

### No eager retry cleanup

A retryable catalog conflict does not immediately delete action-owned metadata.

### Reachability-based success cleanup

After confirmed success, an action-owned artifact is deleted only if it is unreachable from the committed metadata produced by the transaction.

### Terminal cleanup covers every entry

At a terminal outcome, the transaction drives `cleanup` across all entries,
including entries whose own attempts succeeded before another entry failed.

### Unknown outcome deletes nothing

No action-owned metadata is cleaned up while commit state is ambiguous, and
absence of the stable `snapshot_id` from refreshed metadata is never treated
as proof the commit will not land.

---

# 9. Rationale and Alternatives

## 9.1 Explicit RetryState versus a long-lived producer

Java naturally retains retry state because one long-lived producer object survives repeated calls to `apply()`.

That object also owns nested stateful manifest filter/merge managers.

Rust does not need to couple retry lifetime to execution-object lifetime.

This RFC instead uses:

```text
ActionEntry
    owns RetryState

attempt
    creates MergingSnapshotProducer
        borrowing RetryState
```

This makes state lifetime explicit and keeps the producer attempt-scoped.

---

## 9.2 `MergingSnapshotProducer` versus `SnapshotChanges`

An alternative architecture normalizes action semantics into a generic snapshot change set:

```text
Action
    ↓
SnapshotChanges
    ↓
generic SnapshotCommitBuilder
```

This can provide a unified snapshot mutation engine and naturally centralize retry caching.

This RFC instead keeps merging execution as a first-class abstraction:

```text
Action
    ↓
MergingSnapshotProducer
    ↓
SnapshotProducer::finalize
```

The reason is semantic layering.

Rewrite, overwrite, row-delta, delete, and replace-partitions all require a common process involving:

* conflict validation;
* current-manifest processing;
* removals;
* additions;
* manifest organization;
* summary generation.

`MergingSnapshotProducer` owns this attempt-level orchestration while `RetryState` owns reusable cross-attempt work.

The retry-state design itself is intentionally independent of this choice. If implementation experience demonstrates that a `SnapshotChanges` representation is a better execution boundary, the same `SnapshotActionState`, `SourceCache`, `DerivedCache`, and `ArtifactTracker` model can still be used.

---

## 9.3 Typed entry versus independent erased action/state

This RFC erases the complete typed `Action + State` entry rather than independently erasing the two values and reconnecting them through runtime downcasts.

The compile-time association removes a runtime mismatch/panic path and allows each action to define its own state without a central state enum.

Lazy initialization is part of the same decision: eager `new_state` at apply
time would force `Transaction::clone` to choose between sharing state
(unacceptable — shared identity and shared artifact ownership) and panicking
inside an infallible trait method (since `new_state` is fallible and
table-dependent). Deferring initialization to the first commit makes `fork`
total, moves the collision check onto fresher metadata, and matches Java's
lazily generated `snapshotId()`.

---

## 9.4 Dependency-based caching versus recompute-all

Recomputing every operation on every retry is simpler but can repeatedly:

* fetch immutable metadata;
* decode manifests;
* scan historical snapshots;
* evaluate identical filters;
* rewrite identical metadata artifacts.

Committed metadata is immutable, and action intent is immutable.

The retry architecture should therefore allow deterministic sub-results to survive rebases when their complete semantic dependency set is unchanged.

---

## 9.5 Separate ArtifactTracker versus deriving ownership from caches

`DerivedCache` and artifact ownership overlap for some values but represent different concerns.

For example:

```text
M1 -> Rewritten(M1')
```

is both a reusable computation and a physical artifact.

But:

```text
M2 -> Unchanged
```

is reusable without an artifact, while:

```text
manifest list L1
```

is an owned artifact that may not be reusable after a rebase.

Cleanup should therefore not need to understand every cache's internal representation.

`ArtifactTracker` provides one uniform ownership boundary for all generated metadata.

---

## 9.6 Attempt-qualified paths versus a monotonic manifest counter

Java keeps manifest paths unique across attempts with a monotonic
`manifestCount` on the long-lived producer:

```text
Java:      {commitUUID}-m{N}.avro      N monotonic across attempts
This RFC:  {commit_uuid}-{attempt}-m{n}.avro    n attempt-local
```

Porting the monotonic counter into retry state works, but its uniqueness is
enforced by discipline: every code path that writes a manifest must remember
to persist and thread the counter, and forgetting once reintroduces a silent
collision. Attempt-qualified paths make uniqueness structural — no path can
collide across attempts as long as `attempt` increments (Invariant 8.3) — and
the per-attempt counter stays attempt-local exactly as on current `main`.

The divergence from Java's manifest file naming is cosmetic: manifests are
located through manifest-list entries, paths are opaque to readers and orphan
tooling, and Java itself already attempt-qualifies manifest-*list* names. The
attempt segment also aids forensics — a listing shows which attempt wrote
each file.

---

# 10. Implementation Plan

## Phase 1 — Stateful Transaction Actions and Retry State

Introduce the transaction state foundation:

* `TransactionActionState` (with the `snapshot_id()` accessor);
* `TransactionAction::State` + `new_state`;
* typed `TransactionActionEntry<A>` with `Option<A::State>` and lazy
  initialization at first commit;
* erased `DynTransactionActionEntry` with `commit`, `snapshot_id`,
  `fork`, and a `cleanup` stub;
* manual `Transaction: Clone` via `fork`;
* stable `SnapshotActionState` including the `attempt` counter;
* `MergeRetryState`;
* empty/skeletal `SourceCache` (with `SnapshotFingerprint` type);
* empty/skeletal `DerivedCache`;
* `ArtifactTracker`.

Migrate existing transaction actions to the new interface.

FastAppend should use stable `SnapshotActionState`.

The objective is lifecycle correctness, not cache efficiency.

### Tests

* state survives catalog retry;
* action state is not shared between independent transactions;
* state initializes exactly once, at first commit, against the
  transaction-local table (not at apply time);
* a cloned transaction starts uninitialized and acquires fresh identity;
* `snapshot_id` remains stable;
* `commit_uuid` remains stable;
* `attempt` increments per commit invocation, before any I/O;
* replay ordering remains unchanged;
* public API baseline regenerated deliberately (Clone retained; auto-trait
  changes reviewed).

---

## Phase 2 — Retry-Aware Snapshot-Producing Execution

Implement the end-to-end path from:

```text
refreshed transaction-local table
+
RetryState
+
action intent
    ↓
snapshot-producing execution
    ↓
ActionCommit
```

Introduce/refactor:

* attempt context;
* conflict-validation boundary;
* attempt-scoped `MergingSnapshotProducer`;
* current-manifest filtering baseline;
* added-manifest generation baseline;
* attempt-qualified artifact naming (`{uuid}-{attempt}-m{n}`, manifest list
  unchanged in shape);
* manifest organization;
* summary generation;
* thin `SnapshotProducer::finalize`.

At this phase, execution may recompute most work on every attempt.

The goal is correctness of:

* retry;
* rebase;
* validation;
* attempt-local state regeneration;
* final snapshot construction.

### Tests

* forced catalog conflict and successful rebase;
* parent/sequence/row IDs recomputed;
* semantic conflict stops retry;
* validation occurs before metadata writing;
* action ordering through rebase;
* stable identity across attempts;
* artifact paths unique across attempts (force N attempts, assert all
  written paths distinct, reused artifacts referenced by original paths).

---

## Phase 3 — Incremental Retry Caching

Populate the retry caches and make them effective.

### Source caching

Add:

* processed-snapshot cache with fingerprint verification on every re-walk;
* incremental history discovery;
* parsed manifest cache, bounded (or with the unbounded window explicitly
  accepted and documented).

A retry that advances from `S3` to `S5` should process only newly introduced `S4` and `S5`.

### Validation caching

Add reusable historical validation results/evidence keyed by a safe validation context, guarded on `(current_schema_id, relevant partition-spec ids)`.

A retry may reuse validation of immutable history and validate only newly introduced snapshots.

### Manifest filtering caching

Cache deterministic filter results:

```text
M1 -> Rewritten(M1')
M2 -> Unchanged
```

A rebase that still references `M1` and `M2` should reuse those results.

### Generated-manifest caching

Reuse:

* added data manifests;
* added delete manifests;
* other deterministic metadata outputs whose dependencies are unchanged.

### Artifact tracking

Connect every metadata write to `ArtifactTracker`.

Implement the terminal cleanup driver (`cleanup` across all entries),
reachability cleanup, and unknown-outcome safety.

### Tests

* no repeated load of historical snapshots;
* no repeated load/decode of cached manifests;
* fingerprint mismatch on a cached snapshot fails cleanly, never reuses;
* historical validation reused safely;
* a rebase that changes the current schema or relevant specs drops cached
  validation;
* new history always validated;
* filtered manifest reused after non-conflicting append;
* stale source cache entries are harmless when source manifests disappear;
* added manifests reused (referenced by original attempt-qualified paths);
* every successful metadata write is tracked;
* unknown outcome deletes nothing;
* committed reachability determines cleanup, including the case where an
  earlier attempt's manifest list is the one that committed;
* terminal failure of one entry cleans other entries' artifacts too.

---

## Phase 4 — RewriteFiles End to End

Implement `RewriteFilesAction` as the first full consumer of the new architecture.

RewriteFiles exercises:

* explicit files to remove;
* explicit files to add;
* file-existence validation;
* concurrent delete validation;
* manifest filtering;
* added-manifest generation;
* retry/rebase;
* cache reuse;
* artifact cleanup.

This phase is the proving ground for the complete retry-state design.

### Success criterion

RewriteFiles must not require transaction-specific special cases outside the abstractions introduced in Phases 1–3.

---

## Phase 5 — Remaining Merging Actions

Implement the remaining merging actions using the same architecture:

* DeleteFiles;
* OverwriteFiles;
* RowDelta;
* ReplacePartitions.

Each action primarily defines:

* immutable intent;
* operation-specific validation policy;
* operation-specific removal/addition semantics.

### Success criterion

Adding these actions must not require redesigning:

* transaction replay;
* retry-state ownership;
* stable snapshot identity;
* source caching;
* derived caching;
* artifact tracking.

---

## Phase 6 — Future Improvements

Potential optimizations that are deliberately not required by the initial architecture.

### Same-base whole-attempt caching

If catalog submission fails but refresh returns the exact same base:

```text
attempt built against S10
catalog transient failure
refresh -> S10
```

a future implementation may retain:

```text
CachedAttempt {
    ProducedSnapshot,
    TableCommit,
    ...
}
```

and resubmit without rebuilding the attempt.

This is a stronger optimization than source/derived caching and is not part of the initial implementation.

### Commit-ambiguity handling

Richer treatment of ambiguous catalog responses beyond the minimum of
section 6.6: an unknown-outcome error class, automatic post-refresh detection
of a landed commit via the stable `snapshot_id`, expected-state comparison
for metadata-only transactions, optimistic replay, and adoption of the REST
`Idempotency-Key` header in the REST catalog crate. The retry-state design
already carries everything this needs — stable identity and tracked
artifacts — so it can land later without touching the architecture in this
RFC.

### Additional future work

* caching deterministic manifest-merge outputs;
* more aggressive predicate-evaluation caching;
* cache eviction policy refinement and instrumentation;
* parallel action replay where dependencies permit;
* cross-process retry state;
* table-format-specific cache implementations.

---

# 11. Summary

The transaction retry model is built around four responsibilities:

```text
MergeRetryState

├── SnapshotActionState
│      logical identity: snapshot_id, commit_uuid, attempt
│
├── SourceCache
│      immutable committed metadata already read
│      (fingerprint-verified on every re-walk)
│
├── DerivedCache
│      deterministic work already completed
│
└── ArtifactTracker
       physical metadata written by this action
```

An attempt-scoped `MergingSnapshotProducer` borrows this state and executes:

```text
derive attempt context
        ↓
establish validation coverage
        ↓
process current manifests using caches
        ↓
reuse/write added manifests (attempt-qualified, write-once paths)
        ↓
organize and summarize
        ↓
SnapshotProducer::finalize
        ↓
ActionCommit
```

The transaction owns the loop around it: refresh, replay, submit. Cleanup
runs only at terminal outcomes, driven by reachability from committed
metadata.

A catalog conflict triggers refresh and replay.

The action keeps safe retry-persistent state while all values tied to the old base are rebuilt.

The governing rule is not simply "persistent versus attempt-local":

> **Completed work survives a retry or rebase exactly when the semantic dependencies that produced it remain unchanged.**
