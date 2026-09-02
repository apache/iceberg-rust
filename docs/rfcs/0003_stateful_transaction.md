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

**Scope:** Transaction/action lifecycle, retry-persistent state, retry and rebase semantics, conflict validation, incremental metadata reuse, snapshot-producing execution, and generated-metadata cleanup.

---

# 1. Motivation

The current transaction implementation replays actions after a catalog conflict, but an action has no persistent mutable state across attempts. This is sufficient for simple metadata updates, but merging snapshot operations such as rewrite-files, overwrite, delete, row-delta, and replace-partitions need to retain state across replay.

Without persistent action state, a retry cannot safely or efficiently retain:

- stable snapshot identity;
- already-processed immutable metadata;
- already-completed validation and manifest work;
- generated metadata that may be reusable;
- ownership information required to clean up uncommitted metadata.

This RFC introduces explicit per-action retry state and defines how snapshot-producing actions use it during retry and rebase.

## 1.1 Goals

This RFC aims to provide:

- persistent state owned by each applied transaction action;
- stable snapshot identity across retries;
- clear retry and rebase semantics;
- incremental validation and metadata reuse;
- a common execution path for merging snapshot actions;
- explicit ownership and cleanup of generated metadata.

## 1.2 Non-goals

The initial design does not require:

- whole-attempt caching;
- cross-process retry state;
- aggressive manifest-merge caching;
- a specific cache eviction policy beyond requiring bounded behavior where needed;
- advanced recovery from ambiguous catalog commit outcomes;
- a particular implementation for future Iceberg table formats.

---

# 2. Architecture Overview

The design separates three lifetimes:

1. **Transaction lifetime** — refreshes the catalog, replays actions in order, and submits the final `TableCommit`.
2. **Action lifetime** — owns immutable action intent and retry-persistent state.
3. **Attempt lifetime** — executes one action against one refreshed transaction-local table.

```text
Transaction
│
│ owns refresh / replay / action ordering
│
├── ActionEntry<A>
│      ├── immutable Action A
│      └── retry-persistent State A::State
│
├── ActionEntry<B>
│      ├── immutable Action B
│      └── retry-persistent State B::State
│
└── ...

                     one action attempt
                            │
                            ▼
                  MergingSnapshotProducer
                  ├── validate
                  ├── filter manifests
                  ├── add manifests
                  ├── organize
                  └── summarize
                            │
                            ▼
                    SnapshotProducer
                  ├── manifest list
                  ├── Snapshot
                  ├── TableUpdates
                  └── TableRequirements
```

The key ownership rule is:

> **`ActionEntry` owns retry-persistent state. `MergingSnapshotProducer` is attempt-scoped and only borrows that state.**

This keeps retry lifetime explicit without requiring a long-lived producer object.

## 2.1 Retry state at a glance

Merging actions use a retry state with four responsibilities:

```text
MergeRetryState
│
├── SnapshotActionState
│      stable logical identity
│
├── SourceCache
│      immutable committed inputs already materialized
│
├── DerivedCache
│      reusable deterministic work
│
└── ArtifactTracker
       physical metadata created by this action
```

The four components answer different questions:

- **SnapshotActionState:** Which logical snapshot-producing action is this?
- **SourceCache:** What immutable committed state have I already read?
- **DerivedCache:** What deterministic work have I already completed and can still reuse?
- **ArtifactTracker:** What physical metadata files did this action create?

## 2.2 Core reuse rule

Retry state is reusable based on semantic dependencies, not simply on whether it was created in a previous attempt.

> **A completed result can survive retry or rebase if every semantic dependency that produced it remains unchanged.**

At a high level:

| State | Retry / rebase behavior |
| --- | --- |
| Action intent | preserve |
| `snapshot_id` / `commit_uuid` | preserve |
| Immutable snapshot/manifest metadata already read | preserve |
| Historical validation result | preserve if validation context is unchanged |
| Per-manifest filter result | preserve if source manifest and action intent are unchanged |
| Added-file manifest | preserve if its content is independent of the refreshed base |
| Current parent / sequence / row-ID allocation | rebuild |
| Complete resulting manifest set | rebuild |
| Manifest list / `TableCommit` | rebuild on a new base |

A rebase therefore re-executes the action, but does not discard all previous work.

---

# 3. End-to-End Retry Flow

## 3.1 First attempt

The transaction refreshes the table and replays actions in order. Each entry initializes its state lazily on first use.

```text
refresh catalog table
        │
        ▼
transaction-local table
        │
        ▼
for action entry in order
        │
        ├── initialize state if needed
        ├── execute action against current transaction-local table
        └── apply ActionCommit locally
        │
        ▼
build final TableCommit
        │
        ▼
catalog.update_table(...)
```

Action B always observes the transaction-local result of Action A.

## 3.2 Catalog conflict and rebase

If catalog submission fails because the base became stale:

```text
catalog conflict
      │
      ▼
refresh newer table
      │
      ▼
replay actions in order
      │
      ├── keep retry-persistent state
      ├── extend validation to new history
      ├── reuse safe previous work
      └── rebuild state tied to the old base
      │
      ▼
submit again
```

The transaction owns refresh and replay. Individual actions never refresh the catalog independently.

## 3.3 Example: RewriteFiles across a concurrent append

Assume:

```text
S10
├── M1 = [A, B]
└── M2 = [C]

RewriteFiles:
  remove A
  add D
```

Attempt 1 computes:

```text
SourceCache:
  M1, M2

DerivedCache:
  M1 -> Rewritten(M1')
  M2 -> Unchanged
  add D -> M3

Artifacts written:
  M1'
  M3
  manifest-list L1
```

Before catalog commit succeeds, another writer appends `M4` and advances the table to `S11`.

On retry:

```text
reuse:
  previously processed history
  M1 -> M1'
  M2 -> Unchanged
  added manifest M3

new work:
  process S11 / M4
  validate newly introduced history
  filter M4

rebuild:
  complete manifest set for S11
  manifest-list L2
  TableCommit
```

If attempt 2 commits, cleanup keeps artifacts reachable from the committed snapshot and removes stale attempt artifacts such as `L1`.

This example captures the central behavior of the RFC: **reuse immutable work, rebuild base-dependent work.**

---

# 4. Transaction Action State Model

## 4.1 TransactionAction interface

Each action declares its own retry-persistent state:

```rust
pub(crate) trait TransactionActionState: Send + 'static {
    fn snapshot_id(&self) -> Option<i64> {
        None
    }
}

#[async_trait]
pub(crate) trait TransactionAction: Sync + Send + 'static {
    type State: TransactionActionState;

    fn new_state(&self, table: &Table) -> Result<Self::State>;

    async fn commit(
        &self,
        state: &mut Self::State,
        table: &Table,
    ) -> Result<ActionCommit>;
}
```

`new_state` is called once for one logical execution of the action. `commit` may be called multiple times as the transaction retries.

The action and its state are paired before type erasure:

```text
Action A + A::State
        │
        ▼
TransactionActionEntry<A>
        │
        ▼
dyn DynTransactionActionEntry
```

This preserves the `Action -> State` relationship statically and avoids independently erased action/state values that must be reconnected through runtime downcasts.

## 4.2 Lazy state initialization and cloning

State is initialized on the first commit attempt against the refreshed transaction-local table, rather than when the action is added to the transaction.

This keeps `Transaction::clone` infallible and avoids sharing retry state between independent logical executions.

A cloned transaction copies the action plan but not its execution state:

```rust
pub(crate) trait DynTransactionActionEntry: Send {
    async fn commit(&mut self, table: &Table) -> Result<ActionCommit>;

    fn snapshot_id(&self) -> Option<i64>;

    fn clone_without_state(&self) -> Box<dyn DynTransactionActionEntry>;

    async fn cleanup(&mut self, outcome: TerminalOutcome<'_>) -> Result<()>;
}
```

Conceptually:

```text
Transaction::clone()
      │
      ├── clone immutable actions
      └── discard retry state
              │
              ▼
       fresh execution identity
```

The exact type-erasure and clone implementation is shown in Appendix A.

## 4.3 Snapshot identity

Snapshot-producing actions keep a small stable identity state:

```rust
pub(crate) struct SnapshotActionState {
    snapshot_id: i64,
    commit_uuid: Uuid,
    producer_attempt: u32,
}
```

### `snapshot_id`

Identifies the logical snapshot being produced. It is generated once when the action state is initialized and remains stable across retries and rebases.

### `commit_uuid`

Namespaces metadata artifacts generated by this logical action. It is also stable across attempts so generated manifests may be reused.

### `producer_attempt`

Counts executions of this snapshot-producing action. It is **action-scoped**, not the transaction replay iteration.

It is used to make artifact paths unique across attempts:

```text
manifest:       {commit_uuid}-{producer_attempt}-m{n}.avro
manifest list:  snap-{snapshot_id}-{producer_attempt}-{commit_uuid}.avro
```

Reused artifacts retain their original paths; they are referenced, not rewritten.

---

# 5. Retry State Model

## 5.1 MergeRetryState

Merging actions compose snapshot identity, reusable caches, and artifact ownership:

```rust
pub(crate) struct MergeRetryState {
    snapshot: SnapshotActionState,
    source: SourceCache,
    derived: DerivedCache,
    artifacts: ArtifactTracker,
}
```

The separation is intentional:

```text
SourceCache      = reusable inputs
DerivedCache     = reusable work
ArtifactTracker  = cleanup ownership
```

## 5.2 SourceCache

`SourceCache` contains immutable committed metadata already materialized by this action.

Conceptually:

```rust
struct SourceCache {
    processed_snapshots: ...,
    manifests: ...,
}
```

Two kinds of source caching are useful.

### Snapshot history cache

Validation repeatedly walks history from a starting snapshot to the current parent. Previously processed committed snapshots are immutable and may remain cached.

```text
attempt 1:
S1 -> S2 -> S3

retry:
S1 -> S2 -> S3 -> S4 -> S5

reuse S1/S2/S3
process only S4/S5
```

The cache must detect inconsistent reuse of the same snapshot identity. The exact integrity guard is an implementation detail described in Appendix B.

### Manifest source cache

A parsed manifest may also be reused because committed manifest files are immutable.

```text
manifest path
     │
     ▼
object-store read + decode
     │
     ▼
SourceCache
```

Decoded manifest caching can consume significant memory and therefore must be bounded. The cache representation and eviction strategy are implementation details; the architecture only requires that reusable immutable source metadata can be retained safely.

## 5.3 DerivedCache

`DerivedCache` contains deterministic results computed from stable action intent and source metadata.

Conceptually:

```rust
struct DerivedCache {
    validation: ValidationCache,
    filtering: ManifestFilterCache,
    added_manifests: AddedManifestCache,
}
```

Not every derived result creates a physical artifact.

```text
M1 -> Rewritten(M1')   reusable result + physical artifact
M2 -> Unchanged        reusable result only
```

This is why derived work and artifact ownership are represented separately.

### 5.3.1 Validation cache

Conflict validation is naturally incremental over immutable history.

Suppose attempt 1 validates:

```text
S1 PASS
S2 PASS
S3 PASS
```

After refresh, history becomes:

```text
S1 S2 S3 S4 S5
```

If the validation context is unchanged, attempt 2 may reuse validation of `S1` through `S3` and validate only `S4` and `S5`.

The important correctness rule is:

> **Every attempt must establish validation coverage through the refreshed parent. This does not require recomputing validation for immutable history that has already been validated under the same context.**

Validation context includes operation-specific semantics such as the conflict filter and referenced files, and may also depend on schema or partition-spec binding. If that context changes, cached validation is invalidated.

### 5.3.2 Manifest filter cache

Manifest filtering is reusable at the granularity of an immutable source manifest.

For example:

```text
M1 + remove A -> Rewritten(M1')
M2 + remove A -> Unchanged
```

After a concurrent append introduces `M4`, a retry may do:

```text
M1 -> reuse M1'
M2 -> reuse Unchanged
M4 -> evaluate now
```

If a source manifest disappears from the refreshed base, its old cache entry does not require eager invalidation. The new attempt starts from the refreshed base's current manifest set, so the old result simply becomes unreachable.

### 5.3.3 Added-manifest cache

Added data/delete files are immutable action intent. A manifest generated from them may be reused across attempts when its content does not depend on refreshed attempt context.

```text
added files
    │
    ├── cached manifest exists -> reuse
    │
    └── otherwise -> write manifest and cache it
```

Whether this reuse is valid is table-format-dependent; Appendix C documents the inheritance assumptions used by current formats.

## 5.4 ArtifactTracker

`ArtifactTracker` records physical metadata created by the action:

```rust
struct ArtifactTracker {
    written: HashSet<String>,
}
```

It is independent from cache reuse.

For example:

```text
M1'   reusable + owned
M3    reusable + owned
L1    attempt-only + owned
```

A successful metadata write is recorded after the write completes. Failed writes are not recorded.

This gives cleanup one uniform ownership boundary without requiring cleanup logic to understand every cache's internal representation.

---

# 6. Snapshot-Producing Execution

`MergingSnapshotProducer` executes one action attempt against one refreshed transaction-local table. It borrows `MergeRetryState` but does not own retry-persistent state.

Its logical pipeline is:

```text
refreshed table + action intent + RetryState
                    │
                    ▼
            derive attempt context
                    │
                    ▼
                validate
                    │
                    ▼
          process current manifests
                    │
                    ▼
          apply removals / additions
                    │
                    ▼
          organize + summarize
                    │
                    ▼
         SnapshotProducer::finalize
                    │
                    ▼
               ActionCommit
```

## 6.1 Attempt context

Each execution derives base-dependent values from the refreshed table, such as:

- parent snapshot;
- sequence number;
- row-ID allocation.

These values are attempt-local and must be rebuilt after a rebase.

## 6.2 Validation phase

Validation occurs before the attempt writes new snapshot metadata.

```text
starting snapshot
      │
      ▼
discover required history through current parent
      │
      ├── SourceCache: reuse already materialized history
      │
      └── DerivedCache: reuse valid historical validation
      │
      ▼
validate newly introduced history
      │
      ├── conflict -> fail
      └── pass -> continue
```

A cache therefore reduces repeated work but never reduces required validation coverage.

## 6.3 Manifest processing

After validation passes, the producer processes the current base's manifests.

For each current source manifest:

```text
source manifest
      │
      ├── filter cache hit -> reuse result
      │
      └── cache miss
             │
             ├── load/decode from SourceCache or storage
             ├── evaluate removal logic
             └── cache Unchanged or Rewritten(...)
```

Added manifests are handled similarly through the added-manifest cache.

The complete resulting manifest set is still rebuilt for the refreshed base.

## 6.4 Manifest organization

Manifest grouping and merge decisions depend on the complete current manifest set. A rebase may add or replace source manifests, so organization is attempt-local in the initial design.

Deterministic merge outputs may be cached later if their dependency set can be represented safely.

## 6.5 Snapshot finalization

`SnapshotProducer` remains a thin generic finalization layer.

```rust
struct ProducedSnapshot {
    operation: Operation,
    manifests: Vec<ManifestFile>,
    summary: SnapshotSummary,
}
```

It is responsible for:

- writing the manifest list / snapshot metadata root;
- building the `Snapshot`;
- producing `TableUpdate`s;
- producing `TableRequirement`s.

It receives stable identity from the action state and current base-dependent values from the attempt. It does not understand rewrite, overwrite, row-delta, or replace-partitions semantics.

## 6.6 Execution abstraction

This RFC uses `MergingSnapshotProducer` as the common execution abstraction for merging actions:

```text
Action
  ↓
MergingSnapshotProducer
  ↓
SnapshotProducer::finalize
```

A `SnapshotChanges -> SnapshotCommitBuilder` representation is a viable alternative for normalizing mutations. The retry-state model is intentionally independent of this choice: `SnapshotActionState`, `SourceCache`, `DerivedCache`, and `ArtifactTracker` remain useful with either execution representation.

---

# 7. Artifact Lifecycle

Snapshot-producing actions create metadata before the final catalog commit. Those artifacts must survive retry safely and must eventually be cleaned up if they do not become reachable from committed metadata.

```text
metadata write succeeds
        │
        ▼
 ArtifactTracker records it
        │
        ├──────── retry / rebase ────────┐
        │                                │
        │                                ▼
        │                           keep for reuse
        │
        ├──────── confirmed success
        │              │
        │              ▼
        │       reachability cleanup
        │
        ├──────── confirmed failure
        │              │
        │              ▼
        │         delete all owned
        │
        └──────── unknown outcome
                       │
                       ▼
                  delete nothing
```

## 7.1 Ownership and tracking

Automatic cleanup applies only to metadata owned by the snapshot-producing layer, such as:

- generated manifests;
- rewritten manifests;
- delete manifests;
- manifest lists.

A caller-provided `DataFile` or `DeleteFile` is not automatically owned by the transaction merely because it is staged for addition.

Every successfully completed action-owned metadata write is recorded in `ArtifactTracker`.

## 7.2 Cleanup rules

### Retryable catalog conflict

Do not eagerly delete artifacts. Some may be reused by the next attempt, while attempt-local artifacts may simply remain tracked until the transaction reaches a terminal outcome.

### Confirmed success

Cleanup is based on reachability from committed metadata:

```text
all action-owned artifacts written
        -
artifacts reachable from committed transaction snapshots
        =
safe-to-delete artifacts
```

For a multi-action transaction, reachability must include all snapshots committed by the transaction, not only the final current snapshot.

### Confirmed terminal failure

If the transaction definitively did not commit and will not retry, all action-owned generated metadata may be deleted.

### Unknown outcome

If the catalog response is ambiguous, the generated metadata may already be live.

> **Unknown outcome deletes nothing.**

Stable `snapshot_id` provides a durable identity that may help later determine whether the snapshot landed, but richer automatic ambiguity resolution is future work.

## 7.3 Transaction-level cleanup

The transaction determines when a terminal outcome has been reached and drives cleanup across every action entry.

This matters because an earlier action may have written artifacts successfully before a later action fails validation during replay.

Process crashes or abandoned transactions may still leave orphan metadata; ordinary orphan-file cleanup tooling remains the final safety net.

---

# 8. Correctness Rules

The design is governed by a small set of rules:

| Area | Rule |
| --- | --- |
| Action state | action intent does not change across attempts |
| Identity | `snapshot_id` and `commit_uuid` remain stable for one logical action execution |
| Reuse | cached work is reused only while all semantic dependencies remain unchanged |
| Validation | every attempt establishes coverage through the refreshed parent |
| New history | newly introduced history is validated before new snapshot metadata is written |
| Base-derived state | parent, sequence, row IDs, complete manifest set, manifest list, and `TableCommit` are rebuilt for a new base |
| Ordering | actions replay in their original transaction order |
| Artifact ownership | every successful action-owned metadata write is tracked |
| Artifact paths | generated paths are write-once across action attempts |
| Cleanup | confirmed success uses reachability; confirmed failure deletes owned artifacts; unknown outcome deletes nothing |

Three rules are especially important.

## 8.1 Reuse is dependency-based

The system does not classify all retry state as either globally persistent or globally invalidated. Reuse is decided at the granularity of the semantic dependency that produced a result.

## 8.2 Validation coverage is complete but incremental

Cached validation may avoid recomputation, but the refreshed parent must always be covered. A retry that moves from `S3` to `S5` cannot stop at the cached result through `S3`.

## 8.3 Attempt-local output never crosses a new base

Anything representing the complete result against one base — such as the manifest list or `TableCommit` — must be rebuilt after rebasing to another base.

---

# 9. Design Decisions and Alternatives

## 9.1 Explicit RetryState versus a long-lived producer

Java naturally retains retry state because one long-lived producer object survives repeated `apply()` calls and owns stateful manifest managers.

This RFC separates the lifetimes explicitly:

```text
ActionEntry owns RetryState
        │
        ▼
each attempt creates a new MergingSnapshotProducer
        │
        └── borrows RetryState
```

This fits Rust ownership naturally while preserving the same ability to reuse work across retries.

## 9.2 Typed ActionEntry versus independently erased action/state

The action and state are type-erased together as `TransactionActionEntry<A>` rather than separately erasing `Action` and `State` and reconnecting them with runtime downcasts.

This keeps the `Action -> State` relationship compile-time checked.

## 9.3 MergingSnapshotProducer versus SnapshotChanges

`SnapshotChanges` can normalize actions into a generic mutation representation and is a reasonable execution alternative.

This RFC chooses `MergingSnapshotProducer` because rewrite, overwrite, delete, row-delta, and replace-partitions share a natural execution pipeline: validate, process current manifests, remove/add files, organize, summarize, finalize.

The retry-state architecture is independent of this choice.

## 9.4 Dependency-based reuse versus recompute-all

Recomputing everything after every catalog conflict is simpler, but repeatedly loads immutable metadata, re-runs the same validation/filtering, and rewrites identical manifests.

Because committed metadata and action intent are immutable, deterministic sub-results can be retained safely when their dependencies remain unchanged.

---

# 10. Implementation Plan

## Phase 1 — Stateful Transaction Actions

Introduce the state lifecycle:

- `TransactionAction::State` and `new_state`;
- typed `TransactionActionEntry<A>`;
- lazy state initialization;
- `clone_without_state()` for transaction cloning;
- stable `SnapshotActionState`;
- `MergeRetryState` skeleton;
- `SourceCache`, `DerivedCache`, and `ArtifactTracker` skeletons.

Migrate existing transaction actions to the new interface and preserve current replay ordering.

**Success criterion:** action state survives retries but is not shared by independent cloned transactions.

## Phase 2 — Correct Retry-Aware Snapshot Execution

Implement the correctness-complete snapshot-producing path:

- attempt-scoped `MergingSnapshotProducer`;
- attempt context;
- baseline conflict validation;
- baseline manifest filtering and additions;
- snapshot finalization;
- unique attempt-qualified artifact paths;
- `ArtifactTracker` wiring for every metadata write;
- transaction-driven terminal cleanup.

At this stage, most work may still be recomputed on every retry.

**Success criterion:** retry/rebase, validation, snapshot generation, artifact ownership, and cleanup are correct without relying on cache optimization.

## Phase 3 — Incremental Retry Caching

Enable reuse through `SourceCache` and `DerivedCache`:

- incremental snapshot-history processing;
- bounded source-manifest caching;
- incremental validation reuse;
- manifest-filter result caching;
- added data/delete manifest reuse.

**Success criterion:** a non-conflicting rebase processes only newly introduced history/manifests while producing the same result as recompute-all execution.

## Phase 4 — RewriteFiles End to End

Implement `RewriteFilesAction` as the first full consumer of the architecture.

RewriteFiles exercises:

- explicit additions/removals;
- file-existence and concurrent-delete validation;
- manifest filtering;
- generated-manifest reuse;
- retry/rebase;
- cleanup.

**Success criterion:** RewriteFiles requires no transaction-specific retry special cases outside the abstractions introduced in Phases 1–3.

## Phase 5 — Remaining Merging Actions

Add:

- DeleteFiles;
- OverwriteFiles;
- RowDelta;
- ReplacePartitions.

Each action should primarily define immutable intent, validation policy, and operation-specific removal/addition semantics.

**Success criterion:** new actions do not require redesigning transaction replay, retry-state ownership, cache layers, or artifact tracking.

---

# 11. Future Work

The following optimizations and extensions are intentionally outside the initial implementation:

- same-base whole-attempt caching (`ProducedSnapshot` / `TableCommit` reuse);
- caching deterministic manifest-merge outputs;
- more aggressive predicate-evaluation caching;
- cache eviction policy refinement and instrumentation;
- advanced ambiguous-commit resolution and REST `Idempotency-Key` integration;
- parallel action replay where dependencies permit;
- cross-process retry state;
- table-format-specific cache implementations.

---

# 12. Summary

The proposal adds an explicit retry-state lifecycle to transaction actions.

```text
Transaction
    owns refresh / replay / ordering / terminal outcome

ActionEntry
    owns immutable action intent + RetryState

MergeRetryState
    ├── SnapshotActionState  stable identity
    ├── SourceCache          reusable immutable inputs
    ├── DerivedCache         reusable deterministic work
    └── ArtifactTracker      physical metadata ownership

MergingSnapshotProducer
    executes one attempt

SnapshotProducer
    finalizes generic snapshot metadata
```

The central rule is:

> **Completed work survives retry or rebase exactly when the semantic dependencies that produced it remain unchanged.**

A rebase therefore does not mean starting over. It means re-validating and rebuilding what depends on the new base while retaining safe, immutable work from previous attempts.

---

# Appendix A — Type-Erased Entry and Clone Semantics

The detailed entry shape is conceptually:

```rust
struct TransactionActionEntry<A: TransactionAction> {
    action: Arc<A>,
    state: Option<A::State>,
}

#[async_trait]
pub(crate) trait DynTransactionActionEntry: Send {
    async fn commit(&mut self, table: &Table) -> Result<ActionCommit>;

    fn snapshot_id(&self) -> Option<i64>;

    fn clone_without_state(&self) -> Box<dyn DynTransactionActionEntry>;

    async fn cleanup(&mut self, outcome: TerminalOutcome<'_>) -> Result<()>;
}
```

`clone_without_state()` clones immutable action configuration but deliberately leaves retry state uninitialized:

```rust
fn clone_without_state(&self) -> Box<dyn DynTransactionActionEntry> {
    Box::new(TransactionActionEntry::<A> {
        action: Arc::clone(&self.action),
        state: None,
    })
}
```

`Transaction::clone()` uses this method for every entry. The cloned transaction is therefore a fresh logical execution of the same action plan and receives fresh snapshot identity on its first commit attempt.

State initialization remains lazy because `new_state` is fallible and table-dependent, while `Clone` must remain infallible.

---

# Appendix B — Source Cache Integrity and Memory

Committed snapshot metadata and manifests are immutable, which makes them safe retry-cache inputs. An implementation should still guard against accidentally reusing inconsistent metadata under the same logical key.

For processed snapshot history, one possible integrity guard is a compact snapshot fingerprint such as:

```rust
struct SnapshotFingerprint {
    parent_snapshot_id: Option<i64>,
    sequence_number: i64,
    manifest_list: String,
}
```

A mismatch should fail rather than silently reuse stale cached contents.

Decoded manifests can be large. A manifest source cache must therefore have an explicit bound, whether by entry count, estimated memory, or another suitable policy. The exact policy is not part of the core architecture.

---

# Appendix C — Format-Specific Manifest Reuse

Added-manifest reuse is valid only while manifest content does not embed attempt-specific values that change on rebase.

For current Iceberg formats, several values are inheritance-based rather than fixed directly in the reusable manifest. For example, depending on format version, snapshot ID, data sequence number, and first-row-id may be resolved through manifest-list metadata.

A future format that embeds refreshed attempt-specific values directly into manifests would change the dependency set and may disable this reuse. This is a table-format implementation seam, not a reason to change retry-state ownership.

---

# Appendix D — Detailed Retry Example

Assume:

```text
S10
├── M1 = [A, B]
└── M2 = [C]

RewriteFiles:
  remove A
  add D
```

On first execution, action state initializes once:

```text
snapshot_id      = Snew
commit_uuid      = U
producer_attempt = 1
```

The producer validates the required history, reads `M1` and `M2`, rewrites `M1` as `M1'`, records `M2` as unchanged, and writes an added-file manifest `M3` for `D`.

Retry state becomes approximately:

```text
SourceCache:
  history through S10
  M1
  M2

DerivedCache:
  validation through S10
  M1 -> Rewritten(M1')
  M2 -> Unchanged
  added D -> M3

ArtifactTracker:
  M1'
  M3
  L1
```

The catalog rejects the commit because a concurrent writer appended `M4` and advanced the table to `S11`.

On the second execution:

```text
snapshot_id      = Snew   unchanged
commit_uuid      = U      unchanged
producer_attempt = 2
```

The producer reuses cached history through `S10`, processes only newly introduced history for `S11`, and extends validation through the refreshed parent.

Current manifests are now `M1`, `M2`, and `M4`:

```text
M1 -> reuse Rewritten(M1')
M2 -> reuse Unchanged
M4 -> evaluate now
```

The added manifest `M3` is reused. The complete result is rebuilt for `S11`, and the producer writes a new manifest list `L2`.

If the catalog commit succeeds, reachability cleanup keeps the artifacts referenced by the committed snapshot and deletes stale attempt-only artifacts:

```text
keep:
  M1'
  M3
  L2

delete:
  L1
```

If instead validation fails before new metadata is written, the transaction reaches confirmed failure and cleans all action-owned artifacts. If catalog outcome is ambiguous, cleanup does not run.
