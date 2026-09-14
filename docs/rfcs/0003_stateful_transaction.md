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

**Scope:** The transaction/action lifecycle, retry-persistent action state, and
the snapshot producers that carry that state for snapshot-producing actions.

---

# 1. Motivation

The current transaction implementation replays actions after a catalog
conflict, but an action has no state that survives the replay. Every retry
starts from nothing. This is fine for simple metadata updates, but merging
snapshot operations (rewrite-files, overwrite, delete-files, row-delta,
replace-partitions) need to retain work across attempts:

- a stable snapshot identity, so one logical operation produces one snapshot;
- already-materialized immutable metadata, so retries do not re-read and
  re-decode the same committed manifests;
- already-completed deterministic work (filtered manifests, generated added
  manifests), so retries do not rewrite identical files;
- ownership of generated metadata, so uncommitted files can be cleaned up.

This RFC defines where that state lives, how it is initialized, how it
survives catalog retries, and how each retry combines it with a refreshed
table. It intentionally does **not** specify cache layouts, conflict-detection
predicates, or per-operation semantics; those are follow-up work (§8).

## Non-goals

Cross-process retry state, whole-attempt caching, cache eviction policy,
automatic recovery from ambiguous catalog outcomes, and the concrete conflict
validation predicates (to be covered separately).

---

# 2. Architecture Overview

The design separates three lifetimes:

1. **Transaction lifetime** — owns refresh, retry, action ordering, replay,
   and the terminal outcome.
2. **Action lifetime** — owns immutable action intent plus retry-persistent
   state. Spans all attempts of one logical execution.
3. **Attempt lifetime** — one execution of one action against one refreshed
   transaction-local table.

```text
Transaction
│   owns refresh / retry / replay ordering / terminal outcome
│
├── ActionEntry
│      ├── immutable action intent            (shared with clones)
│      └── retry-persistent state             (never shared)
│
├── ActionEntry
│      └── ...
│
retry attempt:
    refreshed transaction-local Table  +  same persistent state
                        │
                        ▼
        reuse still-valid work + recompute base-dependent work
                        │
                        ▼
                  new ActionCommit
```

For snapshot-producing actions, the retry-persistent state **is a persistent
snapshot producer**: a `SimpleSnapshotProducer` for append-shaped operations
or a `MergingSnapshotProducer` for operations that also remove files (§5).
The producer's fields are the retry-persistent data; the values derived from
one particular base are locals of one attempt.

## 2.1 Core reuse rule

> **Retry-persistent work may be reused when the semantic inputs that produced
> it are still valid. Base-dependent final state must be recomputed after
> rebasing.**

Concretely:

| State | Retry / rebase behavior |
| --- | --- |
| Action intent | preserve |
| `snapshot_id` / `commit_uuid` | preserve |
| Immutable committed metadata already read | preserve |
| Deterministic derived work (filtered manifests, added manifests) | preserve while its inputs are unchanged |
| Parent snapshot, sequence number, row-ID allocation | recompute |
| Complete resulting manifest set | recompute |
| Manifest list / `ActionCommit` / `TableCommit` | recompute |

A rebase therefore re-executes the action but does not discard all previous
work.

---

# 3. Transaction Action Model

## 3.1 The `TransactionAction` trait

Each action declares its own retry-persistent state as an associated type:

```rust
#[async_trait]
pub(crate) trait TransactionAction: Send + Sync + 'static {
    /// Retry-persistent state for one logical execution of this action.
    type State: Send + Sync + 'static;

    /// Creates fresh state. Infallible and table-independent; called once
    /// when the action is applied to a transaction (and again for clones).
    fn new_state(&self) -> Self::State;

    /// Executes one attempt: immutable intent + persistent state +
    /// the refreshed transaction-local table.
    async fn commit(&self, state: &mut Self::State, table: &Table) -> Result<ActionCommit>;

    /// Called exactly once, after the transaction reaches a terminal
    /// outcome (§6). Default: nothing to clean up.
    async fn finish(&self, _state: &mut Self::State, _outcome: TerminalOutcome<'_>) {}
}
```

The signature encodes the ownership model directly: the action is immutable
(`&self`), the state is mutable and survives attempts (`&mut Self::State`),
and the table is borrowed per attempt. Stateless actions use `State = ()`.

`new_state` is deliberately infallible and table-independent. Anything
table-dependent — most importantly the generated `snapshot_id` — is resolved
*lazily inside the state* on the first attempt and kept stable afterwards.
This keeps state construction and `Transaction::clone` trivially infallible
while still guaranteeing stable identity (§5.1).

`TransactionAction` and everything in this section is `pub(crate)`. External
crates cannot define actions; the erasure machinery below is not public API.

## 3.2 `ActionEntry` and ownership

The transaction stores one entry per applied action:

```rust
struct ActionEntry<A: TransactionAction> {
    action: Arc<A>,   // immutable intent, may be shared with cloned transactions
    state: A::State,  // retry-persistent, owned exclusively by this transaction
}

pub struct Transaction {
    table: Table,                            // transaction-local base
    actions: Vec<Box<dyn DynActionEntry>>,   // entries, in application order
}
```

Entries are heterogeneous, so they are type-erased behind one object-safe
trait:

```rust
#[async_trait]
trait DynActionEntry: Send + Sync {
    async fn commit(&mut self, table: &Table) -> Result<ActionCommit>;
    async fn finish(&mut self, outcome: TerminalOutcome<'_>);
    fn fork(&self) -> Box<dyn DynActionEntry>;   // same action, fresh state
}
```

Because the action and its state are paired *before* erasure, the
`Action -> State` relationship is fixed by construction and no runtime
downcast can be wrong. An equivalent implementation that erases the action
and the state as separate objects and reconnects them with a checked downcast
is also acceptable; the pairing invariant is what matters, not the mechanism.
**The erasure mechanism is an internal, reversible implementation choice.**

## 3.3 Clone semantics

Retry state belongs to one logical execution. A cloned transaction is a new
logical execution of the same action plan:

```text
Transaction::clone()
      ├── shares immutable actions (Arc)
      └── forks fresh state via new_state()
```

Two transactions therefore never observe each other's retry state, and a
clone that commits produces its own snapshot identity. Since `new_state` is
infallible, `Clone` needs no `Option<State>` or lazy-initialization plumbing.

---

# 4. Retry Lifecycle

`Transaction::commit` owns the retry loop (backoff configured from table
properties, as today):

```text
attempt N:
    refresh table from catalog; if stale, rebase transaction-local table
        │
        ▼
    for each entry, in application order:
        entry.commit(&mut state, &current_table)
        apply ActionCommit to the transaction-local table
        (action B always observes the local result of action A)
        │
        ▼
    build TableCommit; catalog.update_table(...)
        │
        ├── success            → terminal: Committed
        ├── retryable conflict → attempt N+1 (state retained, nothing deleted)
        └── non-retryable or retries exhausted
                               → terminal: Failed, or Ambiguous if the
                                 catalog outcome is unknown
        │
        ▼
terminal: entry.finish(outcome) for every entry (§6)
```

The transaction owns refresh and replay; individual actions never refresh the
catalog themselves. Each retry calls `commit` with the **same** `&mut state`
and a **refreshed** table. Inside the state, work is reused or recomputed
according to the rule in §2.1.

## 4.1 What the persistent state may contain

Every retry-persistent field must fall into one of four categories:

1. **Stable identity** — `snapshot_id`, `commit_uuid`, attempt counters.
2. **Cache of immutable committed metadata** — already-read snapshots,
   manifest lists, decoded manifests. Safe because committed metadata never
   changes; entries are keyed by the metadata's own identity.
3. **Derived deterministic work** — results computed from stable action
   intent plus immutable inputs (a filtered manifest, a generated added
   manifest), keyed by those inputs. A rebase that changes an input is a
   cache miss, never stale reuse.
4. **Artifact ownership** — the set of metadata files this state has written,
   used only for terminal cleanup (§6).

A base-dependent value stored as a bare field is a bug by definition: it
would silently survive a rebase. Base-dependent values are either locals of
one attempt or cache entries guarded by a key that changes with the base
(for example, cached current-snapshot manifests keyed by snapshot equality).

This classification is a design rule, not a struct layout. Earlier drafts
named these categories as concrete types (`SourceCache`, `DerivedCache`,
`ArtifactTracker`); this RFC intentionally does not, because the right
representations — including whether some caching moves into lower-level types
such as `Snapshot` — are reversible implementation decisions.

---

# 5. Snapshot Producers

Snapshot-producing actions share two concrete, persistent producer types used
as their `State`. Neither is a trait hierarchy; Java's
`SnapshotProducer <- MergingSnapshotProducer <- operation` inheritance chain
is replaced by two sibling structs plus shared pieces by composition:

```text
SimpleSnapshotProducer          MergingSnapshotProducer
  (append-shaped ops)             (ops that also remove files)
        │                                │
        ├── embeds CommitIdentity ───────┤   shared stateful core (§5.1)
        └── calls snapshot helpers ──────┘   shared stateless utilities (§5.2)
```

Each producer exposes an `apply(&mut self, table, …) -> Result<ActionCommit>`
that executes one attempt, and a terminal cleanup hook driven through
`TransactionAction::finish`. The existing attempt-scoped `SnapshotProducer`
and its `SnapshotProduceOperation`/`ManifestProcess` hook traits are removed:
those hooks existed to let Java-style subclasses inject behavior into a base
class pipeline, and with concrete producers whose `apply` owns the pipeline
and takes operation-specific inputs as arguments, they have no remaining job.

## 5.1 `CommitIdentity` — shared identity and artifact ownership

Both producers embed one small shared component that owns identity, path
allocation, and artifact ownership:

```rust
struct CommitIdentity {
    commit_uuid: Option<Uuid>,   // resolved once, first attempt; then stable
    snapshot_id: Option<i64>,    // resolved once, against the first table seen
    attempt: u64,                // makes each attempt's manifest list unique
    manifest_counter: u64,       // monotonic: generated paths are write-once
    owned_artifacts: HashSet<String>,   // every metadata file this state wrote
}
```

Its guarantees:

- **Stable identity.** `snapshot_id` and `commit_uuid` identify one logical
  snapshot-producing execution and never change across retries or rebases.
- **Write-once paths.** Generated metadata paths embed `commit_uuid` and a
  monotonic counter (manifests) or attempt number (manifest lists), so no
  path is ever written twice. A retry can never overwrite metadata that an
  earlier, ambiguously-completed attempt may have made live. Reused artifacts
  keep their original paths; they are referenced, not rewritten.
- **Complete ownership.** Every successful producer-owned metadata write is
  recorded before it can need cleanup; failed writes are not recorded.
  Caller-provided data files are never owned by the producer.

## 5.2 Stateless snapshot helpers

Generic snapshot production that both producers need — snapshot-ID
generation, summary construction, manifest-list writing, snapshot/
`TableUpdate`/`TableRequirement` assembly — lives in stateless free
functions that translate explicit inputs into metadata. They hold no mutable
state and understand no operation semantics. This replaces the generic
`SnapshotProducer` finalization layer with plain functions, and gives a
simple boundary rule: **logic that touches persistent fields is a method on
the owning producer; pure functions of explicit inputs are free helpers.**
Format-version specifics concentrate here and in the manifest writers, which
keeps the architecture above this line stable across format evolution.

## 5.3 `SimpleSnapshotProducer`

The persistent state for append-shaped operations (`FastAppend`). Besides
`CommitIdentity`, it retains across retries:

- **Current-snapshot manifest cache** — the manifest list of the base
  snapshot, keyed by snapshot equality. A rebase to a new base is a cache
  miss and reloads; a retry on the same base reuses.
- **Added-manifest cache** — the manifest generated from the action's added
  data files. Reusable across attempts because its content does not depend
  on the base under current formats (Appendix A).

An attempt validates the added files against the refreshed table, assembles
the complete manifest set (recomputed), and writes a new manifest list at an
attempt-unique path.

## 5.4 `MergingSnapshotProducer`

The persistent state for operations that remove and/or add files (rewrite,
overwrite, delete-files, row-delta, replace-partitions). Its internals are
sketched here as black boxes: the RFC fixes what each component does and how
it behaves across retries, not its representation.

- **Current-snapshot manifest cache** — as in §5.3.
- **Added-manifest caches** (data and delete) — manifests generated from the
  action's added files, grouped as needed (for example by partition spec),
  reused across attempts.
- **Manifest filter managers** (data and delete) — apply the operation's
  removals to the base's manifests. For each source manifest they produce
  either *unchanged* or *rewritten-without-the-removed-entries*, and detect
  files that were requested for removal but are no longer live. Results are
  reusable per source manifest: a source manifest is immutable, so its
  filter result remains valid as long as the removal intent is unchanged; a
  rebase only evaluates manifests it has not seen. Rewritten manifests
  preserve the source manifest's partition spec. Missing-file handling
  (fail vs ignore) is operation policy supplied by the action.
- **Validation history cache** — records which committed snapshots this
  execution has already processed for conflict validation, so a retry
  validates only newly introduced history. Caching reduces repeated work but
  never reduces coverage: every attempt must establish validation coverage
  through the refreshed parent. The conflict predicates themselves are out
  of scope for this RFC.

One attempt runs a fixed pipeline owned by `apply`:

```text
refreshed table + operation inputs
        │
        ▼
resolve identity (first attempt only)
        │
        ▼
validate against refreshed base          (reuse processed history)
        │
        ▼
filter current manifests for removals    (reuse per-manifest results)
add generated manifests for additions    (reuse added manifests)
        │
        ▼
recompute: parent, sequence number, complete manifest set,
           summary (reflecting adds AND removes)
        │
        ▼
write manifest list at attempt-unique path; record artifacts
        │
        ▼
ActionCommit
```

Everything above the last two steps may hit caches; the last two steps are
always recomputed for the current base.

---

# 6. Terminal Outcomes and Artifact Lifecycle

Producers write metadata before the catalog commit succeeds, so generated
artifacts must survive retries and be cleaned up if they never become
reachable. The transaction classifies the end of the retry loop into exactly
one terminal outcome and reports it to every entry once:

```rust
pub(crate) enum TerminalOutcome<'a> {
    /// The transaction committed. `table` reflects committed metadata.
    Committed(&'a Table),
    /// The transaction definitively did not commit and will not retry.
    Failed(&'a Table),
    /// The catalog outcome is unknown; committed metadata may exist.
    Ambiguous,
}
```

Cleanup semantics:

| Outcome | Behavior |
| --- | --- |
| Retryable conflict (not terminal) | delete nothing; work may be reused |
| `Committed` | keep artifacts reachable from this execution's committed snapshot(s); delete the rest (e.g. stale attempt manifest lists) |
| `Failed` | delete all owned artifacts |
| `Ambiguous` | **delete nothing** |

Classifying errors into outcomes happens in one place, in the transaction —
not in producers pattern-matching on error kinds. For a multi-action
transaction, an earlier action may have written artifacts before a later
action failed; running `finish` on every entry with the same outcome covers
this. Reachability must consider all snapshots the transaction committed,
not only the final current snapshot.

Process crashes and abandoned transactions can still leave orphans; ordinary
orphan-file cleanup remains the final safety net. Richer resolution of
ambiguous outcomes (e.g. discovering after refresh that our stable
`snapshot_id` did land) is future work; the write-once path rule (§5.1)
already guarantees retries cannot corrupt an ambiguously committed attempt.

---

# 7. Correctness Rules

| Area | Rule |
| --- | --- |
| Intent | action intent never changes across attempts |
| Identity | `snapshot_id` / `commit_uuid` are stable for one logical execution; clones get fresh identity |
| State isolation | retry state is never shared between transactions |
| Reuse | cached work is reused only while all of its semantic inputs are unchanged |
| Fields | every persistent field is identity, immutable-source cache, keyed derived work, or artifact ownership (§4.1) |
| Validation | every attempt establishes coverage through the refreshed parent |
| Base-derived output | parent, sequence numbers, complete manifest set, manifest list, `ActionCommit`, `TableCommit` are recomputed per attempt |
| Ordering | actions replay in application order; later actions observe earlier actions' local results |
| Artifacts | every successful owned metadata write is recorded; generated paths are write-once |
| Cleanup | `Committed` = reachability; `Failed` = delete owned; `Ambiguous` = delete nothing |

---

# 8. Design Decisions and Alternatives

**Persistent producer as state vs a passive retry-state struct borrowed by an
attempt-scoped producer.** An earlier draft kept the producer attempt-scoped
and had it borrow a long-lived `RetryState`. Field-for-field, that state
converges on exactly what a persistent producer holds, while adding a borrow
split and a second struct. The persistent producer expresses the same
boundary more simply: struct fields are retry-persistent, locals of `apply`
are attempt-local. The residual risk — accidentally persisting a
base-dependent value — is handled by the field classification rule (§4.1)
rather than by structure.

**Generic `State` vs a producer-typed association.** `TransactionAction`
could have tied its state to a snapshot-producer trait. But many actions are
stateless or not snapshot-producing, and no transaction-level code needs to
know a state is a producer — replay calls `commit`, cleanup calls `finish`,
both on the action. The producers are simply the shared `State` types that
snapshot-producing actions choose.

**Erasing action and state together vs separately.** Pairing them in a typed
entry before erasure makes a mismatched pair unrepresentable and avoids
downcasts; erasing them separately with a checked downcast also works and
keeps two independent trait objects. Both preserve the required invariant;
the choice is internal and reversible (§3.2).

**Sibling producers + composition vs inheritance.** Java reuses snapshot
production through a class hierarchy. Rust gets the same reuse from two
concrete siblings sharing `CommitIdentity` (stateful, by composition) and the
stateless helpers — without base classes, hook traits, or the lock that an
internally-shared producer would need. `MergingSnapshotProducer` deliberately
does not contain `SimpleSnapshotProducer`.

**Eager infallible state vs lazy fallible state.** Creating state when the
action is applied — with table-dependent identity resolved lazily inside it —
removes the `Option<State>` and initialization branch that a lazy, fallible
`new_state(&Table)` would force, while providing the same identity stability.

---

# 9. Deferred Work

Deliberately not fixed by this RFC, because they are reversible or separable:

- concrete cache representations, keys, bounds, and eviction (including
  whether snapshot→manifest caching moves into `Snapshot` or other
  lower-level types, as in Java);
- conflict-validation predicates and isolation levels (companion document);
- manifest merge/organization and its cacheability;
- filter-manager internals, including per-spec bookkeeping;
- ambiguous-outcome resolution beyond "delete nothing";
- whole-attempt caching (`TableCommit` reuse on an unchanged base);
- REST idempotency integration and cross-process retry state.

---

# 10. Suggested Sequencing

1. `TransactionAction::State`, `ActionEntry`, fork-on-clone; migrate existing
   actions with `State = ()`.
2. Stateless snapshot helpers and `CommitIdentity`.
3. `SimpleSnapshotProducer`; migrate `FastAppend`; remove the old
   `SnapshotProducer`/`SnapshotProduceOperation`/`ManifestProcess`.
4. `TerminalOutcome`, `finish`, artifact cleanup.
5. `MergingSnapshotProducer` core (added manifests, current-snapshot cache).
6. Manifest filter managers; validation history cache.
7. First merging action end-to-end (e.g. `RowDelta` or `RewriteFiles`), then
   the remaining merging actions.

---

# Appendix A — Format-Specific Manifest Reuse

Added-manifest reuse across attempts is valid only while manifest content
does not embed attempt-specific values that change on rebase. In current
formats this holds because snapshot ID, data sequence number, and
first-row-id are (depending on format version) inherited through
manifest-list metadata rather than fixed in the manifest file. A future
format that embeds base-dependent values directly into manifests would
shrink this reuse to nothing — which would change what the added-manifest
cache may return, but not the ownership or lifecycle architecture. This is a
table-format seam inside the producers and helpers, not a reason to change
the transaction model.
