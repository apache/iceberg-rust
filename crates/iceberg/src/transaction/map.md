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

# map.md — crates/iceberg/src/transaction/

## Purpose

The atomic table-mutation layer: every metadata- or data-changing operation is a
`TransactionAction` applied through a `Transaction` (Java analogues: `Transaction`,
`SnapshotProducer`, `MergingSnapshotProducer`, and the per-action `Base*` classes in
`core/`). This is the heart of the Phase-1 (metadata evolution) and Phase-2 (write
engine) parity work.

## Contents

| File | Java analogue | What it does |
|---|---|---|
| `mod.rs` | `Transaction` | `Transaction` struct: action registry, `commit()` retry loop (backon exponential), `starting_snapshot_id` capture, the `pub fn` ctor per action; test fixtures `make_v1_table`/`make_v2_table`/`make_v2_minimal_table` |
| `action.rs` | `PendingUpdate` | The `TransactionAction` / `ApplyTransactionAction` seam: `commit(self: Arc<Self>, &Table) -> Result<ActionCommit>`; default-no-op `validate` hook (conflict validation runs in `do_commit` against the refreshed base) |
| `snapshot.rs` | `SnapshotProducer` / `MergingSnapshotProducer` | Shared snapshot-producing machinery: manifest writing, `process_deletes`, `resolve_delete_paths`, `resolve_partition_deletes`, `current_manifests` (returns the FULL manifest list — DATA **and** DELETE — so every delete-bearing action carries outstanding MoR delete manifests forward, Java `MergingSnapshotProducer.apply` L973-1011; dropping them silently resurrects deleted rows), delete-manifest support (`write_added_delete_manifest`), `new_cluster_manifest_writer` (fresh DATA manifest per spec id, shared by `rewrite_manifests` + `merge_append`), the `ManifestProcess` seam (async + `Result` + `&mut SnapshotProducer`; `DefaultManifestProcess` passthrough for fast append, `MergeManifestProcess` for merge append), the concurrent-commit walk `files_after` + wrappers (`added_data_files_after`, `added_delete_files_after`, `deleted_data_files_after`) and the shared conflict test `first_conflicting_file` / `validate_no_conflicting_added_data_files` |
| `append.rs` | `FastAppend` | Fast append |
| `merge_append.rs` | `MergeAppend` / `ManifestMergeManager` | Merge append (Java `newAppend`): append data files in one `Operation::Append` snapshot like fast append, then BIN-PACK + MERGE the manifest list per `commit.manifest-merge.enabled` / `commit.manifest.min-count-to-merge` / `commit.manifest.target-size-bytes`. Provenance-preserving (carried entries → `add_existing_entry`; this-commit entries → `add_entry`; old tombstones suppressed). Ported `BinPacking.packEnd` lives in a private `bin_packing` submodule. Delete-manifest merging deferred (carried unchanged) |
| `delete_files.rs` | `StreamingDelete` | Delete data files by path/reference; opt-in `validate_files_exist()` |
| `overwrite_files.rs` | `BaseOverwriteFiles` | Explicit add+delete in one `Overwrite` snapshot; opt-in `validate_no_conflicting_data()` + `conflict_detection_filter` |
| `replace_partitions.rs` | `BaseReplacePartitions` | Dynamic partition overwrite; opt-in conflict validation |
| `rewrite_files.rs` | `BaseRewriteFiles` | Compaction-commit primitive (`Operation::Replace`); `data_sequence_number` preservation (added files keep the replaced files' data seq so outstanding equality deletes still apply), `validate_from_snapshot` + `validate` (shared `validate_no_new_deletes_for_data_files`, `ignore_equality_deletes = seq preserved`); carries DELETE manifests forward unchanged |
| `rewrite_manifests.rs` | `BaseRewriteManifests` | Manifest re-organization (NOT data change): cluster live data-manifest entries into new manifests via the provenance-preserving `add_existing_entry` path, and/or explicit add/delete manifest replacement; `Operation::Replace`, live set unchanged. Extends `SnapshotProducer` (NOT `MergingSnapshotProducer`) |
| `row_delta.rs` | `BaseRowDelta` | Merge-on-read commit: data + position/equality delete files in one snapshot; `validate_no_conflicting_data_files/_delete_files`, `validate_data_files_exist` |
| `manage_snapshots.rs` | `ManageSnapshots` | Branch/tag CRUD, rollback(-to-time), set-current, fast-forward, retention |
| `update_schema.rs` | `SchemaUpdate` | Schema evolution incl. `union_by_name`, column defaults (✅ interop-proven) |
| `update_partition_spec.rs` | `BaseUpdatePartitionSpec` | Partition evolution (✅ interop-proven) |
| `sort_order.rs` | `BaseReplaceSortOrder` | **The template action** — mirror this when adding a new action |
| `update_location.rs` / `update_properties.rs` / `update_statistics.rs` / `upgrade_format_version.rs` | misc `Base*` | Location / properties / statistics / format-version updates |

## I want to...

| I want to... | go to |
|---|---|
| Add a new transaction action | copy the `sort_order.rs` pattern: builder struct + `#[async_trait] impl TransactionAction` returning `ActionCommit::new(updates, requirements)`; wire `mod` + `pub fn` ctor in `mod.rs` |
| Add conflict validation to an action | the `validate` hook (`action.rs`) + the shared walk/helpers in `snapshot.rs`; mirror `replace_partitions.rs` or `row_delta.rs` |
| Understand the commit/retry loop | `mod.rs` `Transaction::commit` — refresh, re-apply, `validate` (non-retryable `DataInvalid` on conflict), catalog `update_table` |
| Find the Java source being mirrored | reference checkout at `/tmp/iceberg-java-ref` (re-clone `apache/iceberg` if absent) |
| Check what is/isn't interop-proven | [docs/parity/GAP_MATRIX.md](../../../../docs/parity/GAP_MATRIX.md) |

## Pointers

- **Up:** [crates/iceberg/src/](..) (no map yet) · repo intent: [CLAUDE.md](../../../../CLAUDE.md)
- **Related:** `spec/table_metadata_builder.rs` (the low-level metadata primitives actions emit into);
  [../writer/map.md](../writer/map.md) (produces the `DataFile`s actions commit);
  [../../tests/map.md](../../tests/map.md) (interop tests); [dev/java-interop/map.md](../../../../dev/java-interop/map.md) (the oracle)

## Debug

### Known failure modes

| Symptom | Likely cause |
|---|---|
| New action's `.commit()` not found in tests | The `TransactionAction` trait must be `use`d in the test module to call `.commit()` |
| Flaky test comparing `last_updated_ms` with `<` | Two metadata versions can share a millisecond — assert `<=` plus a structural change (e.g. metadata-log growth) |
| Conflict validation never fires | `validate` runs in `do_commit` against the *refreshed* base; a test must commit the concurrent change between transaction *build* and transaction *commit* |
| Retry loop spins on a validation failure | Conflict errors must be **non-retryable** `ErrorKind::DataInvalid` (Java's non-retryable `ValidationException`) — check the error kind |
| Fixture lacks the ref/snapshot your test needs | `make_v2_table()` has only `main`; build a forked fixture via `add_snapshot` + `set_ref`, and set the grafted snapshot's `timestamp_ms` against the metadata's `last-updated-ms` |
| Parity divergence from Java found late | Verify against the Java *source* (`Preconditions.checkArgument`, early-return no-ops) before implementing — not intuition |
| Schema/spec guard never fires for CTAS or catalog commits | Guards belong at the `TableMetadataBuilder` choke point (e.g. `add_schema`), NOT in an action's `commit()` — the action only EMITS updates; `apply` is where every path converges. Note the blast radius: only tests that APPLY updates hit it, not tests that merely inspect the emitted shape |
| Action emits over-constrained commit requirements | Derive each `TableRequirement` from the update that induces it (Java `UpdateRequirements`): `AddSpec` ⇒ last-assigned-partition-id, `SetDefaultSpec` ⇒ default-spec-id — never emit guards unconditionally |
| Surviving entries silently corrupted by a rewrite | The #1 corruption class: re-stamping a surviving/carried-forward entry's snapshot id or sequence numbers. `add_existing_entry` preserves provenance; `add_entry` RESTAMPS. Pin with a cross-snapshot provenance test (see docs/testing.md) |

### First checks

- Does the action emit **only changed** refs/updates (no-op suppression)? Java emits nothing for
  create-then-remove / replace-to-same.
- Run the **full** parallel lib suite (`cargo test -p iceberg --lib`), not just the new module's
  filter — added load surfaces latent races elsewhere.
- Prove a metadata action end-to-end by driving its emitted updates through
  `TableMetadataBuilder` (`update.apply(builder)`) — the unbound `apply()` shape skips spec dedup,
  `LAST_ADDED` resolution, and bind-time name checks; only a full catalog commit exercises bind.
- New conflict validation? It needs the no-override tx-captured-start test (docs/testing.md), a
  non-retryable `DataInvalid` (+ `!retryable()` assertion), and a REAL concurrent commit through the
  catalog between txn-build and txn-commit.

### Escalate to

- On-disk format questions → `spec/` (source of truth for serialization).
- Interop failures → [dev/java-interop/map.md#debug](../../../../dev/java-interop/map.md#debug).
