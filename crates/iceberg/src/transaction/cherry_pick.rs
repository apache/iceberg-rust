// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! This module contains the cherry-pick action — the write-audit-publish (WAP) "publish" commit.
//!
//! [`CherryPickAction`] mirrors Java `CherryPickOperation` (`core/CherryPickOperation.java`, 288 lines).
//! Given the id of a STAGED snapshot (one that exists in table metadata but is NOT on the `main` branch —
//! typically written by a WAP audit job with a `wap.id` summary property), it publishes that snapshot onto
//! `main`. There are three published shapes, dispatched on the staged snapshot's operation + a precedence
//! rule:
//!
//! 1. **Fast-forward (precedence over replay, Java `apply` L193-204):** when the staged snapshot's PARENT is
//!    the current `main` head (or both are null), `main` is moved to the staged snapshot AS-IS — NO new
//!    snapshot is produced, no files are replayed. This is checked BEFORE the replay paths, so an APPEND or a
//!    replace-partitions OVERWRITE whose parent == head ALSO fast-forwards (cite L193-204:
//!    `requireFastForward || isFastForward(base)`).
//! 2. **APPEND replay (Java `cherrypick` L78-92):** the staged snapshot's added data files are replayed
//!    through the producer's add path into a NEW snapshot on `main` (operation `Append`), tagged with the
//!    `source-snapshot-id` summary property (the staged id) and — iff the staged snapshot carries a non-empty
//!    `wap.id` — the `published-wap-id` summary property.
//! 3. **OVERWRITE + `replace-partitions=true` replay (Java `cherrypick` L93-129):** the staged snapshot's
//!    added AND removed data files are replayed into a NEW `Overwrite` snapshot (the removed files are dropped
//!    by-path through the producer's `process_deletes`, so they must still be live in the table —
//!    `failMissingDeletePaths`). Same `source-snapshot-id` / `published-wap-id` tagging. The replaced
//!    partitions are tracked so [`CherryPickAction::validate`] can reject a concurrent change in them.
//!
//! Anything else (a staged DELETE, a non-replace OVERWRITE, or an append/overwrite whose parent is neither
//! head nor null) is rejected: "Cannot cherry-pick snapshot %s: not append, dynamic overwrite, or
//! fast-forward".
//!
//! **The action is STATELESS across the commit retry loop** (mirroring every other action): it stores only
//! the staged snapshot id. Both [`CherryPickAction::validate`] and the `TransactionAction::commit` resolve
//! everything — case dispatch, file replay, WAP/dedup checks — against the REFRESHED table passed in by
//! `do_commit`. An "unknown snapshot id" is rejected the same way against the refreshed base.
//!
//! **Java-parity surface note.** Java exposes cherry-pick via `ManageSnapshots.cherrypick(long)`, routed
//! through the transaction. Rust exposes it as a STANDALONE action ([`crate::transaction::Transaction::cherry_pick`])
//! rather than a method on `ManageSnapshotsAction`: the ref-op action only EMITS `SetSnapshotRef` /
//! `RemoveSnapshotRef` updates and has no snapshot-producing path, whereas cherry-pick (in its replay shapes)
//! needs the full [`SnapshotProducer`]. The two do not compose cleanly, so the honest shape is a separate
//! action — the same choice already made for `replace_partitions` / `overwrite_files`, which Java also reaches
//! through producer subclasses. `ManageSnapshotsAction` carries a doc pointer here.
//!
//! **Multi-spec replay (Java parity since 2026-06-11).** Java's add path (`MergingSnapshotProducer.add`)
//! preserves each added file's OWN partition spec id and writes per-spec manifests, so cherry-picking a
//! snapshot whose data files belong to an OLDER partition spec onto a table whose default spec has since moved
//! SUCCEEDS in Java. The Rust replay reuses the producer's `validate_added_data_files`, which now mirrors
//! Java: an added file is accepted as long as its `partition_spec_id` EXISTS in the table's specs (an UNKNOWN
//! spec id is rejected with Java's "Cannot find partition spec %s for data file: %s"), and the producer
//! writes ONE manifest per partition-spec group — so the same old-spec replay SUCCEEDS, the replayed files
//! land in a manifest stamped with their own spec id, and a scan reads them correctly. Pinned by
//! `test_cherrypick_multispec_replay_produces_per_spec_manifest`.
//!
//! The stage-only WAP WRITE path that creates a staged snapshot in the first place
//! (`FastAppendAction::stage_only()` / `DeleteFilesAction::stage_only()`, Java `SnapshotProducer.stageOnly()`)
//! landed in Group V (2026-06-11); see [`crate::transaction::snapshot::SnapshotProducer::with_stage_only`].
//! Most tests here still graft staged snapshots by `set_current`-ing main off a normally-published snapshot
//! (a staged snapshot is just a dangling one), which is equivalent for the publish path under test.
//!
//! **WAP-path publish dedup (Group V V2, 2026-06-11).** Two dedup paths run in [`CherryPickAction::validate`],
//! in Java's order (`CherryPickOperation.validate`, 1.10.0 bytecode): `validateNonAncestor` FIRST (the
//! ancestry path — already-an-ancestor + the `source-snapshot-id` double-publish lookup), `validateWapPublish`
//! LAST (the WAP-id path). When BOTH apply, the ANCESTRY error fires (Java order). The WAP-id check
//! ([`Self::validate_wap_publish`] → [`is_wap_id_published`]) walks the CURRENT ancestry and rejects with the
//! verbatim `DuplicateWAPCommitException` message iff the picked snapshot's non-empty `wap.id` already appears
//! among the ancestors — comparing it against each ancestor's OWN `wap.id` (`STAGED_WAP_ID_PROP`) AND its
//! `published-wap-id` (`PUBLISHED_WAP_ID_PROP`). BOTH arms matter: a REPLAY publish stamps `published-wap-id`
//! (caught via that arm), but a FAST-FORWARD publish keeps only the staged snapshot's own `wap.id` (caught via
//! the STAGED arm — Java `TestWapWorkflow.testDuplicateCherrypick`'s first publish is a fast-forward). The
//! corruption this prevents is a duplicate WAP publish double-applying the same audited change.
//!
//! **Dedup scope is the CURRENT ANCESTRY of `main`, by design (Java-faithful escape hatch).** Because the walk
//! roots at `metadata.current_snapshot()` (Java `WapUtil.isWapIdPublished` → `SnapshotUtil.ancestorIds(meta.
//! currentSnapshot(), ...)`), a `wap.id` that has left the live `main` line is NO LONGER seen as published: a
//! WAP publish that is rolled BACK past — or whose publishing snapshot is orphaned (cherry-pick only ever
//! targets `main`) — reopens its `wap.id`, so a second same-id publish then succeeds. This is identical in Java
//! (same ancestry source); it is NOT a Rust divergence, and is pinned by
//! `test_cherrypick_rollback_reopens_wap_id_java_faithful`.
//!
//! **`write.wap.enabled` is ENGINE-side only — core does NOT gate ordinary commits (V3, settled OUT by
//! 1.10.0 bytecode).** `TableProperties.WRITE_AUDIT_PUBLISH_ENABLED = "write.wap.enabled"` is defined in core
//! but read by NO core production class (only `SparkWriteConf`/`SparkReadConf`/`SparkTableUtil` consume it,
//! to decide whether the engine stages and sets `wap.id`). `SnapshotProducer.apply()` calls only the
//! overridable per-subclass `validate` — never `WapUtil`; only `CherryPickOperation` calls `validateWapPublish`.
//! So a `wap.id` present on a NON-staged ordinary commit is not validated/blocked core-side. No core gate to port.
//!
//! **Out of scope (deferred):** Java↔Rust byte-level interop for the published snapshot, incl. the staged-WAP
//! interop fixture (this is a 🟡 unit-proven action).

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{
    DataFile, MAIN_BRANCH, ManifestContentType, ManifestEntry, ManifestFile, ManifestStatus,
    Operation, SnapshotRef, SnapshotReference, SnapshotRetention, Struct, TableMetadata,
};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

/// The summary property a WAP audit job stamps on a STAGED snapshot (Java
/// `SnapshotSummary.STAGED_WAP_ID_PROP`).
const STAGED_WAP_ID_PROP: &str = "wap.id";
/// The summary property the PUBLISHED snapshot carries when its source was a WAP-staged snapshot (Java
/// `SnapshotSummary.PUBLISHED_WAP_ID_PROP`).
const PUBLISHED_WAP_ID_PROP: &str = "published-wap-id";
/// The summary property that links a published snapshot back to the staged snapshot it cherry-picked (Java
/// `SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP`). Cherry-pick's double-publish dedup keys on this.
const SOURCE_SNAPSHOT_ID_PROP: &str = "source-snapshot-id";
/// The summary property that marks an OVERWRITE snapshot as a dynamic partition overwrite (Java
/// `SnapshotSummary.REPLACE_PARTITIONS_PROP`) — the gate that routes the OVERWRITE replay path.
const REPLACE_PARTITIONS_PROP: &str = "replace-partitions";

/// Build a non-retryable [`ErrorKind::DataInvalid`] error (Java's non-retryable `ValidationException`).
fn data_invalid(message: String) -> Error {
    Error::new(ErrorKind::DataInvalid, message)
}

/// A transaction action that PUBLISHES a staged snapshot onto `main` (write-audit-publish), mirroring Java
/// `CherryPickOperation`.
///
/// Use [`crate::transaction::Transaction::cherry_pick`] to create one. The action stores only the staged
/// snapshot id; everything else resolves at commit/validate time against the refreshed table (the stateless
/// retry pattern). See the module docs for the three published shapes and the fast-forward precedence.
pub struct CherryPickAction {
    /// The id of the STAGED snapshot to publish.
    snapshot_id: i64,
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
}

impl CherryPickAction {
    pub(crate) fn new(snapshot_id: i64) -> Self {
        Self {
            snapshot_id,
            commit_uuid: None,
            key_metadata: None,
        }
    }

    /// Set the commit UUID for the produced snapshot (otherwise a fresh v7 UUID is generated). No-op for the
    /// fast-forward shape, which produces no new snapshot.
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set key metadata for produced manifest files. No-op for the fast-forward shape.
    pub fn set_key_metadata(mut self, key_metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(key_metadata);
        self
    }
}

/// The decision the cherry-pick dispatch reaches against a refreshed base — Java's `cherrypick(long)`
/// case split (L78-138) combined with the `apply()` fast-forward precedence (L193-204).
enum CherryPickPlan {
    /// Fast-forward `main` to the staged snapshot AS-IS — no new snapshot (Java `apply` returns
    /// `base.snapshot(picked.id)`). Carries the picked snapshot's id.
    FastForward { picked_id: i64 },
    /// Replay the staged snapshot's added (and, for the replace-partitions shape, removed) data files into a
    /// NEW snapshot recording `operation`.
    Replay {
        /// The picked snapshot's operation, recorded on the produced snapshot (Java `operation()` returns
        /// `cherrypickSnapshot.operation()`).
        operation: Operation,
        /// Data files to add (the picked snapshot's `ADDED` data entries).
        added_data_files: Vec<DataFile>,
        /// Paths of data files to remove by-path (the picked snapshot's `DELETED` data entries) — empty for
        /// the APPEND shape. Resolved against the current table (`failMissingDeletePaths`) at produce time.
        removed_data_file_paths: HashSet<String>,
        /// The `(spec_id, partition)` tuples the replay replaces (empty for APPEND) — tracked for
        /// [`CherryPickAction::validate`]'s `validateReplacedPartitions`.
        replaced_partitions: HashSet<(i32, Struct)>,
        /// The `published-wap-id` to set, iff the picked snapshot had a non-empty `wap.id`.
        published_wap_id: Option<String>,
    },
}

impl CherryPickAction {
    /// Resolve the staged snapshot against `metadata`, returning it or the unknown-id error.
    ///
    /// Java `cherrypick` L70-73: `ValidationException.check(cherrypickSnapshot != null, "Cannot cherry-pick
    /// unknown snapshot ID: %s", snapshotId)`.
    fn require_picked<'a>(&self, metadata: &'a TableMetadata) -> Result<&'a SnapshotRef> {
        metadata.snapshot_by_id(self.snapshot_id).ok_or_else(|| {
            data_invalid(format!(
                "Cannot cherry-pick unknown snapshot ID: {}",
                self.snapshot_id
            ))
        })
    }

    /// Whether the staged snapshot can fast-forward `main` (Java `isFastForward(base)` L173-182): its parent
    /// is the current head, or — when the table has no current snapshot — its parent is also null.
    fn is_fast_forward(picked: &SnapshotRef, metadata: &TableMetadata) -> bool {
        match metadata.current_snapshot_id() {
            Some(head) => picked.parent_snapshot_id() == Some(head),
            None => picked.parent_snapshot_id().is_none(),
        }
    }

    /// Whether the staged snapshot is a dynamic partition overwrite (Java `cherrypick` L93-95): operation is
    /// `OVERWRITE` AND the `replace-partitions` summary property is `true`.
    fn is_replace_partitions(picked: &SnapshotRef) -> bool {
        picked.summary().operation == Operation::Overwrite
            && picked
                .summary()
                .additional_properties
                .get(REPLACE_PARTITIONS_PROP)
                .map(|value| value == "true")
                .unwrap_or(false)
    }

    /// Read the staged snapshot's non-empty `wap.id`, the value to publish as `published-wap-id` (Java
    /// `cherrypick` L80-83 / L108-111: `WapUtil.validateWapPublish` returns the staged `wap.id`, set only when
    /// non-null). The duplicate-WAP rejection is done separately in [`Self::validate_wap_publish`] against the
    /// refreshed base; here we only extract the id to stamp.
    fn published_wap_id(picked: &SnapshotRef) -> Option<String> {
        picked
            .summary()
            .additional_properties
            .get(STAGED_WAP_ID_PROP)
            .filter(|wap_id| !wap_id.is_empty())
            .cloned()
    }

    /// Decide the published shape against the refreshed `table`, mirroring Java `cherrypick(long)` (L69-141)
    /// with the `apply()` fast-forward precedence (L193-204). The fast-forward check runs FIRST, so an APPEND
    /// or replace-partitions OVERWRITE whose parent == head fast-forwards (no replay) — exactly Java's
    /// `requireFastForward || isFastForward(base)` ordering.
    async fn plan(&self, table: &Table) -> Result<CherryPickPlan> {
        let metadata = table.metadata();
        let picked = self.require_picked(metadata)?.clone();

        // Fast-forward PRECEDENCE (Java `apply` L193-204): if the staged snapshot's parent is the current head
        // (or both null), publish it as-is with no replay, regardless of its operation.
        if Self::is_fast_forward(&picked, metadata) {
            return Ok(CherryPickPlan::FastForward {
                picked_id: picked.snapshot_id(),
            });
        }

        let operation = picked.summary().operation.clone();
        let published_wap_id = Self::published_wap_id(&picked);

        if operation == Operation::Append {
            // APPEND replay (Java L78-92): replay the picked snapshot's ADDED data files only.
            let changes = picked_snapshot_changes(table, &picked).await?;
            return Ok(CherryPickPlan::Replay {
                operation: Operation::Append,
                added_data_files: changes.added,
                removed_data_file_paths: HashSet::new(),
                replaced_partitions: HashSet::new(),
                published_wap_id,
            });
        }

        if Self::is_replace_partitions(&picked) {
            // OVERWRITE + replace-partitions replay (Java L93-129). The picked snapshot's parent must be null
            // (overwrite based on an empty table) or an ancestor of the current state — otherwise the
            // since-parent change detection in `validateReplacedPartitions` is meaningless (Java L101-105).
            let parent_ok = match picked.parent_snapshot_id() {
                None => true,
                Some(parent_id) => is_current_ancestor(metadata, parent_id),
            };
            if !parent_ok {
                return Err(data_invalid(format!(
                    "Cannot cherry-pick overwrite not based on an ancestor of the current state: {}",
                    self.snapshot_id
                )));
            }

            let changes = picked_snapshot_changes(table, &picked).await?;
            let replaced_partitions = changes
                .added
                .iter()
                .map(|file| (file.partition_spec_id, file.partition().clone()))
                .collect();
            let removed_data_file_paths = changes
                .removed
                .iter()
                .map(|file| file.file_path().to_string())
                .collect();
            return Ok(CherryPickPlan::Replay {
                operation: Operation::Overwrite,
                added_data_files: changes.added,
                removed_data_file_paths,
                replaced_partitions,
                published_wap_id,
            });
        }

        // Not append, not a dynamic overwrite, and not a fast-forward (Java L131-138).
        Err(data_invalid(format!(
            "Cannot cherry-pick snapshot {}: not append, dynamic overwrite, or fast-forward",
            picked.snapshot_id()
        )))
    }

    /// Java `validateNonAncestor` (L207-216): reject if the staged snapshot is already an ancestor of the
    /// current head, or if some current ancestor was ITSELF a publish of the staged snapshot (its
    /// `source-snapshot-id` summary equals the staged id — the double-publish dedup).
    fn validate_non_ancestor(&self, metadata: &TableMetadata) -> Result<()> {
        if is_current_ancestor(metadata, self.snapshot_id) {
            // CherrypickAncestorCommitException(long) — exact Java message.
            return Err(data_invalid(format!(
                "Cannot cherrypick snapshot {}: already an ancestor",
                self.snapshot_id
            )));
        }

        if let Some(ancestor_id) = lookup_ancestor_by_source_snapshot(metadata, self.snapshot_id) {
            // CherrypickAncestorCommitException(long, long) — exact Java message.
            return Err(data_invalid(format!(
                "Cannot cherrypick snapshot {}: already picked to create ancestor {}",
                self.snapshot_id, ancestor_id
            )));
        }

        Ok(())
    }

    /// Java `validateReplacedPartitions` (L218-252). Only meaningful for the replace-partitions replay shape
    /// (`replaced_partitions` non-empty) and only when the table has a current snapshot. First re-checks the
    /// parent-ancestry against the refreshed base, then walks every snapshot committed between the current
    /// head and `picked.parent` (`SnapshotUtil.ancestorsBetween(currentSnapshot, parentId)` — inclusive of the
    /// head, EXCLUSIVE of the parent) and rejects if any file ADDED by those snapshots lands in a replaced
    /// partition.
    ///
    /// **Concurrent-window pin (the mandatory no-override rule): the walk's starting id is `picked.parentId`,
    /// NOT the transaction-captured `starting_snapshot_id`.** Cherry-pick's concurrent window is defined by the
    /// PICKED snapshot's parent (Java `ancestorsBetween(currentSnapshot, picked.parentId)`), so the
    /// tx-captured start is **N/A for this shape** — there is no `validate_from_snapshot` override and the
    /// transaction's read point is irrelevant here. The `do_commit`-supplied `starting_snapshot_id` is
    /// therefore intentionally unused by this validation; the walk re-derives its window from the picked
    /// snapshot every time, which is exactly what survives a commit retry.
    async fn validate_replaced_partitions(
        &self,
        table: &Table,
        picked_parent_id: Option<i64>,
        replaced_partitions: &HashSet<(i32, Struct)>,
    ) -> Result<()> {
        let metadata = table.metadata();
        if replaced_partitions.is_empty() || metadata.current_snapshot().is_none() {
            return Ok(());
        }

        // Java L221-224: the parent must (still) be null or an ancestor of the current state.
        let parent_ok = match picked_parent_id {
            None => true,
            Some(parent_id) => is_current_ancestor(metadata, parent_id),
        };
        if !parent_ok {
            return Err(data_invalid(format!(
                "Cannot cherry-pick overwrite, based on non-ancestor of the current state: {}",
                picked_parent_id
                    .map(|id| id.to_string())
                    .unwrap_or_default()
            )));
        }

        // Java L225-246: every file ADDED between the current head and the picked parent that lands in a
        // replaced partition is a "changed partition" — reject. The starting id is `picked.parentId`.
        let added = added_data_files_between(table, picked_parent_id).await?;
        if let Some(conflict) = added.iter().find(|file| {
            replaced_partitions.contains(&(file.partition_spec_id, file.partition().clone()))
        }) {
            return Err(data_invalid(format!(
                "Cannot cherry-pick replace partitions with changed partition: {:?}",
                conflict.partition()
            )));
        }

        Ok(())
    }

    /// Java `WapUtil.validateWapPublish` re-run against the refreshed base (L169): if the staged snapshot's
    /// `wap.id` is already STAGED or PUBLISHED among the current ancestors, reject with the duplicate-WAP
    /// error. A non-WAP snapshot (no `wap.id`) passes.
    fn validate_wap_publish(&self, metadata: &TableMetadata) -> Result<()> {
        let Some(picked) = metadata.snapshot_by_id(self.snapshot_id) else {
            // The id was already validated by the dispatch; if it vanished, the unknown-id error fires there.
            return Ok(());
        };
        let Some(wap_id) = Self::published_wap_id(picked) else {
            return Ok(());
        };

        if is_wap_id_published(metadata, &wap_id) {
            // DuplicateWAPCommitException(String) — exact Java message.
            return Err(data_invalid(format!(
                "Duplicate request to cherry pick wap id that was published already: {wap_id}"
            )));
        }

        Ok(())
    }

    /// Build the fast-forward `ActionCommit`: move `main` to the picked snapshot AS-IS, with the
    /// optimistic-concurrency guard that `main` is still where the refreshed base has it (the
    /// `ManageSnapshots` set-current shape). NO `AddSnapshot` — the snapshot already exists in metadata.
    fn fast_forward_commit(table: &Table, picked_id: i64) -> ActionCommit {
        let updates = vec![TableUpdate::SetSnapshotRef {
            ref_name: MAIN_BRANCH.to_string(),
            reference: SnapshotReference::new(
                picked_id,
                SnapshotRetention::branch(None, None, None),
            ),
        }];
        let requirements = vec![TableRequirement::RefSnapshotIdMatch {
            r#ref: MAIN_BRANCH.to_string(),
            snapshot_id: table.metadata().current_snapshot_id(),
        }];
        ActionCommit::new(updates, requirements)
    }
}

#[async_trait]
impl TransactionAction for CherryPickAction {
    /// Serializable-isolation validation (Java `CherryPickOperation.validate` L161-171). Runs against the
    /// REFRESHED base BEFORE the commit produces anything. Skipped entirely for the fast-forward shape (Java
    /// `if (!isFastForward(base))`). For the replay shapes it runs `validateNonAncestor`,
    /// `validateReplacedPartitions`, and the WAP-publish re-check.
    ///
    /// `starting_snapshot_id` (the transaction-captured head) is intentionally unused: cherry-pick's
    /// concurrent window is defined by the PICKED snapshot's parent, not the transaction's read point — see
    /// [`CherryPickAction::validate_replaced_partitions`]. An unknown staged id surfaces from [`Self::plan`] in
    /// `commit`; here, the dispatch errors (unknown id / not-eligible op) are produced by `plan` too, so a
    /// failing `plan` propagates non-retryably before any update is emitted.
    async fn validate(
        self: Arc<Self>,
        _starting_snapshot_id: Option<i64>,
        current: &Table,
    ) -> Result<()> {
        let plan = self.plan(current).await?;

        let CherryPickPlan::Replay {
            replaced_partitions,
            ..
        } = &plan
        else {
            // Fast-forward: Java skips validate entirely.
            return Ok(());
        };

        let metadata = current.metadata();
        // Java L166: already-an-ancestor / already-cherry-picked dedup.
        self.validate_non_ancestor(metadata)?;
        // Java L167-168: a concurrent change in a replaced partition (replace-partitions shape only).
        let picked_parent_id = metadata
            .snapshot_by_id(self.snapshot_id)
            .and_then(|snapshot| snapshot.parent_snapshot_id());
        self.validate_replaced_partitions(current, picked_parent_id, replaced_partitions)
            .await?;
        // Java L169: the WAP id must not already be staged/published among current ancestors.
        self.validate_wap_publish(metadata)?;

        Ok(())
    }

    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        match self.plan(table).await? {
            CherryPickPlan::FastForward { picked_id } => {
                Ok(Self::fast_forward_commit(table, picked_id))
            }
            CherryPickPlan::Replay {
                operation,
                added_data_files,
                removed_data_file_paths,
                published_wap_id,
                ..
            } => {
                // Tag the produced snapshot: `source-snapshot-id` always, `published-wap-id` iff the staged
                // snapshot had a non-empty `wap.id` (Java L80-86 / L108-114).
                let mut snapshot_properties = std::collections::HashMap::new();
                snapshot_properties.insert(
                    SOURCE_SNAPSHOT_ID_PROP.to_string(),
                    self.snapshot_id.to_string(),
                );
                if let Some(wap_id) = published_wap_id {
                    snapshot_properties.insert(PUBLISHED_WAP_ID_PROP.to_string(), wap_id);
                }

                let snapshot_producer = SnapshotProducer::new(
                    table,
                    self.commit_uuid.unwrap_or_else(Uuid::now_v7),
                    self.key_metadata.clone(),
                    snapshot_properties,
                    added_data_files,
                );
                // Validate the replayed adds like fast append (data content type, partition-spec match,
                // partition-value compatibility). The replayed removes are resolved by-path inside the
                // producer via the operation's `delete_files` seam (`failMissingDeletePaths`).
                snapshot_producer.validate_added_data_files()?;

                snapshot_producer
                    .commit(
                        CherryPickReplayOperation {
                            operation,
                            removed_data_file_paths,
                        },
                        DefaultManifestProcess,
                    )
                    .await
            }
        }
    }
}

/// The [`SnapshotProduceOperation`] for the cherry-pick REPLAY shapes.
///
/// Records the picked snapshot's operation (Java `operation()` = `cherrypickSnapshot.operation()`), exposes
/// every current manifest as the filter set, and resolves the replayed removed-file PATHS against the current
/// snapshot's live data entries — feeding the SAME `process_deletes` rewrite path the delete-bearing actions
/// use, with the `failMissingDeletePaths` missing-path check (Java `cherrypick` L117). The APPEND shape passes
/// an empty `removed_data_file_paths`, so no manifest is rewritten and the operation behaves like fast append.
struct CherryPickReplayOperation {
    operation: Operation,
    removed_data_file_paths: HashSet<String>,
}

impl SnapshotProduceOperation for CherryPickReplayOperation {
    fn operation(&self) -> Operation {
        self.operation.clone()
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        Ok(vec![])
    }

    async fn delete_files(&self, snapshot_produce: &SnapshotProducer<'_>) -> Result<Vec<DataFile>> {
        // Resolve the replayed removed paths against the current snapshot's live data entries. Java
        // `cherrypick` L117 calls `failMissingDeletePaths()`, so a replayed delete whose target is no longer
        // live is an error — `resolve_delete_paths` performs exactly that missing-path check. Empty for the
        // APPEND shape.
        snapshot_produce
            .resolve_delete_paths(&self.removed_data_file_paths)
            .await
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        // Expose EVERY current manifest — DATA and DELETE — so the producer's `process_deletes` can rewrite
        // the DATA manifests that hold replayed-removed files and carry every DELETE manifest forward unchanged
        // (the established merge-on-read-preserving posture; see `SnapshotProducer::current_manifests`).
        snapshot_produce.current_manifests().await
    }
}

/// The added / removed DATA files a single snapshot recorded in its OWN manifests — the Rust port of Java
/// `SnapshotChanges.builderFor(snapshot, io, specsById).build()` (`core/SnapshotChanges.java`
/// `cacheDataFileChanges`).
struct PickedSnapshotChanges {
    added: Vec<DataFile>,
    removed: Vec<DataFile>,
}

/// Load `picked`'s DATA manifests that IT wrote (`added_snapshot_id == picked.snapshot_id()`, Java
/// `manifest.snapshotId() == snapshot.snapshotId()`) and split their non-`Existing` entries: `Added` →
/// `added`, `Deleted` → `removed` (Java `cacheDataFileChanges` filters `status != EXISTING` then switches on
/// `ADDED` / `DELETED`). This is the established tombstone-sourcing pattern — the SAME manifest filter the
/// concurrent-commit walk (`files_after`) uses, scoped to one snapshot.
async fn picked_snapshot_changes(
    table: &Table,
    picked: &SnapshotRef,
) -> Result<PickedSnapshotChanges> {
    let manifest_list = picked
        .load_manifest_list(table.file_io(), table.metadata())
        .await?;

    let mut added = Vec::new();
    let mut removed = Vec::new();
    for manifest_file in manifest_list.entries() {
        // Only DATA manifests this snapshot WROTE — a carried-forward manifest belongs to an older snapshot
        // and its files were not added/removed by `picked` (Java `manifest.snapshotId() == snapshot.id`).
        if manifest_file.content != ManifestContentType::Data
            || manifest_file.added_snapshot_id != picked.snapshot_id()
        {
            continue;
        }
        let manifest = manifest_file.load_manifest(table.file_io()).await?;
        for entry in manifest.entries() {
            match entry.status() {
                ManifestStatus::Added => added.push(entry.data_file().clone()),
                ManifestStatus::Deleted => removed.push(entry.data_file().clone()),
                ManifestStatus::Existing => {}
            }
        }
    }

    Ok(PickedSnapshotChanges { added, removed })
}

/// Enumerate the DATA files ADDED to `table` by snapshots committed between the current head (inclusive) and
/// `starting_snapshot_id` (EXCLUSIVE) — the Rust port of Java's `validateReplacedPartitions` file walk
/// (`SnapshotUtil.ancestorsBetween(currentSnapshot, parentId)` filtered to `manifestsCreatedBy(snap)` →
/// `ADDED` entries, L225-246).
///
/// This is the SAME walk shape as `snapshot::files_after` (inclusive of the head, exclusive of the start, only
/// manifests the snapshot itself wrote, only `Added` entries) but with NO operation filter — Java's
/// `manifestsCreatedBy(snap)` inspects every ancestor's own data manifests regardless of operation. It is
/// kept local to cherry-pick rather than widening `snapshot.rs`'s walk family because cherry-pick is the only
/// caller that wants the operation-unfiltered variant.
async fn added_data_files_between(
    table: &Table,
    starting_snapshot_id: Option<i64>,
) -> Result<Vec<DataFile>> {
    let metadata = table.metadata();
    let Some(mut current) = metadata.current_snapshot().cloned() else {
        return Ok(vec![]);
    };

    let mut collected = Vec::new();
    loop {
        // `ancestorsBetween` is EXCLUSIVE of the starting snapshot (the picked parent): stop before it.
        if Some(current.snapshot_id()) == starting_snapshot_id {
            break;
        }

        let manifest_list = current
            .load_manifest_list(table.file_io(), metadata)
            .await?;
        for manifest_file in manifest_list.entries() {
            // Only DATA manifests THIS snapshot wrote (Java `manifestsCreatedBy`: `m.snapshotId() ==
            // snap.snapshotId()`); a carried-forward manifest's files were added by an earlier snapshot.
            if manifest_file.content != ManifestContentType::Data
                || manifest_file.added_snapshot_id != current.snapshot_id()
            {
                continue;
            }
            let manifest = manifest_file.load_manifest(table.file_io()).await?;
            for entry in manifest.entries() {
                if entry.status() == ManifestStatus::Added {
                    collected.push(entry.data_file().clone());
                }
            }
        }

        match current.parent_snapshot_id() {
            Some(parent_id) => match metadata.snapshot_by_id(parent_id) {
                Some(parent) => current = parent.clone(),
                None => break,
            },
            None => break,
        }
    }

    Ok(collected)
}

/// Whether `snapshot_id` is the current snapshot or any ancestor of it (Java `isCurrentAncestor` /
/// `SnapshotUtil.ancestorIds(currentSnapshot).contains(snapshotId)`).
fn is_current_ancestor(metadata: &TableMetadata, snapshot_id: i64) -> bool {
    let mut current = metadata.current_snapshot_id();
    while let Some(id) = current {
        if id == snapshot_id {
            return true;
        }
        current = metadata
            .snapshot_by_id(id)
            .and_then(|snapshot| snapshot.parent_snapshot_id());
    }
    false
}

/// Whether any current ancestor's summary carries `source-snapshot-id == snapshot_id` (Java
/// `lookupAncestorBySourceSnapshot` L268-279) — returning that ancestor's id. This is the double-publish dedup:
/// an earlier cherry-pick of the same staged snapshot tagged its produced snapshot with this source id.
fn lookup_ancestor_by_source_snapshot(metadata: &TableMetadata, snapshot_id: i64) -> Option<i64> {
    let target = snapshot_id.to_string();
    let mut current = metadata.current_snapshot_id();
    while let Some(id) = current {
        let snapshot = metadata.snapshot_by_id(id)?;
        if snapshot
            .summary()
            .additional_properties
            .get(SOURCE_SNAPSHOT_ID_PROP)
            == Some(&target)
        {
            return Some(id);
        }
        current = snapshot.parent_snapshot_id();
    }
    None
}

/// Whether `wap_id` is already staged or published among the current ancestors (Java
/// `WapUtil.isWapIdPublished` L62-70): some ancestor's summary has `wap.id == wap_id` or
/// `published-wap-id == wap_id`.
fn is_wap_id_published(metadata: &TableMetadata, wap_id: &str) -> bool {
    let mut current = metadata.current_snapshot_id();
    while let Some(id) = current {
        let Some(snapshot) = metadata.snapshot_by_id(id) else {
            break;
        };
        let properties = &snapshot.summary().additional_properties;
        if properties.get(STAGED_WAP_ID_PROP).map(String::as_str) == Some(wap_id)
            || properties.get(PUBLISHED_WAP_ID_PROP).map(String::as_str) == Some(wap_id)
        {
            return true;
        }
        current = snapshot.parent_snapshot_id();
    }
    false
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::*;
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{DataContentType, DataFileBuilder, DataFileFormat, Literal};
    use crate::transaction::tests::make_v3_minimal_table_in_catalog;
    use crate::transaction::{ApplyTransactionAction, Transaction};
    use crate::{Catalog, ErrorKind};
    // `TransactionAction` (for calling `.commit(&table)` directly) and the action's helpers come via
    // `super::*`.

    // ============================================================================================
    // Test fixtures. The V3 minimal table is partitioned by identity(x), spec id 0. A STAGED
    // snapshot is one that exists in metadata but is NOT on `main` — we produce it by appending (or
    // replace-partitioning) onto `main`, then rolling `main` back so the produced snapshot is left
    // dangling. This mirrors a WAP audit job that stages a snapshot off the published line.
    // ============================================================================================

    /// A data file routed to partition `x = part_value` (identity(x), spec id 0) with a unique path.
    fn data_file(path: &str, part_value: i64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(part_value))]))
            .build()
            .unwrap()
    }

    /// Append `files` in one fast-append commit and return the updated table.
    async fn append(catalog: &impl Catalog, table: &Table, files: Vec<DataFile>) -> Table {
        let tx = Transaction::new(table);
        let action = tx.fast_append().add_data_files(files);
        let tx = action.apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    /// Append `files` with a `wap.id` summary property in one fast-append commit. The produced snapshot
    /// records `operation = append` and carries `wap.id` — exactly a WAP-staged append snapshot.
    async fn append_wap(
        catalog: &impl Catalog,
        table: &Table,
        files: Vec<DataFile>,
        wap_id: &str,
    ) -> Table {
        let mut properties = HashMap::new();
        properties.insert(STAGED_WAP_ID_PROP.to_string(), wap_id.to_string());
        let tx = Transaction::new(table);
        let action = tx
            .fast_append()
            .set_snapshot_properties(properties)
            .add_data_files(files);
        let tx = action.apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    /// Move `main` to `snapshot_id` (Java `setCurrentSnapshot`) WITHOUT removing any snapshot, leaving the
    /// snapshots that were after it dangling/staged. Returns the rolled-back table.
    async fn set_current(catalog: &impl Catalog, table: &Table, snapshot_id: i64) -> Table {
        let tx = Transaction::new(table);
        let action = tx.manage_snapshots().set_current_snapshot(snapshot_id);
        let tx = action.apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    /// Cherry-pick `snapshot_id` and return the published table (panicking on error).
    async fn cherry_pick(catalog: &impl Catalog, table: &Table, snapshot_id: i64) -> Table {
        let tx = Transaction::new(table);
        let action = tx.cherry_pick(snapshot_id);
        let tx = action.apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    /// Cherry-pick `snapshot_id` expecting a failure, returning the error.
    async fn cherry_pick_err(
        catalog: &impl Catalog,
        table: &Table,
        snapshot_id: i64,
    ) -> crate::Error {
        let tx = Transaction::new(table);
        let action = tx.cherry_pick(snapshot_id);
        let tx = action.apply(tx).unwrap();
        tx.commit(catalog)
            .await
            .expect_err("cherry-pick should fail")
    }

    /// The live (Added/Existing) data-file paths in the table's current snapshot — what a scan would read.
    async fn live_file_paths(table: &Table) -> HashSet<String> {
        let Some(snapshot) = table.metadata().current_snapshot() else {
            return HashSet::new();
        };
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let mut live = HashSet::new();
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                if entry.is_alive() {
                    live.insert(entry.file_path().to_string());
                }
            }
        }
        live
    }

    /// The number of snapshots that exist in the table metadata.
    fn snapshot_count(table: &Table) -> usize {
        table.metadata().snapshots().count()
    }

    /// Read a summary property off the current snapshot.
    fn current_summary_prop(table: &Table, key: &str) -> Option<String> {
        table
            .metadata()
            .current_snapshot()
            .unwrap()
            .summary()
            .additional_properties
            .get(key)
            .cloned()
    }

    /// Stage an APPEND snapshot with a `wap.id`, off a non-head parent so cherry-picking REPLAYS (not FF).
    ///
    /// Sequence: S0 (base @ x=9) on `main`; S_staged (file @ part_value, wap.id) appended onto S0 then `main`
    /// rolled back to S0; finally S_head (another file @ x=9) appended onto S0 so `main = S_head` and
    /// `S_staged.parent = S0 != S_head`. Returns `(table_at_head, staged_id, s0_id)`.
    async fn stage_append_for_replay(
        catalog: &impl Catalog,
        table: &Table,
        staged_path: &str,
        part_value: i64,
        wap_id: &str,
    ) -> (Table, i64, i64) {
        let table = append(catalog, table, vec![data_file("test/base.parquet", 9)]).await;
        let s0 = table.metadata().current_snapshot_id().unwrap();

        let table = append_wap(
            catalog,
            &table,
            vec![data_file(staged_path, part_value)],
            wap_id,
        )
        .await;
        let staged_id = table.metadata().current_snapshot_id().unwrap();

        let table = set_current(catalog, &table, s0).await;
        // Advance main past S0 with an unrelated head so the staged snapshot's parent (S0) is no longer head.
        let table = append(catalog, &table, vec![data_file("test/head.parquet", 9)]).await;
        (table, staged_id, s0)
    }

    // ============================================================================================
    // Happy-path APPEND cherry-pick.
    // ============================================================================================

    /// HAPPY APPEND. A staged append snapshot (wap.id) whose parent is NOT the head is REPLAYED: a NEW
    /// snapshot lands on `main` carrying the replayed file, with BOTH summary props
    /// (`source-snapshot-id` = staged id, `published-wap-id` = the staged wap.id), and the post-commit scan
    /// live set includes the replayed file. Risk pinned: a publish that loses the staged file or omits the
    /// provenance/WAP tags (silent loss of audit lineage / un-deduplicatable republish).
    #[tokio::test]
    async fn test_cherrypick_append_replays_file_and_sets_summary_props() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let (table, staged_id, _s0) =
            stage_append_for_replay(&catalog, &table, "test/staged.parquet", 0, "wap-123").await;

        let before = snapshot_count(&table);
        let table = cherry_pick(&catalog, &table, staged_id).await;

        // A NEW snapshot was produced (not a fast-forward).
        assert_eq!(
            snapshot_count(&table),
            before + 1,
            "an append replay produces a NEW snapshot"
        );
        let published = table.metadata().current_snapshot().unwrap();
        assert_ne!(
            published.snapshot_id(),
            staged_id,
            "the published snapshot is a new one, not the staged snapshot itself"
        );
        assert_eq!(
            published.summary().operation,
            Operation::Append,
            "the published snapshot records the PICKED snapshot's operation (append)"
        );
        // Both summary props.
        assert_eq!(
            current_summary_prop(&table, SOURCE_SNAPSHOT_ID_PROP),
            Some(staged_id.to_string()),
            "source-snapshot-id links the publish to the staged snapshot"
        );
        assert_eq!(
            current_summary_prop(&table, PUBLISHED_WAP_ID_PROP),
            Some("wap-123".to_string()),
            "published-wap-id carries the staged wap.id"
        );
        // The replayed file is live; the unrelated head file survives.
        let live = live_file_paths(&table).await;
        assert!(
            live.contains("test/staged.parquet"),
            "the replayed staged file must be live after publish, got: {live:?}"
        );
        assert!(
            live.contains("test/head.parquet"),
            "the prior head file must survive, got: {live:?}"
        );
    }

    /// A staged append with NO `wap.id` still publishes with `source-snapshot-id` but NO `published-wap-id`
    /// (Java sets `published-wap-id` only when the staged `wap.id` is non-empty). Risk pinned: emitting a
    /// spurious/empty `published-wap-id` for a non-WAP cherry-pick.
    #[tokio::test]
    async fn test_cherrypick_append_without_wap_omits_published_wap_id() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        // Stage WITHOUT a wap.id by using the empty-string wap id (filtered out as non-WAP).
        let (table, staged_id, _s0) =
            stage_append_for_replay(&catalog, &table, "test/staged.parquet", 0, "").await;

        let table = cherry_pick(&catalog, &table, staged_id).await;
        assert_eq!(
            current_summary_prop(&table, SOURCE_SNAPSHOT_ID_PROP),
            Some(staged_id.to_string())
        );
        assert_eq!(
            current_summary_prop(&table, PUBLISHED_WAP_ID_PROP),
            None,
            "no wap.id ⇒ no published-wap-id"
        );
    }

    // ============================================================================================
    // Fast-forward (no new snapshot).
    // ============================================================================================

    /// FAST-FORWARD. A staged snapshot whose parent IS the current head moves `main` to the staged snapshot
    /// AS-IS — NO new snapshot is produced (the snapshot count is unchanged) and `main` points at the staged
    /// id. Risk pinned: a fast-forward that needlessly produces a new snapshot (re-stamps ids, duplicates the
    /// audit lineage) or fails to move main.
    #[tokio::test]
    async fn test_cherrypick_fast_forward_moves_main_without_new_snapshot() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // S0 on main; stage S1 (parent=S0) then roll main back to S0 so S1.parent == head.
        let table = append(&catalog, &table, vec![data_file("test/base.parquet", 9)]).await;
        let s0 = table.metadata().current_snapshot_id().unwrap();
        let table = append_wap(
            &catalog,
            &table,
            vec![data_file("test/s1.parquet", 0)],
            "wap-ff",
        )
        .await;
        let s1 = table.metadata().current_snapshot_id().unwrap();
        let table = set_current(&catalog, &table, s0).await;
        assert_eq!(table.metadata().current_snapshot_id(), Some(s0));

        let before = snapshot_count(&table);
        let table = cherry_pick(&catalog, &table, s1).await;

        assert_eq!(
            snapshot_count(&table),
            before,
            "a fast-forward produces NO new snapshot"
        );
        assert_eq!(
            table.metadata().current_snapshot_id(),
            Some(s1),
            "main is fast-forwarded to the staged snapshot itself"
        );
        // No source-snapshot-id / published-wap-id are stamped (the staged snapshot is published verbatim).
        assert_eq!(current_summary_prop(&table, SOURCE_SNAPSHOT_ID_PROP), None);
        let live = live_file_paths(&table).await;
        assert!(live.contains("test/s1.parquet") && live.contains("test/base.parquet"));
    }

    /// FF PRECEDENCE for an APPEND. An APPEND staged snapshot whose parent == head FAST-FORWARDS (no replay,
    /// no new snapshot) — Java checks `requireFastForward || isFastForward(base)` in `apply` BEFORE the
    /// merging/replay path (L193-204), so the append-with-parent==head case publishes the snapshot as-is.
    /// Risk pinned: replaying an append whose parent is the head (would mint a NEW id and break the
    /// "publish verbatim" contract — the snapshot count would grow). This is the exact precedence subtlety
    /// the brief flags.
    #[tokio::test]
    async fn test_cherrypick_append_with_parent_equal_head_fast_forwards() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // S0 on main; stage an APPEND S1 (parent=S0); roll main back to S0 so S1.parent == head.
        let table = append(&catalog, &table, vec![data_file("test/base.parquet", 9)]).await;
        let s0 = table.metadata().current_snapshot_id().unwrap();
        let table = append_wap(
            &catalog,
            &table,
            vec![data_file("test/s1.parquet", 0)],
            "wap-x",
        )
        .await;
        let s1 = table.metadata().current_snapshot_id().unwrap();
        let table = set_current(&catalog, &table, s0).await;

        let before = snapshot_count(&table);
        let table = cherry_pick(&catalog, &table, s1).await;

        // The append FAST-FORWARDED: no new snapshot, main == the staged append snapshot.
        assert_eq!(
            snapshot_count(&table),
            before,
            "an APPEND whose parent == head must FAST-FORWARD (no replay, no new snapshot)"
        );
        assert_eq!(table.metadata().current_snapshot_id(), Some(s1));
    }

    // ============================================================================================
    // Replace-partitions replay + the changed-partition rejection.
    // ============================================================================================

    /// Stage a replace-partitions OVERWRITE snapshot off a non-head parent (so cherry-pick REPLAYS).
    ///
    /// Sequence: S0 (a@x=0, b@x=1) on main; S_staged = replace_partitions(a2@x=0) onto S0 (operation=overwrite,
    /// replace-partitions=true, removes a, adds a2); roll main back to S0; then S_head = append(c@x=2) so
    /// `main = S_head` and `S_staged.parent = S0`. Returns `(table_at_head, staged_id, s0_id)`.
    async fn stage_replace_partitions_for_replay(
        catalog: &impl Catalog,
        table: &Table,
    ) -> (Table, i64, i64) {
        let table = append(catalog, table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 1),
        ])
        .await;
        let s0 = table.metadata().current_snapshot_id().unwrap();

        let tx = Transaction::new(&table);
        let action = tx
            .replace_partitions()
            .add_file(data_file("test/a2.parquet", 0));
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(catalog).await.unwrap();
        let staged_id = table.metadata().current_snapshot_id().unwrap();

        let table = set_current(catalog, &table, s0).await;
        let table = append(catalog, &table, vec![data_file("test/c.parquet", 2)]).await;
        (table, staged_id, s0)
    }

    /// HAPPY REPLACE-PARTITIONS. A staged replace-partitions snapshot is REPLAYED: a NEW `Overwrite` snapshot
    /// lands on main, the added file is live, the replaced partition's old file is removed, and the
    /// `source-snapshot-id` prop links the publish. Risk pinned: a replace-partitions publish that loses the
    /// adds or fails to drop the replaced-partition file (cross-partition data corruption).
    #[tokio::test]
    async fn test_cherrypick_replace_partitions_replays_adds_and_removes() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let (table, staged_id, _s0) = stage_replace_partitions_for_replay(&catalog, &table).await;

        let before = snapshot_count(&table);
        let table = cherry_pick(&catalog, &table, staged_id).await;

        assert_eq!(
            snapshot_count(&table),
            before + 1,
            "replay produces a new snapshot"
        );
        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Overwrite,
            "the publish records the picked OVERWRITE operation"
        );
        assert_eq!(
            current_summary_prop(&table, SOURCE_SNAPSHOT_ID_PROP),
            Some(staged_id.to_string())
        );
        let live = live_file_paths(&table).await;
        assert!(
            live.contains("test/a2.parquet"),
            "the replayed add is live: {live:?}"
        );
        assert!(
            !live.contains("test/a.parquet"),
            "the replaced-partition old file is dropped: {live:?}"
        );
        // The untouched partition (b@x=1) and the unrelated head file (c@x=2) survive.
        assert!(live.contains("test/b.parquet") && live.contains("test/c.parquet"));
    }

    /// CHANGED-PARTITION REJECTION. If a snapshot committed since the staged snapshot's parent ADDED a file
    /// to a replaced partition, the replace-partitions cherry-pick is rejected with the exact Java message
    /// ("Cannot cherry-pick replace partitions with changed partition: %s"). Risk pinned: publishing a
    /// dynamic overwrite over a partition that changed underneath it = a lost concurrent write.
    #[tokio::test]
    async fn test_cherrypick_replace_partitions_rejects_changed_partition() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // S0 (a@x=0, b@x=1); stage replace_partitions(a2@x=0); roll main back to S0.
        let table = append(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 1),
        ])
        .await;
        let s0 = table.metadata().current_snapshot_id().unwrap();
        let tx = Transaction::new(&table);
        let action = tx
            .replace_partitions()
            .add_file(data_file("test/a2.parquet", 0));
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let staged_id = table.metadata().current_snapshot_id().unwrap();
        let table = set_current(&catalog, &table, s0).await;

        // A concurrent append lands a file in the REPLACED partition x=0 (changing it underneath the pick).
        let table = append(&catalog, &table, vec![data_file("test/x0_new.parquet", 0)]).await;

        let err = cherry_pick_err(&catalog, &table, staged_id).await;
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(!err.retryable(), "a validation conflict is non-retryable");
        assert!(
            err.message()
                .contains("Cannot cherry-pick replace partitions with changed partition"),
            "unexpected message: {}",
            err.message()
        );
    }

    /// NEGATIVE CONTROL for the changed-partition check: a concurrent append in an UNTOUCHED partition does
    /// NOT block the replace-partitions cherry-pick. Risk pinned: an over-eager check rejecting a legal
    /// publish (false positive).
    #[tokio::test]
    async fn test_cherrypick_replace_partitions_allows_change_in_other_partition() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let table = append(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 1),
        ])
        .await;
        let s0 = table.metadata().current_snapshot_id().unwrap();
        let tx = Transaction::new(&table);
        let action = tx
            .replace_partitions()
            .add_file(data_file("test/a2.parquet", 0));
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let staged_id = table.metadata().current_snapshot_id().unwrap();
        let table = set_current(&catalog, &table, s0).await;

        // Concurrent append in the UNTOUCHED partition x=2 — not a conflict.
        let table = append(&catalog, &table, vec![data_file("test/x2_new.parquet", 2)]).await;

        let table = cherry_pick(&catalog, &table, staged_id).await;
        let live = live_file_paths(&table).await;
        assert!(
            live.contains("test/a2.parquet"),
            "the publish succeeded: {live:?}"
        );
        assert!(
            live.contains("test/x2_new.parquet"),
            "the untouched x=2 file survives"
        );
    }

    // ============================================================================================
    // Non-ancestor / already-ancestor / dedup / duplicate-WAP / unknown / not-eligible.
    // ============================================================================================

    /// NON-ANCESTOR OVERWRITE REJECTION. A staged replace-partitions snapshot whose parent is NOT an ancestor
    /// of the current head (a sibling line) cannot be cherry-picked — the change-detection window would be
    /// meaningless. Java L101-105 message: "Cannot cherry-pick overwrite not based on an ancestor of the
    /// current state: %s". Risk pinned: replaying a dynamic overwrite against an unrelated base.
    ///
    /// The dispatch (`plan`) is what enforces this, so this test drives the action's `commit` DIRECTLY against
    /// a grafted fixture whose `main` points at a SIBLING of the staged snapshot's parent (so the parent is no
    /// longer an ancestor of the head) — bypassing the catalog reload that `Transaction::commit` performs (a
    /// committed sibling line off a single-root catalog cannot be constructed). Mirrors the manage_snapshots
    /// `forked_table` direct-`commit` pattern.
    #[tokio::test]
    async fn test_cherrypick_replace_partitions_rejects_non_ancestor_parent() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // S0 (a@x=0). Stage replace_partitions(a2@x=0) onto S0 → S_staged.parent = S0.
        let table = append(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;
        let s0 = table.metadata().current_snapshot_id().unwrap();
        let tx = Transaction::new(&table);
        let action = tx
            .replace_partitions()
            .add_file(data_file("test/a2.parquet", 0));
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let staged_id = table.metadata().current_snapshot_id().unwrap();

        // Graft a sibling head so S0 (the staged snapshot's parent) is NOT an ancestor of the head.
        let sibling_table = grafted_sibling_head(&table, s0).await;

        let action = Arc::new(CherryPickAction::new(staged_id));
        let err = action
            .commit(&sibling_table)
            .await
            .map(drop)
            .expect_err("cherry-pick of a non-ancestor-parent overwrite must fail");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message().contains(
                "Cannot cherry-pick overwrite not based on an ancestor of the current state"
            ),
            "unexpected message: {}",
            err.message()
        );
    }

    /// Build an in-memory table view whose `main` points at a NEW root snapshot that is a SIBLING of `s0`
    /// (shares no ancestry with it), so `s0` is no longer an ancestor of the head. This grafts a snapshot
    /// without committing through the catalog — it is a metadata-only fixture for the non-ancestor path, used
    /// only by tests that call the action's `commit` DIRECTLY (no catalog reload).
    async fn grafted_sibling_head(table: &Table, s0: i64) -> Table {
        use crate::spec::{Operation, Snapshot, SnapshotReference, SnapshotRetention, Summary};

        // A sibling root snapshot (parent = None, so it is NOT a descendant of s0, and s0 is not its ancestor).
        let sibling_id = s0 ^ 0x5555_5555;
        let s0_snapshot = table.metadata().snapshot_by_id(s0).unwrap();
        let next_row_id = table.metadata().next_row_id();
        let sibling = Snapshot::builder()
            .with_snapshot_id(sibling_id)
            .with_parent_snapshot_id(None)
            .with_sequence_number(s0_snapshot.sequence_number() + 100)
            .with_timestamp_ms(s0_snapshot.timestamp_ms() + 100_000)
            .with_manifest_list(s0_snapshot.manifest_list().to_string())
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .with_schema_id(s0_snapshot.schema_id().unwrap_or(0))
            // V3 requires a row range (first-row-id) on every added snapshot.
            .with_row_range(next_row_id, 0)
            .build();
        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .add_snapshot(sibling)
            .unwrap()
            .set_ref(
                MAIN_BRANCH,
                SnapshotReference::new(sibling_id, SnapshotRetention::branch(None, None, None)),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        table.clone().with_metadata(std::sync::Arc::new(metadata))
    }

    /// ALREADY-AN-ANCESTOR REJECTION. Cherry-picking a snapshot that is ALREADY on `main`'s ancestry fails
    /// with the exact Java message ("Cannot cherrypick snapshot %s: already an ancestor"). Risk pinned: a
    /// redundant publish of an already-published snapshot (a double-apply of the same data).
    #[tokio::test]
    async fn test_cherrypick_already_ancestor_is_rejected() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // S0 then S1 both on main; S0 is now an ancestor of the head. Cherry-picking S0 must reject.
        let table = append(&catalog, &table, vec![data_file("test/base.parquet", 9)]).await;
        let s0 = table.metadata().current_snapshot_id().unwrap();
        let table = append(&catalog, &table, vec![data_file("test/s1.parquet", 0)]).await;

        let err = cherry_pick_err(&catalog, &table, s0).await;
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message().contains(&format!(
                "Cannot cherrypick snapshot {s0}: already an ancestor"
            )),
            "unexpected message: {}",
            err.message()
        );
    }

    /// DOUBLE-PUBLISH DEDUP. Cherry-pick a staged append once (publishing P, tagged
    /// `source-snapshot-id = staged`), then cherry-pick the SAME staged id again → rejected with the
    /// "already picked to create ancestor" variant (Java `validateNonAncestor`'s
    /// `lookupAncestorBySourceSnapshot` path). Risk pinned: republishing the SAME staged data twice (the
    /// exact double-apply the `source-snapshot-id` link exists to prevent) — THE correctness headline.
    #[tokio::test]
    async fn test_cherrypick_double_publish_is_deduped_via_source_snapshot_id() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let (table, staged_id, _s0) =
            stage_append_for_replay(&catalog, &table, "test/staged.parquet", 0, "wap-dedup").await;

        // First publish: succeeds, tags the published snapshot with source-snapshot-id = staged_id.
        let table = cherry_pick(&catalog, &table, staged_id).await;
        let published_id = table.metadata().current_snapshot_id().unwrap();

        // Second publish of the SAME staged id: rejected as already-cherry-picked.
        let err = cherry_pick_err(&catalog, &table, staged_id).await;
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message().contains(&format!(
                "Cannot cherrypick snapshot {staged_id}: already picked to create ancestor {published_id}"
            )),
            "unexpected message: {}",
            err.message()
        );
    }

    /// DUPLICATE WAP ID. Two staged snapshots share the SAME `wap.id`. Publish one (so its wap id becomes
    /// published among the ancestors), then cherry-pick the OTHER → rejected with the exact
    /// `DuplicateWAPCommitException` message. Risk pinned: publishing the same logical WAP write twice via two
    /// distinct staged snapshots that share a wap id (a silent duplicate).
    #[tokio::test]
    async fn test_cherrypick_duplicate_wap_id_is_rejected() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // S0 base on main.
        let table = append(&catalog, &table, vec![data_file("test/base.parquet", 9)]).await;
        let s0 = table.metadata().current_snapshot_id().unwrap();

        // Stage TWO append snapshots off S0, both with the SAME wap id.
        let table = append_wap(
            &catalog,
            &table,
            vec![data_file("test/w1.parquet", 0)],
            "wap-dup",
        )
        .await;
        let staged_1 = table.metadata().current_snapshot_id().unwrap();
        let table = set_current(&catalog, &table, s0).await;
        let table = append_wap(
            &catalog,
            &table,
            vec![data_file("test/w2.parquet", 1)],
            "wap-dup",
        )
        .await;
        let staged_2 = table.metadata().current_snapshot_id().unwrap();
        let table = set_current(&catalog, &table, s0).await;

        // Advance main off S0 so neither staged snapshot fast-forwards (both parents = S0 != head).
        let table = append(&catalog, &table, vec![data_file("test/head.parquet", 9)]).await;

        // Publish staged_1 (its wap.id becomes published among ancestors).
        let table = cherry_pick(&catalog, &table, staged_1).await;

        // Publishing staged_2 (same wap id) must fail as a duplicate WAP commit.
        let err = cherry_pick_err(&catalog, &table, staged_2).await;
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message().contains(
                "Duplicate request to cherry pick wap id that was published already: wap-dup"
            ),
            "unexpected message: {}",
            err.message()
        );
    }

    /// UNKNOWN ID. Cherry-picking a snapshot id absent from metadata fails with the exact Java message
    /// ("Cannot cherry-pick unknown snapshot ID: %s"). Risk pinned: a silent no-op (or panic) on a bad id.
    #[tokio::test]
    async fn test_cherrypick_unknown_snapshot_id_is_rejected() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append(&catalog, &table, vec![data_file("test/base.parquet", 9)]).await;

        let err = cherry_pick_err(&catalog, &table, 99_999).await;
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message()
                .contains("Cannot cherry-pick unknown snapshot ID: 99999"),
            "unexpected message: {}",
            err.message()
        );
    }

    /// NOT-ELIGIBLE OPERATION. A staged DELETE snapshot (operation=delete, not append/dynamic-overwrite) whose
    /// parent is not the head cannot be cherry-picked: "Cannot cherry-pick snapshot %s: not append, dynamic
    /// overwrite, or fast-forward". Risk pinned: silently replaying an unsupported operation incorrectly.
    #[tokio::test]
    async fn test_cherrypick_non_eligible_delete_operation_is_rejected() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // S0 (a@x=0, b@x=1) on main.
        let table = append(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 1),
        ])
        .await;
        let s0 = table.metadata().current_snapshot_id().unwrap();

        // Stage a DELETE snapshot (delete a@x=0) → operation = delete.
        let tx = Transaction::new(&table);
        let action = tx.delete_files().delete_file("test/a.parquet");
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let staged_delete = table.metadata().current_snapshot_id().unwrap();
        assert_eq!(
            table
                .metadata()
                .snapshot_by_id(staged_delete)
                .unwrap()
                .summary()
                .operation,
            Operation::Delete
        );
        // Roll main back to S0, then advance to a sibling head so the staged delete's parent (S0) != head.
        let table = set_current(&catalog, &table, s0).await;
        let table = append(&catalog, &table, vec![data_file("test/head.parquet", 2)]).await;

        let err = cherry_pick_err(&catalog, &table, staged_delete).await;
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message().contains(&format!(
                "Cannot cherry-pick snapshot {staged_delete}: not append, dynamic overwrite, or fast-forward"
            )),
            "unexpected message: {}",
            err.message()
        );
    }

    // ============================================================================================
    // Reviewer-added pins (2026-06-11): the dangling-publish negative control (the dedup scope —
    // CURRENT ancestors only, not all snapshots), the FF-precedence cell for a NON-eligible op, and
    // the multi-spec replay pin (converted 2026-06-11 from the former fail-loud divergence to the
    // per-spec Java-parity contract — the replay now SUCCEEDS under its own spec id).
    // ============================================================================================

    /// DANGLING-PUBLISH NEGATIVE CONTROL (the double-publish dedup SCOPE). The `source-snapshot-id`
    /// double-publish dedup (`lookupAncestorBySourceSnapshot`) walks ONLY the CURRENT ancestry (Java
    /// `currentAncestors(meta)`), NOT every snapshot in metadata. So a prior publish of the same staged
    /// snapshot that was later rolled OFF `main` (now dangling) must NOT block a fresh re-publish.
    ///
    /// Risk pinned: a dedup that scanned ALL snapshots (incl. dangling) would FALSELY reject a legitimate
    /// re-publish after a rollback — the exact over-broadening this test catches (a reviewer mutation that
    /// scanned `metadata.snapshots()` instead of the ancestry chain passed all OTHER tests; this is the
    /// negative control that fails it). The flip side of the dedup: it must dedup against LIVE lineage only.
    #[tokio::test]
    async fn test_cherrypick_dangling_prior_publish_does_not_block_republish() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // S0 base, then S_head1 on main; stage S_staged off S0 (so it REPLAYS, parent S0 != head).
        let table = append(&catalog, &table, vec![data_file("test/base.parquet", 9)]).await;
        let s0 = table.metadata().current_snapshot_id().unwrap();
        let table = append_wap(
            &catalog,
            &table,
            vec![data_file("test/staged.parquet", 0)],
            "",
        )
        .await;
        let staged_id = table.metadata().current_snapshot_id().unwrap();
        let table = set_current(&catalog, &table, s0).await;
        let table = append(&catalog, &table, vec![data_file("test/head1.parquet", 9)]).await;
        let head1 = table.metadata().current_snapshot_id().unwrap();

        // First publish: P lands on main (parent = head1), tagged source-snapshot-id = staged_id.
        let table = cherry_pick(&catalog, &table, staged_id).await;
        let published_p = table.metadata().current_snapshot_id().unwrap();
        assert_ne!(published_p, head1);

        // Roll main back to head1 so the prior publish P is now DANGLING (off the current ancestry). The
        // staged snapshot is STILL not on main (its parent S0 != head1, so it would replay again).
        let table = set_current(&catalog, &table, head1).await;
        assert!(
            table.metadata().snapshot_by_id(published_p).is_some(),
            "P still exists in metadata (dangling), it was not removed"
        );

        // Re-publish the SAME staged id. With the ancestry-only dedup, the dangling P does NOT block it —
        // a fresh publish succeeds. (An all-snapshots dedup would FALSELY reject here.)
        let table = cherry_pick(&catalog, &table, staged_id).await;
        let republished = table.metadata().current_snapshot_id().unwrap();
        assert_ne!(
            republished, published_p,
            "the re-publish produced a NEW snapshot, not the dangling P"
        );
        assert_eq!(
            current_summary_prop(&table, SOURCE_SNAPSHOT_ID_PROP),
            Some(staged_id.to_string()),
            "the fresh publish re-links source-snapshot-id"
        );
        let live = live_file_paths(&table).await;
        assert!(
            live.contains("test/staged.parquet"),
            "the replayed staged file is live after re-publish: {live:?}"
        );
    }

    /// FF-PRECEDENCE FOR A NON-ELIGIBLE OP (matrix cell `{picked op = delete} × {parent == head}`). A staged
    /// DELETE whose parent IS the current head FAST-FORWARDS — Java's `else` branch accepts ANY operation as
    /// long as it is fast-forwardable (`cherrypick(long)` L131-138 sets `requireFastForward` only AFTER the
    /// `isFastForward(current)` check passes; `apply` L193-204 then publishes verbatim). The Rust dispatch
    /// runs the FF check FIRST, unconditionally, so a delete-with-parent==head publishes as-is with NO replay
    /// and NO new snapshot. Risk pinned: rejecting a fast-forwardable delete (the not-eligible error firing on
    /// a case Java fast-forwards), or replaying it (which would mint a new id / re-stamp lineage).
    #[tokio::test]
    async fn test_cherrypick_delete_with_parent_equal_head_fast_forwards() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // S0 (a@x=0, b@x=1) on main.
        let table = append(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 1),
        ])
        .await;
        let s0 = table.metadata().current_snapshot_id().unwrap();

        // Stage a DELETE snapshot (delete a@x=0) off S0 → operation = delete, parent = S0.
        let tx = Transaction::new(&table);
        let action = tx.delete_files().delete_file("test/a.parquet");
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let staged_delete = table.metadata().current_snapshot_id().unwrap();
        assert_eq!(
            table
                .metadata()
                .snapshot_by_id(staged_delete)
                .unwrap()
                .summary()
                .operation,
            Operation::Delete
        );

        // Roll main back to S0 so the staged delete's parent (S0) == head — now it is FAST-FORWARDABLE.
        let table = set_current(&catalog, &table, s0).await;

        let before = snapshot_count(&table);
        let table = cherry_pick(&catalog, &table, staged_delete).await;

        // The delete FAST-FORWARDED: no new snapshot, main == the staged delete snapshot itself.
        assert_eq!(
            snapshot_count(&table),
            before,
            "a fast-forwardable delete must publish VERBATIM (no replay, no new snapshot)"
        );
        assert_eq!(
            table.metadata().current_snapshot_id(),
            Some(staged_delete),
            "main is fast-forwarded to the staged delete snapshot"
        );
        // The delete took effect: a@x=0 is gone, b@x=1 survives.
        let live = live_file_paths(&table).await;
        assert!(
            !live.contains("test/a.parquet"),
            "the deleted file is gone after FF: {live:?}"
        );
        assert!(
            live.contains("test/b.parquet"),
            "the untouched file survives: {live:?}"
        );
    }

    /// MULTI-SPEC REPLAY — JAVA PARITY (converted 2026-06-11 from the former fail-loud divergence pin).
    /// Java's cherry-pick add path (`MergingSnapshotProducer.add(DataFile)`) preserves each added file's OWN
    /// `specId()` and writes per-spec manifests, so replaying a snapshot whose files belong to an OLDER
    /// partition spec onto a table whose default spec has since moved SUCCEEDS. The Rust producer now mirrors
    /// this: `validate_added_data_files` accepts any added file whose `partition_spec_id` EXISTS in the
    /// table's specs, and `write_added_manifests` writes ONE manifest per partition-spec group.
    ///
    /// Risk pinned: the replayed spec-0 staged file must (a) commit successfully (no spurious spec-mismatch
    /// rejection), (b) be written into a manifest stamped with ITS OWN spec id 0 (NOT the new default), and
    /// (c) be live after publish so a scan reads it. A regression that reverted to default-spec-only writing
    /// would either reject the commit or stamp the manifest under the new spec id (partition corruption).
    #[tokio::test]
    async fn test_cherrypick_multispec_replay_produces_per_spec_manifest() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // S0 base on main (default spec 0 = identity(x)).
        let table = append(&catalog, &table, vec![data_file("test/base.parquet", 9)]).await;
        let s0 = table.metadata().current_snapshot_id().unwrap();
        // Stage a spec-0 append off S0.
        let table = append_wap(
            &catalog,
            &table,
            vec![data_file("test/staged.parquet", 0)],
            "",
        )
        .await;
        let staged_id = table.metadata().current_snapshot_id().unwrap();
        let table = set_current(&catalog, &table, s0).await;

        // Evolve the partition spec (add identity(y)) → default spec id moves to 1.
        let tx = Transaction::new(&table);
        let action = tx.update_partition_spec().add_field("y");
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let new_spec = table.metadata().default_partition_spec_id();
        assert_ne!(
            new_spec, 0,
            "the spec evolved away from the staged file's spec 0"
        );

        // Advance main so the staged snapshot's parent (S0) != head ⇒ it must REPLAY (not fast-forward).
        let table = append(&catalog, &table, vec![data_file_spec(
            "test/head.parquet",
            9,
            7,
            new_spec,
        )])
        .await;

        // Replaying the spec-0 staged files onto a table whose default spec is now 1 SUCCEEDS (Java parity).
        let before = snapshot_count(&table);
        let table = cherry_pick(&catalog, &table, staged_id).await;
        assert_eq!(
            snapshot_count(&table),
            before + 1,
            "an old-spec append replay produces a NEW snapshot"
        );

        // The replayed spec-0 file is live (a scan reads it) and the head spec-1 file survives.
        let live = live_file_paths(&table).await;
        assert!(
            live.contains("test/staged.parquet"),
            "the replayed old-spec file is live after publish: {live:?}"
        );
        assert!(
            live.contains("test/head.parquet"),
            "the new-spec head file survives: {live:?}"
        );

        // The replayed file is in a manifest stamped with ITS OWN spec id 0, NOT the new default spec.
        let published = table.metadata().current_snapshot().unwrap();
        let manifest_list = published
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let mut staged_spec_id = None;
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                if entry.file_path() == "test/staged.parquet" {
                    staged_spec_id = Some(manifest_file.partition_spec_id);
                }
            }
        }
        assert_eq!(
            staged_spec_id,
            Some(0),
            "the replayed old-spec file must live in a manifest stamped with spec id 0, not the new default"
        );
    }

    // ============================================================================================
    // WAP-PATH publish dedup (Group V increment V2, 2026-06-11). The `wap.id`-keyed duplicate-publish
    // rejection (Java `WapUtil.validateWapPublish` / `isWapIdPublished`), complementing the ancestry-path
    // dedup above. `is_wap_id_published` walks the CURRENT ancestors and compares the picked snapshot's
    // `wap.id` against EACH ancestor's OWN `wap.id` (STAGED_WAP_ID_PROP) AND its `published-wap-id`
    // (PUBLISHED_WAP_ID_PROP) — both arms (1.10.0 bytecode `WapUtil.isWapIdPublished` offsets 53-74).
    //
    // The existing `test_cherrypick_duplicate_wap_id_is_rejected` exercises the REPLAY → `published-wap-id`
    // arm (both staged snapshots replay because their parent != head, so the first publish mints a NEW
    // snapshot stamped with `published-wap-id`). The tests below add the FAST-FORWARD → own-`wap.id` arm
    // (a FF publish does NOT restamp `published-wap-id`, so the second publish is caught only by the
    // STAGED `wap.id` arm), the no-false-positive distinct-ids case, the non-WAP negative control, the
    // state-unchanged-on-rejection re-parse, and the both-paths ordering pin.
    // ============================================================================================

    /// FAST-FORWARD-PATH WAP DEDUP (crown jewel — pins the STAGED `wap.id` arm of `is_wap_id_published`).
    /// Stage S1 with `wap.id = X` off the current head (so cherry-pick FAST-FORWARDS — publishes S1 verbatim
    /// onto `main` WITHOUT minting a new snapshot or stamping `published-wap-id`). Stage S2 ALSO with
    /// `wap.id = X` off the same parent. After the FF publish of S1, the second cherry-pick of S2 is REJECTED
    /// with the verbatim `DuplicateWAPCommitException` message.
    ///
    /// Risk pinned: a duplicate WAP publish via the FF path slips through because `published-wap-id` was never
    /// stamped (the FF publishes verbatim). Java catches it because `isWapIdPublished` also compares each
    /// ancestor's OWN `wap.id`: after the FF, S1 is a current ancestor carrying `wap.id = X`. A dedup that
    /// only checked `published-wap-id` (dropping the STAGED arm) would FALSELY ALLOW this double-apply — the
    /// exact corruption WAP exists to prevent. This is the FF-path coverage the brief flags; the existing
    /// replay test does NOT cover it (replay stamps `published-wap-id`, so it hits the other arm).
    /// Mirror of Java `TestWapWorkflow.testDuplicateCherrypick` (its first publish IS a fast-forward).
    #[tokio::test]
    async fn test_cherrypick_duplicate_wap_id_rejected_via_fast_forward_published() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // S0 base on main.
        let table = append(&catalog, &table, vec![data_file("test/base.parquet", 9)]).await;
        let s0 = table.metadata().current_snapshot_id().unwrap();

        // Stage TWO append snapshots off S0, both with the SAME wap id. Both parents == S0.
        let table = append_wap(
            &catalog,
            &table,
            vec![data_file("test/w1.parquet", 0)],
            "wap-ff-dup",
        )
        .await;
        let staged_1 = table.metadata().current_snapshot_id().unwrap();
        let table = set_current(&catalog, &table, s0).await;
        let table = append_wap(
            &catalog,
            &table,
            vec![data_file("test/w2.parquet", 1)],
            "wap-ff-dup",
        )
        .await;
        let staged_2 = table.metadata().current_snapshot_id().unwrap();
        // Roll main back to S0 so staged_1's parent (S0) == head ⇒ publishing staged_1 FAST-FORWARDS.
        let table = set_current(&catalog, &table, s0).await;

        // Publish staged_1 via FAST-FORWARD: no new snapshot, main moves to staged_1 verbatim.
        let before = snapshot_count(&table);
        let table = cherry_pick(&catalog, &table, staged_1).await;
        assert_eq!(
            snapshot_count(&table),
            before,
            "publishing staged_1 with parent == head must FAST-FORWARD (no new snapshot)"
        );
        assert_eq!(
            table.metadata().current_snapshot_id(),
            Some(staged_1),
            "main fast-forwarded to staged_1 itself"
        );
        // The FF publish did NOT stamp published-wap-id — staged_1 carries only its own wap.id.
        assert_eq!(
            current_summary_prop(&table, PUBLISHED_WAP_ID_PROP),
            None,
            "a fast-forward publishes verbatim and does NOT restamp published-wap-id"
        );
        assert_eq!(
            current_summary_prop(&table, STAGED_WAP_ID_PROP),
            Some("wap-ff-dup".to_string()),
            "the fast-forwarded snapshot keeps its own wap.id"
        );

        // Publishing staged_2 (same wap id) must fail — caught via the STAGED wap.id arm (staged_1 is now an
        // ancestor carrying wap.id = wap-ff-dup; published-wap-id was never stamped).
        let err = cherry_pick_err(&catalog, &table, staged_2).await;
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            !err.retryable(),
            "a duplicate-WAP rejection is non-retryable"
        );
        assert!(
            err.message().contains(
                "Duplicate request to cherry pick wap id that was published already: wap-ff-dup"
            ),
            "unexpected message: {}",
            err.message()
        );
    }

    /// STATE-UNCHANGED ON REJECTION (the rejected attempt must not mutate the table). Re-parse the table
    /// metadata before and after a rejected duplicate-WAP cherry-pick: current-snapshot-id, the live file
    /// set, and the snapshot count are all IDENTICAL. Risk pinned: a rejection that still committed a partial
    /// update (a stamped source-snapshot-id, a moved ref, an added snapshot) — the double-apply WAP guards
    /// against would land anyway if the validation rejected AFTER mutating metadata.
    #[tokio::test]
    async fn test_cherrypick_duplicate_wap_rejection_leaves_table_unchanged() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // S0 base; stage two same-wap-id appends off S0 (both REPLAY — parent S0 != head after the head append).
        let table = append(&catalog, &table, vec![data_file("test/base.parquet", 9)]).await;
        let s0 = table.metadata().current_snapshot_id().unwrap();
        let table = append_wap(
            &catalog,
            &table,
            vec![data_file("test/w1.parquet", 0)],
            "wap-state",
        )
        .await;
        let staged_1 = table.metadata().current_snapshot_id().unwrap();
        let table = set_current(&catalog, &table, s0).await;
        let table = append_wap(
            &catalog,
            &table,
            vec![data_file("test/w2.parquet", 1)],
            "wap-state",
        )
        .await;
        let staged_2 = table.metadata().current_snapshot_id().unwrap();
        let table = set_current(&catalog, &table, s0).await;
        let table = append(&catalog, &table, vec![data_file("test/head.parquet", 9)]).await;

        // Publish staged_1 (its wap id becomes published among ancestors).
        let table = cherry_pick(&catalog, &table, staged_1).await;

        // Snapshot the table state BEFORE the rejected attempt (the re-parse oracle).
        let current_before = table.metadata().current_snapshot_id();
        let count_before = snapshot_count(&table);
        let live_before = live_file_paths(&table).await;

        // The second publish (same wap id) is rejected.
        let err = cherry_pick_err(&catalog, &table, staged_2).await;
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message().contains(
                "Duplicate request to cherry pick wap id that was published already: wap-state"
            ),
            "unexpected message: {}",
            err.message()
        );

        // Re-load the table from the catalog (re-parse) and assert NOTHING moved.
        let reloaded = catalog
            .load_table(table.identifier())
            .await
            .expect("reload after rejected cherry-pick");
        assert_eq!(
            reloaded.metadata().current_snapshot_id(),
            current_before,
            "the rejected attempt must not move current-snapshot-id"
        );
        assert_eq!(
            snapshot_count(&reloaded),
            count_before,
            "the rejected attempt must not add a snapshot"
        );
        assert_eq!(
            live_file_paths(&reloaded).await,
            live_before,
            "the rejected attempt must not change the live file set"
        );
        // The would-be-double-applied file (w2) is NOT live — the double-apply was prevented.
        assert!(
            !live_file_paths(&reloaded).await.contains("test/w2.parquet"),
            "the rejected staged file must NOT have been applied"
        );
    }

    /// NO FALSE POSITIVE — distinct wap ids both publish. Stage S1 with `wap.id = X` and S2 with
    /// `wap.id = Y` (X != Y); both cherry-pick successfully. Risk pinned: an over-eager dedup that rejected
    /// a SECOND legitimate WAP publish whose id differs (e.g. comparing presence-of-any-wap-id instead of the
    /// id value). Mirror of Java `TestWapWorkflow`'s distinct-wap-id workflow.
    #[tokio::test]
    async fn test_cherrypick_distinct_wap_ids_both_publish() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // S0 base; stage S1 (wap=X) and S2 (wap=Y) off S0.
        let table = append(&catalog, &table, vec![data_file("test/base.parquet", 9)]).await;
        let s0 = table.metadata().current_snapshot_id().unwrap();
        let table = append_wap(
            &catalog,
            &table,
            vec![data_file("test/w1.parquet", 0)],
            "wap-X",
        )
        .await;
        let staged_1 = table.metadata().current_snapshot_id().unwrap();
        let table = set_current(&catalog, &table, s0).await;
        let table = append_wap(
            &catalog,
            &table,
            vec![data_file("test/w2.parquet", 1)],
            "wap-Y",
        )
        .await;
        let staged_2 = table.metadata().current_snapshot_id().unwrap();
        let table = set_current(&catalog, &table, s0).await;
        // Advance main off S0 so both staged snapshots REPLAY (parents = S0 != head).
        let table = append(&catalog, &table, vec![data_file("test/head.parquet", 9)]).await;

        // Publish S1 (wap=X) then S2 (wap=Y) — distinct ids, no false positive.
        let table = cherry_pick(&catalog, &table, staged_1).await;
        assert_eq!(
            current_summary_prop(&table, PUBLISHED_WAP_ID_PROP),
            Some("wap-X".to_string())
        );
        let table = cherry_pick(&catalog, &table, staged_2).await;
        assert_eq!(
            current_summary_prop(&table, PUBLISHED_WAP_ID_PROP),
            Some("wap-Y".to_string()),
            "a distinct wap id publishes fine — no false positive from the dedup"
        );
        let live = live_file_paths(&table).await;
        assert!(
            live.contains("test/w1.parquet") && live.contains("test/w2.parquet"),
            "both distinct-wap-id files are live: {live:?}"
        );
    }

    /// NON-WAP NEGATIVE CONTROL — a staged snapshot with NO `wap.id` cherry-picks without touching the WAP
    /// dedup. Two distinct non-WAP staged appends both publish; the WAP-publish check is a no-op for them
    /// (`validate_wap_publish` returns early when the picked snapshot has no non-empty `wap.id`). Risk pinned:
    /// the dedup spuriously rejecting a non-WAP cherry-pick (e.g. treating an ABSENT/empty `wap.id` as a
    /// matchable value, so two non-WAP publishes collide on the empty string).
    #[tokio::test]
    async fn test_cherrypick_non_wap_snapshots_bypass_wap_dedup() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // S0 base; stage two NON-WAP appends off S0 (empty wap id ⇒ treated as non-WAP).
        let table = append(&catalog, &table, vec![data_file("test/base.parquet", 9)]).await;
        let s0 = table.metadata().current_snapshot_id().unwrap();
        let table = append_wap(&catalog, &table, vec![data_file("test/n1.parquet", 0)], "").await;
        let staged_1 = table.metadata().current_snapshot_id().unwrap();
        let table = set_current(&catalog, &table, s0).await;
        let table = append_wap(&catalog, &table, vec![data_file("test/n2.parquet", 1)], "").await;
        let staged_2 = table.metadata().current_snapshot_id().unwrap();
        let table = set_current(&catalog, &table, s0).await;
        let table = append(&catalog, &table, vec![data_file("test/head.parquet", 9)]).await;

        // Both non-WAP staged snapshots publish — the empty wap id never collides in the dedup.
        let table = cherry_pick(&catalog, &table, staged_1).await;
        assert_eq!(
            current_summary_prop(&table, PUBLISHED_WAP_ID_PROP),
            None,
            "a non-WAP publish stamps NO published-wap-id"
        );
        let table = cherry_pick(&catalog, &table, staged_2).await;
        assert_eq!(
            current_summary_prop(&table, PUBLISHED_WAP_ID_PROP),
            None,
            "the second non-WAP publish is NOT blocked by the dedup (empty wap id is not matchable)"
        );
        let live = live_file_paths(&table).await;
        assert!(
            live.contains("test/n1.parquet") && live.contains("test/n2.parquet"),
            "both non-WAP files are live: {live:?}"
        );
    }

    /// ORDERING PIN (both dedup paths apply). When a staged snapshot is BOTH already an ancestor of the head
    /// AND carries a `wap.id` already published among the ancestors, Java's `validate` runs
    /// `validateNonAncestor` BEFORE `validateWapPublish` (1.10.0 bytecode `CherryPickOperation.validate`
    /// offsets 8-55), so the ANCESTRY error fires first. Here we re-pick the SAME staged snapshot after it was
    /// fast-forwarded onto main: it is now an ancestor (ancestry path) AND its own `wap.id` is published (WAP
    /// path) — both conditions hold. The error must be the ancestry one ("already an ancestor"), NOT the WAP
    /// one. Risk pinned: mirroring Java's rejection ORDER — a port that ran the WAP check first would surface
    /// the wrong (DuplicateWAPCommitException-shaped) message for an already-ancestor pick.
    #[tokio::test]
    async fn test_cherrypick_both_dedup_paths_ancestry_error_fires_first() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // S0 base; stage S1 (wap=Z) off S0 with parent == head so it FAST-FORWARDS on publish.
        let table = append(&catalog, &table, vec![data_file("test/base.parquet", 9)]).await;
        let s0 = table.metadata().current_snapshot_id().unwrap();
        let table = append_wap(
            &catalog,
            &table,
            vec![data_file("test/s1.parquet", 0)],
            "wap-Z",
        )
        .await;
        let s1 = table.metadata().current_snapshot_id().unwrap();
        let table = set_current(&catalog, &table, s0).await;

        // Fast-forward publish S1 onto main: S1 is now the head (and an ancestor), carrying wap.id = Z.
        let table = cherry_pick(&catalog, &table, s1).await;
        assert_eq!(table.metadata().current_snapshot_id(), Some(s1));

        // Re-pick S1: it is BOTH already an ancestor AND its wap id (Z) is published (on S1 itself). Java's
        // validate runs validateNonAncestor FIRST ⇒ the ancestry error wins, NOT the duplicate-WAP error.
        let err = cherry_pick_err(&catalog, &table, s1).await;
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message().contains(&format!(
                "Cannot cherrypick snapshot {s1}: already an ancestor"
            )),
            "the ANCESTRY error must fire first (Java order), got: {}",
            err.message()
        );
        assert!(
            !err.message()
                .contains("Duplicate request to cherry pick wap id"),
            "the WAP error must NOT be the one surfaced when both paths apply, got: {}",
            err.message()
        );
    }

    /// ROLLBACK REOPENS A WAP ID — DOCUMENTED JAVA-FAITHFUL ESCAPE HATCH. The WAP dedup walks the CURRENT
    /// ancestry ONLY (`is_wap_id_published` roots at `metadata.current_snapshot_id()` — the exact mirror of
    /// Java `WapUtil.isWapIdPublished`, which walks `SnapshotUtil.ancestorIds(meta.currentSnapshot(), ...)`;
    /// `SnapshotUtil.ancestorIds` → `ancestorsOf` follows parent links from that root). So once `main` is
    /// rolled BACK past a WAP publish, the publishing snapshot leaves the current ancestry and its `wap.id` is
    /// no longer "published" for dedup purposes — a SECOND staged snapshot carrying the SAME `wap.id` then
    /// publishes successfully (a double-publish *after* a rollback). This is NOT a Rust divergence: Java has the
    /// identical hole because both implementations key the dedup off the live ancestry chain, by design (WAP
    /// deduplicates publishes that are still on the line, not every publish that ever happened).
    ///
    /// Risk pinned: that the dedup-walk SCOPE matches Java's exactly. A port that walked ALL snapshots (or any
    /// retained-but-orphaned chain) instead of the current ancestry would REJECT this second publish — a
    /// stricter-than-Java divergence that would surface as a spurious `DuplicateWAPCommitException` after a
    /// legitimate rollback-and-redo. The test asserts the second publish SUCCEEDS (the Java-faithful outcome).
    #[tokio::test]
    async fn test_cherrypick_rollback_reopens_wap_id_java_faithful() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // S0 base; stage S1 (wap=R) and S2 (wap=R, same id) off S0, both REPLAY (parent S0 != head).
        let table = append(&catalog, &table, vec![data_file("test/base.parquet", 9)]).await;
        let s0 = table.metadata().current_snapshot_id().unwrap();
        let table = append_wap(
            &catalog,
            &table,
            vec![data_file("test/w1.parquet", 0)],
            "wap-R",
        )
        .await;
        let staged_1 = table.metadata().current_snapshot_id().unwrap();
        let table = set_current(&catalog, &table, s0).await;
        let table = append_wap(
            &catalog,
            &table,
            vec![data_file("test/w2.parquet", 1)],
            "wap-R",
        )
        .await;
        let staged_2 = table.metadata().current_snapshot_id().unwrap();
        let table = set_current(&catalog, &table, s0).await;
        // Advance main off S0 so staged_1 REPLAYS (its publish mints a snapshot stamped published-wap-id=R).
        let table = append(&catalog, &table, vec![data_file("test/head.parquet", 9)]).await;
        let head_before_publish = table.metadata().current_snapshot_id().unwrap();

        // Publish staged_1 → its published-wap-id=R now sits in the CURRENT ancestry.
        let table = cherry_pick(&catalog, &table, staged_1).await;
        let publish_snapshot = table.metadata().current_snapshot_id().unwrap();
        assert_eq!(
            current_summary_prop(&table, PUBLISHED_WAP_ID_PROP),
            Some("wap-R".to_string()),
            "the replay publish stamped published-wap-id=R into the current ancestry"
        );
        // Sanity: re-publishing staged_2 RIGHT NOW (R still in ancestry) WOULD be rejected — the hole only
        // opens after the rollback below.
        let blocked = cherry_pick_err(&catalog, &table, staged_2).await;
        assert!(
            blocked.message().contains(
                "Duplicate request to cherry pick wap id that was published already: wap-R"
            ),
            "pre-rollback, the same wap id is still deduped: {}",
            blocked.message()
        );

        // ROLL BACK past the publish: move main to the snapshot before staged_1's publish. The publishing
        // snapshot (carrying published-wap-id=R) is now OFF the current ancestry — but still in metadata.
        let table = set_current(&catalog, &table, head_before_publish).await;
        assert_ne!(
            table.metadata().current_snapshot_id(),
            Some(publish_snapshot),
            "main rolled back off the publishing snapshot"
        );
        assert!(
            table.metadata().snapshot_by_id(publish_snapshot).is_some(),
            "the publishing snapshot still EXISTS in metadata — it is just no longer a current ancestor"
        );

        // Now staged_2 (same wap id R) publishes SUCCESSFULLY — R left the current ancestry on rollback, so the
        // dedup walk no longer sees it. This is the documented Java-faithful escape hatch (current-ancestry walk).
        let table = cherry_pick(&catalog, &table, staged_2).await;
        assert_eq!(
            current_summary_prop(&table, PUBLISHED_WAP_ID_PROP),
            Some("wap-R".to_string()),
            "after rollback, the same wap id re-publishes (current-ancestry dedup, Java-faithful)"
        );
        let live = live_file_paths(&table).await;
        assert!(
            live.contains("test/w2.parquet"),
            "the re-published staged file is live after the rollback-and-redo: {live:?}"
        );
    }

    /// A data file routed to partition `(x, y)` under an explicit `spec_id` (the two-field identity spec the
    /// multi-spec test produces). Used only by the multi-spec replay pin.
    fn data_file_spec(path: &str, x: i64, y: i64, spec_id: i32) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(spec_id)
            .partition(Struct::from_iter([
                Some(Literal::long(x)),
                Some(Literal::long(y)),
            ]))
            .build()
            .unwrap()
    }
}
