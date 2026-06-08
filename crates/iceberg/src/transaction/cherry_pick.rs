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

//! This module contains the cherry-pick action (snapshot replay onto `main`).
//!
//! [`CherryPickAction`] replays a snapshot the table already knows about (typically one OFF the current
//! branch — a staged/forked snapshot) onto `main`, mirroring Java `CherryPickOperation.cherrypick`
//! (`core/.../CherryPickOperation.java` L69-141 + `validate` L162-171 + `isFastForward` L173-182). It is the
//! engine for `ManageSnapshots.cherrypick`. Unlike the metadata-only ref operations in
//! `transaction/manage_snapshots.rs`, cherry-pick REPLAYS DATA FILES, so it extends the snapshot-producing
//! write engine (Java `MergingSnapshotProducer` → the Rust [`SnapshotProducer`]).
//!
//! # The three modes (each citing the Java branch it mirrors)
//!
//! - **Unknown snapshot id → error.** `ValidationException.check(cherrypickSnapshot != null, "Cannot
//!   cherry-pick unknown snapshot ID: %s")` (L72-73).
//! - **validateNonAncestor** (L207-216). The source must NOT already be an ancestor of `main`'s current
//!   snapshot (re-picking an already-applied snapshot), and must NOT already be published as a
//!   `source-snapshot-id` of an ancestor — error either way (Java `CherrypickAncestorCommitException`).
//! - **Fast-forward** (`isFastForward`, L173-182): the source's parent IS `main`'s current snapshot (or both
//!   are null). NO new snapshot is created — `main` is simply moved to the source snapshot (Java returns
//!   `base.snapshot(cherrypickSnapshot.snapshotId())` from `apply()` and `updateEvent()` fires no
//!   CreateSnapshotEvent). The Rust action emits a single `SetSnapshotRef(main → source)` (the
//!   ManageSnapshots set-main pattern) with no producer.
//! - **APPEND replay** (source op == `Append`, not fast-forward; L78-91): a NEW snapshot is produced on
//!   `main` that re-adds the source snapshot's ADDED data files. The recorded operation is the source's
//!   operation (`Append`). The summary carries `source-snapshot-id` = the source id, and `published-wap-id`
//!   if the source carries a `wap.id` (Java `WapUtil.validateWapPublish`, L50-60).
//! - **Dynamic-OVERWRITE replay** (source op == `Overwrite` AND summary `replace-partitions` == "true";
//!   L93-129): a NEW `Overwrite` snapshot re-adds the source's ADDED data files AND re-deletes its REMOVED
//!   data files. The source's parent must be null OR an ancestor of `main`'s current snapshot (L101-105). Same
//!   `source-snapshot-id` + `published-wap-id` summary handling.
//! - **Other source operations → error** (L131-138): "not append, dynamic overwrite, or fast-forward".
//!
//! # Out of scope (deferred)
//!
//! - **`validateReplacedPartitions`** (Java L218-252): for a dynamic-overwrite cherry-pick, Java additionally
//!   walks the snapshots committed between the source's parent and the current head and rejects the pick if
//!   any of them added files to a partition the source replaces ("Cannot cherry-pick replace partitions with
//!   changed partition"). That is the serializable-isolation guard; this increment implements the replay +
//!   the parent-ancestor precondition but not the concurrent-change partition scan (it needs the same
//!   validation-history replay the Phase-2 conflict-validation sub-sequence builds). Tracked as a follow-up.
//! - **Full `WapUtil` edge cases:** the staged-vs-published distinction and `STAGED_WAP_ID_PROP` lifecycle
//!   beyond the publish-once check are not modeled — only the core "if the source has a `wap.id`, reject if
//!   already published, else publish it" path.
//! - **Data-level Java interop** (the round-trip that flips the row to ✅).

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{
    DataFile, MAIN_BRANCH, ManifestEntry, ManifestFile, Operation, SnapshotReference,
    SnapshotRetention, TableMetadata,
};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
    added_data_files_by_snapshot, removed_data_files_by_snapshot,
};
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

/// Summary property linking a published snapshot to the snapshot it cherry-picked
/// (Java `SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP`).
const SOURCE_SNAPSHOT_ID_PROP: &str = "source-snapshot-id";
/// Summary property recording the WAP id that this snapshot publishes
/// (Java `SnapshotSummary.PUBLISHED_WAP_ID_PROP`).
const PUBLISHED_WAP_ID_PROP: &str = "published-wap-id";
/// Summary property a staged WAP snapshot carries (Java `SnapshotSummary.STAGED_WAP_ID_PROP`).
const STAGED_WAP_ID_PROP: &str = "wap.id";
/// Summary property marking a dynamic partition overwrite (Java `SnapshotSummary.REPLACE_PARTITIONS_PROP`).
const REPLACE_PARTITIONS_PROP: &str = "replace-partitions";

/// A transaction action that cherry-picks (replays) a snapshot the table already knows about onto `main`.
///
/// Use [`crate::transaction::Transaction::cherry_pick`] to create one with the source snapshot id, then apply
/// and commit the transaction. At commit the source snapshot is resolved against the current table metadata
/// and replayed per the mode it falls into (fast-forward, append replay, or dynamic-overwrite replay) — see
/// the module docs.
pub struct CherryPickAction {
    /// The id of the snapshot to cherry-pick (the source). Resolved against the current table metadata at
    /// commit time.
    cherrypick_snapshot_id: i64,
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
}

impl CherryPickAction {
    pub(crate) fn new(cherrypick_snapshot_id: i64) -> Self {
        Self {
            cherrypick_snapshot_id,
            commit_uuid: None,
            key_metadata: None,
        }
    }

    /// Set the commit UUID for the produced snapshot (otherwise a fresh v7 UUID is generated). No effect in
    /// fast-forward mode, which produces no snapshot.
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set key metadata for the produced manifest files.
    pub fn set_key_metadata(mut self, key_metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(key_metadata);
        self
    }
}

/// Which replay mode the source snapshot falls into, decided at commit time after the
/// unknown-id / non-ancestor validations pass.
enum CherryPickMode {
    /// The source's parent is `main`'s current snapshot (or both null): move `main` to the source, no new
    /// snapshot (Java `isFastForward`).
    FastForward,
    /// The source is an `Append`: re-add its added data files in a new snapshot (Java APPEND branch).
    AppendReplay,
    /// The source is a dynamic overwrite (`replace-partitions`): re-add its added files + re-delete its
    /// removed files in a new `Overwrite` snapshot (Java OVERWRITE branch).
    DynamicOverwriteReplay,
}

#[async_trait]
impl TransactionAction for CherryPickAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let metadata = table.metadata();
        let source_id = self.cherrypick_snapshot_id;

        // Unknown snapshot id → error (Java `ValidationException.check(cherrypickSnapshot != null, ...)`).
        let source = metadata.snapshot_by_id(source_id).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Cannot cherry-pick unknown snapshot ID: {source_id}"),
            )
        })?;
        let source_parent_id = source.parent_snapshot_id();
        let source_operation = source.summary().operation.clone();
        let source_summary = source.summary().additional_properties.clone();

        let current_main_id = metadata.current_snapshot_id();

        // Fast-forward iff the source's parent IS the current head (or both are null) — Java `isFastForward`
        // (L173-182). Decided BEFORE validateNonAncestor: Java's `validate` skips the non-ancestor check in
        // the fast-forward case (`if (!isFastForward(base)) { validateNonAncestor(...) ... }`, L165).
        let is_fast_forward = match current_main_id {
            Some(current_id) => source_parent_id == Some(current_id),
            None => source_parent_id.is_none(),
        };

        if !is_fast_forward {
            // validateNonAncestor (Java L162-171 → L207-216): reject re-picking a snapshot already applied to
            // main, either directly (it is an ancestor) or via a prior cherry-pick (an ancestor's
            // `source-snapshot-id` == this id).
            validate_non_ancestor(metadata, source_id)?;
        }

        // Classify the replay mode (Java's APPEND / OVERWRITE+replace-partitions / fast-forward / else chain,
        // L78-138).
        let mode = if is_fast_forward {
            CherryPickMode::FastForward
        } else if source_operation == Operation::Append {
            CherryPickMode::AppendReplay
        } else if source_operation == Operation::Overwrite
            && source_summary
                .get(REPLACE_PARTITIONS_PROP)
                .map(|v| v == "true")
                .unwrap_or(false)
        {
            // Dynamic overwrite: the source's parent must be null OR an ancestor of the current state (Java
            // L101-105) — re-applying the deletes requires the base the overwrite was computed against to be
            // on the current branch.
            let parent_ok = match source_parent_id {
                None => true,
                Some(parent_id) => current_main_id
                    .map(|current_id| is_ancestor_of(metadata, parent_id, current_id))
                    .unwrap_or(false),
            };
            if !parent_ok {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot cherry-pick overwrite not based on an ancestor of the current state: \
                         {source_id}"
                    ),
                ));
            }
            CherryPickMode::DynamicOverwriteReplay
        } else {
            // Java L131-138: not append, dynamic overwrite, or a (valid) fast-forward.
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot cherry-pick snapshot {source_id}: not append, dynamic overwrite, or \
                     fast-forward"
                ),
            ));
        };

        match mode {
            CherryPickMode::FastForward => {
                // No producer, no new snapshot. Move `main` to the source snapshot, preserving main's
                // retention (Java publishes by setting the branch ref; the snapshot already exists). Emit the
                // same optimistic-concurrency guard the producer uses so a concurrent commit on main is caught.
                let reference = SnapshotReference::new(
                    source_id,
                    metadata
                        .refs
                        .get(MAIN_BRANCH)
                        .map(|r| r.retention.clone())
                        .unwrap_or_else(|| SnapshotRetention::branch(None, None, None)),
                );
                let updates = vec![TableUpdate::SetSnapshotRef {
                    ref_name: MAIN_BRANCH.to_string(),
                    reference,
                }];
                let requirements = vec![
                    TableRequirement::UuidMatch {
                        uuid: metadata.uuid(),
                    },
                    TableRequirement::RefSnapshotIdMatch {
                        r#ref: MAIN_BRANCH.to_string(),
                        snapshot_id: current_main_id,
                    },
                ];
                Ok(ActionCommit::new(updates, requirements))
            }
            CherryPickMode::AppendReplay | CherryPickMode::DynamicOverwriteReplay => {
                // Re-add the source snapshot's ADDED data files (Java `for (DataFile addedFile :
                // changes.addedDataFiles()) add(addedFile)`).
                let added_data_files = added_data_files_by_snapshot(table, source_id).await?;

                // For the dynamic-overwrite replay, also re-delete the source's REMOVED data files (Java
                // `for (DataFile deletedFile : changes.removedDataFiles()) delete(deletedFile)`). The
                // resolved removed files are matched by path against the current snapshot inside the producer.
                let removed_data_files = match mode {
                    CherryPickMode::DynamicOverwriteReplay => {
                        removed_data_files_by_snapshot(table, source_id).await?
                    }
                    _ => vec![],
                };

                // Snapshot summary props: link to the source snapshot (Java
                // `set(SOURCE_SNAPSHOT_ID_PROP, ...)`) and publish the WAP id if the source carries one
                // (Java `WapUtil.validateWapPublish` → `set(PUBLISHED_WAP_ID_PROP, wapId)`).
                let mut snapshot_properties = HashMap::new();
                snapshot_properties
                    .insert(SOURCE_SNAPSHOT_ID_PROP.to_string(), source_id.to_string());
                if let Some(wap_id) = validate_wap_publish(metadata, source_id)? {
                    snapshot_properties.insert(PUBLISHED_WAP_ID_PROP.to_string(), wap_id);
                }

                let snapshot_producer = SnapshotProducer::new(
                    table,
                    self.commit_uuid.unwrap_or_else(Uuid::now_v7),
                    self.key_metadata.clone(),
                    snapshot_properties,
                    added_data_files,
                );

                // The re-added files must be valid like a fast append (data content, partition-spec match,
                // partition-value compatibility) — Java re-adds them through `MergingSnapshotProducer.add`.
                snapshot_producer.validate_added_data_files()?;

                snapshot_producer
                    .commit(
                        CherryPickOperation {
                            operation: source_operation,
                            removed_data_files,
                        },
                        DefaultManifestProcess,
                    )
                    .await
            }
        }
    }
}

/// The [`SnapshotProduceOperation`] for a non-fast-forward cherry-pick replay.
///
/// Records the SOURCE snapshot's operation (`Append` for an append replay, `Overwrite` for a dynamic-overwrite
/// replay — Java `CherryPickOperation.operation()` returns `cherrypickSnapshot.operation()`). The added files
/// reach the producer separately (passed to `SnapshotProducer::new`, exactly as fast-append does); the removed
/// files (dynamic-overwrite only) are resolved against the current snapshot by the producer's by-path rewrite
/// machinery via the `delete_files` seam.
struct CherryPickOperation {
    operation: Operation,
    removed_data_files: Vec<DataFile>,
}

impl SnapshotProduceOperation for CherryPickOperation {
    fn operation(&self) -> Operation {
        self.operation.clone()
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        Ok(vec![])
    }

    async fn delete_files(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<DataFile>> {
        // The removed files were resolved from the SOURCE snapshot up front (its `Deleted` entries); hand
        // them to the producer, which rewrites the current snapshot's manifests to mark the matching live
        // entries deleted (Java `delete(deletedFile)`). Empty for an append replay.
        Ok(self.removed_data_files.clone())
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        // Expose every current data manifest so the producer can both carry them forward and (for the
        // dynamic-overwrite replay) rewrite the ones holding removed files — mirroring `OverwriteFiles`. For
        // an append replay there are no removed files, so every manifest is carried forward unchanged.
        snapshot_produce.current_data_manifests().await
    }
}

/// Reject cherry-picking a snapshot that is already applied to `main` (Java
/// `CherryPickOperation.validateNonAncestor`, L207-216).
///
/// Two ways a snapshot can already be applied: (a) it IS an ancestor of the current head (a direct re-pick),
/// or (b) a prior cherry-pick already published it — some ancestor carries `source-snapshot-id == source_id`.
fn validate_non_ancestor(metadata: &TableMetadata, source_id: i64) -> Result<()> {
    let Some(current_id) = metadata.current_snapshot_id() else {
        // No current snapshot ⇒ nothing has been applied yet; nothing to reject.
        return Ok(());
    };

    if is_ancestor_of(metadata, source_id, current_id) {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Cannot cherry-pick snapshot {source_id}: already an ancestor of the current state"
            ),
        ));
    }

    // Java `lookupAncestorBySourceSnapshot` (L268-279): some ancestor was itself a publish OF this source.
    let source_id_str = source_id.to_string();
    let mut current = Some(current_id);
    while let Some(id) = current {
        if let Some(snapshot) = metadata.snapshot_by_id(id) {
            if snapshot
                .summary()
                .additional_properties
                .get(SOURCE_SNAPSHOT_ID_PROP)
                == Some(&source_id_str)
            {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot cherry-pick snapshot {source_id}: already published to the current state \
                         by ancestor {id}"
                    ),
                ));
            }
            current = snapshot.parent_snapshot_id();
        } else {
            break;
        }
    }

    Ok(())
}

/// Check whether a staged WAP snapshot's `wap.id` was already published, and return the id to publish.
///
/// Mirrors Java `WapUtil.validateWapPublish` (L50-60): if the source snapshot carries a non-empty
/// `wap.id` (`STAGED_WAP_ID_PROP`), reject the cherry-pick when that id was already published in the current
/// ancestry (Java `DuplicateWAPCommitException` → here a non-retryable [`ErrorKind::DataInvalid`]); otherwise
/// return `Some(wap_id)` so the caller can set it as `published-wap-id`. Returns `None` for a non-WAP source
/// (no `wap.id`), which is the common case — cherry-pick does not require WAP.
fn validate_wap_publish(metadata: &TableMetadata, source_id: i64) -> Result<Option<String>> {
    let Some(source) = metadata.snapshot_by_id(source_id) else {
        return Ok(None);
    };
    let wap_id = match source
        .summary()
        .additional_properties
        .get(STAGED_WAP_ID_PROP)
    {
        Some(id) if !id.is_empty() => id.clone(),
        _ => return Ok(None),
    };

    // Java `isWapIdPublished`: walk the current ancestry and reject if any ancestor staged or published this
    // wap id.
    if let Some(current_id) = metadata.current_snapshot_id() {
        let mut current = Some(current_id);
        while let Some(id) = current {
            if let Some(snapshot) = metadata.snapshot_by_id(id) {
                let props = &snapshot.summary().additional_properties;
                if props.get(STAGED_WAP_ID_PROP) == Some(&wap_id)
                    || props.get(PUBLISHED_WAP_ID_PROP) == Some(&wap_id)
                {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Duplicate request to cherry pick wap id that was published already: {wap_id}"
                        ),
                    ));
                }
                current = snapshot.parent_snapshot_id();
            } else {
                break;
            }
        }
    }

    Ok(Some(wap_id))
}

/// Returns true if `ancestor_id` is `descendant_id` or any snapshot reachable from it by following
/// `parent_snapshot_id` (the parent-chain ancestry walk — Java `SnapshotUtil.isAncestorOf`).
fn is_ancestor_of(metadata: &TableMetadata, ancestor_id: i64, descendant_id: i64) -> bool {
    let mut current = Some(descendant_id);
    while let Some(id) = current {
        if id == ancestor_id {
            return true;
        }
        current = metadata
            .snapshot_by_id(id)
            .and_then(|s| s.parent_snapshot_id());
    }
    false
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, Operation, Struct,
    };
    use crate::table::Table;
    use crate::transaction::snapshot::{
        added_data_files_by_snapshot, removed_data_files_by_snapshot,
    };
    use crate::transaction::tests::make_v3_minimal_table_in_catalog;
    use crate::transaction::{ApplyTransactionAction, Transaction};
    use crate::{Catalog, ErrorKind};

    /// Build a data file routed to partition `x = part_value` (the V3 minimal table is identity(x), spec 0).
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

    /// Append the given files in a single fast-append commit and return the updated table.
    async fn append_files(catalog: &impl Catalog, table: &Table, files: Vec<DataFile>) -> Table {
        let tx = Transaction::new(table);
        let action = tx.fast_append().add_data_files(files);
        let tx = action.apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    /// Append a single file carrying a staged WAP id in the snapshot summary, returning the updated table.
    async fn append_wap(
        catalog: &impl Catalog,
        table: &Table,
        file: DataFile,
        wap_id: &str,
    ) -> Table {
        let mut props = HashMap::new();
        props.insert("wap.id".to_string(), wap_id.to_string());
        let tx = Transaction::new(table);
        let action = tx
            .fast_append()
            .set_snapshot_properties(props)
            .add_data_files(vec![file]);
        let tx = action.apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    /// Roll `main` back to `snapshot_id` (Java `setCurrentSnapshot`) so a later snapshot becomes off-branch.
    async fn set_main_to(catalog: &impl Catalog, table: &Table, snapshot_id: i64) -> Table {
        let tx = Transaction::new(table);
        let action = tx.manage_snapshots().set_current_snapshot(snapshot_id);
        let tx = action.apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    /// Collect the set of live (Added or Existing) data-file paths across the table's CURRENT snapshot — the
    /// real correctness signal (what a scan would read). THIS is the crown-jewel assertion: the picked data
    /// must actually be reachable from `main`, not merely emitted as an update.
    async fn live_file_paths(table: &Table) -> HashSet<String> {
        let snapshot = table
            .metadata()
            .current_snapshot()
            .expect("table should have a current snapshot");
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .expect("manifest list should load");

        let mut live = HashSet::new();
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file
                .load_manifest(table.file_io())
                .await
                .expect("manifest should load");
            for entry in manifest.entries() {
                if entry.is_alive() {
                    live.insert(entry.file_path().to_string());
                }
            }
        }
        live
    }

    fn current_id(table: &Table) -> i64 {
        table.metadata().current_snapshot().unwrap().snapshot_id()
    }

    fn summary_prop(table: &Table, key: &str) -> Option<String> {
        table
            .metadata()
            .current_snapshot()
            .unwrap()
            .summary()
            .additional_properties
            .get(key)
            .cloned()
    }

    /// THE CROWN-JEWEL TEST (non-fast-forward APPEND replay): the picked data is actually PUBLISHED to `main`.
    ///
    /// Stage a snapshot whose data is NOT on `main`: append A→S1, X→S2, C→S3 (main at S3); roll `main` back
    /// to S1 so S3 is off-branch (its parent S2 ≠ current S1 → NOT a fast-forward). `cherry_pick(S3)` must
    /// produce a NEW snapshot on `main` carrying S3's ADDED file (C) — so the post-commit SCAN of `main` is
    /// exactly {A, C} (NOT X — S3 only added C; NOT a fast-forward — S3 itself is never made current).
    ///
    /// Risk pinned: the picked data is not actually reachable from `main` (the #1 cherry-pick failure — the
    /// commit "succeeds" but a scan can't see the rows), or the wrong snapshot's files are replayed.
    #[tokio::test]
    async fn test_cherry_pick_append_publishes_picked_data_to_main_scan() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;
        let s1 = current_id(&table);
        let table = append_files(&catalog, &table, vec![data_file("test/x.parquet", 1)]).await;
        let table = append_files(&catalog, &table, vec![data_file("test/c.parquet", 2)]).await;
        let s3 = current_id(&table);

        // Roll main back to S1 → S3 is off-branch (parent S2 ≠ current S1).
        let table = set_main_to(&catalog, &table, s1).await;
        assert_eq!(current_id(&table), s1);
        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/a.parquet".to_string()]),
            "after rollback only A is on main"
        );

        // Cherry-pick S3 onto main.
        let tx = Transaction::new(&table);
        let action = tx.cherry_pick(s3);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let new_id = current_id(&table);
        assert_ne!(
            new_id, s3,
            "a non-fast-forward pick creates a NEW snapshot, not S3 itself"
        );
        assert_ne!(new_id, s1, "main advanced past S1");
        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/a.parquet".to_string(), "test/c.parquet".to_string()]),
            "the picked file C is now on main alongside A; X (only on S2) is NOT picked"
        );
        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Append,
            "the replay records the SOURCE snapshot's operation (Append)"
        );
        assert_eq!(
            summary_prop(&table, "source-snapshot-id"),
            Some(s3.to_string()),
            "the published snapshot links to the picked source via source-snapshot-id"
        );
        assert_eq!(
            summary_prop(&table, "published-wap-id"),
            None,
            "a non-WAP source publishes no wap id"
        );
    }

    /// REPLAY-EXACTNESS PROBE (the load-bearing helper, in isolation): `added_data_files_by_snapshot`
    /// returns EXACTLY the files the SOURCE snapshot ADDED — never a carried-forward ancestor's files.
    ///
    /// Fast-append A→S1, B→S2, C→S3. Each fast append carries forward the prior snapshots' manifests into
    /// its own manifest list (so S3's manifest list references S1's + S2's manifests as `Existing`). The
    /// helper must still return only `{C}` for S3 (its own `Added` entries from the manifest it created),
    /// `{B}` for S2, `{A}` for S1 — proving the `added_snapshot_id == source_id` + `Added`-status filters
    /// exclude the carried-forward entries. A bug here publishes the WRONG data on a cherry-pick (the #2
    /// risk: more-or-fewer files than the source added).
    #[tokio::test]
    async fn test_added_data_files_by_snapshot_returns_only_that_snapshots_added_files() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;
        let s1 = current_id(&table);
        let table = append_files(&catalog, &table, vec![data_file("test/b.parquet", 1)]).await;
        let s2 = current_id(&table);
        let table = append_files(&catalog, &table, vec![data_file("test/c.parquet", 2)]).await;
        let s3 = current_id(&table);

        let paths = |files: Vec<DataFile>| -> HashSet<String> {
            files
                .into_iter()
                .map(|f| f.file_path().to_string())
                .collect()
        };

        assert_eq!(
            paths(added_data_files_by_snapshot(&table, s3).await.unwrap()),
            HashSet::from(["test/c.parquet".to_string()]),
            "S3 added ONLY C — A (S1) and B (S2) are carried forward, not re-reported"
        );
        assert_eq!(
            paths(added_data_files_by_snapshot(&table, s2).await.unwrap()),
            HashSet::from(["test/b.parquet".to_string()]),
            "S2 added ONLY B"
        );
        assert_eq!(
            paths(added_data_files_by_snapshot(&table, s1).await.unwrap()),
            HashSet::from(["test/a.parquet".to_string()]),
            "S1 added ONLY A"
        );

        // A pure-append snapshot removed NOTHING — the removed helper is empty for each.
        for id in [s1, s2, s3] {
            assert!(
                removed_data_files_by_snapshot(&table, id)
                    .await
                    .unwrap()
                    .is_empty(),
                "append snapshot {id} removed no data files"
            );
        }
    }

    /// WAP publish path: cherry-picking a staged snapshot (carrying `wap.id`) sets `published-wap-id`.
    ///
    /// Stage A→S1, X→S2, then a WAP append B(`wap.id=wap-123`)→S3 (off-branch after rollback to S1).
    /// `cherry_pick(S3)` publishes B to main AND records `published-wap-id = wap-123` (Java
    /// `WapUtil.validateWapPublish` → `set(PUBLISHED_WAP_ID_PROP, wapId)`).
    ///
    /// Risk pinned: the WAP id is dropped on publish (so a duplicate-publish guard can never fire), or the
    /// staged data is not actually published.
    #[tokio::test]
    async fn test_cherry_pick_wap_source_sets_published_wap_id() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;
        let s1 = current_id(&table);
        let table = append_files(&catalog, &table, vec![data_file("test/x.parquet", 1)]).await;
        let table = append_wap(&catalog, &table, data_file("test/b.parquet", 2), "wap-123").await;
        let s3 = current_id(&table);

        let table = set_main_to(&catalog, &table, s1).await;

        let tx = Transaction::new(&table);
        let action = tx.cherry_pick(s3);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/a.parquet".to_string(), "test/b.parquet".to_string()]),
            "the staged WAP data B is published to main"
        );
        assert_eq!(
            summary_prop(&table, "published-wap-id"),
            Some("wap-123".to_string()),
            "the published snapshot records the source's wap.id as published-wap-id"
        );
        assert_eq!(
            summary_prop(&table, "source-snapshot-id"),
            Some(s3.to_string())
        );
    }

    /// Re-picking the SAME WAP id (already published) is rejected (Java `DuplicateWAPCommitException`).
    ///
    /// Publish wap-dup once (S1→S2 staged WAP→pick), then stage ANOTHER snapshot with the SAME wap id and try
    /// to pick it → error. Risk pinned: a WAP id published twice (double-applied data).
    #[tokio::test]
    async fn test_cherry_pick_rejects_already_published_wap_id() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;
        let s1 = current_id(&table);
        let table = append_files(&catalog, &table, vec![data_file("test/x.parquet", 1)]).await;
        let table = append_wap(&catalog, &table, data_file("test/b.parquet", 2), "wap-dup").await;
        let s3 = current_id(&table);
        let table = set_main_to(&catalog, &table, s1).await;

        // First pick publishes wap-dup.
        let tx = Transaction::new(&table);
        let table = tx
            .cherry_pick(s3)
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();
        assert_eq!(
            summary_prop(&table, "published-wap-id"),
            Some("wap-dup".to_string())
        );
        let published_main = current_id(&table);

        // Stage a SECOND snapshot reusing the same wap id, OFF-branch and NON-fast-forward (its parent is an
        // intermediate, not `published_main`), so the pick takes the append-replay path that runs the WAP
        // publish check.
        let table = append_files(&catalog, &table, vec![data_file("test/i.parquet", 2)]).await;
        let table = append_wap(&catalog, &table, data_file("test/d.parquet", 1), "wap-dup").await;
        let s_dup = current_id(&table);
        let table = set_main_to(&catalog, &table, published_main).await;

        // Picking it must be rejected: wap-dup is already published in main's ancestry.
        let tx = Transaction::new(&table);
        let result = tx
            .cherry_pick(s_dup)
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await;
        let Err(err) = result else {
            panic!("re-publishing a wap id must fail");
        };
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message().contains("wap"),
            "error should mention the duplicate wap publish: {}",
            err.message()
        );
    }

    /// FAST-FORWARD mode: the source's parent IS the current head → `main` MOVES to the source with NO new
    /// snapshot.
    ///
    /// Append A→S1, B→S2 (main at S2); roll main back to S1; `cherry_pick(S2)` → S2.parent == S1 == current
    /// → fast-forward: `main` becomes S2 itself (no NEW snapshot id), live set {A, B}, and there is NO
    /// `source-snapshot-id` (no new snapshot was produced).
    ///
    /// Risk pinned: a fast-forward that wrongly forges a NEW snapshot (duplicating S2's data into a fresh
    /// snapshot) instead of just moving the ref.
    #[tokio::test]
    async fn test_cherry_pick_fast_forward_moves_main_without_new_snapshot() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;
        let s1 = current_id(&table);
        let table = append_files(&catalog, &table, vec![data_file("test/b.parquet", 1)]).await;
        let s2 = current_id(&table);

        let table = set_main_to(&catalog, &table, s1).await;
        assert_eq!(current_id(&table), s1);

        let tx = Transaction::new(&table);
        let action = tx.cherry_pick(s2);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(
            current_id(&table),
            s2,
            "fast-forward sets main to the SOURCE snapshot itself (no new snapshot)"
        );
        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/a.parquet".to_string(), "test/b.parquet".to_string()]),
            "main now sees both A and B (it fast-forwarded to S2)"
        );
        assert_eq!(
            summary_prop(&table, "source-snapshot-id"),
            None,
            "no new snapshot was created, so no source-snapshot-id is set (S2 keeps its own append summary)"
        );
    }

    /// DYNAMIC-OVERWRITE replay: a `replace-partitions` source is replayed as a new `Overwrite` snapshot that
    /// re-adds the source's added files AND re-deletes its removed files.
    ///
    /// Build: append A@x=0 → S1; `replace_partitions` A2@x=0 → S2 (Overwrite, parent S1, replaces x=0,
    /// removing A); roll main back to S1; append D@x=2 → Q (a child of S1, parent S1, so S2 is now
    /// off-branch and S2.parent S1 ≠ current Q → not fast-forward, parent-is-ancestor holds).
    /// `cherry_pick(S2)` re-adds A2 and re-deletes A → main scan = {A2@x=0, D@x=2}, op == Overwrite.
    ///
    /// Risk pinned: the replayed overwrite drops the re-delete (A survives → duplicated/stale partition) or
    /// drops the re-add (A2 missing), or records the wrong operation.
    #[tokio::test]
    async fn test_cherry_pick_dynamic_overwrite_replays_adds_and_deletes() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // S1: A @ x=0.
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;
        let s1 = current_id(&table);

        // S2: replace partition x=0 with A2 (Overwrite, replace-partitions=true; removes A, adds A2).
        let tx = Transaction::new(&table);
        let action = tx
            .replace_partitions()
            .add_file(data_file("test/a2.parquet", 0));
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let s2 = current_id(&table);
        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Overwrite
        );

        // Isolate the replay helpers for the overwrite source: S2 ADDED exactly {A2} and REMOVED exactly
        // {A}. This pins `removed_data_files_by_snapshot` (the dynamic-overwrite re-delete input) directly —
        // a wrong removed set would re-delete the wrong file (or nothing), corrupting the replayed partition.
        let probe_paths = |files: Vec<DataFile>| -> HashSet<String> {
            files
                .into_iter()
                .map(|f| f.file_path().to_string())
                .collect()
        };
        assert_eq!(
            probe_paths(added_data_files_by_snapshot(&table, s2).await.unwrap()),
            HashSet::from(["test/a2.parquet".to_string()]),
            "S2 (replace-partitions) added exactly A2"
        );
        assert_eq!(
            probe_paths(removed_data_files_by_snapshot(&table, s2).await.unwrap()),
            HashSet::from(["test/a.parquet".to_string()]),
            "S2 (replace-partitions) removed exactly A"
        );

        // Roll main back to S1, then append D @ x=2 → Q (child of S1). S2 is now off-branch; S2.parent = S1,
        // current = Q ≠ S2 → not fast-forward; S1 is an ancestor of Q → overwrite-parent precondition holds.
        let table = set_main_to(&catalog, &table, s1).await;
        let table = append_files(&catalog, &table, vec![data_file("test/d.parquet", 2)]).await;
        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/a.parquet".to_string(), "test/d.parquet".to_string()]),
            "before the pick main holds A (x=0) and D (x=2)"
        );

        // Cherry-pick the overwrite S2: re-add A2, re-delete A.
        let tx = Transaction::new(&table);
        let action = tx.cherry_pick(s2);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/a2.parquet".to_string(), "test/d.parquet".to_string()]),
            "the overwrite replay removes A and adds A2; D (untouched partition) survives"
        );
        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Overwrite,
            "the replay records the source's Overwrite operation"
        );
        assert_eq!(
            summary_prop(&table, "source-snapshot-id"),
            Some(s2.to_string())
        );
    }

    /// Cherry-picking a snapshot already applied to `main` (an ANCESTOR of the current head) is rejected
    /// (Java `validateNonAncestor` → `CherrypickAncestorCommitException`).
    ///
    /// Append A→S1, B→S2 (main at S2, S1 is an ancestor). `cherry_pick(S1)` → error (re-applying already-
    /// applied data). Risk pinned: silently re-picking already-committed data (double-apply / corruption).
    #[tokio::test]
    async fn test_cherry_pick_already_ancestor_is_rejected() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;
        let s1 = current_id(&table);
        let table = append_files(&catalog, &table, vec![data_file("test/b.parquet", 1)]).await;

        let tx = Transaction::new(&table);
        let result = tx.cherry_pick(s1).apply(tx).unwrap().commit(&catalog).await;
        let Err(err) = result else {
            panic!("picking an ancestor must fail");
        };
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message().contains("ancestor"),
            "error should explain the snapshot is already an ancestor: {}",
            err.message()
        );
    }

    /// Cherry-picking a source that an ANCESTOR already PUBLISHED (carries `source-snapshot-id == this id`)
    /// is rejected — the second leg of `validateNonAncestor` (Java `lookupAncestorBySourceSnapshot`,
    /// L268-279 → `CherrypickAncestorCommitException(snapshotId, ancestorId)`).
    ///
    /// This is distinct from the direct-ancestor leg: the source snapshot S3 is NEVER itself made an
    /// ancestor of `main` (a non-fast-forward pick produces a NEW snapshot P, P != S3), so the
    /// `is_ancestor_of(S3, current)` check stays false. The guard must instead notice that P (now an
    /// ancestor of `main`) was a publish OF S3 (`source-snapshot-id == S3`). Without this leg the same
    /// off-branch source could be replayed twice, DOUBLE-APPLYING its data.
    ///
    /// Build: append A→S1, X→S2, C→S3; roll main to S1 (S3 off-branch, non-FF); cherry_pick(S3) → P
    /// (P.source-snapshot-id == S3, P now main's head). cherry_pick(S3) AGAIN → error via the
    /// source-snapshot-id ancestry lookup.
    #[tokio::test]
    async fn test_cherry_pick_already_published_via_source_snapshot_id_is_rejected() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;
        let s1 = current_id(&table);
        let table = append_files(&catalog, &table, vec![data_file("test/x.parquet", 1)]).await;
        let table = append_files(&catalog, &table, vec![data_file("test/c.parquet", 2)]).await;
        let s3 = current_id(&table);

        // Roll main back to S1 so S3 is off-branch and the pick is non-fast-forward (S3.parent = S2 ≠ S1).
        let table = set_main_to(&catalog, &table, s1).await;

        // First pick publishes S3 → P (P.source-snapshot-id == S3). P is NOT S3 itself.
        let tx = Transaction::new(&table);
        let table = tx
            .cherry_pick(s3)
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();
        let published = current_id(&table);
        assert_ne!(
            published, s3,
            "a non-fast-forward pick creates a NEW snapshot"
        );
        assert_eq!(
            summary_prop(&table, "source-snapshot-id"),
            Some(s3.to_string())
        );

        // Re-picking S3 must be rejected: an ancestor (P) already published it. S3 itself is still NOT an
        // ancestor of main, so ONLY the source-snapshot-id lookup leg can catch this.
        let tx = Transaction::new(&table);
        let result = tx.cherry_pick(s3).apply(tx).unwrap().commit(&catalog).await;
        let Err(err) = result else {
            panic!("re-picking an already-published source must fail");
        };
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message().contains("already published"),
            "error should explain the source was already published by an ancestor: {}",
            err.message()
        );
    }

    /// Cherry-picking an unknown snapshot id is rejected (Java "Cannot cherry-pick unknown snapshot ID").
    ///
    /// Risk pinned: a bogus id silently producing an empty/garbage snapshot instead of a clear error.
    #[tokio::test]
    async fn test_cherry_pick_unknown_snapshot_id_is_rejected() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;

        let tx = Transaction::new(&table);
        let result = tx
            .cherry_pick(999_999_999)
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await;
        let Err(err) = result else {
            panic!("picking an unknown id must fail");
        };
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message().contains("unknown snapshot ID"),
            "error should name the unknown snapshot id: {}",
            err.message()
        );
    }

    /// Cherry-picking a snapshot whose operation is neither append, dynamic overwrite, nor a fast-forward is
    /// rejected (Java "not append, dynamic overwrite, or fast-forward").
    ///
    /// Build a non-dynamic Overwrite via `overwrite_files` (it records Overwrite with NO `replace-partitions`
    /// marker), keep it off-branch as a non-fast-forward source, and pick it → error.
    #[tokio::test]
    async fn test_cherry_pick_unsupported_operation_is_rejected() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // S1: A @ x=0, B @ x=1.
        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 1),
        ])
        .await;
        let s1 = current_id(&table);

        // S2: an explicit overwrite (delete A, add C) — Operation::Overwrite, but NOT replace-partitions.
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .delete_files(vec!["test/a.parquet"])
            .add_files(vec![data_file("test/c.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let s2 = current_id(&table);
        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Overwrite
        );
        assert!(
            summary_prop(&table, "replace-partitions").is_none(),
            "an explicit overwrite carries no replace-partitions marker"
        );

        // Advance main to a sibling of S2: roll back to S1, append D → Q (so S2 is off-branch, non-FF).
        let table = set_main_to(&catalog, &table, s1).await;
        let table = append_files(&catalog, &table, vec![data_file("test/d.parquet", 2)]).await;

        let tx = Transaction::new(&table);
        let result = tx.cherry_pick(s2).apply(tx).unwrap().commit(&catalog).await;
        let Err(err) = result else {
            panic!("an unsupported-op source must fail");
        };
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message()
                .contains("not append, dynamic overwrite, or fast-forward"),
            "error should explain the unsupported operation: {}",
            err.message()
        );
    }
}
