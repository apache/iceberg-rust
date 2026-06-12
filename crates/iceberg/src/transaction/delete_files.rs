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

//! This module contains the delete-files action.
//!
//! [`DeleteFilesAction`] removes data files from a table by file path (or [`DataFile`] reference),
//! producing a new snapshot whose manifests no longer reference the removed files. It mirrors Java
//! `StreamingDelete` / `DeleteFiles` and reuses the manifest-filter / rewrite machinery in
//! [`SnapshotProducer`]: at commit time the requested paths are resolved against the current
//! snapshot's manifests, each manifest containing a matching live entry is rewritten (matching
//! entries → `Deleted`, the rest copied forward as `Existing`), and the new snapshot is committed.
//!
//! Deleting a path that is not present in any live entry of the table is an error (mirrors Java
//! `failMissingDeletePaths`).
//!
//! **Out of scope (deferred):** delete-by-row-filter / partition-predicate (Java
//! `DeleteFiles.deleteFromRowFilter` / `dropPartition`) — these need inclusive/strict metrics
//! evaluation and will land with the `OverwriteFiles` / `ReplacePartitions` increments.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{DataFile, ManifestEntry, ManifestFile, Operation};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer, deleted_data_files_after,
};
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind};

/// A transaction action that deletes data files from a table by file path.
///
/// Use [`crate::transaction::Transaction::delete_files`] to create one. Accumulate the files to
/// remove with [`DeleteFilesAction::delete_file`] / [`DeleteFilesAction::delete_files`] /
/// [`DeleteFilesAction::delete_data_files`], then apply and commit the transaction. Committing
/// produces a new `Delete` snapshot whose live file set excludes the removed files.
pub struct DeleteFilesAction {
    /// Fully-qualified file paths to remove from the table.
    delete_paths: HashSet<String>,
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    /// Whether to reject the commit if a data file this action is deleting was already DELETED by a
    /// concurrent commit since the starting snapshot (Java `StreamingDelete.validateFilesExist` /
    /// `MergingSnapshotProducer.validateDataFilesExist`). OFF by default = snapshot isolation (no check).
    validate_files_exist: bool,
    /// An explicit starting snapshot for the files-exist check (Java `validateFromSnapshot`). When `None`,
    /// the check uses the transaction's starting snapshot (the table head when the transaction was created).
    validate_from_snapshot: Option<i64>,
    /// Stage the produced delete snapshot for write-audit-publish instead of moving `main` (Java
    /// `SnapshotProducer.stageOnly()`). See [`DeleteFilesAction::stage_only`].
    stage_only: bool,
}

impl DeleteFilesAction {
    pub(crate) fn new() -> Self {
        Self {
            delete_paths: HashSet::default(),
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
            validate_files_exist: false,
            validate_from_snapshot: None,
            stage_only: false,
        }
    }

    /// Delete a single file by its fully-qualified path.
    ///
    /// To remove a file from the table, this path must equal a path in the table's metadata. Paths
    /// that are different but equivalent (e.g. `file:/p/f.parquet` vs `file:///p/f.parquet`) will not
    /// be removed (mirrors Java `DeleteFiles.deleteFile(CharSequence)`).
    pub fn delete_file(mut self, path: impl Into<String>) -> Self {
        self.delete_paths.insert(path.into());
        self
    }

    /// Delete multiple files by their fully-qualified paths.
    pub fn delete_files(mut self, paths: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.delete_paths.extend(paths.into_iter().map(Into::into));
        self
    }

    /// Delete multiple files referenced by [`DataFile`]s (their paths are used).
    pub fn delete_data_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.delete_paths
            .extend(files.into_iter().map(|file| file.file_path));
        self
    }

    /// Set the commit UUID for the snapshot (otherwise a fresh v7 UUID is generated).
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set key metadata for manifest files.
    pub fn set_key_metadata(mut self, key_metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(key_metadata);
        self
    }

    /// Set snapshot summary properties.
    pub fn set_snapshot_properties(mut self, snapshot_properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }

    /// ENABLE the files-exist conflict check (Java `StreamingDelete.validateFilesExist` →
    /// `MergingSnapshotProducer.validateDataFilesExist`): the commit is rejected with a non-retryable
    /// `ValidationException` if any data file this action is deleting was ALREADY DELETED by a snapshot
    /// committed since the starting snapshot. Without it, a concurrent removal of the same file is
    /// silently absorbed (the path simply no longer resolves to a live entry on the re-based commit).
    ///
    /// Default (this method NOT called) = snapshot isolation = no check (current behavior unchanged).
    pub fn validate_files_exist(mut self) -> Self {
        self.validate_files_exist = true;
        self
    }

    /// Override the snapshot from which the files-exist check starts (Java
    /// `DeleteFiles.validateFromSnapshot(long)`). By default the check uses the transaction's starting
    /// snapshot (the table head when [`crate::transaction::Transaction::new`] was called); this lets the
    /// caller pin a specific earlier snapshot id (the snapshot it read when selecting the files to delete).
    ///
    /// On its own this does NOT enable the check — call [`Self::validate_files_exist`] for that.
    pub fn validate_from_snapshot(mut self, snapshot_id: i64) -> Self {
        self.validate_from_snapshot = Some(snapshot_id);
        self
    }

    /// STAGE this delete for write-audit-publish (WAP) instead of publishing it to `main` (Java
    /// `SnapshotProducer.stageOnly()`). When called, committing this action ADDS the new `Delete` snapshot
    /// (with its rewritten/tombstoned manifests) to table metadata but moves NO ref: `current-snapshot-id`,
    /// the `main` ref, and the snapshot-log are left UNCHANGED, so readers continue to see the pre-staging
    /// data — the deleted rows stay visible — until a later
    /// [`crate::transaction::Transaction::cherry_pick`] publishes the staged snapshot. The staged snapshot
    /// still consumes a sequence number exactly like a normal commit. Mirrors `FastAppendAction::stage_only`
    /// on a delete-bearing action (Java's `stageOnly()` is on the base producer, so it stages identically).
    pub fn stage_only(mut self) -> Self {
        self.stage_only = true;
        self
    }
}

#[async_trait]
impl TransactionAction for DeleteFilesAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            // A delete-only commit adds no data files.
            vec![],
        )
        .with_stage_only(self.stage_only);

        snapshot_producer
            .commit(
                DeleteFilesOperation {
                    delete_paths: self.delete_paths.clone(),
                },
                DefaultManifestProcess,
            )
            .await
    }

    /// Files-exist conflict validation (Java `StreamingDelete.validate` → `failMissingDeletePaths` /
    /// `MergingSnapshotProducer.validateDataFilesExist`). Only runs when [`Self::validate_files_exist`] was
    /// enabled; otherwise a no-op (snapshot isolation).
    ///
    /// When enabled: compute the effective starting snapshot ([`Self::validate_from_snapshot`] if set, else
    /// the transaction-provided `starting_snapshot_id`), enumerate every DATA file DELETED from the refreshed
    /// base by snapshots committed since it (the shared [`deleted_data_files_after`] helper = Java
    /// `deletedDataFiles` over `VALIDATE_DATA_FILES_EXIST_OPERATIONS` + `ManifestStatus::Deleted`), and reject
    /// the commit if ANY of those removed files is a file this action also needs to delete (its path ∈
    /// `self.delete_paths`, Java `requiredDataFiles.contains(entry.file().location())`). The rejection is a
    /// NON-retryable [`ErrorKind::DataInvalid`] (Java's non-retryable `ValidationException`), naming the
    /// missing file so the retry loop stops and the validation message propagates.
    ///
    /// `requiredDataFiles` here is `self.delete_paths` — the set of files the delete operation requires (the
    /// ones it is removing) — mirroring Java `StreamingDelete`, whose required-deletes are the paths/files
    /// passed to `deleteFile`.
    async fn validate(
        self: Arc<Self>,
        starting_snapshot_id: Option<i64>,
        current: &Table,
    ) -> Result<()> {
        if !self.validate_files_exist {
            // Default: snapshot isolation, no files-exist check (current behavior unchanged).
            return Ok(());
        }

        // Nothing requested to delete ⇒ nothing can be missing — skip the manifest walk.
        if self.delete_paths.is_empty() {
            return Ok(());
        }

        // Java `validateDataFilesExist` uses `startingSnapshotId` (the `validateFromSnapshot` override) when
        // set, else the operation's starting snapshot.
        let effective_start = self.validate_from_snapshot.or(starting_snapshot_id);

        // `skip_deletes == false`: a `DeleteFiles` commit validates against ALL data-removing operations
        // INCLUDING concurrent DELETE-op snapshots (Java `validateDataFilesExist` is called with
        // `skipDeletes = false` here — the files this op needs to delete must still exist regardless of which
        // operation removed them). `RowDelta` is the variant that skips DELETE-op snapshots by default.
        let deleted = deleted_data_files_after(current, effective_start, false).await?;

        // Reject on the FIRST concurrently-deleted file this action also requires (Java throws on the first
        // matching entry, naming the missing data files).
        if let Some(missing) = deleted
            .iter()
            .find(|file| self.delete_paths.contains(file.file_path()))
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Cannot commit, missing data files: {}", missing.file_path()),
            ));
        }

        Ok(())
    }
}

/// The [`SnapshotProduceOperation`] for [`DeleteFilesAction`].
///
/// Records `Operation::Delete`, exposes every current data manifest as the set to filter, and
/// resolves the requested paths against the current snapshot's live data entries (the resolved
/// [`DataFile`]s drive the producer's manifest rewrite).
struct DeleteFilesOperation {
    delete_paths: HashSet<String>,
}

impl SnapshotProduceOperation for DeleteFilesOperation {
    fn operation(&self) -> Operation {
        Operation::Delete
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        Ok(vec![])
    }

    async fn delete_files(&self, snapshot_produce: &SnapshotProducer<'_>) -> Result<Vec<DataFile>> {
        // Resolve the requested paths against the current snapshot's live data entries, validating that
        // EVERY requested path matched a live entry (Java `failMissingDeletePaths`). Shared with
        // `OverwriteFiles` via `SnapshotProducer::resolve_delete_paths`.
        snapshot_produce
            .resolve_delete_paths(&self.delete_paths)
            .await
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        // Expose EVERY current manifest — DATA and DELETE — via the shared
        // [`SnapshotProducer::current_manifests`]. The producer's `process_deletes` decides per DATA manifest
        // whether to rewrite, carry forward unchanged, or drop it; every DELETE manifest carries forward
        // UNCHANGED (its entries are delete-file paths, never in the data-file `delete_paths`), so a delete
        // on a merge-on-read table preserves all outstanding position / equality deletes instead of silently
        // dropping them and resurrecting deleted rows. The conservative dangling-delete posture (no pruning)
        // is documented on the helper.
        snapshot_produce.current_manifests().await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array, RecordBatch};
    use futures::TryStreamExt;

    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, ManifestContentType,
        ManifestStatus, Operation, Struct,
    };
    use crate::table::Table;
    use crate::transaction::tests::{
        make_v2_minimal_table_in_catalog, make_v3_minimal_table_in_catalog,
    };
    use crate::transaction::{ApplyTransactionAction, Transaction};
    use crate::writer::base_writer::position_delete_writer::{
        PositionDeleteFileWriterBuilder, PositionDeleteWriterConfig,
    };
    use crate::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use crate::writer::file_writer::{FileWriter, FileWriterBuilder, ParquetWriterBuilder};
    use crate::writer::{IcebergWriter, IcebergWriterBuilder};
    use crate::{Catalog, ErrorKind};

    /// Build a data file routed to partition `x = part_value` (the V3 minimal table is partitioned by
    /// identity(x), spec id 0) with a unique path.
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

    /// Collect the set of live (Added or Existing) data file paths across the table's current
    /// snapshot — the real correctness signal (what a scan would read).
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

    /// Append the given files in a single fast-append commit and return the updated table.
    async fn append_files(catalog: &impl Catalog, table: &Table, files: Vec<DataFile>) -> Table {
        let tx = Transaction::new(table);
        let action = tx.fast_append().add_data_files(files);
        let tx = action.apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    /// Commit a CONCURRENT `delete_files` that removes the given paths in its own `Delete` snapshot, via the
    /// catalog — the removal that a files-exist check must detect. The resulting snapshot records
    /// `Operation::Delete` (in `VALIDATE_DATA_FILES_EXIST_OPERATIONS`) and rewrites the affected manifest,
    /// stamping a `Deleted` tombstone for each removed path under its own `added_snapshot_id`.
    async fn commit_concurrent_delete(
        catalog: &impl Catalog,
        table: &Table,
        paths: impl IntoIterator<Item = &str>,
    ) -> Table {
        let tx = Transaction::new(table);
        let action = tx
            .delete_files()
            .delete_files(paths.into_iter().map(str::to_string));
        let tx = action.apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    /// Pins: deleting one of several files in the SAME manifest removes exactly that file from the
    /// live scan set and leaves the others — the core "wrong live set / data loss" risk.
    #[tokio::test]
    async fn test_delete_files_removes_only_targeted_file_from_live_scan() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Fast-append A, B, C in one commit (one manifest containing all three).
        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 0),
            data_file("test/c.parquet", 0),
        ])
        .await;
        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from([
                "test/a.parquet".to_string(),
                "test/b.parquet".to_string(),
                "test/c.parquet".to_string(),
            ])
        );

        // Delete B.
        let tx = Transaction::new(&table);
        let action = tx.delete_files().delete_file("test/b.parquet");
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // The new snapshot is a delete, and the live set is exactly {A, C}.
        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Delete
        );
        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/a.parquet".to_string(), "test/c.parquet".to_string()])
        );
    }

    /// Pins: the deleted file's entry is present as DELETED (informational) in the rewritten manifest
    /// and the manifest counts (existing vs deleted) are correct — guards against manifest corruption.
    #[tokio::test]
    async fn test_delete_files_marks_entry_deleted_and_counts_are_correct() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 0),
        ])
        .await;

        let tx = Transaction::new(&table);
        let action = tx.delete_files().delete_file("test/b.parquet");
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        // One rewritten manifest, with one existing (A) and one deleted (B) entry.
        assert_eq!(manifest_list.entries().len(), 1);
        let manifest_file = &manifest_list.entries()[0];
        assert_eq!(manifest_file.existing_files_count, Some(1));
        assert_eq!(manifest_file.deleted_files_count, Some(1));
        assert_eq!(manifest_file.added_files_count, Some(0));

        let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
        let mut statuses: Vec<(String, ManifestStatus)> = manifest
            .entries()
            .iter()
            .map(|entry| (entry.file_path().to_string(), entry.status()))
            .collect();
        statuses.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(statuses, vec![
            ("test/a.parquet".to_string(), ManifestStatus::Existing),
            ("test/b.parquet".to_string(), ManifestStatus::Deleted),
        ]);
    }

    /// Pins: a delete that targets a file in only ONE of two manifests leaves the OTHER manifest
    /// carried forward UNCHANGED (same manifest_path) — guards efficiency + against rewriting (and
    /// possibly corrupting) manifests the delete does not touch.
    #[tokio::test]
    async fn test_delete_files_carries_untouched_manifest_forward_unchanged() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Two separate appends → two separate manifests.
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;
        let table = append_files(&catalog, &table, vec![data_file("test/b.parquet", 0)]).await;

        // Identify the manifest that holds A (the one we will NOT touch).
        let before = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let mut a_manifest_path = None;
        for manifest_file in before.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            if manifest
                .entries()
                .iter()
                .any(|e| e.file_path() == "test/a.parquet")
            {
                a_manifest_path = Some(manifest_file.manifest_path.clone());
            }
        }
        let a_manifest_path = a_manifest_path.expect("A's manifest should exist");

        // Delete B.
        let tx = Transaction::new(&table);
        let action = tx.delete_files().delete_file("test/b.parquet");
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // A's manifest is carried forward byte-for-byte (same path), and the live set is {A}.
        let after = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert!(
            after
                .entries()
                .iter()
                .any(|m| m.manifest_path == a_manifest_path),
            "the manifest untouched by the delete must be carried forward unchanged"
        );
        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/a.parquet".to_string()])
        );
    }

    /// Pins: a delete that spans MULTIPLE manifests removes the targeted file from each — guards
    /// against only rewriting the first matching manifest.
    #[tokio::test]
    async fn test_delete_files_across_multiple_manifests() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Three appends → three manifests, each with one file.
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;
        let table = append_files(&catalog, &table, vec![data_file("test/b.parquet", 0)]).await;
        let table = append_files(&catalog, &table, vec![data_file("test/c.parquet", 0)]).await;

        // Delete A and C (in different manifests) in one commit; keep B.
        let tx = Transaction::new(&table);
        let action = tx
            .delete_files()
            .delete_files(["test/a.parquet", "test/c.parquet"]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/b.parquet".to_string()])
        );
    }

    /// Pins: deleting EVERY live file in a manifest leaves an empty live set (no carried-forward
    /// added/existing files) and does not error — the all-deleted-manifest case.
    #[tokio::test]
    async fn test_delete_all_files_in_a_manifest_leaves_empty_live_set() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 0),
        ])
        .await;

        let tx = Transaction::new(&table);
        let action = tx
            .delete_files()
            .delete_files(["test/a.parquet", "test/b.parquet"]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert!(live_file_paths(&table).await.is_empty());
    }

    /// Pins: a delete-only commit (no added files) is allowed — the relaxed precondition. Without the
    /// relaxation this would fail the "no added data files" guard.
    #[tokio::test]
    async fn test_delete_only_commit_is_allowed() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;

        let tx = Transaction::new(&table);
        let action = tx.delete_files().delete_file("test/a.parquet");
        let tx = action.apply(tx).unwrap();
        let result = tx.commit(&catalog).await;

        assert!(result.is_ok(), "delete-only commit should be allowed");
        assert!(live_file_paths(&result.unwrap()).await.is_empty());
    }

    /// Pins: a truly-empty delete commit (no paths requested → no adds, no deletes, no snapshot
    /// properties) is REJECTED. The precondition relaxation lets delete-only commits through, but it
    /// must still reject a no-op commit (Java `SnapshotProducer` rejects an empty commit). Guards
    /// against the relaxation being too permissive (producing an empty no-op delete snapshot).
    #[tokio::test]
    async fn test_empty_delete_commit_is_rejected() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;

        // No delete_file() / delete_files() calls → nothing to delete, nothing added.
        let tx = Transaction::new(&table);
        let action = tx.delete_files();
        let tx = action.apply(tx).unwrap();
        let result = tx.commit(&catalog).await;

        assert!(
            result.is_err(),
            "a truly-empty delete commit must be rejected"
        );
    }

    /// Pins: deleting a file NOT present in the table is an error (Java `failMissingDeletePaths`) —
    /// guards against silently dropping an unmatched path and producing a no-op delete snapshot.
    #[tokio::test]
    async fn test_delete_absent_file_errors() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;

        let tx = Transaction::new(&table);
        let action = tx.delete_files().delete_file("test/does-not-exist.parquet");
        let tx = action.apply(tx).unwrap();
        let error = tx
            .commit(&catalog)
            .await
            .expect_err("absent file must error");
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert!(
            error.message().contains("Missing required files to delete"),
            "unexpected error message: {}",
            error.message()
        );
    }

    /// Pins provenance preservation across snapshots — the #1 corruption risk. When a delete
    /// rewrites a manifest, every SURVIVING entry must be copied forward as `Existing` carrying its
    /// ORIGINAL `snapshot_id`, `sequence_number`, and `file_sequence_number` (Java `writer.existing`
    /// preserves all three) — NOT re-stamped with the new delete snapshot/seq. The `Deleted`
    /// tombstone for the removed file gets the NEW snapshot id (Java `writer.delete`) but keeps the
    /// removed file's original data/file seq. A carried-forward (untouched) manifest's entries also
    /// keep their provenance.
    ///
    /// Risk pinned: a rewrite that re-stamps surviving entries with the commit snapshot/seq is
    /// silent table corruption (wrong data-sequence number breaks merge-on-read delete application
    /// and incremental scans). The other delete_files tests assert only the live PATH set + statuses
    /// + counts, so they all pass under a snapshot-id re-stamp (verified by mutation) — only this
    /// test catches it.
    #[tokio::test]
    async fn test_delete_preserves_surviving_entry_provenance_across_snapshots() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Append A in its OWN commit (snapshot S1, data seq 1).
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;
        let s1 = table.metadata().current_snapshot().unwrap().snapshot_id();

        // Append B and C in ONE commit (snapshot S2, data seq 2; one manifest with both).
        let table = append_files(&catalog, &table, vec![
            data_file("test/b.parquet", 0),
            data_file("test/c.parquet", 0),
        ])
        .await;
        let s2 = table.metadata().current_snapshot().unwrap().snapshot_id();
        assert_ne!(s1, s2);

        // Capture A's original provenance before the delete.
        let (a_snap, a_seq, a_fseq) = entry_provenance(&table, "test/a.parquet").await;
        let (b_snap, b_seq, b_fseq) = entry_provenance(&table, "test/b.parquet").await;
        assert_eq!(a_snap, Some(s1), "A added by S1");
        assert_eq!(b_snap, Some(s2), "B added by S2");
        assert_ne!(a_seq, b_seq, "A and B must have different data seq numbers");

        // Delete B → forces a rewrite of S2's manifest. C (the surviving S2 entry) must keep S2/seq2.
        let tx = Transaction::new(&table);
        let action = tx.delete_files().delete_file("test/b.parquet");
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let s3 = table.metadata().current_snapshot().unwrap().snapshot_id();
        assert_ne!(s3, s2);

        // C survived: rewritten as Existing, MUST keep S2's snapshot id + seq numbers (NOT S3).
        let (c_snap, c_seq, c_fseq) = entry_provenance(&table, "test/c.parquet").await;
        assert_eq!(
            c_snap,
            Some(s2),
            "surviving C must keep its ORIGINAL snapshot id S2, not S3"
        );
        assert_eq!(
            c_seq, b_seq,
            "surviving C must keep its ORIGINAL data seq, not the delete seq"
        );
        assert_eq!(
            c_fseq, b_fseq,
            "surviving C must keep its ORIGINAL file seq"
        );

        // A survived in its own (untouched, carried-forward) manifest with S1 provenance intact.
        let (a2_snap, a2_seq, a2_fseq) = entry_provenance(&table, "test/a.parquet").await;
        assert_eq!(a2_snap, Some(s1), "carried-forward A keeps S1");
        assert_eq!(a2_seq, a_seq, "carried-forward A keeps its data seq");
        assert_eq!(a2_fseq, a_fseq, "carried-forward A keeps its file seq");

        // The DELETED tombstone for B carries the NEW snapshot id S3 (Java writer.delete) but keeps
        // B's original data/file seq numbers.
        let del = deleted_entry_provenance(&table, "test/b.parquet").await;
        assert_eq!(
            del.0,
            Some(s3),
            "the Deleted tombstone for B gets the new snapshot id S3"
        );
        assert_eq!(
            del.1, b_seq,
            "the Deleted tombstone keeps B's original data seq"
        );
        assert_eq!(
            del.2, b_fseq,
            "the Deleted tombstone keeps B's original file seq"
        );
    }

    /// Pins the all-deleted-manifest lifecycle (Java `MergingSnapshotProducer.apply` keep/drop): a
    /// rewritten manifest whose live entries all became `Deleted` is KEPT by the commit that created
    /// it (its `added_snapshot_id` == the new snapshot id, Java's `snapshotId()==snapshotId()`), so
    /// its tombstones survive for one snapshot; the NEXT delete commit then DROPS it (it has no live
    /// files and `added_snapshot_id` != that commit's id, so `hasAddedFiles||hasExistingFiles` is
    /// false). Risk pinned: wrongly dropping the tombstones in the creating commit (loses Deleted
    /// records downstream/expiry needs) or wrongly retaining an empty manifest forever (clutter).
    #[tokio::test]
    async fn test_all_deleted_manifest_kept_by_creating_commit_then_dropped_by_next() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // M1: {A, B} in one manifest. M2: {C} in another.
        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 0),
        ])
        .await;
        let table = append_files(&catalog, &table, vec![data_file("test/c.parquet", 0)]).await;

        // Delete A and B → M1 becomes all-deleted; it must be KEPT (tombstones for A,B present).
        let tx = Transaction::new(&table);
        let action = tx
            .delete_files()
            .delete_files(["test/a.parquet", "test/b.parquet"]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let manifest_list = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let mut total_deleted = 0;
        let mut total_existing = 0;
        let mut total_added = 0;
        for mf in manifest_list.entries() {
            let m = mf.load_manifest(table.file_io()).await.unwrap();
            for e in m.entries() {
                match e.status() {
                    ManifestStatus::Deleted => total_deleted += 1,
                    ManifestStatus::Existing => total_existing += 1,
                    ManifestStatus::Added => total_added += 1,
                }
            }
        }
        // The all-deleted M1' is kept: 2 Deleted tombstones present. C carried forward (Added in M2).
        assert_eq!(
            total_deleted, 2,
            "all-deleted manifest must be KEPT with its tombstones"
        );
        assert_eq!(total_added + total_existing, 1, "only C is live");

        // Now delete C → a NEW commit. The all-deleted M1' (no live files) must be DROPPED now.
        let tx = Transaction::new(&table);
        let action = tx.delete_files().delete_file("test/c.parquet");
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let manifest_list = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        // M1' is gone (dropped); only M2-rewritten remains (C tombstone). A/B tombstones not retained.
        let mut a_or_b_tombstone = false;
        for mf in manifest_list.entries() {
            let m = mf.load_manifest(table.file_io()).await.unwrap();
            for e in m.entries() {
                if e.file_path() == "test/a.parquet" || e.file_path() == "test/b.parquet" {
                    a_or_b_tombstone = true;
                }
            }
        }
        assert!(
            !a_or_b_tombstone,
            "the all-deleted M1' must be DROPPED by the next commit (no live files)"
        );
        assert!(live_file_paths(&table).await.is_empty());
    }

    /// Return (snapshot_id, sequence_number, file_sequence_number) of the live entry for `path`.
    async fn entry_provenance(
        table: &Table,
        path: &str,
    ) -> (Option<i64>, Option<i64>, Option<i64>) {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                if entry.is_alive() && entry.file_path() == path {
                    return (
                        entry.snapshot_id(),
                        entry.sequence_number(),
                        entry.file_sequence_number,
                    );
                }
            }
        }
        panic!("no live entry for {path}");
    }

    /// Return (snapshot_id, sequence_number, file_sequence_number) of the DELETED entry for `path`.
    async fn deleted_entry_provenance(
        table: &Table,
        path: &str,
    ) -> (Option<i64>, Option<i64>, Option<i64>) {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                if entry.status() == ManifestStatus::Deleted && entry.file_path() == path {
                    return (
                        entry.snapshot_id(),
                        entry.sequence_number(),
                        entry.file_sequence_number,
                    );
                }
            }
        }
        panic!("no deleted entry for {path}");
    }

    /// Pins: a delete that targets a present file AND an absent file still errors (the present file
    /// is not silently removed while the absent one is ignored).
    #[tokio::test]
    async fn test_delete_mixed_present_and_absent_errors() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;

        let tx = Transaction::new(&table);
        let action = tx
            .delete_files()
            .delete_files(["test/a.parquet", "test/absent.parquet"]);
        let tx = action.apply(tx).unwrap();
        let error = tx
            .commit(&catalog)
            .await
            .expect_err("mixed delete must error");
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert!(error.message().contains("test/absent.parquet"));
    }

    // ============================================================================================
    // Files-exist conflict validation (Java `StreamingDelete.validateFilesExist` /
    // `MergingSnapshotProducer.validateDataFilesExist`).
    //
    // The race these tests simulate: a `delete_files` is BUILT against table head S0, but BEFORE it commits a
    // SEPARATE commit lands that DELETES a live data file (advancing the head to S1). When the delete then
    // commits, `do_commit` refreshes to S1 and runs the action's `validate` against that refreshed base. With
    // `validate_files_exist()` enabled, a concurrent removal of a file THIS action is also deleting must FAIL
    // the commit (non-retryable) naming the missing file — committing over a vanished required file is a
    // serializable-isolation violation. A concurrent removal of a DIFFERENT file must NOT fail. With the check
    // OFF (the default), neither fails (snapshot isolation, unchanged behavior).
    // ============================================================================================

    /// NO CONCURRENT DELETION. With the files-exist check enabled but nothing landing concurrently, the
    /// delete commits normally (the concurrent-deleted set is empty ⇒ no conflict). Pins that enabling the
    /// check does not block a race-free commit. Risk: a files-exist check that wrongly fails with no
    /// concurrent deletion.
    #[tokio::test]
    async fn test_delete_files_exist_validation_no_concurrent_deletion_succeeds() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 0),
        ])
        .await;
        let s0 = table.metadata().current_snapshot().unwrap().snapshot_id();

        let tx = Transaction::new(&table);
        let action = tx
            .delete_files()
            .delete_file("test/a.parquet")
            .validate_from_snapshot(s0)
            .validate_files_exist();
        let tx = action.apply(tx).unwrap();
        let table = tx
            .commit(&catalog)
            .await
            .expect("a race-free delete must commit even with the files-exist check enabled");

        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/b.parquet".to_string()]),
            "the delete applied: only b survives"
        );
    }

    /// THE HEADLINE FILES-EXIST TEST. Append S0 ({a, b}). Build a `delete_files(a)` with
    /// `.validate_from_snapshot(S0).validate_files_exist()`. Then a CONCURRENT `delete_files(a)` lands (S1),
    /// removing the SAME file this action is deleting. Committing must FAIL with a NON-retryable `DataInvalid`
    /// whose message NAMES the missing file `a` — a concurrent removal of a required file is a lost-update
    /// conflict under serializable isolation.
    ///
    /// Risk pinned: silently committing over a file that a concurrent commit already deleted. Without the
    /// check the re-based delete would either no-op or (because the file is no longer live) fail with a
    /// generic "missing required files to delete" — NOT the serializable-isolation validation error.
    #[tokio::test]
    async fn test_delete_files_exist_rejects_concurrent_deletion_of_same_file() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // S0: {a, b}.
        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 0),
        ])
        .await;
        let s0 = table.metadata().current_snapshot().unwrap().snapshot_id();

        // Build delete(a) with the files-exist check enabled, pinned to start at S0 (the head we read).
        let tx = Transaction::new(&table);
        let action = tx
            .delete_files()
            .delete_file("test/a.parquet")
            .validate_from_snapshot(s0)
            .validate_files_exist();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a separate delete removes the SAME file a.
        let _table_after_concurrent =
            commit_concurrent_delete(&catalog, &table, ["test/a.parquet"]).await;

        // Committing the delete must FAIL: S1 already removed the required file a.
        let err = tx.commit(&catalog).await.expect_err(
            "delete must fail: a concurrent commit removed the file this delete also requires",
        );

        assert_eq!(
            err.kind(),
            ErrorKind::DataInvalid,
            "a files-exist conflict is a non-retryable validation failure (DataInvalid)"
        );
        assert!(
            !err.retryable(),
            "the validation failure must be NON-retryable so the retry loop stops and it propagates \
             (it is NOT a retry-exhausted CatalogCommitConflicts)"
        );
        assert!(
            err.message().contains("Cannot commit, missing data files"),
            "the error must use the validateDataFilesExist wording, got: {}",
            err.message()
        );
        assert!(
            err.message().contains("test/a.parquet"),
            "the error must NAME the missing file, got: {}",
            err.message()
        );

        // The catalog head is still S1 (the concurrent delete) — the conflicting delete did NOT commit.
        let reloaded = catalog.load_table(table.identifier()).await.unwrap();
        assert_eq!(
            live_file_paths(&reloaded).await,
            HashSet::from(["test/b.parquet".to_string()]),
            "only the concurrent delete applied: b survives, a is gone"
        );
    }

    /// NEGATIVE CONTROL: same setup, but the concurrent deletion removes a DIFFERENT file (b) than the one
    /// this action deletes (a). The `delete_files(a)` files-exist check PASSES and the commit succeeds — a
    /// concurrent removal of an unrelated file is not a conflict.
    ///
    /// Risk pinned: an over-eager check that rejects ANY concurrent deletion (false positive) would break
    /// legitimate concurrent deletes of disjoint files.
    #[tokio::test]
    async fn test_delete_files_exist_allows_concurrent_deletion_of_different_file() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // S0: {a, b, c} in one manifest.
        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 0),
            data_file("test/c.parquet", 0),
        ])
        .await;
        let s0 = table.metadata().current_snapshot().unwrap().snapshot_id();

        // Build delete(a) with the check enabled.
        let tx = Transaction::new(&table);
        let action = tx
            .delete_files()
            .delete_file("test/a.parquet")
            .validate_from_snapshot(s0)
            .validate_files_exist();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a separate delete removes a DIFFERENT file b.
        let _ = commit_concurrent_delete(&catalog, &table, ["test/b.parquet"]).await;

        // The delete must SUCCEED — b's removal does not race a's deletion.
        let table = tx.commit(&catalog).await.expect(
            "delete must succeed: the concurrent deletion removed a different file (b), not a",
        );

        // Both a (this delete) and b (concurrent delete) are gone; c survives.
        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/c.parquet".to_string()]),
            "a and b both deleted (this + concurrent), c survives"
        );
    }

    /// OFF CONTROL: with the files-exist check NOT enabled (no `validate_files_exist()` call), a concurrent
    /// deletion of the same file does NOT fail with the validation error — this is snapshot isolation, the
    /// DEFAULT behavior, unchanged by this increment. (The re-based delete instead finds the file already
    /// gone and reports the generic missing-file error from path resolution — distinct from the
    /// validateDataFilesExist message — so this also proves the validation path is the OPT-IN one.)
    ///
    /// Risk pinned: the files-exist validation must be OPT-IN — turning it on for every delete by default
    /// would change existing behavior.
    #[tokio::test]
    async fn test_delete_files_exist_off_does_not_run_validation() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // S0: {a, b}.
        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 0),
        ])
        .await;

        // Build delete(a) WITHOUT enabling the check (default = snapshot isolation).
        let tx = Transaction::new(&table);
        let action = tx.delete_files().delete_file("test/a.parquet");
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a separate delete removes the SAME file a.
        let _ = commit_concurrent_delete(&catalog, &table, ["test/a.parquet"]).await;

        // With the check OFF, the validateDataFilesExist path never runs. The re-based delete finds a already
        // gone and fails the generic path-resolution check instead (NOT the validation error).
        let err = tx.commit(&catalog).await.expect_err(
            "with the check OFF, the re-based delete still cannot resolve the vanished file",
        );
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message().contains("Missing required files to delete"),
            "with validation OFF, the failure is the generic path-resolution error, not \
             validateDataFilesExist — got: {}",
            err.message()
        );
        assert!(
            !err.message().contains("Cannot commit, missing data files"),
            "the validateDataFilesExist message must NOT appear when the check is OFF: {}",
            err.message()
        );
    }

    /// VALIDATE-FROM-SNAPSHOT OVERRIDE TEST. The `validate_from_snapshot(id)` override changes which commits
    /// count as concurrent. Append S0 ({a, b}); a concurrent delete removes a (S1); then build delete(a) and
    /// commit. With `validate_from_snapshot(S0)` (the EARLIER snapshot) the S1 removal IS in the window ⇒ the
    /// files-exist check FAILS. With `validate_from_snapshot(S1)` (the CURRENT head) the window is empty ⇒ the
    /// check finds nothing (and the commit fails LATER, on path resolution, because a is already gone — NOT on
    /// the validation error).
    ///
    /// Risk pinned: ignoring the override (always using the tx start) would change which concurrent removals
    /// are detected; this proves the override widens / narrows the window as specified.
    #[tokio::test]
    async fn test_delete_files_exist_validate_from_snapshot_override_changes_window() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // S0: {a, b}.
        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 0),
        ])
        .await;
        let s0 = table.metadata().current_snapshot().unwrap().snapshot_id();

        // S1: a concurrent delete removes a (committed via the catalog, advancing the head).
        let table_s1 = commit_concurrent_delete(&catalog, &table, ["test/a.parquet"]).await;
        let s1 = table_s1
            .metadata()
            .current_snapshot()
            .unwrap()
            .snapshot_id();
        assert_ne!(s0, s1);

        // With validate_from_snapshot(S0): the window includes S1's removal ⇒ files-exist check FAILS naming a.
        let tx = Transaction::new(&table_s1);
        let action = tx
            .delete_files()
            .delete_file("test/a.parquet")
            .validate_from_snapshot(s0)
            .validate_files_exist();
        let tx = action.apply(tx).unwrap();
        let err = tx.commit(&catalog).await.expect_err(
            "validate_from_snapshot(S0) includes S1's removal of a ⇒ files-exist conflict",
        );
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(!err.retryable());
        assert!(
            err.message().contains("Cannot commit, missing data files")
                && err.message().contains("test/a.parquet"),
            "the override-widened window must surface the validateDataFilesExist error naming a: {}",
            err.message()
        );

        // With validate_from_snapshot(S1): the window starts AT the current head ⇒ empty ⇒ the validation
        // check finds nothing. The commit still fails, but on the GENERIC path-resolution error (a is gone),
        // NOT the validation error — proving the narrowed window skipped S1's removal.
        let tx = Transaction::new(&table_s1);
        let action = tx
            .delete_files()
            .delete_file("test/a.parquet")
            .validate_from_snapshot(s1)
            .validate_files_exist();
        let tx = action.apply(tx).unwrap();
        let err = tx.commit(&catalog).await.expect_err(
            "a is already gone, so even with an empty validation window the path cannot resolve",
        );
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message().contains("Missing required files to delete")
                && !err.message().contains("Cannot commit, missing data files"),
            "validate_from_snapshot(S1) empties the window ⇒ the failure is path resolution, not \
             validateDataFilesExist — got: {}",
            err.message()
        );
    }

    /// TX-CAPTURED START SURVIVES RE-BASE. The files-exist check works WITHOUT an explicit
    /// `validate_from_snapshot`, relying SOLELY on the transaction-captured starting snapshot id surviving
    /// `do_commit`'s re-base. Build `delete_files(a).validate_files_exist()` (NO `validate_from_snapshot`);
    /// the starting snapshot is the one captured in `Transaction::new` (= S0). A concurrent `delete_files(a)`
    /// lands (S1) removing the SAME file; `do_commit` overwrites `self.table` with the refreshed base (S1),
    /// but `starting_snapshot_id` must SURVIVE — so S1's removal of `a` is still enumerated and rejected with
    /// the `validateDataFilesExist` message naming `a`.
    ///
    /// Risk pinned: if `effective_start` were re-read from the REFRESHED head at validation time
    /// (`current.metadata().current_snapshot_id()`) instead of the tx-captured `starting_snapshot_id`, the
    /// window would start AT the current head ⇒ empty ⇒ the check silently always passes (a
    /// serializable-isolation hole) and the commit would instead fail on the GENERIC path-resolution error.
    /// Every OTHER files-exist test pins `validate_from_snapshot`, which short-circuits
    /// `validate_from_snapshot.or(starting_snapshot_id)` and never reads the tx-captured field — so this is
    /// the ONLY test that pins the `Transaction::new` capture surviving the re-base for DeleteFiles.
    #[tokio::test]
    async fn test_delete_files_exist_rejects_concurrent_using_tx_captured_starting_snapshot() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // S0: {a, b} (the head captured when the transaction is created below).
        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 0),
        ])
        .await;

        // Build delete(a) with the check enabled but WITHOUT validate_from_snapshot — the start is the
        // tx-captured head (S0).
        let tx = Transaction::new(&table);
        let action = tx
            .delete_files()
            .delete_file("test/a.parquet")
            .validate_files_exist();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a separate delete removes the SAME file a, advancing the head.
        let _concurrent = commit_concurrent_delete(&catalog, &table, ["test/a.parquet"]).await;

        // Committing must FAIL via the tx-captured start surviving the re-base: S1's removal of a is in the
        // window, so the files-exist check fires with the validateDataFilesExist message naming a.
        let err = tx.commit(&catalog).await.expect_err(
            "conflict must be detected via the tx-captured starting snapshot (no validate_from_snapshot)",
        );
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(!err.retryable());
        assert!(
            err.message().contains("Cannot commit, missing data files")
                && err.message().contains("test/a.parquet"),
            "the tx-captured window must surface the validateDataFilesExist error naming a (NOT the generic \
             path-resolution error) — got: {}",
            err.message()
        );
    }

    // ============================================================================================
    // Merge-on-read DELETE-MANIFEST CARRY (Increment 2b — the silent-resurrection bug fix).
    //
    // `existing_manifest` now returns the FULL manifest list (DATA + DELETE) via the shared
    // `SnapshotProducer::current_manifests`, so a `delete_files` commit on a table that already carries
    // outstanding position/equality deletes preserves those delete manifests instead of dropping them
    // table-wide. These tests use the row_delta crown-jewel fixture (real parquet + a REAL position-delete
    // file written by the production writer + a production scan), so the resurrection physics is proven
    // end-to-end, not just at the manifest-metadata level.
    // ============================================================================================

    /// Write a REAL parquet data file with rows `(x, y, z)` into the table location, routed to partition
    /// `x = part_value`. Returns the finished partitioned [`DataFile`]. Mirrors the row_delta fixture.
    async fn write_data_file(
        table: &Table,
        file_name: &str,
        part_value: i64,
        rows: &[(i64, i64, i64)],
    ) -> DataFile {
        use crate::arrow::schema_to_arrow_schema;

        let schema = table.metadata().current_schema();
        let arrow_schema = Arc::new(schema_to_arrow_schema(schema).unwrap());

        let xs: Vec<i64> = rows.iter().map(|(x, _, _)| *x).collect();
        let ys: Vec<i64> = rows.iter().map(|(_, y, _)| *y).collect();
        let zs: Vec<i64> = rows.iter().map(|(_, _, z)| *z).collect();
        let batch = RecordBatch::try_new(arrow_schema, vec![
            Arc::new(Int64Array::from(xs)) as ArrayRef,
            Arc::new(Int64Array::from(ys)) as ArrayRef,
            Arc::new(Int64Array::from(zs)) as ArrayRef,
        ])
        .unwrap();

        let file_path = format!("{}/data/{}", table.metadata().location(), file_name);
        let output = table.file_io().new_output(file_path).unwrap();
        let parquet_builder = ParquetWriterBuilder::new(
            parquet::file::properties::WriterProperties::builder().build(),
            schema.clone(),
        );
        let mut writer = parquet_builder.build(output).await.unwrap();
        writer.write(&batch).await.unwrap();
        let data_file_builders = writer.close().await.unwrap();

        let mut builder = data_file_builders.into_iter().next().unwrap();
        builder
            .content(DataContentType::Data)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(part_value))]))
            .build()
            .unwrap()
    }

    /// Write a REAL position-delete parquet file (via the production `PositionDeleteFileWriter`) into the
    /// table location, deleting the given `(data_file_path, pos)` pairs, in partition `x = part_value`.
    async fn write_position_delete_file(
        table: &Table,
        part_value: i64,
        deletes: &[(String, i64)],
    ) -> DataFile {
        use arrow_array::StringArray;

        let config = PositionDeleteWriterConfig::new().unwrap();
        let location_gen = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
        let file_name_gen = DefaultFileNameGenerator::new(
            "pos-del".to_string(),
            Some(uuid::Uuid::now_v7().to_string()),
            DataFileFormat::Parquet,
        );
        let parquet_builder = ParquetWriterBuilder::new(
            parquet::file::properties::WriterProperties::builder().build(),
            config.schema().clone(),
        );
        let rolling = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_builder,
            table.file_io().clone(),
            location_gen,
            file_name_gen,
        );
        let partition_key = crate::spec::PartitionKey::new(
            table.metadata().default_partition_spec().as_ref().clone(),
            table.metadata().current_schema().clone(),
            Struct::from_iter([Some(Literal::long(part_value))]),
        );
        let mut writer = PositionDeleteFileWriterBuilder::new(rolling, config.clone())
            .build(Some(partition_key))
            .await
            .unwrap();

        let paths: Vec<&str> = deletes.iter().map(|(p, _)| p.as_str()).collect();
        let positions: Vec<i64> = deletes.iter().map(|(_, pos)| *pos).collect();
        let batch = RecordBatch::try_new(config.arrow_schema().clone(), vec![
            Arc::new(StringArray::from(paths)) as ArrayRef,
            Arc::new(Int64Array::from(positions)) as ArrayRef,
        ])
        .unwrap();
        writer.write(batch).await.unwrap();
        writer.close().await.unwrap().into_iter().next().unwrap()
    }

    /// Scan the table and collect the `y` column values across all returned batches — the real read-side
    /// signal (what a query would see, with merge-on-read deletes applied).
    async fn scan_y_values(table: &Table) -> HashSet<i64> {
        let stream = table
            .scan()
            .select(["y"])
            .build()
            .unwrap()
            .to_arrow()
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let mut values = HashSet::new();
        for batch in batches {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for index in 0..col.len() {
                values.insert(col.value(index));
            }
        }
        values
    }

    /// Count the DELETE-content manifests in the table's current snapshot manifest list (structural
    /// signal, independent of the read path). A delete must carry outstanding delete manifests forward,
    /// so this count must NOT drop to 0 across the commit.
    async fn count_delete_manifests(table: &Table) -> usize {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        manifest_list
            .entries()
            .iter()
            .filter(|m| m.content == ManifestContentType::Deletes)
            .count()
    }

    /// THE CROWN JEWEL (risk: a `delete_files` on a merge-on-read table silently DROPS every outstanding
    /// delete manifest, resurrecting deleted rows table-wide). Data file X (partition 0) carries a real
    /// position delete masking its row y=20; data file Y lives in partition 1. `delete_files(Y)` must
    /// remove Y AND keep X's delete applying — the scan after the commit is exactly {10} (X's masked y=20
    /// stays absent, Y's rows are gone).
    ///
    /// MUTATION (run manually, then restore): in `DeleteFilesOperation::existing_manifest`, filter the
    /// `current_manifests()` result to DATA manifests only (the old data-only behavior) ⇒ this test FAILS
    /// with y=20 resurrected (the scan returns {10, 20}) AND the structural delete-manifest count drops to 0.
    #[tokio::test]
    async fn test_delete_files_preserves_outstanding_delete_manifests_no_resurrection() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;

        // X in partition 0 with rows y = [10, 20]; Y in partition 1 with rows y = [60, 70].
        let x = write_data_file(&table, "x.parquet", 0, &[(0, 10, 100), (0, 20, 200)]).await;
        let x_path = x.file_path().to_string();
        let y = write_data_file(&table, "y.parquet", 1, &[(1, 60, 600), (1, 70, 700)]).await;
        let y_path = y.file_path().to_string();
        let table = append_files(&catalog, &table, vec![x, y]).await;

        // RowDelta a REAL position delete masking X's row at position 1 (y=20).
        let pos_delete = write_position_delete_file(&table, 0, &[(x_path.clone(), 1)]).await;
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![pos_delete]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        assert_eq!(
            count_delete_manifests(&table).await,
            1,
            "the row_delta must leave one delete manifest in the snapshot"
        );

        // Sanity: before the delete_files, the scan drops y=20 (X's masked row) and shows Y's rows.
        assert_eq!(
            scan_y_values(&table).await,
            HashSet::from([10, 60, 70]),
            "the position delete masks y=20 from X; Y's rows are present"
        );

        // Delete Y (partition 1). This must NOT drop X's outstanding delete manifest.
        let tx = Transaction::new(&table);
        let action = tx.delete_files().delete_file(&y_path);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Delete
        );

        // SCAN PIN: X's masked row y=20 is STILL ABSENT and Y's rows are gone ⇒ exactly {10}.
        assert_eq!(
            scan_y_values(&table).await,
            HashSet::from([10]),
            "Y's rows are deleted AND X's masked y=20 stays absent — no resurrection"
        );

        // STRUCTURAL PIN: the delete manifest survived the commit (count must not drop to 0).
        assert_eq!(
            count_delete_manifests(&table).await,
            1,
            "the delete_files commit must carry the outstanding delete manifest forward (not drop it)"
        );
    }

    // ===============================================================================================
    // stage_only() on a DELETE-bearing action (Java `SnapshotProducer.stageOnly()` is on the base
    // producer, so a delete stages identically to an append). A staged delete ADDS its `Delete` snapshot
    // (with its rewritten/tombstoned manifests) to metadata but moves NO ref, so the deleted rows stay
    // VISIBLE to readers until the staged snapshot is published.
    // ===============================================================================================

    /// STAGE_ONLY on a delete-bearing action stages identically to an append: the staged `Delete` snapshot
    /// is added to metadata but `main` / current-snapshot-id / snapshot-log are UNCHANGED, and a scan still
    /// sees the row the staged delete would remove (the delete is invisible until published).
    ///
    /// Risk pinned: a staged DELETE that publishes its removal early (the deleted rows vanish from readers
    /// before audit) — the inverse of the append crown jewel, on the removal path.
    #[tokio::test]
    async fn test_delete_files_stage_only_stages_without_moving_main() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Publish {a, b}.
        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 0),
        ])
        .await;
        let base_id = table.metadata().current_snapshot_id();
        let base_log = table.metadata().history().to_vec();
        let base_snapshot_count = table.metadata().snapshots().count();

        // STAGE a delete of a.
        let tx = Transaction::new(&table);
        let action = tx.delete_files().stage_only().delete_file("test/a.parquet");
        let tx = action.apply(tx).unwrap();
        let staged_table = tx.commit(&catalog).await.unwrap();
        let reloaded = catalog.load_table(staged_table.identifier()).await.unwrap();
        let metadata = reloaded.metadata();

        // A new (Delete) snapshot was ADDED, but main did not move.
        assert_eq!(
            metadata.snapshots().count(),
            base_snapshot_count + 1,
            "the staged delete snapshot must be added to metadata"
        );
        assert_eq!(
            metadata.current_snapshot_id(),
            base_id,
            "stage_only on a delete must NOT advance current-snapshot-id"
        );
        assert_eq!(
            metadata.snapshot_for_ref("main").unwrap().snapshot_id(),
            base_id.unwrap(),
            "stage_only on a delete must NOT move the main ref"
        );
        assert_eq!(
            metadata.history().to_vec(),
            base_log,
            "stage_only on a delete must NOT add a snapshot-log entry"
        );
        // The staged snapshot records Operation::Delete.
        let staged_snapshot = metadata
            .snapshots()
            .find(|s| Some(s.snapshot_id()) != base_id)
            .unwrap();
        assert_eq!(staged_snapshot.summary().operation, Operation::Delete);

        // SCAN PIN: the readable (current-snapshot) live set still includes a — the staged delete is hidden.
        assert_eq!(
            live_file_paths(&reloaded).await,
            HashSet::from(["test/a.parquet".to_string(), "test/b.parquet".to_string()]),
            "the staged delete does not remove a from the readable table; both files stay live"
        );
    }
}
