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
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};

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
}

impl DeleteFilesAction {
    pub(crate) fn new() -> Self {
        Self {
            delete_paths: HashSet::default(),
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
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
        );

        snapshot_producer
            .commit(
                DeleteFilesOperation {
                    delete_paths: self.delete_paths.clone(),
                },
                DefaultManifestProcess,
            )
            .await
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
        // Expose every current data manifest; the producer's `process_deletes` decides per manifest
        // whether to rewrite, carry forward unchanged, or drop it.
        snapshot_produce.current_data_manifests().await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, ManifestStatus,
        Operation, Struct,
    };
    use crate::table::Table;
    use crate::transaction::tests::make_v3_minimal_table_in_catalog;
    use crate::transaction::{ApplyTransactionAction, Transaction};
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
}
