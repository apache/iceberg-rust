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

//! This module contains the overwrite-files action.
//!
//! [`OverwriteFilesAction`] adds data files AND removes data files in a single `Overwrite` snapshot
//! (Java `BaseOverwriteFiles`). It composes the two existing producer paths: the added files reach
//! the producer exactly as fast-append does (written to a new added manifest), and the deleted files
//! are resolved + filtered out of the current snapshot's manifests exactly as `DeleteFiles` does
//! (via the shared [`SnapshotProducer::resolve_delete_paths`] / `process_deletes` machinery). Both
//! happen in one snapshot.
//!
//! **Operation recorded:** this action records the snapshot operation DYNAMICALLY, mirroring Java
//! `BaseOverwriteFiles.operation()` exactly: a delete-only overwrite (deletes requested, no adds) records
//! [`Operation::Delete`], an add-only overwrite (adds requested, no deletes) records [`Operation::Append`],
//! and an overwrite that both adds and deletes records [`Operation::Overwrite`]. The classification is
//! keyed on the REQUESTED sets (whether any file was added, whether any delete path was requested) —
//! before the delete paths are resolved against the table — matching Java's `addsDataFiles()` /
//! `deletesDataFiles()`. The snapshot summary carries the precise added/deleted file & record counts in
//! every case.
//!
//! **Out of scope (deferred):**
//! - `overwriteByRowFilter(Expression)` (Java) — needs inclusive/strict metrics evaluators to select
//!   and validate files by row predicate.
//! - Concurrent-commit conflict validation (`validateNoConflictingData` /
//!   `validateNoConflictingDeletes` / `validateFromSnapshot` / `conflictDetectionFilter`) — these
//!   implement serializable isolation and need validation-history replay across the ancestor chain.
//!
//! This increment is the explicit add + delete core.

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

/// A transaction action that overwrites files: it adds data files AND removes data files in a single
/// `Overwrite` snapshot.
///
/// Use [`crate::transaction::Transaction::overwrite_files`] to create one. Accumulate the files to
/// add with [`OverwriteFilesAction::add_file`] / [`OverwriteFilesAction::add_files`] and the files to
/// remove with [`OverwriteFilesAction::delete_file`] / [`OverwriteFilesAction::delete_files`] /
/// [`OverwriteFilesAction::delete_data_files`], then apply and commit the transaction.
///
/// An overwrite with adds-only (no deletes) or deletes-only (no adds) is allowed; a truly-empty
/// overwrite (no adds, no deletes, no snapshot properties) is rejected.
pub struct OverwriteFilesAction {
    /// Data files to add to the table (validated like fast append).
    added_data_files: Vec<DataFile>,
    /// Fully-qualified file paths to remove from the table.
    delete_paths: HashSet<String>,
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
}

impl OverwriteFilesAction {
    pub(crate) fn new() -> Self {
        Self {
            added_data_files: vec![],
            delete_paths: HashSet::default(),
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
        }
    }

    /// Add a single [`DataFile`] to the table (Java `OverwriteFiles.addFile`).
    pub fn add_file(mut self, data_file: DataFile) -> Self {
        self.added_data_files.push(data_file);
        self
    }

    /// Add multiple [`DataFile`]s to the table.
    pub fn add_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(data_files);
        self
    }

    /// Delete a single file by its fully-qualified path.
    ///
    /// To remove a file from the table, this path must equal a path in the table's metadata (mirrors
    /// Java `OverwriteFiles.deleteFile` / `MergingSnapshotProducer.delete`).
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
impl TransactionAction for OverwriteFilesAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
        );

        // Validate the added files like fast append: data content type, partition-spec match, and
        // partition-value compatibility (Java `MergingSnapshotProducer.add` checks the spec exists; the
        // producer reuses fast-append's `validate_added_data_files`). The delete paths are resolved and
        // validated (present; mixed present+absent errors) inside the producer's commit via the
        // operation's `delete_files` seam (Java `failMissingDeletePaths`).
        snapshot_producer.validate_added_data_files()?;

        snapshot_producer
            .commit(
                OverwriteFilesOperation {
                    delete_paths: self.delete_paths.clone(),
                    // The recorded operation is classified on the REQUESTED sets (Java `addsDataFiles()` =
                    // requested added files non-empty), evaluated before the deletes are resolved.
                    adds_data_files: !self.added_data_files.is_empty(),
                },
                DefaultManifestProcess,
            )
            .await
    }
}

/// The [`SnapshotProduceOperation`] for [`OverwriteFilesAction`].
///
/// Classifies the snapshot operation dynamically (Java `BaseOverwriteFiles.operation()`), exposes every
/// current data manifest as the set to filter, and resolves the requested delete paths against the
/// current snapshot's live data entries (the resolved [`DataFile`]s drive the producer's manifest
/// rewrite). The added files reach the producer separately (passed to `SnapshotProducer::new`), so a
/// single snapshot carries both the added manifest and the rewritten (filtered) manifests.
struct OverwriteFilesOperation {
    delete_paths: HashSet<String>,
    /// Whether this overwrite requested any added data files. Combined with whether any delete path was
    /// requested (`delete_paths` non-empty), this classifies the recorded operation the way Java
    /// `BaseOverwriteFiles.operation()` does.
    adds_data_files: bool,
}

impl SnapshotProduceOperation for OverwriteFilesOperation {
    /// Classify the recorded operation exactly as Java `BaseOverwriteFiles.operation()` does, on the
    /// REQUESTED add/delete sets: delete-only → [`Operation::Delete`], add-only → [`Operation::Append`],
    /// both → [`Operation::Overwrite`]. An empty overwrite (neither) is rejected before this is read, so
    /// the both-empty case here would fall through to `Overwrite` and never commit.
    fn operation(&self) -> Operation {
        let deletes_data_files = !self.delete_paths.is_empty();
        match (self.adds_data_files, deletes_data_files) {
            (false, true) => Operation::Delete,
            (true, false) => Operation::Append,
            _ => Operation::Overwrite,
        }
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
        // `DeleteFiles` via `SnapshotProducer::resolve_delete_paths`.
        snapshot_produce
            .resolve_delete_paths(&self.delete_paths)
            .await
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        // Expose every current data manifest; the producer's `process_deletes` decides per manifest
        // whether to rewrite (to drop deleted files), carry forward unchanged, or drop it.
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

    /// THE KEY TEST. Append A, B, C; then `overwrite_files()` delete B + add D → the post-commit SCAN
    /// live set is exactly {A, C, D}, the snapshot operation is Overwrite, B's entry is Deleted, and the
    /// surviving entries keep their provenance. Pins the core compose-add-and-delete-in-one-snapshot
    /// risk: a wrong live set (lost A/C, kept B, or missing D) is silent data corruption.
    #[tokio::test]
    async fn test_overwrite_delete_one_add_one_yields_correct_live_scan_set() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Fast-append A, B, C in one commit (one manifest containing all three).
        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 0),
            data_file("test/c.parquet", 0),
        ])
        .await;

        // Overwrite: delete B, add D — in one snapshot.
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .delete_file("test/b.parquet")
            .add_file(data_file("test/d.parquet", 0));
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // The new snapshot is an Overwrite, and the live scan set is exactly {A, C, D}.
        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Overwrite
        );
        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from([
                "test/a.parquet".to_string(),
                "test/c.parquet".to_string(),
                "test/d.parquet".to_string(),
            ])
        );

        // B's entry is present as Deleted (the rewritten manifest tombstone).
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let mut b_deleted = false;
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                if entry.file_path() == "test/b.parquet" {
                    assert_eq!(entry.status(), ManifestStatus::Deleted);
                    b_deleted = true;
                }
            }
        }
        assert!(b_deleted, "B must appear as a Deleted tombstone");
    }

    /// Pins: an overwrite with adds only (no deletes) succeeds, adds the files, and records the
    /// `Append` operation — matching Java `BaseOverwriteFiles.operation()` (add-only → APPEND). Risk: the
    /// action recording the wrong operation (e.g. always `Overwrite`), or wrongly rejecting an add-only
    /// overwrite. A wrong operation misleads every downstream consumer that branches on snapshot type.
    #[tokio::test]
    async fn test_overwrite_add_only_records_append_operation() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;

        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .add_file(data_file("test/b.parquet", 0));
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Append,
            "an add-only overwrite records Append (Java BaseOverwriteFiles.operation())"
        );
        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/a.parquet".to_string(), "test/b.parquet".to_string()])
        );
    }

    /// Pins: an overwrite with deletes only (no adds) succeeds, removes the file, and records the
    /// `Delete` operation — matching Java `BaseOverwriteFiles.operation()` (delete-only → DELETE). Risk:
    /// the add-only precondition wrongly tripping on a delete-only overwrite, or recording the wrong op
    /// (e.g. always `Overwrite`). A delete-only commit mislabeled `Overwrite` corrupts snapshot history
    /// semantics for consumers that distinguish pure deletes.
    #[tokio::test]
    async fn test_overwrite_delete_only_records_delete_operation() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 0),
        ])
        .await;

        let tx = Transaction::new(&table);
        let action = tx.overwrite_files().delete_file("test/b.parquet");
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Delete,
            "a delete-only overwrite records Delete (Java BaseOverwriteFiles.operation())"
        );
        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/a.parquet".to_string()])
        );
    }

    /// Pins: replacing a file with a new one in the SAME partition (identity(x)=0) overwrites correctly —
    /// the old file is removed and the new file added, both routed to the same partition. Risk: a
    /// partition-keyed rewrite wrongly dropping the new file or keeping the old one in the same
    /// partition (the canonical "replace a partition's contents" overwrite shape).
    #[tokio::test]
    async fn test_overwrite_replaces_file_in_same_partition() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        // old.parquet lives in partition x=0.
        let table = append_files(&catalog, &table, vec![data_file("test/old.parquet", 0)]).await;

        // Replace old.parquet with new.parquet, both in partition x=0.
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .delete_file("test/old.parquet")
            .add_file(data_file("test/new.parquet", 0));
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/new.parquet".to_string()])
        );
    }

    /// Pins: an overwrite that deletes an ABSENT file errors (Java `failMissingDeletePaths`), and does
    /// NOT silently add the added file. Risk: silently dropping the unmatched delete path and committing
    /// a partial overwrite (the added file lands but the intended removal never happened).
    #[tokio::test]
    async fn test_overwrite_delete_absent_file_errors() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;

        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .delete_file("test/does-not-exist.parquet")
            .add_file(data_file("test/b.parquet", 0));
        let tx = action.apply(tx).unwrap();
        let error = tx
            .commit(&catalog)
            .await
            .expect_err("absent delete file must error");
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert!(
            error.message().contains("Missing required files to delete"),
            "unexpected error message: {}",
            error.message()
        );

        // The table is unchanged — the failed overwrite did not add b.parquet.
        let reloaded = catalog.load_table(table.identifier()).await.unwrap();
        assert_eq!(
            live_file_paths(&reloaded).await,
            HashSet::from(["test/a.parquet".to_string()])
        );
    }

    /// Pins: an overwrite that deletes a present file AND an absent file still errors (the present file
    /// is not silently removed while the absent one is ignored). Risk: a partial delete that removes the
    /// matched file and silently skips the unmatched one.
    #[tokio::test]
    async fn test_overwrite_mixed_present_and_absent_delete_errors() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;

        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .delete_files(["test/a.parquet", "test/absent.parquet"]);
        let tx = action.apply(tx).unwrap();
        let error = tx
            .commit(&catalog)
            .await
            .expect_err("mixed present+absent delete must error");
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert!(error.message().contains("test/absent.parquet"));
    }

    /// Pins: a truly-empty overwrite (no adds, no deletes, no snapshot properties) is REJECTED. Risk:
    /// the add+delete precondition relaxation being too permissive and producing an empty no-op
    /// Overwrite snapshot.
    #[tokio::test]
    async fn test_empty_overwrite_is_rejected() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;

        let tx = Transaction::new(&table);
        let action = tx.overwrite_files();
        let tx = action.apply(tx).unwrap();
        let result = tx.commit(&catalog).await;

        assert!(result.is_err(), "a truly-empty overwrite must be rejected");
    }

    /// Pins provenance preservation across snapshots — the #1 corruption risk (the Increment-1 lesson).
    /// When an overwrite rewrites a manifest to delete a file, every SURVIVING entry must be copied
    /// forward as `Existing` carrying its ORIGINAL `snapshot_id` + both sequence numbers (NOT re-stamped
    /// with the new overwrite snapshot/seq), and the added file gets the NEW snapshot's provenance. The
    /// `Deleted` tombstone keeps the removed file's original data/file seq but gets the new snapshot id.
    ///
    /// Risk pinned: a rewrite that re-stamps surviving entries with the commit snapshot/seq is silent
    /// table corruption (wrong data-sequence number breaks merge-on-read delete application and
    /// incremental scans). The other overwrite tests assert only the live PATH set + op, so they pass
    /// under a snapshot-id re-stamp — only this test catches it.
    #[tokio::test]
    async fn test_overwrite_preserves_surviving_entry_provenance_across_snapshots() {
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

        // Capture original provenance before the overwrite.
        let (a_snap, a_seq, a_fseq) = entry_provenance(&table, "test/a.parquet").await;
        let (b_snap, b_seq, b_fseq) = entry_provenance(&table, "test/b.parquet").await;
        assert_eq!(a_snap, Some(s1), "A added by S1");
        assert_eq!(b_snap, Some(s2), "B added by S2");
        assert_ne!(a_seq, b_seq, "A and B must have different data seq numbers");

        // Overwrite: delete B + add D → rewrites S2's manifest (C survives) and adds a new manifest (D).
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .delete_file("test/b.parquet")
            .add_file(data_file("test/d.parquet", 0));
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let s3 = table.metadata().current_snapshot().unwrap().snapshot_id();
        assert_ne!(s3, s2);

        // C survived: rewritten as Existing, MUST keep S2's snapshot id + seq numbers (NOT S3).
        let (c_snap, c_seq, c_fseq) = entry_provenance(&table, "test/c.parquet").await;
        assert_eq!(
            c_snap,
            Some(s2),
            "surviving C must keep its ORIGINAL snapshot id S2, not the overwrite snapshot S3"
        );
        assert_eq!(
            c_seq, b_seq,
            "surviving C must keep its ORIGINAL data seq, not the overwrite seq"
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

        // The added file D gets the NEW overwrite snapshot's provenance (S3 + the new seq).
        let (d_snap, d_seq, _d_fseq) = entry_provenance(&table, "test/d.parquet").await;
        assert_eq!(
            d_snap,
            Some(s3),
            "added D gets the new overwrite snapshot id"
        );
        assert_ne!(
            d_seq, b_seq,
            "added D gets the new (higher) data seq, not the deleted file's seq"
        );

        // The DELETED tombstone for B carries the NEW snapshot id S3 but keeps B's original data/file seq.
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let mut b_tombstone = None;
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                if entry.status() == ManifestStatus::Deleted
                    && entry.file_path() == "test/b.parquet"
                {
                    b_tombstone = Some((
                        entry.snapshot_id(),
                        entry.sequence_number(),
                        entry.file_sequence_number,
                    ));
                }
            }
        }
        let b_tombstone = b_tombstone.expect("B must have a Deleted tombstone");
        assert_eq!(
            b_tombstone.0,
            Some(s3),
            "the Deleted tombstone for B gets the new snapshot id S3"
        );
        assert_eq!(
            b_tombstone.1, b_seq,
            "the Deleted tombstone keeps B's original data seq"
        );
        assert_eq!(
            b_tombstone.2, b_fseq,
            "the Deleted tombstone keeps B's original file seq"
        );
    }

    /// Pins the overwrite SUMMARY reflecting BOTH added and deleted file/record counts (Java
    /// `MergingSnapshotProducer.apply` merges the added-files summary AND the filter-manager's deleted
    /// summary). Risk: the summary only summing added files (the pre-existing behavior) — it would
    /// under-report the overwrite, breaking downstream tooling that reads `deleted-data-files` /
    /// `deleted-records`.
    #[tokio::test]
    async fn test_overwrite_summary_reflects_added_and_deleted_counts() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        // A and B each carry one record.
        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 0),
        ])
        .await;

        // Overwrite: delete B (1 file, 1 record) + add D (1 file, 1 record).
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .delete_file("test/b.parquet")
            .add_file(data_file("test/d.parquet", 0));
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let summary = table.metadata().current_snapshot().unwrap().summary();
        let props = &summary.additional_properties;
        assert_eq!(
            props.get("added-data-files").map(String::as_str),
            Some("1"),
            "summary must report one added data file"
        );
        assert_eq!(
            props.get("added-records").map(String::as_str),
            Some("1"),
            "summary must report one added record"
        );
        assert_eq!(
            props.get("deleted-data-files").map(String::as_str),
            Some("1"),
            "summary must report one deleted data file (Java overwrite summary)"
        );
        assert_eq!(
            props.get("deleted-records").map(String::as_str),
            Some("1"),
            "summary must report one deleted record (Java overwrite summary)"
        );
    }

    /// Read a usize total from a snapshot summary property, defaulting to 0 when absent.
    fn total(table: &Table, prop: &str) -> u64 {
        table
            .metadata()
            .current_snapshot()
            .unwrap()
            .summary()
            .additional_properties
            .get(prop)
            .map(|value| value.parse::<u64>().unwrap())
            .unwrap_or(0)
    }

    /// Pins the CUMULATIVE running totals across snapshots (Java `SnapshotProducer.summary(previous)`
    /// seeds each snapshot's totals from the PREVIOUS branch head's summary, so `total-data-files` /
    /// `total-records` ACCUMULATE). Append two files, append two more, then overwrite-delete one: the
    /// running totals must be 2 → 4 → 3, never just the current commit's delta.
    ///
    /// Risk pinned: the producer seeding `previous_snapshot` from the not-yet-committed new snapshot id
    /// (the pre-fix bug) makes every snapshot's totals reflect only THIS commit (seed 0), AND a net
    /// removal underflows `0 - removed`. This test FAILS under the old seed-0 logic — the final delete
    /// snapshot would compute `total-data-files = 0 + 0 - 1` (u64 underflow / panic) instead of
    /// `4 - 1 = 3`. It is the regression guard for fix (a) and affects EVERY snapshot action, not just
    /// overwrite.
    #[tokio::test]
    async fn test_running_totals_accumulate_across_snapshots() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Snapshot 1: append A, B (2 files, 2 records). Running totals = 2.
        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 0),
        ])
        .await;
        assert_eq!(
            total(&table, "total-data-files"),
            2,
            "after appending 2 files, total-data-files = 2"
        );
        assert_eq!(
            total(&table, "total-records"),
            2,
            "after appending 2 files (1 record each), total-records = 2"
        );

        // Snapshot 2: append C, D (2 more). Running totals must ACCUMULATE to 4 (not reset to 2).
        let table = append_files(&catalog, &table, vec![
            data_file("test/c.parquet", 0),
            data_file("test/d.parquet", 0),
        ])
        .await;
        assert_eq!(
            total(&table, "total-data-files"),
            4,
            "totals accumulate from the previous branch head: 2 + 2 = 4, not just this commit's 2"
        );
        assert_eq!(
            total(&table, "total-records"),
            4,
            "records accumulate: 2 + 2 = 4"
        );

        // Snapshot 3: overwrite-delete A (net removal). Running totals must be 4 - 1 = 3. Under the old
        // seed-0 logic this computes 0 - 1 and underflows; under the fix it is the correct cumulative 3.
        let tx = Transaction::new(&table);
        let action = tx.overwrite_files().delete_file("test/a.parquet");
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        assert_eq!(
            total(&table, "total-data-files"),
            3,
            "after deleting 1 of 4 files, total-data-files = 4 - 1 = 3 (cumulative, no underflow)"
        );
        assert_eq!(
            total(&table, "total-records"),
            3,
            "after deleting 1 record, total-records = 4 - 1 = 3 (cumulative, no underflow)"
        );
        // The delete-only overwrite is recorded as a Delete (dynamic operation).
        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Delete
        );
    }
}
