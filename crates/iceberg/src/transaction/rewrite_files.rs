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

//! This module contains the rewrite-files action (the compaction-commit primitive).
//!
//! [`RewriteFilesAction`] atomically REPLACES a set of data files with a new set of data files in a
//! single `Replace` snapshot (Java `BaseRewriteFiles`). This is the commit primitive a compaction job
//! uses: it rewrites the same logical rows into a new (usually smaller / better-laid-out) set of files,
//! so the live row set must be logically unchanged. It composes the two existing producer paths exactly
//! like [`crate::transaction::overwrite_files::OverwriteFilesAction`]: the added files reach the producer
//! as fast-append does (written to a new added manifest), and the to-delete files are resolved BY PATH +
//! filtered out of the current snapshot's manifests via the shared [`SnapshotProducer::resolve_delete_paths`]
//! / `process_deletes` machinery. Both happen in one snapshot.
//!
//! **Operation recorded:** always [`Operation::Replace`] (Java `BaseRewriteFiles.operation()` returns
//! `DataOperations.REPLACE`). Unlike `OverwriteFiles` (whose operation is dynamic), a rewrite is always
//! a Replace — the row set is unchanged, only the physical files differ.
//!
//! **Validation mirrored (cited against `core/.../BaseRewriteFiles.java`):**
//! - the constructor calls `failMissingDeletePaths()` — every data file to delete MUST be present in the
//!   current snapshot (error if absent). This is enforced by the shared `resolve_delete_paths` (Java
//!   `failMissingDeletePaths` / `validateRequiredDeletes`).
//! - `validate()` → `validateReplacedAndAddedFiles()` requires (for the data-file case):
//!   1. `deletesDataFiles() || deletesDeleteFiles()` ⇒ the files-to-delete set must be non-empty
//!      (**"Files to delete cannot be empty"**). A delete-only rewrite (delete files, add nothing) is
//!      legal; an add-only or fully-empty rewrite is rejected.
//!   2. `deletesDataFiles() || !addsDataFiles()` ⇒ data files may be added only if data files are being
//!      deleted (**"Data files to add must be empty because there's no data file to be rewritten"**).
//!      For the data-only case this is subsumed by (1), but the distinct message is mirrored.
//! - added files are validated as usual (data content type + partition-spec match + partition-value
//!   compatibility, via [`SnapshotProducer::validate_added_data_files`]).
//!
//! **Summary:** the snapshot summary carries the added + deleted file & record counts (the producer
//! already merges the added-files and removed-files summaries) under `Operation::Replace`.
//!
//! **Provenance:** surviving (non-rewritten) entries are copied forward as `Existing` keeping their
//! original snapshot id + both sequence numbers; this is inherited from the Increment-1 rewrite machinery.
//!
//! **Out of scope (deferred, with precise reasons):**
//! - **DELETE-file rewrite** — Java `RewriteFiles` can also rewrite position-delete / deletion-vector
//!   files (`deleteFile(DeleteFile)` / `addFile(DeleteFile)`). That needs the delete-file write path (a
//!   later Phase-2 increment). This action rewrites DATA files only.
//! - **`dataSequenceNumber` preservation** — Java `RewriteFiles.dataSequenceNumber(seq)` /
//!   `setNewDataFilesDataSequenceNumber` lets a compaction stamp the added data files with the MAX data
//!   sequence number of the files they replace, so any outstanding merge-on-read DELETE files still apply
//!   to the rewritten data. Here the added files get a FRESH (higher) sequence number via the standard add
//!   path, which is correct ONLY for a PURE data rewrite of a table with no outstanding row-level deletes.
//!   On a table that DOES carry outstanding delete files, a fresh higher seq would make those deletes stop
//!   applying to the rewritten data and silently RESURRECT deleted rows (data corruption). Because this is
//!   not yet implemented, [`RewriteFilesAction::commit`] enforces a HARD PRECONDITION: it REJECTS a rewrite
//!   when the current snapshot references any delete manifest (see the SAFETY GUARD in `commit`). The
//!   `dataSequenceNumber`-preservation work that lifts this restriction is a tracked compaction-correctness
//!   follow-up (it pairs with the delete-file write path above).
//! - **conflict validation** — `validateFromSnapshot` / `validateNoNewDeletesForDataFiles` /
//!   concurrent-commit conflict detection (serializable isolation) — needs ancestor-chain validation-history
//!   replay. The optimistic-concurrency `RefSnapshotIdMatch` guard the producer already emits still protects
//!   against a stale-base commit.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::{Error, ErrorKind, Result};
use crate::spec::{DataFile, ManifestContentType, ManifestEntry, ManifestFile, Operation};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};

/// A transaction action that rewrites data files: it atomically removes a set of data files and adds a
/// new set of data files in a single `Replace` snapshot — the compaction-commit primitive.
///
/// Use [`crate::transaction::Transaction::rewrite_files`] to create one. The primary entry point is
/// [`RewriteFilesAction::rewrite_files`] (the files to delete + the files to add); the
/// [`RewriteFilesAction::delete_file`] / [`RewriteFilesAction::add_file`] (and `*_files`) builders are the
/// incremental equivalents. The files to delete are passed as [`DataFile`]s (callers hold them after a
/// scan) and resolved against the current snapshot BY PATH.
///
/// The set of files to delete MUST be non-empty (Java `BaseRewriteFiles` "Files to delete cannot be
/// empty"); a delete-only rewrite is allowed, but an add-only rewrite is rejected. Deleting a file that is
/// not present in the current snapshot errors (Java `failMissingDeletePaths`).
///
/// The table must NOT carry outstanding row-level (merge-on-read) delete files: until `dataSequenceNumber`
/// preservation lands, a rewrite on such a table would resurrect deleted rows, so the commit is rejected
/// (see the SAFETY GUARD in [`RewriteFilesAction::commit`]).
pub struct RewriteFilesAction {
    /// Data files to add to the table (validated like fast append).
    added_data_files: Vec<DataFile>,
    /// Data files to remove from the table (their paths are resolved against the current snapshot).
    deleted_data_files: Vec<DataFile>,
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
}

impl RewriteFilesAction {
    pub(crate) fn new() -> Self {
        Self {
            added_data_files: vec![],
            deleted_data_files: vec![],
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
        }
    }

    /// Rewrite `files_to_delete` into `files_to_add` (Java `RewriteFiles.rewriteFiles(filesToDelete,
    /// filesToAdd)`) — the primary entry point. Equivalent to calling [`RewriteFilesAction::delete_file`]
    /// for each file to delete and [`RewriteFilesAction::add_file`] for each file to add.
    pub fn rewrite_files(
        mut self,
        files_to_delete: impl IntoIterator<Item = DataFile>,
        files_to_add: impl IntoIterator<Item = DataFile>,
    ) -> Self {
        self.deleted_data_files.extend(files_to_delete);
        self.added_data_files.extend(files_to_add);
        self
    }

    /// Add a single rewritten [`DataFile`] to the table (Java `RewriteFiles.addFile`).
    pub fn add_file(mut self, data_file: DataFile) -> Self {
        self.added_data_files.push(data_file);
        self
    }

    /// Add multiple rewritten [`DataFile`]s to the table.
    pub fn add_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(data_files);
        self
    }

    /// Remove a single rewritten [`DataFile`] from the table (Java `RewriteFiles.deleteFile`). Its path
    /// must equal a live file path in the current snapshot, or the commit errors.
    pub fn delete_file(mut self, data_file: DataFile) -> Self {
        self.deleted_data_files.push(data_file);
        self
    }

    /// Remove multiple rewritten [`DataFile`]s from the table.
    pub fn delete_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.deleted_data_files.extend(data_files);
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

    /// The set of paths to delete, derived from the to-delete [`DataFile`]s.
    fn delete_paths(&self) -> HashSet<String> {
        self.deleted_data_files
            .iter()
            .map(|file| file.file_path.clone())
            .collect()
    }
}

#[async_trait]
impl TransactionAction for RewriteFilesAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        // Java `BaseRewriteFiles.validateReplacedAndAddedFiles()` (the data-file subset). Run BEFORE the
        // producer's own machinery so the exact Java message surfaces.
        //
        // Precondition (1): `deletesDataFiles() || deletesDeleteFiles()` — the files-to-delete set must be
        // non-empty ("Files to delete cannot be empty"). A delete-only rewrite is legal; an add-only or
        // fully-empty rewrite is rejected.
        //
        // Java precondition (2) `deletesDataFiles() || !addsDataFiles()` ("Data files to add must be empty
        // because there's no data file to be rewritten") is SUBSUMED here: with DELETE-file rewrite out of
        // scope, the only way to delete files is to delete DATA files, so `deletesDataFiles()` is true
        // exactly when this delete set is non-empty — i.e. whenever (1) passes — and an add is therefore
        // always permitted. (When the delete-file write path lands, a delete-file-only rewrite that tries
        // to add data files will need precondition (2) enforced explicitly.)
        if self.deleted_data_files.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Files to delete cannot be empty",
            ));
        }

        // SAFETY GUARD — outstanding merge-on-read deletes (data-loss prevention, not a Java mirror).
        //
        // Rewriting a data file gives the added file a FRESH (higher) data sequence number via the
        // standard add path. Merge-on-read DELETE files (position / equality deletes, deletion vectors)
        // apply only to data files whose `data_sequence_number <= delete_sequence_number`. So compacting
        // a data file that an outstanding delete targets into a new file with a HIGHER sequence number
        // makes the old delete NO LONGER apply — the deleted rows silently RESURRECT (data corruption).
        //
        // Java avoids this with `RewriteFiles.dataSequenceNumber` / `setNewDataFilesDataSequenceNumber`
        // (carry the replaced files' max data-seq onto the added files), which this action does not yet
        // implement (tracked, see the module docs). Until it does, refuse to rewrite a table that has any
        // outstanding delete files rather than corrupt it. This library cannot itself yet WRITE delete
        // files (no `RowDelta`), but it can READ and operate on a Java-written table that has them — that
        // is exactly the case this guard makes impossible. Remove the guard when `dataSequenceNumber`
        // preservation lands.
        if has_outstanding_delete_files(table).await? {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Cannot rewrite data files on a table with outstanding row-level (merge-on-read) delete \
                 files: the rewritten data would receive a fresh, higher data sequence number and the \
                 existing deletes would no longer apply, resurrecting deleted rows. dataSequenceNumber \
                 preservation is not yet implemented.",
            ));
        }

        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
        );

        // Validate the added files like fast append: data content type, partition-spec match, and
        // partition-value compatibility (Java `MergingSnapshotProducer.add`). The delete paths are resolved
        // and validated (every one present; an absent path errors) inside the producer's commit via the
        // operation's `delete_files` seam (Java `failMissingDeletePaths`).
        snapshot_producer.validate_added_data_files()?;

        snapshot_producer
            .commit(
                RewriteFilesOperation {
                    delete_paths: self.delete_paths(),
                },
                DefaultManifestProcess,
            )
            .await
    }
}

/// Returns `true` if the table's current snapshot references any delete manifest (a manifest with
/// [`ManifestContentType::Deletes`], i.e. outstanding position / equality deletes or deletion vectors).
///
/// Used by [`RewriteFilesAction::commit`] to refuse a data-file rewrite that would resurrect deleted rows
/// (see the SAFETY GUARD comment there). Returns `false` when the table has no current snapshot.
async fn has_outstanding_delete_files(table: &Table) -> Result<bool> {
    let Some(snapshot) = table.metadata().current_snapshot() else {
        return Ok(false);
    };
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), &table.metadata_ref())
        .await?;
    Ok(manifest_list
        .entries()
        .iter()
        .any(|entry| entry.content == ManifestContentType::Deletes))
}

/// The [`SnapshotProduceOperation`] for [`RewriteFilesAction`].
///
/// Records [`Operation::Replace`] (Java `BaseRewriteFiles.operation()` = `DataOperations.REPLACE`), exposes
/// every current data manifest as the set to filter, and resolves the requested delete paths against the
/// current snapshot's live data entries (the resolved [`DataFile`]s drive the producer's by-path manifest
/// rewrite). The added files reach the producer separately (passed to `SnapshotProducer::new`), so a single
/// snapshot carries both the added manifest and the rewritten (filtered) manifests.
struct RewriteFilesOperation {
    delete_paths: HashSet<String>,
}

impl SnapshotProduceOperation for RewriteFilesOperation {
    fn operation(&self) -> Operation {
        // Java `BaseRewriteFiles.operation()` returns `DataOperations.REPLACE`.
        Operation::Replace
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
        // `DeleteFiles` / `OverwriteFiles` via `SnapshotProducer::resolve_delete_paths`.
        snapshot_produce
            .resolve_delete_paths(&self.delete_paths)
            .await
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        // Expose every current data manifest; the producer's `process_deletes` decides per manifest
        // whether to rewrite (to drop the rewritten files), carry forward unchanged, or drop it.
        snapshot_produce.current_data_manifests().await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, MAIN_BRANCH,
        ManifestEntry, ManifestListWriter, ManifestStatus, ManifestWriterBuilder, Operation,
        Snapshot, SnapshotReference, SnapshotRetention, Struct, Summary,
    };
    use crate::table::Table;
    use crate::transaction::tests::make_v3_minimal_table_in_catalog;
    use crate::transaction::{ApplyTransactionAction, Transaction};
    use crate::{Catalog, ErrorKind, TableCommit, TableUpdate};

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

    /// Collect the set of live (Added or Existing) data file paths across the table's current snapshot —
    /// the real correctness signal (what a scan would read).
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

    /// Assert that `path` appears as a `Deleted` tombstone in the table's current snapshot.
    async fn assert_deleted_tombstone(table: &Table, path: &str) {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                if entry.file_path() == path && entry.status() == ManifestStatus::Deleted {
                    return;
                }
            }
        }
        panic!("{path} must appear as a Deleted tombstone");
    }

    /// Read a u64 total from a snapshot summary property, defaulting to 0 when absent.
    fn summary_prop(table: &Table, prop: &str) -> Option<String> {
        table
            .metadata()
            .current_snapshot()
            .unwrap()
            .summary()
            .additional_properties
            .get(prop)
            .cloned()
    }

    /// THE KEY TEST (risk: wrong live set = silent data loss / corruption). Append A, B, C; then
    /// `rewrite_files(delete=[A, B], add=[D])` → the post-commit SCAN live set is exactly {C, D}, the
    /// snapshot operation is Replace, A & B are Deleted tombstones, and C (untouched) keeps its provenance.
    /// A wrong live set (lost C, kept A/B, or missing D) is silent data corruption.
    #[tokio::test]
    async fn test_rewrite_delete_two_add_one_yields_correct_live_scan_set() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let a = data_file("test/a.parquet", 0);
        let b = data_file("test/b.parquet", 0);
        let c = data_file("test/c.parquet", 0);
        // Fast-append A, B, C in one commit (one manifest containing all three).
        let table = append_files(&catalog, &table, vec![a.clone(), b.clone(), c.clone()]).await;
        let s_append = table.metadata().current_snapshot().unwrap().snapshot_id();
        let (_, c_seq, c_fseq) = entry_provenance(&table, "test/c.parquet").await;

        // Rewrite: replace A, B with D — in one Replace snapshot.
        let tx = Transaction::new(&table);
        let action = tx.rewrite_files(vec![a, b], vec![data_file("test/d.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // The new snapshot is a Replace, and the live scan set is exactly {C, D}.
        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Replace
        );
        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/c.parquet".to_string(), "test/d.parquet".to_string()])
        );

        // A and B are present as Deleted tombstones (the rewritten manifest).
        assert_deleted_tombstone(&table, "test/a.parquet").await;
        assert_deleted_tombstone(&table, "test/b.parquet").await;

        // C (untouched) keeps its ORIGINAL provenance — it was NOT re-stamped with the rewrite snapshot.
        let (c_snap2, c_seq2, c_fseq2) = entry_provenance(&table, "test/c.parquet").await;
        assert_eq!(
            c_snap2,
            Some(s_append),
            "surviving C keeps its original snapshot id, not the rewrite snapshot"
        );
        assert_eq!(c_seq2, c_seq, "surviving C keeps its original data seq");
        assert_eq!(c_fseq2, c_fseq, "surviving C keeps its original file seq");
    }

    /// Risk: corruption across manifests — a rewrite must correctly drop files that live in DIFFERENT
    /// source manifests and preserve the rest. Append A (snapshot 1), B+C (snapshot 2): A lives in its own
    /// manifest, B+C in another. Rewrite delete=[A, B] add=[D] → live {C, D}: the resolver must reach both
    /// manifests, drop A from one and B from the other, keep C, and add D.
    #[tokio::test]
    async fn test_rewrite_across_multiple_manifests() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let a = data_file("test/a.parquet", 0);
        let table = append_files(&catalog, &table, vec![a.clone()]).await;
        let b = data_file("test/b.parquet", 0);
        let c = data_file("test/c.parquet", 0);
        let table = append_files(&catalog, &table, vec![b.clone(), c.clone()]).await;

        let tx = Transaction::new(&table);
        let action = tx.rewrite_files(vec![a, b], vec![data_file("test/d.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Replace
        );
        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/c.parquet".to_string(), "test/d.parquet".to_string()])
        );
        assert_deleted_tombstone(&table, "test/a.parquet").await;
        assert_deleted_tombstone(&table, "test/b.parquet").await;
    }

    /// Risk: the canonical compaction shape — many small files into one — must keep the exact live row set.
    /// Append A, B, C (3 files); rewrite delete=[A, B, C] add=[big] → live set is exactly {big} (3 → 1). A
    /// rewrite that dropped a file it should keep, or kept a compacted-away file, is data corruption.
    #[tokio::test]
    async fn test_rewrite_compaction_to_fewer_files() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let a = data_file("test/a.parquet", 0);
        let b = data_file("test/b.parquet", 0);
        let c = data_file("test/c.parquet", 0);
        let table = append_files(&catalog, &table, vec![a.clone(), b.clone(), c.clone()]).await;

        let tx = Transaction::new(&table);
        let action = tx.rewrite_files(vec![a, b, c], vec![data_file("test/big.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/big.parquet".to_string()]),
            "compaction must replace the 3 small files with the single big file"
        );
        assert_deleted_tombstone(&table, "test/a.parquet").await;
        assert_deleted_tombstone(&table, "test/b.parquet").await;
        assert_deleted_tombstone(&table, "test/c.parquet").await;
    }

    /// Risk: silently committing a partial rewrite. Deleting a file that is NOT in the current snapshot must
    /// error (Java `failMissingDeletePaths`) and NOT add the added file. A silent drop of the unmatched
    /// delete commits a rewrite that lost the intended removal but kept the add — a corrupt live set.
    #[tokio::test]
    async fn test_rewrite_delete_absent_file_errors() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;

        let tx = Transaction::new(&table);
        let action = tx.rewrite_files(vec![data_file("test/does-not-exist.parquet", 0)], vec![
            data_file("test/b.parquet", 0),
        ]);
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

        // The table is unchanged — the failed rewrite did not add b.parquet.
        let reloaded = catalog.load_table(table.identifier()).await.unwrap();
        assert_eq!(
            live_file_paths(&reloaded).await,
            HashSet::from(["test/a.parquet".to_string()])
        );
    }

    /// Risk: an empty rewrite producing a no-op Replace snapshot. Java `validateReplacedAndAddedFiles`
    /// requires the files-to-delete set to be non-empty ("Files to delete cannot be empty"). A rewrite with
    /// nothing to delete (and nothing to add) must be rejected — it is not a valid compaction.
    #[tokio::test]
    async fn test_rewrite_empty_delete_set_rejected() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;

        let tx = Transaction::new(&table);
        let action = tx.rewrite_files(vec![], vec![]);
        let tx = action.apply(tx).unwrap();
        let error = tx
            .commit(&catalog)
            .await
            .expect_err("an empty-delete rewrite must be rejected");
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert!(
            error.message().contains("Files to delete cannot be empty"),
            "unexpected error message: {}",
            error.message()
        );
    }

    /// Risk: an add-only "rewrite" (add files, delete nothing) silently behaving like an append and
    /// corrupting the live set (a rewrite must REPLACE, not add). Java rejects it with "Files to delete
    /// cannot be empty" (precondition (1) fires first). Pins that an add-only rewrite is rejected.
    #[tokio::test]
    async fn test_rewrite_add_without_delete_rejected() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;

        let tx = Transaction::new(&table);
        let action = tx.rewrite_files(vec![], vec![data_file("test/b.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let error = tx
            .commit(&catalog)
            .await
            .expect_err("an add-only rewrite must be rejected");
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert!(
            error.message().contains("Files to delete cannot be empty"),
            "unexpected error message: {}",
            error.message()
        );

        // The table is unchanged — the rejected rewrite did not add b.parquet.
        let reloaded = catalog.load_table(table.identifier()).await.unwrap();
        assert_eq!(
            live_file_paths(&reloaded).await,
            HashSet::from(["test/a.parquet".to_string()])
        );
    }

    /// Risk: a delete-only rewrite (delete files, add nothing) being wrongly rejected. Java
    /// `validateReplacedAndAddedFiles` requires only the DELETE set to be non-empty, so a rewrite that drops
    /// files and adds none is LEGAL (e.g. a compaction that discarded fully-deleted data). Pins live {B}
    /// after rewriting away A, recorded as Replace.
    #[tokio::test]
    async fn test_rewrite_delete_only_is_allowed() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let a = data_file("test/a.parquet", 0);
        let b = data_file("test/b.parquet", 0);
        let table = append_files(&catalog, &table, vec![a.clone(), b]).await;

        let tx = Transaction::new(&table);
        let action = tx.rewrite_files(vec![a], vec![]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Replace,
            "a delete-only rewrite still records Replace"
        );
        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/b.parquet".to_string()])
        );
        assert_deleted_tombstone(&table, "test/a.parquet").await;
    }

    /// Risk: the rewrite summary under-reporting the change. Java `MergingSnapshotProducer.apply` merges the
    /// added-files summary AND the removed-files summary, so a rewrite reports BOTH `added-*` and
    /// `deleted-*`. Append A, B; rewrite delete=[A, B] add=[D] → summary: added 1 file/1 record, deleted
    /// 2 files/2 records.
    #[tokio::test]
    async fn test_rewrite_summary_reflects_added_and_deleted_counts() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let a = data_file("test/a.parquet", 0);
        let b = data_file("test/b.parquet", 0);
        let table = append_files(&catalog, &table, vec![a.clone(), b.clone()]).await;

        let tx = Transaction::new(&table);
        let action = tx.rewrite_files(vec![a, b], vec![data_file("test/d.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(
            summary_prop(&table, "added-data-files").as_deref(),
            Some("1"),
            "summary must report one added data file"
        );
        assert_eq!(
            summary_prop(&table, "added-records").as_deref(),
            Some("1"),
            "summary must report one added record"
        );
        assert_eq!(
            summary_prop(&table, "deleted-data-files").as_deref(),
            Some("2"),
            "summary must report two deleted data files"
        );
        assert_eq!(
            summary_prop(&table, "deleted-records").as_deref(),
            Some("2"),
            "summary must report two deleted records"
        );
    }

    /// Risk: the #1 corruption class — re-stamping a SURVIVING entry with the rewrite snapshot's id/seq
    /// instead of preserving its original (breaks merge-on-read delete application + incremental scans).
    /// Append A (snapshot S1); append B + C in one commit (snapshot S2); rewrite delete=[B] add=[D]. The
    /// surviving C must keep S2 + its original seqs (NOT S3), the carried-forward A keeps S1, the added D
    /// gets S3, and B's Deleted tombstone gets S3 but keeps B's original seqs.
    #[tokio::test]
    async fn test_rewrite_preserves_surviving_entry_provenance_across_snapshots() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Append A in its OWN commit (snapshot S1).
        let a = data_file("test/a.parquet", 0);
        let table = append_files(&catalog, &table, vec![a.clone()]).await;
        let s1 = table.metadata().current_snapshot().unwrap().snapshot_id();

        // Append B and C in ONE commit (snapshot S2; one manifest with both).
        let b = data_file("test/b.parquet", 0);
        let c = data_file("test/c.parquet", 0);
        let table = append_files(&catalog, &table, vec![b.clone(), c]).await;
        let s2 = table.metadata().current_snapshot().unwrap().snapshot_id();
        assert_ne!(s1, s2);

        let (a_snap, a_seq, a_fseq) = entry_provenance(&table, "test/a.parquet").await;
        let (b_snap, b_seq, b_fseq) = entry_provenance(&table, "test/b.parquet").await;
        assert_eq!(a_snap, Some(s1), "A added by S1");
        assert_eq!(b_snap, Some(s2), "B added by S2");
        assert_ne!(a_seq, b_seq, "A and B must have different data seq numbers");

        // Rewrite: delete B + add D → rewrites S2's manifest (C survives) and adds a new manifest (D).
        let tx = Transaction::new(&table);
        let action = tx.rewrite_files(vec![b], vec![data_file("test/d.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let s3 = table.metadata().current_snapshot().unwrap().snapshot_id();
        assert_ne!(s3, s2);

        // C survived: rewritten as Existing, MUST keep S2's snapshot id + seq numbers (NOT S3).
        let (c_snap, c_seq, c_fseq) = entry_provenance(&table, "test/c.parquet").await;
        assert_eq!(
            c_snap,
            Some(s2),
            "surviving C must keep its ORIGINAL snapshot id S2, not the rewrite snapshot S3"
        );
        assert_eq!(
            c_seq, b_seq,
            "surviving C must keep its ORIGINAL data seq, not the rewrite seq"
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

        // The added file D gets the NEW rewrite snapshot's provenance (S3 + a new seq).
        let (d_snap, d_seq, _d_fseq) = entry_provenance(&table, "test/d.parquet").await;
        assert_eq!(d_snap, Some(s3), "added D gets the new rewrite snapshot id");
        assert_ne!(
            d_seq, b_seq,
            "added D gets the new (higher) data seq, not the rewritten file's seq"
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

    /// Risk: a rewrite using the `delete_file`/`add_file` incremental builders behaving differently from
    /// the `rewrite_files` primary entry. Pins that the builder methods produce the same {C, D} live set.
    #[tokio::test]
    async fn test_rewrite_via_incremental_builders() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let a = data_file("test/a.parquet", 0);
        let b = data_file("test/b.parquet", 0);
        let c = data_file("test/c.parquet", 0);
        let table = append_files(&catalog, &table, vec![a.clone(), b.clone(), c]).await;

        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files(vec![], vec![])
            .delete_file(a)
            .delete_file(b)
            .add_file(data_file("test/d.parquet", 0));
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Replace
        );
        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/c.parquet".to_string(), "test/d.parquet".to_string()])
        );
    }

    /// Build a position-delete [`DataFile`] (merge-on-read delete) referencing `target`.
    fn position_delete_file(path: &str, target: &str) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(50)
            .record_count(1)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(0))]))
            .referenced_data_file(Some(target.to_string()))
            .build()
            .unwrap()
    }

    /// Commit a new snapshot whose manifest list references a real DELETE manifest (a position-delete
    /// file), simulating a table that carries outstanding merge-on-read deletes — the shape a Java
    /// `RowDelta` produces, which this library can READ but cannot yet write. Returns the updated table.
    ///
    /// This drives the production manifest / manifest-list writers directly (there is no public action
    /// that writes delete files yet), then commits the snapshot through the catalog so the table's current
    /// snapshot genuinely has a `Deletes`-content manifest in its manifest list.
    async fn commit_snapshot_with_delete_manifest(
        catalog: &impl Catalog,
        table: &Table,
        delete_target: &str,
    ) -> Table {
        let metadata = table.metadata();
        let parent = metadata.current_snapshot();
        let snapshot_id = 999_000 + metadata.snapshots().count() as i64;
        let sequence_number = metadata.next_sequence_number();

        // Write a DELETE manifest containing one position-delete file.
        let delete_manifest_path = format!("{}/metadata/test-delete-m0.avro", metadata.location());
        let output = table.file_io().new_output(&delete_manifest_path).unwrap();
        let mut writer = ManifestWriterBuilder::new(
            output,
            Some(snapshot_id),
            None,
            metadata.current_schema().clone(),
            metadata.default_partition_spec().as_ref().clone(),
        )
        .build_v3_deletes();
        writer
            .add_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(position_delete_file("test/del-0.parquet", delete_target))
                    .build(),
            )
            .unwrap();
        let delete_manifest = writer.write_manifest_file().await.unwrap();

        // Carry forward the parent's existing (data) manifests, then add the delete manifest.
        let mut manifests = if let Some(parent) = parent {
            parent
                .load_manifest_list(table.file_io(), metadata)
                .await
                .unwrap()
                .entries()
                .to_vec()
        } else {
            vec![]
        };
        manifests.push(delete_manifest);

        let manifest_list_path = format!(
            "{}/metadata/snap-{}-test.avro",
            metadata.location(),
            snapshot_id
        );
        let mut list_writer = ManifestListWriter::v3(
            table.file_io().new_output(&manifest_list_path).unwrap(),
            snapshot_id,
            metadata.current_snapshot_id(),
            sequence_number,
            Some(metadata.next_row_id()),
        );
        list_writer.add_manifests(manifests.into_iter()).unwrap();
        list_writer.close().await.unwrap();

        let new_snapshot = Snapshot::builder()
            .with_manifest_list(manifest_list_path)
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(metadata.current_snapshot_id())
            .with_sequence_number(sequence_number)
            .with_summary(Summary {
                operation: Operation::Overwrite,
                additional_properties: HashMap::new(),
            })
            .with_schema_id(metadata.current_schema_id())
            .with_timestamp_ms(chrono::Utc::now().timestamp_millis())
            .with_row_range(metadata.next_row_id(), 0)
            .build();

        let commit = TableCommit::builder()
            .ident(table.identifier().clone())
            .requirements(vec![])
            .updates(vec![
                TableUpdate::AddSnapshot {
                    snapshot: new_snapshot,
                },
                TableUpdate::SetSnapshotRef {
                    ref_name: MAIN_BRANCH.to_string(),
                    reference: SnapshotReference::new(
                        snapshot_id,
                        SnapshotRetention::branch(None, None, None),
                    ),
                },
            ])
            .build();
        catalog.update_table(commit).await.unwrap()
    }

    /// THE DATA-LOSS GUARD (risk: silent row resurrection = data corruption). A data-file rewrite gives the
    /// added file a FRESH, higher data sequence number; merge-on-read deletes only apply to data files with
    /// `data_seq <= delete_seq`. Rewriting a deleted-from data file into a higher-seq file would make the
    /// outstanding deletes stop applying — resurrecting deleted rows. Until `dataSequenceNumber` preservation
    /// lands, a rewrite on a table that has ANY outstanding delete manifest must be REJECTED (not silently
    /// committed). Pins that the guard fires and the table is left unchanged.
    #[tokio::test]
    async fn test_rewrite_rejected_when_table_has_outstanding_delete_files() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let a = data_file("test/a.parquet", 0);
        let table = append_files(&catalog, &table, vec![a.clone()]).await;

        // Add a snapshot carrying a position-delete file targeting a.parquet (a merge-on-read delete).
        let table = commit_snapshot_with_delete_manifest(&catalog, &table, "test/a.parquet").await;

        // Rewriting a.parquet now would resurrect the rows the delete file removed — must be rejected.
        let tx = Transaction::new(&table);
        let action = tx.rewrite_files(vec![a], vec![data_file("test/compacted.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let error = tx
            .commit(&catalog)
            .await
            .expect_err("a rewrite on a table with outstanding deletes must be rejected");
        assert_eq!(error.kind(), ErrorKind::FeatureUnsupported);
        assert!(
            error.message().contains("outstanding row-level"),
            "unexpected error message: {}",
            error.message()
        );

        // The table is unchanged — the rejected rewrite did not add the compacted file.
        let reloaded = catalog.load_table(table.identifier()).await.unwrap();
        assert!(
            live_file_paths(&reloaded).await.contains("test/a.parquet"),
            "a.parquet must still be live after the rejected rewrite"
        );
        assert!(
            !live_file_paths(&reloaded)
                .await
                .contains("test/compacted.parquet"),
            "the rejected rewrite must not have added the compacted file"
        );
    }
}
