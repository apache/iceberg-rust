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

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use futures::future::try_join_all;
use tracing::warn;
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{DataFile, ManifestEntry, ManifestFile, ManifestStatus, Operation};
use crate::table::Table;
use crate::transaction::merging_state::MergingState;
use crate::transaction::snapshot::{ManifestProcess, SnapshotProduceOperation, SnapshotProducer};
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind};

/// Action to rewrite data files in a table — primarily compaction.
///
/// On every commit, the action runs Java's `MergingSnapshotProducer` filter+merge
/// pipeline:
///
///   1. **Filter pass.** Each manifest in the current snapshot's manifest list is
///      inspected. Manifests with no replaced entries pass through unchanged;
///      manifests whose alive entries are all replaced are dropped; manifests
///      with both replaced and surviving entries are rewritten as residuals
///      containing only the survivors with their original sequence numbers
///      preserved.
///   2. **Merge pass.** The result of the filter pass plus the new added/delete
///      manifests are bin-packed by partition spec id and target manifest size
///      (default 8 MB). Bins with multiple sibling manifests are consolidated
///      into a single merged manifest, suppressing prior-snapshot tombstones in
///      the process. The bin containing the action's "first" manifest is
///      exempted when fewer than `commit.manifest.min-count-to-merge` (default
///      100) siblings are present, so a cluster of small commits doesn't trigger
///      a giant historical rewrite.
///
/// Java analog: `org.apache.iceberg.BaseRewriteFiles` extending
/// `MergingSnapshotProducer`.
pub struct RewriteFilesAction {
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    files_to_delete: Vec<DataFile>,
    files_to_add: Vec<DataFile>,
    /// When set, validate `files_to_delete` against this snapshot instead of the current one.
    /// Useful for concurrent compaction: validate against the planning snapshot to avoid false
    /// rejections if another writer commits between planning and committing.
    validate_from_snapshot_id: Option<i64>,
    /// When set, all added (compacted) manifest entries will carry this explicit data sequence
    /// number instead of inheriting from the new snapshot. Required for v2 equality-delete
    /// correctness: compacted files that predate an equality delete must retain the original
    /// sequence number so the delete continues to apply to them.
    data_sequence_number: Option<i64>,
    /// Lazy-initialized on the first commit attempt and shared across retries so
    /// the filter+merge caches survive the catalog round-trip — exactly Java's
    /// idempotency contract (brainstorm §5.5, §6.5). The state holds the three
    /// merge table-property values read at first construction.
    merging_state: OnceLock<Arc<MergingState>>,
}

impl RewriteFilesAction {
    pub(crate) fn new() -> Self {
        Self {
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
            files_to_delete: vec![],
            files_to_add: vec![],
            validate_from_snapshot_id: None,
            data_sequence_number: None,
            merging_state: OnceLock::new(),
        }
    }

    fn merging_state(&self, table: &Table) -> Arc<MergingState> {
        self.merging_state
            .get_or_init(|| Arc::new(MergingState::from_table(table)))
            .clone()
    }

    /// Add files to delete (old files being replaced).
    ///
    /// When a manifest contains both files being deleted and files not being deleted, this action
    /// writes a new "residual" manifest containing only the surviving entries, preserving their
    /// original sequence numbers.
    ///
    /// **Path uniqueness constraint**: Paths in `files_to_delete` cannot appear in the `add_files`
    /// list. The duplicate-file check (`validate_duplicate_files`) compares against all alive
    /// snapshot entries, including those being deleted in this same operation.
    pub fn delete_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.files_to_delete.extend(files);
        self
    }

    /// Add files to add (new files replacing old ones).
    pub fn add_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.files_to_add.extend(files);
        self
    }

    /// Set commit UUID.
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
    pub fn set_snapshot_properties(mut self, props: HashMap<String, String>) -> Self {
        self.snapshot_properties = props;
        self
    }

    /// Validate files to delete against a specific historical snapshot rather than the current one.
    ///
    /// In concurrent compaction workflows, a job reads the table at snapshot S, plans which files
    /// to compact, then commits. If another writer creates snapshot S+1 between planning and
    /// committing, validating against S+1 may reject files legitimately planned from S. Setting
    /// this to the planning snapshot avoids false rejections.
    pub fn validate_from_snapshot(mut self, snapshot_id: i64) -> Self {
        self.validate_from_snapshot_id = Some(snapshot_id);
        self
    }

    /// Set an explicit data sequence number on all added (compacted) manifest entries.
    ///
    /// When compacting files written before an equality delete was applied, the compacted output
    /// must carry the *original* sequence number so that equality deletes with a higher sequence
    /// number continue to apply to them. Without this, a compacted file would inherit the new
    /// snapshot's sequence number and any equality delete with a lower sequence number would stop
    /// applying, causing previously deleted rows to reappear.
    ///
    /// Only relevant for Iceberg v2 tables with equality deletes.
    pub fn data_sequence_number(mut self, seq_num: i64) -> Self {
        self.data_sequence_number = Some(seq_num);
        self
    }
}

#[async_trait]
impl TransactionAction for RewriteFilesAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        if self.files_to_add.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Rewrite operation requires files to add",
            ));
        }

        // Validate no duplicate paths in files_to_delete — duplicates would
        // inflate summary statistics (remove_file called twice for the same file).
        {
            let mut seen = HashSet::new();
            for f in &self.files_to_delete {
                if !seen.insert(f.file_path.as_str()) {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Duplicate path in files_to_delete: {}", f.file_path),
                    ));
                }
            }
        }

        // NOTE: `commit` takes `Arc<Self>` and cannot move out of fields, so
        // `files_to_add` and `files_to_delete` must be cloned here. For large
        // compaction jobs with many DataFiles this is non-trivial overhead.
        // Tracked as a known Phase 1 limitation; revisit if memory becomes an issue.
        let commit_uuid = self.commit_uuid.unwrap_or_else(Uuid::now_v7);
        let mut snapshot_producer = SnapshotProducer::new(
            table,
            commit_uuid,
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            self.files_to_add.clone(),
        )
        .with_removed_data_files(self.files_to_delete.clone());

        if let Some(seq_num) = self.data_sequence_number {
            snapshot_producer = snapshot_producer.with_data_sequence_number(seq_num);
        }

        snapshot_producer.validate_added_data_files()?;
        snapshot_producer.validate_duplicate_files().await?;

        // Build the replaced-paths set for both the merging-state delete bookkeeping
        // and the validate_no_new_deletes_for_data_files check. Compute once.
        let replaced_paths: HashSet<String> = self
            .files_to_delete
            .iter()
            .map(|f| f.file_path.clone())
            .collect();

        if !replaced_paths.is_empty() {
            snapshot_producer
                .validate_no_new_deletes_for_data_files(
                    self.validate_from_snapshot_id,
                    &replaced_paths,
                )
                .await?;
        }

        let state = self.merging_state(table);
        // Mark each replaced path on the filter manager. Idempotent: re-marking on
        // a retry is a no-op.
        for f in &self.files_to_delete {
            state.delete(f);
        }

        let rewrite_op = RewriteOperation {
            files_to_delete: replaced_paths,
            validate_from_snapshot_id: self.validate_from_snapshot_id,
            state: state.clone(),
        };
        let process = RewriteManifestProcess {
            state: state.clone(),
        };
        let file_io = table.file_io().clone();

        match snapshot_producer.commit(rewrite_op, process).await {
            Ok(commit) => Ok(commit),
            Err(err) => {
                // Best-effort cleanup of orphan residuals + merged manifests this
                // attempt wrote but never committed. The merging state's caches
                // persist into the next retry, so paths returned to disk on a
                // successful retry are not deleted here. Cleanup IO failures are
                // logged inside clean_uncommitted; the orphan-file sweeper picks
                // up anything missed.
                state
                    .clean_uncommitted(&file_io, &HashSet::new())
                    .await;
                Err(err)
            }
        }
    }
}

struct RewriteOperation {
    files_to_delete: HashSet<String>,
    validate_from_snapshot_id: Option<i64>,
    state: Arc<MergingState>,
}

struct RewriteManifestProcess {
    state: Arc<MergingState>,
}

impl ManifestProcess for RewriteManifestProcess {
    async fn process_manifests(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
        manifests: Vec<ManifestFile>,
    ) -> Result<Vec<ManifestFile>> {
        // The "first" manifest of this snapshot is at the front of `manifests`
        // (existing_manifest's filtered output is followed by the added manifest
        // and the optional delete manifest in `manifest_file()`). The merge
        // manager re-orders inputs to put the new manifest in the head bin via
        // pack_end internally, so the order at the boundary doesn't matter for
        // the first-guard.
        self.state.merge_manifests(snapshot_produce, manifests).await
    }

    fn replaced_manifests_count(&self) -> u64 {
        self.state.replaced_manifests_count()
    }
}

impl SnapshotProduceOperation for RewriteOperation {
    fn operation(&self) -> Operation {
        Operation::Replace
    }

    async fn delete_entries(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        if self.files_to_delete.is_empty() {
            return Ok(vec![]);
        }

        let (snapshot, fell_back) = if let Some(snap_id) = self.validate_from_snapshot_id {
            match snapshot_produce.table.metadata().snapshot_by_id(snap_id) {
                Some(s) => (s, false),
                None => {
                    // Planning snapshot was expired by the catalog between plan time and
                    // commit time (common during long compaction runs). Fall back to the
                    // current snapshot: the validation goal is to confirm that the files
                    // to delete are still alive, which the current snapshot satisfies.
                    //
                    // Note: we cannot distinguish between an expired snapshot (expected in
                    // long compaction runs) and a truly invalid snapshot ID (programmer
                    // error). Both fall back to the current snapshot.
                    warn!(
                        "validate_from_snapshot {} not found (likely expired by catalog); \
                         falling back to current snapshot for file-existence validation",
                        snap_id
                    );
                    let s = snapshot_produce
                        .table
                        .metadata()
                        .current_snapshot()
                        .ok_or_else(|| {
                            Error::new(
                                ErrorKind::DataInvalid,
                                "Cannot delete files from a table with no snapshots",
                            )
                        })?;
                    (s, true)
                }
            }
        } else {
            let s = snapshot_produce
                .table
                .metadata()
                .current_snapshot()
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Cannot delete files from a table with no snapshots",
                    )
                })?;
            (s, false)
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_produce.table.file_io(),
                &snapshot_produce.table.metadata_ref(),
            )
            .await?;

        // Uses validate_from_snapshot_id when set (historical planning snapshot), or
        // current_snapshot otherwise. See existing_manifest() for the two-snapshot design
        // rationale and what happens to residual manifests on commit failure.
        let manifests = try_join_all(
            manifest_list
                .entries()
                .iter()
                .map(|e| e.load_manifest(snapshot_produce.table.file_io())),
        )
        .await?;

        let mut deleted_entries = Vec::new();
        let mut found_paths = HashSet::new();
        for manifest in &manifests {
            for entry in manifest.entries() {
                if entry.is_alive() && self.files_to_delete.contains(entry.file_path()) {
                    found_paths.insert(entry.file_path().to_string());
                    let mut deleted_entry = (**entry).clone();
                    deleted_entry.status = ManifestStatus::Deleted;
                    deleted_entries.push(deleted_entry);
                }
            }
        }

        // Verify all requested delete paths were found alive in the snapshot.
        let missing: Vec<&str> = self
            .files_to_delete
            .iter()
            .map(|s| s.as_str())
            .filter(|p| !found_paths.contains(*p))
            .collect();
        if !missing.is_empty() {
            // Use a precise label so users debugging errors know which snapshot was checked.
            let snapshot_label = match (self.validate_from_snapshot_id, fell_back) {
                (Some(id), false) => format!("snapshot {id}"),
                (Some(_), true) => "current snapshot (planning snapshot expired)".to_string(),
                (None, _) => "current snapshot".to_string(),
            };
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Files in files_to_delete not found as alive entries in {snapshot_label}: {}",
                    missing.join(", ")
                ),
            ));
        }

        Ok(deleted_entries)
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        // Always reads from current_snapshot — the carry-forward must reflect current
        // table state, not the historical planning snapshot used by delete_entries().
        // This asymmetry is intentional: existing_manifest() rewrites survivors;
        // delete_entries() validates deletions.
        //
        // If the commit subsequently fails RefSnapshotIdMatch (e.g., a concurrent writer
        // changed the snapshot), any residual manifests written here become orphans.
        // The merging state's clean_uncommitted hook (run by Transaction's retry) plus
        // the orphan-file sweeper handle cleanup.
        let Some(snapshot) = snapshot_produce.table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_produce.table.file_io(),
                &snapshot_produce.table.metadata_ref(),
            )
            .await?;

        let current_manifests: Vec<ManifestFile> =
            manifest_list.entries().iter().cloned().collect();

        // The filter pass replaces the residual-write loop that lived inline here.
        // It loads each manifest with bounded concurrency, drops manifests whose
        // alive entries are all being replaced, and writes residuals for the rest.
        // Sequence numbers on surviving entries are preserved unchanged.
        self.state
            .filter_manifests(snapshot_produce, current_manifests)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, MAIN_BRANCH,
        ManifestStatus, Operation, Struct,
    };
    use crate::transaction::tests::{apply_updates_to_table, make_v2_minimal_table};
    use crate::transaction::{Transaction, TransactionAction};
    use crate::{TableRequirement, TableUpdate};

    fn make_data_file(table: &crate::table::Table, path: &str, record_count: u64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(record_count)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap()
    }

    async fn append_files(table: crate::table::Table, files: Vec<DataFile>) -> crate::table::Table {
        let tx = Transaction::new(&table);
        let append = tx.fast_append().add_data_files(files);
        let updates = Arc::new(append)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        apply_updates_to_table(&table, &updates)
    }

    fn unwrap_add_snapshot(updates: &[TableUpdate]) -> &crate::spec::Snapshot {
        if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            panic!("expected AddSnapshot update at index 0")
        }
    }

    async fn collect_alive_files(
        snapshot: &crate::spec::Snapshot,
        table: &crate::table::Table,
    ) -> Vec<String> {
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let mut alive_files: Vec<String> = Vec::new();
        for mf in manifest_list.entries() {
            let manifest = mf.load_manifest(table.file_io()).await.unwrap();
            for me in manifest.entries() {
                if me.is_alive() {
                    alive_files.push(me.file_path().to_string());
                }
            }
        }
        alive_files.sort();
        alive_files
    }

    #[tokio::test]
    async fn test_rewrite_requires_files_to_add() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx.rewrite_files();
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_rewrite_add_file_already_in_snapshot_errors() {
        // validate_duplicate_files() must reject adding a file that already exists as a live entry.
        let table = make_v2_minimal_table();

        let file1 = make_data_file(&table, "data/file1.parquet", 50);
        let tx = Transaction::new(&table);
        let append = tx.fast_append().add_data_files(vec![file1.clone()]);
        let updates = Arc::new(append)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        let table_with_file = apply_updates_to_table(&table, &updates);

        // Try to add file1 again — it's already alive in the snapshot.
        let tx = Transaction::new(&table_with_file);
        let action = tx
            .rewrite_files()
            .delete_files(vec![file1.clone()])
            .add_files(vec![file1.clone()]); // re-adding the same path
        let Err(err) = Arc::new(action).commit(&table_with_file).await else {
            panic!("expected duplicate-file error");
        };
        assert!(
            err.to_string().contains("data/file1.parquet"),
            "Expected duplicate-file error mentioning the path, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_rewrite_phantom_delete_errors() {
        // Phantom delete: file is not alive in the current snapshot.
        let table = make_v2_minimal_table();

        let file1 = make_data_file(&table, "data/file1.parquet", 50);
        let tx = Transaction::new(&table);
        let append = tx.fast_append().add_data_files(vec![file1.clone()]);
        let updates = Arc::new(append)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        let table_with_file = apply_updates_to_table(&table, &updates);

        // Try to delete a path that does not exist in the snapshot.
        let phantom = make_data_file(&table, "data/does_not_exist.parquet", 10);
        let compacted = make_data_file(&table, "data/compacted.parquet", 50);
        let tx = Transaction::new(&table_with_file);
        let action = tx
            .rewrite_files()
            .delete_files(vec![phantom])
            .add_files(vec![compacted]);
        let Err(err) = Arc::new(action).commit(&table_with_file).await else {
            panic!("expected phantom-delete error");
        };
        assert!(
            err.to_string()
                .contains("not found as alive entries in current snapshot"),
            "Expected phantom-delete error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_rewrite_duplicate_paths_in_delete_errors() {
        let table = make_v2_minimal_table();

        let file1 = make_data_file(&table, "data/file1.parquet", 50);
        let tx = Transaction::new(&table);
        let append = tx.fast_append().add_data_files(vec![file1.clone()]);
        let updates = Arc::new(append)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        let table_with_file = apply_updates_to_table(&table, &updates);

        let compacted = make_data_file(&table, "data/compacted.parquet", 50);
        let tx = Transaction::new(&table_with_file);
        let action = tx
            .rewrite_files()
            .delete_files(vec![file1.clone(), file1.clone()]) // duplicate
            .add_files(vec![compacted]);
        let Err(err) = Arc::new(action).commit(&table_with_file).await else {
            panic!("expected duplicate-path error");
        };
        assert!(
            err.to_string()
                .contains("Duplicate path in files_to_delete"),
            "Expected duplicate-path error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_rewrite_action_full_table() {
        let table = make_v2_minimal_table();

        // Step 1: fast-append two data files to create initial state.
        let file1 = make_data_file(&table, "data/file1.parquet", 50);
        let file2 = make_data_file(&table, "data/file2.parquet", 60);
        let tx = Transaction::new(&table);
        let append_action = tx
            .fast_append()
            .add_data_files(vec![file1.clone(), file2.clone()]);
        let append_updates = Arc::new(append_action)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        let table_with_files = apply_updates_to_table(&table, &append_updates);

        assert!(
            table_with_files.metadata().current_snapshot().is_some(),
            "Table should have a snapshot after append"
        );

        // Step 2: Replace both files with one compacted file.
        let compacted = make_data_file(&table, "data/compacted.parquet", 110);
        let tx = Transaction::new(&table_with_files);
        let replace_action = tx
            .rewrite_files()
            .delete_files(vec![file1.clone(), file2.clone()])
            .add_files(vec![compacted.clone()]);
        let mut replace_commit = Arc::new(replace_action)
            .commit(&table_with_files)
            .await
            .unwrap();

        let updates = replace_commit.take_updates();
        let requirements = replace_commit.take_requirements();

        // Verify updates contain AddSnapshot + SetSnapshotRef.
        assert!(
            matches!(
                (&updates[0], &updates[1]),
                (
                    TableUpdate::AddSnapshot { snapshot },
                    TableUpdate::SetSnapshotRef { ref_name, .. }
                ) if snapshot.summary().operation == Operation::Replace
                    && ref_name == MAIN_BRANCH
            ),
            "Expected AddSnapshot(Replace) + SetSnapshotRef"
        );

        // Verify requirements include UuidMatch + RefSnapshotIdMatch.
        assert!(
            requirements
                .iter()
                .any(|r| matches!(r, TableRequirement::UuidMatch { .. })),
            "Requirements should include UuidMatch"
        );
        assert!(
            requirements
                .iter()
                .any(|r| matches!(r, TableRequirement::RefSnapshotIdMatch { .. })),
            "Requirements should include RefSnapshotIdMatch"
        );

        // Load manifest list and gather files by status in a single pass.
        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            unreachable!()
        };
        let manifest_list = new_snapshot
            .load_manifest_list(table_with_files.file_io(), table_with_files.metadata())
            .await
            .unwrap();

        assert_eq!(
            manifest_list.entries().len(),
            2,
            "Expected 2 manifests: added + deleted"
        );

        let mut added_files: Vec<String> = Vec::new();
        let mut deleted_files: Vec<String> = Vec::new();
        for entry in manifest_list.entries() {
            let manifest = entry
                .load_manifest(table_with_files.file_io())
                .await
                .unwrap();
            for me in manifest.entries() {
                match me.status() {
                    ManifestStatus::Added => added_files.push(me.file_path().to_string()),
                    ManifestStatus::Deleted => deleted_files.push(me.file_path().to_string()),
                    _ => {}
                }
            }
        }

        assert_eq!(added_files, vec!["data/compacted.parquet"]);

        deleted_files.sort();
        assert_eq!(
            deleted_files,
            vec!["data/file1.parquet", "data/file2.parquet"],
            "Old files should appear as Deleted entries"
        );

        // Verify snapshot summary totals — regression for the snapshot_by_id bug
        // (using the wrong snapshot when computing prior totals in update_snapshot_summaries).
        // Previous snapshot had 2 data files; replace adds 1, deletes 2 → total = 1.
        let summary_props = &new_snapshot.summary().additional_properties;
        assert_eq!(
            summary_props.get("total-data-files").map(String::as_str),
            Some("1"),
            "total-data-files should be 1 after replacing both files"
        );
        assert_eq!(
            summary_props.get("added-data-files").map(String::as_str),
            Some("1"),
            "added-data-files should be 1"
        );
        assert_eq!(
            summary_props.get("deleted-data-files").map(String::as_str),
            Some("2"),
            "deleted-data-files should be 2"
        );
    }

    #[tokio::test]
    async fn test_rewrite_action_partial() {
        // NOTE: this test covers "partial table replace" in the sense that one manifest survives
        // (file3's manifest is untouched) while another is fully deleted (file1+file2 share a
        // manifest and both are deleted). It does NOT test the mixed-manifest case — that is
        // covered by test_rewrite_partial_manifest_rewritten below.
        let table = make_v2_minimal_table();

        let file1 = make_data_file(&table, "data/file1.parquet", 10);
        let file2 = make_data_file(&table, "data/file2.parquet", 20);
        let file3 = make_data_file(&table, "data/file3.parquet", 30);

        // Append file1+file2 first — they share one manifest.
        let tx = Transaction::new(&table);
        let append12 = tx
            .fast_append()
            .add_data_files(vec![file1.clone(), file2.clone()]);
        let updates12 = Arc::new(append12)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        let table_after_12 = apply_updates_to_table(&table, &updates12);

        // Append file3 separately — it gets its own manifest.
        let tx = Transaction::new(&table_after_12);
        let append3 = tx.fast_append().add_data_files(vec![file3.clone()]);
        let updates3 = Arc::new(append3)
            .commit(&table_after_12)
            .await
            .unwrap()
            .take_updates();
        let table_with_files = apply_updates_to_table(&table_after_12, &updates3);

        // Replace only file1 and file2 with one compacted file; file3 should survive.
        let compacted = make_data_file(&table, "data/compacted.parquet", 30);
        let tx = Transaction::new(&table_with_files);
        let replace_action = tx
            .rewrite_files()
            .delete_files(vec![file1, file2])
            .add_files(vec![compacted]);
        let updates = Arc::new(replace_action)
            .commit(&table_with_files)
            .await
            .unwrap()
            .take_updates();

        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            unreachable!()
        };

        assert_eq!(new_snapshot.summary().operation, Operation::Replace);

        let manifest_list = new_snapshot
            .load_manifest_list(table_with_files.file_io(), table_with_files.metadata())
            .await
            .unwrap();

        let mut alive_files: Vec<String> = Vec::new();
        for entry in manifest_list.entries() {
            let manifest = entry
                .load_manifest(table_with_files.file_io())
                .await
                .unwrap();
            for me in manifest.entries() {
                if me.is_alive() {
                    alive_files.push(me.file_path().to_string());
                }
            }
        }
        alive_files.sort();

        assert!(
            alive_files.contains(&"data/compacted.parquet".to_string()),
            "Compacted file should be present"
        );
        assert!(
            alive_files.contains(&"data/file3.parquet".to_string()),
            "file3 should survive"
        );
        assert!(
            !alive_files.contains(&"data/file1.parquet".to_string()),
            "file1 should be gone"
        );
        assert!(
            !alive_files.contains(&"data/file2.parquet".to_string()),
            "file2 should be gone"
        );

        // Verify snapshot summary totals.
        // Previous snapshot had 3 data files; replace adds 1, deletes 2 → total = 2.
        let summary_props = &new_snapshot.summary().additional_properties;
        assert_eq!(
            summary_props.get("total-data-files").map(String::as_str),
            Some("2"),
            "total-data-files should be 2 after partial replace"
        );
        assert_eq!(
            summary_props.get("added-data-files").map(String::as_str),
            Some("1"),
            "added-data-files should be 1"
        );
        assert_eq!(
            summary_props.get("deleted-data-files").map(String::as_str),
            Some("2"),
            "deleted-data-files should be 2"
        );
    }

    // --- 012: FastAppend must carry forward delete-only manifests ---

    #[tokio::test]
    async fn test_rewrite_then_fast_append_preserves_delete_manifest() {
        // Verify that a FastAppend after a Replace does NOT drop the delete-only manifest.
        // Regression for the bug fixed in upstream apache/iceberg-rust PR #2149.
        let table = make_v2_minimal_table();

        // Step 1: append two files.
        let file1 = make_data_file(&table, "data/file1.parquet", 10);
        let file2 = make_data_file(&table, "data/file2.parquet", 20);
        let tx = Transaction::new(&table);
        let append = tx
            .fast_append()
            .add_data_files(vec![file1.clone(), file2.clone()]);
        let updates = Arc::new(append)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        let table1 = apply_updates_to_table(&table, &updates);

        // Step 2: replace both files with a compacted file.
        let compacted = make_data_file(&table, "data/compacted.parquet", 30);
        let tx = Transaction::new(&table1);
        let replace = tx
            .rewrite_files()
            .delete_files(vec![file1, file2])
            .add_files(vec![compacted.clone()]);
        let updates = Arc::new(replace)
            .commit(&table1)
            .await
            .unwrap()
            .take_updates();
        let table2 = apply_updates_to_table(&table1, &updates);

        // Step 3: fast-append a new file on top.
        let file3 = make_data_file(&table, "data/file3.parquet", 5);
        let tx = Transaction::new(&table2);
        let append2 = tx.fast_append().add_data_files(vec![file3]);
        let updates = Arc::new(append2)
            .commit(&table2)
            .await
            .unwrap()
            .take_updates();

        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            unreachable!()
        };

        let manifest_list = new_snapshot
            .load_manifest_list(table2.file_io(), table2.metadata())
            .await
            .unwrap();

        // Count manifests containing deleted entries — the delete manifest from Replace must
        // survive into the snapshot produced by FastAppend.
        let deleted_count: usize = {
            let mut count = 0;
            for entry in manifest_list.entries() {
                let manifest = entry.load_manifest(table2.file_io()).await.unwrap();
                for me in manifest.entries() {
                    if matches!(me.status(), ManifestStatus::Deleted) {
                        count += 1;
                    }
                }
            }
            count
        };

        assert_eq!(
            deleted_count, 2,
            "Delete manifest from Replace should survive the subsequent FastAppend"
        );
    }

    // --- 013: validate_from_snapshot ---

    #[tokio::test]
    async fn test_rewrite_validate_from_snapshot_succeeds() {
        // validate_from_snapshot with the planning snapshot should succeed even when validated
        // against a historical snapshot that contains the files.
        let table = make_v2_minimal_table();

        let file1 = make_data_file(&table, "data/file1.parquet", 10);
        let tx = Transaction::new(&table);
        let append = tx.fast_append().add_data_files(vec![file1.clone()]);
        let updates = Arc::new(append)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        let table1 = apply_updates_to_table(&table, &updates);

        // Record the snapshot ID at planning time.
        let planning_snapshot_id = table1.metadata().current_snapshot().unwrap().snapshot_id();

        let compacted = make_data_file(&table, "data/compacted.parquet", 10);
        let tx = Transaction::new(&table1);
        let replace = tx
            .rewrite_files()
            .validate_from_snapshot(planning_snapshot_id)
            .delete_files(vec![file1])
            .add_files(vec![compacted]);

        // Should succeed — file1 exists in the specified snapshot.
        assert!(Arc::new(replace).commit(&table1).await.is_ok());
    }

    #[tokio::test]
    async fn test_rewrite_validate_from_snapshot_expired_falls_back_to_current() {
        // validate_from_snapshot with a non-existent (expired) snapshot ID falls back
        // to the current snapshot. The commit should succeed because the files to delete
        // are still alive in the current snapshot.
        let table = make_v2_minimal_table();

        let file1 = make_data_file(&table, "data/file1.parquet", 10);
        let tx = Transaction::new(&table);
        let append = tx.fast_append().add_data_files(vec![file1.clone()]);
        let updates = Arc::new(append)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        let table1 = apply_updates_to_table(&table, &updates);

        let compacted = make_data_file(&table, "data/compacted.parquet", 10);
        let tx = Transaction::new(&table1);
        let replace = tx
            .rewrite_files()
            .validate_from_snapshot(999_999_999) // does not exist — simulates an expired snapshot
            .delete_files(vec![file1])
            .add_files(vec![compacted]);

        // Should succeed: expired planning snapshot falls back to current snapshot,
        // and file1 is alive there.
        assert!(
            Arc::new(replace).commit(&table1).await.is_ok(),
            "expected commit to succeed when validate_from_snapshot is expired"
        );
    }

    #[tokio::test]
    async fn test_rewrite_validate_from_snapshot_expired_file_concurrently_deleted() {
        // Scenario: validate_from_snapshot refers to an expired snapshot (simulated by a
        // non-existent ID so the fallback fires), AND the file to delete was concurrently
        // removed before the commit. Expected: commit fails with DataInvalid; error message
        // references "current snapshot (planning snapshot expired)", not the expired ID.
        let table = make_v2_minimal_table();

        // Step 1: Append file1 → table1.
        let file1 = make_data_file(&table, "data/file1.parquet", 10);
        let table1 = append_files(table.clone(), vec![file1.clone()]).await;

        // Step 2: Simulate concurrent delete — replace file1 so it is no longer alive.
        let replacement = make_data_file(&table, "data/replacement.parquet", 10);
        let tx = Transaction::new(&table1);
        let replace_concurrent = tx
            .rewrite_files()
            .delete_files(vec![file1.clone()])
            .add_files(vec![replacement]);
        let updates2 = Arc::new(replace_concurrent)
            .commit(&table1)
            .await
            .unwrap()
            .take_updates();
        let table2 = apply_updates_to_table(&table1, &updates2);

        // Step 3: Attempt another replace of file1 using an expired planning snapshot ID.
        // Fallback fires (999_999_999 not found → current snapshot used for validation).
        // file1 is not alive in table2's current snapshot → validation must reject.
        let compacted = make_data_file(&table, "data/compacted.parquet", 10);
        let tx = Transaction::new(&table2);
        let replace_stale = tx
            .rewrite_files()
            .validate_from_snapshot(999_999_999) // non-existent → triggers expired fallback
            .delete_files(vec![file1])
            .add_files(vec![compacted]);

        let Err(err) = Arc::new(replace_stale).commit(&table2).await else {
            panic!("commit should fail: file1 is no longer alive in current snapshot");
        };

        let msg = err.to_string();
        assert!(
            msg.contains("current snapshot"),
            "error should reference 'current snapshot', not the expired ID; got: {msg}"
        );
        assert!(
            !msg.contains("999999999"),
            "error should not reference the expired snapshot ID; got: {msg}"
        );
    }

    // --- 014: data_sequence_number ---

    // --- Mixed manifest detection ---

    #[tokio::test]
    async fn test_rewrite_partial_manifest_rewritten() {
        // file_a and file_b share one manifest; file_c is in its own manifest.
        // Deleting only file_a must produce a residual manifest with file_b surviving.
        //
        // Also serves as regression coverage for the inherit_data call in the
        // survivors loop: the sequence_number assertion below would fire if inherited
        // fields were written as 0 instead of their actual values.
        let table = make_v2_minimal_table();

        let file_a = make_data_file(&table, "data/a.parquet", 10);
        let file_b = make_data_file(&table, "data/b.parquet", 20);
        let file_c = make_data_file(&table, "data/c.parquet", 30);

        // Append A and B together — they share one manifest.
        let table_after_ab =
            append_files(table.clone(), vec![file_a.clone(), file_b.clone()]).await;

        // Append C separately — its own manifest.
        let table_with_files = append_files(table_after_ab.clone(), vec![file_c]).await;

        // Delete only A. B survives in a residual manifest; C is untouched.
        let compacted = make_data_file(&table, "data/compacted.parquet", 10);
        let tx = Transaction::new(&table_with_files);
        let replace = tx
            .rewrite_files()
            .delete_files(vec![file_a])
            .add_files(vec![compacted]);
        let mut commit = Arc::new(replace)
            .commit(&table_with_files)
            .await
            .expect("partial manifest should succeed, not error");
        let updates = commit.take_updates();

        let new_snapshot = unwrap_add_snapshot(&updates);

        // Collect alive files and verify B and compacted are present, A is gone.
        let alive_files = collect_alive_files(new_snapshot, &table_with_files).await;

        // Also extract Existing entries to check sequence numbers are preserved.
        let manifest_list = new_snapshot
            .load_manifest_list(table_with_files.file_io(), table_with_files.metadata())
            .await
            .unwrap();
        let mut existing_entries: Vec<(String, Option<i64>)> = Vec::new();
        for mf in manifest_list.entries() {
            let manifest = mf.load_manifest(table_with_files.file_io()).await.unwrap();
            for me in manifest.entries() {
                if me.is_alive() && me.status() == ManifestStatus::Existing {
                    existing_entries.push((me.file_path().to_string(), me.sequence_number()));
                }
            }
        }

        assert!(
            alive_files.contains(&"data/b.parquet".to_string()),
            "file_b must survive in residual manifest; alive: {alive_files:?}"
        );
        assert!(
            alive_files.contains(&"data/c.parquet".to_string()),
            "file_c must survive in untouched manifest; alive: {alive_files:?}"
        );
        assert!(
            alive_files.contains(&"data/compacted.parquet".to_string()),
            "compacted file must be present; alive: {alive_files:?}"
        );
        assert!(
            !alive_files.contains(&"data/a.parquet".to_string()),
            "file_a must be deleted; alive: {alive_files:?}"
        );

        // Survivor (b) must be Existing with a non-None sequence number.
        let b_entry = existing_entries
            .iter()
            .find(|(p, _)| p == "data/b.parquet")
            .expect("file_b must have an Existing entry");
        assert!(
            b_entry.1.is_some(),
            "surviving file_b must have sequence_number set"
        );
    }

    #[tokio::test]
    async fn test_rewrite_full_manifest_delete_no_regression() {
        // Deleting ALL files from a manifest must succeed (not a mixed manifest).
        let table = make_v2_minimal_table();

        let file_a = make_data_file(&table, "data/a.parquet", 10);
        let file_b = make_data_file(&table, "data/b.parquet", 20);

        // Append A and B together — they share one manifest.
        let tx = Transaction::new(&table);
        let append = tx
            .fast_append()
            .add_data_files(vec![file_a.clone(), file_b.clone()]);
        let updates = Arc::new(append)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        let table_with_files = apply_updates_to_table(&table, &updates);

        // Delete both A and B — full manifest delete, no survivors.
        let compacted = make_data_file(&table, "data/compacted.parquet", 30);
        let tx = Transaction::new(&table_with_files);
        let replace = tx
            .rewrite_files()
            .delete_files(vec![file_a, file_b])
            .add_files(vec![compacted]);
        assert!(
            Arc::new(replace).commit(&table_with_files).await.is_ok(),
            "Full manifest delete should succeed"
        );
    }

    #[tokio::test]
    async fn test_rewrite_single_file_manifests_no_false_positive() {
        // A and B are in separate single-file manifests. Deleting A must succeed; B must survive.
        let table = make_v2_minimal_table();

        let file_a = make_data_file(&table, "data/a.parquet", 10);
        let file_b = make_data_file(&table, "data/b.parquet", 20);

        // Append A alone — its own manifest.
        let tx = Transaction::new(&table);
        let append_a = tx.fast_append().add_data_files(vec![file_a.clone()]);
        let updates_a = Arc::new(append_a)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        let table_after_a = apply_updates_to_table(&table, &updates_a);

        // Append B alone — its own manifest.
        let tx = Transaction::new(&table_after_a);
        let append_b = tx.fast_append().add_data_files(vec![file_b]);
        let updates_b = Arc::new(append_b)
            .commit(&table_after_a)
            .await
            .unwrap()
            .take_updates();
        let table_with_files = apply_updates_to_table(&table_after_a, &updates_b);

        // Delete A only. B is in its own manifest — must succeed.
        let compacted = make_data_file(&table, "data/compacted.parquet", 10);
        let tx = Transaction::new(&table_with_files);
        let replace = tx
            .rewrite_files()
            .delete_files(vec![file_a])
            .add_files(vec![compacted]);
        let mut commit = Arc::new(replace)
            .commit(&table_with_files)
            .await
            .expect("single-file-manifest delete should succeed");
        let updates = commit.take_updates();

        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            unreachable!()
        };

        let manifest_list = new_snapshot
            .load_manifest_list(table_with_files.file_io(), table_with_files.metadata())
            .await
            .unwrap();

        let mut alive_files: Vec<String> = Vec::new();
        for entry in manifest_list.entries() {
            let manifest = entry
                .load_manifest(table_with_files.file_io())
                .await
                .unwrap();
            for me in manifest.entries() {
                if me.is_alive() {
                    alive_files.push(me.file_path().to_string());
                }
            }
        }

        assert!(
            alive_files.contains(&"data/b.parquet".to_string()),
            "file_b should survive; alive: {alive_files:?}"
        );
        assert!(
            alive_files.contains(&"data/compacted.parquet".to_string()),
            "compacted file should be present"
        );
        assert!(
            !alive_files.contains(&"data/a.parquet".to_string()),
            "file_a should be gone"
        );
    }

    #[tokio::test]
    async fn test_rewrite_multiple_partial_manifests() {
        // Two manifests, each with two files; delete one file from each.
        // Both must produce residual manifests — each survivor appears in the snapshot.
        let table = make_v2_minimal_table();

        let file_a = make_data_file(&table, "data/a.parquet", 10);
        let file_b = make_data_file(&table, "data/b.parquet", 20);
        let file_c = make_data_file(&table, "data/c.parquet", 30);
        let file_d = make_data_file(&table, "data/d.parquet", 40);

        // A+B share manifest 1.
        let table_after_ab =
            append_files(table.clone(), vec![file_a.clone(), file_b.clone()]).await;

        // C+D share manifest 2.
        let table_with_files =
            append_files(table_after_ab.clone(), vec![file_c.clone(), file_d.clone()]).await;

        // Delete A and C — one from each manifest; B and D survive.
        let compacted = make_data_file(&table, "data/compacted.parquet", 50);
        let tx = Transaction::new(&table_with_files);
        let replace = tx
            .rewrite_files()
            .delete_files(vec![file_a, file_c])
            .add_files(vec![compacted]);
        let mut commit = Arc::new(replace)
            .commit(&table_with_files)
            .await
            .expect("multiple partial manifests should succeed");
        let updates = commit.take_updates();

        let new_snapshot = unwrap_add_snapshot(&updates);
        let alive_files = collect_alive_files(new_snapshot, &table_with_files).await;

        assert!(
            alive_files.contains(&"data/b.parquet".to_string()),
            "file_b must survive; alive: {alive_files:?}"
        );
        assert!(
            alive_files.contains(&"data/d.parquet".to_string()),
            "file_d must survive; alive: {alive_files:?}"
        );
        assert!(
            alive_files.contains(&"data/compacted.parquet".to_string()),
            "compacted file must be present; alive: {alive_files:?}"
        );
        assert!(
            !alive_files.contains(&"data/a.parquet".to_string()),
            "file_a must be gone; alive: {alive_files:?}"
        );
        assert!(
            !alive_files.contains(&"data/c.parquet".to_string()),
            "file_c must be gone; alive: {alive_files:?}"
        );
    }

    #[tokio::test]
    async fn test_rewrite_complete_and_partial_mix() {
        // Three manifests: manifest 1 fully consumed, manifest 2 partially consumed (residual),
        // manifest 3 untouched. Verify each is handled correctly.
        let table = make_v2_minimal_table();

        let file_a = make_data_file(&table, "data/a.parquet", 10);
        let file_b = make_data_file(&table, "data/b.parquet", 20);
        let file_c = make_data_file(&table, "data/c.parquet", 30);
        let file_d = make_data_file(&table, "data/d.parquet", 40);
        let file_e = make_data_file(&table, "data/e.parquet", 50);

        // A+B → manifest 1 (will be fully consumed).
        let table1 = append_files(table.clone(), vec![file_a.clone(), file_b.clone()]).await;

        // C+D → manifest 2 (will be partially consumed; D survives).
        let table2 = append_files(table1.clone(), vec![file_c.clone(), file_d.clone()]).await;

        // E → manifest 3 (untouched).
        let table_with_files = append_files(table2.clone(), vec![file_e]).await;

        // Delete A, B (fully consumes manifest 1), and C (partially consumes manifest 2).
        let compacted = make_data_file(&table, "data/compacted.parquet", 60);
        let tx = Transaction::new(&table_with_files);
        let replace = tx
            .rewrite_files()
            .delete_files(vec![file_a, file_b, file_c])
            .add_files(vec![compacted]);
        let mut commit = Arc::new(replace)
            .commit(&table_with_files)
            .await
            .expect("complete+partial mix should succeed");
        let updates = commit.take_updates();

        let new_snapshot = unwrap_add_snapshot(&updates);
        let alive_files = collect_alive_files(new_snapshot, &table_with_files).await;

        // D survives from the residual of manifest 2.
        assert!(
            alive_files.contains(&"data/d.parquet".to_string()),
            "file_d must survive in residual; alive: {alive_files:?}"
        );
        // E survives from the untouched manifest 3.
        assert!(
            alive_files.contains(&"data/e.parquet".to_string()),
            "file_e must survive from untouched manifest; alive: {alive_files:?}"
        );
        // Compacted file is present.
        assert!(
            alive_files.contains(&"data/compacted.parquet".to_string()),
            "compacted file must be present; alive: {alive_files:?}"
        );
        // A, B, C are all deleted.
        assert!(
            !alive_files.contains(&"data/a.parquet".to_string()),
            "file_a must be gone; alive: {alive_files:?}"
        );
        assert!(
            !alive_files.contains(&"data/b.parquet".to_string()),
            "file_b must be gone; alive: {alive_files:?}"
        );
        assert!(
            !alive_files.contains(&"data/c.parquet".to_string()),
            "file_c must be gone; alive: {alive_files:?}"
        );
    }

    #[tokio::test]
    async fn test_rewrite_data_sequence_number_is_set_on_entries() {
        // data_sequence_number() must propagate to the added manifest entries.
        let table = make_v2_minimal_table();

        let file1 = make_data_file(&table, "data/file1.parquet", 10);
        let tx = Transaction::new(&table);
        let append = tx.fast_append().add_data_files(vec![file1.clone()]);
        let updates = Arc::new(append)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        let table1 = apply_updates_to_table(&table, &updates);

        let compacted = make_data_file(&table, "data/compacted.parquet", 10);
        let custom_seq: i64 = 42;

        let tx = Transaction::new(&table1);
        let replace = tx
            .rewrite_files()
            .data_sequence_number(custom_seq)
            .delete_files(vec![file1])
            .add_files(vec![compacted]);

        let updates = Arc::new(replace)
            .commit(&table1)
            .await
            .unwrap()
            .take_updates();

        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            unreachable!()
        };

        let manifest_list = new_snapshot
            .load_manifest_list(table1.file_io(), table1.metadata())
            .await
            .unwrap();

        // Find the Added entry and verify its sequence number equals custom_seq.
        let mut found = false;
        for entry in manifest_list.entries() {
            let manifest = entry.load_manifest(table1.file_io()).await.unwrap();
            for me in manifest.entries() {
                if matches!(me.status(), ManifestStatus::Added) {
                    assert_eq!(
                        me.sequence_number(),
                        Some(custom_seq),
                        "Added entry must carry the explicit data_sequence_number"
                    );
                    found = true;
                }
            }
        }
        assert!(found, "No Added manifest entry found");
    }
}
