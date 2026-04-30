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
    /// Validate against this historical snapshot rather than the current one.
    /// Used by concurrent compaction so a snapshot committed between planning
    /// and committing doesn't reject files that were alive at plan time.
    validate_from_snapshot_id: Option<i64>,
    /// Stamps added entries with this sequence number instead of inheriting it.
    /// Required for v2 equality-delete coverage on rewrites.
    data_sequence_number: Option<i64>,
    /// Initialized on the first commit attempt and shared across catalog retries
    /// so the filter+merge caches survive — Java's idempotent retry contract.
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

    /// Old files being replaced. Paths cannot also appear in `add_files`:
    /// `validate_duplicate_files` checks the new paths against all alive entries.
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

    /// Validate `files_to_delete` against `snapshot_id` rather than the current
    /// snapshot, so a concurrent writer between plan and commit doesn't reject
    /// files that were alive at planning time.
    pub fn validate_from_snapshot(mut self, snapshot_id: i64) -> Self {
        self.validate_from_snapshot_id = Some(snapshot_id);
        self
    }

    /// Stamp every added entry with this explicit data sequence number rather
    /// than inheriting from the new snapshot. Required for v2 equality-delete
    /// correctness: compacted files that predate an equality delete must keep
    /// the original sequence number so the delete continues to apply.
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

        // Reject duplicate paths in files_to_delete — duplicates would inflate
        // summary statistics (remove_file would fire twice for the same path).
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

        // `commit` takes `Arc<Self>` and can't move out of fields, so the file
        // vectors must be cloned. Non-trivial for large compaction jobs.
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

        match snapshot_producer.commit(rewrite_op, process).await {
            Ok(commit) => Ok(commit),
            Err(err) => {
                // Do not clean up on error: commit may have actually succeeded but the ACK was lost.
                // Deleting here risks removing committed manifests, leading to silent table corruption.
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
                    // Planning snapshot was expired by the catalog between plan and
                    // commit (common on long compaction runs). Fall back to the
                    // current snapshot — the validation only needs to confirm the
                    // files are still alive somewhere.
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

        let object_cache = snapshot_produce.table.object_cache();
        let manifests = try_join_all(
            manifest_list
                .entries()
                .iter()
                .map(|e| object_cache.get_manifest(e)),
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

        let missing: Vec<&str> = self
            .files_to_delete
            .iter()
            .map(|s| s.as_str())
            .filter(|p| !found_paths.contains(*p))
            .collect();
        if !missing.is_empty() {
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
        // Carry-forward always reads from current_snapshot, even when
        // delete_entries() validated against a historical planning snapshot —
        // the new manifest list must reflect the table as committed.
        let Some(snapshot) = snapshot_produce.table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_produce.table.file_io(),
                &snapshot_produce.table.metadata_ref(),
            )
            .await?;
        let current_manifests: Vec<ManifestFile> = manifest_list.entries().to_vec();
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
        // validate_duplicate_files() must reject adding a file already alive
        // in the snapshot, even if the same call also marks it for deletion.
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

        let tx = Transaction::new(&table_with_file);
        let action = tx
            .rewrite_files()
            .delete_files(vec![file1.clone()])
            .add_files(vec![file1.clone()]);
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

        assert!(table_with_files.metadata().current_snapshot().is_some());

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

        // Pin total-data-files = 1 (had 2; rewrote 2, added 1) — regression
        // for the prior-totals bug where update_snapshot_summaries used the
        // wrong snapshot.
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
        // Tests the "one manifest fully consumed, another untouched" path:
        // file1+file2 share a manifest (both deleted), file3's manifest is
        // unaffected. The mixed (residual) case lives in
        // test_rewrite_partial_manifest_rewritten.
        let table = make_v2_minimal_table();

        let file1 = make_data_file(&table, "data/file1.parquet", 10);
        let file2 = make_data_file(&table, "data/file2.parquet", 20);
        let file3 = make_data_file(&table, "data/file3.parquet", 30);

        // file1+file2 share one manifest; file3 gets its own.
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

        let tx = Transaction::new(&table_after_12);
        let append3 = tx.fast_append().add_data_files(vec![file3.clone()]);
        let updates3 = Arc::new(append3)
            .commit(&table_after_12)
            .await
            .unwrap()
            .take_updates();
        let table_with_files = apply_updates_to_table(&table_after_12, &updates3);

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

        // 3 data files initially; rewrote 2, added 1 → total 2.
        let summary_props = &new_snapshot.summary().additional_properties;
        assert_eq!(summary_props.get("total-data-files").map(String::as_str), Some("2"));
        assert_eq!(summary_props.get("added-data-files").map(String::as_str), Some("1"));
        assert_eq!(summary_props.get("deleted-data-files").map(String::as_str), Some("2"));
    }

    #[tokio::test]
    async fn test_rewrite_then_fast_append_preserves_delete_manifest() {
        // Regression for apache/iceberg-rust PR #2149: a FastAppend after a
        // rewrite must not drop the delete-only manifest.
        let table = make_v2_minimal_table();

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

        assert_eq!(deleted_count, 2);
    }

    #[tokio::test]
    async fn test_rewrite_validate_from_snapshot_succeeds() {
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

        let planning_snapshot_id = table1.metadata().current_snapshot().unwrap().snapshot_id();

        let compacted = make_data_file(&table, "data/compacted.parquet", 10);
        let tx = Transaction::new(&table1);
        let replace = tx
            .rewrite_files()
            .validate_from_snapshot(planning_snapshot_id)
            .delete_files(vec![file1])
            .add_files(vec![compacted]);
        assert!(Arc::new(replace).commit(&table1).await.is_ok());
    }

    #[tokio::test]
    async fn test_rewrite_validate_from_snapshot_expired_falls_back_to_current() {
        // Non-existent planning snapshot ID falls back to the current snapshot.
        // The commit succeeds when the files-to-delete are still alive there.
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
            .validate_from_snapshot(999_999_999)
            .delete_files(vec![file1])
            .add_files(vec![compacted]);
        assert!(Arc::new(replace).commit(&table1).await.is_ok());
    }

    #[tokio::test]
    async fn test_rewrite_validate_from_snapshot_expired_file_concurrently_deleted() {
        // Expired planning snapshot AND the file was concurrently removed: must
        // fail, and the error must point to the current snapshot (not the
        // expired ID) so callers can debug the cause.
        let table = make_v2_minimal_table();

        let file1 = make_data_file(&table, "data/file1.parquet", 10);
        let table1 = append_files(table.clone(), vec![file1.clone()]).await;

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

        let compacted = make_data_file(&table, "data/compacted.parquet", 10);
        let tx = Transaction::new(&table2);
        let replace_stale = tx
            .rewrite_files()
            .validate_from_snapshot(999_999_999)
            .delete_files(vec![file1])
            .add_files(vec![compacted]);

        let Err(err) = Arc::new(replace_stale).commit(&table2).await else {
            panic!("commit should fail: file1 is no longer alive in current snapshot");
        };

        let msg = err.to_string();
        assert!(msg.contains("current snapshot"), "got: {msg}");
        assert!(!msg.contains("999999999"), "got: {msg}");
    }

    #[tokio::test]
    async fn test_rewrite_partial_manifest_rewritten() {
        // a+b share one manifest, c sits in its own. Deleting only a forces a
        // residual manifest with b surviving. Also covers the inherit_data path
        // in the residual writer: if the inherited sequence_number weren't
        // restored, the assertion below would catch it.
        let table = make_v2_minimal_table();

        let file_a = make_data_file(&table, "data/a.parquet", 10);
        let file_b = make_data_file(&table, "data/b.parquet", 20);
        let file_c = make_data_file(&table, "data/c.parquet", 30);

        let table_after_ab =
            append_files(table.clone(), vec![file_a.clone(), file_b.clone()]).await;
        let table_with_files = append_files(table_after_ab.clone(), vec![file_c]).await;

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
        let alive_files = collect_alive_files(new_snapshot, &table_with_files).await;

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

        assert!(alive_files.contains(&"data/b.parquet".to_string()), "{alive_files:?}");
        assert!(alive_files.contains(&"data/c.parquet".to_string()), "{alive_files:?}");
        assert!(alive_files.contains(&"data/compacted.parquet".to_string()), "{alive_files:?}");
        assert!(!alive_files.contains(&"data/a.parquet".to_string()), "{alive_files:?}");

        let b_entry = existing_entries
            .iter()
            .find(|(p, _)| p == "data/b.parquet")
            .expect("file_b must have an Existing entry");
        assert!(b_entry.1.is_some(), "surviving file_b must keep a sequence_number");
    }

    #[tokio::test]
    async fn test_rewrite_full_manifest_delete_no_regression() {
        // Deleting every alive entry of a single manifest must drop the
        // manifest cleanly rather than misclassifying it as a mixed-residual.
        let table = make_v2_minimal_table();

        let file_a = make_data_file(&table, "data/a.parquet", 10);
        let file_b = make_data_file(&table, "data/b.parquet", 20);

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

        let compacted = make_data_file(&table, "data/compacted.parquet", 30);
        let tx = Transaction::new(&table_with_files);
        let replace = tx
            .rewrite_files()
            .delete_files(vec![file_a, file_b])
            .add_files(vec![compacted]);
        assert!(Arc::new(replace).commit(&table_with_files).await.is_ok());
    }

    #[tokio::test]
    async fn test_rewrite_single_file_manifests_no_false_positive() {
        // a and b are each in their own single-file manifest. Deleting a must
        // not falsely flag b's manifest as mixed.
        let table = make_v2_minimal_table();

        let file_a = make_data_file(&table, "data/a.parquet", 10);
        let file_b = make_data_file(&table, "data/b.parquet", 20);

        let tx = Transaction::new(&table);
        let append_a = tx.fast_append().add_data_files(vec![file_a.clone()]);
        let updates_a = Arc::new(append_a)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        let table_after_a = apply_updates_to_table(&table, &updates_a);

        let tx = Transaction::new(&table_after_a);
        let append_b = tx.fast_append().add_data_files(vec![file_b]);
        let updates_b = Arc::new(append_b)
            .commit(&table_after_a)
            .await
            .unwrap()
            .take_updates();
        let table_with_files = apply_updates_to_table(&table_after_a, &updates_b);

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

        assert!(alive_files.contains(&"data/b.parquet".to_string()), "{alive_files:?}");
        assert!(alive_files.contains(&"data/compacted.parquet".to_string()), "{alive_files:?}");
        assert!(!alive_files.contains(&"data/a.parquet".to_string()), "{alive_files:?}");
    }

    #[tokio::test]
    async fn test_rewrite_multiple_partial_manifests() {
        // Two manifests, each with two files; delete one file from each →
        // both must produce residuals.
        let table = make_v2_minimal_table();

        let file_a = make_data_file(&table, "data/a.parquet", 10);
        let file_b = make_data_file(&table, "data/b.parquet", 20);
        let file_c = make_data_file(&table, "data/c.parquet", 30);
        let file_d = make_data_file(&table, "data/d.parquet", 40);

        let table_after_ab =
            append_files(table.clone(), vec![file_a.clone(), file_b.clone()]).await;
        let table_with_files =
            append_files(table_after_ab.clone(), vec![file_c.clone(), file_d.clone()]).await;

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

        assert!(alive_files.contains(&"data/b.parquet".to_string()), "{alive_files:?}");
        assert!(alive_files.contains(&"data/d.parquet".to_string()), "{alive_files:?}");
        assert!(alive_files.contains(&"data/compacted.parquet".to_string()), "{alive_files:?}");
        assert!(!alive_files.contains(&"data/a.parquet".to_string()), "{alive_files:?}");
        assert!(!alive_files.contains(&"data/c.parquet".to_string()), "{alive_files:?}");
    }

    #[tokio::test]
    async fn test_rewrite_complete_and_partial_mix() {
        // m1=[a,b] fully consumed, m2=[c,d] partially consumed (d survives in
        // a residual), m3=[e] untouched.
        let table = make_v2_minimal_table();

        let file_a = make_data_file(&table, "data/a.parquet", 10);
        let file_b = make_data_file(&table, "data/b.parquet", 20);
        let file_c = make_data_file(&table, "data/c.parquet", 30);
        let file_d = make_data_file(&table, "data/d.parquet", 40);
        let file_e = make_data_file(&table, "data/e.parquet", 50);

        let table1 = append_files(table.clone(), vec![file_a.clone(), file_b.clone()]).await;
        let table2 = append_files(table1.clone(), vec![file_c.clone(), file_d.clone()]).await;
        let table_with_files = append_files(table2.clone(), vec![file_e]).await;

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

        for kept in ["data/d.parquet", "data/e.parquet", "data/compacted.parquet"] {
            assert!(alive_files.contains(&kept.to_string()), "{alive_files:?}");
        }
        for gone in ["data/a.parquet", "data/b.parquet", "data/c.parquet"] {
            assert!(!alive_files.contains(&gone.to_string()), "{alive_files:?}");
        }
    }

    #[tokio::test]
    async fn test_rewrite_data_sequence_number_is_set_on_entries() {
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

        let mut found = false;
        for entry in manifest_list.entries() {
            let manifest = entry.load_manifest(table1.file_io()).await.unwrap();
            for me in manifest.entries() {
                if matches!(me.status(), ManifestStatus::Added) {
                    assert_eq!(me.sequence_number(), Some(custom_seq));
                    found = true;
                }
            }
        }
        assert!(found, "no Added entry");
    }

    #[tokio::test]
    async fn test_rewrite_empty_table_errors() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx.rewrite_files();
        let Err(err) = Arc::new(action).commit(&table).await else {
            panic!("empty rewrite must error");
        };
        assert!(err.to_string().contains("files to add"));
    }

    #[tokio::test]
    async fn test_rewrite_add_only_succeeds() {
        // Java's BaseRewriteFiles.validateReplacedAndAddedFiles forbids
        // addsDataFiles() without deletesDataFiles(). This fork doesn't enforce
        // that yet; pin the current behavior so a future tightening is a
        // deliberate decision rather than an accidental regression.
        let table = make_v2_minimal_table();
        let f1 = make_data_file(&table, "data/keep.parquet", 10);
        let table_with_files = append_files(table.clone(), vec![f1]).await;

        let new_file = make_data_file(&table, "data/new.parquet", 10);
        let tx = Transaction::new(&table_with_files);
        let action = tx.rewrite_files().add_files(vec![new_file]);
        assert!(Arc::new(action).commit(&table_with_files).await.is_ok());
    }

    #[tokio::test]
    async fn test_rewrite_already_deleted_file() {
        let table = make_v2_minimal_table();
        let f1 = make_data_file(&table, "data/f1.parquet", 10);
        let table = append_files(table, vec![f1.clone()]).await;

        let new1 = make_data_file(&table, "data/new1.parquet", 10);
        let tx = Transaction::new(&table);
        let r1 = tx
            .rewrite_files()
            .delete_files(vec![f1.clone()])
            .add_files(vec![new1]);
        let updates = Arc::new(r1)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        let table = apply_updates_to_table(&table, &updates);

        let new2 = make_data_file(&table, "data/new2.parquet", 10);
        let tx = Transaction::new(&table);
        let r2 = tx.rewrite_files().delete_files(vec![f1]).add_files(vec![new2]);
        let Err(err) = Arc::new(r2).commit(&table).await else {
            panic!("rewriting an already-deleted file must error");
        };
        assert!(err.to_string().contains("not found as alive entries"));
    }

    #[tokio::test]
    async fn test_rewrite_emits_manifest_summary_keys() {
        let table = make_v2_minimal_table();
        let f1 = make_data_file(&table, "data/f1.parquet", 10);
        let table = append_files(table, vec![f1.clone()]).await;

        let new = make_data_file(&table, "data/new.parquet", 10);
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files()
            .delete_files(vec![f1])
            .add_files(vec![new]);
        let updates = Arc::new(action)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        let snap = unwrap_add_snapshot(&updates);
        let props = &snap.summary().additional_properties;
        assert!(props.contains_key("manifests-created"), "{props:?}");
        assert!(props.contains_key("manifests-kept"));
        assert!(props.contains_key("manifests-replaced"));
    }

    #[tokio::test]
    async fn test_rewrite_merge_disabled_passthrough() {
        // commit.manifest-merge.enabled=false → filter still runs but the merge
        // step no-ops, so manifest count grows monotonically across compactions.
        let mut table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let prop_action = tx.update_table_properties().set(
            "commit.manifest-merge.enabled".to_string(),
            "false".to_string(),
        );
        let updates = Arc::new(prop_action)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        table = apply_updates_to_table(&table, &updates);

        let f1 = make_data_file(&table, "data/f1.parquet", 10);
        let f2 = make_data_file(&table, "data/f2.parquet", 10);
        let table = append_files(table, vec![f1.clone()]).await;
        let table = append_files(table, vec![f2.clone()]).await;

        let initial_manifest_count = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap()
            .entries()
            .len();
        assert!(initial_manifest_count >= 2);

        let c1 = make_data_file(&table, "data/c1.parquet", 10);
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files()
            .delete_files(vec![f1])
            .add_files(vec![c1.clone()]);
        let updates = Arc::new(action)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        let table = apply_updates_to_table(&table, &updates);

        let c2 = make_data_file(&table, "data/c2.parquet", 10);
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files()
            .delete_files(vec![f2])
            .add_files(vec![c2]);
        let updates = Arc::new(action)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        let table = apply_updates_to_table(&table, &updates);

        let final_count = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap()
            .entries()
            .len();
        assert!(
            final_count >= initial_manifest_count,
            "initial={initial_manifest_count}, final={final_count}"
        );
    }

    #[tokio::test]
    async fn test_rewrite_merge_consolidates_small_siblings() {
        // min-count-to-merge=2 forces the merge to fire on small bins; a single
        // rewrite should not increase manifest count even with several small
        // sibling manifests in scope.
        let mut table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let prop_action = tx
            .update_table_properties()
            .set(
                "commit.manifest.min-count-to-merge".to_string(),
                "2".to_string(),
            )
            .set(
                "commit.manifest.target-size-bytes".to_string(),
                "1048576".to_string(),
            );
        let updates = Arc::new(prop_action)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        table = apply_updates_to_table(&table, &updates);

        let f1 = make_data_file(&table, "data/f1.parquet", 10);
        let f2 = make_data_file(&table, "data/f2.parquet", 10);
        let f3 = make_data_file(&table, "data/f3.parquet", 10);
        let f4 = make_data_file(&table, "data/f4.parquet", 10);
        let mut t = table;
        for f in [&f1, &f2, &f3, &f4] {
            t = append_files(t, vec![f.clone()]).await;
        }

        let pre_count = t
            .metadata()
            .current_snapshot()
            .unwrap()
            .load_manifest_list(t.file_io(), t.metadata())
            .await
            .unwrap()
            .entries()
            .len();

        let c1 = make_data_file(&t, "data/c1.parquet", 10);
        let tx = Transaction::new(&t);
        let action = tx
            .rewrite_files()
            .delete_files(vec![f1])
            .add_files(vec![c1]);
        let updates = Arc::new(action)
            .commit(&t)
            .await
            .unwrap()
            .take_updates();
        let t = apply_updates_to_table(&t, &updates);

        let post_count = t
            .metadata()
            .current_snapshot()
            .unwrap()
            .load_manifest_list(t.file_io(), t.metadata())
            .await
            .unwrap()
            .entries()
            .len();
        assert!(post_count <= pre_count, "pre={pre_count}, post={post_count}");

        let snap = t.metadata().current_snapshot().unwrap();
        let alive = collect_alive_files(snap, &t).await;
        for expected in ["data/c1.parquet", "data/f2.parquet", "data/f3.parquet", "data/f4.parquet"]
        {
            assert!(alive.contains(&expected.to_string()), "{alive:?}");
        }
    }

    #[tokio::test]
    async fn test_rewrite_sequence_number_preserved_across_merge() {
        // ADDED→EXISTING demotion during a merge must preserve the survivor's
        // original sequence number. Re-stamping it would break equality-delete
        // coverage for any delete pinned at the original seq.
        let mut table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let prop_action = tx.update_table_properties().set(
            "commit.manifest.min-count-to-merge".to_string(),
            "2".to_string(),
        );
        let updates = Arc::new(prop_action)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        table = apply_updates_to_table(&table, &updates);

        let stays_alive = make_data_file(&table, "data/stays.parquet", 10);
        let table = append_files(table, vec![stays_alive.clone()]).await;
        let stays_seq = table.metadata().current_snapshot().unwrap().sequence_number();

        let other = make_data_file(&table, "data/other.parquet", 10);
        let table = append_files(table, vec![other.clone()]).await;

        let compacted = make_data_file(&table, "data/compacted.parquet", 10);
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files()
            .delete_files(vec![other])
            .add_files(vec![compacted]);
        let updates = Arc::new(action)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        let table = apply_updates_to_table(&table, &updates);

        let snap = table.metadata().current_snapshot().unwrap();
        let manifest_list = snap
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let mut found = None;
        for ml in manifest_list.entries() {
            let manifest = ml.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                if entry.file_path() == stays_alive.file_path && entry.is_alive() {
                    found = entry.sequence_number();
                }
            }
        }
        let actual = found.expect("stays_alive must still be alive");
        assert_eq!(actual, stays_seq);
    }

    #[tokio::test]
    async fn test_rewrite_tombstone_suppression_across_merges() {
        // Prior-snapshot tombstones must NOT survive into a merged manifest —
        // this is the load-bearing rule that stops manifest-entry count from
        // growing monotonically across many compactions.
        let mut table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let prop_action = tx.update_table_properties().set(
            "commit.manifest.min-count-to-merge".to_string(),
            "2".to_string(),
        );
        let updates = Arc::new(prop_action)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        table = apply_updates_to_table(&table, &updates);

        let f1 = make_data_file(&table, "data/f1.parquet", 10);
        let f2 = make_data_file(&table, "data/f2.parquet", 10);
        let table = append_files(table, vec![f1.clone()]).await;
        let table = append_files(table, vec![f2.clone()]).await;

        let c1 = make_data_file(&table, "data/c1.parquet", 10);
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files()
            .delete_files(vec![f1])
            .add_files(vec![c1]);
        let updates = Arc::new(action)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        let table = apply_updates_to_table(&table, &updates);

        // Compacting f2 should fold the previous compaction's delete-manifest
        // (still carrying f1's tombstone) into a merged manifest, dropping the
        // tombstone in the process.
        let c2 = make_data_file(&table, "data/c2.parquet", 10);
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files()
            .delete_files(vec![f2])
            .add_files(vec![c2]);
        let updates = Arc::new(action)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        let table = apply_updates_to_table(&table, &updates);

        let snap = table.metadata().current_snapshot().unwrap();
        let current_snap_id = snap.snapshot_id();
        let manifest_list = snap
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let mut prior_tombstones = 0u64;
        for ml in manifest_list.entries() {
            let manifest = ml.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                if entry.status() == ManifestStatus::Deleted
                    && entry.snapshot_id() != Some(current_snap_id)
                {
                    prior_tombstones += 1;
                }
            }
        }
        assert_eq!(prior_tombstones, 0);
    }

    #[tokio::test]
    async fn test_rewrite_idempotent_merging_state() {
        // Re-invoking commit() on the same Arc<RewriteFilesAction> structurally
        // models the Transaction-level retry path. The merging-state caches
        // must short-circuit so the second attempt produces identical paths.
        let mut table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let prop_action = tx.update_table_properties().set(
            "commit.manifest.min-count-to-merge".to_string(),
            "2".to_string(),
        );
        let updates = Arc::new(prop_action)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        table = apply_updates_to_table(&table, &updates);

        let f1 = make_data_file(&table, "data/f1.parquet", 10);
        let f2 = make_data_file(&table, "data/f2.parquet", 10);
        let table = append_files(table, vec![f1.clone()]).await;
        let table = append_files(table, vec![f2.clone()]).await;

        let c1 = make_data_file(&table, "data/c1.parquet", 10);
        let action = Arc::new(
            Transaction::new(&table)
                .rewrite_files()
                .delete_files(vec![f1])
                .add_files(vec![c1]),
        );
        let _ = action.clone().commit(&table).await.unwrap();
        let _ = action.commit(&table).await.unwrap();
    }
}
