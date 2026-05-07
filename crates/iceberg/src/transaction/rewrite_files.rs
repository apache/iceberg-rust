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

use crate::error::Result;
use crate::spec::{DataFile, ManifestEntry, ManifestFile, Operation};
use crate::table::Table;
use crate::transaction::commit_ids::CommitIds;
use crate::transaction::merging_state::MergingState;
use crate::transaction::snapshot::{CommitResult, SnapshotProduceOperation, SnapshotProducer};
use crate::transaction::{ActionCommit, TransactionAction};
use crate::util::available_parallelism;
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
    ids: CommitIds,
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
    manifest_read_concurrency: usize,
    manifest_write_concurrency: usize,
}

impl RewriteFilesAction {
    pub(crate) fn new() -> Self {
        let num_cpus = available_parallelism().get();
        Self {
            ids: CommitIds::new(),
            key_metadata: None,
            snapshot_properties: HashMap::default(),
            files_to_delete: vec![],
            files_to_add: vec![],
            validate_from_snapshot_id: None,
            data_sequence_number: None,
            merging_state: OnceLock::new(),
            manifest_read_concurrency: num_cpus,
            manifest_write_concurrency: std::cmp::max(1, num_cpus / 4),
        }
    }

    /// Override the number of manifests read concurrently during the filter pass.
    pub fn with_manifest_read_concurrency(mut self, n: usize) -> Self {
        self.manifest_read_concurrency = std::cmp::max(1, n);
        self
    }

    /// Override the number of manifest bins written concurrently during the merge pass.
    /// Each write task buffers a full bin's entries; lower values protect memory.
    pub fn with_manifest_write_concurrency(mut self, n: usize) -> Self {
        self.manifest_write_concurrency = std::cmp::max(1, n);
        self
    }

    fn merging_state(&self, table: &Table) -> Result<Arc<MergingState>> {
        if let Some(state) = self.merging_state.get() {
            return Ok(state.clone());
        }
        let read = self.manifest_read_concurrency;
        let write = self.manifest_write_concurrency;
        let state = Arc::new(MergingState::from_table(table, read, write)?);
        let _ = self.merging_state.set(state);
        Ok(self.merging_state.get().expect("just initialized").clone())
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
        let mut snapshot_producer = SnapshotProducer::new(
            table,
            self.ids.snapshot_id(table),
            self.ids.commit_uuid(),
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

            snapshot_producer
                .validate_data_files_exist(&replaced_paths)
                .await?;
        }

        let state = self.merging_state(table)?;
        for f in &self.files_to_delete {
            state.delete(f);
        }

        let rewrite_op = RewriteOperation {
            state: state.clone(),
        };

        let CommitResult {
            commit,
            committed_manifest_paths,
        } = snapshot_producer.commit(rewrite_op, state.clone()).await?;
        // Only clean up on confirmed success. On error the commit may have reached the
        // catalog (ACK loss) — deleting committed manifests would cause silent corruption.
        // Java analog: MergingSnapshotProducer#cleanUncommitted.
        state
            .clean_uncommitted(table.file_io(), &committed_manifest_paths)
            .await;
        Ok(commit)
    }
}

struct RewriteOperation {
    state: Arc<MergingState>,
}

impl SnapshotProduceOperation for RewriteOperation {
    fn operation(&self) -> Operation {
        Operation::Replace
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        Ok(vec![])
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

    use crate::spec::{MAIN_BRANCH, ManifestStatus, Operation};
    use crate::transaction::action::ActionCommit;
    use crate::transaction::tests::{
        append_files, apply_updates_to_table, collect_alive_files, commit_uuid_from,
        make_data_file, make_v2_minimal_table, snapshot_id_from,
    };
    use crate::transaction::{Transaction, TransactionAction};
    use crate::{TableRequirement, TableUpdate};

    fn unwrap_add_snapshot(updates: &[TableUpdate]) -> &crate::spec::Snapshot {
        if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            panic!("expected AddSnapshot update at index 0")
        }
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
        let table_with_file = append_files(table.clone(), vec![file1.clone()]).await;

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
        let table_with_file = append_files(table.clone(), vec![file1.clone()]).await;

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
            err.to_string().contains("no longer alive"),
            "Expected phantom-delete error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_rewrite_duplicate_paths_in_delete_errors() {
        let table = make_v2_minimal_table();

        let file1 = make_data_file(&table, "data/file1.parquet", 50);
        let table_with_file = append_files(table.clone(), vec![file1.clone()]).await;

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
        let table_with_files =
            append_files(table.clone(), vec![file1.clone(), file2.clone()]).await;

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

        let new_snapshot = unwrap_add_snapshot(&updates);

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

        let manifest_list = new_snapshot
            .load_manifest_list(table_with_files.file_io(), table_with_files.metadata())
            .await
            .unwrap();

        assert_eq!(
            manifest_list.entries().len(),
            1,
            "Expected 1 manifest: added only"
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
        assert!(
            deleted_files.is_empty(),
            "No DELETED entries should exist after fix"
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
        let table_after_12 = append_files(table.clone(), vec![file1.clone(), file2.clone()]).await;
        let table_with_files = append_files(table_after_12, vec![file3.clone()]).await;

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

        let new_snapshot = unwrap_add_snapshot(&updates);

        assert_eq!(new_snapshot.summary().operation, Operation::Replace);

        let alive_files = collect_alive_files(new_snapshot, &table_with_files).await;

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
        assert_eq!(
            summary_props.get("total-data-files").map(String::as_str),
            Some("2")
        );
        assert_eq!(
            summary_props.get("added-data-files").map(String::as_str),
            Some("1")
        );
        assert_eq!(
            summary_props.get("deleted-data-files").map(String::as_str),
            Some("2")
        );
    }

    #[tokio::test]
    async fn test_rewrite_validate_from_snapshot_succeeds() {
        let table = make_v2_minimal_table();

        let file1 = make_data_file(&table, "data/file1.parquet", 10);
        let table1 = append_files(table, vec![file1.clone()]).await;

        let planning_snapshot_id = table1.metadata().current_snapshot().unwrap().snapshot_id();

        let compacted = make_data_file(&table1, "data/compacted.parquet", 10);
        let tx = Transaction::new(&table1);
        let replace = tx
            .rewrite_files()
            .validate_from_snapshot(planning_snapshot_id)
            .delete_files(vec![file1])
            .add_files(vec![compacted]);
        assert!(Arc::new(replace).commit(&table1).await.is_ok());
    }

    #[tokio::test]
    async fn test_rewrite_validate_from_snapshot_expired_errors() {
        // Expired planning snapshot is a hard error; the message must mention
        // the expired ID so callers can debug.
        let table = make_v2_minimal_table();

        let file1 = make_data_file(&table, "data/file1.parquet", 10);
        let table1 = append_files(table, vec![file1.clone()]).await;

        let compacted = make_data_file(&table1, "data/compacted.parquet", 10);
        let tx = Transaction::new(&table1);
        let replace = tx
            .rewrite_files()
            .validate_from_snapshot(999_999_999)
            .delete_files(vec![file1])
            .add_files(vec![compacted]);
        let Err(err) = Arc::new(replace).commit(&table1).await else {
            panic!("expected hard error on expired planning snapshot");
        };
        let msg = err.to_string();
        assert!(msg.contains("Cannot determine history"), "got: {msg}");
        assert!(msg.contains("999999999"), "got: {msg}");
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

        assert!(
            alive_files.contains(&"data/b.parquet".to_string()),
            "{alive_files:?}"
        );
        assert!(
            alive_files.contains(&"data/c.parquet".to_string()),
            "{alive_files:?}"
        );
        assert!(
            alive_files.contains(&"data/compacted.parquet".to_string()),
            "{alive_files:?}"
        );
        assert!(
            !alive_files.contains(&"data/a.parquet".to_string()),
            "{alive_files:?}"
        );

        let b_entry = existing_entries
            .iter()
            .find(|(p, _)| p == "data/b.parquet")
            .expect("file_b must have an Existing entry");
        assert!(
            b_entry.1.is_some(),
            "surviving file_b must keep a sequence_number"
        );
    }

    #[tokio::test]
    async fn test_rewrite_full_manifest_delete_no_regression() {
        // Deleting every alive entry of a single manifest must drop the
        // manifest cleanly rather than misclassifying it as a mixed-residual.
        let table = make_v2_minimal_table();

        let file_a = make_data_file(&table, "data/a.parquet", 10);
        let file_b = make_data_file(&table, "data/b.parquet", 20);

        let table_with_files =
            append_files(table.clone(), vec![file_a.clone(), file_b.clone()]).await;

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

        let table_after_a = append_files(table.clone(), vec![file_a.clone()]).await;
        let table_with_files = append_files(table_after_a, vec![file_b]).await;

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

        let new_snapshot = unwrap_add_snapshot(&updates);
        let alive_files = collect_alive_files(new_snapshot, &table_with_files).await;

        assert!(
            alive_files.contains(&"data/b.parquet".to_string()),
            "{alive_files:?}"
        );
        assert!(
            alive_files.contains(&"data/compacted.parquet".to_string()),
            "{alive_files:?}"
        );
        assert!(
            !alive_files.contains(&"data/a.parquet".to_string()),
            "{alive_files:?}"
        );
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

        assert!(
            alive_files.contains(&"data/b.parquet".to_string()),
            "{alive_files:?}"
        );
        assert!(
            alive_files.contains(&"data/d.parquet".to_string()),
            "{alive_files:?}"
        );
        assert!(
            alive_files.contains(&"data/compacted.parquet".to_string()),
            "{alive_files:?}"
        );
        assert!(
            !alive_files.contains(&"data/a.parquet".to_string()),
            "{alive_files:?}"
        );
        assert!(
            !alive_files.contains(&"data/c.parquet".to_string()),
            "{alive_files:?}"
        );
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
        let table1 = append_files(table.clone(), vec![file1.clone()]).await;

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

        let new_snapshot = unwrap_add_snapshot(&updates);

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
        let updates = Arc::new(r1).commit(&table).await.unwrap().take_updates();
        let table = apply_updates_to_table(&table, &updates);

        let new2 = make_data_file(&table, "data/new2.parquet", 10);
        let tx = Transaction::new(&table);
        let r2 = tx
            .rewrite_files()
            .delete_files(vec![f1])
            .add_files(vec![new2]);
        let Err(err) = Arc::new(r2).commit(&table).await else {
            panic!("rewriting an already-deleted file must error");
        };
        assert!(err.to_string().contains("no longer alive"));
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
        let updates = Arc::new(action).commit(&t).await.unwrap().take_updates();
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
        assert!(
            post_count <= pre_count,
            "pre={pre_count}, post={post_count}"
        );

        let snap = t.metadata().current_snapshot().unwrap();
        let alive = collect_alive_files(snap, &t).await;
        for expected in [
            "data/c1.parquet",
            "data/f2.parquet",
            "data/f3.parquet",
            "data/f4.parquet",
        ] {
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
        let stays_seq = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number();

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

    #[tokio::test]
    async fn test_rewrite_idempotent_replaced_count() {
        let table = make_v2_minimal_table();
        let f1 = make_data_file(&table, "data/f1.parquet", 10);
        let table = append_files(table, vec![f1.clone()]).await;

        let c1 = make_data_file(&table, "data/c1.parquet", 10);
        let action = Arc::new(
            Transaction::new(&table)
                .rewrite_files()
                .delete_files(vec![f1])
                .add_files(vec![c1]),
        );

        fn replaced_count(ac: &mut ActionCommit) -> String {
            for u in ac.take_updates() {
                if let TableUpdate::AddSnapshot { snapshot } = u
                    && let Some(v) = snapshot
                        .summary()
                        .additional_properties
                        .get("manifests-replaced")
                {
                    return v.clone();
                }
            }
            panic!("no manifests-replaced in snapshot summary");
        }

        let count1 = replaced_count(&mut action.clone().commit(&table).await.unwrap());
        let count2 = replaced_count(&mut action.commit(&table).await.unwrap());
        assert_eq!(
            count1, count2,
            "manifests-replaced must be stable across retries"
        );
    }

    #[tokio::test]
    async fn test_parallel_rewrites_targeting_same_file_rejected() {
        // Compaction A and Compaction B both plan against snapshot S0, both
        // target file F. A commits first. B's validation must see that F is no
        // longer alive in the current snapshot (because A replaced it) and
        // reject B's commit.
        let mut table = make_v2_minimal_table();
        let f1 = make_data_file(&table, "data/f1.parquet", 10);
        table = append_files(table, vec![f1.clone()]).await;

        let s0_id = table.metadata().current_snapshot().unwrap().snapshot_id();

        // Compaction A
        let new_a = make_data_file(&table, "data/new_a.parquet", 10);
        let action_a = Arc::new(
            Transaction::new(&table)
                .rewrite_files()
                .delete_files(vec![f1.clone()])
                .add_files(vec![new_a])
                .validate_from_snapshot(s0_id),
        );

        // Compaction B
        let new_b = make_data_file(&table, "data/new_b.parquet", 10);
        let action_b = Arc::new(
            Transaction::new(&table)
                .rewrite_files()
                .delete_files(vec![f1.clone()])
                .add_files(vec![new_b])
                .validate_from_snapshot(s0_id),
        );

        // A commits first
        let updates_a = action_a.commit(&table).await.unwrap().take_updates();
        table = apply_updates_to_table(&table, &updates_a);

        // B attempts to commit against the new table state
        let Err(err) = action_b.commit(&table).await else {
            panic!("Parallel rewrite targeting same file must be rejected");
        };

        assert!(
            err.to_string()
                .contains("files being replaced are no longer alive"),
            "Error was: {err}"
        );
    }

    #[tokio::test]
    async fn test_parallel_rewrites_disjoint_files_both_succeed() {
        let mut table = make_v2_minimal_table();
        let f1 = make_data_file(&table, "data/f1.parquet", 10);
        let f2 = make_data_file(&table, "data/f2.parquet", 10);
        table = append_files(table, vec![f1.clone(), f2.clone()]).await;

        let s0_id = table.metadata().current_snapshot().unwrap().snapshot_id();

        // Compaction A targets f1
        let new_a = make_data_file(&table, "data/new_a.parquet", 10);
        let action_a = Arc::new(
            Transaction::new(&table)
                .rewrite_files()
                .delete_files(vec![f1])
                .add_files(vec![new_a])
                .validate_from_snapshot(s0_id),
        );

        // Compaction B targets f2
        let new_b = make_data_file(&table, "data/new_b.parquet", 10);
        let action_b = Arc::new(
            Transaction::new(&table)
                .rewrite_files()
                .delete_files(vec![f2])
                .add_files(vec![new_b])
                .validate_from_snapshot(s0_id),
        );

        // A commits first
        let updates_a = action_a.commit(&table).await.unwrap().take_updates();
        table = apply_updates_to_table(&table, &updates_a);

        // B attempts to commit against the new table state and should succeed
        // because f2 is still alive.
        let result_b = action_b.commit(&table).await;
        assert!(
            result_b.is_ok(),
            "Parallel disjoint rewrite failed: {:?}",
            result_b.err()
        );
    }

    #[tokio::test]
    async fn test_merge_size_guard_exempts_new_manifest_bin() {
        // Guard must exempt the bin containing the NEW commit's manifest, not a
        // carry-forward. With min-count-to-merge=3 the guard fires on any bin with
        // fewer than 3 entries. We build 2 carry-forward manifests that pack into
        // one bin together, then do a rewrite that produces a third (new) manifest
        // in its own bin. The carry-forward bin must be merged; the new manifest's
        // bin (size=1) must pass through.
        let mut table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let prop_action = tx
            .update_table_properties()
            .set(
                "commit.manifest.min-count-to-merge".to_string(),
                "3".to_string(),
            )
            // Large target so all manifests would naturally fit in one bin;
            // we rely on the guard, not bin overflow, to exempt the new manifest.
            .set(
                "commit.manifest.target-size-bytes".to_string(),
                "104857600".to_string(),
            );
        let updates = Arc::new(prop_action)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        table = apply_updates_to_table(&table, &updates);

        // Two carry-forward appends — each produces one manifest.
        let f1 = make_data_file(&table, "data/f1.parquet", 10);
        let f2 = make_data_file(&table, "data/f2.parquet", 10);
        let table = append_files(table, vec![f1.clone()]).await;
        let table = append_files(table, vec![f2.clone()]).await;

        let pre_count = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap()
            .entries()
            .len();
        assert_eq!(pre_count, 2, "expected 2 carry-forward manifests");

        // Rewrite f1 → c1. This adds one new data manifest.
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

        let snap = table.metadata().current_snapshot().unwrap();
        let manifest_list = snap
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        // The 2 carry-forward data manifests have < 3 entries but are NOT the new
        // manifest's bin, so the guard must NOT protect them — they should be merged
        // into 1. The new data manifest's bin (1 entry) is protected by the guard
        // and passes through. Post-commit: 1 merged carry-forward + 1 new = 2 total.
        let manifest_count = manifest_list.entries().len();
        assert!(
            manifest_count <= pre_count + 1,
            "carry-forwards should have been merged; got {manifest_count} manifests (pre={pre_count})"
        );

        // All files must still be alive.
        let alive = collect_alive_files(snap, &table).await;
        for expected in ["data/c1.parquet", "data/f2.parquet"] {
            assert!(
                alive.contains(&expected.to_string()),
                "missing {expected}; alive={alive:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_merge_size_guard_with_min_count_above_two() {
        // min-count-to-merge=3 means the guard fires on bins of size 1 or 2.
        // Current tests use min-count=2, which causes the guard to only fire on
        // empty bins (impossible) — the guard is bypassed entirely. This test
        // explicitly exercises the guard path.
        let mut table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let prop_action = tx
            .update_table_properties()
            .set(
                "commit.manifest.min-count-to-merge".to_string(),
                "3".to_string(),
            )
            .set(
                "commit.manifest.target-size-bytes".to_string(),
                "104857600".to_string(),
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
        let mut t = table;
        // Three separate appends → three separate carry-forward manifests.
        for f in [&f1, &f2, &f3] {
            t = append_files(t, vec![f.clone()]).await;
        }

        // Now rewrite f1. The new data manifest lands in its own bin (size=1),
        // which the guard exempts. The three carry-forwards pack together (size=3),
        // which meets min-count-to-merge=3 so they ARE merged.
        let c1 = make_data_file(&t, "data/c1.parquet", 10);
        let tx = Transaction::new(&t);
        let action = tx
            .rewrite_files()
            .delete_files(vec![f1])
            .add_files(vec![c1.clone()]);
        let updates = Arc::new(action).commit(&t).await.unwrap().take_updates();
        let t = apply_updates_to_table(&t, &updates);

        let snap = t.metadata().current_snapshot().unwrap();
        let alive = collect_alive_files(snap, &t).await;
        for expected in ["data/c1.parquet", "data/f2.parquet", "data/f3.parquet"] {
            assert!(
                alive.contains(&expected.to_string()),
                "missing {expected}; alive={alive:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_steady_state_appends_do_not_trigger_historical_rewrite() {
        // Behavioral guarantee: a new manifest that lands in a small bin must NOT
        // cause adjacent historical bins to be rewritten. We verify that historical
        // manifests from prior appends are merged among themselves (as expected)
        // while the new commit's manifest survives intact (not merged away).
        let mut table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let prop_action = tx
            .update_table_properties()
            .set(
                "commit.manifest.min-count-to-merge".to_string(),
                "3".to_string(),
            )
            .set(
                "commit.manifest.target-size-bytes".to_string(),
                "104857600".to_string(),
            );
        let updates = Arc::new(prop_action)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        table = apply_updates_to_table(&table, &updates);

        // Build up 4 carry-forward manifests via separate appends.
        let mut t = table;
        let mut carry_files = Vec::new();
        for i in 0..4usize {
            let f = make_data_file(&t, &format!("data/carry_{i}.parquet"), 10);
            carry_files.push(f.clone());
            t = append_files(t, vec![f]).await;
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
        assert_eq!(pre_count, 4);

        // A single rewrite: replace carry_0 with a new file.
        let new_f = make_data_file(&t, "data/new.parquet", 10);
        let tx = Transaction::new(&t);
        let action = tx
            .rewrite_files()
            .delete_files(vec![carry_files[0].clone()])
            .add_files(vec![new_f.clone()]);
        let updates = Arc::new(action).commit(&t).await.unwrap().take_updates();
        let t = apply_updates_to_table(&t, &updates);

        let snap = t.metadata().current_snapshot().unwrap();
        let post_count = snap
            .load_manifest_list(t.file_io(), t.metadata())
            .await
            .unwrap()
            .entries()
            .len();

        // Carry-forwards (4 manifests) were eligible to merge (≥3); they should
        // consolidate. The new commit adds 1 data manifest. Total must
        // be well below pre_count + 2 (no merge at all).
        assert!(
            post_count < pre_count + 2,
            "historical manifests should have been merged; pre={pre_count}, post={post_count}"
        );

        // All surviving files must be alive.
        let alive = collect_alive_files(snap, &t).await;
        assert!(
            alive.contains(&"data/new.parquet".to_string()),
            "alive={alive:?}"
        );
        for i in 1..4usize {
            let path = format!("data/carry_{i}.parquet");
            assert!(alive.contains(&path), "missing {path}; alive={alive:?}");
        }
    }

    #[tokio::test]
    async fn test_rewrite_action_manifest_count_does_not_grow() {
        // Regression: each compaction cycle must produce exactly 1 manifest.
        // With the fix, delete_entries returns empty so no tombstone manifest
        // is written; prior bloat was +1 manifest per compaction run.
        let table = make_v2_minimal_table();

        let f1 = make_data_file(&table, "data/f1.parquet", 10);
        let f2 = make_data_file(&table, "data/f2.parquet", 10);
        let mut t = append_files(table, vec![f1.clone(), f2.clone()]).await;
        let mut current = vec![f1, f2];

        for i in 0..3usize {
            let compacted = make_data_file(&t, &format!("data/c{i}.parquet"), 20);
            let tx = Transaction::new(&t);
            let action = tx
                .rewrite_files()
                .delete_files(current.clone())
                .add_files(vec![compacted.clone()]);
            let updates = Arc::new(action).commit(&t).await.unwrap().take_updates();
            t = apply_updates_to_table(&t, &updates);

            let snap = t.metadata().current_snapshot().unwrap();
            let count = snap
                .load_manifest_list(t.file_io(), t.metadata())
                .await
                .unwrap()
                .entries()
                .len();
            assert_eq!(
                count, 1,
                "cycle {i}: manifest count should be 1, got {count}"
            );

            current = vec![compacted];
        }
    }

    #[tokio::test]
    async fn test_filter_manifests_drops_existing_tombstone_manifest() {
        // Verify that a tombstone-only manifest injected from a hypothetical
        // prior buggy compaction run is cleaned up by the merge pass when the
        // next compaction runs. With min-count-to-merge=2 the bin containing
        // [tombstone, new_added] (size=2) is merged; the merge pass drops
        // prior-snapshot Deleted entries, so the tombstone path disappears.
        use std::collections::HashMap;

        use crate::spec::{
            ManifestListWriter, ManifestWriterBuilder, SnapshotReference, SnapshotRetention,
            Summary,
        };

        let mut table = make_v2_minimal_table();

        // min-count-to-merge=2 ensures a bin of [tombstone + new_added] (size=2)
        // is merged rather than guarded.
        let tx = Transaction::new(&table);
        let prop = tx.update_table_properties().set(
            "commit.manifest.min-count-to-merge".to_string(),
            "2".to_string(),
        );
        let updates = Arc::new(prop).commit(&table).await.unwrap().take_updates();
        table = apply_updates_to_table(&table, &updates);

        let f1 = make_data_file(&table, "data/f1.parquet", 10);
        let mut table = append_files(table, vec![f1.clone()]).await;

        let prior_snap = table.metadata().current_snapshot().unwrap();
        let prior_snap_id = prior_snap.snapshot_id();
        let prior_seq_num = prior_snap.sequence_number();

        let existing_manifests: Vec<crate::spec::ManifestFile> = prior_snap
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap()
            .entries()
            .to_vec();

        // Write a tombstone Avro manifest simulating a prior buggy compaction.
        let tombstone_path = format!(
            "{}/metadata/tombstone-test-m0.avro",
            table.metadata().location()
        );
        let output = table.file_io().new_output(&tombstone_path).unwrap();
        let mut writer = ManifestWriterBuilder::new(
            output,
            Some(prior_snap_id),
            None,
            table.metadata().current_schema().clone(),
            table.metadata().default_partition_spec().as_ref().clone(),
        )
        .build_v2_data();

        writer
            .add_delete_file(f1.clone(), prior_seq_num, Some(prior_seq_num))
            .unwrap();
        let mut tombstone_mf = writer.write_manifest_file().await.unwrap();

        // Set concrete sequence numbers so ManifestListWriter skips the
        // added_snapshot_id consistency check (our injected snap has a different id).
        tombstone_mf.sequence_number = prior_seq_num;
        tombstone_mf.min_sequence_number = prior_seq_num;

        // Write a new manifest list containing the original data manifests
        // plus the synthetic tombstone manifest.
        let injected_snap_id = prior_snap_id + 1_000_000;
        let injected_seq_num = prior_seq_num + 1;
        let injected_ml_path = format!(
            "{}/metadata/snap-injected.avro",
            table.metadata().location()
        );
        let ml_output = table.file_io().new_output(&injected_ml_path).unwrap();
        let mut ml_writer = ManifestListWriter::v2(
            ml_output,
            injected_snap_id,
            Some(prior_snap_id),
            injected_seq_num,
        );
        let mut all_manifests = existing_manifests;
        all_manifests.push(tombstone_mf.clone());
        ml_writer.add_manifests(all_manifests.into_iter()).unwrap();
        ml_writer.close().await.unwrap();

        // Build a synthetic snapshot pointing to the injected manifest list.
        let injected_snap = crate::spec::Snapshot::builder()
            .with_snapshot_id(injected_snap_id)
            .with_parent_snapshot_id(Some(prior_snap_id))
            .with_sequence_number(injected_seq_num)
            .with_timestamp_ms(prior_snap.timestamp_ms() + 1)
            .with_manifest_list(injected_ml_path)
            .with_summary(Summary {
                operation: Operation::Replace,
                additional_properties: HashMap::new(),
            })
            .build();

        let injected_updates = vec![
            TableUpdate::AddSnapshot {
                snapshot: injected_snap,
            },
            TableUpdate::SetSnapshotRef {
                ref_name: MAIN_BRANCH.to_string(),
                reference: SnapshotReference::new(injected_snap_id, SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                }),
            },
        ];
        table = apply_updates_to_table(&table, &injected_updates);

        // Confirm tombstone is present before compaction.
        let pre_ml = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert!(
            pre_ml
                .entries()
                .iter()
                .any(|e| e.manifest_path == tombstone_mf.manifest_path),
            "tombstone should be present before compaction"
        );

        // Compact: replace f1 with c1.
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

        // Tombstone path must be absent from the new snapshot's manifest list.
        let snap = table.metadata().current_snapshot().unwrap();
        let post_ml = snap
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert!(
            !post_ml
                .entries()
                .iter()
                .any(|e| e.manifest_path == tombstone_mf.manifest_path),
            "tombstone manifest must be gone after compaction (merged away by merge pass)"
        );
    }

    #[tokio::test]
    async fn test_occ_retry_stable_ids() {
        let table = make_v2_minimal_table();
        let f1 = make_data_file(&table, "data/f1.parquet", 10);
        let table = append_files(table, vec![f1.clone()]).await;
        let c1 = make_data_file(&table, "data/c1.parquet", 10);

        let action = Arc::new(
            Transaction::new(&table)
                .rewrite_files()
                .delete_files(vec![f1])
                .add_files(vec![c1]),
        );

        let updates1 = action.clone().commit(&table).await.unwrap().take_updates();
        let updates2 = action.commit(&table).await.unwrap().take_updates();

        assert_eq!(
            snapshot_id_from(&updates1),
            snapshot_id_from(&updates2),
            "snapshot_id must be stable across OCC retries"
        );
        assert_eq!(
            commit_uuid_from(&updates1),
            commit_uuid_from(&updates2),
            "commit_uuid must be stable across OCC retries"
        );
    }
}
