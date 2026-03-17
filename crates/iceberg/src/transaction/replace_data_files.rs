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
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{DataFile, ManifestEntry, ManifestFile, ManifestStatus, Operation};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind};

/// Action to replace data files in a table (for compaction/rewrite operations).
pub struct ReplaceDataFilesAction {
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
}

impl ReplaceDataFilesAction {
    pub(crate) fn new() -> Self {
        Self {
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
            files_to_delete: vec![],
            files_to_add: vec![],
            validate_from_snapshot_id: None,
            data_sequence_number: None,
        }
    }

    /// Add files to delete (old files being replaced).
    ///
    /// Every manifest that contains a file being deleted must contain **only** files being deleted
    /// (no surviving files). If a manifest mixes files-to-delete with files-to-keep, this action
    /// returns [`ErrorKind::DataInvalid`]. This is a Phase 1 interim constraint; Phase 2 will
    /// rewrite mixed manifests transparently using `ManifestWriter::add_existing_entry`.
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
impl TransactionAction for ReplaceDataFilesAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        if self.files_to_add.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Replace operation requires files to add",
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
        let mut snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
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

        let replace_op = ReplaceOperation {
            files_to_delete: self
                .files_to_delete
                .iter()
                .map(|f| f.file_path.clone())
                .collect(),
            validate_from_snapshot_id: self.validate_from_snapshot_id,
        };

        snapshot_producer
            .commit(replace_op, DefaultManifestProcess)
            .await
    }
}

struct ReplaceOperation {
    files_to_delete: HashSet<String>,
    validate_from_snapshot_id: Option<i64>,
}

impl SnapshotProduceOperation for ReplaceOperation {
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

        let snapshot = if let Some(snap_id) = self.validate_from_snapshot_id {
            snapshot_produce
                .table
                .metadata()
                .snapshot_by_id(snap_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Snapshot not found for validate_from_snapshot: {snap_id}"),
                    )
                })?
        } else {
            snapshot_produce
                .table
                .metadata()
                .current_snapshot()
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Cannot delete files from a table with no snapshots",
                    )
                })?
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_produce.table.file_io(),
                &snapshot_produce.table.metadata_ref(),
            )
            .await?;

        let mut deleted_entries = Vec::new();
        let mut found_paths = HashSet::new();
        for entry in manifest_list.entries() {
            let manifest = entry
                .load_manifest(snapshot_produce.table.file_io())
                .await?;
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
            let snapshot_label = match self.validate_from_snapshot_id {
                Some(id) => format!("snapshot {id}"),
                None => "current snapshot".to_string(),
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
        let Some(snapshot) = snapshot_produce.table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_produce.table.file_io(),
                &snapshot_produce.table.metadata_ref(),
            )
            .await?;

        let mut result = Vec::new();
        for entry in manifest_list.entries() {
            let manifest = entry
                .load_manifest(snapshot_produce.table.file_io())
                .await?;

            let has_deletions = manifest
                .entries()
                .iter()
                .any(|e| e.is_alive() && self.files_to_delete.contains(e.file_path()));

            if !has_deletions {
                result.push(entry.clone());
                continue;
            }

            let has_survivors = manifest
                .entries()
                .iter()
                .any(|e| e.is_alive() && !self.files_to_delete.contains(e.file_path()));

            if has_survivors {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Manifest '{}' contains files being deleted alongside files not being \
                         deleted; use delete_data_files with complete manifest sets",
                        entry.manifest_path
                    ),
                ));
            }
            // All alive entries in this manifest are being deleted; exclude it.
            // delete_entries() will mark them as Deleted in the new snapshot.
        }

        Ok(result)
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

    #[tokio::test]
    async fn test_replace_requires_files_to_add() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx.replace_data_files();
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_replace_add_file_already_in_snapshot_errors() {
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
            .replace_data_files()
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
    async fn test_replace_phantom_delete_errors() {
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
            .replace_data_files()
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
    async fn test_replace_duplicate_paths_in_delete_errors() {
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
            .replace_data_files()
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
    async fn test_replace_action_full_table() {
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
            .replace_data_files()
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
    async fn test_replace_action_partial() {
        // NOTE: this test covers "partial table replace" in the sense that one manifest survives
        // (file3's manifest is untouched) while another is fully deleted (file1+file2 share a
        // manifest and both are deleted). It does NOT test the mixed-manifest case — that is
        // covered by test_replace_mixed_manifest_errors below.
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
            .replace_data_files()
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
    async fn test_replace_then_fast_append_preserves_delete_manifest() {
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
            .replace_data_files()
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
    async fn test_replace_validate_from_snapshot_succeeds() {
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
            .replace_data_files()
            .validate_from_snapshot(planning_snapshot_id)
            .delete_files(vec![file1])
            .add_files(vec![compacted]);

        // Should succeed — file1 exists in the specified snapshot.
        assert!(Arc::new(replace).commit(&table1).await.is_ok());
    }

    #[tokio::test]
    async fn test_replace_validate_from_snapshot_unknown_id_errors() {
        // validate_from_snapshot with a non-existent snapshot ID should return an error.
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
            .replace_data_files()
            .validate_from_snapshot(999_999_999) // does not exist
            .delete_files(vec![file1])
            .add_files(vec![compacted]);

        let Err(err) = Arc::new(replace).commit(&table1).await else {
            panic!("expected error for unknown snapshot id");
        };
        assert!(
            err.to_string().contains("Snapshot not found"),
            "Expected snapshot-not-found error, got: {err}"
        );
    }

    // --- 014: data_sequence_number ---

    // --- Mixed manifest detection ---

    #[tokio::test]
    async fn test_replace_mixed_manifest_errors() {
        // file_a and file_b share one manifest; file_c is in its own manifest.
        // Deleting only file_a (not file_b) must return DataInvalid — mixed manifest.
        let table = make_v2_minimal_table();

        let file_a = make_data_file(&table, "data/a.parquet", 10);
        let file_b = make_data_file(&table, "data/b.parquet", 20);
        let file_c = make_data_file(&table, "data/c.parquet", 30);

        // Append A and B together — they share one manifest.
        let tx = Transaction::new(&table);
        let append_ab = tx
            .fast_append()
            .add_data_files(vec![file_a.clone(), file_b.clone()]);
        let updates_ab = Arc::new(append_ab)
            .commit(&table)
            .await
            .unwrap()
            .take_updates();
        let table_after_ab = apply_updates_to_table(&table, &updates_ab);

        // Append C separately — its own manifest.
        let tx = Transaction::new(&table_after_ab);
        let append_c = tx.fast_append().add_data_files(vec![file_c]);
        let updates_c = Arc::new(append_c)
            .commit(&table_after_ab)
            .await
            .unwrap()
            .take_updates();
        let table_with_files = apply_updates_to_table(&table_after_ab, &updates_c);

        // Compact: delete only A. B is in the same manifest — must error.
        let compacted = make_data_file(&table, "data/compacted.parquet", 10);
        let tx = Transaction::new(&table_with_files);
        let replace = tx
            .replace_data_files()
            .delete_files(vec![file_a])
            .add_files(vec![compacted]);
        let Err(err) = Arc::new(replace).commit(&table_with_files).await else {
            panic!("expected DataInvalid error for mixed manifest");
        };
        assert!(
            err.to_string()
                .contains("contains files being deleted alongside files not being deleted"),
            "Expected mixed-manifest error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_replace_full_manifest_delete_no_regression() {
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
            .replace_data_files()
            .delete_files(vec![file_a, file_b])
            .add_files(vec![compacted]);
        assert!(
            Arc::new(replace).commit(&table_with_files).await.is_ok(),
            "Full manifest delete should succeed"
        );
    }

    #[tokio::test]
    async fn test_replace_single_file_manifests_no_false_positive() {
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
            .replace_data_files()
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
    async fn test_replace_data_sequence_number_is_set_on_entries() {
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
            .replace_data_files()
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
