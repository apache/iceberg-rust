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
use crate::spec::{DataFile, ManifestContentType, ManifestEntry, ManifestFile, Operation};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};

/// Transaction action for Copy-on-Write row-level modifications (UPDATE, DELETE, MERGE INTO).
///
/// Corresponds to `org.apache.iceberg.RowDelta` in the Java implementation.
pub struct RowDeltaAction {
    added_data_files: Vec<DataFile>,
    removed_data_files: Vec<DataFile>,
    /// MoR delete files (position/equality deletes, incl. V3 deletion vectors) to add.
    added_delete_files: Vec<DataFile>,
    commit_uuid: Option<Uuid>,
    snapshot_properties: HashMap<String, String>,
    starting_snapshot_id: Option<i64>,
}

impl RowDeltaAction {
    pub(crate) fn new() -> Self {
        Self {
            added_data_files: vec![],
            removed_data_files: vec![],
            added_delete_files: vec![],
            commit_uuid: None,
            snapshot_properties: HashMap::default(),
            starting_snapshot_id: None,
        }
    }

    /// Add new data files (INSERT rows or Copy-on-Write rewritten files).
    pub fn add_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(data_files);
        self
    }

    /// Mark existing data files as deleted (Copy-on-Write mode).
    ///
    /// Corresponds to `removeRows(DataFile)` in the Java implementation.
    pub fn remove_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.removed_data_files.extend(data_files);
        self
    }

    /// Add Merge-on-Read delete files (position/equality deletes, incl. V3 deletion
    /// vectors). Written into a content=Deletes manifest at commit time.
    pub fn add_delete_files(mut self, delete_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_delete_files.extend(delete_files);
        self
    }

    /// Set the commit UUID used for manifest file naming.
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Attach custom key/value metadata to the snapshot summary.
    pub fn set_snapshot_properties(mut self, snapshot_properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }

    /// Reject the commit if the table has advanced past `snapshot_id` (optimistic concurrency).
    pub fn validate_from_snapshot(mut self, snapshot_id: i64) -> Self {
        self.starting_snapshot_id = Some(snapshot_id);
        self
    }
}

#[async_trait]
impl TransactionAction for RowDeltaAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        if let Some(expected_snapshot_id) = self.starting_snapshot_id
            && table.metadata().current_snapshot_id() != Some(expected_snapshot_id)
        {
            return Err(crate::Error::new(
                crate::ErrorKind::DataInvalid,
                format!(
                    "Cannot commit RowDelta based on stale snapshot. Expected: {}, Current: {:?}",
                    expected_snapshot_id,
                    table.metadata().current_snapshot_id()
                ),
            ));
        }

        let mut snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            None,
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
        );

        // Validate newly added data files (partition value type-checks, etc.).
        // removed_data_files are not re-validated: they are existing table files that were
        // already validated when originally committed. This matches Java's MergingSnapshotProducer.
        snapshot_producer.validate_added_data_files()?;

        // MoR delete files (position/equality deletes, incl. V3 deletion vectors) are
        // written into a separate content=Deletes manifest by the snapshot producer.
        snapshot_producer.set_added_delete_files(self.added_delete_files.clone());

        let operation = RowDeltaOperation {
            removed_data_files: self.removed_data_files.clone(),
            has_added_data_files: !self.added_data_files.is_empty(),
            has_added_delete_files: !self.added_delete_files.is_empty(),
        };

        snapshot_producer
            .commit(operation, DefaultManifestProcess)
            .await
    }
}

struct RowDeltaOperation {
    removed_data_files: Vec<DataFile>,
    has_added_data_files: bool,
    has_added_delete_files: bool,
}

impl SnapshotProduceOperation for RowDeltaOperation {
    /// Operation type (mirrors Java `BaseRowDelta.operation()`):
    /// - Any data files removed → `Overwrite`
    /// - MoR delete files added → `Overwrite` if data files also added, else `Delete`
    /// - Only data files added (or nothing) → `Append`
    fn operation(&self) -> Operation {
        if !self.removed_data_files.is_empty() {
            Operation::Overwrite
        } else if self.has_added_delete_files {
            if self.has_added_data_files {
                Operation::Overwrite
            } else {
                Operation::Delete
            }
        } else {
            Operation::Append
        }
    }

    /// Delete entries are handled inside `existing_manifest` by rewriting the manifest.
    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        Ok(vec![])
    }

    /// Returns manifest files for the new snapshot.
    ///
    /// For each manifest in the previous snapshot:
    /// - If it contains any file being removed: rewrite it with DELETED entries for removed files
    ///   and EXISTING entries for survivors, preserving original sequence numbers.
    /// - Otherwise: carry it forward unchanged.
    ///
    /// This matches Java's `ManifestFilterManager.filterManifestWithDeletedFiles` logic.
    async fn existing_manifest(
        &self,
        snapshot_produce: &mut SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        let Some(snapshot) = snapshot_produce.table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot_produce
            .table
            .manifest_list_reader(snapshot)
            .load()
            .await?;

        let deleted_paths: HashSet<&str> = self
            .removed_data_files
            .iter()
            .map(|f| f.file_path())
            .collect();

        let mut result = Vec::new();
        for manifest_file in manifest_list.entries() {
            if !manifest_file.has_added_files() && !manifest_file.has_existing_files() {
                continue;
            }

            let manifest = manifest_file
                .load_manifest(snapshot_produce.table.file_io())
                .await?;

            let needs_rewrite = manifest
                .entries()
                .iter()
                .any(|e| e.is_alive() && deleted_paths.contains(e.data_file().file_path()));

            if !needs_rewrite {
                result.push(manifest_file.clone());
                continue;
            }

            // Rewrite: deleted files → DELETED (new snapshot_id, original seq nums preserved),
            // surviving files → EXISTING (all original fields preserved).
            let mut writer =
                snapshot_produce.new_manifest_writer(ManifestContentType::Data)?;
            for entry in manifest.entries() {
                if deleted_paths.contains(entry.data_file().file_path()) {
                    writer.add_delete_entry((**entry).clone())?;
                } else {
                    writer.add_existing_entry((**entry).clone())?;
                }
            }
            result.push(writer.write_manifest_file().await?);
        }

        Ok(result)
    }

    fn removed_data_files(&self) -> &[DataFile] {
        &self.removed_data_files
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, MAIN_BRANCH,
        ManifestStatus, Struct, TableMetadataBuilder,
    };
    use crate::table::Table;
    use crate::transaction::tests::make_v2_minimal_table;
    use crate::transaction::{Transaction, TransactionAction};
    use crate::{TableIdent, TableUpdate};

    fn make_data_file(table: &Table, path: &str, size: u64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(size)
            .record_count(10)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(100))]))
            .build()
            .unwrap()
    }

    /// Build a table that has `snapshot` as its current snapshot, backed by the same FileIO.
    async fn table_with_snapshot(base: &Table, snapshot: crate::spec::Snapshot) -> Table {
        let updated_metadata = TableMetadataBuilder::new_from_metadata(
            base.metadata_ref().as_ref().clone(),
            None,
        )
        .set_branch_snapshot(snapshot, MAIN_BRANCH)
        .unwrap()
        .build()
        .unwrap()
        .metadata;

        Table::builder()
            .metadata(updated_metadata)
            .metadata_location("s3://bucket/test/location/metadata/v2.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(base.file_io().clone())
            .runtime(crate::test_utils::test_runtime())
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_row_delta_add_only() {
        let table = make_v2_minimal_table();
        let data_file = make_data_file(&table, "test/1.parquet", 100);
        let action = Transaction::new(&table)
            .row_delta()
            .add_data_files(vec![data_file]);

        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = commit.take_updates();

        if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            assert_eq!(snapshot.summary().operation, crate::spec::Operation::Append);
        } else {
            panic!("expected AddSnapshot");
        }
    }

    #[tokio::test]
    async fn test_row_delta_with_snapshot_properties() {
        let table = make_v2_minimal_table();
        let data_file = make_data_file(&table, "test/1.parquet", 100);
        let mut props = std::collections::HashMap::new();
        props.insert("key".to_string(), "value".to_string());
        let action = Transaction::new(&table)
            .row_delta()
            .set_snapshot_properties(props)
            .add_data_files(vec![data_file]);

        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = commit.take_updates();

        if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            assert_eq!(
                snapshot.summary().additional_properties.get("key").unwrap(),
                "value"
            );
        } else {
            panic!("expected AddSnapshot");
        }
    }

    #[tokio::test]
    async fn test_row_delta_validate_from_snapshot() {
        let table = make_v2_minimal_table();
        let data_file = make_data_file(&table, "test/1.parquet", 100);
        let action = Transaction::new(&table)
            .row_delta()
            .validate_from_snapshot(99999)
            .add_data_files(vec![data_file]);

        let result = Arc::new(action).commit(&table).await;
        match result {
            Ok(_) => panic!("expected DataInvalid error for stale snapshot"),
            Err(e) => assert_eq!(e.kind(), crate::ErrorKind::DataInvalid),
        }
    }

    #[tokio::test]
    async fn test_row_delta_empty_action() {
        let table = make_v2_minimal_table();
        assert!(
            Arc::new(Transaction::new(&table).row_delta())
                .commit(&table)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_row_delta_incompatible_partition_value() {
        let table = make_v2_minimal_table();
        let bad_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/bad.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::string("wrong"))]))
            .build()
            .unwrap();
        let action = Transaction::new(&table)
            .row_delta()
            .add_data_files(vec![bad_file]);
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    /// MoR: adding a position-delete file via RowDelta commits a content=Deletes
    /// manifest and an `Operation::Delete` snapshot (replaces the old "errors" test
    /// now that `add_delete_files` is implemented).
    #[tokio::test]
    async fn test_row_delta_add_delete_files_mor() {
        let base = make_v2_minimal_table();

        // S1: append a data file.
        let data_file = make_data_file(&base, "test/data.parquet", 100);
        let mut c1 = Arc::new(
            Transaction::new(&base)
                .fast_append()
                .add_data_files(vec![data_file]),
        )
        .commit(&base)
        .await
        .unwrap();
        let snap_s1 = if let TableUpdate::AddSnapshot { snapshot } =
            c1.take_updates().into_iter().next().unwrap()
        {
            snapshot
        } else {
            panic!("expected AddSnapshot");
        };
        let table_s1 = table_with_snapshot(&base, snap_s1).await;

        // S2: add a MoR position-delete file referencing the data file.
        let delete_file = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/pos-delete.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(50)
            .record_count(3)
            .partition_spec_id(table_s1.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(100))]))
            .referenced_data_file(Some("test/data.parquet".to_string()))
            .build()
            .unwrap();
        let mut c2 = Arc::new(
            Transaction::new(&table_s1)
                .row_delta()
                .add_delete_files(vec![delete_file]),
        )
        .commit(&table_s1)
        .await
        .unwrap();
        let updates2 = c2.take_updates();
        let snap_s2 = if let TableUpdate::AddSnapshot { ref snapshot } = updates2[0] {
            snapshot
        } else {
            panic!("expected AddSnapshot");
        };

        // Only delete files added (no data adds/removes) → Operation::Delete.
        assert_eq!(snap_s2.summary().operation, crate::spec::Operation::Delete);

        // A PositionDeletes entry must exist in the new snapshot's manifests.
        let manifest_list = table_s1
            .manifest_list_reader(&std::sync::Arc::new(snap_s2.clone()))
            .load()
            .await
            .unwrap();
        let mut found_position_delete = false;
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file
                .load_manifest(table_s1.file_io())
                .await
                .unwrap();
            for entry in manifest.entries() {
                if entry.data_file().content_type() == DataContentType::PositionDeletes {
                    found_position_delete = true;
                }
            }
        }
        assert!(
            found_position_delete,
            "expected a PositionDeletes entry in the RowDelta snapshot's manifests"
        );
    }

    /// End-to-end CoW test: append two files, then remove one via RowDelta.
    ///
    /// Verifies:
    /// - The removed file appears as DELETED with correct sequence numbers.
    /// - The surviving file appears as EXISTING with correct sequence numbers.
    /// - The new file appears as ADDED.
    /// - The snapshot summary counts `deleted-data-files = 1`.
    #[tokio::test]
    async fn test_row_delta_cow_manifest_rewrite() {
        let base_table = make_v2_minimal_table();

        // --- S1: append file-A and file-B ---
        let file_a = make_data_file(&base_table, "test/a.parquet", 100);
        let file_b = make_data_file(&base_table, "test/b.parquet", 200);

        let action1 = Transaction::new(&base_table)
            .fast_append()
            .add_data_files(vec![file_a.clone(), file_b.clone()]);
        let mut commit1 = Arc::new(action1).commit(&base_table).await.unwrap();
        let updates1 = commit1.take_updates();

        let snapshot_s1 = if let TableUpdate::AddSnapshot { snapshot } = updates1.into_iter().next().unwrap() {
            snapshot
        } else {
            panic!("expected AddSnapshot");
        };

        let table_s1 = table_with_snapshot(&base_table, snapshot_s1).await;

        // --- S2: remove file-A (CoW), add file-C ---
        let file_c = make_data_file(&table_s1, "test/c.parquet", 300);
        let action2 = Transaction::new(&table_s1)
            .row_delta()
            .remove_data_files(vec![file_a.clone()])
            .add_data_files(vec![file_c.clone()]);
        let mut commit2 = Arc::new(action2).commit(&table_s1).await.unwrap();
        let updates2 = commit2.take_updates();

        let snapshot_s2 = if let TableUpdate::AddSnapshot { ref snapshot } = updates2[0] {
            snapshot
        } else {
            panic!("expected AddSnapshot");
        };

        assert_eq!(
            snapshot_s2.summary().operation,
            crate::spec::Operation::Overwrite
        );

        // Verify snapshot summary metrics
        let props = &snapshot_s2.summary().additional_properties;
        assert_eq!(
            props.get("deleted-data-files").map(String::as_str),
            Some("1"),
            "summary should count 1 deleted file"
        );

        // Scan all manifest entries in S2
        let manifest_list = table_s1
            .manifest_list_reader(&std::sync::Arc::new(snapshot_s2.clone()))
            .load()
            .await
            .unwrap();

        let mut found_deleted_a = false;
        let mut found_existing_b = false;
        let mut found_added_c = false;

        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file
                .load_manifest(table_s1.file_io())
                .await
                .unwrap();
            for entry in manifest.entries() {
                match entry.data_file().file_path() {
                    "test/a.parquet" => {
                        assert_eq!(
                            entry.status(),
                            ManifestStatus::Deleted,
                            "file-A must be DELETED"
                        );
                        assert!(
                            entry.sequence_number().is_some(),
                            "DELETED entry must have sequence number"
                        );
                        assert!(
                            entry.file_sequence_number.is_some(),
                            "DELETED entry must have file sequence number"
                        );
                        found_deleted_a = true;
                    }
                    "test/b.parquet" => {
                        assert_eq!(
                            entry.status(),
                            ManifestStatus::Existing,
                            "file-B must be EXISTING"
                        );
                        assert!(
                            entry.sequence_number().is_some(),
                            "EXISTING entry must have sequence number"
                        );
                        found_existing_b = true;
                    }
                    "test/c.parquet" => {
                        found_added_c = true;
                    }
                    other => panic!("unexpected file in S2 manifests: {other}"),
                }
            }
        }

        assert!(found_deleted_a, "file-A should have a DELETED entry in S2");
        assert!(found_existing_b, "file-B should have an EXISTING entry in S2");
        assert!(found_added_c, "file-C should have an ADDED entry in S2");
    }
}
