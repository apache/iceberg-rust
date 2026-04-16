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

use std::collections::HashMap;
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

/// RowDeltaAction handles both data file additions and deletions in a single snapshot.
/// This is the core transaction type for MERGE, UPDATE, DELETE operations.
///
/// Corresponds to `org.apache.iceberg.RowDelta` in the Java implementation.
///
/// # Copy-on-Write (COW) Strategy
///
/// For row-level modifications:
/// 1. Read target data files that contain rows to be modified
/// 2. Apply modifications (UPDATE/DELETE logic)
/// 3. Write modified rows to new data files via `add_data_files()`
/// 4. Mark original files as deleted via `remove_data_files()`
///
/// For inserts (NOT MATCHED in MERGE):
/// 1. Write new rows to data files
/// 2. Add files via `add_data_files()`
///
/// # Future: Merge-on-Read Strategy
///
/// The `add_delete_files()` method is reserved for future Merge-on-Read support, which uses
/// delete files instead of rewriting data files.
pub struct RowDeltaAction {
    /// New data files to add (for inserts or rewritten files in COW mode)
    added_data_files: Vec<DataFile>,
    /// Data files to mark as deleted (for COW mode when rewriting files)
    removed_data_files: Vec<DataFile>,
    /// Delete files to add (reserved for future Merge-on-Read mode support)
    added_delete_files: Vec<DataFile>,
    /// Optional commit UUID for manifest file naming
    commit_uuid: Option<Uuid>,
    /// Additional properties to add to snapshot summary
    snapshot_properties: HashMap<String, String>,
    /// Optional starting snapshot ID for conflict detection
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

    /// Add new data files to the snapshot. Used for:
    /// - New rows from INSERT operations
    /// - Rewritten data files in COW mode (after applying UPDATE/DELETE)
    pub fn add_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(data_files);
        self
    }

    /// Mark data files as deleted in the snapshot.
    /// Used in COW mode to mark original files as deleted when they've been rewritten with modifications.
    /// Corresponds to `removeRows(DataFile)` in Java implementation.
    pub fn remove_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.removed_data_files.extend(data_files);
        self
    }

    /// Add delete files to the snapshot (reserved for future Merge-on-Read mode).
    /// #Note: This is not yet implemented and is reserved for future Merge-on-Read optimization
    /// where delete files are used instead of rewriting data files.
    pub fn add_delete_files(mut self, delete_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_delete_files.extend(delete_files);
        self
    }

    /// Set commit UUID for the snapshot.
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set snapshot summary properties.
    pub fn set_snapshot_properties(mut self, snapshot_properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }

    /// Validate that the operation is applied on top of a specific snapshot.
    /// This can be used for conflict detection in concurrent modification scenarios.
    pub fn validate_from_snapshot(mut self, snapshot_id: i64) -> Self {
        self.starting_snapshot_id = Some(snapshot_id);
        self
    }
}

#[async_trait]
impl TransactionAction for RowDeltaAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        // Validate starting snapshot if specified
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

        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            None, // key_metadata - not used for row delta
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
        );

        // Validate newly added data files (partition value type-checks, etc.).
        // removed_data_files are not validated: they are existing table files that
        // were already validated when originally committed, so re-validating them
        // here would be redundant. This matches Java's MergingSnapshotProducer behavior.
        snapshot_producer.validate_added_data_files()?;

        // Create RowDeltaOperation with removed files
        let operation = RowDeltaOperation {
            removed_data_files: self.removed_data_files.clone(),
            added_delete_files: self.added_delete_files.clone(),
        };

        snapshot_producer
            .commit(operation, DefaultManifestProcess)
            .await
    }
}

/// Implements the snapshot production logic for RowDelta operations.
///
/// This determines:
/// - Which operation type is recorded (Append/Delete/Overwrite)
/// - Which manifest entries should be marked as deleted
/// - Which existing manifests should be carried forward
struct RowDeltaOperation {
    removed_data_files: Vec<DataFile>,
    added_delete_files: Vec<DataFile>,
}

impl SnapshotProduceOperation for RowDeltaOperation {
    /// Determine operation type based on what's being added/removed.
    ///
    /// Logic based on Java `BaseRowDelta.operation()`:
    /// - Only adds data files (no deletes or removes) → `Append`
    /// - Removes data files or has delete files, AND also adds data files → `Overwrite`
    /// - Only removes/deletes with no new data added → `Delete` (future: MoR path)
    ///
    /// Note: `Operation::Delete` is not yet returned because `add_delete_files` is
    /// not fully implemented. Once Merge-on-Read support is wired up, the operation
    /// will be `Delete` when only delete files are added with no new data rows.
    fn operation(&self) -> Operation {
        let has_added_deletes = !self.added_delete_files.is_empty();
        let has_removed_data = !self.removed_data_files.is_empty();

        if has_removed_data || has_added_deletes {
            Operation::Overwrite
        } else {
            Operation::Append
        }
    }

    /// Returns manifest entries for files that should be marked as deleted.
    /// This creates DELETED entries for removed data files in COW mode.
    async fn delete_entries(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        let snapshot_id = snapshot_produce.table.metadata().current_snapshot_id();

        // Create DELETED manifest entries for removed data files
        let deleted_entries: Vec<ManifestEntry> = self
            .removed_data_files
            .iter()
            .map(|data_file| {
                if let Some(snapshot_id) = snapshot_id {
                    ManifestEntry::builder()
                        .status(ManifestStatus::Deleted)
                        .snapshot_id(snapshot_id)
                        // TODO: Get actual sequence numbers from original manifest entries
                        // For now, use 0 as a placeholder - this should be the sequence
                        // number from when the file was originally added
                        .sequence_number(0)
                        .file_sequence_number(0)
                        .data_file(data_file.clone())
                        .build()
                } else {
                    ManifestEntry::builder()
                        .status(ManifestStatus::Deleted)
                        .sequence_number(0)
                        .file_sequence_number(0)
                        .data_file(data_file.clone())
                        .build()
                }
            })
            .collect();

        Ok(deleted_entries)
    }

    /// Returns existing manifest files that should be included in the new snapshot.
    ///
    /// For RowDelta in Copy-on-Write mode:
    /// - We're rewriting entire data files (not just modifying rows)
    /// - Files being deleted are completely replaced by new files
    /// - We should NOT carry forward manifests that contain any of the deleted files
    ///
    /// Note: For future precision COW or Merge-on-Read modes, this logic may need refinement.
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

        // In COW mode, we rewrite entire files, so we need to exclude manifests
        // that contain any files we're deleting. Create a set of deleted file paths for fast lookup.
        let deleted_file_paths: std::collections::HashSet<String> = self
            .removed_data_files
            .iter()
            .map(|f| f.file_path().to_string())
            .collect();

        // Filter out manifests that contain deleted files
        let mut filtered_manifests = Vec::new();
        for manifest_file in manifest_list.entries().iter() {
            if manifest_file.has_added_files() || manifest_file.has_existing_files() {
                // Load the manifest to check if it contains any deleted files
                let manifest = manifest_file
                    .load_manifest(snapshot_produce.table.file_io())
                    .await?;

                // Check if any entries in this manifest are files we're deleting
                let contains_deleted_file = manifest
                    .entries()
                    .iter()
                    .any(|entry| deleted_file_paths.contains(entry.data_file().file_path()));

                if !contains_deleted_file {
                    // This manifest doesn't contain any files we're deleting, keep it
                    filtered_manifests.push(manifest_file.clone());
                }
            }
        }

        Ok(filtered_manifests)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::TableUpdate;
    use crate::spec::{DataContentType, DataFileBuilder, DataFileFormat, Literal, Struct};
    use crate::transaction::tests::make_v2_minimal_table;
    use crate::transaction::{Transaction, TransactionAction};

    #[tokio::test]
    async fn test_row_delta_add_only() {
        // Test adding data files only (pure append)
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(100))]))
            .build()
            .unwrap();

        let action = tx.row_delta().add_data_files(vec![data_file.clone()]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        // Verify snapshot was created
        assert!(matches!(&updates[0], TableUpdate::AddSnapshot { .. }));

        // Verify the snapshot summary shows Append operation
        if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            assert_eq!(snapshot.summary().operation, crate::spec::Operation::Append);
        }
    }

    #[tokio::test]
    async fn test_row_delta_remove_only() {
        // Test removing data files (COW delete) - should succeed
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/old.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(100))]))
            .build()
            .unwrap();

        let action = tx.row_delta().remove_data_files(vec![data_file]);

        // This should succeed - delete-only operations are valid
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        // Verify snapshot was created with Overwrite operation
        if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            assert_eq!(
                snapshot.summary().operation,
                crate::spec::Operation::Overwrite
            );
        }
    }

    #[tokio::test]
    async fn test_row_delta_add_and_remove() {
        // Test COW update: remove old file, add new file
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let old_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/old.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(100))]))
            .build()
            .unwrap();

        let new_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/new.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(120)
            .record_count(12)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(100))]))
            .build()
            .unwrap();

        let action = tx
            .row_delta()
            .remove_data_files(vec![old_file])
            .add_data_files(vec![new_file]);

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        // Verify snapshot was created with Overwrite operation
        if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            assert_eq!(
                snapshot.summary().operation,
                crate::spec::Operation::Overwrite
            );
        }
    }

    #[tokio::test]
    async fn test_row_delta_with_snapshot_properties() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let mut snapshot_properties = HashMap::new();
        snapshot_properties.insert("key".to_string(), "value".to_string());

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(100))]))
            .build()
            .unwrap();

        let action = tx
            .row_delta()
            .set_snapshot_properties(snapshot_properties)
            .add_data_files(vec![data_file]);

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        // Check customized properties in snapshot summary
        if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            assert_eq!(
                snapshot.summary().additional_properties.get("key").unwrap(),
                "value"
            );
        }
    }

    #[tokio::test]
    async fn test_row_delta_validate_from_snapshot() {
        // Test the snapshot validation logic
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(100))]))
            .build()
            .unwrap();

        // Test with invalid snapshot ID (table has no snapshot, so any ID should fail)
        let action = tx
            .row_delta()
            .validate_from_snapshot(99999)
            .add_data_files(vec![data_file.clone()]);

        let result = Arc::new(action).commit(&table).await;
        match result {
            Ok(_) => panic!("expected DataInvalid error for stale snapshot"),
            Err(e) => assert_eq!(e.kind(), crate::ErrorKind::DataInvalid),
        }
    }

    #[tokio::test]
    async fn test_row_delta_empty_action() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx.row_delta();

        // Empty row delta should fail
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_row_delta_incompatible_partition_value() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        // Create file with incompatible partition value (string instead of long)
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/bad.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::string("wrong"))]))
            .build()
            .unwrap();

        let action = tx.row_delta().add_data_files(vec![data_file]);

        // Should fail validation
        assert!(Arc::new(action).commit(&table).await.is_err());
    }
}
