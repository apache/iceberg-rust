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
use crate::spec::{DataFile, ManifestEntry, ManifestFile, Operation};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};

/// RowDeltaAction is a transaction action for atomically committing both data files
/// and delete files to a table. This enables Merge-on-Read (MOR) operations where
/// deletes are tracked separately from data files.
///
/// This action supports:
/// - Adding new data files (inserts)
/// - Adding equality delete files (delete rows matching certain column values)
/// - Adding position delete files (delete specific row positions in data files)
pub struct RowDeltaAction {
    /// Whether to check for duplicate files before committing
    check_duplicate: bool,
    /// Optional commit UUID for tracking this transaction
    commit_uuid: Option<Uuid>,
    /// Optional encryption key metadata for manifest files
    key_metadata: Option<Vec<u8>>,
    /// Custom properties to add to the snapshot summary
    snapshot_properties: HashMap<String, String>,
    /// Data files to add (for inserts)
    added_data_files: Vec<DataFile>,
    /// Delete files to add (equality or position deletes)
    added_delete_files: Vec<DataFile>,
}

impl RowDeltaAction {
    /// Creates a new RowDeltaAction with default settings.
    pub(crate) fn new() -> Self {
        Self {
            check_duplicate: true,
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
            added_data_files: vec![],
            added_delete_files: vec![],
        }
    }

    /// Set whether to check for duplicate files.
    ///
    /// When enabled (default), the action will verify that added files are not
    /// already referenced by the table, preventing accidental duplication.
    ///
    /// # Arguments
    /// * `v` - true to enable duplicate checking, false to disable
    pub fn with_check_duplicate(mut self, v: bool) -> Self {
        self.check_duplicate = v;
        self
    }

    /// Add data files to insert into the table.
    ///
    /// These files should have content type `DataContentType::Data`.
    ///
    /// # Arguments
    /// * `data_files` - Iterator of data files to add
    pub fn add_rows(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(data_files);
        self
    }

    /// Add delete files to the table.
    ///
    /// These files should have content type `DataContentType::EqualityDeletes`
    /// or `DataContentType::PositionDeletes`.
    ///
    /// For equality deletes:
    /// - The `equality_ids` field must be populated with the field IDs used for equality matching
    /// - Each row in the delete file represents a set of column values to match and delete
    ///
    /// For position deletes:
    /// - The `equality_ids` field must be empty
    /// - Each row specifies a file path and row position to delete
    ///
    /// # Arguments
    /// * `delete_files` - Iterator of delete files to add
    pub fn add_deletes(mut self, delete_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_delete_files.extend(delete_files);
        self
    }

    /// Set the commit UUID for this transaction.
    ///
    /// If not set, a UUID will be generated automatically when the transaction is committed.
    ///
    /// # Arguments
    /// * `commit_uuid` - UUID to use for this commit
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set encryption key metadata for manifest files.
    ///
    /// This metadata is implementation-specific and used for encrypting manifest files.
    ///
    /// # Arguments
    /// * `key_metadata` - Binary key metadata
    pub fn set_key_metadata(mut self, key_metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(key_metadata);
        self
    }

    /// Set custom properties to include in the snapshot summary.
    ///
    /// These properties will be merged with automatically generated summary properties
    /// like record counts and file counts.
    ///
    /// # Arguments
    /// * `snapshot_properties` - Map of custom property names to values
    pub fn set_snapshot_properties(mut self, snapshot_properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }

    /// Returns true if this action adds any data files.
    pub(crate) fn has_data_files(&self) -> bool {
        !self.added_data_files.is_empty()
    }

    /// Returns true if this action adds any delete files.
    pub(crate) fn has_delete_files(&self) -> bool {
        !self.added_delete_files.is_empty()
    }

    /// Returns a reference to the data files to be added.
    pub(crate) fn data_files(&self) -> &[DataFile] {
        &self.added_data_files
    }

    /// Returns a reference to the delete files to be added.
    pub(crate) fn delete_files(&self) -> &[DataFile] {
        &self.added_delete_files
    }

    /// Returns the check duplicate setting.
    pub(crate) fn check_duplicate(&self) -> bool {
        self.check_duplicate
    }

    /// Returns the commit UUID, if set.
    pub(crate) fn commit_uuid(&self) -> Option<Uuid> {
        self.commit_uuid
    }

    /// Returns the key metadata, if set.
    pub(crate) fn key_metadata(&self) -> Option<&[u8]> {
        self.key_metadata.as_deref()
    }

    /// Returns a reference to the snapshot properties.
    pub(crate) fn snapshot_properties(&self) -> &HashMap<String, String> {
        &self.snapshot_properties
    }
}

#[async_trait]
impl TransactionAction for RowDeltaAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
            self.added_delete_files.clone(),
        );

        // Validate data files if present
        if !self.added_data_files.is_empty() {
            snapshot_producer.validate_added_data_files(&self.added_data_files)?;
        }

        // Validate delete files if present
        if !self.added_delete_files.is_empty() {
            snapshot_producer.validate_added_delete_files(&self.added_delete_files)?;
        }

        // Check for duplicates if enabled
        if self.check_duplicate {
            if !self.added_data_files.is_empty() {
                snapshot_producer
                    .validate_duplicate_files(&self.added_data_files)
                    .await?;
            }
            // Note: Delete files typically don't need duplicate checking as they're meant to delete existing data
        }

        snapshot_producer
            .commit(RowDeltaOperation::new(&self), DefaultManifestProcess)
            .await
    }
}

/// Operation type for RowDelta transactions.
/// Determines the operation based on what files are being added.
struct RowDeltaOperation {
    has_data_files: bool,
    has_delete_files: bool,
}

impl RowDeltaOperation {
    fn new(action: &RowDeltaAction) -> Self {
        Self {
            has_data_files: !action.added_data_files.is_empty(),
            has_delete_files: !action.added_delete_files.is_empty(),
        }
    }
}

impl SnapshotProduceOperation for RowDeltaOperation {
    fn operation(&self) -> Operation {
        // If only adding deletes, operation is DELETE
        // Otherwise (deletes + data, or only data), operation is OVERWRITE
        if self.has_delete_files && !self.has_data_files {
            Operation::Delete
        } else {
            Operation::Overwrite
        }
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        // RowDelta doesn't remove existing manifest entries
        Ok(vec![])
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

        // Include all existing manifests (both data and delete)
        Ok(manifest_list
            .entries()
            .iter()
            .filter(|entry| entry.has_added_files() || entry.has_existing_files())
            .cloned()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct};

    #[test]
    fn test_new_row_delta_action() {
        let action = RowDeltaAction::new();
        assert!(action.check_duplicate);
        assert!(action.commit_uuid.is_none());
        assert!(action.key_metadata.is_none());
        assert!(action.snapshot_properties.is_empty());
        assert!(action.added_data_files.is_empty());
        assert!(action.added_delete_files.is_empty());
    }

    #[test]
    fn test_add_rows() {
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/data.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(0)
            .partition(Struct::empty())
            .build()
            .unwrap();

        let action = RowDeltaAction::new().add_rows(vec![data_file.clone()]);

        assert_eq!(action.added_data_files.len(), 1);
        assert_eq!(action.added_delete_files.len(), 0);
        assert!(action.has_data_files());
        assert!(!action.has_delete_files());
    }

    #[test]
    fn test_add_deletes() {
        let delete_file = DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path("test/deletes.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(50)
            .record_count(5)
            .partition_spec_id(0)
            .partition(Struct::empty())
            .equality_ids(vec![1])
            .build()
            .unwrap();

        let action = RowDeltaAction::new().add_deletes(vec![delete_file.clone()]);

        assert_eq!(action.added_data_files.len(), 0);
        assert_eq!(action.added_delete_files.len(), 1);
        assert!(!action.has_data_files());
        assert!(action.has_delete_files());
    }

    #[test]
    fn test_add_rows_and_deletes() {
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/data.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(0)
            .partition(Struct::empty())
            .build()
            .unwrap();

        let delete_file = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/deletes.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(50)
            .record_count(5)
            .partition_spec_id(0)
            .partition(Struct::empty())
            .build()
            .unwrap();

        let action = RowDeltaAction::new()
            .add_rows(vec![data_file])
            .add_deletes(vec![delete_file]);

        assert_eq!(action.added_data_files.len(), 1);
        assert_eq!(action.added_delete_files.len(), 1);
        assert!(action.has_data_files());
        assert!(action.has_delete_files());
    }

    #[test]
    fn test_with_check_duplicate() {
        let action = RowDeltaAction::new().with_check_duplicate(false);
        assert!(!action.check_duplicate());
    }

    #[test]
    fn test_set_commit_uuid() {
        let uuid = Uuid::new_v4();
        let action = RowDeltaAction::new().set_commit_uuid(uuid);
        assert_eq!(action.commit_uuid(), Some(uuid));
    }

    #[test]
    fn test_set_key_metadata() {
        let metadata = vec![1, 2, 3, 4];
        let action = RowDeltaAction::new().set_key_metadata(metadata.clone());
        assert_eq!(action.key_metadata(), Some(metadata.as_slice()));
    }

    #[test]
    fn test_set_snapshot_properties() {
        let mut props = HashMap::new();
        props.insert("key1".to_string(), "value1".to_string());
        props.insert("key2".to_string(), "value2".to_string());

        let action = RowDeltaAction::new().set_snapshot_properties(props.clone());
        assert_eq!(action.snapshot_properties(), &props);
    }

    #[test]
    fn test_builder_chain() {
        let uuid = Uuid::new_v4();
        let metadata = vec![1, 2, 3];
        let mut props = HashMap::new();
        props.insert("test".to_string(), "value".to_string());

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/data.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(0)
            .partition(Struct::empty())
            .build()
            .unwrap();

        let action = RowDeltaAction::new()
            .with_check_duplicate(false)
            .set_commit_uuid(uuid)
            .set_key_metadata(metadata.clone())
            .set_snapshot_properties(props.clone())
            .add_rows(vec![data_file]);

        assert!(!action.check_duplicate());
        assert_eq!(action.commit_uuid(), Some(uuid));
        assert_eq!(action.key_metadata(), Some(metadata.as_slice()));
        assert_eq!(action.snapshot_properties(), &props);
        assert_eq!(action.added_data_files.len(), 1);
    }

    // Integration tests for RowDelta transaction
    use crate::spec::Literal;
    use crate::transaction::tests::make_v2_minimal_table;
    use crate::transaction::{Transaction, TransactionAction};
    use crate::TableUpdate;

    #[tokio::test]
    async fn test_row_delta_with_only_data_files() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/data1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(100))]))
            .build()
            .unwrap();

        let action = tx.row_delta().add_rows(vec![data_file]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        // Should create snapshot with OVERWRITE operation
        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            panic!("Expected AddSnapshot update");
        };

        assert_eq!(new_snapshot.summary().operation, crate::spec::Operation::Overwrite);
    }

    #[tokio::test]
    async fn test_row_delta_with_only_equality_deletes() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let delete_file = DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path("test/deletes1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(50)
            .record_count(5)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(100))]))
            .equality_ids(vec![1]) // Must have equality_ids for equality deletes
            .build()
            .unwrap();

        let action = tx.row_delta().add_deletes(vec![delete_file]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        // Should create snapshot with DELETE operation
        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            panic!("Expected AddSnapshot update");
        };

        assert_eq!(new_snapshot.summary().operation, crate::spec::Operation::Delete);
    }

    #[tokio::test]
    async fn test_row_delta_with_data_and_deletes() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/data2.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(100))]))
            .build()
            .unwrap();

        let delete_file = DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path("test/deletes2.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(50)
            .record_count(5)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(100))]))
            .equality_ids(vec![1])
            .build()
            .unwrap();

        let action = tx
            .row_delta()
            .add_rows(vec![data_file])
            .add_deletes(vec![delete_file]);

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        // Should create snapshot with OVERWRITE operation (mixed data+deletes)
        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            panic!("Expected AddSnapshot update");
        };

        assert_eq!(new_snapshot.summary().operation, crate::spec::Operation::Overwrite);

        // Verify manifest list has both data and delete manifests
        let manifest_list = new_snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        // Should have 2 manifests: one for data, one for deletes
        assert_eq!(manifest_list.entries().len(), 2);
    }

    #[tokio::test]
    async fn test_row_delta_with_position_deletes() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let delete_file = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/pos_deletes.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(30)
            .record_count(3)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(100))]))
            // Position deletes should NOT have equality_ids
            .build()
            .unwrap();

        let action = tx.row_delta().add_deletes(vec![delete_file]);
        let action_commit = Arc::new(action).commit(&table).await;

        assert!(action_commit.is_ok());
    }

    #[tokio::test]
    async fn test_row_delta_validation_equality_deletes_without_equality_ids() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        // Invalid: equality delete without equality_ids
        let delete_file = DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path("test/invalid_deletes.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(50)
            .record_count(5)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(100))]))
            // Missing equality_ids!
            .build()
            .unwrap();

        let action = tx.row_delta().add_deletes(vec![delete_file]);
        let result = Arc::new(action).commit(&table).await;

        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.to_string().contains("equality_ids"));
    }

    #[tokio::test]
    async fn test_row_delta_validation_position_deletes_with_equality_ids() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        // Invalid: position delete with equality_ids set
        let delete_file = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/invalid_pos_deletes.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(30)
            .record_count(3)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(100))]))
            .equality_ids(vec![1]) // Should NOT have this!
            .build()
            .unwrap();

        let action = tx.row_delta().add_deletes(vec![delete_file]);
        let result = Arc::new(action).commit(&table).await;

        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.to_string().contains("should not have equality_ids"));
    }

    #[tokio::test]
    async fn test_row_delta_empty_action() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        // Empty action - no files added
        let action = tx.row_delta();
        let result = Arc::new(action).commit(&table).await;

        // Should fail - no files to commit
        assert!(result.is_err());
    }
}
