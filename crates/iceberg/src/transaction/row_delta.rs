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

//! Row delta transaction for encoding row-level changes to Iceberg tables.
//!
//! This module provides the `RowDeltaAction` which enables atomic application of row-level
//! modifications including inserts, updates, and deletes. It supports both position deletes
//! (for specific row locations) and equality deletes (for rows matching equality conditions).
//!
//! This is the appropriate transaction type for CDC (Change Data Capture) ingestion,
//! upsert operations, and row-level deletions.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{DataContentType, DataFile, ManifestEntry, ManifestFile, Operation};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind};

/// RowDeltaAction is a transaction action for encoding row-level changes to a table.
///
/// This action supports:
/// - Adding new data files
/// - Adding delete files (both position and equality deletes)
///
/// This is the appropriate action to use for:
/// - CDC (Change Data Capture) ingestion
/// - Upsert operations
/// - Row-level deletions
///
/// # Example
/// ```ignore
/// use iceberg::transaction::Transaction;
///
/// let tx = Transaction::new(&table);
/// let action = tx.row_delta()
///     .add_data_files(new_data_files)
///     .add_delete_files(equality_delete_files);
/// let tx = action.apply(tx).unwrap();
/// let table = tx.commit(&catalog).await.unwrap();
/// ```
pub struct RowDeltaAction {
    check_duplicate: bool,
    // below are properties used to create SnapshotProducer when commit
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    added_delete_files: Vec<DataFile>,
}

impl RowDeltaAction {
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

    /// Set whether to check duplicate files
    pub fn with_check_duplicate(mut self, v: bool) -> Self {
        self.check_duplicate = v;
        self
    }

    /// Add data files to the snapshot.
    pub fn add_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(data_files);
        self
    }

    /// Add delete files to the snapshot.
    ///
    /// Delete files can be either position deletes or equality deletes.
    /// The content type of each file will be validated.
    pub fn add_delete_files(mut self, delete_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_delete_files.extend(delete_files);
        self
    }

    /// Set commit UUID for the snapshot.
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

    /// Validate that delete files have appropriate content types
    fn validate_delete_files(delete_files: &[DataFile]) -> Result<()> {
        for delete_file in delete_files {
            match delete_file.content_type() {
                DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                    // Valid delete file types
                }
                DataContentType::Data => {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "File {} has content type Data but was added as a delete file. Use add_data_files() instead.",
                            delete_file.file_path()
                        ),
                    ));
                }
            }

            // Additional validation for equality deletes
            if delete_file.content_type() == DataContentType::EqualityDeletes
                && delete_file.equality_ids().is_none()
            {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Equality delete file {} must have equality_ids set",
                        delete_file.file_path()
                    ),
                ));
            }
        }
        Ok(())
    }
}

#[async_trait]
impl TransactionAction for RowDeltaAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        // Validate delete files have correct content types
        Self::validate_delete_files(&self.added_delete_files)?;

        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
            self.added_delete_files.clone(),
        );

        // Validate added data files (partition specs, etc.)
        if !self.added_data_files.is_empty() {
            snapshot_producer.validate_added_data_files(&self.added_data_files)?;
        }

        // Validate added delete files (partition specs, etc.)
        if !self.added_delete_files.is_empty() {
            snapshot_producer.validate_added_data_files(&self.added_delete_files)?;
        }

        // Check duplicate files
        if self.check_duplicate {
            snapshot_producer.validate_duplicate_files().await?;
        }

        snapshot_producer
            .commit(RowDeltaOperation, DefaultManifestProcess)
            .await
    }
}

struct RowDeltaOperation;

impl SnapshotProduceOperation for RowDeltaOperation {
    fn operation(&self) -> Operation {
        Operation::Append
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
        let Some(snapshot) = snapshot_produce.table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_produce.table.file_io(),
                &snapshot_produce.table.metadata_ref(),
            )
            .await?;

        // Include all existing manifests with added or existing files
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
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, MAIN_BRANCH, Struct,
    };
    use crate::transaction::tests::make_v2_minimal_table;
    use crate::transaction::{Transaction, TransactionAction};
    use crate::{TableRequirement, TableUpdate};

    #[tokio::test]
    async fn test_row_delta_with_data_and_deletes() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/data-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(100))]))
            .build()
            .unwrap();

        let delete_file = DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path("test/delete-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(50)
            .record_count(5)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(100))]))
            .equality_ids(Some(vec![1])) // Assuming field id 1 is the key
            .build()
            .unwrap();

        let action = tx
            .row_delta()
            .add_data_files(vec![data_file.clone()])
            .add_delete_files(vec![delete_file.clone()]);

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let requirements = action_commit.take_requirements();

        // Check updates and requirements
        assert!(
            matches!((&updates[0],&updates[1]), (TableUpdate::AddSnapshot { snapshot },TableUpdate::SetSnapshotRef { reference,ref_name }) if snapshot.snapshot_id() == reference.snapshot_id && ref_name == MAIN_BRANCH)
        );
        assert_eq!(
            vec![
                TableRequirement::UuidMatch {
                    uuid: table.metadata().uuid()
                },
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: MAIN_BRANCH.to_string(),
                    snapshot_id: table.metadata().current_snapshot_id
                }
            ],
            requirements
        );

        // Check manifest list
        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            unreachable!()
        };
        let manifest_list = new_snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        // Should have 2 manifests: one for data, one for deletes
        assert_eq!(2, manifest_list.entries().len());
    }

    #[tokio::test]
    async fn test_row_delta_rejects_data_file_as_delete() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/data-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(100))]))
            .build()
            .unwrap();

        // Try to add a data file as a delete file - should fail
        let action = tx.row_delta().add_delete_files(vec![data_file]);

        let result = Arc::new(action).commit(&table).await;
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(
            err.to_string()
                .contains("has content type Data but was added as a delete file")
        );
    }

    #[tokio::test]
    async fn test_row_delta_rejects_equality_delete_without_ids() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let delete_file = DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path("test/delete-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(50)
            .record_count(5)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(100))]))
            // Missing equality_ids!
            .build()
            .unwrap();

        let action = tx.row_delta().add_delete_files(vec![delete_file]);

        let result = Arc::new(action).commit(&table).await;
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.to_string().contains("must have equality_ids set"));
    }

    #[tokio::test]
    async fn test_row_delta_with_only_deletes() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let delete_file = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/delete-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(50)
            .record_count(5)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(100))]))
            .build()
            .unwrap();

        let action = tx.row_delta().add_delete_files(vec![delete_file]);

        let result = Arc::new(action).commit(&table).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_row_delta_with_snapshot_properties() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let mut snapshot_properties = HashMap::new();
        snapshot_properties.insert("custom-key".to_string(), "custom-value".to_string());

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/data-1.parquet".to_string())
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

        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            unreachable!()
        };
        assert_eq!(
            new_snapshot
                .summary()
                .additional_properties
                .get("custom-key")
                .unwrap(),
            "custom-value"
        );
    }
}
