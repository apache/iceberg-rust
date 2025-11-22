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
use crate::spec::{
    DataContentType, DataFile, ManifestEntry, ManifestFile, ManifestStatus, Operation,
};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind};

/// AppendDeleteFilesAction is a transaction action for appending delete files to the table.
///
/// This action allows you to add position delete files or equality delete files to mark
/// rows as deleted without rewriting data files. This is essential for implementing
/// DELETE and UPDATE operations efficiently.
///
/// # Example
///
/// ```rust,no_run
/// # use iceberg::table::Table;
/// # use iceberg::transaction::Transaction;
/// # use iceberg::spec::DataFile;
/// # async fn example(table: Table, delete_files: Vec<DataFile>) -> iceberg::Result<()> {
/// let tx = Transaction::new(&table);
/// let action = tx.append_delete_files().add_files(delete_files);
///
/// // Apply to transaction and commit
/// let tx = action.apply(tx)?;
/// tx.commit(&table.catalog()).await?;
/// # Ok(())
/// # }
/// ```
pub struct AppendDeleteFilesAction {
    // below are properties used to create SnapshotProducer when commit
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    added_delete_files: Vec<DataFile>,
}

impl AppendDeleteFilesAction {
    pub(crate) fn new() -> Self {
        Self {
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
            added_delete_files: vec![],
        }
    }

    /// Add delete files to the snapshot.
    ///
    /// The files should be position delete files or equality delete files created
    /// by `PositionDeleteFileWriter` or `EqualityDeleteFileWriter`.
    pub fn add_files(mut self, delete_files: impl IntoIterator<Item = DataFile>) -> Self {
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

    /// Validate that all files are delete files (position or equality deletes).
    fn validate_delete_files(&self) -> Result<()> {
        if self.added_delete_files.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "No delete files provided for append delete operation",
            ));
        }

        for delete_file in &self.added_delete_files {
            match delete_file.content_type() {
                DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                    // Valid delete file
                }
                DataContentType::Data => {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "File {} has content type 'Data' but should be PositionDeletes or EqualityDeletes",
                            delete_file.file_path
                        ),
                    ));
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl TransactionAction for AppendDeleteFilesAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        // Validate delete files
        self.validate_delete_files()?;

        // Create snapshot producer with empty data files
        // The delete files will be returned via delete_entries() method
        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            vec![], // No data files for delete operation
        );

        snapshot_producer
            .commit(
                AppendDeleteOperation::new(self.added_delete_files.clone()),
                DefaultManifestProcess,
            )
            .await
    }
}

struct AppendDeleteOperation {
    delete_files: Vec<DataFile>,
}

impl AppendDeleteOperation {
    fn new(delete_files: Vec<DataFile>) -> Self {
        Self { delete_files }
    }
}

impl SnapshotProduceOperation for AppendDeleteOperation {
    fn operation(&self) -> Operation {
        // Using Append operation for delete files
        // The manifest content type will distinguish data from deletes
        Operation::Append
    }

    async fn data_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        Ok(vec![])
    }

    async fn delete_entries(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        // Convert delete files to manifest entries
        let snapshot_id = snapshot_produce.snapshot_id();
        let format_version = snapshot_produce.table.metadata().format_version();

        Ok(self
            .delete_files
            .iter()
            .map(|delete_file| {
                let builder = ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(delete_file.clone());
                if format_version == crate::spec::FormatVersion::V1 {
                    builder.snapshot_id(snapshot_id).build()
                } else {
                    // For format version > 1, we set the snapshot id at the inherited time
                    builder.build()
                }
            })
            .collect())
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        // Carry forward all existing manifests (both data and delete)
        let Some(snapshot) = snapshot_produce.table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_produce.table.file_io(),
                &snapshot_produce.table.metadata_ref(),
            )
            .await?;

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
    async fn test_empty_delete_files_fails() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx.append_delete_files().add_files(vec![]);
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_data_file_rejected() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        // Try to add a data file instead of delete file
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let action = tx.append_delete_files().add_files(vec![data_file]);
        let result = Arc::new(action).commit(&table).await;
        assert!(result.is_err());
        if let Err(error) = result {
            assert!(error.to_string().contains("content type 'Data'"));
        }
    }

    #[tokio::test]
    async fn test_append_position_delete_files() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let position_delete_file = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/pos-del-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(5)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(100))]))
            .build()
            .unwrap();

        let action = tx
            .append_delete_files()
            .add_files(vec![position_delete_file.clone()]);
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
        assert_eq!(1, manifest_list.entries().len());

        // Check manifest contains delete file
        let manifest = manifest_list.entries()[0]
            .load_manifest(table.file_io())
            .await
            .unwrap();
        assert_eq!(1, manifest.entries().len());
        assert_eq!(
            DataContentType::PositionDeletes,
            manifest.entries()[0].data_file().content_type()
        );
        assert_eq!(position_delete_file, *manifest.entries()[0].data_file());
    }

    #[tokio::test]
    async fn test_append_equality_delete_files() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let equality_delete_file = DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path("test/eq-del-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(3)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(200))]))
            .equality_ids(Some(vec![1, 2])) // Field IDs used for equality
            .build()
            .unwrap();

        let action = tx
            .append_delete_files()
            .add_files(vec![equality_delete_file.clone()]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

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
        assert_eq!(1, manifest_list.entries().len());

        // Check manifest contains equality delete file
        let manifest = manifest_list.entries()[0]
            .load_manifest(table.file_io())
            .await
            .unwrap();
        assert_eq!(1, manifest.entries().len());
        assert_eq!(
            DataContentType::EqualityDeletes,
            manifest.entries()[0].data_file().content_type()
        );
        assert_eq!(equality_delete_file, *manifest.entries()[0].data_file());
    }

    #[tokio::test]
    async fn test_append_delete_files_with_properties() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let mut snapshot_properties = HashMap::new();
        snapshot_properties.insert("delete_reason".to_string(), "GDPR_request".to_string());

        let delete_file = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/pos-del-gdpr.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(50)
            .record_count(10)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let action = tx
            .append_delete_files()
            .set_snapshot_properties(snapshot_properties)
            .add_files(vec![delete_file]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        // Check customized properties in snapshot summary
        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            unreachable!()
        };
        assert_eq!(
            new_snapshot
                .summary()
                .additional_properties
                .get("delete_reason")
                .unwrap(),
            "GDPR_request"
        );
    }
}
