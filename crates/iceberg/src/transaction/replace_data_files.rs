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
use crate::{Error, ErrorKind};

/// Action to replace data files in a table (for compaction/rewrite operations).
pub struct ReplaceDataFilesAction {
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    files_to_delete: Vec<DataFile>,
    files_to_add: Vec<DataFile>,
}

impl ReplaceDataFilesAction {
    pub(crate) fn new() -> Self {
        Self {
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
            files_to_delete: vec![],
            files_to_add: vec![],
        }
    }

    /// Add files to delete (old files being replaced).
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
}

#[async_trait]
impl TransactionAction for ReplaceDataFilesAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        if self.files_to_delete.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Replace operation requires files to delete",
            ));
        }

        if self.files_to_add.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Replace operation requires files to add",
            ));
        }

        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            self.files_to_add.clone(),
        );

        snapshot_producer.validate_added_data_files()?;

        let replace_op = ReplaceOperation {
            files_to_delete: self.files_to_delete.clone(),
        };

        snapshot_producer
            .commit(replace_op, DefaultManifestProcess)
            .await
    }
}

struct ReplaceOperation {
    files_to_delete: Vec<DataFile>,
}

impl SnapshotProduceOperation for ReplaceOperation {
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
        let Some(snapshot) = snapshot_produce.table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let files_to_delete: std::collections::HashSet<&str> = self
            .files_to_delete
            .iter()
            .map(|f| f.file_path.as_str())
            .collect();

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_produce.table.file_io(),
                &snapshot_produce.table.metadata_ref(),
            )
            .await?;

        // Include existing manifests that don't contain deleted files
        let mut result = Vec::new();
        for entry in manifest_list.entries() {
            if !entry.has_added_files() && !entry.has_existing_files() {
                continue;
            }

            let manifest = entry.load_manifest(snapshot_produce.table.file_io()).await?;
            let has_deleted_file = manifest
                .entries()
                .iter()
                .any(|e| e.is_alive() && files_to_delete.contains(e.file_path()));

            if !has_deleted_file {
                result.push(entry.clone());
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, Operation, Struct,
    };
    use crate::transaction::tests::make_v2_minimal_table;
    use crate::transaction::{Transaction, TransactionAction};
    use crate::TableUpdate;

    fn create_data_file(table: &crate::table::Table, path: &str, record_count: u64) -> DataFile {
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
    async fn test_replace_data_files_basic() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let old_file = create_data_file(&table, "data/old.parquet", 100);
        let new_file = create_data_file(&table, "data/new.parquet", 100);

        let action = tx
            .replace_data_files()
            .delete_files(vec![old_file])
            .add_files(vec![new_file]);

        let result = Arc::new(action).commit(&table).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_replace_data_files_empty_deletes_fails() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let new_file = create_data_file(&table, "data/new.parquet", 100);

        let action = tx.replace_data_files().add_files(vec![new_file]);

        let result = Arc::new(action).commit(&table).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_replace_data_files_empty_adds_fails() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let old_file = create_data_file(&table, "data/old.parquet", 100);

        let action = tx.replace_data_files().delete_files(vec![old_file]);

        let result = Arc::new(action).commit(&table).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_replace_uses_replace_operation() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let old_file = create_data_file(&table, "data/old.parquet", 100);
        let new_file = create_data_file(&table, "data/new.parquet", 100);

        let action = tx
            .replace_data_files()
            .delete_files(vec![old_file])
            .add_files(vec![new_file]);

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            panic!("Expected AddSnapshot");
        };

        assert_eq!(new_snapshot.summary().operation, Operation::Replace);
    }
}

#[cfg(test)]
mod integration_tests {
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, Operation, Struct,
    };
    use crate::transaction::tests::make_v3_minimal_table_in_catalog;
    use crate::transaction::{ApplyTransactionAction, Transaction};

    fn create_file(path: &str, record_count: u64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(record_count)
            .partition(Struct::from_iter([Some(Literal::long(0))]))
            .partition_spec_id(0)
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_replace_after_append() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // First append some files
        let file1 = create_file("data/file1.parquet", 100);
        let file2 = create_file("data/file2.parquet", 100);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![file1.clone(), file2.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(table.metadata().snapshots().count(), 1);

        // Now replace file1 and file2 with a single compacted file
        let compacted = create_file("data/compacted.parquet", 200);
        let tx = Transaction::new(&table);
        let action = tx
            .replace_data_files()
            .delete_files(vec![file1, file2])
            .add_files(vec![compacted]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(table.metadata().snapshots().count(), 2);
        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Replace);

        // Verify manifest has only the compacted file
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert_eq!(manifest_list.entries().len(), 1);

        let manifest = manifest_list.entries()[0]
            .load_manifest(table.file_io())
            .await
            .unwrap();
        assert_eq!(manifest.entries().len(), 1);
        assert_eq!(manifest.entries()[0].file_path(), "data/compacted.parquet");
    }

    #[tokio::test]
    async fn test_replace_preserves_unrelated_files() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Append file1
        let file1 = create_file("data/file1.parquet", 100);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![file1.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Append file2 in separate transaction
        let file2 = create_file("data/file2.parquet", 100);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![file2.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Replace only file1
        let file1_compacted = create_file("data/file1_compacted.parquet", 100);
        let tx = Transaction::new(&table);
        let action = tx
            .replace_data_files()
            .delete_files(vec![file1])
            .add_files(vec![file1_compacted]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Verify both file2 manifest and new compacted manifest exist
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        // Should have 2 manifests: one for file2 (preserved), one for compacted
        assert_eq!(manifest_list.entries().len(), 2);

        // Collect all file paths
        let mut all_files = Vec::new();
        for entry in manifest_list.entries() {
            let manifest = entry.load_manifest(table.file_io()).await.unwrap();
            for e in manifest.entries() {
                if e.is_alive() {
                    all_files.push(e.file_path().to_string());
                }
            }
        }
        all_files.sort();
        assert_eq!(all_files, vec!["data/file1_compacted.parquet", "data/file2.parquet"]);
    }
}
