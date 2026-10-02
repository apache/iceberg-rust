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

use crate::error::{Result, invalid_data};
use crate::spec::{DataFile, ManifestEntry, ManifestFile, Operation};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};

/// OverwriteAction is a transaction action for overwriting data files in the table.
///
/// Creates a snapshot with `Operation::Overwrite` semantics — adds new data files and
/// optionally removes existing data files by rewriting affected manifests with those
/// entries marked as `ManifestStatus::Deleted`.
pub struct OverwriteAction {
    check_duplicate: bool,
    commit_uuid: Option<Uuid>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    deleted_data_files: Vec<DataFile>,
}

impl OverwriteAction {
    pub(crate) fn new() -> Self {
        Self {
            check_duplicate: true,
            commit_uuid: None,
            snapshot_properties: HashMap::default(),
            added_data_files: vec![],
            deleted_data_files: vec![],
        }
    }

    /// Set whether to check duplicate files.
    pub fn with_check_duplicate(mut self, v: bool) -> Self {
        self.check_duplicate = v;
        self
    }

    /// Add data files to the snapshot.
    pub fn add_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(data_files);
        self
    }

    /// Specify data files to be removed from the table in this overwrite.
    pub fn delete_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.deleted_data_files.extend(data_files);
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
}

#[async_trait]
impl TransactionAction for OverwriteAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
            self.deleted_data_files.clone(),
        );

        snapshot_producer.validate_added_data_files()?;

        if self.check_duplicate {
            snapshot_producer.validate_duplicate_files().await?;
        }

        let operation = OverwriteOperation {
            has_added_data_files: !self.added_data_files.is_empty(),
            deleted_file_paths: self
                .deleted_data_files
                .iter()
                .map(|f| f.file_path.clone())
                .collect(),
        };
        snapshot_producer
            .commit(operation, DefaultManifestProcess)
            .await
    }
}

struct OverwriteOperation {
    has_added_data_files: bool,
    deleted_file_paths: HashSet<String>,
}

impl SnapshotProduceOperation for OverwriteOperation {
    fn operation(&self) -> Operation {
        match (
            self.has_added_data_files,
            !self.deleted_file_paths.is_empty(),
        ) {
            (true, true) => Operation::Overwrite,
            (false, true) => Operation::Delete,
            // Also a properties-only commit, the workaround from #1548.
            _ => Operation::Append,
        }
    }

    // Only the listed files are replaced, so the table totals carry over.
    fn truncate_full_table(&self) -> bool {
        false
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
        let table = snapshot_produce.table;
        let mut manifests = vec![];
        let mut matched: HashSet<&str> = HashSet::new();

        if let Some(snapshot) = table.metadata().current_snapshot() {
            let manifest_list = table.manifest_list_reader(snapshot).load().await?;
            for manifest_file in manifest_list.entries() {
                // Delete-only manifests record which files were removed and must survive
                // until `expire_snapshots` cleans them up (see #2148).
                if !manifest_file.has_added_files()
                    && !manifest_file.has_existing_files()
                    && !manifest_file.has_deleted_files()
                {
                    continue;
                }
                if self.deleted_file_paths.is_empty() {
                    manifests.push(manifest_file.clone());
                    continue;
                }

                let manifest = table.manifest_reader().read(manifest_file).await?;
                let deletes: Vec<&str> = manifest
                    .entries()
                    .iter()
                    .filter(|entry| entry.is_alive())
                    .filter_map(|entry| self.deleted_file_paths.get(entry.file_path()))
                    .map(String::as_str)
                    .collect();
                if deletes.is_empty() {
                    manifests.push(manifest_file.clone());
                    continue;
                }
                matched.extend(deletes);

                let mut writer = snapshot_produce.new_manifest_writer(
                    manifest_file.content,
                    manifest.metadata().schema.clone(),
                    Arc::new(manifest.metadata().partition_spec.clone()),
                )?;
                // Like Java, only live entries are carried into the rewrite.
                for entry in manifest.entries().iter().filter(|entry| entry.is_alive()) {
                    if self.deleted_file_paths.contains(entry.file_path()) {
                        writer.add_delete_entry(entry.as_ref().clone())?;
                    } else {
                        writer.add_existing_entry(entry.as_ref().clone())?;
                    }
                }
                manifests.push(writer.write_manifest_file().await?);
            }
        }

        let mut missing: Vec<&str> = self
            .deleted_file_paths
            .iter()
            .map(String::as_str)
            .filter(|path| !matched.contains(path))
            .collect();
        if !missing.is_empty() {
            missing.sort_unstable();
            return Err(invalid_data!(
                "Missing required files to delete: {}",
                missing.join(", ")
            ));
        }

        Ok(manifests)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, ManifestStatus,
        Operation, PrimitiveType, SnapshotRef, Struct, Transform, Type, UnboundPartitionField,
        UnboundPartitionSpec,
    };
    use crate::table::Table;
    use crate::transaction::tests::make_v3_minimal_table_in_catalog;
    use crate::transaction::{AddColumn, ApplyTransactionAction, Transaction, TransactionAction};
    use crate::{ErrorKind, TableUpdate};

    fn test_data_file(path: &str, partition_spec_id: i32) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(partition_spec_id)
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap()
    }

    async fn current_manifest_entries(table: &Table) -> Vec<(ManifestStatus, String)> {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();

        let mut entries = vec![];
        for manifest_file in manifest_list.entries() {
            let manifest = table.manifest_reader().read(manifest_file).await.unwrap();
            for entry in manifest.entries() {
                entries.push((entry.status(), entry.file_path().to_string()));
            }
        }
        entries
    }

    #[tokio::test]
    async fn test_overwrite_with_deleted_files() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        let original_file1 = test_data_file("test/original1.parquet", spec_id);
        let original_file2 = test_data_file("test/original2.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            .add_data_files(vec![original_file1.clone(), original_file2.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let replacement_file = test_data_file("test/replacement.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite()
            .add_data_files(vec![replacement_file.clone()])
            .delete_data_files(vec![original_file1.clone(), original_file2.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Overwrite);
        let summary = &snapshot.summary().additional_properties;
        assert_eq!(summary.get("deleted-data-files").unwrap(), "2");
        assert_eq!(summary.get("deleted-records").unwrap(), "2");

        let entries = current_manifest_entries(&table).await;
        assert_eq!(3, entries.len(), "{entries:?}");
        for entry in [
            (ManifestStatus::Deleted, "test/original1.parquet"),
            (ManifestStatus::Deleted, "test/original2.parquet"),
            (ManifestStatus::Added, "test/replacement.parquet"),
        ] {
            assert!(
                entries.contains(&(entry.0, entry.1.to_string())),
                "{entries:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_second_overwrite_does_not_resurrect_a_deleted_file() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        let file_a = test_data_file("test/a.parquet", spec_id);
        let file_b = test_data_file("test/b.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            .add_data_files(vec![file_a.clone(), file_b.clone()]);
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

        let tx = Transaction::new(&table);
        let action = tx
            .overwrite()
            .add_data_files(vec![test_data_file("test/c.parquet", spec_id)])
            .delete_data_files(vec![file_a.clone()]);
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

        let tx = Transaction::new(&table);
        let action = tx
            .overwrite()
            .add_data_files(vec![test_data_file("test/d.parquet", spec_id)])
            .delete_data_files(vec![file_b.clone()]);
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

        let entries = current_manifest_entries(&table).await;
        assert!(
            !entries.iter().any(
                |(status, path)| path == "test/a.parquet" && *status != ManifestStatus::Deleted
            ),
            "the first overwrite's delete must not come back, entries: {entries:?}"
        );
    }

    #[tokio::test]
    async fn test_rewritten_manifest_keeps_the_schema_and_spec_it_was_written_with() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let original_spec_id = table.metadata().default_partition_spec_id();
        let original_schema_id = table.metadata().current_schema_id();

        let file_a = test_data_file("test/a.parquet", original_spec_id);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![file_a.clone()]);
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

        let tx = Transaction::new(&table);
        let action = tx.update_schema().add_column(AddColumn::optional(
            "extra",
            Type::Primitive(PrimitiveType::Int),
        ));
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

        // There is no transaction action for partition evolution yet, so the spec is replaced
        // on the metadata directly.
        let evolved = table
            .metadata()
            .clone()
            .into_builder(None)
            .add_default_partition_spec(
                UnboundPartitionSpec::builder()
                    .add_partition_field(
                        UnboundPartitionField::builder()
                            .source_ids(vec![2])
                            .name("y")
                            .transform(Transform::Identity)
                            .build()
                            .unwrap(),
                    )
                    .unwrap()
                    .build(),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let table = table.with_metadata(Arc::new(evolved));
        assert_ne!(original_schema_id, table.metadata().current_schema_id());
        assert_ne!(
            original_spec_id,
            table.metadata().default_partition_spec_id()
        );

        let tx = Transaction::new(&table);
        let action = tx
            .overwrite()
            .add_data_files(vec![test_data_file(
                "test/b.parquet",
                table.metadata().default_partition_spec_id(),
            )])
            .delete_data_files(vec![file_a.clone()]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let new_snapshot: SnapshotRef = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            SnapshotRef::new(snapshot.clone())
        } else {
            unreachable!()
        };

        let manifest_list = table
            .manifest_list_reader(&new_snapshot)
            .load()
            .await
            .unwrap();
        let mut rewritten = None;
        for manifest_file in manifest_list.entries() {
            let manifest = table.manifest_reader().read(manifest_file).await.unwrap();
            if manifest
                .entries()
                .iter()
                .any(|entry| entry.status() == ManifestStatus::Deleted)
            {
                rewritten = Some((manifest_file.clone(), manifest));
            }
        }
        let (rewritten_file, rewritten_manifest) =
            rewritten.expect("the manifest holding test/a.parquet must have been rewritten");

        assert_eq!(original_schema_id, rewritten_manifest.metadata().schema_id);
        assert_eq!(original_spec_id, rewritten_file.partition_spec_id);
    }

    #[tokio::test]
    async fn test_overwrite_counts_a_duplicated_delete_path_once() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        let file_a = test_data_file("test/a.parquet", spec_id);
        let file_b = test_data_file("test/b.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            .add_data_files(vec![file_a.clone(), file_b.clone()]);
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

        let tx = Transaction::new(&table);
        let action = tx
            .overwrite()
            .add_data_files(vec![test_data_file("test/c.parquet", spec_id)])
            .delete_data_files(vec![file_a.clone(), file_a.clone()]);
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

        let summary = table.metadata().current_snapshot().unwrap().summary();
        assert_eq!(
            summary.additional_properties.get("deleted-data-files"),
            Some(&"1".to_string())
        );
        assert_eq!(
            summary.additional_properties.get("deleted-records"),
            Some(&"1".to_string())
        );
        assert_eq!(
            summary.additional_properties.get("total-data-files"),
            Some(&"2".to_string())
        );
    }

    #[tokio::test]
    async fn test_delete_only_overwrite_reports_a_delete_operation() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        let file_a = test_data_file("test/a.parquet", spec_id);
        let file_b = test_data_file("test/b.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            .add_data_files(vec![file_a.clone(), file_b.clone()]);
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

        let tx = Transaction::new(&table);
        let action = tx.overwrite().delete_data_files(vec![file_a.clone()]);
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Delete);
        assert_eq!(
            snapshot
                .summary()
                .additional_properties
                .get("deleted-data-files"),
            Some(&"1".to_string())
        );

        let entries = current_manifest_entries(&table).await;
        assert!(
            entries.contains(&(ManifestStatus::Deleted, "test/a.parquet".to_string())),
            "the deleted file must be a tombstone, entries: {entries:?}"
        );
        assert!(
            entries.contains(&(ManifestStatus::Existing, "test/b.parquet".to_string())),
            "the untouched file must stay live, entries: {entries:?}"
        );
    }

    #[tokio::test]
    async fn test_overwrite_rejects_deleting_a_file_the_table_does_not_have() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        let file_a = test_data_file("test/a.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![file_a.clone()]);
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

        let tx = Transaction::new(&table);
        let action = tx
            .overwrite()
            .add_data_files(vec![test_data_file("test/b.parquet", spec_id)])
            .delete_data_files(vec![
                file_a.clone(),
                test_data_file("test/never-committed.parquet", spec_id),
            ]);
        let err = Arc::new(action).commit(&table).await.err().unwrap();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.to_string()
                .contains("Missing required files to delete: test/never-committed.parquet"),
            "{err}"
        );
    }
}
