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
use crate::spec::{
    DataFile, FormatVersion, Manifest, ManifestContentType, ManifestEntry, ManifestFile,
    ManifestWriterBuilder, Operation, PartitionSpecRef, SchemaRef,
};
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

        let deleted_file_paths: HashSet<String> = self
            .deleted_data_files
            .iter()
            .map(|f| f.file_path.clone())
            .collect();

        let snapshot_id = snapshot_producer.snapshot_id();
        snapshot_producer
            .commit(
                OverwriteOperation {
                    deleted_file_paths,
                    snapshot_id,
                    removed_data_files: vec![],
                },
                DefaultManifestProcess,
            )
            .await
    }
}

struct OverwriteOperation {
    deleted_file_paths: HashSet<String>,
    snapshot_id: i64,
    removed_data_files: Vec<(DataFile, SchemaRef, PartitionSpecRef)>,
}

impl SnapshotProduceOperation for OverwriteOperation {
    fn operation(&self) -> Operation {
        Operation::Overwrite
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        Ok(vec![])
    }

    fn removed_data_files(&self) -> &[(DataFile, SchemaRef, PartitionSpecRef)] {
        &self.removed_data_files
    }

    async fn existing_manifest(
        &mut self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        let Some(snapshot) = snapshot_produce.table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot_produce
            .table
            .manifest_list_reader(snapshot)
            .load()
            .await?;

        if self.deleted_file_paths.is_empty() {
            return Ok(manifest_list
                .entries()
                .iter()
                .filter(|entry| {
                    // Delete-only manifests record which files were removed and must survive
                    // until `expire_snapshots` cleans them up (see #2148).
                    entry.has_added_files()
                        || entry.has_existing_files()
                        || entry.has_deleted_files()
                })
                .cloned()
                .collect());
        }

        let mut result = Vec::new();

        for manifest_file in manifest_list.entries() {
            if !manifest_file.has_added_files()
                && !manifest_file.has_existing_files()
                && !manifest_file.has_deleted_files()
            {
                continue;
            }

            let manifest = snapshot_produce
                .table
                .manifest_reader()
                .read(manifest_file)
                .await?;

            let has_deletes = manifest.entries().iter().any(|entry| {
                entry.is_alive() && self.deleted_file_paths.contains(entry.file_path())
            });

            if has_deletes {
                let rewritten = self
                    .rewrite_manifest(snapshot_produce, manifest_file, &manifest)
                    .await?;
                result.push(rewritten);
            } else {
                result.push(manifest_file.clone());
            }
        }

        Ok(result)
    }
}

impl OverwriteOperation {
    /// Rewrite a manifest, marking entries whose file paths are in `deleted_file_paths`
    /// as `ManifestStatus::Deleted`.
    async fn rewrite_manifest(
        &mut self,
        snapshot_produce: &SnapshotProducer<'_>,
        manifest_file: &ManifestFile,
        manifest: &Manifest,
    ) -> Result<ManifestFile> {
        let table = snapshot_produce.table;

        let new_manifest_path = format!(
            "{}/metadata/{}-m-overwrite.avro",
            table.metadata().location(),
            Uuid::now_v7(),
        );
        let output_file = table.file_io().new_output(&new_manifest_path)?;
        let partition_spec: PartitionSpecRef = Arc::new(manifest.metadata().partition_spec.clone());
        let builder = ManifestWriterBuilder::new(
            output_file,
            Some(self.snapshot_id),
            manifest.metadata().schema.clone(),
            manifest.metadata().partition_spec.clone(),
        );

        let mut writer = match table.metadata().format_version() {
            FormatVersion::V1 => builder.build_v1(),
            FormatVersion::V2 => match manifest_file.content {
                ManifestContentType::Data => builder.build_v2_data(),
                ManifestContentType::Deletes => builder.build_v2_deletes(),
            },
            FormatVersion::V3 => match manifest_file.content {
                ManifestContentType::Data => builder.build_v3_data(),
                ManifestContentType::Deletes => builder.build_v3_deletes(),
            },
        };

        for entry in manifest.entries() {
            let entry: ManifestEntry = (**entry).clone();
            if !entry.is_alive() {
                // A tombstone keeps its original snapshot id so the delete stays attributed
                // to the snapshot that made it.
                writer.add_deleted_entry(entry)?;
            } else if self.deleted_file_paths.contains(entry.file_path()) {
                let mut deleted = entry;
                deleted.snapshot_id = Some(self.snapshot_id);
                self.removed_data_files.push((
                    deleted.data_file().clone(),
                    manifest.metadata().schema.clone(),
                    partition_spec.clone(),
                ));
                writer.add_deleted_entry(deleted)?;
            } else {
                writer.add_existing_entry(entry)?;
            }
        }

        writer.write_manifest_file().await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, MAIN_BRANCH,
        ManifestEntryRef, ManifestStatus, Operation, PrimitiveType, SnapshotRef, Struct, Transform,
        Type, UnboundPartitionSpec,
    };
    use crate::table::Table;
    use crate::transaction::tests::{make_v2_minimal_table, make_v3_minimal_table_in_catalog};
    use crate::transaction::{AddColumn, ApplyTransactionAction, Transaction, TransactionAction};
    use crate::{TableRequirement, TableUpdate};

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

    async fn current_manifest_entry(table: &Table, path: &str) -> ManifestEntryRef {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();

        for manifest_file in manifest_list.entries() {
            let manifest = table.manifest_reader().read(manifest_file).await.unwrap();
            if let Some(entry) = manifest.entries().iter().find(|e| e.file_path() == path) {
                return entry.clone();
            }
        }
        panic!("no manifest entry for {path}");
    }

    #[tokio::test]
    async fn test_empty_data_overwrite_action() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx.overwrite().add_data_files(vec![]);
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_overwrite_snapshot_properties() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let mut snapshot_properties = HashMap::new();
        snapshot_properties.insert("key".to_string(), "val".to_string());

        let data_file = test_data_file(
            "test/1.parquet",
            table.metadata().default_partition_spec_id(),
        );

        let action = tx
            .overwrite()
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
                .get("key")
                .unwrap(),
            "val"
        );
    }

    #[tokio::test]
    async fn test_overwrite_incompatible_partition_value() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/3.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::string("test"))]))
            .build()
            .unwrap();

        let action = tx.overwrite().add_data_files(vec![data_file]);
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_overwrite_basic() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let data_file = test_data_file(
            "test/3.parquet",
            table.metadata().default_partition_spec_id(),
        );

        let action = tx.overwrite().add_data_files(vec![data_file.clone()]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let requirements = action_commit.take_requirements();

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

        let new_snapshot: SnapshotRef = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            SnapshotRef::new(snapshot.clone())
        } else {
            unreachable!()
        };
        assert_eq!(new_snapshot.summary().operation, Operation::Overwrite);

        let manifest_list = table
            .manifest_list_reader(&new_snapshot)
            .load()
            .await
            .unwrap();
        assert_eq!(1, manifest_list.entries().len());
        assert_eq!(
            manifest_list.entries()[0].sequence_number,
            new_snapshot.sequence_number()
        );

        let manifest = table
            .manifest_reader()
            .read(&manifest_list.entries()[0])
            .await
            .unwrap();
        assert_eq!(1, manifest.entries().len());
        assert_eq!(
            new_snapshot.sequence_number(),
            manifest.entries()[0]
                .sequence_number()
                .expect("Inherit sequence number by load manifest")
        );
        assert_eq!(
            new_snapshot.snapshot_id(),
            manifest.entries()[0].snapshot_id().unwrap()
        );
        assert_eq!(data_file, *manifest.entries()[0].data_file());
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

        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();
        assert_eq!(1, manifest_list.entries().len());

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

        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();

        assert_eq!(2, manifest_list.entries().len());

        let mut all_entries = vec![];
        for manifest_file in manifest_list.entries() {
            let manifest = table.manifest_reader().read(manifest_file).await.unwrap();
            for entry in manifest.entries() {
                all_entries.push((entry.status(), entry.file_path().to_string()));
            }
        }

        for original_file in ["test/original1.parquet", "test/original2.parquet"] {
            assert!(
                all_entries
                    .iter()
                    .any(|(status, path)| *status == ManifestStatus::Deleted
                        && path == original_file),
                "Original file {original_file} should be marked as Deleted, entries: {all_entries:?}",
            );
        }

        assert!(
            all_entries
                .iter()
                .any(|(status, path)| *status == ManifestStatus::Added
                    && path == "test/replacement.parquet"),
            "Replacement file should be marked as Added, entries: {all_entries:?}",
        );

        // Verify snapshot summary reports the deleted file.
        assert_eq!(
            snapshot
                .summary()
                .additional_properties
                .get("deleted-data-files")
                .map(|s| s.as_str()),
            Some("2")
        );
        assert_eq!(
            snapshot
                .summary()
                .additional_properties
                .get("deleted-records")
                .map(|s| s.as_str()),
            Some("2")
        );

        // Step 3: Fast append after overwrite — delete-only manifest must survive.
        let appended_file = test_data_file("test/appended.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![appended_file.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();

        // 3 manifests: rewritten (deleted entry), overwrite added, fast_append added.
        assert_eq!(3, manifest_list.entries().len());

        let mut all_entries = vec![];
        for manifest_file in manifest_list.entries() {
            let manifest = table.manifest_reader().read(manifest_file).await.unwrap();
            for entry in manifest.entries() {
                all_entries.push((entry.status(), entry.file_path().to_string()));
            }
        }

        // The deleted entry must still be present after fast_append.
        for original_file in ["test/original1.parquet", "test/original2.parquet"] {
            assert!(
                all_entries
                    .iter()
                    .any(|(status, path)| *status == ManifestStatus::Deleted
                        && path == original_file),
                "Deleted entry should survive fast_append, entries: {all_entries:?}",
            );
        }
    }

    #[tokio::test]
    async fn test_second_overwrite_keeps_prior_deleted_entries_deleted() {
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
        let tombstone = current_manifest_entry(&table, "test/a.parquet").await;
        assert_eq!(ManifestStatus::Deleted, tombstone.status());

        let tx = Transaction::new(&table);
        let action = tx
            .overwrite()
            .add_data_files(vec![test_data_file("test/d.parquet", spec_id)])
            .delete_data_files(vec![file_b.clone()]);
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

        let carried = current_manifest_entry(&table, "test/a.parquet").await;
        assert_eq!(
            ManifestStatus::Deleted,
            carried.status(),
            "the first overwrite's tombstone must stay Deleted"
        );
        assert_eq!(
            tombstone.snapshot_id, carried.snapshot_id,
            "the tombstone must keep the snapshot that deleted the file"
        );
        assert_eq!(
            (tombstone.sequence_number, tombstone.file_sequence_number),
            (carried.sequence_number, carried.file_sequence_number),
            "the tombstone must keep its original sequence numbers"
        );

        let entries = current_manifest_entries(&table).await;
        assert!(
            entries.contains(&(ManifestStatus::Deleted, "test/b.parquet".to_string())),
            "the second overwrite's own delete must be a tombstone, entries: {entries:?}"
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
                    .add_partition_field(2, "y", Transform::Identity)
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
    async fn test_overwrite_preserves_delete_only_manifest() {
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
            .delete_data_files(vec![file_a.clone()]);
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

        let tx = Transaction::new(&table);
        let action = tx
            .overwrite()
            .add_data_files(vec![test_data_file("test/c.parquet", spec_id)]);
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

        let entries = current_manifest_entries(&table).await;
        assert!(
            entries.contains(&(ManifestStatus::Deleted, "test/a.parquet".to_string())),
            "the delete-only manifest must survive a later overwrite, entries: {entries:?}"
        );
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
}
