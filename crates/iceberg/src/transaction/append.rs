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

/// FastAppendAction is a transaction action for fast append data files to the table.
pub struct FastAppendAction {
    check_duplicate: bool,
    // below are properties used to create SnapshotProducer when commit
    commit_uuid: Option<Uuid>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
}

impl FastAppendAction {
    pub(crate) fn new() -> Self {
        Self {
            check_duplicate: true,
            commit_uuid: None,
            snapshot_properties: HashMap::default(),
            added_data_files: vec![],
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
impl TransactionAction for FastAppendAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
        );

        // validate added files
        snapshot_producer.validate_added_data_files()?;

        // Checks duplicate files
        if self.check_duplicate {
            snapshot_producer.validate_duplicate_files().await?;
        }

        snapshot_producer
            .commit(FastAppendOperation, DefaultManifestProcess)
            .await
    }
}

struct FastAppendOperation;

impl SnapshotProduceOperation for FastAppendOperation {
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
        &mut self,
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

        Ok(manifest_list
            .entries()
            .iter()
            .filter(|entry| {
                // Keep delete-only manifests too: they record which files were removed and
                // must persist across snapshots until `expire_snapshots` cleans them up.
                // Dropping them lets the removed files reappear as live data (see #2148).
                entry.has_added_files() || entry.has_existing_files() || entry.has_deleted_files()
            })
            .cloned()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::sync::Arc;

    use minijinja::{AutoEscape, Environment, Value, context};
    use tempfile::TempDir;
    use uuid::Uuid;

    use crate::io::FileIO;
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, MAIN_BRANCH, ManifestEntry,
        ManifestListWriter, ManifestStatus, ManifestWriterBuilder, SnapshotRef, Struct,
        TableMetadata,
    };
    use crate::table::Table;
    use crate::test_utils::test_runtime;
    use crate::transaction::tests::make_v2_minimal_table;
    use crate::transaction::{Transaction, TransactionAction};
    use crate::{TableIdent, TableRequirement, TableUpdate};

    fn render_template(template: &str, ctx: Value) -> String {
        let mut env = Environment::new();
        env.set_auto_escape_callback(|_| AutoEscape::None);
        env.render_str(template, ctx).unwrap()
    }

    /// Builds a table whose current snapshot's manifest list contains a data manifest
    /// followed by a delete-only manifest (one entry with `ManifestStatus::Deleted`,
    /// so `deleted_files_count > 0` while `added_files_count == existing_files_count == 0`).
    ///
    /// Returns the table plus the `manifest_path` of the delete-only manifest so callers
    /// can assert whether a subsequent append carries it forward.
    async fn make_table_with_delete_only_manifest() -> (Table, TempDir, String) {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().join("table1");
        let manifest_list_location = table_location.join("metadata/manifests_list_1.avro");
        let table_metadata_location = table_location.join("metadata/v1.json");

        let file_io = FileIO::new_with_fs();

        let template = fs::read_to_string(format!(
            "{}/testdata/example_table_metadata_v2.json",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap();
        // The template has two snapshots; point the current one at our manifest list.
        let metadata_json = render_template(&template, context! {
            table_location => &table_location,
            manifest_list_1_location => &manifest_list_location,
            manifest_list_2_location => &manifest_list_location,
            table_metadata_1_location => &table_metadata_location,
        });
        let table_metadata = serde_json::from_str::<TableMetadata>(&metadata_json).unwrap();

        let table = Table::builder()
            .metadata(table_metadata)
            .identifier(TableIdent::from_strs(["db", "table1"]).unwrap())
            .file_io(file_io)
            .metadata_location(table_metadata_location.to_str().unwrap())
            .runtime(test_runtime())
            .build()
            .unwrap();

        let current_snapshot = table.metadata().current_snapshot().unwrap();
        let schema = current_snapshot.schema(table.metadata()).unwrap();
        let partition_spec = table.metadata().default_partition_spec();

        let next_manifest_file = |location: &str| {
            table
                .file_io()
                .new_output(format!(
                    "{}/metadata/manifest_{}.avro",
                    location,
                    Uuid::new_v4()
                ))
                .unwrap()
        };
        let table_location_str = table_location.to_str().unwrap().to_string();

        // Data manifest: one Added data file.
        let mut data_writer = ManifestWriterBuilder::new(
            next_manifest_file(&table_location_str),
            Some(current_snapshot.snapshot_id()),
            schema.clone(),
            partition_spec.as_ref().clone(),
        )
        .build_v2_data();
        data_writer
            .add_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::Data)
                            .file_path(format!("{table_location_str}/data.parquet"))
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(100)
                            .record_count(1)
                            .partition(Struct::from_iter([Some(Literal::long(100))]))
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap();
        let data_manifest = data_writer.write_manifest_file().await.unwrap();

        // Delete-only manifest: a single Deleted entry, nothing added or existing.
        let mut delete_writer = ManifestWriterBuilder::new(
            next_manifest_file(&table_location_str),
            Some(current_snapshot.snapshot_id()),
            schema.clone(),
            partition_spec.as_ref().clone(),
        )
        .build_v2_data();
        delete_writer
            .add_delete_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Deleted)
                    .sequence_number(0)
                    .file_sequence_number(0)
                    .data_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::Data)
                            .file_path(format!("{table_location_str}/removed.parquet"))
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(100)
                            .record_count(1)
                            .partition(Struct::from_iter([Some(Literal::long(100))]))
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap();
        let delete_manifest = delete_writer.write_manifest_file().await.unwrap();
        let delete_manifest_path = delete_manifest.manifest_path.clone();

        // Sanity: the delete manifest really is delete-only.
        assert!(delete_manifest.has_deleted_files());
        assert!(!delete_manifest.has_added_files());
        assert!(!delete_manifest.has_existing_files());

        let mut manifest_list_write = ManifestListWriter::v2(
            table
                .file_io()
                .new_output(current_snapshot.manifest_list())
                .unwrap()
                .writer()
                .await
                .unwrap(),
            current_snapshot.snapshot_id(),
            current_snapshot.parent_snapshot_id(),
            current_snapshot.sequence_number(),
        );
        manifest_list_write
            .add_manifests(vec![data_manifest, delete_manifest].into_iter())
            .unwrap();
        manifest_list_write.close().await.unwrap();

        (table, tmp_dir, delete_manifest_path)
    }

    /// Regression test for #2148: a `fast_append` must carry delete-only manifests
    /// forward into the new snapshot. Dropping them lets the files they mark as
    /// removed reappear as live data on the next append.
    #[tokio::test]
    async fn test_fast_append_preserves_delete_only_manifest() {
        let (table, _tmp_dir, delete_manifest_path) = make_table_with_delete_only_manifest().await;

        // Append a new data file via the public transaction API.
        let new_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(format!("{}/appended.parquet", table.metadata().location()))
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(100))]))
            .build()
            .unwrap();

        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![new_file]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_snapshot: SnapshotRef = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            SnapshotRef::new(snapshot.clone())
        } else {
            unreachable!("first update of a fast append should be AddSnapshot")
        };

        let manifest_list = table
            .manifest_list_reader(&new_snapshot)
            .load()
            .await
            .unwrap();

        assert!(
            manifest_list
                .entries()
                .iter()
                .any(|m| m.manifest_path == delete_manifest_path),
            "delete-only manifest {delete_manifest_path} was dropped from the new snapshot's \
             manifest list; the files it removed would reappear as live data"
        );
    }

    #[tokio::test]
    async fn test_empty_data_append_action() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![]);
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_set_snapshot_properties() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let mut snapshot_properties = HashMap::new();
        snapshot_properties.insert("key".to_string(), "val".to_string());

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

        let action = tx
            .fast_append()
            .set_snapshot_properties(snapshot_properties)
            .add_data_files(vec![data_file]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        // Check customized properties is contained in snapshot summary properties.
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
    async fn test_snapshot_properties_cannot_override_computed_metrics() {
        // A user-supplied snapshot property must not shadow a computed metric key
        // such as `added-data-files`. Matching iceberg-java, the computed value
        // wins, so the summary reflects the real count and a bad value can neither
        // corrupt the summary nor panic total computation (see #2184-adjacent fix).
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let mut snapshot_properties = HashMap::new();
        // Both a benign-but-wrong value and a non-integer value collide with
        // computed metric keys; neither should reach the final summary.
        snapshot_properties.insert("added-data-files".to_string(), "9999".to_string());
        snapshot_properties.insert("added-records".to_string(), "not-a-number".to_string());

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

        let action = tx
            .fast_append()
            .set_snapshot_properties(snapshot_properties)
            .add_data_files(vec![data_file]);
        // Must not panic during total computation.
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            unreachable!()
        };
        let props = &new_snapshot.summary().additional_properties;

        // Computed metric wins over the user's colliding values.
        assert_eq!(
            props.get("added-data-files").unwrap(),
            "1",
            "computed added-data-files must override the user-supplied value"
        );
        assert_eq!(
            props.get("added-records").unwrap(),
            "1",
            "computed added-records must override the user-supplied non-integer value"
        );
    }

    #[tokio::test]
    async fn test_append_snapshot_properties() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let mut snapshot_properties = HashMap::new();
        snapshot_properties.insert("key".to_string(), "val".to_string());

        let action = tx
            .fast_append()
            .set_snapshot_properties(snapshot_properties);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        // Check customized properties is contained in snapshot summary properties.
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
    async fn test_fast_append_file_with_incompatible_partition_value() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx.fast_append();

        // check add data file with incompatible partition value
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

        let action = action.add_data_files(vec![data_file.clone()]);

        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_fast_append() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx.fast_append();

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/3.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let action = action.add_data_files(vec![data_file.clone()]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let requirements = action_commit.take_requirements();

        // check updates and requirements
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

        // check manifest list
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
        assert_eq!(1, manifest_list.entries().len());
        assert_eq!(
            manifest_list.entries()[0].sequence_number,
            new_snapshot.sequence_number()
        );

        // check manifest
        let manifest = manifest_list.entries()[0]
            .load_manifest(table.file_io())
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
}
