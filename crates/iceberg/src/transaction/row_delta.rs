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

/// RowDeltaAction is a transaction action that commits data files and delete
/// files (position or equality deletes) together in a single `overwrite`
/// snapshot.
///
/// This is the write-side primitive for merge-on-read row-level changes,
/// equivalent to `Table.newRowDelta()` in iceberg-java: both the added data
/// files and the added delete files receive the new snapshot's sequence
/// number, so the delete files apply to data with strictly smaller sequence
/// numbers only. Data files committed in the same row delta are therefore
/// not affected by its own delete files, which gives upsert semantics when a
/// row delta pairs equality deletes with the rows' new values.
pub struct RowDeltaAction {
    check_duplicate: bool,
    // below are properties used to create SnapshotProducer when commit
    commit_uuid: Option<Uuid>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    added_delete_files: Vec<DataFile>,
}

impl RowDeltaAction {
    pub(crate) fn new() -> Self {
        Self {
            check_duplicate: true,
            commit_uuid: None,
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

    /// Add delete files (position or equality deletes) to the snapshot.
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
}

#[async_trait]
impl TransactionAction for RowDeltaAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
            self.added_delete_files.clone(),
        );

        // validate added files
        snapshot_producer.validate_added_data_files()?;
        snapshot_producer.validate_added_delete_files()?;

        // Checks duplicate files
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
        // Matches iceberg-java's BaseRowDelta: a row delta commits both data
        // and delete files, so the snapshot operation is `overwrite`.
        Operation::Overwrite
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
    use std::sync::Arc;

    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, MAIN_BRANCH,
        ManifestContentType, Operation, SnapshotRef, Struct,
    };
    use crate::transaction::tests::make_v2_minimal_table;
    use crate::transaction::{Transaction, TransactionAction};
    use crate::{TableRequirement, TableUpdate};

    fn test_data_file(table: &crate::table::Table) -> crate::spec::DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap()
    }

    fn test_equality_delete_file(table: &crate::table::Table) -> crate::spec::DataFile {
        DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path("test/1-deletes.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(50)
            .record_count(1)
            .equality_ids(Some(vec![1]))
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_empty_row_delta_action() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx.row_delta();
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_row_delta_rejects_data_file_in_delete_list() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_delete_files(vec![test_data_file(&table)]);
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_row_delta_rejects_delete_file_in_data_list() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_data_files(vec![test_equality_delete_file(&table)]);
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_row_delta() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let data_file = test_data_file(&table);
        let delete_file = test_equality_delete_file(&table);

        let action = tx
            .row_delta()
            .add_data_files(vec![data_file.clone()])
            .add_delete_files(vec![delete_file.clone()]);
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

        // a row delta commits an `overwrite` snapshot
        let new_snapshot: SnapshotRef = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            SnapshotRef::new(snapshot.clone())
        } else {
            unreachable!()
        };
        assert_eq!(new_snapshot.summary().operation, Operation::Overwrite);
        assert_eq!(
            new_snapshot
                .summary()
                .additional_properties
                .get("added-delete-files")
                .map(String::as_str),
            Some("1")
        );

        // check manifest list: one data manifest and one delete manifest
        let manifest_list = table
            .manifest_list_reader(&new_snapshot)
            .load()
            .await
            .unwrap();
        assert_eq!(2, manifest_list.entries().len());

        let data_manifest_file = manifest_list
            .entries()
            .iter()
            .find(|m| m.content == ManifestContentType::Data)
            .expect("expected a data manifest");
        let delete_manifest_file = manifest_list
            .entries()
            .iter()
            .find(|m| m.content == ManifestContentType::Deletes)
            .expect("expected a delete manifest");

        // both manifests carry the new snapshot's sequence number, so the
        // delete files apply to strictly older data only
        assert_eq!(
            data_manifest_file.sequence_number,
            new_snapshot.sequence_number()
        );
        assert_eq!(
            delete_manifest_file.sequence_number,
            new_snapshot.sequence_number()
        );

        // check the delete manifest contents
        let delete_manifest = delete_manifest_file
            .load_manifest(table.file_io())
            .await
            .unwrap();
        assert_eq!(1, delete_manifest.entries().len());
        assert_eq!(
            new_snapshot.sequence_number(),
            delete_manifest.entries()[0]
                .sequence_number()
                .expect("Inherit sequence number by load manifest")
        );
        assert_eq!(delete_file, *delete_manifest.entries()[0].data_file());

        // check the data manifest contents
        let data_manifest = data_manifest_file
            .load_manifest(table.file_io())
            .await
            .unwrap();
        assert_eq!(1, data_manifest.entries().len());
        assert_eq!(data_file, *data_manifest.entries()[0].data_file());
    }

    #[tokio::test]
    async fn test_row_delta_deletes_only() {
        // A row delta with only delete files is valid (pure row-level delete).
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let delete_file = test_equality_delete_file(&table);
        let action = tx.row_delta().add_delete_files(vec![delete_file]);
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
        assert_eq!(1, manifest_list.entries().len());
        assert_eq!(
            manifest_list.entries()[0].content,
            ManifestContentType::Deletes
        );
    }

    #[tokio::test]
    async fn test_row_delta_write_and_read_back() {
        // End-to-end merge-on-read round trip: append rows, then commit a row
        // delta pairing an equality delete with a replacement row (an upsert),
        // and read the table back through the scan path.
        use arrow_array::{Int64Array, RecordBatch, StringArray};
        use futures::TryStreamExt;
        use parquet::file::properties::WriterProperties;

        use crate::arrow::schema_to_arrow_schema;
        use crate::memory::tests::new_memory_catalog;
        use crate::spec::{NestedField, PrimitiveType, Schema, Type};
        use crate::transaction::ApplyTransactionAction;
        use crate::writer::base_writer::data_file_writer::DataFileWriterBuilder;
        use crate::writer::base_writer::equality_delete_writer::{
            EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig,
        };
        use crate::writer::file_writer::ParquetWriterBuilder;
        use crate::writer::file_writer::location_generator::{
            DefaultFileNameGenerator, DefaultLocationGenerator,
        };
        use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
        use crate::writer::{IcebergWriter, IcebergWriterBuilder};
        use crate::{Catalog, TableCreation, TableIdent};

        let catalog = new_memory_catalog().await;

        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(2, "data", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        let table_ident = TableIdent::from_strs(["nsrd", "rowdelta"]).unwrap();
        catalog
            .create_namespace(table_ident.namespace(), std::collections::HashMap::new())
            .await
            .unwrap();
        let table = catalog
            .create_table(
                table_ident.namespace(),
                TableCreation::builder()
                    .name(table_ident.name().to_string())
                    .schema(schema.clone())
                    .build(),
            )
            .await
            .unwrap();

        let arrow_schema =
            std::sync::Arc::new(schema_to_arrow_schema(table.metadata().current_schema()).unwrap());
        let location_gen = DefaultLocationGenerator::new(table.metadata()).unwrap();

        let new_data_writer = |prefix: &str| {
            let file_name_gen = DefaultFileNameGenerator::new(
                prefix.to_string(),
                None,
                crate::spec::DataFileFormat::Parquet,
            );
            let parquet_builder = ParquetWriterBuilder::new(
                WriterProperties::builder().build(),
                table.metadata().current_schema().clone(),
            );
            DataFileWriterBuilder::new(RollingFileWriterBuilder::new_with_default_file_size(
                parquet_builder,
                table.file_io().clone(),
                location_gen.clone(),
                file_name_gen,
            ))
        };

        // Commit 1: fast append rows (1, "a"), (2, "b"), (3, "c").
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![
            std::sync::Arc::new(Int64Array::from(vec![1, 2, 3])),
            std::sync::Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
        ])
        .unwrap();
        let mut writer = new_data_writer("base").build(None).await.unwrap();
        writer.write(batch).await.unwrap();
        let base_files = writer.close().await.unwrap();

        let tx = Transaction::new(&table);
        let tx = tx
            .fast_append()
            .add_data_files(base_files)
            .apply(tx)
            .unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Commit 2: a row delta that upserts id=2 -> (2, "b2"): an equality
        // delete for id=2 plus a data file with the replacement row.
        let equality_config =
            EqualityDeleteWriterConfig::new(vec![1], table.metadata().current_schema().clone())
                .unwrap();
        let delete_arrow_schema = equality_config.projected_arrow_schema_ref().clone();
        let delete_iceberg_schema = std::sync::Arc::new(
            crate::arrow::arrow_schema_to_schema(&delete_arrow_schema).unwrap(),
        );
        let delete_parquet_builder =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), delete_iceberg_schema);
        let delete_file_name_gen = DefaultFileNameGenerator::new(
            "delete".to_string(),
            None,
            crate::spec::DataFileFormat::Parquet,
        );
        let mut delete_writer = EqualityDeleteFileWriterBuilder::new(
            RollingFileWriterBuilder::new_with_default_file_size(
                delete_parquet_builder,
                table.file_io().clone(),
                location_gen.clone(),
                delete_file_name_gen,
            ),
            equality_config,
        )
        .build(None)
        .await
        .unwrap();
        // The equality delete writer projects the equality columns itself, so
        // it receives full-schema batches carrying the keys to delete.
        let delete_batch = RecordBatch::try_new(arrow_schema.clone(), vec![
            std::sync::Arc::new(Int64Array::from(vec![2])),
            std::sync::Arc::new(StringArray::from(vec![Some("b")])),
        ])
        .unwrap();
        delete_writer.write(delete_batch).await.unwrap();
        let delete_files = delete_writer.close().await.unwrap();
        assert!(
            delete_files
                .iter()
                .all(|f| f.content_type() == DataContentType::EqualityDeletes)
        );

        let upsert_batch = RecordBatch::try_new(arrow_schema.clone(), vec![
            std::sync::Arc::new(Int64Array::from(vec![2])),
            std::sync::Arc::new(StringArray::from(vec![Some("b2")])),
        ])
        .unwrap();
        let mut writer = new_data_writer("upsert").build(None).await.unwrap();
        writer.write(upsert_batch).await.unwrap();
        let upsert_files = writer.close().await.unwrap();

        let tx = Transaction::new(&table);
        let tx = tx
            .row_delta()
            .add_data_files(upsert_files)
            .add_delete_files(delete_files)
            .apply(tx)
            .unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Overwrite
        );

        // Read back through the scan path: the equality delete must hide the
        // old (2, "b") row while the same commit's (2, "b2") row survives.
        let batches: Vec<RecordBatch> = table
            .scan()
            .select_all()
            .build()
            .unwrap()
            .to_arrow()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        let mut rows: Vec<(i64, String)> = batches
            .iter()
            .flat_map(|b| {
                let ids = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .map(|v| v.unwrap());
                let data = b
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .map(|v| v.unwrap().to_string());
                ids.zip(data).collect::<Vec<_>>()
            })
            .collect();
        rows.sort();

        assert_eq!(rows, vec![
            (1, "a".to_string()),
            (2, "b2".to_string()),
            (3, "c".to_string())
        ]);
    }
}
