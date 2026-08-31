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

//! Shared test fixtures for the table scan API.

use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
};
use minijinja::value::Value;
use minijinja::{AutoEscape, Environment, context};
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use tempfile::TempDir;
use uuid::Uuid;

use crate::TableIdent;
use crate::io::{FileIO, OutputFile};
use crate::metadata_columns::{
    RESERVED_COL_NAME_DELETE_FILE_PATH, RESERVED_COL_NAME_DELETE_FILE_POS,
    RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER, RESERVED_FIELD_ID_DELETE_FILE_PATH,
    RESERVED_FIELD_ID_DELETE_FILE_POS,
};
use crate::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, FormatVersion, Literal, ManifestEntry,
    ManifestListWriter, ManifestStatus, ManifestWriterBuilder, PartitionSpec, Struct, StructType,
    TableMetadata, TableMetadataBuilder,
};
use crate::table::Table;
use crate::test_utils::test_runtime;

fn render_template(template: &str, ctx: Value) -> String {
    let mut env = Environment::new();
    env.set_auto_escape_callback(|_| AutoEscape::None);
    env.render_str(template, ctx).unwrap()
}

/// Asserts every row of the `_last_updated_sequence_number` column across all
/// batches equals `expected` (or is null when `expected` is `None`), decoding
/// the logical value independent of the physical (run-end) encoding.
pub fn assert_last_updated_seq_all(batches: &[RecordBatch], expected: Option<i64>) {
    use arrow_cast::cast;
    use arrow_schema::DataType;
    for batch in batches {
        let col = batch
            .column_by_name(RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER)
            .expect("_last_updated_sequence_number column should be present");
        let logical = cast(col, &DataType::Int64).unwrap();
        let values = logical.as_primitive::<arrow_array::types::Int64Type>();
        for i in 0..values.len() {
            let actual = (!values.is_null(i)).then(|| values.value(i));
            assert_eq!(actual, expected, "row {i}");
        }
    }
}

pub struct TableTestFixture {
    pub table_location: String,
    pub table: Table,
}

impl TableTestFixture {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().join("table1");
        let manifest_list1_location = table_location.join("metadata/manifests_list_1.avro");
        let manifest_list2_location = table_location.join("metadata/manifests_list_2.avro");
        let table_metadata1_location = table_location.join("metadata/v1.json");

        let file_io = FileIO::new_with_fs();

        let table_metadata = {
            let template_json_str = fs::read_to_string(format!(
                "{}/testdata/example_table_metadata_v2.json",
                env!("CARGO_MANIFEST_DIR")
            ))
            .unwrap();
            let metadata_json = render_template(&template_json_str, context! {
                table_location => &table_location,
                manifest_list_1_location => &manifest_list1_location,
                manifest_list_2_location => &manifest_list2_location,
                table_metadata_1_location => &table_metadata1_location,
            });
            serde_json::from_str::<TableMetadata>(&metadata_json).unwrap()
        };

        let table = Table::builder()
            .metadata(table_metadata)
            .identifier(TableIdent::from_strs(["db", "table1"]).unwrap())
            .file_io(file_io.clone())
            .metadata_location(table_metadata1_location.as_os_str().to_str().unwrap())
            .runtime(test_runtime())
            .build()
            .unwrap();

        Self {
            table_location: table_location.to_str().unwrap().to_string(),
            table,
        }
    }

    #[allow(clippy::new_without_default)]
    pub fn new_empty() -> Self {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().join("table1");
        let table_metadata1_location = table_location.join("metadata/v1.json");

        let file_io = FileIO::new_with_fs();

        let table_metadata = {
            let template_json_str = fs::read_to_string(format!(
                "{}/testdata/example_empty_table_metadata_v2.json",
                env!("CARGO_MANIFEST_DIR")
            ))
            .unwrap();
            let metadata_json = render_template(&template_json_str, context! {
                table_location => &table_location,
                table_metadata_1_location => &table_metadata1_location,
            });
            serde_json::from_str::<TableMetadata>(&metadata_json).unwrap()
        };

        let table = Table::builder()
            .metadata(table_metadata)
            .identifier(TableIdent::from_strs(["db", "table1"]).unwrap())
            .file_io(file_io.clone())
            .metadata_location(table_metadata1_location.as_os_str().to_str().unwrap())
            .runtime(test_runtime())
            .build()
            .unwrap();

        Self {
            table_location: table_location.to_str().unwrap().to_string(),
            table,
        }
    }

    /// Creates a fixture with 5 snapshots chained as:
    ///   S1 (root) -> S2 -> S3 -> S4 -> S5 (current)
    /// Useful for testing snapshot history traversal.
    pub fn new_with_deep_history() -> Self {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().join("table1");
        let table_metadata1_location = table_location.join("metadata/v1.json");

        let file_io = FileIO::new_with_fs();

        let table_metadata = {
            let json_str = fs::read_to_string(format!(
                "{}/testdata/example_table_metadata_v2_deep_history.json",
                env!("CARGO_MANIFEST_DIR")
            ))
            .unwrap();
            serde_json::from_str::<TableMetadata>(&json_str).unwrap()
        };

        let table = Table::builder()
            .metadata(table_metadata)
            .identifier(TableIdent::from_strs(["db", "table1"]).unwrap())
            .file_io(file_io.clone())
            .metadata_location(table_metadata1_location.as_os_str().to_str().unwrap())
            .runtime(test_runtime())
            .build()
            .unwrap();

        Self {
            table_location: table_location.to_str().unwrap().to_string(),
            table,
        }
    }

    pub fn new_unpartitioned() -> Self {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().join("table1");
        let manifest_list1_location = table_location.join("metadata/manifests_list_1.avro");
        let manifest_list2_location = table_location.join("metadata/manifests_list_2.avro");
        let table_metadata1_location = table_location.join("metadata/v1.json");

        let file_io = FileIO::new_with_fs();

        let mut table_metadata = {
            let template_json_str = fs::read_to_string(format!(
                "{}/testdata/example_table_metadata_v2.json",
                env!("CARGO_MANIFEST_DIR")
            ))
            .unwrap();
            let metadata_json = render_template(&template_json_str, context! {
                table_location => &table_location,
                manifest_list_1_location => &manifest_list1_location,
                manifest_list_2_location => &manifest_list2_location,
                table_metadata_1_location => &table_metadata1_location,
            });
            serde_json::from_str::<TableMetadata>(&metadata_json).unwrap()
        };

        table_metadata.default_spec = Arc::new(PartitionSpec::unpartition_spec());
        table_metadata.partition_specs.clear();
        table_metadata.default_partition_type = StructType::new(vec![]);
        table_metadata
            .partition_specs
            .insert(0, table_metadata.default_spec.clone());

        let table = Table::builder()
            .metadata(table_metadata)
            .identifier(TableIdent::from_strs(["db", "table1"]).unwrap())
            .file_io(file_io.clone())
            .metadata_location(table_metadata1_location.to_str().unwrap())
            .runtime(test_runtime())
            .build()
            .unwrap();

        Self {
            table_location: table_location.to_str().unwrap().to_string(),
            table,
        }
    }

    pub fn new_with_partition_evolution() -> Self {
        let table = Self::new().table;
        let table_location = table.metadata().location.clone();

        let manifest_list1_location = format!("{}/metadata/manifests_list_1.avro", table_location);
        let manifest_list2_location = format!("{}/metadata/manifests_list_2.avro", table_location);
        let manifest_list3_location = format!("{}/metadata/manifests_list_3.avro", table_location);
        let table_metadata1_location = format!("{}/metadata/v1.json", table_location);

        let new_table_metadata = {
            let template_json_str = fs::read_to_string(format!(
                "{}/testdata/example_table_metadata_v2_partition_evolution.json",
                env!("CARGO_MANIFEST_DIR")
            ))
            .unwrap();
            let metadata_json = render_template(&template_json_str, context! {
                table_location => &table_location,
                manifest_list_1_location => &manifest_list1_location,
                manifest_list_2_location => &manifest_list2_location,
                manifest_list_3_location => &manifest_list3_location,
                table_metadata_1_location => &table_metadata1_location,
            });
            Arc::new(serde_json::from_str::<TableMetadata>(&metadata_json).unwrap())
        };

        Self {
            table_location,
            table: table.with_metadata(new_table_metadata),
        }
    }

    fn next_manifest_file(&self) -> OutputFile {
        self.table
            .file_io()
            .new_output(format!(
                "{}/metadata/manifest_{}.avro",
                self.table_location,
                Uuid::new_v4()
            ))
            .unwrap()
    }

    pub async fn setup_manifest_files(&mut self) {
        let current_snapshot = self.table.metadata().current_snapshot().unwrap();
        let parent_snapshot = current_snapshot
            .parent_snapshot(self.table.metadata())
            .unwrap();
        let current_schema = current_snapshot.schema(self.table.metadata()).unwrap();
        let current_partition_spec = self.table.metadata().default_partition_spec();

        // Write the data files first, then use the file size in the manifest entries
        let parquet_file_size = self.write_parquet_data_files();

        let mut writer = ManifestWriterBuilder::new(
            self.next_manifest_file(),
            Some(current_snapshot.snapshot_id()),
            current_schema.clone(),
            current_partition_spec.as_ref().clone(),
        )
        .build_v2_data();
        writer
            .add_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::Data)
                            .file_path(format!("{}/1.parquet", &self.table_location))
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(parquet_file_size)
                            .record_count(1)
                            .partition(Struct::from_iter([Some(Literal::long(100))]))
                            .key_metadata(None)
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap();
        writer
            .add_delete_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Deleted)
                    .snapshot_id(parent_snapshot.snapshot_id())
                    .sequence_number(parent_snapshot.sequence_number())
                    .file_sequence_number(parent_snapshot.sequence_number())
                    .data_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::Data)
                            .file_path(format!("{}/2.parquet", &self.table_location))
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(parquet_file_size)
                            .record_count(1)
                            .partition(Struct::from_iter([Some(Literal::long(200))]))
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap();
        writer
            .add_existing_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Existing)
                    .snapshot_id(parent_snapshot.snapshot_id())
                    .sequence_number(parent_snapshot.sequence_number())
                    .file_sequence_number(parent_snapshot.sequence_number())
                    .data_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::Data)
                            .file_path(format!("{}/3.parquet", &self.table_location))
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(parquet_file_size)
                            .record_count(1)
                            .partition(Struct::from_iter([Some(Literal::long(300))]))
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap();
        let data_file_manifest = writer.write_manifest_file().await.unwrap();

        // Write to manifest list
        let manifest_list_writer = self
            .table
            .file_io()
            .new_output(current_snapshot.manifest_list())
            .unwrap()
            .writer()
            .await
            .unwrap();
        let mut manifest_list_write = ManifestListWriter::v2(
            manifest_list_writer,
            current_snapshot.snapshot_id(),
            current_snapshot.parent_snapshot_id(),
            current_snapshot.sequence_number(),
        );
        manifest_list_write
            .add_manifests(vec![data_file_manifest].into_iter())
            .unwrap();
        manifest_list_write.close().await.unwrap();
    }

    /// Writes a v3 data manifest with a manifest-level `first_row_id` of 42,
    /// so live entries inherit a per-file `first_row_id` on read. Upgrades the
    /// table to v3 first, so the manifest list is read as v3.
    pub async fn setup_v3_manifest_files(&mut self) {
        let metadata = TableMetadataBuilder::new_from_metadata(
            self.table.metadata().clone(),
            self.table.metadata_location().map(str::to_string),
        )
        .upgrade_format_version(FormatVersion::V3)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        self.table = Table::builder()
            .metadata(metadata)
            .identifier(self.table.identifier().clone())
            .file_io(self.table.file_io().clone())
            .metadata_location(self.table.metadata_location().unwrap().to_string())
            .runtime(test_runtime())
            .build()
            .unwrap();

        let current_snapshot = self.table.metadata().current_snapshot().unwrap();
        let current_schema = current_snapshot.schema(self.table.metadata()).unwrap();
        let current_partition_spec = self.table.metadata().default_partition_spec();

        let parquet_file_size = self.write_parquet_data_files();

        let mut writer = ManifestWriterBuilder::new(
            self.next_manifest_file(),
            Some(current_snapshot.snapshot_id()),
            current_schema.clone(),
            current_partition_spec.as_ref().clone(),
        )
        .build_v3_data();
        writer
            .add_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::Data)
                            .file_path(format!("{}/1.parquet", &self.table_location))
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(parquet_file_size)
                            .record_count(1)
                            .partition(Struct::from_iter([Some(Literal::long(100))]))
                            .key_metadata(None)
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap();
        let data_file_manifest = writer.write_manifest_file().await.unwrap();

        let manifest_list_writer = self
            .table
            .file_io()
            .new_output(current_snapshot.manifest_list())
            .unwrap()
            .writer()
            .await
            .unwrap();
        let mut manifest_list_write = ManifestListWriter::v3(
            manifest_list_writer,
            current_snapshot.snapshot_id(),
            current_snapshot.parent_snapshot_id(),
            current_snapshot.sequence_number(),
            Some(42),
        );
        manifest_list_write
            .add_manifests(vec![data_file_manifest].into_iter())
            .unwrap();
        manifest_list_write.close().await.unwrap();
    }

    pub async fn setup_manifest_files_with_partition_evolution(&mut self) {
        let current_snapshot = self.table.metadata().current_snapshot().unwrap();
        let parent_snapshot = current_snapshot
            .parent_snapshot(self.table.metadata())
            .unwrap();
        let current_schema = current_snapshot.schema(self.table.metadata()).unwrap();
        let current_partition_spec = self.table.metadata().default_partition_spec();

        // Write the data files first, then use the file size in the manifest entries
        let parquet_file_size = self.write_parquet_data_files();

        let mut writer = ManifestWriterBuilder::new(
            self.next_manifest_file(),
            Some(current_snapshot.snapshot_id()),
            current_schema.clone(),
            current_partition_spec.as_ref().clone(),
        )
        .build_v2_data();
        writer
            .add_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(
                        DataFileBuilder::default()
                            .partition_spec_id(1)
                            .content(DataContentType::Data)
                            .file_path(format!("{}/1.parquet", &self.table_location))
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(parquet_file_size)
                            .record_count(1)
                            .partition(Struct::from_iter([
                                Some(Literal::long(100)),
                                Some(Literal::string("apa")),
                                Some(Literal::int(27)),
                            ]))
                            .key_metadata(None)
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap();
        writer
            .add_delete_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Deleted)
                    .snapshot_id(parent_snapshot.snapshot_id())
                    .sequence_number(parent_snapshot.sequence_number())
                    .file_sequence_number(parent_snapshot.sequence_number())
                    .data_file(
                        DataFileBuilder::default()
                            .partition_spec_id(1)
                            .content(DataContentType::Data)
                            .file_path(format!("{}/2.parquet", &self.table_location))
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(parquet_file_size)
                            .record_count(1)
                            .partition(Struct::from_iter([
                                Some(Literal::long(200)),
                                Some(Literal::string("ice")),
                                Some(Literal::int(5)),
                            ]))
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap();
        writer
            .add_existing_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Existing)
                    .snapshot_id(parent_snapshot.snapshot_id())
                    .sequence_number(parent_snapshot.sequence_number())
                    .file_sequence_number(parent_snapshot.sequence_number())
                    .data_file(
                        DataFileBuilder::default()
                            .partition_spec_id(1)
                            .content(DataContentType::Data)
                            .file_path(format!("{}/3.parquet", &self.table_location))
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(parquet_file_size)
                            .record_count(1)
                            .partition(Struct::from_iter([
                                Some(Literal::long(300)),
                                Some(Literal::string("apa")),
                                Some(Literal::int(19)),
                            ]))
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap();
        let data_file_manifest = writer.write_manifest_file().await.unwrap();

        // Write to manifest list
        let manifest_list_writer = self
            .table
            .file_io()
            .new_output(current_snapshot.manifest_list())
            .unwrap()
            .writer()
            .await
            .unwrap();
        let mut manifest_list_write = ManifestListWriter::v2(
            manifest_list_writer,
            current_snapshot.snapshot_id(),
            current_snapshot.parent_snapshot_id(),
            current_snapshot.sequence_number(),
        );
        manifest_list_write
            .add_manifests(vec![data_file_manifest].into_iter())
            .unwrap();
        manifest_list_write.close().await.unwrap();
    }

    /// Writes identical Parquet data files (1.parquet, 2.parquet, 3.parquet)
    /// and returns the file size in bytes.
    fn write_parquet_data_files(&self) -> u64 {
        fs::create_dir_all(&self.table_location).unwrap();

        let schema = {
            let fields = vec![
                arrow_schema::Field::new("x", arrow_schema::DataType::Int64, false).with_metadata(
                    HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string())]),
                ),
                arrow_schema::Field::new("y", arrow_schema::DataType::Int64, false).with_metadata(
                    HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "2".to_string())]),
                ),
                arrow_schema::Field::new("z", arrow_schema::DataType::Int64, false).with_metadata(
                    HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "3".to_string())]),
                ),
                arrow_schema::Field::new("a", arrow_schema::DataType::Utf8, false).with_metadata(
                    HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "4".to_string())]),
                ),
                arrow_schema::Field::new("dbl", arrow_schema::DataType::Float64, false)
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "5".to_string(),
                    )])),
                arrow_schema::Field::new("i32", arrow_schema::DataType::Int32, false)
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "6".to_string(),
                    )])),
                arrow_schema::Field::new("i64", arrow_schema::DataType::Int64, false)
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "7".to_string(),
                    )])),
                arrow_schema::Field::new("bool", arrow_schema::DataType::Boolean, false)
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "8".to_string(),
                    )])),
            ];
            Arc::new(arrow_schema::Schema::new(fields))
        };
        // x: [1, 1, 1, 1, ...]
        let col1 = Arc::new(Int64Array::from_iter_values(vec![1; 1024])) as ArrayRef;

        let mut values = vec![2; 512];
        values.append(vec![3; 200].as_mut());
        values.append(vec![4; 300].as_mut());
        values.append(vec![5; 12].as_mut());

        // y: [2, 2, 2, 2, ..., 3, 3, 3, 3, ..., 4, 4, 4, 4, ..., 5, 5, 5, 5]
        let col2 = Arc::new(Int64Array::from_iter_values(values)) as ArrayRef;

        let mut values = vec![3; 512];
        values.append(vec![4; 512].as_mut());

        // z: [3, 3, 3, 3, ..., 4, 4, 4, 4]
        let col3 = Arc::new(Int64Array::from_iter_values(values)) as ArrayRef;

        // a: ["Apache", "Apache", "Apache", ..., "Iceberg", "Iceberg", "Iceberg"]
        let mut values = vec!["Apache"; 512];
        values.append(vec!["Iceberg"; 512].as_mut());
        let col4 = Arc::new(StringArray::from_iter_values(values)) as ArrayRef;

        // dbl:
        let mut values = vec![100.0f64; 512];
        values.append(vec![150.0f64; 12].as_mut());
        values.append(vec![200.0f64; 500].as_mut());
        let col5 = Arc::new(Float64Array::from_iter_values(values)) as ArrayRef;

        // i32:
        let mut values = vec![100i32; 512];
        values.append(vec![150i32; 12].as_mut());
        values.append(vec![200i32; 500].as_mut());
        let col6 = Arc::new(Int32Array::from_iter_values(values)) as ArrayRef;

        // i64:
        let mut values = vec![100i64; 512];
        values.append(vec![150i64; 12].as_mut());
        values.append(vec![200i64; 500].as_mut());
        let col7 = Arc::new(Int64Array::from_iter_values(values)) as ArrayRef;

        // bool:
        let mut values = vec![false; 512];
        values.append(vec![true; 512].as_mut());
        let values: BooleanArray = values.into();
        let col8 = Arc::new(values) as ArrayRef;

        let to_write = RecordBatch::try_new(schema.clone(), vec![
            col1, col2, col3, col4, col5, col6, col7, col8,
        ])
        .unwrap();

        // Write the Parquet files
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        for n in 1..=3 {
            let file = File::create(format!("{}/{}.parquet", &self.table_location, n)).unwrap();
            let mut writer =
                ArrowWriter::try_new(file, to_write.schema(), Some(props.clone())).unwrap();

            writer.write(&to_write).expect("Writing batch");

            // writer must be closed to write footer
            writer.close().unwrap();
        }

        fs::metadata(format!("{}/1.parquet", &self.table_location))
            .unwrap()
            .len()
    }

    pub async fn setup_unpartitioned_manifest_files(&mut self) {
        let current_snapshot = self.table.metadata().current_snapshot().unwrap();
        let parent_snapshot = current_snapshot
            .parent_snapshot(self.table.metadata())
            .unwrap();
        let current_schema = current_snapshot.schema(self.table.metadata()).unwrap();
        let current_partition_spec = Arc::new(PartitionSpec::unpartition_spec());

        // Write the data files first, then use the file size in the manifest entries
        let parquet_file_size = self.write_parquet_data_files();

        // Write data files using an empty partition for unpartitioned tables.
        let mut writer = ManifestWriterBuilder::new(
            self.next_manifest_file(),
            Some(current_snapshot.snapshot_id()),
            current_schema.clone(),
            current_partition_spec.as_ref().clone(),
        )
        .build_v2_data();

        // Create an empty partition value.
        let empty_partition = Struct::empty();

        writer
            .add_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::Data)
                            .file_path(format!("{}/1.parquet", &self.table_location))
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(parquet_file_size)
                            .record_count(1)
                            .partition(empty_partition.clone())
                            .key_metadata(None)
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap();

        writer
            .add_delete_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Deleted)
                    .snapshot_id(parent_snapshot.snapshot_id())
                    .sequence_number(parent_snapshot.sequence_number())
                    .file_sequence_number(parent_snapshot.sequence_number())
                    .data_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::Data)
                            .file_path(format!("{}/2.parquet", &self.table_location))
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(parquet_file_size)
                            .record_count(1)
                            .partition(empty_partition.clone())
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap();

        writer
            .add_existing_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Existing)
                    .snapshot_id(parent_snapshot.snapshot_id())
                    .sequence_number(parent_snapshot.sequence_number())
                    .file_sequence_number(parent_snapshot.sequence_number())
                    .data_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::Data)
                            .file_path(format!("{}/3.parquet", &self.table_location))
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(parquet_file_size)
                            .record_count(1)
                            .partition(empty_partition.clone())
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap();

        let data_file_manifest = writer.write_manifest_file().await.unwrap();

        // Write to manifest list
        let manifest_list_writer = self
            .table
            .file_io()
            .new_output(current_snapshot.manifest_list())
            .unwrap()
            .writer()
            .await
            .unwrap();
        let mut manifest_list_write = ManifestListWriter::v2(
            manifest_list_writer,
            current_snapshot.snapshot_id(),
            current_snapshot.parent_snapshot_id(),
            current_snapshot.sequence_number(),
        );
        manifest_list_write
            .add_manifests(vec![data_file_manifest].into_iter())
            .unwrap();
        manifest_list_write.close().await.unwrap();
    }

    pub async fn setup_deadlock_manifests(&mut self) {
        let current_snapshot = self.table.metadata().current_snapshot().unwrap();
        let _parent_snapshot = current_snapshot
            .parent_snapshot(self.table.metadata())
            .unwrap();
        let current_schema = current_snapshot.schema(self.table.metadata()).unwrap();
        let current_partition_spec = self.table.metadata().default_partition_spec();

        // 1. Write DATA manifest with MULTIPLE entries to fill buffer
        let mut writer = ManifestWriterBuilder::new(
            self.next_manifest_file(),
            Some(current_snapshot.snapshot_id()),
            current_schema.clone(),
            current_partition_spec.as_ref().clone(),
        )
        .build_v2_data();

        // Add 10 data entries
        for i in 0..10 {
            writer
                .add_entry(
                    ManifestEntry::builder()
                        .status(ManifestStatus::Added)
                        .data_file(
                            DataFileBuilder::default()
                                .partition_spec_id(0)
                                .content(DataContentType::Data)
                                .file_path(format!("{}/{}.parquet", &self.table_location, i))
                                .file_format(DataFileFormat::Parquet)
                                .file_size_in_bytes(100)
                                .record_count(1)
                                .partition(Struct::from_iter([Some(Literal::long(100))]))
                                .key_metadata(None)
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .unwrap();
        }
        let data_manifest = writer.write_manifest_file().await.unwrap();

        // 2. Write DELETE manifest
        let mut writer = ManifestWriterBuilder::new(
            self.next_manifest_file(),
            Some(current_snapshot.snapshot_id()),
            current_schema.clone(),
            current_partition_spec.as_ref().clone(),
        )
        .build_v2_deletes();

        writer
            .add_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::PositionDeletes)
                            .file_path(format!("{}/del.parquet", &self.table_location))
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
        let delete_manifest = writer.write_manifest_file().await.unwrap();

        // Write to manifest list - DATA FIRST then DELETE
        // This order is crucial for reproduction
        let manifest_list_writer = self
            .table
            .file_io()
            .new_output(current_snapshot.manifest_list())
            .unwrap()
            .writer()
            .await
            .unwrap();
        let mut manifest_list_write = ManifestListWriter::v2(
            manifest_list_writer,
            current_snapshot.snapshot_id(),
            current_snapshot.parent_snapshot_id(),
            current_snapshot.sequence_number(),
        );
        manifest_list_write
            .add_manifests(vec![data_manifest, delete_manifest].into_iter())
            .unwrap();
        manifest_list_write.close().await.unwrap();
    }

    /// Sets up a single data file `mrg.parquet` with three 100-row row groups
    /// (column `x` = 1000..1300, so row position `p` carries `x = 1000 + p`) and
    /// registers it in the current snapshot. When `delete_positions` is non-empty,
    /// also writes a positional delete file targeting those file-absolute positions
    /// and registers it in a delete manifest.
    ///
    /// Used to exercise the `_pos` metadata column through the real `TableScan`
    /// planning path across row-group boundaries and (optionally) positional deletes.
    pub async fn setup_multi_row_group_manifest(&mut self, delete_positions: &[i64]) {
        let current_snapshot = self.table.metadata().current_snapshot().unwrap();
        let current_schema = current_snapshot.schema(self.table.metadata()).unwrap();
        let current_partition_spec = self.table.metadata().default_partition_spec();

        // The table's spec 0 is identity on `x`, so give the data and delete files a
        // fixed partition value. Filter tests deliberately filter on `y` (a
        // non-partition column) so pruning is driven by Parquet row-group statistics
        // rather than partition values.
        let partition = Struct::from_iter([Some(Literal::long(1000))]);

        let (data_file_path, data_file_size) = self.write_multi_row_group_data_file();

        let mut data_writer = ManifestWriterBuilder::new(
            self.next_manifest_file(),
            Some(current_snapshot.snapshot_id()),
            current_schema.clone(),
            current_partition_spec.as_ref().clone(),
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
                            .file_path(data_file_path.clone())
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(data_file_size)
                            .record_count(300)
                            .partition(partition.clone())
                            .key_metadata(None)
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap();
        let data_manifest = data_writer.write_manifest_file().await.unwrap();

        let mut manifests = vec![data_manifest];

        if !delete_positions.is_empty() {
            let (del_path, del_size) =
                self.write_positional_delete_file(&data_file_path, delete_positions);

            let mut delete_writer = ManifestWriterBuilder::new(
                self.next_manifest_file(),
                Some(current_snapshot.snapshot_id()),
                current_schema.clone(),
                current_partition_spec.as_ref().clone(),
            )
            .build_v2_deletes();
            delete_writer
                .add_entry(
                    ManifestEntry::builder()
                        .status(ManifestStatus::Added)
                        .data_file(
                            DataFileBuilder::default()
                                .partition_spec_id(0)
                                .content(DataContentType::PositionDeletes)
                                .file_path(del_path)
                                .file_format(DataFileFormat::Parquet)
                                .file_size_in_bytes(del_size)
                                .record_count(delete_positions.len() as u64)
                                .partition(partition.clone())
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .unwrap();
            manifests.push(delete_writer.write_manifest_file().await.unwrap());
        }

        let manifest_list_writer = self
            .table
            .file_io()
            .new_output(current_snapshot.manifest_list())
            .unwrap()
            .writer()
            .await
            .unwrap();
        let mut manifest_list_write = ManifestListWriter::v2(
            manifest_list_writer,
            current_snapshot.snapshot_id(),
            current_snapshot.parent_snapshot_id(),
            current_snapshot.sequence_number(),
        );
        manifest_list_write
            .add_manifests(manifests.into_iter())
            .unwrap();
        manifest_list_write.close().await.unwrap();
    }

    /// Writes `mrg.parquet` with three 100-row row groups. Columns `x` (field
    /// id `1`) and `y` (field id `2`) both run 1000..1300, so row position `p`
    /// carries `x = y = 1000 + p`. Returns `(path, file_size_in_bytes)`.
    fn write_multi_row_group_data_file(&self) -> (String, u64) {
        fs::create_dir_all(&self.table_location).unwrap();

        let arrow_schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("x", arrow_schema::DataType::Int64, false).with_metadata(
                HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string())]),
            ),
            arrow_schema::Field::new("y", arrow_schema::DataType::Int64, false).with_metadata(
                HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "2".to_string())]),
            ),
        ]));

        let path = format!("{}/mrg.parquet", &self.table_location);
        let max_row_group_row_count = 100;
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_row_count(Some(max_row_group_row_count))
            .build();

        let file = File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props)).unwrap();
        for group in 0..3i64 {
            let base = 1000 + group * max_row_group_row_count as i64;
            let col = Arc::new(Int64Array::from_iter_values(
                base..base + max_row_group_row_count as i64,
            )) as ArrayRef;
            let batch = RecordBatch::try_new(arrow_schema.clone(), vec![col.clone(), col]).unwrap();
            writer.write(&batch).unwrap();
        }
        writer.close().unwrap();

        let size = fs::metadata(&path).unwrap().len();
        (path, size)
    }

    /// Writes a positional delete file targeting `positions` in `data_path`.
    /// Returns `(path, file_size_in_bytes)`.
    fn write_positional_delete_file(&self, data_path: &str, positions: &[i64]) -> (String, u64) {
        let del_schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new(
                RESERVED_COL_NAME_DELETE_FILE_PATH,
                arrow_schema::DataType::Utf8,
                false,
            )
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                RESERVED_FIELD_ID_DELETE_FILE_PATH.to_string(), // 2147483546
            )])),
            arrow_schema::Field::new(
                RESERVED_COL_NAME_DELETE_FILE_POS,
                arrow_schema::DataType::Int64,
                false,
            )
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                RESERVED_FIELD_ID_DELETE_FILE_POS.to_string(), // 2147483545
            )])),
        ]));

        let batch = RecordBatch::try_new(del_schema.clone(), vec![
            Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                data_path.to_string(),
                positions.len(),
            ))) as ArrayRef,
            Arc::new(Int64Array::from_iter_values(positions.iter().copied())) as ArrayRef,
        ])
        .unwrap();

        let path = format!("{}/pos-del.parquet", &self.table_location);
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let file = File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, del_schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let size = fs::metadata(&path).unwrap().len();
        (path, size)
    }
}
