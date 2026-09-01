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

#![allow(missing_docs)]

use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
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
    RESERVED_FIELD_ID_DELETE_FILE_PATH, RESERVED_FIELD_ID_DELETE_FILE_POS,
};
use crate::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, FormatVersion, Literal, ManifestEntry,
    ManifestFile, ManifestListWriter, ManifestStatus, ManifestWriterBuilder, PartitionSpec, Struct,
    StructType, TableMetadata, TableMetadataBuilder, UNASSIGNED_SEQUENCE_NUMBER,
};
use crate::table::Table;
use crate::test_utils::test_runtime;

fn render_template(template: &str, ctx: Value) -> String {
    let mut env = Environment::new();
    env.set_auto_escape_callback(|_| AutoEscape::None);
    env.render_str(template, ctx).unwrap()
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
    ///   S1 (append) -> S2 (append) -> S3 (append) -> S4 (overwrite) -> S5 (append, current)
    /// Useful for testing snapshot history traversal and incremental scans
    /// with non-append operations in the chain.
    pub fn new_with_deep_history() -> Self {
        Self::new_from_deep_history_metadata("example_table_metadata_v2_deep_history.json")
    }

    /// Like [`Self::new_with_deep_history`] but every snapshot references
    /// the older single-column schema (`schema-id` 0) while the table's
    /// `current-schema-id` stays at the three-column schema (`schema-id`
    /// 1). This models a table whose schema evolved *after* the snapshots
    /// in an incremental range were written, so we can assert that an
    /// incremental scan projects onto the current schema.
    pub fn new_with_deep_history_stale_schema() -> Self {
        let fixture = Self::new_from_deep_history_metadata(
            "example_table_metadata_v2_deep_history_stale_schema.json",
        );

        // Sanity check: current schema (3 cols) differs from the schema the
        // snapshots reference (1 col), otherwise the test would be vacuous.
        assert_eq!(fixture.table.metadata().current_schema_id(), 1);
        fixture
    }

    /// Like [`Self::new_with_deep_history`] but the S4 snapshot is a
    /// `replace` (the operation a compaction / `rewrite_data_files`
    /// commits) rather than an `overwrite`. Used to prove that an
    /// incremental append scan skips compaction output and never
    /// double-counts the appended rows against their rewritten copies.
    pub fn new_with_deep_history_compaction() -> Self {
        Self::new_from_deep_history_metadata(
            "example_table_metadata_v2_deep_history_compaction.json",
        )
    }

    /// Builds a deep-history fixture from the named templated metadata file
    /// in `testdata`. The five snapshot manifest-list paths are rendered to
    /// point at this fixture's temp directory.
    fn new_from_deep_history_metadata(metadata_file: &str) -> Self {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().join("table1");
        let table_metadata1_location = table_location.join("metadata/v1.json");

        let manifest_list_s1 = table_location.join("metadata/snap-3051729675574597004.avro");
        let manifest_list_s2 = table_location.join("metadata/snap-3055729675574597004.avro");
        let manifest_list_s3 = table_location.join("metadata/snap-3056729675574597004.avro");
        let manifest_list_s4 = table_location.join("metadata/snap-3057729675574597004.avro");
        let manifest_list_s5 = table_location.join("metadata/snap-3059729675574597004.avro");

        let file_io = FileIO::new_with_fs();

        let table_metadata = {
            let template_json_str = fs::read_to_string(format!(
                "{}/testdata/{metadata_file}",
                env!("CARGO_MANIFEST_DIR")
            ))
            .unwrap();
            let metadata_json = render_template(&template_json_str, context! {
                table_location => &table_location,
                manifest_list_s1_location => &manifest_list_s1,
                manifest_list_s2_location => &manifest_list_s2,
                manifest_list_s3_location => &manifest_list_s3,
                manifest_list_s4_location => &manifest_list_s4,
                manifest_list_s5_location => &manifest_list_s5,
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

    /// Sets up manifest files for the deep history fixture.
    ///
    /// Creates one data file per snapshot (s1.parquet through s5.parquet),
    /// each with a manifest and manifest list. Manifest lists are cumulative
    /// (each snapshot's list includes all prior manifests), matching real
    /// Iceberg behavior. The incremental scan should skip s4.parquet
    /// (added in the overwrite snapshot S4).
    pub async fn setup_manifest_files_deep_history(&mut self) {
        let parquet_file_size = self.write_parquet_data_files_deep_history();
        let partition_spec = self.table.metadata().default_partition_spec();

        // Snapshot chain: S1 -> S2 -> S3 -> S4 (overwrite) -> S5
        let snapshot_ids: Vec<i64> = vec![
            3051729675574597004,
            3055729675574597004,
            3056729675574597004,
            3057729675574597004,
            3059729675574597004,
        ];

        // Accumulate manifests across snapshots (each manifest list is cumulative)
        let mut all_manifests: Vec<ManifestFile> = Vec::new();

        for (i, &snap_id) in snapshot_ids.iter().enumerate() {
            let snapshot = self
                .table
                .metadata()
                .snapshot_by_id(snap_id)
                .unwrap()
                .clone();
            let schema = snapshot.schema(self.table.metadata()).unwrap();

            let file_name = format!("s{}.parquet", i + 1);
            let partition_value = (i + 1) as i64 * 100;

            let mut writer = ManifestWriterBuilder::new(
                self.next_manifest_file(),
                Some(snap_id),
                schema,
                partition_spec.as_ref().clone(),
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
                                .file_path(format!("{}/{}", &self.table_location, file_name))
                                .file_format(DataFileFormat::Parquet)
                                .file_size_in_bytes(parquet_file_size)
                                .record_count(1)
                                .partition(Struct::from_iter([Some(Literal::long(
                                    partition_value,
                                ))]))
                                .key_metadata(None)
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .unwrap();

            let mut data_file_manifest = writer.write_manifest_file().await.unwrap();
            // Assign sequence numbers so the manifest can be included in
            // later snapshots' cumulative manifest lists without triggering
            // the "unassigned sequence number" validation.
            data_file_manifest.sequence_number = snapshot.sequence_number();
            data_file_manifest.min_sequence_number = snapshot.sequence_number();
            all_manifests.push(data_file_manifest);

            // Write cumulative manifest list for this snapshot
            let manifest_list_writer = self
                .table
                .file_io()
                .new_output(snapshot.manifest_list())
                .unwrap()
                .writer()
                .await
                .unwrap();
            let mut manifest_list_write = ManifestListWriter::v2(
                manifest_list_writer,
                snap_id,
                snapshot.parent_snapshot_id(),
                snapshot.sequence_number(),
            );
            manifest_list_write
                .add_manifests(all_manifests.clone().into_iter())
                .unwrap();
            manifest_list_write.close().await.unwrap();
        }
    }

    /// Like [`Self::setup_manifest_files_deep_history`], but the manifest lists
    /// model writers that *rewrite* manifests rather than carrying every one
    /// forward verbatim:
    ///
    /// ```text
    /// S1 append     -> [A1]        A1 = {s1 ADDED@S1}
    /// S2 append     -> [M2]        M2 = {s1 EXISTING@S1, s2 ADDED@S2}   (merge-append: A1 is gone)
    /// S3 append     -> [M2, A3]    A3 = {s3 ADDED@S3}
    /// S4 overwrite  -> [C4]        C4 = {s1,s2,s3 EXISTING, s4 ADDED@S4} (rewrite: M2, A3 are gone)
    /// S5 append     -> [C4, A5]    A5 = {s5 ADDED@S5}
    /// ```
    ///
    /// The surviving copies of the earlier entries are `EXISTING`, not `ADDED`, so
    /// a scan reading only the to-snapshot's list silently drops them.
    pub async fn setup_manifest_files_deep_history_rewritten(&mut self) {
        let file_size = self.write_parquet_data_files_deep_history();

        let (s1, s2, s3, s4, s5) = (
            3051729675574597004_i64,
            3055729675574597004_i64,
            3056729675574597004_i64,
            3057729675574597004_i64,
            3059729675574597004_i64,
        );

        // (file index, originating snapshot, that snapshot's sequence number)
        let (f1, f2, f3, f4, f5) = ((1, s1, 0), (2, s2, 1), (3, s3, 2), (4, s4, 3), (5, s5, 4));

        let a1 = self
            .write_rewritten_manifest(s1, &[f1], &[], file_size)
            .await;
        let m2 = self
            .write_rewritten_manifest(s2, &[f2], &[f1], file_size)
            .await;
        let a3 = self
            .write_rewritten_manifest(s3, &[f3], &[], file_size)
            .await;
        let c4 = self
            .write_rewritten_manifest(s4, &[f4], &[f1, f2, f3], file_size)
            .await;
        let a5 = self
            .write_rewritten_manifest(s5, &[f5], &[], file_size)
            .await;

        self.write_deep_history_manifest_list(s1, vec![a1]).await;
        self.write_deep_history_manifest_list(s2, vec![m2.clone()])
            .await;
        self.write_deep_history_manifest_list(s3, vec![m2, a3])
            .await;
        self.write_deep_history_manifest_list(s4, vec![c4.clone()])
            .await;
        self.write_deep_history_manifest_list(s5, vec![c4, a5])
            .await;
    }

    /// Writes one data manifest owned by `owner_snapshot_id`.
    ///
    /// `added` and `existing` are `(file index, originating snapshot id, sequence
    /// number)` triples naming `s{index}.parquet`. Added entries take the owning
    /// snapshot's ID (the writer enforces this); existing entries keep the ID and
    /// sequence number of the snapshot that first added them.
    async fn write_rewritten_manifest(
        &self,
        owner_snapshot_id: i64,
        added: &[(usize, i64, i64)],
        existing: &[(usize, i64, i64)],
        file_size: u64,
    ) -> ManifestFile {
        let snapshot = self
            .table
            .metadata()
            .snapshot_by_id(owner_snapshot_id)
            .unwrap()
            .clone();
        let schema = snapshot.schema(self.table.metadata()).unwrap();
        let partition_spec = self.table.metadata().default_partition_spec();

        let mut writer = ManifestWriterBuilder::new(
            self.next_manifest_file(),
            Some(owner_snapshot_id),
            schema,
            partition_spec.as_ref().clone(),
        )
        .build_v2_data();

        let data_file = |index: usize| {
            DataFileBuilder::default()
                .partition_spec_id(0)
                .content(DataContentType::Data)
                .file_path(format!("{}/s{}.parquet", &self.table_location, index))
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(file_size)
                .record_count(1)
                .partition(Struct::from_iter([Some(Literal::long(index as i64 * 100))]))
                .key_metadata(None)
                .build()
                .unwrap()
        };

        for &(index, _, sequence_number) in added {
            writer.add_file(data_file(index), sequence_number).unwrap();
        }
        for &(index, snapshot_id, sequence_number) in existing {
            writer
                .add_existing_file(
                    data_file(index),
                    snapshot_id,
                    sequence_number,
                    Some(sequence_number),
                )
                .unwrap();
        }

        let mut manifest = writer.write_manifest_file().await.unwrap();
        manifest.sequence_number = snapshot.sequence_number();
        if manifest.min_sequence_number == UNASSIGNED_SEQUENCE_NUMBER {
            manifest.min_sequence_number = snapshot.sequence_number();
        }
        manifest
    }

    /// Writes `manifests` as the manifest list of the named snapshot.
    async fn write_deep_history_manifest_list(
        &self,
        snapshot_id: i64,
        manifests: Vec<ManifestFile>,
    ) {
        let snapshot = self
            .table
            .metadata()
            .snapshot_by_id(snapshot_id)
            .unwrap()
            .clone();

        let output = self
            .table
            .file_io()
            .new_output(snapshot.manifest_list())
            .unwrap()
            .writer()
            .await
            .unwrap();
        let mut writer = ManifestListWriter::v2(
            output,
            snapshot_id,
            snapshot.parent_snapshot_id(),
            snapshot.sequence_number(),
        );
        writer.add_manifests(manifests.into_iter()).unwrap();
        writer.close().await.unwrap();
    }

    /// Writes parquet data files for the deep history fixture (3-column schema: x, y, z).
    fn write_parquet_data_files_deep_history(&self) -> u64 {
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
            ];
            Arc::new(arrow_schema::Schema::new(fields))
        };

        let col1 = Arc::new(Int64Array::from_iter_values(vec![1; 10])) as ArrayRef;
        let col2 = Arc::new(Int64Array::from_iter_values(vec![2; 10])) as ArrayRef;
        let col3 = Arc::new(Int64Array::from_iter_values(vec![3; 10])) as ArrayRef;

        let batch = RecordBatch::try_new(schema.clone(), vec![col1, col2, col3]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        for i in 1..=5 {
            let file = File::create(format!("{}/s{}.parquet", &self.table_location, i)).unwrap();
            let mut writer =
                ArrowWriter::try_new(file, batch.schema(), Some(props.clone())).unwrap();
            writer.write(&batch).expect("Writing batch");
            writer.close().unwrap();
        }

        fs::metadata(format!("{}/s1.parquet", &self.table_location))
            .unwrap()
            .len()
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
