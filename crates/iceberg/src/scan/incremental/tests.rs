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
use std::fs;
use std::fs::File;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use futures::TryStreamExt;
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use tempfile::TempDir;
use uuid::Uuid;

use crate::TableIdent;
use crate::io::{FileIO, OutputFile};
use crate::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, ManifestEntry, ManifestListWriter,
    ManifestStatus, ManifestWriterBuilder, PartitionSpec, Struct, TableMetadata,
};
use crate::table::Table;

/// Represents an operation to perform on a snapshot of a table with schema (id: Int32,
/// data: String).
#[derive(Debug, Clone)]
pub enum Operation {
    /// Add rows with the given (n, data) tuples, and write to the specified parquet file name.
    /// Example: `Add(vec![(1, "a".to_string()), (2, "b".to_string())], "data-1.parquet".to_string())`
    /// adds two rows with n=1,2 and data="a","b" to a file named "data-1.parquet"
    Add(Vec<(i32, String)>, String),

    /// Delete rows by their positions within specific parquet files (uses positional deletes).
    /// Takes a vector of (position, file_name) tuples specifying which position in which file to delete.
    /// Example: `Delete(vec![(0, "data-1.parquet"), (1, "data-1.parquet")])` deletes positions 0 and 1 from data-1.parquet
    Delete(Vec<(i64, String)>),

    /// Overwrite operation that can append new rows, delete specific positions, and remove entire data files.
    /// This is a combination of append and delete operations in a single atomic snapshot.
    ///
    /// Parameters:
    /// 1. Rows to append: Vec<(n, data)> tuples and the filename to write them to
    /// 2. Positions to delete: Vec<(position, file_name)> tuples for positional deletes
    /// 3. Data files to delete: Vec<String> of file names to completely remove
    ///
    /// All three parameters can be empty, allowing for various combinations:
    /// - Pure append: `Overwrite((rows, "file.parquet"), vec![], vec![])`
    /// - Pure positional delete: `Overwrite((vec![], ""), vec![(pos, "file")], vec![])`
    /// - Pure file deletion: `Overwrite((vec![], ""), vec![], vec!["file.parquet"])`
    /// - Delete entire files: `Overwrite((vec![], ""), vec![], vec!["old-file.parquet"])`
    ///
    /// Example: `Overwrite((vec![(1, "new".to_string())], "new.parquet"), vec![(0, "old.parquet")], vec!["remove.parquet"])`
    /// This adds new data to "new.parquet", deletes position 0 from "old.parquet", and removes "remove.parquet" entirely.
    Overwrite(
        (Vec<(i32, String)>, String),
        Vec<(i64, String)>,
        Vec<String>,
    ),
}

/// Tracks the state of data files across snapshots
#[derive(Debug, Clone)]
struct DataFileInfo {
    path: String,
    snapshot_id: i64,
    sequence_number: i64,
    n_values: Vec<i32>,
}

/// Test fixture that creates a table with custom snapshots based on operations.
///
/// # Example
/// ```
/// let fixture = IncrementalTestFixture::new(vec![
///     Operation::Add(vec![], "empty.parquet".to_string()), // Empty snapshot
///     Operation::Add(
///         vec![
///             (1, "1".to_string()),
///             (2, "2".to_string()),
///             (3, "3".to_string()),
///         ],
///         "data-1.parquet".to_string(),
///     ), // Add 3 rows
///     Operation::Delete(vec![(1, "data-1.parquet".to_string())]), // Delete position 1 from data-1.parquet
/// ])
/// .await;
/// ```
pub struct IncrementalTestFixture {
    pub table_location: String,
    pub table: Table,
    _tmp_dir: TempDir, // Keep temp dir alive
}

impl IncrementalTestFixture {
    /// Create a new test fixture with the given operations.
    pub async fn new(operations: Vec<Operation>) -> Self {
        // Use pwd
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().join("incremental_test_table");

        // Create directory structure
        fs::create_dir_all(table_location.join("metadata")).unwrap();
        fs::create_dir_all(table_location.join("data")).unwrap();

        let file_io = FileIO::from_path(table_location.as_os_str().to_str().unwrap())
            .unwrap()
            .build()
            .unwrap();

        let num_snapshots = operations.len();
        let current_snapshot_id = num_snapshots as i64;
        let last_sequence_number = (num_snapshots - 1) as i64;

        // Build the snapshots JSON dynamically
        let mut snapshots_json = Vec::new();
        let mut snapshot_log_json = Vec::new();
        let mut manifest_list_locations = Vec::new();

        for (i, op) in operations.iter().enumerate() {
            let snapshot_id = (i + 1) as i64;
            let parent_id = if i == 0 { None } else { Some(i as i64) };
            let sequence_number = i as i64;
            let timestamp = 1515100955770 + (i as i64 * 1000);

            let operation_type = match op {
                Operation::Add(..) => "append",
                Operation::Delete(..) => "delete",
                Operation::Overwrite(..) => "overwrite",
            };

            let manifest_list_location =
                table_location.join(format!("metadata/snap-{}-manifest-list.avro", snapshot_id));
            manifest_list_locations.push(manifest_list_location.clone());

            let parent_str = if let Some(pid) = parent_id {
                format!(r#""parent-snapshot-id": {},"#, pid)
            } else {
                String::new()
            };

            snapshots_json.push(format!(
                r#"    {{
      "snapshot-id": {},
      {}
      "timestamp-ms": {},
      "sequence-number": {},
      "summary": {{"operation": "{}"}},
      "manifest-list": "{}",
      "schema-id": 0
    }}"#,
                snapshot_id,
                parent_str,
                timestamp,
                sequence_number,
                operation_type,
                manifest_list_location.display()
            ));

            snapshot_log_json.push(format!(
                r#"    {{"snapshot-id": {}, "timestamp-ms": {}}}"#,
                snapshot_id, timestamp
            ));
        }

        let snapshots_str = snapshots_json.join(",\n");
        let snapshot_log_str = snapshot_log_json.join(",\n");

        // Create the table metadata
        let metadata_json = format!(
            r#"{{
  "format-version": 2,
  "table-uuid": "{}",
  "location": "{}",
  "last-sequence-number": {},
  "last-updated-ms": 1602638573590,
  "last-column-id": 2,
  "current-schema-id": 0,
  "schemas": [
    {{
      "type": "struct",
      "schema-id": 0,
      "fields": [
        {{"id": 1, "name": "n", "required": true, "type": "int"}},
        {{"id": 2, "name": "data", "required": true, "type": "string"}}
      ]
    }}
  ],
  "default-spec-id": 0,
  "partition-specs": [
    {{
      "spec-id": 0,
      "fields": []
    }}
  ],
  "last-partition-id": 0,
  "default-sort-order-id": 0,
  "sort-orders": [
    {{
      "order-id": 0,
      "fields": []
    }}
  ],
  "properties": {{}},
  "current-snapshot-id": {},
  "snapshots": [
{}
  ],
  "snapshot-log": [
{}
  ],
  "metadata-log": []
}}"#,
            Uuid::new_v4(),
            table_location.display(),
            last_sequence_number,
            current_snapshot_id,
            snapshots_str,
            snapshot_log_str
        );

        let table_metadata_location = table_location.join("metadata/v1.json");
        let table_metadata = serde_json::from_str::<TableMetadata>(&metadata_json).unwrap();

        let table = Table::builder()
            .metadata(table_metadata)
            .identifier(TableIdent::from_strs(["db", "incremental_test"]).unwrap())
            .file_io(file_io.clone())
            .metadata_location(table_metadata_location.as_os_str().to_str().unwrap())
            .build()
            .unwrap();

        let mut fixture = Self {
            table_location: table_location.to_str().unwrap().to_string(),
            table,
            _tmp_dir: tmp_dir,
        };

        // Setup all snapshots based on operations
        fixture.setup_snapshots(operations).await;

        fixture
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

    async fn setup_snapshots(&mut self, operations: Vec<Operation>) {
        let current_schema = self
            .table
            .metadata()
            .current_snapshot()
            .unwrap()
            .schema(self.table.metadata())
            .unwrap();
        let partition_spec = Arc::new(PartitionSpec::unpartition_spec());
        let empty_partition = Struct::empty();

        // Track all data files and their contents across snapshots
        let mut data_files: Vec<DataFileInfo> = Vec::new();
        #[allow(clippy::type_complexity)]
        let mut delete_files: Vec<(String, i64, i64, Vec<(String, i64)>)> = Vec::new(); // (path, snapshot_id, sequence_number, [(data_file_path, position)])

        for (snapshot_idx, operation) in operations.iter().enumerate() {
            let snapshot_id = (snapshot_idx + 1) as i64;
            let sequence_number = snapshot_idx as i64;
            let parent_snapshot_id = if snapshot_idx == 0 {
                None
            } else {
                Some(snapshot_idx as i64)
            };

            match operation {
                Operation::Add(rows, file_name) => {
                    // Extract n_values and data_values from tuples
                    let n_values: Vec<i32> = rows.iter().map(|(n, _)| *n).collect();
                    let data_values: Vec<String> = rows.iter().map(|(_, d)| d.clone()).collect();

                    // Create data manifest
                    let mut data_writer = ManifestWriterBuilder::new(
                        self.next_manifest_file(),
                        Some(snapshot_id),
                        None,
                        current_schema.clone(),
                        partition_spec.as_ref().clone(),
                    )
                    .build_v2_data();

                    // Add existing data files from previous snapshots
                    for data_file in &data_files {
                        data_writer
                            .add_existing_entry(
                                ManifestEntry::builder()
                                    .status(ManifestStatus::Existing)
                                    .snapshot_id(data_file.snapshot_id)
                                    .sequence_number(data_file.sequence_number)
                                    .file_sequence_number(data_file.sequence_number)
                                    .data_file(
                                        DataFileBuilder::default()
                                            .partition_spec_id(0)
                                            .content(DataContentType::Data)
                                            .file_path(data_file.path.clone())
                                            .file_format(DataFileFormat::Parquet)
                                            .file_size_in_bytes(1024)
                                            .record_count(data_file.n_values.len() as u64)
                                            .partition(empty_partition.clone())
                                            .key_metadata(None)
                                            .build()
                                            .unwrap(),
                                    )
                                    .build(),
                            )
                            .unwrap();
                    }

                    // Add new data if not empty
                    if !n_values.is_empty() {
                        let data_file_path = format!("{}/data/{}", &self.table_location, file_name);
                        self.write_parquet_file(&data_file_path, &n_values, &data_values)
                            .await;

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
                                            .file_size_in_bytes(1024)
                                            .record_count(n_values.len() as u64)
                                            .partition(empty_partition.clone())
                                            .key_metadata(None)
                                            .build()
                                            .unwrap(),
                                    )
                                    .build(),
                            )
                            .unwrap();

                        // Track this data file
                        data_files.push(DataFileInfo {
                            path: data_file_path,
                            snapshot_id,
                            sequence_number,
                            n_values,
                        });
                    }

                    let data_manifest = data_writer.write_manifest_file().await.unwrap();

                    // Create delete manifest if there are any delete files
                    let mut manifests = vec![data_manifest];
                    if !delete_files.is_empty() {
                        let mut delete_writer = ManifestWriterBuilder::new(
                            self.next_manifest_file(),
                            Some(snapshot_id),
                            None,
                            current_schema.clone(),
                            partition_spec.as_ref().clone(),
                        )
                        .build_v2_deletes();

                        for (delete_path, del_snapshot_id, del_sequence_number, _) in &delete_files
                        {
                            let delete_count = delete_files
                                .iter()
                                .filter(|(p, _, _, _)| p == delete_path)
                                .map(|(_, _, _, deletes)| deletes.len())
                                .sum::<usize>();

                            delete_writer
                                .add_existing_entry(
                                    ManifestEntry::builder()
                                        .status(ManifestStatus::Existing)
                                        .snapshot_id(*del_snapshot_id)
                                        .sequence_number(*del_sequence_number)
                                        .file_sequence_number(*del_sequence_number)
                                        .data_file(
                                            DataFileBuilder::default()
                                                .partition_spec_id(0)
                                                .content(DataContentType::PositionDeletes)
                                                .file_path(delete_path.clone())
                                                .file_format(DataFileFormat::Parquet)
                                                .file_size_in_bytes(512)
                                                .record_count(delete_count as u64)
                                                .partition(empty_partition.clone())
                                                .key_metadata(None)
                                                .build()
                                                .unwrap(),
                                        )
                                        .build(),
                                )
                                .unwrap();
                        }

                        manifests.push(delete_writer.write_manifest_file().await.unwrap());
                    }

                    // Write manifest list
                    let mut manifest_list_write = ManifestListWriter::v2(
                        self.table
                            .file_io()
                            .new_output(format!(
                                "{}/metadata/snap-{}-manifest-list.avro",
                                self.table_location, snapshot_id
                            ))
                            .unwrap(),
                        snapshot_id,
                        parent_snapshot_id,
                        sequence_number,
                    );
                    manifest_list_write
                        .add_manifests(manifests.into_iter())
                        .unwrap();
                    manifest_list_write.close().await.unwrap();
                }

                Operation::Delete(positions_to_delete) => {
                    // Group deletes by file
                    let mut deletes_by_file: HashMap<String, Vec<i64>> = HashMap::new();

                    for (position, file_name) in positions_to_delete {
                        let data_file_path = format!("{}/data/{}", &self.table_location, file_name);
                        deletes_by_file
                            .entry(data_file_path)
                            .or_default()
                            .push(*position);
                    }

                    // Create data manifest with existing data files
                    let mut data_writer = ManifestWriterBuilder::new(
                        self.next_manifest_file(),
                        Some(snapshot_id),
                        None,
                        current_schema.clone(),
                        partition_spec.as_ref().clone(),
                    )
                    .build_v2_data();

                    for data_file in &data_files {
                        data_writer
                            .add_existing_entry(
                                ManifestEntry::builder()
                                    .status(ManifestStatus::Existing)
                                    .snapshot_id(data_file.snapshot_id)
                                    .sequence_number(data_file.sequence_number)
                                    .file_sequence_number(data_file.sequence_number)
                                    .data_file(
                                        DataFileBuilder::default()
                                            .partition_spec_id(0)
                                            .content(DataContentType::Data)
                                            .file_path(data_file.path.clone())
                                            .file_format(DataFileFormat::Parquet)
                                            .file_size_in_bytes(1024)
                                            .record_count(data_file.n_values.len() as u64)
                                            .partition(empty_partition.clone())
                                            .key_metadata(None)
                                            .build()
                                            .unwrap(),
                                    )
                                    .build(),
                            )
                            .unwrap();
                    }

                    let data_manifest = data_writer.write_manifest_file().await.unwrap();

                    // Create delete manifest
                    let mut delete_writer = ManifestWriterBuilder::new(
                        self.next_manifest_file(),
                        Some(snapshot_id),
                        None,
                        current_schema.clone(),
                        partition_spec.as_ref().clone(),
                    )
                    .build_v2_deletes();

                    // Add existing delete files
                    for (delete_path, del_snapshot_id, del_sequence_number, _) in &delete_files {
                        let delete_count = delete_files
                            .iter()
                            .filter(|(p, _, _, _)| p == delete_path)
                            .map(|(_, _, _, deletes)| deletes.len())
                            .sum::<usize>();

                        delete_writer
                            .add_existing_entry(
                                ManifestEntry::builder()
                                    .status(ManifestStatus::Existing)
                                    .snapshot_id(*del_snapshot_id)
                                    .sequence_number(*del_sequence_number)
                                    .file_sequence_number(*del_sequence_number)
                                    .data_file(
                                        DataFileBuilder::default()
                                            .partition_spec_id(0)
                                            .content(DataContentType::PositionDeletes)
                                            .file_path(delete_path.clone())
                                            .file_format(DataFileFormat::Parquet)
                                            .file_size_in_bytes(512)
                                            .record_count(delete_count as u64)
                                            .partition(empty_partition.clone())
                                            .key_metadata(None)
                                            .build()
                                            .unwrap(),
                                    )
                                    .build(),
                            )
                            .unwrap();
                    }

                    // Add new delete files
                    for (data_file_path, positions) in deletes_by_file {
                        let delete_file_path = format!(
                            "{}/data/delete-{}-{}.parquet",
                            &self.table_location,
                            snapshot_id,
                            Uuid::new_v4()
                        );
                        self.write_positional_delete_file(
                            &delete_file_path,
                            &data_file_path,
                            &positions,
                        )
                        .await;

                        delete_writer
                            .add_entry(
                                ManifestEntry::builder()
                                    .status(ManifestStatus::Added)
                                    .data_file(
                                        DataFileBuilder::default()
                                            .partition_spec_id(0)
                                            .content(DataContentType::PositionDeletes)
                                            .file_path(delete_file_path.clone())
                                            .file_format(DataFileFormat::Parquet)
                                            .file_size_in_bytes(512)
                                            .record_count(positions.len() as u64)
                                            .partition(empty_partition.clone())
                                            .key_metadata(None)
                                            .build()
                                            .unwrap(),
                                    )
                                    .build(),
                            )
                            .unwrap();

                        // Track this delete file
                        delete_files.push((
                            delete_file_path,
                            snapshot_id,
                            sequence_number,
                            positions
                                .into_iter()
                                .map(|pos| (data_file_path.clone(), pos))
                                .collect(),
                        ));
                    }

                    let delete_manifest = delete_writer.write_manifest_file().await.unwrap();

                    // Write manifest list
                    let mut manifest_list_write = ManifestListWriter::v2(
                        self.table
                            .file_io()
                            .new_output(format!(
                                "{}/metadata/snap-{}-manifest-list.avro",
                                self.table_location, snapshot_id
                            ))
                            .unwrap(),
                        snapshot_id,
                        parent_snapshot_id,
                        sequence_number,
                    );
                    manifest_list_write
                        .add_manifests(vec![data_manifest, delete_manifest].into_iter())
                        .unwrap();
                    manifest_list_write.close().await.unwrap();
                }

                Operation::Overwrite((rows, file_name), positions_to_delete, files_to_delete) => {
                    // Overwrite creates a single snapshot that can:
                    // 1. Add new data files
                    // 2. Delete positions from existing files
                    // 3. Remove entire data files

                    // Create data manifest
                    let mut data_writer = ManifestWriterBuilder::new(
                        self.next_manifest_file(),
                        Some(snapshot_id),
                        None,
                        current_schema.clone(),
                        partition_spec.as_ref().clone(),
                    )
                    .build_v2_data();

                    // Determine which files to delete
                    let files_to_delete_set: std::collections::HashSet<String> = files_to_delete
                        .iter()
                        .map(|f| format!("{}/data/{}", &self.table_location, f))
                        .collect();

                    // Add existing data files (mark deleted ones as DELETED, others as EXISTING)
                    for data_file in &data_files {
                        if files_to_delete_set.contains(&data_file.path) {
                            // Mark file for deletion
                            data_writer
                                .add_delete_entry(
                                    ManifestEntry::builder()
                                        .status(ManifestStatus::Deleted)
                                        .snapshot_id(data_file.snapshot_id)
                                        .sequence_number(data_file.sequence_number)
                                        .file_sequence_number(data_file.sequence_number)
                                        .data_file(
                                            DataFileBuilder::default()
                                                .partition_spec_id(0)
                                                .content(DataContentType::Data)
                                                .file_path(data_file.path.clone())
                                                .file_format(DataFileFormat::Parquet)
                                                .file_size_in_bytes(1024)
                                                .record_count(data_file.n_values.len() as u64)
                                                .partition(empty_partition.clone())
                                                .key_metadata(None)
                                                .build()
                                                .unwrap(),
                                        )
                                        .build(),
                                )
                                .unwrap();
                        } else {
                            // Keep existing file
                            data_writer
                                .add_existing_entry(
                                    ManifestEntry::builder()
                                        .status(ManifestStatus::Existing)
                                        .snapshot_id(data_file.snapshot_id)
                                        .sequence_number(data_file.sequence_number)
                                        .file_sequence_number(data_file.sequence_number)
                                        .data_file(
                                            DataFileBuilder::default()
                                                .partition_spec_id(0)
                                                .content(DataContentType::Data)
                                                .file_path(data_file.path.clone())
                                                .file_format(DataFileFormat::Parquet)
                                                .file_size_in_bytes(1024)
                                                .record_count(data_file.n_values.len() as u64)
                                                .partition(empty_partition.clone())
                                                .key_metadata(None)
                                                .build()
                                                .unwrap(),
                                        )
                                        .build(),
                                )
                                .unwrap();
                        }
                    }

                    // Add new data file if rows provided
                    if !rows.is_empty() {
                        let n_values: Vec<i32> = rows.iter().map(|(n, _)| *n).collect();
                        let data_values: Vec<String> =
                            rows.iter().map(|(_, d)| d.clone()).collect();
                        let data_file_path = format!("{}/data/{}", &self.table_location, file_name);

                        self.write_parquet_file(&data_file_path, &n_values, &data_values)
                            .await;

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
                                            .file_size_in_bytes(1024)
                                            .record_count(n_values.len() as u64)
                                            .partition(empty_partition.clone())
                                            .key_metadata(None)
                                            .build()
                                            .unwrap(),
                                    )
                                    .build(),
                            )
                            .unwrap();

                        // Track this new data file
                        data_files.push(DataFileInfo {
                            path: data_file_path,
                            snapshot_id,
                            sequence_number,
                            n_values,
                        });
                    }

                    // Remove deleted files from tracking
                    data_files.retain(|df| !files_to_delete_set.contains(&df.path));

                    let data_manifest = data_writer.write_manifest_file().await.unwrap();

                    // Handle positional deletes if any
                    let mut manifests = vec![data_manifest];

                    if !positions_to_delete.is_empty() || !delete_files.is_empty() {
                        let mut delete_writer = ManifestWriterBuilder::new(
                            self.next_manifest_file(),
                            Some(snapshot_id),
                            None,
                            current_schema.clone(),
                            partition_spec.as_ref().clone(),
                        )
                        .build_v2_deletes();

                        // Add existing delete files
                        for (delete_path, del_snapshot_id, del_sequence_number, _) in &delete_files
                        {
                            let delete_count = delete_files
                                .iter()
                                .filter(|(p, _, _, _)| p == delete_path)
                                .map(|(_, _, _, deletes)| deletes.len())
                                .sum::<usize>();

                            delete_writer
                                .add_existing_entry(
                                    ManifestEntry::builder()
                                        .status(ManifestStatus::Existing)
                                        .snapshot_id(*del_snapshot_id)
                                        .sequence_number(*del_sequence_number)
                                        .file_sequence_number(*del_sequence_number)
                                        .data_file(
                                            DataFileBuilder::default()
                                                .partition_spec_id(0)
                                                .content(DataContentType::PositionDeletes)
                                                .file_path(delete_path.clone())
                                                .file_format(DataFileFormat::Parquet)
                                                .file_size_in_bytes(512)
                                                .record_count(delete_count as u64)
                                                .partition(empty_partition.clone())
                                                .key_metadata(None)
                                                .build()
                                                .unwrap(),
                                        )
                                        .build(),
                                )
                                .unwrap();
                        }

                        // Add new positional delete files
                        if !positions_to_delete.is_empty() {
                            // Group deletes by file
                            let mut deletes_by_file: HashMap<String, Vec<i64>> = HashMap::new();
                            for (position, file_name) in positions_to_delete {
                                let data_file_path =
                                    format!("{}/data/{}", &self.table_location, file_name);
                                deletes_by_file
                                    .entry(data_file_path)
                                    .or_default()
                                    .push(*position);
                            }

                            for (data_file_path, positions) in deletes_by_file {
                                let delete_file_path = format!(
                                    "{}/data/delete-{}-{}.parquet",
                                    &self.table_location,
                                    snapshot_id,
                                    Uuid::new_v4()
                                );
                                self.write_positional_delete_file(
                                    &delete_file_path,
                                    &data_file_path,
                                    &positions,
                                )
                                .await;

                                delete_writer
                                    .add_entry(
                                        ManifestEntry::builder()
                                            .status(ManifestStatus::Added)
                                            .data_file(
                                                DataFileBuilder::default()
                                                    .partition_spec_id(0)
                                                    .content(DataContentType::PositionDeletes)
                                                    .file_path(delete_file_path.clone())
                                                    .file_format(DataFileFormat::Parquet)
                                                    .file_size_in_bytes(512)
                                                    .record_count(positions.len() as u64)
                                                    .partition(empty_partition.clone())
                                                    .key_metadata(None)
                                                    .build()
                                                    .unwrap(),
                                            )
                                            .build(),
                                    )
                                    .unwrap();

                                // Track this delete file
                                delete_files.push((
                                    delete_file_path,
                                    snapshot_id,
                                    sequence_number,
                                    positions
                                        .into_iter()
                                        .map(|pos| (data_file_path.clone(), pos))
                                        .collect(),
                                ));
                            }
                        }

                        manifests.push(delete_writer.write_manifest_file().await.unwrap());
                    }

                    // Write manifest list
                    let mut manifest_list_write = ManifestListWriter::v2(
                        self.table
                            .file_io()
                            .new_output(format!(
                                "{}/metadata/snap-{}-manifest-list.avro",
                                self.table_location, snapshot_id
                            ))
                            .unwrap(),
                        snapshot_id,
                        parent_snapshot_id,
                        sequence_number,
                    );
                    manifest_list_write
                        .add_manifests(manifests.into_iter())
                        .unwrap();

                    manifest_list_write.close().await.unwrap();
                }
            }
        }
    }

    async fn write_parquet_file(&self, path: &str, n_values: &[i32], data_values: &[String]) {
        let schema = {
            let fields = vec![
                arrow_schema::Field::new("n", arrow_schema::DataType::Int32, false).with_metadata(
                    HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string())]),
                ),
                arrow_schema::Field::new("data", arrow_schema::DataType::Utf8, false)
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "2".to_string(),
                    )])),
            ];
            Arc::new(arrow_schema::Schema::new(fields))
        };

        let col_n = Arc::new(Int32Array::from(n_values.to_vec())) as ArrayRef;
        let col_data = Arc::new(StringArray::from(data_values.to_vec())) as ArrayRef;

        let batch = RecordBatch::try_new(schema.clone(), vec![col_n, col_data]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let file = File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    async fn write_positional_delete_file(
        &self,
        path: &str,
        data_file_path: &str,
        positions: &[i64],
    ) {
        let schema = {
            let fields = vec![
                arrow_schema::Field::new("file_path", arrow_schema::DataType::Utf8, false)
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "2147483546".to_string(),
                    )])),
                arrow_schema::Field::new("pos", arrow_schema::DataType::Int64, false)
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "2147483545".to_string(),
                    )])),
            ];
            Arc::new(arrow_schema::Schema::new(fields))
        };

        let file_paths: Vec<&str> = vec![data_file_path; positions.len()];
        let col_file_path = Arc::new(StringArray::from(file_paths)) as ArrayRef;
        let col_pos = Arc::new(arrow_array::Int64Array::from(positions.to_vec())) as ArrayRef;

        let batch = RecordBatch::try_new(schema.clone(), vec![col_file_path, col_pos]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let file = File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    /// Verify incremental scan results.
    ///
    /// Verifies that the incremental scan contains the expected appended and deleted records.
    pub async fn verify_incremental_scan(
        &self,
        from_snapshot_id: i64,
        to_snapshot_id: i64,
        expected_appends: Vec<(i32, &str)>,
        expected_deletes: Vec<(u64, &str)>,
    ) {
        use arrow_array::cast::AsArray;
        use arrow_select::concat::concat_batches;
        use futures::TryStreamExt;

        let incremental_scan = self
            .table
            .incremental_scan(from_snapshot_id, to_snapshot_id)
            .build()
            .unwrap();

        let stream = incremental_scan.to_arrow().await.unwrap();
        let batches: Vec<_> = stream.try_collect().await.unwrap();

        // Separate appends and deletes
        let append_batches: Vec<_> = batches
            .iter()
            .filter(|(t, _)| *t == crate::arrow::IncrementalBatchType::Append)
            .map(|(_, b)| b.clone())
            .collect();

        let delete_batches: Vec<_> = batches
            .iter()
            .filter(|(t, _)| *t == crate::arrow::IncrementalBatchType::Delete)
            .map(|(_, b)| b.clone())
            .collect();

        // Verify appended records
        if !append_batches.is_empty() {
            let append_batch =
                concat_batches(&append_batches[0].schema(), append_batches.iter()).unwrap();

            let n_array = append_batch
                .column(0)
                .as_primitive::<arrow_array::types::Int32Type>();
            let data_array = append_batch.column(1).as_string::<i32>();

            let mut appended_pairs: Vec<(i32, String)> = (0..append_batch.num_rows())
                .map(|i| (n_array.value(i), data_array.value(i).to_string()))
                .collect();
            appended_pairs.sort();

            let expected_appends: Vec<(i32, String)> = expected_appends
                .into_iter()
                .map(|(n, s)| (n, s.to_string()))
                .collect();

            assert_eq!(appended_pairs, expected_appends);
        } else {
            assert!(expected_appends.is_empty(), "Expected appends but got none");
        }

        // Verify deleted records
        if !delete_batches.is_empty() {
            let delete_batch =
                concat_batches(&delete_batches[0].schema(), delete_batches.iter()).unwrap();

            let pos_array = delete_batch
                .column(0)
                .as_primitive::<arrow_array::types::UInt64Type>();

            // The file path column is a StringArray with materialized values
            let file_path_column = delete_batch.column(1);
            let file_path_array = file_path_column.as_string::<i32>();

            let mut deleted_pairs: Vec<(u64, String)> = (0..delete_batch.num_rows())
                .map(|i| {
                    let pos = pos_array.value(i);
                    let file_path = file_path_array.value(i).to_string();
                    (pos, file_path)
                })
                .collect();
            deleted_pairs.sort();

            let expected_deletes: Vec<(u64, String)> = expected_deletes
                .into_iter()
                .map(|(pos, file)| (pos, file.to_string()))
                .collect();

            assert_eq!(deleted_pairs, expected_deletes);
        } else {
            assert!(expected_deletes.is_empty(), "Expected deletes but got none");
        }
    }
}

#[tokio::test]
async fn test_incremental_fixture_simple() {
    let fixture = IncrementalTestFixture::new(vec![
        Operation::Add(vec![], "empty.parquet".to_string()),
        Operation::Add(
            vec![
                (1, "1".to_string()),
                (2, "2".to_string()),
                (3, "3".to_string()),
            ],
            "data-2.parquet".to_string(),
        ),
        Operation::Delete(vec![(1, "data-2.parquet".to_string())]), // Delete position 1 (n=2, data="2")
    ])
    .await;

    // Verify we have 3 snapshots
    let mut snapshots = fixture.table.metadata().snapshots().collect::<Vec<_>>();
    snapshots.sort_by_key(|s| s.snapshot_id());
    assert_eq!(snapshots.len(), 3);

    // Verify snapshot IDs
    assert_eq!(snapshots[0].snapshot_id(), 1);
    assert_eq!(snapshots[1].snapshot_id(), 2);
    assert_eq!(snapshots[2].snapshot_id(), 3);

    // Verify parent relationships
    assert_eq!(snapshots[0].parent_snapshot_id(), None);
    assert_eq!(snapshots[1].parent_snapshot_id(), Some(1));
    assert_eq!(snapshots[2].parent_snapshot_id(), Some(2));

    // Verify incremental scan from snapshot 1 to snapshot 3.
    // Expected appends: snapshot 2 adds [1, 2, 3]
    // Expected deletes: snapshot 3 deletes [2]
    // In total we expect appends [1, 3] and deletes []
    fixture
        .verify_incremental_scan(1, 3, vec![(1, "1"), (3, "3")], vec![])
        .await;

    // Verify incremental scan from snapshot 2 to snapshot 3.
    let data_file_path = format!("{}/data/data-2.parquet", fixture.table_location);
    fixture
        .verify_incremental_scan(2, 3, vec![], vec![(1, &data_file_path)])
        .await;

    // Verify incremental scan from snapshot 1 to snapshot 1.
    fixture.verify_incremental_scan(1, 1, vec![], vec![]).await;
}

#[tokio::test]
async fn test_incremental_fixture_complex() {
    let fixture = IncrementalTestFixture::new(vec![
        Operation::Add(vec![], "empty.parquet".to_string()), // Snapshot 1: Empty
        Operation::Add(
            vec![
                (1, "a".to_string()),
                (2, "b".to_string()),
                (3, "c".to_string()),
                (4, "d".to_string()),
                (5, "e".to_string()),
            ],
            "data-2.parquet".to_string(),
        ), // Snapshot 2: Add 5 rows (positions 0-4)
        Operation::Delete(vec![
            (1, "data-2.parquet".to_string()),
            (3, "data-2.parquet".to_string()),
        ]), // Snapshot 3: Delete positions 1,3 (n=2,4; data=b,d)
        Operation::Add(
            vec![(6, "f".to_string()), (7, "g".to_string())],
            "data-4.parquet".to_string(),
        ), // Snapshot 4: Add 2 more rows (positions 5-6)
        Operation::Delete(vec![
            (0, "data-2.parquet".to_string()),
            (2, "data-2.parquet".to_string()),
            (4, "data-2.parquet".to_string()),
            (0, "data-4.parquet".to_string()),
            (1, "data-4.parquet".to_string()),
        ]), // Snapshot 5: Delete positions 0,2,4,5,6 (all remaining rows: n=1,3,5,6,7)
    ])
    .await;

    // Verify we have 5 snapshots
    let mut snapshots = fixture.table.metadata().snapshots().collect::<Vec<_>>();
    snapshots.sort_by_key(|s| s.snapshot_id());
    assert_eq!(snapshots.len(), 5);

    // Verify parent chain
    assert_eq!(snapshots[0].parent_snapshot_id(), None);
    for (i, snapshot) in snapshots.iter().enumerate().take(5).skip(1) {
        assert_eq!(snapshot.parent_snapshot_id(), Some(i as i64));
    }

    // Verify current snapshot
    assert_eq!(
        fixture
            .table
            .metadata()
            .current_snapshot()
            .unwrap()
            .snapshot_id(),
        5
    );

    // Verify incremental scan from snapshot 1 to snapshot 5.
    // All data has been deleted, so we expect the empty result.
    fixture.verify_incremental_scan(1, 5, vec![], vec![]).await;

    // Verify incremental scan from snapshot 2 to snapshot 5.
    // Snapshot 2 starts with: (1,a), (2,b), (3,c), (4,d), (5,e) in data-2.parquet
    // Snapshot 3: Deletes positions 1,3 from data-2.parquet (n=2,4; data=b,d)
    // Snapshot 4: Adds (6,f), (7,g) in data-4.parquet
    // Snapshot 5: Deletes positions 0,2,4 from data-2.parquet and 0,1 from data-4.parquet (n=1,3,5,6,7; data=a,c,e,f,g)
    //
    // The incremental scan computes the NET EFFECT between snapshot 2 and 5:
    // - Files added in snapshot 4 were completely deleted in snapshot 5, so NO net appends
    // - Net deletes from data-2.parquet: positions 0,1,2,3,4 (all 5 rows deleted across snapshots 3 and 5)
    // - Since data-4 was added and deleted between 2 and 5, it doesn't appear in the scan
    let data_2_path = format!("{}/data/data-2.parquet", fixture.table_location);
    fixture
        .verify_incremental_scan(
            2,
            5,
            vec![], // No net appends (data-4 was added and fully deleted)
            vec![
                (0, data_2_path.as_str()), // All 5 positions from data-2.parquet
                (1, data_2_path.as_str()),
                (2, data_2_path.as_str()),
                (3, data_2_path.as_str()),
                (4, data_2_path.as_str()),
            ],
        )
        .await;

    // Verify incremental scan from snapshot 3 to snapshot 5.
    // Snapshot 3 state: (1,a), (3,c), (5,e) remain in data-2.parquet at positions 0,2,4
    // Snapshot 4: Adds (6,f), (7,g) in data-4.parquet
    // Snapshot 5: Deletes positions 0,2,4 from data-2.parquet (n=1,3,5) and 0,1 from data-4.parquet (n=6,7)
    //
    // Net effect from snapshot 3 to 5:
    // - No net appends (data-4 was added and fully deleted between 3 and 5)
    // - Net deletes from data-2.parquet: positions 0,2,4 (the three remaining rows deleted in snapshot 5)
    fixture
        .verify_incremental_scan(
            3,
            5,
            vec![], // No net appends (data-4 was added and fully deleted)
            vec![
                (0, data_2_path.as_str()), // Positions 0,2,4 from data-2.parquet
                (2, data_2_path.as_str()), // (n=1,3,5; data=a,c,e)
                (4, data_2_path.as_str()),
            ],
        )
        .await;

    // Verify incremental scan from snapshot 1 to snapshot 4.
    // Snapshot 1: Empty
    // Snapshot 2: Adds (1,a), (2,b), (3,c), (4,d), (5,e) in data-2.parquet
    // Snapshot 3: Deletes positions 1,3 from data-2.parquet (n=2,4; data=b,d)
    // Snapshot 4: Adds (6,f), (7,g) in data-4.parquet
    //
    // Net effect from snapshot 1 to 4:
    // - Net appends: (1,a), (3,c), (5,e), (6,f), (7,g) - all rows that exist at snapshot 4
    // - No deletes: rows deleted in snapshot 3 were added after snapshot 1, so they don't count as deletes
    fixture
        .verify_incremental_scan(
            1,
            4,
            vec![(1, "a"), (3, "c"), (5, "e"), (6, "f"), (7, "g")],
            vec![], // No deletes (deleted rows were added after snapshot 1)
        )
        .await;

    // Verify incremental scan from snapshot 2 to snapshot 4.
    // Snapshot 2: Has (1,a), (2,b), (3,c), (4,d), (5,e) in data-2.parquet
    // Snapshot 3: Deletes positions 1,3 from data-2.parquet (n=2,4; data=b,d)
    // Snapshot 4: Adds (6,f), (7,g) in data-4.parquet
    //
    // Net effect from snapshot 2 to 4:
    // - Net appends: (6,f), (7,g) from data-4.parquet
    // - Net deletes: positions 1,3 from data-2.parquet (n=2,4; data=b,d) - existed at snapshot 2 and deleted in 3
    fixture
        .verify_incremental_scan(2, 4, vec![(6, "f"), (7, "g")], vec![
            (1, data_2_path.as_str()), // Positions 1,3 from data-2.parquet
            (3, data_2_path.as_str()), // (n=2,4; data=b,d)
        ])
        .await;
}

#[tokio::test]
async fn test_incremental_scan_edge_cases() {
    // This test covers several edge cases:
    // 1. Multiple data files added in separate snapshots
    // 2. Deletes spread across multiple data files
    // 3. Partial deletes from multiple files
    // 4. Cross-file delete operations in a single snapshot
    let fixture = IncrementalTestFixture::new(vec![
        // Snapshot 1: Empty starting point
        Operation::Add(vec![], "empty.parquet".to_string()),
        // Snapshot 2: Add file A with 3 rows
        Operation::Add(
            vec![
                (1, "a1".to_string()),
                (2, "a2".to_string()),
                (3, "a3".to_string()),
            ],
            "file-a.parquet".to_string(),
        ),
        // Snapshot 3: Add file B with 4 rows
        Operation::Add(
            vec![
                (10, "b1".to_string()),
                (20, "b2".to_string()),
                (30, "b3".to_string()),
                (40, "b4".to_string()),
            ],
            "file-b.parquet".to_string(),
        ),
        // Snapshot 4: Partial delete from file A (delete middle row n=2)
        Operation::Delete(vec![(1, "file-a.parquet".to_string())]),
        // Snapshot 5: Partial delete from file B (delete first and last rows n=10,40)
        Operation::Delete(vec![
            (0, "file-b.parquet".to_string()),
            (3, "file-b.parquet".to_string()),
        ]),
        // Snapshot 6: Add file C with 2 rows
        Operation::Add(
            vec![(100, "c1".to_string()), (200, "c2".to_string())],
            "file-c.parquet".to_string(),
        ),
        // Snapshot 7: Delete from multiple files in one snapshot (cross-file deletes)
        Operation::Delete(vec![
            (0, "file-a.parquet".to_string()), // n=1
            (1, "file-b.parquet".to_string()), // n=20
            (0, "file-c.parquet".to_string()), // n=100
        ]),
    ])
    .await;

    // Verify we have 7 snapshots
    let n_snapshots = fixture.table.metadata().snapshots().count();
    assert_eq!(n_snapshots, 7);

    let file_a_path = format!("{}/data/file-a.parquet", fixture.table_location);
    let file_b_path = format!("{}/data/file-b.parquet", fixture.table_location);

    // Test 1: Scan from snapshot 1 to 4
    // Should see: file-a (1,2,3), file-b (10,20,30,40) added, then (2) deleted from file-a
    // BUT: The row n=2 was added AFTER snapshot 1, so it won't show as a delete!
    // Net: appends (1,3) from file-a (n=2 added then deleted = net zero), (10,20,30,40) from file-b
    // No deletes (n=2 was added and deleted between snapshots 1 and 4)
    fixture
        .verify_incremental_scan(
            1,
            4,
            vec![
                (1, "a1"),
                (3, "a3"),
                (10, "b1"),
                (20, "b2"),
                (30, "b3"),
                (40, "b4"),
            ],
            vec![], // No deletes - n=2 was added and deleted between snapshots
        )
        .await;

    // Test 2: Scan from snapshot 4 to 6
    // Snapshot 4: has file-a (1,3) and file-b (10,20,30,40)
    // Snapshot 5: deletes positions 0,3 from file-b (n=10,40)
    // Snapshot 6: adds file-c (100,200)
    // Net: appends (100,200) from file-c; deletes pos 0,3 from file-b
    fixture
        .verify_incremental_scan(4, 6, vec![(100, "c1"), (200, "c2")], vec![
            (0, file_b_path.as_str()), // n=10
            (3, file_b_path.as_str()), // n=40
        ])
        .await;

    // Test 3: Scan from snapshot 2 to 7
    // This tests the full lifecycle: multiple adds, partial deletes, more adds, cross-file deletes
    // Starting at snapshot 2: file-a (1,2,3) exists
    // File-b is added in snapshot 3 (after snapshot 2)
    // File-c is added in snapshot 6 (after snapshot 2)
    // By snapshot 7: file-a has (3) at position 2, file-b has (30), file-c has (200)
    //
    // Net appends: file-b (30) and file-c (200) were added after snapshot 2
    // Net deletes: positions 0,1 from file-a (n=1,2) existed at snapshot 2 and were deleted
    // Note: (3) from file-a already existed at snapshot 2, so it's not a net append!
    fixture
        .verify_incremental_scan(2, 7, vec![(30, "b3"), (200, "c2")], vec![
            (0, file_a_path.as_str()), // n=1
            (1, file_a_path.as_str()), // n=2
        ])
        .await;

    // Test 4: Scan from snapshot 5 to 6
    // Simple test: just adding a new file
    // Snapshot 5 state: file-a (1,3), file-b (20,30)
    // Snapshot 6: adds file-c (100,200)
    // Net: appends (100,200) from file-c, no deletes
    fixture
        .verify_incremental_scan(5, 6, vec![(100, "c1"), (200, "c2")], vec![])
        .await;

    // Test 5: Scan from snapshot 3 to 4
    // Tests a single delete operation
    // State at 3: file-a (1,2,3), file-b (10,20,30,40)
    // State at 4: file-a (1,3), file-b (10,20,30,40)
    // Net: no appends, 1 delete (position 1, n=2) from file-a
    fixture
        .verify_incremental_scan(
            3,
            4,
            vec![],
            vec![(1, file_a_path.as_str())], // n=2
        )
        .await;
}

#[tokio::test]
async fn test_incremental_scan_builder_options() {
    // This test demonstrates using the incremental scan builder API with various options:
    // - Column projection (selecting specific columns)
    // - Batch size configuration
    // - Verifying the schema and batch structure
    let fixture = IncrementalTestFixture::new(vec![
        Operation::Add(vec![], "empty.parquet".to_string()),
        // Snapshot 2: Add 10 rows to test batch size behavior
        Operation::Add(
            vec![
                (1, "data-1".to_string()),
                (2, "data-2".to_string()),
                (3, "data-3".to_string()),
                (4, "data-4".to_string()),
                (5, "data-5".to_string()),
                (6, "data-6".to_string()),
                (7, "data-7".to_string()),
                (8, "data-8".to_string()),
                (9, "data-9".to_string()),
                (10, "data-10".to_string()),
            ],
            "data-2.parquet".to_string(),
        ),
        // Snapshot 3: Delete some rows
        Operation::Delete(vec![
            (2, "data-2.parquet".to_string()), // n=3
            (5, "data-2.parquet".to_string()), // n=6
            (8, "data-2.parquet".to_string()), // n=9
        ]),
        // Snapshot 4: Add more rows
        Operation::Add(
            vec![
                (20, "data-20".to_string()),
                (21, "data-21".to_string()),
                (22, "data-22".to_string()),
                (23, "data-23".to_string()),
                (24, "data-24".to_string()),
            ],
            "data-4.parquet".to_string(),
        ),
    ])
    .await;

    // Test 1: Column projection - select only the "n" column
    let scan = fixture
        .table
        .incremental_scan(1, 4)
        .select(vec!["n"])
        .build()
        .unwrap();

    let stream = scan.to_arrow().await.unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();

    // Verify we have both append and delete batches
    let append_batches: Vec<_> = batches
        .iter()
        .filter(|(t, _)| *t == crate::arrow::IncrementalBatchType::Append)
        .map(|(_, b)| b.clone())
        .collect();

    assert!(!append_batches.is_empty(), "Should have append batches");

    // Check schema - should only have "n" column
    for batch in &append_batches {
        assert_eq!(
            batch.schema().fields().len(),
            1,
            "Should have only 1 column when projecting 'n'"
        );
        assert_eq!(
            batch.schema().field(0).name(),
            "n",
            "Projected column should be 'n'"
        );
    }

    // Test 2: Column projection - select only the "data" column
    let scan = fixture
        .table
        .incremental_scan(1, 4)
        .select(vec!["data"])
        .build()
        .unwrap();

    let stream = scan.to_arrow().await.unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();

    let append_batches: Vec<_> = batches
        .iter()
        .filter(|(t, _)| *t == crate::arrow::IncrementalBatchType::Append)
        .map(|(_, b)| b.clone())
        .collect();

    for batch in &append_batches {
        assert_eq!(
            batch.schema().fields().len(),
            1,
            "Should have only 1 column when projecting 'data'"
        );
        assert_eq!(
            batch.schema().field(0).name(),
            "data",
            "Projected column should be 'data'"
        );
    }

    // Test 3: Select both columns explicitly
    let scan = fixture
        .table
        .incremental_scan(1, 4)
        .select(vec!["n", "data"])
        .build()
        .unwrap();

    let stream = scan.to_arrow().await.unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();

    let append_batches: Vec<_> = batches
        .iter()
        .filter(|(t, _)| *t == crate::arrow::IncrementalBatchType::Append)
        .map(|(_, b)| b.clone())
        .collect();

    for batch in &append_batches {
        assert_eq!(
            batch.schema().fields().len(),
            2,
            "Should have 2 columns when projecting both"
        );
        assert_eq!(batch.schema().field(0).name(), "n");
        assert_eq!(batch.schema().field(1).name(), "data");
    }

    // Test 4: Batch size configuration
    let scan = fixture
        .table
        .incremental_scan(1, 2)
        .with_batch_size(Some(3)) // Small batch size to test batching
        .build()
        .unwrap();

    let stream = scan.to_arrow().await.unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();

    let append_batches = batches
        .iter()
        .filter(|(t, _)| *t == crate::arrow::IncrementalBatchType::Append)
        .map(|(_, b)| b.clone());

    for batch in append_batches {
        // Each batch should have at most 3 rows (except possibly the last)
        assert!(
            batch.num_rows() <= 3,
            "Batch size should be <= 3 as configured"
        );
    }

    // Test 5: Combining column projection and batch size
    let scan = fixture
        .table
        .incremental_scan(1, 4)
        .select(vec!["n"])
        .with_batch_size(Some(4))
        .build()
        .unwrap();

    let stream = scan.to_arrow().await.unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();

    let append_batches = batches
        .iter()
        .filter(|(t, _)| *t == crate::arrow::IncrementalBatchType::Append)
        .map(|(_, b)| b.clone());

    for batch in append_batches {
        assert_eq!(batch.schema().fields().len(), 1, "Should project only 'n'");
        assert!(batch.num_rows() <= 4, "Batch size should be <= 4");
    }

    // Test 6: Verify actual data with column projection
    let scan = fixture
        .table
        .incremental_scan(1, 2)
        .select(vec!["n"])
        .build()
        .unwrap();

    let stream = scan.to_arrow().await.unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();

    let append_batches: Vec<_> = batches
        .iter()
        .filter(|(t, _)| *t == crate::arrow::IncrementalBatchType::Append)
        .map(|(_, b)| b.clone())
        .collect();

    if !append_batches.is_empty() {
        use arrow_select::concat::concat_batches;
        let combined_batch =
            concat_batches(&append_batches[0].schema(), append_batches.iter()).unwrap();

        let n_array = combined_batch
            .column(0)
            .as_primitive::<arrow_array::types::Int32Type>();

        let mut n_values: Vec<i32> = (0..combined_batch.num_rows())
            .map(|i| n_array.value(i))
            .collect();
        n_values.sort();

        assert_eq!(
            n_values,
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "Should have all 10 n values from snapshot 2"
        );
    }

    // Test 7: Delete batches always have the same schema.
    let scan = fixture.table.incremental_scan(2, 3).build().unwrap();

    let stream = scan.to_arrow().await.unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();

    let delete_batches: Vec<_> = batches
        .iter()
        .filter(|(t, _)| *t == crate::arrow::IncrementalBatchType::Delete)
        .map(|(_, b)| b.clone())
        .collect();

    if !delete_batches.is_empty() {
        for batch in &delete_batches {
            // Delete batches should have "pos" and "_file" columns
            assert!(
                batch.schema().fields().len() == 2,
                "Delete batch should have exactly position and file columns"
            );
            assert_eq!(
                batch.num_rows(),
                3,
                "Should have 3 deleted positions from snapshot 3"
            );
        }
    }
}

#[tokio::test]
async fn test_incremental_scan_with_deleted_files_errors() {
    // This test verifies that incremental scans properly error out when entire data files
    // are deleted (overwrite operation), since this is not yet supported.
    //
    // Test scenario:
    // Snapshot 1: Add file-1.parquet with data
    // Snapshot 2: Add file-2.parquet with data
    // Snapshot 3: Overwrite - delete file-1.parquet entirely
    // Snapshot 4: Add file-3.parquet with data
    //
    // Incremental scan from snapshot 1 to snapshot 3 should error because file-1
    // was completely removed in the overwrite operation.

    let fixture = IncrementalTestFixture::new(vec![
        // Snapshot 1: Add file-1 with rows
        Operation::Add(
            vec![
                (1, "a".to_string()),
                (2, "b".to_string()),
                (3, "c".to_string()),
            ],
            "file-1.parquet".to_string(),
        ),
        // Snapshot 2: Add file-2 with rows
        Operation::Add(
            vec![(10, "x".to_string()), (20, "y".to_string())],
            "file-2.parquet".to_string(),
        ),
        // Snapshot 3: Overwrite - delete file-1 entirely
        Operation::Overwrite(
            (vec![], "".to_string()),           // No new data to add
            vec![],                             // No positional deletes
            vec!["file-1.parquet".to_string()], // Delete file-1 completely
        ),
        // Snapshot 4: Add file-3 (to have more snapshots)
        Operation::Add(vec![(100, "p".to_string())], "file-3.parquet".to_string()),
    ])
    .await;

    // Test 1: Incremental scan from snapshot 1 to 3 should error when building the stream
    // because file-1 was deleted entirely in snapshot 3
    let scan = fixture
        .table
        .incremental_scan(1, 3)
        .build()
        .expect("Building the scan should succeed");

    let stream_result = scan.to_arrow().await;

    match stream_result {
        Err(error) => {
            assert_eq!(
                error.message(),
                "Processing deleted data files is not supported yet in incremental scans",
                "Error message should indicate that deleted files are not supported. Got: {}",
                error
            );
        }
        Ok(_) => panic!(
            "Expected error when building stream over a snapshot that deletes entire data files"
        ),
    }

    // Test 2: Incremental scan from snapshot 2 to 4 should also error
    // because it includes snapshot 3 which deletes a file
    let scan = fixture
        .table
        .incremental_scan(2, 4)
        .build()
        .expect("Building the scan should succeed");

    let stream_result = scan.to_arrow().await;

    match stream_result {
        Err(_) => {
            // Expected error
        }
        Ok(_) => panic!("Expected error when scan range includes a snapshot that deletes files"),
    }

    // Test 3: Incremental scan from snapshot 1 to 2 should work fine
    // (no files deleted)
    let scan = fixture
        .table
        .incremental_scan(1, 2)
        .build()
        .expect("Building the scan should succeed");

    let stream_result = scan.to_arrow().await;

    assert!(
        stream_result.is_ok(),
        "Scan should succeed when no files are deleted. Error: {:?}",
        stream_result.err()
    );

    // Test 4: Incremental scan from snapshot 3 to 4 should work
    // (starting from after the deletion)
    let scan = fixture
        .table
        .incremental_scan(3, 4)
        .build()
        .expect("Building the scan should succeed");

    let stream_result = scan.to_arrow().await;

    assert!(
        stream_result.is_ok(),
        "Scan should succeed when starting after the file deletion. Error: {:?}",
        stream_result.err()
    );
}
