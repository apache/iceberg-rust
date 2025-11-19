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
use crate::metadata_columns::RESERVED_COL_NAME_FILE;
use crate::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, ManifestEntry, ManifestListWriter,
    ManifestStatus, ManifestWriterBuilder, PartitionSpec, SchemaRef, Struct, TableMetadata,
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

    /// Replace operation for file compaction/reorganization.
    /// The logical table content does NOT change - only the physical file representation changes.
    ///
    /// Parameters:
    /// 1. Files to compact: Vec<String> of existing file names that are being compacted
    /// 2. Target file: String name of the new compacted file
    ///
    /// Example: `Replace(vec!["file-a.parquet", "file-b.parquet"], "file-a-b-compacted.parquet")`
    /// This compacts two existing files into one new file with the same logical content.
    ///
    /// For an incremental scan that only contains Replace operations, the result should be
    /// zero additions and zero deletions, because the logical data hasn't changed.
    Replace(Vec<String>, String),
}

/// Tracks the state of data files across snapshots
#[derive(Debug, Clone)]
struct DataFileInfo {
    path: String,
    snapshot_id: i64,
    sequence_number: i64,
    n_values: Vec<i32>,
    data_values: Vec<String>,
    file_size: u64,
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
                Operation::Replace(..) => "replace",
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
        let mut delete_files: Vec<(String, i64, i64, Vec<(String, i64)>, u64)> = Vec::new(); // (path, snapshot_id, sequence_number, [(data_file_path, position)], file_size)

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
                                            .file_size_in_bytes(data_file.file_size)
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
                        let file_size = self
                            .write_parquet_file(&data_file_path, &n_values, &data_values)
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
                                            .file_size_in_bytes(file_size)
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
                            data_values,
                            file_size,
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

                        for (delete_path, del_snapshot_id, del_sequence_number, _, del_file_size) in
                            &delete_files
                        {
                            let delete_count = delete_files
                                .iter()
                                .filter(|(p, _, _, _, _)| p == delete_path)
                                .map(|(_, _, _, deletes, _)| deletes.len())
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
                                                .file_size_in_bytes(*del_file_size)
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
                                            .file_size_in_bytes(data_file.file_size)
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
                    for (delete_path, del_snapshot_id, del_sequence_number, _, del_file_size) in
                        &delete_files
                    {
                        let delete_count = delete_files
                            .iter()
                            .filter(|(p, _, _, _, _)| p == delete_path)
                            .map(|(_, _, _, deletes, _)| deletes.len())
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
                                            .file_size_in_bytes(*del_file_size)
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
                        let delete_file_size = self
                            .write_positional_delete_file(
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
                                            .file_size_in_bytes(delete_file_size)
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
                            delete_file_size,
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
                                                .file_size_in_bytes(data_file.file_size)
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
                                                .file_size_in_bytes(data_file.file_size)
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

                        let file_size = self
                            .write_parquet_file(&data_file_path, &n_values, &data_values)
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
                                            .file_size_in_bytes(file_size)
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
                            data_values,
                            file_size,
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
                        for (delete_path, del_snapshot_id, del_sequence_number, _, del_file_size) in
                            &delete_files
                        {
                            let delete_count = delete_files
                                .iter()
                                .filter(|(p, _, _, _, _)| p == delete_path)
                                .map(|(_, _, _, deletes, _)| deletes.len())
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
                                                .file_size_in_bytes(*del_file_size)
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
                                let delete_file_size = self
                                    .write_positional_delete_file(
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
                                                    .file_size_in_bytes(delete_file_size)
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
                                    delete_file_size,
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

                Operation::Replace(files_to_compact, target_file) => {
                    // Replace operation: compact existing files into a new file
                    // The logical content doesn't change, only the physical representation
                    self.handle_replace_operation(
                        files_to_compact,
                        target_file,
                        &mut data_files,
                        &delete_files,
                        current_schema.clone(),
                        &partition_spec,
                        snapshot_id,
                        sequence_number,
                        parent_snapshot_id,
                    )
                    .await
                    .unwrap();
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments, clippy::type_complexity)]
    async fn handle_replace_operation(
        &mut self,
        files_to_compact: &[String],
        target_file: &str,
        data_files: &mut Vec<DataFileInfo>,
        delete_files: &[(String, i64, i64, Vec<(String, i64)>, u64)],
        current_schema: SchemaRef,
        partition_spec: &PartitionSpec,
        snapshot_id: i64,
        sequence_number: i64,
        parent_snapshot_id: Option<i64>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let empty_partition = Struct::empty();

        // Create data manifest
        let mut data_writer = ManifestWriterBuilder::new(
            self.next_manifest_file(),
            Some(snapshot_id),
            None,
            current_schema,
            partition_spec.clone(),
        )
        .build_v2_data();

        // Determine which files are being compacted
        let files_to_compact_set: std::collections::HashSet<String> = files_to_compact
            .iter()
            .map(|f| format!("{}/data/{}", &self.table_location, f))
            .collect();

        // Build a set of deleted positions for each file being compacted
        let mut deleted_positions: std::collections::HashMap<
            String,
            std::collections::HashSet<i64>,
        > = std::collections::HashMap::new();
        for (_, _, _, delete_records, _) in delete_files {
            for (file_path, position) in delete_records {
                deleted_positions
                    .entry(file_path.clone())
                    .or_default()
                    .insert(*position);
            }
        }

        // Track the data being compacted
        let mut compacted_data: Vec<(i32, String)> = Vec::new();
        let mut compacted_record_count: u64 = 0;

        // Add existing data files (mark compacted ones as DELETED, others as EXISTING)
        for data_file in data_files.iter() {
            if files_to_compact_set.contains(&data_file.path) {
                // Mark file as deleted (being compacted away)
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
                                    .file_size_in_bytes(data_file.file_size)
                                    .record_count(data_file.n_values.len() as u64)
                                    .partition(empty_partition.clone())
                                    .key_metadata(None)
                                    .build()
                                    .unwrap(),
                            )
                            .build(),
                    )
                    .unwrap();

                // Collect data from compacted files, filtering out deleted records
                let file_deleted_positions = deleted_positions.get(&data_file.path);
                for (position, (n, d)) in data_file
                    .n_values
                    .iter()
                    .zip(data_file.data_values.iter())
                    .enumerate()
                {
                    // Skip this record if it was deleted via positional delete
                    if let Some(deleted) = file_deleted_positions {
                        if deleted.contains(&(position as i64)) {
                            continue;
                        }
                    }
                    compacted_data.push((*n, d.clone()));
                }
                compacted_record_count += data_file.n_values.len() as u64;
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
                                    .file_size_in_bytes(data_file.file_size)
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

        // Create the compacted file with the collected data
        if !compacted_data.is_empty() {
            let compacted_n_values: Vec<i32> = compacted_data.iter().map(|(n, _)| *n).collect();
            let compacted_data_values: Vec<String> =
                compacted_data.iter().map(|(_, d)| d.clone()).collect();
            let compacted_file_path = format!("{}/data/{}", &self.table_location, target_file);

            let file_size = self
                .write_parquet_file(
                    &compacted_file_path,
                    &compacted_n_values,
                    &compacted_data_values,
                )
                .await;

            data_writer
                .add_entry(
                    ManifestEntry::builder()
                        .status(ManifestStatus::Added)
                        .data_file(
                            DataFileBuilder::default()
                                .partition_spec_id(0)
                                .content(DataContentType::Data)
                                .file_path(compacted_file_path.clone())
                                .file_format(DataFileFormat::Parquet)
                                .file_size_in_bytes(file_size)
                                .record_count(compacted_record_count)
                                .partition(empty_partition.clone())
                                .key_metadata(None)
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .unwrap();

            // Update data_files tracking: remove compacted, add new
            data_files.retain(|df| !files_to_compact_set.contains(&df.path));
            data_files.push(DataFileInfo {
                path: compacted_file_path,
                snapshot_id,
                sequence_number,
                n_values: compacted_n_values,
                data_values: compacted_data_values,
                file_size,
            });
        }

        let data_manifest = data_writer.write_manifest_file().await.unwrap();

        // Write manifest list (no delete manifests for Replace)
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
            .add_manifests(vec![data_manifest].into_iter())
            .unwrap();
        manifest_list_write.close().await.unwrap();

        Ok(())
    }

    async fn write_parquet_file(
        &self,
        path: &str,
        n_values: &[i32],
        data_values: &[String],
    ) -> u64 {
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

        // Return the actual file size
        fs::metadata(path).unwrap().len()
    }

    async fn write_positional_delete_file(
        &self,
        path: &str,
        data_file_path: &str,
        positions: &[i64],
    ) -> u64 {
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

        // Return the actual file size
        fs::metadata(path).unwrap().len()
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
            .incremental_scan(Some(from_snapshot_id), Some(to_snapshot_id))
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
    // Scan from root (None) to last (None)
    let scan = fixture
        .table
        .incremental_scan(None, None)
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
    // Scan from root (None) to last (None)
    let scan = fixture
        .table
        .incremental_scan(None, None)
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
    // Scan from root (None) to last (None)
    let scan = fixture
        .table
        .incremental_scan(None, None)
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
    // Scan from root (None) to snapshot 2
    let scan = fixture
        .table
        .incremental_scan(None, Some(2))
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
    // Scan from root (None) to last (None)
    let scan = fixture
        .table
        .incremental_scan(None, None)
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
    // Scan from root (None) to snapshot 2
    let scan = fixture
        .table
        .incremental_scan(None, Some(2))
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
    let scan = fixture
        .table
        .incremental_scan(Some(2), Some(3))
        .build()
        .unwrap();

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
async fn test_incremental_scan_append_then_delete_file() {
    // This test verifies the basic delete file functionality:
    // - Snapshot 1: Empty starting point
    // - Snapshot 2: Append a file with 3 records
    // - Snapshot 3: Overwrite that deletes the entire file
    //
    // An incremental scan from snapshot 1 to 3 should yield no tuples, as there is no net change.
    let fixture = IncrementalTestFixture::new(vec![
        // Snapshot 1: Empty starting point
        Operation::Add(vec![], "empty.parquet".to_string()),
        // Snapshot 2: Append a file with 3 records
        Operation::Add(
            vec![
                (1, "a".to_string()),
                (2, "b".to_string()),
                (3, "c".to_string()),
            ],
            "data-1.parquet".to_string(),
        ),
        // Snapshot 3: Overwrite that deletes the entire file
        Operation::Overwrite(
            (vec![], "".to_string()),           // No new data to add
            vec![],                             // No positional deletes
            vec!["data-1.parquet".to_string()], // Delete the file completely
        ),
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

    // Incremental scan from snapshot 1 to 3 should yield no tuples
    // The file was added in snapshot 2 and deleted in snapshot 3, so they cancel out
    fixture
        .verify_incremental_scan(
            1,
            3,
            vec![], // No net appends
            vec![], // No net deletes
        )
        .await;

    // Incremental scan from snapshot 1 to 2 should yield the appended records
    fixture
        .verify_incremental_scan(
            1,
            2,
            vec![(1, "a"), (2, "b"), (3, "c")], // All 3 records appended
            vec![],                             // No deletes
        )
        .await;

    // Incremental scan from snapshot 2 to 3 should produce delete records
    // The file existed at snapshot 2 and was deleted in snapshot 3
    let data_file_path = format!("{}/data/data-1.parquet", fixture.table_location);
    fixture
        .verify_incremental_scan(
            2,
            3,
            vec![], // No appends
            vec![
                // All 3 positions deleted (pos values are 0-indexed: 0, 1, 2)
                (0, data_file_path.as_str()),
                (1, data_file_path.as_str()),
                (2, data_file_path.as_str()),
            ],
        )
        .await;
}

#[tokio::test]
async fn test_incremental_scan_positional_deletes_then_file_delete() {
    // This test verifies that the system correctly handles the case where:
    // - Snapshot 1: Empty starting point
    // - Snapshot 2: Append a file with 3 records
    // - Snapshot 3: Delete these 3 records using positional deletes
    // - Snapshot 4: Delete the file entirely (that was introduced in snapshot 2)
    //
    // The system should avoid double-deletes by filtering out positional deletes
    // for files that are subsequently deleted entirely.
    let fixture = IncrementalTestFixture::new(vec![
        // Snapshot 1: Empty starting point
        Operation::Add(vec![], "empty.parquet".to_string()),
        // Snapshot 2: Append a file with 3 records
        Operation::Add(
            vec![
                (1, "a".to_string()),
                (2, "b".to_string()),
                (3, "c".to_string()),
            ],
            "data-1.parquet".to_string(),
        ),
        // Snapshot 3: Delete one record using a positional delete
        Operation::Delete(vec![(0, "data-1.parquet".to_string())]),
        // Snapshot 4: Delete the file entirely
        Operation::Overwrite(
            (vec![], "".to_string()),           // No new data to add
            vec![],                             // No positional deletes
            vec!["data-1.parquet".to_string()], // Delete the file completely
        ),
    ])
    .await;

    // Verify we have 4 snapshots
    let mut snapshots = fixture.table.metadata().snapshots().collect::<Vec<_>>();
    snapshots.sort_by_key(|s| s.snapshot_id());
    assert_eq!(snapshots.len(), 4);

    // Incremental scan from snapshot 1 to 4
    // Since data-1.parquet was both appended (snapshot 2) and deleted (snapshot 4)
    // in the scan range, BOTH appends and deletes are fully cancelled out.
    // Even though there were positional deletes in snapshot 3, those are filtered out
    // because the file qualifies for cancellation (appended && deleted).
    let data_file_path = format!("{}/data/data-1.parquet", fixture.table_location);
    fixture
        .verify_incremental_scan(
            1,
            4,
            vec![], // No appends (file was added and fully deleted, cancelled out)
            vec![], // No deletes (fully cancelled since file was added and deleted in range)
        )
        .await;

    // Incremental scan from snapshot 3 to 4
    // This demonstrates the double-delete scenario:
    // - At snapshot 3, all records have already been deleted via positional deletes
    // - At snapshot 4, the entire file is deleted
    // - The scan from 3 to 4 should still emit delete records for the file,
    //   even though the records were already deleted in snapshot 3
    // This is because the incremental scan doesn't know about the prior state
    fixture
        .verify_incremental_scan(
            3,
            4,
            vec![], // No appends
            vec![
                // File deletion emits positions 0, 1, 2
                // These are "redundant" deletes since the records were already
                // deleted by positional deletes in snapshot 3
                (0, data_file_path.as_str()),
                (1, data_file_path.as_str()),
                (2, data_file_path.as_str()),
            ],
        )
        .await;
}

#[tokio::test]
async fn test_incremental_scan_with_replace_operation() {
    // This test verifies the Replace operation semantics:
    // - A Replace operation compacts multiple files into one
    // - The logical table content does NOT change (same data)
    // - But physically, files are reorganized: old files are DELETED, new file is ADDED
    // - For incremental scans, we report these physical changes (additions and deletions)
    //   even though the logical data is identical
    let fixture = IncrementalTestFixture::new(vec![
        // Snapshot 1: Empty starting point
        Operation::Add(vec![], "empty.parquet".to_string()),
        // Snapshot 2: Add file-a with 3 rows
        Operation::Add(
            vec![
                (1, "a".to_string()),
                (2, "b".to_string()),
                (3, "c".to_string()),
            ],
            "file-a.parquet".to_string(),
        ),
        // Snapshot 3: Add file-b with 2 rows
        Operation::Add(
            vec![(10, "x".to_string()), (20, "y".to_string())],
            "file-b.parquet".to_string(),
        ),
        // Snapshot 4: Replace (compact file-a and file-b into file-ab-compact)
        // This will delete file-a and file-b, and add file-ab-compact with the same data
        Operation::Replace(
            vec!["file-a.parquet".to_string(), "file-b.parquet".to_string()],
            "file-ab-compact.parquet".to_string(),
        ),
    ])
    .await;

    // Verify we have 4 snapshots
    let mut snapshots = fixture.table.metadata().snapshots().collect::<Vec<_>>();
    snapshots.sort_by_key(|s| s.snapshot_id());
    assert_eq!(snapshots.len(), 4);

    // Verify snapshot operations
    assert_eq!(
        snapshots[3].summary().operation,
        crate::spec::Operation::Replace,
        "Snapshot 4 should be a Replace operation"
    );

    let file_a_path = format!("{}/data/file-a.parquet", fixture.table_location);
    let file_b_path = format!("{}/data/file-b.parquet", fixture.table_location);

    // Test 1: Incremental scan from snapshot 3 to 4 (ONLY Replace operation)
    // Physical changes: file-a and file-b are deleted, file-ab-compact is added
    // But the data is the same, so:
    // - Additions: all 5 rows from file-ab-compact (1,2,3,10,20)
    // - Deletions: all 5 rows from file-a and file-b (positions sorted by (position, file_path))
    fixture
        .verify_incremental_scan(
            3,
            4,
            vec![(1, "a"), (2, "b"), (3, "c"), (10, "x"), (20, "y")],
            vec![
                (0, &file_a_path),
                (0, &file_b_path),
                (1, &file_a_path),
                (1, &file_b_path),
                (2, &file_a_path),
            ],
        )
        .await;

    // Test 2: Incremental scan from snapshot 1 to 4 (includes Appends and Replace)
    // Snapshot 2 adds file-a (1,2,3)
    // Snapshot 3 adds file-b (10,20)
    // Snapshot 4 replaces both files (deletes file-a, file-b; adds file-ab-compact)
    // Since file-a and file-b are added in the scan range and then deleted in the Replace:
    // - The additions of file-a and file-b cancel with their deletions
    // - Net result: only file-ab-compact is added (1,2,3,10,20)
    // - Deletions: empty (file-a and file-b deletions cancelled by their additions)
    fixture
        .verify_incremental_scan(
            1,
            4,
            vec![(1, "a"), (2, "b"), (3, "c"), (10, "x"), (20, "y")],
            vec![],
        )
        .await;

    // Test 3: Incremental scan from snapshot 2 to 4
    // Starting from snapshot 2: file-a (1,2,3) exists (added before scan start)
    // Snapshot 3: adds file-b (10,20)
    // Snapshot 4: replaces both files with file-ab-compact
    // Since file-a was added before the scan started, its deletion is not cancelled
    // But file-b is added and deleted within the scan, so cancels out
    // Net result:
    // - Additions: file-ab-compact (1,2,3,10,20)
    // - Deletions: file-a (0,1,2) since it existed at scan start
    fixture
        .verify_incremental_scan(
            2,
            4,
            vec![(1, "a"), (2, "b"), (3, "c"), (10, "x"), (20, "y")],
            vec![(0, &file_a_path), (1, &file_a_path), (2, &file_a_path)],
        )
        .await;
}

#[tokio::test]
async fn test_incremental_scan_with_deleted_files_cancellation() {
    // This test verifies that incremental scans properly handle file deletions with cancellation logic:
    // - Files added and deleted in the same scan range should cancel out
    // - Files deleted that weren't added in the scan range should produce Delete tasks
    //
    // Test scenario:
    // Snapshot 1: Empty starting point
    // Snapshot 2: Add file-1.parquet with data (3 records)
    // Snapshot 3: Add file-2.parquet with data (2 records)
    // Snapshot 4: Overwrite - delete file-1.parquet entirely
    // Snapshot 5: Add file-3.parquet with data (1 record)
    //
    // Incremental scan from snapshot 1 to 4: file-1 added and deleted, should cancel out (only file-2 remains).
    // Incremental scan from snapshot 3 to 5: file-1 deleted but not added in range, produces Delete tasks.
    // Incremental scan from snapshot 1 to 3: file-1 and file-2 added before any deletion.
    // Incremental scan from snapshot 4 to 5: file-3 added after file-1 deletion occurred.

    let fixture = IncrementalTestFixture::new(vec![
        // Snapshot 1: Empty starting point
        Operation::Add(vec![], "empty.parquet".to_string()),
        // Snapshot 2: Add file-1 with rows
        Operation::Add(
            vec![
                (1, "a".to_string()),
                (2, "b".to_string()),
                (3, "c".to_string()),
            ],
            "file-1.parquet".to_string(),
        ),
        // Snapshot 3: Add file-2 with rows
        Operation::Add(
            vec![(10, "x".to_string()), (20, "y".to_string())],
            "file-2.parquet".to_string(),
        ),
        // Snapshot 4: Overwrite - delete file-1 entirely
        Operation::Overwrite(
            (vec![], "".to_string()),           // No new data to add
            vec![],                             // No positional deletes
            vec!["file-1.parquet".to_string()], // Delete file-1 completely
        ),
        // Snapshot 5: Add file-3 (to have more snapshots)
        Operation::Add(vec![(100, "p".to_string())], "file-3.parquet".to_string()),
    ])
    .await;

    let file_1_path = format!("{}/data/file-1.parquet", fixture.table_location);

    // Test 1: Incremental scan from snapshot 1 to 4
    // file-1 was added in snapshot 2 and deleted in snapshot 4
    // Both appends and deletes for file-1 are fully cancelled out:
    // - Appends: only file-2 records (10, x), (20, y)
    // - Deletes: none (file-1 deletions cancelled since it was added in range)
    fixture
        .verify_incremental_scan(
            1,
            4,
            vec![(10, "x"), (20, "y")], // Only file-2 (file-1 cancelled out)
            vec![],                     // No deletes (file-1 fully cancelled)
        )
        .await;

    // Test 2: Incremental scan from snapshot 3 to 5
    // file-1 was deleted in snapshot 4 but wasn't added in the scan range (added in snapshot 2)
    // This produces a net Delete task with positions 0, 1, 2 (all records in file-1)
    fixture
        .verify_incremental_scan(
            3,
            5,
            vec![(100, "p")], // file-3 added in snapshot 5
            vec![
                // file-1 deleted in snapshot 4 (all 3 positions: 0, 1, 2)
                (0, file_1_path.as_str()),
                (1, file_1_path.as_str()),
                (2, file_1_path.as_str()),
            ],
        )
        .await;

    // Test 3: Incremental scan from snapshot 1 to 3
    // Verifies basic append-only operations before any deletions occur.
    // Both file-1 (snapshot 2) and file-2 (snapshot 3) are added, no deletions yet.
    // Expected: All records from both files appear in appends, no deletes.
    fixture
        .verify_incremental_scan(
            1,
            3,
            vec![(1, "a"), (2, "b"), (3, "c"), (10, "x"), (20, "y")],
            vec![], // No deletes
        )
        .await;

    // Test 4: Incremental scan from snapshot 4 to 5
    // Verifies scanning after the deletion has already occurred.
    // Starting from snapshot 4 (after file-1 deletion), only file-3 is added in snapshot 5.
    // Expected: Only file-3 records in appends, no deletes (deletion happened before scan range).
    fixture
        .verify_incremental_scan(
            4,
            5,
            vec![(100, "p")], // file-3 added
            vec![],           // No deletes
        )
        .await;
}

#[tokio::test]
async fn test_incremental_scan_with_replace_and_positional_deletes() {
    // This test verifies Replace operations with positional deletes before and after the replace.
    //
    // Test scenario:
    // Snapshot 1: Empty starting point
    // Snapshot 2: Add file-a with 5 records (1,2,3,4,5)
    // Snapshot 3: Add file-b with 3 records (10,11,12)
    // Snapshot 4: Delete record at position 1 in file-a (delete "2")
    // Snapshot 5: Replace - compact file-a and file-b into file-ab-compact
    //             (containing records 1,3,4,5 from file-a and 10,11,12 from file-b after the previous delete)
    // Snapshot 6: Delete record at position 2 in file-ab-compact (delete "4")

    let fixture = IncrementalTestFixture::new(vec![
        // Snapshot 1: Empty starting point
        Operation::Add(vec![], "empty.parquet".to_string()),
        // Snapshot 2: Add file-a with 5 rows
        Operation::Add(
            vec![
                (1, "1".to_string()),
                (2, "2".to_string()),
                (3, "3".to_string()),
                (4, "4".to_string()),
                (5, "5".to_string()),
            ],
            "file-a.parquet".to_string(),
        ),
        // Snapshot 3: Add file-b with 3 rows
        Operation::Add(
            vec![
                (10, "10".to_string()),
                (11, "11".to_string()),
                (12, "12".to_string()),
            ],
            "file-b.parquet".to_string(),
        ),
        // Snapshot 4: Delete position 1 (record "2") from file-a
        Operation::Delete(vec![(1, "file-a.parquet".to_string())]),
        // Snapshot 5: Replace - compact file-a and file-b into file-ab-compact
        Operation::Replace(
            vec!["file-a.parquet".to_string(), "file-b.parquet".to_string()],
            "file-ab-compact.parquet".to_string(),
        ),
        // Snapshot 6: Delete position 2 (record "4") from file-ab-compact
        Operation::Delete(vec![(2, "file-ab-compact.parquet".to_string())]),
    ])
    .await;

    // Test 1: Full scan from snapshot 1 to 6
    // Snapshot 2: Add file-a (1,2,3,4,5)
    // Snapshot 3: Add file-b (10,11,12)
    // Snapshot 4: Delete position 1 from file-a (record "2" deleted)
    // Snapshot 5: Replace both files with file-ab-compact (compacts to 1,3,4,5,10,11,12, filtering out position 1)
    // Snapshot 6: Delete position 2 from file-ab-compact (record "4" deleted)
    // Net result:
    // - Additions: records from compacted file with deleted positions filtered (1,3,5,10,11,12)
    //   Note: position 1 from file-a (record "2") is filtered during compaction
    //         position 2 from compacted file (record "4") is never added since it's deleted in snapshot 6
    // - Deletions: empty (deletes from Replace are absorbed into the additions of the compacted file)
    fixture
        .verify_incremental_scan(
            1,
            6,
            vec![
                (1, "1"),
                (3, "3"),
                (5, "5"),
                (10, "10"),
                (11, "11"),
                (12, "12"),
            ],
            vec![],
        )
        .await;

    // Test 2: Scan from snapshot 3 to 6 (after file-b added, through replace and final delete)
    // Snapshot 3: Starting point with file-a and file-b (both exist in starting snapshot)
    // Snapshot 4: Delete position 1 from file-a (record "2" deleted)
    // Snapshot 5: Replace both files with file-ab-compact (filters out position 1 from file-a)
    // Snapshot 6: Delete position 2 from file-ab-compact (record "4" deleted)
    // Net result:
    // - Additions: compacted file records with filtered positions (1,3,5,10,11,12)
    // - Deletions: All positions from file-a (0-4) and file-b (0-2) because these files
    //   existed in the starting snapshot (3) and are deleted/replaced in snapshot 5.
    //   Deletes are sorted by (position, file_path) tuple ordering
    let file_a_path = format!("{}/data/file-a.parquet", fixture.table_location);
    let file_b_path = format!("{}/data/file-b.parquet", fixture.table_location);
    let test2_deletes = vec![
        (0, file_a_path.as_str()),
        (0, file_b_path.as_str()),
        (1, file_a_path.as_str()),
        (1, file_b_path.as_str()),
        (2, file_a_path.as_str()),
        (2, file_b_path.as_str()),
        (3, file_a_path.as_str()),
        (4, file_a_path.as_str()),
    ];
    fixture
        .verify_incremental_scan(
            3,
            6,
            vec![
                (1, "1"),
                (3, "3"),
                (5, "5"),
                (10, "10"),
                (11, "11"),
                (12, "12"),
            ],
            test2_deletes,
        )
        .await;

    // Test 3: Scan from snapshot 4 to 6 (after first delete, through replace and final delete)
    // Snapshot 4: Starting point - file-a already has position 1 deleted (record "2")
    // Snapshot 5: Replace both files with file-ab-compact (filters out position 1)
    // Snapshot 6: Delete position 2 from file-ab-compact (record "4" deleted)
    // Net result:
    // - Additions: compacted records with position 1 filtered from file-a (1,3,5,10,11,12)
    // - Deletions: All positions from file-a (0-4) and file-b (0-2) because these files
    //   existed in the starting snapshot (4) and are deleted/replaced in snapshot 5.
    //   Sorted by (position, file_path) tuple ordering
    let file_a_path = format!("{}/data/file-a.parquet", fixture.table_location);
    let file_b_path = format!("{}/data/file-b.parquet", fixture.table_location);
    let test3_deletes = vec![
        (0, file_a_path.as_str()),
        (0, file_b_path.as_str()),
        (1, file_a_path.as_str()),
        (1, file_b_path.as_str()),
        (2, file_a_path.as_str()),
        (2, file_b_path.as_str()),
        (3, file_a_path.as_str()),
        (4, file_a_path.as_str()),
    ];
    fixture
        .verify_incremental_scan(
            4,
            6,
            vec![
                (1, "1"),
                (3, "3"),
                (5, "5"),
                (10, "10"),
                (11, "11"),
                (12, "12"),
            ],
            test3_deletes,
        )
        .await;

    // Test 4: Scan from snapshot 5 to 6 (after replace, only sees the final delete)
    // Snapshot 5: Starting point - file-ab-compact is newly created with records (1,3,4,5,10,11,12)
    // Snapshot 6: Delete position 2 from file-ab-compact (record "4" deleted)
    // Net result:
    // - Additions: empty (file-ab-compact was added in snapshot 5, not in this scan range)
    // - Deletions: position 2 from file-ab-compact (the positional delete in snapshot 6)
    let file_ab_path = format!("{}/data/file-ab-compact.parquet", fixture.table_location);
    let test4_deletes = vec![(2, file_ab_path.as_str())];
    fixture
        .verify_incremental_scan(5, 6, vec![], test4_deletes)
        .await;
}

#[tokio::test]
async fn test_incremental_scan_includes_root_when_from_is_none() {
    // Test that when from=None, the root snapshot is INCLUDED in the scan
    // (not excluded like when an explicit from snapshot is specified)
    let fixture = IncrementalTestFixture::new(vec![
        // Snapshot 1 (root): Add initial data - this should be INCLUDED when from=None
        Operation::Add(
            vec![(1, "one".to_string()), (2, "two".to_string())],
            "root-data.parquet".to_string(),
        ),
        // Snapshot 2: Add more data
        Operation::Add(
            vec![(10, "ten".to_string()), (20, "twenty".to_string())],
            "second-data.parquet".to_string(),
        ),
        // Snapshot 3: Add final data
        Operation::Add(
            vec![(100, "hundred".to_string())],
            "third-data.parquet".to_string(),
        ),
    ])
    .await;

    // Test 1: Scan with explicit from=1 should EXCLUDE snapshot 1 (normal behavior)
    fixture
        .verify_incremental_scan(
            1,                                                   // from snapshot 1 - EXCLUDED
            3,                                                   // to snapshot 3
            vec![(10, "ten"), (20, "twenty"), (100, "hundred")], // Only snapshots 2 and 3
            vec![],
        )
        .await;

    // Test 2: Scan using table.incremental_scan(None, None) API
    // This should INCLUDE the root snapshot
    let scan = fixture.table.incremental_scan(None, None).build().unwrap();
    let stream = scan.to_arrow().await.unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();

    // Collect all appended data
    let append_batches: Vec<_> = batches
        .iter()
        .filter(|(t, _)| *t == crate::arrow::IncrementalBatchType::Append)
        .map(|(_, b)| b.clone())
        .collect();

    // Extract n and data values
    use arrow_array::cast::AsArray;
    let mut results = Vec::new();
    for batch in append_batches {
        let n_array = batch
            .column_by_name("n")
            .unwrap()
            .as_primitive::<arrow_array::types::Int32Type>();
        let data_array = batch.column_by_name("data").unwrap().as_string::<i32>();
        for i in 0..batch.num_rows() {
            results.push((n_array.value(i), data_array.value(i).to_string()));
        }
    }

    // Sort for consistent comparison
    results.sort();

    // Should include ALL data including root snapshot
    assert_eq!(
        results,
        vec![
            (1, "one".to_string()),
            (2, "two".to_string()),
            (10, "ten".to_string()),
            (20, "twenty".to_string()),
            (100, "hundred".to_string()),
        ],
        "Scan with from=None should include root snapshot data"
    );
}

#[tokio::test]
async fn test_incremental_scan_with_file_column() {
    // Test that the _file metadata column works correctly in incremental scans

    let fixture = IncrementalTestFixture::new(vec![
        Operation::Add(vec![], "empty.parquet".to_string()),
        Operation::Add(
            vec![(1, "data1".to_string()), (2, "data2".to_string())],
            "file1.parquet".to_string(),
        ),
        Operation::Add(
            vec![(10, "data10".to_string())],
            "file2.parquet".to_string(),
        ),
    ])
    .await;

    // Scan with _file column using the builder helper
    let scan = fixture
        .table
        .incremental_scan(Some(1), Some(3))
        .select(vec!["n", "data"])
        .with_file_column()
        .build()
        .unwrap();

    let stream = scan.to_arrow().await.unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();

    // Get append batches
    let append_batches: Vec<_> = batches
        .iter()
        .filter(|(t, _)| *t == crate::arrow::IncrementalBatchType::Append)
        .map(|(_, b)| b.clone())
        .collect();

    // Verify we have data and the _file column
    assert!(!append_batches.is_empty(), "Should have append batches");

    for batch in append_batches {
        // Should have 3 columns: n, data, _file
        assert_eq!(
            batch.num_columns(),
            3,
            "Should have n, data, and _file columns"
        );

        // Verify _file column exists
        let file_column = batch.column_by_name(RESERVED_COL_NAME_FILE);
        assert!(file_column.is_some(), "_file column should exist");

        // Verify _file column contains a file path
        let file_col = file_column.unwrap();

        // The _file column will be a RunEndEncoded array with the file path
        if let Some(run_array) = file_col
            .as_any()
            .downcast_ref::<arrow_array::RunArray<arrow_array::types::Int32Type>>()
        {
            let values = run_array.values();
            let string_values = values
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            let file_path = string_values.value(0);

            // Verify file path ends with .parquet and contains the table location
            assert!(
                file_path.ends_with(".parquet"),
                "File path should end with .parquet: {}",
                file_path
            );
            assert!(
                file_path.contains("/data/"),
                "File path should contain /data/: {}",
                file_path
            );
        } else {
            panic!("_file column should be RunEndEncoded array");
        }
    }
}
