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

//! End-to-end delete-application tests for [`ArrowReader`]: build a data file and
//! one or more delete files, read the file scan task through `ArrowReader::read`,
//! and assert the surviving rows. These complement the loader/predicate/row-group
//! unit tests by covering the full read pipeline for scenarios that combine or
//! stack delete files:
//!
//! - a single equality delete (end-to-end through the reader),
//! - multiple equality delete files targeting one data file (unioned),
//! - multiple position delete files targeting one data file (unioned),
//! - mixed position + equality deletes on the same data file,
//! - a data file whose every row is deleted.

use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use futures::TryStreamExt;
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use tempfile::TempDir;

use crate::Runtime;
use crate::arrow::ArrowReaderBuilder;
use crate::io::FileIO;
use crate::scan::{FileScanTask, FileScanTaskDeleteFile, FileScanTaskStream};
use crate::spec::{
    DataContentType, DataFileFormat, NestedField, PrimitiveType, Schema, SchemaRef, Type,
};

// Reserved Iceberg field IDs for the two columns of a position-delete file.
const POS_DELETE_FILE_PATH_FIELD_ID: i32 = 2147483546;
const POS_DELETE_POS_FIELD_ID: i32 = 2147483545;

fn field_with_id(name: &str, dt: DataType, id: i32, nullable: bool) -> Field {
    Field::new(name, dt, nullable).with_metadata(HashMap::from([(
        PARQUET_FIELD_ID_META_KEY.to_string(),
        id.to_string(),
    )]))
}

fn write_parquet(path: &str, schema: Arc<ArrowSchema>, columns: Vec<ArrayRef>) {
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

/// The `(id: long, val: string)` Iceberg table schema used by all tests here.
fn id_val_schema() -> SchemaRef {
    Arc::new(
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(2, "val", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap(),
    )
}

/// Write an `(id, val)` data file with `val = "v{id}"`.
fn write_data(path: &str, ids: Vec<i64>) {
    let vals: Vec<String> = ids.iter().map(|i| format!("v{i}")).collect();
    write_parquet(
        path,
        Arc::new(ArrowSchema::new(vec![
            field_with_id("id", DataType::Int64, 1, false),
            field_with_id("val", DataType::Utf8, 2, true),
        ])),
        vec![
            Arc::new(Int64Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(vals)) as ArrayRef,
        ],
    );
}

/// Write an equality-delete file keyed on `id` and return its descriptor.
fn eq_delete_file(path: &str, ids: Vec<i64>) -> FileScanTaskDeleteFile {
    write_parquet(
        path,
        Arc::new(ArrowSchema::new(vec![field_with_id(
            "id",
            DataType::Int64,
            1,
            false,
        )])),
        vec![Arc::new(Int64Array::from(ids)) as ArrayRef],
    );
    FileScanTaskDeleteFile::builder()
        .with_file_path(path.to_string())
        .with_file_size_in_bytes(std::fs::metadata(path).unwrap().len())
        .with_file_type(DataContentType::EqualityDeletes)
        .with_equality_ids(Some(vec![1]))
        .with_partition_spec_id(0)
        .build()
}

/// Write a position-delete file marking the given 0-based `positions` of
/// `data_path` and return its descriptor.
fn pos_delete_file(path: &str, data_path: &str, positions: Vec<i64>) -> FileScanTaskDeleteFile {
    let n = positions.len();
    write_parquet(
        path,
        Arc::new(ArrowSchema::new(vec![
            field_with_id(
                "file_path",
                DataType::Utf8,
                POS_DELETE_FILE_PATH_FIELD_ID,
                false,
            ),
            field_with_id("pos", DataType::Int64, POS_DELETE_POS_FIELD_ID, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec![data_path; n])) as ArrayRef,
            Arc::new(Int64Array::from(positions)) as ArrayRef,
        ],
    );
    FileScanTaskDeleteFile::builder()
        .with_file_path(path.to_string())
        .with_file_size_in_bytes(std::fs::metadata(path).unwrap().len())
        .with_file_type(DataContentType::PositionDeletes)
        .with_partition_spec_id(0)
        .build()
}

/// Read `data_path` with `deletes` applied and return the surviving `id`s in
/// file order.
async fn read_surviving_ids(
    data_path: &str,
    table_schema: SchemaRef,
    deletes: Vec<FileScanTaskDeleteFile>,
) -> Vec<i64> {
    let file_io = FileIO::new_with_fs();
    let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();
    let task = FileScanTask::builder()
        .with_file_size_in_bytes(std::fs::metadata(data_path).unwrap().len())
        .with_start(0)
        .with_length(0)
        .with_data_file_path(data_path.to_string())
        .with_data_file_format(DataFileFormat::Parquet)
        .with_schema(table_schema)
        .with_project_field_ids(vec![1, 2])
        .with_deletes(deletes)
        .with_case_sensitive(false)
        .build();
    let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
    let batches = reader
        .read(tasks)
        .unwrap()
        .stream()
        .try_collect::<Vec<RecordBatch>>()
        .await
        .unwrap();
    batches
        .iter()
        .flat_map(|b| b.column(0).as_primitive::<Int64Type>().iter().flatten())
        .collect()
}

// A single equality delete removes exactly the matching rows, end-to-end.
#[tokio::test]
async fn test_equality_delete_removes_matching_rows() {
    let tmp = TempDir::new().unwrap();
    let loc = tmp.path().to_str().unwrap();
    let data = format!("{loc}/data.parquet");
    write_data(&data, vec![1, 2, 3, 4, 5]);

    let del = format!("{loc}/eq.parquet");
    let result =
        read_surviving_ids(&data, id_val_schema(), vec![eq_delete_file(&del, vec![3])]).await;

    assert_eq!(result, vec![1, 2, 4, 5]);
}

// Two equality delete files targeting the same data file are both applied
// (their keys are unioned).
#[tokio::test]
async fn test_multiple_equality_delete_files_union() {
    let tmp = TempDir::new().unwrap();
    let loc = tmp.path().to_str().unwrap();
    let data = format!("{loc}/data.parquet");
    write_data(&data, vec![1, 2, 3, 4, 5, 6]);

    let eq_a = format!("{loc}/eq-a.parquet");
    let eq_b = format!("{loc}/eq-b.parquet");
    let result = read_surviving_ids(&data, id_val_schema(), vec![
        eq_delete_file(&eq_a, vec![2]),
        eq_delete_file(&eq_b, vec![5]),
    ])
    .await;

    assert_eq!(result, vec![1, 3, 4, 6]);
}

// Two position delete files targeting the same data file are both applied
// (their positions are unioned).
#[tokio::test]
async fn test_multiple_position_delete_files_union() {
    let tmp = TempDir::new().unwrap();
    let loc = tmp.path().to_str().unwrap();
    let data = format!("{loc}/data.parquet");
    write_data(&data, vec![1, 2, 3, 4, 5, 6]);

    // pos 1 -> id=2, pos 3 -> id=4.
    let pd_a = format!("{loc}/pd-a.parquet");
    let pd_b = format!("{loc}/pd-b.parquet");
    let result = read_surviving_ids(&data, id_val_schema(), vec![
        pos_delete_file(&pd_a, &data, vec![1]),
        pos_delete_file(&pd_b, &data, vec![3]),
    ])
    .await;

    assert_eq!(result, vec![1, 3, 5, 6]);
}

// Position and equality deletes on the same data file are both honoured.
#[tokio::test]
async fn test_mixed_position_and_equality_deletes() {
    let tmp = TempDir::new().unwrap();
    let loc = tmp.path().to_str().unwrap();
    let data = format!("{loc}/data.parquet");
    write_data(&data, vec![1, 2, 3, 4, 5, 6]);

    // Position 0 -> id=1; equality id=4. Expect {2,3,5,6}.
    let pd = format!("{loc}/pd.parquet");
    let eq = format!("{loc}/eq.parquet");
    let result = read_surviving_ids(&data, id_val_schema(), vec![
        pos_delete_file(&pd, &data, vec![0]),
        eq_delete_file(&eq, vec![4]),
    ])
    .await;

    assert_eq!(result, vec![2, 3, 5, 6]);
}

// A data file whose every row is deleted yields no surviving rows.
#[tokio::test]
async fn test_fully_deleted_data_file_returns_no_rows() {
    let tmp = TempDir::new().unwrap();
    let loc = tmp.path().to_str().unwrap();
    let data = format!("{loc}/data.parquet");
    write_data(&data, vec![1, 2, 3]);

    let eq = format!("{loc}/eq.parquet");
    let result = read_surviving_ids(&data, id_val_schema(), vec![eq_delete_file(&eq, vec![
        1, 2, 3,
    ])])
    .await;

    assert_eq!(result, Vec::<i64>::new());
}
