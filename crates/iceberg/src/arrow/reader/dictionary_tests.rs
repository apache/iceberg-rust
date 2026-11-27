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

//! Reads with `ArrowReaderOptions::dictionary_field_ids`.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int8Type, Int32Type};
use arrow_array::{ArrayRef, DictionaryArray, Int32Array, RecordBatch, StringArray};
use arrow_cast::cast;
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use arrow_select::concat::concat_batches;
use futures::TryStreamExt;
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use parquet::file::properties::WriterProperties;
use rstest::rstest;
use tempfile::TempDir;

use super::{ArrowReaderBuilder, ArrowReaderOptions};
use crate::arrow::schema::dictionary_string_type;
use crate::expr::{Bind, Predicate, Reference};
use crate::io::FileIO;
use crate::scan::{FileScanTask, FileScanTaskDeleteFile, FileScanTaskStream};
use crate::spec::{
    DataContentType, DataFileFormat, Datum, Literal, NestedField, PartitionSpec, PrimitiveType,
    Schema, SchemaRef, Struct, StructType, Transform, Type,
};
use crate::test_utils::test_runtime;
use crate::{Error, ErrorKind, Result};

/// Data files hold fields 1 `id` and 2 `name`. Fields 3 `added`, 4 `defaulted`
/// (initial default "d"), 5 `region` and 6 `nested` (with string child 7) are absent.
fn table_schema() -> SchemaRef {
    let string = || Type::Primitive(PrimitiveType::String);
    let nested = StructType::new(vec![NestedField::optional(7, "inner", string()).into()]);
    Arc::new(
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "name", string()).into(),
                NestedField::optional(3, "added", string()).into(),
                NestedField::optional(4, "defaulted", string())
                    .with_initial_default(Literal::string("d"))
                    .into(),
                NestedField::optional(5, "region", string()).into(),
                NestedField::optional(6, "nested", Type::Struct(nested)).into(),
            ])
            .build()
            .unwrap(),
    )
}

fn field(name: &str, data_type: DataType, nullable: bool, id: i32) -> Field {
    Field::new(name, data_type, nullable).with_metadata(HashMap::from([(
        PARQUET_FIELD_ID_META_KEY.to_string(),
        id.to_string(),
    )]))
}

/// Five distinct values, with a null every seventh row.
fn names() -> Vec<Option<String>> {
    (0..300)
        .map(|i| (i % 7 != 0).then(|| format!("name-{}", i % 5)))
        .collect()
}

fn write_batch(path: &str, batch: &RecordBatch, props: WriterProperties) {
    let mut writer =
        ArrowWriter::try_new(File::create(path).unwrap(), batch.schema(), Some(props)).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
}

/// Writes ids 0..300 and `names()`, with the string column dictionary-encoded or plain.
fn write_data_file(dir: &TempDir, file: &str, dictionary: bool) -> String {
    let schema = Arc::new(ArrowSchema::new(vec![
        field("id", DataType::Int32, false, 1),
        field("name", DataType::Utf8, true, 2),
    ]));
    let batch = RecordBatch::try_new(schema, vec![
        Arc::new(Int32Array::from_iter_values(0..300)) as ArrayRef,
        Arc::new(StringArray::from(names())),
    ])
    .unwrap();
    let path = format!("{}/{file}", dir.path().to_str().unwrap());
    let props = WriterProperties::builder()
        .set_dictionary_enabled(dictionary)
        .set_max_row_group_row_count(Some(100))
        .build();
    write_batch(&path, &batch, props);
    path
}

fn task(path: &str, project: Vec<i32>, predicate: Option<Predicate>) -> FileScanTask {
    let schema = table_schema();
    FileScanTask::builder()
        .with_file_size_in_bytes(std::fs::metadata(path).unwrap().len())
        .with_start(0)
        .with_length(0)
        .with_data_file_path(path.to_string())
        .with_data_file_format(DataFileFormat::Parquet)
        .with_schema(schema.clone())
        .with_project_field_ids(project)
        .with_predicate(predicate.map(|p| p.bind(schema, true).unwrap()))
        .with_case_sensitive(true)
        .build()
        .unwrap()
}

async fn read_batches(
    options: ArrowReaderOptions,
    tasks: Vec<FileScanTask>,
) -> Result<Vec<RecordBatch>> {
    let tasks = Box::pin(futures::stream::iter(tasks.into_iter().map(Ok))) as FileScanTaskStream;
    ArrowReaderBuilder::new(FileIO::new_with_fs(), test_runtime())
        .with_options(options)
        .build()
        .read(tasks)?
        .stream()
        .try_collect()
        .await
}

fn options(dictionary_field_ids: &[i32]) -> ArrowReaderOptions {
    ArrowReaderOptions::builder()
        .with_data_file_concurrency_limit(1)
        .with_dictionary_field_ids(dictionary_field_ids.iter().copied())
        .build()
}

/// All rows read, or `None` when the reader yields no batch.
async fn read(dictionary_field_ids: &[i32], tasks: Vec<FileScanTask>) -> Option<RecordBatch> {
    let batches = read_batches(options(dictionary_field_ids), tasks)
        .await
        .unwrap();
    let first = batches.first()?;
    Some(concat_batches(&first.schema(), &batches).unwrap())
}

/// The batch's columns with dictionaries cast back to `Utf8`.
fn as_utf8(batch: &RecordBatch) -> Vec<ArrayRef> {
    batch
        .columns()
        .iter()
        .map(|c| match c.data_type() {
            DataType::Dictionary(_, _) => cast(c, &DataType::Utf8).unwrap(),
            _ => Arc::clone(c),
        })
        .collect()
}

fn column_types(batch: &RecordBatch) -> Vec<DataType> {
    batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.data_type().clone())
        .collect()
}

/// A dictionary-encoded file and a plain-encoded file, projecting `name` and the two
/// absent string fields.
fn mixed_encoding_tasks(dir: &TempDir) -> Vec<FileScanTask> {
    [
        write_data_file(dir, "dict.parquet", true),
        write_data_file(dir, "plain.parquet", false),
    ]
    .iter()
    .map(|f| task(f, vec![1, 2, 3, 4], None))
    .collect()
}

/// The innermost iceberg error, below the reader's per-task wrapping.
fn innermost(err: &Error) -> &Error {
    let mut current = err;
    while let Some(next) =
        std::error::Error::source(current).and_then(|source| source.downcast_ref::<Error>())
    {
        current = next;
    }
    current
}

async fn read_error(dictionary_field_ids: &[i32]) -> (ErrorKind, String) {
    let dir = TempDir::new().unwrap();
    let path = write_data_file(&dir, "data.parquet", true);
    let err = read_batches(options(dictionary_field_ids), vec![task(
        &path,
        vec![1, 2],
        None,
    )])
    .await
    .unwrap_err();
    let err = innermost(&err);
    (err.kind(), err.message().to_string())
}

#[tokio::test]
async fn dictionary_type_is_the_same_in_every_batch() {
    let dir = TempDir::new().unwrap();
    let batches = read_batches(options(&[2, 3, 4]), mixed_encoding_tasks(&dir))
        .await
        .unwrap();

    let types: HashSet<Vec<DataType>> = batches.iter().map(column_types).collect();
    assert_eq!(
        types,
        HashSet::from([vec![
            DataType::Int32,
            dictionary_string_type(),
            dictionary_string_type(),
            dictionary_string_type(),
        ]])
    );
}

#[tokio::test]
async fn dictionary_values_match_a_utf8_read() {
    let dir = TempDir::new().unwrap();

    let utf8 = read(&[], mixed_encoding_tasks(&dir)).await.unwrap();
    let dictionary = read(&[2, 3, 4], mixed_encoding_tasks(&dir)).await.unwrap();

    assert_eq!(as_utf8(&dictionary), utf8.columns().to_vec());
}

#[tokio::test]
async fn dictionary_unlisted_string_fields_stay_utf8() {
    let dir = TempDir::new().unwrap();
    let path = write_data_file(&dir, "dict.parquet", true);

    let batch = read(&[3], vec![task(&path, vec![1, 2, 3], None)])
        .await
        .unwrap();

    assert_eq!(column_types(&batch), vec![
        DataType::Int32,
        DataType::Utf8,
        dictionary_string_type()
    ]);
}

#[tokio::test]
async fn dictionary_field_that_is_not_projected_has_no_effect() {
    let dir = TempDir::new().unwrap();
    let path = write_data_file(&dir, "dict.parquet", true);

    let batch = read(&[2], vec![task(&path, vec![1], None)]).await.unwrap();

    assert_eq!(column_types(&batch), vec![DataType::Int32]);
}

#[tokio::test]
async fn dictionary_encoded_chunks_keep_their_dictionaries() {
    let dir = TempDir::new().unwrap();
    let path = write_data_file(&dir, "dict.parquet", true);
    let options = ArrowReaderOptions::builder()
        .with_batch_size(2)
        .with_dictionary_field_ids([2])
        .build();

    let batches = read_batches(options, vec![task(&path, vec![2], None)])
        .await
        .unwrap();

    let dictionary_sizes: HashSet<usize> = batches
        .iter()
        .map(|b| b.column(0).as_dictionary::<Int32Type>().values().len())
        .collect();
    assert_eq!(dictionary_sizes, HashSet::from([5]));
}

#[tokio::test]
async fn dictionary_identity_partition_constant() {
    let dir = TempDir::new().unwrap();
    let path = write_data_file(&dir, "data.parquet", true);
    let schema = table_schema();
    let spec = Arc::new(
        PartitionSpec::builder(schema.clone())
            .with_spec_id(1)
            .add_partition_field("region", "region", Transform::Identity)
            .unwrap()
            .build()
            .unwrap(),
    );
    let partitioned = || {
        FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(&path).unwrap().len())
            .with_start(0)
            .with_length(0)
            .with_data_file_path(path.clone())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema.clone())
            .with_project_field_ids(vec![1, 5])
            .with_partition_spec(Some(spec.clone()))
            .with_partition(Some(Struct::from_iter([Some(Literal::string("eu"))])))
            .with_case_sensitive(true)
            .build()
            .unwrap()
    };

    let batch = read(&[5], vec![partitioned()]).await.unwrap();

    let expected = DictionaryArray::<Int32Type>::new(
        Int32Array::from(vec![0; 300]),
        Arc::new(StringArray::from(vec!["eu"])),
    );
    assert_eq!(batch.column(1).as_dictionary::<Int32Type>(), &expected);
}

/// The writer stores `Dictionary(Int8, Utf8)` in the embedded Arrow schema, but the
/// column chunk's dictionary has more entries than `Int8` keys can address.
#[tokio::test]
async fn dictionary_embedded_arrow_dictionary_type_is_normalized() {
    let dir = TempDir::new().unwrap();
    let path = format!("{}/int8.parquet", dir.path().to_str().unwrap());
    let int8_dictionary = DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8));
    let schema = Arc::new(ArrowSchema::new(vec![field(
        "name",
        int8_dictionary,
        true,
        2,
    )]));
    let values: Vec<String> = (0..200).map(|i| format!("value-{i}")).collect();
    let mut writer =
        ArrowWriter::try_new(File::create(&path).unwrap(), schema.clone(), None).unwrap();
    for chunk in values.chunks(100) {
        let column: DictionaryArray<Int8Type> = chunk.iter().map(String::as_str).collect();
        writer
            .write(&RecordBatch::try_new(schema.clone(), vec![Arc::new(column)]).unwrap())
            .unwrap();
    }
    writer.close().unwrap();

    let batch = read(&[2], vec![task(&path, vec![2], None)]).await.unwrap();

    assert_eq!(column_types(&batch), vec![dictionary_string_type()]);
    assert_eq!(as_utf8(&batch), vec![
        Arc::new(StringArray::from(values)) as ArrayRef
    ]);
}

#[tokio::test]
async fn dictionary_unknown_field_id_is_rejected() {
    let (kind, message) = read_error(&[99]).await;
    assert_eq!(kind, ErrorKind::DataInvalid);
    assert!(message.contains("field id 99"), "{message}");
}

#[tokio::test]
async fn dictionary_non_string_field_id_is_rejected() {
    let (kind, message) = read_error(&[1]).await;
    assert_eq!(kind, ErrorKind::FeatureUnsupported);
    assert!(message.contains("id 1,"), "{message}");
    assert!(message.contains("only string fields"), "{message}");
}

#[tokio::test]
async fn dictionary_nested_field_id_is_rejected() {
    let (kind, message) = read_error(&[7]).await;
    assert_eq!(kind, ErrorKind::FeatureUnsupported);
    assert!(message.contains("id 7,"), "{message}");
    assert!(message.contains("only top-level fields"), "{message}");
}

#[rstest]
#[case::eq(Reference::new("name").equal_to(Datum::string("name-3")))]
#[case::not_eq(Reference::new("name").not_equal_to(Datum::string("name-3")))]
#[case::is_in(Reference::new("name").is_in([Datum::string("name-0"), Datum::string("name-4")]))]
#[case::not_in(Reference::new("name").is_not_in([Datum::string("name-0"), Datum::string("name-4")]))]
#[case::starts_with(Reference::new("name").starts_with(Datum::string("name-1")))]
#[case::is_null(Reference::new("name").is_null())]
#[case::not_null(Reference::new("name").is_not_null())]
#[tokio::test]
async fn dictionary_predicates_select_the_same_rows(#[case] predicate: Predicate) {
    let dir = TempDir::new().unwrap();
    let files = [
        write_data_file(&dir, "dict.parquet", true),
        write_data_file(&dir, "plain.parquet", false),
    ];
    let tasks = || {
        files
            .iter()
            .map(|f| task(f, vec![1, 2], Some(predicate.clone())))
            .collect::<Vec<_>>()
    };

    let utf8 = read(&[], tasks()).await.map(|b| b.columns().to_vec());
    let dictionary = read(&[2], tasks()).await.map(|b| as_utf8(&b));

    assert_eq!(dictionary, utf8);
}

#[tokio::test]
async fn dictionary_predicate_with_page_index_and_bloom_filters_selects_the_same_rows() {
    let dir = TempDir::new().unwrap();
    let path = write_data_file(&dir, "dict.parquet", true);
    let predicate = Reference::new("name").equal_to(Datum::string("name-3"));
    let read_with = |dictionary_field_ids: &'static [i32]| {
        let options = ArrowReaderOptions::builder()
            .with_row_selection_enabled(true)
            .with_bloom_filter_enabled(true)
            .with_dictionary_field_ids(dictionary_field_ids.iter().copied())
            .build();
        read_batches(options, vec![task(
            &path,
            vec![1, 2],
            Some(predicate.clone()),
        )])
    };

    let utf8 = read_with(&[]).await.unwrap();
    let dictionary = read_with(&[2]).await.unwrap();

    let utf8 = concat_batches(&utf8[0].schema(), &utf8).unwrap();
    let dictionary = concat_batches(&dictionary[0].schema(), &dictionary).unwrap();
    assert_eq!(as_utf8(&dictionary), utf8.columns().to_vec());
}

#[tokio::test]
async fn dictionary_equality_deletes_on_a_string_column() {
    let dir = TempDir::new().unwrap();
    let path = write_data_file(&dir, "data.parquet", true);
    let delete_schema = Arc::new(ArrowSchema::new(vec![field(
        "name",
        DataType::Utf8,
        true,
        2,
    )]));
    let deletes = RecordBatch::try_new(delete_schema, vec![Arc::new(StringArray::from(vec![
        "name-1", "name-4",
    ])) as ArrayRef])
    .unwrap();
    let delete_path = format!("{}/eq-del.parquet", dir.path().to_str().unwrap());
    write_batch(&delete_path, &deletes, WriterProperties::builder().build());
    let delete = FileScanTaskDeleteFile::builder()
        .with_file_path(delete_path.clone())
        .with_file_size_in_bytes(std::fs::metadata(&delete_path).unwrap().len())
        .with_file_type(DataContentType::EqualityDeletes)
        .with_file_format(DataFileFormat::Parquet)
        .with_partition_spec_id(0)
        .with_equality_ids(Some(vec![2]))
        .build()
        .unwrap();
    let schema = table_schema();
    let with_delete = FileScanTask::builder()
        .with_file_size_in_bytes(std::fs::metadata(&path).unwrap().len())
        .with_start(0)
        .with_length(0)
        .with_data_file_path(path.clone())
        .with_data_file_format(DataFileFormat::Parquet)
        .with_schema(schema)
        .with_project_field_ids(vec![2])
        .with_deletes(vec![delete])
        .with_case_sensitive(true)
        .build()
        .unwrap();

    let batch = read(&[2], vec![with_delete]).await.unwrap();

    let expected: Vec<Option<String>> = names()
        .into_iter()
        .filter(|n| !matches!(n.as_deref(), Some("name-1" | "name-4")))
        .collect();
    assert_eq!(as_utf8(&batch), vec![
        Arc::new(StringArray::from(expected)) as ArrayRef
    ]);
}
