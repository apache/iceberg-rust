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
use std::fs::File;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::{
    ArrayRef, Decimal128Array, Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray,
};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use futures::{StreamExt, TryStreamExt};
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use tempfile::TempDir;

use super::{ArrowReaderBuilder, RuntimePredicateProvider, RuntimePredicateSnapshot};
use crate::arrow::ScanMetrics;
use crate::expr::{Bind, Predicate, Reference};
use crate::io::FileIO;
use crate::scan::{
    ArrowRecordBatchStream, FileScanTask, FileScanTaskDeleteFile, FileScanTaskStream,
};
use crate::spec::{
    DataContentType, DataFileFormat, Datum, NestedField, PrimitiveType, Schema, SchemaRef, Type,
};
use crate::{Result, Runtime};

#[derive(Debug)]
struct FixedRuntimePredicate {
    predicate: Predicate,
    generation: u64,
    snapshots: AtomicU64,
}

impl FixedRuntimePredicate {
    fn new(predicate: Predicate) -> Self {
        Self {
            predicate,
            generation: 1,
            snapshots: AtomicU64::new(0),
        }
    }

    fn snapshots(&self) -> u64 {
        self.snapshots.load(Ordering::Relaxed)
    }
}

impl RuntimePredicateProvider for FixedRuntimePredicate {
    fn generation(&self) -> u64 {
        self.generation
    }

    fn snapshot(&self) -> Result<RuntimePredicateSnapshot> {
        self.snapshots.fetch_add(1, Ordering::Relaxed);
        Ok(RuntimePredicateSnapshot::new(
            Some(self.predicate.clone()),
            self.generation,
        ))
    }
}

fn iceberg_schema() -> SchemaRef {
    Arc::new(
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "payload", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap(),
    )
}

fn write_three_row_group_file(dir: &str, name: &str) -> String {
    write_row_group_file(dir, name, &[0, 100, 200])
}

fn write_row_group_file(dir: &str, name: &str, bases: &[i32]) -> String {
    write_row_group_file_with_page_size(dir, name, bases, 1024)
}

fn write_row_group_file_with_page_size(
    dir: &str,
    name: &str,
    bases: &[i32],
    page_rows: usize,
) -> String {
    let id_field = Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
        PARQUET_FIELD_ID_META_KEY.to_string(),
        "1".to_string(),
    )]));
    let payload_field = Field::new("payload", DataType::Utf8, false).with_metadata(HashMap::from(
        [(PARQUET_FIELD_ID_META_KEY.to_string(), "2".to_string())],
    ));
    let arrow_schema = Arc::new(ArrowSchema::new(vec![id_field, payload_field]));

    let file_path = format!("{dir}/{name}");
    let file = File::create(&file_path).unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_max_row_group_row_count(Some(4))
        .set_data_page_row_count_limit(page_rows)
        .set_write_batch_size(page_rows)
        .set_dictionary_enabled(false)
        .build();
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&arrow_schema), Some(props)).unwrap();

    for &base in bases {
        let ids: Vec<i32> = (base..base + 4).collect();
        let payloads: Vec<String> = (0..4)
            .map(|row| {
                (0..16_384)
                    .map(|offset| {
                        let value = ((base as usize) + row + offset * 17) % 94;
                        char::from_u32(33 + value as u32).unwrap()
                    })
                    .collect()
            })
            .collect();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(payloads)),
        ];
        let batch = RecordBatch::try_new(Arc::clone(&arrow_schema), columns).unwrap();
        writer.write(&batch).unwrap();
        writer.flush().unwrap();
    }
    writer.close().unwrap();
    file_path
}

fn scan_task(
    file_path: String,
    schema: SchemaRef,
    predicate: Option<crate::expr::BoundPredicate>,
) -> FileScanTask {
    scan_task_with_deletes(file_path, schema, predicate, vec![])
}

fn scan_task_with_deletes(
    file_path: String,
    schema: SchemaRef,
    predicate: Option<crate::expr::BoundPredicate>,
    deletes: Vec<FileScanTaskDeleteFile>,
) -> FileScanTask {
    scan_task_with_deletes_and_projection(file_path, schema, predicate, deletes, vec![1, 2])
}

fn scan_task_with_deletes_and_projection(
    file_path: String,
    schema: SchemaRef,
    predicate: Option<crate::expr::BoundPredicate>,
    deletes: Vec<FileScanTaskDeleteFile>,
    project_field_ids: Vec<i32>,
) -> FileScanTask {
    FileScanTask::builder()
        .with_file_size_in_bytes(std::fs::metadata(&file_path).unwrap().len())
        .with_start(0)
        .with_length(0)
        .with_data_file_path(file_path)
        .with_data_file_format(DataFileFormat::Parquet)
        .with_schema(schema)
        .with_project_field_ids(project_field_ids)
        .with_predicate(predicate)
        .with_deletes(deletes)
        .with_case_sensitive(false)
        .build()
        .unwrap()
}

fn ids(batches: &[RecordBatch]) -> Vec<i32> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        })
        .collect()
}

fn write_delete(path: &str, fields: Vec<Field>, arrays: Vec<ArrayRef>) {
    let schema = Arc::new(ArrowSchema::new(fields));
    let batch = RecordBatch::try_new(Arc::clone(&schema), arrays).unwrap();
    let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn field(name: &str, data_type: DataType, id: i32) -> Field {
    Field::new(name, data_type, false).with_metadata(HashMap::from([(
        PARQUET_FIELD_ID_META_KEY.to_string(),
        id.to_string(),
    )]))
}

/// Writes one four-row group per base with small payloads.
fn write_groups(path: &str, bases: &[i32], field_ids: bool, key: Option<&[u8]>, bloom: bool) {
    let metadata = |id: &str| {
        if field_ids {
            HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string())])
        } else {
            HashMap::new()
        }
    };
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false).with_metadata(metadata("1")),
        Field::new("payload", DataType::Utf8, false).with_metadata(metadata("2")),
    ]));
    let mut properties = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_max_row_group_row_count(Some(4))
        .set_bloom_filter_enabled(bloom);
    if let Some(key) = key {
        properties = properties.with_file_encryption_properties(
            parquet::encryption::encrypt::FileEncryptionProperties::builder(key.to_vec())
                .build()
                .unwrap(),
        );
    }
    let mut writer = ArrowWriter::try_new(
        File::create(path).unwrap(),
        Arc::clone(&schema),
        Some(properties.build()),
    )
    .unwrap();
    for &base in bases {
        let ids: Vec<i32> = (base..base + 4).collect();
        let payloads: Vec<String> = ids.iter().map(|id| format!("payload-{id}")).collect();
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(payloads)),
        ])
        .unwrap();
        writer.write(&batch).unwrap();
        writer.flush().unwrap();
    }
    writer.close().unwrap();
}

#[derive(Default)]
struct FailedProvider {
    snapshots: AtomicU64,
}

impl RuntimePredicateProvider for FailedProvider {
    fn generation(&self) -> u64 {
        0
    }

    fn snapshot(&self) -> Result<RuntimePredicateSnapshot> {
        self.snapshots.fetch_add(1, Ordering::Relaxed);
        Err(crate::Error::new(
            crate::ErrorKind::Unexpected,
            "publication failed",
        ))
    }
}

async fn execute_tasks(
    tasks: Vec<FileScanTask>,
    provider: Option<Arc<dyn RuntimePredicateProvider>>,
    bloom_filter: bool,
) -> (Vec<RecordBatch>, ScanMetrics) {
    execute_tasks_with_concurrency(tasks, provider, bloom_filter, 1).await
}

async fn execute_tasks_with_concurrency(
    tasks: Vec<FileScanTask>,
    provider: Option<Arc<dyn RuntimePredicateProvider>>,
    bloom_filter: bool,
    concurrency: usize,
) -> (Vec<RecordBatch>, ScanMetrics) {
    let mut builder = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current())
        .with_data_file_concurrency_limit(concurrency)
        .with_row_selection_enabled(true)
        .with_bloom_filter_enabled(bloom_filter);
    if let Some(provider) = provider {
        builder = builder.with_runtime_predicate_provider(provider);
    }
    let tasks = Box::pin(futures::stream::iter(tasks.into_iter().map(Ok))) as FileScanTaskStream;
    let scan = builder.build().read(tasks).unwrap();
    let metrics = scan.metrics().clone();
    let batches = scan.stream().try_collect().await.unwrap();
    (batches, metrics)
}

async fn execute(
    task: FileScanTask,
    provider: Option<Arc<dyn RuntimePredicateProvider>>,
) -> (Vec<RecordBatch>, ScanMetrics) {
    execute_tasks(vec![task], provider, false).await
}

fn all_ids() -> Vec<i32> {
    [0, 100, 200]
        .into_iter()
        .flat_map(|base| base..base + 4)
        .collect()
}

#[tokio::test]
async fn runtime_predicate_prunes_row_groups_and_bytes() {
    let temp = TempDir::new().unwrap();
    let path = write_three_row_group_file(temp.path().to_str().unwrap(), "prune.parquet");
    let task = scan_task(path, iceberg_schema(), None);
    let (_, baseline) = execute(task.clone(), None).await;

    let provider = Arc::new(FixedRuntimePredicate::new(
        Reference::new("id")
            .greater_than_or_equal_to(Datum::int(100))
            .and(Reference::new("id").less_than_or_equal_to(Datum::int(103))),
    ));
    let (batches, metrics) = execute(task, Some(provider.clone())).await;
    assert_eq!(ids(&batches), vec![100, 101, 102, 103]);
    assert_eq!(provider.snapshots(), 1);
    assert!(
        metrics.bytes_read() < baseline.bytes_read(),
        "runtime={} baseline={}",
        metrics.bytes_read(),
        baseline.bytes_read()
    );
}

#[tokio::test]
async fn runtime_predicate_is_anded_with_task_predicate() {
    let temp = TempDir::new().unwrap();
    let path = write_three_row_group_file(temp.path().to_str().unwrap(), "and.parquet");
    let planned = Reference::new("id")
        .greater_than_or_equal_to(Datum::int(100))
        .bind(iceberg_schema(), false)
        .unwrap();
    let provider = Arc::new(FixedRuntimePredicate::new(
        Reference::new("id").less_than_or_equal_to(Datum::int(103)),
    ));
    let (batches, _) = execute(
        scan_task(path, iceberg_schema(), Some(planned)),
        Some(provider),
    )
    .await;
    assert_eq!(ids(&batches), vec![100, 101, 102, 103]);
}

#[tokio::test]
async fn runtime_predicate_failures_keep_the_planned_filter() {
    let temp = TempDir::new().unwrap();
    let path = write_three_row_group_file(temp.path().to_str().unwrap(), "fail.parquet");
    let planned = || {
        Some(
            Reference::new("id")
                .equal_to(Datum::int(101))
                .bind(iceberg_schema(), false)
                .unwrap(),
        )
    };
    // A failing provider, and a predicate that cannot be bound.
    for provider in [
        Arc::new(FailedProvider::default()) as Arc<dyn RuntimePredicateProvider>,
        Arc::new(FixedRuntimePredicate::new(
            Reference::new("missing").equal_to(Datum::int(1)),
        )),
    ] {
        let (batches, _) = execute(
            scan_task(path.clone(), iceberg_schema(), planned()),
            Some(provider),
        )
        .await;
        assert_eq!(ids(&batches), vec![101]);
        let (batches, _) = execute(scan_task(path.clone(), iceberg_schema(), None), None).await;
        assert_eq!(ids(&batches), all_ids());
    }
}

#[tokio::test]
async fn runtime_not_predicates_keep_matching_rows() {
    let temp = TempDir::new().unwrap();
    let path = write_three_row_group_file(temp.path().to_str().unwrap(), "not.parquet");
    // RG1 holds 100..=103, so NOT(id < 102) must keep 102 and 103.
    let provider = Arc::new(FixedRuntimePredicate::new(
        !Reference::new("id").less_than(Datum::int(102)),
    ));
    let (batches, _) = execute(scan_task(path, iceberg_schema(), None), Some(provider)).await;
    assert_eq!(ids(&batches), vec![102, 103, 200, 201, 202, 203]);
}

#[tokio::test]
async fn runtime_predicate_on_column_missing_from_file_is_ignored() {
    use crate::spec::Literal;

    let temp = TempDir::new().unwrap();
    let path = write_three_row_group_file(temp.path().to_str().unwrap(), "evolved.parquet");
    // `added` was added after this file was written. Its value in this file is
    // the initial default (7) or null, which physical filters never see.
    for default in [Some(Literal::int(7)), None] {
        let mut added = NestedField::optional(3, "added", Type::Primitive(PrimitiveType::Int));
        if let Some(default) = default.clone() {
            added = added.with_initial_default(default);
        }
        let schema: SchemaRef = Arc::new(
            Schema::builder()
                .with_schema_id(2)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "payload", Type::Primitive(PrimitiveType::String))
                        .into(),
                    added.into(),
                ])
                .build()
                .unwrap(),
        );
        for predicate in [
            Reference::new("added").is_null(),
            Reference::new("added").greater_than_or_equal_to(Datum::int(5)),
            Reference::new("added").less_than(Datum::int(5)),
        ] {
            let provider = Arc::new(FixedRuntimePredicate::new(predicate.clone()));
            let (batches, _) = execute(
                scan_task(path.clone(), Arc::clone(&schema), None),
                Some(provider),
            )
            .await;
            assert_eq!(
                ids(&batches),
                all_ids(),
                "{predicate} with default {default:?}"
            );
        }
    }
}

#[tokio::test]
async fn runtime_predicate_on_promoted_column_is_ignored() {
    let temp = TempDir::new().unwrap();
    // The file stores `id` as INT; the table has since promoted it to BIGINT.
    let path = write_three_row_group_file(temp.path().to_str().unwrap(), "promoted.parquet");
    let schema: SchemaRef = Arc::new(
        Schema::builder()
            .with_schema_id(2)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "payload", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap(),
    );
    // Above i32::MAX: a literal cast down to the physical INT type would overflow.
    let provider = Arc::new(FixedRuntimePredicate::new(
        Reference::new("id").less_than(Datum::long(i64::from(i32::MAX) + 1)),
    ));
    let (batches, _) = execute(scan_task(path, schema, None), Some(provider)).await;
    let values: Vec<i64> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(
        values,
        all_ids().into_iter().map(i64::from).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn runtime_predicate_preserves_position_and_equality_deletes() {
    const FIELD_ID_POSITIONAL_DELETE_FILE_PATH: i32 = 2_147_483_546;
    const FIELD_ID_POSITIONAL_DELETE_POS: i32 = 2_147_483_545;

    let temp = TempDir::new().unwrap();
    let dir = temp.path().to_str().unwrap();
    let data_path = write_three_row_group_file(dir, "data.parquet");
    let position_path = format!("{dir}/positions.parquet");
    let equality_path = format!("{dir}/equalities.parquet");
    // Position 5 is id=101 in the second row group. If positions were
    // renumbered after the first group is pruned, the wrong row would go.
    write_delete(
        &position_path,
        vec![
            field(
                "file_path",
                DataType::Utf8,
                FIELD_ID_POSITIONAL_DELETE_FILE_PATH,
            ),
            field("pos", DataType::Int64, FIELD_ID_POSITIONAL_DELETE_POS),
        ],
        vec![
            Arc::new(StringArray::from(vec![data_path.as_str()])),
            Arc::new(Int64Array::from(vec![5])),
        ],
    );
    write_delete(&equality_path, vec![field("id", DataType::Int32, 1)], vec![
        Arc::new(Int32Array::from(vec![102])),
    ]);
    let delete = |path: String, file_type, equality_ids| {
        FileScanTaskDeleteFile::builder()
            .with_file_size_in_bytes(std::fs::metadata(&path).unwrap().len())
            .with_file_path(path)
            .with_file_type(file_type)
            .with_file_format(DataFileFormat::Parquet)
            .with_partition_spec_id(0)
            .with_equality_ids(equality_ids)
            .build()
    };
    let position = delete(position_path, DataContentType::PositionDeletes, None);
    let equality = delete(
        equality_path,
        DataContentType::EqualityDeletes,
        Some(vec![1]),
    );
    for (deletes, expected) in [
        (vec![position.clone()], vec![100, 102, 103]),
        (vec![equality.clone()], vec![100, 101, 103]),
        (vec![position, equality], vec![100, 103]),
    ] {
        let task = scan_task_with_deletes(data_path.clone(), iceberg_schema(), None, deletes);
        let (baseline, baseline_metrics) = execute(task.clone(), None).await;
        let provider = Arc::new(FixedRuntimePredicate::new(
            Reference::new("id")
                .greater_than_or_equal_to(Datum::int(100))
                .and(Reference::new("id").less_than_or_equal_to(Datum::int(103))),
        ));
        let (runtime, metrics) = execute(task, Some(provider)).await;
        assert_eq!(ids(&runtime), expected);
        assert_eq!(
            ids(&baseline)
                .into_iter()
                .filter(|id| (100..=103).contains(id))
                .collect::<Vec<_>>(),
            expected
        );
        assert!(metrics.bytes_read() < baseline_metrics.bytes_read());
    }
}

#[tokio::test]
async fn runtime_predicate_respects_task_byte_range() {
    use parquet::file::reader::{FileReader, SerializedFileReader};

    let temp = TempDir::new().unwrap();
    let path = write_three_row_group_file(temp.path().to_str().unwrap(), "split.parquet");
    let parquet = SerializedFileReader::new(File::open(&path).unwrap()).unwrap();
    let start = 4 + parquet.metadata().row_group(0).compressed_size() as u64;
    let file_size = std::fs::metadata(&path).unwrap().len();
    let task = FileScanTask::builder()
        .with_file_size_in_bytes(file_size)
        .with_start(start)
        .with_length(file_size - start)
        .with_data_file_path(path)
        .with_data_file_format(DataFileFormat::Parquet)
        .with_schema(iceberg_schema())
        .with_project_field_ids(vec![1, 2])
        .with_case_sensitive(false)
        .build()
        .unwrap();
    // RG0 belongs to another split, so even a predicate matching it reads nothing there.
    let provider = Arc::new(FixedRuntimePredicate::new(
        Reference::new("id")
            .less_than(Datum::int(4))
            .or(Reference::new("id").greater_than_or_equal_to(Datum::int(200))),
    ));
    let (batches, _) = execute(task, Some(provider)).await;
    assert_eq!(ids(&batches), vec![200, 201, 202, 203]);
}

#[tokio::test]
async fn runtime_predicate_with_bloom_filters_keeps_planned_equality() {
    let temp = TempDir::new().unwrap();
    let path = format!("{}/bloom.parquet", temp.path().to_str().unwrap());
    write_groups(&path, &[0, 100, 200], true, None, true);
    let planned = Reference::new("id")
        .equal_to(Datum::int(201))
        .bind(iceberg_schema(), false)
        .unwrap();
    let provider = Arc::new(FixedRuntimePredicate::new(
        Reference::new("id").greater_than_or_equal_to(Datum::int(100)),
    ));
    let (batches, _) = execute_tasks(
        vec![scan_task(path, iceberg_schema(), Some(planned))],
        Some(provider),
        true,
    )
    .await;
    assert_eq!(ids(&batches), vec![201]);
}

/// A provider whose predicate changes when [`Self::publish`] is called.
struct ChangingRuntimePredicate {
    generation: AtomicU64,
    predicate: std::sync::Mutex<Option<Predicate>>,
    snapshots: AtomicU64,
}

impl ChangingRuntimePredicate {
    fn new(predicate: Option<Predicate>, generation: u64) -> Self {
        Self {
            generation: AtomicU64::new(generation),
            predicate: std::sync::Mutex::new(predicate),
            snapshots: AtomicU64::new(0),
        }
    }

    fn publish(&self, predicate: Option<Predicate>, generation: u64) {
        *self.predicate.lock().unwrap() = predicate;
        self.generation.store(generation, Ordering::Release);
    }

    fn snapshots(&self) -> u64 {
        self.snapshots.load(Ordering::Relaxed)
    }
}

impl RuntimePredicateProvider for ChangingRuntimePredicate {
    fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    fn snapshot(&self) -> Result<RuntimePredicateSnapshot> {
        self.snapshots.fetch_add(1, Ordering::Relaxed);
        let predicate = self.predicate.lock().unwrap();
        Ok(RuntimePredicateSnapshot::new(
            predicate.clone(),
            self.generation.load(Ordering::Acquire),
        ))
    }
}

#[tokio::test]
async fn runtime_predicate_is_bound_once_per_generation_across_tasks() {
    let temp = TempDir::new().unwrap();
    let dir = temp.path().to_str().unwrap();
    let tasks: Vec<_> = ["a.parquet", "b.parquet", "c.parquet"]
        .into_iter()
        .map(|name| {
            scan_task(
                write_three_row_group_file(dir, name),
                iceberg_schema(),
                None,
            )
        })
        .collect();
    let provider = Arc::new(ChangingRuntimePredicate::new(
        Some(Reference::new("id").greater_than_or_equal_to(Datum::int(100))),
        1,
    ));
    let scan = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current())
        .with_data_file_concurrency_limit(1)
        .with_runtime_predicate_provider(provider.clone())
        .build()
        .read(Box::pin(futures::stream::iter(tasks.into_iter().map(Ok))) as FileScanTaskStream)
        .unwrap();
    let mut stream = scan.stream();
    // The first task reads RG1 and RG2 under generation 1.
    let mut batches = vec![
        stream.try_next().await.unwrap().unwrap(),
        stream.try_next().await.unwrap().unwrap(),
    ];
    assert_eq!(ids(&batches), vec![100, 101, 102, 103, 200, 201, 202, 203]);
    assert_eq!(provider.snapshots(), 1);
    // A tighter generation is picked up by the tasks that start afterwards and
    // bound once for both of them.
    provider.publish(
        Some(Reference::new("id").greater_than_or_equal_to(Datum::int(200))),
        2,
    );
    batches.extend(stream.try_collect::<Vec<_>>().await.unwrap());
    let mut expected = vec![100, 101, 102, 103, 200, 201, 202, 203];
    for _ in 0..2 {
        expected.extend([200, 201, 202, 203]);
    }
    assert_eq!(ids(&batches), expected);
    assert_eq!(provider.snapshots(), 2);
}

#[tokio::test]
async fn runtime_predicate_on_a_column_that_is_not_projected() {
    let temp = TempDir::new().unwrap();
    let path = write_three_row_group_file(temp.path().to_str().unwrap(), "filter-only.parquet");
    // Only `payload` is projected; the runtime predicate filters on `id`.
    let task = scan_task_with_deletes_and_projection(path, iceberg_schema(), None, vec![], vec![2]);
    let (baseline, baseline_metrics) = execute(task.clone(), None).await;
    let provider = Arc::new(FixedRuntimePredicate::new(
        Reference::new("id").greater_than_or_equal_to(Datum::int(200)),
    ));
    let (batches, metrics) = execute(task, Some(provider)).await;
    let rows = |batches: &[RecordBatch]| batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    assert_eq!(rows(&baseline), 12);
    assert_eq!(rows(&batches), 4);
    assert!(batches.iter().all(|batch| batch.num_columns() == 1));
    assert!(metrics.bytes_read() < baseline_metrics.bytes_read());
}

#[tokio::test]
async fn runtime_predicate_resolves_files_without_field_ids() {
    use crate::spec::{MappedField, NameMapping};

    let temp = TempDir::new().unwrap();
    let path = format!("{}/no-ids.parquet", temp.path().to_str().unwrap());
    write_groups(&path, &[0, 100, 200], false, None, false);
    let mapping = Arc::new(NameMapping::new(vec![
        MappedField::new(Some(1), vec!["id".to_string()], vec![]),
        MappedField::new(Some(2), vec!["payload".to_string()], vec![]),
    ]));
    // A name mapping assigns ids by name; without one, ids follow column positions.
    for name_mapping in [Some(mapping), None] {
        let task = FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(&path).unwrap().len())
            .with_start(0)
            .with_length(0)
            .with_data_file_path(path.clone())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(iceberg_schema())
            .with_project_field_ids(vec![1, 2])
            .with_name_mapping(name_mapping)
            .with_case_sensitive(false)
            .build()
            .unwrap();
        let provider = Arc::new(FixedRuntimePredicate::new(
            Reference::new("id").greater_than_or_equal_to(Datum::int(200)),
        ));
        let (batches, _) = execute(task, Some(provider)).await;
        assert_eq!(ids(&batches), vec![200, 201, 202, 203]);
    }
}

#[tokio::test]
async fn runtime_predicate_prunes_pages_within_a_row_group() {
    use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};

    const PAGE_ROWS: i32 = 1024;
    let temp = TempDir::new().unwrap();
    let path = format!("{}/pages.parquet", temp.path().to_str().unwrap());
    // One row group of four pages. Pages hold many rows so the reader skips
    // unselected pages instead of decoding them under a row mask.
    let schema = Arc::new(ArrowSchema::new(vec![field("id", DataType::Int32, 1)]));
    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_data_page_row_count_limit(PAGE_ROWS as usize)
        .set_write_batch_size(PAGE_ROWS as usize)
        .build();
    let mut writer = ArrowWriter::try_new(
        File::create(&path).unwrap(),
        Arc::clone(&schema),
        Some(props),
    )
    .unwrap();
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(
        Int32Array::from_iter_values(0..4 * PAGE_ROWS),
    )])
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Required)
        .parse_and_finish(&File::open(&path).unwrap())
        .unwrap();
    assert_eq!(metadata.num_row_groups(), 1);
    assert_eq!(
        metadata.offset_index().unwrap()[0][0]
            .page_locations()
            .len(),
        4
    );

    let read = |row_selection: bool| {
        let task = scan_task_with_deletes_and_projection(
            path.clone(),
            iceberg_schema(),
            None,
            vec![],
            vec![1],
        );
        async move {
            let provider = Arc::new(FixedRuntimePredicate::new(
                Reference::new("id")
                    .greater_than_or_equal_to(Datum::int(2 * PAGE_ROWS))
                    .and(Reference::new("id").less_than(Datum::int(3 * PAGE_ROWS))),
            ));
            let scan = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current())
                .with_row_group_filtering_enabled(false)
                .with_row_selection_enabled(row_selection)
                // Keep the skipped page ranges separate; the default 1 MiB
                // coalescing threshold would read them along with selected pages.
                .with_range_coalesce_bytes(0)
                .with_runtime_predicate_provider(provider)
                .build()
                .read(Box::pin(futures::stream::iter([Ok(task)])) as FileScanTaskStream)
                .unwrap();
            let metrics = scan.metrics().clone();
            let batches: Vec<RecordBatch> = scan.stream().try_collect().await.unwrap();
            (ids(&batches), metrics.bytes_read())
        }
    };
    let expected: Vec<i32> = (2 * PAGE_ROWS..3 * PAGE_ROWS).collect();
    // Without page selection the row filter decodes every page.
    let (unpruned_ids, unpruned) = read(false).await;
    let (pruned_ids, pruned) = read(true).await;
    assert_eq!(unpruned_ids, expected);
    assert_eq!(pruned_ids, expected);
    assert!(pruned < unpruned, "pruned={pruned} unpruned={unpruned}");
}

#[tokio::test]
async fn runtime_predicate_that_cannot_be_planned_keeps_the_planned_filter() {
    use arrow_array::StructArray;

    let temp = TempDir::new().unwrap();
    let path = format!("{}/nested.parquet", temp.path().to_str().unwrap());
    let x = field("x", DataType::Int32, 4);
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        field("id", DataType::Int32, 1),
        field("s", DataType::Struct(vec![x.clone()].into()), 3),
    ]));
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(4))
        .build();
    let mut writer = ArrowWriter::try_new(
        File::create(&path).unwrap(),
        Arc::clone(&arrow_schema),
        Some(props),
    )
    .unwrap();
    for base in [0, 100, 200] {
        let s = StructArray::from(vec![(
            Arc::new(x.clone()),
            Arc::new(Int32Array::from(vec![-1; 4])) as ArrayRef,
        )]);
        let batch = RecordBatch::try_new(Arc::clone(&arrow_schema), vec![
            Arc::new(Int32Array::from((base..base + 4).collect::<Vec<_>>())),
            Arc::new(s),
        ])
        .unwrap();
        writer.write(&batch).unwrap();
        writer.flush().unwrap();
    }
    writer.close().unwrap();

    let schema: SchemaRef = Arc::new(
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(
                    3,
                    "s",
                    Type::Struct(crate::spec::StructType::new(vec![
                        NestedField::required(4, "x", Type::Primitive(PrimitiveType::Int)).into(),
                    ])),
                )
                .into(),
            ])
            .build()
            .unwrap(),
    );
    let task = |predicate: Option<crate::expr::BoundPredicate>| {
        scan_task_with_deletes_and_projection(
            path.clone(),
            Arc::clone(&schema),
            predicate,
            vec![],
            vec![1],
        )
    };
    let planned = Reference::new("id")
        .greater_than_or_equal_to(Datum::int(100))
        .bind(Arc::clone(&schema), false)
        .unwrap();
    // The reader cannot build row filters on nested columns, so this passes the
    // column checks but fails planning; applied, it would reject every row.
    let provider = Arc::new(FixedRuntimePredicate::new(
        Reference::new("s.x").greater_than_or_equal_to(Datum::int(0)),
    ));
    let (_, baseline) = execute(task(None), None).await;
    let (batches, metrics) = execute(task(Some(planned)), Some(provider)).await;
    assert_eq!(ids(&batches), vec![100, 101, 102, 103, 200, 201, 202, 203]);
    // The planned predicate still prunes RG0.
    assert!(metrics.bytes_read() < baseline.bytes_read());
}

#[test]
fn runtime_page_selection_failure_keeps_the_planned_selection() {
    use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
    use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};

    use super::ArrowReader;
    use super::runtime_predicate::intersect_page_selection;

    let temp = TempDir::new().unwrap();
    let path = write_row_group_file_with_page_size(
        temp.path().to_str().unwrap(),
        "pages.parquet",
        &[0],
        1,
    );
    let metadata = Arc::new(
        ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Required)
            .parse_and_finish(&File::open(&path).unwrap())
            .unwrap(),
    );
    // The page-index evaluator rejects NOT, which the reader rewrites away
    // before planning, so this yields a real evaluation error.
    let predicate = (!Reference::new("id").less_than(Datum::int(2)))
        .bind(iceberg_schema(), false)
        .unwrap();
    let field_id_map = HashMap::from([(1, 0), (2, 1)]);
    let selection = || {
        ArrowReader::get_row_selection_for_filter_predicate(
            &predicate,
            &metadata,
            &None,
            &field_id_map,
            &iceberg_schema(),
        )
    };
    assert!(selection().is_err());

    let planned = RowSelection::from(vec![RowSelector::skip(1), RowSelector::select(3)]);
    let combined =
        intersect_page_selection(Some(planned.clone()), selection(), true, &path).unwrap();
    assert_eq!(combined, Some(planned));
    assert!(intersect_page_selection(None, selection(), false, &path).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runtime_predicate_concurrent_tasks_reuse_success_and_failure() {
    let temp = TempDir::new().unwrap();
    let path = write_three_row_group_file(temp.path().to_str().unwrap(), "concurrent.parquet");
    let tasks: Vec<_> = (0..12)
        .map(|_| scan_task(path.clone(), iceberg_schema(), None))
        .collect();
    let success = Arc::new(FixedRuntimePredicate::new(
        Reference::new("id").greater_than_or_equal_to(Datum::int(200)),
    ));
    let absent = Arc::new(ChangingRuntimePredicate::new(None, 1));
    let failure = Arc::new(FailedProvider::default());
    let binding_failure = Arc::new(FixedRuntimePredicate::new(
        Reference::new("missing").equal_to(Datum::int(0)),
    ));
    for (provider, expected) in [
        (success.clone() as Arc<dyn RuntimePredicateProvider>, vec![
            200, 201, 202, 203,
        ]),
        (
            absent.clone() as Arc<dyn RuntimePredicateProvider>,
            all_ids(),
        ),
        (
            failure.clone() as Arc<dyn RuntimePredicateProvider>,
            all_ids(),
        ),
        (
            binding_failure.clone() as Arc<dyn RuntimePredicateProvider>,
            all_ids(),
        ),
    ] {
        let (batches, _) =
            execute_tasks_with_concurrency(tasks.clone(), Some(provider), false, 4).await;
        let mut actual = ids(&batches);
        actual.sort_unstable();
        let mut expected = expected.repeat(tasks.len());
        expected.sort_unstable();
        assert_eq!(actual, expected);
    }
    assert_eq!(success.snapshots(), 1);
    assert_eq!(absent.snapshots(), 1);
    assert_eq!(failure.snapshots.load(Ordering::Relaxed), 1);
    assert_eq!(binding_failure.snapshots(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runtime_predicate_concurrent_tasks_pick_up_new_generation_without_regression() {
    let temp = TempDir::new().unwrap();
    let path = write_three_row_group_file(temp.path().to_str().unwrap(), "generations.parquet");
    let task = scan_task(path, iceberg_schema(), None);
    let provider = Arc::new(ChangingRuntimePredicate::new(
        Some(Reference::new("id").greater_than_or_equal_to(Datum::int(100))),
        1,
    ));
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let next = task.clone();
    // Withhold the second wave until the four first-wave tasks have completed.
    // This tests a publication boundary without depending on file-read timing.
    let tasks = futures::stream::iter(vec![task.clone(); 4].into_iter().map(Ok))
        .chain(futures::stream::once(async move {
            release_rx.await.unwrap();
            Ok(next)
        }))
        .chain(futures::stream::iter(vec![task; 3].into_iter().map(Ok)));
    let scan = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current())
        .with_data_file_concurrency_limit(4)
        .with_runtime_predicate_provider(provider.clone())
        .build()
        .read(Box::pin(tasks) as FileScanTaskStream)
        .unwrap();
    let mut stream = scan.stream();
    let mut first_wave = vec![];
    // Each first-wave file produces two four-row batches (RG1 and RG2).
    for _ in 0..8 {
        first_wave.push(stream.try_next().await.unwrap().unwrap());
    }
    let mut actual = ids(&first_wave);
    actual.sort_unstable();
    let mut expected = [100, 101, 102, 103, 200, 201, 202, 203].repeat(4);
    expected.sort_unstable();
    assert_eq!(actual, expected);
    assert_eq!(provider.snapshots(), 1);

    provider.publish(
        Some(Reference::new("id").greater_than_or_equal_to(Datum::int(200))),
        2,
    );
    release_tx.send(()).unwrap();
    let second_wave = stream.try_collect::<Vec<_>>().await.unwrap();
    let mut actual = ids(&second_wave);
    actual.sort_unstable();
    let mut expected = [200, 201, 202, 203].repeat(4);
    expected.sort_unstable();
    assert_eq!(actual, expected);
    assert_eq!(provider.snapshots(), 2);
}

async fn check_promoted_runtime_column(
    path: &str,
    file_type: DataType,
    table_type: PrimitiveType,
    values: ArrayRef,
    predicate: Predicate,
) -> Vec<RecordBatch> {
    write_delete(
        path,
        vec![
            field("id", DataType::Int32, 1),
            field("value", file_type, 3),
        ],
        vec![Arc::new(Int32Array::from(vec![0, 1, 2, 3])), values],
    );
    let schema = Arc::new(
        Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(3, "value", Type::Primitive(table_type)).into(),
            ])
            .build()
            .unwrap(),
    );
    let planned = Reference::new("id")
        .greater_than_or_equal_to(Datum::int(1))
        .bind(schema.clone(), false)
        .unwrap();
    let task = scan_task_with_deletes_and_projection(
        path.to_string(),
        schema,
        Some(planned),
        vec![],
        vec![1, 3],
    );
    let (baseline, _) = execute(task.clone(), None).await;
    let provider = Arc::new(FixedRuntimePredicate::new(predicate));
    let (batches, _) = execute(task, Some(provider.clone())).await;
    assert_eq!(ids(&batches), vec![1, 2, 3]);
    assert_eq!(
        batches, baseline,
        "promotion must fail open and preserve the planned filter"
    );
    assert_eq!(provider.snapshots(), 1);
    batches
}

#[tokio::test]
async fn runtime_predicate_on_float_promoted_to_double_is_ignored() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("float.parquet");
    // The DOUBLE boundary is between adjacent FLOAT values. Narrowing it
    // changes the comparison even though it lies within the FLOAT range.
    let boundary = 1.0 + f64::from(f32::EPSILON) / 2.0;
    let batches = check_promoted_runtime_column(
        path.to_str().unwrap(),
        DataType::Float32,
        PrimitiveType::Double,
        Arc::new(Float32Array::from(vec![
            1.0,
            1.0,
            1.0 + f32::EPSILON,
            1.0 + f32::EPSILON,
        ])),
        Reference::new("value").less_than(Datum::double(boundary)),
    )
    .await;
    assert_eq!(batches[0].column(1).data_type(), &DataType::Float64);
    let values = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(values.values().as_ref(), &[
        1.0,
        f64::from(1.0 + f32::EPSILON),
        f64::from(1.0 + f32::EPSILON)
    ]);
}

#[tokio::test]
async fn runtime_predicate_on_widened_decimal_precision_is_ignored() {
    let temp = TempDir::new().unwrap();
    // 8 -> 9 can share the same physical width; precision itself must be
    // checked, rather than only the Parquet physical storage type.
    for precision in [9, 12] {
        let path = temp.path().join(format!("decimal-{precision}.parquet"));
        let batches = check_promoted_runtime_column(
            path.to_str().unwrap(),
            DataType::Decimal128(8, 2),
            PrimitiveType::Decimal {
                precision,
                scale: 2,
            },
            Arc::new(
                Decimal128Array::from(vec![123, 456, 789, 1000])
                    .with_precision_and_scale(8, 2)
                    .unwrap(),
            ),
            // Fits the table precision but exceeds the file's precision.
            Reference::new("value").greater_than(Datum::decimal_from_str("1000000.00").unwrap()),
        )
        .await;
        assert_eq!(
            batches[0].column(1).data_type(),
            &DataType::Decimal128(precision as u8, 2)
        );
        let values = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(values.values().as_ref(), &[456, 789, 1000]);
    }
}

#[tokio::test]
async fn runtime_predicate_row_group_pruning_preserves_projected_positions() {
    use crate::metadata_columns::RESERVED_FIELD_ID_POS;

    let temp = TempDir::new().unwrap();
    let path = write_three_row_group_file(temp.path().to_str().unwrap(), "positions.parquet");
    let task = scan_task_with_deletes_and_projection(path, iceberg_schema(), None, vec![], vec![
        1,
        2,
        RESERVED_FIELD_ID_POS,
    ]);
    let positions = |batches: &[RecordBatch]| {
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("_pos")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect::<Vec<_>>()
    };
    let (baseline, baseline_metrics) = execute(task.clone(), None).await;
    assert_eq!(positions(&baseline), (0..12).collect::<Vec<i64>>());
    let provider = Arc::new(FixedRuntimePredicate::new(
        Reference::new("id")
            .greater_than_or_equal_to(Datum::int(100))
            .and(Reference::new("id").less_than_or_equal_to(Datum::int(103))),
    ));
    let (batches, metrics) = execute(task, Some(provider)).await;
    assert_eq!(ids(&batches), vec![100, 101, 102, 103]);
    // RG0 was pruned, but RowNumber must still use RG1's original file ordinals.
    assert_eq!(positions(&batches), vec![4, 5, 6, 7]);
    assert!(metrics.bytes_read() < baseline_metrics.bytes_read());
}

#[tokio::test]
async fn runtime_predicate_shares_bloom_filter_reads_with_planned_predicate() {
    let temp = TempDir::new().unwrap();
    let path = format!("{}/shared-bloom.parquet", temp.path().to_str().unwrap());
    // Each row group spans the probed values, so statistics prune nothing and
    // only the bloom filters can prune row groups 0 and 2.
    let schema = Arc::new(ArrowSchema::new(vec![
        field("id", DataType::Int32, 1),
        field("payload", DataType::Utf8, 2),
    ]));
    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_max_row_group_row_count(Some(4))
        .set_bloom_filter_enabled(true)
        .build();
    let mut writer = ArrowWriter::try_new(
        File::create(&path).unwrap(),
        Arc::clone(&schema),
        Some(props),
    )
    .unwrap();
    for ids in [[0, 500, 1, 501], [2, 502, 3, 503], [4, 504, 5, 505]] {
        let payloads: Vec<String> = ids.iter().map(|id| format!("payload-{id}")).collect();
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(StringArray::from(payloads)),
        ])
        .unwrap();
        writer.write(&batch).unwrap();
        writer.flush().unwrap();
    }
    writer.close().unwrap();

    let bind = |predicate: Predicate| predicate.bind(iceberg_schema(), false).unwrap();
    let planned = Reference::new("id").equal_to(Datum::int(3));
    let runtime = Reference::new("id").is_in([Datum::int(3), Datum::int(77)]);
    // The same restriction as one planned predicate, which reads each bloom
    // filter once.
    let (expected, expected_metrics) = execute_tasks(
        vec![scan_task(
            path.clone(),
            iceberg_schema(),
            Some(bind(planned.clone().and(runtime.clone()))),
        )],
        None,
        true,
    )
    .await;
    let (batches, metrics) = execute_tasks(
        vec![scan_task(path, iceberg_schema(), Some(bind(planned)))],
        Some(Arc::new(FixedRuntimePredicate::new(runtime))),
        true,
    )
    .await;
    assert_eq!(ids(&expected), vec![3]);
    assert_eq!(ids(&batches), vec![3]);
    // A bloom filter is far larger than the rows read, so a repeated read of
    // the overlapping column would show up here.
    assert_eq!(metrics.bytes_read(), expected_metrics.bytes_read());
}

async fn next_ids(stream: &mut ArrowRecordBatchStream, batches: usize) -> Vec<i32> {
    let mut read = vec![];
    for _ in 0..batches {
        read.push(stream.try_next().await.unwrap().unwrap());
    }
    ids(&read)
}

#[tokio::test]
async fn runtime_predicate_transitions_to_none_and_then_to_a_tighter_predicate() {
    let temp = TempDir::new().unwrap();
    let path = write_three_row_group_file(temp.path().to_str().unwrap(), "transitions.parquet");
    let task = scan_task(path, iceberg_schema(), None);
    let provider = Arc::new(ChangingRuntimePredicate::new(
        Some(Reference::new("id").greater_than_or_equal_to(Datum::int(100))),
        1,
    ));
    // Tasks are released one at a time, so each is planned after the
    // preceding publication.
    let (tasks, released) = futures::channel::mpsc::unbounded::<FileScanTask>();
    let scan = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current())
        .with_data_file_concurrency_limit(1)
        .with_runtime_predicate_provider(provider.clone())
        .build()
        .read(Box::pin(released.map(Ok)) as FileScanTaskStream)
        .unwrap();
    let mut stream = scan.stream();

    tasks.unbounded_send(task.clone()).unwrap();
    assert_eq!(next_ids(&mut stream, 2).await, vec![
        100, 101, 102, 103, 200, 201, 202, 203
    ]);
    assert_eq!(provider.snapshots(), 1);

    // `None` stops pruning for later tasks; the cached predicate must not be reused.
    provider.publish(None, 2);
    tasks.unbounded_send(task.clone()).unwrap();
    assert_eq!(next_ids(&mut stream, 3).await, all_ids());
    assert_eq!(provider.snapshots(), 2);

    // A later predicate tighter than the first one applies again.
    provider.publish(
        Some(Reference::new("id").greater_than_or_equal_to(Datum::int(200))),
        3,
    );
    tasks.unbounded_send(task.clone()).unwrap();
    assert_eq!(next_ids(&mut stream, 1).await, vec![200, 201, 202, 203]);
    assert_eq!(provider.snapshots(), 3);

    // The unchanged generation is reused, not snapshotted again.
    tasks.unbounded_send(task).unwrap();
    assert_eq!(next_ids(&mut stream, 1).await, vec![200, 201, 202, 203]);
    assert_eq!(provider.snapshots(), 3);

    drop(tasks);
    assert!(stream.try_next().await.unwrap().is_none());
}
