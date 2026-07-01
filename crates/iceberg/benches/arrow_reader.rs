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

//! Benchmarks for [`ArrowReader`], focused on the per-`FileScanTask` overhead
//! that dominates scans of tables with many small data files (see epic #2172).
//!
//! The files are written to a local temp directory and read back through the
//! normal `FileIO` (local FS) path, so these measure CPU and per-task work
//! (operator construction, metadata loading, schema resolution, projection /
//! row-filter setup, stream wiring) rather than network latency. They give an
//! in-repo, reproducible baseline against which I/O- and CPU-reuse
//! optimizations can be measured.
//!
//! The `same_file_splits` group additionally reports `ScanMetrics::bytes_read`
//! as its throughput basis. Because that is a deterministic count (not a
//! wall-clock sample), it surfaces redundant I/O directly: reading one file as
//! N byte-range splits fetches the Parquet metadata N times, so total bytes
//! read grows with the split count even though the file contents are identical.
//! This is the cost that same-file metadata caching (proposed in #2172,
//! attempted in #2100) targets, and is measurable even on the local FS.
//!
//! Run with: `cargo bench -p iceberg --bench arrow_reader`

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::{TryStreamExt, stream};
use iceberg::Runtime;
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::expr::{Bind, Reference};
use iceberg::io::FileIO;
use iceberg::scan::{FileScanTask, FileScanTaskStream};
use iceberg::spec::{
    DataFileFormat, Datum, MappedField, NameMapping, NestedField, PrimitiveType, Schema, SchemaRef,
    Type,
};
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use tempfile::TempDir;
use tokio::runtime::Runtime as TokioRuntime;

/// Rows written into each generated Parquet data file. Kept small so the
/// benchmark stresses per-file overhead (the regime epic #2172 targets) rather
/// than column decoding throughput.
const ROWS_PER_FILE: usize = 100;

/// Build the Iceberg + Arrow schema pair used for every generated file:
/// `(id: long, name: string)` with embedded Parquet field IDs, so the reader
/// takes the "file has field IDs" fast path.
fn schemas() -> (SchemaRef, arrow_schema::SchemaRef) {
    let iceberg_schema = Arc::new(
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap(),
    );

    let arrow_schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false).with_metadata(
            std::collections::HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )]),
        ),
        arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, true).with_metadata(
            std::collections::HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )]),
        ),
    ]));

    (iceberg_schema, arrow_schema)
}

/// Write `num_files` small Parquet files into `dir` and return their paths.
fn write_files(
    dir: &TempDir,
    num_files: usize,
    arrow_schema: &arrow_schema::SchemaRef,
) -> Vec<String> {
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..ROWS_PER_FILE as i64));
    let names: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..ROWS_PER_FILE).map(|i| format!("row-{i}")),
    ));
    let batch =
        RecordBatch::try_new(arrow_schema.clone(), vec![ids, names]).expect("build record batch");

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    (0..num_files)
        .map(|i| {
            let path = format!("{}/data_{i}.parquet", dir.path().to_str().unwrap());
            let file = std::fs::File::create(&path).expect("create parquet file");
            let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props.clone()))
                .expect("create arrow writer");
            writer.write(&batch).expect("write batch");
            writer.close().expect("close writer");
            path
        })
        .collect()
}

/// Arrow schema WITHOUT embedded Parquet field IDs, simulating files migrated
/// from Hive/Spark via `add_files`. Forces the reader's name-mapping branch.
fn arrow_schema_without_field_ids() -> arrow_schema::SchemaRef {
    Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
        arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, true),
    ]))
}

/// Name mapping that assigns the table's field IDs by column name — what the
/// reader applies when a file lacks embedded field IDs.
fn name_mapping() -> Arc<NameMapping> {
    Arc::new(NameMapping::new(vec![
        MappedField::new(Some(1), vec!["id".to_string()], vec![]),
        MappedField::new(Some(2), vec!["name".to_string()], vec![]),
    ]))
}

/// Write a single Parquet file with many small row groups, so that byte-range
/// splits select disjoint row-group sets — the same-file-split scenario that
/// metadata caching (#2100 / item #5) targets.
fn write_multi_row_group_file(
    dir: &TempDir,
    total_rows: usize,
    rows_per_row_group: usize,
    arrow_schema: &arrow_schema::SchemaRef,
) -> String {
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..total_rows as i64));
    let names: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..total_rows).map(|i| format!("row-{i}")),
    ));
    let batch =
        RecordBatch::try_new(arrow_schema.clone(), vec![ids, names]).expect("build record batch");

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_row_count(Some(rows_per_row_group))
        .build();

    let path = format!("{}/big.parquet", dir.path().to_str().unwrap());
    let file = std::fs::File::create(&path).expect("create parquet file");
    let mut writer =
        ArrowWriter::try_new(file, arrow_schema.clone(), Some(props)).expect("create arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
    path
}

/// Build a whole-file `FileScanTask` for each path (no predicate, no deletes —
/// the projection-only path, which isolates per-task setup overhead).
fn tasks_for(paths: &[String], schema: &SchemaRef) -> Vec<FileScanTask> {
    paths.iter().map(|p| base_task(p, schema)).collect()
}

/// A whole-file projection-only task for a single path.
fn base_task(path: &str, schema: &SchemaRef) -> FileScanTask {
    let size = std::fs::metadata(path).expect("stat parquet file").len();
    FileScanTask {
        file_size_in_bytes: size,
        start: 0,
        length: size,
        record_count: Some(ROWS_PER_FILE as u64),
        data_file_path: path.to_string(),
        data_file_format: DataFileFormat::Parquet,
        schema: schema.clone(),
        project_field_ids: vec![1, 2],
        predicate: None,
        deletes: vec![],
        partition: None,
        partition_spec: None,
        name_mapping: None,
        case_sensitive: false,
    }
}

/// Like [`tasks_for`] but attaches a name mapping (migrated-table path).
fn tasks_with_name_mapping(paths: &[String], schema: &SchemaRef) -> Vec<FileScanTask> {
    let mapping = name_mapping();
    paths
        .iter()
        .map(|p| FileScanTask {
            name_mapping: Some(mapping.clone()),
            ..base_task(p, schema)
        })
        .collect()
}

/// Build `num_splits` byte-range tasks over the SAME file, partitioning its
/// length into contiguous ranges (mirroring how Iceberg Java splits a large
/// file across partitions).
fn same_file_split_tasks(path: &str, schema: &SchemaRef, num_splits: u64) -> Vec<FileScanTask> {
    let size = std::fs::metadata(path).expect("stat parquet file").len();
    let chunk = size.div_ceil(num_splits);
    (0..num_splits)
        .map(|i| {
            let start = i * chunk;
            let length = chunk.min(size.saturating_sub(start));
            FileScanTask {
                file_size_in_bytes: size,
                start,
                length,
                record_count: None,
                data_file_path: path.to_string(),
                data_file_format: DataFileFormat::Parquet,
                schema: schema.clone(),
                project_field_ids: vec![1, 2],
                predicate: None,
                deletes: vec![],
                partition: None,
                partition_spec: None,
                name_mapping: None,
                case_sensitive: false,
            }
        })
        .collect()
}

/// Like [`tasks_for`] but attaches a bound predicate (`id < total/2`),
/// exercising row-filter and row-group-filtering setup per task.
fn tasks_with_predicate(paths: &[String], schema: &SchemaRef) -> Vec<FileScanTask> {
    let predicate = Reference::new("id")
        .less_than(Datum::long(ROWS_PER_FILE as i64 / 2))
        .bind(schema.clone(), true)
        .expect("bind predicate");
    paths
        .iter()
        .map(|p| FileScanTask {
            predicate: Some(predicate.clone()),
            ..base_task(p, schema)
        })
        .collect()
}

/// Read every task to completion, draining all record batches. `filtering`
/// turns on row-group filtering and row selection (only meaningful when the
/// tasks carry a predicate).
async fn read_all(tasks: Vec<FileScanTask>, concurrency: usize, filtering: bool) {
    let file_io = FileIO::new_with_fs();
    let reader = ArrowReaderBuilder::new(file_io, Runtime::current())
        .with_data_file_concurrency_limit(concurrency)
        .with_row_group_filtering_enabled(filtering)
        .with_row_selection_enabled(filtering)
        .build();

    let task_stream: FileScanTaskStream = Box::pin(stream::iter(tasks.into_iter().map(Ok)));

    let batches: Vec<RecordBatch> = reader
        .read(task_stream)
        .expect("build read stream")
        .stream()
        .try_collect()
        .await
        .expect("read all batches");

    criterion::black_box(batches);
}

/// Read every task to completion and return the total number of bytes fetched
/// from storage (`ScanMetrics::bytes_read`). Unlike wall-clock time this is a
/// deterministic measurement, which makes it the right tool for surfacing
/// redundant I/O — e.g. the per-split metadata re-fetch that same-file metadata
/// caching (proposed in #2172, attempted in #2100) targets.
async fn read_all_bytes(tasks: Vec<FileScanTask>, concurrency: usize) -> u64 {
    let file_io = FileIO::new_with_fs();
    let reader = ArrowReaderBuilder::new(file_io, Runtime::current())
        .with_data_file_concurrency_limit(concurrency)
        .build();

    let task_stream: FileScanTaskStream = Box::pin(stream::iter(tasks.into_iter().map(Ok)));

    let result = reader.read(task_stream).expect("build read stream");
    let metrics = result.metrics().clone();
    let _: Vec<RecordBatch> = result
        .stream()
        .try_collect()
        .await
        .expect("read all batches");

    metrics.bytes_read()
}

/// Scans of many small files at a fixed concurrency. Throughput is reported in
/// files/sec so the per-file overhead is directly visible across file counts.
fn bench_many_small_files(c: &mut Criterion) {
    let tokio = TokioRuntime::new().unwrap();
    let (iceberg_schema, arrow_schema) = schemas();

    let mut group = c.benchmark_group("many_small_files");
    for num_files in [16usize, 64, 256] {
        // Files are written once and reused across all samples for this size.
        let dir = TempDir::new().unwrap();
        let paths = write_files(&dir, num_files, &arrow_schema);

        group.throughput(Throughput::Elements(num_files as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(num_files),
            &num_files,
            |b, _| {
                b.to_async(&tokio).iter(|| {
                    let tasks = tasks_for(&paths, &iceberg_schema);
                    read_all(tasks, num_cpus_limit(), false)
                });
            },
        );
    }
    group.finish();
}

/// How concurrency affects throughput for a fixed corpus — exercises both the
/// single-concurrency fast path and the buffered/flattened multi-task path.
fn bench_concurrency(c: &mut Criterion) {
    let tokio = TokioRuntime::new().unwrap();
    let (iceberg_schema, arrow_schema) = schemas();

    let num_files = 128usize;
    let dir = TempDir::new().unwrap();
    let paths = write_files(&dir, num_files, &arrow_schema);

    let mut group = c.benchmark_group("concurrency");
    group.throughput(Throughput::Elements(num_files as u64));
    for concurrency in [1usize, 4, 16] {
        group.bench_with_input(
            BenchmarkId::from_parameter(concurrency),
            &concurrency,
            |b, &concurrency| {
                b.to_async(&tokio).iter(|| {
                    let tasks = tasks_for(&paths, &iceberg_schema);
                    read_all(tasks, concurrency, false)
                });
            },
        );
    }
    group.finish();
}

/// Migrated tables: files lack embedded field IDs, so each task must apply the
/// name mapping and rebuild `ArrowReaderMetadata`. Compared against the
/// field-ID fast path, this isolates the migrated-table schema-resolution cost.
fn bench_migrated_table(c: &mut Criterion) {
    let tokio = TokioRuntime::new().unwrap();
    let (iceberg_schema, _) = schemas();
    let arrow_schema = arrow_schema_without_field_ids();

    let num_files = 128usize;
    let dir = TempDir::new().unwrap();
    let paths = write_files(&dir, num_files, &arrow_schema);

    let mut group = c.benchmark_group("migrated_table");
    group.throughput(Throughput::Elements(num_files as u64));
    group.bench_function("name_mapping", |b| {
        b.to_async(&tokio).iter(|| {
            let tasks = tasks_with_name_mapping(&paths, &iceberg_schema);
            read_all(tasks, num_cpus_limit(), false)
        });
    });
    group.finish();
}

/// Same-file splits: one large multi-row-group file read as N byte-range
/// tasks. Each task currently re-fetches the file's Parquet metadata
/// independently — the scenario same-file metadata caching (proposed in #2172,
/// attempted in #2100) targets.
fn bench_same_file_splits(c: &mut Criterion) {
    let tokio = TokioRuntime::new().unwrap();
    let (iceberg_schema, arrow_schema) = schemas();

    let total_rows = 100_000usize;
    let rows_per_row_group = 2_000usize;
    let dir = TempDir::new().unwrap();
    let path = write_multi_row_group_file(&dir, total_rows, rows_per_row_group, &arrow_schema);
    let file_size = std::fs::metadata(&path).expect("stat parquet file").len();

    let mut group = c.benchmark_group("same_file_splits");
    for num_splits in [1u64, 8, 32] {
        // Measure bytes_read once (deterministic) and report it as the
        // benchmark's throughput basis, so criterions own output shows how
        // total bytes fetched grows with the split count even though the file
        // contents are identical — i.e. the redundant per-split metadata I/O.
        let bytes_read = tokio.block_on(read_all_bytes(
            same_file_split_tasks(&path, &iceberg_schema, num_splits),
            num_cpus_limit(),
        ));
        eprintln!(
            "same_file_splits/{num_splits}: bytes_read = {bytes_read} \
             ({:.2}x file size of {file_size})",
            bytes_read as f64 / file_size as f64
        );

        group.throughput(Throughput::Bytes(bytes_read));
        group.bench_with_input(
            BenchmarkId::from_parameter(num_splits),
            &num_splits,
            |b, &num_splits| {
                b.to_async(&tokio).iter(|| {
                    let tasks = same_file_split_tasks(&path, &iceberg_schema, num_splits);
                    read_all(tasks, num_cpus_limit(), false)
                });
            },
        );
    }
    group.finish();
}

/// Scans carrying a predicate, with row-group filtering and row selection
/// enabled — exercises the per-task row-filter / field-id-map / row-group
/// pruning setup that the projection-only benchmarks skip.
fn bench_with_predicate(c: &mut Criterion) {
    let tokio = TokioRuntime::new().unwrap();
    let (iceberg_schema, arrow_schema) = schemas();

    let num_files = 128usize;
    let dir = TempDir::new().unwrap();
    let paths = write_files(&dir, num_files, &arrow_schema);

    let mut group = c.benchmark_group("with_predicate");
    group.throughput(Throughput::Elements(num_files as u64));
    group.bench_function("id_lt_half", |b| {
        b.to_async(&tokio).iter(|| {
            let tasks = tasks_with_predicate(&paths, &iceberg_schema);
            read_all(tasks, num_cpus_limit(), true)
        });
    });
    group.finish();
}

fn num_cpus_limit() -> usize {
    std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(4)
}

criterion_group!(
    benches,
    bench_many_small_files,
    bench_concurrency,
    bench_migrated_table,
    bench_same_file_splits,
    bench_with_predicate,
);
criterion_main!(benches);
