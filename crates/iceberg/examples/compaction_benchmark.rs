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

//! Native compaction benchmark using iceberg-rust + DataFusion.
//!
//! Demonstrates the full native compaction pipeline:
//!   1. Generate N small Parquet files (simulate micro-batch fragmentation)
//!   2. Read all files via iceberg-rust scan (Arrow RecordBatch stream)
//!   3. Write a single compacted Parquet file via iceberg-rust ParquetWriter
//!   4. Commit replacement via ReplaceDataFilesAction
//!   5. Verify the compacted table via scan
//!
//! Compare these timings against Spark's SparkBinPackFileRewriteRunner.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::SchemaRef as ArrowSchemaRef;
use futures::TryStreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder, Type};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, TableCreation};
use parquet::file::properties::WriterProperties;

fn create_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(3, "value", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(4, "category", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(5, "ts", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()
        .unwrap()
}

fn generate_batch(arrow_schema: &ArrowSchemaRef, start_id: i64, num_rows: usize) -> RecordBatch {
    let ids: Vec<i64> = (start_id..start_id + num_rows as i64).collect();
    let names: Vec<String> = ids.iter().map(|i| format!("name_{i}")).collect();
    let values: Vec<i64> = ids.iter().map(|i| i * 100).collect();
    let categories: Vec<String> = ids.iter().map(|i| format!("cat_{}", i % 10)).collect();
    let timestamps: Vec<i64> = ids.iter().map(|i| 1700000000 + i).collect();

    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(ids)),
        Arc::new(StringArray::from(names)),
        Arc::new(Int64Array::from(values)),
        Arc::new(StringArray::from(categories)),
        Arc::new(Int64Array::from(timestamps)),
    ];

    RecordBatch::try_new(arrow_schema.clone(), columns).unwrap()
}

async fn run_benchmark(num_files: usize, rows_per_file: usize) {
    let total_rows = num_files * rows_per_file;
    println!("==========================================================================");
    println!(
        "Native Compaction Benchmark: {num_files} files x {rows_per_file} rows = {total_rows} total rows"
    );
    println!("==========================================================================");

    // Setup catalog with temp directory
    let temp_dir = tempfile::TempDir::new().unwrap();
    let warehouse_path = temp_dir.path().to_str().unwrap().to_string();

    let catalog = MemoryCatalogBuilder::default()
        .load(
            "bench_catalog",
            HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse_path)]),
        )
        .await
        .unwrap();

    let ns = iceberg::NamespaceIdent::new("bench_ns".to_string());
    catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

    let schema = create_schema();
    let table_creation = TableCreation::builder()
        .name("fragmented_table".to_string())
        .schema(schema.clone())
        .partition_spec(PartitionSpec::unpartition_spec())
        .sort_order(SortOrder::unsorted_order())
        .build();

    let mut table = catalog.create_table(&ns, table_creation).await.unwrap();

    // Derive Arrow schema from Iceberg schema (includes field ID metadata)
    let arrow_schema: ArrowSchemaRef =
        Arc::new(schema_to_arrow_schema(table.metadata().current_schema()).unwrap());

    // Phase 1: Write N small files (simulating micro-batch ingestion)
    let write_start = Instant::now();
    let mut all_data_files = Vec::new();

    for file_idx in 0..num_files {
        let start_id = (file_idx * rows_per_file) as i64;
        let batch = generate_batch(&arrow_schema, start_id, rows_per_file);

        let location_gen = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
        let file_name_gen = DefaultFileNameGenerator::new(
            format!("frag_{file_idx:04}"),
            None,
            iceberg::spec::DataFileFormat::Parquet,
        );
        let pw_builder = ParquetWriterBuilder::new(
            WriterProperties::default(),
            table.metadata().current_schema().clone(),
        );
        let rolling_builder = RollingFileWriterBuilder::new_with_default_file_size(
            pw_builder,
            table.file_io().clone(),
            location_gen,
            file_name_gen,
        );
        let dfw_builder = DataFileWriterBuilder::new(rolling_builder);
        let mut writer = dfw_builder.build(None).await.unwrap();
        writer.write(batch).await.unwrap();
        let data_files = writer.close().await.unwrap();
        all_data_files.extend(data_files);
    }

    // Commit all small files in a single transaction
    let tx = Transaction::new(&table);
    let action = tx.fast_append().add_data_files(all_data_files.clone());
    let tx = action.apply(tx).unwrap();
    table = tx.commit(&catalog).await.unwrap();
    let write_elapsed = write_start.elapsed();
    println!(
        "Phase 1 - Write {} small files:       {:>8.1} ms",
        num_files,
        write_elapsed.as_secs_f64() * 1000.0
    );
    println!(
        "  Files committed: {}, total rows: {}",
        all_data_files.len(),
        total_rows
    );

    // Phase 2: Read all files (scan) -- this is the compaction READ path
    let scan_start = Instant::now();
    let scan = table.scan().select_all().build().unwrap();
    let stream = scan.to_arrow().await.unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    let scan_elapsed = scan_start.elapsed();

    let scanned_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!(
        "Phase 2 - Scan all files (read path): {:>8.1} ms  ({} rows, {} batches)",
        scan_elapsed.as_secs_f64() * 1000.0,
        scanned_rows,
        batches.len()
    );

    // Phase 3: Write compacted file -- this is the compaction WRITE path
    let compact_write_start = Instant::now();
    let location_gen = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let file_name_gen = DefaultFileNameGenerator::new(
        "compacted".to_string(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );
    let pw_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
    );
    let rolling_builder = RollingFileWriterBuilder::new_with_default_file_size(
        pw_builder,
        table.file_io().clone(),
        location_gen,
        file_name_gen,
    );
    let dfw_builder = DataFileWriterBuilder::new(rolling_builder);
    let mut compact_writer = dfw_builder.build(None).await.unwrap();

    for batch in &batches {
        compact_writer.write(batch.clone()).await.unwrap();
    }
    let compacted_data_files = compact_writer.close().await.unwrap();
    let compact_write_elapsed = compact_write_start.elapsed();
    println!(
        "Phase 3 - Write compacted file:       {:>8.1} ms  ({} output files)",
        compact_write_elapsed.as_secs_f64() * 1000.0,
        compacted_data_files.len()
    );

    // Phase 4: Commit replacement via ReplaceDataFilesAction
    let commit_start = Instant::now();
    let snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();

    let tx = Transaction::new(&table);
    let action = tx
        .replace_data_files()
        .validate_from_snapshot(snapshot_id)
        .delete_files(all_data_files)
        .add_files(compacted_data_files);
    let tx = action.apply(tx).unwrap();
    table = tx.commit(&catalog).await.unwrap();
    let commit_elapsed = commit_start.elapsed();
    println!(
        "Phase 4 - Commit replacement:         {:>8.1} ms",
        commit_elapsed.as_secs_f64() * 1000.0
    );

    // Phase 5: Verify by scanning compacted table
    let verify_start = Instant::now();
    let scan = table.scan().select_all().build().unwrap();
    let stream = scan.to_arrow().await.unwrap();
    let verify_batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    let verify_rows: usize = verify_batches.iter().map(|b| b.num_rows()).sum();
    let verify_elapsed = verify_start.elapsed();
    println!(
        "Phase 5 - Verify (scan compacted):    {:>8.1} ms  ({} rows)",
        verify_elapsed.as_secs_f64() * 1000.0,
        verify_rows
    );

    assert_eq!(
        verify_rows, total_rows,
        "Row count mismatch after compaction"
    );

    let total_compaction = scan_elapsed + compact_write_elapsed + commit_elapsed;
    println!("--------------------------------------------------------------------------");
    println!(
        "Total compaction time (read+write+commit): {:>8.1} ms",
        total_compaction.as_secs_f64() * 1000.0
    );
    println!(
        "  Read:   {:>6.1} ms ({:.0}%)",
        scan_elapsed.as_secs_f64() * 1000.0,
        scan_elapsed.as_secs_f64() / total_compaction.as_secs_f64() * 100.0
    );
    println!(
        "  Write:  {:>6.1} ms ({:.0}%)",
        compact_write_elapsed.as_secs_f64() * 1000.0,
        compact_write_elapsed.as_secs_f64() / total_compaction.as_secs_f64() * 100.0
    );
    println!(
        "  Commit: {:>6.1} ms ({:.0}%)",
        commit_elapsed.as_secs_f64() * 1000.0,
        commit_elapsed.as_secs_f64() / total_compaction.as_secs_f64() * 100.0
    );
    println!();

    // Snapshot verification
    let snapshots: Vec<_> = table.metadata().snapshots().collect();
    println!("Snapshots: {} (append + replace)", snapshots.len());
    let current = table.metadata().current_snapshot().unwrap();
    println!(
        "Current snapshot operation: {:?}",
        current.summary().operation
    );
    println!();
}

#[tokio::main]
async fn main() {
    println!();
    println!("========================================================================");
    println!("  Native Iceberg Compaction Benchmark (iceberg-rust + Arrow)");
    println!("  No JVM, no Spark -- pure Rust pipeline");
    println!("========================================================================");
    println!();

    // Small: 20 files x 1K rows = 20K rows
    run_benchmark(20, 1_000).await;

    // Medium: 50 files x 10K rows = 500K rows
    run_benchmark(50, 10_000).await;

    // Large: 100 files x 50K rows = 5M rows
    run_benchmark(100, 50_000).await;

    // XL: 200 files x 50K rows = 10M rows
    run_benchmark(200, 50_000).await;

    println!("========================================================================");
    println!("  Benchmark complete.");
    println!("========================================================================");
}
