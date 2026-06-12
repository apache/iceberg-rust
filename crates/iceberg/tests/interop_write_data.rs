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

//! DATA-LEVEL write-action interop (sprint increment S1) — `MergeAppend` and `RewriteFiles`
//! proven against Java's `IcebergGenerics` production scan in TWO fixtures.
//!
//! ## Fixture A — `merge_append` data-level
//!
//! V2 partitioned table (identity(category), schema `{id long, category string, data string}`).
//! Rust performs the SAME chain as Java's `MergeAppendDataOracle.generate`:
//! - `fast_append` files A (cat=a, ids 10/20/30, data a/b/c) and B (cat=b, id 40, data d) — seq 1
//! - `update_table_properties` set `commit.manifest.min-count-to-merge=2` (no snapshot)
//! - `merge_append` G (cat=a, id 60, data g) — seq 2, MERGING: all manifests fit in one bin ⇒
//!   ONE merged manifest with Existing A+B entries + G as Added
//!
//! Correctness point: every row from A, B, and G must survive the scan. A Rust `merge_append` that
//! silently discards Existing entries would yield a wrong (missing-row) result here.
//! Java verifies: `IcebergGenerics.read(rust_table)` must yield exactly `{10,20,30,40,60}`.
//!
//! ## Fixture B — `rewrite_files` data-level (seq-preservation)
//!
//! Unpartitioned 2-field table `{id long, data string}`.
//! Rust performs the SAME chain as Java's revised `RewriteFilesDataOracle.generate`:
//! - `fast_append` A (5 rows: ids 10..50, data a..e) — seq 1
//! - `row_delta` equality-delete (equality_ids=[1], deletes ids 20+40) — seq 2
//! - `rewrite_files` {A}→{A'} with `data_sequence_number(1)` — seq 3
//!
//! Correctness point: A' is stamped with `data_sequence_number=1` (the replaced file's seq). The
//! equality-delete has seq 2. Applicability rule: eq-delete applies to data files with
//! `data_seq STRICTLY LESS THAN` the delete's seq. `A'.data_seq=1 < eq_del.seq=2`, so the delete
//! STILL APPLIES after the rewrite. Live rows must be `{10,30,50}` (ids 20+40 absent). Were Rust
//! to stamp A' with seq 3 the delete would not apply and ids 20/40 would wrongly survive.
//! Java verifies: `IcebergGenerics.read(rust_table)` must yield `{(10,a),(30,c),(50,e)}`.
//!
//! NOTE: position deletes are PATH-BASED — after A→A' the delete on A's path is dangling (A' has
//! a new path). To prove `data_sequence_number` preservation you MUST use an equality delete,
//! which is governed by seq applicability rules, not by file path.
//!
//! Both fixtures use REAL parquet files written via the production Rust writers (`DataFileWriter`,
//! `EqualityDeleteFileWriter`). The tables land at `<gen_dir>/rust_table` with
//! `metadata/final.metadata.json`; Java's `verify-interop-{merge-append-data,rewrite-data}` modes
//! read them.
//!
//! GATED on env vars (all four unset ⇒ clean no-ops; offline `cargo test` gate stays green):
//!
//! - `ICEBERG_INTEROP_MERGE_APPEND_DATA_GEN_DIR` — fixture A GEN (Rust writes)
//! - `ICEBERG_INTEROP_MERGE_APPEND_DATA_DIR`     — fixture A comparison (Rust reads Java rows)
//! - `ICEBERG_INTEROP_REWRITE_DATA_GEN_DIR`      — fixture B GEN (Rust writes)
//! - `ICEBERG_INTEROP_REWRITE_DATA_DIR`          — fixture B comparison (Rust reads Java rows)

use std::cmp::Ordering;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use futures::TryStreamExt;
use iceberg::io::{FileIO, LocalFsStorageFactory};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, FormatVersion, Literal, NestedField, PartitionKey,
    PartitionSpec, PrimitiveType, Schema, SchemaRef, SortOrder, Struct, Transform, Type,
    UnboundPartitionSpec,
};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::base_writer::equality_delete_writer::{
    EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use serde::Deserialize;

// ===========================================================================================
// Row model — the Java oracle's `{id, data}` JSON format (same as interop_scan_exec.rs).
// ===========================================================================================

/// One live row from Java's `IcebergGenerics` read: `id` (long) + nullable `data` string.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct ScanRow {
    id: i64,
    data: Option<String>,
}

/// Sort rows by id for an order-independent comparison.
fn sorted_by_id(mut rows: Vec<ScanRow>) -> Vec<ScanRow> {
    rows.sort_by(|a, b| a.id.cmp(&b.id).then_with(|| cmp_opt(&a.data, &b.data)));
    rows
}

fn cmp_opt(a: &Option<String>, b: &Option<String>) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(x), Some(y)) => x.cmp(y),
    }
}

// ===========================================================================================
// Env-var gates.
// ===========================================================================================

fn merge_append_data_gen_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_MERGE_APPEND_DATA_GEN_DIR")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

fn merge_append_data_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_MERGE_APPEND_DATA_DIR")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

fn rewrite_data_gen_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_REWRITE_DATA_GEN_DIR")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

fn rewrite_data_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_REWRITE_DATA_DIR")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

// ===========================================================================================
// Schema + spec helpers.
// ===========================================================================================

/// Fixture B schema: unpartitioned 2-field `{1 id long required, 2 data string optional}`.
/// Matches `ScanExecOracle` / `EqDeleteOracle` — the simplest shape for the seq-preservation proof.
fn rewrite_data_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "data", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("build the {id long, data string} schema")
}

/// Fixture A schema: `{1 id long required, 2 category string required, 3 data string optional}`.
fn write_data_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "category", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(3, "data", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("build the {id long, category string, data string} schema")
}

/// `identity(category)` unbound partition spec, spec id 0.
fn write_data_spec() -> UnboundPartitionSpec {
    UnboundPartitionSpec::builder()
        .with_spec_id(0)
        .add_partition_field(2, "category".to_string(), Transform::Identity)
        .expect("add identity(category) partition field")
        .build()
}

/// Build the `PartitionKey` for `category = <value>` over the bound spec + schema.
fn partition_key(schema: SchemaRef, spec: PartitionSpec, category: &str) -> PartitionKey {
    PartitionKey::new(
        spec,
        schema,
        Struct::from_iter([Some(Literal::string(category))]),
    )
}

// ===========================================================================================
// Real-parquet writer helpers (production paths — the same pattern as interop_scan_exec.rs).
// ===========================================================================================

/// Write a REAL parquet DATA file for one partition via the production `DataFileWriter`.
/// Each row carries the given `ids` and `data_values`; `category` matches the partition.
async fn write_data_file(
    table: &Table,
    partition_key: &PartitionKey,
    category: &str,
    ids: Vec<i64>,
    data_values: Vec<&str>,
) -> DataFile {
    use iceberg::arrow::schema_to_arrow_schema;

    let schema = table.metadata().current_schema();
    let arrow_schema = Arc::new(schema_to_arrow_schema(schema).expect("iceberg schema → arrow"));
    let row_count = ids.len();
    let categories: Vec<&str> = std::iter::repeat_n(category, row_count).collect();
    let batch = RecordBatch::try_new(arrow_schema, vec![
        Arc::new(Int64Array::from(ids)) as ArrayRef,
        Arc::new(StringArray::from(categories)) as ArrayRef,
        Arc::new(StringArray::from(data_values)) as ArrayRef,
    ])
    .expect("build the per-partition data batch");

    let location_gen =
        DefaultLocationGenerator::new(table.metadata().clone()).expect("location generator");
    let file_name_gen = DefaultFileNameGenerator::new(
        "wdata".to_string(),
        Some(uuid::Uuid::now_v7().to_string()),
        DataFileFormat::Parquet,
    );
    let parquet_builder = ParquetWriterBuilder::new(
        parquet::file::properties::WriterProperties::builder().build(),
        schema.clone(),
    );
    let rolling = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_builder,
        table.file_io().clone(),
        location_gen,
        file_name_gen,
    );

    let mut writer = DataFileWriterBuilder::new(rolling)
        .build(Some(partition_key.clone()))
        .await
        .expect("build partitioned data file writer");
    writer.write(batch).await.expect("write data batch");
    writer
        .close()
        .await
        .expect("close data file writer")
        .into_iter()
        .next()
        .expect("one data file")
}

// ===========================================================================================
// Scan helper — Arrow → ScanRow extraction (same as interop_scan_exec.rs).
// ===========================================================================================

fn extract_rows(batch: &RecordBatch) -> Vec<ScanRow> {
    let id = batch
        .column_by_name("id")
        .expect("id column present")
        .as_primitive::<Int64Type>();
    let data = batch.column_by_name("data").expect("data column present");

    (0..batch.num_rows())
        .map(|i| ScanRow {
            id: id.value(i),
            data: string_value(data, i),
        })
        .collect()
}

fn string_value(array: &ArrayRef, i: usize) -> Option<String> {
    use arrow_schema::DataType;
    if array.is_null(i) {
        return None;
    }
    match array.data_type() {
        DataType::Utf8 => Some(array.as_string::<i32>().value(i).to_string()),
        DataType::LargeUtf8 => Some(array.as_string::<i64>().value(i).to_string()),
        other => panic!("unexpected data column arrow type: {other:?}"),
    }
}

/// Extract the `(id → category)` mapping from a fixture-A scan batch. The `category` column is the
/// IDENTITY-PARTITION value; the `{id, data}` row compare deliberately drops it (Java's
/// `readLiveRowsToJson` only emits `{id, data}`), so a partition-routing divergence in
/// `merge_append` (a row materialized into the wrong partition) is invisible to the row compare.
/// This map lets the fixture-A tests pin the partition column directly. Risk pinned: a wrong-partition
/// write — the audit's Mutation 1 (route G to category="b") passed the row compare silently without it.
fn extract_id_to_category(batch: &RecordBatch) -> Vec<(i64, Option<String>)> {
    let id = batch
        .column_by_name("id")
        .expect("id column present")
        .as_primitive::<Int64Type>();
    let category = batch
        .column_by_name("category")
        .expect("category column present");
    (0..batch.num_rows())
        .map(|i| (id.value(i), string_value(category, i)))
        .collect()
}

/// The expected `(id → category)` partition routing for fixture A: A's rows (10,20,30) and G (60)
/// are `category="a"`; B (40) is `category="b"`. Sorted by id for a stable compare.
fn expected_merge_append_categories() -> Vec<(i64, Option<String>)> {
    vec![
        (10, Some("a".to_string())),
        (20, Some("a".to_string())),
        (30, Some("a".to_string())),
        (40, Some("b".to_string())),
        (60, Some("a".to_string())),
    ]
}

/// Collect + sort the `(id → category)` map from all batches, for the fixture-A partition pin.
fn id_to_category_sorted(batches: &[RecordBatch]) -> Vec<(i64, Option<String>)> {
    let mut pairs: Vec<(i64, Option<String>)> = Vec::new();
    for batch in batches {
        pairs.extend(extract_id_to_category(batch));
    }
    pairs.sort_by_key(|(id, _)| *id);
    pairs
}

/// Load + parse a Java ground-truth rows JSON file as `Vec<ScanRow>`.
fn load_java_rows(path: &Path) -> Vec<ScanRow> {
    let json =
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str::<Vec<ScanRow>>(&json)
        .unwrap_or_else(|e| panic!("parse {}: {e}", path.display()))
}

/// Create the Rust table for a data fixture at `<warehouse>/rust_table`, V2, partitioned by
/// `identity(category)`.
async fn create_write_data_table(catalog: &impl Catalog, table_location: &str) -> Table {
    let namespace = NamespaceIdent::new("interop".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create namespace");
    let creation = TableCreation::builder()
        .name("rust_table".to_string())
        .location(table_location.to_string())
        .schema(write_data_schema())
        .partition_spec(write_data_spec())
        .sort_order(SortOrder::unsorted_order())
        .format_version(FormatVersion::V2)
        .build();
    catalog
        .create_table(&namespace, creation)
        .await
        .expect("create write-data rust_table")
}

/// Create the UNPARTITIONED V2 table for fixture B at `<warehouse>/rust_table`.
async fn create_rewrite_data_table(catalog: &impl Catalog, table_location: &str) -> Table {
    let namespace = NamespaceIdent::new("interop".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create namespace");
    let creation = TableCreation::builder()
        .name("rust_table".to_string())
        .location(table_location.to_string())
        .schema(rewrite_data_schema())
        .sort_order(SortOrder::unsorted_order())
        .format_version(FormatVersion::V2)
        .build();
    catalog
        .create_table(&namespace, creation)
        .await
        .expect("create rewrite-data rust_table")
}

/// Write a REAL unpartitioned parquet DATA file (5 rows: 10..50 with data a..e) for fixture B.
async fn write_unpartitioned_data_file(table: &Table) -> DataFile {
    use iceberg::arrow::schema_to_arrow_schema;

    let schema = table.metadata().current_schema();
    let arrow_schema = Arc::new(schema_to_arrow_schema(schema).expect("schema → arrow"));
    let batch = RecordBatch::try_new(arrow_schema, vec![
        Arc::new(Int64Array::from(vec![10_i64, 20, 30, 40, 50])) as ArrayRef,
        Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])) as ArrayRef,
    ])
    .expect("build unpartitioned data batch");

    let location_gen =
        DefaultLocationGenerator::new(table.metadata().clone()).expect("location generator");
    let file_name_gen = DefaultFileNameGenerator::new(
        "rdata".to_string(),
        Some(uuid::Uuid::now_v7().to_string()),
        DataFileFormat::Parquet,
    );
    let parquet_builder = ParquetWriterBuilder::new(
        parquet::file::properties::WriterProperties::builder().build(),
        schema.clone(),
    );
    let rolling = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_builder,
        table.file_io().clone(),
        location_gen,
        file_name_gen,
    );

    let mut writer = DataFileWriterBuilder::new(rolling)
        .build(None)
        .await
        .expect("build unpartitioned data file writer");
    writer.write(batch).await.expect("write data batch");
    writer
        .close()
        .await
        .expect("close data file writer")
        .into_iter()
        .next()
        .expect("one data file")
}

/// Write a REAL parquet EQUALITY-DELETE file (equality_ids=[1], deletes id=20 and id=40) for fixture B.
/// The schema is the 2-field `{id, data}` table schema; the writer projects to the `id` column.
async fn write_unpartitioned_eq_delete_file(table: &Table) -> DataFile {
    use iceberg::arrow::schema_to_arrow_schema;

    let schema = table.metadata().current_schema();
    let config = EqualityDeleteWriterConfig::new(vec![1], schema.clone())
        .expect("equality-delete writer config (equality_ids=[1])");

    let location_gen =
        DefaultLocationGenerator::new(table.metadata().clone()).expect("location generator");
    let file_name_gen = DefaultFileNameGenerator::new(
        "reqdel".to_string(),
        Some(uuid::Uuid::now_v7().to_string()),
        DataFileFormat::Parquet,
    );
    // The parquet writer must use the PROJECTED schema (just `id`), matching EqDeleteOracle.
    let projected_iceberg_schema = Arc::new(
        iceberg::arrow::arrow_schema_to_schema(config.projected_arrow_schema_ref())
            .expect("projected arrow schema → iceberg schema"),
    );
    let parquet_builder = ParquetWriterBuilder::new(
        parquet::file::properties::WriterProperties::builder().build(),
        projected_iceberg_schema,
    );
    let rolling = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_builder,
        table.file_io().clone(),
        location_gen,
        file_name_gen,
    );

    let mut writer = EqualityDeleteFileWriterBuilder::new(rolling, config)
        .build(None)
        .await
        .expect("build equality-delete writer");

    // A FULL-schema (id, data) batch carrying the two delete keys; the projector keeps only `id`.
    let arrow_schema = Arc::new(schema_to_arrow_schema(schema).expect("schema → arrow"));
    let ids = Int64Array::from(vec![20_i64, 40]);
    let data = StringArray::from(vec!["b", "d"]);
    let batch = RecordBatch::try_new(arrow_schema, vec![
        Arc::new(ids) as ArrayRef,
        Arc::new(data) as ArrayRef,
    ])
    .expect("build equality-delete key batch");
    writer
        .write(batch)
        .await
        .expect("write equality-delete batch");
    writer
        .close()
        .await
        .expect("close equality-delete writer")
        .into_iter()
        .next()
        .expect("one equality-delete file")
}

// ===========================================================================================
// Fixture A GEN — Rust writes the merge_append data table for Java to verify.
// ===========================================================================================

/// Rust performs the SAME merge_append chain as Java's `MergeAppendDataOracle.generate`:
/// fast_append A(cat=a,ids=10/20/30)+B(cat=b,id=40) → set min-count-to-merge=2 →
/// merge_append G(cat=a,id=60), landing `final.metadata.json` for the Java verify step.
/// The real parquet files let Java's `IcebergGenerics` scan actually read and return rows.
#[tokio::test]
async fn test_merge_append_data_gen_rust_writes_java_readable_table() {
    let Some(gen_dir) = merge_append_data_gen_dir() else {
        println!(
            "skipping interop_write_data merge_append GEN — set \
             ICEBERG_INTEROP_MERGE_APPEND_DATA_GEN_DIR \
             (run dev/java-interop/run-interop-write-data.sh)"
        );
        return;
    };

    let warehouse = gen_dir.to_string_lossy().to_string();
    let table_location = format!("{warehouse}/rust_table");
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "interop_merge_append_data_gen",
            HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse.clone())]),
        )
        .await
        .expect("build MemoryCatalog over local FS");

    let table = create_write_data_table(&catalog, &table_location).await;
    let schema = table.metadata().current_schema().clone();
    let bound_spec = table.metadata().default_partition_spec().as_ref().clone();
    let pk_a = partition_key(schema.clone(), bound_spec.clone(), "a");
    let pk_b = partition_key(schema.clone(), bound_spec.clone(), "b");

    // Write real parquet files.
    let file_a = write_data_file(&table, &pk_a, "a", vec![10, 20, 30], vec!["a", "b", "c"]).await;
    let file_b = write_data_file(&table, &pk_b, "b", vec![40], vec!["d"]).await;
    let file_g = write_data_file(&table, &pk_a, "a", vec![60], vec!["g"]).await;

    // s1 fast_append A+B (seq 1).
    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(vec![file_a, file_b])
        .apply(tx)
        .expect("apply fast append A+B");
    let table = tx.commit(&catalog).await.expect("commit s1 fast_append");

    // s2 update_table_properties: set min-count-to-merge=2 (no snapshot — arms the merge in s3).
    let tx = Transaction::new(&table);
    let tx = tx
        .update_table_properties()
        .set(
            "commit.manifest.min-count-to-merge".to_string(),
            "2".to_string(),
        )
        .apply(tx)
        .expect("apply update_table_properties");
    let table = tx.commit(&catalog).await.expect("commit s2 property set");

    // s3 merge_append G (seq 2). With min-count=2 and KB-size manifests vs the 8 MB target, every
    // manifest lands in ONE bin ⇒ the merge fires into ONE merged manifest carrying A+B as Existing
    // plus G as Added. Rust must scan ALL three files (no Existing entry silently dropped).
    let tx = Transaction::new(&table);
    let tx = tx
        .merge_append()
        .add_data_files(vec![file_g])
        .apply(tx)
        .expect("apply merge_append G");
    let table = tx.commit(&catalog).await.expect("commit s3 merge_append");

    // Sanity: Rust's OWN scan should already return all 5 rows before handing to Java.
    let batches: Vec<RecordBatch> = table
        .scan()
        .build()
        .expect("build scan")
        .to_arrow()
        .await
        .expect("scan to_arrow")
        .try_collect()
        .await
        .expect("collect batches");
    let mut rust_rows = Vec::new();
    for batch in &batches {
        rust_rows.extend(extract_rows(batch));
    }
    let rust_rows = sorted_by_id(rust_rows);
    let live_ids: Vec<i64> = rust_rows.iter().map(|r| r.id).collect();
    assert_eq!(
        live_ids,
        vec![10, 20, 30, 40, 60],
        "Rust scan of the merge_append table must yield {{10,20,30,40,60}} (all rows, no deletes)"
    );

    // Pin the IDENTITY-PARTITION column too: the `{id, data}` compare drops `category`, so a
    // partition-routing divergence (a row in the wrong partition) would be invisible to it. Assert
    // each id materialized into the correct partition before handing the table to Java.
    assert_eq!(
        id_to_category_sorted(&batches),
        expected_merge_append_categories(),
        "Rust merge_append must route each row to the correct identity(category) partition \
         (A/G → 'a', B → 'b'); a wrong partition is invisible to the {{id,data}} row compare"
    );

    // Land final metadata at a known path for Java.
    let final_metadata_path = format!("{table_location}/metadata/final.metadata.json");
    table
        .metadata()
        .clone()
        .write_to(table.file_io(), &final_metadata_path)
        .await
        .expect("write final.metadata.json");

    println!(
        "interop_write_data merge_append GEN OK — Rust wrote {table_location} \
         (fast_append A+B, merge_append G → merged manifest, Rust scan = {{10,20,30,40,60}}, \
         partitions A/G→a B→b pinned). Java verify-interop-merge-append-data reads it next."
    );
}

// ===========================================================================================
// Fixture B GEN — Rust writes the rewrite-data table (seq-preservation) for Java to verify.
//
// DESIGN: unpartitioned 2-field schema {id, data}, equality delete (not position delete).
// A position-delete is PATH-BASED: after A→A' the delete on A's path is dangling (A' has a new
// path) and cannot apply. To prove `data_sequence_number` matters for applicability you MUST use
// an equality delete (which applies to data files with data_seq STRICTLY LESS than the delete's
// seq). This is the same design as Java's revised `RewriteFilesDataOracle.generate`.
// ===========================================================================================

/// Rust performs the SAME chain as Java's revised `RewriteFilesDataOracle.generate`:
/// fast_append A (5 rows, seq 1) → row_delta eq-delete (ids 20+40, seq 2) → rewrite {A}→{A'}
/// with `data_sequence_number(1)` (seq 3). A' carries data_seq=1 < eq_del.seq=2, so the delete
/// STILL APPLIES. Live rows = `{10,30,50}`. The transaction for the rewrite is built AFTER the
/// row_delta commit (tx-captured start = row_delta snapshot = semantic twin of Java's explicit
/// `validateFromSnapshot(rowDeltaSnapshotId)`).
#[tokio::test]
async fn test_rewrite_data_gen_rust_writes_java_readable_seq_preserving_table() {
    let Some(gen_dir) = rewrite_data_gen_dir() else {
        println!(
            "skipping interop_write_data rewrite GEN — set ICEBERG_INTEROP_REWRITE_DATA_GEN_DIR \
             (run dev/java-interop/run-interop-write-data.sh)"
        );
        return;
    };

    let warehouse = gen_dir.to_string_lossy().to_string();
    let table_location = format!("{warehouse}/rust_table");
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "interop_rewrite_data_gen",
            HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse.clone())]),
        )
        .await
        .expect("build MemoryCatalog over local FS");

    // Unpartitioned 2-field table for fixture B.
    let table = create_rewrite_data_table(&catalog, &table_location).await;

    // Write real parquet files: A (5 rows) and A' (same 5 rows, different path — the "compacted"
    // replacement). A' carries data_seq=1 after the rewrite.
    let file_a = write_unpartitioned_data_file(&table).await;
    let file_a_prime = write_unpartitioned_data_file(&table).await;

    // b1 fast_append A (seq 1).
    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(vec![file_a.clone()])
        .apply(tx)
        .expect("apply fast append A");
    let table = tx.commit(&catalog).await.expect("commit b1 fast_append");

    // b2 row_delta: equality-delete (equality_ids=[1], deletes ids 20+40) at seq 2.
    // Data-seq ordering rule: the delete applies to data with data_seq STRICTLY LESS than 2.
    let delete_file = write_unpartitioned_eq_delete_file(&table).await;
    assert_eq!(delete_file.content_type(), DataContentType::EqualityDeletes);
    assert_eq!(
        delete_file.equality_ids(),
        Some(vec![1]),
        "equality delete must carry equality_ids=[1]"
    );
    let tx = Transaction::new(&table);
    let tx = tx
        .row_delta()
        .add_deletes(vec![delete_file])
        .apply(tx)
        .expect("apply row_delta eq-delete");
    let table = tx.commit(&catalog).await.expect("commit b2 row_delta");

    // b3 rewrite_files {A}→{A'} preserving data_sequence_number=1. The transaction is created HERE,
    // AFTER the row_delta commit, so its tx-captured starting snapshot IS b2 — the semantic twin of
    // Java's explicit `validateFromSnapshot(rowDeltaSnapshotId)`; the concurrent window is empty.
    // A' is stamped with data_seq=1 < eq_del.seq=2, so the equality-delete STILL APPLIES to A'.
    let tx = Transaction::new(&table);
    let tx = tx
        .rewrite_files(vec![file_a], vec![file_a_prime])
        .data_sequence_number(1)
        .apply(tx)
        .expect("apply rewrite_files {A}→{A'} with data_sequence_number=1");
    let table = tx.commit(&catalog).await.expect("commit b3 rewrite");

    // Sanity: Rust's OWN scan must yield {10,30,50} (ids 20+40 deleted by eq-delete that still
    // applies to A' because A'.data_seq=1 < eq_del.seq=2 — seq-preservation holds).
    let batches: Vec<RecordBatch> = table
        .scan()
        .build()
        .expect("build scan")
        .to_arrow()
        .await
        .expect("scan to_arrow")
        .try_collect()
        .await
        .expect("collect batches");
    let mut rust_rows = Vec::new();
    for batch in &batches {
        rust_rows.extend(extract_rows(batch));
    }
    let rust_rows = sorted_by_id(rust_rows);
    let live_ids: Vec<i64> = rust_rows.iter().map(|r| r.id).collect();
    assert_eq!(
        live_ids,
        vec![10, 30, 50],
        "Rust scan after rewrite must yield {{10,30,50}} — ids 20+40 deleted by eq-delete; \
         A'.data_seq=1 < eq_del.seq=2 (seq-preservation holds)"
    );

    // Land final metadata at a known path for Java.
    let final_metadata_path = format!("{table_location}/metadata/final.metadata.json");
    table
        .metadata()
        .clone()
        .write_to(table.file_io(), &final_metadata_path)
        .await
        .expect("write final.metadata.json");

    println!(
        "interop_write_data rewrite GEN OK — Rust wrote {table_location} \
         (fast_append A, eq-delete ids 20+40, rewrite {{A}}→{{A'}} data_seq=1, \
         Rust scan = {{10,30,50}}). Java verify-interop-rewrite-data reads it next."
    );
}

// ===========================================================================================
// Fixture A comparison — Rust reads Java's ground-truth rows.
// ===========================================================================================

/// Rust reads the JAVA-written merge_append table and compares to `java_merge_append_rows.json`.
///
/// This is DIRECTION 1 of fixture A: Java's `MergeAppendDataOracle.generate` wrote a real table
/// under `<dir>/table`, emitting `java_merge_append_rows.json` as the ground truth. Here Rust
/// loads the SAME table, runs `scan().to_arrow()`, and asserts the live rows equal the JSON.
/// This proves Rust reads a merge-appended Java table (carried Existing entries scan correctly).
#[tokio::test]
async fn test_rust_reads_java_merge_append_data_table() {
    let Some(dir) = merge_append_data_dir() else {
        println!(
            "skipping interop_write_data merge_append D1 — set \
             ICEBERG_INTEROP_MERGE_APPEND_DATA_DIR \
             (run dev/java-interop/run-interop-write-data.sh)"
        );
        return;
    };

    let java_rows = sorted_by_id(load_java_rows(&dir.join("java_merge_append_rows.json")));

    // Load the Java-written table via a local-FS FileIO.
    let metadata_path = dir.join("table/metadata/final.metadata.json");
    let json = std::fs::read_to_string(&metadata_path)
        .unwrap_or_else(|e| panic!("read {}: {e}", metadata_path.display()));
    let metadata: iceberg::spec::TableMetadata = serde_json::from_str(&json)
        .unwrap_or_else(|e| panic!("parse {}: {e}", metadata_path.display()));
    let table = Table::builder()
        .metadata(metadata)
        .metadata_location(metadata_path.to_string_lossy().to_string())
        .identifier(
            TableIdent::from_strs(["interop", "merge_append_data"]).expect("valid identifier"),
        )
        .file_io(FileIO::new_with_fs())
        .build()
        .expect("build table from Java-written final.metadata.json");

    let batches: Vec<RecordBatch> = table
        .scan()
        .build()
        .expect("build table scan")
        .to_arrow()
        .await
        .expect("scan to_arrow")
        .try_collect()
        .await
        .expect("collect scan batches");

    let mut rust_rows = Vec::new();
    for batch in &batches {
        rust_rows.extend(extract_rows(batch));
    }
    let rust_rows = sorted_by_id(rust_rows);

    // Merged manifest must carry ALL Existing entries (A, B) + Added (G): 5 rows total.
    assert_eq!(
        rust_rows.len(),
        5,
        "expected 5 live rows from merge_append table (A:3 + B:1 + G:1, no deletes)"
    );
    assert_eq!(
        rust_rows, java_rows,
        "Rust scan of Java merge_append table must equal Java's IcebergGenerics read (field-for-field)"
    );
    let live_ids: Vec<i64> = rust_rows.iter().map(|r| r.id).collect();
    assert_eq!(
        live_ids,
        vec![10, 20, 30, 40, 60],
        "live id set must be {{10,20,30,40,60}}"
    );

    // Pin the IDENTITY-PARTITION column: Java's `java_merge_append_rows.json` only carries
    // `{id, data}` (not `category`), so the row compare above cannot see a partition divergence.
    // Assert Rust reads each Java-written row's partition correctly.
    assert_eq!(
        id_to_category_sorted(&batches),
        expected_merge_append_categories(),
        "Rust scan of the Java merge_append table must read each row's identity(category) \
         partition correctly (A/G → 'a', B → 'b')"
    );

    println!(
        "interop_write_data merge_append D1 OK — Rust scan of Java table = Java IcebergGenerics \
         read: 5 live rows {{10,20,30,40,60}}, partitions A/G→a B→b (merge boundary carried all \
         Existing entries)"
    );
}

// ===========================================================================================
// Fixture B comparison — Rust reads Java's ground-truth rows (seq-preservation proof).
// ===========================================================================================

/// Rust reads the JAVA-written rewrite-data table and compares to `java_rewrite_data_rows.json`.
///
/// This is DIRECTION 1 of fixture B: Java's revised `RewriteFilesDataOracle.generate` wrote an
/// unpartitioned 2-field table under `<dir>/table`, emitting `java_rewrite_data_rows.json` as the
/// ground truth. Here Rust loads the SAME table, runs `scan().to_arrow()`, and asserts the live
/// rows equal the JSON. The correctness point: ids 20 and 40 must be ABSENT (the equality-delete
/// at seq 2 applied to A' because A' was stamped with `data_sequence_number=1` via
/// `rewriteFiles(.., 1L)` in Java; `A'.data_seq=1 < eq_del.seq=2`).
#[tokio::test]
async fn test_rust_reads_java_rewrite_data_table() {
    let Some(dir) = rewrite_data_dir() else {
        println!(
            "skipping interop_write_data rewrite D1 — set ICEBERG_INTEROP_REWRITE_DATA_DIR \
             (run dev/java-interop/run-interop-write-data.sh)"
        );
        return;
    };

    let java_rows = sorted_by_id(load_java_rows(&dir.join("java_rewrite_data_rows.json")));

    let metadata_path = dir.join("table/metadata/final.metadata.json");
    let json = std::fs::read_to_string(&metadata_path)
        .unwrap_or_else(|e| panic!("read {}: {e}", metadata_path.display()));
    let metadata: iceberg::spec::TableMetadata = serde_json::from_str(&json)
        .unwrap_or_else(|e| panic!("parse {}: {e}", metadata_path.display()));
    let table = Table::builder()
        .metadata(metadata)
        .metadata_location(metadata_path.to_string_lossy().to_string())
        .identifier(TableIdent::from_strs(["interop", "rewrite_data"]).expect("valid identifier"))
        .file_io(FileIO::new_with_fs())
        .build()
        .expect("build table from Java-written final.metadata.json");

    let batches: Vec<RecordBatch> = table
        .scan()
        .build()
        .expect("build table scan")
        .to_arrow()
        .await
        .expect("scan to_arrow")
        .try_collect()
        .await
        .expect("collect scan batches");

    let mut rust_rows = Vec::new();
    for batch in &batches {
        rust_rows.extend(extract_rows(batch));
    }
    let rust_rows = sorted_by_id(rust_rows);

    // ids 20 and 40 must be absent (eq-delete at seq 2 applies to A' at data_seq 1).
    assert!(
        !rust_rows.iter().any(|r| r.id == 20),
        "id 20 (deleted by equality-delete at seq 2) must be ABSENT — A'.data_seq=1 < eq_del.seq=2"
    );
    assert!(
        !rust_rows.iter().any(|r| r.id == 40),
        "id 40 (deleted by equality-delete at seq 2) must be ABSENT — A'.data_seq=1 < eq_del.seq=2"
    );
    assert_eq!(
        rust_rows.len(),
        3,
        "expected 3 live rows after rewrite + eq-delete: {{10,30,50}}"
    );
    assert_eq!(
        rust_rows, java_rows,
        "Rust scan of Java rewrite-data table must equal Java's IcebergGenerics read (field-for-field)"
    );
    let live_ids: Vec<i64> = rust_rows.iter().map(|r| r.id).collect();
    assert_eq!(
        live_ids,
        vec![10, 30, 50],
        "live id set must be {{10,30,50}}"
    );

    println!(
        "interop_write_data rewrite D1 OK — Rust scan of Java table = Java IcebergGenerics read: \
         3 live rows {{10,30,50}} (ids 20+40 absent; seq-preservation: A'.data_seq=1 < eq_del.seq=2)"
    );
}
