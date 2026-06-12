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

//! DATA-LEVEL write-action interop (sprint increments S1 + W1 + W2) — `MergeAppend`,
//! `RewriteFiles`, `OverwriteFiles`, `DeleteFiles`, `ReplacePartitions`, and partitioned
//! `RewriteFiles` proven against Java's `IcebergGenerics` production scan.
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
//! ## Fixture C — `overwrite_files` data-level
//!
//! V2 partitioned table (identity(category), same 3-field schema as fixture A).
//! Rust performs the SAME chain as Java's `OverwriteFilesDataOracle.generate`:
//! - `fast_append` A (cat=a, ids 10/20/30, data a/b/c) and B (cat=b, id 40, data d) — seq 1
//! - `overwrite_files` DELETE B + ADD B' (cat=b, id 41, data d') — seq 2, operation=overwrite
//!
//! Correctness point: B (id=40) is GONE; B' (id=41) is present; A's rows (10,20,30) INTACT.
//! The partition column is pinned: a wrong-partition write of B' to cat="a" would be invisible to
//! the `{id, data}` row compare but is caught by the `id_to_category_sorted` assertion.
//! Java verifies: `IcebergGenerics.read(rust_table)` must yield `{(10,a),(20,b),(30,c),(41,d')}`.
//!
//! ## Fixture D — `delete_files` data-level
//!
//! V2 partitioned table (identity(category), same 3-field schema as fixture A).
//! Rust performs the SAME chain as Java's `DeleteFilesDataOracle.generate`:
//! - `fast_append` A (cat=a, ids 10/20/30, data a/b/c), B (cat=b, id 40, data d),
//!   C_file (cat=a, id 50, data e) — seq 1
//! - `delete_files` {B} by path — seq 2, operation=delete
//!
//! Correctness point: B (cat=b, id=40) is GONE; A and C_file (cat=a) are INTACT.
//! Java verifies: `IcebergGenerics.read(rust_table)` must yield `{(10,a),(20,b),(30,c),(50,e)}`.
//!
//! All fixtures use REAL parquet files written via the production Rust writers (`DataFileWriter`).
//! The tables land at `<gen_dir>/rust_table` with `metadata/final.metadata.json`; Java's
//! `verify-interop-*` modes read them. The S3 partition-projection lesson is binding: every fixture
//! pins the `category` column explicitly via `id_to_category_sorted` to catch wrong-partition writes.
//!
//! ## Fixture E — `replace_partitions` data-level
//!
//! V2 partitioned table (identity(category), same 3-field schema as fixture A).
//! Rust performs the SAME chain as Java's `ReplacePartitionsDataOracle.generate`:
//! - `fast_append` A (cat=a, ids 10/20/30, data a/b/c) and B (cat=b, id 40, data d) — seq 1
//! - `replace_partitions` E_new (cat=a, id=11, data="a'") — seq 2, operation=overwrite,
//!   summary replace-partitions=true; this REPLACES ALL of partition a (A deleted) while
//!   partition b (B) carries forward with EXISTING manifest status
//!
//! Correctness point: A's rows (10,20,30) are ALL GONE; E_new (id=11) is present; B (id=40)
//! is byte-untouched. Additionally: B's file carries forward with EXISTING manifest status
//! (not re-added as ADDED) — proven by the Java oracle's manifest-entry assertion (step 3f).
//! Java verifies: `IcebergGenerics.read(rust_table)` must yield `{(11,a'),(40,d)}`.
//!
//! ## Fixture F — partitioned `rewrite_files` with outstanding eq-delete
//!
//! V2 partitioned table (identity(category), same 3-field schema as fixture A).
//! Rust performs the SAME chain as Java's `PartitionedRewriteFilesDataOracle.generate`:
//! - `fast_append` A (cat=a, ids 10/20/30, data a/b/c) and B (cat=b, id 40, data d) — seq 1
//! - `row_delta` equality-delete (equality_ids=[1], deletes id=20, SCOPED to partition a) — seq 2
//! - `rewrite_files` {A}→{A'} with `data_sequence_number(1)` — seq 3
//!
//! Correctness point: A' is stamped with `data_sequence_number=1` (the replaced file's seq).
//! The equality-delete has seq 2. `A'.data_seq=1 < eq_del.seq=2`, so the delete STILL APPLIES.
//! id=20 must be ABSENT; id=10 and id=30 survive (cat=a); B (id=40, cat=b) is untouched.
//! Java verifies: `IcebergGenerics.read(rust_table)` must yield `{(10,a),(30,c),(40,d)}`.
//!
//! ## Fixture G — multi-bin `merge_append` data-level (W3)
//!
//! V2 partitioned table (identity(category), same 3-field schema as fixture A).
//! Four SEPARATE fast_appends build up four separate manifests m1..m4:
//! - fast_append A (cat=a, ids 10/20/30, data a/b/c) — seq 1, manifest m1
//! - fast_append B (cat=b, id 40, data d)             — seq 2, manifest m2
//! - fast_append C1 (cat=a, id 50, data e)            — seq 3, manifest m3
//! - fast_append C2 (cat=b, id 55, data f)            — seq 4, manifest m4
//!
//! After the four appends, the actual manifest sizes are measured from the manifest-list.
//! `target-size-bytes` is set to `max_manifest_size * 2 + 1` so that `pack_end` fits exactly
//! 2 manifests per bin. With `min-count-to-merge = 2` and 4 existing manifests, `pack_end`
//! yields 2 bins of 2 → both satisfy min-count → both MERGE → ≥2 merged manifests output.
//!
//! Then `merge_append` G (cat=a, id 60, data g) fires the multi-bin merge (seq 5).
//!
//! Correctness point: all 7 rows must survive: A(10/20/30) + B(40) + C1(50) + C2(55) + G(60).
//! The bin-count assertion (`≥2 manifests with existing_files_count > 0`) proves the multi-bin
//! merge path fired — not just the single-bin path covered by fixture A.
//! Java verifies: `IcebergGenerics.read(rust_table)` must yield
//! `{(10,a),(20,b),(30,c),(40,d),(50,e),(55,f),(60,g)}` with the partition pin.
//!
//! GATED on env vars (all fourteen unset ⇒ clean no-ops; offline `cargo test` gate stays green):
//!
//! - `ICEBERG_INTEROP_MERGE_APPEND_DATA_GEN_DIR`       — fixture A GEN (Rust writes)
//! - `ICEBERG_INTEROP_MERGE_APPEND_DATA_DIR`           — fixture A comparison (Rust reads Java rows)
//! - `ICEBERG_INTEROP_REWRITE_DATA_GEN_DIR`            — fixture B GEN (Rust writes)
//! - `ICEBERG_INTEROP_REWRITE_DATA_DIR`                — fixture B comparison (Rust reads Java rows)
//! - `ICEBERG_INTEROP_OVERWRITE_DATA_GEN_DIR`          — fixture C GEN (Rust writes)
//! - `ICEBERG_INTEROP_OVERWRITE_DATA_DIR`              — fixture C comparison (Rust reads Java rows)
//! - `ICEBERG_INTEROP_DELETE_DATA_GEN_DIR`             — fixture D GEN (Rust writes)
//! - `ICEBERG_INTEROP_DELETE_DATA_DIR`                 — fixture D comparison (Rust reads Java rows)
//! - `ICEBERG_INTEROP_REPLACE_PARTITIONS_DATA_GEN_DIR` — fixture E GEN (Rust writes)
//! - `ICEBERG_INTEROP_REPLACE_PARTITIONS_DATA_DIR`     — fixture E comparison (Rust reads Java rows)
//! - `ICEBERG_INTEROP_PARTITIONED_REWRITE_DATA_GEN_DIR`— fixture F GEN (Rust writes)
//! - `ICEBERG_INTEROP_PARTITIONED_REWRITE_DATA_DIR`    — fixture F comparison (Rust reads Java rows)
//! - `ICEBERG_INTEROP_MULTI_BIN_MERGE_DATA_GEN_DIR`    — fixture G GEN (Rust writes)
//! - `ICEBERG_INTEROP_MULTI_BIN_MERGE_DATA_DIR`        — fixture G comparison (Rust reads Java rows)

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
    DataContentType, DataFile, DataFileFormat, FormatVersion, Literal, ManifestContentType,
    NestedField, PartitionKey, PartitionSpec, PrimitiveType, Schema, SchemaRef, SortOrder, Struct,
    TableProperties, Transform, Type, UnboundPartitionSpec,
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

fn overwrite_data_gen_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_OVERWRITE_DATA_GEN_DIR")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

fn overwrite_data_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_OVERWRITE_DATA_DIR")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

fn delete_data_gen_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_DELETE_DATA_GEN_DIR")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

fn delete_data_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_DELETE_DATA_DIR")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

fn replace_partitions_data_gen_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_REPLACE_PARTITIONS_DATA_GEN_DIR")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

fn replace_partitions_data_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_REPLACE_PARTITIONS_DATA_DIR")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

fn partitioned_rewrite_data_gen_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_PARTITIONED_REWRITE_DATA_GEN_DIR")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

fn partitioned_rewrite_data_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_PARTITIONED_REWRITE_DATA_DIR")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

fn multi_bin_merge_data_gen_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_MULTI_BIN_MERGE_DATA_GEN_DIR")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

fn multi_bin_merge_data_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_MULTI_BIN_MERGE_DATA_DIR")
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

/// The expected `(id → category)` partition routing for fixture C (overwrite_files):
/// A's rows (10,20,30) are `category="a"`; B' (41) is `category="b"`. B (40) is ABSENT.
/// Sorted by id for a stable compare.
///
/// S3 partition-projection lesson: a scan that drops the `category` column cannot detect a
/// wrong-partition write of B' to `category="a"`. This pin makes that class of regression visible.
fn expected_overwrite_categories() -> Vec<(i64, Option<String>)> {
    vec![
        (10, Some("a".to_string())),
        (20, Some("a".to_string())),
        (30, Some("a".to_string())),
        (41, Some("b".to_string())),
    ]
}

/// The expected `(id → category)` partition routing for fixture D (delete_files):
/// A's rows (10,20,30) and C_file (50) are `category="a"`; B (40) is ABSENT (deleted).
/// Sorted by id for a stable compare.
fn expected_delete_categories() -> Vec<(i64, Option<String>)> {
    vec![
        (10, Some("a".to_string())),
        (20, Some("a".to_string())),
        (30, Some("a".to_string())),
        (50, Some("a".to_string())),
    ]
}

/// The expected `(id → category)` partition routing for fixture E (replace_partitions):
/// E_new (id=11) is `category="a"` (it replaced ALL of partition a); B (id=40) is `category="b"`.
/// A's rows (10,20,30) are ABSENT (replaced). Sorted by id for a stable compare.
///
/// S3 partition-projection lesson: a wrong-partition write of E_new to `category="b"` would pass
/// the `{id,data}` compare but is caught here (category would be `{11="b", 40="b"}` instead of
/// `{11="a", 40="b"}`).
fn expected_replace_partitions_categories() -> Vec<(i64, Option<String>)> {
    vec![(11, Some("a".to_string())), (40, Some("b".to_string()))]
}

/// The expected `(id → category)` partition routing for fixture F (partitioned rewrite_files):
/// A's surviving rows (10,30) are `category="a"` (id=20 was deleted by the eq-delete);
/// B (id=40) is `category="b"` (untouched by both the eq-delete and the rewrite).
/// Sorted by id for a stable compare.
fn expected_partitioned_rewrite_categories() -> Vec<(i64, Option<String>)> {
    vec![
        (10, Some("a".to_string())),
        (30, Some("a".to_string())),
        (40, Some("b".to_string())),
    ]
}

/// The expected `(id → category)` partition routing for fixture G (multi-bin merge_append):
/// a-rows = {10,20,30,50,60}; b-rows = {40,55}. All 7 rows must survive the multi-bin merge
/// and be read from the correct partition (S3 partition-projection lesson binding).
///
/// A wrong-partition write (e.g. C2 routed to cat="a" instead of cat="b") passes the `{id,data}`
/// row compare but is caught here.
fn expected_multi_bin_merge_append_categories() -> Vec<(i64, Option<String>)> {
    vec![
        (10, Some("a".to_string())),
        (20, Some("a".to_string())),
        (30, Some("a".to_string())),
        (40, Some("b".to_string())),
        (50, Some("a".to_string())),
        (55, Some("b".to_string())),
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

/// Write a REAL parquet EQUALITY-DELETE file for fixture F, SCOPED to a partition.
/// equality_ids=[1] (the `id` field); deletes ONLY id=20; partition key = category="a".
/// The schema is the 3-field `{id, category, data}` table schema; the writer projects to `id`.
async fn write_partitioned_eq_delete_file(table: &Table, partition_key: &PartitionKey) -> DataFile {
    use iceberg::arrow::schema_to_arrow_schema;

    let schema = table.metadata().current_schema();
    let config = EqualityDeleteWriterConfig::new(vec![1], schema.clone())
        .expect("equality-delete writer config (equality_ids=[1])");

    let location_gen =
        DefaultLocationGenerator::new(table.metadata().clone()).expect("location generator");
    let file_name_gen = DefaultFileNameGenerator::new(
        "pfeqdel".to_string(),
        Some(uuid::Uuid::now_v7().to_string()),
        DataFileFormat::Parquet,
    );
    // The parquet writer must use the PROJECTED schema (just `id`).
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
        .build(Some(partition_key.clone()))
        .await
        .expect("build partitioned equality-delete writer");

    // Full-schema batch ({id, category, data}) with just id=20; projector keeps only `id`.
    let arrow_schema = Arc::new(schema_to_arrow_schema(schema).expect("schema → arrow"));
    let ids = Int64Array::from(vec![20_i64]);
    let categories = StringArray::from(vec!["a"]);
    let data_vals = StringArray::from(vec!["b"]);
    let batch = RecordBatch::try_new(arrow_schema, vec![
        Arc::new(ids) as ArrayRef,
        Arc::new(categories) as ArrayRef,
        Arc::new(data_vals) as ArrayRef,
    ])
    .expect("build partitioned equality-delete key batch");
    writer
        .write(batch)
        .await
        .expect("write partitioned equality-delete batch");
    writer
        .close()
        .await
        .expect("close partitioned equality-delete writer")
        .into_iter()
        .next()
        .expect("one equality-delete file")
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

// ===========================================================================================
// Fixture C GEN — Rust writes the overwrite_files data table for Java to verify.
// ===========================================================================================

/// Rust performs the SAME chain as Java's `OverwriteFilesDataOracle.generate`:
/// fast_append A(cat=a,ids=10/20/30)+B(cat=b,id=40) → overwrite_files DELETE B ADD B'(cat=b,id=41,d'),
/// landing `final.metadata.json` for the Java verify step.
/// Correctness point: B (id=40) is GONE; B' (id=41) is PRESENT; A's rows INTACT.
/// The partition column is pinned: a wrong-partition write of B' is caught by the category assert.
#[tokio::test]
async fn test_overwrite_data_gen_rust_writes_java_readable_table() {
    let Some(gen_dir) = overwrite_data_gen_dir() else {
        println!(
            "skipping interop_write_data overwrite GEN — set \
             ICEBERG_INTEROP_OVERWRITE_DATA_GEN_DIR \
             (run dev/java-interop/run-interop-write-data.sh)"
        );
        return;
    };

    let warehouse = gen_dir.to_string_lossy().to_string();
    let table_location = format!("{warehouse}/rust_table");
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "interop_overwrite_data_gen",
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
    let file_b_prime = write_data_file(&table, &pk_b, "b", vec![41], vec!["d'"]).await;

    // s1 fast_append A+B (seq 1).
    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(vec![file_a, file_b.clone()])
        .apply(tx)
        .expect("apply fast append A+B");
    let table = tx.commit(&catalog).await.expect("commit s1 fast_append");

    // s2 overwrite_files: DELETE B, ADD B' (seq 2, operation = overwrite).
    let tx = Transaction::new(&table);
    let tx = tx
        .overwrite_files()
        .delete_data_files(vec![file_b])
        .add_file(file_b_prime)
        .apply(tx)
        .expect("apply overwrite_files DELETE B ADD B'");
    let table = tx
        .commit(&catalog)
        .await
        .expect("commit s2 overwrite_files");

    // Sanity: Rust's OWN scan must yield {10,20,30,41} (B gone, B' present, A intact).
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
        vec![10, 20, 30, 41],
        "Rust scan of the overwrite table must yield {{10,20,30,41}} (B id=40 gone, B' id=41 present)"
    );
    assert!(
        !live_ids.contains(&40),
        "id 40 (B, deleted by overwrite_files) must be ABSENT from the Rust scan"
    );

    // S3 partition-projection pin: B' must be in cat="b", not misrouted to cat="a".
    assert_eq!(
        id_to_category_sorted(&batches),
        expected_overwrite_categories(),
        "Rust overwrite must route B' (id=41) to identity(category)='b', not misroute to 'a'; \
         the {{id,data}} row compare alone is blind to wrong-partition writes"
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
        "interop_write_data overwrite GEN OK — Rust wrote {table_location} \
         (fast_append A+B, overwrite DELETE B ADD B', Rust scan = {{10,20,30,41}}, \
         B'→b pinned). Java verify-interop-overwrite-data reads it next."
    );
}

// ===========================================================================================
// Fixture C comparison — Rust reads Java's ground-truth rows.
// ===========================================================================================

/// Rust reads the JAVA-written overwrite table and compares to `java_overwrite_data_rows.json`.
///
/// Direction 1 of fixture C: Java's `OverwriteFilesDataOracle.generate` wrote the table under
/// `<dir>/table`, emitting `java_overwrite_data_rows.json`. Rust loads the SAME table, runs
/// `scan().to_arrow()`, and asserts the live rows equal the JSON. Correctness point: id=40 absent,
/// id=41 present, A rows intact. The partition column is pinned via `expected_overwrite_categories`.
#[tokio::test]
async fn test_rust_reads_java_overwrite_data_table() {
    let Some(dir) = overwrite_data_dir() else {
        println!(
            "skipping interop_write_data overwrite D1 — set \
             ICEBERG_INTEROP_OVERWRITE_DATA_DIR \
             (run dev/java-interop/run-interop-write-data.sh)"
        );
        return;
    };

    let java_rows = sorted_by_id(load_java_rows(&dir.join("java_overwrite_data_rows.json")));

    let metadata_path = dir.join("table/metadata/final.metadata.json");
    let json = std::fs::read_to_string(&metadata_path)
        .unwrap_or_else(|e| panic!("read {}: {e}", metadata_path.display()));
    let metadata: iceberg::spec::TableMetadata = serde_json::from_str(&json)
        .unwrap_or_else(|e| panic!("parse {}: {e}", metadata_path.display()));
    let table = Table::builder()
        .metadata(metadata)
        .metadata_location(metadata_path.to_string_lossy().to_string())
        .identifier(TableIdent::from_strs(["interop", "overwrite_data"]).expect("valid identifier"))
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

    // id=40 (B, deleted by overwrite_files) must be absent.
    assert!(
        !rust_rows.iter().any(|r| r.id == 40),
        "id 40 (B, deleted by overwrite_files) must be ABSENT from the Rust scan"
    );
    // id=41 (B', added by overwrite_files) must be present.
    assert!(
        rust_rows.iter().any(|r| r.id == 41),
        "id 41 (B', added by overwrite_files) must be PRESENT in the Rust scan"
    );
    assert_eq!(
        rust_rows.len(),
        4,
        "expected 4 live rows (A:3 + B':1; B deleted)"
    );
    assert_eq!(
        rust_rows, java_rows,
        "Rust scan of Java overwrite table must equal Java's IcebergGenerics read (field-for-field)"
    );
    let live_ids: Vec<i64> = rust_rows.iter().map(|r| r.id).collect();
    assert_eq!(
        live_ids,
        vec![10, 20, 30, 41],
        "live id set must be {{10,20,30,41}}"
    );

    // S3 partition-projection pin.
    assert_eq!(
        id_to_category_sorted(&batches),
        expected_overwrite_categories(),
        "Rust scan of Java overwrite table must read each row's identity(category) correctly \
         (A→a, B'→b); the {{id,data}} row compare alone cannot catch a wrong-partition Java write"
    );

    println!(
        "interop_write_data overwrite D1 OK — Rust scan of Java table = Java IcebergGenerics read: \
         4 live rows {{10,20,30,41}} (id=40 absent, id=41 present, A intact, B'→b pinned)"
    );
}

// ===========================================================================================
// Fixture D GEN — Rust writes the delete_files data table for Java to verify.
// ===========================================================================================

/// Rust performs the SAME chain as Java's `DeleteFilesDataOracle.generate`:
/// fast_append A(cat=a,ids=10/20/30)+B(cat=b,id=40)+C_file(cat=a,id=50,e) →
/// delete_files {B} by path, landing `final.metadata.json` for the Java verify step.
/// Correctness point: B (cat=b, id=40) is GONE; A and C_file (cat=a) INTACT.
/// The partition column is pinned: B's cat="b" rows must be absent; A+C rows must be in cat="a".
#[tokio::test]
async fn test_delete_data_gen_rust_writes_java_readable_table() {
    let Some(gen_dir) = delete_data_gen_dir() else {
        println!(
            "skipping interop_write_data delete GEN — set \
             ICEBERG_INTEROP_DELETE_DATA_GEN_DIR \
             (run dev/java-interop/run-interop-write-data.sh)"
        );
        return;
    };

    let warehouse = gen_dir.to_string_lossy().to_string();
    let table_location = format!("{warehouse}/rust_table");
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "interop_delete_data_gen",
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
    let file_c = write_data_file(&table, &pk_a, "a", vec![50], vec!["e"]).await;

    // s1 fast_append A+B+C_file (seq 1).
    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(vec![file_a, file_b.clone(), file_c])
        .apply(tx)
        .expect("apply fast append A+B+C");
    let table = tx.commit(&catalog).await.expect("commit s1 fast_append");

    // s2 delete_files {B} by path (seq 2, operation = delete).
    let tx = Transaction::new(&table);
    let tx = tx
        .delete_files()
        .delete_data_files(vec![file_b])
        .apply(tx)
        .expect("apply delete_files {B}");
    let table = tx.commit(&catalog).await.expect("commit s2 delete_files");

    // Sanity: Rust's OWN scan must yield {10,20,30,50} (B gone, A + C_file intact).
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
        vec![10, 20, 30, 50],
        "Rust scan of the delete table must yield {{10,20,30,50}} (B id=40 gone, A + C_file intact)"
    );
    assert!(
        !live_ids.contains(&40),
        "id 40 (B, deleted by delete_files) must be ABSENT from the Rust scan"
    );

    // S3 partition-projection pin: all surviving rows must be in cat="a" (B's cat="b" is gone).
    assert_eq!(
        id_to_category_sorted(&batches),
        expected_delete_categories(),
        "Rust delete_files must leave rows 10/20/30/50 in category='a' and id=40 absent; \
         the {{id,data}} row compare alone cannot catch a wrong-partition residue"
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
        "interop_write_data delete GEN OK — Rust wrote {table_location} \
         (fast_append A+B+C, delete_files B, Rust scan = {{10,20,30,50}}, \
         id=40 absent, A+C→a pinned). Java verify-interop-delete-data reads it next."
    );
}

// ===========================================================================================
// Fixture D comparison — Rust reads Java's ground-truth rows.
// ===========================================================================================

/// Rust reads the JAVA-written delete table and compares to `java_delete_data_rows.json`.
///
/// Direction 1 of fixture D: Java's `DeleteFilesDataOracle.generate` wrote the table under
/// `<dir>/table`, emitting `java_delete_data_rows.json`. Rust loads the SAME table, runs
/// `scan().to_arrow()`, and asserts the live rows equal the JSON. Correctness point: id=40 absent,
/// A and C_file rows intact. The partition column is pinned via `expected_delete_categories`.
#[tokio::test]
async fn test_rust_reads_java_delete_data_table() {
    let Some(dir) = delete_data_dir() else {
        println!(
            "skipping interop_write_data delete D1 — set ICEBERG_INTEROP_DELETE_DATA_DIR \
             (run dev/java-interop/run-interop-write-data.sh)"
        );
        return;
    };

    let java_rows = sorted_by_id(load_java_rows(&dir.join("java_delete_data_rows.json")));

    let metadata_path = dir.join("table/metadata/final.metadata.json");
    let json = std::fs::read_to_string(&metadata_path)
        .unwrap_or_else(|e| panic!("read {}: {e}", metadata_path.display()));
    let metadata: iceberg::spec::TableMetadata = serde_json::from_str(&json)
        .unwrap_or_else(|e| panic!("parse {}: {e}", metadata_path.display()));
    let table = Table::builder()
        .metadata(metadata)
        .metadata_location(metadata_path.to_string_lossy().to_string())
        .identifier(TableIdent::from_strs(["interop", "delete_data"]).expect("valid identifier"))
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

    // id=40 (B, deleted by delete_files) must be absent.
    assert!(
        !rust_rows.iter().any(|r| r.id == 40),
        "id 40 (B, deleted by delete_files) must be ABSENT from the Rust scan"
    );
    assert_eq!(
        rust_rows.len(),
        4,
        "expected 4 live rows (A:3 + C_file:1; B deleted)"
    );
    assert_eq!(
        rust_rows, java_rows,
        "Rust scan of Java delete table must equal Java's IcebergGenerics read (field-for-field)"
    );
    let live_ids: Vec<i64> = rust_rows.iter().map(|r| r.id).collect();
    assert_eq!(
        live_ids,
        vec![10, 20, 30, 50],
        "live id set must be {{10,20,30,50}}"
    );

    // S3 partition-projection pin.
    assert_eq!(
        id_to_category_sorted(&batches),
        expected_delete_categories(),
        "Rust scan of Java delete table must read each row's identity(category) correctly \
         (A+C_file→a, B absent); the {{id,data}} compare alone cannot catch a wrong-partition residue"
    );

    println!(
        "interop_write_data delete D1 OK — Rust scan of Java table = Java IcebergGenerics read: \
         4 live rows {{10,20,30,50}} (id=40 absent, A+C_file intact, all in cat='a')"
    );
}

// ===========================================================================================
// Fixture E GEN — Rust writes the replace_partitions data table for Java to verify.
//
// Chain: fast_append A(cat=a,10/20/30)+B(cat=b,40) → replace_partitions E_new(cat=a,11,"a'").
// ALL of partition a is replaced (A deleted); partition b (B) carries forward with EXISTING
// manifest status. Live set: {(11,"a'"),(40,"d")}.
// ===========================================================================================

/// Rust performs the SAME chain as Java's `ReplacePartitionsDataOracle.generate`:
/// fast_append A+B (seq 1) → replace_partitions E_new(cat=a, id=11, data="a'") (seq 2).
/// A's rows (10,20,30) must be ABSENT; E_new (id=11) and B (id=40) must be PRESENT.
/// The partition column is pinned: a wrong-partition write of E_new to cat="b" would be
/// caught by the category assertion but invisible to the `{id,data}` row compare.
#[tokio::test]
async fn test_replace_partitions_data_gen_rust_writes_java_readable_table() {
    let Some(gen_dir) = replace_partitions_data_gen_dir() else {
        println!(
            "skipping interop_write_data replace_partitions GEN — set \
             ICEBERG_INTEROP_REPLACE_PARTITIONS_DATA_GEN_DIR \
             (run dev/java-interop/run-interop-write-data.sh)"
        );
        return;
    };

    let warehouse = gen_dir.to_string_lossy().to_string();
    let table_location = format!("{warehouse}/rust_table");
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "interop_replace_partitions_data_gen",
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
    let file_e_new = write_data_file(&table, &pk_a, "a", vec![11], vec!["a'"]).await;

    // e1 fast_append A+B (seq 1).
    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(vec![file_a, file_b])
        .apply(tx)
        .expect("apply fast append A+B");
    let table = tx.commit(&catalog).await.expect("commit e1 fast_append");

    // e2 replace_partitions: ADD E_new (cat=a, id=11, data="a'"). This REPLACES all of partition a
    // (A is deleted) while partition b (B) carries forward untouched with EXISTING manifest status.
    let tx = Transaction::new(&table);
    let tx = tx
        .replace_partitions()
        .add_file(file_e_new)
        .apply(tx)
        .expect("apply replace_partitions E_new");
    let table = tx
        .commit(&catalog)
        .await
        .expect("commit e2 replace_partitions");

    // Sanity: Rust's OWN scan must yield {11,40} (A gone, E_new present, B intact).
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
        vec![11, 40],
        "Rust scan of the replace_partitions table must yield {{11,40}} \
         (A's rows 10/20/30 replaced, E_new id=11 present, B id=40 intact)"
    );
    assert!(
        !live_ids.contains(&10) && !live_ids.contains(&20) && !live_ids.contains(&30),
        "A's rows (10,20,30) must all be ABSENT after replace_partitions replaced partition a"
    );

    // S3 partition-projection pin: E_new must be in cat="a", not misrouted to cat="b".
    assert_eq!(
        id_to_category_sorted(&batches),
        expected_replace_partitions_categories(),
        "Rust replace_partitions must route E_new (id=11) to identity(category)='a'; B (id=40) \
         stays in 'b'; a wrong-partition write of E_new is invisible to the {{id,data}} row compare"
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
        "interop_write_data replace_partitions GEN OK — Rust wrote {table_location} \
         (fast_append A+B, replace_partitions E_new, Rust scan = {{11,40}}, \
         E_new→a B→b pinned). Java verify-interop-replace-partitions-data reads it next."
    );
}

// ===========================================================================================
// Fixture E comparison — Rust reads Java's ground-truth rows.
// ===========================================================================================

/// Rust reads the JAVA-written replace_partitions table and compares to
/// `java_replace_partitions_rows.json`.
///
/// Direction 1 of fixture E: Java's `ReplacePartitionsDataOracle.generate` wrote the table under
/// `<dir>/table`, emitting `java_replace_partitions_rows.json`. Rust loads the SAME table, runs
/// `scan().to_arrow()`, and asserts the live rows equal the JSON. Correctness point: A's ids
/// (10,20,30) absent; E_new (id=11) and B (id=40) present. The partition column is pinned.
#[tokio::test]
async fn test_rust_reads_java_replace_partitions_data_table() {
    let Some(dir) = replace_partitions_data_dir() else {
        println!(
            "skipping interop_write_data replace_partitions D1 — set \
             ICEBERG_INTEROP_REPLACE_PARTITIONS_DATA_DIR \
             (run dev/java-interop/run-interop-write-data.sh)"
        );
        return;
    };

    let java_rows = sorted_by_id(load_java_rows(
        &dir.join("java_replace_partitions_rows.json"),
    ));

    let metadata_path = dir.join("table/metadata/final.metadata.json");
    let json = std::fs::read_to_string(&metadata_path)
        .unwrap_or_else(|e| panic!("read {}: {e}", metadata_path.display()));
    let metadata: iceberg::spec::TableMetadata = serde_json::from_str(&json)
        .unwrap_or_else(|e| panic!("parse {}: {e}", metadata_path.display()));
    let table = Table::builder()
        .metadata(metadata)
        .metadata_location(metadata_path.to_string_lossy().to_string())
        .identifier(
            TableIdent::from_strs(["interop", "replace_partitions_data"])
                .expect("valid identifier"),
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

    // A's rows (10,20,30) must be absent (partition a was replaced).
    for absent_id in [10, 20, 30] {
        assert!(
            !rust_rows.iter().any(|r| r.id == absent_id),
            "id {absent_id} (A, replaced by replace_partitions) must be ABSENT from the Rust scan"
        );
    }
    // E_new (id=11) and B (id=40) must be present.
    assert!(
        rust_rows.iter().any(|r| r.id == 11),
        "id 11 (E_new, added by replace_partitions) must be PRESENT in the Rust scan"
    );
    assert!(
        rust_rows.iter().any(|r| r.id == 40),
        "id 40 (B, untouched by replace_partitions) must be PRESENT in the Rust scan"
    );
    assert_eq!(
        rust_rows.len(),
        2,
        "expected 2 live rows (E_new + B; A's 3 rows replaced)"
    );
    assert_eq!(
        rust_rows, java_rows,
        "Rust scan of Java replace_partitions table must equal Java's IcebergGenerics read (field-for-field)"
    );
    let live_ids: Vec<i64> = rust_rows.iter().map(|r| r.id).collect();
    assert_eq!(live_ids, vec![11, 40], "live id set must be {{11,40}}");

    // S3 partition-projection pin.
    assert_eq!(
        id_to_category_sorted(&batches),
        expected_replace_partitions_categories(),
        "Rust scan of Java replace_partitions table must read each row's identity(category) \
         correctly (E_new→a, B→b); the {{id,data}} compare alone cannot catch a wrong-partition write"
    );

    println!(
        "interop_write_data replace_partitions D1 OK — Rust scan of Java table = Java IcebergGenerics \
         read: 2 live rows {{11,40}} (A's rows 10/20/30 absent; E_new→a, B→b pinned)"
    );
}

// ===========================================================================================
// Fixture F GEN — Rust writes the partitioned rewrite_files table for Java to verify.
//
// Chain: fast_append A(cat=a,10/20/30)+B(cat=b,40) → row_delta eq-delete(cat=a, id=20, seq 2)
// → rewrite {A}→{A'} data_seq=1 (seq 3). Eq-delete still applies to A' (1 < 2). B untouched.
// Live set: {(10,"a"),(30,"c"),(40,"d")}.
// ===========================================================================================

/// Rust performs the SAME chain as Java's `PartitionedRewriteFilesDataOracle.generate`:
/// fast_append A+B (seq 1) → row_delta eq-delete(cat=a, id=20) (seq 2) →
/// rewrite {A}→{A'} with `data_sequence_number(1)` (seq 3).
/// Correctness point: A' has data_seq=1 < eq_del.seq=2, so id=20 STILL DELETED after the rewrite.
/// B (id=40, cat=b) is untouched by both the eq-delete (scoped to cat=a) and the rewrite.
/// Live rows = {(10,a),(30,c),(40,d)}.
#[tokio::test]
async fn test_partitioned_rewrite_data_gen_rust_writes_java_readable_table() {
    let Some(gen_dir) = partitioned_rewrite_data_gen_dir() else {
        println!(
            "skipping interop_write_data partitioned_rewrite GEN — set \
             ICEBERG_INTEROP_PARTITIONED_REWRITE_DATA_GEN_DIR \
             (run dev/java-interop/run-interop-write-data.sh)"
        );
        return;
    };

    let warehouse = gen_dir.to_string_lossy().to_string();
    let table_location = format!("{warehouse}/rust_table");
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "interop_partitioned_rewrite_data_gen",
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
    // A': same logical rows as A but a different path (compacted). After the rewrite A' carries
    // data_seq=1 (the replaced file's seq) < eq_del.seq=2 → eq-delete STILL APPLIES to A'.
    let file_a_prime =
        write_data_file(&table, &pk_a, "a", vec![10, 20, 30], vec!["a", "b", "c"]).await;

    // f1 fast_append A+B (seq 1).
    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(vec![file_a.clone(), file_b])
        .apply(tx)
        .expect("apply fast append A+B");
    let table = tx.commit(&catalog).await.expect("commit f1 fast_append");

    // f2 row_delta: equality-delete scoped to partition a (equality_ids=[1], deletes id=20, seq 2).
    // Data-seq ordering rule: eq-delete applies to data files with data_seq STRICTLY LESS THAN 2.
    let delete_file = write_partitioned_eq_delete_file(&table, &pk_a).await;
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
        .expect("apply row_delta eq-delete scoped to partition a");
    let table = tx.commit(&catalog).await.expect("commit f2 row_delta");

    // f3 rewrite_files {A}→{A'} preserving data_sequence_number=1. The transaction is created AFTER
    // the row_delta commit, so its tx-captured starting snapshot IS f2 — the semantic twin of
    // Java's explicit `validateFromSnapshot(rowDeltaSnapshotId)`. A' carries data_seq=1 < 2.
    let tx = Transaction::new(&table);
    let tx = tx
        .rewrite_files(vec![file_a], vec![file_a_prime])
        .data_sequence_number(1)
        .apply(tx)
        .expect("apply rewrite_files {A}→{A'} with data_sequence_number=1");
    let table = tx.commit(&catalog).await.expect("commit f3 rewrite_files");

    // Sanity: Rust's OWN scan must yield {10,30,40} (id=20 deleted; A'.data_seq=1 < eq_del.seq=2;
    // B untouched).
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
        vec![10, 30, 40],
        "Rust scan after partitioned rewrite must yield {{10,30,40}} — id=20 deleted by eq-delete; \
         A'.data_seq=1 < eq_del.seq=2 (seq-preservation holds); B intact"
    );
    assert!(
        !live_ids.contains(&20),
        "id 20 (deleted by eq-delete at seq 2, must still apply to A' data_seq=1) must be ABSENT"
    );

    // S3 partition-projection pin: A' rows must be in cat="a", B must be in cat="b".
    assert_eq!(
        id_to_category_sorted(&batches),
        expected_partitioned_rewrite_categories(),
        "Rust partitioned rewrite must route A'→cat='a' and B→cat='b'; \
         the {{id,data}} row compare alone cannot catch a wrong-partition write"
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
        "interop_write_data partitioned_rewrite GEN OK — Rust wrote {table_location} \
         (fast_append A+B, eq-delete id=20 cat=a seq 2, rewrite {{A}}→{{A'}} data_seq=1, \
         Rust scan = {{10,30,40}}, id=20 absent, A'→a B→b pinned). \
         Java verify-interop-partitioned-rewrite-data reads it next."
    );
}

// ===========================================================================================
// Fixture F comparison — Rust reads Java's ground-truth rows.
// ===========================================================================================

/// Rust reads the JAVA-written partitioned-rewrite table and compares to
/// `java_partitioned_rewrite_rows.json`.
///
/// Direction 1 of fixture F: Java's `PartitionedRewriteFilesDataOracle.generate` wrote the table
/// under `<dir>/table`, emitting `java_partitioned_rewrite_rows.json`. Rust loads the SAME table,
/// runs `scan().to_arrow()`, and asserts the live rows equal the JSON. Correctness point: id=20
/// absent (eq-delete at seq 2 applied to A' at data_seq 1); ids {10,30,40} present. Partition
/// column pinned.
#[tokio::test]
async fn test_rust_reads_java_partitioned_rewrite_data_table() {
    let Some(dir) = partitioned_rewrite_data_dir() else {
        println!(
            "skipping interop_write_data partitioned_rewrite D1 — set \
             ICEBERG_INTEROP_PARTITIONED_REWRITE_DATA_DIR \
             (run dev/java-interop/run-interop-write-data.sh)"
        );
        return;
    };

    let java_rows = sorted_by_id(load_java_rows(
        &dir.join("java_partitioned_rewrite_rows.json"),
    ));

    let metadata_path = dir.join("table/metadata/final.metadata.json");
    let json = std::fs::read_to_string(&metadata_path)
        .unwrap_or_else(|e| panic!("read {}: {e}", metadata_path.display()));
    let metadata: iceberg::spec::TableMetadata = serde_json::from_str(&json)
        .unwrap_or_else(|e| panic!("parse {}: {e}", metadata_path.display()));
    let table = Table::builder()
        .metadata(metadata)
        .metadata_location(metadata_path.to_string_lossy().to_string())
        .identifier(
            TableIdent::from_strs(["interop", "partitioned_rewrite_data"])
                .expect("valid identifier"),
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

    // id=20 must be absent (eq-delete at seq 2 applied to A' at data_seq 1).
    assert!(
        !rust_rows.iter().any(|r| r.id == 20),
        "id 20 (deleted by eq-delete at seq 2; A'.data_seq=1 < eq_del.seq=2) must be ABSENT"
    );
    assert_eq!(
        rust_rows.len(),
        3,
        "expected 3 live rows after partitioned rewrite + eq-delete: {{10,30,40}}"
    );
    assert_eq!(
        rust_rows, java_rows,
        "Rust scan of Java partitioned-rewrite table must equal Java's IcebergGenerics read (field-for-field)"
    );
    let live_ids: Vec<i64> = rust_rows.iter().map(|r| r.id).collect();
    assert_eq!(
        live_ids,
        vec![10, 30, 40],
        "live id set must be {{10,30,40}}"
    );

    // S3 partition-projection pin.
    assert_eq!(
        id_to_category_sorted(&batches),
        expected_partitioned_rewrite_categories(),
        "Rust scan of Java partitioned-rewrite table must read each row's identity(category) \
         correctly (A' survivors→a, B→b); the {{id,data}} compare alone cannot catch a wrong-partition write"
    );

    println!(
        "interop_write_data partitioned_rewrite D1 OK — Rust scan of Java table = Java IcebergGenerics \
         read: 3 live rows {{10,30,40}} (id=20 absent; seq-preservation: A'.data_seq=1 < eq_del.seq=2; B intact)"
    );
}

// ===========================================================================================
// Fixture G GEN — Rust writes the multi-bin merge_append table for Java to verify.
//
// Chain: 4 separate fast_appends (each in its own commit → 4 manifests m1..m4):
//   fast_append A(cat=a,10/20/30) → m1 (seq 1)
//   fast_append B(cat=b,40)       → m2 (seq 2)
//   fast_append C1(cat=a,50)      → m3 (seq 3)
//   fast_append C2(cat=b,55)      → m4 (seq 4)
// Measure manifest lengths → set target-size-bytes = max_len*2+1 + min-count=2 (no snapshot).
// merge_append G(cat=a,60) → pack_end yields ≥2 bins of 2 → ≥2 merged manifests (seq 5).
// All 7 rows survive: A(10/20/30)+B(40)+C1(50)+C2(55)+G(60).
// ===========================================================================================

/// Rust performs the SAME chain as Java's `MultiBinMergeAppendDataOracle.generate`:
/// four separate fast_appends → measure manifest sizes → set target-size-bytes=max*2+1 +
/// min-count=2 → merge_append G, landing `final.metadata.json` for the Java verify step.
///
/// Correctness point: all 7 rows survive. The bin-count assertion (≥2 manifests with
/// existing_files_count > 0) proves the multi-bin merge path fired, not just the single-bin
/// path covered by fixture A.
#[tokio::test]
async fn test_multi_bin_merge_append_data_gen_rust_writes_java_readable_table() {
    let Some(gen_dir) = multi_bin_merge_data_gen_dir() else {
        println!(
            "skipping interop_write_data multi_bin_merge_append GEN — set \
             ICEBERG_INTEROP_MULTI_BIN_MERGE_DATA_GEN_DIR \
             (run dev/java-interop/run-interop-write-data.sh)"
        );
        return;
    };

    let warehouse = gen_dir.to_string_lossy().to_string();
    let table_location = format!("{warehouse}/rust_table");
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "interop_multi_bin_merge_data_gen",
            HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse.clone())]),
        )
        .await
        .expect("build MemoryCatalog over local FS");

    let table = create_write_data_table(&catalog, &table_location).await;
    let schema = table.metadata().current_schema().clone();
    let bound_spec = table.metadata().default_partition_spec().as_ref().clone();
    let pk_a = partition_key(schema.clone(), bound_spec.clone(), "a");
    let pk_b = partition_key(schema.clone(), bound_spec.clone(), "b");

    // Write real parquet files for all five data sources.
    let file_a = write_data_file(&table, &pk_a, "a", vec![10, 20, 30], vec!["a", "b", "c"]).await;
    let file_b = write_data_file(&table, &pk_b, "b", vec![40], vec!["d"]).await;
    let file_c1 = write_data_file(&table, &pk_a, "a", vec![50], vec!["e"]).await;
    let file_c2 = write_data_file(&table, &pk_b, "b", vec![55], vec!["f"]).await;
    let file_g = write_data_file(&table, &pk_a, "a", vec![60], vec!["g"]).await;

    // Four SEPARATE fast_appends — each in its own commit — so each produces its own manifest file.
    // This creates 4 existing manifests m1..m4 that the merge_append G step will bin-pack.
    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(vec![file_a])
        .apply(tx)
        .expect("apply fast_append A");
    let table = tx.commit(&catalog).await.expect("commit g1 fast_append A");

    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(vec![file_b])
        .apply(tx)
        .expect("apply fast_append B");
    let table = tx.commit(&catalog).await.expect("commit g2 fast_append B");

    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(vec![file_c1])
        .apply(tx)
        .expect("apply fast_append C1");
    let table = tx.commit(&catalog).await.expect("commit g3 fast_append C1");

    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(vec![file_c2])
        .apply(tx)
        .expect("apply fast_append C2");
    let table = tx.commit(&catalog).await.expect("commit g4 fast_append C2");

    // Measure actual manifest sizes from the manifest-list. The target-size-bytes is set to
    // max_manifest_size * 2 + 1 so that pack_end fits exactly 2 manifests per bin. With 4
    // existing manifests, this yields 2 bins of 2, both satisfying min-count=2 → BOTH MERGE.
    let snapshot = table
        .metadata()
        .current_snapshot()
        .expect("current snapshot after 4 appends");
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .expect("load manifest list after 4 appends");
    let max_manifest_len: i64 = manifest_list
        .entries()
        .iter()
        .filter(|m| m.content == ManifestContentType::Data)
        .map(|m| m.manifest_length)
        .max()
        .unwrap_or(0);
    assert!(
        max_manifest_len > 0,
        "manifest_length must be positive after real-parquet appends; got {max_manifest_len}"
    );
    let target_size_bytes = (max_manifest_len as u64) * 2 + 1;
    println!(
        "interop_write_data multi_bin GEN: 4 manifests, max manifest_length={max_manifest_len} bytes, \
         target_size_bytes={target_size_bytes} (fits exactly 2 per bin)"
    );

    // g5 update_table_properties: set min-count=2 + target=max*2+1 (no snapshot).
    let tx = Transaction::new(&table);
    let tx = tx
        .update_table_properties()
        .set(
            TableProperties::PROPERTY_COMMIT_MANIFEST_MIN_COUNT_TO_MERGE.to_string(),
            "2".to_string(),
        )
        .set(
            TableProperties::PROPERTY_COMMIT_MANIFEST_TARGET_SIZE_BYTES.to_string(),
            target_size_bytes.to_string(),
        )
        .apply(tx)
        .expect("apply update_table_properties for multi-bin merge");
    let table = tx.commit(&catalog).await.expect("commit g5 property set");

    // g6 merge_append G — fires the two-bin merge.
    let tx = Transaction::new(&table);
    let tx = tx
        .merge_append()
        .add_data_files(vec![file_g])
        .apply(tx)
        .expect("apply merge_append G");
    let table = tx.commit(&catalog).await.expect("commit g6 merge_append G");

    // Assert ≥2 merged manifests (manifests with existing_files_count > 0 in the manifest-list).
    // This is the BIN-COUNT PROOF: only a multi-bin merge produces multiple manifests with Existing
    // entries. A single-bin merge (fixture A) produces exactly ONE such manifest.
    let final_snapshot = table
        .metadata()
        .current_snapshot()
        .expect("current snapshot after merge_append G");
    let final_manifest_list = final_snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .expect("load final manifest list");
    let merged_manifest_count = final_manifest_list
        .entries()
        .iter()
        .filter(|m| {
            m.content == ManifestContentType::Data && m.existing_files_count.is_some_and(|c| c > 0)
        })
        .count();
    println!(
        "interop_write_data multi_bin GEN: final manifest list has {} manifests, \
         {merged_manifest_count} with existing_files_count > 0 (bin-count proof: need ≥ 2)",
        final_manifest_list.entries().len()
    );
    assert!(
        merged_manifest_count >= 2,
        "multi-bin merge_append must produce ≥2 manifests with existing_files_count > 0 \
         (bin-count proof); got {merged_manifest_count}. Check target_size_bytes={target_size_bytes} \
         vs max_manifest_len={max_manifest_len}."
    );

    // Sanity: Rust's OWN scan must yield all 7 rows (no deletes).
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
        vec![10, 20, 30, 40, 50, 55, 60],
        "Rust scan of the multi-bin merge_append table must yield \
         {{10,20,30,40,50,55,60}} (all 7 rows, no deletes)"
    );

    // S3 partition-projection pin.
    assert_eq!(
        id_to_category_sorted(&batches),
        expected_multi_bin_merge_append_categories(),
        "Rust multi-bin merge_append must route rows to the correct identity(category) partition \
         (a={{10,20,30,50,60}}, b={{40,55}}); a wrong-partition write is invisible to {{id,data}} compare"
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
        "interop_write_data multi_bin_merge_append GEN OK — Rust wrote {table_location} \
         (4 fast_appends + set multi-bin props + merge_append G, {merged_manifest_count} merged \
         manifests with Existing entries ≥ 2, all 7 rows, partition pin OK). \
         Java verify-interop-multi-bin-merge-append-data reads it next."
    );
}

// ===========================================================================================
// Fixture G comparison — Rust reads Java's ground-truth rows.
// ===========================================================================================

/// Rust reads the JAVA-written multi-bin merge-append table and compares to
/// `java_multi_bin_merge_append_rows.json`.
///
/// Direction 1 of fixture G: Java's `MultiBinMergeAppendDataOracle.generate` wrote the table
/// under `<dir>/table`, emitting `java_multi_bin_merge_append_rows.json`. Rust loads the SAME
/// table, runs `scan().to_arrow()`, and asserts the live rows equal the JSON. Correctness point:
/// all 7 ids must be present; ≥2 merged manifests each with Existing entries. The partition
/// column is pinned via `expected_multi_bin_merge_append_categories`.
#[tokio::test]
async fn test_rust_reads_java_multi_bin_merge_append_data_table() {
    let Some(dir) = multi_bin_merge_data_dir() else {
        println!(
            "skipping interop_write_data multi_bin_merge_append D1 — set \
             ICEBERG_INTEROP_MULTI_BIN_MERGE_DATA_DIR \
             (run dev/java-interop/run-interop-write-data.sh)"
        );
        return;
    };

    let java_rows = sorted_by_id(load_java_rows(
        &dir.join("java_multi_bin_merge_append_rows.json"),
    ));

    let metadata_path = dir.join("table/metadata/final.metadata.json");
    let json = std::fs::read_to_string(&metadata_path)
        .unwrap_or_else(|e| panic!("read {}: {e}", metadata_path.display()));
    let metadata: iceberg::spec::TableMetadata = serde_json::from_str(&json)
        .unwrap_or_else(|e| panic!("parse {}: {e}", metadata_path.display()));
    let table = Table::builder()
        .metadata(metadata)
        .metadata_location(metadata_path.to_string_lossy().to_string())
        .identifier(
            TableIdent::from_strs(["interop", "multi_bin_merge_append_data"])
                .expect("valid identifier"),
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

    // All 7 rows must be present.
    for absent_id in [10, 20, 30, 40, 50, 55, 60] {
        assert!(
            rust_rows.iter().any(|r| r.id == absent_id),
            "id {absent_id} must be PRESENT in the Rust scan of Java's multi-bin merge-append table"
        );
    }
    assert_eq!(
        rust_rows.len(),
        7,
        "expected 7 live rows (A:3+B:1+C1:1+C2:1+G:1; no deletes)"
    );
    assert_eq!(
        rust_rows, java_rows,
        "Rust scan of Java multi-bin merge-append table must equal Java's IcebergGenerics read \
         (field-for-field)"
    );
    let live_ids: Vec<i64> = rust_rows.iter().map(|r| r.id).collect();
    assert_eq!(
        live_ids,
        vec![10, 20, 30, 40, 50, 55, 60],
        "live id set must be {{10,20,30,40,50,55,60}}"
    );

    // S3 partition-projection pin.
    assert_eq!(
        id_to_category_sorted(&batches),
        expected_multi_bin_merge_append_categories(),
        "Rust scan of Java multi-bin merge-append table must read each row's identity(category) \
         correctly (a={{10,20,30,50,60}}, b={{40,55}}); the {{id,data}} compare alone cannot catch \
         a wrong-partition write"
    );

    println!(
        "interop_write_data multi_bin_merge_append D1 OK — Rust scan of Java table = Java \
         IcebergGenerics read: 7 live rows {{10,20,30,40,50,55,60}} (all rows, partition pin OK)"
    );
}
