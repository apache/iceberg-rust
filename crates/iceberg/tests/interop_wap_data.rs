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

//! DATA-LEVEL WAP interop (increment I3) — proving Rust's REAL staging (`stage_only()` +
//! `DataFileWriter`) and Java's REAL cherry-pick (`ManageSnapshots.cherrypick()`) interoperate at
//! the row level, and vice versa.
//!
//! # Table shape
//!
//! V2 partitioned by `identity(category)`, schema `{1 id long required, 2 category string
//! required, 3 data string optional}`. Identical to the `MergeAppendDataOracle` family in
//! `interop_write_data.rs` — all helpers are reused.
//!
//! # Direction 1 — Rust stages REAL parquet, Java cherry-picks and verifies
//!
//! [`test_wap_data_gen_rust_writes_staged_table`] (env
//! `ICEBERG_INTEROP_WAP_DATA_GEN_DIR`):
//!
//! Rust creates a partitioned V2 table at `<gen_dir>/rust_table`, writes REAL parquet base data:
//! - `cat=a`: rows `{(10,"a"), (20,"b"), (30,"c")}` — seq 1 fast-append
//! - `cat=b`: row  `{(40,"d")}` — seq 1 fast-append (same commit as cat=a)
//!
//! Then (S-replay order) Rust stages WAP data FIRST (while current=base, seq 2 staged):
//! - `cat=a`: rows `{(50,"e"), (60,"f")}`
//! - `cat=b`: row  `{(70,"g")}`
//!
//! Then a "bump" commit (fast-append id=99, cat=a, data="bump") advances main to seq 3.
//! Now `staged.parent = base (seq 1) ≠ current head = bump (seq 3)` → **REPLAY** shape.
//! Java's `manageSnapshots().cherrypick()` produces a NEW snapshot with `source-snapshot-id`
//! + `published-wap-id` in the summary.
//!
//! Emits:
//! - `rust_table/metadata/final.metadata.json` — staged-state table (current = bump, staged = w1)
//! - `rust_staged_snapshot_id.json` — `{"staged_snapshot_id": NNN}`
//!
//! Java then cherry-picks and asserts 8 rows + WAP semantics (step 4 of the chain script).
//!
//! # Direction 2 — Java stages + cherry-picks, Rust reads
//!
//! [`test_wap_data_d2_rust_reads_java_cherrypick_table`] (env
//! `ICEBERG_INTEROP_WAP_DATA_DIR`):
//!
//! Java's `generate-interop-wap-data-java-table` step wrote:
//! - `java_cherrypick_table/metadata/final.metadata.json` — post-cherry-pick table
//! - `java_cherrypick_rows.json` — `[{id, data}, ...]` 8 expected rows (base 4 + bump 1 + staged 3)
//! - `java_cherrypick_snapshot_summary.json` — `{source_snapshot_id, published_wap_id}`
//!
//! Rust loads the Java-cherry-picked table, scans to Arrow via the production path,
//! asserts exactly 8 live rows, asserts row content matches `java_cherrypick_rows.json`,
//! pins the `category` column for each row (S3 partition-projection lesson), and asserts
//! WAP semantics on the current snapshot.
//!
//! # WAP staged-snapshot invariant (semantic pin)
//!
//! Before cherry-pick (in the GEN test):
//! - `current-snapshot-id` is the bump snapshot (NOT the staged snapshot — bump follows stage).
//! - The staged snapshot IS in `metadata.snapshots()` with `wap.id=w1`.
//! - The staged snapshot is NOT reachable via `current_snapshot().parent_id` ancestry.
//!
//! # Env gates
//!
//! Both tests are clean NO-OPS (runtime early-return, not `#[ignore]`) unless their env var is set
//! non-empty, so offline `cargo test` stays green.
//!
//! - `ICEBERG_INTEROP_WAP_DATA_GEN_DIR` — Direction 1 GEN (Rust writes staged table)
//! - `ICEBERG_INTEROP_WAP_DATA_DIR`     — Direction 2 compare (Rust reads Java table)

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use futures::TryStreamExt;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    DataFile, DataFileFormat, FormatVersion, Literal, NestedField, PartitionKey, PartitionSpec,
    PrimitiveType, Schema, SchemaRef, SortOrder, Struct, Type, UnboundPartitionSpec,
};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use serde::Deserialize;
use serde_json::json;

// ===========================================================================================
// Row model — `{id, data}` shape matching Java's `readLiveWapRowsToJson`.
// ===========================================================================================

/// One live row from Java's `readLiveWapRowsToJson`: id (long) + nullable data string.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct WapRow {
    id: i64,
    data: Option<String>,
}

fn sorted_by_id(mut rows: Vec<WapRow>) -> Vec<WapRow> {
    rows.sort_by_key(|r| r.id);
    rows
}

// ===========================================================================================
// Env-var gates.
// ===========================================================================================

fn wap_data_gen_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_WAP_DATA_GEN_DIR")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

fn wap_data_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_WAP_DATA_DIR")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

// ===========================================================================================
// Schema + spec helpers (identical to interop_write_data.rs fixtures A/C/D/E/F/G).
// ===========================================================================================

/// `{1 id long required, 2 category string required, 3 data string optional}`.
fn wap_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "category", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(3, "data", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("build WAP {id long, category string, data string} schema")
}

/// `identity(category)` unbound partition spec, spec id 0.
fn wap_spec() -> UnboundPartitionSpec {
    UnboundPartitionSpec::builder()
        .with_spec_id(0)
        .add_partition_field(
            2,
            "category".to_string(),
            iceberg::spec::Transform::Identity,
        )
        .expect("add identity(category) partition field")
        .build()
}

/// Build the `PartitionKey` for `category = <value>`.
fn partition_key(schema: SchemaRef, spec: PartitionSpec, category: &str) -> PartitionKey {
    PartitionKey::new(
        spec,
        schema,
        Struct::from_iter([Some(Literal::string(category))]),
    )
}

// ===========================================================================================
// Real-parquet writer helper (same pattern as interop_write_data.rs `write_data_file`).
// ===========================================================================================

/// Write a REAL parquet data file for one partition via the production `DataFileWriter`.
async fn write_wap_data_file(
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
    .expect("build per-partition data batch");

    let location_gen =
        DefaultLocationGenerator::new(table.metadata().clone()).expect("location generator");
    let file_name_gen = DefaultFileNameGenerator::new(
        "wapdata".to_string(),
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
        .expect("at least one data file")
}

// ===========================================================================================
// Scan helper — Arrow RecordBatch → (id, data) and (id, category) extraction.
// ===========================================================================================

fn extract_wap_rows(batch: &RecordBatch) -> Vec<WapRow> {
    let id = batch
        .column_by_name("id")
        .expect("id column present")
        .as_primitive::<Int64Type>();
    let data = batch.column_by_name("data").expect("data column present");
    (0..batch.num_rows())
        .map(|i| WapRow {
            id: id.value(i),
            data: arrow_string_value(data, i),
        })
        .collect()
}

fn extract_id_to_category(batch: &RecordBatch) -> Vec<(i64, Option<String>)> {
    let id = batch
        .column_by_name("id")
        .expect("id column present")
        .as_primitive::<Int64Type>();
    let category = batch
        .column_by_name("category")
        .expect("category column present");
    (0..batch.num_rows())
        .map(|i| (id.value(i), arrow_string_value(category, i)))
        .collect()
}

fn arrow_string_value(array: &ArrayRef, i: usize) -> Option<String> {
    use arrow_schema::DataType;
    if array.is_null(i) {
        return None;
    }
    match array.data_type() {
        DataType::Utf8 => Some(array.as_string::<i32>().value(i).to_string()),
        DataType::LargeUtf8 => Some(array.as_string::<i64>().value(i).to_string()),
        other => panic!("unexpected string column arrow type: {other:?}"),
    }
}

fn id_to_category_sorted(batches: &[RecordBatch]) -> Vec<(i64, Option<String>)> {
    let mut pairs: Vec<(i64, Option<String>)> = Vec::new();
    for batch in batches {
        pairs.extend(extract_id_to_category(batch));
    }
    pairs.sort_by_key(|(id, _)| *id);
    pairs
}

/// Load + parse Java ground-truth rows JSON as `Vec<WapRow>`.
fn load_java_wap_rows(path: &Path) -> Vec<WapRow> {
    let json =
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str::<Vec<WapRow>>(&json)
        .unwrap_or_else(|e| panic!("parse {}: {e}", path.display()))
}

// ===========================================================================================
// Table creation helper.
// ===========================================================================================

async fn create_wap_table(catalog: &impl Catalog, table_location: &str) -> Table {
    let namespace = NamespaceIdent::new("interop".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create namespace");
    let creation = TableCreation::builder()
        .name("rust_table".to_string())
        .location(table_location.to_string())
        .schema(wap_schema())
        .partition_spec(wap_spec())
        .sort_order(SortOrder::unsorted_order())
        .format_version(FormatVersion::V2)
        .build();
    catalog
        .create_table(&namespace, creation)
        .await
        .expect("create WAP rust_table")
}

// ===========================================================================================
// Direction 1 — Rust GEN: write staged WAP table for Java to cherry-pick.
// ===========================================================================================

/// Direction 1 GEN: Rust creates a partitioned V2 table with REAL parquet base data and a
/// staged WAP append (REAL parquet, wap.id=w1). REPLAY shape: base → bump (property commit,
/// no data) → stage (staged.parent=base ≠ current head=bump). Java cherry-picks and asserts.
///
/// Emits `<gen_dir>/rust_table/metadata/final.metadata.json` (staged-state) and
/// `<gen_dir>/rust_staged_snapshot_id.json`.
#[tokio::test]
async fn test_wap_data_gen_rust_writes_staged_table() {
    let Some(gen_dir) = wap_data_gen_dir() else {
        println!(
            "skipping interop_wap_data GEN — \
             set ICEBERG_INTEROP_WAP_DATA_GEN_DIR to an absolute path \
             (run dev/java-interop/run-interop-wap-data.sh)"
        );
        return;
    };

    let table_location = gen_dir.join("rust_table").to_string_lossy().to_string();

    std::fs::create_dir_all(&table_location).expect("create rust_table dir");

    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "interop_wap_data",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_string(),
                gen_dir.to_string_lossy().to_string(),
            )]),
        )
        .await
        .expect("build MemoryCatalog over local FS");

    let mut table = create_wap_table(&catalog, &table_location).await;

    // Bind schema + spec once (table may be re-loaded on each commit).
    let bound_schema = table.metadata().current_schema().clone();
    let bound_spec = table.metadata().default_partition_spec().as_ref().clone();
    let pk_a = partition_key(bound_schema.clone(), bound_spec.clone(), "a");
    let pk_b = partition_key(bound_schema.clone(), bound_spec.clone(), "b");

    // -----------------------------------------------------------------------
    // Step 1: commit BASE data — fast-append cat=a(10,20,30) + cat=b(40).
    // -----------------------------------------------------------------------
    let base_a =
        write_wap_data_file(&table, &pk_a, "a", vec![10, 20, 30], vec!["a", "b", "c"]).await;
    let base_b = write_wap_data_file(&table, &pk_b, "b", vec![40], vec!["d"]).await;

    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(vec![base_a, base_b])
        .apply(tx)
        .expect("apply base fast_append");
    table = tx.commit(&catalog).await.expect("commit base fast_append");

    let base_snapshot_id = table
        .metadata()
        .current_snapshot_id()
        .expect("base snapshot id");
    println!(
        "interop_wap_data GEN: base snapshot id={base_snapshot_id} (cat=a 10/20/30, cat=b 40)"
    );

    // -----------------------------------------------------------------------
    // Step 2: STAGE WAP append FIRST (S-replay pattern: stage while current=base,
    // then advance main so staged.parent ≠ head → REPLAY cherry-pick).
    // staged.parent = base. Current still = base after stage_only().
    // -----------------------------------------------------------------------
    let bound_schema2 = table.metadata().current_schema().clone();
    let bound_spec2 = table.metadata().default_partition_spec().as_ref().clone();
    let pk_a2 = partition_key(bound_schema2.clone(), bound_spec2.clone(), "a");
    let pk_b2 = partition_key(bound_schema2.clone(), bound_spec2.clone(), "b");

    let staged_a = write_wap_data_file(&table, &pk_a2, "a", vec![50, 60], vec!["e", "f"]).await;
    let staged_b = write_wap_data_file(&table, &pk_b2, "b", vec![70], vec!["g"]).await;

    let mut stage_props = HashMap::new();
    stage_props.insert("wap.id".to_string(), "w1".to_string());

    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .set_snapshot_properties(stage_props)
        .stage_only()
        .add_data_files(vec![staged_a, staged_b])
        .apply(tx)
        .expect("apply stage_only WAP fast_append");
    table = tx
        .commit(&catalog)
        .await
        .expect("commit stage_only WAP fast_append");

    // Find the staged snapshot id (before bumping, so current = base).
    let staged_snapshot_id = {
        let mut found = None;
        for snap in table.metadata().snapshots() {
            let props = &snap.summary().additional_properties;
            if props.get("wap.id").map(String::as_str) == Some("w1")
                && Some(snap.snapshot_id()) != table.metadata().current_snapshot_id()
            {
                found = Some(snap.snapshot_id());
            }
        }
        found.expect(
            "interop_wap_data GEN: staged snapshot with wap.id=w1 must exist in metadata.snapshots()",
        )
    };

    // Verify current is still base (staging must not move current).
    let current_after_stage = table
        .metadata()
        .current_snapshot_id()
        .expect("current snapshot after staging");
    assert_eq!(
        current_after_stage, base_snapshot_id,
        "interop_wap_data GEN: current-snapshot-id MUST still be base after \
         stage_only(), but got {current_after_stage} (expected base={base_snapshot_id})"
    );

    // -----------------------------------------------------------------------
    // Step 3: BUMP commit — advance main so staged.parent (base) ≠ head (bump).
    // The bump row (id=99, cat=a, data="bump") is a known fixture row included in
    // the expected final rows (8 total: base 4 + bump 1 + staged 3).
    // -----------------------------------------------------------------------
    let bound_schema_bump = table.metadata().current_schema().clone();
    let bound_spec_bump = table.metadata().default_partition_spec().as_ref().clone();
    let pk_a_bump = partition_key(bound_schema_bump.clone(), bound_spec_bump.clone(), "a");

    let bump_file = write_wap_data_file(&table, &pk_a_bump, "a", vec![99], vec!["bump"]).await;

    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(vec![bump_file])
        .apply(tx)
        .expect("apply bump fast_append");
    table = tx.commit(&catalog).await.expect("commit bump fast_append");

    let bump_snapshot_id = table
        .metadata()
        .current_snapshot_id()
        .expect("bump snapshot id");
    assert_ne!(
        bump_snapshot_id, base_snapshot_id,
        "interop_wap_data GEN: bump snapshot id must differ from base id"
    );

    // -----------------------------------------------------------------------
    // Semantic invariant pin: staged snapshot is NOT in the current ancestry.
    // (staged.parent = base; current = bump; they differ → REPLAY shape.)
    // -----------------------------------------------------------------------
    {
        let current_snap = table
            .metadata()
            .current_snapshot()
            .expect("current snapshot");
        let mut walk_id = Some(current_snap.snapshot_id());
        let mut found_in_ancestry = false;
        while let Some(id) = walk_id {
            if id == staged_snapshot_id {
                found_in_ancestry = true;
                break;
            }
            walk_id = table
                .metadata()
                .snapshot_by_id(id)
                .and_then(|s| s.parent_snapshot_id());
        }
        assert!(
            !found_in_ancestry,
            "interop_wap_data GEN: staged snapshot {staged_snapshot_id} must NOT be in \
             current ancestry (it is staged, not published)"
        );
    }

    println!(
        "interop_wap_data GEN: staged={staged_snapshot_id} wap.id=w1 \
         (staged.parent=base={base_snapshot_id}, head=bump={bump_snapshot_id} → REPLAY shape)"
    );

    // -----------------------------------------------------------------------
    // Emit artifacts.
    // -----------------------------------------------------------------------

    // 1. final.metadata.json at <rust_table>/metadata/final.metadata.json (staged state).
    let final_meta_path = format!("{table_location}/metadata/final.metadata.json");
    table
        .metadata_ref()
        .write_to(table.file_io(), &final_meta_path)
        .await
        .expect("write final.metadata.json");
    println!("interop_wap_data GEN: final.metadata.json written at {final_meta_path}");

    // 2. rust_staged_snapshot_id.json.
    let staged_id_json = json!({"staged_snapshot_id": staged_snapshot_id});
    let staged_id_path = gen_dir.join("rust_staged_snapshot_id.json");
    std::fs::write(
        &staged_id_path,
        serde_json::to_string_pretty(&staged_id_json).expect("serialize staged_id JSON"),
    )
    .expect("write rust_staged_snapshot_id.json");
    println!(
        "interop_wap_data GEN: rust_staged_snapshot_id.json written at {}",
        staged_id_path.display()
    );

    println!(
        "interop_wap_data GEN complete: rust_table staged (base={base_snapshot_id}, \
         staged_wap={staged_snapshot_id}, bump={bump_snapshot_id}) — \
         8 rows ready for Java cherry-pick (base 4 + bump 1 + staged 3)"
    );
}

// ===========================================================================================
// Direction 2 — Java acts, Rust verifies.
// ===========================================================================================

/// Direction 2: load the Java-cherry-picked table, scan rows via the production path,
/// assert 7 live rows with correct content, pin the `category` column (S3 partition-projection
/// lesson), and assert WAP semantics on the current snapshot.
#[tokio::test]
async fn test_wap_data_d2_rust_reads_java_cherrypick_table() {
    let Some(compare_dir) = wap_data_dir() else {
        println!(
            "skipping interop_wap_data D2 — \
             set ICEBERG_INTEROP_WAP_DATA_DIR to an absolute path \
             (run dev/java-interop/run-interop-wap-data.sh)"
        );
        return;
    };

    // -----------------------------------------------------------------------
    // Load artifacts written by Java's `generate-interop-wap-data-java-table`.
    // -----------------------------------------------------------------------
    let final_meta_path = compare_dir
        .join("java_cherrypick_table")
        .join("metadata")
        .join("final.metadata.json");
    assert!(
        final_meta_path.exists(),
        "interop_wap_data D2: missing java_cherrypick_table/metadata/final.metadata.json at \
         {} (run generate-interop-wap-data-java-table first)",
        final_meta_path.display()
    );

    let expected_rows_path = compare_dir.join("java_cherrypick_rows.json");
    assert!(
        expected_rows_path.exists(),
        "interop_wap_data D2: missing java_cherrypick_rows.json at {} \
         (run generate-interop-wap-data-java-table first)",
        expected_rows_path.display()
    );

    let summary_path = compare_dir.join("java_cherrypick_snapshot_summary.json");
    assert!(
        summary_path.exists(),
        "interop_wap_data D2: missing java_cherrypick_snapshot_summary.json at {} \
         (run generate-interop-wap-data-java-table first)",
        summary_path.display()
    );

    // -----------------------------------------------------------------------
    // Parse expected rows and WAP summary.
    // -----------------------------------------------------------------------
    let expected_rows = sorted_by_id(load_java_wap_rows(&expected_rows_path));
    assert_eq!(
        expected_rows.len(),
        8,
        "interop_wap_data D2: java_cherrypick_rows.json must contain 8 rows \
         (base 4 + bump 1 + staged 3), got {}",
        expected_rows.len()
    );

    let summary_json: serde_json::Value = {
        let raw = std::fs::read_to_string(&summary_path)
            .unwrap_or_else(|e| panic!("read {}: {e}", summary_path.display()));
        serde_json::from_str(&raw)
            .unwrap_or_else(|e| panic!("parse {}: {e}", summary_path.display()))
    };
    let expected_source_id = summary_json["source_snapshot_id"]
        .as_str()
        .expect("source_snapshot_id must be a string in java_cherrypick_snapshot_summary.json")
        .to_string();
    let expected_published_wap_id = summary_json["published_wap_id"]
        .as_str()
        .expect("published_wap_id must be a string in java_cherrypick_snapshot_summary.json")
        .to_string();

    // -----------------------------------------------------------------------
    // Load the Java-cherry-picked table via the production Rust path.
    // -----------------------------------------------------------------------
    let _table_location = compare_dir
        .join("java_cherrypick_table")
        .to_string_lossy()
        .to_string();

    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "interop_wap_data_d2",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_string(),
                compare_dir.to_string_lossy().to_string(),
            )]),
        )
        .await
        .expect("build MemoryCatalog over local FS");

    // Register the Java-cherry-picked table with the catalog under a known ident.
    let namespace = NamespaceIdent::new("interop".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create namespace");

    // Load the table from the final.metadata.json path by registering it with the catalog.
    // (Load and parse the metadata to verify it's readable — the catalog register_table path reads it.)
    let _final_meta_str = std::fs::read_to_string(&final_meta_path)
        .unwrap_or_else(|e| panic!("read final.metadata.json: {e}"));
    let table_ident = TableIdent::new(namespace.clone(), "java_cherrypick_table".to_string());
    let table = catalog
        .register_table(&table_ident, final_meta_path.to_string_lossy().to_string())
        .await
        .expect("register java_cherrypick_table with catalog");

    // -----------------------------------------------------------------------
    // WAP semantics pin: verify the current snapshot carries the expected
    // source-snapshot-id and published-wap-id.
    // -----------------------------------------------------------------------
    let current_snap = table
        .metadata()
        .current_snapshot()
        .expect("interop_wap_data D2: java_cherrypick_table must have a current snapshot");

    let got_source_id = current_snap
        .summary()
        .additional_properties
        .get("source-snapshot-id")
        .cloned()
        .unwrap_or_default();
    let got_published_wap_id = current_snap
        .summary()
        .additional_properties
        .get("published-wap-id")
        .cloned()
        .unwrap_or_default();

    assert_eq!(
        got_source_id, expected_source_id,
        "interop_wap_data D2: current snapshot source-snapshot-id mismatch: \
         got={got_source_id} expected={expected_source_id}"
    );
    assert_eq!(
        got_published_wap_id, expected_published_wap_id,
        "interop_wap_data D2: current snapshot published-wap-id mismatch: \
         got={got_published_wap_id} expected={expected_published_wap_id}"
    );
    assert_eq!(
        got_published_wap_id, "w1",
        "interop_wap_data D2: published-wap-id must be 'w1', got '{got_published_wap_id}'"
    );
    println!(
        "interop_wap_data D2: WAP semantics OK — \
         source-snapshot-id={got_source_id} published-wap-id={got_published_wap_id}"
    );

    // -----------------------------------------------------------------------
    // Scan all rows via the production scan path.
    // -----------------------------------------------------------------------
    let batches: Vec<RecordBatch> = table
        .scan()
        .build()
        .expect("build scan")
        .to_arrow()
        .await
        .expect("to_arrow")
        .try_collect()
        .await
        .expect("collect batches");

    let mut actual_rows: Vec<WapRow> = batches.iter().flat_map(extract_wap_rows).collect();
    actual_rows = sorted_by_id(actual_rows);

    // -----------------------------------------------------------------------
    // Row-content pin: must match Java's 8 rows exactly
    // (base 4: 10,20,30,40 + bump 1: 99 + staged 3: 50,60,70).
    // -----------------------------------------------------------------------
    assert_eq!(
        actual_rows.len(),
        8,
        "interop_wap_data D2: expected 8 live rows (base 4 + bump 1 + staged 3), \
         got {} rows={actual_rows:?}",
        actual_rows.len()
    );
    assert_eq!(
        actual_rows, expected_rows,
        "interop_wap_data D2: live rows mismatch:\n  got={actual_rows:?}\n  expected={expected_rows:?}"
    );

    // ANTI-CIRCULAR ground-truth pin (I3 REVIEWER 2026-06-12): `expected_rows` above is loaded
    // from `java_cherrypick_rows.json`, which Java produced by reading the SAME table — so a
    // data-value corruption Java ALSO reads would round-trip and the cross-check above would pass
    // silently (Rust-reads-X vs Java-reads-X always agree). Pin the `data` column against the
    // hand-declared fixture (10→a … 99→bump) so a genuine value move is caught independently of
    // Java's read. This is the only check on this leg that detects a parse-clean `data` move.
    let ground_truth_rows: Vec<WapRow> = vec![
        WapRow {
            id: 10,
            data: Some("a".to_string()),
        },
        WapRow {
            id: 20,
            data: Some("b".to_string()),
        },
        WapRow {
            id: 30,
            data: Some("c".to_string()),
        },
        WapRow {
            id: 40,
            data: Some("d".to_string()),
        },
        WapRow {
            id: 50,
            data: Some("e".to_string()),
        },
        WapRow {
            id: 60,
            data: Some("f".to_string()),
        },
        WapRow {
            id: 70,
            data: Some("g".to_string()),
        },
        WapRow {
            id: 99,
            data: Some("bump".to_string()),
        },
    ];
    assert_eq!(
        actual_rows, ground_truth_rows,
        "interop_wap_data D2: live rows diverge from the hand-declared fixture (anti-circular):\n  \
         got={actual_rows:?}\n  ground_truth={ground_truth_rows:?}"
    );
    println!("interop_wap_data D2: row-content pin OK — 8 rows {actual_rows:?}");

    // -----------------------------------------------------------------------
    // Partition-routing pin (S3 partition-projection lesson):
    // cat=a-rows are {10,20,30,50,60,99(bump)}; cat=b-rows are {40,70}.
    //
    // SCOPE NOTE (I3 REVIEWER 2026-06-12): for an identity(category) table the `category` value is
    // PROJECTED from the manifest partition stamp, NOT read from the parquet `category` column. A
    // writer that places wrong-category rows in a partition (column="b", partition stamp="a") reads
    // back as "a" here — the projection masks it, so this pin canNOT catch a wrong-partition DATA
    // move (proven by the critic's parse-clean probe). It DOES catch a wrong partition STAMP in the
    // manifest. The data-VALUE move is caught by the anti-circular ground-truth pin above; the
    // wrong-stored-category-vs-stamp case is an inherent Iceberg "garbage in" gap (neither Java nor
    // Rust validates column==stamp) and is named residue, not a fixable assertion here.
    // -----------------------------------------------------------------------
    let expected_categories: Vec<(i64, Option<String>)> = vec![
        (10, Some("a".to_string())),
        (20, Some("a".to_string())),
        (30, Some("a".to_string())),
        (40, Some("b".to_string())),
        (50, Some("a".to_string())),
        (60, Some("a".to_string())),
        (70, Some("b".to_string())),
        (99, Some("a".to_string())),
    ];
    let actual_categories = id_to_category_sorted(&batches);
    assert_eq!(
        actual_categories, expected_categories,
        "interop_wap_data D2: partition routing mismatch:\n  got={actual_categories:?}\n  \
         expected={expected_categories:?}"
    );
    println!(
        "interop_wap_data D2: partition routing OK — \
         a={{10,20,30,50,60,99}} b={{40,70}}"
    );

    println!(
        "interop_wap_data D2 PASS: Rust read Java-cherry-picked WAP table — \
         8 rows correct, WAP semantics verified, partition routing pinned"
    );
}
