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

//! Java interop test for DELETION-VECTOR TABLE-level round-trips (Increment D4) — the evidence
//! capstone of the DV arc. Two proofs in one fixture, both driven by
//! `dev/java-interop/run-interop-dv.sh`:
//!
//! 1. **Direction-2 TABLE level (the headline).** [`test_dv_table_gen_rust_writes_java_readable_v3_dv_table`]
//!    writes a COMPLETE V3 table at `<dir>/rust_table` through the production Rust path — a
//!    real-FS `MemoryCatalog`, TWO real parquet data files in TWO identity(category) partitions
//!    `fast_append`ed at sequence 1, then D2's `DVFileWriter` writing ONE Puffin holding TWO
//!    `deletion-vector-v1` blobs (one per data file — the multi-blob case) committed through
//!    D3's `row_delta` at sequence 2 — and emits `expected_rows.json`. The Java oracle's
//!    `verify-interop-dv-table` mode then loads the Rust-written `final.metadata.json` and reads
//!    the table with Java's PRODUCTION scan (`IcebergGenerics`, which loads BOTH DVs via
//!    `BaseDeleteLoader.readDV` and applies them), asserting the live rows equal the expected
//!    set, plus a manifest-API cross-check of the committed `DeleteFile` metadata
//!    (content/format/referenced-data-file/content-offset/size/cardinality). A failure there is
//!    a REAL table-level write-incompatibility finding — Rust committed a V3+DV table Java
//!    cannot read (the silent-resurrection class: a dropped DV resurrects its deleted rows).
//!
//! 2. **Metadata-level DV chain (the E1-family extension).** BOTH sides perform the SAME logical
//!    chain on equivalent V3 tables — {fast_append two partitioned data files, row_delta adding
//!    two DVs} — the Java side via `generate-interop-dv-table` (`newFastAppend` +
//!    `BaseDVFileWriter` + `newRowDelta().addDeletes`). The canonical snapshot-metadata views
//!    (see [`common::snapshot_meta_view`]) are then compared the established three ways:
//!    Java's own view (`java_meta.json`), Java's view of the RUST table byte-diffed against it
//!    by the run script, and Rust's views of BOTH tables asserted equal to it here
//!    ([`test_dv_meta_views_match_java`]). This pins the `added-dvs` summary key, the operation
//!    classification, and the manifest count/sequence-number semantics of a DV commit.
//!    SCOPE NOTE: the canonical view's entry tuple does NOT carry the DV-specific
//!    `referenced_data_file`/`content_offset`/`content_size_in_bytes` fields — those are
//!    cross-checked at the table level by the Java manifest-API step instead, so the
//!    metadata-level claim covers exactly what the view compares (status/content/record-count/
//!    sequence-number/equality-ids/partition + manifest structure + summary counts).
//!
//! THE FIXTURE (identical logical constants on both sides; paths differ):
//! V3, schema {1 id long required, 2 category string required, 3 data string optional},
//! partitioned by identity(category) (spec id 0):
//!   * cat=a data file: (10,a,x) (20,a,y) (30,a,z) at positions 0..2;
//!   * cat=b data file: (40,b,p) (50,b,q) (60,b,r) at positions 0..2;
//!   * ONE Puffin, TWO DVs: positions {1} of the cat=a file (id 20) + positions {0,2} of the
//!     cat=b file (ids 40/60) — DISTINCT cardinalities (1 vs 2) so the canonical entry sort
//!     never ties before the partition key.
//!
//! Live merge-on-read rows = {(10,x),(30,z),(50,q)}.
//!
//! THE ENV GATE. Both tests are clean NO-OPS (runtime early-return, not `#[ignore]`) unless
//! `ICEBERG_INTEROP_DV_TABLE_DIR` is set non-empty, so the offline `cargo test` gate needs no
//! Java/Maven. With the env var SET, [`test_dv_meta_views_match_java`] REQUIRES `java_meta.json`
//! and the Java mirror table to exist (the script runs the Java steps between the two Rust
//! phases) and fails loudly otherwise. Run the phases individually via the test-name filters in
//! `run-interop-dv.sh`; do not run the whole binary with the env var set mid-harness.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use futures::TryStreamExt;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, FormatVersion, Literal, NestedField, PartitionKey,
    PartitionSpec, PrimitiveType, Schema, SchemaRef, SortOrder, Struct, Transform, Type,
    UnboundPartitionSpec,
};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::base_writer::deletion_vector_writer::DVFileWriter;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

mod common;
use common::snapshot_meta_view::snapshot_meta_view;

// ===========================================================================================
// The shared row model + env gate.
// ===========================================================================================

/// One live row of the merge-on-read read: the `id` (long) + nullable `data` string (field 3 —
/// the comparison column, NOT `category`; same convention as the part-scan oracle).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ScanRow {
    id: i64,
    data: Option<String>,
}

/// Sort rows by id (then data) for an order-independent comparison.
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

/// The committed-DV metadata the Java oracle cross-checks against the delete manifests it reads
/// back from the Rust-written table (content/format are implied: position deletes, PUFFIN).
#[derive(Debug, Serialize, Deserialize)]
struct ExpectedDvMeta {
    referenced_data_file: String,
    record_count: u64,
    content_offset: i64,
    content_size_in_bytes: i64,
}

/// The temp dir shared with the Java oracle. `None` when the env var is unset OR empty
/// (set-but-empty must not flip the gate on).
fn dv_table_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_DV_TABLE_DIR")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

/// Extract the (id, data) rows from one scan batch by column name.
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

/// Read row `i` of a nullable string column, tolerating Utf8 (i32) / LargeUtf8 (i64) offsets.
fn string_value(array: &arrow_array::ArrayRef, i: usize) -> Option<String> {
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

// ===========================================================================================
// The GEN fixture machinery — the partitioned V3 table written through the PRODUCTION path
// (the interop_scan_exec partitioned-GEN template, lifted to V3 + DVs).
// ===========================================================================================

/// The PARTITIONED schema both sides use: {1 id long required, 2 category string required,
/// 3 data string optional}; the spec partitions by identity(category).
fn dv_table_schema() -> Schema {
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

/// The identity(category) unbound partition spec (spec id 0).
fn dv_table_unbound_spec() -> UnboundPartitionSpec {
    UnboundPartitionSpec::builder()
        .with_spec_id(0)
        .add_partition_field(2, "category".to_string(), Transform::Identity)
        .expect("add identity(category) partition field")
        .build()
}

/// Create the PARTITIONED **V3** table at EXACTLY `<gen_dir>/rust_table` in a `MemoryCatalog`
/// over the local FS — V3 because deletion vectors are V3-only (D3's format gate).
async fn create_v3_partitioned_rust_table(catalog: &impl Catalog, table_location: &str) -> Table {
    let namespace = NamespaceIdent::new("interop".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create namespace");

    let creation = TableCreation::builder()
        .name("rust_table".to_string())
        .location(table_location.to_string())
        .schema(dv_table_schema())
        .partition_spec(dv_table_unbound_spec())
        .sort_order(SortOrder::unsorted_order())
        .format_version(FormatVersion::V3)
        .build();

    catalog
        .create_table(&namespace, creation)
        .await
        .expect("create V3 partitioned rust_table")
}

/// Build the `PartitionKey` for one identity(category) partition value.
fn category_partition_key(schema: SchemaRef, spec: PartitionSpec, category: &str) -> PartitionKey {
    PartitionKey::new(
        spec,
        schema,
        Struct::from_iter([Some(Literal::string(category))]),
    )
}

/// Write a REAL parquet DATA file for ONE partition via the production `DataFileWriter` built
/// with the partition's `PartitionKey` (which stamps the partition Struct + spec id onto the
/// `DataFile` and routes the parquet under the partition path).
async fn write_partitioned_data_file(
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
        "rust-data".to_string(),
        Some(uuid::Uuid::now_v7().to_string()),
        iceberg::spec::DataFileFormat::Parquet,
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
    writer
        .write(batch)
        .await
        .expect("write per-partition batch");
    writer
        .close()
        .await
        .expect("close partitioned data file writer")
        .into_iter()
        .next()
        .expect("one data file per partition")
}

/// Collect the table's live (id, data) rows via the Rust scan (which applies the DVs), sorted.
async fn scan_live_rows(table: &Table) -> Vec<ScanRow> {
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
    let mut rows = Vec::new();
    for batch in &batches {
        rows.extend(extract_rows(batch));
    }
    sorted_by_id(rows)
}

// ===========================================================================================
// Phase 1 — the GEN test: Rust WRITES the complete V3+DV table; Java reads it back next.
// ===========================================================================================

/// Risk pinned (with the Java `verify-interop-dv-table` step): the WHOLE Direction-2 TABLE-level
/// write path — V3 metadata + manifest-list + delete manifest emission, the DV blob coordinates
/// travelling through the commit, and partition stamping — read back by Java's PRODUCTION scan.
/// A dropped or wrongly keyed DV resurrects its deleted rows in Java's read.
#[tokio::test]
async fn test_dv_table_gen_rust_writes_java_readable_v3_dv_table() {
    let Some(dir) = dv_table_dir() else {
        println!(
            "skipping interop_dv_table GEN — set ICEBERG_INTEROP_DV_TABLE_DIR \
             (run dev/java-interop/run-interop-dv.sh)"
        );
        return;
    };
    fs::create_dir_all(&dir).expect("create interop dir");

    // 1. A MemoryCatalog over the LOCAL FS, warehouse = <dir>, table pinned to <dir>/rust_table.
    let warehouse = dir.to_string_lossy().to_string();
    let table_location = format!("{warehouse}/rust_table");
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "interop_dv_table_gen",
            HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse.clone())]),
        )
        .await
        .expect("build MemoryCatalog over local FS");
    let table = create_v3_partitioned_rust_table(&catalog, &table_location).await;

    let schema = table.metadata().current_schema().clone();
    let spec = table.metadata().default_partition_spec().as_ref().clone();
    let partition_key_a = category_partition_key(schema.clone(), spec.clone(), "a");
    let partition_key_b = category_partition_key(schema.clone(), spec.clone(), "b");

    // 2. One REAL parquet data file PER PARTITION, fast_appended together at SEQUENCE 1.
    let data_file_a =
        write_partitioned_data_file(&table, &partition_key_a, "a", vec![10, 20, 30], vec![
            "x", "y", "z",
        ])
        .await;
    let data_file_b =
        write_partitioned_data_file(&table, &partition_key_b, "b", vec![40, 50, 60], vec![
            "p", "q", "r",
        ])
        .await;
    let data_file_a_path = data_file_a.file_path().to_string();
    let data_file_b_path = data_file_b.file_path().to_string();

    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(vec![data_file_a, data_file_b])
        .apply(tx)
        .expect("apply fast append");
    let table = tx.commit(&catalog).await.expect("commit fast append");

    // 3. D2's DVFileWriter: ONE Puffin holding TWO deletion vectors — positions {1} of the cat=a
    //    file (id 20) and positions {0,2} of the cat=b file (ids 40/60) — each in its own
    //    partition context so the DeleteFiles carry the matching partition + spec id.
    let dv_path = format!("{table_location}/data/deletes-dv.puffin");
    let output_file = table
        .file_io()
        .new_output(&dv_path)
        .expect("new puffin output");
    let mut dv_writer = DVFileWriter::new(output_file);
    dv_writer
        .delete(&data_file_a_path, 1, Some(&partition_key_a))
        .expect("record cat=a deleted position");
    for position in [0, 2] {
        dv_writer
            .delete(&data_file_b_path, position, Some(&partition_key_b))
            .expect("record cat=b deleted position");
    }
    let dv_files = dv_writer.close().await.expect("close DVFileWriter");
    assert_eq!(dv_files.len(), 2, "one DV DeleteFile per referenced file");
    for dv in &dv_files {
        assert_eq!(dv.content_type(), DataContentType::PositionDeletes);
        assert_eq!(dv.file_format(), DataFileFormat::Puffin);
        assert_eq!(
            dv.file_path(),
            dv_path,
            "both DVs live in the ONE shared puffin file (the multi-blob case)"
        );
    }

    // The committed-DV metadata Java cross-checks against the manifests it reads back
    // (the cheap metadata-level pin the canonical view's entry tuple does not carry).
    let expected_dvs: Vec<ExpectedDvMeta> = dv_files
        .iter()
        .map(|dv| ExpectedDvMeta {
            referenced_data_file: dv
                .referenced_data_file()
                .expect("DV carries referenced_data_file"),
            record_count: dv.record_count(),
            content_offset: dv.content_offset().expect("DV carries content_offset"),
            content_size_in_bytes: dv
                .content_size_in_bytes()
                .expect("DV carries content_size_in_bytes"),
        })
        .collect();
    let expected_dvs_json =
        serde_json::to_string_pretty(&expected_dvs).expect("serialize expected DV metadata");
    fs::write(dir.join("expected_dvs.json"), expected_dvs_json).expect("write expected_dvs.json");

    // 4. D3's commit path: row_delta adds BOTH DVs in ONE commit at SEQUENCE 2 (V3 gate passes,
    //    fresh-DV door passes — no prior deletes for either file).
    let tx = Transaction::new(&table);
    let tx = tx
        .row_delta()
        .add_deletes(dv_files)
        .apply(tx)
        .expect("apply row delta");
    let table = tx.commit(&catalog).await.expect("commit row delta");

    // 5. Sanity: OUR OWN scan→Arrow (D1's read path) already applies both DVs → {10,30,50}.
    let rust_rows = scan_live_rows(&table).await;
    let expected_rows = vec![
        ScanRow {
            id: 10,
            data: Some("x".to_string()),
        },
        ScanRow {
            id: 30,
            data: Some("z".to_string()),
        },
        ScanRow {
            id: 50,
            data: Some("q".to_string()),
        },
    ];
    assert_eq!(
        rust_rows, expected_rows,
        "Rust's own scan of the written table must already be {{(10,x),(30,z),(50,q)}} \
         (20 deleted from cat=a; 40/60 deleted from cat=b)"
    );

    // 6. The ground truth for Java's read + the FINAL metadata at a KNOWN path.
    let expected_json =
        serde_json::to_string_pretty(&expected_rows).expect("serialize expected rows");
    fs::write(dir.join("expected_rows.json"), expected_json).expect("write expected_rows.json");

    let final_metadata_path = format!("{table_location}/metadata/final.metadata.json");
    table
        .metadata()
        .write_to(table.file_io(), &final_metadata_path)
        .await
        .expect("write final.metadata.json");

    println!(
        "interop_dv_table GEN OK — Rust wrote {table_location} (2 partitioned parquet data files \
         + ONE puffin with TWO DVs + final.metadata.json); Rust scan = {{(10,x),(30,z),(50,q)}}. \
         Java verify-interop-dv-table reads it next."
    );
}

// ===========================================================================================
// Phase 2 — the metadata-level comparison (run AFTER the Java oracle steps): Rust's canonical
// snapshot-metadata views of BOTH tables equal Java's own view of the Java mirror table.
// ===========================================================================================

fn load_json(path: &Path) -> JsonValue {
    let raw =
        fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    serde_json::from_str(&raw).unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
}

/// Risk pinned: the DV commit's snapshot SEMANTICS — `added-dvs` (+ the other count keys), the
/// operation classification, the data/delete manifest split, and the post-inheritance sequence
/// numbers — are canonically indistinguishable from Java's `newFastAppend` + `newRowDelta`
/// DV chain, in BOTH directions (read parity on the Java table, write parity on the Rust table;
/// the third direction — Java's own view of the Rust table — is byte-diffed by the run script).
#[tokio::test]
async fn test_dv_meta_views_match_java() {
    let Some(dir) = dv_table_dir() else {
        println!(
            "skipping interop_dv_table meta — set ICEBERG_INTEROP_DV_TABLE_DIR \
             (run dev/java-interop/run-interop-dv.sh)"
        );
        return;
    };

    let java_view = load_json(&dir.join("java_meta.json"));

    // Direction 1 (READ parity): Rust's view of the JAVA-written DV chain == Java's own view.
    let java_table_metadata = dir.join("table/metadata/final.metadata.json");
    assert!(
        java_table_metadata.exists(),
        "missing {} — run the Java generate-interop-dv-table step first",
        java_table_metadata.display()
    );
    let rust_view_of_java = snapshot_meta_view(&java_table_metadata).await;
    assert_eq!(
        rust_view_of_java, java_view,
        "Rust's view of the JAVA-written V3 DV table diverges from Java's own view"
    );
    println!("dv_table: Rust view of Java table == Java view OK");

    // Direction 2 (WRITE parity): Rust's view of the RUST-written chain == Java's view too.
    let rust_table_metadata = dir.join("rust_table/metadata/final.metadata.json");
    assert!(
        rust_table_metadata.exists(),
        "missing {} — run the Rust GEN step first",
        rust_table_metadata.display()
    );
    let rust_view_of_rust = snapshot_meta_view(&rust_table_metadata).await;
    assert_eq!(
        rust_view_of_rust, java_view,
        "the RUST-written V3 DV chain's canonical metadata diverges from Java's semantics \
         for the same logical operations (fast_append + row_delta adding two DVs)"
    );
    println!("dv_table: Rust-written table metadata == Java semantics OK");
}
