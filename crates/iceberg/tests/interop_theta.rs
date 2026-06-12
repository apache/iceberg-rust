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

//! Theta-blob Puffin interop harness (increment I1) — bidirectional proof that
//! `apache-datasketches-theta-v1` Puffin blobs are compatible between Rust and Java.
//!
//! # Fixture shape
//!
//! Unpartitioned V2 table, schema `{1 id long, 2 name string, 3 val long}`:
//! - **Exact mode:** `id` (5 distinct) and `name` (2 distinct) columns stay in exact mode.
//! - **Estimation mode:** `val` column receives 1 000 000 distinct longs, engaging Alpha sampling.
//!   The known pin: lgK12 / seed 9001 / n=1M → compact ndv **1 004 032** (Java probe).
//!
//! # Direction 1 (GEN: Rust writes, Java judges)
//!
//! [`test_theta_gen`] builds a real unpartitioned table with REAL parquet data, runs
//! [`ComputeTableStats::execute`], reads the registered blobs back, and writes:
//! - `rust_stats.puffin` — a copy of the registered Puffin statistics file.
//! - `rust_stats_expected.json` — the expected blob metadata (field_id, ndv, snapshot_id,
//!   sequence_number, mode) for Java's verify step.
//!
//! The run script passes these to Java's `verify-interop-theta` which reads the Puffin via
//! the PRODUCTION `PuffinReader`, checks blob type/fields/snapshot_id/seq_num/ndv property,
//! and asserts `(long) CompactSketch.wrap(bytes).getEstimate() == ndv` integer-exact.
//!
//! # Direction 2 (Rust reads Java-written puffin)
//!
//! [`test_theta_d2_rust_reads_java_puffin`] reads `java_stats.puffin` (written by Java's
//! `generate-interop-theta-java-to-rust`) through the PRODUCTION Rust `PuffinReader` +
//! `CompactThetaSketch::deserialize` path and verifies each blob's ndv against
//! `java_stats_expected.json`.  Covers both exact mode and estimation mode.
//!
//! # Estimation-mode pin
//!
//! The `val` column accumulates 1 000 000 distinct longs fed as 8-byte little-endian (the
//! Iceberg single-value byte form for `long`, == Java `Conversions.toByteBuffer`).  The
//! compact-sketch estimate MUST be 1 004 032 (the Java-probe pinned value); the Alpha update
//! sketch's own sampling estimate is 1 002 319 — a different value.  This test pins the
//! CORRECT object (the compact sketch's estimate, matching Java's NDVSketchUtil).
//!
//! # Env gate
//!
//! Tests are clean NO-OPS (runtime early-return, not `#[ignore]`) unless their env var is set
//! non-empty — the offline `cargo test` gate needs no Java/Maven.
//! - `ICEBERG_INTEROP_THETA_GEN_DIR` — GEN path (Direction 1, Rust writes).
//! - `ICEBERG_INTEROP_THETA_DIR` — compare path (Direction 2, Rust reads Java's file).

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::io::{FileIO, LocalFsStorageFactory};
use iceberg::maintenance::ComputeTableStats;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::puffin::{APACHE_DATASKETCHES_THETA_V1, PuffinReader};
use iceberg::spec::{
    DataContentType, DataFile, FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema,
    Struct, Type,
};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::file_writer::{FileWriter, FileWriterBuilder, ParquetWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation};
use iceberg_sketches::CompactThetaSketch;
use serde_json::{Value as JsonValue, json};

// ===========================================================================================
// Known estimation-mode pin (lgK12 / seed 9001 / n = 1M distinct longs, Alpha family).
// ===========================================================================================

/// The compact-sketch estimate for 1M distinct longs at lgK=12 / seed=9001 / Alpha family.
/// Java probe (datasketches-java-3.3.0): `CompactSketch.wrap(bytes).getEstimate()` = 1004032.
/// The Alpha update sketch's sampling estimate (1002319) is the WRONG value.
const ESTIMATION_NDV_PIN: i64 = 1_004_032;

/// The number of distinct longs fed to the estimation-mode column.
const ESTIMATION_DISTINCT_COUNT: i64 = 1_000_000;

// ===========================================================================================
// Env-gate helpers.
// ===========================================================================================

fn gen_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_THETA_GEN_DIR")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn compare_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_THETA_DIR")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

// ===========================================================================================
// Schema — `{1 id long required, 2 name string required, 3 val long required}`.
// ===========================================================================================

fn fixture_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(3, "val", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()
        .expect("build fixture schema")
}

// ===========================================================================================
// Data file writer helper — same pattern as compute_table_stats.rs tests.
// ===========================================================================================

/// Writes a REAL parquet data file with the given (id, name, val) rows into the table location.
async fn write_data_file(
    table: &iceberg::table::Table,
    file_name: &str,
    ids: &[i64],
    names: &[&str],
    vals: &[i64],
) -> DataFile {
    let schema = table.metadata().current_schema();
    let arrow_schema = Arc::new(schema_to_arrow_schema(schema).expect("arrow schema"));

    let batch = RecordBatch::try_new(arrow_schema, vec![
        Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef,
        Arc::new(StringArray::from(names.to_vec())) as ArrayRef,
        Arc::new(Int64Array::from(vals.to_vec())) as ArrayRef,
    ])
    .expect("record batch");

    let file_path = format!("{}/data/{file_name}", table.metadata().location());
    let output = table.file_io().new_output(file_path).expect("new output");
    let parquet_builder = ParquetWriterBuilder::new(
        parquet::file::properties::WriterProperties::builder().build(),
        schema.clone(),
    );
    let mut writer = parquet_builder.build(output).await.expect("build writer");
    writer.write(&batch).await.expect("write batch");
    let data_file_builders = writer.close().await.expect("close writer");

    let mut builder = data_file_builders
        .into_iter()
        .next()
        .expect("one data file builder");
    builder
        .content(DataContentType::Data)
        .partition_spec_id(0)
        .partition(Struct::empty())
        .build()
        .expect("build data file")
}

// ===========================================================================================
// Shared JSON helper.
// ===========================================================================================

fn load_expected_blobs(path: &Path) -> Vec<JsonValue> {
    let raw =
        fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    let value: JsonValue = serde_json::from_str(&raw)
        .unwrap_or_else(|error| panic!("parse {}: {error}", path.display()));
    value
        .as_array()
        .unwrap_or_else(|| panic!("{} must be a JSON array", path.display()))
        .clone()
}

// ===========================================================================================
// Direction 1 — GEN: Rust builds the fixture + calls ComputeTableStats.
// ===========================================================================================

/// GEN test: build a two-file fixture, run ComputeTableStats, write `rust_stats.puffin` and
/// `rust_stats_expected.json`.
///
/// The fixture has two files:
/// - **File A (small):** 5 rows — id = {1..5}, name = {"a","b","a","b","a"}, val = {10..50}.
///   This puts `id` (5 distinct), `name` (2 distinct), `val` (5 distinct) in exact mode after
///   this file alone.
/// - **File B (large estimation):** 1 000 000 rows — id = {0..999999}, val = {0..999999},
///   name = "x" for every row (does not add new name distinct values, just duplicates).
///   After both files: `val` is in estimation mode (θ < MAX); `id` is also in estimation mode;
///   `name` stays in exact mode (at most 3 distinct: "a", "b", "x").
///
/// The `val` column's ndv MUST equal the pinned value 1 004 032.
#[tokio::test]
async fn test_theta_gen() {
    let Some(gen_dir) = gen_dir() else {
        println!(
            "skipping interop_theta GEN — set ICEBERG_INTEROP_THETA_GEN_DIR \
             (run dev/java-interop/run-interop-theta.sh)"
        );
        return;
    };

    fs::create_dir_all(gen_dir.join("rust_table/metadata"))
        .expect("create rust_table/metadata dir");
    fs::create_dir_all(gen_dir.join("rust_table/data")).expect("create rust_table/data dir");

    let table_location = gen_dir.join("rust_table").to_string_lossy().to_string();

    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "interop_theta_gen",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_string(),
                gen_dir.to_string_lossy().to_string(),
            )]),
        )
        .await
        .expect("build MemoryCatalog");

    let namespace = NamespaceIdent::new("interop".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create namespace");

    let schema = fixture_schema();
    let creation = TableCreation::builder()
        .name("rust_table".to_string())
        .location(table_location.clone())
        .schema(schema.clone())
        .partition_spec(PartitionSpec::unpartition_spec())
        .format_version(FormatVersion::V2)
        .build();
    let mut table = catalog
        .create_table(&namespace, creation)
        .await
        .expect("create table");

    // File A: 5 rows — exact mode for all columns.
    let exact_ids = vec![1i64, 2, 3, 4, 5];
    let exact_names = vec!["a", "b", "a", "b", "a"];
    let exact_vals = vec![10i64, 20, 30, 40, 50];
    let file_a = write_data_file(
        &table,
        "file_a.parquet",
        &exact_ids,
        &exact_names,
        &exact_vals,
    )
    .await;

    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(vec![file_a])
        .apply(tx)
        .expect("apply file_a fast_append");
    table = tx.commit(&catalog).await.expect("commit file_a");
    println!(
        "interop_theta GEN: file_a appended (snapshot_id={})",
        table.metadata().current_snapshot_id().unwrap_or(-1)
    );

    // File B: 1M rows — val = {0..999999} feeds 1M distinct longs into the val sketch.
    // We write in a single batch (1M i64 rows is ~8 MB, manageable).
    let est_count = ESTIMATION_DISTINCT_COUNT as usize;
    let est_ids: Vec<i64> = (0..ESTIMATION_DISTINCT_COUNT).collect();
    let est_names: Vec<&str> = vec!["x"; est_count];
    let est_vals: Vec<i64> = (0..ESTIMATION_DISTINCT_COUNT).collect();

    let file_b = write_data_file(&table, "file_b.parquet", &est_ids, &est_names, &est_vals).await;

    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(vec![file_b])
        .apply(tx)
        .expect("apply file_b fast_append");
    table = tx.commit(&catalog).await.expect("commit file_b");
    println!(
        "interop_theta GEN: file_b appended (snapshot_id={}, est rows={})",
        table.metadata().current_snapshot_id().unwrap_or(-1),
        ESTIMATION_DISTINCT_COUNT
    );

    // Run ComputeTableStats over all 3 primitive columns.
    let snapshot_id = table
        .metadata()
        .current_snapshot_id()
        .expect("current snapshot id");
    let sequence_number = table
        .metadata()
        .current_snapshot()
        .expect("current snapshot")
        .sequence_number();

    println!(
        "interop_theta GEN: running ComputeTableStats over snapshot_id={snapshot_id} seq={sequence_number}"
    );

    let stats_result = ComputeTableStats::new(table.clone())
        .execute(&catalog)
        .await
        .expect("ComputeTableStats::execute");

    let stats_file = &stats_result.statistics_file;
    let puffin_path = &stats_file.statistics_path;

    println!(
        "interop_theta GEN: puffin written at {puffin_path} \
         (blobs={}, file_size={}, footer_size={})",
        stats_file.blob_metadata.len(),
        stats_file.file_size_in_bytes,
        stats_file.file_footer_size_in_bytes
    );

    // Copy the puffin to the stable `rust_stats.puffin` path in the gen dir.
    // The production puffin is written under the table location; we need a copy at a known path
    // for the shell script to reference.
    let dest_puffin = gen_dir.join("rust_stats.puffin");
    fs::copy(puffin_path, &dest_puffin)
        .unwrap_or_else(|error| panic!("copy {puffin_path} -> {}: {error}", dest_puffin.display()));
    println!(
        "interop_theta GEN: rust_stats.puffin copied to {}",
        dest_puffin.display()
    );

    // Read the blobs back to build rust_stats_expected.json.
    let input_file = table
        .file_io()
        .new_input(puffin_path)
        .expect("new input for puffin");
    let reader = PuffinReader::new(input_file);
    let file_metadata = reader
        .file_metadata()
        .await
        .expect("read puffin metadata")
        .clone();

    assert_eq!(
        file_metadata.blobs().len(),
        3,
        "expected 3 blobs (one per column: id/name/val)"
    );

    let mut expected_blobs: Vec<JsonValue> = Vec::new();

    for blob_meta in file_metadata.blobs() {
        let blob = reader.blob(blob_meta).await.expect("read blob data");

        assert_eq!(
            blob.blob_type(),
            APACHE_DATASKETCHES_THETA_V1,
            "blob type must be apache-datasketches-theta-v1"
        );
        assert_eq!(blob.fields().len(), 1, "each blob has exactly one field id");

        let field_id = blob.fields()[0];
        let ndv_str = blob
            .properties()
            .get("ndv")
            .cloned()
            .expect("ndv property missing");
        let ndv: i64 = ndv_str
            .parse()
            .expect("ndv property must be a valid integer string");

        // Verify the compact sketch estimate matches the ndv property (the core assertion).
        let sketch =
            CompactThetaSketch::deserialize(blob.data()).expect("deserialize compact theta sketch");
        let estimate_as_long = sketch.estimate() as i64;
        assert_eq!(
            estimate_as_long, ndv,
            "field_id={field_id}: compact sketch estimate {estimate_as_long} != ndv {ndv}"
        );

        // The `val` column (field_id=3) must hit the estimation pin.
        if field_id == 3 {
            assert_eq!(
                ndv, ESTIMATION_NDV_PIN,
                "val column (field_id=3) ndv must equal the Java-pinned value {ESTIMATION_NDV_PIN} \
                 for lgK12/seed9001/n=1M Alpha sketch"
            );
        }

        // Classify the mode for the JSON (not load-bearing for the interop check, informational).
        let mode = if ndv <= 10 { "exact" } else { "estimation" };

        println!(
            "interop_theta GEN: blob field_id={field_id} ndv={ndv} mode={mode} \
             estimate={estimate_as_long}"
        );

        expected_blobs.push(json!({
            "field_id": field_id,
            "ndv": ndv,
            "snapshot_id": snapshot_id,
            "sequence_number": sequence_number,
            "mode": mode,
        }));
    }

    // Write rust_stats_expected.json.
    let expected_path = gen_dir.join("rust_stats_expected.json");
    let expected_json = JsonValue::Array(expected_blobs);
    fs::write(
        &expected_path,
        serde_json::to_string_pretty(&expected_json).expect("serialize expected"),
    )
    .expect("write rust_stats_expected.json");
    println!(
        "interop_theta GEN: rust_stats_expected.json written at {}",
        expected_path.display()
    );

    // Write final.metadata.json so the shell script can locate the stats file.
    let final_meta_path = gen_dir.join("rust_table/metadata/final.metadata.json");
    stats_result
        .table
        .metadata_ref()
        .write_to(
            stats_result.table.file_io(),
            final_meta_path.to_str().expect("utf8 path"),
        )
        .await
        .expect("write final.metadata.json");
    println!("interop_theta GEN: final.metadata.json written");

    println!(
        "interop_theta GEN: all assertions PASSED — \
         snapshot_id={snapshot_id} seq={sequence_number} blobs=3 \
         val_ndv={ESTIMATION_NDV_PIN}"
    );
}

// ===========================================================================================
// Direction 2 — Rust reads Java-written puffin.
// ===========================================================================================

/// Direction 2: Rust reads the Java-written `java_stats.puffin` via the PRODUCTION
/// `PuffinReader` + `CompactThetaSketch::deserialize` path and verifies each blob's metadata
/// against `java_stats_expected.json`.
///
/// This proves the Rust production reader can decode a Java-written
/// `apache-datasketches-theta-v1` blob faithfully.  Covers both exact mode (small ndv) and
/// estimation mode (ndv == 1 004 032, the Java-probe pin).
#[tokio::test]
async fn test_theta_d2_rust_reads_java_puffin() {
    let Some(dir) = compare_dir() else {
        println!(
            "skipping interop_theta D2 — set ICEBERG_INTEROP_THETA_DIR \
             (run dev/java-interop/run-interop-theta.sh)"
        );
        return;
    };

    let puffin_path = dir.join("java_stats.puffin");
    assert!(
        puffin_path.exists(),
        "D2: missing {}; run Java generate-interop-theta-java-to-rust first",
        puffin_path.display()
    );

    let expected_path = dir.join("java_stats_expected.json");
    assert!(
        expected_path.exists(),
        "D2: missing {}",
        expected_path.display()
    );

    let expected_blobs = load_expected_blobs(&expected_path);

    // Build a FileIO for local filesystem access.
    let file_io = FileIO::new_with_fs();

    let input_file = file_io
        .new_input(puffin_path.to_str().expect("utf8 puffin path"))
        .expect("new_input for java_stats.puffin");

    let reader = PuffinReader::new(input_file);
    let file_metadata = reader
        .file_metadata()
        .await
        .expect("read java puffin metadata")
        .clone();

    assert_eq!(
        file_metadata.blobs().len(),
        expected_blobs.len(),
        "D2: blob count mismatch: puffin has {} blobs, expected.json has {}",
        file_metadata.blobs().len(),
        expected_blobs.len()
    );

    for (index, (blob_metadata, expected)) in file_metadata
        .blobs()
        .iter()
        .zip(expected_blobs.iter())
        .enumerate()
    {
        let mode = expected["mode"].as_str().unwrap_or("unknown");
        let label = format!("D2 blob[{index}] mode={mode}");

        // 1. Blob type.
        assert_eq!(
            blob_metadata.blob_type(),
            APACHE_DATASKETCHES_THETA_V1,
            "{label}: blob type"
        );

        // 2. fields == [field_id].
        let expected_field_id = expected["field_id"].as_i64().expect("field_id in expected") as i32;
        assert_eq!(
            blob_metadata.fields(),
            &[expected_field_id],
            "{label}: fields"
        );

        // 3. snapshot-id.
        let expected_snapshot_id = expected["snapshot_id"].as_i64().expect("snapshot_id");
        assert_eq!(
            blob_metadata.snapshot_id(),
            expected_snapshot_id,
            "{label}: snapshot_id"
        );

        // 4. sequence-number.
        let expected_seq = expected["sequence_number"]
            .as_i64()
            .expect("sequence_number");
        assert_eq!(
            blob_metadata.sequence_number(),
            expected_seq,
            "{label}: sequence_number"
        );

        // 5. ndv property (string).
        let expected_ndv: i64 = expected["ndv"].as_i64().expect("ndv");
        let actual_ndv_str = blob_metadata
            .properties()
            .get("ndv")
            .cloned()
            .expect("ndv property missing from blob");
        assert_eq!(
            actual_ndv_str,
            expected_ndv.to_string(),
            "{label}: ndv property string"
        );

        // 6. Read the blob payload and deserialize via production path.
        let blob = reader.blob(blob_metadata).await.expect("read blob data");
        let sketch =
            CompactThetaSketch::deserialize(blob.data()).expect("CompactThetaSketch::deserialize");
        let estimate_as_long = sketch.estimate() as i64;

        assert_eq!(
            estimate_as_long, expected_ndv,
            "{label}: CompactThetaSketch::deserialize.estimate() as i64 == ndv \
             (integer-exact; actual={estimate_as_long} expected={expected_ndv})"
        );

        // Estimation-mode pin: field_id=3 (val column) must be the Java-probe pin.
        if mode == "estimation" && expected_field_id == 3 {
            assert_eq!(
                expected_ndv, ESTIMATION_NDV_PIN,
                "{label}: estimation-mode val column ndv must be the Java-pinned value \
                 {ESTIMATION_NDV_PIN}"
            );
        }

        println!(
            "interop_theta D2 {label}: type/fields/snapshot_id/seq_num/ndv all match; \
             estimate={estimate_as_long}"
        );
    }

    println!(
        "interop_theta D2: PASS — {} blobs decoded from Java puffin, all metadata verified \
         (exact mode + estimation mode)",
        expected_blobs.len()
    );
}
