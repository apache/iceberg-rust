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

//! Java interop for the DELETION-VECTOR REPLACEMENT chain (Arc-E Increment 2 — the
//! `BaseDVFileWriter.loadPreviousDeletes` merge hook). Two proofs in one fixture, driven by
//! `dev/java-interop/run-interop-dv.sh`:
//!
//! 1. **Direction-2 TABLE level (the headline).** [`test_dv_replace_gen_rust_writes_replacement_table`]
//!    commits the WHOLE replacement chain through the production Rust path at `<dir>/rust_table`:
//!    fast-append a real parquet data file (ids 10..50, positions 0..4), `row_delta` DV1 deleting
//!    position {1} (id 20), then — the new behavior — LOAD DV1's positions back through the
//!    PRODUCTION loader and feed them to a NEW `DVFileWriter` writing position {3}; the WRITER
//!    MERGES {1}∪{3} = {1,3} and returns DV1 as a rewritten file; commit
//!    `row_delta().add_deletes(merged).remove_deletes_many(rewritten)`. It emits `final.metadata.json`,
//!    `expected_rows.json` (= {10,30,50}), and `expected_dvs.json` (the ONE surviving merged DV). The
//!    Java oracle's `verify-interop-dv-replace` mode reads the table with its PRODUCTION scan and
//!    asserts the live rows AND cross-checks the manifests: exactly ONE live DV (DV1 ABSENT — a
//!    Java-visible manifest check that the replacement actually removed the old DV).
//!
//! 2. **Metadata-level chain (the E1-family extension — `removed-dvs`'s first LIVE comparison).**
//!    BOTH sides run the SAME logical chain {fast_append, row_delta(DV1), row_delta(add merged DV2 +
//!    remove DV1)} on equivalent V3 tables — the Java side via `generate-interop-dv-replace`
//!    (`BaseDVFileWriter` with a real `loadPreviousDeletes`). The canonical snapshot-metadata views
//!    are compared the established three ways (Java's own view, Java's view of the Rust table
//!    byte-diffed by the run script, and Rust's views of both here). This pins the `removed-dvs`
//!    summary key (in the allowlist, first live comparison), the operation classification of the
//!    replacement commit, and the data/delete manifest tombstone semantics.
//!
//! 3. **The Run-store re-serialization byte question (the D2 tie caveat, now LIVE).** The GEN also
//!    emits the MERGED DV's raw blob bytes (`rust_merged_dv_blob.bin`); the Java oracle performs the
//!    SAME merge (a `BaseDVFileWriter` whose `loadPreviousDeletes` returns DV1's deserialized index)
//!    and dumps its merged blob (`java_merged_dv_blob.bin`). [`test_dv_replace_merged_blob_bytes`]
//!    byte-compares them: for this fixture (previous {1} → array container, merged {1,3} → array)
//!    NO Run-store store sits on the re-serialization tie, so the bytes match. Whether they match for
//!    ALL inputs is the documented D2 caveat (a previous DV whose store is ALREADY a Run container
//!    re-serializes at the `cardinality == 2·runs` tie to an array on `roaring-rs` vs a run on Java —
//!    positions identical, bytes may differ; Java reads our blob either way, proven by the table-level
//!    read above). The byte-compare here is a POSITIVE pin for the array-container case, not a claim
//!    of universal byte parity.
//!
//! THE ENV GATE. All tests are clean NO-OPS (runtime early-return, not `#[ignore]`) unless
//! `ICEBERG_INTEROP_DV_REPLACE_DIR` is set non-empty, so the offline `cargo test` gate needs no
//! Java/Maven. Run phases individually via the test-name filters in `run-interop-dv.sh`.

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
    DataFile, DataFileFormat, FormatVersion, NestedField, PartitionKey, PartitionSpec,
    PrimitiveType, Schema, SchemaRef, SortOrder, Struct, Type, UnboundPartitionSpec,
};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::base_writer::deletion_vector_writer::{
    DVFileWriter, DVWriteResult, PreviousDeletes,
};
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
// Shared model + env gate.
// ===========================================================================================

/// One live row of the merge-on-read read: `id` (long) + nullable `data` string (field 3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ScanRow {
    id: i64,
    data: Option<String>,
}

/// The single surviving (merged) DV's committed metadata, cross-checked by the Java oracle against
/// the delete manifests it reads back. The replacement must leave exactly ONE such entry.
#[derive(Debug, Serialize, Deserialize)]
struct ExpectedDvMeta {
    referenced_data_file: String,
    record_count: u64,
    content_offset: i64,
    content_size_in_bytes: i64,
}

fn dv_replace_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_DV_REPLACE_DIR")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

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
// The table machinery (the interop_dv_table template, unpartitioned).
// ===========================================================================================

/// {1 id long required, 2 unused string optional, 3 data string optional}; the `data` column (field
/// 3) is the comparison column, mirroring the other DV oracles' convention.
fn replace_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "category", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(3, "data", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("build replace schema")
}

async fn create_v3_table(catalog: &impl Catalog, table_location: &str) -> Table {
    let namespace = NamespaceIdent::new("interop".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create namespace");

    let creation = TableCreation::builder()
        .name("rust_table".to_string())
        .location(table_location.to_string())
        .schema(replace_schema())
        .partition_spec(UnboundPartitionSpec::builder().with_spec_id(0).build())
        .sort_order(SortOrder::unsorted_order())
        .format_version(FormatVersion::V3)
        .build();

    catalog
        .create_table(&namespace, creation)
        .await
        .expect("create V3 rust_table")
}

/// Write a REAL parquet data file with rows (id, data) via the production `DataFileWriter` (no
/// partition key — unpartitioned table).
async fn write_data_file(table: &Table, rows: &[(i64, &str)]) -> DataFile {
    use iceberg::arrow::schema_to_arrow_schema;

    let schema = table.metadata().current_schema();
    let arrow_schema = Arc::new(schema_to_arrow_schema(schema).expect("iceberg → arrow"));

    let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
    let categories: Vec<Option<&str>> = rows.iter().map(|_| None).collect();
    let data_values: Vec<&str> = rows.iter().map(|(_, data)| *data).collect();
    let batch = RecordBatch::try_new(arrow_schema, vec![
        Arc::new(Int64Array::from(ids)) as ArrayRef,
        Arc::new(StringArray::from(categories)) as ArrayRef,
        Arc::new(StringArray::from(data_values)) as ArrayRef,
    ])
    .expect("build data batch");

    let location_gen =
        DefaultLocationGenerator::new(table.metadata().clone()).expect("location generator");
    let file_name_gen = DefaultFileNameGenerator::new(
        "rust-data".to_string(),
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
        .expect("build data file writer");
    writer.write(batch).await.expect("write batch");
    writer
        .close()
        .await
        .expect("close data file writer")
        .into_iter()
        .next()
        .expect("one data file")
}

fn unpartitioned_key(schema: SchemaRef, spec: PartitionSpec) -> PartitionKey {
    PartitionKey::new(spec, schema, Struct::empty())
}

/// Write a fresh DV for `data_file_path` at `positions` (no merge) via `DVFileWriter`.
async fn write_dv(
    table: &Table,
    file_name: &str,
    data_file_path: &str,
    positions: &[u64],
) -> DataFile {
    let schema = table.metadata().current_schema().clone();
    let spec = table.metadata().default_partition_spec().as_ref().clone();
    let partition_key = unpartitioned_key(schema, spec);
    let dv_path = format!("{}/data/{}", table.metadata().location(), file_name);
    let output_file = table.file_io().new_output(&dv_path).expect("new output");
    let mut writer = DVFileWriter::new(output_file);
    for pos in positions {
        writer
            .delete(data_file_path, *pos, Some(&partition_key))
            .expect("record delete");
    }
    writer
        .close()
        .await
        .expect("close DV writer")
        .into_iter()
        .next()
        .expect("one DV")
}

/// Load a committed DV's positions back through the PRODUCTION DECODER — read the blob bytes off
/// disk at the recorded coordinates and decode with [`DeleteVector::deserialize_deletion_vector_v1`]
/// (the SAME `deletion-vector-v1` decoder the scan-side loader uses internally). The engine's
/// `loadPreviousDeletes` reads the existing DV off disk; this mirrors it without the caching layer.
async fn load_dv_positions(
    table: &Table,
    dv_file: &DataFile,
) -> iceberg::delete_vector::DeleteVector {
    use iceberg::delete_vector::DeleteVector;

    let blob = table
        .file_io()
        .new_input(dv_file.file_path())
        .expect("new input for the DV puffin")
        .read()
        .await
        .expect("read the DV puffin bytes");
    let offset = usize::try_from(dv_file.content_offset().expect("offset")).expect("usize");
    let size = usize::try_from(dv_file.content_size_in_bytes().expect("size")).expect("usize");
    DeleteVector::deserialize_deletion_vector_v1(&blob[offset..offset + size])
        .expect("decode the committed DV blob via the production decoder")
}

/// Write a MERGED DV via the writer hook: union `previous_positions` (sourced from `previous_dv`)
/// into the new positions, returning the merged DV + the rewritten (superseded) files.
async fn write_merged_dv(
    table: &Table,
    file_name: &str,
    data_file_path: &str,
    new_positions: &[u64],
    previous_positions: iceberg::delete_vector::DeleteVector,
    previous_dv: DataFile,
) -> DVWriteResult {
    let schema = table.metadata().current_schema().clone();
    let spec = table.metadata().default_partition_spec().as_ref().clone();
    let partition_key = unpartitioned_key(schema, spec);
    let dv_path = format!("{}/data/{}", table.metadata().location(), file_name);
    let output_file = table.file_io().new_output(&dv_path).expect("new output");
    let previous = PreviousDeletes::new(previous_positions, vec![previous_dv]);
    let mut writer = DVFileWriter::new(output_file)
        .with_previous_deletes(HashMap::from([(data_file_path.to_string(), previous)]));
    for pos in new_positions {
        writer
            .delete(data_file_path, *pos, Some(&partition_key))
            .expect("record new delete");
    }
    writer.close_with_result().await.expect("close with result")
}

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
// Phase 1 — the GEN test: Rust commits the REPLACEMENT chain via the merge hook.
// ===========================================================================================

/// Risk pinned (with `verify-interop-dv-replace`): the WHOLE replacement chain read back by Java —
/// the WRITER-merged DV deletes the UNION of old + new, the old DV is REMOVED (absent from the
/// manifests), and Java's production scan sees the survivors. A broken merge resurrects the old
/// positions; a broken removal leaves two live DVs (the read door fails) or keeps the stale DV.
#[tokio::test]
async fn test_dv_replace_gen_rust_writes_replacement_table() {
    let Some(dir) = dv_replace_dir() else {
        println!(
            "skipping interop_dv_replace GEN — set ICEBERG_INTEROP_DV_REPLACE_DIR \
             (run dev/java-interop/run-interop-dv.sh)"
        );
        return;
    };
    fs::create_dir_all(&dir).expect("create interop dir");

    let warehouse = dir.to_string_lossy().to_string();
    let table_location = format!("{warehouse}/rust_table");
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "interop_dv_replace_gen",
            HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse.clone())]),
        )
        .await
        .expect("build MemoryCatalog over local FS");
    let table = create_v3_table(&catalog, &table_location).await;

    // 1. A real parquet data file: ids 10..50 at positions 0..4.
    let data_file = write_data_file(&table, &[
        (10, "x"),
        (20, "y"),
        (30, "z"),
        (40, "p"),
        (50, "q"),
    ])
    .await;
    let data_file_path = data_file.file_path().to_string();
    let table = {
        let tx = Transaction::new(&table);
        let tx = tx
            .fast_append()
            .add_data_files(vec![data_file])
            .apply(tx)
            .expect("apply fast append");
        tx.commit(&catalog).await.expect("commit fast append")
    };

    // 2. DV1 deletes position {1} (id 20). Commit via row_delta.
    let dv1 = write_dv(&table, "dv1.puffin", &data_file_path, &[1]).await;
    let table = {
        let tx = Transaction::new(&table);
        let tx = tx
            .row_delta()
            .add_deletes(vec![dv1.clone()])
            .apply(tx)
            .expect("apply DV1 row delta");
        tx.commit(&catalog).await.expect("commit DV1")
    };

    // 3. Load DV1's positions back via the PRODUCTION loader, then MERGE via the writer hook: new
    //    position {3} ∪ previous {1} → merged {1,3}; rewritten = [DV1].
    let previous_positions = load_dv_positions(&table, &dv1).await;
    let merge_result = write_merged_dv(
        &table,
        "dv2.puffin",
        &data_file_path,
        &[3],
        previous_positions,
        dv1.clone(),
    )
    .await;
    assert_eq!(merge_result.delete_files.len(), 1, "one merged DV");
    let merged_dv = merge_result.delete_files[0].clone();
    assert_eq!(
        merged_dv.record_count(),
        2,
        "the merged DV must carry the UNION {{1,3}}"
    );
    assert_eq!(
        merge_result.rewritten_delete_files.len(),
        1,
        "rewritten=[DV1]"
    );

    // The RAW merged blob bytes for the Run-store byte-compare with Java's equivalent merge.
    let merged_puffin = fs::read(merged_dv.file_path()).expect("read merged puffin");
    let offset = usize::try_from(merged_dv.content_offset().expect("offset")).expect("usize");
    let size = usize::try_from(merged_dv.content_size_in_bytes().expect("size")).expect("usize");
    fs::write(
        dir.join("rust_merged_dv_blob.bin"),
        &merged_puffin[offset..offset + size],
    )
    .expect("write rust_merged_dv_blob.bin");

    // 4. Commit add(merged) + remove(rewritten DV1) — the engine flow.
    let table = {
        let tx = Transaction::new(&table);
        let tx = tx
            .row_delta()
            .add_deletes(merge_result.delete_files)
            .remove_deletes_many(merge_result.rewritten_delete_files)
            .apply(tx)
            .expect("apply replacement row delta");
        tx.commit(&catalog).await.expect("commit replacement")
    };

    // 5. Sanity: OUR scan applies the merged DV → {10,30,50}.
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
        "Rust's own scan of the replacement table must be {{(10,x),(30,z),(50,q)}}"
    );

    // The ONE surviving merged DV's metadata (the verify cross-checks DV1 is ABSENT).
    let expected_dvs = vec![ExpectedDvMeta {
        referenced_data_file: merged_dv
            .referenced_data_file()
            .expect("merged DV carries referenced_data_file"),
        record_count: merged_dv.record_count(),
        content_offset: merged_dv.content_offset().expect("offset"),
        content_size_in_bytes: merged_dv.content_size_in_bytes().expect("size"),
    }];

    fs::write(
        dir.join("expected_rows.json"),
        serde_json::to_string_pretty(&expected_rows).expect("serialize rows"),
    )
    .expect("write expected_rows.json");
    fs::write(
        dir.join("expected_dvs.json"),
        serde_json::to_string_pretty(&expected_dvs).expect("serialize DVs"),
    )
    .expect("write expected_dvs.json");

    let final_metadata_path = format!("{table_location}/metadata/final.metadata.json");
    table
        .metadata()
        .write_to(table.file_io(), &final_metadata_path)
        .await
        .expect("write final.metadata.json");

    println!(
        "interop_dv_replace GEN OK — Rust committed the replacement chain at {table_location} \
         (fast_append + DV1{{1}} + writer-merged DV2{{1,3}} replacing DV1); scan = {{10,30,50}}; \
         emitted rust_merged_dv_blob.bin for the byte-compare."
    );
}

// ===========================================================================================
// Phase 2 — the metadata-level comparison (run AFTER the Java oracle steps).
// ===========================================================================================

fn load_json(path: &Path) -> JsonValue {
    let raw =
        fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    serde_json::from_str(&raw).unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
}

/// Risk pinned: the replacement chain's snapshot SEMANTICS — `removed-dvs` (its first LIVE
/// comparison), `added-dvs`, operation classification, the delete-manifest tombstone of the removed
/// DV, post-inheritance seqs — are canonically indistinguishable from Java's equivalent
/// `loadPreviousDeletes` merge + `removeDeletes` chain, in BOTH directions.
#[tokio::test]
async fn test_dv_replace_meta_views_match_java() {
    let Some(dir) = dv_replace_dir() else {
        println!(
            "skipping interop_dv_replace meta — set ICEBERG_INTEROP_DV_REPLACE_DIR \
             (run dev/java-interop/run-interop-dv.sh)"
        );
        return;
    };

    let java_view = load_json(&dir.join("java_meta.json"));

    // Direction 1 (READ parity): Rust's view of the JAVA-written replacement chain == Java's view.
    let java_table_metadata = dir.join("table/metadata/final.metadata.json");
    assert!(
        java_table_metadata.exists(),
        "missing {} — run the Java generate-interop-dv-replace step first",
        java_table_metadata.display()
    );
    let rust_view_of_java = snapshot_meta_view(&java_table_metadata).await;
    assert_eq!(
        rust_view_of_java, java_view,
        "Rust's view of the JAVA-written replacement chain diverges from Java's own view"
    );
    println!("dv_replace: Rust view of Java table == Java view OK");

    // Direction 2 (WRITE parity): Rust's view of the RUST-written replacement chain == Java's view.
    let rust_table_metadata = dir.join("rust_table/metadata/final.metadata.json");
    assert!(
        rust_table_metadata.exists(),
        "missing {} — run the Rust GEN step first",
        rust_table_metadata.display()
    );
    let rust_view_of_rust = snapshot_meta_view(&rust_table_metadata).await;
    assert_eq!(
        rust_view_of_rust, java_view,
        "the RUST-written replacement chain's canonical metadata diverges from Java's semantics \
         (removed-dvs / added-dvs / operation / tombstone)"
    );
    println!("dv_replace: Rust-written table metadata == Java semantics OK");
}

// ===========================================================================================
// Phase 3 — the merged-blob byte-compare (the Run-store re-serialization question).
// ===========================================================================================

/// Risk pinned (the D2 Run-store tie, now exercised): the MERGED blob (previous {1} ∪ new {3} =
/// {1,3}) Rust re-serializes after deserializing DV1 must be BYTE-IDENTICAL to Java's blob from the
/// SAME merge — for this array-container fixture (no Run store on the re-serialization tie). This is
/// a POSITIVE pin for the array case; the universal claim is documented (the tie can diverge in
/// bytes only, never positions — Java reads our blob either way, proven by the table-level read).
#[tokio::test]
async fn test_dv_replace_merged_blob_bytes() {
    let Some(dir) = dv_replace_dir() else {
        println!(
            "skipping interop_dv_replace byte-compare — set ICEBERG_INTEROP_DV_REPLACE_DIR \
             (run dev/java-interop/run-interop-dv.sh)"
        );
        return;
    };

    let rust_blob = fs::read(dir.join("rust_merged_dv_blob.bin"))
        .expect("read rust_merged_dv_blob.bin (run the Rust GEN step first)");
    let java_blob_path = dir.join("java_merged_dv_blob.bin");
    let java_blob = fs::read(&java_blob_path).unwrap_or_else(|error| {
        panic!(
            "read {} (did the Java verify-interop-dv-replace step run?): {error}",
            java_blob_path.display()
        )
    });
    assert_eq!(
        rust_blob, java_blob,
        "the merged DV blob (previous {{1}} ∪ new {{3}}) must be byte-identical to Java's merge \
         (array container, no Run-store tie)"
    );
    println!(
        "dv_replace: merged DV blob byte-identical to Java ({} bytes)",
        rust_blob.len()
    );
}
