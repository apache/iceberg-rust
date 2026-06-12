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

//! METADATA-LEVEL multi-spec interop fixture (Z2) — proving that a Rust-written table produced
//! via spec evolution and a SINGLE multi-spec fast-append commit (carrying files under BOTH the
//! old spec and the new spec in ONE snapshot) matches Java 1.10.0 byte-for-byte in the canonical
//! snapshot-metadata view.
//!
//! # The chain (both sides, identical logical constants; V2 table; NO parquet)
//!
//! - ms1: `fast_append` F1(a="x", rc=10) under spec 0 [identity(a)]          seq 1, op append
//! - ms2: `update_partition_spec` add identity(b) → spec 1 becomes default    NO SNAPSHOT
//! - ms3: `fast_append` F2(a="y", b="z", rc=10) under spec 1                 seq 2, op append
//! - ms4: ONE multi-spec `fast_append`: F0(spec0, a="q", rc=10) AND F3(spec1, a="r", b="s", rc=10)
//!   seq 3, op append
//!
//! The ms4 snapshot has TWO manifests with DIFFERENT partition_spec_id values (0 and 1).
//!
//! # Tie-shaping — the spec-id tiebreaker is the ONLY disambiguator
//!
//! F0 and F3 have IDENTICAL record_count=10, so the two ms4 manifests tie on ALL nine prior
//! sort-tuple keys (content=data, seq=3, min_seq=3, added_files_count=1, existing_files_count=0,
//! deleted_files_count=0, added_rows=10, existing_rows=0, deleted_rows=0) and differ ONLY on
//! `partition_spec_id` (0 vs 1). The W3 tiebreaker at position 10 is the sole disambiguator.
//! This exercises the same-arity masking lesson: without the spec-id tiebreaker the two manifests
//! would tie completely and produce indeterminate ordering.
//!
//! # The two comparison directions
//!
//! **Direction 1 (GEN — Rust writes, Java judges).**
//! [`test_multi_spec_gen_rust_performs_the_chain`] performs the same four-step chain on a local-FS
//! `MemoryCatalog`, landing `final.metadata.json` at `<gen_dir>/rust_table/metadata/`. The run
//! script has Java emit its canonical view of that table and byte-diffs it against `java_meta.json`.
//!
//! **Direction 2 (READ parity — Java writes, Rust verifies).**
//! [`test_rust_view_of_java_multi_spec_chain_matches_java_view`] asserts Rust's canonical view of
//! the Java-written chain equals Java's own `java_meta.json`.
//!
//! **Direction 2b (WRITE parity — Rust writes, Rust verifies).**
//! [`test_rust_multi_spec_chain_matches_java_semantics`] asserts Rust's canonical view of the
//! Rust-written chain (from the GEN step) equals `java_meta.json`.
//!
//! # The env gate
//!
//! Tests are NO-OPS unless their env var is set non-empty (offline `cargo test` passes cleanly).
//! `ICEBERG_INTEROP_MULTI_SPEC_GEN_DIR` — write the Rust chain here (Direction 1).
//! `ICEBERG_INTEROP_MULTI_SPEC_DIR` — compare tests read Java's fixtures from here (Direction 2).

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, FormatVersion, Literal,
    NestedField, PrimitiveType, Schema, SortOrder, Struct, Transform, Type, UnboundPartitionSpec,
};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation};
use serde_json::Value as JsonValue;

mod common;
use common::snapshot_meta_view::snapshot_meta_view;

fn gen_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_MULTI_SPEC_GEN_DIR")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn compare_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_MULTI_SPEC_DIR")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

// ===========================================================================================
// Schema and partition spec builders — identical to the Java MultiSpecOracle.
// ===========================================================================================

/// Schema: `{1 a string required, 2 b string optional}`.
/// `b` is optional so a spec-0 partition tuple (only "a") round-trips without a null-fill error.
fn multi_spec_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "a", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(2, "b", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("build the {a, b} schema")
}

/// Spec 0: `identity(a)` only — the initial partition spec.
/// Field id 1000 (the first sequential id assigned by `BaseUpdatePartitionSpec`/`UpdatePartitionSpecAction`).
/// Partition field named "a" (Java `addField("a")` auto-names to the source column name for identity).
fn spec_zero() -> UnboundPartitionSpec {
    UnboundPartitionSpec::builder()
        .with_spec_id(0)
        .add_partition_field(1, "a".to_string(), Transform::Identity)
        .expect("add identity(a) partition field")
        .build()
}

/// A metadata-only `DataFile` under SPEC 0 (`identity(a)` only, partition tuple has one field).
/// The path need not exist — the fixture only reads manifests. Carries `partition_spec_id=0`.
fn fake_data_file_spec0(
    table_location: &str,
    name: &str,
    a_val: &str,
    record_count: u64,
) -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(format!("{table_location}/data/{name}"))
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(record_count * 100)
        .record_count(record_count)
        .partition_spec_id(0)
        // Spec-0 partition tuple: just the "a" identity value (one field).
        .partition(Struct::from_iter([Some(Literal::string(a_val))]))
        .build()
        .expect("build spec-0 metadata-only data file")
}

/// A metadata-only `DataFile` under SPEC 1 (`identity(a), identity(b)`, partition tuple has two
/// fields). Carries `partition_spec_id=1`.
fn fake_data_file_spec1(
    table_location: &str,
    name: &str,
    a_val: &str,
    b_val: &str,
    record_count: u64,
) -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(format!("{table_location}/data/{name}"))
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(record_count * 100)
        .record_count(record_count)
        .partition_spec_id(1)
        // Spec-1 partition tuple: "a" identity value then "b" identity value (two fields).
        .partition(Struct::from_iter([
            Some(Literal::string(a_val)),
            Some(Literal::string(b_val)),
        ]))
        .build()
        .expect("build spec-1 metadata-only data file")
}

// ===========================================================================================
// The Rust GEN path — Direction 1: Rust writes the chain, Java judges it.
// ===========================================================================================

/// Rust performs the SAME four-step multi-spec chain as Java's `MultiSpecOracle.generate`,
/// writing the table to `<gen_dir>/rust_table` through the PRODUCTION write paths, and lands
/// `final.metadata.json` for the Java emitter + the comparison tests.
///
/// The multi-spec commit (ms4) carries ONE file under spec 0 AND ONE file under spec 1 in a
/// single `fast_append`. `group_files_by_spec` routes them into two manifest writers producing
/// TWO manifests with `partition_spec_id=0` and `partition_spec_id=1` respectively — exercising
/// the W3 spec-id tiebreaker as the ONLY disambiguator (both files have the same record count).
#[tokio::test]
async fn test_multi_spec_gen_rust_performs_the_chain() {
    let Some(gen_dir) = gen_dir() else {
        println!(
            "skipping interop_multi_spec GEN — set ICEBERG_INTEROP_MULTI_SPEC_GEN_DIR \
             (run dev/java-interop/run-interop-multi-spec.sh)"
        );
        return;
    };

    let warehouse = gen_dir.to_string_lossy().to_string();
    let table_location = format!("{warehouse}/rust_table");
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "interop_multi_spec_gen",
            HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse.clone())]),
        )
        .await
        .expect("build MemoryCatalog over local FS");

    let namespace = NamespaceIdent::new("interop".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create namespace");

    // Create the table with spec 0 (identity(a) only) — mirrors Java's unpartitioned→updateSpec
    // bootstrap; the MemoryCatalog path goes directly to spec 0 via the creation spec.
    let creation = TableCreation::builder()
        .name("rust_table".to_string())
        .location(table_location.clone())
        .schema(multi_spec_schema())
        .partition_spec(spec_zero())
        .sort_order(SortOrder::unsorted_order())
        .format_version(FormatVersion::V2)
        .build();
    let table = catalog
        .create_table(&namespace, creation)
        .await
        .expect("create multi-spec rust_table");

    // ms1: fast_append F1 under spec 0 (seq 1).
    let file_f1 = fake_data_file_spec0(&table_location, "s0_f1.parquet", "x", 10);
    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(vec![file_f1])
        .apply(tx)
        .expect("apply ms1 fast_append (spec 0, F1)");
    let table = tx.commit(&catalog).await.expect("commit ms1 fast_append");

    // ms2: evolve the partition spec — add identity(b) to produce spec 1. NO snapshot.
    // `add_field("b")` uses the source column name "b" and generates the partition field name
    // automatically (Java `addField(String)` — identity transform, auto-named). The Rust
    // `add_field` method takes only the source column name and defaults to identity.
    let tx = Transaction::new(&table);
    let tx = tx
        .update_partition_spec()
        .add_field("b")
        .apply(tx)
        .expect("apply ms2 update_partition_spec (add identity(b))");
    let table = tx
        .commit(&catalog)
        .await
        .expect("commit ms2 update_partition_spec");

    // After the spec evolution the default spec is spec 1. Verify the spec ids are as expected
    // (this is a correctness pin — the fast_append must use spec-1-stamped files for spec 1 and
    // spec-0-stamped files for spec 0, not just the default).
    let default_spec_id = table.metadata().default_partition_spec_id();
    assert_eq!(
        default_spec_id, 1,
        "after adding identity(b) the default spec must be spec 1; got {default_spec_id}"
    );

    // ms3: fast_append F2 under spec 1 (seq 2).
    let file_f2 = fake_data_file_spec1(&table_location, "s1_f2.parquet", "y", "z", 10);
    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(vec![file_f2])
        .apply(tx)
        .expect("apply ms3 fast_append (spec 1, F2)");
    let table = tx.commit(&catalog).await.expect("commit ms3 fast_append");

    // ms4: THE MULTI-SPEC COMMIT — one fast_append carrying:
    //   F0: spec-0-stamped (partition_spec_id=0, a="q", record_count=10)
    //   F3: spec-1-stamped (partition_spec_id=1, a="r", b="s", record_count=10)
    //
    // TIE-SHAPING: both F0 and F3 have record_count=10 so the two ms4 manifests tie on ALL nine
    // prior sort-tuple keys and differ ONLY on partition_spec_id (0 vs 1) — the W3 tiebreaker at
    // position 10 is the ONLY disambiguator (same-arity masking lesson, 2026-06-12 X1).
    //
    // The Rust `group_files_by_spec` routes F0 into the spec-0 manifest writer and F3 into the
    // spec-1 manifest writer, producing TWO manifests with different partition_spec_id values
    // within ONE snapshot — identical to Java's newDataFilesBySpec grouping.
    let file_f0 = fake_data_file_spec0(&table_location, "s0_f0.parquet", "q", 10);
    let file_f3 = fake_data_file_spec1(&table_location, "s1_f3.parquet", "r", "s", 10);

    // Assert tie-shaping property: F0 and F3 have the same record_count so the manifests tie
    // on the prior 9 sort-tuple keys. The spec-id field (partition_spec_id=0 for F0,
    // partition_spec_id=1 for F3 — encoded in their respective partition tuple sizes) is the
    // only difference. Spec-0 partition has 1 field (identity(a)); spec-1 partition has 2 fields
    // (identity(a) + identity(b)). The spec-id tiebreaker (W3, position 10) is the ONLY
    // disambiguator.
    assert_eq!(
        file_f0.record_count(),
        file_f3.record_count(),
        "tie-shaping violated: F0 and F3 must have the same record_count so the two ms4 \
         manifests tie on all 9 prior sort keys (same-arity masking lesson, W3 ruling: \
         spec_id tiebreaker at position 10 is the ONLY disambiguator)"
    );
    // Verify the partition tuples differ in arity: spec-0 = 1 field, spec-1 = 2 fields.
    assert_ne!(
        file_f0.partition().fields().len(),
        file_f3.partition().fields().len(),
        "F0 (spec 0, 1-field partition) and F3 (spec 1, 2-field partition) must have \
         different partition tuple arities — they carry different spec ids"
    );

    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(vec![file_f0, file_f3])
        .apply(tx)
        .expect("apply ms4 multi-spec fast_append (spec 0 F0 + spec 1 F3)");
    let table = tx
        .commit(&catalog)
        .await
        .expect("commit ms4 multi-spec fast_append");

    // Assert the multi-spec commit produced TWO manifests in the ms4 snapshot.
    let ms4_snapshot = table
        .metadata()
        .current_snapshot()
        .expect("ms4 snapshot must be current after commit");
    let manifest_list = ms4_snapshot
        .load_manifest_list(table.file_io(), &table.metadata_ref())
        .await
        .expect("load ms4 manifest list");
    // The ms4 snapshot owns exactly two NEW manifests (one per spec group); prior manifests from
    // ms1 and ms3 are also carried, so the total manifest count is ≥ 3. The two NEW ones from
    // ms4 both have sequence_number == ms4_snapshot.sequence_number().
    let ms4_seq = ms4_snapshot.sequence_number();
    let ms4_manifests: Vec<_> = manifest_list
        .entries()
        .iter()
        .filter(|manifest| manifest.sequence_number == ms4_seq)
        .collect();
    assert_eq!(
        ms4_manifests.len(),
        2,
        "the ms4 multi-spec commit must produce exactly 2 NEW manifests (one per spec group); \
         got {} manifests at seq {ms4_seq}",
        ms4_manifests.len()
    );
    let spec_ids_in_ms4: Vec<i32> = {
        let mut ids: Vec<i32> = ms4_manifests.iter().map(|m| m.partition_spec_id).collect();
        ids.sort_unstable();
        ids
    };
    assert_eq!(
        spec_ids_in_ms4,
        vec![0, 1],
        "the two ms4 manifests must carry partition_spec_id 0 and 1 respectively; \
         got {:?}",
        spec_ids_in_ms4
    );

    // Land the final metadata at the known path for the Java emitter + the comparison tests.
    let final_metadata_path = format!("{table_location}/metadata/final.metadata.json");
    table
        .metadata()
        .clone()
        .write_to(table.file_io(), &final_metadata_path)
        .await
        .expect("write final.metadata.json");

    println!(
        "interop_multi_spec GEN OK — Rust performed the four-step multi-spec chain at \
         {table_location} (ms1:spec0 + ms2:evolve + ms3:spec1 + ms4:multi-spec commit). \
         The Java emitter + diff judge it next."
    );
}

// ===========================================================================================
// The comparison tests — Direction 2: Java acts, Rust verifies.
// ===========================================================================================

fn load_json(path: &std::path::Path) -> JsonValue {
    let raw =
        fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    serde_json::from_str(&raw).unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
}

/// READ parity: Rust's canonical view of the JAVA multi-spec chain equals Java's own view.
/// Load-bearing assertions riding inside the canonical view:
/// - ms4 has two manifests with DIFFERENT partition_spec_id values (0 and 1).
/// - The spec-id tiebreaker (position 10) resolves the same-arity tie — both files have rc=10.
/// - Partition tuples rendered under each manifest's OWN spec (the file's-own-spec rule):
///   spec-0 manifest entries render under identity(a) only; spec-1 entries render under
///   identity(a)+identity(b).
#[tokio::test]
async fn test_rust_view_of_java_multi_spec_chain_matches_java_view() {
    let Some(dir) = compare_dir() else {
        println!(
            "skipping interop_multi_spec compare (D2 read parity) — set ICEBERG_INTEROP_MULTI_SPEC_DIR \
             (run dev/java-interop/run-interop-multi-spec.sh)"
        );
        return;
    };

    let java_view = load_json(&dir.join("java_meta.json"));
    let rust_view = snapshot_meta_view(&dir.join("table/metadata/final.metadata.json")).await;
    assert_eq!(
        rust_view, java_view,
        "Rust's view of the JAVA multi-spec chain diverges from Java's own view"
    );
    println!("multi_spec: Rust view of Java chain == Java view OK");
}

/// WRITE parity — the Z2 crown jewel: the RUST-written four-step multi-spec chain produces
/// canonical metadata indistinguishable from Java's across spec evolution, a single multi-spec
/// commit, per-spec manifest grouping, and the spec-id tiebreaker. The tie-shaping property
/// (both ms4 files have the same record count so the two manifests are pure spec-id apart) was
/// asserted in the GEN step above; here we confirm the byte-level view matches Java.
#[tokio::test]
async fn test_rust_multi_spec_chain_matches_java_semantics() {
    let Some(dir) = compare_dir() else {
        println!(
            "skipping interop_multi_spec compare (D2 write parity) — set ICEBERG_INTEROP_MULTI_SPEC_DIR \
             (run dev/java-interop/run-interop-multi-spec.sh)"
        );
        return;
    };

    let rust_metadata = dir.join("rust_table/metadata/final.metadata.json");
    assert!(
        rust_metadata.exists(),
        "missing {} — run the GEN step of run-interop-multi-spec.sh first",
        rust_metadata.display()
    );
    let java_view = load_json(&dir.join("java_meta.json"));
    let rust_view = snapshot_meta_view(&rust_metadata).await;
    assert_eq!(
        rust_view, java_view,
        "the RUST multi-spec chain's canonical metadata diverges from Java's semantics"
    );
    println!("multi_spec: Rust chain metadata == Java semantics OK");
}
