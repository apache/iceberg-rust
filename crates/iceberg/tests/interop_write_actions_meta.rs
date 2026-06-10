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

//! METADATA-LEVEL rewrite-family interop (sprint increment E2) — `DeleteFiles`, `OverwriteFiles`,
//! `ReplacePartitions`, and `RewriteFiles` proven against Java in ONE five-commit chain, through
//! the SAME canonical snapshot-metadata view as the E1 row-delta interop
//! (`common/snapshot_meta_view.rs` ↔ the Java `SnapshotMetaOracle`).
//!
//! THE CHAIN (identical logical constants on both sides; a V2 table partitioned by
//! identity(category); NO parquet — the four actions only read and rewrite MANIFESTS):
//!
//! - s1 `fast_append`        A(cat=a,10) B(cat=b,20) C(cat=a,30)   seq 1, op `append`
//! - s2 `delete_files`       A by path                              seq 2, op `delete`
//!   (the manifest-filter rewrite: A tombstoned with the NEW snapshot id + ORIGINAL seq, B/C
//!   carried as Existing with their ORIGINAL provenance — the silent-corruption axis)
//! - s3 `overwrite_files`    add D(cat=b,40), delete B by path      seq 3, op `overwrite`
//! - s4 `replace_partitions` add E(cat=a,50)                        seq 4, op `overwrite` +
//!   `replace-partitions=true` (drops the live cat=a file C)
//! - s5 `rewrite_files`      delete {E}, add {F(cat=a,50)}          seq 5, op `replace`
//!   (the compaction commit; record count conserved)
//!
//! Comparisons per the E1 pattern (driven by `dev/java-interop/run-interop-write-actions.sh`):
//! Rust's view of the JAVA chain == `java_meta.json`; Rust's view of the RUST chain ==
//! `java_meta.json`; and script-side, Java's view of the Rust chain byte-diffed against its own.
//!
//! GATED on `ICEBERG_INTEROP_WRITE_ACTIONS_DIR` (comparisons) and
//! `ICEBERG_INTEROP_WRITE_ACTIONS_GEN_DIR` (the Rust chain writer). Unset ⇒ clean no-ops.

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

fn actions_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_WRITE_ACTIONS_DIR")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn actions_gen_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_WRITE_ACTIONS_GEN_DIR")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

// ===========================================================================================
// The Rust five-commit chain — the GEN path (mirrors the Java WriteActionsOracle.generate).
// ===========================================================================================

/// The Java fixture's schema: {1 id long required, 2 category string required}.
fn write_actions_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "category", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("build the {id, category} schema")
}

/// identity(category), spec id 0 — the Java fixture's spec.
fn write_actions_spec() -> UnboundPartitionSpec {
    UnboundPartitionSpec::builder()
        .with_spec_id(0)
        .add_partition_field(2, "category".to_string(), Transform::Identity)
        .expect("add identity(category) partition field")
        .build()
}

/// A metadata-only `DataFile` (the path need not exist — the four actions only read manifests).
/// Record count + partition are the cross-language-comparable constants; file size is excluded
/// from the canonical view (kept proportional for realism, like the Java side).
fn fake_data_file(table_location: &str, name: &str, category: &str, record_count: u64) -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(format!("{table_location}/data/{name}"))
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(record_count * 100)
        .record_count(record_count)
        .partition_spec_id(0)
        .partition(Struct::from_iter([Some(Literal::string(category))]))
        .build()
        .expect("build metadata-only data file")
}

/// Rust performs the SAME five-commit chain as Java's `WriteActionsOracle.generate`, writing the
/// table to `<gen_dir>/rust_table` through the PRODUCTION write paths of all four rewrite-family
/// actions, and lands `final.metadata.json` for the Java emitter + the comparison tests.
#[tokio::test]
async fn test_write_actions_gen_rust_performs_the_four_action_chain() {
    let Some(gen_dir) = actions_gen_dir() else {
        println!(
            "skipping interop_write_actions GEN — set ICEBERG_INTEROP_WRITE_ACTIONS_GEN_DIR \
             (run dev/java-interop/run-interop-write-actions.sh)"
        );
        return;
    };

    let warehouse = gen_dir.to_string_lossy().to_string();
    let table_location = format!("{warehouse}/rust_table");
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "interop_write_actions_gen",
            HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse.clone())]),
        )
        .await
        .expect("build MemoryCatalog over local FS");

    let namespace = NamespaceIdent::new("interop".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create namespace");
    let creation = TableCreation::builder()
        .name("rust_table".to_string())
        .location(table_location.clone())
        .schema(write_actions_schema())
        .partition_spec(write_actions_spec())
        .sort_order(SortOrder::unsorted_order())
        .format_version(FormatVersion::V2)
        .build();
    let table = catalog
        .create_table(&namespace, creation)
        .await
        .expect("create partitioned rust_table");

    let file_a = fake_data_file(&table_location, "a1.parquet", "a", 10);
    let file_b = fake_data_file(&table_location, "b1.parquet", "b", 20);
    let file_c = fake_data_file(&table_location, "a2.parquet", "a", 30);
    let file_d = fake_data_file(&table_location, "b2.parquet", "b", 40);
    let file_e = fake_data_file(&table_location, "a3.parquet", "a", 50);
    let file_f = fake_data_file(&table_location, "a4-compacted.parquet", "a", 50);
    let path_a = file_a.file_path().to_string();
    let path_b = file_b.file_path().to_string();

    // s1 append → s2 delete → s3 overwrite → s4 replace-partitions → s5 rewrite (seq 1..5).
    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(vec![file_a, file_b, file_c])
        .apply(tx)
        .expect("apply fast append");
    let table = tx.commit(&catalog).await.expect("commit s1 append");

    let tx = Transaction::new(&table);
    let tx = tx
        .delete_files()
        .delete_file(path_a)
        .apply(tx)
        .expect("apply delete_files");
    let table = tx.commit(&catalog).await.expect("commit s2 delete");

    let tx = Transaction::new(&table);
    let tx = tx
        .overwrite_files()
        .add_file(file_d)
        .delete_file(path_b)
        .apply(tx)
        .expect("apply overwrite_files");
    let table = tx.commit(&catalog).await.expect("commit s3 overwrite");

    let tx = Transaction::new(&table);
    let tx = tx
        .replace_partitions()
        .add_file(file_e.clone())
        .apply(tx)
        .expect("apply replace_partitions");
    let table = tx
        .commit(&catalog)
        .await
        .expect("commit s4 replace-partitions");

    let tx = Transaction::new(&table);
    let tx = tx
        .rewrite_files(vec![file_e], vec![file_f])
        .apply(tx)
        .expect("apply rewrite_files");
    let table = tx.commit(&catalog).await.expect("commit s5 rewrite");

    // Land the final metadata at the known path for the Java emitter + the comparison tests.
    let final_metadata_path = format!("{table_location}/metadata/final.metadata.json");
    table
        .metadata()
        .clone()
        .write_to(table.file_io(), &final_metadata_path)
        .await
        .expect("write final.metadata.json");

    println!(
        "interop_write_actions GEN OK — Rust performed the five-commit \
         append/delete/overwrite/replace-partitions/rewrite chain at {table_location}. \
         The Java emitter + diff judge it next."
    );
}

// ===========================================================================================
// The comparison tests (the E1 pattern over the write-actions fixture).
// ===========================================================================================

fn load_json(path: &std::path::Path) -> JsonValue {
    let raw =
        fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    serde_json::from_str(&raw).unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
}

/// READ parity: Rust's canonical view of the JAVA five-commit chain equals Java's own view —
/// operation classification, summary counts (incl. the replace-partitions marker), the rewritten
/// manifest shapes, and the carried-entry provenance, as Rust reads them.
#[tokio::test]
async fn test_rust_view_of_java_write_actions_chain_matches_java_view() {
    let Some(dir) = actions_dir() else {
        println!(
            "skipping interop_write_actions — set ICEBERG_INTEROP_WRITE_ACTIONS_DIR \
             (run dev/java-interop/run-interop-write-actions.sh)"
        );
        return;
    };

    let java_view = load_json(&dir.join("java_meta.json"));
    let rust_view = snapshot_meta_view(&dir.join("table/metadata/final.metadata.json")).await;
    assert_eq!(
        rust_view, java_view,
        "Rust's view of the JAVA write-actions chain diverges from Java's own view"
    );
    println!("write_actions: Rust view of Java chain == Java view OK");
}

/// WRITE parity — the E2 crown jewel: the RUST-written five-commit chain produces canonical
/// metadata indistinguishable from Java's for the same logical operations, across ALL FOUR
/// rewrite-family actions in one chain (operation per commit, summary counts, manifest
/// rewrite/carry-forward shapes, tombstone + Existing-entry provenance, sequence numbers).
#[tokio::test]
async fn test_rust_write_actions_chain_matches_java_semantics() {
    let Some(dir) = actions_dir() else {
        println!(
            "skipping interop_write_actions — set ICEBERG_INTEROP_WRITE_ACTIONS_DIR \
             (run dev/java-interop/run-interop-write-actions.sh)"
        );
        return;
    };

    let rust_metadata = dir.join("rust_table/metadata/final.metadata.json");
    assert!(
        rust_metadata.exists(),
        "missing {} — run the GEN step of the run script first",
        rust_metadata.display()
    );
    let java_view = load_json(&dir.join("java_meta.json"));
    let rust_view = snapshot_meta_view(&rust_metadata).await;
    assert_eq!(
        rust_view, java_view,
        "the RUST write-actions chain's canonical metadata diverges from Java's semantics"
    );
    println!("write_actions: Rust chain metadata == Java semantics OK");
}
