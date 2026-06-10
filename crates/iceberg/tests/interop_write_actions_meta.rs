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

//! METADATA-LEVEL rewrite-family interop (sprint increment E2, extended for Increment 4) —
//! `DeleteFiles`, `OverwriteFiles`, `ReplacePartitions`, `RewriteFiles`, `RewriteManifests`, and
//! `MergeAppend` proven against Java in ONE chain, through the SAME canonical snapshot-metadata view
//! as the E1 row-delta interop (`common/snapshot_meta_view.rs` ↔ the Java `SnapshotMetaOracle`); a
//! SIBLING fixture (fixture B) additionally pins the seq-preserving `rewrite_files` over a
//! delete-bearing table.
//!
//! THE CHAIN (identical logical constants on both sides; a V2 table partitioned by
//! identity(category); NO parquet — the actions only read and rewrite MANIFESTS):
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
//! - s6 `rewrite_manifests`  cluster_by(partition)                  seq 5 (no data change), op `replace`
//!   (re-group live DATA entries one manifest per partition; every entry carried Existing with its
//!   original provenance; live set {C,D,F} unchanged)
//! - s7 `update_table_properties` set min-count-to-merge=2          NO SNAPSHOT (a property commit)
//!   (arms the s8 merge; the snapshot-level view is unaffected on both sides)
//! - s8 `merge_append`       add G(cat=a,60)                        seq 6, op `append` (MERGING —
//!   one bin ⇒ the merge fires into ONE merged manifest with carried Existing entries + G as Added)
//!
//! FIXTURE B (the delete-bearing seq-preserving rewrite — `RewriteSeqOracle`):
//! - b1 `fast_append`        A(cat=a,10) B(cat=b,20)               seq 1, op `append`
//! - b2 `row_delta`          add a position-delete referencing B   seq 2, op `delete`
//! - b3 `rewrite_files`      {A}→{A'} with `data_sequence_number(1)`, validate-from b2  seq 3,
//!   op `replace`. Pins: A' carries data_seq 1 (not the rewrite snap's seq 3); the delete manifest
//!   survives the rewrite intact. The delete references B (the SURVIVOR) so Java's dangling-delete
//!   machinery stays dormant on both sides. Rust builds the rewrite tx AFTER the row-delta commit, so
//!   its tx-captured start IS b2 — the semantic twin of Java's explicit `validateFromSnapshot`.
//!
//! Comparisons per the E1 pattern (driven by `dev/java-interop/run-interop-write-actions.sh`):
//! Rust's view of the JAVA chain == `java_meta.json`; Rust's view of the RUST chain ==
//! `java_meta.json`; and script-side, Java's view of the Rust chain byte-diffed against its own.
//!
//! GATED on `ICEBERG_INTEROP_WRITE_ACTIONS_DIR` (chain comparisons),
//! `ICEBERG_INTEROP_WRITE_ACTIONS_GEN_DIR` (the Rust chain writer),
//! `ICEBERG_INTEROP_REWRITE_SEQ_DIR` (fixture-B comparisons), and
//! `ICEBERG_INTEROP_REWRITE_SEQ_GEN_DIR` (the fixture-B Rust writer). Unset ⇒ clean no-ops.

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

fn rewrite_seq_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_REWRITE_SEQ_DIR")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn rewrite_seq_gen_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_REWRITE_SEQ_GEN_DIR")
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

/// A metadata-only POSITION-delete `DataFile` (content `PositionDeletes`), routed to `category`. The
/// path need not exist — fixture B's rewrite + the canonical view only read manifests. Mirrors the
/// Java `FileMetadata.deleteFileBuilder(spec).ofPositionDeletes()` metadata-only delete file.
fn fake_position_delete_file(table_location: &str, name: &str, category: &str) -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::PositionDeletes)
        .file_path(format!("{table_location}/data/{name}"))
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(100)
        .record_count(1)
        .partition_spec_id(0)
        .partition(Struct::from_iter([Some(Literal::string(category))]))
        .build()
        .expect("build metadata-only position-delete file")
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
    let file_g = fake_data_file(&table_location, "a5-merged.parquet", "a", 60);
    let path_a = file_a.file_path().to_string();
    let path_b = file_b.file_path().to_string();

    // s1 append → s2 delete → s3 overwrite → s4 replace-partitions → s5 rewrite (seq 1..5),
    // then s6 rewrite_manifests (cluster by partition) → s7 property set (NO snapshot) → s8
    // merge_append (seq 6).
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

    // s6 rewrite_manifests clustered BY PARTITION: re-group the live DATA entries one manifest per
    // partition. The cluster key STRING never appears in metadata — only the GROUPING is compared, so
    // `format!("{:?}", data_file.partition())` (Rust) and `String.valueOf(file.partition())` (Java)
    // are equivalent: both produce one group per distinct partition tuple (one per category). Live
    // data after s5 is {C(cat=a,30), D(cat=b,40), F(cat=a,50)} ⇒ two manifests, every entry carried
    // EXISTING with its original provenance.
    let tx = Transaction::new(&table);
    let tx = tx
        .rewrite_manifests()
        .cluster_by(|data_file| format!("{:?}", data_file.partition()))
        .apply(tx)
        .expect("apply rewrite_manifests");
    let table = tx
        .commit(&catalog)
        .await
        .expect("commit s6 rewrite_manifests");

    // s7 set commit.manifest.min-count-to-merge=2 as a TABLE PROPERTY (no snapshot is produced — the
    // canonical snapshot-level view is unaffected on both sides). This arms the MERGING append in s8.
    let tx = Transaction::new(&table);
    let tx = tx
        .update_table_properties()
        .set(
            "commit.manifest.min-count-to-merge".to_string(),
            "2".to_string(),
        )
        .apply(tx)
        .expect("apply update_table_properties");
    let table = tx.commit(&catalog).await.expect("commit s7 property set");

    // s8 merge_append (Java newAppend — the MERGING producer): add G(cat=a,60). With min-count=2 and
    // KB-size manifests vs the 8 MB target every manifest lands in ONE bin ⇒ the merge fires into ONE
    // merged manifest carrying the Existing entries (original provenance) + G as Added.
    let tx = Transaction::new(&table);
    let tx = tx
        .merge_append()
        .add_data_files(vec![file_g])
        .apply(tx)
        .expect("apply merge_append");
    let table = tx.commit(&catalog).await.expect("commit s8 merge_append");

    // Land the final metadata at the known path for the Java emitter + the comparison tests.
    let final_metadata_path = format!("{table_location}/metadata/final.metadata.json");
    table
        .metadata()
        .clone()
        .write_to(table.file_io(), &final_metadata_path)
        .await
        .expect("write final.metadata.json");

    println!(
        "interop_write_actions GEN OK — Rust performed the eight-step \
         append/delete/overwrite/replace-partitions/rewrite/rewrite-manifests/property-set/merge-append \
         chain at {table_location}. The Java emitter + diff judge it next."
    );
}

// ===========================================================================================
// Fixture B — the delete-bearing seq-preserving rewrite (the GEN path; mirrors RewriteSeqOracle).
// ===========================================================================================

/// Rust performs the SAME three-commit delete-bearing chain as Java's `RewriteSeqOracle.generate`,
/// writing the table to `<gen_dir>/rust_table` through the PRODUCTION write paths, and lands
/// `final.metadata.json` for the Java emitter + the comparison tests.
///
/// b1 `fast_append` A,B → b2 `row_delta` (position-delete on B) → b3 `rewrite_files` {A}→{A'} with
/// `data_sequence_number(1)`. The rewrite transaction is built AFTER the row-delta commit, so its
/// transaction-captured starting snapshot IS the row-delta snapshot and the concurrent window is
/// empty — the semantic twin of Java's explicit `validateFromSnapshot(rowDeltaSnapshotId)`.
#[tokio::test]
async fn test_rewrite_seq_gen_rust_performs_the_delete_bearing_rewrite() {
    let Some(gen_dir) = rewrite_seq_gen_dir() else {
        println!(
            "skipping interop_rewrite_seq GEN — set ICEBERG_INTEROP_REWRITE_SEQ_GEN_DIR \
             (run dev/java-interop/run-interop-write-actions.sh)"
        );
        return;
    };

    let warehouse = gen_dir.to_string_lossy().to_string();
    let table_location = format!("{warehouse}/rust_table");
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "interop_rewrite_seq_gen",
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
    let file_a_prime = fake_data_file(&table_location, "a1-compacted.parquet", "a", 10);
    let delete_b = fake_position_delete_file(&table_location, "b1-deletes.parquet", "b");

    // b1 fast_append A,B (seq 1).
    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(vec![file_a.clone(), file_b])
        .apply(tx)
        .expect("apply fast append");
    let table = tx.commit(&catalog).await.expect("commit b1 append");

    // b2 row_delta: add the position-delete referencing B (seq 2).
    let tx = Transaction::new(&table);
    let tx = tx
        .row_delta()
        .add_deletes(vec![delete_b])
        .apply(tx)
        .expect("apply row_delta");
    let table = tx.commit(&catalog).await.expect("commit b2 row_delta");

    // b3 rewrite_files {A}→{A'} preserving data sequence number 1. The transaction is created HERE,
    // AFTER the row-delta commit, so its tx-captured start IS b2 — the semantic twin of Java's
    // explicit validateFromSnapshot(rowDeltaSnapshotId); the concurrent window is empty.
    let tx = Transaction::new(&table);
    let tx = tx
        .rewrite_files(vec![file_a], vec![file_a_prime])
        .data_sequence_number(1)
        .apply(tx)
        .expect("apply rewrite_files");
    let table = tx.commit(&catalog).await.expect("commit b3 rewrite");

    let final_metadata_path = format!("{table_location}/metadata/final.metadata.json");
    table
        .metadata()
        .clone()
        .write_to(table.file_io(), &final_metadata_path)
        .await
        .expect("write final.metadata.json");

    println!(
        "interop_rewrite_seq GEN OK — Rust performed the delete-bearing seq-preserving rewrite at \
         {table_location} (A'@seq1, delete-on-B intact). The Java emitter + diff judge it next."
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

/// READ parity: Rust's canonical view of the JAVA eight-step chain equals Java's own view —
/// operation classification, summary counts (incl. the replace-partitions marker), the rewritten +
/// re-clustered + merged manifest shapes, and the carried-entry provenance, as Rust reads them.
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

/// WRITE parity — the E2 crown jewel: the RUST-written eight-step chain produces canonical
/// metadata indistinguishable from Java's for the same logical operations, across ALL SIX
/// write actions in one chain (delete/overwrite/replace-partitions/rewrite/rewrite-manifests/
/// merge-append + the property-set non-snapshot): operation per commit, summary counts, manifest
/// rewrite/re-cluster/merge/carry-forward shapes, tombstone + Existing-entry provenance, seqs.
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

// ===========================================================================================
// Fixture B comparison tests — the delete-bearing seq-preserving rewrite (the E1 pattern).
// ===========================================================================================

/// READ parity (fixture B): Rust's canonical view of the JAVA delete-bearing rewrite chain equals
/// Java's own view. The two load-bearing assertions ride inside the canonical view: the rewrite
/// snapshot's data entry for A' carries the POST-INHERITANCE data sequence number 1 (not the
/// rewrite snapshot's seq 3), and the position-delete manifest on B survives the rewrite intact.
#[tokio::test]
async fn test_rust_view_of_java_rewrite_seq_chain_matches_java_view() {
    let Some(dir) = rewrite_seq_dir() else {
        println!(
            "skipping interop_rewrite_seq — set ICEBERG_INTEROP_REWRITE_SEQ_DIR \
             (run dev/java-interop/run-interop-write-actions.sh)"
        );
        return;
    };

    let java_view = load_json(&dir.join("java_meta.json"));
    let rust_view = snapshot_meta_view(&dir.join("table/metadata/final.metadata.json")).await;
    assert_eq!(
        rust_view, java_view,
        "Rust's view of the JAVA delete-bearing rewrite chain diverges from Java's own view"
    );
    println!("rewrite_seq: Rust view of Java chain == Java view OK");
}

/// WRITE parity (fixture B): the RUST-written delete-bearing rewrite chain produces canonical
/// metadata indistinguishable from Java's — proving the seq-preservation (`data_sequence_number`)
/// AND the delete-manifest carry survive a rewrite over a merge-on-read table identically to Java's
/// `rewriteFiles(.., 1L)` with `validateFromSnapshot`. (The Rust tx-captured start is the semantic
/// twin of the explicit validateFromSnapshot — both leave an empty concurrent window.)
#[tokio::test]
async fn test_rust_rewrite_seq_chain_matches_java_semantics() {
    let Some(dir) = rewrite_seq_dir() else {
        println!(
            "skipping interop_rewrite_seq — set ICEBERG_INTEROP_REWRITE_SEQ_DIR \
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
        "the RUST delete-bearing rewrite chain's canonical metadata diverges from Java's semantics"
    );
    println!("rewrite_seq: Rust chain metadata == Java semantics OK");
}
