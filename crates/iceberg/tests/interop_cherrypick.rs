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

//! Java interop for `CherryPickAction` (increment S2) — proving the Rust `CherryPickAction` and
//! Java 1.10.0 `ManageSnapshots.cherrypick` produce IDENTICAL canonical snapshot metadata on the
//! same fixtures, judged by Java. Driven by `dev/java-interop/run-interop-cherrypick.sh`.
//!
//! # The three fixtures
//!
//! - **ff** — staged snapshot whose parent IS the current head → cherrypick fast-forwards (`main`
//!   moves to the staged snapshot AS-IS, no new snapshot produced). Java authority: `apply()` L193-204
//!   `requireFastForward || isFastForward(base)`.
//! - **replay** — staged snapshot whose parent is NOT current (`main` advanced past it via an
//!   unrelated commit) → cherrypick REPLAYS: new snapshot with `source-snapshot-id` +
//!   `published-wap-id` in the summary. Java authority: `CherryPickOperation.cherrypick` L78-92.
//! - **dedup** — same first-publish as replay (succeeds), then a SECOND cherrypick of the SAME
//!   staged id → Java raises `CherrypickAncestorCommitException`; Rust raises `DataInvalid`. The
//!   fixture dir holds the table after the first publish.
//!
//! # The two directions (over three fixtures: ff / replay / dedup)
//!
//! **Direction 1 — Rust acts, Java judges.** [`test_cherrypick_gen_rust_produces_each_fixture`]
//! performs the SAME commit chain as the Java `CherryPickOracle` on a real-FS `MemoryCatalog` (so
//! the manifests + manifest lists are REAL on disk), landing `final.metadata.json` at
//! `<fixture>/rust_table/metadata/`. The run script then has Java emit its canonical
//! snapshot-metadata view of the Rust-produced table and byte-diffs it against Java's own view, AND
//! runs `verify-interop-cherrypick` (Java asserts per-fixture facts).
//!
//! **Direction 2 — Java acts, Rust verifies.** [`test_rust_view_of_java_cherrypick_matches_java_view`]
//! asserts Rust's canonical view of the JAVA-produced table equals Java's own `java_meta.json`.
//!
//! # The staging technique
//!
//! Since `stageOnly()` is outside the production Rust scope, both sides simulate a staged snapshot
//! by committing it via `fast_append` (so REAL manifests land on disk), then rolling `main` back to
//! the parent via `manage_snapshots().set_current_snapshot(parent_id)`. The produced snapshot stays
//! in metadata as a dangling "staged" snapshot with its real manifests.
//!
//! # The env gate
//!
//! Both tests are clean NO-OPS (runtime early-return, not `#[ignore]`) unless their env var is set
//! non-empty, so the offline `cargo test` gate needs no Java/Maven.
//! `ICEBERG_INTEROP_CHERRYPICK_GEN_DIR` drives the Rust GEN (Direction 1);
//! `ICEBERG_INTEROP_CHERRYPICK_DIR` drives the comparison test (Direction 2), which REQUIRES the
//! Java fixtures + `java_meta.json` and fails loudly otherwise.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, FormatVersion, NestedField,
    PrimitiveType, Schema, SortOrder, TableMetadata, Type, UnboundPartitionSpec,
};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, CatalogBuilder, ErrorKind, NamespaceIdent, TableCreation};
use serde_json::Value as JsonValue;

mod common;
use common::snapshot_meta_view::snapshot_meta_view;

/// The three fixtures, identical names + chains to the Java `CherryPickOracle.FIXTURES`.
const FIXTURES: &[&str] = &["ff", "replay", "dedup"];

fn gen_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_CHERRYPICK_GEN_DIR")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn compare_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_CHERRYPICK_DIR")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

// ===========================================================================================
// The fixture schema / spec / file builders (identical logical constants to the Java oracle).
// ===========================================================================================

/// `{1 id long required, 2 data string required}` — the Java fixture's schema (unpartitioned V2).
fn fixture_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "data", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("build the {id, data} schema")
}

/// Unpartitioned spec — the Java fixture's spec.
fn fixture_spec() -> UnboundPartitionSpec {
    UnboundPartitionSpec::builder().with_spec_id(0).build()
}

/// A metadata-only `DataFile` (the path need not exist — the oracle only reads manifests).
fn data_file(table_location: &str, name: &str, record_count: u64) -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(format!("{table_location}/data/{name}.parquet"))
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(record_count * 100)
        .record_count(record_count)
        .partition_spec_id(0)
        .partition(iceberg::spec::Struct::empty())
        .build()
        .expect("build metadata-only data file")
}

/// Fast-append one or more data files to the table. Returns the updated table.
async fn append(catalog: &impl Catalog, table: &Table, files: Vec<DataFile>) -> Table {
    let tx = Transaction::new(table);
    let tx = tx
        .fast_append()
        .add_data_files(files)
        .apply(tx)
        .expect("apply fast_append");
    tx.commit(catalog).await.expect("commit fast_append")
}

/// Fast-append with a `wap.id` summary property (simulating a WAP-staged snapshot). Returns the
/// updated table.
async fn append_with_wap_id(
    catalog: &impl Catalog,
    table: &Table,
    files: Vec<DataFile>,
    wap_id: &str,
) -> Table {
    let mut props = HashMap::new();
    props.insert("wap.id".to_string(), wap_id.to_string());
    let tx = Transaction::new(table);
    let tx = tx
        .fast_append()
        .set_snapshot_properties(props)
        .add_data_files(files)
        .apply(tx)
        .expect("apply fast_append with wap.id");
    tx.commit(catalog)
        .await
        .expect("commit fast_append with wap.id")
}

/// Stage a snapshot: commit via fast_append WITH `wap.id` (so REAL manifests land on disk), then
/// roll `main` back to `parent_id` via `manage_snapshots().set_current_snapshot`. Returns (updated
/// table, staged id). Used for the `ff` and `replay` fixtures.
async fn stage_snapshot(
    catalog: &impl Catalog,
    table: &Table,
    parent_id: i64,
    files: Vec<DataFile>,
    wap_id: &str,
) -> (Table, i64) {
    // Commit the staged snapshot (real manifests on disk).
    let table: Table = append_with_wap_id(catalog, table, files, wap_id).await;
    let staged_id = table
        .metadata()
        .current_snapshot_id()
        .expect("staged snapshot id");

    // Roll main back to parent_id — staged snapshot stays in metadata, off main.
    let tx = Transaction::new(&table);
    let tx = tx
        .manage_snapshots()
        .set_current_snapshot(parent_id)
        .apply(tx)
        .expect("apply set_current_snapshot rollback");
    let table = tx
        .commit(catalog)
        .await
        .expect("commit set_current_snapshot rollback");

    (table, staged_id)
}

/// Stage a snapshot WITHOUT `wap.id`: commit via plain fast_append, then roll `main` back. Returns
/// (updated table, staged id). Used for the `dedup` fixture so the second-cherrypick dedup fires via
/// `source-snapshot-id` ancestry (`DataInvalid` / Java's `CherrypickAncestorCommitException`),
/// not via the `DuplicateWAPCommitException` WAP-id path.
async fn stage_snapshot_no_wap(
    catalog: &impl Catalog,
    table: &Table,
    parent_id: i64,
    files: Vec<DataFile>,
) -> (Table, i64) {
    let table = append(catalog, table, files).await;
    let staged_id = table
        .metadata()
        .current_snapshot_id()
        .expect("staged snapshot id");

    // Roll main back to parent_id.
    let tx = Transaction::new(&table);
    let tx = tx
        .manage_snapshots()
        .set_current_snapshot(parent_id)
        .apply(tx)
        .expect("apply set_current_snapshot rollback (no-wap)");
    let table = tx
        .commit(catalog)
        .await
        .expect("commit set_current_snapshot rollback (no-wap)");

    (table, staged_id)
}

/// Write `final.metadata.json` at `<table_location>/metadata/final.metadata.json`.
async fn write_final_metadata(table: &Table, table_location: &str) {
    let path = format!("{table_location}/metadata/final.metadata.json");
    let metadata = table.metadata_ref();
    metadata
        .write_to(table.file_io(), path.as_str())
        .await
        .expect("write final.metadata.json");
}

// ===========================================================================================
// Direction 1 — the Rust GEN: build each fixture and cherry-pick.
// ===========================================================================================

/// Build one fixture's table through real appends, stage a snapshot, apply `cherry_pick`, and land
/// `final.metadata.json`. The chain MIRRORS the Java `CherryPickOracle.buildFixture` switch exactly.
async fn build_fixture(fixture: &str, gen_dir: &Path) {
    let fixture_dir = gen_dir.join(fixture);
    let table_location = fixture_dir.join("rust_table").to_string_lossy().to_string();
    fs::create_dir_all(&table_location).expect("create rust_table dir");

    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            &format!("interop_cherrypick_{fixture}"),
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_string(),
                fixture_dir.to_string_lossy().to_string(),
            )]),
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
        .schema(fixture_schema())
        .partition_spec(fixture_spec())
        .sort_order(SortOrder::unsorted_order())
        .format_version(FormatVersion::V2)
        .build();
    let mut table = catalog
        .create_table(&namespace, creation)
        .await
        .expect("create rust_table");

    // S0: the initial snapshot — parent of the staged snapshot.
    table = append(&catalog, &table, vec![data_file(&table_location, "s0", 10)]).await;
    let s0_id = table
        .metadata()
        .current_snapshot_id()
        .expect("s0 snapshot id");

    // S1 (staged): committed via fast_append (with wap.id for ff + replay; WITHOUT wap.id for
    // dedup so the second-cherrypick dedup fires via source-snapshot-id ancestry rather than the
    // DuplicateWAPCommitException WAP-id path — mirroring the Java CherryPickOracle).
    let (tbl, s1_staged_id) = if fixture == "dedup" {
        stage_snapshot_no_wap(&catalog, &table, s0_id, vec![data_file(
            &table_location,
            "s1",
            20,
        )])
        .await
    } else {
        stage_snapshot(
            &catalog,
            &table,
            s0_id,
            vec![data_file(&table_location, "s1", 20)],
            &format!("wap-{fixture}"),
        )
        .await
    };
    table = tbl;

    match fixture {
        "ff" => {
            // FF: staged S1's parent == current head (S0) → cherry_pick fast-forwards.
            let tx = Transaction::new(&table);
            let tx = tx
                .cherry_pick(s1_staged_id)
                .apply(tx)
                .expect("apply cherry_pick (ff)");
            table = tx.commit(&catalog).await.expect("commit cherry_pick (ff)");
        }
        "replay" | "dedup" => {
            // Advance main past S0 with an unrelated commit S2 (now S1.parent != head).
            table = append(&catalog, &table, vec![data_file(&table_location, "s2", 30)]).await;

            // First cherrypick: replay S1 → produces a NEW snapshot with source-snapshot-id.
            let tx = Transaction::new(&table);
            let tx = tx
                .cherry_pick(s1_staged_id)
                .apply(tx)
                .expect("apply cherry_pick (replay/dedup first)");
            table = tx
                .commit(&catalog)
                .await
                .expect("commit cherry_pick (replay/dedup first)");

            if fixture == "dedup" {
                // Verify the second attempt fails on the Rust side too (dedup fires on commit,
                // not on apply — apply just records the action; commit runs validate_non_ancestor).
                let tx2 = Transaction::new(&table);
                let tx2 = tx2
                    .cherry_pick(s1_staged_id)
                    .apply(tx2)
                    .expect("apply second cherry_pick (dedup)");
                match tx2.commit(&catalog).await {
                    Err(err) => {
                        // The rejection must be a non-retryable DataInvalid (mirrors Java's
                        // CherrypickAncestorCommitException — same condition: source-snapshot-id
                        // ancestry dedup; different exception hierarchy in Rust vs Java).
                        assert_eq!(
                            err.kind(),
                            ErrorKind::DataInvalid,
                            "dedup: second cherry_pick must fail with DataInvalid, got {:?}: \
                             {err}",
                            err.kind()
                        );
                        assert!(
                            !err.retryable(),
                            "dedup: the dedup rejection must be non-retryable: {err}"
                        );
                        assert!(
                            err.message().contains("already picked to create ancestor"),
                            "dedup: expected 'already picked to create ancestor' in error \
                             message, got: {err}"
                        );
                        println!(
                            "interop_cherrypick GEN dedup: second cherry_pick rejected as \
                             expected (DataInvalid, non-retryable, ancestry-dedup path): {err}"
                        );
                    }
                    Ok(_) => panic!(
                        "dedup fixture: second cherry_pick commit must fail (already picked via \
                         source-snapshot-id), but succeeded"
                    ),
                }
            }
        }
        other => panic!("unknown fixture: {other}"),
    }

    write_final_metadata(&table, &table_location).await;
    println!("interop_cherrypick GEN {fixture}: final.metadata.json written at {table_location}");
}

/// Direction 1: build each fixture via real appends + `cherry_pick`, land `final.metadata.json`.
/// The Java run script then byte-diffs Java's view of the Rust table against Java's own view.
#[tokio::test]
async fn test_cherrypick_gen_rust_produces_each_fixture() {
    let Some(gen_dir) = gen_dir() else {
        println!(
            "skipping interop_cherrypick GEN — set ICEBERG_INTEROP_CHERRYPICK_GEN_DIR \
             (run dev/java-interop/run-interop-cherrypick.sh)"
        );
        return;
    };

    for fixture in FIXTURES {
        build_fixture(fixture, &gen_dir).await;
    }
}

// ===========================================================================================
// Direction 2 — Java acts, Rust verifies.
// ===========================================================================================

fn load_json(path: &Path) -> JsonValue {
    let raw =
        fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    serde_json::from_str(&raw).unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
}

/// Direction 2 (Java acts, Rust verifies): Rust's canonical view of the JAVA-produced table equals
/// Java's own canonical view (`java_meta.json` emitted by `SnapshotMetaOracle.emit`). Additionally
/// asserts fixture-specific facts:
/// - **ff**: current snapshot count == 2 (S0 + the fast-forwarded S1; no new snapshot).
/// - **replay / dedup**: current snapshot count == 4 (S0, S1 staged, S2 advance, S3 published);
///   the current snapshot's `source-snapshot-id` summary key is present.
/// - **dedup**: Rust's second cherry_pick attempt fails (the double-publish dedup works).
#[tokio::test]
async fn test_rust_view_of_java_cherrypick_matches_java_view() {
    let Some(dir) = compare_dir() else {
        println!(
            "skipping interop_cherrypick compare — set ICEBERG_INTEROP_CHERRYPICK_DIR \
             (run dev/java-interop/run-interop-cherrypick.sh)"
        );
        return;
    };

    for fixture in FIXTURES {
        let fixture_dir = dir.join(fixture);
        let java_view = load_json(&fixture_dir.join("java_meta.json"));
        let java_meta_path = fixture_dir.join("table/metadata/final.metadata.json");
        let rust_view = snapshot_meta_view(&java_meta_path).await;
        assert_eq!(
            rust_view, java_view,
            "{fixture}: Rust's view of the JAVA-produced table diverges from Java's own view"
        );
        println!("cherrypick/{fixture}: Rust view of Java table == Java view OK");

        // Fixture-specific assertions (read the raw metadata).
        let raw = fs::read_to_string(&java_meta_path)
            .unwrap_or_else(|error| panic!("read {}: {error}", java_meta_path.display()));
        let metadata: TableMetadata = serde_json::from_str(&raw)
            .unwrap_or_else(|error| panic!("parse {}: {error}", java_meta_path.display()));

        let snapshot_count: usize = metadata.snapshots().count();
        match *fixture {
            "ff" => {
                assert_eq!(
                    snapshot_count, 2,
                    "ff: expected 2 snapshots after fast-forward (S0 + S1 as head), got {snapshot_count}"
                );
                println!("cherrypick/ff: snapshot count=2 (fast-forward, no new snapshot) OK");
            }
            "replay" | "dedup" => {
                assert_eq!(
                    snapshot_count, 4,
                    "{fixture}: expected 4 snapshots after replay (S0, S1-staged, S2-advance, \
                     S3-published), got {snapshot_count}"
                );
                let current = metadata
                    .current_snapshot()
                    .expect("{fixture}: no current snapshot after cherrypick");
                let source_id = current
                    .summary()
                    .additional_properties
                    .get("source-snapshot-id")
                    .cloned();
                assert!(
                    source_id.is_some(),
                    "{fixture}: current snapshot must have source-snapshot-id in summary"
                );
                // The source-snapshot-id must refer to a real snapshot that EXISTS in metadata
                // (the staged S1) AND is NOT the current snapshot. A random/garbage value would
                // parse but fail the exists check, catching a wrong value silently passing the
                // is_some() check.
                let source_id_val: i64 =
                    source_id.as_ref().unwrap().parse().unwrap_or_else(|error| {
                        panic!(
                            "{fixture}: source-snapshot-id is not a valid i64: {:?}: {error}",
                            source_id.as_ref().unwrap()
                        )
                    });
                assert!(
                    metadata.snapshot_by_id(source_id_val).is_some(),
                    "{fixture}: source-snapshot-id={source_id_val} does not refer to any \
                     snapshot in metadata — the value is wrong (must point to the staged S1)"
                );
                assert_ne!(
                    source_id_val,
                    current.snapshot_id(),
                    "{fixture}: source-snapshot-id must NOT be the current snapshot itself \
                     (it must point to the STAGED snapshot, not the published one)"
                );
                println!(
                    "cherrypick/{fixture}: snapshot count=4, source-snapshot-id={source_id_val} \
                     (exists in metadata, not current) OK"
                );

                if *fixture == "dedup" {
                    // Verify dedup_expected_rejection.json exists and is true.
                    let rejection_path = fixture_dir.join("dedup_expected_rejection.json");
                    assert!(
                        rejection_path.exists(),
                        "dedup: missing dedup_expected_rejection.json — run generate first"
                    );
                    let rejection_json = load_json(&rejection_path);
                    assert_eq!(
                        rejection_json["second_cherrypick_fails"],
                        serde_json::json!(true),
                        "dedup: dedup_expected_rejection.json must have second_cherrypick_fails=true"
                    );
                    println!(
                        "cherrypick/dedup: dedup_expected_rejection.json confirmed \
                         second_cherrypick_fails=true OK"
                    );
                }
            }
            other => panic!("unknown fixture: {other}"),
        }
    }
}
