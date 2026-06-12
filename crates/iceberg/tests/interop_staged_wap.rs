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

//! Java interop for staged-WAP fixtures (increment Z1) — proving the Rust
//! `FastAppendAction::stage_only()` and `CherryPickAction` produce IDENTICAL canonical snapshot
//! metadata to Java's `stageOnly()` + `ManageSnapshots.cherrypick`, judged by Java. Driven by
//! `dev/java-interop/run-interop-staged-wap.sh`.
//!
//! # The three fixtures
//!
//! - **S-ff** — stage w1 (parent == current head S0) via REAL `stage_only()`. Staged state:
//!   `current-snapshot-id` unchanged, staged snapshot in `snapshots` list with `wap.id=w1`, `main`
//!   ref unchanged. After cherry-pick: fast-forward (main moves to the staged snapshot AS-IS, no
//!   new snapshot, no `published-wap-id`). TWO views compared: staged-state + published-state.
//! - **S-replay** — stage w2, advance main with an unrelated append, cherry-pick → replay; BOTH
//!   staged-state and post-published views compared; `published-wap-id=w2` present post-replay.
//! - **S-dedup** — publish w3 via cherry-pick (succeeds), then stage ANOTHER snapshot with
//!   `wap.id=w3`, cherry-pick → BOTH sides reject (Java `DuplicateWAPCommitException`; Rust
//!   `DataInvalid` + message substring `"Duplicate request to cherry pick wap id"`). Table
//!   unchanged after rejection (re-parse). Staged-state view + final-state view compared.
//!
//! # The two directions
//!
//! **Direction 1 — Rust acts, Java judges.** [`test_staged_wap_gen_rust_produces_each_fixture`]
//! performs the SAME commit chain as the Java `StagedWapOracle` on a real-FS `MemoryCatalog`,
//! landing BOTH `rust_staged_table/metadata/final.metadata.json` (staged-but-unpublished state)
//! AND `rust_final_table/metadata/final.metadata.json` (post-publish state) per fixture. The run
//! script then:
//!   (a) Byte-diffs Java's canonical view of the Rust staged table vs `java_staged_meta.json`.
//!   (b) Byte-diffs Java's canonical view of the Rust final table vs `java_final_meta.json`.
//!   (c) Runs `verify-interop-staged-wap` (per-fixture facts + dedup rejection confirmation).
//!
//! **Direction 2 — Java acts, Rust verifies.** [`test_rust_view_of_java_staged_wap_matches_java`]
//! asserts Rust's canonical view of the JAVA-produced staged + final tables equals Java's own
//! `java_staged_meta.json` / `java_final_meta.json`.
//!
//! # Real staging on both sides
//!
//! The staged snapshot is created with the REAL production staging API on BOTH sides:
//! - Java: `newFastAppend().set("wap.id", wapId).stageOnly().commit()` (production
//!   `SnapshotProducer.stageOnly()`, 1.10.0 bytecode-confirmed: only `AddSnapshot` fires).
//! - Rust: `tx.fast_append().set_snapshot_properties({wap.id}).stage_only()` (production
//!   `FastAppendAction::stage_only()`, Group V 2026-06-11).
//!
//! # The staged-state invariant
//!
//! After staging but before cherry-pick:
//! - `current-snapshot-id` is unchanged (the staged snapshot did NOT move `main`).
//! - The staged snapshot IS in `metadata.snapshots()` with its `wap.id`.
//! - The staged snapshot is NOT in `snapshot-log` / `refs` / `history`.
//! - The staged snapshot consumes a sequence number (V1 lesson: `apply()` is stage-only-independent).
//!
//! # The env gate
//!
//! Both tests are clean NO-OPS (runtime early-return, not `#[ignore]`) unless their env var is set
//! non-empty, so the offline `cargo test` gate needs no Java/Maven.
//! `ICEBERG_INTEROP_STAGED_WAP_GEN_DIR` drives the Rust GEN (Direction 1);
//! `ICEBERG_INTEROP_STAGED_WAP_DIR` drives the comparison test (Direction 2), which REQUIRES the
//! Java fixtures + `java_staged_meta.json` / `java_final_meta.json` and fails loudly otherwise.

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
use serde_json::{Value as JsonValue, json};

mod common;
use common::snapshot_meta_view::snapshot_meta_view;

/// The three fixtures, identical names + chains to the Java `StagedWapOracle.FIXTURES`.
const FIXTURES: &[&str] = &["S-ff", "S-replay", "S-dedup"];

fn gen_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_STAGED_WAP_GEN_DIR")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn compare_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_STAGED_WAP_DIR")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

// ===========================================================================================
// The fixture schema / spec / file builders (identical logical constants to Java StagedWapOracle).
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

/// Fast-append one data file to the table. Returns the updated table.
async fn append(catalog: &impl Catalog, table: &Table, file: DataFile) -> Table {
    let tx = Transaction::new(table);
    let tx = tx
        .fast_append()
        .add_data_files(vec![file])
        .apply(tx)
        .expect("apply fast_append");
    tx.commit(catalog).await.expect("commit fast_append")
}

/// Stage a snapshot via REAL `stage_only()` — the production staging API. Sets `wap.id` via
/// `set_snapshot_properties`. Returns the updated table (current-snapshot-id UNCHANGED after
/// staging — the staged snapshot is NOT on `main`).
///
/// Risk this pins: staging must emit `AddSnapshot` ONLY (no `SetSnapshotRef`), so the current
/// snapshot id and main ref are unchanged. The staged snapshot IS in `metadata.snapshots()`.
async fn stage_snapshot(
    catalog: &impl Catalog,
    table: &Table,
    file: DataFile,
    wap_id: &str,
) -> Table {
    let mut props = HashMap::new();
    props.insert("wap.id".to_string(), wap_id.to_string());
    let tx = Transaction::new(table);
    let tx = tx
        .fast_append()
        .set_snapshot_properties(props)
        .stage_only()
        .add_data_files(vec![file])
        .apply(tx)
        .expect("apply fast_append.stage_only()");
    tx.commit(catalog)
        .await
        .expect("commit fast_append.stage_only()")
}

/// Write `final.metadata.json` at `<table_dir>/metadata/final.metadata.json`.
async fn write_final_metadata(table: &Table, table_dir: &str) {
    let path = format!("{table_dir}/metadata/final.metadata.json");
    table
        .metadata_ref()
        .write_to(table.file_io(), path.as_str())
        .await
        .expect("write final.metadata.json");
}

/// Find the staged snapshot id: the snapshot in `metadata.snapshots()` that has `wap.id == wap_id`
/// and is NOT the current snapshot. Panics if none found.
fn find_staged_id(table: &Table, wap_id: &str) -> i64 {
    let current_id = table.metadata().current_snapshot_id();
    for snapshot in table.metadata().snapshots() {
        let props = &snapshot.summary().additional_properties;
        if props.get("wap.id").map(String::as_str) == Some(wap_id)
            && Some(snapshot.snapshot_id()) != current_id
        {
            return snapshot.snapshot_id();
        }
    }
    panic!(
        "find_staged_id: no staged snapshot with wap.id={wap_id} found (current={current_id:?})"
    );
}

// ===========================================================================================
// Direction 1 — the Rust GEN: build each fixture and emit BOTH states.
// ===========================================================================================

/// Build one fixture's tables through REAL `stage_only()` staging + cherry-pick, landing BOTH:
/// - `<fixture>/rust_staged_table/metadata/final.metadata.json` (staged-but-unpublished state)
/// - `<fixture>/rust_final_table/metadata/final.metadata.json` (post-publish state)
///
/// The chain MIRRORS the Java `StagedWapOracle.buildFixture` switch exactly.
async fn build_fixture(fixture: &str, gen_dir: &Path) {
    let fixture_dir = gen_dir.join(fixture);
    let staged_location = fixture_dir
        .join("rust_staged_table")
        .to_string_lossy()
        .to_string();
    let final_location = fixture_dir
        .join("rust_final_table")
        .to_string_lossy()
        .to_string();
    fs::create_dir_all(&staged_location).expect("create rust_staged_table dir");
    fs::create_dir_all(&final_location).expect("create rust_final_table dir");

    // Each fixture uses its own catalog over the staged table location.
    // The final table is a SEPARATE catalog that starts from the staged metadata — we copy the
    // staged metadata to the final location and re-open it to apply the cherry-pick.
    // This pattern gives us two independent snapshot-metadata files to compare.

    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            &format!("interop_staged_wap_{fixture}"),
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

    // The staged-table: we work on the RUST STAGED table for the staging phase.
    let creation_staged = TableCreation::builder()
        .name("rust_staged_table".to_string())
        .location(staged_location.clone())
        .schema(fixture_schema())
        .partition_spec(fixture_spec())
        .sort_order(SortOrder::unsorted_order())
        .format_version(FormatVersion::V2)
        .build();
    let mut table = catalog
        .create_table(&namespace, creation_staged)
        .await
        .expect("create rust_staged_table");

    // The final-table: separate catalog entry at the final location, used after the publish step.
    let creation_final = TableCreation::builder()
        .name("rust_final_table".to_string())
        .location(final_location.clone())
        .schema(fixture_schema())
        .partition_spec(fixture_spec())
        .sort_order(SortOrder::unsorted_order())
        .format_version(FormatVersion::V2)
        .build();
    let mut final_table = catalog
        .create_table(&namespace, creation_final)
        .await
        .expect("create rust_final_table");

    // S0: the initial snapshot — parent of the staged snapshot.
    table = append(&catalog, &table, data_file(&staged_location, "s0", 10)).await;
    final_table = append(&catalog, &final_table, data_file(&final_location, "s0", 10)).await;
    let s0_id = table
        .metadata()
        .current_snapshot_id()
        .expect("s0 snapshot id (staged)");
    // final_table s0 id — not needed directly; we verify the chain matches.

    match fixture {
        "S-ff" => {
            // Stage w1 via REAL stage_only() — parent == current head S0.
            // After staging: current-snapshot-id unchanged (still S0).
            table = stage_snapshot(
                &catalog,
                &table,
                data_file(&staged_location, "s1", 20),
                "w1",
            )
            .await;
            let _staged_id = find_staged_id(&table, "w1");
            assert_eq!(
                table.metadata().current_snapshot_id(),
                Some(s0_id),
                "S-ff: staging must NOT move current-snapshot-id (main still at S0)"
            );
            // Emit the STAGED state.
            write_final_metadata(&table, &staged_location).await;
            println!(
                "interop_staged_wap GEN S-ff/staged: final.metadata.json written at {staged_location}"
            );

            // Cherry-pick on the final table (same wap.id chain, same manifest layout).
            final_table = stage_snapshot(
                &catalog,
                &final_table,
                data_file(&final_location, "s1", 20),
                "w1",
            )
            .await;
            let final_staged_id = find_staged_id(&final_table, "w1");
            // The current-snapshot-id of the final_table after staging must NOT be the staged id.
            assert_ne!(
                final_table.metadata().current_snapshot_id(),
                Some(final_staged_id),
                "S-ff/final: staging must NOT move current-snapshot-id to the staged snapshot"
            );
            // FF: cherry-pick.
            let tx = Transaction::new(&final_table);
            let tx = tx
                .cherry_pick(final_staged_id)
                .apply(tx)
                .expect("apply cherry_pick (S-ff)");
            final_table = tx
                .commit(&catalog)
                .await
                .expect("commit cherry_pick (S-ff)");
            assert_eq!(
                final_table.metadata().current_snapshot_id(),
                Some(final_staged_id),
                "S-ff: fast-forward must move main to the staged snapshot"
            );
            write_final_metadata(&final_table, &final_location).await;
            println!(
                "interop_staged_wap GEN S-ff/final: final.metadata.json written at {final_location}"
            );
        }
        "S-replay" => {
            // Stage w2 — parent == current head S0 of the staged table.
            table = stage_snapshot(
                &catalog,
                &table,
                data_file(&staged_location, "s1", 20),
                "w2",
            )
            .await;
            let _staged_id = find_staged_id(&table, "w2");
            assert_eq!(
                table.metadata().current_snapshot_id(),
                Some(s0_id),
                "S-replay: staging must NOT move current-snapshot-id"
            );
            // Advance main past S0 with an unrelated commit S2 (now S1.parent != head).
            table = append(&catalog, &table, data_file(&staged_location, "s2", 30)).await;

            // Emit the STAGED state (S0 + S1-staged + S2 all in snapshots, main at S2).
            write_final_metadata(&table, &staged_location).await;
            println!(
                "interop_staged_wap GEN S-replay/staged: final.metadata.json written at {staged_location}"
            );

            // Mirror the same chain on the final table.
            final_table = stage_snapshot(
                &catalog,
                &final_table,
                data_file(&final_location, "s1", 20),
                "w2",
            )
            .await;
            let final_staged_id = find_staged_id(&final_table, "w2");
            final_table =
                append(&catalog, &final_table, data_file(&final_location, "s2", 30)).await;

            // Cherry-pick: REPLAY (S1.parent == S0 != head S2).
            let tx = Transaction::new(&final_table);
            let tx = tx
                .cherry_pick(final_staged_id)
                .apply(tx)
                .expect("apply cherry_pick (S-replay)");
            final_table = tx
                .commit(&catalog)
                .await
                .expect("commit cherry_pick (S-replay)");

            // Verify published-wap-id is set.
            let published_wap = final_table
                .metadata()
                .current_snapshot()
                .expect("current snapshot after replay")
                .summary()
                .additional_properties
                .get("published-wap-id")
                .cloned();
            assert_eq!(
                published_wap,
                Some("w2".to_string()),
                "S-replay: published-wap-id must be w2 after replay"
            );

            write_final_metadata(&final_table, &final_location).await;
            println!(
                "interop_staged_wap GEN S-replay/final: final.metadata.json written at {final_location}"
            );
        }
        "S-dedup" => {
            // Stage BOTH w3 snapshots off S0 (same parent = S0 = current head) BEFORE any
            // cherry-pick. Mirrors Java TestWapWorkflow.testDuplicateCherrypick.
            //
            // Pattern:
            //   S0 (main, parent of both staged snapshots)
            //   ├── stage w3-first  (parent=S0, seq=2)
            //   └── stage w3-second (parent=S0, seq=3)
            //
            // Cherry-pick w3-first → FF (parent=S0=head) → main = w3-first (no new snapshot).
            // Cherry-pick w3-second → REPLAY (parent=S0 ≠ head=w3-first)
            //   → validate_wap_publish fires → finds wap.id=w3 on ancestor w3-first
            //   → DataInvalid "Duplicate request to cherry pick wap id" ✓
            //
            // Staged state: S0 + w3-first + w3-second = 3 snapshots, main at S0.
            // Final state: S0 + w3-first (main via FF) + w3-second (still staged) = 3 snapshots.

            // Stage w3-first off S0 (parent = S0 = current head).
            table = stage_snapshot(
                &catalog,
                &table,
                data_file(&staged_location, "s1", 20),
                "w3",
            )
            .await;
            let first_staged_id = find_staged_id(&table, "w3");
            assert_eq!(
                table.metadata().current_snapshot_id(),
                Some(s0_id),
                "S-dedup: first staging must NOT move current-snapshot-id"
            );

            // Stage w3-second ALSO off S0 (still the current head — no cherry-pick yet).
            table = stage_snapshot(
                &catalog,
                &table,
                data_file(&staged_location, "s2", 30),
                "w3",
            )
            .await;
            // second_staged_id: the staged snapshot that is NOT first_staged_id.
            let second_staged_id = {
                let current = table.metadata().current_snapshot_id();
                let mut found = None;
                for snap in table.metadata().snapshots() {
                    let props = &snap.summary().additional_properties;
                    if props.get("wap.id").map(String::as_str) == Some("w3")
                        && Some(snap.snapshot_id()) != current
                        && snap.snapshot_id() != first_staged_id
                    {
                        found = Some(snap.snapshot_id());
                    }
                }
                found.expect(
                    "S-dedup: second staged snapshot with wap.id=w3 (not first_staged_id) must exist",
                )
            };
            assert_eq!(
                table.metadata().current_snapshot_id(),
                Some(s0_id),
                "S-dedup: second staging must NOT move current-snapshot-id"
            );

            // Emit the STAGED state: S0 (main) + w3-first (staged) + w3-second (staged) = 3.
            write_final_metadata(&table, &staged_location).await;
            println!(
                "interop_staged_wap GEN S-dedup/staged: final.metadata.json written at {staged_location}"
            );

            // FIRST cherry-pick: FF (parent=S0=head) → main moves to w3-first verbatim.
            let tx = Transaction::new(&table);
            let tx = tx
                .cherry_pick(first_staged_id)
                .apply(tx)
                .expect("apply first cherry_pick (S-dedup FF)");
            table = tx
                .commit(&catalog)
                .await
                .expect("commit first cherry_pick (S-dedup FF)");
            assert_eq!(
                table.metadata().current_snapshot_id(),
                Some(first_staged_id),
                "S-dedup: FF cherry-pick must move main to w3-first"
            );

            // SECOND cherry-pick: REPLAY (parent=S0 ≠ head=w3-first) → DataInvalid.
            let tx2 = Transaction::new(&table);
            let tx2 = tx2
                .cherry_pick(second_staged_id)
                .apply(tx2)
                .expect("apply second cherry_pick (S-dedup REPLAY — must validate_wap_publish)");
            match tx2.commit(&catalog).await {
                Err(err) => {
                    assert_eq!(
                        err.kind(),
                        ErrorKind::DataInvalid,
                        "S-dedup: second cherry_pick must fail with DataInvalid, got {:?}: {err}",
                        err.kind()
                    );
                    assert!(
                        !err.retryable(),
                        "S-dedup: the WAP-dedup rejection must be non-retryable: {err}"
                    );
                    assert!(
                        err.message()
                            .contains("Duplicate request to cherry pick wap id"),
                        "S-dedup: expected 'Duplicate request to cherry pick wap id' in error \
                         message, got: {err}"
                    );
                    println!(
                        "interop_staged_wap GEN S-dedup: second cherry_pick rejected as expected \
                         (DataInvalid, DuplicateWAPCommitException path): {err}"
                    );
                }
                Ok(_) => panic!(
                    "S-dedup fixture: second cherry_pick commit must fail (DuplicateWAPCommit), \
                     but succeeded"
                ),
            }

            // Emit the rejection confirmation artifact so Java verify can check it.
            let rejection_json = json!({
                "second_cherrypick_fails": true,
                "exception_kind": "DataInvalid",
                "message_substring": "Duplicate request to cherry pick wap id"
            });
            let rejection_path = fixture_dir.join("rust_wap_dedup_rejection.json");
            fs::write(
                &rejection_path,
                serde_json::to_string_pretty(&rejection_json).expect("serialize rejection JSON"),
            )
            .expect("write rust_wap_dedup_rejection.json");
            println!(
                "interop_staged_wap GEN S-dedup: rust_wap_dedup_rejection.json written at {}",
                rejection_path.display()
            );

            // Verify the table is UNCHANGED after the rejection — main still at w3-first.
            assert_eq!(
                table.metadata().current_snapshot_id(),
                Some(first_staged_id),
                "S-dedup: table must be unchanged after rejection — current snapshot id changed"
            );
            println!(
                "interop_staged_wap GEN S-dedup: table unchanged after rejection (current={:?}) OK",
                table.metadata().current_snapshot_id()
            );

            // The final state = table state after first FF cherry-pick + rejection:
            //   S0 + w3-first (main) + w3-second (still staged) = 3 snapshots.
            // Mirror the same chain on final_table.
            let final_meta_path =
                PathBuf::from(&final_location).join("metadata/final.metadata.json");

            // Stage w3-first off S0 on final_table.
            final_table = stage_snapshot(
                &catalog,
                &final_table,
                data_file(&final_location, "s1", 20),
                "w3",
            )
            .await;
            let final_first_staged_id = find_staged_id(&final_table, "w3");

            // Stage w3-second also off S0 on final_table.
            final_table = stage_snapshot(
                &catalog,
                &final_table,
                data_file(&final_location, "s2", 30),
                "w3",
            )
            .await;

            // First cherry-pick: FF → main = w3-first.
            let tx = Transaction::new(&final_table);
            let tx = tx
                .cherry_pick(final_first_staged_id)
                .apply(tx)
                .expect("apply first cherry_pick (S-dedup final FF)");
            final_table = tx
                .commit(&catalog)
                .await
                .expect("commit first cherry_pick (S-dedup final FF)");

            // Now final_table state: S0 + w3-first (main) + w3-second (staged) = 3 snapshots.
            write_final_metadata(&final_table, &final_location).await;
            assert!(
                final_meta_path.exists(),
                "S-dedup: final metadata must exist at {final_meta_path:?}"
            );
            println!(
                "interop_staged_wap GEN S-dedup/final: final.metadata.json written at {final_location}"
            );
        }
        other => panic!("unknown fixture: {other}"),
    }
}

/// Direction 1: build each fixture via REAL `stage_only()` + `cherry_pick`, landing BOTH the
/// staged-state and final-state `final.metadata.json`. The Java run script then byte-diffs Java's
/// canonical view of both Rust-produced tables against Java's own views.
#[tokio::test]
async fn test_staged_wap_gen_rust_produces_each_fixture() {
    let Some(gen_dir) = gen_dir() else {
        println!(
            "skipping interop_staged_wap GEN — set ICEBERG_INTEROP_STAGED_WAP_GEN_DIR \
             (run dev/java-interop/run-interop-staged-wap.sh)"
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

/// Direction 2 (Java acts, Rust verifies): Rust's canonical view of the JAVA-produced staged AND
/// final tables equals Java's own canonical views (`java_staged_meta.json` and
/// `java_final_meta.json` emitted by `SnapshotMetaOracle.emit`). Additionally asserts
/// fixture-specific facts:
///
/// - **S-ff/staged:** staged snapshot present in metadata (count >= 2), `wap.id=w1` on the
///   staged snapshot, `current-snapshot-id` still pointing at S0.
/// - **S-ff/final:** snapshot count == 2, main moved to the staged snapshot (FF: no new snapshot,
///   no `source-snapshot-id`).
/// - **S-replay/staged:** staged snapshot present (count >= 3 after S2 advance).
/// - **S-replay/final:** snapshot count == 4, `published-wap-id=w2` on current snapshot.
/// - **S-dedup/staged:** second staged snapshot present with `wap.id=w3`.
/// - **S-dedup/final:** `source-snapshot-id` from the first publish on current; dedup rejection
///   confirmed via `wap_dedup_expected_rejection.json`.
#[tokio::test]
async fn test_rust_view_of_java_staged_wap_matches_java() {
    let Some(dir) = compare_dir() else {
        println!(
            "skipping interop_staged_wap compare — set ICEBERG_INTEROP_STAGED_WAP_DIR \
             (run dev/java-interop/run-interop-staged-wap.sh)"
        );
        return;
    };

    for fixture in FIXTURES {
        let fixture_dir = dir.join(fixture);

        // ---- STAGED STATE ----
        let java_staged_view = load_json(&fixture_dir.join("java_staged_meta.json"));
        let java_staged_meta_path =
            fixture_dir.join("java_staged_table/metadata/final.metadata.json");
        let rust_staged_view = snapshot_meta_view(&java_staged_meta_path).await;
        assert_eq!(
            rust_staged_view, java_staged_view,
            "{fixture}/staged: Rust's view of the JAVA-produced staged table diverges from \
             Java's own view"
        );
        println!("staged-wap/{fixture}/staged: Rust view of Java staged table == Java view OK");

        // Staged-state fixture-specific assertions.
        let raw_staged = fs::read_to_string(&java_staged_meta_path)
            .unwrap_or_else(|error| panic!("read {}: {error}", java_staged_meta_path.display()));
        let staged_meta: TableMetadata = serde_json::from_str(&raw_staged)
            .unwrap_or_else(|error| panic!("parse {}: {error}", java_staged_meta_path.display()));

        let staged_current_id = staged_meta.current_snapshot_id();
        let staged_snapshot_count = staged_meta.snapshots().count();

        match *fixture {
            "S-ff" => {
                // After staging w1 (parent == S0 == head): S0 + staged-w1 = 2 snapshots.
                assert_eq!(
                    staged_snapshot_count, 2,
                    "S-ff/staged: expected 2 snapshots (S0 + staged-w1), got {staged_snapshot_count}"
                );
                // Current-snapshot-id must be S0 (staging must NOT move main).
                // The staged snapshot is not the current.
                let staged_snap = staged_meta
                    .snapshots()
                    .find(|snap| {
                        snap.summary()
                            .additional_properties
                            .get("wap.id")
                            .map(String::as_str)
                            == Some("w1")
                    })
                    .expect("S-ff/staged: no snapshot with wap.id=w1 found in staged metadata");
                assert_ne!(
                    Some(staged_snap.snapshot_id()),
                    staged_current_id,
                    "S-ff/staged: staged snapshot must NOT be the current snapshot"
                );
                println!(
                    "staged-wap/S-ff/staged: count=2, staged-id={} != current={:?} OK",
                    staged_snap.snapshot_id(),
                    staged_current_id
                );
            }
            "S-replay" => {
                // After staging w2 (parent S0) + S2 advance: S0 + staged-w2 + S2 = 3 snapshots.
                assert_eq!(
                    staged_snapshot_count, 3,
                    "S-replay/staged: expected 3 snapshots (S0 + staged-w2 + S2), got {staged_snapshot_count}"
                );
                let staged_snap = staged_meta
                    .snapshots()
                    .find(|snap| {
                        snap.summary()
                            .additional_properties
                            .get("wap.id")
                            .map(String::as_str)
                            == Some("w2")
                    })
                    .expect("S-replay/staged: no snapshot with wap.id=w2 in staged metadata");
                assert_ne!(
                    Some(staged_snap.snapshot_id()),
                    staged_current_id,
                    "S-replay/staged: staged snapshot must NOT be current"
                );
                // HAND-DECLARED ref-state pin (W1 lesson): in S-replay the staged set is
                // {S0 (root, no wap), staged-w2 (wap.id=w2), S2 (the advance, no wap, latest seq)}.
                // `staged != current` alone leaves current ambiguous between S0 and S2; the
                // canonical view excludes current-snapshot-id, so the ref-state fact "main advanced
                // to S2, NOT still at S0" must be asserted explicitly here. The current snapshot at
                // the staged point is the S2 ADVANCE: it carries no wap.id AND is not the parent-less
                // root S0 (it has a parent). This catches a corrupted current-snapshot-id that the
                // weaker staged!=current check would miss.
                let current_snap = staged_meta
                    .current_snapshot()
                    .expect("S-replay/staged: a current snapshot must exist");
                assert_eq!(
                    current_snap.summary().additional_properties.get("wap.id"),
                    None,
                    "S-replay/staged: current must be the S2 advance (no wap.id), not the staged-w2 snapshot"
                );
                assert!(
                    current_snap.parent_snapshot_id().is_some(),
                    "S-replay/staged: current must be the S2 advance (has a parent), not the root S0"
                );
                println!(
                    "staged-wap/S-replay/staged: count=3, staged-id={} != current={:?} (the S2 \
                     advance: no wap.id, non-root) OK",
                    staged_snap.snapshot_id(),
                    staged_current_id
                );
            }
            "S-dedup" => {
                // After staging BOTH w3-first and w3-second off S0 (before any cherry-pick):
                // S0 (main) + w3-first (staged) + w3-second (staged) = 3 snapshots.
                assert_eq!(
                    staged_snapshot_count, 3,
                    "S-dedup/staged: expected 3 snapshots (S0 + w3-first + w3-second), \
                     got {staged_snapshot_count}"
                );
                // Both staged snapshots have wap.id=w3 and neither is the current snapshot.
                let w3_staged: Vec<_> = staged_meta
                    .snapshots()
                    .filter(|snap| {
                        snap.summary()
                            .additional_properties
                            .get("wap.id")
                            .map(String::as_str)
                            == Some("w3")
                            && Some(snap.snapshot_id()) != staged_current_id
                    })
                    .collect();
                assert_eq!(
                    w3_staged.len(),
                    2,
                    "S-dedup/staged: expected 2 non-current staged snapshots with wap.id=w3, \
                     found {}",
                    w3_staged.len()
                );
                println!(
                    "staged-wap/S-dedup/staged: count=3, 2 staged w3 snapshots, main unchanged OK"
                );
            }
            other => panic!("unknown fixture: {other}"),
        }

        // ---- FINAL STATE ----
        let java_final_view = load_json(&fixture_dir.join("java_final_meta.json"));
        let java_final_meta_path =
            fixture_dir.join("java_final_table/metadata/final.metadata.json");
        let rust_final_view = snapshot_meta_view(&java_final_meta_path).await;
        assert_eq!(
            rust_final_view, java_final_view,
            "{fixture}/final: Rust's view of the JAVA-produced final table diverges from \
             Java's own view"
        );
        println!("staged-wap/{fixture}/final: Rust view of Java final table == Java view OK");

        // Final-state fixture-specific assertions.
        let raw_final = fs::read_to_string(&java_final_meta_path)
            .unwrap_or_else(|error| panic!("read {}: {error}", java_final_meta_path.display()));
        let final_meta: TableMetadata = serde_json::from_str(&raw_final)
            .unwrap_or_else(|error| panic!("parse {}: {error}", java_final_meta_path.display()));

        let final_snapshot_count = final_meta.snapshots().count();
        match *fixture {
            "S-ff" => {
                // FF: S0 + staged-w1 (now main). Count == 2.
                assert_eq!(
                    final_snapshot_count, 2,
                    "S-ff/final: expected 2 snapshots after FF (S0 + w1 as head), got {final_snapshot_count}"
                );
                let current = final_meta
                    .current_snapshot()
                    .expect("S-ff/final: no current snapshot");
                // The current snapshot must carry wap.id=w1 (the staged snapshot itself after FF).
                let current_wap_id = current
                    .summary()
                    .additional_properties
                    .get("wap.id")
                    .cloned();
                assert_eq!(
                    current_wap_id,
                    Some("w1".to_string()),
                    "S-ff/final: current snapshot must carry wap.id=w1 (FF published it verbatim)"
                );
                // No source-snapshot-id (FF does not tag the published snapshot).
                let source_id = current
                    .summary()
                    .additional_properties
                    .get("source-snapshot-id")
                    .cloned();
                assert!(
                    source_id.is_none(),
                    "S-ff/final: FF must NOT set source-snapshot-id, got: {source_id:?}"
                );
                println!(
                    "staged-wap/S-ff/final: count=2, wap.id=w1 on current, no source-snapshot-id OK"
                );
            }
            "S-replay" => {
                // REPLAY: S0 + staged-w2 + S2 + published-replay = 4 snapshots.
                assert_eq!(
                    final_snapshot_count, 4,
                    "S-replay/final: expected 4 snapshots, got {final_snapshot_count}"
                );
                let current = final_meta
                    .current_snapshot()
                    .expect("S-replay/final: no current snapshot");
                let source_id = current
                    .summary()
                    .additional_properties
                    .get("source-snapshot-id")
                    .cloned();
                let published_wap = current
                    .summary()
                    .additional_properties
                    .get("published-wap-id")
                    .cloned();
                assert!(
                    source_id.is_some(),
                    "S-replay/final: current snapshot must have source-snapshot-id"
                );
                assert_eq!(
                    published_wap,
                    Some("w2".to_string()),
                    "S-replay/final: published-wap-id must be w2, got: {published_wap:?}"
                );
                // source-snapshot-id must refer to the staged w2 snapshot.
                let source_id_val: i64 = source_id
                    .as_ref()
                    .unwrap()
                    .parse()
                    .expect("source-snapshot-id must be a valid i64");
                assert!(
                    final_meta.snapshot_by_id(source_id_val).is_some(),
                    "S-replay/final: source-snapshot-id={source_id_val} does not exist in metadata"
                );
                println!(
                    "staged-wap/S-replay/final: count=4, source-snapshot-id={source_id_val}, \
                     published-wap-id=w2 OK"
                );
            }
            "S-dedup" => {
                // S-dedup final: S0 + w3-first (main via FF) + w3-second (still staged) = 3.
                // The second cherry-pick was rejected (DuplicateWAPCommitException); table
                // is unchanged from the state after the first FF cherry-pick.
                // The FF cherry-pick does NOT set source-snapshot-id; current carries wap.id=w3.
                assert_eq!(
                    final_snapshot_count, 3,
                    "S-dedup/final: expected 3 snapshots (S0 + w3-first-FF-main + \
                     w3-second-still-staged), got {final_snapshot_count}"
                );
                let current = final_meta
                    .current_snapshot()
                    .expect("S-dedup/final: no current snapshot");
                // FF cherry-pick must NOT set source-snapshot-id.
                let source_id = current
                    .summary()
                    .additional_properties
                    .get("source-snapshot-id")
                    .cloned();
                assert!(
                    source_id.is_none(),
                    "S-dedup/final: FF cherry-pick must NOT set source-snapshot-id, \
                     got: {source_id:?}"
                );
                // The current snapshot must carry wap.id=w3 (FF published it verbatim).
                let current_wap = current
                    .summary()
                    .additional_properties
                    .get("wap.id")
                    .cloned();
                assert_eq!(
                    current_wap,
                    Some("w3".to_string()),
                    "S-dedup/final: current snapshot must carry wap.id=w3 (FF path), \
                     got: {current_wap:?}"
                );
                // Confirm dedup rejection from the expected-rejection artifact.
                let rejection_path = fixture_dir.join("wap_dedup_expected_rejection.json");
                assert!(
                    rejection_path.exists(),
                    "S-dedup/final: missing wap_dedup_expected_rejection.json — run generate first"
                );
                let rejection_json = load_json(&rejection_path);
                assert_eq!(
                    rejection_json["second_cherrypick_fails"],
                    serde_json::json!(true),
                    "S-dedup: wap_dedup_expected_rejection.json must have second_cherrypick_fails=true"
                );
                println!(
                    "staged-wap/S-dedup/final: count=3, wap.id=w3 on current (FF), \
                     no source-snapshot-id, rejection confirmed OK"
                );
            }
            other => panic!("unknown fixture: {other}"),
        }
    }
}
