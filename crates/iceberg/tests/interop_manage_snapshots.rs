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

//! Java interop tests for the `ManageSnapshots` transaction action (snapshot-reference operations).
//!
//! This is the byte-level parity evidence that flips the GAP_MATRIX snapshot-refs / ManageSnapshots rows
//! to ✅. It mirrors the proven `interop_update_schema.rs` / `interop_update_partition_spec.rs` harness and
//! exercises BOTH directions against the Java `iceberg-core` reference oracle in `dev/java-interop/`:
//!
//! - **Direction 1 (Rust reproduces Java's ref evolution).** For each scenario, load the Java-written
//!   `base.metadata.json` (which carries a real snapshot HISTORY + refs: snapshots ROOT, CURRENT (child of
//!   ROOT), SIBLING (child of ROOT); refs `main`→CURRENT, `dev` branch→CURRENT, `stable` tag→ROOT — the
//!   shape the Rust action's `forked_table()` fixture replicates), apply the SAME `ManageSnapshots`
//!   op-sequence via the public transaction API ([`Transaction::manage_snapshots`] +
//!   [`ApplyTransactionAction::apply`] + [`Transaction::commit`] against an in-memory catalog), and assert
//!   the Rust-evolved REFS map is **structurally equal** to Java's `java_evolved.metadata.json` — each
//!   ref's snapshot-id, branch-vs-tag kind, and retention fields (min_snapshots_to_keep /
//!   max_snapshot_age_ms / max_ref_age_ms), plus the current-snapshot-id (`main`). This runs in the normal
//!   offline suite — no Java / Docker needed; it reads the committed fixtures.
//! - **Direction 2 (Java reads what Rust writes).** When the `ICEBERG_INTEROP_GEN` env var is set, the
//!   same test writes the Rust-evolved `TableMetadata` (serialized via `serde_json`) to
//!   `rust_evolved.metadata.json` in each scenario dir for the Java oracle's `verify` step. A normal
//!   `cargo test` run does NOT write files (the env var is unset); only `dev/java-interop/run.sh` sets it.
//!
//! Comparison is by PARSING the `refs` of both metadata files into the Rust [`SnapshotReference`] /
//! [`SnapshotRetention`] model and asserting structural equality — NOT by comparing raw JSON bytes
//! (Jackson and serde_json differ in key order / whitespace). Logical ref identity (snapshot-id + kind +
//! retention) and the current snapshot are the contract, not byte identity. Snapshots themselves are
//! unchanged by ref operations, so the snapshot list is not compared.
//!
//! There is no public `refs()` accessor on `TableMetadata` returning the typed [`SnapshotReference`] (only
//! `snapshot_for_ref`, which yields a `Snapshot` and drops the kind + retention). So the refs are recovered
//! by serializing the evolved `TableMetadata` to a `serde_json::Value`, taking the `refs` object, and
//! deserializing each value into [`SnapshotReference`] — the public serde of the ref model, no production
//! accessor required.
//!
//! The evolution is driven through a real [`MemoryCatalog`] commit (not a raw `TableMetadataBuilder`
//! call) because the `TransactionAction::commit` seam is crate-private; the catalog path is the public API
//! and additionally exercises the optimistic-concurrency `RefSnapshotIdMatch` requirement checks end to
//! end. The catalog is backed by a `LocalFsStorageFactory` over a temp dir so the exact Java-written base
//! metadata (with its precise snapshot history + refs) is the registered table.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use iceberg::io::{FileIOBuilder, LocalFsStorageFactory};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{MAIN_BRANCH, SnapshotReference, SnapshotRetention, TableMetadata};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};

/// The forked base's snapshot ids — MUST match `InteropOracle.SnapshotOracle.{ROOT,CURRENT}_ID`.
/// `main` and `dev` start at CURRENT (a child of ROOT); `stable` (a tag) starts at ROOT.
const ROOT: i64 = 3051729675574597004;
const CURRENT: i64 = 3055729675574597004;

/// The timestamp (ms) of the ROOT snapshot — MUST match `InteropOracle.SnapshotOracle.ROOT_TS_MS`. The
/// `rollback_to_time` scenario uses `ROOT_TS_MS + 1`, which (strictly `<` CURRENT's timestamp) resolves to
/// ROOT.
const ROOT_TS_MS: i64 = 1515100955770;

/// The timestamp (ms) of the CURRENT snapshot — MUST match `InteropOracle.SnapshotOracle.CURRENT_TS_MS`.
/// Used to pin the strict-`<` boundary: rolling to a timestamp EQUAL to CURRENT's own timestamp must NOT
/// select CURRENT (it is not strictly older than itself); it must fall back to ROOT.
const CURRENT_TS_MS: i64 = 1555100955770;

/// Root of the committed interop fixtures, relative to the `iceberg` crate manifest.
fn fixtures_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/interop/manage_snapshots")
}

/// The full set of scenarios, in the same order as the Java oracle's `SnapshotOracle.scenarios()`.
const SCENARIOS: &[&str] = &[
    "create_branch_and_tag",
    "rollback_to_ancestor",
    "rollback_to_time",
    "set_current_snapshot",
    "fast_forward",
    "retention",
    "remove_and_rename",
];

/// Apply a scenario's op-sequence onto a [`Transaction`] via the manage-snapshots action. These MUST
/// mirror, op for op, the Java oracle's `SnapshotOracle.scenarios()` in
/// `dev/java-interop/.../InteropOracle.java`. Returns the transaction with the action queued.
fn apply_scenario_ops(scenario: &str, transaction: Transaction) -> Transaction {
    let action = transaction.manage_snapshots();
    let action = match scenario {
        // Create a branch `audit` @ROOT and a tag `release-1` @CURRENT.
        "create_branch_and_tag" => action
            .create_branch("audit", ROOT)
            .create_tag("release-1", CURRENT),

        // Roll `main` (CURRENT) back to ROOT, which IS an ancestor (ancestry-checked rollback).
        "rollback_to_ancestor" => action.rollback_to(ROOT),

        // A timestamp strictly between ROOT and CURRENT resolves to ROOT (the newest ancestor older than
        // it; CURRENT is too new). Cross-checks the strict-`<` semantics shared with Java
        // `SetSnapshotOperation.findLatestAncestorOlderThan`.
        "rollback_to_time" => action.rollback_to_time(ROOT_TS_MS + 1),

        // Set `main` to ROOT with no ancestry requirement (Java `setCurrentSnapshot`).
        "set_current_snapshot" => action.set_current_snapshot(ROOT),

        // Create a branch `staging` @ROOT and fast-forward it to `main` (@CURRENT). ROOT is an ancestor of
        // CURRENT, so `staging` advances to CURRENT.
        "fast_forward" => action
            .create_branch("staging", ROOT)
            .fast_forward("staging", MAIN_BRANCH),

        // Set branch-only retention on the `dev` branch and tag-legal retention on the `stable` tag.
        "retention" => action
            .set_min_snapshots_to_keep("dev", 5)
            .set_max_snapshot_age_ms("dev", 86400000)
            .set_max_ref_age_ms("stable", 604800000),

        // Remove the `stable` tag and rename the `dev` branch to `feature`.
        "remove_and_rename" => action.remove_tag("stable").rename_branch("dev", "feature"),

        other => panic!("unknown interop scenario: {other}"),
    };
    action
        .apply(transaction)
        .expect("queue ManageSnapshotsAction onto the transaction")
}

/// Load a `TableMetadata` fixture from a scenario directory.
fn load_metadata(scenario_dir: &Path, file_name: &str) -> TableMetadata {
    let path = scenario_dir.join(file_name);
    let json = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    serde_json::from_str::<TableMetadata>(&json)
        .unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
}

/// Recover the typed refs map from a `TableMetadata` by round-tripping through `serde_json`. There is no
/// public `refs()` accessor returning [`SnapshotReference`] (only `snapshot_for_ref`, which drops kind +
/// retention), so the refs are taken from the serialized `refs` object and re-parsed into the public ref
/// model. An empty/absent `refs` object yields an empty map.
fn refs_of(metadata: &TableMetadata) -> HashMap<String, SnapshotReference> {
    let value = serde_json::to_value(metadata).expect("serialize TableMetadata to a JSON value");
    match value.get("refs") {
        Some(refs) => serde_json::from_value::<HashMap<String, SnapshotReference>>(refs.clone())
            .expect("deserialize the refs object into the typed ref model"),
        None => HashMap::new(),
    }
}

/// Register `base` as a table in a fresh local-fs-backed in-memory catalog, apply `queue_action` to a
/// transaction over it, commit, and return the evolved metadata. Drives the SAME public-API path the
/// schema / partition interop tests use: `MemoryCatalog::register_table` → `Transaction` →
/// `ApplyTransactionAction` → `Transaction::commit` (which exercises the optimistic-concurrency
/// `RefSnapshotIdMatch` checks). `table_name` only needs to be unique within the run.
async fn register_and_commit(
    table_name: &str,
    base: TableMetadata,
    queue_action: impl FnOnce(Transaction) -> Transaction,
) -> TableMetadata {
    // A temp warehouse backed by the local filesystem so the catalog can read/write metadata files.
    let warehouse = tempfile::tempdir().expect("create temp warehouse dir");
    let warehouse_path = warehouse.path().to_str().expect("utf-8 warehouse path");

    // Write the exact base metadata into the warehouse via a local-fs FileIO, then register it. The
    // metadata file must live under a `metadata/` subdirectory of the table location AND be named
    // `<version>-<uuid>.metadata.json`, or the catalog's commit (which parses the version from the
    // filename to derive the next metadata path) rejects it.
    let file_io = FileIOBuilder::new(Arc::new(LocalFsStorageFactory)).build();
    let base_location = format!(
        "{warehouse_path}/{table_name}/metadata/00000-00000000-0000-0000-0000-000000000000.metadata.json"
    );
    base.write_to(&file_io, &base_location)
        .await
        .expect("write base metadata to warehouse");

    // The catalog must use the local filesystem (its default storage is in-memory and would not see the
    // base metadata file we just wrote), so inject a `LocalFsStorageFactory`.
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "interop",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_string(),
                warehouse_path.to_string(),
            )]),
        )
        .await
        .expect("build memory catalog");

    let namespace = NamespaceIdent::new("interop".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create namespace");

    let table_ident = TableIdent::new(namespace, table_name.to_string());
    let table = catalog
        .register_table(&table_ident, base_location)
        .await
        .expect("register table from base metadata");

    let transaction = queue_action(Transaction::new(&table));
    transaction
        .commit(&catalog)
        .await
        .unwrap_or_else(|error| panic!("{table_name}: transaction commit failed: {error}"))
        .metadata()
        .clone()
}

/// Run the Rust evolution for a scenario through a real in-memory-catalog commit: write the exact
/// Java-written base metadata into a temp warehouse, register it, queue the scenario's op-sequence,
/// commit, and return the evolved metadata. The base round-trips through `serde_json` (the registration
/// reads it back), so the registered table carries the exact snapshot history + refs the Java oracle wrote.
async fn evolve(scenario: &str, scenario_dir: &Path) -> TableMetadata {
    let base = load_metadata(scenario_dir, "base.metadata.json");
    register_and_commit(scenario, base, |transaction| {
        apply_scenario_ops(scenario, transaction)
    })
    .await
}

/// Direction 1: assert the Rust-evolved refs map + current-snapshot-id are structurally equal to Java's.
///
/// RISK: a divergent ref snapshot-id (a missed rollback / fast-forward / set-current), a flipped
/// branch-vs-tag kind, a dropped or wrongly-set retention field, a missing created ref, a surviving removed
/// ref, or a wrong current-snapshot-id would silently break round-trip interop with Java-written tables.
/// `SnapshotReference: PartialEq` compares snapshot-id + retention (kind + all three retention fields), so
/// an equal refs map pins all of those at once; the explicit check pins the current-snapshot-id (`main`).
async fn assert_direction_one(scenario: &str, scenario_dir: &Path) -> TableMetadata {
    let rust_evolved = evolve(scenario, scenario_dir).await;
    let java_evolved = load_metadata(scenario_dir, "java_evolved.metadata.json");

    let rust_refs = refs_of(&rust_evolved);
    let java_refs = refs_of(&java_evolved);

    assert_eq!(
        rust_refs, java_refs,
        "{scenario}: Rust-evolved refs must equal Java's (per-ref snapshot-id + branch-vs-tag kind + \
         retention fields)",
    );

    assert_eq!(
        rust_evolved.current_snapshot_id(),
        java_evolved.current_snapshot_id(),
        "{scenario}: current-snapshot-id (main) must match Java",
    );

    rust_evolved
}

/// Run Direction 1 for every scenario, and — only when `ICEBERG_INTEROP_GEN` is set — write the
/// Rust-evolved metadata to `rust_evolved.metadata.json` for the Java `verify` step (Direction 2).
#[tokio::test]
async fn test_manage_snapshots_interop_all_scenarios() {
    let root = fixtures_root();
    let generate = std::env::var("ICEBERG_INTEROP_GEN").is_ok();

    for scenario in SCENARIOS {
        let scenario_dir = root.join(scenario);
        assert!(
            scenario_dir.is_dir(),
            "missing interop fixture dir for {scenario}: {} (run dev/java-interop generate)",
            scenario_dir.display(),
        );

        let rust_evolved = assert_direction_one(scenario, &scenario_dir).await;

        if generate {
            let json = serde_json::to_string_pretty(&rust_evolved)
                .unwrap_or_else(|error| panic!("{scenario}: serialize rust_evolved: {error}"));
            let out_path = scenario_dir.join("rust_evolved.metadata.json");
            fs::write(&out_path, json)
                .unwrap_or_else(|error| panic!("write {}: {error}", out_path.display()));
        }
    }
}

/// Pin the `rollback_to_time` strict-`<` ancestor selection explicitly. A timestamp ONE millisecond newer
/// than ROOT's own timestamp must resolve to ROOT (the newest ancestor strictly older than it), NOT
/// CURRENT — proving the resolver picks the right ancestor by timestamp and that the boundary is strict.
/// This is a stronger, scenario-specific assertion than the whole-refs equality: it names the exact
/// snapshot `main` must land on.
#[tokio::test]
async fn test_rollback_to_time_resolves_to_root_not_current() {
    let scenario = "rollback_to_time";
    let scenario_dir = fixtures_root().join(scenario);
    let rust_evolved = evolve(scenario, &scenario_dir).await;

    assert_eq!(
        rust_evolved.current_snapshot_id(),
        Some(ROOT),
        "a timestamp strictly between ROOT and CURRENT must roll main back to ROOT, not keep CURRENT",
    );
    let main = refs_of(&rust_evolved)
        .remove(MAIN_BRANCH)
        .expect("main ref present after rollback-to-time");
    assert_eq!(main.snapshot_id, ROOT);
    assert!(main.is_branch(), "main must stay a branch");

    // Strict-`<` BOUNDARY pin (catches a `<` -> `<=` regression in `find_latest_ancestor_older_than`).
    // The fixture scenario above uses `ROOT_TS_MS + 1`, which sits far below CURRENT's timestamp, so it
    // would resolve to ROOT under EITHER `<` or `<=` and therefore does NOT pin the boundary on its own.
    // Rolling to a timestamp EXACTLY equal to CURRENT's own timestamp is the boundary: under strict `<`,
    // CURRENT is not older than itself, so `main` must fall back to ROOT; under a buggy `<=`, CURRENT would
    // qualify and the rollback would be a no-op (main stays CURRENT). Drive it against the SAME Java-written
    // base so the boundary is pinned by the interop fixture, matching Java
    // `SetSnapshotOperation.findLatestAncestorOlderThan` (`timestampMillis() < timestampMillis`).
    let base = load_metadata(&scenario_dir, "base.metadata.json");
    let boundary_evolved = register_and_commit("rollback_to_time_boundary", base, |transaction| {
        let action = transaction
            .manage_snapshots()
            .rollback_to_time(CURRENT_TS_MS);
        action
            .apply(transaction)
            .expect("queue rollback_to_time(CURRENT_TS_MS)")
    })
    .await;
    assert_eq!(
        boundary_evolved.current_snapshot_id(),
        Some(ROOT),
        "rolling to a timestamp EQUAL to CURRENT's timestamp must fall back to ROOT (strict `<`), \
         not keep CURRENT (which a `<=` regression would do)",
    );
}

/// Pin the branch-vs-tag retention distinction explicitly: branch-only fields (min_snapshots_to_keep +
/// max_snapshot_age_ms) must land on the `dev` BRANCH, while max_ref_age_ms must land on the `stable` TAG
/// (the only retention field a tag accepts). A regression that routed a branch-only field onto the tag —
/// or dropped the tag's max_ref_age_ms — would not be caught by a snapshot-id-only check.
#[tokio::test]
async fn test_retention_lands_branch_fields_on_branch_and_ref_age_on_tag() {
    let scenario = "retention";
    let scenario_dir = fixtures_root().join(scenario);
    let rust_evolved = evolve(scenario, &scenario_dir).await;
    let refs = refs_of(&rust_evolved);

    match &refs.get("dev").expect("dev branch present").retention {
        SnapshotRetention::Branch {
            min_snapshots_to_keep,
            max_snapshot_age_ms,
            max_ref_age_ms,
        } => {
            assert_eq!(*min_snapshots_to_keep, Some(5), "dev min_snapshots_to_keep");
            assert_eq!(
                *max_snapshot_age_ms,
                Some(86400000),
                "dev max_snapshot_age_ms",
            );
            assert_eq!(
                *max_ref_age_ms, None,
                "dev max_ref_age_ms must stay unset (not touched by this scenario)",
            );
        }
        SnapshotRetention::Tag { .. } => panic!("dev must remain a branch"),
    }

    match &refs.get("stable").expect("stable tag present").retention {
        SnapshotRetention::Tag { max_ref_age_ms } => {
            assert_eq!(
                *max_ref_age_ms,
                Some(604800000),
                "stable (a tag) max_ref_age_ms",
            );
        }
        SnapshotRetention::Branch { .. } => panic!("stable must remain a tag"),
    }
}

/// Pin ref removal + branch rename explicitly: removing the `stable` tag must drop it entirely, and
/// renaming `dev` → `feature` must move the ref under the new name (preserving snapshot-id + kind) while
/// the old `dev` name disappears. `main` is untouched. A rename that left `dev` behind, or a removal that
/// kept `stable`, would corrupt round-trip interop and is not implied by the current-snapshot-id check.
#[tokio::test]
async fn test_remove_and_rename_drops_tag_and_moves_branch() {
    let scenario = "remove_and_rename";
    let scenario_dir = fixtures_root().join(scenario);
    let rust_evolved = evolve(scenario, &scenario_dir).await;
    let refs = refs_of(&rust_evolved);

    assert!(
        !refs.contains_key("stable"),
        "the removed `stable` tag must be gone",
    );
    assert!(
        !refs.contains_key("dev"),
        "the old `dev` name must be gone after the rename",
    );
    let feature = refs
        .get("feature")
        .expect("renamed `feature` branch present");
    assert_eq!(
        feature.snapshot_id, CURRENT,
        "the renamed branch keeps `dev`'s original snapshot id",
    );
    assert!(feature.is_branch(), "the renamed ref stays a branch");
    assert!(refs.contains_key(MAIN_BRANCH), "main is untouched");
}
