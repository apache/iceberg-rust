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

//! Java interop tests for the `UpdatePartitionSpec` transaction action.
//!
//! This is the byte-/field-id-level parity evidence that flips the GAP_MATRIX `UpdatePartitionSpec` row
//! to ✅. It mirrors the proven `interop_update_schema.rs` harness and exercises BOTH directions against
//! the Java `iceberg-core` reference oracle in `dev/java-interop/`:
//!
//! - **Direction 1 (Rust reproduces Java's evolution).** For each scenario, load the Java-written
//!   `base.metadata.json`, apply the SAME `UpdatePartitionSpec` op-sequence via the public transaction API
//!   ([`Transaction::update_partition_spec`] + [`ApplyTransactionAction::apply`] + [`Transaction::commit`]
//!   against an in-memory catalog), and assert the Rust-evolved DEFAULT partition spec is **structurally
//!   equal** to the Java-evolved default spec parsed from `java_evolved.metadata.json` — spec-id + each
//!   field's source-id / field-id / name / transform (via `PartitionField: PartialEq`, which compares all
//!   four), plus the table's `last_partition_id`. This runs in the normal offline suite — no Java / Docker
//!   needed; it reads the committed fixtures.
//! - **Direction 2 (Java reads what Rust writes).** When the `ICEBERG_INTEROP_GEN` env var is set, the
//!   same test writes the Rust-evolved `TableMetadata` (serialized via `serde_json`) to
//!   `rust_evolved.metadata.json` in each scenario dir for the Java oracle's `verify` step. A normal
//!   `cargo test` run does NOT write files (the env var is unset); only `dev/java-interop/run.sh` sets it.
//!
//! Comparison is by PARSING both metadata files into the Rust model and asserting structural equality —
//! NOT by comparing raw JSON bytes (Jackson and serde_json differ in key order / whitespace). Logical
//! partition-spec identity *including field ids* is the contract, not byte identity.
//!
//! The evolution is driven through a real [`MemoryCatalog`] commit (not a raw `TableMetadataBuilder`
//! call) because the `TransactionAction::commit` seam is crate-private; the catalog path is the public
//! API and additionally exercises the optimistic-concurrency requirement checks AND the metadata layer's
//! spec dedup / `LAST_ADDED` resolution / field-id recycling end to end. The catalog is backed by a
//! `LocalFsStorageFactory` over a temp dir so the exact Java-written base metadata (with its precise field
//! ids, including the historical spec used by the recycling scenario) is the registered table.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use iceberg::io::{FileIOBuilder, LocalFsStorageFactory};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{PartitionField, PartitionSpec, TableMetadata, Transform};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};

/// Root of the committed interop fixtures, relative to the `iceberg` crate manifest.
fn fixtures_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/interop/update_partition_spec")
}

/// The full set of scenarios, in the same order as the Java oracle's `PartitionOracle.scenarios()`.
const SCENARIOS: &[&str] = &[
    "add_identity_field",
    "add_transform_fields",
    "remove_field_v2",
    "remove_field_v1_void",
    "rename_field",
    "field_id_recycling",
    "delete_then_readd",
];

/// Apply a scenario's op-sequence onto a [`Transaction`] via the partition-spec-update action. These MUST
/// mirror, op for op, the Java oracle's `PartitionOracle.scenarios()` in
/// `dev/java-interop/.../InteropOracle.java`. Returns the transaction with the action queued.
fn apply_scenario_ops(scenario: &str, transaction: Transaction) -> Transaction {
    let action = transaction.update_partition_spec();
    let action = match scenario {
        // Add identity(category) to an unpartitioned base.
        "add_identity_field" => action.add_field("category"),

        // Add bucket[16](id), truncate[8](category), year(event_ts) to an unpartitioned base. Pins the
        // auto-generated names AND the sequentially-assigned field-ids (1000, 1001, 1002).
        "add_transform_fields" => action
            .add_field_with_transform(None, "id", Transform::Bucket(16))
            .add_field_with_transform(None, "category", Transform::Truncate(8))
            .add_field_with_transform(None, "event_ts", Transform::Year),

        // Remove the only base field on V2 → omitted (empty new spec).
        "remove_field_v2" => action.remove_field("category"),

        // Remove the only base field on V1 → re-added with the void transform (field id preserved).
        "remove_field_v1_void" => action.remove_field("category"),

        // Rename a base partition field; field id preserved.
        "rename_field" => action.rename_field("category", "cat"),

        // Re-add bucket[8](id) with no explicit name → recycles the historical field id AND name
        // ("id_shard") from spec 1, NOT a fresh id or the generated default name "id_bucket_8".
        "field_id_recycling" => action.add_field_with_transform(None, "id", Transform::Bucket(8)),

        // Remove then re-add identity(category) → Java's rewrite/un-delete; result equals the base spec,
        // so the metadata layer dedups back to it (default spec id stays 0, no new spec).
        "delete_then_readd" => action.remove_field("category").add_field("category"),

        other => panic!("unknown interop scenario: {other}"),
    };
    action
        .apply(transaction)
        .expect("queue UpdatePartitionSpecAction onto the transaction")
}

/// Load a `TableMetadata` fixture from a scenario directory.
fn load_metadata(scenario_dir: &Path, file_name: &str) -> TableMetadata {
    let path = scenario_dir.join(file_name);
    let json = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    serde_json::from_str::<TableMetadata>(&json)
        .unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
}

/// Register `base` as a table in a fresh local-fs-backed in-memory catalog, apply `queue_action` to a
/// transaction over it, commit, and return the evolved metadata. Drives the SAME public-API path the
/// schema interop test uses: `MemoryCatalog::register_table` → `Transaction` → `ApplyTransactionAction`
/// → `Transaction::commit` (which exercises the optimistic-concurrency `TableRequirement` checks AND the
/// metadata builder's spec dedup / recycling). `table_name` only needs to be unique within the run.
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
/// reads it back), so the registered table carries the exact partition field ids the Java oracle wrote.
async fn evolve(scenario: &str, scenario_dir: &Path) -> TableMetadata {
    let base = load_metadata(scenario_dir, "base.metadata.json");
    register_and_commit(scenario, base, |transaction| {
        apply_scenario_ops(scenario, transaction)
    })
    .await
}

/// Direction 1: assert the Rust-evolved DEFAULT partition spec is structurally equal to Java's.
///
/// RISK: a divergent field id (a missed recycle, a wrong fresh-id assignment), a wrong auto-generated
/// name, a dropped/kept field across the V1/V2 boundary, a lost rename, or a default-spec-id drift would
/// silently break round-trip interop with Java-written tables. `PartitionField: PartialEq` compares
/// source-id + field-id + name + transform, so an equal field list pins all of those at once; the explicit
/// checks pin the default spec-id and last-partition-id.
async fn assert_direction_one(scenario: &str, scenario_dir: &Path) -> TableMetadata {
    let rust_evolved = evolve(scenario, scenario_dir).await;
    let java_evolved = load_metadata(scenario_dir, "java_evolved.metadata.json");

    let rust_spec: &PartitionSpec = rust_evolved.default_partition_spec();
    let java_spec: &PartitionSpec = java_evolved.default_partition_spec();

    assert_eq!(
        rust_spec.spec_id(),
        java_spec.spec_id(),
        "{scenario}: default spec-id must match Java",
    );

    let rust_fields: &[PartitionField] = rust_spec.fields();
    let java_fields: &[PartitionField] = java_spec.fields();
    assert_eq!(
        rust_fields, java_fields,
        "{scenario}: Rust-evolved default spec fields must equal Java's (source-id / field-id / name / \
         transform, in order)",
    );

    assert_eq!(
        rust_evolved.last_partition_id(),
        java_evolved.last_partition_id(),
        "{scenario}: last-partition-id must match Java",
    );

    rust_evolved
}

/// Run Direction 1 for every scenario, and — only when `ICEBERG_INTEROP_GEN` is set — write the
/// Rust-evolved metadata to `rust_evolved.metadata.json` for the Java `verify` step (Direction 2).
#[tokio::test]
async fn test_update_partition_spec_interop_all_scenarios() {
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

/// Pin the historical field-id AND name recycling explicitly (the Increment-2 review bug). This is a
/// stronger, scenario-specific assertion than the whole-spec equality: a `field_id`-only check would NOT
/// catch the regression where the recycled id is reused but the GENERATED default name (`id_bucket_8`) is
/// emitted instead of the historical name (`id_shard`) — the exact bug the Rust action's
/// `recycle_or_create_partition_field` fixes.
#[tokio::test]
async fn test_field_id_recycling_reuses_historical_id_and_name() {
    let scenario = "field_id_recycling";
    let scenario_dir = fixtures_root().join(scenario);
    let rust_evolved = evolve(scenario, &scenario_dir).await;
    let spec = rust_evolved.default_partition_spec();

    let recycled = spec
        .fields()
        .iter()
        .find(|field| field.source_id == 1 && field.transform == Transform::Bucket(8))
        .expect("recycled bucket[8](id) field in the evolved default spec");

    assert_eq!(
        recycled.field_id, 1001,
        "must recycle the historical field id (1001 — assigned when the historical non-default spec \
         was added), not allocate a fresh one (1002)",
    );
    assert_eq!(
        recycled.name, "id_shard",
        "must reuse the historical field NAME, not the generated default `id_bucket_8`",
    );
    // No fresh id was assigned, so last-partition-id must not advance past the historical 1001.
    assert_eq!(
        rust_evolved.last_partition_id(),
        1001,
        "recycling must not advance last-partition-id",
    );
}

/// Pin the V1 alwaysNull (void) replacement explicitly. On a V1 table, removing a partition field must
/// REPLACE it with the void transform (preserving its field id) rather than omit it — dropping it would
/// shift field ids and corrupt the table for older readers. This is the V1-specific branch of `apply()`;
/// the whole-spec equality already covers it, but a named test makes the V1/V2 divergence explicit.
#[tokio::test]
async fn test_remove_field_v1_replaces_with_void_preserving_id() {
    let scenario = "remove_field_v1_void";
    let scenario_dir = fixtures_root().join(scenario);
    let rust_evolved = evolve(scenario, &scenario_dir).await;
    let spec = rust_evolved.default_partition_spec();

    assert_eq!(
        spec.fields().len(),
        1,
        "V1 remove must KEEP the field slot (void replacement), not drop it",
    );
    let field = &spec.fields()[0];
    assert_eq!(field.name, "category");
    assert_eq!(field.source_id, 2);
    assert_eq!(
        field.field_id, 1000,
        "the void-replaced field must keep its original id 1000",
    );
    assert_eq!(
        field.transform,
        Transform::Void,
        "V1 removed field must become an alwaysNull (void) transform",
    );
}

/// Pin the no-op dedup: remove + re-add the same (source, transform) on V2 yields a spec equal to the
/// base, so the metadata layer must dedup back to the EXISTING default spec id (0) and NOT mint a new
/// spec or advance last-partition-id. Matches Java's `updatePartitionSpec` no-op path.
#[tokio::test]
async fn test_delete_then_readd_dedups_to_existing_default_spec() {
    let scenario = "delete_then_readd";
    let scenario_dir = fixtures_root().join(scenario);
    let rust_evolved = evolve(scenario, &scenario_dir).await;

    assert_eq!(
        rust_evolved.default_partition_spec_id(),
        0,
        "a no-op evolution must dedup to the existing spec id 0, not mint a new one",
    );
    assert_eq!(
        rust_evolved.partition_specs_iter().count(),
        1,
        "no new spec should be added",
    );
    assert_eq!(rust_evolved.last_partition_id(), 1000);
}
