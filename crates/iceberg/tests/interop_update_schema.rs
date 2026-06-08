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

//! Java interop tests for the `UpdateSchema` transaction action.
//!
//! This is the byte-/field-id-level parity evidence that flips the GAP_MATRIX `UpdateSchema` row to ✅.
//! It exercises BOTH directions against the Java `iceberg-core` reference oracle in
//! `dev/java-interop/`:
//!
//! - **Direction 1 (Rust reproduces Java's evolution).** For each scenario, load the Java-written
//!   `base.metadata.json`, apply the SAME UpdateSchema op-sequence via the public transaction API
//!   ([`Transaction::update_schema`] + [`ApplyTransactionAction::apply`] + [`Transaction::commit`]
//!   against an in-memory catalog), and assert the Rust-evolved current schema is **structurally equal**
//!   to the Java-evolved current schema parsed from `java_evolved.metadata.json` — recursive
//!   field-id / name / type / required / doc / default equality (via `StructType: PartialEq`, which
//!   compares all of those), plus identifier-field ids, current-schema-id, and last-column-id. This runs
//!   in the normal offline suite — no Java / Docker needed; it reads the committed fixtures.
//! - **Direction 2 (Java reads what Rust writes).** When the `ICEBERG_INTEROP_GEN` env var is set, the
//!   same test writes the Rust-evolved `TableMetadata` (serialized via `serde_json`) to
//!   `rust_evolved.metadata.json` in each scenario dir for the Java oracle's `verify` step. A normal
//!   `cargo test` run does NOT write files (the env var is unset); only `dev/java-interop/run.sh` sets it.
//!
//! Comparison is by PARSING both metadata files into the Rust model and asserting structural equality —
//! NOT by comparing raw JSON bytes (Jackson and serde_json differ in key order / whitespace). Logical
//! table identity *including field ids* is the contract, not byte identity.
//!
//! The evolution is driven through a real [`MemoryCatalog`] commit (not a raw `TableMetadataBuilder`
//! call) because the `TransactionAction::commit` seam is crate-private; the catalog path is the public
//! API and additionally exercises the optimistic-concurrency requirement checks end to end. The catalog
//! is backed by a `LocalFsStorageFactory` over a temp dir so the exact Java-written base metadata (with
//! its precise field ids) is the registered table — not a freshly created one.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use iceberg::io::{FileIOBuilder, LocalFsStorageFactory};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    FormatVersion, Literal, MapType, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder,
    StructType, TableMetadata, TableMetadataBuilder, Type,
};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};

/// Root of the committed interop fixtures, relative to the `iceberg` crate manifest.
fn fixtures_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/interop/update_schema")
}

/// The full set of scenarios, in the same order as the Java oracle.
const SCENARIOS: &[&str] = &[
    "add_top_level_columns",
    "add_nested_struct_and_map",
    "rename_and_move",
    "update_type_promotion",
    "make_optional_and_delete",
    "set_identifier_fields",
    "add_required_with_default_and_update_default",
];

/// Apply a scenario's op-sequence onto a [`Transaction`] via the schema-update action. These MUST
/// mirror, op for op, the Java oracle's `scenarios()` in
/// `dev/java-interop/.../InteropOracle.java`. Returns the transaction with the action queued.
fn apply_scenario_ops(scenario: &str, transaction: Transaction) -> Transaction {
    let action = transaction.update_schema();
    let action = match scenario {
        // Append two optional and one required-with-default top-level columns (V3 base).
        "add_top_level_columns" => action
            .add_column("count", Type::Primitive(PrimitiveType::Int))
            .add_column_to(
                None,
                "note",
                Type::Primitive(PrimitiveType::String),
                Some("a free-text note"),
            )
            .add_required_column_with_default(
                "category",
                Type::Primitive(PrimitiveType::String),
                Literal::string("uncategorized"),
            ),

        // THE level-order fresh-field-id case: add a map<struct,struct> to a 1-column schema. The
        // incoming ids are scrambled to prove they are reassigned level-order (key=3, value=4, key
        // struct 5..8, value struct 9..10).
        "add_nested_struct_and_map" => action.add_column_to(
            None,
            "locations",
            Type::Map(MapType::new(
                NestedField::map_key_element(
                    11,
                    Type::Struct(StructType::new(vec![
                        NestedField::required(
                            20,
                            "address",
                            Type::Primitive(PrimitiveType::String),
                        )
                        .into(),
                        NestedField::required(21, "city", Type::Primitive(PrimitiveType::String))
                            .into(),
                        NestedField::required(22, "state", Type::Primitive(PrimitiveType::String))
                            .into(),
                        NestedField::required(23, "zip", Type::Primitive(PrimitiveType::Int))
                            .into(),
                    ])),
                )
                .into(),
                NestedField::map_value_element(
                    12,
                    Type::Struct(StructType::new(vec![
                        NestedField::required(30, "lat", Type::Primitive(PrimitiveType::Int))
                            .into(),
                        NestedField::optional(31, "long", Type::Primitive(PrimitiveType::Int))
                            .into(),
                    ])),
                    false,
                )
                .into(),
            )),
            None,
        ),

        // Rename a column and reorder. Move targets resolve by ORIGINAL name (`email`), mirroring Java.
        "rename_and_move" => action
            .rename_column("email", "email_address")
            .move_first("email")
            .move_after("id", "first_name"),

        // int->long, float->double, decimal(9,2)->decimal(18,2) widen.
        "update_type_promotion" => action
            .update_column("id", PrimitiveType::Long)
            .update_column("measure", PrimitiveType::Double)
            .update_column("amount", PrimitiveType::Decimal {
                precision: 18,
                scale: 2,
            }),

        // Relax a required column to optional, and delete another.
        "make_optional_and_delete" => action.make_column_optional("name").delete_column("legacy"),

        // Promote two required fields to the identifier-field set.
        "set_identifier_fields" => action.set_identifier_fields(["id", "tenant"]),

        // Add a required column WITH a default (legal without allow_incompatible_changes), then change
        // its write default via update_column_default (sets only the write default) (V3 base).
        "add_required_with_default_and_update_default" => action
            .add_required_column_with_default(
                "status",
                Type::Primitive(PrimitiveType::String),
                Literal::string("active"),
            )
            .update_column_default("status", Literal::string("pending")),

        other => panic!("unknown interop scenario: {other}"),
    };
    action
        .apply(transaction)
        .expect("queue UpdateSchemaAction onto the transaction")
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
/// transaction over it, commit, and return the evolved metadata. Shared by every producer below so they
/// all drive the SAME public-API path: `MemoryCatalog::register_table` → `Transaction` →
/// `ApplyTransactionAction` → `Transaction::commit` (which exercises the optimistic-concurrency
/// `TableRequirement` checks too). `table_name` only needs to be unique within the run.
async fn register_and_commit(
    table_name: &str,
    base: TableMetadata,
    queue_action: impl FnOnce(Transaction) -> Transaction,
) -> TableMetadata {
    register_and_commit_result(table_name, base, queue_action)
        .await
        .unwrap_or_else(|error| panic!("{table_name}: transaction commit failed: {error}"))
        .metadata()
        .clone()
}

/// Like [`register_and_commit`] but returns the commit `Result` instead of panicking on failure, so a
/// test can assert the commit is REJECTED (e.g. the V2 initial-default guard). The catalog setup and
/// the registration of the exact base metadata are identical; only the final commit's error handling
/// differs.
async fn register_and_commit_result(
    table_name: &str,
    base: TableMetadata,
    queue_action: impl FnOnce(Transaction) -> Transaction,
) -> iceberg::Result<iceberg::table::Table> {
    // A temp warehouse backed by the local filesystem so the catalog can read/write metadata files.
    let warehouse = tempfile::tempdir().expect("create temp warehouse dir");
    let warehouse_path = warehouse.path().to_str().expect("utf-8 warehouse path");

    // Write the exact base metadata into the warehouse via a local-fs FileIO, then register it.
    let file_io = FileIOBuilder::new(Arc::new(LocalFsStorageFactory)).build();
    // The metadata file must live under a `metadata/` subdirectory of the table location AND be named
    // `<version>-<uuid>.metadata.json`, or the catalog's commit (which parses the version from the
    // filename to derive the next metadata path) rejects it.
    let base_location = format!(
        "{warehouse_path}/{table_name}/metadata/00000-00000000-0000-0000-0000-000000000000.metadata.json"
    );
    base.write_to(&file_io, &base_location)
        .await
        .expect("write base metadata to warehouse");

    // The catalog must use the local filesystem (its default storage is in-memory and would not see
    // the base metadata file we just wrote), so inject a `LocalFsStorageFactory`.
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
    transaction.commit(&catalog).await
}

/// Run the Rust evolution for a scenario through a real in-memory-catalog commit: write the exact
/// Java-written base metadata into a temp warehouse, register it as a table, queue the scenario's
/// op-sequence, commit, and return the evolved metadata. This is the producer side shared by both
/// directions. The base metadata round-trips through `serde_json` (the registration reads it back), so
/// the registered table carries the exact field ids the Java oracle wrote.
async fn evolve(scenario: &str, scenario_dir: &Path) -> TableMetadata {
    let base = load_metadata(scenario_dir, "base.metadata.json");
    register_and_commit(scenario, base, |transaction| {
        apply_scenario_ops(scenario, transaction)
    })
    .await
}

/// Direction 1: assert the Rust-evolved current schema is structurally equal to the Java-evolved one.
///
/// RISK: a divergent field id, a wrong nested-id assignment order, a dropped/added column, a missed
/// promotion, a lost default, or a mismatched identifier-field set would silently break round-trip
/// interop with Java-written tables. The `StructType: PartialEq` compares field id + name + type +
/// required + doc + default recursively, so an equal `as_struct()` pins all of those at once; the
/// explicit checks pin identifier ids, current-schema-id, and last-column-id.
async fn assert_direction_one(scenario: &str, scenario_dir: &Path) -> TableMetadata {
    let rust_evolved = evolve(scenario, scenario_dir).await;
    let java_evolved = load_metadata(scenario_dir, "java_evolved.metadata.json");

    let rust_schema: &Schema = rust_evolved.current_schema();
    let java_schema: &Schema = java_evolved.current_schema();

    assert_eq!(
        rust_schema.as_struct(),
        java_schema.as_struct(),
        "{scenario}: Rust-evolved struct must equal Java-evolved struct (field id / name / type / \
         required / doc / default, recursively)",
    );

    let mut rust_identifier_ids: Vec<i32> = rust_schema.identifier_field_ids().collect();
    let mut java_identifier_ids: Vec<i32> = java_schema.identifier_field_ids().collect();
    rust_identifier_ids.sort_unstable();
    java_identifier_ids.sort_unstable();
    assert_eq!(
        rust_identifier_ids, java_identifier_ids,
        "{scenario}: identifier-field ids must match Java",
    );

    assert_eq!(
        rust_evolved.current_schema_id(),
        java_evolved.current_schema_id(),
        "{scenario}: current-schema-id must match Java",
    );
    assert_eq!(
        rust_evolved.last_column_id(),
        java_evolved.last_column_id(),
        "{scenario}: last-column-id must match Java",
    );

    rust_evolved
}

/// Run Direction 1 for every scenario, and — only when `ICEBERG_INTEROP_GEN` is set — write the
/// Rust-evolved metadata to `rust_evolved.metadata.json` for the Java `verify` step (Direction 2).
#[tokio::test]
async fn test_update_schema_interop_all_scenarios() {
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

/// Pin the level-order nested-field-id assignment explicitly (the Increment-3 blocker case). This is a
/// stronger, scenario-specific assertion than the whole-schema struct equality: a `last_column_id`-only
/// check would NOT catch a depth-first walk that yields the same highest id but different interior ids.
#[tokio::test]
async fn test_add_nested_struct_and_map_assigns_level_order_ids() {
    let scenario = "add_nested_struct_and_map";
    let scenario_dir = fixtures_root().join(scenario);
    let rust_evolved = evolve(scenario, &scenario_dir).await;
    let schema = rust_evolved.current_schema();

    // map<struct,struct> added to `id` (id=1): key-id=3, value-id=4, key struct 5..8, value struct 9..10.
    let locations = schema.field_by_name("locations").expect("locations field");
    assert_eq!(
        locations.id, 2,
        "the map column itself gets last_column_id+1 = 2"
    );
    let Type::Map(map) = locations.field_type.as_ref() else {
        panic!("locations must be a map, got {:?}", locations.field_type);
    };
    assert_eq!(
        map.key_field.id, 3,
        "map key id is assigned before the value id (level-order)"
    );
    assert_eq!(
        map.value_field.id, 4,
        "map value id is assigned right after the key id (level-order)"
    );

    let Type::Struct(key_struct) = map.key_field.field_type.as_ref() else {
        panic!("map key must be a struct");
    };
    let key_ids: Vec<i32> = key_struct.fields().iter().map(|f| f.id).collect();
    assert_eq!(
        key_ids,
        vec![5, 6, 7, 8],
        "key struct fields get ids 5..8 (assigned AFTER both key+value ids, level-order)",
    );

    let Type::Struct(value_struct) = map.value_field.field_type.as_ref() else {
        panic!("map value must be a struct");
    };
    let value_ids: Vec<i32> = value_struct.fields().iter().map(|f| f.id).collect();
    assert_eq!(
        value_ids,
        vec![9, 10],
        "value struct fields get ids 9..10 (assigned after the key struct's, level-order)",
    );

    assert_eq!(
        rust_evolved.last_column_id(),
        10,
        "last-column-id is 10 after the nested add"
    );
}

/// Pin that column defaults survive the Java round-trip exactly (initial vs write default divergence).
/// RISK: `update_column_default` must change ONLY the write default; if it also rewrote the initial
/// default, the Java-vs-Rust comparison would still pass (both would change) but the Iceberg semantic
/// (existing-row backfill stays fixed at add time) would be violated. The fixture pins init=active,
/// write=pending, so this catches a regression that conflates the two defaults.
#[tokio::test]
async fn test_add_required_with_default_preserves_initial_and_updates_write_default() {
    let scenario = "add_required_with_default_and_update_default";
    let scenario_dir = fixtures_root().join(scenario);
    let rust_evolved = evolve(scenario, &scenario_dir).await;
    let schema = rust_evolved.current_schema();

    let status = schema.field_by_name("status").expect("status field");
    assert!(status.required, "status was added as a required column");
    assert_eq!(
        status.initial_default,
        Some(Literal::string("active")),
        "initial default (existing-row backfill) is fixed at add time and must stay `active`",
    );
    assert_eq!(
        status.write_default,
        Some(Literal::string("pending")),
        "update_column_default must change ONLY the write default to `pending`",
    );
}

/// PARITY GUARD (was a known divergence — now closed and pinned the other way).
///
/// Java `Schema.checkCompatibility(schema, formatVersion)` (`api/.../Schema.java:604`, called on every
/// add-schema build path via `TableMetadata$Builder.addSchemaInternal`) REJECTS a non-null
/// `initialDefault` when `formatVersion < 3` ("...non-null default (...) is not supported until v3";
/// `DEFAULT_VALUES_MIN_FORMAT_VERSION = 3`). The Rust side now mirrors this: `Schema::check_compatibility`
/// (in `spec/schema/mod.rs`) is wired into `TableMetadataBuilder::add_schema`, so a defaulted add on a
/// **V2** table is rejected when the emitted schema reaches the metadata builder — exactly where Java
/// rejects it.
///
/// This test previously pinned the GAP (Rust accepted the V2 default and emitted Java-unreadable
/// metadata). With the guard landed, it is FLIPPED to assert the rejection: the V2 commit must FAIL with
/// the not-supported-until-v3 message. The two default-bearing interop scenarios still use a V3 base
/// (matching Java), so the round-trip is unaffected by this guard.
#[tokio::test]
async fn test_v2_default_is_rejected_by_v3_guard() {
    // A from-scratch V2 base table with a single required `id` column (no defaults).
    let base_schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()
        .expect("build v2 base schema");
    let base = TableMetadataBuilder::new(
        base_schema,
        PartitionSpec::unpartition_spec(),
        SortOrder::unsorted_order(),
        "s3://interop-bucket/v2_default_divergence".to_string(),
        FormatVersion::V2,
        HashMap::new(),
    )
    .expect("build v2 base metadata builder")
    .build()
    .expect("build v2 base metadata")
    .metadata;
    assert_eq!(
        base.format_version(),
        FormatVersion::V2,
        "base must be V2 for the guard to bite",
    );

    // Add a required column WITH a non-null initial default on the V2 table. Java rejects this at
    // `TableMetadata` build time; the Rust V3 guard now rejects it too, at commit time.
    let result = register_and_commit_result("v2_default_divergence", base, |transaction| {
        let action = transaction
            .update_schema()
            .add_required_column_with_default(
                "status",
                Type::Primitive(PrimitiveType::String),
                Literal::string("active"),
            );
        action
            .apply(transaction)
            .expect("queue the defaulted-add onto the transaction")
    })
    .await;

    let error =
        result.expect_err("a V2 defaulted add must be rejected by the V3 initial-default guard");
    let message = error.to_string();
    assert!(
        message.contains("is not supported until v3"),
        "rejection message must mirror Java's not-supported-until-v3, got: {message}",
    );
    assert!(
        message.contains("status"),
        "rejection message must name the offending column `status`, got: {message}",
    );
}
