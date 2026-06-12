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

//! View-metadata interop harness (increment I2) — bidirectional proof that view metadata
//! written by Rust is readable by Java's `ViewMetadataParser.fromJson`, and vice versa.
//!
//! # Fixture shape
//!
//! A single-schema view `{1 id long required, 2 name string required}` with two versions:
//! - **Version 1:** SQL `"SELECT id, name FROM events WHERE id > 0"`, dialect `"spark"`.
//! - **Version 2 (current):** SQL `"SELECT id, name FROM events WHERE id > 100"`, dialect `"spark"`.
//!
//! The two SQL strings are deliberately different so `reuseOrCreateNewViewVersionId` does NOT
//! deduplicate them — the builder assigns version-id 2, giving `version_count=2` and a
//! 2-entry `version-log`.
//!
//! # Direction 1 (GEN: Rust writes, Java judges)
//!
//! [`test_view_gen`] creates a view via `MemoryCatalog` + `ReplaceViewVersionAction`, then
//! writes:
//! - `rust_view_metadata.json` — the on-disk JSON from `ViewMetadata::write_to`.
//! - `rust_view_expected.json` — the expected field values (uuid, location,
//!   current-version-id, version count, version-log count, per-version sql/dialect).
//!
//! The run script passes these to Java's `verify-interop-view` which reads the metadata
//! via the PRODUCTION `ViewMetadataParser.fromJson` and checks every field integer/string-exact.
//!
//! # Direction 2 (Rust reads Java-written metadata)
//!
//! [`test_view_d2_rust_reads_java`] reads `java_view_metadata.json` (Java `ViewMetadataParser.toJson`)
//! through the PRODUCTION Rust `ViewMetadata::read_from` path and verifies all fields against
//! `java_view_expected.json`.
//!
//! # Tolerance control
//!
//! [`test_view_tolerance_controls`] demonstrates the cosmetic divergence between Java and Rust:
//! - Java writes `view-uuid` first; Rust writes `format-version` first — both sides parse
//!   either order (Jackson and serde_json are both key-order-insensitive).
//! - Java omits `"properties"` when empty; Rust always emits `"properties":{}` — both sides
//!   handle the absent/empty case identically.
//!
//! These are pinned as **tolerated cosmetic differences**.  Byte-exact round-trip is a
//! **next-wave item**.
//!
//! # Env gate
//!
//! Tests are NO-OPs (runtime early-return) unless the relevant env var is set non-empty.
//! - `ICEBERG_INTEROP_VIEW_GEN_DIR` — GEN path (Direction 1, Rust writes).
//! - `ICEBERG_INTEROP_VIEW_DIR` — compare path (Direction 2, Rust reads Java's file).

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use iceberg::io::{FileIO, LocalFsStorageFactory};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    NestedField, PrimitiveType, Schema, SqlViewRepresentation, Type, ViewRepresentation,
    ViewRepresentations,
};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, ViewCreation};
use serde_json::{Value as JsonValue, json};

// ===========================================================================================
// Fixture constants — agreed by both sides (anti-circular).
// ===========================================================================================

/// SQL for the first version (same as `ViewOracle.SQL_V1`).
const SQL_V1: &str = "SELECT id, name FROM events WHERE id > 0";
/// Dialect for the first version.
const DIALECT_V1: &str = "spark";
/// SQL for the second version (same as `ViewOracle.SQL_V2`) — MUST differ from SQL_V1.
const SQL_V2: &str = "SELECT id, name FROM events WHERE id > 100";
/// Dialect for the second version.
const DIALECT_V2: &str = "spark";
/// Expected current-version-id after create + replace.
const EXPECTED_CURRENT_VERSION_ID: i32 = 2;
/// Expected version count after create + replace.
const EXPECTED_VERSION_COUNT: usize = 2;
/// Expected version-log length after create + replace.
const EXPECTED_VERSION_LOG_COUNT: usize = 2;

// ===========================================================================================
// Env-gate helpers.
// ===========================================================================================

fn gen_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_VIEW_GEN_DIR")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn compare_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_VIEW_DIR")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

// ===========================================================================================
// Fixture schema — `{1 id long required, 2 name string required}`.
// ===========================================================================================

fn fixture_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("build fixture schema")
}

// ===========================================================================================
// Helper: build a single-representation SQL ViewRepresentations.
// ===========================================================================================

fn sql_repr(sql: &str, dialect: &str) -> ViewRepresentations {
    ViewRepresentations::new(vec![ViewRepresentation::Sql(SqlViewRepresentation {
        sql: sql.to_string(),
        dialect: dialect.to_string(),
    })])
}

// ===========================================================================================
// Helper: read an expected JSON file.
// ===========================================================================================

fn load_expected(path: &Path) -> JsonValue {
    let raw =
        fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    serde_json::from_str(&raw).unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
}

// ===========================================================================================
// Direction 1 — GEN: Rust creates view + ReplaceViewVersionAction.
// ===========================================================================================

/// GEN test: create a two-version view via `MemoryCatalog` and `ReplaceViewVersionAction`,
/// write `rust_view_metadata.json` + `rust_view_expected.json`.
///
/// The fixture has two versions:
/// - **Version 1:** SQL_V1 / DIALECT_V1 — created via `catalog.create_view`.
/// - **Version 2 (current):** SQL_V2 / DIALECT_V2 — committed via `ReplaceViewVersionAction`.
///   Because SQL_V2 ≠ SQL_V1, `reuseOrCreateNewViewVersionId` does NOT dedup and assigns id 2.
///
/// The run script passes `rust_view_metadata.json` to Java's `verify-interop-view`, which
/// reads it via the PRODUCTION `ViewMetadataParser.fromJson` and checks every field.
#[tokio::test]
async fn test_view_gen() {
    let Some(gen_dir) = gen_dir() else {
        println!(
            "skipping interop_view GEN — set ICEBERG_INTEROP_VIEW_GEN_DIR \
             (run dev/java-interop/run-interop-view.sh)"
        );
        return;
    };

    fs::create_dir_all(&gen_dir).expect("create gen dir");

    let view_location = gen_dir.join("rust_view").to_string_lossy().to_string();

    // Build a local-filesystem-backed MemoryCatalog in the gen dir.
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "interop_view_gen",
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

    // Step 1: create with version 1 (SQL_V1 / DIALECT_V1).
    let view = catalog
        .create_view(
            &namespace,
            ViewCreation::builder()
                .name("rust_view".to_string())
                .location(view_location.clone())
                .schema(fixture_schema())
                .default_namespace(namespace.clone())
                .representations(sql_repr(SQL_V1, DIALECT_V1))
                .build(),
        )
        .await
        .expect("create view");

    assert_eq!(
        view.metadata().current_version_id(),
        1,
        "after create: current-version-id must be 1"
    );
    println!(
        "interop_view GEN: view created (current-version-id={} uuid={})",
        view.metadata().current_version_id(),
        view.metadata().uuid()
    );

    // Step 2: replace with version 2 (SQL_V2 / DIALECT_V2) — DIFFERENT SQL forces a new id.
    let replace_commit = view
        .replace_version()
        .with_query(DIALECT_V2, SQL_V2)
        .with_schema(fixture_schema())
        .with_default_namespace(namespace.clone())
        .to_commit()
        .expect("build replace commit");

    let updated_view = catalog
        .update_view(replace_commit)
        .await
        .expect("update view");

    // Post-replace assertions.
    assert_eq!(
        updated_view.metadata().current_version_id(),
        EXPECTED_CURRENT_VERSION_ID,
        "after replace: current-version-id must be {EXPECTED_CURRENT_VERSION_ID}"
    );
    assert_eq!(
        updated_view.metadata().versions().count(),
        EXPECTED_VERSION_COUNT,
        "must have {EXPECTED_VERSION_COUNT} versions (SQL_V1 ≠ SQL_V2, no dedup)"
    );
    assert_eq!(
        updated_view.metadata().history().len(),
        EXPECTED_VERSION_LOG_COUNT,
        "must have {EXPECTED_VERSION_LOG_COUNT} version-log entries"
    );

    let metadata = updated_view.metadata();

    println!(
        "interop_view GEN: view replaced (current-version-id={} versions={} log={})",
        metadata.current_version_id(),
        metadata.versions().count(),
        metadata.history().len(),
    );

    // Write rust_view_metadata.json via the PRODUCTION ViewMetadata::write_to path.
    let metadata_json_path = gen_dir.join("rust_view_metadata.json");
    let file_io = updated_view.file_io().clone();
    metadata
        .write_to(&file_io, metadata_json_path.to_str().expect("utf8 path"))
        .await
        .expect("write rust_view_metadata.json");
    println!(
        "interop_view GEN: rust_view_metadata.json written at {}",
        metadata_json_path.display()
    );

    // Build the expected JSON — same fields the Java `verifyRustMetadata` checks.
    let view_uuid = metadata.uuid().to_string();
    let location = metadata.location().to_string();
    let current_version_id = metadata.current_version_id();
    let version_count = metadata.versions().count() as u64;
    let version_log_count = metadata.history().len() as u64;

    // Per-version: collect sql/dialect from each version's first representation.
    let mut versions_json: Vec<JsonValue> = Vec::new();
    for version in metadata.versions() {
        let (sql, dialect) = first_sql_repr(version.representations());
        versions_json.push(json!({
            "version_id": version.version_id(),
            "schema_id": version.schema_id(),
            "sql": sql,
            "dialect": dialect,
        }));
    }

    // Version-log ids in creation order.
    let version_log_ids: Vec<i32> = metadata
        .history()
        .iter()
        .map(|entry| entry.version_id())
        .collect();

    // Version-log entries with their timestamps — pins that each version-log entry's
    // timestamp-ms equals the matching version's timestamp-ms (cross-field invariant), so a
    // tampered version-log timestamp is caught field-by-field.
    let version_log_json: Vec<JsonValue> = metadata
        .history()
        .iter()
        .map(|entry| {
            json!({
                "version_id": entry.version_id(),
                "timestamp_ms": entry.timestamp_ms(),
            })
        })
        .collect();

    let schema_field_names: Vec<String> = metadata
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|field| field.name.clone())
        .collect();

    // Per-field name/type/required — the schema field TYPE and the required flag are
    // load-bearing correctness fields; pin them, not just the field name.
    let schema_fields_json: Vec<JsonValue> = metadata
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|field| {
            json!({
                "name": field.name.clone(),
                "type": field.field_type.to_string(),
                "required": field.required,
            })
        })
        .collect();

    // Per-version timestamp-ms — pins that a non-current version's timestamp is not silently
    // mutated. Captured as the EXACT value the GEN run produced (the Rust artifact is the
    // source of truth; Java reads these exact values back).
    let mut version_timestamps_json: Vec<JsonValue> = Vec::new();
    for version in metadata.versions() {
        version_timestamps_json.push(json!({
            "version_id": version.version_id(),
            "timestamp_ms": version.timestamp_ms(),
        }));
    }

    let expected = json!({
        "format_version": 1,
        "view_uuid": view_uuid,
        "location": location,
        "current_version_id": current_version_id,
        "version_count": version_count,
        "version_log_count": version_log_count,
        "schema_field_names": schema_field_names,
        "schema_fields": schema_fields_json,
        "versions": versions_json,
        "version_timestamps": version_timestamps_json,
        "version_log_ids": version_log_ids,
        "version_log": version_log_json,
    });

    let expected_path = gen_dir.join("rust_view_expected.json");
    fs::write(
        &expected_path,
        serde_json::to_string_pretty(&expected).expect("serialize expected"),
    )
    .expect("write rust_view_expected.json");
    println!(
        "interop_view GEN: rust_view_expected.json written at {}",
        expected_path.display()
    );

    // Verify version-1 sql/dialect.
    let version_1 = metadata.version_by_id(1).expect("version 1 must exist");
    let (sql1, dialect1) = first_sql_repr(version_1.representations());
    assert_eq!(sql1, SQL_V1, "version 1 sql must be SQL_V1");
    assert_eq!(dialect1, DIALECT_V1, "version 1 dialect must be DIALECT_V1");

    // Verify version-2 sql/dialect.
    let version_2 = metadata
        .version_by_id(EXPECTED_CURRENT_VERSION_ID)
        .expect("version 2 must exist");
    let (sql2, dialect2) = first_sql_repr(version_2.representations());
    assert_eq!(sql2, SQL_V2, "version 2 sql must be SQL_V2");
    assert_eq!(dialect2, DIALECT_V2, "version 2 dialect must be DIALECT_V2");

    println!(
        "interop_view GEN: all assertions PASSED — uuid={} \
         current-version-id={} versions={} log={}",
        view_uuid, current_version_id, version_count, version_log_count
    );
}

// ===========================================================================================
// Direction 2 — Rust reads Java-written metadata.
// ===========================================================================================

/// D2 test: read `java_view_metadata.json` (Java `ViewMetadataParser.toJson`) via the PRODUCTION
/// Rust `ViewMetadata::read_from` path and verify all fields against `java_view_expected.json`.
///
/// This proves the Rust production reader can parse a Java-written view metadata document
/// faithfully across the known cosmetic divergence (field order + always-emit-properties).
#[tokio::test]
async fn test_view_d2_rust_reads_java() {
    let Some(dir) = compare_dir() else {
        println!(
            "skipping interop_view D2 — set ICEBERG_INTEROP_VIEW_DIR \
             (run dev/java-interop/run-interop-view.sh)"
        );
        return;
    };

    let metadata_path = dir.join("java_view_metadata.json");
    assert!(
        metadata_path.exists(),
        "D2: missing {}; run Java generate-interop-view-java-to-rust first",
        metadata_path.display()
    );

    let expected_path = dir.join("java_view_expected.json");
    assert!(
        expected_path.exists(),
        "D2: missing {}",
        expected_path.display()
    );

    let expected = load_expected(&expected_path);

    // Read the Java-written metadata via the PRODUCTION Rust path.
    let file_io = FileIO::new_with_fs();
    let metadata = iceberg::spec::ViewMetadata::read_from(
        &file_io,
        metadata_path.to_str().expect("utf8 metadata path"),
    )
    .await
    .expect("ViewMetadata::read_from java_view_metadata.json");

    // 1. format-version
    let expected_format_version = expected["format_version"].as_i64().unwrap_or(1) as i32;
    assert_eq!(
        metadata.format_version() as i32,
        expected_format_version,
        "D2: format-version"
    );

    // 2. view-uuid
    let expected_uuid = expected["view_uuid"]
        .as_str()
        .expect("view_uuid in expected");
    assert_eq!(metadata.uuid().to_string(), expected_uuid, "D2: view-uuid");

    // 3. location
    let expected_location = expected["location"].as_str().expect("location in expected");
    assert_eq!(metadata.location(), expected_location, "D2: location");

    // 4. current-version-id
    let expected_current_version_id = expected["current_version_id"]
        .as_i64()
        .expect("current_version_id") as i32;
    assert_eq!(
        metadata.current_version_id(),
        expected_current_version_id,
        "D2: current-version-id"
    );

    // 5. version count
    let expected_version_count =
        expected["version_count"].as_u64().expect("version_count") as usize;
    assert_eq!(
        metadata.versions().count(),
        expected_version_count,
        "D2: version count"
    );

    // 6. version-log count
    let expected_log_count = expected["version_log_count"]
        .as_u64()
        .expect("version_log_count") as usize;
    assert_eq!(
        metadata.history().len(),
        expected_log_count,
        "D2: version-log count"
    );

    // 7. Schema field names
    let expected_field_names: Vec<&str> = expected["schema_field_names"]
        .as_array()
        .expect("schema_field_names")
        .iter()
        .map(|value| value.as_str().expect("field name string"))
        .collect();
    let actual_field_names: Vec<&str> = metadata
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|field| field.name.as_str())
        .collect();
    assert_eq!(
        actual_field_names, expected_field_names,
        "D2: schema field names"
    );

    // 7b. Schema field TYPE + required flag — load-bearing correctness fields. A view whose
    // `id` field silently changed long→string (or required→optional) is a real divergence; the
    // field name alone does not catch it. Keyed by name for order-insensitivity.
    if let Some(expected_fields) = expected["schema_fields"].as_array() {
        let actual_struct = metadata.current_schema().as_struct();
        for field_expected in expected_fields {
            let expected_name = field_expected["name"].as_str().expect("schema field name");
            let expected_type = field_expected["type"].as_str().expect("schema field type");
            let expected_required = field_expected["required"]
                .as_bool()
                .expect("schema field required");
            let actual_field = actual_struct
                .field_by_name(expected_name)
                .unwrap_or_else(|| panic!("D2: schema field {expected_name} missing"));
            assert_eq!(
                actual_field.field_type.to_string(),
                expected_type,
                "D2: schema field {expected_name} type"
            );
            assert_eq!(
                actual_field.required, expected_required,
                "D2: schema field {expected_name} required"
            );
        }
    }

    // 8. Per-version assertions (keyed by version-id for order-insensitivity).
    let versions_array = expected["versions"].as_array().expect("versions array");
    for version_expected in versions_array {
        let expected_version_id =
            version_expected["version_id"].as_i64().expect("version_id") as i32;
        let expected_schema_id = version_expected["schema_id"].as_i64().expect("schema_id") as i32;
        let expected_sql = version_expected["sql"].as_str().expect("sql");
        let expected_dialect = version_expected["dialect"].as_str().expect("dialect");

        let version = metadata
            .version_by_id(expected_version_id)
            .unwrap_or_else(|| {
                panic!("D2: version-id {expected_version_id} not found in Rust-parsed metadata")
            });

        assert_eq!(
            version.schema_id(),
            expected_schema_id,
            "D2 version[{expected_version_id}]: schema-id"
        );

        let (actual_sql, actual_dialect) = first_sql_repr(version.representations());
        assert_eq!(
            actual_sql, expected_sql,
            "D2 version[{expected_version_id}]: sql"
        );
        assert_eq!(
            actual_dialect, expected_dialect,
            "D2 version[{expected_version_id}]: dialect"
        );

        println!(
            "interop_view D2 version[{expected_version_id}]: schema-id={expected_schema_id} \
             sql=\"{expected_sql}\" dialect=\"{expected_dialect}\" — OK"
        );
    }

    // 9. Version-log entry ids in order.
    let expected_log_ids: Vec<i32> = expected["version_log_ids"]
        .as_array()
        .expect("version_log_ids")
        .iter()
        .map(|value| value.as_i64().expect("log id") as i32)
        .collect();
    let actual_log_ids: Vec<i32> = metadata
        .history()
        .iter()
        .map(|entry| entry.version_id())
        .collect();
    assert_eq!(
        actual_log_ids, expected_log_ids,
        "D2: version-log ids in order"
    );

    // 9b. Version-log entry timestamps in order — pins that a version-log timestamp is not
    // silently mutated. The Java emitter records the exact timestamp-ms it wrote; Rust must read
    // back the same value for each entry, in order.
    if let Some(expected_log) = expected["version_log"].as_array() {
        let actual_log_timestamps: Vec<i64> = metadata
            .history()
            .iter()
            .map(|entry| entry.timestamp_ms())
            .collect();
        let expected_log_timestamps: Vec<i64> = expected_log
            .iter()
            .map(|entry| entry["timestamp_ms"].as_i64().expect("log timestamp_ms"))
            .collect();
        assert_eq!(
            actual_log_timestamps, expected_log_timestamps,
            "D2: version-log timestamps in order"
        );
    }

    // 9c. Per-version timestamp-ms (keyed by version-id) — pins that a NON-current version's
    // timestamp is not silently mutated.
    if let Some(expected_version_timestamps) = expected["version_timestamps"].as_array() {
        for version_timestamp in expected_version_timestamps {
            let expected_version_id = version_timestamp["version_id"]
                .as_i64()
                .expect("version_id") as i32;
            let expected_timestamp = version_timestamp["timestamp_ms"]
                .as_i64()
                .expect("version timestamp_ms");
            let version = metadata
                .version_by_id(expected_version_id)
                .unwrap_or_else(|| panic!("D2: version {expected_version_id} not found"));
            assert_eq!(
                version.timestamp_ms(),
                expected_timestamp,
                "D2 version[{expected_version_id}]: timestamp-ms"
            );
        }
    }

    println!(
        "interop_view D2: PASS — Java-written metadata fully parsed by Rust \
         (uuid={} current-version-id={} versions={} log={})",
        metadata.uuid(),
        metadata.current_version_id(),
        metadata.versions().count(),
        metadata.history().len(),
    );
}

// ===========================================================================================
// Tolerance control — field-order permutation + empty-properties omission.
// ===========================================================================================

/// Tolerance control: pinning that the cosmetic wire-format divergence between Java and Rust is
/// tolerated — NOT a breaking interop issue.
///
/// Two invariants pinned here:
/// 1. **Field-order permutation:** a valid JSON document with top-level fields in Java's order
///    (`view-uuid` first) parses correctly in Rust (`serde_json` is key-order-insensitive).
/// 2. **Empty-properties omission:** a valid JSON document where `"properties"` is absent
///    (Java's behavior when properties is empty) parses correctly in Rust (`properties` is
///    `Option<...>` + `unwrap_or_default`).
///
/// These are the two documented cosmetic differences from the U1 reviewer's bytecode verdict.
/// They are tolerated by both parsers.  Byte-exact round-trip (which would require either side
/// to match the other's field order) remains a NEXT-WAVE item.
#[test]
fn test_view_tolerance_controls() {
    // A minimal valid view-metadata JSON in Java's field order:
    //   view-uuid first, NO "properties" field.
    // If Rust's serde deserializer were order-sensitive or required "properties", this would fail.
    let java_ordered_no_properties = r#"{
        "view-uuid": "550e8400-e29b-41d4-a716-446655440000",
        "format-version": 1,
        "location": "file:///tmp/tolerance-test",
        "schemas": [
            {
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {"id": 1, "name": "id", "required": true, "type": "long"},
                    {"id": 2, "name": "name", "required": true, "type": "string"}
                ]
            }
        ],
        "current-version-id": 1,
        "versions": [
            {
                "version-id": 1,
                "timestamp-ms": 1700000000000,
                "schema-id": 0,
                "summary": {},
                "default-namespace": ["interop"],
                "representations": [
                    {"type": "sql", "sql": "SELECT id FROM t", "dialect": "spark"}
                ]
            }
        ],
        "version-log": [
            {"version-id": 1, "timestamp-ms": 1700000000000}
        ]
    }"#;

    // Parse the Java-ordered doc via Rust's PRODUCTION serde path.
    let metadata: iceberg::spec::ViewMetadata = serde_json::from_str(java_ordered_no_properties)
        .expect(
            "TOLERANCE CONTROL: Java-ordered JSON (view-uuid first, no 'properties') \
             must parse in Rust — field order must NOT matter and absent properties \
             must be treated as empty",
        );

    assert_eq!(
        metadata.uuid().to_string(),
        "550e8400-e29b-41d4-a716-446655440000",
        "tolerance: uuid round-trips correctly"
    );
    assert_eq!(
        metadata.current_version_id(),
        1,
        "tolerance: current-version-id parses correctly"
    );
    // Properties absent in input must be treated as empty (not an error).
    assert!(
        metadata.properties().is_empty(),
        "tolerance: absent 'properties' must parse as empty HashMap"
    );
    // Version-log must have one entry.
    assert_eq!(metadata.history().len(), 1, "tolerance: version-log length");

    println!(
        "interop_view tolerance: PASS — Java-ordered JSON (view-uuid first, no 'properties') \
         parsed correctly; uuid={} current-version-id={} properties=empty versions={} log={}",
        metadata.uuid(),
        metadata.current_version_id(),
        metadata.versions().count(),
        metadata.history().len(),
    );

    // Now verify that Rust's serialized form (format-version first, always-emit-properties:{})
    // round-trips through Rust's own deserializer.
    let rust_serialized =
        serde_json::to_string(&metadata).expect("Rust must serialize its own ViewMetadata");
    let reparsed: iceberg::spec::ViewMetadata = serde_json::from_str(&rust_serialized)
        .expect("Rust-serialized doc must parse back via Rust serde");

    assert_eq!(
        reparsed.uuid(),
        metadata.uuid(),
        "tolerance: round-trip uuid"
    );
    assert_eq!(
        reparsed.current_version_id(),
        metadata.current_version_id(),
        "tolerance: round-trip current-version-id"
    );

    // Confirm that Rust's serialized form contains "properties":{} (the documented behavior).
    // This is NOT required for correctness — it is the pinned DIVERGENCE from Java.
    assert!(
        rust_serialized.contains(r#""properties""#),
        "tolerance: Rust always emits \"properties\" (even when empty) — diverges from Java's \
         omit-when-empty, but Java reads it fine (node.has() guard)"
    );

    println!(
        "interop_view tolerance: PASS — Rust round-trip and always-emit-properties \
         divergence pinned correctly"
    );
}

// ===========================================================================================
// Helper: extract (sql, dialect) from the first SQL representation.
// ===========================================================================================

fn first_sql_repr(representations: &ViewRepresentations) -> (String, String) {
    let Some(repr) = representations.iter().next() else {
        panic!("no SQL representation found in view version")
    };
    let ViewRepresentation::Sql(sql_repr) = repr;
    (sql_repr.sql.clone(), sql_repr.dialect.clone())
}
