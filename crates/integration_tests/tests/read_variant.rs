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

//! Integration tests for variant type support.
//!
//! These tests require a running Docker environment seeded by `dev/spark/provision.py`.
//! The Spark 4.0 provisioner creates `rest.default.test_variant_column` with a
//! `VARIANT` column containing three rows of JSON data.

use std::sync::Arc;

use arrow_array::StructArray;
use arrow_array::cast::AsArray;
use arrow_schema::DataType;
use futures::TryStreamExt;
use iceberg::spec::Type;
use iceberg::{Catalog, CatalogBuilder, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogBuilder};
use iceberg_integration_tests::get_test_fixture;
use iceberg_storage_opendal::OpenDalStorageFactory;

/// Build a `RestCatalog` against the docker-compose test fixture, wired with
/// the S3 storage factory the seeded tables expect.
async fn rest_catalog() -> RestCatalog {
    let fixture = get_test_fixture();
    RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
            configured_scheme: "s3".to_string(),
            customized_credential_load: None,
        }))
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap()
}

/// Asserts that `dt` is `Struct(metadata: Binary, value: Binary)` — the Parquet
/// physical layout of a variant column.
fn assert_variant_struct(dt: &DataType) {
    let DataType::Struct(fields) = dt else {
        panic!("expected variant to be DataType::Struct, got {dt:?}");
    };
    assert_eq!(
        fields.len(),
        2,
        "variant struct must have exactly 2 sub-fields"
    );
    assert!(
        fields
            .iter()
            .any(|f| f.name() == "metadata" && f.data_type() == &DataType::Binary),
        "variant struct missing 'metadata: Binary' sub-field: {fields:?}"
    );
    assert!(
        fields
            .iter()
            .any(|f| f.name() == "value" && f.data_type() == &DataType::Binary),
        "variant struct missing 'value: Binary' sub-field: {fields:?}"
    );
}

/// Verifies that a table written by Spark with a VARIANT column has its schema
/// parsed into `Type::Variant` by the Rust iceberg implementation.
#[tokio::test]
async fn test_variant_schema_is_parsed() {
    let rest_catalog = rest_catalog().await;

    let table = rest_catalog
        .load_table(&TableIdent::from_strs(["default", "test_variant_column"]).unwrap())
        .await
        .unwrap();

    let schema = table.metadata().current_schema();
    let variant_field = schema
        .field_by_name("v")
        .expect("field 'v' not found in schema");

    assert!(
        matches!(variant_field.field_type.as_ref(), Type::Variant(_)),
        "Expected Type::Variant for field 'v', got {:?}",
        variant_field.field_type,
    );
}

/// Verifies that scanning a table with a VARIANT column produces an Arrow batch
/// where the variant column is represented as `Struct(metadata: Binary, value: Binary)`,
/// matching the Parquet physical layout (§3.3 of the Parquet Variant spec).
#[tokio::test]
async fn test_variant_arrow_schema() {
    let rest_catalog = rest_catalog().await;

    let table = rest_catalog
        .load_table(&TableIdent::from_strs(["default", "test_variant_column"]).unwrap())
        .await
        .unwrap();

    let scan = table.scan().build().unwrap();
    let batch_stream = scan.to_arrow().await.unwrap();
    let batches: Vec<_> = batch_stream.try_collect().await.unwrap();

    assert!(!batches.is_empty(), "expected at least one record batch");

    // Variant column must be a struct with exactly two binary sub-fields
    let v_col = batches[0]
        .column_by_name("v")
        .expect("column 'v' not found in batch");

    let DataType::Struct(fields) = v_col.data_type() else {
        panic!(
            "Expected variant column to be DataType::Struct, got {:?}",
            v_col.data_type()
        );
    };

    assert_eq!(
        fields.len(),
        2,
        "variant struct must have exactly 2 sub-fields"
    );

    let metadata_field = fields
        .iter()
        .find(|f| f.name() == "metadata")
        .expect("sub-field 'metadata' not found");
    let value_field = fields
        .iter()
        .find(|f| f.name() == "value")
        .expect("sub-field 'value' not found");

    assert_eq!(
        metadata_field.data_type(),
        &DataType::Binary,
        "'metadata' sub-field must be DataType::Binary"
    );
    assert_eq!(
        value_field.data_type(),
        &DataType::Binary,
        "'value' sub-field must be DataType::Binary"
    );
}

/// Verifies that a variant column is NOT silently dropped when it is projected
/// alongside ordinary primitive columns (regression test for the projection bug
/// where variant sub-fields had no embedded Parquet field IDs and were therefore
/// excluded from `column_map`, causing the whole variant group to be omitted).
#[tokio::test]
async fn test_variant_projected_with_primitive_columns() {
    let rest_catalog = rest_catalog().await;

    let table = rest_catalog
        .load_table(&TableIdent::from_strs(["default", "test_variant_column"]).unwrap())
        .await
        .unwrap();

    // Explicitly select only the two columns — this exercises the projection path
    // that was previously broken for variant types.
    let scan = table.scan().select(["id", "v"]).build().unwrap();
    let batch_stream = scan.to_arrow().await.unwrap();
    let batches: Vec<_> = batch_stream.try_collect().await.unwrap();

    assert!(!batches.is_empty(), "expected at least one record batch");

    let first_batch = &batches[0];

    // Both columns must be present — the variant must not be silently dropped.
    assert!(
        first_batch.column_by_name("id").is_some(),
        "column 'id' not found in projected batch"
    );
    let v_col = first_batch
        .column_by_name("v")
        .expect("column 'v' was silently dropped from projected scan — projection bug regression");

    // The variant column must still be a struct with the expected sub-fields.
    let DataType::Struct(fields) = v_col.data_type() else {
        panic!(
            "Expected variant column to be DataType::Struct after projection, got {:?}",
            v_col.data_type()
        );
    };
    assert_eq!(
        fields.len(),
        2,
        "projected variant struct must have exactly 2 sub-fields"
    );
    assert!(
        fields.iter().any(|f| f.name() == "metadata"),
        "projected variant struct must have a 'metadata' sub-field"
    );
    assert!(
        fields.iter().any(|f| f.name() == "value"),
        "projected variant struct must have a 'value' sub-field"
    );

    // All three seeded rows must be readable.
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "expected exactly 3 rows");
}

/// Verifies that projecting ONLY a variant column (without any sibling
/// primitive) still includes both of its sub-leaves in the projection mask.
/// This exercises a different branch of the mask builder than
/// [`test_variant_projected_with_primitive_columns`], where `column_map`
/// contains only variant-derived entries.
#[tokio::test]
async fn test_variant_projected_alone() {
    let rest_catalog = rest_catalog().await;

    let table = rest_catalog
        .load_table(&TableIdent::from_strs(["default", "test_variant_column"]).unwrap())
        .await
        .unwrap();

    let scan = table.scan().select(["v"]).build().unwrap();
    let batches: Vec<_> = scan.to_arrow().await.unwrap().try_collect().await.unwrap();

    assert!(!batches.is_empty(), "expected at least one record batch");
    let v_col = batches[0]
        .column_by_name("v")
        .expect("variant-only projection dropped column 'v'");
    assert_variant_struct(v_col.data_type());

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
}

/// Verifies that a table with multiple top-level variant columns is readable
/// end-to-end — guards against the Avro record-name collision
#[tokio::test]
async fn test_multiple_variant_columns() {
    let rest_catalog = rest_catalog().await;

    let table = rest_catalog
        .load_table(&TableIdent::from_strs(["default", "test_variant_multi"]).unwrap())
        .await
        .unwrap();

    // Both variant fields must parse as Type::Variant in the Iceberg schema.
    let schema = table.metadata().current_schema();
    for name in ["v1", "v2"] {
        let f = schema
            .field_by_name(name)
            .unwrap_or_else(|| panic!("field '{name}' missing from schema"));
        assert!(
            matches!(f.field_type.as_ref(), Type::Variant(_)),
            "field '{name}' is not Type::Variant: {:?}",
            f.field_type
        );
    }

    // Full scan returns both variant columns with the correct physical shape.
    let batches: Vec<_> = table
        .scan()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    assert!(!batches.is_empty());
    assert_variant_struct(
        batches[0]
            .column_by_name("v1")
            .expect("missing v1")
            .data_type(),
    );
    assert_variant_struct(
        batches[0]
            .column_by_name("v2")
            .expect("missing v2")
            .data_type(),
    );

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
}

/// Full scan of a table where a variant lives inside a struct.  Confirms that
/// the nested `payload: VARIANT` sub-field is materialized as
/// `Struct(metadata, value)` in the output.
#[tokio::test]
async fn test_nested_variant_full_scan() {
    let rest_catalog = rest_catalog().await;

    let table = rest_catalog
        .load_table(&TableIdent::from_strs(["default", "test_variant_nested"]).unwrap())
        .await
        .unwrap();

    let batches: Vec<_> = table
        .scan()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    assert!(!batches.is_empty());

    let nested = batches[0]
        .column_by_name("nested")
        .expect("column 'nested' missing");
    let nested_struct: &StructArray = nested.as_struct();
    let payload = nested_struct
        .column_by_name("payload")
        .expect("inner 'payload' field missing from nested struct");
    assert_variant_struct(payload.data_type());

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
}

/// Regression: projecting a struct column that contains a variant must keep
/// the inner variant's sub-leaves.
#[tokio::test]
async fn test_nested_variant_projected() {
    let rest_catalog = rest_catalog().await;

    let table = rest_catalog
        .load_table(&TableIdent::from_strs(["default", "test_variant_nested"]).unwrap())
        .await
        .unwrap();

    let batches: Vec<_> = table
        .scan()
        .select(["nested"])
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    assert!(!batches.is_empty());
    let nested = batches[0]
        .column_by_name("nested")
        .expect("projected scan dropped column 'nested'");
    let nested_struct: &StructArray = nested.as_struct();
    let payload = nested_struct.column_by_name("payload").expect(
        "nested.payload (variant) was dropped from the projected scan — \
             nested-variant projection bug regression",
    );
    assert_variant_struct(payload.data_type());

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
}

/// Projecting only a sibling primitive must NOT pull in the variant's leaves
/// from a neighbouring struct field. Guards against over-eager variant
/// inclusion in the projection mask.
#[tokio::test]
async fn test_nested_variant_sibling_projection() {
    let rest_catalog = rest_catalog().await;

    let table = rest_catalog
        .load_table(&TableIdent::from_strs(["default", "test_variant_nested"]).unwrap())
        .await
        .unwrap();

    let batches: Vec<_> = table
        .scan()
        .select(["id"])
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    assert!(!batches.is_empty());
    let first = &batches[0];
    assert!(first.column_by_name("id").is_some(), "column 'id' missing");
    assert!(
        first.column_by_name("nested").is_none(),
        "projecting only 'id' must not pull in sibling 'nested' (variant leaked)"
    );

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
}
