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

use arrow_schema::DataType;
use futures::TryStreamExt;
use iceberg::spec::Type;
use iceberg::{Catalog, CatalogBuilder, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_integration_tests::get_test_fixture;

/// Verifies that a table written by Spark with a VARIANT column has its schema
/// parsed into `Type::Variant` by the Rust iceberg implementation.
#[tokio::test]
async fn test_variant_schema_is_parsed() {
    let fixture = get_test_fixture();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

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
    let fixture = get_test_fixture();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

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
    let fixture = get_test_fixture();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

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
