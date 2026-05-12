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

//! Common schema-update behavior across catalogs.
//!
//! These tests assume Docker containers are started externally via `make docker-up`.

mod common;

use std::collections::HashMap;

use common::{CatalogKind, cleanup_namespace_dyn, load_catalog};
use iceberg::spec::{
    AddColumn, DeleteColumn, NestedField, PrimitiveType, Schema, SchemaOperation, StructType, Type,
};
use iceberg::transaction::Transaction;
use iceberg::{ErrorKind, NamespaceIdent, Result, TableCreation, TableIdent};
use iceberg_test_utils::normalize_test_name_with_parts;
use rstest::rstest;

fn base_schema() -> Schema {
    Schema::builder()
        .with_fields(vec![
            NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
        ])
        .with_identifier_field_ids(vec![2])
        .build()
        .unwrap()
}

// Common behavior: adding a top-level column appends it to the schema.
// HMS is excluded because update_table is not yet supported.
#[rstest]
#[case::rest_catalog(CatalogKind::Rest)]
#[case::glue_catalog(CatalogKind::Glue)]
#[case::sql_catalog(CatalogKind::Sql)]
#[case::s3tables_catalog(CatalogKind::S3Tables)]
#[case::memory_catalog(CatalogKind::Memory)]
#[tokio::test]
async fn test_catalog_schema_add_column(#[case] kind: CatalogKind) -> Result<()> {
    use iceberg::transaction::ApplyTransactionAction;

    let Some(harness) = load_catalog(kind).await else {
        return Ok(());
    };
    let catalog = harness.catalog;
    let namespace = NamespaceIdent::new(normalize_test_name_with_parts!(
        "catalog_schema_add_column",
        harness.label
    ));

    cleanup_namespace_dyn(catalog.as_ref(), &namespace).await;
    catalog.create_namespace(&namespace, HashMap::new()).await?;

    let table_name =
        normalize_test_name_with_parts!("catalog_schema_add_column", harness.label, "table");
    let table = catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name(table_name)
                .schema(base_schema())
                .build(),
        )
        .await?;

    let tx = Transaction::new(&table);
    let tx = tx
        .update_schema()
        .push_operation(SchemaOperation::Add(
            AddColumn::builder()
                .name("a")
                .r#type(PrimitiveType::Int.into())
                .build(),
        ))
        .apply(tx)?;
    let updated = tx.commit(catalog.as_ref()).await?;

    let schema = updated.metadata().current_schema();
    let field_a = schema.field_by_name("a").expect("field 'a' should exist");
    assert_eq!(field_a.id, 4);
    assert_eq!(*field_a.field_type, Type::Primitive(PrimitiveType::Int));

    Ok(())
}

// Common behavior: adding a nested struct column then a sub-field within it,
// and deleting another top-level column, all persist correctly.
// HMS is excluded because update_table is not yet supported.
#[rstest]
#[case::rest_catalog(CatalogKind::Rest)]
#[case::glue_catalog(CatalogKind::Glue)]
#[case::sql_catalog(CatalogKind::Sql)]
#[case::s3tables_catalog(CatalogKind::S3Tables)]
#[case::memory_catalog(CatalogKind::Memory)]
#[tokio::test]
async fn test_catalog_schema_add_nested_and_delete_column(#[case] kind: CatalogKind) -> Result<()> {
    use iceberg::transaction::ApplyTransactionAction;

    let Some(harness) = load_catalog(kind).await else {
        return Ok(());
    };
    let catalog = harness.catalog;
    let namespace = NamespaceIdent::new(normalize_test_name_with_parts!(
        "catalog_schema_add_nested_and_delete_column",
        harness.label
    ));

    cleanup_namespace_dyn(catalog.as_ref(), &namespace).await;
    catalog.create_namespace(&namespace, HashMap::new()).await?;

    let table_name = normalize_test_name_with_parts!(
        "catalog_schema_add_nested_and_delete_column",
        harness.label,
        "table"
    );
    let table = catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name(table_name)
                .schema(base_schema())
                .build(),
        )
        .await?;

    // First transaction: add a nested struct column.
    let tx = Transaction::new(&table);
    let tx = tx
        .update_schema()
        .push_operation(
            AddColumn::builder()
                .name("info")
                .r#type(
                    StructType::new(vec![
                        NestedField::optional(0, "city", PrimitiveType::String.into()).into(),
                    ])
                    .into(),
                )
                .build()
                .into(),
        )
        .apply(tx)?;
    let table = tx.commit(catalog.as_ref()).await?;

    // Second transaction: add a sub-field to the nested struct and delete a top-level column.
    let tx = Transaction::new(&table);
    let tx = tx
        .update_schema()
        .push_operation(
            AddColumn::builder()
                .name("zip")
                .r#type(PrimitiveType::String.into())
                .parent("info".into())
                .build()
                .into(),
        )
        .push_operation(DeleteColumn::new("baz").into())
        .apply(tx)?;
    let table = tx.commit(catalog.as_ref()).await?;

    let schema = table.metadata().current_schema();
    assert!(schema.field_by_name("info").is_some());
    assert!(schema.field_by_name("info.city").is_some());
    assert!(schema.field_by_name("info.zip").is_some());
    assert!(schema.field_by_name("baz").is_none());

    Ok(())
}

// Common behavior: deleting an identifier field or a nonexistent field must fail.
// HMS is excluded because update_table is not yet supported.
#[rstest]
#[case::rest_catalog(CatalogKind::Rest)]
#[case::glue_catalog(CatalogKind::Glue)]
#[case::sql_catalog(CatalogKind::Sql)]
#[case::s3tables_catalog(CatalogKind::S3Tables)]
#[case::memory_catalog(CatalogKind::Memory)]
#[tokio::test]
async fn test_catalog_schema_delete_invalid_column_errors(#[case] kind: CatalogKind) -> Result<()> {
    use iceberg::transaction::ApplyTransactionAction;

    let Some(harness) = load_catalog(kind).await else {
        return Ok(());
    };
    let catalog = harness.catalog;
    let namespace = NamespaceIdent::new(normalize_test_name_with_parts!(
        "catalog_schema_delete_invalid_column_errors",
        harness.label
    ));

    cleanup_namespace_dyn(catalog.as_ref(), &namespace).await;
    catalog.create_namespace(&namespace, HashMap::new()).await?;

    let table_name = normalize_test_name_with_parts!(
        "catalog_schema_delete_invalid_column_errors",
        harness.label,
        "table"
    );
    let table = catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name(table_name)
                .schema(base_schema())
                .build(),
        )
        .await?;

    // Deleting an identifier field must fail.
    let tx = Transaction::new(&table);
    let tx = tx
        .update_schema()
        .push_operation(DeleteColumn::new("bar").into())
        .apply(tx)?;
    let err = tx.commit(catalog.as_ref()).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::PreconditionFailed);

    // Deleting a nonexistent field must fail.
    let tx = Transaction::new(&table);
    let tx = tx
        .update_schema()
        .push_operation(DeleteColumn::new("nonexistent").into())
        .apply(tx)?;
    let err = tx.commit(catalog.as_ref()).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::PreconditionFailed);

    Ok(())
}

// Common behavior: loading a table after a schema update reflects the new schema.
// HMS is excluded because update_table is not yet supported.
#[rstest]
#[case::rest_catalog(CatalogKind::Rest)]
#[case::glue_catalog(CatalogKind::Glue)]
#[case::sql_catalog(CatalogKind::Sql)]
#[case::s3tables_catalog(CatalogKind::S3Tables)]
#[case::memory_catalog(CatalogKind::Memory)]
#[tokio::test]
async fn test_catalog_schema_update_persisted_after_reload(
    #[case] kind: CatalogKind,
) -> Result<()> {
    use iceberg::transaction::ApplyTransactionAction;

    let Some(harness) = load_catalog(kind).await else {
        return Ok(());
    };
    let catalog = harness.catalog;
    let namespace = NamespaceIdent::new(normalize_test_name_with_parts!(
        "catalog_schema_update_persisted_after_reload",
        harness.label
    ));

    cleanup_namespace_dyn(catalog.as_ref(), &namespace).await;
    catalog.create_namespace(&namespace, HashMap::new()).await?;

    let table_name = normalize_test_name_with_parts!(
        "catalog_schema_update_persisted_after_reload",
        harness.label,
        "table"
    );
    let table_ident = TableIdent::new(namespace.clone(), table_name.clone());
    let table = catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name(table_name)
                .schema(base_schema())
                .build(),
        )
        .await?;

    let tx = Transaction::new(&table);
    let tx = tx
        .update_schema()
        .push_operation(
            AddColumn::builder()
                .name("new_field")
                .r#type(PrimitiveType::Long.into())
                .build()
                .into(),
        )
        .apply(tx)?;
    tx.commit(catalog.as_ref()).await?;

    let reloaded = catalog.load_table(&table_ident).await?;
    assert!(
        reloaded
            .metadata()
            .current_schema()
            .field_by_name("new_field")
            .is_some()
    );

    Ok(())
}
