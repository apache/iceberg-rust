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

//! Common table behavior across catalogs.
//!
//! These tests assume Docker containers are started externally via `make docker-up`.

mod common;

use std::collections::HashMap;

use common::{CatalogKind, cleanup_namespace_dyn, load_catalog, table_creation};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{ErrorKind, NamespaceIdent, Result, TableIdent};
use iceberg_test_utils::normalize_test_name_with_parts;
use rstest::rstest;

// Common behavior: table lifecycle CRUD.
#[rstest]
#[case::rest_catalog(CatalogKind::Rest)]
#[case::glue_catalog(CatalogKind::Glue)]
#[case::hms_catalog(CatalogKind::Hms)]
#[case::sql_catalog(CatalogKind::Sql)]
#[case::s3tables_catalog(CatalogKind::S3Tables)]
#[case::memory_catalog(CatalogKind::Memory)]
#[tokio::test]
async fn test_catalog_table_lifecycle(#[case] kind: CatalogKind) -> Result<()> {
    let Some(harness) = load_catalog(kind).await else {
        return Ok(());
    };
    let catalog = harness.catalog;
    let namespace = NamespaceIdent::new(normalize_test_name_with_parts!(
        "catalog_table_lifecycle",
        harness.label
    ));

    cleanup_namespace_dyn(catalog.as_ref(), &namespace).await;
    catalog.create_namespace(&namespace, HashMap::new()).await?;

    let table_name =
        normalize_test_name_with_parts!("catalog_table_lifecycle", harness.label, "table");
    let table = catalog
        .create_table(&namespace, table_creation(table_name))
        .await?;
    let ident = table.identifier().clone();

    assert!(catalog.table_exists(&ident).await?);
    let loaded = catalog.load_table(&ident).await?;
    assert_eq!(loaded.identifier(), &ident);

    let tables = catalog.list_tables(&namespace).await?;
    assert!(tables.contains(&ident));

    let dest = TableIdent::new(ident.namespace.clone(), format!("{}_renamed", ident.name));
    catalog.rename_table(&ident, &dest).await?;
    assert!(catalog.table_exists(&dest).await?);
    assert!(!catalog.table_exists(&ident).await?);

    catalog.drop_table(&dest).await?;
    assert!(!catalog.table_exists(&dest).await?);

    catalog.drop_namespace(&namespace).await?;

    Ok(())
}

// Common behavior: listing tables for a missing namespace should error.
#[rstest]
#[case::rest_catalog(CatalogKind::Rest)]
#[case::glue_catalog(CatalogKind::Glue)]
#[case::hms_catalog(CatalogKind::Hms)]
#[case::sql_catalog(CatalogKind::Sql)]
#[case::s3tables_catalog(CatalogKind::S3Tables)]
#[case::memory_catalog(CatalogKind::Memory)]
#[tokio::test]
async fn test_catalog_list_tables_missing_namespace_errors(
    #[case] kind: CatalogKind,
) -> Result<()> {
    let Some(harness) = load_catalog(kind).await else {
        return Ok(());
    };
    let catalog = harness.catalog;
    let namespace = NamespaceIdent::new(normalize_test_name_with_parts!(
        "catalog_list_tables_missing_namespace_errors",
        harness.label
    ));

    cleanup_namespace_dyn(catalog.as_ref(), &namespace).await;

    assert!(catalog.list_tables(&namespace).await.is_err());
    Ok(())
}

// Common behavior: listing tables in an empty namespace returns empty.
#[rstest]
#[case::rest_catalog(CatalogKind::Rest)]
#[case::glue_catalog(CatalogKind::Glue)]
#[case::hms_catalog(CatalogKind::Hms)]
#[case::sql_catalog(CatalogKind::Sql)]
#[case::s3tables_catalog(CatalogKind::S3Tables)]
#[case::memory_catalog(CatalogKind::Memory)]
#[tokio::test]
async fn test_catalog_list_tables_empty_namespace(#[case] kind: CatalogKind) -> Result<()> {
    let Some(harness) = load_catalog(kind).await else {
        return Ok(());
    };
    let catalog = harness.catalog;
    let namespace = NamespaceIdent::new(normalize_test_name_with_parts!(
        "catalog_list_tables_empty_namespace",
        harness.label
    ));

    cleanup_namespace_dyn(catalog.as_ref(), &namespace).await;
    catalog.create_namespace(&namespace, HashMap::new()).await?;

    let tables = catalog.list_tables(&namespace).await?;
    assert!(tables.is_empty());

    Ok(())
}

// Common behavior: created tables expose the requested schema.
#[rstest]
#[case::rest_catalog(CatalogKind::Rest)]
#[case::glue_catalog(CatalogKind::Glue)]
#[case::hms_catalog(CatalogKind::Hms)]
#[case::sql_catalog(CatalogKind::Sql)]
#[case::s3tables_catalog(CatalogKind::S3Tables)]
#[case::memory_catalog(CatalogKind::Memory)]
#[tokio::test]
async fn test_catalog_create_table_schema(#[case] kind: CatalogKind) -> Result<()> {
    let Some(harness) = load_catalog(kind).await else {
        return Ok(());
    };
    let catalog = harness.catalog;
    let namespace = NamespaceIdent::new(normalize_test_name_with_parts!(
        "catalog_create_table_schema",
        harness.label
    ));

    cleanup_namespace_dyn(catalog.as_ref(), &namespace).await;
    catalog.create_namespace(&namespace, HashMap::new()).await?;

    let table_name =
        normalize_test_name_with_parts!("catalog_create_table_schema", harness.label, "table");
    let creation = table_creation(table_name);
    let expected_schema = creation.schema.clone();

    let table = catalog.create_table(&namespace, creation).await?;
    assert_eq!(table.identifier().namespace, namespace);
    assert_eq!(table.metadata().current_schema().as_ref(), &expected_schema);

    Ok(())
}

// Common behavior: updating table properties persists through the catalog.
// HMS is excluded because update_table is not supported yet.
#[rstest]
#[case::rest_catalog(CatalogKind::Rest)]
#[case::glue_catalog(CatalogKind::Glue)]
#[case::sql_catalog(CatalogKind::Sql)]
#[case::s3tables_catalog(CatalogKind::S3Tables)]
#[case::memory_catalog(CatalogKind::Memory)]
#[tokio::test]
async fn test_catalog_update_table_properties(#[case] kind: CatalogKind) -> Result<()> {
    let Some(harness) = load_catalog(kind).await else {
        return Ok(());
    };
    let catalog = harness.catalog;
    let namespace = NamespaceIdent::new(normalize_test_name_with_parts!(
        "catalog_update_table_properties",
        harness.label
    ));

    cleanup_namespace_dyn(catalog.as_ref(), &namespace).await;
    catalog.create_namespace(&namespace, HashMap::new()).await?;

    let table_name =
        normalize_test_name_with_parts!("catalog_update_table_properties", harness.label, "table");
    let table = catalog
        .create_table(&namespace, table_creation(table_name))
        .await?;

    let tx = Transaction::new(&table);
    let tx = tx
        .update_table_properties()
        .set("test_property".to_string(), "test_value".to_string())
        .apply(tx)?;
    let updated = tx.commit(catalog.as_ref()).await?;

    assert_eq!(
        updated.metadata().properties().get("test_property"),
        Some(&"test_value".to_string())
    );

    Ok(())
}

// Common behavior: update_table_properties is rejected when unsupported.
#[rstest]
#[case::hms_catalog(CatalogKind::Hms)]
#[tokio::test]
async fn test_catalog_update_table_properties_unsupported(#[case] kind: CatalogKind) -> Result<()> {
    let Some(harness) = load_catalog(kind).await else {
        return Ok(());
    };
    let catalog = harness.catalog;
    let namespace = NamespaceIdent::new(normalize_test_name_with_parts!(
        "catalog_update_table_properties_unsupported",
        harness.label
    ));

    cleanup_namespace_dyn(catalog.as_ref(), &namespace).await;
    catalog.create_namespace(&namespace, HashMap::new()).await?;

    let table_name = normalize_test_name_with_parts!(
        "catalog_update_table_properties_unsupported",
        harness.label,
        "table"
    );
    let table = catalog
        .create_table(&namespace, table_creation(table_name))
        .await?;

    let tx = Transaction::new(&table);
    let tx = tx
        .update_table_properties()
        .set("test_property".to_string(), "test_value".to_string())
        .apply(tx)?;

    let err = tx.commit(catalog.as_ref()).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::FeatureUnsupported);

    Ok(())
}

// Common behavior: dropping a missing table should error.
#[rstest]
#[case::rest_catalog(CatalogKind::Rest)]
#[case::glue_catalog(CatalogKind::Glue)]
#[case::hms_catalog(CatalogKind::Hms)]
#[case::sql_catalog(CatalogKind::Sql)]
#[case::s3tables_catalog(CatalogKind::S3Tables)]
#[case::memory_catalog(CatalogKind::Memory)]
#[tokio::test]
async fn test_catalog_drop_table_missing_errors(#[case] kind: CatalogKind) -> Result<()> {
    let Some(harness) = load_catalog(kind).await else {
        return Ok(());
    };
    let catalog = harness.catalog;
    let namespace = NamespaceIdent::new(normalize_test_name_with_parts!(
        "catalog_drop_table_missing_errors",
        harness.label
    ));

    cleanup_namespace_dyn(catalog.as_ref(), &namespace).await;
    catalog.create_namespace(&namespace, HashMap::new()).await?;

    let table_ident = TableIdent::new(namespace.clone(), "missing".to_string());
    assert!(catalog.drop_table(&table_ident).await.is_err());
    Ok(())
}
