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

//! Common rename behavior across catalogs.
//!
//! These tests assume Docker containers are started externally via `make docker-up`.

mod common;

use std::collections::HashMap;

use common::{CatalogKind, cleanup_namespace_dyn, load_catalog, table_creation};
use iceberg::{NamespaceIdent, Result, TableIdent};
use iceberg_test_utils::normalize_test_name_with_parts;
use rstest::rstest;

// Common behavior: renaming across namespaces moves the table.
#[rstest]
#[case::rest_catalog(CatalogKind::Rest)]
#[case::glue_catalog(CatalogKind::Glue)]
#[case::hms_catalog(CatalogKind::Hms)]
#[case::sql_catalog(CatalogKind::Sql)]
#[case::s3tables_catalog(CatalogKind::S3Tables)]
#[case::memory_catalog(CatalogKind::Memory)]
#[tokio::test]
async fn test_catalog_rename_table_across_namespaces(#[case] kind: CatalogKind) -> Result<()> {
    let Some(harness) = load_catalog(kind).await else {
        return Ok(());
    };
    let catalog = harness.catalog;
    let src_namespace = NamespaceIdent::new(normalize_test_name_with_parts!(
        "catalog_rename_table_across_namespaces",
        harness.label,
        "src"
    ));
    let dst_namespace = NamespaceIdent::new(normalize_test_name_with_parts!(
        "catalog_rename_table_across_namespaces",
        harness.label,
        "dst"
    ));

    cleanup_namespace_dyn(catalog.as_ref(), &src_namespace).await;
    cleanup_namespace_dyn(catalog.as_ref(), &dst_namespace).await;
    catalog
        .create_namespace(&src_namespace, HashMap::new())
        .await?;
    catalog
        .create_namespace(&dst_namespace, HashMap::new())
        .await?;

    let table = catalog
        .create_table(
            &src_namespace,
            table_creation(normalize_test_name_with_parts!(
                "catalog_rename_table_across_namespaces",
                harness.label,
                "table"
            )),
        )
        .await?;
    let src_ident = table.identifier().clone();
    let dst_ident = TableIdent::new(dst_namespace.clone(), src_ident.name.clone());

    catalog.rename_table(&src_ident, &dst_ident).await?;
    assert!(catalog.table_exists(&dst_ident).await?);
    assert!(!catalog.table_exists(&src_ident).await?);

    Ok(())
}

// Common behavior: renaming a missing table should error.
#[rstest]
#[case::rest_catalog(CatalogKind::Rest)]
#[case::glue_catalog(CatalogKind::Glue)]
#[case::hms_catalog(CatalogKind::Hms)]
#[case::sql_catalog(CatalogKind::Sql)]
#[case::s3tables_catalog(CatalogKind::S3Tables)]
#[case::memory_catalog(CatalogKind::Memory)]
#[tokio::test]
async fn test_catalog_rename_table_missing_source_errors(#[case] kind: CatalogKind) -> Result<()> {
    let Some(harness) = load_catalog(kind).await else {
        return Ok(());
    };
    let catalog = harness.catalog;
    let namespace = NamespaceIdent::new(normalize_test_name_with_parts!(
        "catalog_rename_table_missing_source_errors",
        harness.label
    ));

    cleanup_namespace_dyn(catalog.as_ref(), &namespace).await;
    catalog.create_namespace(&namespace, HashMap::new()).await?;

    let src_ident = TableIdent::new(namespace.clone(), "missing".to_string());
    let dst_ident = TableIdent::new(namespace.clone(), "dest".to_string());

    assert!(catalog.rename_table(&src_ident, &dst_ident).await.is_err());
    Ok(())
}

// Common behavior: renaming to an existing destination should error.
#[rstest]
#[case::rest_catalog(CatalogKind::Rest)]
#[case::glue_catalog(CatalogKind::Glue)]
#[case::hms_catalog(CatalogKind::Hms)]
#[case::sql_catalog(CatalogKind::Sql)]
#[case::s3tables_catalog(CatalogKind::S3Tables)]
#[case::memory_catalog(CatalogKind::Memory)]
#[tokio::test]
async fn test_catalog_rename_table_dest_exists_errors(#[case] kind: CatalogKind) -> Result<()> {
    let Some(harness) = load_catalog(kind).await else {
        return Ok(());
    };
    let catalog = harness.catalog;
    let namespace = NamespaceIdent::new(normalize_test_name_with_parts!(
        "catalog_rename_table_dest_exists_errors",
        harness.label
    ));

    cleanup_namespace_dyn(catalog.as_ref(), &namespace).await;
    catalog.create_namespace(&namespace, HashMap::new()).await?;

    let src = catalog
        .create_table(
            &namespace,
            table_creation(normalize_test_name_with_parts!(
                "catalog_rename_table_dest_exists_errors",
                harness.label,
                "src"
            )),
        )
        .await?
        .identifier()
        .clone();
    let dst = catalog
        .create_table(
            &namespace,
            table_creation(normalize_test_name_with_parts!(
                "catalog_rename_table_dest_exists_errors",
                harness.label,
                "dst"
            )),
        )
        .await?
        .identifier()
        .clone();

    assert!(catalog.rename_table(&src, &dst).await.is_err());
    Ok(())
}
