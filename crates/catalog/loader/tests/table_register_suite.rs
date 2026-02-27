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

//! Common register-table behavior across catalogs.
//!
//! These tests assume Docker containers are started externally via `make docker-up`.

mod common;

use std::collections::HashMap;

use common::{CatalogKind, cleanup_namespace_dyn, load_catalog, table_creation};
use iceberg::{ErrorKind, NamespaceIdent, Result, TableIdent};
use iceberg_test_utils::normalize_test_name_with_parts;
use rstest::rstest;

// Common behavior: register_table rehydrates a dropped table.
#[rstest]
#[case::rest_catalog(CatalogKind::Rest)]
#[case::glue_catalog(CatalogKind::Glue)]
#[case::sql_catalog(CatalogKind::Sql)]
#[case::memory_catalog(CatalogKind::Memory)]
#[tokio::test]
async fn test_catalog_register_table_roundtrip(#[case] kind: CatalogKind) -> Result<()> {
    let Some(harness) = load_catalog(kind).await else {
        return Ok(());
    };
    let catalog = harness.catalog;
    let namespace = NamespaceIdent::new(normalize_test_name_with_parts!(
        "catalog_register_table_roundtrip",
        harness.label
    ));

    cleanup_namespace_dyn(catalog.as_ref(), &namespace).await;
    catalog.create_namespace(&namespace, HashMap::new()).await?;

    let table = catalog
        .create_table(
            &namespace,
            table_creation(normalize_test_name_with_parts!(
                "catalog_register_table_roundtrip",
                harness.label,
                "table"
            )),
        )
        .await?;
    let table_ident = table.identifier().clone();
    let metadata_location = table
        .metadata_location()
        .ok_or_else(|| iceberg::Error::new(ErrorKind::Unexpected, "Missing metadata location"))?
        .to_string();

    catalog.drop_table(&table_ident).await?;

    let registered = catalog
        .register_table(&table_ident, metadata_location.clone())
        .await?;
    assert_eq!(registered.identifier(), &table_ident);
    assert_eq!(
        registered.metadata_location(),
        Some(metadata_location.as_str())
    );

    Ok(())
}

// HMS and S3Tables do not support register_table yet.
#[rstest]
#[case::hms_catalog(CatalogKind::Hms)]
#[case::s3tables_catalog(CatalogKind::S3Tables)]
#[tokio::test]
async fn test_catalog_register_table_unsupported(#[case] kind: CatalogKind) -> Result<()> {
    let Some(harness) = load_catalog(kind).await else {
        return Ok(());
    };
    let catalog = harness.catalog;
    let namespace = NamespaceIdent::new(normalize_test_name_with_parts!(
        "catalog_register_table_unsupported",
        harness.label
    ));

    cleanup_namespace_dyn(catalog.as_ref(), &namespace).await;
    catalog.create_namespace(&namespace, HashMap::new()).await?;

    let table = catalog
        .create_table(
            &namespace,
            table_creation(normalize_test_name_with_parts!(
                "catalog_register_table_unsupported",
                harness.label,
                "table"
            )),
        )
        .await?;
    let table_ident = table.identifier().clone();
    let metadata_location = table
        .metadata_location()
        .ok_or_else(|| iceberg::Error::new(ErrorKind::Unexpected, "Missing metadata location"))?
        .to_string();

    let err = catalog
        .register_table(&table_ident, metadata_location)
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::FeatureUnsupported);

    Ok(())
}

// Common behavior: registering a table with an existing name should error.
#[rstest]
#[case::rest_catalog(CatalogKind::Rest)]
#[case::glue_catalog(CatalogKind::Glue)]
#[case::sql_catalog(CatalogKind::Sql)]
#[case::memory_catalog(CatalogKind::Memory)]
#[tokio::test]
async fn test_catalog_register_table_conflict_errors(#[case] kind: CatalogKind) -> Result<()> {
    let Some(harness) = load_catalog(kind).await else {
        return Ok(());
    };
    let catalog = harness.catalog;
    let namespace = NamespaceIdent::new(normalize_test_name_with_parts!(
        "catalog_register_table_conflict_errors",
        harness.label
    ));

    cleanup_namespace_dyn(catalog.as_ref(), &namespace).await;
    catalog.create_namespace(&namespace, HashMap::new()).await?;

    let table_ident = TableIdent::new(
        namespace.clone(),
        normalize_test_name_with_parts!(
            "catalog_register_table_conflict_errors",
            harness.label,
            "table"
        ),
    );
    let table = catalog
        .create_table(&namespace, table_creation(table_ident.name.clone()))
        .await?;
    let metadata_location = table
        .metadata_location()
        .ok_or_else(|| iceberg::Error::new(ErrorKind::Unexpected, "Missing metadata location"))?
        .to_string();

    assert!(
        catalog
            .register_table(&table_ident, metadata_location)
            .await
            .is_err()
    );
    Ok(())
}
