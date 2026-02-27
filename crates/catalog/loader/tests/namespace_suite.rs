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

//! Common namespace behavior across catalogs.
//!
//! These tests assume Docker containers are started externally via `make docker-up`.

mod common;

use std::collections::HashMap;

use common::{CatalogKind, assert_map_contains, cleanup_namespace_dyn, load_catalog};
use iceberg::{ErrorKind, NamespaceIdent, Result};
use iceberg_test_utils::normalize_test_name_with_parts;
use rstest::rstest;

// Common behavior: querying a missing namespace should error.
#[rstest]
#[case::rest_catalog(CatalogKind::Rest)]
#[case::glue_catalog(CatalogKind::Glue)]
#[case::hms_catalog(CatalogKind::Hms)]
#[case::sql_catalog(CatalogKind::Sql)]
#[case::s3tables_catalog(CatalogKind::S3Tables)]
#[case::memory_catalog(CatalogKind::Memory)]
#[tokio::test]
async fn test_catalog_namespace_missing_returns_error(#[case] kind: CatalogKind) -> Result<()> {
    let Some(harness) = load_catalog(kind).await else {
        return Ok(());
    };
    let catalog = harness.catalog;
    let namespace = NamespaceIdent::new(normalize_test_name_with_parts!(
        "catalog_namespace_missing_returns_error",
        harness.label
    ));

    cleanup_namespace_dyn(catalog.as_ref(), &namespace).await;

    let err = catalog.get_namespace(&namespace).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NamespaceNotFound);

    Ok(())
}

// Common behavior: namespace lifecycle CRUD.
#[rstest]
#[case::rest_catalog(CatalogKind::Rest)]
#[case::glue_catalog(CatalogKind::Glue)]
#[case::hms_catalog(CatalogKind::Hms)]
#[case::sql_catalog(CatalogKind::Sql)]
#[case::s3tables_catalog(CatalogKind::S3Tables)]
#[case::memory_catalog(CatalogKind::Memory)]
#[tokio::test]
async fn test_catalog_namespace_lifecycle(#[case] kind: CatalogKind) -> Result<()> {
    let Some(harness) = load_catalog(kind).await else {
        return Ok(());
    };
    let catalog = harness.catalog;
    let namespace = NamespaceIdent::new(normalize_test_name_with_parts!(
        "catalog_namespace_lifecycle",
        harness.label
    ));

    cleanup_namespace_dyn(catalog.as_ref(), &namespace).await;

    assert!(!catalog.namespace_exists(&namespace).await?);

    let props = HashMap::from([
        ("owner".to_string(), "rust".to_string()),
        ("purpose".to_string(), "catalog_suite".to_string()),
    ]);
    let created = catalog.create_namespace(&namespace, props.clone()).await?;
    assert_eq!(created.name(), &namespace);
    assert_map_contains(&props, created.properties());

    let fetched = catalog.get_namespace(&namespace).await?;
    assert_eq!(fetched.name(), &namespace);
    assert_map_contains(&props, fetched.properties());

    let namespaces = catalog.list_namespaces(None).await?;
    assert!(namespaces.contains(&namespace));

    catalog.drop_namespace(&namespace).await?;
    assert!(!catalog.namespace_exists(&namespace).await?);

    Ok(())
}

// Common behavior: update_namespace persists changes when supported.
#[rstest]
#[case::glue_catalog(CatalogKind::Glue)]
#[case::hms_catalog(CatalogKind::Hms)]
#[case::sql_catalog(CatalogKind::Sql)]
#[case::memory_catalog(CatalogKind::Memory)]
#[tokio::test]
async fn test_catalog_update_namespace_supported(#[case] kind: CatalogKind) -> Result<()> {
    let Some(harness) = load_catalog(kind).await else {
        return Ok(());
    };
    let catalog = harness.catalog;
    let namespace = NamespaceIdent::new(normalize_test_name_with_parts!(
        "catalog_update_namespace_supported",
        harness.label
    ));

    cleanup_namespace_dyn(catalog.as_ref(), &namespace).await;
    catalog.create_namespace(&namespace, HashMap::new()).await?;

    let updated_props = HashMap::from([("owner".to_string(), "updated".to_string())]);
    catalog
        .update_namespace(&namespace, updated_props.clone())
        .await?;

    let updated = catalog.get_namespace(&namespace).await?;
    assert_map_contains(&updated_props, updated.properties());

    Ok(())
}

// Common behavior: update_namespace returns FeatureUnsupported when not implemented.
#[rstest]
#[case::rest_catalog(CatalogKind::Rest)]
#[case::s3tables_catalog(CatalogKind::S3Tables)]
#[tokio::test]
async fn test_catalog_update_namespace_unsupported(#[case] kind: CatalogKind) -> Result<()> {
    let Some(harness) = load_catalog(kind).await else {
        return Ok(());
    };
    let catalog = harness.catalog;
    let namespace = NamespaceIdent::new(normalize_test_name_with_parts!(
        "catalog_update_namespace_unsupported",
        harness.label
    ));

    cleanup_namespace_dyn(catalog.as_ref(), &namespace).await;
    catalog.create_namespace(&namespace, HashMap::new()).await?;

    let err = catalog
        .update_namespace(
            &namespace,
            HashMap::from([("key".to_string(), "value".to_string())]),
        )
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::FeatureUnsupported);

    Ok(())
}

// Common behavior: listing namespaces under a parent returns its children.
#[rstest]
#[case::rest_catalog(CatalogKind::Rest)]
#[case::sql_catalog(CatalogKind::Sql)]
#[case::s3tables_catalog(CatalogKind::S3Tables)]
#[case::memory_catalog(CatalogKind::Memory)]
#[tokio::test]
async fn test_catalog_namespace_listing_with_parent(#[case] kind: CatalogKind) -> Result<()> {
    let Some(harness) = load_catalog(kind).await else {
        return Ok(());
    };
    let catalog = harness.catalog;
    let parent_name =
        normalize_test_name_with_parts!("catalog_namespace_listing_with_parent", harness.label);
    let parent = NamespaceIdent::new(parent_name.clone());
    let child1 = NamespaceIdent::from_strs([&parent_name, "child1"]).unwrap();
    let child2 = NamespaceIdent::from_strs([&parent_name, "child2"]).unwrap();

    cleanup_namespace_dyn(catalog.as_ref(), &child1).await;
    cleanup_namespace_dyn(catalog.as_ref(), &child2).await;
    cleanup_namespace_dyn(catalog.as_ref(), &parent).await;

    catalog.create_namespace(&parent, HashMap::new()).await?;

    catalog.create_namespace(&child1, HashMap::new()).await?;
    catalog.create_namespace(&child2, HashMap::new()).await?;

    let top_level = catalog.list_namespaces(None).await?;
    assert!(top_level.contains(&parent));

    let children = catalog.list_namespaces(Some(&parent)).await?;
    assert!(children.contains(&child1));
    assert!(children.contains(&child2));

    Ok(())
}

// Common behavior: hierarchical namespaces are rejected when unsupported.
#[rstest]
#[case::glue_catalog(CatalogKind::Glue)]
#[case::hms_catalog(CatalogKind::Hms)]
#[tokio::test]
async fn test_catalog_namespace_listing_with_parent_unsupported(
    #[case] kind: CatalogKind,
) -> Result<()> {
    let Some(harness) = load_catalog(kind).await else {
        return Ok(());
    };
    let catalog = harness.catalog;
    let parent_name = normalize_test_name_with_parts!(
        "catalog_namespace_listing_with_parent_unsupported",
        harness.label
    );
    let parent = NamespaceIdent::new(parent_name.clone());
    let child = NamespaceIdent::from_strs([&parent_name, "child"]).unwrap();

    cleanup_namespace_dyn(catalog.as_ref(), &child).await;
    cleanup_namespace_dyn(catalog.as_ref(), &parent).await;

    catalog.create_namespace(&parent, HashMap::new()).await?;

    let err = catalog
        .create_namespace(&child, HashMap::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::DataInvalid);

    Ok(())
}

// Common behavior: listing top-level namespaces includes created namespaces.
#[rstest]
#[case::rest_catalog(CatalogKind::Rest)]
#[case::glue_catalog(CatalogKind::Glue)]
#[case::hms_catalog(CatalogKind::Hms)]
#[case::sql_catalog(CatalogKind::Sql)]
#[case::s3tables_catalog(CatalogKind::S3Tables)]
#[case::memory_catalog(CatalogKind::Memory)]
#[tokio::test]
async fn test_catalog_list_namespaces_contains_created(#[case] kind: CatalogKind) -> Result<()> {
    let Some(harness) = load_catalog(kind).await else {
        return Ok(());
    };
    let catalog = harness.catalog;
    let ns_one = NamespaceIdent::new(normalize_test_name_with_parts!(
        "catalog_list_namespaces_contains_created",
        harness.label,
        "one"
    ));
    let ns_two = NamespaceIdent::new(normalize_test_name_with_parts!(
        "catalog_list_namespaces_contains_created",
        harness.label,
        "two"
    ));

    cleanup_namespace_dyn(catalog.as_ref(), &ns_one).await;
    cleanup_namespace_dyn(catalog.as_ref(), &ns_two).await;

    catalog.create_namespace(&ns_one, HashMap::new()).await?;
    catalog.create_namespace(&ns_two, HashMap::new()).await?;

    let namespaces = catalog.list_namespaces(None).await?;
    assert!(namespaces.contains(&ns_one));
    assert!(namespaces.contains(&ns_two));

    Ok(())
}

// Common behavior: creating an existing namespace should error.
#[rstest]
#[case::rest_catalog(CatalogKind::Rest)]
#[case::glue_catalog(CatalogKind::Glue)]
#[case::hms_catalog(CatalogKind::Hms)]
#[case::sql_catalog(CatalogKind::Sql)]
#[case::s3tables_catalog(CatalogKind::S3Tables)]
#[case::memory_catalog(CatalogKind::Memory)]
#[tokio::test]
async fn test_catalog_create_namespace_duplicate_fails(#[case] kind: CatalogKind) -> Result<()> {
    let Some(harness) = load_catalog(kind).await else {
        return Ok(());
    };
    let catalog = harness.catalog;
    let namespace = NamespaceIdent::new(normalize_test_name_with_parts!(
        "catalog_create_namespace_duplicate_fails",
        harness.label
    ));

    cleanup_namespace_dyn(catalog.as_ref(), &namespace).await;
    catalog.create_namespace(&namespace, HashMap::new()).await?;

    let err = catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NamespaceAlreadyExists);
    Ok(())
}

// Common behavior: update on a missing namespace should return NamespaceNotFound.
#[rstest]
#[case::glue_catalog(CatalogKind::Glue)]
#[case::hms_catalog(CatalogKind::Hms)]
#[case::sql_catalog(CatalogKind::Sql)]
#[case::memory_catalog(CatalogKind::Memory)]
#[tokio::test]
async fn test_catalog_update_namespace_missing_errors(#[case] kind: CatalogKind) -> Result<()> {
    let Some(harness) = load_catalog(kind).await else {
        return Ok(());
    };
    let catalog = harness.catalog;
    let namespace = NamespaceIdent::new(normalize_test_name_with_parts!(
        "catalog_update_namespace_missing_errors",
        harness.label
    ));

    cleanup_namespace_dyn(catalog.as_ref(), &namespace).await;

    let err = catalog
        .update_namespace(
            &namespace,
            HashMap::from([("key".to_string(), "value".to_string())]),
        )
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NamespaceNotFound);

    Ok(())
}

// Common behavior: dropping a missing namespace should error.
#[rstest]
#[case::rest_catalog(CatalogKind::Rest)]
#[case::glue_catalog(CatalogKind::Glue)]
#[case::hms_catalog(CatalogKind::Hms)]
#[case::sql_catalog(CatalogKind::Sql)]
#[case::s3tables_catalog(CatalogKind::S3Tables)]
#[case::memory_catalog(CatalogKind::Memory)]
#[tokio::test]
async fn test_catalog_drop_namespace_missing_errors(#[case] kind: CatalogKind) -> Result<()> {
    let Some(harness) = load_catalog(kind).await else {
        return Ok(());
    };
    let catalog = harness.catalog;
    let namespace = NamespaceIdent::new(normalize_test_name_with_parts!(
        "catalog_drop_namespace_missing_errors",
        harness.label
    ));

    cleanup_namespace_dyn(catalog.as_ref(), &namespace).await;

    let err = catalog.drop_namespace(&namespace).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NamespaceNotFound);
    Ok(())
}
