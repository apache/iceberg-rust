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

//! Unified catalog integration tests.
//!
//! These assertions represent common catalog behavior that every implementation
//! should satisfy. Catalog-specific behaviors stay in their own crates.
//!
//! These tests assume Docker containers are started externally via `make docker-up`.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{
    Catalog, CatalogBuilder, ErrorKind, NamespaceIdent, Result, TableCreation, TableIdent,
};
use iceberg_catalog_glue::{
    AWS_ACCESS_KEY_ID, AWS_REGION_NAME, AWS_SECRET_ACCESS_KEY, GLUE_CATALOG_PROP_URI,
    GLUE_CATALOG_PROP_WAREHOUSE, GlueCatalog, GlueCatalogBuilder,
};
use iceberg_catalog_hms::{
    HMS_CATALOG_PROP_THRIFT_TRANSPORT, HMS_CATALOG_PROP_URI, HMS_CATALOG_PROP_WAREHOUSE,
    HmsCatalog, HmsCatalogBuilder, THRIFT_TRANSPORT_BUFFERED,
};
use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, RestCatalog, RestCatalogBuilder};
use iceberg_catalog_s3tables::{
    S3TABLES_CATALOG_PROP_ENDPOINT_URL, S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN,
    S3TablesCatalogBuilder,
};
use iceberg_catalog_sql::{
    SQL_CATALOG_PROP_BIND_STYLE, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlBindStyle,
    SqlCatalogBuilder,
};
use iceberg_test_utils::{
    get_glue_endpoint, get_hms_endpoint, get_minio_endpoint, get_rest_catalog_endpoint,
    normalize_test_name_with_parts, set_up,
};
use rstest::rstest;
use sqlx::migrate::MigrateDatabase;
use tempfile::TempDir;
use tokio::time::sleep;

#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
enum CatalogKind {
    Rest,
    Glue,
    Hms,
    Sql,
    S3Tables,
    Memory,
}

struct CatalogHarness {
    catalog: Arc<dyn Catalog>,
    label: &'static str,
    _tempdirs: Vec<TempDir>,
}

// Shared setup for each catalog implementation so the tests below exercise
// the same behavior against all backends.
async fn load_catalog(kind: CatalogKind) -> Option<CatalogHarness> {
    set_up();
    match kind {
        CatalogKind::Rest => Some(CatalogHarness {
            catalog: Arc::new(rest_catalog().await) as Arc<dyn Catalog>,
            label: "rest",
            _tempdirs: Vec::new(),
        }),
        CatalogKind::Glue => Some(CatalogHarness {
            catalog: Arc::new(glue_catalog().await) as Arc<dyn Catalog>,
            label: "glue",
            _tempdirs: Vec::new(),
        }),
        CatalogKind::Hms => Some(CatalogHarness {
            catalog: Arc::new(hms_catalog().await) as Arc<dyn Catalog>,
            label: "hms",
            _tempdirs: Vec::new(),
        }),
        CatalogKind::Sql => {
            let warehouse_dir = TempDir::new().unwrap();
            let db_dir = TempDir::new().unwrap();
            let db_path = db_dir.path().join("catalog.db");
            let db_uri = format!("sqlite:{}", db_path.to_str().unwrap());
            sqlx::Sqlite::create_database(&db_uri).await.unwrap();

            let catalog = SqlCatalogBuilder::default()
                .load(
                    "sql",
                    HashMap::from([
                        (SQL_CATALOG_PROP_URI.to_string(), db_uri),
                        (
                            SQL_CATALOG_PROP_WAREHOUSE.to_string(),
                            warehouse_dir.path().to_str().unwrap().to_string(),
                        ),
                        (
                            SQL_CATALOG_PROP_BIND_STYLE.to_string(),
                            SqlBindStyle::QMark.to_string(),
                        ),
                    ]),
                )
                .await
                .unwrap();

            Some(CatalogHarness {
                catalog: Arc::new(catalog) as Arc<dyn Catalog>,
                label: "sql",
                _tempdirs: vec![warehouse_dir, db_dir],
            })
        }
        CatalogKind::S3Tables => {
            let table_bucket_arn = match std::env::var("TABLE_BUCKET_ARN").ok() {
                Some(value) => value,
                None => return None,
            };

            let mut props = HashMap::from([(
                S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN.to_string(),
                table_bucket_arn,
            )]);

            if let Ok(endpoint_url) = std::env::var("S3TABLES_ENDPOINT_URL") {
                props.insert(S3TABLES_CATALOG_PROP_ENDPOINT_URL.to_string(), endpoint_url);
            }

            let catalog = S3TablesCatalogBuilder::default()
                .load("s3tables", props)
                .await
                .unwrap();

            Some(CatalogHarness {
                catalog: Arc::new(catalog) as Arc<dyn Catalog>,
                label: "s3tables",
                _tempdirs: Vec::new(),
            })
        }
        CatalogKind::Memory => {
            let warehouse_dir = TempDir::new().unwrap();
            let props = HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_string(),
                warehouse_dir.path().to_str().unwrap().to_string(),
            )]);
            let catalog = MemoryCatalogBuilder::default()
                .load("memory", props)
                .await
                .unwrap();

            Some(CatalogHarness {
                catalog: Arc::new(catalog) as Arc<dyn Catalog>,
                label: "memory",
                _tempdirs: vec![warehouse_dir],
            })
        }
    }
}

// Catalog-specific setup is intentionally isolated here so the test cases
// remain implementation-agnostic.
async fn rest_catalog() -> RestCatalog {
    let rest_endpoint = get_rest_catalog_endpoint();

    let client = reqwest::Client::new();
    let mut retries = 0;
    while retries < 30 {
        if client
            .get(format!("{rest_endpoint}/v1/config"))
            .send()
            .await
            .map(|resp| resp.status().is_success())
            .unwrap_or(false)
        {
            break;
        }
        sleep(std::time::Duration::from_millis(1000)).await;
        retries += 1;
    }

    RestCatalogBuilder::default()
        .load(
            "rest",
            HashMap::from([(REST_CATALOG_PROP_URI.to_string(), rest_endpoint)]),
        )
        .await
        .unwrap()
}

async fn glue_catalog() -> GlueCatalog {
    let glue_endpoint = get_glue_endpoint();
    let minio_endpoint = get_minio_endpoint();

    let props = HashMap::from([
        (AWS_ACCESS_KEY_ID.to_string(), "my_access_id".to_string()),
        (
            AWS_SECRET_ACCESS_KEY.to_string(),
            "my_secret_key".to_string(),
        ),
        (AWS_REGION_NAME.to_string(), "us-east-1".to_string()),
        (S3_ENDPOINT.to_string(), minio_endpoint),
        (S3_ACCESS_KEY_ID.to_string(), "admin".to_string()),
        (S3_SECRET_ACCESS_KEY.to_string(), "password".to_string()),
        (S3_REGION.to_string(), "us-east-1".to_string()),
    ]);

    let file_io = iceberg::io::FileIO::from_path("s3a://")
        .unwrap()
        .with_props(props.clone())
        .build()
        .unwrap();

    let mut retries = 0;
    while retries < 30 {
        if file_io.exists("s3a://warehouse/").await.unwrap_or(false) {
            break;
        }
        sleep(std::time::Duration::from_millis(1000)).await;
        retries += 1;
    }

    let mut glue_props = HashMap::from([
        (GLUE_CATALOG_PROP_URI.to_string(), glue_endpoint),
        (
            GLUE_CATALOG_PROP_WAREHOUSE.to_string(),
            "s3a://warehouse/hive".to_string(),
        ),
    ]);
    glue_props.extend(props);

    GlueCatalogBuilder::default()
        .load("glue", glue_props)
        .await
        .unwrap()
}

async fn hms_catalog() -> HmsCatalog {
    let hms_endpoint = get_hms_endpoint();
    let minio_endpoint = get_minio_endpoint();

    let props = HashMap::from([
        (HMS_CATALOG_PROP_URI.to_string(), hms_endpoint),
        (
            HMS_CATALOG_PROP_THRIFT_TRANSPORT.to_string(),
            THRIFT_TRANSPORT_BUFFERED.to_string(),
        ),
        (
            HMS_CATALOG_PROP_WAREHOUSE.to_string(),
            "s3a://warehouse/hive".to_string(),
        ),
        (S3_ENDPOINT.to_string(), minio_endpoint),
        (S3_ACCESS_KEY_ID.to_string(), "admin".to_string()),
        (S3_SECRET_ACCESS_KEY.to_string(), "password".to_string()),
        (S3_REGION.to_string(), "us-east-1".to_string()),
    ]);

    let file_io = iceberg::io::FileIO::from_path("s3a://")
        .unwrap()
        .with_props(props.clone())
        .build()
        .unwrap();

    let mut retries = 0;
    while retries < 30 {
        if file_io.exists("s3a://warehouse/").await.unwrap_or(false) {
            break;
        }
        sleep(std::time::Duration::from_millis(1000)).await;
        retries += 1;
    }

    HmsCatalogBuilder::default()
        .load("hms", props)
        .await
        .unwrap()
}

// Common table schema used across the suite to validate shared behavior.
fn table_creation(name: impl ToString) -> TableCreation {
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "foo", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "bar", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .unwrap();

    TableCreation::builder()
        .name(name.to_string())
        .properties(HashMap::new())
        .schema(schema)
        .build()
}

fn assert_map_contains(expected: &HashMap<String, String>, actual: &HashMap<String, String>) {
    for (key, value) in expected {
        assert_eq!(actual.get(key), Some(value));
    }
}

async fn cleanup_namespace_dyn(catalog: &dyn Catalog, namespace: &NamespaceIdent) {
    if let Ok(tables) = catalog.list_tables(namespace).await {
        for table in tables {
            let _ = catalog.drop_table(&table).await;
        }
    }
    let _ = catalog.drop_namespace(namespace).await;
}

// Common behavior: querying a missing namespace should error.
#[rstest]
#[tokio::test]
#[case::rest(CatalogKind::Rest)]
#[case::glue(CatalogKind::Glue)]
#[case::hms(CatalogKind::Hms)]
#[case::sql(CatalogKind::Sql)]
#[case::s3tables(CatalogKind::S3Tables)]
#[case::memory(CatalogKind::Memory)]
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

    assert!(catalog.get_namespace(&namespace).await.is_err());

    Ok(())
}

// Common behavior: namespace lifecycle CRUD (with optional update support).
#[rstest]
#[tokio::test]
#[case::rest(CatalogKind::Rest)]
#[case::glue(CatalogKind::Glue)]
#[case::hms(CatalogKind::Hms)]
#[case::sql(CatalogKind::Sql)]
#[case::s3tables(CatalogKind::S3Tables)]
#[case::memory(CatalogKind::Memory)]
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

    let updated_props = HashMap::from([("owner".to_string(), "updated".to_string())]);
    match catalog
        .update_namespace(&namespace, updated_props.clone())
        .await
    {
        Ok(()) => {
            let updated = catalog.get_namespace(&namespace).await?;
            assert_map_contains(&updated_props, updated.properties());
        }
        Err(err) if err.kind() == ErrorKind::FeatureUnsupported => {}
        Err(err) => return Err(err),
    }

    catalog.drop_namespace(&namespace).await?;
    assert!(!catalog.namespace_exists(&namespace).await?);

    Ok(())
}

// Common behavior: table lifecycle CRUD.
#[rstest]
#[tokio::test]
#[case::rest(CatalogKind::Rest)]
#[case::glue(CatalogKind::Glue)]
#[case::hms(CatalogKind::Hms)]
#[case::sql(CatalogKind::Sql)]
#[case::s3tables(CatalogKind::S3Tables)]
#[case::memory(CatalogKind::Memory)]
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

// Common behavior: listing namespaces under a parent returns its children.
#[rstest]
#[tokio::test]
#[case::rest(CatalogKind::Rest)]
#[case::glue(CatalogKind::Glue)]
#[case::hms(CatalogKind::Hms)]
#[case::sql(CatalogKind::Sql)]
#[case::s3tables(CatalogKind::S3Tables)]
#[case::memory(CatalogKind::Memory)]
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

// Common behavior: listing top-level namespaces includes created namespaces.
#[rstest]
#[tokio::test]
#[case::rest(CatalogKind::Rest)]
#[case::glue(CatalogKind::Glue)]
#[case::hms(CatalogKind::Hms)]
#[case::sql(CatalogKind::Sql)]
#[case::s3tables(CatalogKind::S3Tables)]
#[case::memory(CatalogKind::Memory)]
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
#[tokio::test]
#[case::rest(CatalogKind::Rest)]
#[case::glue(CatalogKind::Glue)]
#[case::hms(CatalogKind::Hms)]
#[case::sql(CatalogKind::Sql)]
#[case::s3tables(CatalogKind::S3Tables)]
#[case::memory(CatalogKind::Memory)]
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

    assert!(
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .is_err()
    );

    Ok(())
}

// Common behavior: update on a missing namespace should error (or be unsupported).
#[rstest]
#[tokio::test]
#[case::rest(CatalogKind::Rest)]
#[case::glue(CatalogKind::Glue)]
#[case::hms(CatalogKind::Hms)]
#[case::sql(CatalogKind::Sql)]
#[case::s3tables(CatalogKind::S3Tables)]
#[case::memory(CatalogKind::Memory)]
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

    match catalog
        .update_namespace(
            &namespace,
            HashMap::from([("key".to_string(), "value".to_string())]),
        )
        .await
    {
        Err(err) if err.kind() == ErrorKind::FeatureUnsupported => Ok(()),
        Err(_) => Ok(()),
        Ok(()) => Err(iceberg::Error::new(
            ErrorKind::Unexpected,
            "Expected update_namespace to fail for missing namespace",
        )),
    }
}

// Common behavior: dropping a missing namespace should error.
#[rstest]
#[tokio::test]
#[case::rest(CatalogKind::Rest)]
#[case::glue(CatalogKind::Glue)]
#[case::hms(CatalogKind::Hms)]
#[case::sql(CatalogKind::Sql)]
#[case::s3tables(CatalogKind::S3Tables)]
#[case::memory(CatalogKind::Memory)]
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

    assert!(catalog.drop_namespace(&namespace).await.is_err());

    Ok(())
}

// Common behavior: listing tables for a missing namespace should error.
#[rstest]
#[tokio::test]
#[case::rest(CatalogKind::Rest)]
#[case::glue(CatalogKind::Glue)]
#[case::hms(CatalogKind::Hms)]
#[case::sql(CatalogKind::Sql)]
#[case::s3tables(CatalogKind::S3Tables)]
#[case::memory(CatalogKind::Memory)]
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
#[tokio::test]
#[case::rest(CatalogKind::Rest)]
#[case::glue(CatalogKind::Glue)]
#[case::hms(CatalogKind::Hms)]
#[case::sql(CatalogKind::Sql)]
#[case::s3tables(CatalogKind::S3Tables)]
#[case::memory(CatalogKind::Memory)]
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
#[tokio::test]
#[case::rest(CatalogKind::Rest)]
#[case::glue(CatalogKind::Glue)]
#[case::hms(CatalogKind::Hms)]
#[case::sql(CatalogKind::Sql)]
#[case::s3tables(CatalogKind::S3Tables)]
#[case::memory(CatalogKind::Memory)]
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
#[rstest]
#[tokio::test]
#[case::rest(CatalogKind::Rest)]
#[case::glue(CatalogKind::Glue)]
#[case::hms(CatalogKind::Hms)]
#[case::sql(CatalogKind::Sql)]
#[case::s3tables(CatalogKind::S3Tables)]
#[case::memory(CatalogKind::Memory)]
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

// Common behavior: renaming across namespaces moves the table.
#[rstest]
#[tokio::test]
#[case::rest(CatalogKind::Rest)]
#[case::glue(CatalogKind::Glue)]
#[case::hms(CatalogKind::Hms)]
#[case::sql(CatalogKind::Sql)]
#[case::s3tables(CatalogKind::S3Tables)]
#[case::memory(CatalogKind::Memory)]
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

    match catalog.rename_table(&src_ident, &dst_ident).await {
        Ok(()) => {
            assert!(catalog.table_exists(&dst_ident).await?);
            assert!(!catalog.table_exists(&src_ident).await?);
        }
        Err(err) if err.kind() == ErrorKind::FeatureUnsupported => return Ok(()),
        Err(err) => return Err(err),
    }

    Ok(())
}

// Common behavior: renaming a missing table should error.
#[rstest]
#[tokio::test]
#[case::rest(CatalogKind::Rest)]
#[case::glue(CatalogKind::Glue)]
#[case::hms(CatalogKind::Hms)]
#[case::sql(CatalogKind::Sql)]
#[case::s3tables(CatalogKind::S3Tables)]
#[case::memory(CatalogKind::Memory)]
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

    match catalog.rename_table(&src_ident, &dst_ident).await {
        Err(err) if err.kind() == ErrorKind::FeatureUnsupported => Ok(()),
        Err(_) => Ok(()),
        Ok(()) => Err(iceberg::Error::new(
            ErrorKind::Unexpected,
            "Expected rename_table to fail for missing source table",
        )),
    }
}

// Common behavior: renaming to an existing destination should error.
#[rstest]
#[tokio::test]
#[case::rest(CatalogKind::Rest)]
#[case::glue(CatalogKind::Glue)]
#[case::hms(CatalogKind::Hms)]
#[case::sql(CatalogKind::Sql)]
#[case::s3tables(CatalogKind::S3Tables)]
#[case::memory(CatalogKind::Memory)]
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

    match catalog.rename_table(&src, &dst).await {
        Err(err) if err.kind() == ErrorKind::FeatureUnsupported => Ok(()),
        Err(_) => Ok(()),
        Ok(()) => Err(iceberg::Error::new(
            ErrorKind::Unexpected,
            "Expected rename_table to fail for existing destination table",
        )),
    }
}

// Common behavior: dropping a missing table should error.
#[rstest]
#[tokio::test]
#[case::rest(CatalogKind::Rest)]
#[case::glue(CatalogKind::Glue)]
#[case::hms(CatalogKind::Hms)]
#[case::sql(CatalogKind::Sql)]
#[case::s3tables(CatalogKind::S3Tables)]
#[case::memory(CatalogKind::Memory)]
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

// Common behavior: register_table rehydrates a dropped table (if supported).
#[rstest]
#[tokio::test]
#[case::rest(CatalogKind::Rest)]
#[case::glue(CatalogKind::Glue)]
#[case::hms(CatalogKind::Hms)]
#[case::sql(CatalogKind::Sql)]
#[case::s3tables(CatalogKind::S3Tables)]
#[case::memory(CatalogKind::Memory)]
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

    match catalog
        .register_table(&table_ident, metadata_location.clone())
        .await
    {
        Ok(registered) => {
            assert_eq!(registered.identifier(), &table_ident);
            assert_eq!(
                registered.metadata_location(),
                Some(metadata_location.as_str())
            );
        }
        Err(err) if err.kind() == ErrorKind::FeatureUnsupported => return Ok(()),
        Err(err) => return Err(err),
    }

    Ok(())
}

// Common behavior: registering a table with an existing name should error (if supported).
#[rstest]
#[tokio::test]
#[case::rest(CatalogKind::Rest)]
#[case::glue(CatalogKind::Glue)]
#[case::hms(CatalogKind::Hms)]
#[case::sql(CatalogKind::Sql)]
#[case::s3tables(CatalogKind::S3Tables)]
#[case::memory(CatalogKind::Memory)]
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

    match catalog
        .register_table(&table_ident, metadata_location)
        .await
    {
        Err(err) if err.kind() == ErrorKind::FeatureUnsupported => Ok(()),
        Err(_) => Ok(()),
        Ok(_) => Err(iceberg::Error::new(
            ErrorKind::Unexpected,
            "Expected register_table to fail for existing table",
        )),
    }
}
