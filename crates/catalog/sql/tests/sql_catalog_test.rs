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

//! Integration tests for SQL catalog against PostgreSQL and MySQL backends.
//!
//! These tests assume Docker containers are started externally via `make docker-up`.
//! Each test uses unique namespaces based on module path to avoid conflicts.

use std::collections::HashMap;

use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_sql::{
    SQL_CATALOG_PROP_BIND_STYLE, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlBindStyle,
    SqlCatalogBuilder,
};
use iceberg_test_utils::{
    cleanup_namespace, get_mysql_endpoint, get_postgres_endpoint, normalize_test_name_with_parts,
    set_up,
};
use tokio::time::sleep;
use tracing::info;

async fn get_postgres_catalog(warehouse_location: &str) -> impl Catalog {
    set_up();

    let postgres_uri = get_postgres_endpoint();

    let mut retries = 0;
    let catalog = loop {
        match SqlCatalogBuilder::default()
            .load(
                "test_postgres",
                HashMap::from([
                    (SQL_CATALOG_PROP_URI.to_string(), postgres_uri.clone()),
                    (
                        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
                        warehouse_location.to_string(),
                    ),
                    (
                        SQL_CATALOG_PROP_BIND_STYLE.to_string(),
                        SqlBindStyle::DollarNumeric.to_string(),
                    ),
                ]),
            )
            .await
        {
            Ok(catalog) => {
                info!("PostgreSQL catalog is ready");
                break catalog;
            }
            Err(e) => {
                retries += 1;
                if retries >= 30 {
                    panic!("Failed to connect to PostgreSQL after {retries} retries: {e}");
                }
                info!("Waiting for PostgreSQL to be ready... (attempt {retries}): {e}");
                sleep(std::time::Duration::from_millis(1000)).await;
            }
        }
    };

    catalog
}

async fn get_mysql_catalog(warehouse_location: &str) -> impl Catalog {
    set_up();

    let mysql_uri = get_mysql_endpoint();

    let mut retries = 0;
    let catalog = loop {
        match SqlCatalogBuilder::default()
            .load(
                "test_mysql",
                HashMap::from([
                    (SQL_CATALOG_PROP_URI.to_string(), mysql_uri.clone()),
                    (
                        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
                        warehouse_location.to_string(),
                    ),
                    (
                        SQL_CATALOG_PROP_BIND_STYLE.to_string(),
                        SqlBindStyle::QMark.to_string(),
                    ),
                ]),
            )
            .await
        {
            Ok(catalog) => {
                info!("MySQL catalog is ready");
                break catalog;
            }
            Err(e) => {
                retries += 1;
                if retries >= 30 {
                    panic!("Failed to connect to MySQL after {retries} retries: {e}");
                }
                info!("Waiting for MySQL to be ready... (attempt {retries}): {e}");
                sleep(std::time::Duration::from_millis(1000)).await;
            }
        }
    };

    catalog
}

fn simple_table_schema() -> Schema {
    Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "foo", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "bar", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
        ])
        .build()
        .unwrap()
}

/// Generates a complete set of SQL catalog integration tests against a specific backend.
///
/// Each generated module contains tests for namespace CRUD, table CRUD, and table updates
/// that execute against the real database backend via Docker.
macro_rules! sql_catalog_tests {
    ($mod_name:ident, $get_catalog:ident) => {
        mod $mod_name {
            use super::*;

            #[tokio::test]
            async fn test_create_namespace() {
                let warehouse = normalize_test_name_with_parts!("test_create_namespace");
                let catalog = $get_catalog(&warehouse).await;
                let ns =
                    NamespaceIdent::new(normalize_test_name_with_parts!("test_create_namespace"));
                cleanup_namespace(&catalog, &ns).await;

                let created = catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

                assert_eq!(created.name(), &ns);
                assert!(catalog.namespace_exists(&ns).await.unwrap());
            }

            #[tokio::test]
            async fn test_create_namespace_with_properties() {
                let warehouse =
                    normalize_test_name_with_parts!("test_create_namespace_with_properties");
                let catalog = $get_catalog(&warehouse).await;
                let ns = NamespaceIdent::new(normalize_test_name_with_parts!(
                    "test_create_namespace_with_properties"
                ));
                cleanup_namespace(&catalog, &ns).await;

                let props = HashMap::from([
                    ("owner".to_string(), "test".to_string()),
                    ("env".to_string(), "ci".to_string()),
                ]);

                let created = catalog.create_namespace(&ns, props.clone()).await.unwrap();

                let ns_properties = created.properties();
                for (k, v) in &props {
                    assert_eq!(ns_properties.get(k), Some(v));
                }
            }

            #[tokio::test]
            async fn test_create_duplicate_namespace_fails() {
                let warehouse =
                    normalize_test_name_with_parts!("test_create_duplicate_namespace_fails");
                let catalog = $get_catalog(&warehouse).await;
                let ns = NamespaceIdent::new(normalize_test_name_with_parts!(
                    "test_create_duplicate_namespace_fails"
                ));
                cleanup_namespace(&catalog, &ns).await;

                catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

                let result = catalog.create_namespace(&ns, HashMap::new()).await;
                assert!(result.is_err());
            }

            #[tokio::test]
            async fn test_get_namespace() {
                let warehouse = normalize_test_name_with_parts!("test_get_namespace");
                let catalog = $get_catalog(&warehouse).await;
                let ns = NamespaceIdent::new(normalize_test_name_with_parts!("test_get_namespace"));
                cleanup_namespace(&catalog, &ns).await;

                catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

                let got = catalog.get_namespace(&ns).await.unwrap();
                assert_eq!(got.name(), &ns);
            }

            #[tokio::test]
            async fn test_get_non_existent_namespace_fails() {
                let warehouse =
                    normalize_test_name_with_parts!("test_get_non_existent_namespace_fails");
                let catalog = $get_catalog(&warehouse).await;
                let ns = NamespaceIdent::new(normalize_test_name_with_parts!(
                    "test_get_non_existent_namespace_fails"
                ));
                cleanup_namespace(&catalog, &ns).await;

                let result = catalog.get_namespace(&ns).await;
                assert!(result.is_err());
            }

            #[tokio::test]
            async fn test_namespace_exists() {
                let warehouse = normalize_test_name_with_parts!("test_namespace_exists");
                let catalog = $get_catalog(&warehouse).await;
                let ns =
                    NamespaceIdent::new(normalize_test_name_with_parts!("test_namespace_exists"));
                cleanup_namespace(&catalog, &ns).await;

                assert!(!catalog.namespace_exists(&ns).await.unwrap());

                catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

                assert!(catalog.namespace_exists(&ns).await.unwrap());
            }

            #[tokio::test]
            async fn test_list_namespaces() {
                let warehouse = normalize_test_name_with_parts!("test_list_namespaces");
                let catalog = $get_catalog(&warehouse).await;
                let ns1 = NamespaceIdent::new(normalize_test_name_with_parts!(
                    "test_list_namespaces",
                    "ns1"
                ));
                let ns2 = NamespaceIdent::new(normalize_test_name_with_parts!(
                    "test_list_namespaces",
                    "ns2"
                ));
                cleanup_namespace(&catalog, &ns1).await;
                cleanup_namespace(&catalog, &ns2).await;

                catalog
                    .create_namespace(&ns1, HashMap::new())
                    .await
                    .unwrap();
                catalog
                    .create_namespace(&ns2, HashMap::new())
                    .await
                    .unwrap();

                let namespaces = catalog.list_namespaces(None).await.unwrap();
                assert!(namespaces.contains(&ns1));
                assert!(namespaces.contains(&ns2));
            }

            #[tokio::test]
            async fn test_list_nested_namespaces() {
                let warehouse = normalize_test_name_with_parts!("test_list_nested_namespaces");
                let catalog = $get_catalog(&warehouse).await;
                let parent = NamespaceIdent::new(normalize_test_name_with_parts!(
                    "test_list_nested_namespaces",
                    "parent"
                ));
                let child = NamespaceIdent::from_strs(vec![
                    &normalize_test_name_with_parts!("test_list_nested_namespaces", "parent"),
                    "child",
                ])
                .unwrap();
                cleanup_namespace(&catalog, &child).await;
                cleanup_namespace(&catalog, &parent).await;

                catalog
                    .create_namespace(&parent, HashMap::new())
                    .await
                    .unwrap();
                catalog
                    .create_namespace(&child, HashMap::new())
                    .await
                    .unwrap();

                let children = catalog.list_namespaces(Some(&parent)).await.unwrap();
                assert!(children.contains(&child));

                let top_level = catalog.list_namespaces(None).await.unwrap();
                assert!(top_level.contains(&parent));
                assert!(!top_level.contains(&child));
            }

            #[tokio::test]
            async fn test_update_namespace() {
                let warehouse = normalize_test_name_with_parts!("test_update_namespace");
                let catalog = $get_catalog(&warehouse).await;
                let ns =
                    NamespaceIdent::new(normalize_test_name_with_parts!("test_update_namespace"));
                cleanup_namespace(&catalog, &ns).await;

                catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

                let update_props = HashMap::from([
                    ("key1".to_string(), "value1".to_string()),
                    ("key2".to_string(), "value2".to_string()),
                ]);
                catalog
                    .update_namespace(&ns, update_props.clone())
                    .await
                    .unwrap();

                let ns_loaded = catalog.get_namespace(&ns).await.unwrap();
                for (k, v) in &update_props {
                    assert_eq!(ns_loaded.properties().get(k), Some(v));
                }
            }

            #[tokio::test]
            async fn test_drop_namespace() {
                let warehouse = normalize_test_name_with_parts!("test_drop_namespace");
                let catalog = $get_catalog(&warehouse).await;
                let ns =
                    NamespaceIdent::new(normalize_test_name_with_parts!("test_drop_namespace"));
                cleanup_namespace(&catalog, &ns).await;

                catalog.create_namespace(&ns, HashMap::new()).await.unwrap();
                assert!(catalog.namespace_exists(&ns).await.unwrap());

                catalog.drop_namespace(&ns).await.unwrap();
                assert!(!catalog.namespace_exists(&ns).await.unwrap());
            }

            #[tokio::test]
            async fn test_drop_non_existent_namespace_fails() {
                let warehouse =
                    normalize_test_name_with_parts!("test_drop_non_existent_namespace_fails");
                let catalog = $get_catalog(&warehouse).await;
                let ns = NamespaceIdent::new(normalize_test_name_with_parts!(
                    "test_drop_non_existent_namespace_fails"
                ));
                cleanup_namespace(&catalog, &ns).await;

                let result = catalog.drop_namespace(&ns).await;
                assert!(result.is_err());
            }

            #[tokio::test]
            async fn test_create_table() {
                let warehouse = normalize_test_name_with_parts!("test_create_table");
                let catalog = $get_catalog(&warehouse).await;
                let ns = NamespaceIdent::new(normalize_test_name_with_parts!("test_create_table"));
                cleanup_namespace(&catalog, &ns).await;

                catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

                let schema = simple_table_schema();
                let table_creation = TableCreation::builder()
                    .name("test_tbl".to_string())
                    .schema(schema.clone())
                    .build();

                let table = catalog.create_table(&ns, table_creation).await.unwrap();

                assert_eq!(
                    table.identifier(),
                    &TableIdent::new(ns.clone(), "test_tbl".to_string())
                );
                assert_eq!(
                    table.metadata().current_schema().as_struct(),
                    schema.as_struct()
                );
                assert!(table.metadata().current_snapshot().is_none());
            }

            #[tokio::test]
            async fn test_create_duplicate_table_fails() {
                let warehouse =
                    normalize_test_name_with_parts!("test_create_duplicate_table_fails");
                let catalog = $get_catalog(&warehouse).await;
                let ns = NamespaceIdent::new(normalize_test_name_with_parts!(
                    "test_create_duplicate_table_fails"
                ));
                cleanup_namespace(&catalog, &ns).await;

                catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

                let table_creation = TableCreation::builder()
                    .name("dup_tbl".to_string())
                    .schema(simple_table_schema())
                    .build();

                catalog.create_table(&ns, table_creation).await.unwrap();

                let table_creation2 = TableCreation::builder()
                    .name("dup_tbl".to_string())
                    .schema(simple_table_schema())
                    .build();

                let result = catalog.create_table(&ns, table_creation2).await;
                assert!(result.is_err());
            }

            #[tokio::test]
            async fn test_list_tables() {
                let warehouse = normalize_test_name_with_parts!("test_list_tables");
                let catalog = $get_catalog(&warehouse).await;
                let ns = NamespaceIdent::new(normalize_test_name_with_parts!("test_list_tables"));
                cleanup_namespace(&catalog, &ns).await;

                catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

                let table_creation1 = TableCreation::builder()
                    .name("tbl_a".to_string())
                    .schema(simple_table_schema())
                    .build();
                let table_creation2 = TableCreation::builder()
                    .name("tbl_b".to_string())
                    .schema(simple_table_schema())
                    .build();

                catalog.create_table(&ns, table_creation1).await.unwrap();
                catalog.create_table(&ns, table_creation2).await.unwrap();

                let tables = catalog.list_tables(&ns).await.unwrap();
                assert_eq!(tables.len(), 2);
                assert!(tables.contains(&TableIdent::new(ns.clone(), "tbl_a".to_string())));
                assert!(tables.contains(&TableIdent::new(ns.clone(), "tbl_b".to_string())));
            }

            #[tokio::test]
            async fn test_list_tables_returns_empty() {
                let warehouse = normalize_test_name_with_parts!("test_list_tables_returns_empty");
                let catalog = $get_catalog(&warehouse).await;
                let ns = NamespaceIdent::new(normalize_test_name_with_parts!(
                    "test_list_tables_returns_empty"
                ));
                cleanup_namespace(&catalog, &ns).await;

                catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

                let tables = catalog.list_tables(&ns).await.unwrap();
                assert!(tables.is_empty());
            }

            #[tokio::test]
            async fn test_load_table() {
                let warehouse = normalize_test_name_with_parts!("test_load_table");
                let catalog = $get_catalog(&warehouse).await;
                let ns = NamespaceIdent::new(normalize_test_name_with_parts!("test_load_table"));
                cleanup_namespace(&catalog, &ns).await;

                catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

                let schema = simple_table_schema();
                let table_creation = TableCreation::builder()
                    .name("load_tbl".to_string())
                    .schema(schema.clone())
                    .build();

                catalog.create_table(&ns, table_creation).await.unwrap();

                let table_ident = TableIdent::new(ns.clone(), "load_tbl".to_string());
                let table = catalog.load_table(&table_ident).await.unwrap();

                assert_eq!(table.identifier(), &table_ident);
                assert_eq!(
                    table.metadata().current_schema().as_struct(),
                    schema.as_struct()
                );
                assert!(table.metadata_location().is_some());
            }

            #[tokio::test]
            async fn test_rename_table() {
                let warehouse = normalize_test_name_with_parts!("test_rename_table");
                let catalog = $get_catalog(&warehouse).await;
                let ns = NamespaceIdent::new(normalize_test_name_with_parts!("test_rename_table"));
                cleanup_namespace(&catalog, &ns).await;

                catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

                let table_creation = TableCreation::builder()
                    .name("rename_src".to_string())
                    .schema(simple_table_schema())
                    .build();

                catalog.create_table(&ns, table_creation).await.unwrap();

                let src = TableIdent::new(ns.clone(), "rename_src".to_string());
                let dst = TableIdent::new(ns.clone(), "rename_dst".to_string());

                catalog.rename_table(&src, &dst).await.unwrap();

                assert!(catalog.load_table(&dst).await.is_ok());
                assert!(catalog.load_table(&src).await.is_err());
            }

            #[tokio::test]
            async fn test_rename_table_across_namespaces() {
                let warehouse =
                    normalize_test_name_with_parts!("test_rename_table_across_namespaces");
                let catalog = $get_catalog(&warehouse).await;
                let ns1 = NamespaceIdent::new(normalize_test_name_with_parts!(
                    "test_rename_table_across_namespaces",
                    "ns1"
                ));
                let ns2 = NamespaceIdent::new(normalize_test_name_with_parts!(
                    "test_rename_table_across_namespaces",
                    "ns2"
                ));
                cleanup_namespace(&catalog, &ns1).await;
                cleanup_namespace(&catalog, &ns2).await;

                catalog
                    .create_namespace(&ns1, HashMap::new())
                    .await
                    .unwrap();
                catalog
                    .create_namespace(&ns2, HashMap::new())
                    .await
                    .unwrap();

                let table_creation = TableCreation::builder()
                    .name("cross_rename".to_string())
                    .schema(simple_table_schema())
                    .build();

                catalog.create_table(&ns1, table_creation).await.unwrap();

                let src = TableIdent::new(ns1.clone(), "cross_rename".to_string());
                let dst = TableIdent::new(ns2.clone(), "cross_rename".to_string());

                catalog.rename_table(&src, &dst).await.unwrap();

                assert!(catalog.load_table(&dst).await.is_ok());
                assert!(catalog.load_table(&src).await.is_err());
            }

            #[tokio::test]
            async fn test_drop_table() {
                let warehouse = normalize_test_name_with_parts!("test_drop_table");
                let catalog = $get_catalog(&warehouse).await;
                let ns = NamespaceIdent::new(normalize_test_name_with_parts!("test_drop_table"));
                cleanup_namespace(&catalog, &ns).await;

                catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

                let table_creation = TableCreation::builder()
                    .name("drop_tbl".to_string())
                    .schema(simple_table_schema())
                    .build();

                catalog.create_table(&ns, table_creation).await.unwrap();

                let table_ident = TableIdent::new(ns.clone(), "drop_tbl".to_string());
                catalog.drop_table(&table_ident).await.unwrap();

                assert!(catalog.load_table(&table_ident).await.is_err());
            }

            #[tokio::test]
            async fn test_drop_non_existent_table_fails() {
                let warehouse =
                    normalize_test_name_with_parts!("test_drop_non_existent_table_fails");
                let catalog = $get_catalog(&warehouse).await;
                let ns = NamespaceIdent::new(normalize_test_name_with_parts!(
                    "test_drop_non_existent_table_fails"
                ));
                cleanup_namespace(&catalog, &ns).await;

                catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

                let table_ident = TableIdent::new(ns.clone(), "no_such_table".to_string());
                let result = catalog.drop_table(&table_ident).await;
                assert!(result.is_err());
            }

            #[tokio::test]
            async fn test_table_exists() {
                let warehouse = normalize_test_name_with_parts!("test_table_exists");
                let catalog = $get_catalog(&warehouse).await;
                let ns = NamespaceIdent::new(normalize_test_name_with_parts!("test_table_exists"));
                cleanup_namespace(&catalog, &ns).await;

                catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

                let table_ident = TableIdent::new(ns.clone(), "exists_tbl".to_string());
                assert!(!catalog.table_exists(&table_ident).await.unwrap());

                let table_creation = TableCreation::builder()
                    .name("exists_tbl".to_string())
                    .schema(simple_table_schema())
                    .build();

                catalog.create_table(&ns, table_creation).await.unwrap();
                assert!(catalog.table_exists(&table_ident).await.unwrap());
            }

            #[tokio::test]
            async fn test_register_table() {
                let warehouse = normalize_test_name_with_parts!("test_register_table");
                let catalog = $get_catalog(&warehouse).await;
                let ns =
                    NamespaceIdent::new(normalize_test_name_with_parts!("test_register_table"));
                cleanup_namespace(&catalog, &ns).await;

                catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

                let table_creation = TableCreation::builder()
                    .name("reg_src".to_string())
                    .schema(simple_table_schema())
                    .build();

                let table = catalog.create_table(&ns, table_creation).await.unwrap();
                let metadata_location = table.metadata_location().unwrap().to_string();

                catalog.drop_table(table.identifier()).await.unwrap();

                let new_ident = TableIdent::new(ns.clone(), "reg_dst".to_string());
                let registered = catalog
                    .register_table(&new_ident, metadata_location.clone())
                    .await
                    .unwrap();

                assert_eq!(registered.identifier(), &new_ident);
                assert_eq!(
                    registered.metadata_location().unwrap(),
                    metadata_location.as_str()
                );
            }

            #[tokio::test]
            async fn test_update_table() {
                let warehouse = normalize_test_name_with_parts!("test_update_table");
                let catalog = $get_catalog(&warehouse).await;
                let ns = NamespaceIdent::new(normalize_test_name_with_parts!("test_update_table"));
                cleanup_namespace(&catalog, &ns).await;

                catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

                let table_creation = TableCreation::builder()
                    .name("update_tbl".to_string())
                    .schema(simple_table_schema())
                    .build();

                catalog.create_table(&ns, table_creation).await.unwrap();

                let table_ident = TableIdent::new(ns.clone(), "update_tbl".to_string());
                let table = catalog.load_table(&table_ident).await.unwrap();
                let original_metadata_location = table.metadata_location().unwrap().to_string();

                let tx = Transaction::new(&table);
                let tx = tx
                    .update_table_properties()
                    .set("test_key".to_string(), "test_value".to_string())
                    .apply(tx)
                    .unwrap();

                let updated = tx.commit(&catalog).await.unwrap();

                assert_eq!(
                    updated.metadata().properties().get("test_key"),
                    Some(&"test_value".to_string())
                );
                assert_ne!(
                    updated.metadata_location().unwrap(),
                    original_metadata_location.as_str()
                );

                let reloaded = catalog.load_table(&table_ident).await.unwrap();
                assert_eq!(
                    reloaded.metadata().properties().get("test_key"),
                    Some(&"test_value".to_string())
                );
                assert_eq!(reloaded.metadata_location(), updated.metadata_location());
            }
        }
    };
}

sql_catalog_tests!(postgres, get_postgres_catalog);
sql_catalog_tests!(mysql, get_mysql_catalog);
