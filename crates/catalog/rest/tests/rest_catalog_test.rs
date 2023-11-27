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

//! Integration tests for rest catalog.

use iceberg::spec::{FormatVersion, NestedField, PrimitiveType, Schema, Type};
use iceberg::transaction::Transaction;
use iceberg::{Catalog, Namespace, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use iceberg_test_utils::docker::DockerCompose;
use iceberg_test_utils::{normalize_test_name, set_up};
use port_scanner::scan_port_addr;
use std::collections::HashMap;
use tokio::time::sleep;

const REST_CATALOG_PORT: u16 = 8181;

struct TestFixture {
    _docker_compose: DockerCompose,
    rest_catalog: RestCatalog,
}

async fn set_test_fixture(func: &str) -> TestFixture {
    set_up();
    let docker_compose = DockerCompose::new(
        normalize_test_name(format!("{}_{func}", module_path!())),
        format!("{}/testdata/rest_catalog", env!("CARGO_MANIFEST_DIR")),
    );

    // Start docker compose
    docker_compose.run();

    let rest_catalog_ip = docker_compose.get_container_ip("rest");

    let read_port = format!("{}:{}", rest_catalog_ip, REST_CATALOG_PORT);
    loop {
        if !scan_port_addr(&read_port) {
            log::info!("Waiting for 1s rest catalog to ready...");
            sleep(std::time::Duration::from_millis(1000)).await;
        } else {
            break;
        }
    }

    let config = RestCatalogConfig::builder()
        .uri(format!("http://{}:{}", rest_catalog_ip, REST_CATALOG_PORT))
        .build();
    let rest_catalog = RestCatalog::new(config).await.unwrap();

    TestFixture {
        _docker_compose: docker_compose,
        rest_catalog,
    }
}
#[tokio::test]
async fn test_get_non_exist_namespace() {
    let fixture = set_test_fixture("test_get_non_exist_namespace").await;

    let result = fixture
        .rest_catalog
        .get_namespace(&NamespaceIdent::from_strs(["demo"]).unwrap())
        .await;

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Namespace does not exist"));
}

#[tokio::test]
async fn test_get_namespace() {
    let fixture = set_test_fixture("test_get_namespace").await;

    let ns = Namespace::with_properties(
        NamespaceIdent::from_strs(["apple", "ios"]).unwrap(),
        HashMap::from([
            ("owner".to_string(), "ray".to_string()),
            ("community".to_string(), "apache".to_string()),
        ]),
    );

    // Verify that namespace doesn't exist
    assert!(fixture.rest_catalog.get_namespace(ns.name()).await.is_err());

    // Create this namespace
    let created_ns = fixture
        .rest_catalog
        .create_namespace(ns.name(), ns.properties().clone())
        .await
        .unwrap();

    assert_eq!(ns.name(), created_ns.name());
    assert_map_contains(ns.properties(), created_ns.properties());

    // Check that this namespace already exists
    let get_ns = fixture.rest_catalog.get_namespace(ns.name()).await.unwrap();
    assert_eq!(ns.name(), get_ns.name());
    assert_map_contains(ns.properties(), created_ns.properties());
}

#[tokio::test]
async fn test_list_namespace() {
    let fixture = set_test_fixture("test_list_namespace").await;

    let ns1 = Namespace::with_properties(
        NamespaceIdent::from_strs(["apple", "ios"]).unwrap(),
        HashMap::from([
            ("owner".to_string(), "ray".to_string()),
            ("community".to_string(), "apache".to_string()),
        ]),
    );

    let ns2 = Namespace::with_properties(
        NamespaceIdent::from_strs(["apple", "macos"]).unwrap(),
        HashMap::from([
            ("owner".to_string(), "xuanwo".to_string()),
            ("community".to_string(), "apache".to_string()),
        ]),
    );

    // Currently this namespace doesn't exist, so it should return error.
    assert!(fixture
        .rest_catalog
        .list_namespaces(Some(&NamespaceIdent::from_strs(["apple"]).unwrap()))
        .await
        .is_err());

    // Create namespaces
    fixture
        .rest_catalog
        .create_namespace(ns1.name(), ns1.properties().clone())
        .await
        .unwrap();
    fixture
        .rest_catalog
        .create_namespace(ns2.name(), ns1.properties().clone())
        .await
        .unwrap();

    // List namespace
    let mut nss = fixture
        .rest_catalog
        .list_namespaces(Some(&NamespaceIdent::from_strs(["apple"]).unwrap()))
        .await
        .unwrap();
    nss.sort();

    assert_eq!(&nss[0], ns1.name());
    assert_eq!(&nss[1], ns2.name());
}

#[tokio::test]
async fn test_list_empty_namespace() {
    let fixture = set_test_fixture("test_list_empty_namespace").await;

    let ns_apple = Namespace::with_properties(
        NamespaceIdent::from_strs(["apple"]).unwrap(),
        HashMap::from([
            ("owner".to_string(), "ray".to_string()),
            ("community".to_string(), "apache".to_string()),
        ]),
    );

    // Currently this namespace doesn't exist, so it should return error.
    assert!(fixture
        .rest_catalog
        .list_namespaces(Some(ns_apple.name()))
        .await
        .is_err());

    // Create namespaces
    fixture
        .rest_catalog
        .create_namespace(ns_apple.name(), ns_apple.properties().clone())
        .await
        .unwrap();

    // List namespace
    let nss = fixture
        .rest_catalog
        .list_namespaces(Some(&NamespaceIdent::from_strs(["apple"]).unwrap()))
        .await
        .unwrap();
    assert!(nss.is_empty());
}

#[tokio::test]
async fn test_list_root_namespace() {
    let fixture = set_test_fixture("test_list_root_namespace").await;

    let ns1 = Namespace::with_properties(
        NamespaceIdent::from_strs(["apple", "ios"]).unwrap(),
        HashMap::from([
            ("owner".to_string(), "ray".to_string()),
            ("community".to_string(), "apache".to_string()),
        ]),
    );

    let ns2 = Namespace::with_properties(
        NamespaceIdent::from_strs(["google", "android"]).unwrap(),
        HashMap::from([
            ("owner".to_string(), "xuanwo".to_string()),
            ("community".to_string(), "apache".to_string()),
        ]),
    );

    // Currently this namespace doesn't exist, so it should return error.
    assert!(fixture
        .rest_catalog
        .list_namespaces(Some(&NamespaceIdent::from_strs(["apple"]).unwrap()))
        .await
        .is_err());

    // Create namespaces
    fixture
        .rest_catalog
        .create_namespace(ns1.name(), ns1.properties().clone())
        .await
        .unwrap();
    fixture
        .rest_catalog
        .create_namespace(ns2.name(), ns1.properties().clone())
        .await
        .unwrap();

    // List namespace
    let mut nss = fixture.rest_catalog.list_namespaces(None).await.unwrap();
    nss.sort();

    assert_eq!(&nss[0], &NamespaceIdent::from_strs(["apple"]).unwrap());
    assert_eq!(&nss[1], &NamespaceIdent::from_strs(["google"]).unwrap());
}

#[tokio::test]
async fn test_create_table() {
    let fixture = set_test_fixture("test_create_table").await;

    let ns = Namespace::with_properties(
        NamespaceIdent::from_strs(["apple", "ios"]).unwrap(),
        HashMap::from([
            ("owner".to_string(), "ray".to_string()),
            ("community".to_string(), "apache".to_string()),
        ]),
    );

    // Create namespaces
    fixture
        .rest_catalog
        .create_namespace(ns.name(), ns.properties().clone())
        .await
        .unwrap();

    let schema = Schema::builder()
        .with_schema_id(1)
        .with_identifier_field_ids(vec![2])
        .with_fields(vec![
            NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
        ])
        .build()
        .unwrap();

    let table_creation = TableCreation::builder()
        .name("t1".to_string())
        .schema(schema.clone())
        .build();

    let table = fixture
        .rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    assert_eq!(
        table.identifier(),
        &TableIdent::new(ns.name().clone(), "t1".to_string())
    );

    assert_eq!(
        table.metadata().current_schema().as_struct(),
        schema.as_struct()
    );
    assert_eq!(table.metadata().format_version(), FormatVersion::V2);
    assert!(table.metadata().current_snapshot().is_none());
    assert!(table.metadata().history().is_empty());
    assert!(table.metadata().default_sort_order().unwrap().is_unsorted());
    assert!(table
        .metadata()
        .default_partition_spec()
        .unwrap()
        .is_unpartitioned());
}

#[tokio::test]
async fn test_update_table() {
    let fixture = set_test_fixture("test_update_table").await;

    let ns = Namespace::with_properties(
        NamespaceIdent::from_strs(["apple", "ios"]).unwrap(),
        HashMap::from([
            ("owner".to_string(), "ray".to_string()),
            ("community".to_string(), "apache".to_string()),
        ]),
    );

    // Create namespaces
    fixture
        .rest_catalog
        .create_namespace(ns.name(), ns.properties().clone())
        .await
        .unwrap();

    let schema = Schema::builder()
        .with_schema_id(1)
        .with_identifier_field_ids(vec![2])
        .with_fields(vec![
            NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
        ])
        .build()
        .unwrap();

    // Now we create a table
    let table_creation = TableCreation::builder()
        .name("t1".to_string())
        .schema(schema.clone())
        .build();

    let table = fixture
        .rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    assert_eq!(
        table.identifier(),
        &TableIdent::new(ns.name().clone(), "t1".to_string())
    );

    // Update table by committing transaction
    let table2 = Transaction::new(&table)
        .set_properties(HashMap::from([("prop1".to_string(), "v1".to_string())]))
        .unwrap()
        .commit(&fixture.rest_catalog)
        .await
        .unwrap();

    assert_map_contains(
        &HashMap::from([("prop1".to_string(), "v1".to_string())]),
        table2.metadata().properties(),
    );
}

fn assert_map_contains(map1: &HashMap<String, String>, map2: &HashMap<String, String>) {
    for (k, v) in map1 {
        assert!(map2.contains_key(k));
        assert_eq!(map2.get(k).unwrap(), v);
    }
}
