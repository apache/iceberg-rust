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

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use ctor::{ctor, dtor};
use iceberg::spec::{FormatVersion, NestedField, PrimitiveType, Schema, Type};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{
    Catalog, CatalogBuilder, Namespace, NamespaceIdent, Result as IcebergResult, TableCreation,
    TableIdent,
};
use iceberg_catalog_rest::{
    CustomAuthenticator, REST_CATALOG_PROP_URI, RestCatalog, RestCatalogBuilder,
};
use iceberg_test_utils::docker::DockerCompose;
use iceberg_test_utils::{normalize_test_name, set_up};
use port_scanner::scan_port_addr;
use tokio::time::sleep;
use tracing::info;

const REST_CATALOG_PORT: u16 = 8181;
static DOCKER_COMPOSE_ENV: RwLock<Option<DockerCompose>> = RwLock::new(None);

#[ctor]
fn before_all() {
    let mut guard = DOCKER_COMPOSE_ENV.write().unwrap();
    let docker_compose = DockerCompose::new(
        normalize_test_name(module_path!()),
        format!("{}/testdata/rest_catalog", env!("CARGO_MANIFEST_DIR")),
    );
    docker_compose.up();
    guard.replace(docker_compose);
}

#[dtor]
fn after_all() {
    let mut guard = DOCKER_COMPOSE_ENV.write().unwrap();
    guard.take();
}

async fn get_catalog(authenticator: Option<Arc<dyn CustomAuthenticator>>) -> RestCatalog {
    set_up();

    let rest_catalog_ip = {
        let guard = DOCKER_COMPOSE_ENV.read().unwrap();
        let docker_compose = guard.as_ref().unwrap();
        docker_compose.get_container_ip("rest")
    };

    let rest_socket_addr = SocketAddr::new(rest_catalog_ip, REST_CATALOG_PORT);
    while !scan_port_addr(rest_socket_addr) {
        info!("Waiting for 1s rest catalog to ready...");
        sleep(std::time::Duration::from_millis(1000)).await;
    }

    let mut builder = RestCatalogBuilder::default();
    if let Some(auth) = authenticator {
        builder = builder.with_token_authenticator(auth);
    }

    builder
        .load(
            "rest",
            HashMap::from([(
                REST_CATALOG_PROP_URI.to_string(),
                format!("http://{rest_socket_addr}"),
            )]),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn test_get_non_exist_namespace() {
    let catalog = get_catalog(None).await;

    let result = catalog
        .get_namespace(&NamespaceIdent::from_strs(["test_get_non_exist_namespace"]).unwrap())
        .await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("does not exist"));
}

#[tokio::test]
async fn test_get_namespace() {
    let catalog = get_catalog(None).await;

    let ns = Namespace::with_properties(
        NamespaceIdent::from_strs(["apple", "ios"]).unwrap(),
        HashMap::from([
            ("owner".to_string(), "ray".to_string()),
            ("community".to_string(), "apache".to_string()),
        ]),
    );

    // Verify that namespace doesn't exist
    assert!(catalog.get_namespace(ns.name()).await.is_err());

    // Create this namespace
    let created_ns = catalog
        .create_namespace(ns.name(), ns.properties().clone())
        .await
        .unwrap();

    assert_eq!(ns.name(), created_ns.name());
    assert_map_contains(ns.properties(), created_ns.properties());

    // Check that this namespace already exists
    let get_ns = catalog.get_namespace(ns.name()).await.unwrap();
    assert_eq!(ns.name(), get_ns.name());
    assert_map_contains(ns.properties(), created_ns.properties());
}

#[tokio::test]
async fn test_list_namespace() {
    let catalog = get_catalog(None).await;

    let ns1 = Namespace::with_properties(
        NamespaceIdent::from_strs(["test_list_namespace", "ios"]).unwrap(),
        HashMap::from([
            ("owner".to_string(), "ray".to_string()),
            ("community".to_string(), "apache".to_string()),
        ]),
    );

    let ns2 = Namespace::with_properties(
        NamespaceIdent::from_strs(["test_list_namespace", "macos"]).unwrap(),
        HashMap::from([
            ("owner".to_string(), "xuanwo".to_string()),
            ("community".to_string(), "apache".to_string()),
        ]),
    );

    // Currently this namespace doesn't exist, so it should return error.
    assert!(
        catalog
            .list_namespaces(Some(
                &NamespaceIdent::from_strs(["test_list_namespace"]).unwrap()
            ))
            .await
            .is_err()
    );

    // Create namespaces
    catalog
        .create_namespace(ns1.name(), ns1.properties().clone())
        .await
        .unwrap();
    catalog
        .create_namespace(ns2.name(), ns1.properties().clone())
        .await
        .unwrap();

    // List namespace
    let nss = catalog
        .list_namespaces(Some(
            &NamespaceIdent::from_strs(["test_list_namespace"]).unwrap(),
        ))
        .await
        .unwrap();

    assert!(nss.contains(ns1.name()));
    assert!(nss.contains(ns2.name()));
}

#[tokio::test]
async fn test_list_empty_namespace() {
    let catalog = get_catalog(None).await;

    let ns_apple = Namespace::with_properties(
        NamespaceIdent::from_strs(["test_list_empty_namespace", "apple"]).unwrap(),
        HashMap::from([
            ("owner".to_string(), "ray".to_string()),
            ("community".to_string(), "apache".to_string()),
        ]),
    );

    // Currently this namespace doesn't exist, so it should return error.
    assert!(
        catalog
            .list_namespaces(Some(ns_apple.name()))
            .await
            .is_err()
    );

    // Create namespaces
    catalog
        .create_namespace(ns_apple.name(), ns_apple.properties().clone())
        .await
        .unwrap();

    // List namespace
    let nss = catalog
        .list_namespaces(Some(ns_apple.name()))
        .await
        .unwrap();
    assert!(nss.is_empty());
}

#[tokio::test]
async fn test_list_root_namespace() {
    let catalog = get_catalog(None).await;

    let ns1 = Namespace::with_properties(
        NamespaceIdent::from_strs(["test_list_root_namespace", "apple", "ios"]).unwrap(),
        HashMap::from([
            ("owner".to_string(), "ray".to_string()),
            ("community".to_string(), "apache".to_string()),
        ]),
    );

    let ns2 = Namespace::with_properties(
        NamespaceIdent::from_strs(["test_list_root_namespace", "google", "android"]).unwrap(),
        HashMap::from([
            ("owner".to_string(), "xuanwo".to_string()),
            ("community".to_string(), "apache".to_string()),
        ]),
    );

    // Currently this namespace doesn't exist, so it should return error.
    assert!(
        catalog
            .list_namespaces(Some(
                &NamespaceIdent::from_strs(["test_list_root_namespace"]).unwrap()
            ))
            .await
            .is_err()
    );

    // Create namespaces
    catalog
        .create_namespace(ns1.name(), ns1.properties().clone())
        .await
        .unwrap();
    catalog
        .create_namespace(ns2.name(), ns1.properties().clone())
        .await
        .unwrap();

    // List namespace
    let nss = catalog.list_namespaces(None).await.unwrap();
    assert!(nss.contains(&NamespaceIdent::from_strs(["test_list_root_namespace"]).unwrap()));
}

#[tokio::test]
async fn test_create_table() {
    let catalog = get_catalog(None).await;

    let ns = Namespace::with_properties(
        NamespaceIdent::from_strs(["test_create_table", "apple", "ios"]).unwrap(),
        HashMap::from([
            ("owner".to_string(), "ray".to_string()),
            ("community".to_string(), "apache".to_string()),
        ]),
    );

    // Create namespaces
    catalog
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

    let table = catalog
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
    assert!(table.metadata().default_sort_order().is_unsorted());
    assert!(table.metadata().default_partition_spec().is_unpartitioned());
}

#[tokio::test]
async fn test_update_table() {
    let catalog = get_catalog(None).await;

    let ns = Namespace::with_properties(
        NamespaceIdent::from_strs(["test_update_table", "apple", "ios"]).unwrap(),
        HashMap::from([
            ("owner".to_string(), "ray".to_string()),
            ("community".to_string(), "apache".to_string()),
        ]),
    );

    // Create namespaces
    catalog
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

    let table = catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    assert_eq!(
        table.identifier(),
        &TableIdent::new(ns.name().clone(), "t1".to_string())
    );

    let tx = Transaction::new(&table);
    // Update table by committing transaction
    let table2 = tx
        .update_table_properties()
        .set("prop1".to_string(), "v1".to_string())
        .apply(tx)
        .unwrap()
        .commit(&catalog)
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

#[tokio::test]
async fn test_list_empty_multi_level_namespace() {
    let catalog = get_catalog(None).await;

    let ns_apple = Namespace::with_properties(
        NamespaceIdent::from_strs(["test_list_empty_multi_level_namespace", "a_a", "apple"])
            .unwrap(),
        HashMap::from([
            ("owner".to_string(), "ray".to_string()),
            ("community".to_string(), "apache".to_string()),
        ]),
    );

    // Currently this namespace doesn't exist, so it should return error.
    assert!(
        catalog
            .list_namespaces(Some(ns_apple.name()))
            .await
            .is_err()
    );

    // Create namespaces
    catalog
        .create_namespace(ns_apple.name(), ns_apple.properties().clone())
        .await
        .unwrap();

    // List namespace
    let nss = catalog
        .list_namespaces(Some(
            &NamespaceIdent::from_strs(["test_list_empty_multi_level_namespace", "a_a", "apple"])
                .unwrap(),
        ))
        .await
        .unwrap();
    assert!(nss.is_empty());
}

#[tokio::test]
async fn test_register_table() {
    let catalog = get_catalog(None).await;

    // Create namespace
    let ns = NamespaceIdent::from_strs(["ns"]).unwrap();
    catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

    // Create the table, store the metadata location, drop the table
    let empty_schema = Schema::builder().build().unwrap();
    let table_creation = TableCreation::builder()
        .name("t1".to_string())
        .schema(empty_schema)
        .build();

    let table = catalog.create_table(&ns, table_creation).await.unwrap();

    let metadata_location = table.metadata_location().unwrap();
    catalog.drop_table(table.identifier()).await.unwrap();

    let new_table_identifier = TableIdent::from_strs(["ns", "t2"]).unwrap();
    let table_registered = catalog
        .register_table(&new_table_identifier, metadata_location.to_string())
        .await
        .unwrap();

    assert_eq!(
        table.metadata_location(),
        table_registered.metadata_location()
    );
    assert_ne!(
        table.identifier().to_string(),
        table_registered.identifier().to_string()
    );
}

#[derive(Debug)]
struct CountingAuthenticator {
    count: Arc<Mutex<usize>>,
}

#[async_trait]
impl CustomAuthenticator for CountingAuthenticator {
    async fn get_token(&self) -> IcebergResult<String> {
        let mut c = self.count.lock().unwrap();
        *c += 1;
        // Return a unique token each time to ensure dynamic generation
        Ok(format!("token_{}", *c))
    }
}

#[tokio::test]
async fn test_authenticator_token_refresh() {
    // Track how many times tokens were requested
    let token_request_count = Arc::new(Mutex::new(0));
    let token_request_count_clone = token_request_count.clone();

    let authenticator = Arc::new(CountingAuthenticator {
        count: token_request_count_clone,
    });

    let catalog_with_auth = get_catalog(Some(authenticator)).await;

    // Perform multiple operations that should trigger token requests
    let ns1 = Namespace::with_properties(
        NamespaceIdent::from_strs(["test_refresh_1"]).unwrap(),
        HashMap::new(),
    );
    catalog_with_auth
        .create_namespace(ns1.name(), HashMap::new())
        .await
        .unwrap();

    let ns2 = Namespace::with_properties(
        NamespaceIdent::from_strs(["test_refresh_2"]).unwrap(),
        HashMap::new(),
    );
    catalog_with_auth
        .create_namespace(ns2.name(), HashMap::new())
        .await
        .unwrap();

    // Verify authenticator was called multiple times
    let count = *token_request_count.lock().unwrap();
    assert!(
        count >= 2,
        "Authenticator should have been called at least twice, but was called {} times",
        count
    );
}

#[tokio::test]
async fn test_authenticator_persists_across_operations() {
    let operation_count = Arc::new(Mutex::new(0));
    let operation_count_clone = operation_count.clone();

    let authenticator = Arc::new(CountingAuthenticator {
        count: operation_count_clone,
    });

    let catalog_with_auth = get_catalog(Some(authenticator)).await;

    // Create a namespace
    let ns = Namespace::with_properties(
        NamespaceIdent::from_strs(["test_persist", "auth"]).unwrap(),
        HashMap::new(),
    );
    catalog_with_auth
        .create_namespace(ns.name(), HashMap::new())
        .await
        .unwrap();

    let count_after_create = *operation_count.lock().unwrap();

    // List the namespace children (should use the same authenticator)
    // We need to list children of "test_persist" to find "auth"
    let list_result = catalog_with_auth
        .list_namespaces(Some(&NamespaceIdent::from_strs(["test_persist"]).unwrap()))
        .await
        .unwrap();
    assert!(
        list_result.contains(&NamespaceIdent::from_strs(["test_persist", "auth"]).unwrap()),
        "Namespace {:?} not found in list {:?}",
        ns.name(),
        list_result
    );

    let count_after_list = *operation_count.lock().unwrap();

    // Verify authenticator was used for both operations
    assert!(
        count_after_create > 0,
        "Authenticator should be used for create"
    );
    assert!(
        count_after_list > count_after_create,
        "Authenticator should be used for list operation too"
    );
}
