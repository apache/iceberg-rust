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

//! Integration tests for glue catalog.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::RwLock;

use ctor::{ctor, dtor};
use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, Namespace, NamespaceIdent, Result, TableCreation, TableIdent};
use iceberg_catalog_glue::{
    GlueCatalog, GlueCatalogConfig, AWS_ACCESS_KEY_ID, AWS_REGION_NAME, AWS_SECRET_ACCESS_KEY,
};
use iceberg_test_utils::docker::DockerCompose;
use iceberg_test_utils::{normalize_test_name, set_up};
use port_scanner::scan_port_addr;
use tokio::time::sleep;

const GLUE_CATALOG_PORT: u16 = 5000;
const MINIO_PORT: u16 = 9000;
static DOCKER_COMPOSE_ENV: RwLock<Option<DockerCompose>> = RwLock::new(None);

#[ctor]
fn before_all() {
    let mut guard = DOCKER_COMPOSE_ENV.write().unwrap();
    let docker_compose = DockerCompose::new(
        normalize_test_name(module_path!()),
        format!("{}/testdata/glue_catalog", env!("CARGO_MANIFEST_DIR")),
    );
    docker_compose.up();
    guard.replace(docker_compose);
}

#[dtor]
fn after_all() {
    let mut guard = DOCKER_COMPOSE_ENV.write().unwrap();
    guard.take();
}

async fn get_catalog() -> GlueCatalog {
    set_up();

    let (glue_catalog_ip, minio_ip) = {
        let guard = DOCKER_COMPOSE_ENV.read().unwrap();
        let docker_compose = guard.as_ref().unwrap();
        (
            docker_compose.get_container_ip("moto"),
            docker_compose.get_container_ip("minio"),
        )
    };
    let glue_socket_addr = SocketAddr::new(glue_catalog_ip, GLUE_CATALOG_PORT);
    let minio_socket_addr = SocketAddr::new(minio_ip, MINIO_PORT);
    while !scan_port_addr(glue_socket_addr) {
        log::info!("Waiting for 1s glue catalog to ready...");
        sleep(std::time::Duration::from_millis(1000)).await;
    }

    let props = HashMap::from([
        (AWS_ACCESS_KEY_ID.to_string(), "my_access_id".to_string()),
        (
            AWS_SECRET_ACCESS_KEY.to_string(),
            "my_secret_key".to_string(),
        ),
        (AWS_REGION_NAME.to_string(), "us-east-1".to_string()),
        (
            S3_ENDPOINT.to_string(),
            format!("http://{}", minio_socket_addr),
        ),
        (S3_ACCESS_KEY_ID.to_string(), "admin".to_string()),
        (S3_SECRET_ACCESS_KEY.to_string(), "password".to_string()),
        (S3_REGION.to_string(), "us-east-1".to_string()),
    ]);

    let config = GlueCatalogConfig::builder()
        .uri(format!("http://{}", glue_socket_addr))
        .warehouse("s3a://warehouse/hive".to_string())
        .props(props.clone())
        .build();

    GlueCatalog::new(config).await.unwrap()
}

async fn set_test_namespace(catalog: &GlueCatalog, namespace: &NamespaceIdent) -> Result<()> {
    let properties = HashMap::new();
    catalog.create_namespace(namespace, properties).await?;

    Ok(())
}

fn set_table_creation(location: impl ToString, name: impl ToString) -> Result<TableCreation> {
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "foo", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "bar", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()?;

    let creation = TableCreation::builder()
        .location(location.to_string())
        .name(name.to_string())
        .properties(HashMap::new())
        .schema(schema)
        .build();

    Ok(creation)
}

#[tokio::test]
async fn test_rename_table() -> Result<()> {
    let catalog = get_catalog().await;
    let creation = set_table_creation("s3a://warehouse/hive", "my_table")?;
    let namespace = Namespace::new(NamespaceIdent::new("test_rename_table".into()));

    catalog
        .create_namespace(namespace.name(), HashMap::new())
        .await?;

    let table = catalog.create_table(namespace.name(), creation).await?;

    let dest = TableIdent::new(namespace.name().clone(), "my_table_rename".to_string());

    catalog.rename_table(table.identifier(), &dest).await?;

    let table = catalog.load_table(&dest).await?;
    assert_eq!(table.identifier(), &dest);

    let src = TableIdent::new(namespace.name().clone(), "my_table".to_string());

    let src_table_exists = catalog.table_exists(&src).await?;
    assert!(!src_table_exists);

    Ok(())
}

#[tokio::test]
async fn test_table_exists() -> Result<()> {
    let catalog = get_catalog().await;
    let creation = set_table_creation("s3a://warehouse/hive", "my_table")?;
    let namespace = Namespace::new(NamespaceIdent::new("test_table_exists".into()));

    catalog
        .create_namespace(namespace.name(), HashMap::new())
        .await?;

    let ident = TableIdent::new(namespace.name().clone(), "my_table".to_string());

    let exists = catalog.table_exists(&ident).await?;
    assert!(!exists);

    let table = catalog.create_table(namespace.name(), creation).await?;

    let exists = catalog.table_exists(table.identifier()).await?;

    assert!(exists);

    Ok(())
}

#[tokio::test]
async fn test_drop_table() -> Result<()> {
    let catalog = get_catalog().await;
    let creation = set_table_creation("s3a://warehouse/hive", "my_table")?;
    let namespace = Namespace::new(NamespaceIdent::new("test_drop_table".into()));

    catalog
        .create_namespace(namespace.name(), HashMap::new())
        .await?;

    let table = catalog.create_table(namespace.name(), creation).await?;

    catalog.drop_table(table.identifier()).await?;

    let result = catalog.table_exists(table.identifier()).await?;

    assert!(!result);

    Ok(())
}

#[tokio::test]
async fn test_load_table() -> Result<()> {
    let catalog = get_catalog().await;
    let creation = set_table_creation("s3a://warehouse/hive", "my_table")?;
    let namespace = Namespace::new(NamespaceIdent::new("test_load_table".into()));

    catalog
        .create_namespace(namespace.name(), HashMap::new())
        .await?;

    let expected = catalog.create_table(namespace.name(), creation).await?;

    let result = catalog
        .load_table(&TableIdent::new(
            namespace.name().clone(),
            "my_table".to_string(),
        ))
        .await?;

    assert_eq!(result.identifier(), expected.identifier());
    assert_eq!(result.metadata_location(), expected.metadata_location());
    assert_eq!(result.metadata(), expected.metadata());

    Ok(())
}

#[tokio::test]
async fn test_create_table() -> Result<()> {
    let catalog = get_catalog().await;
    let namespace = NamespaceIdent::new("test_create_table".to_string());
    set_test_namespace(&catalog, &namespace).await?;
    let creation = set_table_creation("s3a://warehouse/hive", "my_table")?;

    let result = catalog.create_table(&namespace, creation).await?;

    assert_eq!(result.identifier().name(), "my_table");
    assert!(result
        .metadata_location()
        .is_some_and(|location| location.starts_with("s3a://warehouse/hive/metadata/00000-")));
    assert!(
        catalog
            .file_io()
            .exists("s3a://warehouse/hive/metadata/")
            .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_list_tables() -> Result<()> {
    let catalog = get_catalog().await;
    let namespace = NamespaceIdent::new("test_list_tables".to_string());
    set_test_namespace(&catalog, &namespace).await?;

    let expected = vec![];
    let result = catalog.list_tables(&namespace).await?;

    assert_eq!(result, expected);

    Ok(())
}

#[tokio::test]
async fn test_drop_namespace() -> Result<()> {
    let catalog = get_catalog().await;
    let namespace = NamespaceIdent::new("test_drop_namespace".to_string());
    set_test_namespace(&catalog, &namespace).await?;

    let exists = catalog.namespace_exists(&namespace).await?;
    assert!(exists);

    catalog.drop_namespace(&namespace).await?;

    let exists = catalog.namespace_exists(&namespace).await?;
    assert!(!exists);

    Ok(())
}

#[tokio::test]
async fn test_update_namespace() -> Result<()> {
    let catalog = get_catalog().await;
    let namespace = NamespaceIdent::new("test_update_namespace".into());
    set_test_namespace(&catalog, &namespace).await?;

    let before_update = catalog.get_namespace(&namespace).await?;
    let before_update = before_update.properties().get("description");

    assert_eq!(before_update, None);

    let properties = HashMap::from([("description".to_string(), "my_update".to_string())]);

    catalog.update_namespace(&namespace, properties).await?;

    let after_update = catalog.get_namespace(&namespace).await?;
    let after_update = after_update.properties().get("description");

    assert_eq!(after_update, Some("my_update".to_string()).as_ref());

    Ok(())
}

#[tokio::test]
async fn test_namespace_exists() -> Result<()> {
    let catalog = get_catalog().await;

    let namespace = NamespaceIdent::new("test_namespace_exists".into());

    let exists = catalog.namespace_exists(&namespace).await?;
    assert!(!exists);

    set_test_namespace(&catalog, &namespace).await?;

    let exists = catalog.namespace_exists(&namespace).await?;
    assert!(exists);

    Ok(())
}

#[tokio::test]
async fn test_get_namespace() -> Result<()> {
    let catalog = get_catalog().await;

    let namespace = NamespaceIdent::new("test_get_namespace".into());

    let does_not_exist = catalog.get_namespace(&namespace).await;
    assert!(does_not_exist.is_err());

    set_test_namespace(&catalog, &namespace).await?;

    let result = catalog.get_namespace(&namespace).await?;
    let expected = Namespace::new(namespace);

    assert_eq!(result, expected);

    Ok(())
}

#[tokio::test]
async fn test_create_namespace() -> Result<()> {
    let catalog = get_catalog().await;

    let properties = HashMap::new();
    let namespace = NamespaceIdent::new("test_create_namespace".into());

    let expected = Namespace::new(namespace.clone());

    let result = catalog.create_namespace(&namespace, properties).await?;

    assert_eq!(result, expected);

    Ok(())
}

#[tokio::test]
async fn test_list_namespace() -> Result<()> {
    let catalog = get_catalog().await;

    let namespace = NamespaceIdent::new("test_list_namespace".to_string());
    set_test_namespace(&catalog, &namespace).await?;

    let result = catalog.list_namespaces(None).await?;
    assert!(result.contains(&namespace));

    let empty_result = catalog.list_namespaces(Some(&namespace)).await?;
    assert!(empty_result.is_empty());

    Ok(())
}
