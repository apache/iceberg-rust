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

#[derive(Debug)]
struct TestFixture {
    _docker_compose: DockerCompose,
    glue_catalog: GlueCatalog,
}

async fn set_test_fixture(func: &str) -> TestFixture {
    set_up();

    let docker_compose = DockerCompose::new(
        normalize_test_name(format!("{}_{func}", module_path!())),
        format!("{}/testdata/glue_catalog", env!("CARGO_MANIFEST_DIR")),
    );

    docker_compose.run();

    let glue_catalog_ip = docker_compose.get_container_ip("moto");
    let minio_ip = docker_compose.get_container_ip("minio");

    let read_port = format!("{}:{}", glue_catalog_ip, GLUE_CATALOG_PORT);
    loop {
        if !scan_port_addr(&read_port) {
            log::info!("Waiting for 1s glue catalog to ready...");
            sleep(std::time::Duration::from_millis(1000)).await;
        } else {
            break;
        }
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
            format!("http://{}:{}", minio_ip, MINIO_PORT),
        ),
        (S3_ACCESS_KEY_ID.to_string(), "admin".to_string()),
        (S3_SECRET_ACCESS_KEY.to_string(), "password".to_string()),
        (S3_REGION.to_string(), "us-east-1".to_string()),
    ]);

    let config = GlueCatalogConfig::builder()
        .uri(format!("http://{}:{}", glue_catalog_ip, GLUE_CATALOG_PORT))
        .warehouse("s3a://warehouse/hive".to_string())
        .props(props.clone())
        .build();

    let glue_catalog = GlueCatalog::new(config).await.unwrap();

    TestFixture {
        _docker_compose: docker_compose,
        glue_catalog,
    }
}

async fn set_test_namespace(fixture: &TestFixture, namespace: &NamespaceIdent) -> Result<()> {
    let properties = HashMap::new();

    fixture
        .glue_catalog
        .create_namespace(namespace, properties)
        .await?;

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
    let fixture = set_test_fixture("test_rename_table").await;
    let creation = set_table_creation("s3a://warehouse/hive", "my_table")?;
    let namespace = Namespace::new(NamespaceIdent::new("my_database".into()));

    fixture
        .glue_catalog
        .create_namespace(namespace.name(), HashMap::new())
        .await?;

    let table = fixture
        .glue_catalog
        .create_table(namespace.name(), creation)
        .await?;

    let dest = TableIdent::new(namespace.name().clone(), "my_table_rename".to_string());

    fixture
        .glue_catalog
        .rename_table(table.identifier(), &dest)
        .await?;

    let table = fixture.glue_catalog.load_table(&dest).await?;
    assert_eq!(table.identifier(), &dest);

    let src = TableIdent::new(namespace.name().clone(), "my_table".to_string());

    let src_table_exists = fixture.glue_catalog.table_exists(&src).await?;
    assert!(!src_table_exists);

    Ok(())
}

#[tokio::test]
async fn test_table_exists() -> Result<()> {
    let fixture = set_test_fixture("test_table_exists").await;
    let creation = set_table_creation("s3a://warehouse/hive", "my_table")?;
    let namespace = Namespace::new(NamespaceIdent::new("my_database".into()));

    fixture
        .glue_catalog
        .create_namespace(namespace.name(), HashMap::new())
        .await?;

    let ident = TableIdent::new(namespace.name().clone(), "my_table".to_string());

    let exists = fixture.glue_catalog.table_exists(&ident).await?;
    assert!(!exists);

    let table = fixture
        .glue_catalog
        .create_table(namespace.name(), creation)
        .await?;

    let exists = fixture
        .glue_catalog
        .table_exists(table.identifier())
        .await?;

    assert!(exists);

    Ok(())
}

#[tokio::test]
async fn test_drop_table() -> Result<()> {
    let fixture = set_test_fixture("test_drop_table").await;
    let creation = set_table_creation("s3a://warehouse/hive", "my_table")?;
    let namespace = Namespace::new(NamespaceIdent::new("my_database".into()));

    fixture
        .glue_catalog
        .create_namespace(namespace.name(), HashMap::new())
        .await?;

    let table = fixture
        .glue_catalog
        .create_table(namespace.name(), creation)
        .await?;

    fixture.glue_catalog.drop_table(table.identifier()).await?;

    let result = fixture
        .glue_catalog
        .table_exists(table.identifier())
        .await?;

    assert!(!result);

    Ok(())
}

#[tokio::test]
async fn test_load_table() -> Result<()> {
    let fixture = set_test_fixture("test_load_table").await;
    let creation = set_table_creation("s3a://warehouse/hive", "my_table")?;
    let namespace = Namespace::new(NamespaceIdent::new("my_database".into()));

    fixture
        .glue_catalog
        .create_namespace(namespace.name(), HashMap::new())
        .await?;

    let expected = fixture
        .glue_catalog
        .create_table(namespace.name(), creation)
        .await?;

    let result = fixture
        .glue_catalog
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
    let fixture = set_test_fixture("test_create_table").await;
    let namespace = NamespaceIdent::new("my_database".to_string());
    set_test_namespace(&fixture, &namespace).await?;
    let creation = set_table_creation("s3a://warehouse/hive", "my_table")?;

    let result = fixture
        .glue_catalog
        .create_table(&namespace, creation)
        .await?;

    assert_eq!(result.identifier().name(), "my_table");
    assert!(result
        .metadata_location()
        .is_some_and(|location| location.starts_with("s3a://warehouse/hive/metadata/00000-")));
    assert!(
        fixture
            .glue_catalog
            .file_io()
            .is_exist("s3a://warehouse/hive/metadata/")
            .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_list_tables() -> Result<()> {
    let fixture = set_test_fixture("test_list_tables").await;
    let namespace = NamespaceIdent::new("my_database".to_string());
    set_test_namespace(&fixture, &namespace).await?;

    let expected = vec![];
    let result = fixture.glue_catalog.list_tables(&namespace).await?;

    assert_eq!(result, expected);

    Ok(())
}

#[tokio::test]
async fn test_drop_namespace() -> Result<()> {
    let fixture = set_test_fixture("test_drop_namespace").await;
    let namespace = NamespaceIdent::new("my_database".to_string());
    set_test_namespace(&fixture, &namespace).await?;

    let exists = fixture.glue_catalog.namespace_exists(&namespace).await?;
    assert!(exists);

    fixture.glue_catalog.drop_namespace(&namespace).await?;

    let exists = fixture.glue_catalog.namespace_exists(&namespace).await?;
    assert!(!exists);

    Ok(())
}

#[tokio::test]
async fn test_update_namespace() -> Result<()> {
    let fixture = set_test_fixture("test_update_namespace").await;
    let namespace = NamespaceIdent::new("my_database".into());
    set_test_namespace(&fixture, &namespace).await?;

    let before_update = fixture.glue_catalog.get_namespace(&namespace).await?;
    let before_update = before_update.properties().get("description");

    assert_eq!(before_update, None);

    let properties = HashMap::from([("description".to_string(), "my_update".to_string())]);

    fixture
        .glue_catalog
        .update_namespace(&namespace, properties)
        .await?;

    let after_update = fixture.glue_catalog.get_namespace(&namespace).await?;
    let after_update = after_update.properties().get("description");

    assert_eq!(after_update, Some("my_update".to_string()).as_ref());

    Ok(())
}

#[tokio::test]
async fn test_namespace_exists() -> Result<()> {
    let fixture = set_test_fixture("test_namespace_exists").await;

    let namespace = NamespaceIdent::new("my_database".into());

    let exists = fixture.glue_catalog.namespace_exists(&namespace).await?;
    assert!(!exists);

    set_test_namespace(&fixture, &namespace).await?;

    let exists = fixture.glue_catalog.namespace_exists(&namespace).await?;
    assert!(exists);

    Ok(())
}

#[tokio::test]
async fn test_get_namespace() -> Result<()> {
    let fixture = set_test_fixture("test_get_namespace").await;

    let namespace = NamespaceIdent::new("my_database".into());

    let does_not_exist = fixture.glue_catalog.get_namespace(&namespace).await;
    assert!(does_not_exist.is_err());

    set_test_namespace(&fixture, &namespace).await?;

    let result = fixture.glue_catalog.get_namespace(&namespace).await?;
    let expected = Namespace::new(namespace);

    assert_eq!(result, expected);

    Ok(())
}

#[tokio::test]
async fn test_create_namespace() -> Result<()> {
    let fixture = set_test_fixture("test_create_namespace").await;

    let properties = HashMap::new();
    let namespace = NamespaceIdent::new("my_database".into());

    let expected = Namespace::new(namespace.clone());

    let result = fixture
        .glue_catalog
        .create_namespace(&namespace, properties)
        .await?;

    assert_eq!(result, expected);

    Ok(())
}

#[tokio::test]
async fn test_list_namespace() -> Result<()> {
    let fixture = set_test_fixture("test_list_namespace").await;

    let expected = vec![];
    let result = fixture.glue_catalog.list_namespaces(None).await?;
    assert_eq!(result, expected);

    let namespace = NamespaceIdent::new("my_database".to_string());
    set_test_namespace(&fixture, &namespace).await?;

    let expected = vec![namespace];
    let result = fixture.glue_catalog.list_namespaces(None).await?;
    assert_eq!(result, expected);

    Ok(())
}
