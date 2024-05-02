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

//! Integration tests for hms catalog.

use std::collections::HashMap;

use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, Namespace, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_hms::{HmsCatalog, HmsCatalogConfig, HmsThriftTransport};
use iceberg_test_utils::docker::DockerCompose;
use iceberg_test_utils::{normalize_test_name, set_up};
use port_scanner::scan_port_addr;
use tokio::time::sleep;

const HMS_CATALOG_PORT: u16 = 9083;
const MINIO_PORT: u16 = 9000;
type Result<T> = std::result::Result<T, iceberg::Error>;

struct TestFixture {
    _docker_compose: DockerCompose,
    hms_catalog: HmsCatalog,
}

async fn set_test_fixture(func: &str) -> TestFixture {
    set_up();

    let docker_compose = DockerCompose::new(
        normalize_test_name(format!("{}_{func}", module_path!())),
        format!("{}/testdata/hms_catalog", env!("CARGO_MANIFEST_DIR")),
    );

    docker_compose.run();

    let hms_catalog_ip = docker_compose.get_container_ip("hive-metastore");
    let minio_ip = docker_compose.get_container_ip("minio");

    let read_port = format!("{}:{}", hms_catalog_ip, HMS_CATALOG_PORT);
    loop {
        if !scan_port_addr(&read_port) {
            log::info!("Waiting for 1s hms catalog to ready...");
            sleep(std::time::Duration::from_millis(1000)).await;
        } else {
            break;
        }
    }

    let props = HashMap::from([
        (
            S3_ENDPOINT.to_string(),
            format!("http://{}:{}", minio_ip, MINIO_PORT),
        ),
        (S3_ACCESS_KEY_ID.to_string(), "admin".to_string()),
        (S3_SECRET_ACCESS_KEY.to_string(), "password".to_string()),
        (S3_REGION.to_string(), "us-east-1".to_string()),
    ]);

    let config = HmsCatalogConfig::builder()
        .address(format!("{}:{}", hms_catalog_ip, HMS_CATALOG_PORT))
        .thrift_transport(HmsThriftTransport::Buffered)
        .warehouse("s3a://warehouse/hive".to_string())
        .props(props)
        .build();

    let hms_catalog = HmsCatalog::new(config).unwrap();

    TestFixture {
        _docker_compose: docker_compose,
        hms_catalog,
    }
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
    let namespace = Namespace::new(NamespaceIdent::new("default".into()));

    let table = fixture
        .hms_catalog
        .create_table(namespace.name(), creation)
        .await?;

    let dest = TableIdent::new(namespace.name().clone(), "my_table_rename".to_string());

    fixture
        .hms_catalog
        .rename_table(table.identifier(), &dest)
        .await?;

    let result = fixture.hms_catalog.table_exists(&dest).await?;

    assert!(result);

    Ok(())
}

#[tokio::test]
async fn test_table_exists() -> Result<()> {
    let fixture = set_test_fixture("test_table_exists").await;
    let creation = set_table_creation("s3a://warehouse/hive", "my_table")?;
    let namespace = Namespace::new(NamespaceIdent::new("default".into()));

    let table = fixture
        .hms_catalog
        .create_table(namespace.name(), creation)
        .await?;

    let result = fixture.hms_catalog.table_exists(table.identifier()).await?;

    assert!(result);

    Ok(())
}

#[tokio::test]
async fn test_drop_table() -> Result<()> {
    let fixture = set_test_fixture("test_drop_table").await;
    let creation = set_table_creation("s3a://warehouse/hive", "my_table")?;
    let namespace = Namespace::new(NamespaceIdent::new("default".into()));

    let table = fixture
        .hms_catalog
        .create_table(namespace.name(), creation)
        .await?;

    fixture.hms_catalog.drop_table(table.identifier()).await?;

    let result = fixture.hms_catalog.table_exists(table.identifier()).await?;

    assert!(!result);

    Ok(())
}

#[tokio::test]
async fn test_load_table() -> Result<()> {
    let fixture = set_test_fixture("test_load_table").await;
    let creation = set_table_creation("s3a://warehouse/hive", "my_table")?;
    let namespace = Namespace::new(NamespaceIdent::new("default".into()));

    let expected = fixture
        .hms_catalog
        .create_table(namespace.name(), creation)
        .await?;

    let result = fixture
        .hms_catalog
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
    let creation = set_table_creation("s3a://warehouse/hive", "my_table")?;
    let namespace = Namespace::new(NamespaceIdent::new("default".into()));

    let result = fixture
        .hms_catalog
        .create_table(namespace.name(), creation)
        .await?;

    assert_eq!(result.identifier().name(), "my_table");
    assert!(result
        .metadata_location()
        .is_some_and(|location| location.starts_with("s3a://warehouse/hive/metadata/00000-")));
    assert!(
        fixture
            .hms_catalog
            .file_io()
            .is_exist("s3a://warehouse/hive/metadata/")
            .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_list_tables() -> Result<()> {
    let fixture = set_test_fixture("test_list_tables").await;
    let ns = Namespace::new(NamespaceIdent::new("default".into()));
    let result = fixture.hms_catalog.list_tables(ns.name()).await?;

    assert_eq!(result, vec![]);

    let creation = set_table_creation("s3a://warehouse/hive", "my_table")?;
    fixture
        .hms_catalog
        .create_table(ns.name(), creation)
        .await?;
    let result = fixture.hms_catalog.list_tables(ns.name()).await?;

    assert_eq!(
        result,
        vec![TableIdent::new(ns.name().clone(), "my_table".to_string())]
    );

    Ok(())
}

#[tokio::test]
async fn test_list_namespace() -> Result<()> {
    let fixture = set_test_fixture("test_list_namespace").await;

    let expected_no_parent = vec![NamespaceIdent::new("default".into())];
    let result_no_parent = fixture.hms_catalog.list_namespaces(None).await?;

    let result_with_parent = fixture
        .hms_catalog
        .list_namespaces(Some(&NamespaceIdent::new("parent".into())))
        .await?;

    assert_eq!(expected_no_parent, result_no_parent);
    assert!(result_with_parent.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_create_namespace() -> Result<()> {
    let fixture = set_test_fixture("test_create_namespace").await;

    let properties = HashMap::from([
        ("comment".to_string(), "my_description".to_string()),
        ("location".to_string(), "my_location".to_string()),
        (
            "hive.metastore.database.owner".to_string(),
            "apache".to_string(),
        ),
        (
            "hive.metastore.database.owner-type".to_string(),
            "user".to_string(),
        ),
        ("key1".to_string(), "value1".to_string()),
    ]);

    let ns = Namespace::with_properties(
        NamespaceIdent::new("my_namespace".into()),
        properties.clone(),
    );

    let result = fixture
        .hms_catalog
        .create_namespace(ns.name(), properties)
        .await?;

    assert_eq!(result, ns);

    Ok(())
}

#[tokio::test]
async fn test_get_namespace() -> Result<()> {
    let fixture = set_test_fixture("test_get_namespace").await;

    let ns = Namespace::new(NamespaceIdent::new("default".into()));
    let properties = HashMap::from([
        ("location".to_string(), "s3a://warehouse/hive".to_string()),
        (
            "hive.metastore.database.owner-type".to_string(),
            "Role".to_string(),
        ),
        ("comment".to_string(), "Default Hive database".to_string()),
        (
            "hive.metastore.database.owner".to_string(),
            "public".to_string(),
        ),
    ]);

    let expected = Namespace::with_properties(NamespaceIdent::new("default".into()), properties);

    let result = fixture.hms_catalog.get_namespace(ns.name()).await?;

    assert_eq!(expected, result);

    Ok(())
}

#[tokio::test]
async fn test_namespace_exists() -> Result<()> {
    let fixture = set_test_fixture("test_namespace_exists").await;

    let ns_exists = Namespace::new(NamespaceIdent::new("default".into()));
    let ns_not_exists = Namespace::new(NamespaceIdent::new("not_here".into()));

    let result_exists = fixture
        .hms_catalog
        .namespace_exists(ns_exists.name())
        .await?;
    let result_not_exists = fixture
        .hms_catalog
        .namespace_exists(ns_not_exists.name())
        .await?;

    assert!(result_exists);
    assert!(!result_not_exists);

    Ok(())
}

#[tokio::test]
async fn test_update_namespace() -> Result<()> {
    let fixture = set_test_fixture("test_update_namespace").await;

    let ns = Namespace::new(NamespaceIdent::new("default".into()));
    let properties = HashMap::from([("comment".to_string(), "my_update".to_string())]);

    fixture
        .hms_catalog
        .update_namespace(ns.name(), properties)
        .await?;

    let db = fixture.hms_catalog.get_namespace(ns.name()).await?;

    assert_eq!(
        db.properties().get("comment"),
        Some(&"my_update".to_string())
    );

    Ok(())
}

#[tokio::test]
async fn test_drop_namespace() -> Result<()> {
    let fixture = set_test_fixture("test_drop_namespace").await;

    let ns = Namespace::new(NamespaceIdent::new("delete_me".into()));

    fixture
        .hms_catalog
        .create_namespace(ns.name(), HashMap::new())
        .await?;

    let result = fixture.hms_catalog.namespace_exists(ns.name()).await?;
    assert!(result);

    fixture.hms_catalog.drop_namespace(ns.name()).await?;

    let result = fixture.hms_catalog.namespace_exists(ns.name()).await?;
    assert!(!result);

    Ok(())
}
