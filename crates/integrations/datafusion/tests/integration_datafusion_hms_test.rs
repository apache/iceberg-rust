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

//! Integration tests for Iceberg Datafusion with Hive Metastore.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::execution::context::SessionContext;
use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, NamespaceIdent, Result, TableCreation};
use iceberg_catalog_hms::{HmsCatalog, HmsCatalogConfig, HmsThriftTransport};
use iceberg_datafusion::IcebergCatalogProvider;
use iceberg_test_utils::docker::DockerCompose;
use iceberg_test_utils::{normalize_test_name, set_up};
use port_scanner::scan_port_addr;
use tokio::time::sleep;

const HMS_CATALOG_PORT: u16 = 9083;
const MINIO_PORT: u16 = 9000;

struct TestFixture {
    _docker_compose: DockerCompose,
    hms_catalog: HmsCatalog,
}

async fn set_test_fixture(func: &str) -> TestFixture {
    set_up();

    let docker_compose = DockerCompose::new(
        normalize_test_name(format!("{}_{func}", module_path!())),
        format!("{}/testdata", env!("CARGO_MANIFEST_DIR")),
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
async fn test_provider_get_table_schema() -> Result<()> {
    let fixture = set_test_fixture("test_provider_get_table_schema").await;

    let namespace = NamespaceIdent::new("default".to_string());
    let creation = set_table_creation("s3a://warehouse/hive", "my_table")?;

    fixture
        .hms_catalog
        .create_table(&namespace, creation)
        .await?;

    let client = Arc::new(fixture.hms_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("hive", catalog);

    let provider = ctx.catalog("hive").unwrap();
    let schema = provider.schema("default").unwrap();

    let table = schema.table("my_table").await.unwrap().unwrap();
    let table_schema = table.schema();

    let expected = [("foo", &DataType::Int32), ("bar", &DataType::Utf8)];

    for (field, exp) in table_schema.fields().iter().zip(expected.iter()) {
        assert_eq!(field.name(), exp.0);
        assert_eq!(field.data_type(), exp.1);
        assert!(!field.is_nullable())
    }

    Ok(())
}

#[tokio::test]
async fn test_provider_list_table_names() -> Result<()> {
    let fixture = set_test_fixture("test_provider_list_table_names").await;

    let namespace = NamespaceIdent::new("default".to_string());
    let creation = set_table_creation("s3a://warehouse/hive", "my_table")?;

    fixture
        .hms_catalog
        .create_table(&namespace, creation)
        .await?;

    let client = Arc::new(fixture.hms_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("hive", catalog);

    let provider = ctx.catalog("hive").unwrap();
    let schema = provider.schema("default").unwrap();

    let expected = vec!["my_table"];
    let result = schema.table_names();

    assert_eq!(result, expected);

    Ok(())
}

#[tokio::test]
async fn test_provider_list_schema_names() -> Result<()> {
    let fixture = set_test_fixture("test_provider_list_schema_names").await;
    set_table_creation("default", "my_table")?;

    let client = Arc::new(fixture.hms_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("hive", catalog);

    let provider = ctx.catalog("hive").unwrap();

    let expected = vec!["default"];
    let result = provider.schema_names();

    assert_eq!(result, expected);

    Ok(())
}
