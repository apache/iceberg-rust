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

use iceberg::{Catalog, Namespace, NamespaceIdent, Result};
use iceberg_catalog_glue::{
    GlueCatalog, GlueCatalogConfig, AWS_ACCESS_KEY_ID, AWS_REGION_NAME, AWS_SECRET_ACCESS_KEY,
};
use iceberg_test_utils::docker::DockerCompose;
use iceberg_test_utils::{normalize_test_name, set_up};
use port_scanner::scan_port_addr;
use tokio::time::sleep;

const GLUE_CATALOG_PORT: u16 = 5000;

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
    ]);

    let config = GlueCatalogConfig::builder()
        .uri(format!("http://{}:{}", glue_catalog_ip, GLUE_CATALOG_PORT))
        .props(props)
        .build();

    let glue_catalog = GlueCatalog::new(config).await;

    TestFixture {
        _docker_compose: docker_compose,
        glue_catalog,
    }
}

#[tokio::test]
async fn test_update_namespace() -> Result<()> {
    let fixture = set_test_fixture("test_update_namespace").await;

    let properties = HashMap::new();
    let namespace = NamespaceIdent::new("my_database".into());

    fixture
        .glue_catalog
        .create_namespace(&namespace, properties)
        .await?;

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
    assert_eq!(
        after_update.as_deref(),
        Some("my_update".to_string()).as_ref()
    );

    Ok(())
}

#[tokio::test]
async fn test_namespace_exists() -> Result<()> {
    let fixture = set_test_fixture("test_namespace_exists").await;

    let properties = HashMap::new();
    let namespace = NamespaceIdent::new("my_database".into());

    let exists = fixture.glue_catalog.namespace_exists(&namespace).await?;
    assert!(!exists);

    fixture
        .glue_catalog
        .create_namespace(&namespace, properties)
        .await?;

    let exists = fixture.glue_catalog.namespace_exists(&namespace).await?;
    assert!(exists);

    Ok(())
}

#[tokio::test]
async fn test_get_namespace() -> Result<()> {
    let fixture = set_test_fixture("test_get_namespace").await;

    let properties = HashMap::new();
    let namespace = NamespaceIdent::new("my_database".into());

    let does_not_exist = fixture.glue_catalog.get_namespace(&namespace).await;
    assert!(does_not_exist.is_err());

    fixture
        .glue_catalog
        .create_namespace(&namespace, properties)
        .await?;

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

    Ok(())
}
