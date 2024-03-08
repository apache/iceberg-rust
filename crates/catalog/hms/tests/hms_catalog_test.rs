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

use iceberg::{Catalog, Namespace, NamespaceIdent};
use iceberg_catalog_hms::{HmsCatalog, HmsCatalogConfig, HmsThriftTransport};
use iceberg_test_utils::docker::DockerCompose;
use iceberg_test_utils::{normalize_test_name, set_up};
use port_scanner::scan_port_addr;
use tokio::time::sleep;

const HMS_CATALOG_PORT: u16 = 9083;
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

    let read_port = format!("{}:{}", hms_catalog_ip, HMS_CATALOG_PORT);
    loop {
        if !scan_port_addr(&read_port) {
            log::info!("Waiting for 1s hms catalog to ready...");
            sleep(std::time::Duration::from_millis(1000)).await;
        } else {
            break;
        }
    }

    let config = HmsCatalogConfig::builder()
        .address(format!("{}:{}", hms_catalog_ip, HMS_CATALOG_PORT))
        .thrift_transport(HmsThriftTransport::Buffered)
        .build();

    let hms_catalog = HmsCatalog::new(config).unwrap();

    TestFixture {
        _docker_compose: docker_compose,
        hms_catalog,
    }
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
    assert!(result_with_parent.len() == 0);

    Ok(())
}

#[tokio::test]
async fn test_create_namespace() -> Result<()> {
    let fixture = set_test_fixture("test_create_namespace").await;

    let ns = Namespace::new(NamespaceIdent::new("my_namespace".into()));
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
        (
            "location".to_string(),
            "file:/user/hive/warehouse".to_string(),
        ),
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
