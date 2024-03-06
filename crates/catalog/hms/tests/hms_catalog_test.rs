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

use iceberg::{Catalog, NamespaceIdent};
use iceberg_catalog_hms::{HmsCatalog, HmsCatalogConfig, HmsThriftTransport};
use iceberg_test_utils::docker::DockerCompose;
use iceberg_test_utils::{normalize_test_name, set_up};
use port_scanner::scan_port_addr;
use tokio::time::sleep;

const HMS_CATALOG_PORT: u16 = 9083;

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
async fn test_list_namespace() {
    let fixture = set_test_fixture("test_list_namespace").await;

    let expected = vec![NamespaceIdent::from_strs(["default"]).unwrap()];

    let result = fixture.hms_catalog.list_namespaces(None).await.unwrap();

    assert_eq!(expected, result)
}
