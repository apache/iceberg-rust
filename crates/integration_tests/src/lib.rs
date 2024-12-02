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

use std::collections::HashMap;

use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use iceberg_test_utils::docker::DockerCompose;
use iceberg_test_utils::{normalize_test_name, set_up};
use port_scanner::scan_port_addr;
use tokio::time::sleep;

const REST_CATALOG_PORT: u16 = 8181;

pub struct TestFixture {
    pub _docker_compose: DockerCompose,
    pub rest_catalog: RestCatalog,
}

pub async fn set_test_fixture(func: &str) -> TestFixture {
    set_up();
    let docker_compose = DockerCompose::new(
        normalize_test_name(format!("{}_{func}", module_path!())),
        format!("{}/testdata", env!("CARGO_MANIFEST_DIR")),
    );

    // Start docker compose
    docker_compose.run();

    let rest_catalog_addr = format!("127.0.0.1:{}", REST_CATALOG_PORT);

    loop {
        if !scan_port_addr(&rest_catalog_addr) {
            log::info!("Waiting for 1s rest catalog to ready...");
            sleep(std::time::Duration::from_millis(1000)).await;
        } else {
            break;
        }
    }

    let config = RestCatalogConfig::builder()
        .uri(format!("http://{}", rest_catalog_addr))
        .props(HashMap::from([
            (S3_ENDPOINT.to_string(), "http://127.0.0.1:9000".to_string()),
            (S3_ACCESS_KEY_ID.to_string(), "admin".to_string()),
            (S3_SECRET_ACCESS_KEY.to_string(), "password".to_string()),
            (S3_REGION.to_string(), "us-east-1".to_string()),
        ]))
        .build();
    let rest_catalog = RestCatalog::new(config);

    TestFixture {
        _docker_compose: docker_compose,
        rest_catalog,
    }
}
