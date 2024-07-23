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

pub fn get_docker_compose() -> DockerCompose {
    DockerCompose::new(
        "iceberg-rust-performance",
        format!(
            "{}/../iceberg/testdata/performance",
            env!("CARGO_MANIFEST_DIR")
        ),
    )
    .with_persistence(true)
}

pub async fn build_catalog() -> RestCatalog {
    let docker_compose = get_docker_compose();

    let rest_api_container_ip = docker_compose.get_container_ip("rest");
    let haproxy_container_ip = docker_compose.get_container_ip("haproxy");

    let catalog_uri = format!("http://{}:{}", rest_api_container_ip, 8181);
    let haproxy_uri = format!("http://{}:{}", haproxy_container_ip, 9080);

    let user_props = HashMap::from_iter(
        vec![
            (S3_ENDPOINT.to_string(), haproxy_uri),
            (S3_ACCESS_KEY_ID.to_string(), "admin".to_string()),
            (S3_SECRET_ACCESS_KEY.to_string(), "password".to_string()),
            (S3_REGION.to_string(), "us-east-1".to_string()),
        ]
        .into_iter(),
    );

    RestCatalog::new(
        RestCatalogConfig::builder()
            .uri(catalog_uri)
            .props(user_props)
            .build(),
    )
}
