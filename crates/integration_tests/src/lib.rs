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
use std::sync::OnceLock;

use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg_catalog_rest::REST_CATALOG_PROP_URI;
use iceberg_test_utils::docker::DockerCompose;
use iceberg_test_utils::{
    get_minio_endpoint, get_rest_catalog_endpoint, normalize_test_name, set_up,
};

const REST_CATALOG_PORT: u16 = 8181;

/// Test fixture that manages Docker containers.
/// This is kept for backward compatibility but deprecated in favor of GlobalTestFixture.
pub struct TestFixture {
    pub _docker_compose: DockerCompose,
    pub catalog_config: HashMap<String, String>,
}

/// Global test fixture that uses environment-based configuration.
/// This assumes Docker containers are started externally (e.g., via `make docker-up`).
pub struct GlobalTestFixture {
    pub catalog_config: HashMap<String, String>,
}

static GLOBAL_FIXTURE: OnceLock<GlobalTestFixture> = OnceLock::new();

impl GlobalTestFixture {
    /// Creates a new GlobalTestFixture from environment variables.
    /// Uses default localhost endpoints if environment variables are not set.
    pub fn from_env() -> Self {
        set_up();

        let rest_endpoint = get_rest_catalog_endpoint();
        let minio_endpoint = get_minio_endpoint();

        let catalog_config = HashMap::from([
            (REST_CATALOG_PROP_URI.to_string(), rest_endpoint),
            (S3_ENDPOINT.to_string(), minio_endpoint),
            (S3_ACCESS_KEY_ID.to_string(), "admin".to_string()),
            (S3_SECRET_ACCESS_KEY.to_string(), "password".to_string()),
            (S3_REGION.to_string(), "us-east-1".to_string()),
        ]);

        GlobalTestFixture { catalog_config }
    }
}

/// Returns a reference to the global test fixture.
/// This fixture assumes Docker containers are started externally.
pub fn get_test_fixture() -> &'static GlobalTestFixture {
    GLOBAL_FIXTURE.get_or_init(GlobalTestFixture::from_env)
}

/// Legacy function to create a test fixture with Docker container management.
/// Deprecated: prefer using `get_test_fixture()` with externally managed containers.
pub fn set_test_fixture(func: &str) -> TestFixture {
    set_up();
    let docker_compose = DockerCompose::new(
        normalize_test_name(format!("{}_{func}", module_path!())),
        format!("{}/testdata", env!("CARGO_MANIFEST_DIR")),
    );

    // Stop any containers from previous runs and start new ones
    docker_compose.down();
    docker_compose.up();

    let rest_catalog_ip = docker_compose.get_container_ip("rest");
    let minio_ip = docker_compose.get_container_ip("minio");

    let catalog_config = HashMap::from([
        (
            REST_CATALOG_PROP_URI.to_string(),
            format!("http://{rest_catalog_ip}:{REST_CATALOG_PORT}"),
        ),
        (
            S3_ENDPOINT.to_string(),
            format!("http://{}:{}", minio_ip, 9000),
        ),
        (S3_ACCESS_KEY_ID.to_string(), "admin".to_string()),
        (S3_SECRET_ACCESS_KEY.to_string(), "password".to_string()),
        (S3_REGION.to_string(), "us-east-1".to_string()),
    ]);

    TestFixture {
        _docker_compose: docker_compose,
        catalog_config,
    }
}
