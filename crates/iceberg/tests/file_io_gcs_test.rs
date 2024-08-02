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

//! Integration tests for FileIO Google Cloud Storage (GCS).

use std::net::SocketAddr;
use std::sync::RwLock;

use ctor::{ctor, dtor};
use iceberg::io::{FileIO, FileIOBuilder, GCS_BUCKET, GCS_ENDPOINT};
use iceberg_test_utils::docker::DockerCompose;
use iceberg_test_utils::{normalize_test_name, set_up};

const FAKE_GCS_PORT: u16 = 4443;
static DOCKER_COMPOSE_ENV: RwLock<Option<DockerCompose>> = RwLock::new(None);

#[ctor]
fn before_all() {
    let mut guard = DOCKER_COMPOSE_ENV.write().unwrap();
    let docker_compose = DockerCompose::new(
        normalize_test_name(module_path!()),
        format!("{}/testdata/file_io_gcs", env!("CARGO_MANIFEST_DIR")),
    );
    docker_compose.run();
    guard.replace(docker_compose);
}

#[dtor]
fn after_all() {
    let mut guard = DOCKER_COMPOSE_ENV.write().unwrap();
    guard.take();
}

async fn get_file_io() -> FileIO {
    set_up();

    let guard = DOCKER_COMPOSE_ENV.read().unwrap();
    let docker_compose = guard.as_ref().unwrap();
    let container_ip = docker_compose.get_container_ip("gcs-server");
    let gcs_socket_addr = SocketAddr::new(container_ip, FAKE_GCS_PORT);

    FileIOBuilder::new("gcs")
        .with_props(vec![
            (GCS_ENDPOINT, format!("http://{}", gcs_socket_addr)),
            (GCS_BUCKET, "my-test-bucket".to_string()),
        ])
        .build()
        .unwrap()
}

#[tokio::test]
async fn test_file_io_gcs_exists() {
    let file_io = get_file_io().await;
    assert!(file_io.is_exist("gs://my-test-bucket/").await.unwrap());
}
