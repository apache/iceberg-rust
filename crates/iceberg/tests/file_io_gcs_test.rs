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

use bytes::Bytes;
use iceberg::io::{FileIO, FileIOBuilder};
use iceberg_test_utils::set_up;

// static DOCKER_COMPOSE_ENV: RwLock<Option<DockerCompose>> = RwLock::new(None);

// TODO: use compose with fake-gcs-server
//#[ctor]
//fn before_all() {
//    let mut guard = DOCKER_COMPOSE_ENV.write().unwrap();
//    let docker_compose = DockerCompose::new(
//        normalize_test_name(module_path!()),
//        format!("{}/testdata/file_io_gcs", env!("CARGO_MANIFEST_DIR")),
//    );
//    docker_compose.run();
//    guard.replace(docker_compose);
//}
//
//#[dtor]
//fn after_all() {
//    let mut guard = DOCKER_COMPOSE_ENV.write().unwrap();
//    guard.take();
//}

async fn get_file_io_gcs() -> FileIO {
    set_up();
    FileIOBuilder::new("gcs").build().unwrap()
}

fn get_gs_path() -> String {
    format!(
        "gs://{}",
        std::env::var("GCS_BUCKET").expect("Only runs with var enabled")
    )
}

#[tokio::test]
#[test_with::env(GCS_BUCKET, GCS_CREDENTIAL_PATH)]
async fn gcs_exists() {
    let file_io = get_file_io_gcs().await;
    assert!(file_io
        .is_exist(format!("{}/", get_gs_path()))
        .await
        .unwrap());
}

#[tokio::test]
#[test_with::env(GCS_BUCKET, GCS_CREDENTIAL_PATH)]
async fn gcs_write() {
    let gs_file = format!("{}/write-file", get_gs_path());
    let file_io = get_file_io_gcs().await;
    let output = file_io.new_output(&gs_file).unwrap();
    output
        .write(bytes::Bytes::from_static(b"iceberg-gcs!"))
        .await
        .expect("Write to test output file");
    assert!(file_io.is_exist(gs_file).await.unwrap())
}

#[tokio::test]
#[test_with::env(GCS_BUCKET, GCS_CREDENTIAL_PATH)]
async fn gcs_read() {
    let gs_file = format!("{}/read-gcs", get_gs_path());
    let file_io = get_file_io_gcs().await;
    let output = file_io.new_output(&gs_file).unwrap();
    output
        .write(bytes::Bytes::from_static(b"iceberg!"))
        .await
        .expect("Write to test output file");
    assert!(file_io.is_exist(&gs_file).await.unwrap());

    let input = file_io.new_input(gs_file).unwrap();
    assert_eq!(input.read().await.unwrap(), Bytes::from_static(b"iceberg!"));
}
