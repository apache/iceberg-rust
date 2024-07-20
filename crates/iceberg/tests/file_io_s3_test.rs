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

//! Integration tests for FileIO S3.

use ctor::{ctor, dtor};
use iceberg::io::{
    FileIO, FileIOBuilder, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY,
};
use iceberg_test_utils::docker::DockerCompose;
use iceberg_test_utils::{normalize_test_name, set_up};
use std::sync::RwLock;

static DOCKER_COMPOSE_ENV: RwLock<Option<DockerCompose>> = RwLock::new(None);

#[ctor]
fn before_all() {
    let mut guard = DOCKER_COMPOSE_ENV.write().unwrap();
    let docker_compose = DockerCompose::new(
        normalize_test_name(module_path!()),
        format!("{}/testdata/file_io_s3", env!("CARGO_MANIFEST_DIR")),
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
    let host_port = docker_compose.get_host_port("minio");
    let ip_and_port = format!("localhost:{}", host_port);

    FileIOBuilder::new("s3")
        .with_props(vec![
            (S3_ENDPOINT, format!("http://{}", ip_and_port)),
            (S3_ACCESS_KEY_ID, "admin".to_string()),
            (S3_SECRET_ACCESS_KEY, "password".to_string()),
            (S3_REGION, "us-east-1".to_string()),
        ])
        .build()
        .unwrap()
}

#[tokio::test]
async fn test_file_io_s3_is_exist() {
    let file_io = get_file_io().await;
    assert!(!file_io.is_exist("s3://bucket2/any").await.unwrap());
    assert!(file_io.is_exist("s3://bucket1/").await.unwrap());
}

#[tokio::test]
async fn test_file_io_s3_output() {
    let file_io = get_file_io().await;
    assert!(!file_io.is_exist("s3://bucket1/test_output").await.unwrap());
    let output_file = file_io.new_output("s3://bucket1/test_output").unwrap();
    {
        output_file.write("123".into()).await.unwrap();
    }
    assert!(file_io.is_exist("s3://bucket1/test_output").await.unwrap());
}

#[tokio::test]
async fn test_file_io_s3_input() {
    let file_io = get_file_io().await;
    let output_file = file_io.new_output("s3://bucket1/test_input").unwrap();
    {
        output_file.write("test_input".into()).await.unwrap();
    }

    let input_file = file_io.new_input("s3://bucket1/test_input").unwrap();

    {
        let buffer = input_file.read().await.unwrap();
        assert_eq!(buffer, "test_input".as_bytes());
    }
}
