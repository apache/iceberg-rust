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

use iceberg::io::{
    FileIO, FileIOBuilder, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY,
};
use iceberg_test_utils::docker::DockerCompose;

struct MinIOFixture {
    _docker_compose: DockerCompose,
    file_io: FileIO,
}
impl MinIOFixture {
    async fn new(project_name: impl ToString) -> Self {
        // Start the Docker container for the test fixture
        let docker = DockerCompose::new(
            project_name.to_string(),
            format!("{}/testdata/file_io_s3", env!("CARGO_MANIFEST_DIR")),
        );
        docker.run();
        let container_ip = docker.get_container_ip("minio");
        let read_port = format!("{}:{}", container_ip, 9000);

        MinIOFixture {
            _docker_compose: docker,
            file_io: FileIOBuilder::new("s3")
                .with_props(vec![
                    (S3_ENDPOINT, format!("http://{}", read_port)),
                    (S3_ACCESS_KEY_ID, "admin".to_string()),
                    (S3_SECRET_ACCESS_KEY, "password".to_string()),
                    (S3_REGION, "us-east-1".to_string()),
                ])
                .build()
                .unwrap(),
        }
    }
}

#[tokio::test]
async fn test_file_io_s3_is_exist() {
    let fixture = MinIOFixture::new("test_file_io_s3_is_exist").await;
    assert!(!fixture.file_io.is_exist("s3://bucket2/any").await.unwrap());
    assert!(fixture.file_io.is_exist("s3://bucket1/").await.unwrap());
}

#[tokio::test]
async fn test_file_io_s3_output() {
    // Start the Docker container for the test fixture
    let fixture = MinIOFixture::new("test_file_io_s3_output").await;
    assert!(!fixture
        .file_io
        .is_exist("s3://bucket1/test_output")
        .await
        .unwrap());
    let output_file = fixture
        .file_io
        .new_output("s3://bucket1/test_output")
        .unwrap();
    {
        output_file.write("123".into()).await.unwrap();
    }
    assert!(fixture
        .file_io
        .is_exist("s3://bucket1/test_output")
        .await
        .unwrap());
}

#[tokio::test]
async fn test_file_io_s3_input() {
    let fixture = MinIOFixture::new("test_file_io_s3_input").await;
    let output_file = fixture
        .file_io
        .new_output("s3://bucket1/test_input")
        .unwrap();
    {
        output_file.write("test_input".into()).await.unwrap();
    }

    let input_file = fixture
        .file_io
        .new_input("s3://bucket1/test_input")
        .unwrap();

    {
        let buffer = input_file.read().await.unwrap();
        assert_eq!(buffer, "test_input".as_bytes());
    }
}
