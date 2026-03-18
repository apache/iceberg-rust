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

//! Custom AWS credential loader tests (OpenDAL S3 only).

mod common;

use std::sync::Arc;

use async_trait::async_trait;
use common::{StorageKind, load_storage};
use iceberg::io::{FileIOBuilder, S3_ENDPOINT, S3_REGION};
use iceberg_storage_opendal::{CustomAwsCredentialLoader, OpenDalStorageFactory};
use iceberg_test_utils::get_minio_endpoint;
use reqsign::{AwsCredential, AwsCredentialLoad};
use reqwest::Client;
use rstest::rstest;

/// Mock credential loader for testing custom AWS credential injection.
struct MockCredentialLoader {
    credential: Option<AwsCredential>,
}

impl MockCredentialLoader {
    fn new(credential: Option<AwsCredential>) -> Self {
        Self { credential }
    }

    fn new_minio() -> Self {
        Self::new(Some(AwsCredential {
            access_key_id: "admin".to_string(),
            secret_access_key: "password".to_string(),
            session_token: None,
            expires_in: None,
        }))
    }
}

#[async_trait]
impl AwsCredentialLoad for MockCredentialLoader {
    async fn load_credential(&self, _client: Client) -> anyhow::Result<Option<AwsCredential>> {
        Ok(self.credential.clone())
    }
}

#[test]
fn test_custom_aws_credential_loader_instantiation() {
    let mock_loader = MockCredentialLoader::new_minio();
    let custom_loader = CustomAwsCredentialLoader::new(Arc::new(mock_loader));

    let _builder = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::S3 {
        configured_scheme: "s3".to_string(),
        customized_credential_load: Some(custom_loader),
    }))
    .with_props(vec![
        (S3_ENDPOINT, "http://localhost:9000".to_string()),
        ("bucket", "test-bucket".to_string()),
        (S3_REGION, "us-east-1".to_string()),
    ]);
}

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
#[tokio::test]
async fn test_s3_with_custom_credential_loader_success(
    #[case] kind: StorageKind,
) -> iceberg::Result<()> {
    let Some(_harness) = load_storage(kind).await else {
        return Ok(());
    };

    let mock_loader = MockCredentialLoader::new_minio();
    let custom_loader = CustomAwsCredentialLoader::new(Arc::new(mock_loader));
    let minio_endpoint = get_minio_endpoint();

    let file_io_with_custom_creds = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::S3 {
        configured_scheme: "s3".to_string(),
        customized_credential_load: Some(custom_loader),
    }))
    .with_props(vec![
        (S3_ENDPOINT, minio_endpoint),
        (S3_REGION, "us-east-1".to_string()),
    ])
    .build();

    match file_io_with_custom_creds.exists("s3://bucket1/any").await {
        Ok(_) => {}
        Err(e) => panic!("Failed to check existence of bucket: {e}"),
    }

    Ok(())
}

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
#[tokio::test]
async fn test_s3_with_custom_credential_loader_failure(
    #[case] kind: StorageKind,
) -> iceberg::Result<()> {
    let Some(_harness) = load_storage(kind).await else {
        return Ok(());
    };

    let mock_loader = MockCredentialLoader::new(None);
    let custom_loader = CustomAwsCredentialLoader::new(Arc::new(mock_loader));
    let minio_endpoint = get_minio_endpoint();

    let file_io_with_custom_creds = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::S3 {
        configured_scheme: "s3".to_string(),
        customized_credential_load: Some(custom_loader),
    }))
    .with_props(vec![
        (S3_ENDPOINT, minio_endpoint),
        (S3_REGION, "us-east-1".to_string()),
    ])
    .build();

    match file_io_with_custom_creds.exists("s3://bucket1/any").await {
        Ok(_) => panic!("Expected error, but got Ok"),
        Err(e) => {
            assert!(
                e.to_string()
                    .contains("no valid credential found and anonymous access is not allowed")
            );
        }
    }

    Ok(())
}
