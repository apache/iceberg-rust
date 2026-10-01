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

//! Custom AWS credential loader and FileIO builder property tests.

mod common;

use std::sync::Arc;

use common::{StorageKind, load_storage};
use iceberg::ErrorKind;
use iceberg::io::{FileIOBuilder, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION};
use iceberg_storage_opendal::{
    AwsCredential, CustomAwsCredentialLoader, OpenDalStorageFactory, ProvideCredential,
};
use iceberg_test_utils::get_object_store_endpoint;
use reqsign_core::Context;
use rstest::rstest;

/// Mock credential loader for testing custom AWS credential injection.
#[derive(Debug)]
struct MockCredentialLoader {
    credential: Option<AwsCredential>,
}

impl MockCredentialLoader {
    fn new(credential: Option<AwsCredential>) -> Self {
        Self { credential }
    }

    fn new_object_store() -> Self {
        Self::new(Some(AwsCredential {
            access_key_id: "admin".to_string(),
            secret_access_key: "password".to_string(),
            session_token: None,
            expires_in: None,
        }))
    }
}

impl ProvideCredential for MockCredentialLoader {
    type Credential = AwsCredential;

    async fn provide_credential(
        &self,
        _ctx: &Context,
    ) -> reqsign_core::Result<Option<AwsCredential>> {
        Ok(self.credential.clone())
    }
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

    let mock_loader = MockCredentialLoader::new_object_store();
    let custom_loader = CustomAwsCredentialLoader::new(mock_loader);
    let object_store_endpoint = get_object_store_endpoint();

    let file_io_with_custom_creds = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::S3 {
        customized_credential_load: Some(custom_loader),
    }))
    .with_props(vec![
        (S3_ENDPOINT, object_store_endpoint),
        (S3_REGION, "us-east-1".to_string()),
        (S3_PATH_STYLE_ACCESS, "true".to_string()),
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
    let custom_loader = CustomAwsCredentialLoader::new(mock_loader);
    let object_store_endpoint = get_object_store_endpoint();

    let file_io_with_custom_creds = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::S3 {
        customized_credential_load: Some(custom_loader),
    }))
    .with_props(vec![
        (S3_ENDPOINT, object_store_endpoint),
        (S3_REGION, "us-east-1".to_string()),
        (S3_PATH_STYLE_ACCESS, "true".to_string()),
    ])
    .build();

    let err = file_io_with_custom_creds
        .exists("s3://bucket1/any")
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Unexpected);

    Ok(())
}
