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

//! OpenDAL resolving storage tests (OpenDAL resolving backend only).

mod common;

use std::sync::Arc;

use async_trait::async_trait;
use common::{StorageKind, load_storage, unique_path};
use iceberg::io::{FileIOBuilder, S3_ENDPOINT, S3_REGION};
use iceberg_storage_opendal::{CustomAwsCredentialLoader, OpenDalResolvingStorageFactory};
use iceberg_test_utils::{get_minio_endpoint, set_up};
use reqsign::{AwsCredential, AwsCredentialLoad};
use reqwest::Client;
use rstest::rstest;

fn temp_fs_path(name: &str) -> String {
    let dir = std::env::temp_dir().join("iceberg_resolving_tests");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(name);
    // Clean up from previous runs
    let _ = std::fs::remove_file(&path);
    format!("file:/{}", path.display())
}

#[rstest]
#[case::opendal_resolving(StorageKind::OpenDalResolving)]
#[tokio::test]
async fn test_mixed_scheme_write_and_read(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };

    let s3_path = unique_path(&harness, "test_mixed_scheme_write_and_read");
    let fs_path = temp_fs_path("mixed_write_and_read.txt");
    let mem_path = "memory://test_mixed_scheme_write_and_read";

    // Write to all three schemes
    harness
        .file_io
        .new_output(&s3_path)
        .unwrap()
        .write("from_s3".into())
        .await
        .unwrap();
    harness
        .file_io
        .new_output(&fs_path)
        .unwrap()
        .write("from_fs".into())
        .await
        .unwrap();
    harness
        .file_io
        .new_output(mem_path)
        .unwrap()
        .write("from_memory".into())
        .await
        .unwrap();

    // Read back from all three
    assert_eq!(
        harness
            .file_io
            .new_input(&s3_path)
            .unwrap()
            .read()
            .await
            .unwrap(),
        bytes::Bytes::from("from_s3")
    );
    assert_eq!(
        harness
            .file_io
            .new_input(&fs_path)
            .unwrap()
            .read()
            .await
            .unwrap(),
        bytes::Bytes::from("from_fs")
    );
    assert_eq!(
        harness
            .file_io
            .new_input(mem_path)
            .unwrap()
            .read()
            .await
            .unwrap(),
        bytes::Bytes::from("from_memory")
    );

    Ok(())
}

#[rstest]
#[case::opendal_resolving(StorageKind::OpenDalResolving)]
#[tokio::test]
async fn test_mixed_scheme_exists_independently(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };

    let s3_path = unique_path(&harness, "test_mixed_scheme_exists_independently");
    let fs_path = temp_fs_path("mixed_exists_independently.txt");
    let mem_path = "memory://test_mixed_scheme_exists_independently";

    // Clean up S3 from previous runs
    let _ = harness.file_io.delete(&s3_path).await;

    // None exist initially
    assert!(!harness.file_io.exists(&s3_path).await.unwrap());
    assert!(!harness.file_io.exists(&fs_path).await.unwrap());
    assert!(!harness.file_io.exists(mem_path).await.unwrap());

    // Write only to fs
    harness
        .file_io
        .new_output(&fs_path)
        .unwrap()
        .write("fs_only".into())
        .await
        .unwrap();

    // Only fs exists
    assert!(!harness.file_io.exists(&s3_path).await.unwrap());
    assert!(harness.file_io.exists(&fs_path).await.unwrap());
    assert!(!harness.file_io.exists(mem_path).await.unwrap());

    Ok(())
}

#[rstest]
#[case::opendal_resolving(StorageKind::OpenDalResolving)]
#[tokio::test]
async fn test_mixed_scheme_delete_one_keeps_others(
    #[case] kind: StorageKind,
) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };

    let s3_path = unique_path(&harness, "test_mixed_scheme_delete_one_keeps_others");
    let fs_path = temp_fs_path("mixed_delete_one_keeps_others.txt");
    let mem_path = "memory://test_mixed_scheme_delete_one_keeps_others";

    // Write to all three
    harness
        .file_io
        .new_output(&s3_path)
        .unwrap()
        .write("s3".into())
        .await
        .unwrap();
    harness
        .file_io
        .new_output(&fs_path)
        .unwrap()
        .write("fs".into())
        .await
        .unwrap();
    harness
        .file_io
        .new_output(mem_path)
        .unwrap()
        .write("mem".into())
        .await
        .unwrap();

    // Delete only the fs file
    harness.file_io.delete(&fs_path).await.unwrap();

    // fs gone, S3 and memory still there
    assert!(harness.file_io.exists(&s3_path).await.unwrap());
    assert!(!harness.file_io.exists(&fs_path).await.unwrap());
    assert!(harness.file_io.exists(mem_path).await.unwrap());

    assert_eq!(
        harness
            .file_io
            .new_input(&s3_path)
            .unwrap()
            .read()
            .await
            .unwrap(),
        bytes::Bytes::from("s3")
    );
    assert_eq!(
        harness
            .file_io
            .new_input(mem_path)
            .unwrap()
            .read()
            .await
            .unwrap(),
        bytes::Bytes::from("mem")
    );

    Ok(())
}

#[rstest]
#[case::opendal_resolving(StorageKind::OpenDalResolving)]
#[tokio::test]
async fn test_mixed_scheme_interleaved_operations(
    #[case] kind: StorageKind,
) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };

    let s3_path = unique_path(&harness, "test_mixed_scheme_interleaved");
    let fs_path = temp_fs_path("mixed_interleaved.txt");
    let mem_path = "memory://test_mixed_scheme_interleaved";

    // Interleave: write fs, write memory, write s3
    harness
        .file_io
        .new_output(&fs_path)
        .unwrap()
        .write("fs_data".into())
        .await
        .unwrap();
    harness
        .file_io
        .new_output(mem_path)
        .unwrap()
        .write("mem_data".into())
        .await
        .unwrap();
    harness
        .file_io
        .new_output(&s3_path)
        .unwrap()
        .write("s3_data".into())
        .await
        .unwrap();

    // Read in reverse order: s3, memory, fs
    assert_eq!(
        harness
            .file_io
            .new_input(&s3_path)
            .unwrap()
            .read()
            .await
            .unwrap(),
        bytes::Bytes::from("s3_data")
    );
    assert_eq!(
        harness
            .file_io
            .new_input(mem_path)
            .unwrap()
            .read()
            .await
            .unwrap(),
        bytes::Bytes::from("mem_data")
    );
    assert_eq!(
        harness
            .file_io
            .new_input(&fs_path)
            .unwrap()
            .read()
            .await
            .unwrap(),
        bytes::Bytes::from("fs_data")
    );

    Ok(())
}

#[rstest]
#[case::opendal_resolving(StorageKind::OpenDalResolving)]
#[tokio::test]
async fn test_invalid_scheme(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };

    let result = harness.file_io.exists("unknown://bucket/key").await;
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Unsupported storage scheme")
    );

    Ok(())
}

#[rstest]
#[case::opendal_resolving(StorageKind::OpenDalResolving)]
#[tokio::test]
async fn test_missing_scheme(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };

    let result = harness.file_io.exists("no-scheme-path").await;
    assert!(result.is_err());

    Ok(())
}

#[rstest]
#[case::opendal_resolving(StorageKind::OpenDalResolving)]
#[tokio::test]
async fn test_resolving_with_custom_credential_loader(
    #[case] kind: StorageKind,
) -> iceberg::Result<()> {
    let Some(_harness) = load_storage(kind).await else {
        return Ok(());
    };

    struct MinioCredentialLoader;

    #[async_trait]
    impl AwsCredentialLoad for MinioCredentialLoader {
        async fn load_credential(&self, _client: Client) -> anyhow::Result<Option<AwsCredential>> {
            Ok(Some(AwsCredential {
                access_key_id: "admin".to_string(),
                secret_access_key: "password".to_string(),
                session_token: None,
                expires_in: None,
            }))
        }
    }

    set_up();
    let minio_endpoint = get_minio_endpoint();

    let factory = OpenDalResolvingStorageFactory::new().with_s3_credential_loader(
        CustomAwsCredentialLoader::new(Arc::new(MinioCredentialLoader)),
    );

    let file_io = FileIOBuilder::new(Arc::new(factory))
        .with_props(vec![
            (S3_ENDPOINT, minio_endpoint),
            (S3_REGION, "us-east-1".to_string()),
        ])
        .build();

    // Should be able to access S3 using the custom credential loader
    assert!(file_io.exists("s3://bucket1/").await.unwrap());

    Ok(())
}
