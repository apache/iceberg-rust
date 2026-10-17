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

//! OpenDAL resolving storage integration tests.

mod common;

use std::sync::Arc;

use bytes::Bytes;
use common::{StorageKind, load_storage, unique_path};
use iceberg::io::{FileIOBuilder, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION};
use iceberg_storage_common::roundtrip_file_io;
use iceberg_storage_opendal::{
    AwsCredential, CustomAwsCredentialLoader, OpenDalResolvingStorageFactory, ProvideCredential,
};
use iceberg_test_utils::{get_object_store_endpoint, set_up};
use reqsign_core::Context;
use rstest::rstest;
use tempfile::TempDir;

#[rstest]
#[case::opendal_resolving(StorageKind::OpenDalResolving)]
#[tokio::test]
async fn test_mixed_scheme_write_and_read(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };

    let s3_path = unique_path(&harness, "test_mixed_scheme_write_and_read");
    let temp_dir = TempDir::new().unwrap();
    let fs_path = format!(
        "file:{}/mixed_write_and_read.txt",
        temp_dir.path().display()
    );
    let mem_path = "memory://test_mixed_scheme_write_and_read";

    // Write to all three schemes
    harness
        .file_io
        .new_output(&s3_path)?
        .write("from_s3".into())
        .await?;
    harness
        .file_io
        .new_output(&fs_path)?
        .write("from_fs".into())
        .await?;
    harness
        .file_io
        .new_output(mem_path)?
        .write("from_memory".into())
        .await?;

    // Read back from all three
    assert_eq!(
        harness.file_io.new_input(&s3_path)?.read().await?,
        Bytes::from("from_s3")
    );
    assert_eq!(
        harness.file_io.new_input(&fs_path)?.read().await?,
        Bytes::from("from_fs")
    );
    assert_eq!(
        harness.file_io.new_input(mem_path)?.read().await?,
        Bytes::from("from_memory")
    );

    let _ = harness.file_io.delete(&s3_path).await;
    let _ = harness.file_io.delete(mem_path).await;

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
    let temp_dir = TempDir::new().unwrap();
    let fs_path = format!(
        "file:{}/mixed_exists_independently.txt",
        temp_dir.path().display()
    );
    let mem_path = "memory://test_mixed_scheme_exists_independently";

    // Clean up S3 from previous runs
    let _ = harness.file_io.delete(&s3_path).await;

    // None exist initially
    assert!(!harness.file_io.exists(&s3_path).await?);
    assert!(!harness.file_io.exists(&fs_path).await?);
    assert!(!harness.file_io.exists(mem_path).await?);

    // Write only to fs
    harness
        .file_io
        .new_output(&fs_path)?
        .write("fs_only".into())
        .await?;

    // Only fs exists
    assert!(!harness.file_io.exists(&s3_path).await?);
    assert!(harness.file_io.exists(&fs_path).await?);
    assert!(!harness.file_io.exists(mem_path).await?);

    let _ = harness.file_io.delete(&fs_path).await;

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
    let temp_dir = TempDir::new().unwrap();
    let fs_path = format!(
        "file:{}/mixed_delete_one_keeps_others.txt",
        temp_dir.path().display()
    );
    let mem_path = "memory://test_mixed_scheme_delete_one_keeps_others";

    // Write to all three
    harness
        .file_io
        .new_output(&s3_path)?
        .write("s3".into())
        .await?;
    harness
        .file_io
        .new_output(&fs_path)?
        .write("fs".into())
        .await?;
    harness
        .file_io
        .new_output(mem_path)?
        .write("mem".into())
        .await?;

    // Delete only the fs file
    harness.file_io.delete(&fs_path).await?;

    // fs gone, S3 and memory still there
    assert!(harness.file_io.exists(&s3_path).await?);
    assert!(!harness.file_io.exists(&fs_path).await?);
    assert!(harness.file_io.exists(mem_path).await?);

    assert_eq!(
        harness.file_io.new_input(&s3_path)?.read().await?,
        Bytes::from("s3")
    );
    assert_eq!(
        harness.file_io.new_input(mem_path)?.read().await?,
        Bytes::from("mem")
    );

    let _ = harness.file_io.delete(&s3_path).await;
    let _ = harness.file_io.delete(mem_path).await;

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
    let temp_dir = TempDir::new().unwrap();
    let fs_path = format!("file:{}/mixed_interleaved.txt", temp_dir.path().display());
    let mem_path = "memory://test_mixed_scheme_interleaved";

    // Interleave: write fs, write memory, write s3
    harness
        .file_io
        .new_output(&fs_path)?
        .write("fs_data".into())
        .await?;
    harness
        .file_io
        .new_output(mem_path)?
        .write("mem_data".into())
        .await?;
    harness
        .file_io
        .new_output(&s3_path)?
        .write("s3_data".into())
        .await?;

    // Read in reverse order: s3, memory, fs
    assert_eq!(
        harness.file_io.new_input(&s3_path)?.read().await?,
        Bytes::from("s3_data")
    );
    assert_eq!(
        harness.file_io.new_input(mem_path)?.read().await?,
        Bytes::from("mem_data")
    );
    assert_eq!(
        harness.file_io.new_input(&fs_path)?.read().await?,
        Bytes::from("fs_data")
    );

    let _ = harness.file_io.delete(&s3_path).await;
    let _ = harness.file_io.delete(mem_path).await;

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

    #[derive(Debug)]
    struct ObjectStoreCredentialLoader;

    impl ProvideCredential for ObjectStoreCredentialLoader {
        type Credential = AwsCredential;

        async fn provide_credential(
            &self,
            _ctx: &Context,
        ) -> reqsign_core::Result<Option<AwsCredential>> {
            Ok(Some(AwsCredential {
                access_key_id: "admin".to_string(),
                secret_access_key: "password".to_string(),
                session_token: None,
                expires_in: None,
            }))
        }
    }

    set_up();
    let object_store_endpoint = get_object_store_endpoint();

    let factory = OpenDalResolvingStorageFactory::new()
        .with_s3_credential_loader(CustomAwsCredentialLoader::new(ObjectStoreCredentialLoader));

    let file_io = FileIOBuilder::new(Arc::new(factory))
        .with_props(vec![
            (S3_ENDPOINT, object_store_endpoint),
            (S3_REGION, "us-east-1".to_string()),
            (S3_PATH_STYLE_ACCESS, "true".to_string()),
        ])
        .build();

    assert!(file_io.exists("s3://bucket1/").await?);

    Ok(())
}

#[rstest]
#[case::opendal_resolving(StorageKind::OpenDalResolving)]
#[tokio::test]
async fn test_resolving_serialization_roundtrip(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };
    let file_io = roundtrip_file_io(&harness.file_io);
    let s3_path = unique_path(&harness, "test_resolving_serialization_roundtrip");

    let _ = file_io.delete(&s3_path).await;
    file_io
        .new_output(&s3_path)?
        .write(Bytes::from_static(b"resolving_roundtrip"))
        .await?;
    assert_eq!(
        file_io.new_input(&s3_path)?.read().await?,
        Bytes::from_static(b"resolving_roundtrip")
    );
    file_io.delete(&s3_path).await?;
    assert!(!file_io.exists(&s3_path).await?);
    Ok(())
}
