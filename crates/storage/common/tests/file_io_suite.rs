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

//! Shared FileIO integration tests parameterized over storage backends.

mod common;

use std::sync::Arc;

use common::{StorageKind, load_storage, unique_path};
use futures::StreamExt;
use iceberg::io::{FileIOBuilder, LocalFsStorageFactory};
use rstest::rstest;

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
#[case::opendal_gcs(StorageKind::OpenDalGcs)]
#[tokio::test]
async fn test_file_io_exists(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };
    // Non-existent path should return false
    let non_existent = unique_path(&harness, "non_existent_file_that_does_not_exist");
    assert!(!harness.file_io.exists(&non_existent).await.unwrap());
    // Bucket root should return true
    assert!(harness.file_io.exists(&harness.base_path).await.unwrap());
    Ok(())
}

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
#[case::opendal_gcs(StorageKind::OpenDalGcs)]
#[tokio::test]
async fn test_file_io_write(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };
    let path = unique_path(&harness, "test_file_io_write");
    // Clean up from any previous test runs
    let _ = harness.file_io.delete(&path).await;
    assert!(!harness.file_io.exists(&path).await.unwrap());
    let output_file = harness.file_io.new_output(&path).unwrap();
    output_file.write("123".into()).await.unwrap();
    assert!(harness.file_io.exists(&path).await.unwrap());
    Ok(())
}

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
#[case::opendal_gcs(StorageKind::OpenDalGcs)]
#[tokio::test]
async fn test_file_io_read(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };
    let path = unique_path(&harness, "test_file_io_read");
    let output_file = harness.file_io.new_output(&path).unwrap();
    output_file.write("test_input".into()).await.unwrap();
    let input_file = harness.file_io.new_input(&path).unwrap();
    let buffer = input_file.read().await.unwrap();
    assert_eq!(buffer, "test_input".as_bytes());
    Ok(())
}

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
#[case::opendal_gcs(StorageKind::OpenDalGcs)]
#[tokio::test]
async fn test_file_io_delete(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };
    let path = unique_path(&harness, "test_file_io_delete");
    // Write a file
    harness
        .file_io
        .new_output(&path)
        .unwrap()
        .write("delete_me".into())
        .await
        .unwrap();
    assert!(harness.file_io.exists(&path).await.unwrap());
    // Delete it
    harness.file_io.delete(&path).await.unwrap();
    // Verify it's gone
    assert!(!harness.file_io.exists(&path).await.unwrap());
    Ok(())
}

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
#[case::opendal_gcs(StorageKind::OpenDalGcs)]
#[tokio::test]
async fn test_file_io_delete_nonexistent(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };
    let path = unique_path(&harness, "test_file_io_delete_nonexistent");
    // Delete a non-existent path should succeed (no-op)
    harness.file_io.delete(&path).await.unwrap();
    Ok(())
}

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
#[case::opendal_gcs(StorageKind::OpenDalGcs)]
#[tokio::test]
async fn test_file_io_delete_stream(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };
    let base = unique_path(&harness, "test_file_io_delete_stream");
    let paths: Vec<String> = (0..5).map(|i| format!("{base}/file-{i}")).collect();
    for path in &paths {
        let _ = harness.file_io.delete(path).await;
        harness
            .file_io
            .new_output(path)
            .unwrap()
            .write("delete-me".into())
            .await
            .unwrap();
        assert!(harness.file_io.exists(path).await.unwrap());
    }
    let stream = futures::stream::iter(paths.clone()).boxed();
    harness.file_io.delete_stream(stream).await.unwrap();
    for path in &paths {
        assert!(!harness.file_io.exists(path).await.unwrap());
    }
    Ok(())
}

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
#[case::opendal_gcs(StorageKind::OpenDalGcs)]
#[tokio::test]
async fn test_file_io_delete_stream_empty(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };
    let stream = futures::stream::empty().boxed();
    harness.file_io.delete_stream(stream).await.unwrap();
    Ok(())
}

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
#[case::opendal_gcs(StorageKind::OpenDalGcs)]
#[tokio::test]
async fn test_file_io_delete_prefix(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };
    let prefix = unique_path(&harness, "test_file_io_delete_prefix");
    // Write files under the prefix
    let paths: Vec<String> = (0..3).map(|i| format!("{prefix}/file-{i}")).collect();
    for path in &paths {
        harness
            .file_io
            .new_output(path)
            .unwrap()
            .write("data".into())
            .await
            .unwrap();
        assert!(harness.file_io.exists(path).await.unwrap());
    }
    // Delete the prefix
    harness.file_io.delete_prefix(&prefix).await.unwrap();
    // Verify all files are gone
    for path in &paths {
        assert!(!harness.file_io.exists(path).await.unwrap());
    }
    Ok(())
}

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
#[case::opendal_gcs(StorageKind::OpenDalGcs)]
#[tokio::test]
async fn test_file_io_delete_prefix_nonexistent(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };
    let prefix = unique_path(&harness, "test_file_io_delete_prefix_nonexistent");
    // Delete a non-existent prefix should succeed (no-op)
    harness.file_io.delete_prefix(&prefix).await.unwrap();
    Ok(())
}

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
#[case::opendal_gcs(StorageKind::OpenDalGcs)]
#[tokio::test]
async fn test_file_io_metadata(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };
    let path = unique_path(&harness, "test_file_io_metadata");
    let content = "metadata_test_content";
    harness
        .file_io
        .new_output(&path)
        .unwrap()
        .write(content.into())
        .await
        .unwrap();
    let input_file = harness.file_io.new_input(&path).unwrap();
    let metadata = input_file.metadata().await.unwrap();
    assert_eq!(metadata.size, content.len() as u64);
    Ok(())
}

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
#[case::opendal_gcs(StorageKind::OpenDalGcs)]
#[tokio::test]
async fn test_file_io_metadata_nonexistent(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };
    let path = unique_path(&harness, "test_file_io_metadata_nonexistent");
    let input_file = harness.file_io.new_input(&path).unwrap();
    let result = input_file.metadata().await;
    assert!(result.is_err());
    Ok(())
}

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
#[case::opendal_gcs(StorageKind::OpenDalGcs)]
#[tokio::test]
async fn test_file_io_range_read(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };
    let path = unique_path(&harness, "test_file_io_range_read");
    let content = b"0123456789abcdef";
    harness
        .file_io
        .new_output(&path)
        .unwrap()
        .write(bytes::Bytes::from_static(content))
        .await
        .unwrap();
    let input_file = harness.file_io.new_input(&path).unwrap();
    let reader = input_file.reader().await.unwrap();
    // Read a range in the middle
    let range_data = reader.read(4..10).await.unwrap();
    assert_eq!(range_data.as_ref(), &content[4..10]);
    Ok(())
}

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
#[case::opendal_gcs(StorageKind::OpenDalGcs)]
#[tokio::test]
async fn test_file_io_streaming_write(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };
    let path = unique_path(&harness, "test_file_io_streaming_write");
    let output_file = harness.file_io.new_output(&path).unwrap();
    let mut writer = output_file.writer().await.unwrap();
    writer
        .write(bytes::Bytes::from("streaming_content"))
        .await
        .unwrap();
    writer.close().await.unwrap();
    // Read back and verify
    let input_file = harness.file_io.new_input(&path).unwrap();
    let buffer = input_file.read().await.unwrap();
    assert_eq!(buffer, bytes::Bytes::from("streaming_content"));
    Ok(())
}

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
#[case::opendal_gcs(StorageKind::OpenDalGcs)]
#[tokio::test]
async fn test_file_io_streaming_write_double_close(
    #[case] kind: StorageKind,
) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };
    let path = unique_path(&harness, "test_file_io_streaming_write_double_close");
    let output_file = harness.file_io.new_output(&path).unwrap();
    let mut writer = output_file.writer().await.unwrap();
    writer.write(bytes::Bytes::from("data")).await.unwrap();
    writer.close().await.unwrap();
    // Second close should error
    let result = writer.close().await;
    assert!(result.is_err());
    Ok(())
}

#[tokio::test]
async fn test_file_io_builder_with_prop() {
    let builder = FileIOBuilder::new(Arc::new(LocalFsStorageFactory)).with_prop("key1", "value1");
    assert_eq!(builder.config().get("key1"), Some(&"value1".to_string()));
}

#[tokio::test]
async fn test_file_io_builder_with_props() {
    let builder = FileIOBuilder::new(Arc::new(LocalFsStorageFactory)).with_props(vec![
        ("key1", "value1"),
        ("key2", "value2"),
        ("key3", "value3"),
    ]);
    assert_eq!(builder.config().get("key1"), Some(&"value1".to_string()));
    assert_eq!(builder.config().get("key2"), Some(&"value2".to_string()));
    assert_eq!(builder.config().get("key3"), Some(&"value3".to_string()));
}

#[tokio::test]
async fn test_file_io_builder_build_returns_file_io() {
    let file_io = FileIOBuilder::new(Arc::new(LocalFsStorageFactory))
        .with_prop("some_key", "some_value")
        .build();
    assert_eq!(
        file_io.config().get("some_key"),
        Some(&"some_value".to_string())
    );
}
