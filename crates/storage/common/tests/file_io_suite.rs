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

use bytes::Bytes;
use common::{StorageHarness, StorageKind, load_storage, unique_path};
use futures::StreamExt;
use iceberg::io::FileIO;
use rstest::rstest;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn roundtrip_file_io(file_io: &FileIO) -> FileIO {
    let serialized = file_io.serialize_all().unwrap();
    FileIO::deserialize_all(&serialized).unwrap()
}

// ---------------------------------------------------------------------------
// Shared Test Execution Bodies
// ---------------------------------------------------------------------------

async fn run_exists(harness: StorageHarness) -> iceberg::Result<()> {
    let non_existent = unique_path(&harness, "non_existent_file_that_does_not_exist");
    assert!(!harness.file_io.exists(&non_existent).await.unwrap());
    assert!(harness.file_io.exists(&harness.base_path).await.unwrap());
    Ok(())
}

async fn run_write(harness: StorageHarness) -> iceberg::Result<()> {
    let path = unique_path(&harness, "test_file_io_write");
    let _ = harness.file_io.delete(&path).await;
    assert!(!harness.file_io.exists(&path).await.unwrap());

    let output_file = harness.file_io.new_output(&path).unwrap();
    output_file.write("123".into()).await.unwrap();
    assert!(harness.file_io.exists(&path).await.unwrap());

    let _ = harness.file_io.delete(&path).await;
    Ok(())
}

async fn run_read(harness: StorageHarness) -> iceberg::Result<()> {
    let path = unique_path(&harness, "test_file_io_read");
    let _ = harness.file_io.delete(&path).await;

    let output_file = harness.file_io.new_output(&path).unwrap();
    output_file.write("test_input".into()).await.unwrap();

    let input_file = harness.file_io.new_input(&path).unwrap();
    let buffer = input_file.read().await.unwrap();
    assert_eq!(buffer, "test_input".as_bytes());

    let _ = harness.file_io.delete(&path).await;
    Ok(())
}

async fn run_delete(harness: StorageHarness) -> iceberg::Result<()> {
    let path = unique_path(&harness, "test_file_io_delete");
    let _ = harness.file_io.delete(&path).await;

    harness
        .file_io
        .new_output(&path)
        .unwrap()
        .write("delete_me".into())
        .await
        .unwrap();
    assert!(harness.file_io.exists(&path).await.unwrap());

    harness.file_io.delete(&path).await.unwrap();
    assert!(!harness.file_io.exists(&path).await.unwrap());
    Ok(())
}

async fn run_delete_nonexistent(harness: StorageHarness) -> iceberg::Result<()> {
    let path = unique_path(&harness, "test_file_io_delete_nonexistent");
    harness.file_io.delete(&path).await.unwrap();
    Ok(())
}

async fn run_delete_stream(harness: StorageHarness) -> iceberg::Result<()> {
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

async fn run_delete_stream_empty(harness: StorageHarness) -> iceberg::Result<()> {
    let stream = futures::stream::empty().boxed();
    harness.file_io.delete_stream(stream).await.unwrap();
    Ok(())
}

async fn run_metadata(harness: StorageHarness) -> iceberg::Result<()> {
    let path = unique_path(&harness, "test_file_io_metadata");
    let _ = harness.file_io.delete(&path).await;
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
    let _ = harness.file_io.delete(&path).await;
    Ok(())
}

async fn run_range_read(harness: StorageHarness) -> iceberg::Result<()> {
    let path = unique_path(&harness, "test_file_io_range_read");
    let _ = harness.file_io.delete(&path).await;
    let content = b"0123456789abcdef";
    harness
        .file_io
        .new_output(&path)
        .unwrap()
        .write(Bytes::from_static(content))
        .await
        .unwrap();
    let input_file = harness.file_io.new_input(&path).unwrap();
    let reader = input_file.reader().await.unwrap();
    let range_data = reader.read(4..10).await.unwrap();
    assert_eq!(range_data.as_ref(), &content[4..10]);
    let _ = harness.file_io.delete(&path).await;
    Ok(())
}

async fn run_zero_byte_file(harness: StorageHarness) -> iceberg::Result<()> {
    let path = unique_path(&harness, "test_file_io_zero_byte_file");
    let _ = harness.file_io.delete(&path).await;

    let output_file = harness.file_io.new_output(&path).unwrap();
    output_file.write(Bytes::new()).await.unwrap();

    assert!(harness.file_io.exists(&path).await.unwrap());

    let input_file = harness.file_io.new_input(&path).unwrap();
    let metadata = input_file.metadata().await.unwrap();
    assert_eq!(metadata.size, 0);

    let data = input_file.read().await.unwrap();
    assert_eq!(data, Bytes::new());

    let _ = harness.file_io.delete(&path).await;
    Ok(())
}

async fn run_delete_stream_mixed(harness: StorageHarness) -> iceberg::Result<()> {
    let base = unique_path(&harness, "test_file_io_delete_stream_mixed");
    let existing_paths: Vec<String> = (0..3).map(|i| format!("{base}/exists-{i}")).collect();
    let nonexistent_paths: Vec<String> = (0..3).map(|i| format!("{base}/missing-{i}")).collect();

    for path in &existing_paths {
        let _ = harness.file_io.delete(path).await;
        harness
            .file_io
            .new_output(path)
            .unwrap()
            .write("data".into())
            .await
            .unwrap();
        assert!(harness.file_io.exists(path).await.unwrap());
    }
    for path in &nonexistent_paths {
        let _ = harness.file_io.delete(path).await;
        assert!(!harness.file_io.exists(path).await.unwrap());
    }

    let mut all_paths = existing_paths.clone();
    all_paths.extend(nonexistent_paths);

    let stream = futures::stream::iter(all_paths).boxed();
    harness.file_io.delete_stream(stream).await.unwrap();

    for path in &existing_paths {
        assert!(!harness.file_io.exists(path).await.unwrap());
    }
    Ok(())
}

async fn run_concurrent_writes(harness: StorageHarness) -> iceberg::Result<()> {
    let base = unique_path(&harness, "test_file_io_concurrent_writes");
    let mut handles = Vec::new();

    for i in 0..8 {
        let file_io = harness.file_io.clone();
        let path = format!("{base}/concurrent-{i}");
        let payload = format!("payload-{i}");

        handles.push(tokio::spawn(async move {
            let output = file_io.new_output(&path).unwrap();
            output.write(payload.clone().into()).await.unwrap();

            let input = file_io.new_input(&path).unwrap();
            let data = input.read().await.unwrap();
            assert_eq!(data, payload.as_bytes());

            let _ = file_io.delete(&path).await;
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Matrix Tests
// ---------------------------------------------------------------------------

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
#[case::opendal_gcs(StorageKind::OpenDalGcs)]
#[case::opendal_fs(StorageKind::OpenDalFs)]
#[case::opendal_memory(StorageKind::OpenDalMemory)]
#[tokio::test]
async fn test_file_io_exists(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };
    run_exists(harness).await
}

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
#[case::opendal_gcs(StorageKind::OpenDalGcs)]
#[case::opendal_fs(StorageKind::OpenDalFs)]
#[case::opendal_memory(StorageKind::OpenDalMemory)]
#[tokio::test]
async fn test_file_io_write(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };
    run_write(harness).await
}

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
#[case::opendal_gcs(StorageKind::OpenDalGcs)]
#[case::opendal_fs(StorageKind::OpenDalFs)]
#[case::opendal_memory(StorageKind::OpenDalMemory)]
#[tokio::test]
async fn test_file_io_read(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };
    run_read(harness).await
}

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
#[case::opendal_gcs(StorageKind::OpenDalGcs)]
#[case::opendal_fs(StorageKind::OpenDalFs)]
#[case::opendal_memory(StorageKind::OpenDalMemory)]
#[tokio::test]
async fn test_file_io_delete(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };
    run_delete(harness).await
}

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
#[case::opendal_gcs(StorageKind::OpenDalGcs)]
#[case::opendal_fs(StorageKind::OpenDalFs)]
#[case::opendal_memory(StorageKind::OpenDalMemory)]
#[tokio::test]
async fn test_file_io_delete_nonexistent(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };
    run_delete_nonexistent(harness).await
}

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
#[case::opendal_fs(StorageKind::OpenDalFs)]
#[case::opendal_memory(StorageKind::OpenDalMemory)]
// Note: fake-gcs-server emulator does not support batch delete (https://github.com/fsouza/fake-gcs-server/issues/1443)
#[tokio::test]
async fn test_file_io_delete_stream(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };
    run_delete_stream(harness).await
}

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
#[case::opendal_fs(StorageKind::OpenDalFs)]
#[case::opendal_memory(StorageKind::OpenDalMemory)]
#[tokio::test]
async fn test_file_io_delete_stream_empty(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };
    run_delete_stream_empty(harness).await
}

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
#[case::opendal_fs(StorageKind::OpenDalFs)]
#[case::opendal_memory(StorageKind::OpenDalMemory)]
#[tokio::test]
async fn test_file_io_delete_stream_mixed(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };
    run_delete_stream_mixed(harness).await
}

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
#[case::opendal_fs(StorageKind::OpenDalFs)]
#[case::opendal_memory(StorageKind::OpenDalMemory)]
#[tokio::test]
async fn test_file_io_delete_prefix(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };
    let prefix = unique_path(&harness, "test_file_io_delete_prefix");
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
    harness.file_io.delete_prefix(&prefix).await.unwrap();
    for path in &paths {
        assert!(!harness.file_io.exists(path).await.unwrap());
    }
    Ok(())
}

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
#[case::opendal_fs(StorageKind::OpenDalFs)]
#[case::opendal_memory(StorageKind::OpenDalMemory)]
#[tokio::test]
async fn test_file_io_delete_prefix_nonexistent(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };
    let prefix = unique_path(&harness, "test_file_io_delete_prefix_nonexistent");
    harness.file_io.delete_prefix(&prefix).await.unwrap();
    Ok(())
}

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
#[case::opendal_gcs(StorageKind::OpenDalGcs)]
#[case::opendal_fs(StorageKind::OpenDalFs)]
#[case::opendal_memory(StorageKind::OpenDalMemory)]
#[tokio::test]
async fn test_file_io_metadata(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };
    run_metadata(harness).await
}

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
#[case::opendal_gcs(StorageKind::OpenDalGcs)]
#[case::opendal_fs(StorageKind::OpenDalFs)]
#[case::opendal_memory(StorageKind::OpenDalMemory)]
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
#[case::opendal_fs(StorageKind::OpenDalFs)]
#[case::opendal_memory(StorageKind::OpenDalMemory)]
#[tokio::test]
async fn test_file_io_range_read(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };
    run_range_read(harness).await
}

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
#[case::opendal_gcs(StorageKind::OpenDalGcs)]
#[case::opendal_fs(StorageKind::OpenDalFs)]
#[case::opendal_memory(StorageKind::OpenDalMemory)]
#[tokio::test]
async fn test_file_io_range_read_out_of_bounds(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };
    let path = unique_path(&harness, "test_file_io_range_read_out_of_bounds");
    let _ = harness.file_io.delete(&path).await;
    let content = b"0123456789";
    harness
        .file_io
        .new_output(&path)
        .unwrap()
        .write(Bytes::from_static(content))
        .await
        .unwrap();

    let input_file = harness.file_io.new_input(&path).unwrap();
    let reader = input_file.reader().await.unwrap();
    let result = reader.read(100..200).await;
    assert!(result.is_err());

    let _ = harness.file_io.delete(&path).await;
    Ok(())
}

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
#[case::opendal_gcs(StorageKind::OpenDalGcs)]
#[case::opendal_fs(StorageKind::OpenDalFs)]
#[case::opendal_memory(StorageKind::OpenDalMemory)]
#[tokio::test]
async fn test_file_io_zero_byte_file(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };
    run_zero_byte_file(harness).await
}

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
#[case::opendal_gcs(StorageKind::OpenDalGcs)]
#[case::opendal_fs(StorageKind::OpenDalFs)]
#[case::opendal_memory(StorageKind::OpenDalMemory)]
#[tokio::test]
async fn test_file_io_concurrent_writes(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };
    run_concurrent_writes(harness).await
}

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
#[case::opendal_gcs(StorageKind::OpenDalGcs)]
#[case::opendal_fs(StorageKind::OpenDalFs)]
#[case::opendal_memory(StorageKind::OpenDalMemory)]
#[tokio::test]
async fn test_file_io_streaming_write(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };
    let path = unique_path(&harness, "test_file_io_streaming_write");
    let output_file = harness.file_io.new_output(&path).unwrap();
    let mut writer = output_file.writer().await.unwrap();
    writer
        .write(Bytes::from("streaming_content"))
        .await
        .unwrap();
    writer.close().await.unwrap();
    let input_file = harness.file_io.new_input(&path).unwrap();
    let buffer = input_file.read().await.unwrap();
    assert_eq!(buffer, Bytes::from("streaming_content"));
    Ok(())
}

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
#[case::opendal_gcs(StorageKind::OpenDalGcs)]
#[case::opendal_fs(StorageKind::OpenDalFs)]
#[case::opendal_memory(StorageKind::OpenDalMemory)]
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
    writer.write(Bytes::from("data")).await.unwrap();
    writer.close().await.unwrap();
    let result = writer.close().await;
    assert!(result.is_err());
    Ok(())
}

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
#[case::opendal_fs(StorageKind::OpenDalFs)]
#[case::opendal_memory(StorageKind::OpenDalMemory)]
#[tokio::test]
async fn test_file_io_serialization_roundtrip(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };
    let file_io = roundtrip_file_io(&harness.file_io);
    let path = unique_path(&harness, "test_file_io_serialization_roundtrip");

    let _ = file_io.delete(&path).await;
    file_io
        .new_output(&path)
        .unwrap()
        .write(Bytes::from_static(b"roundtrip"))
        .await
        .unwrap();
    assert_eq!(
        file_io.new_input(&path).unwrap().read().await.unwrap(),
        Bytes::from_static(b"roundtrip")
    );
    file_io.delete(&path).await.unwrap();
    assert!(!file_io.exists(&path).await.unwrap());
    Ok(())
}
