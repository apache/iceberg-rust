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

//! Generic `FileIO` contract tests executable across all storage backends.

use bytes::Bytes;
use futures::StreamExt;
use iceberg::io::FileIO;

use crate::harness::StorageHarness;

/// Helper to serialize and deserialize a `FileIO` instance.
pub fn roundtrip_file_io(file_io: &FileIO) -> FileIO {
    let serialized = file_io
        .serialize_all()
        .expect("FileIO serialization must succeed");
    FileIO::deserialize_all(&serialized).expect("FileIO deserialization must succeed")
}

/// Tests that `exists` returns true for base directory and false for nonexistent path.
pub async fn run_file_io_exists(harness: &StorageHarness) -> iceberg::Result<()> {
    let non_existent = harness.unique_path("non_existent_file_that_does_not_exist");
    assert!(!harness.file_io.exists(&non_existent).await?);
    assert!(harness.file_io.exists(&harness.base_path).await?);
    Ok(())
}

/// Tests basic write and existence validation.
pub async fn run_file_io_write(harness: &StorageHarness) -> iceberg::Result<()> {
    let path = harness.unique_path("test_file_io_write");
    let _ = harness.file_io.delete(&path).await;
    assert!(!harness.file_io.exists(&path).await?);

    let output_file = harness.file_io.new_output(&path)?;
    output_file.write("123".into()).await?;
    assert!(harness.file_io.exists(&path).await?);

    let _ = harness.file_io.delete(&path).await;
    Ok(())
}

/// Tests write followed by reading back contents.
pub async fn run_file_io_read(harness: &StorageHarness) -> iceberg::Result<()> {
    let path = harness.unique_path("test_file_io_read");
    let _ = harness.file_io.delete(&path).await;

    let output_file = harness.file_io.new_output(&path)?;
    output_file.write("test_input".into()).await?;

    let input_file = harness.file_io.new_input(&path)?;
    let buffer = input_file.read().await?;
    assert_eq!(buffer, "test_input".as_bytes());

    let _ = harness.file_io.delete(&path).await;
    Ok(())
}

/// Tests writing and deleting a file.
pub async fn run_file_io_delete(harness: &StorageHarness) -> iceberg::Result<()> {
    let path = harness.unique_path("test_file_io_delete");
    let _ = harness.file_io.delete(&path).await;

    harness
        .file_io
        .new_output(&path)?
        .write("delete_me".into())
        .await?;
    assert!(harness.file_io.exists(&path).await?);

    harness.file_io.delete(&path).await?;
    assert!(!harness.file_io.exists(&path).await?);
    Ok(())
}

/// Tests deleting a nonexistent file (must succeed without error).
pub async fn run_file_io_delete_nonexistent(harness: &StorageHarness) -> iceberg::Result<()> {
    let path = harness.unique_path("test_file_io_delete_nonexistent");
    harness.file_io.delete(&path).await?;
    Ok(())
}

/// Tests bulk batch deletion via stream.
pub async fn run_file_io_delete_stream(harness: &StorageHarness) -> iceberg::Result<()> {
    let base = harness.unique_path("test_file_io_delete_stream");
    let paths: Vec<String> = (0..5).map(|i| format!("{base}/file-{i}")).collect();
    for path in &paths {
        let _ = harness.file_io.delete(path).await;
        harness
            .file_io
            .new_output(path)?
            .write("delete-me".into())
            .await?;
        assert!(harness.file_io.exists(path).await?);
    }
    let stream = futures::stream::iter(paths.clone()).boxed();
    harness.file_io.delete_stream(stream).await?;
    for path in &paths {
        assert!(!harness.file_io.exists(path).await?);
    }
    Ok(())
}

/// Tests empty delete stream (must succeed).
pub async fn run_file_io_delete_stream_empty(harness: &StorageHarness) -> iceberg::Result<()> {
    let stream = futures::stream::empty().boxed();
    harness.file_io.delete_stream(stream).await?;
    Ok(())
}

/// Tests bulk delete stream containing mixed existing and nonexistent paths.
pub async fn run_file_io_delete_stream_mixed(harness: &StorageHarness) -> iceberg::Result<()> {
    let base = harness.unique_path("test_file_io_delete_stream_mixed");
    let existing_paths: Vec<String> = (0..3).map(|i| format!("{base}/exists-{i}")).collect();
    let nonexistent_paths: Vec<String> = (0..3).map(|i| format!("{base}/missing-{i}")).collect();

    for path in &existing_paths {
        let _ = harness.file_io.delete(path).await;
        harness
            .file_io
            .new_output(path)?
            .write("data".into())
            .await?;
        assert!(harness.file_io.exists(path).await?);
    }
    for path in &nonexistent_paths {
        let _ = harness.file_io.delete(path).await;
        assert!(!harness.file_io.exists(path).await?);
    }

    let mut all_paths = existing_paths.clone();
    all_paths.extend(nonexistent_paths);

    let stream = futures::stream::iter(all_paths).boxed();
    harness.file_io.delete_stream(stream).await?;

    for path in &existing_paths {
        assert!(!harness.file_io.exists(path).await?);
    }
    Ok(())
}

/// Tests prefix deletion.
pub async fn run_file_io_delete_prefix(harness: &StorageHarness) -> iceberg::Result<()> {
    let prefix = harness.unique_path("test_file_io_delete_prefix");
    let paths: Vec<String> = (0..3).map(|i| format!("{prefix}/file-{i}")).collect();
    for path in &paths {
        harness
            .file_io
            .new_output(path)?
            .write("data".into())
            .await?;
        assert!(harness.file_io.exists(path).await?);
    }
    harness.file_io.delete_prefix(&prefix).await?;
    for path in &paths {
        assert!(!harness.file_io.exists(path).await?);
    }
    Ok(())
}

/// Tests deleting a nonexistent prefix (must succeed).
pub async fn run_file_io_delete_prefix_nonexistent(
    harness: &StorageHarness,
) -> iceberg::Result<()> {
    let prefix = harness.unique_path("test_file_io_delete_prefix_nonexistent");
    harness.file_io.delete_prefix(&prefix).await?;
    Ok(())
}

/// Tests file metadata inspection.
pub async fn run_file_io_metadata(harness: &StorageHarness) -> iceberg::Result<()> {
    let path = harness.unique_path("test_file_io_metadata");
    let _ = harness.file_io.delete(&path).await;
    let content = "metadata_test_content";
    harness
        .file_io
        .new_output(&path)?
        .write(content.into())
        .await?;
    let input_file = harness.file_io.new_input(&path)?;
    let metadata = input_file.metadata().await?;
    assert_eq!(metadata.size, content.len() as u64);
    let _ = harness.file_io.delete(&path).await;
    Ok(())
}

/// Tests metadata on nonexistent file returns an error.
pub async fn run_file_io_metadata_nonexistent(harness: &StorageHarness) -> iceberg::Result<()> {
    let path = harness.unique_path("test_file_io_metadata_nonexistent");
    let input_file = harness.file_io.new_input(&path)?;
    assert!(input_file.metadata().await.is_err());
    Ok(())
}

/// Tests byte range reading.
pub async fn run_file_io_range_read(harness: &StorageHarness) -> iceberg::Result<()> {
    let path = harness.unique_path("test_file_io_range_read");
    let _ = harness.file_io.delete(&path).await;
    let content = b"0123456789abcdef";
    harness
        .file_io
        .new_output(&path)?
        .write(Bytes::from_static(content))
        .await?;
    let input_file = harness.file_io.new_input(&path)?;
    let reader = input_file.reader().await?;
    let range_data = reader.read(4..10).await?;
    assert_eq!(range_data.as_ref(), &content[4..10]);
    let _ = harness.file_io.delete(&path).await;
    Ok(())
}

/// Tests out-of-bounds byte range read behavior.
pub async fn run_file_io_range_read_out_of_bounds(harness: &StorageHarness) -> iceberg::Result<()> {
    let path = harness.unique_path("test_file_io_range_read_out_of_bounds");
    let _ = harness.file_io.delete(&path).await;
    let content = b"0123456789";
    harness
        .file_io
        .new_output(&path)?
        .write(Bytes::from_static(content))
        .await?;

    let input_file = harness.file_io.new_input(&path)?;
    let reader = input_file.reader().await?;
    let result = reader.read(100..200).await;

    // TODO: Standardize out-of-bounds range-read behavior in `FileRead` trait docs.
    // Cloud backends (S3/GCS via HTTP 416) or object_store may surface an error,
    // while POSIX/memory backends may return an empty buffer (EOF semantics).
    // Accept either an error or 0 bytes until the contract is formally specified.
    if let Ok(bytes) = result {
        assert!(
            bytes.is_empty(),
            "expected empty read for out-of-bounds range, got {} bytes",
            bytes.len()
        );
    }

    let _ = harness.file_io.delete(&path).await;
    Ok(())
}

/// Tests 0-byte file write, existence, metadata, and read.
pub async fn run_file_io_zero_byte_file(harness: &StorageHarness) -> iceberg::Result<()> {
    let path = harness.unique_path("test_file_io_zero_byte_file");
    let _ = harness.file_io.delete(&path).await;

    let output_file = harness.file_io.new_output(&path)?;
    output_file.write(Bytes::new()).await?;

    assert!(harness.file_io.exists(&path).await?);

    let input_file = harness.file_io.new_input(&path)?;
    let metadata = input_file.metadata().await?;
    assert_eq!(metadata.size, 0);

    let data = input_file.read().await?;
    assert_eq!(data, Bytes::new());

    let _ = harness.file_io.delete(&path).await;
    Ok(())
}

/// Tests concurrent writes from multiple spawned tokio tasks.
pub async fn run_file_io_concurrent_writes(harness: &StorageHarness) -> iceberg::Result<()> {
    let base = harness.unique_path("test_file_io_concurrent_writes");
    let mut handles = Vec::new();

    for i in 0..8 {
        let file_io = harness.file_io.clone();
        let path = format!("{base}/concurrent-{i}");
        let payload = format!("payload-{i}");

        handles.push(tokio::spawn(async move {
            let output = file_io.new_output(&path)?;
            output.write(payload.clone().into()).await?;

            let input = file_io.new_input(&path)?;
            let data = input.read().await?;
            assert_eq!(data, payload.as_bytes());

            let _ = file_io.delete(&path).await;
            Ok::<(), iceberg::Error>(())
        }));
    }

    for handle in handles {
        handle.await.expect("task join failed")?;
    }

    Ok(())
}

/// Tests streaming multipart writer.
pub async fn run_file_io_streaming_write(harness: &StorageHarness) -> iceberg::Result<()> {
    let path = harness.unique_path("test_file_io_streaming_write");
    let output_file = harness.file_io.new_output(&path)?;
    let mut writer = output_file.writer().await?;
    writer.write(Bytes::from("streaming_content")).await?;
    writer.close().await?;
    let input_file = harness.file_io.new_input(&path)?;
    let buffer = input_file.read().await?;
    assert_eq!(buffer, Bytes::from("streaming_content"));
    let _ = harness.file_io.delete(&path).await;
    Ok(())
}

/// Tests double-closing a writer returns an error.
pub async fn run_file_io_streaming_write_double_close(
    harness: &StorageHarness,
) -> iceberg::Result<()> {
    let path = harness.unique_path("test_file_io_streaming_write_double_close");
    let output_file = harness.file_io.new_output(&path)?;
    let mut writer = output_file.writer().await?;
    writer.write(Bytes::from("data")).await?;
    writer.close().await?;
    assert!(writer.close().await.is_err());
    let _ = harness.file_io.delete(&path).await;
    Ok(())
}

/// Tests serialization and deserialization roundtrip.
pub async fn run_file_io_serialization_roundtrip(harness: &StorageHarness) -> iceberg::Result<()> {
    let file_io = roundtrip_file_io(&harness.file_io);
    let path = harness.unique_path("test_file_io_serialization_roundtrip");

    let _ = file_io.delete(&path).await;
    file_io
        .new_output(&path)?
        .write(Bytes::from_static(b"roundtrip"))
        .await?;
    assert_eq!(
        file_io.new_input(&path)?.read().await?,
        Bytes::from_static(b"roundtrip")
    );
    file_io.delete(&path).await?;
    assert!(!file_io.exists(&path).await?);
    Ok(())
}

/// Run all basic `FileIO` contract tests sequentially against a storage harness.
pub async fn run_all_file_io_contract_tests(harness: &StorageHarness) -> iceberg::Result<()> {
    run_file_io_exists(harness).await?;
    run_file_io_write(harness).await?;
    run_file_io_read(harness).await?;
    run_file_io_delete(harness).await?;
    run_file_io_delete_nonexistent(harness).await?;
    run_file_io_metadata(harness).await?;
    run_file_io_range_read(harness).await?;
    run_file_io_zero_byte_file(harness).await?;
    run_file_io_concurrent_writes(harness).await?;
    run_file_io_streaming_write(harness).await?;
    run_file_io_serialization_roundtrip(harness).await?;
    Ok(())
}
