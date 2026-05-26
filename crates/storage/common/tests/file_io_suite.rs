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

use common::{StorageHarness, StorageKind, load_storage, unique_path};
use futures::StreamExt;
use iceberg::io::{FileIOBuilder, HF_REVISION, HF_TOKEN, LocalFsStorageFactory};
use iceberg_storage_opendal::OpenDalStorageFactory;
use iceberg_test_utils::{normalize_test_name, set_up};
use rstest::rstest;

// ---------------------------------------------------------------------------
// Shared bodies — used by both the cross-backend rstest matrix and the HF
// `#[ignore]`-gated entry points below.
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
        .write(bytes::Bytes::from_static(content))
        .await
        .unwrap();
    let input_file = harness.file_io.new_input(&path).unwrap();
    let reader = input_file.reader().await.unwrap();
    let range_data = reader.read(4..10).await.unwrap();
    assert_eq!(range_data.as_ref(), &content[4..10]);
    let _ = harness.file_io.delete(&path).await;
    Ok(())
}

// ---------------------------------------------------------------------------
// Cross-backend matrix (S3 + GCS). HF runs only via the `#[ignore]`-gated
// functions further down so non-HF CI runs don't pull HF env vars.
// ---------------------------------------------------------------------------

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
#[case::opendal_gcs(StorageKind::OpenDalGcs)]
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
#[tokio::test]
async fn test_file_io_delete_nonexistent(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };
    run_delete_nonexistent(harness).await
}

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
// We use fake-gcs-server for testing and it doesn't support batch delete
// https://github.com/fsouza/fake-gcs-server/issues/1443
// #[case::opendal_gcs(StorageKind::OpenDalGcs)]
#[tokio::test]
async fn test_file_io_delete_stream(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };
    run_delete_stream(harness).await
}

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
// We use fake-gcs-server for testing and it doesn't support batch delete
// https://github.com/fsouza/fake-gcs-server/issues/1443
// #[case::opendal_gcs(StorageKind::OpenDalGcs)]
#[tokio::test]
async fn test_file_io_delete_stream_empty(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };
    run_delete_stream_empty(harness).await
}

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
// We use fake-gcs-server for testing and it doesn't support batch delete
// https://github.com/fsouza/fake-gcs-server/issues/1443
// #[case::opendal_gcs(StorageKind::OpenDalGcs)]
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
// We use fake-gcs-server for testing and it doesn't support batch delete
// https://github.com/fsouza/fake-gcs-server/issues/1443
// #[case::opendal_gcs(StorageKind::OpenDalGcs)]
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
    run_range_read(harness).await
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

// ---------------------------------------------------------------------------
// HuggingFace Hub tests
//
// All HF tests are marked `#[ignore]` so they only run when the HF workflow
// opts in via `--run-ignored=only` (nextest) or `-- --include-ignored` (cargo
// test). They expect `HF_TOKEN`, `HF_BUCKET`, and (where used) `HF_DATASET`
// to be set; absence of those env vars is treated as a hard failure when
// these tests are explicitly run, since the only reason to opt into them is
// the HF integration job.
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "HF integration; run via `--run-ignored=only` with HF_TOKEN/HF_BUCKET set"]
async fn test_file_io_hf_write() -> iceberg::Result<()> {
    let harness = load_storage(StorageKind::OpenDalHf)
        .await
        .expect("HF_TOKEN and HF_BUCKET must be set for HF tests");
    run_write(harness).await
}

#[tokio::test]
#[ignore = "HF integration; run via `--run-ignored=only` with HF_TOKEN/HF_BUCKET set"]
async fn test_file_io_hf_read() -> iceberg::Result<()> {
    let harness = load_storage(StorageKind::OpenDalHf)
        .await
        .expect("HF_TOKEN and HF_BUCKET must be set for HF tests");
    run_read(harness).await
}

#[tokio::test]
#[ignore = "HF integration; run via `--run-ignored=only` with HF_TOKEN/HF_BUCKET set"]
async fn test_file_io_hf_delete() -> iceberg::Result<()> {
    let harness = load_storage(StorageKind::OpenDalHf)
        .await
        .expect("HF_TOKEN and HF_BUCKET must be set for HF tests");
    run_delete(harness).await
}

#[tokio::test]
#[ignore = "HF integration; run via `--run-ignored=only` with HF_TOKEN/HF_BUCKET set"]
async fn test_file_io_hf_delete_stream() -> iceberg::Result<()> {
    let harness = load_storage(StorageKind::OpenDalHf)
        .await
        .expect("HF_TOKEN and HF_BUCKET must be set for HF tests");
    run_delete_stream(harness).await
}

#[tokio::test]
#[ignore = "HF integration; run via `--run-ignored=only` with HF_TOKEN/HF_BUCKET set"]
async fn test_file_io_hf_delete_stream_empty() -> iceberg::Result<()> {
    let harness = load_storage(StorageKind::OpenDalHf)
        .await
        .expect("HF_TOKEN and HF_BUCKET must be set for HF tests");
    run_delete_stream_empty(harness).await
}

#[tokio::test]
#[ignore = "HF integration; run via `--run-ignored=only` with HF_TOKEN/HF_BUCKET set"]
async fn test_file_io_hf_metadata() -> iceberg::Result<()> {
    let harness = load_storage(StorageKind::OpenDalHf)
        .await
        .expect("HF_TOKEN and HF_BUCKET must be set for HF tests");
    run_metadata(harness).await
}

#[tokio::test]
#[ignore = "HF integration; run via `--run-ignored=only` with HF_TOKEN/HF_BUCKET set"]
async fn test_file_io_hf_range_read() -> iceberg::Result<()> {
    let harness = load_storage(StorageKind::OpenDalHf)
        .await
        .expect("HF_TOKEN and HF_BUCKET must be set for HF tests");
    run_range_read(harness).await
}

// HF-specific behavior tests: overwrite, repo-typed paths, revisions.

#[tokio::test]
#[ignore = "HF integration; run via `--run-ignored=only` with HF_TOKEN/HF_BUCKET set"]
async fn test_file_io_hf_overwrite() -> iceberg::Result<()> {
    let harness = load_storage(StorageKind::OpenDalHf)
        .await
        .expect("HF_TOKEN and HF_BUCKET must be set for HF tests");
    let path = unique_path(&harness, "test_file_io_hf_overwrite");
    let _ = harness.file_io.delete(&path).await;

    harness
        .file_io
        .new_output(&path)
        .unwrap()
        .write(bytes::Bytes::from_static(b"first"))
        .await
        .unwrap();
    harness
        .file_io
        .new_output(&path)
        .unwrap()
        .write(bytes::Bytes::from_static(b"second"))
        .await
        .unwrap();

    let data = harness
        .file_io
        .new_input(&path)
        .unwrap()
        .read()
        .await
        .unwrap();
    assert_eq!(data, bytes::Bytes::from_static(b"second"));

    harness.file_io.delete(&path).await.unwrap();
    Ok(())
}

#[tokio::test]
#[ignore = "HF integration; run via `--run-ignored=only` with HF_TOKEN/HF_DATASET set"]
async fn test_file_io_hf_dataset_path() -> iceberg::Result<()> {
    let token = std::env::var(common::ENV_HF_TOKEN).expect("HF_TOKEN must be set for HF tests");
    let dataset =
        std::env::var(common::ENV_HF_DATASET).expect("HF_DATASET must be set for HF tests");
    set_up();

    let file_io = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::Hf))
        .with_props(vec![(HF_TOKEN, token)])
        .build();
    let path = format!(
        "hf://datasets/{}/{}",
        dataset,
        normalize_test_name("test_file_io_hf_dataset_path")
    );

    let _ = file_io.delete(&path).await;
    assert!(!file_io.exists(&path).await.unwrap());

    file_io
        .new_output(&path)
        .unwrap()
        .write(bytes::Bytes::from_static(b"iceberg-hf-dataset"))
        .await
        .unwrap();
    assert!(file_io.exists(&path).await.unwrap());

    let data = file_io.new_input(&path).unwrap().read().await.unwrap();
    assert_eq!(data, bytes::Bytes::from_static(b"iceberg-hf-dataset"));

    file_io.delete(&path).await.unwrap();
    assert!(!file_io.exists(&path).await.unwrap());
    Ok(())
}

#[tokio::test]
#[ignore = "HF integration; run via `--run-ignored=only` with HF_TOKEN/HF_BUCKET set"]
async fn test_file_io_hf_explicit_revision_in_uri() -> iceberg::Result<()> {
    let token = std::env::var(common::ENV_HF_TOKEN).expect("HF_TOKEN must be set for HF tests");
    let bucket = std::env::var(common::ENV_HF_BUCKET).expect("HF_BUCKET must be set for HF tests");
    set_up();

    let file_io = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::Hf))
        .with_props(vec![(HF_TOKEN, token)])
        .build();
    let name = normalize_test_name("test_file_io_hf_explicit_revision_in_uri");
    // Write without a revision, read back with explicit @main.
    let write_path = format!("hf://buckets/{bucket}/{name}");
    let read_path = format!("hf://buckets/{bucket}@main/{name}");

    let _ = file_io.delete(&write_path).await;
    file_io
        .new_output(&write_path)
        .unwrap()
        .write(bytes::Bytes::from_static(b"revision-test"))
        .await
        .unwrap();

    let data = file_io.new_input(&read_path).unwrap().read().await.unwrap();
    assert_eq!(data, bytes::Bytes::from_static(b"revision-test"));

    file_io.delete(&write_path).await.unwrap();
    Ok(())
}

#[tokio::test]
#[ignore = "HF integration; run via `--run-ignored=only` with HF_TOKEN/HF_BUCKET set"]
async fn test_file_io_hf_revision_from_config() -> iceberg::Result<()> {
    let token = std::env::var(common::ENV_HF_TOKEN).expect("HF_TOKEN must be set for HF tests");
    let bucket = std::env::var(common::ENV_HF_BUCKET).expect("HF_BUCKET must be set for HF tests");
    set_up();

    // Build FileIO with HF_REVISION set in config — paths without @revision use it.
    let file_io = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::Hf))
        .with_props(vec![(HF_TOKEN, token), (HF_REVISION, "main".to_string())])
        .build();
    let path = format!(
        "hf://buckets/{}/{}",
        bucket,
        normalize_test_name("test_file_io_hf_revision_from_config")
    );

    let _ = file_io.delete(&path).await;
    file_io
        .new_output(&path)
        .unwrap()
        .write(bytes::Bytes::from_static(b"config-revision"))
        .await
        .unwrap();

    let data = file_io.new_input(&path).unwrap().read().await.unwrap();
    assert_eq!(data, bytes::Bytes::from_static(b"config-revision"));

    file_io.delete(&path).await.unwrap();
    Ok(())
}
