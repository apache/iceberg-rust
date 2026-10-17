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

use common::{StorageKind, load_storage};
use iceberg_storage_common::file_io::*;
use rstest::rstest;

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
    run_file_io_exists(&harness).await
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
    run_file_io_write(&harness).await
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
    run_file_io_read(&harness).await
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
    run_file_io_delete(&harness).await
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
    run_file_io_delete_nonexistent(&harness).await
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
    run_file_io_delete_stream(&harness).await
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
    run_file_io_delete_stream_empty(&harness).await
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
    run_file_io_delete_stream_mixed(&harness).await
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
    run_file_io_delete_prefix(&harness).await
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
    run_file_io_delete_prefix_nonexistent(&harness).await
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
    run_file_io_metadata(&harness).await
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
    run_file_io_metadata_nonexistent(&harness).await
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
    run_file_io_range_read(&harness).await
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
    run_file_io_range_read_out_of_bounds(&harness).await
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
    run_file_io_zero_byte_file(&harness).await
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
    run_file_io_concurrent_writes(&harness).await
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
    run_file_io_streaming_write(&harness).await
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
    run_file_io_streaming_write_double_close(&harness).await
}

#[rstest]
#[case::opendal_s3(StorageKind::OpenDalS3)]
#[case::opendal_gcs(StorageKind::OpenDalGcs)]
#[case::opendal_fs(StorageKind::OpenDalFs)]
#[case::opendal_memory(StorageKind::OpenDalMemory)]
#[tokio::test]
async fn test_file_io_serialization_roundtrip(#[case] kind: StorageKind) -> iceberg::Result<()> {
    let Some(harness) = load_storage(kind).await else {
        return Ok(());
    };
    run_file_io_serialization_roundtrip(&harness).await
}
