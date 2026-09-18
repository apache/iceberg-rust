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

//! Integration tests for FileIO S3 using object_store backend.
//!
//! These tests assume Docker containers are started externally via `make docker-up`.
//! Each test uses unique file paths based on module path to avoid conflicts.

#[cfg(feature = "object_store-s3")]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use futures::StreamExt;
    use iceberg::io::{
        FileIO, FileIOBuilder, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION,
        S3_SECRET_ACCESS_KEY, S3_SSE_KEY, S3_SSE_TYPE,
    };
    use iceberg_storage_object_store::ObjectStoreStorageFactory;
    use iceberg_test_utils::{get_minio_endpoint, normalize_test_name_with_parts, set_up};

    async fn get_file_io() -> FileIO {
        set_up();

        let minio_endpoint = get_minio_endpoint();

        FileIOBuilder::new(Arc::new(ObjectStoreStorageFactory::S3))
            .with_props(vec![
                (S3_ENDPOINT, minio_endpoint),
                (S3_ACCESS_KEY_ID, "admin".to_string()),
                (S3_SECRET_ACCESS_KEY, "password".to_string()),
                (S3_REGION, "us-east-1".to_string()),
                (S3_PATH_STYLE_ACCESS, "true".to_string()),
            ])
            .build()
    }

    fn roundtrip_file_io(file_io: &FileIO) -> FileIO {
        let serialized = file_io.serialize_all().unwrap();
        FileIO::deserialize_all(&serialized).unwrap()
    }

    #[tokio::test]
    async fn test_file_io_s3_serialization_roundtrip() {
        let file_io = roundtrip_file_io(&get_file_io().await);
        let path = format!(
            "s3://bucket1/{}",
            normalize_test_name_with_parts!("test_file_io_s3_serialization_roundtrip")
        );

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
    }

    #[tokio::test]
    async fn test_file_io_s3_exists() {
        let file_io = get_file_io().await;
        assert!(!file_io.exists("s3://bucket2/any").await.unwrap());
        assert!(file_io.exists("s3://bucket1/").await.unwrap());
    }

    #[tokio::test]
    async fn test_file_io_s3_output() {
        let file_io = get_file_io().await;
        let output_path = format!(
            "s3://bucket1/{}",
            normalize_test_name_with_parts!("test_file_io_s3_output")
        );
        let _ = file_io.delete(&output_path).await;
        assert!(!file_io.exists(&output_path).await.unwrap());
        let output_file = file_io.new_output(&output_path).unwrap();
        {
            output_file.write("123".into()).await.unwrap();
        }
        assert!(file_io.exists(&output_path).await.unwrap());
    }

    #[tokio::test]
    async fn test_file_io_s3_input() {
        let file_io = get_file_io().await;
        let file_path = format!(
            "s3://bucket1/{}",
            normalize_test_name_with_parts!("test_file_io_s3_input")
        );
        let output_file = file_io.new_output(&file_path).unwrap();
        {
            output_file.write("test_input".into()).await.unwrap();
        }

        let input_file = file_io.new_input(&file_path).unwrap();
        {
            let buffer = input_file.read().await.unwrap();
            assert_eq!(buffer, "test_input".as_bytes());
        }
    }

    #[tokio::test]
    async fn test_file_io_s3_delete_stream() {
        let file_io = get_file_io().await;

        let paths: Vec<String> = (0..5)
            .map(|i| {
                format!(
                    "s3://bucket1/{}/file-{i}",
                    normalize_test_name_with_parts!("test_file_io_s3_delete_stream")
                )
            })
            .collect();
        for path in &paths {
            let _ = file_io.delete(path).await;
            file_io
                .new_output(path)
                .unwrap()
                .write("delete-me".into())
                .await
                .unwrap();
            assert!(file_io.exists(path).await.unwrap());
        }

        let stream = futures::stream::iter(paths.clone()).boxed();
        file_io.delete_stream(stream).await.unwrap();

        for path in &paths {
            assert!(!file_io.exists(path).await.unwrap());
        }
    }

    #[tokio::test]
    async fn test_file_io_s3_delete_stream_empty() {
        let file_io = get_file_io().await;
        let stream = futures::stream::empty().boxed();
        file_io.delete_stream(stream).await.unwrap();
    }

    #[tokio::test]
    async fn test_file_io_s3_delete_stream_invalid_url() {
        let file_io = get_file_io().await;
        let stream = futures::stream::iter(vec!["invalid-url".to_string()]).boxed();
        let res = file_io.delete_stream(stream).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_file_io_s3_multipart_writer() {
        let file_io = get_file_io().await;
        let file_path = format!(
            "s3://bucket1/{}",
            normalize_test_name_with_parts!("test_file_io_s3_multipart_writer")
        );
        let _ = file_io.delete(&file_path).await;

        let output_file = file_io.new_output(&file_path).unwrap();
        let mut writer = output_file.writer().await.unwrap();

        let chunk1 = Bytes::from_static(b"hello ");
        let chunk2 = Bytes::from_static(b"multipart ");
        let chunk3 = Bytes::from_static(b"world!");

        writer.write(chunk1).await.unwrap();
        writer.write(chunk2).await.unwrap();
        writer.write(chunk3).await.unwrap();
        writer.close().await.unwrap();

        assert!(file_io.exists(&file_path).await.unwrap());
        let input_file = file_io.new_input(&file_path).unwrap();
        let content = input_file.read().await.unwrap();
        assert_eq!(content, Bytes::from_static(b"hello multipart world!"));

        file_io.delete(&file_path).await.unwrap();
        assert!(!file_io.exists(&file_path).await.unwrap());
    }

    #[tokio::test]
    async fn test_file_io_s3_multipart_writer_drop_aborts() {
        let file_io = get_file_io().await;
        let file_path = format!(
            "s3://bucket1/{}",
            normalize_test_name_with_parts!("test_file_io_s3_multipart_writer_drop_aborts")
        );
        let _ = file_io.delete(&file_path).await;

        let output_file = file_io.new_output(&file_path).unwrap();
        let mut writer = output_file.writer().await.unwrap();
        writer
            .write(Bytes::from_static(b"uncommitted chunk"))
            .await
            .unwrap();

        // Dropping writer without close() should abort the multipart upload
        drop(writer);

        // Give background abort task a moment to execute
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        assert!(!file_io.exists(&file_path).await.unwrap());
    }

    #[tokio::test]
    async fn test_file_io_s3_percent_encoded_bucket() {
        let file_io = get_file_io().await;
        let file_path = format!(
            "s3://bucket%31/{}",
            normalize_test_name_with_parts!("test_file_io_s3_percent_encoded_bucket")
        );
        let canonical_path = format!(
            "s3://bucket1/{}",
            normalize_test_name_with_parts!("test_file_io_s3_percent_encoded_bucket")
        );

        let _ = file_io.delete(&file_path).await;
        file_io
            .new_output(&file_path)
            .unwrap()
            .write(Bytes::from_static(b"encoded-bucket-content"))
            .await
            .unwrap();

        assert!(file_io.exists(&file_path).await.unwrap());
        assert!(file_io.exists(&canonical_path).await.unwrap());

        let content = file_io
            .new_input(&canonical_path)
            .unwrap()
            .read()
            .await
            .unwrap();
        assert_eq!(content, Bytes::from_static(b"encoded-bucket-content"));

        file_io.delete(&canonical_path).await.unwrap();
        assert!(!file_io.exists(&file_path).await.unwrap());
    }

    #[tokio::test]
    async fn test_file_io_s3_sse_kms_default() {
        set_up();
        let endpoint = get_minio_endpoint();

        let file_io = FileIOBuilder::new(Arc::new(ObjectStoreStorageFactory::S3))
            .with_props(vec![
                (S3_ENDPOINT, endpoint),
                (S3_ACCESS_KEY_ID, "admin".to_string()),
                (S3_SECRET_ACCESS_KEY, "password".to_string()),
                (S3_REGION, "us-east-1".to_string()),
                (S3_PATH_STYLE_ACCESS, "true".to_string()),
                (S3_SSE_TYPE, "kms".to_string()),
            ])
            .build();

        let file_path = format!(
            "s3://bucket1/{}",
            normalize_test_name_with_parts!("test_file_io_s3_sse_kms_default")
        );

        let _ = file_io.delete(&file_path).await;
        match file_io
            .new_output(&file_path)
            .unwrap()
            .write(Bytes::from_static(b"kms-encrypted-data"))
            .await
        {
            Ok(_) => {
                assert!(file_io.exists(&file_path).await.unwrap());
                let content = file_io.new_input(&file_path).unwrap().read().await.unwrap();
                assert_eq!(content, Bytes::from_static(b"kms-encrypted-data"));
                file_io.delete(&file_path).await.unwrap();
            }
            Err(e)
                if e.to_string().contains("501")
                    || e.to_string().contains("NotImplemented")
                    || e.to_string().contains("KMS is not configured") =>
            {
                // MinIO without KES does not configure KMS; passing 501 verifies header was sent
            }
            Err(e) => panic!("Unexpected error: {e:?}"),
        }
    }

    #[tokio::test]
    async fn test_file_io_s3_sse_kms_custom_key() {
        set_up();
        let endpoint = get_minio_endpoint();

        let file_io = FileIOBuilder::new(Arc::new(ObjectStoreStorageFactory::S3))
            .with_props(vec![
                (S3_ENDPOINT, endpoint),
                (S3_ACCESS_KEY_ID, "admin".to_string()),
                (S3_SECRET_ACCESS_KEY, "password".to_string()),
                (S3_REGION, "us-east-1".to_string()),
                (S3_PATH_STYLE_ACCESS, "true".to_string()),
                (S3_SSE_TYPE, "kms".to_string()),
                (
                    S3_SSE_KEY,
                    "arn:aws:kms:us-east-1:000000000000:key/a4644f9c-2149-414e-b6a6-a8e82cd6b69e"
                        .to_string(),
                ),
            ])
            .build();

        let file_path = format!(
            "s3://bucket1/{}",
            normalize_test_name_with_parts!("test_file_io_s3_sse_kms_custom_key")
        );

        let _ = file_io.delete(&file_path).await;
        match file_io
            .new_output(&file_path)
            .unwrap()
            .write(Bytes::from_static(b"kms-custom-key-encrypted-data"))
            .await
        {
            Ok(_) => {
                assert!(file_io.exists(&file_path).await.unwrap());
                let content = file_io.new_input(&file_path).unwrap().read().await.unwrap();
                assert_eq!(
                    content,
                    Bytes::from_static(b"kms-custom-key-encrypted-data")
                );
                file_io.delete(&file_path).await.unwrap();
            }
            Err(e)
                if e.to_string().contains("501")
                    || e.to_string().contains("NotImplemented")
                    || e.to_string().contains("KMS is not configured") =>
            {
                // MinIO without KES does not configure KMS; passing 501 verifies header was sent
            }
            Err(e) => panic!("Unexpected error: {e:?}"),
        }
    }

    /// Writes 12 MiB (3 × 4 MiB chunks) to exercise real S3 multipart uploads
    /// past the 10 MiB `WriteMultipart` buffer threshold, then reads back and
    /// verifies byte-for-byte integrity.
    #[tokio::test]
    async fn test_file_io_s3_multipart_writer_past_threshold() {
        let file_io = get_file_io().await;
        let file_path = format!(
            "s3://bucket1/{}",
            normalize_test_name_with_parts!("test_file_io_s3_multipart_writer_past_threshold")
        );
        let _ = file_io.delete(&file_path).await;

        // 4 MiB chunk with deterministic pattern (repeating 0..=255)
        const CHUNK_SIZE: usize = 4 * 1024 * 1024;
        let pattern: Vec<u8> = (0..CHUNK_SIZE).map(|i| (i % 256) as u8).collect();
        let chunk = Bytes::from(pattern.clone());

        let output_file = file_io.new_output(&file_path).unwrap();
        let mut writer = output_file.writer().await.unwrap();

        // Write 3 chunks = 12 MiB total (past 10 MiB threshold)
        for _ in 0..3 {
            writer.write(chunk.clone()).await.unwrap();
        }
        writer.close().await.unwrap();

        // Read back and verify
        let content = file_io.new_input(&file_path).unwrap().read().await.unwrap();
        assert_eq!(content.len(), 3 * CHUNK_SIZE);
        for i in 0..3 {
            assert_eq!(
                &content[i * CHUNK_SIZE..(i + 1) * CHUNK_SIZE],
                &pattern[..],
                "chunk {i} mismatch"
            );
        }

        file_io.delete(&file_path).await.unwrap();
    }

    /// Creates 15 files under `test_prefix/` and 2 under `other_prefix/`,
    /// calls `delete_prefix` on `test_prefix/`, then asserts all 15 are gone
    /// and the 2 outside the prefix are untouched.
    #[tokio::test]
    async fn test_file_io_s3_delete_prefix_bulk() {
        let file_io = get_file_io().await;
        let base = normalize_test_name_with_parts!("test_file_io_s3_delete_prefix_bulk");
        let target_prefix = format!("s3://bucket1/{base}/test_prefix");
        let other_prefix = format!("s3://bucket1/{base}/other_prefix");

        // Create 15 files under test_prefix/
        for i in 0..15 {
            let path = format!("{target_prefix}/file_{i}");
            let _ = file_io.delete(&path).await;
            file_io
                .new_output(&path)
                .unwrap()
                .write(Bytes::from(format!("data-{i}")))
                .await
                .unwrap();
        }

        // Create 2 files under other_prefix/
        let keep_paths: Vec<String> = (0..2).map(|i| format!("{other_prefix}/keep_{i}")).collect();
        for path in &keep_paths {
            let _ = file_io.delete(path).await;
            file_io
                .new_output(path)
                .unwrap()
                .write(Bytes::from_static(b"keep-me"))
                .await
                .unwrap();
        }

        // Bulk delete under test_prefix/
        file_io.delete_prefix(&target_prefix).await.unwrap();

        // Assert all 15 test_prefix files are gone
        for i in 0..15 {
            let path = format!("{target_prefix}/file_{i}");
            assert!(
                !file_io.exists(&path).await.unwrap(),
                "file_{i} should be deleted"
            );
        }

        // Assert the 2 other_prefix files still exist
        for path in &keep_paths {
            assert!(
                file_io.exists(path).await.unwrap(),
                "{path} should still exist"
            );
        }

        // Clean up
        for path in &keep_paths {
            file_io.delete(path).await.unwrap();
        }
    }

    /// Writes a 1024-byte payload and verifies range reads return exact slices.
    #[tokio::test]
    async fn test_file_io_s3_range_reader() {
        let file_io = get_file_io().await;
        let file_path = format!(
            "s3://bucket1/{}",
            normalize_test_name_with_parts!("test_file_io_s3_range_reader")
        );
        let _ = file_io.delete(&file_path).await;

        // 1024 bytes: 0..=255 repeated 4 times
        let payload: Vec<u8> = (0..1024).map(|i| (i % 256) as u8).collect();
        file_io
            .new_output(&file_path)
            .unwrap()
            .write(Bytes::from(payload.clone()))
            .await
            .unwrap();

        let reader = file_io
            .new_input(&file_path)
            .unwrap()
            .reader()
            .await
            .unwrap();

        // Test various ranges
        let r1 = reader.read(0..10).await.unwrap();
        assert_eq!(&r1[..], &payload[0..10]);

        let r2 = reader.read(10..50).await.unwrap();
        assert_eq!(&r2[..], &payload[10..50]);

        let r3 = reader.read(100..200).await.unwrap();
        assert_eq!(&r3[..], &payload[100..200]);

        // Cross the 256-byte pattern boundary
        let r4 = reader.read(250..260).await.unwrap();
        assert_eq!(&r4[..], &payload[250..260]);

        // Last 10 bytes
        let r5 = reader.read(1014..1024).await.unwrap();
        assert_eq!(&r5[..], &payload[1014..1024]);

        file_io.delete(&file_path).await.unwrap();
    }

    #[tokio::test]
    async fn test_file_io_s3_sse_s3_aes256() {
        set_up();
        let endpoint = get_minio_endpoint();

        let file_io = FileIOBuilder::new(Arc::new(ObjectStoreStorageFactory::S3))
            .with_props(vec![
                (S3_ENDPOINT, endpoint),
                (S3_ACCESS_KEY_ID, "admin".to_string()),
                (S3_SECRET_ACCESS_KEY, "password".to_string()),
                (S3_REGION, "us-east-1".to_string()),
                (S3_PATH_STYLE_ACCESS, "true".to_string()),
                (S3_SSE_TYPE, "s3".to_string()),
            ])
            .build();

        let file_path = format!(
            "s3://bucket1/{}",
            normalize_test_name_with_parts!("test_file_io_s3_sse_s3_aes256")
        );

        let _ = file_io.delete(&file_path).await;
        match file_io
            .new_output(&file_path)
            .unwrap()
            .write(Bytes::from_static(b"aes256-encrypted-data"))
            .await
        {
            Ok(_) => {
                assert!(file_io.exists(&file_path).await.unwrap());
                let content = file_io.new_input(&file_path).unwrap().read().await.unwrap();
                assert_eq!(content, Bytes::from_static(b"aes256-encrypted-data"));
                file_io.delete(&file_path).await.unwrap();
            }
            Err(e)
                if e.to_string().contains("501")
                    || e.to_string().contains("NotImplemented")
                    || e.to_string().contains("KMS is not configured") =>
            {
                // MinIO without KES does not configure server-side encryption; passing 501 verifies header was sent
            }
            Err(e) => panic!("Unexpected error: {e:?}"),
        }
    }
}
