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
        S3_SECRET_ACCESS_KEY,
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
}
