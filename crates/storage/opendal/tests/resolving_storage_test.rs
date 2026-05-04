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

//! Integration tests for OpenDalResolvingStorage.
//!
//! These tests assume Docker containers are started externally via `make docker-up`.
//! Each test uses unique file paths based on module path to avoid conflicts.

#[cfg(all(
    feature = "opendal-s3",
    feature = "opendal-fs",
    feature = "opendal-memory"
))]
mod tests {
    use std::sync::Arc;

    use iceberg::io::{
        FileIOBuilder, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION,
        S3_SECRET_ACCESS_KEY,
    };
    use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
    use iceberg_test_utils::{get_minio_endpoint, normalize_test_name_with_parts, set_up};

    fn get_resolving_file_io() -> iceberg::io::FileIO {
        set_up();

        let minio_endpoint = get_minio_endpoint();

        FileIOBuilder::new(Arc::new(OpenDalResolvingStorageFactory::new()))
            .with_props(vec![
                (S3_ENDPOINT, minio_endpoint),
                (S3_ACCESS_KEY_ID, "admin".to_string()),
                (S3_SECRET_ACCESS_KEY, "password".to_string()),
                (S3_REGION, "us-east-1".to_string()),
                (S3_PATH_STYLE_ACCESS, "true".to_string()),
            ])
            .build()
    }

    fn temp_fs_path(name: &str) -> String {
        let dir = std::env::temp_dir().join("iceberg_resolving_tests");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        // Clean up from previous runs
        let _ = std::fs::remove_file(&path);
        format!("file:/{}", path.display())
    }

    #[tokio::test]
    async fn test_mixed_scheme_write_and_read() {
        let file_io = get_resolving_file_io();

        let s3_path = format!(
            "s3://bucket1/{}",
            normalize_test_name_with_parts!("test_mixed_scheme_write_and_read")
        );
        let fs_path = temp_fs_path("mixed_write_and_read.txt");
        let mem_path = "memory://test_mixed_scheme_write_and_read";

        // Write to all three schemes
        file_io
            .new_output(&s3_path)
            .unwrap()
            .write("from_s3".into())
            .await
            .unwrap();
        file_io
            .new_output(&fs_path)
            .unwrap()
            .write("from_fs".into())
            .await
            .unwrap();
        file_io
            .new_output(mem_path)
            .unwrap()
            .write("from_memory".into())
            .await
            .unwrap();

        // Read back from all three
        assert_eq!(
            file_io.new_input(&s3_path).unwrap().read().await.unwrap(),
            bytes::Bytes::from("from_s3")
        );
        assert_eq!(
            file_io.new_input(&fs_path).unwrap().read().await.unwrap(),
            bytes::Bytes::from("from_fs")
        );
        assert_eq!(
            file_io.new_input(mem_path).unwrap().read().await.unwrap(),
            bytes::Bytes::from("from_memory")
        );
    }

    #[tokio::test]
    async fn test_mixed_scheme_exists_independently() {
        let file_io = get_resolving_file_io();

        let s3_path = format!(
            "s3://bucket1/{}",
            normalize_test_name_with_parts!("test_mixed_scheme_exists_independently")
        );
        let fs_path = temp_fs_path("mixed_exists_independently.txt");
        let mem_path = "memory://test_mixed_scheme_exists_independently";

        // Clean up S3 from previous runs
        let _ = file_io.delete(&s3_path).await;

        // None exist initially
        assert!(!file_io.exists(&s3_path).await.unwrap());
        assert!(!file_io.exists(&fs_path).await.unwrap());
        assert!(!file_io.exists(mem_path).await.unwrap());

        // Write only to fs
        file_io
            .new_output(&fs_path)
            .unwrap()
            .write("fs_only".into())
            .await
            .unwrap();

        // Only fs exists
        assert!(!file_io.exists(&s3_path).await.unwrap());
        assert!(file_io.exists(&fs_path).await.unwrap());
        assert!(!file_io.exists(mem_path).await.unwrap());
    }

    #[tokio::test]
    async fn test_mixed_scheme_delete_one_keeps_others() {
        let file_io = get_resolving_file_io();

        let s3_path = format!(
            "s3://bucket1/{}",
            normalize_test_name_with_parts!("test_mixed_scheme_delete_one_keeps_others")
        );
        let fs_path = temp_fs_path("mixed_delete_one_keeps_others.txt");
        let mem_path = "memory://test_mixed_scheme_delete_one_keeps_others";

        // Write to all three
        file_io
            .new_output(&s3_path)
            .unwrap()
            .write("s3".into())
            .await
            .unwrap();
        file_io
            .new_output(&fs_path)
            .unwrap()
            .write("fs".into())
            .await
            .unwrap();
        file_io
            .new_output(mem_path)
            .unwrap()
            .write("mem".into())
            .await
            .unwrap();

        // Delete only the fs file
        file_io.delete(&fs_path).await.unwrap();

        // fs gone, S3 and memory still there
        assert!(file_io.exists(&s3_path).await.unwrap());
        assert!(!file_io.exists(&fs_path).await.unwrap());
        assert!(file_io.exists(mem_path).await.unwrap());

        assert_eq!(
            file_io.new_input(&s3_path).unwrap().read().await.unwrap(),
            bytes::Bytes::from("s3")
        );
        assert_eq!(
            file_io.new_input(mem_path).unwrap().read().await.unwrap(),
            bytes::Bytes::from("mem")
        );
    }

    #[tokio::test]
    async fn test_mixed_scheme_interleaved_operations() {
        let file_io = get_resolving_file_io();

        let s3_path = format!(
            "s3://bucket1/{}",
            normalize_test_name_with_parts!("test_mixed_scheme_interleaved")
        );
        let fs_path = temp_fs_path("mixed_interleaved.txt");
        let mem_path = "memory://test_mixed_scheme_interleaved";

        // Interleave: write fs, write memory, write s3
        file_io
            .new_output(&fs_path)
            .unwrap()
            .write("fs_data".into())
            .await
            .unwrap();
        file_io
            .new_output(mem_path)
            .unwrap()
            .write("mem_data".into())
            .await
            .unwrap();
        file_io
            .new_output(&s3_path)
            .unwrap()
            .write("s3_data".into())
            .await
            .unwrap();

        // Read in reverse order: s3, memory, fs
        assert_eq!(
            file_io.new_input(&s3_path).unwrap().read().await.unwrap(),
            bytes::Bytes::from("s3_data")
        );
        assert_eq!(
            file_io.new_input(mem_path).unwrap().read().await.unwrap(),
            bytes::Bytes::from("mem_data")
        );
        assert_eq!(
            file_io.new_input(&fs_path).unwrap().read().await.unwrap(),
            bytes::Bytes::from("fs_data")
        );
    }

    #[tokio::test]
    async fn test_invalid_scheme() {
        let file_io = get_resolving_file_io();
        let result = file_io.exists("unknown://bucket/key").await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Unsupported storage scheme"),
        );
    }

    #[tokio::test]
    async fn test_missing_scheme() {
        let file_io = get_resolving_file_io();
        let result = file_io.exists("no-scheme-path").await;
        assert!(result.is_err());
    }

    #[cfg(feature = "opendal-s3")]
    #[tokio::test]
    async fn test_with_custom_credential_loader() {
        use async_trait::async_trait;
        use iceberg_storage_opendal::CustomAwsCredentialLoader;
        use reqsign::{AwsCredential, AwsCredentialLoad};
        use reqwest::Client;

        struct MinioCredentialLoader;

        #[async_trait]
        impl AwsCredentialLoad for MinioCredentialLoader {
            async fn load_credential(
                &self,
                _client: Client,
            ) -> anyhow::Result<Option<AwsCredential>> {
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
                (S3_PATH_STYLE_ACCESS, "true".to_string()),
            ])
            .build();

        // Should be able to access S3 using the custom credential loader
        assert!(file_io.exists("s3://bucket1/").await.unwrap());
    }
}
