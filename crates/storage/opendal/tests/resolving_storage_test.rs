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
#[cfg(feature = "opendal-s3")]
mod tests {
    use std::sync::Arc;

    use iceberg::io::{
        FileIOBuilder, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY,
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
            ])
            .build()
    }

    #[tokio::test]
    async fn test_resolving_storage_s3_exists() {
        let file_io = get_resolving_file_io();
        assert!(!file_io.exists("s3://bucket2/any").await.unwrap());
        assert!(file_io.exists("s3://bucket1/").await.unwrap());
    }

    #[tokio::test]
    async fn test_resolving_storage_s3_write_and_read() {
        let file_io = get_resolving_file_io();
        let path = format!(
            "s3://bucket1/{}",
            normalize_test_name_with_parts!("test_resolving_storage_s3_write_and_read")
        );

        // Clean up from any previous test runs
        let _ = file_io.delete(&path).await;
        assert!(!file_io.exists(&path).await.unwrap());

        // Write via output file
        let output_file = file_io.new_output(&path).unwrap();
        output_file.write("hello_resolving".into()).await.unwrap();
        assert!(file_io.exists(&path).await.unwrap());

        // Read via input file
        let input_file = file_io.new_input(&path).unwrap();
        let content = input_file.read().await.unwrap();
        assert_eq!(content, bytes::Bytes::from("hello_resolving"));
    }

    #[tokio::test]
    async fn test_resolving_storage_s3_delete() {
        let file_io = get_resolving_file_io();
        let path = format!(
            "s3://bucket1/{}",
            normalize_test_name_with_parts!("test_resolving_storage_s3_delete")
        );

        // Write a file first
        let output_file = file_io.new_output(&path).unwrap();
        output_file.write("to_delete".into()).await.unwrap();
        assert!(file_io.exists(&path).await.unwrap());

        // Delete it
        file_io.delete(&path).await.unwrap();
        assert!(!file_io.exists(&path).await.unwrap());
    }

    #[tokio::test]
    async fn test_resolving_storage_caches_backend() {
        let file_io = get_resolving_file_io();
        let path1 = format!(
            "s3://bucket1/{}",
            normalize_test_name_with_parts!("test_resolving_storage_caches_backend_1")
        );
        let path2 = format!(
            "s3://bucket1/{}",
            normalize_test_name_with_parts!("test_resolving_storage_caches_backend_2")
        );

        // Both operations should succeed, using the same cached S3 backend
        let output1 = file_io.new_output(&path1).unwrap();
        output1.write("file1".into()).await.unwrap();

        let output2 = file_io.new_output(&path2).unwrap();
        output2.write("file2".into()).await.unwrap();

        let input1 = file_io.new_input(&path1).unwrap();
        assert_eq!(input1.read().await.unwrap(), bytes::Bytes::from("file1"));

        let input2 = file_io.new_input(&path2).unwrap();
        assert_eq!(input2.read().await.unwrap(), bytes::Bytes::from("file2"));
    }

    #[tokio::test]
    async fn test_resolving_storage_invalid_scheme() {
        let file_io = get_resolving_file_io();
        let result = file_io.exists("unknown://bucket/key").await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("Unsupported storage scheme"),
            "Expected unsupported scheme error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_resolving_storage_missing_scheme() {
        let file_io = get_resolving_file_io();
        let result = file_io.exists("no-scheme-path").await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("missing scheme"),
            "Expected missing scheme error, got: {err}"
        );
    }

    #[cfg(feature = "opendal-s3")]
    #[tokio::test]
    async fn test_resolving_storage_with_custom_credential_loader() {
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
            ])
            .build();

        // Should be able to access S3 using the custom credential loader
        assert!(file_io.exists("s3://bucket1/").await.unwrap());
    }
}

#[cfg(all(feature = "opendal-s3", feature = "opendal-memory"))]
mod mixed_scheme_tests {
    use std::sync::Arc;

    use iceberg::io::{
        FileIOBuilder, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY,
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
            ])
            .build()
    }

    #[tokio::test]
    async fn test_mixed_s3_and_memory_write_read() {
        let file_io = get_resolving_file_io();

        let s3_path = format!(
            "s3://bucket1/{}",
            normalize_test_name_with_parts!("test_mixed_s3_and_memory_write_read")
        );
        let memory_path = "memory://test_mixed_s3_and_memory_write_read";

        // Write to both schemes through the same FileIO
        let s3_output = file_io.new_output(&s3_path).unwrap();
        s3_output.write("s3_content".into()).await.unwrap();

        let mem_output = file_io.new_output(memory_path).unwrap();
        mem_output.write("memory_content".into()).await.unwrap();

        // Read back from both schemes
        let s3_input = file_io.new_input(&s3_path).unwrap();
        assert_eq!(
            s3_input.read().await.unwrap(),
            bytes::Bytes::from("s3_content")
        );

        let mem_input = file_io.new_input(memory_path).unwrap();
        assert_eq!(
            mem_input.read().await.unwrap(),
            bytes::Bytes::from("memory_content")
        );
    }

    #[tokio::test]
    async fn test_mixed_schemes_exist_independently() {
        let file_io = get_resolving_file_io();

        let s3_path = format!(
            "s3://bucket1/{}",
            normalize_test_name_with_parts!("test_mixed_schemes_exist_independently")
        );
        let memory_path = "memory://test_mixed_schemes_exist_independently";

        // Clean up S3 from previous runs
        let _ = file_io.delete(&s3_path).await;

        // Neither should exist initially
        assert!(!file_io.exists(&s3_path).await.unwrap());
        assert!(!file_io.exists(memory_path).await.unwrap());

        // Write only to S3
        let s3_output = file_io.new_output(&s3_path).unwrap();
        s3_output.write("only_s3".into()).await.unwrap();

        // S3 path exists, memory path does not
        assert!(file_io.exists(&s3_path).await.unwrap());
        assert!(!file_io.exists(memory_path).await.unwrap());

        // Now write to memory
        let mem_output = file_io.new_output(memory_path).unwrap();
        mem_output.write("only_memory".into()).await.unwrap();

        // Both exist
        assert!(file_io.exists(&s3_path).await.unwrap());
        assert!(file_io.exists(memory_path).await.unwrap());
    }

    #[tokio::test]
    async fn test_mixed_schemes_delete_one_keeps_other() {
        let file_io = get_resolving_file_io();

        let s3_path = format!(
            "s3://bucket1/{}",
            normalize_test_name_with_parts!("test_mixed_schemes_delete_one_keeps_other")
        );
        let memory_path = "memory://test_mixed_schemes_delete_one_keeps_other";

        // Write to both
        file_io
            .new_output(&s3_path)
            .unwrap()
            .write("s3_data".into())
            .await
            .unwrap();
        file_io
            .new_output(memory_path)
            .unwrap()
            .write("mem_data".into())
            .await
            .unwrap();

        // Delete only the S3 file
        file_io.delete(&s3_path).await.unwrap();

        // S3 gone, memory still there
        assert!(!file_io.exists(&s3_path).await.unwrap());
        assert!(file_io.exists(memory_path).await.unwrap());

        let mem_input = file_io.new_input(memory_path).unwrap();
        assert_eq!(
            mem_input.read().await.unwrap(),
            bytes::Bytes::from("mem_data")
        );
    }

    #[tokio::test]
    async fn test_mixed_schemes_interleaved_operations() {
        let file_io = get_resolving_file_io();

        let s3_path_1 = format!(
            "s3://bucket1/{}",
            normalize_test_name_with_parts!("test_mixed_interleaved_1")
        );
        let mem_path_1 = "memory://test_mixed_interleaved_1";
        let s3_path_2 = format!(
            "s3://bucket1/{}",
            normalize_test_name_with_parts!("test_mixed_interleaved_2")
        );
        let mem_path_2 = "memory://test_mixed_interleaved_2";

        // Interleave writes across schemes
        file_io
            .new_output(&s3_path_1)
            .unwrap()
            .write("s3_1".into())
            .await
            .unwrap();
        file_io
            .new_output(mem_path_1)
            .unwrap()
            .write("mem_1".into())
            .await
            .unwrap();
        file_io
            .new_output(&s3_path_2)
            .unwrap()
            .write("s3_2".into())
            .await
            .unwrap();
        file_io
            .new_output(mem_path_2)
            .unwrap()
            .write("mem_2".into())
            .await
            .unwrap();

        // Read back all four, interleaved
        let r1 = file_io.new_input(mem_path_2).unwrap().read().await.unwrap();
        let r2 = file_io.new_input(&s3_path_1).unwrap().read().await.unwrap();
        let r3 = file_io.new_input(mem_path_1).unwrap().read().await.unwrap();
        let r4 = file_io.new_input(&s3_path_2).unwrap().read().await.unwrap();

        assert_eq!(r1, bytes::Bytes::from("mem_2"));
        assert_eq!(r2, bytes::Bytes::from("s3_1"));
        assert_eq!(r3, bytes::Bytes::from("mem_1"));
        assert_eq!(r4, bytes::Bytes::from("s3_2"));
    }
}

#[cfg(all(
    feature = "opendal-s3",
    feature = "opendal-gcs",
    feature = "opendal-memory"
))]
mod mixed_s3_gcs_memory_tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use iceberg::io::{
        FileIOBuilder, GCS_NO_AUTH, GCS_SERVICE_PATH, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION,
        S3_SECRET_ACCESS_KEY,
    };
    use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
    use iceberg_test_utils::{
        get_gcs_endpoint, get_minio_endpoint, normalize_test_name_with_parts, set_up,
    };

    static FAKE_GCS_BUCKET: &str = "resolving-test-bucket";

    async fn ensure_gcs_bucket(endpoint: &str) {
        let mut bucket_data = HashMap::new();
        bucket_data.insert("name", FAKE_GCS_BUCKET);
        let client = reqwest::Client::new();
        let url = format!("{endpoint}/storage/v1/b");
        let _ = client.post(url).json(&bucket_data).send().await;
    }

    async fn get_resolving_file_io() -> iceberg::io::FileIO {
        set_up();

        let minio_endpoint = get_minio_endpoint();
        let gcs_endpoint = get_gcs_endpoint();

        ensure_gcs_bucket(&gcs_endpoint).await;

        FileIOBuilder::new(Arc::new(OpenDalResolvingStorageFactory::new()))
            .with_props(vec![
                (S3_ENDPOINT, minio_endpoint),
                (S3_ACCESS_KEY_ID, "admin".to_string()),
                (S3_SECRET_ACCESS_KEY, "password".to_string()),
                (S3_REGION, "us-east-1".to_string()),
                (GCS_SERVICE_PATH, gcs_endpoint),
                (GCS_NO_AUTH, "true".to_string()),
            ])
            .build()
    }

    #[tokio::test]
    async fn test_three_scheme_write_and_read() {
        let file_io = get_resolving_file_io().await;

        let s3_path = format!(
            "s3://bucket1/{}",
            normalize_test_name_with_parts!("test_three_scheme_write_and_read")
        );
        let gcs_path = format!(
            "gs://{FAKE_GCS_BUCKET}/{}",
            normalize_test_name_with_parts!("test_three_scheme_write_and_read")
        );
        let mem_path = "memory://test_three_scheme_write_and_read";

        // Write to all three schemes
        file_io
            .new_output(&s3_path)
            .unwrap()
            .write("from_s3".into())
            .await
            .unwrap();
        file_io
            .new_output(&gcs_path)
            .unwrap()
            .write("from_gcs".into())
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
            file_io.new_input(&gcs_path).unwrap().read().await.unwrap(),
            bytes::Bytes::from("from_gcs")
        );
        assert_eq!(
            file_io.new_input(mem_path).unwrap().read().await.unwrap(),
            bytes::Bytes::from("from_memory")
        );
    }

    #[tokio::test]
    async fn test_three_scheme_exists_independently() {
        let file_io = get_resolving_file_io().await;

        let s3_path = format!(
            "s3://bucket1/{}",
            normalize_test_name_with_parts!("test_three_scheme_exists_independently")
        );
        let gcs_path = format!(
            "gs://{FAKE_GCS_BUCKET}/{}",
            normalize_test_name_with_parts!("test_three_scheme_exists_independently")
        );
        let mem_path = "memory://test_three_scheme_exists_independently";

        // Clean up from previous runs
        let _ = file_io.delete(&s3_path).await;
        let _ = file_io.delete(&gcs_path).await;

        // None exist initially
        assert!(!file_io.exists(&s3_path).await.unwrap());
        assert!(!file_io.exists(&gcs_path).await.unwrap());
        assert!(!file_io.exists(mem_path).await.unwrap());

        // Write only to GCS
        file_io
            .new_output(&gcs_path)
            .unwrap()
            .write("gcs_only".into())
            .await
            .unwrap();

        // Only GCS exists
        assert!(!file_io.exists(&s3_path).await.unwrap());
        assert!(file_io.exists(&gcs_path).await.unwrap());
        assert!(!file_io.exists(mem_path).await.unwrap());
    }

    #[tokio::test]
    async fn test_three_scheme_interleaved_operations() {
        let file_io = get_resolving_file_io().await;

        let s3_path = format!(
            "s3://bucket1/{}",
            normalize_test_name_with_parts!("test_three_scheme_interleaved")
        );
        let gcs_path = format!(
            "gs://{FAKE_GCS_BUCKET}/{}",
            normalize_test_name_with_parts!("test_three_scheme_interleaved")
        );
        let mem_path = "memory://test_three_scheme_interleaved";

        // Interleave: write gcs, write memory, write s3
        file_io
            .new_output(&gcs_path)
            .unwrap()
            .write("gcs_data".into())
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

        // Read in reverse order: s3, memory, gcs
        assert_eq!(
            file_io.new_input(&s3_path).unwrap().read().await.unwrap(),
            bytes::Bytes::from("s3_data")
        );
        assert_eq!(
            file_io.new_input(mem_path).unwrap().read().await.unwrap(),
            bytes::Bytes::from("mem_data")
        );
        assert_eq!(
            file_io.new_input(&gcs_path).unwrap().read().await.unwrap(),
            bytes::Bytes::from("gcs_data")
        );
    }

    #[tokio::test]
    async fn test_three_scheme_delete_one_keeps_others() {
        let file_io = get_resolving_file_io().await;

        let s3_path = format!(
            "s3://bucket1/{}",
            normalize_test_name_with_parts!("test_three_scheme_delete_one_keeps_others")
        );
        let gcs_path = format!(
            "gs://{FAKE_GCS_BUCKET}/{}",
            normalize_test_name_with_parts!("test_three_scheme_delete_one_keeps_others")
        );
        let mem_path = "memory://test_three_scheme_delete_one_keeps_others";

        // Write to all three
        file_io
            .new_output(&s3_path)
            .unwrap()
            .write("s3".into())
            .await
            .unwrap();
        file_io
            .new_output(&gcs_path)
            .unwrap()
            .write("gcs".into())
            .await
            .unwrap();
        file_io
            .new_output(mem_path)
            .unwrap()
            .write("mem".into())
            .await
            .unwrap();

        // Delete only GCS
        file_io.delete(&gcs_path).await.unwrap();

        // GCS gone, S3 and memory still there
        assert!(file_io.exists(&s3_path).await.unwrap());
        assert!(!file_io.exists(&gcs_path).await.unwrap());
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
}
