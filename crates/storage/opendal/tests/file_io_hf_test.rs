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

//! Integration tests for FileIO HuggingFace Hub.
//!
//! These tests require a real HuggingFace token and are skipped when
//! `HF_OPENDAL_TOKEN` is not set in the environment.
//!
//! The following environment variables are used:
//! - `HF_OPENDAL_TOKEN`  — HuggingFace API token (required)
//! - `HF_OPENDAL_BUCKET` — `owner/repo` for a bucket-type repo (required when running bucket tests)
//! - `HF_OPENDAL_DATASET` — `owner/repo` for a dataset-type repo (required when running dataset tests)

#[cfg(feature = "opendal-hf")]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use futures::StreamExt;
    use iceberg::io::{FileIO, FileIOBuilder, HF_REVISION, HF_TOKEN};
    use iceberg_storage_opendal::{OpenDalResolvingStorageFactory, OpenDalStorageFactory};
    use iceberg_test_utils::{normalize_test_name_with_parts, set_up};

    const ENV_HF_TOKEN: &str = "HF_OPENDAL_TOKEN";
    const ENV_HF_BUCKET: &str = "HF_OPENDAL_BUCKET";
    const ENV_HF_DATASET: &str = "HF_OPENDAL_DATASET";

    macro_rules! require_env {
        ($var:expr) => {
            match std::env::var($var) {
                Ok(v) => v,
                Err(_) => {
                    eprintln!("Skipping HF test: {} not set", $var);
                    return;
                }
            }
        };
    }

    fn get_file_io(token: &str) -> FileIO {
        set_up();
        FileIOBuilder::new(Arc::new(OpenDalStorageFactory::Hf))
            .with_props(vec![(HF_TOKEN, token.to_string())])
            .build()
    }

    fn get_resolving_file_io(token: &str) -> FileIO {
        set_up();
        FileIOBuilder::new(Arc::new(OpenDalResolvingStorageFactory::new()))
            .with_props(vec![(HF_TOKEN, token.to_string())])
            .build()
    }

    // --- bucket tests ---

    #[tokio::test]
    async fn test_hf_bucket_write_read_delete() {
        let token = require_env!(ENV_HF_TOKEN);
        let bucket = require_env!(ENV_HF_BUCKET);
        let file_io = get_file_io(&token);
        let path = format!(
            "hf://buckets/{}/{}",
            bucket,
            normalize_test_name_with_parts!("test_hf_bucket_write_read_delete")
        );

        let _ = file_io.delete(&path).await;
        assert!(!file_io.exists(&path).await.unwrap());

        file_io
            .new_output(&path)
            .unwrap()
            .write(Bytes::from_static(b"iceberg-hf-bucket"))
            .await
            .unwrap();
        assert!(file_io.exists(&path).await.unwrap());

        let data = file_io.new_input(&path).unwrap().read().await.unwrap();
        assert_eq!(data, Bytes::from_static(b"iceberg-hf-bucket"));

        file_io.delete(&path).await.unwrap();
        assert!(!file_io.exists(&path).await.unwrap());
    }

    #[tokio::test]
    async fn test_hf_bucket_overwrite() {
        let token = require_env!(ENV_HF_TOKEN);
        let bucket = require_env!(ENV_HF_BUCKET);
        let file_io = get_file_io(&token);
        let path = format!(
            "hf://buckets/{}/{}",
            bucket,
            normalize_test_name_with_parts!("test_hf_bucket_overwrite")
        );

        let _ = file_io.delete(&path).await;

        file_io
            .new_output(&path)
            .unwrap()
            .write(Bytes::from_static(b"first"))
            .await
            .unwrap();
        file_io
            .new_output(&path)
            .unwrap()
            .write(Bytes::from_static(b"second"))
            .await
            .unwrap();

        let data = file_io.new_input(&path).unwrap().read().await.unwrap();
        assert_eq!(data, Bytes::from_static(b"second"));

        file_io.delete(&path).await.unwrap();
    }

    #[tokio::test]
    async fn test_hf_bucket_range_read() {
        let token = require_env!(ENV_HF_TOKEN);
        let bucket = require_env!(ENV_HF_BUCKET);
        let file_io = get_file_io(&token);
        let path = format!(
            "hf://buckets/{}/{}",
            bucket,
            normalize_test_name_with_parts!("test_hf_bucket_range_read")
        );

        let _ = file_io.delete(&path).await;
        file_io
            .new_output(&path)
            .unwrap()
            .write(Bytes::from_static(b"hello world"))
            .await
            .unwrap();

        let reader = file_io.new_input(&path).unwrap().reader().await.unwrap();
        let chunk = reader.read(6..11).await.unwrap();
        assert_eq!(chunk, Bytes::from_static(b"world"));

        file_io.delete(&path).await.unwrap();
    }

    #[tokio::test]
    async fn test_hf_bucket_metadata() {
        let token = require_env!(ENV_HF_TOKEN);
        let bucket = require_env!(ENV_HF_BUCKET);
        let file_io = get_file_io(&token);
        let path = format!(
            "hf://buckets/{}/{}",
            bucket,
            normalize_test_name_with_parts!("test_hf_bucket_metadata")
        );

        let _ = file_io.delete(&path).await;
        file_io
            .new_output(&path)
            .unwrap()
            .write(Bytes::from_static(b"metadata-test"))
            .await
            .unwrap();

        let meta = file_io.new_input(&path).unwrap().metadata().await.unwrap();
        assert_eq!(meta.size, b"metadata-test".len() as u64);

        file_io.delete(&path).await.unwrap();
    }

    #[tokio::test]
    async fn test_hf_bucket_delete_stream() {
        let token = require_env!(ENV_HF_TOKEN);
        let bucket = require_env!(ENV_HF_BUCKET);
        let file_io = get_file_io(&token);

        let paths: Vec<String> = (0..3)
            .map(|i| {
                format!(
                    "hf://buckets/{}/{}/file-{i}",
                    bucket,
                    normalize_test_name_with_parts!("test_hf_bucket_delete_stream")
                )
            })
            .collect();

        for path in &paths {
            let _ = file_io.delete(path).await;
            file_io
                .new_output(path)
                .unwrap()
                .write(Bytes::from_static(b"x"))
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
    async fn test_hf_bucket_delete_stream_empty() {
        let token = require_env!(ENV_HF_TOKEN);
        let file_io = get_file_io(&token);
        file_io
            .delete_stream(futures::stream::empty().boxed())
            .await
            .unwrap();
    }

    // --- dataset tests ---

    #[tokio::test]
    async fn test_hf_dataset_write_read_delete() {
        let token = require_env!(ENV_HF_TOKEN);
        let dataset = require_env!(ENV_HF_DATASET);
        let file_io = get_file_io(&token);
        let path = format!(
            "hf://datasets/{}/{}",
            dataset,
            normalize_test_name_with_parts!("test_hf_dataset_write_read_delete")
        );

        let _ = file_io.delete(&path).await;
        assert!(!file_io.exists(&path).await.unwrap());

        file_io
            .new_output(&path)
            .unwrap()
            .write(Bytes::from_static(b"iceberg-hf-dataset"))
            .await
            .unwrap();
        assert!(file_io.exists(&path).await.unwrap());

        let data = file_io.new_input(&path).unwrap().read().await.unwrap();
        assert_eq!(data, Bytes::from_static(b"iceberg-hf-dataset"));

        file_io.delete(&path).await.unwrap();
        assert!(!file_io.exists(&path).await.unwrap());
    }

    // --- revision tests ---

    #[tokio::test]
    async fn test_hf_explicit_revision_in_uri() {
        let token = require_env!(ENV_HF_TOKEN);
        let file_io = get_file_io(&token);
        let name = normalize_test_name_with_parts!("test_hf_explicit_revision_in_uri");

        let bucket = require_env!(ENV_HF_BUCKET);
        // Write without revision, read back with explicit @main.
        let write_path = format!("hf://buckets/{}/{}", bucket, name);
        let read_path = format!("hf://buckets/{}@main/{}", bucket, name);

        let _ = file_io.delete(&write_path).await;
        file_io
            .new_output(&write_path)
            .unwrap()
            .write(Bytes::from_static(b"revision-test"))
            .await
            .unwrap();

        let data = file_io.new_input(&read_path).unwrap().read().await.unwrap();
        assert_eq!(data, Bytes::from_static(b"revision-test"));

        file_io.delete(&write_path).await.unwrap();
    }

    #[tokio::test]
    async fn test_hf_revision_from_config() {
        let token = require_env!(ENV_HF_TOKEN);
        set_up();

        // Build FileIO with HF_REVISION set in config — paths without @revision use it.
        let file_io = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::Hf))
            .with_props(vec![
                (HF_TOKEN, token.to_string()),
                (HF_REVISION, "main".to_string()),
            ])
            .build();

        let bucket = require_env!(ENV_HF_BUCKET);
        let path = format!(
            "hf://buckets/{}/{}",
            bucket,
            normalize_test_name_with_parts!("test_hf_revision_from_config")
        );

        let _ = file_io.delete(&path).await;
        file_io
            .new_output(&path)
            .unwrap()
            .write(Bytes::from_static(b"config-revision"))
            .await
            .unwrap();

        let data = file_io.new_input(&path).unwrap().read().await.unwrap();
        assert_eq!(data, Bytes::from_static(b"config-revision"));

        file_io.delete(&path).await.unwrap();
    }

    // --- resolving storage tests ---

    #[tokio::test]
    async fn test_hf_resolving_storage() {
        let token = require_env!(ENV_HF_TOKEN);
        let file_io = get_resolving_file_io(&token);

        let bucket = require_env!(ENV_HF_BUCKET);
        let path = format!(
            "hf://buckets/{}/{}",
            bucket,
            normalize_test_name_with_parts!("test_hf_resolving_storage")
        );

        let _ = file_io.delete(&path).await;

        file_io
            .new_output(&path)
            .unwrap()
            .write(Bytes::from_static(b"resolving"))
            .await
            .unwrap();

        let data = file_io.new_input(&path).unwrap().read().await.unwrap();
        assert_eq!(data, Bytes::from_static(b"resolving"));

        file_io.delete(&path).await.unwrap();
    }

    #[tokio::test]
    async fn test_hf_resolving_delete_stream_across_repo_types() {
        let token = require_env!(ENV_HF_TOKEN);
        let file_io = get_resolving_file_io(&token);

        let bucket = require_env!(ENV_HF_BUCKET);
        let dataset = require_env!(ENV_HF_DATASET);
        let name = normalize_test_name_with_parts!("test_hf_resolving_delete_stream_across");
        let bucket_path = format!("hf://buckets/{}/{}", bucket, name);
        let dataset_path = format!("hf://datasets/{}/{}", dataset, name);

        for path in [&bucket_path, &dataset_path] {
            let _ = file_io.delete(path).await;
            file_io
                .new_output(path)
                .unwrap()
                .write(Bytes::from_static(b"x"))
                .await
                .unwrap();
            assert!(file_io.exists(path).await.unwrap());
        }

        let stream = futures::stream::iter(vec![bucket_path.clone(), dataset_path.clone()]).boxed();
        file_io.delete_stream(stream).await.unwrap();

        assert!(!file_io.exists(&bucket_path).await.unwrap());
        assert!(!file_io.exists(&dataset_path).await.unwrap());
    }
}
