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

//! Integration tests for FileIO Azure Blob Storage.
//!
//! These tests require a real Azure Blob Storage account and are skipped when
//! the `TEST_ENV_*` variables used by storage integration tests are not set.

#[cfg(feature = "opendal-azblob")]
mod tests {
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use bytes::Bytes;
    use iceberg::io::{AZBLOB_ACCOUNT_KEY, AZBLOB_ACCOUNT_NAME, AZBLOB_ENDPOINT, FileIOBuilder};
    use iceberg_storage_opendal::OpenDalStorageFactory;

    macro_rules! require_env {
        ($var:expr) => {
            match std::env::var($var) {
                Ok(value) if !value.is_empty() => value,
                _ => {
                    eprintln!("Skipping Azure Blob test: {} not set", $var);
                    return;
                }
            }
        };
    }

    #[tokio::test]
    async fn test_file_io_azblob_non_hns_account() {
        let provider = require_env!("TEST_ENV_CLOUD_PROVIDER");
        if !provider.eq_ignore_ascii_case("azure") {
            eprintln!("Skipping Azure Blob test: TEST_ENV_CLOUD_PROVIDER is not azure");
            return;
        }

        let account_name = require_env!("TEST_ENV_ACCESS_KEY");
        let account_key = require_env!("TEST_ENV_SECRET_KEY");
        let endpoint_suffix = require_env!("TEST_ENV_ADDRESS");
        let container = require_env!("TEST_ENV_BUCKET_NAME");
        let use_ssl = require_env!("TEST_ENV_USE_SSL");
        let http_scheme = if use_ssl.eq_ignore_ascii_case("true") {
            "https"
        } else {
            "http"
        };
        let endpoint = format!(
            "{http_scheme}://{account_name}.blob.{}",
            endpoint_suffix.trim_start_matches('.')
        );

        let file_io = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::Azblob))
            .with_props(vec![
                (AZBLOB_ENDPOINT, endpoint),
                (AZBLOB_ACCOUNT_NAME, account_name),
                (AZBLOB_ACCOUNT_KEY, account_key),
            ])
            .build();

        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let prefix = format!(
            "azblob://{container}/iceberg-rust-tests/non-hns-{}-{nonce}",
            std::process::id()
        );
        let file = format!("{prefix}/file.bin");
        let nested_file = format!("{prefix}/nested/file.bin");
        let content = Bytes::from_static(b"0123456789");

        let _ = file_io.delete_prefix(&prefix).await;
        assert!(!file_io.exists(&file).await.unwrap());

        file_io
            .new_output(&file)
            .unwrap()
            .write(content.clone())
            .await
            .unwrap();
        assert!(file_io.exists(&file).await.unwrap());

        let input = file_io.new_input(&file).unwrap();
        assert_eq!(input.metadata().await.unwrap().size, content.len() as u64);
        assert_eq!(input.read().await.unwrap(), content);
        assert_eq!(
            input.reader().await.unwrap().read(2..6).await.unwrap(),
            Bytes::from_static(b"2345")
        );

        file_io.delete(&file).await.unwrap();
        assert!(!file_io.exists(&file).await.unwrap());

        file_io
            .new_output(&nested_file)
            .unwrap()
            .write(Bytes::from_static(b"delete-prefix"))
            .await
            .unwrap();
        assert!(file_io.exists(&nested_file).await.unwrap());

        file_io.delete_prefix(&prefix).await.unwrap();
        assert!(!file_io.exists(&nested_file).await.unwrap());
    }
}
