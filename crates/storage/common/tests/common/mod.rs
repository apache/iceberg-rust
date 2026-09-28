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

//! Shared test harness and helpers for storage integration suites.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use iceberg::io::{
    FileIO, FileIOBuilder, GCS_NO_AUTH, GCS_SERVICE_HOST, S3_ACCESS_KEY_ID, S3_ENDPOINT,
    S3_PATH_STYLE_ACCESS, S3_REGION, S3_SECRET_ACCESS_KEY,
};
use iceberg_storage_opendal::{OpenDalResolvingStorageFactory, OpenDalStorageFactory};
use iceberg_test_utils::{
    get_gcs_endpoint, get_object_store_endpoint, normalize_test_name, set_up,
};
use tempfile::TempDir;
use tokio::time::sleep;

static FAKE_GCS_BUCKET: &str = "test-bucket";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageKind {
    OpenDalS3,
    OpenDalGcs,
    OpenDalFs,
    OpenDalMemory,
    OpenDalResolving,
    // TODO: Wire ObjectStoreStorage::S3 once PR #3165 is merged (https://github.com/apache/iceberg-rust/pull/3165)
}

pub struct StorageHarness {
    pub file_io: FileIO,
    pub label: &'static str,
    pub base_path: String,
    pub _tempdirs: Vec<TempDir>,
}

impl StorageKind {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::OpenDalS3 => "opendal_s3",
            Self::OpenDalGcs => "opendal_gcs",
            Self::OpenDalFs => "opendal_fs",
            Self::OpenDalMemory => "opendal_memory",
            Self::OpenDalResolving => "opendal_resolving",
        }
    }
}

impl std::fmt::Display for StorageKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Fast probe to check if an endpoint service is listening before entering retry loops.
pub async fn is_endpoint_reachable(endpoint: &str) -> bool {
    let Ok(client) = reqwest::Client::builder()
        .timeout(Duration::from_millis(300))
        .build()
    else {
        return false;
    };
    client.get(endpoint).send().await.is_ok()
}

pub async fn load_storage(kind: StorageKind) -> Option<StorageHarness> {
    set_up();
    match kind {
        StorageKind::OpenDalS3 => load_opendal_s3().await,
        StorageKind::OpenDalGcs => load_opendal_gcs().await,
        StorageKind::OpenDalFs => load_opendal_fs().await,
        StorageKind::OpenDalMemory => load_opendal_memory().await,
        StorageKind::OpenDalResolving => load_opendal_resolving().await,
    }
}

async fn load_opendal_s3() -> Option<StorageHarness> {
    let object_store_endpoint = get_object_store_endpoint();

    if !is_endpoint_reachable(&object_store_endpoint).await {
        eprintln!("Skipping S3 storage test: {object_store_endpoint} not reachable");
        return None;
    }

    let file_io = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::S3 {
        customized_credential_load: None,
    }))
    .with_props(vec![
        (S3_ENDPOINT, object_store_endpoint),
        (S3_ACCESS_KEY_ID, "admin".to_string()),
        (S3_SECRET_ACCESS_KEY, "password".to_string()),
        (S3_REGION, "us-east-1".to_string()),
        (S3_PATH_STYLE_ACCESS, "true".to_string()),
    ])
    .build();

    let mut retries = 0;
    while retries < 15 {
        if file_io.exists("s3://bucket1/").await.unwrap_or(false) {
            return Some(StorageHarness {
                file_io,
                label: "opendal_s3",
                base_path: "s3://bucket1/".to_string(),
                _tempdirs: Vec::new(),
            });
        }
        sleep(Duration::from_millis(500)).await;
        retries += 1;
    }

    None
}

async fn load_opendal_gcs() -> Option<StorageHarness> {
    let gcs_endpoint = get_gcs_endpoint();

    if !is_endpoint_reachable(&gcs_endpoint).await {
        eprintln!("Skipping GCS storage test: {gcs_endpoint} not reachable");
        return None;
    }

    let mut bucket_data = HashMap::new();
    bucket_data.insert("name", FAKE_GCS_BUCKET);

    let client = reqwest::Client::new();
    let endpoint = format!("{gcs_endpoint}/storage/v1/b");
    if client
        .post(&endpoint)
        .json(&bucket_data)
        .send()
        .await
        .is_err()
    {
        return None;
    }

    let file_io = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::Gcs))
        .with_props(vec![
            (GCS_SERVICE_HOST, gcs_endpoint),
            (GCS_NO_AUTH, "true".to_string()),
        ])
        .build();

    let base_path = format!("gs://{FAKE_GCS_BUCKET}/");
    let mut retries = 0;
    while retries < 15 {
        if file_io.exists(&base_path).await.unwrap_or(false) {
            return Some(StorageHarness {
                file_io,
                label: "opendal_gcs",
                base_path,
                _tempdirs: Vec::new(),
            });
        }
        sleep(Duration::from_millis(500)).await;
        retries += 1;
    }

    None
}

async fn load_opendal_fs() -> Option<StorageHarness> {
    let temp_dir = TempDir::new().ok()?;
    let base_path = format!("file:{}/", temp_dir.path().display());
    let file_io = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::Fs)).build();

    Some(StorageHarness {
        file_io,
        label: "opendal_fs",
        base_path,
        _tempdirs: vec![temp_dir],
    })
}

async fn load_opendal_memory() -> Option<StorageHarness> {
    let file_io = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::Memory)).build();

    Some(StorageHarness {
        file_io,
        label: "opendal_memory",
        base_path: "memory:/".to_string(),
        _tempdirs: Vec::new(),
    })
}

async fn load_opendal_resolving() -> Option<StorageHarness> {
    let object_store_endpoint = get_object_store_endpoint();

    if !is_endpoint_reachable(&object_store_endpoint).await {
        eprintln!("Skipping Resolving storage test: {object_store_endpoint} not reachable");
        return None;
    }

    let file_io = FileIOBuilder::new(Arc::new(OpenDalResolvingStorageFactory::new()))
        .with_props(vec![
            (S3_ENDPOINT, object_store_endpoint),
            (S3_ACCESS_KEY_ID, "admin".to_string()),
            (S3_SECRET_ACCESS_KEY, "password".to_string()),
            (S3_REGION, "us-east-1".to_string()),
            (S3_PATH_STYLE_ACCESS, "true".to_string()),
        ])
        .build();

    let mut retries = 0;
    while retries < 15 {
        if file_io.exists("s3://bucket1/").await.unwrap_or(false) {
            return Some(StorageHarness {
                file_io,
                label: "opendal_resolving",
                base_path: "s3://bucket1/".to_string(),
                _tempdirs: Vec::new(),
            });
        }
        sleep(Duration::from_millis(500)).await;
        retries += 1;
    }

    None
}

pub fn unique_path(harness: &StorageHarness, test_name: &str) -> String {
    format!("{}{}", harness.base_path, normalize_test_name(test_name))
}

#[cfg(test)]
mod endpoint_tests {
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    use super::*;

    #[tokio::test]
    async fn test_endpoint_unreachable_on_closed_port() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let url = format!("http://127.0.0.1:{port}");
        assert!(!is_endpoint_reachable(&url).await);
    }

    #[tokio::test]
    async fn test_endpoint_unreachable_on_invalid_url() {
        assert!(!is_endpoint_reachable("not_a_valid_url").await);
        assert!(!is_endpoint_reachable("http://invalid-host-that-does-not-exist:9999").await);
    }

    #[tokio::test]
    async fn test_endpoint_reachable_on_active_server() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let url = format!("http://127.0.0.1:{port}");

        let server_handle = tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let response = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });

        assert!(is_endpoint_reachable(&url).await);
        let _ = server_handle.await;
    }
}
