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
    pub _tempdirs: Option<Box<TempDir>>,
}

fn handle_unreachable_endpoint(kind: &'static str, endpoint: &str) -> Option<StorageHarness> {
    if std::env::var("ICEBERG_REQUIRE_STORAGE")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
    {
        panic!(
            "storage backed '{kind}' is required by ICEBERG_REQUIRE_STORAGE, but endpoint '{endpoint}' is unreachable"
        );
    }
    eprintln!("Skipping {kind} storage test: {endpoint} not reachable");
    None
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

const DEFAULT_PROBE_TIMEOUT_MS: u64 = 1000;

fn get_probe_timeout() -> Duration {
    let ms = std::env::var("ICEBERG_PROBE_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_PROBE_TIMEOUT_MS);
    Duration::from_millis(ms)
}

/// Fast probe to check if an endpoint service is listening before entering retry loops.
///
/// Note: Any HTTP response from `.send().await.is_ok()` (including 4xx/5xx) is treated
/// as reachable, as it proves the underlying server is up, listening on the port,
/// and actively responding to HTTP requests.
pub async fn is_endpoint_reachable(endpoint: &str) -> bool {
    let Ok(client) = reqwest::Client::builder()
        .timeout(get_probe_timeout())
        .build()
    else {
        return false;
    };
    client.get(endpoint).send().await.is_ok()
}

async fn wait_until_ready(file_io: &FileIO, check_path: &str, kind: &'static str, endpoint: &str) {
    let mut retries = 0;
    while retries < 15 {
        if file_io.exists(check_path).await.unwrap_or(false) {
            return;
        }
        sleep(Duration::from_millis(500)).await;
        retries += 1;
    }

    panic!(
        "Storage backend '{kind}' was reachable at '{endpoint}', but failed readiness check on '{check_path}' after 15 retries"
    );
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
        return handle_unreachable_endpoint("opendal_s3", &object_store_endpoint);
    }

    let file_io = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::S3 {
        customized_credential_load: None,
    }))
    .with_props(vec![
        (S3_ENDPOINT, object_store_endpoint.clone()),
        (S3_ACCESS_KEY_ID, "admin".to_string()),
        (S3_SECRET_ACCESS_KEY, "password".to_string()),
        (S3_REGION, "us-east-1".to_string()),
        (S3_PATH_STYLE_ACCESS, "true".to_string()),
    ])
    .build();

    wait_until_ready(
        &file_io,
        "s3://bucket1/",
        "opendal_s3",
        &object_store_endpoint,
    )
    .await;

    Some(StorageHarness {
        file_io,
        label: "opendal_s3",
        base_path: "s3://bucket1/".to_string(),
        _tempdirs: None,
    })
}

async fn load_opendal_gcs() -> Option<StorageHarness> {
    let gcs_endpoint = get_gcs_endpoint();

    if !is_endpoint_reachable(&gcs_endpoint).await {
        return handle_unreachable_endpoint("opendal_gcs", &gcs_endpoint);
    }

    let mut bucket_data = HashMap::new();
    bucket_data.insert("name", FAKE_GCS_BUCKET);

    let client = reqwest::Client::new();
    let endpoint = format!("{gcs_endpoint}/storage/v1/b");
    let response = client
        .post(&endpoint)
        .json(&bucket_data)
        .send()
        .await
        .unwrap_or_else(|e| {
            panic!("Failed to send GCS Bucket creation request to '{endpoint}': {e}")
        });

    let status = response.status();
    if !status.is_success() && status != reqwest::StatusCode::CONFLICT {
        panic!("failed to create GCS test bucket '{FAKE_GCS_BUCKET}': HTTP status {status}");
    }
    let file_io = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::Gcs))
        .with_props(vec![
            (GCS_SERVICE_HOST, gcs_endpoint.clone()),
            (GCS_NO_AUTH, "true".to_string()),
        ])
        .build();

    let base_path = format!("gs://{FAKE_GCS_BUCKET}/");

    wait_until_ready(&file_io, &base_path, "opendal_gcs", &gcs_endpoint).await;

    Some(StorageHarness {
        file_io,
        label: "opendal_gcs",
        base_path,
        _tempdirs: None,
    })
}

async fn load_opendal_fs() -> Option<StorageHarness> {
    let temp_dir = TempDir::new().ok()?;
    let base_path = format!("file:{}/", temp_dir.path().display());
    let file_io = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::Fs)).build();

    Some(StorageHarness {
        file_io,
        label: "opendal_fs",
        base_path,
        _tempdirs: Some(Box::new(temp_dir)),
    })
}

async fn load_opendal_memory() -> Option<StorageHarness> {
    let file_io = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::Memory)).build();

    Some(StorageHarness {
        file_io,
        label: "opendal_memory",
        base_path: "memory:/".to_string(),
        _tempdirs: None,
    })
}

async fn load_opendal_resolving() -> Option<StorageHarness> {
    let object_store_endpoint = get_object_store_endpoint();

    if !is_endpoint_reachable(&object_store_endpoint).await {
        return handle_unreachable_endpoint("opendal_resolving", &object_store_endpoint);
    }

    let file_io = FileIOBuilder::new(Arc::new(OpenDalResolvingStorageFactory::new()))
        .with_props(vec![
            (S3_ENDPOINT, object_store_endpoint.clone()),
            (S3_ACCESS_KEY_ID, "admin".to_string()),
            (S3_SECRET_ACCESS_KEY, "password".to_string()),
            (S3_REGION, "us-east-1".to_string()),
            (S3_PATH_STYLE_ACCESS, "true".to_string()),
        ])
        .build();

    wait_until_ready(
        &file_io,
        "s3://bucket1/",
        "opendal_resolving",
        &object_store_endpoint,
    )
    .await;

    Some(StorageHarness {
        file_io,
        label: "opendal_resolving",
        base_path: "s3://bucket1/".to_string(),
        _tempdirs: None,
    })
}

pub fn unique_path(harness: &StorageHarness, test_name: &str) -> String {
    format!("{}{}", harness.base_path, normalize_test_name(test_name))
}
