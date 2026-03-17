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

//! Shared helpers for storage integration suites.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::io::{
    FileIO, FileIOBuilder, GCS_NO_AUTH, GCS_SERVICE_PATH, S3_ACCESS_KEY_ID, S3_ENDPOINT,
    S3_REGION, S3_SECRET_ACCESS_KEY,
};
use iceberg_storage_opendal::{OpenDalResolvingStorageFactory, OpenDalStorageFactory};
use iceberg_test_utils::{get_gcs_endpoint, get_minio_endpoint, set_up};
use tempfile::TempDir;
use tokio::time::sleep;

static FAKE_GCS_BUCKET: &str = "test-bucket";

#[derive(Debug, Clone, Copy)]
pub enum StorageKind {
    OpenDalS3,
    OpenDalGcs,
    OpenDalResolving,
}

pub struct StorageHarness {
    pub file_io: FileIO,
    pub label: &'static str,
    pub base_path: String,
    pub _tempdirs: Vec<TempDir>,
}

/// Creates a `FileIO` for the given backend. Returns `None` when infrastructure
/// is unavailable (e.g., MinIO Docker container not running).
pub async fn load_storage(kind: StorageKind) -> Option<StorageHarness> {
    set_up();
    match kind {
        StorageKind::OpenDalS3 => load_opendal_s3().await,
        StorageKind::OpenDalGcs => load_opendal_gcs().await,
        StorageKind::OpenDalResolving => load_opendal_resolving().await,
    }
}

async fn load_opendal_s3() -> Option<StorageHarness> {
    let minio_endpoint = get_minio_endpoint();

    let file_io = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::S3 {
        configured_scheme: "s3".to_string(),
        customized_credential_load: None,
    }))
    .with_props(vec![
        (S3_ENDPOINT, minio_endpoint),
        (S3_ACCESS_KEY_ID, "admin".to_string()),
        (S3_SECRET_ACCESS_KEY, "password".to_string()),
        (S3_REGION, "us-east-1".to_string()),
    ])
    .build();

    // Poll until MinIO is ready
    let mut retries = 0;
    while retries < 30 {
        if file_io.exists("s3://bucket1/").await.unwrap_or(false) {
            return Some(StorageHarness {
                file_io,
                label: "opendal_s3",
                base_path: "s3://bucket1/".to_string(),
                _tempdirs: Vec::new(),
            });
        }
        sleep(std::time::Duration::from_secs(1)).await;
        retries += 1;
    }

    None
}

async fn load_opendal_gcs() -> Option<StorageHarness> {
    let gcs_endpoint = get_gcs_endpoint();

    // Create the test bucket via HTTP
    let mut bucket_data = HashMap::new();
    bucket_data.insert("name", FAKE_GCS_BUCKET);

    let client = reqwest::Client::new();
    let endpoint = format!("{gcs_endpoint}/storage/v1/b");
    if client.post(&endpoint).json(&bucket_data).send().await.is_err() {
        return None;
    }

    let file_io = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::Gcs))
        .with_props(vec![
            (GCS_SERVICE_PATH, gcs_endpoint),
            (GCS_NO_AUTH, "true".to_string()),
        ])
        .build();

    // Poll until GCS emulator is ready
    let base_path = format!("gs://{FAKE_GCS_BUCKET}/");
    let mut retries = 0;
    while retries < 30 {
        if file_io.exists(&base_path).await.unwrap_or(false) {
            return Some(StorageHarness {
                file_io,
                label: "opendal_gcs",
                base_path,
                _tempdirs: Vec::new(),
            });
        }
        sleep(std::time::Duration::from_secs(1)).await;
        retries += 1;
    }

    None
}

async fn load_opendal_resolving() -> Option<StorageHarness> {
    let minio_endpoint = get_minio_endpoint();

    let file_io = FileIOBuilder::new(Arc::new(OpenDalResolvingStorageFactory::new()))
        .with_props(vec![
            (S3_ENDPOINT, minio_endpoint),
            (S3_ACCESS_KEY_ID, "admin".to_string()),
            (S3_SECRET_ACCESS_KEY, "password".to_string()),
            (S3_REGION, "us-east-1".to_string()),
        ])
        .build();

    // Poll until MinIO is ready
    let mut retries = 0;
    while retries < 30 {
        if file_io.exists("s3://bucket1/").await.unwrap_or(false) {
            return Some(StorageHarness {
                file_io,
                label: "opendal_resolving",
                base_path: "s3://bucket1/".to_string(),
                _tempdirs: Vec::new(),
            });
        }
        sleep(std::time::Duration::from_secs(1)).await;
        retries += 1;
    }

    None
}

/// Generates a unique file path under `harness.base_path` to avoid conflicts
/// between concurrent test runs.
pub fn unique_path(harness: &StorageHarness, test_name: &str) -> String {
    format!(
        "{}{}",
        harness.base_path,
        iceberg_test_utils::normalize_test_name(test_name)
    )
}
