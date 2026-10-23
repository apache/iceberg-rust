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

use iceberg::io::{
    FileIOBuilder, GCS_NO_AUTH, GCS_SERVICE_HOST, S3_ACCESS_KEY_ID, S3_ENDPOINT,
    S3_PATH_STYLE_ACCESS, S3_REGION, S3_SECRET_ACCESS_KEY,
};
#[allow(unused_imports)]
pub use iceberg_storage_common::{
    StorageHarness, handle_unreachable_endpoint, is_endpoint_reachable, unique_path,
    wait_until_ready,
};
use iceberg_storage_opendal::{OpenDalResolvingStorageFactory, OpenDalStorageFactory};
use iceberg_test_utils::{get_gcs_endpoint, get_object_store_endpoint, set_up};
use tempfile::TempDir;

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

    Some(StorageHarness::new(file_io, "s3://bucket1/", "opendal_s3"))
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

    Some(StorageHarness::new(file_io, base_path, "opendal_gcs"))
}

async fn load_opendal_fs() -> Option<StorageHarness> {
    let temp_dir = TempDir::new()
        .unwrap_or_else(|e| panic!("Failed to create temporary directory for fs storage: {e}"));
    let base_path = format!("file:{}/", temp_dir.path().display());
    let file_io = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::Fs)).build();

    Some(StorageHarness::new(file_io, base_path, "opendal_fs").with_tempdir(temp_dir))
}

async fn load_opendal_memory() -> Option<StorageHarness> {
    let file_io = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::Memory)).build();

    Some(StorageHarness::new(file_io, "memory:/", "opendal_memory"))
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

    Some(StorageHarness::new(
        file_io,
        "s3://bucket1/",
        "opendal_resolving",
    ))
}
