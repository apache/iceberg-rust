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

pub mod backends;

use iceberg_test_utils::set_up;
#[allow(unused_imports)]
pub use iceberg_test_utils::storage::{
    StorageHarness, handle_unreachable_endpoint, is_endpoint_reachable, roundtrip_file_io,
    unique_path, wait_until_ready,
};

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
        StorageKind::OpenDalS3 => backends::opendal::load_opendal_s3().await,
        StorageKind::OpenDalGcs => backends::opendal::load_opendal_gcs().await,
        StorageKind::OpenDalFs => backends::opendal::load_opendal_fs().await,
        StorageKind::OpenDalMemory => backends::opendal::load_opendal_memory().await,
        StorageKind::OpenDalResolving => backends::opendal::load_opendal_resolving().await,
    }
}
