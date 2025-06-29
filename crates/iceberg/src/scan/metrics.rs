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

/// Subset of [ScanMetrics] produced by manifest-handling functions.
#[derive(Default)]
pub(crate) struct ManifestMetrics {
    pub(crate) total_data_manifests: u32,
    pub(crate) total_delete_manifests: u32,
    pub(crate) skipped_data_manifests: u32,
    pub(crate) skipped_delete_manifests: u32,
    pub(crate) scanned_data_manifests: u32,
    pub(crate) scanned_delete_manifests: u32,
}

/// Represents an update to a single data or delete file.
pub(crate) enum FileMetricsUpdate {
    Skipped,
    Scanned { size_in_bytes: u64 },
}
