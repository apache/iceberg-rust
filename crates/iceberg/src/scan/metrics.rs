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

use std::sync::atomic::AtomicU64;

#[derive(Debug, Default)]
pub(super) struct PlanningCounters {
    pub(super) total_data_manifests: AtomicU64,
    pub(super) total_delete_manifests: AtomicU64,
    pub(super) skipped_data_manifests: AtomicU64,
    pub(super) skipped_delete_manifests: AtomicU64,
    pub(super) scanned_data_manifests: AtomicU64,
    pub(super) scanned_delete_manifests: AtomicU64,
    pub(super) result_data_files: AtomicU64,
    pub(super) skipped_data_files: AtomicU64,
    pub(super) total_file_size_in_bytes: AtomicU64,
    pub(super) result_delete_files: AtomicU64,
    pub(super) skipped_delete_files: AtomicU64,
    pub(super) total_delete_file_size_in_bytes: AtomicU64,
    pub(super) indexed_delete_files: AtomicU64,
    pub(super) equality_delete_files: AtomicU64,
    pub(super) positional_delete_files: AtomicU64,
    pub(super) dvs: AtomicU64,
}

#[cfg(test)]
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct PlanningCounts {
    pub(super) total_data_manifests: u64,
    pub(super) total_delete_manifests: u64,
    pub(super) skipped_data_manifests: u64,
    pub(super) skipped_delete_manifests: u64,
    pub(super) scanned_data_manifests: u64,
    pub(super) scanned_delete_manifests: u64,
    pub(super) result_data_files: u64,
    pub(super) skipped_data_files: u64,
    pub(super) total_file_size_in_bytes: u64,
    pub(super) result_delete_files: u64,
    pub(super) skipped_delete_files: u64,
    pub(super) total_delete_file_size_in_bytes: u64,
    pub(super) indexed_delete_files: u64,
    pub(super) equality_delete_files: u64,
    pub(super) positional_delete_files: u64,
    pub(super) dvs: u64,
}

#[cfg(test)]
impl PlanningCounters {
    pub(super) fn snapshot(&self) -> PlanningCounts {
        use std::sync::atomic::Ordering;

        PlanningCounts {
            total_data_manifests: self.total_data_manifests.load(Ordering::Relaxed),
            total_delete_manifests: self.total_delete_manifests.load(Ordering::Relaxed),
            skipped_data_manifests: self.skipped_data_manifests.load(Ordering::Relaxed),
            skipped_delete_manifests: self.skipped_delete_manifests.load(Ordering::Relaxed),
            scanned_data_manifests: self.scanned_data_manifests.load(Ordering::Relaxed),
            scanned_delete_manifests: self.scanned_delete_manifests.load(Ordering::Relaxed),
            result_data_files: self.result_data_files.load(Ordering::Relaxed),
            skipped_data_files: self.skipped_data_files.load(Ordering::Relaxed),
            total_file_size_in_bytes: self.total_file_size_in_bytes.load(Ordering::Relaxed),
            result_delete_files: self.result_delete_files.load(Ordering::Relaxed),
            skipped_delete_files: self.skipped_delete_files.load(Ordering::Relaxed),
            total_delete_file_size_in_bytes: self
                .total_delete_file_size_in_bytes
                .load(Ordering::Relaxed),
            indexed_delete_files: self.indexed_delete_files.load(Ordering::Relaxed),
            equality_delete_files: self.equality_delete_files.load(Ordering::Relaxed),
            positional_delete_files: self.positional_delete_files.load(Ordering::Relaxed),
            dvs: self.dvs.load(Ordering::Relaxed),
        }
    }
}
