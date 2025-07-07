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

use std::time::Instant;

use futures::channel::mpsc::Receiver;
use futures::{StreamExt, join};

use crate::delete_file_index::DeleteIndexMetrics;
use crate::metrics::ScanMetrics;
use crate::runtime::JoinHandle;

/// Awaits metrics updates from different sources and combines them into the
/// [ScanMetrics] struct used for reporting.
pub(crate) async fn aggregate_metrics(
    planning_start: Instant,
    manifest_metrics: ManifestMetrics,
    data_file_metrics_handle: JoinHandle<FileMetrics>,
    delete_file_metrics_handle: JoinHandle<FileMetrics>,
    index_metrics_handle: JoinHandle<DeleteIndexMetrics>,
) -> ScanMetrics {
    let (data_file_metrics, delete_file_metrics, index_metrics) = join!(
        data_file_metrics_handle,
        delete_file_metrics_handle,
        index_metrics_handle
    );

    // Only now (after consuming all metrics updates) do we know that
    // all concurrent work is finished and we can stop timing the
    // planning phase.
    let total_planning_duration = planning_start.elapsed();

    ScanMetrics {
        total_planning_duration,

        total_data_manifests: manifest_metrics.total_data_manifests,
        total_delete_manifests: manifest_metrics.total_delete_manifests,
        skipped_data_manifests: manifest_metrics.skipped_data_manifests,
        skipped_delete_manifests: manifest_metrics.skipped_delete_manifests,
        scanned_data_manifests: manifest_metrics.scanned_data_manifests,
        scanned_delete_manifests: manifest_metrics.scanned_delete_manifests,

        result_data_files: data_file_metrics.result_files,
        skipped_data_files: data_file_metrics.skipped_files,
        total_file_size_in_bytes: data_file_metrics.total_file_size_in_bytes,

        result_delete_files: delete_file_metrics.result_files,
        skipped_delete_files: delete_file_metrics.skipped_files,
        total_delete_file_size_in_bytes: delete_file_metrics.total_file_size_in_bytes,

        indexed_delete_files: index_metrics.indexed_delete_files,
        equality_delete_files: index_metrics.equality_delete_files,
        positional_delete_files: index_metrics.positional_delete_files,
    }
}

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

/// Subset of [ScanMetrics] produced by file-handling functions.
#[derive(Default)]
pub(crate) struct FileMetrics {
    result_files: u32,
    skipped_files: u32,
    total_file_size_in_bytes: u64,
}

impl FileMetrics {
    pub(crate) async fn accumulate(mut updates: Receiver<FileMetricsUpdate>) -> Self {
        let mut accumulator = Self::default();
        while let Some(update) = updates.next().await {
            match update {
                FileMetricsUpdate::Skipped => accumulator.skipped_files += 1,
                FileMetricsUpdate::Scanned { size_in_bytes } => {
                    accumulator.total_file_size_in_bytes += size_in_bytes;
                    accumulator.result_files += 1;
                }
            }
        }
        accumulator
    }
}

/// Represents an update to a single data or delete file.
pub(crate) enum FileMetricsUpdate {
    Skipped,
    Scanned { size_in_bytes: u64 },
}
