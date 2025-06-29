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

use futures::StreamExt;
use futures::channel::mpsc::Receiver;
use futures::channel::oneshot;

use crate::delete_file_index::DeleteIndexMetrics;
use crate::metrics::ScanMetrics;

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

/// Receives metrics updates from different sources and combines them into the
/// [ScanMetrics] struct used for reporting.
pub(crate) async fn aggregate_metrics(
    planning_start: Instant,
    manifest_metrics: ManifestMetrics,
    mut data_file_metrics_rx: Receiver<FileMetricsUpdate>,
    mut delete_file_metrics_rx: Receiver<FileMetricsUpdate>,
    index_metrics_rx: oneshot::Receiver<DeleteIndexMetrics>,
) -> ScanMetrics {
    // TODO: Double-check the order of blocking operations. We should start with
    // result streams that we need to unblock first. This is because we attach
    // some concurrency limit to each metrics channel and we currently
    // sequentially read from each one to prevent a lock on the metrics struct.

    let mut result_data_files = 0;
    let mut total_file_size_in_bytes = 0;
    let mut skipped_data_files = 0;
    while let Some(data_file) = data_file_metrics_rx.next().await {
        match data_file {
            FileMetricsUpdate::Scanned { size_in_bytes } => {
                result_data_files += 1;
                total_file_size_in_bytes += size_in_bytes;
            }
            FileMetricsUpdate::Skipped => skipped_data_files += 1,
        }
    }

    let mut result_delete_files = 0;
    let mut total_delete_file_size_in_bytes = 0;
    let mut skipped_delete_files = 0;
    while let Some(delete_file) = delete_file_metrics_rx.next().await {
        match delete_file {
            FileMetricsUpdate::Scanned { size_in_bytes } => {
                result_delete_files += 1;
                total_delete_file_size_in_bytes += size_in_bytes;
            }
            FileMetricsUpdate::Skipped => skipped_delete_files += 1,
        }
    }

    let index_metrics = index_metrics_rx.await.unwrap();

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

        result_data_files,
        skipped_data_files,
        total_file_size_in_bytes,

        result_delete_files,
        skipped_delete_files,
        total_delete_file_size_in_bytes,

        indexed_delete_files: index_metrics.indexed_delete_files,
        equality_delete_files: index_metrics.equality_delete_files,
        positional_delete_files: index_metrics.positional_delete_files,
    }
}
