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

//! This module contains the metrics reporting API for Iceberg.
//!
//! It is used to report table operations in a pluggable way. See the [docs]
//! for more details.
//!
//! [docs] https://iceberg.apache.org/docs/latest/metrics-reporting

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tracing::info;

use crate::TableIdent;
use crate::expr::Predicate;
use crate::spec::SchemaId;

/// This trait defines the API for reporting metrics of table operations.
///
/// Refer to the [Iceberg docs] for details.
///
/// [Iceberg docs]: https://iceberg.apache.org/docs/latest/metrics-reporting/
#[async_trait]
pub(crate) trait MetricsReporter: Debug + Send + Sync {
    /// Indicates that an operation is done by reporting a MetricsReport.
    ///
    /// Any errors are expected to be handled internally.
    async fn report(&self, report: MetricsReport);
}

/// An enum of all metrics reports.
#[derive(Debug)]
pub(crate) enum MetricsReport {
    /// A Table Scan report that contains all relevant information from a Table Scan.
    Scan {
        table: TableIdent,
        snapshot_id: i64,
        schema_id: SchemaId,

        /// If None, the scan is an unfiltered full table scan.
        filter: Option<Arc<Predicate>>,

        /// If None, the scan projects all fields.
        // TODO: We could default to listing all field names in those cases: check what Java is doing.
        projected_field_names: Option<Vec<String>>,
        projected_field_ids: Arc<Vec<i32>>,

        metrics: Arc<ScanMetrics>,
        metadata: HashMap<String, String>,
    },
}

/// Carries all metrics for a particular scan.
#[derive(Debug)]
pub(crate) struct ScanMetrics {
    pub(crate) total_planning_duration: Duration,

    // Manifest-level metrics, computed by walking the snapshot's manifest list
    // file entries and checking which manifests match the scan's predicates.
    pub(crate) total_data_manifests: u32,
    pub(crate) total_delete_manifests: u32,
    pub(crate) skipped_data_manifests: u32,
    pub(crate) skipped_delete_manifests: u32,
    pub(crate) scanned_data_manifests: u32,
    pub(crate) scanned_delete_manifests: u32,

    // Data file-level metrics.
    pub(crate) result_data_files: u32,
    pub(crate) skipped_data_files: u32,
    pub(crate) total_file_size_in_bytes: u64,

    // Delete file-level metrics.
    pub(crate) result_delete_files: u32,
    pub(crate) skipped_delete_files: u32,
    pub(crate) total_delete_file_size_in_bytes: u64,

    pub(crate) indexed_delete_files: u32,
    pub(crate) equality_delete_files: u32,
    pub(crate) positional_delete_files: u32,
}

/// A reporter that logs the metrics to the console.
#[derive(Clone, Debug)]
pub(crate) struct LoggingMetricsReporter {}

impl LoggingMetricsReporter {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl MetricsReporter for LoggingMetricsReporter {
    async fn report(&self, report: MetricsReport) {
        match report {
            MetricsReport::Scan {
                table,
                snapshot_id,
                schema_id,
                filter,
                projected_field_names,
                projected_field_ids,
                metrics,
                metadata,
            } => {
                info!(
                    table = %table,
                    snapshot_id = snapshot_id,
                    schema_id = schema_id,
                    filter = ?filter,
                    projected_field_names = ?projected_field_names,
                    projected_field_ids = ?projected_field_ids,
                    scan_metrics.total_planning_duration = ?metrics.total_planning_duration,
                    scan_metrics.total_data_manifests = metrics.total_data_manifests,
                    scan_metrics.total_delete_manifests = metrics.total_delete_manifests,
                    scan_metrics.scanned_data_manifests = metrics.scanned_data_manifests,
                    scan_metrics.scanned_delete_manifests = metrics.scanned_delete_manifests,
                    scan_metrics.skipped_data_manifests = metrics.skipped_data_manifests,
                    scan_metrics.skipped_delete_manifests = metrics.skipped_delete_manifests,
                    scan_metrics.result_data_files = metrics.result_data_files,
                    scan_metrics.result_delete_files = metrics.result_delete_files,
                    scan_metrics.skipped_data_files = metrics.skipped_data_files,
                    scan_metrics.skipped_delete_files = metrics.skipped_delete_files,
                    scan_metrics.total_file_size_in_bytes = metrics.total_file_size_in_bytes,
                    scan_metrics.total_delete_file_size_in_bytes = metrics.total_delete_file_size_in_bytes,
                    scan_metrics.indexed_delete_files = metrics.indexed_delete_files,
                    scan_metrics.equality_delete_files = metrics.equality_delete_files,
                    scan_metrics.positional_delete_files = metrics.positional_delete_files,
                    metadata = ?metadata,
                    "Received metrics report"
                );
            }
        }
    }
}
