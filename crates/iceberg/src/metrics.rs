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
//! It is used to report table operations in a pluggable way. See [1] for more
//! details.
//!
//! [1] https://iceberg.apache.org/docs/latest/metrics-reporting

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::TableIdent;
use crate::expr::Predicate;
use crate::spec::SchemaId;

/// This trait defines the basic API for reporting metrics for operations to a Table.
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
        filter: Option<Arc<Predicate>>,
        schema_id: SchemaId,
        projected_field_ids: Arc<Vec<i32>>,
        // TODO: We could default to listing all field names, if all are selected
        // check what Java is doing.
        projected_field_names: Option<Vec<String>>,
        metrics: Box<ScanMetrics>,
        metadata: HashMap<String, String>,
    },
}

/// Carries all metrics for a particular scan.
#[derive(Debug)]
pub(crate) struct ScanMetrics {
    pub(crate) total_planning_duration: Duration,

    // Manfiest-level metrics, computed by walking the snapshot's manifest list
    // file entries and checking which manifests match the scan's predicates.
    pub(crate) total_data_manifests: u32, // TODO: Are these really just skipped+scanned?
    pub(crate) total_delete_manifests: u32,
    pub(crate) skipped_data_manifests: u32,
    pub(crate) skipped_delete_manifests: u32,
    pub(crate) scanned_data_manifests: u32,
    pub(crate) scanned_delete_manifests: u32,

    // Data file-level metrics.
    pub(crate) result_data_files: u32,
    pub(crate) skipped_data_files: u32,
    pub(crate) total_file_size_in_bytes: u64, // TODO: should then all be u64s?

    // Delete file-level metrics.
    pub(crate) result_delete_files: u32,
    pub(crate) skipped_delete_files: u32,
    pub(crate) total_delete_file_size_in_bytes: u64,

    pub(crate) indexed_delete_files: u32,
    pub(crate) equality_delete_files: u32,
    pub(crate) positional_delete_files: u32,
}

// TODO: This impl will provide public accessors for the fields, because
// crate-external implementators will need to access them, while (so far) only
// code within the crate will need to mutate them.
impl ScanMetrics {
    pub fn result_data_files(&self) -> Counter {
        Counter {
            value: self.result_data_files,
            unit: "file".to_string(),
        }
    }

    pub fn result_delete_files(&self) -> Counter {
        Counter {
            value: self.result_delete_files,
            unit: "file".to_string(),
        }
    }

    pub fn total_data_manifests(&self) -> Counter {
        Counter {
            value: self.total_data_manifests,
            unit: "manifest".to_string(),
        }
    }
}

struct Counter {
    value: u32,
    unit: String,
}

/// A reporter that logs the metrics to the console.
#[derive(Clone, Debug)]
pub(crate) struct LoggingMetricsReporter {}

#[async_trait]
impl MetricsReporter for LoggingMetricsReporter {
    async fn report(&self, report: MetricsReport) {
        println!("Reporting metrics: {:?}", report);
    }
}
