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

//! Operation-scoped planning reports and asynchronous consumers.
//!
//! Reports describe work performed, including partial or failed scans, without an outcome tag.
//! The default consumer logs at debug level.

use std::fmt::Debug;
use std::time::Duration;

use crate::TableIdent;
use crate::expr::Predicate;

/// Consumes finalized operation reports asynchronously.
///
/// Each snapshot-bearing planning invocation attempts delivery at most once, after the consumer
/// terminates and all contributing workers stop. Reports also cover early drop (including LIMIT),
/// errors, and unwinding panics; they do not certify successful or exhaustive planning. Tables
/// without snapshots and unpolled planning futures do not report. Calls may run concurrently.
///
/// Callbacks run on the table's I/O runtime with the originating tracing dispatcher, including
/// the default debug logging consumer.
/// Planning, including successful stream exhaustion, does not wait for the callback. Delivery
/// requires the runtime to remain alive after planning; runtime shutdown or process termination
/// can lose reports. Consumers that need delivery confirmation must provide their own signal.
/// There are no automatic retries.
///
/// Implementations handle and log their own delivery failures; reporting cannot return a
/// planning error. Unwinding callback panics are isolated from planning. Implementations should
/// yield during I/O, bound their own waits, and avoid blocking or aborting the process.
/// Reporter time is excluded from the report's planning duration.
#[async_trait::async_trait]
pub(crate) trait MetricsReporter: Debug + Send + Sync {
    /// Consumes one finalized report.
    async fn report(&self, report: MetricsReport);
}

/// A finalized operation report, extensible to additional operation types.
#[derive(Debug)]
#[non_exhaustive]
pub(crate) enum MetricsReport {
    /// Metrics from one scan-planning invocation, including COW planning.
    Scan(Box<ScanReport>),
}

/// An immutable snapshot of a terminated scan's identity, projection, and planning metrics.
///
/// Reports include work planned ahead of consumption and have no success or cancellation tag.
/// Iceberg constructs reports; consumers receive them through [`MetricsReporter`].
#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub(crate) struct ScanReport {
    pub(crate) table_ident: TableIdent,
    pub(crate) snapshot_id: i64,
    pub(crate) schema_id: i32,
    pub(crate) projected_field_ids: Vec<i32>,
    pub(crate) projected_field_names: Vec<String>,
    pub(crate) filter: Predicate,
    pub(crate) metrics: ScanMetrics,
    pub(crate) total_planning_duration: Duration,
}

impl ScanReport {
    /// Returns the scanned table's identifier.
    pub(crate) fn table_ident(&self) -> &TableIdent {
        &self.table_ident
    }

    /// Returns the selected snapshot ID.
    pub(crate) fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }

    /// Returns the source schema ID for a full projection, or 0 for a reduced schema.
    pub(crate) fn schema_id(&self) -> i32 {
        self.schema_id
    }

    /// Returns resolved projected IDs, including nested and filter-required fields.
    pub(crate) fn projected_field_ids(&self) -> &[i32] {
        &self.projected_field_ids
    }

    /// Returns names corresponding to [`Self::projected_field_ids`].
    pub(crate) fn projected_field_names(&self) -> &[String] {
        &self.projected_field_names
    }

    /// Returns the unsanitized selection predicate, including its literal values.
    pub(crate) fn filter(&self) -> &Predicate {
        &self.filter
    }

    /// Returns the scan's planning counters.
    pub(crate) fn metrics(&self) -> &ScanMetrics {
        &self.metrics
    }

    /// Returns elapsed planning, consumption, and worker-teardown time, excluding delivery.
    pub(crate) fn total_planning_duration(&self) -> Duration {
        self.total_planning_duration
    }
}

/// Immutable scan-planning counters, distinct from Arrow row-reading metrics.
#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub(crate) struct ScanMetrics {
    pub(crate) total_data_manifests: u64,
    pub(crate) total_delete_manifests: u64,
    pub(crate) scanned_data_manifests: u64,
    pub(crate) scanned_delete_manifests: u64,
    pub(crate) skipped_data_manifests: u64,
    pub(crate) skipped_delete_manifests: u64,
    pub(crate) result_data_files: u64,
    pub(crate) result_delete_files: u64,
    pub(crate) skipped_data_files: u64,
    pub(crate) skipped_delete_files: u64,
    pub(crate) indexed_delete_files: u64,
    pub(crate) equality_delete_files: u64,
    pub(crate) positional_delete_files: u64,
    pub(crate) dvs: u64,
    pub(crate) total_file_size_in_bytes: u64,
    pub(crate) total_delete_file_size_in_bytes: u64,
}

impl ScanMetrics {
    /// Data manifests listed for the snapshot.
    pub(crate) fn total_data_manifests(&self) -> u64 {
        self.total_data_manifests
    }

    /// Delete manifests listed for the snapshot.
    pub(crate) fn total_delete_manifests(&self) -> u64 {
        self.total_delete_manifests
    }

    /// Data manifests whose processing started.
    pub(crate) fn scanned_data_manifests(&self) -> u64 {
        self.scanned_data_manifests
    }

    /// Delete manifests whose processing started.
    pub(crate) fn scanned_delete_manifests(&self) -> u64 {
        self.scanned_delete_manifests
    }

    /// Data manifests rejected by partition predicates or live-entry counts.
    pub(crate) fn skipped_data_manifests(&self) -> u64 {
        self.skipped_data_manifests
    }

    /// Delete manifests rejected by partition predicates or live-entry counts.
    pub(crate) fn skipped_delete_manifests(&self) -> u64 {
        self.skipped_delete_manifests
    }

    /// Data-file tasks planned, including buffered tasks not consumed.
    pub(crate) fn result_data_files(&self) -> u64 {
        self.result_data_files
    }

    /// Delete attachments across planned data-file tasks, including repeated files.
    pub(crate) fn result_delete_files(&self) -> u64 {
        self.result_delete_files
    }

    /// Live data-file entries rejected by predicates, excluding dead entries.
    pub(crate) fn skipped_data_files(&self) -> u64 {
        self.skipped_data_files
    }

    /// Live delete-file entries rejected by predicates, excluding dead entries.
    pub(crate) fn skipped_delete_files(&self) -> u64 {
        self.skipped_delete_files
    }

    /// Delete files accepted for indexing, including unmatched deletes and DVs.
    pub(crate) fn indexed_delete_files(&self) -> u64 {
        self.indexed_delete_files
    }

    /// Equality delete files accepted for indexing.
    pub(crate) fn equality_delete_files(&self) -> u64 {
        self.equality_delete_files
    }

    /// Position delete files accepted for indexing, excluding deletion vectors.
    pub(crate) fn positional_delete_files(&self) -> u64 {
        self.positional_delete_files
    }

    /// Deletion vectors accepted for indexing.
    pub(crate) fn dvs(&self) -> u64 {
        self.dvs
    }

    /// Sum of physical sizes of planned data files, in bytes.
    pub(crate) fn total_file_size_in_bytes(&self) -> u64 {
        self.total_file_size_in_bytes
    }

    /// Bytes attached to tasks: physical delete-file sizes or DV content sizes, repeated per attachment.
    pub(crate) fn total_delete_file_size_in_bytes(&self) -> u64 {
        self.total_delete_file_size_in_bytes
    }

    pub(crate) fn empty() -> Self {
        Self {
            total_data_manifests: 0,
            total_delete_manifests: 0,
            scanned_data_manifests: 0,
            scanned_delete_manifests: 0,
            skipped_data_manifests: 0,
            skipped_delete_manifests: 0,
            result_data_files: 0,
            result_delete_files: 0,
            skipped_data_files: 0,
            skipped_delete_files: 0,
            indexed_delete_files: 0,
            equality_delete_files: 0,
            positional_delete_files: 0,
            dvs: 0,
            total_file_size_in_bytes: 0,
            total_delete_file_size_in_bytes: 0,
        }
    }
}

#[derive(Debug)]
pub(crate) struct LoggingMetricsReporter;

#[async_trait::async_trait]
impl MetricsReporter for LoggingMetricsReporter {
    async fn report(&self, report: MetricsReport) {
        let MetricsReport::Scan(report) = report;
        tracing::debug!(target: "iceberg::scan::report",
            table_name = %report.table_ident(),
            snapshot_id = report.snapshot_id(),
            schema_id = report.schema_id(),
            projected_field_ids = ?report.projected_field_ids(),
            projected_field_names = ?report.projected_field_names(),
            has_filter = report.filter() != &Predicate::AlwaysTrue,
            total_planning_duration_ns = report.total_planning_duration().as_nanos() as u64,
            total_data_manifests = report.metrics().total_data_manifests(),
            total_delete_manifests = report.metrics().total_delete_manifests(),
            scanned_data_manifests = report.metrics().scanned_data_manifests(),
            scanned_delete_manifests = report.metrics().scanned_delete_manifests(),
            skipped_data_manifests = report.metrics().skipped_data_manifests(),
            skipped_delete_manifests = report.metrics().skipped_delete_manifests(),
            result_data_files = report.metrics().result_data_files(),
            result_delete_files = report.metrics().result_delete_files(),
            skipped_data_files = report.metrics().skipped_data_files(),
            skipped_delete_files = report.metrics().skipped_delete_files(),
            indexed_delete_files = report.metrics().indexed_delete_files(),
            equality_delete_files = report.metrics().equality_delete_files(),
            positional_delete_files = report.metrics().positional_delete_files(),
            dvs = report.metrics().dvs(),
            total_file_size_in_bytes = report.metrics().total_file_size_in_bytes(),
            total_delete_file_size_in_bytes = report.metrics().total_delete_file_size_in_bytes(),
            "Scan planning terminated"
        );
    }
}
