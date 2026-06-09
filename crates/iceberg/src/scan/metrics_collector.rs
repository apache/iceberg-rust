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

//! A thread-safe accumulator for the scan-planning metrics that a
//! [`MetricsReporter`](crate::metrics::MetricsReporter) receives as a
//! [`ScanReport`](crate::metrics::ScanReport).
//!
//! # Java parity
//!
//! This mirrors the role of Java's `ScanMetrics` (`core/.../metrics/ScanMetrics.java`)
//! during a `DataTableScan`: the planning code increments the counters as it loads
//! manifest-list entries, prunes manifests against the partition filter, and produces
//! [`FileScanTask`](super::FileScanTask)s (Java `ScanMetricsUtil.fileTask`). When planning
//! completes, the collector is [`snapshot`](ScanMetricsCollector::snapshot)ted into the
//! serializable [`ScanMetricsResult`] that the report carries (Java
//! `ScanMetricsResult.fromScanMetrics`).
//!
//! # Opt-in
//!
//! A collector exists ONLY when the scan was configured with a reporter
//! (`TableScanBuilder::with_metrics_reporter`). When no reporter is set, the planning
//! path threads `None` and performs no counting, no allocation, and no timing — the scan
//! is byte-for-byte the un-instrumented path.
//!
//! # Concurrency
//!
//! `plan_files` fans manifest and entry processing out across spawned tasks that each hold
//! a clone of the same `Arc<ScanMetricsCollector>`. The counters are therefore plain
//! [`AtomicI64`]s incremented with [`Ordering::Relaxed`]: the increments are commutative
//! and order-independent, and a single happens-before barrier (the stream draining to
//! completion before [`snapshot`](ScanMetricsCollector::snapshot) reads them) is enough to
//! observe the final totals. `Relaxed` is correct here — no counter value gates another
//! thread's control flow.
//!
//! # Populated vs. left `None`
//!
//! The collector populates exactly the metrics that scan planning can count cleanly and
//! accurately:
//!
//! - `total_planning_duration` (the timer, captured by the caller, not this struct),
//! - `total_data_manifests` / `total_delete_manifests` (manifest-list entries by content),
//! - `scanned_data_manifests` / `skipped_data_manifests` (data manifests kept / pruned by
//!   the partition filter),
//! - `scanned_delete_manifests` / `skipped_delete_manifests` (delete manifests kept /
//!   pruned),
//! - `result_data_files` (produced [`FileScanTask`](super::FileScanTask)s),
//! - `result_delete_files` (delete-file references attached across the produced tasks),
//! - `total_file_size_in_bytes` / `total_delete_file_size_in_bytes` (summed file sizes).
//!
//! The remaining Java metrics — `skipped_data_files`, `skipped_delete_files`,
//! `indexed_delete_files`, `equality_delete_files`, `positional_delete_files`, `dvs` —
//! are deliberately left `None` (absent from the report). They count delete-index
//! internals and per-file metrics/partition pruning that the Rust planner either does not
//! expose at a single accumulation point yet or does not distinguish; populating them is a
//! follow-up. `None` (not `Some(0)`) matches Java's `@Nullable` "never incremented" shape.

use std::sync::atomic::{AtomicI64, Ordering};

use crate::metrics::{CounterResult, MetricUnit, ScanMetricsResult};

/// A thread-safe accumulator of scan-planning counters.
///
/// Shared (via `Arc`) across the concurrent manifest/entry processing of a single
/// `plan_files` call, then [`snapshot`](Self::snapshot)ted into a [`ScanMetricsResult`]
/// once planning has fully completed. See the [module docs](self) for the Java-parity
/// mapping and the opt-in contract.
#[derive(Debug, Default)]
pub(crate) struct ScanMetricsCollector {
    /// Data manifests listed in the snapshot's manifest list. Java `totalDataManifests`.
    total_data_manifests: AtomicI64,
    /// Delete manifests listed in the snapshot's manifest list. Java `totalDeleteManifests`.
    total_delete_manifests: AtomicI64,
    /// Data manifests kept after partition-filter pruning. Java `scannedDataManifests`.
    scanned_data_manifests: AtomicI64,
    /// Data manifests pruned by the partition filter. Java `skippedDataManifests`.
    skipped_data_manifests: AtomicI64,
    /// Delete manifests kept after partition-filter pruning. Java `scannedDeleteManifests`.
    scanned_delete_manifests: AtomicI64,
    /// Delete manifests pruned by the partition filter. Java `skippedDeleteManifests`.
    skipped_delete_manifests: AtomicI64,
    /// Data files in the produced scan tasks. Java `resultDataFiles`.
    result_data_files: AtomicI64,
    /// Delete-file references across the produced tasks. Java `resultDeleteFiles`.
    result_delete_files: AtomicI64,
    /// Summed size of the produced tasks' data files. Java `totalFileSizeInBytes`.
    total_file_size_in_bytes: AtomicI64,
    /// Summed size of the produced tasks' delete files. Java `totalDeleteFileSizeInBytes`.
    total_delete_file_size_in_bytes: AtomicI64,
}

impl ScanMetricsCollector {
    /// Creates a collector with every counter at zero.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Records that a data manifest passed the partition filter and will be scanned.
    pub(crate) fn increment_scanned_data_manifests(&self) {
        self.scanned_data_manifests.fetch_add(1, Ordering::Relaxed);
    }

    /// Records that a data manifest was pruned by the partition filter (Java
    /// `skippedDataManifests().increment()` at the `ManifestGroup` filter point).
    pub(crate) fn increment_skipped_data_manifests(&self) {
        self.skipped_data_manifests.fetch_add(1, Ordering::Relaxed);
    }

    /// Records that a delete manifest passed the partition filter and will be scanned.
    pub(crate) fn increment_scanned_delete_manifests(&self) {
        self.scanned_delete_manifests
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Records that a delete manifest was pruned by the partition filter.
    pub(crate) fn increment_skipped_delete_manifests(&self) {
        self.skipped_delete_manifests
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Records the manifest-list totals by content type (Java `DataTableScan.doPlanFiles`
    /// `totalDataManifests().increment(dataManifests.size())` /
    /// `totalDeleteManifests().increment(deleteManifests.size())`).
    pub(crate) fn add_total_manifests(&self, data_manifests: i64, delete_manifests: i64) {
        self.total_data_manifests
            .fetch_add(data_manifests, Ordering::Relaxed);
        self.total_delete_manifests
            .fetch_add(delete_manifests, Ordering::Relaxed);
    }

    /// Records a single produced data-file task and its attached delete files, mirroring
    /// Java `ScanMetricsUtil.fileTask`: bump `result_data_files` by one, accumulate the
    /// data file's size, count each attached delete file, and accumulate their sizes.
    pub(crate) fn record_file_task(
        &self,
        data_file_size_in_bytes: i64,
        delete_file_count: i64,
        delete_file_size_in_bytes: i64,
    ) {
        self.result_data_files.fetch_add(1, Ordering::Relaxed);
        self.total_file_size_in_bytes
            .fetch_add(data_file_size_in_bytes, Ordering::Relaxed);
        self.result_delete_files
            .fetch_add(delete_file_count, Ordering::Relaxed);
        self.total_delete_file_size_in_bytes
            .fetch_add(delete_file_size_in_bytes, Ordering::Relaxed);
    }

    /// Snapshots the accumulated counters into a [`ScanMetricsResult`].
    ///
    /// Each counter is rendered as a `Some(CounterResult)` (so a zero counter is still
    /// reported, matching Java once a `DataTableScan` always increments its totals); the
    /// metrics this planner does not yet collect are left `None`. The planning timer is
    /// added by the caller (it owns the [`Instant`](std::time::Instant)). Reads use
    /// `Ordering::Relaxed`; the caller guarantees the producing tasks have completed
    /// before calling this.
    pub(crate) fn snapshot(&self) -> ScanMetricsResult {
        ScanMetricsResult {
            // The timer is populated by the caller (owns the `Instant`).
            total_planning_duration: None,
            result_data_files: Some(count(self.result_data_files.load(Ordering::Relaxed))),
            result_delete_files: Some(count(self.result_delete_files.load(Ordering::Relaxed))),
            total_data_manifests: Some(count(self.total_data_manifests.load(Ordering::Relaxed))),
            total_delete_manifests: Some(count(
                self.total_delete_manifests.load(Ordering::Relaxed),
            )),
            scanned_data_manifests: Some(count(
                self.scanned_data_manifests.load(Ordering::Relaxed),
            )),
            skipped_data_manifests: Some(count(
                self.skipped_data_manifests.load(Ordering::Relaxed),
            )),
            total_file_size_in_bytes: Some(bytes(
                self.total_file_size_in_bytes.load(Ordering::Relaxed),
            )),
            total_delete_file_size_in_bytes: Some(bytes(
                self.total_delete_file_size_in_bytes.load(Ordering::Relaxed),
            )),
            scanned_delete_manifests: Some(count(
                self.scanned_delete_manifests.load(Ordering::Relaxed),
            )),
            skipped_delete_manifests: Some(count(
                self.skipped_delete_manifests.load(Ordering::Relaxed),
            )),
            // Not yet collected by the Rust planner — left absent (see module docs).
            skipped_data_files: None,
            skipped_delete_files: None,
            indexed_delete_files: None,
            equality_delete_files: None,
            positional_delete_files: None,
            dvs: None,
        }
    }
}

/// Builds a plain-count [`CounterResult`] (Java `MetricsContext.Unit.COUNT`).
fn count(value: i64) -> CounterResult {
    CounterResult::new(MetricUnit::Count, value)
}

/// Builds a byte-count [`CounterResult`] (Java `MetricsContext.Unit.BYTES`).
fn bytes(value: i64) -> CounterResult {
    CounterResult::new(MetricUnit::Bytes, value)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Risk: a counter is reported under the wrong metric name/field, or a byte counter is
    /// rendered with the `count` unit (or vice versa), so a consumer reads the wrong value
    /// or unit. Pins each increment lands on its own field with the right unit.
    #[test]
    fn test_collector_snapshot_maps_each_counter_to_its_field_and_unit() {
        let collector = ScanMetricsCollector::new();
        collector.add_total_manifests(4, 2);
        collector.increment_scanned_data_manifests();
        collector.increment_scanned_data_manifests();
        collector.increment_scanned_data_manifests();
        collector.increment_skipped_data_manifests();
        collector.increment_scanned_delete_manifests();
        collector.increment_skipped_delete_manifests();
        collector.increment_skipped_delete_manifests();
        // one task: 1000-byte data file, 2 delete files totaling 30 bytes
        collector.record_file_task(1000, 2, 30);
        // a second task: 500-byte data file, no deletes
        collector.record_file_task(500, 0, 0);

        let result = collector.snapshot();

        assert_eq!(result.total_data_manifests, Some(count(4)));
        assert_eq!(result.total_delete_manifests, Some(count(2)));
        assert_eq!(result.scanned_data_manifests, Some(count(3)));
        assert_eq!(result.skipped_data_manifests, Some(count(1)));
        assert_eq!(result.scanned_delete_manifests, Some(count(1)));
        assert_eq!(result.skipped_delete_manifests, Some(count(2)));
        assert_eq!(result.result_data_files, Some(count(2)));
        assert_eq!(result.result_delete_files, Some(count(2)));
        assert_eq!(result.total_file_size_in_bytes, Some(bytes(1500)));
        assert_eq!(result.total_delete_file_size_in_bytes, Some(bytes(30)));

        // The planning timer is the caller's responsibility — absent here.
        assert_eq!(result.total_planning_duration, None);
        // The not-yet-collected metrics stay absent (Java `@Nullable` shape).
        assert_eq!(result.skipped_data_files, None);
        assert_eq!(result.skipped_delete_files, None);
        assert_eq!(result.indexed_delete_files, None);
        assert_eq!(result.equality_delete_files, None);
        assert_eq!(result.positional_delete_files, None);
        assert_eq!(result.dvs, None);
    }

    /// Risk: the byte counters use the plain `count` unit (not `bytes`), diverging from
    /// Java `ScanMetrics.totalFileSizeInBytes` (declared with `Unit.BYTES`). Pins the unit
    /// distinction the field-mapping test would miss if `count` and `bytes` were swapped.
    #[test]
    fn test_byte_counters_carry_the_bytes_unit() {
        let collector = ScanMetricsCollector::new();
        collector.record_file_task(2048, 1, 64);

        let result = collector.snapshot();

        assert_eq!(
            result.total_file_size_in_bytes.unwrap().unit,
            MetricUnit::Bytes
        );
        assert_eq!(
            result.total_delete_file_size_in_bytes.unwrap().unit,
            MetricUnit::Bytes
        );
        // result counters are plain counts, not bytes.
        assert_eq!(result.result_data_files.unwrap().unit, MetricUnit::Count);
    }
}
