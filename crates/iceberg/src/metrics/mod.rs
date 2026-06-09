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

//! Metrics reporting for table operations.
//!
//! This module ports the self-contained core of Java's `org.apache.iceberg.metrics`
//! package: the immutable metrics-report data model, the [`MetricsReporter`] API, and
//! the [`InMemoryMetricsReporter`]. It is also the wire contract for the REST catalog's
//! `report-metrics` endpoint. (Java's `tracing`-based `LoggingMetricsReporter` is a
//! trivial follow-up deferred until a logging-facade dependency is approved.)
//!
//! # Java parity
//!
//! - [`MetricsReport`] mirrors Java's marker interface `MetricsReport`, modelled here as
//!   an `enum` (currently a single [`MetricsReport::Scan`] variant) rather than a trait
//!   object: a closed sum type avoids `dyn` downcasting and makes an illegal report kind
//!   unrepresentable, while still admitting future report kinds (e.g. a commit report) as
//!   new variants.
//! - [`ScanReport`] / [`ScanMetricsResult`] / [`CounterResult`] / [`TimerResult`] mirror the
//!   Java types of the same names. Every metric on [`ScanMetricsResult`] is an `Option`,
//!   matching Java's `@Nullable` accessors: a counter (or timer) that was never incremented
//!   is absent from the result and omitted from the JSON.
//! - The metric names match Java's `ScanMetrics` constants exactly (e.g.
//!   `total-planning-duration`, `result-data-files`).
//!
//! # JSON serialization
//!
//! The serde representation matches Java's `ScanReportParser` / `ScanMetricsResultParser` /
//! `CounterResultParser` / `TimerResultParser` for the metrics object, the counter shape
//! (`{"unit": <display-name>, "value": <i64>}`), the timer shape
//! (`{"count": <i64>, "time-unit": <lowercase>, "total-duration": <i64 in that unit>}`),
//! and the report's top-level field names (`table-name`, `snapshot-id`, `schema-id`,
//! `projected-field-ids`, `projected-field-names`, `metrics`, `metadata`).
//!
//! **Deferred divergence — the `filter` field.** Java serializes the scan `filter`
//! ([`Expression`]) with `ExpressionParser.toJson`, a structured expression-tree JSON.
//! Rust serializes the [`Predicate`] with its own `serde` derive, which does NOT byte-match
//! Java's `ExpressionParser` shape. Porting `ExpressionParser` is a large, separate effort;
//! it is tracked as a follow-up. The metric data — the high-value part of the contract — is
//! faithful; only the `filter` sub-document differs.
//!
//! [`Expression`]: crate::expr::Predicate

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::expr::Predicate;

/// The metric name constants, matching Java's `ScanMetrics` string constants.
///
/// =====================================================================================
mod metric_names {
    pub(super) const TOTAL_PLANNING_DURATION: &str = "total-planning-duration";
    pub(super) const RESULT_DATA_FILES: &str = "result-data-files";
    pub(super) const RESULT_DELETE_FILES: &str = "result-delete-files";
    pub(super) const TOTAL_DATA_MANIFESTS: &str = "total-data-manifests";
    pub(super) const TOTAL_DELETE_MANIFESTS: &str = "total-delete-manifests";
    pub(super) const SCANNED_DATA_MANIFESTS: &str = "scanned-data-manifests";
    pub(super) const SKIPPED_DATA_MANIFESTS: &str = "skipped-data-manifests";
    pub(super) const TOTAL_FILE_SIZE_IN_BYTES: &str = "total-file-size-in-bytes";
    pub(super) const TOTAL_DELETE_FILE_SIZE_IN_BYTES: &str = "total-delete-file-size-in-bytes";
    pub(super) const SKIPPED_DATA_FILES: &str = "skipped-data-files";
    pub(super) const SKIPPED_DELETE_FILES: &str = "skipped-delete-files";
    pub(super) const SCANNED_DELETE_MANIFESTS: &str = "scanned-delete-manifests";
    pub(super) const SKIPPED_DELETE_MANIFESTS: &str = "skipped-delete-manifests";
    pub(super) const INDEXED_DELETE_FILES: &str = "indexed-delete-files";
    pub(super) const EQUALITY_DELETE_FILES: &str = "equality-delete-files";
    pub(super) const POSITIONAL_DELETE_FILES: &str = "positional-delete-files";
    pub(super) const DVS: &str = "dvs";
}

/// The unit a [`CounterResult`] is measured in.
///
/// Mirrors Java's `MetricsContext.Unit`. The serde representation uses the lowercase
/// display name (`undefined` / `bytes` / `count`), matching Java's
/// `Unit.displayName()` used by `CounterResultParser`.
///
/// =====================================================================================
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetricUnit {
    /// An unspecified unit. Java `Unit.UNDEFINED` (`"undefined"`).
    #[serde(rename = "undefined")]
    Undefined,
    /// A byte count. Java `Unit.BYTES` (`"bytes"`).
    #[serde(rename = "bytes")]
    Bytes,
    /// A plain count of items. Java `Unit.COUNT` (`"count"`).
    #[serde(rename = "count")]
    Count,
}

/// The time unit a [`TimerResult`]'s duration is reported in.
///
/// Mirrors `java.util.concurrent.TimeUnit` (the subset Iceberg uses). The serde
/// representation uses the lowercase name (e.g. `nanoseconds`), matching Java's
/// `TimerResultParser`, which writes `timeUnit.name().toLowerCase()`.
///
/// =====================================================================================
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeUnit {
    /// Nanoseconds — the unit Iceberg's planning timer uses.
    #[serde(rename = "nanoseconds")]
    Nanoseconds,
    /// Microseconds.
    #[serde(rename = "microseconds")]
    Microseconds,
    /// Milliseconds.
    #[serde(rename = "milliseconds")]
    Milliseconds,
    /// Seconds.
    #[serde(rename = "seconds")]
    Seconds,
    /// Minutes.
    #[serde(rename = "minutes")]
    Minutes,
    /// Hours.
    #[serde(rename = "hours")]
    Hours,
    /// Days.
    #[serde(rename = "days")]
    Days,
}

impl TimeUnit {
    /// Returns the number of nanoseconds in one tick of this unit.
    ///
    /// Used to convert a [`Duration`] (always nanosecond-precise) to and from the integer
    /// `total-duration` value the JSON carries, which is expressed in this unit — exactly
    /// Java `TimerResultParser.fromDuration`/`toDuration` (`unit.convert(...)`).
    const fn nanos_per_unit(self) -> u128 {
        match self {
            TimeUnit::Nanoseconds => 1,
            TimeUnit::Microseconds => 1_000,
            TimeUnit::Milliseconds => 1_000_000,
            TimeUnit::Seconds => 1_000_000_000,
            TimeUnit::Minutes => 60 * 1_000_000_000,
            TimeUnit::Hours => 60 * 60 * 1_000_000_000,
            TimeUnit::Days => 24 * 60 * 60 * 1_000_000_000,
        }
    }
}

/// A serializable counter value, mirroring Java's `CounterResult`.
///
/// =====================================================================================
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CounterResult {
    /// The unit the counter is measured in.
    pub unit: MetricUnit,
    /// The counter's value. Java uses a signed `long`; we keep `i64`.
    pub value: i64,
}

impl CounterResult {
    /// Builds a counter result. Mirrors Java `CounterResult.of(unit, value)`.
    pub fn new(unit: MetricUnit, value: i64) -> Self {
        Self { unit, value }
    }
}

/// A serializable timer value, mirroring Java's `TimerResult`.
///
/// Java carries a `java.time.Duration` plus the `TimeUnit` the JSON reports in and a
/// `count` of timed events. The JSON `total-duration` is the duration expressed in
/// `time_unit` ticks; this type preserves the [`Duration`] exactly and converts only at
/// the serde boundary.
///
/// =====================================================================================
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimerResult {
    /// The unit the JSON `total-duration` is expressed in.
    pub time_unit: TimeUnit,
    /// The total measured duration.
    pub total_duration: Duration,
    /// The number of timed events. Java uses a signed `long`; we keep `i64`.
    pub count: i64,
}

impl TimerResult {
    /// Builds a timer result. Mirrors Java `TimerResult.of(timeUnit, duration, count)`.
    pub fn new(time_unit: TimeUnit, total_duration: Duration, count: i64) -> Self {
        Self {
            time_unit,
            total_duration,
            count,
        }
    }
}

/// The on-the-wire shape of a [`TimerResult`], matching Java `TimerResultParser`.
///
/// `total-duration` is the duration expressed in `time-unit` ticks (a truncating integer
/// conversion, exactly Java's `unit.convert(duration.toNanos(), NANOSECONDS)`).
#[derive(Serialize, Deserialize)]
struct TimerResultSerde {
    count: i64,
    #[serde(rename = "time-unit")]
    time_unit: TimeUnit,
    #[serde(rename = "total-duration")]
    total_duration: i64,
}

impl Serialize for TimerResult {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let nanos_per_unit = self.time_unit.nanos_per_unit();
        // Truncating division mirrors Java's `unit.convert(nanos, NANOSECONDS)`.
        let total_duration = (self.total_duration.as_nanos() / nanos_per_unit) as i64;
        TimerResultSerde {
            count: self.count,
            time_unit: self.time_unit,
            total_duration,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for TimerResult {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = TimerResultSerde::deserialize(deserializer)?;
        let total_nanos = (raw.total_duration.max(0) as u128) * raw.time_unit.nanos_per_unit();
        Ok(TimerResult {
            time_unit: raw.time_unit,
            // `Duration::from_nanos` takes a u64; saturate rather than panic on overflow.
            total_duration: Duration::from_nanos(u64::try_from(total_nanos).unwrap_or(u64::MAX)),
            count: raw.count,
        })
    }
}

/// A serializable snapshot of the counters and the timer collected during a scan.
///
/// Mirrors Java's `ScanMetricsResult`. Every field is optional, matching Java's
/// `@Nullable` accessors: a metric that was never recorded is `None` and is omitted from
/// the JSON.
///
/// =====================================================================================
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ScanMetricsResult {
    /// Total wall-clock time spent planning the scan. Java `totalPlanningDuration()`.
    #[serde(
        rename = "total-planning-duration",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub total_planning_duration: Option<TimerResult>,
    /// Number of data files in the scan result. Java `resultDataFiles()`.
    #[serde(
        rename = "result-data-files",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub result_data_files: Option<CounterResult>,
    /// Number of delete files in the scan result. Java `resultDeleteFiles()`.
    #[serde(
        rename = "result-delete-files",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub result_delete_files: Option<CounterResult>,
    /// Total number of data manifests considered. Java `totalDataManifests()`.
    #[serde(
        rename = "total-data-manifests",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub total_data_manifests: Option<CounterResult>,
    /// Total number of delete manifests considered. Java `totalDeleteManifests()`.
    #[serde(
        rename = "total-delete-manifests",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub total_delete_manifests: Option<CounterResult>,
    /// Number of data manifests scanned. Java `scannedDataManifests()`.
    #[serde(
        rename = "scanned-data-manifests",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub scanned_data_manifests: Option<CounterResult>,
    /// Number of data manifests skipped by manifest filtering. Java `skippedDataManifests()`.
    #[serde(
        rename = "skipped-data-manifests",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub skipped_data_manifests: Option<CounterResult>,
    /// Total size in bytes of all scanned data files. Java `totalFileSizeInBytes()`.
    #[serde(
        rename = "total-file-size-in-bytes",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub total_file_size_in_bytes: Option<CounterResult>,
    /// Total size in bytes of all scanned delete files. Java `totalDeleteFileSizeInBytes()`.
    #[serde(
        rename = "total-delete-file-size-in-bytes",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub total_delete_file_size_in_bytes: Option<CounterResult>,
    /// Number of data files skipped by metrics/partition filtering. Java `skippedDataFiles()`.
    #[serde(
        rename = "skipped-data-files",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub skipped_data_files: Option<CounterResult>,
    /// Number of delete files skipped. Java `skippedDeleteFiles()`.
    #[serde(
        rename = "skipped-delete-files",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub skipped_delete_files: Option<CounterResult>,
    /// Number of delete manifests scanned. Java `scannedDeleteManifests()`.
    #[serde(
        rename = "scanned-delete-manifests",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub scanned_delete_manifests: Option<CounterResult>,
    /// Number of delete manifests skipped. Java `skippedDeleteManifests()`.
    #[serde(
        rename = "skipped-delete-manifests",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub skipped_delete_manifests: Option<CounterResult>,
    /// Number of delete files indexed for the scan. Java `indexedDeleteFiles()`.
    #[serde(
        rename = "indexed-delete-files",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub indexed_delete_files: Option<CounterResult>,
    /// Number of equality delete files. Java `equalityDeleteFiles()`.
    #[serde(
        rename = "equality-delete-files",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub equality_delete_files: Option<CounterResult>,
    /// Number of positional delete files. Java `positionalDeleteFiles()`.
    #[serde(
        rename = "positional-delete-files",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub positional_delete_files: Option<CounterResult>,
    /// Number of deletion vectors. Java `dvs()`.
    #[serde(rename = "dvs", skip_serializing_if = "Option::is_none", default)]
    pub dvs: Option<CounterResult>,
}

// A compile-time guard that the metric-name constants match the serde `rename`s above.
// If a name and its `#[serde(rename = ...)]` ever drift, this `const` block fails to
// compile, catching the mismatch at build time rather than in a round-trip test.
const _: () = {
    assert!(matches_str(
        metric_names::TOTAL_PLANNING_DURATION,
        "total-planning-duration"
    ));
    assert!(matches_str(
        metric_names::RESULT_DATA_FILES,
        "result-data-files"
    ));
    assert!(matches_str(
        metric_names::RESULT_DELETE_FILES,
        "result-delete-files"
    ));
    assert!(matches_str(
        metric_names::TOTAL_DATA_MANIFESTS,
        "total-data-manifests"
    ));
    assert!(matches_str(
        metric_names::TOTAL_DELETE_MANIFESTS,
        "total-delete-manifests"
    ));
    assert!(matches_str(
        metric_names::SCANNED_DATA_MANIFESTS,
        "scanned-data-manifests"
    ));
    assert!(matches_str(
        metric_names::SKIPPED_DATA_MANIFESTS,
        "skipped-data-manifests"
    ));
    assert!(matches_str(
        metric_names::TOTAL_FILE_SIZE_IN_BYTES,
        "total-file-size-in-bytes"
    ));
    assert!(matches_str(
        metric_names::TOTAL_DELETE_FILE_SIZE_IN_BYTES,
        "total-delete-file-size-in-bytes"
    ));
    assert!(matches_str(
        metric_names::SKIPPED_DATA_FILES,
        "skipped-data-files"
    ));
    assert!(matches_str(
        metric_names::SKIPPED_DELETE_FILES,
        "skipped-delete-files"
    ));
    assert!(matches_str(
        metric_names::SCANNED_DELETE_MANIFESTS,
        "scanned-delete-manifests"
    ));
    assert!(matches_str(
        metric_names::SKIPPED_DELETE_MANIFESTS,
        "skipped-delete-manifests"
    ));
    assert!(matches_str(
        metric_names::INDEXED_DELETE_FILES,
        "indexed-delete-files"
    ));
    assert!(matches_str(
        metric_names::EQUALITY_DELETE_FILES,
        "equality-delete-files"
    ));
    assert!(matches_str(
        metric_names::POSITIONAL_DELETE_FILES,
        "positional-delete-files"
    ));
    assert!(matches_str(metric_names::DVS, "dvs"));
};

/// Compile-time string equality (`==` on `&str` is not yet `const`-stable for slices here).
const fn matches_str(left: &str, right: &str) -> bool {
    let left = left.as_bytes();
    let right = right.as_bytes();
    if left.len() != right.len() {
        return false;
    }
    let mut index = 0;
    while index < left.len() {
        if left[index] != right[index] {
            return false;
        }
        index += 1;
    }
    true
}

/// A report describing a completed table scan.
///
/// Mirrors Java's `ScanReport`. The `filter` is the (bound or unbound) row filter applied
/// to the scan; see the module docs for the JSON divergence on this one field.
///
/// =====================================================================================
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScanReport {
    /// The fully-qualified table name. Java `tableName()` → `table-name`.
    #[serde(rename = "table-name")]
    pub table_name: String,
    /// The snapshot the scan read. Java `snapshotId()` → `snapshot-id`.
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    /// The row filter applied. Java `filter()`. See module docs for the JSON divergence.
    pub filter: Predicate,
    /// The id of the schema the scan projected against. Java `schemaId()` → `schema-id`.
    #[serde(rename = "schema-id")]
    pub schema_id: i32,
    /// The field ids the scan projected. Java `projectedFieldIds()` → `projected-field-ids`.
    #[serde(rename = "projected-field-ids")]
    pub projected_field_ids: Vec<i32>,
    /// The field names the scan projected. Java `projectedFieldNames()` → `projected-field-names`.
    #[serde(rename = "projected-field-names")]
    pub projected_field_names: Vec<String>,
    /// The collected scan metrics. Java `scanMetrics()` → `metrics`.
    #[serde(rename = "metrics")]
    pub scan_metrics: ScanMetricsResult,
    /// Free-form metadata attached to the report. Java `metadata()` → `metadata`.
    ///
    /// Omitted from the JSON when empty, matching Java `ScanReportParser` (which only
    /// writes `metadata` when the map is non-empty).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, String>,
}

/// A metrics report produced by a table operation.
///
/// Mirrors Java's `MetricsReport` marker interface as a closed `enum`. A scan produces a
/// [`MetricsReport::Scan`]; future operations (e.g. commits) become new variants. Modelling
/// the closed set as an `enum` (rather than a `dyn MetricsReport`) avoids downcasting and
/// makes an unknown report kind unrepresentable.
///
/// =====================================================================================
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum MetricsReport {
    /// A table-scan report.
    Scan(ScanReport),
}

/// Reports [`MetricsReport`]s produced by table operations.
///
/// Mirrors Java's `MetricsReporter` interface (`report(MetricsReport)`).
///
/// =====================================================================================
pub trait MetricsReporter: Send + Sync {
    /// Reports a completed operation's metrics.
    ///
    /// Mirrors Java `MetricsReporter.report(MetricsReport)`. Implementations must not panic.
    fn report(&self, report: MetricsReport);
}

// NOTE: Java's `LoggingMetricsReporter` (a `tracing`-based reporter) is intentionally NOT ported
// in this increment — the `iceberg` crate has no logging-facade dependency, and adding one
// (`tracing`) is a dependency change that needs explicit approval. It is a trivial follow-up once
// the `tracing` dep is approved: a unit `MetricsReporter` whose `report` emits a `tracing::info!`.
// The reporter trait + `InMemoryMetricsReporter` below are the dependency-free core.

/// A [`MetricsReporter`] that retains the most recently reported [`MetricsReport`].
///
/// Mirrors Java's `InMemoryMetricsReporter` (`report` stores; `scanReport()` reads). Useful
/// for tests and for the future scan-emission wiring. The last report is held behind a
/// [`Mutex`] so the reporter is `Send + Sync` and can be shared.
///
/// =====================================================================================
#[derive(Debug, Default)]
pub struct InMemoryMetricsReporter {
    last_report: Mutex<Option<MetricsReport>>,
}

impl InMemoryMetricsReporter {
    /// Creates a new in-memory reporter with no stored report.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a clone of the most recently reported [`MetricsReport`], if any.
    ///
    /// Mirrors the read side of Java's `InMemoryMetricsReporter`.
    pub fn last_report(&self) -> Option<MetricsReport> {
        self.last_report
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Returns the most recent report if it is a [`ScanReport`], else `None`.
    ///
    /// Mirrors Java `InMemoryMetricsReporter.scanReport()` (without the throw-on-mismatch:
    /// the Rust enum already encodes the kind, so a non-scan report simply yields `None`).
    pub fn last_scan_report(&self) -> Option<ScanReport> {
        match self.last_report()? {
            MetricsReport::Scan(scan_report) => Some(scan_report),
        }
    }
}

impl MetricsReporter for InMemoryMetricsReporter {
    fn report(&self, report: MetricsReport) {
        let mut guard = self
            .last_report
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *guard = Some(report);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::Reference;
    use crate::spec::Datum;

    /// Builds a `ScanMetricsResult` with a populated planning timer and a couple of counters,
    /// leaving the rest absent — the shape a real (partial) scan produces.
    fn sample_metrics() -> ScanMetricsResult {
        ScanMetricsResult {
            total_planning_duration: Some(TimerResult::new(
                TimeUnit::Nanoseconds,
                Duration::from_nanos(123_456),
                1,
            )),
            result_data_files: Some(CounterResult::new(MetricUnit::Count, 5)),
            total_file_size_in_bytes: Some(CounterResult::new(MetricUnit::Bytes, 4096)),
            ..Default::default()
        }
    }

    fn sample_report() -> ScanReport {
        ScanReport {
            table_name: "db.table".to_string(),
            snapshot_id: 42,
            filter: Reference::new("x").less_than(Datum::long(10)),
            schema_id: 3,
            projected_field_ids: vec![1, 2, 3],
            projected_field_names: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            scan_metrics: sample_metrics(),
            metadata: HashMap::new(),
        }
    }

    /// Risk: the report's accessors silently drop or reorder a field, so a caller reads a
    /// different value than was constructed. Pins every Java-named field round-trips through
    /// the struct.
    #[test]
    fn test_scan_report_fields_round_trip_through_accessors() {
        let report = sample_report();

        assert_eq!(report.table_name, "db.table");
        assert_eq!(report.snapshot_id, 42);
        assert_eq!(report.schema_id, 3);
        assert_eq!(report.projected_field_ids, vec![1, 2, 3]);
        assert_eq!(report.projected_field_names, vec!["a", "b", "c"]);
        assert_eq!(
            report.filter,
            Reference::new("x").less_than(Datum::long(10))
        );

        let metrics = &report.scan_metrics;
        assert_eq!(
            metrics.total_planning_duration,
            Some(TimerResult::new(
                TimeUnit::Nanoseconds,
                Duration::from_nanos(123_456),
                1
            ))
        );
        assert_eq!(
            metrics.result_data_files,
            Some(CounterResult::new(MetricUnit::Count, 5))
        );
        assert_eq!(
            metrics.total_file_size_in_bytes,
            Some(CounterResult::new(MetricUnit::Bytes, 4096))
        );
    }

    /// Risk: a metric that was never incremented is reported as `Some(0)` instead of absent,
    /// diverging from Java's `@Nullable` optionality (and bloating the JSON). Pins the default
    /// is `None` and stays `None` when other metrics are set.
    #[test]
    fn test_absent_counter_is_none() {
        let metrics = sample_metrics();

        // Never set in `sample_metrics` ⇒ absent, not zero.
        assert_eq!(metrics.result_delete_files, None);
        assert_eq!(metrics.skipped_data_files, None);
        assert_eq!(metrics.dvs, None);
        assert_eq!(metrics.equality_delete_files, None);

        // A wholly-default result has every metric absent.
        let empty = ScanMetricsResult::default();
        assert_eq!(empty.total_planning_duration, None);
        assert_eq!(empty.result_data_files, None);
    }

    /// Risk: the in-memory reporter does not actually store the report (or stores a stale one),
    /// breaking the test-and-wiring contract. Pins `report` stores and `last_report` reads it.
    #[test]
    fn test_in_memory_reporter_stores_the_report() {
        let reporter = InMemoryMetricsReporter::new();
        assert_eq!(reporter.last_report(), None);

        reporter.report(MetricsReport::Scan(sample_report()));

        assert_eq!(
            reporter.last_report(),
            Some(MetricsReport::Scan(sample_report()))
        );
        assert_eq!(reporter.last_scan_report(), Some(sample_report()));
    }

    /// Risk: the in-memory reporter keeps the FIRST report instead of the LAST, so a caller
    /// reading after several scans sees a stale report. Pins that the most recent wins.
    #[test]
    fn test_in_memory_reporter_keeps_the_last_report() {
        let reporter = InMemoryMetricsReporter::new();

        let first = sample_report();
        let mut second = sample_report();
        second.snapshot_id = 99;
        second.table_name = "db.other".to_string();

        reporter.report(MetricsReport::Scan(first));
        reporter.report(MetricsReport::Scan(second.clone()));

        assert_eq!(reporter.last_scan_report(), Some(second));
    }

    /// Risk: the JSON serialization drifts from Java's `ScanReportParser` shape — a renamed or
    /// reordered top-level field, or a wrong metric/counter name — silently breaking the REST
    /// `report-metrics` contract. Pins the round-trip AND the exact field/metric names against
    /// a hand-written expected JSON.
    #[test]
    fn test_scan_report_json_round_trips_and_matches_java_shape() {
        let report = sample_report();

        let json = serde_json::to_value(&report).expect("serialize scan report");

        // Top-level field names match Java `ScanReportParser`.
        assert!(json.get("table-name").is_some(), "table-name present");
        assert!(json.get("snapshot-id").is_some(), "snapshot-id present");
        assert!(json.get("schema-id").is_some(), "schema-id present");
        assert!(
            json.get("projected-field-ids").is_some(),
            "projected-field-ids present"
        );
        assert!(
            json.get("projected-field-names").is_some(),
            "projected-field-names present"
        );
        assert!(json.get("filter").is_some(), "filter present");
        assert!(json.get("metrics").is_some(), "metrics present");
        // `metadata` is empty here ⇒ omitted, matching Java.
        assert!(json.get("metadata").is_none(), "empty metadata omitted");

        assert_eq!(json["table-name"], serde_json::json!("db.table"));
        assert_eq!(json["snapshot-id"], serde_json::json!(42));
        assert_eq!(json["schema-id"], serde_json::json!(3));
        assert_eq!(json["projected-field-ids"], serde_json::json!([1, 2, 3]));
        assert_eq!(
            json["projected-field-names"],
            serde_json::json!(["a", "b", "c"])
        );

        // Metric object: dashed names, counter shape, timer shape — matching the Java parsers.
        let metrics = &json["metrics"];
        assert_eq!(
            metrics["result-data-files"],
            serde_json::json!({ "unit": "count", "value": 5 })
        );
        assert_eq!(
            metrics["total-file-size-in-bytes"],
            serde_json::json!({ "unit": "bytes", "value": 4096 })
        );
        assert_eq!(
            metrics["total-planning-duration"],
            serde_json::json!({
                "count": 1,
                "time-unit": "nanoseconds",
                "total-duration": 123_456,
            })
        );
        // Absent metrics are omitted, not null.
        assert!(
            metrics.get("result-delete-files").is_none(),
            "absent counter omitted from JSON"
        );
        assert!(metrics.get("dvs").is_none(), "absent dvs omitted from JSON");

        // Full round-trip preserves the report.
        let restored: ScanReport = serde_json::from_value(json).expect("deserialize scan report");
        assert_eq!(restored, report);
    }

    /// Risk: the timer's `total-duration` is written in nanoseconds regardless of `time-unit`
    /// (or converted with the wrong factor), diverging from Java `TimerResultParser`, which
    /// expresses the duration in the reported unit. Pins a non-nanosecond unit converts.
    #[test]
    fn test_timer_total_duration_is_expressed_in_its_time_unit() {
        let timer = TimerResult::new(TimeUnit::Milliseconds, Duration::from_millis(250), 4);
        let json = serde_json::to_value(&timer).expect("serialize timer");

        assert_eq!(json["count"], serde_json::json!(4));
        assert_eq!(json["time-unit"], serde_json::json!("milliseconds"));
        // 250 ms expressed in milliseconds, not 250_000_000 ns.
        assert_eq!(json["total-duration"], serde_json::json!(250));

        let restored: TimerResult = serde_json::from_value(json).expect("deserialize timer");
        assert_eq!(restored, timer);
    }

    /// Risk: non-empty metadata is dropped on the wire. Pins it serializes when present and
    /// round-trips.
    #[test]
    fn test_non_empty_metadata_round_trips() {
        let mut report = sample_report();
        report
            .metadata
            .insert("engine-version".to_string(), "1.0".to_string());

        let json = serde_json::to_value(&report).expect("serialize");
        assert_eq!(
            json["metadata"],
            serde_json::json!({ "engine-version": "1.0" })
        );

        let restored: ScanReport = serde_json::from_value(json).expect("deserialize");
        assert_eq!(restored, report);
    }
}
