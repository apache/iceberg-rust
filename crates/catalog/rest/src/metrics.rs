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

use std::collections::HashMap;
use std::time::Duration;

use iceberg::expr::{Predicate, PredicateOperator};
use iceberg::metrics::{ScanMetrics, ScanReport};
use iceberg::spec::{Datum, Literal, PrimitiveLiteral, PrimitiveType, SchemaId, Type};
use serde::ser::{Error, SerializeStruct};
use serde::{Serialize, Serializer};
use serde_json::Value;

/// A metrics report request for the Iceberg REST catalog.
///
/// Filter literals are serialized as supplied. Metrics outside the signed 64-bit range
/// (nanoseconds for planning duration), non-finite literals, and temporal literals outside
/// the supported date/time range return serialization errors.
#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct ReportMetricsRequest {
    report_type: &'static str,
    table_name: String,
    snapshot_id: i64,
    schema_id: SchemaId,
    #[serde(serialize_with = "filter")]
    filter: Option<Predicate>,
    projected_field_names: Vec<String>,
    projected_field_ids: Vec<i32>,
    metrics: ScanMetricsResult,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    metadata: HashMap<String, String>,
}

impl From<&ScanReport> for ReportMetricsRequest {
    fn from(report: &ScanReport) -> Self {
        Self {
            report_type: "scan-report",
            table_name: report.table_ident().to_string(),
            snapshot_id: report.snapshot_id(),
            schema_id: report.schema_id(),
            filter: report.filter().cloned(),
            projected_field_names: report.projected_field_names().to_vec(),
            projected_field_ids: report.projected_field_ids().to_vec(),
            metrics: report.metrics().into(),
            metadata: report.metadata().clone(),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
struct ScanMetricsResult {
    #[serde(serialize_with = "duration")]
    total_planning_duration: Duration,
    #[serde(serialize_with = "count")]
    total_data_manifests: u64,
    #[serde(serialize_with = "count")]
    total_delete_manifests: u64,
    #[serde(serialize_with = "count")]
    skipped_data_manifests: u64,
    #[serde(serialize_with = "count")]
    skipped_delete_manifests: u64,
    #[serde(serialize_with = "count")]
    scanned_data_manifests: u64,
    #[serde(serialize_with = "count")]
    scanned_delete_manifests: u64,
    #[serde(serialize_with = "count")]
    result_data_files: u64,
    #[serde(serialize_with = "count")]
    skipped_data_files: u64,
    #[serde(serialize_with = "bytes")]
    total_file_size_in_bytes: u64,
    #[serde(serialize_with = "count")]
    result_delete_files: u64,
    #[serde(serialize_with = "count")]
    skipped_delete_files: u64,
    #[serde(serialize_with = "bytes")]
    total_delete_file_size_in_bytes: u64,
    #[serde(serialize_with = "count")]
    indexed_delete_files: u64,
    #[serde(serialize_with = "count")]
    equality_delete_files: u64,
    #[serde(serialize_with = "count")]
    positional_delete_files: u64,
    #[serde(serialize_with = "count")]
    dvs: u64,
}

impl From<&ScanMetrics> for ScanMetricsResult {
    fn from(metrics: &ScanMetrics) -> Self {
        Self {
            total_planning_duration: metrics.total_planning_duration(),
            total_data_manifests: metrics.total_data_manifests(),
            total_delete_manifests: metrics.total_delete_manifests(),
            skipped_data_manifests: metrics.skipped_data_manifests(),
            skipped_delete_manifests: metrics.skipped_delete_manifests(),
            scanned_data_manifests: metrics.scanned_data_manifests(),
            scanned_delete_manifests: metrics.scanned_delete_manifests(),
            result_data_files: metrics.result_data_files(),
            skipped_data_files: metrics.skipped_data_files(),
            total_file_size_in_bytes: metrics.total_file_size_in_bytes(),
            result_delete_files: metrics.result_delete_files(),
            skipped_delete_files: metrics.skipped_delete_files(),
            total_delete_file_size_in_bytes: metrics.total_delete_file_size_in_bytes(),
            indexed_delete_files: metrics.indexed_delete_files(),
            equality_delete_files: metrics.equality_delete_files(),
            positional_delete_files: metrics.positional_delete_files(),
            dvs: metrics.dvs(),
        }
    }
}

fn filter<S: Serializer>(filter: &Option<Predicate>, serializer: S) -> Result<S::Ok, S::Error> {
    RestPredicate(filter.as_ref().unwrap_or(&Predicate::AlwaysTrue)).serialize(serializer)
}

fn count<S: Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
    counter("count", *value, serializer)
}

fn bytes<S: Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
    counter("bytes", *value, serializer)
}

fn counter<S: Serializer>(
    unit: &'static str,
    value: u64,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    CounterResult {
        unit,
        value: i64::try_from(value).map_err(S::Error::custom)?,
    }
    .serialize(serializer)
}

fn duration<S: Serializer>(value: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
    TimerResult {
        time_unit: "nanoseconds",
        count: 1,
        total_duration: i64::try_from(value.as_nanos()).map_err(S::Error::custom)?,
    }
    .serialize(serializer)
}

#[derive(Serialize)]
struct CounterResult {
    unit: &'static str,
    value: i64,
}

#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct TimerResult {
    time_unit: &'static str,
    count: i64,
    total_duration: i64,
}

struct RestPredicate<'a>(&'a Predicate);

// The term/value form is accepted by the REST spec and matches Java's ExpressionParser.
// https://github.com/apache/iceberg/blob/c44bd8e7c183cf92339939f2aff5ca2aa2a0fd3d/open-api/rest-catalog-open-api.yaml
impl Serialize for RestPredicate<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0 {
            Predicate::AlwaysTrue => serializer.serialize_bool(true),
            Predicate::AlwaysFalse => serializer.serialize_bool(false),
            Predicate::And(expr) | Predicate::Or(expr) => {
                let [left, right] = expr.inputs();
                let mut state = serializer.serialize_struct("Predicate", 3)?;
                state.serialize_field(
                    "type",
                    if matches!(self.0, Predicate::And(_)) {
                        "and"
                    } else {
                        "or"
                    },
                )?;
                state.serialize_field("left", &RestPredicate(left))?;
                state.serialize_field("right", &RestPredicate(right))?;
                state.end()
            }
            Predicate::Not(expr) => {
                let mut state = serializer.serialize_struct("Predicate", 2)?;
                state.serialize_field("type", "not")?;
                state.serialize_field("child", &RestPredicate(expr.inputs()[0]))?;
                state.end()
            }
            Predicate::Unary(expr) => {
                let mut state = serializer.serialize_struct("Predicate", 2)?;
                state.serialize_field("type", operator(expr.op()).map_err(S::Error::custom)?)?;
                state.serialize_field("term", expr.term().name())?;
                state.end()
            }
            Predicate::Binary(expr) => {
                let mut state = serializer.serialize_struct("Predicate", 3)?;
                state.serialize_field("type", operator(expr.op()).map_err(S::Error::custom)?)?;
                state.serialize_field("term", expr.term().name())?;
                state.serialize_field(
                    "value",
                    &literal(expr.literal()).map_err(S::Error::custom)?,
                )?;
                state.end()
            }
            Predicate::Set(expr) => {
                let mut values = expr
                    .literals()
                    .iter()
                    .map(literal)
                    .collect::<iceberg::Result<Vec<_>>>()
                    .map_err(S::Error::custom)?;
                values.sort_unstable_by_key(Value::to_string);
                let mut state = serializer.serialize_struct("Predicate", 3)?;
                state.serialize_field("type", operator(expr.op()).map_err(S::Error::custom)?)?;
                state.serialize_field("term", expr.term().name())?;
                state.serialize_field("values", &values)?;
                state.end()
            }
        }
    }
}

fn literal(datum: &Datum) -> iceberg::Result<Value> {
    let in_range = match (datum.data_type(), datum.literal()) {
        (PrimitiveType::Date, PrimitiveLiteral::Int(days)) => {
            chrono::TimeDelta::try_days(i64::from(*days))
                .and_then(|delta| {
                    chrono::DateTime::<chrono::Utc>::UNIX_EPOCH.checked_add_signed(delta)
                })
                .is_some()
        }
        (PrimitiveType::Timestamp | PrimitiveType::Timestamptz, PrimitiveLiteral::Long(micros)) => {
            chrono::DateTime::from_timestamp_micros(*micros).is_some()
        }
        _ => true,
    };
    if !in_range {
        return Err(iceberg::Error::new(
            iceberg::ErrorKind::DataInvalid,
            "Temporal scan-report literal is outside the supported date/time range",
        ));
    }
    let value = Literal::Primitive(datum.literal().clone())
        .try_into_json(&Type::Primitive(datum.data_type().clone()))?;
    if value.is_null() {
        return Err(iceberg::Error::new(
            iceberg::ErrorKind::DataInvalid,
            "Non-finite scan-report literals cannot be represented in REST JSON",
        ));
    }
    Ok(value)
}

fn operator(op: PredicateOperator) -> iceberg::Result<&'static str> {
    Ok(match op {
        PredicateOperator::IsNull => "is-null",
        PredicateOperator::NotNull => "not-null",
        PredicateOperator::IsNan => "is-nan",
        PredicateOperator::NotNan => "not-nan",
        PredicateOperator::LessThan => "lt",
        PredicateOperator::LessThanOrEq => "lt-eq",
        PredicateOperator::GreaterThan => "gt",
        PredicateOperator::GreaterThanOrEq => "gt-eq",
        PredicateOperator::Eq => "eq",
        PredicateOperator::NotEq => "not-eq",
        PredicateOperator::StartsWith => "starts-with",
        PredicateOperator::NotStartsWith => "not-starts-with",
        PredicateOperator::In => "in",
        PredicateOperator::NotIn => "not-in",
        _ => {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::FeatureUnsupported,
                "Unsupported scan-report predicate operator",
            ));
        }
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use iceberg::expr::{BinaryExpression, Reference, SetExpression, UnaryExpression};
    use serde_json::json;

    use super::*;

    fn scan_report() -> ReportMetricsRequest {
        ReportMetricsRequest {
            report_type: "scan-report",
            table_name: "analytics.sales.orders".into(),
            snapshot_id: -12345,
            schema_id: 7,
            filter: None,
            projected_field_ids: vec![1, 2, -1],
            projected_field_names: vec!["id".into(), "customer.名".into(), "_file".into()],
            metadata: HashMap::from([("query-id".into(), "query-123".into())]),
            metrics: ScanMetricsResult {
                total_planning_duration: Duration::new(2, 644_235_116),
                total_data_manifests: 1,
                total_delete_manifests: 2,
                skipped_data_manifests: 3,
                skipped_delete_manifests: 4,
                scanned_data_manifests: 5,
                scanned_delete_manifests: 6,
                result_data_files: 7,
                skipped_data_files: 8,
                total_file_size_in_bytes: 9,
                result_delete_files: 10,
                skipped_delete_files: 11,
                total_delete_file_size_in_bytes: 12,
                indexed_delete_files: 13,
                equality_delete_files: 14,
                positional_delete_files: 15,
                dvs: 16,
            },
        }
    }

    #[test]
    fn test_scan_report_rest_request() {
        let expected: Value =
            serde_json::from_str(include_str!("../testdata/metrics/scan-report.json")).unwrap();
        let report = scan_report();
        assert_eq!(serde_json::to_value(report).unwrap(), expected);
    }

    #[test]
    fn test_scan_report_empty_projection_and_metadata() {
        let mut report = scan_report();
        report.projected_field_ids.clear();
        report.projected_field_names.clear();
        report.metadata.clear();
        report.filter = Some(Predicate::AlwaysFalse);
        let mut expected: Value =
            serde_json::from_str(include_str!("../testdata/metrics/scan-report.json")).unwrap();
        expected.as_object_mut().unwrap().remove("metadata");
        expected["projected-field-ids"] = json!([]);
        expected["projected-field-names"] = json!([]);
        expected["filter"] = json!(false);
        assert_eq!(serde_json::to_value(report).unwrap(), expected);
    }

    #[test]
    fn test_scan_report_nested_filter() {
        let mut report = scan_report();
        report.filter = Some(
            Reference::new("a").is_null().and(
                Reference::new("b")
                    .equal_to(Datum::int(10))
                    .or(!Reference::new("c").is_nan()),
            ),
        );
        let mut expected: Value =
            serde_json::from_str(include_str!("../testdata/metrics/scan-report.json")).unwrap();
        expected["filter"] = json!({
            "type": "and",
            "left": {"type": "is-null", "term": "a"},
            "right": {
                "type": "or",
                "left": {"type": "eq", "term": "b", "value": 10},
                "right": {"type": "not", "child": {"type": "is-nan", "term": "c"}}
            }
        });
        assert_eq!(serde_json::to_value(report).unwrap(), expected);
    }

    #[test]
    fn test_predicate_operators() {
        for (op, name) in [
            (PredicateOperator::IsNull, "is-null"),
            (PredicateOperator::NotNull, "not-null"),
            (PredicateOperator::IsNan, "is-nan"),
            (PredicateOperator::NotNan, "not-nan"),
        ] {
            let predicate = Predicate::Unary(UnaryExpression::new(op, Reference::new("a.b")));
            assert_eq!(
                serde_json::to_value(RestPredicate(&predicate)).unwrap(),
                json!({"type": name, "term": "a.b"})
            );
        }
        for (op, name) in [
            (PredicateOperator::LessThan, "lt"),
            (PredicateOperator::LessThanOrEq, "lt-eq"),
            (PredicateOperator::GreaterThan, "gt"),
            (PredicateOperator::GreaterThanOrEq, "gt-eq"),
            (PredicateOperator::Eq, "eq"),
            (PredicateOperator::NotEq, "not-eq"),
            (PredicateOperator::StartsWith, "starts-with"),
            (PredicateOperator::NotStartsWith, "not-starts-with"),
        ] {
            let predicate = Predicate::Binary(BinaryExpression::new(
                op,
                Reference::new("a"),
                Datum::string("x"),
            ));
            assert_eq!(
                serde_json::to_value(RestPredicate(&predicate)).unwrap(),
                json!({"type": name, "term": "a", "value": "x"})
            );
        }
        for (op, name) in [
            (PredicateOperator::In, "in"),
            (PredicateOperator::NotIn, "not-in"),
        ] {
            let predicate = Predicate::Set(SetExpression::new(
                op,
                Reference::new("a"),
                [Datum::string("b"), Datum::string("a")]
                    .into_iter()
                    .collect(),
            ));
            assert_eq!(
                serde_json::to_value(RestPredicate(&predicate)).unwrap(),
                json!({"type": name, "term": "a", "values": ["a", "b"]})
            );
        }
        for (predicate, expected) in [
            (Predicate::AlwaysTrue, json!(true)),
            (Predicate::AlwaysFalse, json!(false)),
        ] {
            assert_eq!(
                serde_json::to_value(RestPredicate(&predicate)).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn test_out_of_range_temporal_filter_literals_return_errors() {
        let epoch = chrono::DateTime::<chrono::Utc>::UNIX_EPOCH.date_naive();
        let min_days = chrono::NaiveDate::MIN
            .signed_duration_since(epoch)
            .num_days() as i32;
        let max_days = chrono::NaiveDate::MAX
            .signed_duration_since(epoch)
            .num_days() as i32;
        let min_micros = chrono::DateTime::<chrono::Utc>::MIN_UTC.timestamp_micros();
        let max_micros = chrono::DateTime::<chrono::Utc>::MAX_UTC.timestamp_micros();
        let mut invalid = vec![
            Datum::date(i32::MIN),
            Datum::date(i32::MAX),
            Datum::date(min_days - 1),
            Datum::date(max_days + 1),
        ];
        for micros in [i64::MIN, i64::MAX, min_micros - 1, max_micros + 1] {
            invalid.extend([
                Datum::timestamp_micros(micros),
                Datum::timestamptz_micros(micros),
            ]);
        }
        for datum in invalid {
            for predicate in [
                Reference::new("value").equal_to(datum.clone()),
                Reference::new("value").is_in([datum.clone()]),
            ] {
                let mut report = scan_report();
                report.filter = Some(predicate);
                assert!(serde_json::to_value(report).is_err());
            }
        }
    }

    #[test]
    fn test_temporal_filter_literal_boundaries() {
        let epoch = chrono::DateTime::<chrono::Utc>::UNIX_EPOCH.date_naive();
        let mut cases = Vec::new();
        for date in [chrono::NaiveDate::MIN, chrono::NaiveDate::MAX] {
            cases.push((
                Datum::date(date.signed_duration_since(epoch).num_days() as i32),
                json!(date.to_string()),
            ));
        }
        for datetime in [
            chrono::DateTime::<chrono::Utc>::MIN_UTC,
            chrono::DateTime::<chrono::Utc>::MAX_UTC,
        ] {
            let micros = datetime.timestamp_micros();
            let datetime = chrono::DateTime::from_timestamp_micros(micros).unwrap();
            cases.push((
                Datum::timestamp_micros(micros),
                json!(datetime.format("%Y-%m-%dT%H:%M:%S%.f").to_string()),
            ));
            cases.push((
                Datum::timestamptz_micros(micros),
                json!(datetime.format("%Y-%m-%dT%H:%M:%S%.f+00:00").to_string()),
            ));
        }
        for nanos in [i64::MIN, i64::MAX] {
            let datetime = chrono::DateTime::from_timestamp_nanos(nanos);
            cases.push((
                Datum::timestamp_nanos(nanos),
                json!(datetime.format("%Y-%m-%dT%H:%M:%S%.f").to_string()),
            ));
            cases.push((
                Datum::timestamptz_nanos(nanos),
                json!(datetime.format("%Y-%m-%dT%H:%M:%S%.f+00:00").to_string()),
            ));
        }
        for (datum, expected) in cases {
            let predicate = Reference::new("value").equal_to(datum);
            assert_eq!(
                serde_json::to_value(RestPredicate(&predicate)).unwrap(),
                json!({"type":"eq", "term":"value", "value":expected})
            );
        }
    }

    #[test]
    fn test_predicate_literal_types() {
        for (datum, expected) in [
            (Datum::bool(true), json!(true)),
            (Datum::int(i32::MIN), json!(i32::MIN)),
            (Datum::long(i64::MAX), json!(i64::MAX)),
            (Datum::float(1.5_f32), json!(1.5)),
            (Datum::double(-2.25), json!(-2.25)),
            (Datum::string("a\"\n名"), json!("a\"\n名")),
            (
                Datum::date_from_str("2026-11-24").unwrap(),
                json!("2026-11-24"),
            ),
            (
                Datum::time_from_str("12:34:56.123456").unwrap(),
                json!("12:34:56.123456"),
            ),
            (
                Datum::timestamp_from_str("2026-11-24T12:34:56.123456").unwrap(),
                json!("2026-11-24T12:34:56.123456"),
            ),
            (
                Datum::timestamptz_from_str("2026-11-24T12:34:56.123456+00:00").unwrap(),
                json!("2026-11-24T12:34:56.123456+00:00"),
            ),
            (
                Datum::timestamp_nanos(1),
                json!("1970-01-01T00:00:00.000000001"),
            ),
            (
                Datum::timestamptz_nanos(1),
                json!("1970-01-01T00:00:00.000000001+00:00"),
            ),
            (
                Datum::uuid_from_str("f79c3e09-677c-4bbd-a479-3f349cb785e7").unwrap(),
                json!("f79c3e09-677c-4bbd-a479-3f349cb785e7"),
            ),
            (Datum::fixed([0, 255]), json!("00ff")),
            (Datum::binary([0, 255]), json!("00ff")),
            (Datum::decimal_from_str("-12.34").unwrap(), json!("-12.34")),
        ] {
            let predicate = Reference::new("value").equal_to(datum);
            assert_eq!(
                serde_json::to_value(RestPredicate(&predicate)).unwrap(),
                json!({"type": "eq", "term": "value", "value": expected})
            );
        }
    }

    #[test]
    fn test_metrics_int64_boundaries() {
        let mut report = scan_report();
        report.metrics.result_data_files = i64::MAX as u64;
        report.metrics.total_file_size_in_bytes = i64::MAX as u64;
        report.metrics.total_planning_duration = Duration::from_nanos(i64::MAX as u64);
        let mut expected: Value =
            serde_json::from_str(include_str!("../testdata/metrics/scan-report.json")).unwrap();
        expected["metrics"]["result-data-files"]["value"] = json!(i64::MAX);
        expected["metrics"]["total-file-size-in-bytes"]["value"] = json!(i64::MAX);
        expected["metrics"]["total-planning-duration"]["total-duration"] = json!(i64::MAX);
        assert_eq!(serde_json::to_value(report).unwrap(), expected);

        let mut report = scan_report();
        report.metrics.result_data_files = i64::MAX as u64 + 1;
        assert!(serde_json::to_value(report).is_err());
        let mut report = scan_report();
        report.metrics.total_file_size_in_bytes = u64::MAX;
        assert!(serde_json::to_value(report).is_err());
        let mut report = scan_report();
        report.metrics.total_planning_duration = Duration::from_nanos(i64::MAX as u64 + 1);
        assert!(serde_json::to_value(report).is_err());
        let mut report = scan_report();
        report.metrics.total_planning_duration = Duration::ZERO;
        report.metrics.result_data_files = 0;
        let mut expected: Value =
            serde_json::from_str(include_str!("../testdata/metrics/scan-report.json")).unwrap();
        expected["metrics"]["result-data-files"]["value"] = json!(0);
        expected["metrics"]["total-planning-duration"]["total-duration"] = json!(0);
        assert_eq!(serde_json::to_value(report).unwrap(), expected);
    }

    #[test]
    fn test_non_finite_filter_literals_fail_serialization() {
        for datum in [
            Datum::float(f32::NAN),
            Datum::double(f64::INFINITY),
            Datum::double(f64::NEG_INFINITY),
        ] {
            let mut report = scan_report();
            report.filter = Some(Reference::new("value").equal_to(datum));
            assert!(serde_json::to_value(report).is_err());
        }
        let mut report = scan_report();
        report.filter = Some(Reference::new("value").is_in([Datum::double(f64::NAN)]));
        assert!(serde_json::to_value(report).is_err());
    }
}
