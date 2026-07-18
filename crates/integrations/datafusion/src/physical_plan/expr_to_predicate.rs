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

use std::vec;

use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{BinaryExpr, Expr, Like, Operator, TableProviderFilterPushDown};
use datafusion::scalar::ScalarValue;
use iceberg::expr::{BinaryExpression, Predicate, PredicateOperator, Reference, UnaryExpression};
use iceberg::spec::{Datum, PrimitiveLiteral};

// A DataFusion expression could be an Iceberg predicate, column, or literal.
enum TransformedValue {
    Predicate(Predicate),
    Column(Reference),
    Literal(Datum),
    NotTransformed,
}

struct TransformedResult {
    value: TransformedValue,
    exact: bool,
}

impl TransformedResult {
    fn predicate(predicate: Predicate, exact: bool) -> Self {
        Self {
            value: TransformedValue::Predicate(predicate),
            exact,
        }
    }

    fn column(column: Reference) -> Self {
        Self {
            value: TransformedValue::Column(column),
            exact: true,
        }
    }

    fn literal(literal: Datum) -> Self {
        Self {
            value: TransformedValue::Literal(literal),
            exact: true,
        }
    }

    fn not_transformed() -> Self {
        Self {
            value: TransformedValue::NotTransformed,
            exact: false,
        }
    }

    fn mark_inexact(mut self) -> Self {
        self.exact = false;
        self
    }
}

enum OpTransformedResult {
    Operator(PredicateOperator),
    And,
    Or,
    NotTransformed,
}

/// Converts DataFusion filters ([`Expr`]) to an iceberg [`Predicate`].
/// If none of the filters could be converted, return `None` which adds no predicates to the scan operation.
/// If the conversion was successful, return the converted predicates combined with an AND operator.
pub fn convert_filters_to_predicate(filters: &[Expr]) -> Option<Predicate> {
    filters
        .iter()
        .filter_map(|filter| convert_filter_to_predicate(filter).map(|(predicate, _)| predicate))
        .reduce(Predicate::and)
}

fn convert_filter_to_predicate(expr: &Expr) -> Option<(Predicate, bool)> {
    let transformed = to_iceberg_predicate(expr);
    match transformed.value {
        TransformedValue::Predicate(predicate) => Some((predicate, transformed.exact)),
        TransformedValue::Column(column) => {
            // A bare column in a filter context represents a boolean column check
            // Convert it to: column = true
            Some((
                Predicate::Binary(BinaryExpression::new(
                    PredicateOperator::Eq,
                    column,
                    Datum::bool(true),
                )),
                transformed.exact,
            ))
        }
        TransformedValue::Literal(_) => {
            // Literal values in filter context cannot be pushed down
            None
        }
        _ => None,
    }
}

pub(crate) fn classify_filter_pushdown(expr: &Expr) -> TableProviderFilterPushDown {
    match convert_filter_to_predicate(expr) {
        Some((_, true)) => TableProviderFilterPushDown::Exact,
        Some((_, false)) => TableProviderFilterPushDown::Inexact,
        None => TableProviderFilterPushDown::Unsupported,
    }
}

fn to_iceberg_predicate(expr: &Expr) -> TransformedResult {
    match expr {
        Expr::BinaryExpr(binary) => {
            let left = to_iceberg_predicate(&binary.left);
            let right = to_iceberg_predicate(&binary.right);
            let op = to_iceberg_operation(binary.op);
            match op {
                OpTransformedResult::Operator(op) => to_iceberg_binary_predicate(left, right, op),
                OpTransformedResult::And => to_iceberg_and_predicate(left, right),
                OpTransformedResult::Or => to_iceberg_or_predicate(left, right),
                OpTransformedResult::NotTransformed => TransformedResult::not_transformed(),
            }
        }
        Expr::Not(exp) => {
            let expr = to_iceberg_predicate(exp);
            match expr.value {
                TransformedValue::Predicate(p) if expr.exact => TransformedResult::predicate(
                    !p,
                    !matches!(
                        exp.as_ref(),
                        Expr::ScalarFunction(ScalarFunction { func, .. }) if func.name() == "isnan"
                    ),
                ),
                TransformedValue::Column(column) => {
                    // NOT of a bare boolean column: NOT col => col = false
                    TransformedResult::predicate(
                        Predicate::Binary(BinaryExpression::new(
                            PredicateOperator::Eq,
                            column,
                            Datum::bool(false),
                        )),
                        expr.exact,
                    )
                }
                // Negating an inexact predicate can turn a safe superset into
                // a subset and incorrectly prune matching rows.
                _ => TransformedResult::not_transformed(),
            }
        }
        Expr::Column(column) => TransformedResult::column(Reference::new(column.name())),
        Expr::Literal(literal, _) => match scalar_value_to_datum(literal) {
            Some(data) => TransformedResult::literal(data),
            None => TransformedResult::not_transformed(),
        },
        Expr::InList(inlist) => {
            let mut datums = vec![];
            let mut exact = true;
            for expr in &inlist.list {
                let p = to_iceberg_predicate(expr);
                exact &= p.exact;
                match p.value {
                    TransformedValue::Literal(l) => datums.push(l),
                    _ => return TransformedResult::not_transformed(),
                }
            }

            let expr = to_iceberg_predicate(&inlist.expr);
            exact &= expr.exact;
            match expr.value {
                TransformedValue::Column(r) => match inlist.negated {
                    false => TransformedResult::predicate(r.is_in(datums), exact),
                    // The Arrow reader conservatively matches NOT IN when the
                    // column is absent from an older data file.
                    true => TransformedResult::predicate(r.is_not_in(datums), false),
                },
                _ => TransformedResult::not_transformed(),
            }
        }
        Expr::IsNull(expr) => {
            let p = to_iceberg_predicate(expr);
            match p.value {
                TransformedValue::Column(r) => TransformedResult::predicate(
                    Predicate::Unary(UnaryExpression::new(PredicateOperator::IsNull, r)),
                    p.exact,
                ),
                _ => TransformedResult::not_transformed(),
            }
        }
        Expr::IsNotNull(expr) => {
            let p = to_iceberg_predicate(expr);
            match p.value {
                TransformedValue::Column(r) => TransformedResult::predicate(
                    Predicate::Unary(UnaryExpression::new(PredicateOperator::NotNull, r)),
                    p.exact,
                ),
                _ => TransformedResult::not_transformed(),
            }
        }
        Expr::Cast(c) => {
            if *c.field.data_type() == DataType::Date32 || *c.field.data_type() == DataType::Date64
            {
                // Casts to date truncate the expression, we cannot simply extract it as it
                // can create erroneous predicates.
                return TransformedResult::not_transformed();
            }
            // Keep using the cast-stripped predicate for pruning, but retain the
            // original DataFusion filter because this layer cannot prove the cast
            // preserves comparison semantics.
            to_iceberg_predicate(&c.expr).mark_inexact()
        }
        Expr::Like(Like {
            negated,
            expr,
            pattern,
            escape_char,
            case_insensitive,
        }) => {
            // Only support simple prefix patterns (e.g., 'prefix%')
            // Note: Iceberg's StartsWith operator is case-sensitive, so we cannot
            // push down case-insensitive LIKE (ILIKE) patterns
            // Escape characters are also not supported for pushdown
            if escape_char.is_some() || *case_insensitive {
                return TransformedResult::not_transformed();
            }

            // Extract the pattern string
            let pattern_str = match to_iceberg_predicate(pattern) {
                TransformedResult {
                    value: TransformedValue::Literal(d),
                    ..
                } => match d.literal() {
                    PrimitiveLiteral::String(s) => s.clone(),
                    _ => return TransformedResult::not_transformed(),
                },
                _ => return TransformedResult::not_transformed(),
            };

            // Check if it's a simple prefix pattern (ends with % and no other wildcards)
            if pattern_str.ends_with('%')
                && !pattern_str[..pattern_str.len() - 1].contains(['%', '_'])
            {
                // Extract the prefix (remove trailing %)
                let prefix = pattern_str[..pattern_str.len() - 1].to_string();

                // Get the column reference
                let column = match to_iceberg_predicate(expr) {
                    TransformedResult {
                        value: TransformedValue::Column(r),
                        ..
                    } => r,
                    _ => return TransformedResult::not_transformed(),
                };

                // Create the appropriate predicate
                let predicate = if *negated {
                    column.not_starts_with(Datum::string(prefix))
                } else {
                    column.starts_with(Datum::string(prefix))
                };

                // The Arrow reader conservatively matches NOT STARTS WITH when
                // the column is absent from an older data file.
                TransformedResult::predicate(predicate, !*negated)
            } else {
                // Complex LIKE patterns cannot be pushed down
                TransformedResult::not_transformed()
            }
        }
        Expr::ScalarFunction(ScalarFunction { func, args }) => {
            scalar_function_to_iceberg_predicate(func.name(), args)
        }
        _ => TransformedResult::not_transformed(),
    }
}

fn to_iceberg_operation(op: Operator) -> OpTransformedResult {
    match op {
        Operator::Eq => OpTransformedResult::Operator(PredicateOperator::Eq),
        Operator::NotEq => OpTransformedResult::Operator(PredicateOperator::NotEq),
        Operator::Lt => OpTransformedResult::Operator(PredicateOperator::LessThan),
        Operator::LtEq => OpTransformedResult::Operator(PredicateOperator::LessThanOrEq),
        Operator::Gt => OpTransformedResult::Operator(PredicateOperator::GreaterThan),
        Operator::GtEq => OpTransformedResult::Operator(PredicateOperator::GreaterThanOrEq),
        // AND OR
        Operator::And => OpTransformedResult::And,
        Operator::Or => OpTransformedResult::Or,
        // Others not supported
        _ => OpTransformedResult::NotTransformed,
    }
}

/// Translates a DataFusion scalar function into an Iceberg predicate.
/// Unlike dedicated Expr variants (e.g. `Expr::IsNull`), scalar functions are
/// identified by name at runtime, so we need to handle them here.
fn scalar_function_to_iceberg_predicate(func_name: &str, args: &[Expr]) -> TransformedResult {
    match func_name {
        "isnan" if args.len() == 1 => match resolve_nan_preserving_reference(&args[0]) {
            Some(r) => TransformedResult::predicate(r.is_nan(), true),
            None => TransformedResult::not_transformed(),
        },
        _ => TransformedResult::not_transformed(),
    }
}

/// Attempts to resolve a numeric expression argument down to a single column
/// [`Reference`] such that `isnan(arg)` is logically equivalent to
/// `isnan(reference)`.
///
/// Positive `isnan` filters produced here may be reported as `Exact` by
/// [`IcebergTableProvider::supports_filters_pushdown`], so every transformation
/// handled here must preserve NaN-ness in both directions: the result is NaN if
/// and only if the wrapped column is NaN. Negated `isnan` remains `Inexact`
/// because DataFusion and the Iceberg Arrow predicate differ for null values.
///
/// * negation: `-x` is NaN iff `x` is NaN
/// * `abs(x)`: `abs(x)` is NaN iff `x` is NaN
/// * casts between numeric types preserve NaN
/// * `x + c`, `c + x`, `x - c`, `c - x` for a finite literal `c`
/// * `x * c`, `c * x`, `x / c` for a finite, non-zero literal `c`
///
/// Multiplication/division by zero and `c / x` are intentionally rejected: e.g.
/// `x * 0` is NaN when `x` is `±inf`, so it does not imply `x` is NaN.
///
/// [`IcebergTableProvider::supports_filters_pushdown`]: crate::table::IcebergTableProvider
fn resolve_nan_preserving_reference(expr: &Expr) -> Option<Reference> {
    match expr {
        Expr::Column(column) => Some(Reference::new(column.name())),
        Expr::Negative(inner) => resolve_nan_preserving_reference(inner),
        Expr::Cast(cast) => {
            // Casts to date truncate the value and are not numeric, so they
            // cannot be treated as NaN-preserving.
            if *cast.field.data_type() == DataType::Date32
                || *cast.field.data_type() == DataType::Date64
            {
                return None;
            }
            resolve_nan_preserving_reference(&cast.expr)
        }
        Expr::ScalarFunction(ScalarFunction { func, args })
            if func.name() == "abs" && args.len() == 1 =>
        {
            resolve_nan_preserving_reference(&args[0])
        }
        Expr::BinaryExpr(binary) => resolve_nan_preserving_binary(binary),
        _ => None,
    }
}

/// Resolves the column reference from an arithmetic expression that combines a
/// single column with a finite literal while preserving NaN-ness. See
/// [`resolve_nan_preserving_reference`] for the soundness argument.
///
/// Expressions with column references on both sides (e.g. `(x + 1) * (x - 2)`)
/// are not supported. Handling them safely would require both operands to
/// resolve to the *same* column (`x + y` cannot be expressed as a single
/// `col IS NAN`) and the operator combination itself to be NaN-preserving:
/// `(x + 1) * (x - 2)` is NaN iff `x` is NaN, but `(x + 1) - (x - 2)` is NaN
/// for `x = inf` (`inf - inf`) even though `x` is not.
///
/// TODO: support NaN-preserving expressions with column references on both
/// sides, see <https://github.com/apache/iceberg-rust/issues/2154>.
fn resolve_nan_preserving_binary(binary: &BinaryExpr) -> Option<Reference> {
    let (left, right) = (&binary.left, &binary.right);
    match binary.op {
        // `x + c`, `c + x`, `x - c` and `c - x` are NaN iff `x` is NaN, for any
        // finite literal `c`. The column may be on either side.
        Operator::Plus | Operator::Minus => {
            if finite_literal(right).is_some() {
                resolve_nan_preserving_reference(left)
            } else if finite_literal(left).is_some() {
                resolve_nan_preserving_reference(right)
            } else {
                None
            }
        }

        // `x * c` and `c * x` are NaN iff `x` is NaN, but only when `c` is
        // non-zero. Per IEEE-754:
        //   - inf is not NaN
        //   - inf * 0 is NaN
        // so multiplying by zero is rejected. The column may be on either side.
        Operator::Multiply => {
            if matches!(finite_literal(right), Some(c) if c != 0.0) {
                resolve_nan_preserving_reference(left)
            } else if matches!(finite_literal(left), Some(c) if c != 0.0) {
                resolve_nan_preserving_reference(right)
            } else {
                None
            }
        }

        // `x / c` is NaN iff `x` is NaN, for a finite non-zero literal `c`.
        // `c / x` is rejected and the column must be the dividend (left side).
        // Per IEEE-754:
        //   - 0 is not NaN
        //   - 0 / 0 is NaN
        // so `c / x` is not NaN-preserving.
        Operator::Divide => {
            if matches!(finite_literal(right), Some(c) if c != 0.0) {
                resolve_nan_preserving_reference(left)
            } else {
                None
            }
        }

        _ => None,
    }
}

/// Returns the value of `expr` as an `f64` if it is a finite numeric literal
/// (i.e. not a non-literal, non-numeric, or infinite/NaN value). The numeric
/// conversion is delegated to DataFusion's [`ScalarValue::cast_to`]; the value
/// is only used to inspect finiteness and sign (precision loss is irrelevant).
fn finite_literal(expr: &Expr) -> Option<f64> {
    let Expr::Literal(value, _) = expr else {
        return None;
    };
    match value.cast_to(&DataType::Float64).ok()? {
        ScalarValue::Float64(Some(v)) if v.is_finite() => Some(v),
        _ => None,
    }
}

fn to_iceberg_and_predicate(
    left: TransformedResult,
    right: TransformedResult,
) -> TransformedResult {
    let exact = left.exact && right.exact;
    match (left.value, right.value) {
        (TransformedValue::Predicate(left), TransformedValue::Predicate(right)) => {
            TransformedResult::predicate(left.and(right), exact)
        }
        (TransformedValue::Predicate(left), _) => TransformedResult::predicate(left, false),
        (_, TransformedValue::Predicate(right)) => TransformedResult::predicate(right, false),
        _ => TransformedResult::not_transformed(),
    }
}

fn to_iceberg_or_predicate(left: TransformedResult, right: TransformedResult) -> TransformedResult {
    let exact = left.exact && right.exact;
    match (left.value, right.value) {
        (TransformedValue::Predicate(left), TransformedValue::Predicate(right)) => {
            TransformedResult::predicate(left.or(right), exact)
        }
        _ => TransformedResult::not_transformed(),
    }
}

fn to_iceberg_binary_predicate(
    left: TransformedResult,
    right: TransformedResult,
    op: PredicateOperator,
) -> TransformedResult {
    let exact = left.exact && right.exact;
    let (r, d, op) = match (left.value, right.value) {
        (TransformedValue::NotTransformed, _) => {
            return TransformedResult::not_transformed();
        }
        (_, TransformedValue::NotTransformed) => {
            return TransformedResult::not_transformed();
        }
        (TransformedValue::Column(r), TransformedValue::Literal(d)) => (r, d, op),
        (TransformedValue::Literal(d), TransformedValue::Column(r)) => {
            (r, d, reverse_predicate_operator(op))
        }
        _ => return TransformedResult::not_transformed(),
    };
    // For schema-evolved files that do not contain the referenced column, the
    // Arrow reader conservatively matches < and <= predicates. They remain safe
    // pruning predicates, but DataFusion must apply the original filter again.
    let exact = exact
        && !matches!(
            op,
            PredicateOperator::LessThan | PredicateOperator::LessThanOrEq
        );
    TransformedResult::predicate(Predicate::Binary(BinaryExpression::new(op, r, d)), exact)
}

fn reverse_predicate_operator(op: PredicateOperator) -> PredicateOperator {
    match op {
        PredicateOperator::Eq => PredicateOperator::Eq,
        PredicateOperator::NotEq => PredicateOperator::NotEq,
        PredicateOperator::GreaterThan => PredicateOperator::LessThan,
        PredicateOperator::GreaterThanOrEq => PredicateOperator::LessThanOrEq,
        PredicateOperator::LessThan => PredicateOperator::GreaterThan,
        PredicateOperator::LessThanOrEq => PredicateOperator::GreaterThanOrEq,
        _ => unreachable!("Reverse {}", op),
    }
}

const MILLIS_PER_DAY: i64 = 24 * 60 * 60 * 1000;

/// Convert a scalar value to an iceberg datum.
fn scalar_value_to_datum(value: &ScalarValue) -> Option<Datum> {
    match value {
        ScalarValue::Boolean(Some(v)) => Some(Datum::bool(*v)),
        ScalarValue::Int8(Some(v)) => Some(Datum::int(*v as i32)),
        ScalarValue::Int16(Some(v)) => Some(Datum::int(*v as i32)),
        ScalarValue::Int32(Some(v)) => Some(Datum::int(*v)),
        ScalarValue::Int64(Some(v)) => Some(Datum::long(*v)),
        ScalarValue::Float32(Some(v)) => Some(Datum::double(*v as f64)),
        ScalarValue::Float64(Some(v)) => Some(Datum::double(*v)),
        ScalarValue::Utf8(Some(v)) => Some(Datum::string(v.clone())),
        ScalarValue::LargeUtf8(Some(v)) => Some(Datum::string(v.clone())),
        ScalarValue::Binary(Some(v)) => Some(Datum::binary(v.clone())),
        ScalarValue::LargeBinary(Some(v)) => Some(Datum::binary(v.clone())),
        ScalarValue::Date32(Some(v)) => Some(Datum::date(*v)),
        ScalarValue::Date64(Some(v)) => Some(Datum::date((*v / MILLIS_PER_DAY) as i32)),
        // Timestamp conversions
        // Note: TimestampSecond and TimestampMillisecond are not handled here because
        // DataFusion's type coercion always converts them to match the column type
        // (either TimestampMicrosecond or TimestampNanosecond) before predicate pushdown.
        // See unit tests for how those conversions would work if needed.
        ScalarValue::TimestampMicrosecond(Some(v), _) => Some(Datum::timestamp_micros(*v)),
        ScalarValue::TimestampNanosecond(Some(v), _) => Some(Datum::timestamp_nanos(*v)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::common::DFSchema;
    use datafusion::logical_expr::TableProviderFilterPushDown;
    use datafusion::logical_expr::utils::split_conjunction;
    use datafusion::prelude::{Expr, SessionContext};
    use iceberg::expr::{Predicate, Reference};
    use iceberg::spec::Datum;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    use super::{classify_filter_pushdown, convert_filters_to_predicate};

    fn create_test_schema() -> DFSchema {
        let arrow_schema = Schema::new(vec![
            Field::new("foo", DataType::Int32, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            Field::new("bar", DataType::Utf8, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
            Field::new("ts", DataType::Timestamp(TimeUnit::Second, None), true).with_metadata(
                HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "3".to_string())]),
            ),
            Field::new("qux", DataType::Float64, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "4".to_string(),
            )])),
        ]);
        DFSchema::try_from_qualified_schema("my_table", &arrow_schema).unwrap()
    }

    fn convert_to_iceberg_predicate(sql: &str) -> Option<Predicate> {
        let df_schema = create_test_schema();
        let expr = SessionContext::new()
            .parse_sql_expr(sql, &df_schema)
            .unwrap();
        let exprs: Vec<Expr> = split_conjunction(&expr).into_iter().cloned().collect();
        convert_filters_to_predicate(&exprs[..])
    }

    fn classify_sql_filter(sql: &str) -> TableProviderFilterPushDown {
        let df_schema = create_test_schema();
        let expr = SessionContext::new()
            .parse_sql_expr(sql, &df_schema)
            .unwrap();
        classify_filter_pushdown(&expr)
    }

    #[test]
    fn test_filter_pushdown_classification() {
        assert_eq!(
            classify_sql_filter("foo > 1 AND bar = 'test'"),
            TableProviderFilterPushDown::Exact
        );
        assert_eq!(
            classify_sql_filter("foo > 1 AND length(bar) = 1"),
            TableProviderFilterPushDown::Inexact
        );
        assert_eq!(
            classify_sql_filter("length(bar) = 1"),
            TableProviderFilterPushDown::Unsupported
        );
        assert_eq!(
            classify_sql_filter("foo < 1"),
            TableProviderFilterPushDown::Inexact
        );
        assert_eq!(
            classify_sql_filter("bar NOT IN ('test')"),
            TableProviderFilterPushDown::Inexact
        );
        assert_eq!(
            classify_sql_filter("bar NOT LIKE 'test%'"),
            TableProviderFilterPushDown::Inexact
        );
    }

    #[test]
    fn test_filter_pushdown_with_cast_is_inexact() {
        assert_eq!(
            classify_sql_filter("ts >= timestamp '2023-01-05T00:00:00'"),
            TableProviderFilterPushDown::Inexact
        );
    }

    #[test]
    fn test_negated_partial_filter_is_unsupported() {
        assert_eq!(
            classify_sql_filter("NOT (foo > 1 AND length(bar) = 1)"),
            TableProviderFilterPushDown::Unsupported
        );
    }

    #[test]
    fn test_negated_isnan_filter_is_inexact() {
        assert_eq!(
            classify_sql_filter("isnan(qux)"),
            TableProviderFilterPushDown::Exact
        );
        assert_eq!(
            classify_sql_filter("NOT isnan(qux)"),
            TableProviderFilterPushDown::Inexact
        );
    }

    #[test]
    fn test_predicate_conversion_with_single_condition() {
        let predicate = convert_to_iceberg_predicate("foo = 1").unwrap();
        assert_eq!(predicate, Reference::new("foo").equal_to(Datum::long(1)));

        let predicate = convert_to_iceberg_predicate("foo != 1").unwrap();
        assert_eq!(
            predicate,
            Reference::new("foo").not_equal_to(Datum::long(1))
        );

        let predicate = convert_to_iceberg_predicate("foo > 1").unwrap();
        assert_eq!(
            predicate,
            Reference::new("foo").greater_than(Datum::long(1))
        );

        let predicate = convert_to_iceberg_predicate("foo >= 1").unwrap();
        assert_eq!(
            predicate,
            Reference::new("foo").greater_than_or_equal_to(Datum::long(1))
        );

        let predicate = convert_to_iceberg_predicate("foo < 1").unwrap();
        assert_eq!(predicate, Reference::new("foo").less_than(Datum::long(1)));

        let predicate = convert_to_iceberg_predicate("foo <= 1").unwrap();
        assert_eq!(
            predicate,
            Reference::new("foo").less_than_or_equal_to(Datum::long(1))
        );

        let predicate = convert_to_iceberg_predicate("foo is null").unwrap();
        assert_eq!(predicate, Reference::new("foo").is_null());

        let predicate = convert_to_iceberg_predicate("foo is not null").unwrap();
        assert_eq!(predicate, Reference::new("foo").is_not_null());

        let predicate = convert_to_iceberg_predicate("foo in (5, 6)").unwrap();
        assert_eq!(
            predicate,
            Reference::new("foo").is_in([Datum::long(5), Datum::long(6)])
        );

        let predicate = convert_to_iceberg_predicate("foo not in (5, 6)").unwrap();
        assert_eq!(
            predicate,
            Reference::new("foo").is_not_in([Datum::long(5), Datum::long(6)])
        );

        let predicate = convert_to_iceberg_predicate("not foo = 1").unwrap();
        assert_eq!(predicate, !Reference::new("foo").equal_to(Datum::long(1)));
    }

    #[test]
    fn test_predicate_conversion_with_single_unsupported_condition() {
        let predicate = convert_to_iceberg_predicate("foo + 1 = 1");
        assert_eq!(predicate, None);

        let predicate = convert_to_iceberg_predicate("length(bar) = 1");
        assert_eq!(predicate, None);

        let predicate = convert_to_iceberg_predicate("foo in (1, 2, foo)");
        assert_eq!(predicate, None);
    }

    #[test]
    fn test_predicate_conversion_with_single_condition_rev() {
        let predicate = convert_to_iceberg_predicate("1 < foo").unwrap();
        assert_eq!(
            predicate,
            Reference::new("foo").greater_than(Datum::long(1))
        );
    }

    #[test]
    fn test_predicate_conversion_with_and_condition() {
        let sql = "foo > 1 and bar = 'test'";
        let predicate = convert_to_iceberg_predicate(sql).unwrap();
        let expected_predicate = Predicate::and(
            Reference::new("foo").greater_than(Datum::long(1)),
            Reference::new("bar").equal_to(Datum::string("test")),
        );
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_and_condition_unsupported() {
        let sql = "foo > 1 and length(bar) = 1";
        let predicate = convert_to_iceberg_predicate(sql).unwrap();
        let expected_predicate = Reference::new("foo").greater_than(Datum::long(1));
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_and_condition_both_unsupported() {
        let sql = "foo in (1, 2, foo) and length(bar) = 1";
        let predicate = convert_to_iceberg_predicate(sql);
        assert_eq!(predicate, None);
    }

    #[test]
    fn test_predicate_conversion_with_or_condition_unsupported() {
        let sql = "foo > 1 or length(bar) = 1";
        let predicate = convert_to_iceberg_predicate(sql);
        assert_eq!(predicate, None);
    }

    #[test]
    fn test_predicate_conversion_with_or_condition_supported() {
        let sql = "foo > 1 or bar = 'test'";
        let predicate = convert_to_iceberg_predicate(sql).unwrap();
        let expected_predicate = Predicate::or(
            Reference::new("foo").greater_than(Datum::long(1)),
            Reference::new("bar").equal_to(Datum::string("test")),
        );
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_complex_binary_expr() {
        let sql = "(foo > 1 and bar = 'test') or foo < 0 ";
        let predicate = convert_to_iceberg_predicate(sql).unwrap();

        let inner_predicate = Predicate::and(
            Reference::new("foo").greater_than(Datum::long(1)),
            Reference::new("bar").equal_to(Datum::string("test")),
        );
        let expected_predicate = Predicate::or(
            inner_predicate,
            Reference::new("foo").less_than(Datum::long(0)),
        );
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_one_and_expr_supported() {
        let sql = "(foo > 1 and length(bar) = 1 ) or foo < 0 ";
        let predicate = convert_to_iceberg_predicate(sql).unwrap();

        let inner_predicate = Reference::new("foo").greater_than(Datum::long(1));
        let expected_predicate = Predicate::or(
            inner_predicate,
            Reference::new("foo").less_than(Datum::long(0)),
        );
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_complex_binary_expr_unsupported() {
        let sql = "(foo > 1 or length(bar) = 1 ) and foo < 0 ";
        let predicate = convert_to_iceberg_predicate(sql).unwrap();
        let expected_predicate = Reference::new("foo").less_than(Datum::long(0));
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_cast() {
        let sql = "ts >= timestamp '2023-01-05T00:00:00'";
        let predicate = convert_to_iceberg_predicate(sql).unwrap();
        let expected_predicate =
            Reference::new("ts").greater_than_or_equal_to(Datum::string("2023-01-05T00:00:00"));
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_date_cast() {
        let sql = "ts >= date '2023-01-05T11:00:00'";
        let predicate = convert_to_iceberg_predicate(sql);
        assert_eq!(predicate, None);
    }

    #[test]
    fn test_scalar_value_to_datum_timestamp() {
        use datafusion::common::ScalarValue;

        // Test TimestampMicrosecond - maps directly to Datum::timestamp_micros
        let ts_micros = 1672876800000000i64; // 2023-01-05 00:00:00 UTC in microseconds
        let datum =
            super::scalar_value_to_datum(&ScalarValue::TimestampMicrosecond(Some(ts_micros), None));
        assert_eq!(datum, Some(Datum::timestamp_micros(ts_micros)));

        // Test TimestampNanosecond - maps to Datum::timestamp_nanos to preserve precision
        let ts_nanos = 1672876800000000500i64; // 2023-01-05 00:00:00.000000500 UTC in nanoseconds
        let datum =
            super::scalar_value_to_datum(&ScalarValue::TimestampNanosecond(Some(ts_nanos), None));
        assert_eq!(datum, Some(Datum::timestamp_nanos(ts_nanos)));

        // Test None timestamp
        let datum = super::scalar_value_to_datum(&ScalarValue::TimestampMicrosecond(None, None));
        assert_eq!(datum, None);

        // Note: TimestampSecond and TimestampMillisecond are not supported because
        // DataFusion's type coercion converts them to TimestampMicrosecond or TimestampNanosecond
        // before they reach scalar_value_to_datum in SQL queries.
        //
        // These return None (not pushed down):
        let ts_seconds = 1672876800i64; // 2023-01-05 00:00:00 UTC in seconds
        let datum =
            super::scalar_value_to_datum(&ScalarValue::TimestampSecond(Some(ts_seconds), None));
        assert_eq!(datum, None);

        let ts_millis = 1672876800000i64; // 2023-01-05 00:00:00 UTC in milliseconds
        let datum =
            super::scalar_value_to_datum(&ScalarValue::TimestampMillisecond(Some(ts_millis), None));
        assert_eq!(datum, None);
    }

    #[test]
    fn test_scalar_value_to_datum_binary() {
        use datafusion::common::ScalarValue;

        let bytes = vec![1u8, 2u8, 3u8];
        let datum = super::scalar_value_to_datum(&ScalarValue::Binary(Some(bytes.clone())));
        assert_eq!(datum, Some(Datum::binary(bytes.clone())));

        let datum = super::scalar_value_to_datum(&ScalarValue::LargeBinary(Some(bytes.clone())));
        assert_eq!(datum, Some(Datum::binary(bytes)));

        let datum = super::scalar_value_to_datum(&ScalarValue::Binary(None));
        assert_eq!(datum, None);
    }

    #[test]
    fn test_predicate_conversion_with_binary() {
        let sql = "foo = 1 and bar = X'0102'";
        let predicate = convert_to_iceberg_predicate(sql).unwrap();
        // Binary literals are converted to Datum::binary
        // Note: SQL literal 1 is converted to Long by DataFusion
        let expected_predicate = Reference::new("foo")
            .equal_to(Datum::long(1))
            .and(Reference::new("bar").equal_to(Datum::binary(vec![1u8, 2u8])));
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_scalar_value_to_datum_boolean() {
        use datafusion::common::ScalarValue;

        // Test boolean true
        let datum = super::scalar_value_to_datum(&ScalarValue::Boolean(Some(true)));
        assert_eq!(datum, Some(Datum::bool(true)));

        // Test boolean false
        let datum = super::scalar_value_to_datum(&ScalarValue::Boolean(Some(false)));
        assert_eq!(datum, Some(Datum::bool(false)));

        // Test None boolean
        let datum = super::scalar_value_to_datum(&ScalarValue::Boolean(None));
        assert_eq!(datum, None);
    }

    #[test]
    fn test_predicate_conversion_with_like_starts_with() {
        let sql = "bar LIKE 'test%'";
        let predicate = convert_to_iceberg_predicate(sql).unwrap();
        assert_eq!(
            predicate,
            Reference::new("bar").starts_with(Datum::string("test"))
        );
    }

    #[test]
    fn test_predicate_conversion_with_not_like_starts_with() {
        let sql = "bar NOT LIKE 'test%'";
        let predicate = convert_to_iceberg_predicate(sql).unwrap();
        assert_eq!(
            predicate,
            Reference::new("bar").not_starts_with(Datum::string("test"))
        );
    }

    #[test]
    fn test_predicate_conversion_with_like_empty_prefix() {
        let sql = "bar LIKE '%'";
        let predicate = convert_to_iceberg_predicate(sql).unwrap();
        assert_eq!(
            predicate,
            Reference::new("bar").starts_with(Datum::string(""))
        );
    }

    #[test]
    fn test_predicate_conversion_with_like_complex_pattern() {
        // Patterns with wildcards in the middle cannot be pushed down
        let sql = "bar LIKE 'te%st'";
        let predicate = convert_to_iceberg_predicate(sql);
        assert_eq!(predicate, None);
    }

    #[test]
    fn test_predicate_conversion_with_like_underscore_wildcard() {
        // Patterns with underscore wildcard cannot be pushed down
        let sql = "bar LIKE 'test_'";
        let predicate = convert_to_iceberg_predicate(sql);
        assert_eq!(predicate, None);
    }

    #[test]
    fn test_predicate_conversion_with_like_no_wildcard() {
        // Patterns without trailing % cannot be pushed down as StartsWith
        let sql = "bar LIKE 'test'";
        let predicate = convert_to_iceberg_predicate(sql);
        assert_eq!(predicate, None);
    }

    #[test]
    fn test_predicate_conversion_with_ilike() {
        // Case-insensitive LIKE (ILIKE) is not supported
        let sql = "bar ILIKE 'test%'";
        let predicate = convert_to_iceberg_predicate(sql);
        assert_eq!(predicate, None);
    }

    #[test]
    fn test_predicate_conversion_with_like_and_other_conditions() {
        let sql = "bar LIKE 'test%' AND foo > 1";
        let predicate = convert_to_iceberg_predicate(sql).unwrap();
        let expected_predicate = Predicate::and(
            Reference::new("bar").starts_with(Datum::string("test")),
            Reference::new("foo").greater_than(Datum::long(1)),
        );
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_like_special_characters() {
        // Test LIKE with special characters in prefix
        let sql = "bar LIKE 'test-abc_123%'";
        let predicate = convert_to_iceberg_predicate(sql);
        // This should not be pushed down because it contains underscore
        assert_eq!(predicate, None);
    }

    #[test]
    fn test_predicate_conversion_with_like_unicode() {
        // Test LIKE with unicode characters in prefix
        let sql = "bar LIKE '测试%'";
        let predicate = convert_to_iceberg_predicate(sql).unwrap();
        assert_eq!(
            predicate,
            Reference::new("bar").starts_with(Datum::string("测试"))
        );
    }

    #[test]
    fn test_predicate_conversion_with_isnan() {
        let predicate = convert_to_iceberg_predicate("isnan(qux)").unwrap();
        assert_eq!(predicate, Reference::new("qux").is_nan());
    }

    #[test]
    fn test_predicate_conversion_with_not_isnan() {
        let predicate = convert_to_iceberg_predicate("NOT isnan(qux)").unwrap();
        assert_eq!(predicate, !Reference::new("qux").is_nan());
    }

    #[test]
    fn test_predicate_conversion_with_isnan_and_other_condition() {
        let sql = "isnan(qux) AND foo > 1";
        let predicate = convert_to_iceberg_predicate(sql).unwrap();
        let expected_predicate = Predicate::and(
            Reference::new("qux").is_nan(),
            Reference::new("foo").greater_than(Datum::long(1)),
        );
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_isnan_negation() {
        // -x is NaN iff x is NaN
        let predicate = convert_to_iceberg_predicate("isnan(-qux)").unwrap();
        assert_eq!(predicate, Reference::new("qux").is_nan());

        let predicate = convert_to_iceberg_predicate("NOT isnan(-qux)").unwrap();
        assert_eq!(predicate, !Reference::new("qux").is_nan());
    }

    #[test]
    fn test_predicate_conversion_with_isnan_abs() {
        // abs(x) is NaN iff x is NaN
        let predicate = convert_to_iceberg_predicate("isnan(abs(qux))").unwrap();
        assert_eq!(predicate, Reference::new("qux").is_nan());
    }

    #[test]
    fn test_predicate_conversion_with_isnan_additive() {
        // x + c, c + x, x - c, c - x are NaN iff x is NaN (for finite c)
        for sql in [
            "isnan(qux + 1)",
            "isnan(1 + qux)",
            "isnan(qux - 1)",
            "isnan(1 - qux)",
            "isnan(qux + 1.5)",
        ] {
            let predicate = convert_to_iceberg_predicate(sql).unwrap();
            assert_eq!(predicate, Reference::new("qux").is_nan(), "sql: {sql}");
        }
    }

    #[test]
    fn test_predicate_conversion_with_isnan_multiplicative() {
        // x * c, c * x, x / c are NaN iff x is NaN (for finite non-zero c)
        for sql in ["isnan(qux * 2)", "isnan(2 * qux)", "isnan(qux / 2)"] {
            let predicate = convert_to_iceberg_predicate(sql).unwrap();
            assert_eq!(predicate, Reference::new("qux").is_nan(), "sql: {sql}");
        }
    }

    #[test]
    fn test_predicate_conversion_with_isnan_nested_expr() {
        // Nested NaN-preserving transformations resolve to the inner column
        let predicate = convert_to_iceberg_predicate("isnan(-(abs(qux) + 1) * 3)").unwrap();
        assert_eq!(predicate, Reference::new("qux").is_nan());
    }

    #[test]
    fn test_predicate_conversion_with_isnan_and_other_complex_condition() {
        let sql = "isnan(qux + 1) AND foo > 1";
        let predicate = convert_to_iceberg_predicate(sql).unwrap();
        let expected_predicate = Predicate::and(
            Reference::new("qux").is_nan(),
            Reference::new("foo").greater_than(Datum::long(1)),
        );
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_isnan_unsupported_arg() {
        // Multiplying/dividing by zero does not preserve NaN-ness: `x * 0` is NaN
        // when `x` is ±inf, so it cannot be pushed down.
        assert_eq!(convert_to_iceberg_predicate("isnan(qux * 0)"), None);
        assert_eq!(convert_to_iceberg_predicate("isnan(qux / 0)"), None);

        // `c / x` is not NaN-preserving (e.g. `0 / 0` is NaN while `0` is not).
        assert_eq!(convert_to_iceberg_predicate("isnan(1 / qux)"), None);

        // Expressions referencing more than one column cannot be reduced to a
        // single column reference.
        assert_eq!(convert_to_iceberg_predicate("isnan(qux + foo)"), None);

        // Unknown scalar functions are not pushed down.
        assert_eq!(convert_to_iceberg_predicate("isnan(sqrt(qux))"), None);
    }
}
