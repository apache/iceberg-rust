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

use crate::expr::visitors::bound_predicate_visitor::{visit, BoundPredicateVisitor};
use crate::expr::{BoundPredicate, BoundReference};
use crate::spec::{DataFile, Datum, Literal, PrimitiveLiteral};
use crate::{Error, ErrorKind};
use fnv::FnvHashSet;

const IN_PREDICATE_LIMIT: usize = 200;
const ROWS_MIGHT_MATCH: crate::Result<bool> = Ok(true);
const ROWS_CANNOT_MATCH: crate::Result<bool> = Ok(false);

pub(crate) struct InclusiveMetricsEvaluator<'a> {
    data_file: &'a DataFile,
}

impl<'a> InclusiveMetricsEvaluator<'a> {
    fn new(data_file: &'a DataFile) -> Self {
        InclusiveMetricsEvaluator { data_file }
    }

    /// Evaluate this `InclusiveMetricsEvaluator`'s filter predicate against the
    /// provided [`DataFile`]'s metrics. Used by [`TableScan`] to
    /// see if this `DataFile` contains data that could match
    /// the scan's filter.
    pub(crate) fn eval(
        filter: &'a BoundPredicate,
        data_file: &'a DataFile,
        include_empty_files: bool,
    ) -> crate::Result<bool> {
        if !include_empty_files && data_file.record_count == 0 {
            return ROWS_CANNOT_MATCH;
        }

        let mut evaluator = Self::new(data_file);
        visit(&mut evaluator, filter)
    }

    fn nan_count(&self, field_id: i32) -> Option<&u64> {
        self.data_file.nan_value_counts.get(&field_id)
    }

    fn null_count(&self, field_id: i32) -> Option<&u64> {
        self.data_file.null_value_counts.get(&field_id)
    }

    fn value_count(&self, field_id: i32) -> Option<&u64> {
        self.data_file.value_counts.get(&field_id)
    }

    fn lower_bound(&self, field_id: i32) -> Option<&Literal> {
        self.data_file.lower_bounds.get(&field_id)
    }

    fn upper_bound(&self, field_id: i32) -> Option<&Literal> {
        self.data_file.upper_bounds.get(&field_id)
    }

    fn contains_nans_only(&self, field_id: i32) -> bool {
        let nan_count = self.nan_count(field_id);
        let value_count = self.value_count(field_id);

        nan_count == value_count
    }

    fn contains_nulls_only(&self, field_id: i32) -> bool {
        let null_count = self.null_count(field_id);
        let value_count = self.value_count(field_id);

        null_count == value_count
    }

    fn may_contain_null(&self, field_id: i32) -> bool {
        if let Some(&null_count) = self.null_count(field_id) {
            null_count > 0
        } else {
            true
        }
    }

    fn visit_inequality(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        cmp_fn: fn(&PrimitiveLiteral, &PrimitiveLiteral) -> bool,
        use_lower_bound: bool,
    ) -> crate::Result<bool> {
        let field_id = reference.field().id;

        if self.contains_nulls_only(field_id) || self.contains_nans_only(field_id) {
            return ROWS_CANNOT_MATCH;
        }

        if datum.is_nan() {
            // NaN indicates unreliable bounds.
            // See the InclusiveMetricsEvaluator docs for more.
            return ROWS_MIGHT_MATCH;
        }

        let bound = if use_lower_bound {
            self.lower_bound(field_id)
        } else {
            self.upper_bound(field_id)
        };

        if let Some(bound) = bound {
            let Literal::Primitive(bound) = bound else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Inequality Predicates can only compare against a Primitive Literal",
                ));
            };

            if cmp_fn(datum.literal(), bound) {
                return ROWS_MIGHT_MATCH;
            }

            return ROWS_CANNOT_MATCH;
        }

        ROWS_MIGHT_MATCH
    }
}

// Remove this annotation once all todos have been removed
#[allow(unused_variables)]
impl BoundPredicateVisitor for InclusiveMetricsEvaluator<'_> {
    type T = bool;

    fn always_true(&mut self) -> crate::Result<bool> {
        ROWS_MIGHT_MATCH
    }

    fn always_false(&mut self) -> crate::Result<bool> {
        ROWS_CANNOT_MATCH
    }

    fn and(&mut self, lhs: bool, rhs: bool) -> crate::Result<bool> {
        Ok(lhs && rhs)
    }

    fn or(&mut self, lhs: bool, rhs: bool) -> crate::Result<bool> {
        Ok(lhs || rhs)
    }

    fn not(&mut self, inner: bool) -> crate::Result<bool> {
        Ok(!inner)
    }

    fn is_null(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field_id = reference.field().id;

        match self.null_count(field_id) {
            Some(&0) => ROWS_CANNOT_MATCH,
            Some(_) => ROWS_MIGHT_MATCH,
            None => ROWS_MIGHT_MATCH,
        }
    }

    fn not_null(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field_id = reference.field().id;

        if self.contains_nulls_only(field_id) {
            return ROWS_CANNOT_MATCH;
        }

        ROWS_MIGHT_MATCH
    }

    fn is_nan(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field_id = reference.field().id;

        match self.nan_count(field_id) {
            Some(&0) => ROWS_CANNOT_MATCH,
            _ if self.contains_nulls_only(field_id) => ROWS_CANNOT_MATCH,
            _ => ROWS_MIGHT_MATCH,
        }
    }

    fn not_nan(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field_id = reference.field().id;

        if self.contains_nans_only(field_id) {
            return ROWS_CANNOT_MATCH;
        }

        ROWS_MIGHT_MATCH
    }

    fn less_than(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        self.visit_inequality(reference, datum, PartialOrd::lt, true)
    }

    fn less_than_or_eq(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        self.visit_inequality(reference, datum, PartialOrd::le, true)
    }

    fn greater_than(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        self.visit_inequality(reference, datum, PartialOrd::gt, false)
    }

    fn greater_than_or_eq(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        self.visit_inequality(reference, datum, PartialOrd::ge, false)
    }

    fn eq(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field_id = reference.field().id;

        if self.contains_nulls_only(field_id) || self.contains_nans_only(field_id) {
            return ROWS_CANNOT_MATCH;
        }

        if let Some(lower_bound) = self.lower_bound(field_id) {
            let Literal::Primitive(lower_bound) = lower_bound else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Eq Predicate can only compare against a Primitive Literal",
                ));
            };
            if lower_bound.is_nan() {
                // NaN indicates unreliable bounds.
                // See the InclusiveMetricsEvaluator docs for more.
                return ROWS_MIGHT_MATCH;
            } else if lower_bound.gt(datum.literal()) {
                return ROWS_CANNOT_MATCH;
            }
        }

        if let Some(upper_bound) = self.upper_bound(field_id) {
            let Literal::Primitive(upper_bound) = upper_bound else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Eq Predicate can only compare against a Primitive Literal",
                ));
            };
            if upper_bound.is_nan() {
                // NaN indicates unreliable bounds.
                // See the InclusiveMetricsEvaluator docs for more.
                return ROWS_MIGHT_MATCH;
            } else if upper_bound.lt(datum.literal()) {
                return ROWS_CANNOT_MATCH;
            }
        }

        ROWS_MIGHT_MATCH
    }

    fn not_eq(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        // Because the bounds are not necessarily a min or max value,
        // this cannot be answered using them. notEq(col, X) with (X, Y)
        // doesn't guarantee that X is a value in col.
        ROWS_MIGHT_MATCH
    }

    fn starts_with(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field_id = reference.field().id;

        if self.contains_nulls_only(field_id) {
            return ROWS_CANNOT_MATCH;
        }

        let PrimitiveLiteral::String(prefix) = datum.literal() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Cannot use StartsWith operator on non-string values",
            ));
        };

        if let Some(lower_bound) = self.lower_bound(field_id) {
            let Literal::Primitive(PrimitiveLiteral::String(lower_bound)) = lower_bound else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Cannot use StartsWith operator on non-string lower_bound value",
                ));
            };

            let length = lower_bound.len().min(prefix.len());

            // truncate lower bound so that its length
            // is not greater than the length of prefix
            if prefix[..length] < lower_bound[..length] {
                return ROWS_CANNOT_MATCH;
            }
        }

        if let Some(upper_bound) = self.upper_bound(field_id) {
            let Literal::Primitive(PrimitiveLiteral::String(upper_bound)) = upper_bound else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Cannot use StartsWith operator on non-string upper_bound value",
                ));
            };

            let length = upper_bound.len().min(prefix.len());

            // truncate upper bound so that its length
            // is not greater than the length of prefix
            if prefix[..length] > upper_bound[..length] {
                return ROWS_CANNOT_MATCH;
            }
        }

        ROWS_MIGHT_MATCH
    }

    fn not_starts_with(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field_id = reference.field().id;

        if self.may_contain_null(field_id) {
            return ROWS_MIGHT_MATCH;
        }

        // notStartsWith will match unless all values must start with the prefix.
        // This happens when the lower and upper bounds both start with the prefix.

        let PrimitiveLiteral::String(prefix) = datum.literal() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Cannot use StartsWith operator on non-string values",
            ));
        };

        let Some(lower_bound) = self.lower_bound(field_id) else {
            return ROWS_MIGHT_MATCH;
        };

        let Literal::Primitive(PrimitiveLiteral::String(lower_bound_str)) = lower_bound else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Cannot use NotStartsWith operator on non-string lower_bound value",
            ));
        };

        if lower_bound_str < prefix {
            // if lower is shorter than the prefix then lower doesn't start with the prefix
            return ROWS_MIGHT_MATCH;
        }

        if lower_bound_str[..prefix.len()] == *prefix {
            // lower bound matches the prefix

            let Some(upper_bound) = self.upper_bound(field_id) else {
                return ROWS_MIGHT_MATCH;
            };

            let Literal::Primitive(PrimitiveLiteral::String(upper_bound)) = upper_bound else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Cannot use NotStartsWith operator on non-string upper_bound value",
                ));
            };

            // if upper is shorter than the prefix then upper can't start with the prefix
            if upper_bound.len() < prefix.len() {
                return ROWS_MIGHT_MATCH;
            }

            if upper_bound[..prefix.len()] == *prefix {
                // both bounds match the prefix, so all rows must match the
                // prefix and therefore do not satisfy the predicate
                return ROWS_CANNOT_MATCH;
            }
        }

        ROWS_MIGHT_MATCH
    }

    fn r#in(
        &mut self,
        reference: &BoundReference,
        literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field_id = reference.field().id;

        if self.contains_nulls_only(field_id) || self.contains_nans_only(field_id) {
            return ROWS_CANNOT_MATCH;
        }

        if literals.len() > IN_PREDICATE_LIMIT {
            // skip evaluating the predicate if the number of values is too big
            return ROWS_MIGHT_MATCH;
        }

        if let Some(lower_bound) = self.lower_bound(field_id) {
            let Literal::Primitive(lower_bound) = lower_bound else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Eq Predicate can only compare against a Primitive Literal",
                ));
            };
            if lower_bound.is_nan() {
                // NaN indicates unreliable bounds. See the InclusiveMetricsEvaluator docs for more.
                return ROWS_MIGHT_MATCH;
            }

            if !literals.iter().any(|datum| datum.literal().ge(lower_bound)) {
                // if all values are less than lower bound, rows cannot match.
                return ROWS_CANNOT_MATCH;
            }
        }

        if let Some(upper_bound) = self.upper_bound(field_id) {
            let Literal::Primitive(upper_bound) = upper_bound else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Eq Predicate can only compare against a Primitive Literal",
                ));
            };
            if upper_bound.is_nan() {
                // NaN indicates unreliable bounds. See the InclusiveMetricsEvaluator docs for more.
                return ROWS_MIGHT_MATCH;
            }

            if !literals.iter().any(|datum| datum.literal().le(upper_bound)) {
                // if all values are greater than upper bound, rows cannot match.
                return ROWS_CANNOT_MATCH;
            }
        }

        ROWS_MIGHT_MATCH
    }

    fn not_in(
        &mut self,
        reference: &BoundReference,
        literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        // Because the bounds are not necessarily a min or max value,
        // this cannot be answered using them. notIn(col, {X, ...})
        // with (X, Y) doesn't guarantee that X is a value in col.
        ROWS_MIGHT_MATCH
    }
}

#[cfg(test)]
mod test {
    use crate::expr::visitors::inclusive_metrics_evaluator::InclusiveMetricsEvaluator;
    use crate::expr::PredicateOperator::NotNull;
    use crate::expr::{Bind, BoundPredicate, Predicate, Reference, UnaryExpression};
    use crate::spec::{
        DataContentType, DataFile, DataFileFormat, FieldSummary, NestedField, PartitionField,
        PartitionSpec, PrimitiveType, Schema, Struct, Transform, Type,
    };
    use std::sync::Arc;

    #[test]
    fn test_data_file_no_partitions() {
        let (table_schema_ref, _partition_spec_ref) = create_test_schema_and_partition_spec();

        let partition_filter = Predicate::AlwaysTrue
            .bind(table_schema_ref.clone(), false)
            .unwrap();

        let case_sensitive = false;

        let manifest_file_partitions = vec![];
        let data_file = create_test_data_file(manifest_file_partitions);

        let result =
            InclusiveMetricsEvaluator::eval(&partition_filter, &data_file, case_sensitive).unwrap();

        assert!(result);
    }

    #[test]
    fn test_all_nulls() {
        let result =
            InclusiveMetricsEvaluator::eval(&not_null("all_nulls"), get_test_file(), true).unwrap();

        assert!(!result, "Should skip: no non-null value in all null column");
    }

    #[test]
    fn test_no_nulls() {}

    #[test]
    fn test_is_nan() {}

    #[test]
    fn test_not_nan() {}

    #[test]
    fn test_required_column() {}

    #[test]
    fn test_missing_column() {}

    #[test]
    fn test_missing_stats() {}

    #[test]
    fn test_zero_record_file() {}

    #[test]
    fn test_and() {}

    #[test]
    fn test_or() {}

    #[test]
    fn test_integer_lt() {}

    #[test]
    fn test_integer_lt_eq() {}

    #[test]
    fn test_integer_gt() {}

    #[test]
    fn test_integer_gt_eq() {}

    #[test]
    fn test_integer_eq() {}

    #[test]
    fn test_integer_not_eq() {}

    #[test]
    fn test_integer_not_eq_rewritten() {}

    #[test]
    fn test_case_insensitive_integer_not_eq_rewritten() {}

    #[test]
    fn test_case_sensitive_integer_not_eq_rewritten() {}

    #[test]
    fn test_string_starts_with() {}

    #[test]
    fn test_string_not_starts_with() {}

    #[test]
    fn test_integer_in() {}

    #[test]
    fn test_integer_not_in() {}

    fn create_test_schema_and_partition_spec() -> (Arc<Schema>, Arc<PartitionSpec>) {
        let table_schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "a",
                Type::Primitive(PrimitiveType::Float),
            ))])
            .build()
            .unwrap();
        let table_schema_ref = Arc::new(table_schema);

        let partition_spec = PartitionSpec::builder()
            .with_spec_id(1)
            .with_fields(vec![PartitionField::builder()
                .source_id(1)
                .name("a".to_string())
                .field_id(1)
                .transform(Transform::Identity)
                .build()])
            .build()
            .unwrap();
        let partition_spec_ref = Arc::new(partition_spec);
        (table_schema_ref, partition_spec_ref)
    }

    fn not_null(reference: &str) -> BoundPredicate {
        let schema = create_test_schema();
        let filter = Predicate::Unary(UnaryExpression::new(NotNull, Reference::new(reference)));
        filter.bind(schema.clone(), true).unwrap()
    }

    fn create_test_schema() -> Arc<Schema> {
        let table_schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "no_stats",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::required(
                    2,
                    "required",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::optional(
                    4,
                    "all_nulls",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::optional(
                    5,
                    "some_nulls",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::optional(
                    6,
                    "no_nulls",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::optional(
                    7,
                    "all_nans",
                    Type::Primitive(PrimitiveType::Double),
                )),
                Arc::new(NestedField::optional(
                    8,
                    "some_nans",
                    Type::Primitive(PrimitiveType::Float),
                )),
                Arc::new(NestedField::optional(
                    9,
                    "no_nans",
                    Type::Primitive(PrimitiveType::Float),
                )),
                Arc::new(NestedField::optional(
                    10,
                    "all_nulls_double",
                    Type::Primitive(PrimitiveType::Double),
                )),
                Arc::new(NestedField::optional(
                    11,
                    "all_nans_v1_stats",
                    Type::Primitive(PrimitiveType::Float),
                )),
                Arc::new(NestedField::optional(
                    12,
                    "nan_and_null_only",
                    Type::Primitive(PrimitiveType::Double),
                )),
                Arc::new(NestedField::optional(
                    13,
                    "no_nan_stats",
                    Type::Primitive(PrimitiveType::Double),
                )),
                Arc::new(NestedField::optional(
                    14,
                    "some_empty",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .unwrap();

        let table_schema_ref = Arc::new(table_schema);

        table_schema_ref
    }

    fn create_test_data_file(_manifest_file_partitions: Vec<FieldSummary>) -> DataFile {
        DataFile {
            content: DataContentType::Data,
            file_path: "/test/path".to_string(),
            file_format: DataFileFormat::Parquet,
            partition: Struct::empty(),
            record_count: 10,
            file_size_in_bytes: 10,
            column_sizes: Default::default(),
            value_counts: Default::default(),
            null_value_counts: Default::default(),
            nan_value_counts: Default::default(),
            lower_bounds: Default::default(),
            upper_bounds: Default::default(),
            key_metadata: vec![],
            split_offsets: vec![],
            equality_ids: vec![],
            sort_order_id: None,
        }
    }
}
