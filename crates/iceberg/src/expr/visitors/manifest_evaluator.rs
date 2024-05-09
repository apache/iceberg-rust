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
use crate::spec::{Datum, FieldSummary, Literal, ManifestFile, PrimitiveLiteral};
use crate::Result;
use crate::{Error, ErrorKind};
use fnv::FnvHashSet;

#[derive(Debug)]
/// Evaluates a [`ManifestFile`] to see if the partition summaries
/// match a provided [`BoundPredicate`].
///
/// Used by [`TableScan`] to prune the list of [`ManifestFile`]s
/// in which data might be found that matches the TableScan's filter.
pub(crate) struct ManifestEvaluator {
    partition_filter: BoundPredicate,
    case_sensitive: bool,
}

impl ManifestEvaluator {
    pub(crate) fn new(partition_filter: BoundPredicate, case_sensitive: bool) -> Self {
        Self {
            partition_filter,
            case_sensitive,
        }
    }

    /// Evaluate this `ManifestEvaluator`'s filter predicate against the
    /// provided [`ManifestFile`]'s partitions. Used by [`TableScan`] to
    /// see if this `ManifestFile` could possibly contain data that matches
    /// the scan's filter.
    pub(crate) fn eval(&self, manifest_file: &ManifestFile) -> Result<bool> {
        if manifest_file.partitions.is_empty() {
            return Ok(true);
        }

        let mut evaluator = ManifestFilterVisitor::new(self, &manifest_file.partitions);

        visit(&mut evaluator, &self.partition_filter)
    }
}

struct ManifestFilterVisitor<'a> {
    manifest_evaluator: &'a ManifestEvaluator,
    partitions: &'a Vec<FieldSummary>,
}

impl<'a> ManifestFilterVisitor<'a> {
    fn new(manifest_evaluator: &'a ManifestEvaluator, partitions: &'a Vec<FieldSummary>) -> Self {
        ManifestFilterVisitor {
            manifest_evaluator,
            partitions,
        }
    }
}

const ROWS_MIGHT_MATCH: Result<bool> = Ok(true);
const ROWS_CANNOT_MATCH: Result<bool> = Ok(false);
const IN_PREDICATE_LIMIT: usize = 200;

impl BoundPredicateVisitor for ManifestFilterVisitor<'_> {
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
        Ok(self.field_summary_for_reference(reference).contains_null)
    }

    fn not_null(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field: &FieldSummary = self.field_summary_for_reference(reference);

        // contains_null encodes whether at least one partition value is null,
        // lowerBound is null if all partition values are null
        let mut all_null: bool = field.contains_null && field.lower_bound.is_none();

        if all_null && reference.field().field_type.is_floating_type() {
            // floating point types may include NaN values, which we check separately.
            // In case bounds don't include NaN value, contains_nan needs to be checked against.
            all_null = field.contains_nan.is_some_and(|x| !x);
        }

        Ok(!all_null)
    }

    fn is_nan(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field: &FieldSummary = self.field_summary_for_reference(reference);
        if let Some(contains_nan) = field.contains_nan {
            Ok(contains_nan)
        } else {
            ROWS_MIGHT_MATCH
        }
    }

    fn not_nan(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field: &FieldSummary = self.field_summary_for_reference(reference);
        if field.contains_nan.is_some_and(|x| x)
            && !field.contains_null
            && field.lower_bound.is_none()
        {
            ROWS_CANNOT_MATCH
        } else {
            ROWS_MIGHT_MATCH
        }
    }

    fn less_than(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field: &FieldSummary = self.field_summary_for_reference(reference);
        if let Some(lower_bound) = &field.lower_bound {
            Ok(lower_bound < &Literal::from(datum.clone()))
        } else {
            ROWS_CANNOT_MATCH
        }
    }

    fn less_than_or_eq(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field: &FieldSummary = self.field_summary_for_reference(reference);
        if let Some(lower_bound) = &field.lower_bound {
            Ok(lower_bound <= &Literal::from(datum.clone()))
        } else {
            ROWS_CANNOT_MATCH
        }
    }

    fn greater_than(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field: &FieldSummary = self.field_summary_for_reference(reference);
        if let Some(upper_bound) = &field.upper_bound {
            Ok(upper_bound > &Literal::from(datum.clone()))
        } else {
            ROWS_CANNOT_MATCH
        }
    }

    fn greater_than_or_eq(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field: &FieldSummary = self.field_summary_for_reference(reference);
        if let Some(upper_bound) = &field.upper_bound {
            Ok(upper_bound >= &Literal::from(datum.clone()))
        } else {
            ROWS_CANNOT_MATCH
        }
    }

    fn eq(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        Ok(self.less_than_or_eq(reference, datum, _predicate)?
            && self.greater_than_or_eq(reference, datum, _predicate)?)
    }

    fn not_eq(
        &mut self,
        _reference: &BoundReference,
        _datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        // because the bounds are not necessarily a min or max value, this cannot be answered using
        // them. notEq(col, X) with (X, Y) doesn't guarantee that X is a value in col.
        ROWS_MIGHT_MATCH
    }

    fn starts_with(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field: &FieldSummary = self.field_summary_for_reference(reference);

        if field.lower_bound.is_none() || field.upper_bound.is_none() {
            return ROWS_CANNOT_MATCH;
        }

        let PrimitiveLiteral::String(prefix) = datum.literal() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Cannot perfom starts_with on non-string value",
            ));
        };

        let prefix_len = prefix.chars().count();

        if let Some(lower_bound) = field.lower_bound.clone() {
            let Literal::Primitive(PrimitiveLiteral::String(lower_bound)) = lower_bound else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Cannot perfom starts_with on non-string lower bound",
                ));
            };

            let min_len = lower_bound.chars().count().min(prefix_len);
            let truncated_lower_bound = String::from(&lower_bound[..min_len]);
            if prefix < &truncated_lower_bound {
                return ROWS_CANNOT_MATCH;
            }
        }

        if let Some(upper_bound) = field.upper_bound.clone() {
            let Literal::Primitive(PrimitiveLiteral::String(upper_bound)) = upper_bound else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Cannot perfom starts_with on non-string upper bound",
                ));
            };

            let min_len = upper_bound.chars().count().min(prefix_len);
            let truncated_upper_bound = String::from(&upper_bound[..min_len]);
            if prefix > &truncated_upper_bound {
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
        let field: &FieldSummary = self.field_summary_for_reference(reference);

        if field.contains_null || field.lower_bound.is_none() || field.upper_bound.is_none() {
            return ROWS_MIGHT_MATCH;
        }

        let PrimitiveLiteral::String(prefix) = datum.literal() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Cannot perfom not_starts_with on non-string value",
            ));
        };

        let prefix_len = prefix.chars().count();

        // not_starts_with will match unless all values must start with the prefix. This happens when
        // the lower and upper bounds both start with the prefix.
        if let Some(lower_bound) = field.lower_bound.clone() {
            let Literal::Primitive(PrimitiveLiteral::String(lower_bound)) = lower_bound else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Cannot perfom not_starts_with on non-string lower bound",
                ));
            };

            // if lower is shorter than the prefix then lower doesn't start with the prefix
            if prefix_len > lower_bound.chars().count() {
                return ROWS_MIGHT_MATCH;
            }

            let truncated_lower_bound = String::from(&lower_bound[..prefix_len]);
            if prefix == &truncated_lower_bound {
                if let Some(upper_bound) = field.upper_bound.clone() {
                    let Literal::Primitive(PrimitiveLiteral::String(upper_bound)) = upper_bound
                    else {
                        return Err(Error::new(
                            ErrorKind::Unexpected,
                            "Cannot perfom not_starts_with on non-string upper bound",
                        ));
                    };

                    // if upper is shorter than the prefix then upper can't start with the prefix
                    if prefix_len > upper_bound.chars().count() {
                        return ROWS_MIGHT_MATCH;
                    }

                    let truncated_upper_bound = String::from(&upper_bound[..prefix_len]);
                    if prefix == &truncated_upper_bound {
                        return ROWS_CANNOT_MATCH;
                    }
                }
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
        let field: &FieldSummary = self.field_summary_for_reference(reference);
        if field.lower_bound.is_none() {
            return ROWS_CANNOT_MATCH;
        }

        if literals.len() > IN_PREDICATE_LIMIT {
            return ROWS_MIGHT_MATCH;
        }

        if literals
            .iter()
            .all(|datum| field.lower_bound.as_ref().unwrap() > &Literal::from(datum.clone()))
        {
            return ROWS_CANNOT_MATCH;
        }

        if field.upper_bound.is_some()
            && literals
                .iter()
                .all(|datum| field.upper_bound.as_ref().unwrap() < &Literal::from(datum.clone()))
        {
            return ROWS_CANNOT_MATCH;
        }

        ROWS_MIGHT_MATCH
    }

    fn not_in(
        &mut self,
        _reference: &BoundReference,
        _literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        // because the bounds are not necessarily a min or max value, this cannot be answered using
        // them. notIn(col, {X, ...}) with (X, Y) doesn't guarantee that X is a value in col.
        ROWS_MIGHT_MATCH
    }
}

impl ManifestFilterVisitor<'_> {
    fn field_summary_for_reference(&self, reference: &BoundReference) -> &FieldSummary {
        let pos = reference.accessor().position();
        &self.partitions[pos]
    }
}

#[cfg(test)]
mod test {
    use crate::expr::visitors::manifest_evaluator::ManifestEvaluator;
    use crate::expr::{
        BinaryExpression, Bind, Predicate, PredicateOperator, Reference, SetExpression,
        UnaryExpression,
    };
    use crate::spec::{
        Datum, FieldSummary, Literal, ManifestContentType, ManifestFile, NestedField,
        PrimitiveType, Schema, SchemaRef, Type,
    };
    use crate::Result;
    use fnv::FnvHashSet;
    use std::ops::Not;
    use std::sync::Arc;

    const INT_MIN_VALUE: i32 = 30;
    const INT_MAX_VALUE: i32 = 79;

    const STRING_MIN_VALUE: &str = "a";
    const STRING_MAX_VALUE: &str = "z";

    fn create_schema() -> Result<SchemaRef> {
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "all_nulls_missing_nan",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::optional(
                    3,
                    "some_nulls",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::optional(
                    4,
                    "no_nulls",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::optional(
                    5,
                    "float",
                    Type::Primitive(PrimitiveType::Float),
                )),
                Arc::new(NestedField::optional(
                    6,
                    "all_nulls_double",
                    Type::Primitive(PrimitiveType::Double),
                )),
                Arc::new(NestedField::optional(
                    7,
                    "all_nulls_no_nans",
                    Type::Primitive(PrimitiveType::Float),
                )),
                Arc::new(NestedField::optional(
                    8,
                    "all_nans",
                    Type::Primitive(PrimitiveType::Double),
                )),
                Arc::new(NestedField::optional(
                    9,
                    "both_nan_and_null",
                    Type::Primitive(PrimitiveType::Float),
                )),
                Arc::new(NestedField::optional(
                    10,
                    "no_nan_or_null",
                    Type::Primitive(PrimitiveType::Double),
                )),
                Arc::new(NestedField::optional(
                    11,
                    "all_nulls_missing_nan_float",
                    Type::Primitive(PrimitiveType::Float),
                )),
                Arc::new(NestedField::optional(
                    12,
                    "all_same_value_or_null",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::optional(
                    13,
                    "no_nulls_same_value_a",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()?;

        Ok(Arc::new(schema))
    }

    fn create_partitions() -> Vec<FieldSummary> {
        vec![
            // id
            FieldSummary {
                contains_null: false,
                contains_nan: None,
                lower_bound: Some(Literal::int(INT_MIN_VALUE)),
                upper_bound: Some(Literal::int(INT_MAX_VALUE)),
            },
            // all_nulls_missing_nan
            FieldSummary {
                contains_null: true,
                contains_nan: None,
                lower_bound: None,
                upper_bound: None,
            },
            // some_nulls
            FieldSummary {
                contains_null: true,
                contains_nan: None,
                lower_bound: Some(Literal::string(STRING_MIN_VALUE)),
                upper_bound: Some(Literal::string(STRING_MAX_VALUE)),
            },
            // no_nulls
            FieldSummary {
                contains_null: false,
                contains_nan: None,
                lower_bound: Some(Literal::string(STRING_MIN_VALUE)),
                upper_bound: Some(Literal::string(STRING_MAX_VALUE)),
            },
            // float
            FieldSummary {
                contains_null: true,
                contains_nan: None,
                lower_bound: Some(Literal::float(0.0)),
                upper_bound: Some(Literal::float(20.0)),
            },
            // all_nulls_double
            FieldSummary {
                contains_null: true,
                contains_nan: None,
                lower_bound: None,
                upper_bound: None,
            },
            // all_nulls_no_nans
            FieldSummary {
                contains_null: true,
                contains_nan: Some(false),
                lower_bound: None,
                upper_bound: None,
            },
            // all_nans
            FieldSummary {
                contains_null: false,
                contains_nan: Some(true),
                lower_bound: None,
                upper_bound: None,
            },
            // both_nan_and_null
            FieldSummary {
                contains_null: true,
                contains_nan: Some(true),
                lower_bound: None,
                upper_bound: None,
            },
            // no_nan_or_null
            FieldSummary {
                contains_null: false,
                contains_nan: Some(false),
                lower_bound: Some(Literal::float(0.0)),
                upper_bound: Some(Literal::float(20.0)),
            },
            // all_nulls_missing_nan_float
            FieldSummary {
                contains_null: true,
                contains_nan: None,
                lower_bound: None,
                upper_bound: None,
            },
            // all_same_value_or_null
            FieldSummary {
                contains_null: true,
                contains_nan: None,
                lower_bound: Some(Literal::string(STRING_MIN_VALUE)),
                upper_bound: Some(Literal::string(STRING_MIN_VALUE)),
            },
            // no_nulls_same_value_a
            FieldSummary {
                contains_null: false,
                contains_nan: None,
                lower_bound: Some(Literal::string(STRING_MIN_VALUE)),
                upper_bound: Some(Literal::string(STRING_MIN_VALUE)),
            },
        ]
    }

    fn create_manifest_file(partitions: Vec<FieldSummary>) -> ManifestFile {
        ManifestFile {
            manifest_path: "/test/path".to_string(),
            manifest_length: 0,
            partition_spec_id: 1,
            content: ManifestContentType::Data,
            sequence_number: 0,
            min_sequence_number: 0,
            added_snapshot_id: 0,
            added_data_files_count: None,
            existing_data_files_count: None,
            deleted_data_files_count: None,
            added_rows_count: None,
            existing_rows_count: None,
            deleted_rows_count: None,
            partitions,
            key_metadata: vec![],
        }
    }

    #[test]
    fn test_always_true() -> Result<()> {
        let case_sensitive = false;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        let filter = Predicate::AlwaysTrue.bind(schema.clone(), case_sensitive)?;

        assert!(ManifestEvaluator::new(filter, case_sensitive).eval(&manifest_file)?);

        Ok(())
    }

    #[test]
    fn test_always_false() -> Result<()> {
        let case_sensitive = false;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        let filter = Predicate::AlwaysFalse.bind(schema.clone(), case_sensitive)?;

        assert!(!ManifestEvaluator::new(filter, case_sensitive).eval(&manifest_file)?);

        Ok(())
    }

    #[test]
    fn test_all_nulls() -> Result<()> {
        let case_sensitive = true;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        // all_nulls_missing_nan
        let all_nulls_missing_nan_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::NotNull,
            Reference::new("all_nulls_missing_nan"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            !ManifestEvaluator::new(all_nulls_missing_nan_filter, case_sensitive)
                .eval(&manifest_file)?,
            "Should skip: all nulls column with non-floating type contains all null"
        );

        // all_nulls_missing_nan_float
        let all_nulls_missing_nan_float_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::NotNull,
            Reference::new("all_nulls_missing_nan_float"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::new(all_nulls_missing_nan_float_filter, case_sensitive)
                .eval(&manifest_file)?,
            "Should read: no NaN information may indicate presence of NaN value"
        );

        // some_nulls
        let some_nulls_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::NotNull,
            Reference::new("some_nulls"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::new(some_nulls_filter, case_sensitive).eval(&manifest_file)?,
            "Should read: column with some nulls contains a non-null value"
        );

        // no_nulls
        let no_nulls_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::NotNull,
            Reference::new("no_nulls"),
        ))
        .bind(schema.clone(), case_sensitive)?;

        assert!(
            ManifestEvaluator::new(no_nulls_filter, case_sensitive).eval(&manifest_file)?,
            "Should read: non-null column contains a non-null value"
        );

        Ok(())
    }

    #[test]
    fn test_no_nulls() -> Result<()> {
        let case_sensitive = true;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        // all_nulls_missing_nan
        let all_nulls_missing_nan_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNull,
            Reference::new("all_nulls_missing_nan"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::new(all_nulls_missing_nan_filter, case_sensitive)
                .eval(&manifest_file)?,
            "Should read: at least one null value in all null column"
        );

        // some_nulls
        let some_nulls_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNull,
            Reference::new("some_nulls"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::new(some_nulls_filter, case_sensitive).eval(&manifest_file)?,
            "Should read: column with some nulls contains a null value"
        );

        // no_nulls
        let no_nulls_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNull,
            Reference::new("no_nulls"),
        ))
        .bind(schema.clone(), case_sensitive)?;

        assert!(
            !ManifestEvaluator::new(no_nulls_filter, case_sensitive).eval(&manifest_file)?,
            "Should skip: non-null column contains no null values"
        );

        // both_nan_and_null
        let both_nan_and_null_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNull,
            Reference::new("both_nan_and_null"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::new(both_nan_and_null_filter, case_sensitive)
                .eval(&manifest_file)?,
            "Should read: both_nan_and_null column contains no null values"
        );

        Ok(())
    }

    #[test]
    fn test_is_nan() -> Result<()> {
        let case_sensitive = true;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        // float
        let float_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNan,
            Reference::new("float"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::new(float_filter, case_sensitive).eval(&manifest_file)?,
            "Should read: no information on if there are nan value in float column"
        );

        // all_nulls_double
        let all_nulls_double_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNan,
            Reference::new("all_nulls_double"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::new(all_nulls_double_filter, case_sensitive).eval(&manifest_file)?,
            "Should read: no NaN information may indicate presence of NaN value"
        );

        // all_nulls_missing_nan_float
        let all_nulls_missing_nan_float_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNan,
            Reference::new("all_nulls_missing_nan_float"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::new(all_nulls_missing_nan_float_filter, case_sensitive)
                .eval(&manifest_file)?,
            "Should read: no NaN information may indicate presence of NaN value"
        );

        // all_nulls_no_nans
        let all_nulls_no_nans_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNan,
            Reference::new("all_nulls_no_nans"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            !ManifestEvaluator::new(all_nulls_no_nans_filter, case_sensitive)
                .eval(&manifest_file)?,
            "Should skip: no nan column doesn't contain nan value"
        );

        // all_nans
        let all_nans_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNan,
            Reference::new("all_nans"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::new(all_nans_filter, case_sensitive).eval(&manifest_file)?,
            "Should read: all_nans column contains nan value"
        );

        // both_nan_and_null
        let both_nan_and_null_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNan,
            Reference::new("both_nan_and_null"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::new(both_nan_and_null_filter, case_sensitive)
                .eval(&manifest_file)?,
            "Should read: both_nan_and_null column contains nan value"
        );

        // no_nan_or_null
        let no_nan_or_null_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNan,
            Reference::new("no_nan_or_null"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            !ManifestEvaluator::new(no_nan_or_null_filter, case_sensitive).eval(&manifest_file)?,
            "Should skip: no_nan_or_null column doesn't contain nan value"
        );

        Ok(())
    }

    #[test]
    fn test_not_nan() -> Result<()> {
        let case_sensitive = true;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        // float
        let float_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::NotNan,
            Reference::new("float"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::new(float_filter, case_sensitive).eval(&manifest_file)?,
            "Should read: no information on if there are nan value in float column"
        );

        // all_nulls_double
        let all_nulls_double_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::NotNan,
            Reference::new("all_nulls_double"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::new(all_nulls_double_filter, case_sensitive).eval(&manifest_file)?,
            "Should read: all null column contains non nan value"
        );

        // all_nulls_no_nans
        let all_nulls_no_nans_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::NotNan,
            Reference::new("all_nulls_no_nans"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::new(all_nulls_no_nans_filter, case_sensitive)
                .eval(&manifest_file)?,
            "Should read: no_nans column contains non nan value"
        );

        // all_nans
        let all_nans_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::NotNan,
            Reference::new("all_nans"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            !ManifestEvaluator::new(all_nans_filter, case_sensitive).eval(&manifest_file)?,
            "Should skip: all nans column doesn't contain non nan value"
        );

        // both_nan_and_null
        let both_nan_and_null_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::NotNan,
            Reference::new("both_nan_and_null"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::new(both_nan_and_null_filter, case_sensitive)
                .eval(&manifest_file)?,
            "Should read: both_nan_and_null nans column contains non nan value"
        );

        // no_nan_or_null
        let no_nan_or_null_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::NotNan,
            Reference::new("no_nan_or_null"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::new(no_nan_or_null_filter, case_sensitive).eval(&manifest_file)?,
            "Should read: no_nan_or_null column contains non nan value"
        );

        Ok(())
    }

    #[test]
    fn test_and() -> Result<()> {
        let case_sensitive = true;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        let filter = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::LessThan,
            Reference::new("id"),
            Datum::int(INT_MIN_VALUE - 25),
        ))
        .and(Predicate::Binary(BinaryExpression::new(
            PredicateOperator::GreaterThanOrEq,
            Reference::new("id"),
            Datum::int(INT_MIN_VALUE - 30),
        )))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            !ManifestEvaluator::new(filter, case_sensitive).eval(&manifest_file)?,
            "Should read: no information on if there are nan value in float column"
        );

        Ok(())
    }

    #[test]
    fn test_or() -> Result<()> {
        let case_sensitive = true;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        let filter = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::LessThan,
            Reference::new("id"),
            Datum::int(INT_MIN_VALUE - 25),
        ))
        .or(Predicate::Binary(BinaryExpression::new(
            PredicateOperator::GreaterThanOrEq,
            Reference::new("id"),
            Datum::int(INT_MAX_VALUE + 1),
        )))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            !ManifestEvaluator::new(filter, case_sensitive).eval(&manifest_file)?,
            "Should skip: or(false, false)"
        );

        Ok(())
    }

    #[test]
    fn test_not() -> Result<()> {
        let case_sensitive = true;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        let filter = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::LessThan,
            Reference::new("id"),
            Datum::int(INT_MIN_VALUE - 25),
        ))
        .not()
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::new(filter, case_sensitive).eval(&manifest_file)?,
            "Should read: not(false)"
        );

        let filter = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::GreaterThan,
            Reference::new("id"),
            Datum::int(INT_MIN_VALUE - 25),
        ))
        .not()
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            !ManifestEvaluator::new(filter, case_sensitive).eval(&manifest_file)?,
            "Should skip: not(true)"
        );

        Ok(())
    }

    #[test]
    fn test_less_than() -> Result<()> {
        let case_sensitive = true;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        let filter = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::LessThan,
            Reference::new("id"),
            Datum::int(INT_MIN_VALUE - 25),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            !ManifestEvaluator::new(filter, case_sensitive).eval(&manifest_file)?,
            "Should not read: id range below lower bound (5 < 30)"
        );

        Ok(())
    }

    #[test]
    fn test_less_than_or_eq() -> Result<()> {
        let case_sensitive = true;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        let filter = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::LessThanOrEq,
            Reference::new("id"),
            Datum::int(INT_MIN_VALUE - 25),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            !ManifestEvaluator::new(filter, case_sensitive).eval(&manifest_file)?,
            "Should not read: id range below lower bound (5 < 30)"
        );

        Ok(())
    }

    #[test]
    fn test_greater_than() -> Result<()> {
        let case_sensitive = true;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        let filter = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::GreaterThan,
            Reference::new("id"),
            Datum::int(INT_MAX_VALUE + 6),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            !ManifestEvaluator::new(filter, case_sensitive).eval(&manifest_file)?,
            "Should not read: id range above upper bound (85 < 79)"
        );

        Ok(())
    }

    #[test]
    fn test_greater_than_or_eq() -> Result<()> {
        let case_sensitive = true;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        let filter = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::GreaterThanOrEq,
            Reference::new("id"),
            Datum::int(INT_MAX_VALUE + 6),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            !ManifestEvaluator::new(filter, case_sensitive).eval(&manifest_file)?,
            "Should not read: id range above upper bound (85 < 79)"
        );

        let filter = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::GreaterThanOrEq,
            Reference::new("id"),
            Datum::int(INT_MAX_VALUE),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::new(filter, case_sensitive).eval(&manifest_file)?,
            "Should read: one possible id"
        );

        Ok(())
    }

    #[test]
    fn test_eq() -> Result<()> {
        let case_sensitive = true;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        let filter = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::Eq,
            Reference::new("id"),
            Datum::int(INT_MIN_VALUE - 25),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            !ManifestEvaluator::new(filter, case_sensitive).eval(&manifest_file)?,
            "Should not read: id below lower bound"
        );

        let filter = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::Eq,
            Reference::new("id"),
            Datum::int(INT_MIN_VALUE),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::new(filter, case_sensitive).eval(&manifest_file)?,
            "Should read: id equal to lower bound"
        );

        Ok(())
    }

    #[test]
    fn test_not_eq() -> Result<()> {
        let case_sensitive = true;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        let filter = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::NotEq,
            Reference::new("id"),
            Datum::int(INT_MIN_VALUE - 25),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::new(filter, case_sensitive).eval(&manifest_file)?,
            "Should read: id below lower bound"
        );

        Ok(())
    }

    #[test]
    fn test_in() -> Result<()> {
        let case_sensitive = true;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        let filter = Predicate::Set(SetExpression::new(
            PredicateOperator::In,
            Reference::new("id"),
            FnvHashSet::from_iter(vec![
                Datum::int(INT_MIN_VALUE - 25),
                Datum::int(INT_MIN_VALUE - 24),
            ]),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            !ManifestEvaluator::new(filter, case_sensitive).eval(&manifest_file)?,
            "Should not read: id below lower bound (5 < 30, 6 < 30)"
        );

        let filter = Predicate::Set(SetExpression::new(
            PredicateOperator::In,
            Reference::new("id"),
            FnvHashSet::from_iter(vec![
                Datum::int(INT_MIN_VALUE - 1),
                Datum::int(INT_MIN_VALUE),
            ]),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::new(filter, case_sensitive).eval(&manifest_file)?,
            "Should read: id equal to lower bound (30 == 30)"
        );

        Ok(())
    }

    #[test]
    fn test_not_in() -> Result<()> {
        let case_sensitive = true;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        let filter = Predicate::Set(SetExpression::new(
            PredicateOperator::NotIn,
            Reference::new("id"),
            FnvHashSet::from_iter(vec![
                Datum::int(INT_MIN_VALUE - 25),
                Datum::int(INT_MIN_VALUE - 24),
            ]),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::new(filter, case_sensitive).eval(&manifest_file)?,
            "Should read: id below lower bound (5 < 30, 6 < 30)"
        );

        Ok(())
    }

    #[test]
    fn test_starts_with() -> Result<()> {
        let case_sensitive = false;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        let filter = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::StartsWith,
            Reference::new("some_nulls"),
            Datum::string("a"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::new(filter, case_sensitive).eval(&manifest_file)?,
            "Should read: range matches"
        );

        let filter = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::StartsWith,
            Reference::new("some_nulls"),
            Datum::string("zzzz"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            !ManifestEvaluator::new(filter, case_sensitive).eval(&manifest_file)?,
            "Should skip: range doesn't match"
        );

        Ok(())
    }

    #[test]
    fn test_not_starts_with() -> Result<()> {
        let case_sensitive = false;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        let filter = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::NotStartsWith,
            Reference::new("some_nulls"),
            Datum::string("a"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::new(filter, case_sensitive).eval(&manifest_file)?,
            "Should read: range matches"
        );

        let filter = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::NotStartsWith,
            Reference::new("no_nulls_same_value_a"),
            Datum::string("a"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            !ManifestEvaluator::new(filter, case_sensitive).eval(&manifest_file)?,
            "Should not read: all values start with the prefix"
        );

        Ok(())
    }
}
