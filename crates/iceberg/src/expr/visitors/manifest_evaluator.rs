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
use crate::spec::{Datum, FieldSummary, Literal, ManifestFile, PrimitiveLiteral, Type};
use crate::{Error, ErrorKind, Result};
use fnv::FnvHashSet;

const IN_PREDICATE_LIMIT: usize = 200;
const ROWS_MIGHT_MATCH: Result<bool> = Ok(true);
const ROWS_CANT_MATCH: Result<bool> = Ok(false);

/// Evaluates a [`ManifestFile`] to see if the partition summaries
/// match a provided [`BoundPredicate`].
///
/// Used by [`TableScan`] to prune the list of [`ManifestFile`]s
/// in which data might be found that matches the TableScan's filter.
#[derive(Debug)]
pub(crate) struct ManifestEvaluator {
    partition_filter: BoundPredicate,
}

impl ManifestEvaluator {
    pub(crate) fn new(partition_filter: BoundPredicate) -> Self {
        Self { partition_filter }
    }

    /// Evaluate this `ManifestEvaluator`'s filter predicate against the
    /// provided [`ManifestFile`]'s partitions. Used by [`TableScan`] to
    /// see if this `ManifestFile` could possibly contain data that matches
    /// the scan's filter.
    pub(crate) fn eval(&self, manifest_file: &ManifestFile) -> Result<bool> {
        if manifest_file.partitions.is_empty() {
            return ROWS_MIGHT_MATCH;
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

// Remove this annotation once all todos have been removed
#[allow(unused_variables)]
impl BoundPredicateVisitor for ManifestFilterVisitor<'_> {
    type T = bool;

    fn always_true(&mut self) -> Result<bool> {
        ROWS_MIGHT_MATCH
    }

    fn always_false(&mut self) -> Result<bool> {
        ROWS_CANT_MATCH
    }

    fn and(&mut self, lhs: bool, rhs: bool) -> Result<bool> {
        Ok(lhs && rhs)
    }

    fn or(&mut self, lhs: bool, rhs: bool) -> Result<bool> {
        Ok(lhs || rhs)
    }

    fn not(&mut self, inner: bool) -> Result<bool> {
        Ok(!inner)
    }

    fn is_null(&mut self, reference: &BoundReference, _predicate: &BoundPredicate) -> Result<bool> {
        if self.field_summary_for_reference(reference).contains_null {
            return ROWS_CANT_MATCH;
        }

        ROWS_MIGHT_MATCH
    }

    fn not_null(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        let field_summary = self.field_summary_for_reference(reference);

        if self.contains_nulls_only(field_summary, reference.field().field_type.as_ref()) {
            return ROWS_CANT_MATCH;
        }

        ROWS_MIGHT_MATCH
    }

    fn is_nan(&mut self, reference: &BoundReference, _predicate: &BoundPredicate) -> Result<bool> {
        let field_summary = self.field_summary_for_reference(reference);

        if let Some(contains_nan) = field_summary.contains_nan {
            if !contains_nan {
                return ROWS_CANT_MATCH;
            }
        }

        if self.contains_nulls_only(field_summary, reference.field().field_type.as_ref()) {
            return ROWS_CANT_MATCH;
        }

        ROWS_MIGHT_MATCH
    }

    fn not_nan(&mut self, reference: &BoundReference, _predicate: &BoundPredicate) -> Result<bool> {
        let FieldSummary {
            contains_nan,
            contains_null,
            lower_bound,
            ..
        } = self.field_summary_for_reference(reference);

        if let Some(contains_nan) = contains_nan {
            if *contains_nan && !contains_null && lower_bound.is_none() {
                return ROWS_CANT_MATCH;
            }
        }

        ROWS_MIGHT_MATCH
    }

    fn less_than(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        self.visit_inequality(reference, literal, PartialOrd::lt, true)
    }

    fn less_than_or_eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        self.visit_inequality(reference, literal, PartialOrd::le, true)
    }

    fn greater_than(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        self.visit_inequality(reference, literal, PartialOrd::gt, false)
    }

    fn greater_than_or_eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        self.visit_inequality(reference, literal, PartialOrd::ge, false)
    }

    fn eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        if let Ok(result) = self.visit_inequality(reference, literal, PartialOrd::lt, true) {
            if !result {
                return ROWS_CANT_MATCH;
            }
        }

        if let Ok(result) = self.visit_inequality(reference, literal, PartialOrd::gt, false) {
            if !result {
                return ROWS_CANT_MATCH;
            }
        }

        ROWS_MIGHT_MATCH
    }

    fn not_eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        // because the bounds are not necessarily a min or max value, this cannot be answered using
        // them. notEq(col, X) with (X, Y) doesn't guarantee that X is a value in col.
        ROWS_MIGHT_MATCH
    }

    fn starts_with(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        let field_summary = self.field_summary_for_reference(reference);

        if self.contains_nulls_only(field_summary, reference.field().field_type.as_ref()) {
            return ROWS_CANT_MATCH;
        }

        let PrimitiveLiteral::String(datum) = literal.literal() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Cannot use StartsWith operator on non-string values",
            ));
        };

        if let Some(lower_bound) = &field_summary.lower_bound {
            let Literal::Primitive(PrimitiveLiteral::String(lower_bound)) = lower_bound else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Cannot use StartsWith operator on non-string lower_bound value",
                ));
            };

            let prefix_length = lower_bound.chars().count().min(datum.chars().count());

            // truncate lower bound so that its length
            // is not greater than the length of prefix
            let truncated_lower_bound = lower_bound.chars().take(prefix_length).collect::<String>();
            if datum < &truncated_lower_bound {
                return ROWS_CANT_MATCH;
            }
        }

        if let Some(upper_bound) = &field_summary.upper_bound {
            let Literal::Primitive(PrimitiveLiteral::String(upper_bound)) = upper_bound else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Cannot use StartsWith operator on non-string upper_bound value",
                ));
            };

            let prefix_length = upper_bound.chars().count().min(datum.chars().count());

            // truncate upper bound so that its length
            // is not greater than the length of prefix
            let truncated_upper_bound = upper_bound.chars().take(prefix_length).collect::<String>();
            if datum > &truncated_upper_bound {
                return ROWS_CANT_MATCH;
            }
        }

        ROWS_MIGHT_MATCH
    }

    fn not_starts_with(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        let field_summary = self.field_summary_for_reference(reference);

        if field_summary.contains_null {
            return ROWS_MIGHT_MATCH;
        }

        let PrimitiveLiteral::String(literal) = literal.literal() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Cannot use StartsWith operator on non-string values",
            ));
        };

        // notStartsWith will match unless all values start with the prefix. This happens when
        // the lower and upper bounds both start with the prefix.
        if let Some(lower_bound) = &field_summary.lower_bound {
            let Literal::Primitive(PrimitiveLiteral::String(lower_bound)) = lower_bound else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Cannot use StartsWith operator on non-string lower_bound value",
                ));
            };

            if let Some(upper_bound) = &field_summary.upper_bound {
                let Literal::Primitive(PrimitiveLiteral::String(upper_bound)) = upper_bound else {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        "Cannot use StartsWith operator on non-string upper_bound value",
                    ));
                };

                let literal_length = literal.chars().count();
                let lower_bound_length = lower_bound.chars().count();

                // if lower is shorter than the prefix, it can't start with the prefix
                if lower_bound_length < literal_length {
                    return ROWS_MIGHT_MATCH;
                }

                // truncate lower bound so that its length
                // is not greater than the length of prefix
                let truncated_lower_bound =
                    lower_bound.chars().take(literal_length).collect::<String>();

                if truncated_lower_bound.eq(literal) {
                    let upper_bound_length = upper_bound.chars().count();

                    // the lower bound starts with the prefix; check the upper bound
                    // if upper is shorter than the prefix, it can't start with the prefix
                    if upper_bound_length < literal_length {
                        return ROWS_MIGHT_MATCH;
                    }

                    let truncated_upper_bound =
                        upper_bound.chars().take(literal_length).collect::<String>();

                    if truncated_upper_bound.eq(literal) {
                        // both bounds match the prefix, so all rows must match the prefix and none do not match
                        return ROWS_CANT_MATCH;
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
    ) -> Result<bool> {
        let field_summary = self.field_summary_for_reference(reference);

        let Some(lower_bound) = &field_summary.lower_bound else {
            // values are all null and `literals` cannot contain null.
            return ROWS_CANT_MATCH;
        };

        if literals.len() > IN_PREDICATE_LIMIT {
            // skip evaluating the predicate if the number of values is too big
            return ROWS_MIGHT_MATCH;
        }

        if let Some(lower_bound) = &field_summary.lower_bound {
            let Literal::Primitive(PrimitiveLiteral::String(lower_bound)) = lower_bound else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Cannot use StartsWith operator on non-string lower_bound value",
                ));
            };

            let lower_bound_lit = PrimitiveLiteral::String(lower_bound.to_string());

            let all_literals_less_than_lower_bound = literals
                .iter()
                .all(|lit| lit.literal().lt(&lower_bound_lit));

            if all_literals_less_than_lower_bound {
                return ROWS_CANT_MATCH;
            }
        }

        if let Some(upper_bound) = &field_summary.upper_bound {
            let Literal::Primitive(PrimitiveLiteral::String(upper_bound)) = upper_bound else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Cannot use StartsWith operator on non-string upper_bound value",
                ));
            };

            let upper_bound_lit = PrimitiveLiteral::String(upper_bound.to_string());

            let all_literals_greater_than_upper_bound = literals
                .iter()
                .all(|lit| lit.literal().gt(&upper_bound_lit));

            if all_literals_greater_than_upper_bound {
                return ROWS_CANT_MATCH;
            }
        }

        ROWS_MIGHT_MATCH
    }

    fn not_in(
        &mut self,
        reference: &BoundReference,
        literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        ROWS_MIGHT_MATCH
    }
}

impl ManifestFilterVisitor<'_> {
    fn field_summary_for_reference(&self, reference: &BoundReference) -> &FieldSummary {
        let pos = reference.accessor().position();
        &self.partitions[pos]
    }

    fn contains_nulls_only(&self, summary: &FieldSummary, type_id: &Type) -> bool {
        // containsNull encodes whether at least one partition value is null,
        // lowerBound is null if all partition values are null
        let all_null = summary.contains_null && summary.lower_bound.is_none();

        if all_null && (type_id.is_floating_type()) {
            // floating point types may include NaN values, which we check separately.
            // In case bounds don't include NaN value, containsNaN needs to be checked against.
            if let Some(contains_nan) = summary.contains_nan {
                return !contains_nan;
            }
            return false;
        }

        all_null
    }

    fn visit_inequality(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        cmp_fn: fn(&PrimitiveLiteral, &PrimitiveLiteral) -> bool,
        use_lower_bound: bool,
    ) -> Result<bool> {
        let field_summary = self.field_summary_for_reference(reference);

        let bound = if use_lower_bound {
            &field_summary.lower_bound
        } else {
            &field_summary.upper_bound
        };

        let Some(bound) = bound else {
            return ROWS_CANT_MATCH;
        };

        if let Literal::Primitive(bound) = bound {
            if cmp_fn(bound, datum.literal()) {
                return ROWS_MIGHT_MATCH;
            }
        }

        ROWS_MIGHT_MATCH
    }
}

#[cfg(test)]
mod test {
    use crate::expr::visitors::inclusive_projection::InclusiveProjection;
    use crate::expr::visitors::manifest_evaluator::ManifestEvaluator;
    use crate::expr::{
        Bind, BoundPredicate, Predicate, PredicateOperator, Reference, UnaryExpression,
    };
    use crate::spec::{
        FieldSummary, ManifestContentType, ManifestFile, NestedField, PartitionField,
        PartitionSpec, PartitionSpecRef, PrimitiveType, Schema, SchemaRef, Transform, Type,
    };
    use crate::Result;
    use std::sync::Arc;

    fn create_schema_and_partition_spec() -> Result<(SchemaRef, PartitionSpecRef)> {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "a",
                Type::Primitive(PrimitiveType::Float),
            ))])
            .build()?;

        let spec = PartitionSpec::builder()
            .with_spec_id(1)
            .with_fields(vec![PartitionField::builder()
                .source_id(1)
                .name("a".to_string())
                .field_id(1)
                .transform(Transform::Identity)
                .build()])
            .build()
            .unwrap();

        Ok((Arc::new(schema), Arc::new(spec)))
    }

    fn create_schema_and_partition_spec_with_id_mismatch() -> Result<(SchemaRef, PartitionSpecRef)>
    {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "a",
                Type::Primitive(PrimitiveType::Float),
            ))])
            .build()?;

        let spec = PartitionSpec::builder()
            .with_spec_id(999)
            .with_fields(vec![PartitionField::builder()
                .source_id(1)
                .name("a".to_string())
                .field_id(1)
                .transform(Transform::Identity)
                .build()])
            .build()
            .unwrap();

        Ok((Arc::new(schema), Arc::new(spec)))
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

    fn create_partition_schema(
        partition_spec: &PartitionSpecRef,
        schema: &Schema,
    ) -> Result<SchemaRef> {
        let partition_type = partition_spec.partition_type(schema)?;

        let partition_fields: Vec<_> = partition_type.fields().iter().map(Arc::clone).collect();

        let partition_schema = Arc::new(
            Schema::builder()
                .with_schema_id(partition_spec.spec_id)
                .with_fields(partition_fields)
                .build()?,
        );

        Ok(partition_schema)
    }

    fn create_partition_filter(
        partition_spec: PartitionSpecRef,
        partition_schema: SchemaRef,
        filter: &BoundPredicate,
        case_sensitive: bool,
    ) -> Result<BoundPredicate> {
        let mut inclusive_projection = InclusiveProjection::new(partition_spec);

        let partition_filter = inclusive_projection
            .project(filter)?
            .rewrite_not()
            .bind(partition_schema, case_sensitive)?;

        Ok(partition_filter)
    }

    fn create_manifest_evaluator(
        schema: SchemaRef,
        partition_spec: PartitionSpecRef,
        filter: &BoundPredicate,
        case_sensitive: bool,
    ) -> Result<ManifestEvaluator> {
        let partition_schema = create_partition_schema(&partition_spec, &schema)?;
        let partition_filter = create_partition_filter(
            partition_spec,
            partition_schema.clone(),
            filter,
            case_sensitive,
        )?;

        Ok(ManifestEvaluator::new(partition_filter))
    }

    #[test]
    fn test_manifest_file_empty_partitions() -> Result<()> {
        let case_sensitive = false;

        let (schema, partition_spec) = create_schema_and_partition_spec()?;

        let filter = Predicate::AlwaysTrue.bind(schema.clone(), case_sensitive)?;

        let manifest_file = create_manifest_file(vec![]);

        let manifest_evaluator =
            create_manifest_evaluator(schema, partition_spec, &filter, case_sensitive)?;

        let result = manifest_evaluator.eval(&manifest_file)?;

        assert!(result);

        Ok(())
    }

    #[test]
    fn test_manifest_file_trivial_partition_passing_filter() -> Result<()> {
        let case_sensitive = true;

        let (schema, partition_spec) = create_schema_and_partition_spec()?;

        let filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNull,
            Reference::new("a"),
        ))
        .bind(schema.clone(), case_sensitive)?;

        let manifest_file = create_manifest_file(vec![FieldSummary {
            contains_null: true,
            contains_nan: None,
            lower_bound: None,
            upper_bound: None,
        }]);

        let manifest_evaluator =
            create_manifest_evaluator(schema, partition_spec, &filter, case_sensitive)?;

        let result = manifest_evaluator.eval(&manifest_file)?;

        assert!(result);

        Ok(())
    }

    #[test]
    fn test_manifest_file_trivial_partition_rejected_filter() -> Result<()> {
        let case_sensitive = true;

        let (schema, partition_spec) = create_schema_and_partition_spec()?;

        let filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNan,
            Reference::new("a"),
        ))
        .bind(schema.clone(), case_sensitive)?;

        let manifest_file = create_manifest_file(vec![FieldSummary {
            contains_null: false,
            contains_nan: None,
            lower_bound: None,
            upper_bound: None,
        }]);

        let manifest_evaluator =
            create_manifest_evaluator(schema, partition_spec, &filter, case_sensitive)?;

        let result = manifest_evaluator.eval(&manifest_file).unwrap();

        assert!(!result);

        Ok(())
    }
}
