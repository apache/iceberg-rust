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

//! The residual evaluator computes the *residual* of a row filter for a single
//! partition's values: the part of the filter that is NOT already decided by
//! the partition. It is the Rust port of Java
//! `org.apache.iceberg.expressions.ResidualEvaluator`.
//!
//! A residual expression is made by partially evaluating an expression using
//! partition values. For example, if a table is partitioned by
//! `day(utc_timestamp)` and is read with a filter `utc_timestamp >= a AND
//! utc_timestamp <= b`, then for a partition value `d` there are 4 possible
//! residuals:
//!
//! - if `d > day(a)` and `d < day(b)`, the residual is always true;
//! - if `d == day(a)` and `d != day(b)`, the residual is `utc_timestamp >= a`;
//! - if `d == day(b)` and `d != day(a)`, the residual is `utc_timestamp <= b`;
//! - if `d == day(a) == day(b)`, the residual is `utc_timestamp >= a AND
//!   utc_timestamp <= b`.
//!
//! The evaluator is wired into scan planning (`scan/context.rs` computes each
//! [`crate::scan::FileScanTask`]'s partition-reduced residual via
//! [`ResidualEvaluator::residual_for`]); Increment 3 adds a second consumer in
//! filter-based conflict validation.

use fnv::FnvHashSet;

use crate::Result;
use crate::expr::visitors::bound_predicate_visitor::{BoundPredicateVisitor, visit};
use crate::expr::visitors::expression_evaluator::ExpressionEvaluatorVisitor;
use crate::expr::{
    BinaryExpression, Bind, BoundPredicate, BoundReference, Predicate, Reference, SetExpression,
    UnaryExpression,
};
use crate::spec::{Datum, PartitionSpecRef, Schema, SchemaRef, Struct};

/// Computes the residual of a row filter for a given partition's values.
///
/// Mirrors Java `ResidualEvaluator`. Construct one with [`ResidualEvaluator::of`]
/// (or [`ResidualEvaluator::unpartitioned`]) and call [`ResidualEvaluator::residual_for`]
/// once per partition tuple.
///
/// Unlike Java — whose `Expression` is a single unbound/bound union — this port
/// holds the filter as a [`BoundPredicate`] (the scan binds the filter before
/// planning) and returns the residual as an unbound [`Predicate`]. The two leaf
/// "keep the original predicate" cases reconstruct the unbound predicate from the
/// bound one by name; partition source columns are always top-level schema
/// fields, so the [`BoundReference`]'s field name is the unbound reference name.
#[derive(Debug, Clone)]
pub(crate) struct ResidualEvaluator {
    /// `Some(spec, partition_schema)` for a partitioned spec; `None` for an
    /// unpartitioned spec (the residual is then always the whole filter).
    partitioned: Option<PartitionedState>,
    /// The bound row filter.
    filter: BoundPredicate,
    /// Case sensitivity used when binding projected predicates to the partition type.
    case_sensitive: bool,
}

/// The state needed to evaluate residuals against a non-empty partition spec.
#[derive(Debug, Clone)]
struct PartitionedState {
    /// The partition spec whose fields decide the residual.
    spec: PartitionSpecRef,
    /// The partition type as a [`Schema`], used to bind projected predicates
    /// (Java `spec.partitionType()`).
    partition_schema: SchemaRef,
}

impl ResidualEvaluator {
    /// Returns a residual evaluator for an unpartitioned spec: every residual is
    /// the whole filter, unchanged (Java `ResidualEvaluator.unpartitioned`).
    pub(crate) fn unpartitioned(filter: BoundPredicate) -> Self {
        Self {
            partitioned: None,
            filter,
            case_sensitive: false,
        }
    }

    /// Returns a residual evaluator for a partition spec and a bound filter.
    ///
    /// When the spec has no fields the evaluator degrades to the unpartitioned
    /// form (Java `ResidualEvaluator.of`: `spec.fields().isEmpty()`). `schema` is
    /// the table schema the filter was bound against; it is needed to compute the
    /// partition type the projections bind to (Java's `PartitionSpec` carries its
    /// schema, the Rust one does not).
    pub(crate) fn of(
        spec: PartitionSpecRef,
        schema: &Schema,
        filter: BoundPredicate,
        case_sensitive: bool,
    ) -> Result<Self> {
        if spec.fields().is_empty() {
            return Ok(Self::unpartitioned(filter));
        }

        let partition_type = spec.partition_type(schema)?;
        let partition_schema = Schema::builder()
            .with_schema_id(spec.spec_id())
            .with_fields(partition_type.fields().to_vec())
            .build()?;

        Ok(Self {
            partitioned: Some(PartitionedState {
                spec,
                partition_schema: SchemaRef::new(partition_schema),
            }),
            filter,
            case_sensitive,
        })
    }

    /// Returns the residual of the filter for the given partition values.
    ///
    /// `partition` is the partition tuple (Java `StructLike partitionData`). For
    /// an unpartitioned evaluator the filter is returned verbatim (as an unbound
    /// predicate).
    pub(crate) fn residual_for(&self, partition: &Struct) -> Result<Predicate> {
        let Some(state) = &self.partitioned else {
            return bound_to_unbound(&self.filter);
        };

        let mut visitor = ResidualVisitor {
            partition,
            spec: state,
            case_sensitive: self.case_sensitive,
        };
        visit(&mut visitor, &self.filter)
    }
}

/// The visitor that walks the bound filter and reduces each leaf against the
/// partition values, mirroring Java `ResidualEvaluator.ResidualVisitor`.
struct ResidualVisitor<'a> {
    /// The partition tuple being evaluated.
    partition: &'a Struct,
    /// The partition spec + partition schema.
    spec: &'a PartitionedState,
    /// Case sensitivity for binding projected predicates.
    case_sensitive: bool,
}

impl ResidualVisitor<'_> {
    /// Applies Java `ResidualVisitor.predicate(BoundPredicate)` (L227-288): for
    /// every partition field on this predicate's source column, the STRICT
    /// projection deciding `true` reduces the residual to `AlwaysTrue`, and the
    /// INCLUSIVE projection deciding `false` reduces it to `AlwaysFalse`. If no
    /// partition field is conclusive, the original predicate is kept.
    fn reduce_leaf(
        &self,
        reference: &BoundReference,
        predicate: &BoundPredicate,
    ) -> Result<Predicate> {
        // Not associated with a partition field — the predicate can't be reduced.
        let source_id = reference.field().id;
        let parts: Vec<_> = self
            .spec
            .spec
            .fields()
            .iter()
            .filter(|field| field.source_id == source_id)
            .collect();
        if parts.is_empty() {
            return bound_to_unbound(predicate);
        }

        for part in parts {
            // Strict projection: true iff the original predicate is guaranteed
            // true for every row in the partition, so the predicate is dropped.
            if let Some(strict) = part.transform.strict_project(&part.name, predicate)?
                && self.evaluate_projection(strict)?
            {
                return Ok(Predicate::AlwaysTrue);
            }

            // Inclusive projection: false iff the original predicate is
            // guaranteed false for every row in the partition, so the whole
            // residual is false.
            if let Some(inclusive) = part.transform.project(&part.name, predicate)?
                && !self.evaluate_projection(inclusive)?
            {
                return Ok(Predicate::AlwaysFalse);
            }
        }

        // Neither strict nor inclusive was conclusive — keep the original predicate.
        bound_to_unbound(predicate)
    }

    /// Binds a projected (partition-column) predicate to the partition type and
    /// evaluates it against the partition values, returning the boolean Java
    /// reaches via `super.predicate(...)` → the leaf evaluation methods.
    ///
    /// An `AlwaysTrue` / `AlwaysFalse` projection short-circuits to its constant
    /// (Java's "if the result is not a predicate, it must be a constant").
    fn evaluate_projection(&self, projection: Predicate) -> Result<bool> {
        let bound = projection.bind(self.spec.partition_schema.clone(), self.case_sensitive)?;
        match bound {
            BoundPredicate::AlwaysTrue => Ok(true),
            BoundPredicate::AlwaysFalse => Ok(false),
            other => {
                // `Not` must be rewritten before the expression evaluator can run
                // it; the partition projections from `Transform::{project,
                // strict_project}` never introduce a `Not`, but normalize anyway.
                let mut evaluator = ExpressionEvaluatorVisitor::new(self.partition);
                visit(&mut evaluator, &other.rewrite_not())
            }
        }
    }
}

impl BoundPredicateVisitor for ResidualVisitor<'_> {
    type T = Predicate;

    fn always_true(&mut self) -> Result<Predicate> {
        Ok(Predicate::AlwaysTrue)
    }

    fn always_false(&mut self) -> Result<Predicate> {
        Ok(Predicate::AlwaysFalse)
    }

    fn and(&mut self, lhs: Predicate, rhs: Predicate) -> Result<Predicate> {
        Ok(lhs.and(rhs))
    }

    fn or(&mut self, lhs: Predicate, rhs: Predicate) -> Result<Predicate> {
        Ok(lhs.or(rhs))
    }

    fn not(&mut self, inner: Predicate) -> Result<Predicate> {
        Ok(simplifying_not(inner))
    }

    fn is_null(
        &mut self,
        reference: &BoundReference,
        predicate: &BoundPredicate,
    ) -> Result<Predicate> {
        self.reduce_leaf(reference, predicate)
    }

    fn not_null(
        &mut self,
        reference: &BoundReference,
        predicate: &BoundPredicate,
    ) -> Result<Predicate> {
        self.reduce_leaf(reference, predicate)
    }

    fn is_nan(
        &mut self,
        reference: &BoundReference,
        predicate: &BoundPredicate,
    ) -> Result<Predicate> {
        self.reduce_leaf(reference, predicate)
    }

    fn not_nan(
        &mut self,
        reference: &BoundReference,
        predicate: &BoundPredicate,
    ) -> Result<Predicate> {
        self.reduce_leaf(reference, predicate)
    }

    fn less_than(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        predicate: &BoundPredicate,
    ) -> Result<Predicate> {
        self.reduce_leaf(reference, predicate)
    }

    fn less_than_or_eq(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        predicate: &BoundPredicate,
    ) -> Result<Predicate> {
        self.reduce_leaf(reference, predicate)
    }

    fn greater_than(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        predicate: &BoundPredicate,
    ) -> Result<Predicate> {
        self.reduce_leaf(reference, predicate)
    }

    fn greater_than_or_eq(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        predicate: &BoundPredicate,
    ) -> Result<Predicate> {
        self.reduce_leaf(reference, predicate)
    }

    fn eq(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        predicate: &BoundPredicate,
    ) -> Result<Predicate> {
        self.reduce_leaf(reference, predicate)
    }

    fn not_eq(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        predicate: &BoundPredicate,
    ) -> Result<Predicate> {
        self.reduce_leaf(reference, predicate)
    }

    fn starts_with(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        predicate: &BoundPredicate,
    ) -> Result<Predicate> {
        self.reduce_leaf(reference, predicate)
    }

    fn not_starts_with(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        predicate: &BoundPredicate,
    ) -> Result<Predicate> {
        self.reduce_leaf(reference, predicate)
    }

    fn r#in(
        &mut self,
        reference: &BoundReference,
        _literals: &FnvHashSet<Datum>,
        predicate: &BoundPredicate,
    ) -> Result<Predicate> {
        self.reduce_leaf(reference, predicate)
    }

    fn not_in(
        &mut self,
        reference: &BoundReference,
        _literals: &FnvHashSet<Datum>,
        predicate: &BoundPredicate,
    ) -> Result<Predicate> {
        self.reduce_leaf(reference, predicate)
    }
}

/// Reconstructs an unbound [`Predicate`] from a bound one — used for the leaf
/// "keep the original predicate" residual case and for the unpartitioned
/// evaluator. Logical nodes recurse; leaves rebuild the unbound reference from
/// the bound field's name (partition-source columns are top-level fields, so the
/// field name is the unbound reference name).
fn bound_to_unbound(predicate: &BoundPredicate) -> Result<Predicate> {
    Ok(match predicate {
        BoundPredicate::AlwaysTrue => Predicate::AlwaysTrue,
        BoundPredicate::AlwaysFalse => Predicate::AlwaysFalse,
        BoundPredicate::And(expr) => {
            let [left, right] = expr.inputs();
            bound_to_unbound(left)?.and(bound_to_unbound(right)?)
        }
        BoundPredicate::Or(expr) => {
            let [left, right] = expr.inputs();
            bound_to_unbound(left)?.or(bound_to_unbound(right)?)
        }
        BoundPredicate::Not(expr) => {
            let [inner] = expr.inputs();
            simplifying_not(bound_to_unbound(inner)?)
        }
        BoundPredicate::Unary(expr) => Predicate::Unary(UnaryExpression::new(
            expr.op(),
            unbound_reference(expr.term()),
        )),
        BoundPredicate::Binary(expr) => Predicate::Binary(BinaryExpression::new(
            expr.op(),
            unbound_reference(expr.term()),
            expr.literal().clone(),
        )),
        BoundPredicate::Set(expr) => Predicate::Set(SetExpression::new(
            expr.op(),
            unbound_reference(expr.term()),
            expr.literals().clone(),
        )),
    })
}

/// Builds an unbound [`Reference`] from a bound reference's field name.
fn unbound_reference(reference: &BoundReference) -> Reference {
    Reference::new(reference.field().name.clone())
}

/// Negates a residual the way Java `Expressions.not` does: `not(true)=false`,
/// `not(false)=true`, `not(not(x))=x`, otherwise wrap in `Not`. The `Predicate`
/// `!` operator does NOT simplify constants, so `ResidualVisitor.not` must.
fn simplifying_not(inner: Predicate) -> Predicate {
    match inner {
        Predicate::AlwaysTrue => Predicate::AlwaysFalse,
        Predicate::AlwaysFalse => Predicate::AlwaysTrue,
        Predicate::Not(expr) => {
            let [child] = expr.inputs();
            child.clone()
        }
        other => !other,
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Not;
    use std::sync::Arc;

    use super::*;
    use crate::spec::{
        Literal, NestedField, PartitionSpec, PrimitiveLiteral, PrimitiveType, Transform, Type,
        UnboundPartitionField,
    };

    /// A schema with a `ts` timestamp column (id 1), an `id` int column (id 2),
    /// and a `name` string column (id 3).
    fn day_example_schema() -> SchemaRef {
        Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        1,
                        "ts",
                        Type::Primitive(PrimitiveType::Timestamp),
                    )),
                    Arc::new(NestedField::required(
                        2,
                        "id",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    Arc::new(NestedField::optional(
                        3,
                        "name",
                        Type::Primitive(PrimitiveType::String),
                    )),
                ])
                .build()
                .expect("schema builds"),
        )
    }

    /// `PARTITIONED BY day(ts)` over [`day_example_schema`].
    fn day_partition_spec(schema: SchemaRef) -> PartitionSpecRef {
        Arc::new(
            PartitionSpec::builder(schema)
                .with_spec_id(1)
                .add_unbound_field(
                    UnboundPartitionField::builder()
                        .source_id(1)
                        .name("ts_day".to_string())
                        .field_id(1000)
                        .transform(Transform::Day)
                        .build(),
                )
                .expect("add day field")
                .build()
                .expect("spec builds"),
        )
    }

    /// Microseconds since the Unix epoch for the given UTC date-time. Used to
    /// build deterministic timestamp literals for the day-example tests.
    fn micros(datetime: &str) -> i64 {
        match Datum::timestamp_from_str(datetime)
            .expect("valid timestamp")
            .literal()
        {
            PrimitiveLiteral::Long(value) => *value,
            other => panic!("expected a Long timestamp literal, got {other:?}"),
        }
    }

    /// The day-partition value (days since epoch) for a UTC date string, as the
    /// partition `Struct` the evaluator receives.
    fn day_partition(date: &str) -> Struct {
        let day_value = match Datum::date_from_str(date).expect("valid date").literal() {
            PrimitiveLiteral::Int(value) => *value,
            other => panic!("expected an Int date literal, got {other:?}"),
        };
        Struct::from_iter([Some(Literal::date(day_value))])
    }

    /// Builds the bound `ts >= a AND ts <= b` filter from the day-example javadoc.
    fn ts_between_filter(schema: SchemaRef, a: &str, b: &str) -> BoundPredicate {
        Reference::new("ts")
            .greater_than_or_equal_to(Datum::timestamp_micros(micros(a)))
            .and(Reference::new("ts").less_than_or_equal_to(Datum::timestamp_micros(micros(b))))
            .bind(schema, true)
            .expect("filter binds")
    }

    // ---- The Javadoc day(utc_timestamp) worked example: 4 residual cases ----

    #[test]
    fn test_day_example_partition_strictly_between_bounds_reduces_to_always_true() {
        // d > day(a) and d < day(b) => residual is always true.
        let schema = day_example_schema();
        let spec = day_partition_spec(schema.clone());
        let filter =
            ts_between_filter(schema.clone(), "2021-01-01T10:00:00", "2021-01-31T10:00:00");
        let evaluator = ResidualEvaluator::of(spec, &schema, filter, true).unwrap();

        let residual = evaluator
            .residual_for(&day_partition("2021-01-15"))
            .unwrap();
        assert_eq!(residual, Predicate::AlwaysTrue);
    }

    #[test]
    fn test_day_example_partition_equals_lower_bound_keeps_lower_predicate_only() {
        // d == day(a) and d != day(b) => residual is ts >= a (lower kept, upper dropped).
        let schema = day_example_schema();
        let spec = day_partition_spec(schema.clone());
        let filter =
            ts_between_filter(schema.clone(), "2021-01-01T10:00:00", "2021-01-31T10:00:00");
        let evaluator = ResidualEvaluator::of(spec, &schema, filter, true).unwrap();

        let residual = evaluator
            .residual_for(&day_partition("2021-01-01"))
            .unwrap();

        let expected = Reference::new("ts")
            .greater_than_or_equal_to(Datum::timestamp_micros(micros("2021-01-01T10:00:00")));
        assert_eq!(residual, expected);
    }

    #[test]
    fn test_day_example_partition_equals_upper_bound_keeps_upper_predicate_only() {
        // d == day(b) and d != day(a) => residual is ts <= b (upper kept, lower dropped).
        let schema = day_example_schema();
        let spec = day_partition_spec(schema.clone());
        let filter =
            ts_between_filter(schema.clone(), "2021-01-01T10:00:00", "2021-01-31T10:00:00");
        let evaluator = ResidualEvaluator::of(spec, &schema, filter, true).unwrap();

        let residual = evaluator
            .residual_for(&day_partition("2021-01-31"))
            .unwrap();

        let expected = Reference::new("ts")
            .less_than_or_equal_to(Datum::timestamp_micros(micros("2021-01-31T10:00:00")));
        assert_eq!(residual, expected);
    }

    #[test]
    fn test_day_example_partition_equals_both_bounds_keeps_both_predicates() {
        // d == day(a) == day(b) => residual is ts >= a AND ts <= b (both kept).
        let schema = day_example_schema();
        let spec = day_partition_spec(schema.clone());
        let filter =
            ts_between_filter(schema.clone(), "2021-01-10T08:00:00", "2021-01-10T20:00:00");
        let evaluator = ResidualEvaluator::of(spec, &schema, filter, true).unwrap();

        let residual = evaluator
            .residual_for(&day_partition("2021-01-10"))
            .unwrap();

        let expected = Reference::new("ts")
            .greater_than_or_equal_to(Datum::timestamp_micros(micros("2021-01-10T08:00:00")))
            .and(
                Reference::new("ts")
                    .less_than_or_equal_to(Datum::timestamp_micros(micros("2021-01-10T20:00:00"))),
            );
        assert_eq!(residual, expected);
    }

    // ---- Identity partition ----

    fn identity_schema() -> SchemaRef {
        Arc::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "category",
                    Type::Primitive(PrimitiveType::Int),
                ))])
                .build()
                .expect("schema builds"),
        )
    }

    fn identity_spec(schema: SchemaRef) -> PartitionSpecRef {
        Arc::new(
            PartitionSpec::builder(schema)
                .with_spec_id(1)
                .add_unbound_field(
                    UnboundPartitionField::builder()
                        .source_id(1)
                        .name("category".to_string())
                        .field_id(1000)
                        .transform(Transform::Identity)
                        .build(),
                )
                .expect("add identity field")
                .build()
                .expect("spec builds"),
        )
    }

    #[test]
    fn test_identity_partition_eq_matching_value_reduces_to_always_true() {
        let schema = identity_schema();
        let spec = identity_spec(schema.clone());
        let filter = Reference::new("category")
            .equal_to(Datum::int(5))
            .bind(schema.clone(), true)
            .unwrap();
        let evaluator = ResidualEvaluator::of(spec, &schema, filter, true).unwrap();

        let residual = evaluator
            .residual_for(&Struct::from_iter([Some(Literal::int(5))]))
            .unwrap();
        assert_eq!(residual, Predicate::AlwaysTrue);
    }

    #[test]
    fn test_identity_partition_eq_non_matching_value_reduces_to_always_false() {
        let schema = identity_schema();
        let spec = identity_spec(schema.clone());
        let filter = Reference::new("category")
            .equal_to(Datum::int(5))
            .bind(schema.clone(), true)
            .unwrap();
        let evaluator = ResidualEvaluator::of(spec, &schema, filter, true).unwrap();

        let residual = evaluator
            .residual_for(&Struct::from_iter([Some(Literal::int(7))]))
            .unwrap();
        assert_eq!(residual, Predicate::AlwaysFalse);
    }

    // ---- Bucket partition (non-invertible) ----

    #[test]
    fn test_bucket_partition_keeps_predicate_unchanged() {
        use crate::transform::create_transform_function;

        let schema = identity_schema();
        let spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(1)
                .add_unbound_field(
                    UnboundPartitionField::builder()
                        .source_id(1)
                        .name("category_bucket".to_string())
                        .field_id(1000)
                        .transform(Transform::Bucket(16))
                        .build(),
                )
                .expect("add bucket field")
                .build()
                .expect("spec builds"),
        );
        let filter = Reference::new("category")
            .equal_to(Datum::int(5))
            .bind(schema.clone(), true)
            .unwrap();
        let evaluator = ResidualEvaluator::of(spec, &schema, filter, true).unwrap();

        // Use the partition that an `category = 5` row actually lands in:
        // bucket(category) == bucket(5). The inclusive projection is then TRUE
        // (so it does not reduce to false) and bucket has no strict projection
        // for `eq` (non-invertible), so the original predicate survives — Java
        // keeps the predicate because the partition value cannot decide it.
        let bucket_value = match create_transform_function(&Transform::Bucket(16))
            .unwrap()
            .transform_literal(&Datum::int(5))
            .unwrap()
            .expect("bucket of 5")
            .literal()
        {
            PrimitiveLiteral::Int(value) => *value,
            other => panic!("expected an Int bucket literal, got {other:?}"),
        };

        let residual = evaluator
            .residual_for(&Struct::from_iter([Some(Literal::int(bucket_value))]))
            .unwrap();
        let expected = Reference::new("category").equal_to(Datum::int(5));
        assert_eq!(residual, expected);
    }

    // ---- Predicate on a non-partition column ----

    #[test]
    fn test_predicate_on_non_partition_column_is_kept() {
        let schema = day_example_schema();
        let spec = day_partition_spec(schema.clone());
        // `id` (column 2) is not a partition source column.
        let filter = Reference::new("id")
            .greater_than(Datum::int(100))
            .bind(schema.clone(), true)
            .unwrap();
        let evaluator = ResidualEvaluator::of(spec, &schema, filter, true).unwrap();

        let residual = evaluator
            .residual_for(&day_partition("2021-01-01"))
            .unwrap();
        let expected = Reference::new("id").greater_than(Datum::int(100));
        assert_eq!(residual, expected);
    }

    // ---- Unpartitioned spec ----

    #[test]
    fn test_unpartitioned_spec_returns_filter_verbatim() {
        let schema = day_example_schema();
        let filter = Reference::new("id")
            .greater_than(Datum::int(100))
            .and(
                Reference::new("ts")
                    .less_than_or_equal_to(Datum::timestamp_micros(micros("2021-01-31T10:00:00"))),
            )
            .bind(schema.clone(), true)
            .unwrap();
        // An empty spec degrades to unpartitioned via `of`.
        let empty_spec = Arc::new(PartitionSpec::unpartition_spec());
        let evaluator = ResidualEvaluator::of(empty_spec, &schema, filter, true).unwrap();

        let residual = evaluator
            .residual_for(&Struct::from_iter(Vec::<Option<Literal>>::new()))
            .unwrap();
        let expected = Reference::new("id").greater_than(Datum::int(100)).and(
            Reference::new("ts")
                .less_than_or_equal_to(Datum::timestamp_micros(micros("2021-01-31T10:00:00"))),
        );
        assert_eq!(residual, expected);
    }

    #[test]
    fn test_unpartitioned_constructor_returns_filter_verbatim() {
        let schema = day_example_schema();
        let filter = Reference::new("id")
            .greater_than(Datum::int(100))
            .bind(schema.clone(), true)
            .unwrap();
        let evaluator = ResidualEvaluator::unpartitioned(filter);

        let residual = evaluator
            .residual_for(&Struct::from_iter(Vec::<Option<Literal>>::new()))
            .unwrap();
        assert_eq!(residual, Reference::new("id").greater_than(Datum::int(100)));
    }

    #[test]
    fn test_unpartitioned_spec_round_trips_a_not_filter_keeping_the_negation() {
        // The unpartitioned path returns the filter verbatim through
        // `bound_to_unbound`, which is the ONLY caller that reaches the And/Or/Not
        // *logical* arms (the partitioned path decomposes those before the leaf
        // methods). Pin the `Not` arm: a `NOT(id > 100)` filter must come back with
        // the negation intact, not dropped to `id > 100`.
        let schema = day_example_schema();
        let filter = Reference::new("id")
            .greater_than(Datum::int(100))
            .not()
            .bind(schema.clone(), true)
            .unwrap();
        let evaluator = ResidualEvaluator::unpartitioned(filter);

        let residual = evaluator
            .residual_for(&Struct::from_iter(Vec::<Option<Literal>>::new()))
            .unwrap();
        assert_eq!(
            residual,
            Reference::new("id").greater_than(Datum::int(100)).not()
        );
    }

    // ---- AlwaysTrue / AlwaysFalse filters ----

    #[test]
    fn test_always_true_filter_passes_through() {
        let schema = day_example_schema();
        let spec = day_partition_spec(schema.clone());
        let filter = Predicate::AlwaysTrue.bind(schema.clone(), true).unwrap();
        let evaluator = ResidualEvaluator::of(spec, &schema, filter, true).unwrap();

        let residual = evaluator
            .residual_for(&day_partition("2021-01-01"))
            .unwrap();
        assert_eq!(residual, Predicate::AlwaysTrue);
    }

    #[test]
    fn test_always_false_filter_passes_through() {
        let schema = day_example_schema();
        let spec = day_partition_spec(schema.clone());
        let filter = Predicate::AlwaysFalse.bind(schema.clone(), true).unwrap();
        let evaluator = ResidualEvaluator::of(spec, &schema, filter, true).unwrap();

        let residual = evaluator
            .residual_for(&day_partition("2021-01-01"))
            .unwrap();
        assert_eq!(residual, Predicate::AlwaysFalse);
    }

    // ---- Mixed reducible + non-reducible leaves under and / or / not ----

    #[test]
    fn test_and_of_reducible_and_non_reducible_keeps_only_the_non_reducible() {
        // (category == 5) AND (other_predicate_on_partition is true) — here the
        // identity partition leaf reduces to AlwaysTrue and the non-partition leaf
        // survives; AND with AlwaysTrue simplifies to the survivor.
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        1,
                        "category",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    Arc::new(NestedField::required(
                        2,
                        "id",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                ])
                .build()
                .unwrap(),
        );
        let spec = identity_spec_with_source(schema.clone());
        let filter = Reference::new("category")
            .equal_to(Datum::int(5))
            .and(Reference::new("id").greater_than(Datum::int(100)))
            .bind(schema.clone(), true)
            .unwrap();
        let evaluator = ResidualEvaluator::of(spec, &schema, filter, true).unwrap();

        let residual = evaluator
            .residual_for(&Struct::from_iter([Some(Literal::int(5))]))
            .unwrap();
        // category==5 → AlwaysTrue; AND AlwaysTrue id>100 → id>100.
        assert_eq!(residual, Reference::new("id").greater_than(Datum::int(100)));
    }

    #[test]
    fn test_and_short_circuits_to_false_when_partition_excludes_the_partition_leaf() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        1,
                        "category",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    Arc::new(NestedField::required(
                        2,
                        "id",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                ])
                .build()
                .unwrap(),
        );
        let spec = identity_spec_with_source(schema.clone());
        let filter = Reference::new("category")
            .equal_to(Datum::int(5))
            .and(Reference::new("id").greater_than(Datum::int(100)))
            .bind(schema.clone(), true)
            .unwrap();
        let evaluator = ResidualEvaluator::of(spec, &schema, filter, true).unwrap();

        // partition category=7 → the partition leaf is AlwaysFalse → AND is AlwaysFalse.
        let residual = evaluator
            .residual_for(&Struct::from_iter([Some(Literal::int(7))]))
            .unwrap();
        assert_eq!(residual, Predicate::AlwaysFalse);
    }

    #[test]
    fn test_or_of_reducible_true_and_non_reducible_short_circuits_to_true() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        1,
                        "category",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    Arc::new(NestedField::required(
                        2,
                        "id",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                ])
                .build()
                .unwrap(),
        );
        let spec = identity_spec_with_source(schema.clone());
        let filter = Reference::new("category")
            .equal_to(Datum::int(5))
            .or(Reference::new("id").greater_than(Datum::int(100)))
            .bind(schema.clone(), true)
            .unwrap();
        let evaluator = ResidualEvaluator::of(spec, &schema, filter, true).unwrap();

        // category=5 → AlwaysTrue; OR anything → AlwaysTrue.
        let residual = evaluator
            .residual_for(&Struct::from_iter([Some(Literal::int(5))]))
            .unwrap();
        assert_eq!(residual, Predicate::AlwaysTrue);
    }

    /// An identity spec over `category` (id 1) for a 2-column schema.
    fn identity_spec_with_source(schema: SchemaRef) -> PartitionSpecRef {
        Arc::new(
            PartitionSpec::builder(schema)
                .with_spec_id(1)
                .add_unbound_field(
                    UnboundPartitionField::builder()
                        .source_id(1)
                        .name("category".to_string())
                        .field_id(1000)
                        .transform(Transform::Identity)
                        .build(),
                )
                .expect("add identity field")
                .build()
                .expect("spec builds"),
        )
    }

    // ---- not(...) over a partition leaf ----

    #[test]
    fn test_not_over_partition_leaf_negates_the_reduced_constant() {
        let schema = identity_schema();
        let spec = identity_spec(schema.clone());
        // NOT(category == 5): the inner leaf reduces, then `not` negates the constant.
        let filter = Reference::new("category")
            .equal_to(Datum::int(5))
            .not()
            .bind(schema.clone(), true)
            .unwrap();
        let evaluator = ResidualEvaluator::of(spec, &schema, filter, true).unwrap();

        // partition category=5: inner is AlwaysTrue → NOT → AlwaysFalse.
        let residual_match = evaluator
            .residual_for(&Struct::from_iter([Some(Literal::int(5))]))
            .unwrap();
        assert_eq!(residual_match, Predicate::AlwaysFalse);

        // partition category=7: inner is AlwaysFalse → NOT → AlwaysTrue.
        let residual_miss = evaluator
            .residual_for(&Struct::from_iter([Some(Literal::int(7))]))
            .unwrap();
        assert_eq!(residual_miss, Predicate::AlwaysTrue);
    }

    // ---- Truncate partition (reviewer-added: pin a non-day/identity/bucket class) ----

    /// A schema with a single required `amount` int column (id 1).
    fn amount_schema() -> SchemaRef {
        Arc::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "amount",
                    Type::Primitive(PrimitiveType::Int),
                ))])
                .build()
                .expect("schema builds"),
        )
    }

    /// `PARTITIONED BY truncate(amount, 10)` over [`amount_schema`].
    fn truncate_spec(schema: SchemaRef) -> PartitionSpecRef {
        Arc::new(
            PartitionSpec::builder(schema)
                .with_spec_id(1)
                .add_unbound_field(
                    UnboundPartitionField::builder()
                        .source_id(1)
                        .name("amount_trunc".to_string())
                        .field_id(1000)
                        .transform(Transform::Truncate(10))
                        .build(),
                )
                .expect("add truncate field")
                .build()
                .expect("spec builds"),
        )
    }

    #[test]
    fn test_truncate_partition_reduces_all_three_ways() {
        // filter: amount >= 15, partitioned by truncate(amount, 10).
        let schema = amount_schema();
        let spec = truncate_spec(schema.clone());
        let filter = Reference::new("amount")
            .greater_than_or_equal_to(Datum::int(15))
            .bind(schema.clone(), true)
            .unwrap();
        let evaluator = ResidualEvaluator::of(spec, &schema, filter, true).unwrap();

        // Partition bucket truncate=10 holds amounts 10..=19, which straddles 15:
        // neither strict nor inclusive is conclusive, so the predicate is kept.
        let straddling = evaluator
            .residual_for(&Struct::from_iter([Some(Literal::int(10))]))
            .unwrap();
        assert_eq!(
            straddling,
            Reference::new("amount").greater_than_or_equal_to(Datum::int(15))
        );

        // Partition truncate=20 holds amounts 20..=29, all >= 15: strict projection
        // is true → residual AlwaysTrue.
        let all_match = evaluator
            .residual_for(&Struct::from_iter([Some(Literal::int(20))]))
            .unwrap();
        assert_eq!(all_match, Predicate::AlwaysTrue);

        // Partition truncate=0 holds amounts 0..=9, all < 15: inclusive projection
        // is false → residual AlwaysFalse.
        let none_match = evaluator
            .residual_for(&Struct::from_iter([Some(Literal::int(0))]))
            .unwrap();
        assert_eq!(none_match, Predicate::AlwaysFalse);
    }

    // ---- Temporal (year) partition (reviewer-added) ----

    /// The `year(ts)` partition value (years since 1970) for a UTC date-time.
    fn year_partition_value(datetime: &str) -> i32 {
        use crate::transform::create_transform_function;
        match create_transform_function(&Transform::Year)
            .unwrap()
            .transform_literal(&Datum::timestamp_micros(micros(datetime)))
            .unwrap()
            .expect("year of timestamp")
            .literal()
        {
            PrimitiveLiteral::Int(value) => *value,
            other => panic!("expected an Int year literal, got {other:?}"),
        }
    }

    #[test]
    fn test_year_partition_keeps_both_bounds_inside_the_year_and_excludes_other_years() {
        // filter: ts >= 2021-06-01 AND ts <= 2021-06-30 (both inside year 2021),
        // partitioned by year(ts).
        let schema = day_example_schema();
        let spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(1)
                .add_unbound_field(
                    UnboundPartitionField::builder()
                        .source_id(1)
                        .name("ts_year".to_string())
                        .field_id(1000)
                        .transform(Transform::Year)
                        .build(),
                )
                .expect("add year field")
                .build()
                .expect("spec builds"),
        );
        let filter =
            ts_between_filter(schema.clone(), "2021-06-01T00:00:00", "2021-06-30T00:00:00");
        let evaluator = ResidualEvaluator::of(spec, &schema, filter, true).unwrap();

        // Partition year=2021 contains the whole [a, b] range but the year value
        // alone cannot decide either bound (a row in 2021 could be before a or
        // after b), so BOTH predicates are kept — the temporal analogue of the
        // day-example "d == day(a) == day(b)" case.
        let in_year = evaluator
            .residual_for(&Struct::from_iter([Some(Literal::int(
                year_partition_value("2021-06-15T00:00:00"),
            ))]))
            .unwrap();
        let expected = Reference::new("ts")
            .greater_than_or_equal_to(Datum::timestamp_micros(micros("2021-06-01T00:00:00")))
            .and(
                Reference::new("ts")
                    .less_than_or_equal_to(Datum::timestamp_micros(micros("2021-06-30T00:00:00"))),
            );
        assert_eq!(in_year, expected);

        // Partition year=2020 is entirely before the range (inclusive projection of
        // `ts <= b` is false there) → AlwaysFalse.
        let before = evaluator
            .residual_for(&Struct::from_iter([Some(Literal::int(
                year_partition_value("2020-06-15T00:00:00"),
            ))]))
            .unwrap();
        assert_eq!(before, Predicate::AlwaysFalse);

        // Partition year=2022 is entirely after the range → AlwaysFalse.
        let after = evaluator
            .residual_for(&Struct::from_iter([Some(Literal::int(
                year_partition_value("2022-06-15T00:00:00"),
            ))]))
            .unwrap();
        assert_eq!(after, Predicate::AlwaysFalse);
    }

    // ---- Void partition (reviewer-added: non-invertible, projects to null) ----

    #[test]
    fn test_void_partition_keeps_predicate_unchanged() {
        // void projects every value to null and has neither an inclusive nor a
        // strict projection (Java `VoidTransform.project`/`projectStrict` return
        // null), so a predicate on a void-partitioned column can never be reduced —
        // it is always kept, with no panic on the null partition value.
        let schema = identity_schema();
        let spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(1)
                .add_unbound_field(
                    UnboundPartitionField::builder()
                        .source_id(1)
                        .name("category_void".to_string())
                        .field_id(1000)
                        .transform(Transform::Void)
                        .build(),
                )
                .expect("add void field")
                .build()
                .expect("spec builds"),
        );
        let filter = Reference::new("category")
            .equal_to(Datum::int(5))
            .bind(schema.clone(), true)
            .unwrap();
        let evaluator = ResidualEvaluator::of(spec, &schema, filter, true).unwrap();

        // The void partition value is always null.
        let residual = evaluator.residual_for(&Struct::from_iter([None])).unwrap();
        assert_eq!(residual, Reference::new("category").equal_to(Datum::int(5)));
    }

    // ---- Leaf reconstruction round-trip (reviewer-added) ----
    //
    // The "keep the original predicate" path rebuilds an unbound `Predicate` from
    // the bound leaf by field name (`bound_to_unbound`). A dropped negation, an
    // operator swap, or a set collapsed to one value would be silent — pin the
    // set (in/not_in) and unary (is_null) shapes for a non-partition column, since
    // the existing kept-predicate tests only cover the binary shape.

    #[test]
    fn test_in_predicate_on_non_partition_column_round_trips_as_same_in() {
        let schema = day_example_schema();
        let spec = day_partition_spec(schema.clone());
        // `id` (column 2) is not a partition source column; an `in (1, 2, 3)` must
        // come back as the same set, same operator, same column.
        let filter = Reference::new("id")
            .is_in([Datum::int(1), Datum::int(2), Datum::int(3)])
            .bind(schema.clone(), true)
            .unwrap();
        let evaluator = ResidualEvaluator::of(spec, &schema, filter, true).unwrap();

        let residual = evaluator
            .residual_for(&day_partition("2021-01-01"))
            .unwrap();
        let expected = Reference::new("id").is_in([Datum::int(1), Datum::int(2), Datum::int(3)]);
        assert_eq!(residual, expected);
    }

    #[test]
    fn test_not_in_predicate_on_non_partition_column_round_trips_as_same_not_in() {
        let schema = day_example_schema();
        let spec = day_partition_spec(schema.clone());
        let filter = Reference::new("id")
            .is_in([Datum::int(1), Datum::int(2)])
            .not()
            .bind(schema.clone(), true)
            .unwrap();
        let evaluator = ResidualEvaluator::of(spec, &schema, filter, true).unwrap();

        let residual = evaluator
            .residual_for(&day_partition("2021-01-01"))
            .unwrap();
        // `not(in (1, 2))` binds to `Not(In {1, 2})` (the binder keeps the `Not`
        // wrapper rather than folding it into a `NotIn` operator). The residual
        // must round-trip that exact shape — the negation kept, the set intact —
        // not drop the `Not` or collapse the set.
        let expected = Reference::new("id")
            .is_in([Datum::int(1), Datum::int(2)])
            .not();
        assert_eq!(residual, expected);
    }

    #[test]
    fn test_is_null_unary_on_non_partition_column_round_trips_as_same_is_null() {
        let schema = day_example_schema();
        let spec = day_partition_spec(schema.clone());
        // `name` (column 3, optional) is not a partition source column.
        let filter = Reference::new("name")
            .is_null()
            .bind(schema.clone(), true)
            .unwrap();
        let evaluator = ResidualEvaluator::of(spec, &schema, filter, true).unwrap();

        let residual = evaluator
            .residual_for(&day_partition("2021-01-01"))
            .unwrap();
        assert_eq!(residual, Reference::new("name").is_null());
    }

    // ---- Multiple partition fields on one source column (reviewer-added) ----

    #[test]
    fn test_multiple_partition_fields_on_one_source_reduce_via_the_conclusive_field() {
        // Java loops over EVERY partition field whose source id matches the
        // predicate's column (`getFieldsBySourceId`). Here `category` is the source
        // of BOTH bucket(category, 16) (non-invertible — never conclusive for `eq`)
        // and identity(category) (conclusive). The loop must skip past the bucket
        // field and let the identity field decide; the existing tests only ever
        // have one partition field per source, so they cannot exercise this loop.
        let schema = identity_schema();
        let spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(1)
                .add_unbound_field(
                    UnboundPartitionField::builder()
                        .source_id(1)
                        .name("category_bucket".to_string())
                        .field_id(1000)
                        .transform(Transform::Bucket(16))
                        .build(),
                )
                .expect("add bucket field")
                .add_unbound_field(
                    UnboundPartitionField::builder()
                        .source_id(1)
                        .name("category_id".to_string())
                        .field_id(1001)
                        .transform(Transform::Identity)
                        .build(),
                )
                .expect("add identity field")
                .build()
                .expect("spec builds"),
        );
        let filter = Reference::new("category")
            .equal_to(Datum::int(5))
            .bind(schema.clone(), true)
            .unwrap();
        let evaluator = ResidualEvaluator::of(spec, &schema, filter, true).unwrap();

        // Partition struct order follows spec field order: [bucket(category), category].
        let bucket_value = {
            use crate::transform::create_transform_function;
            match create_transform_function(&Transform::Bucket(16))
                .unwrap()
                .transform_literal(&Datum::int(5))
                .unwrap()
                .expect("bucket of 5")
                .literal()
            {
                PrimitiveLiteral::Int(value) => *value,
                other => panic!("expected an Int bucket literal, got {other:?}"),
            }
        };

        // bucket(5) + identity=5: the identity field's strict projection is true →
        // the loop reaches it past the inconclusive bucket field → AlwaysTrue.
        let matching = evaluator
            .residual_for(&Struct::from_iter([
                Some(Literal::int(bucket_value)),
                Some(Literal::int(5)),
            ]))
            .unwrap();
        assert_eq!(matching, Predicate::AlwaysTrue);

        // bucket(5) + identity=7: the identity field's inclusive projection is
        // false → AlwaysFalse (again only reachable by continuing the loop).
        let non_matching = evaluator
            .residual_for(&Struct::from_iter([
                Some(Literal::int(bucket_value)),
                Some(Literal::int(7)),
            ]))
            .unwrap();
        assert_eq!(non_matching, Predicate::AlwaysFalse);
    }

    // ---- Mutation guard: strict vs inclusive direction ----
    //
    // The day-example reduction tests are the canonical mutation check. Swapping
    // `strict_project` <-> `project` in `reduce_leaf` flips the headline cases:
    // the "strictly between bounds" case stops reducing to AlwaysTrue (strict was
    // the load-bearing projection there), and the "equals one bound" cases stop
    // dropping the satisfied half. Making `residual_for` ignore the partition
    // (always return the filter) breaks every reduction test above. Both
    // mutations are exercised by the existing assertions; this comment documents
    // the manual mutation runs.
}
