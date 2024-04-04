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

use crate::expr::visitors::bound_predicate_visitor::{visit, BoundPredicateVisitor, OpLiteral};
use crate::expr::{BoundPredicate, BoundReference, Predicate, PredicateOperator};
use crate::spec::{PartitionField, PartitionSpecRef};
use std::collections::HashMap;
use std::ops::Not;

pub(crate) struct InclusiveProjection {
    partition_spec: PartitionSpecRef,
    cached_parts: HashMap<i32, Vec<PartitionField>>,
}

impl InclusiveProjection {
    pub(crate) fn new(partition_spec: PartitionSpecRef) -> Self {
        Self {
            partition_spec,
            cached_parts: HashMap::new(),
        }
    }

    fn get_parts_for_field_id(&mut self, field_id: i32) -> &Vec<PartitionField> {
        if let std::collections::hash_map::Entry::Vacant(e) = self.cached_parts.entry(field_id) {
            let mut parts: Vec<PartitionField> = vec![];
            for partition_spec_field in &self.partition_spec.fields {
                if partition_spec_field.source_id == field_id {
                    parts.push(partition_spec_field.clone())
                }
            }

            e.insert(parts);
        }

        &self.cached_parts[&field_id]
    }

    pub(crate) fn project(&mut self, predicate: &BoundPredicate) -> crate::Result<Predicate> {
        visit(self, predicate)
    }
}

impl BoundPredicateVisitor for InclusiveProjection {
    type T = Predicate;

    fn always_true(&mut self) -> crate::Result<Self::T> {
        Ok(Predicate::AlwaysTrue)
    }

    fn always_false(&mut self) -> crate::Result<Self::T> {
        Ok(Predicate::AlwaysFalse)
    }

    fn and(&mut self, lhs: Self::T, rhs: Self::T) -> crate::Result<Self::T> {
        Ok(lhs.and(rhs))
    }

    fn or(&mut self, lhs: Self::T, rhs: Self::T) -> crate::Result<Self::T> {
        Ok(lhs.or(rhs))
    }

    fn not(&mut self, inner: Self::T) -> crate::Result<Self::T> {
        Ok(inner.not())
    }

    fn op(
        &mut self,
        _op: PredicateOperator,
        reference: &BoundReference,
        _literal: Option<OpLiteral>,
        predicate: &BoundPredicate,
    ) -> crate::Result<Self::T> {
        let field_id = reference.field().id;

        // This could be made a bit neater if `try_reduce` ever becomes stable
        self.get_parts_for_field_id(field_id)
            .iter()
            .try_fold(Predicate::AlwaysTrue, |res, part| {
                Ok(
                    if let Some(pred_for_part) =
                        part.transform.project(part.name.clone(), predicate)?
                    {
                        if res == Predicate::AlwaysTrue {
                            pred_for_part
                        } else {
                            res.and(pred_for_part)
                        }
                    } else {
                        res
                    },
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use crate::expr::visitors::inclusive_projection::InclusiveProjection;
    use crate::expr::{Bind, Predicate, Reference};
    use crate::spec::{
        Datum, NestedField, PartitionField, PartitionSpec, PrimitiveType, Schema, Transform, Type,
    };
    use std::ops::Not;
    use std::sync::Arc;

    fn build_test_schema() -> Schema {
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "a",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::required(
                    2,
                    "date",
                    Type::Primitive(PrimitiveType::Date),
                )),
            ])
            .build()
            .unwrap();
        schema
    }

    #[test]
    fn test_inclusive_projection_logic_ops() {
        let schema = build_test_schema();

        let partition_spec = PartitionSpec::builder()
            .with_spec_id(1)
            .with_fields(vec![])
            .build()
            .unwrap();

        let arc_schema = Arc::new(schema);
        let arc_partition_spec = Arc::new(partition_spec);

        // this predicate contains only logic operators,
        // AlwaysTrue, and AlwaysFalse.
        let unbound_predicate = Predicate::AlwaysTrue
            .and(Predicate::AlwaysFalse)
            .or(Predicate::AlwaysTrue)
            .not();

        let bound_predicate = unbound_predicate.bind(arc_schema.clone(), false).unwrap();

        // applying InclusiveProjection to bound_predicate
        // should result in the same Predicate as the original
        // `unbound_predicate`, since `InclusiveProjection`
        // simply unbinds logic ops, AlwaysTrue, and AlwaysFalse.
        let mut inclusive_projection = InclusiveProjection::new(arc_partition_spec.clone());
        let result = inclusive_projection.project(&bound_predicate).unwrap();

        assert_eq!(result.to_string(), "FALSE".to_string())
    }

    #[test]
    fn test_inclusive_projection_simple_transform() {
        let schema = build_test_schema();

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

        let arc_schema = Arc::new(schema);
        let arc_partition_spec = Arc::new(partition_spec);

        let unbound_predicate = Reference::new("a").less_than(Datum::int(10));

        let bound_predicate = unbound_predicate.bind(arc_schema.clone(), false).unwrap();

        // applying InclusiveProjection to bound_predicate
        // should result in the same Predicate as the original
        // `unbound_predicate`, since we have just a single partition field,
        // and it has an Identity transform
        let mut inclusive_projection = InclusiveProjection::new(arc_partition_spec.clone());
        let result = inclusive_projection.project(&bound_predicate).unwrap();

        let expected = "a < 10".to_string();

        assert_eq!(result.to_string(), expected)
    }

    #[test]
    fn test_inclusive_projection_repeated_field_transform() {
        let schema = build_test_schema();

        let partition_spec = PartitionSpec::builder()
            .with_spec_id(1)
            .with_fields(vec![
                PartitionField::builder()
                    .source_id(2)
                    .name("year".to_string())
                    .field_id(2)
                    .transform(Transform::Year)
                    .build(),
                PartitionField::builder()
                    .source_id(2)
                    .name("month".to_string())
                    .field_id(2)
                    .transform(Transform::Month)
                    .build(),
                PartitionField::builder()
                    .source_id(2)
                    .name("day".to_string())
                    .field_id(2)
                    .transform(Transform::Day)
                    .build(),
            ])
            .build()
            .unwrap();

        let arc_schema = Arc::new(schema);
        let arc_partition_spec = Arc::new(partition_spec);

        let unbound_predicate =
            Reference::new("date").less_than(Datum::date_from_str("2024-01-01").unwrap());

        let bound_predicate = unbound_predicate.bind(arc_schema.clone(), false).unwrap();

        // applying InclusiveProjection to bound_predicate
        // should result in a predicate that correctly handles
        // year, month and date
        let mut inclusive_projection = InclusiveProjection::new(arc_partition_spec.clone());
        let result = inclusive_projection.project(&bound_predicate).unwrap();

        let expected = "((year <= 53) AND (month <= 647)) AND (day <= 19722)".to_string();

        assert_eq!(result.to_string(), expected);
    }
}
