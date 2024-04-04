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

use crate::expr::{BoundPredicate, BoundReference, PredicateOperator};
use crate::spec::Datum;
use crate::Result;
use fnv::FnvHashSet;

pub(crate) enum OpLiteral<'a> {
    Single(&'a Datum),
    Set(&'a FnvHashSet<Datum>),
}

/// A visitor for [`BoundPredicate`]s. Visits in post-order.
pub trait BoundPredicateVisitor {
    /// The return type of this visitor
    type T;

    /// Called after an `AlwaysTrue` predicate is visited
    fn always_true(&mut self) -> Result<Self::T>;

    /// Called after an `AlwaysFalse` predicate is visited
    fn always_false(&mut self) -> Result<Self::T>;

    /// Called after an `And` predicate is visited
    fn and(&mut self, lhs: Self::T, rhs: Self::T) -> Result<Self::T>;

    /// Called after an `Or` predicate is visited
    fn or(&mut self, lhs: Self::T, rhs: Self::T) -> Result<Self::T>;

    /// Called after a `Not` predicate is visited
    fn not(&mut self, inner: Self::T) -> Result<Self::T>;

    /// Called after visiting a UnaryPredicate, BinaryPredicate,
    /// or SetPredicate. Passes the predicate's operator in all cases,
    /// as well as the term and literals in the case of binary and set
    /// predicates.
    fn op(
        &mut self,
        op: PredicateOperator,
        reference: &BoundReference,
        literal: Option<OpLiteral>,
        predicate: &BoundPredicate,
    ) -> Result<Self::T>;
}

/// Visits a [`BoundPredicate`] with the provided visitor,
/// in post-order
pub(crate) fn visit<V: BoundPredicateVisitor>(
    visitor: &mut V,
    predicate: &BoundPredicate,
) -> Result<V::T> {
    match predicate {
        BoundPredicate::AlwaysTrue => visitor.always_true(),
        BoundPredicate::AlwaysFalse => visitor.always_false(),
        BoundPredicate::And(expr) => {
            let [left_pred, right_pred] = expr.inputs();

            let left_result = visit(visitor, left_pred)?;
            let right_result = visit(visitor, right_pred)?;

            visitor.and(left_result, right_result)
        }
        BoundPredicate::Or(expr) => {
            let [left_pred, right_pred] = expr.inputs();

            let left_result = visit(visitor, left_pred)?;
            let right_result = visit(visitor, right_pred)?;

            visitor.or(left_result, right_result)
        }
        BoundPredicate::Not(expr) => {
            let [inner_pred] = expr.inputs();

            let inner_result = visit(visitor, inner_pred)?;

            visitor.not(inner_result)
        }
        BoundPredicate::Unary(expr) => visitor.op(expr.op(), expr.term(), None, predicate),
        BoundPredicate::Binary(expr) => visitor.op(
            expr.op(),
            expr.term(),
            Some(OpLiteral::Single(expr.literal())),
            predicate,
        ),
        BoundPredicate::Set(expr) => visitor.op(
            expr.op(),
            expr.term(),
            Some(OpLiteral::Set(expr.literals())),
            predicate,
        ),
    }
}

#[cfg(test)]
mod tests {
    use crate::expr::visitors::bound_predicate_visitor::{visit, BoundPredicateVisitor, OpLiteral};
    use crate::expr::{
        BinaryExpression, Bind, BoundPredicate, BoundReference, Predicate, PredicateOperator,
        Reference,
    };
    use crate::spec::{Datum, NestedField, PrimitiveType, Schema, SchemaRef, Type};
    use std::ops::Not;
    use std::sync::Arc;

    struct TestEvaluator {}
    impl BoundPredicateVisitor for TestEvaluator {
        type T = bool;

        fn always_true(&mut self) -> crate::Result<Self::T> {
            Ok(true)
        }

        fn always_false(&mut self) -> crate::Result<Self::T> {
            Ok(false)
        }

        fn and(&mut self, lhs: Self::T, rhs: Self::T) -> crate::Result<Self::T> {
            Ok(lhs && rhs)
        }

        fn or(&mut self, lhs: Self::T, rhs: Self::T) -> crate::Result<Self::T> {
            Ok(lhs || rhs)
        }

        fn not(&mut self, inner: Self::T) -> crate::Result<Self::T> {
            Ok(!inner)
        }

        fn op(
            &mut self,
            op: PredicateOperator,
            _reference: &BoundReference,
            _literal: Option<OpLiteral>,
            _predicate: &BoundPredicate,
        ) -> crate::Result<Self::T> {
            Ok(match op {
                PredicateOperator::IsNull => true,
                PredicateOperator::NotNull => false,
                PredicateOperator::IsNan => true,
                PredicateOperator::NotNan => false,
                PredicateOperator::LessThan => true,
                PredicateOperator::LessThanOrEq => false,
                PredicateOperator::GreaterThan => true,
                PredicateOperator::GreaterThanOrEq => false,
                PredicateOperator::Eq => true,
                PredicateOperator::NotEq => false,
                PredicateOperator::StartsWith => true,
                PredicateOperator::NotStartsWith => false,
                PredicateOperator::In => false,
                PredicateOperator::NotIn => true,
            })
        }
    }

    fn create_test_schema() -> SchemaRef {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "a",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .unwrap();

        let schema_arc = Arc::new(schema);
        schema_arc.clone()
    }

    #[test]
    fn test_always_true() {
        let predicate = Predicate::AlwaysTrue;
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = visit(&mut test_evaluator, &bound_predicate);

        assert!(result.unwrap());
    }

    #[test]
    fn test_always_false() {
        let predicate = Predicate::AlwaysFalse;
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = visit(&mut test_evaluator, &bound_predicate);

        assert!(!result.unwrap());
    }

    #[test]
    fn test_logical_and() {
        let predicate = Predicate::AlwaysTrue.and(Predicate::AlwaysFalse);
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = visit(&mut test_evaluator, &bound_predicate);

        assert!(!result.unwrap());

        let predicate = Predicate::AlwaysFalse.and(Predicate::AlwaysFalse);
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = visit(&mut test_evaluator, &bound_predicate);

        assert!(!result.unwrap());

        let predicate = Predicate::AlwaysTrue.and(Predicate::AlwaysTrue);
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = visit(&mut test_evaluator, &bound_predicate);

        assert!(result.unwrap());
    }

    #[test]
    fn test_logical_or() {
        let predicate = Predicate::AlwaysTrue.or(Predicate::AlwaysFalse);
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = visit(&mut test_evaluator, &bound_predicate);

        assert!(result.unwrap());

        let predicate = Predicate::AlwaysFalse.or(Predicate::AlwaysFalse);
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = visit(&mut test_evaluator, &bound_predicate);

        assert!(!result.unwrap());

        let predicate = Predicate::AlwaysTrue.or(Predicate::AlwaysTrue);
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = visit(&mut test_evaluator, &bound_predicate);

        assert!(result.unwrap());
    }

    #[test]
    fn test_not() {
        let predicate = Predicate::AlwaysFalse.not();
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = visit(&mut test_evaluator, &bound_predicate);

        assert!(result.unwrap());

        let predicate = Predicate::AlwaysTrue.not();
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = visit(&mut test_evaluator, &bound_predicate);

        assert!(!result.unwrap());
    }

    #[test]
    fn test_op() {
        let predicate = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::LessThan,
            Reference::new("a"),
            Datum::int(10),
        ));
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = visit(&mut test_evaluator, &bound_predicate);

        assert!(result.unwrap());

        let predicate = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::LessThanOrEq,
            Reference::new("a"),
            Datum::int(10),
        ));
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = visit(&mut test_evaluator, &bound_predicate);

        assert!(!result.unwrap());
    }
}
