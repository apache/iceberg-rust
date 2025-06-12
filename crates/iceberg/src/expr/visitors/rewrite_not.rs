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

use fnv::FnvHashSet;

use crate::Result;
use crate::expr::visitors::predicate_visitor::PredicateVisitor;
use crate::expr::{
    BinaryExpression, Predicate, PredicateOperator, Reference, SetExpression, UnaryExpression,
};
use crate::spec::Datum;

/// A visitor that rewrites predicates by removing `NOT` predicates and
/// directly negating the inner expressions instead. This applies logical
/// laws (such as De Morgan's laws) to recursively negate and simplify
/// inner expressions within `NOT` predicates.
pub struct RewriteNotVisitor;

impl RewriteNotVisitor {
    /// Creates a new `RewriteNotVisitor`
    pub fn new() -> Self {
        Self
    }
}

impl PredicateVisitor for RewriteNotVisitor {
    type T = Predicate;

    fn always_true(&mut self) -> Result<Self::T> {
        Ok(Predicate::AlwaysTrue)
    }

    fn always_false(&mut self) -> Result<Self::T> {
        Ok(Predicate::AlwaysFalse)
    }

    fn and(&mut self, lhs: Self::T, rhs: Self::T) -> Result<Self::T> {
        Ok(lhs.and(rhs))
    }

    fn or(&mut self, lhs: Self::T, rhs: Self::T) -> Result<Self::T> {
        Ok(lhs.or(rhs))
    }

    fn not(&mut self, inner: Self::T) -> Result<Self::T> {
        // This is the key method: instead of creating a NOT predicate,
        // we directly negate the inner predicate
        Ok(inner.negate())
    }

    fn is_null(&mut self, reference: &Reference, _predicate: &Predicate) -> Result<Self::T> {
        Ok(Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNull,
            reference.clone(),
        )))
    }

    fn not_null(&mut self, reference: &Reference, _predicate: &Predicate) -> Result<Self::T> {
        Ok(Predicate::Unary(UnaryExpression::new(
            PredicateOperator::NotNull,
            reference.clone(),
        )))
    }

    fn is_nan(&mut self, reference: &Reference, _predicate: &Predicate) -> Result<Self::T> {
        Ok(Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNan,
            reference.clone(),
        )))
    }

    fn not_nan(&mut self, reference: &Reference, _predicate: &Predicate) -> Result<Self::T> {
        Ok(Predicate::Unary(UnaryExpression::new(
            PredicateOperator::NotNan,
            reference.clone(),
        )))
    }

    fn less_than(
        &mut self,
        reference: &Reference,
        literal: &Datum,
        _predicate: &Predicate,
    ) -> Result<Self::T> {
        Ok(Predicate::Binary(BinaryExpression::new(
            PredicateOperator::LessThan,
            reference.clone(),
            literal.clone(),
        )))
    }

    fn less_than_or_eq(
        &mut self,
        reference: &Reference,
        literal: &Datum,
        _predicate: &Predicate,
    ) -> Result<Self::T> {
        Ok(Predicate::Binary(BinaryExpression::new(
            PredicateOperator::LessThanOrEq,
            reference.clone(),
            literal.clone(),
        )))
    }

    fn greater_than(
        &mut self,
        reference: &Reference,
        literal: &Datum,
        _predicate: &Predicate,
    ) -> Result<Self::T> {
        Ok(Predicate::Binary(BinaryExpression::new(
            PredicateOperator::GreaterThan,
            reference.clone(),
            literal.clone(),
        )))
    }

    fn greater_than_or_eq(
        &mut self,
        reference: &Reference,
        literal: &Datum,
        _predicate: &Predicate,
    ) -> Result<Self::T> {
        Ok(Predicate::Binary(BinaryExpression::new(
            PredicateOperator::GreaterThanOrEq,
            reference.clone(),
            literal.clone(),
        )))
    }

    fn eq(
        &mut self,
        reference: &Reference,
        literal: &Datum,
        _predicate: &Predicate,
    ) -> Result<Self::T> {
        Ok(Predicate::Binary(BinaryExpression::new(
            PredicateOperator::Eq,
            reference.clone(),
            literal.clone(),
        )))
    }

    fn not_eq(
        &mut self,
        reference: &Reference,
        literal: &Datum,
        _predicate: &Predicate,
    ) -> Result<Self::T> {
        Ok(Predicate::Binary(BinaryExpression::new(
            PredicateOperator::NotEq,
            reference.clone(),
            literal.clone(),
        )))
    }

    fn starts_with(
        &mut self,
        reference: &Reference,
        literal: &Datum,
        _predicate: &Predicate,
    ) -> Result<Self::T> {
        Ok(Predicate::Binary(BinaryExpression::new(
            PredicateOperator::StartsWith,
            reference.clone(),
            literal.clone(),
        )))
    }

    fn not_starts_with(
        &mut self,
        reference: &Reference,
        literal: &Datum,
        _predicate: &Predicate,
    ) -> Result<Self::T> {
        Ok(Predicate::Binary(BinaryExpression::new(
            PredicateOperator::NotStartsWith,
            reference.clone(),
            literal.clone(),
        )))
    }

    fn r#in(
        &mut self,
        reference: &Reference,
        literals: &FnvHashSet<Datum>,
        _predicate: &Predicate,
    ) -> Result<Self::T> {
        Ok(Predicate::Set(SetExpression::new(
            PredicateOperator::In,
            reference.clone(),
            literals.clone(),
        )))
    }

    fn not_in(
        &mut self,
        reference: &Reference,
        literals: &FnvHashSet<Datum>,
        _predicate: &Predicate,
    ) -> Result<Self::T> {
        Ok(Predicate::Set(SetExpression::new(
            PredicateOperator::NotIn,
            reference.clone(),
            literals.clone(),
        )))
    }
}
