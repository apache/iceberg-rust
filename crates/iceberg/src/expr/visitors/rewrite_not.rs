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
use crate::expr::{Predicate, Reference};
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

    fn is_null(&mut self, _reference: &Reference, predicate: &Predicate) -> Result<Self::T> {
        Ok(predicate.clone())
    }

    fn not_null(&mut self, _reference: &Reference, predicate: &Predicate) -> Result<Self::T> {
        Ok(predicate.clone())
    }

    fn is_nan(&mut self, _reference: &Reference, predicate: &Predicate) -> Result<Self::T> {
        Ok(predicate.clone())
    }

    fn not_nan(&mut self, _reference: &Reference, predicate: &Predicate) -> Result<Self::T> {
        Ok(predicate.clone())
    }

    fn less_than(
        &mut self,
        _reference: &Reference,
        _literal: &Datum,
        predicate: &Predicate,
    ) -> Result<Self::T> {
        Ok(predicate.clone())
    }

    fn less_than_or_eq(
        &mut self,
        _reference: &Reference,
        _literal: &Datum,
        predicate: &Predicate,
    ) -> Result<Self::T> {
        Ok(predicate.clone())
    }

    fn greater_than(
        &mut self,
        _reference: &Reference,
        _literal: &Datum,
        predicate: &Predicate,
    ) -> Result<Self::T> {
        Ok(predicate.clone())
    }

    fn greater_than_or_eq(
        &mut self,
        _reference: &Reference,
        _literal: &Datum,
        predicate: &Predicate,
    ) -> Result<Self::T> {
        Ok(predicate.clone())
    }

    fn eq(
        &mut self,
        _reference: &Reference,
        _literal: &Datum,
        predicate: &Predicate,
    ) -> Result<Self::T> {
        Ok(predicate.clone())
    }

    fn not_eq(
        &mut self,
        _reference: &Reference,
        _literal: &Datum,
        predicate: &Predicate,
    ) -> Result<Self::T> {
        Ok(predicate.clone())
    }

    fn starts_with(
        &mut self,
        _reference: &Reference,
        _literal: &Datum,
        predicate: &Predicate,
    ) -> Result<Self::T> {
        Ok(predicate.clone())
    }

    fn not_starts_with(
        &mut self,
        _reference: &Reference,
        _literal: &Datum,
        predicate: &Predicate,
    ) -> Result<Self::T> {
        Ok(predicate.clone())
    }

    fn r#in(
        &mut self,
        _reference: &Reference,
        _literals: &FnvHashSet<Datum>,
        predicate: &Predicate,
    ) -> Result<Self::T> {
        Ok(predicate.clone())
    }

    fn not_in(
        &mut self,
        _reference: &Reference,
        _literals: &FnvHashSet<Datum>,
        predicate: &Predicate,
    ) -> Result<Self::T> {
        Ok(predicate.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Not;

    use super::*;

    #[test]
    fn test_rewrite_not_deeply_nested() {
        // Test nested expression: not((not((not(ref(name="bar") < 40) and ref(name="bar") < 40)) and ref(name="bar") < 40))
        // Expected rewrite not result: ((bar >= 40) AND (bar < 40)) OR (bar >= 40)
        let complex_expression = Reference::new("bar")
            .less_than(Datum::int(40))
            .not()
            .and(Reference::new("bar").less_than(Datum::int(40)))
            .not()
            .and(Reference::new("bar").less_than(Datum::int(40)))
            .not();

        let expected = Reference::new("bar")
            .greater_than_or_equal_to(Datum::int(40))
            .and(Reference::new("bar").less_than(Datum::int(40)))
            .or(Reference::new("bar").greater_than_or_equal_to(Datum::int(40)));

        let result = complex_expression.rewrite_not();

        assert_eq!(result, expected);

        let result_str = format!("{result}");
        assert_eq!(&result_str, "((bar >= 40) AND (bar < 40)) OR (bar >= 40)");
    }
}
