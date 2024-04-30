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

use crate::{
    expr::{BoundPredicate, BoundReference},
    spec::{DataFile, Datum, Struct},
    Result,
};

use super::bound_predicate_visitor::{visit, BoundPredicateVisitor};

/// Evaluates a [`DataFile`]'s partition [`Struct`] to check
/// if the partition tuples match the given [`BoundPredicate`].
///
/// Use within [`TableScan`] to prune the list of [`DataFile`]s
/// that could potentially match the TableScan's filter.
#[derive(Debug)]
pub(crate) struct ExpressionEvaluator {
    /// The provided partition filter.
    partition_filter: BoundPredicate,
}

impl ExpressionEvaluator {
    /// Creates a new [`ExpressionEvaluator`].
    pub(crate) fn new(partition_filter: BoundPredicate) -> Self {
        Self { partition_filter }
    }

    /// Evaluate this [`ExpressionEvaluator`]'s partition filter against
    /// the provided [`DataFile`]'s partition [`Struct`]. Used by [`TableScan`]
    /// to see if this [`DataFile`] could possible contain data that matches
    /// the scan's filter.
    pub(crate) fn eval(&self, data_file: &DataFile) -> Result<bool> {
        let mut visitor = ExpressionEvaluatorVisitor::new(self, data_file.partition());

        visit(&mut visitor, &self.partition_filter)
    }
}

/// Acts as a visitor for [`ExpressionEvaluator`] to apply
/// evaluation logic to different parts of a data structure,
/// specifically for data file partitions.
#[derive(Debug)]
struct ExpressionEvaluatorVisitor<'a> {
    /// Reference to an [`ExpressionEvaluator`].
    expression_evaluator: &'a ExpressionEvaluator,
    /// Reference to a [`DataFile`]'s partition [`Struct`].
    partition: &'a Struct,
}

impl<'a> ExpressionEvaluatorVisitor<'a> {
    fn new(expression_evaluator: &'a ExpressionEvaluator, partition: &'a Struct) -> Self {
        Self {
            expression_evaluator,
            partition,
        }
    }
}

#[allow(unused_variables)]
impl BoundPredicateVisitor for ExpressionEvaluatorVisitor<'_> {
    type T = bool;

    fn always_true(&mut self) -> Result<Self::T> {
        todo!()
    }

    fn always_false(&mut self) -> Result<Self::T> {
        todo!()
    }

    fn and(&mut self, lhs: Self::T, rhs: Self::T) -> Result<Self::T> {
        todo!()
    }

    fn or(&mut self, lhs: Self::T, rhs: Self::T) -> Result<Self::T> {
        todo!()
    }

    fn not(&mut self, inner: Self::T) -> Result<Self::T> {
        todo!()
    }

    fn is_null(
        &mut self,
        reference: &BoundReference,
        predicate: &BoundPredicate,
    ) -> Result<Self::T> {
        todo!()
    }

    fn not_null(
        &mut self,
        reference: &BoundReference,
        predicate: &BoundPredicate,
    ) -> Result<Self::T> {
        todo!()
    }

    fn is_nan(
        &mut self,
        reference: &BoundReference,
        predicate: &BoundPredicate,
    ) -> Result<Self::T> {
        todo!()
    }

    fn not_nan(
        &mut self,
        reference: &BoundReference,
        predicate: &BoundPredicate,
    ) -> Result<Self::T> {
        todo!()
    }

    fn less_than(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        predicate: &BoundPredicate,
    ) -> Result<Self::T> {
        todo!()
    }

    fn less_than_or_eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        predicate: &BoundPredicate,
    ) -> Result<Self::T> {
        todo!()
    }

    fn greater_than(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        predicate: &BoundPredicate,
    ) -> Result<Self::T> {
        todo!()
    }

    fn greater_than_or_eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        predicate: &BoundPredicate,
    ) -> Result<Self::T> {
        todo!()
    }

    fn eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        predicate: &BoundPredicate,
    ) -> Result<Self::T> {
        todo!()
    }

    fn not_eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        todo!()
    }

    fn starts_with(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        todo!()
    }

    fn not_starts_with(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        todo!()
    }

    fn r#in(
        &mut self,
        reference: &BoundReference,
        literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        todo!()
    }

    fn not_in(
        &mut self,
        reference: &BoundReference,
        literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        todo!()
    }
}

#[cfg(test)]
mod tests {}
