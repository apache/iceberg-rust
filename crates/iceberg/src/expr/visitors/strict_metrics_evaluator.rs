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

use crate::expr::visitors::bound_predicate_visitor::{visit, BoundPredicateVisitor};
use crate::expr::{BoundPredicate, BoundReference};
use crate::spec::{DataFile, Datum, Schema};
use crate::Result;

const ROWS_MUST_MATCH: Result<bool> = Ok(true);
const ROWS_MIGHT_NOT_MATCH: Result<bool> = Ok(false);

pub(crate) struct StrictMetricsEvaluator<'a> {
    data_file: &'a DataFile,
    schema: Schema,
}

impl<'a> StrictMetricsEvaluator<'a> {
    fn new(data_file: &'a DataFile, schema: Schema) -> Self {
        StrictMetricsEvaluator { data_file, schema }
    }

    /// Evaluate this `StrictMetricsEvaluator`'s filter predicate against the
    /// provided [`DataFile`]'s metrics. Used by [`TableScan`] to
    /// see if this `DataFile` contains data that could match
    /// the scan's filter.
    pub(crate) fn eval(
        filter: &'a BoundPredicate,
        data_file: &'a DataFile,
        schema: Schema,
    ) -> crate::Result<bool> {
        if data_file.record_count == 0 {
            return ROWS_MUST_MATCH;
        }

        let mut evaluator = Self::new(data_file, schema);
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

    fn lower_bound(&self, field_id: i32) -> Option<&Datum> {
        self.data_file.lower_bounds.get(&field_id)
    }

    fn upper_bound(&self, field_id: i32) -> Option<&Datum> {
        self.data_file.upper_bounds.get(&field_id)
    }

    fn contains_nans_only(&self, field_id: i32) -> bool {
        let nan_count = self.nan_count(field_id);
        let value_count = self.value_count(field_id);

        nan_count.is_some() && nan_count == value_count
    }

    fn contains_nulls_only(&self, field_id: i32) -> bool {
        let null_count = self.null_count(field_id);
        let value_count = self.value_count(field_id);

        null_count.is_some() && null_count == value_count
    }

    fn may_contain_null(&self, field_id: i32) -> bool {
        if let Some(&null_count) = self.null_count(field_id) {
            null_count > 0
        } else {
            true
        }
    }

    fn may_contain_nan(&self, field_id: i32) -> bool {
        if let Some(&nan_count) = self.nan_count(field_id) {
            nan_count > 0
        } else {
            true
        }
    }

    fn visit_inequality(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        cmp_fn: fn(&Datum, &Datum) -> bool,
        use_lower_bound: bool,
    ) -> crate::Result<bool> {
        let field_id = reference.field().id;

        if self.contains_nulls_only(field_id) || self.contains_nans_only(field_id) {
            return ROWS_MIGHT_NOT_MATCH;
        }

        if datum.is_nan() {
            return ROWS_MIGHT_NOT_MATCH;
        }

        let bound = if use_lower_bound {
            self.lower_bound(field_id)
        } else {
            self.upper_bound(field_id)
        };

        if let Some(bound) = bound {
            if cmp_fn(bound, datum) {
                return ROWS_MUST_MATCH;
            }

            return ROWS_MIGHT_NOT_MATCH;
        }

        ROWS_MIGHT_NOT_MATCH
    }
}

impl BoundPredicateVisitor for StrictMetricsEvaluator<'_> {
    type T = bool;

    fn always_true(&mut self) -> crate::Result<bool> {
        ROWS_MUST_MATCH
    }

    fn always_false(&mut self) -> crate::Result<bool> {
        ROWS_MIGHT_NOT_MATCH
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

        if self.contains_nulls_only(field_id) {
            return ROWS_MUST_MATCH;
        }

        ROWS_MIGHT_NOT_MATCH
    }

    fn not_null(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field_id = reference.field().id;

        match self.null_count(field_id) {
            Some(&0) => ROWS_MUST_MATCH,
            _ => ROWS_MIGHT_NOT_MATCH,
        }
    }

    fn is_nan(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field_id = reference.field().id;

        if self.contains_nans_only(field_id) {
            return ROWS_MUST_MATCH;
        }

        ROWS_MIGHT_NOT_MATCH
    }

    fn not_nan(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field_id = reference.field().id;

        if let Some(&nan_count) = self.nan_count(field_id) {
            if nan_count == 0 {
                return ROWS_MUST_MATCH;
            }
        }

        if self.contains_nulls_only(field_id) {
            return ROWS_MUST_MATCH;
        }

        ROWS_MIGHT_NOT_MATCH
    }

    fn less_than(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        self.visit_inequality(reference, datum, PartialOrd::lt, false)
    }

    fn less_than_or_eq(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        self.visit_inequality(reference, datum, PartialOrd::le, false)
    }

    fn greater_than(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        self.visit_inequality(reference, datum, PartialOrd::gt, true)
    }

    fn greater_than_or_eq(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        self.visit_inequality(reference, datum, PartialOrd::ge, true)
    }

    fn eq(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field_id = reference.field().id;

        if self.may_contain_null(field_id) || self.may_contain_nan(field_id) {
            return ROWS_MIGHT_NOT_MATCH;
        }

        if let (Some(lower), Some(upper)) = (self.lower_bound(field_id), self.upper_bound(field_id))
        {
            // For an equality predicate to hold strictly, we must have:
            //     lower == literal.value == upper.
            if lower.literal() != datum.literal() || upper.literal() != datum.literal() {
                return ROWS_MIGHT_NOT_MATCH;
            } else {
                return ROWS_MUST_MATCH;
            }
        }

        ROWS_MIGHT_NOT_MATCH
    }

    fn not_eq(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field_id = reference.field().id;

        if self.may_contain_null(field_id) || self.may_contain_nan(field_id) {
            return ROWS_MUST_MATCH;
        }

        if let Some(lower) = self.lower_bound(field_id) {
            if lower.is_nan() {
                return ROWS_MIGHT_NOT_MATCH;
            }

            if lower.literal() > datum.literal() {
                return ROWS_MUST_MATCH;
            }
        }

        if let Some(upper) = self.upper_bound(field_id) {
            if upper.is_nan() {
                return ROWS_MIGHT_NOT_MATCH;
            }

            if upper.literal() < datum.literal() {
                return ROWS_MUST_MATCH;
            }
        }

        ROWS_MIGHT_NOT_MATCH
    }

    fn starts_with(
        &mut self,
        _reference: &BoundReference,
        _datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        ROWS_MIGHT_NOT_MATCH
    }

    fn not_starts_with(
        &mut self,
        _reference: &BoundReference,
        _datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        ROWS_MIGHT_NOT_MATCH
    }

    fn r#in(
        &mut self,
        reference: &BoundReference,
        literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field_id = reference.field().id;

        if self.may_contain_null(field_id) || self.may_contain_nan(field_id) {
            return ROWS_MIGHT_NOT_MATCH;
        }

        if let (Some(lower), Some(upper)) = (self.lower_bound(field_id), self.upper_bound(field_id))
        {
            if !literals.contains(lower) || !literals.contains(upper) || lower != upper {
                return ROWS_MIGHT_NOT_MATCH;
            }

            return ROWS_MUST_MATCH;
        }

        ROWS_MIGHT_NOT_MATCH
    }

    fn not_in(
        &mut self,
        reference: &BoundReference,
        literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field_id = reference.field().id;

        if self.may_contain_null(field_id) || self.may_contain_nan(field_id) {
            return ROWS_MUST_MATCH;
        }

        let mut filtered_literals = literals.clone();

        if let Some(lower) = self.lower_bound(field_id) {
            if lower.is_nan() {
                return ROWS_MIGHT_NOT_MATCH;
            }

            filtered_literals.retain(|val| lower <= val);
            if filtered_literals.is_empty() {
                return ROWS_MUST_MATCH;
            }
        }

        if let Some(upper) = self.upper_bound(field_id) {
            filtered_literals.retain(|val| *val <= *upper);
            if filtered_literals.is_empty() {
                return ROWS_MUST_MATCH;
            }
        }

        ROWS_MIGHT_NOT_MATCH
    }
}
