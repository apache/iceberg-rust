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

//! Evaluates Parquet Row Group metrics

use std::collections::HashMap;

use fnv::FnvHashSet;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::statistics::Statistics;

use crate::expr::visitors::bound_predicate_visitor::{visit, BoundPredicateVisitor};
use crate::expr::{BoundPredicate, BoundReference};
use crate::spec::{Datum, PrimitiveLiteral, PrimitiveType, Schema};
use crate::{Error, ErrorKind, Result};

pub(crate) struct RowGroupMetricsEvaluator<'a> {
    row_group_metadata: &'a RowGroupMetaData,
    iceberg_field_id_to_parquet_column_index: &'a HashMap<i32, usize>,
    snapshot_schema: &'a Schema,
}

const IN_PREDICATE_LIMIT: usize = 200;
const ROW_GROUP_MIGHT_MATCH: Result<bool> = Ok(true);
const ROW_GROUP_CANT_MATCH: Result<bool> = Ok(false);

impl<'a> RowGroupMetricsEvaluator<'a> {
    fn new(
        row_group_metadata: &'a RowGroupMetaData,
        field_id_map: &'a HashMap<i32, usize>,
        snapshot_schema: &'a Schema,
    ) -> Self {
        Self {
            row_group_metadata,
            iceberg_field_id_to_parquet_column_index: field_id_map,
            snapshot_schema,
        }
    }

    /// Evaluate this `RowGroupMetricsEvaluator`'s filter predicate against the
    /// provided [`RowGroupMetaData`]'. Used by [`ArrowReader`] to
    /// see if a Parquet file RowGroup could possibly contain data that matches
    /// the scan's filter.
    pub(crate) fn eval(
        filter: &'a BoundPredicate,
        row_group_metadata: &'a RowGroupMetaData,
        field_id_map: &'a HashMap<i32, usize>,
        snapshot_schema: &'a Schema,
    ) -> Result<bool> {
        if row_group_metadata.num_rows() == 0 {
            return ROW_GROUP_CANT_MATCH;
        }

        let mut evaluator = Self::new(row_group_metadata, field_id_map, snapshot_schema);

        visit(&mut evaluator, filter)
    }

    fn stats_for_field_id(&self, field_id: i32) -> Option<&Statistics> {
        let parquet_column_index = *self
            .iceberg_field_id_to_parquet_column_index
            .get(&field_id)?;
        self.row_group_metadata
            .column(parquet_column_index)
            .statistics()
    }

    fn null_count(&self, field_id: i32) -> Option<u64> {
        self.stats_for_field_id(field_id)
            .map(|stats| stats.null_count())
    }

    fn value_count(&self) -> u64 {
        self.row_group_metadata.num_rows() as u64
    }

    fn contains_nulls_only(&self, field_id: i32) -> bool {
        let null_count = self.null_count(field_id);
        let value_count = self.value_count();

        null_count.is_some() && null_count == Some(value_count)
    }

    fn may_contain_null(&self, field_id: i32) -> bool {
        if let Some(null_count) = self.null_count(field_id) {
            null_count > 0
        } else {
            true
        }
    }

    fn stats_and_type_for_field_id(
        &self,
        field_id: i32,
    ) -> Result<Option<(&Statistics, PrimitiveType)>> {
        let Some(stats) = self.stats_for_field_id(field_id) else {
            // No statistics for column
            return Ok(None);
        };

        let Some(field) = self.snapshot_schema.field_by_id(field_id) else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!(
                    "Could not find a field with id '{}' in the snapshot schema",
                    &field_id
                ),
            ));
        };

        let Some(primitive_type) = field.field_type.as_primitive_type() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!(
                    "Could not determine the PrimitiveType for field id '{}'",
                    &field_id
                ),
            ));
        };

        Ok(Some((stats, primitive_type.clone())))
    }

    fn min_value(&self, field_id: i32) -> Result<Option<Datum>> {
        let Some((stats, primitive_type)) = self.stats_and_type_for_field_id(field_id)? else {
            return Ok(None);
        };

        Ok(Some(Datum::try_from_bytes(
            stats.min_bytes(),
            primitive_type,
        )?))
    }

    fn max_value(&self, field_id: i32) -> Result<Option<Datum>> {
        let Some((stats, primitive_type)) = self.stats_and_type_for_field_id(field_id)? else {
            return Ok(None);
        };

        Ok(Some(Datum::try_from_bytes(
            stats.max_bytes(),
            primitive_type,
        )?))
    }

    fn visit_inequality(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        cmp_fn: fn(&Datum, &Datum) -> bool,
        use_lower_bound: bool,
    ) -> Result<bool> {
        let field_id = reference.field().id;

        if self.contains_nulls_only(field_id) {
            return ROW_GROUP_CANT_MATCH;
        }

        if datum.is_nan() {
            // NaN indicates unreliable bounds.
            // See the InclusiveMetricsEvaluator docs for more.
            return ROW_GROUP_MIGHT_MATCH;
        }

        let bound = if use_lower_bound {
            self.min_value(field_id)
        } else {
            self.max_value(field_id)
        }?;

        if let Some(bound) = bound {
            if cmp_fn(&bound, datum) {
                return ROW_GROUP_MIGHT_MATCH;
            }

            return ROW_GROUP_CANT_MATCH;
        }

        ROW_GROUP_MIGHT_MATCH
    }
}

impl BoundPredicateVisitor for RowGroupMetricsEvaluator<'_> {
    type T = bool;

    fn always_true(&mut self) -> Result<bool> {
        ROW_GROUP_MIGHT_MATCH
    }

    fn always_false(&mut self) -> Result<bool> {
        ROW_GROUP_CANT_MATCH
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
        let field_id = reference.field().id;

        match self.null_count(field_id) {
            Some(0) => ROW_GROUP_CANT_MATCH,
            Some(_) => ROW_GROUP_MIGHT_MATCH,
            None => ROW_GROUP_MIGHT_MATCH,
        }
    }

    fn not_null(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        let field_id = reference.field().id;

        if self.contains_nulls_only(field_id) {
            return ROW_GROUP_CANT_MATCH;
        }

        ROW_GROUP_MIGHT_MATCH
    }

    fn is_nan(&mut self, _reference: &BoundReference, _predicate: &BoundPredicate) -> Result<bool> {
        // NaN counts not in ColumnChunkMetadata Statistics
        ROW_GROUP_MIGHT_MATCH
    }

    fn not_nan(
        &mut self,
        _reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        // NaN counts not in ColumnChunkMetadata Statistics
        ROW_GROUP_MIGHT_MATCH
    }

    fn less_than(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        self.visit_inequality(reference, datum, PartialOrd::lt, true)
    }

    fn less_than_or_eq(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        self.visit_inequality(reference, datum, PartialOrd::le, true)
    }

    fn greater_than(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        self.visit_inequality(reference, datum, PartialOrd::gt, false)
    }

    fn greater_than_or_eq(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        self.visit_inequality(reference, datum, PartialOrd::ge, false)
    }

    fn eq(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        let field_id = reference.field().id;

        if self.contains_nulls_only(field_id) {
            return ROW_GROUP_CANT_MATCH;
        }

        if let Some(lower_bound) = self.min_value(field_id)? {
            if lower_bound.is_nan() {
                // NaN indicates unreliable bounds.
                // See the InclusiveMetricsEvaluator docs for more.
                return ROW_GROUP_MIGHT_MATCH;
            } else if lower_bound.gt(datum) {
                return ROW_GROUP_CANT_MATCH;
            }
        }

        if let Some(upper_bound) = self.max_value(field_id)? {
            if upper_bound.is_nan() {
                // NaN indicates unreliable bounds.
                // See the InclusiveMetricsEvaluator docs for more.
                return ROW_GROUP_MIGHT_MATCH;
            } else if upper_bound.lt(datum) {
                return ROW_GROUP_CANT_MATCH;
            }
        }

        ROW_GROUP_MIGHT_MATCH
    }

    fn not_eq(
        &mut self,
        _reference: &BoundReference,
        _datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        // Because the bounds are not necessarily a min or max value,
        // this cannot be answered using them. notEq(col, X) with (X, Y)
        // doesn't guarantee that X is a value in col.
        ROW_GROUP_MIGHT_MATCH
    }

    fn starts_with(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        let field_id = reference.field().id;

        if self.contains_nulls_only(field_id) {
            return ROW_GROUP_CANT_MATCH;
        }

        let PrimitiveLiteral::String(datum) = datum.literal() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Cannot use StartsWith operator on non-string values",
            ));
        };

        if let Some(lower_bound) = self.min_value(field_id)? {
            let PrimitiveLiteral::String(lower_bound) = lower_bound.literal() else {
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
                return ROW_GROUP_CANT_MATCH;
            }
        }

        if let Some(upper_bound) = self.max_value(field_id)? {
            let PrimitiveLiteral::String(upper_bound) = upper_bound.literal() else {
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
                return ROW_GROUP_CANT_MATCH;
            }
        }

        ROW_GROUP_MIGHT_MATCH
    }

    fn not_starts_with(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        let field_id = reference.field().id;

        if self.may_contain_null(field_id) {
            return ROW_GROUP_MIGHT_MATCH;
        }

        // notStartsWith will match unless all values must start with the prefix.
        // This happens when the lower and upper bounds both start with the prefix.

        let PrimitiveLiteral::String(prefix) = datum.literal() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Cannot use StartsWith operator on non-string values",
            ));
        };

        let Some(lower_bound) = self.min_value(field_id)? else {
            return ROW_GROUP_MIGHT_MATCH;
        };

        let PrimitiveLiteral::String(lower_bound_str) = lower_bound.literal() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Cannot use NotStartsWith operator on non-string lower_bound value",
            ));
        };

        if lower_bound_str < prefix {
            // if lower is shorter than the prefix then lower doesn't start with the prefix
            return ROW_GROUP_MIGHT_MATCH;
        }

        let prefix_len = prefix.chars().count();

        if lower_bound_str.chars().take(prefix_len).collect::<String>() == *prefix {
            // lower bound matches the prefix

            let Some(upper_bound) = self.max_value(field_id)? else {
                return ROW_GROUP_MIGHT_MATCH;
            };

            let PrimitiveLiteral::String(upper_bound) = upper_bound.literal() else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Cannot use NotStartsWith operator on non-string upper_bound value",
                ));
            };

            // if upper is shorter than the prefix then upper can't start with the prefix
            if upper_bound.chars().count() < prefix_len {
                return ROW_GROUP_MIGHT_MATCH;
            }

            if upper_bound.chars().take(prefix_len).collect::<String>() == *prefix {
                // both bounds match the prefix, so all rows must match the
                // prefix and therefore do not satisfy the predicate
                return ROW_GROUP_CANT_MATCH;
            }
        }

        ROW_GROUP_MIGHT_MATCH
    }

    fn r#in(
        &mut self,
        reference: &BoundReference,
        literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        let field_id = reference.field().id;

        if self.contains_nulls_only(field_id) {
            return ROW_GROUP_CANT_MATCH;
        }

        if literals.len() > IN_PREDICATE_LIMIT {
            // skip evaluating the predicate if the number of values is too big
            return ROW_GROUP_MIGHT_MATCH;
        }

        if let Some(lower_bound) = self.min_value(field_id)? {
            if lower_bound.is_nan() {
                // NaN indicates unreliable bounds. See the InclusiveMetricsEvaluator docs for more.
                return ROW_GROUP_MIGHT_MATCH;
            }

            if !literals.iter().any(|datum| datum.ge(&lower_bound)) {
                // if all values are less than lower bound, rows cannot match.
                return ROW_GROUP_CANT_MATCH;
            }
        }

        if let Some(upper_bound) = self.max_value(field_id)? {
            if upper_bound.is_nan() {
                // NaN indicates unreliable bounds. See the InclusiveMetricsEvaluator docs for more.
                return ROW_GROUP_MIGHT_MATCH;
            }

            if !literals.iter().any(|datum| datum.le(&upper_bound)) {
                // if all values are greater than upper bound, rows cannot match.
                return ROW_GROUP_CANT_MATCH;
            }
        }

        ROW_GROUP_MIGHT_MATCH
    }

    fn not_in(
        &mut self,
        _reference: &BoundReference,
        _literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        // Because the bounds are not necessarily a min or max value,
        // this cannot be answered using them. notIn(col, {X, ...})
        // with (X, Y) doesn't guarantee that X is a value in col.
        ROW_GROUP_MIGHT_MATCH
    }
}
