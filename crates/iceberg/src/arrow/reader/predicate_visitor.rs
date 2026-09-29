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

//! Visitors that translate Iceberg bound predicates into the pieces needed for
//! Arrow-level evaluation: collecting referenced field IDs and producing
//! per-record-batch predicate closures.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_arith::boolean::{and, and_kleene, is_not_null, is_null, not, or, or_kleene};
use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, Float64Type};
use arrow_array::{Array, ArrayRef, BooleanArray, Datum as ArrowDatum, RecordBatch, Scalar};
use arrow_buffer::BooleanBuffer;
use arrow_cast::cast::cast;
use arrow_ord::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow_schema::{ArrowError, DataType};
use arrow_string::like::starts_with;
use fnv::FnvHashSet;
use parquet::schema::types::SchemaDescriptor;

use crate::arrow::get_arrow_datum;
use crate::arrow::record_batch_transformer::constants_map;
use crate::error::{Result, invalid_data};
use crate::expr::visitors::bound_predicate_visitor::{BoundPredicateVisitor, visit};
use crate::expr::visitors::expression_evaluator::ExpressionEvaluatorVisitor;
use crate::expr::{BoundPredicate, BoundReference, LogicalExpression};
use crate::spec::{Datum, Literal, PartitionSpec, Schema, Struct};

/// A visitor to collect field ids from bound predicates.
pub(super) struct CollectFieldIdVisitor {
    pub(super) field_ids: HashSet<i32>,
}

impl CollectFieldIdVisitor {
    pub(super) fn field_ids(self) -> HashSet<i32> {
        self.field_ids
    }
}

impl BoundPredicateVisitor for CollectFieldIdVisitor {
    type T = ();

    fn always_true(&mut self) -> Result<()> {
        Ok(())
    }

    fn always_false(&mut self) -> Result<()> {
        Ok(())
    }

    fn and(&mut self, _lhs: (), _rhs: ()) -> Result<()> {
        Ok(())
    }

    fn or(&mut self, _lhs: (), _rhs: ()) -> Result<()> {
        Ok(())
    }

    fn not(&mut self, _inner: ()) -> Result<()> {
        Ok(())
    }

    fn is_null(&mut self, reference: &BoundReference, _predicate: &BoundPredicate) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn not_null(&mut self, reference: &BoundReference, _predicate: &BoundPredicate) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn is_nan(&mut self, reference: &BoundReference, _predicate: &BoundPredicate) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn not_nan(&mut self, reference: &BoundReference, _predicate: &BoundPredicate) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn less_than(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn less_than_or_eq(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn greater_than(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn greater_than_or_eq(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn eq(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn not_eq(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn starts_with(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn not_starts_with(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn r#in(
        &mut self,
        reference: &BoundReference,
        _literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn not_in(
        &mut self,
        reference: &BoundReference,
        _literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }
}

/// Returns the residual of `predicate` for one data file: each leaf on a top-level field that is
/// missing from the file becomes `AlwaysTrue` or `AlwaysFalse`, based on the value projection
/// returns for that field. That value is the identity partition value, otherwise the field's
/// `initial-default`, and every row of the file holds it. Leaves on missing fields with neither
/// keep the null handling of the row filter and the page index evaluator.
///
/// Leaves on nested fields are kept, because a nested field also reads as null in any row where
/// an ancestor struct is null.
pub(super) fn residual_for_missing_fields(
    predicate: BoundPredicate,
    predicate_field_ids: &HashSet<i32>,
    field_id_map: &HashMap<i32, usize>,
    schema: &Schema,
    partition_spec: Option<&PartitionSpec>,
    partition: Option<&Struct>,
) -> Result<BoundPredicate> {
    if predicate_field_ids
        .iter()
        .all(|id| field_id_map.contains_key(id))
    {
        return Ok(predicate);
    }

    let partition_constants = match (partition_spec, partition) {
        (Some(spec), Some(data)) => constants_map(spec, data, schema)?,
        _ => HashMap::new(),
    };

    let mut field_ids = HashSet::new();
    let row: Struct = schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| {
            if !predicate_field_ids.contains(&field.id) || field_id_map.contains_key(&field.id) {
                return None;
            }
            let value = match partition_constants.get(&field.id) {
                Some(datum) => Some(Literal::Primitive(datum.literal().clone())),
                None => field.initial_default.clone(),
            };
            if value.is_some() {
                field_ids.insert(field.id);
            }
            value
        })
        .collect();

    if field_ids.is_empty() {
        return Ok(predicate);
    }
    visit(
        &mut MissingFieldResidualVisitor {
            row: &row,
            field_ids: &field_ids,
        },
        &predicate,
    )
}

/// Replaces leaves on the fields in `field_ids` with their result on `row`, which holds each
/// top-level field's value at its position in the schema.
struct MissingFieldResidualVisitor<'a> {
    row: &'a Struct,
    field_ids: &'a HashSet<i32>,
}

impl MissingFieldResidualVisitor<'_> {
    fn residual(
        &self,
        reference: &BoundReference,
        predicate: &BoundPredicate,
    ) -> Result<BoundPredicate> {
        if !self.field_ids.contains(&reference.field().id) {
            return Ok(predicate.clone());
        }
        if visit(&mut ExpressionEvaluatorVisitor::new(self.row), predicate)? {
            Ok(BoundPredicate::AlwaysTrue)
        } else {
            Ok(BoundPredicate::AlwaysFalse)
        }
    }
}

impl BoundPredicateVisitor for MissingFieldResidualVisitor<'_> {
    type T = BoundPredicate;

    fn always_true(&mut self) -> Result<BoundPredicate> {
        Ok(BoundPredicate::AlwaysTrue)
    }

    fn always_false(&mut self) -> Result<BoundPredicate> {
        Ok(BoundPredicate::AlwaysFalse)
    }

    fn and(&mut self, lhs: BoundPredicate, rhs: BoundPredicate) -> Result<BoundPredicate> {
        Ok(lhs.and(rhs))
    }

    fn or(&mut self, lhs: BoundPredicate, rhs: BoundPredicate) -> Result<BoundPredicate> {
        Ok(lhs.or(rhs))
    }

    fn not(&mut self, inner: BoundPredicate) -> Result<BoundPredicate> {
        Ok(BoundPredicate::Not(LogicalExpression::new([Box::new(
            inner,
        )])))
    }

    fn is_null(
        &mut self,
        reference: &BoundReference,
        predicate: &BoundPredicate,
    ) -> Result<BoundPredicate> {
        self.residual(reference, predicate)
    }

    fn not_null(
        &mut self,
        reference: &BoundReference,
        predicate: &BoundPredicate,
    ) -> Result<BoundPredicate> {
        self.residual(reference, predicate)
    }

    fn is_nan(
        &mut self,
        reference: &BoundReference,
        predicate: &BoundPredicate,
    ) -> Result<BoundPredicate> {
        self.residual(reference, predicate)
    }

    fn not_nan(
        &mut self,
        reference: &BoundReference,
        predicate: &BoundPredicate,
    ) -> Result<BoundPredicate> {
        self.residual(reference, predicate)
    }

    fn less_than(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        predicate: &BoundPredicate,
    ) -> Result<BoundPredicate> {
        self.residual(reference, predicate)
    }

    fn less_than_or_eq(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        predicate: &BoundPredicate,
    ) -> Result<BoundPredicate> {
        self.residual(reference, predicate)
    }

    fn greater_than(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        predicate: &BoundPredicate,
    ) -> Result<BoundPredicate> {
        self.residual(reference, predicate)
    }

    fn greater_than_or_eq(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        predicate: &BoundPredicate,
    ) -> Result<BoundPredicate> {
        self.residual(reference, predicate)
    }

    fn eq(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        predicate: &BoundPredicate,
    ) -> Result<BoundPredicate> {
        self.residual(reference, predicate)
    }

    fn not_eq(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        predicate: &BoundPredicate,
    ) -> Result<BoundPredicate> {
        self.residual(reference, predicate)
    }

    fn starts_with(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        predicate: &BoundPredicate,
    ) -> Result<BoundPredicate> {
        self.residual(reference, predicate)
    }

    fn not_starts_with(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        predicate: &BoundPredicate,
    ) -> Result<BoundPredicate> {
        self.residual(reference, predicate)
    }

    fn r#in(
        &mut self,
        reference: &BoundReference,
        _literals: &FnvHashSet<Datum>,
        predicate: &BoundPredicate,
    ) -> Result<BoundPredicate> {
        self.residual(reference, predicate)
    }

    fn not_in(
        &mut self,
        reference: &BoundReference,
        _literals: &FnvHashSet<Datum>,
        predicate: &BoundPredicate,
    ) -> Result<BoundPredicate> {
        self.residual(reference, predicate)
    }
}

/// A visitor to convert Iceberg bound predicates to Arrow predicates.
pub(super) struct PredicateConverter<'a> {
    /// The Parquet schema descriptor.
    pub(super) parquet_schema: &'a SchemaDescriptor,
    /// The map between field id and leaf column index in Parquet schema.
    pub(super) column_map: &'a HashMap<i32, usize>,
    /// The required column indices in Parquet schema for the predicates.
    pub(super) column_indices: &'a Vec<usize>,
}

impl PredicateConverter<'_> {
    /// When visiting a bound reference, we return index of the leaf column in the
    /// required column indices which is used to project the column in the record batch.
    /// Return None if the field id is not found in the column map, which is possible
    /// due to schema evolution.
    fn bound_reference(&mut self, reference: &BoundReference) -> Result<Option<usize>> {
        // The leaf column's index in Parquet schema.
        if let Some(column_idx) = self.column_map.get(&reference.field().id) {
            if self.parquet_schema.get_column_root(*column_idx).is_group() {
                return Err(invalid_data!(
                    "Leaf column `{}` in predicates isn't a root column in Parquet schema.",
                    reference.field().name
                ));
            }

            // The leaf column's index in the required column indices.
            let index = self
                .column_indices
                .iter()
                .position(|&idx| idx == *column_idx)
                .ok_or(invalid_data!(
                "Leaf column `{}` in predicates cannot be found in the required column indices.",
                reference.field().name
            ))?;

            Ok(Some(index))
        } else {
            Ok(None)
        }
    }

    /// Build an Arrow predicate that always returns true.
    fn build_always_true(&self) -> Result<Box<PredicateResult>> {
        Ok(Box::new(|batch| {
            Ok(constant_bool_array(true, batch.num_rows()))
        }))
    }

    /// Build an Arrow predicate that always returns false.
    fn build_always_false(&self) -> Result<Box<PredicateResult>> {
        Ok(Box::new(|batch| {
            Ok(constant_bool_array(false, batch.num_rows()))
        }))
    }
}

/// Builds a non-null `BooleanArray` of `len` elements all set to `value`.
fn constant_bool_array(value: bool, len: usize) -> BooleanArray {
    let buffer = if value {
        BooleanBuffer::new_set(len)
    } else {
        BooleanBuffer::new_unset(len)
    };

    BooleanArray::new(buffer, None)
}

/// Gets the leaf column from the record batch for the required column index. Only
/// supports top-level columns for now.
fn project_column(
    batch: &RecordBatch,
    column_idx: usize,
) -> std::result::Result<ArrayRef, ArrowError> {
    let column = batch.column(column_idx);

    match column.data_type() {
        DataType::Struct(_) => Err(ArrowError::SchemaError(
            "Does not support struct column yet.".to_string(),
        )),
        _ => Ok(column.clone()),
    }
}

fn compute_is_nan(array: &ArrayRef) -> std::result::Result<BooleanArray, ArrowError> {
    // Compute NaN over the contiguous values slice, then fold the null bitmap
    // in with a single bitwise AND so that null slots become false.
    let (is_nan, nulls) = match array.data_type() {
        DataType::Float32 => {
            let arr = array.as_primitive::<Float32Type>();
            (
                BooleanBuffer::from_iter(arr.values().iter().map(|v| v.is_nan())),
                arr.nulls(),
            )
        }
        DataType::Float64 => {
            let arr = array.as_primitive::<Float64Type>();
            (
                BooleanBuffer::from_iter(arr.values().iter().map(|v| v.is_nan())),
                arr.nulls(),
            )
        }
        _ => unreachable!("is_nan is only valid for float types"),
    };

    let values = match nulls {
        Some(nulls) => &is_nan & nulls.inner(),
        None => is_nan,
    };

    Ok(BooleanArray::new(values, None))
}

pub(super) type PredicateResult =
    dyn FnMut(RecordBatch) -> std::result::Result<BooleanArray, ArrowError> + Send + 'static;

impl BoundPredicateVisitor for PredicateConverter<'_> {
    type T = Box<PredicateResult>;

    fn always_true(&mut self) -> Result<Box<PredicateResult>> {
        self.build_always_true()
    }

    fn always_false(&mut self) -> Result<Box<PredicateResult>> {
        self.build_always_false()
    }

    fn and(
        &mut self,
        mut lhs: Box<PredicateResult>,
        mut rhs: Box<PredicateResult>,
    ) -> Result<Box<PredicateResult>> {
        Ok(Box::new(move |batch| {
            let left = lhs(batch.clone())?;
            let right = rhs(batch)?;
            and_kleene(&left, &right)
        }))
    }

    fn or(
        &mut self,
        mut lhs: Box<PredicateResult>,
        mut rhs: Box<PredicateResult>,
    ) -> Result<Box<PredicateResult>> {
        Ok(Box::new(move |batch| {
            let left = lhs(batch.clone())?;
            let right = rhs(batch)?;
            or_kleene(&left, &right)
        }))
    }

    fn not(&mut self, mut inner: Box<PredicateResult>) -> Result<Box<PredicateResult>> {
        Ok(Box::new(move |batch| {
            let pred_ret = inner(batch)?;
            not(&pred_ret)
        }))
    }

    fn is_null(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            Ok(Box::new(move |batch| {
                let column = project_column(&batch, idx)?;
                is_null(&column)
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_true()
        }
    }

    fn not_null(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            Ok(Box::new(move |batch| {
                let column = project_column(&batch, idx)?;
                is_not_null(&column)
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn is_nan(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            Ok(Box::new(move |batch| {
                let column = project_column(&batch, idx)?;
                compute_is_nan(&column)
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn not_nan(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            Ok(Box::new(move |batch| {
                let column = project_column(&batch, idx)?;
                let is_nan = compute_is_nan(&column)?;
                not(&is_nan)
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_true()
        }
    }

    fn less_than(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, idx)?;
                let literal = try_cast_literal(&literal, left.data_type())?;
                lt(&left, literal.as_ref())
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_true()
        }
    }

    fn less_than_or_eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, idx)?;
                let literal = try_cast_literal(&literal, left.data_type())?;
                lt_eq(&left, literal.as_ref())
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_true()
        }
    }

    fn greater_than(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, idx)?;
                let literal = try_cast_literal(&literal, left.data_type())?;
                gt(&left, literal.as_ref())
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn greater_than_or_eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, idx)?;
                let literal = try_cast_literal(&literal, left.data_type())?;
                gt_eq(&left, literal.as_ref())
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, idx)?;
                let literal = try_cast_literal(&literal, left.data_type())?;
                eq(&left, literal.as_ref())
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn not_eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, idx)?;
                let literal = try_cast_literal(&literal, left.data_type())?;
                neq(&left, literal.as_ref())
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn starts_with(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, idx)?;
                let literal = try_cast_literal(&literal, left.data_type())?;
                starts_with(&left, literal.as_ref())
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn not_starts_with(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, idx)?;
                let literal = try_cast_literal(&literal, left.data_type())?;
                // update here if arrow ever adds a native not_starts_with
                not(&starts_with(&left, literal.as_ref())?)
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_true()
        }
    }

    fn r#in(
        &mut self,
        reference: &BoundReference,
        literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literals: Vec<_> = literals
                .iter()
                .map(|lit| get_arrow_datum(lit).unwrap())
                .collect();

            Ok(Box::new(move |batch| {
                // update this if arrow ever adds a native is_in kernel
                let left = project_column(&batch, idx)?;

                let mut acc = constant_bool_array(false, batch.num_rows());
                for literal in &literals {
                    let literal = try_cast_literal(literal, left.data_type())?;
                    acc = or(&acc, &eq(&left, literal.as_ref())?)?
                }

                Ok(acc)
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn not_in(
        &mut self,
        reference: &BoundReference,
        literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literals: Vec<_> = literals
                .iter()
                .map(|lit| get_arrow_datum(lit).unwrap())
                .collect();

            Ok(Box::new(move |batch| {
                // update this if arrow ever adds a native not_in kernel
                let left = project_column(&batch, idx)?;
                let mut acc = constant_bool_array(true, batch.num_rows());
                for literal in &literals {
                    let literal = try_cast_literal(literal, left.data_type())?;
                    acc = and(&acc, &neq(&left, literal.as_ref())?)?
                }

                Ok(acc)
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_true()
        }
    }
}

/// The Arrow type of an array that the Parquet reader reads may not match the exact Arrow type
/// that Iceberg uses for literals - but they are effectively the same logical type,
/// i.e. LargeUtf8 and Utf8 or Utf8View and Utf8 or Utf8View and LargeUtf8.
///
/// The Arrow compute kernels that we use must match the type exactly, so first cast the literal
/// into the type of the batch we read from Parquet before sending it to the compute kernel.
fn try_cast_literal(
    literal: &Arc<dyn ArrowDatum + Send + Sync>,
    column_type: &DataType,
) -> std::result::Result<Arc<dyn ArrowDatum + Send + Sync>, ArrowError> {
    let literal_array = literal.get().0;

    // No cast required
    if literal_array.data_type() == column_type {
        return Ok(Arc::clone(literal));
    }

    let literal_array = cast(literal_array, column_type)?;
    Ok(Arc::new(Scalar::new(literal_array)))
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use arrow_array::{Array, BooleanArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use parquet::schema::parser::parse_message_type;
    use parquet::schema::types::SchemaDescriptor;

    use super::{
        CollectFieldIdVisitor, PredicateConverter, constant_bool_array, residual_for_missing_fields,
    };
    use crate::expr::visitors::bound_predicate_visitor::visit;
    use crate::expr::{Bind, BoundPredicate, LogicalExpression, Predicate, Reference};
    use crate::spec::{
        Datum, Literal, NestedField, PartitionSpec, PrimitiveType, Schema, SchemaRef, Struct,
        StructType, Transform, Type,
    };

    fn table_schema_simple() -> SchemaRef {
        Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_identifier_field_ids(vec![2])
                .with_fields(vec![
                    NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
                    NestedField::optional(4, "qux", Type::Primitive(PrimitiveType::Float)).into(),
                ])
                .build()
                .unwrap(),
        )
    }

    #[test]
    fn test_collect_field_id() {
        let schema = table_schema_simple();
        let expr = Reference::new("qux").is_null();
        let bound_expr = expr.bind(schema, true).unwrap();

        let mut visitor = CollectFieldIdVisitor {
            field_ids: HashSet::default(),
        };
        visit(&mut visitor, &bound_expr).unwrap();

        let mut expected = HashSet::default();
        expected.insert(4_i32);

        assert_eq!(visitor.field_ids, expected);
    }

    #[test]
    fn test_collect_field_id_with_and() {
        let schema = table_schema_simple();
        let expr = Reference::new("qux")
            .is_null()
            .and(Reference::new("baz").is_null());
        let bound_expr = expr.bind(schema, true).unwrap();

        let mut visitor = CollectFieldIdVisitor {
            field_ids: HashSet::default(),
        };
        visit(&mut visitor, &bound_expr).unwrap();

        let mut expected = HashSet::default();
        expected.insert(4_i32);
        expected.insert(3);

        assert_eq!(visitor.field_ids, expected);
    }

    #[test]
    fn test_collect_field_id_with_or() {
        let schema = table_schema_simple();
        let expr = Reference::new("qux")
            .is_null()
            .or(Reference::new("baz").is_null());
        let bound_expr = expr.bind(schema, true).unwrap();

        let mut visitor = CollectFieldIdVisitor {
            field_ids: HashSet::default(),
        };
        visit(&mut visitor, &bound_expr).unwrap();

        let mut expected = HashSet::default();
        expected.insert(4_i32);
        expected.insert(3);

        assert_eq!(visitor.field_ids, expected);
    }

    #[test]
    fn test_constant_bool_array() {
        for len in [0, 8192] {
            let all_true = constant_bool_array(true, len);
            assert_eq!(all_true.len(), len);
            assert_eq!(all_true.null_count(), 0);
            assert!(all_true.iter().all(|v| v == Some(true)));

            let all_false = constant_bool_array(false, len);
            assert_eq!(all_false.len(), len);
            assert_eq!(all_false.null_count(), 0);
            assert!(all_false.iter().all(|v| v == Some(false)));
        }
    }

    fn apply_predicate_to_batch(
        predicate: Predicate,
        schema: SchemaRef,
        batch: RecordBatch,
    ) -> BooleanArray {
        let bound = predicate.bind(schema, true).unwrap();

        // Build a trivial Parquet schema with one float column at field id 4
        let message_type = "
            message schema {
              optional float qux = 4;
            }
        ";
        let parquet_type = parse_message_type(message_type).expect("parse schema");
        let parquet_schema = SchemaDescriptor::new(Arc::new(parquet_type));

        let column_map = HashMap::from([(4i32, 0usize)]);
        let column_indices = vec![0usize];

        let mut converter = PredicateConverter {
            parquet_schema: &parquet_schema,
            column_map: &column_map,
            column_indices: &column_indices,
        };

        let mut predicate_fn = visit(&mut converter, &bound).unwrap();
        predicate_fn(batch).unwrap()
    }

    #[test]
    fn test_predicate_converter_nan() {
        use arrow_array::Float32Array;

        let schema = table_schema_simple();
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "qux",
            DataType::Float32,
            true,
        )]));
        let values = vec![Some(1.0f32), Some(f32::NAN), None, Some(0.0f32)];

        // is_nan: non-null-propagating per Java's implementation - NULL → false
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(Float32Array::from(
            values.clone(),
        ))])
        .unwrap();
        let result =
            apply_predicate_to_batch(Reference::new("qux").is_nan(), schema.clone(), batch);
        assert_eq!(
            [
                result.value(0),
                result.value(1),
                result.value(2),
                result.value(3)
            ],
            [false, true, false, false]
        );
        assert!(!result.is_null(2));

        // not_nan: non-null-propagating per Java's implementation - NULL → true
        let batch =
            RecordBatch::try_new(arrow_schema, vec![Arc::new(Float32Array::from(values))]).unwrap();
        let result = apply_predicate_to_batch(Reference::new("qux").is_not_nan(), schema, batch);
        assert_eq!(
            [
                result.value(0),
                result.value(1),
                result.value(2),
                result.value(3)
            ],
            [true, false, true, true]
        );
        assert!(!result.is_null(2));
    }

    /// Returns the residual of `predicate` for a data file that stores only field 1.
    fn residual_for_file_with_field_1(
        schema: &SchemaRef,
        predicate: Predicate,
        partition: Option<(&PartitionSpec, &Struct)>,
    ) -> BoundPredicate {
        let predicate = predicate.bind(schema.clone(), true).unwrap();
        let mut collector = CollectFieldIdVisitor {
            field_ids: HashSet::default(),
        };
        visit(&mut collector, &predicate).unwrap();
        residual_for_missing_fields(
            predicate,
            &collector.field_ids(),
            &HashMap::from([(1, 0)]),
            schema,
            partition.map(|(spec, _)| spec),
            partition.map(|(_, data)| data),
        )
        .unwrap()
    }

    /// Asserts that each predicate in `always_true` has the residual `AlwaysTrue` and each in
    /// `always_false` has `AlwaysFalse`, for a file that doesn't store the field `field`.
    fn assert_residuals_for_missing_field_with_initial_default(
        field: NestedField,
        always_true: Vec<Predicate>,
        always_false: Vec<Predicate>,
    ) {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    field.into(),
                ])
                .build()
                .unwrap(),
        );

        let cases: Vec<_> = always_true
            .into_iter()
            .map(|predicate| (predicate, BoundPredicate::AlwaysTrue))
            .chain(
                always_false
                    .into_iter()
                    .map(|predicate| (predicate, BoundPredicate::AlwaysFalse)),
            )
            .collect();
        let actual: Vec<_> = cases
            .iter()
            .map(|(predicate, _)| {
                (
                    predicate.to_string(),
                    residual_for_file_with_field_1(&schema, predicate.clone(), None),
                )
            })
            .collect();
        let expected: Vec<_> = cases
            .into_iter()
            .map(|(predicate, residual)| (predicate.to_string(), residual))
            .collect();
        assert_eq!(actual, expected);
    }

    /// Mirrors Java's `TestMetricsRowGroupFilter.testColumnNotInFileWithInitialDefault`.
    #[test]
    fn test_residual_for_missing_string_field_with_initial_default() {
        let country = || Reference::new("country");
        let string = Datum::string;
        assert_residuals_for_missing_field_with_initial_default(
            NestedField::optional(99, "country", Type::Primitive(PrimitiveType::String))
                .with_initial_default(Literal::string("US")),
            vec![
                country().equal_to(string("US")),
                country().is_not_null(),
                country().not_equal_to(string("CA")),
                country().is_in([string("US"), string("MX")]),
                country().is_not_in([string("CA"), string("MX")]),
                country().greater_than_or_equal_to(string("US")),
                country().less_than(string("ZW")),
                country().starts_with(string("U")),
                country().not_starts_with(string("X")),
            ],
            vec![
                country().equal_to(string("CA")),
                country().is_null(),
                country().not_equal_to(string("US")),
                country().is_in([string("CA"), string("MX")]),
                country().is_not_in([string("US"), string("MX")]),
                country().less_than(string("AD")),
                country().greater_than(string("US")),
                country().starts_with(string("X")),
                country().not_starts_with(string("U")),
            ],
        );
    }

    /// Mirrors Java's `TestMetricsRowGroupFilter.testDateColumnNotInFileWithInitialDefault`.
    #[test]
    fn test_residual_for_missing_date_field_with_initial_default() {
        let event_date = || Reference::new("event_date");
        assert_residuals_for_missing_field_with_initial_default(
            NestedField::optional(100, "event_date", Type::Primitive(PrimitiveType::Date))
                .with_initial_default(Literal::date(42)),
            vec![
                event_date().equal_to(Datum::date(42)),
                event_date().less_than(Datum::date(43)),
                event_date().greater_than(Datum::date(41)),
            ],
            vec![
                event_date().equal_to(Datum::date(41)),
                event_date().less_than(Datum::date(42)),
                event_date().greater_than(Datum::date(42)),
            ],
        );
    }

    /// Mirrors Java's `TestMetricsRowGroupFilter.testDoubleColumnNotInFileWithInitialDefault`.
    #[test]
    fn test_residual_for_missing_double_field_with_initial_default() {
        assert_residuals_for_missing_field_with_initial_default(
            NestedField::optional(101, "measurement", Type::Primitive(PrimitiveType::Double))
                .with_initial_default(Literal::double(12.5)),
            vec![Reference::new("measurement").is_not_nan()],
            vec![Reference::new("measurement").is_nan()],
        );
    }

    /// Covers which value a leaf is evaluated against, and which leaves are kept.
    #[test]
    fn test_residual_for_missing_fields() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "a", Type::Primitive(PrimitiveType::Long))
                        .with_initial_default(Literal::long(5))
                        .into(),
                    NestedField::optional(2, "b", Type::Primitive(PrimitiveType::Long))
                        .with_initial_default(Literal::long(7))
                        .into(),
                    NestedField::optional(4, "d", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(5, "p", Type::Primitive(PrimitiveType::Long))
                        .with_initial_default(Literal::long(7))
                        .into(),
                    NestedField::optional(6, "q", Type::Primitive(PrimitiveType::Long))
                        .with_initial_default(Literal::long(7))
                        .into(),
                    NestedField::optional(
                        7,
                        "s",
                        Type::Struct(StructType::new(vec![
                            NestedField::optional(8, "x", Type::Primitive(PrimitiveType::Long))
                                .with_initial_default(Literal::long(7))
                                .into(),
                        ])),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let partition_spec = PartitionSpec::builder(schema.clone())
            .add_partition_field("a", "a", Transform::Identity)
            .unwrap()
            .add_partition_field("p", "p", Transform::Identity)
            .unwrap()
            .add_partition_field("q", "q_bucket", Transform::Bucket(4))
            .unwrap()
            .build()
            .unwrap();
        let partition = Struct::from_iter([
            Some(Literal::long(100)),
            Some(Literal::long(9)),
            Some(Literal::int(2)),
        ]);

        let bind = |predicate: Predicate| predicate.bind(schema.clone(), true).unwrap();
        let cases = [
            (
                "p = 9 uses the identity partition value over the initial-default",
                Reference::new("p").equal_to(Datum::long(9)),
                BoundPredicate::AlwaysTrue,
            ),
            (
                "p = 7",
                Reference::new("p").equal_to(Datum::long(7)),
                BoundPredicate::AlwaysFalse,
            ),
            (
                "q = 7 ignores the bucket partition value",
                Reference::new("q").equal_to(Datum::long(7)),
                BoundPredicate::AlwaysTrue,
            ),
            (
                "a = 3 reads the file despite a partition value and initial-default",
                Reference::new("a").equal_to(Datum::long(3)),
                bind(Reference::new("a").equal_to(Datum::long(3))),
            ),
            (
                "d IS NULL has no value to apply",
                Reference::new("d").is_null(),
                bind(Reference::new("d").is_null()),
            ),
            (
                "s.x = 7 is nested",
                Reference::new("s.x").equal_to(Datum::long(7)),
                bind(Reference::new("s.x").equal_to(Datum::long(7))),
            ),
            (
                "NOT (b = 7)",
                !Reference::new("b").equal_to(Datum::long(7)),
                BoundPredicate::Not(LogicalExpression::new([Box::new(
                    BoundPredicate::AlwaysTrue,
                )])),
            ),
            (
                "a > 0 AND b = 8",
                Reference::new("a")
                    .greater_than(Datum::long(0))
                    .and(Reference::new("b").equal_to(Datum::long(8))),
                bind(Reference::new("a").greater_than(Datum::long(0)))
                    .and(BoundPredicate::AlwaysFalse),
            ),
            (
                "d = 1 OR b = 7",
                Reference::new("d")
                    .equal_to(Datum::long(1))
                    .or(Reference::new("b").equal_to(Datum::long(7))),
                bind(Reference::new("d").equal_to(Datum::long(1))).or(BoundPredicate::AlwaysTrue),
            ),
        ];

        let actual: Vec<_> = cases
            .iter()
            .map(|(name, predicate, _)| {
                let residual = residual_for_file_with_field_1(
                    &schema,
                    predicate.clone(),
                    Some((&partition_spec, &partition)),
                );
                (*name, residual)
            })
            .collect();
        let expected: Vec<_> = cases
            .into_iter()
            .map(|(name, _, residual)| (name, residual))
            .collect();
        assert_eq!(actual, expected);
    }
}
