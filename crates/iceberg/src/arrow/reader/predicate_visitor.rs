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
use arrow_array::types::{Float32Type, Float64Type, Int32Type, Int64Type};
use arrow_array::{Array, ArrayRef, BooleanArray, Datum as ArrowDatum, RecordBatch, Scalar};
use arrow_buffer::BooleanBuffer;
use arrow_cast::cast::cast;
use arrow_ord::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow_schema::{ArrowError, DataType};
use arrow_select::take::take;
use arrow_string::like::starts_with;
use fnv::FnvHashSet;
use once_cell::unsync::OnceCell;
use parquet::schema::types::SchemaDescriptor;

use crate::arrow::get_arrow_datum;
use crate::error::{Result, invalid_data};
use crate::expr::visitors::bound_predicate_visitor::BoundPredicateVisitor;
use crate::expr::{BoundPredicate, BoundReference, SetIndex};
use crate::spec::{Datum, PrimitiveType};

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

    fn set_predicate(
        &mut self,
        reference: &BoundReference,
        predicate: &BoundPredicate,
        negated: bool,
    ) -> Result<Box<PredicateResult>> {
        let Some(idx) = self.bound_reference(reference)? else {
            return Ok(Box::new(move |batch| {
                Ok(constant_bool_array(negated, batch.num_rows()))
            }));
        };
        let BoundPredicate::Set(set) = predicate else {
            unreachable!("set visitor receives a set predicate")
        };
        // The scan benchmark crosses over from scalar comparisons to hashing at different list sizes.
        let index = match (
            reference.field().field_type.as_primitive_type(),
            set.literals().len(),
        ) {
            (Some(PrimitiveType::String), 4..)
            | (Some(PrimitiveType::Int), 8..)
            | (Some(PrimitiveType::Long), 16..) => set.membership_index(),
            _ => None,
        };
        let set = set.clone();
        let arrow_literals = move || {
            set.literals()
                .iter()
                .map(get_arrow_datum)
                .collect::<Result<Vec<_>>>()
        };
        let scalars = if index.is_some() {
            OnceCell::new()
        } else {
            OnceCell::with_value(arrow_literals()?)
        };
        Ok(Box::new(move |batch| {
            let column = project_column(&batch, idx)?;
            if let Some(index) = &index
                && let Some(result) = probe_set(index, column.as_ref(), negated)?
            {
                return Ok(result);
            }
            // Schema evolution can give a file a different physical type from the bound literals.
            let scalars = scalars
                .get_or_try_init(&arrow_literals)
                .map_err(|err| ArrowError::ExternalError(Box::new(err)))?;
            let mut acc = constant_bool_array(negated, batch.num_rows());
            for scalar in scalars {
                let scalar = try_cast_literal(scalar, column.data_type())?;
                acc = if negated {
                    and(&acc, &neq(&column, scalar.as_ref())?)?
                } else {
                    or(&acc, &eq(&column, scalar.as_ref())?)?
                };
            }
            Ok(acc)
        }))
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

fn probe_set(
    index: &SetIndex,
    column: &dyn Array,
    negated: bool,
) -> std::result::Result<Option<BooleanArray>, ArrowError> {
    if let Some(dict) = column.as_any_dictionary_opt() {
        let Some(values) = probe_set(index, dict.values().as_ref(), negated)? else {
            return Ok(None);
        };
        return Ok(Some(take(&values, dict.keys(), None)?.as_boolean().clone()));
    }
    Ok(Some(match (index, column.data_type()) {
        (SetIndex::Int(keys), DataType::Int32) => {
            BooleanArray::from_unary(column.as_primitive::<Int32Type>(), |x| {
                keys.contains(&x) ^ negated
            })
        }
        (SetIndex::Long(keys), DataType::Int64) => {
            BooleanArray::from_unary(column.as_primitive::<Int64Type>(), |x| {
                keys.contains(&x) ^ negated
            })
        }
        (SetIndex::String(keys), DataType::Utf8) => {
            BooleanArray::from_unary(column.as_string::<i32>(), |s| keys.contains(s) ^ negated)
        }
        (SetIndex::String(keys), DataType::LargeUtf8) => {
            BooleanArray::from_unary(column.as_string::<i64>(), |s| keys.contains(s) ^ negated)
        }
        (SetIndex::String(keys), DataType::Utf8View) => {
            BooleanArray::from_unary(column.as_string_view(), |s| keys.contains(s) ^ negated)
        }
        _ => return Ok(None),
    }))
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
        _literals: &FnvHashSet<Datum>,
        predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        self.set_predicate(reference, predicate, false)
    }

    fn not_in(
        &mut self,
        reference: &BoundReference,
        _literals: &FnvHashSet<Datum>,
        predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        self.set_predicate(reference, predicate, true)
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

    use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use parquet::arrow::ArrowSchemaConverter;

    use super::{CollectFieldIdVisitor, PredicateConverter, constant_bool_array, probe_set};
    use crate::expr::visitors::bound_predicate_visitor::visit;
    use crate::expr::{Bind, Predicate, Reference, SetIndex};
    use crate::spec::{Datum, NestedField, PrimitiveType, Schema, SchemaRef, Type};

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
        let id = schema
            .field_by_name(batch.schema().field(0).name())
            .unwrap()
            .id;
        let bound = predicate.bind(schema, true).unwrap();

        let parquet_schema = ArrowSchemaConverter::new()
            .convert(&batch.schema())
            .unwrap();
        let column_map = HashMap::from([(id, 0)]);
        let column_indices = vec![0usize];

        let mut converter = PredicateConverter {
            parquet_schema: &parquet_schema,
            column_map: &column_map,
            column_indices: &column_indices,
        };

        let mut predicate_fn = visit(&mut converter, &bound).unwrap();
        predicate_fn(batch).unwrap()
    }

    fn check_set_filter(column: ArrayRef, ty: PrimitiveType, literals: Vec<Datum>) {
        let schema = Arc::new(
            Schema::builder()
                .with_fields([NestedField::optional(1, "key", Type::Primitive(ty)).into()])
                .build()
                .unwrap(),
        );
        let batch = RecordBatch::try_from_iter([("key", column)])
            .unwrap()
            .slice(1, 4);
        let key = Reference::new("key");
        let matches = [Some(true), None, Some(false), Some(true)];
        assert_eq!(
            apply_predicate_to_batch(
                key.clone().is_in(literals.clone()),
                schema.clone(),
                batch.clone()
            ),
            BooleanArray::from_iter(matches)
        );
        assert_eq!(
            apply_predicate_to_batch(key.is_not_in(literals), schema, batch),
            BooleanArray::from_iter(matches.map(|v| v.map(|matched| !matched)))
        );
    }

    #[test]
    fn integer_set_filters_preserve_nulls_and_physical_casts() {
        use arrow_array::{Int32Array, Int64Array};

        let values = [Some(-1), Some(0), None, Some(-1), Some(63)];
        let ints: ArrayRef = Arc::new(Int32Array::from_iter(values));
        check_set_filter(
            ints.clone(),
            PrimitiveType::Int,
            (0..64).map(Datum::int).collect(),
        );
        let longs: ArrayRef = Arc::new(Int64Array::from_iter(values.map(|v| v.map(i64::from))));
        check_set_filter(
            longs,
            PrimitiveType::Long,
            (0..64).map(Datum::long).collect(),
        );
        // An older file can still contain Int32 after the table column is promoted.
        check_set_filter(
            ints,
            PrimitiveType::Long,
            (0..64).map(Datum::long).collect(),
        );
    }

    #[test]
    fn string_set_filters_match_across_arrow_encodings() {
        use arrow_array::StringArray;
        use arrow_cast::cast::cast;

        let strings: ArrayRef = Arc::new(StringArray::from(vec![
            None,
            Some(""),
            None,
            Some("another long string"),
            Some("a long string outside the view"),
        ]));
        let literals = (0..64)
            .map(|i| Datum::string(i.to_string()))
            .chain([
                Datum::string(""),
                Datum::string("a long string outside the view"),
            ])
            .collect::<Vec<_>>();
        for ty in [
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Utf8View,
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        ] {
            check_set_filter(
                cast(&strings, &ty).unwrap(),
                PrimitiveType::String,
                literals.clone(),
            );
        }
    }

    #[test]
    fn set_index_probes_supported_column_types() {
        use arrow_array::{Int32Array, Int64Array, StringArray};
        use arrow_cast::cast::cast;

        let ints = Int32Array::from(vec![1, 2]);
        let longs = Int64Array::from(vec![1, 2]);
        let strings = StringArray::from(vec!["1", "2"]);
        let int_index = SetIndex::Int([1].into_iter().collect());
        let long_index = SetIndex::Long([1].into_iter().collect());
        let string_index = SetIndex::String([String::from("1")].into_iter().collect());
        let expected = Some(BooleanArray::from(vec![true, false]));
        assert_eq!(probe_set(&int_index, &ints, false).unwrap(), expected);
        assert_eq!(probe_set(&long_index, &longs, false).unwrap(), expected);
        assert_eq!(probe_set(&long_index, &ints, false).unwrap(), None);
        for ty in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            let column = cast(&strings, &ty).unwrap();
            assert_eq!(
                probe_set(&string_index, column.as_ref(), false).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn dictionary_set_probes_preserve_null_keys_and_values() {
        use arrow_array::types::Int32Type;
        use arrow_array::{DictionaryArray, Int32Array, StringArray};

        let values = Arc::new(StringArray::from(vec![
            Some("hit"),
            None,
            Some("miss"),
            Some("unused"),
        ]));
        let keys = Int32Array::from(vec![Some(0), Some(2), None, Some(1), Some(0)]);
        let dictionary = DictionaryArray::<Int32Type>::try_new(keys, values).unwrap();
        let index = SetIndex::String([String::from("hit")].into_iter().collect());
        let matches = [Some(true), Some(false), None, None, Some(true)];
        for negated in [false, true] {
            let expected = BooleanArray::from_iter(matches.map(|v| v.map(|hit| hit ^ negated)));
            assert_eq!(
                probe_set(&index, &dictionary, negated).unwrap(),
                Some(expected)
            );
        }
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
}
