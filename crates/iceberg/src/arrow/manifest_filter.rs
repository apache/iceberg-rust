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

//! Filtering manifests read into Arrow by a scan's row filter.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_arith::boolean::{and, is_null, not, or};
use arrow_array::cast::AsArray;
use arrow_array::types::Int32Type;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Decimal128Array, Float32Array, Float64Array,
    Int32Array, Int64Array, RecordBatch, Scalar,
};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder};
use arrow_ord::cmp::{gt, gt_eq, lt, lt_eq};
use arrow_schema::ArrowError;
use arrow_select::take::take;
use fnv::FnvHashSet;
use typed_builder::TypedBuilder;

use crate::error::invalid_data;
use crate::expr::visitors::bound_predicate_visitor::{BoundPredicateVisitor, visit};
use crate::expr::visitors::inclusive_projection::project_partition_filter;
use crate::expr::visitors::manifest_evaluator::IN_PREDICATE_LIMIT;
use crate::expr::{Bind, BoundPredicate, BoundReference, Predicate};
use crate::spec::decimal_utils::i128_from_be_bytes;
use crate::spec::{Datum, PartitionSpecRef, PrimitiveLiteral, PrimitiveType, SchemaRef};
use crate::{Error, ErrorKind, Result};

/// Evaluates a row filter against batches from
/// [`read_manifest_list`](crate::arrow::read_manifest_list), with the same
/// results as the manifest pruning in [`TableScan`](crate::scan::TableScan).
///
/// # Example
///
/// ```
/// use std::sync::Arc;
///
/// use iceberg::arrow::{ManifestFilter, read_manifest_list};
/// use iceberg::expr::Reference;
/// use iceberg::spec::{Datum, NestedField, PartitionSpec, PrimitiveType, Schema, Type};
///
/// let schema = Arc::new(
///     Schema::builder()
///         .with_fields(vec![
///             NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
///         ])
///         .build()?,
/// );
/// let filter = ManifestFilter::builder()
///     .schema(schema.clone())
///     .partition_specs(vec![Arc::new(PartitionSpec::unpartition_spec())])
///     .filter(Reference::new("id").equal_to(Datum::long(1)))
///     .build()?;
///
/// let bytes = std::fs::read(concat!(
///     env!("CARGO_MANIFEST_DIR"),
///     "/testdata/manifests_lists/manifest-list-v2-2.avro"
/// ))?;
/// for batch in read_manifest_list(&bytes)? {
///     // An unpartitioned manifest may contain any row.
///     let mask = filter.manifest_list_mask(&batch?)?;
///     assert!(mask.values().iter().all(|might_match| might_match));
/// }
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, TypedBuilder)]
#[builder(build_method(into = Result<ManifestFilter>))]
pub struct ManifestFilter {
    schema: SchemaRef,
    partition_specs: Vec<PartitionSpecRef>,
    filter: Predicate,
    #[builder(default = true)]
    case_sensitive: bool,
    /// The filter projected to each spec's partition values, by spec ID.
    #[builder(setter(skip), default)]
    partition_filters: HashMap<i32, PartitionFilter>,
}

#[derive(Debug)]
struct PartitionFilter {
    field_count: usize,
    filter: BoundPredicate,
}

impl From<ManifestFilter> for Result<ManifestFilter> {
    fn from(mut manifest_filter: ManifestFilter) -> Self {
        // `TableScanBuilder::with_filter` rewrites NOT the same way, and the
        // projection rejects NOT.
        let filter = manifest_filter.filter.clone().rewrite_not().bind(
            manifest_filter.schema.clone(),
            manifest_filter.case_sensitive,
        )?;
        for spec in &manifest_filter.partition_specs {
            let partition_filter = PartitionFilter {
                field_count: spec.fields().len(),
                // A spec that references a dropped source column can't prune, as
                // in `TableScan`.
                filter: project_partition_filter(
                    &manifest_filter.schema,
                    spec,
                    &filter,
                    manifest_filter.case_sensitive,
                )?
                .unwrap_or(BoundPredicate::AlwaysTrue),
            };
            let previous = manifest_filter
                .partition_filters
                .insert(spec.spec_id(), partition_filter);
            if previous.is_some() {
                return Err(invalid_data!(
                    "duplicate partition spec ID {}",
                    spec.spec_id()
                ));
            }
        }
        Ok(manifest_filter)
    }
}

impl ManifestFilter {
    /// Returns true for each manifest in `batch` that may contain rows matching
    /// the filter, judged by its partition summaries.
    ///
    /// # Errors
    ///
    /// Returns an error if `batch` doesn't have the schema that
    /// [`read_manifest_list`](crate::arrow::read_manifest_list) returns, if a
    /// manifest's partition spec isn't one the filter was built with, or if its
    /// partition summaries don't match the spec.
    pub fn manifest_list_mask(&self, batch: &RecordBatch) -> Result<BooleanArray> {
        let spec_ids = column(batch, "partition_spec_id")?
            .as_primitive_opt::<Int32Type>()
            .ok_or_else(|| invalid_data!("partition_spec_id is not an int column"))?;
        let partitions = column(batch, "partitions")?
            .as_list_opt::<i32>()
            .ok_or_else(|| invalid_data!("partitions is not a list column"))?;
        let summaries = SummaryColumns::try_new(partitions.values())?;

        // A manifest without partition summaries may contain any row, as in
        // `ManifestEvaluator`.
        let mut rows_by_spec: HashMap<i32, Vec<usize>> = HashMap::new();
        for row in 0..batch.num_rows() {
            if partitions.is_valid(row) && partitions.value_length(row) > 0 {
                if spec_ids.is_null(row) {
                    return Err(invalid_data!("manifest has no partition_spec_id"));
                }
                rows_by_spec
                    .entry(spec_ids.value(row))
                    .or_default()
                    .push(row);
            }
        }

        let mut mask = BooleanBufferBuilder::new(batch.num_rows());
        mask.append_n(batch.num_rows(), true);
        for (spec_id, rows) in rows_by_spec {
            let partition_filter = self
                .partition_filters
                .get(&spec_id)
                .ok_or_else(|| invalid_data!("no partition spec with ID {spec_id}"))?;
            let mut starts = Vec::with_capacity(rows.len());
            for &row in &rows {
                let length = usize::try_from(partitions.value_length(row))?;
                if length != partition_filter.field_count {
                    return Err(invalid_data!(
                        "manifest has {length} partition summaries but partition spec {spec_id} has {} fields",
                        partition_filter.field_count
                    ));
                }
                starts.push(partitions.value_offsets()[row]);
            }
            if matches!(partition_filter.filter, BoundPredicate::AlwaysTrue) {
                continue;
            }
            let mut visitor = SummaryVisitor {
                summaries: &summaries,
                starts,
                fields: HashMap::new(),
            };
            let spec_mask = visit(&mut visitor, &partition_filter.filter)?;
            for (index, row) in rows.into_iter().enumerate() {
                mask.set_bit(row, spec_mask.value(index));
            }
        }
        Ok(BooleanArray::new(mask.finish(), None))
    }
}

fn column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef> {
    batch
        .column_by_name(name)
        .ok_or_else(|| invalid_data!("manifest list batch has no {name} column"))
}

/// The `field_summary` columns of a manifest list's `partitions`, with one row
/// per summary.
struct SummaryColumns<'a> {
    contains_null: &'a BooleanArray,
    contains_nan: &'a BooleanArray,
    lower_bound: &'a BinaryArray,
    upper_bound: &'a BinaryArray,
}

impl<'a> SummaryColumns<'a> {
    fn try_new(summaries: &'a ArrayRef) -> Result<Self> {
        let summaries = summaries
            .as_struct_opt()
            .ok_or_else(|| invalid_data!("partitions is not a list of structs"))?;
        let field = |name: &str| {
            summaries
                .column_by_name(name)
                .ok_or_else(|| invalid_data!("partition summaries have no {name} field"))
        };
        let boolean = |name: &str| {
            field(name)?
                .as_boolean_opt()
                .ok_or_else(|| invalid_data!("partition summary {name} is not a boolean"))
        };
        let binary = |name: &str| {
            field(name)?
                .as_binary_opt::<i32>()
                .ok_or_else(|| invalid_data!("partition summary {name} is not binary"))
        };
        Ok(Self {
            contains_null: boolean("contains_null")?,
            contains_nan: boolean("contains_nan")?,
            lower_bound: binary("lower_bound")?,
            upper_bound: binary("upper_bound")?,
        })
    }
}

/// One partition field's summaries for the rows being evaluated.
struct FieldSummaries {
    contains_null: BooleanArray,
    /// Null where the manifest list doesn't say.
    contains_nan: BooleanArray,
    lower_bound: BinaryArray,
    upper_bound: BinaryArray,
    /// The bounds decoded into arrays that order values the way `Datum` does.
    lower: ArrayRef,
    upper: ArrayRef,
    is_floating: bool,
}

impl FieldSummaries {
    /// Whether every partition value is null, as in `ManifestEvaluator`.
    fn are_all_null(&self) -> Result<BooleanArray> {
        let all_null = and(&self.contains_null, &is_null(&self.lower_bound)?)?;
        if self.is_floating {
            // NaN values have no bounds either, so a float field is all null
            // only if it's known to contain no NaN.
            let no_nan = null_as_false(&not(&self.contains_nan)?);
            Ok(and(&all_null, &no_nan)?)
        } else {
            Ok(all_null)
        }
    }
}

/// Evaluates a partition filter over the summaries of the manifests whose
/// summaries start at `starts`.
struct SummaryVisitor<'a> {
    summaries: &'a SummaryColumns<'a>,
    starts: Vec<i32>,
    /// Summaries by partition field position, taken when a predicate first
    /// references the field.
    fields: HashMap<usize, FieldSummaries>,
}

impl SummaryVisitor<'_> {
    fn field(&mut self, reference: &BoundReference) -> Result<&FieldSummaries> {
        let position = reference.accessor().position();
        if !self.fields.contains_key(&position) {
            let field_type = reference
                .field()
                .field_type
                .as_primitive_type()
                .ok_or_else(|| invalid_data!("partition field is not a primitive type"))?;
            let offset = i32::try_from(position)?;
            let indices = Int32Array::from(
                self.starts
                    .iter()
                    .map(|start| {
                        start
                            .checked_add(offset)
                            .ok_or_else(|| invalid_data!("partition summary index overflows"))
                    })
                    .collect::<Result<Vec<_>>>()?,
            );
            let take_boolean = |array: &BooleanArray| -> Result<BooleanArray> {
                Ok(take(array, &indices, None)?.as_boolean().clone())
            };
            let take_binary = |array: &BinaryArray| -> Result<BinaryArray> {
                Ok(take(array, &indices, None)?.as_binary::<i32>().clone())
            };
            let lower_bound = take_binary(self.summaries.lower_bound)?;
            let upper_bound = take_binary(self.summaries.upper_bound)?;
            let summaries = FieldSummaries {
                contains_null: null_as_false(&take_boolean(self.summaries.contains_null)?),
                contains_nan: take_boolean(self.summaries.contains_nan)?,
                lower: decode_bounds(&lower_bound, field_type)?,
                upper: decode_bounds(&upper_bound, field_type)?,
                lower_bound,
                upper_bound,
                is_floating: matches!(field_type, PrimitiveType::Float | PrimitiveType::Double),
            };
            self.fields.insert(position, summaries);
        }
        Ok(&self.fields[&position])
    }

    fn constant(&self, value: bool) -> BooleanArray {
        let values = if value {
            BooleanBuffer::new_set(self.starts.len())
        } else {
            BooleanBuffer::new_unset(self.starts.len())
        };
        BooleanArray::new(values, None)
    }
}

impl BoundPredicateVisitor for SummaryVisitor<'_> {
    type T = BooleanArray;

    fn always_true(&mut self) -> Result<BooleanArray> {
        Ok(self.constant(true))
    }

    fn always_false(&mut self) -> Result<BooleanArray> {
        Ok(self.constant(false))
    }

    fn and(&mut self, lhs: BooleanArray, rhs: BooleanArray) -> Result<BooleanArray> {
        Ok(and(&lhs, &rhs)?)
    }

    fn or(&mut self, lhs: BooleanArray, rhs: BooleanArray) -> Result<BooleanArray> {
        Ok(or(&lhs, &rhs)?)
    }

    fn not(&mut self, _inner: BooleanArray) -> Result<BooleanArray> {
        Err(Error::new(
            ErrorKind::Unexpected,
            "not operator is not supported in partition filter",
        ))
    }

    fn is_null(&mut self, reference: &BoundReference, _: &BoundPredicate) -> Result<BooleanArray> {
        Ok(self.field(reference)?.contains_null.clone())
    }

    fn not_null(&mut self, reference: &BoundReference, _: &BoundPredicate) -> Result<BooleanArray> {
        Ok(not(&self.field(reference)?.are_all_null()?)?)
    }

    fn is_nan(&mut self, reference: &BoundReference, _: &BoundPredicate) -> Result<BooleanArray> {
        let field = self.field(reference)?;
        let no_nan = null_as_false(&not(&field.contains_nan)?);
        Ok(not(&or(&no_nan, &field.are_all_null()?)?)?)
    }

    fn not_nan(&mut self, reference: &BoundReference, _: &BoundPredicate) -> Result<BooleanArray> {
        // A field is all NaN if it contains NaN but no null and has no bounds.
        let field = self.field(reference)?;
        let all_nan = and(
            &and(
                &null_as_false(&field.contains_nan),
                &not(&field.contains_null)?,
            )?,
            &is_null(&field.lower_bound)?,
        )?;
        Ok(not(&all_nan)?)
    }

    fn less_than(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _: &BoundPredicate,
    ) -> Result<BooleanArray> {
        let literal = literal_scalar(datum)?;
        compare(lt, &self.field(reference)?.lower, &literal)
    }

    fn less_than_or_eq(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _: &BoundPredicate,
    ) -> Result<BooleanArray> {
        let literal = literal_scalar(datum)?;
        compare(lt_eq, &self.field(reference)?.lower, &literal)
    }

    fn greater_than(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _: &BoundPredicate,
    ) -> Result<BooleanArray> {
        let literal = literal_scalar(datum)?;
        compare(gt, &self.field(reference)?.upper, &literal)
    }

    fn greater_than_or_eq(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _: &BoundPredicate,
    ) -> Result<BooleanArray> {
        let literal = literal_scalar(datum)?;
        compare(gt_eq, &self.field(reference)?.upper, &literal)
    }

    fn eq(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _: &BoundPredicate,
    ) -> Result<BooleanArray> {
        let literal = literal_scalar(datum)?;
        let field = self.field(reference)?;
        Ok(and(
            &compare(lt_eq, &field.lower, &literal)?,
            &compare(gt_eq, &field.upper, &literal)?,
        )?)
    }

    fn not_eq(
        &mut self,
        _: &BoundReference,
        _: &Datum,
        _: &BoundPredicate,
    ) -> Result<BooleanArray> {
        // The bounds aren't necessarily values in the manifest, so they can't
        // rule out a value other than the literal.
        Ok(self.constant(true))
    }

    fn starts_with(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _: &BoundPredicate,
    ) -> Result<BooleanArray> {
        let prefix = string_literal(datum, "Cannot perform starts_with on non-string value")?;
        let field = self.field(reference)?;
        // Matches `ManifestEvaluator`, which compares the prefix with each
        // bound truncated to the prefix length.
        let values = BooleanBuffer::collect_bool(field.lower_bound.len(), |row| {
            let (Some(lower), Some(upper)) = (
                field
                    .lower_bound
                    .is_valid(row)
                    .then(|| field.lower_bound.value(row)),
                field
                    .upper_bound
                    .is_valid(row)
                    .then(|| field.upper_bound.value(row)),
            ) else {
                return false;
            };
            prefix >= &lower[..lower.len().min(prefix.len())]
                && prefix <= &upper[..upper.len().min(prefix.len())]
        });
        Ok(BooleanArray::new(values, None))
    }

    fn not_starts_with(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _: &BoundPredicate,
    ) -> Result<BooleanArray> {
        let prefix = string_literal(datum, "Cannot perform not_starts_with on non-string value")?;
        let field = self.field(reference)?;
        // Every value starts with the prefix only if both bounds do.
        let values = BooleanBuffer::collect_bool(field.lower_bound.len(), |row| {
            if field.contains_null.value(row)
                || field.lower_bound.is_null(row)
                || field.upper_bound.is_null(row)
            {
                return true;
            }
            let both_start_with_prefix = field.lower_bound.value(row).starts_with(prefix)
                && field.upper_bound.value(row).starts_with(prefix);
            !both_start_with_prefix
        });
        Ok(BooleanArray::new(values, None))
    }

    fn r#in(
        &mut self,
        reference: &BoundReference,
        literals: &FnvHashSet<Datum>,
        _: &BoundPredicate,
    ) -> Result<BooleanArray> {
        let field = self.field(reference)?;
        // A missing lower bound means every value is null or NaN.
        let has_lower = not(&is_null(&field.lower_bound)?)?;
        if literals.len() > IN_PREDICATE_LIMIT {
            return Ok(has_lower);
        }
        let no_upper = is_null(&field.upper_bound)?;
        let mut any_inside = BooleanArray::new(BooleanBuffer::new_unset(has_lower.len()), None);
        for literal in literals {
            let literal = literal_scalar(literal)?;
            let inside = and(
                &compare(lt_eq, &field.lower, &literal)?,
                &or(&no_upper, &compare(gt_eq, &field.upper, &literal)?)?,
            )?;
            any_inside = or(&any_inside, &inside)?;
        }
        Ok(and(&has_lower, &any_inside)?)
    }

    fn not_in(
        &mut self,
        _: &BoundReference,
        _: &FnvHashSet<Datum>,
        _: &BoundPredicate,
    ) -> Result<BooleanArray> {
        // The bounds aren't necessarily values in the manifest, so they can't
        // rule out values other than the literals.
        Ok(self.constant(true))
    }
}

/// Compares `bounds` with `literal`, with false where a bound is missing.
fn compare(
    op: fn(
        &dyn arrow_array::Datum,
        &dyn arrow_array::Datum,
    ) -> std::result::Result<BooleanArray, ArrowError>,
    bounds: &ArrayRef,
    literal: &Scalar<ArrayRef>,
) -> Result<BooleanArray> {
    Ok(null_as_false(&op(bounds, literal)?))
}

fn null_as_false(array: &BooleanArray) -> BooleanArray {
    match array.nulls() {
        Some(nulls) => BooleanArray::new(array.values() & nulls.inner(), None),
        None => array.clone(),
    }
}

fn string_literal<'a>(datum: &'a Datum, message: &str) -> Result<&'a [u8]> {
    match datum.literal() {
        PrimitiveLiteral::String(value) => Ok(value.as_bytes()),
        _ => Err(Error::new(ErrorKind::Unexpected, message)),
    }
}

/// Decodes binary single-value serialized bounds the way
/// `Datum::try_from_bytes` does, into an array that Arrow orders the way
/// `Datum` does. Both order floats by IEEE 754 `totalOrder`. Strings, binary,
/// fixed, and uuid compare bytewise, so their bounds stay as they are.
fn decode_bounds(bounds: &BinaryArray, field_type: &PrimitiveType) -> Result<ArrayRef> {
    fn decode<T>(
        bounds: &BinaryArray,
        decode: impl Fn(&[u8]) -> Result<T>,
    ) -> Result<Vec<Option<T>>> {
        bounds
            .iter()
            .map(|bound| bound.map(&decode).transpose())
            .collect()
    }

    let array: ArrayRef = match field_type {
        PrimitiveType::Boolean => {
            Arc::new(BooleanArray::from(decode(
                bounds,
                |bytes| Ok(bytes != [0]),
            )?))
        }
        PrimitiveType::Int | PrimitiveType::Date => {
            Arc::new(Int32Array::from(decode(bounds, |bytes| {
                Ok(i32::from_le_bytes(bytes.try_into()?))
            })?))
        }
        PrimitiveType::Long => Arc::new(Int64Array::from(decode(bounds, |bytes| {
            // A 4-byte bound was written before the field was promoted from int.
            Ok(match bytes.len() {
                4 => i64::from(i32::from_le_bytes(bytes.try_into()?)),
                _ => i64::from_le_bytes(bytes.try_into()?),
            })
        })?)),
        PrimitiveType::Time
        | PrimitiveType::Timestamp
        | PrimitiveType::Timestamptz
        | PrimitiveType::TimestampNs
        | PrimitiveType::TimestamptzNs => Arc::new(Int64Array::from(decode(bounds, |bytes| {
            Ok(i64::from_le_bytes(bytes.try_into()?))
        })?)),
        PrimitiveType::Float => Arc::new(Float32Array::from(decode(bounds, |bytes| {
            Ok(f32::from_le_bytes(bytes.try_into()?))
        })?)),
        PrimitiveType::Double => Arc::new(Float64Array::from(decode(bounds, |bytes| {
            // A 4-byte bound was written before the field was promoted from float.
            Ok(match bytes.len() {
                4 => f64::from(f32::from_le_bytes(bytes.try_into()?)),
                _ => f64::from_le_bytes(bytes.try_into()?),
            })
        })?)),
        // The default precision and scale are safe because only the order of
        // unscaled values matters, and binding gives the literal the field's
        // scale. `literal_scalar` uses the same default, so the types match.
        PrimitiveType::Decimal { .. } => {
            Arc::new(Decimal128Array::from(decode(bounds, |bytes| {
                i128_from_be_bytes(bytes)
                    .ok_or_else(|| invalid_data!("Can't convert bytes to i128: {bytes:?}"))
            })?))
        }
        PrimitiveType::String
        | PrimitiveType::Uuid
        | PrimitiveType::Fixed(_)
        | PrimitiveType::Binary => Arc::new(bounds.clone()),
        PrimitiveType::Unknown | PrimitiveType::Geometry(_) | PrimitiveType::Geography(_) => {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!("Partition summaries of type {field_type} are not supported"),
            ));
        }
    };
    Ok(array)
}

/// Converts a literal to a one-element array of the type `decode_bounds`
/// returns for the literal's type.
fn literal_scalar(datum: &Datum) -> Result<Scalar<ArrayRef>> {
    let array: ArrayRef = match datum.literal() {
        PrimitiveLiteral::Boolean(value) => Arc::new(BooleanArray::from(vec![*value])),
        PrimitiveLiteral::Int(value) => Arc::new(Int32Array::from(vec![*value])),
        PrimitiveLiteral::Long(value) => Arc::new(Int64Array::from(vec![*value])),
        PrimitiveLiteral::Float(value) => Arc::new(Float32Array::from(vec![value.into_inner()])),
        PrimitiveLiteral::Double(value) => Arc::new(Float64Array::from(vec![value.into_inner()])),
        PrimitiveLiteral::Int128(value) => Arc::new(Decimal128Array::from(vec![*value])),
        PrimitiveLiteral::String(value) => Arc::new(BinaryArray::from_vec(vec![value.as_bytes()])),
        PrimitiveLiteral::Binary(value) => Arc::new(BinaryArray::from_vec(vec![value.as_slice()])),
        PrimitiveLiteral::UInt128(value) => {
            // Uuid bounds are big-endian, which orders them like the integer.
            Arc::new(BinaryArray::from_vec(vec![value.to_be_bytes().as_slice()]))
        }
        PrimitiveLiteral::AboveMax | PrimitiveLiteral::BelowMin => {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!("Cannot compare partition summaries with {datum}"),
            ));
        }
    };
    Ok(Scalar::new(array))
}

#[cfg(test)]
mod tests {
    use arrow_select::concat::concat_batches;
    use uuid::Uuid;

    use super::*;
    use crate::arrow::read_manifest_list;
    use crate::expr::Reference;
    use crate::expr::visitors::manifest_evaluator::ManifestEvaluator;
    use crate::io::FileIO;
    use crate::spec::{
        FieldSummary, FormatVersion, ManifestContentType, ManifestFile, ManifestListWriter,
        NestedField, PartitionSpec, Schema, Transform, Type,
    };

    fn table_schema() -> SchemaRef {
        let columns = [
            (1, "i", PrimitiveType::Int),
            (2, "l", PrimitiveType::Long),
            (3, "f", PrimitiveType::Float),
            (4, "d", PrimitiveType::Double),
            (5, "s", PrimitiveType::String),
            (6, "date", PrimitiveType::Date),
            (7, "dec", PrimitiveType::Decimal {
                precision: 10,
                scale: 2,
            }),
            (8, "b", PrimitiveType::Boolean),
            (9, "bin", PrimitiveType::Binary),
            (10, "fx", PrimitiveType::Fixed(3)),
            (11, "ts", PrimitiveType::Timestamp),
            (12, "u", PrimitiveType::Uuid),
        ];
        Arc::new(
            Schema::builder()
                .with_fields(columns.into_iter().map(|(id, name, ty)| {
                    NestedField::optional(id, name, Type::Primitive(ty)).into()
                }))
                .build()
                .unwrap(),
        )
    }

    fn partition_spec(
        schema: &SchemaRef,
        spec_id: i32,
        fields: &[(&str, Transform)],
    ) -> PartitionSpecRef {
        let spec = fields
            .iter()
            .fold(
                PartitionSpec::builder(schema.clone()).with_spec_id(spec_id),
                |builder, (source, transform)| {
                    builder
                        .add_partition_field(*source, format!("{source}_{transform}"), *transform)
                        .unwrap()
                },
            )
            .build()
            .unwrap();
        Arc::new(spec)
    }

    /// Spec 0 partitions by every column. Spec 1 uses transforms whose
    /// result types differ from their sources.
    fn partition_specs() -> Vec<PartitionSpecRef> {
        let columns = [
            "i", "l", "f", "d", "s", "date", "dec", "b", "bin", "fx", "ts", "u",
        ];
        let identity: Vec<_> = columns.iter().map(|c| (*c, Transform::Identity)).collect();
        let schema = table_schema();
        vec![
            partition_spec(&schema, 0, &identity),
            partition_spec(&schema, 1, &[
                ("i", Transform::Bucket(4)),
                ("s", Transform::Truncate(2)),
                ("ts", Transform::Day),
            ]),
        ]
    }

    /// Five ascending values for each column. A manifest's bounds are the
    /// second and fourth.
    fn values(column: &str) -> [Datum; 5] {
        match column {
            "i" => [5, 10, 15, 20, 25].map(Datum::int),
            "l" => [5, 10, 15, 20, 25].map(Datum::long),
            "f" => [0.5_f32, 1.5, 2.0, 2.5, 3.5].map(Datum::float),
            "d" => [0.5, 1.5, 2.0, 2.5, 3.5].map(Datum::double),
            "s" => ["aaa", "abc", "abm", "abz", "b"].map(Datum::string),
            "date" => [50, 100, 150, 200, 250].map(Datum::date),
            "dec" => ["0.50", "1.50", "2.00", "2.50", "3.50"]
                .map(|v| Datum::decimal_from_str(v).unwrap()),
            "b" => [false, false, true, true, true].map(Datum::bool),
            "bin" => [vec![0], vec![1, 2], vec![1, 5], vec![1, 9], vec![2]].map(Datum::binary),
            "fx" => [[0, 0, 0], [1, 2, 3], [1, 5, 5], [1, 9, 9], [2, 0, 0]].map(Datum::fixed),
            "ts" => [500, 1000, 1500, 2000, 2500].map(Datum::timestamp_micros),
            "u" => [1, 2, 3, 4, 5].map(|v| Datum::uuid(Uuid::from_u128(v))),
            _ => unreachable!("no column {column}"),
        }
    }

    /// Bounds for spec 1's fields: `bucket[4](i)`, `truncate[2](s)`, and
    /// `day(ts)`.
    fn transformed_bounds() -> [(Datum, Datum); 3] {
        [
            (Datum::int(1), Datum::int(2)),
            (Datum::string("ab"), Datum::string("ab")),
            (Datum::date(0), Datum::date(1)),
        ]
    }

    #[derive(Clone, Copy, Debug)]
    enum Summary {
        Bounded,
        BoundedWithNull,
        AllNull,
        AllNan,
        NanUnknown,
    }

    fn field_summary(summary: Summary, lower: &Datum, upper: &Datum) -> FieldSummary {
        let bounds = || {
            (
                Some(lower.to_bytes().unwrap()),
                Some(upper.to_bytes().unwrap()),
            )
        };
        let (contains_null, contains_nan, (lower_bound, upper_bound)) = match summary {
            Summary::Bounded => (false, Some(false), bounds()),
            Summary::BoundedWithNull => (true, Some(false), bounds()),
            Summary::AllNull => (true, Some(false), (None, None)),
            Summary::AllNan => (false, Some(true), (None, None)),
            Summary::NanUnknown => (false, None, bounds()),
        };
        FieldSummary {
            contains_null,
            contains_nan,
            lower_bound,
            upper_bound,
        }
    }

    fn manifest(path: &str, spec_id: i32, partitions: Option<Vec<FieldSummary>>) -> ManifestFile {
        ManifestFile {
            manifest_path: path.to_string(),
            manifest_length: 1,
            partition_spec_id: spec_id,
            content: ManifestContentType::Data,
            sequence_number: 1,
            min_sequence_number: 1,
            added_snapshot_id: 1,
            added_files_count: Some(1),
            existing_files_count: Some(0),
            deleted_files_count: Some(0),
            added_rows_count: Some(1),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            partitions,
            key_metadata: None,
            first_row_id: None,
        }
    }

    /// One manifest per summary kind for each spec, then one manifest
    /// without partition summaries and one with an empty list.
    fn manifests() -> Vec<ManifestFile> {
        let kinds = [
            Summary::Bounded,
            Summary::BoundedWithNull,
            Summary::AllNull,
            Summary::AllNan,
            Summary::NanUnknown,
        ];
        let identity_columns = partition_specs()[0]
            .fields()
            .iter()
            .map(|field| field.name.trim_end_matches("_identity").to_string())
            .collect::<Vec<_>>();
        let mut manifests = Vec::new();
        for kind in kinds {
            let summaries = identity_columns
                .iter()
                .map(|column| {
                    let values = values(column);
                    field_summary(kind, &values[1], &values[3])
                })
                .collect();
            manifests.push(manifest(&format!("spec0-{kind:?}"), 0, Some(summaries)));
            let summaries = transformed_bounds()
                .iter()
                .map(|(lower, upper)| field_summary(kind, lower, upper))
                .collect();
            manifests.push(manifest(&format!("spec1-{kind:?}"), 1, Some(summaries)));
        }
        manifests.push(manifest("no-summaries", 0, None));
        manifests.push(manifest("empty-summaries", 0, Some(vec![])));
        manifests
    }

    fn predicates() -> Vec<Predicate> {
        let mut predicates = vec![Predicate::AlwaysTrue, Predicate::AlwaysFalse];
        for column in [
            "i", "l", "f", "d", "s", "date", "dec", "bin", "fx", "ts", "u",
        ] {
            let values = values(column);
            let r = || Reference::new(column);
            predicates.push(r().is_null());
            predicates.push(r().is_not_null());
            for v in &values {
                predicates.push(r().less_than(v.clone()));
                predicates.push(r().less_than_or_equal_to(v.clone()));
                predicates.push(r().greater_than(v.clone()));
                predicates.push(r().greater_than_or_equal_to(v.clone()));
                predicates.push(r().equal_to(v.clone()));
                predicates.push(r().not_equal_to(v.clone()));
                predicates.push(r().is_in([v.clone()]));
                predicates.push(r().is_not_in([v.clone()]));
            }
            predicates.push(r().is_in([values[0].clone(), values[4].clone()]));
            predicates.push(r().is_in([values[0].clone(), values[2].clone()]));
        }
        for column in ["f", "d"] {
            predicates.push(Reference::new(column).is_nan());
            predicates.push(Reference::new(column).is_not_nan());
        }
        let b = || Reference::new("b");
        predicates.extend([
            b().equal_to(Datum::bool(true)),
            b().not_equal_to(Datum::bool(true)),
        ]);
        for prefix in ["a", "ab", "abc", "abm", "abcd", "abz", "abzz", "b"] {
            predicates.push(Reference::new("s").starts_with(Datum::string(prefix)));
            predicates.push(Reference::new("s").not_starts_with(Datum::string(prefix)));
        }
        // More literals than `IN_PREDICATE_LIMIT`, all outside the bounds, which
        // `ManifestEvaluator` doesn't check.
        predicates.push(Reference::new("i").is_in((1000..=1200).map(Datum::int)));
        let lt = Reference::new("i").less_than(Datum::int(5));
        let gt = Reference::new("l").greater_than(Datum::long(15));
        predicates.extend([
            lt.clone().and(gt.clone()),
            lt.clone().or(gt.clone()),
            !lt.clone(),
            !(lt.or(gt)),
        ]);
        predicates
    }

    async fn write_manifest_list(version: FormatVersion, manifests: Vec<ManifestFile>) -> Vec<u8> {
        let io = FileIO::new_with_memory();
        let path = "memory:///manifest-list.avro";
        let output = io.new_output(path).unwrap().writer().await.unwrap();
        let mut writer = match version {
            FormatVersion::V1 => ManifestListWriter::v1(output, 1, None),
            _ => ManifestListWriter::v2(output, 1, None, 1),
        };
        writer.add_manifests(manifests.into_iter()).unwrap();
        writer.close().await.unwrap();
        io.new_input(path).unwrap().read().await.unwrap().to_vec()
    }

    async fn manifest_list_batch(
        version: FormatVersion,
        manifests: Vec<ManifestFile>,
    ) -> RecordBatch {
        let bytes = write_manifest_list(version, manifests).await;
        let reader = read_manifest_list(&bytes).unwrap();
        let schema = reader.schema();
        let batches = reader.collect::<Result<Vec<_>>>().unwrap();
        concat_batches(&schema, &batches).unwrap()
    }

    /// Evaluates `filter` on each manifest the way `TableScan` does, which
    /// rewrites NOT before binding (`TableScanBuilder::with_filter`).
    fn manifest_evaluator_results(
        schema: &SchemaRef,
        specs: &[PartitionSpecRef],
        filter: &Predicate,
        case_sensitive: bool,
        manifests: &[ManifestFile],
    ) -> BooleanArray {
        let bound = filter
            .clone()
            .rewrite_not()
            .bind(schema.clone(), case_sensitive)
            .unwrap();
        let results = manifests
            .iter()
            .map(|manifest| {
                let spec = specs
                    .iter()
                    .find(|spec| spec.spec_id() == manifest.partition_spec_id)
                    .unwrap();
                let partition_filter =
                    project_partition_filter(schema, spec, &bound, case_sensitive)
                        .unwrap()
                        .unwrap_or(BoundPredicate::AlwaysTrue);
                ManifestEvaluator::builder(partition_filter)
                    .build()
                    .eval(manifest)
                    .unwrap()
            })
            .collect::<Vec<_>>();
        BooleanArray::from(results)
    }

    /// Checks that the mask for each predicate matches `ManifestEvaluator`,
    /// and returns how many manifests the predicates pruned in total.
    async fn assert_masks_match_manifest_evaluator(
        schema: &SchemaRef,
        specs: &[PartitionSpecRef],
        manifests: Vec<ManifestFile>,
        predicates: Vec<Predicate>,
        version: FormatVersion,
        case_sensitive: bool,
    ) -> usize {
        let batch = manifest_list_batch(version, manifests.clone()).await;
        let expected = predicates
            .into_iter()
            .map(|predicate| {
                let results = manifest_evaluator_results(
                    schema,
                    specs,
                    &predicate,
                    case_sensitive,
                    &manifests,
                );
                (predicate, results)
            })
            .collect::<Vec<_>>();
        let mut pruned = 0;
        for (predicate, expected) in expected {
            let filter = ManifestFilter::builder()
                .schema(schema.clone())
                .partition_specs(specs.to_vec())
                .filter(predicate.clone())
                .case_sensitive(case_sensitive)
                .build()
                .unwrap();
            let mask = filter.manifest_list_mask(&batch).unwrap();
            assert_eq!(mask, expected, "{predicate}");
            pruned += expected.false_count();
        }
        pruned
    }

    #[tokio::test]
    async fn test_manifest_list_mask_matches_manifest_evaluator() {
        for version in [FormatVersion::V1, FormatVersion::V2] {
            let pruned = assert_masks_match_manifest_evaluator(
                &table_schema(),
                &partition_specs(),
                manifests(),
                predicates(),
                version,
                true,
            )
            .await;
            assert!(pruned > 0, "{version}");
        }
    }

    #[tokio::test]
    async fn test_case_insensitive_filter() {
        let predicates = vec![
            Reference::new("I").less_than(Datum::int(10)),
            Reference::new("S").starts_with(Datum::string("abm")),
        ];
        let pruned = assert_masks_match_manifest_evaluator(
            &table_schema(),
            &partition_specs(),
            manifests(),
            predicates,
            FormatVersion::V2,
            false,
        )
        .await;
        assert!(pruned > 0);

        let result = ManifestFilter::builder()
            .schema(table_schema())
            .partition_specs(partition_specs())
            .filter(Reference::new("I").less_than(Datum::int(10)))
            .build();
        assert!(result.is_err());
    }

    /// `Datum` orders -0.0 before 0.0, as IEEE 754 `totalOrder` does.
    #[tokio::test]
    async fn test_signed_zero_bounds() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "f", Type::Primitive(PrimitiveType::Float)).into(),
                    NestedField::optional(2, "d", Type::Primitive(PrimitiveType::Double)).into(),
                ])
                .build()
                .unwrap(),
        );
        let specs = vec![partition_spec(&schema, 0, &[
            ("f", Transform::Identity),
            ("d", Transform::Identity),
        ])];
        let manifests = [(-0.0, -0.0), (-0.0, 0.0), (0.0, 0.0)]
            .into_iter()
            .map(|(lower, upper): (f32, f32)| {
                let summaries = vec![
                    field_summary(Summary::Bounded, &Datum::float(lower), &Datum::float(upper)),
                    field_summary(
                        Summary::Bounded,
                        &Datum::double(f64::from(lower)),
                        &Datum::double(f64::from(upper)),
                    ),
                ];
                manifest(&format!("{lower}-{upper}"), 0, Some(summaries))
            })
            .collect();
        let mut predicates = Vec::new();
        for zero in [-0.0_f32, 0.0] {
            for literal in [Datum::float(zero), Datum::double(f64::from(zero))] {
                let r = || {
                    let column = if matches!(literal.data_type(), PrimitiveType::Float) {
                        "f"
                    } else {
                        "d"
                    };
                    Reference::new(column)
                };
                predicates.extend([
                    r().less_than(literal.clone()),
                    r().less_than_or_equal_to(literal.clone()),
                    r().greater_than(literal.clone()),
                    r().greater_than_or_equal_to(literal.clone()),
                    r().equal_to(literal.clone()),
                    r().is_in([literal.clone()]),
                ]);
            }
        }
        let pruned = assert_masks_match_manifest_evaluator(
            &schema,
            &specs,
            manifests,
            predicates,
            FormatVersion::V2,
            true,
        )
        .await;
        assert!(pruned > 0);
    }

    /// `TableScan` doesn't prune a spec that references a dropped source column.
    #[tokio::test]
    async fn test_spec_with_dropped_source_column_is_not_pruned() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(2, "l", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap(),
        );
        let pruned = assert_masks_match_manifest_evaluator(
            &schema,
            &partition_specs(),
            manifests(),
            vec![Reference::new("l").less_than(Datum::long(0))],
            FormatVersion::V2,
            true,
        )
        .await;
        assert_eq!(pruned, 0);
    }

    #[tokio::test]
    async fn test_empty_manifest_list() {
        let batch = manifest_list_batch(FormatVersion::V2, vec![]).await;
        let filter = ManifestFilter::builder()
            .schema(table_schema())
            .partition_specs(partition_specs())
            .filter(Reference::new("i").less_than(Datum::int(10)))
            .build()
            .unwrap();
        assert_eq!(filter.manifest_list_mask(&batch).unwrap().len(), 0);
    }

    fn assert_data_invalid(result: Result<BooleanArray>, message: &str) {
        let error = result.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert!(error.message().contains(message), "{error}");
    }

    #[tokio::test]
    async fn test_unknown_partition_spec_is_an_error() {
        let batch = manifest_list_batch(FormatVersion::V2, manifests()).await;
        let filter = ManifestFilter::builder()
            .schema(table_schema())
            .partition_specs(partition_specs()[1..].to_vec())
            .filter(Reference::new("i").less_than(Datum::int(10)))
            .build()
            .unwrap();
        assert_data_invalid(
            filter.manifest_list_mask(&batch),
            "no partition spec with ID 0",
        );
    }

    #[tokio::test]
    async fn test_wrong_number_of_summaries_is_an_error() {
        let summaries = transformed_bounds()[..2]
            .iter()
            .map(|(lower, upper)| field_summary(Summary::Bounded, lower, upper))
            .collect();
        let batch =
            manifest_list_batch(FormatVersion::V2, vec![manifest("m", 1, Some(summaries))]).await;
        let filter = ManifestFilter::builder()
            .schema(table_schema())
            .partition_specs(partition_specs())
            .filter(Reference::new("i").less_than(Datum::int(10)))
            .build()
            .unwrap();
        assert_data_invalid(
            filter.manifest_list_mask(&batch),
            "manifest has 2 partition summaries but partition spec 1 has 3 fields",
        );
    }

    /// `ManifestEvaluator` panics on a bound that doesn't decode.
    #[tokio::test]
    async fn test_malformed_bound_is_an_error() {
        let mut summaries = transformed_bounds()
            .iter()
            .map(|(lower, upper)| field_summary(Summary::Bounded, lower, upper))
            .collect::<Vec<_>>();
        // The bucket field is an int, which has 4 bytes.
        summaries[0].lower_bound = Some(vec![1, 2, 3].into());
        let batch =
            manifest_list_batch(FormatVersion::V2, vec![manifest("m", 1, Some(summaries))]).await;
        let filter = ManifestFilter::builder()
            .schema(table_schema())
            .partition_specs(partition_specs())
            .filter(Reference::new("i").equal_to(Datum::int(10)))
            .build()
            .unwrap();
        assert!(filter.manifest_list_mask(&batch).is_err());
    }

    #[test]
    fn test_duplicate_partition_spec_ids_are_an_error() {
        let spec = partition_specs()[0].clone();
        let result = ManifestFilter::builder()
            .schema(table_schema())
            .partition_specs(vec![spec.clone(), spec])
            .filter(Predicate::AlwaysTrue)
            .build();
        assert_eq!(result.unwrap_err().kind(), ErrorKind::DataInvalid);
    }

    #[test]
    fn test_batch_without_manifest_list_columns_is_an_error() {
        let filter = ManifestFilter::builder()
            .schema(table_schema())
            .partition_specs(partition_specs())
            .filter(Predicate::AlwaysTrue)
            .build()
            .unwrap();
        let batch = RecordBatch::new_empty(Arc::new(arrow_schema::Schema::empty()));
        assert_data_invalid(
            filter.manifest_list_mask(&batch),
            "manifest list batch has no partition_spec_id column",
        );
    }
}
