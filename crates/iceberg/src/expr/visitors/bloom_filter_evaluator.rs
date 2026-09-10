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

//! Evaluates predicates against Parquet bloom filters to determine whether
//! a row group can be skipped.

use std::collections::{HashMap, HashSet};

use fnv::FnvHashSet;
use parquet::basic::Type as PhysicalType;
use parquet::bloom_filter::Sbbf;
use parquet::data_type::ByteArray;

use crate::Result;
use crate::expr::visitors::bound_predicate_visitor::{BoundPredicateVisitor, visit};
use crate::expr::{BoundPredicate, BoundReference};
use crate::spec::decimal_utils::decimal_to_fixed_length_bytes_exact;
use crate::spec::{Datum, PrimitiveLiteral};

const ROW_GROUP_MIGHT_MATCH: Result<bool> = Ok(true);
const ROW_GROUP_CANT_MATCH: Result<bool> = Ok(false);

/// A column's bloom filter for one row group, together with the file's physical
/// encoding of that column. A probe must be encoded the way the writer encoded
/// the values it inserted, so the encoding travels with the filter.
pub(crate) struct ColumnBloomFilter {
    sbbf: Sbbf,
    physical_type: PhysicalType,
    /// `type_length` from the file's column descriptor. Only meaningful for
    /// `FIXED_LEN_BYTE_ARRAY`.
    type_length: i32,
}

impl ColumnBloomFilter {
    pub(crate) fn new(sbbf: Sbbf, physical_type: PhysicalType, type_length: i32) -> Self {
        Self {
            sbbf,
            physical_type,
            type_length,
        }
    }
}

pub(crate) struct BloomFilterEvaluator<'a> {
    /// Maps Iceberg field_id -> bloom filter for this row group
    bloom_filters: &'a HashMap<i32, ColumnBloomFilter>,
}

impl BloomFilterEvaluator<'_> {
    /// Evaluate the predicate against the provided bloom filters.
    /// Returns `false` if the row group definitely does not match,
    /// `true` if it might match.
    pub(crate) fn eval(
        filter: &BoundPredicate,
        bloom_filters: &HashMap<i32, ColumnBloomFilter>,
    ) -> Result<bool> {
        if bloom_filters.is_empty() {
            return ROW_GROUP_MIGHT_MATCH;
        }

        let mut evaluator = BloomFilterEvaluator { bloom_filters };
        visit(&mut evaluator, filter)
    }

    fn check_datum(&self, reference: &BoundReference, datum: &Datum) -> bool {
        let field_id = reference.field().id;
        let Some(column) = self.bloom_filters.get(&field_id) else {
            // No bloom filter for this column — conservatively might match
            return true;
        };

        check_in_bloom_filter(column, datum)
    }
}

/// Collects field IDs that appear in `eq` or `in` predicates — the only
/// predicate types that benefit from bloom filter checks.
///
/// Each node returns the field IDs its subtree contributes, mirroring
/// [`BloomFilterEvaluator`]'s structure so that a field is collected only where
/// the evaluator can act on it. In particular `not` discards its subtree: the
/// evaluator's `not` returns might-match regardless, so a filter fetched for a
/// field under a `NOT` could never prune and would be pure wasted I/O.
pub(crate) fn collect_bloom_filter_field_ids(predicate: &BoundPredicate) -> Result<HashSet<i32>> {
    visit(&mut BloomFilterFieldIdCollector, predicate)
}

struct BloomFilterFieldIdCollector;

impl BoundPredicateVisitor for BloomFilterFieldIdCollector {
    type T = HashSet<i32>;

    fn always_true(&mut self) -> Result<Self::T> {
        Ok(HashSet::new())
    }

    fn always_false(&mut self) -> Result<Self::T> {
        Ok(HashSet::new())
    }

    fn and(&mut self, mut lhs: Self::T, rhs: Self::T) -> Result<Self::T> {
        lhs.extend(rhs);
        Ok(lhs)
    }

    fn or(&mut self, mut lhs: Self::T, rhs: Self::T) -> Result<Self::T> {
        lhs.extend(rhs);
        Ok(lhs)
    }

    fn not(&mut self, _inner: Self::T) -> Result<Self::T> {
        Ok(HashSet::new())
    }

    fn is_null(&mut self, _r: &BoundReference, _p: &BoundPredicate) -> Result<Self::T> {
        Ok(HashSet::new())
    }

    fn not_null(&mut self, _r: &BoundReference, _p: &BoundPredicate) -> Result<Self::T> {
        Ok(HashSet::new())
    }

    fn is_nan(&mut self, _r: &BoundReference, _p: &BoundPredicate) -> Result<Self::T> {
        Ok(HashSet::new())
    }

    fn not_nan(&mut self, _r: &BoundReference, _p: &BoundPredicate) -> Result<Self::T> {
        Ok(HashSet::new())
    }

    fn less_than(
        &mut self,
        _r: &BoundReference,
        _l: &Datum,
        _p: &BoundPredicate,
    ) -> Result<Self::T> {
        Ok(HashSet::new())
    }

    fn less_than_or_eq(
        &mut self,
        _r: &BoundReference,
        _l: &Datum,
        _p: &BoundPredicate,
    ) -> Result<Self::T> {
        Ok(HashSet::new())
    }

    fn greater_than(
        &mut self,
        _r: &BoundReference,
        _l: &Datum,
        _p: &BoundPredicate,
    ) -> Result<Self::T> {
        Ok(HashSet::new())
    }

    fn greater_than_or_eq(
        &mut self,
        _r: &BoundReference,
        _l: &Datum,
        _p: &BoundPredicate,
    ) -> Result<Self::T> {
        Ok(HashSet::new())
    }

    fn eq(&mut self, r: &BoundReference, _l: &Datum, _p: &BoundPredicate) -> Result<Self::T> {
        Ok(HashSet::from([r.field().id]))
    }

    fn not_eq(&mut self, _r: &BoundReference, _l: &Datum, _p: &BoundPredicate) -> Result<Self::T> {
        Ok(HashSet::new())
    }

    fn starts_with(
        &mut self,
        _r: &BoundReference,
        _l: &Datum,
        _p: &BoundPredicate,
    ) -> Result<Self::T> {
        Ok(HashSet::new())
    }

    fn not_starts_with(
        &mut self,
        _r: &BoundReference,
        _l: &Datum,
        _p: &BoundPredicate,
    ) -> Result<Self::T> {
        Ok(HashSet::new())
    }

    fn r#in(
        &mut self,
        r: &BoundReference,
        _literals: &FnvHashSet<Datum>,
        _p: &BoundPredicate,
    ) -> Result<Self::T> {
        Ok(HashSet::from([r.field().id]))
    }

    fn not_in(
        &mut self,
        _r: &BoundReference,
        _literals: &FnvHashSet<Datum>,
        _p: &BoundPredicate,
    ) -> Result<Self::T> {
        Ok(HashSet::new())
    }
}

/// Check whether a datum value might be present in the bloom filter.
///
/// The value must be checked using the same physical encoding the Parquet
/// writer used when inserting into the bloom filter. We use the actual
/// physical type from the column metadata to ensure correctness regardless
/// of which writer produced the file.
fn check_in_bloom_filter(column: &ColumnBloomFilter, datum: &Datum) -> bool {
    let ColumnBloomFilter {
        sbbf,
        physical_type,
        type_length,
    } = column;
    let physical_type = *physical_type;

    match datum.literal() {
        PrimitiveLiteral::Boolean(v) => sbbf.check(v),
        // A promoted column (int -> long, float -> double) keeps its original
        // physical width in files written before the promotion, and the writer
        // hashed that width, so probe at the file's width rather than the
        // predicate's.
        PrimitiveLiteral::Int(v) => match physical_type {
            PhysicalType::INT32 => sbbf.check(v),
            PhysicalType::INT64 => sbbf.check(&i64::from(*v)),
            _ => true,
        },
        PrimitiveLiteral::Long(v) => match physical_type {
            PhysicalType::INT64 => sbbf.check(v),
            PhysicalType::INT32 => match i32::try_from(*v) {
                Ok(narrowed) => sbbf.check(&narrowed),
                // Too wide for an INT32 column to hold, so there is nothing
                // meaningful to probe — keep the row group.
                Err(_) => true,
            },
            _ => true,
        },
        PrimitiveLiteral::Float(v) => match physical_type {
            PhysicalType::FLOAT => sbbf.check(&v.0),
            PhysicalType::DOUBLE => sbbf.check(&f64::from(v.0)),
            _ => true,
        },
        PrimitiveLiteral::Double(v) => match physical_type {
            PhysicalType::DOUBLE => sbbf.check(&v.0),
            PhysicalType::FLOAT => {
                let narrowed = v.0 as f32;
                // Only an exactly representable value can equal a widened f32.
                if f64::from(narrowed) == v.0 {
                    sbbf.check(&narrowed)
                } else {
                    true
                }
            }
            _ => true,
        },
        PrimitiveLiteral::String(v) => sbbf.check(v.as_str()),
        PrimitiveLiteral::Binary(v) => sbbf.check(v.as_slice()),
        PrimitiveLiteral::Int128(v) => {
            // Decimal: dispatch based on the actual Parquet physical type
            // from the file, not inferred from precision.
            match physical_type {
                // Narrow only when the mantissa round-trips; a truncated copy would
                // hash to an unrelated slot.
                PhysicalType::INT32 => match i32::try_from(*v) {
                    Ok(narrowed) => sbbf.check(&narrowed),
                    Err(_) => true,
                },
                PhysicalType::INT64 => match i64::try_from(*v) {
                    Ok(narrowed) => sbbf.check(&narrowed),
                    Err(_) => true,
                },
                PhysicalType::FIXED_LEN_BYTE_ARRAY => {
                    // Encode to the file's declared length, not one derived from
                    // the Iceberg precision: a widened precision would change the
                    // length and miss every entry the writer inserted.
                    match usize::try_from(*type_length)
                        .ok()
                        .and_then(|len| decimal_to_fixed_length_bytes_exact(*v, len))
                    {
                        Some(bytes) => sbbf.check(&ByteArray::from(bytes)),
                        // Unusable length, or a value too large for the column to
                        // hold — conservatively might match.
                        None => true,
                    }
                }
                // Known gap: BYTE_ARRAY decimals are valid in Parquet (though not in
                // Iceberg's Appendix A) and some older Spark writers emit them. Not
                // probed because BYTE_ARRAY carries no `type_length` and sign-extension
                // padding is writer-dependent, so a single-length probe would miss
                // padded entries and prune row groups that do hold the value. A sound
                // version ORs a check over every length from minimal..=16.
                _ => true, // Conservatively might match
            }
        }
        PrimitiveLiteral::UInt128(v) => {
            // UUID: stored as FIXED_LEN_BYTE_ARRAY(16), big-endian
            let bytes = v.to_be_bytes();
            sbbf.check(&ByteArray::from(bytes.to_vec()))
        }
        PrimitiveLiteral::AboveMax | PrimitiveLiteral::BelowMin => true,
    }
}

impl BoundPredicateVisitor for BloomFilterEvaluator<'_> {
    type T = bool;

    fn always_true(&mut self) -> Result<Self::T> {
        ROW_GROUP_MIGHT_MATCH
    }

    fn always_false(&mut self) -> Result<Self::T> {
        ROW_GROUP_CANT_MATCH
    }

    fn and(&mut self, lhs: Self::T, rhs: Self::T) -> Result<Self::T> {
        Ok(lhs && rhs)
    }

    fn or(&mut self, lhs: Self::T, rhs: Self::T) -> Result<Self::T> {
        Ok(lhs || rhs)
    }

    fn not(&mut self, _inner: Self::T) -> Result<Self::T> {
        // Bloom filters are not invertible — we cannot prove presence,
        // so NOT of any result must conservatively return "might match".
        ROW_GROUP_MIGHT_MATCH
    }

    fn is_null(
        &mut self,
        _reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<Self::T> {
        ROW_GROUP_MIGHT_MATCH
    }

    fn not_null(
        &mut self,
        _reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<Self::T> {
        ROW_GROUP_MIGHT_MATCH
    }

    fn is_nan(
        &mut self,
        _reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<Self::T> {
        ROW_GROUP_MIGHT_MATCH
    }

    fn not_nan(
        &mut self,
        _reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<Self::T> {
        ROW_GROUP_MIGHT_MATCH
    }

    fn less_than(
        &mut self,
        _reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Self::T> {
        ROW_GROUP_MIGHT_MATCH
    }

    fn less_than_or_eq(
        &mut self,
        _reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Self::T> {
        ROW_GROUP_MIGHT_MATCH
    }

    fn greater_than(
        &mut self,
        _reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Self::T> {
        ROW_GROUP_MIGHT_MATCH
    }

    fn greater_than_or_eq(
        &mut self,
        _reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Self::T> {
        ROW_GROUP_MIGHT_MATCH
    }

    fn eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Self::T> {
        if self.check_datum(reference, literal) {
            ROW_GROUP_MIGHT_MATCH
        } else {
            ROW_GROUP_CANT_MATCH
        }
    }

    fn not_eq(
        &mut self,
        _reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Self::T> {
        ROW_GROUP_MIGHT_MATCH
    }

    fn starts_with(
        &mut self,
        _reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Self::T> {
        ROW_GROUP_MIGHT_MATCH
    }

    fn not_starts_with(
        &mut self,
        _reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Self::T> {
        ROW_GROUP_MIGHT_MATCH
    }

    fn r#in(
        &mut self,
        reference: &BoundReference,
        literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> Result<Self::T> {
        let field_id = reference.field().id;
        let Some(column) = self.bloom_filters.get(&field_id) else {
            return ROW_GROUP_MIGHT_MATCH;
        };

        // If ANY literal might be present, the row group might match
        for literal in literals {
            if check_in_bloom_filter(column, literal) {
                return ROW_GROUP_MIGHT_MATCH;
            }
        }

        // All literals are definitely absent
        ROW_GROUP_CANT_MATCH
    }

    fn not_in(
        &mut self,
        _reference: &BoundReference,
        _literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> Result<Self::T> {
        ROW_GROUP_MIGHT_MATCH
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ops::Not;

    use parquet::basic::Type as PhysicalType;
    use parquet::bloom_filter::Sbbf;
    use parquet::data_type::ByteArray;

    use super::{BloomFilterEvaluator, ColumnBloomFilter, collect_bloom_filter_field_ids};
    use crate::expr::{Bind, BoundPredicate, Reference};
    use crate::spec::decimal_utils::decimal_to_fixed_length_bytes_exact;
    use crate::spec::{Datum, NestedField, PrimitiveType, Schema, Type};

    fn create_test_schema() -> Schema {
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap()
    }

    fn create_bloom_filter_with_values_i32(values: &[i32]) -> Sbbf {
        let mut sbbf = Sbbf::new_with_ndv_fpp(values.len() as u64, 0.01).unwrap();
        for v in values {
            sbbf.insert(v);
        }
        sbbf
    }

    fn create_bloom_filter_with_values_str(values: &[&str]) -> Sbbf {
        let mut sbbf = Sbbf::new_with_ndv_fpp(values.len() as u64, 0.01).unwrap();
        for v in values {
            sbbf.insert(*v);
        }
        sbbf
    }

    #[test]
    fn test_eq_value_present() {
        let schema = create_test_schema();
        let bloom_filters = HashMap::from([(
            1,
            ColumnBloomFilter::new(
                create_bloom_filter_with_values_i32(&[1, 2, 3]),
                PhysicalType::INT32,
                0,
            ),
        )]);

        let predicate = Reference::new("id")
            .equal_to(Datum::int(2))
            .bind(schema.into(), true)
            .unwrap();

        let result = BloomFilterEvaluator::eval(&predicate, &bloom_filters).unwrap();
        assert!(result, "Row group should might-match when value is present");
    }

    #[test]
    fn test_eq_value_absent() {
        let schema = create_test_schema();
        let bloom_filters = HashMap::from([(
            1,
            ColumnBloomFilter::new(
                create_bloom_filter_with_values_i32(&[1, 2, 3]),
                PhysicalType::INT32,
                0,
            ),
        )]);

        let predicate = Reference::new("id")
            .equal_to(Datum::int(999))
            .bind(schema.into(), true)
            .unwrap();

        let result = BloomFilterEvaluator::eval(&predicate, &bloom_filters).unwrap();
        assert!(
            !result,
            "Row group should not match when value is absent from bloom filter"
        );
    }

    #[test]
    fn test_eq_no_bloom_filter_for_column() {
        let schema = create_test_schema();
        let bloom_filters = HashMap::new(); // No bloom filters

        let predicate = Reference::new("id")
            .equal_to(Datum::int(1))
            .bind(schema.into(), true)
            .unwrap();

        let result = BloomFilterEvaluator::eval(&predicate, &bloom_filters).unwrap();
        assert!(
            result,
            "Row group should might-match when no bloom filter available"
        );
    }

    #[test]
    fn test_in_all_absent() {
        let schema = create_test_schema();
        let bloom_filters = HashMap::from([(
            1,
            ColumnBloomFilter::new(
                create_bloom_filter_with_values_i32(&[1, 2, 3]),
                PhysicalType::INT32,
                0,
            ),
        )]);

        let predicate = Reference::new("id")
            .is_in([Datum::int(100), Datum::int(200), Datum::int(300)])
            .bind(schema.into(), true)
            .unwrap();

        let result = BloomFilterEvaluator::eval(&predicate, &bloom_filters).unwrap();
        assert!(
            !result,
            "Row group should not match when all IN values are absent"
        );
    }

    #[test]
    fn test_in_some_present() {
        let schema = create_test_schema();
        let bloom_filters = HashMap::from([(
            1,
            ColumnBloomFilter::new(
                create_bloom_filter_with_values_i32(&[1, 2, 3]),
                PhysicalType::INT32,
                0,
            ),
        )]);

        let predicate = Reference::new("id")
            .is_in([Datum::int(2), Datum::int(200)])
            .bind(schema.into(), true)
            .unwrap();

        let result = BloomFilterEvaluator::eval(&predicate, &bloom_filters).unwrap();
        assert!(
            result,
            "Row group should might-match when at least one IN value is present"
        );
    }

    #[test]
    fn test_and_one_absent() {
        let schema = create_test_schema();
        let bloom_filters = HashMap::from([
            (
                1,
                ColumnBloomFilter::new(
                    create_bloom_filter_with_values_i32(&[1, 2, 3]),
                    PhysicalType::INT32,
                    0,
                ),
            ),
            (
                2,
                ColumnBloomFilter::new(
                    create_bloom_filter_with_values_str(&["alice", "bob"]),
                    PhysicalType::BYTE_ARRAY,
                    0,
                ),
            ),
        ]);

        // id = 999 AND name = 'alice'
        // id=999 is absent, so AND should be false
        let predicate = Reference::new("id")
            .equal_to(Datum::int(999))
            .and(Reference::new("name").equal_to(Datum::string("alice")))
            .bind(schema.into(), true)
            .unwrap();

        let result = BloomFilterEvaluator::eval(&predicate, &bloom_filters).unwrap();
        assert!(
            !result,
            "AND should be false when one operand is definitely absent"
        );
    }

    #[test]
    fn test_or_one_present() {
        let schema = create_test_schema();
        let bloom_filters = HashMap::from([(
            1,
            ColumnBloomFilter::new(
                create_bloom_filter_with_values_i32(&[1, 2, 3]),
                PhysicalType::INT32,
                0,
            ),
        )]);

        // id = 999 OR id = 2
        // id=2 is present, so OR should be true
        let predicate = Reference::new("id")
            .equal_to(Datum::int(999))
            .or(Reference::new("id").equal_to(Datum::int(2)))
            .bind(schema.into(), true)
            .unwrap();

        let result = BloomFilterEvaluator::eval(&predicate, &bloom_filters).unwrap();
        assert!(result, "OR should be true when one operand might match");
    }

    #[test]
    fn test_not_always_might_match() {
        let schema = create_test_schema();
        let bloom_filters = HashMap::from([(
            1,
            ColumnBloomFilter::new(
                create_bloom_filter_with_values_i32(&[1, 2, 3]),
                PhysicalType::INT32,
                0,
            ),
        )]);

        // NOT(id = 999) — even though 999 is absent, NOT should still return true
        let predicate = Reference::new("id")
            .equal_to(Datum::int(999))
            .not()
            .bind(schema.into(), true)
            .unwrap();

        let result = BloomFilterEvaluator::eval(&predicate, &bloom_filters).unwrap();
        assert!(result, "NOT should always return might-match");
    }

    // --- Field ID collection ---
    //
    // The collector decides which bloom filters get fetched, one round trip per
    // column per row group, so anything it reports that the evaluator cannot act
    // on is wasted I/O.

    fn collected_ids(predicate: BoundPredicate) -> Vec<i32> {
        let mut ids: Vec<i32> = collect_bloom_filter_field_ids(&predicate)
            .unwrap()
            .into_iter()
            .collect();
        ids.sort_unstable();
        ids
    }

    #[test]
    fn test_collects_eq_and_in_field_ids() {
        let schema = create_test_schema();
        let predicate = Reference::new("id")
            .equal_to(Datum::int(1))
            .and(Reference::new("name").is_in([Datum::string("alice")]))
            .bind(schema.into(), true)
            .unwrap();

        assert_eq!(collected_ids(predicate), vec![1, 2]);
    }

    /// `not` discards its subtree, so `eq`/`in` beneath a `NOT` are not collected.
    /// The evaluator's `not` returns might-match regardless, so fetching those
    /// filters could never prune.
    #[test]
    fn test_does_not_collect_eq_under_not() {
        let schema = create_test_schema();
        let predicate = Reference::new("id")
            .equal_to(Datum::int(1))
            .not()
            .bind(schema.into(), true)
            .unwrap();

        assert!(collected_ids(predicate).is_empty());
    }

    #[test]
    fn test_does_not_collect_in_under_not() {
        let schema = create_test_schema();
        let predicate = Reference::new("id")
            .is_in([Datum::int(1), Datum::int(2)])
            .not()
            .bind(schema.into(), true)
            .unwrap();

        assert!(collected_ids(predicate).is_empty());
    }

    /// A `NOT` must not suppress collection for its siblings.
    #[test]
    fn test_collects_sibling_of_not() {
        let schema = create_test_schema();
        let predicate = Reference::new("id")
            .equal_to(Datum::int(1))
            .not()
            .and(Reference::new("name").equal_to(Datum::string("alice")))
            .bind(schema.into(), true)
            .unwrap();

        assert_eq!(collected_ids(predicate), vec![2]);
    }

    /// Range and `not_eq` predicates cannot be probed, so they contribute nothing.
    #[test]
    fn test_does_not_collect_unprobeable_operators() {
        let schema = create_test_schema();
        let predicate = Reference::new("id")
            .less_than(Datum::int(1))
            .and(Reference::new("id").not_equal_to(Datum::int(2)))
            .and(Reference::new("name").is_not_null())
            .bind(schema.into(), true)
            .unwrap();

        assert!(collected_ids(predicate).is_empty());
    }

    #[test]
    fn test_range_predicates_always_might_match() {
        let schema = create_test_schema();
        let bloom_filters = HashMap::from([(
            1,
            ColumnBloomFilter::new(
                create_bloom_filter_with_values_i32(&[1, 2, 3]),
                PhysicalType::INT32,
                0,
            ),
        )]);

        let predicate = Reference::new("id")
            .less_than(Datum::int(0))
            .bind(schema.into(), true)
            .unwrap();

        let result = BloomFilterEvaluator::eval(&predicate, &bloom_filters).unwrap();
        assert!(result, "Range predicates should always return might-match");
    }

    #[test]
    fn test_string_eq_present() {
        let schema = create_test_schema();
        let bloom_filters = HashMap::from([(
            2,
            ColumnBloomFilter::new(
                create_bloom_filter_with_values_str(&["alice", "bob"]),
                PhysicalType::BYTE_ARRAY,
                0,
            ),
        )]);

        let predicate = Reference::new("name")
            .equal_to(Datum::string("alice"))
            .bind(schema.into(), true)
            .unwrap();

        let result = BloomFilterEvaluator::eval(&predicate, &bloom_filters).unwrap();
        assert!(result, "Should might-match when string is in bloom filter");
    }

    #[test]
    fn test_string_eq_absent() {
        let schema = create_test_schema();
        let bloom_filters = HashMap::from([(
            2,
            ColumnBloomFilter::new(
                create_bloom_filter_with_values_str(&["alice", "bob"]),
                PhysicalType::BYTE_ARRAY,
                0,
            ),
        )]);

        let predicate = Reference::new("name")
            .equal_to(Datum::string("charlie"))
            .bind(schema.into(), true)
            .unwrap();

        let result = BloomFilterEvaluator::eval(&predicate, &bloom_filters).unwrap();
        assert!(
            !result,
            "Should not match when string is absent from bloom filter"
        );
    }

    // --- Decimal tests ---

    fn create_decimal_schema(precision: u32, scale: u32) -> Schema {
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(
                    1,
                    "amount",
                    Type::Primitive(PrimitiveType::Decimal { precision, scale }),
                )
                .into(),
            ])
            .build()
            .unwrap()
    }

    /// Decimal with precision <= 9 is stored as INT32 in Parquet.
    /// The bloom filter contains i32 values (the unscaled mantissa).
    #[test]
    fn test_decimal_int32_present() {
        let schema = create_decimal_schema(9, 2);

        // Parquet stores decimal(9,2) as INT32 with unscaled value
        // Value "123.45" has mantissa 12345
        let mut sbbf = Sbbf::new_with_ndv_fpp(10, 0.01).unwrap();
        sbbf.insert(&12345_i32);
        sbbf.insert(&67890_i32);

        let bloom_filters =
            HashMap::from([(1, ColumnBloomFilter::new(sbbf, PhysicalType::INT32, 0))]);

        let predicate = Reference::new("amount")
            .equal_to(
                Datum::decimal_with_precision(
                    crate::spec::decimal_utils::decimal_from_i128_with_scale(12345, 2),
                    9,
                )
                .unwrap(),
            )
            .bind(schema.into(), true)
            .unwrap();

        let result = BloomFilterEvaluator::eval(&predicate, &bloom_filters).unwrap();
        assert!(result, "Decimal INT32 value present should might-match");
    }

    #[test]
    fn test_decimal_int32_absent() {
        let schema = create_decimal_schema(9, 2);

        let mut sbbf = Sbbf::new_with_ndv_fpp(10, 0.01).unwrap();
        sbbf.insert(&12345_i32);
        sbbf.insert(&67890_i32);

        let bloom_filters =
            HashMap::from([(1, ColumnBloomFilter::new(sbbf, PhysicalType::INT32, 0))]);

        // Value "999.99" has mantissa 99999, not in the bloom filter
        let predicate = Reference::new("amount")
            .equal_to(
                Datum::decimal_with_precision(
                    crate::spec::decimal_utils::decimal_from_i128_with_scale(99999, 2),
                    9,
                )
                .unwrap(),
            )
            .bind(schema.into(), true)
            .unwrap();

        let result = BloomFilterEvaluator::eval(&predicate, &bloom_filters).unwrap();
        assert!(!result, "Decimal INT32 value absent should not match");
    }

    /// Decimal with precision 10-18 is stored as INT64 in Parquet.
    #[test]
    fn test_decimal_int64_present() {
        let schema = create_decimal_schema(15, 2);

        // "1234567890123.45" has mantissa 123456789012345
        let mantissa: i64 = 123456789012345;
        let mut sbbf = Sbbf::new_with_ndv_fpp(10, 0.01).unwrap();
        sbbf.insert(&mantissa);

        let bloom_filters =
            HashMap::from([(1, ColumnBloomFilter::new(sbbf, PhysicalType::INT64, 0))]);

        let predicate = Reference::new("amount")
            .equal_to(
                Datum::decimal_with_precision(
                    crate::spec::decimal_utils::decimal_from_i128_with_scale(mantissa as i128, 2),
                    15,
                )
                .unwrap(),
            )
            .bind(schema.into(), true)
            .unwrap();

        let result = BloomFilterEvaluator::eval(&predicate, &bloom_filters).unwrap();
        assert!(result, "Decimal INT64 value present should might-match");
    }

    #[test]
    fn test_decimal_int64_absent() {
        let schema = create_decimal_schema(15, 2);

        let mut sbbf = Sbbf::new_with_ndv_fpp(10, 0.01).unwrap();
        sbbf.insert(&123456789012345_i64);

        let bloom_filters =
            HashMap::from([(1, ColumnBloomFilter::new(sbbf, PhysicalType::INT64, 0))]);

        let predicate = Reference::new("amount")
            .equal_to(
                Datum::decimal_with_precision(
                    crate::spec::decimal_utils::decimal_from_i128_with_scale(999999999999999, 2),
                    15,
                )
                .unwrap(),
            )
            .bind(schema.into(), true)
            .unwrap();

        let result = BloomFilterEvaluator::eval(&predicate, &bloom_filters).unwrap();
        assert!(!result, "Decimal INT64 value absent should not match");
    }

    /// Decimal with precision 19+ is stored as FIXED_LEN_BYTE_ARRAY in Parquet.
    #[test]
    fn test_decimal_fixed_bytes_present() {
        let schema = create_decimal_schema(25, 2);

        // Large mantissa that requires FIXED_LEN_BYTE_ARRAY
        let mantissa: i128 = 12345678901234567890;
        let bytes = decimal_to_fixed_length_bytes_exact(mantissa, 11).unwrap();
        let type_length = bytes.len() as i32;

        let mut sbbf = Sbbf::new_with_ndv_fpp(10, 0.01).unwrap();
        sbbf.insert(&ByteArray::from(bytes));

        let bloom_filters = HashMap::from([(
            1,
            ColumnBloomFilter::new(sbbf, PhysicalType::FIXED_LEN_BYTE_ARRAY, type_length),
        )]);

        let predicate = Reference::new("amount")
            .equal_to(
                Datum::decimal_with_precision(
                    crate::spec::decimal_utils::decimal_from_i128_with_scale(mantissa, 2),
                    25,
                )
                .unwrap(),
            )
            .bind(schema.into(), true)
            .unwrap();

        let result = BloomFilterEvaluator::eval(&predicate, &bloom_filters).unwrap();
        assert!(
            result,
            "Decimal FIXED_LEN_BYTE_ARRAY value present should might-match"
        );
    }

    #[test]
    fn test_decimal_fixed_bytes_absent() {
        let schema = create_decimal_schema(25, 2);

        let mantissa: i128 = 12345678901234567890;
        let bytes = decimal_to_fixed_length_bytes_exact(mantissa, 11).unwrap();
        let type_length = bytes.len() as i32;

        let mut sbbf = Sbbf::new_with_ndv_fpp(10, 0.01).unwrap();
        sbbf.insert(&ByteArray::from(bytes));

        let bloom_filters = HashMap::from([(
            1,
            ColumnBloomFilter::new(sbbf, PhysicalType::FIXED_LEN_BYTE_ARRAY, type_length),
        )]);

        // Different value not in the bloom filter
        let predicate = Reference::new("amount")
            .equal_to(
                Datum::decimal_with_precision(
                    crate::spec::decimal_utils::decimal_from_i128_with_scale(
                        99999999999999999999,
                        2,
                    ),
                    25,
                )
                .unwrap(),
            )
            .bind(schema.into(), true)
            .unwrap();

        let result = BloomFilterEvaluator::eval(&predicate, &bloom_filters).unwrap();
        assert!(
            !result,
            "Decimal FIXED_LEN_BYTE_ARRAY value absent should not match"
        );
    }

    /// Negative decimal values should also work correctly.
    #[test]
    fn test_decimal_negative_int32() {
        let schema = create_decimal_schema(9, 2);

        // "-123.45" has mantissa -12345
        let mut sbbf = Sbbf::new_with_ndv_fpp(10, 0.01).unwrap();
        sbbf.insert(&(-12345_i32));

        let bloom_filters =
            HashMap::from([(1, ColumnBloomFilter::new(sbbf, PhysicalType::INT32, 0))]);

        let predicate = Reference::new("amount")
            .equal_to(
                Datum::decimal_with_precision(
                    crate::spec::decimal_utils::decimal_from_i128_with_scale(-12345, 2),
                    9,
                )
                .unwrap(),
            )
            .bind(schema.into(), true)
            .unwrap();

        let result = BloomFilterEvaluator::eval(&predicate, &bloom_filters).unwrap();
        assert!(result, "Negative decimal INT32 present should might-match");
    }

    #[test]
    fn test_decimal_negative_fixed_bytes() {
        let schema = create_decimal_schema(25, 2);

        let mantissa: i128 = -12345678901234567890;
        let bytes = decimal_to_fixed_length_bytes_exact(mantissa, 11).unwrap();
        let type_length = bytes.len() as i32;

        let mut sbbf = Sbbf::new_with_ndv_fpp(10, 0.01).unwrap();
        sbbf.insert(&ByteArray::from(bytes));

        let bloom_filters = HashMap::from([(
            1,
            ColumnBloomFilter::new(sbbf, PhysicalType::FIXED_LEN_BYTE_ARRAY, type_length),
        )]);

        let predicate = Reference::new("amount")
            .equal_to(
                Datum::decimal_with_precision(
                    crate::spec::decimal_utils::decimal_from_i128_with_scale(mantissa, 2),
                    25,
                )
                .unwrap(),
            )
            .bind(schema.into(), true)
            .unwrap();

        let result = BloomFilterEvaluator::eval(&predicate, &bloom_filters).unwrap();
        assert!(
            result,
            "Negative decimal FIXED_LEN_BYTE_ARRAY present should might-match"
        );
    }

    /// A Parquet writer other than arrow-rs (e.g. Spark, Java Parquet) might
    /// choose FIXED_LEN_BYTE_ARRAY for a low-precision decimal rather than INT32.
    /// The evaluator must use the physical type from the file metadata, not assume
    /// a mapping based on precision.
    #[test]
    fn test_decimal_low_precision_stored_as_fixed_len_byte_array() {
        let schema = create_decimal_schema(5, 2);

        // Simulate a file where decimal(5,2) was stored as FIXED_LEN_BYTE_ARRAY
        let mantissa: i128 = 12345;
        let bytes = decimal_to_fixed_length_bytes_exact(mantissa, 3).unwrap();
        let type_length = bytes.len() as i32;

        let mut sbbf = Sbbf::new_with_ndv_fpp(10, 0.01).unwrap();
        sbbf.insert(&ByteArray::from(bytes));

        let bloom_filters = HashMap::from([(
            1,
            ColumnBloomFilter::new(sbbf, PhysicalType::FIXED_LEN_BYTE_ARRAY, type_length),
        )]);

        let predicate = Reference::new("amount")
            .equal_to(
                Datum::decimal_with_precision(
                    crate::spec::decimal_utils::decimal_from_i128_with_scale(mantissa, 2),
                    5,
                )
                .unwrap(),
            )
            .bind(schema.into(), true)
            .unwrap();

        let result = BloomFilterEvaluator::eval(&predicate, &bloom_filters).unwrap();
        assert!(
            result,
            "Should match when physical type is FIXED_LEN_BYTE_ARRAY even for low-precision decimal"
        );
    }

    /// Builds the (schema, bloom filter) pair for a `decimal` column that the file
    /// stored at `file_type_length` bytes while the table schema now declares
    /// `schema_precision`, as a precision-widening evolution produces.
    fn widened_decimal_case(
        mantissa: i128,
        file_type_length: usize,
        schema_precision: u32,
    ) -> (Schema, HashMap<i32, ColumnBloomFilter>) {
        let bytes = decimal_to_fixed_length_bytes_exact(mantissa, file_type_length).unwrap();
        let mut sbbf = Sbbf::new_with_ndv_fpp(10, 0.01).unwrap();
        sbbf.insert(&ByteArray::from(bytes));

        let filters = HashMap::from([(
            1,
            ColumnBloomFilter::new(
                sbbf,
                PhysicalType::FIXED_LEN_BYTE_ARRAY,
                file_type_length as i32,
            ),
        )]);

        (create_decimal_schema(schema_precision, 2), filters)
    }

    fn decimal_eq_predicate(schema: Schema, mantissa: i128, precision: u32) -> BoundPredicate {
        Reference::new("amount")
            .equal_to(
                Datum::decimal_with_precision(
                    crate::spec::decimal_utils::decimal_from_i128_with_scale(mantissa, 2),
                    precision,
                )
                .unwrap(),
            )
            .bind(schema.into(), true)
            .unwrap()
    }

    /// Widening a decimal's precision does not rewrite existing files, so the
    /// column keeps its original `type_length`. Deriving the probe length from the
    /// widened precision changed the encoding and missed every entry the writer
    /// inserted, silently pruning row groups holding matching rows.
    #[test]
    fn test_decimal_widened_precision_present_is_not_pruned() {
        let mantissa: i128 = 12345678901234567890;
        // File written as decimal(20,2) -> 9 bytes; schema since widened to 38.
        let (schema, filters) = widened_decimal_case(mantissa, 9, 38);
        let predicate = decimal_eq_predicate(schema, mantissa, 38);

        let result = BloomFilterEvaluator::eval(&predicate, &filters).unwrap();
        assert!(
            result,
            "widened decimal precision must still find the value at the file's length"
        );
    }

    #[test]
    fn test_decimal_widened_precision_absent_still_prunes() {
        let mantissa: i128 = 12345678901234567890;
        let (schema, filters) = widened_decimal_case(mantissa, 9, 38);
        let predicate = decimal_eq_predicate(schema, 999999999999999999, 38);

        let result = BloomFilterEvaluator::eval(&predicate, &filters).unwrap();
        assert!(!result, "genuinely absent value must still prune");
    }

    /// A value too wide for the column's `type_length` cannot be present, but
    /// truncating it would probe an unrelated slot, so stay conservative.
    #[test]
    fn test_decimal_value_wider_than_column_does_not_prune() {
        let (schema, filters) = widened_decimal_case(1234, 2, 38);
        let predicate = decimal_eq_predicate(schema, i64::MAX as i128, 38);

        let result = BloomFilterEvaluator::eval(&predicate, &filters).unwrap();
        assert!(result, "value too wide for the column must not prune");
    }

    /// The `INT32` / `INT64` counterparts of the case above: after a precision
    /// widening the bound literal can carry a mantissa wider than the file's
    /// physical column, and a truncated probe would hash an unrelated slot.
    #[test]
    fn test_decimal_wider_than_int32_column_does_not_prune() {
        let mut sbbf = Sbbf::new_with_ndv_fpp(10, 0.01).unwrap();
        sbbf.insert(&12345_i32);
        let filters = HashMap::from([(1, ColumnBloomFilter::new(sbbf, PhysicalType::INT32, 0))]);

        // File written as decimal(9,2) -> INT32; schema since widened to decimal(38,2).
        let predicate = decimal_eq_predicate(create_decimal_schema(38, 2), i64::MAX as i128, 38);

        let result = BloomFilterEvaluator::eval(&predicate, &filters).unwrap();
        assert!(
            result,
            "mantissa too wide for an INT32 column must not prune"
        );
    }

    #[test]
    fn test_decimal_wider_than_int64_column_does_not_prune() {
        let mut sbbf = Sbbf::new_with_ndv_fpp(10, 0.01).unwrap();
        sbbf.insert(&12345_i64);
        let filters = HashMap::from([(1, ColumnBloomFilter::new(sbbf, PhysicalType::INT64, 0))]);

        let predicate =
            decimal_eq_predicate(create_decimal_schema(38, 2), i128::from(i64::MAX) + 1, 38);

        let result = BloomFilterEvaluator::eval(&predicate, &filters).unwrap();
        assert!(
            result,
            "mantissa too wide for an INT64 column must not prune"
        );
    }

    /// A mantissa that does fit must still prune when genuinely absent, so the
    /// `try_from` guard above does not disable pruning outright.
    #[test]
    fn test_decimal_within_int64_column_still_prunes() {
        let mut sbbf = Sbbf::new_with_ndv_fpp(10, 0.01).unwrap();
        sbbf.insert(&12345_i64);
        let filters = HashMap::from([(1, ColumnBloomFilter::new(sbbf, PhysicalType::INT64, 0))]);

        let predicate = decimal_eq_predicate(create_decimal_schema(38, 2), 999_999, 38);

        let result = BloomFilterEvaluator::eval(&predicate, &filters).unwrap();
        assert!(!result, "absent in-range mantissa must still prune");
    }

    /// A zero or oversized `type_length` is unusable; never prune on it.
    #[test]
    fn test_decimal_unusable_type_length_does_not_prune() {
        let mantissa: i128 = 1234;
        let bytes = decimal_to_fixed_length_bytes_exact(mantissa, 4).unwrap();
        let mut sbbf = Sbbf::new_with_ndv_fpp(10, 0.01).unwrap();
        sbbf.insert(&ByteArray::from(bytes));

        for bogus_length in [0, -1, 17] {
            let filters = HashMap::from([(
                1,
                ColumnBloomFilter::new(
                    Sbbf::new_with_ndv_fpp(10, 0.01).unwrap(),
                    PhysicalType::FIXED_LEN_BYTE_ARRAY,
                    bogus_length,
                ),
            )]);
            let predicate = decimal_eq_predicate(create_decimal_schema(38, 2), mantissa, 38);

            let result = BloomFilterEvaluator::eval(&predicate, &filters).unwrap();
            assert!(
                result,
                "type_length {bogus_length} is unusable and must not prune"
            );
        }
    }

    fn single_field_schema(name: &str, ty: PrimitiveType) -> Schema {
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, name, Type::Primitive(ty)).into(),
            ])
            .build()
            .unwrap()
    }

    /// After an `int` -> `long` promotion the predicate carries a `long`, but files
    /// written before the promotion store the column as `INT32` and hashed it at
    /// that width. Probing at the predicate's width made every lookup miss and
    /// silently pruned row groups holding matching rows.
    #[test]
    fn test_promoted_int_to_long_present_is_not_pruned() {
        let schema = single_field_schema("id", PrimitiveType::Long);
        let bloom_filters = HashMap::from([(
            1,
            ColumnBloomFilter::new(
                create_bloom_filter_with_values_i32(&[100, 150, 199]),
                PhysicalType::INT32,
                0,
            ),
        )]);

        let predicate = Reference::new("id")
            .equal_to(Datum::long(150))
            .bind(schema.into(), true)
            .unwrap();

        let result = BloomFilterEvaluator::eval(&predicate, &bloom_filters).unwrap();
        assert!(
            result,
            "long predicate against an INT32 column must find the promoted value"
        );
    }

    #[test]
    fn test_promoted_int_to_long_absent_still_prunes() {
        let schema = single_field_schema("id", PrimitiveType::Long);
        let bloom_filters = HashMap::from([(
            1,
            ColumnBloomFilter::new(
                create_bloom_filter_with_values_i32(&[100, 150, 199]),
                PhysicalType::INT32,
                0,
            ),
        )]);

        let predicate = Reference::new("id")
            .equal_to(Datum::long(4242))
            .bind(schema.into(), true)
            .unwrap();

        let result = BloomFilterEvaluator::eval(&predicate, &bloom_filters).unwrap();
        assert!(!result, "genuinely absent value must still prune");
    }

    /// A `long` predicate outside `i32` range cannot be present in an `INT32`
    /// column; stay conservative rather than probe a truncated value.
    #[test]
    fn test_long_out_of_int32_range_does_not_prune() {
        let schema = single_field_schema("id", PrimitiveType::Long);
        let bloom_filters = HashMap::from([(
            1,
            ColumnBloomFilter::new(
                create_bloom_filter_with_values_i32(&[1, 2, 3]),
                PhysicalType::INT32,
                0,
            ),
        )]);

        let predicate = Reference::new("id")
            .equal_to(Datum::long(i64::from(i32::MAX) + 1))
            .bind(schema.into(), true)
            .unwrap();

        let result = BloomFilterEvaluator::eval(&predicate, &bloom_filters).unwrap();
        assert!(result, "out-of-range long must not prune");
    }

    fn create_bloom_filter_with_values_f32(values: &[f32]) -> Sbbf {
        let mut sbbf = Sbbf::new_with_ndv_fpp(values.len() as u64, 0.01).unwrap();
        for v in values {
            sbbf.insert(v);
        }
        sbbf
    }

    /// The `float` -> `double` counterpart of the `int` -> `long` promotion above.
    #[test]
    fn test_promoted_float_to_double_present_is_not_pruned() {
        let schema = single_field_schema("val", PrimitiveType::Double);
        let bloom_filters = HashMap::from([(
            1,
            ColumnBloomFilter::new(
                create_bloom_filter_with_values_f32(&[1.5, 2.25, 4.0]),
                PhysicalType::FLOAT,
                0,
            ),
        )]);

        let predicate = Reference::new("val")
            .equal_to(Datum::double(1.5))
            .bind(schema.into(), true)
            .unwrap();

        let result = BloomFilterEvaluator::eval(&predicate, &bloom_filters).unwrap();
        assert!(
            result,
            "double predicate against a FLOAT column must find the promoted value"
        );
    }

    #[test]
    fn test_promoted_float_to_double_absent_still_prunes() {
        let schema = single_field_schema("val", PrimitiveType::Double);
        let bloom_filters = HashMap::from([(
            1,
            ColumnBloomFilter::new(
                create_bloom_filter_with_values_f32(&[1.5, 2.25, 4.0]),
                PhysicalType::FLOAT,
                0,
            ),
        )]);

        let predicate = Reference::new("val")
            .equal_to(Datum::double(9.75))
            .bind(schema.into(), true)
            .unwrap();

        let result = BloomFilterEvaluator::eval(&predicate, &bloom_filters).unwrap();
        assert!(!result, "genuinely absent value must still prune");
    }

    /// A double with no exact `f32` representation cannot equal any widened
    /// `f32` in the column, so probing it would be meaningless; stay conservative.
    #[test]
    fn test_double_not_representable_as_f32_does_not_prune() {
        let schema = single_field_schema("val", PrimitiveType::Double);
        let bloom_filters = HashMap::from([(
            1,
            ColumnBloomFilter::new(
                create_bloom_filter_with_values_f32(&[1.5, 2.25]),
                PhysicalType::FLOAT,
                0,
            ),
        )]);

        let predicate = Reference::new("val")
            .equal_to(Datum::double(0.1))
            .bind(schema.into(), true)
            .unwrap();

        let result = BloomFilterEvaluator::eval(&predicate, &bloom_filters).unwrap();
        assert!(result, "non-representable double must not prune");
    }
}
