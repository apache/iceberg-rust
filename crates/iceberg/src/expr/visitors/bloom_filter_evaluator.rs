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
use crate::spec::decimal_utils::decimal_to_fixed_length_bytes;
use crate::spec::{Datum, PrimitiveLiteral};

const ROW_GROUP_MIGHT_MATCH: Result<bool> = Ok(true);
const ROW_GROUP_CANT_MATCH: Result<bool> = Ok(false);

pub(crate) struct BloomFilterEvaluator<'a> {
    /// Maps Iceberg field_id -> (bloom filter, Parquet physical type) for this row group
    bloom_filters: &'a HashMap<i32, (Sbbf, PhysicalType)>,
}

impl<'a> BloomFilterEvaluator<'a> {
    /// Evaluate the predicate against the provided bloom filters.
    /// Returns `false` if the row group definitely does not match,
    /// `true` if it might match.
    pub(crate) fn eval(
        filter: &BoundPredicate,
        bloom_filters: &HashMap<i32, (Sbbf, PhysicalType)>,
    ) -> Result<bool> {
        if bloom_filters.is_empty() {
            return ROW_GROUP_MIGHT_MATCH;
        }

        let mut evaluator = BloomFilterEvaluator { bloom_filters };
        visit(&mut evaluator, filter)
    }

    fn check_datum(&self, reference: &BoundReference, datum: &Datum) -> bool {
        let field_id = reference.field().id;
        let Some((sbbf, physical_type)) = self.bloom_filters.get(&field_id) else {
            // No bloom filter for this column — conservatively might match
            return true;
        };

        check_in_bloom_filter(sbbf, datum, *physical_type)
    }
}

/// Collects field IDs that appear in `eq` or `in` predicates — the only
/// predicate types that benefit from bloom filter checks.
pub(crate) fn collect_bloom_filter_field_ids(predicate: &BoundPredicate) -> Result<HashSet<i32>> {
    let mut visitor = BloomFilterFieldIdCollector {
        field_ids: HashSet::new(),
    };
    visit(&mut visitor, predicate)?;
    Ok(visitor.field_ids)
}

struct BloomFilterFieldIdCollector {
    field_ids: HashSet<i32>,
}

impl BoundPredicateVisitor for BloomFilterFieldIdCollector {
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

    fn is_null(&mut self, _r: &BoundReference, _p: &BoundPredicate) -> Result<()> {
        Ok(())
    }

    fn not_null(&mut self, _r: &BoundReference, _p: &BoundPredicate) -> Result<()> {
        Ok(())
    }

    fn is_nan(&mut self, _r: &BoundReference, _p: &BoundPredicate) -> Result<()> {
        Ok(())
    }

    fn not_nan(&mut self, _r: &BoundReference, _p: &BoundPredicate) -> Result<()> {
        Ok(())
    }

    fn less_than(&mut self, _r: &BoundReference, _l: &Datum, _p: &BoundPredicate) -> Result<()> {
        Ok(())
    }

    fn less_than_or_eq(
        &mut self,
        _r: &BoundReference,
        _l: &Datum,
        _p: &BoundPredicate,
    ) -> Result<()> {
        Ok(())
    }

    fn greater_than(&mut self, _r: &BoundReference, _l: &Datum, _p: &BoundPredicate) -> Result<()> {
        Ok(())
    }

    fn greater_than_or_eq(
        &mut self,
        _r: &BoundReference,
        _l: &Datum,
        _p: &BoundPredicate,
    ) -> Result<()> {
        Ok(())
    }

    fn eq(&mut self, r: &BoundReference, _l: &Datum, _p: &BoundPredicate) -> Result<()> {
        self.field_ids.insert(r.field().id);
        Ok(())
    }

    fn not_eq(&mut self, _r: &BoundReference, _l: &Datum, _p: &BoundPredicate) -> Result<()> {
        Ok(())
    }

    fn starts_with(&mut self, _r: &BoundReference, _l: &Datum, _p: &BoundPredicate) -> Result<()> {
        Ok(())
    }

    fn not_starts_with(
        &mut self,
        _r: &BoundReference,
        _l: &Datum,
        _p: &BoundPredicate,
    ) -> Result<()> {
        Ok(())
    }

    fn r#in(
        &mut self,
        r: &BoundReference,
        _literals: &FnvHashSet<Datum>,
        _p: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(r.field().id);
        Ok(())
    }

    fn not_in(
        &mut self,
        _r: &BoundReference,
        _literals: &FnvHashSet<Datum>,
        _p: &BoundPredicate,
    ) -> Result<()> {
        Ok(())
    }
}

/// Check whether a datum value might be present in the bloom filter.
///
/// The value must be checked using the same physical encoding the Parquet
/// writer used when inserting into the bloom filter. We use the actual
/// physical type from the column metadata to ensure correctness regardless
/// of which writer produced the file.
fn check_in_bloom_filter(sbbf: &Sbbf, datum: &Datum, physical_type: PhysicalType) -> bool {
    match datum.literal() {
        PrimitiveLiteral::Boolean(v) => sbbf.check(v),
        PrimitiveLiteral::Int(v) => sbbf.check(v),
        PrimitiveLiteral::Long(v) => sbbf.check(v),
        PrimitiveLiteral::Float(v) => sbbf.check(&v.0),
        PrimitiveLiteral::Double(v) => sbbf.check(&v.0),
        PrimitiveLiteral::String(v) => sbbf.check(v.as_str()),
        PrimitiveLiteral::Binary(v) => sbbf.check(v.as_slice()),
        PrimitiveLiteral::Int128(v) => {
            // Decimal: dispatch based on the actual Parquet physical type
            // from the file, not inferred from precision.
            match physical_type {
                PhysicalType::INT32 => sbbf.check(&(*v as i32)),
                PhysicalType::INT64 => sbbf.check(&(*v as i64)),
                PhysicalType::FIXED_LEN_BYTE_ARRAY => {
                    // to_be_bytes() truncated to the column's fixed length.
                    // We use the Iceberg type's precision to determine the
                    // byte length, which must match the file's fixed length.
                    let crate::spec::PrimitiveType::Decimal { precision, .. } = datum.data_type()
                    else {
                        return true;
                    };
                    let bytes = decimal_to_fixed_length_bytes(*v, *precision);
                    sbbf.check(&ByteArray::from(bytes))
                }
                _ => true, // Unexpected physical type — conservatively might match
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

impl<'a> BoundPredicateVisitor for BloomFilterEvaluator<'a> {
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
        let Some((sbbf, physical_type)) = self.bloom_filters.get(&field_id) else {
            return ROW_GROUP_MIGHT_MATCH;
        };

        // If ANY literal might be present, the row group might match
        for literal in literals {
            if check_in_bloom_filter(sbbf, literal, *physical_type) {
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

    use super::BloomFilterEvaluator;
    use crate::expr::{Bind, Reference};
    use crate::spec::decimal_utils::decimal_to_fixed_length_bytes;
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
            (
                create_bloom_filter_with_values_i32(&[1, 2, 3]),
                PhysicalType::INT32,
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
            (
                create_bloom_filter_with_values_i32(&[1, 2, 3]),
                PhysicalType::INT32,
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
            (
                create_bloom_filter_with_values_i32(&[1, 2, 3]),
                PhysicalType::INT32,
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
            (
                create_bloom_filter_with_values_i32(&[1, 2, 3]),
                PhysicalType::INT32,
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
                (
                    create_bloom_filter_with_values_i32(&[1, 2, 3]),
                    PhysicalType::INT32,
                ),
            ),
            (
                2,
                (
                    create_bloom_filter_with_values_str(&["alice", "bob"]),
                    PhysicalType::BYTE_ARRAY,
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
            (
                create_bloom_filter_with_values_i32(&[1, 2, 3]),
                PhysicalType::INT32,
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
            (
                create_bloom_filter_with_values_i32(&[1, 2, 3]),
                PhysicalType::INT32,
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

    #[test]
    fn test_range_predicates_always_might_match() {
        let schema = create_test_schema();
        let bloom_filters = HashMap::from([(
            1,
            (
                create_bloom_filter_with_values_i32(&[1, 2, 3]),
                PhysicalType::INT32,
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
            (
                create_bloom_filter_with_values_str(&["alice", "bob"]),
                PhysicalType::BYTE_ARRAY,
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
            (
                create_bloom_filter_with_values_str(&["alice", "bob"]),
                PhysicalType::BYTE_ARRAY,
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

        let bloom_filters = HashMap::from([(1, (sbbf, PhysicalType::INT32))]);

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

        let bloom_filters = HashMap::from([(1, (sbbf, PhysicalType::INT32))]);

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

        let bloom_filters = HashMap::from([(1, (sbbf, PhysicalType::INT64))]);

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

        let bloom_filters = HashMap::from([(1, (sbbf, PhysicalType::INT64))]);

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
        let bytes = decimal_to_fixed_length_bytes(mantissa, 25);

        let mut sbbf = Sbbf::new_with_ndv_fpp(10, 0.01).unwrap();
        sbbf.insert(&ByteArray::from(bytes));

        let bloom_filters = HashMap::from([(1, (sbbf, PhysicalType::FIXED_LEN_BYTE_ARRAY))]);

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
        let bytes = decimal_to_fixed_length_bytes(mantissa, 25);

        let mut sbbf = Sbbf::new_with_ndv_fpp(10, 0.01).unwrap();
        sbbf.insert(&ByteArray::from(bytes));

        let bloom_filters = HashMap::from([(1, (sbbf, PhysicalType::FIXED_LEN_BYTE_ARRAY))]);

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

        let bloom_filters = HashMap::from([(1, (sbbf, PhysicalType::INT32))]);

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
        let bytes = decimal_to_fixed_length_bytes(mantissa, 25);

        let mut sbbf = Sbbf::new_with_ndv_fpp(10, 0.01).unwrap();
        sbbf.insert(&ByteArray::from(bytes));

        let bloom_filters = HashMap::from([(1, (sbbf, PhysicalType::FIXED_LEN_BYTE_ARRAY))]);

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
        let bytes = decimal_to_fixed_length_bytes(mantissa, 5);

        let mut sbbf = Sbbf::new_with_ndv_fpp(10, 0.01).unwrap();
        sbbf.insert(&ByteArray::from(bytes));

        let bloom_filters = HashMap::from([(1, (sbbf, PhysicalType::FIXED_LEN_BYTE_ARRAY))]);

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
}
