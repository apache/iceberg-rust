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

//! Transforms in iceberg.

use crate::error::{Error, Result};
use crate::expr::{
    BinaryExpression, BoundPredicate, Predicate, PredicateOperator, Reference, SetExpression,
    UnaryExpression,
};
use crate::spec::datatypes::{PrimitiveType, Type};
use crate::transform::{create_transform_function, BoxedTransformFunction};
use crate::ErrorKind;
use fnv::FnvHashSet;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::{Display, Formatter};
use std::str::FromStr;

use super::{Datum, PrimitiveLiteral};

/// Transform is used to transform predicates to partition predicates,
/// in addition to transforming data values.
///
/// Deriving partition predicates from column predicates on the table data
/// is used to separate the logical queries from physical storage: the
/// partitioning can change and the correct partition filters are always
/// derived from column predicates.
///
/// This simplifies queries because users don’t have to supply both logical
/// predicates and partition predicates.
///
/// All transforms must return `null` for a `null` input value.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Transform {
    /// Source value, unmodified
    ///
    /// - Source type could be any type.
    /// - Return type is the same with source type.
    Identity,
    /// Hash of value, mod `N`.
    ///
    /// Bucket partition transforms use a 32-bit hash of the source value.
    /// The 32-bit hash implementation is the 32-bit Murmur3 hash, x86
    /// variant, seeded with 0.
    ///
    /// Transforms are parameterized by a number of buckets, N. The hash mod
    /// N must produce a positive value by first discarding the sign bit of
    /// the hash value. In pseudo-code, the function is:
    ///
    /// ```text
    /// def bucket_N(x) = (murmur3_x86_32_hash(x) & Integer.MAX_VALUE) % N
    /// ```
    ///
    /// - Source type could be `int`, `long`, `decimal`, `date`, `time`,
    ///   `timestamp`, `timestamptz`, `string`, `uuid`, `fixed`, `binary`.
    /// - Return type is `int`.
    Bucket(u32),
    /// Value truncated to width `W`
    ///
    /// For `int`:
    ///
    /// - `v - (v % W)` remainders must be positive
    /// - example: W=10: 1 ￫ 0, -1 ￫ -10
    /// - note: The remainder, v % W, must be positive.
    ///
    /// For `long`:
    ///
    /// - `v - (v % W)` remainders must be positive
    /// - example: W=10: 1 ￫ 0, -1 ￫ -10
    /// - note: The remainder, v % W, must be positive.
    ///
    /// For `decimal`:
    ///
    /// - `scaled_W = decimal(W, scale(v)) v - (v % scaled_W)`
    /// - example: W=50, s=2: 10.65 ￫ 10.50
    ///
    /// For `string`:
    ///
    /// - Substring of length L: `v.substring(0, L)`
    /// - example: L=3: iceberg ￫ ice
    /// - note: Strings are truncated to a valid UTF-8 string with no more
    ///   than L code points.
    ///
    /// - Source type could be `int`, `long`, `decimal`, `string`
    /// - Return type is the same with source type.
    Truncate(u32),
    /// Extract a date or timestamp year, as years from 1970
    ///
    /// - Source type could be `date`, `timestamp`, `timestamptz`
    /// - Return type is `int`
    Year,
    /// Extract a date or timestamp month, as months from 1970-01-01
    ///
    /// - Source type could be `date`, `timestamp`, `timestamptz`
    /// - Return type is `int`
    Month,
    /// Extract a date or timestamp day, as days from 1970-01-01
    ///
    /// - Source type could be `date`, `timestamp`, `timestamptz`
    /// - Return type is `int`
    Day,
    /// Extract a timestamp hour, as hours from 1970-01-01 00:00:00
    ///
    /// - Source type could be `timestamp`, `timestamptz`
    /// - Return type is `int`
    Hour,
    /// Always produces `null`
    ///
    /// The void transform may be used to replace the transform in an
    /// existing partition field so that the field is effectively dropped in
    /// v1 tables.
    ///
    /// - Source type could be any type..
    /// - Return type is Source type.
    Void,
    /// Used to represent some customized transform that can't be recognized or supported now.
    Unknown,
}

impl Transform {
    /// Get the return type of transform given the input type.
    /// Returns `None` if it can't be transformed.
    pub fn result_type(&self, input_type: &Type) -> Result<Type> {
        match self {
            Transform::Identity => {
                if matches!(input_type, Type::Primitive(_)) {
                    Ok(input_type.clone())
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("{input_type} is not a valid input type of identity transform",),
                    ))
                }
            }
            Transform::Void => Ok(input_type.clone()),
            Transform::Unknown => Ok(Type::Primitive(PrimitiveType::String)),
            Transform::Bucket(_) => {
                if let Type::Primitive(p) = input_type {
                    match p {
                        PrimitiveType::Int
                        | PrimitiveType::Long
                        | PrimitiveType::Decimal { .. }
                        | PrimitiveType::Date
                        | PrimitiveType::Time
                        | PrimitiveType::Timestamp
                        | PrimitiveType::Timestamptz
                        | PrimitiveType::String
                        | PrimitiveType::Uuid
                        | PrimitiveType::Fixed(_)
                        | PrimitiveType::Binary => Ok(Type::Primitive(PrimitiveType::Int)),
                        _ => Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!("{input_type} is not a valid input type of bucket transform",),
                        )),
                    }
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("{input_type} is not a valid input type of bucket transform",),
                    ))
                }
            }
            Transform::Truncate(_) => {
                if let Type::Primitive(p) = input_type {
                    match p {
                        PrimitiveType::Int
                        | PrimitiveType::Long
                        | PrimitiveType::String
                        | PrimitiveType::Binary
                        | PrimitiveType::Decimal { .. } => Ok(input_type.clone()),
                        _ => Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!("{input_type} is not a valid input type of truncate transform",),
                        )),
                    }
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("{input_type} is not a valid input type of truncate transform",),
                    ))
                }
            }
            Transform::Year | Transform::Month | Transform::Day => {
                if let Type::Primitive(p) = input_type {
                    match p {
                        PrimitiveType::Timestamp
                        | PrimitiveType::Timestamptz
                        | PrimitiveType::Date => Ok(Type::Primitive(PrimitiveType::Int)),
                        _ => Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!("{input_type} is not a valid input type of {self} transform",),
                        )),
                    }
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("{input_type} is not a valid input type of {self} transform",),
                    ))
                }
            }
            Transform::Hour => {
                if let Type::Primitive(p) = input_type {
                    match p {
                        PrimitiveType::Timestamp | PrimitiveType::Timestamptz => {
                            Ok(Type::Primitive(PrimitiveType::Int))
                        }
                        _ => Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!("{input_type} is not a valid input type of {self} transform",),
                        )),
                    }
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("{input_type} is not a valid input type of {self} transform",),
                    ))
                }
            }
        }
    }

    /// Whether the transform preserves the order of values.
    pub fn preserves_order(&self) -> bool {
        !matches!(
            self,
            Transform::Void | Transform::Bucket(_) | Transform::Unknown
        )
    }

    /// Return the unique transform name to check if similar transforms for the same source field
    /// are added multiple times in partition spec builder.
    pub fn dedup_name(&self) -> String {
        match self {
            Transform::Year | Transform::Month | Transform::Day | Transform::Hour => {
                "time".to_string()
            }
            _ => format!("{self}"),
        }
    }

    /// Whether ordering by this transform's result satisfies the ordering of another transform's
    /// result.
    ///
    /// For example, sorting by day(ts) will produce an ordering that is also by month(ts) or
    ///  year(ts). However, sorting by day(ts) will not satisfy the order of hour(ts) or identity(ts).
    pub fn satisfies_order_of(&self, other: &Self) -> bool {
        match self {
            Transform::Identity => other.preserves_order(),
            Transform::Hour => matches!(
                other,
                Transform::Hour | Transform::Day | Transform::Month | Transform::Year
            ),
            Transform::Day => matches!(other, Transform::Day | Transform::Month | Transform::Year),
            Transform::Month => matches!(other, Transform::Month | Transform::Year),
            _ => self == other,
        }
    }

    /// Projects a given predicate according to the transformation
    /// specified by the `Transform` instance.
    ///
    /// This allows predicates to be effectively applied to data
    /// that has undergone transformation, enabling efficient querying
    /// and filtering based on the original, untransformed data.
    ///
    /// # Example
    /// Suppose, we have row filter `a = 10`, and a partition spec
    /// `bucket(a, 37) as bs`, if one row matches `a = 10`, then its partition
    /// value should match `bucket(10, 37) as bs`, and we project `a = 10` to
    /// `bs = bucket(10, 37)`
    pub fn project(&self, name: String, predicate: &BoundPredicate) -> Result<Option<Predicate>> {
        let func = create_transform_function(self)?;

        let projection = match predicate {
            BoundPredicate::Unary(expr) => match self {
                Transform::Identity
                | Transform::Bucket(_)
                | Transform::Truncate(_)
                | Transform::Year
                | Transform::Month
                | Transform::Day
                | Transform::Hour => Some(Predicate::Unary(UnaryExpression::new(
                    expr.op(),
                    Reference::new(name),
                ))),
                _ => None,
            },
            BoundPredicate::Binary(expr) => match self {
                Transform::Identity => Some(Predicate::Binary(BinaryExpression::new(
                    expr.op(),
                    Reference::new(name),
                    expr.literal().to_owned(),
                ))),
                Transform::Bucket(_) => {
                    if expr.op() != PredicateOperator::Eq || !self.can_transform(expr.literal()) {
                        return Ok(None);
                    }

                    Some(Predicate::Binary(BinaryExpression::new(
                        expr.op(),
                        Reference::new(name),
                        func.transform_literal_result(expr.literal())?,
                    )))
                }
                Transform::Truncate(width) => {
                    if !self.can_transform(expr.literal()) {
                        return Ok(None);
                    }

                    self.transform_projected_boundary(
                        name,
                        expr.literal(),
                        &expr.op(),
                        &func,
                        Some(*width),
                    )?
                }
                Transform::Year | Transform::Month | Transform::Day | Transform::Hour => {
                    if !self.can_transform(expr.literal()) {
                        return Ok(None);
                    }

                    self.transform_projected_boundary(
                        name,
                        expr.literal(),
                        &expr.op(),
                        &func,
                        None,
                    )?
                }
                _ => None,
            },
            BoundPredicate::Set(expr) => match self {
                Transform::Identity => Some(Predicate::Set(SetExpression::new(
                    expr.op(),
                    Reference::new(name),
                    expr.literals().to_owned(),
                ))),
                Transform::Bucket(_)
                | Transform::Truncate(_)
                | Transform::Year
                | Transform::Month
                | Transform::Day
                | Transform::Hour => {
                    if expr.op() != PredicateOperator::In
                        || expr.literals().iter().any(|d| !self.can_transform(d))
                    {
                        return Ok(None);
                    }

                    Some(Predicate::Set(SetExpression::new(
                        expr.op(),
                        Reference::new(name),
                        self.transform_set(expr.literals(), &func)?,
                    )))
                }
                _ => None,
            },
            _ => None,
        };

        Ok(projection)
    }

    /// Check if `Transform` is applicable on datum's `PrimitiveType`
    fn can_transform(&self, datum: &Datum) -> bool {
        let input_type = datum.data_type().clone();
        self.result_type(&Type::Primitive(input_type)).is_ok()
    }

    /// Transform each literal value of `FnvHashSet<Datum>`
    fn transform_set(
        &self,
        literals: &FnvHashSet<Datum>,
        func: &BoxedTransformFunction,
    ) -> Result<FnvHashSet<Datum>> {
        let mut new_set = FnvHashSet::default();

        for lit in literals {
            let datum = func.transform_literal_result(lit)?;

            if let Some(AdjustedProjection::Single(d)) =
                self.adjust_projection(&PredicateOperator::In, lit, &datum)
            {
                new_set.insert(d);
            };

            new_set.insert(datum);
        }

        Ok(new_set)
    }

    /// Apply transform on `Datum` with adjusted boundaries.
    /// Returns Predicate with projection and possibly
    /// rewritten `PredicateOperator`
    fn transform_projected_boundary(
        &self,
        name: String,
        datum: &Datum,
        op: &PredicateOperator,
        func: &BoxedTransformFunction,
        width: Option<u32>,
    ) -> Result<Option<Predicate>> {
        if let Some(boundary) = self.projected_boundary(op, datum)? {
            let transformed = func.transform_literal_result(&boundary)?;
            let adjusted = self.adjust_projection(op, datum, &transformed);
            let op = self.projected_operator(op, datum, width);

            if let Some(op) = op {
                let predicate = match adjusted {
                    None => Predicate::Binary(BinaryExpression::new(
                        op,
                        Reference::new(name),
                        transformed,
                    )),
                    Some(AdjustedProjection::Single(d)) => {
                        Predicate::Binary(BinaryExpression::new(op, Reference::new(name), d))
                    }
                    Some(AdjustedProjection::Set(d)) => Predicate::Set(SetExpression::new(
                        PredicateOperator::In,
                        Reference::new(name),
                        d,
                    )),
                };
                return Ok(Some(predicate));
            }
        };

        Ok(None)
    }

    /// Create a new `Datum` with adjusted projection boundary.
    /// Returns `None` if `PredicateOperator` and `PrimitiveLiteral`
    /// can not be projected
    fn projected_boundary(&self, op: &PredicateOperator, datum: &Datum) -> Result<Option<Datum>> {
        let literal = datum.literal();

        let projected_boundary = match op {
            PredicateOperator::LessThan => match literal {
                PrimitiveLiteral::Int(v) => Some(Datum::int(v - 1)),
                PrimitiveLiteral::Long(v) => Some(Datum::long(v - 1)),
                PrimitiveLiteral::Decimal(v) => Some(Datum::decimal(v - 1)?),
                PrimitiveLiteral::Date(v) => Some(Datum::date(v - 1)),
                PrimitiveLiteral::Timestamp(v) => Some(Datum::timestamp_micros(v - 1)),
                _ => Some(datum.to_owned()),
            },
            PredicateOperator::GreaterThan => match literal {
                PrimitiveLiteral::Int(v) => Some(Datum::int(v + 1)),
                PrimitiveLiteral::Long(v) => Some(Datum::long(v + 1)),
                PrimitiveLiteral::Decimal(v) => Some(Datum::decimal(v + 1)?),
                PrimitiveLiteral::Date(v) => Some(Datum::date(v + 1)),
                PrimitiveLiteral::Timestamp(v) => Some(Datum::timestamp_micros(v + 1)),
                _ => Some(datum.to_owned()),
            },
            PredicateOperator::Eq
            | PredicateOperator::LessThanOrEq
            | PredicateOperator::GreaterThanOrEq
            | PredicateOperator::StartsWith
            | PredicateOperator::NotStartsWith => Some(datum.to_owned()),
            _ => None,
        };

        Ok(projected_boundary)
    }

    /// Create a new `PredicateOperator`, rewritten for projection
    fn projected_operator(
        &self,
        op: &PredicateOperator,
        datum: &Datum,
        width: Option<u32>,
    ) -> Option<PredicateOperator> {
        match op {
            PredicateOperator::LessThan => Some(PredicateOperator::LessThanOrEq),
            PredicateOperator::GreaterThan => Some(PredicateOperator::GreaterThanOrEq),
            PredicateOperator::StartsWith => match datum.literal() {
                PrimitiveLiteral::String(s) => {
                    if let Some(w) = width {
                        if s.len() == w as usize {
                            return Some(PredicateOperator::Eq);
                        };
                    };
                    Some(*op)
                }
                _ => Some(*op),
            },
            PredicateOperator::NotStartsWith => match datum.literal() {
                PrimitiveLiteral::String(s) => {
                    if let Some(w) = width {
                        let w = w as usize;

                        if s.len() == w {
                            return Some(PredicateOperator::NotEq);
                        }

                        if s.len() < w {
                            return Some(*op);
                        }

                        return None;
                    };
                    Some(*op)
                }
                _ => Some(*op),
            },
            _ => Some(*op),
        }
    }

    /// Adjust time projection
    ///https://github.com/apache/iceberg/blob/main/api/src/main/java/org/apache/iceberg/transforms/ProjectionUtil.java#L275
    fn adjust_projection(
        &self,
        op: &PredicateOperator,
        original: &Datum,
        transformed: &Datum,
    ) -> Option<AdjustedProjection> {
        let should_adjust = match self {
            Transform::Day => matches!(original.literal(), PrimitiveLiteral::Timestamp(_)),
            Transform::Year | Transform::Month => true,
            _ => false,
        };

        if should_adjust {
            if let &PrimitiveLiteral::Int(v) = transformed.literal() {
                match op {
                    PredicateOperator::LessThan
                    | PredicateOperator::LessThanOrEq
                    | PredicateOperator::In => {
                        if v < 0 {
                            return Some(AdjustedProjection::Single(Datum::int(v + 1)));
                        };
                    }
                    PredicateOperator::Eq => {
                        if v < 0 {
                            let new_set = FnvHashSet::from_iter(vec![
                                transformed.to_owned(),
                                Datum::int(v + 1),
                            ]);
                            return Some(AdjustedProjection::Set(new_set));
                        }
                    }
                    _ => {
                        return None;
                    }
                }
            };
        }
        None
    }
}

impl Display for Transform {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Transform::Identity => write!(f, "identity"),
            Transform::Year => write!(f, "year"),
            Transform::Month => write!(f, "month"),
            Transform::Day => write!(f, "day"),
            Transform::Hour => write!(f, "hour"),
            Transform::Void => write!(f, "void"),
            Transform::Bucket(length) => write!(f, "bucket[{length}]"),
            Transform::Truncate(width) => write!(f, "truncate[{width}]"),
            Transform::Unknown => write!(f, "unknown"),
        }
    }
}

impl FromStr for Transform {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let t = match s {
            "identity" => Transform::Identity,
            "year" => Transform::Year,
            "month" => Transform::Month,
            "day" => Transform::Day,
            "hour" => Transform::Hour,
            "void" => Transform::Void,
            "unknown" => Transform::Unknown,
            v if v.starts_with("bucket") => {
                let length = v
                    .strip_prefix("bucket")
                    .expect("transform must starts with `bucket`")
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .parse()
                    .map_err(|err| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!("transform bucket type {v:?} is invalid"),
                        )
                        .with_source(err)
                    })?;

                Transform::Bucket(length)
            }
            v if v.starts_with("truncate") => {
                let width = v
                    .strip_prefix("truncate")
                    .expect("transform must starts with `truncate`")
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .parse()
                    .map_err(|err| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!("transform truncate type {v:?} is invalid"),
                        )
                        .with_source(err)
                    })?;

                Transform::Truncate(width)
            }
            v => {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("transform {v:?} is invalid"),
                ))
            }
        };

        Ok(t)
    }
}

impl Serialize for Transform {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(format!("{self}").as_str())
    }
}

impl<'de> Deserialize<'de> for Transform {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(<D::Error as serde::de::Error>::custom)
    }
}

#[derive(Debug)]
enum AdjustedProjection {
    Single(Datum),
    Set(FnvHashSet<Datum>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::Arc;

    use crate::expr::{BoundPredicate, BoundReference, PredicateOperator};
    use crate::spec::datatypes::PrimitiveType::{
        Binary, Date, Decimal, Fixed, Int, Long, String as StringType, Time, Timestamp,
        Timestamptz, Uuid,
    };
    use crate::spec::datatypes::Type::{Primitive, Struct};
    use crate::spec::datatypes::{NestedField, StructType, Type};
    use crate::spec::transform::Transform;
    use crate::spec::{Datum, NestedFieldRef, PrimitiveType};

    struct TestParameter {
        display: String,
        json: String,
        dedup_name: String,
        preserves_order: bool,
        satisfies_order_of: Vec<(Transform, bool)>,
        trans_types: Vec<(Type, Option<Type>)>,
    }

    struct TestProjectionParameter {
        transform: Transform,
        name: String,
        field: NestedFieldRef,
    }

    impl TestProjectionParameter {
        fn new(transform: Transform, name: impl Into<String>, field: NestedField) -> Self {
            TestProjectionParameter {
                transform,
                name: name.into(),
                field: Arc::new(field),
            }
        }
        fn name(&self) -> String {
            self.name.clone()
        }
        fn field(&self) -> NestedFieldRef {
            self.field.clone()
        }
        fn project(&self, predicate: &BoundPredicate) -> Result<Option<Predicate>> {
            self.transform.project(self.name(), predicate)
        }
        fn _unary_predicate(&self, op: PredicateOperator) -> BoundPredicate {
            BoundPredicate::Unary(UnaryExpression::new(
                op,
                BoundReference::new(self.name(), self.field()),
            ))
        }
        fn binary_predicate(&self, op: PredicateOperator, literal: Datum) -> BoundPredicate {
            BoundPredicate::Binary(BinaryExpression::new(
                op,
                BoundReference::new(self.name(), self.field()),
                literal,
            ))
        }
        fn set_predicate(&self, op: PredicateOperator, literals: Vec<Datum>) -> BoundPredicate {
            BoundPredicate::Set(SetExpression::new(
                op,
                BoundReference::new(self.name(), self.field()),
                HashSet::from_iter(literals),
            ))
        }
    }

    fn assert_projection(
        predicate: &BoundPredicate,
        fixture: &TestProjectionParameter,
        expected: Option<&str>,
    ) -> Result<()> {
        let result = fixture.project(predicate)?;
        match expected {
            Some(exp) => assert_eq!(format!("{}", result.unwrap()), exp),
            None => assert!(result.is_none()),
        }
        Ok(())
    }

    fn check_transform(trans: Transform, param: TestParameter) {
        assert_eq!(param.display, format!("{trans}"));
        assert_eq!(param.json, serde_json::to_string(&trans).unwrap());
        assert_eq!(trans, serde_json::from_str(param.json.as_str()).unwrap());
        assert_eq!(param.dedup_name, trans.dedup_name());
        assert_eq!(param.preserves_order, trans.preserves_order());

        for (other_trans, satisfies_order_of) in param.satisfies_order_of {
            assert_eq!(
                satisfies_order_of,
                trans.satisfies_order_of(&other_trans),
                "Failed to check satisfies order {}, {}, {}",
                trans,
                other_trans,
                satisfies_order_of
            );
        }

        for (input_type, result_type) in param.trans_types {
            assert_eq!(result_type, trans.result_type(&input_type).ok());
        }
    }
    #[test]
    fn test_projection_timestamp_hour_lower_bound() -> Result<()> {
        // 420034 //420010
        let value = "2017-12-01T10:00:00.000000";
        // 411288
        let _another = "2016-12-02T00:00:00.000000";

        let fixture = TestProjectionParameter::new(
            Transform::Hour,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Timestamp)),
        );

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name <= 420033"),
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_timestamp_year_upper_bound() -> Result<()> {
        let value = "2017-12-31T23:59:59.999999";
        let another = "2016-12-31T23:59:59.999999";

        let fixture = TestProjectionParameter::new(
            Transform::Year,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Timestamp)),
        );

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name <= 47"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name <= 47"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name >= 48"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name >= 47"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::timestamp_from_str(value)?),
            &fixture,
            Some("name = 47"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::timestamp_from_str(value)?),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::In,
                vec![
                    Datum::timestamp_from_str(value)?,
                    Datum::timestamp_from_str(another)?,
                ],
            ),
            &fixture,
            Some("name IN (47, 46)"),
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::NotIn,
                vec![
                    Datum::timestamp_from_str(value)?,
                    Datum::timestamp_from_str(another)?,
                ],
            ),
            &fixture,
            None,
        )?;
        Ok(())
    }

    #[test]
    fn test_projection_timestamp_year_lower_bound() -> Result<()> {
        let value = "2017-01-01T00:00:00.000000";
        let another = "2016-12-02T00:00:00.000000";

        let fixture = TestProjectionParameter::new(
            Transform::Year,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Timestamp)),
        );

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name <= 46"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name <= 47"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name >= 47"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name >= 47"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::timestamp_from_str(value)?),
            &fixture,
            Some("name = 47"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::timestamp_from_str(value)?),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::In,
                vec![
                    Datum::timestamp_from_str(value)?,
                    Datum::timestamp_from_str(another)?,
                ],
            ),
            &fixture,
            Some("name IN (47, 46)"),
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::NotIn,
                vec![
                    Datum::timestamp_from_str(value)?,
                    Datum::timestamp_from_str(another)?,
                ],
            ),
            &fixture,
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_timestamp_month_negative_upper_bound() -> Result<()> {
        let value = "1969-12-31T23:59:59.999999";
        let another = "1970-01-01T00:00:00.000000";

        let fixture = TestProjectionParameter::new(
            Transform::Month,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Timestamp)),
        );

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name <= 0"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name <= 0"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name >= 0"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name >= -1"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::timestamp_from_str(value)?),
            &fixture,
            Some("name IN (-1, 0)"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::timestamp_from_str(value)?),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::In,
                vec![
                    Datum::timestamp_from_str(value)?,
                    Datum::timestamp_from_str(another)?,
                ],
            ),
            &fixture,
            Some("name IN (0, -1)"),
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::NotIn,
                vec![
                    Datum::timestamp_from_str(value)?,
                    Datum::timestamp_from_str(another)?,
                ],
            ),
            &fixture,
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_timestamp_month_upper_bound() -> Result<()> {
        let value = "2017-12-01T23:59:59.999999";
        let another = "2017-11-02T00:00:00.000000";

        let fixture = TestProjectionParameter::new(
            Transform::Month,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Timestamp)),
        );

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name <= 575"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name <= 575"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name >= 575"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name >= 575"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::timestamp_from_str(value)?),
            &fixture,
            Some("name = 575"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::timestamp_from_str(value)?),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::In,
                vec![
                    Datum::timestamp_from_str(value)?,
                    Datum::timestamp_from_str(another)?,
                ],
            ),
            &fixture,
            Some("name IN (575, 574)"),
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::NotIn,
                vec![
                    Datum::timestamp_from_str(value)?,
                    Datum::timestamp_from_str(another)?,
                ],
            ),
            &fixture,
            None,
        )?;
        Ok(())
    }

    #[test]
    fn test_projection_timestamp_month_negative_lower_bound() -> Result<()> {
        let value = "1969-01-01T00:00:00.000000";
        let another = "1969-03-01T00:00:00.000000";

        let fixture = TestProjectionParameter::new(
            Transform::Month,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Timestamp)),
        );

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name <= -12"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name <= -11"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name >= -12"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name >= -12"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::timestamp_from_str(value)?),
            &fixture,
            Some("name IN (-12, -11)"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::timestamp_from_str(value)?),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::In,
                vec![
                    Datum::timestamp_from_str(value)?,
                    Datum::timestamp_from_str(another)?,
                ],
            ),
            &fixture,
            Some("name IN (-10, -9, -12, -11)"),
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::NotIn,
                vec![
                    Datum::timestamp_from_str(value)?,
                    Datum::timestamp_from_str(another)?,
                ],
            ),
            &fixture,
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_timestamp_month_lower_bound() -> Result<()> {
        let value = "2017-12-01T00:00:00.000000";
        let another = "2017-12-02T00:00:00.000000";

        let fixture = TestProjectionParameter::new(
            Transform::Month,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Timestamp)),
        );

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name <= 574"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name <= 575"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name >= 575"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name >= 575"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::timestamp_from_str(value)?),
            &fixture,
            Some("name = 575"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::timestamp_from_str(value)?),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::In,
                vec![
                    Datum::timestamp_from_str(value)?,
                    Datum::timestamp_from_str(another)?,
                ],
            ),
            &fixture,
            Some("name IN (575)"),
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::NotIn,
                vec![
                    Datum::timestamp_from_str(value)?,
                    Datum::timestamp_from_str(another)?,
                ],
            ),
            &fixture,
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_timestamp_day_negative_upper_bound() -> Result<()> {
        // -1
        let value = "1969-12-31T23:59:59.999999";
        // 0
        let another = "1970-01-01T00:00:00.000000";

        let fixture = TestProjectionParameter::new(
            Transform::Day,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Timestamp)),
        );

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name <= 0"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name <= 0"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name >= 0"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name >= -1"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::timestamp_from_str(value)?),
            &fixture,
            Some("name IN (-1, 0)"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::timestamp_from_str(value)?),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::In,
                vec![
                    Datum::timestamp_from_str(value)?,
                    Datum::timestamp_from_str(another)?,
                ],
            ),
            &fixture,
            Some("name IN (0, -1)"),
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::NotIn,
                vec![
                    Datum::timestamp_from_str(value)?,
                    Datum::timestamp_from_str(another)?,
                ],
            ),
            &fixture,
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_timestamp_day_upper_bound() -> Result<()> {
        // 17501
        let value = "2017-12-01T23:59:59.999999";
        // 17502
        let another = "2017-12-02T00:00:00.000000";

        let fixture = TestProjectionParameter::new(
            Transform::Day,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Timestamp)),
        );

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name <= 17501"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name <= 17501"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name >= 17502"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name >= 17501"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::timestamp_from_str(value)?),
            &fixture,
            Some("name = 17501"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::timestamp_from_str(value)?),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::In,
                vec![
                    Datum::timestamp_from_str(value)?,
                    Datum::timestamp_from_str(another)?,
                ],
            ),
            &fixture,
            Some("name IN (17501, 17502)"),
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::NotIn,
                vec![
                    Datum::timestamp_from_str(value)?,
                    Datum::timestamp_from_str(another)?,
                ],
            ),
            &fixture,
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_timestamp_day_negative_lower_bound() -> Result<()> {
        // -365
        let value = "1969-01-01T00:00:00.000000";
        // -364
        let another = "1969-01-02T00:00:00.000000";

        let fixture = TestProjectionParameter::new(
            Transform::Day,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Timestamp)),
        );

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name <= -365"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name <= -364"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name >= -365"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name >= -365"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::timestamp_from_str(value)?),
            &fixture,
            Some("name IN (-364, -365)"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::timestamp_from_str(value)?),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::In,
                vec![
                    Datum::timestamp_from_str(value)?,
                    Datum::timestamp_from_str(another)?,
                ],
            ),
            &fixture,
            Some("name IN (-363, -364, -365)"),
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::NotIn,
                vec![
                    Datum::timestamp_from_str(value)?,
                    Datum::timestamp_from_str(another)?,
                ],
            ),
            &fixture,
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_timestamp_day_lower_bound() -> Result<()> {
        // 17501
        let value = "2017-12-01T00:00:00.000000";
        // 17502
        let another = "2017-12-02T00:00:00.000000";

        let fixture = TestProjectionParameter::new(
            Transform::Day,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Timestamp)),
        );

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name <= 17500"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name <= 17501"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name >= 17501"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name >= 17501"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::timestamp_from_str(value)?),
            &fixture,
            Some("name = 17501"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::timestamp_from_str(value)?),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::In,
                vec![
                    Datum::timestamp_from_str(value)?,
                    Datum::timestamp_from_str(another)?,
                ],
            ),
            &fixture,
            Some("name IN (17501, 17502)"),
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::NotIn,
                vec![
                    Datum::timestamp_from_str(value)?,
                    Datum::timestamp_from_str(another)?,
                ],
            ),
            &fixture,
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_timestamp_day_epoch() -> Result<()> {
        // 0
        let value = "1970-01-01T00:00:00.00000";
        // 1
        let another = "1970-01-02T00:00:00.00000";

        let fixture = TestProjectionParameter::new(
            Transform::Day,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Timestamp)),
        );

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name <= 0"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name <= 0"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name >= 0"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            &fixture,
            Some("name >= 0"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::timestamp_from_str(value)?),
            &fixture,
            Some("name = 0"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::timestamp_from_str(value)?),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::In,
                vec![
                    Datum::timestamp_from_str(value)?,
                    Datum::timestamp_from_str(another)?,
                ],
            ),
            &fixture,
            Some("name IN (1, 0)"),
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::NotIn,
                vec![
                    Datum::timestamp_from_str(value)?,
                    Datum::timestamp_from_str(another)?,
                ],
            ),
            &fixture,
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_date_day_negative() -> Result<()> {
        // -2
        let value = "1969-12-30";
        // -4
        let another = "1969-12-28";

        let fixture = TestProjectionParameter::new(
            Transform::Day,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Date)),
        );

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::date_from_str(value)?),
            &fixture,
            Some("name <= -3"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::date_from_str(value)?,
            ),
            &fixture,
            Some("name <= -2"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::date_from_str(value)?),
            &fixture,
            Some("name >= -1"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::date_from_str(value)?,
            ),
            &fixture,
            Some("name >= -2"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::date_from_str(value)?),
            &fixture,
            Some("name = -2"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::date_from_str(value)?),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::In,
                vec![Datum::date_from_str(value)?, Datum::date_from_str(another)?],
            ),
            &fixture,
            Some("name IN (-2, -4)"),
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::NotIn,
                vec![Datum::date_from_str(value)?, Datum::date_from_str(another)?],
            ),
            &fixture,
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_date_day() -> Result<()> {
        // 17167
        let value = "2017-01-01";
        // 17531
        let another = "2017-12-31";

        let fixture = TestProjectionParameter::new(
            Transform::Day,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Date)),
        );

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::date_from_str(value)?),
            &fixture,
            Some("name <= 17166"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::date_from_str(value)?,
            ),
            &fixture,
            Some("name <= 17167"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::date_from_str(value)?),
            &fixture,
            Some("name >= 17168"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::date_from_str(value)?,
            ),
            &fixture,
            Some("name >= 17167"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::date_from_str(value)?),
            &fixture,
            Some("name = 17167"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::date_from_str(value)?),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::In,
                vec![Datum::date_from_str(value)?, Datum::date_from_str(another)?],
            ),
            &fixture,
            Some("name IN (17531, 17167)"),
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::NotIn,
                vec![Datum::date_from_str(value)?, Datum::date_from_str(another)?],
            ),
            &fixture,
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_date_month_negative_upper_bound() -> Result<()> {
        // -1 => 1969-12
        let value = "1969-12-31";
        // -12 => 1969-01
        let another = "1969-01-01";

        let fixture = TestProjectionParameter::new(
            Transform::Month,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Date)),
        );

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::date_from_str(value)?),
            &fixture,
            Some("name <= 0"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::date_from_str(value)?,
            ),
            &fixture,
            Some("name <= 0"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::date_from_str(value)?),
            &fixture,
            Some("name >= 0"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::date_from_str(value)?,
            ),
            &fixture,
            Some("name >= -1"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::date_from_str(value)?),
            &fixture,
            Some("name IN (-1, 0)"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::date_from_str(value)?),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::In,
                vec![Datum::date_from_str(value)?, Datum::date_from_str(another)?],
            ),
            &fixture,
            Some("name IN (-1, -12, -11, 0)"),
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::NotIn,
                vec![Datum::date_from_str(value)?, Datum::date_from_str(another)?],
            ),
            &fixture,
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_date_month_upper_bound() -> Result<()> {
        // 575 => 2017-12
        let value = "2017-12-31";
        // 564 => 2017-01
        let another = "2017-01-01";

        let fixture = TestProjectionParameter::new(
            Transform::Month,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Date)),
        );

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::date_from_str(value)?),
            &fixture,
            Some("name <= 575"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::date_from_str(value)?,
            ),
            &fixture,
            Some("name <= 575"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::date_from_str(value)?),
            &fixture,
            Some("name >= 576"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::date_from_str(value)?,
            ),
            &fixture,
            Some("name >= 575"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::date_from_str(value)?),
            &fixture,
            Some("name = 575"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::date_from_str(value)?),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::In,
                vec![Datum::date_from_str(value)?, Datum::date_from_str(another)?],
            ),
            &fixture,
            Some("name IN (575, 564)"),
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::NotIn,
                vec![Datum::date_from_str(value)?, Datum::date_from_str(another)?],
            ),
            &fixture,
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_date_month_negative_lower_bound() -> Result<()> {
        // -12 => 1969-01
        let value = "1969-01-01";
        // -1 => 1969-12
        let another = "1969-12-31";

        let fixture = TestProjectionParameter::new(
            Transform::Month,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Date)),
        );

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::date_from_str(value)?),
            &fixture,
            Some("name <= -12"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::date_from_str(value)?,
            ),
            &fixture,
            Some("name <= -11"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::date_from_str(value)?),
            &fixture,
            Some("name >= -12"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::date_from_str(value)?,
            ),
            &fixture,
            Some("name >= -12"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::date_from_str(value)?),
            &fixture,
            Some("name IN (-12, -11)"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::date_from_str(value)?),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::In,
                vec![Datum::date_from_str(value)?, Datum::date_from_str(another)?],
            ),
            &fixture,
            Some("name IN (-1, -12, -11, 0)"),
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::NotIn,
                vec![Datum::date_from_str(value)?, Datum::date_from_str(another)?],
            ),
            &fixture,
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_date_month_lower_bound() -> Result<()> {
        // 575 => 2017-12
        let value = "2017-12-01";
        // 564 => 2017-01
        let another = "2017-01-01";

        let fixture = TestProjectionParameter::new(
            Transform::Month,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Date)),
        );

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::date_from_str(value)?),
            &fixture,
            Some("name <= 574"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::date_from_str(value)?,
            ),
            &fixture,
            Some("name <= 575"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::date_from_str(value)?),
            &fixture,
            Some("name >= 575"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::date_from_str(value)?,
            ),
            &fixture,
            Some("name >= 575"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::date_from_str(value)?),
            &fixture,
            Some("name = 575"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::date_from_str(value)?),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::In,
                vec![Datum::date_from_str(value)?, Datum::date_from_str(another)?],
            ),
            &fixture,
            Some("name IN (575, 564)"),
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::NotIn,
                vec![Datum::date_from_str(value)?, Datum::date_from_str(another)?],
            ),
            &fixture,
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_date_month_epoch() -> Result<()> {
        // 0 => 1970-01
        let value = "1970-01-01";
        // -1 => 1969-12
        let another = "1969-12-31";

        let fixture = TestProjectionParameter::new(
            Transform::Month,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Date)),
        );

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::date_from_str(value)?),
            &fixture,
            Some("name <= 0"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::date_from_str(value)?,
            ),
            &fixture,
            Some("name <= 0"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::date_from_str(value)?),
            &fixture,
            Some("name >= 0"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::date_from_str(value)?,
            ),
            &fixture,
            Some("name >= 0"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::date_from_str(value)?),
            &fixture,
            Some("name = 0"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::date_from_str(value)?),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::In,
                vec![Datum::date_from_str(value)?, Datum::date_from_str(another)?],
            ),
            &fixture,
            Some("name IN (0, -1)"),
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::NotIn,
                vec![Datum::date_from_str(value)?, Datum::date_from_str(another)?],
            ),
            &fixture,
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_date_year_negative_upper_bound() -> Result<()> {
        // -1 => 1969
        let value = "1969-12-31";
        let another = "1969-01-01";

        let fixture = TestProjectionParameter::new(
            Transform::Year,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Date)),
        );

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::date_from_str(value)?),
            &fixture,
            Some("name <= 0"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::date_from_str(value)?,
            ),
            &fixture,
            Some("name <= 0"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::date_from_str(value)?),
            &fixture,
            Some("name >= 0"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::date_from_str(value)?,
            ),
            &fixture,
            Some("name >= -1"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::date_from_str(value)?),
            &fixture,
            Some("name IN (-1, 0)"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::date_from_str(value)?),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::In,
                vec![Datum::date_from_str(value)?, Datum::date_from_str(another)?],
            ),
            &fixture,
            Some("name IN (0, -1)"),
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::NotIn,
                vec![Datum::date_from_str(value)?, Datum::date_from_str(another)?],
            ),
            &fixture,
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_date_year_upper_bound() -> Result<()> {
        // 47 => 2017
        let value = "2017-12-31";
        // 46 => 2016
        let another = "2016-01-01";

        let fixture = TestProjectionParameter::new(
            Transform::Year,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Date)),
        );

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::date_from_str(value)?),
            &fixture,
            Some("name <= 47"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::date_from_str(value)?,
            ),
            &fixture,
            Some("name <= 47"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::date_from_str(value)?),
            &fixture,
            Some("name >= 48"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::date_from_str(value)?,
            ),
            &fixture,
            Some("name >= 47"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::date_from_str(value)?),
            &fixture,
            Some("name = 47"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::date_from_str(value)?),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::In,
                vec![Datum::date_from_str(value)?, Datum::date_from_str(another)?],
            ),
            &fixture,
            Some("name IN (47, 46)"),
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::NotIn,
                vec![Datum::date_from_str(value)?, Datum::date_from_str(another)?],
            ),
            &fixture,
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_date_year_negative_lower_bound() -> Result<()> {
        // 0 => 1970
        let value = "1970-01-01";
        // -1 => 1969
        let another = "1969-12-31";

        let fixture = TestProjectionParameter::new(
            Transform::Year,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Date)),
        );

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::date_from_str(value)?),
            &fixture,
            Some("name <= 0"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::date_from_str(value)?,
            ),
            &fixture,
            Some("name <= 0"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::date_from_str(value)?),
            &fixture,
            Some("name >= 0"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::date_from_str(value)?,
            ),
            &fixture,
            Some("name >= 0"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::date_from_str(value)?),
            &fixture,
            Some("name = 0"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::date_from_str(value)?),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::In,
                vec![Datum::date_from_str(value)?, Datum::date_from_str(another)?],
            ),
            &fixture,
            Some("name IN (0, -1)"),
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::NotIn,
                vec![Datum::date_from_str(value)?, Datum::date_from_str(another)?],
            ),
            &fixture,
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_date_year_lower_bound() -> Result<()> {
        // 47 => 2017
        let value = "2017-01-01";
        // 46 => 2016
        let another = "2016-12-31";

        let fixture = TestProjectionParameter::new(
            Transform::Year,
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Date)),
        );

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::date_from_str(value)?),
            &fixture,
            Some("name <= 46"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::date_from_str(value)?,
            ),
            &fixture,
            Some("name <= 47"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::date_from_str(value)?),
            &fixture,
            Some("name >= 47"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::date_from_str(value)?,
            ),
            &fixture,
            Some("name >= 47"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::date_from_str(value)?),
            &fixture,
            Some("name = 47"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::date_from_str(value)?),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::In,
                vec![Datum::date_from_str(value)?, Datum::date_from_str(another)?],
            ),
            &fixture,
            Some("name IN (47, 46)"),
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::NotIn,
                vec![Datum::date_from_str(value)?, Datum::date_from_str(another)?],
            ),
            &fixture,
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_truncate_string_rewrite_op() -> Result<()> {
        let fixture = TestProjectionParameter::new(
            Transform::Truncate(5),
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::String)),
        );

        let value = "abcde";
        assert_projection(
            &fixture.binary_predicate(PredicateOperator::StartsWith, Datum::string(value)),
            &fixture,
            Some(r#"name = "abcde""#),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotStartsWith, Datum::string(value)),
            &fixture,
            Some(r#"name != "abcde""#),
        )?;

        let value = "abcdefg";
        assert_projection(
            &fixture.binary_predicate(PredicateOperator::StartsWith, Datum::string(value)),
            &fixture,
            Some(r#"name STARTS WITH "abcde""#),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotStartsWith, Datum::string(value)),
            &fixture,
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_truncate_string() -> Result<()> {
        let value = "abcdefg";
        let fixture = TestProjectionParameter::new(
            Transform::Truncate(5),
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::String)),
        );

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::string(value)),
            &fixture,
            Some(r#"name <= "abcde""#),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThanOrEq, Datum::string(value)),
            &fixture,
            Some(r#"name <= "abcde""#),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::string(value)),
            &fixture,
            Some(r#"name >= "abcde""#),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThanOrEq, Datum::string(value)),
            &fixture,
            Some(r#"name >= "abcde""#),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::string(value)),
            &fixture,
            Some(r#"name = "abcde""#),
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::In,
                vec![Datum::string(value), Datum::string(format!("{}abc", value))],
            ),
            &fixture,
            Some(r#"name IN ("abcde")"#),
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::NotIn,
                vec![Datum::string(value), Datum::string(format!("{}abc", value))],
            ),
            &fixture,
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_truncate_upper_bound_decimal() -> Result<()> {
        let prev = "98.99";
        let curr = "99.99";
        let next = "100.99";

        let fixture = TestProjectionParameter::new(
            Transform::Truncate(10),
            "name",
            NestedField::required(
                1,
                "value",
                Type::Primitive(PrimitiveType::Decimal {
                    precision: 9,
                    scale: 2,
                }),
            ),
        );

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::decimal_from_str(curr)?),
            &fixture,
            Some("name <= 9990"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::decimal_from_str(curr)?,
            ),
            &fixture,
            Some("name <= 9990"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::decimal_from_str(curr)?,
            ),
            &fixture,
            Some("name >= 9990"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::decimal_from_str(curr)?),
            &fixture,
            Some("name = 9990"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::decimal_from_str(curr)?),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::In,
                vec![
                    Datum::decimal_from_str(prev)?,
                    Datum::decimal_from_str(curr)?,
                    Datum::decimal_from_str(next)?,
                ],
            ),
            &fixture,
            Some("name IN (10090, 9990, 9890)"),
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::NotIn,
                vec![
                    Datum::decimal_from_str(curr)?,
                    Datum::decimal_from_str(next)?,
                ],
            ),
            &fixture,
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_truncate_lower_bound_decimal() -> Result<()> {
        let prev = "99.00";
        let curr = "100.00";
        let next = "101.00";

        let fixture = TestProjectionParameter::new(
            Transform::Truncate(10),
            "name",
            NestedField::required(
                1,
                "value",
                Type::Primitive(PrimitiveType::Decimal {
                    precision: 9,
                    scale: 2,
                }),
            ),
        );

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::decimal_from_str(curr)?),
            &fixture,
            Some("name <= 9990"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::decimal_from_str(curr)?,
            ),
            &fixture,
            Some("name <= 10000"),
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::decimal_from_str(curr)?,
            ),
            &fixture,
            Some("name >= 10000"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::decimal_from_str(curr)?),
            &fixture,
            Some("name = 10000"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::decimal_from_str(curr)?),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::In,
                vec![
                    Datum::decimal_from_str(prev)?,
                    Datum::decimal_from_str(curr)?,
                    Datum::decimal_from_str(next)?,
                ],
            ),
            &fixture,
            Some("name IN (9900, 10000, 10100)"),
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::NotIn,
                vec![
                    Datum::decimal_from_str(curr)?,
                    Datum::decimal_from_str(next)?,
                ],
            ),
            &fixture,
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_truncate_upper_bound_long() -> Result<()> {
        let value = 99i64;

        let fixture = TestProjectionParameter::new(
            Transform::Truncate(10),
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Long)),
        );

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::long(value)),
            &fixture,
            Some("name <= 90"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThanOrEq, Datum::long(value)),
            &fixture,
            Some("name <= 90"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThanOrEq, Datum::long(value)),
            &fixture,
            Some("name >= 90"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::long(value)),
            &fixture,
            Some("name = 90"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::long(value)),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::In,
                vec![
                    Datum::long(value - 1),
                    Datum::long(value),
                    Datum::long(value + 1),
                ],
            ),
            &fixture,
            Some("name IN (100, 90)"),
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::NotIn,
                vec![Datum::long(value), Datum::long(value + 1)],
            ),
            &fixture,
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_truncate_lower_bound_long() -> Result<()> {
        let value = 100i64;

        let fixture = TestProjectionParameter::new(
            Transform::Truncate(10),
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Long)),
        );

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::long(value)),
            &fixture,
            Some("name <= 90"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThanOrEq, Datum::long(value)),
            &fixture,
            Some("name <= 100"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThanOrEq, Datum::long(value)),
            &fixture,
            Some("name >= 100"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::long(value)),
            &fixture,
            Some("name = 100"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::long(value)),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::In,
                vec![
                    Datum::long(value - 1),
                    Datum::long(value),
                    Datum::long(value + 1),
                ],
            ),
            &fixture,
            Some("name IN (100, 90)"),
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::NotIn,
                vec![Datum::long(value), Datum::long(value + 1)],
            ),
            &fixture,
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_truncate_upper_bound_integer() -> Result<()> {
        let value = 99;

        let fixture = TestProjectionParameter::new(
            Transform::Truncate(10),
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Int)),
        );

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::int(value)),
            &fixture,
            Some("name <= 90"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThanOrEq, Datum::int(value)),
            &fixture,
            Some("name <= 90"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThanOrEq, Datum::int(value)),
            &fixture,
            Some("name >= 90"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::int(value)),
            &fixture,
            Some("name = 90"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::int(value)),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::In,
                vec![
                    Datum::int(value - 1),
                    Datum::int(value),
                    Datum::int(value + 1),
                ],
            ),
            &fixture,
            Some("name IN (100, 90)"),
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::NotIn,
                vec![Datum::int(value), Datum::int(value + 1)],
            ),
            &fixture,
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_truncate_lower_bound_integer() -> Result<()> {
        let value = 100;

        let fixture = TestProjectionParameter::new(
            Transform::Truncate(10),
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Int)),
        );

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::int(value)),
            &fixture,
            Some("name <= 90"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThanOrEq, Datum::int(value)),
            &fixture,
            Some("name <= 100"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThanOrEq, Datum::int(value)),
            &fixture,
            Some("name >= 100"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::int(value)),
            &fixture,
            Some("name = 100"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::int(value)),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::In,
                vec![
                    Datum::int(value - 1),
                    Datum::int(value),
                    Datum::int(value + 1),
                ],
            ),
            &fixture,
            Some("name IN (100, 90)"),
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::NotIn,
                vec![Datum::int(value), Datum::int(value + 1)],
            ),
            &fixture,
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_bucket_uuid() -> Result<()> {
        let value = uuid::Uuid::from_u64_pair(123, 456);
        let another = uuid::Uuid::from_u64_pair(456, 123);

        let fixture = TestProjectionParameter::new(
            Transform::Bucket(10),
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Uuid)),
        );

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::uuid(value)),
            &fixture,
            Some("name = 4"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::uuid(value)),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::uuid(value)),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThanOrEq, Datum::uuid(value)),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::uuid(value)),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThanOrEq, Datum::uuid(value)),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::In,
                vec![Datum::uuid(value), Datum::uuid(another)],
            ),
            &fixture,
            Some("name IN (4, 6)"),
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::NotIn,
                vec![Datum::uuid(value), Datum::uuid(another)],
            ),
            &fixture,
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_bucket_fixed() -> Result<()> {
        let value = "abcdefg".as_bytes().to_vec();
        let another = "abcdehij".as_bytes().to_vec();

        let fixture = TestProjectionParameter::new(
            Transform::Bucket(10),
            "name",
            NestedField::required(
                1,
                "value",
                Type::Primitive(PrimitiveType::Fixed(value.len() as u64)),
            ),
        );

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::fixed(value.clone())),
            &fixture,
            Some("name = 4"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::fixed(value.clone())),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::fixed(value.clone())),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThanOrEq, Datum::fixed(value.clone())),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::fixed(value.clone())),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::fixed(value.clone()),
            ),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::In,
                vec![Datum::fixed(value.clone()), Datum::fixed(another.clone())],
            ),
            &fixture,
            Some("name IN (4, 6)"),
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::NotIn,
                vec![Datum::fixed(value.clone()), Datum::fixed(another.clone())],
            ),
            &fixture,
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_bucket_string() -> Result<()> {
        let value = "abcdefg";
        let another = "abcdefgabc";

        let fixture = TestProjectionParameter::new(
            Transform::Bucket(10),
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::String)),
        );

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::string(value)),
            &fixture,
            Some("name = 4"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::string(value)),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::string(value)),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThanOrEq, Datum::string(value)),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::string(value)),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThanOrEq, Datum::string(value)),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::In,
                vec![Datum::string(value), Datum::string(another)],
            ),
            &fixture,
            Some("name IN (9, 4)"),
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::NotIn,
                vec![Datum::string(value), Datum::string(another)],
            ),
            &fixture,
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_bucket_decimal() -> Result<()> {
        let prev = "99.00";
        let curr = "100.00";
        let next = "101.00";

        let fixture = TestProjectionParameter::new(
            Transform::Bucket(10),
            "name",
            NestedField::required(
                1,
                "value",
                Type::Primitive(PrimitiveType::Decimal {
                    precision: 9,
                    scale: 2,
                }),
            ),
        );

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::decimal_from_str(curr)?),
            &fixture,
            Some("name = 2"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::decimal_from_str(curr)?),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::decimal_from_str(curr)?),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::decimal_from_str(curr)?,
            ),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::decimal_from_str(curr)?,
            ),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::decimal_from_str(curr)?,
            ),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::In,
                vec![
                    Datum::decimal_from_str(next)?,
                    Datum::decimal_from_str(curr)?,
                    Datum::decimal_from_str(prev)?,
                ],
            ),
            &fixture,
            Some("name IN (6, 2)"),
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::NotIn,
                vec![
                    Datum::decimal_from_str(curr)?,
                    Datum::decimal_from_str(next)?,
                ],
            ),
            &fixture,
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_bucket_long() -> Result<()> {
        let value = 100;
        let fixture = TestProjectionParameter::new(
            Transform::Bucket(10),
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Long)),
        );

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::long(value)),
            &fixture,
            Some("name = 6"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::long(value)),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::long(value)),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThanOrEq, Datum::long(value)),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::long(value)),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThanOrEq, Datum::long(value)),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::In,
                vec![
                    Datum::long(value - 1),
                    Datum::long(value),
                    Datum::long(value + 1),
                ],
            ),
            &fixture,
            Some("name IN (8, 7, 6)"),
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::NotIn,
                vec![Datum::long(value), Datum::long(value + 1)],
            ),
            &fixture,
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_bucket_integer() -> Result<()> {
        let value = 100;
        let fixture = TestProjectionParameter::new(
            Transform::Bucket(10),
            "name",
            NestedField::required(1, "value", Type::Primitive(PrimitiveType::Int)),
        );

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::int(value)),
            &fixture,
            Some("name = 6"),
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::int(value)),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::int(value)),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThanOrEq, Datum::int(value)),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::int(value)),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThanOrEq, Datum::int(value)),
            &fixture,
            None,
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::In,
                vec![
                    Datum::int(value - 1),
                    Datum::int(value),
                    Datum::int(value + 1),
                ],
            ),
            &fixture,
            Some("name IN (8, 7, 6)"),
        )?;

        assert_projection(
            &fixture.set_predicate(
                PredicateOperator::NotIn,
                vec![Datum::int(value), Datum::int(value + 1)],
            ),
            &fixture,
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_bucket_transform() {
        let trans = Transform::Bucket(8);

        let test_param = TestParameter {
            display: "bucket[8]".to_string(),
            json: r#""bucket[8]""#.to_string(),
            dedup_name: "bucket[8]".to_string(),
            preserves_order: false,
            satisfies_order_of: vec![
                (Transform::Bucket(8), true),
                (Transform::Bucket(4), false),
                (Transform::Void, false),
                (Transform::Day, false),
            ],
            trans_types: vec![
                (Primitive(Binary), Some(Primitive(Int))),
                (Primitive(Date), Some(Primitive(Int))),
                (
                    Primitive(Decimal {
                        precision: 8,
                        scale: 5,
                    }),
                    Some(Primitive(Int)),
                ),
                (Primitive(Fixed(8)), Some(Primitive(Int))),
                (Primitive(Int), Some(Primitive(Int))),
                (Primitive(Long), Some(Primitive(Int))),
                (Primitive(StringType), Some(Primitive(Int))),
                (Primitive(Uuid), Some(Primitive(Int))),
                (Primitive(Time), Some(Primitive(Int))),
                (Primitive(Timestamp), Some(Primitive(Int))),
                (Primitive(Timestamptz), Some(Primitive(Int))),
                (
                    Struct(StructType::new(vec![NestedField::optional(
                        1,
                        "a",
                        Primitive(Timestamp),
                    )
                    .into()])),
                    None,
                ),
            ],
        };

        check_transform(trans, test_param);
    }

    #[test]
    fn test_truncate_transform() {
        let trans = Transform::Truncate(4);

        let test_param = TestParameter {
            display: "truncate[4]".to_string(),
            json: r#""truncate[4]""#.to_string(),
            dedup_name: "truncate[4]".to_string(),
            preserves_order: true,
            satisfies_order_of: vec![
                (Transform::Truncate(4), true),
                (Transform::Truncate(2), false),
                (Transform::Bucket(4), false),
                (Transform::Void, false),
                (Transform::Day, false),
            ],
            trans_types: vec![
                (Primitive(Binary), Some(Primitive(Binary))),
                (Primitive(Date), None),
                (
                    Primitive(Decimal {
                        precision: 8,
                        scale: 5,
                    }),
                    Some(Primitive(Decimal {
                        precision: 8,
                        scale: 5,
                    })),
                ),
                (Primitive(Fixed(8)), None),
                (Primitive(Int), Some(Primitive(Int))),
                (Primitive(Long), Some(Primitive(Long))),
                (Primitive(StringType), Some(Primitive(StringType))),
                (Primitive(Uuid), None),
                (Primitive(Time), None),
                (Primitive(Timestamp), None),
                (Primitive(Timestamptz), None),
                (
                    Struct(StructType::new(vec![NestedField::optional(
                        1,
                        "a",
                        Primitive(Timestamp),
                    )
                    .into()])),
                    None,
                ),
            ],
        };

        check_transform(trans, test_param);
    }

    #[test]
    fn test_identity_transform() {
        let trans = Transform::Identity;

        let test_param = TestParameter {
            display: "identity".to_string(),
            json: r#""identity""#.to_string(),
            dedup_name: "identity".to_string(),
            preserves_order: true,
            satisfies_order_of: vec![
                (Transform::Truncate(4), true),
                (Transform::Truncate(2), true),
                (Transform::Bucket(4), false),
                (Transform::Void, false),
                (Transform::Day, true),
            ],
            trans_types: vec![
                (Primitive(Binary), Some(Primitive(Binary))),
                (Primitive(Date), Some(Primitive(Date))),
                (
                    Primitive(Decimal {
                        precision: 8,
                        scale: 5,
                    }),
                    Some(Primitive(Decimal {
                        precision: 8,
                        scale: 5,
                    })),
                ),
                (Primitive(Fixed(8)), Some(Primitive(Fixed(8)))),
                (Primitive(Int), Some(Primitive(Int))),
                (Primitive(Long), Some(Primitive(Long))),
                (Primitive(StringType), Some(Primitive(StringType))),
                (Primitive(Uuid), Some(Primitive(Uuid))),
                (Primitive(Time), Some(Primitive(Time))),
                (Primitive(Timestamp), Some(Primitive(Timestamp))),
                (Primitive(Timestamptz), Some(Primitive(Timestamptz))),
                (
                    Struct(StructType::new(vec![NestedField::optional(
                        1,
                        "a",
                        Primitive(Timestamp),
                    )
                    .into()])),
                    None,
                ),
            ],
        };

        check_transform(trans, test_param);
    }

    #[test]
    fn test_year_transform() {
        let trans = Transform::Year;

        let test_param = TestParameter {
            display: "year".to_string(),
            json: r#""year""#.to_string(),
            dedup_name: "time".to_string(),
            preserves_order: true,
            satisfies_order_of: vec![
                (Transform::Year, true),
                (Transform::Month, false),
                (Transform::Day, false),
                (Transform::Hour, false),
                (Transform::Void, false),
                (Transform::Identity, false),
            ],
            trans_types: vec![
                (Primitive(Binary), None),
                (Primitive(Date), Some(Primitive(Int))),
                (
                    Primitive(Decimal {
                        precision: 8,
                        scale: 5,
                    }),
                    None,
                ),
                (Primitive(Fixed(8)), None),
                (Primitive(Int), None),
                (Primitive(Long), None),
                (Primitive(StringType), None),
                (Primitive(Uuid), None),
                (Primitive(Time), None),
                (Primitive(Timestamp), Some(Primitive(Int))),
                (Primitive(Timestamptz), Some(Primitive(Int))),
                (
                    Struct(StructType::new(vec![NestedField::optional(
                        1,
                        "a",
                        Primitive(Timestamp),
                    )
                    .into()])),
                    None,
                ),
            ],
        };

        check_transform(trans, test_param);
    }

    #[test]
    fn test_month_transform() {
        let trans = Transform::Month;

        let test_param = TestParameter {
            display: "month".to_string(),
            json: r#""month""#.to_string(),
            dedup_name: "time".to_string(),
            preserves_order: true,
            satisfies_order_of: vec![
                (Transform::Year, true),
                (Transform::Month, true),
                (Transform::Day, false),
                (Transform::Hour, false),
                (Transform::Void, false),
                (Transform::Identity, false),
            ],
            trans_types: vec![
                (Primitive(Binary), None),
                (Primitive(Date), Some(Primitive(Int))),
                (
                    Primitive(Decimal {
                        precision: 8,
                        scale: 5,
                    }),
                    None,
                ),
                (Primitive(Fixed(8)), None),
                (Primitive(Int), None),
                (Primitive(Long), None),
                (Primitive(StringType), None),
                (Primitive(Uuid), None),
                (Primitive(Time), None),
                (Primitive(Timestamp), Some(Primitive(Int))),
                (Primitive(Timestamptz), Some(Primitive(Int))),
                (
                    Struct(StructType::new(vec![NestedField::optional(
                        1,
                        "a",
                        Primitive(Timestamp),
                    )
                    .into()])),
                    None,
                ),
            ],
        };

        check_transform(trans, test_param);
    }

    #[test]
    fn test_day_transform() {
        let trans = Transform::Day;

        let test_param = TestParameter {
            display: "day".to_string(),
            json: r#""day""#.to_string(),
            dedup_name: "time".to_string(),
            preserves_order: true,
            satisfies_order_of: vec![
                (Transform::Year, true),
                (Transform::Month, true),
                (Transform::Day, true),
                (Transform::Hour, false),
                (Transform::Void, false),
                (Transform::Identity, false),
            ],
            trans_types: vec![
                (Primitive(Binary), None),
                (Primitive(Date), Some(Primitive(Int))),
                (
                    Primitive(Decimal {
                        precision: 8,
                        scale: 5,
                    }),
                    None,
                ),
                (Primitive(Fixed(8)), None),
                (Primitive(Int), None),
                (Primitive(Long), None),
                (Primitive(StringType), None),
                (Primitive(Uuid), None),
                (Primitive(Time), None),
                (Primitive(Timestamp), Some(Primitive(Int))),
                (Primitive(Timestamptz), Some(Primitive(Int))),
                (
                    Struct(StructType::new(vec![NestedField::optional(
                        1,
                        "a",
                        Primitive(Timestamp),
                    )
                    .into()])),
                    None,
                ),
            ],
        };

        check_transform(trans, test_param);
    }

    #[test]
    fn test_hour_transform() {
        let trans = Transform::Hour;

        let test_param = TestParameter {
            display: "hour".to_string(),
            json: r#""hour""#.to_string(),
            dedup_name: "time".to_string(),
            preserves_order: true,
            satisfies_order_of: vec![
                (Transform::Year, true),
                (Transform::Month, true),
                (Transform::Day, true),
                (Transform::Hour, true),
                (Transform::Void, false),
                (Transform::Identity, false),
            ],
            trans_types: vec![
                (Primitive(Binary), None),
                (Primitive(Date), None),
                (
                    Primitive(Decimal {
                        precision: 8,
                        scale: 5,
                    }),
                    None,
                ),
                (Primitive(Fixed(8)), None),
                (Primitive(Int), None),
                (Primitive(Long), None),
                (Primitive(StringType), None),
                (Primitive(Uuid), None),
                (Primitive(Time), None),
                (Primitive(Timestamp), Some(Primitive(Int))),
                (Primitive(Timestamptz), Some(Primitive(Int))),
                (
                    Struct(StructType::new(vec![NestedField::optional(
                        1,
                        "a",
                        Primitive(Timestamp),
                    )
                    .into()])),
                    None,
                ),
            ],
        };

        check_transform(trans, test_param);
    }

    #[test]
    fn test_void_transform() {
        let trans = Transform::Void;

        let test_param = TestParameter {
            display: "void".to_string(),
            json: r#""void""#.to_string(),
            dedup_name: "void".to_string(),
            preserves_order: false,
            satisfies_order_of: vec![
                (Transform::Year, false),
                (Transform::Month, false),
                (Transform::Day, false),
                (Transform::Hour, false),
                (Transform::Void, true),
                (Transform::Identity, false),
            ],
            trans_types: vec![
                (Primitive(Binary), Some(Primitive(Binary))),
                (Primitive(Date), Some(Primitive(Date))),
                (
                    Primitive(Decimal {
                        precision: 8,
                        scale: 5,
                    }),
                    Some(Primitive(Decimal {
                        precision: 8,
                        scale: 5,
                    })),
                ),
                (Primitive(Fixed(8)), Some(Primitive(Fixed(8)))),
                (Primitive(Int), Some(Primitive(Int))),
                (Primitive(Long), Some(Primitive(Long))),
                (Primitive(StringType), Some(Primitive(StringType))),
                (Primitive(Uuid), Some(Primitive(Uuid))),
                (Primitive(Time), Some(Primitive(Time))),
                (Primitive(Timestamp), Some(Primitive(Timestamp))),
                (Primitive(Timestamptz), Some(Primitive(Timestamptz))),
                (
                    Struct(StructType::new(vec![NestedField::optional(
                        1,
                        "a",
                        Primitive(Timestamp),
                    )
                    .into()])),
                    Some(Struct(StructType::new(vec![NestedField::optional(
                        1,
                        "a",
                        Primitive(Timestamp),
                    )
                    .into()]))),
                ),
            ],
        };

        check_transform(trans, test_param);
    }

    #[test]
    fn test_known_transform() {
        let trans = Transform::Unknown;

        let test_param = TestParameter {
            display: "unknown".to_string(),
            json: r#""unknown""#.to_string(),
            dedup_name: "unknown".to_string(),
            preserves_order: false,
            satisfies_order_of: vec![
                (Transform::Year, false),
                (Transform::Month, false),
                (Transform::Day, false),
                (Transform::Hour, false),
                (Transform::Void, false),
                (Transform::Identity, false),
                (Transform::Unknown, true),
            ],
            trans_types: vec![
                (Primitive(Binary), Some(Primitive(StringType))),
                (Primitive(Date), Some(Primitive(StringType))),
                (
                    Primitive(Decimal {
                        precision: 8,
                        scale: 5,
                    }),
                    Some(Primitive(StringType)),
                ),
                (Primitive(Fixed(8)), Some(Primitive(StringType))),
                (Primitive(Int), Some(Primitive(StringType))),
                (Primitive(Long), Some(Primitive(StringType))),
                (Primitive(StringType), Some(Primitive(StringType))),
                (Primitive(Uuid), Some(Primitive(StringType))),
                (Primitive(Time), Some(Primitive(StringType))),
                (Primitive(Timestamp), Some(Primitive(StringType))),
                (Primitive(Timestamptz), Some(Primitive(StringType))),
                (
                    Struct(StructType::new(vec![NestedField::optional(
                        1,
                        "a",
                        Primitive(Timestamp),
                    )
                    .into()])),
                    Some(Primitive(StringType)),
                ),
            ],
        };

        check_transform(trans, test_param);
    }
}
