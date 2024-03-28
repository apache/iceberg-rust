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

        let projection = match self {
            Transform::Bucket(_) => match predicate {
                BoundPredicate::Unary(expr) => Some(Predicate::Unary(UnaryExpression::new(
                    expr.op(),
                    Reference::new(name),
                ))),
                BoundPredicate::Binary(expr) => {
                    if expr.op() != PredicateOperator::Eq
                        || self.can_transform(expr.literal()).is_err()
                    {
                        return Ok(None);
                    }

                    Some(Predicate::Binary(BinaryExpression::new(
                        expr.op(),
                        Reference::new(name),
                        func.transform_literal_result(expr.literal())?,
                    )))
                }
                BoundPredicate::Set(expr) => {
                    if expr.op() != PredicateOperator::In {
                        return Ok(None);
                    }

                    Some(Predicate::Set(SetExpression::new(
                        expr.op(),
                        Reference::new(name),
                        self.apply_transform_on_set(expr.literals(), &func)?,
                    )))
                }
                _ => None,
            },
            Transform::Identity => match predicate {
                BoundPredicate::Unary(expr) => Some(Predicate::Unary(UnaryExpression::new(
                    expr.op(),
                    Reference::new(name),
                ))),
                BoundPredicate::Binary(expr) => Some(Predicate::Binary(BinaryExpression::new(
                    expr.op(),
                    Reference::new(name),
                    expr.literal().to_owned(),
                ))),
                BoundPredicate::Set(expr) => Some(Predicate::Set(SetExpression::new(
                    expr.op(),
                    Reference::new(name),
                    expr.literals().to_owned(),
                ))),
                _ => None,
            },
            Transform::Truncate(_) => match predicate {
                BoundPredicate::Unary(expr) => Some(Predicate::Unary(UnaryExpression::new(
                    expr.op(),
                    Reference::new(name),
                ))),
                BoundPredicate::Binary(expr) => {
                    if self.can_transform(expr.literal()).is_err() {
                        return Ok(None);
                    }

                    let op = expr.op();
                    let datum = expr.literal();
                    self.apply_transform_boundary(name, datum, &op, &func)?
                }
                BoundPredicate::Set(expr) => {
                    if expr.op() != PredicateOperator::In {
                        return Ok(None);
                    }

                    Some(Predicate::Set(SetExpression::new(
                        expr.op(),
                        Reference::new(name),
                        self.apply_transform_on_set(expr.literals(), &func)?,
                    )))
                }
                _ => None,
            },
            Transform::Year | Transform::Month | Transform::Day => match predicate {
                BoundPredicate::Unary(expr) => Some(Predicate::Unary(UnaryExpression::new(
                    expr.op(),
                    Reference::new(name),
                ))),
                BoundPredicate::Binary(expr) => {
                    let op = expr.op();
                    let datum = expr.literal();
                    self.apply_transform_boundary(name, datum, &op, &func)?
                }
                BoundPredicate::Set(expr) => {
                    if expr.op() != PredicateOperator::In {
                        return Ok(None);
                    }

                    Some(Predicate::Set(SetExpression::new(
                        expr.op(),
                        Reference::new(name),
                        self.apply_transform_on_set(expr.literals(), &func)?,
                    )))
                }
                _ => None,
            },
            _ => None,
        };

        Ok(projection)
    }

    /// Check if `Transform` is applicable on datum's `PrimitiveType`
    fn can_transform(&self, datum: &Datum) -> Result<()> {
        let input_type = datum.data_type().clone();
        self.result_type(&Type::Primitive(input_type))?;

        Ok(())
    }

    /// Transform each literal value of `FnvHashSet<Datum>`
    fn apply_transform_on_set(
        &self,
        literals: &FnvHashSet<Datum>,
        func: &BoxedTransformFunction,
    ) -> Result<FnvHashSet<Datum>> {
        literals
            .iter()
            .try_fold(FnvHashSet::default(), |mut acc, d| {
                self.can_transform(d)?;
                acc.insert(func.transform_literal_result(d)?);
                Ok(acc)
            })
    }

    /// Apply truncate transform on `Datum` with new boundaries
    /// and adjusted `PredicateOperator`
    fn apply_transform_boundary(
        &self,
        name: String,
        datum: &Datum,
        op: &PredicateOperator,
        func: &BoxedTransformFunction,
    ) -> Result<Option<Predicate>> {
        let boundary = self.datum_with_boundary(op, datum)?;

        match boundary {
            None => Ok(None),
            Some(boundary) => {
                let new_op = match op {
                    PredicateOperator::LessThan => PredicateOperator::LessThanOrEq,
                    PredicateOperator::GreaterThan => PredicateOperator::GreaterThanOrEq,
                    _ => *op,
                };

                Ok(Some(Predicate::Binary(BinaryExpression::new(
                    new_op,
                    Reference::new(name),
                    func.transform_literal_result(&boundary)?,
                ))))
            }
        }
    }

    /// Create a new `Datum` with adjusted boundary for projection
    fn datum_with_boundary(&self, op: &PredicateOperator, datum: &Datum) -> Result<Option<Datum>> {
        let literal = datum.literal();

        let adj_datum = match op {
            PredicateOperator::LessThan => match literal {
                PrimitiveLiteral::Int(v) => Some(Datum::int(v - 1)),
                PrimitiveLiteral::Long(v) => Some(Datum::long(v - 1)),
                PrimitiveLiteral::Decimal(v) => Some(Datum::decimal(v - 1)?),
                PrimitiveLiteral::Fixed(v) => Some(Datum::fixed(v.clone())),
                PrimitiveLiteral::Date(v) => Some(Datum::date(v - 1)),
                _ => None,
            },
            PredicateOperator::GreaterThan => match literal {
                PrimitiveLiteral::Int(v) => Some(Datum::int(v + 1)),
                PrimitiveLiteral::Long(v) => Some(Datum::long(v + 1)),
                PrimitiveLiteral::Decimal(v) => Some(Datum::decimal(v + 1)?),
                PrimitiveLiteral::Fixed(v) => Some(Datum::fixed(v.clone())),
                PrimitiveLiteral::Date(v) => Some(Datum::date(v + 1)),
                _ => None,
            },
            PredicateOperator::Eq
            | PredicateOperator::LessThanOrEq
            | PredicateOperator::GreaterThanOrEq => match literal {
                PrimitiveLiteral::Int(v) => Some(Datum::int(*v)),
                PrimitiveLiteral::Long(v) => Some(Datum::long(*v)),
                PrimitiveLiteral::Decimal(v) => Some(Datum::decimal(*v)?),
                PrimitiveLiteral::Fixed(v) => Some(Datum::fixed(v.clone())),
                PrimitiveLiteral::Date(v) => Some(Datum::date(*v)),
                _ => None,
            },
            PredicateOperator::StartsWith => match literal {
                PrimitiveLiteral::Fixed(v) => Some(Datum::fixed(v.clone())),
                _ => None,
            },
            _ => None,
        };

        Ok(adj_datum)
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

impl Datum {}

#[cfg(test)]
mod tests {
    use fnv::FnvHashSet;

    use super::*;
    use std::sync::Arc;

    use crate::expr::{BoundPredicate, BoundReference, PredicateOperator, UnaryExpression};
    use crate::spec::datatypes::PrimitiveType::{
        Binary, Date, Decimal, Fixed, Int, Long, String as StringType, Time, Timestamp,
        Timestamptz, Uuid,
    };
    use crate::spec::datatypes::Type::{Primitive, Struct};
    use crate::spec::datatypes::{NestedField, StructType, Type};
    use crate::spec::transform::Transform;
    use crate::spec::{Datum, PrimitiveType};

    struct TestParameter {
        display: String,
        json: String,
        dedup_name: String,
        preserves_order: bool,
        satisfies_order_of: Vec<(Transform, bool)>,
        trans_types: Vec<(Type, Option<Type>)>,
    }

    struct TestPredicates {
        unary: BoundPredicate,
        binary: BoundPredicate,
        set: BoundPredicate,
    }

    impl TestPredicates {
        fn new() -> Self {
            let field = Arc::new(NestedField::required(
                1,
                "a",
                Type::Primitive(PrimitiveType::Int),
            ));

            let unary = BoundPredicate::Unary(UnaryExpression::new(
                PredicateOperator::IsNull,
                BoundReference::new("original_name", field.clone()),
            ));
            let binary = BoundPredicate::Binary(BinaryExpression::new(
                PredicateOperator::Eq,
                BoundReference::new("original_name", field.clone()),
                Datum::int(5),
            ));
            let set = BoundPredicate::Set(SetExpression::new(
                PredicateOperator::In,
                BoundReference::new("original_name", field.clone()),
                FnvHashSet::from_iter([Datum::int(5), Datum::int(6)]),
            ));

            TestPredicates { unary, binary, set }
        }
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
    fn test_projection_dates_year() -> Result<()> {
        let name = "projected_name".to_string();

        let field = Arc::new(NestedField::required(
            1,
            "date",
            Type::Primitive(PrimitiveType::Date),
        ));

        let predicate = BoundPredicate::Binary(BinaryExpression::new(
            PredicateOperator::LessThan,
            BoundReference::new("date", field),
            Datum::date_from_str("1971-01-01")?,
        ));

        let transform = Transform::Year;

        let result = transform.project(name.clone(), &predicate)?.unwrap();

        assert_eq!(format!("{}", result), "projected_name <= 0");

        Ok(())
    }

    #[test]
    fn test_void_projection() -> Result<()> {
        let name = "projected_name".to_string();
        let preds = TestPredicates::new();

        let transform = Transform::Void;
        let result_unary = transform.project(name.clone(), &preds.unary)?;
        assert!(result_unary.is_none());

        Ok(())
    }

    #[test]
    fn test_truncate_project() -> Result<()> {
        let name = "projected_name".to_string();
        let preds = TestPredicates::new();

        let transform = Transform::Truncate(10);

        let result_unary = transform.project(name.clone(), &preds.unary)?.unwrap();
        let result_binary = transform.project(name.clone(), &preds.binary)?.unwrap();
        let result_set = transform.project(name.clone(), &preds.set)?.unwrap();

        assert_eq!(format!("{}", result_unary), "projected_name IS NULL");
        assert_eq!(format!("{}", result_binary), "projected_name = 0");
        assert_eq!(format!("{}", result_set), "projected_name IN (0)");

        Ok(())
    }

    #[test]
    fn test_identity_project() -> Result<()> {
        let name = "projected_name".to_string();
        let preds = TestPredicates::new();

        let transform = Transform::Identity;

        let result_unary = transform.project(name.clone(), &preds.unary)?.unwrap();
        let result_binary = transform.project(name.clone(), &preds.binary)?.unwrap();
        let result_set = transform.project(name.clone(), &preds.set)?.unwrap();

        assert_eq!(format!("{}", result_unary), "projected_name IS NULL");
        assert_eq!(format!("{}", result_binary), "projected_name = 5");
        assert_eq!(format!("{}", result_set), "projected_name IN (5, 6)");

        Ok(())
    }

    #[test]
    fn test_bucket_project() -> Result<()> {
        let name = "projected_name".to_string();
        let preds = TestPredicates::new();

        let transform = Transform::Bucket(8);

        let result_unary = transform.project(name.clone(), &preds.unary)?.unwrap();
        let result_binary = transform.project(name.clone(), &preds.binary)?.unwrap();
        let result_set = transform.project(name.clone(), &preds.set)?.unwrap();

        assert_eq!(format!("{}", result_unary), "projected_name IS NULL");
        assert_eq!(format!("{}", result_binary), "projected_name = 7");
        assert_eq!(format!("{}", result_set), "projected_name IN (1, 7)");

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
