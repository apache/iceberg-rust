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

use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

use chrono::{DateTime, Datelike};
use fnv::FnvHashSet;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::values::decimal_utils::decimal_from_i128_with_scale;
use super::values::temporal::date;
use super::{Datum, PrimitiveLiteral};
use crate::ErrorKind;
use crate::error::{Error, Result};
use crate::expr::{
    BinaryExpression, BoundPredicate, BoundReference, Predicate, PredicateOperator, Reference,
    SetExpression, UnaryExpression,
};
use crate::spec::Literal;
use crate::spec::datatypes::{PrimitiveType, Type};
use crate::transform::{BoxedTransformFunction, create_transform_function};

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
#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
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
    /// Returns a human-readable String representation of a transformed value.
    ///
    /// The temporal transforms store an ordinal count since the Unix epoch, and
    /// this method renders that count as a date so that partition paths and
    /// snapshot summary keys match the Java reference implementation:
    ///
    /// | Transform | Format          | Example         |
    /// |-----------|-----------------|-----------------|
    /// | `Year`    | `yyyy`          | `2017`          |
    /// | `Month`   | `yyyy-MM`       | `2017-06`       |
    /// | `Day`     | `yyyy-MM-dd`    | `2017-06-15`    |
    /// | `Hour`    | `yyyy-MM-dd-HH` | `2017-06-15-16` |
    ///
    /// `Void` renders as `null`, as does an absent value for any transform.
    ///
    /// # Example
    ///
    /// ```
    /// use iceberg::spec::{Literal, PrimitiveType, Transform, Type};
    ///
    /// let int = Type::Primitive(PrimitiveType::Int);
    /// let date = Type::Primitive(PrimitiveType::Date);
    ///
    /// // A stored value carries no logical type of its own. For transforms that do
    /// // not format it themselves, the declared field type decides how it renders.
    /// let stored = Literal::int(17332);
    /// assert_eq!(
    ///     Transform::Identity.to_human_string(&int, Some(&stored)),
    ///     "17332"
    /// );
    /// assert_eq!(
    ///     Transform::Identity.to_human_string(&date, Some(&stored)),
    ///     "2017-06-15"
    /// );
    ///
    /// // The temporal transforms format their own ordinal and ignore the declared
    /// // type. All four ordinals below are the same instant, 2017-06-15T16:00:00Z,
    /// // counted at four granularities.
    /// assert_eq!(
    ///     Transform::Year.to_human_string(&int, Some(&Literal::int(47))),
    ///     "2017"
    /// );
    /// assert_eq!(
    ///     Transform::Month.to_human_string(&int, Some(&Literal::int(569))),
    ///     "2017-06"
    /// );
    /// assert_eq!(
    ///     Transform::Day.to_human_string(&int, Some(&Literal::int(17332))),
    ///     "2017-06-15"
    /// );
    /// assert_eq!(
    ///     Transform::Hour.to_human_string(&int, Some(&Literal::int(415984))),
    ///     "2017-06-15-16"
    /// );
    ///
    /// // `Void` and an absent value render as `null`.
    /// assert_eq!(
    ///     Transform::Void.to_human_string(&int, Some(&Literal::int(47))),
    ///     "null"
    /// );
    /// assert_eq!(Transform::Year.to_human_string(&int, None), "null");
    /// ```
    pub fn to_human_string(&self, field_type: &Type, value: Option<&Literal>) -> String {
        let Some(value) = value else {
            return "null".to_string();
        };

        if let Some(value) = value.as_primitive_literal() {
            let field_type = field_type.as_primitive_type().unwrap();
            let datum = Datum::new(field_type.clone(), value);

            match (self, datum.literal()) {
                (Self::Void, _) => "null".to_string(),
                // The temporal transforms produce an ordinal count since the Unix
                // epoch, so `Datum::to_human_string` would render the raw count for
                // `Year`, `Month` and `Hour`, and would leave `Day` dependent on the
                // field type happening to be `date`. The Java reference
                // implementation overrides `toHumanString` on each of these
                // transforms and ignores the declared type, so do the same here.
                // Any other literal falls through to the datum.
                (Self::Year, PrimitiveLiteral::Int(ordinal)) => Self::human_year(*ordinal),
                (Self::Month, PrimitiveLiteral::Int(ordinal)) => Self::human_month(*ordinal),
                (Self::Day, PrimitiveLiteral::Int(ordinal)) => Self::human_day(*ordinal),
                (Self::Hour, PrimitiveLiteral::Int(ordinal)) => Self::human_hour(*ordinal),
                _ => datum.to_human_string(),
            }
        } else {
            "null".to_string()
        }
    }

    /// Formats a year ordinal, the number of years since 1970, as `yyyy`.
    ///
    /// Mirrors `TransformUtil.humanYear` in the Java reference implementation.
    fn human_year(year_ordinal: i32) -> String {
        format!("{:04}", DateTime::UNIX_EPOCH.year() + year_ordinal)
    }

    /// Formats a month ordinal, the number of months since 1970-01, as `yyyy-MM`.
    ///
    /// Mirrors `TransformUtil.humanMonth`, which uses `Math.floorDiv` and
    /// `Math.floorMod` rather than `/` and `%`. Truncating division rounds toward
    /// zero, which is the wrong direction before 1970: ordinal -1 is 1969-12, but
    /// truncating yields 1970-01. `div_euclid` and `rem_euclid` round toward
    /// negative infinity and so match the Java reference implementation.
    fn human_month(month_ordinal: i32) -> String {
        format!(
            "{:04}-{:02}",
            DateTime::UNIX_EPOCH.year() + month_ordinal.div_euclid(12),
            1 + month_ordinal.rem_euclid(12)
        )
    }

    /// Formats a day ordinal, the number of days since 1970-01-01, as `yyyy-MM-dd`.
    ///
    /// Mirrors `TransformUtil.humanDay`. Like the Java `Days` transform, whose
    /// signature is `toHumanString(Type alwaysDate, Integer value)`, this ignores
    /// the declared field type rather than relying on it being `date`.
    fn human_day(day_ordinal: i32) -> String {
        let date = date::days_to_date(day_ordinal);
        format!("{:04}-{:02}-{:02}", date.year(), date.month(), date.day())
    }

    /// Formats an hour ordinal, the number of hours since 1970-01-01T00:00:00Z,
    /// as `yyyy-MM-dd-HH`.
    ///
    /// Mirrors `TransformUtil.humanHour`. `div_euclid` and `rem_euclid` split the
    /// ordinal into whole days and the hour within the day so that hours before
    /// 1970 round the same way they do in the Java reference implementation.
    fn human_hour(hour_ordinal: i32) -> String {
        format!(
            "{}-{:02}",
            Self::human_day(hour_ordinal.div_euclid(24)),
            hour_ordinal.rem_euclid(24)
        )
    }

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
                        | PrimitiveType::TimestampNs
                        | PrimitiveType::TimestamptzNs
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
            Transform::Year | Transform::Month => {
                if let Type::Primitive(p) = input_type {
                    match p {
                        PrimitiveType::Timestamp
                        | PrimitiveType::Timestamptz
                        | PrimitiveType::TimestampNs
                        | PrimitiveType::TimestamptzNs
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
            Transform::Day => {
                if let Type::Primitive(p) = input_type {
                    match p {
                        PrimitiveType::Timestamp
                        | PrimitiveType::Timestamptz
                        | PrimitiveType::TimestampNs
                        | PrimitiveType::TimestamptzNs
                        | PrimitiveType::Date => Ok(Type::Primitive(PrimitiveType::Date)),
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
                        PrimitiveType::Timestamp
                        | PrimitiveType::Timestamptz
                        | PrimitiveType::TimestampNs
                        | PrimitiveType::TimestamptzNs => Ok(Type::Primitive(PrimitiveType::Int)),
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

    /// Strictly projects a given predicate according to the transformation
    /// specified by the `Transform` instance.
    ///
    /// This method ensures that the projected predicate is strictly aligned
    /// with the transformation logic, providing a more precise filtering
    /// mechanism for transformed data.
    ///
    /// # Example
    /// Suppose, we have row filter `a = 10`, and a partition spec
    /// `bucket(a, 37) as bs`, if one row matches `a = 10`, then its partition
    /// value should match `bucket(10, 37) as bs`, and we project `a = 10` to
    /// `bs = bucket(10, 37)`
    pub fn strict_project(
        &self,
        name: &str,
        predicate: &BoundPredicate,
    ) -> Result<Option<Predicate>> {
        let func = create_transform_function(self)?;

        match self {
            Transform::Identity => match predicate {
                BoundPredicate::Unary(expr) => Self::project_unary(expr.op(), name),
                BoundPredicate::Binary(expr) => Ok(Some(Predicate::Binary(BinaryExpression::new(
                    expr.op(),
                    Reference::new(name),
                    expr.literal().to_owned(),
                )))),
                BoundPredicate::Set(expr) => Ok(Some(Predicate::Set(SetExpression::new(
                    expr.op(),
                    Reference::new(name),
                    expr.literals().to_owned(),
                )))),
                _ => Ok(None),
            },
            Transform::Bucket(_) => match predicate {
                BoundPredicate::Unary(expr) => Self::project_unary(expr.op(), name),
                BoundPredicate::Binary(expr) => {
                    self.project_binary_expr(name, PredicateOperator::NotEq, expr, &func)
                }
                BoundPredicate::Set(expr) => {
                    self.project_set_expr(expr, PredicateOperator::NotIn, name, &func)
                }
                _ => Ok(None),
            },
            Transform::Truncate(width) => match predicate {
                BoundPredicate::Unary(expr) => Self::project_unary(expr.op(), name),
                BoundPredicate::Binary(expr) => {
                    if matches!(
                        expr.term().field().field_type.as_primitive_type(),
                        Some(&PrimitiveType::Int)
                            | Some(&PrimitiveType::Long)
                            | Some(&PrimitiveType::Decimal { .. })
                    ) {
                        self.truncate_number_strict(name, expr, &func)
                    } else if expr.op() == PredicateOperator::StartsWith {
                        let len = match expr.literal().literal() {
                            PrimitiveLiteral::String(s) => s.len(),
                            PrimitiveLiteral::Binary(b) => b.len(),
                            _ => {
                                return Err(Error::new(
                                    ErrorKind::DataInvalid,
                                    format!(
                                        "Expected a string or binary literal, got: {:?}",
                                        expr.literal()
                                    ),
                                ));
                            }
                        };
                        match len.cmp(&(*width as usize)) {
                            Ordering::Less => Ok(Some(Predicate::Binary(BinaryExpression::new(
                                PredicateOperator::StartsWith,
                                Reference::new(name),
                                expr.literal().to_owned(),
                            )))),
                            Ordering::Equal => Ok(Some(Predicate::Binary(BinaryExpression::new(
                                PredicateOperator::Eq,
                                Reference::new(name),
                                expr.literal().to_owned(),
                            )))),
                            Ordering::Greater => Ok(None),
                        }
                    } else if expr.op() == PredicateOperator::NotStartsWith {
                        let len = match expr.literal().literal() {
                            PrimitiveLiteral::String(s) => s.len(),
                            PrimitiveLiteral::Binary(b) => b.len(),
                            _ => {
                                return Err(Error::new(
                                    ErrorKind::DataInvalid,
                                    format!(
                                        "Expected a string or binary literal, got: {:?}",
                                        expr.literal()
                                    ),
                                ));
                            }
                        };
                        match len.cmp(&(*width as usize)) {
                            Ordering::Less => Ok(Some(Predicate::Binary(BinaryExpression::new(
                                PredicateOperator::NotStartsWith,
                                Reference::new(name),
                                expr.literal().to_owned(),
                            )))),
                            Ordering::Equal => Ok(Some(Predicate::Binary(BinaryExpression::new(
                                PredicateOperator::NotEq,
                                Reference::new(name),
                                expr.literal().to_owned(),
                            )))),
                            Ordering::Greater => {
                                Ok(Some(Predicate::Binary(BinaryExpression::new(
                                    expr.op(),
                                    Reference::new(name),
                                    func.transform_literal_result(expr.literal())?,
                                ))))
                            }
                        }
                    } else {
                        self.truncate_array_strict(name, expr, &func)
                    }
                }
                BoundPredicate::Set(expr) => {
                    self.project_set_expr(expr, PredicateOperator::NotIn, name, &func)
                }
                _ => Ok(None),
            },
            Transform::Year | Transform::Month | Transform::Day | Transform::Hour => {
                match predicate {
                    BoundPredicate::Unary(expr) => Self::project_unary(expr.op(), name),
                    BoundPredicate::Binary(expr) => self.truncate_number_strict(name, expr, &func),
                    BoundPredicate::Set(expr) => {
                        self.project_set_expr(expr, PredicateOperator::NotIn, name, &func)
                    }
                    _ => Ok(None),
                }
            }
            _ => Ok(None),
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
    pub fn project(&self, name: &str, predicate: &BoundPredicate) -> Result<Option<Predicate>> {
        let func = create_transform_function(self)?;

        match self {
            Transform::Identity => match predicate {
                BoundPredicate::Unary(expr) => Self::project_unary(expr.op(), name),
                BoundPredicate::Binary(expr) => Ok(Some(Predicate::Binary(BinaryExpression::new(
                    expr.op(),
                    Reference::new(name),
                    expr.literal().to_owned(),
                )))),
                BoundPredicate::Set(expr) => Ok(Some(Predicate::Set(SetExpression::new(
                    expr.op(),
                    Reference::new(name),
                    expr.literals().to_owned(),
                )))),
                _ => Ok(None),
            },
            Transform::Bucket(_) => match predicate {
                BoundPredicate::Unary(expr) => Self::project_unary(expr.op(), name),
                BoundPredicate::Binary(expr) => {
                    self.project_binary_expr(name, PredicateOperator::Eq, expr, &func)
                }
                BoundPredicate::Set(expr) => {
                    self.project_set_expr(expr, PredicateOperator::In, name, &func)
                }
                _ => Ok(None),
            },
            Transform::Truncate(width) => match predicate {
                BoundPredicate::Unary(expr) => Self::project_unary(expr.op(), name),
                BoundPredicate::Binary(expr) => {
                    self.project_binary_with_adjusted_boundary(name, expr, &func, Some(*width))
                }
                BoundPredicate::Set(expr) => {
                    self.project_set_expr(expr, PredicateOperator::In, name, &func)
                }
                _ => Ok(None),
            },
            Transform::Year | Transform::Month | Transform::Day | Transform::Hour => {
                match predicate {
                    BoundPredicate::Unary(expr) => Self::project_unary(expr.op(), name),
                    BoundPredicate::Binary(expr) => {
                        self.project_binary_with_adjusted_boundary(name, expr, &func, None)
                    }
                    BoundPredicate::Set(expr) => {
                        self.project_set_expr(expr, PredicateOperator::In, name, &func)
                    }
                    _ => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }

    /// Check if `Transform` is applicable on datum's `PrimitiveType`
    fn can_transform(&self, datum: &Datum) -> bool {
        let input_type = datum.data_type().clone();
        self.result_type(&Type::Primitive(input_type)).is_ok()
    }

    /// Creates a unary predicate from a given operator and a reference name.
    fn project_unary(op: PredicateOperator, name: &str) -> Result<Option<Predicate>> {
        Ok(Some(Predicate::Unary(UnaryExpression::new(
            op,
            Reference::new(name),
        ))))
    }

    /// Attempts to create a binary predicate based on a binary expression,
    /// if applicable.
    ///
    /// This method evaluates a given binary expression and, if the operation
    /// is the given operator and the literal can be transformed, constructs a
    /// `Predicate::Binary`variant representing the binary operation.
    fn project_binary_expr(
        &self,
        name: &str,
        op: PredicateOperator,
        expr: &BinaryExpression<BoundReference>,
        func: &BoxedTransformFunction,
    ) -> Result<Option<Predicate>> {
        if expr.op() != op || !self.can_transform(expr.literal()) {
            return Ok(None);
        }

        Ok(Some(Predicate::Binary(BinaryExpression::new(
            expr.op(),
            Reference::new(name),
            func.transform_literal_result(expr.literal())?,
        ))))
    }

    /// Projects a binary expression to a predicate with an adjusted boundary.
    ///
    /// Checks if the literal within the given binary expression is
    /// transformable. If transformable, it proceeds to potentially adjust
    /// the boundary of the expression based on the comparison operator (`op`).
    /// The potential adjustments involve incrementing or decrementing the
    /// literal value and changing the `PredicateOperator` itself to its
    /// inclusive variant.
    fn project_binary_with_adjusted_boundary(
        &self,
        name: &str,
        expr: &BinaryExpression<BoundReference>,
        func: &BoxedTransformFunction,
        width: Option<u32>,
    ) -> Result<Option<Predicate>> {
        if !self.can_transform(expr.literal()) {
            return Ok(None);
        }

        let op = &expr.op();
        let datum = &expr.literal();

        if let Some(boundary) = Self::adjust_boundary(op, datum)? {
            let transformed_projection = func.transform_literal_result(&boundary)?;

            let adjusted_projection =
                self.adjust_time_projection(op, datum, &transformed_projection);

            let adjusted_operator = Self::adjust_operator(op, datum, width);

            if let Some(op) = adjusted_operator {
                let predicate = match adjusted_projection {
                    None => Predicate::Binary(BinaryExpression::new(
                        op,
                        Reference::new(name),
                        transformed_projection,
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

    /// Projects a set expression to a predicate,
    /// applying a transformation to each literal in the set.
    fn project_set_expr(
        &self,
        expr: &SetExpression<BoundReference>,
        op: PredicateOperator,
        name: &str,
        func: &BoxedTransformFunction,
    ) -> Result<Option<Predicate>> {
        if expr.op() != op || expr.literals().iter().any(|d| !self.can_transform(d)) {
            return Ok(None);
        }

        let mut new_set = FnvHashSet::default();

        for lit in expr.literals() {
            let datum = func.transform_literal_result(lit)?;

            if let Some(AdjustedProjection::Single(d)) =
                self.adjust_time_projection(&op, lit, &datum)
            {
                new_set.insert(d);
            };

            new_set.insert(datum);
        }

        Ok(Some(Predicate::Set(SetExpression::new(
            expr.op(),
            Reference::new(name),
            new_set,
        ))))
    }

    /// Adjusts the boundary value for comparison operations
    /// based on the specified `PredicateOperator` and `Datum`.
    ///
    /// This function modifies the boundary value for certain comparison
    /// operators (`LessThan`, `GreaterThan`) by incrementing or decrementing
    /// the literal value within the given `Datum`. For operators that do not
    /// imply a boundary shift (`Eq`, `LessThanOrEq`, `GreaterThanOrEq`,
    /// `StartsWith`, `NotStartsWith`), the original datum is returned
    /// unmodified.
    fn adjust_boundary(op: &PredicateOperator, datum: &Datum) -> Result<Option<Datum>> {
        let adjusted_boundary = match op {
            PredicateOperator::LessThan => match (datum.data_type(), datum.literal()) {
                (PrimitiveType::Int, PrimitiveLiteral::Int(v)) => Some(Datum::int(v - 1)),
                (PrimitiveType::Long, PrimitiveLiteral::Long(v)) => Some(Datum::long(v - 1)),
                (PrimitiveType::Decimal { .. }, PrimitiveLiteral::Int128(v)) => {
                    Some(Datum::decimal(decimal_from_i128_with_scale(v - 1, 0))?)
                }
                (PrimitiveType::Date, PrimitiveLiteral::Int(v)) => Some(Datum::date(v - 1)),
                (PrimitiveType::Timestamp, PrimitiveLiteral::Long(v)) => {
                    Some(Datum::timestamp_micros(v - 1))
                }
                (PrimitiveType::Timestamptz, PrimitiveLiteral::Long(v)) => {
                    Some(Datum::timestamptz_micros(v - 1))
                }
                (PrimitiveType::TimestampNs, PrimitiveLiteral::Long(v)) => {
                    Some(Datum::timestamp_nanos(v - 1))
                }
                (PrimitiveType::TimestamptzNs, PrimitiveLiteral::Long(v)) => {
                    Some(Datum::timestamptz_nanos(v - 1))
                }
                _ => Some(datum.to_owned()),
            },
            PredicateOperator::GreaterThan => match (datum.data_type(), datum.literal()) {
                (PrimitiveType::Int, PrimitiveLiteral::Int(v)) => Some(Datum::int(v + 1)),
                (PrimitiveType::Long, PrimitiveLiteral::Long(v)) => Some(Datum::long(v + 1)),
                (PrimitiveType::Decimal { .. }, PrimitiveLiteral::Int128(v)) => {
                    Some(Datum::decimal(decimal_from_i128_with_scale(v + 1, 0))?)
                }
                (PrimitiveType::Date, PrimitiveLiteral::Int(v)) => Some(Datum::date(v + 1)),
                (PrimitiveType::Timestamp, PrimitiveLiteral::Long(v)) => {
                    Some(Datum::timestamp_micros(v + 1))
                }
                (PrimitiveType::Timestamptz, PrimitiveLiteral::Long(v)) => {
                    Some(Datum::timestamptz_micros(v + 1))
                }
                (PrimitiveType::TimestampNs, PrimitiveLiteral::Long(v)) => {
                    Some(Datum::timestamp_nanos(v + 1))
                }
                (PrimitiveType::TimestamptzNs, PrimitiveLiteral::Long(v)) => {
                    Some(Datum::timestamptz_nanos(v + 1))
                }
                _ => Some(datum.to_owned()),
            },
            PredicateOperator::Eq
            | PredicateOperator::LessThanOrEq
            | PredicateOperator::GreaterThanOrEq
            | PredicateOperator::StartsWith
            | PredicateOperator::NotStartsWith => Some(datum.to_owned()),
            _ => None,
        };

        Ok(adjusted_boundary)
    }

    /// Adjusts the comparison operator based on the specified datum and an
    /// optional width constraint.
    ///
    /// This function modifies the comparison operator for `LessThan` and
    /// `GreaterThan` cases to their inclusive counterparts (`LessThanOrEq`,
    /// `GreaterThanOrEq`) unconditionally. For `StartsWith` and
    /// `NotStartsWith` operators acting on string literals, the operator may
    /// be adjusted to `Eq` or `NotEq` if the string length matches the
    /// specified width, indicating a precise match rather than a prefix
    /// condition.
    fn adjust_operator(
        op: &PredicateOperator,
        datum: &Datum,
        width: Option<u32>,
    ) -> Option<PredicateOperator> {
        match op {
            PredicateOperator::LessThan => Some(PredicateOperator::LessThanOrEq),
            PredicateOperator::GreaterThan => Some(PredicateOperator::GreaterThanOrEq),
            PredicateOperator::StartsWith => match datum.literal() {
                PrimitiveLiteral::String(s) => {
                    if let Some(w) = width
                        && s.len() == w as usize
                    {
                        return Some(PredicateOperator::Eq);
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

    /// Adjust projection for temporal transforms, align with Java
    /// implementation: https://github.com/apache/iceberg/blob/main/api/src/main/java/org/apache/iceberg/transforms/ProjectionUtil.java#L275
    fn adjust_time_projection(
        &self,
        op: &PredicateOperator,
        original: &Datum,
        transformed: &Datum,
    ) -> Option<AdjustedProjection> {
        let should_adjust = match self {
            Transform::Day => matches!(original.data_type(), PrimitiveType::Timestamp),
            Transform::Year | Transform::Month => true,
            _ => false,
        };

        if should_adjust && let &PrimitiveLiteral::Int(v) = transformed.literal() {
            match op {
                PredicateOperator::LessThan
                | PredicateOperator::LessThanOrEq
                | PredicateOperator::In => {
                    if v < 0 {
                        // # TODO
                        // An ugly hack to fix. Refine the increment and decrement logic later.
                        match self {
                            Transform::Day => {
                                return Some(AdjustedProjection::Single(Datum::date(v + 1)));
                            }
                            _ => {
                                return Some(AdjustedProjection::Single(Datum::int(v + 1)));
                            }
                        }
                    };
                }
                PredicateOperator::Eq => {
                    if v < 0 {
                        let new_set = FnvHashSet::from_iter(vec![
                            transformed.to_owned(),
                            // # TODO
                            // An ugly hack to fix. Refine the increment and decrement logic later.
                            {
                                match self {
                                    Transform::Day => Datum::date(v + 1),
                                    _ => Datum::int(v + 1),
                                }
                            },
                        ]);
                        return Some(AdjustedProjection::Set(new_set));
                    }
                }
                _ => {
                    return None;
                }
            }
        };
        None
    }

    // Increment for Int, Long, Decimal, Date, Timestamp
    // Ignore other types
    #[inline]
    fn try_increment_number(datum: &Datum) -> Result<Datum> {
        match (datum.data_type(), datum.literal()) {
            (PrimitiveType::Int, PrimitiveLiteral::Int(v)) => Ok(Datum::int(v + 1)),
            (PrimitiveType::Long, PrimitiveLiteral::Long(v)) => Ok(Datum::long(v + 1)),
            (PrimitiveType::Decimal { .. }, PrimitiveLiteral::Int128(v)) => {
                Datum::decimal(decimal_from_i128_with_scale(v + 1, 0))
            }
            (PrimitiveType::Date, PrimitiveLiteral::Int(v)) => Ok(Datum::date(v + 1)),
            (PrimitiveType::Timestamp, PrimitiveLiteral::Long(v)) => {
                Ok(Datum::timestamp_micros(v + 1))
            }
            (PrimitiveType::TimestampNs, PrimitiveLiteral::Long(v)) => {
                Ok(Datum::timestamp_nanos(v + 1))
            }
            (PrimitiveType::Timestamptz, PrimitiveLiteral::Long(v)) => {
                Ok(Datum::timestamptz_micros(v + 1))
            }
            (PrimitiveType::TimestamptzNs, PrimitiveLiteral::Long(v)) => {
                Ok(Datum::timestamptz_nanos(v + 1))
            }
            (PrimitiveType::Int, _)
            | (PrimitiveType::Long, _)
            | (PrimitiveType::Decimal { .. }, _)
            | (PrimitiveType::Date, _)
            | (PrimitiveType::Timestamp, _) => Err(Error::new(
                ErrorKind::Unexpected,
                format!(
                    "Unsupported literal increment for type: {:?}",
                    datum.data_type()
                ),
            )),
            _ => Ok(datum.to_owned()),
        }
    }

    // Decrement for Int, Long, Decimal, Date, Timestamp
    // Ignore other types
    #[inline]
    fn try_decrement_number(datum: &Datum) -> Result<Datum> {
        match (datum.data_type(), datum.literal()) {
            (PrimitiveType::Int, PrimitiveLiteral::Int(v)) => Ok(Datum::int(v - 1)),
            (PrimitiveType::Long, PrimitiveLiteral::Long(v)) => Ok(Datum::long(v - 1)),
            (PrimitiveType::Decimal { .. }, PrimitiveLiteral::Int128(v)) => {
                Datum::decimal(decimal_from_i128_with_scale(v - 1, 0))
            }
            (PrimitiveType::Date, PrimitiveLiteral::Int(v)) => Ok(Datum::date(v - 1)),
            (PrimitiveType::Timestamp, PrimitiveLiteral::Long(v)) => {
                Ok(Datum::timestamp_micros(v - 1))
            }
            (PrimitiveType::TimestampNs, PrimitiveLiteral::Long(v)) => {
                Ok(Datum::timestamp_nanos(v - 1))
            }
            (PrimitiveType::Timestamptz, PrimitiveLiteral::Long(v)) => {
                Ok(Datum::timestamptz_micros(v - 1))
            }
            (PrimitiveType::TimestamptzNs, PrimitiveLiteral::Long(v)) => {
                Ok(Datum::timestamptz_nanos(v - 1))
            }
            (PrimitiveType::Int, _)
            | (PrimitiveType::Long, _)
            | (PrimitiveType::Decimal { .. }, _)
            | (PrimitiveType::Date, _)
            | (PrimitiveType::Timestamp, _) => Err(Error::new(
                ErrorKind::Unexpected,
                format!(
                    "Unsupported literal decrement for type: {:?}",
                    datum.data_type()
                ),
            )),
            _ => Ok(datum.to_owned()),
        }
    }

    fn truncate_number_strict(
        &self,
        name: &str,
        expr: &BinaryExpression<BoundReference>,
        func: &BoxedTransformFunction,
    ) -> Result<Option<Predicate>> {
        let boundary = expr.literal();

        if !matches!(
            boundary.data_type(),
            &PrimitiveType::Int
                | &PrimitiveType::Long
                | &PrimitiveType::Decimal { .. }
                | &PrimitiveType::Date
                | &PrimitiveType::Timestamp
                | &PrimitiveType::Timestamptz
                | &PrimitiveType::TimestampNs
                | &PrimitiveType::TimestamptzNs
        ) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Expected a numeric literal, got: {boundary:?}"),
            ));
        }

        let predicate = match expr.op() {
            PredicateOperator::LessThan => Some(Predicate::Binary(BinaryExpression::new(
                PredicateOperator::LessThan,
                Reference::new(name),
                func.transform_literal_result(boundary)?,
            ))),
            PredicateOperator::LessThanOrEq => Some(Predicate::Binary(BinaryExpression::new(
                PredicateOperator::LessThan,
                Reference::new(name),
                func.transform_literal_result(&Self::try_increment_number(boundary)?)?,
            ))),
            PredicateOperator::GreaterThan => Some(Predicate::Binary(BinaryExpression::new(
                PredicateOperator::GreaterThan,
                Reference::new(name),
                func.transform_literal_result(boundary)?,
            ))),
            PredicateOperator::GreaterThanOrEq => Some(Predicate::Binary(BinaryExpression::new(
                PredicateOperator::GreaterThan,
                Reference::new(name),
                func.transform_literal_result(&Self::try_decrement_number(boundary)?)?,
            ))),
            PredicateOperator::NotEq => Some(Predicate::Binary(BinaryExpression::new(
                PredicateOperator::NotEq,
                Reference::new(name),
                func.transform_literal_result(boundary)?,
            ))),
            _ => None,
        };

        Ok(predicate)
    }

    fn truncate_array_strict(
        &self,
        name: &str,
        expr: &BinaryExpression<BoundReference>,
        func: &BoxedTransformFunction,
    ) -> Result<Option<Predicate>> {
        let boundary = expr.literal();

        match expr.op() {
            PredicateOperator::LessThan | PredicateOperator::LessThanOrEq => {
                Ok(Some(Predicate::Binary(BinaryExpression::new(
                    PredicateOperator::LessThan,
                    Reference::new(name),
                    func.transform_literal_result(boundary)?,
                ))))
            }
            PredicateOperator::GreaterThan | PredicateOperator::GreaterThanOrEq => {
                Ok(Some(Predicate::Binary(BinaryExpression::new(
                    PredicateOperator::GreaterThan,
                    Reference::new(name),
                    func.transform_literal_result(boundary)?,
                ))))
            }
            PredicateOperator::NotEq => Ok(Some(Predicate::Binary(BinaryExpression::new(
                PredicateOperator::NotEq,
                Reference::new(name),
                func.transform_literal_result(boundary)?,
            )))),
            _ => Ok(None),
        }
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
                ));
            }
        };

        Ok(t)
    }
}

impl Serialize for Transform {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where S: Serializer {
        serializer.serialize_str(format!("{self}").as_str())
    }
}

impl<'de> Deserialize<'de> for Transform {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where D: Deserializer<'de> {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(<D::Error as serde::de::Error>::custom)
    }
}

/// An enum representing the result of the adjusted projection.
/// Either being a single adjusted datum or a set.
#[derive(Debug)]
enum AdjustedProjection {
    Single(Datum),
    Set(FnvHashSet<Datum>),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check_boundary(op: PredicateOperator, input: Datum, expected: Datum) {
        let result = Transform::adjust_boundary(&op, &input).unwrap().unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_adjust_boundary_timestamp_types() {
        for (datum, dec, inc) in [
            (
                Datum::timestamptz_micros(1000),
                Datum::timestamptz_micros(999),
                Datum::timestamptz_micros(1001),
            ),
            (
                Datum::timestamp_nanos(5000),
                Datum::timestamp_nanos(4999),
                Datum::timestamp_nanos(5001),
            ),
            (
                Datum::timestamptz_nanos(5000),
                Datum::timestamptz_nanos(4999),
                Datum::timestamptz_nanos(5001),
            ),
        ] {
            check_boundary(PredicateOperator::LessThan, datum.clone(), dec);
            check_boundary(PredicateOperator::GreaterThan, datum.clone(), inc);
            check_boundary(
                PredicateOperator::LessThanOrEq,
                datum.clone(),
                datum.clone(),
            );
            check_boundary(PredicateOperator::GreaterThanOrEq, datum.clone(), datum);
        }
    }

    /// Renders `ordinal` through the public API with the given declared type.
    fn human(transform: Transform, primitive: PrimitiveType, ordinal: i32) -> String {
        transform.to_human_string(&Type::Primitive(primitive), Some(&Literal::int(ordinal)))
    }

    /// Renders `ordinal` for a transform whose result type is `int`.
    fn human_int(transform: Transform, ordinal: i32) -> String {
        human(transform, PrimitiveType::Int, ordinal)
    }

    #[test]
    fn test_to_human_string_year() {
        assert_eq!(human_int(Transform::Year, -1970), "0000");
        assert_eq!(human_int(Transform::Year, -1), "1969");
        assert_eq!(human_int(Transform::Year, 0), "1970");
        assert_eq!(human_int(Transform::Year, 47), "2017");
    }

    #[test]
    fn test_to_human_string_month() {
        assert_eq!(human_int(Transform::Month, -1970 * 12), "0000-01");
        assert_eq!(human_int(Transform::Month, -13), "1968-12");
        assert_eq!(human_int(Transform::Month, -12), "1969-01");
        assert_eq!(human_int(Transform::Month, -1), "1969-12");
        assert_eq!(human_int(Transform::Month, 0), "1970-01");
        assert_eq!(human_int(Transform::Month, 11), "1970-12");
        assert_eq!(human_int(Transform::Month, 12), "1971-01");
        assert_eq!(human_int(Transform::Month, 569), "2017-06");
    }

    #[test]
    fn test_to_human_string_day() {
        assert_eq!(human_int(Transform::Day, -1), "1969-12-31");
        assert_eq!(human_int(Transform::Day, 0), "1970-01-01");
        assert_eq!(human_int(Transform::Day, 31), "1970-02-01");
        assert_eq!(human_int(Transform::Day, 17332), "2017-06-15");
    }

    #[test]
    fn test_to_human_string_hour() {
        assert_eq!(human_int(Transform::Hour, -24), "1969-12-31-00");
        assert_eq!(human_int(Transform::Hour, -1), "1969-12-31-23");
        assert_eq!(human_int(Transform::Hour, 0), "1970-01-01-00");
        assert_eq!(human_int(Transform::Hour, 23), "1970-01-01-23");
        assert_eq!(human_int(Transform::Hour, 24), "1970-01-02-00");
        assert_eq!(human_int(Transform::Hour, 1000), "1970-02-11-16");
        assert_eq!(human_int(Transform::Hour, 415984), "2017-06-15-16");
    }

    /// The temporal transforms ignore the declared field type, matching the Java
    /// signatures `toHumanString(Type alwaysInt, ..)` and
    /// `toHumanString(Type alwaysDate, ..)`.
    #[test]
    fn test_to_human_string_ignores_declared_type_for_temporal_transforms() {
        assert_eq!(human(Transform::Year, PrimitiveType::Date, 47), "2017");
        assert_eq!(human(Transform::Month, PrimitiveType::Date, 569), "2017-06");
        assert_eq!(
            human(Transform::Day, PrimitiveType::Int, 17332),
            "2017-06-15"
        );
        assert_eq!(
            human(Transform::Hour, PrimitiveType::Date, 415984),
            "2017-06-15-16"
        );
    }

    /// Transforms with no temporal format keep deferring to the datum, which
    /// renders according to the declared field type.
    #[test]
    fn test_to_human_string_defers_to_datum_for_other_transforms() {
        assert_eq!(
            human(Transform::Identity, PrimitiveType::Int, 17332),
            "17332"
        );
        assert_eq!(
            human(Transform::Identity, PrimitiveType::Date, 17332),
            "2017-06-15"
        );
        assert_eq!(human(Transform::Bucket(16), PrimitiveType::Int, 5), "5");

        // A literal that is not an `int` also defers to the datum rather than being
        // reported as `null`, so the temporal arms add no second behaviour change.
        assert_eq!(
            Transform::Year.to_human_string(
                &Type::Primitive(PrimitiveType::String),
                Some(&Literal::string("unformatted"))
            ),
            "unformatted"
        );
    }

    #[test]
    fn test_to_human_string_null_cases() {
        assert_eq!(human(Transform::Void, PrimitiveType::Int, 47), "null");
        assert_eq!(human(Transform::Void, PrimitiveType::Date, 17332), "null");
        for transform in [
            Transform::Year,
            Transform::Month,
            Transform::Day,
            Transform::Hour,
            Transform::Identity,
            Transform::Void,
        ] {
            assert_eq!(
                transform.to_human_string(&Type::Primitive(PrimitiveType::Int), None),
                "null"
            );
        }
    }
}
