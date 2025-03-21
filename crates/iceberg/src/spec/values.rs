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

/*!
 * Value in iceberg
 */

use std::any::Any;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::ops::Index;
use std::str::FromStr;

pub use _serde::RawLiteral;
use bitvec::vec::BitVec;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use num_bigint::BigInt;
use ordered_float::OrderedFloat;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::de::{
    MapAccess, {self},
};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use serde_json::{Map as JsonMap, Number, Value as JsonValue};
use timestamp::nanoseconds_to_datetime;
use uuid::Uuid;

use super::datatypes::{PrimitiveType, Type};
use crate::error::Result;
use crate::spec::values::date::{date_from_naive_date, days_to_date, unix_epoch};
use crate::spec::values::time::microseconds_to_time;
use crate::spec::values::timestamp::microseconds_to_datetime;
use crate::spec::values::timestamptz::{microseconds_to_datetimetz, nanoseconds_to_datetimetz};
use crate::spec::MAX_DECIMAL_PRECISION;
use crate::{ensure_data_valid, Error, ErrorKind};

/// Maximum value for [`PrimitiveType::Time`] type in microseconds, e.g. 23 hours 59 minutes 59 seconds 999999 microseconds.
const MAX_TIME_VALUE: i64 = 24 * 60 * 60 * 1_000_000i64 - 1;

const INT_MAX: i32 = 2147483647;
const INT_MIN: i32 = -2147483648;
const LONG_MAX: i64 = 9223372036854775807;
const LONG_MIN: i64 = -9223372036854775808;

/// Values present in iceberg type
#[derive(Clone, Debug, PartialOrd, PartialEq, Hash, Eq)]
pub enum PrimitiveLiteral {
    /// 0x00 for false, non-zero byte for true
    Boolean(bool),
    /// Stored as 4-byte little-endian
    Int(i32),
    /// Stored as 8-byte little-endian
    Long(i64),
    /// Stored as 4-byte little-endian
    Float(OrderedFloat<f32>),
    /// Stored as 8-byte little-endian
    Double(OrderedFloat<f64>),
    /// UTF-8 bytes (without length)
    String(String),
    /// Binary value (without length)
    Binary(Vec<u8>),
    /// Stored as 16-byte little-endian
    Int128(i128),
    /// Stored as 16-byte little-endian
    UInt128(u128),
    /// When a number is larger than it can hold
    AboveMax,
    /// When a number is smaller than it can hold
    BelowMin,
}

impl PrimitiveLiteral {
    /// Returns true if the Literal represents a primitive type
    /// that can be a NaN, and that it's value is NaN
    pub fn is_nan(&self) -> bool {
        match self {
            PrimitiveLiteral::Double(val) => val.is_nan(),
            PrimitiveLiteral::Float(val) => val.is_nan(),
            _ => false,
        }
    }
}

/// Literal associated with its type. The value and type pair is checked when construction, so the type and value is
/// guaranteed to be correct when used.
///
/// By default, we decouple the type and value of a literal, so we can use avoid the cost of storing extra type info
/// for each literal. But associate type with literal can be useful in some cases, for example, in unbound expression.
#[derive(Clone, Debug, PartialEq, Hash, Eq)]
pub struct Datum {
    r#type: PrimitiveType,
    literal: PrimitiveLiteral,
}

impl Serialize for Datum {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut struct_ser = serializer
            .serialize_struct("Datum", 2)
            .map_err(serde::ser::Error::custom)?;
        struct_ser
            .serialize_field("type", &self.r#type)
            .map_err(serde::ser::Error::custom)?;
        struct_ser
            .serialize_field(
                "literal",
                &RawLiteral::try_from(
                    Literal::Primitive(self.literal.clone()),
                    &Type::Primitive(self.r#type.clone()),
                )
                .map_err(serde::ser::Error::custom)?,
            )
            .map_err(serde::ser::Error::custom)?;
        struct_ser.end()
    }
}

impl<'de> Deserialize<'de> for Datum {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "lowercase")]
        enum Field {
            Type,
            Literal,
        }

        struct DatumVisitor;

        impl<'de> serde::de::Visitor<'de> for DatumVisitor {
            type Value = Datum;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("struct Datum")
            }

            fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
            where A: serde::de::SeqAccess<'de> {
                let r#type = seq
                    .next_element::<PrimitiveType>()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                let value = seq
                    .next_element::<RawLiteral>()?
                    .ok_or_else(|| serde::de::Error::invalid_length(1, &self))?;
                let Literal::Primitive(primitive) = value
                    .try_into(&Type::Primitive(r#type.clone()))
                    .map_err(serde::de::Error::custom)?
                    .ok_or_else(|| serde::de::Error::custom("None value"))?
                else {
                    return Err(serde::de::Error::custom("Invalid value"));
                };

                Ok(Datum::new(r#type, primitive))
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Datum, V::Error>
            where V: MapAccess<'de> {
                let mut raw_primitive: Option<RawLiteral> = None;
                let mut r#type: Option<PrimitiveType> = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Type => {
                            if r#type.is_some() {
                                return Err(de::Error::duplicate_field("type"));
                            }
                            r#type = Some(map.next_value()?);
                        }
                        Field::Literal => {
                            if raw_primitive.is_some() {
                                return Err(de::Error::duplicate_field("literal"));
                            }
                            raw_primitive = Some(map.next_value()?);
                        }
                    }
                }
                let Some(r#type) = r#type else {
                    return Err(serde::de::Error::missing_field("type"));
                };
                let Some(raw_primitive) = raw_primitive else {
                    return Err(serde::de::Error::missing_field("literal"));
                };
                let Literal::Primitive(primitive) = raw_primitive
                    .try_into(&Type::Primitive(r#type.clone()))
                    .map_err(serde::de::Error::custom)?
                    .ok_or_else(|| serde::de::Error::custom("None value"))?
                else {
                    return Err(serde::de::Error::custom("Invalid value"));
                };
                Ok(Datum::new(r#type, primitive))
            }
        }
        const FIELDS: &[&str] = &["type", "literal"];
        deserializer.deserialize_struct("Datum", FIELDS, DatumVisitor)
    }
}

impl PartialOrd for Datum {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (&self.literal, &other.literal, &self.r#type, &other.r#type) {
            // generate the arm with same type and same literal
            (
                PrimitiveLiteral::Boolean(val),
                PrimitiveLiteral::Boolean(other_val),
                PrimitiveType::Boolean,
                PrimitiveType::Boolean,
            ) => val.partial_cmp(other_val),
            (
                PrimitiveLiteral::Int(val),
                PrimitiveLiteral::Int(other_val),
                PrimitiveType::Int,
                PrimitiveType::Int,
            ) => val.partial_cmp(other_val),
            (
                PrimitiveLiteral::Long(val),
                PrimitiveLiteral::Long(other_val),
                PrimitiveType::Long,
                PrimitiveType::Long,
            ) => val.partial_cmp(other_val),
            (
                PrimitiveLiteral::Float(val),
                PrimitiveLiteral::Float(other_val),
                PrimitiveType::Float,
                PrimitiveType::Float,
            ) => val.partial_cmp(other_val),
            (
                PrimitiveLiteral::Double(val),
                PrimitiveLiteral::Double(other_val),
                PrimitiveType::Double,
                PrimitiveType::Double,
            ) => val.partial_cmp(other_val),
            (
                PrimitiveLiteral::Int(val),
                PrimitiveLiteral::Int(other_val),
                PrimitiveType::Date,
                PrimitiveType::Date,
            ) => val.partial_cmp(other_val),
            (
                PrimitiveLiteral::Long(val),
                PrimitiveLiteral::Long(other_val),
                PrimitiveType::Time,
                PrimitiveType::Time,
            ) => val.partial_cmp(other_val),
            (
                PrimitiveLiteral::Long(val),
                PrimitiveLiteral::Long(other_val),
                PrimitiveType::Timestamp,
                PrimitiveType::Timestamp,
            ) => val.partial_cmp(other_val),
            (
                PrimitiveLiteral::Long(val),
                PrimitiveLiteral::Long(other_val),
                PrimitiveType::Timestamptz,
                PrimitiveType::Timestamptz,
            ) => val.partial_cmp(other_val),
            (
                PrimitiveLiteral::String(val),
                PrimitiveLiteral::String(other_val),
                PrimitiveType::String,
                PrimitiveType::String,
            ) => val.partial_cmp(other_val),
            (
                PrimitiveLiteral::UInt128(val),
                PrimitiveLiteral::UInt128(other_val),
                PrimitiveType::Uuid,
                PrimitiveType::Uuid,
            ) => Uuid::from_u128(*val).partial_cmp(&Uuid::from_u128(*other_val)),
            (
                PrimitiveLiteral::Binary(val),
                PrimitiveLiteral::Binary(other_val),
                PrimitiveType::Fixed(_),
                PrimitiveType::Fixed(_),
            ) => val.partial_cmp(other_val),
            (
                PrimitiveLiteral::Binary(val),
                PrimitiveLiteral::Binary(other_val),
                PrimitiveType::Binary,
                PrimitiveType::Binary,
            ) => val.partial_cmp(other_val),
            (
                PrimitiveLiteral::Int128(val),
                PrimitiveLiteral::Int128(other_val),
                PrimitiveType::Decimal {
                    precision: _,
                    scale,
                },
                PrimitiveType::Decimal {
                    precision: _,
                    scale: other_scale,
                },
            ) => {
                let val = Decimal::from_i128_with_scale(*val, *scale);
                let other_val = Decimal::from_i128_with_scale(*other_val, *other_scale);
                val.partial_cmp(&other_val)
            }
            _ => None,
        }
    }
}

impl Display for Datum {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match (&self.r#type, &self.literal) {
            (_, PrimitiveLiteral::Boolean(val)) => write!(f, "{}", val),
            (PrimitiveType::Int, PrimitiveLiteral::Int(val)) => write!(f, "{}", val),
            (PrimitiveType::Long, PrimitiveLiteral::Long(val)) => write!(f, "{}", val),
            (_, PrimitiveLiteral::Float(val)) => write!(f, "{}", val),
            (_, PrimitiveLiteral::Double(val)) => write!(f, "{}", val),
            (PrimitiveType::Date, PrimitiveLiteral::Int(val)) => {
                write!(f, "{}", days_to_date(*val))
            }
            (PrimitiveType::Time, PrimitiveLiteral::Long(val)) => {
                write!(f, "{}", microseconds_to_time(*val))
            }
            (PrimitiveType::Timestamp, PrimitiveLiteral::Long(val)) => {
                write!(f, "{}", microseconds_to_datetime(*val))
            }
            (PrimitiveType::Timestamptz, PrimitiveLiteral::Long(val)) => {
                write!(f, "{}", microseconds_to_datetimetz(*val))
            }
            (PrimitiveType::TimestampNs, PrimitiveLiteral::Long(val)) => {
                write!(f, "{}", nanoseconds_to_datetime(*val))
            }
            (PrimitiveType::TimestamptzNs, PrimitiveLiteral::Long(val)) => {
                write!(f, "{}", nanoseconds_to_datetimetz(*val))
            }
            (_, PrimitiveLiteral::String(val)) => write!(f, r#""{}""#, val),
            (PrimitiveType::Uuid, PrimitiveLiteral::UInt128(val)) => {
                write!(f, "{}", Uuid::from_u128(*val))
            }
            (_, PrimitiveLiteral::Binary(val)) => display_bytes(val, f),
            (
                PrimitiveType::Decimal {
                    precision: _,
                    scale,
                },
                PrimitiveLiteral::Int128(val),
            ) => {
                write!(f, "{}", Decimal::from_i128_with_scale(*val, *scale))
            }
            (_, _) => {
                unreachable!()
            }
        }
    }
}

fn display_bytes(bytes: &[u8], f: &mut Formatter<'_>) -> std::fmt::Result {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02X}", b));
    }
    f.write_str(&s)
}

impl From<Datum> for Literal {
    fn from(value: Datum) -> Self {
        Literal::Primitive(value.literal)
    }
}

impl From<Datum> for PrimitiveLiteral {
    fn from(value: Datum) -> Self {
        value.literal
    }
}

impl Datum {
    /// Creates a `Datum` from a `PrimitiveType` and a `PrimitiveLiteral`
    pub(crate) fn new(r#type: PrimitiveType, literal: PrimitiveLiteral) -> Self {
        Datum { r#type, literal }
    }

    /// Create iceberg value from bytes.
    ///
    /// See [this spec](https://iceberg.apache.org/spec/#binary-single-value-serialization) for reference.
    pub fn try_from_bytes(bytes: &[u8], data_type: PrimitiveType) -> Result<Self> {
        let literal = match data_type {
            PrimitiveType::Boolean => {
                if bytes.len() == 1 && bytes[0] == 0u8 {
                    PrimitiveLiteral::Boolean(false)
                } else {
                    PrimitiveLiteral::Boolean(true)
                }
            }
            PrimitiveType::Int => PrimitiveLiteral::Int(i32::from_le_bytes(bytes.try_into()?)),
            PrimitiveType::Long => PrimitiveLiteral::Long(i64::from_le_bytes(bytes.try_into()?)),
            PrimitiveType::Float => {
                PrimitiveLiteral::Float(OrderedFloat(f32::from_le_bytes(bytes.try_into()?)))
            }
            PrimitiveType::Double => {
                PrimitiveLiteral::Double(OrderedFloat(f64::from_le_bytes(bytes.try_into()?)))
            }
            PrimitiveType::Date => PrimitiveLiteral::Int(i32::from_le_bytes(bytes.try_into()?)),
            PrimitiveType::Time => PrimitiveLiteral::Long(i64::from_le_bytes(bytes.try_into()?)),
            PrimitiveType::Timestamp => {
                PrimitiveLiteral::Long(i64::from_le_bytes(bytes.try_into()?))
            }
            PrimitiveType::Timestamptz => {
                PrimitiveLiteral::Long(i64::from_le_bytes(bytes.try_into()?))
            }
            PrimitiveType::TimestampNs => {
                PrimitiveLiteral::Long(i64::from_le_bytes(bytes.try_into()?))
            }
            PrimitiveType::TimestamptzNs => {
                PrimitiveLiteral::Long(i64::from_le_bytes(bytes.try_into()?))
            }
            PrimitiveType::String => {
                PrimitiveLiteral::String(std::str::from_utf8(bytes)?.to_string())
            }
            PrimitiveType::Uuid => {
                PrimitiveLiteral::UInt128(u128::from_be_bytes(bytes.try_into()?))
            }
            PrimitiveType::Fixed(_) => PrimitiveLiteral::Binary(Vec::from(bytes)),
            PrimitiveType::Binary => PrimitiveLiteral::Binary(Vec::from(bytes)),
            PrimitiveType::Decimal { .. } => {
                let unscaled_value = BigInt::from_signed_bytes_be(bytes);
                PrimitiveLiteral::Int128(unscaled_value.to_i128().ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Can't convert bytes to i128: {:?}", bytes),
                    )
                })?)
            }
        };
        Ok(Datum::new(data_type, literal))
    }

    /// Convert the value to bytes
    ///
    /// See [this spec](https://iceberg.apache.org/spec/#binary-single-value-serialization) for reference.
    pub fn to_bytes(&self) -> Result<ByteBuf> {
        let buf = match &self.literal {
            PrimitiveLiteral::Boolean(val) => {
                if *val {
                    ByteBuf::from([1u8])
                } else {
                    ByteBuf::from([0u8])
                }
            }
            PrimitiveLiteral::Int(val) => ByteBuf::from(val.to_le_bytes()),
            PrimitiveLiteral::Long(val) => ByteBuf::from(val.to_le_bytes()),
            PrimitiveLiteral::Float(val) => ByteBuf::from(val.to_le_bytes()),
            PrimitiveLiteral::Double(val) => ByteBuf::from(val.to_le_bytes()),
            PrimitiveLiteral::String(val) => ByteBuf::from(val.as_bytes()),
            PrimitiveLiteral::UInt128(val) => ByteBuf::from(val.to_be_bytes()),
            PrimitiveLiteral::Binary(val) => ByteBuf::from(val.as_slice()),
            PrimitiveLiteral::Int128(val) => {
                let PrimitiveType::Decimal { precision, .. } = self.r#type else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "PrimitiveLiteral Int128 must be PrimitiveType Decimal but got {}",
                            &self.r#type
                        ),
                    ));
                };

                // It's required by iceberg spec that we must keep the minimum
                // number of bytes for the value
                let Ok(required_bytes) = Type::decimal_required_bytes(precision) else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "PrimitiveType Decimal must has valid precision but got {}",
                            precision
                        ),
                    ));
                };

                // The primitive literal is unscaled value.
                let unscaled_value = BigInt::from(*val);
                // Convert into two's-complement byte representation of the BigInt
                // in big-endian byte order.
                let mut bytes = unscaled_value.to_signed_bytes_be();
                // Truncate with required bytes to make sure.
                bytes.truncate(required_bytes as usize);

                ByteBuf::from(bytes)
            }
            PrimitiveLiteral::AboveMax | PrimitiveLiteral::BelowMin => {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Cannot convert AboveMax or BelowMin to bytes".to_string(),
                ));
            }
        };

        Ok(buf)
    }

    /// Creates a boolean value.
    ///
    /// Example:
    /// ```rust
    /// use iceberg::spec::{Datum, Literal, PrimitiveLiteral};
    /// let t = Datum::bool(true);
    ///
    /// assert_eq!(format!("{}", t), "true".to_string());
    /// assert_eq!(
    ///     Literal::from(t),
    ///     Literal::Primitive(PrimitiveLiteral::Boolean(true))
    /// );
    /// ```
    pub fn bool<T: Into<bool>>(t: T) -> Self {
        Self {
            r#type: PrimitiveType::Boolean,
            literal: PrimitiveLiteral::Boolean(t.into()),
        }
    }

    /// Creates a boolean value from string.
    /// See [Parse bool from str](https://doc.rust-lang.org/stable/std/primitive.bool.html#impl-FromStr-for-bool) for reference.
    ///
    /// Example:
    /// ```rust
    /// use iceberg::spec::{Datum, Literal, PrimitiveLiteral};
    /// let t = Datum::bool_from_str("false").unwrap();
    ///
    /// assert_eq!(&format!("{}", t), "false");
    /// assert_eq!(
    ///     Literal::Primitive(PrimitiveLiteral::Boolean(false)),
    ///     t.into()
    /// );
    /// ```
    pub fn bool_from_str<S: AsRef<str>>(s: S) -> Result<Self> {
        let v = s.as_ref().parse::<bool>().map_err(|e| {
            Error::new(ErrorKind::DataInvalid, "Can't parse string to bool.").with_source(e)
        })?;
        Ok(Self::bool(v))
    }

    /// Creates an 32bit integer.
    ///
    /// Example:
    /// ```rust
    /// use iceberg::spec::{Datum, Literal, PrimitiveLiteral};
    /// let t = Datum::int(23i8);
    ///
    /// assert_eq!(&format!("{}", t), "23");
    /// assert_eq!(Literal::Primitive(PrimitiveLiteral::Int(23)), t.into());
    /// ```
    pub fn int<T: Into<i32>>(t: T) -> Self {
        Self {
            r#type: PrimitiveType::Int,
            literal: PrimitiveLiteral::Int(t.into()),
        }
    }

    /// Creates an 64bit integer.
    ///
    /// Example:
    /// ```rust
    /// use iceberg::spec::{Datum, Literal, PrimitiveLiteral};
    /// let t = Datum::long(24i8);
    ///
    /// assert_eq!(&format!("{t}"), "24");
    /// assert_eq!(Literal::Primitive(PrimitiveLiteral::Long(24)), t.into());
    /// ```
    pub fn long<T: Into<i64>>(t: T) -> Self {
        Self {
            r#type: PrimitiveType::Long,
            literal: PrimitiveLiteral::Long(t.into()),
        }
    }

    /// Creates an 32bit floating point number.
    ///
    /// Example:
    /// ```rust
    /// use iceberg::spec::{Datum, Literal, PrimitiveLiteral};
    /// use ordered_float::OrderedFloat;
    /// let t = Datum::float(32.1f32);
    ///
    /// assert_eq!(&format!("{t}"), "32.1");
    /// assert_eq!(
    ///     Literal::Primitive(PrimitiveLiteral::Float(OrderedFloat(32.1))),
    ///     t.into()
    /// );
    /// ```
    pub fn float<T: Into<f32>>(t: T) -> Self {
        Self {
            r#type: PrimitiveType::Float,
            literal: PrimitiveLiteral::Float(OrderedFloat(t.into())),
        }
    }

    /// Creates an 64bit floating point number.
    ///
    /// Example:
    /// ```rust
    /// use iceberg::spec::{Datum, Literal, PrimitiveLiteral};
    /// use ordered_float::OrderedFloat;
    /// let t = Datum::double(32.1f64);
    ///
    /// assert_eq!(&format!("{t}"), "32.1");
    /// assert_eq!(
    ///     Literal::Primitive(PrimitiveLiteral::Double(OrderedFloat(32.1))),
    ///     t.into()
    /// );
    /// ```
    pub fn double<T: Into<f64>>(t: T) -> Self {
        Self {
            r#type: PrimitiveType::Double,
            literal: PrimitiveLiteral::Double(OrderedFloat(t.into())),
        }
    }

    /// Creates date literal from number of days from unix epoch directly.
    ///
    /// Example:
    /// ```rust
    /// use iceberg::spec::{Datum, Literal, PrimitiveLiteral};
    /// // 2 days after 1970-01-01
    /// let t = Datum::date(2);
    ///
    /// assert_eq!(&format!("{t}"), "1970-01-03");
    /// assert_eq!(Literal::Primitive(PrimitiveLiteral::Int(2)), t.into());
    /// ```
    pub fn date(days: i32) -> Self {
        Self {
            r#type: PrimitiveType::Date,
            literal: PrimitiveLiteral::Int(days),
        }
    }

    /// Creates date literal in `%Y-%m-%d` format, assume in utc timezone.
    ///
    /// See [`NaiveDate::from_str`].
    ///
    /// Example
    /// ```rust
    /// use iceberg::spec::{Datum, Literal};
    /// let t = Datum::date_from_str("1970-01-05").unwrap();
    ///
    /// assert_eq!(&format!("{t}"), "1970-01-05");
    /// assert_eq!(Literal::date(4), t.into());
    /// ```
    pub fn date_from_str<S: AsRef<str>>(s: S) -> Result<Self> {
        let t = s.as_ref().parse::<NaiveDate>().map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Can't parse date from string: {}", s.as_ref()),
            )
            .with_source(e)
        })?;

        Ok(Self::date(date_from_naive_date(t)))
    }

    /// Create date literal from calendar date (year, month and day).
    ///
    /// See [`NaiveDate::from_ymd_opt`].
    ///
    /// Example:
    ///
    ///```rust
    /// use iceberg::spec::{Datum, Literal};
    /// let t = Datum::date_from_ymd(1970, 1, 5).unwrap();
    ///
    /// assert_eq!(&format!("{t}"), "1970-01-05");
    /// assert_eq!(Literal::date(4), t.into());
    /// ```
    pub fn date_from_ymd(year: i32, month: u32, day: u32) -> Result<Self> {
        let t = NaiveDate::from_ymd_opt(year, month, day).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Can't create date from year: {year}, month: {month}, day: {day}"),
            )
        })?;

        Ok(Self::date(date_from_naive_date(t)))
    }

    /// Creates time literal in microseconds directly.
    ///
    /// It will return error when it's negative or too large to fit in 24 hours.
    ///
    /// Example:
    ///
    /// ```rust
    /// use iceberg::spec::{Datum, Literal};
    /// let micro_secs = {
    ///     1 * 3600 * 1_000_000 + // 1 hour
    ///     2 * 60 * 1_000_000 +   // 2 minutes
    ///     1 * 1_000_000 + // 1 second
    ///     888999 // microseconds
    /// };
    ///
    /// let t = Datum::time_micros(micro_secs).unwrap();
    ///
    /// assert_eq!(&format!("{t}"), "01:02:01.888999");
    /// assert_eq!(Literal::time(micro_secs), t.into());
    ///
    /// let negative_value = -100;
    /// assert!(Datum::time_micros(negative_value).is_err());
    ///
    /// let too_large_value = 36 * 60 * 60 * 1_000_000; // Too large to fit in 24 hours.
    /// assert!(Datum::time_micros(too_large_value).is_err());
    /// ```
    pub fn time_micros(value: i64) -> Result<Self> {
        ensure_data_valid!(
            (0..=MAX_TIME_VALUE).contains(&value),
            "Invalid value for Time type: {}",
            value
        );

        Ok(Self {
            r#type: PrimitiveType::Time,
            literal: PrimitiveLiteral::Long(value),
        })
    }

    /// Creates time literal from [`chrono::NaiveTime`].
    fn time_from_naive_time(t: NaiveTime) -> Self {
        let duration = t - unix_epoch().time();
        // It's safe to unwrap here since less than 24 hours will never overflow.
        let micro_secs = duration.num_microseconds().unwrap();

        Self {
            r#type: PrimitiveType::Time,
            literal: PrimitiveLiteral::Long(micro_secs),
        }
    }

    /// Creates time literal in microseconds in `%H:%M:%S:.f` format.
    ///
    /// See [`NaiveTime::from_str`] for details.
    ///
    /// Example:
    /// ```rust
    /// use iceberg::spec::{Datum, Literal};
    /// let t = Datum::time_from_str("01:02:01.888999777").unwrap();
    ///
    /// assert_eq!(&format!("{t}"), "01:02:01.888999");
    /// ```
    pub fn time_from_str<S: AsRef<str>>(s: S) -> Result<Self> {
        let t = s.as_ref().parse::<NaiveTime>().map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Can't parse time from string: {}", s.as_ref()),
            )
            .with_source(e)
        })?;

        Ok(Self::time_from_naive_time(t))
    }

    /// Creates time literal from hour, minute, second, and microseconds.
    ///
    /// See [`NaiveTime::from_hms_micro_opt`].
    ///
    /// Example:
    /// ```rust
    /// use iceberg::spec::{Datum, Literal};
    /// let t = Datum::time_from_hms_micro(22, 15, 33, 111).unwrap();
    ///
    /// assert_eq!(&format!("{t}"), "22:15:33.000111");
    /// ```
    pub fn time_from_hms_micro(hour: u32, min: u32, sec: u32, micro: u32) -> Result<Self> {
        let t = NaiveTime::from_hms_micro_opt(hour, min, sec, micro)
            .ok_or_else(|| Error::new(
                ErrorKind::DataInvalid,
                format!("Can't create time from hour: {hour}, min: {min}, second: {sec}, microsecond: {micro}"),
            ))?;
        Ok(Self::time_from_naive_time(t))
    }

    /// Creates a timestamp from unix epoch in microseconds.
    ///
    /// Example:
    ///
    /// ```rust
    /// use iceberg::spec::Datum;
    /// let t = Datum::timestamp_micros(1000);
    ///
    /// assert_eq!(&format!("{t}"), "1970-01-01 00:00:00.001");
    /// ```
    pub fn timestamp_micros(value: i64) -> Self {
        Self {
            r#type: PrimitiveType::Timestamp,
            literal: PrimitiveLiteral::Long(value),
        }
    }

    /// Creates a timestamp from unix epoch in nanoseconds.
    ///
    /// Example:
    ///
    /// ```rust
    /// use iceberg::spec::Datum;
    /// let t = Datum::timestamp_nanos(1000);
    ///
    /// assert_eq!(&format!("{t}"), "1970-01-01 00:00:00.000001");
    /// ```
    pub fn timestamp_nanos(value: i64) -> Self {
        Self {
            r#type: PrimitiveType::TimestampNs,
            literal: PrimitiveLiteral::Long(value),
        }
    }

    /// Creates a timestamp from [`DateTime`].
    ///
    /// Example:
    ///
    /// ```rust
    /// use chrono::{NaiveDate, NaiveDateTime, TimeZone, Utc};
    /// use iceberg::spec::Datum;
    /// let t = Datum::timestamp_from_datetime(
    ///     NaiveDate::from_ymd_opt(1992, 3, 1)
    ///         .unwrap()
    ///         .and_hms_micro_opt(1, 2, 3, 88)
    ///         .unwrap(),
    /// );
    ///
    /// assert_eq!(&format!("{t}"), "1992-03-01 01:02:03.000088");
    /// ```
    pub fn timestamp_from_datetime(dt: NaiveDateTime) -> Self {
        Self::timestamp_micros(dt.and_utc().timestamp_micros())
    }

    /// Parse a timestamp in [`%Y-%m-%dT%H:%M:%S%.f`] format.
    ///
    /// See [`NaiveDateTime::from_str`].
    ///
    /// Example:
    ///
    /// ```rust
    /// use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
    /// use iceberg::spec::{Datum, Literal};
    /// let t = Datum::timestamp_from_str("1992-03-01T01:02:03.000088").unwrap();
    ///
    /// assert_eq!(&format!("{t}"), "1992-03-01 01:02:03.000088");
    /// ```
    pub fn timestamp_from_str<S: AsRef<str>>(s: S) -> Result<Self> {
        let dt = s.as_ref().parse::<NaiveDateTime>().map_err(|e| {
            Error::new(ErrorKind::DataInvalid, "Can't parse timestamp.").with_source(e)
        })?;

        Ok(Self::timestamp_from_datetime(dt))
    }

    /// Creates a timestamp with timezone from unix epoch in microseconds.
    ///
    /// Example:
    ///
    /// ```rust
    /// use iceberg::spec::Datum;
    /// let t = Datum::timestamptz_micros(1000);
    ///
    /// assert_eq!(&format!("{t}"), "1970-01-01 00:00:00.001 UTC");
    /// ```
    pub fn timestamptz_micros(value: i64) -> Self {
        Self {
            r#type: PrimitiveType::Timestamptz,
            literal: PrimitiveLiteral::Long(value),
        }
    }

    /// Creates a timestamp with timezone from unix epoch in nanoseconds.
    ///
    /// Example:
    ///
    /// ```rust
    /// use iceberg::spec::Datum;
    /// let t = Datum::timestamptz_nanos(1000);
    ///
    /// assert_eq!(&format!("{t}"), "1970-01-01 00:00:00.000001 UTC");
    /// ```
    pub fn timestamptz_nanos(value: i64) -> Self {
        Self {
            r#type: PrimitiveType::TimestamptzNs,
            literal: PrimitiveLiteral::Long(value),
        }
    }

    /// Creates a timestamp with timezone from [`DateTime`].
    /// Example:
    ///
    /// ```rust
    /// use chrono::{TimeZone, Utc};
    /// use iceberg::spec::Datum;
    /// let t = Datum::timestamptz_from_datetime(Utc.timestamp_opt(1000, 0).unwrap());
    ///
    /// assert_eq!(&format!("{t}"), "1970-01-01 00:16:40 UTC");
    /// ```
    pub fn timestamptz_from_datetime<T: TimeZone>(dt: DateTime<T>) -> Self {
        Self::timestamptz_micros(dt.with_timezone(&Utc).timestamp_micros())
    }

    /// Parse timestamp with timezone in RFC3339 format.
    ///
    /// See [`DateTime::from_str`].
    ///
    /// Example:
    ///
    /// ```rust
    /// use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
    /// use iceberg::spec::{Datum, Literal};
    /// let t = Datum::timestamptz_from_str("1992-03-01T01:02:03.000088+08:00").unwrap();
    ///
    /// assert_eq!(&format!("{t}"), "1992-02-29 17:02:03.000088 UTC");
    /// ```
    pub fn timestamptz_from_str<S: AsRef<str>>(s: S) -> Result<Self> {
        let dt = DateTime::<Utc>::from_str(s.as_ref()).map_err(|e| {
            Error::new(ErrorKind::DataInvalid, "Can't parse datetime.").with_source(e)
        })?;

        Ok(Self::timestamptz_from_datetime(dt))
    }

    /// Creates a string literal.
    ///
    /// Example:
    ///
    /// ```rust
    /// use iceberg::spec::Datum;
    /// let t = Datum::string("ss");
    ///
    /// assert_eq!(&format!("{t}"), r#""ss""#);
    /// ```
    pub fn string<S: ToString>(s: S) -> Self {
        Self {
            r#type: PrimitiveType::String,
            literal: PrimitiveLiteral::String(s.to_string()),
        }
    }

    /// Creates uuid literal.
    ///
    /// Example:
    ///
    /// ```rust
    /// use iceberg::spec::Datum;
    /// use uuid::uuid;
    /// let t = Datum::uuid(uuid!("a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8"));
    ///
    /// assert_eq!(&format!("{t}"), "a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8");
    /// ```
    pub fn uuid(uuid: Uuid) -> Self {
        Self {
            r#type: PrimitiveType::Uuid,
            literal: PrimitiveLiteral::UInt128(uuid.as_u128()),
        }
    }

    /// Creates uuid from str. See [`Uuid::parse_str`].
    ///
    /// Example:
    ///
    /// ```rust
    /// use iceberg::spec::Datum;
    /// let t = Datum::uuid_from_str("a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8").unwrap();
    ///
    /// assert_eq!(&format!("{t}"), "a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8");
    /// ```
    pub fn uuid_from_str<S: AsRef<str>>(s: S) -> Result<Self> {
        let uuid = Uuid::parse_str(s.as_ref()).map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Can't parse uuid from string: {}", s.as_ref()),
            )
            .with_source(e)
        })?;
        Ok(Self::uuid(uuid))
    }

    /// Creates a fixed literal from bytes.
    ///
    /// Example:
    ///
    /// ```rust
    /// use iceberg::spec::{Datum, Literal, PrimitiveLiteral};
    /// let t = Datum::fixed(vec![1u8, 2u8]);
    ///
    /// assert_eq!(&format!("{t}"), "0102");
    /// ```
    pub fn fixed<I: IntoIterator<Item = u8>>(input: I) -> Self {
        let value: Vec<u8> = input.into_iter().collect();
        Self {
            r#type: PrimitiveType::Fixed(value.len() as u64),
            literal: PrimitiveLiteral::Binary(value),
        }
    }

    /// Creates a binary literal from bytes.
    ///
    /// Example:
    ///
    /// ```rust
    /// use iceberg::spec::Datum;
    /// let t = Datum::binary(vec![1u8, 100u8]);
    ///
    /// assert_eq!(&format!("{t}"), "0164");
    /// ```
    pub fn binary<I: IntoIterator<Item = u8>>(input: I) -> Self {
        Self {
            r#type: PrimitiveType::Binary,
            literal: PrimitiveLiteral::Binary(input.into_iter().collect()),
        }
    }

    /// Creates decimal literal from string. See [`Decimal::from_str_exact`].
    ///
    /// Example:
    ///
    /// ```rust
    /// use iceberg::spec::Datum;
    /// use itertools::assert_equal;
    /// use rust_decimal::Decimal;
    /// let t = Datum::decimal_from_str("123.45").unwrap();
    ///
    /// assert_eq!(&format!("{t}"), "123.45");
    /// ```
    pub fn decimal_from_str<S: AsRef<str>>(s: S) -> Result<Self> {
        let decimal = Decimal::from_str_exact(s.as_ref()).map_err(|e| {
            Error::new(ErrorKind::DataInvalid, "Can't parse decimal.").with_source(e)
        })?;

        Self::decimal(decimal)
    }

    /// Try to create a decimal literal from [`Decimal`].
    ///
    /// Example:
    ///
    /// ```rust
    /// use iceberg::spec::Datum;
    /// use rust_decimal::Decimal;
    ///
    /// let t = Datum::decimal(Decimal::new(123, 2)).unwrap();
    ///
    /// assert_eq!(&format!("{t}"), "1.23");
    /// ```
    pub fn decimal(value: impl Into<Decimal>) -> Result<Self> {
        let decimal = value.into();
        let scale = decimal.scale();

        let r#type = Type::decimal(MAX_DECIMAL_PRECISION, scale)?;
        if let Type::Primitive(p) = r#type {
            Ok(Self {
                r#type: p,
                literal: PrimitiveLiteral::Int128(decimal.mantissa()),
            })
        } else {
            unreachable!("Decimal type must be primitive.")
        }
    }

    /// Try to create a decimal literal from [`Decimal`] with precision.
    ///
    /// Example:
    ///
    /// ```rust
    /// use iceberg::spec::Datum;
    /// use rust_decimal::Decimal;
    ///
    /// let t = Datum::decimal_with_precision(Decimal::new(123, 2), 30).unwrap();
    ///
    /// assert_eq!(&format!("{t}"), "1.23");
    /// ```
    pub fn decimal_with_precision(value: impl Into<Decimal>, precision: u32) -> Result<Self> {
        let decimal = value.into();
        let scale = decimal.scale();

        let available_bytes = Type::decimal_required_bytes(precision)? as usize;
        let unscaled_value = BigInt::from(decimal.mantissa());
        let actual_bytes = unscaled_value.to_signed_bytes_be();
        if actual_bytes.len() > available_bytes {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Decimal value {} is too large for precision {}",
                    decimal, precision
                ),
            ));
        }

        let r#type = Type::decimal(precision, scale)?;
        if let Type::Primitive(p) = r#type {
            Ok(Self {
                r#type: p,
                literal: PrimitiveLiteral::Int128(decimal.mantissa()),
            })
        } else {
            unreachable!("Decimal type must be primitive.")
        }
    }

    fn i64_to_i32<T: Into<i64> + PartialOrd<i64>>(val: T) -> Datum {
        if val > INT_MAX as i64 {
            Datum::new(PrimitiveType::Int, PrimitiveLiteral::AboveMax)
        } else if val < INT_MIN as i64 {
            Datum::new(PrimitiveType::Int, PrimitiveLiteral::BelowMin)
        } else {
            Datum::int(val.into() as i32)
        }
    }

    fn i128_to_i32<T: Into<i128> + PartialOrd<i128>>(val: T) -> Datum {
        if val > INT_MAX as i128 {
            Datum::new(PrimitiveType::Int, PrimitiveLiteral::AboveMax)
        } else if val < INT_MIN as i128 {
            Datum::new(PrimitiveType::Int, PrimitiveLiteral::BelowMin)
        } else {
            Datum::int(val.into() as i32)
        }
    }

    fn i128_to_i64<T: Into<i128> + PartialOrd<i128>>(val: T) -> Datum {
        if val > LONG_MAX as i128 {
            Datum::new(PrimitiveType::Long, PrimitiveLiteral::AboveMax)
        } else if val < LONG_MIN as i128 {
            Datum::new(PrimitiveType::Long, PrimitiveLiteral::BelowMin)
        } else {
            Datum::long(val.into() as i64)
        }
    }

    fn string_to_i128<S: AsRef<str>>(s: S) -> Result<i128> {
        s.as_ref().parse::<i128>().map_err(|e| {
            Error::new(ErrorKind::DataInvalid, "Can't parse string to i128.").with_source(e)
        })
    }

    /// Convert the datum to `target_type`.
    pub fn to(self, target_type: &Type) -> Result<Datum> {
        match target_type {
            Type::Primitive(target_primitive_type) => {
                match (&self.literal, &self.r#type, target_primitive_type) {
                    (PrimitiveLiteral::Int(val), _, PrimitiveType::Int) => Ok(Datum::int(*val)),
                    (PrimitiveLiteral::Int(val), _, PrimitiveType::Date) => Ok(Datum::date(*val)),
                    (PrimitiveLiteral::Int(val), _, PrimitiveType::Long) => Ok(Datum::long(*val)),
                    (PrimitiveLiteral::Long(val), _, PrimitiveType::Int) => {
                        Ok(Datum::i64_to_i32(*val))
                    }
                    (PrimitiveLiteral::Long(val), _, PrimitiveType::Timestamp) => {
                        Ok(Datum::timestamp_micros(*val))
                    }
                    (PrimitiveLiteral::Long(val), _, PrimitiveType::Timestamptz) => {
                        Ok(Datum::timestamptz_micros(*val))
                    }
                    // Let's wait with nano's until this clears up: https://github.com/apache/iceberg/pull/11775
                    (PrimitiveLiteral::Int128(val), _, PrimitiveType::Long) => {
                        Ok(Datum::i128_to_i64(*val))
                    }

                    (PrimitiveLiteral::String(val), _, PrimitiveType::Boolean) => {
                        Datum::bool_from_str(val)
                    }
                    (PrimitiveLiteral::String(val), _, PrimitiveType::Int) => {
                        Datum::string_to_i128(val).map(Datum::i128_to_i32)
                    }
                    (PrimitiveLiteral::String(val), _, PrimitiveType::Long) => {
                        Datum::string_to_i128(val).map(Datum::i128_to_i64)
                    }
                    (PrimitiveLiteral::String(val), _, PrimitiveType::Timestamp) => {
                        Datum::timestamp_from_str(val)
                    }
                    (PrimitiveLiteral::String(val), _, PrimitiveType::Timestamptz) => {
                        Datum::timestamptz_from_str(val)
                    }

                    // TODO: implement more type conversions
                    (_, self_type, target_type) if self_type == target_type => Ok(self),
                    _ => Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Can't convert datum from {} type to {} type.",
                            self.r#type, target_primitive_type
                        ),
                    )),
                }
            }
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Can't convert datum from {} type to {} type.",
                    self.r#type, target_type
                ),
            )),
        }
    }

    /// Get the primitive literal from datum.
    pub fn literal(&self) -> &PrimitiveLiteral {
        &self.literal
    }

    /// Get the primitive type from datum.
    pub fn data_type(&self) -> &PrimitiveType {
        &self.r#type
    }

    /// Returns true if the Literal represents a primitive type
    /// that can be a NaN, and that it's value is NaN
    pub fn is_nan(&self) -> bool {
        match self.literal {
            PrimitiveLiteral::Double(val) => val.is_nan(),
            PrimitiveLiteral::Float(val) => val.is_nan(),
            _ => false,
        }
    }
}

/// Map is a collection of key-value pairs with a key type and a value type.
///
/// It used in Literal::Map, to make it hashable, the order of key-value pairs is stored in a separate vector
/// so that we can hash the map in a deterministic way. But it also means that the order of key-value pairs is matter
/// for the hash value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Map {
    index: HashMap<Literal, usize>,
    pair: Vec<(Literal, Option<Literal>)>,
}

impl Map {
    /// Creates a new empty map.
    pub fn new() -> Self {
        Self {
            index: HashMap::new(),
            pair: Vec::new(),
        }
    }

    /// Return the number of key-value pairs in the map.
    pub fn len(&self) -> usize {
        self.pair.len()
    }

    /// Returns true if the map contains no elements.
    pub fn is_empty(&self) -> bool {
        self.pair.is_empty()
    }

    /// Inserts a key-value pair into the map.
    /// If the map did not have this key present, None is returned.
    /// If the map did have this key present, the value is updated, and the old value is returned.
    pub fn insert(&mut self, key: Literal, value: Option<Literal>) -> Option<Option<Literal>> {
        if let Some(index) = self.index.get(&key) {
            let old_value = std::mem::replace(&mut self.pair[*index].1, value);
            Some(old_value)
        } else {
            self.pair.push((key.clone(), value));
            self.index.insert(key, self.pair.len() - 1);
            None
        }
    }

    /// Returns a reference to the value corresponding to the key.
    /// If the key is not present in the map, None is returned.
    pub fn get(&self, key: &Literal) -> Option<&Option<Literal>> {
        self.index.get(key).map(|index| &self.pair[*index].1)
    }

    /// The order of map is matter, so this method used to compare two maps has same key-value pairs without considering the order.
    pub fn has_same_content(&self, other: &Map) -> bool {
        if self.len() != other.len() {
            return false;
        }

        for (key, value) in &self.pair {
            match other.get(key) {
                Some(other_value) if value == other_value => (),
                _ => return false,
            }
        }

        true
    }
}

impl Default for Map {
    fn default() -> Self {
        Self::new()
    }
}

impl Hash for Map {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for (key, value) in &self.pair {
            key.hash(state);
            value.hash(state);
        }
    }
}

impl FromIterator<(Literal, Option<Literal>)> for Map {
    fn from_iter<T: IntoIterator<Item = (Literal, Option<Literal>)>>(iter: T) -> Self {
        let mut map = Map::new();
        for (key, value) in iter {
            map.insert(key, value);
        }
        map
    }
}

impl IntoIterator for Map {
    type Item = (Literal, Option<Literal>);
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.pair.into_iter()
    }
}

impl<const N: usize> From<[(Literal, Option<Literal>); N]> for Map {
    fn from(value: [(Literal, Option<Literal>); N]) -> Self {
        value.iter().cloned().collect()
    }
}

/// Values present in iceberg type
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Literal {
    /// A primitive value
    Primitive(PrimitiveLiteral),
    /// A struct is a tuple of typed values. Each field in the tuple is named and has an integer id that is unique in the table schema.
    /// Each field can be either optional or required, meaning that values can (or cannot) be null. Fields may be any type.
    /// Fields may have an optional comment or doc string. Fields can have default values.
    Struct(Struct),
    /// A list is a collection of values with some element type.
    /// The element field has an integer id that is unique in the table schema.
    /// Elements can be either optional or required. Element types may be any type.
    List(Vec<Option<Literal>>),
    /// A map is a collection of key-value pairs with a key type and a value type.
    /// Both the key field and value field each have an integer id that is unique in the table schema.
    /// Map keys are required and map values can be either optional or required. Both map keys and map values may be any type, including nested types.
    Map(Map),
}

impl Literal {
    /// Creates a boolean value.
    ///
    /// Example:
    /// ```rust
    /// use iceberg::spec::{Literal, PrimitiveLiteral};
    /// let t = Literal::bool(true);
    ///
    /// assert_eq!(Literal::Primitive(PrimitiveLiteral::Boolean(true)), t);
    /// ```
    pub fn bool<T: Into<bool>>(t: T) -> Self {
        Self::Primitive(PrimitiveLiteral::Boolean(t.into()))
    }

    /// Creates a boolean value from string.
    /// See [Parse bool from str](https://doc.rust-lang.org/stable/std/primitive.bool.html#impl-FromStr-for-bool) for reference.
    ///
    /// Example:
    /// ```rust
    /// use iceberg::spec::{Literal, PrimitiveLiteral};
    /// let t = Literal::bool_from_str("false").unwrap();
    ///
    /// assert_eq!(Literal::Primitive(PrimitiveLiteral::Boolean(false)), t);
    /// ```
    pub fn bool_from_str<S: AsRef<str>>(s: S) -> Result<Self> {
        let v = s.as_ref().parse::<bool>().map_err(|e| {
            Error::new(ErrorKind::DataInvalid, "Can't parse string to bool.").with_source(e)
        })?;
        Ok(Self::Primitive(PrimitiveLiteral::Boolean(v)))
    }

    /// Creates an 32bit integer.
    ///
    /// Example:
    /// ```rust
    /// use iceberg::spec::{Literal, PrimitiveLiteral};
    /// let t = Literal::int(23i8);
    ///
    /// assert_eq!(Literal::Primitive(PrimitiveLiteral::Int(23)), t);
    /// ```
    pub fn int<T: Into<i32>>(t: T) -> Self {
        Self::Primitive(PrimitiveLiteral::Int(t.into()))
    }

    /// Creates an 64bit integer.
    ///
    /// Example:
    /// ```rust
    /// use iceberg::spec::{Literal, PrimitiveLiteral};
    /// let t = Literal::long(24i8);
    ///
    /// assert_eq!(Literal::Primitive(PrimitiveLiteral::Long(24)), t);
    /// ```
    pub fn long<T: Into<i64>>(t: T) -> Self {
        Self::Primitive(PrimitiveLiteral::Long(t.into()))
    }

    /// Creates an 32bit floating point number.
    ///
    /// Example:
    /// ```rust
    /// use iceberg::spec::{Literal, PrimitiveLiteral};
    /// use ordered_float::OrderedFloat;
    /// let t = Literal::float(32.1f32);
    ///
    /// assert_eq!(
    ///     Literal::Primitive(PrimitiveLiteral::Float(OrderedFloat(32.1))),
    ///     t
    /// );
    /// ```
    pub fn float<T: Into<f32>>(t: T) -> Self {
        Self::Primitive(PrimitiveLiteral::Float(OrderedFloat(t.into())))
    }

    /// Creates an 32bit floating point number.
    ///
    /// Example:
    /// ```rust
    /// use iceberg::spec::{Literal, PrimitiveLiteral};
    /// use ordered_float::OrderedFloat;
    /// let t = Literal::double(32.1f64);
    ///
    /// assert_eq!(
    ///     Literal::Primitive(PrimitiveLiteral::Double(OrderedFloat(32.1))),
    ///     t
    /// );
    /// ```
    pub fn double<T: Into<f64>>(t: T) -> Self {
        Self::Primitive(PrimitiveLiteral::Double(OrderedFloat(t.into())))
    }

    /// Creates date literal from number of days from unix epoch directly.
    pub fn date(days: i32) -> Self {
        Self::Primitive(PrimitiveLiteral::Int(days))
    }

    /// Creates a date in `%Y-%m-%d` format, assume in utc timezone.
    ///
    /// See [`NaiveDate::from_str`].
    ///
    /// Example
    /// ```rust
    /// use iceberg::spec::Literal;
    /// let t = Literal::date_from_str("1970-01-03").unwrap();
    ///
    /// assert_eq!(Literal::date(2), t);
    /// ```
    pub fn date_from_str<S: AsRef<str>>(s: S) -> Result<Self> {
        let t = s.as_ref().parse::<NaiveDate>().map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Can't parse date from string: {}", s.as_ref()),
            )
            .with_source(e)
        })?;

        Ok(Self::date(date_from_naive_date(t)))
    }

    /// Create a date from calendar date (year, month and day).
    ///
    /// See [`NaiveDate::from_ymd_opt`].
    ///
    /// Example:
    ///
    ///```rust
    /// use iceberg::spec::Literal;
    /// let t = Literal::date_from_ymd(1970, 1, 5).unwrap();
    ///
    /// assert_eq!(Literal::date(4), t);
    /// ```
    pub fn date_from_ymd(year: i32, month: u32, day: u32) -> Result<Self> {
        let t = NaiveDate::from_ymd_opt(year, month, day).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Can't create date from year: {year}, month: {month}, day: {day}"),
            )
        })?;

        Ok(Self::date(date_from_naive_date(t)))
    }

    /// Creates time in microseconds directly
    pub fn time(value: i64) -> Self {
        Self::Primitive(PrimitiveLiteral::Long(value))
    }

    /// Creates time literal from [`chrono::NaiveTime`].
    fn time_from_naive_time(t: NaiveTime) -> Self {
        let duration = t - unix_epoch().time();
        // It's safe to unwrap here since less than 24 hours will never overflow.
        let micro_secs = duration.num_microseconds().unwrap();

        Literal::time(micro_secs)
    }

    /// Creates time in microseconds in `%H:%M:%S:.f` format.
    ///
    /// See [`NaiveTime::from_str`] for details.
    ///
    /// Example:
    /// ```rust
    /// use iceberg::spec::Literal;
    /// let t = Literal::time_from_str("01:02:01.888999777").unwrap();
    ///
    /// let micro_secs = {
    ///     1 * 3600 * 1_000_000 + // 1 hour
    ///     2 * 60 * 1_000_000 +   // 2 minutes
    ///     1 * 1_000_000 + // 1 second
    ///     888999 // microseconds
    /// };
    /// assert_eq!(Literal::time(micro_secs), t);
    /// ```
    pub fn time_from_str<S: AsRef<str>>(s: S) -> Result<Self> {
        let t = s.as_ref().parse::<NaiveTime>().map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Can't parse time from string: {}", s.as_ref()),
            )
            .with_source(e)
        })?;

        Ok(Self::time_from_naive_time(t))
    }

    /// Creates time literal from hour, minute, second, and microseconds.
    ///
    /// See [`NaiveTime::from_hms_micro_opt`].
    ///
    /// Example:
    /// ```rust
    /// use iceberg::spec::Literal;
    /// let t = Literal::time_from_hms_micro(22, 15, 33, 111).unwrap();
    ///
    /// assert_eq!(Literal::time_from_str("22:15:33.000111").unwrap(), t);
    /// ```
    pub fn time_from_hms_micro(hour: u32, min: u32, sec: u32, micro: u32) -> Result<Self> {
        let t = NaiveTime::from_hms_micro_opt(hour, min, sec, micro)
            .ok_or_else(|| Error::new(
                ErrorKind::DataInvalid,
                format!("Can't create time from hour: {hour}, min: {min}, second: {sec}, microsecond: {micro}"),
            ))?;
        Ok(Self::time_from_naive_time(t))
    }

    /// Creates a timestamp from unix epoch in microseconds.
    pub fn timestamp(value: i64) -> Self {
        Self::Primitive(PrimitiveLiteral::Long(value))
    }

    /// Creates a timestamp with timezone from unix epoch in microseconds.
    pub fn timestamptz(value: i64) -> Self {
        Self::Primitive(PrimitiveLiteral::Long(value))
    }

    /// Creates a timestamp from unix epoch in nanoseconds.
    pub(crate) fn timestamp_nano(value: i64) -> Self {
        Self::Primitive(PrimitiveLiteral::Long(value))
    }

    /// Creates a timestamp with timezone from unix epoch in nanoseconds.
    pub(crate) fn timestamptz_nano(value: i64) -> Self {
        Self::Primitive(PrimitiveLiteral::Long(value))
    }

    /// Creates a timestamp from [`DateTime`].
    pub fn timestamp_from_datetime<T: TimeZone>(dt: DateTime<T>) -> Self {
        Self::timestamp(dt.with_timezone(&Utc).timestamp_micros())
    }

    /// Creates a timestamp with timezone from [`DateTime`].
    pub fn timestamptz_from_datetime<T: TimeZone>(dt: DateTime<T>) -> Self {
        Self::timestamptz(dt.with_timezone(&Utc).timestamp_micros())
    }

    /// Parse a timestamp in RFC3339 format.
    ///
    /// See [`DateTime<Utc>::from_str`].
    ///
    /// Example:
    ///
    /// ```rust
    /// use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
    /// use iceberg::spec::Literal;
    /// let t = Literal::timestamp_from_str("2012-12-12 12:12:12.8899-04:00").unwrap();
    ///
    /// let t2 = {
    ///     let date = NaiveDate::from_ymd_opt(2012, 12, 12).unwrap();
    ///     let time = NaiveTime::from_hms_micro_opt(12, 12, 12, 889900).unwrap();
    ///     let dt = NaiveDateTime::new(date, time);
    ///     Literal::timestamp_from_datetime(DateTime::<FixedOffset>::from_local(
    ///         dt,
    ///         FixedOffset::west_opt(4 * 3600).unwrap(),
    ///     ))
    /// };
    ///
    /// assert_eq!(t, t2);
    /// ```
    pub fn timestamp_from_str<S: AsRef<str>>(s: S) -> Result<Self> {
        let dt = DateTime::<Utc>::from_str(s.as_ref()).map_err(|e| {
            Error::new(ErrorKind::DataInvalid, "Can't parse datetime.").with_source(e)
        })?;

        Ok(Self::timestamp_from_datetime(dt))
    }

    /// Similar to [`Literal::timestamp_from_str`], but return timestamp with timezone literal.
    pub fn timestamptz_from_str<S: AsRef<str>>(s: S) -> Result<Self> {
        let dt = DateTime::<Utc>::from_str(s.as_ref()).map_err(|e| {
            Error::new(ErrorKind::DataInvalid, "Can't parse datetime.").with_source(e)
        })?;

        Ok(Self::timestamptz_from_datetime(dt))
    }

    /// Creates a string literal.
    pub fn string<S: ToString>(s: S) -> Self {
        Self::Primitive(PrimitiveLiteral::String(s.to_string()))
    }

    /// Creates uuid literal.
    pub fn uuid(uuid: Uuid) -> Self {
        Self::Primitive(PrimitiveLiteral::UInt128(uuid.as_u128()))
    }

    /// Creates uuid from str. See [`Uuid::parse_str`].
    ///
    /// Example:
    ///
    /// ```rust
    /// use iceberg::spec::Literal;
    /// use uuid::Uuid;
    /// let t1 = Literal::uuid_from_str("a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8").unwrap();
    /// let t2 = Literal::uuid(Uuid::from_u128_le(0xd8d7d6d5d4d3d2d1c2c1b2b1a4a3a2a1));
    ///
    /// assert_eq!(t1, t2);
    /// ```
    pub fn uuid_from_str<S: AsRef<str>>(s: S) -> Result<Self> {
        let uuid = Uuid::parse_str(s.as_ref()).map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Can't parse uuid from string: {}", s.as_ref()),
            )
            .with_source(e)
        })?;
        Ok(Self::uuid(uuid))
    }

    /// Creates a fixed literal from bytes.
    ///
    /// Example:
    ///
    /// ```rust
    /// use iceberg::spec::{Literal, PrimitiveLiteral};
    /// let t1 = Literal::fixed(vec![1u8, 2u8]);
    /// let t2 = Literal::Primitive(PrimitiveLiteral::Binary(vec![1u8, 2u8]));
    ///
    /// assert_eq!(t1, t2);
    /// ```
    pub fn fixed<I: IntoIterator<Item = u8>>(input: I) -> Self {
        Literal::Primitive(PrimitiveLiteral::Binary(input.into_iter().collect()))
    }

    /// Creates a binary literal from bytes.
    ///
    /// Example:
    ///
    /// ```rust
    /// use iceberg::spec::{Literal, PrimitiveLiteral};
    /// let t1 = Literal::binary(vec![1u8, 2u8]);
    /// let t2 = Literal::Primitive(PrimitiveLiteral::Binary(vec![1u8, 2u8]));
    ///
    /// assert_eq!(t1, t2);
    /// ```
    pub fn binary<I: IntoIterator<Item = u8>>(input: I) -> Self {
        Literal::Primitive(PrimitiveLiteral::Binary(input.into_iter().collect()))
    }

    /// Creates a decimal literal.
    pub fn decimal(decimal: i128) -> Self {
        Self::Primitive(PrimitiveLiteral::Int128(decimal))
    }

    /// Creates decimal literal from string. See [`Decimal::from_str_exact`].
    ///
    /// Example:
    ///
    /// ```rust
    /// use iceberg::spec::Literal;
    /// use rust_decimal::Decimal;
    /// let t1 = Literal::decimal(12345);
    /// let t2 = Literal::decimal_from_str("123.45").unwrap();
    ///
    /// assert_eq!(t1, t2);
    /// ```
    pub fn decimal_from_str<S: AsRef<str>>(s: S) -> Result<Self> {
        let decimal = Decimal::from_str_exact(s.as_ref()).map_err(|e| {
            Error::new(ErrorKind::DataInvalid, "Can't parse decimal.").with_source(e)
        })?;
        Ok(Self::decimal(decimal.mantissa()))
    }

    /// Attempts to convert the Literal to a PrimitiveLiteral
    pub fn as_primitive_literal(&self) -> Option<PrimitiveLiteral> {
        match self {
            Literal::Primitive(primitive) => Some(primitive.clone()),
            _ => None,
        }
    }
}

/// The partition struct stores the tuple of partition values for each file.
///
/// Its type is derived from the partition fields of the partition spec used to write the manifest file.
/// In v2, the partition struct’s field ids must match the ids from the partition spec.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Struct {
    /// Vector to store the field values
    fields: Vec<Literal>,
    /// Null bitmap
    null_bitmap: BitVec,
}

impl Struct {
    /// Create a empty struct.
    pub fn empty() -> Self {
        Self {
            fields: Vec::new(),
            null_bitmap: BitVec::new(),
        }
    }

    /// Create a iterator to read the field in order of field_value.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = Option<&Literal>> {
        self.null_bitmap.iter().zip(self.fields.iter()).map(
            |(null, value)| {
                if *null {
                    None
                } else {
                    Some(value)
                }
            },
        )
    }

    /// returns true if the field at position `index` is null
    pub fn is_null_at_index(&self, index: usize) -> bool {
        self.null_bitmap[index]
    }

    /// Return fields in the struct.
    pub fn fields(&self) -> &[Literal] {
        &self.fields
    }
}

impl Index<usize> for Struct {
    type Output = Literal;

    fn index(&self, idx: usize) -> &Self::Output {
        &self.fields[idx]
    }
}

/// An iterator that moves out of a struct.
pub struct StructValueIntoIter {
    null_bitmap: bitvec::boxed::IntoIter,
    fields: std::vec::IntoIter<Literal>,
}

impl Iterator for StructValueIntoIter {
    type Item = Option<Literal>;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.null_bitmap.next(), self.fields.next()) {
            (Some(null), Some(value)) => Some(if null { None } else { Some(value) }),
            _ => None,
        }
    }
}

impl IntoIterator for Struct {
    type Item = Option<Literal>;

    type IntoIter = StructValueIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        StructValueIntoIter {
            null_bitmap: self.null_bitmap.into_iter(),
            fields: self.fields.into_iter(),
        }
    }
}

impl FromIterator<Option<Literal>> for Struct {
    fn from_iter<I: IntoIterator<Item = Option<Literal>>>(iter: I) -> Self {
        let mut fields = Vec::new();
        let mut null_bitmap = BitVec::new();

        for value in iter.into_iter() {
            match value {
                Some(value) => {
                    fields.push(value);
                    null_bitmap.push(false)
                }
                None => {
                    fields.push(Literal::Primitive(PrimitiveLiteral::Boolean(false)));
                    null_bitmap.push(true)
                }
            }
        }
        Struct {
            fields,
            null_bitmap,
        }
    }
}

impl Literal {
    /// Create iceberg value from a json value
    ///
    /// See [this spec](https://iceberg.apache.org/spec/#json-single-value-serialization) for reference.
    pub fn try_from_json(value: JsonValue, data_type: &Type) -> Result<Option<Self>> {
        match data_type {
            Type::Primitive(primitive) => match (primitive, value) {
                (PrimitiveType::Boolean, JsonValue::Bool(bool)) => {
                    Ok(Some(Literal::Primitive(PrimitiveLiteral::Boolean(bool))))
                }
                (PrimitiveType::Int, JsonValue::Number(number)) => {
                    Ok(Some(Literal::Primitive(PrimitiveLiteral::Int(
                        number
                            .as_i64()
                            .ok_or(Error::new(
                                crate::ErrorKind::DataInvalid,
                                "Failed to convert json number to int",
                            ))?
                            .try_into()?,
                    ))))
                }
                (PrimitiveType::Long, JsonValue::Number(number)) => Ok(Some(Literal::Primitive(
                    PrimitiveLiteral::Long(number.as_i64().ok_or(Error::new(
                        crate::ErrorKind::DataInvalid,
                        "Failed to convert json number to long",
                    ))?),
                ))),
                (PrimitiveType::Float, JsonValue::Number(number)) => Ok(Some(Literal::Primitive(
                    PrimitiveLiteral::Float(OrderedFloat(number.as_f64().ok_or(Error::new(
                        crate::ErrorKind::DataInvalid,
                        "Failed to convert json number to float",
                    ))? as f32)),
                ))),
                (PrimitiveType::Double, JsonValue::Number(number)) => Ok(Some(Literal::Primitive(
                    PrimitiveLiteral::Double(OrderedFloat(number.as_f64().ok_or(Error::new(
                        crate::ErrorKind::DataInvalid,
                        "Failed to convert json number to double",
                    ))?)),
                ))),
                (PrimitiveType::Date, JsonValue::String(s)) => {
                    Ok(Some(Literal::Primitive(PrimitiveLiteral::Int(
                        date::date_to_days(&NaiveDate::parse_from_str(&s, "%Y-%m-%d")?),
                    ))))
                }
                (PrimitiveType::Time, JsonValue::String(s)) => {
                    Ok(Some(Literal::Primitive(PrimitiveLiteral::Long(
                        time::time_to_microseconds(&NaiveTime::parse_from_str(&s, "%H:%M:%S%.f")?),
                    ))))
                }
                (PrimitiveType::Timestamp, JsonValue::String(s)) => Ok(Some(Literal::Primitive(
                    PrimitiveLiteral::Long(timestamp::datetime_to_microseconds(
                        &NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.f")?,
                    )),
                ))),
                (PrimitiveType::Timestamptz, JsonValue::String(s)) => {
                    Ok(Some(Literal::Primitive(PrimitiveLiteral::Long(
                        timestamptz::datetimetz_to_microseconds(&Utc.from_utc_datetime(
                            &NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.f+00:00")?,
                        )),
                    ))))
                }
                (PrimitiveType::String, JsonValue::String(s)) => {
                    Ok(Some(Literal::Primitive(PrimitiveLiteral::String(s))))
                }
                (PrimitiveType::Uuid, JsonValue::String(s)) => Ok(Some(Literal::Primitive(
                    PrimitiveLiteral::UInt128(Uuid::parse_str(&s)?.as_u128()),
                ))),
                (PrimitiveType::Fixed(_), JsonValue::String(_)) => todo!(),
                (PrimitiveType::Binary, JsonValue::String(_)) => todo!(),
                (
                    PrimitiveType::Decimal {
                        precision: _,
                        scale,
                    },
                    JsonValue::String(s),
                ) => {
                    let mut decimal = Decimal::from_str_exact(&s)?;
                    decimal.rescale(*scale);
                    Ok(Some(Literal::Primitive(PrimitiveLiteral::Int128(
                        decimal.mantissa(),
                    ))))
                }
                (_, JsonValue::Null) => Ok(None),
                (i, j) => Err(Error::new(
                    crate::ErrorKind::DataInvalid,
                    format!(
                        "The json value {} doesn't fit to the iceberg type {}.",
                        j, i
                    ),
                )),
            },
            Type::Struct(schema) => {
                if let JsonValue::Object(mut object) = value {
                    Ok(Some(Literal::Struct(Struct::from_iter(
                        schema.fields().iter().map(|field| {
                            object.remove(&field.id.to_string()).and_then(|value| {
                                Literal::try_from_json(value, &field.field_type)
                                    .and_then(|value| {
                                        value.ok_or(Error::new(
                                            ErrorKind::DataInvalid,
                                            "Key of map cannot be null",
                                        ))
                                    })
                                    .ok()
                            })
                        }),
                    ))))
                } else {
                    Err(Error::new(
                        crate::ErrorKind::DataInvalid,
                        "The json value for a struct type must be an object.",
                    ))
                }
            }
            Type::List(list) => {
                if let JsonValue::Array(array) = value {
                    Ok(Some(Literal::List(
                        array
                            .into_iter()
                            .map(|value| {
                                Literal::try_from_json(value, &list.element_field.field_type)
                            })
                            .collect::<Result<Vec<_>>>()?,
                    )))
                } else {
                    Err(Error::new(
                        crate::ErrorKind::DataInvalid,
                        "The json value for a list type must be an array.",
                    ))
                }
            }
            Type::Map(map) => {
                if let JsonValue::Object(mut object) = value {
                    if let (Some(JsonValue::Array(keys)), Some(JsonValue::Array(values))) =
                        (object.remove("keys"), object.remove("values"))
                    {
                        Ok(Some(Literal::Map(Map::from_iter(
                            keys.into_iter()
                                .zip(values.into_iter())
                                .map(|(key, value)| {
                                    Ok((
                                        Literal::try_from_json(key, &map.key_field.field_type)
                                            .and_then(|value| {
                                                value.ok_or(Error::new(
                                                    ErrorKind::DataInvalid,
                                                    "Key of map cannot be null",
                                                ))
                                            })?,
                                        Literal::try_from_json(value, &map.value_field.field_type)?,
                                    ))
                                })
                                .collect::<Result<Vec<_>>>()?,
                        ))))
                    } else {
                        Err(Error::new(
                            crate::ErrorKind::DataInvalid,
                            "The json value for a list type must be an array.",
                        ))
                    }
                } else {
                    Err(Error::new(
                        crate::ErrorKind::DataInvalid,
                        "The json value for a list type must be an array.",
                    ))
                }
            }
        }
    }

    /// Converting iceberg value to json value.
    ///
    /// See [this spec](https://iceberg.apache.org/spec/#json-single-value-serialization) for reference.
    pub fn try_into_json(self, r#type: &Type) -> Result<JsonValue> {
        match (self, r#type) {
            (Literal::Primitive(prim), Type::Primitive(prim_type)) => match (prim_type, prim) {
                (PrimitiveType::Boolean, PrimitiveLiteral::Boolean(val)) => {
                    Ok(JsonValue::Bool(val))
                }
                (PrimitiveType::Int, PrimitiveLiteral::Int(val)) => {
                    Ok(JsonValue::Number((val).into()))
                }
                (PrimitiveType::Long, PrimitiveLiteral::Long(val)) => {
                    Ok(JsonValue::Number((val).into()))
                }
                (PrimitiveType::Float, PrimitiveLiteral::Float(val)) => {
                    match Number::from_f64(val.0 as f64) {
                        Some(number) => Ok(JsonValue::Number(number)),
                        None => Ok(JsonValue::Null),
                    }
                }
                (PrimitiveType::Double, PrimitiveLiteral::Double(val)) => {
                    match Number::from_f64(val.0) {
                        Some(number) => Ok(JsonValue::Number(number)),
                        None => Ok(JsonValue::Null),
                    }
                }
                (PrimitiveType::Date, PrimitiveLiteral::Int(val)) => {
                    Ok(JsonValue::String(date::days_to_date(val).to_string()))
                }
                (PrimitiveType::Time, PrimitiveLiteral::Long(val)) => Ok(JsonValue::String(
                    time::microseconds_to_time(val).to_string(),
                )),
                (PrimitiveType::Timestamp, PrimitiveLiteral::Long(val)) => Ok(JsonValue::String(
                    timestamp::microseconds_to_datetime(val)
                        .format("%Y-%m-%dT%H:%M:%S%.f")
                        .to_string(),
                )),
                (PrimitiveType::Timestamptz, PrimitiveLiteral::Long(val)) => Ok(JsonValue::String(
                    timestamptz::microseconds_to_datetimetz(val)
                        .format("%Y-%m-%dT%H:%M:%S%.f+00:00")
                        .to_string(),
                )),
                (PrimitiveType::TimestampNs, PrimitiveLiteral::Long(val)) => Ok(JsonValue::String(
                    timestamp::nanoseconds_to_datetime(val)
                        .format("%Y-%m-%dT%H:%M:%S%.f")
                        .to_string(),
                )),
                (PrimitiveType::TimestamptzNs, PrimitiveLiteral::Long(val)) => {
                    Ok(JsonValue::String(
                        timestamptz::nanoseconds_to_datetimetz(val)
                            .format("%Y-%m-%dT%H:%M:%S%.f+00:00")
                            .to_string(),
                    ))
                }
                (PrimitiveType::String, PrimitiveLiteral::String(val)) => {
                    Ok(JsonValue::String(val.clone()))
                }
                (_, PrimitiveLiteral::UInt128(val)) => {
                    Ok(JsonValue::String(Uuid::from_u128(val).to_string()))
                }
                (_, PrimitiveLiteral::Binary(val)) => Ok(JsonValue::String(val.iter().fold(
                    String::new(),
                    |mut acc, x| {
                        acc.push_str(&format!("{:x}", x));
                        acc
                    },
                ))),
                (_, PrimitiveLiteral::Int128(val)) => match r#type {
                    Type::Primitive(PrimitiveType::Decimal {
                        precision: _precision,
                        scale,
                    }) => {
                        let decimal = Decimal::try_from_i128_with_scale(val, *scale)?;
                        Ok(JsonValue::String(decimal.to_string()))
                    }
                    _ => Err(Error::new(
                        ErrorKind::DataInvalid,
                        "The iceberg type for decimal literal must be decimal.",
                    ))?,
                },
                _ => Err(Error::new(
                    ErrorKind::DataInvalid,
                    "The iceberg value doesn't fit to the iceberg type.",
                )),
            },
            (Literal::Struct(s), Type::Struct(struct_type)) => {
                let mut id_and_value = Vec::with_capacity(struct_type.fields().len());
                for (value, field) in s.into_iter().zip(struct_type.fields()) {
                    let json = match value {
                        Some(val) => val.try_into_json(&field.field_type)?,
                        None => JsonValue::Null,
                    };
                    id_and_value.push((field.id.to_string(), json));
                }
                Ok(JsonValue::Object(JsonMap::from_iter(id_and_value)))
            }
            (Literal::List(list), Type::List(list_type)) => Ok(JsonValue::Array(
                list.into_iter()
                    .map(|opt| match opt {
                        Some(literal) => literal.try_into_json(&list_type.element_field.field_type),
                        None => Ok(JsonValue::Null),
                    })
                    .collect::<Result<Vec<JsonValue>>>()?,
            )),
            (Literal::Map(map), Type::Map(map_type)) => {
                let mut object = JsonMap::with_capacity(2);
                let mut json_keys = Vec::with_capacity(map.len());
                let mut json_values = Vec::with_capacity(map.len());
                for (key, value) in map.into_iter() {
                    json_keys.push(key.try_into_json(&map_type.key_field.field_type)?);
                    json_values.push(match value {
                        Some(literal) => literal.try_into_json(&map_type.value_field.field_type)?,
                        None => JsonValue::Null,
                    });
                }
                object.insert("keys".to_string(), JsonValue::Array(json_keys));
                object.insert("values".to_string(), JsonValue::Array(json_values));
                Ok(JsonValue::Object(object))
            }
            (value, r#type) => Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "The iceberg value {:?} doesn't fit to the iceberg type {}.",
                    value, r#type
                ),
            )),
        }
    }

    /// Convert Value to the any type
    pub fn into_any(self) -> Box<dyn Any> {
        match self {
            Literal::Primitive(prim) => match prim {
                PrimitiveLiteral::Boolean(any) => Box::new(any),
                PrimitiveLiteral::Int(any) => Box::new(any),
                PrimitiveLiteral::Long(any) => Box::new(any),
                PrimitiveLiteral::Float(any) => Box::new(any),
                PrimitiveLiteral::Double(any) => Box::new(any),
                PrimitiveLiteral::Binary(any) => Box::new(any),
                PrimitiveLiteral::String(any) => Box::new(any),
                PrimitiveLiteral::UInt128(any) => Box::new(any),
                PrimitiveLiteral::Int128(any) => Box::new(any),
                PrimitiveLiteral::AboveMax | PrimitiveLiteral::BelowMin => unimplemented!(),
            },
            _ => unimplemented!(),
        }
    }
}

mod date {
    use chrono::{DateTime, NaiveDate, TimeDelta, TimeZone, Utc};

    pub(crate) fn date_to_days(date: &NaiveDate) -> i32 {
        date.signed_duration_since(
            // This is always the same and shouldn't fail
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
        )
        .num_days() as i32
    }

    pub(crate) fn days_to_date(days: i32) -> NaiveDate {
        // This shouldn't fail until the year 262000
        (chrono::DateTime::UNIX_EPOCH + TimeDelta::try_days(days as i64).unwrap())
            .naive_utc()
            .date()
    }

    /// Returns unix epoch.
    pub(crate) fn unix_epoch() -> DateTime<Utc> {
        Utc.timestamp_nanos(0)
    }

    /// Creates date literal from `NaiveDate`, assuming it's utc timezone.
    pub(crate) fn date_from_naive_date(date: NaiveDate) -> i32 {
        (date - unix_epoch().date_naive()).num_days() as i32
    }
}

mod time {
    use chrono::NaiveTime;

    pub(crate) fn time_to_microseconds(time: &NaiveTime) -> i64 {
        time.signed_duration_since(
            // This is always the same and shouldn't fail
            NaiveTime::from_num_seconds_from_midnight_opt(0, 0).unwrap(),
        )
        .num_microseconds()
        .unwrap()
    }

    pub(crate) fn microseconds_to_time(micros: i64) -> NaiveTime {
        let (secs, rem) = (micros / 1_000_000, micros % 1_000_000);

        NaiveTime::from_num_seconds_from_midnight_opt(secs as u32, rem as u32 * 1_000).unwrap()
    }
}

mod timestamp {
    use chrono::{DateTime, NaiveDateTime};

    pub(crate) fn datetime_to_microseconds(time: &NaiveDateTime) -> i64 {
        time.and_utc().timestamp_micros()
    }

    pub(crate) fn microseconds_to_datetime(micros: i64) -> NaiveDateTime {
        // This shouldn't fail until the year 262000
        DateTime::from_timestamp_micros(micros).unwrap().naive_utc()
    }

    pub(crate) fn nanoseconds_to_datetime(nanos: i64) -> NaiveDateTime {
        DateTime::from_timestamp_nanos(nanos).naive_utc()
    }
}

mod timestamptz {
    use chrono::{DateTime, Utc};

    pub(crate) fn datetimetz_to_microseconds(time: &DateTime<Utc>) -> i64 {
        time.timestamp_micros()
    }

    pub(crate) fn microseconds_to_datetimetz(micros: i64) -> DateTime<Utc> {
        let (secs, rem) = (micros / 1_000_000, micros % 1_000_000);

        DateTime::from_timestamp(secs, rem as u32 * 1_000).unwrap()
    }

    pub(crate) fn nanoseconds_to_datetimetz(nanos: i64) -> DateTime<Utc> {
        let (secs, rem) = (nanos / 1_000_000_000, nanos % 1_000_000_000);

        DateTime::from_timestamp(secs, rem as u32).unwrap()
    }
}

mod _serde {
    use serde::de::Visitor;
    use serde::ser::{SerializeMap, SerializeSeq, SerializeStruct};
    use serde::{Deserialize, Serialize};
    use serde_bytes::ByteBuf;
    use serde_derive::{Deserialize as DeserializeDerive, Serialize as SerializeDerive};

    use super::{Literal, Map, PrimitiveLiteral};
    use crate::spec::{PrimitiveType, Type, MAP_KEY_FIELD_NAME, MAP_VALUE_FIELD_NAME};
    use crate::{Error, ErrorKind};

    #[derive(SerializeDerive, DeserializeDerive, Debug)]
    #[serde(transparent)]
    /// Raw literal representation used for serde. The serialize way is used for Avro serializer.
    pub struct RawLiteral(RawLiteralEnum);

    impl RawLiteral {
        /// Covert literal to raw literal.
        pub fn try_from(literal: Literal, ty: &Type) -> Result<Self, Error> {
            Ok(Self(RawLiteralEnum::try_from(literal, ty)?))
        }

        /// Convert raw literal to literal.
        pub fn try_into(self, ty: &Type) -> Result<Option<Literal>, Error> {
            self.0.try_into(ty)
        }
    }

    #[derive(SerializeDerive, Clone, Debug)]
    #[serde(untagged)]
    enum RawLiteralEnum {
        Null,
        Boolean(bool),
        Int(i32),
        Long(i64),
        Float(f32),
        Double(f64),
        String(String),
        Bytes(ByteBuf),
        List(List),
        StringMap(StringMap),
        Record(Record),
    }

    #[derive(Clone, Debug)]
    struct Record {
        required: Vec<(String, RawLiteralEnum)>,
        optional: Vec<(String, Option<RawLiteralEnum>)>,
    }

    impl Serialize for Record {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: serde::Serializer {
            let len = self.required.len() + self.optional.len();
            let mut record = serializer.serialize_struct("", len)?;
            for (k, v) in &self.required {
                record.serialize_field(Box::leak(k.clone().into_boxed_str()), &v)?;
            }
            for (k, v) in &self.optional {
                record.serialize_field(Box::leak(k.clone().into_boxed_str()), &v)?;
            }
            record.end()
        }
    }

    #[derive(Clone, Debug)]
    struct List {
        list: Vec<Option<RawLiteralEnum>>,
        required: bool,
    }

    impl Serialize for List {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: serde::Serializer {
            let mut seq = serializer.serialize_seq(Some(self.list.len()))?;
            for value in &self.list {
                if self.required {
                    seq.serialize_element(value.as_ref().ok_or_else(|| {
                        serde::ser::Error::custom(
                            "List element is required, element cannot be null",
                        )
                    })?)?;
                } else {
                    seq.serialize_element(&value)?;
                }
            }
            seq.end()
        }
    }

    #[derive(Clone, Debug)]
    struct StringMap {
        raw: Vec<(String, Option<RawLiteralEnum>)>,
        required: bool,
    }

    impl Serialize for StringMap {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: serde::Serializer {
            let mut map = serializer.serialize_map(Some(self.raw.len()))?;
            for (k, v) in &self.raw {
                if self.required {
                    map.serialize_entry(
                        k,
                        v.as_ref().ok_or_else(|| {
                            serde::ser::Error::custom(
                                "Map element is required, element cannot be null",
                            )
                        })?,
                    )?;
                } else {
                    map.serialize_entry(k, v)?;
                }
            }
            map.end()
        }
    }

    impl<'de> Deserialize<'de> for RawLiteralEnum {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: serde::Deserializer<'de> {
            struct RawLiteralVisitor;
            impl<'de> Visitor<'de> for RawLiteralVisitor {
                type Value = RawLiteralEnum;

                fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                    formatter.write_str("expect")
                }

                fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
                where E: serde::de::Error {
                    Ok(RawLiteralEnum::Boolean(v))
                }

                fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E>
                where E: serde::de::Error {
                    Ok(RawLiteralEnum::Int(v))
                }

                fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
                where E: serde::de::Error {
                    Ok(RawLiteralEnum::Long(v))
                }

                /// Used in json
                fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
                where E: serde::de::Error {
                    Ok(RawLiteralEnum::Long(v as i64))
                }

                fn visit_f32<E>(self, v: f32) -> Result<Self::Value, E>
                where E: serde::de::Error {
                    Ok(RawLiteralEnum::Float(v))
                }

                fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
                where E: serde::de::Error {
                    Ok(RawLiteralEnum::Double(v))
                }

                fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
                where E: serde::de::Error {
                    Ok(RawLiteralEnum::String(v.to_string()))
                }

                fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
                where E: serde::de::Error {
                    Ok(RawLiteralEnum::Bytes(ByteBuf::from(v)))
                }

                fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
                where E: serde::de::Error {
                    Ok(RawLiteralEnum::String(v.to_string()))
                }

                fn visit_unit<E>(self) -> Result<Self::Value, E>
                where E: serde::de::Error {
                    Ok(RawLiteralEnum::Null)
                }

                fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
                where A: serde::de::MapAccess<'de> {
                    let mut required = Vec::new();
                    while let Some(key) = map.next_key::<String>()? {
                        let value = map.next_value::<RawLiteralEnum>()?;
                        required.push((key, value));
                    }
                    Ok(RawLiteralEnum::Record(Record {
                        required,
                        optional: Vec::new(),
                    }))
                }

                fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
                where A: serde::de::SeqAccess<'de> {
                    let mut list = Vec::new();
                    while let Some(value) = seq.next_element::<RawLiteralEnum>()? {
                        list.push(Some(value));
                    }
                    Ok(RawLiteralEnum::List(List {
                        list,
                        // `required` only used in serialize, just set default in deserialize.
                        required: false,
                    }))
                }
            }
            deserializer.deserialize_any(RawLiteralVisitor)
        }
    }

    impl RawLiteralEnum {
        pub fn try_from(literal: Literal, ty: &Type) -> Result<Self, Error> {
            let raw = match literal {
                Literal::Primitive(prim) => match prim {
                    super::PrimitiveLiteral::Boolean(v) => RawLiteralEnum::Boolean(v),
                    super::PrimitiveLiteral::Int(v) => RawLiteralEnum::Int(v),
                    super::PrimitiveLiteral::Long(v) => RawLiteralEnum::Long(v),
                    super::PrimitiveLiteral::Float(v) => RawLiteralEnum::Float(v.0),
                    super::PrimitiveLiteral::Double(v) => RawLiteralEnum::Double(v.0),
                    super::PrimitiveLiteral::String(v) => RawLiteralEnum::String(v),
                    super::PrimitiveLiteral::UInt128(v) => {
                        RawLiteralEnum::Bytes(ByteBuf::from(v.to_be_bytes()))
                    }
                    super::PrimitiveLiteral::Binary(v) => RawLiteralEnum::Bytes(ByteBuf::from(v)),
                    super::PrimitiveLiteral::Int128(v) => {
                        RawLiteralEnum::Bytes(ByteBuf::from(v.to_be_bytes()))
                    }
                    super::PrimitiveLiteral::AboveMax | super::PrimitiveLiteral::BelowMin => {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            "Can't convert AboveMax or BelowMax",
                        ));
                    }
                },
                Literal::Struct(r#struct) => {
                    let mut required = Vec::new();
                    let mut optional = Vec::new();
                    if let Type::Struct(struct_ty) = ty {
                        for (value, field) in r#struct.into_iter().zip(struct_ty.fields()) {
                            if field.required {
                                if let Some(value) = value {
                                    required.push((
                                        field.name.clone(),
                                        RawLiteralEnum::try_from(value, &field.field_type)?,
                                    ));
                                } else {
                                    return Err(Error::new(
                                        ErrorKind::DataInvalid,
                                        "Can't convert null to required field",
                                    ));
                                }
                            } else if let Some(value) = value {
                                optional.push((
                                    field.name.clone(),
                                    Some(RawLiteralEnum::try_from(value, &field.field_type)?),
                                ));
                            } else {
                                optional.push((field.name.clone(), None));
                            }
                        }
                    } else {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!("Type {} should be a struct", ty),
                        ));
                    }
                    RawLiteralEnum::Record(Record { required, optional })
                }
                Literal::List(list) => {
                    if let Type::List(list_ty) = ty {
                        let list = list
                            .into_iter()
                            .map(|v| {
                                v.map(|v| {
                                    RawLiteralEnum::try_from(v, &list_ty.element_field.field_type)
                                })
                                .transpose()
                            })
                            .collect::<Result<_, Error>>()?;
                        RawLiteralEnum::List(List {
                            list,
                            required: list_ty.element_field.required,
                        })
                    } else {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!("Type {} should be a list", ty),
                        ));
                    }
                }
                Literal::Map(map) => {
                    if let Type::Map(map_ty) = ty {
                        if let Type::Primitive(PrimitiveType::String) = *map_ty.key_field.field_type
                        {
                            let mut raw = Vec::with_capacity(map.len());
                            for (k, v) in map {
                                if let Literal::Primitive(PrimitiveLiteral::String(k)) = k {
                                    raw.push((
                                        k,
                                        v.map(|v| {
                                            RawLiteralEnum::try_from(
                                                v,
                                                &map_ty.value_field.field_type,
                                            )
                                        })
                                        .transpose()?,
                                    ));
                                } else {
                                    return Err(Error::new(
                                        ErrorKind::DataInvalid,
                                        "literal type is inconsistent with type",
                                    ));
                                }
                            }
                            RawLiteralEnum::StringMap(StringMap {
                                raw,
                                required: map_ty.value_field.required,
                            })
                        } else {
                            let list = map.into_iter().map(|(k,v)| {
                                let raw_k =
                                    RawLiteralEnum::try_from(k, &map_ty.key_field.field_type)?;
                                let raw_v = v
                                    .map(|v| {
                                        RawLiteralEnum::try_from(v, &map_ty.value_field.field_type)
                                    })
                                    .transpose()?;
                                if map_ty.value_field.required {
                                    Ok(Some(RawLiteralEnum::Record(Record {
                                        required: vec![
                                            (MAP_KEY_FIELD_NAME.to_string(), raw_k),
                                            (MAP_VALUE_FIELD_NAME.to_string(), raw_v.ok_or_else(||Error::new(ErrorKind::DataInvalid, "Map value is required, value cannot be null"))?),
                                        ],
                                        optional: vec![],
                                    })))
                                } else {
                                    Ok(Some(RawLiteralEnum::Record(Record {
                                        required: vec![
                                            (MAP_KEY_FIELD_NAME.to_string(), raw_k),
                                        ],
                                        optional: vec![
                                            (MAP_VALUE_FIELD_NAME.to_string(), raw_v)
                                        ],
                                    })))
                                }
                            }).collect::<Result<_, Error>>()?;
                            RawLiteralEnum::List(List {
                                list,
                                required: true,
                            })
                        }
                    } else {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!("Type {} should be a map", ty),
                        ));
                    }
                }
            };
            Ok(raw)
        }

        pub fn try_into(self, ty: &Type) -> Result<Option<Literal>, Error> {
            let invalid_err = |v: &str| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Unable to convert raw literal ({}) fail convert to type {} for: type mismatch",
                        v, ty
                    ),
                )
            };
            let invalid_err_with_reason = |v: &str, reason: &str| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Unable to convert raw literal ({}) fail convert to type {} for: {}",
                        v, ty, reason
                    ),
                )
            };
            match self {
                RawLiteralEnum::Null => Ok(None),
                RawLiteralEnum::Boolean(v) => Ok(Some(Literal::bool(v))),
                RawLiteralEnum::Int(v) => match ty {
                    Type::Primitive(PrimitiveType::Int) => Ok(Some(Literal::int(v))),
                    Type::Primitive(PrimitiveType::Long) => Ok(Some(Literal::long(i64::from(v)))),
                    Type::Primitive(PrimitiveType::Date) => Ok(Some(Literal::date(v))),
                    _ => Err(invalid_err("int")),
                },
                RawLiteralEnum::Long(v) => match ty {
                    Type::Primitive(PrimitiveType::Int) => Ok(Some(Literal::int(
                        i32::try_from(v).map_err(|_| invalid_err("long"))?,
                    ))),
                    Type::Primitive(PrimitiveType::Date) => Ok(Some(Literal::date(
                        i32::try_from(v).map_err(|_| invalid_err("long"))?,
                    ))),
                    Type::Primitive(PrimitiveType::Long) => Ok(Some(Literal::long(v))),
                    Type::Primitive(PrimitiveType::Time) => Ok(Some(Literal::time(v))),
                    Type::Primitive(PrimitiveType::Timestamp) => Ok(Some(Literal::timestamp(v))),
                    Type::Primitive(PrimitiveType::Timestamptz) => {
                        Ok(Some(Literal::timestamptz(v)))
                    }
                    _ => Err(invalid_err("long")),
                },
                RawLiteralEnum::Float(v) => match ty {
                    Type::Primitive(PrimitiveType::Float) => Ok(Some(Literal::float(v))),
                    Type::Primitive(PrimitiveType::Double) => {
                        Ok(Some(Literal::double(f64::from(v))))
                    }
                    _ => Err(invalid_err("float")),
                },
                RawLiteralEnum::Double(v) => match ty {
                    Type::Primitive(PrimitiveType::Float) => {
                        let v_32 = v as f32;
                        if v_32.is_finite() {
                            let v_64 = f64::from(v_32);
                            if (v_64 - v).abs() > f32::EPSILON as f64 {
                                // there is a precision loss
                                return Err(invalid_err("double"));
                            }
                        }
                        Ok(Some(Literal::float(v_32)))
                    }
                    Type::Primitive(PrimitiveType::Double) => Ok(Some(Literal::double(v))),
                    _ => Err(invalid_err("double")),
                },
                RawLiteralEnum::String(v) => match ty {
                    Type::Primitive(PrimitiveType::String) => Ok(Some(Literal::string(v))),
                    _ => Err(invalid_err("string")),
                },
                // # TODO:https://github.com/apache/iceberg-rust/issues/86
                // rust avro don't support deserialize any bytes representation now.
                RawLiteralEnum::Bytes(_) => Err(invalid_err_with_reason(
                    "bytes",
                    "todo: rust avro doesn't support deserialize any bytes representation now",
                )),
                RawLiteralEnum::List(v) => {
                    match ty {
                        Type::List(ty) => Ok(Some(Literal::List(
                            v.list
                                .into_iter()
                                .map(|v| {
                                    if let Some(v) = v {
                                        v.try_into(&ty.element_field.field_type)
                                    } else {
                                        Ok(None)
                                    }
                                })
                                .collect::<Result<_, Error>>()?,
                        ))),
                        Type::Map(map_ty) => {
                            let key_ty = map_ty.key_field.field_type.as_ref();
                            let value_ty = map_ty.value_field.field_type.as_ref();
                            let mut map = Map::new();
                            for k_v in v.list {
                                let k_v = k_v.ok_or_else(|| invalid_err_with_reason("list","In deserialize, None will be represented as Some(RawLiteral::Null), all element in list must be valid"))?;
                                if let RawLiteralEnum::Record(Record {
                                    required,
                                    optional: _,
                                }) = k_v
                                {
                                    if required.len() != 2 {
                                        return Err(invalid_err_with_reason("list","Record must contains two element(key and value) of array"));
                                    }
                                    let mut key = None;
                                    let mut value = None;
                                    required.into_iter().for_each(|(k, v)| {
                                        if k == MAP_KEY_FIELD_NAME {
                                            key = Some(v);
                                        } else if k == MAP_VALUE_FIELD_NAME {
                                            value = Some(v);
                                        }
                                    });
                                    match (key, value) {
                                        (Some(k), Some(v)) => {
                                            let key = k.try_into(key_ty)?.ok_or_else(|| {
                                                invalid_err_with_reason(
                                                    "list",
                                                    "Key element in Map must be valid",
                                                )
                                            })?;
                                            let value = v.try_into(value_ty)?;
                                            if map_ty.value_field.required && value.is_none() {
                                                return Err(invalid_err_with_reason(
                                                    "list",
                                                    "Value element is required in this Map",
                                                ));
                                            }
                                            map.insert(key, value);
                                        }
                                        _ => return Err(invalid_err_with_reason(
                                            "list",
                                            "The elements of record in list are not key and value",
                                        )),
                                    }
                                } else {
                                    return Err(invalid_err_with_reason(
                                        "list",
                                        "Map should represented as record array.",
                                    ));
                                }
                            }
                            Ok(Some(Literal::Map(map)))
                        }
                        Type::Primitive(PrimitiveType::Uuid) => {
                            if v.list.len() != 16 {
                                return Err(invalid_err_with_reason(
                                    "list",
                                    "The length of list should be 16",
                                ));
                            }
                            let mut bytes = [0u8; 16];
                            for (i, v) in v.list.iter().enumerate() {
                                if let Some(RawLiteralEnum::Long(v)) = v {
                                    bytes[i] = *v as u8;
                                } else {
                                    return Err(invalid_err_with_reason(
                                        "list",
                                        "The element of list should be int",
                                    ));
                                }
                            }
                            Ok(Some(Literal::uuid(uuid::Uuid::from_bytes(bytes))))
                        }
                        Type::Primitive(PrimitiveType::Decimal {
                            precision: _,
                            scale: _,
                        }) => {
                            if v.list.len() != 16 {
                                return Err(invalid_err_with_reason(
                                    "list",
                                    "The length of list should be 16",
                                ));
                            }
                            let mut bytes = [0u8; 16];
                            for (i, v) in v.list.iter().enumerate() {
                                if let Some(RawLiteralEnum::Long(v)) = v {
                                    bytes[i] = *v as u8;
                                } else {
                                    return Err(invalid_err_with_reason(
                                        "list",
                                        "The element of list should be int",
                                    ));
                                }
                            }
                            Ok(Some(Literal::decimal(i128::from_be_bytes(bytes))))
                        }
                        Type::Primitive(PrimitiveType::Binary) => {
                            let bytes = v
                                .list
                                .into_iter()
                                .map(|v| {
                                    if let Some(RawLiteralEnum::Long(v)) = v {
                                        Ok(v as u8)
                                    } else {
                                        Err(invalid_err_with_reason(
                                            "list",
                                            "The element of list should be int",
                                        ))
                                    }
                                })
                                .collect::<Result<Vec<_>, Error>>()?;
                            Ok(Some(Literal::binary(bytes)))
                        }
                        Type::Primitive(PrimitiveType::Fixed(size)) => {
                            if v.list.len() != *size as usize {
                                return Err(invalid_err_with_reason(
                                    "list",
                                    "The length of list should be equal to size",
                                ));
                            }
                            let bytes = v
                                .list
                                .into_iter()
                                .map(|v| {
                                    if let Some(RawLiteralEnum::Long(v)) = v {
                                        Ok(v as u8)
                                    } else {
                                        Err(invalid_err_with_reason(
                                            "list",
                                            "The element of list should be int",
                                        ))
                                    }
                                })
                                .collect::<Result<Vec<_>, Error>>()?;
                            Ok(Some(Literal::fixed(bytes)))
                        }
                        _ => Err(invalid_err("list")),
                    }
                }
                RawLiteralEnum::Record(Record {
                    required,
                    optional: _,
                }) => match ty {
                    Type::Struct(struct_ty) => {
                        let iters: Vec<Option<Literal>> = required
                            .into_iter()
                            .map(|(field_name, value)| {
                                let field = struct_ty
                                    .field_by_name(field_name.as_str())
                                    .ok_or_else(|| {
                                        invalid_err_with_reason(
                                            "record",
                                            &format!("field {} is not exist", &field_name),
                                        )
                                    })?;
                                let value = value.try_into(&field.field_type)?;
                                Ok(value)
                            })
                            .collect::<Result<_, Error>>()?;
                        Ok(Some(Literal::Struct(super::Struct::from_iter(iters))))
                    }
                    Type::Map(map_ty) => {
                        if *map_ty.key_field.field_type != Type::Primitive(PrimitiveType::String) {
                            return Err(invalid_err_with_reason(
                                "record",
                                "Map key must be string",
                            ));
                        }
                        let mut map = Map::new();
                        for (k, v) in required {
                            let value = v.try_into(&map_ty.value_field.field_type)?;
                            if map_ty.value_field.required && value.is_none() {
                                return Err(invalid_err_with_reason(
                                    "record",
                                    "Value element is required in this Map",
                                ));
                            }
                            map.insert(Literal::string(k), value);
                        }
                        Ok(Some(Literal::Map(map)))
                    }
                    _ => Err(invalid_err("record")),
                },
                RawLiteralEnum::StringMap(_) => Err(invalid_err("string map")),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use apache_avro::to_value;
    use apache_avro::types::Value;

    use super::*;
    use crate::avro::schema_to_avro_schema;
    use crate::spec::datatypes::{ListType, MapType, NestedField, StructType};
    use crate::spec::Schema;
    use crate::spec::Type::Primitive;

    fn check_json_serde(json: &str, expected_literal: Literal, expected_type: &Type) {
        let raw_json_value = serde_json::from_str::<JsonValue>(json).unwrap();
        let desered_literal =
            Literal::try_from_json(raw_json_value.clone(), expected_type).unwrap();
        assert_eq!(desered_literal, Some(expected_literal.clone()));

        let expected_json_value: JsonValue = expected_literal.try_into_json(expected_type).unwrap();
        let sered_json = serde_json::to_string(&expected_json_value).unwrap();
        let parsed_json_value = serde_json::from_str::<JsonValue>(&sered_json).unwrap();

        assert_eq!(parsed_json_value, raw_json_value);
    }

    fn check_avro_bytes_serde(
        input: Vec<u8>,
        expected_datum: Datum,
        expected_type: &PrimitiveType,
    ) {
        let raw_schema = r#""bytes""#;
        let schema = apache_avro::Schema::parse_str(raw_schema).unwrap();

        let bytes = ByteBuf::from(input);
        let datum = Datum::try_from_bytes(&bytes, expected_type.clone()).unwrap();
        assert_eq!(datum, expected_datum);

        let mut writer = apache_avro::Writer::new(&schema, Vec::new());
        writer.append_ser(datum.to_bytes().unwrap()).unwrap();
        let encoded = writer.into_inner().unwrap();
        let reader = apache_avro::Reader::with_schema(&schema, &*encoded).unwrap();

        for record in reader {
            let result = apache_avro::from_value::<ByteBuf>(&record.unwrap()).unwrap();
            let desered_datum = Datum::try_from_bytes(&result, expected_type.clone()).unwrap();
            assert_eq!(desered_datum, expected_datum);
        }
    }

    fn check_convert_with_avro(expected_literal: Literal, expected_type: &Type) {
        let fields = vec![NestedField::required(1, "col", expected_type.clone()).into()];
        let schema = Schema::builder()
            .with_fields(fields.clone())
            .build()
            .unwrap();
        let avro_schema = schema_to_avro_schema("test", &schema).unwrap();
        let struct_type = Type::Struct(StructType::new(fields));
        let struct_literal =
            Literal::Struct(Struct::from_iter(vec![Some(expected_literal.clone())]));

        let mut writer = apache_avro::Writer::new(&avro_schema, Vec::new());
        let raw_literal = RawLiteral::try_from(struct_literal.clone(), &struct_type).unwrap();
        writer.append_ser(raw_literal).unwrap();
        let encoded = writer.into_inner().unwrap();

        let reader = apache_avro::Reader::new(&*encoded).unwrap();
        for record in reader {
            let result = apache_avro::from_value::<RawLiteral>(&record.unwrap()).unwrap();
            let desered_literal = result.try_into(&struct_type).unwrap().unwrap();
            assert_eq!(desered_literal, struct_literal);
        }
    }

    fn check_serialize_avro(literal: Literal, ty: &Type, expect_value: Value) {
        let expect_value = Value::Record(vec![("col".to_string(), expect_value)]);

        let fields = vec![NestedField::required(1, "col", ty.clone()).into()];
        let schema = Schema::builder()
            .with_fields(fields.clone())
            .build()
            .unwrap();
        let avro_schema = schema_to_avro_schema("test", &schema).unwrap();
        let struct_type = Type::Struct(StructType::new(fields));
        let struct_literal = Literal::Struct(Struct::from_iter(vec![Some(literal.clone())]));
        let mut writer = apache_avro::Writer::new(&avro_schema, Vec::new());
        let raw_literal = RawLiteral::try_from(struct_literal.clone(), &struct_type).unwrap();
        let value = to_value(raw_literal)
            .unwrap()
            .resolve(&avro_schema)
            .unwrap();
        writer.append_value_ref(&value).unwrap();
        let encoded = writer.into_inner().unwrap();

        let reader = apache_avro::Reader::new(&*encoded).unwrap();
        for record in reader {
            assert_eq!(record.unwrap(), expect_value);
        }
    }

    #[test]
    fn json_boolean() {
        let record = r#"true"#;

        check_json_serde(
            record,
            Literal::Primitive(PrimitiveLiteral::Boolean(true)),
            &Type::Primitive(PrimitiveType::Boolean),
        );
    }

    #[test]
    fn json_int() {
        let record = r#"32"#;

        check_json_serde(
            record,
            Literal::Primitive(PrimitiveLiteral::Int(32)),
            &Type::Primitive(PrimitiveType::Int),
        );
    }

    #[test]
    fn json_long() {
        let record = r#"32"#;

        check_json_serde(
            record,
            Literal::Primitive(PrimitiveLiteral::Long(32)),
            &Type::Primitive(PrimitiveType::Long),
        );
    }

    #[test]
    fn json_float() {
        let record = r#"1.0"#;

        check_json_serde(
            record,
            Literal::Primitive(PrimitiveLiteral::Float(OrderedFloat(1.0))),
            &Type::Primitive(PrimitiveType::Float),
        );
    }

    #[test]
    fn json_double() {
        let record = r#"1.0"#;

        check_json_serde(
            record,
            Literal::Primitive(PrimitiveLiteral::Double(OrderedFloat(1.0))),
            &Type::Primitive(PrimitiveType::Double),
        );
    }

    #[test]
    fn json_date() {
        let record = r#""2017-11-16""#;

        check_json_serde(
            record,
            Literal::Primitive(PrimitiveLiteral::Int(17486)),
            &Type::Primitive(PrimitiveType::Date),
        );
    }

    #[test]
    fn json_time() {
        let record = r#""22:31:08.123456""#;

        check_json_serde(
            record,
            Literal::Primitive(PrimitiveLiteral::Long(81068123456)),
            &Type::Primitive(PrimitiveType::Time),
        );
    }

    #[test]
    fn json_timestamp() {
        let record = r#""2017-11-16T22:31:08.123456""#;

        check_json_serde(
            record,
            Literal::Primitive(PrimitiveLiteral::Long(1510871468123456)),
            &Type::Primitive(PrimitiveType::Timestamp),
        );
    }

    #[test]
    fn json_timestamptz() {
        let record = r#""2017-11-16T22:31:08.123456+00:00""#;

        check_json_serde(
            record,
            Literal::Primitive(PrimitiveLiteral::Long(1510871468123456)),
            &Type::Primitive(PrimitiveType::Timestamptz),
        );
    }

    #[test]
    fn json_string() {
        let record = r#""iceberg""#;

        check_json_serde(
            record,
            Literal::Primitive(PrimitiveLiteral::String("iceberg".to_string())),
            &Type::Primitive(PrimitiveType::String),
        );
    }

    #[test]
    fn json_uuid() {
        let record = r#""f79c3e09-677c-4bbd-a479-3f349cb785e7""#;

        check_json_serde(
            record,
            Literal::Primitive(PrimitiveLiteral::UInt128(
                Uuid::parse_str("f79c3e09-677c-4bbd-a479-3f349cb785e7")
                    .unwrap()
                    .as_u128(),
            )),
            &Type::Primitive(PrimitiveType::Uuid),
        );
    }

    #[test]
    fn json_decimal() {
        let record = r#""14.20""#;

        check_json_serde(
            record,
            Literal::Primitive(PrimitiveLiteral::Int128(1420)),
            &Type::decimal(28, 2).unwrap(),
        );
    }

    #[test]
    fn json_struct() {
        let record = r#"{"1": 1, "2": "bar", "3": null}"#;

        check_json_serde(
            record,
            Literal::Struct(Struct::from_iter(vec![
                Some(Literal::Primitive(PrimitiveLiteral::Int(1))),
                Some(Literal::Primitive(PrimitiveLiteral::String(
                    "bar".to_string(),
                ))),
                None,
            ])),
            &Type::Struct(StructType::new(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(3, "address", Type::Primitive(PrimitiveType::String)).into(),
            ])),
        );
    }

    #[test]
    fn json_list() {
        let record = r#"[1, 2, 3, null]"#;

        check_json_serde(
            record,
            Literal::List(vec![
                Some(Literal::Primitive(PrimitiveLiteral::Int(1))),
                Some(Literal::Primitive(PrimitiveLiteral::Int(2))),
                Some(Literal::Primitive(PrimitiveLiteral::Int(3))),
                None,
            ]),
            &Type::List(ListType {
                element_field: NestedField::list_element(
                    0,
                    Type::Primitive(PrimitiveType::Int),
                    true,
                )
                .into(),
            }),
        );
    }

    #[test]
    fn json_map() {
        let record = r#"{ "keys": ["a", "b", "c"], "values": [1, 2, null] }"#;

        check_json_serde(
            record,
            Literal::Map(Map::from([
                (
                    Literal::Primitive(PrimitiveLiteral::String("a".to_string())),
                    Some(Literal::Primitive(PrimitiveLiteral::Int(1))),
                ),
                (
                    Literal::Primitive(PrimitiveLiteral::String("b".to_string())),
                    Some(Literal::Primitive(PrimitiveLiteral::Int(2))),
                ),
                (
                    Literal::Primitive(PrimitiveLiteral::String("c".to_string())),
                    None,
                ),
            ])),
            &Type::Map(MapType {
                key_field: NestedField::map_key_element(0, Type::Primitive(PrimitiveType::String))
                    .into(),
                value_field: NestedField::map_value_element(
                    1,
                    Type::Primitive(PrimitiveType::Int),
                    true,
                )
                .into(),
            }),
        );
    }

    #[test]
    fn avro_bytes_boolean() {
        let bytes = vec![1u8];

        check_avro_bytes_serde(bytes, Datum::bool(true), &PrimitiveType::Boolean);
    }

    #[test]
    fn avro_bytes_int() {
        let bytes = vec![32u8, 0u8, 0u8, 0u8];

        check_avro_bytes_serde(bytes, Datum::int(32), &PrimitiveType::Int);
    }

    #[test]
    fn avro_bytes_long() {
        let bytes = vec![32u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];

        check_avro_bytes_serde(bytes, Datum::long(32), &PrimitiveType::Long);
    }

    #[test]
    fn avro_bytes_float() {
        let bytes = vec![0u8, 0u8, 128u8, 63u8];

        check_avro_bytes_serde(bytes, Datum::float(1.0), &PrimitiveType::Float);
    }

    #[test]
    fn avro_bytes_double() {
        let bytes = vec![0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 240u8, 63u8];

        check_avro_bytes_serde(bytes, Datum::double(1.0), &PrimitiveType::Double);
    }

    #[test]
    fn avro_bytes_string() {
        let bytes = vec![105u8, 99u8, 101u8, 98u8, 101u8, 114u8, 103u8];

        check_avro_bytes_serde(bytes, Datum::string("iceberg"), &PrimitiveType::String);
    }

    #[test]
    fn avro_bytes_decimal() {
        // (input_bytes, decimal_num, expect_scale, expect_precision)
        let cases = vec![
            (vec![4u8, 210u8], 1234, 2, 38),
            (vec![251u8, 46u8], -1234, 2, 38),
            (vec![4u8, 210u8], 1234, 3, 38),
            (vec![251u8, 46u8], -1234, 3, 38),
            (vec![42u8], 42, 2, 1),
            (vec![214u8], -42, 2, 1),
        ];

        for (input_bytes, decimal_num, expect_scale, expect_precision) in cases {
            check_avro_bytes_serde(
                input_bytes,
                Datum::decimal_with_precision(
                    Decimal::new(decimal_num, expect_scale),
                    expect_precision,
                )
                .unwrap(),
                &PrimitiveType::Decimal {
                    precision: expect_precision,
                    scale: expect_scale,
                },
            );
        }
    }

    #[test]
    fn avro_bytes_decimal_expect_error() {
        // (decimal_num, expect_scale, expect_precision)
        let cases = vec![(1234, 2, 1)];

        for (decimal_num, expect_scale, expect_precision) in cases {
            let result = Datum::decimal_with_precision(
                Decimal::new(decimal_num, expect_scale),
                expect_precision,
            );
            assert!(result.is_err(), "expect error but got {:?}", result);
            assert_eq!(
                result.unwrap_err().kind(),
                ErrorKind::DataInvalid,
                "expect error DataInvalid",
            );
        }
    }

    #[test]
    fn avro_convert_test_int() {
        check_convert_with_avro(
            Literal::Primitive(PrimitiveLiteral::Int(32)),
            &Type::Primitive(PrimitiveType::Int),
        );
    }

    #[test]
    fn avro_convert_test_long() {
        check_convert_with_avro(
            Literal::Primitive(PrimitiveLiteral::Long(32)),
            &Type::Primitive(PrimitiveType::Long),
        );
    }

    #[test]
    fn avro_convert_test_float() {
        check_convert_with_avro(
            Literal::Primitive(PrimitiveLiteral::Float(OrderedFloat(1.0))),
            &Type::Primitive(PrimitiveType::Float),
        );
    }

    #[test]
    fn avro_convert_test_double() {
        check_convert_with_avro(
            Literal::Primitive(PrimitiveLiteral::Double(OrderedFloat(1.0))),
            &Type::Primitive(PrimitiveType::Double),
        );
    }

    #[test]
    fn avro_convert_test_string() {
        check_convert_with_avro(
            Literal::Primitive(PrimitiveLiteral::String("iceberg".to_string())),
            &Type::Primitive(PrimitiveType::String),
        );
    }

    #[test]
    fn avro_convert_test_date() {
        check_convert_with_avro(
            Literal::Primitive(PrimitiveLiteral::Int(17486)),
            &Type::Primitive(PrimitiveType::Date),
        );
    }

    #[test]
    fn avro_convert_test_time() {
        check_convert_with_avro(
            Literal::Primitive(PrimitiveLiteral::Long(81068123456)),
            &Type::Primitive(PrimitiveType::Time),
        );
    }

    #[test]
    fn avro_convert_test_timestamp() {
        check_convert_with_avro(
            Literal::Primitive(PrimitiveLiteral::Long(1510871468123456)),
            &Type::Primitive(PrimitiveType::Timestamp),
        );
    }

    #[test]
    fn avro_convert_test_timestamptz() {
        check_convert_with_avro(
            Literal::Primitive(PrimitiveLiteral::Long(1510871468123456)),
            &Type::Primitive(PrimitiveType::Timestamptz),
        );
    }

    #[test]
    fn avro_convert_test_list() {
        check_convert_with_avro(
            Literal::List(vec![
                Some(Literal::Primitive(PrimitiveLiteral::Int(1))),
                Some(Literal::Primitive(PrimitiveLiteral::Int(2))),
                Some(Literal::Primitive(PrimitiveLiteral::Int(3))),
                None,
            ]),
            &Type::List(ListType {
                element_field: NestedField::list_element(
                    0,
                    Type::Primitive(PrimitiveType::Int),
                    false,
                )
                .into(),
            }),
        );

        check_convert_with_avro(
            Literal::List(vec![
                Some(Literal::Primitive(PrimitiveLiteral::Int(1))),
                Some(Literal::Primitive(PrimitiveLiteral::Int(2))),
                Some(Literal::Primitive(PrimitiveLiteral::Int(3))),
            ]),
            &Type::List(ListType {
                element_field: NestedField::list_element(
                    0,
                    Type::Primitive(PrimitiveType::Int),
                    true,
                )
                .into(),
            }),
        );
    }

    fn check_convert_with_avro_map(expected_literal: Literal, expected_type: &Type) {
        let fields = vec![NestedField::required(1, "col", expected_type.clone()).into()];
        let schema = Schema::builder()
            .with_fields(fields.clone())
            .build()
            .unwrap();
        let avro_schema = schema_to_avro_schema("test", &schema).unwrap();
        let struct_type = Type::Struct(StructType::new(fields));
        let struct_literal =
            Literal::Struct(Struct::from_iter(vec![Some(expected_literal.clone())]));

        let mut writer = apache_avro::Writer::new(&avro_schema, Vec::new());
        let raw_literal = RawLiteral::try_from(struct_literal.clone(), &struct_type).unwrap();
        writer.append_ser(raw_literal).unwrap();
        let encoded = writer.into_inner().unwrap();

        let reader = apache_avro::Reader::new(&*encoded).unwrap();
        for record in reader {
            let result = apache_avro::from_value::<RawLiteral>(&record.unwrap()).unwrap();
            let desered_literal = result.try_into(&struct_type).unwrap().unwrap();
            match (&desered_literal, &struct_literal) {
                (Literal::Struct(desered), Literal::Struct(expected)) => {
                    match (&desered.fields[0], &expected.fields[0]) {
                        (Literal::Map(desered), Literal::Map(expected)) => {
                            assert!(desered.has_same_content(expected))
                        }
                        _ => {
                            unreachable!()
                        }
                    }
                }
                _ => {
                    panic!("unexpected literal type");
                }
            }
        }
    }

    #[test]
    fn avro_convert_test_map() {
        check_convert_with_avro_map(
            Literal::Map(Map::from([
                (
                    Literal::Primitive(PrimitiveLiteral::Int(1)),
                    Some(Literal::Primitive(PrimitiveLiteral::Long(1))),
                ),
                (
                    Literal::Primitive(PrimitiveLiteral::Int(2)),
                    Some(Literal::Primitive(PrimitiveLiteral::Long(2))),
                ),
                (Literal::Primitive(PrimitiveLiteral::Int(3)), None),
            ])),
            &Type::Map(MapType {
                key_field: NestedField::map_key_element(2, Type::Primitive(PrimitiveType::Int))
                    .into(),
                value_field: NestedField::map_value_element(
                    3,
                    Type::Primitive(PrimitiveType::Long),
                    false,
                )
                .into(),
            }),
        );

        check_convert_with_avro_map(
            Literal::Map(Map::from([
                (
                    Literal::Primitive(PrimitiveLiteral::Int(1)),
                    Some(Literal::Primitive(PrimitiveLiteral::Long(1))),
                ),
                (
                    Literal::Primitive(PrimitiveLiteral::Int(2)),
                    Some(Literal::Primitive(PrimitiveLiteral::Long(2))),
                ),
                (
                    Literal::Primitive(PrimitiveLiteral::Int(3)),
                    Some(Literal::Primitive(PrimitiveLiteral::Long(3))),
                ),
            ])),
            &Type::Map(MapType {
                key_field: NestedField::map_key_element(2, Type::Primitive(PrimitiveType::Int))
                    .into(),
                value_field: NestedField::map_value_element(
                    3,
                    Type::Primitive(PrimitiveType::Long),
                    true,
                )
                .into(),
            }),
        );
    }

    #[test]
    fn avro_convert_test_string_map() {
        check_convert_with_avro_map(
            Literal::Map(Map::from([
                (
                    Literal::Primitive(PrimitiveLiteral::String("a".to_string())),
                    Some(Literal::Primitive(PrimitiveLiteral::Int(1))),
                ),
                (
                    Literal::Primitive(PrimitiveLiteral::String("b".to_string())),
                    Some(Literal::Primitive(PrimitiveLiteral::Int(2))),
                ),
                (
                    Literal::Primitive(PrimitiveLiteral::String("c".to_string())),
                    None,
                ),
            ])),
            &Type::Map(MapType {
                key_field: NestedField::map_key_element(2, Type::Primitive(PrimitiveType::String))
                    .into(),
                value_field: NestedField::map_value_element(
                    3,
                    Type::Primitive(PrimitiveType::Int),
                    false,
                )
                .into(),
            }),
        );

        check_convert_with_avro_map(
            Literal::Map(Map::from([
                (
                    Literal::Primitive(PrimitiveLiteral::String("a".to_string())),
                    Some(Literal::Primitive(PrimitiveLiteral::Int(1))),
                ),
                (
                    Literal::Primitive(PrimitiveLiteral::String("b".to_string())),
                    Some(Literal::Primitive(PrimitiveLiteral::Int(2))),
                ),
                (
                    Literal::Primitive(PrimitiveLiteral::String("c".to_string())),
                    Some(Literal::Primitive(PrimitiveLiteral::Int(3))),
                ),
            ])),
            &Type::Map(MapType {
                key_field: NestedField::map_key_element(2, Type::Primitive(PrimitiveType::String))
                    .into(),
                value_field: NestedField::map_value_element(
                    3,
                    Type::Primitive(PrimitiveType::Int),
                    true,
                )
                .into(),
            }),
        );
    }

    #[test]
    fn avro_convert_test_record() {
        check_convert_with_avro(
            Literal::Struct(Struct::from_iter(vec![
                Some(Literal::Primitive(PrimitiveLiteral::Int(1))),
                Some(Literal::Primitive(PrimitiveLiteral::String(
                    "bar".to_string(),
                ))),
                None,
            ])),
            &Type::Struct(StructType::new(vec![
                NestedField::required(2, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(3, "name", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(4, "address", Type::Primitive(PrimitiveType::String)).into(),
            ])),
        );
    }

    // # TODO:https://github.com/apache/iceberg-rust/issues/86
    // rust avro don't support deserialize any bytes representation now:
    // - binary
    // - decimal
    #[test]
    fn avro_convert_test_binary_ser() {
        let literal = Literal::Primitive(PrimitiveLiteral::Binary(vec![1, 2, 3, 4, 5]));
        let ty = Type::Primitive(PrimitiveType::Binary);
        let expect_value = Value::Bytes(vec![1, 2, 3, 4, 5]);
        check_serialize_avro(literal, &ty, expect_value);
    }

    #[test]
    fn avro_convert_test_decimal_ser() {
        let literal = Literal::decimal(12345);
        let ty = Type::Primitive(PrimitiveType::Decimal {
            precision: 9,
            scale: 8,
        });
        let expect_value = Value::Decimal(apache_avro::Decimal::from(12345_i128.to_be_bytes()));
        check_serialize_avro(literal, &ty, expect_value);
    }

    // # TODO:https://github.com/apache/iceberg-rust/issues/86
    // rust avro can't support to convert any byte-like type to fixed in avro now.
    // - uuid ser/de
    // - fixed ser/de

    #[test]
    fn test_parse_timestamp() {
        let value = Datum::timestamp_from_str("2021-08-01T01:09:00.0899").unwrap();
        assert_eq!(&format!("{value}"), "2021-08-01 01:09:00.089900");

        let value = Datum::timestamp_from_str("2023-01-06T00:00:00").unwrap();
        assert_eq!(&format!("{value}"), "2023-01-06 00:00:00");

        let value = Datum::timestamp_from_str("2021-08-01T01:09:00.0899+0800");
        assert!(value.is_err(), "Parse timestamp with timezone should fail!");

        let value = Datum::timestamp_from_str("dfa");
        assert!(
            value.is_err(),
            "Parse timestamp with invalid input should fail!"
        );
    }

    #[test]
    fn test_parse_timestamptz() {
        let value = Datum::timestamptz_from_str("2021-08-01T09:09:00.0899+0800").unwrap();
        assert_eq!(&format!("{value}"), "2021-08-01 01:09:00.089900 UTC");

        let value = Datum::timestamptz_from_str("2021-08-01T01:09:00.0899");
        assert!(
            value.is_err(),
            "Parse timestamptz without timezone should fail!"
        );

        let value = Datum::timestamptz_from_str("dfa");
        assert!(
            value.is_err(),
            "Parse timestamptz with invalid input should fail!"
        );
    }

    #[test]
    fn test_datum_ser_deser() {
        let test_fn = |datum: Datum| {
            let json = serde_json::to_value(&datum).unwrap();
            let desered_datum: Datum = serde_json::from_value(json).unwrap();
            assert_eq!(datum, desered_datum);
        };
        let datum = Datum::int(1);
        test_fn(datum);
        let datum = Datum::long(1);
        test_fn(datum);

        let datum = Datum::float(1.0);
        test_fn(datum);
        let datum = Datum::float(0_f32);
        test_fn(datum);
        let datum = Datum::float(-0_f32);
        test_fn(datum);
        let datum = Datum::float(f32::MAX);
        test_fn(datum);
        let datum = Datum::float(f32::MIN);
        test_fn(datum);

        // serde_json can't serialize f32::INFINITY, f32::NEG_INFINITY, f32::NAN
        let datum = Datum::float(f32::INFINITY);
        let json = serde_json::to_string(&datum).unwrap();
        assert!(serde_json::from_str::<Datum>(&json).is_err());
        let datum = Datum::float(f32::NEG_INFINITY);
        let json = serde_json::to_string(&datum).unwrap();
        assert!(serde_json::from_str::<Datum>(&json).is_err());
        let datum = Datum::float(f32::NAN);
        let json = serde_json::to_string(&datum).unwrap();
        assert!(serde_json::from_str::<Datum>(&json).is_err());

        let datum = Datum::double(1.0);
        test_fn(datum);
        let datum = Datum::double(f64::MAX);
        test_fn(datum);
        let datum = Datum::double(f64::MIN);
        test_fn(datum);

        // serde_json can't serialize f32::INFINITY, f32::NEG_INFINITY, f32::NAN
        let datum = Datum::double(f64::INFINITY);
        let json = serde_json::to_string(&datum).unwrap();
        assert!(serde_json::from_str::<Datum>(&json).is_err());
        let datum = Datum::double(f64::NEG_INFINITY);
        let json = serde_json::to_string(&datum).unwrap();
        assert!(serde_json::from_str::<Datum>(&json).is_err());
        let datum = Datum::double(f64::NAN);
        let json = serde_json::to_string(&datum).unwrap();
        assert!(serde_json::from_str::<Datum>(&json).is_err());

        let datum = Datum::string("iceberg");
        test_fn(datum);
        let datum = Datum::bool(true);
        test_fn(datum);
        let datum = Datum::date(17486);
        test_fn(datum);
        let datum = Datum::time_from_hms_micro(22, 15, 33, 111).unwrap();
        test_fn(datum);
        let datum = Datum::timestamp_micros(1510871468123456);
        test_fn(datum);
        let datum = Datum::timestamptz_micros(1510871468123456);
        test_fn(datum);
        let datum = Datum::uuid(Uuid::parse_str("f79c3e09-677c-4bbd-a479-3f349cb785e7").unwrap());
        test_fn(datum);
        let datum = Datum::decimal(1420).unwrap();
        test_fn(datum);
        let datum = Datum::binary(vec![1, 2, 3, 4, 5]);
        test_fn(datum);
        let datum = Datum::fixed(vec![1, 2, 3, 4, 5]);
        test_fn(datum);
    }

    #[test]
    fn test_datum_date_convert_to_int() {
        let datum_date = Datum::date(12345);

        let result = datum_date.to(&Primitive(PrimitiveType::Int)).unwrap();

        let expected = Datum::int(12345);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_datum_int_convert_to_date() {
        let datum_int = Datum::int(12345);

        let result = datum_int.to(&Primitive(PrimitiveType::Date)).unwrap();

        let expected = Datum::date(12345);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_datum_long_convert_to_int() {
        let datum = Datum::long(12345);

        let result = datum.to(&Primitive(PrimitiveType::Int)).unwrap();

        let expected = Datum::int(12345);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_datum_long_convert_to_int_above_max() {
        let datum = Datum::long(INT_MAX as i64 + 1);

        let result = datum.to(&Primitive(PrimitiveType::Int)).unwrap();

        let expected = Datum::new(PrimitiveType::Int, PrimitiveLiteral::AboveMax);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_datum_long_convert_to_int_below_min() {
        let datum = Datum::long(INT_MIN as i64 - 1);

        let result = datum.to(&Primitive(PrimitiveType::Int)).unwrap();

        let expected = Datum::new(PrimitiveType::Int, PrimitiveLiteral::BelowMin);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_datum_long_convert_to_timestamp() {
        let datum = Datum::long(12345);

        let result = datum.to(&Primitive(PrimitiveType::Timestamp)).unwrap();

        let expected = Datum::timestamp_micros(12345);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_datum_long_convert_to_timestamptz() {
        let datum = Datum::long(12345);

        let result = datum.to(&Primitive(PrimitiveType::Timestamptz)).unwrap();

        let expected = Datum::timestamptz_micros(12345);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_datum_decimal_convert_to_long() {
        let datum = Datum::decimal(12345).unwrap();

        let result = datum.to(&Primitive(PrimitiveType::Long)).unwrap();

        let expected = Datum::long(12345);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_datum_decimal_convert_to_long_above_max() {
        let datum = Datum::decimal(LONG_MAX as i128 + 1).unwrap();

        let result = datum.to(&Primitive(PrimitiveType::Long)).unwrap();

        let expected = Datum::new(PrimitiveType::Long, PrimitiveLiteral::AboveMax);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_datum_decimal_convert_to_long_below_min() {
        let datum = Datum::decimal(LONG_MIN as i128 - 1).unwrap();

        let result = datum.to(&Primitive(PrimitiveType::Long)).unwrap();

        let expected = Datum::new(PrimitiveType::Long, PrimitiveLiteral::BelowMin);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_datum_string_convert_to_boolean() {
        let datum = Datum::string("true");

        let result = datum.to(&Primitive(PrimitiveType::Boolean)).unwrap();

        let expected = Datum::bool(true);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_datum_string_convert_to_int() {
        let datum = Datum::string("12345");

        let result = datum.to(&Primitive(PrimitiveType::Int)).unwrap();

        let expected = Datum::int(12345);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_datum_string_convert_to_long() {
        let datum = Datum::string("12345");

        let result = datum.to(&Primitive(PrimitiveType::Long)).unwrap();

        let expected = Datum::long(12345);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_datum_string_convert_to_timestamp() {
        let datum = Datum::string("1925-05-20T19:25:00.000");

        let result = datum.to(&Primitive(PrimitiveType::Timestamp)).unwrap();

        let expected = Datum::timestamp_micros(-1407990900000000);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_datum_string_convert_to_timestamptz() {
        let datum = Datum::string("1925-05-20T19:25:00.000 UTC");

        let result = datum.to(&Primitive(PrimitiveType::Timestamptz)).unwrap();

        let expected = Datum::timestamptz_micros(-1407990900000000);

        assert_eq!(result, expected);
    }
}
