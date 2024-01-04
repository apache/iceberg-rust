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

use std::str::FromStr;
use std::{any::Any, collections::BTreeMap};

use crate::error::Result;
use bitvec::vec::BitVec;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use ordered_float::OrderedFloat;
use rust_decimal::Decimal;
use serde_bytes::ByteBuf;
use serde_json::{Map as JsonMap, Number, Value as JsonValue};
use uuid::Uuid;

use crate::{Error, ErrorKind};

use super::datatypes::{PrimitiveType, Type};

pub use _serde::RawLiteral;

/// Values present in iceberg type
#[derive(Clone, Debug, PartialEq, Hash, Eq, PartialOrd, Ord)]
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
    /// Stores days from the 1970-01-01 in an 4-byte little-endian int
    Date(i32),
    /// Stores microseconds from midnight in an 8-byte little-endian long
    Time(i64),
    /// Timestamp without timezone
    Timestamp(i64),
    /// Timestamp with timezone
    TimestampTZ(i64),
    /// UTF-8 bytes (without length)
    String(String),
    /// 16-byte big-endian value
    UUID(Uuid),
    /// Binary value
    Fixed(Vec<u8>),
    /// Binary value (without length)
    Binary(Vec<u8>),
    /// Stores unscaled value as big int. According to iceberg spec, the precision must less than 38(`MAX_DECIMAL_PRECISION`) , so i128 is suit here.
    Decimal(i128),
}

/// Values present in iceberg type
#[derive(Clone, Debug, PartialEq, Hash, Eq, PartialOrd, Ord)]
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
    Map(BTreeMap<Literal, Option<Literal>>),
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
    /// use ordered_float::OrderedFloat;
    /// use iceberg::spec::{Literal, PrimitiveLiteral};
    /// let t = Literal::float( 32.1f32 );
    ///
    /// assert_eq!(Literal::Primitive(PrimitiveLiteral::Float(OrderedFloat(32.1))), t);
    /// ```
    pub fn float<T: Into<f32>>(t: T) -> Self {
        Self::Primitive(PrimitiveLiteral::Float(OrderedFloat(t.into())))
    }

    /// Creates an 32bit floating point number.
    ///
    /// Example:
    /// ```rust
    /// use ordered_float::OrderedFloat;
    /// use iceberg::spec::{Literal, PrimitiveLiteral};
    /// let t = Literal::double( 32.1f64 );
    ///
    /// assert_eq!(Literal::Primitive(PrimitiveLiteral::Double(OrderedFloat(32.1))), t);
    /// ```
    pub fn double<T: Into<f64>>(t: T) -> Self {
        Self::Primitive(PrimitiveLiteral::Double(OrderedFloat(t.into())))
    }

    /// Returns unix epoch.
    pub fn unix_epoch() -> DateTime<Utc> {
        Utc.timestamp_nanos(0)
    }

    /// Creates date literal from number of days from unix epoch directly.
    pub fn date(days: i32) -> Self {
        Self::Primitive(PrimitiveLiteral::Date(days))
    }

    /// Creates date literal from `NaiveDate`, assuming it's utc timezone.
    fn date_from_naive_date(date: NaiveDate) -> Self {
        let days = (date - Self::unix_epoch().date_naive()).num_days();
        Self::date(days as i32)
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

        Ok(Self::date_from_naive_date(t))
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

        Ok(Self::date_from_naive_date(t))
    }

    /// Creates time in microseconds directly
    pub fn time(value: i64) -> Self {
        Self::Primitive(PrimitiveLiteral::Time(value))
    }

    /// Creates time literal from [`chrono::NaiveTime`].
    fn time_from_naive_time(t: NaiveTime) -> Self {
        let duration = t - Self::unix_epoch().time();
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
    ///     888999  // microseconds
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
    ///
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
        Self::Primitive(PrimitiveLiteral::Timestamp(value))
    }

    /// Creates a timestamp with timezone from unix epoch in microseconds.
    pub fn timestamptz(value: i64) -> Self {
        Self::Primitive(PrimitiveLiteral::TimestampTZ(value))
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
    ///  let date = NaiveDate::from_ymd_opt(2012, 12, 12).unwrap();
    ///  let time = NaiveTime::from_hms_micro_opt(12, 12, 12, 889900).unwrap();
    ///  let dt = NaiveDateTime::new(date, time);
    ///  Literal::timestamp_from_datetime(DateTime::<FixedOffset>::from_local(dt, FixedOffset::west_opt(4 * 3600).unwrap()))
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
        Self::Primitive(PrimitiveLiteral::UUID(uuid))
    }

    /// Creates uuid from str. See [`Uuid::parse_str`].
    ///
    /// Example:
    ///
    /// ```rust
    /// use uuid::Uuid;
    /// use iceberg::spec::Literal;
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
    /// let t2 = Literal::Primitive(PrimitiveLiteral::Fixed(vec![1u8, 2u8]));
    ///
    /// assert_eq!(t1, t2);
    /// ```
    pub fn fixed<I: IntoIterator<Item = u8>>(input: I) -> Self {
        Literal::Primitive(PrimitiveLiteral::Fixed(input.into_iter().collect()))
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
        Self::Primitive(PrimitiveLiteral::Decimal(decimal))
    }

    /// Creates decimal literal from string. See [`Decimal::from_str_exact`].
    ///
    /// Example:
    ///
    /// ```rust
    /// use rust_decimal::Decimal;
    /// use iceberg::spec::Literal;
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
}

impl From<Literal> for ByteBuf {
    fn from(value: Literal) -> Self {
        match value {
            Literal::Primitive(prim) => match prim {
                PrimitiveLiteral::Boolean(val) => {
                    if val {
                        ByteBuf::from([1u8])
                    } else {
                        ByteBuf::from([0u8])
                    }
                }
                PrimitiveLiteral::Int(val) => ByteBuf::from(val.to_le_bytes()),
                PrimitiveLiteral::Long(val) => ByteBuf::from(val.to_le_bytes()),
                PrimitiveLiteral::Float(val) => ByteBuf::from(val.to_le_bytes()),
                PrimitiveLiteral::Double(val) => ByteBuf::from(val.to_le_bytes()),
                PrimitiveLiteral::Date(val) => ByteBuf::from(val.to_le_bytes()),
                PrimitiveLiteral::Time(val) => ByteBuf::from(val.to_le_bytes()),
                PrimitiveLiteral::Timestamp(val) => ByteBuf::from(val.to_le_bytes()),
                PrimitiveLiteral::TimestampTZ(val) => ByteBuf::from(val.to_le_bytes()),
                PrimitiveLiteral::String(val) => ByteBuf::from(val.as_bytes()),
                PrimitiveLiteral::UUID(val) => ByteBuf::from(val.as_u128().to_be_bytes()),
                PrimitiveLiteral::Fixed(val) => ByteBuf::from(val),
                PrimitiveLiteral::Binary(val) => ByteBuf::from(val),
                PrimitiveLiteral::Decimal(_) => todo!(),
            },
            _ => unimplemented!(),
        }
    }
}

impl From<Literal> for Vec<u8> {
    fn from(value: Literal) -> Self {
        match value {
            Literal::Primitive(prim) => match prim {
                PrimitiveLiteral::Boolean(val) => {
                    if val {
                        Vec::from([1u8])
                    } else {
                        Vec::from([0u8])
                    }
                }
                PrimitiveLiteral::Int(val) => Vec::from(val.to_le_bytes()),
                PrimitiveLiteral::Long(val) => Vec::from(val.to_le_bytes()),
                PrimitiveLiteral::Float(val) => Vec::from(val.to_le_bytes()),
                PrimitiveLiteral::Double(val) => Vec::from(val.to_le_bytes()),
                PrimitiveLiteral::Date(val) => Vec::from(val.to_le_bytes()),
                PrimitiveLiteral::Time(val) => Vec::from(val.to_le_bytes()),
                PrimitiveLiteral::Timestamp(val) => Vec::from(val.to_le_bytes()),
                PrimitiveLiteral::TimestampTZ(val) => Vec::from(val.to_le_bytes()),
                PrimitiveLiteral::String(val) => Vec::from(val.as_bytes()),
                PrimitiveLiteral::UUID(val) => Vec::from(val.as_u128().to_be_bytes()),
                PrimitiveLiteral::Fixed(val) => val,
                PrimitiveLiteral::Binary(val) => val,
                PrimitiveLiteral::Decimal(_) => todo!(),
            },
            _ => unimplemented!(),
        }
    }
}

/// The partition struct stores the tuple of partition values for each file.
/// Its type is derived from the partition fields of the partition spec used to write the manifest file.
/// In v2, the partition struct’s field ids must match the ids from the partition spec.
#[derive(Debug, Clone, PartialEq, Hash, Eq, PartialOrd, Ord)]
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
    pub fn iter(&self) -> impl Iterator<Item = Option<&Literal>> {
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
    /// Create iceberg value from bytes
    pub fn try_from_bytes(bytes: &[u8], data_type: &Type) -> Result<Self> {
        match data_type {
            Type::Primitive(primitive) => match primitive {
                PrimitiveType::Boolean => {
                    if bytes.len() == 1 && bytes[0] == 0u8 {
                        Ok(Literal::Primitive(PrimitiveLiteral::Boolean(false)))
                    } else {
                        Ok(Literal::Primitive(PrimitiveLiteral::Boolean(true)))
                    }
                }
                PrimitiveType::Int => Ok(Literal::Primitive(PrimitiveLiteral::Int(
                    i32::from_le_bytes(bytes.try_into()?),
                ))),
                PrimitiveType::Long => Ok(Literal::Primitive(PrimitiveLiteral::Long(
                    i64::from_le_bytes(bytes.try_into()?),
                ))),
                PrimitiveType::Float => Ok(Literal::Primitive(PrimitiveLiteral::Float(
                    OrderedFloat(f32::from_le_bytes(bytes.try_into()?)),
                ))),
                PrimitiveType::Double => Ok(Literal::Primitive(PrimitiveLiteral::Double(
                    OrderedFloat(f64::from_le_bytes(bytes.try_into()?)),
                ))),
                PrimitiveType::Date => Ok(Literal::Primitive(PrimitiveLiteral::Date(
                    i32::from_le_bytes(bytes.try_into()?),
                ))),
                PrimitiveType::Time => Ok(Literal::Primitive(PrimitiveLiteral::Time(
                    i64::from_le_bytes(bytes.try_into()?),
                ))),
                PrimitiveType::Timestamp => Ok(Literal::Primitive(PrimitiveLiteral::Timestamp(
                    i64::from_le_bytes(bytes.try_into()?),
                ))),
                PrimitiveType::Timestamptz => Ok(Literal::Primitive(
                    PrimitiveLiteral::TimestampTZ(i64::from_le_bytes(bytes.try_into()?)),
                )),
                PrimitiveType::String => Ok(Literal::Primitive(PrimitiveLiteral::String(
                    std::str::from_utf8(bytes)?.to_string(),
                ))),
                PrimitiveType::Uuid => Ok(Literal::Primitive(PrimitiveLiteral::UUID(
                    Uuid::from_u128(u128::from_be_bytes(bytes.try_into()?)),
                ))),
                PrimitiveType::Fixed(_) => Ok(Literal::Primitive(PrimitiveLiteral::Fixed(
                    Vec::from(bytes),
                ))),
                PrimitiveType::Binary => Ok(Literal::Primitive(PrimitiveLiteral::Binary(
                    Vec::from(bytes),
                ))),
                PrimitiveType::Decimal {
                    precision: _,
                    scale: _,
                } => todo!(),
            },
            _ => Err(Error::new(
                crate::ErrorKind::DataInvalid,
                "Converting bytes to non-primitive types is not supported.",
            )),
        }
    }

    /// Create iceberg value from a json value
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
                    Ok(Some(Literal::Primitive(PrimitiveLiteral::Date(
                        date::date_to_days(&NaiveDate::parse_from_str(&s, "%Y-%m-%d")?),
                    ))))
                }
                (PrimitiveType::Time, JsonValue::String(s)) => {
                    Ok(Some(Literal::Primitive(PrimitiveLiteral::Time(
                        time::time_to_microseconds(&NaiveTime::parse_from_str(&s, "%H:%M:%S%.f")?),
                    ))))
                }
                (PrimitiveType::Timestamp, JsonValue::String(s)) => Ok(Some(Literal::Primitive(
                    PrimitiveLiteral::Timestamp(timestamp::datetime_to_microseconds(
                        &NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.f")?,
                    )),
                ))),
                (PrimitiveType::Timestamptz, JsonValue::String(s)) => {
                    Ok(Some(Literal::Primitive(PrimitiveLiteral::TimestampTZ(
                        timestamptz::datetimetz_to_microseconds(&Utc.from_utc_datetime(
                            &NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.f+00:00")?,
                        )),
                    ))))
                }
                (PrimitiveType::String, JsonValue::String(s)) => {
                    Ok(Some(Literal::Primitive(PrimitiveLiteral::String(s))))
                }
                (PrimitiveType::Uuid, JsonValue::String(s)) => Ok(Some(Literal::Primitive(
                    PrimitiveLiteral::UUID(Uuid::parse_str(&s)?),
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
                    Ok(Some(Literal::Primitive(PrimitiveLiteral::Decimal(
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
                        Ok(Some(Literal::Map(BTreeMap::from_iter(
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
            (Literal::Primitive(prim), _) => match prim {
                PrimitiveLiteral::Boolean(val) => Ok(JsonValue::Bool(val)),
                PrimitiveLiteral::Int(val) => Ok(JsonValue::Number((val).into())),
                PrimitiveLiteral::Long(val) => Ok(JsonValue::Number((val).into())),
                PrimitiveLiteral::Float(val) => match Number::from_f64(val.0 as f64) {
                    Some(number) => Ok(JsonValue::Number(number)),
                    None => Ok(JsonValue::Null),
                },
                PrimitiveLiteral::Double(val) => match Number::from_f64(val.0) {
                    Some(number) => Ok(JsonValue::Number(number)),
                    None => Ok(JsonValue::Null),
                },
                PrimitiveLiteral::Date(val) => {
                    Ok(JsonValue::String(date::days_to_date(val).to_string()))
                }
                PrimitiveLiteral::Time(val) => Ok(JsonValue::String(
                    time::microseconds_to_time(val).to_string(),
                )),
                PrimitiveLiteral::Timestamp(val) => Ok(JsonValue::String(
                    timestamp::microseconds_to_datetime(val)
                        .format("%Y-%m-%dT%H:%M:%S%.f")
                        .to_string(),
                )),
                PrimitiveLiteral::TimestampTZ(val) => Ok(JsonValue::String(
                    timestamptz::microseconds_to_datetimetz(val)
                        .format("%Y-%m-%dT%H:%M:%S%.f+00:00")
                        .to_string(),
                )),
                PrimitiveLiteral::String(val) => Ok(JsonValue::String(val.clone())),
                PrimitiveLiteral::UUID(val) => Ok(JsonValue::String(val.to_string())),
                PrimitiveLiteral::Fixed(val) => Ok(JsonValue::String(val.iter().fold(
                    String::new(),
                    |mut acc, x| {
                        acc.push_str(&format!("{:x}", x));
                        acc
                    },
                ))),
                PrimitiveLiteral::Binary(val) => Ok(JsonValue::String(val.iter().fold(
                    String::new(),
                    |mut acc, x| {
                        acc.push_str(&format!("{:x}", x));
                        acc
                    },
                ))),
                PrimitiveLiteral::Decimal(val) => match r#type {
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
                PrimitiveLiteral::Date(any) => Box::new(any),
                PrimitiveLiteral::Time(any) => Box::new(any),
                PrimitiveLiteral::Timestamp(any) => Box::new(any),
                PrimitiveLiteral::TimestampTZ(any) => Box::new(any),
                PrimitiveLiteral::Fixed(any) => Box::new(any),
                PrimitiveLiteral::Binary(any) => Box::new(any),
                PrimitiveLiteral::String(any) => Box::new(any),
                PrimitiveLiteral::UUID(any) => Box::new(any),
                PrimitiveLiteral::Decimal(any) => Box::new(any),
            },
            _ => unimplemented!(),
        }
    }
}

mod date {
    use chrono::{NaiveDate, NaiveDateTime};

    pub(crate) fn date_to_days(date: &NaiveDate) -> i32 {
        date.signed_duration_since(
            // This is always the same and shouldn't fail
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
        )
        .num_days() as i32
    }

    pub(crate) fn days_to_date(days: i32) -> NaiveDate {
        // This shouldn't fail until the year 262000
        NaiveDateTime::from_timestamp_opt(days as i64 * 86_400, 0)
            .unwrap()
            .date()
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
    use chrono::NaiveDateTime;

    pub(crate) fn datetime_to_microseconds(time: &NaiveDateTime) -> i64 {
        time.timestamp_micros()
    }

    pub(crate) fn microseconds_to_datetime(micros: i64) -> NaiveDateTime {
        let (secs, rem) = (micros / 1_000_000, micros % 1_000_000);

        // This shouldn't fail until the year 262000
        NaiveDateTime::from_timestamp_opt(secs, rem as u32 * 1_000).unwrap()
    }
}

mod timestamptz {
    use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};

    pub(crate) fn datetimetz_to_microseconds(time: &DateTime<Utc>) -> i64 {
        time.timestamp_micros()
    }

    pub(crate) fn microseconds_to_datetimetz(micros: i64) -> DateTime<Utc> {
        let (secs, rem) = (micros / 1_000_000, micros % 1_000_000);

        Utc.from_utc_datetime(
            // This shouldn't fail until the year 262000
            &NaiveDateTime::from_timestamp_opt(secs, rem as u32 * 1_000).unwrap(),
        )
    }
}

mod _serde {
    use std::collections::BTreeMap;

    use crate::{
        spec::{PrimitiveType, Type, MAP_KEY_FIELD_NAME, MAP_VALUE_FIELD_NAME},
        Error, ErrorKind,
    };

    use super::{Literal, PrimitiveLiteral};
    use serde::{
        de::Visitor,
        ser::{SerializeMap, SerializeSeq, SerializeStruct},
        Deserialize, Serialize,
    };
    use serde_bytes::ByteBuf;
    use serde_derive::Deserialize as DeserializeDerive;
    use serde_derive::Serialize as SerializeDerive;

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
        where
            S: serde::Serializer,
        {
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
        where
            S: serde::Serializer,
        {
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
        where
            S: serde::Serializer,
        {
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
        where
            D: serde::Deserializer<'de>,
        {
            struct RawLiteralVisitor;
            impl<'de> Visitor<'de> for RawLiteralVisitor {
                type Value = RawLiteralEnum;

                fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                    formatter.write_str("expect")
                }

                fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
                where
                    E: serde::de::Error,
                {
                    Ok(RawLiteralEnum::Boolean(v))
                }

                fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E>
                where
                    E: serde::de::Error,
                {
                    Ok(RawLiteralEnum::Int(v))
                }

                fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
                where
                    E: serde::de::Error,
                {
                    Ok(RawLiteralEnum::Long(v))
                }

                /// Used in json
                fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
                where
                    E: serde::de::Error,
                {
                    Ok(RawLiteralEnum::Long(v as i64))
                }

                fn visit_f32<E>(self, v: f32) -> Result<Self::Value, E>
                where
                    E: serde::de::Error,
                {
                    Ok(RawLiteralEnum::Float(v))
                }

                fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
                where
                    E: serde::de::Error,
                {
                    Ok(RawLiteralEnum::Double(v))
                }

                fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
                where
                    E: serde::de::Error,
                {
                    Ok(RawLiteralEnum::String(v.to_string()))
                }

                fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
                where
                    E: serde::de::Error,
                {
                    Ok(RawLiteralEnum::Bytes(ByteBuf::from(v)))
                }

                fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
                where
                    E: serde::de::Error,
                {
                    Ok(RawLiteralEnum::String(v.to_string()))
                }

                fn visit_unit<E>(self) -> Result<Self::Value, E>
                where
                    E: serde::de::Error,
                {
                    Ok(RawLiteralEnum::Null)
                }

                fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
                where
                    A: serde::de::MapAccess<'de>,
                {
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
                where
                    A: serde::de::SeqAccess<'de>,
                {
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
                    super::PrimitiveLiteral::Date(v) => RawLiteralEnum::Int(v),
                    super::PrimitiveLiteral::Time(v) => RawLiteralEnum::Long(v),
                    super::PrimitiveLiteral::Timestamp(v) => RawLiteralEnum::Long(v),
                    super::PrimitiveLiteral::TimestampTZ(v) => RawLiteralEnum::Long(v),
                    super::PrimitiveLiteral::String(v) => RawLiteralEnum::String(v),
                    super::PrimitiveLiteral::UUID(v) => {
                        RawLiteralEnum::Bytes(ByteBuf::from(v.as_u128().to_be_bytes()))
                    }
                    super::PrimitiveLiteral::Fixed(v) => RawLiteralEnum::Bytes(ByteBuf::from(v)),
                    super::PrimitiveLiteral::Binary(v) => RawLiteralEnum::Bytes(ByteBuf::from(v)),
                    super::PrimitiveLiteral::Decimal(v) => {
                        RawLiteralEnum::Bytes(ByteBuf::from(v.to_be_bytes()))
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
                    Type::Primitive(PrimitiveType::Date) => Ok(Some(Literal::date(v))),
                    _ => Err(invalid_err("int")),
                },
                RawLiteralEnum::Long(v) => match ty {
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
                    _ => Err(invalid_err("float")),
                },
                RawLiteralEnum::Double(v) => match ty {
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
                            let mut map = BTreeMap::new();
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
                        let mut map = BTreeMap::new();
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

    use apache_avro::{to_value, types::Value};

    use crate::{
        avro::schema_to_avro_schema,
        spec::{
            datatypes::{ListType, MapType, NestedField, StructType},
            Schema,
        },
    };

    use super::*;

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

    fn check_avro_bytes_serde(input: Vec<u8>, expected_literal: Literal, expected_type: &Type) {
        let raw_schema = r#""bytes""#;
        let schema = apache_avro::Schema::parse_str(raw_schema).unwrap();

        let bytes = ByteBuf::from(input);
        let literal = Literal::try_from_bytes(&bytes, expected_type).unwrap();
        assert_eq!(literal, expected_literal);

        let mut writer = apache_avro::Writer::new(&schema, Vec::new());
        writer.append_ser(ByteBuf::from(literal)).unwrap();
        let encoded = writer.into_inner().unwrap();
        let reader = apache_avro::Reader::with_schema(&schema, &*encoded).unwrap();

        for record in reader {
            let result = apache_avro::from_value::<ByteBuf>(&record.unwrap()).unwrap();
            let desered_literal = Literal::try_from_bytes(&result, expected_type).unwrap();
            assert_eq!(desered_literal, expected_literal);
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
            Literal::Primitive(PrimitiveLiteral::Date(17486)),
            &Type::Primitive(PrimitiveType::Date),
        );
    }

    #[test]
    fn json_time() {
        let record = r#""22:31:08.123456""#;

        check_json_serde(
            record,
            Literal::Primitive(PrimitiveLiteral::Time(81068123456)),
            &Type::Primitive(PrimitiveType::Time),
        );
    }

    #[test]
    fn json_timestamp() {
        let record = r#""2017-11-16T22:31:08.123456""#;

        check_json_serde(
            record,
            Literal::Primitive(PrimitiveLiteral::Timestamp(1510871468123456)),
            &Type::Primitive(PrimitiveType::Timestamp),
        );
    }

    #[test]
    fn json_timestamptz() {
        let record = r#""2017-11-16T22:31:08.123456+00:00""#;

        check_json_serde(
            record,
            Literal::Primitive(PrimitiveLiteral::TimestampTZ(1510871468123456)),
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
            Literal::Primitive(PrimitiveLiteral::UUID(
                Uuid::parse_str("f79c3e09-677c-4bbd-a479-3f349cb785e7").unwrap(),
            )),
            &Type::Primitive(PrimitiveType::Uuid),
        );
    }

    #[test]
    fn json_decimal() {
        let record = r#""14.20""#;

        check_json_serde(
            record,
            Literal::Primitive(PrimitiveLiteral::Decimal(1420)),
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
            Literal::Map(BTreeMap::from([
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

        check_avro_bytes_serde(
            bytes,
            Literal::Primitive(PrimitiveLiteral::Boolean(true)),
            &Type::Primitive(PrimitiveType::Boolean),
        );
    }

    #[test]
    fn avro_bytes_int() {
        let bytes = vec![32u8, 0u8, 0u8, 0u8];

        check_avro_bytes_serde(
            bytes,
            Literal::Primitive(PrimitiveLiteral::Int(32)),
            &Type::Primitive(PrimitiveType::Int),
        );
    }

    #[test]
    fn avro_bytes_long() {
        let bytes = vec![32u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];

        check_avro_bytes_serde(
            bytes,
            Literal::Primitive(PrimitiveLiteral::Long(32)),
            &Type::Primitive(PrimitiveType::Long),
        );
    }

    #[test]
    fn avro_bytes_float() {
        let bytes = vec![0u8, 0u8, 128u8, 63u8];

        check_avro_bytes_serde(
            bytes,
            Literal::Primitive(PrimitiveLiteral::Float(OrderedFloat(1.0))),
            &Type::Primitive(PrimitiveType::Float),
        );
    }

    #[test]
    fn avro_bytes_double() {
        let bytes = vec![0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 240u8, 63u8];

        check_avro_bytes_serde(
            bytes,
            Literal::Primitive(PrimitiveLiteral::Double(OrderedFloat(1.0))),
            &Type::Primitive(PrimitiveType::Double),
        );
    }

    #[test]
    fn avro_bytes_string() {
        let bytes = vec![105u8, 99u8, 101u8, 98u8, 101u8, 114u8, 103u8];

        check_avro_bytes_serde(
            bytes,
            Literal::Primitive(PrimitiveLiteral::String("iceberg".to_string())),
            &Type::Primitive(PrimitiveType::String),
        );
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
            Literal::Primitive(PrimitiveLiteral::Date(17486)),
            &Type::Primitive(PrimitiveType::Date),
        );
    }

    #[test]
    fn avro_convert_test_time() {
        check_convert_with_avro(
            Literal::Primitive(PrimitiveLiteral::Time(81068123456)),
            &Type::Primitive(PrimitiveType::Time),
        );
    }

    #[test]
    fn avro_convert_test_timestamp() {
        check_convert_with_avro(
            Literal::Primitive(PrimitiveLiteral::Timestamp(1510871468123456)),
            &Type::Primitive(PrimitiveType::Timestamp),
        );
    }

    #[test]
    fn avro_convert_test_timestamptz() {
        check_convert_with_avro(
            Literal::Primitive(PrimitiveLiteral::TimestampTZ(1510871468123456)),
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

    #[test]
    fn avro_convert_test_map() {
        check_convert_with_avro(
            Literal::Map(BTreeMap::from([
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
                key_field: NestedField::map_key_element(0, Type::Primitive(PrimitiveType::Int))
                    .into(),
                value_field: NestedField::map_value_element(
                    1,
                    Type::Primitive(PrimitiveType::Long),
                    false,
                )
                .into(),
            }),
        );

        check_convert_with_avro(
            Literal::Map(BTreeMap::from([
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
                key_field: NestedField::map_key_element(0, Type::Primitive(PrimitiveType::Int))
                    .into(),
                value_field: NestedField::map_value_element(
                    1,
                    Type::Primitive(PrimitiveType::Long),
                    true,
                )
                .into(),
            }),
        );
    }

    #[test]
    fn avro_convert_test_string_map() {
        check_convert_with_avro(
            Literal::Map(BTreeMap::from([
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
                    false,
                )
                .into(),
            }),
        );

        check_convert_with_avro(
            Literal::Map(BTreeMap::from([
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
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(3, "address", Type::Primitive(PrimitiveType::String)).into(),
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
}
