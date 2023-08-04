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

use std::{any::Any, collections::HashMap, fmt, ops::Deref};

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use ordered_float::OrderedFloat;
use rust_decimal::Decimal;
use serde::{
    de::{MapAccess, Visitor},
    ser::SerializeStruct,
    Deserialize, Deserializer, Serialize,
};
use serde_bytes::ByteBuf;
use uuid::Uuid;

use crate::Error;

use super::datatypes::{PrimitiveType, Type};

/// Values present in iceberg type
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(untagged)]
pub enum PrimitiveLiteral {
    /// 0x00 for false, non-zero byte for true
    Boolean(bool),
    /// Stored as 4-byte little-endian
    Int(i32),
    /// Stored as 8-byte little-endian
    Long(i64),
    /// Stored as 4-byte little-endian
    Float(#[serde(with = "float")] OrderedFloat<f32>),
    /// Stored as 8-byte little-endian
    Double(#[serde(with = "double")] OrderedFloat<f64>),
    /// Stores days from the 1970-01-01 in an 4-byte little-endian int
    Date(#[serde(with = "date")] NaiveDate),
    /// Stores microseconds from midnight in an 8-byte little-endian long
    Time(#[serde(with = "time")] NaiveTime),
    /// Timestamp without timezone
    Timestamp(#[serde(with = "timestamp")] NaiveDateTime),
    /// Timestamp with timezone
    TimestampTZ(#[serde(with = "timestamptz")] DateTime<Utc>),
    /// UTF-8 bytes (without length)
    String(String),
    /// 16-byte big-endian value
    UUID(Uuid),
    /// Binary value
    Fixed(usize, Vec<u8>),
    /// Binary value (without length)
    Binary(Vec<u8>),
    /// Stores unscaled value as two’s-complement big-endian binary,
    /// using the minimum number of bytes for the value
    Decimal(Decimal),
}

/// Values present in iceberg type
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(untagged)]
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
    Map(HashMap<String, Option<Literal>>),
}

impl TryFrom<Literal> for ByteBuf {
    type Error = Error;
    fn try_from(value: Literal) -> Result<Self, Self::Error> {
        match value {
            Literal::Primitive(prim) => match prim {
                PrimitiveLiteral::Boolean(val) => {
                    if val {
                        Ok(ByteBuf::from([0u8]))
                    } else {
                        Ok(ByteBuf::from([1u8]))
                    }
                }
                PrimitiveLiteral::Int(val) => Ok(ByteBuf::from(val.to_le_bytes())),
                PrimitiveLiteral::Long(val) => Ok(ByteBuf::from(val.to_le_bytes())),
                PrimitiveLiteral::Float(val) => Ok(ByteBuf::from(val.to_le_bytes())),
                PrimitiveLiteral::Double(val) => Ok(ByteBuf::from(val.to_le_bytes())),
                PrimitiveLiteral::Date(val) => {
                    Ok(ByteBuf::from(date::date_to_days(&val)?.to_le_bytes()))
                }
                PrimitiveLiteral::Time(val) => Ok(ByteBuf::from(
                    time::time_to_microseconds(&val)?.to_le_bytes(),
                )),
                PrimitiveLiteral::Timestamp(val) => Ok(ByteBuf::from(
                    timestamp::datetime_to_microseconds(&val)?.to_le_bytes(),
                )),
                PrimitiveLiteral::TimestampTZ(val) => Ok(ByteBuf::from(
                    timestamptz::datetimetz_to_microseconds(&val)?.to_le_bytes(),
                )),
                PrimitiveLiteral::String(val) => Ok(ByteBuf::from(val.as_bytes())),
                PrimitiveLiteral::UUID(val) => Ok(ByteBuf::from(val.as_u128().to_be_bytes())),
                PrimitiveLiteral::Fixed(_, val) => Ok(ByteBuf::from(val)),
                PrimitiveLiteral::Binary(val) => Ok(ByteBuf::from(val)),
                PrimitiveLiteral::Decimal(_) => todo!(),
            },
            _ => Err(Error::new(
                crate::ErrorKind::DataInvalid,
                "Complex types can't be converted to bytes",
            )),
        }
    }
}

/// The partition struct stores the tuple of partition values for each file.
/// Its type is derived from the partition fields of the partition spec used to write the manifest file.
/// In v2, the partition struct’s field ids must match the ids from the partition spec.
#[derive(Debug, Clone, PartialEq)]
pub struct Struct {
    /// Vector to store the field values
    fields: Vec<Option<Literal>>,
    /// A lookup that matches the field name to the entry in the vector
    lookup: HashMap<String, usize>,
}

impl Deref for Struct {
    type Target = [Option<Literal>];

    fn deref(&self) -> &Self::Target {
        &self.fields
    }
}

impl Struct {
    /// Get reference to partition value
    pub fn get(&self, name: &str) -> Option<&Literal> {
        self.fields
            .get(*self.lookup.get(name)?)
            .and_then(|x| x.as_ref())
    }
    /// Get mutable reference to partition value
    pub fn get_mut(&mut self, name: &str) -> Option<&mut Literal> {
        self.fields
            .get_mut(*self.lookup.get(name)?)
            .and_then(|x| x.as_mut())
    }
}

impl Serialize for Struct {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut record = serializer.serialize_struct("r102", self.fields.len())?;
        for (i, value) in self.fields.iter().enumerate() {
            let (key, _) = self.lookup.iter().find(|(_, value)| **value == i).unwrap();
            record.serialize_field(Box::leak(key.clone().into_boxed_str()), value)?;
        }
        record.end()
    }
}

impl<'de> Deserialize<'de> for Struct {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct PartitionStructVisitor;

        impl<'de> Visitor<'de> for PartitionStructVisitor {
            type Value = Struct;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("map")
            }

            fn visit_map<V>(self, mut map: V) -> Result<Struct, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut fields: Vec<Option<Literal>> = Vec::new();
                let mut lookup: HashMap<String, usize> = HashMap::new();
                let mut index = 0;
                while let Some(key) = map.next_key()? {
                    fields.push(map.next_value()?);
                    lookup.insert(key, index);
                    index += 1;
                }
                Ok(Struct { fields, lookup })
            }
        }
        deserializer.deserialize_struct(
            "r102",
            Box::leak(vec![].into_boxed_slice()),
            PartitionStructVisitor,
        )
    }
}

impl FromIterator<(String, Option<Literal>)> for Struct {
    fn from_iter<I: IntoIterator<Item = (String, Option<Literal>)>>(iter: I) -> Self {
        let mut fields = Vec::new();
        let mut lookup = HashMap::new();

        for (i, (key, value)) in iter.into_iter().enumerate() {
            fields.push(value);
            lookup.insert(key, i);
        }

        Struct { fields, lookup }
    }
}

impl Literal {
    #[inline]
    /// Create iceberg value from bytes
    pub fn try_from_bytes(bytes: &[u8], data_type: &Type) -> Result<Self, Error> {
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
                    date::days_to_date(i32::from_le_bytes(bytes.try_into()?))?,
                ))),
                PrimitiveType::Time => Ok(Literal::Primitive(PrimitiveLiteral::Time(
                    time::microseconds_to_time(i64::from_le_bytes(bytes.try_into()?))?,
                ))),
                PrimitiveType::Timestamp => Ok(Literal::Primitive(PrimitiveLiteral::Timestamp(
                    timestamp::microseconds_to_datetime(i64::from_le_bytes(bytes.try_into()?))?,
                ))),
                PrimitiveType::Timestamptz => Ok(Literal::Primitive(
                    PrimitiveLiteral::TimestampTZ(timestamptz::microseconds_to_datetimetz(
                        i64::from_le_bytes(bytes.try_into()?),
                    )?),
                )),
                PrimitiveType::String => Ok(Literal::Primitive(PrimitiveLiteral::String(
                    std::str::from_utf8(bytes)?.to_string(),
                ))),
                PrimitiveType::Uuid => Ok(Literal::Primitive(PrimitiveLiteral::UUID(
                    Uuid::from_u128(u128::from_be_bytes(bytes.try_into()?)),
                ))),
                PrimitiveType::Fixed(len) => Ok(Literal::Primitive(PrimitiveLiteral::Fixed(
                    *len as usize,
                    Vec::from(bytes),
                ))),
                PrimitiveType::Binary => Ok(Literal::Primitive(PrimitiveLiteral::Binary(
                    Vec::from(bytes),
                ))),
                _ => todo!(),
            },
            _ => Err(Error::new(
                crate::ErrorKind::DataInvalid,
                "Converting bytes to non-primitive types is not supported.",
            )),
        }
    }

    /// Get datatype of value
    pub fn datatype(&self) -> Type {
        match self {
            Literal::Primitive(prim) => match prim {
                PrimitiveLiteral::Boolean(_) => Type::Primitive(PrimitiveType::Boolean),
                PrimitiveLiteral::Int(_) => Type::Primitive(PrimitiveType::Int),
                PrimitiveLiteral::Long(_) => Type::Primitive(PrimitiveType::Long),
                PrimitiveLiteral::Float(_) => Type::Primitive(PrimitiveType::Float),
                PrimitiveLiteral::Double(_) => Type::Primitive(PrimitiveType::Double),
                PrimitiveLiteral::Date(_) => Type::Primitive(PrimitiveType::Date),
                PrimitiveLiteral::Time(_) => Type::Primitive(PrimitiveType::Time),
                PrimitiveLiteral::Timestamp(_) => Type::Primitive(PrimitiveType::Timestamp),
                PrimitiveLiteral::TimestampTZ(_) => Type::Primitive(PrimitiveType::Timestamptz),
                PrimitiveLiteral::Fixed(len, _) => {
                    Type::Primitive(PrimitiveType::Fixed(*len as u64))
                }
                PrimitiveLiteral::Binary(_) => Type::Primitive(PrimitiveType::Binary),
                PrimitiveLiteral::String(_) => Type::Primitive(PrimitiveType::String),
                PrimitiveLiteral::UUID(_) => Type::Primitive(PrimitiveType::Uuid),
                PrimitiveLiteral::Decimal(dec) => Type::Primitive(PrimitiveType::Decimal {
                    precision: 38,
                    scale: dec.scale(),
                }),
            },
            _ => unimplemented!(),
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
                PrimitiveLiteral::Fixed(_, any) => Box::new(any),
                PrimitiveLiteral::Binary(any) => Box::new(any),
                PrimitiveLiteral::String(any) => Box::new(any),
                PrimitiveLiteral::UUID(any) => Box::new(any),
                PrimitiveLiteral::Decimal(any) => Box::new(any),
            },

            _ => unimplemented!(),
        }
    }
}

mod float {
    use ordered_float::OrderedFloat;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(value: &OrderedFloat<f32>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        value.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<OrderedFloat<f32>, D::Error>
    where
        D: Deserializer<'de>,
    {
        f32::deserialize(deserializer).map(OrderedFloat)
    }
}

mod double {
    use ordered_float::OrderedFloat;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(value: &OrderedFloat<f64>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        value.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<OrderedFloat<f64>, D::Error>
    where
        D: Deserializer<'de>,
    {
        f64::deserialize(deserializer).map(OrderedFloat)
    }
}

mod date {
    use chrono::NaiveDate;
    use serde::{de, ser, Deserialize, Deserializer, Serialize, Serializer};

    use crate::Error;

    pub fn serialize<S>(value: &NaiveDate, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let days = date_to_days(value).map_err(|err| ser::Error::custom(err.to_string()))?;
        days.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<NaiveDate, D::Error>
    where
        D: Deserializer<'de>,
    {
        let days = i32::deserialize(deserializer)?;

        days_to_date(days).map_err(|err| de::Error::custom(err.to_string()))
    }

    pub(crate) fn date_to_days(date: &NaiveDate) -> Result<i32, Error> {
        Ok(date
            .signed_duration_since(NaiveDate::from_ymd_opt(1970, 0, 0).ok_or(Error::new(
                crate::ErrorKind::DataInvalid,
                "Failed to get time from midnight",
            ))?)
            .num_days() as i32)
    }

    pub(crate) fn days_to_date(days: i32) -> Result<NaiveDate, Error> {
        NaiveDate::from_num_days_from_ce_opt(days).ok_or(Error::new(
            crate::ErrorKind::DataInvalid,
            "Failed to convert microseconds to time",
        ))
    }
}

mod time {
    use chrono::NaiveTime;
    use serde::{de, ser, Deserialize, Deserializer, Serialize, Serializer};

    use crate::Error;

    pub fn serialize<S>(value: &NaiveTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let micros =
            time_to_microseconds(value).map_err(|err| ser::Error::custom(err.to_string()))?;
        micros.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<NaiveTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let micros = i64::deserialize(deserializer)?;

        microseconds_to_time(micros).map_err(|err| de::Error::custom(err.to_string()))
    }

    pub(crate) fn time_to_microseconds(time: &NaiveTime) -> Result<i64, Error> {
        time.signed_duration_since(NaiveTime::from_num_seconds_from_midnight_opt(0, 0).ok_or(
            Error::new(
                crate::ErrorKind::DataInvalid,
                "Failed to get time from midnight",
            ),
        )?)
        .num_microseconds()
        .ok_or(Error::new(
            crate::ErrorKind::DataInvalid,
            "Failed to convert time to microseconds",
        ))
    }

    pub(crate) fn microseconds_to_time(micros: i64) -> Result<NaiveTime, Error> {
        let (secs, rem) = (micros / 1_000_000, micros % 1_000_000);

        NaiveTime::from_num_seconds_from_midnight_opt(secs as u32, rem as u32 * 1000).ok_or(
            Error::new(
                crate::ErrorKind::DataInvalid,
                "Failed to convert microseconds to time",
            ),
        )
    }
}

mod timestamp {
    use chrono::NaiveDateTime;
    use serde::{de, ser, Deserialize, Deserializer, Serialize, Serializer};

    use crate::Error;

    pub fn serialize<S>(value: &NaiveDateTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let micros =
            datetime_to_microseconds(value).map_err(|err| ser::Error::custom(err.to_string()))?;
        micros.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<NaiveDateTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let micros = i64::deserialize(deserializer)?;

        microseconds_to_datetime(micros).map_err(|err| de::Error::custom(err.to_string()))
    }

    pub(crate) fn datetime_to_microseconds(time: &NaiveDateTime) -> Result<i64, Error> {
        time.signed_duration_since(NaiveDateTime::from_timestamp_opt(0, 0).ok_or(Error::new(
            crate::ErrorKind::DataInvalid,
            "Failed to get time from midnight",
        ))?)
        .num_microseconds()
        .ok_or(Error::new(
            crate::ErrorKind::DataInvalid,
            "Failed to convert time to microseconds",
        ))
    }

    pub(crate) fn microseconds_to_datetime(micros: i64) -> Result<NaiveDateTime, Error> {
        let (secs, rem) = (micros / 1_000_000, micros % 1_000_000);

        NaiveDateTime::from_timestamp_opt(secs, rem as u32 * 1000).ok_or(Error::new(
            crate::ErrorKind::DataInvalid,
            "Failed to convert microseconds to time",
        ))
    }
}

mod timestamptz {
    use chrono::{DateTime, NaiveDateTime, Utc};
    use serde::{de, ser, Deserialize, Deserializer, Serialize, Serializer};

    use crate::Error;

    pub fn serialize<S>(value: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let micros =
            datetimetz_to_microseconds(value).map_err(|err| ser::Error::custom(err.to_string()))?;
        micros.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let micros = i64::deserialize(deserializer)?;

        microseconds_to_datetimetz(micros).map_err(|err| de::Error::custom(err.to_string()))
    }

    pub(crate) fn datetimetz_to_microseconds(time: &DateTime<Utc>) -> Result<i64, Error> {
        time.signed_duration_since(DateTime::<Utc>::from_utc(
            NaiveDateTime::from_timestamp_opt(0, 0).ok_or(Error::new(
                crate::ErrorKind::DataInvalid,
                "Failed to get time from midnight",
            ))?,
            Utc,
        ))
        .num_microseconds()
        .ok_or(Error::new(
            crate::ErrorKind::DataInvalid,
            "Failed to convert time to microseconds",
        ))
    }

    pub(crate) fn microseconds_to_datetimetz(micros: i64) -> Result<DateTime<Utc>, Error> {
        let (secs, rem) = (micros / 1_000_000, micros % 1_000_000);

        Ok(DateTime::<Utc>::from_utc(
            NaiveDateTime::from_timestamp_opt(secs, rem as u32 * 1000).ok_or(Error::new(
                crate::ErrorKind::DataInvalid,
                "Failed to convert microseconds to time",
            ))?,
            Utc,
        ))
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    pub fn boolean() {
        let input = Literal::Primitive(PrimitiveLiteral::Boolean(true));

        let raw_schema = r#""boolean""#;

        let schema = apache_avro::Schema::parse_str(raw_schema).unwrap();

        let mut writer = apache_avro::Writer::new(&schema, Vec::new());

        writer.append_ser(input.clone()).unwrap();

        let encoded = writer.into_inner().unwrap();

        let reader = apache_avro::Reader::new(&*encoded).unwrap();

        for record in reader {
            let result = apache_avro::from_value::<Literal>(&record.unwrap()).unwrap();
            assert_eq!(input, result);
        }
    }

    #[test]
    pub fn int() {
        let input = Literal::Primitive(PrimitiveLiteral::Int(42));

        let raw_schema = r#""int""#;

        let schema = apache_avro::Schema::parse_str(raw_schema).unwrap();

        let mut writer = apache_avro::Writer::new(&schema, Vec::new());

        writer.append_ser(input.clone()).unwrap();

        let encoded = writer.into_inner().unwrap();

        let reader = apache_avro::Reader::new(&*encoded).unwrap();

        for record in reader {
            let result = apache_avro::from_value::<Literal>(&record.unwrap()).unwrap();
            assert_eq!(input, result);
        }
    }

    #[test]
    pub fn float() {
        let input = Literal::Primitive(PrimitiveLiteral::Float(OrderedFloat(42.0)));

        let raw_schema = r#""float""#;

        let schema = apache_avro::Schema::parse_str(raw_schema).unwrap();

        let mut writer = apache_avro::Writer::new(&schema, Vec::new());

        writer.append_ser(input.clone()).unwrap();

        let encoded = writer.into_inner().unwrap();

        let reader = apache_avro::Reader::new(&*encoded).unwrap();

        for record in reader {
            let result = apache_avro::from_value::<Literal>(&record.unwrap()).unwrap();
            assert_eq!(input, result);
        }
    }

    #[test]
    pub fn string() {
        let input = Literal::Primitive(PrimitiveLiteral::String("test".to_string()));

        let raw_schema = r#""string""#;

        let schema = apache_avro::Schema::parse_str(raw_schema).unwrap();

        let mut writer = apache_avro::Writer::new(&schema, Vec::new());

        writer.append_ser(input.clone()).unwrap();

        let encoded = writer.into_inner().unwrap();

        let reader = apache_avro::Reader::new(&*encoded).unwrap();

        for record in reader {
            let result = apache_avro::from_value::<Literal>(&record.unwrap()).unwrap();
            assert_eq!(input, result);
        }
    }

    #[test]
    pub fn struct_value() {
        let input = Literal::Struct(Struct::from_iter(vec![(
            "name".to_string(),
            Some(Literal::Primitive(PrimitiveLiteral::String(
                "Alice".to_string(),
            ))),
        )]));

        let raw_schema = r#"{"type": "record","name": "r102","fields": [{
                    "name": "name", 
                    "type":  ["null","string"],
                    "default": null
                }]}"#;

        let schema = apache_avro::Schema::parse_str(raw_schema).unwrap();

        let mut writer = apache_avro::Writer::new(&schema, Vec::new());

        writer.append_ser(input.clone()).unwrap();

        let encoded = writer.into_inner().unwrap();

        let reader = apache_avro::Reader::new(&*encoded).unwrap();

        for record in reader {
            let result = apache_avro::from_value::<Literal>(&record.unwrap()).unwrap();
            assert_eq!(input, result);
        }
    }
}
