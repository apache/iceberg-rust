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

use std::{any::Any, collections::BTreeMap};

use bitvec::vec::BitVec;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use ordered_float::OrderedFloat;
use rust_decimal::Decimal;
use serde_bytes::ByteBuf;
use serde_json::{Map as JsonMap, Number, Value as JsonValue};
use uuid::Uuid;

use crate::Error;

use super::datatypes::{PrimitiveType, Type};

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
    Date(NaiveDate),
    /// Stores microseconds from midnight in an 8-byte little-endian long
    Time(NaiveTime),
    /// Timestamp without timezone
    Timestamp(NaiveDateTime),
    /// Timestamp with timezone
    TimestampTZ(DateTime<Utc>),
    /// UTF-8 bytes (without length)
    String(String),
    /// 16-byte big-endian value
    UUID(Uuid),
    /// Binary value
    Fixed(Vec<u8>),
    /// Binary value (without length)
    Binary(Vec<u8>),
    /// Stores unscaled value as two’s-complement big-endian binary,
    /// using the minimum number of bytes for the value
    Decimal(Decimal),
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

impl From<Literal> for ByteBuf {
    fn from(value: Literal) -> Self {
        match value {
            Literal::Primitive(prim) => match prim {
                PrimitiveLiteral::Boolean(val) => {
                    if val {
                        ByteBuf::from([0u8])
                    } else {
                        ByteBuf::from([1u8])
                    }
                }
                PrimitiveLiteral::Int(val) => ByteBuf::from(val.to_le_bytes()),
                PrimitiveLiteral::Long(val) => ByteBuf::from(val.to_le_bytes()),
                PrimitiveLiteral::Float(val) => ByteBuf::from(val.to_le_bytes()),
                PrimitiveLiteral::Double(val) => ByteBuf::from(val.to_le_bytes()),
                PrimitiveLiteral::Date(val) => {
                    ByteBuf::from(date::date_to_days(&val).to_le_bytes())
                }
                PrimitiveLiteral::Time(val) => {
                    ByteBuf::from(time::time_to_microseconds(&val).to_le_bytes())
                }
                PrimitiveLiteral::Timestamp(val) => {
                    ByteBuf::from(timestamp::datetime_to_microseconds(&val).to_le_bytes())
                }
                PrimitiveLiteral::TimestampTZ(val) => {
                    ByteBuf::from(timestamptz::datetimetz_to_microseconds(&val).to_le_bytes())
                }
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

impl From<&Literal> for JsonValue {
    fn from(value: &Literal) -> Self {
        match value {
            Literal::Primitive(prim) => match prim {
                PrimitiveLiteral::Boolean(val) => JsonValue::Bool(*val),
                PrimitiveLiteral::Int(val) => JsonValue::Number((*val).into()),
                PrimitiveLiteral::Long(val) => JsonValue::Number((*val).into()),
                PrimitiveLiteral::Float(val) => match Number::from_f64(val.0 as f64) {
                    Some(number) => JsonValue::Number(number),
                    None => JsonValue::Null,
                },
                PrimitiveLiteral::Double(val) => match Number::from_f64(val.0) {
                    Some(number) => JsonValue::Number(number),
                    None => JsonValue::Null,
                },
                PrimitiveLiteral::Date(val) => JsonValue::String(val.to_string()),
                PrimitiveLiteral::Time(val) => JsonValue::String(val.to_string()),
                PrimitiveLiteral::Timestamp(val) => {
                    JsonValue::String(val.format("%Y-%m-%dT%H:%M:%S%.f").to_string())
                }
                PrimitiveLiteral::TimestampTZ(val) => {
                    JsonValue::String(val.format("%Y-%m-%dT%H:%M:%S%.f+00:00").to_string())
                }
                PrimitiveLiteral::String(val) => JsonValue::String(val.clone()),
                PrimitiveLiteral::UUID(val) => JsonValue::String(val.to_string()),
                PrimitiveLiteral::Fixed(val) => {
                    JsonValue::String(val.iter().fold(String::new(), |mut acc, x| {
                        acc.push_str(&format!("{:x}", x));
                        acc
                    }))
                }
                PrimitiveLiteral::Binary(val) => {
                    JsonValue::String(val.iter().fold(String::new(), |mut acc, x| {
                        acc.push_str(&format!("{:x}", x));
                        acc
                    }))
                }
                PrimitiveLiteral::Decimal(_) => todo!(),
            },
            Literal::Struct(s) => {
                JsonValue::Object(JsonMap::from_iter(s.iter().map(|(id, value, _)| {
                    let json: JsonValue = match value {
                        Some(val) => val.into(),
                        None => JsonValue::Null,
                    };
                    (id.to_string(), json)
                })))
            }
            Literal::List(list) => JsonValue::Array(
                list.into_iter()
                    .map(|opt| match opt {
                        Some(literal) => literal.into(),
                        None => JsonValue::Null,
                    })
                    .collect(),
            ),
            Literal::Map(map) => {
                let mut object = JsonMap::with_capacity(2);
                object.insert(
                    "keys".to_string(),
                    JsonValue::Array(map.keys().map(|literal| literal.into()).collect()),
                );
                object.insert(
                    "values".to_string(),
                    JsonValue::Array(
                        map.values()
                            .map(|literal| match literal {
                                Some(literal) => literal.into(),
                                None => JsonValue::Null,
                            })
                            .collect(),
                    ),
                );
                JsonValue::Object(object)
            }
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
    /// Vector to store the field ids
    field_ids: Vec<i32>,
    /// Vector to store the field names
    field_names: Vec<String>,
    /// Null bitmap
    null_bitmap: BitVec,
}

impl Struct {
    /// Create a iterator to read the field in order of (field_id, field_value, field_name).
    pub fn iter(&self) -> impl Iterator<Item = (&i32, Option<&Literal>, &str)> {
        self.null_bitmap
            .iter()
            .zip(self.fields.iter())
            .zip(self.field_ids.iter())
            .zip(self.field_names.iter())
            .map(|(((null, value), id), name)| {
                (id, if *null { None } else { Some(value) }, name.as_str())
            })
    }
}

impl FromIterator<(i32, Option<Literal>, String)> for Struct {
    fn from_iter<I: IntoIterator<Item = (i32, Option<Literal>, String)>>(iter: I) -> Self {
        let mut fields = Vec::new();
        let mut field_ids = Vec::new();
        let mut field_names = Vec::new();
        let mut null_bitmap = BitVec::new();

        for (id, value, name) in iter.into_iter() {
            field_ids.push(id);
            field_names.push(name);
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
            field_ids,
            field_names,
            null_bitmap,
        }
    }
}

impl Literal {
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
                PrimitiveType::Fixed(_) => Ok(Literal::Primitive(PrimitiveLiteral::Fixed(
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

    /// Create iceberg value from a json value
    pub fn try_from_json(value: JsonValue, data_type: &Type) -> Result<Self, Error> {
        match data_type {
            Type::Primitive(primitive) => match (primitive, value) {
                (PrimitiveType::Boolean, JsonValue::Bool(bool)) => {
                    Ok(Literal::Primitive(PrimitiveLiteral::Boolean(bool)))
                }
                (PrimitiveType::Int, JsonValue::Number(number)) => {
                    Ok(Literal::Primitive(PrimitiveLiteral::Int(
                        number
                            .as_i64()
                            .ok_or(Error::new(
                                crate::ErrorKind::DataInvalid,
                                "Failed to convert json number to int",
                            ))?
                            .try_into()?,
                    )))
                }
                (PrimitiveType::Long, JsonValue::Number(number)) => Ok(Literal::Primitive(
                    PrimitiveLiteral::Long(number.as_i64().ok_or(Error::new(
                        crate::ErrorKind::DataInvalid,
                        "Failed to convert json number to long",
                    ))?),
                )),
                (PrimitiveType::Float, JsonValue::Number(number)) => Ok(Literal::Primitive(
                    PrimitiveLiteral::Float(OrderedFloat(number.as_f64().ok_or(Error::new(
                        crate::ErrorKind::DataInvalid,
                        "Failed to convert json number to float",
                    ))? as f32)),
                )),
                (PrimitiveType::Double, JsonValue::Number(number)) => Ok(Literal::Primitive(
                    PrimitiveLiteral::Double(OrderedFloat(number.as_f64().ok_or(Error::new(
                        crate::ErrorKind::DataInvalid,
                        "Failed to convert json number to double",
                    ))?)),
                )),
                (PrimitiveType::Date, JsonValue::String(s)) => Ok(Literal::Primitive(
                    PrimitiveLiteral::Date(NaiveDate::parse_from_str(&s, "%Y-%m-%d")?),
                )),
                (PrimitiveType::Time, JsonValue::String(s)) => Ok(Literal::Primitive(
                    PrimitiveLiteral::Time(NaiveTime::parse_from_str(&s, "%H:%M:%S%.f")?),
                )),
                (PrimitiveType::Timestamp, JsonValue::String(s)) => {
                    Ok(Literal::Primitive(PrimitiveLiteral::Timestamp(
                        NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.f")?,
                    )))
                }
                (PrimitiveType::Timestamptz, JsonValue::String(s)) => Ok(Literal::Primitive(
                    PrimitiveLiteral::TimestampTZ(DateTime::from_utc(
                        NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.f+00:00")?,
                        Utc,
                    )),
                )),
                (PrimitiveType::String, JsonValue::String(s)) => {
                    Ok(Literal::Primitive(PrimitiveLiteral::String(s)))
                }
                (PrimitiveType::Uuid, JsonValue::String(s)) => Ok(Literal::Primitive(
                    PrimitiveLiteral::UUID(Uuid::parse_str(&s)?),
                )),
                (PrimitiveType::Fixed(_), JsonValue::String(_)) => todo!(),
                (PrimitiveType::Binary, JsonValue::String(_)) => todo!(),
                _ => todo!(),
            },
            Type::Struct(schema) => {
                if let JsonValue::Object(mut object) = value {
                    Ok(Literal::Struct(Struct::from_iter(schema.iter().map(
                        |field| {
                            (
                                field.id,
                                object.remove(&field.id.to_string()).and_then(|value| {
                                    Literal::try_from_json(value, &field.field_type).ok()
                                }),
                                field.name.clone(),
                            )
                        },
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
                    Ok(Literal::List(
                        array
                            .into_iter()
                            .map(|value| {
                                Ok(Some(Literal::try_from_json(
                                    value,
                                    &list.element_field.field_type,
                                )?))
                            })
                            .collect::<Result<Vec<_>, Error>>()?,
                    ))
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
                        Ok(Literal::Map(BTreeMap::from_iter(
                            keys.into_iter()
                                .zip(values.into_iter())
                                .map(|(key, value)| {
                                    Ok((
                                        Literal::try_from_json(key, &map.key_field.field_type)?,
                                        Some(Literal::try_from_json(
                                            value,
                                            &map.value_field.field_type,
                                        )?),
                                    ))
                                })
                                .collect::<Result<Vec<_>, Error>>()?
                                .into_iter(),
                        )))
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
                PrimitiveLiteral::Fixed(vec) => {
                    Type::Primitive(PrimitiveType::Fixed(vec.len() as u64))
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
    use chrono::NaiveDate;

    use crate::Error;

    pub(crate) fn date_to_days(date: &NaiveDate) -> i32 {
        date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 0, 0).unwrap())
            .num_days() as i32
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

    use crate::Error;

    pub(crate) fn time_to_microseconds(time: &NaiveTime) -> i64 {
        time.signed_duration_since(NaiveTime::from_num_seconds_from_midnight_opt(0, 0).unwrap())
            .num_microseconds()
            .unwrap()
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

    use crate::Error;

    pub(crate) fn datetime_to_microseconds(time: &NaiveDateTime) -> i64 {
        time.timestamp_micros()
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

    use crate::Error;

    pub(crate) fn datetimetz_to_microseconds(time: &DateTime<Utc>) -> i64 {
        time.timestamp_micros()
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

    use crate::spec::datatypes::{ListType, MapType, NestedField, StructType};

    use super::*;

    fn check_json_serde(json: &str, expected_literal: Literal, expected_type: &Type) {
        let raw_json_value = serde_json::from_str::<JsonValue>(json).unwrap();
        let desered_literal =
            Literal::try_from_json(raw_json_value.clone(), expected_type).unwrap();
        assert_eq!(desered_literal, expected_literal);

        let expected_json_value: JsonValue = (&expected_literal).into();
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
        writer.append_ser(bytes).unwrap();
        let encoded = writer.into_inner().unwrap();
        let reader = apache_avro::Reader::new(&*encoded).unwrap();

        for record in reader {
            let result = apache_avro::from_value::<ByteBuf>(&record.unwrap()).unwrap();
            let desered_literal = Literal::try_from_bytes(&result, expected_type).unwrap();
            assert_eq!(desered_literal, expected_literal);
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
            Literal::Primitive(PrimitiveLiteral::Date(
                NaiveDate::from_ymd_opt(2017, 11, 16).unwrap(),
            )),
            &Type::Primitive(PrimitiveType::Date),
        );
    }

    #[test]
    fn json_time() {
        let record = r#""22:31:08.123456""#;

        check_json_serde(
            record,
            Literal::Primitive(PrimitiveLiteral::Time(
                NaiveTime::from_hms_micro_opt(22, 31, 8, 123456).unwrap(),
            )),
            &Type::Primitive(PrimitiveType::Time),
        );
    }

    #[test]
    fn json_timestamp() {
        let record = r#""2017-11-16T22:31:08.123456""#;

        check_json_serde(
            record,
            Literal::Primitive(PrimitiveLiteral::Timestamp(NaiveDateTime::new(
                NaiveDate::from_ymd_opt(2017, 11, 16).unwrap(),
                NaiveTime::from_hms_micro_opt(22, 31, 8, 123456).unwrap(),
            ))),
            &Type::Primitive(PrimitiveType::Timestamp),
        );
    }

    #[test]
    fn json_timestamptz() {
        let record = r#""2017-11-16T22:31:08.123456+00:00""#;

        check_json_serde(
            record,
            Literal::Primitive(PrimitiveLiteral::TimestampTZ(DateTime::<Utc>::from_utc(
                NaiveDateTime::new(
                    NaiveDate::from_ymd_opt(2017, 11, 16).unwrap(),
                    NaiveTime::from_hms_micro_opt(22, 31, 8, 123456).unwrap(),
                ),
                Utc,
            ))),
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
    fn json_struct() {
        let record = r#"{"1": 1, "2": "bar"}"#;

        check_json_serde(
            record,
            Literal::Struct(Struct::from_iter(vec![
                (
                    1,
                    Some(Literal::Primitive(PrimitiveLiteral::Int(1))),
                    "id".to_string(),
                ),
                (
                    2,
                    Some(Literal::Primitive(PrimitiveLiteral::String(
                        "bar".to_string(),
                    ))),
                    "name".to_string(),
                ),
            ])),
            &Type::Struct(StructType::new(vec![
                NestedField {
                    id: 1,
                    name: "id".to_string(),
                    required: true,
                    field_type: Box::new(Type::Primitive(PrimitiveType::Int)),
                    doc: None,
                    initial_default: None,
                    write_default: None,
                },
                NestedField {
                    id: 2,
                    name: "name".to_string(),
                    required: false,
                    field_type: Box::new(Type::Primitive(PrimitiveType::String)),
                    doc: None,
                    initial_default: None,
                    write_default: None,
                },
            ])),
        );
    }

    #[test]
    fn json_list() {
        let record = r#"[1, 2, 3]"#;

        check_json_serde(
            record,
            Literal::List(vec![
                Some(Literal::Primitive(PrimitiveLiteral::Int(1))),
                Some(Literal::Primitive(PrimitiveLiteral::Int(2))),
                Some(Literal::Primitive(PrimitiveLiteral::Int(3))),
            ]),
            &Type::List(ListType {
                element_field: NestedField {
                    id: 0,
                    name: "".to_string(),
                    required: true,
                    field_type: Box::new(Type::Primitive(PrimitiveType::Int)),
                    doc: None,
                    initial_default: None,
                    write_default: None,
                },
            }),
        );
    }

    #[test]
    fn json_map() {
        let record = r#"{ "keys": ["a", "b"], "values": [1, 2] }"#;

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
            ])),
            &Type::Map(MapType {
                key_field: NestedField {
                    id: 0,
                    name: "key".to_string(),
                    required: true,
                    field_type: Box::new(Type::Primitive(PrimitiveType::String)),
                    doc: None,
                    initial_default: None,
                    write_default: None,
                },
                value_field: NestedField {
                    id: 1,
                    name: "value".to_string(),
                    required: true,
                    field_type: Box::new(Type::Primitive(PrimitiveType::Int)),
                    doc: None,
                    initial_default: None,
                    write_default: None,
                },
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
}
