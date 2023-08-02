/*!
 * Value in iceberg
 */

use std::{any::Any, collections::HashMap, fmt, ops::Deref};

use rust_decimal::Decimal;
use serde::{
    de::{MapAccess, Visitor},
    ser::SerializeStruct,
    Deserialize, Deserializer, Serialize,
};
use serde_bytes::ByteBuf;

use crate::Error;

use super::datatypes::{PrimitiveType, Type};

/// Values present in iceberg type
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(untagged)]
pub enum Value {
    /// 0x00 for false, non-zero byte for true
    Boolean(bool),
    /// Stored as 4-byte little-endian
    Int(i32),
    /// Stored as 8-byte little-endian
    LongInt(i64),
    /// Stored as 4-byte little-endian
    Float(f32),
    /// Stored as 8-byte little-endian
    Double(f64),
    /// Stores days from the 1970-01-01 in an 4-byte little-endian int
    Date(i32),
    /// Stores microseconds from midnight in an 8-byte little-endian long
    Time(i64),
    /// Stores microseconds from 1970-01-01 00:00:00.000000 in an 8-byte little-endian long
    Timestamp(i64),
    /// Stores microseconds from 1970-01-01 00:00:00.000000 in an 8-byte little-endian long
    TimestampTZ(i64),
    /// UTF-8 bytes (without length)
    String(String),
    /// 16-byte big-endian value
    UUID(i128),
    /// Binary value
    Fixed(usize, Vec<u8>),
    /// Binary value (without length)
    Binary(Vec<u8>),
    /// Stores unscaled value as two’s-complement big-endian binary,
    /// using the minimum number of bytes for the value
    Decimal(Decimal),
    /// A struct is a tuple of typed values. Each field in the tuple is named and has an integer id that is unique in the table schema.
    /// Each field can be either optional or required, meaning that values can (or cannot) be null. Fields may be any type.
    /// Fields may have an optional comment or doc string. Fields can have default values.
    Struct(Struct),
    /// A list is a collection of values with some element type.
    /// The element field has an integer id that is unique in the table schema.
    /// Elements can be either optional or required. Element types may be any type.
    List(Vec<Option<Value>>),
    /// A map is a collection of key-value pairs with a key type and a value type.
    /// Both the key field and value field each have an integer id that is unique in the table schema.
    /// Map keys are required and map values can be either optional or required. Both map keys and map values may be any type, including nested types.
    Map(HashMap<String, Option<Value>>),
}

impl Into<ByteBuf> for Value {
    fn into(self) -> ByteBuf {
        match self {
            Self::Boolean(val) => {
                if val {
                    ByteBuf::from([0u8])
                } else {
                    ByteBuf::from([1u8])
                }
            }
            Self::Int(val) => ByteBuf::from(val.to_le_bytes()),
            Self::LongInt(val) => ByteBuf::from(val.to_le_bytes()),
            Self::Float(val) => ByteBuf::from(val.to_le_bytes()),
            Self::Double(val) => ByteBuf::from(val.to_le_bytes()),
            Self::Date(val) => ByteBuf::from(val.to_le_bytes()),
            Self::Time(val) => ByteBuf::from(val.to_le_bytes()),
            Self::Timestamp(val) => ByteBuf::from(val.to_le_bytes()),
            Self::TimestampTZ(val) => ByteBuf::from(val.to_le_bytes()),
            Self::String(val) => ByteBuf::from(val.as_bytes()),
            Self::UUID(val) => ByteBuf::from(val.to_be_bytes()),
            Self::Fixed(_, val) => ByteBuf::from(val),
            Self::Binary(val) => ByteBuf::from(val),
            _ => todo!(),
        }
    }
}

/// The partition struct stores the tuple of partition values for each file.
/// Its type is derived from the partition fields of the partition spec used to write the manifest file.
/// In v2, the partition struct’s field ids must match the ids from the partition spec.
#[derive(Debug, Clone, PartialEq)]
pub struct Struct {
    /// Vector to store the field values
    fields: Vec<Option<Value>>,
    /// A lookup that matches the field name to the entry in the vector
    lookup: HashMap<String, usize>,
}

impl Deref for Struct {
    type Target = [Option<Value>];

    fn deref(&self) -> &Self::Target {
        &self.fields
    }
}

impl Struct {
    /// Get reference to partition value
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.fields
            .get(*self.lookup.get(name)?)
            .and_then(|x| x.as_ref())
    }
    /// Get mutable reference to partition value
    pub fn get_mut(&mut self, name: &str) -> Option<&mut Value> {
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
                let mut fields: Vec<Option<Value>> = Vec::new();
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

impl FromIterator<(String, Option<Value>)> for Struct {
    fn from_iter<I: IntoIterator<Item = (String, Option<Value>)>>(iter: I) -> Self {
        let mut fields = Vec::new();
        let mut lookup = HashMap::new();

        for (i, (key, value)) in iter.into_iter().enumerate() {
            fields.push(value);
            lookup.insert(key, i);
        }

        Struct { fields, lookup }
    }
}

impl Value {
    #[inline]
    /// Create iceberg value from bytes
    pub fn try_from_bytes(bytes: &[u8], data_type: &Type) -> Result<Self, Error> {
        match data_type {
            Type::Primitive(primitive) => match primitive {
                PrimitiveType::Boolean => {
                    if bytes.len() == 1 && bytes[0] == 0u8 {
                        Ok(Value::Boolean(false))
                    } else {
                        Ok(Value::Boolean(true))
                    }
                }
                PrimitiveType::Int => Ok(Value::Int(i32::from_le_bytes(bytes.try_into()?))),
                PrimitiveType::Long => Ok(Value::LongInt(i64::from_le_bytes(bytes.try_into()?))),
                PrimitiveType::Float => Ok(Value::Float(f32::from_le_bytes(bytes.try_into()?))),
                PrimitiveType::Double => Ok(Value::Double(f64::from_le_bytes(bytes.try_into()?))),
                PrimitiveType::Date => Ok(Value::Date(i32::from_le_bytes(bytes.try_into()?))),
                PrimitiveType::Time => Ok(Value::Time(i64::from_le_bytes(bytes.try_into()?))),
                PrimitiveType::Timestamp => {
                    Ok(Value::Timestamp(i64::from_le_bytes(bytes.try_into()?)))
                }
                PrimitiveType::Timestamptz => {
                    Ok(Value::TimestampTZ(i64::from_le_bytes(bytes.try_into()?)))
                }
                PrimitiveType::String => Ok(Value::String(std::str::from_utf8(bytes)?.to_string())),
                PrimitiveType::Uuid => Ok(Value::UUID(i128::from_be_bytes(bytes.try_into()?))),
                PrimitiveType::Fixed(len) => Ok(Value::Fixed(*len as usize, Vec::from(bytes))),
                PrimitiveType::Binary => Ok(Value::Binary(Vec::from(bytes))),
                _ => Err(Error::new(
                    crate::ErrorKind::ValueByteConversionFailed,
                    "Converting bytes to decimal is not supported.",
                )),
            },
            _ => Err(Error::new(
                crate::ErrorKind::ValueByteConversionFailed,
                "Converting bytes to non-primitive types is not supported.",
            )),
        }
    }

    /// Get datatype of value
    pub fn datatype(&self) -> Type {
        match self {
            Value::Boolean(_) => Type::Primitive(PrimitiveType::Boolean),
            Value::Int(_) => Type::Primitive(PrimitiveType::Int),
            Value::LongInt(_) => Type::Primitive(PrimitiveType::Long),
            Value::Float(_) => Type::Primitive(PrimitiveType::Float),
            Value::Double(_) => Type::Primitive(PrimitiveType::Double),
            Value::Date(_) => Type::Primitive(PrimitiveType::Date),
            Value::Time(_) => Type::Primitive(PrimitiveType::Time),
            Value::Timestamp(_) => Type::Primitive(PrimitiveType::Timestamp),
            Value::TimestampTZ(_) => Type::Primitive(PrimitiveType::Timestamptz),
            Value::Fixed(len, _) => Type::Primitive(PrimitiveType::Fixed(*len as u64)),
            Value::Binary(_) => Type::Primitive(PrimitiveType::Binary),
            Value::String(_) => Type::Primitive(PrimitiveType::String),
            Value::UUID(_) => Type::Primitive(PrimitiveType::Uuid),
            Value::Decimal(dec) => Type::Primitive(PrimitiveType::Decimal {
                precision: 38,
                scale: dec.scale(),
            }),
            _ => unimplemented!(),
        }
    }

    /// Convert Value to the any type
    pub fn into_any(self) -> Box<dyn Any> {
        match self {
            Value::Boolean(any) => Box::new(any),
            Value::Int(any) => Box::new(any),
            Value::LongInt(any) => Box::new(any),
            Value::Float(any) => Box::new(any),
            Value::Double(any) => Box::new(any),
            Value::Date(any) => Box::new(any),
            Value::Time(any) => Box::new(any),
            Value::Timestamp(any) => Box::new(any),
            Value::TimestampTZ(any) => Box::new(any),
            Value::Fixed(_, any) => Box::new(any),
            Value::Binary(any) => Box::new(any),
            Value::String(any) => Box::new(any),
            Value::UUID(any) => Box::new(any),
            Value::Decimal(any) => Box::new(any),
            _ => unimplemented!(),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    pub fn boolean() {
        let input = Value::Boolean(true);

        let raw_schema = r#"{"type": "boolean"}"#;

        let schema = apache_avro::Schema::parse_str(raw_schema).unwrap();

        let mut writer = apache_avro::Writer::new(&schema, Vec::new());

        writer.append_ser(input.clone()).unwrap();

        let encoded = writer.into_inner().unwrap();

        let reader = apache_avro::Reader::new(&*encoded).unwrap();

        for record in reader {
            let result = apache_avro::from_value::<Value>(&record.unwrap()).unwrap();
            assert_eq!(input, result);
        }
    }

    #[test]
    pub fn int() {
        let input = Value::Int(42);

        let raw_schema = r#"{"type": "int"}"#;

        let schema = apache_avro::Schema::parse_str(raw_schema).unwrap();

        let mut writer = apache_avro::Writer::new(&schema, Vec::new());

        writer.append_ser(input.clone()).unwrap();

        let encoded = writer.into_inner().unwrap();

        let reader = apache_avro::Reader::new(&*encoded).unwrap();

        for record in reader {
            let result = apache_avro::from_value::<Value>(&record.unwrap()).unwrap();
            assert_eq!(input, result);
        }
    }

    #[test]
    pub fn float() {
        let input = Value::Float(42.0);

        let raw_schema = r#"{"type": "float"}"#;

        let schema = apache_avro::Schema::parse_str(raw_schema).unwrap();

        let mut writer = apache_avro::Writer::new(&schema, Vec::new());

        writer.append_ser(input.clone()).unwrap();

        let encoded = writer.into_inner().unwrap();

        let reader = apache_avro::Reader::new(&*encoded).unwrap();

        for record in reader {
            let result = apache_avro::from_value::<Value>(&record.unwrap()).unwrap();
            assert_eq!(input, result);
        }
    }

    #[test]
    pub fn string() {
        let input = Value::String("test".to_string());

        let raw_schema = r#"{"type": "string"}"#;

        let schema = apache_avro::Schema::parse_str(raw_schema).unwrap();

        let mut writer = apache_avro::Writer::new(&schema, Vec::new());

        writer.append_ser(input.clone()).unwrap();

        let encoded = writer.into_inner().unwrap();

        let reader = apache_avro::Reader::new(&*encoded).unwrap();

        for record in reader {
            let result = apache_avro::from_value::<Value>(&record.unwrap()).unwrap();
            assert_eq!(input, result);
        }
    }

    #[test]
    pub fn struct_value() {
        let input = Value::Struct(Struct::from_iter(vec![(
            "name".to_string(),
            Some(Value::String("Alice".to_string())),
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
            let result = apache_avro::from_value::<Value>(&record.unwrap()).unwrap();
            assert_eq!(input, result);
        }
    }
}
