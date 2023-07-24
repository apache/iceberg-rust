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
 * Data Types
*/
use std::{fmt, ops::Index};

use serde::{
    de::{Error, IntoDeserializer},
    Deserialize, Deserializer, Serialize, Serializer,
};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(untagged)]
/// All data types are either primitives or nested types, which are maps, lists, or structs.
pub enum Type {
    /// Primitive types
    Primitive(PrimitiveType),
    /// Struct type
    Struct(StructType),
    /// List type.
    List(ListType),
    /// Map type
    Map(MapType),
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Type::Primitive(primitive) => write!(f, "{}", primitive),
            Type::Struct(_) => write!(f, "struct"),
            Type::List(_) => write!(f, "list"),
            Type::Map(_) => write!(f, "map"),
        }
    }
}

/// Primitive data types
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "lowercase", remote = "Self")]
pub enum PrimitiveType {
    /// True or False
    Boolean,
    /// 32-bit signed integer
    Int,
    /// 64-bit signed integer
    Long,
    /// 32-bit IEEE 753 floating bit.
    Float,
    /// 64-bit IEEE 753 floating bit.
    Double,
    /// Fixed point decimal
    Decimal {
        /// Precision
        precision: u32,
        /// Scale
        scale: u32,
    },
    /// Calendar date without timezone or time.
    Date,
    /// Time of day without date or timezone.
    Time,
    /// Timestamp without timezone
    Timestamp,
    /// Timestamp with timezone
    Timestamptz,
    /// Arbitrary-length character sequences encoded in utf-8
    String,
    /// Universally Unique Identifiers
    Uuid,
    /// Fixed length byte array
    Fixed(u64),
    /// Arbitrary-length byte array.
    Binary,
}

impl<'de> Deserialize<'de> for PrimitiveType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s.starts_with("decimal") {
            deserialize_decimal(s.into_deserializer())
        } else if s.starts_with("fixed") {
            deserialize_fixed(s.into_deserializer())
        } else {
            PrimitiveType::deserialize(s.into_deserializer())
        }
    }
}

impl Serialize for PrimitiveType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            PrimitiveType::Decimal { precision, scale } => {
                serialize_decimal(precision, scale, serializer)
            }
            PrimitiveType::Fixed(l) => serialize_fixed(l, serializer),
            _ => PrimitiveType::serialize(self, serializer),
        }
    }
}

fn deserialize_decimal<'de, D>(deserializer: D) -> Result<PrimitiveType, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    let (precision, scale) = s
        .trim_start_matches(r"decimal(")
        .trim_end_matches(')')
        .split_once(',')
        .ok_or_else(|| D::Error::custom("Decimal requires precision and scale: {s}"))?;

    Ok(PrimitiveType::Decimal {
        precision: precision.trim().parse().map_err(D::Error::custom)?,
        scale: scale.trim().parse().map_err(D::Error::custom)?,
    })
}

fn serialize_decimal<S>(precision: &u32, scale: &u32, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&format!("decimal({precision},{scale})"))
}

fn deserialize_fixed<'de, D>(deserializer: D) -> Result<PrimitiveType, D::Error>
where
    D: Deserializer<'de>,
{
    let fixed = String::deserialize(deserializer)?
        .trim_start_matches(r"fixed[")
        .trim_end_matches(']')
        .to_owned();

    fixed
        .parse()
        .map(PrimitiveType::Fixed)
        .map_err(D::Error::custom)
}

fn serialize_fixed<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&format!("fixed[{value}]"))
}

impl fmt::Display for PrimitiveType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PrimitiveType::Boolean => write!(f, "boolean"),
            PrimitiveType::Int => write!(f, "int"),
            PrimitiveType::Long => write!(f, "long"),
            PrimitiveType::Float => write!(f, "float"),
            PrimitiveType::Double => write!(f, "double"),
            PrimitiveType::Decimal { precision, scale } => {
                write!(f, "decimal({},{})", precision, scale)
            }
            PrimitiveType::Date => write!(f, "date"),
            PrimitiveType::Time => write!(f, "time"),
            PrimitiveType::Timestamp => write!(f, "timestamp"),
            PrimitiveType::Timestamptz => write!(f, "timestamptz"),
            PrimitiveType::String => write!(f, "string"),
            PrimitiveType::Uuid => write!(f, "uuid"),
            PrimitiveType::Fixed(size) => write!(f, "fixed({})", size),
            PrimitiveType::Binary => write!(f, "binary"),
        }
    }
}

/// DataType for a specific struct
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename = "struct", tag = "type")]
pub struct StructType {
    /// Struct fields
    fields: Vec<StructField>,
}

impl StructType {
    /// Get structfield with certain id
    pub fn get(&self, id: usize) -> Option<&StructField> {
        self.fields.iter().find(|field| field.id as usize == id)
    }
    /// Get structfield with certain name
    pub fn get_name(&self, name: &str) -> Option<&StructField> {
        self.fields.iter().find(|field| field.name == name)
    }
}

impl Index<usize> for StructType {
    type Output = StructField;

    fn index(&self, index: usize) -> &Self::Output {
        &self.fields[index]
    }
}

impl fmt::Display for StructType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "struct<")?;
        for field in &self.fields {
            write!(f, "{}", field.field_type)?;
        }
        write!(f, ">")
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// A struct is a tuple of typed values. Each field in the tuple is named and has an integer id that is unique in the table schema.
/// Each field can be either optional or required, meaning that values can (or cannot) be null. Fields may be any type.
/// Fields may have an optional comment or doc string. Fields can have default values.
pub struct StructField {
    /// Id unique in table schema
    pub id: i32,
    /// Field Name
    pub name: String,
    /// Optional or required
    pub required: bool,
    /// Datatype
    #[serde(rename = "type")]
    pub field_type: Type,
    /// Fields may have an optional comment or doc string.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
    /// Used to populate the field’s value for all records that were written before the field was added to the schema
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial_default: Option<String>,
    /// Used to populate the field’s value for any records written after the field was added to the schema, if the writer does not supply the field’s value
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write_default: Option<String>,
}

impl fmt::Display for StructField {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}: ", self.id)?;
        write!(f, "{}: ", self.name)?;
        if self.required {
            write!(f, "required ")?;
        } else {
            write!(f, "optional ")?;
        }
        write!(f, "{} ", self.field_type)?;
        if let Some(doc) = &self.doc {
            write!(f, "{}", doc)?;
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename = "list", rename_all = "kebab-case", tag = "type")]
/// A list is a collection of values with some element type. The element field has an integer id that is unique in the table schema.
/// Elements can be either optional or required. Element types may be any type.
pub struct ListType {
    /// Id unique in table schema
    pub element_id: i32,

    /// Elements can be either optional or required.
    pub element_required: bool,

    /// Datatype
    pub element: Box<Type>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename = "map", rename_all = "kebab-case", tag = "type")]
/// A map is a collection of key-value pairs with a key type and a value type.
/// Both the key field and value field each have an integer id that is unique in the table schema.
/// Map keys are required and map values can be either optional or required.
/// Both map keys and map values may be any type, including nested types.
pub struct MapType {
    /// Key Id that is unique in table schema
    pub key_id: i32,
    /// Datatype of key
    pub key: Box<Type>,
    /// Value Id that is unique in table schema
    pub value_id: i32,
    /// If value is optional or required
    pub value_required: bool,
    /// Datatype of value
    pub value: Box<Type>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check_type_serde(json: &str, expected_type: Type) {
        let desered_type: Type = serde_json::from_str(json).unwrap();
        assert_eq!(desered_type, expected_type);

        let sered_json = serde_json::to_string(&expected_type).unwrap();
        let parsed_json_value = serde_json::from_str::<serde_json::Value>(&sered_json).unwrap();
        let raw_json_value = serde_json::from_str::<serde_json::Value>(json).unwrap();

        assert_eq!(parsed_json_value, raw_json_value);
    }

    #[test]
    fn decimal() {
        let record = r#"
        {
            "type": "struct",
            "fields": [
                {
                    "id": 1,
                    "name": "id",
                    "required": true,
                    "type": "decimal(9,2)"
                }
            ]
        }
        "#;

        check_type_serde(
            record,
            Type::Struct(StructType {
                fields: vec![StructField {
                    id: 1,
                    name: "id".to_string(),
                    required: true,
                    field_type: Type::Primitive(PrimitiveType::Decimal {
                        precision: 9,
                        scale: 2,
                    }),
                    doc: None,
                    initial_default: None,
                    write_default: None,
                }],
            }),
        )
    }

    #[test]
    fn fixed() {
        let record = r#"
        {
            "type": "struct",
            "fields": [
                {
                    "id": 1,
                    "name": "id",
                    "required": true,
                    "type": "fixed[8]"
                }
            ]
        }
        "#;

        check_type_serde(
            record,
            Type::Struct(StructType {
                fields: vec![StructField {
                    id: 1,
                    name: "id".to_string(),
                    required: true,
                    field_type: Type::Primitive(PrimitiveType::Fixed(8)),
                    doc: None,
                    initial_default: None,
                    write_default: None,
                }],
            }),
        )
    }

    #[test]
    fn struct_type() {
        let record = r#"
        {
            "type": "struct",
            "fields": [ 
                {
                    "id": 1,
                    "name": "id",
                    "required": true,
                    "type": "uuid",
                    "initial-default": "0db3e2a8-9d1d-42b9-aa7b-74ebe558dceb",
                    "write-default": "ec5911be-b0a7-458c-8438-c9a3e53cffae"
                }, {
                    "id": 2,
                    "name": "data",
                    "required": false,
                    "type": "int"
                } 
            ]
        }
        "#;

        check_type_serde(
            record,
            Type::Struct(StructType {
                fields: vec![
                    StructField {
                        id: 1,
                        name: "id".to_string(),
                        required: true,
                        field_type: Type::Primitive(PrimitiveType::Uuid),
                        doc: None,
                        initial_default: Some("0db3e2a8-9d1d-42b9-aa7b-74ebe558dceb".to_string()),
                        write_default: Some("ec5911be-b0a7-458c-8438-c9a3e53cffae".to_string()),
                    },
                    StructField {
                        id: 2,
                        name: "data".to_string(),
                        required: false,
                        field_type: Type::Primitive(PrimitiveType::Int),
                        doc: None,
                        initial_default: None,
                        write_default: None,
                    },
                ],
            }),
        )
    }

    #[test]
    fn list() {
        let record = r#"
        {
            "type": "list",
            "element-id": 3,
            "element-required": true,
            "element": "string"
        }
        "#;

        check_type_serde(
            record,
            Type::List(ListType {
                element_id: 3,
                element_required: true,
                element: Box::new(Type::Primitive(PrimitiveType::String)),
            }),
        );
    }

    #[test]
    fn map() {
        let record = r#"
        {
            "type": "map",
            "key-id": 4,
            "key": "string",
            "value-id": 5,
            "value-required": false,
            "value": "double"
        }
        "#;

        check_type_serde(
            record,
            Type::Map(MapType {
                key_id: 4,
                key: Box::new(Type::Primitive(PrimitiveType::String)),
                value_id: 5,
                value_required: false,
                value: Box::new(Type::Primitive(PrimitiveType::Double)),
            }),
        );
    }

    #[test]
    fn map_int() {
        let record = r#"
        {
            "type": "map",
            "key-id": 4,
            "key": "int",
            "value-id": 5,
            "value-required": false,
            "value": "string"
        }
        "#;

        check_type_serde(
            record,
            Type::Map(MapType {
                key_id: 4,
                key: Box::new(Type::Primitive(PrimitiveType::Int)),
                value_id: 5,
                value_required: false,
                value: Box::new(Type::Primitive(PrimitiveType::String)),
            }),
        );
    }
}
