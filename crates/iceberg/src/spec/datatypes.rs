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
use crate::ensure_data_valid;
use crate::error::Result;
use crate::spec::datatypes::_decimal::{MAX_PRECISION, REQUIRED_LENGTH};
use ::serde::de::{MapAccess, Visitor};
use serde::de::{Error, IntoDeserializer};
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value as JsonValue;
use std::convert::identity;
use std::sync::Arc;
use std::sync::OnceLock;
use std::{collections::HashMap, fmt, ops::Index};

use super::values::Literal;

/// Field name for list type.
pub(crate) const LIST_FILED_NAME: &str = "element";
pub(crate) const MAP_KEY_FIELD_NAME: &str = "key";
pub(crate) const MAP_VALUE_FIELD_NAME: &str = "value";

pub(crate) const MAX_DECIMAL_BYTES: u32 = 24;
pub(crate) const MAX_DECIMAL_PRECISION: u32 = 38;

mod _decimal {
    use lazy_static::lazy_static;

    use crate::spec::{MAX_DECIMAL_BYTES, MAX_DECIMAL_PRECISION};

    lazy_static! {
        // Max precision of bytes, starts from 1
        pub(super) static ref MAX_PRECISION: [u32; MAX_DECIMAL_BYTES as usize] = {
            let mut ret: [u32; 24] = [0; 24];
            for (i, prec) in ret.iter_mut().enumerate() {
                *prec = 2f64.powi((8 * (i + 1) - 1) as i32).log10().floor() as u32;
            }

            ret
        };

        //  Required bytes of precision, starts from 1
        pub(super) static ref REQUIRED_LENGTH: [u32; MAX_DECIMAL_PRECISION as usize] = {
            let mut ret: [u32; MAX_DECIMAL_PRECISION as usize] = [0; MAX_DECIMAL_PRECISION as usize];

            for (i, required_len) in ret.iter_mut().enumerate() {
                for j in 0..MAX_PRECISION.len() {
                    if MAX_PRECISION[j] >= ((i+1) as u32) {
                        *required_len = (j+1) as u32;
                        break;
                    }
                }
            }

            ret
        };

    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
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
            Type::Struct(s) => write!(f, "{}", s),
            Type::List(_) => write!(f, "list"),
            Type::Map(_) => write!(f, "map"),
        }
    }
}

impl Type {
    /// Whether the type is primitive type.
    #[inline(always)]
    pub fn is_primitive(&self) -> bool {
        matches!(self, Type::Primitive(_))
    }

    /// Whether the type is struct type.
    #[inline(always)]
    pub fn is_struct(&self) -> bool {
        matches!(self, Type::Struct(_))
    }

    /// Return max precision for decimal given [`num_bytes`] bytes.
    #[inline(always)]
    pub fn decimal_max_precision(num_bytes: u32) -> Result<u32> {
        ensure_data_valid!(
            num_bytes > 0 && num_bytes <= MAX_DECIMAL_BYTES,
            "Decimal length larger than {MAX_DECIMAL_BYTES} is not supported: {num_bytes}",
        );
        Ok(MAX_PRECISION[num_bytes as usize - 1])
    }

    /// Returns minimum bytes required for decimal with [`precision`].
    #[inline(always)]
    pub fn decimal_required_bytes(precision: u32) -> Result<u32> {
        ensure_data_valid!(precision > 0 && precision <= MAX_DECIMAL_PRECISION, "Decimals with precision larger than {MAX_DECIMAL_PRECISION} are not supported: {precision}",);
        Ok(REQUIRED_LENGTH[precision as usize - 1])
    }

    /// Creates  decimal type.
    #[inline(always)]
    pub fn decimal(precision: u32, scale: u32) -> Result<Self> {
        ensure_data_valid!(precision > 0 && precision <= MAX_DECIMAL_PRECISION, "Decimals with precision larger than {MAX_DECIMAL_PRECISION} are not supported: {precision}",);
        Ok(Type::Primitive(PrimitiveType::Decimal { precision, scale }))
    }
}

impl From<PrimitiveType> for Type {
    fn from(value: PrimitiveType) -> Self {
        Self::Primitive(value)
    }
}

impl From<StructType> for Type {
    fn from(value: StructType) -> Self {
        Type::Struct(value)
    }
}

impl From<ListType> for Type {
    fn from(value: ListType) -> Self {
        Type::List(value)
    }
}

impl From<MapType> for Type {
    fn from(value: MapType) -> Self {
        Type::Map(value)
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
    /// 32-bit IEEE 754 floating bit.
    Float,
    /// 64-bit IEEE 754 floating bit.
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

impl Serialize for Type {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let type_serde = _serde::SerdeType::from(self);
        type_serde.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Type {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let type_serde = _serde::SerdeType::deserialize(deserializer)?;
        Ok(Type::from(type_serde))
    }
}

impl<'de> Deserialize<'de> for PrimitiveType {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
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
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
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

fn deserialize_decimal<'de, D>(deserializer: D) -> std::result::Result<PrimitiveType, D::Error>
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

fn serialize_decimal<S>(
    precision: &u32,
    scale: &u32,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&format!("decimal({precision},{scale})"))
}

fn deserialize_fixed<'de, D>(deserializer: D) -> std::result::Result<PrimitiveType, D::Error>
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

fn serialize_fixed<S>(value: &u64, serializer: S) -> std::result::Result<S::Ok, S::Error>
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
#[derive(Debug, Serialize, Clone)]
#[serde(rename = "struct", tag = "type")]
pub struct StructType {
    /// Struct fields
    fields: Vec<NestedFieldRef>,
    /// Lookup for index by field id
    #[serde(skip_serializing)]
    id_lookup: OnceLock<HashMap<i32, usize>>,
    #[serde(skip_serializing)]
    name_lookup: OnceLock<HashMap<String, usize>>,
}

impl<'de> Deserialize<'de> for StructType {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "lowercase")]
        enum Field {
            Type,
            Fields,
        }

        struct StructTypeVisitor;

        impl<'de> Visitor<'de> for StructTypeVisitor {
            type Value = StructType;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<StructType, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut fields = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Type => (),
                        Field::Fields => {
                            if fields.is_some() {
                                return Err(serde::de::Error::duplicate_field("fields"));
                            }
                            fields = Some(map.next_value()?);
                        }
                    }
                }
                let fields: Vec<NestedFieldRef> =
                    fields.ok_or_else(|| de::Error::missing_field("fields"))?;

                Ok(StructType::new(fields))
            }
        }

        const FIELDS: &[&str] = &["type", "fields"];
        deserializer.deserialize_struct("struct", FIELDS, StructTypeVisitor)
    }
}

impl StructType {
    /// Creates a struct type with the given fields.
    pub fn new(fields: Vec<NestedFieldRef>) -> Self {
        Self {
            fields,
            id_lookup: OnceLock::new(),
            name_lookup: OnceLock::new(),
        }
    }

    /// Get struct field with certain id
    pub fn field_by_id(&self, id: i32) -> Option<&NestedFieldRef> {
        self.field_id_to_index(id).map(|idx| &self.fields[idx])
    }

    fn field_id_to_index(&self, field_id: i32) -> Option<usize> {
        self.id_lookup
            .get_or_init(|| {
                HashMap::from_iter(self.fields.iter().enumerate().map(|(i, x)| (x.id, i)))
            })
            .get(&field_id)
            .copied()
    }

    /// Get struct field with certain field name
    pub fn field_by_name(&self, name: &str) -> Option<&NestedFieldRef> {
        self.field_name_to_index(name).map(|idx| &self.fields[idx])
    }

    fn field_name_to_index(&self, name: &str) -> Option<usize> {
        self.name_lookup
            .get_or_init(|| {
                HashMap::from_iter(
                    self.fields
                        .iter()
                        .enumerate()
                        .map(|(i, x)| (x.name.clone(), i)),
                )
            })
            .get(name)
            .copied()
    }

    /// Get fields.
    pub fn fields(&self) -> &[NestedFieldRef] {
        &self.fields
    }
}

impl PartialEq for StructType {
    fn eq(&self, other: &Self) -> bool {
        self.fields == other.fields
    }
}

impl Eq for StructType {}

impl Index<usize> for StructType {
    type Output = NestedField;

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

#[derive(Debug, PartialEq, Serialize, Deserialize, Eq, Clone)]
#[serde(from = "SerdeNestedField", into = "SerdeNestedField")]
/// A struct is a tuple of typed values. Each field in the tuple is named and has an integer id that is unique in the table schema.
/// Each field can be either optional or required, meaning that values can (or cannot) be null. Fields may be any type.
/// Fields may have an optional comment or doc string. Fields can have default values.
pub struct NestedField {
    /// Id unique in table schema
    pub id: i32,
    /// Field Name
    pub name: String,
    /// Optional or required
    pub required: bool,
    /// Datatype
    pub field_type: Box<Type>,
    /// Fields may have an optional comment or doc string.
    pub doc: Option<String>,
    /// Used to populate the field’s value for all records that were written before the field was added to the schema
    pub initial_default: Option<Literal>,
    /// Used to populate the field’s value for any records written after the field was added to the schema, if the writer does not supply the field’s value
    pub write_default: Option<Literal>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
struct SerdeNestedField {
    pub id: i32,
    pub name: String,
    pub required: bool,
    #[serde(rename = "type")]
    pub field_type: Box<Type>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial_default: Option<JsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write_default: Option<JsonValue>,
}

impl From<SerdeNestedField> for NestedField {
    fn from(value: SerdeNestedField) -> Self {
        NestedField {
            id: value.id,
            name: value.name,
            required: value.required,
            initial_default: value.initial_default.and_then(|x| {
                Literal::try_from_json(x, &value.field_type)
                    .ok()
                    .and_then(identity)
            }),
            write_default: value.write_default.and_then(|x| {
                Literal::try_from_json(x, &value.field_type)
                    .ok()
                    .and_then(identity)
            }),
            field_type: value.field_type,
            doc: value.doc,
        }
    }
}

impl From<NestedField> for SerdeNestedField {
    fn from(value: NestedField) -> Self {
        let initial_default = value.initial_default.map(|x| x.try_into_json(&value.field_type).expect("We should have checked this in NestedField::with_initial_default, it can't be converted to json value"));
        let write_default = value.write_default.map(|x| x.try_into_json(&value.field_type).expect("We should have checked this in NestedField::with_write_default, it can't be converted to json value"));
        SerdeNestedField {
            id: value.id,
            name: value.name,
            required: value.required,
            field_type: value.field_type,
            doc: value.doc,
            initial_default,
            write_default,
        }
    }
}

/// Reference to nested field.
pub type NestedFieldRef = Arc<NestedField>;

impl NestedField {
    /// Construct a required field.
    pub fn required(id: i32, name: impl ToString, field_type: Type) -> Self {
        Self {
            id,
            name: name.to_string(),
            required: true,
            field_type: Box::new(field_type),
            doc: None,
            initial_default: None,
            write_default: None,
        }
    }

    /// Construct an optional field.
    pub fn optional(id: i32, name: impl ToString, field_type: Type) -> Self {
        Self {
            id,
            name: name.to_string(),
            required: false,
            field_type: Box::new(field_type),
            doc: None,
            initial_default: None,
            write_default: None,
        }
    }

    /// Construct list type's element field.
    pub fn list_element(id: i32, field_type: Type, required: bool) -> Self {
        if required {
            Self::required(id, LIST_FILED_NAME, field_type)
        } else {
            Self::optional(id, LIST_FILED_NAME, field_type)
        }
    }

    /// Construct map type's key field.
    pub fn map_key_element(id: i32, field_type: Type) -> Self {
        Self::required(id, MAP_KEY_FIELD_NAME, field_type)
    }

    /// Construct map type's value field.
    pub fn map_value_element(id: i32, field_type: Type, required: bool) -> Self {
        if required {
            Self::required(id, MAP_VALUE_FIELD_NAME, field_type)
        } else {
            Self::optional(id, MAP_VALUE_FIELD_NAME, field_type)
        }
    }

    /// Set the field's doc.
    pub fn with_doc(mut self, doc: impl ToString) -> Self {
        self.doc = Some(doc.to_string());
        self
    }

    /// Set the field's initial default value.
    pub fn with_initial_default(mut self, value: Literal) -> Self {
        self.initial_default = Some(value);
        self
    }

    /// Set the field's initial default value.
    pub fn with_write_default(mut self, value: Literal) -> Self {
        self.write_default = Some(value);
        self
    }
}

impl fmt::Display for NestedField {
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

#[derive(Debug, PartialEq, Eq, Clone)]
/// A list is a collection of values with some element type. The element field has an integer id that is unique in the table schema.
/// Elements can be either optional or required. Element types may be any type.
pub struct ListType {
    /// Element field of list type.
    pub element_field: NestedFieldRef,
}

/// Module for type serialization/deserialization.
pub(super) mod _serde {
    use crate::spec::datatypes::Type::Map;
    use crate::spec::datatypes::{
        ListType, MapType, NestedField, NestedFieldRef, PrimitiveType, StructType, Type,
    };
    use serde_derive::{Deserialize, Serialize};
    use std::borrow::Cow;

    /// List type for serialization and deserialization
    #[derive(Serialize, Deserialize)]
    #[serde(untagged)]
    pub(super) enum SerdeType<'a> {
        #[serde(rename_all = "kebab-case")]
        List {
            r#type: String,
            element_id: i32,
            element_required: bool,
            element: Cow<'a, Type>,
        },
        Struct {
            r#type: String,
            fields: Cow<'a, Vec<NestedFieldRef>>,
        },
        #[serde(rename_all = "kebab-case")]
        Map {
            r#type: String,
            key_id: i32,
            key: Cow<'a, Type>,
            value_id: i32,
            value_required: bool,
            value: Cow<'a, Type>,
        },
        Primitive(PrimitiveType),
    }

    impl<'a> From<SerdeType<'a>> for Type {
        fn from(value: SerdeType) -> Self {
            match value {
                SerdeType::List {
                    r#type: _,
                    element_id,
                    element_required,
                    element,
                } => Self::List(ListType {
                    element_field: NestedField::list_element(
                        element_id,
                        element.into_owned(),
                        element_required,
                    )
                    .into(),
                }),
                SerdeType::Map {
                    r#type: _,
                    key_id,
                    key,
                    value_id,
                    value_required,
                    value,
                } => Map(MapType {
                    key_field: NestedField::map_key_element(key_id, key.into_owned()).into(),
                    value_field: NestedField::map_value_element(
                        value_id,
                        value.into_owned(),
                        value_required,
                    )
                    .into(),
                }),
                SerdeType::Struct { r#type: _, fields } => {
                    Self::Struct(StructType::new(fields.into_owned()))
                }
                SerdeType::Primitive(p) => Self::Primitive(p),
            }
        }
    }

    impl<'a> From<&'a Type> for SerdeType<'a> {
        fn from(value: &'a Type) -> Self {
            match value {
                Type::List(list) => SerdeType::List {
                    r#type: "list".to_string(),
                    element_id: list.element_field.id,
                    element_required: list.element_field.required,
                    element: Cow::Borrowed(&list.element_field.field_type),
                },
                Type::Map(map) => SerdeType::Map {
                    r#type: "map".to_string(),
                    key_id: map.key_field.id,
                    key: Cow::Borrowed(&map.key_field.field_type),
                    value_id: map.value_field.id,
                    value_required: map.value_field.required,
                    value: Cow::Borrowed(&map.value_field.field_type),
                },
                Type::Struct(s) => SerdeType::Struct {
                    r#type: "struct".to_string(),
                    fields: Cow::Borrowed(&s.fields),
                },
                Type::Primitive(p) => SerdeType::Primitive(p.clone()),
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
/// A map is a collection of key-value pairs with a key type and a value type.
/// Both the key field and value field each have an integer id that is unique in the table schema.
/// Map keys are required and map values can be either optional or required.
/// Both map keys and map values may be any type, including nested types.
pub struct MapType {
    /// Field for key.
    pub key_field: NestedFieldRef,
    /// Field for value.
    pub value_field: NestedFieldRef,
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use uuid::Uuid;

    use crate::spec::values::PrimitiveLiteral;

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
                fields: vec![NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Decimal {
                        precision: 9,
                        scale: 2,
                    }),
                )
                .into()],
                id_lookup: OnceLock::default(),
                name_lookup: OnceLock::default(),
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
                fields: vec![NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Fixed(8)),
                )
                .into()],
                id_lookup: OnceLock::default(),
                name_lookup: OnceLock::default(),
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
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Uuid))
                        .with_initial_default(Literal::Primitive(PrimitiveLiteral::UUID(
                            Uuid::parse_str("0db3e2a8-9d1d-42b9-aa7b-74ebe558dceb").unwrap(),
                        )))
                        .with_write_default(Literal::Primitive(PrimitiveLiteral::UUID(
                            Uuid::parse_str("ec5911be-b0a7-458c-8438-c9a3e53cffae").unwrap(),
                        )))
                        .into(),
                    NestedField::optional(2, "data", Type::Primitive(PrimitiveType::Int)).into(),
                ],
                id_lookup: HashMap::from([(1, 0), (2, 1)]).into(),
                name_lookup: HashMap::from([("id".to_string(), 0), ("data".to_string(), 1)]).into(),
            }),
        )
    }

    #[test]
    fn test_deeply_nested_struct() {
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
    },
    {
      "id": 2,
      "name": "data",
      "required": false,
      "type": "int"
    },
    {
      "id": 3,
      "name": "address",
      "required": true,
      "type": {
        "type": "struct",
        "fields": [
          {
            "id": 4,
            "name": "street",
            "required": true,
            "type": "string"
          },
          {
            "id": 5,
            "name": "province",
            "required": false,
            "type": "string"
          },
          {
            "id": 6,
            "name": "zip",
            "required": true,
            "type": "int"
          }
        ]
      }
    }
  ]
}
"#;

        let struct_type = Type::Struct(StructType::new(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Uuid))
                .with_initial_default(Literal::Primitive(PrimitiveLiteral::UUID(
                    Uuid::parse_str("0db3e2a8-9d1d-42b9-aa7b-74ebe558dceb").unwrap(),
                )))
                .with_write_default(Literal::Primitive(PrimitiveLiteral::UUID(
                    Uuid::parse_str("ec5911be-b0a7-458c-8438-c9a3e53cffae").unwrap(),
                )))
                .into(),
            NestedField::optional(2, "data", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(
                3,
                "address",
                Type::Struct(StructType::new(vec![
                    NestedField::required(4, "street", Type::Primitive(PrimitiveType::String))
                        .into(),
                    NestedField::optional(5, "province", Type::Primitive(PrimitiveType::String))
                        .into(),
                    NestedField::required(6, "zip", Type::Primitive(PrimitiveType::Int)).into(),
                ])),
            )
            .into(),
        ]));

        check_type_serde(record, struct_type)
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
                element_field: NestedField::list_element(
                    3,
                    Type::Primitive(PrimitiveType::String),
                    true,
                )
                .into(),
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
                key_field: NestedField::map_key_element(4, Type::Primitive(PrimitiveType::String))
                    .into(),
                value_field: NestedField::map_value_element(
                    5,
                    Type::Primitive(PrimitiveType::Double),
                    false,
                )
                .into(),
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
                key_field: NestedField::map_key_element(4, Type::Primitive(PrimitiveType::Int))
                    .into(),
                value_field: NestedField::map_value_element(
                    5,
                    Type::Primitive(PrimitiveType::String),
                    false,
                )
                .into(),
            }),
        );
    }

    #[test]
    fn test_decimal_precision() {
        let expected_max_precision = [
            2, 4, 6, 9, 11, 14, 16, 18, 21, 23, 26, 28, 31, 33, 35, 38, 40, 43, 45, 47, 50, 52, 55,
            57,
        ];
        for (i, max_precision) in expected_max_precision.iter().enumerate() {
            assert_eq!(
                *max_precision,
                Type::decimal_max_precision(i as u32 + 1).unwrap(),
                "Failed calculate max precision for {i}"
            );
        }

        assert_eq!(5, Type::decimal_required_bytes(10).unwrap());
        assert_eq!(16, Type::decimal_required_bytes(38).unwrap());
    }
}
