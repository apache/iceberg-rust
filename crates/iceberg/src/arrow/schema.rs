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

//! Conversion between Arrow schema and Iceberg schema.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::types::{validate_decimal_precision_and_scale, Decimal128Type};
use arrow_array::{
    BooleanArray, Datum as ArrowDatum, Float32Array, Float64Array, Int32Array, Int64Array,
    StringArray,
};
use arrow_schema::{DataType, Field, Fields, Schema as ArrowSchema, TimeUnit};
use bitvec::macros::internal::funty::Fundamental;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use rust_decimal::prelude::ToPrimitive;

use crate::error::Result;
use crate::spec::{
    Datum, ListType, MapType, NestedField, NestedFieldRef, PrimitiveLiteral, PrimitiveType, Schema,
    SchemaVisitor, StructType, Type,
};
use crate::{Error, ErrorKind};

/// When iceberg map type convert to Arrow map type, the default map field name is "key_value".
pub(crate) const DEFAULT_MAP_FIELD_NAME: &str = "key_value";

/// A post order arrow schema visitor.
///
/// For order of methods called, please refer to [`to_iceberg_schema`].
pub trait ArrowSchemaVisitor {
    /// Return type of this visitor on arrow field.
    type T;

    /// Return type of this visitor on arrow schema.
    type U;

    /// Called before struct/list/map field.
    fn before_field(&mut self, _field: &Field) -> Result<()> {
        Ok(())
    }

    /// Called after struct/list/map field.
    fn after_field(&mut self, _field: &Field) -> Result<()> {
        Ok(())
    }

    /// Called before list element.
    fn before_list_element(&mut self, _field: &Field) -> Result<()> {
        Ok(())
    }

    /// Called after list element.
    fn after_list_element(&mut self, _field: &Field) -> Result<()> {
        Ok(())
    }

    /// Called before map key.
    fn before_map_key(&mut self, _field: &Field) -> Result<()> {
        Ok(())
    }

    /// Called after map key.
    fn after_map_key(&mut self, _field: &Field) -> Result<()> {
        Ok(())
    }

    /// Called before map value.
    fn before_map_value(&mut self, _field: &Field) -> Result<()> {
        Ok(())
    }

    /// Called after map value.
    fn after_map_value(&mut self, _field: &Field) -> Result<()> {
        Ok(())
    }
}

/// Convert Arrow schema to ceberg schema.
#[allow(dead_code)]
pub fn arrow_schema_to_schema(schema: &ArrowSchema) -> Result<Schema> {
    let mut visitor = ArrowSchemaConverter::new();
    visitor.to_iceberg_schema(schema)
}

const ARROW_FIELD_DOC_KEY: &str = "doc";

struct ArrowSchemaConverter;

impl ArrowSchemaVisitor for ArrowSchemaConverter {
    type T = Type;
    type U = Schema;
}

impl ArrowSchemaConverter {
    #[allow(dead_code)]
    fn new() -> Self {
        Self {}
    }

    /// Convert Arrow schema to Iceberg schema.
    fn to_iceberg_schema(&mut self, schema: &ArrowSchema) -> Result<Schema> {
        let mut fields = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            self.before_field(field)?;
            let tp = self.convert_type(field.data_type())?;
            self.after_field(field)?;
            let iceberg_field = self.convert_field(field, &tp)?;
            fields.push(Arc::new(iceberg_field));
        }
        Schema::builder().with_fields(fields).build()
    }

    /// Convert Arrow field to Iceberg field.
    fn convert_field(&self, field: &Field, field_type: &Type) -> Result<NestedField> {
        let id = field
            .metadata()
            .get(PARQUET_FIELD_ID_META_KEY)
            .map_or_else(
                || {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        "Field id not found in metadata",
                    ))
                },
                |value| {
                    value.parse::<i32>().map_err(|e| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "Failed to parse field id".to_string(),
                        )
                        .with_context("value", value)
                        .with_source(e)
                    })
                },
            )?;

        let doc = field.metadata().get(ARROW_FIELD_DOC_KEY).map(|x| x.into());
        Ok(NestedField {
            id,
            doc,
            name: field.name().clone(),
            required: !field.is_nullable(),
            field_type: Box::new(field_type.clone()),
            initial_default: None,
            write_default: None,
        })
    }

    /// Convert Arrow data type to Iceberg type.
    fn convert_type(&mut self, tp: &DataType) -> Result<Type> {
        match tp {
            DataType::Boolean => Ok(Type::Primitive(PrimitiveType::Boolean)),
            DataType::Int32 => Ok(Type::Primitive(PrimitiveType::Int)),
            DataType::Int64 => Ok(Type::Primitive(PrimitiveType::Long)),
            DataType::Float32 => Ok(Type::Primitive(PrimitiveType::Float)),
            DataType::Float64 => Ok(Type::Primitive(PrimitiveType::Double)),
            DataType::Decimal128(p, s) => Type::decimal(*p as u32, *s as u32).map_err(|e| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Failed to create decimal type".to_string(),
                )
                .with_source(e)
            }),
            DataType::Date32 => Ok(Type::Primitive(PrimitiveType::Date)),
            DataType::Time64(unit) if unit == &TimeUnit::Microsecond => {
                Ok(Type::Primitive(PrimitiveType::Time))
            }
            DataType::Timestamp(unit, None) if unit == &TimeUnit::Microsecond => {
                Ok(Type::Primitive(PrimitiveType::Timestamp))
            }
            DataType::Timestamp(unit, Some(zone))
                if unit == &TimeUnit::Microsecond
                    && (zone.as_ref() == "UTC" || zone.as_ref() == "+00:00") =>
            {
                Ok(Type::Primitive(PrimitiveType::Timestamptz))
            }
            DataType::Binary | DataType::LargeBinary => Ok(Type::Primitive(PrimitiveType::Binary)),
            DataType::FixedSizeBinary(width) => {
                Ok(Type::Primitive(PrimitiveType::Fixed(*width as u64)))
            }
            DataType::Utf8 | DataType::LargeUtf8 => Ok(Type::Primitive(PrimitiveType::String)),
            // list:
            DataType::List(element_field)
            | DataType::LargeList(element_field)
            | DataType::FixedSizeList(element_field, _) => self.convert_list_type(element_field),

            DataType::Map(field, _) => self.convert_map_type(field),
            DataType::Struct(fields) => self.convert_struct_type(fields),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Unsupported Arrow data type: {tp}"),
            )),
        }
    }

    /// Convert Arrow list type to Iceberg list type.
    fn convert_list_type(&mut self, element_field: &Field) -> Result<Type> {
        // before_list_element
        let tp = self.convert_type(element_field.data_type())?;
        // after list element
        let element_field = self.convert_field(&element_field, &tp)?;
        Ok(Type::List(ListType::new(element_field.into())))
    }

    /// Convert Arrow map type to Iceberg map type.
    fn convert_map_type(&mut self, field: &Field) -> Result<Type> {
        match field.data_type() {
            DataType::Struct(fields) => {
                if fields.len() != 2 {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "Map field must have exactly 2 fields",
                    ));
                }

                let key_field = {
                    let field = &fields[0];
                    self.before_map_key(field)?;
                    let key_type = self.convert_type(field.data_type())?;
                    self.after_map_key(field)?;
                    self.convert_field(field, &key_type)?
                };
                let value_field = {
                    let field = &fields[1];
                    self.before_map_value(field)?;
                    let value_type = self.convert_type(field.data_type())?;
                    self.after_map_value(field)?;
                    self.convert_field(field, &value_type)?
                };

                Ok(Type::Map(MapType::new(
                    key_field.into(),
                    value_field.into(),
                )))
            }
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                "Map field must have struct type",
            )),
        }
    }

    /// Convert Arrow struct type to Iceberg struct type.
    fn convert_struct_type(&mut self, fields: &Fields) -> Result<Type> {
        let mut ice_fields = Vec::with_capacity(fields.len());
        for field in fields {
            self.before_field(field)?;
            let field_type = self.convert_type(&field.data_type())?;
            self.after_field(field)?;
            let icebug_field = self.convert_field(field, &field_type)?;
            ice_fields.push(Arc::new(icebug_field));
        }
        Ok(Type::Struct(StructType::new(ice_fields)))
    }
}

struct ToArrowSchemaConverter;

enum ArrowSchemaOrFieldOrType {
    Schema(ArrowSchema),
    Field(Field),
    Type(DataType),
}

impl SchemaVisitor for ToArrowSchemaConverter {
    type T = ArrowSchemaOrFieldOrType;

    fn schema(
        &mut self,
        _schema: &crate::spec::Schema,
        value: ArrowSchemaOrFieldOrType,
    ) -> crate::Result<ArrowSchemaOrFieldOrType> {
        let struct_type = match value {
            ArrowSchemaOrFieldOrType::Type(DataType::Struct(fields)) => fields,
            _ => unreachable!(),
        };
        Ok(ArrowSchemaOrFieldOrType::Schema(ArrowSchema::new(
            struct_type,
        )))
    }

    fn field(
        &mut self,
        field: &crate::spec::NestedFieldRef,
        value: ArrowSchemaOrFieldOrType,
    ) -> crate::Result<ArrowSchemaOrFieldOrType> {
        let ty = match value {
            ArrowSchemaOrFieldOrType::Type(ty) => ty,
            _ => unreachable!(),
        };
        let metadata = if let Some(doc) = &field.doc {
            HashMap::from([
                (PARQUET_FIELD_ID_META_KEY.to_string(), field.id.to_string()),
                (ARROW_FIELD_DOC_KEY.to_string(), doc.clone()),
            ])
        } else {
            HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), field.id.to_string())])
        };
        Ok(ArrowSchemaOrFieldOrType::Field(
            Field::new(field.name.clone(), ty, !field.required).with_metadata(metadata),
        ))
    }

    fn r#struct(
        &mut self,
        _: &crate::spec::StructType,
        results: Vec<ArrowSchemaOrFieldOrType>,
    ) -> crate::Result<ArrowSchemaOrFieldOrType> {
        let fields = results
            .into_iter()
            .map(|result| match result {
                ArrowSchemaOrFieldOrType::Field(field) => field,
                _ => unreachable!(),
            })
            .collect();
        Ok(ArrowSchemaOrFieldOrType::Type(DataType::Struct(fields)))
    }

    fn list(
        &mut self,
        list: &crate::spec::ListType,
        value: ArrowSchemaOrFieldOrType,
    ) -> crate::Result<Self::T> {
        let field = match self.field(&list.element_field, value)? {
            ArrowSchemaOrFieldOrType::Field(field) => field,
            _ => unreachable!(),
        };
        let meta = if let Some(doc) = &list.element_field.doc {
            HashMap::from([
                (
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    list.element_field.id.to_string(),
                ),
                (ARROW_FIELD_DOC_KEY.to_string(), doc.clone()),
            ])
        } else {
            HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                list.element_field.id.to_string(),
            )])
        };
        let field = field.with_metadata(meta);
        Ok(ArrowSchemaOrFieldOrType::Type(DataType::List(Arc::new(
            field,
        ))))
    }

    fn map(
        &mut self,
        map: &crate::spec::MapType,
        key_value: ArrowSchemaOrFieldOrType,
        value: ArrowSchemaOrFieldOrType,
    ) -> crate::Result<ArrowSchemaOrFieldOrType> {
        let key_field = match self.field(&map.key_field, key_value)? {
            ArrowSchemaOrFieldOrType::Field(field) => field,
            _ => unreachable!(),
        };
        let value_field = match self.field(&map.value_field, value)? {
            ArrowSchemaOrFieldOrType::Field(field) => field,
            _ => unreachable!(),
        };
        let field = Field::new(
            DEFAULT_MAP_FIELD_NAME,
            DataType::Struct(vec![key_field, value_field].into()),
            // Map field is always not nullable
            false,
        );

        Ok(ArrowSchemaOrFieldOrType::Type(DataType::Map(
            field.into(),
            false,
        )))
    }

    fn primitive(
        &mut self,
        p: &crate::spec::PrimitiveType,
    ) -> crate::Result<ArrowSchemaOrFieldOrType> {
        match p {
            crate::spec::PrimitiveType::Boolean => {
                Ok(ArrowSchemaOrFieldOrType::Type(DataType::Boolean))
            }
            crate::spec::PrimitiveType::Int => Ok(ArrowSchemaOrFieldOrType::Type(DataType::Int32)),
            crate::spec::PrimitiveType::Long => Ok(ArrowSchemaOrFieldOrType::Type(DataType::Int64)),
            crate::spec::PrimitiveType::Float => {
                Ok(ArrowSchemaOrFieldOrType::Type(DataType::Float32))
            }
            crate::spec::PrimitiveType::Double => {
                Ok(ArrowSchemaOrFieldOrType::Type(DataType::Float64))
            }
            crate::spec::PrimitiveType::Decimal { precision, scale } => {
                let (precision, scale) = {
                    let precision: u8 = precision.to_owned().try_into().map_err(|err| {
                        Error::new(
                            crate::ErrorKind::DataInvalid,
                            "incompatible precision for decimal type convert",
                        )
                        .with_source(err)
                    })?;
                    let scale = scale.to_owned().try_into().map_err(|err| {
                        Error::new(
                            crate::ErrorKind::DataInvalid,
                            "incompatible scale for decimal type convert",
                        )
                        .with_source(err)
                    })?;
                    (precision, scale)
                };
                validate_decimal_precision_and_scale::<Decimal128Type>(precision, scale).map_err(
                    |err| {
                        Error::new(
                            crate::ErrorKind::DataInvalid,
                            "incompatible precision and scale for decimal type convert",
                        )
                        .with_source(err)
                    },
                )?;
                Ok(ArrowSchemaOrFieldOrType::Type(DataType::Decimal128(
                    precision, scale,
                )))
            }
            crate::spec::PrimitiveType::Date => {
                Ok(ArrowSchemaOrFieldOrType::Type(DataType::Date32))
            }
            crate::spec::PrimitiveType::Time => Ok(ArrowSchemaOrFieldOrType::Type(
                DataType::Time64(TimeUnit::Microsecond),
            )),
            crate::spec::PrimitiveType::Timestamp => Ok(ArrowSchemaOrFieldOrType::Type(
                DataType::Timestamp(TimeUnit::Microsecond, None),
            )),
            crate::spec::PrimitiveType::Timestamptz => Ok(ArrowSchemaOrFieldOrType::Type(
                // Timestampz always stored as UTC
                DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            )),
            crate::spec::PrimitiveType::String => {
                Ok(ArrowSchemaOrFieldOrType::Type(DataType::Utf8))
            }
            crate::spec::PrimitiveType::Uuid => Ok(ArrowSchemaOrFieldOrType::Type(
                DataType::FixedSizeBinary(16),
            )),
            crate::spec::PrimitiveType::Fixed(len) => Ok(ArrowSchemaOrFieldOrType::Type(
                len.to_i32()
                    .map(DataType::FixedSizeBinary)
                    .unwrap_or(DataType::LargeBinary),
            )),
            crate::spec::PrimitiveType::Binary => {
                Ok(ArrowSchemaOrFieldOrType::Type(DataType::LargeBinary))
            }
        }
    }
}

/// Convert iceberg schema to an arrow schema.
pub fn schema_to_arrow_schema(schema: &crate::spec::Schema) -> crate::Result<ArrowSchema> {
    let mut converter = ToArrowSchemaConverter;
    match crate::spec::visit_schema(schema, &mut converter)? {
        ArrowSchemaOrFieldOrType::Schema(schema) => Ok(schema),
        _ => unreachable!(),
    }
}

/// Convert Iceberg Datum to Arrow Datum.
pub(crate) fn get_arrow_datum(datum: &Datum) -> Result<Box<dyn ArrowDatum + Send>> {
    match datum.literal() {
        PrimitiveLiteral::Boolean(value) => Ok(Box::new(BooleanArray::new_scalar(*value))),
        PrimitiveLiteral::Int(value) => Ok(Box::new(Int32Array::new_scalar(*value))),
        PrimitiveLiteral::Long(value) => Ok(Box::new(Int64Array::new_scalar(*value))),
        PrimitiveLiteral::Float(value) => Ok(Box::new(Float32Array::new_scalar(value.as_f32()))),
        PrimitiveLiteral::Double(value) => Ok(Box::new(Float64Array::new_scalar(value.as_f64()))),
        PrimitiveLiteral::String(value) => Ok(Box::new(StringArray::new_scalar(value.as_str()))),
        l => Err(Error::new(
            ErrorKind::FeatureUnsupported,
            format!(
                "Converting datum from type {:?} to arrow not supported yet.",
                l
            ),
        )),
    }
}

impl TryFrom<&ArrowSchema> for crate::spec::Schema {
    type Error = Error;

    fn try_from(schema: &ArrowSchema) -> crate::Result<Self> {
        ArrowSchemaConverter::new().to_iceberg_schema(schema)
    }
}

impl TryFrom<&crate::spec::Schema> for ArrowSchema {
    type Error = Error;

    fn try_from(schema: &crate::spec::Schema) -> crate::Result<Self> {
        schema_to_arrow_schema(schema)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};

    use super::*;
    use crate::spec::Schema;

    /// Create a simple field with metadata.
    fn simple_field(name: &str, ty: DataType, nullable: bool, value: &str) -> Field {
        Field::new(name, ty, nullable).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            value.to_string(),
        )]))
    }

    fn arrow_schema_for_arrow_schema_to_schema_test() -> ArrowSchema {
        let fields = Fields::from(vec![
            simple_field("key", DataType::Int32, false, "17"),
            simple_field("value", DataType::Utf8, true, "18"),
        ]);

        let r#struct = DataType::Struct(fields);
        let map = DataType::Map(
            Arc::new(simple_field(DEFAULT_MAP_FIELD_NAME, r#struct, false, "17")),
            false,
        );

        let fields = Fields::from(vec![
            simple_field("aa", DataType::Int32, false, "18"),
            simple_field("bb", DataType::Utf8, true, "19"),
            simple_field(
                "cc",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
                "20",
            ),
        ]);

        let r#struct = DataType::Struct(fields);

        ArrowSchema::new(vec![
            simple_field("a", DataType::Int32, false, "2"),
            simple_field("b", DataType::Int64, false, "1"),
            simple_field("c", DataType::Utf8, false, "3"),
            simple_field("n", DataType::Utf8, false, "21"),
            simple_field(
                "d",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
                "4",
            ),
            simple_field("e", DataType::Boolean, true, "6"),
            simple_field("f", DataType::Float32, false, "5"),
            simple_field("g", DataType::Float64, false, "7"),
            simple_field("p", DataType::Decimal128(10, 2), false, "27"),
            simple_field("h", DataType::Date32, false, "8"),
            simple_field("i", DataType::Time64(TimeUnit::Microsecond), false, "9"),
            simple_field(
                "j",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
                "10",
            ),
            simple_field(
                "k",
                DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                false,
                "12",
            ),
            simple_field("l", DataType::Binary, false, "13"),
            simple_field("o", DataType::LargeBinary, false, "22"),
            simple_field("m", DataType::FixedSizeBinary(10), false, "11"),
            simple_field(
                "list",
                DataType::List(Arc::new(simple_field(
                    "element",
                    DataType::Int32,
                    false,
                    "15",
                ))),
                true,
                "14",
            ),
            simple_field(
                "large_list",
                DataType::LargeList(Arc::new(simple_field(
                    "element",
                    DataType::Utf8,
                    false,
                    "23",
                ))),
                true,
                "24",
            ),
            simple_field(
                "fixed_list",
                DataType::FixedSizeList(
                    Arc::new(simple_field("element", DataType::Binary, false, "26")),
                    10,
                ),
                true,
                "25",
            ),
            simple_field("map", map, false, "16"),
            simple_field("struct", r#struct, false, "17"),
        ])
    }

    fn iceberg_schema_for_arrow_schema_to_schema_test() -> Schema {
        let schema_json = r#"{
            "type":"struct",
            "schema-id":0,
            "fields":[
                {
                    "id":2,
                    "name":"a",
                    "required":true,
                    "type":"int"
                },
                {
                    "id":1,
                    "name":"b",
                    "required":true,
                    "type":"long"
                },
                {
                    "id":3,
                    "name":"c",
                    "required":true,
                    "type":"string"
                },
                {
                    "id":21,
                    "name":"n",
                    "required":true,
                    "type":"string"
                },
                {
                    "id":4,
                    "name":"d",
                    "required":false,
                    "type":"timestamp"
                },
                {
                    "id":6,
                    "name":"e",
                    "required":false,
                    "type":"boolean"
                },
                {
                    "id":5,
                    "name":"f",
                    "required":true,
                    "type":"float"
                },
                {
                    "id":7,
                    "name":"g",
                    "required":true,
                    "type":"double"
                },
                {
                    "id":27,
                    "name":"p",
                    "required":true,
                    "type":"decimal(10,2)"
                },
                {
                    "id":8,
                    "name":"h",
                    "required":true,
                    "type":"date"
                },
                {
                    "id":9,
                    "name":"i",
                    "required":true,
                    "type":"time"
                },
                {
                    "id":10,
                    "name":"j",
                    "required":true,
                    "type":"timestamptz"
                },
                {
                    "id":12,
                    "name":"k",
                    "required":true,
                    "type":"timestamptz"
                },
                {
                    "id":13,
                    "name":"l",
                    "required":true,
                    "type":"binary"
                },
                {
                    "id":22,
                    "name":"o",
                    "required":true,
                    "type":"binary"
                },
                {
                    "id":11,
                    "name":"m",
                    "required":true,
                    "type":"fixed[10]"
                },
                {
                    "id":14,
                    "name":"list",
                    "required": false,
                    "type": {
                        "type": "list",
                        "element-id": 15,
                        "element-required": true,
                        "element": "int"
                    }
                },
                {
                    "id":24,
                    "name":"large_list",
                    "required": false,
                    "type": {
                        "type": "list",
                        "element-id": 23,
                        "element-required": true,
                        "element": "string"
                    }
                },
                {
                    "id":25,
                    "name":"fixed_list",
                    "required": false,
                    "type": {
                        "type": "list",
                        "element-id": 26,
                        "element-required": true,
                        "element": "binary"
                    }
                },
                {
                    "id":16,
                    "name":"map",
                    "required": true,
                    "type": {
                        "type": "map",
                        "key-id": 17,
                        "key": "int",
                        "value-id": 18,
                        "value-required": false,
                        "value": "string"
                    }
                },
                {
                    "id":17,
                    "name":"struct",
                    "required": true,
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "id":18,
                                "name":"aa",
                                "required":true,
                                "type":"int"
                            },
                            {
                                "id":19,
                                "name":"bb",
                                "required":false,
                                "type":"string"
                            },
                            {
                                "id":20,
                                "name":"cc",
                                "required":true,
                                "type":"timestamp"
                            }
                        ]
                    }
                }
            ],
            "identifier-field-ids":[]
        }"#;

        let schema: Schema = serde_json::from_str(schema_json).unwrap();
        schema
    }

    #[test]
    fn test_arrow_schema_to_schema() {
        let arrow_schema = arrow_schema_for_arrow_schema_to_schema_test();
        let schema = iceberg_schema_for_arrow_schema_to_schema_test();
        let converted_schema = arrow_schema_to_schema(&arrow_schema).unwrap();
        assert_eq!(converted_schema, schema);
    }

    fn arrow_schema_for_schema_to_arrow_schema_test() -> ArrowSchema {
        let fields = Fields::from(vec![
            simple_field("key", DataType::Int32, false, "17"),
            simple_field("value", DataType::Utf8, true, "18"),
        ]);

        let r#struct = DataType::Struct(fields);
        let map = DataType::Map(
            Arc::new(Field::new(DEFAULT_MAP_FIELD_NAME, r#struct, false)),
            false,
        );

        let fields = Fields::from(vec![
            simple_field("aa", DataType::Int32, false, "18"),
            simple_field("bb", DataType::Utf8, true, "19"),
            simple_field(
                "cc",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
                "20",
            ),
        ]);

        let r#struct = DataType::Struct(fields);

        ArrowSchema::new(vec![
            simple_field("a", DataType::Int32, false, "2"),
            simple_field("b", DataType::Int64, false, "1"),
            simple_field("c", DataType::Utf8, false, "3"),
            simple_field("n", DataType::Utf8, false, "21"),
            simple_field(
                "d",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
                "4",
            ),
            simple_field("e", DataType::Boolean, true, "6"),
            simple_field("f", DataType::Float32, false, "5"),
            simple_field("g", DataType::Float64, false, "7"),
            simple_field("p", DataType::Decimal128(10, 2), false, "27"),
            simple_field("h", DataType::Date32, false, "8"),
            simple_field("i", DataType::Time64(TimeUnit::Microsecond), false, "9"),
            simple_field(
                "j",
                DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                false,
                "10",
            ),
            simple_field(
                "k",
                DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                false,
                "12",
            ),
            simple_field("l", DataType::LargeBinary, false, "13"),
            simple_field("o", DataType::LargeBinary, false, "22"),
            simple_field("m", DataType::FixedSizeBinary(10), false, "11"),
            simple_field(
                "list",
                DataType::List(Arc::new(simple_field(
                    "element",
                    DataType::Int32,
                    false,
                    "15",
                ))),
                true,
                "14",
            ),
            simple_field(
                "large_list",
                DataType::List(Arc::new(simple_field(
                    "element",
                    DataType::Utf8,
                    false,
                    "23",
                ))),
                true,
                "24",
            ),
            simple_field(
                "fixed_list",
                DataType::List(Arc::new(simple_field(
                    "element",
                    DataType::LargeBinary,
                    false,
                    "26",
                ))),
                true,
                "25",
            ),
            simple_field("map", map, false, "16"),
            simple_field("struct", r#struct, false, "17"),
            simple_field("uuid", DataType::FixedSizeBinary(16), false, "26"),
        ])
    }

    fn iceberg_schema_for_schema_to_arrow_schema() -> Schema {
        let schema_json = r#"{
            "type":"struct",
            "schema-id":0,
            "fields":[
                {
                    "id":2,
                    "name":"a",
                    "required":true,
                    "type":"int"
                },
                {
                    "id":1,
                    "name":"b",
                    "required":true,
                    "type":"long"
                },
                {
                    "id":3,
                    "name":"c",
                    "required":true,
                    "type":"string"
                },
                {
                    "id":21,
                    "name":"n",
                    "required":true,
                    "type":"string"
                },
                {
                    "id":4,
                    "name":"d",
                    "required":false,
                    "type":"timestamp"
                },
                {
                    "id":6,
                    "name":"e",
                    "required":false,
                    "type":"boolean"
                },
                {
                    "id":5,
                    "name":"f",
                    "required":true,
                    "type":"float"
                },
                {
                    "id":7,
                    "name":"g",
                    "required":true,
                    "type":"double"
                },
                {
                    "id":27,
                    "name":"p",
                    "required":true,
                    "type":"decimal(10,2)"
                },
                {
                    "id":8,
                    "name":"h",
                    "required":true,
                    "type":"date"
                },
                {
                    "id":9,
                    "name":"i",
                    "required":true,
                    "type":"time"
                },
                {
                    "id":10,
                    "name":"j",
                    "required":true,
                    "type":"timestamptz"
                },
                {
                    "id":12,
                    "name":"k",
                    "required":true,
                    "type":"timestamptz"
                },
                {
                    "id":13,
                    "name":"l",
                    "required":true,
                    "type":"binary"
                },
                {
                    "id":22,
                    "name":"o",
                    "required":true,
                    "type":"binary"
                },
                {
                    "id":11,
                    "name":"m",
                    "required":true,
                    "type":"fixed[10]"
                },
                {
                    "id":14,
                    "name":"list",
                    "required": false,
                    "type": {
                        "type": "list",
                        "element-id": 15,
                        "element-required": true,
                        "element": "int"
                    }
                },
                {
                    "id":24,
                    "name":"large_list",
                    "required": false,
                    "type": {
                        "type": "list",
                        "element-id": 23,
                        "element-required": true,
                        "element": "string"
                    }
                },
                {
                    "id":25,
                    "name":"fixed_list",
                    "required": false,
                    "type": {
                        "type": "list",
                        "element-id": 26,
                        "element-required": true,
                        "element": "binary"
                    }
                },
                {
                    "id":16,
                    "name":"map",
                    "required": true,
                    "type": {
                        "type": "map",
                        "key-id": 17,
                        "key": "int",
                        "value-id": 18,
                        "value-required": false,
                        "value": "string"
                    }
                },
                {
                    "id":17,
                    "name":"struct",
                    "required": true,
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "id":18,
                                "name":"aa",
                                "required":true,
                                "type":"int"
                            },
                            {
                                "id":19,
                                "name":"bb",
                                "required":false,
                                "type":"string"
                            },
                            {
                                "id":20,
                                "name":"cc",
                                "required":true,
                                "type":"timestamp"
                            }
                        ]
                    }
                },
                {
                    "id":26,
                    "name":"uuid",
                    "required":true,
                    "type":"uuid"
                }
            ],
            "identifier-field-ids":[]
        }"#;

        let schema: Schema = serde_json::from_str(schema_json).unwrap();
        schema
    }

    #[test]
    fn test_schema_to_arrow_schema() {
        let arrow_schema = arrow_schema_for_schema_to_arrow_schema_test();
        let schema = iceberg_schema_for_schema_to_arrow_schema();
        let converted_arrow_schema = schema_to_arrow_schema(&schema).unwrap();
        assert_eq!(converted_arrow_schema, arrow_schema);
    }
}
