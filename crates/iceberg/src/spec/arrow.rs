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

use crate::error::Result;
use crate::spec::{
    ListType, MapType, NestedField, NestedFieldRef, PrimitiveType, Schema, StructType, Type,
};
use crate::{Error, ErrorKind};
use arrow_schema::{DataType, Field, Fields, Schema as ArrowSchema, TimeUnit};
use std::sync::Arc;

/// A post order arrow schema visitor.
///
/// For order of methods called, please refer to [`visit_schema`].
pub trait ArrowSchemaVisitor {
    /// Return type of this visitor.
    type T;
    type U;

    /// Called before struct/list/map field.
    fn before_field(&mut self, _field: &Field) -> Result<()> {
        Ok(())
    }

    /// Called after struct/list/map field.
    fn after_field(&mut self, _field: &Field) -> Result<()> {
        Ok(())
    }

    /// Called after schema's type visited.
    fn schema(&mut self, schema: &ArrowSchema, values: Vec<Self::T>) -> Result<Self::U>;

    /// Called after struct's fields visited.
    fn r#struct(&mut self, fields: &Fields, results: Vec<Self::T>) -> Result<Self::T>;

    /// Called after list fields visited.
    fn list(&mut self, list: &DataType, value: Self::T) -> Result<Self::T>;

    /// Called after map's key and value fields visited.
    fn map(&mut self, map: &DataType, key_value: Self::T, value: Self::T) -> Result<Self::T>;

    /// Called when see a primitive type.
    fn primitive(&mut self, p: &DataType) -> Result<Self::T>;
}

/// Visiting a type in post order.
fn visit_type<V: ArrowSchemaVisitor>(r#type: &DataType, visitor: &mut V) -> Result<V::T> {
    match r#type {
        p if p.is_primitive()
            || matches!(
                p,
                DataType::Boolean
                    | DataType::Utf8
                    | DataType::Binary
                    | DataType::FixedSizeBinary(_)
            ) =>
        {
            visitor.primitive(p)
        }
        DataType::List(element_field) => {
            visitor.before_field(element_field)?;
            let value = visit_type(element_field.data_type(), visitor)?;
            visitor.after_field(element_field)?;
            visitor.list(r#type, value)
        }
        DataType::Map(field, _) => match field.data_type() {
            DataType::Struct(fields) => {
                if fields.len() != 2 {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "Map field must have exactly 2 fields",
                    ));
                }

                let key_field = &fields[0];
                let value_field = &fields[1];

                let key_result = {
                    visitor.before_field(key_field)?;
                    let ret = visit_type(key_field.data_type(), visitor)?;
                    visitor.after_field(key_field)?;
                    ret
                };

                let value_result = {
                    visitor.before_field(value_field)?;
                    let ret = visit_type(value_field.data_type(), visitor)?;
                    visitor.after_field(value_field)?;
                    ret
                };

                visitor.map(r#type, key_result, value_result)
            }
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                "Map field must have struct type",
            )),
        },
        DataType::Struct(fields) => visit_struct(fields, visitor),
        other => Err(Error::new(
            ErrorKind::DataInvalid,
            format!("Cannot visit Arrow data type: {other}"),
        )),
    }
}

/// Visit struct type in post order.
#[allow(dead_code)]
fn visit_struct<V: ArrowSchemaVisitor>(fields: &Fields, visitor: &mut V) -> Result<V::T> {
    let mut results = Vec::with_capacity(fields.len());
    for field in fields {
        visitor.before_field(field)?;
        let result = visit_type(field.data_type(), visitor)?;
        visitor.after_field(field)?;
        results.push(result);
    }

    visitor.r#struct(fields, results)
}

/// Visit schema in post order.
#[allow(dead_code)]
pub fn visit_schema<V: ArrowSchemaVisitor>(schema: &ArrowSchema, visitor: &mut V) -> Result<V::U> {
    let mut results = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        visitor.before_field(field)?;
        let result = visit_type(field.data_type(), visitor)?;
        visitor.after_field(field)?;
        results.push(result);
    }
    visitor.schema(schema, results)
}

const ARROW_FIELD_ID_KEYS: [&str; 2] = ["PARQUET:field_id", "field_id"];
const ARROW_FIELD_DOC_KEYS: [&str; 3] = ["PARQUET:field_doc", "field_doc", "doc"];

fn get_field_id(field: &Field) -> Option<i32> {
    for key in ARROW_FIELD_ID_KEYS {
        if let Some(value) = field.metadata().get(key) {
            return value.parse::<i32>().ok();
        }
    }
    None
}

fn get_field_doc(field: &Field) -> Option<String> {
    for key in ARROW_FIELD_DOC_KEYS {
        if let Some(value) = field.metadata().get(key) {
            return Some(value.clone());
        }
    }
    None
}

struct ArrowSchemaConverter {}

impl ArrowSchemaConverter {
    #[allow(dead_code)]
    fn new() -> Self {
        Self {}
    }

    fn convert_fields(fields: &Fields, field_results: &[Type]) -> Result<Vec<NestedFieldRef>> {
        let mut results = Vec::with_capacity(fields.len());
        for i in 0..fields.len() {
            let field = &fields[i];
            let field_type = &field_results[i];
            let id = get_field_id(field).ok_or(Error::new(
                ErrorKind::DataInvalid,
                "Field id not found in metadata",
            ))?;
            let doc = get_field_doc(field);
            let nested_field = NestedField {
                id,
                doc,
                name: field.name().clone(),
                required: !field.is_nullable(),
                field_type: Box::new(field_type.clone()),
                initial_default: None,
                write_default: None,
            };
            results.push(Arc::new(nested_field));
        }
        Ok(results)
    }
}

impl ArrowSchemaVisitor for ArrowSchemaConverter {
    type T = Type;
    type U = Schema;

    fn schema(&mut self, schema: &ArrowSchema, values: Vec<Self::T>) -> Result<Self::U> {
        let fields = Self::convert_fields(schema.fields(), &values)?;
        let builder = Schema::builder().with_fields(fields);
        builder.build()
    }

    fn r#struct(&mut self, fields: &Fields, results: Vec<Self::T>) -> Result<Self::T> {
        let fields = Self::convert_fields(fields, &results)?;
        Ok(Type::Struct(StructType::new(fields)))
    }

    fn list(&mut self, list: &DataType, value: Self::T) -> Result<Self::T> {
        match list {
            DataType::List(element_field) => {
                let id = get_field_id(element_field).ok_or(Error::new(
                    ErrorKind::DataInvalid,
                    "Field id not found in metadata",
                ))?;
                let doc = get_field_doc(element_field);
                let element_field = Arc::new(NestedField {
                    id,
                    doc,
                    name: element_field.name().clone(),
                    required: !element_field.is_nullable(),
                    field_type: Box::new(value.clone()),
                    initial_default: None,
                    write_default: None,
                });
                Ok(Type::List(ListType { element_field }))
            }
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                "List type must have list data type",
            )),
        }
    }

    fn map(&mut self, map: &DataType, key_value: Self::T, value: Self::T) -> Result<Self::T> {
        match map {
            DataType::Map(field, _) => match field.data_type() {
                DataType::Struct(fields) => {
                    if fields.len() != 2 {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            "Map field must have exactly 2 fields",
                        ));
                    }

                    let key_field = &fields[0];
                    let value_field = &fields[1];

                    let key_id = get_field_id(key_field).ok_or(Error::new(
                        ErrorKind::DataInvalid,
                        "Field id not found in metadata",
                    ))?;
                    let key_doc = get_field_doc(key_field);
                    let key_field = Arc::new(NestedField {
                        id: key_id,
                        doc: key_doc,
                        name: key_field.name().clone(),
                        required: !key_field.is_nullable(),
                        field_type: Box::new(key_value.clone()),
                        initial_default: None,
                        write_default: None,
                    });

                    let value_id = get_field_id(value_field).ok_or(Error::new(
                        ErrorKind::DataInvalid,
                        "Field id not found in metadata",
                    ))?;
                    let value_doc = get_field_doc(value_field);
                    let value_field = Arc::new(NestedField {
                        id: value_id,
                        doc: value_doc,
                        name: value_field.name().clone(),
                        required: !value_field.is_nullable(),
                        field_type: Box::new(value.clone()),
                        initial_default: None,
                        write_default: None,
                    });

                    Ok(Type::Map(MapType {
                        key_field,
                        value_field,
                    }))
                }
                _ => Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Map field must have struct type",
                )),
            },
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                "Map type must have map data type",
            )),
        }
    }

    fn primitive(&mut self, p: &DataType) -> Result<Self::T> {
        match p {
            DataType::Boolean => Ok(Type::Primitive(PrimitiveType::Boolean)),
            DataType::Int32 => Ok(Type::Primitive(PrimitiveType::Int)),
            DataType::Int64 => Ok(Type::Primitive(PrimitiveType::Long)),
            DataType::Float32 => Ok(Type::Primitive(PrimitiveType::Float)),
            DataType::Float64 => Ok(Type::Primitive(PrimitiveType::Double)),
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
            DataType::Binary => Ok(Type::Primitive(PrimitiveType::Binary)),
            DataType::FixedSizeBinary(width) => {
                Ok(Type::Primitive(PrimitiveType::Fixed(*width as u64)))
            }
            DataType::Utf8 => Ok(Type::Primitive(PrimitiveType::String)),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Unsupported Arrow data type: {p}"),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::DataType;
    use arrow_schema::Field;
    use arrow_schema::Schema as ArrowSchema;
    use arrow_schema::TimeUnit;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn test_arrow_primitive() {
        let schema = ArrowSchema::new(vec![
            Field::new("a", DataType::Int32, false).with_metadata(HashMap::from([(
                ARROW_FIELD_ID_KEYS[0].to_string(),
                "2".to_string(),
            )])),
            Field::new("b", DataType::Utf8, false).with_metadata(HashMap::from([(
                ARROW_FIELD_ID_KEYS[0].to_string(),
                "0".to_string(),
            )])),
            Field::new("c", DataType::Timestamp(TimeUnit::Microsecond, None), false).with_metadata(
                HashMap::from([(ARROW_FIELD_ID_KEYS[0].to_string(), "1".to_string())]),
            ),
        ]);
        let schema = Arc::new(schema);
        let mut visitor = ArrowSchemaConverter::new();
        let result = visit_schema(&schema, &mut visitor).unwrap();
        let schema_struct = result.as_struct();
        assert_eq!(schema_struct.fields().len(), 3);

        assert_eq!(schema_struct.fields()[0].name, "a");
        assert_eq!(schema_struct.fields()[0].id, 2);
        assert_eq!(
            schema_struct.fields()[0].field_type,
            Box::new(Type::Primitive(PrimitiveType::Int))
        );

        assert_eq!(schema_struct.fields()[1].name, "b");
        assert_eq!(schema_struct.fields()[1].id, 0);
        assert_eq!(
            schema_struct.fields()[1].field_type,
            Box::new(Type::Primitive(PrimitiveType::String))
        );

        assert_eq!(schema_struct.fields()[2].name, "c");
        assert_eq!(schema_struct.fields()[2].id, 1);
        assert_eq!(
            schema_struct.fields()[2].field_type,
            Box::new(Type::Primitive(PrimitiveType::Timestamp))
        );
    }

    #[test]
    fn test_arrow_list() {
        let schema = ArrowSchema::new(vec![Field::new(
            "a",
            DataType::List(Arc::new(
                Field::new("item", DataType::Int32, false).with_metadata(HashMap::from([(
                    ARROW_FIELD_ID_KEYS[0].to_string(),
                    "0".to_string(),
                )])),
            )),
            true,
        )
        .with_metadata(HashMap::from([(
            ARROW_FIELD_ID_KEYS[0].to_string(),
            "1".to_string(),
        )]))]);
        let schema = Arc::new(schema);
        let mut visitor = ArrowSchemaConverter::new();
        let result = visit_schema(&schema, &mut visitor).unwrap();
        let schema_struct = result.as_struct();
        assert_eq!(schema_struct.fields().len(), 1);

        assert_eq!(schema_struct.fields()[0].name, "a");
        assert_eq!(schema_struct.fields()[0].id, 1);
        assert!(!schema_struct.fields()[0].required);
        assert_eq!(
            schema_struct.fields()[0].field_type,
            Box::new(Type::List(ListType {
                element_field: Arc::new(NestedField {
                    id: 0,
                    doc: None,
                    name: "item".to_string(),
                    required: true,
                    field_type: Box::new(Type::Primitive(PrimitiveType::Int)),
                    initial_default: None,
                    write_default: None,
                })
            }))
        );
    }

    #[test]
    fn test_arrow_map() {
        let fields = Fields::from(vec![
            Field::new("key", DataType::Int32, false).with_metadata(HashMap::from([(
                ARROW_FIELD_ID_KEYS[0].to_string(),
                "2".to_string(),
            )])),
            Field::new("value", DataType::Utf8, true).with_metadata(HashMap::from([(
                ARROW_FIELD_ID_KEYS[0].to_string(),
                "0".to_string(),
            )])),
        ]);

        let r#struct = DataType::Struct(fields);
        let map = DataType::Map(
            Arc::new(
                Field::new("entries", r#struct, false).with_metadata(HashMap::from([(
                    ARROW_FIELD_ID_KEYS[0].to_string(),
                    "1".to_string(),
                )])),
            ),
            false,
        );

        let schema = ArrowSchema::new(vec![Field::new("m", map, false).with_metadata(
            HashMap::from([(ARROW_FIELD_ID_KEYS[0].to_string(), "4".to_string())]),
        )]);
        let schema = Arc::new(schema);
        let mut visitor = ArrowSchemaConverter::new();
        let result = visit_schema(&schema, &mut visitor).unwrap();
        let schema_struct = result.as_struct();
        assert_eq!(schema_struct.fields().len(), 1);

        assert_eq!(schema_struct.fields()[0].name, "m");
        assert_eq!(schema_struct.fields()[0].id, 4);
        assert!(schema_struct.fields()[0].required);
        assert_eq!(
            schema_struct.fields()[0].field_type,
            Box::new(Type::Map(MapType {
                key_field: Arc::new(NestedField {
                    id: 2,
                    doc: None,
                    name: "key".to_string(),
                    required: true,
                    field_type: Box::new(Type::Primitive(PrimitiveType::Int)),
                    initial_default: None,
                    write_default: None,
                }),
                value_field: Arc::new(NestedField {
                    id: 0,
                    doc: None,
                    name: "value".to_string(),
                    required: false,
                    field_type: Box::new(Type::Primitive(PrimitiveType::String)),
                    initial_default: None,
                    write_default: None,
                })
            }))
        );
    }

    #[test]
    fn test_arrow_struct() {
        let fields = Fields::from(vec![
            Field::new("a", DataType::Int32, false).with_metadata(HashMap::from([(
                ARROW_FIELD_ID_KEYS[0].to_string(),
                "2".to_string(),
            )])),
            Field::new("b", DataType::Utf8, true).with_metadata(HashMap::from([(
                ARROW_FIELD_ID_KEYS[0].to_string(),
                "0".to_string(),
            )])),
            Field::new("c", DataType::Timestamp(TimeUnit::Microsecond, None), false).with_metadata(
                HashMap::from([(ARROW_FIELD_ID_KEYS[0].to_string(), "1".to_string())]),
            ),
        ]);

        let r#struct = DataType::Struct(fields);
        let schema = ArrowSchema::new(vec![Field::new("s", r#struct, false).with_metadata(
            HashMap::from([(ARROW_FIELD_ID_KEYS[0].to_string(), "2".to_string())]),
        )]);
        let schema = Arc::new(schema);
        let mut visitor = ArrowSchemaConverter::new();
        let result = visit_schema(&schema, &mut visitor).unwrap();
        let schema_struct = result.as_struct();
        assert_eq!(schema_struct.fields().len(), 1);

        assert_eq!(schema_struct.fields()[0].name, "s");
        assert_eq!(schema_struct.fields()[0].id, 2);
        assert_eq!(
            schema_struct.fields()[0].field_type,
            Box::new(Type::Struct(StructType::new(vec![
                Arc::new(NestedField {
                    id: 2,
                    doc: None,
                    name: "a".to_string(),
                    required: true,
                    field_type: Box::new(Type::Primitive(PrimitiveType::Int)),
                    initial_default: None,
                    write_default: None,
                }),
                Arc::new(NestedField {
                    id: 0,
                    doc: None,
                    name: "b".to_string(),
                    required: false,
                    field_type: Box::new(Type::Primitive(PrimitiveType::String)),
                    initial_default: None,
                    write_default: None,
                }),
                Arc::new(NestedField {
                    id: 1,
                    doc: None,
                    name: "c".to_string(),
                    required: true,
                    field_type: Box::new(Type::Primitive(PrimitiveType::Timestamp)),
                    initial_default: None,
                    write_default: None,
                }),
            ])))
        );
    }
}
