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

use crate::error::Result;
use crate::spec::{
    Datum, ListType, MapType, NestedField, NestedFieldRef, PrimitiveLiteral, PrimitiveType, Schema,
    SchemaVisitor, StructType, Type,
};
use crate::{Error, ErrorKind};
use arrow_array::types::{validate_decimal_precision_and_scale, Decimal128Type};
use arrow_array::{
    BooleanArray, Datum as ArrowDatum, Float32Array, Float64Array, Int32Array, Int64Array,
};
use arrow_schema::{DataType, Field, Fields, Schema as ArrowSchema, TimeUnit};
use bitvec::macros::internal::funty::Fundamental;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use rust_decimal::prelude::ToPrimitive;
use std::collections::HashMap;
use std::sync::Arc;

/// A post order arrow schema visitor.
///
/// For order of methods called, please refer to [`visit_schema`].
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
                    | DataType::LargeUtf8
                    | DataType::Binary
                    | DataType::LargeBinary
                    | DataType::FixedSizeBinary(_)
            ) =>
        {
            visitor.primitive(p)
        }
        DataType::List(element_field) => visit_list(r#type, element_field, visitor),
        DataType::LargeList(element_field) => visit_list(r#type, element_field, visitor),
        DataType::FixedSizeList(element_field, _) => visit_list(r#type, element_field, visitor),
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
                    visitor.before_map_key(key_field)?;
                    let ret = visit_type(key_field.data_type(), visitor)?;
                    visitor.after_map_key(key_field)?;
                    ret
                };

                let value_result = {
                    visitor.before_map_value(value_field)?;
                    let ret = visit_type(value_field.data_type(), visitor)?;
                    visitor.after_map_value(value_field)?;
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

/// Visit list types in post order.
#[allow(dead_code)]
fn visit_list<V: ArrowSchemaVisitor>(
    data_type: &DataType,
    element_field: &Field,
    visitor: &mut V,
) -> Result<V::T> {
    visitor.before_list_element(element_field)?;
    let value = visit_type(element_field.data_type(), visitor)?;
    visitor.after_list_element(element_field)?;
    visitor.list(data_type, value)
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
fn visit_schema<V: ArrowSchemaVisitor>(schema: &ArrowSchema, visitor: &mut V) -> Result<V::U> {
    let mut results = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        visitor.before_field(field)?;
        let result = visit_type(field.data_type(), visitor)?;
        visitor.after_field(field)?;
        results.push(result);
    }
    visitor.schema(schema, results)
}

/// Convert Arrow schema to ceberg schema.
#[allow(dead_code)]
pub fn arrow_schema_to_schema(schema: &ArrowSchema) -> Result<Schema> {
    let mut visitor = ArrowSchemaConverter::new();
    visit_schema(schema, &mut visitor)
}

const ARROW_FIELD_DOC_KEY: &str = "doc";

fn get_field_id(field: &Field) -> Result<i32> {
    if let Some(value) = field.metadata().get(PARQUET_FIELD_ID_META_KEY) {
        return value.parse::<i32>().map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                "Failed to parse field id".to_string(),
            )
            .with_context("value", value)
            .with_source(e)
        });
    }
    Err(Error::new(
        ErrorKind::DataInvalid,
        "Field id not found in metadata",
    ))
}

fn get_field_doc(field: &Field) -> Option<String> {
    if let Some(value) = field.metadata().get(ARROW_FIELD_DOC_KEY) {
        return Some(value.clone());
    }
    None
}

struct ArrowSchemaConverter;

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
            let id = get_field_id(field)?;
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
        let element_field = match list {
            DataType::List(element_field) => element_field,
            DataType::LargeList(element_field) => element_field,
            DataType::FixedSizeList(element_field, _) => element_field,
            _ => {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "List type must have list data type",
                ))
            }
        };

        let id = get_field_id(element_field)?;
        let doc = get_field_doc(element_field);
        let mut element_field =
            NestedField::list_element(id, value.clone(), !element_field.is_nullable());
        if let Some(doc) = doc {
            element_field = element_field.with_doc(doc);
        }
        let element_field = Arc::new(element_field);
        Ok(Type::List(ListType { element_field }))
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

                    let key_id = get_field_id(key_field)?;
                    let key_doc = get_field_doc(key_field);
                    let mut key_field = NestedField::map_key_element(key_id, key_value.clone());
                    if let Some(doc) = key_doc {
                        key_field = key_field.with_doc(doc);
                    }
                    let key_field = Arc::new(key_field);

                    let value_id = get_field_id(value_field)?;
                    let value_doc = get_field_doc(value_field);
                    let mut value_field = NestedField::map_value_element(
                        value_id,
                        value.clone(),
                        !value_field.is_nullable(),
                    );
                    if let Some(doc) = value_doc {
                        value_field = value_field.with_doc(doc);
                    }
                    let value_field = Arc::new(value_field);

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
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Unsupported Arrow data type: {p}"),
            )),
        }
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
            "entries",
            DataType::Struct(vec![key_field, value_field].into()),
            map.value_field.required,
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
                DataType::Time32(TimeUnit::Microsecond),
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
        arrow_schema_to_schema(schema)
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
    use super::*;
    use crate::spec::Schema;
    use arrow_schema::DataType;
    use arrow_schema::Field;
    use arrow_schema::Schema as ArrowSchema;
    use arrow_schema::TimeUnit;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn arrow_schema_for_arrow_schema_to_schema_test() -> ArrowSchema {
        let fields = Fields::from(vec![
            Field::new("key", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "17".to_string(),
            )])),
            Field::new("value", DataType::Utf8, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "18".to_string(),
            )])),
        ]);

        let r#struct = DataType::Struct(fields);
        let map = DataType::Map(
            Arc::new(
                Field::new("entries", r#struct, false).with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "19".to_string(),
                )])),
            ),
            false,
        );

        let fields = Fields::from(vec![
            Field::new("aa", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "18".to_string(),
            )])),
            Field::new("bb", DataType::Utf8, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "19".to_string(),
            )])),
            Field::new(
                "cc",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            )
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "20".to_string(),
            )])),
        ]);

        let r#struct = DataType::Struct(fields);

        ArrowSchema::new(vec![
            Field::new("a", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
            Field::new("b", DataType::Int64, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            Field::new("c", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "3".to_string(),
            )])),
            Field::new("n", DataType::LargeUtf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "21".to_string(),
            )])),
            Field::new("d", DataType::Timestamp(TimeUnit::Microsecond, None), true).with_metadata(
                HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "4".to_string())]),
            ),
            Field::new("e", DataType::Boolean, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "6".to_string(),
            )])),
            Field::new("f", DataType::Float32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "5".to_string(),
            )])),
            Field::new("g", DataType::Float64, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "7".to_string(),
            )])),
            Field::new("p", DataType::Decimal128(10, 2), false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "27".to_string(),
            )])),
            Field::new("h", DataType::Date32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "8".to_string(),
            )])),
            Field::new("i", DataType::Time64(TimeUnit::Microsecond), false).with_metadata(
                HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "9".to_string())]),
            ),
            Field::new(
                "j",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            )
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "10".to_string(),
            )])),
            Field::new(
                "k",
                DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                false,
            )
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "12".to_string(),
            )])),
            Field::new("l", DataType::Binary, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "13".to_string(),
            )])),
            Field::new("o", DataType::LargeBinary, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "22".to_string(),
            )])),
            Field::new("m", DataType::FixedSizeBinary(10), false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "11".to_string(),
            )])),
            Field::new(
                "list",
                DataType::List(Arc::new(
                    Field::new("element", DataType::Int32, false).with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "15".to_string(),
                    )])),
                )),
                true,
            )
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "14".to_string(),
            )])),
            Field::new(
                "large_list",
                DataType::LargeList(Arc::new(
                    Field::new("element", DataType::Utf8, false).with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "23".to_string(),
                    )])),
                )),
                true,
            )
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "24".to_string(),
            )])),
            Field::new(
                "fixed_list",
                DataType::FixedSizeList(
                    Arc::new(
                        Field::new("element", DataType::Binary, false).with_metadata(
                            HashMap::from([(
                                PARQUET_FIELD_ID_META_KEY.to_string(),
                                "26".to_string(),
                            )]),
                        ),
                    ),
                    10,
                ),
                true,
            )
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "25".to_string(),
            )])),
            Field::new("map", map, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "16".to_string(),
            )])),
            Field::new("struct", r#struct, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "17".to_string(),
            )])),
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
            Field::new("key", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "17".to_string(),
            )])),
            Field::new("value", DataType::Utf8, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "18".to_string(),
            )])),
        ]);

        let r#struct = DataType::Struct(fields);
        let map = DataType::Map(Arc::new(Field::new("entries", r#struct, false)), false);

        let fields = Fields::from(vec![
            Field::new("aa", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "18".to_string(),
            )])),
            Field::new("bb", DataType::Utf8, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "19".to_string(),
            )])),
            Field::new(
                "cc",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            )
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "20".to_string(),
            )])),
        ]);

        let r#struct = DataType::Struct(fields);

        ArrowSchema::new(vec![
            Field::new("a", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
            Field::new("b", DataType::Int64, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            Field::new("c", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "3".to_string(),
            )])),
            Field::new("n", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "21".to_string(),
            )])),
            Field::new("d", DataType::Timestamp(TimeUnit::Microsecond, None), true).with_metadata(
                HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "4".to_string())]),
            ),
            Field::new("e", DataType::Boolean, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "6".to_string(),
            )])),
            Field::new("f", DataType::Float32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "5".to_string(),
            )])),
            Field::new("g", DataType::Float64, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "7".to_string(),
            )])),
            Field::new("p", DataType::Decimal128(10, 2), false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "27".to_string(),
            )])),
            Field::new("h", DataType::Date32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "8".to_string(),
            )])),
            Field::new("i", DataType::Time32(TimeUnit::Microsecond), false).with_metadata(
                HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "9".to_string())]),
            ),
            Field::new(
                "j",
                DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                false,
            )
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "10".to_string(),
            )])),
            Field::new(
                "k",
                DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                false,
            )
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "12".to_string(),
            )])),
            Field::new("l", DataType::LargeBinary, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "13".to_string(),
            )])),
            Field::new("o", DataType::LargeBinary, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "22".to_string(),
            )])),
            Field::new("m", DataType::FixedSizeBinary(10), false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "11".to_string(),
            )])),
            Field::new(
                "list",
                DataType::List(Arc::new(
                    Field::new("element", DataType::Int32, false).with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "15".to_string(),
                    )])),
                )),
                true,
            )
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "14".to_string(),
            )])),
            Field::new(
                "large_list",
                DataType::List(Arc::new(
                    Field::new("element", DataType::Utf8, false).with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "23".to_string(),
                    )])),
                )),
                true,
            )
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "24".to_string(),
            )])),
            Field::new(
                "fixed_list",
                DataType::List(Arc::new(
                    Field::new("element", DataType::LargeBinary, false).with_metadata(
                        HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "26".to_string())]),
                    ),
                )),
                true,
            )
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "25".to_string(),
            )])),
            Field::new("map", map, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "16".to_string(),
            )])),
            Field::new("struct", r#struct, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "17".to_string(),
            )])),
            Field::new("uuid", DataType::FixedSizeBinary(16), false).with_metadata(HashMap::from(
                [(PARQUET_FIELD_ID_META_KEY.to_string(), "26".to_string())],
            )),
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
