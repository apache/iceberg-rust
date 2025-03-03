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

use arrow_array::types::{
    validate_decimal_precision_and_scale, Decimal128Type, TimestampMicrosecondType,
};
use arrow_array::{
    BooleanArray, Date32Array, Datum as ArrowDatum, Float32Array, Float64Array, Int32Array,
    Int64Array, PrimitiveArray, Scalar, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Fields, Schema as ArrowSchema, TimeUnit};
use bitvec::macros::internal::funty::Fundamental;
use num_bigint::BigInt;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::file::statistics::Statistics;
use rust_decimal::prelude::ToPrimitive;
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{
    Datum, ListType, MapType, NestedField, NestedFieldRef, PrimitiveLiteral, PrimitiveType, Schema,
    SchemaVisitor, StructType, Type,
};
use crate::{Error, ErrorKind};

/// When iceberg map type convert to Arrow map type, the default map field name is "key_value".
pub const DEFAULT_MAP_FIELD_NAME: &str = "key_value";
/// UTC time zone for Arrow timestamp type.
pub const UTC_TIME_ZONE: &str = "+00:00";

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
                    | DataType::Utf8View
                    | DataType::Binary
                    | DataType::LargeBinary
                    | DataType::BinaryView
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

/// Convert Arrow schema to Iceberg schema.
pub fn arrow_schema_to_schema(schema: &ArrowSchema) -> Result<Schema> {
    let mut visitor = ArrowSchemaConverter::new();
    visit_schema(schema, &mut visitor)
}

/// Convert Arrow type to iceberg type.
pub fn arrow_type_to_type(ty: &DataType) -> Result<Type> {
    let mut visitor = ArrowSchemaConverter::new();
    visit_type(ty, &mut visitor)
}

const ARROW_FIELD_DOC_KEY: &str = "doc";

pub(super) fn get_field_id(field: &Field) -> Result<i32> {
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
            DataType::Int8 | DataType::Int16 | DataType::Int32 => {
                Ok(Type::Primitive(PrimitiveType::Int))
            }
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
            DataType::Timestamp(unit, None) if unit == &TimeUnit::Nanosecond => {
                Ok(Type::Primitive(PrimitiveType::TimestampNs))
            }
            DataType::Timestamp(unit, Some(zone))
                if unit == &TimeUnit::Microsecond
                    && (zone.as_ref() == "UTC" || zone.as_ref() == "+00:00") =>
            {
                Ok(Type::Primitive(PrimitiveType::Timestamptz))
            }
            DataType::Timestamp(unit, Some(zone))
                if unit == &TimeUnit::Nanosecond
                    && (zone.as_ref() == "UTC" || zone.as_ref() == "+00:00") =>
            {
                Ok(Type::Primitive(PrimitiveType::TimestamptzNs))
            }
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
                Ok(Type::Primitive(PrimitiveType::Binary))
            }
            DataType::FixedSizeBinary(width) => {
                Ok(Type::Primitive(PrimitiveType::Fixed(*width as u64)))
            }
            DataType::Utf8View | DataType::Utf8 | DataType::LargeUtf8 => {
                Ok(Type::Primitive(PrimitiveType::String))
            }
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
                DataType::Timestamp(TimeUnit::Microsecond, Some(UTC_TIME_ZONE.into())),
            )),
            crate::spec::PrimitiveType::TimestampNs => Ok(ArrowSchemaOrFieldOrType::Type(
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            )),
            crate::spec::PrimitiveType::TimestamptzNs => Ok(ArrowSchemaOrFieldOrType::Type(
                // Store timestamptz_ns as UTC
                DataType::Timestamp(TimeUnit::Nanosecond, Some(UTC_TIME_ZONE.into())),
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

/// Convert iceberg type to an arrow type.
pub fn type_to_arrow_type(ty: &crate::spec::Type) -> crate::Result<DataType> {
    let mut converter = ToArrowSchemaConverter;
    match crate::spec::visit_type(ty, &mut converter)? {
        ArrowSchemaOrFieldOrType::Type(ty) => Ok(ty),
        _ => unreachable!(),
    }
}

/// Convert Iceberg Datum to Arrow Datum.
pub(crate) fn get_arrow_datum(datum: &Datum) -> Result<Box<dyn ArrowDatum + Send>> {
    match (datum.data_type(), datum.literal()) {
        (PrimitiveType::Boolean, PrimitiveLiteral::Boolean(value)) => {
            Ok(Box::new(BooleanArray::new_scalar(*value)))
        }
        (PrimitiveType::Int, PrimitiveLiteral::Int(value)) => {
            Ok(Box::new(Int32Array::new_scalar(*value)))
        }
        (PrimitiveType::Long, PrimitiveLiteral::Long(value)) => {
            Ok(Box::new(Int64Array::new_scalar(*value)))
        }
        (PrimitiveType::Float, PrimitiveLiteral::Float(value)) => {
            Ok(Box::new(Float32Array::new_scalar(value.as_f32())))
        }
        (PrimitiveType::Double, PrimitiveLiteral::Double(value)) => {
            Ok(Box::new(Float64Array::new_scalar(value.as_f64())))
        }
        (PrimitiveType::String, PrimitiveLiteral::String(value)) => {
            Ok(Box::new(StringArray::new_scalar(value.as_str())))
        }
        (PrimitiveType::Date, PrimitiveLiteral::Int(value)) => {
            Ok(Box::new(Date32Array::new_scalar(*value)))
        }
        (PrimitiveType::Timestamp, PrimitiveLiteral::Long(value)) => {
            Ok(Box::new(TimestampMicrosecondArray::new_scalar(*value)))
        }
        (PrimitiveType::Timestamptz, PrimitiveLiteral::Long(value)) => Ok(Box::new(Scalar::new(
            PrimitiveArray::<TimestampMicrosecondType>::new(vec![*value; 1].into(), None)
                .with_timezone("UTC"),
        ))),

        (primitive_type, _) => Err(Error::new(
            ErrorKind::FeatureUnsupported,
            format!(
                "Converting datum from type {:?} to arrow not supported yet.",
                primitive_type
            ),
        )),
    }
}

macro_rules! get_parquet_stat_as_datum {
    ($limit_type:tt) => {
        paste::paste! {
        /// Gets the $limit_type value from a parquet Statistics struct, as a Datum
        pub(crate) fn [<get_parquet_stat_ $limit_type _as_datum>](
            primitive_type: &PrimitiveType, stats: &Statistics
        ) -> Result<Option<Datum>> {
            Ok(match (primitive_type, stats) {
                (PrimitiveType::Boolean, Statistics::Boolean(stats)) => stats.[<$limit_type _opt>]().map(|val|Datum::bool(*val)),
                (PrimitiveType::Int, Statistics::Int32(stats)) => stats.[<$limit_type _opt>]().map(|val|Datum::int(*val)),
                (PrimitiveType::Date, Statistics::Int32(stats)) => stats.[<$limit_type _opt>]().map(|val|Datum::date(*val)),
                (PrimitiveType::Long, Statistics::Int64(stats)) => stats.[<$limit_type _opt>]().map(|val|Datum::long(*val)),
                (PrimitiveType::Time, Statistics::Int64(stats)) => {
                    let Some(val) = stats.[<$limit_type _opt>]() else {
                        return Ok(None);
                    };

                    Some(Datum::time_micros(*val)?)
                }
                (PrimitiveType::Timestamp, Statistics::Int64(stats)) => {
                    stats.[<$limit_type _opt>]().map(|val|Datum::timestamp_micros(*val))
                }
                (PrimitiveType::Timestamptz, Statistics::Int64(stats)) => {
                    stats.[<$limit_type _opt>]().map(|val|Datum::timestamptz_micros(*val))
                }
                (PrimitiveType::TimestampNs, Statistics::Int64(stats)) => {
                    stats.[<$limit_type _opt>]().map(|val|Datum::timestamp_nanos(*val))
                }
                (PrimitiveType::TimestamptzNs, Statistics::Int64(stats)) => {
                    stats.[<$limit_type _opt>]().map(|val|Datum::timestamptz_nanos(*val))
                }
                (PrimitiveType::Float, Statistics::Float(stats)) => stats.[<$limit_type _opt>]().map(|val|Datum::float(*val)),
                (PrimitiveType::Double, Statistics::Double(stats)) => stats.[<$limit_type _opt>]().map(|val|Datum::double(*val)),
                (PrimitiveType::String, Statistics::ByteArray(stats)) => {
                    let Some(val) = stats.[<$limit_type _opt>]() else {
                        return Ok(None);
                    };

                    Some(Datum::string(val.as_utf8()?))
                }
                (PrimitiveType::Decimal {
                    precision: _,
                    scale: _,
                }, Statistics::ByteArray(stats)) => {
                    let Some(bytes) = stats.[<$limit_type _bytes_opt>]() else {
                        return Ok(None);
                    };
                    Some(Datum::new(
                        primitive_type.clone(),
                        PrimitiveLiteral::Int128(i128::from_be_bytes(bytes.try_into()?)),
                    ))
                }
                (PrimitiveType::Decimal {
                    precision: _,
                    scale: _,
                }, Statistics::FixedLenByteArray(stats)) => {
                    let Some(bytes) = stats.[<$limit_type _bytes_opt>]() else {
                        return Ok(None);
                    };
                    let unscaled_value = BigInt::from_signed_bytes_be(bytes);
                    Some(Datum::new(
                        primitive_type.clone(),
                        PrimitiveLiteral::Int128(unscaled_value.to_i128().ok_or_else(|| {
                            Error::new(
                                ErrorKind::DataInvalid,
                                format!("Can't convert bytes to i128: {:?}", bytes),
                            )
                        })?),
                    ))
                }
                (
                PrimitiveType::Decimal {
                    precision: _,
                    scale: _,
                },
                Statistics::Int32(stats)) => {
                    stats.[<$limit_type _opt>]().map(|val| {
                        Datum::new(
                            primitive_type.clone(),
                            PrimitiveLiteral::Int128(i128::from(*val)),
                        )
                    })
                }

                (
                    PrimitiveType::Decimal {
                        precision: _,
                        scale: _,
                    },
                    Statistics::Int64(stats),
                ) => {
                    stats.[<$limit_type _opt>]().map(|val| {
                        Datum::new(
                            primitive_type.clone(),
                            PrimitiveLiteral::Int128(i128::from(*val)),
                        )
                    })
                }
                (PrimitiveType::Uuid, Statistics::FixedLenByteArray(stats)) => {
                    let Some(bytes) = stats.[<$limit_type _bytes_opt>]() else {
                        return Ok(None);
                    };
                    if bytes.len() != 16 {
                        return Err(Error::new(
                            ErrorKind::Unexpected,
                            "Invalid length of uuid bytes.",
                        ));
                    }
                    Some(Datum::uuid(Uuid::from_bytes(
                        bytes[..16].try_into().unwrap(),
                    )))
                }
                (PrimitiveType::Fixed(len), Statistics::FixedLenByteArray(stat)) => {
                    let Some(bytes) = stat.[<$limit_type _bytes_opt>]() else {
                        return Ok(None);
                    };
                    if bytes.len() != *len as usize {
                        return Err(Error::new(
                            ErrorKind::Unexpected,
                            "Invalid length of fixed bytes.",
                        ));
                    }
                    Some(Datum::fixed(bytes.to_vec()))
                }
                (PrimitiveType::Binary, Statistics::ByteArray(stat)) => {
                    return Ok(stat.[<$limit_type _bytes_opt>]().map(|bytes|Datum::binary(bytes.to_vec())))
                }
                _ => {
                    return Ok(None);
                }
            })
            }
        }
    }
}

get_parquet_stat_as_datum!(min);

get_parquet_stat_as_datum!(max);

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
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};

    use super::*;
    use crate::spec::{Literal, Schema};

    /// Create a simple field with metadata.
    fn simple_field(name: &str, ty: DataType, nullable: bool, value: &str) -> Field {
        Field::new(name, ty, nullable).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            value.to_string(),
        )]))
    }

    fn arrow_schema_for_arrow_schema_to_schema_test() -> ArrowSchema {
        let fields = Fields::from(vec![
            simple_field("key", DataType::Int32, false, "28"),
            simple_field("value", DataType::Utf8, true, "29"),
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
                        "key-id": 28,
                        "key": "int",
                        "value-id": 29,
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
            simple_field("key", DataType::Int32, false, "28"),
            simple_field("value", DataType::Utf8, true, "29"),
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
            simple_field("uuid", DataType::FixedSizeBinary(16), false, "30"),
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
                        "key-id": 28,
                        "key": "int",
                        "value-id": 29,
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
                    "id":30,
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

    #[test]
    fn test_type_conversion() {
        // test primitive type
        {
            let arrow_type = DataType::Int32;
            let iceberg_type = Type::Primitive(PrimitiveType::Int);
            assert_eq!(arrow_type, type_to_arrow_type(&iceberg_type).unwrap());
            assert_eq!(iceberg_type, arrow_type_to_type(&arrow_type).unwrap());
        }

        // test struct type
        {
            // no metadata will cause error
            let arrow_type = DataType::Struct(Fields::from(vec![
                Field::new("a", DataType::Int64, false),
                Field::new("b", DataType::Utf8, true),
            ]));
            assert_eq!(
                &arrow_type_to_type(&arrow_type).unwrap_err().to_string(),
                "DataInvalid => Field id not found in metadata"
            );

            let arrow_type = DataType::Struct(Fields::from(vec![
                Field::new("a", DataType::Int64, false).with_metadata(HashMap::from_iter([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    1.to_string(),
                )])),
                Field::new("b", DataType::Utf8, true).with_metadata(HashMap::from_iter([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    2.to_string(),
                )])),
            ]));
            let iceberg_type = Type::Struct(StructType::new(vec![
                NestedField {
                    id: 1,
                    doc: None,
                    name: "a".to_string(),
                    required: true,
                    field_type: Box::new(Type::Primitive(PrimitiveType::Long)),
                    initial_default: None,
                    write_default: None,
                }
                .into(),
                NestedField {
                    id: 2,
                    doc: None,
                    name: "b".to_string(),
                    required: false,
                    field_type: Box::new(Type::Primitive(PrimitiveType::String)),
                    initial_default: None,
                    write_default: None,
                }
                .into(),
            ]));
            assert_eq!(iceberg_type, arrow_type_to_type(&arrow_type).unwrap());
            assert_eq!(arrow_type, type_to_arrow_type(&iceberg_type).unwrap());

            // initial_default and write_default is ignored
            let iceberg_type = Type::Struct(StructType::new(vec![
                NestedField {
                    id: 1,
                    doc: None,
                    name: "a".to_string(),
                    required: true,
                    field_type: Box::new(Type::Primitive(PrimitiveType::Long)),
                    initial_default: Some(Literal::Primitive(PrimitiveLiteral::Int(114514))),
                    write_default: None,
                }
                .into(),
                NestedField {
                    id: 2,
                    doc: None,
                    name: "b".to_string(),
                    required: false,
                    field_type: Box::new(Type::Primitive(PrimitiveType::String)),
                    initial_default: None,
                    write_default: Some(Literal::Primitive(PrimitiveLiteral::String(
                        "514".to_string(),
                    ))),
                }
                .into(),
            ]));
            assert_eq!(arrow_type, type_to_arrow_type(&iceberg_type).unwrap());
        }
    }
}
