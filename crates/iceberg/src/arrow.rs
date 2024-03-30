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

//! Parquet file data reader

use crate::{Error, ErrorKind};
use arrow_schema::SchemaRef as ArrowSchemaRef;
use async_stream::try_stream;
use futures::stream::StreamExt;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask, PARQUET_FIELD_ID_META_KEY};
use parquet::schema::types::{SchemaDescriptor, Type};
use std::collections::HashMap;
use std::str::FromStr;

use crate::io::FileIO;
use crate::scan::{ArrowRecordBatchStream, FileScanTaskStream};
use crate::spec::SchemaRef;

use crate::error::Result;
use crate::spec::{
    ListType, MapType, NestedField, NestedFieldRef, PrimitiveType, Schema, StructType, Type,
};
use crate::{Error, ErrorKind};
use arrow_schema::{DataType, Field, Fields, Schema as ArrowSchema, TimeUnit};
use std::sync::Arc;

/// Builder to create ArrowReader
pub struct ArrowReaderBuilder {
    batch_size: Option<usize>,
    field_ids: Vec<usize>,
    file_io: FileIO,
    schema: SchemaRef,
}

impl ArrowReaderBuilder {
    /// Create a new ArrowReaderBuilder
    pub fn new(file_io: FileIO, schema: SchemaRef) -> Self {
        ArrowReaderBuilder {
            batch_size: None,
            field_ids: vec![],
            file_io,
            schema,
        }
    }

    /// Sets the desired size of batches in the response
    /// to something other than the default
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    /// Sets the desired column projection with a list of field ids.
    pub fn with_field_ids(mut self, field_ids: impl IntoIterator<Item = usize>) -> Self {
        self.field_ids = field_ids.into_iter().collect();
        self
    }

    /// Build the ArrowReader.
    pub fn build(self) -> ArrowReader {
        ArrowReader {
            batch_size: self.batch_size,
            field_ids: self.field_ids,
            schema: self.schema,
            file_io: self.file_io,
        }
    }
}

/// Reads data from Parquet files
pub struct ArrowReader {
    batch_size: Option<usize>,
    field_ids: Vec<usize>,
    #[allow(dead_code)]
    schema: SchemaRef,
    file_io: FileIO,
}

impl ArrowReader {
    /// Take a stream of FileScanTasks and reads all the files.
    /// Returns a stream of Arrow RecordBatches containing the data from the files
    pub fn read(self, mut tasks: FileScanTaskStream) -> crate::Result<ArrowRecordBatchStream> {
        let file_io = self.file_io.clone();

        Ok(try_stream! {
            while let Some(Ok(task)) = tasks.next().await {
                let parquet_reader = file_io
                    .new_input(task.data().data_file().file_path())?
                    .reader()
                    .await?;

                let mut batch_stream_builder = ParquetRecordBatchStreamBuilder::new(parquet_reader)
                    .await?;

                let parquet_schema = batch_stream_builder.parquet_schema();
                let arrow_schema = batch_stream_builder.schema();
                let projection_mask = self.get_arrow_projection_mask(parquet_schema, arrow_schema)?;
                batch_stream_builder = batch_stream_builder.with_projection(projection_mask);

                if let Some(batch_size) = self.batch_size {
                    batch_stream_builder = batch_stream_builder.with_batch_size(batch_size);
                }

                let mut batch_stream = batch_stream_builder.build()?;

                while let Some(batch) = batch_stream.next().await {
                    yield batch?;
                }
            }
        }
        .boxed())
    }

    fn get_arrow_projection_mask(
        &self,
        parquet_schema: &SchemaDescriptor,
        arrow_schema: &ArrowSchemaRef,
    ) -> crate::Result<ProjectionMask> {
        if self.field_ids.is_empty() {
            Ok(ProjectionMask::all())
        } else {
            // Build the map between field id and column index in Parquet schema.
            let mut column_map = HashMap::new();

            let fields = arrow_schema.fields();
            let filtered = fields.filter_leaves(|idx, field| {
                let field_id =
                    field
                        .metadata()
                        .get(PARQUET_FIELD_ID_META_KEY)
                        .ok_or(Error::new(
                            ErrorKind::DataInvalid,
                            format!("Parquet field {} does not contain field id", field),
                        ))?;
                let field_id = i32::from_str(field_id)?;
                let iceberg_field = self.schema.field_by_id(field_id).ok_or(Error::new(
                    ErrorKind::DataInvalid,
                    format!("Field {} is not found in Iceberg schema", field),
                ))?;


                iceberg_field.field_type.is_primitive()
                column_map.insert(field_id, idx);
                true
            });

            for (idx, field) in parquet_schema.columns().iter().enumerate() {
                let field_type = field.self_type();
                match field_type {
                    Type::PrimitiveType { basic_info, .. } => {
                        if !basic_info.has_id() {
                            return Err(Error::new(
                                ErrorKind::DataInvalid,
                                format!(
                                    "Leave column {:?} in schema doesn't have field id",
                                    field_type
                                ),
                            ));
                        }
                        column_map.insert(basic_info.id(), idx);
                    }
                    Type::GroupType { .. } => {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!(
                                "Leave column in schema should be primitive type but got {:?}",
                                field_type
                            ),
                        ));
                    }
                };
            }

            let mut indices = vec![];
            for field_id in &self.field_ids {
                if let Some(col_idx) = column_map.get(&(*field_id as i32)) {
                    indices.push(*col_idx);
                } else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Field {} is not found in Parquet schema.", field_id),
                    ));
                }
            }
            Ok(ProjectionMask::leaves(parquet_schema, indices))
        }
    }
}

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

const ARROW_FIELD_ID_KEY: &str = "PARQUET:field_id";
const ARROW_FIELD_DOC_KEY: &str = "doc";

fn get_field_id(field: &Field) -> Result<i32> {
    if let Some(value) = field.metadata().get(ARROW_FIELD_ID_KEY) {
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
    fn test_arrow_schema_to_schema() {
        let fields = Fields::from(vec![
            Field::new("key", DataType::Int32, false).with_metadata(HashMap::from([(
                ARROW_FIELD_ID_KEY.to_string(),
                "17".to_string(),
            )])),
            Field::new("value", DataType::Utf8, true).with_metadata(HashMap::from([(
                ARROW_FIELD_ID_KEY.to_string(),
                "18".to_string(),
            )])),
        ]);

        let r#struct = DataType::Struct(fields);
        let map = DataType::Map(
            Arc::new(
                Field::new("entries", r#struct, false).with_metadata(HashMap::from([(
                    ARROW_FIELD_ID_KEY.to_string(),
                    "19".to_string(),
                )])),
            ),
            false,
        );

        let fields = Fields::from(vec![
            Field::new("aa", DataType::Int32, false).with_metadata(HashMap::from([(
                ARROW_FIELD_ID_KEY.to_string(),
                "18".to_string(),
            )])),
            Field::new("bb", DataType::Utf8, true).with_metadata(HashMap::from([(
                ARROW_FIELD_ID_KEY.to_string(),
                "19".to_string(),
            )])),
            Field::new(
                "cc",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            )
            .with_metadata(HashMap::from([(
                ARROW_FIELD_ID_KEY.to_string(),
                "20".to_string(),
            )])),
        ]);

        let r#struct = DataType::Struct(fields);

        let schema =
            ArrowSchema::new(vec![
                Field::new("a", DataType::Int32, false).with_metadata(HashMap::from([(
                    ARROW_FIELD_ID_KEY.to_string(),
                    "2".to_string(),
                )])),
                Field::new("b", DataType::Int64, false).with_metadata(HashMap::from([(
                    ARROW_FIELD_ID_KEY.to_string(),
                    "1".to_string(),
                )])),
                Field::new("c", DataType::Utf8, false).with_metadata(HashMap::from([(
                    ARROW_FIELD_ID_KEY.to_string(),
                    "3".to_string(),
                )])),
                Field::new("n", DataType::LargeUtf8, false).with_metadata(HashMap::from([(
                    ARROW_FIELD_ID_KEY.to_string(),
                    "21".to_string(),
                )])),
                Field::new("d", DataType::Timestamp(TimeUnit::Microsecond, None), true)
                    .with_metadata(HashMap::from([(
                        ARROW_FIELD_ID_KEY.to_string(),
                        "4".to_string(),
                    )])),
                Field::new("e", DataType::Boolean, true).with_metadata(HashMap::from([(
                    ARROW_FIELD_ID_KEY.to_string(),
                    "6".to_string(),
                )])),
                Field::new("f", DataType::Float32, false).with_metadata(HashMap::from([(
                    ARROW_FIELD_ID_KEY.to_string(),
                    "5".to_string(),
                )])),
                Field::new("g", DataType::Float64, false).with_metadata(HashMap::from([(
                    ARROW_FIELD_ID_KEY.to_string(),
                    "7".to_string(),
                )])),
                Field::new("p", DataType::Decimal128(10, 2), false).with_metadata(HashMap::from([
                    (ARROW_FIELD_ID_KEY.to_string(), "27".to_string()),
                ])),
                Field::new("h", DataType::Date32, false).with_metadata(HashMap::from([(
                    ARROW_FIELD_ID_KEY.to_string(),
                    "8".to_string(),
                )])),
                Field::new("i", DataType::Time64(TimeUnit::Microsecond), false).with_metadata(
                    HashMap::from([(ARROW_FIELD_ID_KEY.to_string(), "9".to_string())]),
                ),
                Field::new(
                    "j",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    false,
                )
                .with_metadata(HashMap::from([(
                    ARROW_FIELD_ID_KEY.to_string(),
                    "10".to_string(),
                )])),
                Field::new(
                    "k",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                    false,
                )
                .with_metadata(HashMap::from([(
                    ARROW_FIELD_ID_KEY.to_string(),
                    "12".to_string(),
                )])),
                Field::new("l", DataType::Binary, false).with_metadata(HashMap::from([(
                    ARROW_FIELD_ID_KEY.to_string(),
                    "13".to_string(),
                )])),
                Field::new("o", DataType::LargeBinary, false).with_metadata(HashMap::from([(
                    ARROW_FIELD_ID_KEY.to_string(),
                    "22".to_string(),
                )])),
                Field::new("m", DataType::FixedSizeBinary(10), false).with_metadata(HashMap::from(
                    [(ARROW_FIELD_ID_KEY.to_string(), "11".to_string())],
                )),
                Field::new(
                    "list",
                    DataType::List(Arc::new(
                        Field::new("element", DataType::Int32, false).with_metadata(HashMap::from(
                            [(ARROW_FIELD_ID_KEY.to_string(), "15".to_string())],
                        )),
                    )),
                    true,
                )
                .with_metadata(HashMap::from([(
                    ARROW_FIELD_ID_KEY.to_string(),
                    "14".to_string(),
                )])),
                Field::new(
                    "large_list",
                    DataType::LargeList(Arc::new(
                        Field::new("element", DataType::Utf8, false).with_metadata(HashMap::from(
                            [(ARROW_FIELD_ID_KEY.to_string(), "23".to_string())],
                        )),
                    )),
                    true,
                )
                .with_metadata(HashMap::from([(
                    ARROW_FIELD_ID_KEY.to_string(),
                    "24".to_string(),
                )])),
                Field::new(
                    "fixed_list",
                    DataType::FixedSizeList(
                        Arc::new(
                            Field::new("element", DataType::Binary, false).with_metadata(
                                HashMap::from([(ARROW_FIELD_ID_KEY.to_string(), "26".to_string())]),
                            ),
                        ),
                        10,
                    ),
                    true,
                )
                .with_metadata(HashMap::from([(
                    ARROW_FIELD_ID_KEY.to_string(),
                    "25".to_string(),
                )])),
                Field::new("map", map, false).with_metadata(HashMap::from([(
                    ARROW_FIELD_ID_KEY.to_string(),
                    "16".to_string(),
                )])),
                Field::new("struct", r#struct, false).with_metadata(HashMap::from([(
                    ARROW_FIELD_ID_KEY.to_string(),
                    "17".to_string(),
                )])),
            ]);
        let schema = Arc::new(schema);
        let result = arrow_schema_to_schema(&schema).unwrap();

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

        let expected_type: Schema = serde_json::from_str(schema_json).unwrap();
        assert_eq!(result, expected_type);
    }
}
