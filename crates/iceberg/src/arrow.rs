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

use std::collections::HashMap;

use async_stream::try_stream;
use futures::stream::StreamExt;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask, PARQUET_FIELD_ID_META_KEY};
use rust_decimal::prelude::ToPrimitive;

use crate::io::FileIO;
use crate::scan::{ArrowRecordBatchStream, FileScanTask, FileScanTaskStream};
use crate::spec::{visit_schema, SchemaRef, SchemaVisitor};
use crate::Error;
use arrow_schema::Field as ArrowField;
use arrow_schema::FieldRef as ArrowFieldRef;
use arrow_schema::Schema as ArrowSchema;
use arrow_schema::{DataType as ArrowType, TimeUnit};

/// Builder to create ArrowReader
pub struct ArrowReaderBuilder {
    batch_size: Option<usize>,
    file_io: FileIO,
    schema: SchemaRef,
}

impl ArrowReaderBuilder {
    /// Create a new ArrowReaderBuilder
    pub fn new(file_io: FileIO, schema: SchemaRef) -> Self {
        ArrowReaderBuilder {
            batch_size: None,
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

    /// Build the ArrowReader.
    pub fn build(self) -> ArrowReader {
        ArrowReader {
            batch_size: self.batch_size,
            schema: self.schema,
            file_io: self.file_io,
        }
    }
}

/// Reads data from Parquet files
pub struct ArrowReader {
    batch_size: Option<usize>,
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

                let projection_mask = self.get_arrow_projection_mask(&task);

                let parquet_reader = file_io
                    .new_input(task.data_file().file_path())?
                    .reader()
                    .await?;

                let mut batch_stream_builder = ParquetRecordBatchStreamBuilder::new(parquet_reader)
                    .await?
                    .with_projection(projection_mask);

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

    fn get_arrow_projection_mask(&self, _task: &FileScanTask) -> ProjectionMask {
        // TODO: full implementation
        ProjectionMask::all()
    }
}

/// The key of column id in the metadata of arrow field.
pub const COLUMN_ID_META_KEY: &str = "column_id";
/// The key of doc in the metadata of arrow field.
pub const DOC: &str = "doc";

struct ToArrowSchemaConverter;

enum ArrowSchemaOrFieldOrType {
    Schema(ArrowSchema),
    Field(ArrowFieldRef),
    Type(ArrowType),
}

impl SchemaVisitor for ToArrowSchemaConverter {
    type T = ArrowSchemaOrFieldOrType;

    fn schema(&mut self, _schema: &crate::spec::Schema, value: Self::T) -> crate::Result<Self::T> {
        let struct_type = match value {
            ArrowSchemaOrFieldOrType::Type(ArrowType::Struct(fields)) => fields,
            _ => unreachable!(),
        };
        Ok(ArrowSchemaOrFieldOrType::Schema(ArrowSchema::new(
            struct_type,
        )))
    }

    fn field(
        &mut self,
        field: &crate::spec::NestedFieldRef,
        value: Self::T,
    ) -> crate::Result<Self::T> {
        let ty = match value {
            ArrowSchemaOrFieldOrType::Type(ty) => ty,
            _ => unreachable!(),
        };
        let mut metadata = HashMap::new();
        metadata.insert(COLUMN_ID_META_KEY.to_string(), field.id.to_string());
        metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), field.id.to_string());
        if let Some(doc) = &field.doc {
            metadata.insert(DOC.to_string(), doc.clone());
        }
        Ok(ArrowSchemaOrFieldOrType::Field(
            ArrowField::new(field.name.clone(), ty, !field.required)
                .with_metadata(metadata)
                .into(),
        ))
    }

    fn r#struct(
        &mut self,
        _: &crate::spec::StructType,
        results: Vec<Self::T>,
    ) -> crate::Result<Self::T> {
        let fields = results
            .into_iter()
            .map(|result| match result {
                ArrowSchemaOrFieldOrType::Field(field) => field,
                _ => unreachable!(),
            })
            .collect();
        Ok(ArrowSchemaOrFieldOrType::Type(ArrowType::Struct(fields)))
    }

    fn list(&mut self, list: &crate::spec::ListType, value: Self::T) -> crate::Result<Self::T> {
        let field = match self.field(&list.element_field, value)? {
            ArrowSchemaOrFieldOrType::Field(field) => field,
            _ => unreachable!(),
        };
        Ok(ArrowSchemaOrFieldOrType::Type(ArrowType::List(field)))
    }

    fn map(
        &mut self,
        map: &crate::spec::MapType,
        key_value: Self::T,
        value: Self::T,
    ) -> crate::Result<Self::T> {
        let key_field = match self.field(&map.key_field, key_value)? {
            ArrowSchemaOrFieldOrType::Field(field) => field,
            _ => unreachable!(),
        };
        let value_field = match self.field(&map.value_field, value)? {
            ArrowSchemaOrFieldOrType::Field(field) => field,
            _ => unreachable!(),
        };
        let field = ArrowField::new(
            "entries",
            ArrowType::Struct(vec![key_field, value_field].into()),
            map.value_field.required,
        );

        Ok(ArrowSchemaOrFieldOrType::Type(ArrowType::Map(
            field.into(),
            false,
        )))
    }

    fn primitive(&mut self, p: &crate::spec::PrimitiveType) -> crate::Result<Self::T> {
        match p {
            crate::spec::PrimitiveType::Boolean => {
                Ok(ArrowSchemaOrFieldOrType::Type(ArrowType::Boolean))
            }
            crate::spec::PrimitiveType::Int => Ok(ArrowSchemaOrFieldOrType::Type(ArrowType::Int32)),
            crate::spec::PrimitiveType::Long => {
                Ok(ArrowSchemaOrFieldOrType::Type(ArrowType::Int64))
            }
            crate::spec::PrimitiveType::Float => {
                Ok(ArrowSchemaOrFieldOrType::Type(ArrowType::Float32))
            }
            crate::spec::PrimitiveType::Double => {
                Ok(ArrowSchemaOrFieldOrType::Type(ArrowType::Float64))
            }
            crate::spec::PrimitiveType::Decimal { precision, scale } => {
                Ok(ArrowSchemaOrFieldOrType::Type(ArrowType::Decimal128(
                    TryInto::try_into(*precision).map_err(|err| {
                        Error::new(
                            crate::ErrorKind::DataInvalid,
                            "incompatible precision for decimal type convert",
                        )
                        .with_source(err)
                    })?,
                    TryInto::try_into(*scale).map_err(|err| {
                        Error::new(
                            crate::ErrorKind::DataInvalid,
                            "incompatible scale for decimal type convert",
                        )
                        .with_source(err)
                    })?,
                )))
            }
            crate::spec::PrimitiveType::Date => {
                Ok(ArrowSchemaOrFieldOrType::Type(ArrowType::Date32))
            }
            crate::spec::PrimitiveType::Time => Ok(ArrowSchemaOrFieldOrType::Type(
                ArrowType::Time32(TimeUnit::Microsecond),
            )),
            crate::spec::PrimitiveType::Timestamp => Ok(ArrowSchemaOrFieldOrType::Type(
                ArrowType::Timestamp(TimeUnit::Microsecond, None),
            )),
            crate::spec::PrimitiveType::Timestamptz => Ok(ArrowSchemaOrFieldOrType::Type(
                // Timestampz always stored as UTC
                ArrowType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            )),
            crate::spec::PrimitiveType::String => {
                Ok(ArrowSchemaOrFieldOrType::Type(ArrowType::Utf8))
            }
            crate::spec::PrimitiveType::Uuid => Ok(ArrowSchemaOrFieldOrType::Type(
                ArrowType::FixedSizeBinary(16),
            )),
            crate::spec::PrimitiveType::Fixed(len) => Ok(ArrowSchemaOrFieldOrType::Type(
                len.to_i32()
                    .map(ArrowType::FixedSizeBinary)
                    .unwrap_or(ArrowType::LargeBinary),
            )),
            crate::spec::PrimitiveType::Binary => {
                Ok(ArrowSchemaOrFieldOrType::Type(ArrowType::LargeBinary))
            }
        }
    }
}

pub(crate) fn schema_to_arrow_schema(schema: &crate::spec::Schema) -> crate::Result<ArrowSchema> {
    let mut converter = ToArrowSchemaConverter;
    match visit_schema(schema, &mut converter)? {
        ArrowSchemaOrFieldOrType::Schema(schema) => Ok(schema),
        _ => unreachable!(),
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
    use crate::spec::{NestedField, PrimitiveType, Schema, Type};

    use super::*;

    #[test]
    fn test_try_into_arrow_schema() {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField {
                    id: 0,
                    name: "id".to_string(),
                    required: true,
                    field_type: Box::new(Type::Primitive(PrimitiveType::Long)),
                    doc: None,
                    initial_default: None,
                    write_default: None,
                }
                .into(),
                NestedField {
                    id: 1,
                    name: "data".to_string(),
                    required: false,
                    field_type: Box::new(Type::Primitive(PrimitiveType::String)),
                    doc: None,
                    initial_default: None,
                    write_default: None,
                }
                .into(),
            ])
            .build()
            .unwrap();

        let arrow_schema = ArrowSchema::try_from(&schema).unwrap();

        assert_eq!(arrow_schema.fields().len(), 2);
        assert_eq!(arrow_schema.fields()[0].name(), "id");
        assert_eq!(arrow_schema.fields()[0].data_type(), &ArrowType::Int64);
        assert_eq!(arrow_schema.fields()[1].name(), "data");
        assert_eq!(arrow_schema.fields()[1].data_type(), &ArrowType::Utf8);
    }
}
