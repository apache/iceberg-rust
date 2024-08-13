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

//! The module contains the file writer for parquet file format.

use std::collections::HashMap;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;

use arrow_schema::SchemaRef as ArrowSchemaRef;
use bytes::Bytes;
use futures::future::BoxFuture;
use itertools::Itertools;
use parquet::arrow::async_writer::AsyncFileWriter as ArrowAsyncFileWriter;
use parquet::arrow::AsyncArrowWriter;
use parquet::data_type::{
    BoolType, ByteArray, ByteArrayType, DataType as ParquetDataType, DoubleType, FixedLenByteArray,
    FixedLenByteArrayType, FloatType, Int32Type, Int64Type,
};
use parquet::file::properties::WriterProperties;
use parquet::file::statistics::{from_thrift, Statistics, TypedStatistics};
use parquet::format::FileMetaData;
use uuid::Uuid;

use super::location_generator::{FileNameGenerator, LocationGenerator};
use super::track_writer::TrackWriter;
use super::{FileWriter, FileWriterBuilder};
use crate::arrow::DEFAULT_MAP_FIELD_NAME;
use crate::io::{FileIO, FileWrite, OutputFile};
use crate::spec::{
    visit_schema, DataFileBuilder, DataFileFormat, Datum, ListType, MapType, NestedFieldRef,
    PrimitiveLiteral, PrimitiveType, Schema, SchemaRef, SchemaVisitor, StructType, Type,
};
use crate::writer::CurrentFileStatus;
use crate::{Error, ErrorKind, Result};

/// ParquetWriterBuilder is used to builder a [`ParquetWriter`]
#[derive(Clone)]
pub struct ParquetWriterBuilder<T: LocationGenerator, F: FileNameGenerator> {
    props: WriterProperties,
    schema: SchemaRef,

    file_io: FileIO,
    location_generator: T,
    file_name_generator: F,
}

impl<T: LocationGenerator, F: FileNameGenerator> ParquetWriterBuilder<T, F> {
    /// Create a new `ParquetWriterBuilder`
    /// To construct the write result, the schema should contain the `PARQUET_FIELD_ID_META_KEY` metadata for each field.
    pub fn new(
        props: WriterProperties,
        schema: SchemaRef,
        file_io: FileIO,
        location_generator: T,
        file_name_generator: F,
    ) -> Self {
        Self {
            props,
            schema,
            file_io,
            location_generator,
            file_name_generator,
        }
    }
}

impl<T: LocationGenerator, F: FileNameGenerator> FileWriterBuilder for ParquetWriterBuilder<T, F> {
    type R = ParquetWriter;

    async fn build(self) -> crate::Result<Self::R> {
        let arrow_schema: ArrowSchemaRef = Arc::new(self.schema.as_ref().try_into()?);
        let written_size = Arc::new(AtomicI64::new(0));
        let out_file = self.file_io.new_output(
            self.location_generator
                .generate_location(&self.file_name_generator.generate_file_name()),
        )?;
        let inner_writer = TrackWriter::new(out_file.writer().await?, written_size.clone());
        let async_writer = AsyncFileWriter::new(inner_writer);
        let writer =
            AsyncArrowWriter::try_new(async_writer, arrow_schema.clone(), Some(self.props))
                .map_err(|err| {
                    Error::new(ErrorKind::Unexpected, "Failed to build parquet writer.")
                        .with_source(err)
                })?;

        Ok(ParquetWriter {
            schema: self.schema.clone(),
            writer,
            written_size,
            current_row_num: 0,
            out_file,
        })
    }
}

struct IndexByParquetPathName {
    name_to_id: HashMap<String, i32>,

    field_names: Vec<String>,

    field_id: i32,
}

impl IndexByParquetPathName {
    pub fn new() -> Self {
        Self {
            name_to_id: HashMap::new(),
            field_names: Vec::new(),
            field_id: 0,
        }
    }

    pub fn get(&self, name: &str) -> Option<&i32> {
        self.name_to_id.get(name)
    }
}

impl SchemaVisitor for IndexByParquetPathName {
    type T = ();

    fn before_struct_field(&mut self, field: &NestedFieldRef) -> Result<()> {
        self.field_names.push(field.name.to_string());
        self.field_id = field.id;
        Ok(())
    }

    fn after_struct_field(&mut self, _field: &NestedFieldRef) -> Result<()> {
        self.field_names.pop();
        Ok(())
    }

    fn before_list_element(&mut self, field: &NestedFieldRef) -> Result<()> {
        self.field_names.push(format!("list.{}", field.name));
        self.field_id = field.id;
        Ok(())
    }

    fn after_list_element(&mut self, _field: &NestedFieldRef) -> Result<()> {
        self.field_names.pop();
        Ok(())
    }

    fn before_map_key(&mut self, field: &NestedFieldRef) -> Result<()> {
        self.field_names
            .push(format!("{DEFAULT_MAP_FIELD_NAME}.key"));
        self.field_id = field.id;
        Ok(())
    }

    fn after_map_key(&mut self, _field: &NestedFieldRef) -> Result<()> {
        self.field_names.pop();
        Ok(())
    }

    fn before_map_value(&mut self, field: &NestedFieldRef) -> Result<()> {
        self.field_names
            .push(format!("{DEFAULT_MAP_FIELD_NAME}.value"));
        self.field_id = field.id;
        Ok(())
    }

    fn after_map_value(&mut self, _field: &NestedFieldRef) -> Result<()> {
        self.field_names.pop();
        Ok(())
    }

    fn schema(&mut self, _schema: &Schema, _value: Self::T) -> Result<Self::T> {
        Ok(())
    }

    fn field(&mut self, _field: &NestedFieldRef, _value: Self::T) -> Result<Self::T> {
        Ok(())
    }

    fn r#struct(&mut self, _struct: &StructType, _results: Vec<Self::T>) -> Result<Self::T> {
        Ok(())
    }

    fn list(&mut self, _list: &ListType, _value: Self::T) -> Result<Self::T> {
        Ok(())
    }

    fn map(&mut self, _map: &MapType, _key_value: Self::T, _value: Self::T) -> Result<Self::T> {
        Ok(())
    }

    fn primitive(&mut self, _p: &PrimitiveType) -> Result<Self::T> {
        let full_name = self.field_names.iter().map(String::as_str).join(".");
        let field_id = self.field_id;
        if let Some(existing_field_id) = self.name_to_id.get(full_name.as_str()) {
            return Err(Error::new(ErrorKind::DataInvalid, format!("Invalid schema: multiple fields for name {full_name}: {field_id} and {existing_field_id}")));
        } else {
            self.name_to_id.insert(full_name, field_id);
        }

        Ok(())
    }
}

/// `ParquetWriter`` is used to write arrow data into parquet file on storage.
pub struct ParquetWriter {
    schema: SchemaRef,
    out_file: OutputFile,
    writer: AsyncArrowWriter<AsyncFileWriter<TrackWriter>>,
    written_size: Arc<AtomicI64>,
    current_row_num: usize,
}

/// Used to aggregate min and max value of each column.
struct MinMaxColAggregator {
    lower_bounds: HashMap<i32, Datum>,
    upper_bounds: HashMap<i32, Datum>,
    schema: SchemaRef,
}

impl MinMaxColAggregator {
    fn new(schema: SchemaRef) -> Self {
        Self {
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            schema,
        }
    }

    fn update_state<T: ParquetDataType>(
        &mut self,
        field_id: i32,
        state: &TypedStatistics<T>,
        convert_func: impl Fn(<T as ParquetDataType>::T) -> Result<Datum>,
    ) {
        if state.min_is_exact() {
            let val = convert_func(state.min().clone()).unwrap();
            self.lower_bounds
                .entry(field_id)
                .and_modify(|e| {
                    if *e > val {
                        *e = val.clone()
                    }
                })
                .or_insert(val);
        }
        if state.max_is_exact() {
            let val = convert_func(state.max().clone()).unwrap();
            self.upper_bounds
                .entry(field_id)
                .and_modify(|e| {
                    if *e < val {
                        *e = val.clone()
                    }
                })
                .or_insert(val);
        }
    }

    fn update(&mut self, field_id: i32, value: Statistics) -> Result<()> {
        let Some(ty) = self
            .schema
            .field_by_id(field_id)
            .map(|f| f.field_type.as_ref())
        else {
            // Following java implementation: https://github.com/apache/iceberg/blob/29a2c456353a6120b8c882ed2ab544975b168d7b/parquet/src/main/java/org/apache/iceberg/parquet/ParquetUtil.java#L163
            // Ignore the field if it is not in schema.
            return Ok(());
        };
        let Type::Primitive(ty) = ty.clone() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!(
                    "Composed type {} is not supported for min max aggregation.",
                    ty
                ),
            ));
        };

        match (&ty, value) {
            (PrimitiveType::Boolean, Statistics::Boolean(stat)) => {
                let convert_func = |v: bool| Result::<Datum>::Ok(Datum::bool(v));
                self.update_state::<BoolType>(field_id, &stat, convert_func)
            }
            (PrimitiveType::Int, Statistics::Int32(stat)) => {
                let convert_func = |v: i32| Result::<Datum>::Ok(Datum::int(v));
                self.update_state::<Int32Type>(field_id, &stat, convert_func)
            }
            (PrimitiveType::Long, Statistics::Int64(stat)) => {
                let convert_func = |v: i64| Result::<Datum>::Ok(Datum::long(v));
                self.update_state::<Int64Type>(field_id, &stat, convert_func)
            }
            (PrimitiveType::Float, Statistics::Float(stat)) => {
                let convert_func = |v: f32| Result::<Datum>::Ok(Datum::float(v));
                self.update_state::<FloatType>(field_id, &stat, convert_func)
            }
            (PrimitiveType::Double, Statistics::Double(stat)) => {
                let convert_func = |v: f64| Result::<Datum>::Ok(Datum::double(v));
                self.update_state::<DoubleType>(field_id, &stat, convert_func)
            }
            (PrimitiveType::String, Statistics::ByteArray(stat)) => {
                let convert_func = |v: ByteArray| {
                    Result::<Datum>::Ok(Datum::string(
                        String::from_utf8(v.data().to_vec()).unwrap(),
                    ))
                };
                self.update_state::<ByteArrayType>(field_id, &stat, convert_func)
            }
            (PrimitiveType::Binary, Statistics::ByteArray(stat)) => {
                let convert_func =
                    |v: ByteArray| Result::<Datum>::Ok(Datum::binary(v.data().to_vec()));
                self.update_state::<ByteArrayType>(field_id, &stat, convert_func)
            }
            (PrimitiveType::Date, Statistics::Int32(stat)) => {
                let convert_func = |v: i32| Result::<Datum>::Ok(Datum::date(v));
                self.update_state::<Int32Type>(field_id, &stat, convert_func)
            }
            (PrimitiveType::Time, Statistics::Int64(stat)) => {
                let convert_func = |v: i64| Datum::time_micros(v);
                self.update_state::<Int64Type>(field_id, &stat, convert_func)
            }
            (PrimitiveType::Timestamp, Statistics::Int64(stat)) => {
                let convert_func = |v: i64| Result::<Datum>::Ok(Datum::timestamp_micros(v));
                self.update_state::<Int64Type>(field_id, &stat, convert_func)
            }
            (PrimitiveType::Timestamptz, Statistics::Int64(stat)) => {
                let convert_func = |v: i64| Result::<Datum>::Ok(Datum::timestamptz_micros(v));
                self.update_state::<Int64Type>(field_id, &stat, convert_func)
            }
            (
                PrimitiveType::Decimal {
                    precision: _,
                    scale: _,
                },
                Statistics::ByteArray(stat),
            ) => {
                let convert_func = |v: ByteArray| -> Result<Datum> {
                    Result::<Datum>::Ok(Datum::new(
                        ty.clone(),
                        PrimitiveLiteral::Int128(i128::from_le_bytes(v.data().try_into().unwrap())),
                    ))
                };
                self.update_state::<ByteArrayType>(field_id, &stat, convert_func)
            }
            (
                PrimitiveType::Decimal {
                    precision: _,
                    scale: _,
                },
                Statistics::Int32(stat),
            ) => {
                let convert_func = |v: i32| {
                    Result::<Datum>::Ok(Datum::new(
                        ty.clone(),
                        PrimitiveLiteral::Int128(i128::from(v)),
                    ))
                };
                self.update_state::<Int32Type>(field_id, &stat, convert_func)
            }
            (
                PrimitiveType::Decimal {
                    precision: _,
                    scale: _,
                },
                Statistics::Int64(stat),
            ) => {
                let convert_func = |v: i64| {
                    Result::<Datum>::Ok(Datum::new(
                        ty.clone(),
                        PrimitiveLiteral::Int128(i128::from(v)),
                    ))
                };
                self.update_state::<Int64Type>(field_id, &stat, convert_func)
            }
            (PrimitiveType::Uuid, Statistics::FixedLenByteArray(stat)) => {
                let convert_func = |v: FixedLenByteArray| {
                    if v.len() != 16 {
                        return Err(Error::new(
                            ErrorKind::Unexpected,
                            "Invalid length of uuid bytes.",
                        ));
                    }
                    Ok(Datum::uuid(Uuid::from_bytes(
                        v.data()[..16].try_into().unwrap(),
                    )))
                };
                self.update_state::<FixedLenByteArrayType>(field_id, &stat, convert_func)
            }
            (PrimitiveType::Fixed(len), Statistics::FixedLenByteArray(stat)) => {
                let convert_func = |v: FixedLenByteArray| {
                    if v.len() != *len as usize {
                        return Err(Error::new(
                            ErrorKind::Unexpected,
                            "Invalid length of fixed bytes.",
                        ));
                    }
                    Ok(Datum::fixed(v.data().to_vec()))
                };
                self.update_state::<FixedLenByteArrayType>(field_id, &stat, convert_func)
            }
            (ty, value) => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("Statistics {} is not match with field type {}.", value, ty),
                ))
            }
        }
        Ok(())
    }

    fn produce(self) -> (HashMap<i32, Datum>, HashMap<i32, Datum>) {
        (self.lower_bounds, self.upper_bounds)
    }
}

impl ParquetWriter {
    fn to_data_file_builder(
        schema: SchemaRef,
        metadata: FileMetaData,
        written_size: usize,
        file_path: String,
    ) -> Result<DataFileBuilder> {
        let index_by_parquet_path = {
            let mut visitor = IndexByParquetPathName::new();
            visit_schema(&schema, &mut visitor)?;
            visitor
        };

        let (column_sizes, value_counts, null_value_counts, (lower_bounds, upper_bounds)) = {
            let mut per_col_size: HashMap<i32, u64> = HashMap::new();
            let mut per_col_val_num: HashMap<i32, u64> = HashMap::new();
            let mut per_col_null_val_num: HashMap<i32, u64> = HashMap::new();
            let mut min_max_agg = MinMaxColAggregator::new(schema);

            for row_group in &metadata.row_groups {
                for column_chunk in row_group.columns.iter() {
                    let Some(column_chunk_metadata) = &column_chunk.meta_data else {
                        continue;
                    };
                    let physical_type = column_chunk_metadata.type_;
                    let Some(&field_id) =
                        index_by_parquet_path.get(&column_chunk_metadata.path_in_schema.join("."))
                    else {
                        // Following java implementation: https://github.com/apache/iceberg/blob/29a2c456353a6120b8c882ed2ab544975b168d7b/parquet/src/main/java/org/apache/iceberg/parquet/ParquetUtil.java#L163
                        // Ignore the field if it is not in schema.
                        continue;
                    };
                    *per_col_size.entry(field_id).or_insert(0) +=
                        column_chunk_metadata.total_compressed_size as u64;
                    *per_col_val_num.entry(field_id).or_insert(0) +=
                        column_chunk_metadata.num_values as u64;
                    if let Some(null_count) = column_chunk_metadata
                        .statistics
                        .as_ref()
                        .and_then(|s| s.null_count)
                    {
                        *per_col_null_val_num.entry(field_id).or_insert(0_u64) += null_count as u64;
                    }
                    if let Some(statistics) = &column_chunk_metadata.statistics {
                        min_max_agg.update(
                            field_id,
                            from_thrift(physical_type.try_into()?, Some(statistics.clone()))?
                                .unwrap(),
                        )?;
                    }
                }
            }

            (
                per_col_size,
                per_col_val_num,
                per_col_null_val_num,
                min_max_agg.produce(),
            )
        };

        let mut builder = DataFileBuilder::default();
        builder
            .file_path(file_path)
            .file_format(DataFileFormat::Parquet)
            .record_count(metadata.num_rows as u64)
            .file_size_in_bytes(written_size as u64)
            .column_sizes(column_sizes)
            .value_counts(value_counts)
            .null_value_counts(null_value_counts)
            .lower_bounds(lower_bounds)
            .upper_bounds(upper_bounds)
            // # TODO(#417)
            // - nan_value_counts
            // - distinct_counts
            .key_metadata(metadata.footer_signing_key_metadata.unwrap_or_default())
            .split_offsets(
                metadata
                    .row_groups
                    .iter()
                    .filter_map(|group| group.file_offset)
                    .collect(),
            );
        Ok(builder)
    }
}

impl FileWriter for ParquetWriter {
    async fn write(&mut self, batch: &arrow_array::RecordBatch) -> crate::Result<()> {
        self.current_row_num += batch.num_rows();
        self.writer.write(batch).await.map_err(|err| {
            Error::new(
                ErrorKind::Unexpected,
                "Failed to write using parquet writer.",
            )
            .with_source(err)
        })?;
        Ok(())
    }

    async fn close(self) -> crate::Result<Vec<crate::spec::DataFileBuilder>> {
        let metadata = self.writer.close().await.map_err(|err| {
            Error::new(ErrorKind::Unexpected, "Failed to close parquet writer.").with_source(err)
        })?;

        let written_size = self.written_size.load(std::sync::atomic::Ordering::Relaxed);

        Ok(vec![Self::to_data_file_builder(
            self.schema,
            metadata,
            written_size as usize,
            self.out_file.location().to_string(),
        )?])
    }
}

impl CurrentFileStatus for ParquetWriter {
    fn current_file_path(&self) -> String {
        self.out_file.location().to_string()
    }

    fn current_row_num(&self) -> usize {
        self.current_row_num
    }

    fn current_written_size(&self) -> usize {
        self.written_size.load(std::sync::atomic::Ordering::Relaxed) as usize
    }
}

/// AsyncFileWriter is a wrapper of FileWrite to make it compatible with tokio::io::AsyncWrite.
///
/// # NOTES
///
/// We keep this wrapper been used inside only.
struct AsyncFileWriter<W: FileWrite>(W);

impl<W: FileWrite> AsyncFileWriter<W> {
    /// Create a new `AsyncFileWriter` with the given writer.
    pub fn new(writer: W) -> Self {
        Self(writer)
    }
}

impl<W: FileWrite> ArrowAsyncFileWriter for AsyncFileWriter<W> {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async {
            self.0
                .write(bs)
                .await
                .map_err(|err| parquet::errors::ParquetError::External(Box::new(err)))
        })
    }

    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async {
            self.0
                .close()
                .await
                .map_err(|err| parquet::errors::ParquetError::External(Box::new(err)))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use anyhow::Result;
    use arrow_array::types::Int64Type;
    use arrow_array::{
        ArrayRef, BooleanArray, Int32Array, Int64Array, ListArray, RecordBatch, StructArray,
    };
    use arrow_schema::{DataType, SchemaRef as ArrowSchemaRef};
    use arrow_select::concat::concat_batches;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use tempfile::TempDir;

    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::{PrimitiveLiteral, Struct, *};
    use crate::writer::file_writer::location_generator::test::MockLocationGenerator;
    use crate::writer::file_writer::location_generator::DefaultFileNameGenerator;
    use crate::writer::tests::check_parquet_data_file;

    fn schema_for_all_type() -> Schema {
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::optional(0, "boolean", Type::Primitive(PrimitiveType::Boolean)).into(),
                NestedField::optional(1, "int", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "long", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(3, "float", Type::Primitive(PrimitiveType::Float)).into(),
                NestedField::optional(4, "double", Type::Primitive(PrimitiveType::Double)).into(),
                NestedField::optional(5, "string", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(6, "binary", Type::Primitive(PrimitiveType::Binary)).into(),
                NestedField::optional(7, "date", Type::Primitive(PrimitiveType::Date)).into(),
                NestedField::optional(8, "time", Type::Primitive(PrimitiveType::Time)).into(),
                NestedField::optional(9, "timestamp", Type::Primitive(PrimitiveType::Timestamp))
                    .into(),
                NestedField::optional(
                    10,
                    "timestamptz",
                    Type::Primitive(PrimitiveType::Timestamptz),
                )
                .into(),
                NestedField::optional(
                    11,
                    "decimal",
                    Type::Primitive(PrimitiveType::Decimal {
                        precision: 10,
                        scale: 5,
                    }),
                )
                .into(),
                NestedField::optional(12, "uuid", Type::Primitive(PrimitiveType::Uuid)).into(),
                NestedField::optional(13, "fixed", Type::Primitive(PrimitiveType::Fixed(10)))
                    .into(),
            ])
            .build()
            .unwrap()
    }

    fn nested_schema_for_test() -> Schema {
        // Int, Struct(Int,Int), String, List(Int), Struct(Struct(Int)), Map(String, List(Int))
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(0, "col0", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(
                    1,
                    "col1",
                    Type::Struct(StructType::new(vec![
                        NestedField::required(5, "col_1_5", Type::Primitive(PrimitiveType::Long))
                            .into(),
                        NestedField::required(6, "col_1_6", Type::Primitive(PrimitiveType::Long))
                            .into(),
                    ])),
                )
                .into(),
                NestedField::required(2, "col2", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(
                    3,
                    "col3",
                    Type::List(ListType::new(
                        NestedField::required(7, "element", Type::Primitive(PrimitiveType::Long))
                            .into(),
                    )),
                )
                .into(),
                NestedField::required(
                    4,
                    "col4",
                    Type::Struct(StructType::new(vec![NestedField::required(
                        8,
                        "col_4_8",
                        Type::Struct(StructType::new(vec![NestedField::required(
                            9,
                            "col_4_8_9",
                            Type::Primitive(PrimitiveType::Long),
                        )
                        .into()])),
                    )
                    .into()])),
                )
                .into(),
                NestedField::required(
                    10,
                    "col5",
                    Type::Map(MapType::new(
                        NestedField::required(11, "key", Type::Primitive(PrimitiveType::String))
                            .into(),
                        NestedField::required(
                            12,
                            "value",
                            Type::List(ListType::new(
                                NestedField::required(
                                    13,
                                    "item",
                                    Type::Primitive(PrimitiveType::Long),
                                )
                                .into(),
                            )),
                        )
                        .into(),
                    )),
                )
                .into(),
            ])
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_index_by_parquet_path() {
        let expect = HashMap::from([
            ("col0".to_string(), 0),
            ("col1.col_1_5".to_string(), 5),
            ("col1.col_1_6".to_string(), 6),
            ("col2".to_string(), 2),
            ("col3.list.element".to_string(), 7),
            ("col4.col_4_8.col_4_8_9".to_string(), 9),
            ("col5.key_value.key".to_string(), 11),
            ("col5.key_value.value.list.item".to_string(), 13),
        ]);
        let mut visitor = IndexByParquetPathName::new();
        visit_schema(&nested_schema_for_test(), &mut visitor).unwrap();
        assert_eq!(visitor.name_to_id, expect);
    }

    #[tokio::test]
    async fn test_parquet_writer() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let loccation_gen =
            MockLocationGenerator::new(temp_dir.path().to_str().unwrap().to_string());
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // prepare data
        let schema = {
            let fields = vec![
                arrow_schema::Field::new("col", arrow_schema::DataType::Int64, true).with_metadata(
                    HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "0".to_string())]),
                ),
            ];
            Arc::new(arrow_schema::Schema::new(fields))
        };
        let col = Arc::new(Int64Array::from_iter_values(0..1024)) as ArrayRef;
        let null_col = Arc::new(Int64Array::new_null(1024)) as ArrayRef;
        let to_write = RecordBatch::try_new(schema.clone(), vec![col]).unwrap();
        let to_write_null = RecordBatch::try_new(schema.clone(), vec![null_col]).unwrap();

        // write data
        let mut pw = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::new(to_write.schema().as_ref().try_into().unwrap()),
            file_io.clone(),
            loccation_gen,
            file_name_gen,
        )
        .build()
        .await?;
        pw.write(&to_write).await?;
        pw.write(&to_write_null).await?;
        let res = pw.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res
            .into_iter()
            .next()
            .unwrap()
            // Put dummy field for build successfully.
            .content(crate::spec::DataContentType::Data)
            .partition(Struct::empty())
            .build()
            .unwrap();

        // check data file
        assert_eq!(data_file.record_count(), 2048);
        assert_eq!(*data_file.value_counts(), HashMap::from([(0, 2048)]));
        assert_eq!(
            *data_file.lower_bounds(),
            HashMap::from([(0, Datum::long(0))])
        );
        assert_eq!(
            *data_file.upper_bounds(),
            HashMap::from([(0, Datum::long(1023))])
        );
        assert_eq!(*data_file.null_value_counts(), HashMap::from([(0, 1024)]));

        // check the written file
        let expect_batch = concat_batches(&schema, vec![&to_write, &to_write_null]).unwrap();
        check_parquet_data_file(&file_io, &data_file, &expect_batch).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_parquet_writer_with_complex_schema() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen =
            MockLocationGenerator::new(temp_dir.path().to_str().unwrap().to_string());
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // prepare data
        let schema = nested_schema_for_test();
        let arrow_schema: ArrowSchemaRef = Arc::new((&schema).try_into().unwrap());
        let col0 = Arc::new(Int64Array::from_iter_values(0..1024)) as ArrayRef;
        let col1 = Arc::new(StructArray::new(
            {
                if let DataType::Struct(fields) = arrow_schema.field(1).data_type() {
                    fields.clone()
                } else {
                    unreachable!()
                }
            },
            vec![
                Arc::new(Int64Array::from_iter_values(0..1024)),
                Arc::new(Int64Array::from_iter_values(0..1024)),
            ],
            None,
        ));
        let col2 = Arc::new(arrow_array::StringArray::from_iter_values(
            (0..1024).map(|n| n.to_string()),
        )) as ArrayRef;
        let col3 = Arc::new({
            let list_parts = arrow_array::ListArray::from_iter_primitive::<Int64Type, _, _>(
                (0..1024).map(|n| Some(vec![Some(n)])),
            )
            .into_parts();
            arrow_array::ListArray::new(
                {
                    if let DataType::List(field) = arrow_schema.field(3).data_type() {
                        field.clone()
                    } else {
                        unreachable!()
                    }
                },
                list_parts.1,
                list_parts.2,
                list_parts.3,
            )
        }) as ArrayRef;
        let col4 = Arc::new(StructArray::new(
            {
                if let DataType::Struct(fields) = arrow_schema.field(4).data_type() {
                    fields.clone()
                } else {
                    unreachable!()
                }
            },
            vec![Arc::new(StructArray::new(
                {
                    if let DataType::Struct(fields) = arrow_schema.field(4).data_type() {
                        if let DataType::Struct(fields) = fields[0].data_type() {
                            fields.clone()
                        } else {
                            unreachable!()
                        }
                    } else {
                        unreachable!()
                    }
                },
                vec![Arc::new(Int64Array::from_iter_values(0..1024))],
                None,
            ))],
            None,
        ));
        let col5 = Arc::new({
            let mut map_array_builder = arrow_array::builder::MapBuilder::new(
                None,
                arrow_array::builder::StringBuilder::new(),
                arrow_array::builder::ListBuilder::new(arrow_array::builder::PrimitiveBuilder::<
                    Int64Type,
                >::new()),
            );
            for i in 0..1024 {
                map_array_builder.keys().append_value(i.to_string());
                map_array_builder
                    .values()
                    .append_value(vec![Some(i as i64); i + 1]);
                map_array_builder.append(true)?;
            }
            let (_, offset_buffer, struct_array, null_buffer, ordered) =
                map_array_builder.finish().into_parts();
            let struct_array = {
                let (_, mut arrays, nulls) = struct_array.into_parts();
                let list_array = {
                    let list_array = arrays[1]
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .unwrap()
                        .clone();
                    let (_, offsets, array, nulls) = list_array.into_parts();
                    let list_field = {
                        if let DataType::Map(map_field, _) = arrow_schema.field(5).data_type() {
                            if let DataType::Struct(fields) = map_field.data_type() {
                                if let DataType::List(list_field) = fields[1].data_type() {
                                    list_field.clone()
                                } else {
                                    unreachable!()
                                }
                            } else {
                                unreachable!()
                            }
                        } else {
                            unreachable!()
                        }
                    };
                    ListArray::new(list_field, offsets, array, nulls)
                };
                arrays[1] = Arc::new(list_array) as ArrayRef;
                StructArray::new(
                    {
                        if let DataType::Map(map_field, _) = arrow_schema.field(5).data_type() {
                            if let DataType::Struct(fields) = map_field.data_type() {
                                fields.clone()
                            } else {
                                unreachable!()
                            }
                        } else {
                            unreachable!()
                        }
                    },
                    arrays,
                    nulls,
                )
            };
            arrow_array::MapArray::new(
                {
                    if let DataType::Map(map_field, _) = arrow_schema.field(5).data_type() {
                        map_field.clone()
                    } else {
                        unreachable!()
                    }
                },
                offset_buffer,
                struct_array,
                null_buffer,
                ordered,
            )
        }) as ArrayRef;
        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![
            col0, col1, col2, col3, col4, col5,
        ])
        .unwrap();

        // write data
        let mut pw = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::new(schema),
            file_io.clone(),
            location_gen,
            file_name_gen,
        )
        .build()
        .await?;
        pw.write(&to_write).await?;
        let res = pw.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res
            .into_iter()
            .next()
            .unwrap()
            // Put dummy field for build successfully.
            .content(crate::spec::DataContentType::Data)
            .partition(Struct::empty())
            .build()
            .unwrap();

        // check data file
        assert_eq!(data_file.record_count(), 1024);
        assert_eq!(
            *data_file.value_counts(),
            HashMap::from([
                (0, 1024),
                (5, 1024),
                (6, 1024),
                (2, 1024),
                (7, 1024),
                (9, 1024),
                (11, 1024),
                (13, (1..1025).sum()),
            ])
        );
        assert_eq!(
            *data_file.lower_bounds(),
            HashMap::from([
                (0, Datum::long(0)),
                (5, Datum::long(0)),
                (6, Datum::long(0)),
                (2, Datum::string("0")),
                (7, Datum::long(0)),
                (9, Datum::long(0)),
                (11, Datum::string("0")),
                (13, Datum::long(0))
            ])
        );
        assert_eq!(
            *data_file.upper_bounds(),
            HashMap::from([
                (0, Datum::long(1023)),
                (5, Datum::long(1023)),
                (6, Datum::long(1023)),
                (2, Datum::string("999")),
                (7, Datum::long(1023)),
                (9, Datum::long(1023)),
                (11, Datum::string("999")),
                (13, Datum::long(1023))
            ])
        );

        // check the written file
        check_parquet_data_file(&file_io, &data_file, &to_write).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_all_type_for_write() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let loccation_gen =
            MockLocationGenerator::new(temp_dir.path().to_str().unwrap().to_string());
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // prepare data
        // generate iceberg schema for all type
        let schema = schema_for_all_type();
        let arrow_schema: ArrowSchemaRef = Arc::new((&schema).try_into().unwrap());
        let col0 = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            Some(true),
        ])) as ArrayRef;
        let col1 = Arc::new(Int32Array::from(vec![Some(1), Some(2), None, Some(4)])) as ArrayRef;
        let col2 = Arc::new(Int64Array::from(vec![Some(1), Some(2), None, Some(4)])) as ArrayRef;
        let col3 = Arc::new(arrow_array::Float32Array::from(vec![
            Some(0.5),
            Some(2.0),
            None,
            Some(3.5),
        ])) as ArrayRef;
        let col4 = Arc::new(arrow_array::Float64Array::from(vec![
            Some(0.5),
            Some(2.0),
            None,
            Some(3.5),
        ])) as ArrayRef;
        let col5 = Arc::new(arrow_array::StringArray::from(vec![
            Some("a"),
            Some("b"),
            None,
            Some("d"),
        ])) as ArrayRef;
        let col6 = Arc::new(arrow_array::LargeBinaryArray::from_opt_vec(vec![
            Some(b"one"),
            None,
            Some(b""),
            Some(b"zzzz"),
        ])) as ArrayRef;
        let col7 = Arc::new(arrow_array::Date32Array::from(vec![
            Some(0),
            Some(1),
            None,
            Some(3),
        ])) as ArrayRef;
        let col8 = Arc::new(arrow_array::Time64MicrosecondArray::from(vec![
            Some(0),
            Some(1),
            None,
            Some(3),
        ])) as ArrayRef;
        let col9 = Arc::new(arrow_array::TimestampMicrosecondArray::from(vec![
            Some(0),
            Some(1),
            None,
            Some(3),
        ])) as ArrayRef;
        let col10 = Arc::new(
            arrow_array::TimestampMicrosecondArray::from(vec![Some(0), Some(1), None, Some(3)])
                .with_timezone_utc(),
        ) as ArrayRef;
        let col11 = Arc::new(
            arrow_array::Decimal128Array::from(vec![Some(1), Some(2), None, Some(100)])
                .with_precision_and_scale(10, 5)
                .unwrap(),
        ) as ArrayRef;
        let col12 = Arc::new(
            arrow_array::FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                vec![
                    Some(Uuid::from_u128(0).as_bytes().to_vec()),
                    Some(Uuid::from_u128(1).as_bytes().to_vec()),
                    None,
                    Some(Uuid::from_u128(3).as_bytes().to_vec()),
                ]
                .into_iter(),
                16,
            )
            .unwrap(),
        ) as ArrayRef;
        let col13 = Arc::new(
            arrow_array::FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                vec![
                    Some(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
                    Some(vec![11, 12, 13, 14, 15, 16, 17, 18, 19, 20]),
                    None,
                    Some(vec![21, 22, 23, 24, 25, 26, 27, 28, 29, 30]),
                ]
                .into_iter(),
                10,
            )
            .unwrap(),
        ) as ArrayRef;
        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![
            col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13,
        ])
        .unwrap();

        // write data
        let mut pw = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::new(schema),
            file_io.clone(),
            loccation_gen,
            file_name_gen,
        )
        .build()
        .await?;
        pw.write(&to_write).await?;
        let res = pw.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res
            .into_iter()
            .next()
            .unwrap()
            // Put dummy field for build successfully.
            .content(crate::spec::DataContentType::Data)
            .partition(Struct::empty())
            .build()
            .unwrap();

        // check data file
        assert_eq!(data_file.record_count(), 4);
        assert!(data_file.value_counts().iter().all(|(_, &v)| { v == 4 }));
        assert!(data_file
            .null_value_counts()
            .iter()
            .all(|(_, &v)| { v == 1 }));
        assert_eq!(
            *data_file.lower_bounds(),
            HashMap::from([
                (0, Datum::bool(false)),
                (1, Datum::int(1)),
                (2, Datum::long(1)),
                (3, Datum::float(0.5)),
                (4, Datum::double(0.5)),
                (5, Datum::string("a")),
                (6, Datum::binary(vec![])),
                (7, Datum::date(0)),
                (8, Datum::time_micros(0).unwrap()),
                (9, Datum::timestamp_micros(0)),
                (10, Datum::timestamptz_micros(0)),
                (
                    11,
                    Datum::new(
                        PrimitiveType::Decimal {
                            precision: 10,
                            scale: 5
                        },
                        PrimitiveLiteral::Int128(1)
                    )
                ),
                (12, Datum::uuid(Uuid::from_u128(0))),
                (13, Datum::fixed(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10])),
                (12, Datum::uuid(Uuid::from_u128(0))),
                (13, Datum::fixed(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10])),
            ])
        );
        assert_eq!(
            *data_file.upper_bounds(),
            HashMap::from([
                (0, Datum::bool(true)),
                (1, Datum::int(4)),
                (2, Datum::long(4)),
                (3, Datum::float(3.5)),
                (4, Datum::double(3.5)),
                (5, Datum::string("d")),
                (6, Datum::binary(vec![122, 122, 122, 122])),
                (7, Datum::date(3)),
                (8, Datum::time_micros(3).unwrap()),
                (9, Datum::timestamp_micros(3)),
                (10, Datum::timestamptz_micros(3)),
                (
                    11,
                    Datum::new(
                        PrimitiveType::Decimal {
                            precision: 10,
                            scale: 5
                        },
                        PrimitiveLiteral::Int128(100)
                    )
                ),
                (12, Datum::uuid(Uuid::from_u128(3))),
                (
                    13,
                    Datum::fixed(vec![21, 22, 23, 24, 25, 26, 27, 28, 29, 30])
                ),
            ])
        );

        // check the written file
        check_parquet_data_file(&file_io, &data_file, &to_write).await;

        Ok(())
    }
}
