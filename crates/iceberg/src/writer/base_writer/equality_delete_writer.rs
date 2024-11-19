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

//! This module provide `EqualityDeleteWriter`.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StructArray};
use arrow_schema::{DataType, FieldRef, Fields, Schema, SchemaRef};
use itertools::Itertools;

use crate::spec::{DataFile, Struct};
use crate::writer::file_writer::{FileWriter, FileWriterBuilder};
use crate::writer::{IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind, Result};

/// Builder for `EqualityDeleteWriter`.
#[derive(Clone)]
pub struct EqualityDeleteFileWriterBuilder<B: FileWriterBuilder> {
    inner: B,
}

impl<B: FileWriterBuilder> EqualityDeleteFileWriterBuilder<B> {
    /// Create a new `EqualityDeleteFileWriterBuilder` using a `FileWriterBuilder`.
    pub fn new(inner: B) -> Self {
        Self { inner }
    }
}

/// Config for `EqualityDeleteWriter`.
pub struct EqualityDeleteWriterConfig {
    equality_ids: Vec<usize>,
    projector: ArrowFieldProjector,
    partition_value: Struct,
}

impl EqualityDeleteWriterConfig {
    /// Create a new `DataFileWriterConfig` with equality ids.
    pub fn new(
        equality_ids: Vec<usize>,
        projector: ArrowFieldProjector,
        partition_value: Option<Struct>,
    ) -> Self {
        Self {
            equality_ids,
            projector,
            partition_value: partition_value.unwrap_or(Struct::empty()),
        }
    }
}

#[async_trait::async_trait]
impl<B: FileWriterBuilder> IcebergWriterBuilder for EqualityDeleteFileWriterBuilder<B> {
    type R = EqualityDeleteFileWriter<B>;
    type C = EqualityDeleteWriterConfig;

    async fn build(self, config: Self::C) -> Result<Self::R> {
        Ok(EqualityDeleteFileWriter {
            inner_writer: Some(self.inner.clone().build().await?),
            projector: config.projector,
            equality_ids: config.equality_ids,
            partition_value: config.partition_value,
        })
    }
}

/// A writer write data
pub struct EqualityDeleteFileWriter<B: FileWriterBuilder> {
    inner_writer: Option<B::R>,
    projector: ArrowFieldProjector,
    equality_ids: Vec<usize>,
    partition_value: Struct,
}

#[async_trait::async_trait]
impl<B: FileWriterBuilder> IcebergWriter for EqualityDeleteFileWriter<B> {
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        let batch = self.projector.project_bacth(batch)?;
        if let Some(writer) = self.inner_writer.as_mut() {
            writer.write(&batch).await
        } else {
            Err(Error::new(
                ErrorKind::Unexpected,
                "Equality delete inner writer does not exist",
            ))
        }
    }

    async fn close(&mut self) -> Result<Vec<DataFile>> {
        if let Some(writer) = self.inner_writer.take() {
            Ok(writer
                .close()
                .await?
                .into_iter()
                .map(|mut res| {
                    res.content(crate::spec::DataContentType::EqualityDeletes);
                    res.equality_ids(self.equality_ids.iter().map(|id| *id as i32).collect_vec());
                    res.partition(self.partition_value.clone());
                    res.build().expect("msg")
                })
                .collect_vec())
        } else {
            Err(Error::new(
                ErrorKind::Unexpected,
                "Equality delete inner writer does not exist",
            ))
        }
    }
}

/// Help to project specific field from `RecordBatch`` according to the column id.
#[derive(Clone)]
pub struct ArrowFieldProjector {
    index_vec_vec: Vec<Vec<usize>>,
    projected_schema: SchemaRef,
}

impl ArrowFieldProjector {
    /// Init FieldProjector
    pub fn new(
        original_schema: SchemaRef,
        column_ids: &[usize],
        column_id_meta_key: &str,
    ) -> Result<Self> {
        let mut index_vec_vec = Vec::with_capacity(column_ids.len());
        let mut fields = Vec::with_capacity(column_ids.len());
        for &id in column_ids {
            let mut index_vec = vec![];
            if let Ok(field) = Self::fetch_column_index(
                original_schema.fields(),
                &mut index_vec,
                id as i64,
                column_id_meta_key,
            ) {
                fields.push(field.clone());
                index_vec_vec.push(index_vec);
            } else {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Can't find source column id or column data type invalid: {}",
                        id
                    ),
                ));
            }
        }
        let delete_arrow_schema = Arc::new(Schema::new(fields));
        Ok(Self {
            index_vec_vec,
            projected_schema: delete_arrow_schema,
        })
    }

    /// Return the reference of projected schema
    pub fn projected_schema_ref(&self) -> &SchemaRef {
        &self.projected_schema
    }

    /// Do projection with record batch
    pub fn project_bacth(&self, batch: RecordBatch) -> Result<RecordBatch> {
        RecordBatch::try_new(
            self.projected_schema.clone(),
            self.project_column(batch.columns())?,
        )
        .map_err(|err| Error::new(ErrorKind::DataInvalid, format!("{err}")))
    }

    /// Do projection with columns
    pub fn project_column(&self, batch: &[ArrayRef]) -> Result<Vec<ArrayRef>> {
        self.index_vec_vec
            .iter()
            .map(|index_vec| Self::get_column_by_index_vec(batch, index_vec))
            .collect::<Result<Vec<_>>>()
    }

    fn fetch_column_index(
        fields: &Fields,
        index_vec: &mut Vec<usize>,
        col_id: i64,
        column_id_meta_key: &str,
    ) -> Result<FieldRef> {
        for (pos, field) in fields.iter().enumerate() {
            match field.data_type() {
                DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "Delete column data type cannot be float or double",
                    ));
                }
                _ => {
                    let id: i64 = field
                        .metadata()
                        .get(column_id_meta_key)
                        .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "column_id must be set"))?
                        .parse::<i64>()
                        .map_err(|_| {
                            Error::new(ErrorKind::DataInvalid, "column_id must be parsable as i64")
                        })?;
                    if col_id == id {
                        index_vec.push(pos);
                        return Ok(field.clone());
                    }
                    if let DataType::Struct(inner) = field.data_type() {
                        let res =
                            Self::fetch_column_index(inner, index_vec, col_id, column_id_meta_key);
                        if !index_vec.is_empty() {
                            index_vec.push(pos);
                            return res;
                        }
                    }
                }
            }
        }
        Err(Error::new(
            ErrorKind::DataInvalid,
            "Column id not found in fields",
        ))
    }

    fn get_column_by_index_vec(batch: &[ArrayRef], index_vec: &[usize]) -> Result<ArrayRef> {
        let mut rev_iterator = index_vec.iter().rev();
        let mut array = batch[*rev_iterator.next().unwrap()].clone();
        for idx in rev_iterator {
            array = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or(Error::new(
                    ErrorKind::Unexpected,
                    "Cannot convert Array to StructArray",
                ))?
                .column(*idx)
                .clone();
        }
        Ok(array)
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::types::Int32Type;
    use arrow_array::{ArrayRef, Int32Array, RecordBatch, StructArray};
    use arrow_schema::DataType;
    use arrow_select::concat::concat_batches;
    use itertools::Itertools;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use crate::arrow::{arrow_schema_to_schema, schema_to_arrow_schema};
    use crate::io::{FileIO, FileIOBuilder};
    use crate::spec::{
        DataFile, DataFileFormat, ListType, NestedField, PrimitiveType, Schema, StructType, Type,
    };
    use crate::writer::base_writer::equality_delete_writer::{
        ArrowFieldProjector, EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig,
    };
    use crate::writer::file_writer::location_generator::test::MockLocationGenerator;
    use crate::writer::file_writer::location_generator::DefaultFileNameGenerator;
    use crate::writer::file_writer::ParquetWriterBuilder;
    use crate::writer::{IcebergWriter, IcebergWriterBuilder};

    async fn check_parquet_data_file_with_equality_delete_write(
        file_io: &FileIO,
        data_file: &DataFile,
        batch: &RecordBatch,
    ) {
        assert_eq!(data_file.file_format, DataFileFormat::Parquet);

        // read the written file
        let input_file = file_io.new_input(data_file.file_path.clone()).unwrap();
        // read the written file
        let input_content = input_file.read().await.unwrap();
        let reader_builder =
            ParquetRecordBatchReaderBuilder::try_new(input_content.clone()).unwrap();
        let metadata = reader_builder.metadata().clone();

        // check data
        let reader = reader_builder.build().unwrap();
        let batches = reader.map(|batch| batch.unwrap()).collect::<Vec<_>>();
        let res = concat_batches(&batch.schema(), &batches).unwrap();
        assert_eq!(*batch, res);

        // check metadata
        let expect_column_num = batch.num_columns();

        assert_eq!(
            data_file.record_count,
            metadata
                .row_groups()
                .iter()
                .map(|group| group.num_rows())
                .sum::<i64>() as u64
        );

        assert_eq!(data_file.file_size_in_bytes, input_content.len() as u64);

        assert_eq!(data_file.column_sizes.len(), expect_column_num);

        for (index, id) in data_file.column_sizes().keys().sorted().enumerate() {
            metadata
                .row_groups()
                .iter()
                .map(|group| group.columns())
                .for_each(|column| {
                    assert_eq!(
                        *data_file.column_sizes.get(id).unwrap() as i64,
                        column.get(index).unwrap().compressed_size()
                    );
                });
        }

        assert_eq!(data_file.value_counts.len(), expect_column_num);
        data_file.value_counts.iter().for_each(|(_, &v)| {
            let expect = metadata
                .row_groups()
                .iter()
                .map(|group| group.num_rows())
                .sum::<i64>() as u64;
            assert_eq!(v, expect);
        });

        for (index, id) in data_file.null_value_counts().keys().enumerate() {
            let expect = batch.column(index).null_count() as u64;
            assert_eq!(*data_file.null_value_counts.get(id).unwrap(), expect);
        }

        assert_eq!(data_file.split_offsets.len(), metadata.num_row_groups());
        data_file
            .split_offsets
            .iter()
            .enumerate()
            .for_each(|(i, &v)| {
                let expect = metadata.row_groups()[i].file_offset().unwrap();
                assert_eq!(v, expect);
            });
    }

    #[tokio::test]
    async fn test_equality_delete_writer() -> Result<(), anyhow::Error> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen =
            MockLocationGenerator::new(temp_dir.path().to_str().unwrap().to_string());
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // prepare data
        // Int, Struct(Int), String, List(Int), Struct(Struct(Int))
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(0, "col0", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(
                    1,
                    "col1",
                    Type::Struct(StructType::new(vec![NestedField::required(
                        5,
                        "sub_col",
                        Type::Primitive(PrimitiveType::Int),
                    )
                    .into()])),
                )
                .into(),
                NestedField::required(2, "col2", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(
                    3,
                    "col3",
                    Type::List(ListType::new(
                        NestedField::required(6, "element", Type::Primitive(PrimitiveType::Int))
                            .into(),
                    )),
                )
                .into(),
                NestedField::required(
                    4,
                    "col4",
                    Type::Struct(StructType::new(vec![NestedField::required(
                        7,
                        "sub_col",
                        Type::Struct(StructType::new(vec![NestedField::required(
                            8,
                            "sub_sub_col",
                            Type::Primitive(PrimitiveType::Int),
                        )
                        .into()])),
                    )
                    .into()])),
                )
                .into(),
            ])
            .build()
            .unwrap();
        let arrow_schema = Arc::new(schema_to_arrow_schema(&schema).unwrap());
        let col0 = Arc::new(Int32Array::from_iter_values(vec![1; 1024])) as ArrayRef;
        let col1 = Arc::new(StructArray::new(
            if let DataType::Struct(fields) = arrow_schema.fields.get(1).unwrap().data_type() {
                fields.clone()
            } else {
                unreachable!()
            },
            vec![Arc::new(Int32Array::from_iter_values(vec![1; 1024]))],
            None,
        ));
        let col2 = Arc::new(arrow_array::StringArray::from_iter_values(vec![
            "test";
            1024
        ])) as ArrayRef;
        let col3 = Arc::new({
            let list_parts = arrow_array::ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
              Some(
                  vec![Some(1),]
              );
              1024
          ])
            .into_parts();
            arrow_array::ListArray::new(
                if let DataType::List(field) = arrow_schema.fields.get(3).unwrap().data_type() {
                    field.clone()
                } else {
                    unreachable!()
                },
                list_parts.1,
                list_parts.2,
                list_parts.3,
            )
        }) as ArrayRef;
        let col4 = Arc::new(StructArray::new(
            if let DataType::Struct(fields) = arrow_schema.fields.get(4).unwrap().data_type() {
                fields.clone()
            } else {
                unreachable!()
            },
            vec![Arc::new(StructArray::new(
                if let DataType::Struct(fields) = arrow_schema.fields.get(4).unwrap().data_type() {
                    if let DataType::Struct(fields) = fields.first().unwrap().data_type() {
                        fields.clone()
                    } else {
                        unreachable!()
                    }
                } else {
                    unreachable!()
                },
                vec![Arc::new(Int32Array::from_iter_values(vec![1; 1024]))],
                None,
            ))],
            None,
        ));
        let columns = vec![col0, col1, col2, col3, col4];
        let to_write = RecordBatch::try_new(arrow_schema.clone(), columns).unwrap();

        let equality_ids = vec![1, 3];
        let projector =
            ArrowFieldProjector::new(arrow_schema, &equality_ids, PARQUET_FIELD_ID_META_KEY)?;
        let delete_schema = arrow_schema_to_schema(projector.projected_schema_ref()).unwrap();

        // prepare writer
        let pb = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::new(delete_schema),
            file_io.clone(),
            location_gen,
            file_name_gen,
        );

        let mut equality_delete_writer = EqualityDeleteFileWriterBuilder::new(pb)
            .build(EqualityDeleteWriterConfig::new(
                equality_ids,
                projector.clone(),
                None,
            ))
            .await?;
        // write
        equality_delete_writer.write(to_write.clone()).await?;
        let res = equality_delete_writer.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res.into_iter().next().unwrap();

        // check
        let to_write_projected = projector.project_bacth(to_write)?;
        check_parquet_data_file_with_equality_delete_write(
            &file_io,
            &data_file,
            &to_write_projected,
        )
        .await;
        Ok(())
    }

    #[tokio::test]
    async fn test_equality_delete_float_or_double_column() -> Result<(), anyhow::Error> {
        // Float32, Float64
        let schema = {
            let fields = vec![
                arrow_schema::Field::new("col0", arrow_schema::DataType::Float32, true)
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "0".to_string(),
                    )])),
                arrow_schema::Field::new("col1", arrow_schema::DataType::Float64, true)
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "1".to_string(),
                    )])),
            ];
            Arc::new(arrow_schema::Schema::new(fields))
        };

        let equality_id_float = vec![0];
        let result_float = ArrowFieldProjector::new(
            schema.clone(),
            &equality_id_float,
            PARQUET_FIELD_ID_META_KEY,
        );
        assert!(result_float.is_err());

        let equality_ids_double = vec![1];
        let result_double = ArrowFieldProjector::new(
            schema.clone(),
            &equality_ids_double,
            PARQUET_FIELD_ID_META_KEY,
        );
        assert!(result_double.is_err());

        Ok(())
    }
}
