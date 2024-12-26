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

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, SchemaRef as ArrowSchemaRef};
use itertools::Itertools;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::arrow::record_batch_projector::RecordBatchProjector;
use crate::arrow::schema_to_arrow_schema;
use crate::spec::{DataFile, SchemaRef, Struct};
use crate::writer::file_writer::{FileWriter, FileWriterBuilder};
use crate::writer::{IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind, Result};

/// Builder for `EqualityDeleteWriter`.
#[derive(Clone, Debug)]
pub struct EqualityDeleteFileWriterBuilder<B: FileWriterBuilder> {
    inner: B,
    config: EqualityDeleteWriterConfig,
}

impl<B: FileWriterBuilder> EqualityDeleteFileWriterBuilder<B> {
    /// Create a new `EqualityDeleteFileWriterBuilder` using a `FileWriterBuilder`.
    pub fn new(inner: B, config: EqualityDeleteWriterConfig) -> Self {
        Self { inner, config }
    }
}

/// Config for `EqualityDeleteWriter`.
#[derive(Clone, Debug)]
pub struct EqualityDeleteWriterConfig {
    // Field ids used to determine row equality in equality delete files.
    equality_ids: Vec<i32>,
    // Projector used to project the data chunk into specific fields.
    projector: RecordBatchProjector,
    partition_value: Struct,
}

impl EqualityDeleteWriterConfig {
    /// Create a new `DataFileWriterConfig` with equality ids.
    pub fn new(
        equality_ids: Vec<i32>,
        original_schema: SchemaRef,
        partition_value: Option<Struct>,
    ) -> Result<Self> {
        let original_arrow_schema = Arc::new(schema_to_arrow_schema(&original_schema)?);
        let projector = RecordBatchProjector::new(
            original_arrow_schema,
            &equality_ids,
            // The following rule comes from https://iceberg.apache.org/spec/#identifier-field-ids
            // - The identifier field ids must be used for primitive types.
            // - The identifier field ids must not be used for floating point types or nullable fields.
            // - The identifier field ids can be nested field of struct but not nested field of nullable struct.
            |field| {
                // Only primitive type is allowed to be used for identifier field ids
                if field.is_nullable()
                    || field.data_type().is_nested()
                    || matches!(
                        field.data_type(),
                        DataType::Float16 | DataType::Float32 | DataType::Float64
                    )
                {
                    return Ok(None);
                }
                Ok(Some(
                    field
                        .metadata()
                        .get(PARQUET_FIELD_ID_META_KEY)
                        .ok_or_else(|| {
                            Error::new(ErrorKind::Unexpected, "Field metadata is missing.")
                        })?
                        .parse::<i64>()
                        .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?,
                ))
            },
            |field: &Field| !field.is_nullable(),
        )?;
        Ok(Self {
            equality_ids,
            projector,
            partition_value: partition_value.unwrap_or(Struct::empty()),
        })
    }

    /// Return projected Schema
    pub fn projected_arrow_schema_ref(&self) -> &ArrowSchemaRef {
        self.projector.projected_schema_ref()
    }
}

#[async_trait::async_trait]
impl<B: FileWriterBuilder> IcebergWriterBuilder for EqualityDeleteFileWriterBuilder<B> {
    type R = EqualityDeleteFileWriter<B>;

    async fn build(self) -> Result<Self::R> {
        Ok(EqualityDeleteFileWriter {
            inner_writer: Some(self.inner.clone().build().await?),
            projector: self.config.projector,
            equality_ids: self.config.equality_ids,
            partition_value: self.config.partition_value,
        })
    }
}

/// Writer used to write equality delete files.
#[derive(Debug)]
pub struct EqualityDeleteFileWriter<B: FileWriterBuilder> {
    inner_writer: Option<B::R>,
    projector: RecordBatchProjector,
    equality_ids: Vec<i32>,
    partition_value: Struct,
}

#[async_trait::async_trait]
impl<B: FileWriterBuilder> IcebergWriter for EqualityDeleteFileWriter<B> {
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        let batch = self.projector.project_batch(batch)?;
        if let Some(writer) = self.inner_writer.as_mut() {
            writer.write(&batch).await
        } else {
            Err(Error::new(
                ErrorKind::Unexpected,
                "Equality delete inner writer has been closed.",
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
                    res.equality_ids(self.equality_ids.iter().copied().collect_vec());
                    res.partition(self.partition_value.clone());
                    res.build().expect("msg")
                })
                .collect_vec())
        } else {
            Err(Error::new(
                ErrorKind::Unexpected,
                "Equality delete inner writer has been closed.",
            ))
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_array::types::Int32Type;
    use arrow_array::{ArrayRef, BooleanArray, Int32Array, Int64Array, RecordBatch, StructArray};
    use arrow_schema::DataType;
    use arrow_select::concat::concat_batches;
    use itertools::Itertools;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;
    use uuid::Uuid;

    use crate::arrow::{arrow_schema_to_schema, schema_to_arrow_schema};
    use crate::io::{FileIO, FileIOBuilder};
    use crate::spec::{
        DataFile, DataFileFormat, ListType, MapType, NestedField, PrimitiveType, Schema,
        StructType, Type,
    };
    use crate::writer::base_writer::equality_delete_writer::{
        EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig,
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

        let equality_ids = vec![0_i32, 8];
        let equality_config =
            EqualityDeleteWriterConfig::new(equality_ids, Arc::new(schema), None).unwrap();
        let delete_schema =
            arrow_schema_to_schema(equality_config.projected_arrow_schema_ref()).unwrap();
        let projector = equality_config.projector.clone();

        // prepare writer
        let pb = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::new(delete_schema),
            file_io.clone(),
            location_gen,
            file_name_gen,
        );
        let mut equality_delete_writer = EqualityDeleteFileWriterBuilder::new(pb, equality_config)
            .build()
            .await?;

        // write
        equality_delete_writer.write(to_write.clone()).await?;
        let res = equality_delete_writer.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res.into_iter().next().unwrap();

        // check
        let to_write_projected = projector.project_batch(to_write)?;
        check_parquet_data_file_with_equality_delete_write(
            &file_io,
            &data_file,
            &to_write_projected,
        )
        .await;
        Ok(())
    }

    #[tokio::test]
    async fn test_equality_delete_unreachable_column() -> Result<(), anyhow::Error> {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(0, "col0", Type::Primitive(PrimitiveType::Float)).into(),
                    NestedField::required(1, "col1", Type::Primitive(PrimitiveType::Double)).into(),
                    NestedField::optional(2, "col2", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(
                        3,
                        "col3",
                        Type::Struct(StructType::new(vec![NestedField::required(
                            4,
                            "sub_col",
                            Type::Primitive(PrimitiveType::Int),
                        )
                        .into()])),
                    )
                    .into(),
                    NestedField::optional(
                        5,
                        "col4",
                        Type::Struct(StructType::new(vec![NestedField::required(
                            6,
                            "sub_col2",
                            Type::Primitive(PrimitiveType::Int),
                        )
                        .into()])),
                    )
                    .into(),
                    NestedField::required(
                        7,
                        "col5",
                        Type::Map(MapType::new(
                            Arc::new(NestedField::required(
                                8,
                                "key",
                                Type::Primitive(PrimitiveType::String),
                            )),
                            Arc::new(NestedField::required(
                                9,
                                "value",
                                Type::Primitive(PrimitiveType::Int),
                            )),
                        )),
                    )
                    .into(),
                    NestedField::required(
                        10,
                        "col6",
                        Type::List(ListType::new(Arc::new(NestedField::required(
                            11,
                            "element",
                            Type::Primitive(PrimitiveType::Int),
                        )))),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        // Float and Double are not allowed to be used for equality delete
        assert!(EqualityDeleteWriterConfig::new(vec![0], schema.clone(), None).is_err());
        assert!(EqualityDeleteWriterConfig::new(vec![1], schema.clone(), None).is_err());
        // Int is nullable, not allowed to be used for equality delete
        assert!(EqualityDeleteWriterConfig::new(vec![2], schema.clone(), None).is_err());
        // Struct is not allowed to be used for equality delete
        assert!(EqualityDeleteWriterConfig::new(vec![3], schema.clone(), None).is_err());
        // Nested field of struct is allowed to be used for equality delete
        assert!(EqualityDeleteWriterConfig::new(vec![4], schema.clone(), None).is_ok());
        // Nested field of optional struct is not allowed to be used for equality delete
        assert!(EqualityDeleteWriterConfig::new(vec![6], schema.clone(), None).is_err());
        // Nested field of map is not allowed to be used for equality delete
        assert!(EqualityDeleteWriterConfig::new(vec![7], schema.clone(), None).is_err());
        assert!(EqualityDeleteWriterConfig::new(vec![8], schema.clone(), None).is_err());
        assert!(EqualityDeleteWriterConfig::new(vec![9], schema.clone(), None).is_err());
        // Nested field of list is not allowed to be used for equality delete
        assert!(EqualityDeleteWriterConfig::new(vec![10], schema.clone(), None).is_err());
        assert!(EqualityDeleteWriterConfig::new(vec![11], schema.clone(), None).is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_equality_delete_with_primitive_type() -> Result<(), anyhow::Error> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen =
            MockLocationGenerator::new(temp_dir.path().to_str().unwrap().to_string());
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(0, "col0", Type::Primitive(PrimitiveType::Boolean))
                        .into(),
                    NestedField::required(1, "col1", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "col2", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::required(
                        3,
                        "col3",
                        Type::Primitive(PrimitiveType::Decimal {
                            precision: 38,
                            scale: 5,
                        }),
                    )
                    .into(),
                    NestedField::required(4, "col4", Type::Primitive(PrimitiveType::Date)).into(),
                    NestedField::required(5, "col5", Type::Primitive(PrimitiveType::Time)).into(),
                    NestedField::required(6, "col6", Type::Primitive(PrimitiveType::Timestamp))
                        .into(),
                    NestedField::required(7, "col7", Type::Primitive(PrimitiveType::Timestamptz))
                        .into(),
                    NestedField::required(8, "col8", Type::Primitive(PrimitiveType::TimestampNs))
                        .into(),
                    NestedField::required(9, "col9", Type::Primitive(PrimitiveType::TimestamptzNs))
                        .into(),
                    NestedField::required(10, "col10", Type::Primitive(PrimitiveType::String))
                        .into(),
                    NestedField::required(11, "col11", Type::Primitive(PrimitiveType::Uuid)).into(),
                    NestedField::required(12, "col12", Type::Primitive(PrimitiveType::Fixed(10)))
                        .into(),
                    NestedField::required(13, "col13", Type::Primitive(PrimitiveType::Binary))
                        .into(),
                ])
                .build()
                .unwrap(),
        );
        let equality_ids = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13];
        let config = EqualityDeleteWriterConfig::new(equality_ids, schema.clone(), None).unwrap();
        let delete_arrow_schema = config.projected_arrow_schema_ref().clone();
        let delete_schema = arrow_schema_to_schema(&delete_arrow_schema).unwrap();

        let pb = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::new(delete_schema),
            file_io.clone(),
            location_gen,
            file_name_gen,
        );
        let mut equality_delete_writer = EqualityDeleteFileWriterBuilder::new(pb, config)
            .build()
            .await?;

        // prepare data
        let col0 = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(true),
        ])) as ArrayRef;
        let col1 = Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(4)])) as ArrayRef;
        let col2 = Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(4)])) as ArrayRef;
        let col3 = Arc::new(
            arrow_array::Decimal128Array::from(vec![Some(1), Some(2), Some(4)])
                .with_precision_and_scale(38, 5)
                .unwrap(),
        ) as ArrayRef;
        let col4 = Arc::new(arrow_array::Date32Array::from(vec![
            Some(0),
            Some(1),
            Some(3),
        ])) as ArrayRef;
        let col5 = Arc::new(arrow_array::Time64MicrosecondArray::from(vec![
            Some(0),
            Some(1),
            Some(3),
        ])) as ArrayRef;
        let col6 = Arc::new(arrow_array::TimestampMicrosecondArray::from(vec![
            Some(0),
            Some(1),
            Some(3),
        ])) as ArrayRef;
        let col7 = Arc::new(
            arrow_array::TimestampMicrosecondArray::from(vec![Some(0), Some(1), Some(3)])
                .with_timezone_utc(),
        ) as ArrayRef;
        let col8 = Arc::new(arrow_array::TimestampNanosecondArray::from(vec![
            Some(0),
            Some(1),
            Some(3),
        ])) as ArrayRef;
        let col9 = Arc::new(
            arrow_array::TimestampNanosecondArray::from(vec![Some(0), Some(1), Some(3)])
                .with_timezone_utc(),
        ) as ArrayRef;
        let col10 = Arc::new(arrow_array::StringArray::from(vec![
            Some("a"),
            Some("b"),
            Some("d"),
        ])) as ArrayRef;
        let col11 = Arc::new(
            arrow_array::FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                vec![
                    Some(Uuid::from_u128(0).as_bytes().to_vec()),
                    Some(Uuid::from_u128(1).as_bytes().to_vec()),
                    Some(Uuid::from_u128(3).as_bytes().to_vec()),
                ]
                .into_iter(),
                16,
            )
            .unwrap(),
        ) as ArrayRef;
        let col12 = Arc::new(
            arrow_array::FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                vec![
                    Some(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
                    Some(vec![11, 12, 13, 14, 15, 16, 17, 18, 19, 20]),
                    Some(vec![21, 22, 23, 24, 25, 26, 27, 28, 29, 30]),
                ]
                .into_iter(),
                10,
            )
            .unwrap(),
        ) as ArrayRef;
        let col13 = Arc::new(arrow_array::LargeBinaryArray::from_opt_vec(vec![
            Some(b"one"),
            Some(b""),
            Some(b"zzzz"),
        ])) as ArrayRef;
        let to_write = RecordBatch::try_new(delete_arrow_schema.clone(), vec![
            col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13,
        ])
        .unwrap();
        equality_delete_writer.write(to_write.clone()).await?;
        let res = equality_delete_writer.close().await?;
        assert_eq!(res.len(), 1);
        check_parquet_data_file_with_equality_delete_write(
            &file_io,
            &res.into_iter().next().unwrap(),
            &to_write,
        )
        .await;

        Ok(())
    }
}
