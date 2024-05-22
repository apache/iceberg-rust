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

use arrow_array::{ArrayRef, RecordBatch, StructArray};
use arrow_schema::{DataType, FieldRef, Fields, Schema, SchemaRef};
use itertools::Itertools;

use crate::spec::{DataFile, Struct};
use crate::writer::file_writer::FileWriter;
use crate::writer::{file_writer::FileWriterBuilder, IcebergWriter, IcebergWriterBuilder};
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
    projector: FieldProjector,
    schema: SchemaRef,
    partition_value: Struct,
}

impl EqualityDeleteWriterConfig {
    /// Create a new `DataFileWriterConfig` with equality ids.
    pub fn new(
        equality_ids: Vec<usize>,
        projector: FieldProjector,
        schema: Schema,
        partition_value: Option<Struct>,
    ) -> Self {
        Self {
            equality_ids,
            projector,
            schema: schema.into(),
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
            delete_schema_ref: config.schema,
            equality_ids: config.equality_ids,
            partition_value: config.partition_value,
        })
    }
}

/// A writer write data
pub struct EqualityDeleteFileWriter<B: FileWriterBuilder> {
    inner_writer: Option<B::R>,
    projector: FieldProjector,
    delete_schema_ref: SchemaRef,
    equality_ids: Vec<usize>,
    partition_value: Struct,
}

impl<B: FileWriterBuilder> EqualityDeleteFileWriter<B> {
    fn project_record_batch_columns(&self, batch: RecordBatch) -> Result<RecordBatch> {
        RecordBatch::try_new(
            self.delete_schema_ref.clone(),
            self.projector.project(batch.columns()),
        )
        .map_err(|err| Error::new(ErrorKind::DataInvalid, format!("{err}")))
    }
}

#[async_trait::async_trait]
impl<B: FileWriterBuilder> IcebergWriter for EqualityDeleteFileWriter<B> {
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        let batch = self.project_record_batch_columns(batch)?;
        self.inner_writer.as_mut().unwrap().write(&batch).await
    }

    async fn close(&mut self) -> Result<Vec<DataFile>> {
        let writer = self.inner_writer.take().unwrap();
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
    }
}

/// Help to project specific field from `RecordBatch`` according to the column id.
pub struct FieldProjector {
    index_vec_vec: Vec<Vec<usize>>,
}

impl FieldProjector {
    /// Init FieldProjector
    pub fn new(
        batch_fields: &Fields,
        column_ids: &[usize],
        column_id_meta_key: &str,
    ) -> Result<(Self, Fields)> {
        let mut index_vec_vec = Vec::with_capacity(column_ids.len());
        let mut fields = Vec::with_capacity(column_ids.len());
        for &id in column_ids {
            let mut index_vec = vec![];
            if let Ok(field) = Self::fetch_column_index(
                batch_fields,
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
        Ok((Self { index_vec_vec }, Fields::from_iter(fields)))
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
    /// Do projection with batch
    pub fn project(&self, batch: &[ArrayRef]) -> Vec<ArrayRef> {
        self.index_vec_vec
            .iter()
            .map(|index_vec| Self::get_column_by_index_vec(batch, index_vec))
            .collect_vec()
    }

    fn get_column_by_index_vec(batch: &[ArrayRef], index_vec: &[usize]) -> ArrayRef {
        let mut rev_iterator = index_vec.iter().rev();
        let mut array = batch[*rev_iterator.next().unwrap()].clone();
        for idx in rev_iterator {
            array = array
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
                .column(*idx)
                .clone();
        }
        array
    }
}

#[cfg(test)]
mod test {
    use arrow_select::concat::concat_batches;
    use bytes::Bytes;
    use futures::AsyncReadExt;
    use itertools::Itertools;
    use std::{collections::HashMap, sync::Arc};

    use arrow_array::{types::Int64Type, ArrayRef, Int64Array, RecordBatch, StructArray};
    use parquet::{
        arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, PARQUET_FIELD_ID_META_KEY},
        file::properties::WriterProperties,
    };
    use tempfile::TempDir;

    use crate::{
        io::{FileIO, FileIOBuilder},
        spec::{DataFile, DataFileFormat},
        writer::{
            base_writer::equality_delete_writer::{
                EqualityDeleteFileWriterBuilder, FieldProjector,
            },
            file_writer::{
                location_generator::{test::MockLocationGenerator, DefaultFileNameGenerator},
                ParquetWriterBuilder,
            },
            IcebergWriter, IcebergWriterBuilder,
        },
    };

    use super::EqualityDeleteWriterConfig;

    pub(crate) async fn check_parquet_data_file_with_equality_delete_write(
        file_io: &FileIO,
        data_file: &DataFile,
        batch: &RecordBatch,
    ) {
        assert_eq!(data_file.file_format, DataFileFormat::Parquet);

        // read the written file
        let mut input_file = file_io
            .new_input(data_file.file_path.clone())
            .unwrap()
            .reader()
            .await
            .unwrap();
        let mut res = vec![];
        let file_size = input_file.read_to_end(&mut res).await.unwrap();
        let reader_builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(res)).unwrap();
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

        assert_eq!(data_file.file_size_in_bytes, file_size as u64);

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
        let schema = {
            let fields = vec![
                arrow_schema::Field::new("col0", arrow_schema::DataType::Int64, true)
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "0".to_string(),
                    )])),
                arrow_schema::Field::new(
                    "col1",
                    arrow_schema::DataType::Struct(
                        vec![arrow_schema::Field::new(
                            "sub_col",
                            arrow_schema::DataType::Int64,
                            true,
                        )
                        .with_metadata(HashMap::from([(
                            PARQUET_FIELD_ID_META_KEY.to_string(),
                            "5".to_string(),
                        )]))]
                        .into(),
                    ),
                    true,
                )
                .with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "1".to_string(),
                )])),
                arrow_schema::Field::new("col2", arrow_schema::DataType::Utf8, true).with_metadata(
                    HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "2".to_string())]),
                ),
                arrow_schema::Field::new(
                    "col3",
                    arrow_schema::DataType::List(Arc::new(
                        arrow_schema::Field::new("item", arrow_schema::DataType::Int64, true)
                            .with_metadata(HashMap::from([(
                                PARQUET_FIELD_ID_META_KEY.to_string(),
                                "6".to_string(),
                            )])),
                    )),
                    true,
                )
                .with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "3".to_string(),
                )])),
                arrow_schema::Field::new(
                    "col4",
                    arrow_schema::DataType::Struct(
                        vec![arrow_schema::Field::new(
                            "sub_col",
                            arrow_schema::DataType::Struct(
                                vec![arrow_schema::Field::new(
                                    "sub_sub_col",
                                    arrow_schema::DataType::Int64,
                                    true,
                                )
                                .with_metadata(HashMap::from([(
                                    PARQUET_FIELD_ID_META_KEY.to_string(),
                                    "7".to_string(),
                                )]))]
                                .into(),
                            ),
                            true,
                        )
                        .with_metadata(HashMap::from([(
                            PARQUET_FIELD_ID_META_KEY.to_string(),
                            "8".to_string(),
                        )]))]
                        .into(),
                    ),
                    true,
                )
                .with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "4".to_string(),
                )])),
            ];
            arrow_schema::Schema::new(fields)
        };
        let col0 = Arc::new(Int64Array::from_iter_values(vec![1; 1024])) as ArrayRef;
        let col1 = Arc::new(StructArray::new(
            vec![
                arrow_schema::Field::new("sub_col", arrow_schema::DataType::Int64, true)
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "5".to_string(),
                    )])),
            ]
            .into(),
            vec![Arc::new(Int64Array::from_iter_values(vec![1; 1024]))],
            None,
        ));
        let col2 = Arc::new(arrow_array::StringArray::from_iter_values(vec![
            "test";
            1024
        ])) as ArrayRef;
        let col3 = Arc::new({
            let list_parts = arrow_array::ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
              Some(
                  vec![Some(1),]
              );
              1024
          ])
            .into_parts();
            arrow_array::ListArray::new(
                Arc::new(list_parts.0.as_ref().clone().with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "6".to_string(),
                )]))),
                list_parts.1,
                list_parts.2,
                list_parts.3,
            )
        }) as ArrayRef;
        let col4 = Arc::new(StructArray::new(
            vec![arrow_schema::Field::new(
                "sub_col",
                arrow_schema::DataType::Struct(
                    vec![arrow_schema::Field::new(
                        "sub_sub_col",
                        arrow_schema::DataType::Int64,
                        true,
                    )
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "7".to_string(),
                    )]))]
                    .into(),
                ),
                true,
            )
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "8".to_string(),
            )]))]
            .into(),
            vec![Arc::new(StructArray::new(
                vec![
                    arrow_schema::Field::new("sub_sub_col", arrow_schema::DataType::Int64, true)
                        .with_metadata(HashMap::from([(
                            PARQUET_FIELD_ID_META_KEY.to_string(),
                            "7".to_string(),
                        )])),
                ]
                .into(),
                vec![Arc::new(Int64Array::from_iter_values(vec![1; 1024]))],
                None,
            ))],
            None,
        ));
        let columns = vec![col0, col1, col2, col3, col4];

        let equality_ids = vec![1, 3];
        let (projector, fields) =
            FieldProjector::new(schema.fields(), &equality_ids, PARQUET_FIELD_ID_META_KEY)?;
        let delete_schema = arrow_schema::Schema::new(fields);
        let delete_schema_ref = Arc::new(delete_schema.clone());

        // prepare writer
        let to_write = RecordBatch::try_new(Arc::new(schema.clone()), columns).unwrap();
        let pb = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            delete_schema_ref.clone(),
            file_io.clone(),
            location_gen,
            file_name_gen,
        );

        let mut equality_delete_writer = EqualityDeleteFileWriterBuilder::new(pb)
            .build(EqualityDeleteWriterConfig::new(
                equality_ids,
                projector,
                delete_schema.clone(),
                None,
            ))
            .await?;
        // write
        equality_delete_writer.write(to_write.clone()).await?;
        let res = equality_delete_writer.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res.into_iter().next().unwrap();

        // check
        let to_write_projected = equality_delete_writer.project_record_batch_columns(to_write)?;
        check_parquet_data_file_with_equality_delete_write(
            &file_io,
            &data_file,
            &to_write_projected,
        )
        .await;
        Ok(())
    }
}
