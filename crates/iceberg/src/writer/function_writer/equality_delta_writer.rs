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

//! This module contains the equality delta writer.

use std::collections::HashMap;

use arrow_array::builder::BooleanBuilder;
use arrow_array::{Int32Array, RecordBatch};
use arrow_ord::partition::partition;
use arrow_row::{OwnedRow, RowConverter, Rows, SortField};
use arrow_select::filter::filter_record_batch;
use itertools::Itertools;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::arrow::record_batch_projector::RecordBatchProjector;
use crate::arrow::schema_to_arrow_schema;
use crate::spec::DataFile;
use crate::writer::base_writer::sort_position_delete_writer::PositionDeleteInput;
use crate::writer::{CurrentFileStatus, IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind, Result};

/// Insert operation.
pub const INSERT_OP: i32 = 1;
/// Delete operation.
pub const DELETE_OP: i32 = 2;

/// Builder for `EqualityDeltaWriter`.
#[derive(Clone)]
pub struct EqualityDeltaWriterBuilder<DB, PDB, EDB> {
    data_writer_builder: DB,
    position_delete_writer_builder: PDB,
    equality_delete_writer_builder: EDB,
    unique_column_ids: Vec<i32>,
}

impl<DB, PDB, EDB> EqualityDeltaWriterBuilder<DB, PDB, EDB> {
    /// Create a new `EqualityDeltaWriterBuilder`.
    pub fn new(
        data_writer_builder: DB,
        position_delete_writer_builder: PDB,
        equality_delete_writer_builder: EDB,
        unique_column_ids: Vec<i32>,
    ) -> Self {
        Self {
            data_writer_builder,
            position_delete_writer_builder,
            equality_delete_writer_builder,
            unique_column_ids,
        }
    }
}

#[async_trait::async_trait]
impl<DB, PDB, EDB> IcebergWriterBuilder for EqualityDeltaWriterBuilder<DB, PDB, EDB>
where
    DB: IcebergWriterBuilder,
    PDB: IcebergWriterBuilder<PositionDeleteInput>,
    EDB: IcebergWriterBuilder,
    DB::R: CurrentFileStatus,
{
    type R = EqualityDeltaWriter<DB::R, PDB::R, EDB::R>;

    async fn build(self) -> Result<Self::R> {
        Self::R::try_new(
            self.data_writer_builder.build().await?,
            self.position_delete_writer_builder.build().await?,
            self.equality_delete_writer_builder.build().await?,
            self.unique_column_ids,
        )
    }
}

/// Equality delta writer.
pub struct EqualityDeltaWriter<D, PD, ED> {
    data_writer: D,
    position_delete_writer: PD,
    equality_delete_writer: ED,
    projector: RecordBatchProjector,
    inserted_row: HashMap<OwnedRow, PositionDeleteInput>,
    row_converter: RowConverter,
}

impl<D, PD, ED> EqualityDeltaWriter<D, PD, ED>
where
    D: IcebergWriter + CurrentFileStatus,
    PD: IcebergWriter<PositionDeleteInput>,
    ED: IcebergWriter,
{
    pub(crate) fn try_new(
        data_writer: D,
        position_delete_writer: PD,
        equality_delete_writer: ED,
        unique_column_ids: Vec<i32>,
    ) -> Result<Self> {
        let projector = RecordBatchProjector::new(
            &schema_to_arrow_schema(&data_writer.current_schema())?,
            &unique_column_ids,
            |field| {
                if field.data_type().is_nested() {
                    return Ok(None);
                }
                field
                    .metadata()
                    .get(PARQUET_FIELD_ID_META_KEY)
                    .map(|s| {
                        s.parse::<i64>()
                            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))
                    })
                    .transpose()
            },
            |_| true,
        )?;
        let row_converter = RowConverter::new(
            projector
                .projected_schema_ref()
                .fields()
                .iter()
                .map(|field| SortField::new(field.data_type().clone()))
                .collect(),
        )?;
        Ok(Self {
            data_writer,
            position_delete_writer,
            equality_delete_writer,
            projector,
            inserted_row: HashMap::new(),
            row_converter,
        })
    }
    /// Write the batch.
    /// 1. If a row with the same unique column is not written, then insert it.
    /// 2. If a row with the same unique column is written, then delete the previous row and insert the new row.
    async fn insert(&mut self, batch: RecordBatch) -> Result<()> {
        let rows = self.extract_unique_column(&batch)?;
        let current_file_path = self.data_writer.current_file_path();
        let current_file_offset = self.data_writer.current_row_num();
        for (idx, row) in rows.iter().enumerate() {
            let previous_input = self.inserted_row.insert(row.owned(), PositionDeleteInput {
                path: current_file_path.clone(),
                offset: (current_file_offset + idx) as i64,
            });
            if let Some(previous_input) = previous_input {
                self.position_delete_writer.write(previous_input).await?;
            }
        }

        self.data_writer.write(batch).await?;

        Ok(())
    }

    async fn delete(&mut self, batch: RecordBatch) -> Result<()> {
        let rows = self.extract_unique_column(&batch)?;
        let mut delete_row = BooleanBuilder::new();
        for row in rows.iter() {
            if let Some(previous_input) = self.inserted_row.remove(&row.owned()) {
                self.position_delete_writer.write(previous_input).await?;
                delete_row.append_value(false);
            } else {
                delete_row.append_value(true);
            }
        }
        let delete_batch = filter_record_batch(&batch, &delete_row.finish()).map_err(|err| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Failed to filter record batch, error: {}", err),
            )
        })?;
        self.equality_delete_writer.write(delete_batch).await?;
        Ok(())
    }

    fn extract_unique_column(&mut self, batch: &RecordBatch) -> Result<Rows> {
        self.row_converter
            .convert_columns(&self.projector.project_column(batch.columns())?)
            .map_err(|err| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Failed to convert columns, error: {}", err),
                )
            })
    }
}

#[async_trait::async_trait]
impl<D, PD, ED> IcebergWriter for EqualityDeltaWriter<D, PD, ED>
where
    D: IcebergWriter + CurrentFileStatus,
    PD: IcebergWriter<PositionDeleteInput>,
    ED: IcebergWriter,
{
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        // check the last column is int32 array.
        let ops = batch
            .column(batch.num_columns() - 1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or(Error::new(ErrorKind::DataInvalid, ""))?;

        // partition the ops.
        let partitions =
            partition(&[batch.column(batch.num_columns() - 1).clone()]).map_err(|err| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Failed to partition ops, error: {}", err),
                )
            })?;
        for range in partitions.ranges() {
            let batch = batch
                .project(&(0..batch.num_columns() - 1).collect_vec())
                .unwrap()
                .slice(range.start, range.end - range.start);
            match ops.value(range.start) {
                // Insert
                INSERT_OP => self.insert(batch).await?,
                // Delete
                DELETE_OP => self.delete(batch).await?,
                op => {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid ops: {op}"),
                    ))
                }
            }
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<Vec<DataFile>> {
        let data_files = self.data_writer.close().await?;
        let position_delete_files = self.position_delete_writer.close().await?;
        let equality_delete_files = self.equality_delete_writer.close().await?;
        Ok(data_files
            .into_iter()
            .chain(position_delete_files)
            .chain(equality_delete_files)
            .collect())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_array::{Int32Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use arrow_select::concat::concat_batches;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use crate::arrow::arrow_schema_to_schema;
    use crate::io::FileIOBuilder;
    use crate::spec::{DataContentType, DataFileFormat, NestedField, PrimitiveType, Schema, Type};
    use crate::writer::base_writer::data_file_writer::DataFileWriterBuilder;
    use crate::writer::base_writer::equality_delete_writer::{
        EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig,
    };
    use crate::writer::base_writer::sort_position_delete_writer::{
        SortPositionDeleteWriterBuilder, POSITION_DELETE_ARROW_SCHEMA, POSITION_DELETE_SCHEMA,
    };
    use crate::writer::file_writer::location_generator::test::MockLocationGenerator;
    use crate::writer::file_writer::location_generator::DefaultFileNameGenerator;
    use crate::writer::file_writer::ParquetWriterBuilder;
    use crate::writer::function_writer::equality_delta_writer::{
        EqualityDeltaWriterBuilder, DELETE_OP, INSERT_OP,
    };
    use crate::writer::{IcebergWriter, IcebergWriterBuilder};
    use crate::Result;

    #[tokio::test]
    async fn test_equality_delta_writer() -> Result<()> {
        // prepare writer
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        1,
                        "id".to_string(),
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::required(
                        2,
                        "name".to_string(),
                        Type::Primitive(PrimitiveType::String),
                    )),
                ])
                .build()
                .unwrap(),
        );
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let location_gen =
            MockLocationGenerator::new(temp_dir.path().to_str().unwrap().to_string());
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let data_file_writer_builder = {
            let pw = ParquetWriterBuilder::new(
                WriterProperties::builder().build(),
                schema.clone(),
                file_io.clone(),
                location_gen.clone(),
                file_name_gen.clone(),
            );
            DataFileWriterBuilder::new(pw.clone(), None, None)
        };
        let position_delete_writer_builder = {
            let pw = ParquetWriterBuilder::new(
                WriterProperties::builder().build(),
                POSITION_DELETE_SCHEMA.clone(),
                file_io.clone(),
                location_gen.clone(),
                file_name_gen.clone(),
            );
            SortPositionDeleteWriterBuilder::new(pw.clone(), 100, None, None)
        };
        let equality_delete_writer_builder = {
            let config = EqualityDeleteWriterConfig::new(vec![1, 2], schema, None, None)?;
            let pw = ParquetWriterBuilder::new(
                WriterProperties::builder().build(),
                arrow_schema_to_schema(config.projected_arrow_schema_ref())
                    .unwrap()
                    .into(),
                file_io.clone(),
                location_gen,
                file_name_gen,
            );
            EqualityDeleteFileWriterBuilder::new(pw, config)
        };
        let mut equality_delta_writer = EqualityDeltaWriterBuilder::new(
            data_file_writer_builder,
            position_delete_writer_builder,
            equality_delete_writer_builder,
            vec![1, 2],
        )
        .build()
        .await
        .unwrap();

        // write data
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("data", DataType::Utf8, true),
            Field::new("op", DataType::Int32, false),
        ]));
        {
            let id_array = Int64Array::from(vec![1, 2, 1, 3, 2, 3, 1]);
            let data_array = StringArray::from(vec!["a", "b", "c", "d", "e", "f", "g"]);
            let batch = RecordBatch::try_new(schema.clone(), vec![
                Arc::new(id_array),
                Arc::new(data_array),
                Arc::new(Int32Array::from(vec![INSERT_OP; 7])),
            ])
            .expect("Failed to create RecordBatch");
            equality_delta_writer.write(batch).await?;
        }
        {
            let id_array = Int64Array::from(vec![1, 2, 3, 4]);
            let data_array = StringArray::from(vec!["a", "b", "k", "l"]);
            let batch = RecordBatch::try_new(schema.clone(), vec![
                Arc::new(id_array),
                Arc::new(data_array),
                Arc::new(Int32Array::from(vec![
                    DELETE_OP, DELETE_OP, DELETE_OP, INSERT_OP,
                ])),
            ])
            .expect("Failed to create RecordBatch");
            equality_delta_writer.write(batch).await?;
        }

        let data_files = equality_delta_writer.close().await?;
        assert_eq!(data_files.len(), 3);
        // data file
        let data_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("data", DataType::Utf8, true),
        ]));
        let data_file = data_files
            .iter()
            .find(|file| file.content == DataContentType::Data)
            .unwrap();
        let data_file_path = data_file.file_path().to_string();
        let input_file = file_io.new_input(&data_file_path).unwrap();
        let input_content = input_file.read().await.unwrap();
        let reader_builder =
            ParquetRecordBatchReaderBuilder::try_new(input_content.clone()).unwrap();
        let reader = reader_builder.build().unwrap();
        let batches = reader.map(|batch| batch.unwrap()).collect::<Vec<_>>();
        let res = concat_batches(&data_schema, &batches).unwrap();
        let expected_batches = RecordBatch::try_new(data_schema.clone(), vec![
            Arc::new(Int64Array::from(vec![1, 2, 1, 3, 2, 3, 1, 4])),
            Arc::new(StringArray::from(vec![
                "a", "b", "c", "d", "e", "f", "g", "l",
            ])),
        ])
        .unwrap();
        assert_eq!(expected_batches, res);

        // position delete file
        let position_delete_file = data_files
            .iter()
            .find(|file| file.content == DataContentType::PositionDeletes)
            .unwrap();
        let input_file = file_io
            .new_input(position_delete_file.file_path.clone())
            .unwrap();
        let input_content = input_file.read().await.unwrap();
        let reader_builder =
            ParquetRecordBatchReaderBuilder::try_new(input_content.clone()).unwrap();
        let reader = reader_builder.build().unwrap();
        let batches = reader.map(|batch| batch.unwrap()).collect::<Vec<_>>();
        let res = concat_batches(&POSITION_DELETE_ARROW_SCHEMA, &batches).unwrap();
        let expected_batches = RecordBatch::try_new(POSITION_DELETE_ARROW_SCHEMA.clone(), vec![
            Arc::new(StringArray::from(vec![
                data_file_path.clone(),
                data_file_path.clone(),
            ])),
            Arc::new(Int64Array::from(vec![0, 1])),
        ])
        .unwrap();
        assert_eq!(expected_batches, res);

        // equality delete file
        let equality_delete_file = data_files
            .iter()
            .find(|file| file.content == DataContentType::EqualityDeletes)
            .unwrap();
        let input_file = file_io
            .new_input(equality_delete_file.file_path.clone())
            .unwrap();
        let input_content = input_file.read().await.unwrap();
        let reader_builder =
            ParquetRecordBatchReaderBuilder::try_new(input_content.clone()).unwrap();
        let reader = reader_builder.build().unwrap();
        let batches = reader.map(|batch| batch.unwrap()).collect::<Vec<_>>();
        let res = concat_batches(&data_schema, &batches).unwrap();
        let expected_batches = RecordBatch::try_new(data_schema.clone(), vec![
            Arc::new(Int64Array::from(vec![3])),
            Arc::new(StringArray::from(vec!["k"])),
        ])
        .unwrap();
        assert_eq!(expected_batches, res);

        Ok(())
    }
}
