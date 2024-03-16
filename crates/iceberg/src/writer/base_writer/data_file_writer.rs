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

//! This module provide `DataFileWriter`.

use crate::spec::{DataContentType, DataFileBuilder};
use crate::writer::file_writer::FileWriter;
use crate::writer::CurrentFileStatus;
use crate::writer::{file_writer::FileWriterBuilder, IcebergWriter, IcebergWriterBuilder};
use crate::Result;
use arrow_array::RecordBatch;
use itertools::Itertools;

/// Builder for `DataFileWriter`.
#[derive(Clone)]
pub struct DataFileWriterBuilder<B: FileWriterBuilder> {
    inner: B,
}

impl<B: FileWriterBuilder> DataFileWriterBuilder<B> {
    /// Create a new `DataFileWriterBuilder` using a `FileWriterBuilder`.
    pub fn new(inner: B) -> Self {
        Self { inner }
    }
}

#[allow(async_fn_in_trait)]
impl<B: FileWriterBuilder> IcebergWriterBuilder for DataFileWriterBuilder<B> {
    type R = DataFileWriter<B>;

    async fn build(self) -> Result<Self::R> {
        Ok(DataFileWriter {
            inner_writer: self.inner.clone().build().await?,
            builder: self.inner,
        })
    }
}

/// A writer write data is within one spec/partition.
pub struct DataFileWriter<B: FileWriterBuilder> {
    builder: B,
    inner_writer: B::R,
}

#[async_trait::async_trait]
impl<B: FileWriterBuilder> IcebergWriter for DataFileWriter<B> {
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.inner_writer.write(&batch).await
    }

    async fn flush(&mut self) -> Result<Vec<DataFileBuilder>> {
        let writer = std::mem::replace(&mut self.inner_writer, self.builder.clone().build().await?);
        let res = writer
            .close()
            .await?
            .into_iter()
            .map(|mut res| {
                res.content(DataContentType::Data);
                res
            })
            .collect_vec();
        Ok(res)
    }
}

impl<B: FileWriterBuilder> CurrentFileStatus for DataFileWriter<B> {
    fn current_file_path(&self) -> String {
        self.inner_writer.current_file_path()
    }

    fn current_row_num(&self) -> usize {
        self.inner_writer.current_row_num()
    }

    fn current_written_size(&self) -> usize {
        self.inner_writer.current_written_size()
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, sync::Arc};

    use arrow_array::{types::Int64Type, ArrayRef, Int64Array, RecordBatch, StructArray};
    use parquet::{arrow::PARQUET_FIELD_ID_META_KEY, file::properties::WriterProperties};
    use tempfile::TempDir;

    use crate::{
        io::FileIOBuilder,
        spec::{DataFileFormat, Struct},
        writer::{
            base_writer::data_file_writer::DataFileWriterBuilder,
            file_writer::{
                location_generator::{test::MockLocationGenerator, DefaultFileNameGenerator},
                ParquetWriterBuilder,
            },
            tests::check_parquet_data_file,
            IcebergWriter, IcebergWriterBuilder,
        },
    };

    #[tokio::test]
    async fn test_data_file_writer() -> Result<(), anyhow::Error> {
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
                            "-1".to_string(),
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
                                "-1".to_string(),
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
                                    "-1".to_string(),
                                )]))]
                                .into(),
                            ),
                            true,
                        )
                        .with_metadata(HashMap::from([(
                            PARQUET_FIELD_ID_META_KEY.to_string(),
                            "-1".to_string(),
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
            Arc::new(arrow_schema::Schema::new(fields))
        };
        let col0 = Arc::new(Int64Array::from_iter_values(vec![1; 1024])) as ArrayRef;
        let col1 = Arc::new(StructArray::new(
            vec![
                arrow_schema::Field::new("sub_col", arrow_schema::DataType::Int64, true)
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "-1".to_string(),
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
                    "-1".to_string(),
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
                        "-1".to_string(),
                    )]))]
                    .into(),
                ),
                true,
            )
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "-1".to_string(),
            )]))]
            .into(),
            vec![Arc::new(StructArray::new(
                vec![
                    arrow_schema::Field::new("sub_sub_col", arrow_schema::DataType::Int64, true)
                        .with_metadata(HashMap::from([(
                            PARQUET_FIELD_ID_META_KEY.to_string(),
                            "-1".to_string(),
                        )])),
                ]
                .into(),
                vec![Arc::new(Int64Array::from_iter_values(vec![1; 1024]))],
                None,
            ))],
            None,
        ));
        let to_write =
            RecordBatch::try_new(schema.clone(), vec![col0, col1, col2, col3, col4]).unwrap();

        // prepare writer
        let pb = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            to_write.schema(),
            file_io.clone(),
            location_gen,
            file_name_gen,
        );
        let mut data_file_writer = DataFileWriterBuilder::new(pb).build().await?;

        for _ in 0..3 {
            // write
            data_file_writer.write(to_write.clone()).await?;
            let res = data_file_writer.flush().await?;
            assert_eq!(res.len(), 1);
            let data_file = res
                .into_iter()
                .next()
                .unwrap()
                .partition(Struct::empty())
                .build()
                .unwrap();

            // check
            check_parquet_data_file(&file_io, &data_file, &to_write).await;
        }

        Ok(())
    }
}
