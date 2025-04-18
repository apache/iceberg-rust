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

//! This module contains the fanout partition writer.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_row::OwnedRow;
use arrow_schema::SchemaRef as ArrowSchemaRef;
use itertools::Itertools;

use crate::arrow::{schema_to_arrow_schema, RecordBatchPartitionSpliter};
use crate::spec::{DataFile, PartitionSpecRef, SchemaRef};
use crate::writer::{IcebergWriter, IcebergWriterBuilder};
use crate::Result;

/// The builder for `FanoutPartitionWriter`.
#[derive(Clone)]
pub struct FanoutPartitionWriterBuilder<B> {
    inner_builder: B,
    partition_specs: PartitionSpecRef,
    table_schema: SchemaRef,
    arrow_schema: ArrowSchemaRef,
}

impl<B> FanoutPartitionWriterBuilder<B> {
    /// Create a new `FanoutPartitionWriterBuilder` with the default arrow schema.
    pub fn new(
        inner_builder: B,
        partition_specs: PartitionSpecRef,
        table_schema: SchemaRef,
    ) -> Result<Self> {
        Ok(Self::new_with_custom_schema(
            inner_builder,
            Arc::new(schema_to_arrow_schema(&table_schema)?),
            partition_specs,
            table_schema,
        ))
    }

    /// Create a new `FanoutPartitionWriterBuilder` with a custom arrow schema.
    /// This function is useful for the user who has the input with extral columns.
    pub fn new_with_custom_schema(
        inner_builder: B,
        arrow_schema: ArrowSchemaRef,
        partition_specs: PartitionSpecRef,
        table_schema: SchemaRef,
    ) -> Self {
        Self {
            inner_builder,
            partition_specs,
            table_schema,
            arrow_schema,
        }
    }
}

#[async_trait::async_trait]
impl<B: IcebergWriterBuilder> IcebergWriterBuilder for FanoutPartitionWriterBuilder<B> {
    type R = FanoutPartitionWriter<B>;

    async fn build(self) -> Result<Self::R> {
        let partition_splitter = RecordBatchPartitionSpliter::new(
            &self.arrow_schema,
            self.table_schema.clone(),
            self.partition_specs,
        )?;
        Ok(FanoutPartitionWriter {
            inner_writer_builder: self.inner_builder,
            partition_splitter,
            partition_writers: HashMap::new(),
        })
    }
}

/// The fanout partition writer.
/// It will split the input record batch by the partition specs, and write the splitted record batches to the inner writers.
pub struct FanoutPartitionWriter<B: IcebergWriterBuilder> {
    inner_writer_builder: B,
    partition_splitter: RecordBatchPartitionSpliter,
    partition_writers: HashMap<OwnedRow, B::R>,
}

impl<B: IcebergWriterBuilder> FanoutPartitionWriter<B> {
    /// Get the current number of partition writers.
    pub fn partition_num(&self) -> usize {
        self.partition_writers.len()
    }
}

#[async_trait::async_trait]
impl<B: IcebergWriterBuilder> IcebergWriter for FanoutPartitionWriter<B> {
    async fn write(&mut self, input: RecordBatch) -> Result<()> {
        let splits = self.partition_splitter.split(&input)?;

        for (partition, record_batch) in splits {
            match self.partition_writers.entry(partition) {
                Entry::Occupied(entry) => {
                    entry.into_mut().write(record_batch).await?;
                }
                Entry::Vacant(entry) => {
                    let writer = entry.insert(self.inner_writer_builder.clone().build().await?);
                    writer.write(record_batch).await?;
                }
            }
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<Vec<DataFile>> {
        let (partition_rows, writers): (Vec<_>, Vec<_>) = self.partition_writers.drain().unzip();
        let partition_values = self.partition_splitter.convert_row(partition_rows)?;

        let mut result = Vec::new();
        for (partition_value, mut writer) in partition_values.into_iter().zip_eq(writers) {
            let mut data_files = writer.close().await?;
            for data_file in data_files.iter_mut() {
                data_file.rewrite_partition(partition_value.clone());
                data_file.rewrite_partition_id(self.partition_splitter.partition_spec().spec_id());
            }
            result.append(&mut data_files);
        }

        Ok(result)
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use arrow_select::concat::concat_batches;
    use itertools::Itertools;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use crate::io::FileIOBuilder;
    use crate::spec::{
        DataFileFormat, Literal, NestedField, PartitionSpec, PrimitiveLiteral, PrimitiveType,
        Schema, Struct, Transform, Type, UnboundPartitionField,
    };
    use crate::writer::base_writer::data_file_writer::DataFileWriterBuilder;
    use crate::writer::file_writer::location_generator::test::MockLocationGenerator;
    use crate::writer::file_writer::location_generator::DefaultFileNameGenerator;
    use crate::writer::file_writer::ParquetWriterBuilder;
    use crate::writer::function_writer::fanout_partition_writer::FanoutPartitionWriterBuilder;
    use crate::writer::{IcebergWriter, IcebergWriterBuilder};
    use crate::Result;

    #[tokio::test]
    async fn test_fanout_partition_writer() -> Result<()> {
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
        let partition_spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(1)
            .add_unbound_field(UnboundPartitionField {
                source_id: 1,
                field_id: None,
                name: "id_bucket".to_string(),
                transform: Transform::Identity,
            })
            .unwrap()
            .build()
            .unwrap();
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let location_gen =
            MockLocationGenerator::new(temp_dir.path().to_str().unwrap().to_string());
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);
        let pw = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            schema.clone(),
            file_io.clone(),
            location_gen,
            file_name_gen,
        );
        let data_file_writer_builder = DataFileWriterBuilder::new(pw, None, 0);
        let mut fanout_partition_writer = FanoutPartitionWriterBuilder::new(
            data_file_writer_builder,
            Arc::new(partition_spec),
            schema,
        )
        .unwrap()
        .build()
        .await
        .unwrap();

        // prepare data
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                1.to_string(),
            )])),
            Field::new("data", DataType::Utf8, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                2.to_string(),
            )])),
        ]));
        let id_array = Int64Array::from(vec![1, 2, 1, 3, 2, 3, 1]);
        let data_array = StringArray::from(vec!["a", "b", "c", "d", "e", "f", "g"]);
        let batch = RecordBatch::try_new(schema.clone(), vec![
            Arc::new(id_array),
            Arc::new(data_array),
        ])
        .expect("Failed to create RecordBatch");

        fanout_partition_writer.write(batch).await?;
        let data_files = fanout_partition_writer.close().await?;
        assert_eq!(data_files.len(), 3);
        let expected_partitions = vec![
            Struct::from_iter(vec![Some(Literal::Primitive(PrimitiveLiteral::Long(1)))]),
            Struct::from_iter(vec![Some(Literal::Primitive(PrimitiveLiteral::Long(2)))]),
            Struct::from_iter(vec![Some(Literal::Primitive(PrimitiveLiteral::Long(3)))]),
        ];
        let expected_batches = vec![
            RecordBatch::try_new(schema.clone(), vec![
                Arc::new(Int64Array::from(vec![1, 1, 1])),
                Arc::new(StringArray::from(vec!["a", "c", "g"])),
            ])
            .unwrap(),
            RecordBatch::try_new(schema.clone(), vec![
                Arc::new(Int64Array::from(vec![2, 2])),
                Arc::new(StringArray::from(vec!["b", "e"])),
            ])
            .unwrap(),
            RecordBatch::try_new(schema.clone(), vec![
                Arc::new(Int64Array::from(vec![3, 3])),
                Arc::new(StringArray::from(vec!["d", "f"])),
            ])
            .unwrap(),
        ];
        for (partition, batch) in expected_partitions
            .into_iter()
            .zip_eq(expected_batches.into_iter())
        {
            assert!(data_files.iter().any(|file| file.partition == partition));
            let data_file = data_files
                .iter()
                .find(|file| file.partition == partition)
                .unwrap();
            let input_file = file_io.new_input(data_file.file_path.clone()).unwrap();
            let input_content = input_file.read().await.unwrap();
            let reader_builder =
                ParquetRecordBatchReaderBuilder::try_new(input_content.clone()).unwrap();

            // check data
            let reader = reader_builder.build().unwrap();
            let batches = reader.map(|batch| batch.unwrap()).collect::<Vec<_>>();
            let res = concat_batches(&batch.schema(), &batches).unwrap();
            assert_eq!(batch, res);
        }

        Ok(())
    }
}
