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

//! Sort position delete file writer.
use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::SchemaRef as ArrowSchemaRef;
use once_cell::sync::Lazy;

use crate::arrow::schema_to_arrow_schema;
use crate::spec::{DataFile, NestedField, PrimitiveType, Schema, SchemaRef, Struct, Type};
use crate::writer::file_writer::{FileWriter, FileWriterBuilder};
use crate::writer::{IcebergWriter, IcebergWriterBuilder};
use crate::Result;

/// Builder for `MemoryPositionDeleteWriter`.
#[derive(Clone)]
pub struct SortPositionDeleteWriterBuilder<B: FileWriterBuilder> {
    inner: B,
    cache_num: usize,
    partition_value: Option<Struct>,
    partition_spec_id: Option<i32>,
}

impl<B: FileWriterBuilder> SortPositionDeleteWriterBuilder<B> {
    /// Create a new `SortPositionDeleteWriterBuilder` using a `FileWriterBuilder`.
    pub fn new(
        inner: B,
        cache_num: usize,
        partition_value: Option<Struct>,
        partition_spec_id: Option<i32>,
    ) -> Self {
        Self {
            inner,
            cache_num,
            partition_value,
            partition_spec_id,
        }
    }
}

/// Schema for position delete file.
pub static POSITION_DELETE_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    Arc::new(
        Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    2147483546,
                    "file_path",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::required(
                    2147483545,
                    "pos",
                    Type::Primitive(PrimitiveType::Long),
                )),
            ])
            .build()
            .unwrap(),
    )
});

/// Arrow schema for position delete file.
pub static POSITION_DELETE_ARROW_SCHEMA: Lazy<ArrowSchemaRef> =
    Lazy::new(|| Arc::new(schema_to_arrow_schema(&POSITION_DELETE_SCHEMA).unwrap()));

#[async_trait::async_trait]
impl<B: FileWriterBuilder> IcebergWriterBuilder<PositionDeleteInput, Vec<DataFile>>
    for SortPositionDeleteWriterBuilder<B>
{
    type R = SortPositionDeleteWriter<B>;

    async fn build(self) -> Result<Self::R> {
        Ok(SortPositionDeleteWriter {
            inner_writer_builder: self.inner.clone(),
            cache_num: self.cache_num,
            cache: BTreeMap::new(),
            data_files: Vec::new(),
            partition_value: self.partition_value.unwrap_or(Struct::empty()),
            partition_spec_id: self.partition_spec_id.unwrap_or(0),
        })
    }
}

/// Position delete input.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct PositionDeleteInput {
    /// The path of the file.
    pub path: String,
    /// The offset of the position delete.
    pub offset: i64,
}

/// The memory position delete writer.
pub struct SortPositionDeleteWriter<B: FileWriterBuilder> {
    inner_writer_builder: B,
    cache_num: usize,
    cache: BTreeMap<String, Vec<i64>>,
    data_files: Vec<DataFile>,
    partition_value: Struct,
    partition_spec_id: i32,
}

impl<B: FileWriterBuilder> SortPositionDeleteWriter<B> {
    /// Get the current number of cache rows.
    pub fn current_cache_number(&self) -> usize {
        self.cache.len()
    }
}

impl<B: FileWriterBuilder> SortPositionDeleteWriter<B> {
    async fn write_cache_out(&mut self) -> Result<()> {
        let mut keys = Vec::new();
        let mut values = Vec::new();
        let mut cache = std::mem::take(&mut self.cache);
        for (key, offsets) in cache.iter_mut() {
            offsets.sort();
            let key_ref = key.as_str();
            for offset in offsets {
                keys.push(key_ref);
                values.push(*offset);
            }
        }
        let key_array = Arc::new(StringArray::from(keys)) as ArrayRef;
        let value_array = Arc::new(Int64Array::from(values)) as ArrayRef;
        let record_batch = RecordBatch::try_new(POSITION_DELETE_ARROW_SCHEMA.clone(), vec![
            key_array,
            value_array,
        ])?;
        let mut writer = self.inner_writer_builder.clone().build().await?;
        writer.write(&record_batch).await?;
        self.data_files
            .extend(writer.close().await?.into_iter().map(|mut res| {
                res.content(crate::spec::DataContentType::PositionDeletes);
                res.partition(self.partition_value.clone());
                res.partition_spec_id(self.partition_spec_id);
                res.build().expect("Guaranteed to be valid")
            }));
        Ok(())
    }
}

/// Implement `IcebergWriter` for `PositionDeleteWriter`.
#[async_trait::async_trait]
impl<B: FileWriterBuilder> IcebergWriter<PositionDeleteInput> for SortPositionDeleteWriter<B> {
    async fn write(&mut self, input: PositionDeleteInput) -> Result<()> {
        if let Some(v) = self.cache.get_mut(&input.path) {
            v.push(input.offset);
        } else {
            self.cache
                .insert(input.path.to_string(), vec![input.offset]);
        }

        if self.cache.len() >= self.cache_num {
            self.write_cache_out().await?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<Vec<DataFile>> {
        self.write_cache_out().await?;
        Ok(std::mem::take(&mut self.data_files))
    }
}

#[cfg(test)]
mod test {
    use arrow_array::{Int64Array, StringArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use super::POSITION_DELETE_SCHEMA;
    use crate::io::FileIOBuilder;
    use crate::spec::{DataContentType, DataFileFormat, Struct};
    use crate::writer::base_writer::sort_position_delete_writer::{
        PositionDeleteInput, SortPositionDeleteWriterBuilder,
    };
    use crate::writer::file_writer::location_generator::test::MockLocationGenerator;
    use crate::writer::file_writer::location_generator::DefaultFileNameGenerator;
    use crate::writer::file_writer::ParquetWriterBuilder;
    use crate::writer::{IcebergWriter, IcebergWriterBuilder};
    use crate::Result;

    #[tokio::test]
    async fn test_position_delete_writer() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let location_gen =
            MockLocationGenerator::new(temp_dir.path().to_str().unwrap().to_string());
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let pw = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            POSITION_DELETE_SCHEMA.clone(),
            file_io.clone(),
            location_gen,
            file_name_gen,
        );
        let mut position_delete_writer = SortPositionDeleteWriterBuilder::new(pw, 10, None, None)
            .build()
            .await?;

        // Write some position delete inputs
        let mut inputs = [
            PositionDeleteInput {
                path: "file2.parquet".to_string(),
                offset: 2,
            },
            PositionDeleteInput {
                path: "file2.parquet".to_string(),
                offset: 1,
            },
            PositionDeleteInput {
                path: "file2.parquet".to_string(),
                offset: 3,
            },
            PositionDeleteInput {
                path: "file3.parquet".to_string(),
                offset: 2,
            },
            PositionDeleteInput {
                path: "file1.parquet".to_string(),
                offset: 5,
            },
            PositionDeleteInput {
                path: "file1.parquet".to_string(),
                offset: 4,
            },
            PositionDeleteInput {
                path: "file1.parquet".to_string(),
                offset: 1,
            },
        ];
        for input in inputs.iter() {
            position_delete_writer.write(input.clone()).await?;
        }

        let data_files = position_delete_writer.close().await.unwrap();
        assert_eq!(data_files.len(), 1);
        assert_eq!(data_files[0].file_format, DataFileFormat::Parquet);
        assert_eq!(data_files[0].content, DataContentType::PositionDeletes);
        assert_eq!(data_files[0].partition, Struct::empty());

        let parquet_file = file_io
            .new_input(&data_files[0].file_path)?
            .read()
            .await
            .unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(parquet_file).unwrap();
        let reader = builder.build().unwrap();
        let batches = reader.map(|x| x.unwrap()).collect::<Vec<_>>();

        let path_column = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let offset_column = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        inputs.sort_by(|a, b| a.path.cmp(&b.path).then_with(|| a.offset.cmp(&b.offset)));
        for (i, input) in inputs.iter().enumerate() {
            assert_eq!(path_column.value(i), input.path);
            assert_eq!(offset_column.value(i), input.offset);
        }

        Ok(())
    }
}
