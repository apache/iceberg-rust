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

use arrow_array::RecordBatch;
use itertools::Itertools;

use crate::spec::{DataContentType, DataFile, Struct};
use crate::writer::file_writer::{FileWriter, FileWriterBuilder};
use crate::writer::{CurrentFileStatus, IcebergWriter, IcebergWriterBuilder};
use crate::Result;

/// Builder for `DataFileWriter`.
#[derive(Clone, Debug)]
pub struct DataFileWriterBuilder<B: FileWriterBuilder> {
    inner: B,
    partition_value: Option<Struct>,
}

impl<B: FileWriterBuilder> DataFileWriterBuilder<B> {
    /// Create a new `DataFileWriterBuilder` using a `FileWriterBuilder`.
    pub fn new(inner: B, partition_value: Option<Struct>) -> Self {
        Self {
            inner,
            partition_value,
        }
    }
}

#[async_trait::async_trait]
impl<B: FileWriterBuilder> IcebergWriterBuilder for DataFileWriterBuilder<B> {
    type R = DataFileWriter<B>;

    async fn build(self) -> Result<Self::R> {
        Ok(DataFileWriter {
            inner_writer: Some(self.inner.clone().build().await?),
            partition_value: self.partition_value.unwrap_or(Struct::empty()),
        })
    }
}

/// A writer write data is within one spec/partition.
#[derive(Debug)]
pub struct DataFileWriter<B: FileWriterBuilder> {
    inner_writer: Option<B::R>,
    partition_value: Struct,
}

#[async_trait::async_trait]
impl<B: FileWriterBuilder> IcebergWriter for DataFileWriter<B> {
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.inner_writer.as_mut().unwrap().write(&batch).await
    }

    async fn close(&mut self) -> Result<Vec<DataFile>> {
        let writer = self.inner_writer.take().unwrap();
        Ok(writer
            .close()
            .await?
            .into_iter()
            .map(|mut res| {
                res.content(DataContentType::Data);
                res.partition(self.partition_value.clone());
                res.build().expect("Guaranteed to be valid")
            })
            .collect_vec())
    }
}

impl<B: FileWriterBuilder> CurrentFileStatus for DataFileWriter<B> {
    fn current_file_path(&self) -> String {
        self.inner_writer.as_ref().unwrap().current_file_path()
    }

    fn current_row_num(&self) -> usize {
        self.inner_writer.as_ref().unwrap().current_row_num()
    }

    fn current_written_size(&self) -> usize {
        self.inner_writer.as_ref().unwrap().current_written_size()
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field};
    use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use crate::io::FileIOBuilder;
    use crate::spec::{
        DataContentType, DataFileFormat, Literal, NestedField, PrimitiveType, Schema, Struct, Type,
    };
    use crate::writer::base_writer::data_file_writer::DataFileWriterBuilder;
    use crate::writer::file_writer::location_generator::test::MockLocationGenerator;
    use crate::writer::file_writer::location_generator::DefaultFileNameGenerator;
    use crate::writer::file_writer::ParquetWriterBuilder;
    use crate::writer::{IcebergWriter, IcebergWriterBuilder, RecordBatch};
    use crate::Result;

    #[tokio::test]
    async fn test_parquet_writer() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen =
            MockLocationGenerator::new(temp_dir.path().to_str().unwrap().to_string());
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let schema = Schema::builder()
            .with_schema_id(3)
            .with_fields(vec![
                NestedField::required(3, "foo", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(4, "bar", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()?;

        let pw = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::new(schema),
            file_io.clone(),
            location_gen,
            file_name_gen,
        );

        let mut data_file_writer = DataFileWriterBuilder::new(pw, None).build().await.unwrap();

        let data_files = data_file_writer.close().await.unwrap();
        assert_eq!(data_files.len(), 1);

        let data_file = &data_files[0];
        assert_eq!(data_file.file_format, DataFileFormat::Parquet);
        assert_eq!(data_file.content, DataContentType::Data);
        assert_eq!(data_file.partition, Struct::empty());

        let input_file = file_io.new_input(data_file.file_path.clone())?;
        let input_content = input_file.read().await?;

        let parquet_reader =
            ArrowReaderMetadata::load(&input_content, ArrowReaderOptions::default())
                .expect("Failed to load Parquet metadata");

        let field_ids: Vec<i32> = parquet_reader
            .parquet_schema()
            .columns()
            .iter()
            .map(|col| col.self_type().get_basic_info().id())
            .collect();

        assert_eq!(field_ids, vec![3, 4]);
        Ok(())
    }

    #[tokio::test]
    async fn test_parquet_writer_with_partition() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen =
            MockLocationGenerator::new(temp_dir.path().to_str().unwrap().to_string());
        let file_name_gen = DefaultFileNameGenerator::new(
            "test_partitioned".to_string(),
            None,
            DataFileFormat::Parquet,
        );

        let schema = Schema::builder()
            .with_schema_id(5)
            .with_fields(vec![
                NestedField::required(5, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(6, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()?;

        let partition_value = Struct::from_iter([Some(Literal::int(1))]);

        let parquet_writer_builder = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::new(schema.clone()),
            file_io.clone(),
            location_gen,
            file_name_gen,
        );

        let mut data_file_writer =
            DataFileWriterBuilder::new(parquet_writer_builder, Some(partition_value.clone()))
                .build()
                .await?;

        let arrow_schema = arrow_schema::Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        let batch = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ])?;
        data_file_writer.write(batch).await?;

        let data_files = data_file_writer.close().await.unwrap();
        assert_eq!(data_files.len(), 1);

        let data_file = &data_files[0];
        assert_eq!(data_file.file_format, DataFileFormat::Parquet);
        assert_eq!(data_file.content, DataContentType::Data);
        assert_eq!(data_file.partition, partition_value);

        let input_file = file_io.new_input(data_file.file_path.clone())?;
        let input_content = input_file.read().await?;

        let parquet_reader =
            ArrowReaderMetadata::load(&input_content, ArrowReaderOptions::default())?;

        let field_ids: Vec<i32> = parquet_reader
            .parquet_schema()
            .columns()
            .iter()
            .map(|col| col.self_type().get_basic_info().id())
            .collect();
        assert_eq!(field_ids, vec![5, 6]);

        let field_names: Vec<&str> = parquet_reader
            .parquet_schema()
            .columns()
            .iter()
            .map(|col| col.name())
            .collect();
        assert_eq!(field_names, vec!["id", "name"]);

        Ok(())
    }
}
