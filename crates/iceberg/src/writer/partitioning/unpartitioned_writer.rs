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

//! This module provides the `UnpartitionedWriter` implementation.

use std::marker::PhantomData;

use crate::Result;
use crate::writer::{DefaultInput, DefaultOutput, IcebergWriter, IcebergWriterBuilder};

/// A simple wrapper around `IcebergWriterBuilder` for unpartitioned tables.
///
/// This writer lazily creates the underlying writer on the first write operation
/// and writes all data to a single file (or set of files if rolling).
///
/// # Type Parameters
///
/// * `B` - The inner writer builder type
/// * `I` - Input type (defaults to `RecordBatch`)
/// * `O` - Output collection type (defaults to `Vec<DataFile>`)
pub struct UnpartitionedWriter<B, I = DefaultInput, O = DefaultOutput>
where
    B: IcebergWriterBuilder<I, O>,
    O: IntoIterator + FromIterator<<O as IntoIterator>::Item>,
    <O as IntoIterator>::Item: Clone,
{
    inner_builder: B,
    writer: Option<B::R>,
    output: Vec<<O as IntoIterator>::Item>,
    _phantom: PhantomData<I>,
}

impl<B, I, O> UnpartitionedWriter<B, I, O>
where
    B: IcebergWriterBuilder<I, O>,
    I: Send + 'static,
    O: IntoIterator + FromIterator<<O as IntoIterator>::Item>,
    <O as IntoIterator>::Item: Send + Clone,
{
    /// Create a new `UnpartitionedWriter`.
    pub fn new(inner_builder: B) -> Self {
        Self {
            inner_builder,
            writer: None,
            output: Vec::new(),
            _phantom: PhantomData,
        }
    }

    /// Write data to the writer.
    ///
    /// The underlying writer is lazily created on the first write operation.
    ///
    /// # Parameters
    ///
    /// * `input` - The input data to write
    ///
    /// # Returns
    ///
    /// `Ok(())` on success, or an error if the write operation fails.
    pub async fn write(&mut self, input: I) -> Result<()> {
        // Lazily create writer on first write
        if self.writer.is_none() {
            self.writer = Some(self.inner_builder.clone().build(None).await?);
        }

        // Write directly to inner writer
        self.writer
            .as_mut()
            .expect("Writer should be initialized")
            .write(input)
            .await
    }

    /// Close the writer and return all written data files.
    ///
    /// This method consumes the writer to prevent further use.
    ///
    /// # Returns
    ///
    /// The accumulated output from all write operations, or an empty collection
    /// if no data was written.
    pub async fn close(mut self) -> Result<O> {
        if let Some(mut writer) = self.writer.take() {
            self.output.extend(writer.close().await?);
        }
        Ok(O::from_iter(self.output))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use super::*;
    use crate::Result;
    use crate::io::FileIOBuilder;
    use crate::spec::{
        DataFileFormat, NestedField, PrimitiveType, Struct,
        Type,
    };
    use crate::writer::base_writer::data_file_writer::DataFileWriterBuilder;
    use crate::writer::file_writer::ParquetWriterBuilder;
    use crate::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;

    /// Helper function to create a test writer setup with common configuration
    fn create_test_writer_builder(
        temp_dir: &TempDir,
        schema: Arc<crate::spec::Schema>,
    ) -> Result<impl IcebergWriterBuilder> {
        let file_io = FileIOBuilder::new_fs_io().build()?;
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let parquet_writer_builder =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), schema);
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            file_io,
            location_gen,
            file_name_gen,
        );

        Ok(DataFileWriterBuilder::new(rolling_writer_builder))
    }

    /// Helper function to create a simple test schema
    fn create_simple_schema() -> Result<Arc<crate::spec::Schema>> {
        Ok(Arc::new(
            crate::spec::Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()?,
        ))
    }

    /// Helper function to create a schema with a region partition field
    fn create_schema_with_region() -> Result<Arc<crate::spec::Schema>> {
        Ok(Arc::new(
            crate::spec::Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(3, "region", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])
                .build()?,
        ))
    }

    /// Helper function to create Arrow schema with field IDs for simple schema
    fn create_arrow_schema_simple() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                1.to_string(),
            )])),
            Field::new("name", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                2.to_string(),
            )])),
        ])
    }

    /// Helper function to create Arrow schema with field IDs including region
    fn create_arrow_schema_with_region() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                1.to_string(),
            )])),
            Field::new("name", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                2.to_string(),
            )])),
            Field::new("region", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                3.to_string(),
            )])),
        ])
    }

    #[tokio::test]
    async fn test_unpartitioned_writer_basic_functionality() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let schema = create_simple_schema()?;
        let data_file_writer_builder = create_test_writer_builder(&temp_dir, schema.clone())?;

        // Create unpartitioned writer
        let mut writer = UnpartitionedWriter::new(data_file_writer_builder);

        // Create test data
        let arrow_schema = create_arrow_schema_simple();
        let batch1 = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
        ])?;

        let batch2 = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
            Arc::new(Int32Array::from(vec![3, 4])),
            Arc::new(StringArray::from(vec!["Charlie", "Dave"])),
        ])?;

        // Write data without partition keys
        writer.write(batch1).await?;
        writer.write(batch2).await?;

        // Close writer and get data files
        let data_files = writer.close().await?;

        // Verify at least one file was created
        assert!(
            !data_files.is_empty(),
            "Expected at least one data file to be created"
        );

        // Verify that all data files have empty partition value (unpartitioned)
        let partition_value = Struct::empty();
        for data_file in &data_files {
            assert_eq!(data_file.partition, partition_value);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_unpartitioned_writer_writes_all_data_together() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let schema = create_schema_with_region()?;
        let data_file_writer_builder = create_test_writer_builder(&temp_dir, schema.clone())?;

        // Create unpartitioned writer
        let mut writer = UnpartitionedWriter::new(data_file_writer_builder);

        // Create test data with different regions
        let arrow_schema = create_arrow_schema_with_region();
        let batch_us = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            Arc::new(StringArray::from(vec!["US", "US"])),
        ])?;

        let batch_eu = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
            Arc::new(Int32Array::from(vec![3, 4])),
            Arc::new(StringArray::from(vec!["Charlie", "Dave"])),
            Arc::new(StringArray::from(vec!["EU", "EU"])),
        ])?;

        // Write data from different regions - all goes to same file(s)
        writer.write(batch_us).await?;
        writer.write(batch_eu).await?;

        // Close writer and get data files
        let data_files = writer.close().await?;

        // Verify at least one file was created
        assert!(
            !data_files.is_empty(),
            "Expected at least one data file to be created"
        );

        // All data should be written to the same file(s) with empty partition
        for data_file in &data_files {
            assert_eq!(
                data_file.partition,
                Struct::empty(),
                "Expected empty partition for unpartitioned writer"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_unpartitioned_writer_lazy_initialization() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let schema = create_simple_schema()?;
        let data_file_writer_builder = create_test_writer_builder(&temp_dir, schema.clone())?;

        // Create unpartitioned writer - writer should not be initialized yet
        let mut writer = UnpartitionedWriter::new(data_file_writer_builder);

        // Verify writer is None before first write
        assert!(
            writer.writer.is_none(),
            "Writer should not be initialized before first write"
        );

        // Create test data
        let arrow_schema = create_arrow_schema_simple();
        let batch = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
        ])?;

        // Write data - this should trigger lazy initialization
        writer.write(batch).await?;

        // Verify writer is now initialized
        assert!(
            writer.writer.is_some(),
            "Writer should be initialized after first write"
        );

        // Close writer
        let data_files = writer.close().await?;

        // Verify file was created
        assert!(!data_files.is_empty(), "Expected at least one data file");

        Ok(())
    }

    #[tokio::test]
    async fn test_unpartitioned_writer_close_returns_correct_data_files() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let schema = create_simple_schema()?;
        let data_file_writer_builder = create_test_writer_builder(&temp_dir, schema.clone())?;

        // Create unpartitioned writer
        let mut writer = UnpartitionedWriter::new(data_file_writer_builder);

        // Create test data
        let arrow_schema = create_arrow_schema_simple();
        let batch = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ])?;

        // Write data
        writer.write(batch).await?;

        // Close writer and get data files
        let data_files = writer.close().await?;

        // Verify data files were returned
        assert!(!data_files.is_empty(), "Expected at least one data file");

        // Verify each data file has correct properties
        let partition_value = Struct::empty();
        for data_file in &data_files {
            // Check partition is empty (unpartitioned)
            assert_eq!(data_file.partition, partition_value);

            // Check file format is Parquet
            assert_eq!(data_file.file_format, DataFileFormat::Parquet);

            // Check file path is not empty
            assert!(
                !data_file.file_path.is_empty(),
                "File path should not be empty"
            );

            // Check record count is positive
            assert!(
                data_file.record_count > 0,
                "Record count should be positive"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_unpartitioned_writer_close_without_writes() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let schema = create_simple_schema()?;
        let data_file_writer_builder = create_test_writer_builder(&temp_dir, schema.clone())?;

        // Create unpartitioned writer
        let writer = UnpartitionedWriter::new(data_file_writer_builder);

        // Close writer without writing any data
        let data_files = writer.close().await?;

        // Verify no data files were created
        assert!(
            data_files.is_empty(),
            "Expected no data files when closing without writes"
        );

        Ok(())
    }
}
