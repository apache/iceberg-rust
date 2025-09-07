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

use arrow_array::RecordBatch;

use crate::io::FileIO;
use crate::spec::{DataFile, PartitionKey};
use crate::writer::file_writer::location_generator::{FileNameGenerator, LocationGenerator};
use crate::writer::{CurrentFileStatus, IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind, Result};

/// A writer that automatically rolls over to a new file when the data size
/// exceeds a target threshold.
///
/// This writer wraps another writer that tracks the amount of data written.
/// When the data size exceeds the target size, it closes the current file and
/// starts writing to a new one.
pub struct RollingWriter<B, L, F>
where
    B: IcebergWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
{
    inner: Option<B::R>,
    inner_builder: B,
    target_file_size: usize,
    location_generator: L,
    file_name_generator: F,
    file_io: FileIO,
    partition_key: Option<PartitionKey>,
    data_files: Vec<DataFile>, // todo this should be B::R::O? DefaultOutput?
}

impl<B, L, F> RollingWriter<B, L, F>
where
    B: IcebergWriterBuilder,
    B::R: CurrentFileStatus,
    L: LocationGenerator,
    F: FileNameGenerator,
{
    /// Creates a new `RollingWriter` with the specified inner builder and target size.
    ///
    /// # Arguments
    ///
    /// * `inner_builder` - The builder for the underlying writer
    /// * `target_file_size` - The target size in bytes before rolling over to a new file
    /// * `location_generator` - The location generator to use for generating file paths
    /// * `file_name_generator` - The file name generator to use for generating file names
    /// * `file_io` - The file IO to use for creating new files
    /// * `partition_key` - The partition key for the files
    ///
    /// NOTE: The `target_file_size` does not exactly reflect the final size on physical storage.
    /// This is because the input size is based on the Arrow in-memory format and cannot precisely control rollover behavior.
    /// The actual file size on disk is expected to be slightly larger than `target_file_size`.
    pub fn new(
        inner_builder: B,
        target_file_size: usize,
        location_generator: L,
        file_name_generator: F,
        file_io: FileIO,
        partition_key: Option<PartitionKey>,
    ) -> Self {
        Self {
            inner: None,
            inner_builder,
            target_file_size,
            location_generator,
            file_name_generator,
            file_io,
            partition_key,
            data_files: Vec::new(),
        }
    }

    /// Determines if the writer should roll over to a new file.
    ///
    /// # Returns
    ///
    /// `true` if a new file should be started, `false` otherwise
    fn should_roll(&self) -> bool {
        self.current_written_size() > self.target_file_size
    }

    /// Create a new writer for the current partition.
    async fn create_new_writer(&mut self) -> Result<B::R> {
        let file_path = self.location_generator.generate_location(
            self.partition_key.as_ref(),
            &self.file_name_generator.generate_file_name(),
        );

        let output_file = self.file_io.new_output(file_path)?;
        let writer = self.inner_builder.clone().build(output_file).await?;

        Ok(writer)
    }

    /// Write a record batch to the current file, rolling over to a new file if necessary.
    ///
    /// # Arguments
    ///
    /// * `batch` - The record batch to write
    pub async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        if self.inner.is_none() {
            // initialize inner writer
            self.inner = Some(self.create_new_writer().await?);
        }

        if self.should_roll() {
            if let Some(mut inner) = self.inner.take() {
                // close the current writer, roll to a new file
                self.data_files.extend(inner.close().await?);

                // start a new writer
                self.inner = Some(self.create_new_writer().await?);
            }
        }

        // write the input
        if let Some(writer) = &mut self.inner {
            writer.write(batch).await
        } else {
            Err(Error::new(
                ErrorKind::Unexpected,
                "Writer is not initialized!",
            ))
        }
    }

    /// Close the writer and return the data files for all files.
    pub async fn close(&mut self) -> Result<Vec<DataFile>> {
        // close the current writer and merge the output
        if let Some(mut current_writer) = self.inner.take() {
            self.data_files.extend(current_writer.close().await?);
        }

        Ok(std::mem::take(&mut self.data_files))
    }
}

impl<B, L, F> CurrentFileStatus for RollingWriter<B, L, F>
where
    B: IcebergWriterBuilder,
    B::R: IcebergWriter + CurrentFileStatus,
    L: LocationGenerator,
    F: FileNameGenerator,
{
    fn current_file_path(&self) -> String {
        if let Some(inner) = &self.inner {
            inner.current_file_path()
        } else {
            "".to_string()
        }
    }

    fn current_row_num(&self) -> usize {
        if let Some(inner) = &self.inner {
            inner.current_row_num()
        } else {
            0
        }
    }

    fn current_written_size(&self) -> usize {
        if let Some(inner) = &self.inner {
            inner.current_written_size()
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet::file::properties::WriterProperties;
    use rand::prelude::IteratorRandom;
    use tempfile::TempDir;

    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::{DataFileFormat, NestedField, PrimitiveType, Schema, Type};
    use crate::writer::base_writer::data_file_writer::DataFileWriterBuilder;
    use crate::writer::file_writer::ParquetWriterBuilder;
    use crate::writer::file_writer::location_generator::DefaultFileNameGenerator;
    use crate::writer::file_writer::location_generator::test::MockLocationGenerator;
    use crate::writer::tests::check_parquet_data_file;
    use crate::writer::{IcebergWriter, IcebergWriterBuilder, RecordBatch};

    fn make_test_schema() -> Result<Schema> {
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
    }

    fn make_test_arrow_schema() -> ArrowSchema {
        ArrowSchema::new(vec![
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

    #[tokio::test]
    async fn test_rolling_writer_basic() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_io = FileIOBuilder::new_fs_io().build()?;
        let location_gen =
            MockLocationGenerator::new(temp_dir.path().to_str().unwrap().to_string());
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // Create schema
        let schema = make_test_schema()?;

        // Create writer builders
        let parquet_writer_builder = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::new(schema),
            None,
            file_io.clone(),
            location_gen,
            file_name_gen,
        );

        // Set a large target size so no rolling occurs
        let rolling_writer_builder = RollingFileWriterBuilder::new(
            parquet_writer_builder,
            1024 * 1024, // 1MB, large enough to not trigger rolling
        );

        let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder, None, 0);

        // Create writer
        let mut writer = data_file_writer_builder.build().await?;

        // Create test data
        let arrow_schema = make_test_arrow_schema();

        let batch = RecordBatch::try_new(Arc::new(arrow_schema), vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ])?;

        // Write data
        writer.write(batch.clone()).await?;

        // Close writer and get data files
        let data_files = writer.close().await?;

        // Verify only one file was created
        assert_eq!(
            data_files.len(),
            1,
            "Expected only one data file to be created"
        );

        // Verify file content
        check_parquet_data_file(&file_io, &data_files[0], &batch).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_rolling_writer_with_rolling() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_io = FileIOBuilder::new_fs_io().build()?;
        let location_gen =
            MockLocationGenerator::new(temp_dir.path().to_str().unwrap().to_string());
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // Create schema
        let schema = make_test_schema()?;

        // Create writer builders
        let parquet_writer_builder = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::new(schema),
            None,
            file_io.clone(),
            location_gen,
            file_name_gen,
        );

        // Set a very small target size to trigger rolling
        let rolling_writer_builder = RollingFileWriterBuilder::new(parquet_writer_builder, 1024);

        let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder, None, 0);

        // Create writer
        let mut writer = data_file_writer_builder.build().await?;

        // Create test data
        let arrow_schema = make_test_arrow_schema();
        let arrow_schema_ref = Arc::new(arrow_schema.clone());

        let names = vec![
            "Alice", "Bob", "Charlie", "Dave", "Eve", "Frank", "Grace", "Heidi", "Ivan", "Judy",
            "Kelly", "Larry", "Mallory", "Shawn",
        ];

        let mut rng = rand::thread_rng();
        let batch_num = 10;
        let batch_rows = 100;
        let expected_rows = batch_num * batch_rows;

        for i in 0..batch_num {
            let int_values: Vec<i32> = (0..batch_rows).map(|row| i * batch_rows + row).collect();
            let str_values: Vec<&str> = (0..batch_rows)
                .map(|_| *names.iter().choose(&mut rng).unwrap())
                .collect();

            let int_array = Arc::new(Int32Array::from(int_values)) as ArrayRef;
            let str_array = Arc::new(StringArray::from(str_values)) as ArrayRef;

            let batch =
                RecordBatch::try_new(Arc::clone(&arrow_schema_ref), vec![int_array, str_array])
                    .expect("Failed to create RecordBatch");

            writer.write(batch).await?;
        }

        // Close writer and get data files
        let data_files = writer.close().await?;

        // Verify multiple files were created (at least 4)
        assert!(
            data_files.len() > 4,
            "Expected at least 4 data files to be created, but got {}",
            data_files.len()
        );

        // Verify total record count across all files
        let total_records: u64 = data_files.iter().map(|file| file.record_count).sum();
        assert_eq!(
            total_records, expected_rows as u64,
            "Expected {} total records across all files",
            expected_rows
        );

        Ok(())
    }
}
