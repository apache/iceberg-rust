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
use num_cpus;
use tokio::task::JoinSet;

use crate::spec::DataFileBuilder;
use crate::writer::CurrentFileStatus;
use crate::writer::file_writer::{FileWriter, FileWriterBuilder};
use crate::{Error, ErrorKind, Result};

/// Builder for creating a `RollingFileWriter` that rolls over to a new file
/// when the data size exceeds a target threshold.
/// max_concurrent_close_tasks is the max number of file writers to close in parallel running in the background.
#[derive(Clone)]
pub struct RollingFileWriterBuilder<B: FileWriterBuilder> {
    inner_builder: B,
    target_file_size: usize,
    max_concurrent_close_tasks: usize,
}

impl<B: FileWriterBuilder> RollingFileWriterBuilder<B> {
    /// Creates a new `RollingFileWriterBuilder` with the specified inner builder and target size.
    /// By default, this builder is configured to close one file at a time.
    /// To enable parallel file closing, use the `with_concurrency` constructor.
    ///
    /// # Arguments
    ///
    /// * `inner_builder` - The builder for the underlying file writer
    /// * `target_file_size` - The target size in bytes before rolling over to a new file
    ///
    /// NOTE: The `target_file_size` does not exactly reflect the final size on physical storage.
    /// This is because the input size is based on the Arrow in-memory format and cannot precisely control rollover behavior.
    /// The actual file size on disk is expected to be slightly larger than `target_file_size`.
    pub fn new(inner_builder: B, target_file_size: usize) -> Self {
        Self::with_concurrency(inner_builder, target_file_size, Some(1))
    }

    /// Creates a new `RollingFileWriterBuilder` with the specified concurrency for closing file writers in parallel.
    ///
    /// # Arguments
    ///
    /// * `inner_builder` - The builder for the underlying file writer
    /// * `target_file_size` - The target size in bytes before rolling over to a new file
    /// * `max_concurrent_close_tasks` - The max number of file writers to close in parallel running in the background.
    ///   If not provided, it will use the number of CPU cores as the default with a maximum of 4.
    pub fn with_concurrency(
        inner_builder: B,
        target_file_size: usize,
        max_concurrent_close_tasks: Option<usize>,
    ) -> Self {
        let default_concurrency: usize = num_cpus::get().clamp(1, 4);
        let max_concurrent_close_tasks: usize =
            max_concurrent_close_tasks.unwrap_or(default_concurrency);
        Self {
            inner_builder,
            target_file_size,
            max_concurrent_close_tasks,
        }
    }
}

impl<B: FileWriterBuilder> FileWriterBuilder for RollingFileWriterBuilder<B> {
    type R = RollingFileWriter<B>;

    async fn build(self) -> Result<Self::R> {
        Ok(RollingFileWriter {
            inner: None,
            inner_builder: self.inner_builder,
            target_file_size: self.target_file_size,
            data_file_builders: vec![],
            max_concurrent_close_tasks: self.max_concurrent_close_tasks,
            closing_tasks: JoinSet::new(),
        })
    }
}

/// A writer that automatically rolls over to a new file when the data size
/// exceeds a target threshold.
///
/// This writer wraps another file writer that tracks the amount of data written.
/// When the data size exceeds the target size, it closes the current file and
/// starts writing to a new one.
///
/// max_concurrent_close_tasks is the max number of file writers to close in parallel.
/// closing_tasks is a set of tasks that are closing the file writers.
pub struct RollingFileWriter<B: FileWriterBuilder> {
    inner: Option<B::R>,
    inner_builder: B,
    target_file_size: usize,
    data_file_builders: Vec<DataFileBuilder>,
    max_concurrent_close_tasks: usize,
    closing_tasks: JoinSet<Result<Vec<DataFileBuilder>>>,
}

impl<B: FileWriterBuilder> RollingFileWriter<B> {
    /// Determines if the writer should roll over to a new file.
    ///
    /// # Returns
    ///
    /// `true` if a new file should be started, `false` otherwise
    fn should_roll(&self) -> bool {
        if let Some(writer) = &self.inner {
            writer.current_written_size() > self.target_file_size
        } else {
            false
        }
    }
}

impl<B: FileWriterBuilder> FileWriter for RollingFileWriter<B> {
    async fn write(&mut self, input: &RecordBatch) -> Result<()> {
        if self.inner.is_none() {
            // initialize inner writer
            self.inner = Some(self.inner_builder.clone().build().await?);
        }

        if self.should_roll() {
            if let Some(inner) = self.inner.take() {
                if self.closing_tasks.len() >= self.max_concurrent_close_tasks {
                    // Wait for one task to close (non-blocking rollover)
                    if let Some(result) = self.closing_tasks.join_next().await {
                        match result {
                            Ok(Ok(builders)) => self.data_file_builders.extend(builders),
                            Ok(Err(e)) => return Err(e),
                            Err(e) => {
                                return Err(Error::new(
                                    ErrorKind::Unexpected,
                                    "Closing task panicked inside the writer",
                                )
                                .with_source(e));
                            }
                        }
                    }
                }

                // Spawn background close task
                self.closing_tasks.spawn(async move { inner.close().await });

                // start a new writer
                self.inner = Some(self.inner_builder.clone().build().await?);
            }
        }

        // write the input
        if let Some(writer) = self.inner.as_mut() {
            Ok(writer.write(input).await?)
        } else {
            Err(Error::new(
                ErrorKind::Unexpected,
                "Writer is not initialized!",
            ))
        }
    }

    async fn close(mut self) -> Result<Vec<DataFileBuilder>> {
        // Close current writer
        if let Some(current_writer) = self.inner {
            self.data_file_builders
                .extend(current_writer.close().await?);
        }

        // Wait for all closing tasks to complete
        let mut remaining_builders = Vec::new();
        while let Some(result) = self.closing_tasks.join_next().await {
            match result {
                Ok(Ok(builders)) => remaining_builders.extend(builders),
                Ok(Err(e)) => return Err(e),
                Err(e) => {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        "Closing task panicked inside the writer",
                    )
                    .with_source(e));
                }
            }
        }

        self.data_file_builders.extend(remaining_builders);
        Ok(self.data_file_builders)
    }
}

impl<B: FileWriterBuilder> CurrentFileStatus for RollingFileWriter<B> {
    fn current_file_path(&self) -> String {
        self.inner.as_ref().unwrap().current_file_path()
    }

    fn current_row_num(&self) -> usize {
        self.inner.as_ref().unwrap().current_row_num()
    }

    fn current_written_size(&self) -> usize {
        self.inner.as_ref().unwrap().current_written_size()
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

    #[tokio::test]
    async fn test_rolling_writer_with_rolling_with_default_concurrent_should_roll_over_with_default_concurrent_limit()
    -> Result<()> {
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
            file_io.clone(),
            location_gen,
            file_name_gen,
        );

        // Set at None to use the default concurrency
        let rolling_writer_builder =
            RollingFileWriterBuilder::with_concurrency(parquet_writer_builder, 1024, None);

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
