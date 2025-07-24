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
use futures::future::try_join_all;

use crate::runtime::{JoinHandle, spawn};
use crate::spec::DataFileBuilder;
use crate::writer::CurrentFileStatus;
use crate::writer::file_writer::{FileWriter, FileWriterBuilder};
use crate::{Error, ErrorKind, Result};

/// Builder for creating a `RollingFileWriter` that rolls over to a new file
/// when the data size exceeds a target threshold.
#[derive(Clone)]
pub struct RollingFileWriterBuilder<B: FileWriterBuilder> {
    inner_builder: B,
    target_file_size: usize,
}

impl<B: FileWriterBuilder> RollingFileWriterBuilder<B> {
    /// Creates a new `RollingFileWriterBuilder` with the specified inner builder and target size.
    ///
    /// # Arguments
    ///
    /// * `inner_builder` - The builder for the underlying file writer
    /// * `target_file_size` - The target size in bytes before rolling over to a new file
    ///
    /// NOTE: The `target_file_size` does not exactly reflect the final size on physical storage.
    /// This is because the input size is based on the Arrow in-memory format, which differs from the on-disk file format.
    pub fn new(inner_builder: B, target_file_size: usize) -> Self {
        Self {
            inner_builder,
            target_file_size,
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
            close_handles: vec![],
        })
    }
}

/// A writer that automatically rolls over to a new file when the data size
/// exceeds a target threshold.
///
/// This writer wraps another file writer that tracks the amount of data written.
/// When the data size exceeds the target size, it closes the current file and
/// starts writing to a new one.
pub struct RollingFileWriter<B: FileWriterBuilder> {
    inner: Option<B::R>,
    inner_builder: B,
    target_file_size: usize,
    close_handles: Vec<JoinHandle<Result<Vec<DataFileBuilder>>>>,
}

impl<B: FileWriterBuilder> RollingFileWriter<B> {
    /// Determines if the writer should roll over to a new file.
    ///
    /// # Arguments
    ///
    /// * `input_size` - The size in bytes of the incoming data
    ///
    /// # Returns
    ///
    /// `true` if a new file should be started, `false` otherwise
    pub fn should_roll(&self, input_size: usize) -> bool {
        self.current_written_size() + input_size > self.target_file_size
    }
}

impl<B: FileWriterBuilder> FileWriter for RollingFileWriter<B> {
    async fn write(&mut self, input: &RecordBatch) -> Result<()> {
        // The input size is estimated using the Arrow in-memory format
        // and will differ from the final on-disk file size.
        let input_size = input.get_array_memory_size();

        if self.inner.is_none() {
            // initialize inner writer
            self.inner = Some(self.inner_builder.clone().build().await?);
        }

        if self.should_roll(input_size) {
            if let Some(inner) = self.inner.take() {
                // close the current writer, roll to a new file
                let handle = spawn(async move { inner.close().await });
                self.close_handles.push(handle);

                // start a new writer
                self.inner = Some(self.inner_builder.clone().build().await?);
            }
        }

        // write the input
        let Some(writer) = self.inner.as_mut() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Writer is not initialized!",
            ));
        };
        writer.write(input).await?;

        Ok(())
    }

    async fn close(self) -> Result<Vec<DataFileBuilder>> {
        let mut data_file_builders = try_join_all(self.close_handles)
            .await?
            .into_iter()
            .flatten()
            .collect::<Vec<DataFileBuilder>>();

        // close the current writer and merge the output
        if let Some(current_writer) = self.inner {
            data_file_builders.extend(current_writer.close().await?);
        }

        Ok(data_file_builders)
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

    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet::file::properties::WriterProperties;
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
        let rolling_writer_builder = RollingFileWriterBuilder::new(
            parquet_writer_builder,
            100, // Very small target size to ensure rolling
        );

        let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder, None, 0);

        // Create writer
        let mut writer = data_file_writer_builder.build().await?;

        // Create test data
        let arrow_schema = make_test_arrow_schema();

        // Create multiple batches to trigger rolling
        let batch1 = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ])?;

        let batch2 = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
            Arc::new(Int32Array::from(vec![4, 5, 6])),
            Arc::new(StringArray::from(vec!["Dave", "Eve", "Frank"])),
        ])?;

        let batch3 = RecordBatch::try_new(Arc::new(arrow_schema), vec![
            Arc::new(Int32Array::from(vec![7, 8, 9])),
            Arc::new(StringArray::from(vec!["Grace", "Heidi", "Ivan"])),
        ])?;

        // Write data
        writer.write(batch1.clone()).await?;
        writer.write(batch2.clone()).await?;
        writer.write(batch3.clone()).await?;

        // Close writer and get data files
        let data_files = writer.close().await?;

        // Verify multiple files were created (at least 2)
        assert!(
            data_files.len() > 1,
            "Expected multiple data files to be created, got {}",
            data_files.len()
        );

        // Verify total record count across all files
        let total_records: u64 = data_files.iter().map(|file| file.record_count).sum();
        assert_eq!(
            total_records, 9,
            "Expected 9 total records across all files"
        );

        Ok(())
    }
}
