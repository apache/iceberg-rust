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

use std::mem::take;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use futures::future::try_join_all;

use crate::runtime::{JoinHandle, spawn};
use crate::spec::DataFile;
use crate::writer::{IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind, Result};

/// A writer that can roll over to a new file when certain conditions are met.
///
/// This trait extends `IcebergWriter` with the ability to determine when to start
/// writing to a new file based on the size of incoming data.
#[async_trait]
pub trait RollingFileWriter: IcebergWriter {
    /// Determines if the writer should roll over to a new file.
    ///
    /// # Arguments
    ///
    /// * `input_size` - The size in bytes of the incoming data
    ///
    /// # Returns
    ///
    /// `true` if a new file should be started, `false` otherwise
    fn should_roll(&mut self, input_size: u64) -> bool;
}

/// Builder for creating a `RollingDataFileWriter` that rolls over to a new file
/// when the data size exceeds a target threshold.
#[derive(Clone)]
pub struct RollingDataFileWriterBuilder<B: IcebergWriterBuilder> {
    inner_builder: B,
    target_size: u64,
}

impl<B: IcebergWriterBuilder> RollingDataFileWriterBuilder<B> {
    /// Creates a new `RollingDataFileWriterBuilder` with the specified inner builder and target size.
    ///
    /// # Arguments
    ///
    /// * `inner_builder` - The builder for the underlying file writer
    /// * `target_size` - The target size in bytes before rolling over to a new file
    pub fn new(inner_builder: B, target_size: u64) -> Self {
        Self {
            inner_builder,
            target_size,
        }
    }
}

#[async_trait]
impl<B: IcebergWriterBuilder> IcebergWriterBuilder for RollingDataFileWriterBuilder<B> {
    type R = RollingDataFileWriter<B>;

    async fn build(self) -> Result<Self::R> {
        Ok(RollingDataFileWriter {
            inner: None,
            inner_builder: self.inner_builder,
            target_size: self.target_size,
            written_size: 0,
            close_handles: vec![],
        })
    }
}

/// A writer that automatically rolls over to a new file when the data size
/// exceeds a target threshold.
///
/// This writer wraps another file writer and tracks the amount of data written.
/// When the data size exceeds the target size, it closes the current file and
/// starts writing to a new one.
pub struct RollingDataFileWriter<B: IcebergWriterBuilder> {
    inner: Option<B::R>,
    inner_builder: B,
    target_size: u64,
    written_size: u64,
    close_handles: Vec<JoinHandle<Result<Vec<DataFile>>>>,
}

#[async_trait]
impl<B: IcebergWriterBuilder> IcebergWriter for RollingDataFileWriter<B> {
    async fn write(&mut self, input: RecordBatch) -> Result<()> {
        let input_size = input.get_array_memory_size() as u64;
        if self.should_roll(input_size) {
            if let Some(mut inner) = self.inner.take() {
                // close the current writer, roll to a new file
                let handle = spawn(async move { inner.close().await });
                self.close_handles.push(handle)
            }

            // clear bytes written
            self.written_size = 0;
        }

        if self.inner.is_none() {
            // start a new writer
            self.inner = Some(self.inner_builder.clone().build().await?);
        }

        // write the input and count bytes written
        let Some(writer) = self.inner.as_mut() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Writer is not initialized!",
            ));
        };
        writer.write(input).await?;
        self.written_size += input_size;
        Ok(())
    }

    async fn close(&mut self) -> Result<Vec<DataFile>> {
        let mut data_files = try_join_all(take(&mut self.close_handles))
            .await?
            .into_iter()
            .flatten()
            .collect::<Vec<DataFile>>();

        // close the current writer and merge the output
        if let Some(mut current_writer) = take(&mut self.inner) {
            data_files.extend(current_writer.close().await?);
        }

        Ok(data_files)
    }
}

impl<B: IcebergWriterBuilder> RollingFileWriter for RollingDataFileWriter<B> {
    fn should_roll(&mut self, input_size: u64) -> bool {
        self.written_size + input_size > self.target_size
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

    #[tokio::test]
    async fn test_rolling_writer_basic() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen =
            MockLocationGenerator::new(temp_dir.path().to_str().unwrap().to_string());
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // Create schema
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()?;

        // Create writer builders
        let parquet_writer_builder = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::new(schema),
            file_io.clone(),
            location_gen,
            file_name_gen,
        );
        let data_file_writer_builder = DataFileWriterBuilder::new(parquet_writer_builder, None, 0);

        // Set a large target size so no rolling occurs
        let rolling_writer_builder = RollingDataFileWriterBuilder::new(
            data_file_writer_builder,
            1024 * 1024, // 1MB, large enough to not trigger rolling
        );

        // Create writer
        let mut writer = rolling_writer_builder.build().await?;

        // Create test data
        let arrow_schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                1.to_string(),
            )])),
            Field::new("name", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                2.to_string(),
            )])),
        ]);

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
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen =
            MockLocationGenerator::new(temp_dir.path().to_str().unwrap().to_string());
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // Create schema
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()?;

        // Create writer builders
        let parquet_writer_builder = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::new(schema),
            file_io.clone(),
            location_gen,
            file_name_gen,
        );
        let data_file_writer_builder = DataFileWriterBuilder::new(parquet_writer_builder, None, 0);

        // Set a very small target size to trigger rolling
        let rolling_writer_builder = RollingDataFileWriterBuilder::new(
            data_file_writer_builder,
            100, // Very small target size to ensure rolling
        );

        // Create writer
        let mut writer = rolling_writer_builder.build().await?;

        // Create test data
        let arrow_schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                1.to_string(),
            )])),
            Field::new("name", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                2.to_string(),
            )])),
        ]);

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

        // Verify each file has the correct content
        // Note: We can't easily verify which records went to which file without more complex logic,
        // but we can verify the total count and that each file has valid content

        Ok(())
    }
}
