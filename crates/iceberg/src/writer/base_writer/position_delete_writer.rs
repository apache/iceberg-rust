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

//! Position delete writer for encoding row-level deletions in Iceberg tables.
//!
//! Position deletes identify rows to delete by their file path and row position within that file.
//! This is more efficient than equality deletes when the exact location of rows to delete is known,
//! such as during CDC processing or when tracking rows written within the same transaction.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::spec::{DataFile, PartitionKey, Struct};
use crate::writer::file_writer::location_generator::{FileNameGenerator, LocationGenerator};
use crate::writer::file_writer::rolling_writer::{RollingFileWriter, RollingFileWriterBuilder};
use crate::writer::file_writer::FileWriterBuilder;
use crate::writer::{IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind, Result};

/// Builder for `PositionDeleteWriter`.
#[derive(Debug)]
pub struct PositionDeleteFileWriterBuilder<
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
> {
    inner: RollingFileWriterBuilder<B, L, F>,
    config: PositionDeleteWriterConfig,
}

impl<B, L, F> PositionDeleteFileWriterBuilder<B, L, F>
where
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
{
    /// Create a new `PositionDeleteFileWriterBuilder` using a `RollingFileWriterBuilder`.
    pub fn new(
        inner: RollingFileWriterBuilder<B, L, F>,
        config: PositionDeleteWriterConfig,
    ) -> Self {
        Self { inner, config }
    }
}

/// Config for `PositionDeleteWriter`.
#[derive(Clone, Debug)]
pub struct PositionDeleteWriterConfig {
    partition_value: Struct,
    partition_spec_id: i32,
    referenced_data_file: Option<String>,
}

impl PositionDeleteWriterConfig {
    /// Create a new `PositionDeleteWriterConfig`.
    ///
    /// # Arguments
    /// * `partition_value` - The partition value for the delete file
    /// * `partition_spec_id` - The partition spec ID
    /// * `referenced_data_file` - Optional path to the data file being deleted from
    pub fn new(
        partition_value: Option<Struct>,
        partition_spec_id: i32,
        referenced_data_file: Option<String>,
    ) -> Self {
        Self {
            partition_value: partition_value.unwrap_or(Struct::empty()),
            partition_spec_id,
            referenced_data_file,
        }
    }

    /// Returns the Arrow schema for position delete files.
    ///
    /// Position delete files have a fixed schema:
    /// - file_path: string (field id 2147483546)
    /// - pos: long (field id 2147483545)
    pub fn arrow_schema() -> ArrowSchemaRef {
        Arc::new(ArrowSchema::new(vec![
            Field::new("file_path", DataType::Utf8, false).with_metadata(
                [(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "2147483546".to_string(),
                )]
                .into_iter()
                .collect(),
            ),
            Field::new("pos", DataType::Int64, false).with_metadata(
                [(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "2147483545".to_string(),
                )]
                .into_iter()
                .collect(),
            ),
        ]))
    }
}

#[async_trait::async_trait]
impl<B, L, F> IcebergWriterBuilder for PositionDeleteFileWriterBuilder<B, L, F>
where
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
{
    type R = PositionDeleteFileWriter<B, L, F>;

    async fn build(&self, partition_key: Option<PartitionKey>) -> Result<Self::R> {
        Ok(PositionDeleteFileWriter {
            inner: Some(self.inner.build()),
            partition_value: self.config.partition_value.clone(),
            partition_spec_id: self.config.partition_spec_id,
            referenced_data_file: self.config.referenced_data_file.clone(),
            partition_key,
        })
    }
}

/// Writer used to write position delete files.
///
/// Position delete files mark specific rows in data files as deleted
/// by their position (row number). The schema is fixed with two columns:
/// - file_path: The path to the data file
/// - pos: The row position (0-indexed) in that file
#[derive(Debug)]
pub struct PositionDeleteFileWriter<B: FileWriterBuilder, L: LocationGenerator, F: FileNameGenerator>
{
    inner: Option<RollingFileWriter<B, L, F>>,
    partition_value: Struct,
    partition_spec_id: i32,
    referenced_data_file: Option<String>,
    partition_key: Option<PartitionKey>,
}

#[async_trait::async_trait]
impl<B, L, F> IcebergWriter for PositionDeleteFileWriter<B, L, F>
where
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
{
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        // Validate the batch has the correct schema
        let expected_schema = PositionDeleteWriterConfig::arrow_schema();
        if batch.schema().fields() != expected_schema.fields() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Position delete batch has invalid schema. Expected: {:?}, Got: {:?}",
                    expected_schema.fields(),
                    batch.schema().fields()
                ),
            ));
        }

        if let Some(writer) = self.inner.as_mut() {
            writer.write(&self.partition_key, &batch).await
        } else {
            Err(Error::new(
                ErrorKind::Unexpected,
                "Position delete inner writer has been closed.",
            ))
        }
    }

    async fn close(&mut self) -> Result<Vec<DataFile>> {
        if let Some(writer) = self.inner.take() {
            writer
                .close()
                .await?
                .into_iter()
                .map(|mut res| {
                    res.content(crate::spec::DataContentType::PositionDeletes);
                    res.partition(self.partition_value.clone());
                    res.partition_spec_id(self.partition_spec_id);
                    if let Some(ref data_file) = self.referenced_data_file {
                        res.referenced_data_file(Some(data_file.clone()));
                    }
                    // Position deletes must have null sort_order_id (default is None)
                    res.build().map_err(|e| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!("Failed to build position delete data file: {e}"),
                        )
                    })
                })
                .collect()
        } else {
            Err(Error::new(
                ErrorKind::Unexpected,
                "Position delete inner writer has been closed.",
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int32Array, Int64Array, RecordBatch, StringArray};
    use arrow_select::concat::concat_batches;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use super::*;
    use crate::arrow::arrow_schema_to_schema;
    use crate::io::{FileIO, FileIOBuilder, LocalFsStorageFactory};
    use crate::spec::DataFileFormat;
    use crate::writer::IcebergWriterBuilder;
    use crate::writer::file_writer::ParquetWriterBuilder;
    use crate::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;

    async fn check_parquet_position_delete_file(
        file_io: &FileIO,
        data_file: &DataFile,
        expected_batch: &RecordBatch,
    ) {
        assert_eq!(data_file.file_format, DataFileFormat::Parquet);
        assert_eq!(
            data_file.content,
            crate::spec::DataContentType::PositionDeletes
        );

        // Read the written file
        let input_file = file_io.new_input(data_file.file_path.clone()).unwrap();
        let input_content = input_file.read().await.unwrap();
        let reader_builder =
            ParquetRecordBatchReaderBuilder::try_new(input_content.clone()).unwrap();
        let metadata = reader_builder.metadata().clone();

        // Check data
        let reader = reader_builder.build().unwrap();
        let batches = reader.map(|batch| batch.unwrap()).collect::<Vec<_>>();
        let actual = concat_batches(&expected_batch.schema(), &batches).unwrap();
        assert_eq!(*expected_batch, actual);

        // Check metadata
        assert_eq!(
            data_file.record_count,
            metadata
                .row_groups()
                .iter()
                .map(|group| group.num_rows())
                .sum::<i64>() as u64
        );
        assert_eq!(data_file.file_size_in_bytes, input_content.len() as u64);

        // Position deletes must have null sort_order_id
        assert!(data_file.sort_order_id.is_none());
    }

    #[tokio::test]
    async fn test_position_delete_writer() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new(Arc::new(LocalFsStorageFactory)).build();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // Get the position delete schema
        let arrow_schema = PositionDeleteWriterConfig::arrow_schema();
        let schema = Arc::new(arrow_schema_to_schema(&arrow_schema)?);

        // Create writer
        let config = PositionDeleteWriterConfig::new(None, 0, None);
        let pb = ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone());
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            pb,
            schema,
            file_io.clone(),
            location_gen,
            file_name_gen,
        );
        let mut writer = PositionDeleteFileWriterBuilder::new(rolling_writer_builder, config)
            .build(None)
            .await?;

        // Create test data - delete rows 5, 10, 15 from a file
        let file_paths = StringArray::from(vec![
            "s3://bucket/data/file1.parquet",
            "s3://bucket/data/file1.parquet",
            "s3://bucket/data/file1.parquet",
        ]);
        let positions = Int64Array::from(vec![5, 10, 15]);
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![
            Arc::new(file_paths),
            Arc::new(positions),
        ])?;

        // Write
        writer.write(batch.clone()).await?;
        let data_files = writer.close().await?;

        assert_eq!(data_files.len(), 1);
        let data_file = &data_files[0];

        // Verify
        check_parquet_position_delete_file(&file_io, data_file, &batch).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_position_delete_writer_with_referenced_file() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new(Arc::new(LocalFsStorageFactory)).build();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let arrow_schema = PositionDeleteWriterConfig::arrow_schema();
        let schema = Arc::new(arrow_schema_to_schema(&arrow_schema)?);

        // Create writer with referenced data file
        let referenced_file = "s3://bucket/data/file1.parquet".to_string();
        let config = PositionDeleteWriterConfig::new(None, 0, Some(referenced_file.clone()));
        let pb = ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone());
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            pb,
            schema,
            file_io.clone(),
            location_gen,
            file_name_gen,
        );
        let mut writer = PositionDeleteFileWriterBuilder::new(rolling_writer_builder, config)
            .build(None)
            .await?;

        // Create test data
        let file_paths = StringArray::from(vec![referenced_file.as_str()]);
        let positions = Int64Array::from(vec![42]);
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![
            Arc::new(file_paths),
            Arc::new(positions),
        ])?;

        writer.write(batch.clone()).await?;
        let data_files = writer.close().await?;

        assert_eq!(data_files.len(), 1);
        let data_file = &data_files[0];

        // Verify referenced_data_file is set
        assert_eq!(data_file.referenced_data_file, Some(referenced_file));

        Ok(())
    }

    #[tokio::test]
    async fn test_position_delete_writer_invalid_schema() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new(Arc::new(LocalFsStorageFactory)).build();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let arrow_schema = PositionDeleteWriterConfig::arrow_schema();
        let schema = Arc::new(arrow_schema_to_schema(&arrow_schema)?);

        let config = PositionDeleteWriterConfig::new(None, 0, None);
        let pb = ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone());
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            pb,
            schema,
            file_io.clone(),
            location_gen,
            file_name_gen,
        );
        let mut writer = PositionDeleteFileWriterBuilder::new(rolling_writer_builder, config)
            .build(None)
            .await?;

        // Try to write batch with wrong schema (missing pos field)
        let wrong_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "wrong_field",
            DataType::Int32,
            false,
        )]));
        let wrong_batch =
            RecordBatch::try_new(wrong_schema, vec![Arc::new(Int32Array::from(vec![1]))])?;

        let result = writer.write(wrong_batch).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("invalid schema"));

        Ok(())
    }

    #[tokio::test]
    async fn test_position_delete_multiple_files() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new(Arc::new(LocalFsStorageFactory)).build();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let arrow_schema = PositionDeleteWriterConfig::arrow_schema();
        let schema = Arc::new(arrow_schema_to_schema(&arrow_schema)?);

        let config = PositionDeleteWriterConfig::new(None, 0, None);
        let pb = ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone());
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            pb,
            schema,
            file_io.clone(),
            location_gen,
            file_name_gen,
        );
        let mut writer = PositionDeleteFileWriterBuilder::new(rolling_writer_builder, config)
            .build(None)
            .await?;

        // Delete rows from multiple data files
        let file_paths = StringArray::from(vec![
            "s3://bucket/data/file1.parquet",
            "s3://bucket/data/file1.parquet",
            "s3://bucket/data/file2.parquet",
            "s3://bucket/data/file2.parquet",
            "s3://bucket/data/file3.parquet",
        ]);
        let positions = Int64Array::from(vec![0, 10, 5, 15, 100]);
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![
            Arc::new(file_paths),
            Arc::new(positions),
        ])?;

        writer.write(batch.clone()).await?;
        let data_files = writer.close().await?;

        assert_eq!(data_files.len(), 1);
        check_parquet_position_delete_file(&file_io, &data_files[0], &batch).await;

        Ok(())
    }
}
