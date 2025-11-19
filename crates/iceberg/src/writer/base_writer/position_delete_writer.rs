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

//! This module provide `PositionDeleteWriter`.

use arrow_array::RecordBatch;

use crate::spec::{DataFile, PartitionKey};
use crate::writer::file_writer::FileWriterBuilder;
use crate::writer::file_writer::location_generator::{FileNameGenerator, LocationGenerator};
use crate::writer::file_writer::rolling_writer::{RollingFileWriter, RollingFileWriterBuilder};
use crate::writer::{IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind, Result};

/// Field ID for the `file_path` column in position delete files.
///
/// This is the required field that stores the path to the data file
/// from which rows are being deleted. Value: 2147483546
pub const FIELD_ID_POSITION_DELETE_FILE_PATH: i32 = 2147483546;

/// Field ID for the `pos` column in position delete files.
///
/// This is the required field that stores the position (row number)
/// within the data file to delete. Value: 2147483545
pub const FIELD_ID_POSITION_DELETE_POS: i32 = 2147483545;

/// Builder for `PositionDeleteWriter`.
#[derive(Clone, Debug)]
pub struct PositionDeleteFileWriterBuilder<
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
> {
    inner: RollingFileWriterBuilder<B, L, F>,
}

impl<B, L, F> PositionDeleteFileWriterBuilder<B, L, F>
where
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
{
    /// Create a new `PositionDeleteFileWriterBuilder` using a `RollingFileWriterBuilder`.
    ///
    /// # Arguments
    ///
    /// * `inner` - A `RollingFileWriterBuilder` configured with the appropriate schema.
    ///   The schema should contain at minimum the two required fields:
    ///   - `file_path` (string) with field id 2147483546
    ///   - `pos` (long) with field id 2147483545
    ///
    /// The schema may optionally include additional columns from the deleted rows
    /// for more context.
    pub fn new(inner: RollingFileWriterBuilder<B, L, F>) -> Self {
        Self { inner }
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

    async fn build(self, partition_key: Option<PartitionKey>) -> Result<Self::R> {
        Ok(PositionDeleteFileWriter {
            inner: Some(self.inner.clone().build()),
            partition_key,
        })
    }
}

/// Writer for position delete files.
///
/// Position delete files encode rows to delete from a data file by storing
/// the file path and the position (row number) of each deleted row.
///
/// According to the Iceberg spec, position delete files:
/// - Must be sorted by (file_path, pos)
/// - Must have two required fields:
///   - `file_path` (string) with field id 2147483546
///   - `pos` (long) with field id 2147483545
/// - May optionally include additional columns from the deleted rows
/// - Should set `sort_order_id` to null (position deletes use file+pos ordering)
///
/// # Example
///
/// ```rust,no_run
/// use std::collections::HashMap;
/// use std::sync::Arc;
///
/// use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
/// use arrow_schema::{DataType, Field, Schema as ArrowSchema};
/// use iceberg::arrow::arrow_schema_to_schema;
/// use iceberg::io::FileIOBuilder;
/// use iceberg::spec::{DataFileFormat, Schema};
/// use iceberg::writer::base_writer::position_delete_writer::{
///     FIELD_ID_POSITION_DELETE_FILE_PATH, FIELD_ID_POSITION_DELETE_POS,
///     PositionDeleteFileWriterBuilder,
/// };
/// use iceberg::writer::file_writer::ParquetWriterBuilder;
/// use iceberg::writer::file_writer::location_generator::{
///     DefaultFileNameGenerator, DefaultLocationGenerator,
/// };
/// use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
/// use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
/// use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
/// use parquet::file::properties::WriterProperties;
///
/// # async fn example() -> iceberg::Result<()> {
/// // Create the position delete schema
/// let arrow_schema = Arc::new(ArrowSchema::new(vec![
///     Field::new("file_path", DataType::Utf8, false).with_metadata(HashMap::from([(
///         PARQUET_FIELD_ID_META_KEY.to_string(),
///         FIELD_ID_POSITION_DELETE_FILE_PATH.to_string(),
///     )])),
///     Field::new("pos", DataType::Int64, false).with_metadata(HashMap::from([(
///         PARQUET_FIELD_ID_META_KEY.to_string(),
///         FIELD_ID_POSITION_DELETE_POS.to_string(),
///     )])),
/// ]));
///
/// let schema = Arc::new(arrow_schema_to_schema(&arrow_schema)?);
///
/// // Setup file I/O and location generators
/// let file_io = FileIOBuilder::new_fs_io().build()?;
/// let location_gen = DefaultLocationGenerator::with_data_location("/tmp".to_string());
/// let file_name_gen =
///     DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);
///
/// // Create the writer
/// let parquet_writer_builder =
///     ParquetWriterBuilder::new(WriterProperties::builder().build(), schema);
/// let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
///     parquet_writer_builder,
///     file_io,
///     location_gen,
///     file_name_gen,
/// );
/// let mut writer = PositionDeleteFileWriterBuilder::new(rolling_writer_builder)
///     .build(None)
///     .await?;
///
/// // Write position deletes (must be sorted by file_path, then pos)
/// let file_paths = StringArray::from(vec![
///     "s3://bucket/data1.parquet",
///     "s3://bucket/data1.parquet",
/// ]);
/// let positions = Int64Array::from(vec![10, 25]);
/// let batch = RecordBatch::try_new(arrow_schema, vec![
///     Arc::new(file_paths) as ArrayRef,
///     Arc::new(positions) as ArrayRef,
/// ])?;
///
/// writer.write(batch).await?;
/// let data_files = writer.close().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct PositionDeleteFileWriter<
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
> {
    inner: Option<RollingFileWriter<B, L, F>>,
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
                    // Per the Iceberg spec: "Readers must ignore sort order id for
                    // position delete files" because they are sorted by file+position,
                    // not by a table sort order. The default sort_order_id is None.
                    if let Some(pk) = self.partition_key.as_ref() {
                        res.partition(pk.data().clone());
                        res.partition_spec_id(pk.spec().spec_id());
                    }
                    res.build().map_err(|e| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!("Failed to build data file: {e}"),
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
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use arrow_select::concat::concat_batches;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use crate::arrow::arrow_schema_to_schema;
    use crate::io::{FileIO, FileIOBuilder};
    use crate::spec::{DataContentType, DataFile, DataFileFormat};
    use crate::writer::base_writer::position_delete_writer::{
        FIELD_ID_POSITION_DELETE_FILE_PATH, FIELD_ID_POSITION_DELETE_POS,
        PositionDeleteFileWriterBuilder,
    };
    use crate::writer::file_writer::ParquetWriterBuilder;
    use crate::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use crate::writer::{IcebergWriter, IcebergWriterBuilder};

    async fn check_parquet_position_delete_file(
        file_io: &FileIO,
        data_file: &DataFile,
        expected_batch: &RecordBatch,
    ) {
        assert_eq!(data_file.file_format, DataFileFormat::Parquet);
        assert_eq!(data_file.content_type(), DataContentType::PositionDeletes);

        // Position deletes should have null sort_order_id
        assert!(data_file.sort_order_id().is_none());

        let input_file = file_io.new_input(data_file.file_path.clone()).unwrap();
        let input_content = input_file.read().await.unwrap();
        let reader_builder =
            ParquetRecordBatchReaderBuilder::try_new(input_content.clone()).unwrap();
        let metadata = reader_builder.metadata().clone();

        // Check data
        let reader = reader_builder.build().unwrap();
        let batches = reader.map(|batch| batch.unwrap()).collect::<Vec<_>>();
        let res = concat_batches(&expected_batch.schema(), &batches).unwrap();
        assert_eq!(*expected_batch, res);

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
    }

    #[tokio::test]
    async fn test_position_delete_writer_basic() -> Result<(), anyhow::Error> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // Create position delete schema with required fields
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("file_path", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                FIELD_ID_POSITION_DELETE_FILE_PATH.to_string(),
            )])),
            Field::new("pos", DataType::Int64, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                FIELD_ID_POSITION_DELETE_POS.to_string(),
            )])),
        ]));

        let schema = Arc::new(arrow_schema_to_schema(&arrow_schema).unwrap());

        // Prepare writer
        let pb = ParquetWriterBuilder::new(WriterProperties::builder().build(), schema);
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            pb,
            file_io.clone(),
            location_gen,
            file_name_gen,
        );
        let mut position_delete_writer =
            PositionDeleteFileWriterBuilder::new(rolling_writer_builder)
                .build(None)
                .await?;

        // Create test data - position deletes should be sorted by (file_path, pos)
        let file_paths = Arc::new(StringArray::from(vec![
            "s3://bucket/table/data/file1.parquet",
            "s3://bucket/table/data/file1.parquet",
            "s3://bucket/table/data/file2.parquet",
            "s3://bucket/table/data/file2.parquet",
        ])) as ArrayRef;
        let positions = Arc::new(Int64Array::from(vec![5, 10, 3, 7])) as ArrayRef;

        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![file_paths, positions])?;

        // Write and close
        position_delete_writer.write(to_write.clone()).await?;
        let res = position_delete_writer.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res.into_iter().next().unwrap();

        // Verify
        check_parquet_position_delete_file(&file_io, &data_file, &to_write).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_position_delete_writer_with_extra_columns() -> Result<(), anyhow::Error> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // Create position delete schema with extra columns for context
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("file_path", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                FIELD_ID_POSITION_DELETE_FILE_PATH.to_string(),
            )])),
            Field::new("pos", DataType::Int64, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                FIELD_ID_POSITION_DELETE_POS.to_string(),
            )])),
            // Extra column: deleted row ID for debugging
            Field::new("row_id", DataType::Int64, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
        ]));

        let schema = Arc::new(arrow_schema_to_schema(&arrow_schema).unwrap());

        // Prepare writer
        let pb = ParquetWriterBuilder::new(WriterProperties::builder().build(), schema);
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            pb,
            file_io.clone(),
            location_gen,
            file_name_gen,
        );
        let mut position_delete_writer =
            PositionDeleteFileWriterBuilder::new(rolling_writer_builder)
                .build(None)
                .await?;

        // Create test data with extra column
        let file_paths = Arc::new(StringArray::from(vec![
            "s3://bucket/table/data/file1.parquet",
            "s3://bucket/table/data/file1.parquet",
        ])) as ArrayRef;
        let positions = Arc::new(Int64Array::from(vec![100, 200])) as ArrayRef;
        let row_ids = Arc::new(Int64Array::from(vec![Some(42), Some(84)])) as ArrayRef;

        let to_write =
            RecordBatch::try_new(arrow_schema.clone(), vec![file_paths, positions, row_ids])?;

        // Write and close
        position_delete_writer.write(to_write.clone()).await?;
        let res = position_delete_writer.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res.into_iter().next().unwrap();

        // Verify
        check_parquet_position_delete_file(&file_io, &data_file, &to_write).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_position_delete_writer_multiple_batches() -> Result<(), anyhow::Error> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("file_path", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                FIELD_ID_POSITION_DELETE_FILE_PATH.to_string(),
            )])),
            Field::new("pos", DataType::Int64, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                FIELD_ID_POSITION_DELETE_POS.to_string(),
            )])),
        ]));

        let schema = Arc::new(arrow_schema_to_schema(&arrow_schema).unwrap());

        let pb = ParquetWriterBuilder::new(WriterProperties::builder().build(), schema);
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            pb,
            file_io.clone(),
            location_gen,
            file_name_gen,
        );
        let mut position_delete_writer =
            PositionDeleteFileWriterBuilder::new(rolling_writer_builder)
                .build(None)
                .await?;

        // Write multiple batches
        let batch1 = RecordBatch::try_new(arrow_schema.clone(), vec![
            Arc::new(StringArray::from(vec![
                "s3://bucket/data1.parquet",
                "s3://bucket/data1.parquet",
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![0, 1])) as ArrayRef,
        ])?;

        let batch2 = RecordBatch::try_new(arrow_schema.clone(), vec![
            Arc::new(StringArray::from(vec![
                "s3://bucket/data2.parquet",
                "s3://bucket/data2.parquet",
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![5, 10])) as ArrayRef,
        ])?;

        position_delete_writer.write(batch1.clone()).await?;
        position_delete_writer.write(batch2.clone()).await?;
        let res = position_delete_writer.close().await?;

        assert_eq!(res.len(), 1);
        let data_file = res.into_iter().next().unwrap();

        // Verify combined batches
        let expected = concat_batches(&arrow_schema, &[batch1, batch2])?;
        check_parquet_position_delete_file(&file_io, &data_file, &expected).await;

        Ok(())
    }
}
