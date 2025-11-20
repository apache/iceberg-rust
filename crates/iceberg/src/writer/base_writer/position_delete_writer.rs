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

use arrow_array::{Array, RecordBatch, StringArray};

use crate::spec::{DataFile, DataFileBuilder, PartitionKey};
use crate::writer::file_writer::FileWriterBuilder;
use crate::writer::file_writer::location_generator::{FileNameGenerator, LocationGenerator};
use crate::writer::file_writer::rolling_writer::{RollingFileWriter, RollingFileWriterBuilder};
use crate::writer::{IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind, Result};

/// Tracks file_path references for a single batch to enable the referenced_data_file optimization.
///
/// When all position deletes in a batch reference the same data file, we store that file path.
/// This information is later used to set the `referenced_data_file` field on output DataFiles
/// when all deletes in the file reference a single data file.
#[derive(Debug, Clone)]
struct BatchTrackingInfo {
    /// The single file path if all position deletes in this batch reference the same data file.
    /// `None` if the batch references multiple different data files.
    single_referenced_file: Option<String>,
    /// Number of rows in this batch, used to map batches to output files.
    row_count: u64,
}

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
    ///
    /// The schema must contain the two required fields as per the Iceberg spec:
    ///   - `file_path` (string) with field id `2147483546`
    ///   - `pos` (long) with field id `2147483545`
    ///
    /// The schema may optionally include additional columns from the deleted rows
    /// for debugging context.
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
            batch_tracking: Vec::new(),
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
///   - `file_path` (string) with field id `2147483546`
///   - `pos` (long) with field id `2147483545`
/// - May optionally include additional columns from the deleted rows for debugging
/// - Should set `sort_order_id` to null (position deletes use file+pos ordering)
/// - May set `referenced_data_file` when all deletes reference a single data file (optimization)
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
/// use iceberg::spec::DataFileFormat;
/// use iceberg::writer::base_writer::position_delete_writer::PositionDeleteFileWriterBuilder;
/// use iceberg::writer::file_writer::location_generator::{
///     DefaultFileNameGenerator, DefaultLocationGenerator,
/// };
/// use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
/// use iceberg::writer::file_writer::ParquetWriterBuilder;
/// use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
/// use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
/// use parquet::file::properties::WriterProperties;
///
/// # async fn example() -> iceberg::Result<()> {
/// // Create the position delete schema with required field IDs per Iceberg spec
/// let arrow_schema = Arc::new(ArrowSchema::new(vec![
///     Field::new("file_path", DataType::Utf8, false).with_metadata(HashMap::from([(
///         PARQUET_FIELD_ID_META_KEY.to_string(),
///         "2147483546".to_string(), // Required field ID for file_path
///     )])),
///     Field::new("pos", DataType::Int64, false).with_metadata(HashMap::from([(
///         PARQUET_FIELD_ID_META_KEY.to_string(),
///         "2147483545".to_string(), // Required field ID for pos
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
    /// Tracking information for each batch written, used to implement the referenced_data_file optimization.
    batch_tracking: Vec<BatchTrackingInfo>,
}

impl<B, L, F> PositionDeleteFileWriter<B, L, F>
where
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
{
    /// Extracts the single referenced data file from a batch if all position deletes reference the same file.
    ///
    /// Returns `Ok(Some(path))` if all deletes reference the same file, `Ok(None)` if the batch references
    /// multiple files or is empty, and `Err` if the batch schema is invalid.
    fn extract_single_referenced_file(batch: &RecordBatch) -> Result<Option<String>> {
        let schema = batch.schema();

        // Find the file_path column according to the Iceberg spec
        let file_path_idx = schema
            .fields()
            .iter()
            .position(|field| field.name() == "file_path")
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Position delete batch must contain a 'file_path' column per Iceberg spec",
                )
            })?;

        let file_path_column = batch.column(file_path_idx);

        // Downcast to StringArray (file_path must be String type)
        let file_path_array = file_path_column
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "file_path column must be of StringArray type",
                )
            })?;

        // Empty batch has no references
        let array_len = file_path_array.len();
        if array_len == 0 {
            return Ok(None);
        }

        // Per Iceberg spec, file_path should never be null in position delete files,
        // but we validate this to avoid panics from malformed data
        if file_path_array.null_count() > 0 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "file_path column in position delete file must not contain null values per Iceberg spec",
            ));
        }

        // Check if all values are identical
        let first_value = file_path_array.value(0);

        let all_same = (1..array_len).all(|i| {
            file_path_array.value(i) == first_value
        });

        if all_same {
            Ok(Some(first_value.to_string()))
        } else {
            Ok(None)
        }
    }
}

#[async_trait::async_trait]
impl<B, L, F> IcebergWriter for PositionDeleteFileWriter<B, L, F>
where
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
{
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        // Extract the single referenced file for this batch to enable optimization
        let single_referenced_file = Self::extract_single_referenced_file(&batch)?;
        let row_count = batch.num_rows() as u64;

        // Track this batch's information for use in close()
        self.batch_tracking.push(BatchTrackingInfo {
            single_referenced_file,
            row_count,
        });

        // Write the batch to the inner rolling file writer
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
            let data_file_builders = writer.close().await?;

            // First, build the data files to extract metadata (especially record_count)
            // then rebuild with the referenced_data_file optimization applied
            let temp_data_files: Vec<DataFile> = data_file_builders
                .into_iter()
                .map(|builder| {
                    builder.build().map_err(|e| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!("Failed to build data file from rolling writer: {e}"),
                        )
                    })
                })
                .collect::<Result<Vec<DataFile>>>()?;

            // Map batches to output files and apply the referenced_data_file optimization
            let mut batch_idx = 0;
            let mut rows_consumed = 0u64;
            // Track the cumulative start position of the current batch across all files.
            // This is critical for handling batches that span multiple output files.
            let mut batch_cumulative_start = 0u64;

            temp_data_files
                .into_iter()
                .map(|data_file| {
                    // Start with a new builder, copying all fields from the original DataFile
                    let mut data_file_builder = DataFileBuilder::default();
                    data_file_builder
                        .content(crate::spec::DataContentType::PositionDeletes)
                        .file_path(data_file.file_path)
                        .file_format(data_file.file_format)
                        .partition(data_file.partition.clone())
                        .partition_spec_id(data_file.partition_spec_id)
                        .record_count(data_file.record_count)
                        .file_size_in_bytes(data_file.file_size_in_bytes)
                        .column_sizes(data_file.column_sizes.clone())
                        .value_counts(data_file.value_counts.clone())
                        .null_value_counts(data_file.null_value_counts.clone())
                        .nan_value_counts(data_file.nan_value_counts.clone())
                        .lower_bounds(data_file.lower_bounds.clone())
                        .upper_bounds(data_file.upper_bounds.clone())
                        .key_metadata(data_file.key_metadata.clone())
                        .split_offsets(data_file.split_offsets.clone());

                    // Copy optional fields if present
                    if let Some(equality_ids) = data_file.equality_ids.clone() {
                        data_file_builder.equality_ids(Some(equality_ids));
                    }
                    if let Some(sort_order_id) = data_file.sort_order_id {
                        data_file_builder.sort_order_id(sort_order_id);
                    }
                    if let Some(first_row_id) = data_file.first_row_id {
                        data_file_builder.first_row_id(Some(first_row_id));
                    }

                    // Per the Iceberg spec: "Readers must ignore sort order id for
                    // position delete files" because they are sorted by file+position,
                    // not by a table sort order. The default sort_order_id is None.

                    // Apply the referenced_data_file optimization:
                    // If all position deletes in this file reference a single data file,
                    // set referenced_data_file to that path. This is particularly important
                    // for deletion vectors and enables query engines to skip irrelevant delete files.
                    let file_row_count = data_file.record_count;
                    let file_end_row = rows_consumed + file_row_count;

                    // Find all batches that contributed rows to this file
                    let mut referenced_file: Option<String> = None;

                    while batch_idx < self.batch_tracking.len() {
                        let batch_info = &self.batch_tracking[batch_idx];
                        let batch_end_row = batch_cumulative_start + batch_info.row_count;

                        // Check if this batch contributed to the current file
                        // A batch contributes if its range [batch_cumulative_start, batch_end_row) overlaps with [rows_consumed, file_end_row)
                        if batch_cumulative_start < file_end_row && batch_end_row > rows_consumed {
                            match (&referenced_file, &batch_info.single_referenced_file) {
                                // First batch with a single reference - initialize
                                (None, Some(path)) => {
                                    referenced_file = Some(path.clone());
                                }
                                // Subsequent batch with same reference - keep it
                                (Some(current_path), Some(batch_path))
                                    if current_path == batch_path => {}
                                // Different references or batch has multiple references - cannot optimize
                                _ => {
                                    referenced_file = None;
                                    // Continue checking remaining batches for record-keeping
                                }
                            }
                        }

                        // Move to next batch if current batch is fully consumed by this file
                        if batch_end_row <= file_end_row {
                            batch_cumulative_start = batch_end_row;
                            batch_idx += 1;
                        } else {
                            // Current batch extends beyond this file, will continue in next file
                            break;
                        }
                    }

                    // Set referenced_data_file if all deletes in this file reference a single data file
                    if let Some(path) = referenced_file {
                        data_file_builder.referenced_data_file(Some(path));
                    }

                    // Update rows consumed counter for next file
                    rows_consumed = file_end_row;

                    data_file_builder.build().map_err(|e| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!("Failed to build data file with referenced_data_file optimization: {e}"),
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
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use crate::arrow::arrow_schema_to_schema;
    use crate::io::{FileIO, FileIOBuilder};
    use crate::spec::{DataContentType, DataFile, DataFileFormat};
    use crate::writer::base_writer::position_delete_writer::PositionDeleteFileWriterBuilder;
    use crate::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use crate::writer::file_writer::ParquetWriterBuilder;
    use crate::writer::{IcebergWriter, IcebergWriterBuilder};
    use crate::ErrorKind;

    // Field IDs for position delete files as defined by the Iceberg spec
    const FIELD_ID_POSITION_DELETE_FILE_PATH: i32 = 2147483546;
    const FIELD_ID_POSITION_DELETE_POS: i32 = 2147483545;

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

    #[tokio::test]
    async fn test_referenced_data_file_optimization_single_file() -> Result<(), anyhow::Error> {
        // Test that when all position deletes reference a single data file,
        // the referenced_data_file field is set correctly
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

        // All deletes reference the same data file
        let target_data_file = "s3://bucket/table/data/file1.parquet";
        let file_paths = Arc::new(StringArray::from(vec![
            target_data_file,
            target_data_file,
            target_data_file,
        ])) as ArrayRef;
        let positions = Arc::new(Int64Array::from(vec![5, 10, 15])) as ArrayRef;

        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![file_paths, positions])?;

        position_delete_writer.write(batch).await?;
        let result = position_delete_writer.close().await?;

        assert_eq!(result.len(), 1);
        let data_file = &result[0];

        // Verify referenced_data_file is set
        assert_eq!(
            data_file.referenced_data_file(),
            Some(target_data_file.to_string()),
            "referenced_data_file should be set when all deletes reference the same file"
        );
        assert_eq!(data_file.content_type(), DataContentType::PositionDeletes);
        assert_eq!(data_file.record_count, 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_referenced_data_file_optimization_multiple_files() -> Result<(), anyhow::Error> {
        // Test that when position deletes reference multiple data files,
        // the referenced_data_file field is NOT set
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

        // Deletes reference different data files
        let file_paths = Arc::new(StringArray::from(vec![
            "s3://bucket/table/data/file1.parquet",
            "s3://bucket/table/data/file2.parquet",
            "s3://bucket/table/data/file3.parquet",
        ])) as ArrayRef;
        let positions = Arc::new(Int64Array::from(vec![5, 10, 15])) as ArrayRef;

        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![file_paths, positions])?;

        position_delete_writer.write(batch).await?;
        let result = position_delete_writer.close().await?;

        assert_eq!(result.len(), 1);
        let data_file = &result[0];

        // Verify referenced_data_file is NOT set
        assert_eq!(
            data_file.referenced_data_file(),
            None,
            "referenced_data_file should NOT be set when deletes reference multiple files"
        );
        assert_eq!(data_file.content_type(), DataContentType::PositionDeletes);
        assert_eq!(data_file.record_count, 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_referenced_data_file_optimization_multiple_batches_same_file(
    ) -> Result<(), anyhow::Error> {
        // Test that when multiple batches all reference the same data file,
        // the referenced_data_file field is set correctly
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

        let target_data_file = "s3://bucket/table/data/file1.parquet";

        // Write multiple batches, all referencing the same data file
        for i in 0..3 {
            let file_paths = Arc::new(StringArray::from(vec![
                target_data_file,
                target_data_file,
            ])) as ArrayRef;
            let positions =
                Arc::new(Int64Array::from(vec![i * 10, i * 10 + 5])) as ArrayRef;

            let batch =
                RecordBatch::try_new(arrow_schema.clone(), vec![file_paths, positions])?;
            position_delete_writer.write(batch).await?;
        }

        let result = position_delete_writer.close().await?;

        assert_eq!(result.len(), 1);
        let data_file = &result[0];

        // Verify referenced_data_file is set when all batches reference the same file
        assert_eq!(
            data_file.referenced_data_file(),
            Some(target_data_file.to_string()),
            "referenced_data_file should be set when all batches reference the same file"
        );
        assert_eq!(data_file.content_type(), DataContentType::PositionDeletes);
        assert_eq!(data_file.record_count, 6); // 3 batches × 2 rows

        Ok(())
    }

    #[tokio::test]
    async fn test_referenced_data_file_optimization_multiple_batches_different_files(
    ) -> Result<(), anyhow::Error> {
        // Test that when multiple batches reference different data files,
        // the referenced_data_file field is NOT set
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

        // First batch references file1
        let batch1 = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![
                    "s3://bucket/table/data/file1.parquet",
                    "s3://bucket/table/data/file1.parquet",
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![5, 10])) as ArrayRef,
            ],
        )?;

        // Second batch references file2
        let batch2 = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![
                    "s3://bucket/table/data/file2.parquet",
                    "s3://bucket/table/data/file2.parquet",
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![15, 20])) as ArrayRef,
            ],
        )?;

        position_delete_writer.write(batch1).await?;
        position_delete_writer.write(batch2).await?;

        let result = position_delete_writer.close().await?;

        assert_eq!(result.len(), 1);
        let data_file = &result[0];

        // Verify referenced_data_file is NOT set when batches reference different files
        assert_eq!(
            data_file.referenced_data_file(),
            None,
            "referenced_data_file should NOT be set when batches reference different files"
        );
        assert_eq!(data_file.content_type(), DataContentType::PositionDeletes);
        assert_eq!(data_file.record_count, 4);

        Ok(())
    }

    #[tokio::test]
    async fn test_null_file_path_rejected() -> Result<(), anyhow::Error> {
        // Test that position deletes with null file_path values are rejected per Iceberg spec
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // Create schema with nullable file_path to allow testing null validation
        // (In practice, Iceberg spec requires non-null, but we need nullable Arrow type to create the test data)
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("file_path", DataType::Utf8, true).with_metadata(HashMap::from([(
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

        // Create a batch with a null file_path (violates Iceberg spec)
        use arrow_array::builder::StringBuilder;
        let mut file_path_builder = StringBuilder::new();
        file_path_builder.append_value("s3://bucket/file1.parquet");
        file_path_builder.append_null(); // Invalid!
        file_path_builder.append_value("s3://bucket/file1.parquet");
        let file_paths = Arc::new(file_path_builder.finish()) as ArrayRef;

        let positions = Arc::new(Int64Array::from(vec![5, 10, 15])) as ArrayRef;

        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![file_paths, positions])?;

        // Writing should fail with a clear error message
        let result = position_delete_writer.write(batch).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err
            .message()
            .contains("file_path column in position delete file must not contain null values"));

        Ok(())
    }

    #[tokio::test]
    async fn test_referenced_data_file_optimization_with_multiple_output_files(
    ) -> Result<(), anyhow::Error> {
        // This test validates the batch_cumulative_start tracking fix.
        // The scenario: write multiple batches that together exceed target file size,
        // causing the RollingFileWriter to create multiple output files.
        // All batches reference the same data file, so all output files should have
        // referenced_data_file set correctly.
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

        // Use a VERY SMALL target file size to force multiple output files
        let pb = ParquetWriterBuilder::new(WriterProperties::builder().build(), schema);
        let rolling_writer_builder = RollingFileWriterBuilder::new(
            pb,
            1, // Extremely small size to force rollover
            file_io.clone(),
            location_gen,
            file_name_gen,
        );
        let mut position_delete_writer =
            PositionDeleteFileWriterBuilder::new(rolling_writer_builder)
                .build(None)
                .await?;

        let target_data_file = "s3://bucket/table/data/file1.parquet";

        // Write multiple batches, each with 100 rows, all referencing the same data file
        for i in 0..10 {
            let file_paths = Arc::new(StringArray::from(
                (0..100)
                    .map(|_| target_data_file)
                    .collect::<Vec<_>>(),
            )) as ArrayRef;
            let positions = Arc::new(Int64Array::from(
                (i * 100..(i + 1) * 100).collect::<Vec<_>>(),
            )) as ArrayRef;

            let batch =
                RecordBatch::try_new(arrow_schema.clone(), vec![file_paths, positions])?;
            position_delete_writer.write(batch).await?;
        }

        let result = position_delete_writer.close().await?;

        // With very small target size, we should get multiple output files
        assert!(
            result.len() > 1,
            "Expected multiple output files with small target size, got {} file(s)",
            result.len()
        );

        // CRITICAL ASSERTION: All output files should have referenced_data_file set
        // because all batches referenced the same data file
        for (idx, data_file) in result.iter().enumerate() {
            assert_eq!(
                data_file.referenced_data_file(),
                Some(target_data_file.to_string()),
                "File {} should have referenced_data_file set",
                idx
            );
            assert_eq!(data_file.content_type(), DataContentType::PositionDeletes);
        }

        // Verify total row count matches all batches combined
        let total_rows: u64 = result.iter().map(|f| f.record_count).sum();
        assert_eq!(total_rows, 1000, "Total rows across all files should equal all batches");

        Ok(())
    }

    #[tokio::test]
    async fn test_empty_batch_handling() -> Result<(), anyhow::Error> {
        // Test that empty batches don't cause issues
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

        // Write an empty batch
        let empty_batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef,
                Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef,
            ],
        )?;

        position_delete_writer.write(empty_batch).await?;

        // Write a normal batch after the empty one
        let normal_batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![
                    "s3://bucket/file1.parquet",
                    "s3://bucket/file1.parquet",
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![5, 10])) as ArrayRef,
            ],
        )?;

        position_delete_writer.write(normal_batch).await?;
        let result = position_delete_writer.close().await?;

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].record_count, 2); // Only the normal batch's rows
        assert_eq!(
            result[0].referenced_data_file(),
            Some("s3://bucket/file1.parquet".to_string())
        );

        Ok(())
    }
}
