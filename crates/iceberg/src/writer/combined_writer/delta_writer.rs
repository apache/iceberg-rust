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

//! Delta writers handle row-level changes by combining data file and delete file writers.
//!
//! The delta writer has three sub-writers:
//! - A data file writer for new and updated rows.
//! - A position delete file writer for deletions of existing rows (that have been written within this writer)
//! - An equality delete file writer for deletions of rows based on equality conditions (for rows that may exist in other data files).
//!
//! # Input Data Format
//!
//! The `DeltaWriter` expects input data as Arrow `RecordBatch` with a specific structure:
//!
//! **Required Schema:**
//! - All data columns from your table schema (in order)
//! - A final column containing operation indicators as `Int32Array`:
//!   - [`OP_INSERT`] (`1`) = Insert/Update (write to data file)
//!   - [`OP_DELETE`] (`-1`) = Delete (write to delete file)
//!
//! **Example Schema:**
//! ```text
//! ┌─────────────┬──────────────┬──────────────┬──────┐
//! │ id (Int32)  │ name (Utf8)  │ value (Int32)│ _ops │
//! ├─────────────┼──────────────┼──────────────┼──────┤
//! │ 1           │ "Alice"      │ 100          │  1   │  <- Insert
//! │ 2           │ "Bob"        │ 200          │  1   │  <- Insert
//! │ 1           │ "Alice"      │ 150          │ -1   │  <- Delete
//! │ 3           │ "Charlie"    │ 300          │  1   │  <- Insert
//! └─────────────┴──────────────┴──────────────┴──────┘
//! ```
//!
//! # Unique Columns (Row Identity)
//!
//! The writer uses `unique_cols` (specified as Iceberg field IDs) to uniquely identify rows.
//! These columns form a composite key used for:
//! - Tracking rows written in this session (for position deletes)
//! - Generating equality delete predicates (for rows outside this session)
//!
//! Typically, this would be your table's primary key columns.
//!
//! # Memory Management
//!
//! The writer tracks recently written rows to enable efficient position deletes.
//! The `max_seen_rows` parameter controls this behavior:
//!
//! - **Default (100,000)**: Track up to 100K recently written rows
//!   - Deletes for tracked rows → Position deletes (most efficient)
//!   - Deletes for older/evicted rows → Equality deletes
//!
//! - **Custom value**: Adjust based on your workload
//!   - Higher = more position deletes, more memory usage
//!   - Lower = more equality deletes, less memory usage
//!
//! - **Zero (0)**: Disable row tracking completely
//!   - All deletes → Equality deletes
//!   - No memory overhead, but slower reads
//!
//! # How It Works
//!
//! When you call `write()` with a batch:
//!
//! 1. The batch is partitioned by the operations column
//! 2. For each partition:
//!    - **Insert batches** (`ops = OP_INSERT`):
//!      - Written to data file writer
//!      - Row positions recorded in memory (up to `max_seen_rows`)
//!    - **Delete batches** (`ops = OP_DELETE`):
//!      - If row exists in tracked positions → Position delete file
//!      - If row is unknown or evicted → Equality delete file
//!
//! 3. On `close()`, all three writers are closed and their data files are returned
//!
//! # Example Usage
//!
//! ```ignore
//! use arrow_array::{Int32Array, RecordBatch, StringArray};
//! use iceberg::writer::DeltaWriterBuilder;
//!
//! // Build a delta writer with unique columns [field_id: 1] (the "id" column)
//! let delta_writer = DeltaWriterBuilder::new(
//!     data_writer_builder,
//!     pos_delete_writer_builder,
//!     eq_delete_writer_builder,
//!     vec![1], // field IDs of unique columns
//! )
//! .with_max_seen_rows(50_000) // Track 50K rows
//! .build(None)
//! .await?;
//!
//! // Create a batch with inserts and deletes
//! let batch = RecordBatch::try_new(
//!     schema.clone(),
//!     vec![
//!         Arc::new(Int32Array::from(vec![1, 2, 1])),      // id
//!         Arc::new(StringArray::from(vec!["Alice", "Bob", "Alice"])), // name
//!         Arc::new(Int32Array::from(vec![100, 200, -100])), // value
//!         Arc::new(Int32Array::from(vec![OP_INSERT, OP_INSERT, OP_DELETE])), // ops
//!     ],
//! )?;
//!
//! delta_writer.write(batch).await?;
//! let data_files = delta_writer.close().await?;
//! ```

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use arrow_array::builder::BooleanBuilder;
use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow_ord::partition::partition;
use arrow_row::{OwnedRow, RowConverter, Rows, SortField};
use arrow_select::filter::filter_record_batch;
use itertools::Itertools;

use crate::arrow::record_batch_projector::RecordBatchProjector;
use crate::spec::{DataFile, PartitionKey};
use crate::writer::base_writer::position_delete_writer::PositionDeleteWriterConfig;
use crate::writer::{CurrentFileStatus, IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind, Result};

/// Default maximum number of rows to track for position deletes.
/// This limits memory usage for large streaming workloads.
pub const DEFAULT_MAX_SEEN_ROWS: usize = 100_000;

/// Operation marker for insert/update operations in the DeltaWriter input.
/// When the operations column contains this value, the row is written to a data file.
pub const OP_INSERT: i32 = 1;

/// Operation marker for delete operations in the DeltaWriter input.
/// When the operations column contains this value, the row is written to a delete file.
pub const OP_DELETE: i32 = -1;

/// A builder for `DeltaWriter`.
#[derive(Clone, Debug)]
pub struct DeltaWriterBuilder<DWB, PDWB, EDWB> {
    data_writer_builder: DWB,
    pos_delete_writer_builder: PDWB,
    eq_delete_writer_builder: EDWB,
    unique_cols: Vec<i32>,
    max_seen_rows: usize,
}

impl<DWB, PDWB, EDWB> DeltaWriterBuilder<DWB, PDWB, EDWB> {
    /// Creates a new `DeltaWriterBuilder`.
    pub fn new(
        data_writer_builder: DWB,
        pos_delete_writer_builder: PDWB,
        eq_delete_writer_builder: EDWB,
        unique_cols: Vec<i32>,
    ) -> Self {
        Self {
            data_writer_builder,
            pos_delete_writer_builder,
            eq_delete_writer_builder,
            unique_cols,
            max_seen_rows: DEFAULT_MAX_SEEN_ROWS,
        }
    }

    /// Sets the maximum number of rows to track for position deletes.
    ///
    /// When this limit is reached, the oldest tracked rows are evicted.
    /// Deletes for evicted rows will use equality deletes instead of
    /// position deletes. Default is [`DEFAULT_MAX_SEEN_ROWS`].
    ///
    /// Set to `0` to disable row tracking entirely, causing all deletes
    /// to use equality deletes. This eliminates memory overhead but may
    /// reduce read performance.
    pub fn with_max_seen_rows(mut self, max_seen_rows: usize) -> Self {
        self.max_seen_rows = max_seen_rows;
        self
    }
}

#[async_trait::async_trait]
impl<DWB, PDWB, EDWB> IcebergWriterBuilder for DeltaWriterBuilder<DWB, PDWB, EDWB>
where
    DWB: IcebergWriterBuilder,
    PDWB: IcebergWriterBuilder,
    EDWB: IcebergWriterBuilder,
    DWB::R: CurrentFileStatus,
{
    type R = DeltaWriter<DWB::R, PDWB::R, EDWB::R>;
    async fn build(&self, partition_key: Option<PartitionKey>) -> Result<Self::R> {
        let data_writer = self
            .data_writer_builder
            .build(partition_key.clone())
            .await?;
        let pos_delete_writer = self
            .pos_delete_writer_builder
            .build(partition_key.clone())
            .await?;
        let eq_delete_writer = self.eq_delete_writer_builder.build(partition_key).await?;
        DeltaWriter::try_new(
            data_writer,
            pos_delete_writer,
            eq_delete_writer,
            self.unique_cols.clone(),
            self.max_seen_rows,
        )
    }
}

/// Position information of a row in a data file.
pub struct Position {
    row_index: i64,
    file_path: String,
}

/// A writer that handles row-level changes by combining data file and delete file writers.
pub struct DeltaWriter<DW, PDW, EDW> {
    /// The data file writer for new and updated rows.
    pub data_writer: DW,
    /// The position delete file writer for deletions of existing rows (that have been written within
    /// this writer).
    pub pos_delete_writer: PDW,
    /// The equality delete file writer for deletions of rows based on equality conditions (for rows
    /// that may exist in other data files).
    pub eq_delete_writer: EDW,
    /// The list of unique columns used for equality deletes.
    pub unique_cols: Vec<i32>,
    /// A map of rows (projected to unique columns) to their corresponding position information.
    pub seen_rows: HashMap<OwnedRow, Position>,
    /// Tracks insertion order for seen_rows to enable FIFO eviction.
    seen_rows_order: VecDeque<OwnedRow>,
    /// Maximum number of rows to track for position deletes.
    max_seen_rows: usize,
    /// A projector to project the record batch to the unique columns.
    pub(crate) projector: RecordBatchProjector,
    /// A converter to convert the projected columns to rows for easy comparison.
    pub(crate) row_convertor: RowConverter,
}

impl<DW, PDW, EDW> DeltaWriter<DW, PDW, EDW>
where
    DW: IcebergWriter + CurrentFileStatus,
    PDW: IcebergWriter,
    EDW: IcebergWriter,
{
    fn try_new(
        data_writer: DW,
        pos_delete_writer: PDW,
        eq_delete_writer: EDW,
        unique_cols: Vec<i32>,
        max_seen_rows: usize,
    ) -> Result<Self> {
        let projector =
            RecordBatchProjector::from_iceberg_schema(data_writer.current_schema(), &unique_cols)?;

        let row_convertor = RowConverter::new(
            projector
                .projected_schema_ref()
                .fields()
                .iter()
                .map(|f| SortField::new(f.data_type().clone()))
                .collect(),
        )?;

        Ok(Self {
            data_writer,
            pos_delete_writer,
            eq_delete_writer,
            unique_cols,
            seen_rows: HashMap::new(),
            seen_rows_order: VecDeque::new(),
            max_seen_rows,
            projector,
            row_convertor,
        })
    }

    async fn insert(&mut self, batch: RecordBatch) -> Result<()> {
        let batch_num_rows = batch.num_rows();

        // Write first to ensure the data is persisted before updating our tracking state.
        // This prevents inconsistent state if the write fails.
        // Note: We must write before calling current_file_path() because the underlying
        // writer may not have created the file yet (lazy initialization).
        self.data_writer.write(batch.clone()).await?;

        // Skip row tracking if disabled (max_seen_rows == 0)
        if self.max_seen_rows == 0 {
            return Ok(());
        }

        let rows = self.extract_unique_column_rows(&batch)?;

        // Get file path and calculate start_row_index after successful write
        let file_path = self.data_writer.current_file_path();
        let end_row_num = self.data_writer.current_row_num();
        let start_row_index = end_row_num - batch_num_rows;

        // Record positions for each row in this batch
        for (i, row) in rows.iter().enumerate() {
            let owned_row = row.owned();
            self.seen_rows.insert(owned_row.clone(), Position {
                row_index: start_row_index as i64 + i as i64,
                file_path: file_path.clone(),
            });
            self.seen_rows_order.push_back(owned_row);
        }

        // Evict oldest entries if we exceed the limit
        self.evict_oldest_seen_rows();

        Ok(())
    }

    /// Evicts the oldest entries from seen_rows when the limit is exceeded.
    /// Entries that were already deleted are skipped (stale entries in the order queue).
    fn evict_oldest_seen_rows(&mut self) {
        while self.seen_rows.len() > self.max_seen_rows {
            if let Some(old_row) = self.seen_rows_order.pop_front() {
                // Only count as eviction if the row still exists (wasn't already deleted)
                self.seen_rows.remove(&old_row);
            } else {
                // Queue is empty but HashMap still has entries - this shouldn't happen
                // in normal operation, but break to avoid infinite loop
                break;
            }
        }
    }

    async fn delete(&mut self, batch: RecordBatch) -> Result<()> {
        // If row tracking is disabled, write all deletes as equality deletes
        if self.max_seen_rows == 0 {
            self.eq_delete_writer
                .write(batch)
                .await
                .map_err(|e| Error::new(ErrorKind::Unexpected, format!("{e}")))?;
            return Ok(());
        }

        let rows = self.extract_unique_column_rows(&batch)?;
        let mut file_array = vec![];
        let mut row_index_array = vec![];
        // Build a boolean array to track which rows need equality deletes.
        // True = row not seen before, needs equality delete
        // False = row was seen, already handled via position delete
        let mut needs_equality_delete = BooleanBuilder::new();

        for row in rows.iter() {
            if let Some(pos) = self.seen_rows.remove(&row.owned()) {
                // Row was previously inserted, use position delete
                row_index_array.push(pos.row_index);
                file_array.push(pos.file_path.clone());
                needs_equality_delete.append_value(false);
            } else {
                // Row not seen before, use equality delete
                needs_equality_delete.append_value(true);
            }
        }

        // Write position deletes for rows that were previously inserted
        let file_array: ArrayRef = Arc::new(StringArray::from(file_array));
        let row_index_array: ArrayRef = Arc::new(arrow_array::Int64Array::from(row_index_array));

        let position_batch =
            RecordBatch::try_new(PositionDeleteWriterConfig::arrow_schema(), vec![
                file_array,
                row_index_array,
            ])?;

        if position_batch.num_rows() > 0 {
            self.pos_delete_writer
                .write(position_batch)
                .await
                .map_err(|e| Error::new(ErrorKind::Unexpected, format!("{e}")))?;
        }

        // Write equality deletes for rows that were not previously inserted
        let eq_batch = filter_record_batch(&batch, &needs_equality_delete.finish())
            .map_err(|e| Error::new(ErrorKind::Unexpected, format!("{e}")))?;

        if eq_batch.num_rows() > 0 {
            self.eq_delete_writer
                .write(eq_batch)
                .await
                .map_err(|e| Error::new(ErrorKind::Unexpected, format!("{e}")))?;
        }

        Ok(())
    }

    fn extract_unique_column_rows(&mut self, batch: &RecordBatch) -> Result<Rows> {
        self.row_convertor
            .convert_columns(&self.projector.project_column(batch.columns())?)
            .map_err(|e| Error::new(ErrorKind::Unexpected, format!("{e}")))
    }
}

#[async_trait::async_trait]
impl<DW, PDW, EDW> IcebergWriter for DeltaWriter<DW, PDW, EDW>
where
    DW: IcebergWriter + CurrentFileStatus,
    PDW: IcebergWriter,
    EDW: IcebergWriter,
{
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        // Treat the last row as an op indicator +1 for insert, -1 for delete
        let ops = batch
            .column(batch.num_columns() - 1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or(Error::new(
                ErrorKind::Unexpected,
                "Failed to downcast ops column",
            ))?;

        let partition =
            partition(&[batch.column(batch.num_columns() - 1).clone()]).map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("Failed to partition batch: {e}"),
                )
            })?;

        for range in partition.ranges() {
            let batch = batch
                .project(&(0..batch.num_columns() - 1).collect_vec())
                .map_err(|e| {
                    Error::new(
                        ErrorKind::Unexpected,
                        format!("Failed to project batch columns: {e}"),
                    )
                })?
                .slice(range.start, range.end - range.start);
            match ops.value(range.start) {
                OP_INSERT => self.insert(batch).await?,
                OP_DELETE => self.delete(batch).await?,
                op => {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        format!(
                            "Ops column must be {OP_INSERT} (insert) or {OP_DELETE} (delete), not {op}"
                        ),
                    ));
                }
            }
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<Vec<DataFile>> {
        let data_files = self.data_writer.close().await?;
        let pos_delete_files = self.pos_delete_writer.close().await?;
        let eq_delete_files = self.eq_delete_writer.close().await?;

        Ok(data_files
            .into_iter()
            .chain(pos_delete_files)
            .chain(eq_delete_files)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod delta_writer_tests {
        use std::collections::HashMap;

        use arrow_array::{Int32Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use parquet::file::properties::WriterProperties;
        use tempfile::TempDir;

        use super::*;
        use crate::arrow::arrow_schema_to_schema;
        use crate::io::{FileIOBuilder, LocalFsStorageFactory};
        use crate::spec::{
            DataFileFormat, NestedField, PrimitiveType, Schema as IcebergSchema, Type,
        };
        use crate::writer::IcebergWriterBuilder;
        use crate::writer::base_writer::data_file_writer::DataFileWriterBuilder;
        use crate::writer::base_writer::equality_delete_writer::{
            EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig,
        };
        use crate::writer::base_writer::position_delete_writer::PositionDeleteFileWriterBuilder;
        use crate::writer::file_writer::ParquetWriterBuilder;
        use crate::writer::file_writer::location_generator::{
            DefaultFileNameGenerator, DefaultLocationGenerator,
        };
        use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;

        fn create_iceberg_schema() -> Arc<IcebergSchema> {
            Arc::new(
                IcebergSchema::builder()
                    .with_schema_id(0)
                    .with_fields(vec![
                        NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                        NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String))
                            .into(),
                    ])
                    .build()
                    .unwrap(),
            )
        }

        fn create_test_batch_with_ops(
            ids: Vec<i32>,
            names: Vec<Option<&str>>,
            ops: Vec<i32>,
        ) -> RecordBatch {
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "1".to_string(),
                )])),
                Field::new("name", DataType::Utf8, true).with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "2".to_string(),
                )])),
                Field::new("op", DataType::Int32, false),
            ]));

            let id_array: ArrayRef = Arc::new(Int32Array::from(ids));
            let name_array: ArrayRef = Arc::new(StringArray::from(names));
            let op_array: ArrayRef = Arc::new(Int32Array::from(ops));

            RecordBatch::try_new(schema, vec![id_array, name_array, op_array]).unwrap()
        }

        #[tokio::test]
        async fn test_delta_writer_insert_only() -> Result<()> {
            let temp_dir = TempDir::new().unwrap();
            let file_io = FileIOBuilder::new(Arc::new(LocalFsStorageFactory)).build();
            let schema = create_iceberg_schema();

            // Create data writer
            let data_location_gen = DefaultLocationGenerator::with_data_location(format!(
                "{}/data",
                temp_dir.path().to_str().unwrap()
            ));
            let data_file_name_gen =
                DefaultFileNameGenerator::new("data".to_string(), None, DataFileFormat::Parquet);
            let data_parquet_writer =
                ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone());
            let data_rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
                data_parquet_writer,
                schema.clone(),
                file_io.clone(),
                data_location_gen,
                data_file_name_gen,
            );
            let data_writer = DataFileWriterBuilder::new(data_rolling_writer_builder);

            // Create position delete writer
            let pos_delete_schema = Arc::new(arrow_schema_to_schema(
                &PositionDeleteWriterConfig::arrow_schema(),
            )?);
            let pos_delete_location_gen = DefaultLocationGenerator::with_data_location(format!(
                "{}/pos_delete",
                temp_dir.path().to_str().unwrap()
            ));
            let pos_delete_file_name_gen = DefaultFileNameGenerator::new(
                "pos_delete".to_string(),
                None,
                DataFileFormat::Parquet,
            );
            let pos_delete_parquet_writer = ParquetWriterBuilder::new(
                WriterProperties::builder().build(),
                pos_delete_schema.clone(),
            );
            let pos_delete_rolling_writer_builder =
                RollingFileWriterBuilder::new_with_default_file_size(
                    pos_delete_parquet_writer,
                    pos_delete_schema,
                    file_io.clone(),
                    pos_delete_location_gen,
                    pos_delete_file_name_gen,
                );
            let pos_delete_writer = PositionDeleteFileWriterBuilder::new(
                pos_delete_rolling_writer_builder,
                PositionDeleteWriterConfig::new(None, 0, None),
            );

            // Create equality delete writer
            let eq_delete_config = EqualityDeleteWriterConfig::new(vec![1], schema.clone())?;
            let eq_delete_schema = Arc::new(arrow_schema_to_schema(
                eq_delete_config.projected_arrow_schema_ref(),
            )?);
            let eq_delete_location_gen = DefaultLocationGenerator::with_data_location(format!(
                "{}/eq_delete",
                temp_dir.path().to_str().unwrap()
            ));
            let eq_delete_file_name_gen = DefaultFileNameGenerator::new(
                "eq_delete".to_string(),
                None,
                DataFileFormat::Parquet,
            );
            let eq_delete_parquet_writer = ParquetWriterBuilder::new(
                WriterProperties::builder().build(),
                eq_delete_schema.clone(),
            );
            let eq_delete_rolling_writer_builder =
                RollingFileWriterBuilder::new_with_default_file_size(
                    eq_delete_parquet_writer,
                    eq_delete_schema,
                    file_io.clone(),
                    eq_delete_location_gen,
                    eq_delete_file_name_gen,
                );
            let eq_delete_writer = EqualityDeleteFileWriterBuilder::new(
                eq_delete_rolling_writer_builder,
                eq_delete_config,
            );

            // Create delta writer
            let data_writer_instance = data_writer.build(None).await?;
            let pos_delete_writer_instance = pos_delete_writer.build(None).await?;
            let eq_delete_writer_instance = eq_delete_writer.build(None).await?;
            let mut delta_writer = DeltaWriter::try_new(
                data_writer_instance,
                pos_delete_writer_instance,
                eq_delete_writer_instance,
                vec![1], // unique on id column
                DEFAULT_MAX_SEEN_ROWS,
            )?;

            // Write batch with only inserts
            let batch = create_test_batch_with_ops(
                vec![1, 2, 3],
                vec![Some("Alice"), Some("Bob"), Some("Charlie")],
                vec![OP_INSERT, OP_INSERT, OP_INSERT], // all inserts
            );

            delta_writer.write(batch).await?;
            let data_files = delta_writer.close().await?;

            // Should have 1 data file, 0 delete files
            assert_eq!(data_files.len(), 1);
            assert_eq!(data_files[0].content, crate::spec::DataContentType::Data);
            assert_eq!(data_files[0].record_count, 3);

            // Read back and verify
            let input_file = file_io.new_input(data_files[0].file_path.clone())?;
            let content = input_file.read().await?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(content)?.build()?;
            let batches: Vec<_> = reader.map(|b| b.unwrap()).collect();
            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].num_rows(), 3);

            Ok(())
        }

        #[tokio::test]
        async fn test_delta_writer_insert_then_position_delete() -> Result<()> {
            let temp_dir = TempDir::new().unwrap();
            let file_io = FileIOBuilder::new(Arc::new(LocalFsStorageFactory)).build();
            let schema = create_iceberg_schema();

            // Create writers (same setup as above)
            let data_location_gen = DefaultLocationGenerator::with_data_location(format!(
                "{}/data",
                temp_dir.path().to_str().unwrap()
            ));
            let data_file_name_gen =
                DefaultFileNameGenerator::new("data".to_string(), None, DataFileFormat::Parquet);
            let data_parquet_writer =
                ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone());
            let data_rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
                data_parquet_writer,
                schema.clone(),
                file_io.clone(),
                data_location_gen,
                data_file_name_gen,
            );
            let data_writer = DataFileWriterBuilder::new(data_rolling_writer_builder);

            let pos_delete_schema = Arc::new(arrow_schema_to_schema(
                &PositionDeleteWriterConfig::arrow_schema(),
            )?);
            let pos_delete_location_gen = DefaultLocationGenerator::with_data_location(format!(
                "{}/pos_delete",
                temp_dir.path().to_str().unwrap()
            ));
            let pos_delete_file_name_gen = DefaultFileNameGenerator::new(
                "pos_delete".to_string(),
                None,
                DataFileFormat::Parquet,
            );
            let pos_delete_parquet_writer = ParquetWriterBuilder::new(
                WriterProperties::builder().build(),
                pos_delete_schema.clone(),
            );
            let pos_delete_rolling_writer_builder =
                RollingFileWriterBuilder::new_with_default_file_size(
                    pos_delete_parquet_writer,
                    pos_delete_schema,
                    file_io.clone(),
                    pos_delete_location_gen,
                    pos_delete_file_name_gen,
                );
            let pos_delete_writer = PositionDeleteFileWriterBuilder::new(
                pos_delete_rolling_writer_builder,
                PositionDeleteWriterConfig::new(None, 0, None),
            );

            let eq_delete_config = EqualityDeleteWriterConfig::new(vec![1], schema.clone())?;
            let eq_delete_schema = Arc::new(arrow_schema_to_schema(
                eq_delete_config.projected_arrow_schema_ref(),
            )?);
            let eq_delete_location_gen = DefaultLocationGenerator::with_data_location(format!(
                "{}/eq_delete",
                temp_dir.path().to_str().unwrap()
            ));
            let eq_delete_file_name_gen = DefaultFileNameGenerator::new(
                "eq_delete".to_string(),
                None,
                DataFileFormat::Parquet,
            );
            let eq_delete_parquet_writer = ParquetWriterBuilder::new(
                WriterProperties::builder().build(),
                eq_delete_schema.clone(),
            );
            let eq_delete_rolling_writer_builder =
                RollingFileWriterBuilder::new_with_default_file_size(
                    eq_delete_parquet_writer,
                    eq_delete_schema,
                    file_io.clone(),
                    eq_delete_location_gen,
                    eq_delete_file_name_gen,
                );
            let eq_delete_writer = EqualityDeleteFileWriterBuilder::new(
                eq_delete_rolling_writer_builder,
                eq_delete_config,
            );

            let data_writer_instance = data_writer.build(None).await?;
            let pos_delete_writer_instance = pos_delete_writer.build(None).await?;
            let eq_delete_writer_instance = eq_delete_writer.build(None).await?;
            let mut delta_writer = DeltaWriter::try_new(
                data_writer_instance,
                pos_delete_writer_instance,
                eq_delete_writer_instance,
                vec![1],
                DEFAULT_MAX_SEEN_ROWS,
            )?;

            // First, insert some rows
            let insert_batch = create_test_batch_with_ops(
                vec![1, 2, 3],
                vec![Some("Alice"), Some("Bob"), Some("Charlie")],
                vec![OP_INSERT, OP_INSERT, OP_INSERT],
            );
            delta_writer.write(insert_batch).await?;

            // Now delete rows that were just inserted (should create position deletes)
            let delete_batch =
                create_test_batch_with_ops(vec![1, 2], vec![Some("Alice"), Some("Bob")], vec![
                    -1, -1,
                ]);
            delta_writer.write(delete_batch).await?;

            let data_files = delta_writer.close().await?;

            // Should have 1 data file + 1 position delete file
            assert_eq!(data_files.len(), 2);

            let data_file = data_files
                .iter()
                .find(|f| f.content == crate::spec::DataContentType::Data)
                .unwrap();
            let pos_delete_file = data_files
                .iter()
                .find(|f| f.content == crate::spec::DataContentType::PositionDeletes)
                .unwrap();

            assert_eq!(data_file.record_count, 3);
            assert_eq!(pos_delete_file.record_count, 2);

            // Verify position delete file content
            let input_file = file_io.new_input(pos_delete_file.file_path.clone())?;
            let content = input_file.read().await?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(content)?.build()?;
            let batches: Vec<_> = reader.map(|b| b.unwrap()).collect();
            assert_eq!(batches[0].num_rows(), 2);

            Ok(())
        }

        #[tokio::test]
        async fn test_delta_writer_equality_delete() -> Result<()> {
            let temp_dir = TempDir::new().unwrap();
            let file_io = FileIOBuilder::new(Arc::new(LocalFsStorageFactory)).build();
            let schema = create_iceberg_schema();

            // Create writers
            let data_location_gen = DefaultLocationGenerator::with_data_location(format!(
                "{}/data",
                temp_dir.path().to_str().unwrap()
            ));
            let data_file_name_gen =
                DefaultFileNameGenerator::new("data".to_string(), None, DataFileFormat::Parquet);
            let data_parquet_writer =
                ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone());
            let data_rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
                data_parquet_writer,
                schema.clone(),
                file_io.clone(),
                data_location_gen,
                data_file_name_gen,
            );
            let data_writer = DataFileWriterBuilder::new(data_rolling_writer_builder);

            let pos_delete_schema = Arc::new(arrow_schema_to_schema(
                &PositionDeleteWriterConfig::arrow_schema(),
            )?);
            let pos_delete_location_gen = DefaultLocationGenerator::with_data_location(format!(
                "{}/pos_delete",
                temp_dir.path().to_str().unwrap()
            ));
            let pos_delete_file_name_gen = DefaultFileNameGenerator::new(
                "pos_delete".to_string(),
                None,
                DataFileFormat::Parquet,
            );
            let pos_delete_parquet_writer = ParquetWriterBuilder::new(
                WriterProperties::builder().build(),
                pos_delete_schema.clone(),
            );
            let pos_delete_rolling_writer_builder =
                RollingFileWriterBuilder::new_with_default_file_size(
                    pos_delete_parquet_writer,
                    pos_delete_schema,
                    file_io.clone(),
                    pos_delete_location_gen,
                    pos_delete_file_name_gen,
                );
            let pos_delete_writer = PositionDeleteFileWriterBuilder::new(
                pos_delete_rolling_writer_builder,
                PositionDeleteWriterConfig::new(None, 0, None),
            );

            let eq_delete_config = EqualityDeleteWriterConfig::new(vec![1], schema.clone())?;
            let eq_delete_schema = Arc::new(arrow_schema_to_schema(
                eq_delete_config.projected_arrow_schema_ref(),
            )?);
            let eq_delete_location_gen = DefaultLocationGenerator::with_data_location(format!(
                "{}/eq_delete",
                temp_dir.path().to_str().unwrap()
            ));
            let eq_delete_file_name_gen = DefaultFileNameGenerator::new(
                "eq_delete".to_string(),
                None,
                DataFileFormat::Parquet,
            );
            let eq_delete_parquet_writer = ParquetWriterBuilder::new(
                WriterProperties::builder().build(),
                eq_delete_schema.clone(),
            );
            let eq_delete_rolling_writer_builder =
                RollingFileWriterBuilder::new_with_default_file_size(
                    eq_delete_parquet_writer,
                    eq_delete_schema,
                    file_io.clone(),
                    eq_delete_location_gen,
                    eq_delete_file_name_gen,
                );
            let eq_delete_writer = EqualityDeleteFileWriterBuilder::new(
                eq_delete_rolling_writer_builder,
                eq_delete_config,
            );

            let data_writer_instance = data_writer.build(None).await?;
            let pos_delete_writer_instance = pos_delete_writer.build(None).await?;
            let eq_delete_writer_instance = eq_delete_writer.build(None).await?;
            let mut delta_writer = DeltaWriter::try_new(
                data_writer_instance,
                pos_delete_writer_instance,
                eq_delete_writer_instance,
                vec![1],
                DEFAULT_MAX_SEEN_ROWS,
            )?;

            // Delete rows that were never inserted (should create equality deletes)
            let delete_batch =
                create_test_batch_with_ops(vec![99, 100], vec![Some("X"), Some("Y")], vec![
                    OP_DELETE, OP_DELETE,
                ]);
            delta_writer.write(delete_batch).await?;

            let data_files = delta_writer.close().await?;

            // Should have only 1 equality delete file
            assert_eq!(data_files.len(), 1);
            assert_eq!(
                data_files[0].content,
                crate::spec::DataContentType::EqualityDeletes
            );
            assert_eq!(data_files[0].record_count, 2);

            Ok(())
        }

        #[tokio::test]
        async fn test_delta_writer_invalid_op() -> Result<()> {
            let temp_dir = TempDir::new().unwrap();
            let file_io = FileIOBuilder::new(Arc::new(LocalFsStorageFactory)).build();
            let schema = create_iceberg_schema();

            // Create writers
            let data_location_gen = DefaultLocationGenerator::with_data_location(format!(
                "{}/data",
                temp_dir.path().to_str().unwrap()
            ));
            let data_file_name_gen =
                DefaultFileNameGenerator::new("data".to_string(), None, DataFileFormat::Parquet);
            let data_parquet_writer =
                ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone());
            let data_rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
                data_parquet_writer,
                schema.clone(),
                file_io.clone(),
                data_location_gen,
                data_file_name_gen,
            );
            let data_writer = DataFileWriterBuilder::new(data_rolling_writer_builder);

            let pos_delete_schema = Arc::new(arrow_schema_to_schema(
                &PositionDeleteWriterConfig::arrow_schema(),
            )?);
            let pos_delete_location_gen = DefaultLocationGenerator::with_data_location(format!(
                "{}/pos_delete",
                temp_dir.path().to_str().unwrap()
            ));
            let pos_delete_file_name_gen = DefaultFileNameGenerator::new(
                "pos_delete".to_string(),
                None,
                DataFileFormat::Parquet,
            );
            let pos_delete_parquet_writer = ParquetWriterBuilder::new(
                WriterProperties::builder().build(),
                pos_delete_schema.clone(),
            );
            let pos_delete_rolling_writer_builder =
                RollingFileWriterBuilder::new_with_default_file_size(
                    pos_delete_parquet_writer,
                    pos_delete_schema,
                    file_io.clone(),
                    pos_delete_location_gen,
                    pos_delete_file_name_gen,
                );
            let pos_delete_writer = PositionDeleteFileWriterBuilder::new(
                pos_delete_rolling_writer_builder,
                PositionDeleteWriterConfig::new(None, 0, None),
            );

            let eq_delete_config = EqualityDeleteWriterConfig::new(vec![1], schema.clone())?;
            let eq_delete_schema = Arc::new(arrow_schema_to_schema(
                eq_delete_config.projected_arrow_schema_ref(),
            )?);
            let eq_delete_location_gen = DefaultLocationGenerator::with_data_location(format!(
                "{}/eq_delete",
                temp_dir.path().to_str().unwrap()
            ));
            let eq_delete_file_name_gen = DefaultFileNameGenerator::new(
                "eq_delete".to_string(),
                None,
                DataFileFormat::Parquet,
            );
            let eq_delete_parquet_writer = ParquetWriterBuilder::new(
                WriterProperties::builder().build(),
                eq_delete_schema.clone(),
            );
            let eq_delete_rolling_writer_builder =
                RollingFileWriterBuilder::new_with_default_file_size(
                    eq_delete_parquet_writer,
                    eq_delete_schema,
                    file_io.clone(),
                    eq_delete_location_gen,
                    eq_delete_file_name_gen,
                );
            let eq_delete_writer = EqualityDeleteFileWriterBuilder::new(
                eq_delete_rolling_writer_builder,
                eq_delete_config,
            );

            let data_writer_instance = data_writer.build(None).await?;
            let pos_delete_writer_instance = pos_delete_writer.build(None).await?;
            let eq_delete_writer_instance = eq_delete_writer.build(None).await?;
            let mut delta_writer = DeltaWriter::try_new(
                data_writer_instance,
                pos_delete_writer_instance,
                eq_delete_writer_instance,
                vec![1],
                DEFAULT_MAX_SEEN_ROWS,
            )?;

            // Invalid operation code
            let batch = create_test_batch_with_ops(vec![1], vec![Some("Alice")], vec![99]);

            let result = delta_writer.write(batch).await;
            assert!(result.is_err());
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("Ops column must be 1 (insert) or -1 (delete)")
            );

            Ok(())
        }
    }
}
