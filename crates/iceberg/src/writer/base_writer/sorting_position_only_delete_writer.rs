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

//! Sorting position-delete writer for unordered row-level delete input.
//! De-duplicates and emits rows in (file_path, pos) order.

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};

use crate::error::invalid_data;
use crate::spec::{DataFile, PartitionKey};
use crate::writer::base_writer::position_delete_input::PositionDeletes;
use crate::writer::base_writer::position_delete_writer::{
    PositionDeleteFileWriter, PositionDeleteFileWriterBuilder, validate_position_delete_batch,
};
use crate::writer::file_writer::FileWriterBuilder;
use crate::writer::file_writer::location_generator::{FileNameGenerator, LocationGenerator};
use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use crate::writer::{IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind, Result};

/// Number of sorted position-delete rows sent to the underlying writer per Arrow batch.
const DEFAULT_FLUSH_ROWS: usize = 8192;

/// Builder for [`SortingPositionOnlyDeleteWriter`].
///
/// Deletes written through one instance must belong to the same partition/spec.
#[derive(Debug)]
pub struct SortingPositionOnlyDeleteWriterBuilder<
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
> {
    inner: RollingFileWriterBuilder<B, L, F>,
    flush_rows: usize,
}

impl<B, L, F> SortingPositionOnlyDeleteWriterBuilder<B, L, F>
where
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
{
    /// Wraps a rolling position-delete writer with sorting and de-duplication.
    pub fn new(inner: RollingFileWriterBuilder<B, L, F>) -> Self {
        Self {
            inner,
            flush_rows: DEFAULT_FLUSH_ROWS,
        }
    }

    /// Sets the maximum number of rows emitted per Arrow batch.
    #[cfg(test)]
    pub(crate) fn with_flush_rows(mut self, flush_rows: usize) -> Self {
        self.flush_rows = flush_rows;
        self
    }
}

#[async_trait::async_trait]
impl<B, L, F> IcebergWriterBuilder for SortingPositionOnlyDeleteWriterBuilder<B, L, F>
where
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
{
    type R = SortingPositionOnlyDeleteWriter<B, L, F>;

    async fn build(&self, partition_key: Option<PartitionKey>) -> Result<Self::R> {
        if self.flush_rows == 0 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Sorting position-only delete writer flush_rows must be greater than zero.",
            ));
        }

        let inner = PositionDeleteFileWriterBuilder::new(self.inner.clone())
            .build(partition_key)
            .await?;
        Ok(SortingPositionOnlyDeleteWriter {
            inner: Some(inner),
            positions: PositionDeletes::new(),
            flush_rows: self.flush_rows,
        })
    }
}

/// Buffers unordered position deletes and writes sorted, de-duplicated records.
///
/// The index remains in memory until close.
#[derive(Debug)]
pub struct SortingPositionOnlyDeleteWriter<
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
> {
    inner: Option<PositionDeleteFileWriter<B, L, F>>,
    positions: PositionDeletes,
    flush_rows: usize,
}

impl<B, L, F> SortingPositionOnlyDeleteWriter<B, L, F>
where
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
{
    /// Adds one delete record. Duplicate entries are ignored.
    ///
    /// Returns [`ErrorKind::DataInvalid`] when `position` cannot be represented by
    /// the Iceberg `long` position-delete column.
    pub fn write_delete(&mut self, file_path: impl AsRef<str>, position: u64) -> Result<()> {
        self.ensure_open()?;
        if position > i64::MAX as u64 {
            return Err(invalid_data!(
                "Position delete row position exceeds i64::MAX, got {position}."
            ));
        }

        self.positions.insert_dedup(file_path, position);
        Ok(())
    }

    fn ensure_open(&self) -> Result<()> {
        if self.inner.is_none() {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Sorting position-only delete writer is already closed.",
            ));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl<B, L, F> IcebergWriter for SortingPositionOnlyDeleteWriter<B, L, F>
where
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
{
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.ensure_open()?;
        validate_position_delete_batch(&batch)?;

        let paths = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| invalid_data!("Position-delete file_path must be a StringArray."))?;
        let positions = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| invalid_data!("Position-delete pos must be an Int64Array."))?;

        if paths.null_count() != 0 || positions.null_count() != 0 {
            return Err(invalid_data!(
                "Position-delete file_path and pos values must not be null."
            ));
        }
        if positions.values().iter().any(|position| *position < 0) {
            return Err(invalid_data!(
                "Position-delete row positions must be non-negative."
            ));
        }

        // Validate the complete batch before changing writer state.
        for index in 0..batch.num_rows() {
            self.positions
                .insert_dedup(paths.value(index), positions.value(index) as u64);
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<Vec<DataFile>> {
        self.ensure_open()?;

        let positions = std::mem::take(&mut self.positions);
        let flush_rows = self.flush_rows;
        let mut inner = self.inner.take().ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "Sorting position-only delete writer is already closed.",
            )
        })?;

        let write_result: Result<()> = async {
            for batch in positions.to_record_batches(flush_rows) {
                inner.write(batch?).await?;
            }
            Ok(())
        }
        .await;

        if let Err(err) = write_result {
            // Close the underlying writer on failure without returning partial output.
            let _ = inner.close().await;
            return Err(err);
        }

        inner.close().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use super::*;
    use crate::arrow::schema_to_arrow_schema;
    use crate::io::FileIO;
    use crate::spec::DataFileFormat;
    use crate::writer::base_writer::position_delete_writer::position_delete_schema;
    use crate::writer::file_writer::ParquetWriterBuilder;
    use crate::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;

    fn setup(
        file_prefix: &str,
        target_file_size: usize,
    ) -> (
        TempDir,
        FileIO,
        RollingFileWriterBuilder<
            ParquetWriterBuilder,
            DefaultLocationGenerator,
            DefaultFileNameGenerator,
        >,
    ) {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let location_generator = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_generator =
            DefaultFileNameGenerator::new(file_prefix.to_string(), None, DataFileFormat::Parquet);
        let parquet_writer = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            position_delete_schema(),
        );
        let rolling_writer = RollingFileWriterBuilder::new(
            parquet_writer,
            target_file_size,
            file_io.clone(),
            location_generator,
            file_name_generator,
        );
        (temp_dir, file_io, rolling_writer)
    }

    fn delete_batch(paths: Vec<&str>, row_positions: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(schema_to_arrow_schema(&position_delete_schema()).unwrap());
        RecordBatch::try_new(schema, vec![
            Arc::new(StringArray::from(paths)),
            Arc::new(Int64Array::from(row_positions)),
        ])
        .unwrap()
    }

    async fn read_rows(file_io: &FileIO, file: &DataFile) -> Vec<(String, i64)> {
        let input = file_io
            .new_input(file.file_path.clone())
            .unwrap()
            .read()
            .await
            .unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(input)
            .unwrap()
            .build()
            .unwrap();
        let mut rows = Vec::new();
        for batch in reader {
            let batch = batch.unwrap();
            let paths = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let positions = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            rows.extend(
                paths
                    .iter()
                    .zip(positions.iter())
                    .map(|(path, position)| (path.unwrap().to_owned(), position.unwrap())),
            );
        }
        rows
    }

    #[tokio::test]
    async fn sorts_and_deduplicates_across_batches() -> Result<()> {
        let (_temp_dir, file_io, rolling_writer) = setup("sorted_pos_delete", usize::MAX);
        let mut writer = SortingPositionOnlyDeleteWriterBuilder::new(rolling_writer)
            .build(None)
            .await?;
        writer
            .write(delete_batch(vec!["b.parquet", "a.parquet"], vec![8, 9]))
            .await?;
        writer.write_delete("a.parquet", 9)?;
        writer
            .write(delete_batch(
                vec!["a.parquet", "a.parquet", "b.parquet"],
                vec![2, 9, 1],
            ))
            .await?;

        let files = writer.close().await?;
        assert_eq!(files.len(), 1);
        for field_id in [
            crate::metadata_columns::RESERVED_FIELD_ID_DELETE_FILE_PATH,
            crate::metadata_columns::RESERVED_FIELD_ID_DELETE_FILE_POS,
        ] {
            assert!(!files[0].value_counts().contains_key(&field_id));
            assert!(!files[0].null_value_counts().contains_key(&field_id));
            assert!(!files[0].lower_bounds().contains_key(&field_id));
            assert!(!files[0].upper_bounds().contains_key(&field_id));
        }
        let rows = read_rows(&file_io, &files[0]).await;
        assert_eq!(rows, vec![
            ("a.parquet".to_string(), 2),
            ("a.parquet".to_string(), 9),
            ("b.parquet".to_string(), 1),
            ("b.parquet".to_string(), 8),
        ]);
        Ok(())
    }

    #[tokio::test]
    async fn close_without_rows_returns_no_files() -> Result<()> {
        let (_temp_dir, _file_io, rolling_writer) = setup("empty_pos_delete", usize::MAX);
        let mut writer = SortingPositionOnlyDeleteWriterBuilder::new(rolling_writer)
            .build(None)
            .await?;
        assert!(writer.close().await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn rejects_negative_positions_before_mutating_the_batch() -> Result<()> {
        let (_temp_dir, _file_io, rolling_writer) = setup("negative_pos_delete", usize::MAX);
        let mut writer = SortingPositionOnlyDeleteWriterBuilder::new(rolling_writer)
            .build(None)
            .await?;
        assert!(
            writer
                .write(delete_batch(vec!["a.parquet", "b.parquet"], vec![1, -1]))
                .await
                .is_err()
        );
        assert!(writer.close().await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn rolling_writer_metadata_is_returned_for_every_sorted_batch() -> Result<()> {
        const FLUSH_ROWS: usize = 64;
        let (_temp_dir, file_io, rolling_writer) = setup("rolling_pos_delete", 1);
        let mut writer = SortingPositionOnlyDeleteWriterBuilder::new(rolling_writer)
            .with_flush_rows(FLUSH_ROWS)
            .build(None)
            .await?;
        for position in 0..(FLUSH_ROWS as u64 + 1) {
            writer.write_delete("a.parquet", position)?;
        }

        let files = writer.close().await?;
        assert!(files.len() >= 2);
        let mut rows = Vec::new();
        for file in &files {
            rows.extend(read_rows(&file_io, file).await);
        }
        assert_eq!(rows.len(), FLUSH_ROWS + 1);
        assert!(rows.windows(2).all(|pair| pair[0].1 < pair[1].1));
        Ok(())
    }

    #[tokio::test]
    async fn keeps_metrics_scoped_per_rolled_output() -> Result<()> {
        const FLUSH_ROWS: usize = 3;
        let (_temp_dir, file_io, rolling_writer) = setup("scoped_rolling_delete", 1);
        let mut writer = SortingPositionOnlyDeleteWriterBuilder::new(rolling_writer)
            .with_flush_rows(FLUSH_ROWS)
            .build(None)
            .await?;

        writer.write_delete("a.parquet", 0)?;
        writer.write_delete("a.parquet", 1)?;
        writer.write_delete("b.parquet", 0)?;
        writer.write_delete("b.parquet", 1)?;

        let files = writer.close().await?;
        assert_eq!(files.len(), 2);

        let first_rows = read_rows(&file_io, &files[0]).await;
        assert_eq!(first_rows, vec![
            ("a.parquet".to_string(), 0),
            ("a.parquet".to_string(), 1),
            ("b.parquet".to_string(), 0),
        ]);
        for field_id in [
            crate::metadata_columns::RESERVED_FIELD_ID_DELETE_FILE_PATH,
            crate::metadata_columns::RESERVED_FIELD_ID_DELETE_FILE_POS,
        ] {
            assert!(!files[0].lower_bounds().contains_key(&field_id));
            assert!(!files[0].upper_bounds().contains_key(&field_id));
        }

        let second_rows = read_rows(&file_io, &files[1]).await;
        assert_eq!(second_rows, vec![("b.parquet".to_string(), 1)]);
        for field_id in [
            crate::metadata_columns::RESERVED_FIELD_ID_DELETE_FILE_PATH,
            crate::metadata_columns::RESERVED_FIELD_ID_DELETE_FILE_POS,
        ] {
            assert!(files[1].lower_bounds().contains_key(&field_id));
            assert_eq!(
                files[1].lower_bounds().get(&field_id),
                files[1].upper_bounds().get(&field_id)
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn matches_iceberg_java_char_sequence_order_and_accepts_large_positions() -> Result<()> {
        let (_temp_dir, file_io, rolling_writer) = setup("lexical_pos_delete", usize::MAX);
        let mut writer = SortingPositionOnlyDeleteWriterBuilder::new(rolling_writer)
            .build(None)
            .await?;
        writer.write_delete("\u{e000}.parquet", 0)?;
        writer.write_delete("\u{10000}.parquet", i64::MAX as u64)?;

        let files = writer.close().await?;
        let rows = read_rows(&file_io, &files[0]).await;
        assert_eq!(rows, vec![
            ("\u{e000}.parquet".to_string(), 0),
            ("\u{10000}.parquet".to_string(), i64::MAX),
        ]);
        Ok(())
    }

    #[tokio::test]
    async fn rejects_position_above_i64_max_before_mutating() -> Result<()> {
        let (_temp_dir, _file_io, rolling_writer) = setup("position_above_i64_max", usize::MAX);
        let mut writer = SortingPositionOnlyDeleteWriterBuilder::new(rolling_writer)
            .build(None)
            .await?;

        let err = writer
            .write_delete("a.parquet", i64::MAX as u64 + 1)
            .expect_err("position above i64::MAX must be rejected");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(writer.close().await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn rejects_zero_flush_rows() -> Result<()> {
        let (_temp_dir, _file_io, rolling_writer) = setup("zero_flush_rows", usize::MAX);
        let err = SortingPositionOnlyDeleteWriterBuilder::new(rolling_writer)
            .with_flush_rows(0)
            .build(None)
            .await
            .expect_err("expected invalid flush_rows error");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        Ok(())
    }

    #[tokio::test]
    async fn close_is_one_shot() -> Result<()> {
        let (_temp_dir, _file_io, rolling_writer) = setup("one_shot_pos_delete", usize::MAX);
        let mut writer = SortingPositionOnlyDeleteWriterBuilder::new(rolling_writer)
            .build(None)
            .await?;
        assert!(writer.close().await?.is_empty());
        assert!(writer.close().await.is_err());
        Ok(())
    }
}
