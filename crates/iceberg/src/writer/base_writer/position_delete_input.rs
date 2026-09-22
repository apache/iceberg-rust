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

//! Accumulates position delete rows and renders them as spec-conforming [`RecordBatch`]es.
//!
//! [`PositionDeletes`] collects `(file_path, pos)` pairs and emits the two required position
//! delete columns — `file_path` (`Utf8`) and `pos` (`Int64`) — sorted by `(file_path, pos)`
//! with duplicate pairs removed, as the spec requires. Rows can be drawn either as one batch
//! ([`to_record_batch`](PositionDeletes::to_record_batch)) or, to bound peak memory over a
//! large delete set, lazily as fixed-size batches
//! ([`to_record_batches`](PositionDeletes::to_record_batches)). Either batch is exactly what
//! [`PositionDeleteFileWriter`](super::position_delete_writer::PositionDeleteFileWriter)
//! accepts, so it can be handed straight to that writer.
//!
//! Position delete files are a v2 construct. v3 tables use deletion vectors and forbid adding
//! new position delete files, so a format-version gate must be applied at the
//! transaction/commit layer before routing v3 writes here.

// Nothing wires `PositionDeletes` into a writer yet; the write-path plumbing lands in a follow-up.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::builder::{Int64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use roaring::RoaringTreemap;

use crate::{Error, ErrorKind, Result};

/// Accumulates `(file_path, pos)` position delete rows.
///
/// Keying paths in a [`BTreeMap`] and positions in a [`RoaringTreemap`] deduplicates repeated
/// `(file_path, pos)` pairs and, when iterated, yields the spec-required ascending
/// `(file_path, pos)` order for free, so callers may [`insert`](Self::insert) rows in any
/// order.
#[derive(Debug, Default)]
pub(crate) struct PositionDeletes {
    rows: BTreeMap<String, RoaringTreemap>,
}

impl PositionDeletes {
    /// Creates an empty accumulator.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Records that row `pos` of `path` is deleted. `pos` is a row position, so it is
    /// non-negative by construction.
    ///
    /// Re-inserting the same `(path, pos)` is idempotent: the backing [`RoaringTreemap`]
    /// collapses the duplicate, so the pair is recorded once. This silent dedup is
    /// intentional — a position delete file must not list the same `(file_path, pos)` twice,
    /// so repeated inserts of one deleted row are a no-op rather than an error.
    pub(crate) fn insert(&mut self, path: impl Into<String>, pos: u64) {
        self.rows.entry(path.into()).or_default().insert(pos);
    }

    /// Returns whether no positions have been recorded. `insert` never leaves an empty
    /// treemap behind, so an empty map means no positions.
    pub(crate) fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Returns the total number of recorded positions across all paths.
    pub(crate) fn len(&self) -> usize {
        self.rows
            .values()
            .map(|positions| positions.len())
            .sum::<u64>() as usize
    }

    /// Renders all accumulated rows as one spec-conforming position delete [`RecordBatch`].
    ///
    /// The batch has exactly the two required position delete columns, built against the
    /// writer's shared [`position_delete_arrow_schema`](super::position_delete_writer::position_delete_arrow_schema)
    /// so the reserved field ids stay wired in. Rows are emitted sorted by `(file_path, pos)`
    /// with duplicate pairs removed. An empty accumulator yields a valid 0-row batch.
    ///
    /// This materializes every row at once; prefer [`to_record_batches`](Self::to_record_batches)
    /// to bound peak memory over a large delete set.
    pub(crate) fn to_record_batch(&self) -> Result<RecordBatch> {
        // `usize::MAX` never caps a real position count, so this drains everything into one
        // batch (a 0-row batch when empty).
        Self::build_batch(&mut self.stream().peekable(), usize::MAX)
    }

    /// Lazily renders the accumulated rows as position delete [`RecordBatch`]es of at most
    /// `max_rows` rows each, so the whole delete set is never materialized at once.
    ///
    /// Batches carry the two required columns in ascending `(file_path, pos)` order with
    /// duplicate pairs removed, split on `max_rows` boundaries; a single path whose positions
    /// overflow one batch simply resumes in the next. Each batch is built on demand as the
    /// iterator is polled — nothing is pre-collected — and uses the writer's shared
    /// [`position_delete_arrow_schema`](super::position_delete_writer::position_delete_arrow_schema).
    /// An empty accumulator yields no batches.
    ///
    /// `max_rows` must be greater than zero. Rather than panic, a `max_rows` of `0` makes the
    /// iterator yield a single [`ErrorKind::DataInvalid`] error and then stop.
    pub(crate) fn to_record_batches(
        &self,
        max_rows: usize,
    ) -> impl Iterator<Item = Result<RecordBatch>> + '_ {
        let mut rows = self.stream().peekable();
        let mut done = false;
        std::iter::from_fn(move || {
            if done {
                return None;
            }
            if max_rows == 0 {
                // Invalid, but never panic: surface it once and stop the iterator.
                done = true;
                return Some(Err(Error::new(
                    ErrorKind::DataInvalid,
                    "max_rows must be greater than 0",
                )));
            }
            // Nothing left to emit -> end the iteration.
            rows.peek()?;
            let batch = Self::build_batch(&mut rows, max_rows);
            // A failed batch (e.g. a position overflowing `i64`) leaves its offending row
            // unconsumed, so stop after the first error rather than retry it forever.
            if batch.is_err() {
                done = true;
            }
            Some(batch)
        })
    }

    /// Flattens the accumulator into its rows in ascending `(file_path, pos)` order.
    ///
    /// The `BTreeMap` iterates paths ascending and each `RoaringTreemap` iterates positions
    /// ascending, so the stream is sorted by `(file_path, pos)` and already deduplicated.
    fn stream(&self) -> impl Iterator<Item = (&str, u64)> + '_ {
        self.rows
            .iter()
            .flat_map(|(path, positions)| positions.iter().map(move |pos| (path.as_str(), pos)))
    }

    /// Builds one position delete [`RecordBatch`] of at most `max_rows` rows drained from
    /// `rows`, which must yield rows already in `(file_path, pos)` order.
    ///
    /// Consecutive rows sharing a path form a run appended to the `file_path` column in a
    /// single [`StringBuilder::append_value_n`] call, keeping that column run-length friendly
    /// rather than repeating the path per row. The batch is built against the writer's shared
    /// Arrow projection (a cheap `Arc` clone) so the reserved field ids stay wired in. An
    /// exhausted `rows` yields a valid 0-row batch.
    fn build_batch<'a, I>(
        rows: &mut std::iter::Peekable<I>,
        max_rows: usize,
    ) -> Result<RecordBatch>
    where
        I: Iterator<Item = (&'a str, u64)>,
    {
        let mut path_builder = StringBuilder::new();
        let mut pos_builder = Int64Builder::new();

        let mut taken = 0usize;
        while taken < max_rows {
            let Some(&(path, _)) = rows.peek() else {
                break;
            };
            // Consume this path's run: contiguous rows sharing `path`, up to the batch's cap.
            let mut run = 0usize;
            while taken < max_rows {
                let Some(&(next_path, pos)) = rows.peek() else {
                    break;
                };
                if next_path != path {
                    break;
                }
                // The `pos` column is `Int64`; positions are `u64`, so guard the top of the
                // range with a checked conversion rather than a silent wrap.
                let pos = i64::try_from(pos)
                    .map_err(|_| Error::new(ErrorKind::DataInvalid, "position exceeds i64::MAX"))?;
                pos_builder.append_value(pos);
                rows.next();
                run += 1;
                taken += 1;
            }
            // One append per run of `run` rows, not one per row.
            path_builder.append_value_n(path, run);
        }

        let schema = super::position_delete_writer::position_delete_arrow_schema();
        RecordBatch::try_new(schema, vec![
            Arc::new(path_builder.finish()) as ArrayRef,
            Arc::new(pos_builder.finish()) as ArrayRef,
        ])
        .map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Failed to build position delete record batch: {e}"),
            )
        })
    }
}

#[cfg(test)]
mod test {
    use arrow_array::{Array, Int64Array, StringArray};
    use arrow_schema::DataType;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use super::*;
    use crate::io::FileIO;
    use crate::metadata_columns::{
        RESERVED_COL_NAME_DELETE_FILE_PATH, RESERVED_COL_NAME_DELETE_FILE_POS,
        RESERVED_FIELD_ID_DELETE_FILE_PATH, RESERVED_FIELD_ID_DELETE_FILE_POS,
    };
    use crate::spec::{DataContentType, DataFileFormat};
    use crate::writer::base_writer::position_delete_writer::{
        PositionDeleteFileWriterBuilder, position_delete_schema,
    };
    use crate::writer::file_writer::ParquetWriterBuilder;
    use crate::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use crate::writer::{IcebergWriter, IcebergWriterBuilder};

    /// Asserts a batch has the exact two-column position delete schema: names, Arrow types,
    /// non-null flags, and the reserved field ids in the Parquet field-id metadata.
    fn assert_position_delete_schema(batch: &RecordBatch) {
        assert_eq!(batch.num_columns(), 2);
        let schema = batch.schema();

        let path = schema.field(0);
        assert_eq!(path.name(), RESERVED_COL_NAME_DELETE_FILE_PATH);
        assert_eq!(path.data_type(), &DataType::Utf8);
        assert!(!path.is_nullable());
        assert_eq!(
            path.metadata().get(PARQUET_FIELD_ID_META_KEY).unwrap(),
            &RESERVED_FIELD_ID_DELETE_FILE_PATH.to_string()
        );

        let pos = schema.field(1);
        assert_eq!(pos.name(), RESERVED_COL_NAME_DELETE_FILE_POS);
        assert_eq!(pos.data_type(), &DataType::Int64);
        assert!(!pos.is_nullable());
        assert_eq!(
            pos.metadata().get(PARQUET_FIELD_ID_META_KEY).unwrap(),
            &RESERVED_FIELD_ID_DELETE_FILE_POS.to_string()
        );
    }

    /// Extracts the `(file_path, pos)` columns of a batch as parallel vectors.
    fn columns(batch: &RecordBatch) -> (Vec<String>, Vec<i64>) {
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
        assert_eq!(paths.null_count(), 0);
        assert_eq!(positions.null_count(), 0);
        (
            paths.iter().map(|s| s.unwrap().to_string()).collect(),
            positions.values().to_vec(),
        )
    }

    #[test]
    fn test_column_shape_and_field_ids() {
        let mut deletes = PositionDeletes::new();
        deletes.insert("s3://bucket/data/f0.parquet", 1);
        let batch = deletes.to_record_batch().unwrap();
        assert_position_delete_schema(&batch);
    }

    #[test]
    fn test_unsorted_inserts_sorted_by_path_then_pos() {
        let mut deletes = PositionDeletes::new();
        // Inserted out of order, across multiple paths; both the paths and the positions
        // within a path must come out sorted.
        deletes.insert("s3://bucket/data/f1.parquet", 2);
        deletes.insert("s3://bucket/data/f0.parquet", 4);
        deletes.insert("s3://bucket/data/f0.parquet", 1);

        let batch = deletes.to_record_batch().unwrap();
        let (paths, positions) = columns(&batch);
        assert_eq!(paths, vec![
            "s3://bucket/data/f0.parquet",
            "s3://bucket/data/f0.parquet",
            "s3://bucket/data/f1.parquet",
        ]);
        assert_eq!(positions, vec![1, 4, 2]);
    }

    #[test]
    fn test_duplicate_pairs_deduped() {
        let mut deletes = PositionDeletes::new();
        deletes.insert("s3://bucket/data/f0.parquet", 3);
        deletes.insert("s3://bucket/data/f0.parquet", 3);
        deletes.insert("s3://bucket/data/f0.parquet", 1);

        assert_eq!(deletes.len(), 2);
        let batch = deletes.to_record_batch().unwrap();
        let (paths, positions) = columns(&batch);
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(paths, vec![
            "s3://bucket/data/f0.parquet",
            "s3://bucket/data/f0.parquet",
        ]);
        assert_eq!(positions, vec![1, 3]);
    }

    #[test]
    fn test_len_and_is_empty() {
        let mut deletes = PositionDeletes::new();
        assert!(deletes.is_empty());
        assert_eq!(deletes.len(), 0);

        deletes.insert("s3://bucket/data/f0.parquet", 1);
        deletes.insert("s3://bucket/data/f0.parquet", 2);
        deletes.insert("s3://bucket/data/f1.parquet", 9);
        assert!(!deletes.is_empty());
        assert_eq!(deletes.len(), 3);

        // A duplicate pair does not change the count.
        deletes.insert("s3://bucket/data/f0.parquet", 1);
        assert_eq!(deletes.len(), 3);
    }

    #[test]
    fn test_empty_builds_valid_zero_row_batch() {
        let deletes = PositionDeletes::new();
        let batch = deletes.to_record_batch().unwrap();
        assert_eq!(batch.num_rows(), 0);
        // Even with no rows the schema must be the full position delete schema.
        assert_position_delete_schema(&batch);
    }

    #[test]
    fn test_position_above_i64_max_rejected() {
        let mut deletes = PositionDeletes::new();
        // A position beyond `i64::MAX` cannot fit the `Int64` column and must be rejected
        // rather than silently wrapped.
        deletes.insert("s3://bucket/data/f0.parquet", i64::MAX as u64 + 1);
        let err = deletes.to_record_batch().unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    /// Drains [`PositionDeletes::to_record_batches`] into per-batch `(paths, positions)`
    /// column pairs, unwrapping each batch.
    fn batched_columns(deletes: &PositionDeletes, max_rows: usize) -> Vec<(Vec<String>, Vec<i64>)> {
        deletes
            .to_record_batches(max_rows)
            .map(|batch| columns(&batch.unwrap()))
            .collect()
    }

    /// Concatenates per-batch column pairs into one flattened `(paths, positions)` pair.
    fn flatten(batches: &[(Vec<String>, Vec<i64>)]) -> (Vec<String>, Vec<i64>) {
        let mut paths = Vec::new();
        let mut positions = Vec::new();
        for (batch_paths, batch_positions) in batches {
            paths.extend(batch_paths.iter().cloned());
            positions.extend(batch_positions.iter().copied());
        }
        (paths, positions)
    }

    #[test]
    fn test_to_record_batches_splits_and_preserves_order() {
        let mut deletes = PositionDeletes::new();
        // Unsorted inserts across three paths; 6 rows total.
        for (path, pos) in [
            ("s3://bucket/data/f1.parquet", 5),
            ("s3://bucket/data/f0.parquet", 7),
            ("s3://bucket/data/f0.parquet", 1),
            ("s3://bucket/data/f2.parquet", 9),
            ("s3://bucket/data/f1.parquet", 2),
            ("s3://bucket/data/f0.parquet", 4),
        ] {
            deletes.insert(path, pos);
        }

        // 6 rows at 2 per batch -> exactly 3 full batches.
        let batches = batched_columns(&deletes, 2);
        assert_eq!(batches.len(), 3);
        for (paths, positions) in &batches {
            assert_eq!(paths.len(), 2);
            assert_eq!(positions.len(), 2);
        }
        // Every streamed batch carries the full position delete schema.
        for batch in deletes.to_record_batches(2) {
            assert_position_delete_schema(&batch.unwrap());
        }

        // Flattened, the stream is the same rows in the same sorted order as the single batch.
        let (flat_paths, flat_positions) = flatten(&batches);
        let (whole_paths, whole_positions) = columns(&deletes.to_record_batch().unwrap());
        assert_eq!(flat_paths, whole_paths);
        assert_eq!(flat_positions, whole_positions);
        assert_eq!(whole_paths, vec![
            "s3://bucket/data/f0.parquet",
            "s3://bucket/data/f0.parquet",
            "s3://bucket/data/f0.parquet",
            "s3://bucket/data/f1.parquet",
            "s3://bucket/data/f1.parquet",
            "s3://bucket/data/f2.parquet",
        ]);
        assert_eq!(whole_positions, vec![1, 4, 7, 2, 5, 9]);
    }

    #[test]
    fn test_to_record_batches_uneven_last_batch() {
        let mut deletes = PositionDeletes::new();
        for pos in 0..5 {
            deletes.insert("s3://bucket/data/f0.parquet", pos);
        }

        // 5 rows at 2 per batch -> two full batches and a 1-row remainder.
        let sizes: Vec<usize> = deletes
            .to_record_batches(2)
            .map(|batch| batch.unwrap().num_rows())
            .collect();
        assert_eq!(sizes, vec![2, 2, 1]);
    }

    #[test]
    fn test_to_record_batches_path_spans_chunk_boundary() {
        let mut deletes = PositionDeletes::new();
        // A single path with more positions than fit one batch: its run must resume across
        // batch boundaries and stay correct.
        for pos in 1..=5 {
            deletes.insert("s3://bucket/data/f0.parquet", pos);
        }

        let batches = batched_columns(&deletes, 2);
        assert_eq!(batches.len(), 3);
        // Same path in every batch, positions still ascending and contiguous across the split.
        for (paths, _) in &batches {
            assert!(paths.iter().all(|p| p == "s3://bucket/data/f0.parquet"));
        }
        let (paths, positions) = flatten(&batches);
        assert_eq!(paths.len(), 5);
        assert_eq!(positions, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_to_record_batches_zero_max_rows_yields_error_without_panic() {
        let mut deletes = PositionDeletes::new();
        deletes.insert("s3://bucket/data/f0.parquet", 1);

        let mut it = deletes.to_record_batches(0);
        // A `max_rows` of 0 is invalid, but must surface as an error rather than a panic...
        let err = it.next().expect("expected one error item").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        // ...exactly once, after which the iterator is exhausted.
        assert!(it.next().is_none());
    }

    #[test]
    fn test_to_record_batches_stops_after_overflow_error() {
        let mut deletes = PositionDeletes::new();
        // A position beyond `i64::MAX` cannot fit the `Int64` column; the streaming path must
        // surface the error and then stop rather than retry the unconsumed row forever.
        deletes.insert("s3://bucket/data/f0.parquet", i64::MAX as u64 + 1);

        let mut it = deletes.to_record_batches(2);
        let err = it.next().expect("expected one error item").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(it.next().is_none());
    }

    #[test]
    fn test_to_record_batches_empty_yields_no_batches() {
        let deletes = PositionDeletes::new();
        // Empty accumulator streams nothing...
        assert_eq!(deletes.to_record_batches(4).count(), 0);
        // ...while the single-batch path still returns a valid 0-row batch.
        let batch = deletes.to_record_batch().unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_position_delete_schema(&batch);
    }

    #[test]
    fn test_to_record_batches_dedups_repeated_pairs() {
        let mut deletes = PositionDeletes::new();
        deletes.insert("s3://bucket/data/f0.parquet", 3);
        deletes.insert("s3://bucket/data/f0.parquet", 3);
        deletes.insert("s3://bucket/data/f0.parquet", 1);

        // The duplicate collapses, so a generous batch holds just the two distinct rows.
        let batches = batched_columns(&deletes, 100);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].0, vec![
            "s3://bucket/data/f0.parquet",
            "s3://bucket/data/f0.parquet",
        ]);
        assert_eq!(batches[0].1, vec![1, 3]);
    }

    #[test]
    fn test_to_record_batches_repeats_path_across_run() {
        let mut deletes = PositionDeletes::new();
        // A run of one path followed by a second path, all within a single batch: the
        // `append_value_n` run must repeat the file_path value once per position.
        for pos in [1, 2, 3, 4] {
            deletes.insert("s3://bucket/data/f0.parquet", pos);
        }
        deletes.insert("s3://bucket/data/f1.parquet", 8);

        let batches = batched_columns(&deletes, 100);
        assert_eq!(batches.len(), 1);
        let (paths, positions) = &batches[0];
        assert_eq!(paths, &vec![
            "s3://bucket/data/f0.parquet",
            "s3://bucket/data/f0.parquet",
            "s3://bucket/data/f0.parquet",
            "s3://bucket/data/f0.parquet",
            "s3://bucket/data/f1.parquet",
        ]);
        assert_eq!(positions, &vec![1, 2, 3, 4, 8]);
    }

    /// Wires a real [`PositionDeleteFileWriter`] over a local filesystem for the integration
    /// test: the file IO, location/name generators, and the Parquet-backed rolling writer
    /// configured with the [`position_delete_schema`].
    fn writer_setup(
        temp_dir: &TempDir,
    ) -> PositionDeleteFileWriterBuilder<
        ParquetWriterBuilder,
        DefaultLocationGenerator,
        DefaultFileNameGenerator,
    > {
        let file_io = FileIO::new_with_fs();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);
        let parquet_writer_builder = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            position_delete_schema(),
        );
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            file_io,
            location_gen,
            file_name_gen,
        );
        PositionDeleteFileWriterBuilder::new(rolling_writer_builder)
    }

    #[tokio::test]
    async fn test_batch_feeds_position_delete_writer() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let builder = writer_setup(&temp_dir);
        let mut writer = builder.build(None).await?;

        let mut deletes = PositionDeletes::new();
        // Deliberately unsorted; `to_record_batch` sorts to the order the writer needs.
        deletes.insert("s3://bucket/data/f1.parquet", 2);
        deletes.insert("s3://bucket/data/f0.parquet", 4);
        deletes.insert("s3://bucket/data/f0.parquet", 1);

        writer.write(deletes.to_record_batch()?).await?;
        let data_files = writer.close().await?;

        assert_eq!(data_files.len(), 1);
        let data_file = &data_files[0];
        assert_eq!(data_file.content_type(), DataContentType::PositionDeletes);
        assert_eq!(data_file.record_count, deletes.len() as u64);

        Ok(())
    }

    #[tokio::test]
    async fn test_streamed_batches_feed_position_delete_writer() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let builder = writer_setup(&temp_dir);
        let mut writer = builder.build(None).await?;

        let mut deletes = PositionDeletes::new();
        // Deliberately unsorted, across paths, more rows than the batch cap so more than one
        // batch is streamed to the writer.
        deletes.insert("s3://bucket/data/f1.parquet", 2);
        deletes.insert("s3://bucket/data/f0.parquet", 4);
        deletes.insert("s3://bucket/data/f0.parquet", 1);
        deletes.insert("s3://bucket/data/f1.parquet", 5);
        deletes.insert("s3://bucket/data/f2.parquet", 9);

        // Each fixed-size batch is a valid position delete batch the writer accepts, and the
        // batches arrive in the sorted order the writer requires.
        let mut streamed = 0usize;
        for batch in deletes.to_record_batches(2) {
            writer.write(batch?).await?;
            streamed += 1;
        }
        assert_eq!(streamed, 3);

        let data_files = writer.close().await?;
        assert_eq!(data_files.len(), 1);
        let data_file = &data_files[0];
        assert_eq!(data_file.content_type(), DataContentType::PositionDeletes);
        assert_eq!(data_file.record_count, deletes.len() as u64);

        Ok(())
    }
}
