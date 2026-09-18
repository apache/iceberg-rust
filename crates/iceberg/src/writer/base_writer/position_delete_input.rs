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

//! Accumulates position delete rows and renders them as a spec-conforming [`RecordBatch`].
//!
//! [`PositionDeletes`] collects `(file_path, pos)` pairs and, on
//! [`to_record_batch`](PositionDeletes::to_record_batch), emits the two required position
//! delete columns — `file_path` (`Utf8`) and `pos` (`Int64`) — sorted by `(file_path, pos)`
//! with duplicate pairs removed, as the spec requires. That batch is exactly what
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
    /// non-negative by construction. Re-inserting the same `(path, pos)` is a no-op.
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

    /// Renders the accumulated rows as a spec-conforming position delete [`RecordBatch`].
    ///
    /// The batch has exactly the two required position delete columns, built against the
    /// writer's shared [`position_delete_arrow_schema`](super::position_delete_writer::position_delete_arrow_schema)
    /// so the reserved field ids stay wired in. Rows are emitted sorted by `(file_path, pos)`
    /// with duplicate pairs removed. An empty accumulator yields a valid 0-row batch.
    pub(crate) fn to_record_batch(&self) -> Result<RecordBatch> {
        let mut path_builder = StringBuilder::new();
        let mut pos_builder = Int64Builder::new();
        // The `BTreeMap` iterates paths ascending and each `RoaringTreemap` iterates positions
        // ascending, so the columns come out sorted by `(file_path, pos)` and deduplicated.
        for (path, positions) in &self.rows {
            for pos in positions.iter() {
                // The `pos` column is `Int64`; positions are `u64`, so guard the top of the
                // range with a checked conversion rather than a silent wrap.
                let pos = i64::try_from(pos)
                    .map_err(|_| Error::new(ErrorKind::DataInvalid, "position exceeds i64::MAX"))?;
                path_builder.append_value(path);
                pos_builder.append_value(pos);
            }
        }

        // Reuse the writer's crate-internal Arrow projection (a cheap `Arc` clone) so the
        // field-id wiring stays shared.
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
}
