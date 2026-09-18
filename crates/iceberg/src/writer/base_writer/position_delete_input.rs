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

//! Assembles a spec-conforming position delete [`RecordBatch`] from `(file_path, pos)` rows.
//!
//! A [`PositionDeleteFileWriter`](super::position_delete_writer::PositionDeleteFileWriter)
//! accepts batches with exactly the two required position delete columns: `file_path`
//! (`Utf8`, field id [`crate::metadata_columns::RESERVED_FIELD_ID_DELETE_FILE_PATH`]) and
//! `pos` (`Int64`, field id [`crate::metadata_columns::RESERVED_FIELD_ID_DELETE_FILE_POS`]),
//! both required. [`position_delete_batch`] produces exactly such a batch, with rows sorted
//! by `(file_path, pos)` and duplicate pairs removed, as the spec requires for position
//! delete files.
//!
//! Position delete files are a v2 construct. v3 tables use deletion vectors and forbid adding
//! new position delete files, so a format-version gate must be applied at the
//! transaction/commit layer before routing v3 writes here.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use roaring::RoaringTreemap;

use crate::{Error, ErrorKind, Result};

/// Builds a spec-conforming position delete [`RecordBatch`] from `(file_path, pos)` rows.
///
/// The returned batch has exactly the two required position delete columns and passes the
/// per-batch validation of
/// [`PositionDeleteFileWriter`](super::position_delete_writer::PositionDeleteFileWriter), so
/// it can be handed straight to that writer. Rows are emitted sorted by `(file_path, pos)`
/// with duplicate pairs removed, satisfying the spec's ordering requirement regardless of the
/// order in which they are supplied. Empty input yields a valid 0-row batch.
///
/// `pos` is a non-negative row position; a negative `pos` is rejected with
/// [`ErrorKind::DataInvalid`].
///
/// # Example
///
/// ```
/// use iceberg::writer::base_writer::position_delete_input::position_delete_batch;
///
/// // Rows may be supplied in any order; the batch comes out sorted.
/// let batch = position_delete_batch([
///     ("s3://bucket/data/f0.parquet", 4),
///     ("s3://bucket/data/f0.parquet", 1),
/// ])
/// .unwrap();
/// assert_eq!(batch.num_rows(), 2);
/// assert_eq!(batch.num_columns(), 2);
/// ```
pub fn position_delete_batch<S, I>(rows: I) -> Result<RecordBatch>
where
    S: Into<String>,
    I: IntoIterator<Item = (S, i64)>,
{
    // Keying paths in a `BTreeMap` and positions in a `RoaringTreemap` deduplicates repeated
    // `(file_path, pos)` pairs and yields the spec-required ascending `(file_path, pos)` order
    // when iterated.
    let mut by_path: BTreeMap<String, RoaringTreemap> = BTreeMap::new();
    for (path, pos) in rows {
        let pos = u64::try_from(pos).map_err(|_| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Position delete row position must be non-negative, got {pos}"),
            )
        })?;
        by_path.entry(path.into()).or_default().insert(pos);
    }

    let mut paths: Vec<&str> = Vec::new();
    let mut positions: Vec<i64> = Vec::new();
    for (path, pos_set) in &by_path {
        for pos in pos_set.iter() {
            paths.push(path.as_str());
            positions.push(pos as i64);
        }
    }

    // `from_iter_values` produces non-null arrays, matching the required columns.
    let path_array = StringArray::from_iter_values(paths);
    let pos_array = Int64Array::from_iter_values(positions);

    // Reuse the writer's crate-internal Arrow projection (a cheap `Arc` clone) so the
    // field-id wiring stays shared.
    let schema = super::position_delete_writer::position_delete_arrow_schema();
    RecordBatch::try_new(schema, vec![
        Arc::new(path_array) as ArrayRef,
        Arc::new(pos_array) as ArrayRef,
    ])
    .map_err(|e| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Failed to build position delete record batch: {e}"),
        )
    })
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
        let batch = position_delete_batch([("s3://bucket/data/f0.parquet", 1)]).unwrap();
        assert_position_delete_schema(&batch);
    }

    #[test]
    fn test_unsorted_input_is_sorted() {
        // Supplied out of order; both the paths and the positions within a path must sort.
        let batch = position_delete_batch([
            ("s3://bucket/data/f1.parquet", 2),
            ("s3://bucket/data/f0.parquet", 4),
            ("s3://bucket/data/f0.parquet", 1),
        ])
        .unwrap();

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
        let batch = position_delete_batch([
            ("s3://bucket/data/f0.parquet", 3),
            ("s3://bucket/data/f0.parquet", 3),
            ("s3://bucket/data/f0.parquet", 1),
        ])
        .unwrap();

        let (paths, positions) = columns(&batch);
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(paths, vec![
            "s3://bucket/data/f0.parquet",
            "s3://bucket/data/f0.parquet",
        ]);
        assert_eq!(positions, vec![1, 3]);
    }

    #[test]
    fn test_multiple_paths_sorted() {
        let batch = position_delete_batch([
            ("s3://bucket/data/c.parquet", 0),
            ("s3://bucket/data/a.parquet", 5),
            ("s3://bucket/data/b.parquet", 1),
        ])
        .unwrap();

        let (paths, _) = columns(&batch);
        assert_eq!(paths, vec![
            "s3://bucket/data/a.parquet",
            "s3://bucket/data/b.parquet",
            "s3://bucket/data/c.parquet",
        ]);
    }

    #[test]
    fn test_empty_input_builds_valid_zero_row_batch() {
        let batch = position_delete_batch(Vec::<(String, i64)>::new()).unwrap();
        assert_eq!(batch.num_rows(), 0);
        // Even with no rows the schema must be the full position delete schema.
        assert_position_delete_schema(&batch);
    }

    #[test]
    fn test_negative_pos_rejected() {
        let err = position_delete_batch([("s3://bucket/data/f0.parquet", -1)]).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    /// Wires a real [`PositionDeleteFileWriter`] over a local filesystem for the
    /// integration tests: the file IO, location/name generators, and the Parquet-backed
    /// rolling writer configured with the [`position_delete_schema`].
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

        // Deliberately unsorted; `position_delete_batch` sorts to the order the writer needs.
        let batch = position_delete_batch([
            ("s3://bucket/data/f1.parquet", 2),
            ("s3://bucket/data/f0.parquet", 4),
            ("s3://bucket/data/f0.parquet", 1),
        ])?;

        writer.write(batch).await?;
        let data_files = writer.close().await?;

        assert_eq!(data_files.len(), 1);
        let data_file = &data_files[0];
        assert_eq!(data_file.content_type(), DataContentType::PositionDeletes);
        assert_eq!(data_file.record_count, 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_empty_batch_feeds_position_delete_writer() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let builder = writer_setup(&temp_dir);
        let mut writer = builder.build(None).await?;

        // Empty input still produces a valid 0-row batch.
        let batch = position_delete_batch(Vec::<(String, i64)>::new())?;
        assert_eq!(batch.num_rows(), 0);

        writer.write(batch).await?;
        let data_files = writer.close().await?;

        // The Parquet writer skips 0-row batches and never opens a file, so closing
        // after writing only an empty batch produces no data files.
        assert!(
            data_files.is_empty(),
            "expected no data files for an empty batch, got {}",
            data_files.len()
        );

        Ok(())
    }
}
