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

//! This module provides [`PositionDeleteInputBuilder`], a small helper that assembles a
//! spec-conforming position delete [`RecordBatch`] from `(path, pos)` rows.
//!
//! A [`PositionDeleteFileWriter`](super::position_delete_writer::PositionDeleteFileWriter)
//! accepts batches shaped as exactly the two required position delete columns: `file_path`
//! (`Utf8`, field id [`crate::metadata_columns::RESERVED_FIELD_ID_DELETE_FILE_PATH`]) and
//! `pos` (`Int64`, field id [`crate::metadata_columns::RESERVED_FIELD_ID_DELETE_FILE_POS`]),
//! both required. Hand-wiring that Arrow schema is error-prone, so this builder reuses the
//! writer's own Arrow projection of
//! [`position_delete_schema`](super::position_delete_writer::position_delete_schema), keeping
//! a single source of truth, and produces a batch the writer's validation accepts.
//!
//! The builder does not sort or deduplicate rows; position delete files must be sorted by
//! `file_path` then `pos`, so callers are responsible for pushing rows in that order for
//! direct-to-writer use (see
//! [`PositionDeleteFileWriter::write`](crate::writer::IcebergWriter::write), which
//! [`PositionDeleteFileWriter`](super::position_delete_writer::PositionDeleteFileWriter)
//! implements).
//! A future sorting writer will consume unsorted input and lift this requirement.
//!
//! All rows are materialized into two heap [`Vec`]s before the [`RecordBatch`] is built, so
//! for very large inputs prefer constructing one builder per write batch rather than
//! accumulating everything in a single builder.
//!
//! Only the two required columns are produced. The spec's optional third `row` column
//! (field id `i32::MAX - 103`), which inlines the deleted row's data, is not supported yet;
//! see [`PositionDeleteFileWriter`](super::position_delete_writer::PositionDeleteFileWriter).
//!
//! Position delete files are a v2 construct. v3 tables use deletion vectors and forbid adding
//! new position delete files, so a format-version gate must be applied at the
//! transaction/commit layer before routing v3 writes here; this builder and the underlying
//! [`PositionDeleteFileWriter`](super::position_delete_writer::PositionDeleteFileWriter) have
//! no such gate by design.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};

use crate::{Error, ErrorKind, Result};

/// Builds a spec-conforming position delete [`RecordBatch`] from `(file_path, pos)` rows.
///
/// The output has exactly the two required position delete columns and passes the
/// per-batch validation of
/// [`PositionDeleteFileWriter`](super::position_delete_writer::PositionDeleteFileWriter),
/// so it can be handed straight to that writer. An empty builder still produces a valid
/// 0-row batch with the correct schema.
///
/// # Example
///
/// ```
/// use iceberg::writer::base_writer::position_delete_input::PositionDeleteInputBuilder;
///
/// let mut input = PositionDeleteInputBuilder::new();
/// input
///     .push("s3://bucket/data/f0.parquet", 1)
///     .push("s3://bucket/data/f0.parquet", 4);
/// let batch = input.build().unwrap();
/// assert_eq!(batch.num_rows(), 2);
/// assert_eq!(batch.num_columns(), 2);
/// ```
#[derive(Debug, Default, Clone)]
pub struct PositionDeleteInputBuilder {
    paths: Vec<String>,
    positions: Vec<i64>,
}

impl PositionDeleteInputBuilder {
    /// Creates an empty builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an empty builder with room for `n` rows preallocated.
    pub fn with_capacity(n: usize) -> Self {
        Self {
            paths: Vec::with_capacity(n),
            positions: Vec::with_capacity(n),
        }
    }

    /// Appends one `(file_path, pos)` row.
    pub fn push(&mut self, path: impl Into<String>, pos: i64) -> &mut Self {
        self.paths.push(path.into());
        self.positions.push(pos);
        self
    }

    /// Appends every `(file_path, pos)` row yielded by `rows`.
    pub fn extend<S, I>(&mut self, rows: I) -> &mut Self
    where
        S: Into<String>,
        I: IntoIterator<Item = (S, i64)>,
    {
        let rows = rows.into_iter();
        let (lower, _) = rows.size_hint();
        self.paths.reserve(lower);
        self.positions.reserve(lower);
        for (path, pos) in rows {
            self.paths.push(path.into());
            self.positions.push(pos);
        }
        self
    }

    /// Returns the number of rows accumulated so far.
    pub fn len(&self) -> usize {
        self.positions.len()
    }

    /// Returns `true` if no rows have been added.
    pub fn is_empty(&self) -> bool {
        self.positions.is_empty()
    }

    /// Consumes the builder and assembles the position delete [`RecordBatch`].
    ///
    /// The `file_path` column is a non-null `Utf8` [`StringArray`] and the `pos` column a
    /// non-null `Int64` [`Int64Array`], carrying the reserved field ids in their Parquet
    /// field-id metadata. An empty builder yields a valid 0-row batch.
    pub fn build(self) -> Result<RecordBatch> {
        debug_assert_eq!(
            self.paths.len(),
            self.positions.len(),
            "paths/positions length invariant violated"
        );

        // `from_iter_values` produces non-null arrays, matching the required columns.
        let path_array = StringArray::from_iter_values(self.paths);
        let pos_array = Int64Array::from_iter_values(self.positions);

        // Reuse the writer's crate-internal Arrow projection (a cheap `Arc` clone) so the
        // field-id wiring stays a single source of truth.
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

    /// Asserts a built batch has the exact two-column position delete schema: names, Arrow
    /// types, non-null flags, and the reserved field ids in the Parquet field-id metadata.
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

    #[test]
    fn test_build_column_shape_and_field_ids() {
        let mut input = PositionDeleteInputBuilder::new();
        input.push("s3://bucket/data/f0.parquet", 1);
        let batch = input.build().unwrap();
        assert_position_delete_schema(&batch);
    }

    #[test]
    fn test_build_preserves_values_in_order() {
        let mut input = PositionDeleteInputBuilder::new();
        input
            .push("s3://bucket/data/f0.parquet", 1)
            .push("s3://bucket/data/f0.parquet", 4)
            .push("s3://bucket/data/f1.parquet", 2);
        assert_eq!(input.len(), 3);
        assert!(!input.is_empty());

        let batch = input.build().unwrap();
        assert_eq!(batch.num_rows(), 3);

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
        assert_eq!(paths.iter().map(|s| s.unwrap()).collect::<Vec<_>>(), vec![
            "s3://bucket/data/f0.parquet",
            "s3://bucket/data/f0.parquet",
            "s3://bucket/data/f1.parquet",
        ]);
        assert_eq!(positions.values(), &[1, 4, 2]);
    }

    #[test]
    fn test_empty_builder_builds_valid_zero_row_batch() {
        let input = PositionDeleteInputBuilder::new();
        assert!(input.is_empty());
        assert_eq!(input.len(), 0);

        let batch = input.build().unwrap();
        assert_eq!(batch.num_rows(), 0);
        // Even with no rows the schema must be the full position delete schema.
        assert_position_delete_schema(&batch);
    }

    #[test]
    fn test_extend_from_vec() {
        let rows: Vec<(String, i64)> = vec![
            ("s3://bucket/data/f0.parquet".to_string(), 1),
            ("s3://bucket/data/f0.parquet".to_string(), 4),
            ("s3://bucket/data/f1.parquet".to_string(), 2),
        ];
        let mut input = PositionDeleteInputBuilder::with_capacity(rows.len());
        input.extend(rows);
        assert_eq!(input.len(), 3);

        let batch = input.build().unwrap();
        assert_eq!(batch.num_rows(), 3);
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
        // Assert both columns to catch a path/pos transposition.
        assert_eq!(paths.iter().map(|s| s.unwrap()).collect::<Vec<_>>(), vec![
            "s3://bucket/data/f0.parquet",
            "s3://bucket/data/f0.parquet",
            "s3://bucket/data/f1.parquet",
        ]);
        assert_eq!(positions.values(), &[1, 4, 2]);
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
    async fn test_built_batch_feeds_position_delete_writer() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let builder = writer_setup(&temp_dir);
        let mut writer = builder.build(None).await?;

        // Rows sorted by (file_path, pos), as the writer requires.
        let mut input = PositionDeleteInputBuilder::new();
        input
            .push("s3://bucket/data/f0.parquet", 1)
            .push("s3://bucket/data/f0.parquet", 4)
            .push("s3://bucket/data/f1.parquet", 2);
        let batch = input.build()?;

        writer.write(batch).await?;
        let data_files = writer.close().await?;

        assert_eq!(data_files.len(), 1);
        let data_file = &data_files[0];
        assert_eq!(data_file.content_type(), DataContentType::PositionDeletes);
        assert_eq!(data_file.record_count, 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_empty_built_batch_feeds_position_delete_writer() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let builder = writer_setup(&temp_dir);
        let mut writer = builder.build(None).await?;

        // A default (empty) builder still produces a valid 0-row batch.
        let batch = PositionDeleteInputBuilder::new().build()?;
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
