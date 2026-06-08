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

//! This module provides the [`PositionDeleteFileWriter`].
//!
//! A position-delete file marks rows as deleted by `(file_path, pos)` — the full URI of the target
//! data file and the ordinal position (starting at 0) of the deleted row within it. This is the
//! merge-on-read counterpart to the [`EqualityDeleteFileWriter`](super::equality_delete_writer); the
//! read side that consumes these files lives in [`crate::arrow::delete_filter`].
//!
//! # Schema
//!
//! The file's schema is exactly the Iceberg position-delete schema (spec
//! [§position-delete-files](https://iceberg.apache.org/spec/#position-delete-files)):
//!
//! | field id     | name        | type     |
//! |--------------|-------------|----------|
//! | `2147483546` | `file_path` | `string` |
//! | `2147483545` | `pos`       | `long`   |
//!
//! These reserved field ids must match Java (`MetadataColumns.DELETE_FILE_PATH` /
//! `DELETE_FILE_POS`) for interop — a delete file with the wrong ids cannot be read by Java. They are
//! defined once in [`crate::metadata_columns`] and reused here.
//!
//! The optional `row` column (field id `2147483544`, "position deletes with row data") is **out of
//! scope** for this writer.
//!
//! # Sorting
//!
//! The Iceberg spec recommends that rows in a position-delete file be sorted by `file_path` then
//! `pos` so that readers can binary-search. Mirroring Java's basic
//! [`PositionDeleteWriter`](https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/deletes/PositionDeleteWriter.java),
//! **this writer writes records in the order given and never reorders them** — producing the sorted
//! ordering is the caller's responsibility (Java delegates it to `SortingPositionOnlyDeleteWriter`).
//! Feeding unsorted positions yields a valid, readable delete file that is merely sub-optimal for
//! scan-time filtering.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef as ArrowSchemaRef;

use crate::arrow::schema_to_arrow_schema;
use crate::metadata_columns::{delete_file_path_field, delete_file_pos_field};
use crate::spec::{DataContentType, DataFile, PartitionKey, Schema, SchemaRef};
use crate::writer::file_writer::FileWriterBuilder;
use crate::writer::file_writer::location_generator::{FileNameGenerator, LocationGenerator};
use crate::writer::file_writer::rolling_writer::{RollingFileWriter, RollingFileWriterBuilder};
use crate::writer::{IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind, Result};

/// Build the canonical Iceberg position-delete schema: `file_path: string` (field id `2147483546`)
/// followed by `pos: long` (field id `2147483545`), both required.
///
/// The fields (and their reserved ids) come from [`crate::metadata_columns`], so they match the Java
/// `MetadataColumns.DELETE_FILE_PATH` / `DELETE_FILE_POS` definitions for interop.
pub fn pos_delete_schema() -> Result<Schema> {
    Schema::builder()
        .with_fields(vec![
            delete_file_path_field().clone(),
            delete_file_pos_field().clone(),
        ])
        .build()
}

/// Config for [`PositionDeleteFileWriter`].
///
/// Holds the position-delete [`Schema`] and its Arrow projection. The Arrow schema is used to
/// validate every incoming [`RecordBatch`] so a mismatched batch is rejected up front rather than
/// silently producing a delete file Java cannot read.
#[derive(Debug, Clone)]
pub struct PositionDeleteWriterConfig {
    schema: SchemaRef,
    arrow_schema: ArrowSchemaRef,
}

impl PositionDeleteWriterConfig {
    /// Create a new `PositionDeleteWriterConfig`.
    pub fn new() -> Result<Self> {
        let schema = Arc::new(pos_delete_schema()?);
        let arrow_schema = Arc::new(schema_to_arrow_schema(&schema)?);
        Ok(Self {
            schema,
            arrow_schema,
        })
    }

    /// Return the position-delete [`Schema`].
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Return the position-delete Arrow schema (the schema every input batch must match).
    pub fn arrow_schema(&self) -> &ArrowSchemaRef {
        &self.arrow_schema
    }
}

/// Builder for [`PositionDeleteFileWriter`].
#[derive(Debug, Clone)]
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
    ///
    /// The inner [`RollingFileWriterBuilder`] must be configured with a parquet (or other) file
    /// writer whose schema is the position-delete schema (see [`pos_delete_schema`] /
    /// [`PositionDeleteWriterConfig::schema`]).
    pub fn new(
        inner: RollingFileWriterBuilder<B, L, F>,
        config: PositionDeleteWriterConfig,
    ) -> Self {
        Self { inner, config }
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
            arrow_schema: self.config.arrow_schema.clone(),
            partition_key,
        })
    }
}

/// Writer that writes position-delete files within one spec/partition.
///
/// Each input [`RecordBatch`] must match the position-delete Arrow schema exactly
/// (`file_path: string`, `pos: long`, carrying field ids `2147483546` / `2147483545`). Records are
/// written in the order given — see the [module docs](self) for the sorting contract.
#[derive(Debug)]
pub struct PositionDeleteFileWriter<
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
> {
    inner: Option<RollingFileWriter<B, L, F>>,
    arrow_schema: ArrowSchemaRef,
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
        // Validate the incoming batch against the position-delete schema. A delete file whose
        // columns/ids/types do not match the reserved (file_path, pos) schema would silently fail to
        // delete rows (or be unreadable by Java), so reject it here rather than write it.
        if batch.schema().as_ref() != self.arrow_schema.as_ref() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Position delete batch schema does not match the position-delete schema. \
                     Expected {:?}, got {:?}",
                    self.arrow_schema,
                    batch.schema()
                ),
            ));
        }

        if let Some(writer) = self.inner.as_mut() {
            // Write records in the order given — sorting by (file_path, pos) is the caller's
            // responsibility (see the module docs).
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
                    res.content(DataContentType::PositionDeletes);
                    if let Some(pk) = self.partition_key.as_ref() {
                        res.partition(pk.data().clone());
                        res.partition_spec_id(pk.spec().spec_id());
                    }
                    res.build().map_err(|e| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!("Failed to build position delete file: {e}"),
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
mod test {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow_schema::DataType;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use super::{PositionDeleteFileWriterBuilder, PositionDeleteWriterConfig, pos_delete_schema};
    use crate::ErrorKind;
    use crate::io::FileIO;
    use crate::metadata_columns::{
        RESERVED_FIELD_ID_DELETE_FILE_PATH, RESERVED_FIELD_ID_DELETE_FILE_POS,
    };
    use crate::spec::{DataContentType, DataFileFormat, PrimitiveType, Type};
    use crate::writer::file_writer::ParquetWriterBuilder;
    use crate::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use crate::writer::{IcebergWriter, IcebergWriterBuilder};

    /// Build a `(file_path, pos)` RecordBatch in the canonical position-delete Arrow schema.
    fn pos_delete_batch(pairs: &[(&str, i64)]) -> RecordBatch {
        let config = PositionDeleteWriterConfig::new().unwrap();
        let file_paths: Vec<&str> = pairs.iter().map(|(p, _)| *p).collect();
        let positions: Vec<i64> = pairs.iter().map(|(_, p)| *p).collect();
        let file_path_col = Arc::new(StringArray::from(file_paths)) as ArrayRef;
        let pos_col = Arc::new(Int64Array::from(positions)) as ArrayRef;
        RecordBatch::try_new(config.arrow_schema().clone(), vec![file_path_col, pos_col]).unwrap()
    }

    fn make_writer_builder(
        file_io: &FileIO,
        temp_dir: &TempDir,
    ) -> PositionDeleteFileWriterBuilder<
        ParquetWriterBuilder,
        DefaultLocationGenerator,
        DefaultFileNameGenerator,
    > {
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen = DefaultFileNameGenerator::new(
            "test-pos-del".to_string(),
            None,
            DataFileFormat::Parquet,
        );

        let config = PositionDeleteWriterConfig::new().unwrap();
        let parquet_writer_builder =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), config.schema().clone());
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            file_io.clone(),
            location_gen,
            file_name_gen,
        );
        PositionDeleteFileWriterBuilder::new(rolling_writer_builder, config)
    }

    /// The schema this writer advertises must be exactly the Iceberg position-delete schema, with
    /// the reserved field ids that Java uses. A wrong id here means Java cannot read the file.
    #[test]
    fn test_pos_delete_schema_has_reserved_field_ids() {
        let schema = pos_delete_schema().unwrap();
        let fields = schema.as_struct().fields();
        assert_eq!(fields.len(), 2);

        assert_eq!(fields[0].name, "file_path");
        assert_eq!(fields[0].id, RESERVED_FIELD_ID_DELETE_FILE_PATH);
        assert_eq!(fields[0].id, 2147483546);
        assert!(fields[0].required);
        assert_eq!(
            fields[0].field_type.as_ref(),
            &Type::Primitive(PrimitiveType::String)
        );

        assert_eq!(fields[1].name, "pos");
        assert_eq!(fields[1].id, RESERVED_FIELD_ID_DELETE_FILE_POS);
        assert_eq!(fields[1].id, 2147483545);
        assert!(fields[1].required);
        assert_eq!(
            fields[1].field_type.as_ref(),
            &Type::Primitive(PrimitiveType::Long)
        );
    }

    /// Round-trip risk: dropped or mangled positions = data silently NOT deleted. Write a set of
    /// (file_path, pos) pairs, read the parquet back, and assert the exact pairs survive in order.
    #[tokio::test]
    async fn test_position_delete_round_trips_exact_positions() -> Result<(), anyhow::Error> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let mut writer = make_writer_builder(&file_io, &temp_dir).build(None).await?;

        let pairs = [
            ("s3://bucket/data/1.parquet", 0i64),
            ("s3://bucket/data/1.parquet", 5),
            ("s3://bucket/data/1.parquet", 1023),
        ];
        writer.write(pos_delete_batch(&pairs)).await?;
        let data_files = writer.close().await?;

        assert_eq!(data_files.len(), 1);
        let data_file = &data_files[0];
        assert_eq!(data_file.content, DataContentType::PositionDeletes);
        assert_eq!(data_file.record_count, pairs.len() as u64);

        // Read the written parquet back and assert the exact (file_path, pos) pairs round-trip.
        let input = file_io.new_input(data_file.file_path.clone())?;
        let bytes = input.read().await?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)?.build()?;
        let mut read_pairs: Vec<(String, i64)> = Vec::new();
        for batch in reader {
            let batch = batch?;
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
            for i in 0..batch.num_rows() {
                read_pairs.push((paths.value(i).to_string(), positions.value(i)));
            }
        }

        let expected: Vec<(String, i64)> = pairs.iter().map(|(p, n)| (p.to_string(), *n)).collect();
        assert_eq!(read_pairs, expected);
        Ok(())
    }

    /// Interop risk: the written parquet must carry field ids 2147483546 (file_path) / 2147483545
    /// (pos). Wrong ids => Java cannot read the delete file.
    #[tokio::test]
    async fn test_position_delete_written_field_ids_match_reserved() -> Result<(), anyhow::Error> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let mut writer = make_writer_builder(&file_io, &temp_dir).build(None).await?;

        writer
            .write(pos_delete_batch(&[("s3://b/d/1.parquet", 7)]))
            .await?;
        let data_files = writer.close().await?;
        let data_file = &data_files[0];

        let input = file_io.new_input(data_file.file_path.clone())?;
        let bytes = input.read().await?;
        let reader_builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
        let arrow_schema = reader_builder.schema();

        let path_field = arrow_schema.field(0);
        assert_eq!(path_field.name(), "file_path");
        assert_eq!(path_field.data_type(), &DataType::Utf8);
        assert_eq!(
            path_field.metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&"2147483546".to_string())
        );

        let pos_field = arrow_schema.field(1);
        assert_eq!(pos_field.name(), "pos");
        assert_eq!(pos_field.data_type(), &DataType::Int64);
        assert_eq!(
            pos_field.metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&"2147483545".to_string())
        );

        Ok(())
    }

    /// One delete file may carry positions for MULTIPLE data files. Assert every pair round-trips,
    /// preserving the order given (the writer does not reorder — sorting is the caller's job).
    #[tokio::test]
    async fn test_position_delete_multiple_data_files_one_delete_file() -> Result<(), anyhow::Error>
    {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let mut writer = make_writer_builder(&file_io, &temp_dir).build(None).await?;

        // Intentionally interleaved / unsorted across two data files: written AS GIVEN.
        let pairs = [
            ("s3://b/d/2.parquet", 3i64),
            ("s3://b/d/1.parquet", 0),
            ("s3://b/d/2.parquet", 1),
            ("s3://b/d/1.parquet", 9),
        ];
        writer.write(pos_delete_batch(&pairs)).await?;
        let data_files = writer.close().await?;

        assert_eq!(data_files.len(), 1);
        assert_eq!(data_files[0].record_count, pairs.len() as u64);

        let input = file_io.new_input(data_files[0].file_path.clone())?;
        let bytes = input.read().await?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)?.build()?;
        let mut read_pairs: Vec<(String, i64)> = Vec::new();
        for batch in reader {
            let batch = batch?;
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
            for i in 0..batch.num_rows() {
                read_pairs.push((paths.value(i).to_string(), positions.value(i)));
            }
        }

        let expected: Vec<(String, i64)> = pairs.iter().map(|(p, n)| (p.to_string(), *n)).collect();
        assert_eq!(read_pairs, expected);
        Ok(())
    }

    /// A batch whose schema is NOT the position-delete schema must be rejected — writing it would
    /// produce a delete file that fails to delete (or that Java cannot read).
    #[tokio::test]
    async fn test_position_delete_rejects_mismatched_schema() -> Result<(), anyhow::Error> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let mut writer = make_writer_builder(&file_io, &temp_dir).build(None).await?;

        // Wrong: a plain (path, pos) batch with no field-id metadata and a different field order.
        let bad_schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("pos", DataType::Int64, false),
            arrow_schema::Field::new("file_path", DataType::Utf8, false),
        ]));
        let bad_batch = RecordBatch::try_new(bad_schema, vec![
            Arc::new(Int64Array::from(vec![0i64])) as ArrayRef,
            Arc::new(StringArray::from(vec!["s3://b/d/1.parquet"])) as ArrayRef,
        ])
        .unwrap();

        let err = writer.write(bad_batch).await.expect_err("must reject");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.to_string().contains("position-delete schema"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    /// Empty input convention (mirrors the equality-delete writer): a writer that received no rows
    /// still closes cleanly. Closing with zero deletes yields a 0-row delete file.
    #[tokio::test]
    async fn test_position_delete_empty_input_closes() -> Result<(), anyhow::Error> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let mut writer = make_writer_builder(&file_io, &temp_dir).build(None).await?;

        // No write() calls — close immediately.
        let data_files = writer.close().await?;
        // The rolling writer produces no file when nothing was written (matches the data /
        // equality-delete writers' behavior for an empty writer).
        assert!(
            data_files.is_empty(),
            "expected no delete files for empty input, got {}",
            data_files.len()
        );
        Ok(())
    }
}
