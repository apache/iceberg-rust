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

//! This module provides `PositionDeleteFileWriter`.
//!
//! A position delete file has two required columns: `file_path` (`string`, field id
//! [`RESERVED_FIELD_ID_DELETE_FILE_PATH`]) and `pos` (`long`, field id
//! [`RESERVED_FIELD_ID_DELETE_FILE_POS`]). The writer takes batches already shaped as
//! those two columns (see [`position_delete_schema`]) and sets
//! [`DataContentType::PositionDeletes`] on the output. It does not sort its input; see
//! [`PositionDeleteFileWriter::write`].

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, SchemaRef as ArrowSchemaRef};
use once_cell::sync::Lazy;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::arrow::schema_to_arrow_schema;
use crate::metadata_columns::{
    RESERVED_FIELD_ID_DELETE_FILE_PATH, RESERVED_FIELD_ID_DELETE_FILE_POS, delete_file_path_field,
    delete_file_pos_field,
};
use crate::spec::{DataContentType, DataFile, PartitionKey, Schema, SchemaRef};
use crate::writer::file_writer::FileWriterBuilder;
use crate::writer::file_writer::location_generator::{FileNameGenerator, LocationGenerator};
use crate::writer::file_writer::rolling_writer::{RollingFileWriter, RollingFileWriterBuilder};
use crate::writer::{IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind, Result};

/// The canonical Iceberg schema of a position delete file: the required `file_path`
/// (`string`) and `pos` (`long`) columns with their reserved field ids.
static POSITION_DELETE_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    Arc::new(
        Schema::builder()
            .with_fields(vec![
                delete_file_path_field().clone(),
                delete_file_pos_field().clone(),
            ])
            .build()
            .expect("position delete schema is statically valid"),
    )
});

/// [`POSITION_DELETE_SCHEMA`] converted to Arrow, keeping the reserved field ids in
/// each field's Parquet field-id metadata.
static POSITION_DELETE_ARROW_SCHEMA: Lazy<ArrowSchemaRef> = Lazy::new(|| {
    Arc::new(
        schema_to_arrow_schema(&POSITION_DELETE_SCHEMA)
            .expect("position delete arrow schema is statically valid"),
    )
});

/// Returns the canonical Iceberg schema of a position delete file.
///
/// Use this to build the [`ParquetWriterBuilder`](crate::writer::file_writer::ParquetWriterBuilder)
/// that backs a [`PositionDeleteFileWriter`], so the written file matches the
/// spec exactly.
pub fn position_delete_schema() -> SchemaRef {
    POSITION_DELETE_SCHEMA.clone()
}

/// Returns the canonical Arrow schema of a position delete file.
pub fn position_delete_arrow_schema() -> ArrowSchemaRef {
    POSITION_DELETE_ARROW_SCHEMA.clone()
}

/// Reads a field's Iceberg field id from its Parquet field-id metadata.
fn field_id(field: &Field) -> Result<i32> {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Position delete column `{}` is missing its Iceberg field id metadata.",
                    field.name()
                ),
            )
        })?
        .parse::<i32>()
        .map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Position delete column `{}` has an invalid field id: {e}",
                    field.name()
                ),
            )
        })
}

/// Validates that a batch is a position delete file: the `file_path` (`Utf8`) and
/// `pos` (`Int64`) columns, in order, with the two reserved field ids. Checking it
/// here gives a clear error before the batch reaches the Parquet writer.
fn validate_position_delete_batch(batch: &RecordBatch) -> Result<()> {
    let fields = batch.schema_ref().fields();
    if fields.len() != 2 {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "This writer supports only the two required position delete columns (`file_path`, `pos`); \
                 batches with a different column count (e.g. including the optional `row` column) are not supported. Got {} columns.",
                fields.len()
            ),
        ));
    }

    let path = &fields[0];
    let path_id = field_id(path)?;
    if path_id != RESERVED_FIELD_ID_DELETE_FILE_PATH {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "The first position delete column must be `file_path` (field id {RESERVED_FIELD_ID_DELETE_FILE_PATH}), but got field id {path_id}."
            ),
        ));
    }
    // The canonical schema maps Iceberg `string` to `Utf8` and the file writer is
    // configured with it, so a `LargeUtf8` column has to be cast to `Utf8` first.
    if path.data_type() != &DataType::Utf8 {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "The position delete `file_path` column must be Utf8 (cast it first); got {:?}.",
                path.data_type()
            ),
        ));
    }
    // Required column: a nullable field could write nulls under a required schema.
    if path.is_nullable() {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "The position delete `file_path` column must be required (non-nullable).",
        ));
    }

    let pos = &fields[1];
    let pos_id = field_id(pos)?;
    if pos_id != RESERVED_FIELD_ID_DELETE_FILE_POS {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "The second position delete column must be `pos` (field id {RESERVED_FIELD_ID_DELETE_FILE_POS}), but got field id {pos_id}."
            ),
        ));
    }
    if pos.data_type() != &DataType::Int64 {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "The position delete `pos` column must be Int64, but got {:?}.",
                pos.data_type()
            ),
        ));
    }
    if pos.is_nullable() {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "The position delete `pos` column must be required (non-nullable).",
        ));
    }

    Ok(())
}

/// Builder for [`PositionDeleteFileWriter`].
#[derive(Debug)]
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
    /// The `RollingFileWriterBuilder` must be backed by a file writer configured
    /// with the [`position_delete_schema`]; the per-batch validation in
    /// [`PositionDeleteFileWriter::write`] guards against a mismatched batch, but
    /// the caller is responsible for wiring the same schema into the file writer.
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

    async fn build(&self, partition_key: Option<PartitionKey>) -> Result<Self::R> {
        Ok(PositionDeleteFileWriter {
            inner: Some(self.inner.build()),
            partition_key,
        })
    }
}

/// Writer used to write position delete files within one spec/partition.
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
    /// Writes a batch of `(file_path, pos)` records; the shape is validated on every
    /// call.
    ///
    /// The writer does not sort its input. Position delete files must be sorted by
    /// `file_path` then `pos`, so the caller must supply rows in that order across all
    /// `write` calls; a sorting writer will remove this requirement.
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        // Reject a closed writer before validating the batch.
        let Some(writer) = self.inner.as_mut() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Position delete writer is already closed; cannot write.",
            ));
        };
        validate_position_delete_batch(&batch)?;
        writer.write(&self.partition_key, &batch).await
    }

    async fn close(&mut self) -> Result<Vec<DataFile>> {
        if let Some(writer) = self.inner.take() {
            writer
                .close()
                .await?
                .into_iter()
                .map(|mut res| {
                    res.content(DataContentType::PositionDeletes);
                    // sort_order_id stays null, as the spec requires for position deletes.
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
                "Position delete writer is already closed.",
            ))
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{Int32Array, Int64Array, LargeStringArray, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field};
    use arrow_select::concat::concat_batches;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use super::*;
    use crate::io::FileIO;
    use crate::metadata_columns::{
        RESERVED_COL_NAME_DELETE_FILE_PATH, RESERVED_COL_NAME_DELETE_FILE_POS,
    };
    use crate::spec::{
        DataFileFormat, Literal, NestedField, PartitionSpec, PrimitiveType, Struct, Transform, Type,
    };
    use crate::writer::file_writer::ParquetWriterBuilder;
    use crate::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;

    #[test]
    fn test_position_delete_schema_shape() {
        let schema = position_delete_schema();
        let fields = schema.as_struct().fields();
        assert_eq!(fields.len(), 2);

        assert_eq!(fields[0].id, RESERVED_FIELD_ID_DELETE_FILE_PATH);
        assert_eq!(fields[0].name, RESERVED_COL_NAME_DELETE_FILE_PATH);
        assert!(fields[0].required);
        assert_eq!(
            fields[0].field_type.as_ref(),
            &Type::Primitive(PrimitiveType::String)
        );

        assert_eq!(fields[1].id, RESERVED_FIELD_ID_DELETE_FILE_POS);
        assert_eq!(fields[1].name, RESERVED_COL_NAME_DELETE_FILE_POS);
        assert!(fields[1].required);
        assert_eq!(
            fields[1].field_type.as_ref(),
            &Type::Primitive(PrimitiveType::Long)
        );

        // The Arrow projection carries the reserved field ids and non-null flags.
        let arrow_schema = position_delete_arrow_schema();
        assert_eq!(arrow_schema.fields().len(), 2);
        assert_eq!(arrow_schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(arrow_schema.field(1).data_type(), &DataType::Int64);
        assert!(!arrow_schema.field(0).is_nullable());
        assert!(!arrow_schema.field(1).is_nullable());
    }

    fn position_delete_batch(paths: Vec<&str>, positions: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(position_delete_arrow_schema(), vec![
            Arc::new(StringArray::from(paths)),
            Arc::new(Int64Array::from(positions)),
        ])
        .unwrap()
    }

    /// A field carrying an explicit Iceberg field-id metadata entry.
    fn field_with_id(name: &str, data_type: DataType, field_id: i32) -> Field {
        Field::new(name, data_type, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            field_id.to_string(),
        )]))
    }

    fn writer_setup(
        temp_dir: &TempDir,
    ) -> (
        FileIO,
        PositionDeleteFileWriterBuilder<
            ParquetWriterBuilder,
            DefaultLocationGenerator,
            DefaultFileNameGenerator,
        >,
    ) {
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
            file_io.clone(),
            location_gen,
            file_name_gen,
        );
        (
            file_io,
            PositionDeleteFileWriterBuilder::new(rolling_writer_builder),
        )
    }

    #[tokio::test]
    async fn test_position_delete_writer_round_trip() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let (file_io, builder) = writer_setup(&temp_dir);
        let mut writer = builder.build(None).await?;

        // Sorted by (file_path, pos): f0/1, f0/4, then f1/2.
        let batch = position_delete_batch(
            vec![
                "s3://bucket/data/f0.parquet",
                "s3://bucket/data/f0.parquet",
                "s3://bucket/data/f1.parquet",
            ],
            vec![1, 4, 2],
        );
        writer.write(batch.clone()).await?;
        let data_files = writer.close().await?;

        assert_eq!(data_files.len(), 1);
        let data_file = &data_files[0];
        assert_eq!(data_file.content_type(), DataContentType::PositionDeletes);
        assert_eq!(data_file.file_format, DataFileFormat::Parquet);
        assert_eq!(data_file.record_count, 3);
        // Unpartitioned writer leaves the default (empty) partition / spec id.
        assert_eq!(data_file.partition, Struct::empty());
        assert_eq!(data_file.partition_spec_id, 0);
        // The rolling writer fills in file statistics.
        assert!(data_file.file_size_in_bytes > 0);

        // The written Parquet file round-trips back to the exact input rows.
        let read_back = read_back_single(&file_io, data_file, &batch.schema()).await;
        assert_eq!(read_back, batch);

        Ok(())
    }

    #[tokio::test]
    async fn test_position_delete_writer_multiple_writes() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let (file_io, builder) = writer_setup(&temp_dir);
        let mut writer = builder.build(None).await?;

        let batch1 = position_delete_batch(
            vec!["s3://bucket/data/f0.parquet", "s3://bucket/data/f0.parquet"],
            vec![1, 4],
        );
        let batch2 = position_delete_batch(
            vec!["s3://bucket/data/f1.parquet", "s3://bucket/data/f1.parquet"],
            vec![2, 7],
        );
        writer.write(batch1.clone()).await?;
        writer.write(batch2.clone()).await?;
        let data_files = writer.close().await?;

        assert_eq!(data_files.len(), 1);
        assert_eq!(data_files[0].record_count, 4);

        let expected = concat_batches(&batch1.schema(), [&batch1, &batch2]).unwrap();
        let read_back = read_back_single(&file_io, &data_files[0], &batch1.schema()).await;
        assert_eq!(read_back, expected);

        Ok(())
    }

    #[tokio::test]
    async fn test_position_delete_writer_sets_partition() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let (_file_io, builder) = writer_setup(&temp_dir);

        // A table schema + identity partition spec with a non-default spec id, so the
        // assertions distinguish real propagation from the DataFileBuilder defaults.
        let table_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()?,
        );
        let spec = PartitionSpec::builder(table_schema.clone())
            .with_spec_id(7)
            .add_partition_field("id", "id", Transform::Identity)?
            .build()?;
        let partition_value = Struct::from_iter([Some(Literal::int(42))]);
        let partition_key = PartitionKey::new(spec, table_schema.clone(), partition_value.clone());

        let mut writer = builder.build(Some(partition_key)).await?;
        writer
            .write(position_delete_batch(
                vec!["s3://bucket/data/f0.parquet"],
                vec![1],
            ))
            .await?;
        let data_files = writer.close().await?;

        assert_eq!(data_files.len(), 1);
        let data_file = &data_files[0];
        assert_eq!(data_file.content_type(), DataContentType::PositionDeletes);
        assert_eq!(data_file.partition_spec_id, 7);
        assert_eq!(data_file.partition, partition_value);

        Ok(())
    }

    #[tokio::test]
    async fn test_position_delete_writer_rejects_wrong_column_count() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let (_file_io, builder) = writer_setup(&temp_dir);
        let mut writer = builder.build(None).await?;

        // A batch carrying only the `file_path` column is not a position delete file.
        let arrow_schema = position_delete_arrow_schema();
        let path_only = RecordBatch::try_new(Arc::new(arrow_schema.project(&[0]).unwrap()), vec![
            Arc::new(StringArray::from(vec!["s3://bucket/data/f0.parquet"])),
        ])
        .unwrap();

        let err = writer.write(path_only).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.to_string()
                .contains("only the two required position delete columns"),
            "{err}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_position_delete_writer_rejects_missing_field_ids() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let (_file_io, builder) = writer_setup(&temp_dir);
        let mut writer = builder.build(None).await?;

        // Correct shape and types, but plain field names without the reserved
        // Iceberg field-id metadata: must be rejected.
        let plain_schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new(RESERVED_COL_NAME_DELETE_FILE_PATH, DataType::Utf8, false),
            Field::new(RESERVED_COL_NAME_DELETE_FILE_POS, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(plain_schema, vec![
            Arc::new(StringArray::from(vec!["s3://bucket/data/f0.parquet"])),
            Arc::new(Int64Array::from(vec![1_i64])),
        ])
        .unwrap();

        let err = writer.write(batch).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.to_string().contains("field id metadata"), "{err}");
        Ok(())
    }

    #[tokio::test]
    async fn test_position_delete_writer_rejects_wrong_field_id() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let (_file_io, builder) = writer_setup(&temp_dir);
        let mut writer = builder.build(None).await?;

        // Right shape and types, but the two reserved field ids are swapped.
        let swapped = Arc::new(arrow_schema::Schema::new(vec![
            field_with_id(
                RESERVED_COL_NAME_DELETE_FILE_PATH,
                DataType::Utf8,
                RESERVED_FIELD_ID_DELETE_FILE_POS,
            ),
            field_with_id(
                RESERVED_COL_NAME_DELETE_FILE_POS,
                DataType::Int64,
                RESERVED_FIELD_ID_DELETE_FILE_PATH,
            ),
        ]));
        let batch = RecordBatch::try_new(swapped, vec![
            Arc::new(StringArray::from(vec!["s3://bucket/data/f0.parquet"])),
            Arc::new(Int64Array::from(vec![1_i64])),
        ])
        .unwrap();

        let err = writer.write(batch).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.to_string().contains("must be `file_path`"), "{err}");
        Ok(())
    }

    #[tokio::test]
    async fn test_position_delete_writer_rejects_bad_pos_field_id() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let (_file_io, builder) = writer_setup(&temp_dir);
        let mut writer = builder.build(None).await?;

        // Correct `file_path`, but `pos` carries the wrong reserved field id — the
        // pos-specific branch (not the path branch) must fire.
        let wrong_pos_id = Arc::new(arrow_schema::Schema::new(vec![
            field_with_id(
                RESERVED_COL_NAME_DELETE_FILE_PATH,
                DataType::Utf8,
                RESERVED_FIELD_ID_DELETE_FILE_PATH,
            ),
            field_with_id(RESERVED_COL_NAME_DELETE_FILE_POS, DataType::Int64, 999),
        ]));
        let batch = RecordBatch::try_new(wrong_pos_id, vec![
            Arc::new(StringArray::from(vec!["s3://bucket/data/f0.parquet"])),
            Arc::new(Int64Array::from(vec![1_i64])),
        ])
        .unwrap();
        let err = writer.write(batch).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.to_string().contains("must be `pos`"), "{err}");

        // Correct `file_path`, but `pos` is missing its field-id metadata entirely.
        let missing_pos_id = Arc::new(arrow_schema::Schema::new(vec![
            field_with_id(
                RESERVED_COL_NAME_DELETE_FILE_PATH,
                DataType::Utf8,
                RESERVED_FIELD_ID_DELETE_FILE_PATH,
            ),
            Field::new(RESERVED_COL_NAME_DELETE_FILE_POS, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(missing_pos_id, vec![
            Arc::new(StringArray::from(vec!["s3://bucket/data/f0.parquet"])),
            Arc::new(Int64Array::from(vec![1_i64])),
        ])
        .unwrap();
        let err = writer.write(batch).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.to_string().contains("field id metadata"), "{err}");

        Ok(())
    }

    #[tokio::test]
    async fn test_position_delete_writer_rejects_wrong_column_types() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let (_file_io, builder) = writer_setup(&temp_dir);
        let mut writer = builder.build(None).await?;

        // Correct field ids, but `pos` is Int32 rather than Int64.
        let int32_pos = Arc::new(arrow_schema::Schema::new(vec![
            field_with_id(
                RESERVED_COL_NAME_DELETE_FILE_PATH,
                DataType::Utf8,
                RESERVED_FIELD_ID_DELETE_FILE_PATH,
            ),
            field_with_id(
                RESERVED_COL_NAME_DELETE_FILE_POS,
                DataType::Int32,
                RESERVED_FIELD_ID_DELETE_FILE_POS,
            ),
        ]));
        let batch = RecordBatch::try_new(int32_pos, vec![
            Arc::new(StringArray::from(vec!["s3://bucket/data/f0.parquet"])),
            Arc::new(Int32Array::from(vec![1_i32])),
        ])
        .unwrap();
        let err = writer.write(batch).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.to_string().contains("must be Int64"), "{err}");

        // Correct field ids, but `file_path` is not a string.
        let int_path = Arc::new(arrow_schema::Schema::new(vec![
            field_with_id(
                RESERVED_COL_NAME_DELETE_FILE_PATH,
                DataType::Int32,
                RESERVED_FIELD_ID_DELETE_FILE_PATH,
            ),
            field_with_id(
                RESERVED_COL_NAME_DELETE_FILE_POS,
                DataType::Int64,
                RESERVED_FIELD_ID_DELETE_FILE_POS,
            ),
        ]));
        let batch = RecordBatch::try_new(int_path, vec![
            Arc::new(Int32Array::from(vec![1_i32])),
            Arc::new(Int64Array::from(vec![1_i64])),
        ])
        .unwrap();
        let err = writer.write(batch).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.to_string().contains("must be Utf8"), "{err}");

        // Correct field ids, but `file_path` is LargeUtf8 — the common shape from
        // DataFusion/DuckDB/Polars, and the case the validator's comment calls out.
        let large_path = Arc::new(arrow_schema::Schema::new(vec![
            field_with_id(
                RESERVED_COL_NAME_DELETE_FILE_PATH,
                DataType::LargeUtf8,
                RESERVED_FIELD_ID_DELETE_FILE_PATH,
            ),
            field_with_id(
                RESERVED_COL_NAME_DELETE_FILE_POS,
                DataType::Int64,
                RESERVED_FIELD_ID_DELETE_FILE_POS,
            ),
        ]));
        let batch = RecordBatch::try_new(large_path, vec![
            Arc::new(LargeStringArray::from(vec!["s3://bucket/data/f0.parquet"])),
            Arc::new(Int64Array::from(vec![1_i64])),
        ])
        .unwrap();
        let err = writer.write(batch).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.to_string().contains("must be Utf8"), "{err}");

        Ok(())
    }

    #[tokio::test]
    async fn test_position_delete_writer_rejects_nullable_columns() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let (_file_io, builder) = writer_setup(&temp_dir);
        let mut writer = builder.build(None).await?;

        // Correct field ids and types, but the columns are declared nullable. A
        // required schema with a nullable field could emit nulls -> malformed file.
        let nullable = |name: &str, data_type: DataType, id: i32| {
            Field::new(name, data_type, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                id.to_string(),
            )]))
        };

        let nullable_path = Arc::new(arrow_schema::Schema::new(vec![
            nullable(
                RESERVED_COL_NAME_DELETE_FILE_PATH,
                DataType::Utf8,
                RESERVED_FIELD_ID_DELETE_FILE_PATH,
            ),
            field_with_id(
                RESERVED_COL_NAME_DELETE_FILE_POS,
                DataType::Int64,
                RESERVED_FIELD_ID_DELETE_FILE_POS,
            ),
        ]));
        let batch = RecordBatch::try_new(nullable_path, vec![
            Arc::new(StringArray::from(vec!["s3://bucket/data/f0.parquet"])),
            Arc::new(Int64Array::from(vec![1_i64])),
        ])
        .unwrap();
        let err = writer.write(batch).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.to_string()
                .contains("`file_path` column must be required"),
            "{err}"
        );

        let nullable_pos = Arc::new(arrow_schema::Schema::new(vec![
            field_with_id(
                RESERVED_COL_NAME_DELETE_FILE_PATH,
                DataType::Utf8,
                RESERVED_FIELD_ID_DELETE_FILE_PATH,
            ),
            nullable(
                RESERVED_COL_NAME_DELETE_FILE_POS,
                DataType::Int64,
                RESERVED_FIELD_ID_DELETE_FILE_POS,
            ),
        ]));
        let batch = RecordBatch::try_new(nullable_pos, vec![
            Arc::new(StringArray::from(vec!["s3://bucket/data/f0.parquet"])),
            Arc::new(Int64Array::from(vec![1_i64])),
        ])
        .unwrap();
        let err = writer.write(batch).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.to_string().contains("`pos` column must be required"),
            "{err}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_position_delete_writer_close_without_writes() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let (_file_io, builder) = writer_setup(&temp_dir);
        let mut writer = builder.build(None).await?;

        // Closing a writer that never received a batch produces no data files.
        let data_files = writer.close().await?;
        assert!(data_files.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_position_delete_writer_errors_after_close() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let (_file_io, builder) = writer_setup(&temp_dir);
        let mut writer = builder.build(None).await?;
        writer.close().await?;

        // Both write() and a second close() report the writer is already closed.
        let write_err = writer
            .write(position_delete_batch(
                vec!["s3://bucket/data/f0.parquet"],
                vec![1],
            ))
            .await
            .unwrap_err();
        assert_eq!(write_err.kind(), ErrorKind::Unexpected);
        assert!(
            write_err.to_string().contains("cannot write"),
            "{write_err}"
        );

        // Even a malformed batch surfaces the closed error, not a validation error:
        // the closed check runs before validation.
        let arrow_schema = position_delete_arrow_schema();
        let invalid = RecordBatch::try_new(Arc::new(arrow_schema.project(&[0]).unwrap()), vec![
            Arc::new(StringArray::from(vec!["s3://bucket/data/f0.parquet"])),
        ])
        .unwrap();
        let invalid_err = writer.write(invalid).await.unwrap_err();
        assert_eq!(invalid_err.kind(), ErrorKind::Unexpected);

        let close_err = writer.close().await.unwrap_err();
        assert_eq!(close_err.kind(), ErrorKind::Unexpected);
        assert!(
            close_err.to_string().contains("already closed"),
            "{close_err}"
        );
        Ok(())
    }

    async fn read_back_single(
        file_io: &FileIO,
        data_file: &DataFile,
        schema: &arrow_schema::SchemaRef,
    ) -> RecordBatch {
        let input_content = file_io
            .new_input(data_file.file_path.clone())
            .unwrap()
            .read()
            .await
            .unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(input_content)
            .unwrap()
            .build()
            .unwrap();
        let batches = reader.map(|b| b.unwrap()).collect::<Vec<_>>();
        concat_batches(schema, &batches).unwrap()
    }
}
