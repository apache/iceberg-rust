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

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::spec::{DataContentType, DataFile, PartitionKey};
use crate::writer::file_writer::FileWriterBuilder;
use crate::writer::file_writer::location_generator::{FileNameGenerator, LocationGenerator};
use crate::writer::file_writer::rolling_writer::{RollingFileWriter, RollingFileWriterBuilder};
use crate::writer::{IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind, Result};

/// Field ID for file_path column in position delete files.
pub const POSITION_DELETE_FILE_PATH_FIELD_ID: i32 = 2147483546;
/// Field ID for pos column in position delete files.
pub const POSITION_DELETE_POS_FIELD_ID: i32 = 2147483545;

/// Config for `PositionDeleteFileWriter`.
#[derive(Clone, Debug)]
pub struct PositionDeleteWriterConfig {
    delete_schema: ArrowSchemaRef,
}

impl PositionDeleteWriterConfig {
    /// Create a new `PositionDeleteWriterConfig` with the standard delete schema.
    pub fn new() -> Self {
        let delete_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("file_path", DataType::Utf8, false).with_metadata(
                [(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    POSITION_DELETE_FILE_PATH_FIELD_ID.to_string(),
                )]
                .into_iter()
                .collect(),
            ),
            Field::new("pos", DataType::Int64, false).with_metadata(
                [(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    POSITION_DELETE_POS_FIELD_ID.to_string(),
                )]
                .into_iter()
                .collect(),
            ),
        ]));
        Self { delete_schema }
    }

    /// Returns the Arrow schema for position delete files.
    pub fn delete_schema(&self) -> &ArrowSchemaRef {
        &self.delete_schema
    }
}

impl Default for PositionDeleteWriterConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for `PositionDeleteFileWriter`.
#[derive(Clone, Debug)]
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
            config: self.config.clone(),
            partition_key,
            referenced_data_files: HashSet::new(),
        })
    }
}

/// Writer used to write position delete files.
#[derive(Debug)]
pub struct PositionDeleteFileWriter<
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
> {
    inner: Option<RollingFileWriter<B, L, F>>,
    config: PositionDeleteWriterConfig,
    partition_key: Option<PartitionKey>,
    // Track unique data file paths referenced by position deletes
    referenced_data_files: HashSet<String>,
}

impl<B, L, F> PositionDeleteFileWriter<B, L, F>
where
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
{
    /// Returns the Arrow schema for position delete files.
    pub fn delete_schema(&self) -> &ArrowSchemaRef {
        &self.config.delete_schema
    }

    fn validate_schema(&self, batch: &RecordBatch) -> Result<()> {
        let expected = &self.config.delete_schema;
        let actual = batch.schema();

        if actual.fields().len() < 2 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Position delete batch must have at least 2 columns (file_path, pos), got {}",
                    actual.fields().len()
                ),
            ));
        }

        // Validate file_path column (index 0)
        let file_path_field = actual.field(0);
        let expected_file_path = expected.field(0);
        if file_path_field.data_type() != expected_file_path.data_type() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Position delete file_path column must be {}, got {}",
                    expected_file_path.data_type(),
                    file_path_field.data_type()
                ),
            ));
        }
        if file_path_field.is_nullable() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Position delete file_path column must be non-nullable",
            ));
        }

        // Validate pos column (index 1)
        let pos_field = actual.field(1);
        let expected_pos = expected.field(1);
        if pos_field.data_type() != expected_pos.data_type() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Position delete pos column must be {}, got {}",
                    expected_pos.data_type(),
                    pos_field.data_type()
                ),
            ));
        }
        if pos_field.is_nullable() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Position delete pos column must be non-nullable",
            ));
        }

        let file_path_field_id = file_path_field
            .metadata()
            .get(PARQUET_FIELD_ID_META_KEY)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Position delete file_path column must have field ID metadata (expected {})",
                        POSITION_DELETE_FILE_PATH_FIELD_ID
                    ),
                )
            })?;

        let expected_file_path_id = POSITION_DELETE_FILE_PATH_FIELD_ID.to_string();
        if file_path_field_id != &expected_file_path_id {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Position delete file_path field ID must be {}, got {}",
                    expected_file_path_id, file_path_field_id
                ),
            ));
        }

        let pos_field_id = pos_field
            .metadata()
            .get(PARQUET_FIELD_ID_META_KEY)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Position delete pos column must have field ID metadata (expected {})",
                        POSITION_DELETE_POS_FIELD_ID
                    ),
                )
            })?;

        let expected_pos_id = POSITION_DELETE_POS_FIELD_ID.to_string();
        if pos_field_id != &expected_pos_id {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Position delete pos field ID must be {}, got {}",
                    expected_pos_id, pos_field_id
                ),
            ));
        }

        Ok(())
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
        self.validate_schema(&batch)?;

        let file_path_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "First column (file_path) must be StringArray",
                )
            })?;

        let pos_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Second column (pos) must be Int64Array",
                )
            })?;

        for i in 0..file_path_array.len() {
            if file_path_array.is_null(i) {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("Position delete file_path at row {i} is null, but must be non-null"),
                ));
            }
            if pos_array.is_null(i) {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("Position delete pos at row {i} is null, but must be non-null"),
                ));
            }
            let pos = pos_array.value(i);
            if pos < 0 {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("Position delete pos at row {i} is negative ({pos}), must be >= 0"),
                ));
            }
            let path = file_path_array.value(i);
            self.referenced_data_files.insert(path.to_string());
        }

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
            let referenced_data_file = if self.referenced_data_files.len() == 1 {
                self.referenced_data_files.iter().next().cloned()
            } else {
                None
            };

            writer
                .close()
                .await?
                .into_iter()
                .map(|mut res| {
                    res.content(DataContentType::PositionDeletes);

                    if let Some(ref path) = referenced_data_file {
                        res.referenced_data_file(Some(path.clone()));
                    }

                    if let Some(pk) = self.partition_key.as_ref() {
                        res.partition(pk.data().clone());
                        res.partition_spec_id(pk.spec().spec_id());
                    }

                    res.build().map_err(|e| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!("Failed to build position delete data file: {e}"),
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

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_select::concat::concat_batches;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use crate::io::FileIOBuilder;
    use crate::spec::{DataContentType, DataFileFormat, NestedField, PrimitiveType, Schema, Type};
    use crate::writer::base_writer::position_delete_writer::{
        PositionDeleteFileWriterBuilder, PositionDeleteWriterConfig,
    };
    use crate::writer::file_writer::ParquetWriterBuilder;
    use crate::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use crate::writer::{IcebergWriter, IcebergWriterBuilder};

    #[tokio::test]
    async fn test_position_delete_writer_single_file() -> Result<(), anyhow::Error> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // Create config for position deletes
        let config = PositionDeleteWriterConfig::new();
        let delete_schema = config.delete_schema().clone();

        // Create delete schema for parquet writer
        let iceberg_delete_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(
                        super::POSITION_DELETE_FILE_PATH_FIELD_ID,
                        "file_path",
                        Type::Primitive(PrimitiveType::String),
                    )
                    .into(),
                    NestedField::required(
                        super::POSITION_DELETE_POS_FIELD_ID,
                        "pos",
                        Type::Primitive(PrimitiveType::Long),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        let pb =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), iceberg_delete_schema);
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            pb,
            file_io.clone(),
            location_gen,
            file_name_gen,
        );
        let mut position_delete_writer =
            PositionDeleteFileWriterBuilder::new(rolling_writer_builder, config)
                .build(None)
                .await?;

        // Write position deletes for a single data file
        let file_paths = Arc::new(StringArray::from(vec![
            "s3://bucket/table/data/file1.parquet",
            "s3://bucket/table/data/file1.parquet",
            "s3://bucket/table/data/file1.parquet",
        ]));
        let positions = Arc::new(Int64Array::from(vec![0, 5, 10]));
        let batch = RecordBatch::try_new(delete_schema.clone(), vec![file_paths, positions])?;

        position_delete_writer.write(batch.clone()).await?;
        let res = position_delete_writer.close().await?;

        // Verify results
        assert_eq!(res.len(), 1);
        let data_file = &res[0];
        assert_eq!(data_file.content_type(), DataContentType::PositionDeletes);
        assert_eq!(
            data_file.referenced_data_file(),
            Some("s3://bucket/table/data/file1.parquet".to_string())
        );
        assert_eq!(data_file.record_count(), 3);

        // Verify parquet file contents
        let input_file = file_io.new_input(data_file.file_path()).unwrap();
        let input_content = input_file.read().await.unwrap();
        let reader_builder =
            ParquetRecordBatchReaderBuilder::try_new(input_content.clone()).unwrap();
        let reader = reader_builder.build().unwrap();
        let batches: Vec<_> = reader.map(|b| b.unwrap()).collect();
        let result = concat_batches(&batch.schema(), &batches).unwrap();
        assert_eq!(result, batch);

        Ok(())
    }

    #[tokio::test]
    async fn test_position_delete_writer_multiple_files() -> Result<(), anyhow::Error> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let config = PositionDeleteWriterConfig::new();
        let delete_schema = config.delete_schema().clone();

        let iceberg_delete_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(
                        super::POSITION_DELETE_FILE_PATH_FIELD_ID,
                        "file_path",
                        Type::Primitive(PrimitiveType::String),
                    )
                    .into(),
                    NestedField::required(
                        super::POSITION_DELETE_POS_FIELD_ID,
                        "pos",
                        Type::Primitive(PrimitiveType::Long),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        let pb =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), iceberg_delete_schema);
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            pb,
            file_io.clone(),
            location_gen,
            file_name_gen,
        );
        let mut position_delete_writer =
            PositionDeleteFileWriterBuilder::new(rolling_writer_builder, config)
                .build(None)
                .await?;

        // Write position deletes for multiple data files
        let file_paths = Arc::new(StringArray::from(vec![
            "s3://bucket/table/data/file1.parquet",
            "s3://bucket/table/data/file2.parquet",
            "s3://bucket/table/data/file1.parquet",
        ]));
        let positions = Arc::new(Int64Array::from(vec![0, 5, 10]));
        let batch = RecordBatch::try_new(delete_schema, vec![file_paths, positions])?;

        position_delete_writer.write(batch).await?;
        let res = position_delete_writer.close().await?;

        // Verify results
        assert_eq!(res.len(), 1);
        let data_file = &res[0];
        assert_eq!(data_file.content_type(), DataContentType::PositionDeletes);
        // When multiple files are referenced, referenced_data_file should be None
        assert_eq!(data_file.referenced_data_file(), None);
        assert_eq!(data_file.record_count(), 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_position_delete_config_schema() {
        let config = PositionDeleteWriterConfig::new();
        let schema = config.delete_schema();

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "file_path");
        assert_eq!(schema.field(0).data_type(), &arrow_schema::DataType::Utf8);
        assert!(!schema.field(0).is_nullable());
        assert_eq!(schema.field(1).name(), "pos");
        assert_eq!(schema.field(1).data_type(), &arrow_schema::DataType::Int64);
        assert!(!schema.field(1).is_nullable());
    }

    #[tokio::test]
    async fn test_position_delete_writer_closed_error() {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let config = PositionDeleteWriterConfig::new();
        let delete_schema = config.delete_schema().clone();

        let iceberg_delete_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(
                        super::POSITION_DELETE_FILE_PATH_FIELD_ID,
                        "file_path",
                        Type::Primitive(PrimitiveType::String),
                    )
                    .into(),
                    NestedField::required(
                        super::POSITION_DELETE_POS_FIELD_ID,
                        "pos",
                        Type::Primitive(PrimitiveType::Long),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        let pb =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), iceberg_delete_schema);
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            pb,
            file_io,
            location_gen,
            file_name_gen,
        );
        let mut position_delete_writer =
            PositionDeleteFileWriterBuilder::new(rolling_writer_builder, config)
                .build(None)
                .await
                .unwrap();

        // Write and close
        let file_paths = Arc::new(StringArray::from(vec!["s3://bucket/file.parquet"]));
        let positions = Arc::new(Int64Array::from(vec![0i64]));
        let batch =
            RecordBatch::try_new(delete_schema.clone(), vec![file_paths, positions]).unwrap();
        position_delete_writer.write(batch).await.unwrap();
        let _ = position_delete_writer.close().await.unwrap();

        // Try to write after close - should fail
        let file_paths = Arc::new(StringArray::from(vec!["s3://bucket/file2.parquet"]));
        let positions = Arc::new(Int64Array::from(vec![1i64]));
        let batch =
            RecordBatch::try_new(delete_schema.clone(), vec![file_paths, positions]).unwrap();
        let result = position_delete_writer.write(batch).await;
        assert!(result.is_err());

        // Try to close again - should fail
        let result = position_delete_writer.close().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_position_delete_validation_wrong_column_count() {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let config = PositionDeleteWriterConfig::new();

        let iceberg_delete_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(
                        super::POSITION_DELETE_FILE_PATH_FIELD_ID,
                        "file_path",
                        Type::Primitive(PrimitiveType::String),
                    )
                    .into(),
                    NestedField::required(
                        super::POSITION_DELETE_POS_FIELD_ID,
                        "pos",
                        Type::Primitive(PrimitiveType::Long),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        let pb =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), iceberg_delete_schema);
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            pb,
            file_io,
            location_gen,
            file_name_gen,
        );
        let mut writer = PositionDeleteFileWriterBuilder::new(rolling_writer_builder, config)
            .build(None)
            .await
            .unwrap();

        // Create batch with only one column
        let single_col_schema =
            Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "only_col",
                arrow_schema::DataType::Utf8,
                false,
            )]));
        let batch =
            RecordBatch::try_new(single_col_schema, vec![Arc::new(StringArray::from(vec![
                "test",
            ]))])
            .unwrap();

        let result = writer.write(batch).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("at least 2 columns"));
    }

    #[tokio::test]
    async fn test_position_delete_validation_wrong_file_path_type() {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let config = PositionDeleteWriterConfig::new();

        let iceberg_delete_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(
                        super::POSITION_DELETE_FILE_PATH_FIELD_ID,
                        "file_path",
                        Type::Primitive(PrimitiveType::String),
                    )
                    .into(),
                    NestedField::required(
                        super::POSITION_DELETE_POS_FIELD_ID,
                        "pos",
                        Type::Primitive(PrimitiveType::Long),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        let pb =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), iceberg_delete_schema);
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            pb,
            file_io,
            location_gen,
            file_name_gen,
        );
        let mut writer = PositionDeleteFileWriterBuilder::new(rolling_writer_builder, config)
            .build(None)
            .await
            .unwrap();

        // Create batch with wrong type for file_path (Int64 instead of Utf8)
        let wrong_schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("file_path", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("pos", arrow_schema::DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(wrong_schema, vec![
            Arc::new(Int64Array::from(vec![1i64])),
            Arc::new(Int64Array::from(vec![0i64])),
        ])
        .unwrap();

        let result = writer.write(batch).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("file_path column must be"));
    }

    #[tokio::test]
    async fn test_position_delete_validation_wrong_pos_type() {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let config = PositionDeleteWriterConfig::new();

        let iceberg_delete_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(
                        super::POSITION_DELETE_FILE_PATH_FIELD_ID,
                        "file_path",
                        Type::Primitive(PrimitiveType::String),
                    )
                    .into(),
                    NestedField::required(
                        super::POSITION_DELETE_POS_FIELD_ID,
                        "pos",
                        Type::Primitive(PrimitiveType::Long),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        let pb =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), iceberg_delete_schema);
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            pb,
            file_io,
            location_gen,
            file_name_gen,
        );
        let mut writer = PositionDeleteFileWriterBuilder::new(rolling_writer_builder, config)
            .build(None)
            .await
            .unwrap();

        // Create batch with wrong type for pos (Utf8 instead of Int64)
        let wrong_schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("file_path", arrow_schema::DataType::Utf8, false),
            arrow_schema::Field::new("pos", arrow_schema::DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(wrong_schema, vec![
            Arc::new(StringArray::from(vec!["s3://bucket/file.parquet"])),
            Arc::new(StringArray::from(vec!["0"])),
        ])
        .unwrap();

        let result = writer.write(batch).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("pos column must be"));
    }

    #[tokio::test]
    async fn test_position_delete_validation_nullable_file_path() {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let config = PositionDeleteWriterConfig::new();

        let iceberg_delete_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(
                        super::POSITION_DELETE_FILE_PATH_FIELD_ID,
                        "file_path",
                        Type::Primitive(PrimitiveType::String),
                    )
                    .into(),
                    NestedField::required(
                        super::POSITION_DELETE_POS_FIELD_ID,
                        "pos",
                        Type::Primitive(PrimitiveType::Long),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        let pb =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), iceberg_delete_schema);
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            pb,
            file_io,
            location_gen,
            file_name_gen,
        );
        let mut writer = PositionDeleteFileWriterBuilder::new(rolling_writer_builder, config)
            .build(None)
            .await
            .unwrap();

        // Create batch with nullable file_path
        let nullable_schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("file_path", arrow_schema::DataType::Utf8, true), // nullable!
            arrow_schema::Field::new("pos", arrow_schema::DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(nullable_schema, vec![
            Arc::new(StringArray::from(vec!["s3://bucket/file.parquet"])),
            Arc::new(Int64Array::from(vec![0i64])),
        ])
        .unwrap();

        let result = writer.write(batch).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string()
                .contains("file_path column must be non-nullable")
        );
    }

    #[tokio::test]
    async fn test_position_delete_validation_negative_position() {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let config = PositionDeleteWriterConfig::new();
        let delete_schema = config.delete_schema().clone();

        let iceberg_delete_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(
                        super::POSITION_DELETE_FILE_PATH_FIELD_ID,
                        "file_path",
                        Type::Primitive(PrimitiveType::String),
                    )
                    .into(),
                    NestedField::required(
                        super::POSITION_DELETE_POS_FIELD_ID,
                        "pos",
                        Type::Primitive(PrimitiveType::Long),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        let pb =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), iceberg_delete_schema);
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            pb,
            file_io,
            location_gen,
            file_name_gen,
        );
        let mut writer = PositionDeleteFileWriterBuilder::new(rolling_writer_builder, config)
            .build(None)
            .await
            .unwrap();

        // Create batch with negative position
        let batch = RecordBatch::try_new(delete_schema, vec![
            Arc::new(StringArray::from(vec!["s3://bucket/file.parquet"])),
            Arc::new(Int64Array::from(vec![-1i64])),
        ])
        .unwrap();

        let result = writer.write(batch).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("negative"));
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

        let config = PositionDeleteWriterConfig::new();
        let delete_schema = config.delete_schema().clone();

        let iceberg_delete_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(
                        super::POSITION_DELETE_FILE_PATH_FIELD_ID,
                        "file_path",
                        Type::Primitive(PrimitiveType::String),
                    )
                    .into(),
                    NestedField::required(
                        super::POSITION_DELETE_POS_FIELD_ID,
                        "pos",
                        Type::Primitive(PrimitiveType::Long),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        let pb =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), iceberg_delete_schema);
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            pb,
            file_io.clone(),
            location_gen,
            file_name_gen,
        );
        let mut writer = PositionDeleteFileWriterBuilder::new(rolling_writer_builder, config)
            .build(None)
            .await?;

        // Write multiple batches
        let batch1 = RecordBatch::try_new(delete_schema.clone(), vec![
            Arc::new(StringArray::from(vec!["s3://bucket/file1.parquet"])),
            Arc::new(Int64Array::from(vec![0i64])),
        ])?;
        let batch2 = RecordBatch::try_new(delete_schema.clone(), vec![
            Arc::new(StringArray::from(vec![
                "s3://bucket/file1.parquet",
                "s3://bucket/file1.parquet",
            ])),
            Arc::new(Int64Array::from(vec![5i64, 10i64])),
        ])?;
        let batch3 = RecordBatch::try_new(delete_schema, vec![
            Arc::new(StringArray::from(vec!["s3://bucket/file1.parquet"])),
            Arc::new(Int64Array::from(vec![15i64])),
        ])?;

        writer.write(batch1).await?;
        writer.write(batch2).await?;
        writer.write(batch3).await?;

        let res = writer.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = &res[0];
        assert_eq!(data_file.content_type(), DataContentType::PositionDeletes);
        assert_eq!(data_file.record_count(), 4); // 1 + 2 + 1 = 4 records
        // All deletes reference the same file
        assert_eq!(
            data_file.referenced_data_file(),
            Some("s3://bucket/file1.parquet".to_string())
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_position_delete_writer_unicode_paths() -> Result<(), anyhow::Error> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let config = PositionDeleteWriterConfig::new();
        let delete_schema = config.delete_schema().clone();

        let iceberg_delete_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(
                        super::POSITION_DELETE_FILE_PATH_FIELD_ID,
                        "file_path",
                        Type::Primitive(PrimitiveType::String),
                    )
                    .into(),
                    NestedField::required(
                        super::POSITION_DELETE_POS_FIELD_ID,
                        "pos",
                        Type::Primitive(PrimitiveType::Long),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        let pb =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), iceberg_delete_schema);
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            pb,
            file_io.clone(),
            location_gen,
            file_name_gen,
        );
        let mut writer = PositionDeleteFileWriterBuilder::new(rolling_writer_builder, config)
            .build(None)
            .await?;

        // Unicode file paths (Chinese, Japanese, emoji)
        let unicode_paths = vec![
            "s3://bucket/数据/文件.parquet",
            "s3://bucket/データ/ファイル.parquet",
            "s3://bucket/📊/data.parquet",
        ];

        let batch = RecordBatch::try_new(delete_schema, vec![
            Arc::new(StringArray::from(unicode_paths.clone())),
            Arc::new(Int64Array::from(vec![0i64, 1i64, 2i64])),
        ])?;

        writer.write(batch).await?;
        let res = writer.close().await?;

        assert_eq!(res.len(), 1);
        let data_file = &res[0];
        assert_eq!(data_file.content_type(), DataContentType::PositionDeletes);
        assert_eq!(data_file.record_count(), 3);
        // Multiple files referenced, so no referenced_data_file
        assert_eq!(data_file.referenced_data_file(), None);

        // Verify we can read back the Unicode paths
        let input_file = file_io.new_input(data_file.file_path())?;
        let input_content = input_file.read().await?;
        let reader_builder = ParquetRecordBatchReaderBuilder::try_new(input_content)?;
        let reader = reader_builder.build()?;
        let batches: Vec<_> = reader.map(|b| b.unwrap()).collect();

        let result_batch = &batches[0];
        let file_path_col = result_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        for (i, expected_path) in unicode_paths.iter().enumerate() {
            assert_eq!(file_path_col.value(i), *expected_path);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_position_delete_writer_max_position() -> Result<(), anyhow::Error> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let config = PositionDeleteWriterConfig::new();
        let delete_schema = config.delete_schema().clone();

        let iceberg_delete_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(
                        super::POSITION_DELETE_FILE_PATH_FIELD_ID,
                        "file_path",
                        Type::Primitive(PrimitiveType::String),
                    )
                    .into(),
                    NestedField::required(
                        super::POSITION_DELETE_POS_FIELD_ID,
                        "pos",
                        Type::Primitive(PrimitiveType::Long),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        let pb =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), iceberg_delete_schema);
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            pb,
            file_io.clone(),
            location_gen,
            file_name_gen,
        );
        let mut writer = PositionDeleteFileWriterBuilder::new(rolling_writer_builder, config)
            .build(None)
            .await?;

        // Test with i64::MAX position
        let batch = RecordBatch::try_new(delete_schema, vec![
            Arc::new(StringArray::from(vec!["s3://bucket/file.parquet"])),
            Arc::new(Int64Array::from(vec![i64::MAX])),
        ])?;

        writer.write(batch).await?;
        let res = writer.close().await?;

        assert_eq!(res.len(), 1);
        let data_file = &res[0];

        // Verify we can read back the max position
        let input_file = file_io.new_input(data_file.file_path())?;
        let input_content = input_file.read().await?;
        let reader_builder = ParquetRecordBatchReaderBuilder::try_new(input_content)?;
        let reader = reader_builder.build()?;
        let batches: Vec<_> = reader.map(|b| b.unwrap()).collect();

        let result_batch = &batches[0];
        let pos_col = result_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(pos_col.value(0), i64::MAX);

        Ok(())
    }

    #[tokio::test]
    async fn test_position_delete_field_ids_in_parquet() -> Result<(), anyhow::Error> {
        use parquet::arrow::arrow_reader::ArrowReaderMetadata;

        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let config = PositionDeleteWriterConfig::new();
        let delete_schema = config.delete_schema().clone();

        let iceberg_delete_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(
                        super::POSITION_DELETE_FILE_PATH_FIELD_ID,
                        "file_path",
                        Type::Primitive(PrimitiveType::String),
                    )
                    .into(),
                    NestedField::required(
                        super::POSITION_DELETE_POS_FIELD_ID,
                        "pos",
                        Type::Primitive(PrimitiveType::Long),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        let pb =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), iceberg_delete_schema);
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            pb,
            file_io.clone(),
            location_gen,
            file_name_gen,
        );
        let mut writer = PositionDeleteFileWriterBuilder::new(rolling_writer_builder, config)
            .build(None)
            .await?;

        let batch = RecordBatch::try_new(delete_schema, vec![
            Arc::new(StringArray::from(vec!["s3://bucket/file.parquet"])),
            Arc::new(Int64Array::from(vec![0i64])),
        ])?;

        writer.write(batch).await?;
        let res = writer.close().await?;

        assert_eq!(res.len(), 1);
        let data_file = &res[0];

        // Verify Iceberg field IDs are written to Parquet schema
        let input_file = file_io.new_input(data_file.file_path())?;
        let input_content = input_file.read().await?;
        let metadata = ArrowReaderMetadata::load(&input_content, Default::default())?;

        let parquet_schema = metadata.parquet_schema();
        let columns = parquet_schema.columns();

        // Verify file_path field ID
        let file_path_col = &columns[0];
        assert_eq!(
            file_path_col.self_type().get_basic_info().id(),
            super::POSITION_DELETE_FILE_PATH_FIELD_ID
        );

        // Verify pos field ID
        let pos_col = &columns[1];
        assert_eq!(
            pos_col.self_type().get_basic_info().id(),
            super::POSITION_DELETE_POS_FIELD_ID
        );

        Ok(())
    }
}
