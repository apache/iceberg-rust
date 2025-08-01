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

//! This module provides `PositionDeleteWriter`.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use async_trait::async_trait;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::spec::{DataFile, Struct};
use crate::writer::file_writer::{FileWriter, FileWriterBuilder};
use crate::writer::{IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind, Result};

/// Field ID for the file_path column in position delete files.
/// From the Iceberg spec: https://iceberg.apache.org/spec/#position-delete-files
const FIELD_ID_POSITIONAL_DELETE_FILE_PATH: i64 = 2147483546;

/// Field ID for the pos column in position delete files.
/// From the Iceberg spec: https://iceberg.apache.org/spec/#position-delete-files
const FIELD_ID_POSITIONAL_DELETE_POS: i64 = 2147483545;

/// Builder for `PositionDeleteWriter`.
#[derive(Clone, Debug)]
pub struct PositionDeleteFileWriterBuilder<B: FileWriterBuilder> {
    inner: B,
    config: PositionDeleteWriterConfig,
}

impl<B: FileWriterBuilder> PositionDeleteFileWriterBuilder<B> {
    /// Create a new `PositionDeleteFileWriterBuilder` using a `FileWriterBuilder`.
    pub fn new(inner: B, config: PositionDeleteWriterConfig) -> Self {
        Self { inner, config }
    }
}

/// Config for `PositionDeleteWriter`.
#[derive(Clone, Debug)]
pub struct PositionDeleteWriterConfig {
    partition_value: Struct,
    partition_spec_id: i32,
    /// Referenced data file that this position delete file applies to (optional)
    referenced_data_file: Option<String>,
}

impl PositionDeleteWriterConfig {
    /// Create a new `PositionDeleteWriterConfig`.
    pub fn new(
        partition_value: Option<Struct>,
        partition_spec_id: i32,
        referenced_data_file: Option<String>,
    ) -> Self {
        Self {
            partition_value: partition_value.unwrap_or_else(Struct::empty),
            partition_spec_id,
            referenced_data_file,
        }
    }

    /// Get the schema for position delete files.
    /// Position delete files have a required schema with file_path (String) and pos (Long) columns.
    pub fn position_delete_schema() -> ArrowSchemaRef {
        let fields = vec![
            Field::new("file_path", DataType::Utf8, false).with_metadata(
                std::collections::HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    FIELD_ID_POSITIONAL_DELETE_FILE_PATH.to_string(),
                )]),
            ),
            Field::new("pos", DataType::Int64, false).with_metadata(
                std::collections::HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    FIELD_ID_POSITIONAL_DELETE_POS.to_string(),
                )]),
            ),
        ];
        Arc::new(ArrowSchema::new(fields))
    }

    /// Validate that the provided RecordBatch matches the position delete schema.
    pub fn validate_batch(&self, batch: &RecordBatch) -> Result<()> {
        let expected_schema = Self::position_delete_schema();
        
        if batch.num_columns() != 2 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Position delete files must have exactly 2 columns, got {}",
                    batch.num_columns()
                ),
            ));
        }

        // Check file_path column
        let file_path_field = batch.schema().field(0);
        if file_path_field.name() != "file_path"
            || !matches!(file_path_field.data_type(), DataType::Utf8)
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "First column must be 'file_path' of type String",
            ));
        }

        // Check pos column
        let pos_field = batch.schema().field(1);
        if pos_field.name() != "pos"
            || !matches!(pos_field.data_type(), DataType::Int64)
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Second column must be 'pos' of type Int64",
            ));
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl<B: FileWriterBuilder> IcebergWriterBuilder for PositionDeleteFileWriterBuilder<B> {
    type R = PositionDeleteFileWriter<B>;

    async fn build(self) -> Result<Self::R> {
        Ok(PositionDeleteFileWriter {
            inner_writer: Some(self.inner.clone().build().await?),
            config: self.config,
        })
    }
}

/// A writer for position delete files.
/// 
/// Position delete files are used to mark specific rows in data files as deleted by their position.
/// They contain two columns:
/// - file_path: the path of the data file
/// - pos: the 0-based position of the deleted row in the data file
#[derive(Debug)]
pub struct PositionDeleteFileWriter<B: FileWriterBuilder> {
    inner_writer: Option<B::R>,
    config: PositionDeleteWriterConfig,
}

#[async_trait::async_trait]
impl<B: FileWriterBuilder> IcebergWriter for PositionDeleteFileWriter<B> {
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        // Validate that the batch conforms to position delete schema
        self.config.validate_batch(&batch)?;

        if let Some(writer) = self.inner_writer.as_mut() {
            writer.write(&batch).await
        } else {
            Err(Error::new(
                ErrorKind::Unexpected,
                "Position delete inner writer has been closed.",
            ))
        }
    }

    async fn close(&mut self) -> Result<Vec<DataFile>> {
        if let Some(writer) = self.inner_writer.take() {
            Ok(writer
                .close()
                .await?
                .into_iter()
                .map(|mut res| {
                    res.content(crate::spec::DataContentType::PositionDeletes);
                    res.partition(self.config.partition_value.clone());
                    res.partition_spec_id(self.config.partition_spec_id);
                    if let Some(ref referenced_file) = self.config.referenced_data_file {
                        // TODO: Set referenced_data_file in DataFileBuilder when available
                        // res.referenced_data_file(referenced_file.clone());
                    }
                    res.build().expect("Valid position delete data file")
                })
                .collect())
        } else {
            Err(Error::new(
                ErrorKind::Unexpected,
                "Position delete inner writer has been closed.",
            ))
        }
    }
}

/// Utility functions for creating position delete data.
impl PositionDeleteFileWriter<()> {
    /// Create a RecordBatch for position delete data.
    /// 
    /// # Arguments
    /// * `file_paths` - Paths of the data files containing the rows to delete
    /// * `positions` - 0-based positions of the rows to delete in the corresponding files
    /// 
    /// # Returns
    /// A RecordBatch with the position delete schema
    pub fn create_position_delete_batch(
        file_paths: Vec<String>,
        positions: Vec<i64>,
    ) -> Result<RecordBatch> {
        if file_paths.len() != positions.len() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "file_paths and positions must have the same length",
            ));
        }

        let file_path_array = Arc::new(StringArray::from(file_paths));
        let pos_array = Arc::new(Int64Array::from(positions));

        let schema = PositionDeleteWriterConfig::position_delete_schema();
        
        RecordBatch::try_new(schema, vec![file_path_array, pos_array]).map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Failed to create position delete batch: {}", e),
            )
        })?
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::{DataFileFormat, Struct};
    use crate::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use crate::writer::file_writer::ParquetWriterBuilder;

    #[test]
    fn test_position_delete_schema() {
        let schema = PositionDeleteWriterConfig::position_delete_schema();
        
        assert_eq!(schema.fields().len(), 2);
        
        let file_path_field = &schema.fields()[0];
        assert_eq!(file_path_field.name(), "file_path");
        assert_eq!(file_path_field.data_type(), &DataType::Utf8);
        
        let pos_field = &schema.fields()[1];
        assert_eq!(pos_field.name(), "pos");
        assert_eq!(pos_field.data_type(), &DataType::Int64);
    }

    #[test]
    fn test_create_position_delete_batch() {
        let file_paths = vec![
            "/path/to/file1.parquet".to_string(),
            "/path/to/file2.parquet".to_string(),
        ];
        let positions = vec![10, 20];

        let batch = PositionDeleteFileWriter::create_position_delete_batch(
            file_paths.clone(),
            positions.clone(),
        ).expect("Failed to create batch");

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);

        let file_path_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Expected StringArray");
        
        let pos_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Expected Int64Array");

        assert_eq!(file_path_array.value(0), "/path/to/file1.parquet");
        assert_eq!(file_path_array.value(1), "/path/to/file2.parquet");
        assert_eq!(pos_array.value(0), 10);
        assert_eq!(pos_array.value(1), 20);
    }

    #[test]
    fn test_validate_batch_valid() {
        let config = PositionDeleteWriterConfig::new(None, 0, None);
        
        let file_paths = vec!["/path/to/file.parquet".to_string()];
        let positions = vec![5];
        
        let batch = PositionDeleteFileWriter::create_position_delete_batch(
            file_paths,
            positions,
        ).expect("Failed to create batch");
        
        config.validate_batch(&batch).expect("Valid batch should pass validation");
    }

    #[test]
    fn test_validate_batch_wrong_column_count() {
        let config = PositionDeleteWriterConfig::new(None, 0, None);
        
        // Create a batch with only one column
        let file_path_array = Arc::new(StringArray::from(vec!["/path/to/file.parquet"]));
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("file_path", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![file_path_array]).unwrap();
        
        assert!(config.validate_batch(&batch).is_err());
    }

    #[test]
    fn test_validate_batch_wrong_column_types() {
        let config = PositionDeleteWriterConfig::new(None, 0, None);
        
        // Create a batch with wrong data types
        let file_path_array = Arc::new(Int64Array::from(vec![123])); // Wrong type
        let pos_array = Arc::new(StringArray::from(vec!["not_a_number"])); // Wrong type
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("file_path", DataType::Int64, false), // Wrong type
            Field::new("pos", DataType::Utf8, false), // Wrong type
        ]));
        let batch = RecordBatch::try_new(schema, vec![file_path_array, pos_array]).unwrap();
        
        assert!(config.validate_batch(&batch).is_err());
    }

    #[tokio::test]
    async fn test_position_delete_writer() {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new("file")
            .with_prop("table_dir", temp_dir.path().to_str().unwrap())
            .build()
            .unwrap();

        let location_gen =
            DefaultLocationGenerator::new("/tmp/test", "", "", &file_io)
                .unwrap();
        let file_name_gen = DefaultFileNameGenerator::new(
            "test".to_string(),
            None,
            DataFileFormat::Parquet,
        );

        let pb = ParquetWriterBuilder::new(
            WriterProperties::default(),
            PositionDeleteWriterConfig::position_delete_schema(),
            file_io,
            location_gen,
            file_name_gen,
        );

        let config = PositionDeleteWriterConfig::new(None, 0, None);
        let mut writer = PositionDeleteFileWriterBuilder::new(pb, config)
            .build()
            .await
            .unwrap();

        // Create position delete data
        let file_paths = vec![
            "/path/to/file1.parquet".to_string(),
            "/path/to/file2.parquet".to_string(),
        ];
        let positions = vec![10, 20];
        let batch = PositionDeleteFileWriter::create_position_delete_batch(
            file_paths,
            positions,
        ).expect("Failed to create batch");

        writer.write(batch).await.unwrap();
        let data_files = writer.close().await.unwrap();

        assert_eq!(data_files.len(), 1);
        let data_file = &data_files[0];
        assert_eq!(data_file.content, crate::spec::DataContentType::PositionDeletes);
        assert_eq!(data_file.record_count, 2);
    }
} 