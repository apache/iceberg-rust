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

use std::sync::Arc;

use arrow_array::{Array, Int64Array, StringArray};
use futures::{StreamExt, TryStreamExt};
use parquet::arrow::{PARQUET_FIELD_ID_META_KEY, ParquetRecordBatchStreamBuilder, ProjectionMask};

use crate::arrow::ArrowReader;
use crate::arrow::reader::ParquetReadOptions;
use crate::arrow::record_batch_transformer::RecordBatchTransformerBuilder;
use crate::arrow::scan_metrics::ScanMetrics;
use crate::delete_vector::DeleteVector;
use crate::error::invalid_data;
use crate::io::FileIO;
use crate::scan::{ArrowRecordBatchStream, FileScanTaskDeleteFile};
use crate::spec::{DataContentType, DataFileFormat, Schema, SchemaRef};
use crate::{Error, ErrorKind, Result};

/// Delete File Loader
#[allow(unused)]
#[async_trait::async_trait]
pub trait DeleteFileLoader {
    /// Read the delete file referred to in the task
    ///
    /// Returns the contents of the delete file as a RecordBatch stream. Applies schema evolution.
    async fn read_delete_file(
        &self,
        task: &FileScanTaskDeleteFile,
        schema: SchemaRef,
    ) -> Result<ArrowRecordBatchStream>;
}

#[derive(Clone, Debug)]
pub(crate) struct BasicDeleteFileLoader {
    file_io: FileIO,
    scan_metrics: ScanMetrics,
}

#[allow(unused_variables)]
impl BasicDeleteFileLoader {
    pub fn new(file_io: FileIO, scan_metrics: ScanMetrics) -> Self {
        BasicDeleteFileLoader {
            file_io,
            scan_metrics,
        }
    }

    pub(crate) fn file_io(&self) -> &FileIO {
        &self.file_io
    }

    /// Loads a RecordBatchStream for a given datafile.
    pub(crate) async fn parquet_to_batch_stream(
        &self,
        data_file_path: &str,
        file_size_in_bytes: u64,
        key_metadata: Option<&[u8]>,
    ) -> Result<ArrowRecordBatchStream> {
        /*
           Essentially a super-cut-down ArrowReader. We can't use ArrowReader directly
           as that introduces a circular dependency.
        */
        let parquet_read_options = ParquetReadOptions::builder().build();

        let (parquet_file_reader, arrow_metadata) = ArrowReader::open_parquet_file(
            data_file_path,
            &self.file_io,
            file_size_in_bytes,
            parquet_read_options,
            self.scan_metrics.bytes_read_counter(),
            key_metadata,
        )
        .await?;

        let record_batch_stream =
            ParquetRecordBatchStreamBuilder::new_with_metadata(parquet_file_reader, arrow_metadata)
                .build()?
                .map_err(|e| Error::new(ErrorKind::Unexpected, format!("{e}")));

        Ok(Box::pin(record_batch_stream) as ArrowRecordBatchStream)
    }

    /// Evolves the schema of the RecordBatches from an equality delete file.
    ///
    /// Per the [Iceberg spec](https://iceberg.apache.org/spec/#equality-delete-files),
    /// only evolves the specified `equality_ids` columns, not all table columns.
    pub(crate) async fn evolve_schema(
        record_batch_stream: ArrowRecordBatchStream,
        target_schema: Arc<Schema>,
        equality_ids: &[i32],
    ) -> Result<ArrowRecordBatchStream> {
        let mut record_batch_transformer =
            RecordBatchTransformerBuilder::new(target_schema.clone(), equality_ids).build();

        let record_batch_stream = record_batch_stream.map(move |record_batch| {
            record_batch.and_then(|record_batch| {
                record_batch_transformer.process_record_batch(record_batch)
            })
        });

        Ok(Box::pin(record_batch_stream) as ArrowRecordBatchStream)
    }
}

#[async_trait::async_trait]
impl DeleteFileLoader for BasicDeleteFileLoader {
    async fn read_delete_file(
        &self,
        task: &FileScanTaskDeleteFile,
        schema: SchemaRef,
    ) -> Result<ArrowRecordBatchStream> {
        let raw_batch_stream = self
            .parquet_to_batch_stream(
                &task.file_path,
                task.file_size_in_bytes,
                task.key_metadata.as_deref(),
            )
            .await?;

        // For equality deletes, only evolve the equality_ids columns.
        // For positional deletes (equality_ids is None), use all field IDs.
        let field_ids = match &task.equality_ids {
            Some(ids) => ids.clone(),
            None => schema.field_id_to_name_map().keys().cloned().collect(),
        };

        Self::evolve_schema(raw_batch_stream, schema, &field_ids).await
    }
}

/// In-memory positions from a file-scoped position-delete file.
///
/// Iteration is sorted and de-duplicated.
pub struct PositionDeleteIndex {
    positions: DeleteVector,
}

impl PositionDeleteIndex {
    fn new() -> Self {
        Self {
            positions: DeleteVector::default(),
        }
    }

    /// Iterates deleted row positions in ascending order.
    pub fn iter(&self) -> impl Iterator<Item = i64> + '_ {
        self.positions.iter().map(|position| position as i64)
    }
}

/// Loads a file-scoped V2 Parquet position-delete file into an index.
///
/// Validates `file_path`, `pos`, and the expected data-file target. Snapshot applicability and
/// replacement policy remain the caller's responsibility.
pub struct PositionDeleteIndexLoader {
    basic_loader: BasicDeleteFileLoader,
}

impl PositionDeleteIndexLoader {
    /// Creates a loader for the given Iceberg `FileIO`.
    pub fn new(file_io: FileIO) -> Self {
        Self {
            basic_loader: BasicDeleteFileLoader::new(file_io, ScanMetrics::new()),
        }
    }

    fn reserved_field_index(
        schema: &arrow_schema::Schema,
        field_id: i32,
        logical_name: &str,
        delete_file_path: &str,
    ) -> Result<usize> {
        let mut matches = schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(index, field)| {
                field
                    .metadata()
                    .get(PARQUET_FIELD_ID_META_KEY)
                    .and_then(|id| id.parse::<i32>().ok())
                    .filter(|id| *id == field_id)
                    .map(|_| index)
            });

        let Some(index) = matches.next() else {
            return Err(invalid_data!(
                "Position-delete file {delete_file_path} has no `{logical_name}` column with reserved field id {field_id}"
            ));
        };
        if matches.next().is_some() {
            return Err(invalid_data!(
                "Position-delete file {delete_file_path} has multiple columns with reserved field id {field_id}"
            ));
        }

        Ok(index)
    }

    fn position_delete_columns(
        schema: &arrow_schema::Schema,
        delete_file_path: &str,
    ) -> Result<(usize, usize)> {
        let path_index = Self::reserved_field_index(
            schema,
            crate::metadata_columns::RESERVED_FIELD_ID_DELETE_FILE_PATH,
            "file_path",
            delete_file_path,
        )?;
        let position_index = Self::reserved_field_index(
            schema,
            crate::metadata_columns::RESERVED_FIELD_ID_DELETE_FILE_POS,
            "pos",
            delete_file_path,
        )?;

        let path_field = schema.field(path_index);
        if path_field.data_type() != &arrow_schema::DataType::Utf8 || path_field.is_nullable() {
            return Err(invalid_data!(
                "Position-delete file {delete_file_path} requires non-nullable Utf8 file_path"
            ));
        }

        let position_field = schema.field(position_index);
        if position_field.data_type() != &arrow_schema::DataType::Int64
            || position_field.is_nullable()
        {
            return Err(invalid_data!(
                "Position-delete file {delete_file_path} requires non-nullable Int64 pos"
            ));
        }

        Ok((path_index, position_index))
    }

    /// Loads positions targeting `expected_data_file`.
    ///
    /// Positions are sorted and de-duplicated after validating the physical row count.
    pub async fn load_file_scoped_positions(
        &self,
        delete_file: &FileScanTaskDeleteFile,
        expected_data_file: &str,
    ) -> Result<PositionDeleteIndex> {
        if delete_file.file_type != DataContentType::PositionDeletes
            || delete_file.file_format != DataFileFormat::Parquet
        {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!(
                    "Expected a V2 Parquet position-delete file, got {:?}/{:?} at {}",
                    delete_file.file_type, delete_file.file_format, delete_file.file_path
                ),
            ));
        }

        if delete_file.equality_ids.is_some() {
            return Err(invalid_data!(
                "Position-delete file {} must not carry equality_ids",
                delete_file.file_path
            ));
        }

        if delete_file.content_offset.is_some() || delete_file.content_size_in_bytes.is_some() {
            return Err(invalid_data!(
                "V2 Parquet position-delete file {} must not carry deletion-vector content coordinates",
                delete_file.file_path
            ));
        }

        if let Some(referenced_data_file) = delete_file.referenced_data_file.as_deref()
            && referenced_data_file != expected_data_file
        {
            return Err(invalid_data!(
                "Position-delete file {} references {referenced_data_file}, expected {expected_data_file}",
                delete_file.file_path
            ));
        }

        let parquet_read_options = ParquetReadOptions::builder().build();
        let (parquet_file_reader, arrow_metadata) = ArrowReader::open_parquet_file(
            &delete_file.file_path,
            self.basic_loader.file_io(),
            delete_file.file_size_in_bytes,
            parquet_read_options,
            self.basic_loader.scan_metrics.bytes_read_counter(),
            delete_file.key_metadata.as_deref(),
        )
        .await?;

        let mut stream_builder =
            ParquetRecordBatchStreamBuilder::new_with_metadata(parquet_file_reader, arrow_metadata);

        // Validate before reading so malformed empty files are rejected.
        let (path_root_index, position_root_index) = Self::position_delete_columns(
            stream_builder.schema().as_ref(),
            &delete_file.file_path,
        )?;

        // Read only file_path and pos; skip optional deleted-row payloads.
        let projection = ProjectionMask::roots(stream_builder.parquet_schema(), vec![
            path_root_index,
            position_root_index,
        ]);
        stream_builder = stream_builder.with_projection(projection);

        // Projection compacts ordinals, so resolve columns from the projected schema.
        let projected_stream = stream_builder.build()?;
        let (path_index, position_index) = Self::position_delete_columns(
            projected_stream.schema().as_ref(),
            &delete_file.file_path,
        )?;
        let mut batches = projected_stream.map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!(
                    "Failed to read position-delete file {}",
                    delete_file.file_path
                ),
            )
            .with_source(e)
        });

        let mut index = PositionDeleteIndex::new();
        let mut rows_read = 0u64;

        while let Some(batch) = batches.try_next().await? {
            let paths = batch
                .column(path_index)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    invalid_data!(
                        "Position-delete file {} has a non-Utf8 file_path column",
                        delete_file.file_path
                    )
                })?;
            let row_positions = batch
                .column(position_index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    invalid_data!(
                        "Position-delete file {} has a non-Int64 pos column",
                        delete_file.file_path
                    )
                })?;
            if paths.null_count() != 0 || row_positions.null_count() != 0 {
                return Err(invalid_data!(
                    "Position-delete file {} contains nulls",
                    delete_file.file_path
                ));
            }

            rows_read += batch.num_rows() as u64;
            let mut batch_positions = Vec::with_capacity(batch.num_rows());
            for row in 0..batch.num_rows() {
                let data_file = paths.value(row);
                if data_file != expected_data_file {
                    return Err(invalid_data!(
                        "File-scoped position-delete file {} contains target {data_file}, expected {expected_data_file}",
                        delete_file.file_path
                    ));
                }

                let position = row_positions.value(row);
                if position < 0 {
                    return Err(invalid_data!(
                        "Position-delete file {} contains a negative row position {position}",
                        delete_file.file_path
                    ));
                }
                batch_positions.push(position as u64);
            }

            // Fast path ordered batches; fall back for duplicates or out-of-order positions.
            if !batch_positions.is_empty()
                && let Err(err) = index.positions.insert_positions(&batch_positions)
            {
                tracing::debug!(
                    delete_file = %delete_file.file_path,
                    batch_len = batch_positions.len(),
                    error = %err,
                    "position-delete batch fell back to per-position insert"
                );
                for position in batch_positions {
                    index.positions.insert(position);
                }
            }
        }

        if let Some(expected_count) = delete_file.record_count
            && rows_read != expected_count
        {
            return Err(invalid_data!(
                "Position-delete file {} contains {rows_read} rows, expected {expected_count} from record_count",
                delete_file.file_path
            ));
        }

        Ok(index)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::sync::Arc;

    use arrow_array::{
        Array, ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray, StructArray,
    };
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use super::*;
    use crate::arrow::delete_filter::tests::setup;
    use crate::arrow::test_utils::write_encrypted_parquet;

    fn write_plain_parquet(path: &str, batch: &RecordBatch) {
        let file = File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
    }

    fn write_plain_parquet_batches(path: &str, batches: &[RecordBatch], row_group_size: usize) {
        assert!(!batches.is_empty());
        let file = File::create(path).unwrap();
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(row_group_size))
            .build();
        let mut writer = ArrowWriter::try_new(file, batches[0].schema(), Some(properties)).unwrap();
        for batch in batches {
            writer.write(batch).unwrap();
        }
        writer.close().unwrap();
    }

    fn position_delete_batch(paths: Vec<&str>, positions: Vec<i64>) -> RecordBatch {
        let schema = crate::arrow::delete_filter::tests::create_pos_del_schema();
        RecordBatch::try_new(schema, vec![
            Arc::new(StringArray::from(paths)),
            Arc::new(Int64Array::from(positions)),
        ])
        .unwrap()
    }

    fn position_delete_task(
        path: &str,
        record_count: Option<u64>,
        referenced_data_file: Option<&str>,
    ) -> FileScanTaskDeleteFile {
        FileScanTaskDeleteFile {
            file_path: path.to_string(),
            file_size_in_bytes: std::fs::metadata(path).unwrap().len(),
            file_type: DataContentType::PositionDeletes,
            file_format: DataFileFormat::Parquet,
            partition_spec_id: 0,
            equality_ids: None,
            referenced_data_file: referenced_data_file.map(str::to_string),
            content_offset: None,
            content_size_in_bytes: None,
            record_count,
            key_metadata: None,
        }
    }

    #[tokio::test]
    async fn test_position_delete_index_loader_reads_valid_file_and_deduplicates() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("pos-delete.parquet");
        let path = path.to_str().unwrap();
        let batch = position_delete_batch(
            vec![
                "data.parquet",
                "data.parquet",
                "data.parquet",
                "data.parquet",
            ],
            vec![5, 1, 5, i64::MAX],
        );
        write_plain_parquet(path, &batch);

        let task = position_delete_task(path, Some(4), Some("data.parquet"));
        let loader = PositionDeleteIndexLoader::new(FileIO::new_with_fs());
        let index = loader
            .load_file_scoped_positions(&task, "data.parquet")
            .await
            .unwrap();

        assert_eq!(index.iter().collect::<Vec<_>>(), vec![1, 5, i64::MAX]);
    }

    #[tokio::test]
    async fn test_position_delete_index_loader_merges_overlapping_positions_across_row_groups() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("pos-delete-multi-row-group.parquet");
        let path = path.to_str().unwrap();

        let first =
            position_delete_batch(vec!["data.parquet", "data.parquet", "data.parquet"], vec![
                1, 5, 10,
            ]);
        let second =
            position_delete_batch(vec!["data.parquet", "data.parquet", "data.parquet"], vec![
                5, 11, 2,
            ]);
        write_plain_parquet_batches(path, &[first, second], 3);

        let task = position_delete_task(path, Some(6), Some("data.parquet"));
        let index = PositionDeleteIndexLoader::new(FileIO::new_with_fs())
            .load_file_scoped_positions(&task, "data.parquet")
            .await
            .unwrap();

        assert_eq!(index.iter().collect::<Vec<_>>(), vec![1, 2, 5, 10, 11]);
    }

    #[tokio::test]
    async fn test_position_delete_index_loader_rejects_equality_ids_as_invalid_metadata() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("pos-delete-equality-ids.parquet");
        let path = path.to_str().unwrap();
        write_plain_parquet(path, &position_delete_batch(vec!["data.parquet"], vec![1]));

        let mut task = position_delete_task(path, Some(1), Some("data.parquet"));
        task.equality_ids = Some(vec![1]);

        let err = PositionDeleteIndexLoader::new(FileIO::new_with_fs())
            .load_file_scoped_positions(&task, "data.parquet")
            .await
            .err()
            .expect("expected invalid position-delete metadata");

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("must not carry equality_ids"));
    }

    #[tokio::test]
    async fn test_position_delete_index_loader_rejects_unsupported_content_or_format() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("pos-delete-unsupported.parquet");
        let path = path.to_str().unwrap();
        write_plain_parquet(path, &position_delete_batch(vec!["data.parquet"], vec![1]));

        let mut wrong_content = position_delete_task(path, Some(1), Some("data.parquet"));
        wrong_content.file_type = DataContentType::EqualityDeletes;
        let err = PositionDeleteIndexLoader::new(FileIO::new_with_fs())
            .load_file_scoped_positions(&wrong_content, "data.parquet")
            .await
            .err()
            .expect("expected unsupported delete content");
        assert_eq!(err.kind(), ErrorKind::FeatureUnsupported);

        let mut wrong_format = position_delete_task(path, Some(1), Some("data.parquet"));
        wrong_format.file_format = DataFileFormat::Avro;
        let err = PositionDeleteIndexLoader::new(FileIO::new_with_fs())
            .load_file_scoped_positions(&wrong_format, "data.parquet")
            .await
            .err()
            .expect("expected unsupported delete format");
        assert_eq!(err.kind(), ErrorKind::FeatureUnsupported);
    }

    #[tokio::test]
    async fn test_position_delete_index_loader_rejects_duplicate_reserved_field_ids() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("pos-delete-duplicate-id.parquet");
        let path = path.to_str().unwrap();

        let base_schema = crate::arrow::delete_filter::tests::create_pos_del_schema();
        let duplicate_path = Field::new("duplicate_file_path", DataType::Utf8, false)
            .with_metadata(base_schema.field(0).metadata().clone());
        let schema = Arc::new(ArrowSchema::new(vec![
            base_schema.field(0).clone(),
            duplicate_path,
            base_schema.field(1).clone(),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(StringArray::from(vec!["data.parquet"])),
            Arc::new(StringArray::from(vec!["data.parquet"])),
            Arc::new(Int64Array::from(vec![1i64])),
        ])
        .unwrap();
        write_plain_parquet(path, &batch);

        let task = position_delete_task(path, Some(1), Some("data.parquet"));
        let err = PositionDeleteIndexLoader::new(FileIO::new_with_fs())
            .load_file_scoped_positions(&task, "data.parquet")
            .await
            .err()
            .expect("expected duplicate reserved field-id error");

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message()
                .contains("multiple columns with reserved field id")
        );
    }

    #[tokio::test]
    async fn test_position_delete_index_loader_accepts_optional_row_column() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("pos-delete-with-row.parquet");
        let path = path.to_str().unwrap();

        let row_value_field = Arc::new(Field::new("id", DataType::Int64, true).with_metadata(
            HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string())]),
        ));
        let row_array = StructArray::from(vec![(
            row_value_field,
            Arc::new(Int64Array::from(vec![42i64])) as ArrayRef,
        )]);
        let row_field =
            Field::new("row", row_array.data_type().clone(), false).with_metadata(HashMap::from([
                (
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    (i32::MAX - 103).to_string(),
                ),
            ]));

        let base_schema = crate::arrow::delete_filter::tests::create_pos_del_schema();
        let schema = Arc::new(ArrowSchema::new(vec![
            base_schema.field(0).clone(),
            base_schema.field(1).clone(),
            row_field,
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(StringArray::from(vec!["data.parquet"])),
            Arc::new(Int64Array::from(vec![7i64])),
            Arc::new(row_array),
        ])
        .unwrap();
        write_plain_parquet(path, &batch);

        let task = position_delete_task(path, Some(1), Some("data.parquet"));
        let index = PositionDeleteIndexLoader::new(FileIO::new_with_fs())
            .load_file_scoped_positions(&task, "data.parquet")
            .await
            .unwrap();

        assert_eq!(index.iter().collect::<Vec<_>>(), vec![7]);
    }

    #[tokio::test]
    async fn test_position_delete_index_loader_requires_non_nullable_fields() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("pos-delete-nullable-schema.parquet");
        let path = path.to_str().unwrap();

        let base_schema = crate::arrow::delete_filter::tests::create_pos_del_schema();
        let schema = Arc::new(ArrowSchema::new(vec![
            base_schema.field(0).clone().with_nullable(true),
            base_schema.field(1).clone(),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(StringArray::from(vec![Some("data.parquet")])),
            Arc::new(Int64Array::from(vec![1i64])),
        ])
        .unwrap();
        write_plain_parquet(path, &batch);

        let task = position_delete_task(path, Some(1), Some("data.parquet"));
        let err = PositionDeleteIndexLoader::new(FileIO::new_with_fs())
            .load_file_scoped_positions(&task, "data.parquet")
            .await
            .err()
            .expect("expected position-delete loader error");

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("non-nullable Utf8 file_path"));
    }

    #[tokio::test]
    async fn test_position_delete_index_loader_rejects_wrong_file_path_type() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("pos-delete-wrong-path-type.parquet");
        let path = path.to_str().unwrap();

        let base_schema = crate::arrow::delete_filter::tests::create_pos_del_schema();
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("file_path", DataType::Int64, false)
                .with_metadata(base_schema.field(0).metadata().clone()),
            base_schema.field(1).clone(),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(Int64Array::from(vec![1i64])),
            Arc::new(Int64Array::from(vec![1i64])),
        ])
        .unwrap();
        write_plain_parquet(path, &batch);

        let task = position_delete_task(path, Some(1), Some("data.parquet"));
        let err = PositionDeleteIndexLoader::new(FileIO::new_with_fs())
            .load_file_scoped_positions(&task, "data.parquet")
            .await
            .err()
            .expect("expected invalid file_path type");

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("non-nullable Utf8 file_path"));
    }

    #[tokio::test]
    async fn test_position_delete_index_loader_rejects_wrong_position_type() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("pos-delete-wrong-pos-type.parquet");
        let path = path.to_str().unwrap();

        let base_schema = crate::arrow::delete_filter::tests::create_pos_del_schema();
        let schema = Arc::new(ArrowSchema::new(vec![
            base_schema.field(0).clone(),
            Field::new("pos", DataType::Int32, false)
                .with_metadata(base_schema.field(1).metadata().clone()),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(StringArray::from(vec!["data.parquet"])),
            Arc::new(Int32Array::from(vec![1i32])),
        ])
        .unwrap();
        write_plain_parquet(path, &batch);

        let task = position_delete_task(path, Some(1), Some("data.parquet"));
        let err = PositionDeleteIndexLoader::new(FileIO::new_with_fs())
            .load_file_scoped_positions(&task, "data.parquet")
            .await
            .err()
            .expect("expected invalid pos type");

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("non-nullable Int64 pos"));
    }

    #[tokio::test]
    async fn test_position_delete_index_loader_requires_reserved_field_ids() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("pos-delete-bad-id.parquet");
        let path = path.to_str().unwrap();

        let base_schema = crate::arrow::delete_filter::tests::create_pos_del_schema();
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("file_path", DataType::Utf8, false),
            base_schema.field(1).clone(),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(StringArray::from(vec!["data.parquet"])),
            Arc::new(Int64Array::from(vec![1i64])),
        ])
        .unwrap();
        write_plain_parquet(path, &batch);

        let task = position_delete_task(path, Some(1), Some("data.parquet"));
        let err = PositionDeleteIndexLoader::new(FileIO::new_with_fs())
            .load_file_scoped_positions(&task, "data.parquet")
            .await
            .err()
            .expect("expected position-delete loader error");

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("reserved field id"));
    }

    #[tokio::test]
    async fn test_position_delete_index_loader_accepts_empty_valid_file() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("empty-pos-delete.parquet");
        let path = path.to_str().unwrap();
        let batch = position_delete_batch(Vec::new(), Vec::new());
        write_plain_parquet(path, &batch);

        let task = position_delete_task(path, Some(0), Some("data.parquet"));
        let index = PositionDeleteIndexLoader::new(FileIO::new_with_fs())
            .load_file_scoped_positions(&task, "data.parquet")
            .await
            .unwrap();

        assert_eq!(index.iter().count(), 0);
    }

    #[tokio::test]
    async fn test_position_delete_index_loader_validates_empty_file_schema() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("empty-pos-delete-bad-id.parquet");
        let path = path.to_str().unwrap();

        // Empty files must still validate their physical schema.
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("file_path", DataType::Utf8, false),
            Field::new("pos", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(StringArray::from(Vec::<String>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
        ])
        .unwrap();
        write_plain_parquet(path, &batch);

        let task = position_delete_task(path, Some(0), Some("data.parquet"));
        let err = PositionDeleteIndexLoader::new(FileIO::new_with_fs())
            .load_file_scoped_positions(&task, "data.parquet")
            .await
            .err()
            .expect("expected position-delete loader error");

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("reserved field id"));
    }

    #[tokio::test]
    async fn test_position_delete_index_loader_rejects_manifest_target_mismatch() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("pos-delete.parquet");
        let path = path.to_str().unwrap();
        write_plain_parquet(path, &position_delete_batch(vec!["data.parquet"], vec![1]));

        let task = position_delete_task(path, Some(1), Some("other.parquet"));
        let err = PositionDeleteIndexLoader::new(FileIO::new_with_fs())
            .load_file_scoped_positions(&task, "data.parquet")
            .await
            .err()
            .expect("expected position-delete loader error");

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("references other.parquet"));
    }

    #[tokio::test]
    async fn test_position_delete_index_loader_rejects_row_target_mismatch() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("pos-delete.parquet");
        let path = path.to_str().unwrap();
        write_plain_parquet(path, &position_delete_batch(vec!["other.parquet"], vec![1]));

        let task = position_delete_task(path, Some(1), None);
        let err = PositionDeleteIndexLoader::new(FileIO::new_with_fs())
            .load_file_scoped_positions(&task, "data.parquet")
            .await
            .err()
            .expect("expected position-delete loader error");

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("contains target other.parquet"));
    }

    #[tokio::test]
    async fn test_position_delete_index_loader_rejects_negative_position() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("pos-delete.parquet");
        let path = path.to_str().unwrap();
        write_plain_parquet(path, &position_delete_batch(vec!["data.parquet"], vec![-1]));

        let task = position_delete_task(path, Some(1), Some("data.parquet"));
        let err = PositionDeleteIndexLoader::new(FileIO::new_with_fs())
            .load_file_scoped_positions(&task, "data.parquet")
            .await
            .err()
            .expect("expected position-delete loader error");

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("negative row position"));
    }

    #[tokio::test]
    async fn test_position_delete_index_loader_validates_record_count() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("pos-delete.parquet");
        let path = path.to_str().unwrap();
        write_plain_parquet(
            path,
            &position_delete_batch(vec!["data.parquet", "data.parquet"], vec![1, 2]),
        );

        let task = position_delete_task(path, Some(3), Some("data.parquet"));
        let err = PositionDeleteIndexLoader::new(FileIO::new_with_fs())
            .load_file_scoped_positions(&task, "data.parquet")
            .await
            .err()
            .expect("expected position-delete loader error");

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("expected 3 from record_count"));
    }

    #[tokio::test]
    async fn test_position_delete_index_loader_rejects_dv_coordinates() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("pos-delete.parquet");
        let path = path.to_str().unwrap();
        write_plain_parquet(path, &position_delete_batch(vec!["data.parquet"], vec![1]));

        let mut task = position_delete_task(path, Some(1), Some("data.parquet"));
        task.content_offset = Some(0);
        task.content_size_in_bytes = Some(10);

        let err = PositionDeleteIndexLoader::new(FileIO::new_with_fs())
            .load_file_scoped_positions(&task, "data.parquet")
            .await
            .err()
            .expect("expected position-delete loader error");

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message()
                .contains("deletion-vector content coordinates")
        );
    }

    #[tokio::test]
    async fn test_basic_delete_file_loader_read_delete_file() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path();
        let file_io = FileIO::new_with_fs();

        let scan_metrics = ScanMetrics::new();
        let delete_file_loader = BasicDeleteFileLoader::new(file_io.clone(), scan_metrics);

        let file_scan_tasks = setup(table_location);

        let result = delete_file_loader
            .read_delete_file(
                &file_scan_tasks[0].deletes()[0],
                file_scan_tasks[0].schema_ref(),
            )
            .await
            .unwrap();

        let result = result.try_collect::<Vec<_>>().await.unwrap();

        assert_eq!(result.len(), 1);
    }

    #[tokio::test]
    async fn test_read_encrypted_positional_delete_file() {
        use std::sync::Arc;

        use arrow_array::{Int64Array, RecordBatch, StringArray};

        use crate::arrow::delete_filter::tests::create_pos_del_schema;
        use crate::encryption::StandardKeyMetadata;
        use crate::scan::FileScanTaskDeleteFile;
        use crate::spec::{DataContentType, DataFileFormat};

        let encryption_key = b"0123456789abcdef";
        let aad_prefix = b"aad_prefix";

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap();
        let file_io = FileIO::new_with_fs();

        let positional_delete_schema = create_pos_del_schema();
        let file_path_col = Arc::new(StringArray::from_iter_values(vec!["data.parquet"; 4]));
        let pos_col = Arc::new(Int64Array::from(vec![0i64, 1, 5, 10]));
        let batch = RecordBatch::try_new(positional_delete_schema.clone(), vec![
            file_path_col,
            pos_col,
        ])
        .unwrap();

        let del_path = format!("{table_location}/encrypted-pos-del.parquet");
        write_encrypted_parquet(&del_path, &batch, encryption_key, Some(aad_prefix));

        let key_metadata = StandardKeyMetadata::try_new(encryption_key)
            .unwrap()
            .with_aad_prefix(aad_prefix)
            .encode()
            .unwrap();

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    crate::spec::NestedField::required(
                        2147483546,
                        "file_path",
                        crate::spec::Type::Primitive(crate::spec::PrimitiveType::String),
                    )
                    .into(),
                    crate::spec::NestedField::required(
                        2147483545,
                        "pos",
                        crate::spec::Type::Primitive(crate::spec::PrimitiveType::Long),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        let task = FileScanTaskDeleteFile {
            file_path: del_path.clone(),
            file_size_in_bytes: std::fs::metadata(&del_path).unwrap().len(),
            file_type: DataContentType::PositionDeletes,
            file_format: DataFileFormat::Parquet,
            partition_spec_id: 0,
            equality_ids: None,
            key_metadata: Some(Box::from(key_metadata.as_ref())),
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
            record_count: None,
        };

        let scan_metrics = ScanMetrics::new();
        let delete_file_loader = BasicDeleteFileLoader::new(file_io.clone(), scan_metrics);

        let result = delete_file_loader
            .read_delete_file(&task, schema)
            .await
            .unwrap();

        let batches: Vec<_> = result.try_collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 4);

        let index = PositionDeleteIndexLoader::new(file_io)
            .load_file_scoped_positions(&task, "data.parquet")
            .await
            .unwrap();
        assert_eq!(index.iter().collect::<Vec<_>>(), vec![0, 1, 5, 10]);
    }

    #[tokio::test]
    async fn test_read_encrypted_equality_delete_file() {
        use std::collections::HashMap;
        use std::sync::Arc;

        use arrow_array::{Int64Array, RecordBatch};
        use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

        use crate::encryption::StandardKeyMetadata;
        use crate::scan::FileScanTaskDeleteFile;
        use crate::spec::{DataContentType, DataFileFormat};

        let encryption_key = b"0123456789abcdef";
        let aad_prefix = b"my-table-uuid!!";

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap();
        let file_io = FileIO::new_with_fs();

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
        ]));

        let id_col = Arc::new(Int64Array::from(vec![100i64, 200, 300]));
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![id_col]).unwrap();

        let del_path = format!("{table_location}/encrypted-eq-del.parquet");
        write_encrypted_parquet(&del_path, &batch, encryption_key, Some(aad_prefix));

        let key_metadata = StandardKeyMetadata::try_new(encryption_key)
            .unwrap()
            .with_aad_prefix(aad_prefix)
            .encode()
            .unwrap();

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    crate::spec::NestedField::required(
                        1,
                        "id",
                        crate::spec::Type::Primitive(crate::spec::PrimitiveType::Long),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        let task = FileScanTaskDeleteFile {
            file_path: del_path.clone(),
            file_size_in_bytes: std::fs::metadata(&del_path).unwrap().len(),
            file_type: DataContentType::EqualityDeletes,
            file_format: DataFileFormat::Parquet,
            partition_spec_id: 0,
            equality_ids: Some(vec![1]),
            key_metadata: Some(Box::from(key_metadata.as_ref())),
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
            record_count: None,
        };

        let scan_metrics = ScanMetrics::new();
        let delete_file_loader = BasicDeleteFileLoader::new(file_io, scan_metrics);

        let result = delete_file_loader
            .read_delete_file(&task, schema)
            .await
            .unwrap();

        let batches: Vec<_> = result.try_collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }
}
