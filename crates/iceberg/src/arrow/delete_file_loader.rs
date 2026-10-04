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

use futures::{StreamExt, TryStreamExt};
use parquet::arrow::ParquetRecordBatchStreamBuilder;

use crate::arrow::ArrowReader;
use crate::arrow::reader::ParquetReadOptions;
use crate::arrow::record_batch_transformer::RecordBatchTransformerBuilder;
use crate::arrow::scan_metrics::ScanMetrics;
use crate::io::FileIO;
use crate::scan::{ArrowRecordBatchStream, FileScanTaskDeleteFile};
use crate::spec::{Schema, SchemaRef};
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
        // A size of 0 means the task did not carry one (see
        // `FileScanTaskDeleteFile`). Resolve it when the file is first loaded.
        let file_size_in_bytes = if file_size_in_bytes == 0 {
            self.file_io
                .new_input(data_file_path)?
                .metadata()
                .await
                .map_err(|error| {
                    // Keep the storage error's kind and retryability.
                    Error::new(
                        error.kind(),
                        format!("Failed to stat delete file '{data_file_path}'"),
                    )
                    .with_retryable(error.retryable())
                    .with_source(error)
                })?
                .size
        } else {
            file_size_in_bytes
        };
        // Reading a Parquet footer needs at least its last eight bytes. Reject
        // a smaller size rather than read the file as if it had no deletes.
        if file_size_in_bytes < 8 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Delete file '{data_file_path}' has a size of {file_size_in_bytes} bytes, \
                     below the Parquet footer minimum"
                ),
            ));
        }
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

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::arrow::delete_filter::tests::setup;
    use crate::arrow::test_utils::write_encrypted_parquet;

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

        let scan_metrics = ScanMetrics::new();
        let delete_file_loader = BasicDeleteFileLoader::new(file_io, scan_metrics);

        // A known size, and an unknown size (0) resolved before decryption.
        for file_size_in_bytes in [std::fs::metadata(&del_path).unwrap().len(), 0] {
            let task = FileScanTaskDeleteFile {
                file_path: del_path.clone(),
                file_size_in_bytes,
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

            let result = delete_file_loader
                .read_delete_file(&task, Arc::clone(&schema))
                .await
                .unwrap();

            let batches: Vec<_> = result.try_collect().await.unwrap();
            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].num_rows(), 4);
        }
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

        let arrow_schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false).with_metadata(
                HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string())]),
            ),
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

    #[tokio::test]
    async fn unknown_delete_size_still_reports_missing_and_empty_objects() {
        let temp = TempDir::new().unwrap();
        let loader = BasicDeleteFileLoader::new(FileIO::new_with_fs(), ScanMetrics::new());
        let missing = temp.path().join("missing.parquet");
        let error = loader
            .parquet_to_batch_stream(missing.to_str().unwrap(), 0, None)
            .await
            .err()
            .unwrap();
        assert!(error.to_string().contains("Failed to stat delete file"));
        // The storage error's kind and retryability are kept exactly.
        let storage_error = FileIO::new_with_fs()
            .new_input(missing.to_str().unwrap())
            .unwrap()
            .metadata()
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), storage_error.kind());
        assert_eq!(error.retryable(), storage_error.retryable());

        // A size below the Parquet footer minimum is rejected whether it was
        // resolved from storage or supplied by the task.
        let empty = temp.path().join("empty.parquet");
        std::fs::write(&empty, []).unwrap();
        for size in [0, 4] {
            let error = loader
                .parquet_to_batch_stream(empty.to_str().unwrap(), size, None)
                .await
                .err()
                .unwrap();
            assert_eq!(error.kind(), ErrorKind::DataInvalid);
            assert!(error.to_string().contains("footer minimum"), "{error}");
        }

        // A real file with an unknown size is sized lazily and read in full.
        let present = temp.path().join("present.parquet");
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "pos",
            arrow_schema::DataType::Int64,
            false,
        )]));
        let batch = arrow_array::RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(
            arrow_array::Int64Array::from(vec![1, 2, 3]),
        )])
        .unwrap();
        let mut writer = parquet::arrow::ArrowWriter::try_new(
            std::fs::File::create(&present).unwrap(),
            schema,
            None,
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let batches: Vec<_> = loader
            .parquet_to_batch_stream(present.to_str().unwrap(), 0, None)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
    }
}
