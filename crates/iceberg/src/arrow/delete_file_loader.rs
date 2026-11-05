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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions, StructArray};
use arrow_schema::{FieldRef, Fields, Schema as ArrowSchema};
use futures::{StreamExt, TryStreamExt};
use parquet::arrow::{PARQUET_FIELD_ID_META_KEY, ParquetRecordBatchStreamBuilder};

use crate::arrow::ArrowReader;
use crate::arrow::reader::ParquetReadOptions;
use crate::arrow::record_batch_transformer::RecordBatchTransformerBuilder;
use crate::arrow::scan_metrics::ScanMetrics;
use crate::arrow::schema::try_get_field_id_from_metadata;
use crate::io::FileIO;
use crate::scan::{ArrowRecordBatchStream, FileScanTaskDeleteFile};
use crate::spec::{Schema, SchemaRef, prune_columns};
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

/// Reads an equality delete file as `schema` pruned to its equality fields, the way Java's
/// `DeleteFilter` selects them, so a field nested in a struct keeps its parent struct.
///
/// Java writes a nested equality field under its parent structs, while `EqualityDeleteWriter`
/// writes it as a top-level column. Such a column is put back under its parents before schema
/// evolution, so both layouts read the same way.
pub(crate) fn evolve_equality_delete_stream(
    record_batch_stream: ArrowRecordBatchStream,
    schema: &Schema,
    equality_ids: &[i32],
) -> Result<ArrowRecordBatchStream> {
    if let Some(id) = equality_ids
        .iter()
        .find(|&&id| schema.field_by_id(id).is_none())
    {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!("Equality delete field id {id} is not in the scan schema"),
        ));
    }
    let crate::spec::Type::Struct(pruned) =
        prune_columns(schema, equality_ids.iter().copied(), false)?
    else {
        return Err(Error::new(
            ErrorKind::Unexpected,
            "pruning a schema did not return a struct",
        ));
    };
    let projection: Vec<i32> = pruned.fields().iter().map(|field| field.id).collect();
    let pruned = Arc::new(
        Schema::builder()
            .with_schema_id(schema.schema_id())
            .with_fields(pruned.fields().to_vec())
            .build()?,
    );
    let mut transformer = RecordBatchTransformerBuilder::new(pruned.clone(), &projection).build();
    Ok(Box::pin(record_batch_stream.map(move |batch| {
        batch
            .and_then(|batch| nest_top_level_equality_columns(batch, &pruned))
            .and_then(|batch| transformer.process_record_batch(batch))
    })) as ArrowRecordBatchStream)
}

/// Moves top-level columns whose field ids belong to fields nested in `schema` back under their
/// parent structs. A batch with no such column is returned as is.
fn nest_top_level_equality_columns(batch: RecordBatch, schema: &Schema) -> Result<RecordBatch> {
    let top_level_ids: HashSet<i32> = schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| field.id)
        .collect();
    let mut flat = HashMap::new();
    for (index, field) in batch.schema().fields().iter().enumerate() {
        if let Some(id) = try_get_field_id_from_metadata(field)?
            && !top_level_ids.contains(&id)
            && schema.field_by_id(id).is_some()
        {
            flat.insert(id, index);
        }
    }
    if flat.is_empty() {
        return Ok(batch);
    }

    fn nest(
        field: &crate::spec::NestedField,
        batch: &RecordBatch,
        flat: &HashMap<i32, usize>,
    ) -> Option<(FieldRef, ArrayRef)> {
        if let Some(&index) = flat.get(&field.id) {
            return Some((
                batch.schema().field(index).clone().into(),
                batch.column(index).clone(),
            ));
        }
        let crate::spec::Type::Struct(struct_type) = field.field_type.as_ref() else {
            return None;
        };
        let (fields, columns): (Vec<FieldRef>, Vec<ArrayRef>) = struct_type
            .fields()
            .iter()
            .filter_map(|child| nest(child, batch, flat))
            .unzip();
        if fields.is_empty() {
            return None;
        }
        let fields = Fields::from(fields);
        let array = StructArray::new(fields.clone(), columns, None);
        let arrow_field =
            arrow_schema::Field::new(&field.name, arrow_schema::DataType::Struct(fields), true)
                .with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    field.id.to_string(),
                )]));
        Some((Arc::new(arrow_field), Arc::new(array)))
    }

    let mut fields: Vec<FieldRef> = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();
    for (index, field) in batch.schema().fields().iter().enumerate() {
        if !flat.values().any(|&flat_index| flat_index == index) {
            fields.push(field.clone());
            columns.push(batch.column(index).clone());
        }
    }
    let present: HashSet<i32> = fields
        .iter()
        .map(|field| try_get_field_id_from_metadata(field))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect();
    for field in schema.as_struct().fields() {
        if present.contains(&field.id) {
            continue;
        }
        if let Some((arrow_field, column)) = nest(field, &batch, &flat) {
            fields.push(arrow_field);
            columns.push(column);
        }
    }
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    Ok(RecordBatch::try_new_with_options(
        Arc::new(ArrowSchema::new(fields)),
        columns,
        &options,
    )?)
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
                task.file_path(),
                task.file_size_in_bytes(),
                task.key_metadata(),
            )
            .await?;

        // For equality deletes, only evolve the columns holding the equality_ids.
        // For positional deletes (equality_ids is None), use all field IDs.
        match task.equality_ids() {
            Some(ids) => evolve_equality_delete_stream(raw_batch_stream, &schema, ids),
            None => {
                let field_ids: Vec<i32> = schema.field_id_to_name_map().keys().cloned().collect();
                Self::evolve_schema(raw_batch_stream, schema, &field_ids).await
            }
        }
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

        let task = FileScanTaskDeleteFile::builder()
            .with_file_path(del_path.clone())
            .with_file_size_in_bytes(std::fs::metadata(&del_path).unwrap().len())
            .with_file_type(DataContentType::PositionDeletes)
            .with_file_format(DataFileFormat::Parquet)
            .with_partition_spec_id(0)
            .with_key_metadata(Some(Box::from(key_metadata.as_ref())))
            .build()
            .unwrap();

        let scan_metrics = ScanMetrics::new();
        let delete_file_loader = BasicDeleteFileLoader::new(file_io, scan_metrics);

        let result = delete_file_loader
            .read_delete_file(&task, schema)
            .await
            .unwrap();

        let batches: Vec<_> = result.try_collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 4);
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

        let task = FileScanTaskDeleteFile::builder()
            .with_file_path(del_path.clone())
            .with_file_size_in_bytes(std::fs::metadata(&del_path).unwrap().len())
            .with_file_type(DataContentType::EqualityDeletes)
            .with_file_format(DataFileFormat::Parquet)
            .with_partition_spec_id(0)
            .with_equality_ids(Some(vec![1]))
            .with_key_metadata(Some(Box::from(key_metadata.as_ref())))
            .build()
            .unwrap();

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
