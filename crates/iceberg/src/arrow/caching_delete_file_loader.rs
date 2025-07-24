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

use std::collections::HashMap;

use arrow_array::{
    BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, StringArray,
    TimestampMicrosecondArray,
};
use arrow_schema;
use futures::{StreamExt, TryStreamExt};
use tokio::sync::oneshot::{Receiver, channel};

use super::delete_filter::DeleteFilter;
use crate::arrow::delete_file_loader::BasicDeleteFileLoader;
use crate::delete_vector::DeleteVector;
use crate::expr::Predicate;
use crate::io::FileIO;
use crate::scan::{ArrowRecordBatchStream, FileScanTaskDeleteFile};
use crate::spec::{DataContentType, SchemaRef};
use crate::{Error, ErrorKind, Result};

#[derive(Clone, Debug)]
pub(crate) struct CachingDeleteFileLoader {
    basic_delete_file_loader: BasicDeleteFileLoader,
    concurrency_limit_data_files: usize,
}

// Intermediate context during processing of a delete file task.
enum DeleteFileContext {
    DelVecs(HashMap<String, DeleteVector>),
    ExistingEqDel,
    PosDels(ArrowRecordBatchStream),
    FreshEqDel {
        batch_stream: ArrowRecordBatchStream,
        sender: tokio::sync::oneshot::Sender<Predicate>,
    },
}

// Final result of the processing of a delete file task before
// results are fully merged into the DeleteFileManager's state
enum ParsedDeleteFileContext {
    DelVecs(HashMap<String, DeleteVector>),
    EqDel,
}

#[allow(unused_variables)]
impl CachingDeleteFileLoader {
    pub(crate) fn new(file_io: FileIO, concurrency_limit_data_files: usize) -> Self {
        CachingDeleteFileLoader {
            basic_delete_file_loader: BasicDeleteFileLoader::new(file_io),
            concurrency_limit_data_files,
        }
    }

    /// Initiates loading of all deletes for all the specified tasks
    ///
    /// Returned future completes once all positional deletes and delete vectors
    /// have loaded. EQ deletes are not waited for in this method but the returned
    /// DeleteFilter will await their loading when queried for them.
    ///
    ///  * Create a single stream of all delete file tasks irrespective of type,
    ///    so that we can respect the combined concurrency limit
    ///  * We then process each in two phases: load and parse.
    ///  * for positional deletes the load phase instantiates an ArrowRecordBatchStream to
    ///    stream the file contents out
    ///  * for eq deletes, we first check if the EQ delete is already loaded or being loaded by
    ///    another concurrently processing data file scan task. If it is, we skip it.
    ///    If not, the DeleteFilter is updated to contain a notifier to prevent other data file
    ///    tasks from starting to load the same equality delete file. We spawn a task to load
    ///    the EQ delete's record batch stream, convert it to a predicate, update the delete filter,
    ///    and notify any task that was waiting for it.
    ///  * When this gets updated to add support for delete vectors, the load phase will return
    ///    a PuffinReader for them.
    ///  * The parse phase parses each record batch stream according to its associated data type.
    ///    The result of this is a map of data file paths to delete vectors for the positional
    ///    delete tasks (and in future for the delete vector tasks). For equality delete
    ///    file tasks, this results in an unbound Predicate.
    ///  * The unbound Predicates resulting from equality deletes are sent to their associated oneshot
    ///    channel to store them in the right place in the delete file managers state.
    ///  * The results of all of these futures are awaited on in parallel with the specified
    ///    level of concurrency and collected into a vec. We then combine all the delete
    ///    vector maps that resulted from any positional delete or delete vector files into a
    ///    single map and persist it in the state.
    ///
    ///
    ///  Conceptually, the data flow is like this:
    /// ```none
    ///                                          FileScanTaskDeleteFile
    ///                                                     |
    ///                                             Skip Started EQ Deletes
    ///                                                     |
    ///                                                     |
    ///                                       [load recordbatch stream / puffin]
    ///                                             DeleteFileContext
    ///                                                     |
    ///                                                     |
    ///                       +-----------------------------+--------------------------+
    ///                     Pos Del           Del Vec (Not yet Implemented)         EQ Del
    ///                       |                             |                          |
    ///              [parse pos del stream]         [parse del vec puffin]       [parse eq del]
    ///          HashMap<String, RoaringTreeMap> HashMap<String, RoaringTreeMap>   (Predicate, Sender)
    ///                       |                             |                          |
    ///                       |                             |                 [persist to state]
    ///                       |                             |                          ()
    ///                       |                             |                          |
    ///                       +-----------------------------+--------------------------+
    ///                                                     |
    ///                                             [buffer unordered]
    ///                                                     |
    ///                                            [combine del vectors]
    ///                                        HashMap<String, RoaringTreeMap>
    ///                                                     |
    ///                                        [persist del vectors to state]
    ///                                                    ()
    ///                                                    |
    ///                                                    |
    ///                                                 [join!]
    /// ```
    pub(crate) fn load_deletes(
        &self,
        delete_file_entries: &[FileScanTaskDeleteFile],
        schema: SchemaRef,
    ) -> Receiver<Result<DeleteFilter>> {
        let (tx, rx) = channel();
        let del_filter = DeleteFilter::default();

        let stream_items = delete_file_entries
            .iter()
            .map(|t| {
                (
                    t.clone(),
                    self.basic_delete_file_loader.clone(),
                    del_filter.clone(),
                    schema.clone(),
                )
            })
            .collect::<Vec<_>>();
        let task_stream = futures::stream::iter(stream_items);

        let del_filter = del_filter.clone();
        let concurrency_limit_data_files = self.concurrency_limit_data_files;
        let basic_delete_file_loader = self.basic_delete_file_loader.clone();
        crate::runtime::spawn(async move {
            let result = async move {
                let mut del_filter = del_filter;
                let basic_delete_file_loader = basic_delete_file_loader.clone();

                let results: Vec<ParsedDeleteFileContext> = task_stream
                    .map(move |(task, file_io, del_filter, schema)| {
                        let basic_delete_file_loader = basic_delete_file_loader.clone();
                        async move {
                            Self::load_file_for_task(
                                &task,
                                basic_delete_file_loader.clone(),
                                del_filter,
                                schema,
                            )
                            .await
                        }
                    })
                    .map(move |ctx| {
                        Ok(async { Self::parse_file_content_for_task(ctx.await?).await })
                    })
                    .try_buffer_unordered(concurrency_limit_data_files)
                    .try_collect::<Vec<_>>()
                    .await?;

                for item in results {
                    if let ParsedDeleteFileContext::DelVecs(hash_map) = item {
                        for (data_file_path, delete_vector) in hash_map.into_iter() {
                            del_filter.upsert_delete_vector(data_file_path, delete_vector);
                        }
                    }
                }

                Ok(del_filter)
            }
            .await;

            let _ = tx.send(result);
        });

        rx
    }

    async fn load_file_for_task(
        task: &FileScanTaskDeleteFile,
        basic_delete_file_loader: BasicDeleteFileLoader,
        del_filter: DeleteFilter,
        schema: SchemaRef,
    ) -> Result<DeleteFileContext> {
        // Check if the file is a Puffin file (by extension or by trying to read it as such)
        if Self::is_puffin_file(&task.file_path) {
            return Self::load_puffin_delete_vectors(&task.file_path, &basic_delete_file_loader).await;
        }

        match task.file_type {
            DataContentType::PositionDeletes => Ok(DeleteFileContext::PosDels(
                basic_delete_file_loader
                    .parquet_to_batch_stream(&task.file_path)
                    .await?,
            )),

            DataContentType::EqualityDeletes => {
                let Some(notify) = del_filter.try_start_eq_del_load(&task.file_path) else {
                    return Ok(DeleteFileContext::ExistingEqDel);
                };

                let (sender, receiver) = channel();
                del_filter.insert_equality_delete(&task.file_path, receiver);

                Ok(DeleteFileContext::FreshEqDel {
                    batch_stream: BasicDeleteFileLoader::evolve_schema(
                        basic_delete_file_loader
                            .parquet_to_batch_stream(&task.file_path)
                            .await?,
                        schema,
                    )
                    .await?,
                    sender,
                })
            }

            DataContentType::Data => Err(Error::new(
                ErrorKind::Unexpected,
                "tasks with files of type Data not expected here",
            )),
        }
    }

    async fn parse_file_content_for_task(
        ctx: DeleteFileContext,
    ) -> Result<ParsedDeleteFileContext> {
        match ctx {
            DeleteFileContext::DelVecs(hash_map) => Ok(ParsedDeleteFileContext::DelVecs(hash_map)),
            DeleteFileContext::ExistingEqDel => Ok(ParsedDeleteFileContext::EqDel),
            DeleteFileContext::PosDels(batch_stream) => {
                let del_vecs =
                    Self::parse_positional_deletes_record_batch_stream(batch_stream).await?;
                Ok(ParsedDeleteFileContext::DelVecs(del_vecs))
            }
            DeleteFileContext::FreshEqDel {
                sender,
                batch_stream,
            } => {
                let predicate =
                    Self::parse_equality_deletes_record_batch_stream(batch_stream).await?;

                sender
                    .send(predicate)
                    .map_err(|err| {
                        Error::new(
                            ErrorKind::Unexpected,
                            "Could not send eq delete predicate to state",
                        )
                    })
                    .map(|_| ParsedDeleteFileContext::EqDel)
            }
        }
    }

    /// Parses a record batch stream coming from positional delete files
    ///
    /// Returns a map of data file path to a delete vector
    async fn parse_positional_deletes_record_batch_stream(
        mut stream: ArrowRecordBatchStream,
    ) -> Result<HashMap<String, DeleteVector>> {
        let mut result: HashMap<String, DeleteVector> = HashMap::default();

        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let schema = batch.schema();
            let columns = batch.columns();

            let Some(file_paths) = columns[0].as_any().downcast_ref::<StringArray>() else {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Could not downcast file paths array to StringArray",
                ));
            };
            let Some(positions) = columns[1].as_any().downcast_ref::<Int64Array>() else {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Could not downcast positions array to Int64Array",
                ));
            };

            for (file_path, pos) in file_paths.iter().zip(positions.iter()) {
                let (Some(file_path), Some(pos)) = (file_path, pos) else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "null values in delete file",
                    ));
                };

                result
                    .entry(file_path.to_string())
                    .or_default()
                    .insert(pos as u64);
            }
        }

        Ok(result)
    }

    /// Parses record batch streams from individual equality delete files
    ///
    /// Returns an unbound Predicate for each batch stream
    /// 
    /// Equality delete files contain rows where each row represents values that should be deleted.
    /// For example, if the equality IDs are [1, 2] representing columns "name" and "age",
    /// and the file contains rows [("Alice", 25), ("Bob", 30)], then any data rows matching
    /// (name="Alice" AND age=25) OR (name="Bob" AND age=30) should be deleted.
    async fn parse_equality_deletes_record_batch_stream(
        mut stream: ArrowRecordBatchStream,
    ) -> Result<Predicate> {
        use crate::expr::Predicate::*;
        use crate::expr::{Reference, Literal as ExprLiteral};
        use crate::spec::{Literal, PrimitiveLiteral};
        use arrow_array::Array;

        let mut combined_predicates = Vec::new();

        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let schema = batch.schema();
            
            // Process each row in the batch
            for row_idx in 0..batch.num_rows() {
                let mut row_conditions = Vec::new();
                
                // For each column in the equality delete file, create an equality condition
                for col_idx in 0..batch.num_columns() {
                    let column = batch.column(col_idx);
                    let field = schema.field(col_idx);
                    
                    // Extract the field ID from metadata
                    let field_id = field
                        .metadata()
                        .get("parquet_field_id")
                        .or_else(|| field.metadata().get("PARQUET:field_id"))
                        .ok_or_else(|| {
                            Error::new(
                                ErrorKind::DataInvalid,
                                format!("Missing field ID for column '{}'", field.name()),
                            )
                        })?
                        .parse::<i32>()
                        .map_err(|_| {
                            Error::new(
                                ErrorKind::DataInvalid,
                                "Invalid field ID format",
                            )
                        })?;

                    // Skip if the value is null
                    if column.is_null(row_idx) {
                        continue;
                    }

                    // Convert Arrow value to Iceberg Literal based on data type
                    let literal = match field.data_type() {
                        arrow_schema::DataType::Boolean => {
                            let array = column.as_any().downcast_ref::<arrow_array::BooleanArray>()
                                .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "Expected BooleanArray"))?;
                            Literal::bool(array.value(row_idx))
                        },
                        arrow_schema::DataType::Int32 => {
                            let array = column.as_any().downcast_ref::<arrow_array::Int32Array>()
                                .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "Expected Int32Array"))?;
                            Literal::int(array.value(row_idx))
                        },
                        arrow_schema::DataType::Int64 => {
                            let array = column.as_any().downcast_ref::<arrow_array::Int64Array>()
                                .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "Expected Int64Array"))?;
                            Literal::long(array.value(row_idx))
                        },
                        arrow_schema::DataType::Float32 => {
                            let array = column.as_any().downcast_ref::<arrow_array::Float32Array>()
                                .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "Expected Float32Array"))?;
                            Literal::float(array.value(row_idx))
                        },
                        arrow_schema::DataType::Float64 => {
                            let array = column.as_any().downcast_ref::<arrow_array::Float64Array>()
                                .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "Expected Float64Array"))?;
                            Literal::double(array.value(row_idx))
                        },
                        arrow_schema::DataType::Utf8 => {
                            let array = column.as_any().downcast_ref::<arrow_array::StringArray>()
                                .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "Expected StringArray"))?;
                            Literal::string(array.value(row_idx))
                        },
                        arrow_schema::DataType::LargeUtf8 => {
                            let array = column.as_any().downcast_ref::<arrow_array::LargeStringArray>()
                                .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "Expected LargeStringArray"))?;
                            Literal::string(array.value(row_idx))
                        },
                        arrow_schema::DataType::Binary => {
                            let array = column.as_any().downcast_ref::<arrow_array::BinaryArray>()
                                .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "Expected BinaryArray"))?;
                            Literal::binary(array.value(row_idx).to_vec())
                        },
                        arrow_schema::DataType::LargeBinary => {
                            let array = column.as_any().downcast_ref::<arrow_array::LargeBinaryArray>()
                                .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "Expected LargeBinaryArray"))?;
                            Literal::binary(array.value(row_idx).to_vec())
                        },
                        arrow_schema::DataType::Date32 => {
                            let array = column.as_any().downcast_ref::<arrow_array::Date32Array>()
                                .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "Expected Date32Array"))?;
                            Literal::date(array.value(row_idx))
                        },
                        arrow_schema::DataType::Timestamp(_, _) => {
                            let array = column.as_any().downcast_ref::<arrow_array::TimestampMicrosecondArray>()
                                .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "Expected TimestampMicrosecondArray"))?;
                            Literal::timestamp_micros(array.value(row_idx))
                        },
                        arrow_schema::DataType::Decimal128(precision, scale) => {
                            let array = column.as_any().downcast_ref::<arrow_array::Decimal128Array>()
                                .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "Expected Decimal128Array"))?;
                            Literal::decimal_from_i128(array.value(row_idx), *precision as u32, *scale as u32)?
                        },
                        _ => {
                            return Err(Error::new(
                                ErrorKind::FeatureUnsupported,
                                format!("Unsupported data type for equality delete: {:?}", field.data_type()),
                            ));
                        }
                    };

                    // Create equality condition: field_id = literal
                    let condition = Equal {
                        term: Box::new(Reference::new(field.name().to_string())),
                        literal: ExprLiteral::new(literal),
                    };
                    
                    row_conditions.push(condition);
                }
                
                // If we have conditions for this row, combine them with AND
                if !row_conditions.is_empty() {
                    let row_predicate = row_conditions.into_iter().reduce(|acc, condition| And {
                        left: Box::new(acc),
                        right: Box::new(condition),
                    }).unwrap();
                    
                    combined_predicates.push(row_predicate);
                }
            }
        }

        // Combine all row predicates with OR (any matching row should be deleted)
        if combined_predicates.is_empty() {
            Ok(AlwaysFalse) // No rows to delete
        } else {
            let final_predicate = combined_predicates.into_iter().reduce(|acc, predicate| Or {
                left: Box::new(acc),
                right: Box::new(predicate),
            }).unwrap();
            
            Ok(final_predicate)
        }
    }

    /// Check if a file is a Puffin file based on file extension or magic bytes
    fn is_puffin_file(file_path: &str) -> bool {
        file_path.ends_with(".puffin") || file_path.ends_with(".bin")
    }

    /// Load Delete Vectors from a Puffin file
    async fn load_puffin_delete_vectors(
        file_path: &str,
        basic_delete_file_loader: &BasicDeleteFileLoader,
    ) -> Result<DeleteFileContext> {
        use crate::puffin::{PuffinReader, DELETION_VECTOR_V1};

        let input_file = basic_delete_file_loader.file_io().new_input(file_path)?;
        let puffin_reader = PuffinReader::new(input_file);
        let file_metadata = puffin_reader.file_metadata().await?;

        let mut delete_vectors = HashMap::new();

        // Process each blob in the Puffin file
        for blob_metadata in file_metadata.blobs() {
            if blob_metadata.blob_type() == DELETION_VECTOR_V1 {
                let blob = puffin_reader.blob(blob_metadata).await?;
                let delete_vector = Self::parse_delete_vector_blob(&blob)?;
                
                // For now, we'll assume the delete vector applies to all files
                // In a real implementation, we would need to determine which data files
                // this delete vector applies to based on the blob metadata properties
                if let Some(data_file_path) = blob.properties().get("data-file-path") {
                    delete_vectors.insert(data_file_path.clone(), delete_vector);
                }
            }
        }

        Ok(DeleteFileContext::DelVecs(delete_vectors))
    }

    /// Parse a deletion vector blob from Puffin format into a DeleteVector
    fn parse_delete_vector_blob(blob: &crate::puffin::Blob) -> Result<DeleteVector> {
        use roaring::RoaringTreemap;
        
        // According to the Iceberg spec, deletion vectors are stored as RoaringBitmap
        // in the "portable" format for 64-bit implementations
        let data = blob.data();
        
        // Parse the RoaringTreemap from the blob data
        let roaring_treemap = RoaringTreemap::deserialize_from(std::io::Cursor::new(data))
            .map_err(|e| Error::new(
                ErrorKind::DataInvalid,
                format!("Failed to deserialize deletion vector: {}", e),
            ))?;

        Ok(DeleteVector::new(roaring_treemap))
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::arrow::delete_filter::tests::setup;

    #[tokio::test]
    async fn test_caching_delete_file_loader_load_deletes() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path();
        let file_io = FileIO::from_path(table_location.as_os_str().to_str().unwrap())
            .unwrap()
            .build()
            .unwrap();

        let delete_file_loader = CachingDeleteFileLoader::new(file_io.clone(), 10);

        let file_scan_tasks = setup(table_location);

        let delete_filter = delete_file_loader
            .load_deletes(&file_scan_tasks[0].deletes, file_scan_tasks[0].schema_ref())
            .await
            .unwrap()
            .unwrap();

        let result = delete_filter
            .get_delete_vector(&file_scan_tasks[0])
            .unwrap();

        // union of pos dels from pos del file 1 and 2, ie
        // [0, 1, 3, 5, 6, 8, 1022, 1023] | [0, 1, 3, 5, 20, 21, 22, 23]
        // = [0, 1, 3, 5, 6, 8, 20, 21, 22, 23, 1022, 1023]
        assert_eq!(result.lock().unwrap().len(), 12);

        let result = delete_filter.get_delete_vector(&file_scan_tasks[1]);
        assert!(result.is_none()); // no pos dels for file 3
    }
}
