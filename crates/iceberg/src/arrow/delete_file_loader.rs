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
use std::sync::Arc;

use futures::channel::oneshot;
use futures::future::join_all;
use futures::{StreamExt, TryStreamExt};
use tokio::sync::oneshot::{channel, Receiver};

use super::delete_filter::{DeleteFilter, EqDelFuture};
use crate::arrow::record_batch_transformer::RecordBatchTransformer;
use crate::arrow::ArrowReader;
use crate::delete_vector::DeleteVector;
use crate::expr::Predicate;
use crate::io::FileIO;
use crate::scan::{ArrowRecordBatchStream, FileScanTaskDeleteFile};
use crate::spec::{DataContentType, Schema, SchemaRef};
use crate::{Error, ErrorKind, Result};

#[allow(unused)]
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

#[allow(unused)]
#[derive(Clone, Debug)]
pub(crate) struct CachingDeleteFileLoader {
    file_io: FileIO,
    concurrency_limit_data_files: usize,
}

impl DeleteFileLoader for CachingDeleteFileLoader {
    async fn read_delete_file(
        &self,
        task: &FileScanTaskDeleteFile,
        schema: SchemaRef,
    ) -> Result<ArrowRecordBatchStream> {
        let raw_batch_stream =
            CachingDeleteFileLoader::parquet_to_batch_stream(&task.file_path, self.file_io.clone())
                .await?;

        Self::evolve_schema(raw_batch_stream, schema).await
    }
}

// Intermediate context during processing of a delete file task.
enum DeleteFileContext {
    // TODO: Delete Vector loader from Puffin files
    InProgEqDel(EqDelFuture),
    PosDels(ArrowRecordBatchStream),
    FreshEqDel {
        batch_stream: ArrowRecordBatchStream,
        sender: oneshot::Sender<Predicate>,
    },
}

// Final result of the processing of a delete file task before
// results are fully merged into the DeleteFileManager's state
enum ParsedDeleteFileContext {
    InProgEqDel(EqDelFuture),
    DelVecs(HashMap<String, DeleteVector>),
    EqDel,
}

#[allow(unused_variables)]
impl CachingDeleteFileLoader {
    pub(crate) fn new(file_io: FileIO, concurrency_limit_data_files: usize) -> Self {
        CachingDeleteFileLoader {
            file_io,
            concurrency_limit_data_files,
        }
    }

    /// Load the deletes for all the specified tasks
    ///
    /// Returned future completes once all loading has finished.
    ///
    ///  * Create a single stream of all delete file tasks irrespective of type,
    ///      so that we can respect the combined concurrency limit
    ///  * We then process each in two phases: load and parse.
    ///  * for positional deletes the load phase instantiates an ArrowRecordBatchStream to
    ///      stream the file contents out
    ///  * for eq deletes, we first check if the EQ delete is already loaded or being loaded by
    ///      another concurrently processing data file scan task. If it is, we return a future
    ///      for the pre-existing task from the load phase. If not, we create such a future
    ///      and store it in the state to prevent other data file tasks from starting to load
    ///      the same equality delete file, and return a record batch stream from the load phase
    ///      as per the other delete file types - only this time it is accompanied by a one-shot
    ///      channel sender that we will eventually use to resolve the shared future that we stored
    ///      in the state.
    ///  * When this gets updated to add support for delete vectors, the load phase will return
    ///      a PuffinReader for them.
    ///  * The parse phase parses each record batch stream according to its associated data type.
    ///      The result of this is a map of data file paths to delete vectors for the positional
    ///      delete tasks (and in future for the delete vector tasks). For equality delete
    ///      file tasks, this results in an unbound Predicate.
    ///  * The unbound Predicates resulting from equality deletes are sent to their associated oneshot
    ///      channel to store them in the right place in the delete file managers state.
    ///  * The results of all of these futures are awaited on in parallel with the specified
    ///      level of concurrency and collected into a vec. We then combine all the delete
    ///      vector maps that resulted from any positional delete or delete vector files into a
    ///      single map and persist it in the state.
    ///
    ///
    ///  Conceptually, the data flow is like this:
    /// ```none
    ///                                          FileScanTaskDeleteFile
    ///                                                     |
    ///            Already-loading EQ Delete                |             Everything Else
    ///                     +---------------------------------------------------+
    ///                     |                                                   |
    ///            [get existing future]                         [load recordbatch stream / puffin]
    ///         DeleteFileContext::InProgEqDel                           DeleteFileContext
    ///                     |                                                   |
    ///                     |                                                   |
    ///                     |                     +-----------------------------+--------------------------+
    ///                     |                   Pos Del           Del Vec (Not yet Implemented)         EQ Del
    ///                     |                     |                             |                          |
    ///                     |          [parse pos del stream]         [parse del vec puffin]       [parse eq del]
    ///                     |  HashMap<String, RoaringTreeMap> HashMap<String, RoaringTreeMap>   (Predicate, Sender)
    ///                     |                     |                             |                          |
    ///                     |                     |                             |                 [persist to state]
    ///                     |                     |                             |                          ()
    ///                     |                     |                             |                          |
    ///                     |                     +-----------------------------+--------------------------+
    ///                     |                                                   |
    ///                     |                                            [buffer unordered]
    ///                     |                                                   |
    ///                     |                                            [combine del vectors]
    ///                     |                                      HashMap<String, RoaringTreeMap>
    ///                     |                                                   |
    ///                     |                                      [persist del vectors to state]
    ///                     |                                                   ()
    ///                     |                                                   |
    ///                     +-------------------------+-------------------------+
    ///                                               |
    ///                                            [join!]
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
                    self.file_io.clone(),
                    del_filter.clone(),
                    schema.clone(),
                )
            })
            .collect::<Vec<_>>();
        let task_stream = futures::stream::iter(stream_items);
        let del_filter = del_filter.clone();
        let concurrency_limit_data_files = self.concurrency_limit_data_files;
        crate::runtime::spawn(async move {
            let result = async move {
                let mut del_filter = del_filter;

                let results: Vec<ParsedDeleteFileContext> = task_stream
                    .map(move |(task, file_io, del_filter, schema)| async move {
                        Self::load_file_for_task(&task, file_io, del_filter, schema).await
                    })
                    .map(move |ctx| {
                        Ok(async { Self::parse_file_content_for_task(ctx.await?).await })
                    })
                    .try_buffer_unordered(concurrency_limit_data_files)
                    .try_collect::<Vec<_>>()
                    .await?;

                // wait for all in-progress EQ deletes from other tasks
                let _ = join_all(results.iter().filter_map(|i| {
                    if let ParsedDeleteFileContext::InProgEqDel(fut) = i {
                        Some(fut.clone())
                    } else {
                        None
                    }
                }))
                .await;

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
        file_io: FileIO,
        del_filter: DeleteFilter,
        schema: SchemaRef,
    ) -> Result<DeleteFileContext> {
        match task.file_type {
            DataContentType::PositionDeletes => Ok(DeleteFileContext::PosDels(
                Self::parquet_to_batch_stream(&task.file_path, file_io).await?,
            )),

            DataContentType::EqualityDeletes => {
                let sender = {
                    if let Some(existing) = del_filter
                        .get_equality_delete_predicate_for_delete_file_path(&task.file_path)
                    {
                        return Ok(DeleteFileContext::InProgEqDel(existing.clone()));
                    }

                    let (sender, fut) = EqDelFuture::new();

                    del_filter.insert_equality_delete(task.file_path.to_string(), fut);

                    sender
                };

                Ok(DeleteFileContext::FreshEqDel {
                    batch_stream: Self::evolve_schema(
                        Self::parquet_to_batch_stream(&task.file_path, file_io).await?,
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
            DeleteFileContext::InProgEqDel(fut) => Ok(ParsedDeleteFileContext::InProgEqDel(fut)),
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

    /// Loads a RecordBatchStream for a given datafile.
    async fn parquet_to_batch_stream(
        data_file_path: &str,
        file_io: FileIO,
    ) -> Result<ArrowRecordBatchStream> {
        /*
           Essentially a super-cut-down ArrowReader. We can't use ArrowReader directly
           as that introduces a circular dependency.
        */
        let record_batch_stream = ArrowReader::create_parquet_record_batch_stream_builder(
            data_file_path,
            file_io.clone(),
            false,
        )
        .await?
        .build()?
        .map_err(|e| Error::new(ErrorKind::Unexpected, format!("{}", e)));

        Ok(Box::pin(record_batch_stream) as ArrowRecordBatchStream)
    }

    /// Evolves the schema of the RecordBatches from an equality delete file
    async fn evolve_schema(
        record_batch_stream: ArrowRecordBatchStream,
        target_schema: Arc<Schema>,
    ) -> Result<ArrowRecordBatchStream> {
        let eq_ids = target_schema
            .as_ref()
            .field_id_to_name_map()
            .keys()
            .cloned()
            .collect::<Vec<_>>();

        let mut record_batch_transformer =
            RecordBatchTransformer::build(target_schema.clone(), &eq_ids);

        let record_batch_stream = record_batch_stream.map(move |record_batch| {
            record_batch.and_then(|record_batch| {
                record_batch_transformer.process_record_batch(record_batch)
            })
        });

        Ok(Box::pin(record_batch_stream) as ArrowRecordBatchStream)
    }

    /// Parses a record batch stream coming from positional delete files
    ///
    /// Returns a map of data file path to a delete vector
    async fn parse_positional_deletes_record_batch_stream(
        stream: ArrowRecordBatchStream,
    ) -> Result<HashMap<String, DeleteVector>> {
        // TODO

        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "parsing of positional deletes is not yet supported",
        ))
    }

    /// Parses record batch streams from individual equality delete files
    ///
    /// Returns an unbound Predicate for each batch stream
    async fn parse_equality_deletes_record_batch_stream(
        streams: ArrowRecordBatchStream,
    ) -> Result<Predicate> {
        // TODO

        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "parsing of equality deletes is not yet supported",
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::path::Path;
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::Schema as ArrowSchema;
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use super::*;
    use crate::scan::FileScanTask;
    use crate::spec::{DataFileFormat, Schema};

    type ArrowSchemaRef = Arc<ArrowSchema>;

    const FIELD_ID_POSITIONAL_DELETE_FILE_PATH: u64 = 2147483546;
    const FIELD_ID_POSITIONAL_DELETE_POS: u64 = 2147483545;

    #[tokio::test]
    async fn test_delete_file_manager_load_deletes() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path();
        let file_io = FileIO::from_path(table_location.as_os_str().to_str().unwrap())
            .unwrap()
            .build()
            .unwrap();

        // Note that with the delete file parsing not yet in place, all we can test here is that
        // the call to the loader fails with the expected FeatureUnsupportedError.
        let delete_file_manager = CachingDeleteFileLoader::new(file_io.clone(), 10);

        let file_scan_tasks = setup(table_location);

        let result = delete_file_manager
            .load_deletes(&file_scan_tasks[0].deletes, file_scan_tasks[0].schema_ref())
            .await
            .unwrap();

        assert!(result.is_err_and(|e| e.kind() == ErrorKind::FeatureUnsupported));
    }

    fn setup(table_location: &Path) -> Vec<FileScanTask> {
        let data_file_schema = Arc::new(Schema::builder().build().unwrap());
        let positional_delete_schema = create_pos_del_schema();

        let file_path_values = vec![format!("{}/1.parquet", table_location.to_str().unwrap()); 8];
        let pos_values = vec![0, 1, 3, 5, 6, 8, 1022, 1023];

        let file_path_col = Arc::new(StringArray::from_iter_values(file_path_values));
        let pos_col = Arc::new(Int64Array::from_iter_values(pos_values));

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        for n in 1..=3 {
            let positional_deletes_to_write =
                RecordBatch::try_new(positional_delete_schema.clone(), vec![
                    file_path_col.clone(),
                    pos_col.clone(),
                ])
                .unwrap();

            let file = File::create(format!(
                "{}/pos-del-{}.parquet",
                table_location.to_str().unwrap(),
                n
            ))
            .unwrap();
            let mut writer = ArrowWriter::try_new(
                file,
                positional_deletes_to_write.schema(),
                Some(props.clone()),
            )
            .unwrap();

            writer
                .write(&positional_deletes_to_write)
                .expect("Writing batch");

            // writer must be closed to write footer
            writer.close().unwrap();
        }

        let pos_del_1 = FileScanTaskDeleteFile {
            file_path: format!("{}/pos-del-1.parquet", table_location.to_str().unwrap()),
            file_type: DataContentType::PositionDeletes,
            partition_spec_id: 0,
            equality_ids: vec![],
        };

        let pos_del_2 = FileScanTaskDeleteFile {
            file_path: format!("{}/pos-del-2.parquet", table_location.to_str().unwrap()),
            file_type: DataContentType::PositionDeletes,
            partition_spec_id: 0,
            equality_ids: vec![],
        };

        let pos_del_3 = FileScanTaskDeleteFile {
            file_path: format!("{}/pos-del-3.parquet", table_location.to_str().unwrap()),
            file_type: DataContentType::PositionDeletes,
            partition_spec_id: 0,
            equality_ids: vec![],
        };

        let file_scan_tasks = vec![
            FileScanTask {
                start: 0,
                length: 0,
                record_count: None,
                data_file_path: "".to_string(),
                data_file_content: DataContentType::Data,
                data_file_format: DataFileFormat::Parquet,
                schema: data_file_schema.clone(),
                project_field_ids: vec![],
                predicate: None,
                deletes: vec![pos_del_1, pos_del_2.clone()],
            },
            FileScanTask {
                start: 0,
                length: 0,
                record_count: None,
                data_file_path: "".to_string(),
                data_file_content: DataContentType::Data,
                data_file_format: DataFileFormat::Parquet,
                schema: data_file_schema.clone(),
                project_field_ids: vec![],
                predicate: None,
                deletes: vec![pos_del_2, pos_del_3],
            },
        ];

        file_scan_tasks
    }

    fn create_pos_del_schema() -> ArrowSchemaRef {
        let fields = vec![
            arrow_schema::Field::new("file_path", arrow_schema::DataType::Utf8, false)
                .with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    FIELD_ID_POSITIONAL_DELETE_FILE_PATH.to_string(),
                )])),
            arrow_schema::Field::new("pos", arrow_schema::DataType::Int64, false).with_metadata(
                HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    FIELD_ID_POSITIONAL_DELETE_POS.to_string(),
                )]),
            ),
        ];
        Arc::new(arrow_schema::Schema::new(fields))
    }
}
