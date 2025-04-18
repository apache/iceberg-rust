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
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock, RwLock};
use std::task::{Context, Poll};

use arrow_array::{Int64Array, StringArray};
use futures::channel::oneshot;
use futures::future::join_all;
use futures::{StreamExt, TryStreamExt};

use crate::arrow::ArrowReader;
use crate::delete_vector::DeleteVector;
use crate::expr::Predicate::AlwaysTrue;
use crate::expr::{Bind, BoundPredicate, Predicate};
use crate::io::FileIO;
use crate::scan::{ArrowRecordBatchStream, FileScanTask, FileScanTaskDeleteFile};
use crate::spec::DataContentType;
use crate::{Error, ErrorKind, Result};

#[allow(unused)]
pub trait DeleteFileManager {
    /// Read the delete file referred to in the task
    ///
    /// Returns the raw contents of the delete file as a RecordBatch stream
    fn read_delete_file(task: &FileScanTaskDeleteFile) -> Result<ArrowRecordBatchStream>;
}

#[allow(unused)]
#[derive(Clone, Debug)]
pub(crate) struct CachingDeleteFileManager {
    file_io: FileIO,
    concurrency_limit_data_files: usize,
    state: Arc<RwLock<DeleteFileManagerState>>,
}

impl DeleteFileManager for CachingDeleteFileManager {
    fn read_delete_file(_task: &FileScanTaskDeleteFile) -> Result<ArrowRecordBatchStream> {
        // TODO, implementation in https://github.com/apache/iceberg-rust/pull/982

        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Reading delete files is not yet supported",
        ))
    }
}
// Equality deletes may apply to more than one DataFile in a scan, and so
// the same equality delete file may be present in more than one invocation of
// DeleteFileManager::load_deletes in the same scan. We want to deduplicate these
// to avoid having to load them twice, so we immediately store cloneable futures in the
// state that can be awaited upon to get te EQ deletes. That way we can check to see if
// a load of each Eq delete file is already in progress and avoid starting another one.
#[derive(Debug, Clone)]
struct EqDelFuture {
    result: OnceLock<Predicate>,
}

impl EqDelFuture {
    pub fn new() -> (oneshot::Sender<Predicate>, Self) {
        let (tx, rx) = oneshot::channel();
        let result = OnceLock::new();

        crate::runtime::spawn({
            let result = result.clone();
            async move { result.set(rx.await.unwrap()) }
        });

        (tx, Self { result })
    }
}

impl Future for EqDelFuture {
    type Output = Predicate;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.result.get() {
            None => Poll::Pending,
            Some(predicate) => Poll::Ready(predicate.clone()),
        }
    }
}

#[derive(Debug, Default)]
struct DeleteFileManagerState {
    // delete vectors and positional deletes get merged when loaded into a single delete vector
    // per data file
    delete_vectors: HashMap<String, Arc<RwLock<DeleteVector>>>,

    // equality delete files are parsed into unbound `Predicate`s. We store them here as
    // cloneable futures (see note below)
    equality_deletes: HashMap<String, EqDelFuture>,
}

type StateRef = Arc<RwLock<DeleteFileManagerState>>;

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
impl CachingDeleteFileManager {
    pub(crate) fn new(file_io: FileIO, concurrency_limit_data_files: usize) -> Self {
        CachingDeleteFileManager {
            file_io,
            concurrency_limit_data_files,
            state: Arc::new(Default::default()),
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
    ///      level of concurrency and collected into a vec. We then combine all of the delete
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
    pub(crate) async fn load_deletes(
        &self,
        delete_file_entries: &[FileScanTaskDeleteFile],
    ) -> Result<()> {
        let stream_items = delete_file_entries
            .iter()
            .map(|t| (t.clone(), self.file_io.clone(), self.state.clone()))
            .collect::<Vec<_>>();
        // NOTE: removing the collect and just passing the iterator to futures::stream:iter
        // results in an error 'implementation of `std::ops::FnOnce` is not general enough'

        let task_stream = futures::stream::iter(stream_items.into_iter());

        let results: Vec<ParsedDeleteFileContext> = task_stream
            .map(move |(task, file_io, state_ref)| async {
                Self::load_file_for_task(task, file_io, state_ref).await
            })
            .map(move |ctx| Ok(async { Self::parse_file_content_for_task(ctx.await?).await }))
            .try_buffer_unordered(self.concurrency_limit_data_files)
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

        let merged_delete_vectors = results
            .into_iter()
            .fold(HashMap::default(), Self::merge_delete_vectors);

        self.state.write().unwrap().delete_vectors = merged_delete_vectors;

        Ok(())
    }

    async fn load_file_for_task(
        task: FileScanTaskDeleteFile,
        file_io: FileIO,
        state: StateRef,
    ) -> Result<DeleteFileContext> {
        match task.file_type {
            DataContentType::PositionDeletes => Ok(DeleteFileContext::PosDels(
                Self::parquet_to_batch_stream(&task.file_path, file_io).await?,
            )),

            DataContentType::EqualityDeletes => {
                let sender = {
                    let mut state = state.write().unwrap();
                    if let Some(existing) = state.equality_deletes.get(&task.file_path) {
                        return Ok(DeleteFileContext::InProgEqDel(existing.clone()));
                    }

                    let (sender, fut) = EqDelFuture::new();

                    state
                        .equality_deletes
                        .insert(task.file_path.to_string(), fut);

                    sender
                };

                Ok(DeleteFileContext::FreshEqDel {
                    batch_stream: Self::parquet_to_batch_stream(&task.file_path, file_io).await?,
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

    fn merge_delete_vectors(
        mut merged_delete_vectors: HashMap<String, Arc<RwLock<DeleteVector>>>,
        item: ParsedDeleteFileContext,
    ) -> HashMap<String, Arc<RwLock<DeleteVector>>> {
        if let ParsedDeleteFileContext::DelVecs(del_vecs) = item {
            del_vecs.into_iter().for_each(|(key, val)| {
                let entry = merged_delete_vectors.entry(key).or_default();
                {
                    let mut inner = entry.write().unwrap();
                    (*inner).intersect_assign(&val);
                }
            });
        }

        merged_delete_vectors
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
    async fn parse_equality_deletes_record_batch_stream(
        streams: ArrowRecordBatchStream,
    ) -> Result<Predicate> {
        // TODO

        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "parsing of equality deletes is not yet supported",
        ))
    }

    /// Builds eq delete predicate for the provided task.
    ///
    /// Must await on load_deletes before calling this.
    pub(crate) async fn build_delete_predicate_for_task(
        &self,
        file_scan_task: &FileScanTask,
    ) -> Result<Option<BoundPredicate>> {
        // * Filter the task's deletes into just the Equality deletes
        // * Retrieve the unbound predicate for each from self.state.equality_deletes
        // * Logical-AND them all together to get a single combined `Predicate`
        // * Bind the predicate to the task's schema to get a `BoundPredicate`

        let mut combined_predicate = AlwaysTrue;
        for delete in &file_scan_task.deletes {
            if !is_equality_delete(delete) {
                continue;
            }

            let predicate = {
                let state = self.state.read().unwrap();

                let Some(predicate) = state.equality_deletes.get(&delete.file_path) else {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        format!(
                            "Missing predicate for equality delete file '{}'",
                            delete.file_path
                        ),
                    ));
                };

                predicate.clone()
            };

            combined_predicate = combined_predicate.and(predicate.await);
        }

        if combined_predicate == AlwaysTrue {
            return Ok(None);
        }

        // TODO: handle case-insensitive case
        let bound_predicate = combined_predicate.bind(file_scan_task.schema.clone(), false)?;
        Ok(Some(bound_predicate))
    }

    /// Retrieve a delete vector for the data file associated with a given file scan task
    ///
    /// Should only be called after awaiting on load_deletes. Takes the vector to avoid a
    /// clone since each item is specific to a single data file and won't need to be used again
    pub(crate) fn get_delete_vector_for_task(
        &self,
        file_scan_task: &FileScanTask,
    ) -> Option<Arc<RwLock<DeleteVector>>> {
        self.state
            .write()
            .unwrap()
            .delete_vectors
            .get(file_scan_task.data_file_path())
            .map(Clone::clone)
    }
}

pub(crate) fn is_equality_delete(f: &FileScanTaskDeleteFile) -> bool {
    matches!(f.file_type, DataContentType::EqualityDeletes)
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

        let delete_file_manager = CachingDeleteFileManager::new(file_io.clone(), 10);

        let file_scan_tasks = setup(table_location);

        delete_file_manager
            .load_deletes(&file_scan_tasks[0].deletes)
            .await
            .unwrap();

        let result = delete_file_manager
            .get_delete_vector_for_task(&file_scan_tasks[0])
            .unwrap();
        assert_eq!(result.read().unwrap().len(), 3); // pos dels from pos del file 1 and 2

        let result = delete_file_manager
            .get_delete_vector_for_task(&file_scan_tasks[1])
            .unwrap();
        assert_eq!(result.read().unwrap().len(), 3); // pos dels from pos del file 3
    }

    fn setup(table_location: &Path) -> Vec<FileScanTask> {
        let data_file_schema = Arc::new(Schema::builder().build().unwrap());
        let positional_delete_schema = create_pos_del_schema();

        let mut file_path_values = vec![];
        let mut pos_values = vec![];

        file_path_values.push(vec![
            format!(
                "{}/1.parquet",
                table_location.to_str().unwrap()
            );
            3
        ]);
        pos_values.push(vec![0, 1, 3]);

        file_path_values.push(vec![
            format!(
                "{}/1.parquet",
                table_location.to_str().unwrap()
            );
            3
        ]);
        pos_values.push(vec![5, 6, 8]);

        file_path_values.push(vec![
            format!(
                "{}/2.parquet",
                table_location.to_str().unwrap()
            );
            3
        ]);
        pos_values.push(vec![1022, 1023, 1024]);
        // 9 rows in total pos deleted across 3 files

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        for n in 1..=3 {
            let file_path_col = Arc::new(StringArray::from_iter_values(
                file_path_values.pop().unwrap(),
            ));
            let pos_col = Arc::new(Int64Array::from_iter_values(pos_values.pop().unwrap()));

            let positional_deletes_to_write =
                RecordBatch::try_new(positional_delete_schema.clone(), vec![
                    file_path_col.clone(),
                    pos_col,
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
        };

        let pos_del_2 = FileScanTaskDeleteFile {
            file_path: format!("{}/pos-del-2.parquet", table_location.to_str().unwrap()),
            file_type: DataContentType::PositionDeletes,
            partition_spec_id: 0,
        };

        let pos_del_3 = FileScanTaskDeleteFile {
            file_path: format!("{}/pos-del-3.parquet", table_location.to_str().unwrap()),
            file_type: DataContentType::PositionDeletes,
            partition_spec_id: 0,
        };

        let file_scan_tasks = vec![
            FileScanTask {
                start: 0,
                length: 0,
                record_count: None,
                data_file_path: format!("{}/1.parquet", table_location.to_str().unwrap()),
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
                data_file_path: format!("{}/2.parquet", table_location.to_str().unwrap()),
                data_file_content: DataContentType::Data,
                data_file_format: DataFileFormat::Parquet,
                schema: data_file_schema.clone(),
                project_field_ids: vec![],
                predicate: None,
                deletes: vec![pos_del_3],
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
