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

use futures::channel::oneshot;
use futures::future::join_all;
use futures::{StreamExt, TryStreamExt};
use tokio::sync::oneshot::{channel, Receiver};

use super::delete_filter::{DeleteFilter, EqDelFuture};
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
            basic_delete_file_loader: BasicDeleteFileLoader::new(file_io),
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
        basic_delete_file_loader: BasicDeleteFileLoader,
        del_filter: DeleteFilter,
        schema: SchemaRef,
    ) -> Result<DeleteFileContext> {
        match task.file_type {
            DataContentType::PositionDeletes => Ok(DeleteFileContext::PosDels(
                basic_delete_file_loader
                    .parquet_to_batch_stream(&task.file_path)
                    .await?,
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
    use tempfile::TempDir;

    use super::*;
    use crate::arrow::delete_file_loader::tests::setup;

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
}
