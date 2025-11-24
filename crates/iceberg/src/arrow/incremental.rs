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

use std::pin::Pin;
use std::sync::Arc;

use arrow_array::{RecordBatch, UInt64Array};
use arrow_schema::Schema as ArrowSchema;
use futures::channel::mpsc::channel;
use futures::stream::select;
use futures::{Stream, StreamExt, TryStreamExt};
use parquet::arrow::arrow_reader::ArrowReaderOptions;

use crate::arrow::reader::process_record_batch_stream;
use crate::arrow::record_batch_transformer::RecordBatchTransformerBuilder;
use crate::arrow::{ArrowReader, StreamsInto};
use crate::delete_vector::DeleteVector;
use crate::io::FileIO;
use crate::metadata_columns::{RESERVED_FIELD_ID_UNDERSCORE_POS, row_pos_field};
use crate::runtime::spawn;
use crate::scan::ArrowRecordBatchStream;
use crate::scan::incremental::{
    AppendedFileScanTask, IncrementalFileScanTask, IncrementalFileScanTaskStream,
};
use crate::{Error, ErrorKind, Result};

/// Default batch size for incremental delete operations.
const DEFAULT_BATCH_SIZE: usize = 1024;

/// The type of incremental batch: appended data or deleted records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncrementalBatchType {
    /// Appended records.
    Append,
    /// Deleted records.
    Delete,
}

/// The stream of incremental Arrow `RecordBatch`es with batch type.
pub type CombinedIncrementalBatchRecordStream =
    Pin<Box<dyn Stream<Item = Result<(IncrementalBatchType, RecordBatch)>> + Send + 'static>>;

/// Stream type for obtaining a separate stream of appended and deleted record batches.
pub type UnzippedIncrementalBatchRecordStream = (ArrowRecordBatchStream, ArrowRecordBatchStream);

impl StreamsInto<ArrowReader, CombinedIncrementalBatchRecordStream>
    for IncrementalFileScanTaskStream
{
    /// Takes a stream of `IncrementalFileScanTasks` and reads all the files. Returns a
    /// stream of Arrow `RecordBatch`es containing the data from the files.
    fn stream(self, reader: ArrowReader) -> Result<CombinedIncrementalBatchRecordStream> {
        let (appends, deletes) =
            StreamsInto::<ArrowReader, UnzippedIncrementalBatchRecordStream>::stream(self, reader)?;

        let left = appends.map(|res| res.map(|batch| (IncrementalBatchType::Append, batch)));
        let right = deletes.map(|res| res.map(|batch| (IncrementalBatchType::Delete, batch)));

        Ok(Box::pin(select(left, right)) as CombinedIncrementalBatchRecordStream)
    }
}

impl StreamsInto<ArrowReader, UnzippedIncrementalBatchRecordStream>
    for IncrementalFileScanTaskStream
{
    /// Takes a stream of `IncrementalFileScanTasks` and reads all the files. Returns two
    /// separate streams of Arrow `RecordBatch`es containing appended data and deleted records.
    fn stream(self, reader: ArrowReader) -> Result<UnzippedIncrementalBatchRecordStream> {
        let (appends_tx, appends_rx) =
            channel::<Result<RecordBatch>>(reader.concurrency_limit_data_files);
        let (deletes_tx, deletes_rx) =
            channel::<Result<RecordBatch>>(reader.concurrency_limit_data_files);

        let batch_size = reader.batch_size;

        spawn(async move {
            let _ = self
                .try_for_each_concurrent(reader.concurrency_limit_data_files, |task| {
                    let file_io = reader.file_io.clone();
                    let appends_tx = appends_tx.clone();
                    let deletes_tx = deletes_tx.clone();
                    async move {
                        match task {
                            IncrementalFileScanTask::Append(append_task) => {
                                spawn(async move {
                                    let record_batch_stream = process_incremental_append_task(
                                        append_task,
                                        batch_size,
                                        file_io,
                                    )
                                    .await;

                                    process_record_batch_stream(
                                        record_batch_stream,
                                        appends_tx,
                                        "failed to read appended record batch",
                                    )
                                    .await;
                                });
                            }
                            IncrementalFileScanTask::Delete(deleted_file_task) => {
                                spawn(async move {
                                    let file_path = deleted_file_task.data_file_path().to_string();
                                    let total_records =
                                        deleted_file_task.base.record_count.unwrap_or(0);

                                    let record_batch_stream = process_incremental_deleted_file_task(
                                        file_path,
                                        total_records,
                                        batch_size,
                                    );

                                    process_record_batch_stream(
                                        record_batch_stream,
                                        deletes_tx,
                                        "failed to read deleted file record batch",
                                    )
                                    .await;
                                });
                            }
                            IncrementalFileScanTask::PositionalDeletes(
                                file_path,
                                delete_vector,
                            ) => {
                                spawn(async move {
                                    let record_batch_stream = process_incremental_delete_task(
                                        file_path,
                                        delete_vector,
                                        batch_size,
                                    );

                                    process_record_batch_stream(
                                        record_batch_stream,
                                        deletes_tx,
                                        "failed to read deleted record batch",
                                    )
                                    .await;
                                });
                            }
                        }
                        Ok(())
                    }
                })
                .await;
        });

        Ok((
            Box::pin(appends_rx) as ArrowRecordBatchStream,
            Box::pin(deletes_rx) as ArrowRecordBatchStream,
        ))
    }
}

async fn process_incremental_append_task(
    task: AppendedFileScanTask,
    batch_size: Option<usize>,
    file_io: FileIO,
) -> Result<ArrowRecordBatchStream> {
    let mut virtual_columns = Vec::new();

    // Check if _pos column is requested and add it as a virtual column
    let has_pos_column = task
        .base
        .project_field_ids
        .contains(&RESERVED_FIELD_ID_UNDERSCORE_POS);
    if has_pos_column {
        // Add _pos as a virtual column to be produced by the Parquet reader
        virtual_columns.push(Arc::clone(row_pos_field()));
    }

    let arrow_reader_options = if !virtual_columns.is_empty() {
        Some(ArrowReaderOptions::new().with_virtual_columns(virtual_columns.clone())?)
    } else {
        None
    };

    let mut record_batch_stream_builder = ArrowReader::create_parquet_record_batch_stream_builder(
        &task.base.data_file_path,
        file_io,
        true,
        arrow_reader_options,
    )
    .await?;

    // Create a projection mask for the batch stream to select which columns in the
    // Parquet file that we want in the response
    let projection_mask = ArrowReader::get_arrow_projection_mask(
        &task.base.project_field_ids,
        &task.schema_ref(),
        record_batch_stream_builder.parquet_schema(),
        record_batch_stream_builder.schema(),
        false, // use_fallback
    )?;
    record_batch_stream_builder = record_batch_stream_builder.with_projection(projection_mask);

    // RecordBatchTransformer performs any transformations required on the RecordBatches
    // that come back from the file, such as type promotion, default column insertion,
    // column re-ordering, and virtual field addition (like _file)
    let mut record_batch_transformer_builder =
        RecordBatchTransformerBuilder::new(task.schema_ref(), &task.base.project_field_ids)
            .with_constant(
                crate::metadata_columns::RESERVED_FIELD_ID_FILE,
                crate::spec::PrimitiveLiteral::String(task.base.data_file_path.clone()),
            )?;

    if has_pos_column {
        record_batch_transformer_builder =
            record_batch_transformer_builder.with_virtual_field(Arc::clone(row_pos_field()))?;
    }

    let mut record_batch_transformer = record_batch_transformer_builder.build();

    if let Some(batch_size) = batch_size {
        record_batch_stream_builder = record_batch_stream_builder.with_batch_size(batch_size);
    }

    // Apply positional deletes as row selections.
    let row_selection = if let Some(positional_delete_indexes) = task.positional_deletes {
        Some(ArrowReader::build_deletes_row_selection(
            record_batch_stream_builder.metadata().row_groups(),
            &None,
            &positional_delete_indexes.lock().unwrap(),
        )?)
    } else {
        None
    };

    if let Some(row_selection) = row_selection {
        record_batch_stream_builder = record_batch_stream_builder.with_row_selection(row_selection);
    }

    // Build the batch stream and send all the RecordBatches that it generates
    // to the requester.
    let record_batch_stream = record_batch_stream_builder
        .build()?
        .map(move |batch| match batch {
            Ok(batch) => record_batch_transformer.process_record_batch(batch),
            Err(err) => Err(err.into()),
        });

    Ok(Box::pin(record_batch_stream) as ArrowRecordBatchStream)
}

fn process_incremental_delete_task(
    file_path: String,
    delete_vector: DeleteVector,
    batch_size: Option<usize>,
) -> Result<ArrowRecordBatchStream> {
    let schema = Arc::new(ArrowSchema::new(vec![Arc::clone(
        crate::metadata_columns::pos_field(),
    )]));

    let batch_size = batch_size.unwrap_or(DEFAULT_BATCH_SIZE);

    let treemap = delete_vector.inner;

    let stream = futures::stream::iter(treemap)
        .chunks(batch_size)
        .map(move |chunk| {
            let array = UInt64Array::from_iter(chunk);
            RecordBatch::try_new(
                Arc::clone(&schema), // Cheap Arc clone instead of full schema creation
                vec![Arc::new(array)],
            )
            .map_err(|_| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Failed to create RecordBatch for DeleteVector",
                )
            })
            .and_then(|batch| ArrowReader::add_file_path_column(batch, &file_path))
        });

    Ok(Box::pin(stream) as ArrowRecordBatchStream)
}

fn process_incremental_deleted_file_task(
    file_path: String,
    total_records: u64,
    batch_size: Option<usize>,
) -> Result<ArrowRecordBatchStream> {
    let schema = Arc::new(ArrowSchema::new(vec![Arc::clone(
        crate::metadata_columns::pos_field(),
    )]));

    let batch_size = batch_size.unwrap_or(DEFAULT_BATCH_SIZE);

    // Create a stream of position values from 0 to total_records-1 (0-indexed)
    let stream = futures::stream::iter(0..total_records)
        .chunks(batch_size)
        .map(move |chunk| {
            let array = UInt64Array::from_iter(chunk);
            RecordBatch::try_new(
                Arc::clone(&schema), // Cheap Arc clone instead of full schema creation
                vec![Arc::new(array)],
            )
            .map_err(|_| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Failed to create RecordBatch for deleted file",
                )
            })
            .and_then(|batch| ArrowReader::add_file_path_column(batch, &file_path))
        });

    Ok(Box::pin(stream) as ArrowRecordBatchStream)
}
