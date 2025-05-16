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
use std::ops::Not;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array, Int64Array,
    StringArray, Time64MicrosecondArray, TimestampMicrosecondArray, TimestampNanosecondArray,
};
use futures::channel::oneshot;
use futures::future::join_all;
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use tokio::sync::oneshot::{channel, Receiver};

use super::delete_filter::{DeleteFilter, EqDelFuture};
use crate::arrow::record_batch_transformer::RecordBatchTransformer;
use crate::arrow::{arrow_schema_to_schema, ArrowReader};
use crate::delete_vector::DeleteVector;
use crate::expr::Predicate::AlwaysTrue;
use crate::expr::{Predicate, Reference};
use crate::io::FileIO;
use crate::scan::{ArrowRecordBatchStream, FileScanTaskDeleteFile};
use crate::spec::{DataContentType, Datum, NestedFieldRef, PrimitiveType, Schema, SchemaRef};
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
    del_filter: DeleteFilter,
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
        equality_ids: HashSet<i32>,
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
            del_filter: DeleteFilter::default(),
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

        let stream_items = delete_file_entries
            .iter()
            .map(|t| {
                (
                    t.clone(),
                    self.file_io.clone(),
                    self.del_filter.clone(),
                    schema.clone(),
                )
            })
            .collect::<Vec<_>>();
        let task_stream = futures::stream::iter(stream_items);
        let del_filter = self.del_filter.clone();
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
                    equality_ids: HashSet::from_iter(task.equality_ids.clone()),
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
                equality_ids,
            } => {
                let predicate =
                    Self::parse_equality_deletes_record_batch_stream(batch_stream, equality_ids)
                        .await?;

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
        mut stream: ArrowRecordBatchStream,
        equality_ids: HashSet<i32>,
    ) -> Result<Predicate> {
        let mut result_predicate = AlwaysTrue;

        while let Some(record_batch) = stream.next().await {
            let record_batch = record_batch?;

            if record_batch.num_columns() == 0 {
                return Ok(AlwaysTrue);
            }

            let batch_schema_arrow = record_batch.schema();
            let batch_schema_iceberg = arrow_schema_to_schema(batch_schema_arrow.as_ref())?;

            let mut datum_columns_with_names: Vec<_> = record_batch
                .columns()
                .iter()
                .zip(batch_schema_iceberg.as_struct().fields())
                // only use columns that are in the set of equality_ids for this delete file
                .filter(|(field, value)| equality_ids.contains(&value.id))
                .map(|(column, field)| {
                    let col_as_datum_vec = arrow_array_to_datum_iterator(column, field);
                    col_as_datum_vec.map(|c| (c, field.name.to_string()))
                })
                .try_collect()?;

            // consume all the iterators in lockstep, creating per-row predicates that get combined
            // into a single final predicate
            while datum_columns_with_names[0].0.len() > 0 {
                let mut row_predicate = AlwaysTrue;
                for (ref mut column, ref field_name) in &mut datum_columns_with_names {
                    if let Some(item) = column.next() {
                        if let Some(datum) = item? {
                            row_predicate = row_predicate
                                .and(Reference::new(field_name.clone()).equal_to(datum.clone()));
                        }
                    }
                }
                result_predicate = result_predicate.and(row_predicate.not());
            }
        }
        Ok(result_predicate.rewrite_not())
    }
}

macro_rules! prim_to_datum {
    ($column:ident, $arr:ty, $dat:path) => {{
        let arr = $column.as_any().downcast_ref::<$arr>().ok_or(Error::new(
            ErrorKind::Unexpected,
            format!("could not downcast ArrayRef to {}", stringify!($arr)),
        ))?;
        Ok(Box::new(arr.iter().map(|val| Ok(val.map($dat)))))
    }};
}

fn eq_col_unsupported(ty: &str) -> Error {
    Error::new(
        ErrorKind::FeatureUnsupported,
        format!(
            "Equality deletes where a predicate acts upon a {} column are not yet supported",
            ty
        ),
    )
}

fn arrow_array_to_datum_iterator<'a>(
    column: &'a ArrayRef,
    field: &NestedFieldRef,
) -> Result<Box<dyn ExactSizeIterator<Item = Result<Option<Datum>>> + 'a>> {
    match field.field_type.as_primitive_type() {
        Some(primitive_type) => match primitive_type {
            PrimitiveType::Int => prim_to_datum!(column, Int32Array, Datum::int),
            PrimitiveType::Boolean => {
                prim_to_datum!(column, BooleanArray, Datum::bool)
            }
            PrimitiveType::Long => prim_to_datum!(column, Int64Array, Datum::long),
            PrimitiveType::Float => {
                prim_to_datum!(column, Float32Array, Datum::float)
            }
            PrimitiveType::Double => {
                prim_to_datum!(column, Float64Array, Datum::double)
            }
            PrimitiveType::String => {
                prim_to_datum!(column, StringArray, Datum::string)
            }
            PrimitiveType::Date => prim_to_datum!(column, Date32Array, Datum::date),
            PrimitiveType::Timestamp => {
                prim_to_datum!(column, TimestampMicrosecondArray, Datum::timestamp_micros)
            }
            PrimitiveType::Timestamptz => {
                prim_to_datum!(column, TimestampMicrosecondArray, Datum::timestamptz_micros)
            }
            PrimitiveType::TimestampNs => {
                prim_to_datum!(column, TimestampNanosecondArray, Datum::timestamp_nanos)
            }
            PrimitiveType::TimestamptzNs => {
                prim_to_datum!(column, TimestampNanosecondArray, Datum::timestamptz_nanos)
            }
            PrimitiveType::Time => {
                let arr = column
                    .as_any()
                    .downcast_ref::<Time64MicrosecondArray>()
                    .ok_or(Error::new(
                        ErrorKind::Unexpected,
                        "could not downcast ArrayRef to Time64MicrosecondArray",
                    ))?;
                Ok(Box::new(arr.iter().map(|val| match val {
                    None => Ok(None),
                    Some(val) => Datum::time_micros(val).map(Some),
                })))
            }
            PrimitiveType::Decimal { .. } => Err(eq_col_unsupported("Decimal")),
            PrimitiveType::Uuid => Err(eq_col_unsupported("Uuid")),
            PrimitiveType::Fixed(_) => Err(eq_col_unsupported("Fixed")),
            PrimitiveType::Binary => Err(eq_col_unsupported("Binary")),
        },
        None => Err(eq_col_unsupported(
            "non-primitive (i.e. Struct, List, or Map)",
        )),
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
    async fn test_delete_file_loader_load_deletes() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path();
        let file_io = FileIO::from_path(table_location.as_os_str().to_str().unwrap())
            .unwrap()
            .build()
            .unwrap();

        // Note that with the delete file parsing not yet in place, all we can test here is that
        // the call to the loader fails with the expected FeatureUnsupportedError.
        let delete_file_loader = CachingDeleteFileLoader::new(file_io.clone(), 10);

        let file_scan_tasks = setup_load_deletes_test_tasks(table_location);

        let result = delete_file_loader
            .load_deletes(&file_scan_tasks[0].deletes, file_scan_tasks[0].schema_ref())
            .await
            .unwrap();

        assert!(result.is_err_and(|e| e.kind() == ErrorKind::FeatureUnsupported));
    }

    #[tokio::test]
    async fn test_delete_file_loader_parse_equality_deletes() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().as_os_str().to_str().unwrap();
        let file_io = FileIO::from_path(table_location).unwrap().build().unwrap();

        let eq_delete_file_path = setup_write_equality_delete_file_1(table_location);

        let record_batch_stream =
            CachingDeleteFileLoader::parquet_to_batch_stream(&eq_delete_file_path, file_io.clone())
                .await
                .expect("could not get batch stream");

        let eq_ids = HashSet::from_iter(vec![2, 3, 4]);

        let parsed_eq_delete = CachingDeleteFileLoader::parse_equality_deletes_record_batch_stream(
            record_batch_stream,
            eq_ids,
        )
        .await
        .expect("error parsing batch stream");
        println!("{}", parsed_eq_delete);

        let expected = "(((y != 1) OR (z != 100)) OR (a != \"HELP\")) AND (y != 2)".to_string();

        assert_eq!(parsed_eq_delete.to_string(), expected);
    }

    fn setup_load_deletes_test_tasks(table_location: &Path) -> Vec<FileScanTask> {
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

    fn setup_write_equality_delete_file_1(table_location: &str) -> String {
        let col_y_vals = vec![1, 2];
        let col_y = Arc::new(Int64Array::from(col_y_vals)) as ArrayRef;

        let col_z_vals = vec![Some(100), None];
        let col_z = Arc::new(Int64Array::from(col_z_vals)) as ArrayRef;

        let col_a_vals = vec![Some("HELP"), None];
        let col_a = Arc::new(StringArray::from(col_a_vals)) as ArrayRef;

        let equality_delete_schema = {
            let fields = vec![
                arrow_schema::Field::new("y", arrow_schema::DataType::Int64, true).with_metadata(
                    HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "2".to_string())]),
                ),
                arrow_schema::Field::new("z", arrow_schema::DataType::Int64, true).with_metadata(
                    HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "3".to_string())]),
                ),
                arrow_schema::Field::new("a", arrow_schema::DataType::Utf8, true).with_metadata(
                    HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "4".to_string())]),
                ),
            ];
            Arc::new(arrow_schema::Schema::new(fields))
        };

        let equality_deletes_to_write =
            RecordBatch::try_new(equality_delete_schema.clone(), vec![col_y, col_z, col_a])
                .unwrap();

        let path = format!("{}/equality-deletes-1.parquet", &table_location);

        let file = File::create(&path).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let mut writer = ArrowWriter::try_new(
            file,
            equality_deletes_to_write.schema(),
            Some(props.clone()),
        )
        .unwrap();

        writer
            .write(&equality_deletes_to_write)
            .expect("Writing batch");

        // writer must be closed to write footer
        writer.close().unwrap();

        path
    }
}
