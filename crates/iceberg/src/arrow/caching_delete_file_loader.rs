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

use arrow_array::{Array, ArrayRef, Int64Array, StringArray, StructArray};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use tokio::sync::oneshot::{Receiver, channel};

use super::delete_filter::{DeleteFilter, PosDelLoadAction};
use crate::arrow::delete_file_loader::BasicDeleteFileLoader;
use crate::arrow::scan_metrics::ScanMetrics;
use crate::arrow::{arrow_primitive_to_literal, arrow_schema_to_schema};
use crate::delete_vector::DeleteVector;
use crate::encryption::{EncryptedInputFile, StandardKeyMetadata};
use crate::expr::Predicate::AlwaysTrue;
use crate::expr::{Predicate, Reference};
use crate::io::FileIO;
use crate::runtime::Runtime;
use crate::scan::{ArrowRecordBatchStream, FileScanTaskDeleteFile};
use crate::spec::{
    DataContentType, DataFileFormat, Datum, ListType, MapType, NestedField, NestedFieldRef,
    PartnerAccessor, PrimitiveType, Schema, SchemaRef, SchemaWithPartnerVisitor, StructType, Type,
    VariantType, visit_schema_with_partner,
};
use crate::{Error, ErrorKind, Result};

#[derive(Clone, Debug)]
pub(crate) struct CachingDeleteFileLoader {
    basic_delete_file_loader: BasicDeleteFileLoader,
    concurrency_limit_data_files: usize,
    /// Shared filter state to allow caching loaded deletes across multiple
    /// calls to `load_deletes` (e.g., across multiple file scan tasks).
    delete_filter: DeleteFilter,
    runtime: Runtime,
}

// Intermediate context during processing of a delete file task.
enum DeleteFileContext {
    ExistingEqDel,
    ExistingPosDel,
    PosDels {
        file_path: String,
        stream: ArrowRecordBatchStream,
    },
    FreshEqDel {
        batch_stream: ArrowRecordBatchStream,
        equality_ids: HashSet<i32>,
        sender: tokio::sync::oneshot::Sender<Predicate>,
    },
    // A V3 deletion vector: the raw deletion-vector-v1 blob bytes, the data file whose rows it
    // deletes, and the manifest's expected cardinality. The blob is decoded and validated in the
    // parse phase.
    DelVec {
        data_file_path: String,
        blob: Bytes,
        record_count: u64,
        dv_path: String,
    },
}

// Final result of the processing of a delete file task before
// results are fully merged into the DeleteFileManager's state
enum ParsedDeleteFileContext {
    DelVecs {
        file_path: String,
        results: HashMap<String, DeleteVector>,
    },
    // A single deletion vector decoded from a Puffin blob, keyed by the data file it applies to.
    DelVec {
        data_file_path: String,
        delete_vector: DeleteVector,
    },
    EqDel,
    ExistingPosDel,
}

#[allow(unused_variables)]
impl CachingDeleteFileLoader {
    pub(crate) fn new(
        file_io: FileIO,
        concurrency_limit_data_files: usize,
        runtime: Runtime,
    ) -> Self {
        let scan_metrics = ScanMetrics::new();
        CachingDeleteFileLoader {
            basic_delete_file_loader: BasicDeleteFileLoader::new(file_io, scan_metrics),
            concurrency_limit_data_files,
            delete_filter: DeleteFilter::new(runtime.clone()),
            runtime,
        }
    }

    pub(crate) fn with_scan_metrics(mut self, scan_metrics: ScanMetrics) -> Self {
        self.basic_delete_file_loader = BasicDeleteFileLoader::new(
            self.basic_delete_file_loader.file_io().clone(),
            scan_metrics,
        );
        self
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
    ///  * For a V3 deletion vector, the load phase reads the blob's byte range directly from its
    ///    Puffin file (decrypting first if the entry carries key metadata), and the parse phase
    ///    decodes it into a single `DeleteVector`.
    ///  * The parse phase parses each record batch stream according to its associated data type.
    ///    The result of this is a map of data file paths to delete vectors for the positional
    ///    delete tasks, or a single (data file path, delete vector) pair for a deletion vector
    ///    task. For equality delete file tasks, this results in an unbound Predicate.
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
    ///                     Pos Del                       Del Vec                    EQ Del
    ///                       |                             |                          |
    ///              [parse pos del stream]         [parse del vec puffin]       [parse eq del]
    ///          HashMap<String, RoaringTreeMap>        DeleteVector             (Predicate, Sender)
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

        let stream_items = delete_file_entries
            .iter()
            .map(|t| {
                (
                    t.clone(),
                    self.basic_delete_file_loader.clone(),
                    self.delete_filter.clone(),
                    schema.clone(),
                )
            })
            .collect::<Vec<_>>();
        let task_stream = futures::stream::iter(stream_items);

        let del_filter = self.delete_filter.clone();
        let concurrency_limit_data_files = self.concurrency_limit_data_files;
        let basic_delete_file_loader = self.basic_delete_file_loader.clone();
        self.runtime.io().spawn(async move {
            let result = async move {
                let mut del_filter = del_filter;
                let basic_delete_file_loader = basic_delete_file_loader.clone();

                let mut results_stream = task_stream
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
                    .try_buffer_unordered(concurrency_limit_data_files);

                while let Some(item) = results_stream.next().await {
                    match item? {
                        ParsedDeleteFileContext::DelVecs { file_path, results } => {
                            for (data_file_path, delete_vector) in results.into_iter() {
                                del_filter.upsert_delete_vector(data_file_path, delete_vector);
                            }
                            // Mark the positional delete file as fully loaded so waiters can proceed
                            del_filter.finish_pos_del_load(&file_path);
                        }
                        ParsedDeleteFileContext::DelVec {
                            data_file_path,
                            delete_vector,
                        } => {
                            del_filter.upsert_delete_vector(data_file_path, delete_vector);
                        }
                        ParsedDeleteFileContext::EqDel
                        | ParsedDeleteFileContext::ExistingPosDel => {}
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
            DataContentType::PositionDeletes => {
                // A V3 deletion vector arrives as a PositionDeletes entry whose deletes live in
                // a Puffin blob, not in a positional-delete parquet file.
                if task.file_format == DataFileFormat::Puffin {
                    return Self::load_deletion_vector(task, basic_delete_file_loader).await;
                }

                match del_filter.try_start_pos_del_load(&task.file_path) {
                    PosDelLoadAction::AlreadyLoaded => Ok(DeleteFileContext::ExistingPosDel),
                    PosDelLoadAction::WaitFor(notified) => {
                        // Positional deletes are accessed synchronously by ArrowReader.
                        // We must wait here to ensure the data is ready before returning,
                        // otherwise ArrowReader might get an empty/partial result.
                        notified.await;
                        Ok(DeleteFileContext::ExistingPosDel)
                    }
                    PosDelLoadAction::Load => Ok(DeleteFileContext::PosDels {
                        file_path: task.file_path.clone(),
                        stream: basic_delete_file_loader
                            .parquet_to_batch_stream(
                                &task.file_path,
                                task.file_size_in_bytes,
                                task.key_metadata.as_deref(),
                            )
                            .await?,
                    }),
                }
            }

            DataContentType::EqualityDeletes => {
                let Some(notify) = del_filter.try_start_eq_del_load(&task.file_path) else {
                    return Ok(DeleteFileContext::ExistingEqDel);
                };

                let (sender, receiver) = channel();
                del_filter.insert_equality_delete(&task.file_path, receiver);

                // Per the Iceberg spec, evolve schema for equality deletes but only for the
                // equality_ids columns, not all table columns.
                let equality_ids_vec = task.equality_ids.clone().unwrap();
                let evolved_stream = BasicDeleteFileLoader::evolve_schema(
                    basic_delete_file_loader
                        .parquet_to_batch_stream(
                            &task.file_path,
                            task.file_size_in_bytes,
                            task.key_metadata.as_deref(),
                        )
                        .await?,
                    schema,
                    &equality_ids_vec,
                )
                .await?;

                Ok(DeleteFileContext::FreshEqDel {
                    batch_stream: evolved_stream,
                    sender,
                    equality_ids: HashSet::from_iter(equality_ids_vec),
                })
            }

            DataContentType::Data => Err(Error::new(
                ErrorKind::Unexpected,
                "tasks with files of type Data not expected here",
            )),
        }
    }

    /// Validates a deletion-vector task and returns what the read needs as typed values:
    /// `(start, len, referenced data file path, expected cardinality)`.
    ///
    /// The spec requires `referenced_data_file`, `content_offset` and `content_size_in_bytes` on
    /// a deletion vector, and a deletion vector is always built from a manifest entry, so it
    /// always carries `record_count`. A missing one is a manifest-entry inconsistency rather
    /// than an I/O failure.
    ///
    /// Equality and ordinary position deletes have no equivalent validation in this loader: a
    /// malformed equality/position delete file fails loudly when the Parquet reader can't open
    /// it. A deletion vector's coordinates instead drive a raw byte-range read with no format
    /// to fail against, so a bad coordinate would otherwise decode silently into the wrong (or
    /// no) deletes, per the same corrupted-blob concern Iceberg-Java validates in
    /// `BitmapPositionDeleteIndex.deserializeBitmap`.
    fn validate_deletion_vector_task(
        task: &FileScanTaskDeleteFile,
    ) -> Result<(u64, u64, String, u64)> {
        let content_offset = task.content_offset.ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "deletion vector {} is missing content_offset",
                    task.file_path
                ),
            )
        })?;
        let content_size = task.content_size_in_bytes.ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "deletion vector {} is missing content_size_in_bytes",
                    task.file_path
                ),
            )
        })?;
        let data_file_path = task.referenced_data_file.clone().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "deletion vector {} is missing referenced_data_file",
                    task.file_path
                ),
            )
        })?;
        let record_count = task.record_count.ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("deletion vector {} is missing record_count", task.file_path),
            )
        })?;

        let start = u64::try_from(content_offset).map_err(|_| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "deletion vector {} has negative content_offset {content_offset}",
                    task.file_path
                ),
            )
        })?;
        let len = u64::try_from(content_size).map_err(|_| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "deletion vector {} has negative content_size_in_bytes {content_size}",
                    task.file_path
                ),
            )
        })?;

        Ok((start, len, data_file_path, record_count))
    }

    /// Validates a decoded deletion vector's cardinality against the manifest entry's
    /// `record_count`, mirroring Iceberg-Java's `BitmapPositionDeleteIndex.deserializeBitmap`.
    fn validate_deletion_vector_cardinality(
        delete_vector: &DeleteVector,
        expected: u64,
        dv_path: &str,
    ) -> Result<()> {
        let actual = delete_vector.len();
        if actual != expected {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "deletion vector {dv_path} decoded to {actual} positions, expected {expected} from record_count"
                ),
            ));
        }
        Ok(())
    }

    /// Reads a V3 deletion vector blob directly from its Puffin file.
    ///
    /// The spec requires a delete manifest entry's `content_offset` / `content_size_in_bytes` to
    /// match the blob's offset and length in the Puffin footer, so the blob is read by range
    /// without parsing the footer. It is decoded into a [`DeleteVector`] in the parse phase.
    ///
    /// Decrypts the range read when `task.key_metadata` is set, the same way `ManifestReader`
    /// decrypts a manifest file (`spec/manifest/reader.rs`): the coordinate space of
    /// `content_offset` / `content_size_in_bytes` is the plaintext file, which is what
    /// `EncryptedInputFile` reads over.
    async fn load_deletion_vector(
        task: &FileScanTaskDeleteFile,
        basic_delete_file_loader: BasicDeleteFileLoader,
    ) -> Result<DeleteFileContext> {
        let (start, len, data_file_path, record_count) = Self::validate_deletion_vector_task(task)?;

        let input_file = basic_delete_file_loader
            .file_io()
            .new_input(&task.file_path)?;
        let blob = match task.key_metadata.as_deref() {
            Some(key_metadata) => {
                let key_metadata = StandardKeyMetadata::decode(key_metadata)?;
                EncryptedInputFile::new(input_file, key_metadata)
                    .reader()
                    .await?
                    .read(start..start + len)
                    .await?
            }
            None => input_file.reader().await?.read(start..start + len).await?,
        };

        Ok(DeleteFileContext::DelVec {
            data_file_path,
            blob,
            record_count,
            dv_path: task.file_path.clone(),
        })
    }

    async fn parse_file_content_for_task(
        ctx: DeleteFileContext,
    ) -> Result<ParsedDeleteFileContext> {
        match ctx {
            DeleteFileContext::ExistingEqDel => Ok(ParsedDeleteFileContext::EqDel),
            DeleteFileContext::ExistingPosDel => Ok(ParsedDeleteFileContext::ExistingPosDel),
            DeleteFileContext::PosDels { file_path, stream } => {
                let del_vecs = Self::parse_positional_deletes_record_batch_stream(stream).await?;
                Ok(ParsedDeleteFileContext::DelVecs {
                    file_path,
                    results: del_vecs,
                })
            }
            DeleteFileContext::DelVec {
                data_file_path,
                blob,
                record_count,
                dv_path,
            } => {
                let delete_vector = DeleteVector::deserialize(&blob)?;
                Self::validate_deletion_vector_cardinality(&delete_vector, record_count, &dv_path)?;

                Ok(ParsedDeleteFileContext::DelVec {
                    data_file_path,
                    delete_vector,
                })
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

    /// Parses a record batch stream coming from positional delete files
    ///
    /// Returns a map of data file path to a delete vector
    async fn parse_positional_deletes_record_batch_stream(
        mut stream: ArrowRecordBatchStream,
    ) -> Result<HashMap<String, DeleteVector>> {
        let mut result: HashMap<String, DeleteVector> = HashMap::default();
        let mut run_positions: Vec<u64> = Vec::new();

        while let Some(batch) = stream.next().await {
            // run_positions is reused across batches, so the end-of-batch flush
            // below must drain it before the next batch opens a new run.
            debug_assert!(run_positions.is_empty());

            let batch = batch?;
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

            // Within a batch, positional deletes are sorted by (file_path, pos),
            // so the rows for one data file form a contiguous run. Buffer each
            // run and merge it with a single map lookup, allocating and hashing
            // the key once per run instead of once per row. Grouping is per
            // batch, not across the whole stream: a run never spans batch
            // boundaries, so a path that also appears in another batch merges
            // into its existing delete vector (order does not affect the result).
            let mut run_path: Option<&str> = None;

            for (file_path, pos) in file_paths.iter().zip(positions.iter()) {
                let (Some(file_path), Some(pos)) = (file_path, pos) else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "null values in delete file",
                    ));
                };
                if pos < 0 {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("negative position in delete file {file_path}: {pos}"),
                    ));
                }

                if run_path != Some(file_path) {
                    if let Some(prev_path) = run_path {
                        Self::merge_delete_positions(&mut result, prev_path, &run_positions);
                        run_positions.clear();
                    }

                    run_path = Some(file_path);
                }

                run_positions.push(pos as u64);
            }

            if let Some(prev_path) = run_path {
                Self::merge_delete_positions(&mut result, prev_path, &run_positions);
                run_positions.clear();
            }
        }

        Ok(result)
    }

    /// Marks every position in `positions` as deleted for `file_path`, merging
    /// into any delete vector already recorded for that file.
    fn merge_delete_positions(
        result: &mut HashMap<String, DeleteVector>,
        file_path: &str,
        positions: &[u64],
    ) {
        // Callers only flush a run after pushing at least one position onto it.
        debug_assert!(!positions.is_empty());

        let delete_vector = result.entry(file_path.to_string()).or_default();
        // In spec-compliant files rows are sorted by (file_path, pos), so a run is
        // usually ascending with no value already recorded, which `insert_positions`
        // bulk-appends in one pass. Its precondition is stricter than the spec,
        // though: it rejects duplicate positions (sorted means non-decreasing, so
        // ties are compliant) as well as out-of-order rows and runs that overlap
        // positions from an earlier batch. Fall back to per-position inserts in
        // those cases; `insert` is idempotent, so re-inserting any prefix the failed
        // append already added is harmless.
        if let Err(err) = delete_vector.insert_positions(positions) {
            tracing::debug!(
                file_path,
                run_len = positions.len(),
                error = %err,
                "positional delete run fell back to per-position insert"
            );
            for &pos in positions {
                delete_vector.insert(pos);
            }
        }
    }

    async fn parse_equality_deletes_record_batch_stream(
        mut stream: ArrowRecordBatchStream,
        equality_ids: HashSet<i32>,
    ) -> Result<Predicate> {
        let mut row_predicates = Vec::new();
        let mut batch_schema_iceberg: Option<Schema> = None;
        let accessor = EqDelRecordBatchPartnerAccessor;

        while let Some(record_batch) = stream.next().await {
            let record_batch = record_batch?;

            if record_batch.num_columns() == 0 {
                return Ok(AlwaysTrue);
            }

            let schema = match &batch_schema_iceberg {
                Some(schema) => schema,
                None => {
                    let schema = arrow_schema_to_schema(record_batch.schema().as_ref())?;
                    batch_schema_iceberg = Some(schema);
                    batch_schema_iceberg.as_ref().unwrap()
                }
            };

            let root_array: ArrayRef = Arc::new(StructArray::from(record_batch));

            let mut processor = EqDelColumnProcessor::new(&equality_ids);
            visit_schema_with_partner(schema, &root_array, &mut processor, &accessor)?;

            let mut datum_columns_with_names = processor.finish()?;
            if datum_columns_with_names.is_empty() {
                continue;
            }

            // Iceberg spec (Equality Delete Files): a null data value never equals a non-null
            // delete value, so a row with a null equality column must be kept. Build the keep
            // predicate as `col IS NULL OR col != v` (`col IS NOT NULL` for a null delete value);
            // a bare `col != v` drops nulls.
            #[allow(clippy::len_zero)]
            while datum_columns_with_names[0].0.len() > 0 {
                let mut row_keep_predicate = Predicate::AlwaysFalse;
                for &mut (ref mut column, ref field_name) in &mut datum_columns_with_names {
                    if let Some(item) = column.next() {
                        let reference = Reference::new(field_name.clone());
                        let cell_keep_predicate = if let Some(datum) = item? {
                            reference
                                .clone()
                                .is_null()
                                .or(reference.not_equal_to(datum.clone()))
                        } else {
                            reference.is_not_null()
                        };
                        row_keep_predicate = row_keep_predicate.or(cell_keep_predicate);
                    }
                }
                row_predicates.push(row_keep_predicate);
            }
        }

        // All row predicates are combined to a single predicate by creating a balanced binary tree.
        // Using a simple fold would result in a deeply nested predicate that can cause a stack overflow.
        while row_predicates.len() > 1 {
            let mut next_level = Vec::with_capacity(row_predicates.len().div_ceil(2));
            let mut iter = row_predicates.into_iter();
            while let Some(p1) = iter.next() {
                if let Some(p2) = iter.next() {
                    next_level.push(p1.and(p2));
                } else {
                    next_level.push(p1);
                }
            }
            row_predicates = next_level;
        }

        match row_predicates.pop() {
            Some(p) => Ok(p),
            None => Ok(AlwaysTrue),
        }
    }
}

struct EqDelColumnProcessor<'a> {
    equality_ids: &'a HashSet<i32>,
    collected_columns: Vec<(ArrayRef, String, Type)>,
}

impl<'a> EqDelColumnProcessor<'a> {
    fn new(equality_ids: &'a HashSet<i32>) -> Self {
        Self {
            equality_ids,
            collected_columns: Vec::with_capacity(equality_ids.len()),
        }
    }

    #[allow(clippy::type_complexity)]
    fn finish(
        self,
    ) -> Result<
        Vec<(
            Box<dyn ExactSizeIterator<Item = Result<Option<Datum>>>>,
            String,
        )>,
    > {
        self.collected_columns
            .into_iter()
            .map(|(array, field_name, field_type)| {
                let primitive_type = field_type
                    .as_primitive_type()
                    .ok_or_else(|| {
                        Error::new(ErrorKind::Unexpected, "field is not a primitive type")
                    })?
                    .clone();

                let lit_vec = arrow_primitive_to_literal(&array, &field_type)?;
                let datum_iterator: Box<dyn ExactSizeIterator<Item = Result<Option<Datum>>>> =
                    Box::new(lit_vec.into_iter().map(move |c| {
                        c.map(|literal| {
                            literal
                                .as_primitive_literal()
                                .map(|primitive_literal| {
                                    Datum::new(primitive_type.clone(), primitive_literal)
                                })
                                .ok_or(Error::new(
                                    ErrorKind::Unexpected,
                                    "failed to convert to primitive literal",
                                ))
                        })
                        .transpose()
                    }));

                Ok((datum_iterator, field_name))
            })
            .collect::<Result<Vec<_>>>()
    }
}

impl SchemaWithPartnerVisitor<ArrayRef> for EqDelColumnProcessor<'_> {
    type T = ();

    fn schema(&mut self, _schema: &Schema, _partner: &ArrayRef, _value: ()) -> Result<()> {
        Ok(())
    }

    fn field(&mut self, field: &NestedFieldRef, partner: &ArrayRef, _value: ()) -> Result<()> {
        if self.equality_ids.contains(&field.id) && field.field_type.as_primitive_type().is_some() {
            self.collected_columns.push((
                partner.clone(),
                field.name.clone(),
                field.field_type.as_ref().clone(),
            ));
        }
        Ok(())
    }

    fn r#struct(
        &mut self,
        _struct: &StructType,
        _partner: &ArrayRef,
        _results: Vec<()>,
    ) -> Result<()> {
        Ok(())
    }

    fn list(&mut self, _list: &ListType, _partner: &ArrayRef, _value: ()) -> Result<()> {
        Ok(())
    }

    fn map(
        &mut self,
        _map: &MapType,
        _partner: &ArrayRef,
        _key_value: (),
        _value: (),
    ) -> Result<()> {
        Ok(())
    }

    fn primitive(&mut self, _primitive: &PrimitiveType, _partner: &ArrayRef) -> Result<()> {
        Ok(())
    }

    fn variant(&mut self, _v: &VariantType, _partner: &ArrayRef) -> Result<()> {
        Ok(())
    }
}

struct EqDelRecordBatchPartnerAccessor;

impl PartnerAccessor<ArrayRef> for EqDelRecordBatchPartnerAccessor {
    fn struct_partner<'a>(&self, schema_partner: &'a ArrayRef) -> Result<&'a ArrayRef> {
        Ok(schema_partner)
    }

    fn field_partner<'a>(
        &self,
        struct_partner: &'a ArrayRef,
        field: &NestedField,
    ) -> Result<&'a ArrayRef> {
        let Some(struct_array) = struct_partner.as_any().downcast_ref::<StructArray>() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Expected struct array for field extraction",
            ));
        };

        // Find the field by name within the struct
        for (i, field_def) in struct_array.fields().iter().enumerate() {
            if field_def.name() == &field.name {
                return Ok(struct_array.column(i));
            }
        }

        Err(Error::new(
            ErrorKind::Unexpected,
            format!("Field {} not found in parent struct", field.name),
        ))
    }

    fn list_element_partner<'a>(&self, _list_partner: &'a ArrayRef) -> Result<&'a ArrayRef> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "List columns are unsupported in equality deletes",
        ))
    }

    fn map_key_partner<'a>(&self, _map_partner: &'a ArrayRef) -> Result<&'a ArrayRef> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Map columns are unsupported in equality deletes",
        ))
    }

    fn map_value_partner<'a>(&self, _map_partner: &'a ArrayRef) -> Result<&'a ArrayRef> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Map columns are unsupported in equality deletes",
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::sync::Arc;

    use arrow_array::cast::AsArray;
    use arrow_array::{
        ArrayRef, BinaryArray, Int32Array, Int64Array, RecordBatch, StringArray, StructArray,
    };
    use arrow_schema::{DataType, Field, Fields};
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use super::*;
    use crate::arrow::delete_filter::tests::setup;
    use crate::scan::FileScanTaskDeleteFile;
    use crate::spec::{DataContentType, Schema};
    use crate::test_utils::encode_dv_blob;

    #[tokio::test]
    async fn test_delete_file_loader_parse_equality_deletes() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().as_os_str().to_str().unwrap();
        let file_io = FileIO::new_with_fs();

        let eq_delete_file_path = setup_write_equality_delete_file_1(table_location);

        let basic_delete_file_loader =
            BasicDeleteFileLoader::new(file_io.clone(), ScanMetrics::new());
        let record_batch_stream = basic_delete_file_loader
            .parquet_to_batch_stream(
                &eq_delete_file_path,
                std::fs::metadata(&eq_delete_file_path).unwrap().len(),
                None,
            )
            .await
            .expect("could not get batch stream");

        let eq_ids = HashSet::from_iter(vec![2, 3, 4, 6, 8]);

        let parsed_eq_delete = CachingDeleteFileLoader::parse_equality_deletes_record_batch_stream(
            record_batch_stream,
            eq_ids,
        )
        .await
        .expect("error parsing batch stream");

        let expected = "((((((y IS NULL) OR (y != 1)) OR ((z IS NULL) OR (z != 100))) OR ((a IS NULL) OR (a != \"HELP\"))) OR ((sa IS NULL) OR (sa != 4))) OR ((b IS NULL) OR (b != 62696E6172795F64617461))) AND ((((((y IS NULL) OR (y != 2)) OR (z IS NOT NULL)) OR (a IS NOT NULL)) OR ((sa IS NULL) OR (sa != 5))) OR (b IS NOT NULL))".to_string();

        assert_eq!(parsed_eq_delete.to_string(), expected);
    }

    // An equality delete keyed on a nullable column must not delete rows whose value in that
    // column is null: per the Iceberg spec (Equality Delete Files), a null matches only a null
    // delete value. Mirrors Iceberg-Java's
    // TestSparkReaderDeletes.testEqualityDeleteWithSchemaEvolution.
    #[tokio::test]
    async fn test_equality_delete_predicate_preserves_null_rows() {
        let schema = Arc::new(arrow_schema::Schema::new(vec![simple_field(
            "status",
            DataType::Utf8,
            true,
            "3",
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![
                Arc::new(StringArray::from(vec![Some("INACTIVE")])) as ArrayRef,
            ])
            .unwrap();
        let stream: ArrowRecordBatchStream = futures::stream::iter(vec![Ok(batch)]).boxed();

        let predicate = CachingDeleteFileLoader::parse_equality_deletes_record_batch_stream(
            stream,
            HashSet::from_iter(vec![3]),
        )
        .await
        .expect("error parsing equality delete stream");

        assert_eq!(
            predicate.to_string(),
            "(status IS NULL) OR (status != \"INACTIVE\")"
        );
    }

    // A delete row with a null value in the column matches only rows whose value is null (Iceberg
    // spec, Equality Delete Files), so the keep predicate is `col IS NOT NULL`.
    #[tokio::test]
    async fn test_equality_delete_predicate_matches_null_delete_value() {
        let schema = Arc::new(arrow_schema::Schema::new(vec![simple_field(
            "status",
            DataType::Utf8,
            true,
            "3",
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec![
            None as Option<&str>,
        ])) as ArrayRef])
        .unwrap();
        let stream: ArrowRecordBatchStream = futures::stream::iter(vec![Ok(batch)]).boxed();

        let predicate = CachingDeleteFileLoader::parse_equality_deletes_record_batch_stream(
            stream,
            HashSet::from_iter(vec![3]),
        )
        .await
        .expect("error parsing equality delete stream");

        assert_eq!(predicate.to_string(), "status IS NOT NULL");
    }

    // A delete row with several equality columns keeps a data row that differs in any one of them,
    // so the per-column keep predicates are OR-ed.
    #[tokio::test]
    async fn test_equality_delete_predicate_multiple_columns() {
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            simple_field("id", DataType::Int64, true, "1"),
            simple_field("status", DataType::Utf8, true, "3"),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("X")])) as ArrayRef,
        ])
        .unwrap();
        let stream: ArrowRecordBatchStream = futures::stream::iter(vec![Ok(batch)]).boxed();

        let predicate = CachingDeleteFileLoader::parse_equality_deletes_record_batch_stream(
            stream,
            HashSet::from_iter(vec![1, 3]),
        )
        .await
        .expect("error parsing equality delete stream");

        assert_eq!(
            predicate.to_string(),
            "((id IS NULL) OR (id != 1)) OR ((status IS NULL) OR (status != \"X\"))"
        );
    }

    // A data row is kept only if it matches none of the delete rows, so the per-row keep
    // predicates are AND-ed.
    #[tokio::test]
    async fn test_equality_delete_predicate_multiple_delete_rows() {
        let schema = Arc::new(arrow_schema::Schema::new(vec![simple_field(
            "status",
            DataType::Utf8,
            true,
            "3",
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec![
            Some("A"),
            Some("B"),
        ])) as ArrayRef])
        .unwrap();
        let stream: ArrowRecordBatchStream = futures::stream::iter(vec![Ok(batch)]).boxed();

        let predicate = CachingDeleteFileLoader::parse_equality_deletes_record_batch_stream(
            stream,
            HashSet::from_iter(vec![3]),
        )
        .await
        .expect("error parsing equality delete stream");

        assert_eq!(
            predicate.to_string(),
            "((status IS NULL) OR (status != \"A\")) AND ((status IS NULL) OR (status != \"B\"))"
        );
    }

    /// Create a simple field with metadata.
    fn simple_field(name: &str, ty: DataType, nullable: bool, value: &str) -> Field {
        Field::new(name, ty, nullable).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            value.to_string(),
        )]))
    }

    fn setup_write_equality_delete_file_1(table_location: &str) -> String {
        let col_y_vals = vec![1, 2];
        let col_y = Arc::new(Int64Array::from(col_y_vals)) as ArrayRef;

        let col_z_vals = vec![Some(100), None];
        let col_z = Arc::new(Int64Array::from(col_z_vals)) as ArrayRef;

        let col_a_vals = vec![Some("HELP"), None];
        let col_a = Arc::new(StringArray::from(col_a_vals)) as ArrayRef;

        let col_s = Arc::new(StructArray::from(vec![
            (
                Arc::new(simple_field("sa", DataType::Int32, false, "6")),
                Arc::new(Int32Array::from(vec![4, 5])) as ArrayRef,
            ),
            (
                Arc::new(simple_field("sb", DataType::Utf8, true, "7")),
                Arc::new(StringArray::from(vec![Some("x"), None])) as ArrayRef,
            ),
        ]));

        let col_b_vals = vec![Some(&b"binary_data"[..]), None];
        let col_b = Arc::new(BinaryArray::from(col_b_vals)) as ArrayRef;

        let equality_delete_schema = {
            let struct_field = DataType::Struct(Fields::from(vec![
                simple_field("sa", DataType::Int32, false, "6"),
                simple_field("sb", DataType::Utf8, true, "7"),
            ]));

            let fields = vec![
                Field::new("y", DataType::Int64, true).with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "2".to_string(),
                )])),
                Field::new("z", DataType::Int64, true).with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "3".to_string(),
                )])),
                Field::new("a", DataType::Utf8, true).with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "4".to_string(),
                )])),
                simple_field("s", struct_field, false, "5"),
                simple_field("b", DataType::Binary, true, "8"),
            ];
            Arc::new(arrow_schema::Schema::new(fields))
        };

        let equality_deletes_to_write = RecordBatch::try_new(equality_delete_schema.clone(), vec![
            col_y, col_z, col_a, col_s, col_b,
        ])
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

    #[tokio::test]
    async fn test_caching_delete_file_loader_load_deletes() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path();
        let file_io = FileIO::new_with_fs();

        let delete_file_loader =
            CachingDeleteFileLoader::new(file_io.clone(), 10, Runtime::current());

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

    #[tokio::test]
    async fn test_parse_positional_deletes_rejects_negative_positions() {
        let schema = crate::arrow::delete_filter::tests::create_pos_del_schema();
        let file_path_col = Arc::new(StringArray::from_iter_values(vec!["data.parquet"]));
        let pos_col = Arc::new(Int64Array::from_iter_values(vec![-1i64]));
        let batch = RecordBatch::try_new(schema, vec![file_path_col, pos_col]).unwrap();
        let stream = futures::stream::iter(vec![Ok(batch)]).boxed();

        let err = CachingDeleteFileLoader::parse_positional_deletes_record_batch_stream(stream)
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("negative position"));
    }

    fn sorted_positions(dv: &DeleteVector) -> Vec<u64> {
        let mut positions: Vec<u64> = dv.iter().collect();
        positions.sort_unstable();
        positions
    }

    /// Spec-compliant input: rows sorted by (file_path, pos). Exercises the
    /// common shape: multi-position runs, several files in one batch, and a path
    /// "b" whose positions span two batches, so its two runs merge into one
    /// delete vector.
    #[tokio::test]
    async fn test_parse_positional_deletes_merges_sorted_runs() {
        let schema = crate::arrow::delete_filter::tests::create_pos_del_schema();

        let batch1 = RecordBatch::try_new(schema.clone(), vec![
            Arc::new(StringArray::from_iter_values(vec!["a", "a", "a", "b"])),
            Arc::new(Int64Array::from_iter_values(vec![1i64, 3, 5, 2])),
        ])
        .unwrap();
        let batch2 = RecordBatch::try_new(schema, vec![
            Arc::new(StringArray::from_iter_values(vec!["b", "c"])),
            Arc::new(Int64Array::from_iter_values(vec![4i64, 0])),
        ])
        .unwrap();
        let stream = futures::stream::iter(vec![Ok(batch1), Ok(batch2)]).boxed();

        let result = CachingDeleteFileLoader::parse_positional_deletes_record_batch_stream(stream)
            .await
            .unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(sorted_positions(&result["a"]), vec![1, 3, 5]);
        assert_eq!(sorted_positions(&result["b"]), vec![2, 4]);
        assert_eq!(sorted_positions(&result["c"]), vec![0]);
    }

    /// Deliberately unsorted input. The spec requires position delete rows to be
    /// sorted by (file_path, pos), but the reader must not depend on it: run
    /// buffering only groups *contiguous* rows, so a path split into
    /// non-contiguous runs (here "a" before and after "b") must still merge into
    /// a single delete vector rather than silently dropping positions.
    #[tokio::test]
    async fn test_parse_positional_deletes_merges_spec_noncompliant_unsorted_runs() {
        let schema = crate::arrow::delete_filter::tests::create_pos_del_schema();

        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(StringArray::from_iter_values(vec!["a", "b", "a"])),
            Arc::new(Int64Array::from_iter_values(vec![3i64, 2, 1])),
        ])
        .unwrap();
        let stream = futures::stream::iter(vec![Ok(batch)]).boxed();

        let result = CachingDeleteFileLoader::parse_positional_deletes_record_batch_stream(stream)
            .await
            .unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(sorted_positions(&result["a"]), vec![1, 3]);
        assert_eq!(sorted_positions(&result["b"]), vec![2]);
    }

    /// Cross-batch overlap: batch2 carries lower positions for "a" than batch1
    /// already recorded, so the run fails the append precondition (every value
    /// must exceed all recorded) and drops to the per-position fallback. All
    /// positions must still merge.
    #[tokio::test]
    async fn test_parse_positional_deletes_merges_overlapping_cross_batch_runs() {
        let schema = crate::arrow::delete_filter::tests::create_pos_del_schema();

        let batch1 = RecordBatch::try_new(schema.clone(), vec![
            Arc::new(StringArray::from_iter_values(vec!["a", "a"])),
            Arc::new(Int64Array::from_iter_values(vec![5i64, 10])),
        ])
        .unwrap();
        let batch2 = RecordBatch::try_new(schema, vec![
            Arc::new(StringArray::from_iter_values(vec!["a", "a"])),
            Arc::new(Int64Array::from_iter_values(vec![3i64, 7])),
        ])
        .unwrap();
        let stream = futures::stream::iter(vec![Ok(batch1), Ok(batch2)]).boxed();

        let result = CachingDeleteFileLoader::parse_positional_deletes_record_batch_stream(stream)
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(sorted_positions(&result["a"]), vec![3, 5, 7, 10]);
    }

    /// Spec-compliant duplicate positions: sorted by (file_path, pos) is only
    /// non-decreasing, so a repeated position is valid input. It fails the
    /// strictly-ascending append precondition and exercises the fallback, whose
    /// idempotent inserts collapse the duplicate.
    #[tokio::test]
    async fn test_parse_positional_deletes_merges_duplicate_positions() {
        let schema = crate::arrow::delete_filter::tests::create_pos_del_schema();

        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(StringArray::from_iter_values(vec!["a", "a", "a"])),
            Arc::new(Int64Array::from_iter_values(vec![3i64, 3, 7])),
        ])
        .unwrap();
        let stream = futures::stream::iter(vec![Ok(batch)]).boxed();

        let result = CachingDeleteFileLoader::parse_positional_deletes_record_batch_stream(stream)
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(sorted_positions(&result["a"]), vec![3, 7]);
    }

    /// Verifies that evolve_schema on partial-schema equality deletes works correctly
    /// when only equality_ids columns are evolved, not all table columns.
    ///
    /// Per the [Iceberg spec](https://iceberg.apache.org/spec/#equality-delete-files),
    /// equality delete files can contain only a subset of columns.
    #[tokio::test]
    async fn test_partial_schema_equality_deletes_evolve_succeeds() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().as_os_str().to_str().unwrap();

        // Create table schema with REQUIRED fields
        let table_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "data", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap(),
        );

        // Write equality delete file with PARTIAL schema (only 'data' column)
        let delete_file_path = {
            let data_vals = vec!["a", "d", "g"];
            let data_col = Arc::new(StringArray::from(data_vals)) as ArrayRef;

            let delete_schema = Arc::new(arrow_schema::Schema::new(vec![simple_field(
                "data",
                DataType::Utf8,
                false,
                "2", // field ID
            )]));

            let delete_batch = RecordBatch::try_new(delete_schema.clone(), vec![data_col]).unwrap();

            let path = format!("{}/partial-eq-deletes.parquet", &table_location);
            let file = File::create(&path).unwrap();
            let props = WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build();
            let mut writer =
                ArrowWriter::try_new(file, delete_batch.schema(), Some(props)).unwrap();
            writer.write(&delete_batch).expect("Writing batch");
            writer.close().unwrap();
            path
        };

        let file_io = FileIO::new_with_fs();
        let basic_delete_file_loader =
            BasicDeleteFileLoader::new(file_io.clone(), ScanMetrics::new());

        let batch_stream = basic_delete_file_loader
            .parquet_to_batch_stream(
                &delete_file_path,
                std::fs::metadata(&delete_file_path).unwrap().len(),
                None,
            )
            .await
            .unwrap();

        // Only evolve the equality_ids columns (field 2), not all table columns
        let equality_ids = vec![2];
        let evolved_stream =
            BasicDeleteFileLoader::evolve_schema(batch_stream, table_schema, &equality_ids)
                .await
                .unwrap();

        let result = evolved_stream.try_collect::<Vec<_>>().await;

        assert!(
            result.is_ok(),
            "Expected success when evolving only equality_ids columns, got error: {:?}",
            result.err()
        );

        let batches = result.unwrap();
        assert_eq!(batches.len(), 1);

        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 1); // Only 'data' column

        // Verify the actual values are preserved after schema evolution
        let data_col = batch.column(0).as_string::<i32>();
        assert_eq!(data_col.value(0), "a");
        assert_eq!(data_col.value(1), "d");
        assert_eq!(data_col.value(2), "g");
    }

    /// Test loading a FileScanTask with BOTH positional and equality deletes.
    /// Verifies the fix for the inverted condition that caused "Missing predicate for equality delete file" errors.
    #[tokio::test]
    async fn test_load_deletes_with_mixed_types() {
        use crate::scan::FileScanTask;
        use crate::spec::{DataFileFormat, Schema};

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path();
        let file_io = FileIO::new_with_fs();

        // Create the data file schema
        let data_file_schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(2, "y", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(3, "z", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap(),
        );

        // Write positional delete file
        let positional_delete_schema = crate::arrow::delete_filter::tests::create_pos_del_schema();
        let file_path_values =
            vec![format!("{}/data-1.parquet", table_location.to_str().unwrap()); 4];
        let file_path_col = Arc::new(StringArray::from_iter_values(&file_path_values));
        let pos_col = Arc::new(Int64Array::from_iter_values(vec![0i64, 1, 2, 3]));

        let positional_deletes_to_write =
            RecordBatch::try_new(positional_delete_schema.clone(), vec![
                file_path_col,
                pos_col,
            ])
            .unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let pos_del_path = format!("{}/pos-del-mixed.parquet", table_location.to_str().unwrap());
        let file = File::create(&pos_del_path).unwrap();
        let mut writer = ArrowWriter::try_new(
            file,
            positional_deletes_to_write.schema(),
            Some(props.clone()),
        )
        .unwrap();
        writer.write(&positional_deletes_to_write).unwrap();
        writer.close().unwrap();

        // Write equality delete file
        let eq_delete_path = setup_write_equality_delete_file_1(table_location.to_str().unwrap());

        // Create FileScanTask with BOTH positional and equality deletes
        let pos_del = FileScanTaskDeleteFile::builder()
            .with_file_path(pos_del_path.clone())
            .with_file_size_in_bytes(std::fs::metadata(&pos_del_path).unwrap().len())
            .with_file_type(DataContentType::PositionDeletes)
            .with_file_format(DataFileFormat::Parquet)
            .with_partition_spec_id(0)
            .build();

        let eq_del = FileScanTaskDeleteFile::builder()
            .with_file_path(eq_delete_path.clone())
            .with_file_size_in_bytes(std::fs::metadata(&eq_delete_path).unwrap().len())
            .with_file_type(DataContentType::EqualityDeletes)
            .with_file_format(DataFileFormat::Parquet)
            .with_partition_spec_id(0)
            .with_equality_ids(Some(vec![2, 3])) // Only use field IDs that exist in both schemas
            .build();

        let file_scan_task = FileScanTask::builder()
            .with_file_size_in_bytes(0)
            .with_start(0)
            .with_length(0)
            .with_data_file_path(format!(
                "{}/data-1.parquet",
                table_location.to_str().unwrap()
            ))
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(data_file_schema.clone())
            .with_project_field_ids(vec![2, 3])
            .with_deletes(vec![pos_del, eq_del])
            .with_case_sensitive(false)
            .build();

        // Load the deletes - should handle both types without error
        let delete_file_loader =
            CachingDeleteFileLoader::new(file_io.clone(), 10, Runtime::current());
        let delete_filter = delete_file_loader
            .load_deletes(&file_scan_task.deletes, file_scan_task.schema_ref())
            .await
            .unwrap()
            .unwrap();

        // Verify both delete types can be processed together
        let result = delete_filter
            .build_equality_delete_predicate(&file_scan_task)
            .await;
        assert!(
            result.is_ok(),
            "Failed to build equality delete predicate: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn test_large_equality_delete_batch_stack_overflow() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().as_os_str().to_str().unwrap();
        let file_io = FileIO::new_with_fs();

        // Create a large batch of equality deletes
        let num_rows = 20_000;
        let col_y_vals: Vec<i64> = (0..num_rows).collect();
        let col_y = Arc::new(Int64Array::from(col_y_vals)) as ArrayRef;

        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("y", DataType::Int64, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
        ]));

        let record_batch = RecordBatch::try_new(schema.clone(), vec![col_y]).unwrap();

        // Write to file
        let path = format!("{}/large-eq-deletes.parquet", &table_location);
        let file = File::create(&path).unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        writer.write(&record_batch).unwrap();
        writer.close().unwrap();

        let basic_delete_file_loader =
            BasicDeleteFileLoader::new(file_io.clone(), ScanMetrics::new());
        let record_batch_stream = basic_delete_file_loader
            .parquet_to_batch_stream(&path, std::fs::metadata(&path).unwrap().len(), None)
            .await
            .expect("could not get batch stream");

        let eq_ids = HashSet::from_iter(vec![2]);

        let result = CachingDeleteFileLoader::parse_equality_deletes_record_batch_stream(
            record_batch_stream,
            eq_ids,
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_caching_delete_file_loader_caches_results() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path();
        let file_io = FileIO::new_with_fs();

        let delete_file_loader =
            CachingDeleteFileLoader::new(file_io.clone(), 10, Runtime::current());

        let file_scan_tasks = setup(table_location);

        // Load deletes for the first time
        let delete_filter_1 = delete_file_loader
            .load_deletes(&file_scan_tasks[0].deletes, file_scan_tasks[0].schema_ref())
            .await
            .unwrap()
            .unwrap();

        // Load deletes for the second time (same task/files)
        let delete_filter_2 = delete_file_loader
            .load_deletes(&file_scan_tasks[0].deletes, file_scan_tasks[0].schema_ref())
            .await
            .unwrap()
            .unwrap();

        let dv1 = delete_filter_1
            .get_delete_vector(&file_scan_tasks[0])
            .unwrap();
        let dv2 = delete_filter_2
            .get_delete_vector(&file_scan_tasks[0])
            .unwrap();

        // Verify that the delete vectors point to the same memory location,
        // confirming that the second load reused the result from the first.
        assert!(Arc::ptr_eq(&dv1, &dv2));
    }

    fn dv_task(
        dv_path: String,
        file_size: u64,
        data_file_path: String,
        content_offset: i64,
        content_size: i64,
        record_count: u64,
        key_metadata: Option<Box<[u8]>>,
    ) -> FileScanTaskDeleteFile {
        FileScanTaskDeleteFile::builder()
            .with_file_path(dv_path)
            .with_file_size_in_bytes(file_size)
            .with_file_type(DataContentType::PositionDeletes)
            .with_file_format(DataFileFormat::Puffin)
            .with_partition_spec_id(0)
            .with_referenced_data_file(Some(data_file_path))
            .with_content_offset(Some(content_offset))
            .with_content_size_in_bytes(Some(content_size))
            .with_record_count(Some(record_count))
            .with_key_metadata(key_metadata)
            .build()
    }

    #[tokio::test]
    async fn test_load_deletes_applies_deletion_vector() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let blob = encode_dv_blob([0u64, 1, 5]);

        // Embed the blob in a Puffin-like file behind leading bytes so content_offset is
        // non-zero, then let the loader read it back by range.
        let content_offset = 12i64;
        let content_size = blob.len() as i64;
        let mut file_bytes = vec![0u8; content_offset as usize];
        file_bytes.extend_from_slice(&blob);
        file_bytes.extend_from_slice(&[0u8; 8]);
        let dv_path = format!("{table_location}/deletes.puffin");
        std::fs::write(&dv_path, &file_bytes).unwrap();

        let data_file_path = format!("{table_location}/data-1.parquet");
        let dv = dv_task(
            dv_path.clone(),
            std::fs::metadata(&dv_path).unwrap().len(),
            data_file_path.clone(),
            content_offset,
            content_size,
            3,
            None,
        );

        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "x", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap(),
        );

        let loader = CachingDeleteFileLoader::new(file_io, 10, Runtime::current());
        let delete_filter = loader.load_deletes(&[dv], schema).await.unwrap().unwrap();

        let delete_vector = delete_filter
            .get_delete_vector_for_path(&data_file_path)
            .expect("a delete vector should be indexed for the referenced data file");
        let mut positions: Vec<u64> = delete_vector.lock().unwrap().iter().collect();
        positions.sort_unstable();
        assert_eq!(positions, vec![0, 1, 5]);
    }

    #[tokio::test]
    async fn test_load_deletes_decrypts_deletion_vector() {
        use crate::encryption::{EncryptedOutputFile, StandardKeyMetadata};

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let key_metadata = StandardKeyMetadata::try_new(b"0123456789abcdef")
            .unwrap()
            .with_aad_prefix(b"test-aad-prefix!");
        let encoded_key_metadata = key_metadata.encode().unwrap();

        let blob = encode_dv_blob([2u64, 4]);
        let plaintext_size = blob.len() as i64;
        let dv_path = format!("{table_location}/deletes.puffin");
        let output = EncryptedOutputFile::new(file_io.new_output(&dv_path).unwrap(), key_metadata);
        output.write(Bytes::from(blob)).await.unwrap();

        // content_offset / content_size_in_bytes are in the plaintext coordinate space, distinct
        // from the ciphertext's on-disk size (header, nonce, and tag overhead).
        let file_size = std::fs::metadata(&dv_path).unwrap().len();
        let data_file_path = format!("{table_location}/data-1.parquet");
        let dv = dv_task(
            dv_path.clone(),
            file_size,
            data_file_path.clone(),
            0,
            plaintext_size,
            2,
            Some(encoded_key_metadata),
        );

        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "x", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap(),
        );

        let loader = CachingDeleteFileLoader::new(file_io, 10, Runtime::current());
        let delete_filter = loader.load_deletes(&[dv], schema).await.unwrap().unwrap();

        let delete_vector = delete_filter
            .get_delete_vector_for_path(&data_file_path)
            .expect("a delete vector should be indexed for the referenced data file");
        let mut positions: Vec<u64> = delete_vector.lock().unwrap().iter().collect();
        positions.sort_unstable();
        assert_eq!(positions, vec![2, 4]);
    }

    #[tokio::test]
    async fn test_load_deletes_rejects_deletion_vector_cardinality_mismatch() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let blob = encode_dv_blob([0u64, 1, 5]);
        let dv_path = format!("{table_location}/deletes.puffin");
        std::fs::write(&dv_path, &blob).unwrap();

        let data_file_path = format!("{table_location}/data-1.parquet");
        // record_count says 2 positions, but the blob decodes to 3.
        let dv = dv_task(
            dv_path.clone(),
            std::fs::metadata(&dv_path).unwrap().len(),
            data_file_path,
            0,
            blob.len() as i64,
            2,
            None,
        );

        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "x", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap(),
        );

        let loader = CachingDeleteFileLoader::new(file_io, 10, Runtime::current());
        let err = loader
            .load_deletes(&[dv], schema)
            .await
            .unwrap()
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("expected 2 from record_count"));
    }

    // A well-formed deletion-vector task, for tests that then clear or corrupt one field.
    fn valid_dv_task() -> FileScanTaskDeleteFile {
        dv_task(
            "deletes.puffin".to_string(),
            100,
            "data.parquet".to_string(),
            4,
            40,
            2,
            None,
        )
    }

    #[test]
    fn test_validate_deletion_vector_task_rejects_missing_content_offset() {
        let mut task = valid_dv_task();
        task.content_offset = None;

        let err = CachingDeleteFileLoader::validate_deletion_vector_task(&task).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("missing content_offset"));
    }

    #[test]
    fn test_validate_deletion_vector_task_rejects_missing_content_size() {
        let mut task = valid_dv_task();
        task.content_size_in_bytes = None;

        let err = CachingDeleteFileLoader::validate_deletion_vector_task(&task).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("missing content_size_in_bytes"));
    }

    #[test]
    fn test_validate_deletion_vector_task_rejects_missing_referenced_data_file() {
        let mut task = valid_dv_task();
        task.referenced_data_file = None;

        let err = CachingDeleteFileLoader::validate_deletion_vector_task(&task).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("missing referenced_data_file"));
    }

    #[test]
    fn test_validate_deletion_vector_task_rejects_missing_record_count() {
        let mut task = valid_dv_task();
        task.record_count = None;

        let err = CachingDeleteFileLoader::validate_deletion_vector_task(&task).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("missing record_count"));
    }

    #[test]
    fn test_validate_deletion_vector_task_rejects_negative_content_offset() {
        let mut task = valid_dv_task();
        task.content_offset = Some(-1);

        let err = CachingDeleteFileLoader::validate_deletion_vector_task(&task).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("negative content_offset"));
    }

    #[test]
    fn test_validate_deletion_vector_task_rejects_negative_content_size() {
        let mut task = valid_dv_task();
        task.content_size_in_bytes = Some(-1);

        let err = CachingDeleteFileLoader::validate_deletion_vector_task(&task).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("negative content_size_in_bytes"));
    }

    #[test]
    fn test_validate_deletion_vector_task_accepts_valid_coordinates() {
        let (start, len, data_file_path, record_count) =
            CachingDeleteFileLoader::validate_deletion_vector_task(&valid_dv_task()).unwrap();
        assert_eq!(start, 4);
        assert_eq!(len, 40);
        assert_eq!(data_file_path, "data.parquet");
        assert_eq!(record_count, 2);
    }

    #[test]
    fn test_validate_deletion_vector_cardinality_accepts_matching_count() {
        let mut dv = DeleteVector::default();
        dv.insert(1);
        dv.insert(2);

        CachingDeleteFileLoader::validate_deletion_vector_cardinality(&dv, 2, "deletes.puffin")
            .unwrap();
    }

    #[test]
    fn test_validate_deletion_vector_cardinality_rejects_mismatched_count() {
        let mut dv = DeleteVector::default();
        dv.insert(1);

        let err =
            CachingDeleteFileLoader::validate_deletion_vector_cardinality(&dv, 2, "deletes.puffin")
                .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("expected 2 from record_count"));
    }
}
