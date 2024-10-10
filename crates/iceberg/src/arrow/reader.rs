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

//! Parquet file data reader

use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::str::FromStr;
use std::sync::Arc;

use arrow_arith::boolean::{and, is_not_null, is_null, not, or};
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch};
use arrow_ord::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow_schema::{ArrowError, DataType, SchemaRef as ArrowSchemaRef};
use arrow_string::like::starts_with;
use bytes::Bytes;
use fnv::FnvHashSet;
use futures::channel::mpsc::{channel, Sender};
use futures::future::BoxFuture;
use futures::{try_join, FutureExt, SinkExt, StreamExt, TryFutureExt, TryStreamExt};
use parquet::arrow::arrow_reader::{
    ArrowPredicateFn, ArrowReaderOptions, RowFilter, RowSelection, RowSelector,
};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask, PARQUET_FIELD_ID_META_KEY};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use parquet::schema::types::{SchemaDescriptor, Type as ParquetType};

use crate::arrow::record_batch_transformer::RecordBatchTransformer;
use crate::arrow::{arrow_schema_to_schema, get_arrow_datum};
use crate::error::Result;
use crate::expr::visitors::bound_predicate_visitor::{visit, BoundPredicateVisitor};
use crate::expr::visitors::page_index_evaluator::PageIndexEvaluator;
use crate::expr::visitors::row_group_metrics_evaluator::RowGroupMetricsEvaluator;
use crate::expr::{BoundPredicate, BoundReference};
use crate::io::{FileIO, FileMetadata, FileRead};
use crate::runtime::spawn;
use crate::scan::{
    ArrowRecordBatchStream, FileScanTask, FileScanTaskDeleteFile, FileScanTaskStream,
};
use crate::spec::{
    parse_equality_delete_file, parse_positional_delete_file, DataContentType, Datum, Deletes,
    Schema,
};
use crate::utils::available_parallelism;
use crate::{Error, ErrorKind};

/// Builder to create ArrowReader
pub struct ArrowReaderBuilder {
    batch_size: Option<usize>,
    file_io: FileIO,
    concurrency_limit_data_files: usize,
    row_group_filtering_enabled: bool,
    row_selection_enabled: bool,
}

impl ArrowReaderBuilder {
    /// Create a new ArrowReaderBuilder
    pub(crate) fn new(file_io: FileIO) -> Self {
        let num_cpus = available_parallelism().get();

        ArrowReaderBuilder {
            batch_size: None,
            file_io,
            concurrency_limit_data_files: num_cpus,
            row_group_filtering_enabled: true,
            row_selection_enabled: false,
        }
    }

    /// Sets the max number of in flight data files that are being fetched
    pub fn with_data_file_concurrency_limit(mut self, val: usize) -> Self {
        self.concurrency_limit_data_files = val;
        self
    }

    /// Sets the desired size of batches in the response
    /// to something other than the default
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    /// Determines whether to enable row group filtering.
    pub fn with_row_group_filtering_enabled(mut self, row_group_filtering_enabled: bool) -> Self {
        self.row_group_filtering_enabled = row_group_filtering_enabled;
        self
    }

    /// Determines whether to enable row selection.
    pub fn with_row_selection_enabled(mut self, row_selection_enabled: bool) -> Self {
        self.row_selection_enabled = row_selection_enabled;
        self
    }

    /// Build the ArrowReader.
    pub fn build(self) -> ArrowReader {
        ArrowReader {
            batch_size: self.batch_size,
            file_io: self.file_io,
            concurrency_limit_data_files: self.concurrency_limit_data_files,
            row_group_filtering_enabled: self.row_group_filtering_enabled,
            row_selection_enabled: self.row_selection_enabled,
        }
    }
}

/// Reads data from Parquet files
#[derive(Clone)]
pub struct ArrowReader {
    batch_size: Option<usize>,
    file_io: FileIO,

    /// the maximum number of data files that can be fetched at the same time
    concurrency_limit_data_files: usize,

    row_group_filtering_enabled: bool,
    row_selection_enabled: bool,
}

impl ArrowReader {
    /// Take a stream of FileScanTasks and reads all the files.
    /// Returns a stream of Arrow RecordBatches containing the data from the files
    pub fn read(self, tasks: FileScanTaskStream) -> Result<ArrowRecordBatchStream> {
        let file_io = self.file_io.clone();
        let batch_size = self.batch_size;
        let concurrency_limit_data_files = self.concurrency_limit_data_files;
        let row_group_filtering_enabled = self.row_group_filtering_enabled;
        let row_selection_enabled = self.row_selection_enabled;

        let (tx, rx) = channel(concurrency_limit_data_files);
        let mut channel_for_error = tx.clone();

        spawn(async move {
            let result = tasks
                .map(|task| Ok((task, file_io.clone(), tx.clone())))
                .try_for_each_concurrent(
                    concurrency_limit_data_files,
                    |(file_scan_task, file_io, tx)| async move {
                        match file_scan_task {
                            Ok(task) => {
                                let file_path = task.data_file_path.to_string();

                                spawn(async move {
                                    Self::process_file_scan_task(
                                        task,
                                        batch_size,
                                        file_io,
                                        tx,
                                        row_group_filtering_enabled,
                                        row_selection_enabled,
                                    )
                                    .await
                                })
                                .await
                                .map_err(|e| e.with_context("file_path", file_path))
                            }
                            Err(err) => Err(err),
                        }
                    },
                )
                .await;

            if let Err(error) = result {
                let _ = channel_for_error.send(Err(error)).await;
            }
        });

        return Ok(rx.boxed());
    }

    // retrieve all delete files concurrently from FileIO and parse them
    // into `Deletes` objects
    async fn get_deletes(
        delete_file_entries: Option<Arc<Vec<FileScanTaskDeleteFile>>>,
        file_io: FileIO,
        concurrency_limit_data_files: usize,
    ) -> Result<Option<Vec<Deletes>>> {
        let Some(delete_file_entries) = delete_file_entries else {
            return Ok(None);
        };

        let (tx, rx) = channel(concurrency_limit_data_files);
        let mut channel_for_error = tx.clone();

        spawn(async move {
            #[allow(clippy::redundant_closure)] // clippy's recommendation fails to compile
            let result =
                futures::stream::iter(delete_file_entries.iter().map(|df| crate::Result::Ok(df)))
                    .try_for_each_concurrent(concurrency_limit_data_files, |entry| {
                        let file_io = file_io.clone();
                        let mut tx = tx.clone();
                        async move {
                            let FileScanTaskDeleteFile {
                                ref file_path,
                                file_type,
                            } = entry;

                            let record_batch_stream =
                                Self::create_parquet_record_batch_stream_builder(
                                    file_path,
                                    file_io.clone(),
                                    false,
                                )
                                .await?
                                .build()?
                                .map(|item| match item {
                                    Ok(val) => Ok(val),
                                    Err(err) => {
                                        Err(Error::new(ErrorKind::DataInvalid, err.to_string())
                                            .with_source(err))
                                    }
                                })
                                .boxed();

                            let result = match file_type {
                                DataContentType::PositionDeletes => {
                                    parse_positional_delete_file(record_batch_stream).await
                                }
                                DataContentType::EqualityDeletes => {
                                    parse_equality_delete_file(record_batch_stream).await
                                }
                                _ => Err(Error::new(
                                    ErrorKind::Unexpected,
                                    "Expected equality or positional delete",
                                )),
                            }?;

                            tx.send(Ok(result)).await?;
                            Ok(())
                        }
                    })
                    .await;

            if let Err(error) = result {
                let _ = channel_for_error.send(Err(error)).await;
            }
        });

        let results = rx.try_collect::<Vec<_>>().await?;
        if results.is_empty() {
            Ok(None)
        } else {
            Ok(Some(results))
        }
    }

    fn get_positional_delete_indexes(
        data_file_path: &str,
        deletes: &[Deletes],
    ) -> Option<Vec<usize>> {
        // TODO: refactor to avoid allocating vec if no results

        let mut results = vec![];
        deletes.iter().for_each(|d| {
            if let Deletes::Positional(map) = d {
                if let Some(indices) = map.get(data_file_path) {
                    results.extend(indices.iter().map(|&i| i as usize));
                }
            }
        });

        if results.is_empty() {
            None
        } else {
            Some(results)
        }
    }

    fn get_equality_deletes(_delete_files: &[Deletes]) -> Result<Option<BoundPredicate>> {
        let result = None;

        for delete_file in _delete_files {
            let Deletes::Equality(_delete_file_batches) = delete_file else {
                continue;
            };

            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Equality delete files are not yet supported.",
            ));

            // TODO:
            //      * set `file_predicate` = AlwaysTrue
            //      * for each row in the file:
            //          * for each cell in the row:
            //              * create a predicate of the form `field` = `val`
            //                  where `field` is the column name and `val` is the value
            //                  of the cell
            //              * Bind this predicate to the table schema to get a `BoundPredicate`
            //              * set file_predicate = file_predicate.and(bound_predicate_for_cell)
            //      * set result = result.or(file_predicate)
        }

        Ok(result)
    }

    async fn process_file_scan_task(
        task: FileScanTask,
        batch_size: Option<usize>,
        file_io: FileIO,
        mut tx: Sender<Result<RecordBatch>>,
        row_group_filtering_enabled: bool,
        row_selection_enabled: bool,
    ) -> Result<()> {
        // start gathering the delete files
        let delete_file_tasks = task.deletes.clone();
        let delete_files_fut = spawn({
            let file_io = file_io.clone();
            async move { Self::get_deletes(delete_file_tasks, file_io, 5).await }
        });

        let should_load_page_index =
            (row_selection_enabled && task.predicate.is_some()) || task.deletes.is_some();
        let mut record_batch_stream_builder = Self::create_parquet_record_batch_stream_builder(
            &task.data_file_path,
            file_io,
            should_load_page_index,
        )
        .await?;

        // Create a projection mask for the batch stream to select which columns in the
        // Parquet file that we want in the response
        let projection_mask = Self::get_arrow_projection_mask(
            &task.project_field_ids,
            &task.schema,
            record_batch_stream_builder.parquet_schema(),
            record_batch_stream_builder.schema(),
        )?;
        record_batch_stream_builder = record_batch_stream_builder.with_projection(projection_mask);

        // RecordBatchTransformer performs any required transformations on the RecordBatches
        // that come back from the file, such as type promotion, default column insertion
        // and column re-ordering
        let mut record_batch_transformer =
            RecordBatchTransformer::build(task.schema_ref(), task.project_field_ids());

        if let Some(batch_size) = batch_size {
            record_batch_stream_builder = record_batch_stream_builder.with_batch_size(batch_size);
        }

        let delete_files = delete_files_fut.await?;
        let delete_predicate = if let Some(ref delete_files) = delete_files {
            Self::get_equality_deletes(delete_files)?
        } else {
            None
        };

        // As well as the optional predicate supplied in the FileScanTask,
        // if there are any equality delete files that apply to this file,
        // we also need to construct a predicate that will implement the filtering
        // that they require. If both are present, we logical-AND them together
        // to form a single filter predicate that we can pass to
        // Arrow's RecordBatchStreamBuilder.
        let final_predicate = match (&task.predicate, delete_predicate) {
            (None, None) => None,
            (Some(predicate), None) => Some(predicate.clone()),
            (None, Some(ref predicate)) => Some(predicate.clone()),
            (Some(filter_predicate), Some(delete_predicate)) => {
                Some(filter_predicate.clone().and(delete_predicate.clone()))
            }
        };

        // Just as above with the row filter, there are two possible sources
        // for potential lists of selected RowGroup indices and `RowSelection`s.
        // Lists of selected RowGroup indices can come from two sources:
        //   * As above, when there are equality delete files that are applicable;
        //   * When there is a scan predicate
        //  and row_group_filtering_enabled = true.
        // `RowSelection`s can be created in either or both of the following cases:
        //   * When there are positional delete files that are applicable;
        //   * When there is a scan predicate and row_selection_enabled = true
        // Note that, in the former case we only perform row group filtering when
        // there is a scan predicate AND row_group_filtering_enabled = true,
        // but we perform row selection filtering if there are applicable
        // equality delete files OR (there is a scan predicate AND row_selection_enabled),
        // since it is expected that the mos efficient way to apply positional deletes will
        // always be by using a `RowSelection`.
        let mut selected_row_group_indices = None;
        let mut row_selection = None;

        if let Some(predicate) = final_predicate {
            let (iceberg_field_ids, field_id_map) = Self::build_field_id_set_and_map(
                record_batch_stream_builder.parquet_schema(),
                &predicate,
            )?;

            let row_filter = Self::get_row_filter(
                &predicate,
                record_batch_stream_builder.parquet_schema(),
                &iceberg_field_ids,
                &field_id_map,
            )?;
            record_batch_stream_builder = record_batch_stream_builder.with_row_filter(row_filter);

            if row_group_filtering_enabled {
                let result = Self::get_selected_row_group_indices(
                    &predicate,
                    record_batch_stream_builder.metadata(),
                    &field_id_map,
                    &task.schema,
                )?;

                selected_row_group_indices = Some(result);
            }

            if row_selection_enabled {
                row_selection = Some(Self::get_predicate_row_selection(
                    &predicate,
                    record_batch_stream_builder.metadata(),
                    &selected_row_group_indices,
                    &field_id_map,
                    &task.schema,
                )?);
            }
        }

        let positional_delete_indexes = if let Some(ref delete_files) = delete_files {
            Self::get_positional_delete_indexes(&task.data_file_path, delete_files)
        } else {
            None
        };

        if let Some(positional_delete_indexes) = positional_delete_indexes {
            let delete_row_selection = Self::get_deleted_row_selection(
                record_batch_stream_builder.metadata(),
                &selected_row_group_indices,
                &positional_delete_indexes,
            )?;

            // merge the row selection from the delete files with the row selection
            // from the filter predicate, if there is one from the filter predicate
            row_selection = match row_selection {
                None => Some(delete_row_selection),
                Some(filter_row_selection) => {
                    Some(filter_row_selection.intersection(&delete_row_selection))
                }
            };
        }

        if let Some(row_selection) = row_selection {
            record_batch_stream_builder =
                record_batch_stream_builder.with_row_selection(row_selection);
        }

        if let Some(selected_row_group_indices) = selected_row_group_indices {
            record_batch_stream_builder =
                record_batch_stream_builder.with_row_groups(selected_row_group_indices);
        }

        // Build the batch stream and send all the RecordBatches that it generates
        // to the requester.
        let mut record_batch_stream = record_batch_stream_builder.build()?;

        while let Some(batch) = record_batch_stream.try_next().await? {
            tx.send(record_batch_transformer.process_record_batch(batch))
                .await?
        }

        Ok(())
    }

    async fn create_parquet_record_batch_stream_builder(
        data_file_path: &str,
        file_io: FileIO,
        should_load_page_index: bool,
    ) -> Result<ParquetRecordBatchStreamBuilder<ArrowFileReader<impl FileRead + Sized>>> {
        // Get the metadata for the Parquet file we need to read and build
        // a reader for the data within
        let parquet_file = file_io.new_input(data_file_path)?;
        let (parquet_metadata, parquet_reader) =
            try_join!(parquet_file.metadata(), parquet_file.reader())?;
        let parquet_file_reader = ArrowFileReader::new(parquet_metadata, parquet_reader);

        // Start creating the record batch stream, which wraps the parquet file reader
        let record_batch_stream_builder = ParquetRecordBatchStreamBuilder::new_with_options(
            parquet_file_reader,
            ArrowReaderOptions::new().with_page_index(should_load_page_index),
        )
        .await?;
        Ok(record_batch_stream_builder)
    }

    fn build_field_id_set_and_map(
        parquet_schema: &SchemaDescriptor,
        predicate: &BoundPredicate,
    ) -> Result<(HashSet<i32>, HashMap<i32, usize>)> {
        // Collects all Iceberg field IDs referenced in the filter predicate
        let mut collector = CollectFieldIdVisitor {
            field_ids: HashSet::default(),
        };
        visit(&mut collector, predicate)?;

        let iceberg_field_ids = collector.field_ids();
        let field_id_map = build_field_id_map(parquet_schema)?;

        Ok((iceberg_field_ids, field_id_map))
    }

    fn get_arrow_projection_mask(
        field_ids: &[i32],
        iceberg_schema_of_task: &Schema,
        parquet_schema: &SchemaDescriptor,
        arrow_schema: &ArrowSchemaRef,
    ) -> Result<ProjectionMask> {
        if field_ids.is_empty() {
            Ok(ProjectionMask::all())
        } else {
            // Build the map between field id and column index in Parquet schema.
            let mut column_map = HashMap::new();

            let fields = arrow_schema.fields();
            let iceberg_schema = arrow_schema_to_schema(arrow_schema)?;
            fields.filter_leaves(|idx, field| {
                let field_id = field.metadata().get(PARQUET_FIELD_ID_META_KEY);
                if field_id.is_none() {
                    return false;
                }

                let field_id = i32::from_str(field_id.unwrap());
                if field_id.is_err() {
                    return false;
                }
                let field_id = field_id.unwrap();

                if !field_ids.contains(&field_id) {
                    return false;
                }

                let iceberg_field = iceberg_schema_of_task.field_by_id(field_id);
                let parquet_iceberg_field = iceberg_schema.field_by_id(field_id);

                if iceberg_field.is_none() || parquet_iceberg_field.is_none() {
                    return false;
                }

                if iceberg_field.unwrap().field_type != parquet_iceberg_field.unwrap().field_type {
                    return false;
                }

                column_map.insert(field_id, idx);
                true
            });

            if column_map.len() != field_ids.len() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Parquet schema {} and Iceberg schema {} do not match.",
                        iceberg_schema, iceberg_schema_of_task
                    ),
                ));
            }

            let mut indices = vec![];
            for field_id in field_ids {
                if let Some(col_idx) = column_map.get(field_id) {
                    indices.push(*col_idx);
                } else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Field {} is not found in Parquet schema.", field_id),
                    ));
                }
            }
            Ok(ProjectionMask::leaves(parquet_schema, indices))
        }
    }

    fn get_row_filter(
        predicates: &BoundPredicate,
        parquet_schema: &SchemaDescriptor,
        iceberg_field_ids: &HashSet<i32>,
        field_id_map: &HashMap<i32, usize>,
    ) -> Result<RowFilter> {
        // Collect Parquet column indices from field ids.
        // If the field id is not found in Parquet schema, it will be ignored due to schema evolution.
        let mut column_indices = iceberg_field_ids
            .iter()
            .filter_map(|field_id| field_id_map.get(field_id).cloned())
            .collect::<Vec<_>>();
        column_indices.sort();

        // The converter that converts `BoundPredicates` to `ArrowPredicates`
        let mut converter = PredicateConverter {
            parquet_schema,
            column_map: field_id_map,
            column_indices: &column_indices,
        };

        // After collecting required leaf column indices used in the predicate,
        // creates the projection mask for the Arrow predicates.
        let projection_mask = ProjectionMask::leaves(parquet_schema, column_indices.clone());
        let predicate_func = visit(&mut converter, predicates)?;
        let arrow_predicate = ArrowPredicateFn::new(projection_mask, predicate_func);
        Ok(RowFilter::new(vec![Box::new(arrow_predicate)]))
    }

    fn get_selected_row_group_indices(
        predicate: &BoundPredicate,
        parquet_metadata: &Arc<ParquetMetaData>,
        field_id_map: &HashMap<i32, usize>,
        snapshot_schema: &Schema,
    ) -> Result<Vec<usize>> {
        let row_groups_metadata = parquet_metadata.row_groups();
        let mut results = Vec::with_capacity(row_groups_metadata.len());

        for (idx, row_group_metadata) in row_groups_metadata.iter().enumerate() {
            if RowGroupMetricsEvaluator::eval(
                predicate,
                row_group_metadata,
                field_id_map,
                snapshot_schema,
            )? {
                results.push(idx);
            }
        }

        Ok(results)
    }

    fn get_predicate_row_selection(
        predicate: &BoundPredicate,
        parquet_metadata: &Arc<ParquetMetaData>,
        selected_row_groups: &Option<Vec<usize>>,
        field_id_map: &HashMap<i32, usize>,
        snapshot_schema: &Schema,
    ) -> Result<RowSelection> {
        let Some(column_index) = parquet_metadata.column_index() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Parquet file metadata does not contain a column index",
            ));
        };

        let Some(offset_index) = parquet_metadata.offset_index() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Parquet file metadata does not contain an offset index",
            ));
        };

        let mut selected_row_groups_idx = 0;

        let page_index = column_index
            .iter()
            .enumerate()
            .zip(offset_index)
            .zip(parquet_metadata.row_groups());

        let mut results = Vec::new();
        for (((idx, column_index), offset_index), row_group_metadata) in page_index {
            if let Some(selected_row_groups) = selected_row_groups {
                // skip row groups that aren't present in selected_row_groups
                if idx == selected_row_groups[selected_row_groups_idx] {
                    selected_row_groups_idx += 1;
                } else {
                    continue;
                }
            }

            let selections_for_page = PageIndexEvaluator::eval(
                predicate,
                column_index,
                offset_index,
                row_group_metadata,
                field_id_map,
                snapshot_schema,
            )?;

            results.push(selections_for_page);

            if let Some(selected_row_groups) = selected_row_groups {
                if selected_row_groups_idx == selected_row_groups.len() {
                    break;
                }
            }
        }

        Ok(results.into_iter().flatten().collect::<Vec<_>>().into())
    }

    fn get_deleted_row_selection(
        parquet_metadata: &Arc<ParquetMetaData>,
        selected_row_groups: &Option<Vec<usize>>,
        positional_deletes: &[usize],
    ) -> Result<RowSelection> {
        let Some(offset_index) = parquet_metadata.offset_index() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Parquet file metadata does not contain an offset index",
            ));
        };

        let mut selected_row_groups_idx = 0;
        let mut curr_pos_del_idx = 0;
        let pos_del_len = positional_deletes.len();

        let page_index = offset_index
            .iter()
            .enumerate()
            .zip(parquet_metadata.row_groups());

        let mut results: Vec<RowSelector> = Vec::new();
        let mut current_page_base_idx: usize = 0;
        for ((idx, _offset_index), row_group_metadata) in page_index {
            let page_num_rows = row_group_metadata.num_rows() as usize;
            let next_page_base_idx = current_page_base_idx + page_num_rows;

            // skip any row groups that aren't in the row group selection
            if let Some(selected_row_groups) = selected_row_groups {
                // skip row groups that aren't present in selected_row_groups
                if selected_row_groups_idx == selected_row_groups.len() {
                    break;
                }
                if idx == selected_row_groups[selected_row_groups_idx] {
                    selected_row_groups_idx += 1;
                } else {
                    current_page_base_idx += page_num_rows;
                    continue;
                }
            }

            let mut next_deleted_row_idx = positional_deletes[curr_pos_del_idx];

            // if the index of the next deleted row is beyond this page, skip
            // to the next page
            if next_deleted_row_idx >= next_page_base_idx {
                continue;
            }

            let mut current_idx = current_page_base_idx;
            while next_deleted_row_idx < next_page_base_idx {
                // select all rows that precede the next delete index
                if current_idx < next_deleted_row_idx {
                    let run_length = next_deleted_row_idx - current_idx;
                    results.push(RowSelector::select(run_length));
                    current_idx += run_length;
                }

                // skip all consecutive deleted rows
                let mut run_length = 1;
                while curr_pos_del_idx < pos_del_len - 1
                    && positional_deletes[curr_pos_del_idx + 1] == next_deleted_row_idx + 1
                {
                    run_length += 1;
                    curr_pos_del_idx += 1;
                }
                results.push(RowSelector::skip(run_length));
                current_idx += run_length;

                curr_pos_del_idx += 1;
                if curr_pos_del_idx >= pos_del_len {
                    break;
                }
                next_deleted_row_idx = positional_deletes[curr_pos_del_idx];
            }

            if let Some(selected_row_groups) = selected_row_groups {
                if selected_row_groups_idx == selected_row_groups.len() {
                    break;
                }
            }

            current_page_base_idx += page_num_rows;
        }

        Ok(results.into())
    }
}

/// Build the map of parquet field id to Parquet column index in the schema.
fn build_field_id_map(parquet_schema: &SchemaDescriptor) -> Result<HashMap<i32, usize>> {
    let mut column_map = HashMap::new();
    for (idx, field) in parquet_schema.columns().iter().enumerate() {
        let field_type = field.self_type();
        match field_type {
            ParquetType::PrimitiveType { basic_info, .. } => {
                if !basic_info.has_id() {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Leave column idx: {}, name: {}, type {:?} in schema doesn't have field id",
                            idx,
                            basic_info.name(),
                            field_type
                        ),
                    ));
                }
                column_map.insert(basic_info.id(), idx);
            }
            ParquetType::GroupType { .. } => {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Leave column in schema should be primitive type but got {:?}",
                        field_type
                    ),
                ));
            }
        };
    }

    Ok(column_map)
}

/// A visitor to collect field ids from bound predicates.
struct CollectFieldIdVisitor {
    field_ids: HashSet<i32>,
}

impl CollectFieldIdVisitor {
    fn field_ids(self) -> HashSet<i32> {
        self.field_ids
    }
}

impl BoundPredicateVisitor for CollectFieldIdVisitor {
    type T = ();

    fn always_true(&mut self) -> Result<()> {
        Ok(())
    }

    fn always_false(&mut self) -> Result<()> {
        Ok(())
    }

    fn and(&mut self, _lhs: (), _rhs: ()) -> Result<()> {
        Ok(())
    }

    fn or(&mut self, _lhs: (), _rhs: ()) -> Result<()> {
        Ok(())
    }

    fn not(&mut self, _inner: ()) -> Result<()> {
        Ok(())
    }

    fn is_null(&mut self, reference: &BoundReference, _predicate: &BoundPredicate) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn not_null(&mut self, reference: &BoundReference, _predicate: &BoundPredicate) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn is_nan(&mut self, reference: &BoundReference, _predicate: &BoundPredicate) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn not_nan(&mut self, reference: &BoundReference, _predicate: &BoundPredicate) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn less_than(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn less_than_or_eq(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn greater_than(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn greater_than_or_eq(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn eq(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn not_eq(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn starts_with(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn not_starts_with(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn r#in(
        &mut self,
        reference: &BoundReference,
        _literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn not_in(
        &mut self,
        reference: &BoundReference,
        _literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }
}

/// A visitor to convert Iceberg bound predicates to Arrow predicates.
struct PredicateConverter<'a> {
    /// The Parquet schema descriptor.
    pub parquet_schema: &'a SchemaDescriptor,
    /// The map between field id and leaf column index in Parquet schema.
    pub column_map: &'a HashMap<i32, usize>,
    /// The required column indices in Parquet schema for the predicates.
    pub column_indices: &'a Vec<usize>,
}

impl PredicateConverter<'_> {
    /// When visiting a bound reference, we return index of the leaf column in the
    /// required column indices which is used to project the column in the record batch.
    /// Return None if the field id is not found in the column map, which is possible
    /// due to schema evolution.
    fn bound_reference(&mut self, reference: &BoundReference) -> Result<Option<usize>> {
        // The leaf column's index in Parquet schema.
        if let Some(column_idx) = self.column_map.get(&reference.field().id) {
            if self.parquet_schema.get_column_root_idx(*column_idx) != *column_idx {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Leave column `{}` in predicates isn't a root column in Parquet schema.",
                        reference.field().name
                    ),
                ));
            }

            // The leaf column's index in the required column indices.
            let index = self
                .column_indices
                .iter()
                .position(|&idx| idx == *column_idx).ok_or(Error::new(ErrorKind::DataInvalid, format!(
                    "Leave column `{}` in predicates cannot be found in the required column indices.",
                    reference.field().name
                )))?;

            Ok(Some(index))
        } else {
            Ok(None)
        }
    }

    /// Build an Arrow predicate that always returns true.
    fn build_always_true(&self) -> Result<Box<PredicateResult>> {
        Ok(Box::new(|batch| {
            Ok(BooleanArray::from(vec![true; batch.num_rows()]))
        }))
    }

    /// Build an Arrow predicate that always returns false.
    fn build_always_false(&self) -> Result<Box<PredicateResult>> {
        Ok(Box::new(|batch| {
            Ok(BooleanArray::from(vec![false; batch.num_rows()]))
        }))
    }
}

/// Gets the leaf column from the record batch for the required column index. Only
/// supports top-level columns for now.
fn project_column(
    batch: &RecordBatch,
    column_idx: usize,
) -> std::result::Result<ArrayRef, ArrowError> {
    let column = batch.column(column_idx);

    match column.data_type() {
        DataType::Struct(_) => Err(ArrowError::SchemaError(
            "Does not support struct column yet.".to_string(),
        )),
        _ => Ok(column.clone()),
    }
}

type PredicateResult =
    dyn FnMut(RecordBatch) -> std::result::Result<BooleanArray, ArrowError> + Send + 'static;

impl<'a> BoundPredicateVisitor for PredicateConverter<'a> {
    type T = Box<PredicateResult>;

    fn always_true(&mut self) -> Result<Box<PredicateResult>> {
        self.build_always_true()
    }

    fn always_false(&mut self) -> Result<Box<PredicateResult>> {
        self.build_always_false()
    }

    fn and(
        &mut self,
        mut lhs: Box<PredicateResult>,
        mut rhs: Box<PredicateResult>,
    ) -> Result<Box<PredicateResult>> {
        Ok(Box::new(move |batch| {
            let left = lhs(batch.clone())?;
            let right = rhs(batch)?;
            and(&left, &right)
        }))
    }

    fn or(
        &mut self,
        mut lhs: Box<PredicateResult>,
        mut rhs: Box<PredicateResult>,
    ) -> Result<Box<PredicateResult>> {
        Ok(Box::new(move |batch| {
            let left = lhs(batch.clone())?;
            let right = rhs(batch)?;
            or(&left, &right)
        }))
    }

    fn not(&mut self, mut inner: Box<PredicateResult>) -> Result<Box<PredicateResult>> {
        Ok(Box::new(move |batch| {
            let pred_ret = inner(batch)?;
            not(&pred_ret)
        }))
    }

    fn is_null(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            Ok(Box::new(move |batch| {
                let column = project_column(&batch, idx)?;
                is_null(&column)
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_true()
        }
    }

    fn not_null(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            Ok(Box::new(move |batch| {
                let column = project_column(&batch, idx)?;
                is_not_null(&column)
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn is_nan(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if self.bound_reference(reference)?.is_some() {
            self.build_always_true()
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn not_nan(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if self.bound_reference(reference)?.is_some() {
            self.build_always_false()
        } else {
            // A missing column, treating it as null.
            self.build_always_true()
        }
    }

    fn less_than(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, idx)?;
                lt(&left, literal.as_ref())
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_true()
        }
    }

    fn less_than_or_eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, idx)?;
                lt_eq(&left, literal.as_ref())
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_true()
        }
    }

    fn greater_than(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, idx)?;
                gt(&left, literal.as_ref())
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn greater_than_or_eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, idx)?;
                gt_eq(&left, literal.as_ref())
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, idx)?;
                eq(&left, literal.as_ref())
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn not_eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, idx)?;
                neq(&left, literal.as_ref())
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn starts_with(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, idx)?;
                starts_with(&left, literal.as_ref())
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn not_starts_with(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, idx)?;

                // update here if arrow ever adds a native not_starts_with
                not(&starts_with(&left, literal.as_ref())?)
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_true()
        }
    }

    fn r#in(
        &mut self,
        reference: &BoundReference,
        literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literals: Vec<_> = literals
                .iter()
                .map(|lit| get_arrow_datum(lit).unwrap())
                .collect();

            Ok(Box::new(move |batch| {
                // update this if arrow ever adds a native is_in kernel
                let left = project_column(&batch, idx)?;
                let mut acc = BooleanArray::from(vec![false; batch.num_rows()]);
                for literal in &literals {
                    acc = or(&acc, &eq(&left, literal.as_ref())?)?
                }

                Ok(acc)
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn not_in(
        &mut self,
        reference: &BoundReference,
        literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literals: Vec<_> = literals
                .iter()
                .map(|lit| get_arrow_datum(lit).unwrap())
                .collect();

            Ok(Box::new(move |batch| {
                // update this if arrow ever adds a native not_in kernel
                let left = project_column(&batch, idx)?;
                let mut acc = BooleanArray::from(vec![true; batch.num_rows()]);
                for literal in &literals {
                    acc = and(&acc, &neq(&left, literal.as_ref())?)?
                }

                Ok(acc)
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_true()
        }
    }
}

/// ArrowFileReader is a wrapper around a FileRead that impls parquets AsyncFileReader.
///
/// # TODO
///
/// [ParquetObjectReader](https://docs.rs/parquet/latest/src/parquet/arrow/async_reader/store.rs.html#64)
/// contains the following hints to speed up metadata loading, we can consider adding them to this struct:
///
/// - `metadata_size_hint`: Provide a hint as to the size of the parquet file's footer.
/// - `preload_column_index`: Load the Column Index  as part of [`Self::get_metadata`].
/// - `preload_offset_index`: Load the Offset Index as part of [`Self::get_metadata`].
struct ArrowFileReader<R: FileRead> {
    meta: FileMetadata,
    r: R,
}

impl<R: FileRead> ArrowFileReader<R> {
    /// Create a new ArrowFileReader
    fn new(meta: FileMetadata, r: R) -> Self {
        Self { meta, r }
    }
}

impl<R: FileRead> AsyncFileReader for ArrowFileReader<R> {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        Box::pin(
            self.r
                .read(range.start as _..range.end as _)
                .map_err(|err| parquet::errors::ParquetError::External(Box::new(err))),
        )
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        async move {
            let reader = ParquetMetaDataReader::new();
            let size = self.meta.size as usize;
            let meta = reader.load_and_finish(self, size).await?;

            Ok(Arc::new(meta))
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use crate::arrow::reader::CollectFieldIdVisitor;
    use crate::expr::visitors::bound_predicate_visitor::visit;
    use crate::expr::{Bind, Reference};
    use crate::spec::{NestedField, PrimitiveType, Schema, SchemaRef, Type};

    fn table_schema_simple() -> SchemaRef {
        Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_identifier_field_ids(vec![2])
                .with_fields(vec![
                    NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
                    NestedField::optional(4, "qux", Type::Primitive(PrimitiveType::Float)).into(),
                ])
                .build()
                .unwrap(),
        )
    }

    #[test]
    fn test_collect_field_id() {
        let schema = table_schema_simple();
        let expr = Reference::new("qux").is_null();
        let bound_expr = expr.bind(schema, true).unwrap();

        let mut visitor = CollectFieldIdVisitor {
            field_ids: HashSet::default(),
        };
        visit(&mut visitor, &bound_expr).unwrap();

        let mut expected = HashSet::default();
        expected.insert(4_i32);

        assert_eq!(visitor.field_ids, expected);
    }

    #[test]
    fn test_collect_field_id_with_and() {
        let schema = table_schema_simple();
        let expr = Reference::new("qux")
            .is_null()
            .and(Reference::new("baz").is_null());
        let bound_expr = expr.bind(schema, true).unwrap();

        let mut visitor = CollectFieldIdVisitor {
            field_ids: HashSet::default(),
        };
        visit(&mut visitor, &bound_expr).unwrap();

        let mut expected = HashSet::default();
        expected.insert(4_i32);
        expected.insert(3);

        assert_eq!(visitor.field_ids, expected);
    }

    #[test]
    fn test_collect_field_id_with_or() {
        let schema = table_schema_simple();
        let expr = Reference::new("qux")
            .is_null()
            .or(Reference::new("baz").is_null());
        let bound_expr = expr.bind(schema, true).unwrap();

        let mut visitor = CollectFieldIdVisitor {
            field_ids: HashSet::default(),
        };
        visit(&mut visitor, &bound_expr).unwrap();

        let mut expected = HashSet::default();
        expected.insert(4_i32);
        expected.insert(3);

        assert_eq!(visitor.field_ids, expected);
    }
}
