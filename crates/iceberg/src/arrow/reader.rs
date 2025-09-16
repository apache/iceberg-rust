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

use arrow_arith::boolean::{and, and_kleene, is_not_null, is_null, not, or, or_kleene};
use arrow_array::{Array, ArrayRef, BooleanArray, Datum as ArrowDatum, RecordBatch, Scalar};
use arrow_cast::cast::cast;
use arrow_ord::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow_schema::{
    ArrowError, DataType, FieldRef, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use arrow_string::like::starts_with;
use bytes::Bytes;
use fnv::FnvHashSet;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt, try_join};
use parquet::arrow::arrow_reader::{
    ArrowPredicateFn, ArrowReaderOptions, RowFilter, RowSelection, RowSelector,
};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{PARQUET_FIELD_ID_META_KEY, ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader, RowGroupMetaData};
use parquet::schema::types::{SchemaDescriptor, Type as ParquetType};

use crate::arrow::caching_delete_file_loader::CachingDeleteFileLoader;
use crate::arrow::record_batch_transformer::RecordBatchTransformer;
use crate::arrow::{arrow_schema_to_schema, get_arrow_datum};
use crate::delete_vector::DeleteVector;
use crate::error::Result;
use crate::expr::visitors::bound_predicate_visitor::{BoundPredicateVisitor, visit};
use crate::expr::visitors::page_index_evaluator::PageIndexEvaluator;
use crate::expr::visitors::row_group_metrics_evaluator::RowGroupMetricsEvaluator;
use crate::expr::{BoundPredicate, BoundReference};
use crate::io::{FileIO, FileMetadata, FileRead};
use crate::scan::{ArrowRecordBatchStream, FileScanTask, FileScanTaskStream};
use crate::spec::{Datum, NestedField, PrimitiveType, Schema, Type};
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
            file_io: self.file_io.clone(),
            delete_file_loader: CachingDeleteFileLoader::new(
                self.file_io.clone(),
                self.concurrency_limit_data_files,
            ),
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
    delete_file_loader: CachingDeleteFileLoader,

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

        let stream = tasks
            .map_ok(move |task| {
                let file_io = file_io.clone();

                Self::process_file_scan_task(
                    task,
                    batch_size,
                    file_io,
                    self.delete_file_loader.clone(),
                    row_group_filtering_enabled,
                    row_selection_enabled,
                )
            })
            .map_err(|err| {
                Error::new(ErrorKind::Unexpected, "file scan task generate failed").with_source(err)
            })
            .try_buffer_unordered(concurrency_limit_data_files)
            .try_flatten_unordered(concurrency_limit_data_files);

        Ok(Box::pin(stream) as ArrowRecordBatchStream)
    }

    #[allow(clippy::too_many_arguments)]
    async fn process_file_scan_task(
        task: FileScanTask,
        batch_size: Option<usize>,
        file_io: FileIO,
        delete_file_loader: CachingDeleteFileLoader,
        row_group_filtering_enabled: bool,
        row_selection_enabled: bool,
    ) -> Result<ArrowRecordBatchStream> {
        let should_load_page_index =
            (row_selection_enabled && task.predicate.is_some()) || !task.deletes.is_empty();

        let delete_filter_rx = delete_file_loader.load_deletes(&task.deletes, task.schema.clone());

        let mut record_batch_stream_builder = Self::create_parquet_record_batch_stream_builder(
            &task.data_file_path,
            file_io.clone(),
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

        // RecordBatchTransformer performs any transformations required on the RecordBatches
        // that come back from the file, such as type promotion, default column insertion
        // and column re-ordering
        let mut record_batch_transformer =
            RecordBatchTransformer::build(task.schema_ref(), task.project_field_ids());

        if let Some(batch_size) = batch_size {
            record_batch_stream_builder = record_batch_stream_builder.with_batch_size(batch_size);
        }

        let delete_filter = delete_filter_rx.await.unwrap()?;
        let delete_predicate = delete_filter.build_equality_delete_predicate(&task).await?;

        // In addition to the optional predicate supplied in the `FileScanTask`,
        // we also have an optional predicate resulting from equality delete files.
        // If both are present, we logical-AND them together to form a single filter
        // predicate that we can pass to the `RecordBatchStreamBuilder`.
        let final_predicate = match (&task.predicate, delete_predicate) {
            (None, None) => None,
            (Some(predicate), None) => Some(predicate.clone()),
            (None, Some(ref predicate)) => Some(predicate.clone()),
            (Some(filter_predicate), Some(delete_predicate)) => {
                Some(filter_predicate.clone().and(delete_predicate))
            }
        };

        // There are two possible sources both for potential lists of selected RowGroup indices,
        // and for `RowSelection`s.
        // Selected RowGroup index lists can come from two sources:
        //   * When there are equality delete files that are applicable;
        //   * When there is a scan predicate and row_group_filtering_enabled = true.
        // `RowSelection`s can be created in either or both of the following cases:
        //   * When there are positional delete files that are applicable;
        //   * When there is a scan predicate and row_selection_enabled = true
        // Note that, in the former case we only perform row group filtering when
        // there is a scan predicate AND row_group_filtering_enabled = true,
        // but we perform row selection filtering if there are applicable
        // equality delete files OR (there is a scan predicate AND row_selection_enabled),
        // since the only implemented method of applying positional deletes is
        // by using a `RowSelection`.
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
                row_selection = Some(Self::get_row_selection_for_filter_predicate(
                    &predicate,
                    record_batch_stream_builder.metadata(),
                    &selected_row_group_indices,
                    &field_id_map,
                    &task.schema,
                )?);
            }
        }

        let positional_delete_indexes = delete_filter.get_delete_vector(&task);

        if let Some(positional_delete_indexes) = positional_delete_indexes {
            let delete_row_selection = {
                let positional_delete_indexes = positional_delete_indexes.lock().unwrap();

                Self::build_deletes_row_selection(
                    record_batch_stream_builder.metadata().row_groups(),
                    &selected_row_group_indices,
                    &positional_delete_indexes,
                )
            }?;

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
        let record_batch_stream =
            record_batch_stream_builder
                .build()?
                .map(move |batch| match batch {
                    Ok(batch) => record_batch_transformer.process_record_batch(batch),
                    Err(err) => Err(err.into()),
                });

        Ok(Box::pin(record_batch_stream) as ArrowRecordBatchStream)
    }

    pub(crate) async fn create_parquet_record_batch_stream_builder(
        data_file_path: &str,
        file_io: FileIO,
        should_load_page_index: bool,
    ) -> Result<ParquetRecordBatchStreamBuilder<ArrowFileReader<impl FileRead + Sized>>> {
        // Get the metadata for the Parquet file we need to read and build
        // a reader for the data within
        let parquet_file = file_io.new_input(data_file_path)?;
        let (parquet_metadata, parquet_reader) =
            try_join!(parquet_file.metadata(), parquet_file.reader())?;
        let parquet_file_reader = ArrowFileReader::new(parquet_metadata, parquet_reader)
            .with_preload_column_index(true)
            .with_preload_offset_index(true)
            .with_preload_page_index(should_load_page_index);

        // Create the record batch stream builder, which wraps the parquet file reader
        let record_batch_stream_builder = ParquetRecordBatchStreamBuilder::new_with_options(
            parquet_file_reader,
            ArrowReaderOptions::new(),
        )
        .await?;
        Ok(record_batch_stream_builder)
    }

    /// computes a `RowSelection` from positional delete indices.
    ///
    /// Using the Parquet page index, we build a `RowSelection` that rejects rows that are indicated
    /// as having been deleted by a positional delete, taking into account any row groups that have
    /// been skipped entirely by the filter predicate
    fn build_deletes_row_selection(
        row_group_metadata_list: &[RowGroupMetaData],
        selected_row_groups: &Option<Vec<usize>>,
        positional_deletes: &DeleteVector,
    ) -> Result<RowSelection> {
        let mut results: Vec<RowSelector> = Vec::new();
        let mut selected_row_groups_idx = 0;
        let mut current_row_group_base_idx: u64 = 0;
        let mut delete_vector_iter = positional_deletes.iter();
        let mut next_deleted_row_idx_opt = delete_vector_iter.next();

        for (idx, row_group_metadata) in row_group_metadata_list.iter().enumerate() {
            let row_group_num_rows = row_group_metadata.num_rows() as u64;
            let next_row_group_base_idx = current_row_group_base_idx + row_group_num_rows;

            // if row group selection is enabled,
            if let Some(selected_row_groups) = selected_row_groups {
                // if we've consumed all the selected row groups, we're done
                if selected_row_groups_idx == selected_row_groups.len() {
                    break;
                }

                if idx == selected_row_groups[selected_row_groups_idx] {
                    // we're in a selected row group. Increment selected_row_groups_idx
                    // so that next time around the for loop we're looking for the next
                    // selected row group
                    selected_row_groups_idx += 1;
                } else {
                    // remove any positional deletes from the skipped page so that
                    // `positional.deletes.min()` can be used
                    delete_vector_iter.advance_to(next_row_group_base_idx);
                    next_deleted_row_idx_opt = delete_vector_iter.next();

                    // still increment the current page base index but then skip to the next row group
                    // in the file
                    current_row_group_base_idx += row_group_num_rows;
                    continue;
                }
            }

            let mut next_deleted_row_idx = match next_deleted_row_idx_opt {
                Some(next_deleted_row_idx) => {
                    // if the index of the next deleted row is beyond this row group, add a selection for
                    // the remainder of this row group and skip to the next row group
                    if next_deleted_row_idx >= next_row_group_base_idx {
                        results.push(RowSelector::select(row_group_num_rows as usize));
                        continue;
                    }

                    next_deleted_row_idx
                }

                // If there are no more pos deletes, add a selector for the entirety of this row group.
                _ => {
                    results.push(RowSelector::select(row_group_num_rows as usize));
                    continue;
                }
            };

            let mut current_idx = current_row_group_base_idx;
            'chunks: while next_deleted_row_idx < next_row_group_base_idx {
                // `select` all rows that precede the next delete index
                if current_idx < next_deleted_row_idx {
                    let run_length = next_deleted_row_idx - current_idx;
                    results.push(RowSelector::select(run_length as usize));
                    current_idx += run_length;
                }

                // `skip` all consecutive deleted rows in the current row group
                let mut run_length = 0;
                while next_deleted_row_idx == current_idx
                    && next_deleted_row_idx < next_row_group_base_idx
                {
                    run_length += 1;
                    current_idx += 1;

                    next_deleted_row_idx_opt = delete_vector_iter.next();
                    next_deleted_row_idx = match next_deleted_row_idx_opt {
                        Some(next_deleted_row_idx) => next_deleted_row_idx,
                        _ => {
                            // We've processed the final positional delete.
                            // Conclude the skip and then break so that we select the remaining
                            // rows in the row group and move on to the next row group
                            results.push(RowSelector::skip(run_length));
                            break 'chunks;
                        }
                    };
                }
                if run_length > 0 {
                    results.push(RowSelector::skip(run_length));
                }
            }

            if current_idx < next_row_group_base_idx {
                results.push(RowSelector::select(
                    (next_row_group_base_idx - current_idx) as usize,
                ));
            }

            current_row_group_base_idx += row_group_num_rows;
        }

        Ok(results.into())
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

    /// Insert the leaf field id into the field_ids using for projection.
    /// For nested type, it will recursively insert the leaf field id.
    fn include_leaf_field_id(field: &NestedField, field_ids: &mut Vec<i32>) {
        match field.field_type.as_ref() {
            Type::Primitive(_) => {
                field_ids.push(field.id);
            }
            Type::Struct(struct_type) => {
                for nested_field in struct_type.fields() {
                    Self::include_leaf_field_id(nested_field, field_ids);
                }
            }
            Type::List(list_type) => {
                Self::include_leaf_field_id(&list_type.element_field, field_ids);
            }
            Type::Map(map_type) => {
                Self::include_leaf_field_id(&map_type.key_field, field_ids);
                Self::include_leaf_field_id(&map_type.value_field, field_ids);
            }
        }
    }

    fn get_arrow_projection_mask(
        field_ids: &[i32],
        iceberg_schema_of_task: &Schema,
        parquet_schema: &SchemaDescriptor,
        arrow_schema: &ArrowSchemaRef,
    ) -> Result<ProjectionMask> {
        fn type_promotion_is_valid(
            file_type: Option<&PrimitiveType>,
            projected_type: Option<&PrimitiveType>,
        ) -> bool {
            match (file_type, projected_type) {
                (Some(lhs), Some(rhs)) if lhs == rhs => true,
                (Some(PrimitiveType::Int), Some(PrimitiveType::Long)) => true,
                (Some(PrimitiveType::Float), Some(PrimitiveType::Double)) => true,
                (
                    Some(PrimitiveType::Decimal {
                        precision: file_precision,
                        scale: file_scale,
                    }),
                    Some(PrimitiveType::Decimal {
                        precision: requested_precision,
                        scale: requested_scale,
                    }),
                ) if requested_precision >= file_precision && file_scale == requested_scale => true,
                // Uuid will be store as Fixed(16) in parquet file, so the read back type will be Fixed(16).
                (Some(PrimitiveType::Fixed(16)), Some(PrimitiveType::Uuid)) => true,
                _ => false,
            }
        }

        let mut leaf_field_ids = vec![];
        for field_id in field_ids {
            let field = iceberg_schema_of_task.field_by_id(*field_id);
            if let Some(field) = field {
                Self::include_leaf_field_id(field, &mut leaf_field_ids);
            }
        }

        if leaf_field_ids.is_empty() {
            Ok(ProjectionMask::all())
        } else {
            // Build the map between field id and column index in Parquet schema.
            let mut column_map = HashMap::new();

            let fields = arrow_schema.fields();
            // Pre-project only the fields that have been selected, possibly avoiding converting
            // some Arrow types that are not yet supported.
            let mut projected_fields: HashMap<FieldRef, i32> = HashMap::new();
            let projected_arrow_schema = ArrowSchema::new_with_metadata(
                fields.filter_leaves(|_, f| {
                    f.metadata()
                        .get(PARQUET_FIELD_ID_META_KEY)
                        .and_then(|field_id| i32::from_str(field_id).ok())
                        .is_some_and(|field_id| {
                            projected_fields.insert((*f).clone(), field_id);
                            leaf_field_ids.contains(&field_id)
                        })
                }),
                arrow_schema.metadata().clone(),
            );
            let iceberg_schema = arrow_schema_to_schema(&projected_arrow_schema)?;

            fields.filter_leaves(|idx, field| {
                let Some(field_id) = projected_fields.get(field).cloned() else {
                    return false;
                };

                let iceberg_field = iceberg_schema_of_task.field_by_id(field_id);
                let parquet_iceberg_field = iceberg_schema.field_by_id(field_id);

                if iceberg_field.is_none() || parquet_iceberg_field.is_none() {
                    return false;
                }

                if !type_promotion_is_valid(
                    parquet_iceberg_field
                        .unwrap()
                        .field_type
                        .as_primitive_type(),
                    iceberg_field.unwrap().field_type.as_primitive_type(),
                ) {
                    return false;
                }

                column_map.insert(field_id, idx);
                true
            });

            if column_map.len() != leaf_field_ids.len() {
                let missing_fields = leaf_field_ids
                    .iter()
                    .filter(|field_id| !column_map.contains_key(field_id))
                    .collect::<Vec<_>>();
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Parquet schema {} and Iceberg schema {} do not match.",
                        iceberg_schema, iceberg_schema_of_task
                    ),
                )
                .with_context("column_map", format! {"{:?}", column_map})
                .with_context("field_ids", format! {"{:?}", leaf_field_ids})
                .with_context("missing_fields", format! {"{:?}", missing_fields}));
            }

            let mut indices = vec![];
            for field_id in leaf_field_ids {
                if let Some(col_idx) = column_map.get(&field_id) {
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

    fn get_row_selection_for_filter_predicate(
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
            if self.parquet_schema.get_column_root(*column_idx).is_group() {
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
                .position(|&idx| idx == *column_idx)
                .ok_or(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                "Leave column `{}` in predicates cannot be found in the required column indices.",
                reference.field().name
            ),
                ))?;

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

impl BoundPredicateVisitor for PredicateConverter<'_> {
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
            and_kleene(&left, &right)
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
            or_kleene(&left, &right)
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
                let literal = try_cast_literal(&literal, left.data_type())?;
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
                let literal = try_cast_literal(&literal, left.data_type())?;
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
                let literal = try_cast_literal(&literal, left.data_type())?;
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
                let literal = try_cast_literal(&literal, left.data_type())?;
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
                let literal = try_cast_literal(&literal, left.data_type())?;
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
                let literal = try_cast_literal(&literal, left.data_type())?;
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
                let literal = try_cast_literal(&literal, left.data_type())?;
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
                let literal = try_cast_literal(&literal, left.data_type())?;
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
                    let literal = try_cast_literal(literal, left.data_type())?;
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
                    let literal = try_cast_literal(literal, left.data_type())?;
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
pub struct ArrowFileReader<R: FileRead> {
    meta: FileMetadata,
    preload_column_index: bool,
    preload_offset_index: bool,
    preload_page_index: bool,
    metadata_size_hint: Option<usize>,
    r: R,
}

impl<R: FileRead> ArrowFileReader<R> {
    /// Create a new ArrowFileReader
    pub fn new(meta: FileMetadata, r: R) -> Self {
        Self {
            meta,
            preload_column_index: false,
            preload_offset_index: false,
            preload_page_index: false,
            metadata_size_hint: None,
            r,
        }
    }

    /// Enable or disable preloading of the column index
    pub fn with_preload_column_index(mut self, preload: bool) -> Self {
        self.preload_column_index = preload;
        self
    }

    /// Enable or disable preloading of the offset index
    pub fn with_preload_offset_index(mut self, preload: bool) -> Self {
        self.preload_offset_index = preload;
        self
    }

    /// Enable or disable preloading of the page index
    pub fn with_preload_page_index(mut self, preload: bool) -> Self {
        self.preload_page_index = preload;
        self
    }

    /// Provide a hint as to the number of bytes to prefetch for parsing the Parquet metadata
    ///
    /// This hint can help reduce the number of fetch requests. For more details see the
    /// [ParquetMetaDataReader documentation](https://docs.rs/parquet/latest/parquet/file/metadata/struct.ParquetMetaDataReader.html#method.with_prefetch_hint).
    pub fn with_metadata_size_hint(mut self, hint: usize) -> Self {
        self.metadata_size_hint = Some(hint);
        self
    }
}

impl<R: FileRead> AsyncFileReader for ArrowFileReader<R> {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        Box::pin(
            self.r
                .read(range.start..range.end)
                .map_err(|err| parquet::errors::ParquetError::External(Box::new(err))),
        )
    }

    // TODO: currently we don't respect `ArrowReaderOptions` cause it don't expose any method to access the option field
    // we will fix it after `v55.1.0` is released in https://github.com/apache/arrow-rs/issues/7393
    fn get_metadata(
        &mut self,
        _options: Option<&'_ ArrowReaderOptions>,
    ) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        async move {
            let reader = ParquetMetaDataReader::new()
                .with_prefetch_hint(self.metadata_size_hint)
                .with_column_indexes(self.preload_column_index)
                .with_page_indexes(self.preload_page_index)
                .with_offset_indexes(self.preload_offset_index);
            let size = self.meta.size;
            let meta = reader.load_and_finish(self, size).await?;

            Ok(Arc::new(meta))
        }
        .boxed()
    }
}

/// The Arrow type of an array that the Parquet reader reads may not match the exact Arrow type
/// that Iceberg uses for literals - but they are effectively the same logical type,
/// i.e. LargeUtf8 and Utf8 or Utf8View and Utf8 or Utf8View and LargeUtf8.
///
/// The Arrow compute kernels that we use must match the type exactly, so first cast the literal
/// into the type of the batch we read from Parquet before sending it to the compute kernel.
fn try_cast_literal(
    literal: &Arc<dyn ArrowDatum + Send + Sync>,
    column_type: &DataType,
) -> std::result::Result<Arc<dyn ArrowDatum + Send + Sync>, ArrowError> {
    let literal_array = literal.get().0;

    // No cast required
    if literal_array.data_type() == column_type {
        return Ok(Arc::clone(literal));
    }

    let literal_array = cast(literal_array, column_type)?;
    Ok(Arc::new(Scalar::new(literal_array)))
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::fs::File;
    use std::sync::Arc;

    use arrow_array::cast::AsArray;
    use arrow_array::{ArrayRef, LargeStringArray, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};
    use futures::TryStreamExt;
    use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
    use parquet::arrow::{ArrowWriter, ProjectionMask};
    use parquet::basic::Compression;
    use parquet::file::metadata::{ColumnChunkMetaData, RowGroupMetaData};
    use parquet::file::properties::WriterProperties;
    use parquet::schema::parser::parse_message_type;
    use parquet::schema::types::{SchemaDescPtr, SchemaDescriptor};
    use roaring::RoaringTreemap;
    use tempfile::TempDir;

    use crate::ErrorKind;
    use crate::arrow::reader::{CollectFieldIdVisitor, PARQUET_FIELD_ID_META_KEY};
    use crate::arrow::{ArrowReader, ArrowReaderBuilder};
    use crate::delete_vector::DeleteVector;
    use crate::expr::visitors::bound_predicate_visitor::visit;
    use crate::expr::{Bind, Predicate, Reference};
    use crate::io::FileIO;
    use crate::scan::{FileScanTask, FileScanTaskStream};
    use crate::spec::{DataFileFormat, Datum, NestedField, PrimitiveType, Schema, SchemaRef, Type};

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

    #[test]
    fn test_arrow_projection_mask() {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_identifier_field_ids(vec![1])
                .with_fields(vec![
                    NestedField::required(1, "c1", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::optional(2, "c2", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(
                        3,
                        "c3",
                        Type::Primitive(PrimitiveType::Decimal {
                            precision: 38,
                            scale: 3,
                        }),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("c1", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            // Type not supported
            Field::new("c2", DataType::Duration(TimeUnit::Microsecond), true).with_metadata(
                HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "2".to_string())]),
            ),
            // Precision is beyond the supported range
            Field::new("c3", DataType::Decimal128(39, 3), true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "3".to_string(),
            )])),
        ]));

        let message_type = "
message schema {
  required binary c1 (STRING) = 1;
  optional int32 c2 (INTEGER(8,true)) = 2;
  optional fixed_len_byte_array(17) c3 (DECIMAL(39,3)) = 3;
}
    ";
        let parquet_type = parse_message_type(message_type).expect("should parse schema");
        let parquet_schema = SchemaDescriptor::new(Arc::new(parquet_type));

        // Try projecting the fields c2 and c3 with the unsupported data types
        let err = ArrowReader::get_arrow_projection_mask(
            &[1, 2, 3],
            &schema,
            &parquet_schema,
            &arrow_schema,
        )
        .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            err.to_string(),
            "DataInvalid => Unsupported Arrow data type: Duration(Microsecond)".to_string()
        );

        // Omitting field c2, we still get an error due to c3 being selected
        let err = ArrowReader::get_arrow_projection_mask(
            &[1, 3],
            &schema,
            &parquet_schema,
            &arrow_schema,
        )
        .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            err.to_string(),
            "DataInvalid => Failed to create decimal type, source: DataInvalid => Decimals with precision larger than 38 are not supported: 39".to_string()
        );

        // Finally avoid selecting fields with unsupported data types
        let mask =
            ArrowReader::get_arrow_projection_mask(&[1], &schema, &parquet_schema, &arrow_schema)
                .expect("Some ProjectionMask");
        assert_eq!(mask, ProjectionMask::leaves(&parquet_schema, vec![0]));
    }

    #[tokio::test]
    async fn test_kleene_logic_or_behaviour() {
        // a IS NULL OR a = 'foo'
        let predicate = Reference::new("a")
            .is_null()
            .or(Reference::new("a").equal_to(Datum::string("foo")));

        // Table data: [NULL, "foo", "bar"]
        let data_for_col_a = vec![None, Some("foo".to_string()), Some("bar".to_string())];

        // Expected: [NULL, "foo"].
        let expected = vec![None, Some("foo".to_string())];

        let (file_io, schema, table_location, _temp_dir) =
            setup_kleene_logic(data_for_col_a, DataType::Utf8);
        let reader = ArrowReaderBuilder::new(file_io).build();

        let result_data = test_perform_read(predicate, schema, table_location, reader).await;

        assert_eq!(result_data, expected);
    }

    #[tokio::test]
    async fn test_kleene_logic_and_behaviour() {
        // a IS NOT NULL AND a != 'foo'
        let predicate = Reference::new("a")
            .is_not_null()
            .and(Reference::new("a").not_equal_to(Datum::string("foo")));

        // Table data: [NULL, "foo", "bar"]
        let data_for_col_a = vec![None, Some("foo".to_string()), Some("bar".to_string())];

        // Expected: ["bar"].
        let expected = vec![Some("bar".to_string())];

        let (file_io, schema, table_location, _temp_dir) =
            setup_kleene_logic(data_for_col_a, DataType::Utf8);
        let reader = ArrowReaderBuilder::new(file_io).build();

        let result_data = test_perform_read(predicate, schema, table_location, reader).await;

        assert_eq!(result_data, expected);
    }

    #[tokio::test]
    async fn test_predicate_cast_literal() {
        let predicates = vec![
            // a == 'foo'
            (Reference::new("a").equal_to(Datum::string("foo")), vec![
                Some("foo".to_string()),
            ]),
            // a != 'foo'
            (
                Reference::new("a").not_equal_to(Datum::string("foo")),
                vec![Some("bar".to_string())],
            ),
            // STARTS_WITH(a, 'foo')
            (Reference::new("a").starts_with(Datum::string("f")), vec![
                Some("foo".to_string()),
            ]),
            // NOT STARTS_WITH(a, 'foo')
            (
                Reference::new("a").not_starts_with(Datum::string("f")),
                vec![Some("bar".to_string())],
            ),
            // a < 'foo'
            (Reference::new("a").less_than(Datum::string("foo")), vec![
                Some("bar".to_string()),
            ]),
            // a <= 'foo'
            (
                Reference::new("a").less_than_or_equal_to(Datum::string("foo")),
                vec![Some("foo".to_string()), Some("bar".to_string())],
            ),
            // a > 'foo'
            (
                Reference::new("a").greater_than(Datum::string("bar")),
                vec![Some("foo".to_string())],
            ),
            // a >= 'foo'
            (
                Reference::new("a").greater_than_or_equal_to(Datum::string("foo")),
                vec![Some("foo".to_string())],
            ),
            // a IN ('foo', 'bar')
            (
                Reference::new("a").is_in([Datum::string("foo"), Datum::string("baz")]),
                vec![Some("foo".to_string())],
            ),
            // a NOT IN ('foo', 'bar')
            (
                Reference::new("a").is_not_in([Datum::string("foo"), Datum::string("baz")]),
                vec![Some("bar".to_string())],
            ),
        ];

        // Table data: ["foo", "bar"]
        let data_for_col_a = vec![Some("foo".to_string()), Some("bar".to_string())];

        let (file_io, schema, table_location, _temp_dir) =
            setup_kleene_logic(data_for_col_a, DataType::LargeUtf8);
        let reader = ArrowReaderBuilder::new(file_io).build();

        for (predicate, expected) in predicates {
            println!("testing predicate {predicate}");
            let result_data = test_perform_read(
                predicate.clone(),
                schema.clone(),
                table_location.clone(),
                reader.clone(),
            )
            .await;

            assert_eq!(result_data, expected, "predicate={predicate}");
        }
    }

    async fn test_perform_read(
        predicate: Predicate,
        schema: SchemaRef,
        table_location: String,
        reader: ArrowReader,
    ) -> Vec<Option<String>> {
        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask {
                start: 0,
                length: 0,
                record_count: None,
                data_file_path: format!("{}/1.parquet", table_location),
                data_file_format: DataFileFormat::Parquet,
                schema: schema.clone(),
                project_field_ids: vec![1],
                predicate: Some(predicate.bind(schema, true).unwrap()),
                deletes: vec![],
            })]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        let result_data = result[0].columns()[0]
            .as_string_opt::<i32>()
            .unwrap()
            .iter()
            .map(|v| v.map(ToOwned::to_owned))
            .collect::<Vec<_>>();

        result_data
    }

    fn setup_kleene_logic(
        data_for_col_a: Vec<Option<String>>,
        col_a_type: DataType,
    ) -> (FileIO, SchemaRef, String, TempDir) {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(1, "a", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("a", col_a_type.clone(), true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();

        let file_io = FileIO::from_path(&table_location).unwrap().build().unwrap();

        let col = match col_a_type {
            DataType::Utf8 => Arc::new(StringArray::from(data_for_col_a)) as ArrayRef,
            DataType::LargeUtf8 => Arc::new(LargeStringArray::from(data_for_col_a)) as ArrayRef,
            _ => panic!("unexpected col_a_type"),
        };

        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![col]).unwrap();

        // Write the Parquet files
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let file = File::create(format!("{}/1.parquet", &table_location)).unwrap();
        let mut writer =
            ArrowWriter::try_new(file, to_write.schema(), Some(props.clone())).unwrap();

        writer.write(&to_write).expect("Writing batch");

        // writer must be closed to write footer
        writer.close().unwrap();

        (file_io, schema, table_location, tmp_dir)
    }

    #[test]
    fn test_build_deletes_row_selection() {
        let schema_descr = get_test_schema_descr();

        let mut columns = vec![];
        for ptr in schema_descr.columns() {
            let column = ColumnChunkMetaData::builder(ptr.clone()).build().unwrap();
            columns.push(column);
        }

        let row_groups_metadata = vec![
            build_test_row_group_meta(schema_descr.clone(), columns.clone(), 1000, 0),
            build_test_row_group_meta(schema_descr.clone(), columns.clone(), 500, 1),
            build_test_row_group_meta(schema_descr.clone(), columns.clone(), 500, 2),
            build_test_row_group_meta(schema_descr.clone(), columns.clone(), 1000, 3),
            build_test_row_group_meta(schema_descr.clone(), columns.clone(), 500, 4),
        ];

        let selected_row_groups = Some(vec![1, 3]);

        /* cases to cover:
           * {skip|select} {first|intermediate|last} {one row|multiple rows} in
             {first|intermediate|last} {skipped|selected} row group
           * row group selection disabled
        */

        let positional_deletes = RoaringTreemap::from_iter(&[
            1, // in skipped rg 0, should be ignored
            3, // run of three consecutive items in skipped rg0
            4, 5, 998, // two consecutive items at end of skipped rg0
            999, 1000, // solitary row at start of selected rg1 (1, 9)
            1010, // run of 3 rows in selected rg1
            1011, 1012, // (3, 485)
            1498, // run of two items at end of selected rg1
            1499, 1500, // run of two items at start of skipped rg2
            1501, 1600, // should ignore, in skipped rg2
            1999, // single row at end of skipped rg2
            2000, // run of two items at start of selected rg3
            2001, // (4, 98)
            2100, // single row in selected row group 3 (1, 99)
            2200, // run of 3 consecutive rows in selected row group 3
            2201, 2202, // (3, 796)
            2999, // single item at end of selected rg3 (1)
            3000, // single item at start of skipped rg4
        ]);

        let positional_deletes = DeleteVector::new(positional_deletes);

        // using selected row groups 1 and 3
        let result = ArrowReader::build_deletes_row_selection(
            &row_groups_metadata,
            &selected_row_groups,
            &positional_deletes,
        )
        .unwrap();

        let expected = RowSelection::from(vec![
            RowSelector::skip(1),
            RowSelector::select(9),
            RowSelector::skip(3),
            RowSelector::select(485),
            RowSelector::skip(4),
            RowSelector::select(98),
            RowSelector::skip(1),
            RowSelector::select(99),
            RowSelector::skip(3),
            RowSelector::select(796),
            RowSelector::skip(1),
        ]);

        assert_eq!(result, expected);

        // selecting all row groups
        let result = ArrowReader::build_deletes_row_selection(
            &row_groups_metadata,
            &None,
            &positional_deletes,
        )
        .unwrap();

        let expected = RowSelection::from(vec![
            RowSelector::select(1),
            RowSelector::skip(1),
            RowSelector::select(1),
            RowSelector::skip(3),
            RowSelector::select(992),
            RowSelector::skip(3),
            RowSelector::select(9),
            RowSelector::skip(3),
            RowSelector::select(485),
            RowSelector::skip(4),
            RowSelector::select(98),
            RowSelector::skip(1),
            RowSelector::select(398),
            RowSelector::skip(3),
            RowSelector::select(98),
            RowSelector::skip(1),
            RowSelector::select(99),
            RowSelector::skip(3),
            RowSelector::select(796),
            RowSelector::skip(2),
            RowSelector::select(499),
        ]);

        assert_eq!(result, expected);
    }

    fn build_test_row_group_meta(
        schema_descr: SchemaDescPtr,
        columns: Vec<ColumnChunkMetaData>,
        num_rows: i64,
        ordinal: i16,
    ) -> RowGroupMetaData {
        RowGroupMetaData::builder(schema_descr.clone())
            .set_num_rows(num_rows)
            .set_total_byte_size(2000)
            .set_column_metadata(columns)
            .set_ordinal(ordinal)
            .build()
            .unwrap()
    }

    fn get_test_schema_descr() -> SchemaDescPtr {
        use parquet::schema::types::Type as SchemaType;

        let schema = SchemaType::group_type_builder("schema")
            .with_fields(vec![
                Arc::new(
                    SchemaType::primitive_type_builder("a", parquet::basic::Type::INT32)
                        .build()
                        .unwrap(),
                ),
                Arc::new(
                    SchemaType::primitive_type_builder("b", parquet::basic::Type::INT32)
                        .build()
                        .unwrap(),
                ),
            ])
            .build()
            .unwrap();

        Arc::new(SchemaDescriptor::new(Arc::new(schema)))
    }
}
