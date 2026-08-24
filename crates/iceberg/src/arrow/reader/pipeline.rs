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

//! The main `ArrowReader` pipeline: reading a stream of `FileScanTask`s,
//! opening Parquet files and resolving schemas, then wiring projection,
//! predicates, row-group / row selection, and delete handling into a stream
//! of transformed Arrow `RecordBatch`es.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use arrow_schema::{DataType, Field};
use futures::{StreamExt, TryStreamExt};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::{
    PARQUET_FIELD_ID_META_KEY, ParquetRecordBatchStreamBuilder, ProjectionMask, RowNumber,
};
use parquet::encryption::decrypt::FileDecryptionProperties;

use super::{
    ArrowFileReader, ArrowReader, ParquetReadOptions, add_fallback_field_ids_to_arrow_schema,
    apply_name_mapping_to_arrow_schema, find_leaf_by_field_id,
};
use crate::arrow::build_partition_constant;
use crate::arrow::caching_delete_file_loader::CachingDeleteFileLoader;
use crate::arrow::int96::coerce_int96_timestamps;
use crate::arrow::record_batch_transformer::RecordBatchTransformerBuilder;
use crate::arrow::scan_metrics::{CountingFileRead, ScanMetrics, ScanResult};
use crate::encryption::StandardKeyMetadata;
use crate::error::Result;
use crate::io::{FileIO, FileMetadata, FileRead};
use crate::metadata_columns::{
    RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER, RESERVED_COL_NAME_POS,
    RESERVED_COL_NAME_ROW_ID, RESERVED_FIELD_ID_FILE,
    RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER, RESERVED_FIELD_ID_PARTITION,
    RESERVED_FIELD_ID_POS, RESERVED_FIELD_ID_ROW_ID, RESERVED_FIELD_ID_SPEC_ID, is_metadata_field,
};
use crate::scan::{ArrowRecordBatchStream, FileScanTask, FileScanTaskStream};
use crate::spec::{Datum, PartitionSpec, Struct};
use crate::{Error, ErrorKind};

impl ArrowReader {
    /// Take a stream of FileScanTasks and reads all the files.
    /// Returns a [`ScanResult`] containing the record batch stream and scan metrics.
    pub fn read(self, tasks: FileScanTaskStream) -> Result<ScanResult> {
        let concurrency_limit_data_files = self.concurrency_limit_data_files;
        let scan_metrics = ScanMetrics::new();

        let task_reader = FileScanTaskReader {
            batch_size: self.batch_size,
            file_io: self.file_io,
            delete_file_loader: self
                .delete_file_loader
                .with_scan_metrics(scan_metrics.clone()),
            row_group_filtering_enabled: self.row_group_filtering_enabled,
            row_selection_enabled: self.row_selection_enabled,
            parquet_read_options: self.parquet_read_options,
            scan_metrics: scan_metrics.clone(),
        };

        // Fast-path for single concurrency to avoid overhead of try_flatten_unordered
        let stream: ArrowRecordBatchStream = if concurrency_limit_data_files == 1 {
            Box::pin(
                tasks
                    .and_then(move |task| task_reader.clone().process(task))
                    .map_err(|err| {
                        Error::new(ErrorKind::Unexpected, "file scan task generate failed")
                            .with_source(err)
                    })
                    .try_flatten(),
            )
        } else {
            Box::pin(
                tasks
                    .map_ok(move |task| task_reader.clone().process(task))
                    .map_err(|err| {
                        Error::new(ErrorKind::Unexpected, "file scan task generate failed")
                            .with_source(err)
                    })
                    .try_buffer_unordered(concurrency_limit_data_files)
                    .try_flatten_unordered(concurrency_limit_data_files),
            )
        };

        Ok(ScanResult::new(stream, scan_metrics))
    }
}

/// Per-scan state for processing [`FileScanTask`]s. Created once per
/// [`ArrowReader::read`] call and cloned per task.
#[derive(Clone)]
struct FileScanTaskReader {
    batch_size: Option<usize>,
    file_io: FileIO,
    delete_file_loader: CachingDeleteFileLoader,
    row_group_filtering_enabled: bool,
    row_selection_enabled: bool,
    parquet_read_options: ParquetReadOptions,
    scan_metrics: ScanMetrics,
}

impl FileScanTaskReader {
    async fn process(self, task: FileScanTask) -> Result<ArrowRecordBatchStream> {
        let should_load_page_index =
            (self.row_selection_enabled && task.predicate.is_some()) || !task.deletes.is_empty();
        let mut parquet_read_options = self.parquet_read_options;
        parquet_read_options.preload_page_index = should_load_page_index;

        let delete_filter_rx = self
            .delete_file_loader
            .load_deletes(&task.deletes, Arc::clone(&task.schema));

        // Open the Parquet file once, loading its metadata
        let (parquet_file_reader, arrow_metadata) = ArrowReader::open_parquet_file(
            &task.data_file_path,
            &self.file_io,
            task.file_size_in_bytes,
            parquet_read_options,
            self.scan_metrics.bytes_read_counter(),
            task.key_metadata.as_deref(),
        )
        .await?;

        // Check if Parquet file has embedded field IDs
        // Corresponds to Java's ParquetSchemaUtil.hasIds()
        // Reference: parquet/src/main/java/org/apache/iceberg/parquet/ParquetSchemaUtil.java:118
        let missing_field_ids = arrow_metadata
            .schema()
            .fields()
            .iter()
            .next()
            .is_some_and(|f| f.metadata().get(PARQUET_FIELD_ID_META_KEY).is_none());

        // Position-based fallback applies only when the file has no embedded field IDs
        // AND no name mapping is available. With a name mapping, field IDs are assigned
        // to the Arrow schema below, and projection/predicate planning must use them
        // (see #2403).
        let use_position_fallback = missing_field_ids && task.name_mapping.is_none();

        // Three-branch schema resolution strategy matching Java's ReadConf constructor
        //
        // Per Iceberg spec Column Projection rules:
        // "Columns in Iceberg data files are selected by field id. The table schema's column
        //  names and order may change after a data file is written, and projection must be done
        //  using field ids."
        // https://iceberg.apache.org/spec/#column-projection
        //
        // When Parquet files lack field IDs (e.g., Hive/Spark migrations via add_files),
        // we must assign field IDs BEFORE reading data to enable correct projection.
        //
        // Java's ReadConf determines field ID strategy:
        // - Branch 1: hasIds(fileSchema) → trust embedded field IDs, use pruneColumns()
        // - Branch 2: nameMapping present → applyNameMapping(), then pruneColumns()
        // - Branch 3: fallback → addFallbackIds(), then pruneColumnsFallback()
        let arrow_metadata = if missing_field_ids {
            // Parquet file lacks field IDs - must assign them before reading
            let arrow_schema = if let Some(name_mapping) = &task.name_mapping {
                // Branch 2: Apply name mapping to assign correct Iceberg field IDs
                // Per spec rule #2: "Use schema.name-mapping.default metadata to map field id
                // to columns without field id"
                // Corresponds to Java's ParquetSchemaUtil.applyNameMapping()
                apply_name_mapping_to_arrow_schema(
                    Arc::clone(arrow_metadata.schema()),
                    name_mapping,
                )?
            } else {
                // Branch 3: No name mapping - use position-based fallback IDs
                // Corresponds to Java's ParquetSchemaUtil.addFallbackIds()
                add_fallback_field_ids_to_arrow_schema(arrow_metadata.schema())
            };

            let options = ArrowReaderOptions::new().with_schema(arrow_schema);
            ArrowReaderMetadata::try_new(Arc::clone(arrow_metadata.metadata()), options).map_err(
                |e| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "Failed to create ArrowReaderMetadata with field ID schema",
                    )
                    .with_source(e)
                },
            )?
        } else {
            // Branch 1: File has embedded field IDs - trust them
            arrow_metadata
        };

        // Coerce INT96 timestamp columns to the resolution specified by the Iceberg schema.
        // This must happen before building the stream reader to avoid i64 overflow in arrow-rs.
        let arrow_metadata = if let Some(coerced_schema) =
            coerce_int96_timestamps(arrow_metadata.schema(), &task.schema)
        {
            let options = ArrowReaderOptions::new().with_schema(Arc::clone(&coerced_schema));
            ArrowReaderMetadata::try_new(Arc::clone(arrow_metadata.metadata()), options).map_err(
                |e| {
                    Error::new(
                        ErrorKind::Unexpected,
                        format!(
                            "Failed to create ArrowReaderMetadata with INT96-coerced schema: {coerced_schema}"
                        ),
                    )
                    .with_source(e)
                },
            )?
        } else {
            arrow_metadata
        };

        let project_pos = task.project_field_ids().contains(&RESERVED_FIELD_ID_POS);
        let project_row_id = task.project_field_ids().contains(&RESERVED_FIELD_ID_ROW_ID);

        // The RowNumber virtual column materializes `_pos`. It is also the per-row
        // positional fallback for `_row_id` (`first_row_id + pos`), so add it whenever
        // `_row_id` is synthesized. A null `first_row_id` nulls the whole `_row_id`
        // column, so nothing is synthesized and the column is not needed.
        let need_row_number = project_pos || (project_row_id && task.first_row_id.is_some());

        let arrow_metadata = if need_row_number {
            let row_number_field = Arc::new(
                Field::new(RESERVED_COL_NAME_POS, DataType::Int64, false)
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        RESERVED_FIELD_ID_POS.to_string(),
                    )]))
                    .with_extension_type(RowNumber),
            );

            let options = ArrowReaderOptions::new()
                .with_schema(Arc::clone(arrow_metadata.schema()))
                .with_virtual_columns(vec![row_number_field])?;

            ArrowReaderMetadata::try_new(Arc::clone(arrow_metadata.metadata()), options).map_err(
                |e| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "Failed to create ArrowReaderMetadata with the 'row_number' virtual_column",
                    )
                    .with_source(e)
                },
            )?
        } else {
            arrow_metadata
        };

        // Build the stream reader, reusing the already-opened file reader
        let mut record_batch_stream_builder =
            ParquetRecordBatchStreamBuilder::new_with_metadata(parquet_file_reader, arrow_metadata);

        // Whether the file physically carries the `_last_updated_sequence_number` column
        // (some engines, e.g. Iceberg Java on rewrite, write it per-row), resolved by its
        // embedded field id against the Parquet schema.
        let project_last_updated_seq = task
            .project_field_ids()
            .contains(&RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER);

        // Parquet leaf index of the physically-stored column, resolved by its embedded
        // reserved field id. `find_leaf_by_field_id` tolerates id-less leaves (e.g. a
        // Variant column's internal metadata/value leaves, which the spec requires to have
        // no id), so an unprojected variant alongside a metadata column with correct ID does
        // not hide it.
        let phys_last_updated_seq_leaf = if project_last_updated_seq {
            find_leaf_by_field_id(
                record_batch_stream_builder.parquet_schema(),
                RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
            )
        } else {
            None
        };

        // Present by name but not by the embedded id (only meaningful when no by-id column
        // was found). An unthreadable shape we reject rather than coalesce incorrectly.
        let last_updated_seq_present_by_name_only = project_last_updated_seq
            && phys_last_updated_seq_leaf.is_none()
            && record_batch_stream_builder
                .schema()
                .fields()
                .iter()
                .any(|f| f.name() == RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER);

        // Read the physical column only when first_row_id is set (with a data sequence
        // number to fall back to). A null first_row_id drops the leaf and nulls the whole
        // column below, discarding any per-row values the file carries -- matching Java
        // (`ValueReaders.lastUpdated` nulls when the base row id is null).
        let coalesce_last_updated_seq_leaf = phys_last_updated_seq_leaf
            .filter(|_| task.first_row_id.is_some() && task.data_sequence_number.is_some());

        let phys_row_id_leaf = if project_row_id {
            find_leaf_by_field_id(
                record_batch_stream_builder.parquet_schema(),
                RESERVED_FIELD_ID_ROW_ID,
            )
        } else {
            None
        };

        // Present by name but without the embedded id: unthreadable, rejected below.
        let row_id_present_by_name_only = project_row_id
            && phys_row_id_leaf.is_none()
            && record_batch_stream_builder
                .schema()
                .fields()
                .iter()
                .any(|f| f.name() == RESERVED_COL_NAME_ROW_ID);

        // Read the physical column only when first_row_id is set. A null first_row_id
        // nulls the whole column below (matching Java `ValueReaders.rowIds`).
        let coalesce_row_id_leaf = phys_row_id_leaf.filter(|_| task.first_row_id.is_some());

        // Filter out metadata fields for Parquet projection (they don't exist in files)
        let project_field_ids_without_metadata: Vec<i32> = task
            .project_field_ids
            .iter()
            .filter(|&&id| !is_metadata_field(id))
            .copied()
            .collect();

        // Create projection mask based on field IDs
        // - If file has embedded IDs: field-ID-based projection
        // - If name mapping applied: field-ID-based projection using the IDs the name
        //   mapping assigned to the Arrow schema
        // - Otherwise: position-based fallback projection
        let mut projection_mask = ArrowReader::get_arrow_projection_mask(
            &project_field_ids_without_metadata,
            &task.schema,
            record_batch_stream_builder.parquet_schema(),
            record_batch_stream_builder.schema(),
            use_position_fallback, // Whether to use position-based (true) or field-ID-based (false) projection
        )?;

        // A metadata-only projection leaves `project_field_ids_without_metadata` empty,
        // which `get_arrow_projection_mask` maps to "read all columns" (so `COUNT(*)` still
        // gets a row count). Downgrade that to "read no data columns" when a row-count
        // source exists independently of the data columns: the RowNumber virtual column
        // (installed above under `need_row_number`, which covers `_pos` and `_row_id`
        // synthesis) or a physical `_last_updated_sequence_number` leaf unioned in below
        // (that column does not install RowNumber, so it is a separate source). Pure-constant
        // / `COUNT(*)` projections have neither and must keep reading all columns to preserve
        // the row count.
        //
        // This runs BEFORE the union so the physical leaves are added onto a `none` base,
        // pruning the read to just those leaves (`union` with an `all` base stays `all`).
        if project_field_ids_without_metadata.is_empty()
            && (need_row_number || coalesce_last_updated_seq_leaf.is_some())
        {
            projection_mask =
                ProjectionMask::none(record_batch_stream_builder.parquet_schema().num_columns());
        }

        // Union in the physical leaves of any metadata columns we will coalesce. Their
        // reserved field ids are not in the task schema, so they can't be requested through
        // `get_arrow_projection_mask` (which resolves ids against the task schema); add
        // their Parquet leaves directly.
        for leaf in [coalesce_last_updated_seq_leaf, coalesce_row_id_leaf]
            .into_iter()
            .flatten()
        {
            let phys_mask =
                ProjectionMask::leaves(record_batch_stream_builder.parquet_schema(), vec![leaf]);
            projection_mask.union(&phys_mask);
        }

        record_batch_stream_builder =
            record_batch_stream_builder.with_projection(projection_mask.clone());

        // RecordBatchTransformer performs any transformations required on the RecordBatches
        // that come back from the file, such as type promotion, default column insertion,
        // column re-ordering, partition constants, and virtual field addition (like _file)
        let mut record_batch_transformer_builder =
            RecordBatchTransformerBuilder::new(task.schema_ref(), task.project_field_ids());

        // Add the _file metadata column if it's in the projected fields
        if task.project_field_ids().contains(&RESERVED_FIELD_ID_FILE) {
            let file_datum = Datum::string(task.data_file_path.clone());
            record_batch_transformer_builder =
                record_batch_transformer_builder.with_constant(RESERVED_FIELD_ID_FILE, file_datum);
        }

        if task
            .project_field_ids()
            .contains(&RESERVED_FIELD_ID_SPEC_ID)
        {
            let partition_spec = task
                .partition_spec
                .as_ref()
                .ok_or_else(|| Error::new(ErrorKind::Unexpected, "Partition spec is missing"))?;

            let spec_id_datum = Datum::int(partition_spec.spec_id());
            record_batch_transformer_builder = record_batch_transformer_builder
                .with_constant(RESERVED_FIELD_ID_SPEC_ID, spec_id_datum);
        }

        if project_last_updated_seq {
            // Materialize the column, gated on the data file's `first_row_id`. Java gates
            // it this way (`ValueReaders.lastUpdated` returns nulls when the base row id is
            // null); the spec itself only says the column is assigned the manifest entry's
            // sequence number on read.
            record_batch_transformer_builder = match (task.first_row_id, task.data_sequence_number)
            {
                (Some(_), Some(seq)) => {
                    let datum = Datum::long(seq);
                    if coalesce_last_updated_seq_leaf.is_some() {
                        // The file physically carries the column: read the per-row value,
                        // falling back to the data sequence number only where null.
                        record_batch_transformer_builder.with_coalesced_last_updated_seq_column(
                            RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
                            datum,
                        )
                    } else if last_updated_seq_present_by_name_only {
                        // Present by name but without the embedded field id (name mapping /
                        // positional fallback). The transformer keys the source column by
                        // field id, so we can't thread it; no real writer produces this, so
                        // reject loudly rather than silently overwrite with the constant.
                        // Arm-local by design: only this arm reads the physical column, so
                        // only here can a name-only column defeat us. The `(None, _)` arm
                        // nulls the column without reading it, so it needs no such guard.
                        return Err(Error::new(
                            ErrorKind::FeatureUnsupported,
                            "Reading a physically-stored _last_updated_sequence_number column \
                             without an embedded field id is not supported",
                        ));
                    } else {
                        // Column absent: derive it from the data sequence number.
                        record_batch_transformer_builder
                            .with_constant(RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER, datum)
                    }
                }
                // Null first_row_id (v1/v2, or a pre-upgrade v3 snapshot): the column is null.
                (None, _) => record_batch_transformer_builder
                    .with_null_metadata_column(RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER)?,
                // first_row_id present but no data sequence number: after manifest
                // inheritance a committed entry always has one, so this is a malformed
                // manifest rather than a legitimate null.
                (Some(_), None) => {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Data file {} has a first_row_id but no data sequence number",
                            task.data_file_path
                        ),
                    ));
                }
            };
        }

        if project_row_id {
            // Synthesize the column, gated on `first_row_id`. Java gates it the same way
            // (`ValueReaders.rowIds` returns nulls when the base row id is null); unlike
            // `_last_updated_sequence_number` there is no data-sequence-number dependency.
            // Reject a name-only physical column, but only when we would read it.
            if task.first_row_id.is_some() && row_id_present_by_name_only {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    "Reading a physically-stored _row_id column without an embedded field id \
                     is not supported",
                ));
            }

            // Synthesis (first_row_id set) and the null gate (first_row_id absent) both use
            // this builder, producing a plain Int64 column either way. When synthesizing,
            // the transformer resolves the physical `_row_id` source by field id if present,
            // else falls back to `first_row_id + pos`.
            record_batch_transformer_builder = record_batch_transformer_builder
                .with_row_id_column(RESERVED_FIELD_ID_ROW_ID, task.first_row_id);
        }

        if let (Some(partition_spec), Some(partition_data)) =
            (task.partition_spec.clone(), task.partition.clone())
        {
            record_batch_transformer_builder =
                record_batch_transformer_builder.with_partition(partition_spec, partition_data)?;
        }

        if project_pos {
            record_batch_transformer_builder =
                record_batch_transformer_builder.with_virtual_field(RESERVED_FIELD_ID_POS);
        }

        // Add the _partition metadata struct column if it's in the projected fields.
        // Computed lazily here at read time from the unified partition type + task's spec + data.
        if task
            .project_field_ids()
            .contains(&RESERVED_FIELD_ID_PARTITION)
            && let Some(unified_type) = &task.unified_partition_type
        {
            let (spec, partition_data) = match (&task.partition_spec, &task.partition) {
                (Some(spec), Some(data)) => (spec.clone(), data.clone()),
                // A missing spec/data is only acceptable when there are no partition
                // fields to fill (unpartitioned table). If the unified type has fields
                // but we lack a spec or data, the task is inconsistent and we cannot
                // build the _partition column.
                _ if unified_type.fields().is_empty() => {
                    (Arc::new(PartitionSpec::unpartition_spec()), Struct::empty())
                }
                _ => {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        "cannot build _partition column: unified partition type has fields \
                         but the scan task is missing its partition spec or data",
                    ));
                }
            };
            let constant = build_partition_constant(unified_type, &spec, &partition_data)?;
            record_batch_transformer_builder =
                record_batch_transformer_builder.with_partition_constant(constant);
        }

        let mut record_batch_transformer = record_batch_transformer_builder.build();

        if let Some(batch_size) = self.batch_size {
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

        // There are three possible sources for potential lists of selected RowGroup indices,
        // and two for `RowSelection`s.
        // Selected RowGroup index lists can come from three sources:
        //   * When task.start and task.length specify a byte range (file splitting);
        //   * When there are equality delete files that are applicable;
        //   * When there is a scan predicate and row_group_filtering_enabled = true.
        // `RowSelection`s can be created in either or both of the following cases:
        //   * When there are positional delete files that are applicable;
        //   * When there is a scan predicate and row_selection_enabled = true
        // Note that row group filtering from predicates only happens when
        // there is a scan predicate AND row_group_filtering_enabled = true,
        // but we perform row selection filtering if there are applicable
        // equality delete files OR (there is a scan predicate AND row_selection_enabled),
        // since the only implemented method of applying positional deletes is
        // by using a `RowSelection`.
        let mut selected_row_group_indices = None;
        let mut row_selection = None;

        // Filter row groups based on byte range from task.start and task.length.
        // If both start and length are 0, read the entire file (backwards compatibility).
        if task.start != 0 || task.length != 0 {
            let byte_range_filtered_row_groups = ArrowReader::filter_row_groups_by_byte_range(
                record_batch_stream_builder.metadata(),
                task.start,
                task.length,
            )?;
            selected_row_group_indices = Some(byte_range_filtered_row_groups);
        }

        if let Some(predicate) = final_predicate {
            let (iceberg_field_ids, field_id_map) = ArrowReader::build_field_id_set_and_map(
                record_batch_stream_builder.parquet_schema(),
                record_batch_stream_builder.schema(),
                &predicate,
                use_position_fallback,
            )?;

            let row_filter = ArrowReader::get_row_filter(
                &predicate,
                record_batch_stream_builder.parquet_schema(),
                &iceberg_field_ids,
                &field_id_map,
            )?;
            record_batch_stream_builder = record_batch_stream_builder.with_row_filter(row_filter);

            if self.row_group_filtering_enabled {
                let predicate_filtered_row_groups = ArrowReader::get_selected_row_group_indices(
                    &predicate,
                    record_batch_stream_builder.metadata(),
                    &field_id_map,
                    &task.schema,
                )?;

                // Merge predicate-based filtering with byte range filtering (if present)
                // by taking the intersection of both filters
                selected_row_group_indices = match selected_row_group_indices {
                    Some(byte_range_filtered) => {
                        // Keep only row groups that are in both filters
                        let intersection: Vec<usize> = byte_range_filtered
                            .into_iter()
                            .filter(|idx| predicate_filtered_row_groups.contains(idx))
                            .collect();
                        Some(intersection)
                    }
                    None => Some(predicate_filtered_row_groups),
                };
            }

            if self.row_selection_enabled {
                row_selection = ArrowReader::get_row_selection_for_filter_predicate(
                    &predicate,
                    record_batch_stream_builder.metadata(),
                    &selected_row_group_indices,
                    &field_id_map,
                    &task.schema,
                )?;
            }
        }

        let positional_delete_indexes = delete_filter.get_delete_vector(&task);

        if let Some(positional_delete_indexes) = positional_delete_indexes {
            let delete_row_selection = {
                let positional_delete_indexes = positional_delete_indexes.lock().unwrap();

                ArrowReader::build_deletes_row_selection(
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
                    Ok(batch) => {
                        // Process the record batch (type promotion, column reordering, virtual fields, etc.)
                        record_batch_transformer.process_record_batch(batch)
                    }
                    Err(err) => Err(err.into()),
                });

        Ok(Box::pin(record_batch_stream) as ArrowRecordBatchStream)
    }
}

impl ArrowReader {
    /// Opens a Parquet file and loads its metadata, wrapping the reader with
    /// [`CountingFileRead`] so all I/O is accumulated into `bytes_read`.
    pub(crate) async fn open_parquet_file(
        data_file_path: &str,
        file_io: &FileIO,
        file_size_in_bytes: u64,
        parquet_read_options: ParquetReadOptions,
        bytes_read: &Arc<AtomicU64>,
        key_metadata: Option<&[u8]>,
    ) -> Result<(ArrowFileReader, ArrowReaderMetadata)> {
        let parquet_file = file_io.new_input(data_file_path)?;
        let counting_reader =
            CountingFileRead::new(parquet_file.reader().await?, Arc::clone(bytes_read));
        Self::build_parquet_reader(
            Box::new(counting_reader),
            file_size_in_bytes,
            parquet_read_options,
            key_metadata,
        )
        .await
    }

    async fn build_parquet_reader(
        parquet_reader: Box<dyn FileRead>,
        file_size_in_bytes: u64,
        parquet_read_options: ParquetReadOptions,
        key_metadata: Option<&[u8]>,
    ) -> Result<(ArrowFileReader, ArrowReaderMetadata)> {
        let mut reader = ArrowFileReader::new(
            FileMetadata {
                size: file_size_in_bytes,
            },
            parquet_reader,
        )
        .with_parquet_read_options(parquet_read_options);

        let arrow_reader_options = Self::build_arrow_reader_options(key_metadata)?;

        let arrow_metadata = ArrowReaderMetadata::load_async(&mut reader, arrow_reader_options)
            .await
            .map_err(|e| {
                Error::new(ErrorKind::Unexpected, "Failed to load Parquet metadata").with_source(e)
            })?;

        Ok((reader, arrow_metadata))
    }

    /// Builds `ArrowReaderOptions`, adding `FileDecryptionProperties` when
    /// key metadata is present for Parquet Modular Encryption.
    fn build_arrow_reader_options(key_metadata: Option<&[u8]>) -> Result<ArrowReaderOptions> {
        match key_metadata {
            Some(km) => {
                let standard_key_metadata = StandardKeyMetadata::decode(km)?;
                let mut builder = FileDecryptionProperties::builder(
                    standard_key_metadata.encryption_key().as_bytes().to_vec(),
                );
                if let Some(aad) = standard_key_metadata.aad_prefix() {
                    builder = builder.with_aad_prefix(aad.to_vec());
                }
                let decryption_properties = builder.build().map_err(|e| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "Failed to build Parquet file decryption properties",
                    )
                    .with_source(e)
                })?;
                Ok(
                    ArrowReaderOptions::new()
                        .with_file_decryption_properties(decryption_properties),
                )
            }
            None => Ok(ArrowReaderOptions::default()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::sync::Arc;

    use arrow_array::cast::AsArray;
    use arrow_array::{Array, ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
    use arrow_cast::cast;
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use futures::TryStreamExt;
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use crate::Runtime;
    use crate::arrow::ArrowReaderBuilder;
    use crate::arrow::test_utils::write_encrypted_parquet;
    use crate::io::FileIO;
    use crate::metadata_columns::{
        RESERVED_COL_NAME_POS, RESERVED_COL_NAME_ROW_ID, RESERVED_FIELD_ID_FILE,
        RESERVED_FIELD_ID_POS, RESERVED_FIELD_ID_ROW_ID,
    };
    use crate::scan::{FileScanTask, FileScanTaskStream};
    use crate::spec::{DataFileFormat, NestedField, PrimitiveType, Schema, SchemaRef, Type};

    // INT96 encoding: [nanos_low_u32, nanos_high_u32, julian_day_u32]
    // Julian day 2_440_588 = Unix epoch (1970-01-01)
    const UNIX_EPOCH_JULIAN: i64 = 2_440_588;
    const MICROS_PER_DAY: i64 = 86_400_000_000;
    // Noon on 3333-01-01 (Julian day 2_953_529) — outside the i64 nanosecond range (~1677-2262).
    const INT96_TEST_NANOS_WITHIN_DAY: u64 = 43_200_000_000_000;
    const INT96_TEST_JULIAN_DAY: u32 = 2_953_529;

    fn make_int96_test_value() -> (parquet::data_type::Int96, i64) {
        let mut val = parquet::data_type::Int96::new();
        val.set_data(
            (INT96_TEST_NANOS_WITHIN_DAY & 0xFFFFFFFF) as u32,
            (INT96_TEST_NANOS_WITHIN_DAY >> 32) as u32,
            INT96_TEST_JULIAN_DAY,
        );
        let expected_micros = (INT96_TEST_JULIAN_DAY as i64 - UNIX_EPOCH_JULIAN) * MICROS_PER_DAY
            + (INT96_TEST_NANOS_WITHIN_DAY / 1_000) as i64;
        (val, expected_micros)
    }

    async fn read_int96_batches(
        file_path: &str,
        schema: SchemaRef,
        project_field_ids: Vec<i32>,
    ) -> Vec<RecordBatch> {
        let file_io = FileIO::new_with_fs();
        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();

        let file_size = std::fs::metadata(file_path).unwrap().len();
        let task = FileScanTask::builder()
            .with_file_size_in_bytes(file_size)
            .with_start(0)
            .with_length(file_size)
            .with_data_file_path(file_path.to_string())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema)
            .with_project_field_ids(project_field_ids)
            .with_case_sensitive(false)
            .build();

        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect()
            .await
            .unwrap()
    }

    // ArrowWriter cannot write INT96, so we use SerializedFileWriter directly.
    fn write_int96_parquet_file(
        table_location: &str,
        filename: &str,
        with_field_ids: bool,
    ) -> (String, Vec<i64>) {
        use parquet::basic::{Repetition, Type as PhysicalType};
        use parquet::data_type::{Int32Type, Int96, Int96Type};
        use parquet::file::writer::SerializedFileWriter;
        use parquet::schema::types::Type as SchemaType;

        let file_path = format!("{table_location}/{filename}");

        let mut ts_builder = SchemaType::primitive_type_builder("ts", PhysicalType::INT96)
            .with_repetition(Repetition::OPTIONAL);
        let mut id_builder = SchemaType::primitive_type_builder("id", PhysicalType::INT32)
            .with_repetition(Repetition::REQUIRED);

        if with_field_ids {
            ts_builder = ts_builder.with_id(Some(1));
            id_builder = id_builder.with_id(Some(2));
        }

        let schema = SchemaType::group_type_builder("schema")
            .with_fields(vec![
                Arc::new(ts_builder.build().unwrap()),
                Arc::new(id_builder.build().unwrap()),
            ])
            .build()
            .unwrap();

        // Dates outside the i64 nanosecond range (~1677-2262) overflow without coercion.
        const NOON_NANOS: u64 = INT96_TEST_NANOS_WITHIN_DAY;
        const JULIAN_3333: u32 = INT96_TEST_JULIAN_DAY;
        const JULIAN_2100: u32 = 2_488_070;

        let test_data: Vec<(u32, u32, u32, i64)> = vec![
            // 3333-01-01 00:00:00
            (
                0,
                0,
                JULIAN_3333,
                (JULIAN_3333 as i64 - UNIX_EPOCH_JULIAN) * MICROS_PER_DAY,
            ),
            // 3333-01-01 12:00:00
            (
                (NOON_NANOS & 0xFFFFFFFF) as u32,
                (NOON_NANOS >> 32) as u32,
                JULIAN_3333,
                (JULIAN_3333 as i64 - UNIX_EPOCH_JULIAN) * MICROS_PER_DAY
                    + (NOON_NANOS / 1_000) as i64,
            ),
            // 2100-01-01 00:00:00
            (
                0,
                0,
                JULIAN_2100,
                (JULIAN_2100 as i64 - UNIX_EPOCH_JULIAN) * MICROS_PER_DAY,
            ),
        ];

        let int96_values: Vec<Int96> = test_data
            .iter()
            .map(|(lo, hi, day, _)| {
                let mut v = Int96::new();
                v.set_data(*lo, *hi, *day);
                v
            })
            .collect();

        let id_values: Vec<i32> = (0..test_data.len() as i32).collect();
        let expected_micros: Vec<i64> = test_data.iter().map(|(_, _, _, m)| *m).collect();

        let file = File::create(&file_path).unwrap();
        let mut writer =
            SerializedFileWriter::new(file, Arc::new(schema), Default::default()).unwrap();

        let mut row_group = writer.next_row_group().unwrap();
        {
            // def=1: ts is OPTIONAL and present. No repetition levels (top-level columns).
            let mut col = row_group.next_column().unwrap().unwrap();
            col.typed::<Int96Type>()
                .write_batch(&int96_values, Some(&vec![1; test_data.len()]), None)
                .unwrap();
            col.close().unwrap();
        }
        {
            let mut col = row_group.next_column().unwrap().unwrap();
            col.typed::<Int32Type>()
                .write_batch(&id_values, None, None)
                .unwrap();
            col.close().unwrap();
        }
        row_group.close().unwrap();
        writer.close().unwrap();

        (file_path, expected_micros)
    }

    async fn assert_int96_read_matches(
        file_path: &str,
        schema: SchemaRef,
        project_field_ids: Vec<i32>,
        expected_micros: &[i64],
    ) {
        use arrow_array::TimestampMicrosecondArray;

        let batches = read_int96_batches(file_path, schema, project_field_ids).await;

        assert_eq!(batches.len(), 1);
        let ts_array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("Expected TimestampMicrosecondArray");

        for (i, expected) in expected_micros.iter().enumerate() {
            assert_eq!(
                ts_array.value(i),
                *expected,
                "Row {i}: got {}, expected {expected}",
                ts_array.value(i)
            );
        }
    }

    /// Writes a single-column Parquet file encrypted with `encryption_key`, then reads it
    /// back through `ArrowReader` and asserts the round-tripped values. The key length
    /// selects the AES-GCM variant in arrow-rs (16 -> AES-128, 32 -> AES-256).
    async fn assert_encrypted_parquet_roundtrip(encryption_key: &[u8]) {
        let aad_prefix = b"aad_prefix";

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let id_data = Arc::new(Int32Array::from(vec![10, 20, 30])) as ArrayRef;
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![id_data]).unwrap();

        let file_path = format!("{table_location}/encrypted.parquet");
        write_encrypted_parquet(&file_path, &batch, encryption_key, Some(aad_prefix));

        let key_metadata = crate::encryption::StandardKeyMetadata::try_new(encryption_key)
            .unwrap()
            .with_aad_prefix(aad_prefix)
            .encode()
            .unwrap();

        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();

        let task = FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(&file_path).unwrap().len())
            .with_start(0)
            .with_length(0)
            .with_data_file_path(file_path)
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema)
            .with_project_field_ids(vec![1])
            .with_case_sensitive(false)
            .with_key_metadata(Some(key_metadata))
            .build();

        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let batches: Vec<RecordBatch> = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.values(), &[10, 20, 30]);
    }

    #[tokio::test]
    async fn test_read_encrypted_parquet_aes_128() {
        assert_encrypted_parquet_roundtrip(b"0123456789abcdef").await;
    }

    #[tokio::test]
    async fn test_read_encrypted_parquet_aes_256() {
        assert_encrypted_parquet_roundtrip(b"0123456789abcdef0123456789abcdef").await;
    }

    #[tokio::test]
    async fn test_read_encrypted_parquet_without_key_metadata_fails() {
        let encryption_key = b"0123456789abcdef";

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let id_data = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![id_data]).unwrap();

        let file_path = format!("{table_location}/encrypted_no_key.parquet");
        write_encrypted_parquet(&file_path, &batch, encryption_key, None);

        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();

        let task = FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(&file_path).unwrap().len())
            .with_start(0)
            .with_length(0)
            .with_data_file_path(file_path)
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema)
            .with_project_field_ids(vec![1])
            .with_case_sensitive(false)
            .build();

        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let result: Result<Vec<RecordBatch>, _> =
            reader.read(tasks).unwrap().stream().try_collect().await;

        let err = result.unwrap_err();
        assert_eq!(err.kind(), crate::ErrorKind::Unexpected);
        let err_str = format!("{err}");
        assert!(
            err_str.contains("encrypted footer"),
            "Expected error about encrypted footer, got: {err_str}"
        );
        assert!(
            err_str.contains("decryption properties were not provided"),
            "Expected error about missing decryption properties, got: {err_str}"
        );
    }

    /// Writes a plain (unencrypted) single-column Int32 "id" parquet file with the
    /// given extra Arrow fields/columns appended, returning the file path.
    fn write_plain_parquet(
        dir: &str,
        name: &str,
        extra_fields: Vec<Field>,
        extra_columns: Vec<ArrayRef>,
    ) -> String {
        let mut fields =
            vec![
                Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "1".to_string(),
                )])),
            ];
        fields.extend(extra_fields);
        let arrow_schema = Arc::new(ArrowSchema::new(fields));

        let mut columns: Vec<ArrayRef> = vec![Arc::new(Int32Array::from(vec![1, 2, 3]))];
        columns.extend(extra_columns);
        let batch = RecordBatch::try_new(arrow_schema.clone(), columns).unwrap();

        let file_path = format!("{dir}/{name}");
        let file = File::create(&file_path).unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(file, arrow_schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        file_path
    }

    fn last_updated_seq_task(
        file_path: String,
        first_row_id: Option<i64>,
        data_sequence_number: Option<i64>,
    ) -> FileScanTask {
        use crate::metadata_columns::RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER;

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(&file_path).unwrap().len())
            .with_start(0)
            .with_length(0)
            .with_data_file_path(file_path)
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema)
            .with_project_field_ids(vec![1, RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER])
            .with_first_row_id(first_row_id)
            .with_data_sequence_number(data_sequence_number)
            .with_case_sensitive(false)
            .build()
    }

    /// Asserts the logical per-row values of the `_last_updated_sequence_number`
    /// column across all batches, independent of the physical (run-end) encoding.
    fn assert_last_updated_seq_column(batches: &[RecordBatch], expected: &[Option<i64>]) {
        use arrow_array::cast::AsArray;
        use arrow_cast::cast;
        use arrow_schema::DataType;

        use crate::metadata_columns::RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER;

        let mut actual = Vec::new();
        for batch in batches {
            let col = batch
                .column_by_name(RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER)
                .expect("_last_updated_sequence_number column should be present");
            let logical = cast(col, &DataType::Int64).unwrap();
            let values = logical.as_primitive::<arrow_array::types::Int64Type>();
            for i in 0..values.len() {
                actual.push((!values.is_null(i)).then(|| values.value(i)));
            }
        }
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn test_last_updated_sequence_number_null_when_no_first_row_id() {
        let tmp_dir = TempDir::new().unwrap();
        let dir = tmp_dir.path().to_str().unwrap();
        let file_path = write_plain_parquet(dir, "no_first_row_id.parquet", vec![], vec![]);

        // A file with a null first_row_id (v1/v2, or a pre-upgrade v3 snapshot) produces
        // a null _last_updated_sequence_number column, even though it has a data
        // sequence number; the spec gates both lineage columns on first_row_id.
        let task = last_updated_seq_task(file_path, None, Some(9));

        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current()).build();
        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let batches: Vec<RecordBatch> = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect()
            .await
            .unwrap();

        assert_last_updated_seq_column(&batches, &[None, None, None]);
    }

    #[tokio::test]
    async fn test_last_updated_sequence_number_error_when_no_data_seq() {
        let tmp_dir = TempDir::new().unwrap();
        let dir = tmp_dir.path().to_str().unwrap();
        let file_path = write_plain_parquet(dir, "no_data_seq.parquet", vec![], vec![]);

        // first_row_id present but data_sequence_number absent: after manifest
        // inheritance a committed entry always has one, so this is a malformed
        // manifest and must error rather than fabricate or null the column.
        let task = last_updated_seq_task(file_path, Some(42), None);

        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current()).build();
        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let result: Result<Vec<RecordBatch>, _> =
            reader.read(tasks).unwrap().stream().try_collect().await;

        let err = result.unwrap_err();
        assert_eq!(err.kind(), crate::ErrorKind::DataInvalid);
        assert!(
            format!("{err}").contains("no data sequence number"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_last_updated_sequence_number_derived_from_data_seq() {
        let tmp_dir = TempDir::new().unwrap();
        let dir = tmp_dir.path().to_str().unwrap();
        let file_path = write_plain_parquet(dir, "with_first_row_id.parquet", vec![], vec![]);

        // Non-null first_row_id + data sequence number -> the derived value (the data
        // sequence number) for every row. This is the only value-producing arm.
        let task = last_updated_seq_task(file_path, Some(42), Some(7));

        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current()).build();
        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let batches: Vec<RecordBatch> = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect()
            .await
            .unwrap();

        assert_last_updated_seq_column(&batches, &[Some(7), Some(7), Some(7)]);
    }

    #[tokio::test]
    async fn test_last_updated_sequence_number_mixed_files_share_schema() {
        use arrow_select::concat::concat_batches;

        let tmp_dir = TempDir::new().unwrap();
        let dir = tmp_dir.path().to_str().unwrap();

        // Three files in one scan exercising all three column paths, which must all
        // produce the SAME Arrow type (run-end-encoded) or concatenation fails:
        //   - constant: first_row_id set, no physical column -> derived constant
        //   - null gate: no first_row_id -> null column
        //   - coalesce: first_row_id set, physical column present -> per-row + fallback
        let constant = last_updated_seq_task(
            write_plain_parquet(dir, "constant.parquet", vec![], vec![]),
            Some(42),
            Some(7),
        );
        let nulled = last_updated_seq_task(
            write_plain_parquet(dir, "nulled.parquet", vec![], vec![]),
            None,
            Some(7),
        );
        let coalesced = last_updated_seq_task(
            write_plain_parquet(
                dir,
                "coalesced.parquet",
                vec![physical_last_updated_seq_field()],
                vec![Arc::new(Int64Array::from(vec![Some(5), None, Some(8)])) as ArrayRef],
            ),
            Some(50),
            Some(7),
        );

        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current()).build();
        let tasks = Box::pin(futures::stream::iter(vec![
            Ok(constant),
            Ok(nulled),
            Ok(coalesced),
        ])) as FileScanTaskStream;
        let batches: Vec<RecordBatch> = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect()
            .await
            .unwrap();

        assert_eq!(batches.len(), 3);
        // Identical schema across all three paths -> concat succeeds.
        let schema = batches[0].schema();
        concat_batches(&schema, &batches)
            .expect("constant, null and coalesce files must share one column type");
    }

    /// A parquet field carrying the embedded `_last_updated_sequence_number` field id.
    fn physical_last_updated_seq_field() -> Field {
        use crate::metadata_columns::{
            RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER,
            RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
        };
        Field::new(
            RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER,
            DataType::Int64,
            true,
        )
        .with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER.to_string(),
        )]))
    }

    #[tokio::test]
    async fn test_last_updated_sequence_number_physical_column_coalesced() {
        let tmp_dir = TempDir::new().unwrap();
        let dir = tmp_dir.path().to_str().unwrap();
        // A file that physically carries the column, as Iceberg Java writes when
        // carrying rows forward across a rewrite: some rows have a stored value, some
        // are null (added/modified rows, inherited on read).
        let seq_col = Arc::new(Int64Array::from(vec![Some(5), None, Some(8)])) as ArrayRef;
        let file_path = write_plain_parquet(
            dir,
            "with_seq.parquet",
            vec![physical_last_updated_seq_field()],
            vec![seq_col],
        );

        let task = last_updated_seq_task(file_path, Some(100), Some(9));

        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current()).build();
        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let batches: Vec<RecordBatch> = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect()
            .await
            .unwrap();

        // Per-row value where non-null; the data sequence number (9) where null.
        assert_last_updated_seq_column(&batches, &[Some(5), Some(9), Some(8)]);
    }

    #[tokio::test]
    async fn test_last_updated_sequence_number_coalesced_with_pos_column() {
        use crate::metadata_columns::{
            RESERVED_COL_NAME_POS, RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
            RESERVED_FIELD_ID_POS,
        };

        let tmp_dir = TempDir::new().unwrap();
        let dir = tmp_dir.path().to_str().unwrap();
        let seq_col = Arc::new(Int64Array::from(vec![Some(5), None, Some(8)])) as ArrayRef;
        let file_path = write_plain_parquet(
            dir,
            "with_seq_and_pos.parquet",
            vec![physical_last_updated_seq_field()],
            vec![seq_col],
        );

        // Co-project `_pos` (a virtual column appended to the Arrow output schema) with the
        // physical coalesce column. This guards that the physical column's index is
        // resolved in the Parquet schema, not the Arrow schema (whose indices shift once
        // virtual columns are appended).
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );
        let task = FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(&file_path).unwrap().len())
            .with_start(0)
            .with_length(0)
            .with_data_file_path(file_path)
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema)
            .with_project_field_ids(vec![
                1,
                RESERVED_FIELD_ID_POS,
                RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
            ])
            .with_first_row_id(Some(100))
            .with_data_sequence_number(Some(9))
            .with_case_sensitive(false)
            .build();

        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current()).build();
        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let batches: Vec<RecordBatch> = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect()
            .await
            .unwrap();

        // The seq column still coalesces correctly...
        assert_last_updated_seq_column(&batches, &[Some(5), Some(9), Some(8)]);
        // ...and `_pos` is the row position, unaffected by the physical-column union.
        let pos_col = batches[0]
            .column_by_name(RESERVED_COL_NAME_POS)
            .expect("_pos column should be present")
            .as_primitive::<arrow_array::types::Int64Type>();
        assert_eq!(pos_col.values(), &[0, 1, 2]);
    }

    #[tokio::test]
    async fn test_last_updated_sequence_number_physical_column_nulled_without_first_row_id() {
        let tmp_dir = TempDir::new().unwrap();
        let dir = tmp_dir.path().to_str().unwrap();
        let seq_col = Arc::new(Int64Array::from(vec![Some(5), None, Some(8)])) as ArrayRef;
        let file_path = write_plain_parquet(
            dir,
            "with_seq_no_first_row_id.parquet",
            vec![physical_last_updated_seq_field()],
            vec![seq_col],
        );

        // Null first_row_id: the whole column is null even though the file physically
        // carries per-row values -- the gate wins, and the physical column is not read.
        let task = last_updated_seq_task(file_path, None, Some(9));

        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current()).build();
        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let batches: Vec<RecordBatch> = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect()
            .await
            .unwrap();

        assert_last_updated_seq_column(&batches, &[None, None, None]);
    }

    #[tokio::test]
    async fn test_last_updated_sequence_number_present_by_name_without_id_unsupported() {
        let tmp_dir = TempDir::new().unwrap();
        let dir = tmp_dir.path().to_str().unwrap();
        // Column present by name but WITHOUT the embedded field id (e.g. name mapping /
        // positional fallback). The transformer keys the source column by field id, so
        // this shape can't be threaded and is rejected loudly.
        let seq_field = Field::new(
            crate::metadata_columns::RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER,
            DataType::Int64,
            true,
        );
        let seq_col = Arc::new(Int64Array::from(vec![Some(5), None, Some(8)])) as ArrayRef;
        let file_path =
            write_plain_parquet(dir, "with_seq_by_name.parquet", vec![seq_field], vec![
                seq_col,
            ]);

        let task = last_updated_seq_task(file_path, Some(100), Some(9));

        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current()).build();
        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let result: Result<Vec<RecordBatch>, _> =
            reader.read(tasks).unwrap().stream().try_collect().await;

        let err = result.unwrap_err();
        assert_eq!(err.kind(), crate::ErrorKind::FeatureUnsupported);
        assert!(
            format!("{err}").contains("without an embedded field id"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_last_updated_sequence_number_physical_column_first_row_id_without_data_seq() {
        let tmp_dir = TempDir::new().unwrap();
        let dir = tmp_dir.path().to_str().unwrap();
        let seq_col = Arc::new(Int64Array::from(vec![Some(5), None, Some(8)])) as ArrayRef;
        let file_path = write_plain_parquet(
            dir,
            "with_seq_no_data_seq.parquet",
            vec![physical_last_updated_seq_field()],
            vec![seq_col],
        );

        // first_row_id set but no data sequence number: after manifest inheritance a
        // committed entry always has one, so this is a malformed manifest, rejected loudly
        // rather than nulled.
        let task = last_updated_seq_task(file_path, Some(100), None);

        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current()).build();
        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let result: Result<Vec<RecordBatch>, _> =
            reader.read(tasks).unwrap().stream().try_collect().await;

        let err = result.unwrap_err();
        assert_eq!(err.kind(), crate::ErrorKind::DataInvalid);
        assert!(
            format!("{err}").contains("no data sequence number"),
            "unexpected error: {err}"
        );
    }

    /// A scan task projecting `id` + `_row_id`, with the given `first_row_id`.
    fn row_id_task(file_path: String, first_row_id: Option<i64>) -> FileScanTask {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(&file_path).unwrap().len())
            .with_start(0)
            .with_length(0)
            .with_data_file_path(file_path)
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema)
            .with_project_field_ids(vec![1, RESERVED_FIELD_ID_ROW_ID])
            .with_first_row_id(first_row_id)
            .with_case_sensitive(false)
            .build()
    }

    /// Asserts the logical per-row values of the `_row_id` column across all batches,
    /// independent of the physical (run-end) encoding.
    fn assert_row_id_column(batches: &[RecordBatch], expected: &[Option<i64>]) {
        use arrow_array::cast::AsArray;
        use arrow_cast::cast;
        use arrow_schema::DataType;

        let mut actual = Vec::new();
        for batch in batches {
            let col = batch
                .column_by_name(RESERVED_COL_NAME_ROW_ID)
                .expect("_row_id column should be present");
            let logical = cast(col, &DataType::Int64).unwrap();
            let values = logical.as_primitive::<arrow_array::types::Int64Type>();
            for i in 0..values.len() {
                actual.push((!values.is_null(i)).then(|| values.value(i)));
            }
        }
        assert_eq!(actual, expected);
    }

    /// A parquet field carrying the embedded `_row_id` field id.
    fn physical_row_id_field() -> Field {
        Field::new(RESERVED_COL_NAME_ROW_ID, DataType::Int64, true).with_metadata(HashMap::from([
            (
                PARQUET_FIELD_ID_META_KEY.to_string(),
                RESERVED_FIELD_ID_ROW_ID.to_string(),
            ),
        ]))
    }

    #[tokio::test]
    async fn test_row_id_synthesized_from_first_row_id_and_pos() {
        let tmp_dir = TempDir::new().unwrap();
        let dir = tmp_dir.path().to_str().unwrap();
        let file_path = write_plain_parquet(dir, "row_id_synth.parquet", vec![], vec![]);

        // No physical column: every row is first_row_id + pos.
        let task = row_id_task(file_path, Some(100));

        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current()).build();
        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let batches: Vec<RecordBatch> = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect()
            .await
            .unwrap();

        assert_row_id_column(&batches, &[Some(100), Some(101), Some(102)]);
    }

    #[tokio::test]
    async fn test_row_id_physical_column_coalesced() {
        let tmp_dir = TempDir::new().unwrap();
        let dir = tmp_dir.path().to_str().unwrap();
        // A file that physically carries `_row_id`, as written when carrying rows forward
        // across a rewrite: some rows have a stored value, some are null.
        let id_col = Arc::new(Int64Array::from(vec![Some(5), None, Some(8)])) as ArrayRef;
        let file_path = write_plain_parquet(
            dir,
            "row_id_phys.parquet",
            vec![physical_row_id_field()],
            vec![id_col],
        );

        let task = row_id_task(file_path, Some(100));

        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current()).build();
        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let batches: Vec<RecordBatch> = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect()
            .await
            .unwrap();

        // Per-row value where non-null; first_row_id + pos (101) where null.
        assert_row_id_column(&batches, &[Some(5), Some(101), Some(8)]);
    }

    #[tokio::test]
    async fn test_row_id_only_synthesis_reads_no_data_columns() {
        // The common v3 case: a new-row file with `first_row_id` set and NO physically
        // stored `_row_id`, projecting only `_row_id`. `_row_id` synthesis installs the
        // RowNumber virtual column (via `need_row_number`), so the row count comes from it
        // -- the scan must read no data columns, not fall back to reading everything.
        let tmp_dir = TempDir::new().unwrap();
        let dir = tmp_dir.path().to_str().unwrap();

        let mut meta_only = metadata_projection_task(
            write_parquet_with_wide_column(dir, "row_id_only.parquet", vec![], vec![]),
            id_and_wide_schema(),
            vec![RESERVED_FIELD_ID_ROW_ID],
        );
        meta_only.first_row_id = Some(100);
        let (batches, meta_only_bytes) = scan_task(meta_only).await;

        assert_eq!(batches[0].num_columns(), 1);
        assert_row_id_column(&batches, &[Some(100), Some(101), Some(102)]);

        // A scan that also projects the wide data column must read materially more.
        let mut with_data = metadata_projection_task(
            write_parquet_with_wide_column(dir, "row_id_only_ref.parquet", vec![], vec![]),
            id_and_wide_schema(),
            vec![2, RESERVED_FIELD_ID_ROW_ID],
        );
        with_data.first_row_id = Some(100);
        let (_, with_data_bytes) = scan_task(with_data).await;

        assert!(
            meta_only_bytes < with_data_bytes,
            "_row_id-only synthesis should read fewer bytes than a scan of the wide column: \
             {meta_only_bytes} vs {with_data_bytes}"
        );
    }

    #[tokio::test]
    async fn test_row_id_resolves_alongside_id_less_leaf() {
        // A file with an id-less leaf (mimicking a Variant column's internal metadata/value
        // leaves, which the spec requires to have no field id) plus a correctly-IDed
        // physical `_row_id`. The reserved id must still resolve -- an all-or-nothing field
        // map would bail on the id-less leaf and wrongly reject the file.
        let tmp_dir = TempDir::new().unwrap();
        let dir = tmp_dir.path().to_str().unwrap();
        let idless_field = Field::new("variant_internal", DataType::Utf8, true);
        let idless_col = Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef;
        let row_id_col = Arc::new(Int64Array::from(vec![Some(5), None, Some(8)])) as ArrayRef;
        let file_path = write_plain_parquet(
            dir,
            "row_id_with_idless_leaf.parquet",
            vec![idless_field, physical_row_id_field()],
            vec![idless_col, row_id_col],
        );

        let task = row_id_task(file_path, Some(100));

        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current()).build();
        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let batches: Vec<RecordBatch> = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect()
            .await
            .unwrap();

        // Physical value where non-null; first_row_id + pos (101) where null.
        assert_row_id_column(&batches, &[Some(5), Some(101), Some(8)]);
    }

    #[tokio::test]
    async fn test_row_id_and_last_updated_seq_co_projected() {
        use crate::metadata_columns::RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER;

        // Both lineage columns projected together over a file carrying both physical
        // leaves. Each must materialize independently -- neither leaf's mask clobbers the
        // other, and the two synthesized columns keep their own values.
        let tmp_dir = TempDir::new().unwrap();
        let dir = tmp_dir.path().to_str().unwrap();
        let row_id_col = Arc::new(Int64Array::from(vec![Some(5), None, Some(8)])) as ArrayRef;
        let seq_col = Arc::new(Int64Array::from(vec![Some(50), None, Some(70)])) as ArrayRef;
        let file_path = write_plain_parquet(
            dir,
            "row_id_and_seq.parquet",
            vec![physical_row_id_field(), physical_last_updated_seq_field()],
            vec![row_id_col, seq_col],
        );

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );
        let task = FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(&file_path).unwrap().len())
            .with_start(0)
            .with_length(0)
            .with_data_file_path(file_path)
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema)
            .with_project_field_ids(vec![
                1,
                RESERVED_FIELD_ID_ROW_ID,
                RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
            ])
            .with_first_row_id(Some(100))
            .with_data_sequence_number(Some(9))
            .with_case_sensitive(false)
            .build();

        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current()).build();
        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let batches: Vec<RecordBatch> = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect()
            .await
            .unwrap();

        // _row_id: physical value where non-null, else first_row_id + pos (101).
        assert_row_id_column(&batches, &[Some(5), Some(101), Some(8)]);
        // _last_updated_sequence_number: physical value where non-null, else data seq (9).
        assert_last_updated_seq_column(&batches, &[Some(50), Some(9), Some(70)]);
    }

    #[tokio::test]
    async fn test_row_id_null_when_no_first_row_id() {
        let tmp_dir = TempDir::new().unwrap();
        let dir = tmp_dir.path().to_str().unwrap();
        // Physically carries `_row_id`, but the file has a null first_row_id.
        let id_col = Arc::new(Int64Array::from(vec![Some(5), Some(6), Some(7)])) as ArrayRef;
        let file_path = write_plain_parquet(
            dir,
            "row_id_no_first.parquet",
            vec![physical_row_id_field()],
            vec![id_col],
        );

        // Null first_row_id: the whole column is null; the physical values are not read.
        let task = row_id_task(file_path, None);

        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current()).build();
        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let batches: Vec<RecordBatch> = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect()
            .await
            .unwrap();

        assert_row_id_column(&batches, &[None, None, None]);
    }

    #[tokio::test]
    async fn test_row_id_with_pos_column() {
        use crate::metadata_columns::RESERVED_COL_NAME_POS;

        let tmp_dir = TempDir::new().unwrap();
        let dir = tmp_dir.path().to_str().unwrap();
        let id_col = Arc::new(Int64Array::from(vec![Some(5), None, Some(8)])) as ArrayRef;
        let file_path = write_plain_parquet(
            dir,
            "row_id_and_pos.parquet",
            vec![physical_row_id_field()],
            vec![id_col],
        );

        // Co-project `_pos` and `_row_id`. `_row_id` synthesis consumes the position, and
        // `_pos` is also emitted -- the RowNumber column must be added once and the two
        // must not interfere.
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );
        let task = FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(&file_path).unwrap().len())
            .with_start(0)
            .with_length(0)
            .with_data_file_path(file_path)
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema)
            .with_project_field_ids(vec![1, RESERVED_FIELD_ID_POS, RESERVED_FIELD_ID_ROW_ID])
            .with_first_row_id(Some(100))
            .with_case_sensitive(false)
            .build();

        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current()).build();
        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let batches: Vec<RecordBatch> = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect()
            .await
            .unwrap();

        // `_row_id` coalesces correctly...
        assert_row_id_column(&batches, &[Some(5), Some(101), Some(8)]);
        // ...and `_pos` is the row position, not double-counted.
        let pos_col = batches[0]
            .column_by_name(RESERVED_COL_NAME_POS)
            .expect("_pos column should be present")
            .as_primitive::<arrow_array::types::Int64Type>();
        assert_eq!(pos_col.values(), &[0, 1, 2]);
    }

    #[tokio::test]
    async fn test_row_id_mixed_files_share_schema() {
        use arrow_select::concat::concat_batches;

        let tmp_dir = TempDir::new().unwrap();
        let dir = tmp_dir.path().to_str().unwrap();

        // Three files in one scan exercising all three column paths, which must all
        // produce the SAME Arrow type (plain Int64) or concatenation fails:
        //   - synthesis: first_row_id set, no physical column -> first_row_id + pos
        //   - null gate: no first_row_id -> null column
        //   - coalesce: first_row_id set, physical column present -> per-row + fallback
        let synth = row_id_task(
            write_plain_parquet(dir, "row_id_synth2.parquet", vec![], vec![]),
            Some(42),
        );
        let nulled = row_id_task(
            write_plain_parquet(dir, "row_id_null2.parquet", vec![], vec![]),
            None,
        );
        let coalesced = row_id_task(
            write_plain_parquet(
                dir,
                "row_id_coalesced2.parquet",
                vec![physical_row_id_field()],
                vec![Arc::new(Int64Array::from(vec![Some(5), None, Some(8)])) as ArrayRef],
            ),
            Some(50),
        );

        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current()).build();
        let tasks = Box::pin(futures::stream::iter(vec![
            Ok(synth),
            Ok(nulled),
            Ok(coalesced),
        ])) as FileScanTaskStream;
        let batches: Vec<RecordBatch> = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect()
            .await
            .unwrap();

        assert_eq!(batches.len(), 3);
        let schema = batches[0].schema();
        concat_batches(&schema, &batches)
            .expect("synthesis, null and coalesce files must share one column type");
    }

    #[tokio::test]
    async fn test_row_id_present_by_name_without_id_unsupported() {
        let tmp_dir = TempDir::new().unwrap();
        let dir = tmp_dir.path().to_str().unwrap();
        // Column present by name but WITHOUT the embedded field id. The transformer keys
        // the source column by field id, so this shape can't be threaded and is rejected.
        let id_field = Field::new(RESERVED_COL_NAME_ROW_ID, DataType::Int64, true);
        let id_col = Arc::new(Int64Array::from(vec![Some(5), None, Some(8)])) as ArrayRef;
        let file_path =
            write_plain_parquet(dir, "row_id_by_name.parquet", vec![id_field], vec![id_col]);

        let task = row_id_task(file_path, Some(100));

        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current()).build();
        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let result: Result<Vec<RecordBatch>, _> =
            reader.read(tasks).unwrap().stream().try_collect().await;

        let err = result.unwrap_err();
        assert_eq!(err.kind(), crate::ErrorKind::FeatureUnsupported);
        assert!(
            format!("{err}").contains("without an embedded field id"),
            "unexpected error: {err}"
        );
    }

    /// Builds a `row_id_task` (see above) that additionally carries a bound predicate,
    /// so a `RowSelection` is applied when the reader has row selection enabled.
    fn row_id_task_with_predicate(
        file_path: String,
        first_row_id: Option<i64>,
        extra_project_field_ids: Vec<i32>,
        predicate: crate::expr::Predicate,
    ) -> FileScanTask {
        use crate::expr::Bind;

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );
        let bound = predicate.bind(Arc::clone(&schema), false).unwrap();

        let mut project_field_ids = vec![1];
        project_field_ids.extend(extra_project_field_ids);
        project_field_ids.push(RESERVED_FIELD_ID_ROW_ID);

        FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(&file_path).unwrap().len())
            .with_start(0)
            .with_length(0)
            .with_data_file_path(file_path)
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema)
            .with_project_field_ids(project_field_ids)
            .with_predicate(Some(bound))
            .with_first_row_id(first_row_id)
            .with_case_sensitive(false)
            .build()
    }

    #[tokio::test]
    async fn test_row_id_stable_under_row_selection() {
        use crate::expr::Reference;
        use crate::spec::Datum;

        let tmp_dir = TempDir::new().unwrap();
        let dir = tmp_dir.path().to_str().unwrap();
        // id = [1, 2, 3]; drop the middle physical row via a predicate + row selection.
        let file_path = write_plain_parquet(dir, "row_id_selection.parquet", vec![], vec![]);

        let task = row_id_task_with_predicate(
            file_path,
            Some(100),
            vec![],
            Reference::new("id").not_equal_to(Datum::int(2)),
        );

        // Row selection must be enabled for the predicate to produce a RowSelection.
        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current())
            .with_row_selection_enabled(true)
            .build();
        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let batches: Vec<RecordBatch> = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect()
            .await
            .unwrap();

        // The survivors are physical rows 0 and 2, so their _row_id is first_row_id + the
        // PHYSICAL position: [100, 102]. A dense output index would wrongly give [100, 101].
        assert_row_id_column(&batches, &[Some(100), Some(102)]);
    }

    #[tokio::test]
    async fn test_row_id_coalesce_stable_under_row_selection() {
        use crate::expr::Reference;
        use crate::spec::Datum;

        let tmp_dir = TempDir::new().unwrap();
        let dir = tmp_dir.path().to_str().unwrap();
        // Physical _row_id = [Some(5), None, Some(8)] over id = [1, 2, 3]. Dropping the
        // middle row must keep the physical column and the RowNumber fallback row-aligned.
        let id_col = Arc::new(Int64Array::from(vec![Some(5), None, Some(8)])) as ArrayRef;
        let file_path = write_plain_parquet(
            dir,
            "row_id_coalesce_selection.parquet",
            vec![physical_row_id_field()],
            vec![id_col],
        );

        let task = row_id_task_with_predicate(
            file_path,
            Some(100),
            vec![],
            Reference::new("id").not_equal_to(Datum::int(2)),
        );

        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current())
            .with_row_selection_enabled(true)
            .build();
        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let batches: Vec<RecordBatch> = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect()
            .await
            .unwrap();

        // Rows 0 and 2 survive: their stored values (5, 8) pass through. The dropped
        // row's null (which would have fallen back to 100 + 1) is gone -- proving the
        // physical column and the positional fallback are filtered by the same selection.
        assert_row_id_column(&batches, &[Some(5), Some(8)]);
    }

    #[tokio::test]
    async fn test_read_encrypted_parquet_with_wrong_key_fails() {
        let encryption_key = b"0123456789abcdef";
        let wrong_key = b"fedcba9876543210";

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let id_data = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![id_data]).unwrap();

        let file_path = format!("{table_location}/encrypted_wrong_key.parquet");
        write_encrypted_parquet(&file_path, &batch, encryption_key, None);

        let wrong_key_metadata = crate::encryption::StandardKeyMetadata::try_new(wrong_key)
            .unwrap()
            .encode()
            .unwrap();

        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();

        let task = FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(&file_path).unwrap().len())
            .with_start(0)
            .with_length(0)
            .with_data_file_path(file_path)
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema)
            .with_project_field_ids(vec![1])
            .with_case_sensitive(false)
            .with_key_metadata(Some(wrong_key_metadata))
            .build();

        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let result: Result<Vec<RecordBatch>, _> =
            reader.read(tasks).unwrap().stream().try_collect().await;

        let err = result.unwrap_err();
        assert_eq!(err.kind(), crate::ErrorKind::Unexpected);
        let err_str = format!("{err}");
        assert!(
            err_str.contains("unable to decrypt parquet footer"),
            "Expected error about decryption failure, got: {err_str}"
        );
    }

    /// Test that concurrency=1 reads all files correctly and in deterministic order.
    /// This verifies the fast-path optimization for single concurrency.
    #[tokio::test]
    async fn test_read_with_concurrency_one() {
        use arrow_array::Int32Array;

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "file_num", Type::Primitive(PrimitiveType::Int))
                        .into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            Field::new("file_num", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        // Create 3 parquet files with different data
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        for file_num in 0..3 {
            let id_data = Arc::new(Int32Array::from_iter_values(
                file_num * 10..(file_num + 1) * 10,
            )) as ArrayRef;
            let file_num_data = Arc::new(Int32Array::from(vec![file_num; 10])) as ArrayRef;

            let to_write =
                RecordBatch::try_new(arrow_schema.clone(), vec![id_data, file_num_data]).unwrap();

            let file = File::create(format!("{table_location}/file_{file_num}.parquet")).unwrap();
            let mut writer =
                ArrowWriter::try_new(file, to_write.schema(), Some(props.clone())).unwrap();
            writer.write(&to_write).expect("Writing batch");
            writer.close().unwrap();
        }

        // Read with concurrency=1 (fast-path)
        let reader = ArrowReaderBuilder::new(file_io, Runtime::current())
            .with_data_file_concurrency_limit(1)
            .build();

        // Create tasks in a specific order: file_0, file_1, file_2
        let tasks = vec![
            Ok(FileScanTask::builder()
                .with_file_size_in_bytes(
                    std::fs::metadata(format!("{table_location}/file_0.parquet"))
                        .unwrap()
                        .len(),
                )
                .with_start(0)
                .with_length(0)
                .with_data_file_path(format!("{table_location}/file_0.parquet"))
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(schema.clone())
                .with_project_field_ids(vec![1, 2])
                .with_case_sensitive(false)
                .build()),
            Ok(FileScanTask::builder()
                .with_file_size_in_bytes(
                    std::fs::metadata(format!("{table_location}/file_1.parquet"))
                        .unwrap()
                        .len(),
                )
                .with_start(0)
                .with_length(0)
                .with_data_file_path(format!("{table_location}/file_1.parquet"))
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(schema.clone())
                .with_project_field_ids(vec![1, 2])
                .with_case_sensitive(false)
                .build()),
            Ok(FileScanTask::builder()
                .with_file_size_in_bytes(
                    std::fs::metadata(format!("{table_location}/file_2.parquet"))
                        .unwrap()
                        .len(),
                )
                .with_start(0)
                .with_length(0)
                .with_data_file_path(format!("{table_location}/file_2.parquet"))
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(schema.clone())
                .with_project_field_ids(vec![1, 2])
                .with_case_sensitive(false)
                .build()),
        ];

        let tasks_stream = Box::pin(futures::stream::iter(tasks)) as FileScanTaskStream;

        let result = reader
            .read(tasks_stream)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        // Verify we got all 30 rows (10 from each file)
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 30, "Should have 30 total rows");

        // Collect all ids and file_nums to verify data
        let mut all_ids = Vec::new();
        let mut all_file_nums = Vec::new();

        for batch in &result {
            let id_col = batch
                .column(0)
                .as_primitive::<arrow_array::types::Int32Type>();
            let file_num_col = batch
                .column(1)
                .as_primitive::<arrow_array::types::Int32Type>();

            for i in 0..batch.num_rows() {
                all_ids.push(id_col.value(i));
                all_file_nums.push(file_num_col.value(i));
            }
        }

        assert_eq!(all_ids.len(), 30);
        assert_eq!(all_file_nums.len(), 30);

        // With concurrency=1 and sequential processing, files should be processed in order
        // file_0: ids 0-9, file_num=0
        // file_1: ids 10-19, file_num=1
        // file_2: ids 20-29, file_num=2
        for i in 0..10 {
            assert_eq!(all_file_nums[i], 0, "First 10 rows should be from file_0");
            assert_eq!(all_ids[i], i as i32, "IDs should be 0-9");
        }
        for i in 10..20 {
            assert_eq!(all_file_nums[i], 1, "Next 10 rows should be from file_1");
            assert_eq!(all_ids[i], i as i32, "IDs should be 10-19");
        }
        for i in 20..30 {
            assert_eq!(all_file_nums[i], 2, "Last 10 rows should be from file_2");
            assert_eq!(all_ids[i], i as i32, "IDs should be 20-29");
        }
    }

    #[tokio::test]
    async fn test_read_int96_timestamps_with_field_ids() {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(1, "ts", Type::Primitive(PrimitiveType::Timestamp))
                        .into(),
                    NestedField::required(2, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let (file_path, expected_micros) =
            write_int96_parquet_file(&table_location, "with_ids.parquet", true);

        assert_int96_read_matches(&file_path, schema, vec![1, 2], &expected_micros).await;
    }

    #[tokio::test]
    async fn test_read_int96_timestamps_without_field_ids() {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(1, "ts", Type::Primitive(PrimitiveType::Timestamp))
                        .into(),
                    NestedField::required(2, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let (file_path, expected_micros) =
            write_int96_parquet_file(&table_location, "no_ids.parquet", false);

        assert_int96_read_matches(&file_path, schema, vec![1, 2], &expected_micros).await;
    }

    #[tokio::test]
    async fn test_read_int96_timestamps_in_struct() {
        use arrow_array::{StructArray, TimestampMicrosecondArray};
        use parquet::basic::{Repetition, Type as PhysicalType};
        use parquet::data_type::Int96Type;
        use parquet::file::writer::SerializedFileWriter;
        use parquet::schema::types::Type as SchemaType;

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_path = format!("{table_location}/struct_int96.parquet");

        let ts_type = SchemaType::primitive_type_builder("ts", PhysicalType::INT96)
            .with_repetition(Repetition::OPTIONAL)
            .with_id(Some(2))
            .build()
            .unwrap();

        let struct_type = SchemaType::group_type_builder("data")
            .with_repetition(Repetition::REQUIRED)
            .with_id(Some(1))
            .with_fields(vec![Arc::new(ts_type)])
            .build()
            .unwrap();

        let parquet_schema = SchemaType::group_type_builder("schema")
            .with_fields(vec![Arc::new(struct_type)])
            .build()
            .unwrap();

        let (int96_val, expected_micros) = make_int96_test_value();

        let file = File::create(&file_path).unwrap();
        let mut writer =
            SerializedFileWriter::new(file, Arc::new(parquet_schema), Default::default()).unwrap();

        // def=1: struct is REQUIRED so no level, ts is OPTIONAL and present (1).
        // No repetition levels needed (no repeated groups).
        let mut row_group = writer.next_row_group().unwrap();
        {
            let mut col = row_group.next_column().unwrap().unwrap();
            col.typed::<Int96Type>()
                .write_batch(&[int96_val], Some(&[1]), None)
                .unwrap();
            col.close().unwrap();
        }
        row_group.close().unwrap();
        writer.close().unwrap();

        let iceberg_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(
                        1,
                        "data",
                        Type::Struct(crate::spec::StructType::new(vec![
                            NestedField::optional(
                                2,
                                "ts",
                                Type::Primitive(PrimitiveType::Timestamp),
                            )
                            .into(),
                        ])),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        let batches = read_int96_batches(&file_path, iceberg_schema, vec![1]).await;

        assert_eq!(batches.len(), 1);
        let struct_array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("Expected StructArray");
        let ts_array = struct_array
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("Expected TimestampMicrosecondArray inside struct");

        assert_eq!(
            ts_array.value(0),
            expected_micros,
            "INT96 in struct: got {}, expected {expected_micros}",
            ts_array.value(0)
        );
    }

    #[tokio::test]
    async fn test_read_int96_timestamps_in_list() {
        use arrow_array::{ListArray, TimestampMicrosecondArray};
        use parquet::basic::{Repetition, Type as PhysicalType};
        use parquet::data_type::Int96Type;
        use parquet::file::writer::SerializedFileWriter;
        use parquet::schema::types::Type as SchemaType;

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_path = format!("{table_location}/list_int96.parquet");

        // 3-level LIST encoding:
        //   optional group timestamps (LIST) {
        //     repeated group list {
        //       optional int96 element;
        //     }
        //   }
        let element_type = SchemaType::primitive_type_builder("element", PhysicalType::INT96)
            .with_repetition(Repetition::OPTIONAL)
            .with_id(Some(2))
            .build()
            .unwrap();

        let list_group = SchemaType::group_type_builder("list")
            .with_repetition(Repetition::REPEATED)
            .with_fields(vec![Arc::new(element_type)])
            .build()
            .unwrap();

        let list_type = SchemaType::group_type_builder("timestamps")
            .with_repetition(Repetition::OPTIONAL)
            .with_id(Some(1))
            .with_logical_type(Some(parquet::basic::LogicalType::List))
            .with_fields(vec![Arc::new(list_group)])
            .build()
            .unwrap();

        let parquet_schema = SchemaType::group_type_builder("schema")
            .with_fields(vec![Arc::new(list_type)])
            .build()
            .unwrap();

        let (int96_val, expected_micros) = make_int96_test_value();

        let file = File::create(&file_path).unwrap();
        let mut writer =
            SerializedFileWriter::new(file, Arc::new(parquet_schema), Default::default()).unwrap();

        // Write a single row with a list containing one INT96 element.
        // def=3: list present (1) + repeated group (2) + element present (3)
        // rep=0: start of a new list
        let mut row_group = writer.next_row_group().unwrap();
        {
            let mut col = row_group.next_column().unwrap().unwrap();
            col.typed::<Int96Type>()
                .write_batch(&[int96_val], Some(&[3]), Some(&[0]))
                .unwrap();
            col.close().unwrap();
        }
        row_group.close().unwrap();
        writer.close().unwrap();

        let iceberg_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(
                        1,
                        "timestamps",
                        Type::List(crate::spec::ListType {
                            element_field: NestedField::optional(
                                2,
                                "element",
                                Type::Primitive(PrimitiveType::Timestamp),
                            )
                            .into(),
                        }),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        let batches = read_int96_batches(&file_path, iceberg_schema, vec![1]).await;

        assert_eq!(batches.len(), 1);
        let list_array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("Expected ListArray");
        let ts_array = list_array
            .values()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("Expected TimestampMicrosecondArray inside list");

        assert_eq!(
            ts_array.value(0),
            expected_micros,
            "INT96 in list: got {}, expected {expected_micros}",
            ts_array.value(0)
        );
    }

    #[tokio::test]
    async fn test_read_int96_timestamps_in_map() {
        use arrow_array::{MapArray, TimestampMicrosecondArray};
        use parquet::basic::{Repetition, Type as PhysicalType};
        use parquet::data_type::{ByteArrayType, Int96Type};
        use parquet::file::writer::SerializedFileWriter;
        use parquet::schema::types::Type as SchemaType;

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_path = format!("{table_location}/map_int96.parquet");

        // MAP encoding:
        //   optional group ts_map (MAP) {
        //     repeated group key_value {
        //       required binary key (UTF8);
        //       optional int96 value;
        //     }
        //   }
        let key_type = SchemaType::primitive_type_builder("key", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .with_logical_type(Some(parquet::basic::LogicalType::String))
            .with_id(Some(2))
            .build()
            .unwrap();

        let value_type = SchemaType::primitive_type_builder("value", PhysicalType::INT96)
            .with_repetition(Repetition::OPTIONAL)
            .with_id(Some(3))
            .build()
            .unwrap();

        let key_value_group = SchemaType::group_type_builder("key_value")
            .with_repetition(Repetition::REPEATED)
            .with_fields(vec![Arc::new(key_type), Arc::new(value_type)])
            .build()
            .unwrap();

        let map_type = SchemaType::group_type_builder("ts_map")
            .with_repetition(Repetition::OPTIONAL)
            .with_id(Some(1))
            .with_logical_type(Some(parquet::basic::LogicalType::Map))
            .with_fields(vec![Arc::new(key_value_group)])
            .build()
            .unwrap();

        let parquet_schema = SchemaType::group_type_builder("schema")
            .with_fields(vec![Arc::new(map_type)])
            .build()
            .unwrap();

        let (int96_val, expected_micros) = make_int96_test_value();

        let file = File::create(&file_path).unwrap();
        let mut writer =
            SerializedFileWriter::new(file, Arc::new(parquet_schema), Default::default()).unwrap();

        // Write a single row with a map containing one key-value pair.
        // rep=0 for both columns: start of a new map.
        // key def=2: map present (1) + key_value entry present (2), key is REQUIRED.
        // value def=3: map present (1) + key_value entry present (2) + value present (3).
        let mut row_group = writer.next_row_group().unwrap();
        {
            let mut col = row_group.next_column().unwrap().unwrap();
            col.typed::<ByteArrayType>()
                .write_batch(
                    &[parquet::data_type::ByteArray::from("event_time")],
                    Some(&[2]),
                    Some(&[0]),
                )
                .unwrap();
            col.close().unwrap();
        }
        {
            let mut col = row_group.next_column().unwrap().unwrap();
            col.typed::<Int96Type>()
                .write_batch(&[int96_val], Some(&[3]), Some(&[0]))
                .unwrap();
            col.close().unwrap();
        }
        row_group.close().unwrap();
        writer.close().unwrap();

        let iceberg_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(
                        1,
                        "ts_map",
                        Type::Map(crate::spec::MapType {
                            key_field: NestedField::required(
                                2,
                                "key",
                                Type::Primitive(PrimitiveType::String),
                            )
                            .into(),
                            value_field: NestedField::optional(
                                3,
                                "value",
                                Type::Primitive(PrimitiveType::Timestamp),
                            )
                            .into(),
                        }),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        let batches = read_int96_batches(&file_path, iceberg_schema, vec![1]).await;

        assert_eq!(batches.len(), 1);
        let map_array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("Expected MapArray");
        let ts_array = map_array
            .values()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("Expected TimestampMicrosecondArray as map values");

        assert_eq!(
            ts_array.value(0),
            expected_micros,
            "INT96 in map: got {}, expected {expected_micros}",
            ts_array.value(0)
        );
    }

    /// Writes `id` (Int32) plus a wide string column (field id 2) whose bytes dominate
    /// the file, so that reading it is visible in `bytes_read`.
    ///
    /// `extra_fields`/`extra_columns` (e.g. a physical metadata leaf) are appended after
    /// the `id` and wide columns, mirroring `write_plain_parquet`'s shape.
    fn write_parquet_with_wide_column(
        dir: &str,
        name: &str,
        extra_fields: Vec<Field>,
        extra_columns: Vec<ArrayRef>,
    ) -> String {
        let wide_field =
            Field::new("wide", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )]));
        // Varied bytes so the column chunk does not compress away under SNAPPY, keeping
        // the `bytes_read` difference between projecting it and not unambiguous.
        let wide_values: Vec<String> = (0..3)
            .map(|i| {
                (0..2048)
                    .map(|j| ((i * 2048 + j) % 251) as u8 as char)
                    .collect()
            })
            .collect();

        let mut fields = vec![wide_field];
        fields.extend(extra_fields);
        let mut columns: Vec<ArrayRef> = vec![Arc::new(StringArray::from(wide_values))];
        columns.extend(extra_columns);
        write_plain_parquet(dir, name, fields, columns)
    }

    /// Schema with `id` (field 1, Int) and `wide` (field 2, String), matching
    /// `write_parquet_with_wide_column`.
    fn id_and_wide_schema() -> SchemaRef {
        Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "wide", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap(),
        )
    }

    /// Builds a scan task over `file_path` projecting `project_field_ids`.
    fn metadata_projection_task(
        file_path: String,
        schema: SchemaRef,
        project_field_ids: Vec<i32>,
    ) -> FileScanTask {
        FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(&file_path).unwrap().len())
            .with_start(0)
            .with_length(0)
            .with_data_file_path(file_path)
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema)
            .with_project_field_ids(project_field_ids)
            .with_case_sensitive(false)
            .build()
    }

    /// Runs a single-task scan and returns the batches plus the bytes read from storage.
    async fn scan_task(task: FileScanTask) -> (Vec<RecordBatch>, u64) {
        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current()).build();
        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let scan = reader.read(tasks).unwrap();
        let metrics = scan.metrics().clone();
        let batches = scan.stream().try_collect().await.unwrap();
        (batches, metrics.bytes_read())
    }

    #[tokio::test]
    async fn test_pos_only_projection_reads_no_data_columns() {
        let tmp_dir = TempDir::new().unwrap();
        let dir = tmp_dir.path().to_str().unwrap();

        let pos_only = metadata_projection_task(
            write_parquet_with_wide_column(dir, "pos_only.parquet", vec![], vec![]),
            id_and_wide_schema(),
            vec![RESERVED_FIELD_ID_POS],
        );
        let (batches, pos_only_bytes) = scan_task(pos_only).await;

        // Only `_pos` is materialized -- no data columns.
        assert_eq!(batches[0].num_columns(), 1);
        let pos_col = batches[0]
            .column_by_name(RESERVED_COL_NAME_POS)
            .expect("_pos column should be present")
            .as_primitive::<arrow_array::types::Int64Type>();
        assert_eq!(pos_col.values(), &[0, 1, 2]);

        // A scan of the same-shaped file that also projects the wide data column must read
        // materially more, proving the wide column chunk was not fetched above.
        let with_data = metadata_projection_task(
            write_parquet_with_wide_column(dir, "pos_only_ref.parquet", vec![], vec![]),
            id_and_wide_schema(),
            vec![2, RESERVED_FIELD_ID_POS],
        );
        let (_, with_data_bytes) = scan_task(with_data).await;

        assert!(
            pos_only_bytes < with_data_bytes,
            "_pos-only scan should read fewer bytes than a scan of the wide column: \
             {pos_only_bytes} vs {with_data_bytes}"
        );
    }

    #[tokio::test]
    async fn test_pos_only_projection_keeps_absolute_pos_under_predicate() {
        use crate::expr::{Bind, Reference};
        use crate::spec::Datum;

        let tmp_dir = TempDir::new().unwrap();
        let dir = tmp_dir.path().to_str().unwrap();
        // id = [1, 2, 3]; drop the middle physical row via a predicate + row selection.
        let file_path = write_plain_parquet(dir, "pos_only_predicate.parquet", vec![], vec![]);

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );
        let bound = Reference::new("id")
            .not_equal_to(Datum::int(2))
            .bind(Arc::clone(&schema), false)
            .unwrap();
        let task = FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(&file_path).unwrap().len())
            .with_start(0)
            .with_length(0)
            .with_data_file_path(file_path)
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema)
            .with_project_field_ids(vec![RESERVED_FIELD_ID_POS])
            .with_predicate(Some(bound))
            .with_case_sensitive(false)
            .build();

        // Row selection must be enabled for the predicate to filter rows. The row filter
        // reads `id` for its own evaluation even though `id` is not projected; the surviving
        // rows must keep their ABSOLUTE positions (0 and 2), not renumbered (0 and 1).
        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current())
            .with_row_selection_enabled(true)
            .build();
        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let batches: Vec<RecordBatch> = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect()
            .await
            .unwrap();

        let pos: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column_by_name(RESERVED_COL_NAME_POS)
                    .expect("_pos column should be present")
                    .as_primitive::<arrow_array::types::Int64Type>()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(pos, vec![0, 2]);
    }

    #[tokio::test]
    async fn test_pos_and_file_projection() {
        use crate::metadata_columns::RESERVED_COL_NAME_FILE;

        let tmp_dir = TempDir::new().unwrap();
        let dir = tmp_dir.path().to_str().unwrap();
        // The motivating row-lineage shape: a synthesized position column (mask -> none)
        // alongside a materialized per-file constant.
        let file_path = write_parquet_with_wide_column(dir, "pos_and_file.parquet", vec![], vec![]);
        let task = metadata_projection_task(file_path.clone(), id_and_wide_schema(), vec![
            RESERVED_FIELD_ID_POS,
            RESERVED_FIELD_ID_FILE,
        ]);
        let (batches, _) = scan_task(task).await;

        // Both metadata columns materialize; no data column is read.
        assert_eq!(batches[0].num_columns(), 2);
        let pos_col = batches[0]
            .column_by_name(RESERVED_COL_NAME_POS)
            .expect("_pos column should be present")
            .as_primitive::<arrow_array::types::Int64Type>();
        assert_eq!(pos_col.values(), &[0, 1, 2]);
        let file_col = batches[0]
            .column_by_name(RESERVED_COL_NAME_FILE)
            .expect("_file column should be present");
        let file_col = cast(file_col, &DataType::Utf8).unwrap();
        let file_col = file_col.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(file_col.value(0), file_path);
    }

    #[tokio::test]
    async fn test_pos_and_physical_seq_projection_reads_only_the_leaf() {
        use crate::metadata_columns::{
            RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER,
            RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
        };

        // A v3 rewrite that carried rows forward stores `_last_updated_sequence_number`
        // per-row. Projecting only `_pos` + the sequence column must read just that one
        // physical leaf, not every data column.
        let tmp_dir = TempDir::new().unwrap();
        let dir = tmp_dir.path().to_str().unwrap();

        // File: id (1), wide data column (2), physical _last_updated_sequence_number.
        let write = |name: &str| {
            write_parquet_with_wide_column(
                dir,
                name,
                vec![physical_last_updated_seq_field()],
                vec![Arc::new(Int64Array::from(vec![Some(5), None, Some(8)])) as ArrayRef],
            )
        };

        let seq_task = |path: String, ids: Vec<i32>| {
            FileScanTask::builder()
                .with_file_size_in_bytes(std::fs::metadata(&path).unwrap().len())
                .with_start(0)
                .with_length(0)
                .with_data_file_path(path)
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(id_and_wide_schema())
                .with_project_field_ids(ids)
                .with_first_row_id(Some(100))
                .with_data_sequence_number(Some(9))
                .with_case_sensitive(false)
                .build()
        };

        let meta_only = seq_task(write("pos_seq.parquet"), vec![
            RESERVED_FIELD_ID_POS,
            RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
        ]);
        let (batches, meta_only_bytes) = scan_task(meta_only).await;

        // `_pos` and the coalesced sequence column materialize; the wide column does not.
        let pos_col = batches[0]
            .column_by_name(RESERVED_COL_NAME_POS)
            .expect("_pos column should be present")
            .as_primitive::<arrow_array::types::Int64Type>();
        assert_eq!(pos_col.values(), &[0, 1, 2]);
        let seq_col = batches[0]
            .column_by_name(RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER)
            .expect("_last_updated_sequence_number column should be present");
        let seq_col = cast(seq_col, &DataType::Int64).unwrap();
        let seq_col = seq_col.as_any().downcast_ref::<Int64Array>().unwrap();
        // Per-row stored value where non-null, else the data sequence number (9).
        assert_eq!(seq_col.value(0), 5);
        assert_eq!(seq_col.value(1), 9);
        assert_eq!(seq_col.value(2), 8);
        assert!(batches[0].column_by_name("wide").is_none());

        // A scan that also projects the wide data column must read materially more,
        // proving the metadata-only scan pruned to just the sequence leaf.
        let with_data = seq_task(write("pos_seq_ref.parquet"), vec![
            2,
            RESERVED_FIELD_ID_POS,
            RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
        ]);
        let (_, with_data_bytes) = scan_task(with_data).await;

        assert!(
            meta_only_bytes < with_data_bytes,
            "_pos + physical sequence scan should read fewer bytes than one that also \
             reads the wide column: {meta_only_bytes} vs {with_data_bytes}"
        );
    }

    #[tokio::test]
    async fn test_seq_only_projection_reads_only_the_leaf() {
        use crate::metadata_columns::{
            RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER,
            RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
        };

        // `_last_updated_sequence_number` alone (no `_pos`, no data column). The physical
        // leaf is the sole row source, so it -- not the RowNumber virtual column -- must
        // drive the `none()` downgrade and prune the read to just that leaf.
        let tmp_dir = TempDir::new().unwrap();
        let dir = tmp_dir.path().to_str().unwrap();
        let write = |name: &str| {
            write_parquet_with_wide_column(
                dir,
                name,
                vec![physical_last_updated_seq_field()],
                vec![Arc::new(Int64Array::from(vec![Some(5), None, Some(8)])) as ArrayRef],
            )
        };
        let seq_task = |path: String, ids: Vec<i32>| {
            FileScanTask::builder()
                .with_file_size_in_bytes(std::fs::metadata(&path).unwrap().len())
                .with_start(0)
                .with_length(0)
                .with_data_file_path(path)
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(id_and_wide_schema())
                .with_project_field_ids(ids)
                .with_first_row_id(Some(100))
                .with_data_sequence_number(Some(9))
                .with_case_sensitive(false)
                .build()
        };

        let meta_only = seq_task(write("seq_only.parquet"), vec![
            RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
        ]);
        let (batches, meta_only_bytes) = scan_task(meta_only).await;

        let seq_col = batches[0]
            .column_by_name(RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER)
            .expect("_last_updated_sequence_number column should be present");
        let seq_col = cast(seq_col, &DataType::Int64).unwrap();
        let seq_col = seq_col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(seq_col.value(0), 5);
        assert_eq!(seq_col.value(1), 9);
        assert_eq!(seq_col.value(2), 8);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
        assert!(batches[0].column_by_name("wide").is_none());

        let with_data = seq_task(write("seq_only_ref.parquet"), vec![
            2,
            RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
        ]);
        let (_, with_data_bytes) = scan_task(with_data).await;

        assert!(
            meta_only_bytes < with_data_bytes,
            "seq-only scan should read fewer bytes than one that also reads the wide \
             column: {meta_only_bytes} vs {with_data_bytes}"
        );
    }

    #[tokio::test]
    async fn test_seq_only_projection_null_first_row_id_preserves_row_count() {
        use crate::metadata_columns::{
            RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER,
            RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
        };

        // Seq-only projection with a null first_row_id: the column is nulled and the
        // physical leaf is NOT read (the gated `coalesce_last_updated_seq_leaf` is None).
        // The downgrade must therefore not fire -- keying off the raw `project_*` flag
        // instead would drop the only readable column and lose the row count.
        let tmp_dir = TempDir::new().unwrap();
        let dir = tmp_dir.path().to_str().unwrap();
        let file_path = write_parquet_with_wide_column(
            dir,
            "seq_only_null_first.parquet",
            vec![physical_last_updated_seq_field()],
            vec![Arc::new(Int64Array::from(vec![Some(5), Some(6), Some(7)])) as ArrayRef],
        );
        let task = FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(&file_path).unwrap().len())
            .with_start(0)
            .with_length(0)
            .with_data_file_path(file_path)
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(id_and_wide_schema())
            .with_project_field_ids(vec![RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER])
            .with_first_row_id(None)
            .with_data_sequence_number(Some(9))
            .with_case_sensitive(false)
            .build();
        let (batches, _) = scan_task(task).await;

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
        let seq_col = batches[0]
            .column_by_name(RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER)
            .expect("_last_updated_sequence_number column should be present");
        let seq_col = cast(seq_col, &DataType::Int64).unwrap();
        let seq_col = seq_col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert!((0..3).all(|i| seq_col.is_null(i)));
    }

    #[tokio::test]
    async fn test_file_only_projection_preserves_row_count() {
        use crate::metadata_columns::RESERVED_COL_NAME_FILE;

        let tmp_dir = TempDir::new().unwrap();
        let dir = tmp_dir.path().to_str().unwrap();
        let file_path = write_plain_parquet(dir, "file_only.parquet", vec![], vec![]);
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );
        let task =
            metadata_projection_task(file_path.clone(), schema, vec![RESERVED_FIELD_ID_FILE]);
        let (batches, _) = scan_task(task).await;

        // A pure-constant projection has no independent row source, so the row count must
        // still come from the file (the `empty -> all()` path is preserved for this case).
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
        let file_col = batches[0]
            .column_by_name(RESERVED_COL_NAME_FILE)
            .expect("_file column should be present");
        let file_col = cast(file_col, &DataType::Utf8).unwrap();
        let file_col = file_col.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(file_col.value(0), file_path);
    }

    #[tokio::test]
    async fn test_empty_projection_preserves_row_count() {
        let tmp_dir = TempDir::new().unwrap();
        let dir = tmp_dir.path().to_str().unwrap();
        let file_path = write_plain_parquet(dir, "empty_projection.parquet", vec![], vec![]);
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );
        let task = metadata_projection_task(file_path, schema, vec![]);
        let (batches, _) = scan_task(task).await;

        // A bare COUNT(*)-style empty projection must still report the row count.
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }
}
