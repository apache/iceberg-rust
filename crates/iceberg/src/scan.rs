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

//! Table scan api.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use arrow_array::RecordBatch;
use futures::channel::mpsc::{channel, Sender};
use futures::stream::BoxStream;
use futures::{SinkExt, StreamExt, TryFutureExt, TryStreamExt};
use serde::{Deserialize, Serialize};

use crate::arrow::ArrowReaderBuilder;
use crate::delete_file_index::DeleteFileIndex;
use crate::expr::visitors::expression_evaluator::ExpressionEvaluator;
use crate::expr::visitors::inclusive_metrics_evaluator::InclusiveMetricsEvaluator;
use crate::expr::visitors::inclusive_projection::InclusiveProjection;
use crate::expr::visitors::manifest_evaluator::ManifestEvaluator;
use crate::expr::{Bind, BoundPredicate, Predicate};
use crate::io::object_cache::ObjectCache;
use crate::io::FileIO;
use crate::runtime::spawn;
use crate::spec::{
    DataContentType, DataFileFormat, ManifestContentType, ManifestEntryRef, ManifestFile,
    ManifestList, Schema, SchemaRef, SnapshotRef, TableMetadataRef,
};
use crate::table::Table;
use crate::utils::available_parallelism;
use crate::{Error, ErrorKind, Result};

/// A stream of [`FileScanTask`].
pub type FileScanTaskStream = BoxStream<'static, Result<FileScanTask>>;
/// A stream of arrow [`RecordBatch`]es.
pub type ArrowRecordBatchStream = BoxStream<'static, Result<RecordBatch>>;

/// Builder to create table scan.
pub struct TableScanBuilder<'a> {
    table: &'a Table,
    // Defaults to none which means select all columns
    column_names: Option<Vec<String>>,
    snapshot_id: Option<i64>,
    batch_size: Option<usize>,
    case_sensitive: bool,
    filter: Option<Predicate>,
    concurrency_limit_data_files: usize,
    concurrency_limit_manifest_entries: usize,
    concurrency_limit_manifest_files: usize,
    row_group_filtering_enabled: bool,
    row_selection_enabled: bool,

    // TODO: defaults to false for now whilst delete file processing
    // is still being worked on but will switch to a default of true
    // once this work is complete
    delete_file_processing_enabled: bool,
}

impl<'a> TableScanBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        let num_cpus = available_parallelism().get();

        Self {
            table,
            column_names: None,
            snapshot_id: None,
            batch_size: None,
            case_sensitive: true,
            filter: None,
            concurrency_limit_data_files: num_cpus,
            concurrency_limit_manifest_entries: num_cpus,
            concurrency_limit_manifest_files: num_cpus,
            row_group_filtering_enabled: true,
            row_selection_enabled: false,
            delete_file_processing_enabled: false,
        }
    }

    /// Sets the desired size of batches in the response
    /// to something other than the default
    pub fn with_batch_size(mut self, batch_size: Option<usize>) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Sets the scan's case sensitivity
    pub fn with_case_sensitive(mut self, case_sensitive: bool) -> Self {
        self.case_sensitive = case_sensitive;
        self
    }

    /// Specifies a predicate to use as a filter
    pub fn with_filter(mut self, predicate: Predicate) -> Self {
        // calls rewrite_not to remove Not nodes, which must be absent
        // when applying the manifest evaluator
        self.filter = Some(predicate.rewrite_not());
        self
    }

    /// Select all columns.
    pub fn select_all(mut self) -> Self {
        self.column_names = None;
        self
    }

    /// Select empty columns.
    pub fn select_empty(mut self) -> Self {
        self.column_names = Some(vec![]);
        self
    }

    /// Select some columns of the table.
    pub fn select(mut self, column_names: impl IntoIterator<Item = impl ToString>) -> Self {
        self.column_names = Some(
            column_names
                .into_iter()
                .map(|item| item.to_string())
                .collect(),
        );
        self
    }

    /// Set the snapshot to scan. When not set, it uses current snapshot.
    pub fn snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }

    /// Sets the concurrency limit for both manifest files and manifest
    /// entries for this scan
    pub fn with_concurrency_limit(mut self, limit: usize) -> Self {
        self.concurrency_limit_manifest_files = limit;
        self.concurrency_limit_manifest_entries = limit;
        self.concurrency_limit_data_files = limit;
        self
    }

    /// Sets the data file concurrency limit for this scan
    pub fn with_data_file_concurrency_limit(mut self, limit: usize) -> Self {
        self.concurrency_limit_data_files = limit;
        self
    }

    /// Sets the manifest entry concurrency limit for this scan
    pub fn with_manifest_entry_concurrency_limit(mut self, limit: usize) -> Self {
        self.concurrency_limit_manifest_entries = limit;
        self
    }

    /// Determines whether to enable row group filtering.
    /// When enabled, if a read is performed with a filter predicate,
    /// then the metadata for each row group in the parquet file is
    /// evaluated against the filter predicate and row groups
    /// that cant contain matching rows will be skipped entirely.
    ///
    /// Defaults to enabled, as it generally improves performance or
    /// keeps it the same, with performance degradation unlikely.
    pub fn with_row_group_filtering_enabled(mut self, row_group_filtering_enabled: bool) -> Self {
        self.row_group_filtering_enabled = row_group_filtering_enabled;
        self
    }

    /// Determines whether to enable row selection.
    /// When enabled, if a read is performed with a filter predicate,
    /// then (for row groups that have not been skipped) the page index
    /// for each row group in a parquet file is parsed and evaluated
    /// against the filter predicate to determine if ranges of rows
    /// within a row group can be skipped, based upon the page-level
    /// statistics for each column.
    ///
    /// Defaults to being disabled. Enabling requires parsing the parquet page
    /// index, which can be slow enough that parsing the page index outweighs any
    /// gains from the reduced number of rows that need scanning.
    /// It is recommended to experiment with partitioning, sorting, row group size,
    /// page size, and page row limit Iceberg settings on the table being scanned in
    /// order to get the best performance from using row selection.
    pub fn with_row_selection_enabled(mut self, row_selection_enabled: bool) -> Self {
        self.row_selection_enabled = row_selection_enabled;
        self
    }

    /// Determines whether to enable delete file processing (currently disabled by default)
    ///
    /// When disabled, delete files are ignored.
    pub fn with_delete_file_processing_enabled(
        mut self,
        delete_file_processing_enabled: bool,
    ) -> Self {
        self.delete_file_processing_enabled = delete_file_processing_enabled;
        self
    }

    /// Build the table scan.
    pub fn build(self) -> Result<TableScan> {
        let snapshot = match self.snapshot_id {
            Some(snapshot_id) => self
                .table
                .metadata()
                .snapshot_by_id(snapshot_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Snapshot with id {} not found", snapshot_id),
                    )
                })?
                .clone(),
            None => self
                .table
                .metadata()
                .current_snapshot()
                .ok_or_else(|| {
                    Error::new(ErrorKind::Unexpected, "Can't scan table without snapshots")
                })?
                .clone(),
        };

        let schema = snapshot.schema(self.table.metadata())?;

        // Check that all column names exist in the schema.
        if let Some(column_names) = self.column_names.as_ref() {
            for column_name in column_names {
                if schema.field_by_name(column_name).is_none() {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Column {} not found in table. Schema: {}",
                            column_name, schema
                        ),
                    ));
                }
            }
        }

        let mut field_ids = vec![];
        let column_names = self.column_names.clone().unwrap_or_else(|| {
            schema
                .as_struct()
                .fields()
                .iter()
                .map(|f| f.name.clone())
                .collect()
        });

        for column_name in column_names.iter() {
            let field_id = schema.field_id_by_name(column_name).ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Column {} not found in table. Schema: {}",
                        column_name, schema
                    ),
                )
            })?;

            schema
                .as_struct()
                .field_by_id(field_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::FeatureUnsupported,
                        format!(
                            "Column {} is not a direct child of schema but a nested field, which is not supported now. Schema: {}",
                            column_name, schema
                        ),
                    )
                })?;

            field_ids.push(field_id);
        }

        let snapshot_bound_predicate = if let Some(ref predicates) = self.filter {
            Some(predicates.bind(schema.clone(), true)?)
        } else {
            None
        };

        let plan_context = PlanContext {
            snapshot,
            table_metadata: self.table.metadata_ref(),
            snapshot_schema: schema,
            case_sensitive: self.case_sensitive,
            predicate: self.filter.map(Arc::new),
            snapshot_bound_predicate: snapshot_bound_predicate.map(Arc::new),
            object_cache: self.table.object_cache(),
            field_ids: Arc::new(field_ids),
            partition_filter_cache: Arc::new(PartitionFilterCache::new()),
            manifest_evaluator_cache: Arc::new(ManifestEvaluatorCache::new()),
            expression_evaluator_cache: Arc::new(ExpressionEvaluatorCache::new()),
        };

        Ok(TableScan {
            batch_size: self.batch_size,
            column_names: self.column_names,
            file_io: self.table.file_io().clone(),
            plan_context,
            concurrency_limit_data_files: self.concurrency_limit_data_files,
            concurrency_limit_manifest_entries: self.concurrency_limit_manifest_entries,
            concurrency_limit_manifest_files: self.concurrency_limit_manifest_files,
            row_group_filtering_enabled: self.row_group_filtering_enabled,
            row_selection_enabled: self.row_selection_enabled,
            delete_file_processing_enabled: self.delete_file_processing_enabled,
        })
    }
}

/// Table scan.
#[derive(Debug)]
pub struct TableScan {
    plan_context: PlanContext,
    batch_size: Option<usize>,
    file_io: FileIO,
    column_names: Option<Vec<String>>,
    /// The maximum number of manifest files that will be
    /// retrieved from [`FileIO`] concurrently
    concurrency_limit_manifest_files: usize,

    /// The maximum number of [`ManifestEntry`]s that will
    /// be processed in parallel
    concurrency_limit_manifest_entries: usize,

    /// The maximum number of [`ManifestEntry`]s that will
    /// be processed in parallel
    concurrency_limit_data_files: usize,

    row_group_filtering_enabled: bool,
    row_selection_enabled: bool,
    delete_file_processing_enabled: bool,
}

/// PlanContext wraps a [`SnapshotRef`] alongside all the other
/// objects that are required to perform a scan file plan.
#[derive(Debug)]
struct PlanContext {
    snapshot: SnapshotRef,

    table_metadata: TableMetadataRef,
    snapshot_schema: SchemaRef,
    case_sensitive: bool,
    predicate: Option<Arc<Predicate>>,
    snapshot_bound_predicate: Option<Arc<BoundPredicate>>,
    object_cache: Arc<ObjectCache>,
    field_ids: Arc<Vec<i32>>,

    partition_filter_cache: Arc<PartitionFilterCache>,
    manifest_evaluator_cache: Arc<ManifestEvaluatorCache>,
    expression_evaluator_cache: Arc<ExpressionEvaluatorCache>,
}

impl TableScan {
    /// Returns a stream of [`FileScanTask`]s.
    pub async fn plan_files(&self) -> Result<FileScanTaskStream> {
        let concurrency_limit_manifest_files = self.concurrency_limit_manifest_files;
        let concurrency_limit_manifest_entries = self.concurrency_limit_manifest_entries;

        // used to stream ManifestEntryContexts between stages of the file plan operation
        let (manifest_entry_data_ctx_tx, manifest_entry_data_ctx_rx) =
            channel(concurrency_limit_manifest_files);
        let (manifest_entry_delete_ctx_tx, manifest_entry_delete_ctx_rx) =
            channel(concurrency_limit_manifest_files);

        // used to stream the results back to the caller
        let (file_scan_task_tx, file_scan_task_rx) = channel(concurrency_limit_manifest_entries);

        let delete_file_idx_and_tx: Option<(DeleteFileIndex, Sender<DeleteFileContext>)> =
            if self.delete_file_processing_enabled {
                Some(DeleteFileIndex::new())
            } else {
                None
            };

        let manifest_list = self.plan_context.get_manifest_list().await?;

        // get the [`ManifestFile`]s from the [`ManifestList`], filtering out any
        // whose partitions cannot match this
        // scan's filter
        let manifest_file_contexts = self.plan_context.build_manifest_file_contexts(
            manifest_list,
            manifest_entry_data_ctx_tx,
            delete_file_idx_and_tx.as_ref().map(|(delete_file_idx, _)| {
                (delete_file_idx.clone(), manifest_entry_delete_ctx_tx)
            }),
        )?;

        let mut channel_for_manifest_error = file_scan_task_tx.clone();

        // Concurrently load all [`Manifest`]s and stream their [`ManifestEntry`]s
        spawn(async move {
            let result = futures::stream::iter(manifest_file_contexts)
                .try_for_each_concurrent(concurrency_limit_manifest_files, |ctx| async move {
                    ctx.fetch_manifest_and_stream_manifest_entries().await
                })
                .await;

            if let Err(error) = result {
                let _ = channel_for_manifest_error.send(Err(error)).await;
            }
        });

        let mut channel_for_data_manifest_entry_error = file_scan_task_tx.clone();

        if let Some((_, delete_file_tx)) = delete_file_idx_and_tx {
            let mut channel_for_delete_manifest_entry_error = file_scan_task_tx.clone();

            // Process the delete file [`ManifestEntry`] stream in parallel
            spawn(async move {
                let result = manifest_entry_delete_ctx_rx
                    .map(|me_ctx| Ok((me_ctx, delete_file_tx.clone())))
                    .try_for_each_concurrent(
                        concurrency_limit_manifest_entries,
                        |(manifest_entry_context, tx)| async move {
                            spawn(async move {
                                Self::process_delete_manifest_entry(manifest_entry_context, tx)
                                    .await
                            })
                            .await
                        },
                    )
                    .await;

                if let Err(error) = result {
                    let _ = channel_for_delete_manifest_entry_error
                        .send(Err(error))
                        .await;
                }
            })
            .await;
        }

        // Process the data file [`ManifestEntry`] stream in parallel
        spawn(async move {
            let result = manifest_entry_data_ctx_rx
                .map(|me_ctx| Ok((me_ctx, file_scan_task_tx.clone())))
                .try_for_each_concurrent(
                    concurrency_limit_manifest_entries,
                    |(manifest_entry_context, tx)| async move {
                        spawn(async move {
                            Self::process_data_manifest_entry(manifest_entry_context, tx).await
                        })
                        .await
                    },
                )
                .await;

            if let Err(error) = result {
                let _ = channel_for_data_manifest_entry_error.send(Err(error)).await;
            }
        });

        Ok(file_scan_task_rx.boxed())
    }

    /// Returns an [`ArrowRecordBatchStream`].
    pub async fn to_arrow(&self) -> Result<ArrowRecordBatchStream> {
        let mut arrow_reader_builder = ArrowReaderBuilder::new(self.file_io.clone())
            .with_data_file_concurrency_limit(self.concurrency_limit_data_files)
            .with_row_group_filtering_enabled(self.row_group_filtering_enabled)
            .with_row_selection_enabled(self.row_selection_enabled);

        if let Some(batch_size) = self.batch_size {
            arrow_reader_builder = arrow_reader_builder.with_batch_size(batch_size);
        }

        arrow_reader_builder
            .build()
            .read(self.plan_files().await?)
            .await
    }

    /// Returns a reference to the column names of the table scan.
    pub fn column_names(&self) -> Option<&[String]> {
        self.column_names.as_deref()
    }

    /// Returns a reference to the snapshot of the table scan.
    pub fn snapshot(&self) -> &SnapshotRef {
        &self.plan_context.snapshot
    }

    async fn process_data_manifest_entry(
        manifest_entry_context: ManifestEntryContext,
        mut file_scan_task_tx: Sender<Result<FileScanTask>>,
    ) -> Result<()> {
        // skip processing this manifest entry if it has been marked as deleted
        if !manifest_entry_context.manifest_entry.is_alive() {
            return Ok(());
        }

        // abort the plan if we encounter a manifest entry for a delete file
        if manifest_entry_context.manifest_entry.content_type() != DataContentType::Data {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Encountered an entry for a delete file in a data file manifest",
            ));
        }

        if let Some(ref bound_predicates) = manifest_entry_context.bound_predicates {
            let BoundPredicates {
                snapshot_bound_predicate,
                partition_bound_predicate,
            } = bound_predicates.as_ref();

            let expression_evaluator_cache =
                manifest_entry_context.expression_evaluator_cache.as_ref();

            let expression_evaluator = expression_evaluator_cache.get(
                manifest_entry_context.partition_spec_id,
                partition_bound_predicate,
            )?;

            // skip any data file whose partition data indicates that it can't contain
            // any data that matches this scan's filter
            if !expression_evaluator.eval(manifest_entry_context.manifest_entry.data_file())? {
                return Ok(());
            }

            // skip any data file whose metrics don't match this scan's filter
            if !InclusiveMetricsEvaluator::eval(
                snapshot_bound_predicate,
                manifest_entry_context.manifest_entry.data_file(),
                false,
            )? {
                return Ok(());
            }
        }

        // congratulations! the manifest entry has made its way through the
        // entire plan without getting filtered out. Create a corresponding
        // FileScanTask and push it to the result stream
        file_scan_task_tx
            .send(Ok(manifest_entry_context.into_file_scan_task().await?))
            .await?;

        Ok(())
    }

    async fn process_delete_manifest_entry(
        manifest_entry_context: ManifestEntryContext,
        mut delete_file_ctx_tx: Sender<DeleteFileContext>,
    ) -> Result<()> {
        // skip processing this manifest entry if it has been marked as deleted
        if !manifest_entry_context.manifest_entry.is_alive() {
            return Ok(());
        }

        // abort the plan if we encounter a manifest entry that is not for a delete file
        if manifest_entry_context.manifest_entry.content_type() == DataContentType::Data {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Encountered an entry for a data file in a delete manifest",
            ));
        }

        if let Some(ref bound_predicates) = manifest_entry_context.bound_predicates {
            let expression_evaluator_cache =
                manifest_entry_context.expression_evaluator_cache.as_ref();

            let expression_evaluator = expression_evaluator_cache.get(
                manifest_entry_context.partition_spec_id,
                &bound_predicates.partition_bound_predicate,
            )?;

            // skip any data file whose partition data indicates that it can't contain
            // any data that matches this scan's filter
            if !expression_evaluator.eval(manifest_entry_context.manifest_entry.data_file())? {
                return Ok(());
            }
        }

        delete_file_ctx_tx
            .send(DeleteFileContext {
                manifest_entry: manifest_entry_context.manifest_entry.clone(),
                partition_spec_id: manifest_entry_context.partition_spec_id,
            })
            .await?;

        Ok(())
    }
}

struct BoundPredicates {
    partition_bound_predicate: BoundPredicate,
    snapshot_bound_predicate: BoundPredicate,
}

/// Wraps a [`ManifestFile`] alongside the objects that are needed
/// to process it in a thread-safe manner
struct ManifestFileContext {
    manifest_file: ManifestFile,

    sender: Sender<ManifestEntryContext>,

    field_ids: Arc<Vec<i32>>,
    bound_predicates: Option<Arc<BoundPredicates>>,
    object_cache: Arc<ObjectCache>,
    snapshot_schema: SchemaRef,
    expression_evaluator_cache: Arc<ExpressionEvaluatorCache>,
    delete_file_index: Option<DeleteFileIndex>,
}

/// Wraps a [`ManifestEntryRef`] alongside the objects that are needed
/// to process it in a thread-safe manner
struct ManifestEntryContext {
    manifest_entry: ManifestEntryRef,

    expression_evaluator_cache: Arc<ExpressionEvaluatorCache>,
    field_ids: Arc<Vec<i32>>,
    bound_predicates: Option<Arc<BoundPredicates>>,
    partition_spec_id: i32,
    snapshot_schema: SchemaRef,
    delete_file_index: Option<DeleteFileIndex>,
}

impl ManifestFileContext {
    /// Consumes this [`ManifestFileContext`], fetching its Manifest from FileIO and then
    /// streaming its constituent [`ManifestEntries`] to the channel provided in the context
    async fn fetch_manifest_and_stream_manifest_entries(self) -> Result<()> {
        let ManifestFileContext {
            object_cache,
            manifest_file,
            bound_predicates,
            snapshot_schema,
            field_ids,
            mut sender,
            expression_evaluator_cache,
            delete_file_index,
            ..
        } = self;

        let manifest = object_cache.get_manifest(&manifest_file).await?;

        for manifest_entry in manifest.entries() {
            let manifest_entry_context = ManifestEntryContext {
                // TODO: refactor to avoid the expensive ManifestEntry clone
                manifest_entry: manifest_entry.clone(),
                expression_evaluator_cache: expression_evaluator_cache.clone(),
                field_ids: field_ids.clone(),
                partition_spec_id: manifest_file.partition_spec_id,
                bound_predicates: bound_predicates.clone(),
                snapshot_schema: snapshot_schema.clone(),
                delete_file_index: delete_file_index.clone(),
            };

            sender
                .send(manifest_entry_context)
                .map_err(|_| Error::new(ErrorKind::Unexpected, "mpsc channel SendError"))
                .await?;
        }

        Ok(())
    }
}

impl ManifestEntryContext {
    /// consume this `ManifestEntryContext`, returning a `FileScanTask`
    /// created from it
    async fn into_file_scan_task(self) -> Result<FileScanTask> {
        let deletes = if let Some(delete_file_index) = self.delete_file_index {
            delete_file_index
                .get_deletes_for_data_file(
                    self.manifest_entry.data_file(),
                    self.manifest_entry.sequence_number(),
                )
                .await?
        } else {
            vec![]
        };

        Ok(FileScanTask {
            start: 0,
            length: self.manifest_entry.file_size_in_bytes(),
            record_count: Some(self.manifest_entry.record_count()),

            data_file_path: self.manifest_entry.file_path().to_string(),
            data_file_content: self.manifest_entry.content_type(),
            data_file_format: self.manifest_entry.file_format(),

            schema: self.snapshot_schema,
            project_field_ids: self.field_ids.to_vec(),
            predicate: self
                .bound_predicates
                .map(|x| x.as_ref().snapshot_bound_predicate.clone()),

            deletes,
        })
    }
}

impl PlanContext {
    async fn get_manifest_list(&self) -> Result<Arc<ManifestList>> {
        self.object_cache
            .as_ref()
            .get_manifest_list(&self.snapshot, &self.table_metadata)
            .await
    }

    fn get_partition_filter(&self, manifest_file: &ManifestFile) -> Result<Arc<BoundPredicate>> {
        let partition_spec_id = manifest_file.partition_spec_id;

        let partition_filter = self.partition_filter_cache.get(
            partition_spec_id,
            &self.table_metadata,
            &self.snapshot_schema,
            self.case_sensitive,
            self.predicate
                .as_ref()
                .ok_or(Error::new(
                    ErrorKind::Unexpected,
                    "Expected a predicate but none present",
                ))?
                .as_ref()
                .bind(self.snapshot_schema.clone(), self.case_sensitive)?,
        )?;

        Ok(partition_filter)
    }

    fn build_manifest_file_contexts(
        &self,
        manifest_list: Arc<ManifestList>,
        tx_data: Sender<ManifestEntryContext>,
        delete_file_idx_and_tx: Option<(DeleteFileIndex, Sender<ManifestEntryContext>)>,
    ) -> Result<Box<impl Iterator<Item = Result<ManifestFileContext>> + 'static>> {
        let manifest_files = manifest_list.entries().iter();

        // TODO: Ideally we could ditch this intermediate Vec as we return an iterator.
        let mut filtered_mfcs = vec![];

        for manifest_file in manifest_files {
            let (delete_file_idx, tx) = if manifest_file.content == ManifestContentType::Deletes {
                let Some((delete_file_idx, tx)) = delete_file_idx_and_tx.as_ref() else {
                    continue;
                };
                (Some(delete_file_idx.clone()), tx.clone())
            } else {
                (
                    delete_file_idx_and_tx.as_ref().map(|x| x.0.clone()),
                    tx_data.clone(),
                )
            };

            let partition_bound_predicate = if self.predicate.is_some() {
                let partition_bound_predicate = self.get_partition_filter(manifest_file)?;

                // evaluate the ManifestFile against the partition filter. Skip
                // if it cannot contain any matching rows
                if !self
                    .manifest_evaluator_cache
                    .get(
                        manifest_file.partition_spec_id,
                        partition_bound_predicate.clone(),
                    )
                    .eval(manifest_file)?
                {
                    continue;
                }

                Some(partition_bound_predicate)
            } else {
                None
            };

            let mfc = self.create_manifest_file_context(
                manifest_file,
                partition_bound_predicate,
                tx,
                delete_file_idx,
            );

            filtered_mfcs.push(Ok(mfc));
        }

        Ok(Box::new(filtered_mfcs.into_iter()))
    }

    fn create_manifest_file_context(
        &self,
        manifest_file: &ManifestFile,
        partition_filter: Option<Arc<BoundPredicate>>,
        sender: Sender<ManifestEntryContext>,
        delete_file_index: Option<DeleteFileIndex>,
    ) -> ManifestFileContext {
        let bound_predicates =
            if let (Some(ref partition_bound_predicate), Some(snapshot_bound_predicate)) =
                (partition_filter, &self.snapshot_bound_predicate)
            {
                Some(Arc::new(BoundPredicates {
                    partition_bound_predicate: partition_bound_predicate.as_ref().clone(),
                    snapshot_bound_predicate: snapshot_bound_predicate.as_ref().clone(),
                }))
            } else {
                None
            };

        ManifestFileContext {
            manifest_file: manifest_file.clone(),
            bound_predicates,
            sender,
            object_cache: self.object_cache.clone(),
            snapshot_schema: self.snapshot_schema.clone(),
            field_ids: self.field_ids.clone(),
            expression_evaluator_cache: self.expression_evaluator_cache.clone(),
            delete_file_index,
        }
    }
}

/// Manages the caching of [`BoundPredicate`] objects
/// for [`PartitionSpec`]s based on partition spec id.
#[derive(Debug)]
struct PartitionFilterCache(RwLock<HashMap<i32, Arc<BoundPredicate>>>);

impl PartitionFilterCache {
    /// Creates a new [`PartitionFilterCache`]
    /// with an empty internal HashMap.
    fn new() -> Self {
        Self(RwLock::new(HashMap::new()))
    }

    /// Retrieves a [`BoundPredicate`] from the cache
    /// or computes it if not present.
    fn get(
        &self,
        spec_id: i32,
        table_metadata: &TableMetadataRef,
        schema: &Schema,
        case_sensitive: bool,
        filter: BoundPredicate,
    ) -> Result<Arc<BoundPredicate>> {
        // we need a block here to ensure that the `read()` gets dropped before we hit the `write()`
        // below, otherwise we hit deadlock
        {
            let read = self.0.read().map_err(|_| {
                Error::new(
                    ErrorKind::Unexpected,
                    "PartitionFilterCache RwLock was poisoned",
                )
            })?;

            if read.contains_key(&spec_id) {
                return Ok(read.get(&spec_id).unwrap().clone());
            }
        }

        let partition_spec = table_metadata
            .partition_spec_by_id(spec_id)
            .ok_or(Error::new(
                ErrorKind::Unexpected,
                format!("Could not find partition spec for id {}", spec_id),
            ))?;

        let partition_type = partition_spec.partition_type(schema)?;
        let partition_fields = partition_type.fields().to_owned();
        let partition_schema = Arc::new(
            Schema::builder()
                .with_schema_id(partition_spec.spec_id())
                .with_fields(partition_fields)
                .build()?,
        );

        let mut inclusive_projection = InclusiveProjection::new(partition_spec.clone());

        let partition_filter = inclusive_projection
            .project(&filter)?
            .rewrite_not()
            .bind(partition_schema.clone(), case_sensitive)?;

        self.0
            .write()
            .map_err(|_| {
                Error::new(
                    ErrorKind::Unexpected,
                    "PartitionFilterCache RwLock was poisoned",
                )
            })?
            .insert(spec_id, Arc::new(partition_filter));

        let read = self.0.read().map_err(|_| {
            Error::new(
                ErrorKind::Unexpected,
                "PartitionFilterCache RwLock was poisoned",
            )
        })?;

        Ok(read.get(&spec_id).unwrap().clone())
    }
}

/// Manages the caching of [`ManifestEvaluator`] objects
/// for [`PartitionSpec`]s based on partition spec id.
#[derive(Debug)]
struct ManifestEvaluatorCache(RwLock<HashMap<i32, Arc<ManifestEvaluator>>>);

impl ManifestEvaluatorCache {
    /// Creates a new [`ManifestEvaluatorCache`]
    /// with an empty internal HashMap.
    fn new() -> Self {
        Self(RwLock::new(HashMap::new()))
    }

    /// Retrieves a [`ManifestEvaluator`] from the cache
    /// or computes it if not present.
    fn get(&self, spec_id: i32, partition_filter: Arc<BoundPredicate>) -> Arc<ManifestEvaluator> {
        // we need a block here to ensure that the `read()` gets dropped before we hit the `write()`
        // below, otherwise we hit deadlock
        {
            let read = self
                .0
                .read()
                .map_err(|_| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "ManifestEvaluatorCache RwLock was poisoned",
                    )
                })
                .unwrap();

            if read.contains_key(&spec_id) {
                return read.get(&spec_id).unwrap().clone();
            }
        }

        self.0
            .write()
            .map_err(|_| {
                Error::new(
                    ErrorKind::Unexpected,
                    "ManifestEvaluatorCache RwLock was poisoned",
                )
            })
            .unwrap()
            .insert(
                spec_id,
                Arc::new(ManifestEvaluator::new(partition_filter.as_ref().clone())),
            );

        let read = self
            .0
            .read()
            .map_err(|_| {
                Error::new(
                    ErrorKind::Unexpected,
                    "ManifestEvaluatorCache RwLock was poisoned",
                )
            })
            .unwrap();

        read.get(&spec_id).unwrap().clone()
    }
}

/// Manages the caching of [`ExpressionEvaluator`] objects
/// for [`PartitionSpec`]s based on partition spec id.
#[derive(Debug)]
struct ExpressionEvaluatorCache(RwLock<HashMap<i32, Arc<ExpressionEvaluator>>>);

impl ExpressionEvaluatorCache {
    /// Creates a new [`ExpressionEvaluatorCache`]
    /// with an empty internal HashMap.
    fn new() -> Self {
        Self(RwLock::new(HashMap::new()))
    }

    /// Retrieves a [`ExpressionEvaluator`] from the cache
    /// or computes it if not present.
    fn get(
        &self,
        spec_id: i32,
        partition_filter: &BoundPredicate,
    ) -> Result<Arc<ExpressionEvaluator>> {
        // we need a block here to ensure that the `read()` gets dropped before we hit the `write()`
        // below, otherwise we hit deadlock
        {
            let read = self.0.read().map_err(|_| {
                Error::new(
                    ErrorKind::Unexpected,
                    "PartitionFilterCache RwLock was poisoned",
                )
            })?;

            if read.contains_key(&spec_id) {
                return Ok(read.get(&spec_id).unwrap().clone());
            }
        }

        self.0
            .write()
            .map_err(|_| {
                Error::new(
                    ErrorKind::Unexpected,
                    "ManifestEvaluatorCache RwLock was poisoned",
                )
            })
            .unwrap()
            .insert(
                spec_id,
                Arc::new(ExpressionEvaluator::new(partition_filter.clone())),
            );

        let read = self
            .0
            .read()
            .map_err(|_| {
                Error::new(
                    ErrorKind::Unexpected,
                    "ManifestEvaluatorCache RwLock was poisoned",
                )
            })
            .unwrap();

        Ok(read.get(&spec_id).unwrap().clone())
    }
}

/// A task to scan part of file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FileScanTask {
    /// The start offset of the file to scan.
    pub start: u64,
    /// The length of the file to scan.
    pub length: u64,
    /// The number of records in the file to scan.
    ///
    /// This is an optional field, and only available if we are
    /// reading the entire data file.
    pub record_count: Option<u64>,

    /// The data file path corresponding to the task.
    pub data_file_path: String,

    /// The content type of the file to scan.
    pub data_file_content: DataContentType,

    /// The format of the file to scan.
    pub data_file_format: DataFileFormat,

    /// The schema of the file to scan.
    pub schema: SchemaRef,
    /// The field ids to project.
    pub project_field_ids: Vec<i32>,
    /// The predicate to filter.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub predicate: Option<BoundPredicate>,

    /// The list of delete files that may need to be applied to this data file
    pub deletes: Vec<FileScanTaskDeleteFile>,
}

/// A task to scan part of file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FileScanTaskDeleteFile {
    /// The delete file path
    pub file_path: String,

    /// delete file type
    pub file_type: DataContentType,

    /// partition id
    pub partition_spec_id: i32,
}

#[derive(Debug)]
pub(crate) struct DeleteFileContext {
    pub(crate) manifest_entry: ManifestEntryRef,
    pub(crate) partition_spec_id: i32,
}

impl From<&DeleteFileContext> for FileScanTaskDeleteFile {
    fn from(ctx: &DeleteFileContext) -> Self {
        FileScanTaskDeleteFile {
            file_path: ctx.manifest_entry.file_path().to_string(),
            file_type: ctx.manifest_entry.content_type(),
            partition_spec_id: ctx.partition_spec_id,
        }
    }
}

impl FileScanTask {
    /// Returns the data file path of this file scan task.
    pub fn data_file_path(&self) -> &str {
        &self.data_file_path
    }

    /// Returns the project field id of this file scan task.
    pub fn project_field_ids(&self) -> &[i32] {
        &self.project_field_ids
    }

    /// Returns the predicate of this file scan task.
    pub fn predicate(&self) -> Option<&BoundPredicate> {
        self.predicate.as_ref()
    }

    /// Returns the schema of this file scan task as a reference
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Returns the schema of this file scan task as a SchemaRef
    pub fn schema_ref(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[allow(missing_docs)]
#[cfg(test)]
pub mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::fs::File;
    use std::sync::Arc;

    use arrow_array::{
        ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    };
    use futures::{stream, TryStreamExt};
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;
    use tera::{Context, Tera};
    use uuid::Uuid;

    use crate::arrow::ArrowReaderBuilder;
    use crate::expr::{BoundPredicate, Reference};
    use crate::io::{FileIO, OutputFile};
    use crate::scan::FileScanTask;
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Datum, Literal, ManifestEntry,
        ManifestListWriter, ManifestStatus, ManifestWriterBuilder, NestedField, PartitionSpec,
        PrimitiveType, Schema, Struct, StructType, TableMetadata, Type,
    };
    use crate::table::Table;
    use crate::TableIdent;

    pub struct TableTestFixture {
        pub table_location: String,
        pub table: Table,
    }

    impl TableTestFixture {
        #[allow(clippy::new_without_default)]
        pub fn new() -> Self {
            let tmp_dir = TempDir::new().unwrap();
            let table_location = tmp_dir.path().join("table1");
            let manifest_list1_location = table_location.join("metadata/manifests_list_1.avro");
            let manifest_list2_location = table_location.join("metadata/manifests_list_2.avro");
            let table_metadata1_location = table_location.join("metadata/v1.json");

            let file_io = FileIO::from_path(table_location.as_os_str().to_str().unwrap())
                .unwrap()
                .build()
                .unwrap();

            let table_metadata = {
                let template_json_str = fs::read_to_string(format!(
                    "{}/testdata/example_table_metadata_v2.json",
                    env!("CARGO_MANIFEST_DIR")
                ))
                .unwrap();
                let mut context = Context::new();
                context.insert("table_location", &table_location);
                context.insert("manifest_list_1_location", &manifest_list1_location);
                context.insert("manifest_list_2_location", &manifest_list2_location);
                context.insert("table_metadata_1_location", &table_metadata1_location);

                let metadata_json = Tera::one_off(&template_json_str, &context, false).unwrap();
                serde_json::from_str::<TableMetadata>(&metadata_json).unwrap()
            };

            let table = Table::builder()
                .metadata(table_metadata)
                .identifier(TableIdent::from_strs(["db", "table1"]).unwrap())
                .file_io(file_io.clone())
                .metadata_location(table_metadata1_location.as_os_str().to_str().unwrap())
                .build()
                .unwrap();

            Self {
                table_location: table_location.to_str().unwrap().to_string(),
                table,
            }
        }

        pub fn new_unpartitioned() -> Self {
            let tmp_dir = TempDir::new().unwrap();
            let table_location = tmp_dir.path().join("table1");
            let manifest_list1_location = table_location.join("metadata/manifests_list_1.avro");
            let manifest_list2_location = table_location.join("metadata/manifests_list_2.avro");
            let table_metadata1_location = table_location.join("metadata/v1.json");

            let file_io = FileIO::from_path(table_location.to_str().unwrap())
                .unwrap()
                .build()
                .unwrap();

            let mut table_metadata = {
                let template_json_str = fs::read_to_string(format!(
                    "{}/testdata/example_table_metadata_v2.json",
                    env!("CARGO_MANIFEST_DIR")
                ))
                .unwrap();
                let mut context = Context::new();
                context.insert("table_location", &table_location);
                context.insert("manifest_list_1_location", &manifest_list1_location);
                context.insert("manifest_list_2_location", &manifest_list2_location);
                context.insert("table_metadata_1_location", &table_metadata1_location);

                let metadata_json = Tera::one_off(&template_json_str, &context, false).unwrap();
                serde_json::from_str::<TableMetadata>(&metadata_json).unwrap()
            };

            table_metadata.default_spec = Arc::new(PartitionSpec::unpartition_spec());
            table_metadata.partition_specs.clear();
            table_metadata.default_partition_type = StructType::new(vec![]);
            table_metadata
                .partition_specs
                .insert(0, table_metadata.default_spec.clone());

            let table = Table::builder()
                .metadata(table_metadata)
                .identifier(TableIdent::from_strs(["db", "table1"]).unwrap())
                .file_io(file_io.clone())
                .metadata_location(table_metadata1_location.to_str().unwrap())
                .build()
                .unwrap();

            Self {
                table_location: table_location.to_str().unwrap().to_string(),
                table,
            }
        }

        fn next_manifest_file(&self) -> OutputFile {
            self.table
                .file_io()
                .new_output(format!(
                    "{}/metadata/manifest_{}.avro",
                    self.table_location,
                    Uuid::new_v4()
                ))
                .unwrap()
        }

        pub async fn setup_manifest_files(&mut self) {
            let current_snapshot = self.table.metadata().current_snapshot().unwrap();
            let parent_snapshot = current_snapshot
                .parent_snapshot(self.table.metadata())
                .unwrap();
            let current_schema = current_snapshot.schema(self.table.metadata()).unwrap();
            let current_partition_spec = self.table.metadata().default_partition_spec();

            // Write data files
            let mut writer = ManifestWriterBuilder::new(
                self.next_manifest_file(),
                Some(current_snapshot.snapshot_id()),
                vec![],
                current_schema.clone(),
                current_partition_spec.as_ref().clone(),
            )
            .build_v2_data();
            writer
                .add_entry(
                    ManifestEntry::builder()
                        .status(ManifestStatus::Added)
                        .data_file(
                            DataFileBuilder::default()
                                .partition_spec_id(0)
                                .content(DataContentType::Data)
                                .file_path(format!("{}/1.parquet", &self.table_location))
                                .file_format(DataFileFormat::Parquet)
                                .file_size_in_bytes(100)
                                .record_count(1)
                                .partition(Struct::from_iter([Some(Literal::long(100))]))
                                .key_metadata(None)
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .unwrap();
            writer
                .add_delete_entry(
                    ManifestEntry::builder()
                        .status(ManifestStatus::Deleted)
                        .snapshot_id(parent_snapshot.snapshot_id())
                        .sequence_number(parent_snapshot.sequence_number())
                        .file_sequence_number(parent_snapshot.sequence_number())
                        .data_file(
                            DataFileBuilder::default()
                                .partition_spec_id(0)
                                .content(DataContentType::Data)
                                .file_path(format!("{}/2.parquet", &self.table_location))
                                .file_format(DataFileFormat::Parquet)
                                .file_size_in_bytes(100)
                                .record_count(1)
                                .partition(Struct::from_iter([Some(Literal::long(200))]))
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .unwrap();
            writer
                .add_existing_entry(
                    ManifestEntry::builder()
                        .status(ManifestStatus::Existing)
                        .snapshot_id(parent_snapshot.snapshot_id())
                        .sequence_number(parent_snapshot.sequence_number())
                        .file_sequence_number(parent_snapshot.sequence_number())
                        .data_file(
                            DataFileBuilder::default()
                                .partition_spec_id(0)
                                .content(DataContentType::Data)
                                .file_path(format!("{}/3.parquet", &self.table_location))
                                .file_format(DataFileFormat::Parquet)
                                .file_size_in_bytes(100)
                                .record_count(1)
                                .partition(Struct::from_iter([Some(Literal::long(300))]))
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .unwrap();
            let data_file_manifest = writer.write_manifest_file().await.unwrap();

            // Write to manifest list
            let mut manifest_list_write = ManifestListWriter::v2(
                self.table
                    .file_io()
                    .new_output(current_snapshot.manifest_list())
                    .unwrap(),
                current_snapshot.snapshot_id(),
                current_snapshot.parent_snapshot_id(),
                current_snapshot.sequence_number(),
            );
            manifest_list_write
                .add_manifests(vec![data_file_manifest].into_iter())
                .unwrap();
            manifest_list_write.close().await.unwrap();

            // prepare data
            let schema = {
                let fields = vec![
                    arrow_schema::Field::new("x", arrow_schema::DataType::Int64, false)
                        .with_metadata(HashMap::from([(
                            PARQUET_FIELD_ID_META_KEY.to_string(),
                            "1".to_string(),
                        )])),
                    arrow_schema::Field::new("y", arrow_schema::DataType::Int64, false)
                        .with_metadata(HashMap::from([(
                            PARQUET_FIELD_ID_META_KEY.to_string(),
                            "2".to_string(),
                        )])),
                    arrow_schema::Field::new("z", arrow_schema::DataType::Int64, false)
                        .with_metadata(HashMap::from([(
                            PARQUET_FIELD_ID_META_KEY.to_string(),
                            "3".to_string(),
                        )])),
                    arrow_schema::Field::new("a", arrow_schema::DataType::Utf8, false)
                        .with_metadata(HashMap::from([(
                            PARQUET_FIELD_ID_META_KEY.to_string(),
                            "4".to_string(),
                        )])),
                    arrow_schema::Field::new("dbl", arrow_schema::DataType::Float64, false)
                        .with_metadata(HashMap::from([(
                            PARQUET_FIELD_ID_META_KEY.to_string(),
                            "5".to_string(),
                        )])),
                    arrow_schema::Field::new("i32", arrow_schema::DataType::Int32, false)
                        .with_metadata(HashMap::from([(
                            PARQUET_FIELD_ID_META_KEY.to_string(),
                            "6".to_string(),
                        )])),
                    arrow_schema::Field::new("i64", arrow_schema::DataType::Int64, false)
                        .with_metadata(HashMap::from([(
                            PARQUET_FIELD_ID_META_KEY.to_string(),
                            "7".to_string(),
                        )])),
                    arrow_schema::Field::new("bool", arrow_schema::DataType::Boolean, false)
                        .with_metadata(HashMap::from([(
                            PARQUET_FIELD_ID_META_KEY.to_string(),
                            "8".to_string(),
                        )])),
                ];
                Arc::new(arrow_schema::Schema::new(fields))
            };
            // x: [1, 1, 1, 1, ...]
            let col1 = Arc::new(Int64Array::from_iter_values(vec![1; 1024])) as ArrayRef;

            let mut values = vec![2; 512];
            values.append(vec![3; 200].as_mut());
            values.append(vec![4; 300].as_mut());
            values.append(vec![5; 12].as_mut());

            // y: [2, 2, 2, 2, ..., 3, 3, 3, 3, ..., 4, 4, 4, 4, ..., 5, 5, 5, 5]
            let col2 = Arc::new(Int64Array::from_iter_values(values)) as ArrayRef;

            let mut values = vec![3; 512];
            values.append(vec![4; 512].as_mut());

            // z: [3, 3, 3, 3, ..., 4, 4, 4, 4]
            let col3 = Arc::new(Int64Array::from_iter_values(values)) as ArrayRef;

            // a: ["Apache", "Apache", "Apache", ..., "Iceberg", "Iceberg", "Iceberg"]
            let mut values = vec!["Apache"; 512];
            values.append(vec!["Iceberg"; 512].as_mut());
            let col4 = Arc::new(StringArray::from_iter_values(values)) as ArrayRef;

            // dbl:
            let mut values = vec![100.0f64; 512];
            values.append(vec![150.0f64; 12].as_mut());
            values.append(vec![200.0f64; 500].as_mut());
            let col5 = Arc::new(Float64Array::from_iter_values(values)) as ArrayRef;

            // i32:
            let mut values = vec![100i32; 512];
            values.append(vec![150i32; 12].as_mut());
            values.append(vec![200i32; 500].as_mut());
            let col6 = Arc::new(Int32Array::from_iter_values(values)) as ArrayRef;

            // i64:
            let mut values = vec![100i64; 512];
            values.append(vec![150i64; 12].as_mut());
            values.append(vec![200i64; 500].as_mut());
            let col7 = Arc::new(Int64Array::from_iter_values(values)) as ArrayRef;

            // bool:
            let mut values = vec![false; 512];
            values.append(vec![true; 512].as_mut());
            let values: BooleanArray = values.into();
            let col8 = Arc::new(values) as ArrayRef;

            let to_write = RecordBatch::try_new(schema.clone(), vec![
                col1, col2, col3, col4, col5, col6, col7, col8,
            ])
            .unwrap();

            // Write the Parquet files
            let props = WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build();

            for n in 1..=3 {
                let file = File::create(format!("{}/{}.parquet", &self.table_location, n)).unwrap();
                let mut writer =
                    ArrowWriter::try_new(file, to_write.schema(), Some(props.clone())).unwrap();

                writer.write(&to_write).expect("Writing batch");

                // writer must be closed to write footer
                writer.close().unwrap();
            }
        }

        pub async fn setup_unpartitioned_manifest_files(&mut self) {
            let current_snapshot = self.table.metadata().current_snapshot().unwrap();
            let parent_snapshot = current_snapshot
                .parent_snapshot(self.table.metadata())
                .unwrap();
            let current_schema = current_snapshot.schema(self.table.metadata()).unwrap();
            let current_partition_spec = Arc::new(PartitionSpec::unpartition_spec());

            // Write data files using an empty partition for unpartitioned tables.
            let mut writer = ManifestWriterBuilder::new(
                self.next_manifest_file(),
                Some(current_snapshot.snapshot_id()),
                vec![],
                current_schema.clone(),
                current_partition_spec.as_ref().clone(),
            )
            .build_v2_data();

            // Create an empty partition value.
            let empty_partition = Struct::empty();

            writer
                .add_entry(
                    ManifestEntry::builder()
                        .status(ManifestStatus::Added)
                        .data_file(
                            DataFileBuilder::default()
                                .partition_spec_id(0)
                                .content(DataContentType::Data)
                                .file_path(format!("{}/1.parquet", &self.table_location))
                                .file_format(DataFileFormat::Parquet)
                                .file_size_in_bytes(100)
                                .record_count(1)
                                .partition(empty_partition.clone())
                                .key_metadata(None)
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .unwrap();

            writer
                .add_delete_entry(
                    ManifestEntry::builder()
                        .status(ManifestStatus::Deleted)
                        .snapshot_id(parent_snapshot.snapshot_id())
                        .sequence_number(parent_snapshot.sequence_number())
                        .file_sequence_number(parent_snapshot.sequence_number())
                        .data_file(
                            DataFileBuilder::default()
                                .partition_spec_id(0)
                                .content(DataContentType::Data)
                                .file_path(format!("{}/2.parquet", &self.table_location))
                                .file_format(DataFileFormat::Parquet)
                                .file_size_in_bytes(100)
                                .record_count(1)
                                .partition(empty_partition.clone())
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .unwrap();

            writer
                .add_existing_entry(
                    ManifestEntry::builder()
                        .status(ManifestStatus::Existing)
                        .snapshot_id(parent_snapshot.snapshot_id())
                        .sequence_number(parent_snapshot.sequence_number())
                        .file_sequence_number(parent_snapshot.sequence_number())
                        .data_file(
                            DataFileBuilder::default()
                                .partition_spec_id(0)
                                .content(DataContentType::Data)
                                .file_path(format!("{}/3.parquet", &self.table_location))
                                .file_format(DataFileFormat::Parquet)
                                .file_size_in_bytes(100)
                                .record_count(1)
                                .partition(empty_partition.clone())
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .unwrap();

            let data_file_manifest = writer.write_manifest_file().await.unwrap();

            // Write to manifest list
            let mut manifest_list_write = ManifestListWriter::v2(
                self.table
                    .file_io()
                    .new_output(current_snapshot.manifest_list())
                    .unwrap(),
                current_snapshot.snapshot_id(),
                current_snapshot.parent_snapshot_id(),
                current_snapshot.sequence_number(),
            );
            manifest_list_write
                .add_manifests(vec![data_file_manifest].into_iter())
                .unwrap();
            manifest_list_write.close().await.unwrap();

            // prepare data for parquet files
            let schema = {
                let fields = vec![
                    arrow_schema::Field::new("x", arrow_schema::DataType::Int64, false)
                        .with_metadata(HashMap::from([(
                            PARQUET_FIELD_ID_META_KEY.to_string(),
                            "1".to_string(),
                        )])),
                    arrow_schema::Field::new("y", arrow_schema::DataType::Int64, false)
                        .with_metadata(HashMap::from([(
                            PARQUET_FIELD_ID_META_KEY.to_string(),
                            "2".to_string(),
                        )])),
                    arrow_schema::Field::new("z", arrow_schema::DataType::Int64, false)
                        .with_metadata(HashMap::from([(
                            PARQUET_FIELD_ID_META_KEY.to_string(),
                            "3".to_string(),
                        )])),
                    arrow_schema::Field::new("a", arrow_schema::DataType::Utf8, false)
                        .with_metadata(HashMap::from([(
                            PARQUET_FIELD_ID_META_KEY.to_string(),
                            "4".to_string(),
                        )])),
                    arrow_schema::Field::new("dbl", arrow_schema::DataType::Float64, false)
                        .with_metadata(HashMap::from([(
                            PARQUET_FIELD_ID_META_KEY.to_string(),
                            "5".to_string(),
                        )])),
                    arrow_schema::Field::new("i32", arrow_schema::DataType::Int32, false)
                        .with_metadata(HashMap::from([(
                            PARQUET_FIELD_ID_META_KEY.to_string(),
                            "6".to_string(),
                        )])),
                    arrow_schema::Field::new("i64", arrow_schema::DataType::Int64, false)
                        .with_metadata(HashMap::from([(
                            PARQUET_FIELD_ID_META_KEY.to_string(),
                            "7".to_string(),
                        )])),
                    arrow_schema::Field::new("bool", arrow_schema::DataType::Boolean, false)
                        .with_metadata(HashMap::from([(
                            PARQUET_FIELD_ID_META_KEY.to_string(),
                            "8".to_string(),
                        )])),
                ];
                Arc::new(arrow_schema::Schema::new(fields))
            };

            // Build the arrays for the RecordBatch
            let col1 = Arc::new(Int64Array::from_iter_values(vec![1; 1024])) as ArrayRef;

            let mut values = vec![2; 512];
            values.append(vec![3; 200].as_mut());
            values.append(vec![4; 300].as_mut());
            values.append(vec![5; 12].as_mut());
            let col2 = Arc::new(Int64Array::from_iter_values(values)) as ArrayRef;

            let mut values = vec![3; 512];
            values.append(vec![4; 512].as_mut());
            let col3 = Arc::new(Int64Array::from_iter_values(values)) as ArrayRef;

            let mut values = vec!["Apache"; 512];
            values.append(vec!["Iceberg"; 512].as_mut());
            let col4 = Arc::new(StringArray::from_iter_values(values)) as ArrayRef;

            let mut values = vec![100.0f64; 512];
            values.append(vec![150.0f64; 12].as_mut());
            values.append(vec![200.0f64; 500].as_mut());
            let col5 = Arc::new(Float64Array::from_iter_values(values)) as ArrayRef;

            let mut values = vec![100i32; 512];
            values.append(vec![150i32; 12].as_mut());
            values.append(vec![200i32; 500].as_mut());
            let col6 = Arc::new(Int32Array::from_iter_values(values)) as ArrayRef;

            let mut values = vec![100i64; 512];
            values.append(vec![150i64; 12].as_mut());
            values.append(vec![200i64; 500].as_mut());
            let col7 = Arc::new(Int64Array::from_iter_values(values)) as ArrayRef;

            let mut values = vec![false; 512];
            values.append(vec![true; 512].as_mut());
            let values: BooleanArray = values.into();
            let col8 = Arc::new(values) as ArrayRef;

            let to_write = RecordBatch::try_new(schema.clone(), vec![
                col1, col2, col3, col4, col5, col6, col7, col8,
            ])
            .unwrap();

            // Write the Parquet files
            let props = WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build();

            for n in 1..=3 {
                let file = File::create(format!("{}/{}.parquet", &self.table_location, n)).unwrap();
                let mut writer =
                    ArrowWriter::try_new(file, to_write.schema(), Some(props.clone())).unwrap();

                writer.write(&to_write).expect("Writing batch");

                // writer must be closed to write footer
                writer.close().unwrap();
            }
        }
    }

    #[test]
    fn test_table_scan_columns() {
        let table = TableTestFixture::new().table;

        let table_scan = table.scan().select(["x", "y"]).build().unwrap();
        assert_eq!(
            Some(vec!["x".to_string(), "y".to_string()]),
            table_scan.column_names
        );

        let table_scan = table
            .scan()
            .select(["x", "y"])
            .select(["z"])
            .build()
            .unwrap();
        assert_eq!(Some(vec!["z".to_string()]), table_scan.column_names);
    }

    #[test]
    fn test_select_all() {
        let table = TableTestFixture::new().table;

        let table_scan = table.scan().select_all().build().unwrap();
        assert!(table_scan.column_names.is_none());
    }

    #[test]
    fn test_select_no_exist_column() {
        let table = TableTestFixture::new().table;

        let table_scan = table.scan().select(["x", "y", "z", "a", "b"]).build();
        assert!(table_scan.is_err());
    }

    #[test]
    fn test_table_scan_default_snapshot_id() {
        let table = TableTestFixture::new().table;

        let table_scan = table.scan().build().unwrap();
        assert_eq!(
            table.metadata().current_snapshot().unwrap().snapshot_id(),
            table_scan.snapshot().snapshot_id()
        );
    }

    #[test]
    fn test_table_scan_non_exist_snapshot_id() {
        let table = TableTestFixture::new().table;

        let table_scan = table.scan().snapshot_id(1024).build();
        assert!(table_scan.is_err());
    }

    #[test]
    fn test_table_scan_with_snapshot_id() {
        let table = TableTestFixture::new().table;

        let table_scan = table
            .scan()
            .snapshot_id(3051729675574597004)
            .with_row_selection_enabled(true)
            .build()
            .unwrap();
        assert_eq!(table_scan.snapshot().snapshot_id(), 3051729675574597004);
    }

    #[tokio::test]
    async fn test_plan_files_no_deletions() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        // Create table scan for current snapshot and plan files
        let table_scan = fixture
            .table
            .scan()
            .with_row_selection_enabled(true)
            .build()
            .unwrap();

        let mut tasks = table_scan
            .plan_files()
            .await
            .unwrap()
            .try_fold(vec![], |mut acc, task| async move {
                acc.push(task);
                Ok(acc)
            })
            .await
            .unwrap();

        assert_eq!(tasks.len(), 2);

        tasks.sort_by_key(|t| t.data_file_path.to_string());

        // Check first task is added data file
        assert_eq!(
            tasks[0].data_file_path,
            format!("{}/1.parquet", &fixture.table_location)
        );

        // Check second task is existing data file
        assert_eq!(
            tasks[1].data_file_path,
            format!("{}/3.parquet", &fixture.table_location)
        );
    }

    #[tokio::test]
    async fn test_open_parquet_no_deletions() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        // Create table scan for current snapshot and plan files
        let table_scan = fixture
            .table
            .scan()
            .with_row_selection_enabled(true)
            .build()
            .unwrap();

        let batch_stream = table_scan.to_arrow().await.unwrap();

        let batches: Vec<_> = batch_stream.try_collect().await.unwrap();

        let col = batches[0].column_by_name("x").unwrap();

        let int64_arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int64_arr.value(0), 1);
    }

    #[tokio::test]
    async fn test_open_parquet_no_deletions_by_separate_reader() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        // Create table scan for current snapshot and plan files
        let table_scan = fixture
            .table
            .scan()
            .with_row_selection_enabled(true)
            .build()
            .unwrap();

        let mut plan_task: Vec<_> = table_scan
            .plan_files()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(plan_task.len(), 2);

        let reader = ArrowReaderBuilder::new(fixture.table.file_io().clone()).build();
        let batch_stream = reader
            .clone()
            .read(Box::pin(stream::iter(vec![Ok(plan_task.remove(0))])))
            .await
            .unwrap();
        let batch_1: Vec<_> = batch_stream.try_collect().await.unwrap();

        let reader = ArrowReaderBuilder::new(fixture.table.file_io().clone()).build();
        let batch_stream = reader
            .read(Box::pin(stream::iter(vec![Ok(plan_task.remove(0))])))
            .await
            .unwrap();
        let batch_2: Vec<_> = batch_stream.try_collect().await.unwrap();

        assert_eq!(batch_1, batch_2);
    }

    #[tokio::test]
    async fn test_open_parquet_with_projection() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        // Create table scan for current snapshot and plan files
        let table_scan = fixture
            .table
            .scan()
            .select(["x", "z"])
            .with_row_selection_enabled(true)
            .build()
            .unwrap();

        let batch_stream = table_scan.to_arrow().await.unwrap();

        let batches: Vec<_> = batch_stream.try_collect().await.unwrap();

        assert_eq!(batches[0].num_columns(), 2);

        let col1 = batches[0].column_by_name("x").unwrap();
        let int64_arr = col1.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int64_arr.value(0), 1);

        let col2 = batches[0].column_by_name("z").unwrap();
        let int64_arr = col2.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int64_arr.value(0), 3);

        // test empty scan
        let table_scan = fixture.table.scan().select_empty().build().unwrap();
        let batch_stream = table_scan.to_arrow().await.unwrap();
        let batches: Vec<_> = batch_stream.try_collect().await.unwrap();

        assert_eq!(batches[0].num_columns(), 0);
        assert_eq!(batches[0].num_rows(), 1024);
    }

    #[tokio::test]
    async fn test_filter_on_arrow_lt() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        // Filter: y < 3
        let mut builder = fixture.table.scan();
        let predicate = Reference::new("y").less_than(Datum::long(3));
        builder = builder
            .with_filter(predicate)
            .with_row_selection_enabled(true);
        let table_scan = builder.build().unwrap();

        let batch_stream = table_scan.to_arrow().await.unwrap();

        let batches: Vec<_> = batch_stream.try_collect().await.unwrap();

        assert_eq!(batches[0].num_rows(), 512);

        let col = batches[0].column_by_name("x").unwrap();
        let int64_arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int64_arr.value(0), 1);

        let col = batches[0].column_by_name("y").unwrap();
        let int64_arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int64_arr.value(0), 2);
    }

    #[tokio::test]
    async fn test_filter_on_arrow_gt_eq() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        // Filter: y >= 5
        let mut builder = fixture.table.scan();
        let predicate = Reference::new("y").greater_than_or_equal_to(Datum::long(5));
        builder = builder
            .with_filter(predicate)
            .with_row_selection_enabled(true);
        let table_scan = builder.build().unwrap();

        let batch_stream = table_scan.to_arrow().await.unwrap();

        let batches: Vec<_> = batch_stream.try_collect().await.unwrap();

        assert_eq!(batches[0].num_rows(), 12);

        let col = batches[0].column_by_name("x").unwrap();
        let int64_arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int64_arr.value(0), 1);

        let col = batches[0].column_by_name("y").unwrap();
        let int64_arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int64_arr.value(0), 5);
    }

    #[tokio::test]
    async fn test_filter_double_eq() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        // Filter: dbl == 150.0
        let mut builder = fixture.table.scan();
        let predicate = Reference::new("dbl").equal_to(Datum::double(150.0f64));
        builder = builder
            .with_filter(predicate)
            .with_row_selection_enabled(true);
        let table_scan = builder.build().unwrap();

        let batch_stream = table_scan.to_arrow().await.unwrap();

        let batches: Vec<_> = batch_stream.try_collect().await.unwrap();

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 12);

        let col = batches[0].column_by_name("dbl").unwrap();
        let f64_arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(f64_arr.value(1), 150.0f64);
    }

    #[tokio::test]
    async fn test_filter_int_eq() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        // Filter: i32 == 150
        let mut builder = fixture.table.scan();
        let predicate = Reference::new("i32").equal_to(Datum::int(150i32));
        builder = builder
            .with_filter(predicate)
            .with_row_selection_enabled(true);
        let table_scan = builder.build().unwrap();

        let batch_stream = table_scan.to_arrow().await.unwrap();

        let batches: Vec<_> = batch_stream.try_collect().await.unwrap();

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 12);

        let col = batches[0].column_by_name("i32").unwrap();
        let i32_arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(i32_arr.value(1), 150i32);
    }

    #[tokio::test]
    async fn test_filter_long_eq() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        // Filter: i64 == 150
        let mut builder = fixture.table.scan();
        let predicate = Reference::new("i64").equal_to(Datum::long(150i64));
        builder = builder
            .with_filter(predicate)
            .with_row_selection_enabled(true);
        let table_scan = builder.build().unwrap();

        let batch_stream = table_scan.to_arrow().await.unwrap();

        let batches: Vec<_> = batch_stream.try_collect().await.unwrap();

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 12);

        let col = batches[0].column_by_name("i64").unwrap();
        let i64_arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(i64_arr.value(1), 150i64);
    }

    #[tokio::test]
    async fn test_filter_bool_eq() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        // Filter: bool == true
        let mut builder = fixture.table.scan();
        let predicate = Reference::new("bool").equal_to(Datum::bool(true));
        builder = builder
            .with_filter(predicate)
            .with_row_selection_enabled(true);
        let table_scan = builder.build().unwrap();

        let batch_stream = table_scan.to_arrow().await.unwrap();

        let batches: Vec<_> = batch_stream.try_collect().await.unwrap();

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 512);

        let col = batches[0].column_by_name("bool").unwrap();
        let bool_arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(bool_arr.value(1));
    }

    #[tokio::test]
    async fn test_filter_on_arrow_is_null() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        // Filter: y is null
        let mut builder = fixture.table.scan();
        let predicate = Reference::new("y").is_null();
        builder = builder
            .with_filter(predicate)
            .with_row_selection_enabled(true);
        let table_scan = builder.build().unwrap();

        let batch_stream = table_scan.to_arrow().await.unwrap();

        let batches: Vec<_> = batch_stream.try_collect().await.unwrap();
        assert_eq!(batches.len(), 0);
    }

    #[tokio::test]
    async fn test_filter_on_arrow_is_not_null() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        // Filter: y is not null
        let mut builder = fixture.table.scan();
        let predicate = Reference::new("y").is_not_null();
        builder = builder
            .with_filter(predicate)
            .with_row_selection_enabled(true);
        let table_scan = builder.build().unwrap();

        let batch_stream = table_scan.to_arrow().await.unwrap();

        let batches: Vec<_> = batch_stream.try_collect().await.unwrap();
        assert_eq!(batches[0].num_rows(), 1024);
    }

    #[tokio::test]
    async fn test_filter_on_arrow_lt_and_gt() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        // Filter: y < 5 AND z >= 4
        let mut builder = fixture.table.scan();
        let predicate = Reference::new("y")
            .less_than(Datum::long(5))
            .and(Reference::new("z").greater_than_or_equal_to(Datum::long(4)));
        builder = builder
            .with_filter(predicate)
            .with_row_selection_enabled(true);
        let table_scan = builder.build().unwrap();

        let batch_stream = table_scan.to_arrow().await.unwrap();

        let batches: Vec<_> = batch_stream.try_collect().await.unwrap();
        assert_eq!(batches[0].num_rows(), 500);

        let col = batches[0].column_by_name("x").unwrap();
        let expected_x = Arc::new(Int64Array::from_iter_values(vec![1; 500])) as ArrayRef;
        assert_eq!(col, &expected_x);

        let col = batches[0].column_by_name("y").unwrap();
        let mut values = vec![];
        values.append(vec![3; 200].as_mut());
        values.append(vec![4; 300].as_mut());
        let expected_y = Arc::new(Int64Array::from_iter_values(values)) as ArrayRef;
        assert_eq!(col, &expected_y);

        let col = batches[0].column_by_name("z").unwrap();
        let expected_z = Arc::new(Int64Array::from_iter_values(vec![4; 500])) as ArrayRef;
        assert_eq!(col, &expected_z);
    }

    #[tokio::test]
    async fn test_filter_on_arrow_lt_or_gt() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        // Filter: y < 5 AND z >= 4
        let mut builder = fixture.table.scan();
        let predicate = Reference::new("y")
            .less_than(Datum::long(5))
            .or(Reference::new("z").greater_than_or_equal_to(Datum::long(4)));
        builder = builder
            .with_filter(predicate)
            .with_row_selection_enabled(true);
        let table_scan = builder.build().unwrap();

        let batch_stream = table_scan.to_arrow().await.unwrap();

        let batches: Vec<_> = batch_stream.try_collect().await.unwrap();
        assert_eq!(batches[0].num_rows(), 1024);

        let col = batches[0].column_by_name("x").unwrap();
        let expected_x = Arc::new(Int64Array::from_iter_values(vec![1; 1024])) as ArrayRef;
        assert_eq!(col, &expected_x);

        let col = batches[0].column_by_name("y").unwrap();
        let mut values = vec![2; 512];
        values.append(vec![3; 200].as_mut());
        values.append(vec![4; 300].as_mut());
        values.append(vec![5; 12].as_mut());
        let expected_y = Arc::new(Int64Array::from_iter_values(values)) as ArrayRef;
        assert_eq!(col, &expected_y);

        let col = batches[0].column_by_name("z").unwrap();
        let mut values = vec![3; 512];
        values.append(vec![4; 512].as_mut());
        let expected_z = Arc::new(Int64Array::from_iter_values(values)) as ArrayRef;
        assert_eq!(col, &expected_z);
    }

    #[tokio::test]
    async fn test_filter_on_arrow_startswith() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        // Filter: a STARTSWITH "Ice"
        let mut builder = fixture.table.scan();
        let predicate = Reference::new("a").starts_with(Datum::string("Ice"));
        builder = builder
            .with_filter(predicate)
            .with_row_selection_enabled(true);
        let table_scan = builder.build().unwrap();

        let batch_stream = table_scan.to_arrow().await.unwrap();

        let batches: Vec<_> = batch_stream.try_collect().await.unwrap();

        assert_eq!(batches[0].num_rows(), 512);

        let col = batches[0].column_by_name("a").unwrap();
        let string_arr = col.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(string_arr.value(0), "Iceberg");
    }

    #[tokio::test]
    async fn test_filter_on_arrow_not_startswith() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        // Filter: a NOT STARTSWITH "Ice"
        let mut builder = fixture.table.scan();
        let predicate = Reference::new("a").not_starts_with(Datum::string("Ice"));
        builder = builder
            .with_filter(predicate)
            .with_row_selection_enabled(true);
        let table_scan = builder.build().unwrap();

        let batch_stream = table_scan.to_arrow().await.unwrap();

        let batches: Vec<_> = batch_stream.try_collect().await.unwrap();

        assert_eq!(batches[0].num_rows(), 512);

        let col = batches[0].column_by_name("a").unwrap();
        let string_arr = col.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(string_arr.value(0), "Apache");
    }

    #[tokio::test]
    async fn test_filter_on_arrow_in() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        // Filter: a IN ("Sioux", "Iceberg")
        let mut builder = fixture.table.scan();
        let predicate =
            Reference::new("a").is_in([Datum::string("Sioux"), Datum::string("Iceberg")]);
        builder = builder
            .with_filter(predicate)
            .with_row_selection_enabled(true);
        let table_scan = builder.build().unwrap();

        let batch_stream = table_scan.to_arrow().await.unwrap();

        let batches: Vec<_> = batch_stream.try_collect().await.unwrap();

        assert_eq!(batches[0].num_rows(), 512);

        let col = batches[0].column_by_name("a").unwrap();
        let string_arr = col.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(string_arr.value(0), "Iceberg");
    }

    #[tokio::test]
    async fn test_filter_on_arrow_not_in() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        // Filter: a NOT IN ("Sioux", "Iceberg")
        let mut builder = fixture.table.scan();
        let predicate =
            Reference::new("a").is_not_in([Datum::string("Sioux"), Datum::string("Iceberg")]);
        builder = builder
            .with_filter(predicate)
            .with_row_selection_enabled(true);
        let table_scan = builder.build().unwrap();

        let batch_stream = table_scan.to_arrow().await.unwrap();

        let batches: Vec<_> = batch_stream.try_collect().await.unwrap();

        assert_eq!(batches[0].num_rows(), 512);

        let col = batches[0].column_by_name("a").unwrap();
        let string_arr = col.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(string_arr.value(0), "Apache");
    }

    #[test]
    fn test_file_scan_task_serialize_deserialize() {
        let test_fn = |task: FileScanTask| {
            let serialized = serde_json::to_string(&task).unwrap();
            let deserialized: FileScanTask = serde_json::from_str(&serialized).unwrap();

            assert_eq!(task.data_file_path, deserialized.data_file_path);
            assert_eq!(task.start, deserialized.start);
            assert_eq!(task.length, deserialized.length);
            assert_eq!(task.project_field_ids, deserialized.project_field_ids);
            assert_eq!(task.predicate, deserialized.predicate);
            assert_eq!(task.schema, deserialized.schema);
        };

        // without predicate
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "x",
                    Type::Primitive(PrimitiveType::Binary),
                ))])
                .build()
                .unwrap(),
        );
        let task = FileScanTask {
            data_file_path: "data_file_path".to_string(),
            data_file_content: DataContentType::Data,
            start: 0,
            length: 100,
            project_field_ids: vec![1, 2, 3],
            predicate: None,
            schema: schema.clone(),
            record_count: Some(100),
            data_file_format: DataFileFormat::Parquet,
            deletes: vec![],
        };
        test_fn(task);

        // with predicate
        let task = FileScanTask {
            data_file_path: "data_file_path".to_string(),
            data_file_content: DataContentType::Data,
            start: 0,
            length: 100,
            project_field_ids: vec![1, 2, 3],
            predicate: Some(BoundPredicate::AlwaysTrue),
            schema,
            record_count: None,
            data_file_format: DataFileFormat::Avro,
            deletes: vec![],
        };
        test_fn(task);
    }
}
