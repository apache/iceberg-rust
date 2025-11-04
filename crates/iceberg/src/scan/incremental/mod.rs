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

//! Incremental table scan implementation.

use std::collections::HashSet;
use std::sync::Arc;

use crate::arrow::caching_delete_file_loader::CachingDeleteFileLoader;
use crate::arrow::delete_filter::DeleteFilter;
use crate::arrow::{
    ArrowReaderBuilder, CombinedIncrementalBatchRecordStream, StreamsInto,
    UnzippedIncrementalBatchRecordStream,
};
use crate::delete_file_index::DeleteFileIndex;
use crate::io::FileIO;
use crate::scan::DeleteFileContext;
use crate::scan::cache::ExpressionEvaluatorCache;
use crate::scan::context::ManifestEntryContext;
use crate::spec::{DataContentType, ManifestStatus, Snapshot, SnapshotRef};
use crate::table::Table;
use crate::util::snapshot::ancestors_between;
use crate::utils::available_parallelism;
use crate::{Error, ErrorKind, Result};

mod context;
use context::*;
mod task;
use futures::channel::mpsc::{Sender, channel};
use futures::{SinkExt, StreamExt, TryStreamExt};
use itertools::Itertools;
pub use task::*;

use crate::runtime::spawn;

/// Builder for an incremental table scan.
#[derive(Debug)]
pub struct IncrementalTableScanBuilder<'a> {
    table: &'a Table,
    // Defaults to `None`, which means all columns.
    column_names: Option<Vec<String>>,
    from_snapshot_id: i64,
    to_snapshot_id: i64,
    batch_size: Option<usize>,
    concurrency_limit_data_files: usize,
    concurrency_limit_manifest_entries: usize,
    concurrency_limit_manifest_files: usize,
}

impl<'a> IncrementalTableScanBuilder<'a> {
    pub(crate) fn new(table: &'a Table, from_snapshot_id: i64, to_snapshot_id: i64) -> Self {
        let num_cpus = available_parallelism().get();
        Self {
            table,
            column_names: None,
            from_snapshot_id,
            to_snapshot_id,
            batch_size: None,
            concurrency_limit_data_files: num_cpus,
            concurrency_limit_manifest_entries: num_cpus,
            concurrency_limit_manifest_files: num_cpus,
        }
    }

    /// Set the batch size for reading data files.
    pub fn with_batch_size(mut self, batch_size: Option<usize>) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Select all columns of the table.
    pub fn select_all(mut self) -> Self {
        self.column_names = None;
        self
    }

    /// Select no columns of the table.
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

    /// Set the `from_snapshot_id` for the incremental scan.
    pub fn from_snapshot_id(mut self, from_snapshot_id: i64) -> Self {
        self.from_snapshot_id = from_snapshot_id;
        self
    }

    /// Set the `to_snapshot_id` for the incremental scan.
    pub fn to_snapshot_id(mut self, to_snapshot_id: i64) -> Self {
        self.to_snapshot_id = to_snapshot_id;
        self
    }

    /// Set the concurrency limit for reading data files.
    pub fn with_concurrency_limit_data_files(mut self, limit: usize) -> Self {
        self.concurrency_limit_data_files = limit;
        self
    }

    /// Set the concurrency limit for reading manifest entries.
    pub fn with_concurrency_limit_manifest_entries(mut self, limit: usize) -> Self {
        self.concurrency_limit_manifest_entries = limit;
        self
    }

    /// Set the concurrency limit for reading manifest files.
    pub fn with_concurrency_limit_manifest_files(mut self, limit: usize) -> Self {
        self.concurrency_limit_manifest_files = limit;
        self
    }

    /// Build the incremental table scan.
    pub fn build(self) -> Result<IncrementalTableScan> {
        let snapshot_from: Arc<Snapshot> = self
            .table
            .metadata()
            .snapshot_by_id(self.from_snapshot_id)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Snapshot with id {} not found", self.from_snapshot_id),
                )
            })?
            .clone();

        let snapshot_to: Arc<Snapshot> = self
            .table
            .metadata()
            .snapshot_by_id(self.to_snapshot_id)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Snapshot with id {} not found", self.to_snapshot_id),
                )
            })?
            .clone();

        let snapshots = ancestors_between(
            &self.table.metadata_ref(),
            snapshot_to.snapshot_id(),
            Some(snapshot_from.snapshot_id()),
        )
        .collect_vec();

        if !snapshots.is_empty() {
            assert_eq!(
                snapshots.first().map(|s| s.snapshot_id()),
                Some(snapshot_to.snapshot_id())
            );
        }

        let schema = snapshot_to.schema(self.table.metadata())?;

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

        let plan_context = IncrementalPlanContext {
            snapshots,
            from_snapshot: snapshot_from,
            table_metadata: self.table.metadata_ref(),
            to_snapshot_schema: schema,
            object_cache: self.table.object_cache().clone(),
            field_ids: Arc::new(field_ids),
            expression_evaluator_cache: Arc::new(ExpressionEvaluatorCache::new()),
            caching_delete_file_loader: CachingDeleteFileLoader::new(
                self.table.file_io().clone(),
                self.concurrency_limit_data_files,
            ),
        };

        Ok(IncrementalTableScan {
            plan_context,
            file_io: self.table.file_io().clone(),
            column_names: Some(column_names),
            batch_size: self.batch_size,
            concurrency_limit_data_files: self.concurrency_limit_data_files,
            concurrency_limit_manifest_entries: self.concurrency_limit_manifest_entries,
            concurrency_limit_manifest_files: self.concurrency_limit_manifest_files,
        })
    }
}

/// An incremental table scan.
#[derive(Debug)]
pub struct IncrementalTableScan {
    plan_context: IncrementalPlanContext,
    file_io: FileIO,
    column_names: Option<Vec<String>>,
    batch_size: Option<usize>,
    concurrency_limit_data_files: usize,
    concurrency_limit_manifest_entries: usize,
    concurrency_limit_manifest_files: usize,
}

impl IncrementalTableScan {
    /// Returns the `from` snapshot of this incremental table scan.
    pub fn snapshot_from(&self) -> &SnapshotRef {
        &self.plan_context.from_snapshot
    }

    /// Returns the snapshots involved in this incremental table scan.
    pub fn snapshots(&self) -> &[SnapshotRef] {
        &self.plan_context.snapshots
    }

    /// Returns the `to` snapshot of this incremental table scan.
    pub fn snapshot_to(&self) -> &SnapshotRef {
        self.snapshots()
            .first()
            .expect("There is always at least one snapshot")
    }

    /// Returns the selected column names of this incremental table scan.
    /// If `None`, all columns are selected.
    pub fn column_names(&self) -> Option<&[String]> {
        self.column_names.as_deref()
    }

    /// Plans the files to be read in this incremental table scan.
    pub async fn plan_files(&self) -> Result<IncrementalFileScanTaskStream> {
        let concurrency_limit_manifest_files = self.concurrency_limit_manifest_files;
        let concurrency_limit_manifest_entries = self.concurrency_limit_manifest_entries;

        // Used to stream `ManifestEntryContexts` between stages of the planning operation.
        let (manifest_entry_data_ctx_tx, manifest_entry_data_ctx_rx) =
            channel(concurrency_limit_manifest_files);
        let (manifest_entry_delete_ctx_tx, manifest_entry_delete_ctx_rx) =
            channel(concurrency_limit_manifest_files);

        // Used to stream the results back to the caller.
        let (file_scan_task_tx, file_scan_task_rx) = channel(concurrency_limit_manifest_entries);

        let (delete_file_idx, delete_file_tx) = DeleteFileIndex::new();

        let manifest_file_contexts = self
            .plan_context
            .build_manifest_file_contexts(
                manifest_entry_data_ctx_tx,
                delete_file_idx.clone(),
                manifest_entry_delete_ctx_tx,
            )
            .await?;

        let mut channel_for_manifest_error: Sender<Result<_>> = file_scan_task_tx.clone();

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
        let mut channel_for_delete_manifest_entry_error = file_scan_task_tx.clone();

        // Process the delete file [`ManifestEntry`] stream in parallel. Builds the delete
        // index below.
        spawn(async move {
            let result = manifest_entry_delete_ctx_rx
                .map(|me_ctx| Ok((me_ctx, delete_file_tx.clone())))
                .try_for_each_concurrent(
                    concurrency_limit_manifest_entries,
                    |(manifest_entry_context, tx)| async move {
                        spawn(async move {
                            Self::process_delete_manifest_entry(tx, manifest_entry_context).await
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

        // TODO: Streaming this into the delete index seems somewhat redundant, as we
        // could directly stream into the CachingDeleteFileLoader and instantly load the
        // delete files.
        let positional_deletes = delete_file_idx.positional_deletes().await;
        let result = self
            .plan_context
            .caching_delete_file_loader
            .load_deletes(
                &positional_deletes,
                self.plan_context.to_snapshot_schema.clone(),
            )
            .await;

        // Build the delete filter from the loaded deletes.
        let delete_filter = match result {
            Ok(loaded_deletes) => loaded_deletes.unwrap(),
            Err(e) => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("Failed to load positional deletes: {}", e),
                ));
            }
        };

        // Process the data file [`ManifestEntry`] stream in parallel
        let filter = delete_filter.clone();
        spawn(async move {
            let result = manifest_entry_data_ctx_rx
                .map(|me_ctx| Ok((me_ctx, file_scan_task_tx.clone())))
                .try_for_each_concurrent(
                    concurrency_limit_manifest_entries,
                    |(manifest_entry_context, tx)| {
                        let filter = filter.clone();
                        async move {
                            if manifest_entry_context.manifest_entry.status()
                                == ManifestStatus::Added
                            {
                                spawn(async move {
                                    Self::process_data_manifest_entry(
                                        tx,
                                        manifest_entry_context,
                                        &filter,
                                    )
                                    .await
                                })
                                .await
                            } else if manifest_entry_context.manifest_entry.status()
                                == ManifestStatus::Deleted
                            {
                                spawn(async move {
                                    Self::process_deleted_data_manifest_entry(
                                        tx,
                                        manifest_entry_context,
                                    )
                                    .await
                                })
                                .await
                            } else {
                                Ok(())
                            }
                        }
                    },
                )
                .await;

            if let Err(error) = result {
                let _ = channel_for_data_manifest_entry_error.send(Err(error)).await;
            }
        });

        // Collect all tasks from manifest processing.
        let all_tasks = file_scan_task_rx.try_collect::<Vec<_>>().await?;

        // Separate tasks by type and compute file path sets in a single pass
        let mut append_tasks = Vec::new();
        let mut delete_tasks = Vec::new();
        let mut appended_files = HashSet::new();
        let mut deleted_files = HashSet::new();

        for task in all_tasks {
            match task {
                IncrementalFileScanTask::Append(append_task) => {
                    appended_files.insert(append_task.data_file_path().to_string());
                    append_tasks.push(append_task);
                }
                IncrementalFileScanTask::Delete(delete_task) => {
                    deleted_files.insert(delete_task.data_file_path().to_string());
                    delete_tasks.push(delete_task);
                }
                _ => {}
            }
        }

        // Build final task list with net changes
        // We filter out tasks for files that appear in both sets (cancelled out)
        let mut final_tasks: Vec<IncrementalFileScanTask> = Vec::new();

        // Add net append tasks (only files not in deleted_files)
        for append_task in append_tasks {
            if !deleted_files.contains(append_task.data_file_path()) {
                final_tasks.push(IncrementalFileScanTask::Append(append_task));
            }
        }

        // Add net delete tasks (only files not in appended_files)
        for delete_task in delete_tasks {
            if !appended_files.contains(delete_task.data_file_path()) {
                final_tasks.push(IncrementalFileScanTask::Delete(delete_task));
            }
        }

        // Add positional delete tasks (only for files that haven't been deleted)
        let positional_delete_paths: Vec<String> = delete_filter.with_read(|state| {
            Ok(state
                .delete_vectors()
                .keys()
                .filter(|path| {
                    // Only include positional deletes for files that were not appended in
                    // this range and not deleted.
                    !appended_files.contains::<str>(path) && !deleted_files.contains::<str>(path)
                })
                .cloned()
                .collect())
        })?;

        // Now remove and take ownership of each delete vector
        for path in positional_delete_paths {
            let delete_vector_arc = delete_filter.with_write(|state| {
                state.remove_delete_vector(&path).ok_or_else(|| {
                    Error::new(
                        ErrorKind::Unexpected,
                        format!("DeleteVector for path {} not found", path),
                    )
                })
            })?;

            // Try to unwrap the Arc to avoid cloning the DeleteVector
            let delete_vector_inner = Arc::try_unwrap(delete_vector_arc)
                .map_err(|_| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "DeleteVector Arc has multiple references, cannot take ownership",
                    )
                })?
                .into_inner()
                .map_err(|e| {
                    Error::new(ErrorKind::Unexpected, "Failed to unwrap DeleteVector Mutex")
                        .with_source(e)
                })?;

            let positional_delete_task =
                IncrementalFileScanTask::PositionalDeletes(path, delete_vector_inner);
            final_tasks.push(positional_delete_task);
        }

        // We actually would not need a stream here, but we can keep it compatible with
        // other scan types.
        Ok(futures::stream::iter(final_tasks).map(Ok).boxed())
    }

    /// Returns an [`CombinedIncrementalBatchRecordStream`] for this incremental table scan.
    pub async fn to_arrow(&self) -> Result<CombinedIncrementalBatchRecordStream> {
        let mut arrow_reader_builder = ArrowReaderBuilder::new(self.file_io.clone())
            .with_data_file_concurrency_limit(self.concurrency_limit_data_files)
            .with_row_group_filtering_enabled(true)
            .with_row_selection_enabled(true);

        if let Some(batch_size) = self.batch_size {
            arrow_reader_builder = arrow_reader_builder.with_batch_size(batch_size);
        }

        let arrow_reader = arrow_reader_builder.build();
        let file_scan_task_stream = self.plan_files().await?;
        file_scan_task_stream.stream(arrow_reader)
    }

    /// Returns an [`UnzippedIncrementalBatchRecordStream`] for this incremental table scan.
    /// This stream will yield separate streams for appended and deleted record batches.
    pub async fn to_unzipped_arrow(&self) -> Result<UnzippedIncrementalBatchRecordStream> {
        let mut arrow_reader_builder = ArrowReaderBuilder::new(self.file_io.clone())
            .with_data_file_concurrency_limit(self.concurrency_limit_data_files)
            .with_row_group_filtering_enabled(true)
            .with_row_selection_enabled(true);

        if let Some(batch_size) = self.batch_size {
            arrow_reader_builder = arrow_reader_builder.with_batch_size(batch_size);
        }

        let arrow_reader = arrow_reader_builder.build();
        let file_scan_task_stream = self.plan_files().await?;
        file_scan_task_stream.stream(arrow_reader)
    }

    async fn process_delete_manifest_entry(
        mut delete_file_ctx_tx: Sender<DeleteFileContext>,
        manifest_entry_context: ManifestEntryContext,
    ) -> Result<()> {
        // Abort the plan if we encounter a manifest entry for a data file or equality
        // deletes.
        if manifest_entry_context.manifest_entry.content_type() == DataContentType::Data {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Encountered an entry for a data file in a delete file manifest",
            ));
        } else if manifest_entry_context.manifest_entry.content_type()
            == DataContentType::EqualityDeletes
        {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Equality deletes are not supported yet in incremental scans",
            ));
        }

        // Abort if it has been marked as deleted.
        if !manifest_entry_context.manifest_entry.is_alive()
            && manifest_entry_context.manifest_entry.content_type()
                == DataContentType::PositionDeletes
        {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Processing deleted (position) delete files is not supported yet in incremental scans",
            ));
        }

        delete_file_ctx_tx
            .send(DeleteFileContext {
                manifest_entry: manifest_entry_context.manifest_entry.clone(),
                partition_spec_id: manifest_entry_context.partition_spec_id,
            })
            .await?;
        Ok(())
    }

    async fn process_data_manifest_entry(
        mut file_scan_task_tx: Sender<Result<IncrementalFileScanTask>>,
        manifest_entry_context: ManifestEntryContext,
        delete_filter: &DeleteFilter,
    ) -> Result<()> {
        // Skip processing this manifest entry if it has been marked as deleted.
        if !manifest_entry_context.manifest_entry.is_alive() {
            return Ok(());
        }

        // Abort the plan if we encounter a manifest entry for a delete file
        if manifest_entry_context.manifest_entry.content_type() != DataContentType::Data {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Encountered an entry for a delete file in a data file manifest",
            ));
        }

        let file_scan_task = IncrementalFileScanTask::append_from_manifest_entry(
            &manifest_entry_context,
            delete_filter,
        );

        file_scan_task_tx.send(Ok(file_scan_task)).await?;
        Ok(())
    }

    async fn process_deleted_data_manifest_entry(
        mut file_scan_task_tx: Sender<Result<IncrementalFileScanTask>>,
        manifest_entry_context: ManifestEntryContext,
    ) -> Result<()> {
        // Abort the plan if we encounter a manifest entry for a delete file
        if manifest_entry_context.manifest_entry.content_type() != DataContentType::Data {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Encountered an entry for a delete file in a data file manifest",
            ));
        }

        let data_file_path = manifest_entry_context.manifest_entry.file_path();
        let file_scan_task = IncrementalFileScanTask::Delete(DeletedFileScanTask {
            base: BaseIncrementalFileScanTask {
                start: 0,
                length: manifest_entry_context.manifest_entry.file_size_in_bytes(),
                record_count: Some(manifest_entry_context.manifest_entry.record_count()),
                data_file_path: data_file_path.to_string(),
                data_file_format: manifest_entry_context.manifest_entry.file_format(),
                schema: manifest_entry_context.snapshot_schema.clone(),
                project_field_ids: manifest_entry_context.field_ids.as_ref().clone(),
            },
        });

        file_scan_task_tx.send(Ok(file_scan_task)).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
