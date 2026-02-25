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

use std::sync::Arc;

use futures::channel::mpsc::Sender;
use futures::{SinkExt, TryFutureExt};

use crate::delete_file_index::DeleteFileIndex;
use crate::expr::{Bind, BoundPredicate, Predicate};
use crate::io::object_cache::ObjectCache;
use crate::scan::{
    BoundPredicates, ExpressionEvaluatorCache, FileScanTask, FileScanTaskDeleteFile,
    ManifestEvaluatorCache, PartitionFilterCache,
};
use crate::spec::{
    ManifestContentType, ManifestEntryRef, ManifestFile, ManifestList, SchemaRef, SnapshotRef,
    TableMetadataRef,
};
use crate::{Error, ErrorKind, Result};

/// Wraps a [`ManifestFile`] alongside the objects that are needed
/// to process it in a thread-safe manner
pub(crate) struct ManifestFileContext {
    manifest_file: ManifestFile,

    sender: Sender<ManifestEntryContext>,

    field_ids: Arc<Vec<i32>>,
    bound_predicates: Option<Arc<BoundPredicates>>,
    object_cache: Arc<ObjectCache>,
    snapshot_schema: SchemaRef,
    expression_evaluator_cache: Arc<ExpressionEvaluatorCache>,
    delete_file_index: DeleteFileIndex,
    case_sensitive: bool,
    /// Target size in bytes for splitting large files (0 = disabled)
    split_target_size: u64,
}

/// Wraps a [`ManifestEntryRef`] alongside the objects that are needed
/// to process it in a thread-safe manner
pub(crate) struct ManifestEntryContext {
    pub manifest_entry: ManifestEntryRef,

    pub expression_evaluator_cache: Arc<ExpressionEvaluatorCache>,
    pub field_ids: Arc<Vec<i32>>,
    pub bound_predicates: Option<Arc<BoundPredicates>>,
    pub partition_spec_id: i32,
    pub snapshot_schema: SchemaRef,
    pub delete_file_index: DeleteFileIndex,
    pub case_sensitive: bool,
    /// Target size in bytes for splitting large files (0 = disabled)
    pub split_target_size: u64,
}

impl ManifestFileContext {
    /// Consumes this [`ManifestFileContext`], fetching its Manifest from FileIO and then
    /// streaming its constituent [`ManifestEntries`] to the channel provided in the context
    pub(crate) async fn fetch_manifest_and_stream_manifest_entries(self) -> Result<()> {
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
                case_sensitive: self.case_sensitive,
                split_target_size: self.split_target_size,
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
    /// Consume this `ManifestEntryContext`, returning one or more `FileScanTask`s.
    ///
    /// If `split_target_size` is set and the file is larger than that threshold,
    /// the file will be split into multiple tasks. Each split task will read a
    /// portion of the file using byte-range filtering.
    ///
    /// If the file has `split_offsets` metadata (e.g., Parquet row group boundaries),
    /// splits will align with those offsets for optimal performance.
    pub(crate) async fn into_file_scan_tasks(self) -> Result<Vec<FileScanTask>> {
        let file_size = self.manifest_entry.file_size_in_bytes();
        let record_count = self.manifest_entry.record_count();
        let split_target_size = self.split_target_size;

        // Get delete files (shared across all splits from the same data file)
        let deletes = self
            .delete_file_index
            .get_deletes_for_data_file(
                self.manifest_entry.data_file(),
                self.manifest_entry.sequence_number(),
            )
            .await;

        // If splitting is disabled or file is small enough, return a single task
        if split_target_size == 0 || file_size <= split_target_size {
            return Ok(vec![self.create_file_scan_task(
                0,
                file_size,
                Some(record_count),
                deletes,
            )]);
        }

        // Try to split using split_offsets if available (aligns with row group boundaries)
        if let Some(offsets) = self.manifest_entry.data_file().split_offsets()
            && !offsets.is_empty()
        {
            return Ok(self.split_by_offsets(offsets, file_size, deletes));
        }

        // Fall back to splitting by byte ranges
        Ok(self.split_by_size(file_size, split_target_size, deletes))
    }

    /// Split the file using pre-computed offsets (e.g., Parquet row group boundaries).
    /// This produces optimal splits that align with the file's internal structure.
    fn split_by_offsets(
        &self,
        offsets: &[i64],
        file_size: u64,
        mut deletes: Vec<FileScanTaskDeleteFile>,
    ) -> Vec<FileScanTask> {
        // Pre-compute valid splits to know total count
        let splits: Vec<(u64, u64)> = offsets
            .iter()
            .enumerate()
            .filter_map(|(i, &offset)| {
                let start = offset as u64;
                let length = if i + 1 < offsets.len() {
                    (offsets[i + 1] - offset) as u64
                } else {
                    file_size.saturating_sub(start)
                };
                if length > 0 {
                    Some((start, length))
                } else {
                    None
                }
            })
            .collect();

        let num_splits = splits.len();
        if num_splits == 0 {
            return vec![];
        }

        let mut tasks = Vec::with_capacity(num_splits);

        for (i, (start, length)) in splits.into_iter().enumerate() {
            // Last split takes ownership, others clone
            let task_deletes = if i == num_splits - 1 {
                std::mem::take(&mut deletes)
            } else {
                deletes.clone()
            };
            tasks.push(self.create_file_scan_task(start, length, None, task_deletes));
        }

        tasks
    }

    /// Split the file into chunks of approximately `target_size` bytes.
    /// This is used when split_offsets are not available.
    fn split_by_size(
        &self,
        file_size: u64,
        target_size: u64,
        mut deletes: Vec<FileScanTaskDeleteFile>,
    ) -> Vec<FileScanTask> {
        let num_splits = file_size.div_ceil(target_size) as usize;
        if num_splits == 0 {
            return vec![];
        }

        let mut tasks = Vec::with_capacity(num_splits);
        let mut offset = 0u64;
        let mut split_idx = 0usize;

        while offset < file_size {
            let length = std::cmp::min(target_size, file_size - offset);
            // Last split takes ownership, others clone
            let task_deletes = if split_idx == num_splits - 1 {
                std::mem::take(&mut deletes)
            } else {
                deletes.clone()
            };
            tasks.push(self.create_file_scan_task(offset, length, None, task_deletes));
            offset += length;
            split_idx += 1;
        }

        tasks
    }

    /// Create a single FileScanTask with the given byte range.
    fn create_file_scan_task(
        &self,
        start: u64,
        length: u64,
        record_count: Option<u64>,
        deletes: Vec<FileScanTaskDeleteFile>,
    ) -> FileScanTask {
        FileScanTask {
            start,
            length,
            record_count,

            data_file_path: self.manifest_entry.file_path().to_string(),
            data_file_format: self.manifest_entry.file_format(),

            schema: self.snapshot_schema.clone(),
            project_field_ids: self.field_ids.to_vec(),
            predicate: self
                .bound_predicates
                .as_ref()
                .map(|x| x.as_ref().snapshot_bound_predicate.clone()),

            deletes,

            // Include partition data and spec from manifest entry
            partition: Some(self.manifest_entry.data_file.partition.clone()),
            // TODO: Pass actual PartitionSpec through context chain for native flow
            partition_spec: None,
            // TODO: Extract name_mapping from table metadata property "schema.name-mapping.default"
            name_mapping: None,
            case_sensitive: self.case_sensitive,
        }
    }
}

/// PlanContext wraps a [`SnapshotRef`] alongside all the other
/// objects that are required to perform a scan file plan.
#[derive(Debug)]
pub(crate) struct PlanContext {
    pub snapshot: SnapshotRef,

    pub table_metadata: TableMetadataRef,
    pub snapshot_schema: SchemaRef,
    pub case_sensitive: bool,
    pub predicate: Option<Arc<Predicate>>,
    pub snapshot_bound_predicate: Option<Arc<BoundPredicate>>,
    pub object_cache: Arc<ObjectCache>,
    pub field_ids: Arc<Vec<i32>>,

    pub partition_filter_cache: Arc<PartitionFilterCache>,
    pub manifest_evaluator_cache: Arc<ManifestEvaluatorCache>,
    pub expression_evaluator_cache: Arc<ExpressionEvaluatorCache>,

    /// Target size in bytes for splitting large files (0 = disabled)
    pub split_target_size: u64,
}

impl PlanContext {
    pub(crate) async fn get_manifest_list(&self) -> Result<Arc<ManifestList>> {
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

    pub(crate) fn build_manifest_file_contexts(
        &self,
        manifest_list: Arc<ManifestList>,
        tx_data: Sender<ManifestEntryContext>,
        delete_file_idx: DeleteFileIndex,
        delete_file_tx: Sender<ManifestEntryContext>,
    ) -> Result<Box<impl Iterator<Item = Result<ManifestFileContext>> + 'static>> {
        let mut manifest_files = manifest_list.entries().iter().collect::<Vec<_>>();
        // Sort manifest files to process delete manifests first.
        // This avoids a deadlock where the producer blocks on sending data manifest entries
        // (because the data channel is full) while the delete manifest consumer is waiting
        // for delete manifest entries (which haven't been produced yet).
        // By processing delete manifests first, we ensure the delete consumer can finish,
        // which then allows the data consumer to start draining the data channel.
        manifest_files.sort_by_key(|m| match m.content {
            ManifestContentType::Deletes => 0,
            ManifestContentType::Data => 1,
        });

        // TODO: Ideally we could ditch this intermediate Vec as we return an iterator.
        let mut filtered_mfcs = vec![];
        for manifest_file in manifest_files {
            let tx = if manifest_file.content == ManifestContentType::Deletes {
                delete_file_tx.clone()
            } else {
                tx_data.clone()
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
                delete_file_idx.clone(),
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
        delete_file_index: DeleteFileIndex,
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
            case_sensitive: self.case_sensitive,
            split_target_size: self.split_target_size,
        }
    }
}
