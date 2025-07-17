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

use std::collections::HashSet;
use std::sync::Arc;

use futures::channel::mpsc::Sender;
use futures::{SinkExt, TryFutureExt};
use itertools::Itertools;

use crate::delete_file_index::DeleteFileIndex;
use crate::expr::{Bind, BoundPredicate, Predicate};
use crate::io::object_cache::ObjectCache;
use crate::scan::{
    BoundPredicates, ExpressionEvaluatorCache, FileScanTask, ManifestEvaluatorCache,
    PartitionFilterCache,
};
use crate::spec::{
    DataContentType, ManifestContentType, ManifestEntryRef, ManifestFile, ManifestList,
    ManifestStatus, Operation, SchemaRef, SnapshotRef, TableMetadataRef,
};
use crate::utils::ancestors_between;
use crate::{Error, ErrorKind, Result};

type ManifestEntryFilterFn = dyn Fn(&ManifestEntryRef) -> bool + Send + Sync;
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
    delete_file_index: Option<DeleteFileIndex>,

    /// filter manifest entries.
    /// Used for different kind of scans, e.g., only scan newly added files without delete files.
    filter_fn: Option<Arc<ManifestEntryFilterFn>>,
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
    pub delete_file_index: Option<DeleteFileIndex>,
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
            filter_fn,
        } = self;
        let filter_fn = filter_fn.unwrap_or_else(|| Arc::new(|_| true));

        let manifest = object_cache.get_manifest(&manifest_file).await?;

        for manifest_entry in manifest.entries().iter().filter(|e| filter_fn(e)) {
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
    pub(crate) async fn into_file_scan_task(self) -> Result<FileScanTask> {
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
            sequence_number: self.manifest_entry.sequence_number().unwrap_or(0),
            equality_ids: self.manifest_entry.data_file().equality_ids().to_vec(),
            file_size_in_bytes: self.manifest_entry.data_file().file_size_in_bytes(),
        })
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

    // for incremental scan.
    // If `to_snapshot_id` is set, it means incremental scan. `from_snapshot_id` can be `None`.
    pub from_snapshot_id: Option<i64>,
    pub to_snapshot_id: Option<i64>,
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

    pub(crate) async fn build_manifest_file_contexts(
        &self,
        tx_data: Sender<ManifestEntryContext>,
        delete_file_idx_and_tx: Option<(DeleteFileIndex, Sender<ManifestEntryContext>)>,
    ) -> Result<Box<impl Iterator<Item = Result<ManifestFileContext>> + 'static>> {
        let mut filter_fn: Option<Arc<ManifestEntryFilterFn>> = None;
        let manifest_files = {
            if let Some(to_snapshot_id) = self.to_snapshot_id {
                // Incremental scan mode:
                // Get all added files between two snapshots.
                // - data files in `Append` and `Overwrite` snapshots are included.
                // - delete files are ignored
                // - `Replace` snapshots (e.g., compaction) are ignored.
                //
                // `latest_snapshot_id` is inclusive, `oldest_snapshot_id` is exclusive.

                // prevent misuse
                assert!(
                    delete_file_idx_and_tx.is_none(),
                    "delete file is not supported in incremental scan mode"
                );

                let snapshots =
                    ancestors_between(&self.table_metadata, to_snapshot_id, self.from_snapshot_id)
                        .filter(|snapshot| {
                            matches!(
                                snapshot.summary().operation,
                                Operation::Append | Operation::Overwrite
                            )
                        })
                        .collect_vec();
                let snapshot_ids: HashSet<i64> = snapshots
                    .iter()
                    .map(|snapshot| snapshot.snapshot_id())
                    .collect();

                let mut manifest_files = vec![];
                for snapshot in snapshots {
                    let manifest_list = self
                        .object_cache
                        .get_manifest_list(&snapshot, &self.table_metadata)
                        .await?;
                    for entry in manifest_list.entries() {
                        if !snapshot_ids.contains(&entry.added_snapshot_id) {
                            continue;
                        }
                        manifest_files.push(entry.clone());
                    }
                }

                filter_fn = Some(Arc::new(move |entry: &ManifestEntryRef| {
                    matches!(entry.status(), ManifestStatus::Added)
                        && matches!(entry.data_file().content_type(), DataContentType::Data)
                        && (
                            // Is it possible that the snapshot id here is not contained?
                            entry.snapshot_id().is_none()
                                || snapshot_ids.contains(&entry.snapshot_id().unwrap())
                        )
                }));

                manifest_files
            } else {
                let manifest_list = self.get_manifest_list().await?;
                manifest_list.entries().to_vec()
            }
        };

        // TODO: Ideally we could ditch this intermediate Vec as we return an iterator.
        let mut filtered_deletes_mfcs = vec![];
        let mut filtered_data_mfcs = vec![];

        for manifest_file in &manifest_files {
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
                filter_fn.clone(),
            );

            match manifest_file.content {
                ManifestContentType::Deletes => {
                    filtered_deletes_mfcs.push(Ok(mfc));
                }
                ManifestContentType::Data => {
                    filtered_data_mfcs.push(Ok(mfc));
                }
            }
        }

        // Push deletes manifest first then data manifest files.
        Ok(Box::new(
            filtered_deletes_mfcs
                .into_iter()
                .chain(filtered_data_mfcs.into_iter()),
        ))
    }

    fn create_manifest_file_context(
        &self,
        manifest_file: &ManifestFile,
        partition_filter: Option<Arc<BoundPredicate>>,
        sender: Sender<ManifestEntryContext>,
        delete_file_index: Option<DeleteFileIndex>,
        filter_fn: Option<Arc<ManifestEntryFilterFn>>,
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
            filter_fn,
        }
    }
}
