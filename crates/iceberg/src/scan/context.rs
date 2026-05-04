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

use futures::StreamExt;
use futures::stream::BoxStream;

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
use crate::{Error, ErrorKind, Result};

type ManifestEntryFilterFn = dyn Fn(&ManifestEntryRef) -> bool + Send + Sync;

/// Wraps a [`ManifestFile`] alongside the objects that are needed
/// to process it in a thread-safe manner
pub(crate) struct ManifestFileContext {
    manifest_file: ManifestFile,
    field_ids: Arc<Vec<i32>>,
    bound_predicates: Option<Arc<BoundPredicates>>,
    object_cache: Arc<ObjectCache>,
    snapshot_schema: SchemaRef,
    expression_evaluator_cache: Arc<ExpressionEvaluatorCache>,
    case_sensitive: bool,
    /// Filter manifest entries (e.g., only newly added files for incremental scans).
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
    pub case_sensitive: bool,
}

impl ManifestFileContext {
    /// Consumes this [`ManifestFileContext`], fetching its Manifest from FileIO and then
    /// streaming its constituent [`ManifestEntries`]
    pub(crate) async fn fetch_manifest_and_stream_entries(
        self,
    ) -> Result<BoxStream<'static, Result<ManifestEntryContext>>> {
        let ManifestFileContext {
            object_cache,
            manifest_file,
            bound_predicates,
            snapshot_schema,
            field_ids,
            expression_evaluator_cache,
            case_sensitive,
            filter_fn,
        } = self;
        let filter_fn = filter_fn.unwrap_or_else(|| Arc::new(|_| true));

        let manifest = object_cache.get_manifest(&manifest_file).await?;

        Ok(async_stream::stream! {
            for manifest_entry in manifest.entries().iter().filter(|e| filter_fn(e)) {
                yield Ok(ManifestEntryContext {
                    manifest_entry: manifest_entry.clone(),
                    expression_evaluator_cache: expression_evaluator_cache.clone(),
                    field_ids: field_ids.clone(),
                    partition_spec_id: manifest_file.partition_spec_id,
                    bound_predicates: bound_predicates.clone(),
                    snapshot_schema: snapshot_schema.clone(),
                    case_sensitive,
                });
            }
        }
        .boxed())
    }

    pub(crate) fn is_delete(&self) -> bool {
        self.manifest_file.content == ManifestContentType::Deletes
    }
}

impl ManifestEntryContext {
    /// consume this `ManifestEntryContext`, returning a `FileScanTask`
    /// created from it
    pub(crate) fn into_file_scan_task(
        self,
        delete_file_index: Arc<DeleteFileIndex>,
    ) -> Result<FileScanTask> {
        let deletes = delete_file_index.get_deletes_for_data_file(
            self.manifest_entry.data_file(),
            self.manifest_entry.sequence_number(),
        );

        Ok(FileScanTask {
            file_size_in_bytes: self.manifest_entry.file_size_in_bytes(),
            start: 0,
            length: self.manifest_entry.file_size_in_bytes(),
            record_count: Some(self.manifest_entry.record_count()),

            data_file_path: self.manifest_entry.file_path().to_string(),
            data_file_format: self.manifest_entry.file_format(),

            schema: self.snapshot_schema,
            project_field_ids: self.field_ids.to_vec(),
            predicate: self
                .bound_predicates
                .map(|x| x.as_ref().snapshot_bound_predicate.clone()),

            deletes,

            // Include partition data and spec from manifest entry
            partition: Some(self.manifest_entry.data_file.partition.clone()),
            // TODO: Pass actual PartitionSpec through context chain for native flow
            partition_spec: None,
            // TODO: Extract name_mapping from table metadata property "schema.name-mapping.default"
            name_mapping: None,
            column_sizes: Some(self.manifest_entry.data_file().column_sizes().clone()),
            split_offsets: self
                .manifest_entry
                .data_file()
                .split_offsets()
                .map(|s| s.to_vec()),
            case_sensitive: self.case_sensitive,
        })
    }
}

/// PlanContext wraps a [`SnapshotRef`] alongside all the other
/// objects that are required to perform a scan file plan.
#[derive(Clone, Debug)]
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

    /// Exclusive start snapshot for incremental scan. `None` means scan from the beginning.
    pub from_snapshot_id: Option<i64>,
    /// Inclusive end snapshot for incremental scan. When set, enables incremental scan mode.
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

    pub(crate) fn build_manifest_file_context_iter(
        &self,
        manifest_list: Arc<ManifestList>,
    ) -> impl Iterator<Item = Result<ManifestFileContext>> {
        let has_predicate = self.predicate.is_some();

        (0..manifest_list.entries().len())
            .map(move |i| manifest_list.entries()[i].clone())
            .filter_map(move |manifest_file| {
                // TODO: replace closure when `try_blocks` stabilizes
                (|| {
                    let partition_bound_predicate = if has_predicate {
                        let predicate = self.get_partition_filter(&manifest_file)?;

                        if !self
                            .manifest_evaluator_cache
                            .get(manifest_file.partition_spec_id, predicate.clone())
                            .eval(&manifest_file)?
                        {
                            return Ok(None); // Skip this file.
                        }
                        Some(predicate)
                    } else {
                        None
                    };

                    let context = self.create_manifest_file_context(
                        manifest_file,
                        partition_bound_predicate,
                        None,
                    )?;

                    Ok(Some(context))
                })()
                .transpose()
            })
    }

    fn create_manifest_file_context(
        &self,
        manifest_file: ManifestFile,
        partition_filter: Option<Arc<BoundPredicate>>,
        filter_fn: Option<Arc<ManifestEntryFilterFn>>,
    ) -> Result<ManifestFileContext> {
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

        Ok(ManifestFileContext {
            manifest_file,
            bound_predicates,
            object_cache: self.object_cache.clone(),
            snapshot_schema: self.snapshot_schema.clone(),
            field_ids: self.field_ids.clone(),
            expression_evaluator_cache: self.expression_evaluator_cache.clone(),
            case_sensitive: self.case_sensitive,
            filter_fn,
        })
    }

    /// Build manifest file contexts for an incremental scan.
    /// Only returns manifest files from snapshots between `from_snapshot_id`
    /// (exclusive) and `to_snapshot_id` (inclusive), filtering entries to only
    /// newly added data files.
    ///
    /// - Data files in `Append` and `Overwrite` snapshots are included.
    /// - Delete files are ignored.
    /// - `Replace` snapshots (e.g., compaction) are skipped.
    pub(crate) async fn build_manifest_file_contexts_for_incremental_scan(
        &self,
        from_snapshot_id: Option<i64>,
        to_snapshot_id: i64,
    ) -> Result<Vec<Result<ManifestFileContext>>> {
        let snapshots: Vec<SnapshotRef> =
            ancestors_between(&self.table_metadata, to_snapshot_id, from_snapshot_id)
                .filter(|snapshot| {
                    matches!(
                        snapshot.summary().operation,
                        Operation::Append | Operation::Overwrite
                    )
                })
                .collect();

        let snapshot_ids: HashSet<i64> = snapshots.iter().map(|s| s.snapshot_id()).collect();

        // Build a filter that only passes newly-added data file entries
        // whose snapshot_id is in the range
        let snapshot_ids_for_filter = snapshot_ids.clone();
        let filter_fn: Arc<ManifestEntryFilterFn> = Arc::new(move |entry: &ManifestEntryRef| {
            matches!(entry.status(), ManifestStatus::Added)
                && matches!(entry.data_file().content_type(), DataContentType::Data)
                && entry
                    .snapshot_id()
                    .is_none_or(|id| snapshot_ids_for_filter.contains(&id))
        });

        let has_predicate = self.predicate.is_some();
        let mut contexts = Vec::new();

        for snapshot in &snapshots {
            let manifest_list = self
                .object_cache
                .get_manifest_list(snapshot, &self.table_metadata)
                .await?;

            for manifest_file in manifest_list.entries() {
                // Only include manifests that were written by one of the snapshots in our range
                if !snapshot_ids.contains(&manifest_file.added_snapshot_id) {
                    continue;
                }

                if manifest_file.content == ManifestContentType::Deletes {
                    return Err(Error::new(
                        ErrorKind::FeatureUnsupported,
                        format!(
                            "Incremental scan does not support delete files. \
                             Snapshot {} contains a delete manifest.",
                            manifest_file.added_snapshot_id
                        ),
                    ));
                }

                // Apply partition filter if predicate exists
                let partition_bound_predicate = if has_predicate {
                    let predicate = self.get_partition_filter(manifest_file)?;
                    if !self
                        .manifest_evaluator_cache
                        .get(manifest_file.partition_spec_id, predicate.clone())
                        .eval(manifest_file)?
                    {
                        continue; // Skip this manifest
                    }
                    Some(predicate)
                } else {
                    None
                };

                let context = self.create_manifest_file_context(
                    manifest_file.clone(),
                    partition_bound_predicate,
                    Some(filter_fn.clone()),
                )?;
                contexts.push(Ok(context));
            }
        }

        Ok(contexts)
    }
}

struct Ancestors {
    next: Option<SnapshotRef>,
    get_snapshot: Box<dyn Fn(i64) -> Option<SnapshotRef> + Send>,
}

impl Iterator for Ancestors {
    type Item = SnapshotRef;

    fn next(&mut self) -> Option<Self::Item> {
        let snapshot = self.next.take()?;
        let result = snapshot.clone();
        self.next = snapshot
            .parent_snapshot_id()
            .and_then(|id| (self.get_snapshot)(id));
        Some(result)
    }
}

/// Iterate starting from `latest_snapshot_id` (inclusive) to `oldest_snapshot_id` (exclusive).
/// If `oldest_snapshot_id` is `None`, iterates to the root snapshot.
fn ancestors_between(
    table_metadata: &TableMetadataRef,
    latest_snapshot_id: i64,
    oldest_snapshot_id: Option<i64>,
) -> Box<dyn Iterator<Item = SnapshotRef> + Send> {
    if oldest_snapshot_id == Some(latest_snapshot_id) {
        return Box::new(std::iter::empty());
    }

    let Some(snapshot) = table_metadata.snapshot_by_id(latest_snapshot_id) else {
        return Box::new(std::iter::empty());
    };

    let table_metadata = table_metadata.clone();
    let ancestors = Ancestors {
        next: Some(snapshot.clone()),
        get_snapshot: Box::new(move |id| table_metadata.snapshot_by_id(id).cloned()),
    };

    match oldest_snapshot_id {
        Some(oldest) => Box::new(ancestors.take_while(move |s| s.snapshot_id() != oldest)),
        None => Box::new(ancestors),
    }
}
