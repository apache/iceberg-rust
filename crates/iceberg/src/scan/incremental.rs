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

//! Incremental scans.
//!
//! Two incremental / change-data read primitives over the range
//! `(from_snapshot_id exclusive, to_snapshot_id inclusive]`, both SEPARATE planners from
//! the single-snapshot [`TableScan`](super::TableScan) (they do not touch its
//! [`plan_files`](super::TableScan::plan_files)) that REUSE the existing [`PlanContext`]
//! / [`ManifestEntryContext`] machinery (partition-filter pruning, the residual
//! evaluator, and `into_file_scan_task`):
//!
//! - [`IncrementalAppendScan`] returns the data files APPENDED in the range, considering
//!   ONLY `APPEND`-operation snapshots. Overwrites and deletes in the range are excluded
//!   and no delete files are applied. Mirrors Java `BaseIncrementalAppendScan`
//!   (`core/src/main/java/org/apache/iceberg/BaseIncrementalAppendScan.java`):
//!   `doPlanFiles` → `appendsBetween` (the APPEND snapshots in the range, via
//!   `SnapshotUtil.ancestorsBetween` filtered to `operation == APPEND`) →
//!   `appendFilesFromSnapshots` (plan tasks for the data files those snapshots ADDED:
//!   only the `Added` entries of the manifests each snapshot itself added, where
//!   `manifest.snapshotId() == snapshot.snapshotId()`).
//!
//! - [`IncrementalChangelogScan`] returns row-level CHANGE tasks ([`ChangelogScanTask`]):
//!   an INSERT task per data file ADDED and a DELETE task per data file REMOVED by the
//!   snapshots in the range, each tagged with a change ordinal (oldest snapshot → 0) and
//!   its commit snapshot id. `Operation::Replace` snapshots (e.g. compaction) are
//!   excluded — they rewrite files without changing rows. Mirrors Java
//!   `BaseIncrementalChangelogScan`
//!   (`core/src/main/java/org/apache/iceberg/BaseIncrementalChangelogScan.java`).
//!   Like Java's current data-file changelog, a range whose snapshots carry row-level
//!   DELETE manifests is rejected (`FeatureUnsupported`) — only whole-file changes are
//!   supported.

use std::sync::Arc;

use futures::channel::mpsc::{Sender, channel};
use futures::{SinkExt, StreamExt, TryStreamExt};

use super::context::{ManifestEntryContext, PlanContext};
use crate::delete_file_index::DeleteFileIndex;
use crate::expr::{Bind, Predicate};
use crate::io::FileIO;
use crate::metadata_columns::{get_metadata_field_id, is_metadata_column_name};
use crate::runtime::spawn;
use crate::scan::{
    BoundPredicates, ChangelogOperation, ChangelogScanTask, ChangelogScanTaskStream,
    ExpressionEvaluatorCache, FileScanTask, FileScanTaskStream, ManifestEvaluatorCache,
    PartitionFilterCache,
};
use crate::spec::{
    DataContentType, ManifestContentType, ManifestStatus, Operation, SchemaRef, SnapshotRef,
};
use crate::table::Table;
use crate::utils::available_parallelism;
use crate::{Error, ErrorKind, Result};

/// Builder to create an [`IncrementalAppendScan`].
///
/// Mirrors the Java `IncrementalAppendScan` API (`api/IncrementalAppendScan.java` +
/// `BaseIncrementalScan`): the `from` snapshot can be set as exclusive
/// ([`Self::from_snapshot_id_exclusive`], Java `fromSnapshotExclusive`) or inclusive
/// ([`Self::from_snapshot_id_inclusive`], Java `fromSnapshotInclusive`); the `to`
/// snapshot ([`Self::to_snapshot_id`], Java `toSnapshot`) defaults to the table's
/// current snapshot when unset. The builder ergonomics (`with_filter`, `select`,
/// concurrency limits) mirror [`TableScanBuilder`](super::TableScanBuilder).
pub struct IncrementalAppendScanBuilder<'a> {
    table: &'a Table,
    column_names: Option<Vec<String>>,
    /// The exclusive lower bound of the range — the parent below which appends are
    /// counted. `None` means "from the beginning of history" (every ancestor of `to`).
    from_snapshot_id_exclusive: Option<i64>,
    /// When the caller set the `from` bound as INCLUSIVE, this holds that snapshot id.
    /// Resolution at `build()` time converts it to the exclusive bound (the snapshot's
    /// parent), mirroring Java `fromSnapshotIdExclusive` for the inclusive case.
    from_snapshot_id_inclusive: Option<i64>,
    to_snapshot_id: Option<i64>,
    batch_size: Option<usize>,
    case_sensitive: bool,
    filter: Option<Predicate>,
    concurrency_limit_manifest_entries: usize,
    concurrency_limit_manifest_files: usize,
}

impl<'a> IncrementalAppendScanBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        let num_cpus = available_parallelism().get();

        Self {
            table,
            column_names: None,
            from_snapshot_id_exclusive: None,
            from_snapshot_id_inclusive: None,
            to_snapshot_id: None,
            batch_size: None,
            case_sensitive: true,
            filter: None,
            concurrency_limit_manifest_entries: num_cpus,
            concurrency_limit_manifest_files: num_cpus,
        }
    }

    /// Sets the EXCLUSIVE `from` snapshot id (Java `fromSnapshotExclusive`): appends in
    /// `(from, to]` are returned — `from`'s own files are NOT included. Supersedes any
    /// previously-set inclusive bound.
    pub fn from_snapshot_id_exclusive(mut self, from_snapshot_id: i64) -> Self {
        self.from_snapshot_id_exclusive = Some(from_snapshot_id);
        self.from_snapshot_id_inclusive = None;
        self
    }

    /// Sets the INCLUSIVE `from` snapshot id (Java `fromSnapshotInclusive`): appends in
    /// `[from, to]` are returned — `from`'s own files ARE included (provided `from` is an
    /// APPEND snapshot). Resolved to the exclusive bound (`from`'s parent) at `build()`.
    /// Supersedes any previously-set exclusive bound.
    pub fn from_snapshot_id_inclusive(mut self, from_snapshot_id: i64) -> Self {
        self.from_snapshot_id_inclusive = Some(from_snapshot_id);
        self.from_snapshot_id_exclusive = None;
        self
    }

    /// Sets the INCLUSIVE `to` snapshot id (Java `toSnapshot`). When unset, defaults to
    /// the table's current snapshot.
    pub fn to_snapshot_id(mut self, to_snapshot_id: i64) -> Self {
        self.to_snapshot_id = Some(to_snapshot_id);
        self
    }

    /// Sets the desired size of batches in the response to something other than the default.
    pub fn with_batch_size(mut self, batch_size: Option<usize>) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Sets the scan's case sensitivity.
    pub fn with_case_sensitive(mut self, case_sensitive: bool) -> Self {
        self.case_sensitive = case_sensitive;
        self
    }

    /// Specifies a predicate to use as a filter.
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

    /// Sets the concurrency limit for both manifest files and manifest entries.
    pub fn with_concurrency_limit(mut self, limit: usize) -> Self {
        self.concurrency_limit_manifest_files = limit;
        self.concurrency_limit_manifest_entries = limit;
        self
    }

    /// Build the incremental append scan.
    ///
    /// Validates that the `to` snapshot exists (defaulting to the current snapshot),
    /// that an explicit `from` snapshot exists, and that `from` is an ancestor of `to`
    /// (Java requires `from` be an ancestor of `to`). Resolves the schema, projected
    /// field ids, and bound filter exactly as the normal scan does.
    pub fn build(self) -> Result<IncrementalAppendScan> {
        let metadata = self.table.metadata();

        // Resolve the inclusive `to` snapshot (Java `toSnapshotIdInclusive()`): explicit
        // id if set, else the current snapshot. With no current snapshot and no explicit
        // `to`, the scan is empty.
        let to_snapshot = match self.to_snapshot_id {
            Some(to_snapshot_id) => metadata.snapshot_by_id(to_snapshot_id).ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Cannot find the end snapshot: {to_snapshot_id}"),
                )
            })?,
            None => {
                let Some(current_snapshot) = metadata.current_snapshot() else {
                    return Ok(IncrementalAppendScan {
                        from_snapshot_id_exclusive: None,
                        to_snapshot_id: None,
                        plan_context: None,
                        column_names: self.column_names,
                        batch_size: self.batch_size,
                        file_io: self.table.file_io().clone(),
                        concurrency_limit_manifest_entries: self.concurrency_limit_manifest_entries,
                        concurrency_limit_manifest_files: self.concurrency_limit_manifest_files,
                    });
                };
                current_snapshot
            }
        }
        .clone();

        let to_snapshot_id = to_snapshot.snapshot_id();

        // Resolve the EXCLUSIVE `from` bound (Java `fromSnapshotIdExclusive(toInclusive)`).
        //
        // - inclusive `from`: validate `from` is an ancestor of `to`, then the exclusive
        //   bound is `from`'s parent (which may be `None` = the whole history up to and
        //   including `from`).
        // - exclusive `from`: validate `from` is a *parent ancestor* of `to` (some
        //   ancestor of `to` has `parent_id == from`), then the exclusive bound is `from`.
        // - neither set: `None` = scan the whole current lineage.
        let from_snapshot_id_exclusive = if let Some(from_inclusive) =
            self.from_snapshot_id_inclusive
        {
            let from_snapshot = metadata.snapshot_by_id(from_inclusive).ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Cannot find the starting snapshot: {from_inclusive}"),
                )
            })?;
            if !is_ancestor_of(self.table, to_snapshot_id, from_inclusive) {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Starting snapshot (inclusive) {from_inclusive} is not an ancestor of end snapshot {to_snapshot_id}"
                    ),
                ));
            }
            from_snapshot.parent_snapshot_id()
        } else if let Some(from_exclusive) = self.from_snapshot_id_exclusive {
            if !is_parent_ancestor_of(self.table, to_snapshot_id, from_exclusive) {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Starting snapshot (exclusive) {from_exclusive} is not a parent ancestor of end snapshot {to_snapshot_id}"
                    ),
                ));
            }
            Some(from_exclusive)
        } else {
            None
        };

        // Resolve schema, projected field ids, and the bound filter from the `to`
        // snapshot — mirroring `TableScanBuilder::build`.
        let schema = to_snapshot.schema(metadata)?;

        if let Some(column_names) = self.column_names.as_ref() {
            for column_name in column_names {
                if is_metadata_column_name(column_name) {
                    continue;
                }
                if schema.field_by_name(column_name).is_none() {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Column {column_name} not found in table. Schema: {schema}"),
                    ));
                }
            }
        }

        let column_names = self.column_names.clone().unwrap_or_else(|| {
            schema
                .as_struct()
                .fields()
                .iter()
                .map(|f| f.name.clone())
                .collect()
        });

        let mut field_ids = vec![];
        for column_name in column_names.iter() {
            if is_metadata_column_name(column_name) {
                field_ids.push(get_metadata_field_id(column_name)?);
                continue;
            }

            let field_id = schema.field_id_by_name(column_name).ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Column {column_name} not found in table. Schema: {schema}"),
                )
            })?;

            schema
                .as_struct()
                .field_by_id(field_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::FeatureUnsupported,
                        format!(
                            "Column {column_name} is not a direct child of schema but a nested field, which is not supported now. Schema: {schema}"
                        ),
                    )
                })?;

            field_ids.push(field_id);
        }

        let snapshot_bound_predicate = if let Some(ref predicate) = self.filter {
            Some(predicate.bind(schema.clone(), true)?)
        } else {
            None
        };

        let plan_context = PlanContext {
            // The PlanContext's `snapshot` is the `to` snapshot, used only for its
            // schema and as the metadata anchor. The incremental planner does NOT call
            // `PlanContext::get_manifest_list` (which would read only this one snapshot);
            // it drives the manifest selection itself across the range's snapshots.
            snapshot: to_snapshot,
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
            // The incremental-append scan does not emit a `ScanReport` (Java's metrics
            // reporting lives on the snapshot scan, not `IncrementalDataTableScan`); leave
            // it uninstrumented.
            metrics_collector: None,
        };

        Ok(IncrementalAppendScan {
            from_snapshot_id_exclusive,
            to_snapshot_id: Some(to_snapshot_id),
            plan_context: Some(plan_context),
            column_names: self.column_names,
            batch_size: self.batch_size,
            file_io: self.table.file_io().clone(),
            concurrency_limit_manifest_entries: self.concurrency_limit_manifest_entries,
            concurrency_limit_manifest_files: self.concurrency_limit_manifest_files,
        })
    }
}

/// An incremental append scan over the range `(from_snapshot_id exclusive, to_snapshot_id inclusive]`.
///
/// Built via [`Table::incremental_append_scan`](crate::table::Table::incremental_append_scan).
/// Its [`plan_files`](Self::plan_files) streams the [`FileScanTask`]s for the data files
/// the APPEND snapshots in the range added — see the module docs.
#[derive(Debug)]
pub struct IncrementalAppendScan {
    /// The exclusive lower bound — `None` means the whole current lineage of `to`.
    from_snapshot_id_exclusive: Option<i64>,
    /// The inclusive upper bound — `None` only when the table has no snapshots
    /// (the scan is then empty).
    to_snapshot_id: Option<i64>,
    /// `None` when the table has no snapshots and no explicit `to` was set — the scan
    /// produces no rows.
    plan_context: Option<PlanContext>,
    column_names: Option<Vec<String>>,
    batch_size: Option<usize>,
    file_io: FileIO,
    concurrency_limit_manifest_entries: usize,
    concurrency_limit_manifest_files: usize,
}

impl IncrementalAppendScan {
    /// Returns a stream of [`FileScanTask`]s for the appended data files in the range.
    ///
    /// Mirrors Java `BaseIncrementalAppendScan.doPlanFiles` →
    /// `appendFilesFromSnapshots`: compute the APPEND snapshots in
    /// `(from_snapshot_id exclusive, to_snapshot_id inclusive]`; for each, load its
    /// manifest list, keep the DATA manifests it ADDED, read each manifest's `Added`
    /// entries, and stream a `FileScanTask` per entry — applying the same partition
    /// filtering and residual evaluation as the normal scan, with an EMPTY delete index
    /// (an append scan applies no deletes).
    pub async fn plan_files(&self) -> Result<FileScanTaskStream> {
        let Some(plan_context) = self.plan_context.as_ref() else {
            return Ok(Box::pin(futures::stream::empty()));
        };
        let Some(to_snapshot_id) = self.to_snapshot_id else {
            return Ok(Box::pin(futures::stream::empty()));
        };

        // Compute the APPEND snapshots in the range (Java `appendsBetween`). If there are
        // none, the scan is empty.
        let append_snapshots = self.appends_between(
            plan_context,
            self.from_snapshot_id_exclusive,
            to_snapshot_id,
        )?;
        if append_snapshots.is_empty() {
            return Ok(Box::pin(futures::stream::empty()));
        }

        let concurrency_limit_manifest_files = self.concurrency_limit_manifest_files;
        let concurrency_limit_manifest_entries = self.concurrency_limit_manifest_entries;

        let (manifest_entry_data_ctx_tx, manifest_entry_data_ctx_rx) =
            channel(concurrency_limit_manifest_files);
        // A second, never-fed channel for the delete branch — an append scan applies no
        // deletes, so it stays empty. `build_manifest_file_contexts_from_files` needs a
        // delete sender to satisfy its signature; we only pass DATA manifests, so nothing
        // is ever routed to it.
        let (delete_ctx_tx, _delete_ctx_rx) = channel::<ManifestEntryContext>(1);
        let (file_scan_task_tx, file_scan_task_rx) = channel(concurrency_limit_manifest_entries);

        // An EMPTY delete index: an append scan applies no delete files. We build it but
        // never send anything, then drop the sender so the index resolves to "no deletes".
        let (delete_file_idx, delete_file_tx) = DeleteFileIndex::new();
        drop(delete_file_tx);

        // Collect, across every append snapshot, ONLY the DATA manifests that snapshot
        // itself added (Java: `snapshot.dataManifests(io).filter(m -> snapshotIds.contains(
        // m.snapshotId()))`). A manifest carried forward from an older snapshot belongs to
        // that older snapshot and its files were not appended in this range.
        let mut selected_manifests = Vec::new();
        for snapshot in &append_snapshots {
            let manifest_list = snapshot
                .load_manifest_list(&self.file_io, &plan_context.table_metadata)
                .await?;
            for manifest_file in manifest_list.consume_entries() {
                if manifest_file.content == ManifestContentType::Data
                    && manifest_file.added_snapshot_id == snapshot.snapshot_id()
                {
                    selected_manifests.push(manifest_file);
                }
            }
        }

        let manifest_file_contexts = plan_context.build_manifest_file_contexts_from_files(
            selected_manifests,
            manifest_entry_data_ctx_tx,
            delete_file_idx,
            delete_ctx_tx,
        )?;

        let mut channel_for_manifest_error = file_scan_task_tx.clone();

        // Concurrently load all selected manifests and stream their entries.
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

        // Process the data entries in parallel, keeping only `Added` entries.
        spawn(async move {
            let result = manifest_entry_data_ctx_rx
                .map(|me_ctx| Ok((me_ctx, file_scan_task_tx.clone())))
                .try_for_each_concurrent(
                    concurrency_limit_manifest_entries,
                    |(manifest_entry_context, tx)| async move {
                        spawn(async move {
                            Self::process_append_manifest_entry(manifest_entry_context, tx).await
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

    /// Returns the APPEND snapshots in `(from_snapshot_id_exclusive, to_snapshot_id]`,
    /// ordered newest-first (Java `appendsBetween`).
    ///
    /// Walks the parent chain from `to_snapshot_id` back via `parent_snapshot_id`,
    /// stopping BEFORE `from_snapshot_id_exclusive` (the start is excluded), and keeps
    /// only snapshots whose `operation == Append`. `from_snapshot_id_exclusive == None`
    /// walks to the history root (the whole lineage). Mirrors
    /// `SnapshotUtil.ancestorsBetween(to, from, lookup)` filtered to `APPEND`.
    fn appends_between(
        &self,
        plan_context: &PlanContext,
        from_snapshot_id_exclusive: Option<i64>,
        to_snapshot_id: i64,
    ) -> Result<Vec<SnapshotRef>> {
        let metadata = &plan_context.table_metadata;

        // Java `ancestorsBetween`: an equal from/to yields an empty range.
        if from_snapshot_id_exclusive == Some(to_snapshot_id) {
            return Ok(vec![]);
        }

        let mut snapshots = Vec::new();
        let mut current = metadata.snapshot_by_id(to_snapshot_id).cloned();

        while let Some(snapshot) = current {
            // Stop BEFORE the exclusive start (Java's lookup returns null for the start id).
            if Some(snapshot.snapshot_id()) == from_snapshot_id_exclusive {
                break;
            }

            if snapshot.summary().operation == Operation::Append {
                snapshots.push(snapshot.clone());
            }

            current = match snapshot.parent_snapshot_id() {
                Some(parent_id) => metadata.snapshot_by_id(parent_id).cloned(),
                None => None,
            };
        }

        Ok(snapshots)
    }

    /// Processes a single data-manifest entry for the incremental append scan: keeps only
    /// `Added`-status entries (Java `filterManifestEntries(status == ADDED)`), applies the
    /// scan's partition filter, and emits a [`FileScanTask`].
    async fn process_append_manifest_entry(
        manifest_entry_context: ManifestEntryContext,
        mut file_scan_task_tx: Sender<Result<FileScanTask>>,
    ) -> Result<()> {
        // Only ADDED entries: an `Existing` entry was added by an earlier snapshot and
        // copied forward (its files were NOT appended in this range), and a `Deleted`
        // tombstone is a removal. Java keeps `manifestEntry.status() == ADDED`.
        if manifest_entry_context.manifest_entry.status() != ManifestStatus::Added {
            return Ok(());
        }

        // An append scan never reads a delete-file manifest (we only select DATA
        // manifests), but guard the invariant the same way the normal scan does.
        if manifest_entry_context.manifest_entry.content_type() != DataContentType::Data {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Encountered an entry for a delete file in an incremental append scan",
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

            // skip any data file whose partition data indicates it can't match the filter
            if !expression_evaluator.eval(manifest_entry_context.manifest_entry.data_file())? {
                return Ok(());
            }

            // skip any data file whose metrics don't match the filter
            if !crate::expr::visitors::inclusive_metrics_evaluator::InclusiveMetricsEvaluator::eval(
                snapshot_bound_predicate,
                manifest_entry_context.manifest_entry.data_file(),
                false,
            )? {
                return Ok(());
            }
        }

        file_scan_task_tx
            .send(Ok(manifest_entry_context.into_file_scan_task().await?))
            .await?;

        Ok(())
    }

    /// Returns the projected column names of this scan, if a projection was set.
    pub fn column_names(&self) -> Option<&[String]> {
        self.column_names.as_deref()
    }

    /// Returns the inclusive `to` snapshot id of this scan, if the table has snapshots.
    pub fn to_snapshot_id(&self) -> Option<i64> {
        self.to_snapshot_id
    }

    /// Returns the exclusive `from` snapshot id of this scan (`None` = the whole lineage).
    pub fn from_snapshot_id_exclusive(&self) -> Option<i64> {
        self.from_snapshot_id_exclusive
    }

    /// Returns the scan's batch size, if set.
    pub fn batch_size(&self) -> Option<usize> {
        self.batch_size
    }

    /// The schema the scan projects (the `to` snapshot's schema), if the table has snapshots.
    pub fn snapshot_schema(&self) -> Option<&SchemaRef> {
        self.plan_context.as_ref().map(|ctx| &ctx.snapshot_schema)
    }

    /// The resolved [`PlanContext`] (schema + metadata anchor + caches), if the table has
    /// snapshots. Shared with [`IncrementalChangelogScan`], which reuses the append scan's
    /// range resolution but drives its own snapshot selection.
    pub(crate) fn plan_context(&self) -> Option<&PlanContext> {
        self.plan_context.as_ref()
    }
}

/// Builder to create an [`IncrementalChangelogScan`].
///
/// Mirrors the Java `IncrementalChangelogScan` API (`api/IncrementalChangelogScan.java` +
/// `BaseIncrementalScan`): the range bounds (`from` exclusive/inclusive, `to` defaulting
/// to the current snapshot), `with_filter`, and `select*` mirror
/// [`IncrementalAppendScanBuilder`]. The builder shares that scan's range-resolution
/// logic; the difference is entirely in [`IncrementalChangelogScan::plan_files`].
pub struct IncrementalChangelogScanBuilder<'a> {
    table: &'a Table,
    column_names: Option<Vec<String>>,
    from_snapshot_id_exclusive: Option<i64>,
    from_snapshot_id_inclusive: Option<i64>,
    to_snapshot_id: Option<i64>,
    batch_size: Option<usize>,
    case_sensitive: bool,
    filter: Option<Predicate>,
    /// The per-manifest concurrency limit, forwarded to the underlying append scan
    /// builder's `with_concurrency_limit` (which sets both the manifest-file and
    /// manifest-entry limits together — the only public way to set them).
    concurrency_limit: usize,
}

impl<'a> IncrementalChangelogScanBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        let num_cpus = available_parallelism().get();

        Self {
            table,
            column_names: None,
            from_snapshot_id_exclusive: None,
            from_snapshot_id_inclusive: None,
            to_snapshot_id: None,
            batch_size: None,
            case_sensitive: true,
            filter: None,
            concurrency_limit: num_cpus,
        }
    }

    /// Sets the EXCLUSIVE `from` snapshot id (Java `fromSnapshotExclusive`): changes in
    /// `(from, to]` are returned — `from`'s own changes are NOT included. Supersedes any
    /// previously-set inclusive bound.
    pub fn from_snapshot_id_exclusive(mut self, from_snapshot_id: i64) -> Self {
        self.from_snapshot_id_exclusive = Some(from_snapshot_id);
        self.from_snapshot_id_inclusive = None;
        self
    }

    /// Sets the INCLUSIVE `from` snapshot id (Java `fromSnapshotInclusive`): changes in
    /// `[from, to]` are returned — `from`'s own changes ARE included. Resolved to the
    /// exclusive bound (`from`'s parent) at `build()`. Supersedes any previously-set
    /// exclusive bound.
    pub fn from_snapshot_id_inclusive(mut self, from_snapshot_id: i64) -> Self {
        self.from_snapshot_id_inclusive = Some(from_snapshot_id);
        self.from_snapshot_id_exclusive = None;
        self
    }

    /// Sets the INCLUSIVE `to` snapshot id (Java `toSnapshot`). When unset, defaults to
    /// the table's current snapshot.
    pub fn to_snapshot_id(mut self, to_snapshot_id: i64) -> Self {
        self.to_snapshot_id = Some(to_snapshot_id);
        self
    }

    /// Sets the desired size of batches in the response to something other than the default.
    pub fn with_batch_size(mut self, batch_size: Option<usize>) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Sets the scan's case sensitivity.
    pub fn with_case_sensitive(mut self, case_sensitive: bool) -> Self {
        self.case_sensitive = case_sensitive;
        self
    }

    /// Specifies a predicate to use as a filter.
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

    /// Sets the concurrency limit for both manifest files and manifest entries.
    pub fn with_concurrency_limit(mut self, limit: usize) -> Self {
        self.concurrency_limit = limit;
        self
    }

    /// Build the incremental changelog scan.
    ///
    /// Resolves the range bounds, schema, projected field ids, and bound filter exactly
    /// as [`IncrementalAppendScanBuilder::build`] does (the two scans share range
    /// resolution). The changelog-specific snapshot selection — exclude `Replace`,
    /// guard delete manifests, assign ordinals — happens lazily in
    /// [`IncrementalChangelogScan::plan_files`].
    pub fn build(self) -> Result<IncrementalChangelogScan> {
        // Reuse the append scan's builder to resolve the range + plan context. The two
        // builders share every field; only `plan_files` differs. This avoids duplicating
        // the (non-trivial) range-resolution + projection logic. The public API sets both
        // concurrency limits together (`with_concurrency_limit`), so passing the files
        // limit covers the entries limit too.
        let mut append_builder = IncrementalAppendScanBuilder::new(self.table)
            .with_case_sensitive(self.case_sensitive)
            .with_batch_size(self.batch_size)
            .with_concurrency_limit(self.concurrency_limit);

        if let Some(from_exclusive) = self.from_snapshot_id_exclusive {
            append_builder = append_builder.from_snapshot_id_exclusive(from_exclusive);
        } else if let Some(from_inclusive) = self.from_snapshot_id_inclusive {
            append_builder = append_builder.from_snapshot_id_inclusive(from_inclusive);
        }
        if let Some(to_snapshot_id) = self.to_snapshot_id {
            append_builder = append_builder.to_snapshot_id(to_snapshot_id);
        }
        if let Some(ref column_names) = self.column_names {
            append_builder = append_builder.select(column_names);
        }
        if let Some(ref filter) = self.filter {
            // `filter` is already `rewrite_not`-normalized; `with_filter` re-normalizes,
            // which is idempotent.
            append_builder = append_builder.with_filter(filter.clone());
        }

        Ok(IncrementalChangelogScan {
            append_scan: append_builder.build()?,
            file_io: self.table.file_io().clone(),
        })
    }
}

/// An incremental changelog scan over `(from_snapshot_id exclusive, to_snapshot_id inclusive]`.
///
/// Built via
/// [`Table::incremental_changelog_scan`](crate::table::Table::incremental_changelog_scan).
/// Its [`plan_files`](Self::plan_files) streams a [`ChangelogScanTask`] per data file
/// added (INSERT) or removed (DELETE) by the snapshots in the range — see the module docs.
#[derive(Debug)]
pub struct IncrementalChangelogScan {
    /// The underlying append scan carries the resolved range bounds + plan context. The
    /// changelog scan reuses its range resolution and `PlanContext` but drives its own
    /// snapshot selection + per-entry task construction in `plan_files`.
    append_scan: IncrementalAppendScan,
    file_io: FileIO,
}

impl IncrementalChangelogScan {
    /// Returns a stream of [`ChangelogScanTask`]s for the row-level changes in the range.
    ///
    /// Mirrors Java `BaseIncrementalChangelogScan.doPlanFiles`:
    /// 1. compute the changelog snapshots in `(from, to]` oldest-first, EXCLUDING
    ///    `Operation::Replace`; if any range snapshot carries a row-level DELETE manifest,
    ///    return `FeatureUnsupported` (Java throws `UnsupportedOperationException`);
    /// 2. assign each snapshot a change ordinal (oldest → 0, incrementing);
    /// 3. for each changelog snapshot, read the DATA manifests IT added
    ///    (`added_snapshot_id == snapshot_id`) and emit a task per ADDED entry (INSERT) or
    ///    DELETED entry (DELETE) — skipping `Existing` entries — tagged with that
    ///    snapshot's ordinal and `commit_snapshot_id = entry.snapshot_id()`, reusing the
    ///    same partition-filter pruning + residual as the append scan.
    pub async fn plan_files(&self) -> Result<ChangelogScanTaskStream> {
        let Some(plan_context) = self.append_scan.plan_context() else {
            return Ok(Box::pin(futures::stream::empty()));
        };
        let Some(to_snapshot_id) = self.append_scan.to_snapshot_id() else {
            return Ok(Box::pin(futures::stream::empty()));
        };

        // Step 1: the changelog snapshots in the range, oldest-first, excluding Replace,
        // guarding delete manifests.
        let changelog_snapshots = self
            .ordered_changelog_snapshots(
                plan_context,
                self.append_scan.from_snapshot_id_exclusive(),
                to_snapshot_id,
            )
            .await?;
        if changelog_snapshots.is_empty() {
            return Ok(Box::pin(futures::stream::empty()));
        }

        // Step 2 + 3: walk the snapshots oldest-first, assigning ordinals, and build a
        // task per added/deleted entry of each snapshot's own added DATA manifests. The
        // changelog range is bounded, so collecting the tasks eagerly (rather than the
        // append scan's concurrent channel pipeline) keeps the per-snapshot ordinal
        // attachment simple and correct — each snapshot's tasks share one ordinal.
        let mut tasks: Vec<ChangelogScanTask> = Vec::new();
        for (ordinal, snapshot) in changelog_snapshots.iter().enumerate() {
            let change_ordinal = i32::try_from(ordinal).map_err(|_| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Too many changelog snapshots in range to assign a change ordinal",
                )
            })?;

            let selected_manifests = self
                .own_added_data_manifests(plan_context, snapshot)
                .await?;
            if selected_manifests.is_empty() {
                continue;
            }

            let snapshot_tasks = Self::plan_snapshot_change_tasks(
                plan_context,
                selected_manifests,
                change_ordinal,
                snapshot.snapshot_id(),
            )
            .await?;
            tasks.extend(snapshot_tasks);
        }

        Ok(Box::pin(futures::stream::iter(tasks.into_iter().map(Ok))))
    }

    /// Returns the changelog snapshots in `(from_snapshot_id_exclusive, to_snapshot_id]`
    /// ordered OLDEST-FIRST (Java `orderedChangelogSnapshots`).
    ///
    /// Walks the parent chain from `to_snapshot_id` back to (but excluding)
    /// `from_snapshot_id_exclusive` — the same range walk the append scan uses — but:
    /// - EXCLUDES `Operation::Replace` snapshots (compaction rewrites files without
    ///   changing rows, so they produce no changelog), and
    /// - REJECTS the range with `FeatureUnsupported` if any kept snapshot references a
    ///   row-level DELETE manifest (Java throws `UnsupportedOperationException`: "Delete
    ///   files are currently not supported in changelog scans").
    ///
    /// The walk visits newest-first; the result is reversed to oldest-first so the caller
    /// can assign change ordinals (oldest → 0).
    async fn ordered_changelog_snapshots(
        &self,
        plan_context: &PlanContext,
        from_snapshot_id_exclusive: Option<i64>,
        to_snapshot_id: i64,
    ) -> Result<Vec<SnapshotRef>> {
        let metadata = &plan_context.table_metadata;

        // An equal from/to yields an empty range (Java `ancestorsBetween`).
        if from_snapshot_id_exclusive == Some(to_snapshot_id) {
            return Ok(vec![]);
        }

        let mut newest_first = Vec::new();
        let mut current = metadata.snapshot_by_id(to_snapshot_id).cloned();

        while let Some(snapshot) = current {
            // Stop BEFORE the exclusive start.
            if Some(snapshot.snapshot_id()) == from_snapshot_id_exclusive {
                break;
            }

            // Exclude Replace snapshots (compaction): they rewrite files without changing
            // rows, so they contribute no row-level changes.
            if snapshot.summary().operation != Operation::Replace {
                // Guard: a snapshot referencing a row-level DELETE manifest is out of
                // scope for the data-file changelog (Java's current limitation).
                if self
                    .snapshot_has_delete_manifest(plan_context, &snapshot)
                    .await?
                {
                    return Err(Error::new(
                        ErrorKind::FeatureUnsupported,
                        "Delete files are currently not supported in changelog scans",
                    ));
                }
                newest_first.push(snapshot.clone());
            }

            current = match snapshot.parent_snapshot_id() {
                Some(parent_id) => metadata.snapshot_by_id(parent_id).cloned(),
                None => None,
            };
        }

        // Reverse to oldest-first (Java builds the deque with `addFirst`).
        newest_first.reverse();
        Ok(newest_first)
    }

    /// Returns whether `snapshot` references any row-level DELETE manifest — Java
    /// `!snapshot.deleteManifests(io).isEmpty()`. Loads the snapshot's manifest list and
    /// checks for any `ManifestContentType::Deletes` entry.
    async fn snapshot_has_delete_manifest(
        &self,
        plan_context: &PlanContext,
        snapshot: &SnapshotRef,
    ) -> Result<bool> {
        let manifest_list = snapshot
            .load_manifest_list(&self.file_io, &plan_context.table_metadata)
            .await?;
        Ok(manifest_list
            .entries()
            .iter()
            .any(|manifest_file| manifest_file.content == ManifestContentType::Deletes))
    }

    /// Returns the DATA manifests `snapshot` itself ADDED (`added_snapshot_id ==
    /// snapshot_id`) — Java `snapshot.dataManifests(io).filter(m -> snapshotIds.contains(
    /// m.snapshotId()))` restricted to this one snapshot. A manifest carried forward from
    /// an older snapshot belongs to that older snapshot and is read for ITS ordinal, not
    /// this one's; a manifest this snapshot rewrote (because it deleted a file from it)
    /// carries this snapshot's id, so its `Deleted` tombstones are read here.
    async fn own_added_data_manifests(
        &self,
        plan_context: &PlanContext,
        snapshot: &SnapshotRef,
    ) -> Result<Vec<crate::spec::ManifestFile>> {
        let manifest_list = snapshot
            .load_manifest_list(&self.file_io, &plan_context.table_metadata)
            .await?;
        let mut selected = Vec::new();
        for manifest_file in manifest_list.consume_entries() {
            if manifest_file.content == ManifestContentType::Data
                && manifest_file.added_snapshot_id == snapshot.snapshot_id()
            {
                selected.push(manifest_file);
            }
        }
        Ok(selected)
    }

    /// Plans the changelog tasks for ONE snapshot's own added DATA manifests, tagging each
    /// with the snapshot's `change_ordinal` and `commit_snapshot_id`. ADDED entries become
    /// INSERT tasks, DELETED entries become DELETE tasks; `Existing` entries are skipped.
    ///
    /// Reuses the shared `PlanContext::build_manifest_file_contexts_from_files` (the same
    /// partition-filter pruning + residual evaluator the append + normal scans use) over
    /// an EMPTY delete index (a changelog task carries no delete files), then converts the
    /// surviving entries into `ChangelogScanTask`s.
    async fn plan_snapshot_change_tasks(
        plan_context: &PlanContext,
        selected_manifests: Vec<crate::spec::ManifestFile>,
        change_ordinal: i32,
        snapshot_id: i64,
    ) -> Result<Vec<ChangelogScanTask>> {
        let manifest_count = selected_manifests.len().max(1);
        let (manifest_entry_data_ctx_tx, manifest_entry_data_ctx_rx) = channel(manifest_count);
        // A never-fed delete-manifest channel (we only pass DATA manifests).
        let (delete_ctx_tx, _delete_ctx_rx) = channel::<ManifestEntryContext>(1);
        // The output channel carries `Result` so a producer's manifest-fetch error reaches
        // the consumer instead of being silently dropped when the producer task ends.
        let (task_tx, task_rx) = channel::<Result<ChangelogScanTask>>(manifest_count);

        // An EMPTY delete index: a changelog task applies no delete files.
        let (delete_file_idx, delete_file_tx) = DeleteFileIndex::new();
        drop(delete_file_tx);

        let manifest_file_contexts = plan_context.build_manifest_file_contexts_from_files(
            selected_manifests,
            manifest_entry_data_ctx_tx,
            delete_file_idx,
            delete_ctx_tx,
        )?;

        // Spawn the producers (fetch each manifest, stream its entries into the entry
        // channel) so the consumer below can drain CONCURRENTLY — a manifest may hold more
        // entries than the channel's capacity, so a producer's `send` can block until the
        // consumer reads. Draining in the same task after a blocking send would deadlock. A
        // producer error is forwarded into the output channel so it is not lost.
        let mut producer_error_tx = task_tx.clone();
        spawn(async move {
            let result = futures::stream::iter(manifest_file_contexts)
                .try_for_each_concurrent(manifest_count, |ctx| async move {
                    ctx.fetch_manifest_and_stream_manifest_entries().await
                })
                .await;
            if let Err(error) = result {
                let _ = producer_error_tx.send(Err(error)).await;
            }
        });

        // Convert each kept entry into a changelog task, sending the result to the output
        // channel. Run on a task so it interleaves with the producers (avoiding the
        // blocking-send deadlock).
        spawn(async move {
            let result = manifest_entry_data_ctx_rx
                .map(Ok)
                .try_for_each(|manifest_entry_context| {
                    let mut task_tx = task_tx.clone();
                    async move {
                        let task = Self::changelog_task_from_entry(
                            manifest_entry_context,
                            change_ordinal,
                            snapshot_id,
                        )
                        .await;
                        match task {
                            Ok(Some(task)) => {
                                let _ = task_tx.send(Ok(task)).await;
                            }
                            Ok(None) => {}
                            Err(error) => {
                                let _ = task_tx.send(Err(error)).await;
                            }
                        }
                        Ok(())
                    }
                })
                .await;
            // `try_for_each` over `map(Ok)` never yields an Err; bind to satisfy the type.
            let _: Result<()> = result;
        });

        task_rx.try_collect().await
    }

    /// Converts a single manifest entry into a [`ChangelogScanTask`], or `None` when the
    /// entry is `Existing` (no change) or pruned by the partition filter.
    ///
    /// Mirrors Java `CreateDataFileChangeTasks.apply`: ADDED → INSERT, DELETED → DELETE,
    /// each carrying the change ordinal + `commit_snapshot_id = entry.snapshotId()` (the
    /// snapshot stamped on the entry, falling back to the snapshot id when an inherited
    /// entry's snapshot id is not yet materialized). Applies the same partition-filter +
    /// inclusive-metrics pruning as the append scan before emitting.
    async fn changelog_task_from_entry(
        manifest_entry_context: ManifestEntryContext,
        change_ordinal: i32,
        snapshot_id: i64,
    ) -> Result<Option<ChangelogScanTask>> {
        let operation = match manifest_entry_context.manifest_entry.status() {
            ManifestStatus::Added => ChangelogOperation::Insert,
            ManifestStatus::Deleted => ChangelogOperation::Delete,
            // `Existing` entries were added by an earlier snapshot and copied forward —
            // they are not a change committed by THIS snapshot. Java `ignoreExisting()`.
            ManifestStatus::Existing => return Ok(None),
        };

        // A changelog scan never reads a delete-file manifest (we only select DATA
        // manifests), but guard the invariant the same way the append scan does.
        if manifest_entry_context.manifest_entry.content_type() != DataContentType::Data {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Encountered an entry for a delete file in an incremental changelog scan",
            ));
        }

        // The snapshot that committed the change. An `Added`/`Deleted` entry written by
        // this snapshot carries its id; a V2/V3 added entry whose id is inherited at read
        // time may be absent, so fall back to the snapshot id (Java reads the entry's
        // snapshotId, which inheritance has populated by plan time).
        let commit_snapshot_id = manifest_entry_context
            .manifest_entry
            .snapshot_id()
            .unwrap_or(snapshot_id);

        // Apply the same partition-filter + inclusive-metrics pruning as the append scan.
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

            // skip any data file whose partition data indicates it can't match the filter
            if !expression_evaluator.eval(manifest_entry_context.manifest_entry.data_file())? {
                return Ok(None);
            }

            // skip any data file whose metrics don't match the filter
            if !crate::expr::visitors::inclusive_metrics_evaluator::InclusiveMetricsEvaluator::eval(
                snapshot_bound_predicate,
                manifest_entry_context.manifest_entry.data_file(),
                false,
            )? {
                return Ok(None);
            }
        }

        let file_scan_task = manifest_entry_context.into_file_scan_task().await?;

        Ok(Some(ChangelogScanTask {
            change_ordinal,
            commit_snapshot_id,
            operation,
            file_scan_task,
        }))
    }

    /// Returns the inclusive `to` snapshot id of this scan, if the table has snapshots.
    pub fn to_snapshot_id(&self) -> Option<i64> {
        self.append_scan.to_snapshot_id()
    }

    /// Returns the exclusive `from` snapshot id of this scan (`None` = the whole lineage).
    pub fn from_snapshot_id_exclusive(&self) -> Option<i64> {
        self.append_scan.from_snapshot_id_exclusive()
    }

    /// The schema the scan projects (the `to` snapshot's schema), if the table has snapshots.
    pub fn snapshot_schema(&self) -> Option<&SchemaRef> {
        self.append_scan.snapshot_schema()
    }
}

/// Returns whether `ancestor_id` is an ancestor of `snapshot_id` (inclusive of
/// `snapshot_id` itself) — Java `SnapshotUtil.isAncestorOf`. Walks the parent chain of
/// `snapshot_id`.
fn is_ancestor_of(table: &Table, snapshot_id: i64, ancestor_id: i64) -> bool {
    let metadata = table.metadata();
    let mut current = metadata.snapshot_by_id(snapshot_id).cloned();
    while let Some(snapshot) = current {
        if snapshot.snapshot_id() == ancestor_id {
            return true;
        }
        current = match snapshot.parent_snapshot_id() {
            Some(parent_id) => metadata.snapshot_by_id(parent_id).cloned(),
            None => None,
        };
    }
    false
}

/// Returns whether some ancestor of `snapshot_id` has `parent_id == parent_ancestor_id`
/// — Java `SnapshotUtil.isParentAncestorOf`. This is the exclusive-`from` validity check:
/// `from` is a parent of some ancestor of `to`, so the range `(from, to]` is well-defined
/// even when `from` itself has been expired.
fn is_parent_ancestor_of(table: &Table, snapshot_id: i64, parent_ancestor_id: i64) -> bool {
    let metadata = table.metadata();
    let mut current = metadata.snapshot_by_id(snapshot_id).cloned();
    while let Some(snapshot) = current {
        if snapshot.parent_snapshot_id() == Some(parent_ancestor_id) {
            return true;
        }
        current = match snapshot.parent_snapshot_id() {
            Some(parent_id) => metadata.snapshot_by_id(parent_id).cloned(),
            None => None,
        };
    }
    false
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::fs::File;
    use std::io::BufReader;

    use futures::TryStreamExt;

    use crate::expr::Reference;
    use crate::memory::tests::new_memory_catalog;
    use crate::scan::FileScanTask;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Datum, FormatVersion, Literal,
        Operation, Struct, TableMetadata,
    };
    use crate::table::Table;
    use crate::transaction::{ApplyTransactionAction, Transaction};
    use crate::{Catalog, ErrorKind, TableCreation, TableIdent};

    /// Create a V3 table partitioned by identity(x) in the catalog from the shared
    /// `TableMetadataV3ValidMinimal` fixture (schema `x, y, z` longs; spec id 0 =
    /// identity(x)). Inlined here rather than reusing `transaction::tests::
    /// make_v3_minimal_table_in_catalog` because that module is private to `transaction/`;
    /// copying the 15-line helper keeps the visibility narrow (lessons 2026-06-08).
    async fn make_minimal_table(catalog: &impl Catalog) -> Table {
        let table_ident =
            TableIdent::from_strs([format!("ns-{}", uuid::Uuid::new_v4()), "t".to_string()])
                .unwrap();
        catalog
            .create_namespace(table_ident.namespace(), HashMap::new())
            .await
            .unwrap();

        let file = File::open(format!(
            "{}/testdata/table_metadata/TableMetadataV3ValidMinimal.json",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap();
        let base_metadata =
            serde_json::from_reader::<_, TableMetadata>(BufReader::new(file)).unwrap();

        let table_creation = TableCreation::builder()
            .schema((**base_metadata.current_schema()).clone())
            .partition_spec((**base_metadata.default_partition_spec()).clone())
            .sort_order((**base_metadata.default_sort_order()).clone())
            .name(table_ident.name().to_string())
            .format_version(FormatVersion::V3)
            .build();

        catalog
            .create_table(table_ident.namespace(), table_creation)
            .await
            .unwrap()
    }

    /// Build a data file routed to partition `x = part_value` (the V3 minimal table is
    /// partitioned by identity(x), spec id 0) with a unique path.
    fn data_file(path: &str, part_value: i64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(part_value))]))
            .build()
            .unwrap()
    }

    /// Append the given files in a single fast-append commit and return the updated table.
    async fn append_files(catalog: &impl Catalog, table: &Table, files: Vec<DataFile>) -> Table {
        let tx = Transaction::new(table);
        let action = tx.fast_append().add_data_files(files);
        let tx = action.apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    /// Collect the data-file paths an incremental append scan plans.
    async fn planned_paths(scan: &super::IncrementalAppendScan) -> HashSet<String> {
        let tasks: Vec<FileScanTask> = scan
            .plan_files()
            .await
            .expect("plan_files should succeed")
            .try_collect()
            .await
            .expect("collecting file scan tasks should succeed");
        tasks.into_iter().map(|t| t.data_file_path).collect()
    }

    /// CORE BEHAVIOR: from=S0(exclusive) to=S2(inclusive) returns ONLY the files appended
    /// in S1 + S2, never S0's file. Risk pinned: the range planner must include both later
    /// append snapshots and exclude the starting snapshot's own files.
    #[tokio::test]
    async fn test_incremental_append_returns_appends_in_range_excluding_from() {
        let catalog = new_memory_catalog().await;
        let table = make_minimal_table(&catalog).await;

        let table = append_files(&catalog, &table, vec![data_file("s0.parquet", 1)]).await;
        let s0 = table.metadata().current_snapshot_id().unwrap();
        let table = append_files(&catalog, &table, vec![data_file("s1.parquet", 1)]).await;
        let table = append_files(&catalog, &table, vec![data_file("s2.parquet", 1)]).await;
        let s2 = table.metadata().current_snapshot_id().unwrap();

        let scan = table
            .incremental_append_scan()
            .from_snapshot_id_exclusive(s0)
            .to_snapshot_id(s2)
            .build()
            .unwrap();

        let paths = planned_paths(&scan).await;
        assert_eq!(
            paths,
            HashSet::from(["s1.parquet".to_string(), "s2.parquet".to_string()]),
            "should return only S1 + S2 appended files, not S0's"
        );
    }

    /// EXCLUSIVE-FROM BOUNDARY: from=S1(exclusive) to=S2 returns ONLY S2's file — S1's own
    /// file is NOT included even though S1 is the immediate `from`. Mutation-pins the
    /// exclusive boundary (an inclusive walk would wrongly add S1's file).
    #[tokio::test]
    async fn test_incremental_append_from_is_exclusive() {
        let catalog = new_memory_catalog().await;
        let table = make_minimal_table(&catalog).await;

        let table = append_files(&catalog, &table, vec![data_file("s0.parquet", 1)]).await;
        let table = append_files(&catalog, &table, vec![data_file("s1.parquet", 1)]).await;
        let s1 = table.metadata().current_snapshot_id().unwrap();
        let table = append_files(&catalog, &table, vec![data_file("s2.parquet", 1)]).await;
        let s2 = table.metadata().current_snapshot_id().unwrap();

        let scan = table
            .incremental_append_scan()
            .from_snapshot_id_exclusive(s1)
            .to_snapshot_id(s2)
            .build()
            .unwrap();

        let paths = planned_paths(&scan).await;
        assert_eq!(
            paths,
            HashSet::from(["s2.parquet".to_string()]),
            "exclusive from=S1 must exclude S1's own file"
        );
    }

    /// INCLUSIVE-FROM: from=S1(inclusive) to=S2 returns BOTH S1 and S2 — the inclusive
    /// bound resolves to S1's parent (S0) as the exclusive boundary, so S1's APPEND files
    /// are included. Pins the inclusive→parent resolution.
    #[tokio::test]
    async fn test_incremental_append_from_inclusive_includes_from_snapshot() {
        let catalog = new_memory_catalog().await;
        let table = make_minimal_table(&catalog).await;

        let table = append_files(&catalog, &table, vec![data_file("s0.parquet", 1)]).await;
        let table = append_files(&catalog, &table, vec![data_file("s1.parquet", 1)]).await;
        let s1 = table.metadata().current_snapshot_id().unwrap();
        let table = append_files(&catalog, &table, vec![data_file("s2.parquet", 1)]).await;
        let s2 = table.metadata().current_snapshot_id().unwrap();

        let scan = table
            .incremental_append_scan()
            .from_snapshot_id_inclusive(s1)
            .to_snapshot_id(s2)
            .build()
            .unwrap();

        let paths = planned_paths(&scan).await;
        assert_eq!(
            paths,
            HashSet::from(["s1.parquet".to_string(), "s2.parquet".to_string()]),
            "inclusive from=S1 must include S1's own file"
        );
    }

    /// FROM == TO (exclusive): rejected by the `isParentAncestorOf` precondition, exactly
    /// like Java (a snapshot is never a parent ancestor of itself). Java's `ancestorsBetween`
    /// empty short-circuit is unreachable here because the precondition fails first.
    #[tokio::test]
    async fn test_incremental_append_from_equals_to_exclusive_is_rejected() {
        let catalog = new_memory_catalog().await;
        let table = make_minimal_table(&catalog).await;

        let table = append_files(&catalog, &table, vec![data_file("s0.parquet", 1)]).await;
        let table = append_files(&catalog, &table, vec![data_file("s1.parquet", 1)]).await;
        let s1 = table.metadata().current_snapshot_id().unwrap();

        let result = table
            .incremental_append_scan()
            .from_snapshot_id_exclusive(s1)
            .to_snapshot_id(s1)
            .build();
        assert!(
            result.is_err(),
            "from == to (exclusive) must be rejected: a snapshot is not its own parent ancestor"
        );
    }

    /// EMPTY RANGE (no APPEND snapshots in range): the only snapshot in `(from, to]` is an OVERWRITE,
    /// so the append-only filter drops it and zero tasks are planned. This is the
    /// Java-reachable "empty range → zero tasks" case (the range is non-empty but contains
    /// no APPEND snapshot).
    #[tokio::test]
    async fn test_incremental_append_range_with_no_append_snapshots_is_empty() {
        let catalog = new_memory_catalog().await;
        let table = make_minimal_table(&catalog).await;

        // S0 (append a, b), then S1 = OVERWRITE (delete a, add c). The range (S0, S1] holds
        // only the overwrite.
        let table = append_files(&catalog, &table, vec![
            data_file("a.parquet", 1),
            data_file("b.parquet", 1),
        ])
        .await;
        let s0 = table.metadata().current_snapshot_id().unwrap();

        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .delete_file("a.parquet")
            .add_file(data_file("c.parquet", 1));
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let s1 = table.metadata().current_snapshot_id().unwrap();

        let scan = table
            .incremental_append_scan()
            .from_snapshot_id_exclusive(s0)
            .to_snapshot_id(s1)
            .build()
            .unwrap();

        let paths = planned_paths(&scan).await;
        assert!(
            paths.is_empty(),
            "a range whose only snapshot is an overwrite plans zero tasks"
        );
    }

    /// APPEND-ONLY: an OVERWRITE snapshot in the range is EXCLUDED — only files added by
    /// the APPEND snapshot are returned, NOT the file the overwrite added. Mutation-pins the
    /// `Operation::Append` op filter (dropping it would include the overwrite's added file).
    #[tokio::test]
    async fn test_incremental_append_excludes_overwrite_snapshot() {
        let catalog = new_memory_catalog().await;
        let table = make_minimal_table(&catalog).await;

        // S0 (append a), S1 (append b), S2 (overwrite: delete b, add c → Operation::Overwrite).
        let table = append_files(&catalog, &table, vec![data_file("a.parquet", 1)]).await;
        let s0 = table.metadata().current_snapshot_id().unwrap();
        let table = append_files(&catalog, &table, vec![data_file("b.parquet", 1)]).await;

        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .delete_file("b.parquet")
            .add_file(data_file("c.parquet", 1));
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let s2 = table.metadata().current_snapshot_id().unwrap();

        // The S2 snapshot must be an OVERWRITE (delete + add).
        assert_eq!(
            table
                .metadata()
                .snapshot_by_id(s2)
                .unwrap()
                .summary()
                .operation,
            crate::spec::Operation::Overwrite,
            "S2 must be an overwrite for this test to be meaningful"
        );

        let scan = table
            .incremental_append_scan()
            .from_snapshot_id_exclusive(s0)
            .to_snapshot_id(s2)
            .build()
            .unwrap();

        let paths = planned_paths(&scan).await;
        assert_eq!(
            paths,
            HashSet::from(["b.parquet".to_string()]),
            "only the APPEND (S1) file b is returned; the OVERWRITE (S2) file c is excluded"
        );
    }

    /// FILTER PRUNES BY PARTITION: a `with_filter(x == 10)` over an identity(x)-partitioned
    /// table prunes the appended file in partition x = 20. Reuses the same partition-filter
    /// machinery as the normal scan.
    #[tokio::test]
    async fn test_incremental_append_with_filter_prunes_by_partition() {
        let catalog = new_memory_catalog().await;
        let table = make_minimal_table(&catalog).await;

        let table = append_files(&catalog, &table, vec![data_file("base.parquet", 1)]).await;
        let s0 = table.metadata().current_snapshot_id().unwrap();
        // S1 appends one file in x = 10 and one in x = 20.
        let table = append_files(&catalog, &table, vec![
            data_file("x10.parquet", 10),
            data_file("x20.parquet", 20),
        ])
        .await;
        let s1 = table.metadata().current_snapshot_id().unwrap();

        let scan = table
            .incremental_append_scan()
            .from_snapshot_id_exclusive(s0)
            .to_snapshot_id(s1)
            .with_filter(Reference::new("x").equal_to(Datum::long(10)))
            .build()
            .unwrap();

        let paths = planned_paths(&scan).await;
        assert_eq!(
            paths,
            HashSet::from(["x10.parquet".to_string()]),
            "filter x == 10 must prune the x = 20 appended file"
        );
    }

    /// DEFAULT to=current: when `to_snapshot_id` is unset the scan ends at the current
    /// snapshot. from=S0(excl), no `to` ⇒ returns S1 + S2.
    #[tokio::test]
    async fn test_incremental_append_to_defaults_to_current_snapshot() {
        let catalog = new_memory_catalog().await;
        let table = make_minimal_table(&catalog).await;

        let table = append_files(&catalog, &table, vec![data_file("s0.parquet", 1)]).await;
        let s0 = table.metadata().current_snapshot_id().unwrap();
        let table = append_files(&catalog, &table, vec![data_file("s1.parquet", 1)]).await;
        let table = append_files(&catalog, &table, vec![data_file("s2.parquet", 1)]).await;

        let scan = table
            .incremental_append_scan()
            .from_snapshot_id_exclusive(s0)
            .build()
            .unwrap();

        assert_eq!(
            scan.to_snapshot_id(),
            table.metadata().current_snapshot_id(),
            "unset to_snapshot_id must default to the current snapshot"
        );

        let paths = planned_paths(&scan).await;
        assert_eq!(
            paths,
            HashSet::from(["s1.parquet".to_string(), "s2.parquet".to_string()]),
            "default to=current returns the appends after S0"
        );
    }

    /// WHOLE LINEAGE: with no `from` set, the scan returns every appended file in the
    /// current lineage (Java's null `fromSnapshotId` → walk to the root).
    #[tokio::test]
    async fn test_incremental_append_no_from_scans_whole_lineage() {
        let catalog = new_memory_catalog().await;
        let table = make_minimal_table(&catalog).await;

        let table = append_files(&catalog, &table, vec![data_file("s0.parquet", 1)]).await;
        let table = append_files(&catalog, &table, vec![data_file("s1.parquet", 1)]).await;
        let table = append_files(&catalog, &table, vec![data_file("s2.parquet", 1)]).await;

        let scan = table.incremental_append_scan().build().unwrap();

        let paths = planned_paths(&scan).await;
        assert_eq!(
            paths,
            HashSet::from([
                "s0.parquet".to_string(),
                "s1.parquet".to_string(),
                "s2.parquet".to_string(),
            ]),
            "no from bound returns every appended file in the lineage"
        );
    }

    /// EMPTY TABLE: an incremental scan on a table with no snapshots produces zero tasks
    /// (no current snapshot, no explicit to).
    #[tokio::test]
    async fn test_incremental_append_empty_table_is_empty() {
        let catalog = new_memory_catalog().await;
        let table = make_minimal_table(&catalog).await;

        let scan = table.incremental_append_scan().build().unwrap();
        let paths = planned_paths(&scan).await;
        assert!(paths.is_empty(), "an empty table plans zero tasks");
    }

    /// VALIDATION: a non-ancestor exclusive `from` is rejected (Java
    /// `isParentAncestorOf` precondition).
    #[tokio::test]
    async fn test_incremental_append_rejects_non_ancestor_from() {
        let catalog = new_memory_catalog().await;
        let table = make_minimal_table(&catalog).await;

        let table = append_files(&catalog, &table, vec![data_file("s0.parquet", 1)]).await;
        let s0 = table.metadata().current_snapshot_id().unwrap();
        let table = append_files(&catalog, &table, vec![data_file("s1.parquet", 1)]).await;
        let s1 = table.metadata().current_snapshot_id().unwrap();

        // S1 is a descendant of S0, so from=S1 to=S0 is invalid (S1 is not a parent
        // ancestor of S0).
        let result = table
            .incremental_append_scan()
            .from_snapshot_id_exclusive(s1)
            .to_snapshot_id(s0)
            .build();
        assert!(
            result.is_err(),
            "a from that is not a parent ancestor of to must be rejected"
        );
    }

    /// ADDED-MANIFEST FILTER: a manifest carried forward into a later snapshot (not added
    /// by that snapshot) must NOT re-surface its files. After S0 appends a, S1 appends b;
    /// S1's manifest list carries S0's manifest forward, but only S1's OWN added manifest
    /// (holding b) counts for the (S0, S1] range. Mutation-pins the
    /// `added_snapshot_id == snapshot_id` manifest filter and the `Added`-entry filter.
    #[tokio::test]
    async fn test_incremental_append_only_counts_snapshots_own_added_manifests() {
        let catalog = new_memory_catalog().await;
        let table = make_minimal_table(&catalog).await;

        let table = append_files(&catalog, &table, vec![data_file("a.parquet", 1)]).await;
        let s0 = table.metadata().current_snapshot_id().unwrap();
        let table = append_files(&catalog, &table, vec![data_file("b.parquet", 1)]).await;
        let s1 = table.metadata().current_snapshot_id().unwrap();

        let scan = table
            .incremental_append_scan()
            .from_snapshot_id_exclusive(s0)
            .to_snapshot_id(s1)
            .build()
            .unwrap();

        let paths = planned_paths(&scan).await;
        assert_eq!(
            paths,
            HashSet::from(["b.parquet".to_string()]),
            "only S1's own added manifest (b) counts; S0's carried-forward manifest (a) does not"
        );
    }

    /// ADDED-ENTRY FILTER (in isolation): a snapshot's OWN added manifest can carry a
    /// non-`Added` entry (an `Existing` file copied forward, or a `Deleted` tombstone) when
    /// that snapshot rewrote a manifest — Java's `MergeAppend` produces exactly this. The
    /// incremental append scan must keep ONLY the `Added` entry (Java
    /// `filterManifestEntries(status == ADDED)`), never the `Existing`/`Deleted` ones.
    ///
    /// The fast-append fixtures elsewhere in this module can't pin this: a fast-append's own
    /// manifest holds only `Added` entries, so the status filter is never exercised. This
    /// test reuses `TableTestFixture::setup_manifest_files`, which writes a single manifest —
    /// added by the CURRENT (APPEND) snapshot — holding `1.parquet` (Added), `2.parquet`
    /// (Deleted), `3.parquet` (Existing). The current snapshot's parent is the exclusive
    /// `from`, so the range `(parent, current]` selects exactly that manifest. A normal scan
    /// over the same fixture returns BOTH `1.parquet` and `3.parquet`; the incremental scan
    /// must return ONLY `1.parquet`. Mutation-pins the `status == Added` entry filter:
    /// dropping it re-surfaces `3.parquet`.
    #[tokio::test]
    async fn test_incremental_append_keeps_only_added_entries_of_own_manifest() {
        let mut fixture = crate::scan::tests::TableTestFixture::new();
        fixture.setup_manifest_files().await;

        let metadata = fixture.table.metadata();
        let current_snapshot_id = metadata.current_snapshot_id().unwrap();
        let parent_snapshot_id = metadata
            .current_snapshot()
            .unwrap()
            .parent_snapshot_id()
            .unwrap();

        let scan = fixture
            .table
            .incremental_append_scan()
            .from_snapshot_id_exclusive(parent_snapshot_id)
            .to_snapshot_id(current_snapshot_id)
            .build()
            .unwrap();

        let paths = planned_paths(&scan).await;
        assert_eq!(
            paths,
            HashSet::from([format!("{}/1.parquet", &fixture.table_location)]),
            "only the Added entry (1.parquet) is returned; the Existing (3.parquet) and \
             Deleted (2.parquet) entries of the snapshot's own manifest are excluded"
        );
    }

    // ===================================================================================
    // IncrementalChangelogScan tests
    // ===================================================================================

    use super::super::{ChangelogOperation, ChangelogScanTask};

    /// A position-delete file (content `PositionDeletes`) routed to partition `x =
    /// part_value`, used to create a DELETE-content manifest in the range (NOT a real
    /// parquet file — manifest-only).
    fn synthetic_position_delete_file(path: &str, part_value: i64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(part_value))]))
            .build()
            .unwrap()
    }

    /// Collect the changelog tasks a scan plans (path, operation, ordinal, commit id).
    async fn changelog_tasks(scan: &super::IncrementalChangelogScan) -> Vec<ChangelogScanTask> {
        scan.plan_files()
            .await
            .expect("plan_files should succeed")
            .try_collect()
            .await
            .expect("collecting changelog tasks should succeed")
    }

    /// Index the changelog tasks by data-file path for per-file assertions.
    fn by_path(tasks: &[ChangelogScanTask]) -> HashMap<String, &ChangelogScanTask> {
        tasks
            .iter()
            .map(|task| (task.data_file_path().to_string(), task))
            .collect()
    }

    /// CORE: a range with 2 APPEND snapshots yields INSERT tasks with the OLDEST snapshot's
    /// files at ordinal 0 and the next at ordinal 1, each `commit_snapshot_id` = the adding
    /// snapshot. Mutation-pins the ordinal scheme (oldest → 0) and the commit-id stamping:
    /// reversing the ordinal order (newest = 0) flips both files' ordinals and fails this.
    #[tokio::test]
    async fn test_changelog_two_appends_assigns_ordinals_oldest_first() {
        let catalog = new_memory_catalog().await;
        let table = make_minimal_table(&catalog).await;

        let table = append_files(&catalog, &table, vec![data_file("base.parquet", 1)]).await;
        let s0 = table.metadata().current_snapshot_id().unwrap();
        let table = append_files(&catalog, &table, vec![data_file("a.parquet", 1)]).await;
        let s1 = table.metadata().current_snapshot_id().unwrap();
        let table = append_files(&catalog, &table, vec![data_file("b.parquet", 1)]).await;
        let s2 = table.metadata().current_snapshot_id().unwrap();

        let scan = table
            .incremental_changelog_scan()
            .from_snapshot_id_exclusive(s0)
            .to_snapshot_id(s2)
            .build()
            .unwrap();

        let tasks = changelog_tasks(&scan).await;
        let by_path = by_path(&tasks);
        assert_eq!(
            by_path.keys().cloned().collect::<HashSet<_>>(),
            HashSet::from(["a.parquet".to_string(), "b.parquet".to_string()]),
            "only the two appends after S0 are in the changelog"
        );

        let task_a = by_path["a.parquet"];
        assert_eq!(task_a.operation(), ChangelogOperation::Insert);
        assert_eq!(
            task_a.change_ordinal(),
            0,
            "S1 is the oldest in range → ordinal 0"
        );
        assert_eq!(task_a.commit_snapshot_id(), s1);

        let task_b = by_path["b.parquet"];
        assert_eq!(task_b.operation(), ChangelogOperation::Insert);
        assert_eq!(task_b.change_ordinal(), 1, "S2 follows S1 → ordinal 1");
        assert_eq!(task_b.commit_snapshot_id(), s2);
    }

    /// DELETE OPERATION: a snapshot that REMOVES a live data file (an overwrite writing a
    /// `Deleted` manifest ENTRY into its own rewritten manifest, NOT a delete-FILE manifest)
    /// produces a DELETE changelog task for that file, with the added file as an INSERT in
    /// the same snapshot. Mutation-pins the Deleted→Delete mapping: mapping Deleted→Insert
    /// makes the removed file's task assert-fail on operation.
    #[tokio::test]
    async fn test_changelog_overwrite_emits_delete_for_removed_file() {
        let catalog = new_memory_catalog().await;
        let table = make_minimal_table(&catalog).await;

        // S0 appends a + b; S1 overwrites: delete a, add c.
        let table = append_files(&catalog, &table, vec![
            data_file("a.parquet", 1),
            data_file("b.parquet", 1),
        ])
        .await;
        let s0 = table.metadata().current_snapshot_id().unwrap();

        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .delete_file("a.parquet")
            .add_file(data_file("c.parquet", 1));
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let s1 = table.metadata().current_snapshot_id().unwrap();

        let scan = table
            .incremental_changelog_scan()
            .from_snapshot_id_exclusive(s0)
            .to_snapshot_id(s1)
            .build()
            .unwrap();

        let tasks = changelog_tasks(&scan).await;
        let by_path = by_path(&tasks);

        // The overwrite removed `a` (DELETE) and added `c` (INSERT); `b` is untouched and
        // not part of this range's changelog.
        assert_eq!(
            by_path.keys().cloned().collect::<HashSet<_>>(),
            HashSet::from(["a.parquet".to_string(), "c.parquet".to_string()]),
            "the overwrite's removed (a) + added (c) files are the changelog; b is untouched"
        );

        let deleted = by_path["a.parquet"];
        assert_eq!(
            deleted.operation(),
            ChangelogOperation::Delete,
            "the removed file a is a DELETE change"
        );
        assert_eq!(deleted.commit_snapshot_id(), s1);
        assert_eq!(deleted.change_ordinal(), 0);

        let added = by_path["c.parquet"];
        assert_eq!(
            added.operation(),
            ChangelogOperation::Insert,
            "the added file c is an INSERT change"
        );
        assert_eq!(added.commit_snapshot_id(), s1);
    }

    /// REPLACE EXCLUSION: a `RewriteFiles` (compaction) snapshot in the range produces an
    /// `Operation::Replace` snapshot, which the changelog scan EXCLUDES (it rewrites files
    /// without changing rows). After S0 appends a + b, S1 rewrites {a,b}→{c}. The (S0, S1]
    /// changelog is EMPTY. Mutation-pins the Replace exclusion: including Replace snapshots
    /// would surface c (and a/b as deletes), making the changelog non-empty.
    #[tokio::test]
    async fn test_changelog_excludes_replace_snapshot() {
        let catalog = new_memory_catalog().await;
        let table = make_minimal_table(&catalog).await;

        let file_a = data_file("a.parquet", 1);
        let file_b = data_file("b.parquet", 1);
        let table = append_files(&catalog, &table, vec![file_a.clone(), file_b.clone()]).await;
        let s0 = table.metadata().current_snapshot_id().unwrap();

        // Compaction: replace {a, b} with {c} → Operation::Replace.
        let tx = Transaction::new(&table);
        let action = tx.rewrite_files(vec![file_a, file_b], vec![data_file("c.parquet", 1)]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let s1 = table.metadata().current_snapshot_id().unwrap();

        assert_eq!(
            table
                .metadata()
                .snapshot_by_id(s1)
                .unwrap()
                .summary()
                .operation,
            Operation::Replace,
            "the rewrite must be a Replace for this test to be meaningful"
        );

        let scan = table
            .incremental_changelog_scan()
            .from_snapshot_id_exclusive(s0)
            .to_snapshot_id(s1)
            .build()
            .unwrap();

        let tasks = changelog_tasks(&scan).await;
        assert!(
            tasks.is_empty(),
            "a Replace (compaction) snapshot contributes no changelog tasks, got: {:?}",
            tasks.iter().map(|t| t.data_file_path()).collect::<Vec<_>>()
        );
    }

    /// DELETE-MANIFEST GUARD: a range whose snapshots reference a row-level DELETE manifest
    /// (here a `RowDelta` adding a position-delete file) is rejected with
    /// `FeatureUnsupported` — matching Java's current data-file-changelog limitation.
    /// Mutation-pins the guard: dropping the delete-manifest check lets `plan_files` proceed
    /// (returning Ok), failing this assertion.
    #[tokio::test]
    async fn test_changelog_rejects_range_with_delete_manifest() {
        let catalog = new_memory_catalog().await;
        let table = make_minimal_table(&catalog).await;

        let table = append_files(&catalog, &table, vec![data_file("a.parquet", 1)]).await;
        let s0 = table.metadata().current_snapshot_id().unwrap();

        // S1 adds a position-delete file → its manifest list carries a DELETE manifest.
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_position_delete_file("a-pos-del.parquet", 1)]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let s1 = table.metadata().current_snapshot_id().unwrap();

        let scan = table
            .incremental_changelog_scan()
            .from_snapshot_id_exclusive(s0)
            .to_snapshot_id(s1)
            .build()
            .unwrap();

        let result = scan.plan_files().await;
        let error = result
            .err()
            .expect("a range with a delete manifest must error");
        assert_eq!(
            error.kind(),
            ErrorKind::FeatureUnsupported,
            "a delete-manifest range is FeatureUnsupported, got: {error}"
        );
    }

    /// FILTER PRUNES BY PARTITION: a `with_filter(x == 10)` over the identity(x)-partitioned
    /// table prunes the appended file in partition x = 20 from the changelog, keeping x = 10.
    #[tokio::test]
    async fn test_changelog_with_filter_prunes_by_partition() {
        let catalog = new_memory_catalog().await;
        let table = make_minimal_table(&catalog).await;

        let table = append_files(&catalog, &table, vec![data_file("base.parquet", 1)]).await;
        let s0 = table.metadata().current_snapshot_id().unwrap();
        let table = append_files(&catalog, &table, vec![
            data_file("x10.parquet", 10),
            data_file("x20.parquet", 20),
        ])
        .await;
        let s1 = table.metadata().current_snapshot_id().unwrap();

        let scan = table
            .incremental_changelog_scan()
            .from_snapshot_id_exclusive(s0)
            .to_snapshot_id(s1)
            .with_filter(Reference::new("x").equal_to(Datum::long(10)))
            .build()
            .unwrap();

        let tasks = changelog_tasks(&scan).await;
        let paths: HashSet<String> = tasks
            .iter()
            .map(|task| task.data_file_path().to_string())
            .collect();
        assert_eq!(
            paths,
            HashSet::from(["x10.parquet".to_string()]),
            "filter x == 10 must prune the x = 20 appended file from the changelog"
        );
    }

    /// FROM == TO (inclusive): an inclusive `from` equal to `to` resolves to the range
    /// `(to's parent, to]`, which is just `to` itself — its own changes. But `from == to`
    /// EXCLUSIVE is the natural empty case; here we assert the explicit empty range via a
    /// `to` that has no changes after an identical exclusive `from`. We use the
    /// Java-reachable empty case: `from == to` exclusive resolves to an empty changelog.
    #[tokio::test]
    async fn test_changelog_from_equals_to_inclusive_is_only_to_change() {
        let catalog = new_memory_catalog().await;
        let table = make_minimal_table(&catalog).await;

        let table = append_files(&catalog, &table, vec![data_file("s0.parquet", 1)]).await;
        let table = append_files(&catalog, &table, vec![data_file("s1.parquet", 1)]).await;
        let s1 = table.metadata().current_snapshot_id().unwrap();

        // Inclusive from == to == S1: the range is just S1's own change (s1.parquet).
        let scan = table
            .incremental_changelog_scan()
            .from_snapshot_id_inclusive(s1)
            .to_snapshot_id(s1)
            .build()
            .unwrap();

        let tasks = changelog_tasks(&scan).await;
        let paths: HashSet<String> = tasks
            .iter()
            .map(|task| task.data_file_path().to_string())
            .collect();
        assert_eq!(
            paths,
            HashSet::from(["s1.parquet".to_string()]),
            "inclusive from == to returns only that snapshot's own change"
        );
        assert_eq!(
            tasks[0].change_ordinal(),
            0,
            "the single snapshot is ordinal 0"
        );
    }

    /// EMPTY RANGE: `from == to` EXCLUSIVE is rejected by the underlying append scan's
    /// `isParentAncestorOf` precondition (a snapshot is not its own parent ancestor), so the
    /// builder errors — Java-faithful. (The reachable runtime-empty case is the Replace-only
    /// range above.)
    #[tokio::test]
    async fn test_changelog_from_equals_to_exclusive_is_rejected() {
        let catalog = new_memory_catalog().await;
        let table = make_minimal_table(&catalog).await;

        let table = append_files(&catalog, &table, vec![data_file("s0.parquet", 1)]).await;
        let table = append_files(&catalog, &table, vec![data_file("s1.parquet", 1)]).await;
        let s1 = table.metadata().current_snapshot_id().unwrap();

        let result = table
            .incremental_changelog_scan()
            .from_snapshot_id_exclusive(s1)
            .to_snapshot_id(s1)
            .build();
        assert!(
            result.is_err(),
            "from == to (exclusive) must be rejected: a snapshot is not its own parent ancestor"
        );
    }

    /// CARRIED-FORWARD ENTRY (ordinal correctness across snapshots): a file appended in an
    /// OLD snapshot must NOT re-appear in a LATER snapshot's changelog. After S0 appends a,
    /// S1 appends b: the (S0, S1] changelog must contain ONLY b at ordinal 0 — a was added
    /// before the range. Pins that only a snapshot's OWN added manifests are read (a's
    /// manifest is carried forward into S1's list but belongs to S0).
    #[tokio::test]
    async fn test_changelog_only_reads_snapshots_own_added_manifests() {
        let catalog = new_memory_catalog().await;
        let table = make_minimal_table(&catalog).await;

        let table = append_files(&catalog, &table, vec![data_file("a.parquet", 1)]).await;
        let s0 = table.metadata().current_snapshot_id().unwrap();
        let table = append_files(&catalog, &table, vec![data_file("b.parquet", 1)]).await;
        let s1 = table.metadata().current_snapshot_id().unwrap();

        let scan = table
            .incremental_changelog_scan()
            .from_snapshot_id_exclusive(s0)
            .to_snapshot_id(s1)
            .build()
            .unwrap();

        let tasks = changelog_tasks(&scan).await;
        assert_eq!(
            tasks.len(),
            1,
            "only S1's own added file b is in the (S0, S1] changelog"
        );
        assert_eq!(tasks[0].data_file_path(), "b.parquet");
        assert_eq!(tasks[0].operation(), ChangelogOperation::Insert);
        assert_eq!(tasks[0].change_ordinal(), 0);
        assert_eq!(tasks[0].commit_snapshot_id(), s1);
    }
}
