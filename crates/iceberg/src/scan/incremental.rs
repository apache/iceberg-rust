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

//! Incremental append scan for reading only newly added data between snapshots.

use std::collections::HashSet;
use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};

use crate::arrow::ArrowReaderBuilder;
use crate::expr::Predicate;
use crate::io::FileIO;
use crate::runtime::Runtime;
use crate::scan::context::ManifestEntryFilter;
use crate::scan::{
    ArrowRecordBatchStream, ExpressionEvaluatorCache, FileScanTaskStream, ManifestEvaluatorCache,
    PartitionFilterCache, ScanPlanningContext, bind_scan_predicate, plan_scan_files,
    projected_field_ids, projected_partition_type, table_name_mapping,
};
use crate::spec::{
    ManifestContentType, ManifestFile, ManifestList, ManifestStatus, Operation, SnapshotRef,
    TableMetadataRef,
};
use crate::table::Table;
use crate::util::available_parallelism;
use crate::util::snapshot::ancestors_between;
use crate::{Error, ErrorKind, Result};

/// A validated range of snapshots for incremental scanning.
///
/// Holds the APPEND snapshots of the range: their manifest lists are the scan's
/// manifest source, and their IDs select which manifests and entries it keeps.
#[derive(Debug, Clone)]
pub(crate) struct AppendRange {
    /// Newest first.
    snapshots: Vec<SnapshotRef>,
}

impl AppendRange {
    pub(crate) fn build(
        table_metadata: &TableMetadataRef,
        from_snapshot_id: Option<i64>,
        to_snapshot_id: i64,
        from_inclusive: bool,
    ) -> Result<Self> {
        // Determine the exclusive stop point for the ancestry walk.
        // Without a from-snapshot the walk runs to the root, so the range starts at
        // the oldest ancestor of the to-snapshot, inclusive.
        // For inclusive mode the from-snapshot must exist so we can look up
        // its parent. For exclusive mode the snapshot may have been expired
        // (the parent pointer on its child still references it), so we only
        // need the ID — matching Java's BaseIncrementalScan semantics.
        let oldest_exclusive = match from_snapshot_id {
            None => None,
            Some(from_snapshot_id) if from_inclusive => {
                let from_snapshot =
                    table_metadata
                        .snapshot_by_id(from_snapshot_id)
                        .ok_or_else(|| {
                            Error::new(
                                ErrorKind::DataInvalid,
                                format!("Snapshot {from_snapshot_id} not found"),
                            )
                        })?;
                from_snapshot.parent_snapshot_id()
            }
            Some(from_snapshot_id) => Some(from_snapshot_id),
        };

        let snapshots: Vec<_> =
            ancestors_between(table_metadata, to_snapshot_id, oldest_exclusive).collect();

        // Verify the walk actually reached from_snapshot_id: ancestors_between
        // silently returns the whole chain to the root when oldest_exclusive is
        // not in the ancestry. Without a from-snapshot there is nothing to
        // verify — walking to the root is the intent.
        //
        // This mirrors the preconditions in Java's BaseIncrementalScan. Inclusive
        // mode requires from to be an ancestor of to, and a snapshot is its own
        // ancestor, so `from == to` is a valid single-snapshot range. Exclusive
        // mode requires an ancestor of to whose parent is from, which `from == to`
        // can never satisfy — the walk stops immediately and yields nothing.
        if let Some(from_snapshot_id) = from_snapshot_id {
            let connects = snapshots.last().is_some_and(|oldest| {
                if from_inclusive {
                    oldest.snapshot_id() == from_snapshot_id
                } else {
                    oldest.parent_snapshot_id() == Some(from_snapshot_id)
                }
            });

            if !connects {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    if from_inclusive {
                        format!(
                            "Starting snapshot (inclusive) {from_snapshot_id} is not an ancestor of end snapshot {to_snapshot_id}"
                        )
                    } else {
                        format!(
                            "Starting snapshot (exclusive) {from_snapshot_id} is not a parent ancestor of end snapshot {to_snapshot_id}"
                        )
                    },
                ));
            }
        }

        // Keep only APPEND snapshots, silently skipping the rest (replace, overwrite,
        // delete)
        let snapshots = snapshots
            .into_iter()
            .filter(|snapshot| snapshot.summary().operation == Operation::Append)
            .collect();

        Ok(Self { snapshots })
    }

    /// The APPEND snapshots in the range, newest first.
    pub(crate) fn snapshots(&self) -> &[SnapshotRef] {
        &self.snapshots
    }

    fn snapshot_ids(&self) -> HashSet<i64> {
        self.snapshots
            .iter()
            .map(|snapshot| snapshot.snapshot_id())
            .collect()
    }

    fn manifest_files(&self, manifest_lists: &[Arc<ManifestList>]) -> Vec<ManifestFile> {
        let snapshot_ids = self.snapshot_ids();
        let mut seen = HashSet::new();

        manifest_lists
            .iter()
            .flat_map(|manifest_list| manifest_list.entries())
            .filter(|manifest_file| {
                manifest_file.content != ManifestContentType::Deletes
                    && snapshot_ids.contains(&manifest_file.added_snapshot_id)
            })
            .filter(|manifest_file| seen.insert(manifest_file.manifest_path.clone()))
            .cloned()
            .collect()
    }

    /// Create a manifest entry filter that includes only entries with
    /// status ADDED and a snapshot_id within this range.
    pub(crate) fn manifest_entry_filter(&self) -> ManifestEntryFilter {
        let snapshot_ids = self.snapshot_ids();
        Arc::new(move |entry| {
            entry.status() == ManifestStatus::Added
                && entry
                    .snapshot_id()
                    .is_some_and(|id| snapshot_ids.contains(&id))
        })
    }
}

#[derive(Debug)]
struct IncrementalAppendPlanContext {
    append_range: AppendRange,
    scan_context: ScanPlanningContext,
}

impl IncrementalAppendPlanContext {
    async fn manifest_files(&self, concurrency_limit: usize) -> Result<Vec<ManifestFile>> {
        let object_cache = self.scan_context.object_cache.clone();
        let table_metadata = self.scan_context.table_metadata.clone();
        let manifest_lists: Vec<Arc<ManifestList>> =
            futures::stream::iter(self.append_range.snapshots().iter().cloned())
                .map(move |snapshot| {
                    let object_cache = object_cache.clone();
                    let table_metadata = table_metadata.clone();
                    async move {
                        object_cache
                            .get_manifest_list(&snapshot, &table_metadata)
                            .await
                    }
                })
                .buffered(concurrency_limit.max(1))
                .try_collect()
                .await?;

        Ok(self.append_range.manifest_files(&manifest_lists))
    }

    fn manifest_entry_filter(&self) -> ManifestEntryFilter {
        self.append_range.manifest_entry_filter()
    }
}

/// An incremental scan of data appended between two snapshots.
#[derive(Debug)]
pub struct IncrementalAppendScan {
    plan_context: IncrementalAppendPlanContext,
    batch_size: Option<usize>,
    file_io: FileIO,
    column_names: Option<Vec<String>>,
    concurrency_limit_manifest_files: usize,
    concurrency_limit_manifest_entries: usize,
    concurrency_limit_data_files: usize,
    row_group_filtering_enabled: bool,
    row_selection_enabled: bool,
    runtime: Runtime,
}

impl IncrementalAppendScan {
    /// Returns a stream of files appended in the scan's snapshot range.
    pub async fn plan_files(&self) -> Result<FileScanTaskStream> {
        let manifest_files = self
            .plan_context
            .manifest_files(self.concurrency_limit_manifest_files)
            .await?;

        plan_scan_files(
            &self.plan_context.scan_context,
            manifest_files,
            Some(self.plan_context.manifest_entry_filter()),
            &self.runtime,
            self.concurrency_limit_manifest_files,
            self.concurrency_limit_manifest_entries,
        )
        .await
    }

    /// Returns an [`ArrowRecordBatchStream`].
    pub async fn to_arrow(&self) -> Result<ArrowRecordBatchStream> {
        let mut arrow_reader_builder =
            ArrowReaderBuilder::new(self.file_io.clone(), self.runtime.clone())
                .with_data_file_concurrency_limit(self.concurrency_limit_data_files)
                .with_row_group_filtering_enabled(self.row_group_filtering_enabled)
                .with_row_selection_enabled(self.row_selection_enabled);

        if let Some(batch_size) = self.batch_size {
            arrow_reader_builder = arrow_reader_builder.with_batch_size(batch_size);
        }

        arrow_reader_builder
            .build()
            .read(self.plan_files().await?)
            .map(|result| result.stream())
    }

    /// Returns the selected column names.
    pub fn column_names(&self) -> Option<&[String]> {
        self.column_names.as_deref()
    }
}

/// Builder to create an incremental append scan between two snapshots.
///
/// An incremental append scan returns only data files that were added in
/// snapshots between `from_snapshot_id` and the target snapshot. Only
/// snapshots with APPEND operations are supported.
///
/// This is **not** a CDC, net-changes, or changelog scan: non-append
/// snapshots in the range (overwrite, replace/compaction, delete) are
/// ignored rather than applied as net changes. The scan reads only the rows
/// added by append snapshots in the range, so its output does not represent
/// the full table state at `to_snapshot_id`, nor does it reflect rows deleted
/// or rewritten within the range. In particular, files produced by compaction
/// (`replace`) are skipped, so appended rows are never double-counted against
/// their rewritten copies.
///
/// Use [`Table::incremental_append_scan`] or
/// [`Table::incremental_append_scan_inclusive`] to create an instance.
pub struct IncrementalAppendScanBuilder<'a> {
    table: &'a Table,
    from_snapshot_id: Option<i64>,
    from_inclusive: bool,
    to_snapshot_id: Option<i64>,
    column_names: Option<Vec<String>>,
    batch_size: Option<usize>,
    case_sensitive: bool,
    filter: Option<Predicate>,
    concurrency_limit_data_files: usize,
    concurrency_limit_manifest_entries: usize,
    concurrency_limit_manifest_files: usize,
    row_group_filtering_enabled: bool,
    row_selection_enabled: bool,
}

impl<'a> IncrementalAppendScanBuilder<'a> {
    pub(crate) fn new(
        table: &'a Table,
        from_snapshot_id: Option<i64>,
        to_snapshot_id: Option<i64>,
        from_inclusive: bool,
    ) -> Self {
        let num_cpus = available_parallelism().get();

        Self {
            table,
            from_snapshot_id,
            from_inclusive,
            to_snapshot_id,
            column_names: None,
            batch_size: None,
            case_sensitive: true,
            filter: None,
            concurrency_limit_data_files: num_cpus,
            concurrency_limit_manifest_entries: num_cpus,
            concurrency_limit_manifest_files: num_cpus,
            row_group_filtering_enabled: true,
            row_selection_enabled: false,
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

    /// Sets the concurrency limit for manifest files, manifest entries, and
    /// data files for this scan
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
    pub fn with_row_group_filtering_enabled(mut self, row_group_filtering_enabled: bool) -> Self {
        self.row_group_filtering_enabled = row_group_filtering_enabled;
        self
    }

    /// Determines whether to enable row selection.
    pub fn with_row_selection_enabled(mut self, row_selection_enabled: bool) -> Self {
        self.row_selection_enabled = row_selection_enabled;
        self
    }

    /// Build the incremental append scan.
    pub fn build(self) -> Result<IncrementalAppendScan> {
        let to_snapshot_id = match self.to_snapshot_id {
            Some(snapshot_id) => self
                .table
                .metadata()
                .snapshot_by_id(snapshot_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("to_snapshot with id {snapshot_id} not found"),
                    )
                })?
                .snapshot_id(),
            None => {
                let Some(current_snapshot) = self.table.metadata().current_snapshot() else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "Cannot perform incremental scan: table has no snapshots",
                    ));
                };
                current_snapshot.snapshot_id()
            }
        };

        let append_range = AppendRange::build(
            &self.table.metadata_ref(),
            self.from_snapshot_id,
            to_snapshot_id,
            self.from_inclusive,
        )?;

        let schema = self.table.metadata().current_schema().clone();
        let field_ids =
            projected_field_ids(&schema, self.column_names.as_deref(), self.case_sensitive)?;
        let scan_bound_predicate =
            bind_scan_predicate(&schema, self.filter.as_ref(), self.case_sensitive)?;
        let name_mapping = table_name_mapping(self.table)?;
        let unified_partition_type = projected_partition_type(self.table, &schema, &field_ids)?;

        let scan_context = ScanPlanningContext {
            table_metadata: self.table.metadata_ref(),
            scan_schema: schema,
            case_sensitive: self.case_sensitive,
            predicate: self.filter.map(Arc::new),
            scan_bound_predicate,
            object_cache: self.table.object_cache(),
            field_ids: Arc::new(field_ids),
            name_mapping,
            partition_filter_cache: Arc::new(PartitionFilterCache::new()),
            manifest_evaluator_cache: Arc::new(ManifestEvaluatorCache::new()),
            expression_evaluator_cache: Arc::new(ExpressionEvaluatorCache::new()),
            unified_partition_type,
        };
        let plan_context = IncrementalAppendPlanContext {
            append_range,
            scan_context,
        };

        Ok(IncrementalAppendScan {
            plan_context,
            batch_size: self.batch_size,
            file_io: self.table.file_io().clone(),
            column_names: self.column_names,
            concurrency_limit_manifest_files: self.concurrency_limit_manifest_files,
            concurrency_limit_manifest_entries: self.concurrency_limit_manifest_entries,
            concurrency_limit_data_files: self.concurrency_limit_data_files,
            row_group_filtering_enabled: self.row_group_filtering_enabled,
            row_selection_enabled: self.row_selection_enabled,
            runtime: self.table.runtime().clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;

    use super::{AppendRange, IncrementalAppendScan};
    use crate::scan::test_utils::TableTestFixture;

    /// Sorted base names of the data files `scan` yields. Duplicates are kept so
    /// double-counting is visible.
    async fn planned_file_names(scan: &IncrementalAppendScan) -> Vec<String> {
        let tasks: Vec<_> = scan
            .plan_files()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        let mut names: Vec<String> = tasks
            .iter()
            .map(|task| {
                task.data_file_path
                    .rsplit('/')
                    .next()
                    .unwrap_or(&task.data_file_path)
                    .to_string()
            })
            .collect();
        names.sort();
        names
    }

    #[test]
    fn test_incremental_scan_invalid_from_snapshot_exclusive() {
        let table = TableTestFixture::new().table;

        // Exclusive mode doesn't require from-snapshot to exist, but it must
        // be an ancestor of the to-snapshot. 999999999 is not in the ancestry
        // chain so this should fail.
        let result = table.incremental_append_scan(Some(999999999), None).build();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("is not a parent ancestor"),
            "Expected ancestry error, got: {err}"
        );
    }

    #[test]
    fn test_incremental_scan_invalid_from_snapshot_inclusive() {
        let table = TableTestFixture::new().table;

        // Inclusive mode requires from-snapshot to exist (we need its parent ID).
        let result = table
            .incremental_append_scan_inclusive(Some(999999999), None)
            .build();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("not found"),
            "Expected 'not found' error, got: {err}"
        );
    }

    #[test]
    fn test_incremental_scan_exclusive_from_expired_snapshot() {
        // Fixture has S1 (append) -> S2 (append, current).
        // Simulate S1 being expired: use S1's ID as from-snapshot in exclusive
        // mode even though it wouldn't exist in metadata after expiration.
        // Since exclusive mode only needs the ID (not the snapshot object),
        // this should succeed — the child (S2) still has parent_snapshot_id = S1.
        let table = TableTestFixture::new().table;

        let s1_id = 3051729675574597004_i64;
        let s2_id = 3055729675574597004_i64;

        // Verify S2's parent is S1 (simulating the expired-parent scenario)
        assert_eq!(
            table
                .metadata()
                .snapshot_by_id(s2_id)
                .unwrap()
                .parent_snapshot_id(),
            Some(s1_id)
        );

        let result = table
            .incremental_append_scan(Some(s1_id), Some(s2_id))
            .build();

        assert!(
            result.is_ok(),
            "Exclusive scan from an (effectively expired) parent should succeed"
        );
    }

    #[test]
    fn test_incremental_scan_invalid_to_snapshot() {
        let table = TableTestFixture::new().table;

        let result = table
            .incremental_append_scan(Some(3051729675574597004), Some(999999999))
            .build();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_incremental_scan_appends_after() {
        // Fixture has S1 (append) -> S2 (append, current)
        let table = TableTestFixture::new().table;

        let result = table
            .incremental_append_scan(Some(3051729675574597004), None)
            .build();
        assert!(
            result.is_ok(),
            "appends_after should succeed when all snapshots are appends"
        );

        let scan = result.unwrap();
        assert_eq!(scan.plan_context.append_range.snapshots().len(), 1);
    }

    #[test]
    fn test_incremental_scan_appends_between() {
        // Fixture has S1 (append) -> S2 (append, current)
        let table = TableTestFixture::new().table;

        let current_snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();
        let parent_id = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .parent_snapshot_id()
            .expect("Current snapshot should have a parent");

        let result = table
            .incremental_append_scan(Some(parent_id), Some(current_snapshot_id))
            .build();

        assert!(
            result.is_ok(),
            "appends_between should succeed for two append snapshots"
        );
    }

    #[test]
    fn test_incremental_scan_from_snapshot_inclusive() {
        // Fixture has S1 (append) -> S2 (append, current)
        let table = TableTestFixture::new().table;
        let current_snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();

        // Verify the scan builds successfully
        let result = table
            .incremental_append_scan_inclusive(Some(current_snapshot_id), Some(current_snapshot_id))
            .build();
        assert!(
            result.is_ok(),
            "Inclusive scan of a single append snapshot should succeed"
        );

        // Verify AppendRange directly
        let set = AppendRange::build(
            &table.metadata_ref(),
            Some(current_snapshot_id),
            current_snapshot_id,
            true,
        )
        .unwrap();
        assert!(
            set.snapshot_ids().contains(&current_snapshot_id),
            "Inclusive set should contain the from_snapshot"
        );
    }

    #[test]
    fn test_incremental_scan_from_snapshot_exclusive() {
        // Fixture has S1 (append) -> S2 (append, current)
        let table = TableTestFixture::new().table;
        let parent_id = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .parent_snapshot_id()
            .unwrap();
        let current_snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();

        // The from-snapshot itself is excluded from the range.
        let range = AppendRange::build(
            &table.metadata_ref(),
            Some(parent_id),
            current_snapshot_id,
            false,
        )
        .unwrap();
        assert!(
            !range.snapshot_ids().contains(&parent_id),
            "Exclusive range should not contain the from_snapshot"
        );
        assert!(
            range.snapshot_ids().contains(&current_snapshot_id),
            "Exclusive range should contain the to_snapshot"
        );
    }

    #[test]
    fn test_incremental_scan_exclusive_same_snapshot_is_rejected() {
        // Java requires an ancestor of the to-snapshot whose parent is the
        // from-snapshot, which from == to can never satisfy, so it rejects this
        // rather than returning an empty range. Match that.
        let table = TableTestFixture::new().table;
        let current_snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();

        let err = table
            .incremental_append_scan(Some(current_snapshot_id), Some(current_snapshot_id))
            .build()
            .expect_err("exclusive scan from == to should be rejected");

        assert!(
            err.to_string().contains("is not a parent ancestor"),
            "Expected parent-ancestor error, got: {err}"
        );
    }

    #[test]
    fn test_incremental_scan_inclusive_same_snapshot_is_allowed() {
        // A snapshot is its own ancestor, so inclusive from == to is a valid
        // single-snapshot range in Java. Match that too.
        let table = TableTestFixture::new().table;
        let current_snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();

        let range = AppendRange::build(
            &table.metadata_ref(),
            Some(current_snapshot_id),
            current_snapshot_id,
            true,
        )
        .unwrap();

        assert_eq!(
            range.snapshots().len(),
            1,
            "inclusive from == to should yield exactly the one snapshot"
        );
        assert!(range.snapshot_ids().contains(&current_snapshot_id));
    }

    #[test]
    fn test_incremental_scan_skips_non_append_operations() {
        // Deep history fixture: S1 (append) -> S2 (append) -> S3 (append)
        //   -> S4 (overwrite) -> S5 (append, current)
        let table = TableTestFixture::new_with_deep_history().table;

        // Scanning from S1 to S5 crosses S4 (overwrite) — should succeed
        // but only include APPEND snapshots (S2, S3, S5), skipping S4
        let result = table
            .incremental_append_scan(Some(3051729675574597004), Some(3059729675574597004))
            .build();

        assert!(
            result.is_ok(),
            "Should succeed, skipping non-APPEND snapshots"
        );

        let set = AppendRange::build(
            &table.metadata_ref(),
            Some(3051729675574597004),
            3059729675574597004,
            false,
        )
        .unwrap();
        assert!(
            !set.snapshot_ids().contains(&3051729675574597004),
            "S1 (from) should be excluded"
        );
        assert!(
            set.snapshot_ids().contains(&3055729675574597004),
            "S2 (append) should be in set"
        );
        assert!(
            set.snapshot_ids().contains(&3056729675574597004),
            "S3 (append) should be in set"
        );
        assert!(
            !set.snapshot_ids().contains(&3057729675574597004),
            "S4 (overwrite) should be skipped"
        );
        assert!(
            set.snapshot_ids().contains(&3059729675574597004),
            "S5 (append) should be in set"
        );
    }

    #[test]
    fn test_incremental_scan_append_only_range() {
        // Deep history fixture: S1 (append) -> S2 (append) -> S3 (append)
        //   -> S4 (overwrite) -> S5 (append, current)
        let table = TableTestFixture::new_with_deep_history().table;

        // Scanning from S1 to S3 (all appends)
        let set = AppendRange::build(
            &table.metadata_ref(),
            Some(3051729675574597004),
            3056729675574597004,
            false,
        )
        .unwrap();
        assert!(
            !set.snapshot_ids().contains(&3051729675574597004),
            "from_snapshot should be excluded"
        );
        assert!(
            set.snapshot_ids().contains(&3055729675574597004),
            "S2 should be in range"
        );
        assert!(
            set.snapshot_ids().contains(&3056729675574597004),
            "S3 should be in range"
        );
    }

    #[tokio::test]
    async fn test_incremental_scan_returns_only_added_files_in_range() {
        // Fixture has S1 (append) -> S2 (append, current)
        // Manifest contains:
        //   1.parquet: status=Added, snapshot=S2
        //   2.parquet: status=Deleted, snapshot=S1
        //   3.parquet: status=Existing, snapshot=S1
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        let current_snapshot = fixture.table.metadata().current_snapshot().unwrap();
        let parent_snapshot_id = current_snapshot.parent_snapshot_id().unwrap();

        // Incremental scan from S1 (exclusive) to S2 should return only 1.parquet
        let table_scan = fixture
            .table
            .incremental_append_scan(
                Some(parent_snapshot_id),
                Some(current_snapshot.snapshot_id()),
            )
            .build()
            .unwrap();

        let tasks: Vec<_> = table_scan
            .plan_files()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        assert_eq!(
            tasks.len(),
            1,
            "Incremental scan should return exactly 1 file"
        );
        assert_eq!(
            tasks[0].data_file_path,
            format!("{}/1.parquet", &fixture.table_location),
            "Should only return the file added in S2"
        );
    }

    #[tokio::test]
    async fn test_incremental_scan_range_without_appends_returns_empty() {
        // Deep history: S3 (append) -> S4 (overwrite). Scanning from S3 exclusive
        // to S4 is a valid range, but it contains no append snapshots, so there is
        // no manifest list to read and the plan must come back empty.
        let mut fixture = TableTestFixture::new_with_deep_history();
        fixture.setup_manifest_files_deep_history().await;

        let s3_id = 3056729675574597004_i64;
        let s4_id = 3057729675574597004_i64;

        let table_scan = fixture
            .table
            .incremental_append_scan(Some(s3_id), Some(s4_id))
            .build()
            .unwrap();

        let tasks: Vec<_> = table_scan
            .plan_files()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        assert!(
            tasks.is_empty(),
            "a range containing only non-append snapshots should return no files"
        );
    }

    #[tokio::test]
    async fn test_incremental_scan_compaction_not_double_counted() {
        // Compaction (`rewrite_data_files`) commits a `replace` snapshot whose
        // rewritten file re-adds rows that were already appended earlier. An
        // incremental append scan must skip that file so the appended rows are
        // read exactly once — never double-counted against the rewritten copy.
        //
        // Deep history fixture with S4 relabeled as a `replace` (compaction):
        //   S1 (append) -> S2 (append) -> S3 (append) -> S4 (replace) -> S5 (append, current)
        // Scanning from S1 (exclusive) to S5 should return only files from
        // APPEND snapshots: s2.parquet, s3.parquet, s5.parquet. The compacted
        // s4.parquet must be skipped.
        //
        // Uses the rewritten manifest layout: compaction also replaces the manifests
        // it compacts, so S4 re-emits s1/s2/s3 as EXISTING under a manifest it owns.
        let mut fixture = TableTestFixture::new_with_deep_history_compaction();
        fixture.setup_manifest_files_deep_history_rewritten().await;

        let s1_id = 3051729675574597004_i64;
        let s5_id = 3059729675574597004_i64;

        let scan = fixture
            .table
            .incremental_append_scan(Some(s1_id), Some(s5_id))
            .build()
            .unwrap();

        assert_eq!(
            planned_file_names(&scan).await,
            vec!["s2.parquet", "s3.parquet", "s5.parquet"],
            "Compacted file (s4, from the replace snapshot) must be skipped, and the \
             appends it rewrote must each be returned exactly once"
        );
    }

    #[tokio::test]
    async fn test_incremental_scan_without_from_snapshot_starts_at_oldest_ancestor() {
        // Deep history: S1 (append) -> S2 -> S3 -> S4 (overwrite) -> S5 (append)
        // Omitting from_snapshot_id must scan from the oldest ancestor inclusive,
        // so S1's file is included and only the overwrite's file is skipped.
        let mut fixture = TableTestFixture::new_with_deep_history();
        fixture.setup_manifest_files_deep_history().await;

        let s5_id = 3059729675574597004_i64;

        let scan = fixture
            .table
            .incremental_append_scan(None, Some(s5_id))
            .build()
            .unwrap();

        assert_eq!(
            planned_file_names(&scan).await,
            vec!["s1.parquet", "s2.parquet", "s3.parquet", "s5.parquet"],
            "a missing from_snapshot_id scans the whole ancestry, skipping non-appends"
        );
    }

    #[tokio::test]
    async fn test_incremental_scan_without_from_snapshot_ignores_inclusivity() {
        // With no from-snapshot there is nothing to include or exclude, so the
        // inclusive and exclusive entry points must agree.
        let mut fixture = TableTestFixture::new_with_deep_history();
        fixture.setup_manifest_files_deep_history().await;

        let s5_id = 3059729675574597004_i64;

        let exclusive = fixture
            .table
            .incremental_append_scan(None, Some(s5_id))
            .build()
            .unwrap();
        let inclusive = fixture
            .table
            .incremental_append_scan_inclusive(None, Some(s5_id))
            .build()
            .unwrap();

        assert_eq!(
            planned_file_names(&exclusive).await,
            planned_file_names(&inclusive).await
        );
    }

    #[test]
    fn test_incremental_scan_without_from_snapshot_defaults_to_current_snapshot() {
        // Neither end of the range set: the whole history up to the current snapshot.
        let table = TableTestFixture::new_with_deep_history().table;

        let range = AppendRange::build(&table.metadata_ref(), None, 3059729675574597004, false)
            .unwrap()
            .snapshot_ids();

        // Every append in the chain, but not the overwrite S4.
        assert!(range.contains(&3051729675574597004));
        assert!(range.contains(&3055729675574597004));
        assert!(range.contains(&3056729675574597004));
        assert!(!range.contains(&3057729675574597004));
        assert!(range.contains(&3059729675574597004));
    }

    #[tokio::test]
    async fn test_incremental_scan_deep_history_skips_overwrite_files() {
        // Deep history fixture:
        //   S1 (append) -> S2 (append) -> S3 (append) -> S4 (overwrite) -> S5 (append, current)
        // Each snapshot adds one file: s1.parquet .. s5.parquet
        //
        // Incremental scan from S1 (exclusive) to S5 should return only files
        // from APPEND snapshots: s2.parquet, s3.parquet, s5.parquet
        // s4.parquet (added in overwrite S4) must be skipped.
        let mut fixture = TableTestFixture::new_with_deep_history();
        fixture.setup_manifest_files_deep_history().await;

        let s1_id = 3051729675574597004_i64;
        let s5_id = 3059729675574597004_i64;

        let table_scan = fixture
            .table
            .incremental_append_scan(Some(s1_id), Some(s5_id))
            .build()
            .unwrap();

        let mut tasks: Vec<_> = table_scan
            .plan_files()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        // Sort by path for deterministic assertions
        tasks.sort_by(|a, b| a.data_file_path.cmp(&b.data_file_path));

        assert_eq!(
            tasks.len(),
            3,
            "Should return 3 files (s2, s3, s5), skipping s4 (overwrite)"
        );

        let file_names: Vec<&str> = tasks
            .iter()
            .map(|t| {
                t.data_file_path
                    .rsplit('/')
                    .next()
                    .unwrap_or(&t.data_file_path)
            })
            .collect();

        assert_eq!(
            file_names,
            vec!["s2.parquet", "s3.parquet", "s5.parquet"],
            "Only files from APPEND snapshots should be returned"
        );
    }

    #[tokio::test]
    async fn test_incremental_scan_deep_history_partial_range() {
        // Scan from S2 (exclusive) to S3 — both appends, should return only s3.parquet
        let mut fixture = TableTestFixture::new_with_deep_history();
        fixture.setup_manifest_files_deep_history().await;

        let s2_id = 3055729675574597004_i64;
        let s3_id = 3056729675574597004_i64;

        let table_scan = fixture
            .table
            .incremental_append_scan(Some(s2_id), Some(s3_id))
            .build()
            .unwrap();

        let tasks: Vec<_> = table_scan
            .plan_files()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        assert_eq!(tasks.len(), 1, "Should return exactly 1 file");
        assert!(
            tasks[0].data_file_path.ends_with("s3.parquet"),
            "Should return s3.parquet, got: {}",
            tasks[0].data_file_path
        );
    }

    #[test]
    fn test_incremental_scan_projects_onto_current_schema() {
        // The table's current schema (id 1) has three columns (x, y, z), but
        // every snapshot references the older schema (id 0) with a single
        // column (x). An incremental scan must project onto the *current*
        // schema, matching the Java and PyIceberg implementations, so rows
        // written under the older schema get NULLs for the newer columns.
        let table = TableTestFixture::new_with_deep_history_stale_schema().table;

        let s1_id = 3051729675574597004_i64;
        let s5_id = 3059729675574597004_i64;

        let scan = table
            .incremental_append_scan(Some(s1_id), Some(s5_id))
            .build()
            .unwrap();

        let scan_context = &scan.plan_context.scan_context;

        // The scan must use the current schema (3 columns), not the
        // to-snapshot's schema (1 column).
        let current_schema = table.metadata().current_schema();
        assert_eq!(
            scan_context.scan_schema.schema_id(),
            current_schema.schema_id(),
            "incremental scan should project onto the current schema"
        );
        assert_eq!(
            scan_context.scan_schema.as_struct().fields().len(),
            3,
            "current schema has three columns (x, y, z)"
        );

        // Sanity check: the to-snapshot itself references the older schema.
        let to_snapshot = table.metadata().snapshot_by_id(s5_id).unwrap();
        assert_eq!(
            to_snapshot.schema(table.metadata()).unwrap().schema_id(),
            0,
            "to-snapshot should reference the older single-column schema"
        );
    }

    #[tokio::test]
    async fn test_incremental_scan_merge_append_does_not_drop_earlier_appends() {
        // S2 merge-appends S1's manifest away: s1 survives only as an EXISTING entry
        // in a manifest S2 owns. Sourcing manifests from the to-snapshot's list alone
        // would drop it, losing a row appended inside the range.
        let mut fixture = TableTestFixture::new_with_deep_history();
        fixture.setup_manifest_files_deep_history_rewritten().await;

        let s1_id = 3051729675574597004_i64;
        let s2_id = 3055729675574597004_i64;

        let scan = fixture
            .table
            .incremental_append_scan_inclusive(Some(s1_id), Some(s2_id))
            .build()
            .unwrap();

        assert_eq!(
            planned_file_names(&scan).await,
            vec!["s1.parquet", "s2.parquet"],
            "both appends in the range must be returned even though S2 merged S1's manifest away"
        );
    }

    #[tokio::test]
    async fn test_incremental_scan_rewritten_manifests_do_not_drop_appends() {
        // S4 (overwrite) rewrites every live manifest into one it owns, re-emitting
        // s1/s2/s3 as EXISTING. Its added_snapshot_id is not an append, so the whole
        // manifest is filtered out and S2/S3's appends are reachable only through
        // their own manifest lists.
        let mut fixture = TableTestFixture::new_with_deep_history();
        fixture.setup_manifest_files_deep_history_rewritten().await;

        let s1_id = 3051729675574597004_i64;
        let s4_id = 3057729675574597004_i64;

        let scan = fixture
            .table
            .incremental_append_scan(Some(s1_id), Some(s4_id))
            .build()
            .unwrap();

        assert_eq!(
            planned_file_names(&scan).await,
            vec!["s2.parquet", "s3.parquet"],
            "appends rewritten into an overwrite's manifest must still be returned"
        );
    }

    #[tokio::test]
    async fn test_incremental_scan_rewritten_manifests_with_later_append() {
        // Same rewritten history, but scanning past the overwrite to S5. s4 came
        // from the overwrite and must stay excluded; s2, s3 and s5 are appends.
        let mut fixture = TableTestFixture::new_with_deep_history();
        fixture.setup_manifest_files_deep_history_rewritten().await;

        let s1_id = 3051729675574597004_i64;
        let s5_id = 3059729675574597004_i64;

        let scan = fixture
            .table
            .incremental_append_scan(Some(s1_id), Some(s5_id))
            .build()
            .unwrap();

        assert_eq!(
            planned_file_names(&scan).await,
            vec!["s2.parquet", "s3.parquet", "s5.parquet"],
            "appends across a manifest-rewriting overwrite must be returned exactly once"
        );
    }

    #[tokio::test]
    async fn test_incremental_scan_does_not_duplicate_carried_forward_manifests() {
        // S1's manifest appears in S1's, S2's and S3's lists. Reading every append
        // snapshot's list must not emit the same manifest — and rows — twice.
        let mut fixture = TableTestFixture::new_with_deep_history();
        fixture.setup_manifest_files_deep_history().await;

        let s1_id = 3051729675574597004_i64;
        let s3_id = 3056729675574597004_i64;

        let scan = fixture
            .table
            .incremental_append_scan_inclusive(Some(s1_id), Some(s3_id))
            .build()
            .unwrap();

        assert_eq!(
            planned_file_names(&scan).await,
            vec!["s1.parquet", "s2.parquet", "s3.parquet"],
            "each file must appear exactly once despite cumulative manifest lists"
        );
    }

    #[tokio::test]
    async fn test_incremental_scan_deep_history_inclusive_with_overwrite() {
        // Inclusive scan from S3 to S5:
        //   S3 (append) -> S4 (overwrite) -> S5 (append)
        // Should return s3.parquet and s5.parquet, skipping s4.parquet
        let mut fixture = TableTestFixture::new_with_deep_history();
        fixture.setup_manifest_files_deep_history().await;

        let s3_id = 3056729675574597004_i64;
        let s5_id = 3059729675574597004_i64;

        let table_scan = fixture
            .table
            .incremental_append_scan_inclusive(Some(s3_id), Some(s5_id))
            .build()
            .unwrap();

        let mut tasks: Vec<_> = table_scan
            .plan_files()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        tasks.sort_by(|a, b| a.data_file_path.cmp(&b.data_file_path));

        assert_eq!(
            tasks.len(),
            2,
            "Should return 2 files (s3, s5), skipping s4 (overwrite)"
        );

        let file_names: Vec<&str> = tasks
            .iter()
            .map(|t| {
                t.data_file_path
                    .rsplit('/')
                    .next()
                    .unwrap_or(&t.data_file_path)
            })
            .collect();

        assert_eq!(
            file_names,
            vec!["s3.parquet", "s5.parquet"],
            "Only files from APPEND snapshots should be returned"
        );
    }
}
