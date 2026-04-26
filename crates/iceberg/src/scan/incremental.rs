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

use crate::expr::Predicate;
use crate::scan::context::{ManifestEntryFilter, ManifestFileFilter};
use crate::scan::{ScanConfig, TableScan, build_table_scan};
use crate::spec::{ManifestContentType, ManifestStatus, Operation, TableMetadataRef};
use crate::table::Table;
use crate::util::available_parallelism;
use crate::util::snapshot::ancestors_between;
use crate::{Error, ErrorKind, Result};

/// Represents a validated range of snapshots for incremental scanning.
///
/// This struct is used to track which snapshot IDs are included in an incremental
/// scan range, allowing efficient filtering of manifest entries.
#[derive(Debug, Clone)]
pub(crate) struct AppendSnapshotSet {
    /// Snapshot IDs in the range
    snapshot_ids: HashSet<i64>,
}

impl AppendSnapshotSet {
    /// Build a snapshot range by walking the snapshot ancestry chain.
    ///
    /// Validates that `from_snapshot_id` is an ancestor of `to_snapshot_id` and
    /// collects all snapshot IDs in between. Also validates that all snapshots
    /// in the range have APPEND operations.
    ///
    /// # Arguments
    /// * `table_metadata` - The table metadata containing snapshot information
    /// * `from_snapshot_id` - The starting snapshot ID
    /// * `to_snapshot_id` - The ending snapshot ID
    /// * `from_inclusive` - Whether to include the from_snapshot in the range
    pub(crate) fn build(
        table_metadata: &TableMetadataRef,
        from_snapshot_id: i64,
        to_snapshot_id: i64,
        from_inclusive: bool,
    ) -> Result<Self> {
        // Determine the exclusive stop point for the ancestry walk.
        // For inclusive mode the from-snapshot must exist so we can look up
        // its parent. For exclusive mode the snapshot may have been expired
        // (the parent pointer on its child still references it), so we only
        // need the ID — matching Java's BaseIncrementalScan semantics.
        let oldest_exclusive = if from_inclusive {
            let from_snapshot = table_metadata
                .snapshot_by_id(from_snapshot_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Snapshot {from_snapshot_id} not found"),
                    )
                })?;
            from_snapshot.parent_snapshot_id()
        } else {
            Some(from_snapshot_id)
        };

        let snapshots: Vec<_> =
            ancestors_between(table_metadata, to_snapshot_id, oldest_exclusive).collect();

        // ancestors_between silently returns the full chain to root if
        // oldest_exclusive isn't in the ancestry chain. Detect this:
        // if we got snapshots but from_snapshot_id wasn't encountered as
        // the stop point, the chain doesn't connect.
        if from_snapshot_id == to_snapshot_id {
            // Edge case: from == to. In exclusive mode, range is empty.
            // In inclusive mode, we should have exactly one snapshot.
            if !from_inclusive {
                return Ok(Self {
                    snapshot_ids: HashSet::new(),
                });
            }
        } else if snapshots.is_empty() {
            // to_snapshot_id doesn't exist
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "from_snapshot {from_snapshot_id} is not an ancestor of to_snapshot {to_snapshot_id}",
                ),
            ));
        } else {
            // Verify the oldest snapshot in our walk is actually connected
            // to from_snapshot_id. The last snapshot's parent (for exclusive)
            // or the last snapshot itself (for inclusive) should be from_snapshot_id.
            let oldest_collected = snapshots.last().unwrap();
            let connects = if from_inclusive {
                oldest_collected.snapshot_id() == from_snapshot_id
            } else {
                oldest_collected.parent_snapshot_id() == Some(from_snapshot_id)
            };
            if !connects {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "from_snapshot {from_snapshot_id} is not an ancestor of to_snapshot {to_snapshot_id}",
                    ),
                ));
            }
        }

        // Collect only APPEND snapshot IDs, silently skipping non-APPEND
        // snapshots (e.g. replace/compaction, overwrite, delete). This matches
        // the Java BaseIncrementalAppendScan behavior — only append operations
        // contribute new data files to an incremental append scan.
        let mut snapshot_ids = HashSet::with_capacity(snapshots.len());
        for snapshot in &snapshots {
            if snapshot.summary().operation == Operation::Append {
                snapshot_ids.insert(snapshot.snapshot_id());
            }
        }

        Ok(Self { snapshot_ids })
    }

    /// Check if a snapshot_id is within this set
    pub(crate) fn contains(&self, snapshot_id: i64) -> bool {
        self.snapshot_ids.contains(&snapshot_id)
    }

    /// Create a manifest file filter that skips delete manifests and data
    /// manifests whose `added_snapshot_id` is outside this set.
    pub(crate) fn manifest_file_filter(self: &Arc<Self>) -> ManifestFileFilter {
        let set = self.clone();
        Arc::new(move |manifest_file| {
            manifest_file.content != ManifestContentType::Deletes
                && set.contains(manifest_file.added_snapshot_id)
        })
    }

    /// Create a manifest entry filter that includes only entries with
    /// status ADDED and a snapshot_id within this set.
    pub(crate) fn manifest_entry_filter(self: &Arc<Self>) -> ManifestEntryFilter {
        let set = self.clone();
        Arc::new(move |entry| {
            entry.status() == ManifestStatus::Added
                && entry.snapshot_id().is_some_and(|id| set.contains(id))
        })
    }
}

/// Builder to create an incremental append scan between two snapshots.
///
/// An incremental append scan returns only data files that were added in
/// snapshots between `from_snapshot_id` and the target snapshot. Only
/// snapshots with APPEND operations are supported.
///
/// Use [`Table::incremental_append_scan`] or
/// [`Table::incremental_append_scan_inclusive`] to create an instance.
pub struct IncrementalAppendScanBuilder<'a> {
    table: &'a Table,
    from_snapshot_id: i64,
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
        from_snapshot_id: i64,
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
    pub fn build(self) -> Result<TableScan> {
        let to_snapshot = match self.to_snapshot_id {
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
                .clone(),
            None => {
                let Some(current_snapshot) = self.table.metadata().current_snapshot() else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "Cannot perform incremental scan: table has no snapshots",
                    ));
                };
                current_snapshot.clone()
            }
        };

        let append_set = Arc::new(AppendSnapshotSet::build(
            &self.table.metadata_ref(),
            self.from_snapshot_id,
            to_snapshot.snapshot_id(),
            self.from_inclusive,
        )?);

        build_table_scan(
            ScanConfig {
                table: self.table,
                column_names: self.column_names,
                batch_size: self.batch_size,
                case_sensitive: self.case_sensitive,
                filter: self.filter,
                concurrency_limit_data_files: self.concurrency_limit_data_files,
                concurrency_limit_manifest_entries: self.concurrency_limit_manifest_entries,
                concurrency_limit_manifest_files: self.concurrency_limit_manifest_files,
                row_group_filtering_enabled: self.row_group_filtering_enabled,
                row_selection_enabled: self.row_selection_enabled,
            },
            to_snapshot,
            Some(append_set.manifest_file_filter()),
            Some(append_set.manifest_entry_filter()),
        )
    }
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;

    use super::AppendSnapshotSet;
    use crate::scan::tests::TableTestFixture;

    #[test]
    fn test_incremental_scan_invalid_from_snapshot_exclusive() {
        let table = TableTestFixture::new().table;

        // Exclusive mode doesn't require from-snapshot to exist, but it must
        // be an ancestor of the to-snapshot. 999999999 is not in the ancestry
        // chain so this should fail.
        let result = table.incremental_append_scan(999999999, None).build();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("not an ancestor"),
            "Expected ancestry error, got: {err}"
        );
    }

    #[test]
    fn test_incremental_scan_invalid_from_snapshot_inclusive() {
        let table = TableTestFixture::new().table;

        // Inclusive mode requires from-snapshot to exist (we need its parent ID).
        let result = table
            .incremental_append_scan_inclusive(999999999, None)
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
            .incremental_append_scan(s1_id, Some(s2_id))
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
            .incremental_append_scan(3051729675574597004, Some(999999999))
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
            .incremental_append_scan(3051729675574597004, None)
            .build();
        assert!(
            result.is_ok(),
            "appends_after should succeed when all snapshots are appends"
        );

        let scan = result.unwrap();
        assert!(
            scan.plan_context.is_some(),
            "Incremental scan should have a plan context"
        );
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
            .incremental_append_scan(parent_id, Some(current_snapshot_id))
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
            .incremental_append_scan_inclusive(current_snapshot_id, Some(current_snapshot_id))
            .build();
        assert!(
            result.is_ok(),
            "Inclusive scan of a single append snapshot should succeed"
        );

        // Verify AppendSnapshotSet directly
        let set = AppendSnapshotSet::build(
            &table.metadata_ref(),
            current_snapshot_id,
            current_snapshot_id,
            true,
        )
        .unwrap();
        assert!(
            set.contains(current_snapshot_id),
            "Inclusive set should contain the from_snapshot"
        );
    }

    #[test]
    fn test_incremental_scan_from_snapshot_exclusive() {
        // Fixture has S1 (append) -> S2 (append, current)
        let table = TableTestFixture::new().table;
        let current_snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();

        // Verify the scan builds successfully
        let result = table
            .incremental_append_scan(current_snapshot_id, Some(current_snapshot_id))
            .build();
        assert!(
            result.is_ok(),
            "Exclusive scan from=to should succeed with empty range"
        );

        // Verify AppendSnapshotSet directly
        let set = AppendSnapshotSet::build(
            &table.metadata_ref(),
            current_snapshot_id,
            current_snapshot_id,
            false,
        )
        .unwrap();
        assert!(
            !set.contains(current_snapshot_id),
            "Exclusive set should not contain the from_snapshot"
        );
    }

    #[test]
    fn test_incremental_scan_skips_non_append_operations() {
        // Deep history fixture: S1 (append) -> S2 (append) -> S3 (append)
        //   -> S4 (overwrite) -> S5 (append, current)
        let table = TableTestFixture::new_with_deep_history().table;

        // Scanning from S1 to S5 crosses S4 (overwrite) — should succeed
        // but only include APPEND snapshots (S2, S3, S5), skipping S4
        let result = table
            .incremental_append_scan(3051729675574597004, Some(3059729675574597004))
            .build();

        assert!(
            result.is_ok(),
            "Should succeed, skipping non-APPEND snapshots"
        );

        let set = AppendSnapshotSet::build(
            &table.metadata_ref(),
            3051729675574597004,
            3059729675574597004,
            false,
        )
        .unwrap();
        assert!(
            !set.contains(3051729675574597004),
            "S1 (from) should be excluded"
        );
        assert!(
            set.contains(3055729675574597004),
            "S2 (append) should be in set"
        );
        assert!(
            set.contains(3056729675574597004),
            "S3 (append) should be in set"
        );
        assert!(
            !set.contains(3057729675574597004),
            "S4 (overwrite) should be skipped"
        );
        assert!(
            set.contains(3059729675574597004),
            "S5 (append) should be in set"
        );
    }

    #[test]
    fn test_incremental_scan_append_only_range() {
        // Deep history fixture: S1 (append) -> S2 (append) -> S3 (append)
        //   -> S4 (overwrite) -> S5 (append, current)
        let table = TableTestFixture::new_with_deep_history().table;

        // Scanning from S1 to S3 (all appends)
        let set = AppendSnapshotSet::build(
            &table.metadata_ref(),
            3051729675574597004,
            3056729675574597004,
            false,
        )
        .unwrap();
        assert!(
            !set.contains(3051729675574597004),
            "from_snapshot should be excluded"
        );
        assert!(set.contains(3055729675574597004), "S2 should be in range");
        assert!(set.contains(3056729675574597004), "S3 should be in range");
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
            .incremental_append_scan(parent_snapshot_id, Some(current_snapshot.snapshot_id()))
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
    async fn test_incremental_scan_exclusive_same_snapshot_returns_empty() {
        // Fixture has S1 (append) -> S2 (append, current)
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        let current_snapshot_id = fixture
            .table
            .metadata()
            .current_snapshot()
            .unwrap()
            .snapshot_id();

        // Incremental scan from S2 to S2 (exclusive) should return nothing
        let table_scan = fixture
            .table
            .incremental_append_scan(current_snapshot_id, Some(current_snapshot_id))
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
            "Exclusive scan from=to should return no files"
        );
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
            .incremental_append_scan(s1_id, Some(s5_id))
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
            .incremental_append_scan(s2_id, Some(s3_id))
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
            .incremental_append_scan_inclusive(s3_id, Some(s5_id))
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
