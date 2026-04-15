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

use crate::expr::Predicate;
use crate::scan::{ScanConfig, TableScan, build_table_scan};
use crate::spec::{Operation, TableMetadataRef};
use crate::table::Table;
use crate::util::available_parallelism;
use crate::util::snapshot::ancestors_between;
use crate::{Error, ErrorKind, Result};

/// Represents a validated range of snapshots for incremental scanning.
///
/// This struct is used to track which snapshot IDs are included in an incremental
/// scan range, allowing efficient filtering of manifest entries.
#[derive(Debug, Clone)]
pub(crate) struct SnapshotRange {
    /// Snapshot IDs in the range
    snapshot_ids: HashSet<i64>,
}

impl SnapshotRange {
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
        // Verify from_snapshot exists and determine the exclusive stop point.
        let from_snapshot = table_metadata
            .snapshot_by_id(from_snapshot_id)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Snapshot {from_snapshot_id} not found"),
                )
            })?;

        // ancestors_between returns (oldest_exclusive, latest_inclusive].
        // For inclusive mode, stop at from's parent so from itself is included.
        let oldest_exclusive = if from_inclusive {
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

        // Validate all snapshots have APPEND operations and collect IDs.
        let mut snapshot_ids = HashSet::with_capacity(snapshots.len());
        for snapshot in &snapshots {
            if snapshot.summary().operation != Operation::Append {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    format!(
                        "Incremental scan only supports APPEND operations, \
                         snapshot {} has operation: {:?}",
                        snapshot.snapshot_id(),
                        snapshot.summary().operation
                    ),
                ));
            }
            snapshot_ids.insert(snapshot.snapshot_id());
        }

        Ok(Self { snapshot_ids })
    }

    /// Check if a snapshot_id is within this range
    pub(crate) fn contains(&self, snapshot_id: i64) -> bool {
        self.snapshot_ids.contains(&snapshot_id)
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
    pub(crate) fn new(table: &'a Table, from_snapshot_id: i64, from_inclusive: bool) -> Self {
        let num_cpus = available_parallelism().get();

        Self {
            table,
            from_snapshot_id,
            from_inclusive,
            to_snapshot_id: None,
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

    /// Set the ending snapshot for the incremental scan (inclusive).
    /// If not set, defaults to the current snapshot.
    pub fn to_snapshot(mut self, snapshot_id: i64) -> Self {
        self.to_snapshot_id = Some(snapshot_id);
        self
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

        let snapshot_range = SnapshotRange::build(
            &self.table.metadata_ref(),
            self.from_snapshot_id,
            to_snapshot.snapshot_id(),
            self.from_inclusive,
        )?;

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
            Some(snapshot_range),
        )
    }
}
