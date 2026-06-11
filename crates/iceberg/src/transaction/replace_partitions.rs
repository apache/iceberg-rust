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

//! This module contains the replace-partitions action (dynamic partition overwrite).
//!
//! [`ReplacePartitionsAction`] mirrors Java `BaseReplacePartitions`: when committed, for every
//! partition that an ADDED file belongs to it DELETES all existing live data files in that same
//! `(spec_id, partition)` tuple, then adds the new files — in ONE `Overwrite` snapshot. The replace is
//! BY PARTITION VALUE (not by file path or row filter), so no metrics evaluators are needed.
//!
//! How it reuses the existing machinery: the added files reach the producer exactly as fast-append does
//! (written to a new added manifest), and the partition-scoped deletes are resolved + filtered out of the
//! current snapshot's manifests via the shared [`SnapshotProducer::resolve_partition_deletes`] (the
//! by-partition sibling of `resolve_delete_paths`) feeding the SAME `process_deletes` rewrite path that
//! `DeleteFiles` / `OverwriteFiles` use. Both happen in one snapshot.
//!
//! **Java semantics mirrored (cited against `core/.../BaseReplacePartitions.java`):**
//! - `operation()` returns `DataOperations.OVERWRITE` → always [`Operation::Overwrite`].
//! - the constructor sets `SnapshotSummary.REPLACE_PARTITIONS_PROP = "replace-partitions" = "true"`.
//! - `addFile(file)` drops `file.partition()` (in `file.specId()`) then adds the file.
//! - **Unpartitioned table = FULL replace:** every file is in the single empty partition, so adding any
//!   file drops that one partition → ALL existing files are replaced (Java `apply()` reaches the same end
//!   via `deleteByRowFilter(alwaysTrue)` when `dataSpec().isUnpartitioned()`).
//! - **A replaced partition with no existing files is a pure add** (no spurious delete, no error): Java's
//!   `failMissingDeletePaths` guards only path/file deletes, never partition drops.
//!
//! **Summary note (Java-faithful, NOT a Rust truncate):** Java `SnapshotProducer.summary()` has NO
//! full-table-truncate branch — it computes `total = previous + added - removed` unconditionally via
//! `updateTotal`. So the producer's `truncate_full_table` flag stays `false` here: the by-partition
//! resolution already reports EVERY removed file, so `deleted-data-files` / `deleted-records` are correct
//! and `update_totals` yields the right post-replace totals (e.g. an unpartitioned full replace of N files
//! adding M: `N + M - N = M`, no underflow). `replace-partitions=true` is just a summary property.
//!
//! **Concurrent-commit conflict validation (serializable isolation):** opt-in, mirroring Java
//! `BaseReplacePartitions.validate`. Two INDEPENDENT flags, both PARTITION-SET-based over the replaced
//! `(spec_id, partition)` tuples (unlike OverwriteFiles/RowDelta's per-data-file checks):
//! - [`ReplacePartitionsAction::validate_no_conflicting_data`] (Java `validateNoConflictingData`) rejects
//!   when a concurrent snapshot ADDED DATA to a replaced partition.
//! - [`ReplacePartitionsAction::validate_no_conflicting_deletes`] (Java `validateNoConflictingDeletes`)
//!   rejects when, in a replaced partition, a concurrent snapshot either DELETED a data file
//!   (`validateDeletedDataFiles`) or ADDED a delete file (`validateNoNewDeleteFiles`).
//!
//! **Out of scope (deferred):**
//! - static `replaceByRowFilter` / explicit-partition overwrite APIs — need inclusive/strict metrics
//!   evaluators to select files by row predicate.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{DataFile, ManifestEntry, ManifestFile, Operation, Struct};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer, added_data_files_after,
    added_delete_files_after, deleted_data_files_after,
};
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind};

/// The snapshot-summary property Java `BaseReplacePartitions` sets to mark a dynamic partition overwrite
/// (`SnapshotSummary.REPLACE_PARTITIONS_PROP`).
const REPLACE_PARTITIONS_PROP: &str = "replace-partitions";

/// A transaction action that performs a dynamic partition overwrite: for every partition an added file
/// belongs to, it replaces (deletes) all existing live data files in that same partition, then adds the
/// new files — in a single `Overwrite` snapshot.
///
/// Use [`crate::transaction::Transaction::replace_partitions`] to create one. Accumulate the files to add
/// with [`ReplacePartitionsAction::add_file`] / [`ReplacePartitionsAction::add_files`], then apply and
/// commit the transaction. The set of partitions to replace is derived from the added files' partition
/// values — replace is BY PARTITION, not by path.
///
/// On an UNPARTITIONED table every file is in the single empty partition, so adding any file replaces ALL
/// existing files (a full-table replace). A replace touching a partition that has no existing files is a
/// pure add. A replace with no added files (and therefore no resolved deletes) is rejected as empty.
pub struct ReplacePartitionsAction {
    /// Data files to add to the table (validated like fast append). Their partition values also determine
    /// which partitions are replaced.
    added_data_files: Vec<DataFile>,
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    /// Whether concurrent-commit conflict validation is enabled (Java `validateNoConflictingData`). OFF by
    /// default = snapshot isolation (no validation, current behavior). When ON, the commit is rejected if a
    /// concurrent snapshot added data to any partition this action replaces.
    validate_no_conflicting_data: bool,
    /// Whether concurrent-commit conflicting-delete validation is enabled (Java
    /// `validateNoConflictingDeletes`). INDEPENDENT of [`Self::validate_no_conflicting_data`] (neither flag
    /// enables the other). OFF by default. When ON, the commit is rejected if, in any partition this action
    /// replaces, a concurrent snapshot DELETED a data file or ADDED a delete file.
    validate_no_conflicting_deletes: bool,
    /// An explicit starting snapshot for conflict validation (Java `validateFromSnapshot`). When `None`, the
    /// validation uses the transaction's starting snapshot (the table head when the transaction was created).
    validate_from_snapshot: Option<i64>,
}

impl ReplacePartitionsAction {
    pub(crate) fn new() -> Self {
        Self {
            added_data_files: vec![],
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
            validate_no_conflicting_data: false,
            validate_no_conflicting_deletes: false,
            validate_from_snapshot: None,
        }
    }

    /// Add a single [`DataFile`] to the table (Java `ReplacePartitions.addFile`). Its partition is
    /// scheduled for replacement: all existing live data files in the same `(spec_id, partition)` are
    /// removed when the action commits.
    pub fn add_file(mut self, data_file: DataFile) -> Self {
        self.added_data_files.push(data_file);
        self
    }

    /// Add multiple [`DataFile`]s to the table. Every partition they belong to is replaced.
    pub fn add_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(data_files);
        self
    }

    /// Set the commit UUID for the snapshot (otherwise a fresh v7 UUID is generated).
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set key metadata for manifest files.
    pub fn set_key_metadata(mut self, key_metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(key_metadata);
        self
    }

    /// Set snapshot summary properties. The `replace-partitions` marker is added on top of these (Java
    /// sets it in the action constructor), so an explicit value here does not clear it.
    pub fn set_snapshot_properties(mut self, snapshot_properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }

    /// Override the snapshot from which concurrent-commit conflict validation starts (Java
    /// `ReplacePartitions.validateFromSnapshot(long)`). By default the validation uses the transaction's
    /// starting snapshot (the table head when [`crate::transaction::Transaction::new`] was called); this lets
    /// the caller pin a specific earlier snapshot id (the snapshot it read when building the replacement).
    ///
    /// On its own this does NOT enable validation — call [`Self::validate_no_conflicting_data`] and/or
    /// [`Self::validate_no_conflicting_deletes`] for that.
    pub fn validate_from_snapshot(mut self, snapshot_id: i64) -> Self {
        self.validate_from_snapshot = Some(snapshot_id);
        self
    }

    /// ENABLE concurrent-commit conflict validation (Java `ReplacePartitions.validateNoConflictingData`):
    /// the commit is rejected with a non-retryable `ValidationException` if any snapshot committed since the
    /// starting snapshot ADDED data to a partition this action replaces. This is the serializable-isolation
    /// guard against silently clobbering concurrently-appended data.
    ///
    /// Default (this method NOT called) = snapshot isolation = no validation (current behavior unchanged).
    pub fn validate_no_conflicting_data(mut self) -> Self {
        self.validate_no_conflicting_data = true;
        self
    }

    /// ENABLE concurrent-commit conflicting-DELETE validation (Java
    /// `ReplacePartitions.validateNoConflictingDeletes`): the commit is rejected with a non-retryable
    /// `ValidationException` if, in any partition this action replaces, a snapshot committed since the
    /// starting snapshot either DELETED a data file (Java `validateDeletedDataFiles` — you cannot dynamically
    /// replace a partition whose data a concurrent commit removed) or ADDED a delete file (Java
    /// `validateNoNewDeleteFiles` — a concurrent row-delete in a partition you are about to replace).
    ///
    /// INDEPENDENT of [`Self::validate_no_conflicting_data`]: enabling one does NOT enable the other (Java's
    /// `validateConflictingData` / `validateConflictingDeletes` are separate flags branched separately in
    /// `BaseReplacePartitions.validate`).
    ///
    /// Default (this method NOT called) = snapshot isolation = no validation (current behavior unchanged).
    pub fn validate_no_conflicting_deletes(mut self) -> Self {
        self.validate_no_conflicting_deletes = true;
        self
    }

    /// Collect the set of `(partition_spec_id, partition)` tuples the added files belong to — the
    /// partitions to replace (Java `replacedPartitions` / the per-file `dropPartition`).
    fn drop_partitions(&self) -> HashSet<(i32, Struct)> {
        self.added_data_files
            .iter()
            .map(|data_file| (data_file.partition_spec_id, data_file.partition().clone()))
            .collect()
    }
}

/// Whether `file`'s `(partition_spec_id, partition)` is in the replaced-partition set `drop_partitions` —
/// the single partition-membership predicate ReplacePartitions' conflict checks share (Java
/// `partitionSet.contains(file.specId(), file.partition())`). Used by BOTH the conflicting-DATA check
/// ([`ReplacePartitionsAction::validate_no_conflicting_data`]) and the two conflicting-DELETE checks
/// ([`ReplacePartitionsAction::validate_no_conflicting_deletes`]); there is exactly ONE such predicate.
fn file_in_replaced_partition(drop_partitions: &HashSet<(i32, Struct)>, file: &DataFile) -> bool {
    drop_partitions.contains(&(file.partition_spec_id, file.partition().clone()))
}

#[async_trait]
impl TransactionAction for ReplacePartitionsAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        // Mark the snapshot as a dynamic partition overwrite (Java `BaseReplacePartitions` constructor
        // sets `replace-partitions=true`). Layer it on top of any caller-provided properties.
        let mut snapshot_properties = self.snapshot_properties.clone();
        snapshot_properties.insert(REPLACE_PARTITIONS_PROP.to_string(), "true".to_string());

        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            snapshot_properties,
            self.added_data_files.clone(),
        );

        // Validate the added files like fast append: data content type, partition-spec match, and
        // partition-value compatibility (Java `MergingSnapshotProducer.add`). The partition-scoped deletes
        // are resolved inside the producer's commit via the operation's `delete_files` seam.
        snapshot_producer.validate_added_data_files()?;

        snapshot_producer
            .commit(
                ReplacePartitionsOperation {
                    drop_partitions: self.drop_partitions(),
                },
                DefaultManifestProcess,
            )
            .await
    }

    /// Serializable-isolation conflict validation (Java `BaseReplacePartitions.validate`). Two INDEPENDENT,
    /// opt-in checks, both PARTITION-SET-based over the replaced `(spec_id, partition)` tuples:
    /// - [`Self::validate_no_conflicting_data`] (Java `validateConflictingData` branch →
    ///   `validateAddedDataFiles`): reject if a concurrent snapshot ADDED DATA to a replaced partition.
    /// - [`Self::validate_no_conflicting_deletes`] (Java `validateConflictingDeletes` branch →
    ///   `validateDeletedDataFiles` + `validateNoNewDeleteFiles`): reject if, in a replaced partition, a
    ///   concurrent snapshot DELETED a data file or ADDED a delete file.
    ///
    /// Neither flag enables the other (Java branches them separately). When neither is set this is a no-op
    /// (snapshot isolation, current behavior unchanged).
    ///
    /// The effective starting snapshot ([`Self::validate_from_snapshot`] if set, else the transaction-captured
    /// `starting_snapshot_id`) and the replaced-partition set are computed ONCE and shared by all enabled
    /// checks. Every rejection is a NON-retryable [`ErrorKind::DataInvalid`] (Java's non-retryable
    /// `ValidationException`), so the commit retry loop stops and the error propagates rather than looping.
    async fn validate(
        self: Arc<Self>,
        starting_snapshot_id: Option<i64>,
        current: &Table,
    ) -> Result<()> {
        if !self.validate_no_conflicting_data && !self.validate_no_conflicting_deletes {
            // Default: snapshot isolation, no conflict check (current behavior unchanged).
            return Ok(());
        }

        // Java `BaseReplacePartitions.validate` uses `startingSnapshotId` (the `validateFromSnapshot`
        // override) when set, else the operation's starting snapshot.
        let effective_start = self.validate_from_snapshot.or(starting_snapshot_id);

        let drop_partitions = self.drop_partitions();
        // An empty replace touches no partitions, so nothing can conflict — skip every manifest walk.
        if drop_partitions.is_empty() {
            return Ok(());
        }

        // Java `validateConflictingData` branch → `validateAddedDataFiles(partitionSet)`.
        if self.validate_no_conflicting_data {
            self.validate_added_data_files(current, effective_start, &drop_partitions)
                .await?;
        }

        // Java `validateConflictingDeletes` branch → `validateDeletedDataFiles(partitionSet)` THEN
        // `validateNoNewDeleteFiles(partitionSet)` (same order as Java).
        if self.validate_no_conflicting_deletes {
            self.validate_deleted_data_files(current, effective_start, &drop_partitions)
                .await?;
            self.validate_no_new_delete_files(current, effective_start, &drop_partitions)
                .await?;
        }

        Ok(())
    }
}

impl ReplacePartitionsAction {
    /// Java `validateConflictingData` branch (partition-set) → `MergingSnapshotProducer.validateAddedDataFiles(
    /// base, startingSnapshotId, replacedPartitions, parent)`. Enumerate every DATA file ADDED to the refreshed
    /// base by snapshots committed since `effective_start` (the shared [`added_data_files_after`] = Java
    /// `addedDataFiles`, gated to `VALIDATE_ADDED_FILES_OPERATIONS = {APPEND, OVERWRITE}`) and reject on the
    /// FIRST whose partition is replaced ([`file_in_replaced_partition`] = Java `partitionSet.contains(...)`).
    async fn validate_added_data_files(
        &self,
        current: &Table,
        effective_start: Option<i64>,
        drop_partitions: &HashSet<(i32, Struct)>,
    ) -> Result<()> {
        let added = added_data_files_after(current, effective_start).await?;

        // Naming the conflicting partition + file mirrors Java's
        // "Found conflicting files that can contain records matching partitions %s: %s".
        if let Some(conflict) = added
            .iter()
            .find(|file| file_in_replaced_partition(drop_partitions, file))
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Found conflicting files that can contain records matching replaced partition \
                     (spec {}, partition {:?}): {}",
                    conflict.partition_spec_id,
                    conflict.partition(),
                    conflict.file_path()
                ),
            ));
        }

        Ok(())
    }

    /// Java `validateConflictingDeletes` branch (partition-set) → `MergingSnapshotProducer.validateDeletedDataFiles(
    /// base, startingSnapshotId, replacedPartitions, parent)`. Enumerate every DATA file DELETED from the
    /// refreshed base by snapshots committed since `effective_start` (the shared [`deleted_data_files_after`]
    /// with `skip_deletes == false` = Java `deletedDataFiles` gated to `VALIDATE_DATA_FILES_EXIST_OPERATIONS =
    /// {OVERWRITE, REPLACE, DELETE}`; `skip_deletes == false` keeps concurrent DELETE-op snapshots in scope,
    /// matching Java's op-set which INCLUDES `DELETE`) and reject on the FIRST whose partition is replaced
    /// ([`file_in_replaced_partition`] = Java `partitionSet.contains(...)`). You cannot dynamically replace a
    /// partition whose data a concurrent commit removed.
    async fn validate_deleted_data_files(
        &self,
        current: &Table,
        effective_start: Option<i64>,
        drop_partitions: &HashSet<(i32, Struct)>,
    ) -> Result<()> {
        // `skip_deletes == false`: Java's `validateDeletedDataFiles` uses `VALIDATE_DATA_FILES_EXIST_OPERATIONS`
        // (the full `{OVERWRITE, REPLACE, DELETE}` set), NOT the skip-delete subset — so a concurrent DELETE-op
        // snapshot that removed data from a replaced partition IS a conflict.
        let deleted = deleted_data_files_after(current, effective_start, false).await?;

        // Java throws on the first matching entry:
        // "Found conflicting deleted files that can apply to records matching %s: %s".
        if let Some(conflict) = deleted
            .iter()
            .find(|file| file_in_replaced_partition(drop_partitions, file))
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Found conflicting deleted files that can apply to records matching replaced \
                     partition (spec {}, partition {:?}): {}",
                    conflict.partition_spec_id,
                    conflict.partition(),
                    conflict.file_path()
                ),
            ));
        }

        Ok(())
    }

    /// Java `validateConflictingDeletes` branch (partition-set) → `MergingSnapshotProducer.validateNoNewDeleteFiles(
    /// base, startingSnapshotId, replacedPartitions, parent)`. Enumerate every DELETE file ADDED to the
    /// refreshed base by snapshots committed since `effective_start` (the shared [`added_delete_files_after`] =
    /// Java `addedDeleteFiles` gated to `VALIDATE_ADDED_DELETE_FILES_OPERATIONS = {OVERWRITE, DELETE}`,
    /// V2-guarded so a V1 table walks nothing) and reject on the FIRST whose partition is replaced
    /// ([`file_in_replaced_partition`] = Java `partitionSet.contains(...)`). A concurrent row-delete in a
    /// partition you are about to replace is a conflict.
    async fn validate_no_new_delete_files(
        &self,
        current: &Table,
        effective_start: Option<i64>,
        drop_partitions: &HashSet<(i32, Struct)>,
    ) -> Result<()> {
        let added_deletes = added_delete_files_after(current, effective_start).await?;

        // Java throws on the first matching entry:
        // "Found new conflicting delete files that can apply to records matching %s: %s".
        if let Some(conflict) = added_deletes
            .iter()
            .find(|file| file_in_replaced_partition(drop_partitions, file))
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Found new conflicting delete files that can apply to records matching replaced \
                     partition (spec {}, partition {:?}): {}",
                    conflict.partition_spec_id,
                    conflict.partition(),
                    conflict.file_path()
                ),
            ));
        }

        Ok(())
    }
}

/// The [`SnapshotProduceOperation`] for [`ReplacePartitionsAction`].
///
/// Records `Operation::Overwrite` (Java `BaseReplacePartitions.operation()` = `OVERWRITE`), exposes every
/// current data manifest as the set to filter, and resolves the drop-partition set against the current
/// snapshot's live data entries (the resolved [`DataFile`]s drive the producer's by-path manifest rewrite).
/// The added files reach the producer separately (passed to `SnapshotProducer::new`), so a single snapshot
/// carries both the added manifest and the rewritten (filtered) manifests.
struct ReplacePartitionsOperation {
    /// The `(partition_spec_id, partition)` tuples to replace, derived from the added files.
    drop_partitions: HashSet<(i32, Struct)>,
}

impl SnapshotProduceOperation for ReplacePartitionsOperation {
    fn operation(&self) -> Operation {
        // Java `BaseReplacePartitions.operation()` returns `DataOperations.OVERWRITE`.
        Operation::Overwrite
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        Ok(vec![])
    }

    async fn delete_files(&self, snapshot_produce: &SnapshotProducer<'_>) -> Result<Vec<DataFile>> {
        // Resolve the drop-partition set against the current snapshot's live data entries: every live
        // data file whose `(spec_id, partition)` is in the set is removed (Java
        // `ManifestFilterManager`'s `dropPartitions.contains(...)`). No missing-target validation —
        // a replaced partition with no existing files is a pure add.
        snapshot_produce
            .resolve_partition_deletes(&self.drop_partitions)
            .await
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        // Expose EVERY current manifest — DATA and DELETE — via the shared
        // [`SnapshotProducer::current_manifests`]. The producer's `process_deletes` decides per DATA manifest
        // whether to rewrite (to drop replaced-partition files), carry forward unchanged, or drop it; every
        // DELETE manifest carries forward UNCHANGED (its entries are delete-file paths, never in the
        // data-file `delete_paths`), so replacing a partition on a merge-on-read table preserves all
        // outstanding position / equality deletes in the UNREPLACED partitions instead of silently dropping
        // them table-wide and resurrecting deleted rows. The conservative dangling-delete posture (no
        // pruning) is documented on the helper.
        snapshot_produce.current_manifests().await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::fs::File;
    use std::io::BufReader;
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array, RecordBatch};
    use futures::TryStreamExt;

    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, ManifestContentType,
        ManifestStatus, Operation, Struct, TableMetadata,
    };
    use crate::table::Table;
    use crate::transaction::tests::{
        make_v2_minimal_table_in_catalog, make_v3_minimal_table_in_catalog,
    };
    use crate::transaction::{ApplyTransactionAction, Transaction};
    use crate::writer::base_writer::position_delete_writer::{
        PositionDeleteFileWriterBuilder, PositionDeleteWriterConfig,
    };
    use crate::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use crate::writer::file_writer::{FileWriter, FileWriterBuilder, ParquetWriterBuilder};
    use crate::writer::{IcebergWriter, IcebergWriterBuilder};
    use crate::{Catalog, ErrorKind, TableCreation, TableIdent};

    /// Build a data file routed to partition `x = part_value` (the V3 minimal table is partitioned by
    /// identity(x), spec id 0) with a unique path.
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

    /// Build a position-delete file routed to partition `x = part_value` (the V3 minimal table is
    /// partitioned by identity(x), spec id 0). Used by the conflicting-delete validation tests to add a
    /// concurrent row-delete in a given partition (NOT a real parquet file — manifest-only).
    fn position_delete_file(path: &str, part_value: i64) -> DataFile {
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

    /// Build a data file for an UNPARTITIONED table (spec id 0, empty partition struct).
    fn unpartitioned_data_file(path: &str) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(0)
            .partition(Struct::empty())
            .build()
            .unwrap()
    }

    /// Collect the set of live (Added or Existing) data file paths across the table's current snapshot —
    /// the real correctness signal (what a scan would read).
    async fn live_file_paths(table: &Table) -> HashSet<String> {
        let snapshot = table
            .metadata()
            .current_snapshot()
            .expect("table should have a current snapshot");
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .expect("manifest list should load");

        let mut live = HashSet::new();
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file
                .load_manifest(table.file_io())
                .await
                .expect("manifest should load");
            for entry in manifest.entries() {
                if entry.is_alive() {
                    live.insert(entry.file_path().to_string());
                }
            }
        }
        live
    }

    /// Append the given files in a single fast-append commit and return the updated table.
    async fn append_files(catalog: &impl Catalog, table: &Table, files: Vec<DataFile>) -> Table {
        let tx = Transaction::new(table);
        let action = tx.fast_append().add_data_files(files);
        let tx = action.apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    /// Return (snapshot_id, sequence_number, file_sequence_number) of the live entry for `path`.
    async fn entry_provenance(
        table: &Table,
        path: &str,
    ) -> (Option<i64>, Option<i64>, Option<i64>) {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                if entry.is_alive() && entry.file_path() == path {
                    return (
                        entry.snapshot_id(),
                        entry.sequence_number(),
                        entry.file_sequence_number,
                    );
                }
            }
        }
        panic!("no live entry for {path}");
    }

    /// Create an UNPARTITIONED V3 table in the catalog (same schema as the minimal fixture, but with no
    /// partition spec). Used to pin the full-table-replace behavior of `ReplacePartitions`.
    async fn make_v3_unpartitioned_table_in_catalog(catalog: &impl Catalog) -> Table {
        let table_ident =
            TableIdent::from_strs([format!("ns1-{}", uuid::Uuid::new_v4()), "test1".to_string()])
                .unwrap();
        catalog
            .create_namespace(table_ident.namespace(), HashMap::new())
            .await
            .unwrap();

        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV3ValidMinimal.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let base_metadata = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        // No partition_spec → unpartitioned table.
        let table_creation = TableCreation::builder()
            .schema((**base_metadata.current_schema()).clone())
            .sort_order((**base_metadata.default_sort_order()).clone())
            .name(table_ident.name().to_string())
            .format_version(crate::spec::FormatVersion::V3)
            .build();

        catalog
            .create_table(table_ident.namespace(), table_creation)
            .await
            .unwrap()
    }

    /// Read a u64 total from a snapshot summary property, defaulting to 0 when absent.
    fn total(table: &Table, prop: &str) -> u64 {
        table
            .metadata()
            .current_snapshot()
            .unwrap()
            .summary()
            .additional_properties
            .get(prop)
            .map(|value| value.parse::<u64>().unwrap())
            .unwrap_or(0)
    }

    /// THE KEY CROSS-PARTITION-ISOLATION TEST. Append fileA in x=0 and fileB in x=1; then
    /// `replace_partitions().add_file(fileA2 in x=0)` → the post-commit SCAN live set is exactly
    /// {fileA2 (x=0), fileB (x=1)}: fileA is replaced, fileB in the UNTOUCHED partition survives.
    ///
    /// Risk pinned: cross-partition data loss / wrong live set. A bug that loses fileB (over-deletes the
    /// untouched partition) or keeps fileA (under-deletes the replaced partition) is silent data
    /// corruption — the single most dangerous failure for a partition overwrite.
    #[tokio::test]
    async fn test_replace_partitions_replaces_only_the_added_partition_keeps_others() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Append fileA @ x=0 and fileB @ x=1 (one manifest holding both partitions).
        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 1),
        ])
        .await;

        // Replace partition x=0 with fileA2 — fileB in x=1 must be untouched.
        let tx = Transaction::new(&table);
        let action = tx
            .replace_partitions()
            .add_file(data_file("test/a2.parquet", 0));
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Overwrite,
            "ReplacePartitions records an Overwrite operation (Java BaseReplacePartitions.operation())"
        );
        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/a2.parquet".to_string(), "test/b.parquet".to_string(),]),
            "x=0 replaced (a2 replaces a), x=1 untouched (b survives)"
        );
    }

    /// Pins: replacing MULTIPLE partitions in one action replaces each of them and leaves every other
    /// partition untouched. Risk: a replace that only handles the first added file's partition (wrong live
    /// set across multiple replaced partitions).
    #[tokio::test]
    async fn test_replace_partitions_replaces_multiple_partitions_at_once() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Three partitions populated: x=0 (a), x=1 (b), x=2 (c).
        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 1),
            data_file("test/c.parquet", 2),
        ])
        .await;

        // Replace partitions x=0 and x=1; leave x=2 alone.
        let tx = Transaction::new(&table);
        let action = tx
            .replace_partitions()
            .add_file(data_file("test/a2.parquet", 0))
            .add_file(data_file("test/b2.parquet", 1));
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from([
                "test/a2.parquet".to_string(),
                "test/b2.parquet".to_string(),
                "test/c.parquet".to_string(),
            ]),
            "x=0 and x=1 replaced, x=2 (c) untouched"
        );
    }

    /// Pins: replacing a single partition with MULTIPLE new files removes the old file and adds all the
    /// new ones in that partition. Risk: only the first added file landing (lost new files).
    #[tokio::test]
    async fn test_replace_partition_with_multiple_new_files() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let table = append_files(&catalog, &table, vec![
            data_file("test/old.parquet", 0),
            data_file("test/keep.parquet", 1),
        ])
        .await;

        // Replace x=0 with TWO files; keep.parquet in x=1 must survive.
        let tx = Transaction::new(&table);
        let action = tx.replace_partitions().add_files(vec![
            data_file("test/new1.parquet", 0),
            data_file("test/new2.parquet", 0),
        ]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from([
                "test/new1.parquet".to_string(),
                "test/new2.parquet".to_string(),
                "test/keep.parquet".to_string(),
            ]),
            "x=0 now holds new1+new2 (old replaced), x=1 keep survives"
        );
    }

    /// Pins PROVENANCE preservation (the Increment-1 lesson, the #1 corruption risk). When a replace
    /// rewrites a manifest, every SURVIVING entry in an untouched partition must be copied forward as
    /// `Existing` carrying its ORIGINAL `snapshot_id` + both sequence numbers — NOT re-stamped with the
    /// new replace snapshot/seq. A carried-forward (untouched) manifest's entries also keep provenance.
    ///
    /// Risk pinned: a rewrite that re-stamps surviving entries is silent table corruption (a wrong
    /// data-sequence number breaks merge-on-read delete application and incremental scans). The live-set
    /// tests above assert only the live PATH set, so they pass under a snapshot-id re-stamp — only this
    /// test catches it.
    #[tokio::test]
    async fn test_replace_partitions_preserves_surviving_entry_provenance() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Append A@x=0 and B@x=1 in ONE commit (snapshot S1, data seq 1; one manifest holding both).
        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 1),
        ])
        .await;
        let s1 = table.metadata().current_snapshot().unwrap().snapshot_id();
        let (b_snap, b_seq, b_fseq) = entry_provenance(&table, "test/b.parquet").await;
        assert_eq!(b_snap, Some(s1), "B added by S1");

        // Replace partition x=0 → rewrites S1's manifest (B in x=1 survives as Existing).
        let tx = Transaction::new(&table);
        let action = tx
            .replace_partitions()
            .add_file(data_file("test/a2.parquet", 0));
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let s2 = table.metadata().current_snapshot().unwrap().snapshot_id();
        assert_ne!(s1, s2);

        // B survived in the untouched partition: MUST keep S1's snapshot id + seq numbers (NOT S2).
        let (b2_snap, b2_seq, b2_fseq) = entry_provenance(&table, "test/b.parquet").await;
        assert_eq!(
            b2_snap,
            Some(s1),
            "surviving B (untouched partition) must keep its ORIGINAL snapshot id S1, not the replace S2"
        );
        assert_eq!(
            b2_seq, b_seq,
            "surviving B must keep its ORIGINAL data seq, not the replace seq"
        );
        assert_eq!(
            b2_fseq, b_fseq,
            "surviving B must keep its ORIGINAL file seq"
        );

        // The newly added A2 gets the NEW replace snapshot's provenance.
        let (a2_snap, _a2_seq, _) = entry_provenance(&table, "test/a2.parquet").await;
        assert_eq!(
            a2_snap,
            Some(s2),
            "added A2 gets the new replace snapshot id S2"
        );

        // The replaced A is a Deleted tombstone (not live).
        assert!(
            !live_file_paths(&table).await.contains("test/a.parquet"),
            "the replaced A must no longer be live"
        );
    }

    /// THE FULL-REPLACE TEST. On an UNPARTITIONED table every file is in the single empty partition, so
    /// adding any file replaces ALL existing files. Append A, B; `replace_partitions().add_file(C)` →
    /// the live set is {C} only, the `replace-partitions` summary prop is set, and the cumulative totals
    /// are correct (2 prev - 2 removed + 1 added = 1, no underflow).
    ///
    /// Risk pinned: a full replace that fails to delete the old files (live set {A,B,C}) or underflows the
    /// running totals (the Rust `update_totals` is u64 — a wrong removed-count would panic).
    #[tokio::test]
    async fn test_replace_partitions_full_replace_on_unpartitioned_table() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_unpartitioned_table_in_catalog(&catalog).await;

        // Append A, B (2 files, 2 records). Running total-data-files = 2.
        let table = append_files(&catalog, &table, vec![
            unpartitioned_data_file("test/a.parquet"),
            unpartitioned_data_file("test/b.parquet"),
        ])
        .await;
        assert_eq!(total(&table, "total-data-files"), 2);

        // Replace: add C. The single (empty) partition is replaced → A and B removed.
        let tx = Transaction::new(&table);
        let action = tx
            .replace_partitions()
            .add_file(unpartitioned_data_file("test/c.parquet"));
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/c.parquet".to_string()]),
            "unpartitioned replace is a FULL replace: only C survives"
        );
        // The replace-partitions marker is present.
        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .additional_properties
                .get("replace-partitions")
                .map(String::as_str),
            Some("true"),
            "the replace-partitions summary property must be set (Java REPLACE_PARTITIONS_PROP)"
        );
        // Totals are correct: 2 - 2 + 1 = 1 (cumulative, no underflow).
        assert_eq!(
            total(&table, "total-data-files"),
            1,
            "full replace of 2 files adding 1 → total-data-files = 2 - 2 + 1 = 1"
        );
        assert_eq!(
            total(&table, "total-records"),
            1,
            "full replace → total-records = 2 - 2 + 1 = 1 (no underflow)"
        );
        // The deleted counts reflect the full replace.
        let summary = table.metadata().current_snapshot().unwrap().summary();
        assert_eq!(
            summary
                .additional_properties
                .get("deleted-data-files")
                .map(String::as_str),
            Some("2"),
            "the full replace reports both prior files as deleted"
        );
    }

    /// Pins: replacing a partition that has NO existing files is a PURE ADD (no spurious deletes, no
    /// error) — Java's `failMissingDeletePaths` guards only path deletes, never partition drops. Append
    /// A@x=0; replace add B@x=1 (an empty partition) → live set {A, B}: A in the untouched x=0 survives and
    /// B is added, with no error.
    ///
    /// Risk pinned: a by-partition resolution that wrongly errors on (or over-deletes for) a replaced
    /// partition with no existing files.
    #[tokio::test]
    async fn test_replace_empty_partition_is_pure_add() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;

        // Replace x=1, which currently has no files → pure add of B; A in x=0 untouched.
        let tx = Transaction::new(&table);
        let action = tx
            .replace_partitions()
            .add_file(data_file("test/b.parquet", 1));
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/a.parquet".to_string(), "test/b.parquet".to_string()]),
            "replacing an empty partition adds B and leaves A untouched"
        );
    }

    /// Pins: the replaced partition's old file appears as a Deleted tombstone in the rewritten manifest
    /// (so downstream tooling / expiry sees the removal), and the `replace-partitions` marker is present on
    /// a PARTITIONED replace too (not only the unpartitioned full-replace case).
    #[tokio::test]
    async fn test_replace_partition_marks_old_file_deleted_and_sets_marker() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![
            data_file("test/old.parquet", 0),
            data_file("test/keep.parquet", 1),
        ])
        .await;

        let tx = Transaction::new(&table);
        let action = tx
            .replace_partitions()
            .add_file(data_file("test/new.parquet", 0));
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // old.parquet is a Deleted tombstone.
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let mut old_deleted = false;
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                if entry.file_path() == "test/old.parquet" {
                    assert_eq!(entry.status(), ManifestStatus::Deleted);
                    old_deleted = true;
                }
            }
        }
        assert!(
            old_deleted,
            "the replaced old.parquet must be a Deleted tombstone"
        );

        assert_eq!(
            snapshot
                .summary()
                .additional_properties
                .get("replace-partitions")
                .map(String::as_str),
            Some("true"),
            "the replace-partitions marker is present on a partitioned replace too"
        );
    }

    /// Pins: a `replace_partitions` with NO added files (and therefore no resolved deletes and no snapshot
    /// properties beyond the marker) does not silently produce an empty no-op snapshot. Java requires the
    /// commit to produce content; the producer's relaxed precondition still rejects a truly-empty commit.
    ///
    /// Note: the action always sets the `replace-partitions` property, so the "no added data files,
    /// deleted data files, or added snapshot properties" precondition is bypassed by that property. This
    /// test pins the resulting behavior (an empty replace produces a no-content Overwrite snapshot with
    /// only the marker) so a future change to the precondition does not silently alter it.
    #[tokio::test]
    async fn test_replace_partitions_with_no_added_files_adds_nothing() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;

        let tx = Transaction::new(&table);
        let action = tx.replace_partitions();
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Nothing was added and no partition was named for replacement, so the existing file is untouched.
        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/a.parquet".to_string()]),
            "a no-added-files replace must not delete or add anything"
        );
    }

    // ============================================================================================
    // Concurrent-commit conflict validation (Java `validateNoConflictingData` / serializable isolation).
    //
    // The race these tests simulate: a `replace_partitions` is BUILT against table head S0, but BEFORE it
    // commits a SEPARATE `fast_append` lands on the catalog (advancing the head to S1). When the replace then
    // commits, `do_commit` refreshes to S1 and runs the action's `validate` against that refreshed base. With
    // `validate_no_conflicting_data()` enabled, a concurrent append into a REPLACED partition must FAIL the
    // commit (non-retryable) — silently clobbering that concurrently-added data is a serializable-isolation
    // violation (lost write). A concurrent append into an UNTOUCHED partition must NOT fail. With validation
    // OFF (the default), neither fails (snapshot isolation, unchanged behavior).
    // ============================================================================================

    /// THE KEY CONCURRENT-COMMIT CONFLICT TEST. Append S0 (x=0, x=1). Build a `replace_partitions(x=0)` with
    /// `.validate_from_snapshot(S0).validate_no_conflicting_data()`. Then perform a CONCURRENT `fast_append`
    /// adding NEW data to partition **x=0** (S1). Committing the replace must FAIL with the validation error,
    /// and the error must be NON-retryable (the retry loop stops + the validation message propagates — it is
    /// NOT a retry-exhaustion `CatalogCommitConflicts`).
    ///
    /// Risk pinned: silently clobbering concurrently-appended data in a replaced partition = a lost write
    /// under serializable isolation. Without the conflict check the replace would commit and drop S1's file.
    #[tokio::test]
    async fn test_replace_partitions_rejects_concurrent_append_to_replaced_partition() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // S0: x=0 (a), x=1 (b).
        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 1),
        ])
        .await;
        let s0 = table.metadata().current_snapshot().unwrap().snapshot_id();

        // Build the replace on x=0 with conflict validation enabled, pinned to start at S0 (the head we read).
        let tx = Transaction::new(&table);
        let action = tx
            .replace_partitions()
            .add_file(data_file("test/a2.parquet", 0))
            .validate_from_snapshot(s0)
            .validate_no_conflicting_data();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a separate fast_append adds NEW data to partition x=0.
        let _table_after_concurrent = append_files(&catalog, &table, vec![data_file(
            "test/a_concurrent.parquet",
            0,
        )])
        .await;

        // Committing the replace must FAIL: S1 added a file to the replaced partition x=0.
        let err = tx.commit(&catalog).await.expect_err(
            "replace must fail: a concurrent append landed in the replaced partition x=0",
        );

        assert_eq!(
            err.kind(),
            ErrorKind::DataInvalid,
            "a conflict is a non-retryable validation failure (DataInvalid), not a commit conflict"
        );
        assert!(
            !err.retryable(),
            "the validation failure must be NON-retryable so the retry loop stops and it propagates \
             (it is NOT a retry-exhausted CatalogCommitConflicts)"
        );
        assert!(
            err.message().contains("conflicting files"),
            "the error must name the conflict, got: {}",
            err.message()
        );

        // The catalog head is still S1 (the concurrent append) — the replace did NOT commit over it.
        let reloaded = catalog.load_table(table.identifier()).await.unwrap();
        let live = live_file_paths(&reloaded).await;
        assert!(
            live.contains("test/a_concurrent.parquet"),
            "the concurrently-added file must survive (the conflicting replace was rejected)"
        );
        assert!(
            !live.contains("test/a2.parquet"),
            "the rejected replace's file must NOT be in the table"
        );
    }

    /// NEGATIVE CONTROL: same setup, but the concurrent append targets the UNTOUCHED partition **x=1**. The
    /// `replace_partitions(x=0)` validation PASSES and the commit succeeds — a concurrent write to a
    /// non-replaced partition is not a conflict (it does not race the replaced data).
    ///
    /// Risk pinned: an over-eager conflict check that rejects ANY concurrent append (false positive) would
    /// break legitimate concurrent writes to disjoint partitions.
    #[tokio::test]
    async fn test_replace_partitions_allows_concurrent_append_to_other_partition() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // S0: x=0 (a), x=1 (b).
        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 1),
        ])
        .await;
        let s0 = table.metadata().current_snapshot().unwrap().snapshot_id();

        let tx = Transaction::new(&table);
        let action = tx
            .replace_partitions()
            .add_file(data_file("test/a2.parquet", 0))
            .validate_from_snapshot(s0)
            .validate_no_conflicting_data();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a separate fast_append adds data to the UNTOUCHED partition x=1.
        let _ = append_files(&catalog, &table, vec![data_file(
            "test/b_concurrent.parquet",
            1,
        )])
        .await;

        // The replace must SUCCEED — x=1 is not replaced, so the concurrent append does not conflict.
        let table = tx.commit(&catalog).await.expect(
            "replace must succeed: the concurrent append was in the untouched partition x=1",
        );

        let live = live_file_paths(&table).await;
        assert_eq!(
            live,
            HashSet::from([
                // x=0 replaced: a2 replaces a.
                "test/a2.parquet".to_string(),
                // x=1 originals + the concurrent append both survive (re-based onto S1).
                "test/b.parquet".to_string(),
                "test/b_concurrent.parquet".to_string(),
            ]),
            "x=0 replaced (a2), x=1 keeps both b and the concurrent file"
        );
    }

    /// OFF CONTROL: with conflict validation NOT enabled (no `validate_no_conflicting_data()` call), a
    /// concurrent append into the replaced partition x=0 does NOT fail the commit — this is snapshot
    /// isolation, the DEFAULT behavior, unchanged by this increment. (The concurrent file IS clobbered; that
    /// is the documented snapshot-isolation semantics the opt-in validation exists to prevent.)
    ///
    /// Risk pinned: the conflict-validation foundation must be OPT-IN — turning it on for every replace by
    /// default would change existing behavior and break callers that rely on snapshot isolation.
    #[tokio::test]
    async fn test_replace_partitions_without_validation_allows_concurrent_append_default_behavior()
    {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // S0: x=0 (a), x=1 (b).
        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 1),
        ])
        .await;

        // Build the replace on x=0 WITHOUT enabling validation (default = snapshot isolation).
        let tx = Transaction::new(&table);
        let action = tx
            .replace_partitions()
            .add_file(data_file("test/a2.parquet", 0));
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a separate fast_append adds data to the replaced partition x=0.
        let _ = append_files(&catalog, &table, vec![data_file(
            "test/a_concurrent.parquet",
            0,
        )])
        .await;

        // With validation OFF, the replace COMMITS (default behavior unchanged) — it re-bases onto S1 and
        // replaces partition x=0 entirely, clobbering the concurrent file (documented snapshot isolation).
        let table = tx
            .commit(&catalog)
            .await
            .expect("with validation OFF, a concurrent append must not block the commit");

        let live = live_file_paths(&table).await;
        assert_eq!(
            live,
            HashSet::from([
                // x=0 fully replaced: a2 only (a AND the concurrent file are dropped).
                "test/a2.parquet".to_string(),
                // x=1 untouched.
                "test/b.parquet".to_string(),
            ]),
            "default snapshot isolation: the replace re-bases onto S1 and replaces all of x=0"
        );
        assert!(
            !live.contains("test/a_concurrent.parquet"),
            "without conflict validation, the concurrent x=0 file is clobbered (snapshot isolation)"
        );
    }

    /// SURVIVES-REBASE PIN: the conflict check works WITHOUT an explicit `validate_from_snapshot` override,
    /// relying solely on the transaction-captured starting snapshot id surviving `do_commit`'s re-base.
    ///
    /// Same race as the KEY test, but the action calls ONLY `.validate_no_conflicting_data()` (no
    /// `validate_from_snapshot`). The starting snapshot is therefore the one captured in `Transaction::new`
    /// (= S0, the head when the tx was built). `do_commit` overwrites `self.table` with the refreshed base
    /// (S1) during the staleness re-base, but `starting_snapshot_id` is its OWN field and must SURVIVE that
    /// re-base — so validation still enumerates the concurrent S1 and rejects.
    ///
    /// Risk pinned: if the start were (re-)read from the refreshed head at validation time, start == current
    /// head ⇒ the concurrent set is empty ⇒ the check silently always passes (a serializable-isolation hole
    /// that lets a real conflict through). The KEY/NEGATIVE/OFF tests all pin `validate_from_snapshot(S0)`,
    /// so NONE of them exercises the tx-captured field — this test is the only guard that the capture in
    /// `Transaction::new` survives the re-base. Mutation-verified: capturing `None` (or re-reading the head)
    /// in `Transaction::new` makes this test fail (the conflict goes undetected, commit wrongly succeeds).
    #[tokio::test]
    async fn test_replace_partitions_rejects_concurrent_append_using_tx_captured_starting_snapshot()
    {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // S0: x=0 (a), x=1 (b). This is the head when the transaction is created below.
        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 1),
        ])
        .await;

        // Build the replace on x=0 with validation enabled but WITHOUT validate_from_snapshot — the
        // starting snapshot is the tx-captured head (S0).
        let tx = Transaction::new(&table);
        let action = tx
            .replace_partitions()
            .add_file(data_file("test/a2.parquet", 0))
            .validate_no_conflicting_data();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a separate fast_append adds NEW data to partition x=0.
        let _ = append_files(&catalog, &table, vec![data_file(
            "test/a_concurrent.parquet",
            0,
        )])
        .await;

        let err = tx
            .commit(&catalog)
            .await
            .expect_err("conflict must be detected via the tx-captured starting snapshot");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(!err.retryable());
        assert!(err.message().contains("conflicting files"));
    }

    /// CARRIED-FORWARD EXCLUSION PIN for `added_data_files_after` (Java
    /// `addedDataFiles`/`validationHistory`: `manifest.snapshotId() == currentSnapshot.snapshotId()` +
    /// `ignoreExisting().ignoreDeleted()`). S0 already has files in x=0 (a) and x=1 (b). A concurrent append
    /// S1 adds ONE new file to x=0. `added_data_files_after(table, S0)` must return EXACTLY that one new x=0
    /// file — NOT the carried-forward old x=0/x=1 files (a fast_append re-references them via their original
    /// manifests, whose `added_snapshot_id` is S0, not S1, with `Existing` — not `Added` — entries).
    ///
    /// Risk pinned: counting carried-forward manifests would return 2-3 files and falsely flag the old x=0
    /// file as a conflict (false-positive rejection of a valid commit). This isolates the helper directly;
    /// the end-to-end KEY/NEGATIVE tests cannot distinguish "1 new file" from "1 new + carried-forward old".
    #[tokio::test]
    async fn test_added_data_files_after_excludes_carried_forward_manifests() {
        use crate::transaction::snapshot::added_data_files_after;

        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // S0: x=0 (a), x=1 (b).
        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 1),
        ])
        .await;
        let s0 = table.metadata().current_snapshot().unwrap().snapshot_id();

        // S1: concurrent append adds ONE new file to x=0.
        let table = append_files(&catalog, &table, vec![data_file("test/a_new.parquet", 0)]).await;

        let added = added_data_files_after(&table, Some(s0)).await.unwrap();
        let paths: HashSet<String> = added.iter().map(|f| f.file_path().to_string()).collect();
        assert_eq!(
            paths,
            HashSet::from(["test/a_new.parquet".to_string()]),
            "must return ONLY the newly-added file, not carried-forward a/b"
        );
    }

    /// EDGE CASE — a non-ancestor `start` does not panic and is Rust-STRICTER than Java. When `start` is a
    /// snapshot id NOT in the current snapshot's ancestor chain, the walk runs to the root and returns ALL
    /// added files (an over-scan). Java's `validationHistory` instead fails loud
    /// ("Cannot determine history between starting snapshot ... and the last known ancestor ...") because
    /// its post-walk check requires `lastSnapshot.parentId() == startingSnapshotId`. The Rust over-scan
    /// direction is SAFE (it can only over-reject, never let a real conflict through), but it diverges from
    /// Java's explicit error. Documented divergence (see lessons/todo follow-up).
    ///
    /// Risk pinned: a non-ancestor start must not panic or silently SKIP the check (which would let a
    /// conflict through); over-scanning to root is the safe fallback.
    #[tokio::test]
    async fn test_added_data_files_after_nonancestor_start_overscans_does_not_panic() {
        use crate::transaction::snapshot::added_data_files_after;
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;
        let table = append_files(&catalog, &table, vec![data_file("test/b.parquet", 0)]).await;
        // A snapshot id that is NOT in the ancestor chain (never existed).
        let bogus: i64 = 999_999_999;
        let added = added_data_files_after(&table, Some(bogus)).await.unwrap();
        let paths: HashSet<String> = added.iter().map(|f| f.file_path().to_string()).collect();
        assert_eq!(
            paths.len(),
            2,
            "over-scans to root when start is a non-ancestor"
        );
    }

    /// EDGE CASE — a table with no current snapshot yields an empty added-files set (no panic), whether the
    /// start is `None` or a stale id. Java `addedDataFiles` returns empty when `parent == null`.
    ///
    /// Risk pinned: an empty/just-created table must not panic on the manifest walk (no current snapshot to
    /// load a manifest list from).
    #[tokio::test]
    async fn test_added_data_files_after_empty_table_yields_empty() {
        use crate::transaction::snapshot::added_data_files_after;
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        // No snapshots at all.
        let added = added_data_files_after(&table, None).await.unwrap();
        assert!(added.is_empty(), "no current snapshot ⇒ empty");
        let added2 = added_data_files_after(&table, Some(123)).await.unwrap();
        assert!(
            added2.is_empty(),
            "no current snapshot ⇒ empty even with a start"
        );
    }

    // ============================================================================================
    // Concurrent-commit CONFLICTING-DELETE validation (Java `validateNoConflictingDeletes` /
    // serializable isolation).
    //
    // INDEPENDENT of `validateNoConflictingData` (above): this is PARTITION-SET-based over the replaced
    // `(spec_id, partition)` tuples and rejects when, in a replaced partition, a concurrent commit either
    //   (a) DELETED a data file (Java `validateDeletedDataFiles` — you cannot dynamically replace a
    //       partition whose data a concurrent commit removed), or
    //   (b) ADDED a delete file (Java `validateNoNewDeleteFiles` — a concurrent row-delete in a partition
    //       you are about to replace).
    // The race mirrors the data tests: the replace is BUILT against head S0; BEFORE it commits a separate
    // commit lands (advancing the head); the replace's `do_commit` refreshes and runs `validate` against
    // that refreshed base.
    // ============================================================================================

    /// REJECT — a concurrent commit ADDS a DELETE file in a partition this replace REPLACES.
    /// Append S0 (x=0, x=1). Build `replace_partitions(x=0).validate_from_snapshot(S0)
    /// .validate_no_conflicting_deletes()`. A concurrent `row_delta` adds a position-delete in **x=0** (S1).
    /// Committing the replace must FAIL non-retryably (Java `validateNoNewDeleteFiles`).
    #[tokio::test]
    async fn test_replace_partitions_rejects_concurrent_added_delete_in_replaced_partition() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;

        // S0: x=0 (a), x=1 (b).
        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 1),
        ])
        .await;
        let s0 = table.metadata().current_snapshot().unwrap().snapshot_id();

        // Build the replace on x=0 with conflicting-delete validation enabled, pinned to start at S0.
        let tx = Transaction::new(&table);
        let action = tx
            .replace_partitions()
            .add_file(data_file("test/a2.parquet", 0))
            .validate_from_snapshot(s0)
            .validate_no_conflicting_deletes();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a separate row_delta adds a position-delete in the replaced partition x=0.
        let concurrent_tx = Transaction::new(&table);
        let concurrent = concurrent_tx
            .row_delta()
            .add_deletes(vec![position_delete_file("test/del_x0.parquet", 0)]);
        let concurrent_tx = concurrent.apply(concurrent_tx).unwrap();
        concurrent_tx.commit(&catalog).await.unwrap();

        // Committing the replace must FAIL: S1 added a delete file in the replaced partition x=0.
        let err = tx.commit(&catalog).await.expect_err(
            "replace must fail: a concurrent delete file landed in the replaced partition x=0",
        );
        assert_eq!(
            err.kind(),
            ErrorKind::DataInvalid,
            "a conflict is a non-retryable validation failure (DataInvalid)"
        );
        assert!(
            !err.retryable(),
            "the validation failure must be NON-retryable so the retry loop stops and it propagates"
        );
        assert!(
            err.message().contains("new conflicting delete files"),
            "the error must name the new-delete-file conflict, got: {}",
            err.message()
        );
    }

    /// REJECT — a concurrent commit DELETES a DATA file in a partition this replace REPLACES.
    /// Append S0 (x=0 has a + a_old, x=1 has b). Build `replace_partitions(x=0)
    /// .validate_from_snapshot(S0).validate_no_conflicting_deletes()`. A concurrent `delete_files` removes
    /// `a_old.parquet` (in x=0) (S1). Committing the replace must FAIL (Java `validateDeletedDataFiles`).
    #[tokio::test]
    async fn test_replace_partitions_rejects_concurrent_deleted_data_in_replaced_partition() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // S0: x=0 (a, a_old), x=1 (b).
        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/a_old.parquet", 0),
            data_file("test/b.parquet", 1),
        ])
        .await;
        let s0 = table.metadata().current_snapshot().unwrap().snapshot_id();

        let tx = Transaction::new(&table);
        let action = tx
            .replace_partitions()
            .add_file(data_file("test/a2.parquet", 0))
            .validate_from_snapshot(s0)
            .validate_no_conflicting_deletes();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a separate delete_files removes a_old (a data file in replaced x=0).
        let concurrent_tx = Transaction::new(&table);
        let concurrent = concurrent_tx
            .delete_files()
            .delete_file("test/a_old.parquet");
        let concurrent_tx = concurrent.apply(concurrent_tx).unwrap();
        concurrent_tx.commit(&catalog).await.unwrap();

        let err = tx.commit(&catalog).await.expect_err(
            "replace must fail: a concurrent commit deleted data in the replaced partition x=0",
        );
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(!err.retryable());
        assert!(
            err.message().contains("conflicting deleted files"),
            "the error must name the deleted-data conflict, got: {}",
            err.message()
        );
    }

    /// NEGATIVE CONTROL (added delete) — the concurrent delete file lands in the UNTOUCHED partition x=1.
    /// `replace_partitions(x=0).validate_no_conflicting_deletes()` must COMMIT: partition-set membership is
    /// load-bearing, so a row-delete in a non-replaced partition is not a conflict.
    #[tokio::test]
    async fn test_replace_partitions_allows_concurrent_added_delete_in_other_partition() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;

        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 1),
        ])
        .await;
        let s0 = table.metadata().current_snapshot().unwrap().snapshot_id();

        let tx = Transaction::new(&table);
        let action = tx
            .replace_partitions()
            .add_file(data_file("test/a2.parquet", 0))
            .validate_from_snapshot(s0)
            .validate_no_conflicting_deletes();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a position-delete in the UNTOUCHED partition x=1.
        let concurrent_tx = Transaction::new(&table);
        let concurrent = concurrent_tx
            .row_delta()
            .add_deletes(vec![position_delete_file("test/del_x1.parquet", 1)]);
        let concurrent_tx = concurrent.apply(concurrent_tx).unwrap();
        concurrent_tx.commit(&catalog).await.unwrap();

        // The replace must SUCCEED — x=1 is not replaced, so the concurrent delete file does not conflict.
        let table = tx.commit(&catalog).await.expect(
            "replace must succeed: the concurrent delete file was in the untouched partition x=1",
        );
        let live = live_file_paths(&table).await;
        assert!(live.contains("test/a2.parquet"), "x=0 replaced (a2 added)");
        assert!(
            live.contains("test/b.parquet"),
            "x=1 (b) untouched and survives"
        );
    }

    /// NEGATIVE CONTROL (deleted data) — the concurrent data deletion lands in the UNTOUCHED partition x=1.
    /// `replace_partitions(x=0).validate_no_conflicting_deletes()` must COMMIT (partition-set membership is
    /// load-bearing).
    #[tokio::test]
    async fn test_replace_partitions_allows_concurrent_deleted_data_in_other_partition() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // x=1 has both b and b_old so b_old can be concurrently deleted without emptying the partition.
        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 1),
            data_file("test/b_old.parquet", 1),
        ])
        .await;
        let s0 = table.metadata().current_snapshot().unwrap().snapshot_id();

        let tx = Transaction::new(&table);
        let action = tx
            .replace_partitions()
            .add_file(data_file("test/a2.parquet", 0))
            .validate_from_snapshot(s0)
            .validate_no_conflicting_deletes();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): delete b_old (a data file in the UNTOUCHED partition x=1).
        let concurrent_tx = Transaction::new(&table);
        let concurrent = concurrent_tx
            .delete_files()
            .delete_file("test/b_old.parquet");
        let concurrent_tx = concurrent.apply(concurrent_tx).unwrap();
        concurrent_tx.commit(&catalog).await.unwrap();

        let table = tx.commit(&catalog).await.expect(
            "replace must succeed: the concurrent data deletion was in the untouched partition x=1",
        );
        let live = live_file_paths(&table).await;
        assert!(live.contains("test/a2.parquet"), "x=0 replaced (a2 added)");
    }

    /// OFF CONTROL — with conflicting-delete validation NOT enabled (no `validate_no_conflicting_deletes()`),
    /// a concurrent delete file in the replaced partition x=0 does NOT fail the commit (snapshot isolation,
    /// the DEFAULT). Pins that the new check is OPT-IN.
    #[tokio::test]
    async fn test_replace_partitions_without_delete_validation_allows_concurrent_delete_default() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;

        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 1),
        ])
        .await;

        // Build the replace on x=0 WITHOUT enabling delete validation (default = snapshot isolation).
        let tx = Transaction::new(&table);
        let action = tx
            .replace_partitions()
            .add_file(data_file("test/a2.parquet", 0));
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a position-delete in the replaced partition x=0.
        let concurrent_tx = Transaction::new(&table);
        let concurrent = concurrent_tx
            .row_delta()
            .add_deletes(vec![position_delete_file("test/del_x0.parquet", 0)]);
        let concurrent_tx = concurrent.apply(concurrent_tx).unwrap();
        concurrent_tx.commit(&catalog).await.unwrap();

        // With delete validation OFF, the replace COMMITS (default behavior unchanged).
        let table = tx
            .commit(&catalog)
            .await
            .expect("with delete validation OFF, a concurrent delete must not block the commit");
        assert!(
            live_file_paths(&table).await.contains("test/a2.parquet"),
            "x=0 replaced (a2 added) — snapshot isolation"
        );
    }

    /// TX-CAPTURED-START PIN (conflicting deletes) — the conflicting-delete check works WITHOUT an explicit
    /// `validate_from_snapshot`, relying solely on the transaction-captured starting snapshot id surviving
    /// `do_commit`'s re-base. Same race as the REJECT test, but the action calls ONLY
    /// `.validate_no_conflicting_deletes()`. If the start were (re-)read from the refreshed head at
    /// validation time, start == current head ⇒ the concurrent delete set would be empty ⇒ the check would
    /// silently always pass (a serializable-isolation hole). This is the only guard for the tx-captured
    /// field on the delete path.
    #[tokio::test]
    async fn test_replace_partitions_rejects_concurrent_added_delete_using_tx_captured_start() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;

        // S0: x=0 (a), x=1 (b) — the head when the transaction is created below.
        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 1),
        ])
        .await;

        // Build the replace on x=0 with delete validation enabled but WITHOUT validate_from_snapshot — the
        // starting snapshot is the tx-captured head (S0).
        let tx = Transaction::new(&table);
        let action = tx
            .replace_partitions()
            .add_file(data_file("test/a2.parquet", 0))
            .validate_no_conflicting_deletes();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a position-delete in the replaced partition x=0.
        let concurrent_tx = Transaction::new(&table);
        let concurrent = concurrent_tx
            .row_delta()
            .add_deletes(vec![position_delete_file("test/del_x0.parquet", 0)]);
        let concurrent_tx = concurrent.apply(concurrent_tx).unwrap();
        concurrent_tx.commit(&catalog).await.unwrap();

        let err = tx
            .commit(&catalog)
            .await
            .expect_err("conflict must be detected via the tx-captured starting snapshot");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(!err.retryable());
        assert!(err.message().contains("new conflicting delete files"));
    }

    /// FLAG INDEPENDENCE (deletes ⇏ data) — enabling ONLY `validate_no_conflicting_deletes()` must NOT
    /// activate the conflicting-DATA check. A concurrent APPEND (added DATA, not a delete) into the replaced
    /// partition x=0 must COMMIT: the data check is OFF, and an append adds no delete file, so the delete
    /// check finds nothing.
    #[tokio::test]
    async fn test_replace_partitions_delete_validation_does_not_enable_data_validation() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 1),
        ])
        .await;
        let s0 = table.metadata().current_snapshot().unwrap().snapshot_id();

        // ONLY delete validation enabled (NOT data validation).
        let tx = Transaction::new(&table);
        let action = tx
            .replace_partitions()
            .add_file(data_file("test/a2.parquet", 0))
            .validate_from_snapshot(s0)
            .validate_no_conflicting_deletes();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a fast_append adds DATA to the replaced partition x=0.
        let _ = append_files(&catalog, &table, vec![data_file(
            "test/a_concurrent.parquet",
            0,
        )])
        .await;

        // Must COMMIT: the conflicting-DATA check is OFF (only deletes enabled), and an append adds no delete.
        let table = tx.commit(&catalog).await.expect(
            "delete validation alone must not reject a concurrent DATA append (data check stays off)",
        );
        assert!(live_file_paths(&table).await.contains("test/a2.parquet"));
    }

    /// FLAG INDEPENDENCE (data ⇏ deletes) — enabling ONLY `validate_no_conflicting_data()` must NOT activate
    /// the conflicting-DELETE check. A concurrent ADDED DELETE in the replaced partition x=0 must COMMIT: the
    /// delete check is OFF, and an added delete file adds no DATA, so the data check finds nothing. (This is
    /// the dual of the prior test and also confirms `validate_no_conflicting_data` is behavior-preserving:
    /// it ignores concurrent deletes exactly as before this increment.)
    #[tokio::test]
    async fn test_replace_partitions_data_validation_does_not_enable_delete_validation() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;

        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 1),
        ])
        .await;
        let s0 = table.metadata().current_snapshot().unwrap().snapshot_id();

        // ONLY data validation enabled (NOT delete validation).
        let tx = Transaction::new(&table);
        let action = tx
            .replace_partitions()
            .add_file(data_file("test/a2.parquet", 0))
            .validate_from_snapshot(s0)
            .validate_no_conflicting_data();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a row_delta adds a DELETE file in the replaced partition x=0.
        let concurrent_tx = Transaction::new(&table);
        let concurrent = concurrent_tx
            .row_delta()
            .add_deletes(vec![position_delete_file("test/del_x0.parquet", 0)]);
        let concurrent_tx = concurrent.apply(concurrent_tx).unwrap();
        concurrent_tx.commit(&catalog).await.unwrap();

        // Must COMMIT: the conflicting-DELETE check is OFF (only data enabled), and the added delete adds no
        // DATA — so `validate_no_conflicting_data` (unchanged) finds nothing to conflict with.
        let table = tx.commit(&catalog).await.expect(
            "data validation alone must not reject a concurrent ADDED DELETE (delete check stays off)",
        );
        assert!(live_file_paths(&table).await.contains("test/a2.parquet"));
    }

    // ============================================================================================
    // Merge-on-read DELETE-MANIFEST CARRY (Increment 2b — the silent-resurrection bug fix).
    //
    // `existing_manifest` now returns the FULL manifest list (DATA + DELETE) via the shared
    // `SnapshotProducer::current_manifests`, so a `replace_partitions` commit on a table that already carries
    // outstanding position/equality deletes in an UNREPLACED partition preserves those delete manifests
    // instead of dropping them table-wide. This test uses the row_delta crown-jewel fixture (real parquet +
    // a REAL position-delete file written by the production writer + a production scan), so the resurrection
    // physics is proven end-to-end, not just at the manifest-metadata level.
    // ============================================================================================

    /// Write a REAL parquet data file with rows `(x, y, z)` into the table location, routed to partition
    /// `x = part_value`. Returns the finished partitioned [`DataFile`]. Mirrors the row_delta fixture.
    async fn write_data_file(
        table: &Table,
        file_name: &str,
        part_value: i64,
        rows: &[(i64, i64, i64)],
    ) -> DataFile {
        use crate::arrow::schema_to_arrow_schema;

        let schema = table.metadata().current_schema();
        let arrow_schema = Arc::new(schema_to_arrow_schema(schema).unwrap());

        let xs: Vec<i64> = rows.iter().map(|(x, _, _)| *x).collect();
        let ys: Vec<i64> = rows.iter().map(|(_, y, _)| *y).collect();
        let zs: Vec<i64> = rows.iter().map(|(_, _, z)| *z).collect();
        let batch = RecordBatch::try_new(arrow_schema, vec![
            Arc::new(Int64Array::from(xs)) as ArrayRef,
            Arc::new(Int64Array::from(ys)) as ArrayRef,
            Arc::new(Int64Array::from(zs)) as ArrayRef,
        ])
        .unwrap();

        let file_path = format!("{}/data/{}", table.metadata().location(), file_name);
        let output = table.file_io().new_output(file_path).unwrap();
        let parquet_builder = ParquetWriterBuilder::new(
            parquet::file::properties::WriterProperties::builder().build(),
            schema.clone(),
        );
        let mut writer = parquet_builder.build(output).await.unwrap();
        writer.write(&batch).await.unwrap();
        let data_file_builders = writer.close().await.unwrap();

        let mut builder = data_file_builders.into_iter().next().unwrap();
        builder
            .content(DataContentType::Data)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(part_value))]))
            .build()
            .unwrap()
    }

    /// Write a REAL position-delete parquet file (via the production `PositionDeleteFileWriter`) into the
    /// table location, deleting the given `(data_file_path, pos)` pairs, in partition `x = part_value`.
    async fn write_position_delete_file(
        table: &Table,
        part_value: i64,
        deletes: &[(String, i64)],
    ) -> DataFile {
        use arrow_array::StringArray;

        let config = PositionDeleteWriterConfig::new().unwrap();
        let location_gen = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
        let file_name_gen = DefaultFileNameGenerator::new(
            "pos-del".to_string(),
            Some(uuid::Uuid::now_v7().to_string()),
            DataFileFormat::Parquet,
        );
        let parquet_builder = ParquetWriterBuilder::new(
            parquet::file::properties::WriterProperties::builder().build(),
            config.schema().clone(),
        );
        let rolling = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_builder,
            table.file_io().clone(),
            location_gen,
            file_name_gen,
        );
        let partition_key = crate::spec::PartitionKey::new(
            table.metadata().default_partition_spec().as_ref().clone(),
            table.metadata().current_schema().clone(),
            Struct::from_iter([Some(Literal::long(part_value))]),
        );
        let mut writer = PositionDeleteFileWriterBuilder::new(rolling, config.clone())
            .build(Some(partition_key))
            .await
            .unwrap();

        let paths: Vec<&str> = deletes.iter().map(|(p, _)| p.as_str()).collect();
        let positions: Vec<i64> = deletes.iter().map(|(_, pos)| *pos).collect();
        let batch = RecordBatch::try_new(config.arrow_schema().clone(), vec![
            Arc::new(StringArray::from(paths)) as ArrayRef,
            Arc::new(Int64Array::from(positions)) as ArrayRef,
        ])
        .unwrap();
        writer.write(batch).await.unwrap();
        writer.close().await.unwrap().into_iter().next().unwrap()
    }

    /// Scan the table and collect the `y` column values across all returned batches — the real read-side
    /// signal (what a query would see, with merge-on-read deletes applied).
    async fn scan_y_values(table: &Table) -> HashSet<i64> {
        let stream = table
            .scan()
            .select(["y"])
            .build()
            .unwrap()
            .to_arrow()
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let mut values = HashSet::new();
        for batch in batches {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for index in 0..col.len() {
                values.insert(col.value(index));
            }
        }
        values
    }

    /// Count the DELETE-content manifests in the table's current snapshot manifest list (structural
    /// signal, independent of the read path). A replace-partitions must carry outstanding delete manifests
    /// in unreplaced partitions forward, so this count must NOT drop to 0 across the commit.
    async fn count_delete_manifests(table: &Table) -> usize {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        manifest_list
            .entries()
            .iter()
            .filter(|m| m.content == ManifestContentType::Deletes)
            .count()
    }

    /// THE CROWN JEWEL (risk: a `replace_partitions` on a merge-on-read table silently DROPS every
    /// outstanding delete manifest, resurrecting deleted rows even in partitions it did NOT replace). Data
    /// file X (partition a=0) carries a real position delete masking its row y=20; data file Y lives in
    /// partition b=1. Replacing partition b with a new file G (partition 1, y=80) must leave partition a's
    /// delete applying — the scan after the commit is exactly {10, 80} (X's masked y=20 stays absent, Y's
    /// rows are gone, G's y=80 present).
    ///
    /// MUTATION (run manually, then restore): in `ReplacePartitionsOperation::existing_manifest`, filter the
    /// `current_manifests()` result to DATA manifests only (the old data-only behavior) ⇒ this test FAILS
    /// with y=20 resurrected (the scan returns {10, 20, 80}) AND the structural delete-manifest count drops
    /// to 0.
    #[tokio::test]
    async fn test_replace_partitions_preserves_outstanding_delete_manifests_no_resurrection() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;

        // X in partition a=0 with rows y = [10, 20]; Y in partition b=1 with rows y = [60, 70].
        let x = write_data_file(&table, "x.parquet", 0, &[(0, 10, 100), (0, 20, 200)]).await;
        let x_path = x.file_path().to_string();
        let y = write_data_file(&table, "y.parquet", 1, &[(1, 60, 600), (1, 70, 700)]).await;
        let table = append_files(&catalog, &table, vec![x, y]).await;

        // RowDelta a REAL position delete masking X's row at position 1 (y=20), in partition a=0.
        let pos_delete = write_position_delete_file(&table, 0, &[(x_path.clone(), 1)]).await;
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![pos_delete]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        assert_eq!(
            count_delete_manifests(&table).await,
            1,
            "the row_delta must leave one delete manifest in the snapshot"
        );

        // Sanity: before the replace, the scan drops y=20 (X's masked row) and shows Y's rows.
        assert_eq!(
            scan_y_values(&table).await,
            HashSet::from([10, 60, 70]),
            "the position delete masks y=20 from X; Y's rows are present"
        );

        // Dynamic partition overwrite: add G in partition b=1 ⇒ replaces every file in partition 1 (Y).
        // Partition a=0 (and its outstanding delete) must be UNTOUCHED.
        let g = write_data_file(&table, "g.parquet", 1, &[(1, 80, 800)]).await;
        let tx = Transaction::new(&table);
        let action = tx.replace_partitions().add_file(g);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Overwrite
        );

        // SCAN PIN: partition b replaced (Y gone, G's y=80 present) AND partition a's masked y=20 stays
        // absent ⇒ exactly {10, 80}.
        assert_eq!(
            scan_y_values(&table).await,
            HashSet::from([10, 80]),
            "partition b replaced by G AND partition a's masked y=20 stays absent — no resurrection"
        );

        // STRUCTURAL PIN: the delete manifest survived the commit (count must not drop to 0).
        assert_eq!(
            count_delete_manifests(&table).await,
            1,
            "the replace_partitions commit must carry the outstanding delete manifest forward (not drop it)"
        );
    }
}
