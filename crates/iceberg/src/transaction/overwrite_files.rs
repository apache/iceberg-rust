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

//! This module contains the overwrite-files action.
//!
//! [`OverwriteFilesAction`] adds data files AND removes data files in a single `Overwrite` snapshot
//! (Java `BaseOverwriteFiles`). It composes the two existing producer paths: the added files reach
//! the producer exactly as fast-append does (written to a new added manifest), and the deleted files
//! are resolved + filtered out of the current snapshot's manifests exactly as `DeleteFiles` does
//! (via the shared [`SnapshotProducer::resolve_delete_paths`] / `process_deletes` machinery). Both
//! happen in one snapshot.
//!
//! **Operation recorded:** this action records the snapshot operation DYNAMICALLY, mirroring Java
//! `BaseOverwriteFiles.operation()` exactly: a delete-only overwrite (deletes requested, no adds) records
//! [`Operation::Delete`], an add-only overwrite (adds requested, no deletes) records [`Operation::Append`],
//! and an overwrite that both adds and deletes records [`Operation::Overwrite`]. The classification is
//! keyed on the REQUESTED sets (whether any file was added, whether any delete path was requested) —
//! before the delete paths are resolved against the table — matching Java's `addsDataFiles()` /
//! `deletesDataFiles()`. The snapshot summary carries the precise added/deleted file & record counts in
//! every case.
//!
//! **Concurrent-commit conflict validation (`validateNoConflictingData`, OPT-IN):** when enabled via
//! [`OverwriteFilesAction::validate_no_conflicting_data`], the commit is rejected (a non-retryable
//! [`Operation::Overwrite`]-blocking `ValidationException` in Java terms) if any DATA file ADDED by a
//! concurrent commit since the operation's starting snapshot COULD contain records matching the
//! conflict-detection filter. This is the serializable-isolation safety layer (Java
//! `BaseOverwriteFiles.validate` → `validateNewDataFiles` →
//! `MergingSnapshotProducer.validateAddedDataFiles`). It delegates to the shared
//! [`validate_no_conflicting_added_data_files`] helper (the concurrent-added-files walk + bind + per-file
//! inclusive-metrics evaluation), which `RowDelta` also uses so the two checks cannot drift. Default
//! (this not enabled) = snapshot isolation, behavior unchanged.
//!
//! **Out of scope (deferred):**
//! - `overwriteByRowFilter(Expression)` (Java) — needs inclusive/strict metrics evaluators to SELECT
//!   and validate added files by row predicate (Java `validateAddedFilesMatchOverwriteFilter`,
//!   `BaseOverwriteFiles.validate` block 1).
//! - `validateNoConflictingDeletes` (Java `BaseOverwriteFiles.validate` block 3) — concurrent delete-file
//!   conflicts; needs the added-delete-files-since / deleted-data-files-since helpers.
//! - `RowDelta` filter-based conflict validation — a separate follow-up on the same foundation.
//!
//! This increment is the explicit add + delete core plus the filter-based `validateNoConflictingData`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::Result;
use crate::expr::Predicate;
use crate::spec::{DataFile, ManifestEntry, ManifestFile, Operation};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
    validate_no_conflicting_added_data_files,
};
use crate::transaction::{ActionCommit, TransactionAction};

/// A transaction action that overwrites files: it adds data files AND removes data files in a single
/// `Overwrite` snapshot.
///
/// Use [`crate::transaction::Transaction::overwrite_files`] to create one. Accumulate the files to
/// add with [`OverwriteFilesAction::add_file`] / [`OverwriteFilesAction::add_files`] and the files to
/// remove with [`OverwriteFilesAction::delete_file`] / [`OverwriteFilesAction::delete_files`] /
/// [`OverwriteFilesAction::delete_data_files`], then apply and commit the transaction.
///
/// An overwrite with adds-only (no deletes) or deletes-only (no adds) is allowed; a truly-empty
/// overwrite (no adds, no deletes, no snapshot properties) is rejected.
pub struct OverwriteFilesAction {
    /// Data files to add to the table (validated like fast append).
    added_data_files: Vec<DataFile>,
    /// Fully-qualified file paths to remove from the table.
    delete_paths: HashSet<String>,
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    /// Whether concurrent-commit conflict validation is enabled (Java `validateNoConflictingData`). OFF by
    /// default = snapshot isolation (no validation, current behavior). When ON, the commit is rejected if a
    /// concurrent snapshot added a DATA file that could contain records matching the conflict filter.
    validate_no_conflicting_data: bool,
    /// The conflict-detection filter (Java `conflictDetectionFilter`). When `Some`, only concurrently-added
    /// files whose metrics COULD match this predicate are conflicts. When `None`, the filter defaults to
    /// `AlwaysTrue` (any concurrently-added DATA file is a conflict — the most conservative serializable
    /// check), mirroring Java `BaseOverwriteFiles.dataConflictDetectionFilter()` when no filter and no row
    /// filter are set.
    conflict_detection_filter: Option<Predicate>,
    /// An explicit starting snapshot for conflict validation (Java `validateFromSnapshot`). When `None`, the
    /// validation uses the transaction's starting snapshot (the table head when the transaction was created).
    validate_from_snapshot: Option<i64>,
}

impl OverwriteFilesAction {
    pub(crate) fn new() -> Self {
        Self {
            added_data_files: vec![],
            delete_paths: HashSet::default(),
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
            validate_no_conflicting_data: false,
            conflict_detection_filter: None,
            validate_from_snapshot: None,
        }
    }

    /// Add a single [`DataFile`] to the table (Java `OverwriteFiles.addFile`).
    pub fn add_file(mut self, data_file: DataFile) -> Self {
        self.added_data_files.push(data_file);
        self
    }

    /// Add multiple [`DataFile`]s to the table.
    pub fn add_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(data_files);
        self
    }

    /// Delete a single file by its fully-qualified path.
    ///
    /// To remove a file from the table, this path must equal a path in the table's metadata (mirrors
    /// Java `OverwriteFiles.deleteFile` / `MergingSnapshotProducer.delete`).
    pub fn delete_file(mut self, path: impl Into<String>) -> Self {
        self.delete_paths.insert(path.into());
        self
    }

    /// Delete multiple files by their fully-qualified paths.
    pub fn delete_files(mut self, paths: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.delete_paths.extend(paths.into_iter().map(Into::into));
        self
    }

    /// Delete multiple files referenced by [`DataFile`]s (their paths are used).
    pub fn delete_data_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.delete_paths
            .extend(files.into_iter().map(|file| file.file_path));
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

    /// Set snapshot summary properties.
    pub fn set_snapshot_properties(mut self, snapshot_properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }

    /// ENABLE concurrent-commit conflict validation (Java `OverwriteFiles.validateNoConflictingData`): the
    /// commit is rejected with a non-retryable `ValidationException` if any DATA file ADDED by a concurrent
    /// snapshot since the starting snapshot could contain records matching the conflict-detection filter
    /// (see [`Self::conflict_detection_filter`]). This is the serializable-isolation guard against silently
    /// overwriting concurrently-appended data.
    ///
    /// Default (this method NOT called) = snapshot isolation = no validation (current behavior unchanged).
    pub fn validate_no_conflicting_data(mut self) -> Self {
        self.validate_no_conflicting_data = true;
        self
    }

    /// Set the conflict-detection filter (Java `OverwriteFiles.conflictDetectionFilter(Expression)`): only a
    /// concurrently-added DATA file whose metrics COULD contain records matching this predicate is treated as
    /// a conflict. When no filter is set (the default), the conflict filter is `AlwaysTrue` — ANY
    /// concurrently-added data file conflicts (the most conservative serializable check), matching Java
    /// `BaseOverwriteFiles.dataConflictDetectionFilter()` (no filter + no row filter ⇒ `alwaysTrue()`).
    ///
    /// On its own this does NOT enable validation — call [`Self::validate_no_conflicting_data`] for that.
    pub fn conflict_detection_filter(mut self, filter: Predicate) -> Self {
        self.conflict_detection_filter = Some(filter);
        self
    }

    /// Override the snapshot from which concurrent-commit conflict validation starts (Java
    /// `OverwriteFiles.validateFromSnapshot(long)`). By default the validation uses the transaction's
    /// starting snapshot (the table head when [`crate::transaction::Transaction::new`] was called); this lets
    /// the caller pin a specific earlier snapshot id (the snapshot it read when building the overwrite).
    ///
    /// On its own this does NOT enable validation — call [`Self::validate_no_conflicting_data`] for that.
    pub fn validate_from_snapshot(mut self, snapshot_id: i64) -> Self {
        self.validate_from_snapshot = Some(snapshot_id);
        self
    }
}

#[async_trait]
impl TransactionAction for OverwriteFilesAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
        );

        // Validate the added files like fast append: data content type, partition-spec match, and
        // partition-value compatibility (Java `MergingSnapshotProducer.add` checks the spec exists; the
        // producer reuses fast-append's `validate_added_data_files`). The delete paths are resolved and
        // validated (present; mixed present+absent errors) inside the producer's commit via the
        // operation's `delete_files` seam (Java `failMissingDeletePaths`).
        snapshot_producer.validate_added_data_files()?;

        snapshot_producer
            .commit(
                OverwriteFilesOperation {
                    delete_paths: self.delete_paths.clone(),
                    // The recorded operation is classified on the REQUESTED sets (Java `addsDataFiles()` =
                    // requested added files non-empty), evaluated before the deletes are resolved.
                    adds_data_files: !self.added_data_files.is_empty(),
                },
                DefaultManifestProcess,
            )
            .await
    }

    /// Serializable-isolation conflict validation (Java `BaseOverwriteFiles.validate` →
    /// `validateNewDataFiles` → `MergingSnapshotProducer.validateAddedDataFiles`, L163-165 / L391-412). Only
    /// runs when [`Self::validate_no_conflicting_data`] was enabled; otherwise a no-op (snapshot isolation).
    ///
    /// When enabled: compute the effective starting snapshot ([`Self::validate_from_snapshot`] if set, else
    /// the transaction-provided `starting_snapshot_id`) and delegate to the shared
    /// [`validate_no_conflicting_added_data_files`] helper, which enumerates every DATA file ADDED to the
    /// refreshed base by snapshots committed since it (Java `addedDataFiles`) and rejects the commit if ANY
    /// of those files COULD contain records matching the conflict-detection filter (the existing
    /// `InclusiveMetricsEvaluator` over the file's metrics). The conflict filter is the caller's
    /// [`Self::conflict_detection_filter`] when set, else `AlwaysTrue` (any concurrently-added data file
    /// conflicts), mirroring Java `dataConflictDetectionFilter()` (we have no `overwriteByRowFilter`, so the
    /// row-filter branch never applies). The rejection is a NON-retryable `DataInvalid` (Java's non-retryable
    /// `ValidationException`), so the commit retry loop stops and the error propagates.
    ///
    /// **Case sensitivity:** Java binds the conflict filter with `isCaseSensitive()`. This action has no such
    /// field, so the filter is bound case-sensitive (`true`) — the Iceberg/Java default for column resolution.
    async fn validate(
        self: Arc<Self>,
        starting_snapshot_id: Option<i64>,
        current: &Table,
    ) -> Result<()> {
        if !self.validate_no_conflicting_data {
            // Default: snapshot isolation, no conflict check (current behavior unchanged).
            return Ok(());
        }

        // Java `BaseOverwriteFiles` uses `startingSnapshotId` (the `validateFromSnapshot` override) when set,
        // else the operation's starting snapshot. The walk + bind + per-file inclusive-metrics evaluation +
        // non-retryable-conflict error are the shared helper (also used by `RowDelta`).
        let effective_start = self.validate_from_snapshot.or(starting_snapshot_id);
        validate_no_conflicting_added_data_files(
            current,
            effective_start,
            self.conflict_detection_filter.as_ref(),
            true,
        )
        .await
    }
}

/// The [`SnapshotProduceOperation`] for [`OverwriteFilesAction`].
///
/// Classifies the snapshot operation dynamically (Java `BaseOverwriteFiles.operation()`), exposes every
/// current data manifest as the set to filter, and resolves the requested delete paths against the
/// current snapshot's live data entries (the resolved [`DataFile`]s drive the producer's manifest
/// rewrite). The added files reach the producer separately (passed to `SnapshotProducer::new`), so a
/// single snapshot carries both the added manifest and the rewritten (filtered) manifests.
struct OverwriteFilesOperation {
    delete_paths: HashSet<String>,
    /// Whether this overwrite requested any added data files. Combined with whether any delete path was
    /// requested (`delete_paths` non-empty), this classifies the recorded operation the way Java
    /// `BaseOverwriteFiles.operation()` does.
    adds_data_files: bool,
}

impl SnapshotProduceOperation for OverwriteFilesOperation {
    /// Classify the recorded operation exactly as Java `BaseOverwriteFiles.operation()` does, on the
    /// REQUESTED add/delete sets: delete-only → [`Operation::Delete`], add-only → [`Operation::Append`],
    /// both → [`Operation::Overwrite`]. An empty overwrite (neither) is rejected before this is read, so
    /// the both-empty case here would fall through to `Overwrite` and never commit.
    fn operation(&self) -> Operation {
        let deletes_data_files = !self.delete_paths.is_empty();
        match (self.adds_data_files, deletes_data_files) {
            (false, true) => Operation::Delete,
            (true, false) => Operation::Append,
            _ => Operation::Overwrite,
        }
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        Ok(vec![])
    }

    async fn delete_files(&self, snapshot_produce: &SnapshotProducer<'_>) -> Result<Vec<DataFile>> {
        // Resolve the requested paths against the current snapshot's live data entries, validating that
        // EVERY requested path matched a live entry (Java `failMissingDeletePaths`). Shared with
        // `DeleteFiles` via `SnapshotProducer::resolve_delete_paths`.
        snapshot_produce
            .resolve_delete_paths(&self.delete_paths)
            .await
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        // Expose every current data manifest; the producer's `process_deletes` decides per manifest
        // whether to rewrite (to drop deleted files), carry forward unchanged, or drop it.
        snapshot_produce.current_data_manifests().await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use crate::expr::Reference;
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Datum, Literal, ManifestStatus,
        Operation, Struct,
    };
    use crate::table::Table;
    use crate::transaction::tests::make_v3_minimal_table_in_catalog;
    use crate::transaction::{ApplyTransactionAction, Transaction};
    use crate::{Catalog, ErrorKind};

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

    /// Build a data file routed to partition `x = part_value` whose column `y` (schema field id 2, a `long`)
    /// carries `[y_lower, y_upper]` value bounds. The bounds let [`InclusiveMetricsEvaluator`] include or
    /// exclude this file against a conflict-detection filter on `y` — the discriminating input for the
    /// metrics-MATCH vs metrics-EXCLUDE conflict tests. The minimal V3 schema is `x,y,z: long` (ids 1,2,3).
    fn data_file_with_y_bounds(
        path: &str,
        part_value: i64,
        y_lower: i64,
        y_upper: i64,
    ) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(part_value))]))
            .lower_bounds(HashMap::from([(2, Datum::long(y_lower))]))
            .upper_bounds(HashMap::from([(2, Datum::long(y_upper))]))
            .build()
            .unwrap()
    }

    /// Collect the set of live (Added or Existing) data file paths across the table's current
    /// snapshot — the real correctness signal (what a scan would read).
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

    /// THE KEY TEST. Append A, B, C; then `overwrite_files()` delete B + add D → the post-commit SCAN
    /// live set is exactly {A, C, D}, the snapshot operation is Overwrite, B's entry is Deleted, and the
    /// surviving entries keep their provenance. Pins the core compose-add-and-delete-in-one-snapshot
    /// risk: a wrong live set (lost A/C, kept B, or missing D) is silent data corruption.
    #[tokio::test]
    async fn test_overwrite_delete_one_add_one_yields_correct_live_scan_set() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Fast-append A, B, C in one commit (one manifest containing all three).
        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 0),
            data_file("test/c.parquet", 0),
        ])
        .await;

        // Overwrite: delete B, add D — in one snapshot.
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .delete_file("test/b.parquet")
            .add_file(data_file("test/d.parquet", 0));
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // The new snapshot is an Overwrite, and the live scan set is exactly {A, C, D}.
        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Overwrite
        );
        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from([
                "test/a.parquet".to_string(),
                "test/c.parquet".to_string(),
                "test/d.parquet".to_string(),
            ])
        );

        // B's entry is present as Deleted (the rewritten manifest tombstone).
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let mut b_deleted = false;
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                if entry.file_path() == "test/b.parquet" {
                    assert_eq!(entry.status(), ManifestStatus::Deleted);
                    b_deleted = true;
                }
            }
        }
        assert!(b_deleted, "B must appear as a Deleted tombstone");
    }

    /// Pins: an overwrite with adds only (no deletes) succeeds, adds the files, and records the
    /// `Append` operation — matching Java `BaseOverwriteFiles.operation()` (add-only → APPEND). Risk: the
    /// action recording the wrong operation (e.g. always `Overwrite`), or wrongly rejecting an add-only
    /// overwrite. A wrong operation misleads every downstream consumer that branches on snapshot type.
    #[tokio::test]
    async fn test_overwrite_add_only_records_append_operation() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;

        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .add_file(data_file("test/b.parquet", 0));
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Append,
            "an add-only overwrite records Append (Java BaseOverwriteFiles.operation())"
        );
        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/a.parquet".to_string(), "test/b.parquet".to_string()])
        );
    }

    /// Pins: an overwrite with deletes only (no adds) succeeds, removes the file, and records the
    /// `Delete` operation — matching Java `BaseOverwriteFiles.operation()` (delete-only → DELETE). Risk:
    /// the add-only precondition wrongly tripping on a delete-only overwrite, or recording the wrong op
    /// (e.g. always `Overwrite`). A delete-only commit mislabeled `Overwrite` corrupts snapshot history
    /// semantics for consumers that distinguish pure deletes.
    #[tokio::test]
    async fn test_overwrite_delete_only_records_delete_operation() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 0),
        ])
        .await;

        let tx = Transaction::new(&table);
        let action = tx.overwrite_files().delete_file("test/b.parquet");
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Delete,
            "a delete-only overwrite records Delete (Java BaseOverwriteFiles.operation())"
        );
        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/a.parquet".to_string()])
        );
    }

    /// Pins: replacing a file with a new one in the SAME partition (identity(x)=0) overwrites correctly —
    /// the old file is removed and the new file added, both routed to the same partition. Risk: a
    /// partition-keyed rewrite wrongly dropping the new file or keeping the old one in the same
    /// partition (the canonical "replace a partition's contents" overwrite shape).
    #[tokio::test]
    async fn test_overwrite_replaces_file_in_same_partition() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        // old.parquet lives in partition x=0.
        let table = append_files(&catalog, &table, vec![data_file("test/old.parquet", 0)]).await;

        // Replace old.parquet with new.parquet, both in partition x=0.
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .delete_file("test/old.parquet")
            .add_file(data_file("test/new.parquet", 0));
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/new.parquet".to_string()])
        );
    }

    /// Pins: an overwrite that deletes an ABSENT file errors (Java `failMissingDeletePaths`), and does
    /// NOT silently add the added file. Risk: silently dropping the unmatched delete path and committing
    /// a partial overwrite (the added file lands but the intended removal never happened).
    #[tokio::test]
    async fn test_overwrite_delete_absent_file_errors() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;

        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .delete_file("test/does-not-exist.parquet")
            .add_file(data_file("test/b.parquet", 0));
        let tx = action.apply(tx).unwrap();
        let error = tx
            .commit(&catalog)
            .await
            .expect_err("absent delete file must error");
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert!(
            error.message().contains("Missing required files to delete"),
            "unexpected error message: {}",
            error.message()
        );

        // The table is unchanged — the failed overwrite did not add b.parquet.
        let reloaded = catalog.load_table(table.identifier()).await.unwrap();
        assert_eq!(
            live_file_paths(&reloaded).await,
            HashSet::from(["test/a.parquet".to_string()])
        );
    }

    /// Pins: an overwrite that deletes a present file AND an absent file still errors (the present file
    /// is not silently removed while the absent one is ignored). Risk: a partial delete that removes the
    /// matched file and silently skips the unmatched one.
    #[tokio::test]
    async fn test_overwrite_mixed_present_and_absent_delete_errors() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;

        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .delete_files(["test/a.parquet", "test/absent.parquet"]);
        let tx = action.apply(tx).unwrap();
        let error = tx
            .commit(&catalog)
            .await
            .expect_err("mixed present+absent delete must error");
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert!(error.message().contains("test/absent.parquet"));
    }

    /// Pins: a truly-empty overwrite (no adds, no deletes, no snapshot properties) is REJECTED. Risk:
    /// the add+delete precondition relaxation being too permissive and producing an empty no-op
    /// Overwrite snapshot.
    #[tokio::test]
    async fn test_empty_overwrite_is_rejected() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;

        let tx = Transaction::new(&table);
        let action = tx.overwrite_files();
        let tx = action.apply(tx).unwrap();
        let result = tx.commit(&catalog).await;

        assert!(result.is_err(), "a truly-empty overwrite must be rejected");
    }

    /// Pins provenance preservation across snapshots — the #1 corruption risk (the Increment-1 lesson).
    /// When an overwrite rewrites a manifest to delete a file, every SURVIVING entry must be copied
    /// forward as `Existing` carrying its ORIGINAL `snapshot_id` + both sequence numbers (NOT re-stamped
    /// with the new overwrite snapshot/seq), and the added file gets the NEW snapshot's provenance. The
    /// `Deleted` tombstone keeps the removed file's original data/file seq but gets the new snapshot id.
    ///
    /// Risk pinned: a rewrite that re-stamps surviving entries with the commit snapshot/seq is silent
    /// table corruption (wrong data-sequence number breaks merge-on-read delete application and
    /// incremental scans). The other overwrite tests assert only the live PATH set + op, so they pass
    /// under a snapshot-id re-stamp — only this test catches it.
    #[tokio::test]
    async fn test_overwrite_preserves_surviving_entry_provenance_across_snapshots() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Append A in its OWN commit (snapshot S1, data seq 1).
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;
        let s1 = table.metadata().current_snapshot().unwrap().snapshot_id();

        // Append B and C in ONE commit (snapshot S2, data seq 2; one manifest with both).
        let table = append_files(&catalog, &table, vec![
            data_file("test/b.parquet", 0),
            data_file("test/c.parquet", 0),
        ])
        .await;
        let s2 = table.metadata().current_snapshot().unwrap().snapshot_id();
        assert_ne!(s1, s2);

        // Capture original provenance before the overwrite.
        let (a_snap, a_seq, a_fseq) = entry_provenance(&table, "test/a.parquet").await;
        let (b_snap, b_seq, b_fseq) = entry_provenance(&table, "test/b.parquet").await;
        assert_eq!(a_snap, Some(s1), "A added by S1");
        assert_eq!(b_snap, Some(s2), "B added by S2");
        assert_ne!(a_seq, b_seq, "A and B must have different data seq numbers");

        // Overwrite: delete B + add D → rewrites S2's manifest (C survives) and adds a new manifest (D).
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .delete_file("test/b.parquet")
            .add_file(data_file("test/d.parquet", 0));
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let s3 = table.metadata().current_snapshot().unwrap().snapshot_id();
        assert_ne!(s3, s2);

        // C survived: rewritten as Existing, MUST keep S2's snapshot id + seq numbers (NOT S3).
        let (c_snap, c_seq, c_fseq) = entry_provenance(&table, "test/c.parquet").await;
        assert_eq!(
            c_snap,
            Some(s2),
            "surviving C must keep its ORIGINAL snapshot id S2, not the overwrite snapshot S3"
        );
        assert_eq!(
            c_seq, b_seq,
            "surviving C must keep its ORIGINAL data seq, not the overwrite seq"
        );
        assert_eq!(
            c_fseq, b_fseq,
            "surviving C must keep its ORIGINAL file seq"
        );

        // A survived in its own (untouched, carried-forward) manifest with S1 provenance intact.
        let (a2_snap, a2_seq, a2_fseq) = entry_provenance(&table, "test/a.parquet").await;
        assert_eq!(a2_snap, Some(s1), "carried-forward A keeps S1");
        assert_eq!(a2_seq, a_seq, "carried-forward A keeps its data seq");
        assert_eq!(a2_fseq, a_fseq, "carried-forward A keeps its file seq");

        // The added file D gets the NEW overwrite snapshot's provenance (S3 + the new seq).
        let (d_snap, d_seq, _d_fseq) = entry_provenance(&table, "test/d.parquet").await;
        assert_eq!(
            d_snap,
            Some(s3),
            "added D gets the new overwrite snapshot id"
        );
        assert_ne!(
            d_seq, b_seq,
            "added D gets the new (higher) data seq, not the deleted file's seq"
        );

        // The DELETED tombstone for B carries the NEW snapshot id S3 but keeps B's original data/file seq.
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let mut b_tombstone = None;
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                if entry.status() == ManifestStatus::Deleted
                    && entry.file_path() == "test/b.parquet"
                {
                    b_tombstone = Some((
                        entry.snapshot_id(),
                        entry.sequence_number(),
                        entry.file_sequence_number,
                    ));
                }
            }
        }
        let b_tombstone = b_tombstone.expect("B must have a Deleted tombstone");
        assert_eq!(
            b_tombstone.0,
            Some(s3),
            "the Deleted tombstone for B gets the new snapshot id S3"
        );
        assert_eq!(
            b_tombstone.1, b_seq,
            "the Deleted tombstone keeps B's original data seq"
        );
        assert_eq!(
            b_tombstone.2, b_fseq,
            "the Deleted tombstone keeps B's original file seq"
        );
    }

    /// Pins the overwrite SUMMARY reflecting BOTH added and deleted file/record counts (Java
    /// `MergingSnapshotProducer.apply` merges the added-files summary AND the filter-manager's deleted
    /// summary). Risk: the summary only summing added files (the pre-existing behavior) — it would
    /// under-report the overwrite, breaking downstream tooling that reads `deleted-data-files` /
    /// `deleted-records`.
    #[tokio::test]
    async fn test_overwrite_summary_reflects_added_and_deleted_counts() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        // A and B each carry one record.
        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 0),
        ])
        .await;

        // Overwrite: delete B (1 file, 1 record) + add D (1 file, 1 record).
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .delete_file("test/b.parquet")
            .add_file(data_file("test/d.parquet", 0));
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let summary = table.metadata().current_snapshot().unwrap().summary();
        let props = &summary.additional_properties;
        assert_eq!(
            props.get("added-data-files").map(String::as_str),
            Some("1"),
            "summary must report one added data file"
        );
        assert_eq!(
            props.get("added-records").map(String::as_str),
            Some("1"),
            "summary must report one added record"
        );
        assert_eq!(
            props.get("deleted-data-files").map(String::as_str),
            Some("1"),
            "summary must report one deleted data file (Java overwrite summary)"
        );
        assert_eq!(
            props.get("deleted-records").map(String::as_str),
            Some("1"),
            "summary must report one deleted record (Java overwrite summary)"
        );
    }

    /// Read a usize total from a snapshot summary property, defaulting to 0 when absent.
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

    /// Pins the CUMULATIVE running totals across snapshots (Java `SnapshotProducer.summary(previous)`
    /// seeds each snapshot's totals from the PREVIOUS branch head's summary, so `total-data-files` /
    /// `total-records` ACCUMULATE). Append two files, append two more, then overwrite-delete one: the
    /// running totals must be 2 → 4 → 3, never just the current commit's delta.
    ///
    /// Risk pinned: the producer seeding `previous_snapshot` from the not-yet-committed new snapshot id
    /// (the pre-fix bug) makes every snapshot's totals reflect only THIS commit (seed 0), AND a net
    /// removal underflows `0 - removed`. This test FAILS under the old seed-0 logic — the final delete
    /// snapshot would compute `total-data-files = 0 + 0 - 1` (u64 underflow / panic) instead of
    /// `4 - 1 = 3`. It is the regression guard for fix (a) and affects EVERY snapshot action, not just
    /// overwrite.
    #[tokio::test]
    async fn test_running_totals_accumulate_across_snapshots() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Snapshot 1: append A, B (2 files, 2 records). Running totals = 2.
        let table = append_files(&catalog, &table, vec![
            data_file("test/a.parquet", 0),
            data_file("test/b.parquet", 0),
        ])
        .await;
        assert_eq!(
            total(&table, "total-data-files"),
            2,
            "after appending 2 files, total-data-files = 2"
        );
        assert_eq!(
            total(&table, "total-records"),
            2,
            "after appending 2 files (1 record each), total-records = 2"
        );

        // Snapshot 2: append C, D (2 more). Running totals must ACCUMULATE to 4 (not reset to 2).
        let table = append_files(&catalog, &table, vec![
            data_file("test/c.parquet", 0),
            data_file("test/d.parquet", 0),
        ])
        .await;
        assert_eq!(
            total(&table, "total-data-files"),
            4,
            "totals accumulate from the previous branch head: 2 + 2 = 4, not just this commit's 2"
        );
        assert_eq!(
            total(&table, "total-records"),
            4,
            "records accumulate: 2 + 2 = 4"
        );

        // Snapshot 3: overwrite-delete A (net removal). Running totals must be 4 - 1 = 3. Under the old
        // seed-0 logic this computes 0 - 1 and underflows; under the fix it is the correct cumulative 3.
        let tx = Transaction::new(&table);
        let action = tx.overwrite_files().delete_file("test/a.parquet");
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        assert_eq!(
            total(&table, "total-data-files"),
            3,
            "after deleting 1 of 4 files, total-data-files = 4 - 1 = 3 (cumulative, no underflow)"
        );
        assert_eq!(
            total(&table, "total-records"),
            3,
            "after deleting 1 record, total-records = 4 - 1 = 3 (cumulative, no underflow)"
        );
        // The delete-only overwrite is recorded as a Delete (dynamic operation).
        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Delete
        );
    }

    // ============================================================================================
    // Filter-based concurrent-commit conflict validation (Java `validateNoConflictingData` —
    // serializable isolation). Java `BaseOverwriteFiles.validate` → `validateNewDataFiles` →
    // `MergingSnapshotProducer.validateAddedDataFiles` (L163-165 / L391-412): enumerate DATA files added by
    // concurrent commits since the starting snapshot, and reject the commit if ANY could contain records
    // matching the conflict-detection filter (via the inclusive metrics evaluator).
    //
    // The race these tests simulate: an `overwrite_files` is BUILT against table head S0, but BEFORE it
    // commits a SEPARATE `fast_append` lands on the catalog (advancing the head to S1). When the overwrite
    // then commits, `do_commit` refreshes to S1 and runs the action's `validate` against that refreshed base.
    // With `validate_no_conflicting_data()` enabled, a concurrent append whose file could match the conflict
    // filter must FAIL the commit (non-retryable). With validation OFF (the default), it does not.
    // ============================================================================================

    /// Append the given files in a fast-append commit and return the snapshot id that commit produced, plus
    /// the updated table. Used to capture the starting snapshot id S0 before a concurrent commit.
    async fn append_and_snapshot_id(
        catalog: &impl Catalog,
        table: &Table,
        files: Vec<DataFile>,
    ) -> (Table, i64) {
        let table = append_files(catalog, table, files).await;
        let id = table.metadata().current_snapshot().unwrap().snapshot_id();
        (table, id)
    }

    /// NO CONCURRENT COMMIT. With validation enabled but nothing landing concurrently, the overwrite commits
    /// normally (the added-files set is empty ⇒ no conflict). Pins that enabling validation does not block a
    /// race-free commit. Risk: a validation that wrongly fails when there is no concurrent commit at all.
    #[tokio::test]
    async fn test_overwrite_validation_no_concurrent_commit_succeeds() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let (table, s0) =
            append_and_snapshot_id(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;

        // Overwrite delete A + add B with validation enabled — but NO concurrent commit lands.
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .delete_file("test/a.parquet")
            .add_file(data_file("test/b.parquet", 0))
            .validate_from_snapshot(s0)
            .validate_no_conflicting_data();
        let tx = action.apply(tx).unwrap();
        let table = tx
            .commit(&catalog)
            .await
            .expect("a race-free overwrite must commit even with validation enabled");

        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/b.parquet".to_string()])
        );
    }

    /// THE HEADLINE TEST. Append S0. Build an `overwrite_files` with `.conflict_detection_filter(y >= 50)`
    /// and `.validate_no_conflicting_data()`. Then a CONCURRENT `fast_append` lands a file whose `y` bounds
    /// `[60,70]` OVERLAP the filter (could contain `y >= 50`). The overwrite commit must FAIL with a
    /// NON-retryable `DataInvalid` that NAMES the conflicting file.
    ///
    /// Risk pinned: silently overwriting concurrently-appended data that matches the conflict filter = a lost
    /// write under serializable isolation. Without the check the overwrite would commit and drop S1's file.
    #[tokio::test]
    async fn test_overwrite_rejects_concurrent_added_file_matching_filter() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let (table, s0) =
            append_and_snapshot_id(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;

        // Overwrite delete A + add B, conflict filter `y >= 50`, validation enabled, pinned to S0.
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .delete_file("test/a.parquet")
            .add_file(data_file("test/b.parquet", 0))
            .conflict_detection_filter(
                Reference::new("y").greater_than_or_equal_to(Datum::long(50)),
            )
            .validate_from_snapshot(s0)
            .validate_no_conflicting_data();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a file whose y bounds [60,70] overlap `y >= 50` (could match).
        let _concurrent = append_files(&catalog, &table, vec![data_file_with_y_bounds(
            "test/concurrent.parquet",
            0,
            60,
            70,
        )])
        .await;

        let err = tx
            .commit(&catalog)
            .await
            .expect_err("overwrite must fail: a concurrent file could match the conflict filter");

        assert_eq!(
            err.kind(),
            ErrorKind::DataInvalid,
            "a conflict is a non-retryable validation failure (DataInvalid), not a commit conflict"
        );
        assert!(
            !err.retryable(),
            "the validation failure must be NON-retryable so the retry loop stops and it propagates"
        );
        assert!(
            err.message().contains("conflicting files"),
            "the error must name the conflict, got: {}",
            err.message()
        );
        assert!(
            err.message().contains("test/concurrent.parquet"),
            "the error must name the conflicting FILE, got: {}",
            err.message()
        );

        // The catalog head is still S1 (the concurrent append) — the overwrite did NOT commit over it.
        let reloaded = catalog.load_table(table.identifier()).await.unwrap();
        let live = live_file_paths(&reloaded).await;
        assert!(
            live.contains("test/concurrent.parquet"),
            "the concurrently-added file must survive (the conflicting overwrite was rejected)"
        );
        assert!(
            !live.contains("test/b.parquet"),
            "the rejected overwrite's added file must NOT be in the table"
        );
    }

    /// NO-FALSE-CONFLICT TEST. Same setup as the headline, but the concurrent file's `y` bounds `[10,20]` lie
    /// ENTIRELY BELOW the filter `y >= 50` — the inclusive evaluator EXCLUDES it. The overwrite must COMMIT.
    ///
    /// Risk pinned: an over-eager check that rejects ANY concurrent append (ignoring the metrics) would break
    /// legitimate concurrent writes whose data cannot match the filter (a false positive).
    #[tokio::test]
    async fn test_overwrite_allows_concurrent_added_file_excluded_by_filter() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let (table, s0) =
            append_and_snapshot_id(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;

        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .delete_file("test/a.parquet")
            .add_file(data_file("test/b.parquet", 0))
            .conflict_detection_filter(
                Reference::new("y").greater_than_or_equal_to(Datum::long(50)),
            )
            .validate_from_snapshot(s0)
            .validate_no_conflicting_data();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a file whose y bounds [10,20] are entirely BELOW `y >= 50` (cannot match).
        let _concurrent = append_files(&catalog, &table, vec![data_file_with_y_bounds(
            "test/concurrent.parquet",
            0,
            10,
            20,
        )])
        .await;

        // The overwrite must SUCCEED — the concurrent file's metrics exclude the filter.
        let table = tx
            .commit(&catalog)
            .await
            .expect("overwrite must commit: the concurrent file cannot match the conflict filter");

        let live = live_file_paths(&table).await;
        assert!(
            live.contains("test/b.parquet"),
            "the overwrite's added file must be in the table (commit succeeded)"
        );
        // The overwrite re-bases onto S1, so the non-conflicting concurrent file also survives.
        assert!(
            live.contains("test/concurrent.parquet"),
            "the non-conflicting concurrent file survives the re-based overwrite"
        );
        assert!(
            !live.contains("test/a.parquet"),
            "A was deleted by the overwrite"
        );
    }

    /// FLAG-OFF CONTROL. With validation NOT enabled (no `validate_no_conflicting_data()` call), a concurrent
    /// append of a file that WOULD match the filter does NOT fail the commit — this is snapshot isolation, the
    /// DEFAULT behavior, unchanged by this increment.
    ///
    /// Risk pinned: the conflict validation must be OPT-IN — turning it on for every overwrite by default
    /// would change existing behavior and break callers relying on snapshot isolation.
    #[tokio::test]
    async fn test_overwrite_without_validation_allows_conflicting_concurrent_append() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;

        // Build an overwrite WITHOUT enabling validation (default = snapshot isolation). A conflict filter is
        // even provided, to prove it is inert without the flag.
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .delete_file("test/a.parquet")
            .add_file(data_file("test/b.parquet", 0))
            .conflict_detection_filter(
                Reference::new("y").greater_than_or_equal_to(Datum::long(50)),
            );
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a file whose y bounds [60,70] WOULD match `y >= 50` if validation were on.
        let _concurrent = append_files(&catalog, &table, vec![data_file_with_y_bounds(
            "test/concurrent.parquet",
            0,
            60,
            70,
        )])
        .await;

        // With validation OFF, the overwrite COMMITS (default behavior unchanged).
        let table = tx.commit(&catalog).await.expect(
            "with validation OFF, a conflicting concurrent append must not block the commit",
        );

        let live = live_file_paths(&table).await;
        assert!(
            live.contains("test/b.parquet"),
            "the overwrite committed (snapshot isolation, no conflict check)"
        );
    }

    /// NONE-FILTER DEFAULT TEST. With validation enabled and NO `conflict_detection_filter` set, the conflict
    /// filter defaults to `AlwaysTrue` (Java `dataConflictDetectionFilter()` → `alwaysTrue()`) — so ANY
    /// concurrently-added data file is a conflict, even one with no bounds at all.
    ///
    /// Risk pinned: a `None` filter silently behaving as "no conflict" (the OPPOSITE of the conservative
    /// serializable default) would let every concurrent append through — a serializable-isolation hole.
    #[tokio::test]
    async fn test_overwrite_none_filter_treats_any_concurrent_add_as_conflict() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let (table, s0) =
            append_and_snapshot_id(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;

        // Overwrite with validation enabled but NO conflict_detection_filter ⇒ AlwaysTrue default.
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .delete_file("test/a.parquet")
            .add_file(data_file("test/b.parquet", 0))
            .validate_from_snapshot(s0)
            .validate_no_conflicting_data();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a plain file with NO bounds — still a conflict under AlwaysTrue.
        let _concurrent = append_files(&catalog, &table, vec![data_file(
            "test/concurrent.parquet",
            0,
        )])
        .await;

        let err = tx
            .commit(&catalog)
            .await
            .expect_err("a None filter defaults to AlwaysTrue: any concurrent add is a conflict");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(!err.retryable());
        assert!(err.message().contains("test/concurrent.parquet"));
    }

    /// VALIDATE-FROM-SNAPSHOT OVERRIDE TEST. The `validate_from_snapshot(id)` override changes which commits
    /// count as concurrent. Append S0, then append S1 (BEFORE the transaction is built), then build the
    /// overwrite. With `validate_from_snapshot(S1)`, the file added in S1 is NOT concurrent (it is at/at-or-
    /// before the start) — so a `None`-filter (AlwaysTrue) validation does NOT flag it and the commit
    /// succeeds. (Without the override, the tx-captured start would be S1's head and the result is the same
    /// here; the discriminating direction is below.)
    ///
    /// The KEY half: build the overwrite when the head is already S1, set `validate_from_snapshot(S0)`
    /// (an EARLIER snapshot), and confirm S1's file IS now counted as concurrent ⇒ rejected. This proves the
    /// override widens the concurrent window to include commits between S0 and S1.
    ///
    /// Risk pinned: ignoring the `validate_from_snapshot` override (always using the tx start) would miss a
    /// conflict the caller explicitly asked to guard against by reading from an earlier snapshot.
    #[tokio::test]
    async fn test_overwrite_validate_from_snapshot_override_changes_concurrent_window() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // S0: a. Capture S0.
        let (table, s0) =
            append_and_snapshot_id(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;
        // S1: a file added BEFORE the transaction is built (so it is part of the base, not "concurrent" by
        // the default tx-captured start).
        let (table, _s1) =
            append_and_snapshot_id(&catalog, &table, vec![data_file("test/s1.parquet", 0)]).await;

        // Build the overwrite when the head is S1. Override the start to the EARLIER S0 so S1 counts as
        // concurrent. None filter ⇒ AlwaysTrue ⇒ S1's added file is a conflict.
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .delete_file("test/a.parquet")
            .add_file(data_file("test/b.parquet", 0))
            .validate_from_snapshot(s0)
            .validate_no_conflicting_data();
        let tx = action.apply(tx).unwrap();

        let err = tx.commit(&catalog).await.expect_err(
            "validate_from_snapshot(S0) widens the window to include S1's add ⇒ conflict",
        );
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(!err.retryable());
        assert!(err.message().contains("test/s1.parquet"));
    }

    /// NEGATIVE HALF of the override test: with `validate_from_snapshot(S1)` (the CURRENT head when the tx is
    /// built), S1's file is at the start boundary and is NOT concurrent — so the same overwrite COMMITS. This
    /// pins that the override genuinely shifts the boundary (the S0 half above rejects the SAME S1 file).
    #[tokio::test]
    async fn test_overwrite_validate_from_snapshot_at_head_finds_no_conflict() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;
        let (table, s1) =
            append_and_snapshot_id(&catalog, &table, vec![data_file("test/s1.parquet", 0)]).await;

        // Override the start to S1 (the current head) — nothing is concurrent.
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .delete_file("test/a.parquet")
            .add_file(data_file("test/b.parquet", 0))
            .validate_from_snapshot(s1)
            .validate_no_conflicting_data();
        let tx = action.apply(tx).unwrap();
        let table = tx
            .commit(&catalog)
            .await
            .expect("with start = current head, nothing is concurrent ⇒ commit succeeds");

        assert!(live_file_paths(&table).await.contains("test/b.parquet"));
    }

    /// TX-CAPTURED START SURVIVES RE-BASE. The conflict check works WITHOUT an explicit
    /// `validate_from_snapshot`, relying solely on the transaction-captured starting snapshot id surviving
    /// `do_commit`'s re-base. The action calls ONLY `.validate_no_conflicting_data()` (None filter ⇒
    /// AlwaysTrue). The starting snapshot is the one captured in `Transaction::new` (= S0); `do_commit`
    /// overwrites `self.table` with the refreshed base (S1), but `starting_snapshot_id` must SURVIVE — so the
    /// concurrent S1 is still enumerated and rejected.
    ///
    /// Risk pinned: if the start were re-read from the refreshed head at validation time, start == current
    /// head ⇒ the concurrent set is empty ⇒ the check silently always passes (a serializable-isolation hole).
    /// All the other enabled tests pin `validate_from_snapshot`, so this is the only guard that the
    /// `Transaction::new` capture survives the re-base for OverwriteFiles.
    #[tokio::test]
    async fn test_overwrite_rejects_concurrent_using_tx_captured_starting_snapshot() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;

        // Build the overwrite with validation enabled but WITHOUT validate_from_snapshot — the start is the
        // tx-captured head (S0).
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite_files()
            .delete_file("test/a.parquet")
            .add_file(data_file("test/b.parquet", 0))
            .validate_no_conflicting_data();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1).
        let _concurrent = append_files(&catalog, &table, vec![data_file(
            "test/concurrent.parquet",
            0,
        )])
        .await;

        let err = tx
            .commit(&catalog)
            .await
            .expect_err("conflict must be detected via the tx-captured starting snapshot");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(!err.retryable());
        assert!(err.message().contains("test/concurrent.parquet"));
    }
}
