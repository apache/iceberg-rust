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

//! This module contains the rewrite-files action (the compaction-commit primitive).
//!
//! [`RewriteFilesAction`] atomically REPLACES a set of data files with a new set of data files in a
//! single `Replace` snapshot (Java `BaseRewriteFiles`). This is the commit primitive a compaction job
//! uses: it rewrites the same logical rows into a new (usually smaller / better-laid-out) set of files,
//! so the live row set must be logically unchanged. It composes the two existing producer paths exactly
//! like [`crate::transaction::overwrite_files::OverwriteFilesAction`]: the added files reach the producer
//! as fast-append does (written to a new added manifest), and the to-delete files are resolved BY PATH +
//! filtered out of the current snapshot's manifests via the shared [`SnapshotProducer::resolve_delete_paths`]
//! / `process_deletes` machinery. Both happen in one snapshot.
//!
//! **Operation recorded:** always [`Operation::Replace`] (Java `BaseRewriteFiles.operation()` returns
//! `DataOperations.REPLACE`). Unlike `OverwriteFiles` (whose operation is dynamic), a rewrite is always
//! a Replace — the row set is unchanged, only the physical files differ.
//!
//! **Validation mirrored (cited against `core/.../BaseRewriteFiles.java`):**
//! - the constructor calls `failMissingDeletePaths()` — every data file to delete MUST be present in the
//!   current snapshot (error if absent). This is enforced by the shared `resolve_delete_paths` (Java
//!   `failMissingDeletePaths` / `validateRequiredDeletes`).
//! - `validate()` → `validateReplacedAndAddedFiles()` requires (for the data-file case):
//!   1. `deletesDataFiles() || deletesDeleteFiles()` ⇒ the files-to-delete set must be non-empty
//!      (**"Files to delete cannot be empty"**). A delete-only rewrite (delete files, add nothing) is
//!      legal; an add-only or fully-empty rewrite is rejected.
//!   2. `deletesDataFiles() || !addsDataFiles()` ⇒ data files may be added only if data files are being
//!      deleted (**"Data files to add must be empty because there's no data file to be rewritten"**).
//!      For the data-only case this is subsumed by (1), but the distinct message is mirrored.
//! - added files are validated as usual (data content type + partition-spec match + partition-value
//!   compatibility, via [`SnapshotProducer::validate_added_data_files`]).
//!
//! **Summary:** the snapshot summary carries the added + deleted file & record counts (the producer
//! already merges the added-files and removed-files summaries) under `Operation::Replace`.
//!
//! **Provenance:** surviving (non-rewritten) entries are copied forward as `Existing` keeping their
//! original snapshot id + both sequence numbers; this is inherited from the Increment-1 rewrite machinery.
//!
//! **`dataSequenceNumber` preservation (Java `RewriteFiles.dataSequenceNumber(seq)` /
//! `setNewDataFilesDataSequenceNumber`, `BaseRewriteFiles` L79-82):** by default a rewrite gives the added
//! files a FRESH (higher) data sequence number via the standard add path. Outstanding merge-on-read EQUALITY
//! deletes apply only to data with a STRICTLY LOWER data seq (`data_seq < delete_seq`), so a fresh higher seq
//! makes those deletes stop applying to the rewritten data and silently RESURRECT deleted rows. The
//! [`RewriteFilesAction::data_sequence_number`] builder lets a compaction stamp the added data files with the
//! (max) data sequence number of the files they replace, so any outstanding equality delete still applies.
//! It threads to the producer via [`SnapshotProducer::with_new_data_files_data_sequence_number`]. A negative
//! `seq` is REJECTED at commit with [`ErrorKind::DataInvalid`] — a Rust-only fail-loud addition: the manifest
//! writer silently STRIPS a negative explicit seq back into re-inheritance (the exact resurrection corruption
//! this path exists to prevent), so the action refuses one rather than corrupt the table. Without
//! `data_sequence_number` the added files take a fresh seq and outstanding equality deletes stop applying to
//! them — this is the Java-identical caller responsibility (Java's `BaseRewriteFiles` never inspects existing
//! delete manifests and has no such guard); preserving seq is the caller's job when the table carries deletes.
//!
//! **Concurrent-commit conflict validation (`validate(base, parent)`, Java `BaseRewriteFiles.validate`
//! L135-142):** [`RewriteFilesAction::validate`] runs against the refreshed base before re-apply. When this
//! rewrite REPLACES data files, it calls the SHARED [`validate_no_new_deletes_for_data_files`] helper
//! (UNCHANGED — the same one `OverwriteFiles` uses) to reject the commit if a concurrent commit since the
//! starting snapshot added a row-level DELETE file that applies to one of the replaced data files. This is
//! UNCONDITIONAL (Java has no opt-in flag here). It passes `ignore_equality_deletes =
//! self.data_sequence_number.is_some()` (Java L475-479, `newDataFilesDataSequenceNumber != null`): when the
//! data seq is preserved, a concurrently-added EQUALITY delete still applies to the rewritten data (no
//! conflict, ignored — Java L500-517 javadoc), but a new POSITION delete is ALWAYS fatal (its path target
//! dies with the replaced file). The starting snapshot is [`RewriteFilesAction::validate_from_snapshot`] if
//! set, else the transaction-captured starting snapshot (Java `startingSnapshotId`). A conflict is a
//! non-retryable [`ErrorKind::DataInvalid`] (Java's non-retryable `ValidationException`).
//!
//! **Out of scope (deferred, with precise reasons):**
//! - **DELETE-file rewrite** — Java `RewriteFiles` can also rewrite position-delete / deletion-vector
//!   files (`deleteFile(DeleteFile)` / `addFile(DeleteFile)`). That needs the delete-file write path (a
//!   later Phase-2 increment). This action rewrites DATA files only. (It carries the third Java precondition
//!   `deletesDeleteFiles() || !addsDeleteFiles()` with it.)

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::{Error, ErrorKind, Result};
use crate::spec::{DataFile, ManifestEntry, ManifestFile, Operation};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
    validate_no_new_deletes_for_data_files,
};
use crate::transaction::{ActionCommit, TransactionAction};

/// A transaction action that rewrites data files: it atomically removes a set of data files and adds a
/// new set of data files in a single `Replace` snapshot — the compaction-commit primitive.
///
/// Use [`crate::transaction::Transaction::rewrite_files`] to create one. The primary entry point is
/// [`RewriteFilesAction::rewrite_files`] (the files to delete + the files to add); the
/// [`RewriteFilesAction::delete_file`] / [`RewriteFilesAction::add_file`] (and `*_files`) builders are the
/// incremental equivalents. The files to delete are passed as [`DataFile`]s (callers hold them after a
/// scan) and resolved against the current snapshot BY PATH.
///
/// The set of files to delete MUST be non-empty (Java `BaseRewriteFiles` "Files to delete cannot be
/// empty"); a delete-only rewrite is allowed, but an add-only rewrite is rejected. Deleting a file that is
/// not present in the current snapshot errors (Java `failMissingDeletePaths`).
///
/// To preserve outstanding merge-on-read EQUALITY deletes across the rewrite, call
/// [`RewriteFilesAction::data_sequence_number`] with the (max) data sequence number of the replaced files
/// (Java `RewriteFiles.dataSequenceNumber`); without it the added files take a fresh, higher sequence number
/// and outstanding equality deletes stop applying to them (the Java-identical caller responsibility — there
/// is no guard against it). Concurrent row-level deletes that would conflict are rejected by
/// [`RewriteFilesAction::validate`] (Java `validateNoNewDeletesForDataFiles`).
pub struct RewriteFilesAction {
    /// Data files to add to the table (validated like fast append).
    added_data_files: Vec<DataFile>,
    /// Data files to remove from the table (their paths are resolved against the current snapshot).
    deleted_data_files: Vec<DataFile>,
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    /// An explicit DATA sequence number to stamp on the added files (Java
    /// `RewriteFiles.dataSequenceNumber` → `newDataFilesDataSequenceNumber`). `Some(seq)` preserves the
    /// replaced files' data seq so outstanding equality deletes still apply; `None` (the default) gives the
    /// added files a fresh, higher seq via the standard add path. A negative value is rejected at commit.
    data_sequence_number: Option<i64>,
    /// An explicit starting snapshot for concurrent-commit conflict validation (Java
    /// `RewriteFiles.validateFromSnapshot`). When `None`, the validation uses the transaction's starting
    /// snapshot (the table head when the transaction was created).
    validate_from_snapshot: Option<i64>,
}

impl RewriteFilesAction {
    pub(crate) fn new() -> Self {
        Self {
            added_data_files: vec![],
            deleted_data_files: vec![],
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
            data_sequence_number: None,
            validate_from_snapshot: None,
        }
    }

    /// Rewrite `files_to_delete` into `files_to_add` (Java `RewriteFiles.rewriteFiles(filesToDelete,
    /// filesToAdd)`) — the primary entry point. Equivalent to calling [`RewriteFilesAction::delete_file`]
    /// for each file to delete and [`RewriteFilesAction::add_file`] for each file to add.
    pub fn rewrite_files(
        mut self,
        files_to_delete: impl IntoIterator<Item = DataFile>,
        files_to_add: impl IntoIterator<Item = DataFile>,
    ) -> Self {
        self.deleted_data_files.extend(files_to_delete);
        self.added_data_files.extend(files_to_add);
        self
    }

    /// Add a single rewritten [`DataFile`] to the table (Java `RewriteFiles.addFile`).
    pub fn add_file(mut self, data_file: DataFile) -> Self {
        self.added_data_files.push(data_file);
        self
    }

    /// Add multiple rewritten [`DataFile`]s to the table.
    pub fn add_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(data_files);
        self
    }

    /// Remove a single rewritten [`DataFile`] from the table (Java `RewriteFiles.deleteFile`). Its path
    /// must equal a live file path in the current snapshot, or the commit errors.
    pub fn delete_file(mut self, data_file: DataFile) -> Self {
        self.deleted_data_files.push(data_file);
        self
    }

    /// Remove multiple rewritten [`DataFile`]s from the table.
    pub fn delete_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.deleted_data_files.extend(data_files);
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

    /// Preserve a DATA sequence number on the added files (Java `RewriteFiles.dataSequenceNumber(long)` →
    /// `setNewDataFilesDataSequenceNumber`, `BaseRewriteFiles` L79-82). Pass the (max) data sequence number
    /// of the files being replaced so any outstanding merge-on-read EQUALITY delete still applies to the
    /// rewritten data (`data_seq < delete_seq`); without this the added files take a fresh, higher seq and
    /// the old equality deletes stop applying, resurrecting deleted rows.
    ///
    /// `sequence_number` must be NON-NEGATIVE; a negative value is rejected at commit with
    /// [`ErrorKind::DataInvalid`] (the manifest writer silently strips a negative explicit seq back into
    /// re-inheritance — the exact corruption this exists to prevent — so the action fails loudly instead).
    pub fn data_sequence_number(mut self, sequence_number: i64) -> Self {
        self.data_sequence_number = Some(sequence_number);
        self
    }

    /// Override the snapshot from which concurrent-commit conflict validation starts (Java
    /// `RewriteFiles.validateFromSnapshot(long)`). By default the validation uses the transaction's starting
    /// snapshot (the table head when [`crate::transaction::Transaction::new`] was called); this lets the
    /// caller pin a specific earlier snapshot id (the snapshot it read when building the rewrite).
    pub fn validate_from_snapshot(mut self, snapshot_id: i64) -> Self {
        self.validate_from_snapshot = Some(snapshot_id);
        self
    }

    /// The set of paths to delete, derived from the to-delete [`DataFile`]s.
    fn delete_paths(&self) -> HashSet<String> {
        self.deleted_data_files
            .iter()
            .map(|file| file.file_path.clone())
            .collect()
    }
}

#[async_trait]
impl TransactionAction for RewriteFilesAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        // Java `BaseRewriteFiles.validateReplacedAndAddedFiles()` (the data-file subset). Run BEFORE the
        // producer's own machinery so the exact Java message surfaces.
        //
        // Precondition (1): `deletesDataFiles() || deletesDeleteFiles()` — the files-to-delete set must be
        // non-empty ("Files to delete cannot be empty"). A delete-only rewrite is legal; an add-only or
        // fully-empty rewrite is rejected.
        //
        // Java precondition (2) `deletesDataFiles() || !addsDataFiles()` ("Data files to add must be empty
        // because there's no data file to be rewritten") is SUBSUMED here: with DELETE-file rewrite out of
        // scope, the only way to delete files is to delete DATA files, so `deletesDataFiles()` is true
        // exactly when this delete set is non-empty — i.e. whenever (1) passes — and an add is therefore
        // always permitted. (When the delete-file write path lands, a delete-file-only rewrite that tries
        // to add data files will need precondition (2) enforced explicitly.)
        if self.deleted_data_files.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Files to delete cannot be empty",
            ));
        }

        // Fail-loud on a negative `dataSequenceNumber` (Rust-only addition, not a Java mirror): the manifest
        // writer's `add_entry` silently STRIPS a negative explicit sequence number back to `None`, which
        // re-inherits the new (higher) snapshot seq at read time — the exact resurrection corruption the
        // preservation path exists to prevent. Reject it here rather than silently corrupt the table.
        if let Some(sequence_number) = self.data_sequence_number
            && sequence_number < 0
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Invalid data sequence number for rewrite: {sequence_number} (must be non-negative; \
                     a negative value would be stripped into sequence-number re-inheritance and resurrect \
                     deleted rows)"
                ),
            ));
        }

        let mut snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
        );

        // Preserve the replaced files' data sequence number on the added files when requested (Java
        // `RewriteFiles.dataSequenceNumber` → the producer's `newDataFilesDataSequenceNumber`). `None` leaves
        // the added files inheriting the new snapshot's seq (the default add path).
        if let Some(sequence_number) = self.data_sequence_number {
            snapshot_producer =
                snapshot_producer.with_new_data_files_data_sequence_number(sequence_number);
        }

        // Validate the added files like fast append: data content type, partition-spec match, and
        // partition-value compatibility (Java `MergingSnapshotProducer.add`). The delete paths are resolved
        // and validated (every one present; an absent path errors) inside the producer's commit via the
        // operation's `delete_files` seam (Java `failMissingDeletePaths`).
        snapshot_producer.validate_added_data_files()?;

        snapshot_producer
            .commit(
                RewriteFilesOperation {
                    delete_paths: self.delete_paths(),
                },
                DefaultManifestProcess,
            )
            .await
    }

    /// Concurrent-commit conflict validation (Java `BaseRewriteFiles.validate`, `core/BaseRewriteFiles.java`
    /// L135-142). Runs against the refreshed base before this action's updates are re-applied. When this
    /// rewrite REPLACES data files (the only case Java validates: `!replacedDataFiles.isEmpty()`), it rejects
    /// the commit if a concurrent commit since the starting snapshot added a row-level DELETE file that
    /// APPLIES to one of the replaced data files. This is UNCONDITIONAL — Java has no opt-in flag here.
    ///
    /// Delegates to the SHARED [`validate_no_new_deletes_for_data_files`] helper (the same one
    /// `OverwriteFiles` uses) with `ignore_equality_deletes = self.data_sequence_number.is_some()` (Java
    /// L475-479, `newDataFilesDataSequenceNumber != null`): when the data seq is preserved, a
    /// concurrently-added EQUALITY delete still applies to the rewritten data (no conflict — Java L500-517),
    /// so only a new POSITION delete is fatal ("Cannot commit, found new position delete for replaced data
    /// file: <path>"); when the seq is NOT preserved, ANY applicable delete is fatal ("Cannot commit, found
    /// new delete for replaced data file: <path>"). A conflict is a non-retryable [`ErrorKind::DataInvalid`]
    /// (Java's non-retryable `ValidationException`), so the commit retry loop stops.
    ///
    /// The starting snapshot is [`Self::validate_from_snapshot`] when set, else the transaction-captured
    /// `starting_snapshot_id` (Java `startingSnapshotId`). CRITICAL: the transaction-captured base is the head
    /// when `Transaction::new` ran, NOT the refreshed head — re-reading the refreshed head would make the
    /// concurrent set empty and silently pass. A no-op on a V1 table or when there is no current snapshot
    /// (Java L526-528, the shared helper's `parent == null || formatVersion < 2` early return).
    async fn validate(
        self: Arc<Self>,
        starting_snapshot_id: Option<i64>,
        current: &Table,
    ) -> Result<()> {
        // Java L137: only validate when this rewrite replaces data files. An add-only / empty rewrite is
        // rejected in `commit`, so in practice `deleted_data_files` is non-empty whenever a rewrite commits,
        // but the guard mirrors Java's `if (!replacedDataFiles.isEmpty())` exactly.
        if self.deleted_data_files.is_empty() {
            return Ok(());
        }

        // Java `BaseRewriteFiles` uses `startingSnapshotId` (the `validateFromSnapshot` override) when set,
        // else the operation's starting snapshot. `starting_snapshot_id` is the TRANSACTION-captured base, not
        // the refreshed head.
        let effective_start = self.validate_from_snapshot.or(starting_snapshot_id);

        // Java L140: `validateNoNewDeletesForDataFiles(base, startingSnapshotId, replacedDataFiles, parent)`.
        // The 4-arg Java overload passes `ignoreEqualityDeletes = newDataFilesDataSequenceNumber != null`
        // (Java L475-479) — preserved-seq ⇒ ignore equality deletes (they still apply, no conflict). No data
        // filter (Java passes `null`).
        validate_no_new_deletes_for_data_files(
            current,
            effective_start,
            None,
            &self.deleted_data_files,
            self.data_sequence_number.is_some(),
        )
        .await
    }
}

/// The [`SnapshotProduceOperation`] for [`RewriteFilesAction`].
///
/// Records [`Operation::Replace`] (Java `BaseRewriteFiles.operation()` = `DataOperations.REPLACE`), exposes
/// every current data manifest as the set to filter, and resolves the requested delete paths against the
/// current snapshot's live data entries (the resolved [`DataFile`]s drive the producer's by-path manifest
/// rewrite). The added files reach the producer separately (passed to `SnapshotProducer::new`), so a single
/// snapshot carries both the added manifest and the rewritten (filtered) manifests.
struct RewriteFilesOperation {
    delete_paths: HashSet<String>,
}

impl SnapshotProduceOperation for RewriteFilesOperation {
    fn operation(&self) -> Operation {
        // Java `BaseRewriteFiles.operation()` returns `DataOperations.REPLACE`.
        Operation::Replace
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
        // `DeleteFiles` / `OverwriteFiles` via `SnapshotProducer::resolve_delete_paths`.
        snapshot_produce
            .resolve_delete_paths(&self.delete_paths)
            .await
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        // Expose EVERY current manifest — DATA and DELETE — via the shared
        // [`SnapshotProducer::current_manifests`]. DELETE manifests carry forward UNCHANGED: a rewrite must
        // preserve outstanding merge-on-read deletes, otherwise the rewrite snapshot would silently drop
        // every delete and resurrect deleted rows — which, combined with `dataSequenceNumber` preservation,
        // is exactly the corruption this increment prevents. The conservative dangling-delete posture (no
        // pruning) is documented on the helper.
        snapshot_produce.current_manifests().await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array, RecordBatch};
    use futures::TryStreamExt;

    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, Manifest,
        ManifestStatus, Operation, Struct,
    };
    use crate::table::Table;
    use crate::transaction::tests::make_v3_minimal_table_in_catalog;
    use crate::transaction::{ApplyTransactionAction, Transaction};
    use crate::writer::base_writer::equality_delete_writer::{
        EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig,
    };
    use crate::writer::base_writer::position_delete_writer::{
        PositionDeleteFileWriterBuilder, PositionDeleteWriterConfig,
    };
    use crate::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use crate::writer::file_writer::{FileWriter, FileWriterBuilder, ParquetWriterBuilder};
    use crate::writer::{IcebergWriter, IcebergWriterBuilder};
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

    /// Assert that `path` appears as a `Deleted` tombstone in the table's current snapshot.
    async fn assert_deleted_tombstone(table: &Table, path: &str) {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                if entry.file_path() == path && entry.status() == ManifestStatus::Deleted {
                    return;
                }
            }
        }
        panic!("{path} must appear as a Deleted tombstone");
    }

    /// Read a u64 total from a snapshot summary property, defaulting to 0 when absent.
    fn summary_prop(table: &Table, prop: &str) -> Option<String> {
        table
            .metadata()
            .current_snapshot()
            .unwrap()
            .summary()
            .additional_properties
            .get(prop)
            .cloned()
    }

    /// THE KEY TEST (risk: wrong live set = silent data loss / corruption). Append A, B, C; then
    /// `rewrite_files(delete=[A, B], add=[D])` → the post-commit SCAN live set is exactly {C, D}, the
    /// snapshot operation is Replace, A & B are Deleted tombstones, and C (untouched) keeps its provenance.
    /// A wrong live set (lost C, kept A/B, or missing D) is silent data corruption.
    #[tokio::test]
    async fn test_rewrite_delete_two_add_one_yields_correct_live_scan_set() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let a = data_file("test/a.parquet", 0);
        let b = data_file("test/b.parquet", 0);
        let c = data_file("test/c.parquet", 0);
        // Fast-append A, B, C in one commit (one manifest containing all three).
        let table = append_files(&catalog, &table, vec![a.clone(), b.clone(), c.clone()]).await;
        let s_append = table.metadata().current_snapshot().unwrap().snapshot_id();
        let (_, c_seq, c_fseq) = entry_provenance(&table, "test/c.parquet").await;

        // Rewrite: replace A, B with D — in one Replace snapshot.
        let tx = Transaction::new(&table);
        let action = tx.rewrite_files(vec![a, b], vec![data_file("test/d.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // The new snapshot is a Replace, and the live scan set is exactly {C, D}.
        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Replace
        );
        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/c.parquet".to_string(), "test/d.parquet".to_string()])
        );

        // A and B are present as Deleted tombstones (the rewritten manifest).
        assert_deleted_tombstone(&table, "test/a.parquet").await;
        assert_deleted_tombstone(&table, "test/b.parquet").await;

        // C (untouched) keeps its ORIGINAL provenance — it was NOT re-stamped with the rewrite snapshot.
        let (c_snap2, c_seq2, c_fseq2) = entry_provenance(&table, "test/c.parquet").await;
        assert_eq!(
            c_snap2,
            Some(s_append),
            "surviving C keeps its original snapshot id, not the rewrite snapshot"
        );
        assert_eq!(c_seq2, c_seq, "surviving C keeps its original data seq");
        assert_eq!(c_fseq2, c_fseq, "surviving C keeps its original file seq");
    }

    /// Risk: corruption across manifests — a rewrite must correctly drop files that live in DIFFERENT
    /// source manifests and preserve the rest. Append A (snapshot 1), B+C (snapshot 2): A lives in its own
    /// manifest, B+C in another. Rewrite delete=[A, B] add=[D] → live {C, D}: the resolver must reach both
    /// manifests, drop A from one and B from the other, keep C, and add D.
    #[tokio::test]
    async fn test_rewrite_across_multiple_manifests() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let a = data_file("test/a.parquet", 0);
        let table = append_files(&catalog, &table, vec![a.clone()]).await;
        let b = data_file("test/b.parquet", 0);
        let c = data_file("test/c.parquet", 0);
        let table = append_files(&catalog, &table, vec![b.clone(), c.clone()]).await;

        let tx = Transaction::new(&table);
        let action = tx.rewrite_files(vec![a, b], vec![data_file("test/d.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Replace
        );
        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/c.parquet".to_string(), "test/d.parquet".to_string()])
        );
        assert_deleted_tombstone(&table, "test/a.parquet").await;
        assert_deleted_tombstone(&table, "test/b.parquet").await;
    }

    /// Risk: the canonical compaction shape — many small files into one — must keep the exact live row set.
    /// Append A, B, C (3 files); rewrite delete=[A, B, C] add=[big] → live set is exactly {big} (3 → 1). A
    /// rewrite that dropped a file it should keep, or kept a compacted-away file, is data corruption.
    #[tokio::test]
    async fn test_rewrite_compaction_to_fewer_files() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let a = data_file("test/a.parquet", 0);
        let b = data_file("test/b.parquet", 0);
        let c = data_file("test/c.parquet", 0);
        let table = append_files(&catalog, &table, vec![a.clone(), b.clone(), c.clone()]).await;

        let tx = Transaction::new(&table);
        let action = tx.rewrite_files(vec![a, b, c], vec![data_file("test/big.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/big.parquet".to_string()]),
            "compaction must replace the 3 small files with the single big file"
        );
        assert_deleted_tombstone(&table, "test/a.parquet").await;
        assert_deleted_tombstone(&table, "test/b.parquet").await;
        assert_deleted_tombstone(&table, "test/c.parquet").await;
    }

    /// Risk: silently committing a partial rewrite. Deleting a file that is NOT in the current snapshot must
    /// error (Java `failMissingDeletePaths`) and NOT add the added file. A silent drop of the unmatched
    /// delete commits a rewrite that lost the intended removal but kept the add — a corrupt live set.
    #[tokio::test]
    async fn test_rewrite_delete_absent_file_errors() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;

        let tx = Transaction::new(&table);
        let action = tx.rewrite_files(vec![data_file("test/does-not-exist.parquet", 0)], vec![
            data_file("test/b.parquet", 0),
        ]);
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

        // The table is unchanged — the failed rewrite did not add b.parquet.
        let reloaded = catalog.load_table(table.identifier()).await.unwrap();
        assert_eq!(
            live_file_paths(&reloaded).await,
            HashSet::from(["test/a.parquet".to_string()])
        );
    }

    /// Risk: an empty rewrite producing a no-op Replace snapshot. Java `validateReplacedAndAddedFiles`
    /// requires the files-to-delete set to be non-empty ("Files to delete cannot be empty"). A rewrite with
    /// nothing to delete (and nothing to add) must be rejected — it is not a valid compaction.
    #[tokio::test]
    async fn test_rewrite_empty_delete_set_rejected() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;

        let tx = Transaction::new(&table);
        let action = tx.rewrite_files(vec![], vec![]);
        let tx = action.apply(tx).unwrap();
        let error = tx
            .commit(&catalog)
            .await
            .expect_err("an empty-delete rewrite must be rejected");
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert!(
            error.message().contains("Files to delete cannot be empty"),
            "unexpected error message: {}",
            error.message()
        );
    }

    /// Risk: an add-only "rewrite" (add files, delete nothing) silently behaving like an append and
    /// corrupting the live set (a rewrite must REPLACE, not add). Java rejects it with "Files to delete
    /// cannot be empty" (precondition (1) fires first). Pins that an add-only rewrite is rejected.
    #[tokio::test]
    async fn test_rewrite_add_without_delete_rejected() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![data_file("test/a.parquet", 0)]).await;

        let tx = Transaction::new(&table);
        let action = tx.rewrite_files(vec![], vec![data_file("test/b.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let error = tx
            .commit(&catalog)
            .await
            .expect_err("an add-only rewrite must be rejected");
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert!(
            error.message().contains("Files to delete cannot be empty"),
            "unexpected error message: {}",
            error.message()
        );

        // The table is unchanged — the rejected rewrite did not add b.parquet.
        let reloaded = catalog.load_table(table.identifier()).await.unwrap();
        assert_eq!(
            live_file_paths(&reloaded).await,
            HashSet::from(["test/a.parquet".to_string()])
        );
    }

    /// Risk: a delete-only rewrite (delete files, add nothing) being wrongly rejected. Java
    /// `validateReplacedAndAddedFiles` requires only the DELETE set to be non-empty, so a rewrite that drops
    /// files and adds none is LEGAL (e.g. a compaction that discarded fully-deleted data). Pins live {B}
    /// after rewriting away A, recorded as Replace.
    #[tokio::test]
    async fn test_rewrite_delete_only_is_allowed() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let a = data_file("test/a.parquet", 0);
        let b = data_file("test/b.parquet", 0);
        let table = append_files(&catalog, &table, vec![a.clone(), b]).await;

        let tx = Transaction::new(&table);
        let action = tx.rewrite_files(vec![a], vec![]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Replace,
            "a delete-only rewrite still records Replace"
        );
        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/b.parquet".to_string()])
        );
        assert_deleted_tombstone(&table, "test/a.parquet").await;
    }

    /// Risk: the rewrite summary under-reporting the change. Java `MergingSnapshotProducer.apply` merges the
    /// added-files summary AND the removed-files summary, so a rewrite reports BOTH `added-*` and
    /// `deleted-*`. Append A, B; rewrite delete=[A, B] add=[D] → summary: added 1 file/1 record, deleted
    /// 2 files/2 records.
    #[tokio::test]
    async fn test_rewrite_summary_reflects_added_and_deleted_counts() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let a = data_file("test/a.parquet", 0);
        let b = data_file("test/b.parquet", 0);
        let table = append_files(&catalog, &table, vec![a.clone(), b.clone()]).await;

        let tx = Transaction::new(&table);
        let action = tx.rewrite_files(vec![a, b], vec![data_file("test/d.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(
            summary_prop(&table, "added-data-files").as_deref(),
            Some("1"),
            "summary must report one added data file"
        );
        assert_eq!(
            summary_prop(&table, "added-records").as_deref(),
            Some("1"),
            "summary must report one added record"
        );
        assert_eq!(
            summary_prop(&table, "deleted-data-files").as_deref(),
            Some("2"),
            "summary must report two deleted data files"
        );
        assert_eq!(
            summary_prop(&table, "deleted-records").as_deref(),
            Some("2"),
            "summary must report two deleted records"
        );
    }

    /// Risk: the #1 corruption class — re-stamping a SURVIVING entry with the rewrite snapshot's id/seq
    /// instead of preserving its original (breaks merge-on-read delete application + incremental scans).
    /// Append A (snapshot S1); append B + C in one commit (snapshot S2); rewrite delete=[B] add=[D]. The
    /// surviving C must keep S2 + its original seqs (NOT S3), the carried-forward A keeps S1, the added D
    /// gets S3, and B's Deleted tombstone gets S3 but keeps B's original seqs.
    #[tokio::test]
    async fn test_rewrite_preserves_surviving_entry_provenance_across_snapshots() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Append A in its OWN commit (snapshot S1).
        let a = data_file("test/a.parquet", 0);
        let table = append_files(&catalog, &table, vec![a.clone()]).await;
        let s1 = table.metadata().current_snapshot().unwrap().snapshot_id();

        // Append B and C in ONE commit (snapshot S2; one manifest with both).
        let b = data_file("test/b.parquet", 0);
        let c = data_file("test/c.parquet", 0);
        let table = append_files(&catalog, &table, vec![b.clone(), c]).await;
        let s2 = table.metadata().current_snapshot().unwrap().snapshot_id();
        assert_ne!(s1, s2);

        let (a_snap, a_seq, a_fseq) = entry_provenance(&table, "test/a.parquet").await;
        let (b_snap, b_seq, b_fseq) = entry_provenance(&table, "test/b.parquet").await;
        assert_eq!(a_snap, Some(s1), "A added by S1");
        assert_eq!(b_snap, Some(s2), "B added by S2");
        assert_ne!(a_seq, b_seq, "A and B must have different data seq numbers");

        // Rewrite: delete B + add D → rewrites S2's manifest (C survives) and adds a new manifest (D).
        let tx = Transaction::new(&table);
        let action = tx.rewrite_files(vec![b], vec![data_file("test/d.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let s3 = table.metadata().current_snapshot().unwrap().snapshot_id();
        assert_ne!(s3, s2);

        // C survived: rewritten as Existing, MUST keep S2's snapshot id + seq numbers (NOT S3).
        let (c_snap, c_seq, c_fseq) = entry_provenance(&table, "test/c.parquet").await;
        assert_eq!(
            c_snap,
            Some(s2),
            "surviving C must keep its ORIGINAL snapshot id S2, not the rewrite snapshot S3"
        );
        assert_eq!(
            c_seq, b_seq,
            "surviving C must keep its ORIGINAL data seq, not the rewrite seq"
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

        // The added file D gets the NEW rewrite snapshot's provenance (S3 + a new seq).
        let (d_snap, d_seq, _d_fseq) = entry_provenance(&table, "test/d.parquet").await;
        assert_eq!(d_snap, Some(s3), "added D gets the new rewrite snapshot id");
        assert_ne!(
            d_seq, b_seq,
            "added D gets the new (higher) data seq, not the rewritten file's seq"
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

    /// Risk: a rewrite using the `delete_file`/`add_file` incremental builders behaving differently from
    /// the `rewrite_files` primary entry. Pins that the builder methods produce the same {C, D} live set.
    #[tokio::test]
    async fn test_rewrite_via_incremental_builders() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let a = data_file("test/a.parquet", 0);
        let b = data_file("test/b.parquet", 0);
        let c = data_file("test/c.parquet", 0);
        let table = append_files(&catalog, &table, vec![a.clone(), b.clone(), c]).await;

        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files(vec![], vec![])
            .delete_file(a)
            .delete_file(b)
            .add_file(data_file("test/d.parquet", 0));
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Replace
        );
        assert_eq!(
            live_file_paths(&table).await,
            HashSet::from(["test/c.parquet".to_string(), "test/d.parquet".to_string()])
        );
    }

    // ============================================================================================
    // dataSequenceNumber preservation + guard lift + validateNoNewDeletes (Increment 2)
    //
    // These tests use REAL parquet data + a REAL equality/position-delete file written by the
    // production writers + a REAL scan, mirroring the row_delta crown-jewel fixture, so the
    // resurrection physics is proven end-to-end (not just at the manifest-metadata level).
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

    /// Write a REAL equality-delete parquet file (via the production `EqualityDeleteFileWriter`) into the
    /// table location, deleting rows whose `y` (field id 2) equals one of `delete_ys`, in partition
    /// `x = part_value`. The equality_ids are `[2]` (the `y` column).
    async fn write_equality_delete_file(
        table: &Table,
        part_value: i64,
        delete_ys: &[i64],
    ) -> DataFile {
        use crate::arrow::arrow_schema_to_schema;

        let schema = table.metadata().current_schema().clone();
        // equality_ids = [2] ⇒ the `y` column is the equality key.
        let config = EqualityDeleteWriterConfig::new(vec![2], schema.clone()).unwrap();
        // The equality-delete file's parquet schema is the PROJECTED (equality_ids-only) schema; the writer
        // projects the full-schema input batch down to it (mirrors the equality_delete_writer unit test).
        let delete_schema =
            Arc::new(arrow_schema_to_schema(config.projected_arrow_schema_ref()).unwrap());

        let location_gen = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
        let file_name_gen = DefaultFileNameGenerator::new(
            "eq-del".to_string(),
            Some(uuid::Uuid::now_v7().to_string()),
            DataFileFormat::Parquet,
        );
        let parquet_builder = ParquetWriterBuilder::new(
            parquet::file::properties::WriterProperties::builder().build(),
            delete_schema,
        );
        let rolling = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_builder,
            table.file_io().clone(),
            location_gen,
            file_name_gen,
        );

        let partition_key = crate::spec::PartitionKey::new(
            table.metadata().default_partition_spec().as_ref().clone(),
            schema.clone(),
            Struct::from_iter([Some(Literal::long(part_value))]),
        );
        let mut writer = EqualityDeleteFileWriterBuilder::new(rolling, config)
            .build(Some(partition_key))
            .await
            .unwrap();

        // The equality-delete file carries one row per deleted `y` value (the full table schema; only the
        // equality_ids column is load-bearing for the match). x and z are filled with the partition value /
        // a dummy.
        use crate::arrow::schema_to_arrow_schema;
        let arrow_schema = Arc::new(schema_to_arrow_schema(&schema).unwrap());
        let xs: Vec<i64> = delete_ys.iter().map(|_| part_value).collect();
        let ys: Vec<i64> = delete_ys.to_vec();
        let zs: Vec<i64> = delete_ys.iter().map(|_| 0).collect();
        let batch = RecordBatch::try_new(arrow_schema, vec![
            Arc::new(Int64Array::from(xs)) as ArrayRef,
            Arc::new(Int64Array::from(ys)) as ArrayRef,
            Arc::new(Int64Array::from(zs)) as ArrayRef,
        ])
        .unwrap();
        writer.write(batch).await.unwrap();
        writer.close().await.unwrap().into_iter().next().unwrap()
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

    /// Read the EXPLICIT (pre-inheritance) data sequence number stored on disk for `path` in the table's
    /// current snapshot, reading the raw avro manifest bytes (`Manifest::try_from_avro_bytes` does NOT run
    /// inheritance). Returns `None` if the entry was written with no explicit seq (would re-inherit).
    async fn on_disk_data_seq(table: &Table, path: &str) -> Option<i64> {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        for manifest_file in manifest_list.entries() {
            let bytes = table
                .file_io()
                .new_input(&manifest_file.manifest_path)
                .unwrap()
                .read()
                .await
                .unwrap();
            let (_, raw_entries) = Manifest::try_from_avro_bytes(&bytes).unwrap();
            for entry in raw_entries {
                if entry.is_alive() && entry.file_path() == path {
                    return entry.sequence_number();
                }
            }
        }
        panic!("no live entry for {path}");
    }

    /// Count the DELETE-content manifests in the table's current snapshot manifest list (structural
    /// signal, independent of the read path). A rewrite must carry outstanding delete manifests forward,
    /// so this count must NOT drop to 0 across the rewrite.
    async fn count_delete_manifests(table: &Table) -> usize {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        manifest_list
            .entries()
            .iter()
            .filter(|m| m.content == crate::spec::ManifestContentType::Deletes)
            .count()
    }

    /// STRUCTURAL CARRY-FORWARD PIN (risk: the rewrite silently DROPS the outstanding delete manifest).
    /// Distinct from the crown jewel's read-side resurrection check: this asserts at the MANIFEST-LIST
    /// level that the DELETE manifest written by the prior `row_delta` is still referenced by the
    /// post-rewrite snapshot. It fails UNIQUELY under a mutation that filters `existing_manifest` down to
    /// DATA manifests only (the old data-only `current_data_manifests` behavior, which dropped delete
    /// manifests) and is INSENSITIVE to the seq-strip mutation — so it disambiguates the carry-forward fix
    /// from the seq-preservation fix.
    ///
    /// MUTATION (run manually, then restore): in `RewriteFilesOperation::existing_manifest`, filter the
    /// `current_manifests()` result to `content == ManifestContentType::Data` only ⇒ this test FAILS
    /// (delete manifest count drops to 0).
    #[tokio::test]
    async fn test_rewrite_carries_delete_manifest_forward_structurally() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let x = write_data_file(&table, "x.parquet", 0, &[(0, 10, 100), (0, 20, 200)]).await;
        let table = append_files(&catalog, &table, vec![x.clone()]).await;
        let x_seq = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number();

        // RowDelta an equality delete ⇒ the table now has exactly one DELETE manifest.
        let eq_delete = write_equality_delete_file(&table, 0, &[20]).await;
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![eq_delete]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        assert_eq!(
            count_delete_manifests(&table).await,
            1,
            "the row_delta must leave one delete manifest in the snapshot"
        );

        // Rewrite X → X' (seq preserved). The delete manifest must survive structurally.
        let x_prime =
            write_data_file(&table, "x-prime.parquet", 0, &[(0, 10, 100), (0, 20, 200)]).await;
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files(vec![x], vec![x_prime])
            .data_sequence_number(x_seq);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(
            count_delete_manifests(&table).await,
            1,
            "the rewrite must carry the outstanding delete manifest forward (not drop it)"
        );
    }

    /// THE CROWN JEWEL (risk: equality-delete row RESURRECTION = silent data corruption). An equality
    /// delete at seq 2 removes y=20 from data file X (seq 1). A rewrite of X→X' that preserves the data
    /// sequence number (`data_sequence_number(1)`) MUST keep the delete applying — the scan still drops
    /// y=20. The guard that previously refused this rewrite is GONE; preservation is what makes it safe.
    ///
    /// Proves: (a) the rewrite COMMITS on a table with outstanding deletes (guard lifted); (b) the scan
    /// after the rewrite is STILL {10, 30} (the delete still applies to X'); (c) X' carries the EXPLICIT
    /// data seq 1 on disk (raw avro, pre-inheritance — never null, never the fresh higher seq).
    ///
    /// MUTATION (run manually, then restore): force `new_data_files_data_sequence_number` to `None` in
    /// `write_added_manifest` ⇒ X' re-inherits the fresh seq ⇒ this test FAILS with y=20 resurrected.
    #[tokio::test]
    async fn test_rewrite_with_preserved_seq_keeps_equality_delete_applying_no_resurrection() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // 1. Append X (seq 1): rows y = [10, 20, 30] in partition x=0.
        let x = write_data_file(&table, "x.parquet", 0, &[
            (0, 10, 100),
            (0, 20, 200),
            (0, 30, 300),
        ])
        .await;
        let table = append_files(&catalog, &table, vec![x.clone()]).await;
        let x_seq = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number();

        // 2. RowDelta an EQUALITY delete (equality_ids=[y]) removing y=20 (seq 2).
        let eq_delete = write_equality_delete_file(&table, 0, &[20]).await;
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![eq_delete]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let delete_seq = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number();
        assert!(x_seq < delete_seq, "delete must be at a higher seq than X");

        // Sanity: before the rewrite the scan drops y=20.
        assert_eq!(
            scan_y_values(&table).await,
            HashSet::from([10, 30]),
            "the equality delete drops y=20 from X"
        );

        // 3. Rewrite X → X' (same rows), PRESERVING the data seq (x_seq). The guard is gone, so this commits.
        let x_prime = write_data_file(&table, "x-prime.parquet", 0, &[
            (0, 10, 100),
            (0, 20, 200),
            (0, 30, 300),
        ])
        .await;
        let x_prime_path = x_prime.file_path().to_string();
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files(vec![x], vec![x_prime])
            .data_sequence_number(x_seq);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Replace
        );

        // 4. The scan is STILL {10, 30} — the equality delete (seq 2) still applies to X' (seq 1 preserved).
        assert_eq!(
            scan_y_values(&table).await,
            HashSet::from([10, 30]),
            "with the seq preserved, the equality delete still drops y=20 — no resurrection"
        );

        // 5. X' carries the EXPLICIT data seq 1 on disk (raw avro, pre-inheritance) — not null, not a fresh
        //    higher seq. This is the on-disk pin the seq-strip mutation breaks.
        assert_eq!(
            on_disk_data_seq(&table, &x_prime_path).await,
            Some(x_seq),
            "X' must store the preserved data seq EXPLICITLY on disk (never null ⇒ never re-inherits)"
        );
    }

    /// Risk: pretending the no-preservation path is "safe" when it is the Java-identical HAZARD. WITHOUT
    /// `data_sequence_number`, the rewrite still COMMITS (the guard is lifted), but X' takes a fresh, higher
    /// seq, so the old equality delete (seq 2) no longer applies (`x'_seq <= 2` is false) and y=20
    /// RESURRECTS — the scan returns {10, 20, 30}. This pins the LIFTED guard AND documents that fresh-seq
    /// rewrite lets old equality deletes expire (Java has no guard; it is the caller's responsibility).
    #[tokio::test]
    async fn test_rewrite_without_preserved_seq_lets_old_equality_deletes_expire_java_parity() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let x = write_data_file(&table, "x.parquet", 0, &[
            (0, 10, 100),
            (0, 20, 200),
            (0, 30, 300),
        ])
        .await;
        let table = append_files(&catalog, &table, vec![x.clone()]).await;

        let eq_delete = write_equality_delete_file(&table, 0, &[20]).await;
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![eq_delete]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        assert_eq!(
            scan_y_values(&table).await,
            HashSet::from([10, 30]),
            "before the rewrite the delete drops y=20"
        );

        // Rewrite X → X' with NO seq preservation: the rewrite commits (guard lifted) but X' gets a fresh
        // higher seq, so the equality delete stops applying.
        let x_prime = write_data_file(&table, "x-prime.parquet", 0, &[
            (0, 10, 100),
            (0, 20, 200),
            (0, 30, 300),
        ])
        .await;
        let tx = Transaction::new(&table);
        let action = tx.rewrite_files(vec![x], vec![x_prime]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(
            scan_y_values(&table).await,
            HashSet::from([10, 20, 30]),
            "without seq preservation the fresh-seq rewrite resurrects y=20 (Java-identical hazard)"
        );
    }

    /// Commit a CONCURRENT row_delta that adds the given delete files, through the catalog, returning the
    /// updated table. Used to land a concurrent delete AFTER a rewrite transaction is built (between
    /// tx-build and tx-commit) so the rewrite's `validate` sees it on the refreshed base.
    async fn commit_concurrent_deletes(
        catalog: &impl Catalog,
        table: &Table,
        deletes: Vec<DataFile>,
    ) -> Table {
        let tx = Transaction::new(table);
        let action = tx.row_delta().add_deletes(deletes);
        let tx = action.apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    /// Risk (ignore_equality_deletes WITH seq): a concurrent EQUALITY delete landing after the rewrite is
    /// built must NOT block a seq-preserving rewrite — the delete still applies to X' (seq preserved), so
    /// there is no conflict (Java L475-479 ignores equality deletes when `dataSequenceNumber` is set). Pins
    /// that the rewrite COMMITS.
    #[tokio::test]
    async fn test_rewrite_with_preserved_seq_ignores_concurrent_equality_delete() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let x = write_data_file(&table, "x.parquet", 0, &[(0, 10, 100), (0, 20, 200)]).await;
        let table = append_files(&catalog, &table, vec![x.clone()]).await;
        let x_seq = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number();

        // Build the rewrite transaction NOW (captures the starting snapshot).
        let x_prime =
            write_data_file(&table, "x-prime.parquet", 0, &[(0, 10, 100), (0, 20, 200)]).await;
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files(vec![x], vec![x_prime])
            .data_sequence_number(x_seq);
        let tx = action.apply(tx).unwrap();

        // A concurrent EQUALITY delete lands AFTER the tx was built.
        let eq_delete = write_equality_delete_file(&table, 0, &[20]).await;
        let _concurrent = commit_concurrent_deletes(&catalog, &table, vec![eq_delete]).await;

        // The seq-preserving rewrite COMMITS — the equality delete is ignored (it still applies to X').
        let committed = tx.commit(&catalog).await;
        assert!(
            committed.is_ok(),
            "a seq-preserving rewrite must ignore a concurrent equality delete (Java L475-479): {:?}",
            committed.err()
        );
    }

    /// Risk (ignore_equality_deletes WITHOUT seq): a concurrent EQUALITY delete landing after the rewrite
    /// is built MUST block a non-preserving rewrite — X' takes a fresh seq, so the delete would stop
    /// applying and resurrect rows (Java rejects: ANY applicable delete is a conflict when seq is not
    /// preserved). Pins the exact message + non-retryable error.
    #[tokio::test]
    async fn test_rewrite_without_preserved_seq_rejects_concurrent_equality_delete() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let x = write_data_file(&table, "x.parquet", 0, &[(0, 10, 100), (0, 20, 200)]).await;
        let x_path = x.file_path().to_string();
        let table = append_files(&catalog, &table, vec![x.clone()]).await;

        let x_prime =
            write_data_file(&table, "x-prime.parquet", 0, &[(0, 10, 100), (0, 20, 200)]).await;
        let tx = Transaction::new(&table);
        // NO data_sequence_number ⇒ ignore_equality_deletes = false ⇒ any applicable delete is a conflict.
        let action = tx.rewrite_files(vec![x], vec![x_prime]);
        let tx = action.apply(tx).unwrap();

        let eq_delete = write_equality_delete_file(&table, 0, &[20]).await;
        let _concurrent = commit_concurrent_deletes(&catalog, &table, vec![eq_delete]).await;

        let error = tx
            .commit(&catalog)
            .await
            .expect_err("a non-preserving rewrite must reject a concurrent equality delete");
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert!(!error.retryable(), "the conflict must be non-retryable");
        assert_eq!(
            error.message(),
            format!("Cannot commit, found new delete for replaced data file: {x_path}"),
            "exact Java message (any applicable delete is a conflict when seq is not preserved)"
        );
    }

    /// Risk (position deletes are ALWAYS fatal): a concurrent POSITION delete targeting a replaced data
    /// file blocks the rewrite EVEN WITH seq preservation — a position delete is path-scoped, so its
    /// target dies with the replaced file (Java L538-543: ignoring equality deletes never ignores position
    /// deletes). Pins the exact position-delete message.
    #[tokio::test]
    async fn test_rewrite_with_preserved_seq_rejects_concurrent_position_delete() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let x = write_data_file(&table, "x.parquet", 0, &[
            (0, 10, 100),
            (0, 20, 200),
            (0, 30, 300),
        ])
        .await;
        let x_path = x.file_path().to_string();
        let table = append_files(&catalog, &table, vec![x.clone()]).await;
        let x_seq = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number();

        let x_prime =
            write_data_file(&table, "x-prime.parquet", 0, &[(0, 10, 100), (0, 30, 300)]).await;
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files(vec![x], vec![x_prime])
            .data_sequence_number(x_seq);
        let tx = action.apply(tx).unwrap();

        // A concurrent POSITION delete targeting X lands after the tx was built.
        let pos_delete = write_position_delete_file(&table, 0, &[(x_path.clone(), 1)]).await;
        let _concurrent = commit_concurrent_deletes(&catalog, &table, vec![pos_delete]).await;

        let error = tx.commit(&catalog).await.expect_err(
            "a concurrent position delete must block the rewrite even with seq preservation",
        );
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert!(!error.retryable(), "the conflict must be non-retryable");
        assert_eq!(
            error.message(),
            format!("Cannot commit, found new position delete for replaced data file: {x_path}"),
            "exact Java message (a new position delete is always fatal)"
        );
    }

    /// THE NO-OVERRIDE TX-CAPTURED-START PIN (docs/testing.md MANDATORY). With NO `validate_from_snapshot`,
    /// a concurrent position delete landing between tx-build and tx-commit must be rejected purely on the
    /// transaction-captured starting snapshot. If the validation read the REFRESHED head instead, the
    /// concurrent set would be empty and the conflict would be missed.
    ///
    /// MUTATION (run manually, then restore): in `RewriteFilesAction::validate`, change `effective_start`
    /// to `Some(current.metadata().current_snapshot_id().unwrap())` (the refreshed head) ⇒ this test FAILS
    /// (the commit wrongly succeeds).
    #[tokio::test]
    async fn test_rewrite_conflict_uses_tx_captured_start_without_override() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let x = write_data_file(&table, "x.parquet", 0, &[(0, 10, 100), (0, 20, 200)]).await;
        let x_path = x.file_path().to_string();
        let table = append_files(&catalog, &table, vec![x.clone()]).await;
        let x_seq = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number();

        // Build the rewrite tx NOW — this captures the starting snapshot. NO validate_from_snapshot.
        let x_prime = write_data_file(&table, "x-prime.parquet", 0, &[(0, 10, 100)]).await;
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files(vec![x], vec![x_prime])
            .data_sequence_number(x_seq);
        let tx = action.apply(tx).unwrap();

        // Concurrent position delete lands AFTER tx-build, BEFORE tx-commit.
        let pos_delete = write_position_delete_file(&table, 0, &[(x_path.clone(), 1)]).await;
        let _concurrent = commit_concurrent_deletes(&catalog, &table, vec![pos_delete]).await;

        let error = tx
            .commit(&catalog)
            .await
            .expect_err("the conflict must be caught via the tx-captured start (no override)");
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert!(
            error
                .message()
                .contains("found new position delete for replaced data file"),
            "unexpected message: {}",
            error.message()
        );
    }

    /// Risk (disjoint negative control / over-firing guard): a concurrent position delete on a DIFFERENT
    /// file Y in a DIFFERENT partition must NOT block a rewrite of X. The walk must not treat an unrelated
    /// delete as a conflict. Pins that the rewrite COMMITS.
    #[tokio::test]
    async fn test_rewrite_commits_when_concurrent_delete_targets_disjoint_file() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        // X in partition 0, Y in partition 1 (different partition).
        let x = write_data_file(&table, "x.parquet", 0, &[(0, 10, 100), (0, 20, 200)]).await;
        let y = write_data_file(&table, "y.parquet", 1, &[(1, 60, 600), (1, 70, 700)]).await;
        let y_path = y.file_path().to_string();
        let table = append_files(&catalog, &table, vec![x.clone(), y]).await;
        let x_seq = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number();

        let x_prime =
            write_data_file(&table, "x-prime.parquet", 0, &[(0, 10, 100), (0, 20, 200)]).await;
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files(vec![x], vec![x_prime])
            .data_sequence_number(x_seq);
        let tx = action.apply(tx).unwrap();

        // Concurrent position delete targets Y (partition 1), not X (partition 0).
        let pos_delete = write_position_delete_file(&table, 1, &[(y_path.clone(), 0)]).await;
        let _concurrent = commit_concurrent_deletes(&catalog, &table, vec![pos_delete]).await;

        let committed = tx.commit(&catalog).await;
        assert!(
            committed.is_ok(),
            "a concurrent delete on a disjoint file must not block the rewrite of X: {:?}",
            committed.err()
        );
    }

    /// Risk (pre-existing deletes are NOT conflicts): a delete committed BEFORE the rewrite tx is built is
    /// part of the base, not a concurrent commit — only the window AFTER the starting snapshot is walked.
    /// A seq-preserving rewrite on a table that already carries an equality delete must COMMIT. Pins the
    /// EXCLUSIVE-of-starting-snapshot walk semantics.
    #[tokio::test]
    async fn test_rewrite_with_preserved_seq_allows_preexisting_delete() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let x = write_data_file(&table, "x.parquet", 0, &[(0, 10, 100), (0, 20, 200)]).await;
        let table = append_files(&catalog, &table, vec![x.clone()]).await;
        let x_seq = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number();

        // The delete is committed BEFORE the rewrite tx is built — it is part of the base.
        let eq_delete = write_equality_delete_file(&table, 0, &[20]).await;
        let table = commit_concurrent_deletes(&catalog, &table, vec![eq_delete]).await;

        // Build the rewrite tx AFTER the delete — the tx-captured start is the post-delete head.
        let x_prime =
            write_data_file(&table, "x-prime.parquet", 0, &[(0, 10, 100), (0, 20, 200)]).await;
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files(vec![x], vec![x_prime])
            .data_sequence_number(x_seq);
        let tx = action.apply(tx).unwrap();

        let committed = tx.commit(&catalog).await;
        assert!(
            committed.is_ok(),
            "a pre-existing delete (before the tx start) must not be treated as a conflict: {:?}",
            committed.err()
        );
    }

    /// Risk (V1 / no-parent early return, Java L526-528): the `validate` walk must not run on a table with
    /// no current snapshot (`parent == null`). A rewrite on a freshly-appended table with no concurrent
    /// commit validates cleanly — exercised here by a rewrite whose starting snapshot IS the current head
    /// (no concurrent window), which short-circuits the walk. Pins that validation does not spuriously fire.
    #[tokio::test]
    async fn test_rewrite_validate_no_concurrent_commit_is_clean() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let x = write_data_file(&table, "x.parquet", 0, &[(0, 10, 100), (0, 20, 200)]).await;
        let table = append_files(&catalog, &table, vec![x.clone()]).await;
        let x_seq = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number();

        // No concurrent commit at all — the tx-captured start equals the head, so the walk yields nothing.
        let x_prime =
            write_data_file(&table, "x-prime.parquet", 0, &[(0, 10, 100), (0, 20, 200)]).await;
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files(vec![x], vec![x_prime])
            .data_sequence_number(x_seq);
        let tx = action.apply(tx).unwrap();

        let committed = tx.commit(&catalog).await;
        assert!(
            committed.is_ok(),
            "a rewrite with no concurrent commit must validate cleanly: {:?}",
            committed.err()
        );
    }

    /// Risk (negative seq silently re-inherits): a negative `data_sequence_number` would be STRIPPED by the
    /// manifest writer back into re-inheritance (fresh higher seq ⇒ resurrection). The action must REJECT
    /// it loudly with a `DataInvalid` naming the reason, not pass it to the writer. Pins the fail-loud guard.
    #[tokio::test]
    async fn test_rewrite_negative_data_sequence_number_rejected() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let a = data_file("test/a.parquet", 0);
        let table = append_files(&catalog, &table, vec![a.clone()]).await;

        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files(vec![a], vec![data_file("test/b.parquet", 0)])
            .data_sequence_number(-1);
        let tx = action.apply(tx).unwrap();
        let error = tx
            .commit(&catalog)
            .await
            .expect_err("a negative data sequence number must be rejected");
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert!(
            error.message().contains("Invalid data sequence number")
                && error.message().contains("non-negative"),
            "unexpected error message: {}",
            error.message()
        );

        // The table is unchanged — the rejected rewrite did not add b.parquet.
        let reloaded = catalog.load_table(table.identifier()).await.unwrap();
        assert!(
            !live_file_paths(&reloaded).await.contains("test/b.parquet"),
            "the rejected rewrite must not have added b.parquet"
        );
    }
}
