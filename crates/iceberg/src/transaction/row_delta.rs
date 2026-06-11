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

//! This module contains the row-delta action — the merge-on-read write commit.
//!
//! [`RowDeltaAction`] adds data files AND adds row-level DELETE files (position / equality) in a
//! single snapshot (Java `BaseRowDelta` / `api/RowDelta.java`). The added delete files are written
//! into a DELETE manifest (`ManifestContentType::Deletes`) alongside the DATA manifest produced for
//! the added data files, both referenced from the same manifest list. The added delete entries
//! inherit the new snapshot's sequence number at read time (the same inheritance mechanism added data
//! files use), so a delete added by this snapshot applies to data written by EARLIER snapshots
//! (`data_seq <= delete_seq`) — the spec's merge-on-read sequence-number rule.
//!
//! This is the write half of merge-on-read: the produced delete files (e.g. from
//! [`crate::writer::base_writer::position_delete_writer::PositionDeleteFileWriter`]) are committed
//! here, and the read side ([`crate::arrow::delete_filter`]) applies them during a scan so the
//! deleted rows are dropped from the result.
//!
//! **Operation recorded:** dynamic, mirroring Java `BaseRowDelta.operation()` exactly — adds-data-only
//! (no delete files) → [`Operation::Append`], adds-deletes-only (no data files) → [`Operation::Delete`],
//! both → [`Operation::Overwrite`]. The snapshot summary carries the added data-file / delete-file and
//! position/equality-delete counts in every case.
//!
//! **Concurrent-commit conflict validation (OPT-IN, two independent checks):** the serializable-isolation
//! safety layer for the merge-on-read write path (Java `BaseRowDelta.validate`). Each is opt-in and a
//! failure of either rejects the commit (a non-retryable `ValidationException` in Java terms). Default
//! (neither enabled) = snapshot isolation, behavior unchanged.
//! - **`validateNoConflictingDataFiles`** — when enabled via
//!   [`RowDeltaAction::validate_no_conflicting_data_files`], the commit is rejected if any DATA file ADDED
//!   by a concurrent commit since the operation's starting snapshot COULD CONTAIN records matching the
//!   conflict-detection filter (Java `validateNewDataFiles` → `validateAddedDataFiles`). Delegates to the
//!   SHARED [`validate_no_conflicting_added_data_files`] helper — the SAME check `OverwriteFiles` uses.
//! - **`validateNoConflictingDeleteFiles`** — when enabled via
//!   [`RowDeltaAction::validate_no_conflicting_delete_files`], the commit is rejected if any DELETE file
//!   (position / equality delete) ADDED by a concurrent commit since the operation's starting snapshot
//!   COULD APPLY to records matching the conflict-detection filter (Java `validateNewDeleteFiles` →
//!   `validateNoNewDeleteFiles`, `MergingSnapshotProducer.java` L562-570). Delegates to
//!   [`validate_no_conflicting_added_delete_files`], which enumerates the concurrently-added DELETE files
//!   (DELETE-manifest walk + the V2 guard — Java `addedDeleteFiles` L601-625 is V2-only and gated to the
//!   `{OVERWRITE, DELETE}` operation set) and tests each with the SAME inclusive-metrics evaluator. A no-op
//!   on a V1 table.
//!
//!   **Over-scan (documented):** Java's `addedDeleteFiles` additionally filters its `DeleteFileIndex` by
//!   the operation's `startingSequenceNumber`; this port enumerates concurrently-added delete files by the
//!   snapshot walk + inclusive-metrics filter only (no sequence-number refinement) — a CONSERVATIVE
//!   over-scan that can only over-reject, never under-reject (the same class as the manifest-summary
//!   pre-filter deferral elsewhere in the conflict-validation sub-sequence).
//!
//! **`validateNoNewDeletesForDataFiles` on REMOVED data files (OPT-IN, the `!removedDataFiles.isEmpty()`
//! sub-branch of Java `validateNewDeleteFiles`):** the DATA files this row delta REMOVES (via
//! [`RowDeltaAction::remove_data_files`] / [`RowDeltaAction::remove_rows`], Java `BaseRowDelta.removeRows`)
//! must not have had a NEW applicable delete added concurrently. This rides the SAME
//! [`RowDeltaAction::validate_no_conflicting_delete_files`] flag (Java's `validateNewDeleteFiles`) as the
//! filter-based delete check above; when that flag is on AND this row delta removed data files, the commit
//! is rejected if a concurrent commit since the start added a position/equality delete that APPLIES to one
//! of those removed data files (you cannot drop a data file out from under a concurrent row-level delete).
//! It delegates to the SHARED [`validate_no_new_deletes_for_data_files`] helper (the same check
//! `OverwriteFiles.validateNoConflictingDeletes` uses), with `ignore_equality_deletes = false` — RowDelta's
//! non-rewrite path counts ALL applicable deletes (Java passes the full `removedDataFiles`).
//!
//! **Apply-side removal of `removed_data_files` is VALIDATION-ONLY / DEFERRED (documented):** Java's
//! `removeRows(file)` both records the file for validation (`removedDataFiles.add(file)`) AND removes it
//! from the table in apply (`delete(file)`). This port stores the removed files as VALIDATION-ONLY metadata
//! — exactly like the existing [`RowDeltaAction::referenced_data_files`] — and does NOT yet drop them from
//! the manifests in apply. Wiring apply-side removal would change the operation classification, the
//! summary, and the manifest-rewrite path (it is `OverwriteFiles`' job today and would re-route RowDelta
//! through `process_deletes`); that is its own follow-up. The serializable-isolation validation — the
//! load-bearing safety check — is faithful now.
//!
//! **`validateAddedDVs` — the V3 deletion-vector conflict check (ALWAYS-ON, self-skipping):** the LAST step
//! of Java `BaseRowDelta.validate` (`MergingSnapshotProducer.validateAddedDVs`, L825-895) is called
//! UNCONDITIONALLY — NOT behind any opt-in flag — but it SELF-SKIPS when this row delta adds no deletion
//! vectors (Java L831 `dvsByReferencedFile.isEmpty()`). A deletion vector (DV) is a delete file whose
//! `file_format() == DataFileFormat::Puffin` (Java `ContentFileUtil.isDV` = `format() == FileFormat.PUFFIN`),
//! and its `referenced_data_file` is the data file it covers (a DV MUST set it). When this row delta adds at
//! least one DV, the commit is rejected if a concurrent commit since the start ALSO added a DV for the SAME
//! referenced data file — two DVs for one data file is a write-write conflict. The concurrent walk is
//! [`added_dv_candidate_delete_files_after`], gated to Java's `VALIDATE_ADDED_DVS_OPERATIONS = {OVERWRITE,
//! DELETE, REPLACE}` (L84-85, 1.10.0-bytecode-verified) — note REPLACE: `Operation::Replace` IS
//! representable in Rust (the rewrite actions record it) and a compaction can rewrite DVs, so the DV walk is
//! strictly WIDER than the `{Overwrite, Delete}` op set the added-delete-file check uses. The
//! concurrently-added deletes are filtered to DVs (`file_format() == Puffin`) and rejected on the first whose
//! `referenced_data_file` collides with this row delta's added-DV set.
//!
//! **Format-version gating (`validateDeleteFileForVersion`):** every added delete file is gated by format
//! version at commit time against the REFRESHED base (see
//! `SnapshotProducer::validate_added_delete_files`): V1 rejects all deletes, V2 rejects Puffin DVs for
//! position deletes, V3 REQUIRES position deletes to be DVs; equality deletes are exempt at every version
//! (Java `MergingSnapshotProducer.validateDeleteFileForVersion`, 1.10.0-bytecode-verified).
//!
//! **Apply-side removal of delete files (`removeDeletes`, NOW LANDED):** Java's
//! `RowDelta.removeDeletes(DeleteFile)` (`BaseRowDelta.removeDeletes` L82-86) drops a superseded delete file
//! from the table — `delete(deletes)` → `deleteFilterManager.delete(file)`, the delete-side sibling of the
//! data-file filter manager. This port supports it via [`RowDeltaAction::remove_deletes`]: the removed delete
//! files are resolved against the current snapshot's DELETE manifests by path and tombstoned in the rewritten
//! DELETE manifest (the producer's `process_deletes` + the content-keyed filtering writer), and they flow
//! through the summary's `remove_file` (a removed DV bumps `removed-dvs`). The primary use case is merge-on
//! -read superseding: a merged super-set deletion vector replaces an older DV for the same data file, the old
//! DV is removed in the same commit, and the reader's one-live-DV-per-file invariant holds post-commit.
//!
//! **The fresh-DV door (Rust-conservative, NOT a Java check) + its `remove_deletes` escape hatch:** a row
//! delta adding a DV is rejected when the CURRENT snapshot already carries a live position-scoped delete for
//! the same referenced data file (a live DV for that file, or a legacy parquet position delete that still
//! applies — possible on a V2→V3 upgraded table) UNLESS this same commit REMOVES that existing delete. Java
//! never commits a "second" DV: `BaseDVFileWriter.loadPreviousDeletes` (L117-126) MERGES the previous deletes
//! into the new DV and the superseded delete files are removed via `rewrittenDeleteFiles` →
//! `RowDelta.removeDeletes`. With the apply-side removal landed, a merged super-set DV + the removal of the
//! old delete commits cleanly; without a matching removal the door still fires (the second DV would make the
//! data file unreadable at the scan's duplicate-DV load door, or silently supersede a parquet delete's
//! positions). See [`RowDeltaAction::validate_fresh_dvs_only`].
//!
//! **Out of scope (deferred — the NEXT increment):**
//! - Equality-delete WRITER end-to-end (the writer exists; the RowDelta-with-equality-deletes scan
//!   application may have gaps — the end-to-end test focuses on POSITION deletes).
//! - APPLY-SIDE removal for `remove_data_files` (`removeRows`) — only the VALIDATION half lands here (the
//!   `removeDeletes` half is now apply-side; removing DATA files is `OverwriteFiles`' job today).
//! - The WRITER-side previous-deletes MERGE for deletion vectors (`BaseDVFileWriter.loadPreviousDeletes`
//!   reading + unioning the old positions into the new DV) — the apply-side removal here is the commit-path
//!   half; the writer hook that auto-merges is the next increment. (The DV WRITE path itself — `DVFileWriter`
//!   → `add_deletes` → commit → scan, plus `remove_deletes` of a superseded DV — is supported now.)

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::delete_file_index::is_deletion_vector;
use crate::error::Result;
use crate::expr::visitors::inclusive_metrics_evaluator::InclusiveMetricsEvaluator;
use crate::expr::{Bind, Predicate};
use crate::spec::{
    DataContentType, DataFile, ManifestContentType, ManifestEntry, ManifestFile, Operation, Struct,
};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
    added_dv_candidate_delete_files_after, deleted_data_files_after, dv_desc,
    validate_no_conflicting_added_data_files, validate_no_conflicting_added_delete_files,
    validate_no_new_deletes_for_data_files,
};
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind};

/// A transaction action that performs a row delta: it adds data files AND adds row-level DELETE files
/// (position / equality) in a single snapshot — the merge-on-read write commit (Java `BaseRowDelta`).
///
/// Use [`crate::transaction::Transaction::row_delta`] to create one. Accumulate the data files to add
/// with [`RowDeltaAction::add_data_files`] and the delete files to add with
/// [`RowDeltaAction::add_deletes`], then apply and commit the transaction.
///
/// An add-deletes-only row delta (no data files) and an add-data-only row delta (no delete files) are
/// both allowed; a truly-empty row delta (no data, no deletes, no snapshot properties) is rejected.
pub struct RowDeltaAction {
    /// Data files (rows) to add to the table — validated like fast append (`Data` content type).
    added_data_files: Vec<DataFile>,
    /// DELETE files (position / equality) to add to the table — written into a DELETE manifest.
    added_delete_files: Vec<DataFile>,
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    /// Whether concurrent-commit DATA-file conflict validation is enabled (Java
    /// `RowDelta.validateNoConflictingDataFiles`). OFF by default = snapshot isolation (no validation,
    /// current behavior). When ON, the commit is rejected if a concurrent snapshot added a DATA file that
    /// could contain records matching the conflict filter.
    validate_no_conflicting_data_files: bool,
    /// Whether concurrent-commit DELETE-file conflict validation is enabled (Java
    /// `RowDelta.validateNoConflictingDeleteFiles`). OFF by default = snapshot isolation (no validation,
    /// current behavior). When ON, the commit is rejected if a concurrent snapshot added a DELETE file
    /// (position / equality delete) that could apply to records matching the conflict filter. Independent
    /// of [`Self::validate_no_conflicting_data_files`] — enabling one does not enable the other (Java's two
    /// `validateNoConflicting*` methods set two separate flags).
    validate_no_conflicting_delete_files: bool,
    /// The conflict-detection filter (Java `RowDelta.conflictDetectionFilter`). When `Some`, only
    /// concurrently-added files whose metrics COULD match this predicate are conflicts. When `None`, the
    /// filter defaults to `AlwaysTrue` (any concurrently-added DATA file is a conflict — the most
    /// conservative serializable check), mirroring Java `BaseRowDelta`'s default `conflictDetectionFilter`
    /// of `Expressions.alwaysTrue()`.
    conflict_detection_filter: Option<Predicate>,
    /// An explicit starting snapshot for conflict validation (Java `validateFromSnapshot`). When `None`, the
    /// validation uses the transaction's starting snapshot (the table head when the transaction was created).
    validate_from_snapshot: Option<i64>,
    /// The set of DATA file paths the added position-delete files REFERENCE (Java `BaseRowDelta`'s
    /// `referencedDataFiles`, a CALLER-PROVIDED `CharSequenceSet` populated by `validateDataFilesExist`). When
    /// NON-EMPTY this ENABLES the files-exist check (Java's `if (!referencedDataFiles.isEmpty())` guard): the
    /// commit is rejected if any of these data files was DELETED by a concurrent commit since the starting
    /// snapshot — a position delete cannot apply to a data file that no longer exists. Empty (the default) ⇒
    /// the check does not run. The set is the caller's responsibility (it is NOT derived from the added delete
    /// files) — mirroring Java, where the engine passes the position deletes' referenced data-file paths.
    referenced_data_files: HashSet<String>,
    /// Whether DELETE-op snapshots are INCLUDED in the files-exist check (Java `BaseRowDelta.validateDeletes`,
    /// set by `validateDeletedFiles()`). `false` by DEFAULT — Java passes `skipDeletes = !validateDeletes` to
    /// `validateDataFilesExist`, so with this `false` the default `skipDeletes` is `true` and the check uses
    /// the `{OVERWRITE}` op set (a concurrent merge-on-read DELETE-op snapshot does NOT trip the check). When
    /// `validate_deleted_files()` is called this becomes `true`, `skipDeletes` becomes `false`, and the check
    /// uses the `{OVERWRITE, DELETE}` op set (DELETE-op deletions are then conflicts too).
    validate_deleted_files: bool,
    /// The DATA files this row delta REMOVES (Java `BaseRowDelta.removedDataFiles`, populated by `removeRows`).
    /// When NON-EMPTY and [`Self::validate_no_conflicting_delete_files`] is enabled, the
    /// `validateNoNewDeletesForDataFiles` sub-branch of Java `validateNewDeleteFiles` runs: the commit is
    /// rejected if a concurrent commit since the start added a position/equality delete that APPLIES to one of
    /// these removed data files (you cannot drop a data file out from under a concurrent row-level delete).
    ///
    /// VALIDATION-ONLY (apply-side removal deferred — see the module docs): unlike Java's `removeRows`, which
    /// also `delete(file)`s the file from the table in apply, these are held purely as the validation set,
    /// mirroring the existing validation-only [`Self::referenced_data_files`]. The apply path does NOT yet drop
    /// them from the manifests.
    removed_data_files: Vec<DataFile>,
    /// The DELETE files (position / equality / deletion vector) this row delta REMOVES from the table — the
    /// apply-side `RowDelta.removeDeletes(DeleteFile)` (Java `BaseRowDelta.removeDeletes` L82-86 →
    /// `delete(deletes)` → `deleteFilterManager.delete(file)`). Unlike [`Self::removed_data_files`] (which is
    /// validation-only), these are ACTUALLY DROPPED in apply: they are passed to the producer via
    /// [`SnapshotProducer::with_removed_delete_files`], resolved against the current snapshot's DELETE
    /// manifests by path, and tombstoned in the rewritten DELETE manifest (`process_deletes`).
    ///
    /// The use case is merge-on-read superseding: a merged super-set deletion vector replaces an older DV for
    /// the same data file, and the old DV is removed in the SAME commit so the table never holds two live DVs
    /// for one file. Each file must be `PositionDeletes` or `EqualityDeletes` content (a `Data` file is
    /// rejected — Java `removeDeletes` takes a `DeleteFile`); repeated calls ACCUMULATE.
    removed_delete_files: Vec<DataFile>,
}

impl RowDeltaAction {
    pub(crate) fn new() -> Self {
        Self {
            added_data_files: vec![],
            added_delete_files: vec![],
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
            validate_no_conflicting_data_files: false,
            validate_no_conflicting_delete_files: false,
            conflict_detection_filter: None,
            validate_from_snapshot: None,
            referenced_data_files: HashSet::default(),
            validate_deleted_files: false,
            removed_data_files: vec![],
            removed_delete_files: vec![],
        }
    }

    /// Add data files (rows) to the table (Java `RowDelta.addRows`). Each file must be `Data` content.
    pub fn add_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(data_files);
        self
    }

    /// Add row-level DELETE files (position / equality) to the table (Java `RowDelta.addDeletes`).
    ///
    /// In Java these are `DeleteFile`s; in this Rust model both data and delete files are [`DataFile`]s
    /// distinguished by their content type. Each file passed here must be `PositionDeletes` or
    /// `EqualityDeletes` content (a `Data` file is rejected at commit).
    pub fn add_deletes(mut self, delete_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_delete_files.extend(delete_files);
        self
    }

    /// Record DATA files this row delta REMOVES, for the `validateNoNewDeletesForDataFiles` conflict check
    /// (Java `RowDelta.removeRows(DataFile)` → `removedDataFiles.add(file)`).
    ///
    /// When this set is NON-EMPTY and [`Self::validate_no_conflicting_delete_files`] is enabled, the commit
    /// is rejected if a concurrent commit since the starting snapshot added a position/equality delete that
    /// APPLIES to one of these removed data files — under serializable isolation you cannot drop a data file
    /// out from under a concurrent row-level delete. The check needs each removed file's partition + metrics,
    /// which is why the full [`DataFile`] is supplied here (a bare path would not carry it).
    ///
    /// VALIDATION-ONLY (apply-side removal deferred — see the module docs): unlike Java's `removeRows`, which
    /// also removes the file from the table in apply, this Rust port holds the supplied files purely as the
    /// validation set (mirroring the validation-only [`Self::validate_data_files_exist`]). It does NOT yet drop
    /// them from the manifests; use [`crate::transaction::Transaction::overwrite_files`]'s `delete_data_files`
    /// to actually remove data files until the apply-side path lands. Repeated calls ACCUMULATE.
    ///
    /// On its own this does NOT enable validation — call [`Self::validate_no_conflicting_delete_files`] for that.
    pub fn remove_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.removed_data_files.extend(data_files);
        self
    }

    /// Record a single DATA file this row delta REMOVES (Java `RowDelta.removeRows(DataFile)`). A convenience
    /// wrapper over [`Self::remove_data_files`]; see it for the full contract (notably that removal is
    /// VALIDATION-ONLY in this port — apply-side removal is deferred).
    pub fn remove_rows(self, data_file: DataFile) -> Self {
        self.remove_data_files([data_file])
    }

    /// Remove a single DELETE file (position / equality / deletion vector) from the table — the apply-side
    /// `RowDelta.removeDeletes(DeleteFile)` (Java `BaseRowDelta.removeDeletes` L82-86 → `delete(deletes)`).
    ///
    /// Unlike [`Self::remove_rows`] (validation-only in this port), this ACTUALLY DROPS the delete file:
    /// at commit time it is resolved against the current snapshot's DELETE manifests by path and tombstoned
    /// in the rewritten DELETE manifest, with surviving delete entries copied forward provenance-preserved.
    ///
    /// The primary use case is merge-on-read superseding: when a merged super-set deletion vector replaces an
    /// older DV for the same data file, the old DV is removed in the SAME commit so the table never holds two
    /// live DVs for one file (and the fresh-DV-only door's escape hatch — see
    /// [`Self::validate_fresh_dvs_only`] — lets the merged DV commit). The supplied file must be
    /// `PositionDeletes` or `EqualityDeletes` content (a `Data` file is rejected at commit — Java's
    /// `removeDeletes` takes a `DeleteFile`). Repeated calls ACCUMULATE.
    pub fn remove_deletes(self, delete_file: DataFile) -> Self {
        self.remove_deletes_many([delete_file])
    }

    /// Remove multiple DELETE files (position / equality / deletion vector) from the table — the plural of
    /// [`Self::remove_deletes`] (Java `RowDelta.removeDeletes` called per file). Each file must be
    /// `PositionDeletes` or `EqualityDeletes` content (a `Data` file is rejected at commit). Repeated calls
    /// ACCUMULATE.
    pub fn remove_deletes_many(mut self, delete_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.removed_delete_files.extend(delete_files);
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

    /// ENABLE concurrent-commit conflict validation (Java `RowDelta.validateNoConflictingDataFiles`): the
    /// commit is rejected with a non-retryable `ValidationException` if any DATA file ADDED by a concurrent
    /// snapshot since the starting snapshot could contain records matching the conflict-detection filter
    /// (see [`Self::conflict_detection_filter`]). This is the serializable-isolation guard against silently
    /// committing a row delta against data that was concurrently appended.
    ///
    /// Default (this method NOT called) = snapshot isolation = no validation (current behavior unchanged).
    pub fn validate_no_conflicting_data_files(mut self) -> Self {
        self.validate_no_conflicting_data_files = true;
        self
    }

    /// ENABLE concurrent-commit DELETE-file conflict validation (Java
    /// `RowDelta.validateNoConflictingDeleteFiles`): the commit is rejected with a non-retryable
    /// `ValidationException` if any DELETE file (position / equality delete) ADDED by a concurrent snapshot
    /// since the starting snapshot could apply to records matching the conflict-detection filter (see
    /// [`Self::conflict_detection_filter`]). This guards against a concurrent merge-on-read delete landing
    /// on the same rows this row delta touches.
    ///
    /// This is INDEPENDENT of [`Self::validate_no_conflicting_data_files`] — enabling one does not enable
    /// the other (Java exposes them as two separate methods setting two separate flags). When both are
    /// enabled, both checks run and EITHER failing rejects the commit.
    ///
    /// On a V1 table this is a no-op (delete files do not exist before format version 2 — Java's
    /// `addedDeleteFiles` V2 guard).
    ///
    /// Default (this method NOT called) = snapshot isolation = no validation (current behavior unchanged).
    pub fn validate_no_conflicting_delete_files(mut self) -> Self {
        self.validate_no_conflicting_delete_files = true;
        self
    }

    /// Set the conflict-detection filter (Java `RowDelta.conflictDetectionFilter(Expression)`): only a
    /// concurrently-added DATA file whose metrics COULD contain records matching this predicate is treated as
    /// a conflict. When no filter is set (the default), the conflict filter is `AlwaysTrue` — ANY
    /// concurrently-added data file conflicts (the most conservative serializable check), matching Java
    /// `BaseRowDelta`'s default `conflictDetectionFilter` of `Expressions.alwaysTrue()`.
    ///
    /// On its own this does NOT enable validation — call [`Self::validate_no_conflicting_data_files`] for that.
    pub fn conflict_detection_filter(mut self, filter: Predicate) -> Self {
        self.conflict_detection_filter = Some(filter);
        self
    }

    /// Override the snapshot from which concurrent-commit conflict validation starts (Java
    /// `RowDelta.validateFromSnapshot(long)`). By default the validation uses the transaction's starting
    /// snapshot (the table head when [`crate::transaction::Transaction::new`] was called); this lets the
    /// caller pin a specific earlier snapshot id (the snapshot it read when building the row delta).
    ///
    /// On its own this does NOT enable validation — call [`Self::validate_no_conflicting_data_files`] for that.
    pub fn validate_from_snapshot(mut self, snapshot_id: i64) -> Self {
        self.validate_from_snapshot = Some(snapshot_id);
        self
    }

    /// Provide the DATA files the added position-delete files REFERENCE, ENABLING the files-exist check (Java
    /// `RowDelta.validateDataFilesExist(Iterable<CharSequence> referencedFiles)`). At commit time the row
    /// delta is rejected (non-retryable `ValidationException`) if ANY of these data files was DELETED by a
    /// concurrent commit since the starting snapshot — a position delete cannot apply to a data file that no
    /// longer exists, so committing it would silently lose the delete.
    ///
    /// The set is CALLER-PROVIDED (the engine passes the data-file paths its position deletes reference); it
    /// is NOT derived from the added delete files. Calling this with a non-empty iterable is what enables the
    /// check (Java's `if (!referencedDataFiles.isEmpty())` guard) — an empty call (or never calling it) leaves
    /// the check off. Repeated calls ACCUMULATE into the referenced set (Java
    /// `referencedFiles.forEach(referencedDataFiles::add)`).
    ///
    /// By DEFAULT the check IGNORES concurrent merge-on-read DELETE-op snapshots (Java's `skipDeletes = true`
    /// default — `validateDeletes` is `false`); call [`Self::validate_deleted_files`] to also treat a
    /// concurrent DELETE-op removal of a referenced file as a conflict.
    pub fn validate_data_files_exist(
        mut self,
        referenced_files: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.referenced_data_files
            .extend(referenced_files.into_iter().map(Into::into));
        self
    }

    /// INCLUDE concurrent DELETE-op snapshots in the files-exist check (Java
    /// `RowDelta.validateDeletedFiles()`, which sets `validateDeletes = true`). By default the files-exist
    /// check uses the `{OVERWRITE}` op set (Java `skipDeletes = !validateDeletes = true`), so a concurrent
    /// merge-on-read DELETE-op snapshot that removed a referenced data file is NOT a conflict. After this
    /// call the check uses the `{OVERWRITE, DELETE}` op set (`skipDeletes = false`), so such a removal IS a
    /// conflict.
    ///
    /// On its own this does NOT enable the files-exist check — call [`Self::validate_data_files_exist`] with
    /// the referenced data files for that.
    pub fn validate_deleted_files(mut self) -> Self {
        self.validate_deleted_files = true;
        self
    }

    /// Build the map of DATA-file path → the added DELETION VECTOR (DV) covering it — the Rust port of
    /// Java `MergingSnapshotProducer.newDVRefs` (1.10.0; MAIN's `dvsByReferencedFile` — populated at `add`
    /// time, keyed on `file.referencedDataFile()` when `ContentFileUtil.isDV(file)`).
    ///
    /// A DV is an added delete file whose `file_format() == DataFileFormat::Puffin` (Java
    /// `ContentFileUtil.isDV` = `format() == FileFormat.PUFFIN`). Each DV's `referenced_data_file` is the data
    /// file it covers and is REQUIRED for a DV (the spec mandates it); a Puffin delete file MISSING
    /// `referenced_data_file` is malformed, so this errors (matching Java, which dereferences
    /// `file.referencedDataFile()` as a non-null map key when populating the set). Non-Puffin deletes
    /// (position / equality) are SKIPPED — they are not DVs — so for the common merge-on-read row delta
    /// (no DVs) this returns an EMPTY map, which makes the always-on `validate_added_dvs` step and the
    /// fresh-DV-only door self-skip. (The door tests shadow applicability against the referenced data
    /// file's LIVE manifest entry, not the mapped DV's own partition — see `validate_fresh_dvs_only`;
    /// the map's [`DataFile`] values are kept for the future previous-deletes merge path.)
    fn added_dvs_by_referenced_file(&self) -> Result<HashMap<String, &DataFile>> {
        let mut referenced = HashMap::new();
        for delete_file in &self.added_delete_files {
            if !is_deletion_vector(delete_file) {
                // Not a DV (Java `ContentFileUtil.isDV` is false) — a position/equality delete, not indexed
                // into the DV-reference map.
                continue;
            }
            match delete_file.referenced_data_file() {
                Some(path) => {
                    referenced.insert(path, delete_file);
                }
                None => {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Deletion vector {} is missing its referenced data file",
                            delete_file.file_path()
                        ),
                    ));
                }
            }
        }
        Ok(referenced)
    }

    /// Reject the commit if a concurrent commit since `effective_start` added a DELETION VECTOR (DV) for a
    /// data file this row delta ALSO adds a DV for — the Rust port of Java
    /// `MergingSnapshotProducer.validateAddedDVs` (`MergingSnapshotProducer.java` L825-895), the last step of
    /// `BaseRowDelta.validate`.
    ///
    /// **Always-on, self-skipping (Java L831):** unlike the opt-in checks (steps 1-3), this runs on EVERY row
    /// delta — Java calls `validateAddedDVs` unconditionally — but it SELF-SKIPS when this row delta adds no
    /// DVs (`added_dvs_by_referenced_file` empty ⇔ Java 1.10.0 `newDVRefs.isEmpty()`). The common
    /// merge-on-read row delta adds NON-Puffin position / equality deletes, so the set is empty and this is a
    /// no-op.
    ///
    /// **Concurrent walk (Java L835-841 + L84-85 `VALIDATE_ADDED_DVS_OPERATIONS = {OVERWRITE, DELETE,
    /// REPLACE}`, 1.10.0-bytecode-verified):** the concurrently-added delete files are enumerated by
    /// [`added_dv_candidate_delete_files_after`] — the DELETE-manifest walk gated to `{Overwrite, Delete,
    /// Replace}`. REPLACE is in the set because a compaction snapshot can rewrite DVs (Java
    /// `RewriteDataFiles`); `Operation::Replace` IS representable in Rust (the rewrite actions record it),
    /// so the DV walk is strictly WIDER than the `{Overwrite, Delete}` op set the added-delete-file conflict
    /// check uses.
    ///
    /// **DV filter + collision (Java L867-873):** of those concurrently-added deletes, keep only the DVs
    /// (`file_format() == Puffin`); each DV is optionally narrowed by `conflict_filter` (Java passes it into
    /// the manifest scan — a DV whose metrics cannot match cannot conflict), then its `referenced_data_file` is
    /// checked against this row delta's added-DV set. The FIRST collision returns a NON-retryable
    /// [`ErrorKind::DataInvalid`] error matching Java's exact message ("Found concurrently added DV for %s:
    /// %s" with `ContentFileUtil.dvDesc`), so the retry loop stops (Java's non-retryable
    /// `ValidationException`).
    async fn validate_added_dvs(
        &self,
        current: &Table,
        effective_start: Option<i64>,
        conflict_filter: Option<&Predicate>,
    ) -> Result<()> {
        // Java L831: skip if this operation adds no DVs (`newDVRefs.isEmpty()`).
        let added_dv_referenced = self.added_dvs_by_referenced_file()?;
        if added_dv_referenced.is_empty() {
            return Ok(());
        }

        // Java L835-841: the concurrently-added delete files (DELETE-manifest walk gated to the
        // `{Overwrite, Delete, Replace}` `VALIDATE_ADDED_DVS_OPERATIONS` op set, 1.10.0-bytecode-verified).
        let added_deletes = added_dv_candidate_delete_files_after(current, effective_start).await?;
        if added_deletes.is_empty() {
            return Ok(());
        }

        // Java passes `conflictDetectionFilter` into the manifest scan: a concurrently-added DV whose metrics
        // cannot match the filter cannot conflict. Bind it ONCE (None ⇒ no narrowing — every concurrent DV is a
        // candidate, the conservative default mirroring Java's `alwaysTrue()`).
        let bound_filter = match conflict_filter {
            Some(filter) => Some(
                filter
                    .clone()
                    .bind(current.metadata().current_schema().clone(), true)?,
            ),
            None => None,
        };

        for concurrent in &added_deletes {
            // Java L867: keep only concurrently-added DVs (`ContentFileUtil.isDV` = `format() == PUFFIN`).
            if !is_deletion_vector(concurrent) {
                continue;
            }

            // Metrics narrowing (Java's `filterRows(conflictDetectionFilter)` on the manifest scan).
            if let Some(bound_filter) = &bound_filter
                && !InclusiveMetricsEvaluator::eval(bound_filter, concurrent, true)?
            {
                continue;
            }

            // Java L867-873: a concurrent DV for a data file THIS row delta also adds a DV for is a conflict.
            // A concurrent DV missing `referenced_data_file` is malformed; it cannot collide with any entry in
            // the set, so it is skipped (it would never be a valid key in Java's DV-reference set).
            if let Some(referenced) = concurrent.referenced_data_file()
                && added_dv_referenced.contains_key(&referenced)
            {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Found concurrently added DV for {}: {}",
                        referenced,
                        dv_desc(concurrent)
                    ),
                ));
            }
        }

        Ok(())
    }

    /// THE FRESH-DV DOOR (Rust-conservative; NOT a Java check — documented divergence). Reject this row
    /// delta when the CURRENT snapshot already carries a live position-scoped delete for a data file this row
    /// delta adds a deletion vector for — UNLESS that existing delete is REMOVED in this same commit (the
    /// `remove_deletes` escape hatch — see below). The blocking cases are:
    ///
    /// - **A live DV for the same `referenced_data_file`.** Committing a second DV would leave TWO live DVs
    ///   for one data file — an invalid table per the spec, which the scan's duplicate-DV load door
    ///   ([`crate::arrow::caching_delete_file_loader`], the D1 fail-loud relocation of Java
    ///   `DeleteFileIndex.add`'s "Can't index multiple DVs") rejects at READ time. Failing the COMMIT here
    ///   converts that fail-late corruption into a fail-loud rejection.
    /// - **A live legacy parquet position delete that still APPLIES to that data file** (possible on a V2→V3
    ///   upgraded table). At read time a DV SUPERSEDES every parquet position delete for its data file (Java
    ///   `DeleteFileIndex.forDataFile`; the D1 index mirrors it), so committing the DV without merging would
    ///   silently RESURRECT the parquet delete's positions. "Applies" is the READ-path test
    ///   (`delete_file_index.rs`), evaluated against the referenced data file's LIVE manifest entry — NOT
    ///   the added DV's own metadata (the DV always carries the CURRENT default spec, so a referenced file
    ///   written under an older partition spec would never match it after a partition evolution): a
    ///   path-scoped delete applies iff it references the same path; a partition-scoped delete applies iff
    ///   its (spec id, partition) equal the DATA file entry's; both only when `delete_seq >= data_seq` (a
    ///   delete never applies to a data file added after it). A referenced file with NO live entry is being
    ///   added in THIS commit and postdates every live delete — nothing applies.
    ///
    /// **The `remove_deletes` escape hatch (the relaxation — the apply-side removal now landed):** Java
    /// never commits a "second" DV because `BaseDVFileWriter.loadPreviousDeletes` (L117-126) MERGES the
    /// file's previous deletes into the new DV and the superseded delete files are removed via
    /// `rewrittenDeleteFiles` → `RowDelta.removeDeletes`. This port now supports the apply-side delete-file
    /// removal ([`RowDeltaAction::remove_deletes`]), so a row delta MAY add a DV for a file with a live
    /// position-scoped delete AS LONG AS it removes that existing delete in the SAME commit: the existing
    /// delete's path is in `self.removed_delete_files`, so post-commit the table holds exactly ONE live DV
    /// per file (the reader's invariant). The door skips its rejection for any existing delete whose path is
    /// in the removed set; with no matching removal it still fires (the D3 fail-loud posture). What remains
    /// DEFERRED is the WRITER-side automatic merge (`loadPreviousDeletes` reading + unioning the old
    /// positions into the new DV) — the test hand-merges the super-set DV; the apply-side removal here is the
    /// commit-path half. Equality deletes are NOT superseded by a DV and do not trip the door.
    ///
    /// Runs in the action's `commit()` against the refreshed base, alongside the producer's added-delete-file
    /// validation. Self-skips when this row delta adds no DVs.
    async fn validate_fresh_dvs_only(&self, table: &Table) -> Result<()> {
        let added_dvs = self.added_dvs_by_referenced_file()?;
        if added_dvs.is_empty() {
            return Ok(());
        }

        // THE RELAXATION (the merge-and-replace escape hatch). An existing live position-scoped delete for
        // a referenced file is LEGAL to shadow IFF this same commit REMOVES it (`remove_deletes`) — the
        // Java contract: `BaseDVFileWriter` merges the previous deletes into the new DV and the engine
        // removes the superseded file via `rewrittenDeleteFiles` → `RowDelta.removeDeletes`, so post-commit
        // the table holds exactly ONE live DV per file (the reader's invariant). The door below skips its
        // rejection when the existing delete's path is in this removed set; without a removal it still
        // fires (the D3 fail-loud posture). Path equality is Java's resolution (`DeleteFileSet` keys on
        // location), so a removed file matches the live entry by path.
        let removed_delete_paths: HashSet<&str> = self
            .removed_delete_files
            .iter()
            .map(|file| file.file_path())
            .collect();

        let Some(snapshot) = table.metadata().current_snapshot() else {
            return Ok(());
        };
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), &table.metadata_ref())
            .await?;

        // Resolve each referenced data file's LIVE manifest entry — (partition spec id, partition,
        // data sequence number) — from the snapshot's DATA manifests. Parquet-delete applicability
        // below is decided against the REFERENCED DATA FILE's entry (the read-path keys:
        // `delete_file_index.rs` matches a partition-scoped delete on the data file's
        // (spec id, partition) and seq-filters with `delete_seq >= data_seq`), not against the
        // added DV's own metadata.
        let mut live_data_entry_by_path: HashMap<String, (i32, Struct, Option<i64>)> =
            HashMap::new();
        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Data {
                continue;
            }
            let manifest = manifest_file.load_manifest(table.file_io()).await?;
            for entry in manifest.entries() {
                if !entry.is_alive() {
                    continue;
                }
                let file = entry.data_file();
                if added_dvs.contains_key(file.file_path()) {
                    live_data_entry_by_path.insert(
                        file.file_path().to_string(),
                        (
                            file.partition_spec_id,
                            file.partition().clone(),
                            entry.sequence_number(),
                        ),
                    );
                }
            }
        }

        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Deletes {
                continue;
            }
            let manifest = manifest_file.load_manifest(table.file_io()).await?;
            for entry in manifest.entries() {
                if !entry.is_alive() {
                    continue;
                }
                let existing = entry.data_file();
                if existing.content_type() != DataContentType::PositionDeletes {
                    // Equality deletes coexist with DVs (a DV supersedes only position deletes).
                    continue;
                }

                // The escape hatch: a delete file REMOVED in this same commit (`remove_deletes`) is being
                // superseded deliberately — the Java merge-and-replace path — so it does NOT block the new
                // DV. Skip it here; `process_deletes` will tombstone it in the rewritten DELETE manifest.
                if removed_delete_paths.contains(existing.file_path()) {
                    continue;
                }

                if is_deletion_vector(existing) {
                    // A live DV for the same referenced data file ⇒ two DVs per file.
                    if let Some(referenced) = existing.referenced_data_file()
                        && added_dvs.contains_key(&referenced)
                    {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!(
                                "Cannot commit deletion vector for {}: the current snapshot already \
                                 carries a live deletion vector for that data file ({}). Merging \
                                 previous deletes into the new DV and removing the old delete file \
                                 (Java BaseDVFileWriter.loadPreviousDeletes + RowDelta.removeDeletes) \
                                 is deferred in this port; committing would leave two DVs for one data \
                                 file, which the scan rejects",
                                referenced,
                                dv_desc(existing)
                            ),
                        ));
                    }
                } else {
                    // A legacy parquet position delete that still APPLIES to a referenced data file would
                    // be silently superseded by the new DV at read time (resurrection). The applicability
                    // test mirrors the READ path (`delete_file_index.rs`) against the referenced file's
                    // LIVE data entry: path-scoped deletes apply iff they reference the same path;
                    // partition-scoped deletes apply iff their (spec id, partition) equal the DATA
                    // entry's; both only when delete_seq >= data_seq. A referenced file with no live
                    // entry is added in THIS commit (or dangling) — it postdates every live delete.
                    for referenced in added_dvs.keys() {
                        let Some((data_spec_id, data_partition, data_seq)) =
                            live_data_entry_by_path.get(referenced)
                        else {
                            continue;
                        };
                        let scope_matches = match existing.referenced_data_file() {
                            Some(path) => &path == referenced,
                            None => {
                                existing.partition_spec_id == *data_spec_id
                                    && existing.partition() == data_partition
                            }
                        };
                        // The read-path sequence filter: a position delete applies iff
                        // delete_seq >= data_seq. An unknown sequence (None — not produced by the
                        // post-inheritance `load_manifest` path) is treated as applying: the door
                        // errs toward rejection.
                        let applies = scope_matches
                            && match (entry.sequence_number(), *data_seq) {
                                (Some(delete_seq), Some(data_seq)) => delete_seq >= data_seq,
                                _ => true,
                            };
                        if applies {
                            return Err(Error::new(
                                ErrorKind::DataInvalid,
                                format!(
                                    "Cannot commit deletion vector for {}: live position delete file \
                                     {} still applies to that data file and would be silently \
                                     superseded by the DV at read time. Merging previous deletes into \
                                     the new DV (Java BaseDVFileWriter.loadPreviousDeletes) is deferred \
                                     in this port",
                                    referenced,
                                    existing.file_path()
                                ),
                            ));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Reject a `Data`-content file in the removed-delete set — Java `RowDelta.removeDeletes(DeleteFile)`
    /// only accepts delete files (a data file must go through `removeRows`). The content check mirrors the
    /// added-delete-file content check (`SnapshotProducer::validate_added_delete_files` rejects `Data`); the
    /// version gate does NOT apply to removals (a removed delete file already exists on a versioned table).
    fn validate_removed_delete_files(&self) -> Result<()> {
        for delete_file in &self.removed_delete_files {
            if delete_file.content_type() == DataContentType::Data {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Only position-delete or equality-delete content is allowed for removed delete files (use remove_rows to remove data files)",
                ));
            }
        }
        Ok(())
    }

    /// Reject a row delta that REMOVES a data file the added delete files REFERENCE — the Rust port of Java
    /// `BaseRowDelta.validateNoConflictingFileAndPositionDeletes` (1.10.0-bytecode-verified, called
    /// UNCONDITIONALLY from `BaseRowDelta.validate` right before `validateAddedDVs`): the intersection of
    /// `removedDataFiles` paths with `referencedDataFiles` must be empty — a delete file referencing a data
    /// file removed in the SAME commit is self-contradictory (the delete would apply to nothing, silently).
    /// Exact Java message: "Cannot delete data files %s that are referenced by new delete files" where `%s`
    /// is the Java `List` rendering `[path, ...]`.
    fn validate_no_conflicting_file_and_position_deletes(&self) -> Result<()> {
        let deleted_files_with_new_deletes: Vec<&str> = self
            .removed_data_files
            .iter()
            .map(|file| file.file_path())
            .filter(|path| self.referenced_data_files.contains(*path))
            .collect();

        if !deleted_files_with_new_deletes.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot delete data files [{}] that are referenced by new delete files",
                    deleted_files_with_new_deletes.join(", ")
                ),
            ));
        }

        Ok(())
    }
}

#[async_trait]
impl TransactionAction for RowDeltaAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        // Reject a DATA-content file in the removed-delete set BEFORE building the producer — Java's
        // `removeDeletes(DeleteFile)` takes a delete file; a `Data` file would be a `removeRows`. (The
        // producer also validates added files; this guards the removal surface at its own door.)
        self.validate_removed_delete_files()?;

        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
        )
        .with_added_delete_files(self.added_delete_files.clone())
        .with_removed_delete_files(self.removed_delete_files.clone());

        // Validate the added data files like fast append (Data content type, partition-spec match,
        // partition-value compatibility) and the added delete files (position/equality content type,
        // the FORMAT-VERSION gate — V1 rejects deletes, V2 rejects DVs, V3 requires DVs for position
        // deletes — and partition-spec match) — mirroring Java `MergingSnapshotProducer.add(DataFile)` /
        // `add(DeleteFile)` → `validateNewDeleteFile`. Runs against the REFRESHED base (`do_commit`
        // re-bases before calling `commit`), so a concurrent format upgrade re-gates the buffered files
        // (the placement Java MAIN's apply-time `validateDeleteFilesForVersion` adopts).
        snapshot_producer.validate_added_data_files()?;
        snapshot_producer.validate_added_delete_files()?;

        // The fresh-DV-only door (Rust-conservative; see its doc): a DV for a data file that already has
        // a live position-scoped delete cannot be committed UNLESS that existing delete is removed in this
        // same commit (the `removed_delete_files` escape hatch — the Java merge-and-replace contract).
        self.validate_fresh_dvs_only(table).await?;

        snapshot_producer
            .commit(
                RowDeltaOperation {
                    // Classified on the REQUESTED sets, before files resolve against the table —
                    // matching Java 1.10.0 `BaseRowDelta.operation()` (`addsDeleteFiles()` /
                    // `addsDataFiles()`; see `RowDeltaOperation::operation`).
                    adds_data_files: !self.added_data_files.is_empty(),
                    adds_delete_files: !self.added_delete_files.is_empty(),
                    removes_delete_files: !self.removed_delete_files.is_empty(),
                },
                DefaultManifestProcess,
            )
            .await
    }

    /// Serializable-isolation conflict validation (Java `BaseRowDelta.validate`,
    /// `core/BaseRowDelta.java` L131-174). Runs the opt-in checks; with neither enabled it is a no-op
    /// (snapshot isolation, current behavior unchanged).
    ///
    /// Both enabled checks share the same effective starting snapshot ([`Self::validate_from_snapshot`] if
    /// set, else the transaction-provided `starting_snapshot_id`) and the same conflict filter (the caller's
    /// [`Self::conflict_detection_filter`] when set, else `AlwaysTrue` — any concurrently-added file
    /// conflicts, mirroring Java `BaseRowDelta`'s default `conflictDetectionFilter` of
    /// `Expressions.alwaysTrue()`). A failure of EITHER rejects the commit with a NON-retryable `DataInvalid`
    /// (Java's non-retryable `ValidationException`), so the commit retry loop stops.
    ///
    /// 1. **`validateNoConflictingDataFiles`** (Java L155-157 → `validateAddedDataFiles`, when
    ///    [`Self::validate_no_conflicting_data_files`] is enabled): enumerate the DATA files added by
    ///    concurrent commits and reject if ANY could CONTAIN records matching the filter. Delegates to the
    ///    SHARED [`validate_no_conflicting_added_data_files`] helper — the SAME check `OverwriteFiles` uses.
    /// 2. **`validateNewDeleteFiles`** (Java L159-168, when [`Self::validate_no_conflicting_delete_files`] is
    ///    enabled): ONE flag gates TWO sub-checks (mirroring Java's single `if (validateNewDeleteFiles)`):
    ///    - **2a. `validateNoNewDeletesForDataFiles`** (Java L161-164, the `!removedDataFiles.isEmpty()`
    ///      sub-branch): when this row delta REMOVED data files (via [`Self::remove_data_files`] /
    ///      [`Self::remove_rows`]), reject if a concurrent commit added a delete that APPLIES to one of those
    ///      removed data files. Delegates to the SHARED [`validate_no_new_deletes_for_data_files`] helper (the
    ///      one `OverwriteFiles.validateNoConflictingDeletes` uses) with `ignore_equality_deletes = false`
    ///      (Java's non-rewrite path counts ALL applicable deletes). A no-op on a V1 table or when no data
    ///      files were removed.
    ///    - **2b. `validateNoNewDeleteFiles`** (Java L167): enumerate the DELETE files (position / equality
    ///      deletes) added by concurrent commits and reject if ANY could APPLY to records matching the filter.
    ///      Delegates to [`validate_no_conflicting_added_delete_files`] (DELETE-manifest walk + the V2 guard +
    ///      the SAME per-file inclusive-metrics test). A no-op on a V1 table.
    /// 3. **`validateDataFilesExist`** (Java L141-149, when [`Self::referenced_data_files`] is NON-EMPTY,
    ///    i.e. [`Self::validate_data_files_exist`] was called): enumerate the DATA files DELETED by concurrent
    ///    commits since the start (the shared [`deleted_data_files_after`] helper) and reject if ANY of them is
    ///    a data file the added position-deletes REFERENCE — a position delete cannot apply to a file that was
    ///    concurrently removed. Java passes `skipDeletes = !validateDeletes`, so by DEFAULT
    ///    ([`Self::validate_deleted_files`] NOT called ⇒ `validate_deleted_files == false`) `skip_deletes` is
    ///    `true` and the walk uses the `{OVERWRITE}` op set (a concurrent DELETE-op snapshot is EXCLUDED);
    ///    after `validate_deleted_files()` the walk uses `{OVERWRITE, DELETE}` and a DELETE-op removal of a
    ///    referenced file is also a conflict. INDEPENDENT of the conflict filter (Java passes the filter to
    ///    `validateDataFilesExist`, but the Rust [`deleted_data_files_after`] walk does not yet thread a filter
    ///    — a conservative over-scan that can only over-reject; the referenced-set intersection is the
    ///    load-bearing gate). The rejection is "Cannot commit, missing data files: {path}".
    ///
    /// The top-level checks are INDEPENDENT (enabling one does not run the others), mirroring Java's separate
    /// flags / the `referencedDataFiles.isEmpty()` guard. (Sub-checks 2a and 2b deliberately share the ONE
    /// `validate_no_conflicting_delete_files` flag — Java's single `validateNewDeleteFiles` gate.)
    ///
    /// 4. **`validateNoConflictingFileAndPositionDeletes`** (Java `BaseRowDelta.validate`, called
    ///    UNCONDITIONALLY right before `validateAddedDVs` — 1.10.0-bytecode-verified): reject when a data
    ///    file this row delta REMOVES is also REFERENCED by its added delete files (the two validation sets
    ///    intersect) — a self-contradictory commit. See
    ///    [`Self::validate_no_conflicting_file_and_position_deletes`].
    ///
    /// 5. **`validateAddedDVs`** (Java L172 → `MergingSnapshotProducer.validateAddedDVs` L825-895, called
    ///    UNCONDITIONALLY — NOT gated by any flag): reject if a concurrent commit since the start added a
    ///    deletion vector (DV) for a data file THIS row delta also adds a DV for (two DVs per data file is a
    ///    write-write conflict). SELF-SKIPS when this row delta adds no DVs (Java L831
    ///    `newDVRefs.isEmpty()`), so it is a no-op for every non-DV row delta. See
    ///    [`Self::validate_added_dvs`].
    ///
    /// **Over-scan vs Java (documented):** the delete-file check omits Java's `DeleteFileIndex`
    /// `startingSequenceNumber` refinement (see [`validate_no_conflicting_added_delete_files`]) — a
    /// conservative over-scan that can only over-reject, never under-reject.
    ///
    /// **Case sensitivity:** Java binds the conflict filter with `isCaseSensitive()`. This action has no such
    /// field, so the filter is bound case-sensitive (`true`) — the Iceberg/Java default for column resolution.
    async fn validate(
        self: Arc<Self>,
        starting_snapshot_id: Option<i64>,
        current: &Table,
    ) -> Result<()> {
        // Java `BaseRowDelta.validate` uses `startingSnapshotId` (the `validateFromSnapshot` override) when
        // set, else the operation's starting snapshot. Both checks share this start + the conflict filter.
        let effective_start = self.validate_from_snapshot.or(starting_snapshot_id);
        let conflict_filter = self.conflict_detection_filter.as_ref();

        // 1. Concurrent-added DATA-file conflict (Java `validateNewDataFiles` branch). The walk + bind +
        //    per-file inclusive-metrics evaluation + non-retryable-conflict error are the shared helper
        //    (also used by `OverwriteFiles`).
        if self.validate_no_conflicting_data_files {
            validate_no_conflicting_added_data_files(
                current,
                effective_start,
                conflict_filter,
                true,
            )
            .await?;
        }

        // 2. Concurrent-added DELETE-file conflict (Java `validateNewDeleteFiles` branch, L159-168). This ONE
        //    flag gates BOTH sub-checks, mirroring Java where both live inside `if (validateNewDeleteFiles)`.
        if self.validate_no_conflicting_delete_files {
            // 2a. `validateNoNewDeletesForDataFiles` on the REMOVED data files (Java L161-164, the
            //     `!removedDataFiles.isEmpty()` sub-branch): reject if a concurrent commit added a delete that
            //     APPLIES to one of the data files this row delta REMOVES. Delegates to the SHARED helper that
            //     `OverwriteFiles.validateNoConflictingDeletes` uses, with `ignore_equality_deletes = false`
            //     (Java's non-rewrite path counts ALL applicable deletes — both position and equality). Runs
            //     only when `remove_data_files`/`remove_rows` supplied removed files; reuses the same
            //     `effective_start` + `conflict_filter` (no refreshed-head re-read). A no-op on a V1 table.
            if !self.removed_data_files.is_empty() {
                validate_no_new_deletes_for_data_files(
                    current,
                    effective_start,
                    conflict_filter,
                    &self.removed_data_files,
                    false,
                )
                .await?;
            }

            // 2b. `validateNoNewDeleteFiles` (Java L167): the filter-based check. The DELETE-manifest walk +
            //     V2 guard live in the delete helper; the per-file test is the SAME inclusive-metrics check as
            //     the data-file branch.
            validate_no_conflicting_added_delete_files(
                current,
                effective_start,
                conflict_filter,
                true,
            )
            .await?;
        }

        // 3. Referenced-data-files-exist check (Java `validateDataFilesExist`, the `!referencedDataFiles
        //    .isEmpty()` guard at L141-149). Only runs when the caller provided referenced files via
        //    `validate_data_files_exist(...)`. `skipDeletes = !validateDeletes` (Java L146) — so the DEFAULT
        //    excludes concurrent DELETE-op snapshots (`{OVERWRITE}` op set) and `validate_deleted_files()`
        //    includes them (`{OVERWRITE, DELETE}`). Reject if any concurrently-DELETED data file is one the
        //    added position-deletes reference.
        if !self.referenced_data_files.is_empty() {
            let skip_deletes = !self.validate_deleted_files;
            let deleted = deleted_data_files_after(current, effective_start, skip_deletes).await?;
            if let Some(missing) = deleted
                .iter()
                .find(|file| self.referenced_data_files.contains(file.file_path()))
            {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("Cannot commit, missing data files: {}", missing.file_path()),
                ));
            }
        }

        // 4. Removed-data-files vs referenced-data-files self-contradiction (Java
        //    `validateNoConflictingFileAndPositionDeletes`, called UNCONDITIONALLY right before
        //    `validateAddedDVs` in `BaseRowDelta.validate` — 1.10.0-bytecode-verified). Self-skips when
        //    either set is empty.
        self.validate_no_conflicting_file_and_position_deletes()?;

        // 5. Concurrently-added deletion-vector conflict (Java `validateAddedDVs`, L172 / L825-895). Called
        //    UNCONDITIONALLY (NOT behind any flag, unlike steps 1-3) but SELF-SKIPS when this row delta adds no
        //    DVs. Reuses the same `effective_start` + `conflict_filter`.
        self.validate_added_dvs(current, effective_start, conflict_filter)
            .await?;

        Ok(())
    }
}

/// The [`SnapshotProduceOperation`] for [`RowDeltaAction`].
///
/// A row delta only ADDS files (data + deletes), so it removes nothing from the existing manifests:
/// `delete_files` returns empty and `existing_manifest` carries every current manifest forward. The
/// added data files reach the producer via `SnapshotProducer::new` and the added delete files via
/// `with_added_delete_files`, so the single snapshot carries the new DATA manifest and the new DELETE
/// manifest alongside the carried-forward manifests.
struct RowDeltaOperation {
    /// Whether this row delta requested any added data files (Java `addsDataFiles()`).
    adds_data_files: bool,
    /// Whether this row delta requested any added delete files (Java `addsDeleteFiles()`).
    adds_delete_files: bool,
    /// Whether this row delta requested any REMOVED delete files (Java `deletesDeleteFiles()` —
    /// `removeDeletes` feeds the delete-file filter manager). NOTE: 1.10.0 `operation()` does NOT
    /// consult this (or `deletesDataFiles()`); it is kept so the classification's inputs document the
    /// full request shape and so the empty-commit check (a remove-deletes-only commit is non-empty) can
    /// be reasoned about at this seam.
    removes_delete_files: bool,
}

impl SnapshotProduceOperation for RowDeltaOperation {
    /// Classify the recorded operation exactly as Java **1.10.0** `BaseRowDelta.operation()` does
    /// (BYTECODE-VERIFIED against `iceberg-core-1.10.0.jar` — the version the interop oracle pins):
    ///
    /// ```text
    /// if (addsDeleteFiles() && !addsDataFiles()) return DELETE;
    /// return OVERWRITE;
    /// ```
    ///
    /// A TWO-branch form: adds-deletes-only (no data files) → [`Operation::Delete`]; everything else →
    /// [`Operation::Overwrite`]. There is **no APPEND branch** in 1.10.0.
    ///
    /// **Divergence from MAIN — and why 1.10.0 wins (the 2026-06-08 lesson's third condition, settled
    /// 2026-06-10):** the MAIN `core/BaseRowDelta.java` source has a THIRD, leading branch
    /// `if (addsDataFiles() && !addsDeleteFiles() && !deletesDataFiles()) return APPEND;` — a
    /// POST-1.10.0 addition. The earlier Rust port mirrored MAIN (a 3-branch form with an APPEND arm),
    /// pinned by `test_row_delta_add_data_only_records_append`. The 2026-06-08 lesson warned that
    /// "when removeRows/removeDeletes land, the third condition MUST be added" — but reading the 1.10.0
    /// BYTECODE (the version the interop oracle pins, and the authority per the
    /// "MAIN lines are navigation hints only" rule) shows 1.10.0 has NEITHER the APPEND branch NOR a
    /// `deletes*` term. So the faithful-to-the-pinned-version fix is to DROP to the two-branch form, NOT
    /// to add MAIN's third condition. This re-classifies an add-data-only row delta as OVERWRITE (1.10.0)
    /// rather than APPEND (MAIN) — unobservable via interop (the oracle never commits an add-data-only
    /// row delta; data is `newFastAppend`ed), and correct for the removal cases: an add-data +
    /// `removeDeletes` row delta is OVERWRITE either way (it adds no delete files and is not
    /// adds-deletes-only), and a remove-deletes-only row delta (no adds) is OVERWRITE (Java's
    /// `addsDeleteFiles()` is false ⇒ the DELETE branch's `&& !addsDataFiles()` is moot, falls through).
    fn operation(&self) -> Operation {
        // 1.10.0 bytecode: `addsDeleteFiles() && !addsDataFiles()` ⇒ DELETE, else OVERWRITE.
        // `removes_delete_files` is intentionally not consulted (1.10.0 `operation()` ignores
        // `deletesDeleteFiles()`); it is bound on the struct only to document the request shape.
        let _ = self.removes_delete_files;
        if self.adds_delete_files && !self.adds_data_files {
            Operation::Delete
        } else {
            Operation::Overwrite
        }
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        Ok(vec![])
    }

    async fn delete_files(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<DataFile>> {
        // A row delta removes no existing files (it only ADDS data + deletes).
        Ok(vec![])
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        // Carry every current manifest (data AND delete) forward unchanged — a row delta adds new
        // manifests without rewriting existing ones (Java `MergingSnapshotProducer` keeps all existing
        // manifests for an add-only operation).
        let Some(snapshot) = snapshot_produce.table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_produce.table.file_io(),
                &snapshot_produce.table.metadata_ref(),
            )
            .await?;

        Ok(manifest_list.entries().to_vec())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use futures::TryStreamExt;

    use crate::arrow::caching_delete_file_loader::CachingDeleteFileLoader;
    use crate::delete_file_index::is_deletion_vector;
    use crate::delete_vector::DeleteVector;
    use crate::expr::Reference;
    use crate::memory::tests::new_memory_catalog;
    use crate::scan::FileScanTaskDeleteFile;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Datum, Literal,
        ManifestContentType, ManifestStatus, Operation, Struct,
    };
    use crate::table::Table;
    use crate::transaction::snapshot::{
        DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
    };
    use crate::transaction::tests::{
        make_v2_minimal_table_in_catalog, make_v3_minimal_table_in_catalog,
    };
    use crate::transaction::{ApplyTransactionAction, Transaction};
    use crate::writer::base_writer::position_delete_writer::{
        PositionDeleteFileWriterBuilder, PositionDeleteWriterConfig,
    };
    use crate::writer::file_writer::ParquetWriterBuilder;
    use crate::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use crate::writer::{IcebergWriter, IcebergWriterBuilder};
    use crate::{Catalog, ErrorKind};

    /// A position-delete file describing a `DataFile` (content `PositionDeletes`) routed to partition
    /// `x = part_value`, with a unique path (NOT a real parquet file — used for manifest-only tests).
    fn synthetic_delete_file(path: &str, part_value: i64) -> DataFile {
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

    /// A synthetic DELETION VECTOR (DV) describing a data file: a PUFFIN-format delete file
    /// (`DataFileFormat::Puffin` ⇒ Java `ContentFileUtil.isDV`), content `PositionDeletes`, routed to
    /// partition `x = part_value`, with the DV-required `referenced_data_file` / `content_offset` /
    /// `content_size_in_bytes` set. Mirrors `synthetic_delete_file` but as a DV (Puffin format) for the
    /// `validateAddedDVs` conflict tests. NOT a real puffin file — used for manifest-only / validation tests.
    fn synthetic_dv_file(path: &str, part_value: i64, referenced_data_file: &str) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Puffin)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(part_value))]))
            .referenced_data_file(Some(referenced_data_file.to_string()))
            .content_offset(Some(4))
            .content_size_in_bytes(Some(40))
            .build()
            .unwrap()
    }

    /// A synthetic data file routed to partition `x = part_value` (NOT a real parquet file).
    fn synthetic_data_file(path: &str, part_value: i64) -> DataFile {
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

    /// Append the given data files in a single fast-append commit and return the updated table.
    async fn append_files(catalog: &impl Catalog, table: &Table, files: Vec<DataFile>) -> Table {
        let tx = Transaction::new(table);
        let action = tx.fast_append().add_data_files(files);
        let tx = action.apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
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

    // ------------------------------------------------------------------------------------------------
    // THE CROWN-JEWEL END-TO-END TEST
    // ------------------------------------------------------------------------------------------------

    /// THE deliverable. Proves the entire merge-on-read WRITE → READ chain:
    /// 1. create a table, fast-append a REAL parquet data file with known rows (x=0 partition, y =
    ///    [10,20,30,40,50]);
    /// 2. produce a REAL position-delete file with the 5a `PositionDeleteFileWriter` pointing at that
    ///    data file at positions {1, 3} (the rows y=20 and y=40);
    /// 3. `row_delta().add_deletes([that delete file]).commit()`;
    /// 4. SCAN the table and assert the deleted rows are ABSENT — the result is exactly {10, 30, 50}.
    ///
    /// Risk pinned: deletes not applied = the feature silently does nothing (a scan that still returns
    /// the deleted rows means RowDelta committed a delete file the read side never honored). Mangled
    /// positions = wrong rows deleted. This is the only test that proves the write path produces delete
    /// files the scan actually applies.
    #[tokio::test]
    async fn test_row_delta_position_deletes_drop_deleted_rows_from_scan() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;

        // 1. Write a real parquet data file with 5 rows, all in partition x=0, y = [10,20,30,40,50].
        let data_file = write_data_file(&table, "rows.parquet", 0, &[
            (0, 10, 100),
            (0, 20, 200),
            (0, 30, 300),
            (0, 40, 400),
            (0, 50, 500),
        ])
        .await;
        let data_file_path = data_file.file_path().to_string();
        let table = append_files(&catalog, &table, vec![data_file]).await;

        // Sanity: before any delete, the scan returns all five y values.
        let before: HashSet<i64> = scan_y_values(&table).await;
        assert_eq!(
            before,
            HashSet::from([10, 20, 30, 40, 50]),
            "before the row delta, the scan returns all five rows"
        );

        // 2. Produce a REAL position-delete file deleting positions 1 and 3 (y=20 and y=40).
        let delete_file = write_position_delete_file(&table, 0, &[
            (data_file_path.clone(), 1),
            (data_file_path.clone(), 3),
        ])
        .await;
        assert_eq!(delete_file.content_type(), DataContentType::PositionDeletes);

        // 3. RowDelta: add the delete file in one snapshot.
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![delete_file]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // 4. Scan: the deleted rows (y=20, y=40) must be ABSENT — the surviving rows are {10, 30, 50}.
        let after: HashSet<i64> = scan_y_values(&table).await;
        assert_eq!(
            after,
            HashSet::from([10, 30, 50]),
            "after the row delta, the scan drops the deleted rows (y=20 and y=40)"
        );
    }

    /// THE FORWARD-APPLICATION NEGATIVE (spec line 1071: a position delete applies only when
    /// `data_seq <= delete_seq`). Proves the delete's sequence number does NOT reach FORWARD to data
    /// written by a LATER snapshot — the corruption inverse of the crown jewel. Scenario:
    /// 1. append D1 (seq 1) with y = [10,20,30,40,50] in partition x=0;
    /// 2. `row_delta().add_deletes` a position-delete for D1 at positions {1,3} (seq 2);
    /// 3. append a NEW data file D2 (seq 3) in the SAME partition x=0 with y = [60,70,80,90,100] —
    ///    D2 ALSO has live rows at positions 1 and 3 (y=70, y=90).
    /// Assert: the scan drops only D1's pos {1,3} (y=20,40 gone) and keeps EVERY D2 row — the delete's
    /// seq 2 does NOT reach D2's seq 3 (`3 <= 2` is false), so D2 is fully intact even though it shares
    /// the partition AND has rows at the deleted positions. A wrong-forward (delete reaching D2) would
    /// wrongly drop y=70 and y=90. This is the test the seq-inheritance correctness hinges on: it isolates
    /// the SEQUENCE dimension (same partition, same positions) so ONLY the seq guard can save D2.
    #[tokio::test]
    async fn test_row_delta_position_delete_does_not_apply_to_later_data() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;

        // 1. D1 (seq 1): y = [10,20,30,40,50], partition x=0.
        let d1 = write_data_file(&table, "d1.parquet", 0, &[
            (0, 10, 100),
            (0, 20, 200),
            (0, 30, 300),
            (0, 40, 400),
            (0, 50, 500),
        ])
        .await;
        let d1_path = d1.file_path().to_string();
        let table = append_files(&catalog, &table, vec![d1]).await;
        let d1_seq = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number();

        // 2. RowDelta a position delete for D1 at positions {1,3} (seq 2).
        let delete_file =
            write_position_delete_file(&table, 0, &[(d1_path.clone(), 1), (d1_path.clone(), 3)])
                .await;
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![delete_file]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let delete_seq = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number();

        // 3. Append D2 (seq 3) in the SAME partition x=0 with rows at positions 1 and 3 too.
        let d2 = write_data_file(&table, "d2.parquet", 0, &[
            (0, 60, 600),
            (0, 70, 700),
            (0, 80, 800),
            (0, 90, 900),
            (0, 100, 1000),
        ])
        .await;
        let table = append_files(&catalog, &table, vec![d2]).await;
        let d2_seq = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number();

        // Sanity on the sequence ordering: data(1) < delete(2) < later-data(3).
        assert!(
            d1_seq < delete_seq && delete_seq < d2_seq,
            "expected d1_seq({d1_seq}) < delete_seq({delete_seq}) < d2_seq({d2_seq})"
        );

        // The scan must drop D1's pos {1,3} (y=20,40) but keep EVERY D2 row — the delete does not reach
        // forward to data added in a later snapshot.
        let after: HashSet<i64> = scan_y_values(&table).await;
        assert_eq!(
            after,
            HashSet::from([10, 30, 50, 60, 70, 80, 90, 100]),
            "the delete (seq 2) drops only D1's pos 1,3 (y=20,40); D2 (seq 3) is fully intact"
        );
        // Belt-and-suspenders: the rows at the SAME positions in D2 (y=70 at pos 1, y=90 at pos 3) must
        // survive — proving it is the sequence number, not the position, that spares D2.
        assert!(
            after.contains(&70) && after.contains(&90),
            "D2's rows at the deleted positions must survive (the delete's seq does not reach forward)"
        );
    }

    // ------------------------------------------------------------------------------------------------
    // Manifest / summary / sequence-number tests (use synthetic files — no scan)
    // ------------------------------------------------------------------------------------------------

    /// Pins: a row delta that adds a delete file writes a DELETE manifest (`content == Deletes`) and
    /// references it in the snapshot's manifest list (alongside any DATA manifests). Risk: the delete
    /// file silently going into a DATA manifest (Java cannot read it / the read side never indexes it),
    /// or no delete manifest being written at all.
    #[tokio::test]
    async fn test_row_delta_writes_delete_manifest_with_deletes_content() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/a-pos-del.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        let delete_manifests: Vec<_> = manifest_list
            .entries()
            .iter()
            .filter(|m| m.content == ManifestContentType::Deletes)
            .collect();
        assert_eq!(
            delete_manifests.len(),
            1,
            "exactly one DELETE manifest must be written and referenced in the manifest list"
        );

        // The delete manifest's single entry is the added position-delete file with Deletes content.
        let delete_manifest = delete_manifests[0]
            .load_manifest(table.file_io())
            .await
            .unwrap();
        let entries: Vec<_> = delete_manifest.entries().iter().collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].content_type(), DataContentType::PositionDeletes);
        assert_eq!(entries[0].file_path(), "test/a-pos-del.parquet");
        assert_eq!(entries[0].status(), ManifestStatus::Added);
    }

    /// Pins: a row delta can add data files AND delete files in ONE snapshot — a DATA manifest and a
    /// DELETE manifest both land in the same manifest list, and the operation is `Overwrite` (Java
    /// `BaseRowDelta.operation()` = OVERWRITE when both data and deletes are added). Risk: only one of
    /// the two manifests being written, or the wrong operation recorded.
    #[tokio::test]
    async fn test_row_delta_add_data_and_deletes_in_one_snapshot() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_data_files(vec![synthetic_data_file("test/b.parquet", 0)])
            .add_deletes(vec![synthetic_delete_file("test/a-pos-del.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(
            snapshot.summary().operation,
            Operation::Overwrite,
            "adds-data + adds-deletes records Overwrite (Java BaseRowDelta.operation())"
        );

        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        // Collect the live data + delete file paths across the new snapshot's manifests, keyed by the
        // manifest's content type, so we can prove the added data file landed in a DATA manifest and the
        // added delete file landed in a DELETE manifest (and the prior fast-appended file survives).
        let mut data_paths = HashSet::new();
        let mut delete_paths = HashSet::new();
        let mut delete_manifest_count = 0;
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            if manifest_file.content == ManifestContentType::Deletes {
                delete_manifest_count += 1;
            }
            for entry in manifest.entries() {
                if !entry.is_alive() {
                    continue;
                }
                match manifest_file.content {
                    ManifestContentType::Data => {
                        data_paths.insert(entry.file_path().to_string());
                    }
                    ManifestContentType::Deletes => {
                        delete_paths.insert(entry.file_path().to_string());
                    }
                }
            }
        }

        assert!(
            data_paths.contains("test/b.parquet"),
            "the added data file b.parquet lands in a DATA manifest; data paths = {data_paths:?}"
        );
        assert!(
            data_paths.contains("test/a.parquet"),
            "the prior fast-appended data file a.parquet survives"
        );
        assert_eq!(
            delete_manifest_count, 1,
            "exactly one DELETE manifest is written in the row-delta snapshot"
        );
        assert!(
            delete_paths.contains("test/a-pos-del.parquet"),
            "the added delete file lands in the DELETE manifest; delete paths = {delete_paths:?}"
        );
    }

    /// Pins: an add-deletes-only row delta (no data files) is ALLOWED (the relaxed precondition) and
    /// records `Delete` (Java `BaseRowDelta.operation()` = DELETE when only deletes are added). Risk:
    /// the producer's empty-commit precondition wrongly rejecting an add-deletes-only commit.
    #[tokio::test]
    async fn test_row_delta_add_deletes_only_allowed() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/a-pos-del.parquet", 0)]);
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
            "an add-deletes-only row delta records Delete (Java BaseRowDelta.operation())"
        );
    }

    /// THE OPERATION-CLASSIFICATION PIN (1.10.0 bytecode; the 2026-06-08 lesson's "third condition",
    /// settled 2026-06-10). An add-DATA-only row delta (no delete files) records `Overwrite` per Java
    /// **1.10.0** `BaseRowDelta.operation()` — the two-branch `addsDeleteFiles() && !addsDataFiles() ⇒
    /// DELETE; else ⇒ OVERWRITE`, which has NO APPEND branch (verified against `iceberg-core-1.10.0.jar`;
    /// MAIN's leading `addsDataFiles() && !addsDeleteFiles() && !deletesDataFiles() ⇒ APPEND` is a
    /// POST-1.10.0 addition). The pre-fix Rust port mirrored MAIN and asserted `Append` here; the interop
    /// oracle pins 1.10.0, so the faithful classification is `Overwrite`.
    ///
    /// Risk pinned: a classifier that wrongly records `Append` (the MAIN/pre-fix behavior) — and, with
    /// `removeDeletes` now landing, the broader risk the lesson flagged: an add + REMOVE row delta
    /// mislabelled `Append`. This and the remove-only / add-deletes-only pins together cover both branches
    /// of the 1.10.0 form. Mutation: re-adding the APPEND branch flips this to `Append` and fails.
    #[tokio::test]
    async fn test_row_delta_add_data_only_records_overwrite_per_1_10_0() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_data_files(vec![synthetic_data_file("test/b.parquet", 0)]);
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
            "an add-data-only row delta records Overwrite per Java 1.10.0 BaseRowDelta.operation() \
             (no APPEND branch in 1.10.0 — MAIN's append arm is post-1.10.0)"
        );
    }

    /// Pins the row-delta SUMMARY counts: an add-data + add-position-delete row delta reports one added
    /// data file, one added delete file, one added position-delete file, and the right record/delete
    /// counts. Risk: the summary not reflecting the added delete files (downstream tooling that reads
    /// `added-delete-files`/`added-position-deletes` would under-report).
    #[tokio::test]
    async fn test_row_delta_summary_reflects_added_data_and_delete_counts() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        // The delete file carries record_count 3 (three deleted positions).
        let delete_file = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/a-pos-del.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(3)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(0))]))
            .build()
            .unwrap();

        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_data_files(vec![synthetic_data_file("test/b.parquet", 0)])
            .add_deletes(vec![delete_file]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(
            summary_prop(&table, "added-data-files").as_deref(),
            Some("1"),
            "one added data file"
        );
        assert_eq!(
            summary_prop(&table, "added-delete-files").as_deref(),
            Some("1"),
            "one added delete file"
        );
        assert_eq!(
            summary_prop(&table, "added-position-delete-files").as_deref(),
            Some("1"),
            "one added position-delete file"
        );
        assert_eq!(
            summary_prop(&table, "added-position-deletes").as_deref(),
            Some("3"),
            "three added position deletes (the delete file's record count)"
        );
    }

    /// Pins the SEQUENCE-NUMBER correctness — the wrong-seq risk: "deletes apply to wrong data". The
    /// added delete entry must carry the NEW snapshot's sequence number (inherited at read time), which
    /// is STRICTLY GREATER than the earlier data file's sequence number, so the delete applies to that
    /// earlier data (`data_seq <= delete_seq`). Risk: stamping the delete entry with an old/zero seq
    /// (so it would NOT apply to existing data) or the data file's own seq.
    #[tokio::test]
    async fn test_row_delta_added_delete_entry_inherits_new_snapshot_sequence_number() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;

        // Append data in its own snapshot → it gets data sequence number 1.
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;
        let data_snapshot = table.metadata().current_snapshot().unwrap();
        let data_seq = data_snapshot.sequence_number();

        // RowDelta the delete in a LATER snapshot.
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/a-pos-del.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let delete_snapshot = table.metadata().current_snapshot().unwrap();
        let delete_seq = delete_snapshot.sequence_number();
        assert!(
            delete_seq > data_seq,
            "the row-delta snapshot's sequence number ({delete_seq}) must exceed the data snapshot's ({data_seq})"
        );

        // The added delete entry must read back with the NEW snapshot's sequence number (inherited),
        // NOT a stale or zero seq — this is what makes the delete apply to the earlier data.
        let manifest_list = delete_snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let mut found = false;
        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Deletes {
                continue;
            }
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                if entry.file_path() == "test/a-pos-del.parquet" {
                    assert_eq!(
                        entry.sequence_number(),
                        Some(delete_seq),
                        "the added delete entry inherits the new snapshot's sequence number"
                    );
                    assert_eq!(
                        entry.snapshot_id(),
                        Some(delete_snapshot.snapshot_id()),
                        "the added delete entry carries the new snapshot id"
                    );
                    found = true;
                }
            }
        }
        assert!(
            found,
            "the added delete entry must be present in a DELETE manifest"
        );
    }

    /// Pins: `add_deletes` rejects a `Data`-content file (a delete file must be position/equality
    /// content). Risk: a data file silently committed as a delete (corrupting the table — it would be
    /// indexed as a delete file and never read as data).
    #[tokio::test]
    async fn test_row_delta_rejects_data_content_in_add_deletes() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        let tx = Transaction::new(&table);
        // A Data-content file passed to add_deletes must be rejected.
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_data_file("test/not-a-delete.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let err = tx
            .commit(&catalog)
            .await
            .expect_err("a Data-content file in add_deletes must be rejected");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message().contains("position-delete or equality-delete"),
            "unexpected error: {}",
            err.message()
        );
    }

    /// Pins: a delete file whose partition spec id does not match the table default is rejected. Risk:
    /// a mismatched-spec delete file that the read side cannot associate to the right partition.
    #[tokio::test]
    async fn test_row_delta_rejects_partition_spec_mismatch() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        let bad_delete = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/bad-spec.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            // Wrong partition spec id (table default is 0).
            .partition_spec_id(999)
            .partition(Struct::from_iter([Some(Literal::long(0))]))
            .build()
            .unwrap();

        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![bad_delete]);
        let tx = action.apply(tx).unwrap();
        let err = tx
            .commit(&catalog)
            .await
            .expect_err("a partition-spec-mismatched delete file must be rejected");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message().contains("partition spec id"),
            "unexpected error: {}",
            err.message()
        );
    }

    /// Pins: a truly-empty row delta (no data, no deletes, no snapshot properties) is REJECTED. Risk:
    /// the relaxed precondition being too permissive and producing an empty no-op snapshot.
    #[tokio::test]
    async fn test_empty_row_delta_is_rejected() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        let tx = Transaction::new(&table);
        let action = tx.row_delta();
        let tx = action.apply(tx).unwrap();
        let result = tx.commit(&catalog).await;
        assert!(result.is_err(), "a truly-empty row delta must be rejected");
    }

    // ------------------------------------------------------------------------------------------------
    // Crown-jewel helpers: write REAL parquet data + position-delete files into the table's FileIO.
    // ------------------------------------------------------------------------------------------------

    /// Write a real parquet DATA file with the (x, y, z) rows into the table's location and return a
    /// [`DataFile`] describing it (content `Data`, partition `x = part_value`, spec id 0). The file is
    /// written via the table's own `FileIO` so the scan can read it back.
    async fn write_data_file(
        table: &Table,
        file_name: &str,
        part_value: i64,
        rows: &[(i64, i64, i64)],
    ) -> DataFile {
        use crate::arrow::schema_to_arrow_schema;
        use crate::writer::file_writer::{FileWriter, FileWriterBuilder};

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

        // Write the parquet directly under the table location so the scan's FileIO can read it.
        let file_path = format!("{}/data/{}", table.metadata().location(), file_name);
        let output = table.file_io().new_output(file_path.clone()).unwrap();
        let parquet_builder = ParquetWriterBuilder::new(
            parquet::file::properties::WriterProperties::builder().build(),
            schema.clone(),
        );
        let mut writer = parquet_builder.build(output).await.unwrap();
        writer.write(&batch).await.unwrap();
        let data_file_builders = writer.close().await.unwrap();

        // The parquet writer returns builders without content/partition stamped — finish them as a
        // partitioned data file.
        let mut builder = data_file_builders.into_iter().next().unwrap();
        builder
            .content(DataContentType::Data)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(part_value))]))
            .build()
            .unwrap()
    }

    /// Write a REAL position-delete parquet file (via the 5a `PositionDeleteFileWriter`) into the
    /// table's location, deleting the given `(data_file_path, pos)` pairs, in partition `x = part_value`.
    async fn write_position_delete_file(
        table: &Table,
        part_value: i64,
        deletes: &[(String, i64)],
    ) -> DataFile {
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

        // Build with the partition key so the delete file's partition matches the data file's (the
        // delete-file index keys position deletes by partition + spec id).
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

    /// Scan the table and collect the `y` column values across all returned batches.
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
            for i in 0..col.len() {
                values.insert(col.value(i));
            }
        }
        values
    }

    // ============================================================================================
    // Filter-based concurrent-commit conflict validation (Java `validateNoConflictingDataFiles` —
    // serializable isolation). Java `BaseRowDelta.validate` → `validateNewDataFiles` →
    // `MergingSnapshotProducer.validateAddedDataFiles` (L155-157 / L391-412): enumerate DATA files added by
    // concurrent commits since the starting snapshot, and reject the commit if ANY could contain records
    // matching the conflict-detection filter (via the inclusive metrics evaluator). RowDelta reuses the SAME
    // shared `validate_no_conflicting_added_data_files` helper as OverwriteFiles.
    //
    // The race these tests simulate: a `row_delta` is BUILT against table head S0, but BEFORE it commits a
    // SEPARATE `fast_append` lands on the catalog (advancing the head to S1). When the row delta then commits,
    // `do_commit` refreshes to S1 and runs the action's `validate` against that refreshed base. With
    // `validate_no_conflicting_data_files()` enabled, a concurrent append whose file could match the conflict
    // filter must FAIL the commit (non-retryable). With validation OFF (the default), it does not.
    // ============================================================================================

    /// A synthetic data file routed to partition `x = part_value` whose column `y` (schema field id 2, a
    /// `long`) carries `[y_lower, y_upper]` value bounds. The bounds let the `InclusiveMetricsEvaluator`
    /// include or exclude this file against a conflict-detection filter on `y` — the discriminating input for
    /// the metrics-MATCH vs metrics-EXCLUDE conflict tests. The minimal V3 schema is `x,y,z: long` (ids 1,2,3).
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

    /// Collect the set of live (Added or Existing) DATA file paths across the table's current snapshot — the
    /// real correctness signal (what files a scan would read).
    async fn live_data_file_paths(table: &Table) -> HashSet<String> {
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
            if manifest_file.content != ManifestContentType::Data {
                continue;
            }
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

    /// NO CONCURRENT COMMIT. With validation enabled but nothing landing concurrently, the row delta commits
    /// normally (the concurrent-added set is empty ⇒ no conflict). Pins that enabling validation does not
    /// block a race-free commit. Risk: a validation that wrongly fails when there is no concurrent commit.
    #[tokio::test]
    async fn test_row_delta_validation_no_concurrent_commit_succeeds() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        // Row delta adds a delete file with validation enabled — but NO concurrent commit lands.
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/a-pos-del.parquet", 0)])
            .validate_from_snapshot(s0)
            .validate_no_conflicting_data_files();
        let tx = action.apply(tx).unwrap();
        let table = tx
            .commit(&catalog)
            .await
            .expect("a race-free row delta must commit even with validation enabled");

        // The delete manifest is present (the commit went through).
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert!(
            manifest_list
                .entries()
                .iter()
                .any(|m| m.content == ManifestContentType::Deletes),
            "the row delta committed: a DELETE manifest is present"
        );
    }

    /// THE HEADLINE TEST. Append S0. Build a `row_delta` with `.conflict_detection_filter(y >= 50)` and
    /// `.validate_no_conflicting_data_files()`. Then a CONCURRENT `fast_append` lands a DATA file whose `y`
    /// bounds `[60,70]` OVERLAP the filter (could contain `y >= 50`). The row-delta commit must FAIL with a
    /// NON-retryable `DataInvalid` that NAMES the conflicting file.
    ///
    /// Risk pinned: silently committing a row delta (e.g. deletes computed against the snapshot the txn read)
    /// while a concurrent append added rows matching the same filter = a lost/incorrect merge-on-read result
    /// under serializable isolation. Without the check the row delta would commit blind to S1's new rows.
    #[tokio::test]
    async fn test_row_delta_rejects_concurrent_added_file_matching_filter() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        // Row delta adds a delete file, conflict filter `y >= 50`, validation enabled, pinned to S0.
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/a-pos-del.parquet", 0)])
            .conflict_detection_filter(
                Reference::new("y").greater_than_or_equal_to(Datum::long(50)),
            )
            .validate_from_snapshot(s0)
            .validate_no_conflicting_data_files();
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
            .expect_err("row delta must fail: a concurrent file could match the conflict filter");

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

        // The catalog head is still S1 (the concurrent append) — the row delta did NOT commit over it.
        let reloaded = catalog.load_table(table.identifier()).await.unwrap();
        let live = live_data_file_paths(&reloaded).await;
        assert!(
            live.contains("test/concurrent.parquet"),
            "the concurrently-added file must survive (the conflicting row delta was rejected)"
        );
    }

    /// NO-FALSE-CONFLICT TEST. Same setup as the headline, but the concurrent file's `y` bounds `[10,20]` lie
    /// ENTIRELY BELOW the filter `y >= 50` — the inclusive evaluator EXCLUDES it. The row delta must COMMIT.
    ///
    /// Risk pinned: an over-eager check that rejects ANY concurrent append (ignoring the metrics) would break
    /// legitimate concurrent writes whose data cannot match the filter (a false positive). This is the test
    /// that fails if the helper's metrics include/exclude decision is inverted.
    #[tokio::test]
    async fn test_row_delta_allows_concurrent_added_file_excluded_by_filter() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/a-pos-del.parquet", 0)])
            .conflict_detection_filter(
                Reference::new("y").greater_than_or_equal_to(Datum::long(50)),
            )
            .validate_from_snapshot(s0)
            .validate_no_conflicting_data_files();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a file whose y bounds [10,20] are entirely BELOW `y >= 50` (cannot match).
        let _concurrent = append_files(&catalog, &table, vec![data_file_with_y_bounds(
            "test/concurrent.parquet",
            0,
            10,
            20,
        )])
        .await;

        // The row delta must SUCCEED — the concurrent file's metrics exclude the filter.
        let table = tx
            .commit(&catalog)
            .await
            .expect("row delta must commit: the concurrent file cannot match the conflict filter");

        // The row delta re-bases onto S1, so the non-conflicting concurrent file also survives, and the
        // delete manifest landed.
        let live = live_data_file_paths(&table).await;
        assert!(
            live.contains("test/concurrent.parquet"),
            "the non-conflicting concurrent file survives the re-based row delta"
        );
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert!(
            manifest_list
                .entries()
                .iter()
                .any(|m| m.content == ManifestContentType::Deletes),
            "the row delta committed: a DELETE manifest is present"
        );
    }

    /// FLAG-OFF CONTROL. With validation NOT enabled (no `validate_no_conflicting_data_files()` call), a
    /// concurrent append of a file that WOULD match the filter does NOT fail the commit — this is snapshot
    /// isolation, the DEFAULT behavior, unchanged by this increment.
    ///
    /// Risk pinned: the conflict validation must be OPT-IN — turning it on for every row delta by default
    /// would change existing behavior and break callers relying on snapshot isolation.
    #[tokio::test]
    async fn test_row_delta_without_validation_allows_conflicting_concurrent_append() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        // Build a row delta WITHOUT enabling validation (default = snapshot isolation). A conflict filter is
        // even provided, to prove it is inert without the flag.
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/a-pos-del.parquet", 0)])
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

        // With validation OFF, the row delta COMMITS (default behavior unchanged).
        let table = tx.commit(&catalog).await.expect(
            "with validation OFF, a conflicting concurrent append must not block the commit",
        );

        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert!(
            manifest_list
                .entries()
                .iter()
                .any(|m| m.content == ManifestContentType::Deletes),
            "the row delta committed (snapshot isolation, no conflict check)"
        );
    }

    /// NONE-FILTER DEFAULT TEST. With validation enabled and NO `conflict_detection_filter` set, the conflict
    /// filter defaults to `AlwaysTrue` (Java `BaseRowDelta`'s default `conflictDetectionFilter` =
    /// `alwaysTrue()`) — so ANY concurrently-added data file is a conflict, even one with no bounds at all.
    ///
    /// Risk pinned: a `None` filter silently behaving as "no conflict" (the OPPOSITE of the conservative
    /// serializable default) would let every concurrent append through — a serializable-isolation hole.
    #[tokio::test]
    async fn test_row_delta_none_filter_treats_any_concurrent_add_as_conflict() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        // Row delta with validation enabled but NO conflict_detection_filter ⇒ AlwaysTrue default.
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/a-pos-del.parquet", 0)])
            .validate_from_snapshot(s0)
            .validate_no_conflicting_data_files();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a plain file with NO bounds — still a conflict under AlwaysTrue.
        let _concurrent = append_files(&catalog, &table, vec![synthetic_data_file(
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
    /// count as concurrent. Append S0, then append S1 (BEFORE the transaction is built), then build the row
    /// delta. With `validate_from_snapshot(S0)` (an EARLIER snapshot), S1's file IS counted as concurrent ⇒
    /// rejected (None filter ⇒ AlwaysTrue). This proves the override widens the concurrent window to include
    /// commits between S0 and S1.
    ///
    /// Risk pinned: ignoring the `validate_from_snapshot` override (always using the tx start) would miss a
    /// conflict the caller explicitly asked to guard against by reading from an earlier snapshot.
    #[tokio::test]
    async fn test_row_delta_validate_from_snapshot_override_changes_concurrent_window() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // S0: a. Capture S0.
        let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;
        // S1: a file added BEFORE the transaction is built (part of the base under the default tx start).
        let (table, _s1) = append_and_snapshot_id(&catalog, &table, vec![synthetic_data_file(
            "test/s1.parquet",
            0,
        )])
        .await;

        // Build the row delta when the head is S1. Override the start to the EARLIER S0 so S1 counts as
        // concurrent. None filter ⇒ AlwaysTrue ⇒ S1's added file is a conflict.
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/a-pos-del.parquet", 0)])
            .validate_from_snapshot(s0)
            .validate_no_conflicting_data_files();
        let tx = action.apply(tx).unwrap();

        let err = tx.commit(&catalog).await.expect_err(
            "validate_from_snapshot(S0) widens the window to include S1's add ⇒ conflict",
        );
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(!err.retryable());
        assert!(err.message().contains("test/s1.parquet"));
    }

    /// NEGATIVE HALF of the override test: with `validate_from_snapshot(S1)` (the CURRENT head when the tx is
    /// built), S1's file is at the start boundary and is NOT concurrent — so the same row delta COMMITS. This
    /// pins that the override genuinely shifts the boundary (the S0 half above rejects the SAME S1 file).
    #[tokio::test]
    async fn test_row_delta_validate_from_snapshot_at_head_finds_no_conflict() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;

        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;
        let (table, s1) = append_and_snapshot_id(&catalog, &table, vec![synthetic_data_file(
            "test/s1.parquet",
            0,
        )])
        .await;

        // Override the start to S1 (the current head) — nothing is concurrent.
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/a-pos-del.parquet", 0)])
            .validate_from_snapshot(s1)
            .validate_no_conflicting_data_files();
        let tx = action.apply(tx).unwrap();
        let table = tx
            .commit(&catalog)
            .await
            .expect("with start = current head, nothing is concurrent ⇒ commit succeeds");

        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert!(
            manifest_list
                .entries()
                .iter()
                .any(|m| m.content == ManifestContentType::Deletes),
            "the row delta committed: a DELETE manifest is present"
        );
    }

    /// TX-CAPTURED START SURVIVES RE-BASE. The conflict check works WITHOUT an explicit
    /// `validate_from_snapshot`, relying solely on the transaction-captured starting snapshot id surviving
    /// `do_commit`'s re-base. The action calls ONLY `.validate_no_conflicting_data_files()` (None filter ⇒
    /// AlwaysTrue). The starting snapshot is the one captured in `Transaction::new` (= S0); `do_commit`
    /// overwrites `self.table` with the refreshed base (S1), but `starting_snapshot_id` must SURVIVE — so the
    /// concurrent S1 is still enumerated and rejected.
    ///
    /// Risk pinned: if the start were re-read from the refreshed head at validation time, start == current
    /// head ⇒ the concurrent set is empty ⇒ the check silently always passes (a serializable-isolation hole).
    /// The other enabled tests pin `validate_from_snapshot`, so this is the only RowDelta test that the
    /// `Transaction::new` capture survives the re-base.
    #[tokio::test]
    async fn test_row_delta_rejects_concurrent_using_tx_captured_starting_snapshot() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        // Build the row delta with validation enabled but WITHOUT validate_from_snapshot — the start is the
        // tx-captured head (S0).
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/a-pos-del.parquet", 0)])
            .validate_no_conflicting_data_files();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1).
        let _concurrent = append_files(&catalog, &table, vec![synthetic_data_file(
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

    // ============================================================================================
    // Filter-based concurrent-commit DELETE-FILE conflict validation (Java
    // `validateNoConflictingDeleteFiles`). Java `BaseRowDelta.validate` → `validateNewDeleteFiles` →
    // `MergingSnapshotProducer.validateNoNewDeleteFiles` (L159-167 / L562-570) → `addedDeleteFiles`
    // (L601-625): enumerate DELETE files (position / equality) added by concurrent commits since the
    // starting snapshot, and reject the commit if ANY could APPLY to records matching the conflict filter
    // (via the inclusive metrics evaluator). `addedDeleteFiles` is V2-ONLY and gated to the
    // `{OVERWRITE, DELETE}` operation set. RowDelta reuses the SHARED `first_conflicting_file` test that the
    // data-file check uses, via the new `validate_no_conflicting_added_delete_files` helper.
    //
    // The race these tests simulate: a `row_delta` is BUILT against table head S0, but BEFORE it commits a
    // SEPARATE `row_delta().add_deletes(...)` lands on the catalog (advancing the head to S1 with a DELETE
    // manifest). When the first row delta then commits, `do_commit` refreshes to S1 and runs the action's
    // `validate` against that refreshed base. With `validate_no_conflicting_delete_files()` enabled, a
    // concurrent delete file whose metrics could match the conflict filter must FAIL the commit
    // (non-retryable). With validation OFF (the default), it does not. The V2 guard makes it a no-op on V1.
    // ============================================================================================

    /// A position-delete file routed to partition `x = part_value` carrying `[y_lower, y_upper]` value
    /// bounds on column `y` (schema field id 2, a `long`). The bounds let the `InclusiveMetricsEvaluator`
    /// include or exclude this DELETE file against a conflict-detection filter on `y` — the discriminating
    /// input for the metrics-MATCH vs metrics-EXCLUDE delete-conflict tests. (The evaluator is
    /// content-agnostic: it reads the file's `lower_bounds`/`upper_bounds` regardless of content type.)
    fn delete_file_with_y_bounds(
        path: &str,
        part_value: i64,
        y_lower: i64,
        y_upper: i64,
    ) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
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

    /// Commit a CONCURRENT row delta that ADDS the given DELETE files (no data) in its own snapshot, via the
    /// catalog — the merge-on-read counterpart of `append_files`. The resulting snapshot's operation is
    /// `Delete` (add-deletes-only, Java `BaseRowDelta.operation()`), which is in
    /// `VALIDATE_ADDED_DELETE_FILES_OPERATIONS = {OVERWRITE, DELETE}` so the delete walk enumerates it.
    async fn commit_concurrent_deletes(
        catalog: &impl Catalog,
        table: &Table,
        delete_files: Vec<DataFile>,
    ) -> Table {
        let tx = Transaction::new(table);
        let action = tx.row_delta().add_deletes(delete_files);
        let tx = action.apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    /// Create a V1 minimal table (schema `x,y,z: long`, identity-partitioned by `x`) registered in the
    /// catalog — mirroring `make_v3_minimal_table_in_catalog`'s shape but at format version 1, to exercise
    /// the V2 guard on the delete-conflict check. The schema is hand-built with NO column defaults (the V3
    /// minimal fixture carries a V3-only `initial-default` on `x` that the V3 schema guard rejects on V1).
    async fn make_v1_minimal_table_in_catalog(catalog: &impl Catalog) -> Table {
        use crate::spec::{
            NestedField, PartitionSpec, PrimitiveType, Schema, Transform, Type,
            UnboundPartitionField,
        };
        use crate::{TableCreation, TableIdent};

        let table_ident =
            TableIdent::from_strs([format!("ns1-{}", uuid::Uuid::new_v4()), "test1".to_string()])
                .unwrap();
        catalog
            .create_namespace(table_ident.namespace(), HashMap::new())
            .await
            .unwrap();

        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "x", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "y", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(3, "z", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();

        let partition_spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .add_unbound_field(
                UnboundPartitionField::builder()
                    .source_id(1)
                    .name("x".to_string())
                    .transform(Transform::Identity)
                    .build(),
            )
            .unwrap()
            .build()
            .unwrap();

        let table_creation = TableCreation::builder()
            .schema(schema)
            .partition_spec(partition_spec)
            .name(table_ident.name().to_string())
            .format_version(crate::spec::FormatVersion::V1)
            .build();

        catalog
            .create_table(table_ident.namespace(), table_creation)
            .await
            .unwrap()
    }

    /// NO CONCURRENT DELETE. With the delete check enabled but nothing landing concurrently, the row delta
    /// commits normally (the concurrent-added delete set is empty ⇒ no conflict). Pins that enabling the
    /// delete check does not block a race-free commit. Risk: a delete check that wrongly fails with no
    /// concurrent delete commit.
    #[tokio::test]
    async fn test_row_delta_delete_validation_no_concurrent_commit_succeeds() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/a-pos-del.parquet", 0)])
            .validate_from_snapshot(s0)
            .validate_no_conflicting_delete_files();
        let tx = action.apply(tx).unwrap();
        let table = tx
            .commit(&catalog)
            .await
            .expect("a race-free row delta must commit even with the delete check enabled");

        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert!(
            manifest_list
                .entries()
                .iter()
                .any(|m| m.content == ManifestContentType::Deletes),
            "the row delta committed: a DELETE manifest is present"
        );
    }

    /// THE HEADLINE DELETE TEST. Append S0. Build a `row_delta` with `.conflict_detection_filter(y >= 50)`
    /// and `.validate_no_conflicting_delete_files()`. Then a CONCURRENT `row_delta().add_deletes(...)` lands
    /// a DELETE file whose `y` bounds `[60,70]` OVERLAP the filter (could apply to `y >= 50`). The row-delta
    /// commit must FAIL with a NON-retryable `DataInvalid` whose message NAMES the conflicting DELETE file
    /// AND uses the DELETE-SPECIFIC wording ("conflicting delete files") — distinguishing it from the
    /// data-file message ("conflicting files that can contain records").
    ///
    /// Risk pinned: silently committing a row delta while a concurrent commit added a delete that applies to
    /// the same rows = a lost/incorrect merge-on-read result under serializable isolation. The DELETE-message
    /// assertion is what proves the delete branch (not the data branch) fired.
    #[tokio::test]
    async fn test_row_delta_rejects_concurrent_added_delete_file_matching_filter() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/my-del.parquet", 0)])
            .conflict_detection_filter(
                Reference::new("y").greater_than_or_equal_to(Datum::long(50)),
            )
            .validate_from_snapshot(s0)
            .validate_no_conflicting_delete_files();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a row delta adding a DELETE file whose y bounds [60,70] overlap `y >= 50`.
        let _concurrent =
            commit_concurrent_deletes(&catalog, &table, vec![delete_file_with_y_bounds(
                "test/concurrent-del.parquet",
                0,
                60,
                70,
            )])
            .await;

        let err = tx.commit(&catalog).await.expect_err(
            "row delta must fail: a concurrent delete file could apply to the conflict filter",
        );

        assert_eq!(
            err.kind(),
            ErrorKind::DataInvalid,
            "a conflict is a non-retryable validation failure (DataInvalid), not a commit conflict"
        );
        assert!(
            !err.retryable(),
            "the validation failure must be NON-retryable so the retry loop stops and it propagates"
        );
        // The DELETE-specific message — NOT the data-file message — must fire.
        assert!(
            err.message().contains("conflicting delete files"),
            "the error must use the DELETE-specific message, got: {}",
            err.message()
        );
        assert!(
            !err.message().contains("can contain records"),
            "the DELETE message must NOT be the data-file message, got: {}",
            err.message()
        );
        assert!(
            err.message().contains("test/concurrent-del.parquet"),
            "the error must name the conflicting DELETE file, got: {}",
            err.message()
        );
    }

    /// NO-FALSE-CONFLICT DELETE TEST. Same setup as the headline, but the concurrent DELETE file's `y` bounds
    /// `[10,20]` lie ENTIRELY BELOW the filter `y >= 50` — the inclusive evaluator EXCLUDES it. The row delta
    /// must COMMIT.
    ///
    /// Risk pinned: an over-eager delete check that rejects ANY concurrent delete (ignoring the metrics) would
    /// break legitimate concurrent deletes that cannot apply to the filtered rows (a false positive). This is
    /// the test that fails if the SHARED `first_conflicting_file` metrics decision is inverted (it fails for
    /// the data check too — the cross-action mutation).
    #[tokio::test]
    async fn test_row_delta_allows_concurrent_added_delete_file_excluded_by_filter() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/my-del.parquet", 0)])
            .conflict_detection_filter(
                Reference::new("y").greater_than_or_equal_to(Datum::long(50)),
            )
            .validate_from_snapshot(s0)
            .validate_no_conflicting_delete_files();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a delete file whose y bounds [10,20] are entirely BELOW `y >= 50`.
        let _concurrent =
            commit_concurrent_deletes(&catalog, &table, vec![delete_file_with_y_bounds(
                "test/concurrent-del.parquet",
                0,
                10,
                20,
            )])
            .await;

        let table = tx.commit(&catalog).await.expect(
            "row delta must commit: the concurrent delete file cannot apply to the conflict filter",
        );

        // The row delta committed: a DELETE manifest landed (its own added delete file).
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert!(
            manifest_list
                .entries()
                .iter()
                .any(|m| m.content == ManifestContentType::Deletes),
            "the row delta committed: a DELETE manifest is present"
        );
    }

    /// FLAG-OFF CONTROL (delete check). With the delete check NOT enabled, a concurrent delete file that
    /// WOULD match the filter does NOT fail the commit — snapshot isolation, the DEFAULT, unchanged.
    ///
    /// Risk pinned: the delete-conflict validation must be OPT-IN — turning it on for every row delta would
    /// change existing behavior.
    #[tokio::test]
    async fn test_row_delta_without_delete_validation_allows_conflicting_concurrent_delete() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        // Build a row delta WITHOUT enabling the delete check. A conflict filter is supplied to prove it is
        // inert without the flag.
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/my-del.parquet", 0)])
            .conflict_detection_filter(
                Reference::new("y").greater_than_or_equal_to(Datum::long(50)),
            );
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a delete file whose y bounds [60,70] WOULD match if the check were on.
        let _concurrent =
            commit_concurrent_deletes(&catalog, &table, vec![delete_file_with_y_bounds(
                "test/concurrent-del.parquet",
                0,
                60,
                70,
            )])
            .await;

        let table = tx.commit(&catalog).await.expect(
            "with the delete check OFF, a conflicting concurrent delete must not block the commit",
        );
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert!(
            manifest_list
                .entries()
                .iter()
                .any(|m| m.content == ManifestContentType::Deletes),
            "the row delta committed (snapshot isolation, no delete-conflict check)"
        );
    }

    /// NONE-FILTER DEFAULT (delete check). With the delete check enabled and NO conflict filter, the filter
    /// defaults to `AlwaysTrue` (Java `BaseRowDelta`'s default `conflictDetectionFilter` = `alwaysTrue()`) —
    /// so ANY concurrently-added delete file is a conflict, even one with no bounds.
    ///
    /// Risk pinned: a `None` filter silently behaving as "no conflict" would let every concurrent delete
    /// through — a serializable-isolation hole.
    #[tokio::test]
    async fn test_row_delta_delete_none_filter_treats_any_concurrent_delete_as_conflict() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/my-del.parquet", 0)])
            .validate_from_snapshot(s0)
            .validate_no_conflicting_delete_files();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a plain delete file with NO bounds — still a conflict under AlwaysTrue.
        let _concurrent = commit_concurrent_deletes(&catalog, &table, vec![synthetic_delete_file(
            "test/concurrent-del.parquet",
            0,
        )])
        .await;

        let err = tx.commit(&catalog).await.expect_err(
            "a None filter defaults to AlwaysTrue: any concurrent delete is a conflict",
        );
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(!err.retryable());
        assert!(err.message().contains("conflicting delete files"));
        assert!(err.message().contains("test/concurrent-del.parquet"));
    }

    /// V2-GUARD TEST. On a V1 table, delete files do not exist (Java `addedDeleteFiles`:
    /// `base.formatVersion() < 2` ⇒ empty). With the delete check enabled, a concurrent commit lands and the
    /// row delta still COMMITS (the delete check is a guarded no-op — no walk, no panic). The row delta adds
    /// DATA only (V1-legal). This proves the V2 guard is genuinely exercised: without it, the walk would run
    /// on a V1 table.
    ///
    /// Risk pinned: the delete check running on a V1 table (where delete manifests can't exist) — a needless
    /// walk at best, a panic / spurious rejection at worst. With the guard, V1 is a clean no-op.
    #[tokio::test]
    async fn test_row_delta_delete_check_is_noop_on_v1_table() {
        let catalog = new_memory_catalog().await;
        let table = make_v1_minimal_table_in_catalog(&catalog).await;
        assert_eq!(
            table.metadata().format_version(),
            crate::spec::FormatVersion::V1,
            "the table must be V1 for the guard to be under test"
        );
        let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        // Row delta adds DATA only (V1 has no delete files), with the delete check enabled.
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_data_files(vec![synthetic_data_file("test/b.parquet", 0)])
            .validate_from_snapshot(s0)
            .validate_no_conflicting_delete_files();
        let tx = action.apply(tx).unwrap();

        // A concurrent DATA append lands (V1 can't add delete files). The V2 guard makes the delete check a
        // no-op, so the row delta commits regardless.
        let _concurrent = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/concurrent.parquet",
            0,
        )])
        .await;

        let table = tx
            .commit(&catalog)
            .await
            .expect("the delete check is a no-op on a V1 table (V2 guard) — the row delta commits");

        let live = live_data_file_paths(&table).await;
        assert!(
            live.contains("test/b.parquet"),
            "the row delta's added data file landed on V1 (delete check no-op)"
        );

        // Direct helper-level assertion: the V2 guard short-circuits the delete-file walk on a V1 table —
        // `added_delete_files_after` returns empty regardless of the starting snapshot (delete files /
        // delete manifests cannot exist on V1, mirroring Java `addedDeleteFiles`' `formatVersion() < 2`
        // early return). This makes the guard's contribution concrete: it returns empty WITHOUT walking.
        let reloaded = catalog.load_table(table.identifier()).await.unwrap();
        let added_deletes = crate::transaction::snapshot::added_delete_files_after(&reloaded, None)
            .await
            .unwrap();
        assert!(
            added_deletes.is_empty(),
            "the V2 guard returns an empty added-delete set on a V1 table"
        );
    }

    /// INDEPENDENCE TEST (delete enabled ⇏ data checked). Enabling ONLY the DELETE check must NOT run the
    /// DATA check: a concurrent DATA append that WOULD match the filter is allowed through, while a
    /// concurrent DELETE that matches is rejected. This proves the two flags are independent (Java's two
    /// separate `validateNew*` flags).
    ///
    /// Risk pinned: the two checks being accidentally coupled (one flag enabling both) — which would either
    /// over-reject (delete flag spuriously running the data check) or under-protect.
    #[tokio::test]
    async fn test_row_delta_delete_check_does_not_run_data_check() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        // Only the DELETE check is enabled (no data check), filter `y >= 50`.
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/my-del.parquet", 0)])
            .conflict_detection_filter(
                Reference::new("y").greater_than_or_equal_to(Datum::long(50)),
            )
            .validate_from_snapshot(s0)
            .validate_no_conflicting_delete_files();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a DATA file whose y bounds [60,70] WOULD match the filter IF the data check
        // ran. Because only the DELETE check is enabled, the data check does not run and this is allowed.
        let _concurrent = append_files(&catalog, &table, vec![data_file_with_y_bounds(
            "test/concurrent-data.parquet",
            0,
            60,
            70,
        )])
        .await;

        let table = tx.commit(&catalog).await.expect(
            "enabling only the DELETE check must not run the DATA check: a matching concurrent DATA append is allowed",
        );
        let live = live_data_file_paths(&table).await;
        assert!(
            live.contains("test/concurrent-data.parquet"),
            "the concurrent DATA file survives — the delete flag did not run the data check"
        );
    }

    /// INDEPENDENCE TEST (data enabled ⇏ delete checked). The mirror of the above: enabling ONLY the DATA
    /// check must NOT run the DELETE check — a concurrent DELETE file that WOULD match the filter is allowed
    /// through. Together the two independence tests pin that neither flag implies the other.
    #[tokio::test]
    async fn test_row_delta_data_check_does_not_run_delete_check() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        // Only the DATA check is enabled (no delete check), filter `y >= 50`.
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/my-del.parquet", 0)])
            .conflict_detection_filter(
                Reference::new("y").greater_than_or_equal_to(Datum::long(50)),
            )
            .validate_from_snapshot(s0)
            .validate_no_conflicting_data_files();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a DELETE file whose y bounds [60,70] WOULD match the filter IF the delete
        // check ran. Because only the DATA check is enabled, the delete check does not run and this is allowed.
        let _concurrent =
            commit_concurrent_deletes(&catalog, &table, vec![delete_file_with_y_bounds(
                "test/concurrent-del.parquet",
                0,
                60,
                70,
            )])
            .await;

        let table = tx.commit(&catalog).await.expect(
            "enabling only the DATA check must not run the DELETE check: a matching concurrent delete is allowed",
        );
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        // The concurrent delete file survives among the delete manifests (the data flag did not run the
        // delete check, so the commit went through and re-based onto S1).
        let mut concurrent_delete_present = false;
        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Deletes {
                continue;
            }
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                if entry.is_alive() && entry.file_path() == "test/concurrent-del.parquet" {
                    concurrent_delete_present = true;
                }
            }
        }
        assert!(
            concurrent_delete_present,
            "the concurrent DELETE file survives — the data flag did not run the delete check"
        );
    }

    // ============================================================================================
    // RowDelta `validateDataFilesExist` — the REFERENCED-data-files-exist check (Java
    // `BaseRowDelta.validate` L141-149 → `MergingSnapshotProducer.validateDataFilesExist` L773-822).
    //
    // A position delete REFERENCES the data file whose rows it removes; if that data file was DELETED by a
    // concurrent commit since the operation's start, the position delete can no longer apply and committing it
    // would silently lose the delete. `validate_data_files_exist([referenced_paths])` (Java
    // `validateDataFilesExist(referencedFiles)`) provides the CALLER-PROVIDED referenced set and ENABLES the
    // check; at commit the row delta is rejected (non-retryable `DataInvalid`, "Cannot commit, missing data
    // files: {path}") if any concurrently-DELETED data file is in the referenced set.
    //
    // The SKIP-DELETES op-set axis (Java `skipDeletes = !validateDeletes`): by DEFAULT (no
    // `validate_deleted_files()` call) the walk uses the `{OVERWRITE}` op set, so a concurrent merge-on-read
    // DELETE-op snapshot that removed a referenced file is EXCLUDED (NOT a conflict). After
    // `validate_deleted_files()` the walk uses `{OVERWRITE, DELETE}` and a DELETE-op removal IS a conflict.
    //
    // These tests simulate the race: a `row_delta` is BUILT against head S0, then a SEPARATE commit DELETES a
    // referenced data file (advancing the head to S1). When the row delta commits, `do_commit` refreshes to S1
    // and runs `validate` against that base. An OVERWRITE-op deletion (`overwrite_files().add+delete`) is in
    // BOTH op sets; a DELETE-op deletion (`delete_files()`) is only in the non-skip set.
    // ============================================================================================

    /// Commit a CONCURRENT OVERWRITE that DELETES `delete_path` and ADDS `add_path` (both partition x=0),
    /// recording `Operation::Overwrite` (in BOTH `{OVERWRITE}` and `{OVERWRITE, DELETE}` op sets). The deleted
    /// data file gets a `Deleted` tombstone on a DATA manifest the new snapshot itself wrote. Used to simulate
    /// a concurrent removal that the skip-deletes-default check still sees.
    async fn commit_concurrent_overwrite_deletion(
        catalog: &impl Catalog,
        table: &Table,
        delete_path: &str,
        add_path: &str,
    ) -> Table {
        let tx = Transaction::new(table);
        let action = tx
            .overwrite_files()
            .add_file(synthetic_data_file(add_path, 0))
            .delete_file(delete_path.to_string());
        let tx = action.apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    /// Commit a CONCURRENT DELETE that removes `delete_path` (partition x=0), recording `Operation::Delete`
    /// (only in the non-skip `{OVERWRITE, DELETE}` op set). The deleted data file gets a `Deleted` tombstone on
    /// a DATA manifest. Used to exercise the skip-deletes DEFAULT (a DELETE-op deletion is excluded by default).
    async fn commit_concurrent_delete_op_deletion(
        catalog: &impl Catalog,
        table: &Table,
        delete_path: &str,
    ) -> Table {
        let tx = Transaction::new(table);
        let action = tx.delete_files().delete_file(delete_path.to_string());
        let tx = action.apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    /// NO CONCURRENT DELETION. With `validate_data_files_exist([f])` enabled but nothing removing `f`
    /// concurrently, the row delta commits normally (the concurrently-deleted set is empty ⇒ no missing file).
    /// Risk pinned: a files-exist check that wrongly fails when the referenced file is still present.
    #[tokio::test]
    async fn test_row_delta_files_exist_no_concurrent_deletion_succeeds() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![synthetic_data_file(
            "test/f.parquet",
            0,
        )])
        .await;

        // Row delta adds a position delete referencing f, with the files-exist check enabled — but f is never
        // concurrently deleted.
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/f-pos-del.parquet", 0)])
            .validate_from_snapshot(s0)
            .validate_data_files_exist(["test/f.parquet"]);
        let tx = action.apply(tx).unwrap();
        let table = tx
            .commit(&catalog)
            .await
            .expect("a row delta whose referenced file still exists must commit");

        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert!(
            manifest_list
                .entries()
                .iter()
                .any(|m| m.content == ManifestContentType::Deletes),
            "the row delta committed: a DELETE manifest is present"
        );
    }

    /// THE HEADLINE TEST. Append S0 with data file `f`. Build a `row_delta` with
    /// `.validate_data_files_exist(["test/f.parquet"])`. Then a CONCURRENT OVERWRITE DELETES `f` (S1). The
    /// row-delta commit must FAIL with a NON-retryable `DataInvalid` naming `f` ("Cannot commit, missing data
    /// files: test/f.parquet").
    ///
    /// Risk pinned: committing a position delete that references a data file a concurrent commit already
    /// removed = a silently-lost delete under serializable isolation. An OVERWRITE-op deletion is in the
    /// skip-deletes-DEFAULT op set, so this rejects WITHOUT `validate_deleted_files()`.
    #[tokio::test]
    async fn test_row_delta_files_exist_rejects_concurrent_deletion_of_referenced_file() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![synthetic_data_file(
            "test/f.parquet",
            0,
        )])
        .await;

        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/f-pos-del.parquet", 0)])
            .validate_from_snapshot(s0)
            .validate_data_files_exist(["test/f.parquet"]);
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): an OVERWRITE that DELETES the referenced f (and adds a sibling).
        let _concurrent = commit_concurrent_overwrite_deletion(
            &catalog,
            &table,
            "test/f.parquet",
            "test/g.parquet",
        )
        .await;

        let err = tx
            .commit(&catalog)
            .await
            .expect_err("row delta must fail: a referenced data file was concurrently deleted");

        assert_eq!(
            err.kind(),
            ErrorKind::DataInvalid,
            "a missing referenced data file is a non-retryable validation failure (DataInvalid)"
        );
        assert!(
            !err.retryable(),
            "the validation failure must be NON-retryable so the retry loop stops and it propagates"
        );
        assert!(
            err.message().contains("Cannot commit, missing data files"),
            "the error must use the missing-data-files message, got: {}",
            err.message()
        );
        assert!(
            err.message().contains("test/f.parquet"),
            "the error must name the missing referenced FILE, got: {}",
            err.message()
        );
    }

    /// NO-FALSE-CONFLICT TEST. Same setup, but the concurrent OVERWRITE deletes a DIFFERENT (non-referenced)
    /// data file. The referenced file `f` is untouched, so the row delta must COMMIT.
    ///
    /// Risk pinned: an over-eager check that rejects ANY concurrent deletion (ignoring the referenced set)
    /// would break legitimate concurrent removals of unrelated files. This is the test that makes
    /// `referencedDataFiles` (not "any concurrent deletion") the load-bearing gate.
    #[tokio::test]
    async fn test_row_delta_files_exist_allows_concurrent_deletion_of_different_file() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        // Two data files: f (referenced) and other (will be concurrently deleted).
        let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![
            synthetic_data_file("test/f.parquet", 0),
            synthetic_data_file("test/other.parquet", 0),
        ])
        .await;

        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/f-pos-del.parquet", 0)])
            .validate_from_snapshot(s0)
            .validate_data_files_exist(["test/f.parquet"]);
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): an OVERWRITE that DELETES the NON-referenced `other` (and adds a sibling).
        let _concurrent = commit_concurrent_overwrite_deletion(
            &catalog,
            &table,
            "test/other.parquet",
            "test/g.parquet",
        )
        .await;

        let table = tx
            .commit(&catalog)
            .await
            .expect("the row delta must commit: the concurrently-deleted file is not referenced");

        // The referenced f still exists; the row delta committed (its DELETE manifest landed).
        let live = live_data_file_paths(&table).await;
        assert!(
            live.contains("test/f.parquet"),
            "the referenced file f survives the concurrent deletion of a DIFFERENT file"
        );
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert!(
            manifest_list
                .entries()
                .iter()
                .any(|m| m.content == ManifestContentType::Deletes),
            "the row delta committed: a DELETE manifest is present"
        );
    }

    /// FLAG-OFF CONTROL. With NO `validate_data_files_exist(...)` call (the referenced set is EMPTY), a
    /// concurrent deletion of the data file the position delete references does NOT fail the commit — the
    /// files-exist check is OPT-IN (snapshot isolation, the DEFAULT, unchanged by this increment).
    ///
    /// Risk pinned: the files-exist check must be enabled ONLY by a non-empty referenced set (Java's
    /// `if (!referencedDataFiles.isEmpty())` guard) — running it for every row delta would change behavior.
    #[tokio::test]
    async fn test_row_delta_files_exist_without_referenced_set_does_not_check() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/f.parquet",
            0,
        )])
        .await;

        // Build a row delta WITHOUT calling validate_data_files_exist (empty referenced set).
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/f-pos-del.parquet", 0)]);
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): an OVERWRITE that DELETES f — would be a conflict IF the check were enabled.
        let _concurrent = commit_concurrent_overwrite_deletion(
            &catalog,
            &table,
            "test/f.parquet",
            "test/g.parquet",
        )
        .await;

        // With no referenced set, the row delta COMMITS (default behavior unchanged).
        let table = tx.commit(&catalog).await.expect(
            "with an empty referenced set, a concurrent deletion must not block the commit",
        );
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert!(
            manifest_list
                .entries()
                .iter()
                .any(|m| m.content == ManifestContentType::Deletes),
            "the row delta committed (no files-exist check ran)"
        );
    }

    /// THE SKIP-DELETES DEFAULT TEST. A CONCURRENT DELETE-OP (`delete_files`) deletion of the referenced file
    /// `f`. By DEFAULT (`validate_deleted_files()` NOT called ⇒ `skip_deletes = true` ⇒ `{OVERWRITE}` op set),
    /// the DELETE-op snapshot is EXCLUDED ⇒ the row delta COMMITS. WITH `validate_deleted_files()`
    /// (`skip_deletes = false` ⇒ `{OVERWRITE, DELETE}`) the same DELETE-op removal IS a conflict ⇒ REJECTED.
    ///
    /// Risk pinned: the skip-deletes DEFAULT — Java `BaseRowDelta` passes `skipDeletes = !validateDeletes`,
    /// and `validateDeletes` is `false` by default. Getting the default wrong (always including DELETE-op
    /// snapshots) would reject a legitimate concurrent merge-on-read delete the default is meant to tolerate.
    /// This is the ONLY test that distinguishes the two op sets, so it pins the `skip_deletes = !flag` default.
    #[tokio::test]
    async fn test_row_delta_files_exist_skip_deletes_default_excludes_delete_op_snapshot() {
        // --- Half A: DEFAULT (no validate_deleted_files) ⇒ a DELETE-op deletion is EXCLUDED ⇒ COMMITS. ---
        {
            let catalog = new_memory_catalog().await;
            let table = make_v2_minimal_table_in_catalog(&catalog).await;
            let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![synthetic_data_file(
                "test/f.parquet",
                0,
            )])
            .await;

            let tx = Transaction::new(&table);
            let action = tx
                .row_delta()
                .add_deletes(vec![synthetic_delete_file("test/f-pos-del.parquet", 0)])
                .validate_from_snapshot(s0)
                .validate_data_files_exist(["test/f.parquet"]);
            let tx = action.apply(tx).unwrap();

            // CONCURRENT commit (S1): a DELETE-op deletion of the referenced f.
            let _concurrent =
                commit_concurrent_delete_op_deletion(&catalog, &table, "test/f.parquet").await;

            // DEFAULT skip_deletes = true ⇒ the DELETE-op snapshot is excluded ⇒ the row delta COMMITS.
            let table = tx.commit(&catalog).await.expect(
                "by default a concurrent DELETE-op deletion is excluded (skip_deletes) ⇒ commit succeeds",
            );
            let snapshot = table.metadata().current_snapshot().unwrap();
            let manifest_list = snapshot
                .load_manifest_list(table.file_io(), table.metadata())
                .await
                .unwrap();
            assert!(
                manifest_list
                    .entries()
                    .iter()
                    .any(|m| m.content == ManifestContentType::Deletes),
                "the row delta committed under the skip-deletes default"
            );
        }

        // --- Half B: WITH validate_deleted_files ⇒ the SAME DELETE-op deletion IS a conflict ⇒ REJECTED. ---
        {
            let catalog = new_memory_catalog().await;
            let table = make_v2_minimal_table_in_catalog(&catalog).await;
            let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![synthetic_data_file(
                "test/f.parquet",
                0,
            )])
            .await;

            let tx = Transaction::new(&table);
            let action = tx
                .row_delta()
                .add_deletes(vec![synthetic_delete_file("test/f-pos-del.parquet", 0)])
                .validate_from_snapshot(s0)
                .validate_data_files_exist(["test/f.parquet"])
                .validate_deleted_files();
            let tx = action.apply(tx).unwrap();

            let _concurrent =
                commit_concurrent_delete_op_deletion(&catalog, &table, "test/f.parquet").await;

            let err = tx.commit(&catalog).await.expect_err(
                "validate_deleted_files() includes DELETE-op snapshots ⇒ the deletion of f is a conflict",
            );
            assert_eq!(err.kind(), ErrorKind::DataInvalid);
            assert!(!err.retryable());
            assert!(
                err.message().contains("Cannot commit, missing data files")
                    && err.message().contains("test/f.parquet"),
                "the error must name the missing referenced file, got: {}",
                err.message()
            );
        }
    }

    /// TX-CAPTURED START SURVIVES RE-BASE. The files-exist check works WITHOUT an explicit
    /// `validate_from_snapshot`, relying SOLELY on the transaction-captured starting snapshot id surviving
    /// `do_commit`'s re-base. Build `row_delta().validate_data_files_exist([f])` (NO `validate_from_snapshot`);
    /// the start is the `Transaction::new` head (S0). `do_commit` overwrites `self.table` with the refreshed
    /// base (S1), but `starting_snapshot_id` must SURVIVE — so S1's OVERWRITE deletion of `f` is still
    /// enumerated and rejected.
    ///
    /// Risk pinned: if `effective_start` were re-read from the REFRESHED head at validation time, start ==
    /// current head ⇒ the concurrently-deleted set is empty ⇒ the check silently always passes. Every OTHER
    /// files-exist test pins `validate_from_snapshot`, which short-circuits
    /// `validate_from_snapshot.or(starting_snapshot_id)` and never reads the tx-captured field — so this is
    /// the only one that pins the `Transaction::new` capture surviving the re-base.
    #[tokio::test]
    async fn test_row_delta_files_exist_rejects_concurrent_using_tx_captured_starting_snapshot() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/f.parquet",
            0,
        )])
        .await;

        // Build the row delta with the check enabled but WITHOUT validate_from_snapshot — the start is the
        // tx-captured head (S0).
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/f-pos-del.parquet", 0)])
            .validate_data_files_exist(["test/f.parquet"]);
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): an OVERWRITE deletion of the referenced f.
        let _concurrent = commit_concurrent_overwrite_deletion(
            &catalog,
            &table,
            "test/f.parquet",
            "test/g.parquet",
        )
        .await;

        let err = tx.commit(&catalog).await.expect_err(
            "the missing referenced file must be detected via the tx-captured starting snapshot",
        );
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(!err.retryable());
        assert!(
            err.message().contains("Cannot commit, missing data files")
                && err.message().contains("test/f.parquet"),
            "got: {}",
            err.message()
        );
    }

    // ============================================================================================
    // RowDelta `validateNoNewDeletesForDataFiles` on REMOVED data files (Java `BaseRowDelta.validate`
    // L161-164 → the `!removedDataFiles.isEmpty()` sub-branch of `validateNewDeleteFiles` →
    // `MergingSnapshotProducer.validateNoNewDeletesForDataFiles` L519-551). The DATA files this row delta
    // REMOVES (via `remove_data_files`/`remove_rows`, Java `removeRows`) must not have had a NEW applicable
    // delete added concurrently — you cannot drop a data file out from under a concurrent row-level delete.
    //
    // This rides the SAME `validate_no_conflicting_delete_files()` flag (Java's single `validateNewDeleteFiles`
    // gate) as the filter-based delete check, and delegates to the SAME shared helper
    // (`validate_no_new_deletes_for_data_files`) `OverwriteFiles.validateNoConflictingDeletes` uses, with
    // `ignore_equality_deletes = false` (RowDelta's non-rewrite path counts ALL applicable deletes).
    //
    // The race: a `row_delta` is BUILT against head S0 removing data file A (via `remove_data_files`, full
    // metadata). BEFORE it commits, a concurrent `row_delta().add_deletes(...)` lands a POSITION delete
    // (seq > start) in A's partition. With the delete check enabled the row delta must FAIL (non-retryable).
    // A delete in a DIFFERENT partition, a delete at seq <= start, the flag OFF, no removed files, or a V1
    // table must all COMMIT.
    // ============================================================================================

    /// A synthetic EQUALITY-delete file routed to partition `x = part_value` (spec id 0), equality on field
    /// id 1 (`x`), with a unique path — manifest-only (not a real parquet file). Used to prove the RowDelta
    /// removed-data-files check counts EQUALITY deletes (`ignore_equality_deletes = false`).
    fn synthetic_equality_delete_file(path: &str, part_value: i64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .equality_ids(Some(vec![1]))
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(part_value))]))
            .build()
            .unwrap()
    }

    /// NO CONCURRENT DELETE. With the delete check enabled and a removed data file A, but nothing landing
    /// concurrently, the row delta commits normally (the concurrent-added delete set is empty ⇒ no conflict).
    /// Pins that enabling the check + removing a file does not block a race-free commit.
    #[tokio::test]
    async fn test_row_delta_removed_data_files_no_concurrent_delete_succeeds() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let a = synthetic_data_file("test/a.parquet", 0);
        let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![a.clone()]).await;

        // Row delta REMOVES A (validation-only) and adds a delete, delete check enabled — no concurrent delete.
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/a-pos-del.parquet", 0)])
            .remove_data_files(vec![a])
            .validate_from_snapshot(s0)
            .validate_no_conflicting_delete_files();
        let tx = action.apply(tx).unwrap();
        let table = tx
            .commit(&catalog)
            .await
            .expect("a race-free row delta removing a data file must commit");

        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert!(
            manifest_list
                .entries()
                .iter()
                .any(|m| m.content == ManifestContentType::Deletes),
            "the row delta committed: a DELETE manifest is present"
        );
    }

    /// THE HEADLINE TEST. Append A. Build a `row_delta` that REMOVES A via `remove_data_files` (full metadata)
    /// with `.validate_no_conflicting_delete_files()`. Then a CONCURRENT `row_delta().add_deletes(...)` lands a
    /// POSITION delete in A's partition (x=0, seq > start). The row-delta commit must FAIL with a NON-retryable
    /// `DataInvalid` naming A (Java "Cannot commit, found new delete for replaced data file: <path>").
    ///
    /// Risk pinned: dropping A out from under a concurrent row-level delete = a lost delete under serializable
    /// isolation. This is the `validateNoNewDeletesForDataFiles` sub-branch of `validateNewDeleteFiles`.
    #[tokio::test]
    async fn test_row_delta_removed_data_files_rejects_concurrent_delete() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let a = synthetic_data_file("test/a.parquet", 0);
        let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![a.clone()]).await;

        // Build the row delta removing A via remove_data_files (full metadata ⇒ validated), delete check on,
        // pinned to S0.
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/a-pos-del.parquet", 0)])
            .remove_data_files(vec![a])
            .validate_from_snapshot(s0)
            .validate_no_conflicting_delete_files();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a position delete in A's partition (x=0), seq > start.
        let _concurrent = commit_concurrent_deletes(&catalog, &table, vec![synthetic_delete_file(
            "test/pos-del.parquet",
            0,
        )])
        .await;

        let err = tx.commit(&catalog).await.expect_err(
            "row delta must fail: a concurrent delete applies to the removed data file A",
        );
        assert_eq!(
            err.kind(),
            ErrorKind::DataInvalid,
            "a conflict is a non-retryable validation failure (DataInvalid)"
        );
        assert!(
            !err.retryable(),
            "the validation failure must be NON-retryable so the retry loop stops"
        );
        assert!(
            err.message()
                .contains("found new delete for replaced data file"),
            "the error must match Java's message, got: {}",
            err.message()
        );
        assert!(
            err.message().contains("test/a.parquet"),
            "the error must name the removed data file, got: {}",
            err.message()
        );
    }

    /// NO-FALSE-CONFLICT (different partition). Same setup, but the concurrent position delete is in a
    /// DIFFERENT partition (x=1) than the removed file A (x=0) — so the removed-data-files sub-check (2a, which
    /// matches by partition) does NOT apply it to A. The delete carries `y` bounds `[10,20]` ENTIRELY BELOW the
    /// conflict filter `y >= 50` so the partition-blind filter-based sub-check (2b) ALSO excludes it — leaving
    /// the row delta free to COMMIT. Risk pinned: a partition-blind 2a check that rejects ANY concurrent delete
    /// (a false positive). (The `y` bounds are needed only to keep 2b — which runs on the same flag — quiet, so
    /// the partition logic of 2a is the load-bearing test.)
    #[tokio::test]
    async fn test_row_delta_removed_data_files_allows_concurrent_delete_in_other_partition() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let a = synthetic_data_file("test/a.parquet", 0);
        let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![a.clone()]).await;

        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/a-pos-del.parquet", 0)])
            .remove_data_files(vec![a])
            .conflict_detection_filter(
                Reference::new("y").greater_than_or_equal_to(Datum::long(50)),
            )
            .validate_from_snapshot(s0)
            .validate_no_conflicting_delete_files();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a position delete in a DIFFERENT partition (x=1) with y bounds [10,20] below
        // the filter — cannot apply to A (x=0) under 2a, and excluded by 2b's metrics.
        let _concurrent =
            commit_concurrent_deletes(&catalog, &table, vec![delete_file_with_y_bounds(
                "test/pos-del-other.parquet",
                1,
                10,
                20,
            )])
            .await;

        let table = tx
            .commit(&catalog)
            .await
            .expect("row delta must commit: the concurrent delete is in a different partition");
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert!(
            manifest_list
                .entries()
                .iter()
                .any(|m| m.content == ManifestContentType::Deletes),
            "the row delta committed: a DELETE manifest is present"
        );
    }

    /// NO-CONFLICT (delete at/before the start). The concurrent delete lands BEFORE the validation window;
    /// here we set `validate_from_snapshot` to the CURRENT head (after the delete already landed) so the delete
    /// is NOT in the concurrent window — the row delta must COMMIT. Risk pinned: a check that ignores the
    /// starting-snapshot boundary and flags a pre-start delete.
    #[tokio::test]
    async fn test_row_delta_removed_data_files_allows_delete_at_or_before_start() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let a = synthetic_data_file("test/a.parquet", 0);
        let table = append_files(&catalog, &table, vec![a.clone()]).await;

        // A delete lands in A's partition BEFORE the transaction's validation window (it becomes the head S1).
        let table = commit_concurrent_deletes(&catalog, &table, vec![synthetic_delete_file(
            "test/pos-del.parquet",
            0,
        )])
        .await;
        let s1 = table.metadata().current_snapshot().unwrap().snapshot_id();

        // Build the row delta pinned to S1 (the current head) — the pre-existing delete is NOT concurrent.
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/a-pos-del.parquet", 0)])
            .remove_data_files(vec![a])
            .validate_from_snapshot(s1)
            .validate_no_conflicting_delete_files();
        let tx = action.apply(tx).unwrap();
        let table = tx
            .commit(&catalog)
            .await
            .expect("with start = current head, the pre-existing delete is not concurrent ⇒ commit succeeds");
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert!(
            manifest_list
                .entries()
                .iter()
                .any(|m| m.content == ManifestContentType::Deletes),
            "the row delta committed: a DELETE manifest is present"
        );
    }

    /// EQUALITY-DELETE IS COUNTED (RowDelta passes `ignore_equality_deletes = false`). An EQUALITY delete in
    /// A's partition added concurrently IS a conflict for the removed file A (Java's else-branch counts ANY
    /// applicable delete). Pins that RowDelta does NOT silently ignore equality deletes here — distinguishing
    /// it from the rewrite path (which would ignore them).
    #[tokio::test]
    async fn test_row_delta_removed_data_files_rejects_concurrent_equality_delete() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let a = synthetic_data_file("test/a.parquet", 0);
        let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![a.clone()]).await;

        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/a-pos-del.parquet", 0)])
            .remove_data_files(vec![a])
            .validate_from_snapshot(s0)
            .validate_no_conflicting_delete_files();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): an EQUALITY delete in A's partition (x=0).
        let _concurrent =
            commit_concurrent_deletes(&catalog, &table, vec![synthetic_equality_delete_file(
                "test/eq-del.parquet",
                0,
            )])
            .await;

        let err = tx.commit(&catalog).await.expect_err(
            "row delta must fail: an equality delete applies to the removed data file A",
        );
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(!err.retryable());
        assert!(
            err.message()
                .contains("found new delete for replaced data file"),
            "got: {}",
            err.message()
        );
        assert!(err.message().contains("test/a.parquet"));
    }

    /// FLAG-OFF CONTROL. With `validate_no_conflicting_delete_files()` NOT called, a concurrent delete applying
    /// to the removed file does NOT fail the commit — snapshot isolation, the DEFAULT, unchanged. Risk pinned:
    /// the check must be OPT-IN. Also proves `remove_data_files` is inert without the gating flag.
    #[tokio::test]
    async fn test_row_delta_removed_data_files_without_validation_allows_conflicting_delete() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let a = synthetic_data_file("test/a.parquet", 0);
        let table = append_files(&catalog, &table, vec![a.clone()]).await;

        // Build the row delta REMOVING A but WITHOUT enabling the delete check (default = snapshot isolation).
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/a-pos-del.parquet", 0)])
            .remove_data_files(vec![a]);
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a position delete applying to A.
        let _concurrent = commit_concurrent_deletes(&catalog, &table, vec![synthetic_delete_file(
            "test/pos-del.parquet",
            0,
        )])
        .await;

        let table = tx.commit(&catalog).await.expect(
            "with the delete check OFF, a conflicting concurrent delete must not block the commit",
        );
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert!(
            manifest_list
                .entries()
                .iter()
                .any(|m| m.content == ManifestContentType::Deletes),
            "the row delta committed (snapshot isolation, no conflicting-delete check)"
        );
    }

    /// NO REMOVED FILES ⇒ THE REMOVED-DATA-FILES CHECK DOES NOT RUN. With the delete check enabled but NO
    /// `remove_data_files` call, the removed-data-files sub-check (2a) is skipped outright (Java's
    /// `!removedDataFiles.isEmpty()` guard). The concurrent delete here carries `y` bounds `[10,20]` ENTIRELY
    /// BELOW the conflict filter `y >= 50`, so the filter-based 2b check also excludes it — neither delete
    /// sub-check fires and the row delta COMMITS. Risk pinned: running the removed-data-files check on an empty
    /// removed set (an over-reject beyond Java's guard) — were 2a wrongly run on the empty set with AlwaysTrue
    /// semantics it could spuriously reject.
    #[tokio::test]
    async fn test_row_delta_no_removed_data_files_skips_removed_check() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        // Delete check enabled, conflict filter `y >= 50`, but NO remove_data_files.
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/a-pos-del.parquet", 0)])
            .conflict_detection_filter(
                Reference::new("y").greater_than_or_equal_to(Datum::long(50)),
            )
            .validate_from_snapshot(s0)
            .validate_no_conflicting_delete_files();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a position delete in A's partition (x=0) with y bounds [10,20] BELOW the
        // filter — excluded by 2b's metrics. (The removed-data-files 2a check is skipped because no files were
        // removed; were it run on the empty set, the per-file loop has nothing to flag anyway — this pins that
        // an empty removed set is a clean skip, not a spurious reject.)
        let _concurrent =
            commit_concurrent_deletes(&catalog, &table, vec![delete_file_with_y_bounds(
                "test/pos-del-other.parquet",
                0,
                10,
                20,
            )])
            .await;

        let table = tx.commit(&catalog).await.expect(
            "with no removed data files, the removed-data-files sub-check is skipped ⇒ commit succeeds",
        );
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert!(
            manifest_list
                .entries()
                .iter()
                .any(|m| m.content == ManifestContentType::Deletes),
            "the row delta committed: a DELETE manifest is present"
        );
    }

    /// V2-GUARD. On a V1 table the removed-data-files check is a guarded no-op (Java
    /// `validateNoNewDeletesForDataFiles`: `base.formatVersion() < 2` ⇒ no delete files exist). With the
    /// delete check enabled and a removed data file, a concurrent commit lands and the row delta still
    /// COMMITS. The row delta removes + adds DATA only (V1-legal). Risk pinned: the removed-data-files check
    /// walking a V1 table where delete manifests cannot exist.
    #[tokio::test]
    async fn test_row_delta_removed_data_files_check_is_noop_on_v1_table() {
        let catalog = new_memory_catalog().await;
        let table = make_v1_minimal_table_in_catalog(&catalog).await;
        assert_eq!(
            table.metadata().format_version(),
            crate::spec::FormatVersion::V1,
            "the table must be V1 for the guard to be under test"
        );
        let a = synthetic_data_file("test/a.parquet", 0);
        let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![a.clone()]).await;

        // Row delta REMOVES A (validation-only) + adds DATA only (V1 has no delete files), delete check on.
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_data_files(vec![synthetic_data_file("test/b.parquet", 0)])
            .remove_data_files(vec![a])
            .validate_from_snapshot(s0)
            .validate_no_conflicting_delete_files();
        let tx = action.apply(tx).unwrap();

        // A concurrent DATA append lands (V1 can't add delete files). The V2 guard makes the removed-data-files
        // check a no-op, so the row delta commits regardless.
        let _concurrent = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/concurrent.parquet",
            0,
        )])
        .await;

        let table = tx.commit(&catalog).await.expect(
            "the removed-data-files check is a no-op on a V1 table (V2 guard) — the row delta commits",
        );
        assert!(
            live_data_file_paths(&table)
                .await
                .contains("test/b.parquet"),
            "the row delta's added data file landed on V1 (removed-data-files check no-op)"
        );
    }

    /// TX-CAPTURED START PIN (the recurring gap). The removed-data-files check works WITHOUT an explicit
    /// `validate_from_snapshot`, relying SOLELY on the transaction-captured starting snapshot surviving
    /// `do_commit`'s re-base. The action calls ONLY `.remove_data_files([A])` +
    /// `.validate_no_conflicting_delete_files()`. The start is the one captured in `Transaction::new` (= S0);
    /// `do_commit` overwrites `self.table` with the refreshed base (S1, the concurrent delete), but
    /// `starting_snapshot_id` must SURVIVE — so the concurrent delete is still enumerated and the commit
    /// rejected.
    ///
    /// Risk pinned: if the start were re-read from the refreshed head at validation time, start == current
    /// head ⇒ the concurrent set is empty ⇒ the check silently always passes (a serializable-isolation hole).
    /// Every OTHER removed-data-files test pins `validate_from_snapshot`, so this is the only one that pins the
    /// `Transaction::new` capture surviving the re-base for this sub-check.
    #[tokio::test]
    async fn test_row_delta_removed_data_files_rejects_using_tx_captured_starting_snapshot() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let a = synthetic_data_file("test/a.parquet", 0);
        let table = append_files(&catalog, &table, vec![a.clone()]).await;

        // Build the row delta with the delete check enabled but WITHOUT validate_from_snapshot — the start is
        // the tx-captured head (S0).
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/a-pos-del.parquet", 0)])
            .remove_data_files(vec![a])
            .validate_no_conflicting_delete_files();
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a position delete applying to A.
        let _concurrent = commit_concurrent_deletes(&catalog, &table, vec![synthetic_delete_file(
            "test/pos-del.parquet",
            0,
        )])
        .await;

        let err = tx
            .commit(&catalog)
            .await
            .expect_err("conflict must be detected via the tx-captured starting snapshot");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(!err.retryable());
        assert!(
            err.message()
                .contains("found new delete for replaced data file"),
            "got: {}",
            err.message()
        );
        assert!(err.message().contains("test/a.parquet"));
    }

    // ============================================================================================
    // RowDelta `validateAddedDVs` — the V3 deletion-vector conflict check (Java `BaseRowDelta.validate` L172
    // → `MergingSnapshotProducer.validateAddedDVs` L825-895). ALWAYS-ON (called UNCONDITIONALLY, NOT behind
    // any opt-in flag) but SELF-SKIPS when this row delta adds no DVs (Java L831 `dvsByReferencedFile
    // .isEmpty()`).
    //
    // A deletion vector (DV) is a delete file whose `file_format() == DataFileFormat::Puffin` (Java
    // `ContentFileUtil.isDV` = `format() == FileFormat.PUFFIN`); its `referenced_data_file` is the data file it
    // covers. A row delta adding a DV for data file A must be rejected if a concurrent commit since the start
    // ALSO added a DV for A — two DVs for one data file is a write-write conflict. The concurrent walk is
    // `added_dv_candidate_delete_files_after`, gated to Java's `VALIDATE_ADDED_DVS_OPERATIONS = {OVERWRITE,
    // DELETE, REPLACE}` (1.10.0-bytecode-verified) — REPLACE included: `Operation::Replace` is representable
    // (the rewrite actions record it) and a compaction can rewrite DVs.
    //
    // The race: a `row_delta` adding a DV for A is BUILT against head S0; BEFORE it commits a concurrent
    // `row_delta().add_deletes([DV for A])` lands (S1). On commit `do_commit` refreshes to S1 and runs
    // `validate` against that base; the concurrent DV for the SAME A collides ⇒ non-retryable rejection.
    // ============================================================================================

    /// Commit a CONCURRENT row delta that ADDS the given DVs (Puffin delete files, no data) in its own
    /// snapshot via the catalog. The resulting snapshot's operation is `Delete` (add-deletes-only), which is in
    /// the DV op set `VALIDATE_ADDED_DVS_OPERATIONS = {OVERWRITE, DELETE, REPLACE}` so the DV walk enumerates
    /// it (the REPLACE member is exercised separately by the hand-built `ReplaceOpAddDvAction` test). Mirrors
    /// `commit_concurrent_deletes` but for DVs.
    async fn commit_concurrent_dvs(
        catalog: &impl Catalog,
        table: &Table,
        dv_files: Vec<DataFile>,
    ) -> Table {
        let tx = Transaction::new(table);
        let action = tx.row_delta().add_deletes(dv_files);
        let tx = action.apply(tx).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    /// THE HEADLINE DV TEST. Append S0 with data file A. Build a `row_delta` that adds a DV for A. Then a
    /// CONCURRENT `row_delta` lands a DV for the SAME A (S1). The row-delta commit must FAIL with a
    /// NON-retryable `DataInvalid` whose message names A as the concurrently-added DV's referenced file. The DV
    /// check is ALWAYS-ON (no flag enables it) — this is the only RowDelta check that fires without opt-in.
    ///
    /// Risk pinned: two DVs for one data file is a write-write conflict (the second DV would silently shadow or
    /// lose the first under serializable isolation). Without `validateAddedDVs` the row delta would commit a
    /// second DV for A blind to the concurrent one.
    #[tokio::test]
    async fn test_row_delta_rejects_concurrent_dv_for_same_referenced_file() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        // Row delta adds a DV for A — NO conflict flag is set (the DV check is always-on). Pin to S0.
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_dv_file(
                "test/a-dv.puffin",
                0,
                "test/a.parquet",
            )])
            .validate_from_snapshot(s0);
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a row delta adding a DV for the SAME A.
        let _concurrent = commit_concurrent_dvs(&catalog, &table, vec![synthetic_dv_file(
            "test/a-dv-concurrent.puffin",
            0,
            "test/a.parquet",
        )])
        .await;

        let err = tx.commit(&catalog).await.expect_err(
            "row delta must fail: a concurrent DV was added for the same referenced data file",
        );

        assert_eq!(
            err.kind(),
            ErrorKind::DataInvalid,
            "a DV conflict is a non-retryable validation failure (DataInvalid)"
        );
        assert!(
            !err.retryable(),
            "the validation failure must be NON-retryable so the retry loop stops and it propagates"
        );
        assert!(
            err.message().contains("Found concurrently added DV for"),
            "the error must use the DV-specific message, got: {}",
            err.message()
        );
        assert!(
            err.message().contains("test/a.parquet"),
            "the error must name the referenced data file, got: {}",
            err.message()
        );
        assert!(
            err.message().contains("test/a-dv-concurrent.puffin"),
            "the error must describe the concurrently-added DV, got: {}",
            err.message()
        );
    }

    /// NO-FALSE-CONFLICT DV TEST. The concurrent commit adds a DV for a DIFFERENT data file (B) than the one
    /// this row delta's DV references (A). No referenced file collides, so the row delta must COMMIT.
    ///
    /// Risk pinned: an over-eager DV check that rejects ANY concurrently-added DV (ignoring the
    /// referenced-file key) would break legitimate concurrent DV writes on unrelated data files. This makes the
    /// `referenced_data_file` collision (not "any concurrent DV") the load-bearing gate.
    #[tokio::test]
    async fn test_row_delta_allows_concurrent_dv_for_different_referenced_file() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![
            synthetic_data_file("test/a.parquet", 0),
            synthetic_data_file("test/b.parquet", 0),
        ])
        .await;

        // Row delta adds a DV for A (always-on DV check), pinned to S0.
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_dv_file(
                "test/a-dv.puffin",
                0,
                "test/a.parquet",
            )])
            .validate_from_snapshot(s0);
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a DV for the DIFFERENT data file B — no referenced-file collision with A.
        let _concurrent = commit_concurrent_dvs(&catalog, &table, vec![synthetic_dv_file(
            "test/b-dv-concurrent.puffin",
            0,
            "test/b.parquet",
        )])
        .await;

        let table = tx
            .commit(&catalog)
            .await
            .expect("row delta must commit: the concurrent DV references a different data file");

        // The row delta committed: a DELETE manifest landed (its own DV).
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert!(
            manifest_list
                .entries()
                .iter()
                .any(|m| m.content == ManifestContentType::Deletes),
            "the row delta committed: a DELETE manifest is present"
        );
    }

    /// NO CONCURRENT DV. A row delta adds a DV for A with NO concurrent DV landing — it commits normally (the
    /// concurrently-added DV set is empty ⇒ no conflict). Pins that the always-on DV check does not block a
    /// race-free DV commit.
    #[tokio::test]
    async fn test_row_delta_dv_no_concurrent_commit_succeeds() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_dv_file(
                "test/a-dv.puffin",
                0,
                "test/a.parquet",
            )])
            .validate_from_snapshot(s0);
        let tx = action.apply(tx).unwrap();
        let table = tx
            .commit(&catalog)
            .await
            .expect("a race-free DV row delta must commit (no concurrent DV ⇒ no conflict)");

        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert!(
            manifest_list
                .entries()
                .iter()
                .any(|m| m.content == ManifestContentType::Deletes),
            "the row delta committed: a DELETE manifest is present"
        );
    }

    /// THE NON-DV NO-OP TEST (the always-on/self-skip semantics). A row delta adding ONLY NON-DV deletes (an
    /// equality delete here — the non-DV content V3's format gate still admits) commits even when a concurrent
    /// DV is present — because this row delta adds NO DV, the always-on `validateAddedDVs` SELF-SKIPS (Java
    /// L831 `newDVRefs.isEmpty()`), leaving nothing to conflict. This is the load-bearing
    /// behavior-preservation pin: the pre-DV RowDelta tests all add non-Puffin deletes, so the DV check is a
    /// no-op for every one of them.
    ///
    /// Risk pinned: the DV check firing on a non-DV row delta (over-rejecting the common merge-on-read case),
    /// or — worse — the always-on check not actually self-skipping (which would change every existing test).
    #[tokio::test]
    async fn test_row_delta_non_dv_delete_is_noop_even_with_concurrent_dv() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        // Row delta adds a NON-DV delete — no DV ⇒ the DV check self-skips. On this V3 table the
        // non-DV delete must be an EQUALITY delete (exempt from the V3-requires-DVs format gate at
        // every version; a parquet POSITION delete would now be rejected by the gate before the DV
        // check is ever reached — D3 migration note).
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_equality_delete_file(
                "test/a-eq-del.parquet",
                0,
            )])
            .validate_from_snapshot(s0);
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a DV for the SAME A. It would collide IF this row delta added a DV — but it
        // does not, so the always-on DV check self-skips and the commit succeeds.
        let _concurrent = commit_concurrent_dvs(&catalog, &table, vec![synthetic_dv_file(
            "test/a-dv-concurrent.puffin",
            0,
            "test/a.parquet",
        )])
        .await;

        let table = tx.commit(&catalog).await.expect(
            "a non-DV row delta adds no DV ⇒ the always-on DV check self-skips ⇒ commit succeeds even with a concurrent DV",
        );

        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert!(
            manifest_list
                .entries()
                .iter()
                .any(|m| m.content == ManifestContentType::Deletes),
            "the non-DV row delta committed: a DELETE manifest is present"
        );
    }

    /// TX-CAPTURED START PIN (no `validate_from_snapshot`). The DV check works WITHOUT an explicit
    /// `validate_from_snapshot`, relying SOLELY on the transaction-captured starting snapshot id surviving
    /// `do_commit`'s re-base. The action adds ONLY a DV for A (no `validate_from_snapshot`); the start is the
    /// `Transaction::new` head (S0). `do_commit` overwrites `self.table` with the refreshed base (S1, the
    /// concurrent DV), but `starting_snapshot_id` must SURVIVE — so the concurrent DV for A is still enumerated
    /// and the commit rejected.
    ///
    /// Risk pinned: if `effective_start` were re-read from the REFRESHED head at validation time, start ==
    /// current head ⇒ the concurrently-added DV set is empty ⇒ the check silently always passes. Every other DV
    /// test pins `validate_from_snapshot`, so this is the only one that pins the tx capture surviving the
    /// re-base for the always-on DV check.
    #[tokio::test]
    async fn test_row_delta_dv_rejects_using_tx_captured_starting_snapshot() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        // Build the DV row delta WITHOUT validate_from_snapshot — the start is the tx-captured head (S0).
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![synthetic_dv_file(
            "test/a-dv.puffin",
            0,
            "test/a.parquet",
        )]);
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit (S1): a DV for the SAME A.
        let _concurrent = commit_concurrent_dvs(&catalog, &table, vec![synthetic_dv_file(
            "test/a-dv-concurrent.puffin",
            0,
            "test/a.parquet",
        )])
        .await;

        let err = tx
            .commit(&catalog)
            .await
            .expect_err("the DV conflict must be detected via the tx-captured starting snapshot");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(!err.retryable());
        assert!(
            err.message().contains("Found concurrently added DV for")
                && err.message().contains("test/a.parquet"),
            "got: {}",
            err.message()
        );
    }

    /// MALFORMED DV REJECTED. A Puffin-format delete file with NO `referenced_data_file` is malformed (a DV
    /// MUST set it — Java dereferences `file.referencedDataFile()` as a non-null `dvsByReferencedFile` key).
    /// Adding such a file and validating must error with a clear DataInvalid (not panic, not silently skip).
    ///
    /// Risk pinned: a DV missing its referenced data file slipping through (it would corrupt the DV index — a
    /// DV that covers no data file). The check is reached via the always-on `validate` step, so no flag is set.
    #[tokio::test]
    async fn test_row_delta_dv_missing_referenced_data_file_is_rejected() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        // A Puffin delete file with NO referenced_data_file ⇒ malformed DV.
        let malformed_dv = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/bad-dv.puffin".to_string())
            .file_format(DataFileFormat::Puffin)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(0))]))
            .content_offset(Some(4))
            .content_size_in_bytes(Some(40))
            .build()
            .unwrap();

        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![malformed_dv])
            .validate_from_snapshot(s0);
        let tx = action.apply(tx).unwrap();
        let err = tx
            .commit(&catalog)
            .await
            .expect_err("a Puffin DV missing its referenced data file must be rejected");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message().contains("missing its referenced data file"),
            "got: {}",
            err.message()
        );
    }

    // ============================================================================================
    // D3: the format-version gate — Java `MergingSnapshotProducer.validateDeleteFileForVersion`
    // (1.10.0-bytecode-verified; the switch is inlined into `validateNewDeleteFile`). V1 throws,
    // V2 forbids DVs for position deletes, V3 REQUIRES DVs for position deletes; equality deletes
    // are exempt at every version. The Rust gate runs in `validate_added_delete_files` (the
    // action's commit against the REFRESHED base — MAIN's apply-time placement, which subsumes
    // 1.10.0's add-time placement).
    //
    // V2 + parquet position delete OK (the regression direction) is pinned by the whole migrated
    // V2 suite (e.g. `test_row_delta_position_deletes_drop_deleted_rows_from_scan`); V3 + DV OK is
    // pinned by `test_row_delta_dv_no_concurrent_commit_succeeds`.
    // ============================================================================================

    /// V2 REJECTS a deletion vector with Java's exact message ("Must not use DVs for position
    /// deletes in V2: %s" + `ContentFileUtil.dvDesc`). Risk pinned: a V2 table carrying a Puffin DV
    /// is unreadable by every V2 reader — the gate must fail the COMMIT, byte-exactly like Java.
    #[tokio::test]
    async fn test_row_delta_v2_rejects_deletion_vector_with_exact_java_message() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![synthetic_dv_file(
            "test/a-dv.puffin",
            0,
            "test/a.parquet",
        )]);
        let tx = action.apply(tx).unwrap();
        let err = tx
            .commit(&catalog)
            .await
            .expect_err("a deletion vector must be rejected on a V2 table");

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(!err.retryable(), "the format gate is non-retryable");
        assert_eq!(
            err.message(),
            "Must not use DVs for position deletes in V2: DV{location=test/a-dv.puffin, \
             offset=4, length=40, referencedDataFile=test/a.parquet}",
            "the V2 gate message must match Java byte-for-byte (incl. dvDesc)"
        );
    }

    /// V3 REJECTS a parquet position delete with Java's exact message ("Must use DVs for position
    /// deletes in V%s: %s" + the file location). Risk pinned: fresh parquet position deletes on a
    /// V3 table break the DV-supersedes-position-deletes read precedence — Java refuses them, so
    /// must Rust.
    #[tokio::test]
    async fn test_row_delta_v3_rejects_parquet_position_delete_with_exact_java_message() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/a-pos-del.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let err = tx
            .commit(&catalog)
            .await
            .expect_err("a parquet position delete must be rejected on a V3 table");

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(!err.retryable(), "the format gate is non-retryable");
        assert_eq!(
            err.message(),
            "Must use DVs for position deletes in V3: test/a-pos-del.parquet",
            "the V3 gate message must match Java byte-for-byte"
        );
    }

    /// V1 rejects EVERY added delete file — position AND equality — with Java's exact message
    /// ("Deletes are supported in V2 and above"). Unit-level on the producer (no in-catalog V1
    /// fixture exists; the gate is reached identically through `validate_added_delete_files`).
    /// Risk pinned: a V1 table must never grow a delete manifest — V1 manifests cannot even encode
    /// delete content.
    #[tokio::test]
    async fn test_v1_producer_rejects_all_added_delete_files() {
        use crate::transaction::tests::make_v1_table;

        let table = make_v1_table();
        for delete_file in [
            synthetic_delete_file("test/a-pos-del.parquet", 0),
            synthetic_equality_delete_file("test/a-eq-del.parquet", 0),
        ] {
            let producer =
                SnapshotProducer::new(&table, uuid::Uuid::now_v7(), None, HashMap::new(), vec![])
                    .with_added_delete_files(vec![delete_file]);
            let err = producer
                .validate_added_delete_files()
                .expect_err("a V1 table must reject every added delete file");
            assert_eq!(err.kind(), ErrorKind::DataInvalid);
            assert_eq!(
                err.message(),
                "Deletes are supported in V2 and above",
                "the V1 gate message must match Java byte-for-byte"
            );
        }
    }

    /// EQUALITY deletes are EXEMPT from the format gate at V2 AND V3 (both Java arms test
    /// `content() == EQUALITY_DELETES` first). Risk pinned: an over-broad V3 gate that demands DVs
    /// for equality deletes too (Puffin cannot carry equality deletes) would break every V3
    /// merge-on-read equality-delete commit.
    #[tokio::test]
    async fn test_equality_deletes_exempt_from_version_gate_on_v2_and_v3() {
        let catalog = new_memory_catalog().await;
        for v3 in [false, true] {
            let table = if v3 {
                make_v3_minimal_table_in_catalog(&catalog).await
            } else {
                make_v2_minimal_table_in_catalog(&catalog).await
            };
            let table = append_files(&catalog, &table, vec![synthetic_data_file(
                "test/a.parquet",
                0,
            )])
            .await;

            let tx = Transaction::new(&table);
            let action = tx
                .row_delta()
                .add_deletes(vec![synthetic_equality_delete_file(
                    "test/a-eq-del.parquet",
                    0,
                )]);
            let tx = action.apply(tx).unwrap();
            tx.commit(&catalog).await.unwrap_or_else(|err| {
                panic!(
                    "an equality delete must pass the format gate on a {} table, got: {err}",
                    if v3 { "V3" } else { "V2" }
                )
            });
        }
    }

    // ============================================================================================
    // D3: the fresh-DV-only door (Rust-conservative — see `validate_fresh_dvs_only`). Java MERGES
    // previous deletes into the new DV (`BaseDVFileWriter.loadPreviousDeletes` L117-126) and
    // removes the old delete file via `rewrittenDeleteFiles`/`removeDeletes`; that apply-side
    // removal is deferred, so a DV add for a data file with a LIVE position-scoped delete is
    // rejected fail-loud at commit instead of corrupting fail-late at scan.
    // The "fresh DV commits" direction is pinned by `test_row_delta_dv_no_concurrent_commit_
    // succeeds`.
    // ============================================================================================

    /// SECOND DV FOR THE SAME FILE REJECTED. DV1 for data file A is committed; a LATER transaction
    /// (started after DV1 — no concurrent window, so `validateAddedDVs` self-passes) adds DV2 for
    /// the same A. The door must reject: without it the commit would leave TWO live DVs for A,
    /// which D1's duplicate-DV load door rejects at SCAN time — fail-late, table unreadable.
    /// The message must name the referenced file AND the deferral.
    #[tokio::test]
    async fn test_row_delta_second_dv_for_same_file_rejected_until_merge_lands() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        // DV1 for A commits (the fresh direction).
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![synthetic_dv_file(
            "test/a-dv1.puffin",
            0,
            "test/a.parquet",
        )]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // DV2 for the SAME A, in a transaction started AFTER DV1 landed (pre-existing, NOT
        // concurrent — the door, not validateAddedDVs, is the only guard here).
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![synthetic_dv_file(
            "test/a-dv2.puffin",
            0,
            "test/a.parquet",
        )]);
        let tx = action.apply(tx).unwrap();
        let err = tx
            .commit(&catalog)
            .await
            .expect_err("a second DV for a data file with a live DV must be rejected");

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(!err.retryable());
        assert!(
            err.message()
                .contains("Cannot commit deletion vector for test/a.parquet"),
            "the door must name the referenced data file, got: {}",
            err.message()
        );
        assert!(
            err.message().contains("test/a-dv1.puffin"),
            "the door must describe the existing live DV, got: {}",
            err.message()
        );
        assert!(
            err.message().contains("deferred"),
            "the door must name the deferral (previous-delete merge not yet supported), got: {}",
            err.message()
        );
    }

    /// NEGATIVE CONTROL: a DV for a DIFFERENT data file commits even though ANOTHER file has a
    /// live DV. Risk pinned: an over-broad door keyed on "any live DV exists" (instead of the
    /// per-referenced-file collision) would freeze all DV writes after the first one.
    #[tokio::test]
    async fn test_row_delta_dv_for_different_file_commits_despite_existing_dv() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![
            synthetic_data_file("test/a.parquet", 0),
            synthetic_data_file("test/b.parquet", 0),
        ])
        .await;

        // DV1 for A commits.
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![synthetic_dv_file(
            "test/a-dv.puffin",
            0,
            "test/a.parquet",
        )]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // DV2 for B (no live position-scoped delete for B) must commit.
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![synthetic_dv_file(
            "test/b-dv.puffin",
            0,
            "test/b.parquet",
        )]);
        let tx = action.apply(tx).unwrap();
        tx.commit(&catalog)
            .await
            .expect("a DV for a different data file must commit — the door is per-file");
    }

    /// LEGACY-PARQUET SHADOW REJECTED (the V2→V3 upgrade scenario). A V2 table commits a
    /// partition-scoped PARQUET position delete (partition x=0), is upgraded to V3, and a DV for a
    /// data file in x=0 is then added. At read time a DV SUPERSEDES every parquet position delete
    /// for its data file (Java `DeleteFileIndex.forDataFile`; the D1 index mirrors it), so
    /// committing the DV without merging would silently RESURRECT the parquet delete's positions —
    /// the door must reject. A DV for a data file in a DIFFERENT partition (x=1, where the parquet
    /// delete does not apply) must still commit (the in-test negative control).
    #[tokio::test]
    async fn test_row_delta_dv_rejected_when_legacy_parquet_position_delete_still_applies() {
        use crate::spec::FormatVersion;

        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![
            synthetic_data_file("test/a.parquet", 0),
            synthetic_data_file("test/b.parquet", 1),
        ])
        .await;

        // A partition-scoped parquet position delete in x=0 (legal on V2).
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/x0-pos-del.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Upgrade the table to V3 — the parquet position delete stays live (legacy).
        let tx = Transaction::new(&table);
        let action = tx
            .upgrade_table_version()
            .set_format_version(FormatVersion::V3);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // A DV for A (x=0): the live parquet delete still APPLIES to A and would be silently
        // superseded — rejected.
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![synthetic_dv_file(
            "test/a-dv.puffin",
            0,
            "test/a.parquet",
        )]);
        let tx = action.apply(tx).unwrap();
        let err = tx.commit(&catalog).await.expect_err(
            "a DV for a data file a live parquet position delete still applies to must be rejected",
        );
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message()
                .contains("Cannot commit deletion vector for test/a.parquet")
                && err.message().contains("test/x0-pos-del.parquet")
                && err.message().contains("superseded"),
            "the door must name the referenced file, the shadowed parquet delete, and the \
             supersede hazard, got: {}",
            err.message()
        );

        // Negative control: a DV for B (x=1) — the x=0 parquet delete does not apply — commits.
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![synthetic_dv_file(
            "test/b-dv.puffin",
            1,
            "test/b.parquet",
        )]);
        let tx = action.apply(tx).unwrap();
        tx.commit(&catalog)
            .await
            .expect("a DV in a partition the legacy parquet delete does not cover must commit");
    }

    // ============================================================================================
    // D3: the `validateAddedDVs` op set — Java `VALIDATE_ADDED_DVS_OPERATIONS = {OVERWRITE,
    // DELETE, REPLACE}` (1.10.0-bytecode-verified). REPLACE is the member the generic
    // added-delete-file op set lacks: a compaction snapshot can rewrite DVs (Java
    // `RewriteDataFiles`).
    // ============================================================================================

    /// A test-only action that commits the given DV in a snapshot whose operation is
    /// `Operation::Replace` — no public Rust action adds delete files under REPLACE yet (Java's
    /// `RewriteDataFiles` does), so the concurrent-REPLACE-adds-DV window is hand-built through
    /// the production producer.
    struct ReplaceOpAddDvAction {
        dv: DataFile,
    }

    struct ReplaceOpAddDvOperation;

    impl SnapshotProduceOperation for ReplaceOpAddDvOperation {
        fn operation(&self) -> Operation {
            Operation::Replace
        }

        async fn delete_entries(
            &self,
            _snapshot_produce: &SnapshotProducer<'_>,
        ) -> crate::Result<Vec<crate::spec::ManifestEntry>> {
            Ok(vec![])
        }

        async fn delete_files(
            &self,
            _snapshot_produce: &SnapshotProducer<'_>,
        ) -> crate::Result<Vec<DataFile>> {
            Ok(vec![])
        }

        async fn existing_manifest(
            &self,
            snapshot_produce: &SnapshotProducer<'_>,
        ) -> crate::Result<Vec<crate::spec::ManifestFile>> {
            snapshot_produce.current_manifests().await
        }
    }

    #[async_trait::async_trait]
    impl crate::transaction::TransactionAction for ReplaceOpAddDvAction {
        async fn commit(
            self: Arc<Self>,
            table: &Table,
        ) -> crate::Result<crate::transaction::ActionCommit> {
            SnapshotProducer::new(table, uuid::Uuid::now_v7(), None, HashMap::new(), vec![])
                .with_added_delete_files(vec![self.dv.clone()])
                .commit(ReplaceOpAddDvOperation, DefaultManifestProcess)
                .await
        }
    }

    /// THE REPLACE-OP WALK PIN. A CONCURRENT snapshot with operation REPLACE adds a DV for the
    /// same referenced data file; `validateAddedDVs` must detect it. Risk pinned: the DV walk
    /// reusing the generic added-delete op set `{Overwrite, Delete}` (the pre-D3 bug — REPLACE was
    /// wrongly documented as unrepresentable) would skip the REPLACE snapshot and miss the
    /// conflict. The assertion is on the WALK's message ("Found concurrently added DV for") — the
    /// fresh-DV-only door would also reject this state, but with a DIFFERENT message, so the
    /// message pin isolates the op-set fix.
    #[tokio::test]
    async fn test_row_delta_dv_conflict_detected_from_concurrent_replace_snapshot() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let (table, s0) = append_and_snapshot_id(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        // Row delta adds a DV for A, pinned to S0.
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_dv_file(
                "test/a-dv.puffin",
                0,
                "test/a.parquet",
            )])
            .validate_from_snapshot(s0);
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit: a REPLACE-op snapshot adds a DV for the SAME A.
        let concurrent_tx = Transaction::new(&table);
        let concurrent_tx = ReplaceOpAddDvAction {
            dv: synthetic_dv_file("test/a-dv-replace.puffin", 0, "test/a.parquet"),
        }
        .apply(concurrent_tx)
        .unwrap();
        let concurrent = concurrent_tx.commit(&catalog).await.unwrap();
        assert_eq!(
            concurrent
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Replace,
            "fixture sanity: the concurrent snapshot records REPLACE"
        );

        let err = tx
            .commit(&catalog)
            .await
            .expect_err("a concurrent REPLACE-op DV for the same referenced file must conflict");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(!err.retryable());
        assert!(
            err.message()
                .contains("Found concurrently added DV for test/a.parquet"),
            "the WALK (not the door) must catch the REPLACE-op DV — its message pins the op set, \
             got: {}",
            err.message()
        );
        assert!(
            err.message().contains("test/a-dv-replace.puffin"),
            "the message must carry Java's dvDesc of the concurrent DV, got: {}",
            err.message()
        );
    }

    // ============================================================================================
    // D3: removed-vs-referenced self-contradiction — Java
    // `BaseRowDelta.validateNoConflictingFileAndPositionDeletes` (1.10.0-bytecode-verified,
    // always-on).
    // ============================================================================================

    /// A row delta that REMOVES a data file its added deletes also REFERENCE is rejected with
    /// Java's message ("Cannot delete data files %s that are referenced by new delete files").
    /// Risk pinned: committing both leaves a delete file referencing a data file removed in the
    /// SAME snapshot — a delete that silently applies to nothing.
    #[tokio::test]
    async fn test_row_delta_rejects_removing_data_file_referenced_by_added_deletes() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/a-pos-del.parquet", 0)])
            .validate_data_files_exist(["test/a.parquet"])
            .remove_rows(synthetic_data_file("test/a.parquet", 0));
        let tx = action.apply(tx).unwrap();
        let err = tx
            .commit(&catalog)
            .await
            .expect_err("removing a data file the added deletes reference must be rejected");

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(!err.retryable());
        assert_eq!(
            err.message(),
            "Cannot delete data files [test/a.parquet] that are referenced by new delete files",
            "the message must match Java's (List rendering of the offending paths)"
        );
    }

    /// NEGATIVE CONTROL: disjoint removed/referenced sets commit. The row delta removes A but its
    /// deletes reference only B — no self-contradiction, the check self-skips. Risk pinned: an
    /// over-broad check rejecting ANY remove+reference combination.
    #[tokio::test]
    async fn test_row_delta_disjoint_removed_and_referenced_files_commit() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![
            synthetic_data_file("test/a.parquet", 0),
            synthetic_data_file("test/b.parquet", 0),
        ])
        .await;

        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/b-pos-del.parquet", 0)])
            .validate_data_files_exist(["test/b.parquet"])
            .remove_rows(synthetic_data_file("test/a.parquet", 0));
        let tx = action.apply(tx).unwrap();
        tx.commit(&catalog)
            .await
            .expect("disjoint removed/referenced sets are not a conflict");
    }

    // ============================================================================================
    // D3: the manifest WRITE→READ round-trip pin for a COMMITTED DV's metadata, and the
    // commit-level summary pin for the DV counters.
    // ============================================================================================

    /// A Rust-COMMITTED DV's `referenced_data_file` / `content_offset` / `content_size_in_bytes` /
    /// `record_count` survive the Rust V3 delete-manifest avro WRITE → raw READ round-trip
    /// (`Manifest::try_from_avro_bytes` on the on-disk bytes). D1 proved only that Rust READS
    /// Java-written manifests; this pins the Rust WRITER's schema. Risk pinned: a write schema
    /// dropping the optional DV fields — the scan could then never locate the DV blob (no
    /// offset/length) nor key it to its data file, table-corrupting for every engine.
    #[tokio::test]
    async fn test_committed_dv_metadata_survives_manifest_write_read_round_trip() {
        use crate::spec::Manifest;

        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![synthetic_dv_file(
            "test/a-dv.puffin",
            0,
            "test/a.parquet",
        )]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Locate the committed DELETE manifest and read its RAW avro bytes back.
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let delete_manifest = manifest_list
            .entries()
            .iter()
            .find(|m| m.content == ManifestContentType::Deletes)
            .expect("the row delta committed a DELETE manifest");
        let bytes = table
            .file_io()
            .new_input(&delete_manifest.manifest_path)
            .unwrap()
            .read()
            .await
            .unwrap();
        let (_, entries) = Manifest::try_from_avro_bytes(&bytes).unwrap();

        assert_eq!(entries.len(), 1, "exactly the one added DV entry");
        let dv = entries[0].data_file();
        assert_eq!(dv.content_type(), DataContentType::PositionDeletes);
        assert_eq!(dv.file_format(), DataFileFormat::Puffin);
        assert_eq!(
            dv.referenced_data_file(),
            Some("test/a.parquet".to_string()),
            "referenced_data_file must survive the Rust manifest write→read round-trip"
        );
        assert_eq!(
            dv.content_offset(),
            Some(4),
            "content_offset must survive the round-trip (the scan's ranged blob read needs it)"
        );
        assert_eq!(
            dv.content_size_in_bytes(),
            Some(40),
            "content_size_in_bytes must survive the round-trip"
        );
        assert_eq!(
            dv.record_count(),
            1,
            "record_count (cardinality) must survive"
        );
    }

    // ============================================================================================
    // D3 CROWN JEWEL: the all-Rust deletion-vector end-to-end — D2's writer through D3's commit
    // into D1's read path.
    // ============================================================================================

    /// THE D3 DELIVERABLE. V3 table on a real warehouse → fast_append a REAL parquet data file
    /// (x=0, y=[10,20,30,40,50]) → D2's `DVFileWriter` writes a REAL Puffin DV deleting positions
    /// {1,3} (y=20, y=40) → `row_delta().add_deletes(dv)` COMMITS (the D3 path: V3 gate passes,
    /// fresh-DV door passes, summary gains `added-dvs`) → `scan().to_arrow()` returns exactly the
    /// survivors {10,30,50}.
    ///
    /// Risk pinned: any break in the write→commit→read chain silently resurrects deleted rows (the
    /// merge-on-read corruption class). The MUTATION probe: strip the DV from the commit (or break
    /// the gate) → y=20/y=40 resurrect → this test fails.
    ///
    /// Also pins the COMMIT-LEVEL summary keys offline (the E1 lesson — the D4 interop's canonical
    /// view compares `added-dvs`): a DV increments `added-dvs` INSTEAD of
    /// `added-position-delete-files` but still counts in `added-delete-files` +
    /// `added-position-deletes` (Java `SnapshotSummary.UpdateMetrics.addedFile`,
    /// 1.10.0-bytecode-verified).
    #[tokio::test]
    async fn test_row_delta_deletion_vector_end_to_end_write_commit_scan() {
        use crate::spec::PartitionKey;
        use crate::writer::base_writer::deletion_vector_writer::DVFileWriter;

        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // 1. A real parquet data file: 5 rows in partition x=0, y = [10,20,30,40,50].
        let data_file = write_data_file(&table, "rows.parquet", 0, &[
            (0, 10, 100),
            (0, 20, 200),
            (0, 30, 300),
            (0, 40, 400),
            (0, 50, 500),
        ])
        .await;
        let data_file_path = data_file.file_path().to_string();
        let table = append_files(&catalog, &table, vec![data_file]).await;

        let before: HashSet<i64> = scan_y_values(&table).await;
        assert_eq!(before, HashSet::from([10, 20, 30, 40, 50]));

        // 2. D2's DVFileWriter writes a REAL deletion vector for positions {1, 3} (y=20, y=40),
        //    in the data file's partition context (x=0) so the DeleteFile carries the matching
        //    partition + spec id.
        let partition_key = PartitionKey::new(
            table.metadata().default_partition_spec().as_ref().clone(),
            table.metadata().current_schema().clone(),
            Struct::from_iter([Some(Literal::long(0))]),
        );
        let dv_path = format!("{}/data/deletes-dv.puffin", table.metadata().location());
        let output_file = table.file_io().new_output(&dv_path).unwrap();
        let mut dv_writer = DVFileWriter::new(output_file);
        dv_writer
            .delete(&data_file_path, 1, Some(&partition_key))
            .unwrap();
        dv_writer
            .delete(&data_file_path, 3, Some(&partition_key))
            .unwrap();
        let dv_files = dv_writer.close().await.unwrap();
        assert_eq!(dv_files.len(), 1, "one DV (one referenced data file)");
        assert_eq!(dv_files[0].file_format(), DataFileFormat::Puffin);
        assert_eq!(
            dv_files[0].referenced_data_file(),
            Some(data_file_path.clone())
        );
        assert_eq!(
            dv_files[0].record_count(),
            2,
            "cardinality = 2 deleted positions"
        );

        // 3. The D3 commit: row_delta adds the DV (V3 gate passes — Puffin position delete;
        //    fresh-DV door passes — no live deletes for the file).
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(dv_files);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // 4. The commit-level summary pin: added-dvs INSTEAD of added-position-delete-files.
        assert_eq!(
            summary_prop(&table, "added-dvs").as_deref(),
            Some("1"),
            "a committed DV must emit added-dvs (the D4 interop canonical view compares it)"
        );
        assert_eq!(
            summary_prop(&table, "added-position-delete-files"),
            None,
            "a DV must NOT count as a position-delete FILE (Java's instead-of branch)"
        );
        assert_eq!(
            summary_prop(&table, "added-delete-files").as_deref(),
            Some("1"),
            "a DV still counts as an added delete file"
        );
        assert_eq!(
            summary_prop(&table, "added-position-deletes").as_deref(),
            Some("2"),
            "a DV's record count still counts as added position deletes"
        );

        // 5. D1's read path: the scan drops exactly positions {1,3} of the referenced file.
        let after: HashSet<i64> = scan_y_values(&table).await;
        assert_eq!(
            after,
            HashSet::from([10, 30, 50]),
            "the scan must return exactly the DV's survivors — resurrection of y=20/y=40 means \
             the commit path broke the write→read chain"
        );
    }

    // ============================================================================================
    // D3 review: the fresh-DV-only door's APPLICABILITY discrimination (added in review). The door
    // must fire exactly when a live parquet position delete would actually apply to the DV's
    // referenced data file at READ time (`delete_file_index.rs`): equality deletes never trip it;
    // path-scoped deletes only for the same path; partition-scoped deletes only on the referenced
    // DATA file's (spec id, partition) — resolved from its live manifest entry, surviving a
    // partition evolution — and only when delete_seq >= data_seq.
    // ============================================================================================

    /// OVER-FIRE CONTROL: a live EQUALITY delete in the DV's partition must NOT trip the door —
    /// a DV supersedes only position deletes; equality deletes coexist with it at read time.
    #[tokio::test]
    async fn test_row_delta_dv_commits_despite_live_equality_delete() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_equality_delete_file("test/a-eq.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![synthetic_dv_file(
            "test/a-dv.puffin",
            0,
            "test/a.parquet",
        )]);
        let tx = action.apply(tx).unwrap();
        tx.commit(&catalog)
            .await
            .expect("a live equality delete must not trip the fresh-DV door");
    }

    /// OVER-FIRE CONTROL: a live PATH-scoped parquet position delete for a DIFFERENT file (same
    /// partition) must NOT trip the door — its content holds only the referenced file's positions,
    /// so the DV supersedes nothing that matters.
    #[tokio::test]
    async fn test_row_delta_dv_commits_despite_path_scoped_delete_for_other_file() {
        use crate::spec::FormatVersion;

        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![
            synthetic_data_file("test/a.parquet", 0),
            synthetic_data_file("test/b.parquet", 0),
        ])
        .await;

        let path_scoped = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/b-pos-del.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(0))]))
            .referenced_data_file(Some("test/b.parquet".to_string()))
            .build()
            .unwrap();
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![path_scoped]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let tx = Transaction::new(&table);
        let action = tx
            .upgrade_table_version()
            .set_format_version(FormatVersion::V3);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // DV for A: the live delete is path-scoped to B — must commit.
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![synthetic_dv_file(
            "test/a-dv.puffin",
            0,
            "test/a.parquet",
        )]);
        let tx = action.apply(tx).unwrap();
        tx.commit(&catalog)
            .await
            .expect("a path-scoped delete for a different file must not trip the door");
    }

    /// THE CROSS-SPEC UNDER-FIRE PIN (review fix, the resurrection direction). A legacy
    /// partition-scoped delete under the OLD spec still applies to its old-spec data file at read
    /// time (the index matches on the DATA file's spec id, which stays 0 after a partition
    /// evolution); the added DV is REQUIRED to carry the NEW default spec, so testing the shadow
    /// against the DV's own spec/partition (the pre-review bug) can never match — the DV would
    /// commit and silently supersede the legacy delete. The door must resolve the referenced
    /// file's LIVE entry and reject.
    #[tokio::test]
    async fn test_row_delta_dv_rejected_when_cross_spec_legacy_partition_delete_applies() {
        use crate::spec::FormatVersion;

        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        // Partition-scoped parquet pos delete, spec 0, part (0) — applies to A at read.
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/x0-pos-del.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Evolve the partition spec: add identity(y) → new default spec id, shape (x, y).
        let tx = Transaction::new(&table);
        let action = tx.update_partition_spec().add_field("y");
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let new_spec_id = table.metadata().default_partition_spec_id();
        assert_ne!(new_spec_id, 0, "fixture sanity: the spec evolved");

        // Upgrade to V3.
        let tx = Transaction::new(&table);
        let action = tx
            .upgrade_table_version()
            .set_format_version(FormatVersion::V3);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // DV for A under the NEW spec (the producer requires the default spec id).
        let dv = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/a-dv.puffin".to_string())
            .file_format(DataFileFormat::Puffin)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(new_spec_id)
            .partition(Struct::from_iter([
                Some(Literal::long(0)),
                Some(Literal::long(0)),
            ]))
            .referenced_data_file(Some("test/a.parquet".to_string()))
            .content_offset(Some(4))
            .content_size_in_bytes(Some(40))
            .build()
            .unwrap();
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![dv]);
        let tx = action.apply(tx).unwrap();
        let err = tx.commit(&catalog).await.expect_err(
            "the legacy spec-0 partition delete still applies to A — the DV must be rejected \
             (silent supersede = resurrection)",
        );
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    /// THE REFRESHED-BASE GATE PIN (review). A row delta adding a parquet position delete is
    /// BUILT against a V2 table; a CONCURRENT `upgrade_format_version` to V3 lands before it
    /// commits. `do_commit` re-loads the table from the catalog, so the format gate must see V3
    /// and reject the now-illegal parquet position delete — the placement claim in
    /// `validate_added_delete_files` (1.10.0 gates at add time and would have ACCEPTED this race;
    /// the commit-time placement matches Java MAIN's apply-time re-validation).
    #[tokio::test]
    async fn test_row_delta_parquet_delete_rejected_after_concurrent_format_upgrade() {
        use crate::spec::FormatVersion;

        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        // Build the row delta against the V2 base (legal at build time).
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/a-pos-del.parquet", 0)]);
        let tx = action.apply(tx).unwrap();

        // CONCURRENT commit from the same base: upgrade the table to V3.
        let concurrent_tx = Transaction::new(&table);
        let concurrent_action = concurrent_tx
            .upgrade_table_version()
            .set_format_version(FormatVersion::V3);
        let concurrent_tx = concurrent_action.apply(concurrent_tx).unwrap();
        concurrent_tx.commit(&catalog).await.unwrap();

        // The stale row delta must be re-gated against the REFRESHED (V3) base and rejected.
        let err = tx
            .commit(&catalog)
            .await
            .expect_err("the gate must re-run against the refreshed V3 base");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            err.message(),
            "Must use DVs for position deletes in V3: test/a-pos-del.parquet",
            "the refreshed-base gate must reject with the V3 message"
        );
    }

    /// THE SEQUENCE OVER-FIRE PIN (review fix, the legal-commit direction). A legacy
    /// partition-scoped delete does NOT apply to a data file appended AFTER it (the read-path
    /// filter: a position delete applies iff delete_seq >= data_seq) — a DV for that newer file
    /// shadows nothing and must COMMIT. The pre-review door fired on bare partition equality,
    /// freezing ALL DV writes into any partition carrying a legacy delete.
    #[tokio::test]
    async fn test_row_delta_dv_commits_when_legacy_delete_predates_data_file() {
        use crate::spec::FormatVersion;

        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        // Partition-scoped parquet pos delete, part (0) — applies to A only (seq 2 >= seq 1).
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/x0-pos-del.parquet", 0)]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Upgrade to V3, then append Y in the SAME partition (seq > delete seq).
        let tx = Transaction::new(&table);
        let action = tx
            .upgrade_table_version()
            .set_format_version(FormatVersion::V3);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/y.parquet",
            0,
        )])
        .await;

        // DV for Y: the legacy delete does NOT apply to Y (delete_seq < Y's data_seq) — no shadow
        // hazard, must commit.
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![synthetic_dv_file(
            "test/y-dv.puffin",
            0,
            "test/y.parquet",
        )]);
        let tx = action.apply(tx).unwrap();
        tx.commit(&catalog)
            .await
            .expect("the legacy delete does not apply to the newer file — the DV must commit");
    }

    // ============================================================================================
    // Arc-E Increment 1: apply-side DELETE-FILE removal (`RowDelta.removeDeletes`) + the fresh-DV
    // door's `remove_deletes` escape hatch. Java `BaseRowDelta.removeDeletes` L82-86 →
    // `delete(deletes)` → `deleteFilterManager.delete(file)`; apply composes
    // `deleteFilterManager.filterManifests(deleteManifests)` (`MergingSnapshotProducer.apply`
    // L997-1000).
    // ============================================================================================

    /// Write a REAL Puffin deletion vector for `data_file_path` at `positions`, in partition
    /// `x = part_value`, via D2's `DVFileWriter`. Returns the single produced DV `DataFile`.
    async fn write_real_dv_file(
        table: &Table,
        file_name: &str,
        data_file_path: &str,
        part_value: i64,
        positions: &[u64],
    ) -> DataFile {
        use crate::spec::PartitionKey;
        use crate::writer::base_writer::deletion_vector_writer::DVFileWriter;

        let partition_key = PartitionKey::new(
            table.metadata().default_partition_spec().as_ref().clone(),
            table.metadata().current_schema().clone(),
            Struct::from_iter([Some(Literal::long(part_value))]),
        );
        let dv_path = format!("{}/data/{}", table.metadata().location(), file_name);
        let output_file = table.file_io().new_output(&dv_path).unwrap();
        let mut dv_writer = DVFileWriter::new(output_file);
        for pos in positions {
            dv_writer
                .delete(data_file_path, *pos, Some(&partition_key))
                .unwrap();
        }
        dv_writer.close().await.unwrap().into_iter().next().unwrap()
    }

    /// Collect the LIVE (alive) delete entries across every DELETE manifest of the table's current
    /// snapshot, returning each file's `(path, is_dv, referenced_data_file)`. Used to assert the
    /// one-live-DV-per-file invariant and the tombstoning of a superseded delete.
    async fn live_delete_entries(table: &Table) -> Vec<(String, bool, Option<String>)> {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let mut out = Vec::new();
        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Deletes {
                continue;
            }
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                if !entry.is_alive() {
                    continue;
                }
                let file = entry.data_file();
                out.push((
                    file.file_path().to_string(),
                    is_deletion_vector(file),
                    file.referenced_data_file(),
                ));
            }
        }
        out
    }

    /// THE CROWN JEWEL: DV-replaces-DV, all-Rust end-to-end. V3 real-FS table → real parquet data
    /// file (y=[10,20,30,40,50]) → DV#1 deleting position {1} (y=20) committed via row_delta → scan
    /// = {10,30,40,50} → build DV#2 for positions {1,3} (the hand-merged super-set: y=20 + y=40) →
    /// `row_delta().add_deletes(dv2).remove_deletes(dv1)` commits → scan = survivors of {1,3} =
    /// {10,30,50}. Asserts: the OLD DV is tombstoned in the rewritten DELETE manifest (raw-avro: the
    /// tombstone carries the NEW snapshot id; the surviving new DV keeps its own provenance); the
    /// summary carries `removed-dvs: 1` + `added-dvs: 1`; the manifest list holds exactly ONE live DV
    /// for the data file (the load-door invariant the door+removal pair protects).
    ///
    /// Risk pinned: a broken removal leaves TWO live DVs for the file (the scan's duplicate-DV load
    /// door would reject — fail-late, table unreadable), or drops the wrong positions (resurrection).
    /// This is the only test that proves the apply-side removal closes the merge-and-replace loop.
    #[tokio::test]
    async fn test_row_delta_dv_replaces_dv_end_to_end_remove_deletes() {
        use crate::spec::Manifest;

        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // 1. A real parquet data file: 5 rows, partition x=0, y=[10,20,30,40,50].
        let data_file = write_data_file(&table, "rows.parquet", 0, &[
            (0, 10, 100),
            (0, 20, 200),
            (0, 30, 300),
            (0, 40, 400),
            (0, 50, 500),
        ])
        .await;
        let data_file_path = data_file.file_path().to_string();
        let table = append_files(&catalog, &table, vec![data_file]).await;

        // 2. DV#1 deletes position {1} (y=20). Commit via row_delta.
        let dv1 = write_real_dv_file(&table, "dv1.puffin", &data_file_path, 0, &[1]).await;
        let dv1_path = dv1.file_path().to_string();
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![dv1.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        assert_eq!(
            scan_y_values(&table).await,
            HashSet::from([10, 30, 40, 50]),
            "after DV#1 the scan drops y=20"
        );

        // 3. DV#2 is the merged super-set {1,3} (y=20 + y=40), hand-merged here (the WRITER-side
        //    loadPreviousDeletes auto-merge is the next increment). Commit add(dv2) + remove(dv1).
        let dv2 = write_real_dv_file(&table, "dv2.puffin", &data_file_path, 0, &[1, 3]).await;
        let dv2_path = dv2.file_path().to_string();
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![dv2.clone()])
            .remove_deletes(dv1.clone());
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let new_snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();

        // 4a. The scan now drops exactly {1,3} of the data file → survivors {10,30,50}.
        assert_eq!(
            scan_y_values(&table).await,
            HashSet::from([10, 30, 50]),
            "after the DV replace, the scan drops the super-set {{1,3}} (y=20, y=40)"
        );

        // 4b. The manifest list holds exactly ONE live DV for the data file (the load-door invariant).
        let live = live_delete_entries(&table).await;
        let live_dvs_for_file: Vec<&(String, bool, Option<String>)> = live
            .iter()
            .filter(|(_, is_dv, referenced)| {
                *is_dv && referenced.as_deref() == Some(data_file_path.as_str())
            })
            .collect();
        assert_eq!(
            live_dvs_for_file.len(),
            1,
            "exactly ONE live DV for the data file post-commit — got {:?}",
            live
        );
        assert_eq!(
            live_dvs_for_file[0].0, dv2_path,
            "the surviving live DV is DV#2 (the merged super-set), not the removed DV#1"
        );

        // 4c. The summary carries removed-dvs: 1 + added-dvs: 1.
        assert_eq!(
            summary_prop(&table, "added-dvs").as_deref(),
            Some("1"),
            "the added super-set DV bumps added-dvs"
        );
        assert_eq!(
            summary_prop(&table, "removed-dvs").as_deref(),
            Some("1"),
            "the removed old DV bumps removed-dvs (the D3 branch, now reachable end-to-end)"
        );

        // 4d. PROVENANCE PIN (raw avro): the rewritten DELETE manifest that contained DV#1 carries it
        //     as a DELETED tombstone stamped with the NEW snapshot id; any surviving EXISTING entry
        //     keeps its ORIGINAL snapshot id (not re-stamped).
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let mut found_dv1_tombstone = false;
        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Deletes {
                continue;
            }
            let bytes = table
                .file_io()
                .new_input(&manifest_file.manifest_path)
                .unwrap()
                .read()
                .await
                .unwrap();
            let (_, entries) = Manifest::try_from_avro_bytes(&bytes).unwrap();
            for entry in &entries {
                if entry.file_path() == dv1_path {
                    assert_eq!(
                        entry.status(),
                        ManifestStatus::Deleted,
                        "DV#1 must be a DELETED tombstone in the rewritten DELETE manifest"
                    );
                    assert_eq!(
                        entry.snapshot_id(),
                        Some(new_snapshot_id),
                        "the tombstone carries the NEW snapshot id (the rewrite's provenance stamp)"
                    );
                    found_dv1_tombstone = true;
                }
            }
        }
        assert!(
            found_dv1_tombstone,
            "DV#1 must appear as a tombstone in a rewritten DELETE manifest (it was not dropped \
             silently)"
        );
    }

    /// Load a committed DV's positions back through the PRODUCTION read path — the D1
    /// `CachingDeleteFileLoader` (ranged Puffin read → `deletion-vector-v1` decode), NOT a hand-built
    /// vector. Returns the decoded [`DeleteVector`] keyed by the DV's `referenced_data_file`. This is
    /// exactly what an engine's `loadPreviousDeletes` does: read the existing DV off disk.
    async fn load_dv_positions_via_production_loader(
        table: &Table,
        dv_file: &DataFile,
        referenced_data_file: &str,
    ) -> DeleteVector {
        let task = FileScanTaskDeleteFile {
            file_path: dv_file.file_path().to_string(),
            file_size_in_bytes: dv_file.file_size_in_bytes(),
            file_type: dv_file.content_type(),
            partition_spec_id: dv_file.partition_spec_id,
            equality_ids: None,
            file_format: dv_file.file_format(),
            referenced_data_file: dv_file.referenced_data_file(),
            content_offset: dv_file.content_offset(),
            content_size_in_bytes: dv_file.content_size_in_bytes(),
            record_count: Some(dv_file.record_count()),
        };
        let loader = CachingDeleteFileLoader::new(table.file_io().clone(), 4);
        let delete_filter = loader
            .load_deletes(
                std::slice::from_ref(&task),
                Arc::new(
                    crate::spec::Schema::builder()
                        .build()
                        .expect("empty schema"),
                ),
            )
            .await
            .expect("loader future")
            .expect("the production loader must load the committed DV");
        let vector = delete_filter
            .get_delete_vector_for_path(referenced_data_file)
            .expect("a delete vector for the referenced data file");
        let guard = vector.lock().expect("lock delete vector");
        DeleteVector::new(guard.iter().collect())
    }

    /// Write a DV via the WRITER-side MERGE hook (`DVFileWriter::with_previous_deletes`): the writer
    /// unions `previous_positions` (sourced from `previous_dv`) into its new positions and returns
    /// the merged DV `DeleteFile`s + the file-scoped rewritten (superseded) delete files. The Rust
    /// mirror of Java `BaseDVFileWriter` driven by a real `loadPreviousDeletes`.
    async fn write_merged_dv_file(
        table: &Table,
        file_name: &str,
        data_file_path: &str,
        part_value: i64,
        new_positions: &[u64],
        previous_positions: DeleteVector,
        previous_dv: DataFile,
    ) -> crate::writer::base_writer::deletion_vector_writer::DVWriteResult {
        use crate::spec::PartitionKey;
        use crate::writer::base_writer::deletion_vector_writer::{DVFileWriter, PreviousDeletes};

        let partition_key = PartitionKey::new(
            table.metadata().default_partition_spec().as_ref().clone(),
            table.metadata().current_schema().clone(),
            Struct::from_iter([Some(Literal::long(part_value))]),
        );
        let dv_path = format!("{}/data/{}", table.metadata().location(), file_name);
        let output_file = table.file_io().new_output(&dv_path).unwrap();
        let previous = PreviousDeletes::new(previous_positions, vec![previous_dv]);
        let mut dv_writer = DVFileWriter::new(output_file)
            .with_previous_deletes(HashMap::from([(data_file_path.to_string(), previous)]));
        for pos in new_positions {
            dv_writer
                .delete(data_file_path, *pos, Some(&partition_key))
                .unwrap();
        }
        dv_writer.close_with_result().await.unwrap()
    }

    /// THE CROWN JEWEL (Arc-E Inc 2): the WRITER-side previous-deletes MERGE closes the loop, all-Rust
    /// end-to-end — mirroring the REAL engine flow (Spark `SparkPositionDeltaWrite` L234-256:
    /// `addDeletes(dv)` + `for f in rewrittenDeleteFiles: removeDeletes(f)`).
    ///   1. V3 real-FS table → real parquet data file (y=[10,20,30,40,50]);
    ///   2. DV#1 deleting position {1} (y=20) committed via row_delta → scan = {10,30,40,50};
    ///   3. LOAD DV#1's positions back via the PRODUCTION loader (`CachingDeleteFileLoader`, NOT a
    ///      hand-built vector), feed them as previous-deletes to a NEW `DVFileWriter` writing only the
    ///      NEW position {3} → the WRITER merges {1}∪{3} = {1,3} and returns rewritten=[DV#1];
    ///   4. `row_delta().add_deletes(merged_dv).remove_deletes_many(rewritten)` commits (the escape
    ///      hatch unlocks); scan = survivors of {1,3} = {10,30,50}, exactly ONE live DV, DV#1 absent.
    ///
    /// Risk pinned: the merge is the load-bearing new behavior — the writer (not the test) must
    /// produce {1,3} from previous {1} + new {3}. A broken merge writes only {3} → y=20 RESURRECTS
    /// (scan would return {10,20,30,50}). The rewritten-file plumbing must also flow DV#1 into
    /// `remove_deletes` (else the door rejects the second DV, or two DVs survive). This is the test
    /// that proves the WRITER-side `loadPreviousDeletes` half (the last deferred piece of the DV
    /// write surface) works end to end against the real read path.
    #[tokio::test]
    async fn test_row_delta_dv_writer_merges_previous_deletes_end_to_end() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // 1. A real parquet data file: 5 rows, partition x=0, y=[10,20,30,40,50].
        let data_file = write_data_file(&table, "rows.parquet", 0, &[
            (0, 10, 100),
            (0, 20, 200),
            (0, 30, 300),
            (0, 40, 400),
            (0, 50, 500),
        ])
        .await;
        let data_file_path = data_file.file_path().to_string();
        let table = append_files(&catalog, &table, vec![data_file]).await;

        // 2. DV#1 deletes position {1} (y=20). Commit via row_delta.
        let dv1 = write_real_dv_file(&table, "dv1.puffin", &data_file_path, 0, &[1]).await;
        let dv1_path = dv1.file_path().to_string();
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![dv1.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        assert_eq!(
            scan_y_values(&table).await,
            HashSet::from([10, 30, 40, 50]),
            "after DV#1 the scan drops y=20"
        );

        // 3. Load DV#1's positions back through the PRODUCTION loader, then write DV#2 with the WRITER
        //    MERGE hook supplying those positions + adding the NEW position {3}. The WRITER produces
        //    the super-set {1,3} (NOT hand-merged) and returns rewritten=[DV#1].
        let previous_positions =
            load_dv_positions_via_production_loader(&table, &dv1, &data_file_path).await;
        assert_eq!(
            previous_positions.iter().collect::<Vec<_>>(),
            vec![1],
            "the production loader must read back DV#1's position {{1}}"
        );

        let merge_result = write_merged_dv_file(
            &table,
            "dv2.puffin",
            &data_file_path,
            0,
            &[3],
            previous_positions,
            dv1.clone(),
        )
        .await;

        // The writer produced exactly one merged DV with the UNION cardinality, plus rewritten=[DV#1].
        assert_eq!(merge_result.delete_files.len(), 1);
        let dv2 = merge_result.delete_files[0].clone();
        let dv2_path = dv2.file_path().to_string();
        assert_eq!(
            dv2.record_count(),
            2,
            "the merged DV must carry the UNION {{1,3}} (cardinality 2), proving the WRITER merged"
        );
        assert_eq!(
            merge_result.rewritten_delete_files.len(),
            1,
            "DV#1 (file-scoped) must be returned as a rewritten/superseded delete file"
        );
        assert_eq!(
            merge_result.rewritten_delete_files[0].file_path(),
            dv1_path,
            "the rewritten file is DV#1"
        );

        // 4. Commit add(merged DV) + remove(rewritten DV#1) — EXACTLY the engine flow. The fresh-DV
        //    door's escape hatch unlocks because the live DV#1 is removed in the same commit.
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(merge_result.delete_files)
            .remove_deletes_many(merge_result.rewritten_delete_files);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // 4a. The scan now drops exactly the merged {1,3} (y=20, y=40) → survivors {10,30,50}. A
        //     broken merge (only {3}) would RESURRECT y=20 here.
        assert_eq!(
            scan_y_values(&table).await,
            HashSet::from([10, 30, 50]),
            "after the writer-merged DV replace, the scan drops {{1,3}} (y=20 + y=40)"
        );

        // 4b. Exactly ONE live DV for the data file post-commit (the load-door invariant), and it is
        //     the merged DV#2 — DV#1 is gone.
        let live = live_delete_entries(&table).await;
        let live_dvs_for_file: Vec<&(String, bool, Option<String>)> = live
            .iter()
            .filter(|(_, is_dv, referenced)| {
                *is_dv && referenced.as_deref() == Some(data_file_path.as_str())
            })
            .collect();
        assert_eq!(
            live_dvs_for_file.len(),
            1,
            "exactly ONE live DV for the data file post-commit — got {live:?}"
        );
        assert_eq!(
            live_dvs_for_file[0].0, dv2_path,
            "the surviving live DV is the writer-merged DV#2, not the removed DV#1"
        );

        // 4c. The summary carries removed-dvs: 1 + added-dvs: 1 (the merge-and-replace shape).
        assert_eq!(summary_prop(&table, "added-dvs").as_deref(), Some("1"));
        assert_eq!(summary_prop(&table, "removed-dvs").as_deref(), Some("1"));
    }

    /// MUTATION (a): the door+removal pair protects the post-commit one-DV invariant. With the
    /// removal SKIPPED (add DV#2 only, no `remove_deletes`), the fresh-DV door REJECTS the commit
    /// (a live DV already covers the file) — proving the door is what stops the two-DV state the
    /// removal would otherwise be needed to clear. (The brief's "door mutated off → scan rejects at
    /// the load door" is the same invariant from the read side; the door rejecting at COMMIT is the
    /// fail-loud conversion of that fail-late read error, and is the directly-testable half without
    /// mutating production code.)
    #[tokio::test]
    async fn test_row_delta_second_dv_without_removal_still_rejected_by_door() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        // DV#1 for A commits.
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![synthetic_dv_file(
            "test/a-dv1.puffin",
            0,
            "test/a.parquet",
        )]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // DV#2 for A WITHOUT removing DV#1 — the door must still reject (no escape hatch engaged).
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![synthetic_dv_file(
            "test/a-dv2.puffin",
            0,
            "test/a.parquet",
        )]);
        let tx = action.apply(tx).unwrap();
        let err = tx.commit(&catalog).await.expect_err(
            "without removing the live DV, a second DV for the same file must still be rejected",
        );
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message()
                .contains("Cannot commit deletion vector for test/a.parquet"),
            "got: {}",
            err.message()
        );
    }

    /// THE DOOR-RELAXATION POSITIVE (synthetic, fast): a DV#2 for A that REMOVES the live DV#1 in the
    /// same commit COMMITS — the escape hatch engages on the removed-set path match. The negative
    /// half (no removal → rejected) is the D3 door test
    /// `test_row_delta_second_dv_for_same_file_rejected_until_merge_lands`, which stays green.
    #[tokio::test]
    async fn test_row_delta_dv_with_removal_of_live_dv_commits() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        let dv1 = synthetic_dv_file("test/a-dv1.puffin", 0, "test/a.parquet");
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![dv1.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // DV#2 for A + remove DV#1 in the same commit — the escape hatch lets it through.
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_dv_file(
                "test/a-dv2.puffin",
                0,
                "test/a.parquet",
            )])
            .remove_deletes(dv1);
        let tx = action.apply(tx).unwrap();
        let table = tx
            .commit(&catalog)
            .await
            .expect("removing the live DV in the same commit must let the new DV through the door");

        // Exactly one live DV remains (DV#2), and the old one is gone.
        let live = live_delete_entries(&table).await;
        let live_dv_paths: Vec<&String> = live
            .iter()
            .filter(|(_, is_dv, _)| *is_dv)
            .map(|(path, _, _)| path)
            .collect();
        assert_eq!(
            live_dv_paths,
            vec![&"test/a-dv2.puffin".to_string()],
            "exactly DV#2 is live; DV#1 is tombstoned — got {:?}",
            live
        );
        assert_eq!(summary_prop(&table, "removed-dvs").as_deref(), Some("1"));
        assert_eq!(summary_prop(&table, "added-dvs").as_deref(), Some("1"));
    }

    /// THE ESCAPE HATCH IS PER-REFERENCED-FILE, NOT GLOBAL (REVIEWER pin — adversarial case ii). Two
    /// data files A and B each carry a live DV. A row delta adds a NEW DV for A but removes B's DV (a
    /// DIFFERENT file's delete). The door must STILL REJECT the new DV for A: the removed-set match is
    /// keyed on the EXISTING delete's path, and only A's own live DV (whose path is NOT in the removed
    /// set) is consulted when deciding whether A's new DV is fresh. A bug that treated "something was
    /// removed in this commit" as a GLOBAL unlock (rather than per-delete-file-path) would wrongly let
    /// the A DV through, leaving two live DVs for A (the load-door corruption the door+removal pair
    /// exists to prevent). Mutation that surfaces it: keying the escape-hatch skip on
    /// `!removed_delete_paths.is_empty()` instead of `removed_delete_paths.contains(existing.file_path())`.
    #[tokio::test]
    async fn test_row_delta_remove_different_files_dv_does_not_unlock_door() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![
            synthetic_data_file("test/a.parquet", 0),
            synthetic_data_file("test/b.parquet", 0),
        ])
        .await;

        // A live DV for A and a live DV for B (committed in one row delta — different referenced files,
        // so neither trips the other's door).
        let a_dv1 = synthetic_dv_file("test/a-dv1.puffin", 0, "test/a.parquet");
        let b_dv1 = synthetic_dv_file("test/b-dv1.puffin", 0, "test/b.parquet");
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![a_dv1.clone(), b_dv1.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Add a NEW DV for A while removing B's DV (the WRONG file's removal). The door must still
        // reject: A's live DV is not in the removed set, so the escape hatch does not engage for A.
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_dv_file(
                "test/a-dv2.puffin",
                0,
                "test/a.parquet",
            )])
            .remove_deletes(b_dv1);
        let tx = action.apply(tx).unwrap();
        let err = tx.commit(&catalog).await.expect_err(
            "removing a DIFFERENT file's DV must NOT unlock the door for A's new DV (per-file, not global)",
        );
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message()
                .contains("Cannot commit deletion vector for test/a.parquet"),
            "got: {}",
            err.message()
        );
    }

    /// REMOVE A PARQUET POSITION DELETE end-to-end (V2 fixture). A real parquet data file + a real
    /// position-delete deleting {1,3} is committed; the scan drops y=20/y=40. Then `row_delta()
    /// .remove_deletes(pos_delete)` (a delete-manifest-only commit, no adds) removes it; the scan
    /// returns all five rows again, and the summary carries `removed-position-delete-files: 1`. The
    /// operation is `Overwrite` (1.10.0: adds nothing ⇒ not the adds-deletes-only DELETE branch).
    ///
    /// Risk pinned: the removal silently doing nothing (scan still missing the rows), or the rewritten
    /// DELETE manifest being misclassified as DATA (the content-keyed filtering writer) so the read
    /// path stops applying its survivors.
    #[tokio::test]
    async fn test_row_delta_remove_parquet_position_delete_restores_rows() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;

        let data_file = write_data_file(&table, "rows.parquet", 0, &[
            (0, 10, 100),
            (0, 20, 200),
            (0, 30, 300),
            (0, 40, 400),
            (0, 50, 500),
        ])
        .await;
        let data_file_path = data_file.file_path().to_string();
        let table = append_files(&catalog, &table, vec![data_file]).await;

        let delete_file = write_position_delete_file(&table, 0, &[
            (data_file_path.clone(), 1),
            (data_file_path.clone(), 3),
        ])
        .await;
        let delete_file_path = delete_file.file_path().to_string();
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![delete_file.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        assert_eq!(
            scan_y_values(&table).await,
            HashSet::from([10, 30, 50]),
            "the position delete drops y=20, y=40"
        );

        // Remove the position delete (a delete-manifest-only commit).
        let tx = Transaction::new(&table);
        let action = tx.row_delta().remove_deletes(delete_file);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(
            scan_y_values(&table).await,
            HashSet::from([10, 20, 30, 40, 50]),
            "removing the position delete restores all five rows"
        );
        assert_eq!(
            summary_prop(&table, "removed-position-delete-files").as_deref(),
            Some("1"),
            "a removed parquet position delete bumps removed-position-delete-files (NOT removed-dvs)"
        );
        assert_eq!(
            summary_prop(&table, "removed-dvs"),
            None,
            "a parquet position delete is not a DV"
        );
        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Overwrite,
            "a remove-only row delta records Overwrite per 1.10.0 (adds no delete files)"
        );

        // The removed delete file must be tombstoned, no longer live.
        let live = live_delete_entries(&table).await;
        assert!(
            !live.iter().any(|(path, _, _)| *path == delete_file_path),
            "the removed position delete must not be live anymore — got {:?}",
            live
        );
    }

    /// REMOVE AN EQUALITY DELETE (V2 fixture, manifest-level). A synthetic equality delete is
    /// committed, then removed via `remove_deletes`; the summary carries
    /// `removed-equality-delete-files: 1` and the entry is tombstoned. Risk pinned: the equality
    /// branch of `remove_file` (summary) and the content-keyed DELETE-manifest rewrite both working.
    #[tokio::test]
    async fn test_row_delta_remove_equality_delete_bumps_removed_equality_counter() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        let eq_delete = synthetic_equality_delete_file("test/a-eq.parquet", 0);
        let eq_path = eq_delete.file_path().to_string();
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![eq_delete.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let tx = Transaction::new(&table);
        let action = tx.row_delta().remove_deletes(eq_delete);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        assert_eq!(
            summary_prop(&table, "removed-equality-delete-files").as_deref(),
            Some("1"),
            "a removed equality delete bumps removed-equality-delete-files"
        );
        let live = live_delete_entries(&table).await;
        assert!(
            !live.iter().any(|(path, _, _)| *path == eq_path),
            "the removed equality delete must be tombstoned — got {:?}",
            live
        );
    }

    /// MISSING-REMOVAL-PATH ERROR. Removing a delete file that is NOT live in the current snapshot
    /// fails loud with the exact Java `failMissingDeletePaths` message shape ("Missing required files
    /// to delete: %s"). Risk pinned: a removal silently no-op'ing when the target is absent (the
    /// present-and-absent validation of `resolve_delete_file_paths`).
    #[tokio::test]
    async fn test_row_delta_remove_nonexistent_delete_file_errors_with_missing_message() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        // No delete file was ever committed — removing one must fail with the missing message.
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .remove_deletes(synthetic_delete_file("test/ghost-pos-del.parquet", 0));
        let tx = action.apply(tx).unwrap();
        let err = tx
            .commit(&catalog)
            .await
            .expect_err("removing a delete file that is not live must fail loud");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            err.message(),
            "Missing required files to delete: test/ghost-pos-del.parquet",
            "the missing-removal-path error must match Java failMissingDeletePaths shape"
        );
    }

    /// REMOVE-ONLY COMMIT OPERATION CLASSIFICATION (1.10.0). A row delta that ONLY removes a delete
    /// file (no adds) records `Operation::Overwrite` per Java 1.10.0 `BaseRowDelta.operation()`
    /// (`addsDeleteFiles()` is false ⇒ not the DELETE branch ⇒ OVERWRITE). Risk pinned: a remove-only
    /// commit being misclassified, and confirms the empty-commit guard treats a removal as content.
    #[tokio::test]
    async fn test_row_delta_remove_deletes_only_records_overwrite() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        let pos_delete = synthetic_delete_file("test/a-pos-del.parquet", 0);
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![pos_delete.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let tx = Transaction::new(&table);
        let action = tx.row_delta().remove_deletes(pos_delete);
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
            "a remove-deletes-only row delta records Overwrite per 1.10.0"
        );
    }

    /// REMOVE-DELETES REJECTS A DATA-CONTENT FILE. `remove_deletes` is the delete-side surface (Java
    /// `removeDeletes(DeleteFile)`); passing a `Data` file is a caller error (use `remove_rows`).
    /// Risk pinned: a data file silently routed into the delete-removal path.
    #[tokio::test]
    async fn test_row_delta_remove_deletes_rejects_data_content() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![synthetic_data_file(
            "test/a.parquet",
            0,
        )])
        .await;

        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .remove_deletes(synthetic_data_file("test/a.parquet", 0));
        let tx = action.apply(tx).unwrap();
        let err = tx
            .commit(&catalog)
            .await
            .expect_err("remove_deletes must reject a Data-content file");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message()
                .contains("Only position-delete or equality-delete content is allowed for removed delete files"),
            "got: {}",
            err.message()
        );
    }

    /// PROVENANCE PIN on the rewritten DELETE manifest (synthetic, isolates the survivor-preservation).
    /// A DELETE manifest holding TWO live deletes (D1, D2) has D1 removed; the rewritten manifest must
    /// carry D1 as a DELETED tombstone (new snapshot id) and D2 as an EXISTING entry keeping its
    /// ORIGINAL snapshot id + sequence number (NOT re-stamped). Mutation (b) from the brief: routing
    /// the survivor through `add_entry` (re-stamp) instead of `add_existing_entry` makes the
    /// original-provenance assertion fail.
    #[tokio::test]
    async fn test_row_delta_remove_delete_preserves_surviving_entry_provenance() {
        use crate::spec::Manifest;

        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![
            synthetic_data_file("test/a.parquet", 0),
            synthetic_data_file("test/b.parquet", 0),
        ])
        .await;

        // Commit TWO position deletes (D1 for A, D2 for B) in ONE delete manifest.
        let d1 = synthetic_delete_file("test/d1-pos-del.parquet", 0);
        let d2 = synthetic_delete_file("test/d2-pos-del.parquet", 0);
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![d1.clone(), d2.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let add_snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();
        let add_seq = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number();

        // Remove ONLY D1; D2 survives.
        let tx = Transaction::new(&table);
        let action = tx.row_delta().remove_deletes(d1);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        let remove_snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();
        assert_ne!(add_snapshot_id, remove_snapshot_id);

        // Read the rewritten DELETE manifest (the one this snapshot wrote) raw.
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let rewritten = manifest_list
            .entries()
            .iter()
            .find(|m| {
                m.content == ManifestContentType::Deletes
                    && m.added_snapshot_id == remove_snapshot_id
            })
            .expect("the remove commit wrote a rewritten DELETE manifest");
        let bytes = table
            .file_io()
            .new_input(&rewritten.manifest_path)
            .unwrap()
            .read()
            .await
            .unwrap();
        let (_, entries) = Manifest::try_from_avro_bytes(&bytes).unwrap();

        let d1_entry = entries
            .iter()
            .find(|e| e.file_path() == "test/d1-pos-del.parquet")
            .expect("D1 is in the rewritten manifest as a tombstone");
        assert_eq!(d1_entry.status(), ManifestStatus::Deleted);
        assert_eq!(
            d1_entry.snapshot_id(),
            Some(remove_snapshot_id),
            "the D1 tombstone carries the NEW (remove) snapshot id"
        );

        let d2_entry = entries
            .iter()
            .find(|e| e.file_path() == "test/d2-pos-del.parquet")
            .expect("D2 survives in the rewritten manifest");
        assert_eq!(
            d2_entry.status(),
            ManifestStatus::Existing,
            "the surviving D2 is copied forward as Existing (provenance preserved)"
        );
        assert_eq!(
            d2_entry.snapshot_id(),
            Some(add_snapshot_id),
            "the surviving D2 keeps its ORIGINAL (add) snapshot id — re-stamping is the corruption \
             class this pins"
        );
        // The surviving D2's data sequence number is preserved verbatim (it flows through
        // `add_existing_entry`, which copies the POST-inheritance seq from the source manifest read
        // — never re-inherits the new snapshot's seq).
        assert_eq!(
            d2_entry.sequence_number(),
            Some(add_seq),
            "the surviving D2 keeps its original sequence number (no re-inheritance)"
        );
    }

    /// CUMULATIVE TOTALS across append → row_delta(DV) → row_delta(replace DV). The total-* summary
    /// keys must reflect previous + added − removed across the chain (the seed-from-previous rule):
    /// after the DV replace, `total-delete-files` stays 1 (added 1 super-set DV, removed 1 old DV) and
    /// `total-position-deletes` reflects the net. Risk pinned: a per-commit seed bug (totals reset
    /// instead of carried from the previous summary) — only a multi-commit chain catches it.
    #[tokio::test]
    async fn test_row_delta_cumulative_totals_across_dv_replace() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let data_file = write_data_file(&table, "rows.parquet", 0, &[
            (0, 10, 100),
            (0, 20, 200),
            (0, 30, 300),
            (0, 40, 400),
            (0, 50, 500),
        ])
        .await;
        let data_file_path = data_file.file_path().to_string();
        let table = append_files(&catalog, &table, vec![data_file]).await;

        // DV#1 {1}: total-delete-files 1, total-position-deletes 1.
        let dv1 = write_real_dv_file(&table, "dv1.puffin", &data_file_path, 0, &[1]).await;
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![dv1.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        assert_eq!(
            summary_prop(&table, "total-delete-files").as_deref(),
            Some("1"),
            "after DV#1: one delete file"
        );
        assert_eq!(
            summary_prop(&table, "total-position-deletes").as_deref(),
            Some("1"),
            "after DV#1: one position delete"
        );

        // DV#2 {1,3} replaces DV#1: total-delete-files stays 1 (1 + 1 − 1), total-position-deletes
        // becomes 2 (1 + 2 − 1).
        let dv2 = write_real_dv_file(&table, "dv2.puffin", &data_file_path, 0, &[1, 3]).await;
        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_deletes(vec![dv2]).remove_deletes(dv1);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        assert_eq!(
            summary_prop(&table, "total-delete-files").as_deref(),
            Some("1"),
            "after the DV replace: total-delete-files = 1 + 1 − 1 = 1 (the seed-from-previous rule)"
        );
        assert_eq!(
            summary_prop(&table, "total-position-deletes").as_deref(),
            Some("2"),
            "after the DV replace: total-position-deletes = 1 + 2 − 1 = 2"
        );
    }

    /// THE DATA-SIDE FILTERING WRITER IS UNCHANGED (regression guard for the content-keyed
    /// `new_filtering_manifest_writer` extension). A DELETE-from-data-files commit
    /// (`overwrite_files().delete_files`) still rewrites the DATA manifest with the removed file as a
    /// DELETED tombstone and survivors EXISTING — the brief's "data-side behavior must be
    /// byte-identical" pin at the read level. Risk pinned: the content-keying mistakenly building a
    /// DELETE writer for a DATA source (which would misclassify the rewritten data manifest).
    #[tokio::test]
    async fn test_overwrite_delete_data_file_still_rewrites_data_manifest() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let table = append_files(&catalog, &table, vec![
            synthetic_data_file("test/a.parquet", 0),
            synthetic_data_file("test/b.parquet", 0),
        ])
        .await;

        // Remove A via overwrite_files (the data-side filter path).
        let tx = Transaction::new(&table);
        let action = tx.overwrite_files().delete_files(["test/a.parquet"]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // The rewritten DATA manifest still classifies correctly: A tombstoned, B live.
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let mut a_tombstoned = false;
        let mut b_live = false;
        for manifest_file in manifest_list.entries() {
            assert_eq!(
                manifest_file.content,
                ManifestContentType::Data,
                "no DELETE manifest should exist on this data-only table"
            );
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                match entry.file_path() {
                    "test/a.parquet" if entry.status() == ManifestStatus::Deleted => {
                        a_tombstoned = true
                    }
                    "test/b.parquet" if entry.is_alive() => b_live = true,
                    _ => {}
                }
            }
        }
        assert!(a_tombstoned, "A must be a DATA-manifest tombstone");
        assert!(b_live, "B must remain live in the DATA manifest");
    }
}
