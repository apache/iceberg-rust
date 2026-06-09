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
//! referenced data file — two DVs for one data file is a write-write conflict. The concurrent walk reuses the
//! existing [`added_delete_files_after`] enumeration: Java's `VALIDATE_ADDED_DVS_OPERATIONS = {OVERWRITE,
//! DELETE, REPLACE}` (L84-85) reduces to `{Overwrite, Delete}` in Rust because `REPLACE` is unrepresentable in
//! the [`Operation`] enum (Rust never records a REPLACE snapshot), which is IDENTICAL to the
//! `VALIDATE_ADDED_DELETE_FILES_OPERATIONS` op-set [`added_delete_files_after`] already walks (its V2 guard is
//! correct — DVs are V3, so the table is V2+ and the guard never excludes a real DV). The concurrently-added
//! deletes are filtered to DVs (`file_format() == Puffin`) and rejected on the first whose
//! `referenced_data_file` collides with this row delta's added-DV set.
//!
//! **Out of scope (deferred):**
//! - Equality-delete WRITER end-to-end (the writer exists; the RowDelta-with-equality-deletes scan
//!   application may have gaps — the end-to-end test focuses on POSITION deletes).
//! - APPLY-SIDE removal for `remove_data_files` (see above) and `removeDeletes` (removing existing delete
//!   files) — only the VALIDATION half of `removeRows` lands here.
//! - The deletion-vector (V3 Puffin) write path.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::Result;
use crate::expr::visitors::inclusive_metrics_evaluator::InclusiveMetricsEvaluator;
use crate::expr::{Bind, Predicate};
use crate::spec::{DataFile, DataFileFormat, ManifestEntry, ManifestFile, Operation};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer, added_delete_files_after,
    deleted_data_files_after, validate_no_conflicting_added_data_files,
    validate_no_conflicting_added_delete_files, validate_no_new_deletes_for_data_files,
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

    /// Build the set of DATA-file paths this row delta is adding DELETION VECTORS (DVs) for — the Rust port of
    /// Java `MergingSnapshotProducer.dvsByReferencedFile` (populated at `add` time, `MergingSnapshotProducer
    /// .java` L280-284, keyed on `file.referencedDataFile()` when `ContentFileUtil.isDV(file)`).
    ///
    /// A DV is an added delete file whose `file_format() == DataFileFormat::Puffin` (Java
    /// `ContentFileUtil.isDV` = `format() == FileFormat.PUFFIN`). Each DV's `referenced_data_file` is the data
    /// file it covers and is REQUIRED for a DV (the spec mandates it); a Puffin delete file MISSING
    /// `referenced_data_file` is malformed, so this errors (matching Java, which dereferences
    /// `file.referencedDataFile()` as a non-null map key when populating `dvsByReferencedFile`). Non-Puffin
    /// deletes (position / equality) are SKIPPED — they are not DVs — so for the common merge-on-read row delta
    /// (no DVs) this returns an EMPTY set, which makes the always-on `validate_added_dvs` step self-skip.
    fn added_dv_referenced_files(&self) -> Result<HashSet<String>> {
        let mut referenced = HashSet::new();
        for delete_file in &self.added_delete_files {
            if delete_file.file_format() != DataFileFormat::Puffin {
                // Not a DV (Java `ContentFileUtil.isDV` is false) — a position/equality delete, not indexed
                // into `dvsByReferencedFile`.
                continue;
            }
            match delete_file.referenced_data_file() {
                Some(path) => {
                    referenced.insert(path);
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
    /// DVs (`added_dv_referenced_files` empty ⇔ Java `dvsByReferencedFile.isEmpty()`). The common merge-on-read
    /// row delta adds NON-Puffin position / equality deletes, so the set is empty and this is a no-op.
    ///
    /// **Concurrent walk (Java L835-841 + L84-85 `VALIDATE_ADDED_DVS_OPERATIONS = {OVERWRITE, DELETE,
    /// REPLACE}`):** the concurrently-added delete files are enumerated by the SHARED
    /// [`added_delete_files_after`] walk. Java's DV op-set is `{OVERWRITE, DELETE, REPLACE}`, but `REPLACE` is
    /// unrepresentable in the Rust [`Operation`] enum (Rust never records a REPLACE snapshot), so it reduces to
    /// `{Overwrite, Delete}` — IDENTICAL to the `VALIDATE_ADDED_DELETE_FILES_OPERATIONS` op-set
    /// [`added_delete_files_after`] already uses. Its V2 guard is correct here: DVs are a V3 feature, so a
    /// table carrying DVs is V2+ and the guard never excludes a real DV.
    ///
    /// **DV filter + collision (Java L867-873):** of those concurrently-added deletes, keep only the DVs
    /// (`file_format() == Puffin`); each DV is optionally narrowed by `conflict_filter` (Java passes it into
    /// the manifest scan — a DV whose metrics cannot match cannot conflict), then its `referenced_data_file` is
    /// checked against this row delta's added-DV set. The FIRST collision returns a NON-retryable
    /// [`ErrorKind::DataInvalid`] error matching Java's message ("Found concurrently added DV for
    /// {referenced_data_file}: {dv description}"), so the retry loop stops (Java's non-retryable
    /// `ValidationException`).
    async fn validate_added_dvs(
        &self,
        current: &Table,
        effective_start: Option<i64>,
        conflict_filter: Option<&Predicate>,
    ) -> Result<()> {
        // Java L831: skip if this operation adds no DVs (`dvsByReferencedFile.isEmpty()`).
        let added_dv_referenced = self.added_dv_referenced_files()?;
        if added_dv_referenced.is_empty() {
            return Ok(());
        }

        // Java L835-841: the concurrently-added delete files (DELETE-manifest walk + V2 guard, gated to the
        // `{Overwrite, Delete}` op set — the Rust-representable subset of Java's `{OVERWRITE, DELETE, REPLACE}`
        // `VALIDATE_ADDED_DVS_OPERATIONS`, identical to the existing added-delete-files op set).
        let added_deletes = added_delete_files_after(current, effective_start).await?;
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
            if concurrent.file_format() != DataFileFormat::Puffin {
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
            // the set, so it is skipped (it would never be a valid key in Java's `dvsByReferencedFile`).
            if let Some(referenced) = concurrent.referenced_data_file()
                && added_dv_referenced.contains(&referenced)
            {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Found concurrently added DV for {}: DV{{location={}, offset={:?}, length={:?}, referencedDataFile={}}}",
                        referenced,
                        concurrent.file_path(),
                        concurrent.content_offset(),
                        concurrent.content_size_in_bytes(),
                        referenced
                    ),
                ));
            }
        }

        Ok(())
    }
}

#[async_trait]
impl TransactionAction for RowDeltaAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
        )
        .with_added_delete_files(self.added_delete_files.clone());

        // Validate the added data files like fast append (Data content type, partition-spec match,
        // partition-value compatibility) and the added delete files (position/equality content type,
        // partition-spec match) — mirroring Java `MergingSnapshotProducer.add(DataFile)` /
        // `add(DeleteFile)`.
        snapshot_producer.validate_added_data_files()?;
        snapshot_producer.validate_added_delete_files()?;

        snapshot_producer
            .commit(
                RowDeltaOperation {
                    // Classified on the REQUESTED sets, before deletes resolve against the table —
                    // matching Java `BaseRowDelta.operation()` (`addsDataFiles()` / `addsDeleteFiles()`).
                    adds_data_files: !self.added_data_files.is_empty(),
                    adds_delete_files: !self.added_delete_files.is_empty(),
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
    /// 4. **`validateAddedDVs`** (Java L172 → `MergingSnapshotProducer.validateAddedDVs` L825-895, called
    ///    UNCONDITIONALLY — NOT gated by any flag): reject if a concurrent commit since the start added a
    ///    deletion vector (DV) for a data file THIS row delta also adds a DV for (two DVs per data file is a
    ///    write-write conflict). SELF-SKIPS when this row delta adds no DVs (Java L831
    ///    `dvsByReferencedFile.isEmpty()`), so it is a no-op for every non-DV row delta. See
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

        // 4. Concurrently-added deletion-vector conflict (Java `validateAddedDVs`, L172 / L825-895). Called
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
}

impl SnapshotProduceOperation for RowDeltaOperation {
    /// Classify the recorded operation exactly as Java `BaseRowDelta.operation()` does, on the
    /// REQUESTED add sets: adds-data-only (no delete files) → [`Operation::Append`], adds-deletes-only
    /// (no data files) → [`Operation::Delete`], both → [`Operation::Overwrite`]. An empty row delta
    /// (neither) is rejected by the producer before this is read.
    fn operation(&self) -> Operation {
        if self.adds_data_files && !self.adds_delete_files {
            Operation::Append
        } else if self.adds_delete_files && !self.adds_data_files {
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

    use crate::expr::Reference;
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Datum, Literal,
        ManifestContentType, ManifestStatus, Operation, Struct,
    };
    use crate::table::Table;
    use crate::transaction::tests::make_v3_minimal_table_in_catalog;
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
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

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
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

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
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
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

    /// Pins: an add-DATA-only row delta (no delete files) records `Append` (Java
    /// `BaseRowDelta.operation()` = APPEND when only data files are added and nothing is removed). Risk:
    /// the dynamic-op classifier wrongly recording `Overwrite`/`Delete` for a pure-add row delta. Note:
    /// this increment never removes files (`removeRows`/`removeDeletes` deferred), so Java's extra
    /// `!deletesDataFiles()` guard on the APPEND branch is always satisfied here.
    #[tokio::test]
    async fn test_row_delta_add_data_only_records_append() {
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
            Operation::Append,
            "an add-data-only row delta records Append (Java BaseRowDelta.operation())"
        );
    }

    /// Pins the row-delta SUMMARY counts: an add-data + add-position-delete row delta reports one added
    /// data file, one added delete file, one added position-delete file, and the right record/delete
    /// counts. Risk: the summary not reflecting the added delete files (downstream tooling that reads
    /// `added-delete-files`/`added-position-deletes` would under-report).
    #[tokio::test]
    async fn test_row_delta_summary_reflects_added_data_and_delete_counts() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
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
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

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
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
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
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
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
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
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
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
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
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

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
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
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
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
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
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
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
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
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
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
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
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
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
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
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
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
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
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
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
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
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
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
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
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
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
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
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
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
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
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
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
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
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
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
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
    // ALSO added a DV for A — two DVs for one data file is a write-write conflict. The concurrent walk reuses
    // `added_delete_files_after` (Java's `VALIDATE_ADDED_DVS_OPERATIONS = {OVERWRITE, DELETE, REPLACE}` reduces
    // to `{Overwrite, Delete}` in Rust, REPLACE being unrepresentable — identical to the added-delete op set).
    //
    // The race: a `row_delta` adding a DV for A is BUILT against head S0; BEFORE it commits a concurrent
    // `row_delta().add_deletes([DV for A])` lands (S1). On commit `do_commit` refreshes to S1 and runs
    // `validate` against that base; the concurrent DV for the SAME A collides ⇒ non-retryable rejection.
    // ============================================================================================

    /// Commit a CONCURRENT row delta that ADDS the given DVs (Puffin delete files, no data) in its own
    /// snapshot via the catalog. The resulting snapshot's operation is `Delete` (add-deletes-only), which is in
    /// `VALIDATE_ADDED_DELETE_FILES_OPERATIONS = {OVERWRITE, DELETE}` (= the Rust-representable DV op set) so
    /// the DV walk enumerates it. Mirrors `commit_concurrent_deletes` but for DVs.
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

    /// THE NON-DV NO-OP TEST (the always-on/self-skip semantics). A row delta adding ONLY NON-DV deletes (a
    /// plain position delete, NOT Puffin) commits even when a concurrent DV is present — because this row delta
    /// adds NO DV, the always-on `validateAddedDVs` SELF-SKIPS (Java L831 `dvsByReferencedFile.isEmpty()`),
    /// leaving nothing to conflict. This is the load-bearing behavior-preservation pin: the ~30 existing
    /// RowDelta tests all add non-Puffin deletes, so the DV check is a no-op for every one of them.
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

        // Row delta adds a NON-DV (plain Parquet position) delete — no DV ⇒ the DV check self-skips.
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_deletes(vec![synthetic_delete_file("test/a-pos-del.parquet", 0)])
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
}
